import json
from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db import DB_BaeminManual_load as manual
from modules.transform.pipelines.db import DB_Beamin_04_orders as collected_orders
from modules.transform.pipelines.db import DB_UnifiedSales_baemin as unified_baemin
from modules.transform.pipelines.db import DB_UnifiedSales_common as unified_common
from modules.transform.pipelines.db.DB_Beamin_04_orders import _COLUMNS
from modules.transform.pipelines.db.beamin_store_io import (
    order_date,
    read_table,
    replace_covered_date_range,
    write_table,
)
from scripts import fix_baemin_price_optname_260721 as price_fix
from scripts import repartition_baemin_orders as repartition


def _orders_stem(root: Path) -> Path:
    return root / "brand=도리당" / "store=해운대중동점" / "ym=2026-06" / "orders_2026-06"


def _row(order_id: str, order_time: str, amount: str, menu: str) -> dict:
    row = {col: "" for col in _COLUMNS}
    row.update(
        {
            "store_name": "해운대중동점",
            "주문상태": "배달완료",
            "주문번호": order_id,
            "주문시각": order_time,
            "주문내역": menu,
            "주문수량": "1",
            "수령방법": "배달",
            "결제금액": amount,
            "주문옵션상세": menu,
            "주문옵션금액": amount,
        }
    )
    return row


def test_price_option_repair_changes_only_option_name_and_is_idempotent():
    original = pd.DataFrame(
        [
            {
                "주문내역": "[복날한정] 1인 미나리 수삼 백숙 외 2건",
                "주문옵션상세": "16,900",
                "주문옵션금액": "12000",
                "결제금액": "12000",
                "주문번호": "A",
            },
            {
                "주문내역": "정상 메뉴",
                "주문옵션상세": "맵기 보통",
                "주문옵션금액": "10000",
                "결제금액": "10000",
                "주문번호": "B",
            },
        ]
    )

    repaired, changed = price_fix._correct_frame(original)
    rerun, rerun_changed = price_fix._correct_frame(repaired)

    assert changed == 1
    assert repaired["주문옵션상세"].tolist() == [
        "[복날한정] 1인 미나리 수삼 백숙",
        "맵기 보통",
    ]
    assert repaired[["주문옵션금액", "결제금액", "주문번호"]].equals(
        original[["주문옵션금액", "결제금액", "주문번호"]]
    )
    assert rerun_changed == 0
    pd.testing.assert_frame_equal(repaired, rerun)


def test_price_option_repair_backup_does_not_overwrite_existing_backup(tmp_path):
    target = tmp_path / "orders_2026-07.parquet"
    target.write_bytes(b"original")

    first_root = tmp_path / "local_backup" / "run_1"
    second_root = tmp_path / "local_backup" / "run_2"
    first = price_fix._backup_paths([target], "run_1", first_root)[target]
    second = price_fix._backup_paths([target], "run_2", second_root)[target]

    assert first != second
    assert first.parent == first_root
    assert second.parent == second_root
    assert not list(target.parent.glob("*.bak"))
    assert first.read_bytes() == b"original"
    assert second.read_bytes() == b"original"
    target.write_bytes(b"changed")
    price_fix._restore_backups({target: first})
    assert target.read_bytes() == b"original"
    price_fix._cleanup_backup_root(first_root)
    assert not first_root.exists()


def test_repartition_backup_is_kept_outside_analytics(tmp_path, monkeypatch):
    orders_root = tmp_path / "analytics" / "baemin_macro" / "orders"
    stem = orders_root / "brand=도리당" / "store=송파삼전점" / "ym=2026-07" / "orders_2026-07"
    parquet = stem.with_suffix(".parquet")
    parquet.parent.mkdir(parents=True)
    parquet.write_bytes(b"original")
    backup_root = tmp_path / "local_backup" / "repartition"
    monkeypatch.setattr(repartition, "BAEMIN_ORDERS_DB", orders_root)

    backups = repartition._backup_existing(stem, True, backup_root)

    assert backups == [
        str(
            backup_root
            / "brand=도리당"
            / "store=송파삼전점"
            / "ym=2026-07"
            / "orders_2026-07.parquet.bak"
        )
    ]
    assert Path(backups[0]).read_bytes() == b"original"
    assert not list(parquet.parent.glob("*.bak*"))


def test_repartition_verified_run_cleans_local_backup(tmp_path, monkeypatch):
    monkeypatch.setattr(repartition, "LOCAL_DB", tmp_path / "local")

    def fake_repartition(apply, backup_root):
        backup_root.mkdir(parents=True)
        (backup_root / "orders.parquet.bak").write_bytes(b"backup")
        return {"backups": 1}

    monkeypatch.setattr(repartition, "repartition_other_stores", fake_repartition)
    monkeypatch.setattr(
        repartition,
        "verify_invariants",
        lambda: {"partition_month_mismatches": 0, "cross_month_duplicate_orders": 0},
    )

    result = repartition.run(
        apply=True,
        skip_repartition=False,
        skip_songpa=True,
        verify=True,
    )

    assert result["meta"]["backup_cleaned"] is True
    assert not Path(result["meta"]["backup_root"]).exists()


def test_manual_baemin_load_upserts_month_partition_without_dropping_other_dates(tmp_path, monkeypatch):
    monkeypatch.setattr(manual, "BAEMIN_ORDERS_DB", tmp_path / "orders")
    monkeypatch.setattr(manual, "_manual_baemin_store_meta", lambda raw, fallback: ("해운대중동점", "도리당"))

    existing = pd.DataFrame(
        [
            _row("A", "2026. 06. 01. (월) 오후 1:00:00", "10000", "기존메뉴"),
            _row("B", "2026. 06. 02. (화) 오후 1:00:00", "20000", "교체전"),
        ],
        columns=_COLUMNS,
    )
    write_table(existing, _orders_stem(manual.BAEMIN_ORDERS_DB))

    csv_path = tmp_path / "baemin_orders_test.csv"
    new_rows = pd.DataFrame(
        [
            _row("B", "2026. 06. 02. (화) 오후 2:00:00", "22000", "교체후"),
            _row("C", "2026. 06. 24. (수) 오후 3:00:00", "30000", "신규메뉴"),
        ]
    )
    new_rows.to_csv(csv_path, index=False, encoding="utf-8-sig")

    loaded_rows, ok = manual._load_one_file(csv_path)

    out = read_table(_orders_stem(manual.BAEMIN_ORDERS_DB))
    assert ok is True
    assert loaded_rows == 2
    assert set(out["주문번호"]) == {"A", "B", "C"}
    assert out.loc[out["주문번호"].eq("A"), "결제금액"].iloc[0] == "10000"
    assert out.loc[out["주문번호"].eq("B"), "결제금액"].iloc[0] == "22000"
    assert out.loc[out["주문번호"].eq("C"), "결제금액"].iloc[0] == "30000"


def test_manual_baemin_latest_upsert_keeps_latest_order_snapshot():
    old = pd.DataFrame(
        [
            {**_row("A", "2026. 06. 01. (월) 오후 1:00:00", "10000", "old-main"), "collected_at": "2026-06-20T00:00:00+09:00"},
            {**_row("A", "2026. 06. 01. (월) 오후 1:00:00", "", "old-option"), "collected_at": "2026-06-20T00:00:00+09:00"},
            {**_row("B", "2026. 06. 02. (화) 오후 1:00:00", "20000", "keep"), "collected_at": "2026-06-20T00:00:00+09:00"},
        ],
        columns=_COLUMNS,
    )
    new = pd.DataFrame(
        [
            {**_row("A", "2026. 06. 01. (월) 오후 1:00:00", "11000", "new-main"), "collected_at": "2026-06-21T00:00:00+09:00"},
            {**_row("A", "2026. 06. 01. (월) 오후 1:00:00", "", "new-option"), "collected_at": "2026-06-21T00:00:00+09:00"},
            {**_row("A", "2026. 06. 01. (월) 오후 1:00:00", "", "new-option"), "collected_at": "2026-06-21T00:00:00+09:00"},
        ],
        columns=_COLUMNS,
    )

    out = manual._upsert_manual_orders(old, new)

    assert set(out["주문번호"]) == {"A", "B"}
    assert out[out["주문번호"].eq("A")]["주문내역"].tolist() == ["new-main", "new-option"]
    assert out[out["주문번호"].eq("B")]["주문내역"].tolist() == ["keep"]


def test_replace_covered_date_range_removes_ghost_and_keeps_outside_range():
    existing = pd.DataFrame(
        [
            _row("OUT", "2026. 05. 20. (수) 오후 1:00:00", "9000", "구간밖"),
            _row("GHOST", "2026. 06. 17. (수) 오후 1:00:00", "22000", "유령"),
            _row("EMPTY_DAY", "2026. 06. 18. (목) 오후 1:00:00", "10000", "빈날짜"),
        ],
        columns=_COLUMNS,
    )
    new = pd.DataFrame(
        [
            _row("B", "2026. 06. 17. (수) 오후 2:00:00", "50000", "정상1"),
            _row("C", "2026. 06. 19. (금) 오후 2:00:00", "60000", "정상2"),
        ],
        columns=_COLUMNS,
    )

    out, info = replace_covered_date_range(
        existing,
        new,
        order_date(existing["주문시각"]),
        order_date(new["주문시각"]),
    )

    assert set(out["주문번호"]) == {"OUT", "B", "C"}
    assert info["range"] == ("2026-06-17", "2026-06-19")
    assert info["removed"] == 2
    assert info["covered_dates"] == ["2026-06-17", "2026-06-18", "2026-06-19"]
    assert info["shrunk_dates"] == ["2026-06-18"]


def test_collected_baemin_orders_replace_same_date_ghost(tmp_path, monkeypatch):
    orders_root = tmp_path / "orders"
    monkeypatch.setattr(collected_orders, "BAEMIN_ORDERS_DB", orders_root)
    recorded = []
    monkeypatch.setattr(
        collected_orders,
        "record_manual_reingest_marker",
        lambda source, store, date, meta: recorded.append((source, store, date, meta)) or True,
    )
    stem = _orders_stem(orders_root)
    existing = pd.DataFrame(
        [
            _row("OUT", "2026. 06. 16. (화) 오후 1:00:00", "9000", "구간밖"),
            _row("GHOST", "2026. 06. 17. (수) 오후 1:00:00", "22000", "유령"),
        ],
        columns=_COLUMNS,
    )
    write_table(existing, stem)

    collected_orders._save_orders_csv(
        [_row("B", "2026. 06. 17. (수) 오후 2:00:00", "50000", "정상")],
        "도리당",
        "해운대중동점",
        "2026-06-17",
    )

    out = read_table(stem)
    assert set(out["주문번호"]) == {"OUT", "B"}
    assert recorded == [
        (
            "배민수동",
            "해운대중동점",
            "2026-06-17",
            {"rows": 1, "removed": 1},
        )
    ]


def test_manual_baemin_all_cancelled_file_preserves_existing_without_force(tmp_path, monkeypatch):
    monkeypatch.setattr(manual, "BAEMIN_ORDERS_DB", tmp_path / "orders")
    monkeypatch.setattr(manual, "_manual_baemin_store_meta", lambda raw, fallback: ("해운대중동점", "도리당"))
    recorded = []
    monkeypatch.setattr(
        manual,
        "record_manual_reingest_marker",
        lambda source, store, date, meta: recorded.append((source, store, date, meta)) or True,
    )

    existing = pd.DataFrame(
        [_row("A", "2026. 06. 01. (월) 오후 1:00:00", "100", "기존메뉴")],
        columns=_COLUMNS,
    )
    write_table(existing, _orders_stem(manual.BAEMIN_ORDERS_DB))
    cancelled = pd.DataFrame(
        [
            {
                **_row("A", "2026. 06. 01. (월) 오후 1:00:00", "100", "취소메뉴"),
                "주문상태": "취소",
            }
        ],
        columns=_COLUMNS,
    )
    csv_path = tmp_path / "baemin_orders_cancelled.csv"
    cancelled.to_csv(csv_path, index=False, encoding="utf-8-sig")

    loaded_rows, ok = manual._load_one_file(csv_path)

    out = read_table(_orders_stem(manual.BAEMIN_ORDERS_DB))
    assert ok is True
    assert loaded_rows == 0
    assert out["주문번호"].tolist() == ["A"]
    assert list(out.columns) == _COLUMNS
    assert recorded == [
        (
            "배민수동",
            "해운대중동점",
            "2026-06-01",
            {"rows": 0, "removed": 0},
        )
    ]


def test_manual_baemin_partial_shrink_overwrites_new_and_preserves_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(manual, "BAEMIN_ORDERS_DB", tmp_path / "orders")
    monkeypatch.setattr(manual, "_manual_baemin_store_meta", lambda raw, fallback: ("해운대중동점", "도리당"))
    recorded = []
    monkeypatch.setattr(
        manual,
        "record_manual_reingest_marker",
        lambda source, store, date, meta: recorded.append((source, store, date, meta)) or True,
    )

    existing = pd.DataFrame(
        [
            _row("A", "2026. 06. 01. (월) 오후 1:00:00", "100", "기존A"),
            _row("B", "2026. 06. 01. (월) 오후 2:00:00", "200", "기존B"),
        ],
        columns=_COLUMNS,
    )
    write_table(existing, _orders_stem(manual.BAEMIN_ORDERS_DB))
    new = pd.DataFrame(
        [
            {
                **_row("A", "2026. 06. 01. (월) 오후 1:00:00", "110", "신규A"),
                "주문상태": "배달완료",
            }
        ],
        columns=_COLUMNS,
    )
    csv_path = tmp_path / "baemin_orders_partial.csv"
    new.to_csv(csv_path, index=False, encoding="utf-8-sig")

    loaded_rows, ok = manual._load_one_file(csv_path)

    out = read_table(_orders_stem(manual.BAEMIN_ORDERS_DB))
    assert ok is True
    assert loaded_rows == 1
    assert set(out["주문번호"]) == {"A", "B"}
    assert out.loc[out["주문번호"].eq("A"), "주문내역"].iloc[0] == "신규A"
    assert out.loc[out["주문번호"].eq("B"), "주문내역"].iloc[0] == "기존B"
    assert recorded == [
        (
            "배민수동",
            "해운대중동점",
            "2026-06-01",
            {"rows": 1, "removed": 1},
        )
    ]


def test_manual_baemin_force_shrink_allows_range_clear(tmp_path, monkeypatch):
    monkeypatch.setattr(manual, "BAEMIN_ORDERS_DB", tmp_path / "orders")
    monkeypatch.setattr(manual, "_manual_baemin_store_meta", lambda raw, fallback: ("해운대중동점", "도리당"))
    recorded = []
    monkeypatch.setattr(
        manual,
        "record_manual_reingest_marker",
        lambda source, store, date, meta: recorded.append((source, store, date, meta)) or True,
    )

    existing = pd.DataFrame(
        [_row("A", "2026. 06. 01. (월) 오후 1:00:00", "100", "기존메뉴")],
        columns=_COLUMNS,
    )
    write_table(existing, _orders_stem(manual.BAEMIN_ORDERS_DB))
    cancelled = pd.DataFrame(
        [
            {
                **_row("A", "2026. 06. 01. (월) 오후 1:00:00", "100", "취소메뉴"),
                "주문상태": "취소",
            }
        ],
        columns=_COLUMNS,
    )
    csv_path = tmp_path / "baemin_orders_cancelled.csv"
    cancelled.to_csv(csv_path, index=False, encoding="utf-8-sig")

    loaded_rows, ok = manual._load_one_file(csv_path, force=True)

    out = read_table(_orders_stem(manual.BAEMIN_ORDERS_DB))
    assert ok is True
    assert loaded_rows == 0
    assert out.empty
    assert recorded == [
        (
            "배민수동",
            "해운대중동점",
            "2026-06-01",
            {"rows": 0, "removed": 1},
        )
    ]


def test_manual_reingest_marker_is_total_only_target(tmp_path, monkeypatch):
    marker_root = tmp_path / "markers"
    unified_root = tmp_path / "unified"
    unified_root.mkdir()
    monkeypatch.setattr(unified_common, "MANUAL_REINGEST_MARKER_ROOT", marker_root)
    monkeypatch.setattr(unified_common, "UNIFIED_ROOT", unified_root)
    monkeypatch.setattr(unified_baemin, "UNIFIED_ROOT", unified_root)

    date = "2000-01-01"
    store = "테스트매장"
    assert unified_common.record_manual_reingest_marker("배민수동", store, date, {}) is True
    base_dates = unified_baemin._resolve_baemin_target_dates([store], None, 14)

    assert date not in base_dates
    assert date in unified_baemin._target_dates_for_store(
        "배민수동",
        store,
        base_dates,
        None,
        14,
        unified_baemin._resolve_baemin_target_dates,
        include_reingest_markers=True,
    )


def test_manual_reingest_recent_window_filter_excludes_last_nine_days():
    now = unified_common.pendulum.datetime(2026, 8, 5, tz="Asia/Seoul")

    assert unified_common.filter_manual_reingest_dates_outside_recent_window(
        ["2026-08-05", "2026-08-04", "2026-07-28", "2026-07-27", "2026-01-02"],
        recent_days=9,
        now=now,
    ) == ["2026-01-02", "2026-07-27"]


def test_cleanup_manual_baemin_orders_moves_down_source_to_collect_dir(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    collect_dir = tmp_path / "collect"
    collect_archive = collect_dir / "_archived"
    down_dir.mkdir()
    collect_dir.mkdir()

    down_file = down_dir / "baemin_orders_down.csv"
    collect_file = collect_dir / "baemin_orders_collect.csv"
    down_file.write_text("x", encoding="utf-8")
    collect_file.write_text("x", encoding="utf-8")

    monkeypatch.setattr(manual, "DOWN_ARCHIVE_DIR", collect_dir)
    monkeypatch.setattr(manual, "ARCHIVE_DIR", collect_archive)
    payload = {
        "loaded_files": [
            {"path": str(down_file), "source": "down"},
            {"path": str(collect_file), "source": "collect"},
        ]
    }

    class TaskInstance:
        def xcom_pull(self, **kwargs):
            return json.dumps(payload, ensure_ascii=False)

    result = manual.cleanup_manual_baemin_orders(task_instance=TaskInstance())

    assert result == "cleanup complete: 2 files"
    assert not down_file.exists()
    assert not collect_file.exists()
    assert (collect_dir / down_file.name).exists()
    assert (collect_archive / collect_file.name).exists()


def test_iter_manual_files_includes_upload_temp_and_deduplicates(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    upload_temp = down_dir / "업로드_temp"
    collect_dir = tmp_path / "collect"
    upload_temp.mkdir(parents=True)
    collect_dir.mkdir()

    direct_file = down_dir / "baemin_marketing_direct.csv"
    temp_file = upload_temp / "baemin_marketing_temp.csv"
    collect_file = collect_dir / "baemin_marketing_collect.csv"
    for path in (direct_file, temp_file, collect_file):
        path.write_text("x", encoding="utf-8")

    monkeypatch.setattr(manual, "DOWN_DIR", down_dir)
    monkeypatch.setattr(manual, "COLLECT_SRC", collect_dir)

    files = manual._iter_manual_files("marketing")

    assert files == [
        {"path": str(direct_file), "source": "down"},
        {"path": str(temp_file), "source": "down"},
        {"path": str(collect_file), "source": "collect"},
    ]


def test_count_pending_manual_baemin_order_files_includes_all_order_sources(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    upload_temp = down_dir / "업로드_temp"
    collect_dir = tmp_path / "collect"
    manual_dir = tmp_path / "manual"
    for directory in (down_dir, upload_temp, collect_dir, manual_dir):
        directory.mkdir(parents=True, exist_ok=True)

    for path in (
        down_dir / "baemin_orders_down.csv",
        upload_temp / "baemin_orders_temp.csv",
        collect_dir / "baemin_orders_collect.csv",
        manual_dir / "baemin_orders_manual.csv",
        manual_dir / "baemin_orders_manual_20260826_nodate.csv",
        manual_dir / "baemin_orders_manual_20260826_partial.csv",
        manual_dir / "baemin_orders_manual_20260826_nodate_partial.csv",
        collect_dir / "baemin_marketing_collect.csv",
    ):
        path.write_text("x", encoding="utf-8")

    monkeypatch.setattr(manual, "DOWN_DIR", down_dir)
    monkeypatch.setattr(manual, "COLLECT_SRC", collect_dir)
    monkeypatch.setattr(manual, "MANUAL_DOWN_DIR", manual_dir)

    assert manual.count_pending_manual_baemin_order_files() == 5
    assert manual.count_partial_manual_baemin_order_files() == 2


def test_load_manual_baemin_orders_reports_actual_order_dates(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    collect_dir = tmp_path / "collect"
    out_root = tmp_path / "orders"
    down_dir.mkdir()
    collect_dir.mkdir()

    monkeypatch.setattr(manual, "DOWN_DIR", down_dir)
    monkeypatch.setattr(manual, "COLLECT_SRC", collect_dir)
    monkeypatch.setattr(manual, "MANUAL_DOWN_DIR", tmp_path / "manual")
    monkeypatch.setattr(manual, "BAEMIN_ORDERS_DB", out_root)
    monkeypatch.setattr(manual, "_manual_baemin_store_meta", lambda raw, fallback: ("해운대중동점", "도리당"))
    monkeypatch.setattr(manual, "record_manual_reingest_marker", lambda *args, **kwargs: True)

    rows = pd.DataFrame(
        [
            _row("A", "2026. 08. 25. (화) 오후 1:00:00", "10000", "메뉴A"),
            _row("B", "2026. 08. 26. (수) 오후 1:00:00", "12000", "메뉴B"),
        ],
        columns=_COLUMNS,
    )
    csv_path = down_dir / "baemin_orders_해운대중동점_1_20260826_nodate.csv"
    rows.to_csv(csv_path, index=False, encoding="utf-8-sig")

    payload = json.loads(manual.load_manual_baemin_orders(dag_run=type("DR", (), {"conf": {}})()))

    assert payload["order_dates"] == ["2026-08-25", "2026-08-26"]
    assert payload["loaded_files"][0]["dates"] == ["2026-08-25", "2026-08-26"]


def test_load_manual_baemin_orders_skips_partial_files_without_loading(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    collect_dir = tmp_path / "collect"
    down_dir.mkdir()
    collect_dir.mkdir()

    partial_path = down_dir / "baemin_orders_해운대중동점_1_20260826_nodate_partial.csv"
    pd.DataFrame([_row("A", "2026. 08. 26. (수) 오후 1:00:00", "10000", "메뉴A")]).to_csv(
        partial_path,
        index=False,
        encoding="utf-8-sig",
    )

    monkeypatch.setattr(manual, "DOWN_DIR", down_dir)
    monkeypatch.setattr(manual, "COLLECT_SRC", collect_dir)
    monkeypatch.setattr(manual, "MANUAL_DOWN_DIR", tmp_path / "manual")
    monkeypatch.setattr(manual, "_load_one_file", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("partial loaded")))

    payload = json.loads(manual.load_manual_baemin_orders(dag_run=type("DR", (), {"conf": {}})()))

    assert payload["loaded_files"] == []
    assert payload["skipped_files"] == []
    assert payload["partial_files"] == [str(partial_path)]
    assert payload["order_dates"] == []
    assert partial_path.exists()


def test_load_manual_baemin_marketing_upserts_latest_by_store_id_and_date(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    collect_dir = tmp_path / "collect"
    out_root = tmp_path / "marketing_out"
    down_dir.mkdir()
    collect_dir.mkdir()

    monkeypatch.setattr(manual, "DOWN_DIR", down_dir)
    monkeypatch.setattr(manual, "COLLECT_SRC", collect_dir)
    monkeypatch.setattr(manual, "BAEMIN_MARKETING_DB", out_root)

    csv_path = down_dir / "baemin_marketing_[음식배달]_닭도리탕_전문_도리당_미사점_14356015_202607.csv"
    pd.DataFrame(
        [
            {
                "collected_at": "2026-07-14T08:22:34.752Z",
                "store_id": "14356015",
                "store_name": "[음식배달] 닭도리탕 전문 도리당 미사점",
                "날짜": "2026-07-01",
                "광고지출": "100",
            },
            {
                "collected_at": "2026-07-15T08:22:34.752Z",
                "store_id": "14356015",
                "store_name": "[음식배달] 닭도리탕 전문 도리당 미사점",
                "날짜": "2026-07-01",
                "광고지출": "200",
            },
        ]
    ).to_csv(csv_path, index=False, encoding="utf-8-sig")

    result = json.loads(manual.load_manual_baemin_marketing())
    out_path = out_root / "brand=도리당" / "store=미사점" / "ym=2026-07" / "baemin_marketing_data.csv"
    out = pd.read_csv(out_path, dtype=str, encoding="utf-8-sig")

    assert result["loaded_files"] == [{"path": str(csv_path), "source": "down"}]
    assert len(out) == 1
    assert out["광고지출"].iloc[0] == "200"
    assert out["날짜"].iloc[0] == "2026-07-01 12:00:00 AM"


def test_load_manual_baemin_marketing_deduplicates_existing_midnight_date(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    collect_dir = tmp_path / "collect"
    out_root = tmp_path / "marketing_out"
    down_dir.mkdir()
    collect_dir.mkdir()

    monkeypatch.setattr(manual, "DOWN_DIR", down_dir)
    monkeypatch.setattr(manual, "COLLECT_SRC", collect_dir)
    monkeypatch.setattr(manual, "BAEMIN_MARKETING_DB", out_root)

    out_dir = out_root / "brand=도리당" / "store=미사점" / "ym=2026-07"
    out_dir.mkdir(parents=True)
    out_path = out_dir / "baemin_marketing_data.csv"
    pd.DataFrame(
        [
            {
                "collected_at": "2026-07-14T08:22:34.752Z",
                "store_id": "14356015",
                "store_name": "[음식배달] 닭도리탕 전문 도리당 미사점",
                "날짜": "2026-07-01 12:00:00 AM",
                "광고지출": "100",
            }
        ]
    ).to_csv(out_path, index=False, encoding="utf-8-sig")

    csv_path = down_dir / "baemin_marketing_[음식배달]_닭도리탕_전문_도리당_미사점_14356015_202607.csv"
    pd.DataFrame(
        [
            {
                "collected_at": "2026-07-15T08:22:34.752Z",
                "store_id": "14356015",
                "store_name": "[음식배달] 닭도리탕 전문 도리당 미사점",
                "날짜": "2026-07-01",
                "광고지출": "200",
            }
        ]
    ).to_csv(csv_path, index=False, encoding="utf-8-sig")

    manual.load_manual_baemin_marketing()
    out = pd.read_csv(out_path, dtype=str, encoding="utf-8-sig")

    assert len(out) == 1
    assert out["광고지출"].iloc[0] == "200"


def test_load_manual_baemin_metrics_replaces_same_date(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    collect_dir = tmp_path / "collect"
    out_root = tmp_path / "metrics_out"
    down_dir.mkdir()
    collect_dir.mkdir()

    monkeypatch.setattr(manual, "DOWN_DIR", down_dir)
    monkeypatch.setattr(manual, "COLLECT_SRC", collect_dir)
    monkeypatch.setattr(manual, "BAEMIN_METRICS_DB", out_root)

    out_dir = out_root / "brand=도리당" / "store=경북상주점" / "ym=2026-07"
    out_dir.mkdir(parents=True)
    out_path = out_dir / "baemin_now.csv"
    pd.DataFrame(
        [
            {
                "date": "2026-07-10",
                "store_id": "14818072",
                "store_name": "[음식배달] 닭도리탕 전문 도리당 경북상주점",
                "최근별점": "4.1",
            }
        ]
    ).to_csv(out_path, index=False, encoding="utf-8-sig")

    csv_path = down_dir / "baemin_metrics_[음식배달]_닭도리탕_전문_도리당_경북상주점_14818072_20260710.csv"
    pd.DataFrame(
        [
            {
                "collected_at": "2026-07-10T08:28:08.233Z",
                "store_id": "14818072",
                "store_name": "[음식배달] 닭도리탕 전문 도리당 경북상주점",
                "최근별점": "4.8",
            }
        ]
    ).to_csv(csv_path, index=False, encoding="utf-8-sig")

    result = json.loads(manual.load_manual_baemin_metrics())
    out = pd.read_csv(out_path, dtype=str, encoding="utf-8-sig")

    assert result["loaded_files"] == [{"path": str(csv_path), "source": "down"}]
    assert len(out) == 1
    assert out["date"].iloc[0] == "2026-07-10"
    assert out["최근별점"].iloc[0] == "4.8"
    assert out["brand_store"].iloc[0] == "도리당|경북상주점"
    assert list(out.columns[-3:]) == ["brand", "store", "brand_store"]


def test_cleanup_manual_baemin_files_moves_unified_payload(tmp_path, monkeypatch):
    down_dir = tmp_path / "down"
    collect_dir = tmp_path / "collect"
    collect_archive = collect_dir / "_archived"
    down_dir.mkdir()
    collect_dir.mkdir()

    order_file = down_dir / "baemin_orders_down.csv"
    marketing_file = collect_dir / "baemin_marketing_collect.csv"
    metrics_file = collect_dir / "baemin_metrics_collect.csv"
    for path in (order_file, marketing_file, metrics_file):
        path.write_text("x", encoding="utf-8")

    monkeypatch.setattr(manual, "DOWN_ARCHIVE_DIR", collect_dir)
    monkeypatch.setattr(manual, "ARCHIVE_DIR", collect_archive)
    payload = {
        "orders": [{"path": str(order_file), "source": "down"}],
        "marketing": [{"path": str(marketing_file), "source": "collect"}],
        "metrics": [{"path": str(metrics_file), "source": "collect"}],
    }

    class TaskInstance:
        def xcom_pull(self, **kwargs):
            return json.dumps(payload, ensure_ascii=False)

    result = manual.cleanup_manual_baemin_files(task_instance=TaskInstance())

    assert result == "cleanup complete: 3 files"
    assert (collect_dir / order_file.name).exists()
    assert (collect_archive / marketing_file.name).exists()
    assert (collect_archive / metrics_file.name).exists()
