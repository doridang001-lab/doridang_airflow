from __future__ import annotations

import itertools
import json
from pathlib import Path

import pandas as pd
import pytest

from modules.transform.pipelines.db import DB_OrderCrossAnalysis as runtime
from modules.transform.pipelines.db import DB_OrderCrossAnalysis_core as core

DATE = "2026-09-08"


def row(item="M", name="닭도리탕", order="O", qty=1, amount=100, source="posfeed", **kwargs):
    return {"sale_date": DATE, "ym": "2026-09", "source": source, "brand": "도리당", "store": "테스트점",
            "order_type": "배달", "order_id": order, "menu_name": "닭도리탕", "item_seq": "1", "item_id": item,
            "item_name": name, "qty": qty, "total_price": amount, "sale_type": "정상", **kwargs}


def catalog(entries=None):
    entries = entries or [("M", "닭도리탕", "메인"), ("P", "분모자", "토핑")]
    base = pd.DataFrame([{"source": "posfeed", "brand": "도리당", "store": "테스트점", "상품코드": i,
                          "상품명": n, "중메뉴": "", "is_main_candidate": "N", "수동분류": c} for i, n, c in entries])
    overlay = pd.DataFrame([{"source": "posfeed", "brand": "도리당", "store": "테스트점", "item_id": i,
                             "standard_menu_name": n, "category": c} for i, n, c in entries])
    return core.make_catalog(base, overlay)


def build(rows, cat=None):
    return core.build_day(pd.DataFrame(rows, columns=core.REQUIRED_SALES_COLUMNS if not rows else None), DATE, cat or catalog())


def test_schema_and_repeated_main_and_pair():
    result = build([row(), row(qty=2), row("P", "분모자", qty=2, amount=200), row("P", "분모자", qty=3, amount=300)])
    c = result["cross"]
    assert list(c) == core.CROSS_COLUMNS
    assert len(c) == 1
    assert c.iloc[0][["co_order_cnt", "co_qty", "co_amount"]].tolist() == [1, 5, 500]
    assert c.main_item_id.tolist() == ["M"]
    assert c.co_qty.dtype == "int64"


def test_denominator_includes_main_only_orders():
    result = build([row(order="A"), row("P", "분모자", order="A"), row(order="B")])
    s = result["standard_pairs"].iloc[0]
    assert (s.co_order_cnt, s.main_order_cnt, s.selection_rate) == (1, 2, 0.5)


@pytest.mark.parametrize("name,category", [("리뷰 분모자", "리뷰"), ("[후.참] 통 가래떡", "리뷰"),
    ("리뷰이벤트", "리뷰참여"), ("리뷰 미참여", "미선택"), ("리뷰 괜찮습니다.", "미선택"),
    ("리뷰 공기밥 괜찮아요", "미선택"), ("기본맛", "옵션")])
def test_review_and_decline_never_become_main(name, category):
    cat = catalog([("M", "닭도리탕", "메인"), ("R", name, "리뷰" if "리뷰" in name else "옵션")])
    cat.base[("posfeed", "도리당", "테스트점", "R")]["category"] = "메인"
    result = build([row(), row("R", name, amount=0)], cat)
    assert result["cross"].main_item_id.tolist() == ["M"]
    assert result["cross"].pair_type.tolist() == [category]


def test_review_mapping_without_name_marker_is_preserved():
    result = build([row(), row("R", "통 가래떡")], catalog([("M", "닭도리탕", "메인"), ("R", "통 가래떡", "리뷰")]))
    assert result["cross"].pair_type.tolist() == ["리뷰"]


def test_manual_other_is_not_inferred_as_topping():
    result = build([row(), row("P", "분모자")], catalog([("M", "닭도리탕", "메인"), ("P", "분모자", "기타")]))
    assert result["cross"].pair_type.tolist() == ["기타"]


def test_no_store_scope_filter_and_unknown_is_visible():
    cat = catalog()
    cat.overlay = {}
    result = build([row(), row("X", "새품목")], cat)
    assert result["cross"].pair_type.tolist() == ["미분류"]


def test_unknown_main_is_accounted_for():
    result = build([row("X", "새품목", menu_name="")])
    assert result["cross"].empty
    assert result["quality"]["stores"][0]["unresolved_main_orders"] == 1


def test_order_header_anchors_unmapped_item_without_changing_product_classification():
    from scripts.order_cross_reference import check_reference
    rows = [row("X", "1인 순살 닭한마리 한상", menu_name="1인 순살 닭한마리 한상"),
            row("R", "[ 후 참 ] 분모자. 1개", menu_name="1인 순살 닭한마리 한상", amount=0),
            row("X", "1인 순살 닭한마리 한상", order="B", menu_name="1인 순살 닭한마리 한상")]
    result = build(rows)
    assert result["cross"].main_item_id.tolist() == ["X"]
    assert result["cross"].pair_type.tolist() == ["리뷰"]
    assert result["standard_pairs"].main_order_cnt.tolist() == [2]
    assert not result["item_mapping"].query("item_id == 'X'").is_main.any()
    check_reference(pd.DataFrame(rows), result)


def test_order_header_without_physical_row_or_catalog_still_anchors():
    from scripts.order_cross_reference import check_reference
    rows = [row("P", "분모자", menu_name="새 주문 메뉴")]
    result = build(rows)
    assert result["cross"].main_source.tolist() == ["menu_name"]
    assert result["cross"].main_name.tolist() == ["새 주문 메뉴"]
    check_reference(pd.DataFrame(rows), result)


def test_header_role_is_per_order_and_includes_non_main_categories():
    from scripts.order_cross_reference import check_reference
    rows = [row("D", "콜라", menu_name="콜라"), row("P", "분모자", menu_name="콜라"),
            row(order="B"), row("D", "콜라", order="B")]
    result = build(rows, catalog([("M", "닭도리탕", "메인"), ("D", "콜라", "음료"), ("P", "분모자", "토핑")]))
    assert set(zip(result["cross"].main_item_id, result["cross"].pair_item_id)) == {("D", "P"), ("M", "D")}
    check_reference(pd.DataFrame(rows), result)


@pytest.mark.parametrize("menu", ["배달비", "리뷰 왕만두 2개", "리뷰 미참여"])
def test_non_product_headers_do_not_invent_main(menu):
    result = build([row("R", "리뷰 왕만두 2개", menu_name=menu)])
    assert result["cross"].empty
    assert result["quality"]["stores"][0]["unresolved_main_orders"] == 1


def test_multiple_unmapped_order_headers_use_actual_ids():
    from scripts.order_cross_reference import check_reference
    rows = [row("X", "새 한상", menu_name="새 한상"), row("Y", "다른 한상", menu_name="다른 한상"),
            row("P", "분모자", menu_name="새 한상")]
    result = build(rows)
    assert set(result["cross"].main_item_id) == {"X", "Y"}
    assert result["cross"].is_multi_main.all()
    check_reference(pd.DataFrame(rows), result)


def test_trusted_synthetic_main_does_not_drop_first_option():
    result = build([row("P", "분모자")])
    assert result["cross"].main_source.tolist() == ["menu_name"]
    assert result["cross"].pair_item_id.tolist() == ["P"]


def test_multiple_mains_are_associated_once_each():
    cat = catalog([("M", "닭도리탕", "메인"), ("N", "곱도리탕", "메인"), ("P", "분모자", "토핑")])
    result = build([row(), row(), row("N", "곱도리탕"), row("P", "분모자", amount=200)], cat)
    c = result["cross"]
    assert len(c) == 4 and c.is_multi_main.all()
    assert c[c.pair_item_id.eq("P")].co_amount.tolist() == [200, 200]


def test_standard_grouping_deduplicates_before_counting():
    cat = catalog([("M", "닭도리탕", "메인"), ("N", "닭도리탕", "메인"), ("P", "분모자", "토핑"), ("Q", "분모자", "토핑")])
    result = build([row(), row("N", "닭도리탕"), row("P", "분모자", qty=2), row("Q", "분모자", qty=3)], cat)
    assert len(result["cross"]) == 6  # 각 메인당 나머지 3품목
    s = result["standard_pairs"]
    assert len(s) == 1
    assert s.iloc[0][["co_order_cnt", "co_qty", "main_order_cnt"]].tolist() == [1, 5, 1]


def test_different_sources_same_order_number_do_not_collapse():
    cat = catalog()
    for key, val in list(cat.base.items()):
        newkey = ("쿠팡수동", *key[1:3], "C" + key[-1])
        cat.base[newkey] = val.copy()
        cat.overlay[newkey] = cat.overlay[key].copy()
    result = build([row(), row("P", "분모자"), row("CM", source="쿠팡수동"), row("CP", "분모자", source="쿠팡수동")], cat)
    assert result["standard_pairs"].iloc[0].co_order_cnt == 2


@pytest.mark.parametrize("kwargs,reason", [({"sale_type": "취소"}, "cancelled"), ({"qty": 0}, "nonpositive_qty"),
    ({"amount": -1}, "negative_amount"), ({"item": ""}, "missing_identity"), ({"name": "정산차액(OKPOS)", "qty": 0}, "fee_or_adjustment")])
def test_exclusions_are_recorded(kwargs, reason):
    result = build([row(**kwargs)])
    assert result["quality"]["excluded"][reason] == 1


@pytest.mark.parametrize("kwargs", [{"qty": "bad"}, {"qty": 0.5}, {"amount": float("inf")}, {"sale_date": "2026-09-07"}])
def test_invalid_input_fails(kwargs):
    with pytest.raises(ValueError):
        build([row(**kwargs)])


def test_pk_conflict_fails_but_exact_duplicate_is_counted_once():
    r = row(_pk="1")
    result = build([r, r, row("P", "분모자", _pk="2")])
    assert result["quality"]["excluded"]["exact_duplicate"] == 1
    with pytest.raises(ValueError, match="PK"):
        build([r, row(qty=2, _pk="1")])


def test_empty_day_schema_and_repeatability():
    first, second = build([]), build([])
    pd.testing.assert_frame_equal(first["cross"], second["cross"])
    assert first["cross"].co_qty.dtype == "int64"


def test_name_with_discount_word_is_not_fee():
    assert not core.is_fee("할인 세트 닭도리탕")
    assert core.is_fee("배달비")


def test_catalog_conflicting_overlay_fails():
    master = pd.DataFrame([{"source": "posfeed", "brand": "도리당", "store": "테스트점", "상품코드": "M", "상품명": "메뉴"}])
    over = pd.DataFrame([{"source": "posfeed", "brand": "도리당", "store": "테스트점", "item_id": "M", "category": c, "standard_menu_name": "메뉴"} for c in ["메인", "리뷰"]])
    with pytest.raises(ValueError, match="충돌"):
        core.make_catalog(master, over)


def local_run(tmp_path, monkeypatch):
    inputs, outputs = tmp_path / "input", tmp_path / "output"
    inputs.mkdir()
    path = runtime._input_path(DATE, inputs)
    pd.DataFrame([row(), row("P", "분모자")]).to_parquet(path, index=False)
    cat = catalog()
    monkeypatch.setattr(runtime, "LOCAL_DB", tmp_path / "local")
    runtime._process_order_cross(DATE, output_root=outputs, input_root=inputs, catalog=cat)
    return inputs, outputs, cat


def test_atomic_publication_schema_support_and_freshness(tmp_path, monkeypatch):
    inputs, outputs, cat = local_run(tmp_path, monkeypatch)
    bundle = runtime.load_validated_support(DATE, outputs, input_root=inputs, catalog=cat)
    assert bundle["standard_pairs"].selection_rate.tolist() == [1.0]
    assert list(pd.read_parquet(runtime._cross_daily_path(DATE, outputs))) == core.CROSS_COLUMNS
    pd.DataFrame([row()]).to_parquet(runtime._input_path(DATE, inputs), index=False)
    with pytest.raises(ValueError, match="갱신 대기"):
        runtime.load_validated_support(DATE, outputs, input_root=inputs, catalog=cat)


def test_input_failure_does_not_replace_previous(tmp_path, monkeypatch):
    inputs, outputs, cat = local_run(tmp_path, monkeypatch)
    path = runtime._cross_daily_path(DATE, outputs)
    before = path.read_bytes()
    runtime._input_path(DATE, inputs).write_bytes(b"corrupt")
    with pytest.raises(Exception):
        runtime._process_order_cross(DATE, output_root=outputs, input_root=inputs, catalog=cat)
    assert path.read_bytes() == before


def test_replace_failure_preserves_previous(tmp_path, monkeypatch):
    inputs, outputs, cat = local_run(tmp_path, monkeypatch)
    path = runtime._cross_daily_path(DATE, outputs)
    before = path.read_bytes()
    def fail(*args):
        raise PermissionError("locked")
    monkeypatch.setattr(runtime.os, "replace", fail)
    with pytest.raises(PermissionError):
        runtime._process_order_cross(DATE, output_root=outputs, input_root=inputs, catalog=cat)
    assert path.read_bytes() == before


def test_onedrive_requires_explicit_publish():
    with pytest.raises(PermissionError):
        runtime._process_order_cross(DATE)


@pytest.mark.skipif(runtime.os.name != "nt", reason="Windows 파일 속성")
def test_cleanup_handles_synced_readonly_directory(tmp_path):
    import stat
    folder = tmp_path / "old_generation"
    folder.mkdir()
    (folder / "quality.json").write_text("{}", encoding="utf-8")
    folder.chmod(stat.S_IREAD)
    runtime._remove_under(folder, tmp_path)
    assert not folder.exists()


def test_support_corruption_is_rejected(tmp_path, monkeypatch):
    inputs, outputs, cat = local_run(tmp_path, monkeypatch)
    path = runtime._cross_daily_path(DATE, outputs)
    support = runtime._support_dir(path, runtime._generation(path))
    (support / "standard_pairs.parquet").write_bytes(b"bad")
    with pytest.raises(ValueError, match="변경"):
        runtime.load_validated_support(DATE, outputs, input_root=inputs, catalog=cat)


def test_independent_order_set_reference():
    entries = [("M", "닭도리탕", "메인"), ("N", "곱도리탕", "메인"), ("P", "분모자", "토핑")]
    cat = catalog(entries)
    rows, expected, denominators = [], {}, {"M": set(), "N": set()}
    for index, quantities in enumerate(itertools.product(range(3), repeat=3)):
        order = str(index)
        present = {item: qty for (item, _, _), qty in zip(entries, quantities) if qty}
        for (item, name, _), qty in zip(entries, quantities):
            if qty:
                rows.extend([row(item, name, order=order, amount=10, menu_name="") for _ in range(qty)])
        for main in set(present) & {"M", "N"}:
            denominators[main].add(order)
            for pair in set(present) - {main}:
                value = expected.setdefault((main, pair), [set(), 0, 0])
                value[0].add(order)
                value[1] += present[pair]
                value[2] += present[pair] * 10
    result = build(rows, cat)
    actual = {(r.main_item_id, r.pair_item_id): [r.co_order_cnt, r.co_qty, r.co_amount] for r in result['cross'].itertuples()}
    assert actual == {k: [len(v[0]), v[1], v[2]] for k, v in expected.items()}


def test_source_alias_and_master_manual_priority():
    cat = catalog()
    base = pd.DataFrame([{"source": "unipos", "brand": "도리당", "store": "테스트점", "상품코드": "R",
                          "상품명": "리뷰 분모자", "is_main_candidate": "Y", "중메뉴": "메인메뉴", "수동분류": "리뷰"}])
    over = pd.DataFrame(columns=core.IDENTITY + ["category", "standard_menu_name"])
    cat2 = core.make_catalog(base, over)
    assert cat2.base[("unipos", "도리당", "테스트점", "R")]["category"] == "리뷰"
    result = build([row("R", "리뷰 분모자", source="unionpos", menu_name="")], cat2)
    assert result["item_mapping"].category.tolist() == ["리뷰"]


def test_same_id_across_sources_is_rejected():
    with pytest.raises(ValueError, match="상품번호 충돌"):
        build([row(), row(source="쿠팡수동")])


def test_input_change_during_publish_preserves_old_result(tmp_path, monkeypatch):
    inputs, outputs, cat = local_run(tmp_path, monkeypatch)
    path = runtime._cross_daily_path(DATE, outputs)
    before = path.read_bytes()
    real_write = runtime.pq.write_table
    def write_and_change(*args, **kwargs):
        real_write(*args, **kwargs)
        runtime._input_path(DATE, inputs).write_bytes(b"changed")
    monkeypatch.setattr(runtime.pq, "write_table", write_and_change)
    with pytest.raises(RuntimeError, match="입력 변경"):
        runtime._process_order_cross(DATE, output_root=outputs, input_root=inputs, catalog=cat)
    assert path.read_bytes() == before


def test_post_commit_failure_rolls_back(tmp_path, monkeypatch):
    inputs, outputs, cat = local_run(tmp_path, monkeypatch)
    path = runtime._cross_daily_path(DATE, outputs)
    before = path.read_bytes()
    def fail(*args, **kwargs):
        raise ValueError("재읽기 실패")
    monkeypatch.setattr(runtime, "load_validated_support", fail)
    with pytest.raises(ValueError, match="재읽기"):
        runtime._process_order_cross(DATE, output_root=outputs, input_root=inputs, catalog=cat)
    assert path.read_bytes() == before


def test_pending_prioritizes_recent_and_limits_history(monkeypatch, tmp_path):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    monkeypatch.setattr(runtime, "_now", lambda: datetime(2026, 9, 9, tzinfo=ZoneInfo("Asia/Seoul")))
    monkeypatch.setattr(runtime, "load_catalog", catalog)
    monkeypatch.setattr(runtime, "_dates", lambda *a: ["2026-08-01", "2026-08-02", "2026-08-03", "2026-09-08"])
    monkeypatch.setattr(runtime, "_is_current", lambda *a: False)
    seen = []
    def process(date, **kwargs):
        seen.append(date)
        return {"date": date, "rows": 1}
    monkeypatch.setattr(runtime, "_process_order_cross", process)
    result = runtime.process_pending(output_root=tmp_path, max_history_dates=2)
    assert seen == ["2026-09-08", "2026-08-01", "2026-08-02"]
    assert result['remaining'] == 1


def test_report_uses_main_denominator_across_order_types(monkeypatch):
    from modules.transform.pipelines.db import DB_Daily_Corporate_Store_Report as report
    rows = [row(order="1"), row("P", "분모자", order="1"), row(order="2", order_type="홀_테이블")]
    bundle = build(rows)
    monkeypatch.setattr(runtime, "load_validated_support", lambda *a, **k: bundle)
    monkeypatch.setattr(report, "STORE_DISPLAY_NAME", "테스트점")
    lines = report._recommended_menu_combo_lines({"report_date": DATE})
    assert any("메인 주문 2건 중 1건, 50.0%" in line for line in lines)


def test_report_stale_input_is_visible(monkeypatch):
    from modules.transform.pipelines.db import DB_Daily_Corporate_Store_Report as report
    def fail(*args, **kwargs):
        raise ValueError("stale")
    monkeypatch.setattr(runtime, "load_validated_support", fail)
    assert report._recommended_menu_combo_lines({"report_date": DATE}) == ["교차분석 갱신 대기"]


def test_empty_pk_rows_are_not_removed_with_unrelated_duplicate():
    rows = [row(_pk="one"), row(_pk="one"), row("P", "분모자", _pk=""), row("P", "분모자", _pk="")]
    result = build(rows)
    assert result["cross"].co_qty.tolist() == [2]


def test_synthetic_self_name_not_in_standard_pairs():
    result = build([row("X", "닭도리탕"), row("P", "분모자")])
    assert result["cross"].pair_item_id.tolist() == ["P"]
    assert result["standard_pairs"].pair_name.tolist() == ["분모자"]
    assert result["cross"].main_item_id.tolist() == ["X"]
    assert result["cross"].main_source.tolist() == ["item_id"]


@pytest.mark.parametrize("name", ["리뷰", "후기", "리뷰 서비스", "[후.참]"])
def test_generic_review_is_participation(name):
    assert core.special_category([name]) == "리뷰참여"


def test_result_content_corruption_is_rejected(tmp_path, monkeypatch):
    inputs, outputs, cat = local_run(tmp_path, monkeypatch)
    path = runtime._cross_daily_path(DATE, outputs)
    table = runtime.pq.read_table(path)
    df = table.to_pandas()
    df["co_amount"] += 1
    changed = runtime.pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata(table.schema.metadata)
    runtime.pq.write_table(changed, path)
    with pytest.raises(ValueError, match="내용 변경"):
        runtime.load_validated_support(DATE, outputs, input_root=inputs, catalog=cat)


def test_independent_dataframe_reference_matches_core():
    from scripts.order_cross_reference import check_reference
    rows = [row(), row(qty=2), row("P", "분모자", qty=3), row("P", "분모자", order="other")]
    bundle = build(rows)
    assert check_reference(pd.DataFrame(rows), bundle)["raw_pairs"] == len(bundle["cross"])


def test_pending_failure_is_not_reported_as_complete(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime, "load_catalog", catalog)
    monkeypatch.setattr(runtime, "_dates", lambda *a: [DATE])
    monkeypatch.setattr(runtime, "_is_current", lambda *a: False)
    def fail(*args, **kwargs):
        raise ValueError("bad partition")
    monkeypatch.setattr(runtime, "_process_order_cross", fail)
    with pytest.raises(RuntimeError, match="미완료"):
        runtime.process_pending(output_root=tmp_path)


def test_concurrent_date_writers_produce_one_valid_generation(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    inputs, outputs, cat = local_run(tmp_path, monkeypatch)
    def run():
        return runtime._process_order_cross(DATE, output_root=outputs, input_root=inputs, catalog=cat)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: run(), range(2)))
    assert [r['rows'] for r in results] == [1, 1]
    bundle = runtime.load_validated_support(DATE, outputs, input_root=inputs, catalog=cat)
    assert len(bundle['cross']) == 1
    assert len(list((outputs / '_support' / '260908').iterdir())) == 1


def test_valid_checkpoint_skips_rebuilding(tmp_path, monkeypatch):
    inputs, outputs, cat = local_run(tmp_path, monkeypatch)
    def fail(*a, **k):
        raise AssertionError("계산하면 안 됨")
    monkeypatch.setattr(runtime, 'build_day', fail)
    assert runtime._process_order_cross(DATE, overwrite=False, output_root=outputs, input_root=inputs, catalog=cat)['skipped']
