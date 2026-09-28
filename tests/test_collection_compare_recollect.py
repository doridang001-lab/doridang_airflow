"""쿠팡 누락수집 목록(export_coupang_recollect_list) 판정 테스트."""

import pandas as pd

from modules.transform.pipelines.db.DB_CollectionCompare import (
    _coupang_recollect_rows,
    _merge_date_ranges,
    export_coupang_recollect_list,
)

TODAY = "2026-09-28"


def _row(date, store, toorder=None, unified=None, coupang=None, group="쿠팡이츠"):
    return {
        "date": date,
        "store": store,
        "platform_group": group,
        "amt_toorder": toorder or 0,
        "present_toorder": toorder is not None,
        "amt_unified": unified or 0,
        "present_unified": unified is not None,
        "amt_coupang_macro": coupang or 0,
        "present_coupang_macro": coupang is not None,
    }


def _targets(rows):
    out = _coupang_recollect_rows(pd.DataFrame(rows), TODAY)
    return set(zip(out["date"], out["store"]))


def test_unified_equals_coupang_is_ok_even_if_toorder_differs():
    rows = [
        _row("2026-09-21", "세종나성점", toorder=458300, unified=468101, coupang=468100),
        _row("2026-09-22", "세종나성점", toorder=404900, unified=410400, coupang=410400),
    ]
    assert _targets(rows) == set()


def test_under_and_missing_are_targets_over_is_not():
    rows = [
        _row("2026-09-23", "부산대신점", toorder=178000, unified=178000, coupang=135500),
        _row("2026-09-27", "김포풍무점", toorder=134600, unified=709600),
        _row("2026-09-24", "과대점", unified=100000, coupang=150000),
        _row("2026-09-24", "소액차점", unified=100000, coupang=99001),
    ]
    assert _targets(rows) == {("2026-09-23", "부산대신점"), ("2026-09-27", "김포풍무점")}


def test_toorder_fallback_and_zero_reference():
    rows = [
        _row("2026-09-25", "토더기준점", toorder=200000, coupang=100000),
        _row("2026-09-25", "기준없음점", coupang=None),
        _row("2026-09-25", "영원점", unified=0, toorder=0),
    ]
    assert _targets(rows) == {("2026-09-25", "토더기준점")}


def test_window_excludes_today_old_and_baemin():
    rows = [
        _row(TODAY, "오늘점", unified=10000),
        _row("2026-08-28", "옛날점", unified=10000),
        _row("2026-08-29", "경계점", unified=10000),
        _row("2026-09-20", "배민점", unified=10000, group="배달의민족"),
    ]
    assert _targets(rows) == {("2026-08-29", "경계점")}


def test_merge_date_ranges():
    dates = ["2026-09-01", "2026-09-02", "2026-09-05", "2026-09-09"]
    assert _merge_date_ranges(dates) == [
        ("2026-09-01", "2026-09-02"),
        ("2026-09-05", "2026-09-05"),
        ("2026-09-09", "2026-09-09"),
    ]
    assert _merge_date_ranges(["2026-09-13", "2026-09-14"]) == [("2026-09-13", "2026-09-14")]
    assert _merge_date_ranges(["2026-09-13", "2026-09-15"]) == [
        ("2026-09-13", "2026-09-13"),
        ("2026-09-15", "2026-09-15"),
    ]
    long = pd.date_range("2026-08-01", "2026-09-05").strftime("%Y-%m-%d").tolist()
    merged = _merge_date_ranges(long)
    assert merged[0] == ("2026-08-01", "2026-08-31")
    assert merged[1] == ("2026-09-01", "2026-09-05")


def test_export_writes_txt_and_header_only_when_empty(tmp_path):
    src = tmp_path / "collection_compare.parquet"
    pd.DataFrame([
        _row("2026-09-23", "부산대신점", unified=178000, coupang=135500),
        _row("2026-09-24", "부산대신점", unified=139100, coupang=121600),
        _row("2026-09-27", "김포풍무점", unified=709600),
    ]).to_parquet(src, index=False)
    msg = export_coupang_recollect_list(src, tmp_path, today=TODAY)
    lines = (tmp_path / "coupang_recollect_latest.txt").read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("# 쿠팡 누락수집")
    assert lines[1:] == [
        "2026-09-23~2026-09-24\t부산대신점\t과소 2일",
        "2026-09-27~2026-09-27\t김포풍무점\t누락 1일",
    ]
    assert (tmp_path / "coupang_recollect_20260928.txt").exists()
    assert "2구간 / 2매장" in msg

    pd.DataFrame([_row("2026-09-23", "정상점", unified=1000, coupang=1000)]).to_parquet(src, index=False)
    export_coupang_recollect_list(src, tmp_path, today=TODAY)
    lines = (tmp_path / "coupang_recollect_latest.txt").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1 and "ranges=0" in lines[0]
