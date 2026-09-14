"""DB_CollectionFreshness — 수집 결손·급감 감시.

2026-09-11: 투오더 6일 결측, 배민·OKPOS 1일 결손, 쿠팡 절반 급감이
아무 알림 없이 지나갔다. 이 테스트는 그 네 가지 상황을 그대로 재현한다.
"""

from collections import Counter

import pandas as pd
import pendulum
import pytest

from modules.transform.pipelines.db import DB_CollectionFreshness as fresh

NOW = pendulum.datetime(2026, 9, 11, 9, 40, tz="Asia/Seoul")  # 감시 창: 09-04 ~ 09-10


def _source(name, counter):
    return fresh.FreshnessSource(name, name, counter)


# ------------------------------------------------------------------
# 판정 로직
# ------------------------------------------------------------------

def test_missing_day_is_flagged():
    counts = Counter({"2026-09-09": 5288, "2026-09-10": 0})
    window = ["2026-09-09", "2026-09-10"]

    issues = fresh.evaluate_source(_source("배민", None), counts, window)

    assert [(i.date, i.kind) for i in issues] == [("2026-09-10", "missing")]


def test_drop_vs_same_weekday_last_week_is_flagged():
    counts = Counter({"2026-09-03": 3787, "2026-09-10": 1500})  # 40% — 50% 미만
    window = ["2026-09-10"]

    issues = fresh.evaluate_source(_source("쿠팡", None), counts, window)

    assert len(issues) == 1
    assert issues[0].kind == "drop"
    assert issues[0].baseline == 3787
    assert "급감" in issues[0].describe()


def test_moderate_drop_within_threshold_is_not_flagged():
    """2026-09-10 실측(3539 -> 2001, 56.5%)처럼 50% 문턱을 넘지 않으면 조용하다."""
    counts = Counter({"2026-09-03": 3539, "2026-09-10": 2001})

    assert fresh.evaluate_source(_source("쿠팡", None), counts, ["2026-09-10"]) == []


def test_normal_day_is_quiet():
    counts = Counter({"2026-09-03": 3700, "2026-09-10": 3600})

    assert fresh.evaluate_source(_source("쿠팡", None), counts, ["2026-09-10"]) == []


def test_drop_without_baseline_is_not_flagged():
    """전주 데이터가 없으면 급감 판정을 하지 않는다 (신규 소스·첫 주)."""
    counts = Counter({"2026-09-10": 3})

    assert fresh.evaluate_source(_source("x", None), counts, ["2026-09-10"]) == []


def test_collect_freshness_scans_extra_week_for_baseline():
    seen = {}

    def counter(dates):
        seen["dates"] = dates
        return Counter({d: 10 for d in dates})

    report = fresh.collect_freshness(window_days=7, now=NOW, sources=[_source("s", counter)])

    assert report.window == [f"2026-09-{d:02d}" for d in range(4, 11)]
    assert len(seen["dates"]) == 14
    assert seen["dates"][0] == "2026-08-28"
    assert not report.has_issues


def test_format_report_lists_issues():
    report = fresh.collect_freshness(
        window_days=2,
        now=NOW,
        sources=[_source("투오더", lambda dates: Counter({"2026-09-09": 60}))],
    )

    body = fresh.format_report(report, labels={"투오더": "투오더 일매출"})

    assert "투오더 일매출: 09-09:60  09-10:0" in body
    assert "이상 1건" in body
    assert "2026-09-10: 0건 (결손)" in body


# ------------------------------------------------------------------
# 실제 파일 카운터 (tmp_path 파티션)
# ------------------------------------------------------------------

def test_count_baemin_reads_only_order_files_and_skips_no_data_marker(tmp_path):
    part = tmp_path / "brand=도리당" / "store=강동점" / "ym=2026-09"
    part.mkdir(parents=True)
    pd.DataFrame({"주문시각": [
        "2026. 09. 09. (수) 오후 09:33:23",
        "2026. 09. 09. (수) 오후 09:40:00",
        "2026. 09. 10. (목) 오전 11:00:00",
    ]}).to_parquet(part / "orders_2026-09.parquet")
    # _no_data 마커: 주문 컬럼이 없다
    pd.DataFrame({"target_date": ["2026-09-08"], "status": ["no_data"]}).to_parquet(
        part / "orders_2026-09_no_data.parquet"
    )

    counts = fresh.count_baemin(["2026-09-09", "2026-09-10"], root=tmp_path)

    assert counts == Counter({"2026-09-09": 2, "2026-09-10": 1})


def test_count_coupang_parses_dotted_dates(tmp_path):
    part = tmp_path / "brand=도리당" / "store=송파점" / "ym=2026-09"
    part.mkdir(parents=True)
    pd.DataFrame({"order_date": ["2026.09.10", "2026.09.10", "2026.09.11"]}).to_parquet(
        part / "orders_2026-09.parquet"
    )

    counts = fresh.count_coupang(["2026-09-10"], root=tmp_path)

    assert counts["2026-09-10"] == 2


def test_count_posfeed_and_okpos_csv(tmp_path):
    pf = tmp_path / "pf" / "brand=나홀로" / "store=가락점" / "ym=2026-09"
    pf.mkdir(parents=True)
    pd.DataFrame({"주문등록 시각": ["2026-09-10 10:00:00", "2026-09-10 11:00:00"], "x": [1, 2]}).to_csv(
        pf / "posfeed_orders.csv", index=False
    )
    ok = tmp_path / "ok" / "brand=도리당" / "store=동두천지행점" / "ym=2026-09"
    ok.mkdir(parents=True)
    pd.DataFrame({"sale_date": ["2026-09-09", "2026-09-10"], "총매출액": [0, 0]}).to_csv(
        ok / "okpos_daily.csv", index=False
    )

    assert fresh.count_posfeed(["2026-09-10"], root=tmp_path / "pf")["2026-09-10"] == 2
    assert fresh.count_okpos(["2026-09-10"], root=tmp_path / "ok") == Counter({"2026-09-09": 1, "2026-09-10": 1})


def test_count_toorder_daily_uses_file_presence(tmp_path):
    (tmp_path / "toorder_daily_sales_20260904.csv").write_text("a,b\n1,2\n3,4\n", encoding="utf-8-sig")

    counts = fresh.count_toorder_daily(["2026-09-04", "2026-09-05"], root=tmp_path)

    assert counts == Counter({"2026-09-04": 2})


def test_missing_root_does_not_raise(tmp_path):
    assert fresh.count_baemin(["2026-09-10"], root=tmp_path / "nope") == Counter()
    assert fresh.count_toorder_daily(["2026-09-10"], root=tmp_path / "nope") == Counter()


# ------------------------------------------------------------------
# 태스크 진입점
# ------------------------------------------------------------------

class _Run:
    def __init__(self, conf):
        self.conf = conf


def test_task_alerts_on_issue_and_respects_dry_run(monkeypatch):
    sent = []
    monkeypatch.setattr(fresh, "send_telegram", lambda text: sent.append(text))
    monkeypatch.setattr(
        fresh,
        "collect_freshness",
        lambda **kw: fresh.FreshnessReport(
            window=["2026-09-10"],
            counts={"toorder_daily": Counter()},
            issues=[fresh.FreshnessIssue("투오더 일매출", "2026-09-10", "missing", 0)],
        ),
    )

    body = fresh.check_collection_freshness(dag_run=_Run({}))
    assert "결손" in body
    assert len(sent) == 1

    fresh.check_collection_freshness(dag_run=_Run({"dry_run": True}))
    assert len(sent) == 1  # dry_run은 알림을 보내지 않는다


def test_task_quiet_when_no_issue(monkeypatch):
    sent = []
    monkeypatch.setattr(fresh, "send_telegram", lambda text: sent.append(text))
    monkeypatch.setattr(
        fresh,
        "collect_freshness",
        lambda **kw: fresh.FreshnessReport(window=["2026-09-10"], counts={"baemin": Counter({"2026-09-10": 5})}),
    )

    body = fresh.check_collection_freshness(dag_run=_Run({}))

    assert "이상 없음" in body
    assert sent == []
