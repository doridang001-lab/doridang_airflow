"""투오더 종합보고서 수집이 전멸했을 때 태스크가 실패하는지.

2026-09-05~09-10: 투오더 날짜 입력 선택자가 깨져 매일 0/N 으로 끝났는데도
crawl_reports가 success로 마감돼 6일치 결측을 아무도 모른 채 지나갔다.
"""

import pytest
from airflow.exceptions import AirflowException

from dags.sales import Sales_ToOrderSalesReport_Crawl_Dags as crawl_dag


class _Ti:
    def __init__(self, date_list):
        self._date_list = date_list
        self.pushed = {}

    def xcom_pull(self, task_ids=None, key=None):
        return self._date_list

    def xcom_push(self, key=None, value=None):
        self.pushed[key] = value


def test_crawl_reports_raises_when_every_date_fails(monkeypatch):
    dates = ["2026-09-08", "2026-09-09"]
    monkeypatch.setattr(
        crawl_dag,
        "run_crawling_date_range",
        lambda **_kwargs: [
            {"date": d, "success": False, "file": None, "error": "날짜 설정 실패"}
            for d in dates
        ],
    )
    ti = _Ti(dates)

    with pytest.raises(AirflowException) as exc:
        crawl_dag.crawl_reports(ti=ti)

    assert "전부 수집 실패" in str(exc.value)
    assert "날짜 설정 실패" in str(exc.value)


def test_crawl_reports_succeeds_on_partial_download(monkeypatch):
    """일부라도 받았으면 다음 lookback이 나머지를 다시 집으므로 실패로 보지 않는다."""
    dates = ["2026-09-08", "2026-09-09"]
    monkeypatch.setattr(
        crawl_dag,
        "run_crawling_date_range",
        lambda **_kwargs: [
            {"date": dates[0], "success": True, "file": "/tmp/a.xlsx", "error": None},
            {"date": dates[1], "success": False, "file": None, "error": "날짜 설정 실패"},
        ],
    )
    ti = _Ti(dates)

    result = crawl_dag.crawl_reports(ti=ti)

    assert "1건" in result
    assert ti.pushed["downloaded_files"] == ["/tmp/a.xlsx"]


def test_crawl_reports_noop_when_nothing_missing(monkeypatch):
    monkeypatch.setattr(crawl_dag, "run_crawling_date_range", lambda **_kwargs: [])
    ti = _Ti([])

    result = crawl_dag.crawl_reports(ti=ti)

    assert "0건" in result
