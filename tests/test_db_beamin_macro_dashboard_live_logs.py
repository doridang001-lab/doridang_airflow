from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.dashboard.db_beamin_macro_dashboard import SnapshotBuilder, StoreProgressParser  # noqa: E402


class FakeRepository:
    def __init__(self, active_run=None):
        self.active_run = active_run

    def get_latest_active_run(self):
        return self.active_run

    def get_latest_completed_run(self):
        return None

    def get_recent_success_runs(self, limit=7):
        return []


def _run(run_id: str) -> dict:
    start = datetime(2026, 6, 6, 6, 18, 25, tzinfo=UTC)
    return {
        "dag_id": "DB_Beamin_Macro_Dags",
        "run_id": run_id,
        "state": "running",
        "execution_date": start,
        "start_date": start,
        "end_date": None,
        "tasks": [
            {"task_id": "load_accounts", "state": "success", "start_date": start, "end_date": start + timedelta(seconds=1), "duration": 1.0},
            {"task_id": "collect_all", "state": "running", "start_date": start + timedelta(seconds=3), "end_date": None, "duration": None},
            {"task_id": "collect_shop_change", "state": "none", "start_date": None, "end_date": None, "duration": None},
        ],
        "xcom": {
            "load_accounts": {
                "account_list": [
                    {"account_id": "acct01", "store_name": "store-a", "brand": "도리당"},
                ]
            }
        },
    }


def _write_log(root: Path, run_id: str, task_id: str, lines: list[str]) -> None:
    log_path = root / "dag_id=DB_Beamin_Macro_Dags" / f"run_id={run_id}" / f"task_id={task_id}" / "attempt=1.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("\n".join(lines), encoding="utf-8")


def test_collect_all_account_activity_is_visible_before_store_section(tmp_path: Path):
    run = _run("manual__account_activity")
    _write_log(
        tmp_path,
        "manual__account_activity",
        "collect_all",
        [
            "[2026-06-06T06:18:46.707+0000] {logging_mixin.py:190} INFO - [acct01] 브라우저 실행 시도 (headless fallback)",
            "[2026-06-06T06:18:47.024+0000] {logging_mixin.py:190} INFO - [acct01] 로그인 시도",
        ],
    )

    builder = SnapshotBuilder(FakeRepository(active_run=run), parser=StoreProgressParser(logs_root=tmp_path))
    snapshot = builder.build(now=datetime(2026, 6, 6, 6, 19, tzinfo=UTC))

    assert snapshot["live_logs"]
    assert snapshot["overview"]["current_store"] == "도리당/store-a"
    assert snapshot["overview"]["progress_count"] == 1
    assert snapshot["stores"][0]["now_status"] == "수집중"
    assert snapshot["stores"][0]["current_section"] == "브라우저 준비"


def test_rendered_html_contains_note_column() -> None:
    from modules.transform.dashboard.db_beamin_macro_dashboard import render_dashboard_html

    html_text = render_dashboard_html(
        dag_id="DB_Beamin_Macro_Dags",
        overview={"overall_status": "RUNNING", "total_stores": 1, "completed_count": 0, "success_count": 0, "warning_count": 0, "failure_count": 0, "progress_count": 1, "elapsed_text": "00:10", "avg_duration_text": "01:00", "latest_completed_at": None, "current_store": "도리당/store-a", "expected_finished_at": "2026-06-06T07:19:00+00:00"},
        stores=[{"seq": 1, "store_name": "store-a", "account_id": "acct01", "brand": "도리당", "orders_status": "대기", "marketing_status": "수집중", "now_status": "수집중", "woori_status": "대기", "ad_status": "대기", "change_status": "대기", "elapsed_text": "00:10", "note": "-", "selected": True, "store_key": "도리당/store-a", "current_section": "브라우저 준비", "retry_status": "-"}],
        live_logs=[],
        generated_at=datetime(2026, 6, 6, 6, 19, tzinfo=UTC),
    )

    assert "비고" in html_text
    assert "재수집" in html_text
    assert "예상 완료" in html_text
