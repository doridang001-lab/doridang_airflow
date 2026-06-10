from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.dashboard.db_beamin_macro_dashboard import (  # noqa: E402
    DashboardService,
    SnapshotBuilder,
    SnapshotStore,
    StoreProgressParser,
    render_dashboard_html,
)


class FakeRepository:
    def __init__(self, active_run=None, completed_run=None, recent_runs=None):
        self.active_run = active_run
        self.completed_run = completed_run
        self.recent_runs = recent_runs or []

    def get_latest_active_run(self):
        return self.active_run

    def get_latest_completed_run(self):
        return self.completed_run

    def get_recent_success_runs(self, limit=7):
        return self.recent_runs[:limit]


def _run(run_id: str, state: str, task_states: dict[str, str], *, start_offset=0, duration=600, xcom=None):
    start = datetime(2026, 6, 5, 10, 0, tzinfo=UTC) + timedelta(seconds=start_offset)
    end = None if state in {"running", "queued"} else start + timedelta(seconds=duration)
    tasks = []
    for idx, (task_id, task_state) in enumerate(task_states.items()):
        task_start = start + timedelta(seconds=idx * 3)
        task_end = None if task_state == "running" else task_start + timedelta(seconds=40)
        tasks.append(
            {
                "task_id": task_id,
                "state": task_state,
                "start_date": task_start,
                "end_date": task_end,
                "duration": None if task_end is None else 40.0,
            }
        )
    return {
        "dag_id": "DB_Beamin_Macro_Dags",
        "run_id": run_id,
        "state": state,
        "execution_date": start,
        "start_date": start,
        "end_date": end,
        "tasks": tasks,
        "xcom": xcom or {},
    }


def _write_log(root: Path, run_id: str, task_id: str, lines: list[str]) -> None:
    log_path = root / "dag_id=DB_Beamin_Macro_Dags" / f"run_id={run_id}" / f"task_id={task_id}" / "attempt=1.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("\n".join(lines), encoding="utf-8")


def test_snapshot_builder_creates_store_rows_from_logs(tmp_path: Path):
    run = _run(
        "manual__store_rows",
        "running",
        {
            "load_accounts": "success",
            "collect_all": "running",
            "collect_shop_change": "none",
        },
        xcom={
            "load_accounts": {
                "account_list": [
                    {"account_id": "jka905", "store_name": "도리당 교대점"},
                    {"account_id": "schoolfoodseoul", "store_name": "도리당 상도역점"},
                ]
            }
        },
    )
    _write_log(
        tmp_path,
        "manual__store_rows",
        "collect_all",
        [
            "20:27:49 INFO - === 주문이력 수집 [jka905 / 교대점] ===",
            "20:28:17 INFO - 수집 완료(정상): brand=도리당 store=교대점 -> x.csv (2건)",
            "20:28:18 INFO - === 광고 funnel 수집 [jka905 / 교대점] ===",
            "20:28:55 INFO - 수집 완료: brand=도리당 store=교대점 -> y.csv",
            "20:30:50 INFO - === 주문이력 수집 [schoolfoodseoul / 상도역점] ===",
            "20:40:54 INFO - 수집 완료(정상): brand=도리당 store=상도역점 -> z.csv (4건)",
        ],
    )

    builder = SnapshotBuilder(FakeRepository(active_run=run), parser=StoreProgressParser(logs_root=tmp_path))
    snapshot = builder.build(now=datetime(2026, 6, 5, 10, 30, tzinfo=UTC))

    assert snapshot["overall_status"] == "RUNNING"
    assert snapshot["total_stores"] == 2
    assert snapshot["live_logs"]


def test_snapshot_store_persists_html_and_split_json_files(tmp_path: Path):
    snapshot = {
        "generated_at": "2026-06-05T02:00:00+00:00",
        "dag_id": "DB_Beamin_Macro_Dags",
        "overview": {"overall_status": "SUCCESS"},
        "stores": [{"store_name": "교대점"}],
        "live_logs": [{"store_name": "교대점", "message": "ok"}],
        "html": "<html><body>ok</body></html>",
    }
    store = SnapshotStore(root=tmp_path)
    store.persist(snapshot)

    assert (tmp_path / "db_beamin_macro_dashboard.html").exists()
    assert (tmp_path / "db_beamin_macro_snapshot.json").exists()
    assert (tmp_path / "db_beamin_macro_store_progress.json").exists()
    assert (tmp_path / "db_beamin_macro_live_log.json").exists()
    payload = json.loads((tmp_path / "db_beamin_macro_snapshot.json").read_text(encoding="utf-8"))
    assert "html" not in payload


def test_render_dashboard_html_contains_store_dashboard_labels():
    html_text = render_dashboard_html(
        dag_id="DB_Beamin_Macro_Dags",
        overview={
            "overall_status": "SUCCESS",
            "current_store": "교대점",
            "last_completed_store": "도리당/교대점",
            "total_stores": 1,
            "completed_count": 1,
            "success_count": 1,
            "partial_count": 0,
            "warning_count": 0,
            "failure_count": 0,
            "progress_count": 0,
            "elapsed_text": "10:00",
            "avg_duration_text": "09:20",
            "latest_completed_at": "2026-06-05T02:00:00+00:00",
        },
        stores=[
            {
                "seq": 1,
                "store_name": "교대점",
                "account_id": "jka905",
                "orders_status": "완료",
                "marketing_status": "완료",
                "store_state": "완료",
                "elapsed_text": "10:00",
                "note": "-",
                "selected": True,
                "store_key": "도리당/교대점",
            }
        ],
        live_logs=[
            {
                "time": "20:20:20",
                "store_name": "교대점",
                "store_key": "도리당/교대점",
                "level": "INFO",
                "message": "완료",
            }
        ],
        generated_at=datetime(2026, 6, 5, 2, 0, tzinfo=UTC),
    )

    assert "실시간 연결 준비중" in html_text
    assert "변경이력" in html_text
    assert "우가클" in html_text
    assert "부분완료" in html_text
    assert "selected-store-card" in html_text
    assert "store-brand-tag--doridang" in html_text
    assert "brand-doridang" in html_text
    assert "function logsForView()" in html_text
    assert "selectedStore.endsWith" in html_text
    assert 'window.location.protocol === "file:"' in html_text
    assert "window.location.reload();" in html_text
    assert 'new EventSource(`/api/db-beamin-macro/events?since=${snapshot.snapshot_id || 0}`)' in html_text
    assert "/api/db-beamin-macro/snapshot" in html_text
    assert "window.setInterval" in html_text


def test_dashboard_service_wait_for_snapshot_returns_newer_snapshot():
    class SequencedBuilder:
        def __init__(self):
            self.counter = 0

        def build(self, *, now=None):
            self.counter += 1
            return {
                "generated_at": "2026-06-05T02:00:00+00:00",
                "dag_id": "DB_Beamin_Macro_Dags",
                "overview": {
                    "overall_status": "RUNNING",
                    "current_store": "도리당/교대점",
                    "last_completed_store": "-",
                    "total_stores": 1,
                    "completed_count": 0,
                    "success_count": 0,
                    "partial_count": 0,
                    "warning_count": 0,
                    "failure_count": 0,
                    "progress_count": 1,
                    "elapsed_text": "00:10",
                    "avg_duration_text": "00:10",
                    "latest_completed_at": None,
                    "expected_finished_at": None,
                },
                "stores": [
                    {
                        "seq": 1,
                        "store_name": "교대점",
                        "account_id": "jka905",
                        "brand": "도리당",
                        "orders_status": "수집중",
                        "marketing_status": "대기",
                        "now_status": "대기",
                        "woori_status": "대기",
                        "ad_status": "대기",
                        "change_status": "대기",
                        "elapsed_text": "00:10",
                        "note": "-",
                        "selected": True,
                        "retry_status": "",
                        "current_section": "주문서",
                        "store_key": "도리당/교대점",
                        "store_state": "수집중",
                    }
                ],
                "live_logs": [],
            }

    service = DashboardService(builder=SequencedBuilder(), store=SnapshotStore(root=Path.cwd() / "tmp_test_dashboard"))
    first = service.refresh_once()
    next_snapshot = service.wait_for_snapshot(int(first["snapshot_id"]), timeout=0.01)
    assert next_snapshot is None

    second = service.refresh_once()
    waited = service.wait_for_snapshot(int(first["snapshot_id"]), timeout=0.01)
    assert waited is not None
    assert waited["snapshot_id"] == second["snapshot_id"]


def test_parse_change_label_handles_brand_store_storeid():
    parser = StoreProgressParser(logs_root=Path("."))

    store_name, account_id, brand = parser._parse_change_label("나홀로/송파삼전점/12345")

    assert store_name == "송파삼전점"
    assert account_id is None
    assert brand == "나홀로"


def test_snapshot_builder_parses_collect_shop_change_success_log(tmp_path: Path):
    run = _run(
        "manual__shop_change_success",
        "success",
        {
            "load_accounts": "success",
            "collect_all": "success",
            "collect_shop_change": "success",
        },
    )
    _write_log(
        tmp_path,
        "manual__shop_change_success",
        "collect_shop_change",
        [
            "20:27:49 INFO - 변경이력 수집 시작: 나홀로/송파삼전점/12345",
            "20:28:17 INFO - 저장 완료: 나홀로/송파삼전점/12345 -> shop_change.csv (2건)",
            "20:28:30 INFO - store 변경이력 수집 완료: 나홀로/송파삼전점/12345",
        ],
    )

    builder = SnapshotBuilder(FakeRepository(completed_run=run), parser=StoreProgressParser(logs_root=tmp_path))
    snapshot = builder.build(now=datetime(2026, 6, 5, 10, 30, tzinfo=UTC))

    stores = snapshot["stores"]
    assert len(stores) == 1
    assert stores[0]["brand"] == "나홀로"
    assert stores[0]["store_name"] == "송파삼전점"
    assert stores[0]["change_status"] == "완료"
    assert stores[0]["marketing_status"] == "완료"


def test_snapshot_builder_parses_collect_shop_change_empty_log(tmp_path: Path):
    run = _run(
        "manual__shop_change_empty",
        "success",
        {
            "load_accounts": "success",
            "collect_all": "success",
            "collect_shop_change": "success",
        },
    )
    _write_log(
        tmp_path,
        "manual__shop_change_empty",
        "collect_shop_change",
        [
            "20:27:49 INFO - 변경이력 수집 시작: 나홀로/송파삼전점/12345",
            "20:28:17 INFO - 정상 빈값 신호: 나홀로/송파삼전점/12345",
        ],
    )

    builder = SnapshotBuilder(FakeRepository(completed_run=run), parser=StoreProgressParser(logs_root=tmp_path))
    snapshot = builder.build(now=datetime(2026, 6, 5, 10, 30, tzinfo=UTC))

    stores = snapshot["stores"]
    assert len(stores) == 1
    assert stores[0]["brand"] == "나홀로"
    assert stores[0]["store_name"] == "송파삼전점"
    assert stores[0]["change_status"] == "건너뜀"
    assert stores[0]["marketing_status"] == "건너뜀"
