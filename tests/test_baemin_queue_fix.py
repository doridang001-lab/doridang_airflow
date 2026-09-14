from unittest.mock import MagicMock
import os
from pathlib import Path
import pytest
if os.name == "nt":
    os.environ.setdefault("AIRFLOW_HOME", str(Path(__file__).resolve().parents[1] / ".tmp" / "airflow-test"))
from modules.transform.pipelines.db import DB_Beamin_04_orders as orders
from modules.transform.utility import workload


@pytest.mark.parametrize("response", [
    {"success": False, "error": "가게 필터 적용 실패"},
    {"success": False, "partial": True, "error": "첫 페이지 펼침 실패", "rows": 0},
    {"success": False, "error": "수집된 주문 없음", "rows": 0},
    {"success": True, "rows": 0}, {},
])
def test_extension_failure_never_becomes_zero_orders(monkeypatch, response):
    monkeypatch.setattr(orders, "_inject_baemin_extension_collector", lambda _: None)
    monkeypatch.setattr(orders, "_read_total_summary", lambda _: {"count": 9, "amount": 238700})
    driver = MagicMock()
    driver.execute_async_script.return_value = response
    with pytest.raises(orders.ExtensionCollectionFailed):
        orders._collect_all_pages_with_extension(driver, {"store": "테스트"})


def test_confirmed_zero_is_compatible_with_legacy_extension(monkeypatch):
    monkeypatch.setattr(orders, "_inject_baemin_extension_collector", lambda _: None)
    monkeypatch.setattr(orders, "_read_total_summary", lambda _: {"count": 0, "amount": 0})
    driver = MagicMock()
    driver.execute_async_script.return_value = {"success": False, "error": "수집된 주문 없음", "rows": 0}
    assert orders._collect_all_pages_with_extension(driver, {}) == []


@pytest.mark.parametrize("account_mode", [False, True])
def test_existing_csv_does_not_create_no_data_marker(monkeypatch, account_mode):
    monkeypatch.setattr(orders, "_open_orders_history", lambda *_: None)
    monkeypatch.setattr(orders, "_wait_for_orders_page_shell", lambda _: True)
    monkeypatch.setattr(orders, "_select_order_store", lambda *_: True)
    monkeypatch.setattr(orders, "_ensure_orders_target_date", lambda *_: True)
    monkeypatch.setattr(orders, "_collect_with_retry_on_mismatch", lambda *a, **kw: (
        [], {"matched": True, "amount_source": "csv_skip"}))
    zero = MagicMock(side_effect=AssertionError("정상 파일을 무매출로 처리함"))
    monkeypatch.setattr(orders, "_handle_zero_orders", zero)
    store = {"store_id": "1", "brand": "도리당", "store": "테스트"}
    if account_mode:
        monkeypatch.setattr(orders, "launch_browser", lambda _: MagicMock())
        monkeypatch.setattr(orders, "login_baemin", lambda *_: True)
        monkeypatch.setattr(orders.time, "sleep", lambda _: None)
        assert orders.collect_orders_for_account("test", "test", [store], "2026-09-08")["failed"] == []
    else:
        assert orders.collect_orders_for_driver(MagicMock(), store, "2026-09-08")["ok"]
    zero.assert_not_called()


def test_structural_extension_error_does_not_fallback(monkeypatch):
    monkeypatch.setenv("BAEMIN_ORDERS_COLLECTOR", "extension_fallback")
    monkeypatch.setattr(orders, "_collect_all_pages_with_extension",
                        MagicMock(side_effect=orders.ExtensionCollectionFailed("상세 추출 실패")))
    fallback = MagicMock()
    monkeypatch.setattr(orders, "_collect_all_pages", fallback)
    with pytest.raises(orders.ExtensionCollectionFailed):
        orders._collect_all_pages_for_mode(MagicMock(), {})
    fallback.assert_not_called()


def test_only_recovered_lookback_stores_are_removed(monkeypatch):
    from modules.transform.pipelines.db import DB_Beamin_Macro_validate as validate
    monkeypatch.setattr(validate, "_baemin_orders_by_store", lambda _: {"완료": 100, "원천없음": 100})
    monkeypatch.setattr(validate, "_toorder_baemin_by_store", lambda _: {"완료": 100, "누락": 100})
    conf = {"target_date": "2026-09-08", "reason": "baemin_orders_store_lookback_gap",
            "stores": ["완료", "누락", "원천없음"]}
    assert workload.refresh_lookback_conf(conf)["stores"] == ["누락", "원천없음"]
    assert conf["stores"] == ["완료", "누락", "원천없음"]
    assert workload.refresh_lookback_conf({**conf, "stores": ["완료"]}) is None
    manual = {**conf, "reason": "manual"}
    assert workload.refresh_lookback_conf(manual) == manual


def test_background_sensor_preserves_failed_batch_cleanup_and_serializes():
    from airflow.serialization.serialized_objects import SerializedDAG
    from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
    from airflow.sensors.python import PythonSensor
    from airflow.utils.trigger_rule import TriggerRule
    dag = workload.build_background_dag("dags.db.DB_Beamin_Macro_Dags", workload.BACKGROUND_COLLECT_DAG)
    gate = dag.get_task("wait_resource__retry_failed")
    assert isinstance(gate, PythonSensor)
    assert gate.trigger_rule == TriggerRule.ALL_DONE
    assert dag.get_task("retry_failed").trigger_rule == TriggerRule.ALL_SUCCESS
    assert dag.get_task("retry_failed").upstream_task_ids == {gate.task_id}
    assert any(isinstance(dep, ReadyToRescheduleDep) for dep in gate.deps)
    serialized = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
    restored = serialized.get_task(gate.task_id)
    assert any(isinstance(dep, ReadyToRescheduleDep) for dep in restored.deps)
    assert gate.mode == "reschedule" and gate.poke_interval == 60
    assert not any(t.task_id.startswith("wait_resource__notify") for t in dag.tasks)
    assert all(t.queue == "history" for t in dag.tasks)


@pytest.mark.parametrize("seconds,passed", [(60, False), (-1, True)])
def test_real_airflow_dependency_enforces_reschedule_time(seconds, passed):
    from datetime import timedelta
    from airflow.models import TaskInstance
    from airflow.ti_deps.dep_context import DepContext
    from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
    from airflow.utils import timezone
    dag = workload.build_background_dag("dags.db.DB_Beamin_Macro_Dags", workload.BACKGROUND_COLLECT_DAG)
    ti = TaskInstance(dag.get_task("wait_resource__collect_batch_1"), run_id="test")
    ti.state = "up_for_reschedule"
    db = MagicMock()
    db.scalar.return_value = timezone.utcnow() + timedelta(seconds=seconds)
    statuses = list(ReadyToRescheduleDep().get_dep_statuses(ti, session=db, dep_context=DepContext()))
    assert statuses and all(status.passed is passed for status in statuses)


def test_new_sensor_does_not_block_already_completed_task(monkeypatch):
    from contextlib import contextmanager
    from airflow.utils import session as sessions
    from types import SimpleNamespace
    db = MagicMock()
    db.query.return_value.filter_by.return_value.scalar.return_value = "success"
    @contextmanager
    def session():
        yield db
    monkeypatch.setattr(sessions, "create_session", session)
    monkeypatch.setattr(workload, "background_ready", MagicMock(side_effect=AssertionError("완료 작업 대기")))
    assert workload.history_task_ready("collect_batch_1", dag=SimpleNamespace(dag_id="test"), run_id="test")
