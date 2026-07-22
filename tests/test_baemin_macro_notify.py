import ast
import html
import re
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pendulum


DAG_SOURCE = Path(__file__).resolve().parents[1] / "dags" / "db" / "DB_Beamin_Macro_Dags.py"


def _tree():
    return ast.parse(DAG_SOURCE.read_text(encoding="utf-8"))


def _load_helpers():
    tree = _tree()
    wanted = []
    for name in (
        "_safe_text",
        "_count_failed_items",
        "_task_duration_seconds",
        "_has_validation_issue",
        "_build_collection_notification",
    ):
        matches = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name]
        assert len(matches) == 1, f"expected exactly one definition for {name}, found {len(matches)}"
        wanted.append(matches[0])

    module_ast = ast.Module(body=wanted, type_ignores=[])
    ast.fix_missing_locations(module_ast)
    namespace = {
        "html": html,
        "pendulum": pendulum,
        "re": re,
        "Any": Any,
        "KST": pendulum.timezone("Asia/Seoul"),
        "dag_id": "DB_Beamin_Macro_Dags",
        "_NOTIFY_TASK_ID": "notify_collection_result",
        "_CORE_TASK_IDS": {"load_accounts", "collect_all", "collect_shop_change", "retry_failed"},
        "_VALIDATION_TASK_IDS": {"validate_orders", "validate_ad_funnel", "validate_toorder"},
        "logger": SimpleNamespace(info=lambda *args, **kwargs: None),
    }
    exec(compile(module_ast, str(DAG_SOURCE), "exec"), namespace)
    return namespace


class _FakeTaskInstance:
    def __init__(self, task_id: str, run_id: str = "manual__test", state: str = "success"):
        self.task_id = task_id
        self.state = state
        self.dag_id = "DB_Beamin_Macro_Dags"
        self.run_id = run_id
        self.try_number = 1
        self.log_url = f"http://example/{task_id}"
        self.start_date = pendulum.datetime(2026, 6, 6, 10, 0, tz="Asia/Seoul")
        self.end_date = self.start_date + timedelta(seconds=1)


class _FakeDagRun:
    def __init__(self, tasks, run_id: str = "manual__test", target_date: str = "2026-06-05"):
        self._tasks = tasks
        self.run_id = run_id
        self.conf = {"target_date": target_date}

    def get_task_instances(self):
        return self._tasks


class _FakeTI:
    def __init__(
        self,
        returns: dict,
        failed: dict | None = None,
        validation: list | None = None,
        residual_failed: dict | None = None,
    ):
        self._returns = returns
        self._failed = failed or {}
        self._validation = validation or []
        self._residual_failed = residual_failed
        self.run_id = "manual__test"
        self.execution_date = pendulum.datetime(2026, 6, 6, 10, 25, tz="Asia/Seoul")

    def xcom_pull(self, task_ids=None, key=None):
        if task_ids == "collect_all" and key == "failed":
            return self._failed
        if task_ids == "collect_all" and key == "validation":
            return self._validation
        if task_ids == "retry_failed" and key == "residual_failed":
            return self._residual_failed
        if key == "return_value":
            return self._returns.get(task_ids)
        return None


def _build_context(
    returns: dict,
    failed: dict | None = None,
    validation: list | None = None,
    residual_failed: dict | None = None,
):
    task_ids = (
        "load_accounts",
        "collect_all",
        "collect_shop_change",
        "retry_failed",
        "validate_orders",
        "validate_ad_funnel",
        "validate_toorder",
    )
    tasks = [_FakeTaskInstance(task_id) for task_id in task_ids]
    dag_run = _FakeDagRun(tasks)
    ti = _FakeTI(returns, failed=failed, validation=validation, residual_failed=residual_failed)
    return {"ti": ti, "dag_run": dag_run, "logical_date": ti.execution_date}


def test_notify_helpers_are_defined_once():
    function_names = [node.name for node in ast.walk(_tree()) if isinstance(node, ast.FunctionDef)]

    assert function_names.count("_build_collection_notification") == 1
    assert function_names.count("notify_collection_result") == 1


def test_notify_task_id_and_all_done_trigger_are_wired():
    source = DAG_SOURCE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    notify_ids = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "_NOTIFY_TASK_ID":
                notify_ids.append(ast.literal_eval(node.value))

    assert notify_ids == ["notify_collection_result"]
    assert "trigger_rule=TriggerRule.ALL_DONE" in source
    assert "t1 >> t2 >> t3 >> t4 >> t5" in source


def test_collection_task_logs_and_emails_but_does_not_send_telegram():
    source = DAG_SOURCE.read_text(encoding="utf-8")

    assert "send_telegram(" not in source
    assert "on_failure_callback_no_telegram" in source
    assert "_send_alert(subject=subject, body=body" in source
    assert "html_content=html_body" in source
    assert "[복구된 경고]" in source
    assert "[문제 로그]" in source
    assert "[정상 빈값 신호]" in source


def test_notify_treats_toorder_missing_and_recovered_warnings_as_success():
    namespace = _load_helpers()
    returns = {
        "load_accounts": "계정 3개",
        "collect_all": "성공 3/3 계정",
        "collect_shop_change": "성공 3/3 계정 / store_fail=0",
        "retry_failed": "재시도 완료: accounts=0 stores=3 orders=0 ads=0",
        "validate_orders": "orders 검증 총 6건(일치 6, 불일치 0, 미확인 0)",
        "validate_ad_funnel": "ad_funnel 빈값 검증: 총 3매장 / 빈값 0건 / 재수집 후 잔존 0건",
        "validate_toorder": "토더 교차검증[2026-06-05]: 비교 0개 매장 / 불일치 0개",
    }
    context = _build_context(
        returns,
        failed={"accounts": [], "stores": ["a", "b", "c"], "orders": [], "ads": []},
        residual_failed={"accounts": [], "stores": [], "orders": [], "ads": []},
    )

    def fake_extract(task_instance, max_lines=8):
        if task_instance.task_id == "collect_all":
            return [], 0, ["[2026-06-06] WARNING - dashboard not ready: 역삼점"]
        if task_instance.task_id == "validate_toorder":
            return [], 1, []
        return [], 0, []

    namespace["_extract_log_signals"] = fake_extract

    subject, body, html_body, should_email = namespace["_build_collection_notification"](context)

    assert "성공" in subject
    assert "부분성공" not in body
    assert "[문제 로그]" not in body
    assert "[복구된 경고]" in body
    assert "dashboard not ready" in body
    assert "원본 수집 실패 신호" in body
    assert "최종 잔여 실패 accounts=0, stores=0, orders=0, ads=0" in body
    assert should_email is False
    assert "복구된 경고" in html_body
    assert "원본 수집 실패 신호" in html_body
    assert "최종 잔여 실패" in html_body


def test_notify_keeps_partial_success_for_unrecovered_problem():
    namespace = _load_helpers()
    returns = {
        "load_accounts": "계정 3개",
        "collect_all": "성공 2/3 계정",
        "collect_shop_change": "성공 3/3 계정 / store_fail=0",
        "retry_failed": "재시도 완료: accounts=0 stores=1 orders=0 ads=0",
        "validate_orders": "orders 검증 총 6건(일치 5, 불일치 1, 미확인 0)",
        "validate_ad_funnel": "ad_funnel 빈값 검증: 총 3매장 / 빈값 0건 / 재수집 후 잔존 0건",
        "validate_toorder": "토더 교차검증[2026-06-05]: 비교 3개 매장 / 불일치 1개",
    }
    context = _build_context(
        returns,
        failed={"accounts": [], "stores": ["a"], "orders": [], "ads": []},
        residual_failed={"accounts": [], "stores": ["a"], "orders": [], "ads": []},
    )

    def fake_extract(task_instance, max_lines=8):
        if task_instance.task_id == "collect_all":
            return ["[2026-06-06] WARNING - invalid session"], 0, []
        return [], 0, []

    namespace["_extract_log_signals"] = fake_extract

    subject, body, html_body, should_email = namespace["_build_collection_notification"](context)

    assert "부분성공" in subject
    assert "[문제 로그]" in body
    assert "invalid session" in body
    assert should_email is True
    assert "문제 로그" in html_body
