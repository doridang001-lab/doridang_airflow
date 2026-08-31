import ast
from pathlib import Path


def _python_operator_kwargs(path: Path, task_id: str) -> dict[str, ast.AST]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func_name = getattr(node.func, "id", "") or getattr(node.func, "attr", "")
        if func_name != "PythonOperator":
            continue
        kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg}
        task_value = kwargs.get("task_id")
        if isinstance(task_value, ast.Constant) and task_value.value == task_id:
            return kwargs
    raise AssertionError(f"PythonOperator task_id={task_id} not found in {path}")


def _assert_pool_kw(path: str, task_id: str):
    kwargs = _python_operator_kwargs(Path(path), task_id)
    pool_value = kwargs.get("pool")
    assert isinstance(pool_value, ast.Name)
    assert pool_value.id == "TOORDER_SELENIUM_POOL"


def test_toorder_daily_store_collect_uses_serial_selenium_pool():
    _assert_pool_kw("dags/db/DB_ToOrder_Daily_Store_Dags.py", "collect_toorder_daily_store")


def test_sales_alert_collect_uses_serial_selenium_pool():
    _assert_pool_kw(
        "dags/sales/DB_Sales_Alert_01_Score_AI_Daily_Collection_Dags.py",
        "collect_ai_daily_sales_report",
    )
