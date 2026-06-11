import ast
from pathlib import Path

from modules.transform.pipelines.db import DB_Beamin_Macro_validate as macro_validate
from modules.transform.pipelines.db import DB_Beamin_combined as combined


DORIDANG = "\uB3C4\uB9AC\uB2F9"
NAHOLLO = "\uB098\uD640\uB85C"
SAMSUNG = "\uC0BC\uC1A1\uC810"
DAG_SOURCE = Path(__file__).resolve().parents[1] / "dags" / "db" / "DB_Beamin_Macro_Dags.py"


def test_filter_store_list_for_request_keeps_sister_brand_options():
    store_list = [
        {"store_id": "1", "brand": DORIDANG, "store": SAMSUNG},
        {"store_id": "2", "brand": NAHOLLO, "store": SAMSUNG},
        {"store_id": "3", "brand": DORIDANG, "store": "\uC5ED\uC0BC\uC810"},
    ]

    filtered = combined._filter_store_list_for_request(store_list, f"{DORIDANG} {SAMSUNG}")

    assert [(item["brand"], item["store"]) for item in filtered] == [
        (DORIDANG, SAMSUNG),
        (NAHOLLO, SAMSUNG),
    ]


def test_validate_toorder_orders_flags_missing_sister_brand(monkeypatch):
    monkeypatch.setattr(
        macro_validate,
        "_toorder_baemin_by_store",
        lambda target_date: {SAMSUNG: 243100},
    )

    baemin_calls = iter([{SAMSUNG: 204200}, {SAMSUNG: 204200}])
    monkeypatch.setattr(
        macro_validate,
        "_baemin_orders_by_store",
        lambda target_date: next(baemin_calls),
    )
    monkeypatch.setattr(macro_validate, "_delete_orders_for_stores", lambda *args, **kwargs: 0)
    monkeypatch.setattr(macro_validate, "_recollect_stores", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        macro_validate,
        "_inspect_brand_coverage",
        lambda target_date, store_names, expected_brands=None: {
            SAMSUNG: {
                "expected_brands": [NAHOLLO, DORIDANG],
                "existing_brands": [DORIDANG],
                "active_brands": [DORIDANG],
                "missing_brands": [NAHOLLO],
                "stale_brands": [],
                "issue_type": "missing_partition",
            }
        },
    )

    result = macro_validate.validate_toorder_orders(
        account_list=[{"account_id": "acct", "password": "pw"}],
        store_info_per_account=[
            {
                "account_id": "acct",
                "stores": [
                    {"brand": DORIDANG, "store": SAMSUNG, "store_id": "1"},
                    {"brand": NAHOLLO, "store": SAMSUNG, "store_id": "2"},
                ],
            }
        ],
        target_date="2026-06-09",
    )

    assert result["matched"] is False
    assert result["mismatched_stores"] == [SAMSUNG]
    assert result["missing_brand_stores"] == [SAMSUNG]
    assert result["store_results"][SAMSUNG]["brand_issue"] == "missing_partition"
    assert result["store_results"][SAMSUNG]["missing_brands"] == [NAHOLLO]


def test_manual_precheck_task_is_wired_before_load_accounts():
    source = DAG_SOURCE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    task_ids = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if getattr(getattr(node.func, "id", None), "strip", lambda: "")() != "PythonOperator":
            continue
        for kw in node.keywords:
            if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                task_ids.append(kw.value.value)

    assert "precheck_manual_baemin_orders" in task_ids
    assert "t_dash >> t0 >> t1 >> t2" in source
