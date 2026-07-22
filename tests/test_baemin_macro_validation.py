import ast
from pathlib import Path

from modules.transform.pipelines.db import DB_Beamin_Macro_validate as macro_validate
from modules.transform.pipelines.db import DB_Beamin_combined as combined
from modules.transform.utility.store_normalize import lookup_store_key


DORIDANG = "\uB3C4\uB9AC\uB2F9"
NAHOLLO = "\uB098\uD640\uB85C"
SAMSUNG = "\uC0BC\uC1A1\uC810"
UPLOAD_DAG_SOURCE = Path(__file__).resolve().parents[1] / "dags" / "db" / "DB_Beamin_Macro_Upload_Dags.py"


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


def test_filter_store_list_for_request_accepts_branch_suffix():
    store_list = [
        {"store_id": "1", "brand": DORIDANG, "store": "부산대신점"},
        {"store_id": "2", "brand": NAHOLLO, "store": "부산대신점"},
        {"store_id": "3", "brand": DORIDANG, "store": "\uc5ed\uc0bc\uc810"},
    ]

    filtered = combined._filter_store_list_for_request(store_list, f"{NAHOLLO} 1인 곱도리탕 대신점")

    assert len(filtered) == 2
    assert {(item["brand"], item["store"]) for item in filtered} == {
        (DORIDANG, "부산대신점"),
        (NAHOLLO, "부산대신점"),
    }


def test_parse_store_option_normalizes_store_name():
    parsed = combined._parse_store_option(
        {
            "store_id": "x",
            "text": "[음식배달] 나홀로 1인 곱도리탕 대신점",
        }
    )
    assert parsed == {"store_id": "x", "brand": NAHOLLO, "store": "부산대신점"}


def test_parse_store_option_normalizes_insangjang_point():
    parsed = combined._parse_store_option(
        {
            "store_id": "y",
            "text": "[음식배달] 나홀로 1인 곱도리탕 간석중앙점",
        }
    )
    assert parsed == {"store_id": "y", "brand": NAHOLLO, "store": "인천간석중앙점"}


def test_lookup_store_key_uses_store_name_map_when_available():
    assert lookup_store_key(NAHOLLO, "대신점") == "부산대신점"
    assert lookup_store_key(NAHOLLO, "간석중앙점") == "인천간석중앙점"
    assert lookup_store_key("도리당", "부산대신점") == "부산대신점"
    assert lookup_store_key("도리당", "역삼점") == "역삼점"


def test_store_option_dedup_keeps_same_store_id_with_different_text(monkeypatch):
    raw_options = [
        {"store_id": "1", "text": "brand-a same-store"},
        {"store_id": "1", "text": "brand-b same-store"},
        {"store_id": "1", "text": "brand-a same-store"},
    ]

    monkeypatch.setattr(
        combined,
        "_parse_store_option",
        lambda option: {
            "store_id": str(option["store_id"]),
            "brand": option["text"].split()[0],
            "store": "same-store",
        },
    )

    store_list = combined._build_store_list_from_options(raw_options)

    assert [(item["store_id"], item["brand"]) for item in store_list] == [
        ("1", "brand-a"),
        ("1", "brand-b"),
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
    monkeypatch.setattr(macro_validate, "_recollect_stores", lambda *args, **kwargs: set())
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


def test_validate_toorder_orders_limits_compare_to_collected_stores(monkeypatch):
    other_store = "\uB3D9\uB450\uCC9C\uC9C0\uD589\uC810"
    deleted_stores = []
    recollected_stores = []

    monkeypatch.setattr(
        macro_validate,
        "_toorder_baemin_by_store",
        lambda target_date: {SAMSUNG: 100000, other_store: 1047700},
    )
    monkeypatch.setattr(
        macro_validate,
        "_baemin_orders_by_store",
        lambda target_date: {SAMSUNG: 100000, other_store: 956400},
    )

    def fake_delete(target_date, store_names):
        deleted_stores.extend(store_names)
        return 0

    def fake_recollect(store_info_per_account, account_list, target_date, store_names):
        recollected_stores.extend(store_names)
        return set(store_names)

    monkeypatch.setattr(macro_validate, "_delete_orders_for_stores", fake_delete)
    monkeypatch.setattr(macro_validate, "_recollect_stores", fake_recollect)
    monkeypatch.setattr(
        macro_validate,
        "_inspect_brand_coverage",
        lambda target_date, store_names, expected_brands=None: {
            store: {
                "expected_brands": [DORIDANG],
                "existing_brands": [DORIDANG],
                "active_brands": [DORIDANG],
                "missing_brands": [],
                "stale_brands": [],
                "issue_type": None,
            }
            for store in store_names
        },
    )

    result = macro_validate.validate_toorder_orders(
        account_list=[{"account_id": "acct", "password": "pw"}],
        store_info_per_account=[
            {
                "account_id": "acct",
                "stores": [{"brand": DORIDANG, "store": SAMSUNG, "store_id": "1"}],
            }
        ],
        target_date="2026-06-28",
    )

    assert result["matched"] is True
    assert result["compared_count"] == 1
    assert set(result["store_results"]) == {SAMSUNG}
    assert other_store not in result["store_results"]
    assert deleted_stores == []
    assert recollected_stores == []


def test_manual_precheck_task_is_wired_before_load_accounts():
    source = UPLOAD_DAG_SOURCE.read_text(encoding="utf-8")
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
    assert "t_ingest >> t_precheck" in source
