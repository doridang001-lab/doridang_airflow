from pathlib import Path

from modules.transform.pipelines.db import DB_Beamin_retry as retry


DAG_RETRY_SOURCE = Path(__file__).resolve().parents[1] / "dags" / "db" / "DB_Beamin_Macro_Dags_Retry.py"


def _store(store_id="store-1", store_name="store-a"):
    return {"store_id": store_id, "brand": "brand", "store": store_name}


def _retry_payload(store=None):
    store = store or _store()
    return {
        "target_date": "2026-07-01",
        "store_info_per_account": [{"account_id": "acct-1", "stores": [store]}],
        "ad_store_infos": [{"account_id": "acct-1", **store}],
    }


def _toorder_result(store_name="store-a"):
    return {
        "store_results": {store_name: {}},
        "mismatched_stores": [store_name],
        "missing_brand_stores": [],
    }


def _ad_result(store=None):
    store = store or _store()
    return {"still_empty": [{"account_id": "acct-1", **store}]}


def test_restore_meta_credentials_uses_central_account_source(monkeypatch):
    monkeypatch.setattr(
        retry,
        "_load_account_map",
        lambda account_ids=None: {
            "acct-1": {"account_id": "acct-1", "password": "secret", "store_name": "store-a"}
        },
    )
    meta = {
        "account_list": [{"account_id": "acct-1", "store_name": "store-a"}],
        "ad_stores": [{"account_id": "acct-1", "store_id": "store-1"}],
        "original_failed": {"accounts": [], "stores": [], "orders": [], "ads": []},
        "failed": {
            "accounts": [],
            "stores": [],
            "orders": [],
            "ads": [{"account": {"account_id": "acct-1"}, "stores": [_store()]}],
        },
    }

    restored = retry.restore_meta_credentials(meta)

    assert "password" not in meta["account_list"][0]
    assert restored["account_list"][0]["password"] == "secret"
    assert restored["ad_stores"][0]["password"] == "secret"
    assert restored["failed"]["ads"][0]["account"]["password"] == "secret"


def test_build_retry_conf_initializes_store_retry_history_once_per_store():
    store = _store()
    conf = retry.build_retry_conf(
        failed={
            "accounts": [],
            "stores": [],
            "orders": [{"account": {"account_id": "acct-1"}, "stores": [store]}],
            "ads": [{"account": {"account_id": "acct-1"}, "stores": [store]}],
        },
        target_date="2026-07-01",
    )

    assert conf["max_attempts"] == 3
    assert conf["retry_history"] == {"acct-1::store-1": 1}


def test_restore_failed_from_conf_filters_to_allowed_account_ids(monkeypatch):
    accounts = {
        "acct-1": {"account_id": "acct-1", "password": "pw", "store_name": "상위 매장"},
        "acct-2": {"account_id": "acct-2", "password": "pw", "store_name": "하위 매장"},
    }

    def fake_account_map(account_ids=None):
        return {
            account_id: accounts[account_id]
            for account_id in account_ids or []
            if account_id in accounts
        }

    monkeypatch.setattr(retry, "_load_account_map", fake_account_map)
    conf = {
        "failed_account_ids": ["acct-1", "acct-2"],
        "failed_accounts_ids_only": ["acct-1", "acct-2"],
        "failed_stores": [
            {"account_id": "acct-1", "store": _store("s1")},
            {"account_id": "acct-2", "store": _store("s2")},
        ],
        "failed_orders": [
            {"account_id": "acct-1", "stores": [_store("s1")]},
            {"account_id": "acct-2", "stores": [_store("s2")]},
        ],
        "failed_ads": [
            {"account_id": "acct-1", "stores": [_store("s1")]},
            {"account_id": "acct-2", "stores": [_store("s2")]},
        ],
        "allowed_account_ids": ["acct-1"],
    }

    failed, retry_accounts = retry.restore_failed_from_conf(conf)

    assert [account["account_id"] for account in retry_accounts] == ["acct-1"]
    assert [account["account_id"] for account in failed["accounts"]] == ["acct-1"]
    assert [item["account"]["account_id"] for item in failed["stores"]] == ["acct-1"]
    assert [item["account"]["account_id"] for item in failed["orders"]] == ["acct-1"]
    assert [item["account"]["account_id"] for item in failed["ads"]] == ["acct-1"]


def test_build_next_retry_conf_exhausts_store_after_per_store_limit():
    store = _store()
    conf = retry.build_retry_conf(
        failed={
            "orders": [{"account": {"account_id": "acct-1"}, "stores": [store]}],
            "ads": [{"account": {"account_id": "acct-1"}, "stores": [store]}],
        },
        target_date="2026-07-01",
        source_run_id="scheduled__2026-06-30T18:10:00+00:00",
    )
    conf["notification_context"] = {"target_date": "2026-07-01", "total_accounts": 1}

    second = retry.build_next_retry_conf(
        previous_conf=conf,
        retry_payload=_retry_payload(store),
        toorder_result=_toorder_result(),
        ad_funnel_result=_ad_result(store),
        attempt=2,
        max_attempts=3,
    )

    assert second["retry_history"] == {"acct-1::store-1": 2}
    assert second["failed_account_ids"] == ["acct-1"]
    assert second["failed_orders"] == [{"account_id": "acct-1", "stores": [store]}]
    assert second["failed_ads"] == [{"account_id": "acct-1", "stores": [store]}]
    assert second["notification_context"] == conf["notification_context"]

    third = retry.build_next_retry_conf(
        previous_conf=second,
        retry_payload=_retry_payload(store),
        toorder_result=_toorder_result(),
        ad_funnel_result=_ad_result(store),
        attempt=3,
        max_attempts=3,
    )

    assert third["retry_history"] == {"acct-1::store-1": 3}
    assert third["failed_account_ids"] == ["acct-1"]
    assert third["failed_orders"] == [{"account_id": "acct-1", "stores": [store]}]
    assert third["failed_ads"] == [{"account_id": "acct-1", "stores": [store]}]

    fourth = retry.build_next_retry_conf(
        previous_conf=third,
        retry_payload=_retry_payload(store),
        toorder_result=_toorder_result(),
        ad_funnel_result=_ad_result(store),
        attempt=4,
        max_attempts=3,
    )

    assert fourth["retry_history"] == {"acct-1::store-1": 3}
    assert fourth["toorder_possible_mismatch_stores"] == ["store-a"]
    assert fourth["toorder_retry_exhausted"] == [
        {"account_id": "acct-1", "store_id": "store-1", "store": "store-a", "attempts": 3}
    ]
    assert fourth["failed_account_ids"] == []
    assert fourth["failed_orders"] == []
    assert fourth["failed_ads"] == []


def test_build_next_retry_conf_seeds_legacy_conf_without_retry_history():
    store = _store()
    legacy_conf = {
        "attempt": 1,
        "max_attempts": 3,
        "target_date": "2026-07-01",
        "source_run_id": "manual__legacy",
        "failed_account_ids": ["acct-1"],
        "failed_accounts_ids_only": [],
        "failed_stores": [],
        "failed_orders": [{"account_id": "acct-1", "stores": [store]}],
        "failed_ads": [],
    }

    second = retry.build_next_retry_conf(
        previous_conf=legacy_conf,
        retry_payload=_retry_payload(store),
        toorder_result=_toorder_result(),
        ad_funnel_result={},
        attempt=2,
        max_attempts=3,
    )

    assert second["retry_history"] == {"acct-1::store-1": 2}
    assert second["failed_orders"] == [{"account_id": "acct-1", "stores": [store]}]

    third = retry.build_next_retry_conf(
        previous_conf=second,
        retry_payload=_retry_payload(store),
        toorder_result=_toorder_result(),
        ad_funnel_result={},
        attempt=3,
        max_attempts=3,
    )

    assert third["retry_history"] == {"acct-1::store-1": 3}
    assert third["failed_orders"] == [{"account_id": "acct-1", "stores": [store]}]

    fourth = retry.build_next_retry_conf(
        previous_conf=third,
        retry_payload=_retry_payload(store),
        toorder_result=_toorder_result(),
        ad_funnel_result={},
        attempt=4,
        max_attempts=3,
    )

    assert fourth["failed_orders"] == []
    assert fourth["toorder_possible_mismatch_stores"] == ["store-a"]


def test_build_next_retry_conf_excludes_source_mismatch_stores():
    store = _store()
    conf = retry.build_retry_conf(
        failed={},
        target_date="2026-07-01",
        source_run_id="scheduled__2026-06-30T18:10:00+00:00",
    )

    next_conf = retry.build_next_retry_conf(
        previous_conf=conf,
        retry_payload=_retry_payload(store),
        toorder_result={**_toorder_result(), "source_mismatch_stores": ["store-a"]},
        ad_funnel_result={},
        attempt=2,
        max_attempts=3,
    )

    assert next_conf["failed_orders"] == []
    assert next_conf["failed_account_ids"] == []


def test_retry_needed_requires_residual_failed_even_when_validation_is_clean():
    assert retry.retry_needed(
        toorder_result={
            "matched": True,
            "compared_count": 0,
            "mismatched_stores": [],
            "missing_brand_stores": [],
            "retried_stores": [],
        },
        ad_funnel_result={"empty_stores": [], "retried": [], "still_empty": []},
        failed={"accounts": ["acct"], "stores": ["store"], "orders": [], "ads": ["ad"]},
    ) is True


def test_retry_needed_skips_when_residual_and_validation_are_clean():
    assert retry.retry_needed(
        toorder_result={
            "matched": True,
            "compared_count": 0,
            "mismatched_stores": [],
            "missing_brand_stores": [],
            "retried_stores": [],
        },
        ad_funnel_result={"empty_stores": [], "retried": [], "still_empty": []},
        failed={"accounts": [], "stores": [], "orders": [], "ads": []},
    ) is False


def test_split_retry_conf_by_lane_keeps_accounts_disjoint_and_allows_empty_lane():
    conf = {
        "attempt": 1,
        "max_attempts": 3,
        "target_date": "2026-07-01",
        "source_dag_id": "DB_Beamin_Macro_Upload_Dags",
        "source_run_id": "collect_export__test",
        "stability_profile": "safe_daily",
        "failed_account_ids": ["acct-3", "acct-1", "acct-2"],
        "failed_accounts_ids_only": ["acct-1"],
        "failed_stores": [{"account_id": "acct-1", "store": _store("s1")}],
        "failed_orders": [{"account_id": "acct-2", "stores": [_store("s2")]}],
        "failed_ads": [{"account_id": "acct-3", "stores": [_store("s3")]}],
        "retry_history": {"acct-1::s1": 1},
        "notification_context": {"target_date": "2026-07-01"},
    }

    lanes = retry.split_retry_conf_by_lane(conf, 2)
    lane_ids = [set(lane["failed_account_ids"]) for lane in lanes]

    assert lane_ids[0].isdisjoint(lane_ids[1])
    assert lane_ids[0] | lane_ids[1] == {"acct-1", "acct-2", "acct-3"}
    assert lanes[0]["target_date"] == conf["target_date"]
    assert lanes[0]["notification_context"] == conf["notification_context"]

    single = retry.split_retry_conf_by_lane({"failed_account_ids": ["acct-1"]}, 2)
    assert single[0]["failed_account_ids"] == ["acct-1"]
    assert single[1]["failed_account_ids"] == []


def test_merge_retry_payloads_dedupes_stores_and_joins_result():
    payload_1 = {
        "target_date": "2026-07-01",
        "retry_result": "lane 1",
        "store_info_per_account": [{"account_id": "acct-1", "stores": [_store("s1")]}],
        "ad_store_infos": [{"account_id": "acct-1", **_store("s1")}],
        "residual_failed": {
            "accounts": [],
            "stores": [],
            "orders": [{"account": {"account_id": "acct-1"}, "stores": [_store("s1")]}],
            "ads": [],
        },
    }
    payload_2 = {
        "target_date": "2026-07-01",
        "retry_result": "lane 2",
        "store_info_per_account": [{"account_id": "acct-1", "stores": [_store("s1"), _store("s2")]}],
        "ad_store_infos": [{"account_id": "acct-1", **_store("s1")}],
        "residual_failed": {
            "accounts": [],
            "stores": [],
            "orders": [{"account": {"account_id": "acct-1"}, "stores": [_store("s1")]}],
            "ads": [],
        },
    }

    merged = retry.merge_retry_payloads(payload_1, payload_2)

    assert merged["target_date"] == "2026-07-01"
    assert merged["retry_result"] == "lane 1\nlane 2"
    assert merged["store_info_per_account"] == [
        {"account_id": "acct-1", "stores": [_store("s1"), _store("s2")]}
    ]
    assert merged["ad_store_infos"] == [{"account_id": "acct-1", **_store("s1")}]
    assert merged["residual_failed"]["orders"] == [
        {"account": {"account_id": "acct-1"}, "stores": [_store("s1")]}
    ]


def test_retry_once_failed_returns_residual_schema(monkeypatch):
    failed = {
        "accounts": [{"account_id": "acct-1"}],
        "stores": [],
        "orders": [],
        "ads": [],
        "stages": [],
    }

    def fake_collect(accounts, target_date=None):
        return {
            "failed": {
                "accounts": accounts,
                "stores": [],
                "orders": [],
                "ads": [],
                "stages": [],
            }
        }

    from modules.transform.pipelines.db import DB_Beamin_combined as combined

    monkeypatch.setattr(combined, "collect_now_and_woori", fake_collect)
    result = combined.retry_once_failed(failed, target_date="2026-07-09")

    assert set(result) == {"summary", "residual_failed"}
    assert set(result["residual_failed"]) == {"accounts", "stores", "orders", "ads", "stages"}
    assert result["residual_failed"]["accounts"] == failed["accounts"]


def test_retry_collect_from_conf_preserves_account_failures_when_collection_raises(monkeypatch):
    failed = {
        "accounts": [{"account_id": "acct-1", "password": "pw", "store_name": "store-a"}],
        "stores": [],
        "orders": [],
        "ads": [],
    }

    monkeypatch.setattr(retry, "restore_failed_from_conf", lambda conf: (failed, failed["accounts"]))

    def raise_collect(*args, **kwargs):
        raise RuntimeError("dashboard down")

    monkeypatch.setattr(retry, "collect_now_and_woori", raise_collect)
    result = retry.retry_collect_from_conf({"target_date": "2026-07-09"})

    assert result["residual_failed"]["accounts"] == failed["accounts"]
    assert "계정 재수집 실패" in result["account_result"]["summary"]


def test_retry_needed_requires_ad_funnel_still_empty():
    assert retry.retry_needed(
        toorder_result={
            "matched": True,
            "compared_count": 1,
            "mismatched_stores": [],
            "missing_brand_stores": [],
        },
        ad_funnel_result={"empty_stores": ["store-a"], "retried": ["store-a"], "still_empty": ["store-a"]},
        failed={},
    ) is True


def test_retry_needed_requires_remaining_toorder_mismatch_even_after_local_retry():
    assert retry.retry_needed(
        toorder_result={
            "matched": False,
            "compared_count": 1,
            "store_results": {"store-a": {}, "store-b": {}},
            "mismatched_stores": ["store-a", "store-b"],
            "missing_brand_stores": [],
            "retried_stores": ["store-a", "store-b"],
        },
        ad_funnel_result={"still_empty": []},
        failed={},
    ) is True


def test_retry_needed_skips_toorder_gap_mismatch():
    assert retry.retry_needed(
        toorder_result={
            "matched": False,
            "compared_count": 1,
            "store_results": {"store-a": {"toorder_gap": True}},
            "mismatched_stores": ["store-a"],
            "missing_brand_stores": [],
            "retried_stores": [],
        },
        ad_funnel_result={"still_empty": []},
        failed={},
    ) is False


def test_retry_dag_uses_deterministic_next_run_id():
    source = DAG_RETRY_SOURCE.read_text(encoding="utf-8")

    assert "MAX_ATTEMPTS = 3" in source
    assert "import uuid" not in source
    assert "uuid.uuid4" not in source
    assert "source_run_id" in source
    assert "__{root}" in source
