import os
import sys
from pathlib import Path
from unittest.mock import patch

import pendulum
import pytest

os.environ.setdefault("AIRFLOW_HOME", str(Path(__file__).resolve().parents[1] / ".tmp" / "airflow-test"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from airflow.utils.trigger_rule import TriggerRule
from dags.db import DB_Beamin_Macro_Dags as macro
from modules.transform.pipelines.db import beamin_staging as staging


class FakeTaskInstance:
    def __init__(self, account_list=None, task_id="collect_all"):
        self.values = {}
        self.account_list = account_list
        self.task_id = task_id

    def xcom_push(self, *, key, value):
        self.values[key] = value

    def xcom_pull(self, *, task_ids, key):
        if task_ids == "load_accounts" and key == "account_list":
            return self.account_list
        return self.values.get(key)


class FakeDagRun:
    def __init__(self, conf=None, run_id="scheduled__test"):
        self.conf = conf or {}
        self.run_id = run_id


def _accounts() -> list[dict]:
    return [
        {
            "account_id": f"acct-{idx:02d}",
            "password": "pw",
            "store_name": f"매장 {idx:02d}",
        }
        for idx in range(67)
    ]


def _load_for(batch_range=None):
    ti = FakeTaskInstance()
    context = {
        "dag_run": FakeDagRun(),
        "params": {"collect_range": batch_range} if batch_range else {},
        "ti": ti,
    }
    with patch.object(macro, "pipeline_load_accounts", return_value=_accounts()), \
         patch.object(macro, "_MACRO_ROLE", {"range": "상위", "slug": "top"}):
        macro.load_accounts(**context)
    return ti.values["account_list"]


def test_role_then_batch_split_produces_eight_accounts():
    selected = _load_for("batch:1/4")

    assert len(selected) == 8


def test_four_batches_cover_upper_half_without_duplicates():
    batches = [_load_for(f"batch:{idx}/4") for idx in range(1, 5)]
    account_ids = [[account["account_id"] for account in batch] for batch in batches]
    flattened = [account_id for batch in account_ids for account_id in batch]

    assert [len(batch) for batch in batches] == [8, 8, 8, 9]
    assert len(set(flattened)) == 33
    assert set(flattened) == {account["account_id"] for account in _load_for()}


def test_batch_unspecified_keeps_upper_half_behavior():
    selected = _load_for()

    assert len(selected) == 33


def test_batch_split_keeps_duplicate_account_id_in_one_batch():
    accounts = [
        {"account_id": "acct-1", "store_name": "매장 01"},
        {"account_id": "acct-1", "store_name": "매장 02"},
        {"account_id": "acct-2", "store_name": "매장 03"},
        {"account_id": "acct-3", "store_name": "매장 04"},
        {"account_id": "acct-4", "store_name": "매장 05"},
    ]
    batches = [macro._split_accounts_by_range(accounts, f"batch:{idx}/4") for idx in range(1, 5)]
    owners = [
        index
        for index, batch in enumerate(batches, start=1)
        if any(account["account_id"] == "acct-1" for account in batch)
    ]

    assert owners == [1]
    assert sum(1 for batch in batches for account in batch if account["account_id"] == "acct-1") == 2


def test_main_dag_uses_two_parallel_batch_lanes_and_one_final_retry():
    assert macro.dag.schedule_interval == "15 0 * * *"
    assert macro.dag.is_paused_upon_creation is False
    assert "trigger_next_batch" not in macro.dag.task_ids
    assert set(macro._BATCH_TASK_IDS).issubset(macro.dag.task_ids)
    assert "init_staging" in macro.dag.task_ids
    assert macro.dag.max_active_tasks == 2

    init = macro.dag.get_task("init_staging")
    batch_1 = macro.dag.get_task("collect_batch_1")
    batch_2 = macro.dag.get_task("collect_batch_2")
    batch_3 = macro.dag.get_task("collect_batch_3")
    batch_4 = macro.dag.get_task("collect_batch_4")
    retry = macro.dag.get_task("retry_failed")

    assert init.upstream_task_ids == {"load_accounts"}
    assert batch_1.upstream_task_ids == {"init_staging"}
    assert batch_2.upstream_task_ids == {"collect_batch_1"}
    assert batch_3.upstream_task_ids == {"init_staging"}
    assert batch_4.upstream_task_ids == {"collect_batch_3"}
    assert retry.upstream_task_ids == {"collect_batch_2", "collect_batch_4"}
    assert batch_2.trigger_rule == TriggerRule.ALL_DONE
    assert batch_3.trigger_rule == TriggerRule.ALL_SUCCESS
    assert batch_4.trigger_rule == TriggerRule.ALL_DONE
    assert batch_3.op_kwargs["lane_offset"] is True
    assert retry.trigger_rule == TriggerRule.ALL_DONE
    assert retry.op_kwargs == {
        "collect_task_ids": list(macro._BATCH_TASK_IDS),
        "retry_all_types": False,
    }


def test_target_date_defaults_to_previous_data_interval_end():
    context = {
        "dag_run": FakeDagRun(conf={}),
        "logical_date": pendulum.datetime(2026, 7, 27, 15, 15, tz=macro.KST),
        "data_interval_end": pendulum.datetime(2026, 7, 29, 0, 15, tz=macro.KST),
    }

    assert macro._target_date_from_context(context) == "2026-07-28"


def test_target_date_conf_overrides_data_interval_end():
    context = {
        "dag_run": FakeDagRun(conf={"target_date": "2026-07-26"}),
        "data_interval_end": pendulum.datetime(2026, 7, 29, 0, 15, tz=macro.KST),
    }

    assert macro._target_date_from_context(context) == "2026-07-26"


def test_legacy_batch_dags_are_removed_from_module():
    assert not hasattr(macro, "dag_2")
    assert not hasattr(macro, "dag_3")
    assert not hasattr(macro, "dag_4")
    assert not hasattr(macro, "trigger_next_batch")


def test_collect_timeout_pushes_only_unfinished_accounts(tmp_path):
    accounts = _accounts()[:3]
    ti = FakeTaskInstance(account_list=accounts)
    context = {
        "dag_run": FakeDagRun(conf={"target_date": "2026-07-23"}),
        "ti": ti,
    }
    local_analytics = tmp_path / "run"
    local_baemin = local_analytics / "baemin_macro"

    def interrupt_collection(_account_list, **kwargs):
        progress = macro.load_progress(
            kwargs["progress_file"],
            run_id=kwargs["progress_run_id"],
            target_date=kwargs["target_date"],
        )
        progress["done_accounts"] = [accounts[0]["account_id"]]
        progress["success"] = 1
        progress["carry"]["validation"] = [{"account_id": accounts[0]["account_id"]}]
        staging.save_progress(kwargs["progress_file"], progress)
        raise macro.AirflowTaskTimeout("timeout")

    with patch.object(macro, "_main_stage_paths", return_value=(local_analytics, local_baemin)), \
         patch.object(macro, "resolve_stability_profile", return_value={
             "name": "test",
             "initial_stagger_range": (0, 0),
             "driver_restart_every_stores": 999,
             "max_session_recovery_per_account": 2,
         }), \
         patch.object(macro, "patch_baemin_staging_paths", return_value=(None, [])), \
         patch.object(macro, "restore_baemin_staging_paths"), \
         patch.object(macro, "pipeline_collect_all", side_effect=interrupt_collection), \
         patch.object(macro.random, "uniform", return_value=0), \
         patch.object(macro.time, "sleep"):
        with pytest.raises(macro.AirflowTaskTimeout):
            macro.collect_all(**context)

    failed_ids = [account["account_id"] for account in ti.values["failed"]["accounts"]]
    assert failed_ids == [accounts[1]["account_id"], accounts[2]["account_id"]]
    assert ti.values["validation"] == [{"account_id": accounts[0]["account_id"]}]


def test_second_batch_preserves_first_batch_staging(tmp_path):
    accounts = _accounts()[:33]
    ti = FakeTaskInstance(account_list=accounts, task_id="collect_batch_2")
    context = {
        "dag_run": FakeDagRun(conf={"target_date": "2026-07-23"}),
        "ti": ti,
    }
    local_analytics = tmp_path / "run"
    local_baemin = local_analytics / "baemin_macro"
    local_baemin.mkdir(parents=True)
    marker = local_baemin / "batch1.parquet"
    marker.write_bytes(b"batch1")
    pipeline_result = {
        "summary": "성공 11/11 계정",
        "failed": {"accounts": [], "stores": [], "orders": [], "ads": []},
        "validation": [],
        "ad_stores": [],
        "store_info_per_account": [],
    }

    with patch.object(macro, "_main_stage_paths", return_value=(local_analytics, local_baemin)), \
         patch.object(macro, "resolve_stability_profile", return_value={
             "name": "test",
             "initial_stagger_range": (0, 0),
             "driver_restart_every_stores": 999,
             "max_session_recovery_per_account": 2,
         }), \
         patch.object(macro, "patch_baemin_staging_paths", return_value=(None, [])), \
         patch.object(macro, "restore_baemin_staging_paths"), \
         patch.object(macro, "pipeline_collect_all", return_value=pipeline_result) as collect, \
         patch.object(macro, "init_empty_staging") as reset, \
        patch.object(macro.random, "uniform", return_value=0), \
         patch.object(macro.time, "sleep"):
        result = macro.collect_batch(
            batch_range="batch:2/4",
            **context,
        )

    assert result == "성공 11/11 계정"
    assert len(collect.call_args.args[0]) == 8
    assert marker.read_bytes() == b"batch1"
    reset.assert_not_called()


def test_final_retry_merges_all_batch_failure_types():
    account_1, account_2 = _accounts()[:2]
    store_1 = {"store_id": "s1", "store": "매장 1"}
    store_2 = {"store_id": "s2", "store": "매장 2"}

    class BatchTI(FakeTaskInstance):
        def __init__(self):
            super().__init__()
            self.batch_values = {
                ("collect_batch_1", "failed"): {
                    "accounts": [],
                    "stores": [],
                    "orders": [{"account": account_1, "stores": [store_1]}],
                    "ads": [{"account": account_2, "stores": [store_2]}],
                },
                ("collect_batch_2", "failed"): {
                    "accounts": [account_1],
                    "stores": [],
                    "orders": [{"account": account_1, "stores": [store_1]}],
                    "ads": [],
                },
                ("collect_batch_3", "failed"): {
                    "accounts": [],
                    "stores": [{"account": account_2, "store": store_1}],
                    "orders": [],
                    "ads": [],
                },
            }

        def xcom_pull(self, *, task_ids, key):
            return self.batch_values.get((task_ids, key))

    ti = BatchTI()
    context = {
        "dag_run": FakeDagRun(conf={"target_date": "2026-07-23"}),
        "ti": ti,
    }
    residual = {"accounts": [], "stores": [], "orders": [], "ads": []}
    with patch.object(macro, "resolve_stability_profile", return_value={"name": "test"}), \
         patch.object(macro, "_main_stage_paths", return_value=(Path("stage"), Path("stage/baemin_macro"))), \
         patch.object(macro, "patch_baemin_staging_paths", return_value=(None, [])), \
         patch.object(macro, "restore_baemin_staging_paths"), \
         patch.object(
             macro,
             "pipeline_retry_failed",
             return_value={"summary": "ok", "residual_failed": residual},
         ) as retry:
        result = macro.retry_failed(
            collect_task_ids=list(macro._BATCH_TASK_IDS),
            retry_all_types=True,
            **context,
        )

    assert result == "ok"
    merged = retry.call_args.args[0]
    assert [account["account_id"] for account in merged["accounts"]] == [
        account_1["account_id"]
    ]
    assert merged["orders"] == []
    assert len(merged["stores"]) == 1
    assert len(merged["ads"]) == 1


def test_export_meta_uses_retry_residual_failed(tmp_path):
    account = {"account_id": "acct-1", "password": "pw", "store_name": "매장 1"}
    original_failed = {
        "accounts": [account],
        "stores": [],
        "orders": [],
        "ads": [],
        "stages": [],
    }
    residual_failed = {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}
    local_analytics = tmp_path / "analytics"
    local_baemin = local_analytics / "baemin_macro"
    local_baemin.mkdir(parents=True)
    captured = {}

    class ExportTI(FakeTaskInstance):
        def __init__(self):
            super().__init__(account_list=[account])
            self.batch_values = {
                ("retry_failed", "original_failed"): original_failed,
                ("retry_failed", "residual_failed"): residual_failed,
                ("collect_batch_1", "validation"): [],
                ("collect_batch_1", "ad_stores"): [],
                ("collect_batch_1", "store_info_per_account"): [],
            }

        def xcom_pull(self, *, task_ids, key):
            if task_ids == "load_accounts" and key == "account_list":
                return self.account_list
            return self.batch_values.get((task_ids, key))

    def fake_export(local_baemin_arg, run_id, *, inbox_dir, meta, replace_existing, folder_prefix):
        captured["meta"] = meta
        return tmp_path / "inbox" / "manual__top__run"

    ti = ExportTI()
    with patch.object(macro, "_main_stage_paths", return_value=(local_analytics, local_baemin)), \
         patch.object(macro, "export_staging_to_inbox", side_effect=fake_export), \
         patch.object(macro, "cleanup_staging"):
        result = macro.export_to_upload_inbox(
            collect_task_ids=["collect_batch_1"],
            ti=ti,
            dag_run=FakeDagRun(conf={"target_date": "2026-07-23"}, run_id="run"),
        )

    assert result.startswith("upload inbox export 완료:")
    assert captured["meta"]["original_failed"] == original_failed
    assert captured["meta"]["failed"] == residual_failed
    assert captured["meta"]["residual_failed"] == residual_failed


def test_trigger_retry_filters_failed_payload_to_loaded_accounts(monkeypatch):
    upper_account = {"account_id": "acct-upper", "password": "pw", "store_name": "상위 매장"}
    lower_account = {"account_id": "acct-lower", "password": "pw", "store_name": "하위 매장"}
    store = {"store_id": "s1", "store": "매장 1"}

    ti = FakeTaskInstance(account_list=[upper_account])
    ti.dag_id = "DB_Beamin_Macro_Dags"
    ti.values["residual_failed"] = {
        "accounts": [upper_account, lower_account],
        "stores": [],
        "orders": [
            {"account": upper_account, "stores": [store]},
            {"account": lower_account, "stores": [store]},
        ],
        "ads": [],
    }
    ti.values["toorder_result"] = {"matched": False}
    ti.values["ad_funnel_result"] = {"still_empty": []}

    context = {
        "dag_run": FakeDagRun(
            conf={"target_date": "2026-07-23"},
            run_id="scheduled__test",
        ),
        "params": {},
        "ti": ti,
    }

    with patch("airflow.api.common.trigger_dag.trigger_dag") as trigger, \
         patch.object(macro, "_MACRO_ROLE", {"range": "상위", "slug": "top"}):
        result = macro.trigger_retry_if_needed(**context)

    assert result.startswith("Retry DAG 트리거 완료:")
    retry_conf = trigger.call_args.kwargs["conf"]
    assert retry_conf["collect_range"] == "상위"
    assert retry_conf["allowed_account_ids"] == ["acct-upper"]
    assert retry_conf["failed_account_ids"] == ["acct-upper"]
    assert retry_conf["failed_accounts_ids_only"] == ["acct-upper"]
    assert retry_conf["failed_orders"] == [
        {"account_id": "acct-upper", "stores": [store]}
    ]


def test_trigger_retry_allows_large_failed_account_ratio():
    accounts = [
        {"account_id": f"acct-{idx:02d}", "password": "pw", "store_name": f"매장 {idx:02d}"}
        for idx in range(33)
    ]
    store = {"store_id": "s1", "store": "매장 1"}
    ti = FakeTaskInstance(account_list=accounts)
    ti.dag_id = "DB_Beamin_Macro_Dags"
    ti.values["residual_failed"] = {
        "accounts": [],
        "stores": [],
        "orders": [
            {"account": account, "stores": [store]}
            for account in accounts[:2]
        ],
        "ads": [],
        "stages": [],
    }
    ti.values["toorder_result"] = {"matched": False}
    ti.values["ad_funnel_result"] = {"still_empty": []}

    context = {
        "dag_run": FakeDagRun(
            conf={"target_date": "2026-07-23"},
            run_id="scheduled__test",
        ),
        "params": {},
        "ti": ti,
    }

    with patch("airflow.api.common.trigger_dag.trigger_dag") as trigger, \
         patch.object(macro, "_MACRO_ROLE", {"range": "상위", "slug": "top"}):
        result = macro.trigger_retry_if_needed(**context)

    assert result.startswith("Retry DAG 트리거 완료:")
    retry_conf = trigger.call_args.kwargs["conf"]
    assert retry_conf["failed_account_ids"] == ["acct-00", "acct-01"]
