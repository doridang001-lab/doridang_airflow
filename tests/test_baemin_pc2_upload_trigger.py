import os
import json
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("AIRFLOW_HOME", str(Path(__file__).resolve().parents[1] / ".tmp" / "airflow-test"))

from dags.db.DB_Beamin_Macro_Upload_Dags import dag as upload_dag
from dags.db.DB_Beamin_Macro_Upload_Dags import trigger_validate
from dags.db.DB_Beamin_Macro_Upload_Pc2_Dags import dag as pc2_dag
from dags.db.DB_Beamin_Macro_Upload_Validate_Dags import dag as validate_dag
from dags.db import DB_Beamin_Macro_Dags as main_collect
from modules.transform.pipelines.db import DB_Beamin_Macro_upload as upload
from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import (
    BOTTOM_FOLDER_PATTERN,
    TOP_FOLDER_PATTERN,
)
from modules.transform.utility.schedule import SMD_BAEMIN_UPLOAD_PC2_TIME


def test_pc2_dag_only_detects_and_triggers_primary_upload():
    assert set(pc2_dag.task_ids) == {"has_bottom_pending", "trigger_upload"}
    assert pc2_dag.is_paused_upon_creation is False
    assert pc2_dag.max_active_runs == 1
    assert pc2_dag.schedule_interval == SMD_BAEMIN_UPLOAD_PC2_TIME

    detector = pc2_dag.get_task("has_bottom_pending")
    trigger = pc2_dag.get_task("trigger_upload")
    assert detector.op_kwargs == {"folder_pattern": BOTTOM_FOLDER_PATTERN}
    assert detector.downstream_task_ids == {"trigger_upload"}
    assert trigger.trigger_dag_id == "DB_Beamin_Macro_Upload_Dags"
    assert trigger.conf == {
        "folder_pattern": BOTTOM_FOLDER_PATTERN,
        "skip_if_empty": True,
        "source": "pc2_bottom_sweep",
    }
    assert trigger.wait_for_completion is False
    assert getattr(trigger, "_defer", False) is False
    assert trigger.skip_when_already_exists is True


def test_primary_upload_only_ingests_and_triggers_validation():
    assert set(upload_dag.task_ids) == {"ingest", "has_pending", "trigger_validate"}
    assert upload_dag.max_active_runs == 1
    assert upload_dag.max_active_tasks != 1
    assert upload_dag.get_task("ingest").downstream_task_ids == {"has_pending"}
    assert upload_dag.get_task("has_pending").downstream_task_ids == {
        "trigger_validate"
    }
    assert upload_dag.get_task("ingest").op_kwargs == {
        "folder_pattern": f"{{{{ dag_run.conf.get('folder_pattern', '{TOP_FOLDER_PATTERN}') }}}}"
    }


def test_upload_gate_continues_for_manual_orders_without_cleaned_folder(monkeypatch):
    monkeypatch.setattr(upload, "count_pending_manual_baemin_order_files", lambda: 2)
    monkeypatch.setattr(upload, "count_partial_manual_baemin_order_files", lambda: 0)

    class FakeTI:
        def xcom_pull(self, task_ids, key):
            if key == "ingest_stats":
                return {"folders": 0, "cleaned": 0, "failed": 0}
            return None

    assert upload.has_ingested_or_manual_files(
        ti=FakeTI(),
        dag_run=SimpleNamespace(conf={"skip_if_empty": True}),
    ) is True


def test_upload_gate_skips_when_only_partial_manual_orders(monkeypatch):
    cleaned = []
    monkeypatch.setattr(upload, "count_pending_manual_baemin_order_files", lambda: 0)
    monkeypatch.setattr(upload, "count_partial_manual_baemin_order_files", lambda: 1)
    monkeypatch.setattr(upload, "_cleanup_handoff", lambda context: cleaned.append(context))

    class FakeTI:
        def xcom_pull(self, task_ids, key):
            if key == "ingest_stats":
                return {"folders": 0, "cleaned": 0, "failed": 0}
            return None

    context = {
        "ti": FakeTI(),
        "dag_run": SimpleNamespace(conf={}),
    }
    assert upload.has_ingested_or_manual_files(**context) is False
    assert cleaned == [context]


def test_validate_dag_contains_validation_chain():
    assert set(validate_dag.task_ids) == {
        "ingest_manual_baemin_orders",
        "precheck_manual_baemin_orders",
        "validate_orders",
        "validate_ad_funnel",
        "validate_toorder",
        "trigger_retry_if_needed",
        "notify_upload_result",
        "cleanup_manual_baemin_orders",
    }
    assert validate_dag.schedule_interval is None
    assert validate_dag.is_paused_upon_creation is False
    assert validate_dag.max_active_runs == 1
    ingest_manual = validate_dag.get_task("ingest_manual_baemin_orders")
    precheck = validate_dag.get_task("precheck_manual_baemin_orders")
    cleanup_manual = validate_dag.get_task("cleanup_manual_baemin_orders")
    assert ingest_manual.downstream_task_ids == {"precheck_manual_baemin_orders"}
    assert precheck.downstream_task_ids == {"validate_orders", "validate_ad_funnel", "validate_toorder"}
    assert validate_dag.get_task("trigger_retry_if_needed").downstream_task_ids == {"notify_upload_result"}
    assert validate_dag.get_task("notify_upload_result").downstream_task_ids == {"cleanup_manual_baemin_orders"}
    assert cleanup_manual.upstream_task_ids == {"notify_upload_result"}


def test_upload_trigger_validate_passes_only_handoff_path(monkeypatch):
    triggered = []
    monkeypatch.setattr(
        "airflow.api.common.trigger_dag.trigger_dag",
        lambda **kwargs: triggered.append(kwargs),
    )

    class FakeTI:
        def xcom_pull(self, task_ids, key):
            assert (task_ids, key) == ("ingest", "handoff_path")
            return "C:/Local_DB/baemin_upload_handoff/run.json"

    result = trigger_validate(
        ti=FakeTI(),
        dag_run=SimpleNamespace(
            run_id="scheduled__2026-07-27T01:30:00+00:00",
            conf={"folder_pattern": BOTTOM_FOLDER_PATTERN},
        ),
    )

    assert result.startswith("validate DAG 트리거 완료")
    assert triggered == [
        {
            "dag_id": "DB_Beamin_Macro_Upload_Validate_Dags",
            "run_id": "upload__scheduled__2026-07-27T01_30_00_00_00",
            "conf": {
                "handoff_path": "C:/Local_DB/baemin_upload_handoff/run.json",
                "folder_pattern": BOTTOM_FOLDER_PATTERN,
                "source": "upload_ingest",
                "source_run_id": "scheduled__2026-07-27T01:30:00+00:00",
            },
        }
    ]


def test_upload_trigger_validate_preserves_manual_conf(monkeypatch):
    triggered = []
    monkeypatch.setattr(
        "airflow.api.common.trigger_dag.trigger_dag",
        lambda **kwargs: triggered.append(kwargs),
    )

    class FakeTI:
        def xcom_pull(self, task_ids, key):
            return "C:/Local_DB/baemin_upload_handoff/run.json"

    result = trigger_validate(
        ti=FakeTI(),
        dag_run=SimpleNamespace(
            run_id="manual__orders",
            conf={
                "folder_pattern": BOTTOM_FOLDER_PATTERN,
                "target_date": "2026-08-26",
                "target_dates": ["2026-08-25", "2026-08-26"],
                "manual_baemin_dir": "E:/d_down",
            },
        ),
    )

    assert result.startswith("validate DAG 트리거 완료")
    assert triggered[0]["conf"]["target_date"] == "2026-08-26"
    assert triggered[0]["conf"]["target_dates"] == ["2026-08-25", "2026-08-26"]
    assert triggered[0]["conf"]["manual_baemin_dir"] == "E:/d_down"


def test_meta_pull_prefers_xcom_and_falls_back_to_handoff(tmp_path):
    handoff = tmp_path / "handoff.json"
    handoff.write_text(
        json.dumps({"target_date": "2026-07-27", "ingest_stats": {"folders": 1, "cleaned": 1}}),
        encoding="utf-8",
    )

    class FakeTI:
        def __init__(self, values):
            self.values = values

        def xcom_pull(self, task_ids, key):
            assert task_ids == "ingest"
            return self.values.get(key)

    context = {
        "ti": FakeTI({"target_date": "2026-07-28"}),
        "dag_run": SimpleNamespace(conf={"handoff_path": str(handoff)}),
    }
    assert upload._meta_pull(context, "target_date") == "2026-07-28"

    context["ti"] = FakeTI({})
    assert upload._meta_pull(context, "target_date") == "2026-07-27"
    assert upload._meta_pull(context, "ingest_stats") == {"folders": 1, "cleaned": 1}


def test_target_dates_uses_manual_ingest_order_dates_when_conf_absent():
    class FakeTI:
        def xcom_pull(self, task_ids, key):
            if (task_ids, key) == ("ingest_manual_baemin_orders", "return_value"):
                return json.dumps({"order_dates": ["2026-08-24", "2026-08-25"]})
            return None

    assert upload._target_dates({"ti": FakeTI(), "dag_run": SimpleNamespace(conf={})}) == [
        "2026-08-24",
        "2026-08-25",
    ]


def test_target_dates_conf_range_wins_over_handoff_target_date(tmp_path):
    handoff = tmp_path / "handoff.json"
    handoff.write_text(json.dumps({"target_date": "2026-08-20"}), encoding="utf-8")

    class FakeTI:
        def xcom_pull(self, task_ids, key):
            return None

    assert upload._target_dates(
        {
            "ti": FakeTI(),
            "dag_run": SimpleNamespace(
                conf={
                    "handoff_path": str(handoff),
                    "target_dates": ["2026-08-24", "2026-08-25"],
                }
            ),
        }
    ) == ["2026-08-24", "2026-08-25"]


def test_validate_toorder_runs_for_manual_backfill_dates(monkeypatch):
    calls = []
    monkeypatch.setattr(upload, "_save_validate_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        upload,
        "validate_toorder_orders",
        lambda account_list, store_info_per_account, target_date: calls.append(target_date)
        or {
            "matched": True,
            "compared_count": 1,
            "retried_stores": [],
            "mismatched_stores": [],
            "store_results": {"매장A": {"baemin": 100, "toorder": 100, "matched": True}},
        },
    )

    class FakeTI:
        def __init__(self):
            self.pushed = {}

        def xcom_pull(self, task_ids, key):
            if (task_ids, key) == ("ingest_manual_baemin_orders", "return_value"):
                return json.dumps({"order_dates": ["2026-08-24", "2026-08-25"]})
            if (task_ids, key) == ("precheck_manual_baemin_orders", "manual_precheck_summary"):
                return {"used": False}
            return None

        def xcom_push(self, key=None, value=None):
            self.pushed[key] = value

    ti = FakeTI()
    summary = upload.validate_toorder(ti=ti, dag_run=SimpleNamespace(conf={}))

    assert calls == ["2026-08-24", "2026-08-25"]
    assert sorted(ti.pushed["toorder_results_by_date"]) == ["2026-08-24", "2026-08-25"]
    assert ti.pushed["toorder_result"]["compared_count"] == 2
    assert "ToOrder 교차검증[2026-08-24]" in summary
    assert "ToOrder 교차검증[2026-08-25]" in summary


def test_upload_validate_retry_uses_residual_failed_before_original_failed(tmp_path, monkeypatch):
    handoff = tmp_path / "handoff.json"
    original_failed = {
        "accounts": [{"account_id": "acct-1"}],
        "stores": [],
        "orders": [{"account": {"account_id": "acct-2"}, "stores": [{"store_id": "s1"}]}],
        "ads": [],
        "stages": [],
    }
    empty_failed = {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}
    handoff.write_text(
        json.dumps(
            {
                "target_date": "2026-07-27",
                "account_list": [{"account_id": "acct-1"}, {"account_id": "acct-2"}],
                "failed": original_failed,
                "original_failed": original_failed,
                "residual_failed": empty_failed,
                "validation": [],
                "ad_stores": [],
                "ingest_stats": {"folders": 1, "cleaned": 1},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    triggered = []
    monkeypatch.setattr(
        "airflow.api.common.trigger_dag.trigger_dag",
        lambda **kwargs: triggered.append(kwargs),
    )

    class FakeTI:
        dag_id = "DB_Beamin_Macro_Upload_Validate_Dags"
        run_id = "upload__test"

        def __init__(self):
            self.pushed = {}

        def xcom_pull(self, task_ids, key):
            return None

        def xcom_push(self, key=None, value=None):
            self.pushed[key] = value

    ti = FakeTI()
    result = upload.trigger_retry_if_needed(
        ti=ti,
        dag_run=SimpleNamespace(run_id="upload__test", conf={"handoff_path": str(handoff)}),
    )

    assert result == "Retry DAG 트리거 스킵: 잔여 실패 없음"
    assert ti.pushed["retry_triggered"] is False
    assert ti.pushed["notification_context"]["residual_failed"] == empty_failed
    assert triggered == []


def test_upload_validate_retry_limits_to_toorder_retry_failed_store(tmp_path, monkeypatch):
    handoff = tmp_path / "handoff.json"
    empty_failed = {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}
    handoff.write_text(
        json.dumps(
            {
                "target_date": "2026-07-27",
                "account_list": [
                    {"account_id": "acct-1", "password": "pw1"},
                    {"account_id": "acct-2", "password": "pw2"},
                ],
                "failed": {"accounts": [{"account_id": "legacy"}], "stores": [], "orders": [], "ads": []},
                "residual_failed": empty_failed,
                "validation": [],
                "ad_stores": [],
                "store_info_per_account": [
                    {
                        "account_id": "acct-1",
                        "stores": [
                            {"store_id": "s1", "store": "도리당 매장A"},
                            {"store_id": "s2", "store": "도리당 매장B"},
                        ],
                    },
                    {
                        "account_id": "acct-2",
                        "stores": [{"store_id": "s3", "store": "도리당 매장C"}],
                    },
                ],
                "ingest_stats": {"folders": 1, "cleaned": 1},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    triggered = []
    monkeypatch.setattr(
        "airflow.api.common.trigger_dag.trigger_dag",
        lambda **kwargs: triggered.append(kwargs),
    )

    class FakeTI:
        dag_id = "DB_Beamin_Macro_Upload_Validate_Dags"
        run_id = "upload__test"

        def __init__(self):
            self.pushed = {}

        def xcom_pull(self, task_ids, key):
            if (task_ids, key) == ("validate_toorder", "toorder_result"):
                return {
                    "retry_failed_stores": ["매장B"],
                    "retry_skipped_stores": [],
                    "mismatched_stores": ["매장B"],
                    "retried_stores": ["매장B"],
                }
            if (task_ids, key) == ("validate_ad_funnel", "ad_funnel_result"):
                return {"still_empty": []}
            return None

        def xcom_push(self, key=None, value=None):
            self.pushed[key] = value

    ti = FakeTI()
    result = upload.trigger_retry_if_needed(
        ti=ti,
        dag_run=SimpleNamespace(run_id="upload__test", conf={"handoff_path": str(handoff)}),
    )

    assert result.startswith("Retry DAG 트리거 완료:")
    assert ti.pushed["retry_triggered"] is True
    retry_conf = triggered[0]["conf"]
    assert retry_conf["failed_account_ids"] == ["acct-1"]
    assert retry_conf["failed_orders"] == [
        {"account_id": "acct-1", "stores": [{"store_id": "s2", "store": "도리당 매장B"}]}
    ]
    assert retry_conf["failed_accounts_ids_only"] == []


def test_upload_validate_retry_triggers_each_backfill_date(tmp_path, monkeypatch):
    handoff = tmp_path / "handoff.json"
    empty_failed = {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}
    handoff.write_text(
        json.dumps(
            {
                "target_dates": ["2026-08-24", "2026-08-25"],
                "account_list": [{"account_id": "acct-1", "password": "pw1"}],
                "failed": empty_failed,
                "residual_failed": empty_failed,
                "validation": [],
                "ad_stores": [],
                "store_info_per_account": [
                    {
                        "account_id": "acct-1",
                        "stores": [
                            {"store_id": "s1", "store": "도리당 매장A"},
                            {"store_id": "s2", "store": "도리당 매장B"},
                        ],
                    }
                ],
                "ingest_stats": {"folders": 0, "cleaned": 0},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    triggered = []
    monkeypatch.setattr(
        "airflow.api.common.trigger_dag.trigger_dag",
        lambda **kwargs: triggered.append(kwargs),
    )

    class FakeTI:
        dag_id = "DB_Beamin_Macro_Upload_Validate_Dags"
        run_id = "upload__backfill"

        def __init__(self):
            self.pushed = {}

        def xcom_pull(self, task_ids, key):
            if (task_ids, key) == ("validate_toorder", "toorder_result"):
                return {
                    "matched": False,
                    "mismatched_stores": ["매장A", "매장B"],
                    "store_results": {},
                }
            if (task_ids, key) == ("validate_toorder", "toorder_results_by_date"):
                return {
                    "2026-08-24": {"matched": False, "mismatched_stores": ["매장A"]},
                    "2026-08-25": {"matched": False, "mismatched_stores": ["매장B"]},
                }
            if (task_ids, key) == ("validate_ad_funnel", "ad_funnel_result"):
                return {"still_empty": []}
            return None

        def xcom_push(self, key=None, value=None):
            self.pushed[key] = value

    ti = FakeTI()
    result = upload.trigger_retry_if_needed(
        ti=ti,
        dag_run=SimpleNamespace(run_id="upload__backfill", conf={"handoff_path": str(handoff)}),
    )

    assert result == "Retry DAG 트리거 완료: 신규 2개 / 기존 0개"
    assert [item["conf"]["target_date"] for item in triggered] == ["2026-08-24", "2026-08-25"]
    assert [item["run_id"] for item in triggered] == [
        "retry__20260824__attempt_1__upload__backfill",
        "retry__20260825__attempt_1__upload__backfill",
    ]


def test_upload_validate_retry_includes_date_untrusted_store(tmp_path, monkeypatch):
    handoff = tmp_path / "handoff.json"
    empty_failed = {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}
    handoff.write_text(
        json.dumps(
            {
                "target_date": "2026-08-04",
                "account_list": [{"account_id": "acct-1", "password": "pw1"}],
                "failed": empty_failed,
                "residual_failed": empty_failed,
                "validation": [
                    {"store": "매장B", "reason": "date_filter", "amount_source": "date_filter"}
                ],
                "ad_stores": [],
                "store_info_per_account": [
                    {
                        "account_id": "acct-1",
                        "stores": [{"store_id": "s2", "store": "도리당 매장B"}],
                    }
                ],
                "ingest_stats": {"folders": 1, "cleaned": 1},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    triggered = []
    monkeypatch.setattr(
        "airflow.api.common.trigger_dag.trigger_dag",
        lambda **kwargs: triggered.append(kwargs),
    )

    class FakeTI:
        dag_id = "DB_Beamin_Macro_Upload_Validate_Dags"
        run_id = "upload__test"

        def __init__(self):
            self.pushed = {}

        def xcom_pull(self, task_ids, key):
            if (task_ids, key) == ("validate_toorder", "toorder_result"):
                return {
                    "date_untrusted_stores": ["매장B"],
                    "mismatched_stores": ["매장B"],
                    "source_mismatch_stores": [],
                    "retried_stores": [],
                }
            if (task_ids, key) == ("validate_ad_funnel", "ad_funnel_result"):
                return {"still_empty": []}
            return None

        def xcom_push(self, key=None, value=None):
            self.pushed[key] = value

    ti = FakeTI()
    result = upload.trigger_retry_if_needed(
        ti=ti,
        dag_run=SimpleNamespace(run_id="upload__test", conf={"handoff_path": str(handoff)}),
    )

    assert result.startswith("Retry DAG 트리거 완료:")
    retry_conf = triggered[0]["conf"]
    assert retry_conf["failed_orders"] == [
        {"account_id": "acct-1", "stores": [{"store_id": "s2", "store": "도리당 매장B"}]}
    ]


def test_upload_validate_retry_falls_back_to_account_store_hint(tmp_path, monkeypatch):
    handoff = tmp_path / "handoff.json"
    empty_failed = {"accounts": [], "stores": [], "orders": [], "ads": [], "stages": []}
    handoff.write_text(
        json.dumps(
            {
                "target_date": "2026-08-04",
                "account_list": [
                    {"account_id": "acct-1", "password": "pw1", "store_id": "s2", "store_name": "도리당 매장B"}
                ],
                "failed": empty_failed,
                "residual_failed": empty_failed,
                "validation": [],
                "ad_stores": [],
                "store_info_per_account": [],
                "ingest_stats": {"folders": 1, "cleaned": 1},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    triggered = []
    monkeypatch.setattr(
        "airflow.api.common.trigger_dag.trigger_dag",
        lambda **kwargs: triggered.append(kwargs),
    )

    class FakeTI:
        dag_id = "DB_Beamin_Macro_Upload_Validate_Dags"
        run_id = "upload__test"

        def __init__(self):
            self.pushed = {}

        def xcom_pull(self, task_ids, key):
            if (task_ids, key) == ("validate_toorder", "toorder_result"):
                return {
                    "mismatched_stores": ["매장B"],
                    "source_mismatch_stores": [],
                    "store_results": {"매장B": {"baemin": 100, "toorder": 90, "matched": False}},
                }
            if (task_ids, key) == ("validate_ad_funnel", "ad_funnel_result"):
                return {"still_empty": []}
            return None

        def xcom_push(self, key=None, value=None):
            self.pushed[key] = value

    result = upload.trigger_retry_if_needed(
        ti=FakeTI(),
        dag_run=SimpleNamespace(run_id="upload__test", conf={"handoff_path": str(handoff)}),
    )

    assert result.startswith("Retry DAG 트리거 완료:")
    assert triggered[0]["conf"]["failed_orders"] == [
        {"account_id": "acct-1", "stores": [{"store_id": "s2", "brand": "도리당", "store": "매장B"}]}
    ]


def test_validate_toorder_marks_date_untrusted_source_mismatch(tmp_path, monkeypatch):
    handoff = tmp_path / "handoff.json"
    handoff.write_text(
        json.dumps(
            {
                "target_date": "2026-08-04",
                "account_list": [{"account_id": "acct-1", "password": "pw1"}],
                "validation": [{"store": "매장B", "reason": "date_filter"}],
                "store_info_per_account": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(upload, "_save_validate_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        upload,
        "validate_toorder_orders",
        lambda account_list, store_info_per_account, target_date: {
            "matched": False,
            "compared_count": 1,
            "retried_stores": [],
            "mismatched_stores": ["매장B"],
            "source_mismatch_stores": ["매장B"],
            "store_results": {"매장B": {"baemin": 100, "toorder": 90}},
        },
    )

    class FakeTI:
        def __init__(self):
            self.pushed = {}

        def xcom_pull(self, task_ids, key):
            return None

        def xcom_push(self, key=None, value=None):
            self.pushed[key] = value

    ti = FakeTI()
    summary = upload.validate_toorder(
        ti=ti,
        dag_run=SimpleNamespace(conf={"handoff_path": str(handoff)}),
    )

    result = ti.pushed["toorder_result"]
    assert result["date_untrusted_stores"] == ["매장B"]
    assert result["source_mismatch_stores"] == []
    assert "날짜 필터 불신뢰 재시도 대상 1건: 매장B" in summary


def test_main_collect_triggers_exact_export_folder(monkeypatch):
    triggered = []
    monkeypatch.setattr(main_collect, "_MACRO_ROLE", {"range": "상위", "slug": "top"})
    monkeypatch.setattr(
        "airflow.api.common.trigger_dag.trigger_dag",
        lambda **kwargs: triggered.append(kwargs),
    )

    class FakeTI:
        def xcom_pull(self, task_ids, key):
            assert (task_ids, key) == ("export_to_upload_inbox", "upload_inbox_run")
            return "/inbox/manual__top__scheduled__2026-07-23T18_15_00_00_00"

    result = main_collect.trigger_upload_after_export(
        ti=FakeTI(),
        dag_run=SimpleNamespace(
            run_id="scheduled__2026-07-23T18:15:00+00:00",
            conf={"target_date": "2026-07-23"},
        ),
    )

    assert result.startswith("upload DAG 트리거 완료")
    assert triggered == [
        {
            "dag_id": "DB_Beamin_Macro_Upload_Dags",
            "run_id": "collect_export__scheduled__2026-07-23T18_15_00_00_00",
            "conf": {
                "folder_pattern": "manual__top__scheduled__2026-07-23T18_15_00_00_00",
                "skip_if_empty": True,
                "source": "main_top_collect_export",
                "target_date": "2026-07-23",
            },
        }
    ]


def test_main_collect_task_chain_uploads_after_export():
    export = main_collect.dag.get_task("export_to_upload_inbox")
    trigger = main_collect.dag.get_task("trigger_upload_after_export")

    assert export.downstream_task_ids == {"trigger_upload_after_export"}
    assert trigger.downstream_task_ids == {"notify_collection_result"}


def test_bottom_collect_never_triggers_local_upload(monkeypatch):
    triggered = []
    monkeypatch.setattr(main_collect, "_MACRO_ROLE", {"range": "하위", "slug": "bottom"})
    monkeypatch.setattr(
        "airflow.api.common.trigger_dag.trigger_dag",
        lambda **kwargs: triggered.append(kwargs),
    )

    result = main_collect.trigger_upload_after_export(ti=object())

    assert result == "upload 직접 트리거 스킵: role=bottom"
    assert triggered == []
