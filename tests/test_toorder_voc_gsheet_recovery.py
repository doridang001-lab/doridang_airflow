from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

import httplib2

from modules.load import load_gsheet


class RetryTests(unittest.TestCase):
    def test_execute_with_retry_retries_transient_http_errors(self):
        attempts = []

        def make_http_error(status: int):
            response = httplib2.Response({"status": str(status)})
            return load_gsheet.HttpError(response, b"boom")

        class Request:
            def __init__(self, effect):
                self.effect = effect

            def execute(self):
                attempts.append("execute")
                if isinstance(self.effect, Exception):
                    raise self.effect
                return self.effect

        effects = iter([make_http_error(503), make_http_error(500), {"ok": True}])

        with mock.patch.object(load_gsheet.time, "sleep"), mock.patch.object(load_gsheet.random, "uniform", return_value=0):
            result = load_gsheet._execute_with_retry(
                lambda: Request(next(effects)),
                action="unit_test",
                sheet_name="retry-sheet",
            )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(len(attempts), 3)

    def test_execute_with_retry_does_not_retry_non_retryable_http_errors(self):
        attempts = []
        response = httplib2.Response({"status": "403"})
        error = load_gsheet.HttpError(response, b"forbidden")

        class Request:
            def execute(self):
                attempts.append("execute")
                raise error

        with mock.patch.object(load_gsheet.time, "sleep") as sleep_mock:
            with self.assertRaises(load_gsheet.HttpError):
                load_gsheet._execute_with_retry(
                    lambda: Request(),
                    action="unit_test",
                    sheet_name="forbidden-sheet",
                )

        self.assertEqual(len(attempts), 1)
        sleep_mock.assert_not_called()


class FailureCallbackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.modules.setdefault("ollama", types.SimpleNamespace())

        class FakeDAG:
            def __init__(self, *args, **kwargs):
                self.dag_id = kwargs.get("dag_id", "test_dag")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeOperator:
            def __init__(self, *args, **kwargs):
                self.task_id = kwargs.get("task_id", "fake_task")

            def __rshift__(self, other):
                return other

            def __rrshift__(self, other):
                return self

        fake_airflow = types.ModuleType("airflow")
        fake_airflow.DAG = FakeDAG
        sys.modules["airflow"] = fake_airflow

        fake_python_module = types.ModuleType("airflow.operators.python")
        fake_python_module.PythonOperator = FakeOperator
        sys.modules["airflow.operators.python"] = fake_python_module

        fake_sensor_module = types.ModuleType("airflow.sensors.external_task")
        fake_sensor_module.ExternalTaskSensor = FakeOperator
        sys.modules["airflow.sensors.external_task"] = fake_sensor_module

        fake_pipeline = types.ModuleType("modules.transform.pipelines.strategy.SMP_crawling_toorder_voc_02_trans")
        for name in [
            "load_toorder_voc_df",
            "load_toorder_voc_upload_temp_df",
            "voc_df_store_summary_preprocess_df",
            "voc_df_store_topic_summary_preprocess_df",
            "review_df_preprocess_df",
            "move_processed_voc_files",
            "upload_store_summary_to_gsheet",
            "upload_topic_summary_to_gsheet",
            "upload_review_summary_to_gsheet",
        ]:
            setattr(fake_pipeline, name, lambda *args, **kwargs: None)
        sys.modules["modules.transform.pipelines.strategy.SMP_crawling_toorder_voc_02_trans"] = fake_pipeline

        fake_io = types.ModuleType("modules.transform.utility.io")
        fake_io.SMP_TOORDER_VOC_TIME = "0 0 * * *"
        sys.modules["modules.transform.utility.io"] = fake_io

        module_path = Path(r"C:\airflow\dags\strategy\Strategy_ToOrderVoc_02_Transform_Dags.py")
        spec = importlib.util.spec_from_file_location("toorder_voc_02_dag_test", module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        cls.dag_module = module

    def test_failure_callback_sends_only_first_and_final_notifications(self):
        sent = []

        def fake_send_email(**kwargs):
            sent.append(kwargs)

        ti = types.SimpleNamespace(
            dag_id="Strategy_ToOrderVoc_02_Transform_Dags",
            task_id="upload_topic_summary_to_gsheet",
            log_url="http://example/log",
        )
        task = types.SimpleNamespace(task_id="upload_topic_summary_to_gsheet", retries=3)

        with mock.patch.object(self.dag_module, "send_email", side_effect=fake_send_email):
            for try_number in (1, 2, 4):
                ti.try_number = try_number
                context = {
                    "ti": ti,
                    "task": task,
                    "run_id": "manual__2026-05-11T00:42:00+00:00",
                    "logical_date": "2026-05-11T00:42:00+00:00",
                    "exception": RuntimeError("boom"),
                    "dag": types.SimpleNamespace(dag_id=ti.dag_id),
                }
                self.dag_module._on_task_failure(context)

        self.assertEqual(len(sent), 2)
        self.assertIn("FIRST FAIL", sent[0]["subject"])
        self.assertIn("FINAL FAIL", sent[1]["subject"])


if __name__ == "__main__":
    unittest.main()
