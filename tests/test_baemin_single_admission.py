"""격리 PostgreSQL에서 실제 잠금/트랜잭션/순차 인계를 검증한다.

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN을 baemin_single_test_* DB로 지정하고
HISTORY_ADMISSION_DB_TEST=1 python tests/test_baemin_single_admission.py 로 실행한다.
"""
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch


@unittest.skipUnless(os.environ.get("HISTORY_ADMISSION_DB_TEST") == "1", "격리 PostgreSQL 전용")
class HistoryAdmissionDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from airflow import settings, DAG
        from airflow.models import DagModel
        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.operators.empty import EmptyOperator
        from airflow.utils import timezone
        from airflow.utils.session import create_session
        from modules.transform.utility import history_admission as admission
        if not settings.engine.url.database.startswith("baemin_single_test_"):
            raise RuntimeError("운영 DB에서는 검증할 수 없습니다")
        cls.admission = admission
        cls.dags = {}
        with create_session() as session:
            for dag_id in admission.backfill_dags():
                dag = DAG(dag_id, schedule=None, start_date=timezone.datetime(2024, 1, 1))
                EmptyOperator(task_id="sample", dag=dag)
                session.merge(DagModel(dag_id=dag_id, is_paused=False))
                SerializedDagModel.write_dag(dag, session=session)
                cls.dags[dag_id] = dag

    def setUp(self):
        from airflow.utils.session import create_session
        from sqlalchemy import text
        with create_session() as session:
            session.execute(text("TRUNCATE dag_run, variable, log CASCADE"))
        from modules.transform.utility.workload import (
            BACKGROUND_COLLECT_DAG, BACKGROUND_UPLOAD_DAG,
            BACKGROUND_VALIDATE_DAG, BACKGROUND_RETRY_DAG,
        )
        self.chain = [BACKGROUND_COLLECT_DAG, BACKGROUND_UPLOAD_DAG,
                      BACKGROUND_VALIDATE_DAG, BACKGROUND_RETRY_DAG]
        self.refresh = patch('modules.transform.utility.workload.refresh_lookback_conf', lambda c: c)
        self.refresh.start()
        self.addCleanup(self.refresh.stop)

    def finish(self, dag_id, run_id, state="success", manual=False):
        from airflow.models import DagRun
        from airflow.models.log import Log
        from airflow.utils.session import create_session
        with create_session() as session:
            run = session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).one()
            run.set_state(state)
            if manual:
                session.add(Log(event="dagrun_failed", dag_id=dag_id, run_id=run_id))

    def test_concurrent_producers_and_dispatchers_have_one_run(self):
        a = self.admission
        def produce(i):
            return a.enqueue(self.chain[i % 4], f"request_{i}", {"target_date": f"2026-08-{i+1:02d}"})
        with ThreadPoolExecutor(max_workers=8) as executor:
            self.assertEqual(set(executor.map(produce, range(20))), {"deferred"})
            results = list(executor.map(lambda _: a.dispatch(), range(12)))
        self.assertEqual(sum(r["created"] for r in results), 1)
        self.assertTrue(all(r["active"] == 1 for r in results))
        self.assertEqual(a.dispatch()["deferred_requests"], 19)

    def test_four_stages_wait_for_parent_and_precede_new_collection(self):
        a = self.admission
        a.enqueue(self.chain[0], "stage0", {"target_date": "2026-09-08"})
        self.assertEqual(a.dispatch()["created"], 1)
        conf = {}
        for index in range(1, 4):
            conf = a.with_parent({}, {"dag_run": SimpleNamespace(
                dag_id=self.chain[index-1], run_id=f"stage{index-1}", conf=conf)})
            a.enqueue(self.chain[index], f"stage{index}", conf)
            a.enqueue(self.chain[0], "unrelated", {"target_date": "2026-09-09"})
            self.assertEqual(a.dispatch()["created"], 0)
            self.finish(self.chain[index-1], f"stage{index-1}")
            self.assertEqual(a.dispatch()["created"], 1)
            from airflow.utils.session import create_session
            with create_session() as session:
                self.assertEqual(a.active_runs(session)[0].dag_id, self.chain[index])

    def test_manual_failure_cancels_child_and_cannot_reenqueue(self):
        a = self.admission
        a.enqueue(self.chain[0], "parent", {})
        a.dispatch()
        child = {"history_ancestors": [{"dag_id": self.chain[0], "run_id": "parent"}]}
        a.enqueue(self.chain[1], "child", child)
        self.finish(self.chain[0], "parent", "failed", manual=True)
        self.assertEqual(a.dispatch()["cancelled"], 1)
        self.assertEqual(a.enqueue(self.chain[0], "parent", {}), "cancelled")
        self.assertEqual(a.enqueue(self.chain[1], "child2", child), "cancelled")
        self.assertEqual(a.dispatch()["active"], 0)

    def test_task_failure_allows_planned_retry(self):
        a = self.admission
        a.enqueue(self.chain[0], "parent", {})
        a.dispatch()
        self.finish(self.chain[0], "parent", "failed")
        a.enqueue(self.chain[3], "retry", {"source_dag_id": self.chain[0], "source_run_id": "parent"})
        self.assertEqual(a.dispatch()["created"], 1)

    def test_crash_rolls_back_created_run_and_preserves_request(self):
        a = self.admission
        a.enqueue(self.chain[0], "root", {})
        original = a.create_run
        def crash(session, request):
            original(session, request)
            raise RuntimeError("simulated crash before request removal")
        with patch.object(a, "create_run", crash), self.assertRaises(RuntimeError):
            a.dispatch()
        result = a.dispatch()
        self.assertEqual((result["created"], result["deferred_requests"], result["active"]), (1, 0, 1))

    def test_duplicate_scope_preserves_distinct_range(self):
        a = self.admission
        conf = {"target_date": "2026-09-08", "stores": ["B", "A"], "orders_only": True}
        self.assertEqual(a.enqueue(self.chain[0], "one", conf), "deferred")
        self.assertEqual(a.enqueue(self.chain[0], "two", {**conf, "stores": ["A", "B"]}), "existing")
        self.assertEqual(a.enqueue(self.chain[0], "three", {**conf, "collect_range": "other"}), "deferred")

    def test_migration_preserves_run_identity_and_resumes_only_unstarted(self):
        a = self.admission
        a.enqueue(self.chain[0], "queued", {"target_date": "2026-09-08"})
        a.dispatch()
        self.assertEqual(len(a.defer_unstarted(apply=True)["deferred"]), 1)
        result = a.dispatch()
        self.assertEqual((result["created"], result["active"]), (1, 1))
        from airflow.models import DagRun
        from airflow.utils.session import create_session
        with create_session() as session:
            self.assertEqual(session.query(DagRun).count(), 1)

    def test_cancel_after_migration_never_resumes(self):
        a = self.admission
        a.enqueue(self.chain[0], "queued", {})
        a.dispatch()
        a.defer_unstarted(apply=True)
        self.finish(self.chain[0], "queued", "failed", manual=True)
        result = a.dispatch()
        self.assertEqual((result["cancelled"], result["active"]), (1, 0))

    def test_pause_and_execution_date_options(self):
        from airflow.models import Variable, DagRun
        from airflow.utils import timezone
        from airflow.utils.session import create_session
        a = self.admission
        date = timezone.utcnow()
        a.enqueue(self.chain[0], "dated", {}, execution_date=date, replace_microseconds=False)
        Variable.set(a.PAUSED, True, serialize_json=True)
        self.assertEqual(a.dispatch()["created"], 0)
        Variable.set(a.PAUSED, False, serialize_json=True)
        self.assertEqual(a.dispatch()["created"], 1)
        with create_session() as session:
            self.assertEqual(session.query(DagRun).one().execution_date, date)

    def test_memory_recovery_cannot_bypass_active_stage_or_manual_cancel(self):
        from airflow.models import TaskInstance
        from airflow.utils.session import create_session
        from airflow.utils import timezone
        from modules.transform.utility import safe_recovery
        a = self.admission
        a.enqueue(self.chain[2], "failed_validate", {})
        a.dispatch()
        self.finish(self.chain[2], "failed_validate", "failed")
        with create_session() as session:
            ti = session.query(TaskInstance).filter_by(run_id="failed_validate").one()
            ti.state = "failed"
            ti.end_date = timezone.utcnow()
        a.enqueue(self.chain[0], "active_collect", {})
        a.dispatch()
        policy = {"mode": "normal", "healthy_samples": 5, "started_at": 0}
        with patch.dict(safe_recovery.ALLOW, {self.chain[2]: {"sample"}}), patch.object(
                safe_recovery, "task_log_tail", side_effect=AssertionError("복구 후보로 진행하면 안 됨")):
            self.assertEqual(safe_recovery.recover_memory_failures(policy), [])
            self.finish(self.chain[0], "active_collect")
            self.finish(self.chain[2], "failed_validate", "failed", manual=True)
            self.assertEqual(safe_recovery.recover_memory_failures(policy), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
