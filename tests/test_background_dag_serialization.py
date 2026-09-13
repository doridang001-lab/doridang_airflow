"""운영 Airflow 컨테이너에서 실행하는 DAG 로딩·직렬화 회귀 검증."""
import unittest
import os
from pathlib import Path

if os.name == "nt":
    os.environ.setdefault("AIRFLOW_HOME", str(Path(__file__).resolve().parents[1] / ".tmp" / "airflow-test"))


class BackgroundDagSerializationTest(unittest.TestCase):
    def test_background_files_register_only_their_own_dag_and_roundtrip(self):
        import dags
        from airflow.models import DagBag
        from airflow.serialization.serialized_objects import SerializedDAG
        root = Path(next(iter(dags.__path__))) / "db"
        paths = sorted(root.glob("DB_Beamin_Macro_Backfill*Dags.py"))
        paths.append(root / "DB_UnifiedSales_Nightly_Dags.py")
        self.assertEqual(len(paths), 5)
        for path in paths:
            with self.subTest(dag=path.stem):
                bag = DagBag(str(path), include_examples=False)
                self.assertEqual(bag.import_errors, {})
                self.assertEqual(set(bag.dags), {path.stem})
                dag = SerializedDAG.from_dict(SerializedDAG.to_dict(bag.dags[path.stem]))
                self.assertEqual(dag.max_active_runs, 1)
                self.assertEqual(dag.max_active_tasks, 1)
                self.assertTrue(dag.tasks)
                for task in dag.tasks:
                    self.assertEqual(task.queue, "history")
                    self.assertEqual(task.dag_id, path.stem)
                    self.assertFalse(isinstance(task.weight_rule, str))


if __name__ == "__main__":
    unittest.main()
