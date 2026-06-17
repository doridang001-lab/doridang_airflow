from dataclasses import dataclass

from modules.transform.pipelines.sales.Sales_ToOrder_Review_collect import t1_prepare, t2_collect, t3_save
from modules.transform.pipelines.db.DB_UnifiedReview import run_review
from modules.transform.utility.paths import TOORDER_REVIEW_ANALYTICS_DIR


@dataclass
class DummyDagRun:
    conf: dict


class DummyTI:
    def __init__(self):
        self.store = {}

    def xcom_push(self, key, value):
        self.store[key] = value

    def xcom_pull(self, task_ids=None, key=None):
        return self.store.get(key)


context = {
    "ti": DummyTI(),
    "run_id": "manual_recollect_20260617",
    "dag_run": DummyDagRun({"start_date": "2026-06-17", "end_date": "2026-06-17", "force_recollect": True}),
}

print("[1] t1_prepare")
print(t1_prepare(**context))
print("[2] t2_collect")
print(t2_collect(**context))
print("[3] t3_save")
print(t3_save(**context))

out_path = TOORDER_REVIEW_ANALYTICS_DIR / "toorder_voc_20260617.parquet"
print("saved_exists:", out_path.exists(), out_path)
print("run unified review:")
print(run_review(mode='date', target_date='2026-06-17'))
