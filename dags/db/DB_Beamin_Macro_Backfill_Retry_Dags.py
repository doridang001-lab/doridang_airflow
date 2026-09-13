"""과거 배민 재수집 전용 DAG."""
from pathlib import Path
from airflow import DAG
from modules.transform.utility.workload import build_background_dag

dag = build_background_dag("dags.db.DB_Beamin_Macro_Dags_Retry", Path(__file__).stem)

dag.fileloc = __file__
