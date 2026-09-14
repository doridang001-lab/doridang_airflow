"""과거 배민 수집: 일반 정기 수집과 별도의 실행 대기열."""
from pathlib import Path
from airflow import DAG
from modules.transform.utility.workload import build_background_dag

dag = build_background_dag("dags.db.DB_Beamin_Macro_Dags", Path(__file__).stem)

dag.fileloc = __file__
