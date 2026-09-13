"""Airflow 과거 배민 적재 전용 DAG."""
from pathlib import Path
from airflow import DAG
from modules.transform.utility.workload import build_background_dag
dag = build_background_dag("dags.db.DB_Beamin_Macro_Upload_Dags", Path(__file__).stem)
dag.fileloc = __file__
