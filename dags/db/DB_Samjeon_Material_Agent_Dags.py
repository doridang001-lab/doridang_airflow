"""삼전점 대화 서비스에 오전 자료 확인 작업을 전달한다."""
from datetime import timedelta
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from modules.transform.utility.schedule import DB_SAMJEON_MATERIAL_AGENT_TIME


def queue_check(**context):
    if str(Variable.get('SAMJEON_MATERIAL_AGENT_ENABLED', default_var='false')).lower() != 'true':
        return {'queued': False, 'reason': '서비스 작업 전달 비활성'}
    from modules.transform.pipelines.db.samjeon_material_service.pipeline import enqueue
    from modules.transform.pipelines.db.samjeon_material_agent.locations import LOCAL_DB
    run = context['dag_run']
    manual = str(getattr(run, 'run_type', '')).lower() not in ('scheduled', 'dagruntype.scheduled')
    slot = context.get('data_interval_end') or context.get('logical_date')
    now = pendulum.now('Asia/Seoul')
    # 지연 실행은 지난 슬롯을 지금의 슬롯으로 위장하지 않는다.
    if slot is None or slot.in_timezone('Asia/Seoul').date() != now.date() or now.strftime('%H:%M') > '12:00':
        return {'queued': False, 'reason': '오래된 실행 또는 정오 이후'}
    return enqueue(LOCAL_DB / 'samjeon_material_agent', now=slot, manual=manual, conf=run.conf)


with DAG(dag_id=Path(__file__).stem, schedule=DB_SAMJEON_MATERIAL_AGENT_TIME,
         start_date=pendulum.datetime(2026, 10, 12, tz='Asia/Seoul'), catchup=False,
         is_paused_upon_creation=True, max_active_runs=1, dagrun_timeout=timedelta(minutes=5),
         default_args={'retries': 0}, tags=['samjeon', 'agent']) as dag:
    PythonOperator(task_id='queue_check', python_callable=queue_check, execution_timeout=timedelta(minutes=1))
