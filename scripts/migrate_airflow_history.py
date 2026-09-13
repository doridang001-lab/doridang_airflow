"""미실행 과거 요청의 전용 DAG 이전. 기본은 읽기 전용 목록 확인."""
import argparse
import json
from datetime import datetime, timezone


def historical_request(run_id, conf):
    conf = conf or {}
    return (conf.get('workload') == 'history' or 'lookback_recovery__' in run_id
            or 'settlement' in run_id or conf.get('source_dag_id') == 'DB_DeliveryCommission_Dags'
            or conf.get('source') == 'DB_Beamin_Macro_Lookback_Trigger_Dags')


def migrate(apply=False):
    from airflow.models import DagRun, TaskInstance, DagModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.utils.state import DagRunState
    from airflow.utils.session import create_session
    from modules.transform.utility.workload import BACKGROUND_COLLECT_DAG, BACKGROUND_RETRY_DAG
    mapping={'DB_Beamin_Macro_Dags':BACKGROUND_COLLECT_DAG,
             'DB_Beamin_Macro_Dags_Retry':BACKGROUND_RETRY_DAG}
    candidates=[]
    with create_session() as session:
        for run in session.query(DagRun).filter(DagRun.dag_id.in_(mapping),DagRun.state=='queued').all():
            tasks=session.query(TaskInstance).filter_by(dag_id=run.dag_id,run_id=run.run_id).all()
            if (historical_request(run.run_id,run.conf) and
                    all(t.start_date is None and t.state in (None,'scheduled') for t in tasks)):
                candidates.append({'dag_id':run.dag_id,'run_id':run.run_id,'target':mapping[run.dag_id]})
    if not apply:
        return {'candidates':candidates}
    from modules.transform.utility.process_lock import named_lock
    with named_lock('history_migration',timeout=0):
        # 원래 DAG의 신규 시작만 잠시 막는다. 실행 중 프로세스는 그대로 둔다.
        with create_session() as session:
            originals=session.query(DagModel).filter(DagModel.dag_id.in_(mapping)).all()
            paused={m.dag_id:m.is_paused for m in originals}
            for model in originals: model.is_paused=True
        migrated=[]
        try:
            for item in candidates:
                with create_session() as session:
                    run=session.query(DagRun).filter_by(dag_id=item['dag_id'],run_id=item['run_id']).with_for_update().one()
                    tasks=session.query(TaskInstance).filter_by(dag_id=run.dag_id,run_id=run.run_id).all()
                    if run.state!='queued' or any(t.start_date is not None or t.state not in (None,'scheduled') for t in tasks):
                        continue
                    target=session.query(DagRun).filter_by(dag_id=item['target'],run_id=run.run_id).first()
                    conf={**(run.conf or {}),'workload':'history','migrated_from':run.dag_id}
                    if target is None:
                        dag=session.query(SerializedDagModel).filter_by(dag_id=item['target']).one().dag
                        # 새 요청 생성과 기존 요청 취소는 같은 트랜잭션으로 반영한다.
                        dag.create_dagrun(run_id=run.run_id, conf=conf, state=DagRunState.QUEUED,
                            execution_date=run.execution_date, run_type=run.run_type,
                            external_trigger=run.external_trigger,
                            data_interval=(run.data_interval_start,run.data_interval_end), session=session)
                    elif target.conf != conf:
                        raise RuntimeError('이전 대상 conf 불일치: '+run.run_id)
                    # 성공으로 위장하지 않고 취소 사유를 원래 conf에 남긴다.
                    run.conf={**(run.conf or {}),'history_migrated_to':item['target'],
                              'history_migrated_at':datetime.now(timezone.utc).isoformat()}
                    run.set_state('failed')
                    migrated.append(item)
        finally:
            with create_session() as session:
                for dag_id,old in paused.items():
                    session.query(DagModel).filter_by(dag_id=dag_id).one().is_paused=old
        return {'migrated':migrated}


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--apply',action='store_true')
    print(json.dumps(migrate(parser.parse_args().apply),ensure_ascii=False))
