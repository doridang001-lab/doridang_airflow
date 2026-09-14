"""메모리를 고갈시키지 않고 실행 제한 및 복구 경계를 검증한다."""
import pytest
import os
from pathlib import Path
os.environ.setdefault("AIRFLOW_HOME", str(Path(__file__).resolve().parents[1] / ".tmp" / "airflow-test"))
from modules.transform.utility.workload import memory_transition, retry_dag_id, BACKGROUND_RETRY_DAG
from modules.transform.utility.safe_recovery import claim_key, memory_failure


@pytest.mark.parametrize('available,expected', [(None,'history_paused'), (0,'all_paused'),
    (1.999,'all_paused'),(2,'history_paused'),(3.999,'history_paused'),(4,'normal'),(6,'normal')])
def test_pressure_boundary(available, expected):
    assert memory_transition({}, available)['mode'] == expected


def test_recovery_requires_five_good_samples():
    state={'mode':'all_paused'}
    for _ in range(4):
        state=memory_transition(state, 7)
        assert state['mode']=='all_paused'
    assert memory_transition(state, 7)['mode']=='normal'


def test_bad_sample_resets_recovery():
    state=memory_transition({'mode':'all_paused','healthy_samples':4},5.9)
    assert state['mode']=='all_paused'
    assert state['healthy_samples']==0


def test_unknown_does_not_release_critical_limit():
    assert memory_transition({'mode':'all_paused'},None)['mode']=='all_paused'


def test_normal_samples_enable_bounded_recovery():
    state={}
    for _ in range(10): state=memory_transition(state,8)
    assert state['healthy_samples']==5


def test_policy_does_not_mutate_input():
    old={'mode':'normal','saved_pool_slots':{'default_pool':128}}
    memory_transition(old,0)
    assert old['mode']=='normal'


def test_history_retry_stays_in_history():
    assert retry_dag_id({'workload':'history'})==BACKGROUND_RETRY_DAG
    assert retry_dag_id({},BACKGROUND_RETRY_DAG)==BACKGROUND_RETRY_DAG
    assert retry_dag_id({})=='DB_Beamin_Macro_Dags_Retry'


def test_recovery_claim_separates_mapped_batches():
    assert claim_key('dag','run','task',0)!=claim_key('dag','run','task',1)
    assert claim_key('dag','run','task')==claim_key('dag','run','task',-1)


def test_only_memory_evidence_is_recoverable():
    assert memory_failure('OSError: [Errno 12] Cannot allocate memory')
    assert not memory_failure('ValueError: invalid input')
    assert not memory_failure('Task timed out')


@pytest.mark.parametrize('hour,allowed', [(6, True), (7, False), (21, False), (22, True)])
def test_nightly_window(hour, allowed):
    import pendulum
    from modules.transform.pipelines.db.DB_UnifiedSales_nightly import in_night_window
    assert in_night_window(pendulum.datetime(2026, 9, 9, hour, tz='Asia/Seoul')) is allowed


def test_checkpoint_resume_and_failed_unit_is_not_completed(monkeypatch):
    from modules.transform.pipelines.db import DB_UnifiedSales_nightly as nightly
    from modules.transform.utility import workload
    state = {'done': []}
    writes = []
    calls = []
    monkeypatch.setattr(nightly, '_persist', lambda s: writes.append(list(s['done'])))
    monkeypatch.setattr(nightly, 'in_night_window', lambda: True)
    monkeypatch.setattr(workload, 'background_ready', lambda: True)
    def collect(day, **kwargs):
        calls.append(day)
        if day == '2026-09-02':
            raise ValueError('원천 오류')
        return day
    token = nightly._progress.set(state)
    try:
        nightly.backfill_unit(collect, '2026-09-01', stores=['매장'])
        nightly.backfill_unit(collect, '2026-09-01', stores=['매장'])
        with pytest.raises(nightly.FailedNightly):
            nightly.backfill_unit(collect, '2026-09-02', stores=['매장'])
        assert calls == ['2026-09-01', '2026-09-02']
        assert len(state['done']) == len(writes) == 1
        monkeypatch.setattr(workload, 'background_ready', lambda: False)
        with pytest.raises(nightly.YieldNightly):
            nightly.backfill_unit(collect, '2026-09-03', stores=['매장'])
        assert len(calls) == 2
    finally:
        nightly._progress.reset(token)


@pytest.mark.parametrize("dag_id", [
    "DB_Beamin_Macro_Backfill_Dags", "DB_Beamin_Macro_Backfill_Retry_Dags",
    "DB_Beamin_Macro_Backfill_Upload_Dags", "DB_Beamin_Macro_Backfill_Validate_Dags",
])
def test_all_backfill_routes_enqueue_without_direct_trigger(monkeypatch, dag_id):
    from unittest.mock import Mock
    from types import SimpleNamespace
    from modules.transform.utility import workload, history_admission
    enqueue = Mock(return_value="deferred")
    direct = Mock(side_effect=AssertionError("직접 생성 금지"))
    monkeypatch.setattr(history_admission, 'enqueue', enqueue)
    parent = SimpleNamespace(dag_id=workload.BACKGROUND_COLLECT_DAG, run_id='parent', conf={})
    assert workload.route_trigger(direct, dag_id=dag_id, run_id='child', conf={},
                                  history_context={'dag_run': parent}) == 'deferred'
    assert enqueue.call_args.args[2]['history_ancestors'] == [
        {'dag_id': parent.dag_id, 'run_id': 'parent'}]
    direct.assert_not_called()


def test_regular_trigger_keeps_original_arguments():
    from unittest.mock import Mock
    from modules.transform.utility.workload import route_trigger
    trigger = Mock(return_value='result')
    assert route_trigger(trigger, dag_id='regular', run_id='run', conf={'a': 1},
                         history_context={}) == 'result'
    trigger.assert_called_once_with(dag_id='regular', run_id='run', conf={'a': 1})


def test_legacy_autoheal_cannot_bypass_backfill_admission(monkeypatch):
    from unittest.mock import Mock
    import watch_heal_queue as watcher
    request = Mock(side_effect=AssertionError('백필 직접 clear 금지'))
    monkeypatch.setattr(watcher, '_request', request)
    assert watcher.clear_task_instance(BACKGROUND_RETRY_DAG, 'cancelled', 'sample') is False
    request.assert_not_called()
