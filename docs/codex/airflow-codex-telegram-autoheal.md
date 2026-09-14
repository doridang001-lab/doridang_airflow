# Airflow Codex Telegram Auto-Heal

Airflow 실패 알림은 `heal_queue.jsonl`에 적재되고, WSL tmux의 `codex-autoheal` 창이 큐를 처리한 뒤 Telegram으로 완료 또는 실패 결과를 보낸다.

## 작업스케줄러

- 이름: `AirflowCodexTelegramAutoHeal`
- WSL 직접 실행 작업 `AirflowCodexTelegramAutoHealWSL`은 중복 실행 방지를 위해 비활성화한다.
- 예약 작업은 `start_codex_autoheal_wsl_hidden.vbs`를 호출하며, VBS는 PowerShell supervisor가 끝날 때까지 기다린 뒤 종료코드를 그대로 반환한다.
- PowerShell supervisor는 WSL launcher를 최대 60초 기다리고 heartbeat가 없으면 Windows watcher로 자동 전환한다.
- WSL tmux 시작 스크립트: `/mnt/c/airflow/scripts/start_codex_autoheal_wsl.sh`
- 5분 반복 supervisor는 backend와 관계없이 heartbeat가 3분 이내면 즉시 종료한다.

작업스케줄러의 최근 결과는 supervisor 실행 결과다. WSL과 Windows watcher가 모두 실패하면 비정상 종료하며 Telegram 장애 알림을 시도한다. 실제 감시 동작 여부는 heartbeat와 queue claim 상태를 함께 판단한다.

## tmux 창

- `codex-autoheal`: Airflow task 실패 queue 처리

`codex-autoheal`은 런타임 소스가 아니라 `CODEX_WORKDIR` 워크트리에서 Codex를 실행한다. 기본값은 `/mnt/c/tmp/airflow-autoheal`이다. 런타임 소스 경로는 `AIRFLOW_RUNTIME_WORKDIR=/mnt/c/airflow`로 프롬프트에만 전달한다.

## 로그와 상태 파일

- 시작 로그: `C:\airflow\logs\codex_autoheal_windows.log`
- watcher 로그: `/tmp/codex_autoheal_queue.log`
- queue: `C:\airflow\logs\heal_queue.jsonl`
- task state: `C:\airflow\logs\heal_task_state.json`
- heartbeat: `C:\airflow\logs\autoheal_heartbeat.json`

정상 기준:

- `autoheal_heartbeat.json`의 `ts`가 최근 1~2분 이내로 갱신된다.
- heartbeat의 `backend`가 `wsl`이면 WSL tmux `doridang_ops` 세션에 `codex-autoheal` 창이 존재한다.
- heartbeat의 `backend`가 `windows`이면 Windows fallback watcher PID가 기록된다.
- Windows fallback 사용 중에는 supervisor가 watcher 종료를 기다리므로 작업 스케줄러 상태가 `Running`인 것이 정상이다.
- `heal_queue.jsonl`의 최신 실패 항목에 `claimed_by`가 채워진다.
- 처리 대상이면 `claimed_by=codex`, 생략 대상이면 `autoheal-skip` 또는 전용 생략 값이 기록된다.
- 처리된 항목은 `result_status`와 `result_finished_at`을 가지며 Telegram 성공 시 `result_reported_at`도 기록된다.

## 수동 확인 명령

```powershell
Get-Content -Path C:\airflow\logs\autoheal_heartbeat.json -Encoding utf8
python -X utf8 -c "import json; from pathlib import Path; rows=[json.loads(x) for x in Path(r'C:\airflow\logs\heal_queue.jsonl').read_text(encoding='utf-8').splitlines() if x.strip()]; print([r for r in rows if r.get('claimed_by') is None][-5:])"
Get-ScheduledTask -TaskName AirflowCodexTelegramAutoHeal | Get-ScheduledTaskInfo
wsl.exe --list --verbose
wsl.exe -u myuser bash -lc "tmux list-windows -t doridang_ops"
```

WSL이 불안정할 때만 Windows fallback을 수동 실행한다.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\airflow\scripts\start_autoheal_windows.ps1
```

fallback도 같은 queue를 읽고 같은 `C:\tmp\airflow-autoheal` 워크트리를 사용한다.

## 미실행 태스크 오탐(executor_state_mismatch)

태스크가 **한 번도 실행되지 않았는데** 스케줄러가 강제로 failed 처리하는 두 유형이 있다.
코드 문제가 아니므로 Codex 자동수정 대상이 아니다.

| 유형 | 알림/로그 문구 | 발생 조건 |
| --- | --- | --- |
| 미래 logical_date | `reported that the task instance ... state attribute is queued` / 워커 로그 `'Execution Date' FAILED: ... is in the future` | DagRun 생성 후 컨테이너 시계가 뒤로 보정돼 워커 기준 logical_date가 미래가 됨. cron 스케줄이 있는 DAG에는 `AIRFLOW__SCHEDULER__ALLOW_TRIGGER_IN_FUTURE`가 적용되지 않는다(`allow_future_exec_dates = ALLOW_FUTURE_EXEC_DATES and not timetable.can_be_scheduled`). |
| 큐 적체 | `in queued state for longer than ...` | 스택이 내려가 있는 창(자정 00:00~00:13 KST 재시작)에 큐된 태스크를 아무도 집어가지 못하고 `task_queued_timeout`(600초) 만료. |

공통 판별 시그니처: `task_instance.state='failed' AND start_date IS NULL AND hostname=''`.

처리 경로:

1. `modules/transform/utility/notifier.py`의 `classify_failure`가 `executor_state_mismatch`로 분류한다
   (미실행 문구에 `timed out`이 섞여 있어 **반드시 transient보다 먼저** 검사한다).
2. Telegram은 `[DAG 미실행]`로 나가고 `해결해라` 트리거를 붙이지 않는다 → Codex 세션이 뜨지 않는다.
3. `watch_heal_queue.py`가 `_handle_not_run_entry`에서 해당 TI를 조회해
   `state=failed` **그리고** `start_date`가 비어 있을 때만 `clearTaskInstances`로 재실행한다.
   한 번이라도 실행된 태스크는 절대 clear 하지 않는다. 동일 (dag, task, 분류) 조합은 하루 1회로 제한된다.

`Strategy_ScheduleGuard_01_Overdue_Dags`는 자정 재시작 창에 상시 노출되므로
`retries=1 / retry_delay=2분`으로 스택 복귀 후 스스로 살아나게 해 두었다.

### 진단 SQL

```sql
-- 미실행 실패(두 유형 공통 시그니처)
SELECT date_trunc('day', end_date) d, dag_id, task_id, count(*)
FROM task_instance
WHERE state='failed' AND start_date IS NULL AND end_date IS NOT NULL
  AND (hostname IS NULL OR hostname='') AND end_date > now() - interval '30 days'
GROUP BY 1,2,3 ORDER BY 1 DESC;

-- 시계 역행(TI가 DagRun보다 먼저 큐됨)
SELECT date_trunc('day', dr.queued_at) d, count(*)
FROM dag_run dr JOIN task_instance ti USING (dag_id, run_id)
WHERE ti.queued_dttm < dr.queued_at - interval '2 seconds'
  AND dr.queued_at > now() - interval '30 days'
GROUP BY 1 ORDER BY 1;
```

재발이 늘면 코드가 아니라 호스트를 본다 — `.tmp/autostart/airflow-autostart.log`의 기동 시각,
PC 자동 재부팅 주기, Docker Desktop의 절전/Resource Saver 및 VM 시계 동기화 설정.
