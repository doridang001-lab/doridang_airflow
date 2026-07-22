# Airflow Codex Telegram Auto-Heal

Airflow 실패 알림은 `heal_queue.jsonl`에 적재되고, WSL tmux의 `codex-autoheal` 창이 큐를 처리한 뒤 Telegram으로 완료 또는 실패 결과를 보낸다.

## 작업스케줄러

- 이름: `AirflowCodexTelegramAutoHeal`
- WSL 직접 실행 작업: `AirflowCodexTelegramAutoHealWSL`
- 실행: `wsl.exe -u myuser bash /mnt/c/airflow/scripts/start_codex_autoheal_wsl.sh`
- 트리거: 30분 반복
- 기존 wrapper 실행: `powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "C:\airflow\scripts\start_codex_autoheal_hidden.ps1"`
- WSL tmux 시작 스크립트: `/mnt/c/airflow/scripts/start_codex_autoheal_wsl.sh`
- 기존 5분 반복 wrapper는 heartbeat가 최근이고 WSL tmux `codex-autoheal` 창이 있으면 WSL 재시작 없이 즉시 종료한다.

작업스케줄러의 최근 결과 성공은 WSL tmux watcher 시작 스크립트가 정상 종료됐다는 뜻이다. 실제 감시 동작 여부는 heartbeat와 queue claim 상태로 판단한다.

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
- WSL tmux `doridang_ops` 세션에 `codex-autoheal` 창이 존재한다.
- `heal_queue.jsonl`의 최신 실패 항목에 `claimed_by`가 채워진다.
- 처리 대상이면 `claimed_by=codex`, 생략 대상이면 `autoheal-skip` 또는 전용 생략 값이 기록된다.

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
