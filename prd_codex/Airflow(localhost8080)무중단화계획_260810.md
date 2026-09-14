# Airflow(localhost8080) 무중단화 계획

## Task

Airflow 웹서버(`http://localhost:8080`)가 2026-08-07 23:59부터 약 57시간 동안 완전히 죽어 있었고 그동안 DAG가 단 한 건도 실행되지 않았다. 원인은 Docker Desktop의 "로그인 시 자동 시작"이 2026-08-07 23:50에 꺼진 것이며(매일 23:59:59 강제 종료 → 00:10 자동 부팅 사이클에서 Docker만 안 뜸), 5분마다 도는 기존 워치독은 장애를 감지하고도 데몬 레벨 복구 수단이 없고 Telegram 알림도 없어 57시간 동안 조용히 실패만 반복했다. 워치독을 데몬 레벨까지 복구하는 "스택 가드"로 승격하고, 부팅 시 확정 기동 작업과 야간 우아한 종료 작업을 추가해 사람 개입 없이 살아남게 만든다.

### 확정된 장애 근거 (재조사 불필요)

| 항목 | 값 |
|---|---|
| 마지막 scheduler 로그 | `logs/scheduler/2026-08-07/` |
| 마지막 Docker backend 로그 | `com.docker.backend.exe.log` → 2026-08-08 00:00:01 |
| Docker electron 로그 | `electron-2026-08-08.log`(0바이트) 이후 없음 → 08-09, 08-10 부팅 시 미실행 |
| `%APPDATA%\Docker\settings-store.json` | `"AutoStart": false`, CreationTime=LastWriteTime=**2026-08-07 23:50:00** |
| `HKCU\...\Explorer\StartupApproved\Run` | `Docker Desktop` = `01 00 00 00` → **DISABLED** (OneDrive/KakaoTalk은 `02` ENABLED) |
| `HKCU\...\CurrentVersion\Run` | `Docker Desktop` = `C:\Program Files\Docker\Docker\Docker Desktop.exe` (따옴표·`-Autostart` 플래그 없음) |
| `com.docker.service` | Stopped / StartType Manual |
| 자동 로그인 | 정상 (explorer.exe 00:10:22 기동) → 로그온 트리거 작업 사용 가능 |
| Docker Desktop 버전 | 4.55.0.213807 |

### 사용자 결정 사항

- 야간 재부팅 사이클(`자동 종료`, 23:59:59 `shutdown /s /f /t 0`)은 **유지**. 대신 **우아한 종료 추가**.
- 복구 강도: **완전 자동 복구 + 알림** (조용히 고치고 끝내지 말 것).

---

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 호스트 스크립트는 `C:\airflow\scripts\` 에 둔다
- 호스트 Python: `C:\airflow\.venv\Scripts\python.exe`
- 호스트 스크립트에서 `modules.*` import 시 `sys.path.insert(0, str(PROJECT_ROOT))` 선행 (아래 Reference Code 참고)
- 런타임 로그/상태 파일: `C:\airflow\.tmp\{작업명}\`
- PowerShell 파일 저장 시 `-Encoding utf8` 명시
- 폴더 구조·기존 파일명 변경 금지
- 작업 스케줄러 등록은 `schtasks.exe` 사용 (멱등: Query → Delete /F → Create)

---

## Files to Create / Modify

### 수정
- `C:\airflow\scripts\airflow_scheduler_watchdog.py` — L0~L2 복구 계층 + Telegram 알림 + `--bootstrap` 플래그 추가 (**파일명 유지**)
- `C:\airflow\scripts\run_airflow_scheduler_watchdog_hidden.vbs` — exit code 전파
- `C:\airflow\.env` — `AIRFLOW_UID=50000` 추가 (선택, 없으면 추가)

### 신규
- `C:\airflow\scripts\repair_docker_autostart.ps1`
- `C:\airflow\scripts\graceful_airflow_shutdown.ps1`
- `C:\airflow\scripts\run_airflow_boot_start_hidden.vbs`
- `C:\airflow\scripts\register_airflow_resilience_tasks.ps1`

### 재사용 (신규 작성 금지)
- `modules/transform/utility/notifier.py` → `send_telegram(text) -> bool`
- `scripts/notify_airflow_space_warning.py` → 중복 억제 마커 패턴
- `scripts/notify_autoheal_supervisor_failure.py` → 메시지 4줄 형식
- `scripts/register_airflow_scheduler_watchdog_task.ps1` → 멱등 작업 등록 패턴

---

## Implementation Steps

### 1. `scripts/airflow_scheduler_watchdog.py` — 스택 가드로 승격

현재 `main()`은 곧바로 `airflow_jobs_check()` → `web_health()`로 들어간다. 그 앞에 데몬 레벨 preflight를 넣어 복구를 4계층으로 만든다.

```
L0  Docker Desktop 자동시작 설정 점검·복구  (repair_docker_autostart.ps1 호출)
L1  Docker 데몬 살아있나?  docker info --format {{.ServerVersion}}
      아니오 → "Docker Desktop.exe" 기동 → 데몬 poll (기본 180s / bootstrap 300s)
L2  컨테이너 다 떠있나?    docker compose ps --format json
      아니오 → docker compose up -d → healthy 대기 (기본 300s / bootstrap 600s)
L3  scheduler heartbeat 정상인가?  ← 기존 로직 그대로 유지
      아니오 → docker compose restart airflow-scheduler
어느 단계든 최종 실패 → notify_stack_down() → exit 1
복구 성공(L0~L2에서 뭔가 고쳤을 때) → notify_stack_recovered() → exit 0
```

추가할 모듈 상수:

```python
DOCKER_DESKTOP_EXE = r"C:\Program Files\Docker\Docker\Docker Desktop.exe"
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yaml"
REPAIR_AUTOSTART_PS1 = PROJECT_ROOT / "scripts" / "repair_docker_autostart.ps1"
NOTIFY_STATE_DIR = PROJECT_ROOT / ".tmp" / "scheduler_watchdog"
NOTIFY_COOLDOWN_SECONDS = 3600
```

추가할 함수 (기존 `run_cmd()` / `log_result()` 재사용):

| 함수 | 시그니처 | 역할 |
|---|---|---|
| `repair_docker_autostart()` | `-> bool` | `powershell -NoProfile -ExecutionPolicy Bypass -File repair_docker_autostart.ps1` 실행. stdout에 `CHANGED`가 있으면 True(변경됨) 반환 |
| `docker_daemon_ready()` | `-> bool` | `docker info --format {{.ServerVersion}}` returncode==0 |
| `start_docker_desktop(timeout)` | `(int) -> bool` | `subprocess.Popen([DOCKER_DESKTOP_EXE])` 후 5초 간격으로 `docker_daemon_ready()` poll |
| `stack_containers_up()` | `-> bool` | `docker compose -f COMPOSE_FILE ps --format json` → 서비스 전부 `State == "running"` 인지 |
| `ensure_stack_up(timeout)` | `(int) -> bool` | `docker compose -f COMPOSE_FILE up -d` 후 `stack_containers_up()` + `web_health()` poll |
| `_notify(prefix, lines, cooldown_key)` | `(str, list[str], str) -> bool` | 쿨다운 마커 확인 → `send_telegram()` → 마커 갱신 |
| `notify_stack_down(stage, detail)` | `(str, str) -> bool` | `[Airflow 스택 장애]` 메시지 |
| `notify_stack_recovered(actions)` | `(list[str]) -> bool` | `[Airflow 스택 자동복구]` 메시지, 쿨다운 없음 |

**Telegram 메시지 형식** (`notify_autoheal_supervisor_failure.py` 4줄 구조 준수):

```python
message = (
    "[Airflow 스택 장애]\n"
    f"단계: {stage}\n"
    f"원인: {detail[:500]}\n"
    "다음조치: Docker Desktop 수동 기동 후 docker compose up -d"
)
```

```python
message = (
    "[Airflow 스택 자동복구]\n"
    "상태: 정상화 완료\n"
    f"조치: {' / '.join(actions)}\n"
    "다음조치: 없음 (자동 복구됨)"
)
```

> `notifier._TELEGRAM_SUPPRESS_PREFIXES`에 `[Airflow 스택 …]`은 없으므로 두 메시지 모두 정상 발송된다. **접두어를 임의로 바꾸지 말 것** — `[DAG 완료]`, `[Airflow 스케줄 미생성 보정]` 등으로 시작하면 정책상 억제된다.

**쿨다운 마커**: `.tmp/scheduler_watchdog/stack_down_{stage}.sent` 에 마지막 발송 ISO 시각을 기록하고, 3600초 이내 재발송을 억제한다. (`notify_airflow_space_warning.py`는 일 1회 마커지만, 스택 다운은 1시간 1회로 한다.)

**CLI 인자 추가**:

```python
parser.add_argument("--bootstrap", action="store_true",
                    help="부팅 직후 모드: 대기 타임아웃 연장, heartbeat stale 판정 생략")
```

`--bootstrap` 시 동작 차이:
- 데몬 대기 180s → **300s**
- healthy 대기 300s → **600s**
- L3 heartbeat stale 판정 **생략** (막 올라온 scheduler는 heartbeat가 없는 게 정상)
- L2까지 성공하면 exit 0

**주의**: 기존 `main()`의 L3 로직(`airflow_jobs_check`, `web_health`, `heartbeat_age_seconds`, `restart_scheduler`, `wait_until_healthy`, `list_import_errors`)은 **그대로 둔다**. 앞단에 L0~L2만 삽입한다.

### 2. `scripts/repair_docker_autostart.ps1` (신규)

이번 장애의 직접 원인을 겨냥. 세 지점을 점검하고 어긋나면 되돌린다. **변경이 발생하면 stdout에 `CHANGED: {항목}` 을 출력**한다(가드가 이 문자열로 판정).

```powershell
# 1) settings-store.json
$settings = "$env:APPDATA\Docker\settings-store.json"
# JSON 파싱 → $json.AutoStart -eq $false 이면 $true 로 수정 → 다른 키 보존하여 -Encoding utf8 저장
# ConvertFrom-Json 은 PSCustomObject 반환 (-AsHashtable 없음, Windows PowerShell 5.1)
# 저장: $json | ConvertTo-Json -Depth 10 | Out-File $settings -Encoding utf8

# 2) StartupApproved (첫 바이트 짝수 = 활성, 홀수 = 비활성)
$sa = "HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\StartupApproved\Run"
# (Get-ItemProperty $sa)."Docker Desktop"[0] 이 짝수가 아니면
# Set-ItemProperty $sa -Name "Docker Desktop" -Value ([byte[]](2,0,0,0,0,0,0,0,0,0,0,0))

# 3) Run 키 값
$run = "HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Run"
$want = '"C:\Program Files\Docker\Docker\Docker Desktop.exe" -Autostart'
# 현재 값이 $want 와 다르면 Set-ItemProperty 로 교정
```

- 로그: `C:\airflow\.tmp\scheduler_watchdog\docker_autostart_repair.log` (타임스탬프 + 변경 내역, Add-Content -Encoding utf8)
- 변경이 하나라도 있으면 exit 0 + `CHANGED:` 출력, 변경 없으면 exit 0 + `OK` 출력
- 예외 발생 시 exit 1

### 3. `scripts/run_airflow_boot_start_hidden.vbs` (신규)

`run_airflow_scheduler_watchdog_hidden.vbs`와 동일 구조이되 `--bootstrap` 인자 + **exit code 전파**.

```vbs
Option Explicit

Dim shell, rc
Set shell = CreateObject("WScript.Shell")

shell.CurrentDirectory = "C:\airflow"
rc = shell.Run("cmd.exe /c cd /d ""C:\airflow"" && ""C:\airflow\.venv\Scripts\python.exe"" ""C:\airflow\scripts\airflow_scheduler_watchdog.py"" --bootstrap", 0, True)
WScript.Quit rc
```

### 4. `scripts/run_airflow_scheduler_watchdog_hidden.vbs` (수정)

현재는 `shell.Run(...)`의 반환값을 버려서 Python exit_code=1이 삼켜지고 작업 스케줄러 `LastTaskResult`가 항상 0으로 보인다. 반환값을 받아 `WScript.Quit rc`로 전파한다.

```vbs
Option Explicit

Dim shell, rc
Set shell = CreateObject("WScript.Shell")

shell.CurrentDirectory = "C:\airflow"
rc = shell.Run("cmd.exe /c cd /d ""C:\airflow"" && ""C:\airflow\.venv\Scripts\python.exe"" ""C:\airflow\scripts\airflow_scheduler_watchdog.py""", 0, True)
WScript.Quit rc
```

### 5. `scripts/graceful_airflow_shutdown.ps1` (신규)

야간 강제 종료(`shutdown /s /f`) 9분 전에 실행해 VHDX 손상 위험을 없앤다.

1. `docker compose -f C:\airflow\docker-compose.yaml stop -t 60`
2. Docker Desktop 종료 — `Stop-Process -Name "Docker Desktop" -Force`, 이어서 `com.docker.backend` 정리 (없으면 무시: `try { ... -ErrorAction Stop } catch {}`)
3. `wsl --shutdown`
4. 각 단계 결과를 `C:\airflow\.tmp\airflow_graceful_stop\stop.log`에 타임스탬프와 함께 기록
5. Docker 데몬이 이미 죽어 있으면 1단계를 건너뛰고 3단계로 진행 (실패로 취급하지 않음)

### 6. `scripts/register_airflow_resilience_tasks.ps1` (신규)

`register_airflow_scheduler_watchdog_task.ps1`의 멱등 패턴(Query → Delete /F → Create)을 그대로 따른다.

| 작업명 | 스케줄 | 실행 |
|---|---|---|
| `DoridangAirflowBootStart` | 로그온 + 90초 지연 | `wscript.exe "C:\airflow\scripts\run_airflow_boot_start_hidden.vbs"` |
| `DoridangAirflowSchedulerWatchdog` | 5분 (기존 재등록) | `wscript.exe "C:\airflow\scripts\run_airflow_scheduler_watchdog_hidden.vbs"` |
| `DoridangAirflowGracefulStop` | 매일 23:50 | `powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\airflow\scripts\graceful_airflow_shutdown.ps1"` |

```powershell
# 로그온 트리거 + 지연
schtasks.exe /Create /TN "DoridangAirflowBootStart" /SC ONLOGON /DELAY 0001:30 /TR $bootRun /F
# 5분 주기
schtasks.exe /Create /TN "DoridangAirflowSchedulerWatchdog" /SC MINUTE /MO 5 /TR $watchdogRun /F
# 매일 23:50
schtasks.exe /Create /TN "DoridangAirflowGracefulStop" /SC DAILY /ST 23:50 /TR $stopRun /F
```

기존 `자동 종료` 작업(23:59:59)은 **건드리지 않는다**.

### 7. `.env` — `AIRFLOW_UID=50000` (선택)

`docker compose` 호출마다 `warning msg="The \"AIRFLOW_UID\" variable is not set."`가 뜨며 로그에서 실제 에러를 가린다. `.env`에 없으면 추가.

---

## Reference Code

### scripts/airflow_scheduler_watchdog.py (수정 대상 — 현재 구조)

```python
PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / ".tmp" / "scheduler_watchdog"
SCHEDULER_SERVICE = "airflow-scheduler"
SCHEDULER_CONTAINER = "airflow-airflow-scheduler-1"
WEBSERVER_CONTAINER = "airflow-airflow-webserver-1"
AIRFLOW_BIN = "/home/airflow/.local/bin/airflow"

logger = logging.getLogger(__name__)

def run_cmd(args: list[str], timeout: int = 60) -> subprocess.CompletedProcess[str]:
    logger.info("실행: %s", " ".join(args))
    return subprocess.run(args, cwd=PROJECT_ROOT, capture_output=True,
                          encoding="utf-8", errors="replace", text=True,
                          timeout=timeout, check=False)

def log_result(result: subprocess.CompletedProcess[str]) -> None: ...
def airflow_jobs_check() -> bool: ...            # docker exec ... airflow jobs check
def web_health() -> tuple[bool, dict]: ...       # docker exec ... curl localhost:8080/health
def heartbeat_age_seconds(payload: dict) -> float | None: ...
def restart_scheduler() -> bool: ...             # docker compose restart airflow-scheduler
def wait_until_healthy(wait_seconds: int, poll_seconds: int) -> bool: ...
def list_import_errors() -> bool: ...

def main() -> int:
    # --stale-minutes 10 / --wait-seconds 180 / --poll-seconds 20 / --dry-run / --console
    setup_logging()
    jobs_ok = airflow_jobs_check()          # ← 여기 앞에 L0~L2 삽입
    health_ok, payload = web_health()
    ...
```

### modules/transform/utility/notifier.py (재사용)

```python
_TELEGRAM_ALLOW_PREFIXES = ("[DAG 실패]", "[Airflow 실패]", "[Auto-Heal]", ...)
_TELEGRAM_SUPPRESS_PREFIXES = ("[DAG 완료]", "[DAG 성공]", "[Airflow 스케줄 미생성 보정]", ...)

def _should_send_telegram(text: str) -> bool:
    normalized = str(text or "").lstrip()
    if normalized.startswith(_TELEGRAM_ALLOW_PREFIXES):
        return True
    if normalized.startswith(_TELEGRAM_SUPPRESS_PREFIXES):
        return False
    return True

def send_telegram(text: str) -> bool:
    if not _should_send_telegram(text):
        logger.info("Telegram suppressed by policy: %s", str(text or "").splitlines()[:1])
        return True
    token, chat_id = _get_telegram_creds()   # Airflow Variable → 없으면 os.getenv
    if not token or not chat_id:
        logger.warning("Telegram credentials missing; skip send")
        return False
    try:
        payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=10):
            pass
        logger.info("Telegram alert sent")
        return True
    except Exception as e:
        logger.warning("Telegram send failed (ignored): %s", e)
        return False
```

### scripts/notify_airflow_space_warning.py (마커 패턴 + sys.path 패턴)

```python
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.transform.utility.notifier import send_telegram

STATE_DIR = PROJECT_ROOT / ".tmp" / "airflow_space_cleanup"

def notify(free_gb: float, threshold_gb: float, log_path: str = "") -> bool:
    today = datetime.now().strftime("%Y%m%d")
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    marker = STATE_DIR / f"low_space_warning_{today}.sent"
    if marker.exists():
        print(f"already sent today: {marker}")
        return True
    ...
    sent = bool(send_telegram(message))
    if sent:
        marker.write_text(datetime.now().isoformat(timespec="seconds"), encoding="utf-8")
```

### scripts/notify_autoheal_supervisor_failure.py (메시지 4줄 형식)

```python
message = (
    "[자동복구 감시기 장애]\n"
    "상태: WSL/Windows watcher 시작 실패\n"
    f"원인: {reason[:500]}\n"
    "다음조치: 작업 스케줄러와 autoheal heartbeat 수동 확인"
)
```

### scripts/register_airflow_scheduler_watchdog_task.ps1 (멱등 등록 패턴)

```powershell
$Exists = $false
try {
    & schtasks.exe /Query /TN $TaskName *> $null
    $Exists = ($LASTEXITCODE -eq 0)
} catch { $Exists = $false }

if ($Exists) { & schtasks.exe /Delete /TN $TaskName /F | Out-Null }

& schtasks.exe /Create /TN $TaskName /SC MINUTE /MO $IntervalMinutes /TR $TaskRun /F | Out-Null
& schtasks.exe /Query /TN $TaskName /FO LIST
```

### scripts/run_airflow_scheduler_watchdog_hidden.vbs (수정 전 — exit code 유실)

```vbs
Option Explicit

Dim shell
Set shell = CreateObject("WScript.Shell")

shell.CurrentDirectory = "C:\airflow"
shell.Run "cmd.exe /c cd /d ""C:\airflow"" && ""C:\airflow\.venv\Scripts\python.exe"" ""C:\airflow\scripts\airflow_scheduler_watchdog.py""", 0, True
```

---

## Test Cases

> **Step 0 (선행, 수동)**: 구현 전에 먼저 서비스를 살린다.
> `repair_docker_autostart.ps1` 구현 직후 1회 실행 → `& "C:\Program Files\Docker\Docker\Docker Desktop.exe"` → 데몬 대기 → `docker compose -f C:\airflow\docker-compose.yaml up -d` → `http://localhost:8080/health` 확인.

1. **[구문] Python 문법**
   `C:\airflow\.venv\Scripts\python.exe -m py_compile C:\airflow\scripts\airflow_scheduler_watchdog.py`
   → 기대: 출력 없음, exit 0

2. **[구문] PowerShell 파싱**
   ```powershell
   foreach($f in @("repair_docker_autostart.ps1","graceful_airflow_shutdown.ps1","register_airflow_resilience_tasks.ps1")){
     $p="C:\airflow\scripts\$f"
     [System.Management.Automation.PSParser]::Tokenize((Get-Content $p -Raw),[ref]$null) | Out-Null
     "$f OK"
   }
   ```
   → 기대: 3개 파일 모두 `OK`, 파싱 에러 없음

3. **[정상 상태] 가드 dry-run** (Docker 살아있는 상태)
   `C:\airflow\.venv\Scripts\python.exe C:\airflow\scripts\airflow_scheduler_watchdog.py --console --dry-run`
   → 기대: `판정: jobs_ok=True health_ok=True stale=False` / `scheduler 정상` / exit 0 / Telegram 미발송

4. **[L0] 자동시작 설정 복구**
   ```powershell
   Set-ItemProperty "HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\StartupApproved\Run" -Name "Docker Desktop" -Value ([byte[]](1,0,0,0,0,0,0,0,0,0,0,0))
   powershell -NoProfile -ExecutionPolicy Bypass -File C:\airflow\scripts\repair_docker_autostart.ps1
   (Get-ItemProperty "HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\StartupApproved\Run")."Docker Desktop"[0]
   (Get-Content "$env:APPDATA\Docker\settings-store.json" -Raw | ConvertFrom-Json).AutoStart
   (Get-ItemProperty "HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Run")."Docker Desktop"
   ```
   → 기대: stdout에 `CHANGED:` 포함 / 첫 바이트 `2` / `AutoStart` = `True` / Run 값 = `"C:\Program Files\Docker\Docker\Docker Desktop.exe" -Autostart`
   → 재실행 시: `OK` 출력, `CHANGED:` 없음 (멱등)

5. **[L1+L2] 데몬 다운 전체 복구 — 이번 장애 재현 (핵심)**
   ```powershell
   try { Stop-Process -Name "Docker Desktop","com.docker.backend" -Force -ErrorAction Stop } catch {}
   Set-ItemProperty "HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\StartupApproved\Run" -Name "Docker Desktop" -Value ([byte[]](1,0,0,0,0,0,0,0,0,0,0,0))
   C:\airflow\.venv\Scripts\python.exe C:\airflow\scripts\airflow_scheduler_watchdog.py --console
   ```
   → 기대 로그 순서: `L0 autostart 복구` → `L1 Docker 데몬 없음, Docker Desktop 기동` → `L2 compose up -d` → `L3 healthy`
   → StartupApproved 첫 바이트 `2` 복원 / `settings-store.json` `AutoStart` = `True`
   → Telegram `[Airflow 스택 자동복구]` 1건 수신
   → `docker exec airflow-airflow-webserver-1 curl -s http://localhost:8080/health` → `"status": "healthy"`
   → exit 0

6. **[알림] 복구 불가 시 Telegram + exit 1**
   `airflow_scheduler_watchdog.py`의 `DOCKER_DESKTOP_EXE`를 존재하지 않는 경로로 임시 변경 → Docker 종료 → 가드 실행
   → 기대: Telegram `[Airflow 스택 장애]` 수신 / exit 1 / 원래 경로로 되돌릴 것

7. **[쿨다운] 중복 알림 억제**
   6번 상태에서 가드를 연속 2회 실행
   → 기대: 2회차는 Telegram 미발송, 로그에 쿨다운 스킵 기록, `.tmp/scheduler_watchdog/stack_down_*.sent` 존재

8. **[exit code 전파] VBS 은폐 제거**
   ```powershell
   wscript.exe "C:\airflow\scripts\run_airflow_scheduler_watchdog_hidden.vbs"
   $LASTEXITCODE
   ```
   → 기대: 정상 상태에서 `0`, 6번 장애 상태에서 `1` (기존엔 실패해도 항상 0이었음)

9. **[작업 등록] 멱등성**
   ```powershell
   powershell -NoProfile -ExecutionPolicy Bypass -File C:\airflow\scripts\register_airflow_resilience_tasks.ps1
   schtasks /Query /TN "DoridangAirflowBootStart" /FO LIST /V
   schtasks /Query /TN "DoridangAirflowGracefulStop" /FO LIST /V
   schtasks /Query /TN "DoridangAirflowSchedulerWatchdog" /FO LIST /V
   schtasks /Query /TN "자동 종료" /FO LIST
   ```
   → 기대: 3개 작업 등록 성공, 2회 실행해도 중복/에러 없음, `자동 종료`는 23:59:59 그대로 유지

10. **[우아한 종료]**
    ```powershell
    powershell -NoProfile -ExecutionPolicy Bypass -File C:\airflow\scripts\graceful_airflow_shutdown.ps1
    docker compose -f C:\airflow\docker-compose.yaml ps
    wsl -l -v
    Get-Content C:\airflow\.tmp\airflow_graceful_stop\stop.log -Tail 10
    ```
    → 기대: 컨테이너 전부 Exited(또는 목록 없음) / `docker-desktop` Stopped / 로그에 4단계 기록
    → 데몬이 이미 죽은 상태에서 재실행해도 exit 0 (실패 취급 금지)

11. **[부트스트랩]**
    10번 직후 `wscript.exe "C:\airflow\scripts\run_airflow_boot_start_hidden.vbs"` → `$LASTEXITCODE`
    → 기대: exit 0, 5분 내 `http://localhost:8080` 응답, 로그에 `bootstrap` 모드 기록

12. **[최종 실환경] 재부팅 검증**
    `DoridangAirflowGracefulStop` 수동 실행 → 실제 재부팅 → 로그인 후 **방치**
    → 기대: 5분 내 `http://localhost:8080` 접속 가능, `.tmp/scheduler_watchdog/` 로그에 부트스트랩 성공 기록
    → 다음날 `logs/scheduler/{당일날짜}/` 디렉터리 생성 확인 (00:00 종료 → 00:10 부팅 사이클 통과 증명)

---

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~11 순서대로 실행 (12번은 재부팅 필요 — 사용자에게 안내만)
  2. FAIL 항목 → 로그(.tmp/scheduler_watchdog/, .tmp/airflow_graceful_stop/) 확인 후 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 1~11 전체 PASS + Constraints 위반 없음
```

**루프 중 반드시 원복할 것**: 6번에서 바꾼 `DOCKER_DESKTOP_EXE` 경로, 4·5번에서 조작한 StartupApproved 바이트.
최종 상태는 반드시 **Docker 자동시작 활성 + 스택 기동 + 8080 응답**이어야 한다.

---

## Constraints

- `scripts/airflow_scheduler_watchdog.py`의 **파일명을 바꾸지 말 것**. 기존 작업 스케줄러 `DoridangAirflowSchedulerWatchdog`와 `run_airflow_scheduler_watchdog_hidden.vbs`가 이 경로를 참조한다.
- 기존 L3 로직(`airflow_jobs_check` / `web_health` / `heartbeat_age_seconds` / `restart_scheduler` / `wait_until_healthy` / `list_import_errors`)은 **삭제·개명하지 말 것**. 앞단에 L0~L2만 삽입한다.
- `자동 종료` 작업(23:59:59 `shutdown /s /f /t 0`)은 **삭제·수정하지 말 것**. 사용자가 유지를 명시했다.
- Telegram 메시지 접두어를 `[Airflow 스택 장애]` / `[Airflow 스택 자동복구]` 외의 것으로 바꾸지 말 것 — `notifier._TELEGRAM_SUPPRESS_PREFIXES`에 걸려 조용히 억제될 수 있다.
- Telegram 알림 함수를 새로 만들지 말 것. `modules.transform.utility.notifier.send_telegram`만 사용한다.
- print 금지 — `logger.info/warning/error` 사용. (단 PowerShell 스크립트의 `Write-Host`는 허용)
- Windows PowerShell 5.1 환경: `&&`/`||`/삼항연산자/`??` 사용 불가, `ConvertFrom-Json -AsHashtable` 없음, 파일 저장 시 `-Encoding utf8` 명시.
- 레지스트리 조작은 `HKCU:` PSDrive 접두어 사용 (`HKEY_CURRENT_USER\...` 원시 경로 금지).
- 워치독은 5분마다 도는 작업이다. L0~L2 preflight 때문에 정상 상태에서 실행 시간이 길어지면 안 된다 — 데몬이 살아있으면 `docker info` 1회로 즉시 통과해야 한다.
- 미수집 데이터(08-08 ~ 08-10 3일치) backfill은 **이 태스크 범위 밖**. 스택 복구 후 별도 작업으로 진행한다.

---

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`airflow_scheduler_watchdog.py`는 `from __future__ import annotations` + 전체 타입 힌트 사용 중 → 동일하게)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- 로그 메시지 언어: 한국어 (기존 워치독이 `판정:`, `scheduler 정상` 등 한국어 사용 중)
- 타임아웃 수치가 애매하면: 위 표의 기본값 사용
- PowerShell 스크립트 인코딩: UTF-8 with BOM (기존 `coupang_boot_autostart.ps1`과 동일)
