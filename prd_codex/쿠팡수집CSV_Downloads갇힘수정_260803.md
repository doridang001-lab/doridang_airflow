# 쿠팡수집CSV_Downloads갇힘수정

## Task

쿠팡 수집 CSV 246개(orders 85 / cmg 84 / options 77, 2026-08-02~08-03 생성)가 `C:\Users\민준\Downloads`에 쌓인 채 적재되지 않고 있다. DAG도 작업스케줄러도 정상이며, 원인은 Chrome이 수집 폴더가 아닌 기본 Downloads 폴더로 파일을 저장하는 것이다. Chrome 프로필 Preferences에 다운로드 경로를 영구 고정하고, 밀린 246개를 backfill 적재한다.

### 진단 근거 (조사 완료, 재조사 불필요)

- `DB_CoupangMacro_Load_Dags`는 `DB_COUPANG_MACRO_TIME = "30,50 * * * *"`로 정상 실행 중이며 최근 런 전부 `success`. 컨테이너 안에서 확인한 결과 스캔 대상 두 곳 모두 파일 0개 → `"처리할 파일이 없습니다"`만 반환.
  ```
  DOWN_DIR    /opt/airflow/download          (host: E:\down)        coupangeats_*.csv → 0개
  COLLECT_SRC /opt/airflow/Collect_Data/영업관리부_수집                coupangeats_*.csv → 0개
  ```
- 로더 `_iter_source_files`가 보는 경로는 위 두 곳뿐이다. Downloads는 스캔 범위 밖이고 컨테이너에 마운트조차 되어 있지 않다.
- **근본 원인: 커밋 `6df65b7`(2026-07-22).** `scripts/coupang_host_chrome.ps1`이 전용 프로필 `C:\coupang_chrome_profile` → 실제 `Default` 프로필로 전환됐다. 신규 프로필이면 `--download-default-directory` 스위치가 초기 Preferences에 반영되지만, 기존 프로필은 자체 Preferences가 우선이라 스위치가 무시된다.
- 실측: `%LOCALAPPDATA%\Google\Chrome\User Data\Default\Preferences`(21,816B)에 `download` / `savefile` 키가 **아예 없음**. `HKLM\SOFTWARE\Policies\Google\Chrome`에도 `DownloadDirectory` 없음(하위 키는 `LocalNetworkAccessAllowedForUrls` 뿐) → Chrome 기본값 `%USERPROFILE%\Downloads`로 저장.
- 타임라인 일치: `E:\down` 마지막 쿠팡 산출물 2026-08-01, Downloads 첫 파일 2026-08-02. 프로필 교체 후 Chrome 재시작 시점에 전환됐다.
- `coupang_host_chrome.ps1:88-93`은 9222 포트가 이미 열려 있으면 `exit 0` 하므로, 평소 Chrome이 떠 있는 상태에서는 실행 플래그가 애초에 적용되지도 않는다.
- **범위 밖**: `CoupangAutoCollect` 예약 작업은 Disabled + `LastTaskResult=1`(마지막 실행 2026-07-06)이지만, 수집 자체는 계속 되고 있으므로(파일이 매일 생성됨) 이번 증상과 무관한 별개 이슈. **건드리지 말 것.**

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- PowerShell 스크립트(`scripts/*.ps1`)는 Windows PowerShell 5.1 대상 — `&&`/`||`/삼항연산자/`??` 사용 불가, `;` + `if ($?)` 사용

## Files to Create / Modify

- **수정** `scripts/coupang_host_chrome.ps1` — Chrome 실행 전 프로필 Preferences의 다운로드 경로를 점검·교정하는 단계 추가
- **생성 없음** — Python 코드/DAG 변경 불필요. 로더의 재적재 방어 로직은 이미 충분하다.
- **일회성 작업** — `C:\Users\민준\Downloads\coupangeats_*.csv` 246개를 `E:\down`으로 이동 (커밋 대상 아님)

## Implementation Steps

### 1. `scripts/coupang_host_chrome.ps1` — Preferences 교정 단계 추가

현재 스크립트는 `--download-default-directory=$collectDownloadDir` 플래그만 넘긴다(114행). 기존 프로필에는 무효이므로 이것만 믿으면 안 된다. Chrome 실행 **전에** Preferences를 직접 점검·교정한다.

스크립트에 넣는 이유: 일회성 수동 편집과 달리 버전관리되고, 프로필이 초기화돼도 자가 치유된다.

**배치 위치**: `$collectDownloadDir` 계산부(현재 95-104행) 직후, 9222 포트 중복 체크(현재 88-93행)보다 **앞으로** 옮겨서 배치한다. 포트 체크가 `exit 0`으로 빠져나가기 전에 pref 교정이 실행돼야 한다.

**로직**:

```powershell
function Ensure-ChromeDownloadPref {
    param([string]$UserDataDir, [string]$ProfileDirectory, [string]$TargetDir)

    $prefPath = Join-Path $UserDataDir "$ProfileDirectory\Preferences"
    if (-not (Test-Path $prefPath)) {
        Write-Warning "Preferences 파일 없음, 다운로드 경로 교정 건너뜀: $prefPath"
        return
    }

    try {
        $prefs = Get-Content -Path $prefPath -Raw -Encoding utf8 | ConvertFrom-Json
    } catch {
        Write-Warning "Preferences 파싱 실패, 교정 건너뜀: $($_.Exception.Message)"
        return
    }

    $current = $null
    if ($prefs.PSObject.Properties.Name -contains 'download') { $current = $prefs.download.default_directory }
    if ($current -eq $TargetDir) {
        Write-Host "다운로드 경로 pref 정상: $current" -ForegroundColor Green
        return
    }

    # Chrome 실행 중이면 덮어써도 종료 시 되돌려 쓰므로 쓰지 않는다
    $running = @(Get-Process chrome -ErrorAction SilentlyContinue)
    if ($running.Count -gt 0) {
        Write-Warning "다운로드 경로가 '$current' 로 잘못되어 있으나 Chrome이 실행 중이라 교정할 수 없습니다."
        Write-Warning "Chrome을 완전히 종료한 뒤 이 스크립트를 다시 실행하세요."
        return
    }

    Copy-Item -Path $prefPath -Destination "$prefPath.bak" -Force

    if ($prefs.PSObject.Properties.Name -notcontains 'download') {
        $prefs | Add-Member -NotePropertyName 'download' -NotePropertyValue ([pscustomobject]@{}) -Force
    }
    $prefs.download | Add-Member -NotePropertyName 'default_directory'   -NotePropertyValue $TargetDir -Force
    $prefs.download | Add-Member -NotePropertyName 'prompt_for_download' -NotePropertyValue $false     -Force

    if ($prefs.PSObject.Properties.Name -notcontains 'savefile') {
        $prefs | Add-Member -NotePropertyName 'savefile' -NotePropertyValue ([pscustomobject]@{}) -Force
    }
    $prefs.savefile | Add-Member -NotePropertyName 'default_directory' -NotePropertyValue $TargetDir -Force

    $prefs | ConvertTo-Json -Depth 100 -Compress | Out-File -FilePath $prefPath -Encoding utf8 -NoNewline
    Write-Host "다운로드 경로 pref 교정: '$current' -> '$TargetDir' (백업: $prefPath.bak)" -ForegroundColor Yellow
}
```

호출:

```powershell
Ensure-ChromeDownloadPref -UserDataDir $userDataDir -ProfileDirectory $profileDirectory -TargetDir $collectDownloadDir
```

**주의**:
- `download.default_directory`는 Chrome의 protected pref가 아니라 평문 `Preferences`에 있으므로 `Secure Preferences`의 MAC 검증 대상이 아니다. 직접 편집해도 Chrome이 초기화하지 않는다.
- `-Encoding utf8` 필수. PowerShell 5.1의 `Set-Content`/기본 인코딩은 ANSI라서 한글 경로 값이 깨진다.
- `ConvertTo-Json -Depth 100` 필수. 기본 Depth 2면 Preferences 하위 구조가 문자열로 뭉개진다.
- 되쓰기 전 `.bak` 백업 필수.

### 2. 기존 9222 포트 중복 체크에 경고 추가

현재 88-93행:

```powershell
$inUse = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
if ($inUse) {
    Write-Host "이미 디버그 포트 $port 로 크롬이 떠 있습니다. 새로 띄우지 않습니다." -ForegroundColor Yellow
    Wait-DevToolsEndpoint -Port $port -TimeoutSeconds 10
    exit 0
}
```

`Write-Host` 다음 줄에 추가:

```powershell
    Write-Warning "이미 떠 있는 Chrome에는 실행 플래그(--download-default-directory 포함)가 적용되지 않습니다."
```

### 3. 일회성 backfill — Downloads → `E:\down`

```powershell
Get-ChildItem "C:\Users\민준\Downloads\coupangeats_*.csv" | Move-Item -Destination "E:\down"
```

**`COLLECT_SRC`가 아니라 `DOWN_DIR`(`E:\down`)로 보내는 이유**: `_cleanup_sources`가 적재 성공 후 원본을 삭제하므로, OneDrive 동기화 폴더를 246개 쓰기/삭제로 흔들 필요가 없다. `E:\down`에는 현재 `coupangeats_*` 파일이 0개라 이름 충돌도 없다.

**이동 대상 아님 — 그대로 둘 것**: `coupang_issue_log_*.txt`, `coupang_log_*.csv`, `송파삼전점_*.csv`

### 4. DAG 트리거

이동 후 30/50분 스케줄에서 자동으로 집어가지만, 즉시 확인하려면:

```powershell
docker exec airflow-airflow-scheduler-1 airflow dags trigger DB_CoupangMacro_Load_Dags
```

### 재적재 안전성 (코드 변경 불필요)

orders 85개는 이미 적재된 날짜(order_date 2026-07-30~08-02, `ym=2026-07`/`ym=2026-08` 파티션)와 겹칠 수 있다. 기존 방어 로직이 그대로 동작한다:

- `replace_covered_date_range` + shrink-guard(`DB_CoupangMacro_load.py:407-447`) — 원천 주문 수가 기존보다 적으면 기존 행을 보존하고 non-duplicate만 append
- `_deduplicate_orders`(`:236`) — `ORDER_DEDUP_COLUMNS` 기준 중복 제거
- `_coupang_load_lock`(`:78`) — `DB_CollectionCompare_Dags`와의 동시 실행 직렬화

파일명의 Chrome 중복 접미사(`..._20260731-20260801 (1).csv`)는 glob 패턴 `coupangeats_{prefix}_*.csv`에 그대로 매칭되므로 문제없다.

## Reference Code

### scripts/coupang_host_chrome.ps1 (현재 87-121행)

```powershell
# 이미 같은 포트로 떠 있으면 중복 실행 방지
$inUse = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
if ($inUse) {
    Write-Host "이미 디버그 포트 $port 로 크롬이 떠 있습니다. 새로 띄우지 않습니다." -ForegroundColor Yellow
    Wait-DevToolsEndpoint -Port $port -TimeoutSeconds 10
    exit 0
}

# 다운로드 경로: OneDrive 영업관리부_수집 우선, 없으면 E:\down 폴백
$onedriveCandidates = @(
    (Join-Path $env:USERPROFILE "OneDrive - 주식회사 도리당\Collect_Data\영업관리부_수집"),
    (Join-Path $env:USERPROFILE "OneDrive - 도리당\Collect_Data\영업관리부_수집")
)
$collectDownloadDir = "E:\down"
foreach ($c in $onedriveCandidates) {
    if (Test-Path $c) { $collectDownloadDir = $c; break }
}
Write-Host "다운로드 경로: $collectDownloadDir" -ForegroundColor Cyan

$chromeArgs = @(
    "--remote-debugging-port=$port",
    "--remote-debugging-address=0.0.0.0",
    "--remote-allow-origins=*",
    "--user-data-dir=$userDataDir",
    "--profile-directory=$profileDirectory",
    "--no-first-run",
    "--no-default-browser-check",
    "--download-default-directory=$collectDownloadDir"
)
$chromeArgs += "--load-extension=$extDir"
Write-Host "확장 로드: $extDir" -ForegroundColor Cyan
$chromeArgs += "about:blank"

Write-Host "doridang Chrome 실행 (debug port $port, profile $profileDirectory, user-data-dir $userDataDir)" -ForegroundColor Green
Start-Process -FilePath $chrome -WindowStyle Normal -ArgumentList $chromeArgs
Wait-DevToolsEndpoint -Port $port -TimeoutSeconds 30
```

`$userDataDir` / `$profileDirectory` 정의(26-27행):

```powershell
$userDataDir = Join-Path $env:LOCALAPPDATA "Google\Chrome\User Data"
$profileDirectory = "Default"
```

### modules/transform/pipelines/db/DB_CoupangMacro_load.py — 스캔 경로 (변경 금지, 참고용)

```python
from modules.transform.utility.paths import (
    COLLECT_DB, COUPANG_ORDERS_DB, COUPANG_ORDERS_DETAIL_DB, DOWN_DIR, TEMP_DIR,
)

COLLECT_SRC = COLLECT_DB / "영업관리부_수집"

def _iter_source_files(prefix: str) -> list[dict[str, Path]]:
    """Collect files from `E:/down` and `Collect_Data/...`, with source tags."""
    down_pattern = str(DOWN_DIR / f"coupangeats_{prefix}_*.csv")
    collect_pattern = str(COLLECT_SRC / f"coupangeats_{prefix}_*.csv")

    items: list[dict[str, Path]] = []
    seen: set[Path] = set()
    for item in sorted(glob(down_pattern)):
        path = Path(item)
        if path in seen:
            continue
        seen.add(path)
        items.append({"path": path, "source": "down"})

    for item in sorted(glob(collect_pattern)):
        path = Path(item)
        if path in seen:
            continue
        seen.add(path)
        items.append({"path": path, "source": "collect"})

    return items
```

### modules/transform/pipelines/db/DB_CoupangMacro_load.py:309 — 적재 후 원본 삭제 (변경 금지, 참고용)

```python
def _cleanup_sources(loaded_files: list[dict[str, Path]]) -> None:
    for item in loaded_files:
        path = Path(item["path"])
        if not path.exists():
            continue
        source = str(item["source"])
        if source == "down":
            try:
                path.unlink()
                logger.info("deleted from DOWN_DIR: %s", path)
            except Exception as exc:
                logger.warning("failed to delete %s: %s", path, exc)
        elif source == "collect":
            ...
```

## Test Cases

1. **[스크립트 문법]**
   `powershell -NoProfile -Command "$null = [System.Management.Automation.PSParser]::Tokenize((Get-Content 'C:\airflow\scripts\coupang_host_chrome.ps1' -Raw), [ref]$null); 'OK'"`
   → 기대: `OK`, 파싱 에러 없음

2. **[Chrome 종료 상태 확인]** (pref 교정 전제 조건 — 현재 chrome 프로세스 0개)
   `powershell -NoProfile -Command "@(Get-Process chrome -ErrorAction SilentlyContinue).Count"`
   → 기대: `0`. 0이 아니면 Chrome을 완전히 종료한 뒤 진행

3. **[스크립트 실행 → pref 교정]**
   `powershell -ExecutionPolicy Bypass -File C:\airflow\scripts\coupang_host_chrome.ps1`
   → 기대: `다운로드 경로 pref 교정: '' -> 'C:\Users\민준\OneDrive - 주식회사 도리당\Collect_Data\영업관리부_수집'` 출력 + `DevTools endpoint ready` 도달

4. **[pref 반영 검증]**
   `powershell -NoProfile -Command "(Get-Content \"$env:LOCALAPPDATA\Google\Chrome\User Data\Default\Preferences\" -Raw -Encoding utf8 | ConvertFrom-Json).download.default_directory"`
   → 기대: `C:\Users\민준\OneDrive - 주식회사 도리당\Collect_Data\영업관리부_수집`

5. **[프로필 무손상 검증]** — Preferences가 깨지지 않았는지
   `powershell -NoProfile -Command "$j = Get-Content \"$env:LOCALAPPDATA\Google\Chrome\User Data\Default\Preferences\" -Raw -Encoding utf8 | ConvertFrom-Json; $j.PSObject.Properties.Name.Count"`
   → 기대: 정수 출력(파싱 성공), 그리고 Chrome이 프로필 오류 없이 정상 기동

6. **[멱등성]** 스크립트 재실행
   → 기대: `다운로드 경로 pref 정상: ...` 출력, 재교정하지 않음

7. **[backfill 이동]**
   `powershell -NoProfile -Command "@(Get-ChildItem 'E:\down\coupangeats_*.csv').Count"`
   → 기대: `246`

8. **[적재 결과]** DAG 트리거 후 task 로그의 반환 JSON 확인
   `docker exec airflow-airflow-scheduler-1 airflow tasks logs DB_CoupangMacro_Load_Dags load_coupang_macro_partition <run_id>`
   → 기대: `orders.files_loaded` ≈ 85, `cmg.files_loaded` ≈ 84, `options.files_loaded` ≈ 77, 각 `rows_loaded > 0`, `status: "ok"`

9. **[원본 정리]**
   `powershell -NoProfile -Command "@(Get-ChildItem 'E:\down\coupangeats_*.csv' -EA SilentlyContinue).Count; @(Get-ChildItem 'C:\Users\민준\Downloads\coupangeats_*.csv' -EA SilentlyContinue).Count"`
   → 기대: `0`, `0` (`_cleanup_sources`가 삭제)

10. **[파티션 반영]**
    `docker exec airflow-airflow-scheduler-1 python -c "from modules.transform.utility.paths import COUPANG_ORDERS_DB; import glob; print(len(glob.glob(str(COUPANG_ORDERS_DB/'brand=*/store=*/ym=2026-08/orders_*.parquet'))))"`
    → 기대: 0보다 큰 정수

11. **[shrink 경고 확인]** — 실패 조건 아님, 기록용
    task 로그에서 `coupang reingest order counts shrank` 검색
    → 경고가 있으면 해당 store/date는 append-only 처리된 것이므로 데이터 손실은 없다. 발생한 store/date를 결과 보고에 나열할 것

12. **[E2E 회귀]** — 다음 수집 사이클(익일 00시대) 이후
    `powershell -NoProfile -Command "@(Get-ChildItem 'C:\Users\민준\OneDrive - 주식회사 도리당\Collect_Data\영업관리부_수집\coupangeats_*.csv' -EA SilentlyContinue).Count"`
    → 기대: 0보다 큼. Downloads가 아니라 여기(또는 적재 직후라면 파티션)에 떨어져야 수정 성공

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~11 순서대로 실행 (12는 익일 확인 항목)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 1~11 전체 PASS + Constraints 위반 없음
```

Test Case 5(프로필 무손상)가 FAIL이면 즉시 `Preferences.bak`으로 복구한 뒤 재시도할 것.

## Constraints

- **Python 코드/DAG는 수정하지 않는다.** `DB_CoupangMacro_Load_Dags.py`, `DB_CoupangMacro_load.py`는 정상 동작 중이다. `_iter_source_files`에 세 번째 경로를 추가하는 식의 변경 금지 — Downloads 폴더는 컨테이너에 마운트되어 있지 않아 어차피 읽을 수 없다.
- **docker-compose.yaml 수정 금지.** 볼륨 마운트 추가는 전체 컨테이너 재기동이 필요하고, 개인 Downloads 폴더에 컨테이너 쓰기 권한이 열린다.
- **Chrome 정책 레지스트리(`HKLM\SOFTWARE\Policies\Google\Chrome\DownloadDirectory`) 설정 금지.** 프로필 pref로 해결한다.
- **`CoupangAutoCollect` 예약 작업은 건드리지 않는다.** Disabled + 마지막 실행 실패지만 이번 증상과 무관한 별개 이슈다.
- Preferences 되쓰기 전 반드시 `.bak` 백업. `-Encoding utf8`, `-Depth 100` 누락 금지.
- Chrome이 실행 중일 때는 Preferences를 쓰지 않는다(종료 시 되돌려 쓰므로 무의미하고, 프로필 손상 위험).
- backfill 시 `coupang_issue_log_*.txt`, `coupang_log_*.csv`, `송파삼전점_*.csv`는 이동하지 않는다.
- `scripts/*.ps1`은 Windows PowerShell 5.1 대상. `&&`/`||`/삼항연산자/`??`/`ConvertFrom-Json -AsHashtable` 사용 불가.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case(Python) / PascalCase-Verb-Noun(PowerShell 함수), 기존 파일과 동일하게
- `Ensure-ChromeDownloadPref` 함수명·시그니처는 자유롭게 조정 가능. 동작만 명세대로면 된다
- backfill 이동을 스크립트로 만들지 인라인 명령으로 처리할지: 인라인 명령(일회성이므로 커밋 대상 아님)
