# Strategy_FdamCS_01_Process 오류 해결 (Relay FMS 로그인 오탐)

## Task
오늘(06-27) `Strategy_FdamCS_01_Process_Dags`의 `download_cs_excel` 태스크가
엑셀 다운로드 버튼 대기 180초 TimeoutException으로 실패했다. 진짜 원인은
**Relay FMS 로그인이 실제로는 실패했는데 "성공"으로 오판**되어, 사이드 메뉴가
끝까지 0개인 상태로 다운로드까지 진행하다 엉뚱한 곳에서 죽은 것이다.
로그인 성공 판정을 "음성(login URL 아님)"에서 "양성(인증 실물 확인)"으로 바꾸고,
사이드 메뉴 미로딩 시 로그인 단계에서 즉시 실패를 노출시킨다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
  (단, 본 파일 `crawling_relay_cs_fdam.py`는 기존 전체가 `print` 사용 중 →
   **기존 파일 스타일 유지: print 그대로 사용**, 새 logging 도입 금지)
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `C:\airflow\modules\extract\crawling_relay_cs_fdam.py` (단일 파일)
- DAG 파일(`dags/strategy/Strategy_FdamCS_01_Process_Dags.py`)은 **수정 불필요**
  (오케스트레이션만 담당)

## Root Cause (로그 근거)
`download_cs_excel/attempt=2.log` 발췌:
```
✅ 로그인 완료 확인: https://erp.relayfms.com/   ← 오판: 성공 처리
⏳ 사이드 메뉴 로딩 중...
  메뉴 후보 수: 0  (×3)
⚠️ 사이드 메뉴 미로딩 → 디버깅 정보 수집
  현재 URL: https://erp.relayfms.com/
  📝 body 텍스트: Login / Server List          ← 실제로는 로그인 화면 잔존
📂 CS관리 메뉴 클릭 (1~3/3) → 서브메뉴 0
📥 엑셀 다운로드 버튼 클릭 중... → 180s 타임아웃 → FAILED
```
- 로그인 버튼 클릭 → SPA가 root(`/`)로 이동 → URL에 "login" 없음 →
  `_wait_for_login_success`가 폼 재등장 직전 찰나를 잡아 `True` 오판.
- `login_relay_fms`의 메뉴 미로딩 처리는 `'login' in URL`일 때만 raise →
  URL이 `/`이므로 예외 없이 return → 실패가 다운로드 단계로 전가됨.

## Implementation Steps

### 1. `_wait_for_login_success` (line 447) — 양성 판정 + 디바운스
URL 변화만으로 성공 처리하지 말고, **인증 성공 실물 근거**를 확인한다.
- 매 폴링마다 `_is_login_page(driver)`가 False인지 확인.
- 추가로 사이드 메뉴 노드(`_collect_menu_nodes(driver)`)가 1개 이상 잡히면 즉시 성공.
- 메뉴가 아직 0이어도 "로그인 폼 없음(`_is_login_page`=False)" 상태가
  **연속 N회(약 2초, 예: 0.8s 폴링 × 3회)** 안정 유지되면 성공.
- 찰나의 SPA 전환 공백(폼이 잠깐 비었다 다시 뜸)을 성공으로 오인하지 않도록
  연속 카운터를 두고, 중간에 `_is_login_page`=True가 한 번이라도 나오면
  카운터를 0으로 리셋.
- 기존 `_is_login_page`, `_collect_menu_nodes` 재사용 (신규 함수 불필요).

핵심 로직 의사코드:
```python
def _wait_for_login_success(driver, pre_url, timeout=30):
    end = time.time() + timeout
    stable = 0
    while time.time() < end:
        if _is_login_page(driver):
            stable = 0
            time.sleep(0.8)
            continue
        # 인증 실물: 사이드 메뉴가 잡히면 확정 성공
        if _collect_menu_nodes(driver):
            return True
        # 폼 없음 상태가 연속 유지되면 성공으로 인정
        stable += 1
        if stable >= 3:
            return True
        time.sleep(0.8)
    return False
```

### 2. 사이드 메뉴 미로딩 시 로그인 실패를 명시적으로 raise (line 749-777)
`not menu_loaded` 블록의 종료 판정을 URL 문자열 검사에서
`_is_login_page(driver)` / `_has_login_marker_text(_safe_body_text(driver))`
기반으로 교체한다.
- 로그인 폼/마커가 보이면(오늘 케이스: body "Login\nServer List") →
  ```python
  raise RuntimeError(
      "로그인 실패: 인증 미성립(로그인 화면 잔존). "
      "자격증명·서버선택 상태를 확인하세요."
  )
  ```
- 이로써 실패가 다운로드(180s 타임아웃)가 아니라 **로그인 단계에서 정확히**
  드러나고, `on_failure_callback` 알림 메일의 원인도 정확해진다.
- 기존 `'login' in url` 분기는 유지하되, 위 폼/마커 검사 분기를 **먼저** 둔다.

### 3. (권장) 인증 실패 시 1회 재로그인 폴백
기존 Enter 키 폴백(line 705-720)은 "URL 미변경"에만 동작. 오늘처럼
"URL은 바뀌었지만 폼 재등장" 케이스도 재로그인 대상에 포함:
- `_wait_for_login_success`가 False면 `driver.get(".../login")` →
  폼 재입력 → 재제출을 **최대 2회** 반복, 그래도 실패면 Step 2의 RuntimeError.
- Relay FMS 렌더링 불안정(직전 06-25 런은 ID 입력창 단계 타임아웃) 대응용.
- 과도한 리팩터 금지 — 기존 login 흐름 안에서 최소 변경으로 감싼다.

## Reference Code
### modules/extract/crawling_relay_cs_fdam.py — 현재 결함 함수들
```python
# line 447 — 오탐의 진원지
def _wait_for_login_success(driver, pre_url: str, timeout: int = 30) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        current_url = _safe_current_url(driver)
        low_url = current_url.lower()
        if _is_login_page(driver):
            time.sleep(0.8)
            continue
        if pre_url and current_url != pre_url:   # ← URL만 다르면 True (오탐)
            return True
        if "login" not in low_url and "signin" not in low_url:  # ← root(/)면 True
            return True
        time.sleep(0.8)
    return False

# line 395 — 재사용할 판정 헬퍼
def _is_login_page(driver) -> bool:
    if _has_login_inputs(driver):
        return True
    return _has_login_marker_text(_safe_body_text(driver))

# line 401 — 재사용할 메뉴 수집기 (반환 list, 비면 [])
def _collect_menu_nodes(driver):
    selectors = [
        "//div[@role='menuitem' and contains(@class,'ant-menu-submenu-title')]",
        "//div[@role='menuitem']",
        "//li[@role='menuitem']",
        # ...
    ]
    # ... nodes 수집 후 반환

# line 380 — 재사용할 마커 텍스트 검사
def _has_login_marker_text(text: str) -> bool:
    markers = ["login", "로그인", "server list", "서버 목록", ...]
    # text에 마커 포함 시 True
```

```python
# line 749-777 — 메뉴 미로딩 처리: 'login' in url 일 때만 raise (오늘 케이스 누락)
        if not menu_loaded:
            print("⚠️ 사이드 메뉴 미로딩 → 디버깅 정보 수집")
            print(f"  현재 URL: {_safe_current_url(driver)}")
            # ... 스크린샷/페이지소스/ body 텍스트 저장 ...
            body_text = driver.find_element(By.TAG_NAME, 'body').text
            print(f"  📝 body 텍스트 (앞 500자):\n{body_text[:500]}")
            if 'login' in _safe_current_url(driver).lower():   # ← URL이 / 면 안 걸림
                print("  ❌ 로그인 페이지로 돌아감 → 로그인 실패")
                raise RuntimeError("로그인 실패: 로그인 페이지에서 벗어나지 못함. ...")
```

## Test Cases
1. [구문/임포트] `python -c "import modules.extract.crawling_relay_cs_fdam"`
   → 기대: ImportError/SyntaxError 없음
2. [DAG import] `python -c "from dags.strategy.Strategy_FdamCS_01_Process_Dags import dag" `
   (불가 시 airflow dags list로 대체) → 기대: 파싱 에러 없음
3. [통합·컨테이너] 단일 태스크 테스트:
   ```
   docker compose exec airflow-scheduler \
     airflow tasks test Strategy_FdamCS_01_Process_Dags download_cs_excel 2026-06-26
   ```
   → 기대(둘 중 하나로 **정확히** 분기):
   - 정상: "✅ 사이드 메뉴 로딩 완료" 후 엑셀 다운로드 성공(파일 경로 반환)
   - 인증 실패: **다운로드 180초 타임아웃이 아니라**, 로그인 단계에서
     `RuntimeError("로그인 실패: 인증 미성립...")`가 수십 초 내 발생
4. [회귀] 정상 로그인 환경에서 false-negative(정상인데 실패 처리) 없는지,
   디바운스가 정상 케이스를 막지 않는지 확인.
5. [증거] 실패 시 `/opt/airflow/download/debug_page.html` ·
   `debug_screenshot.png`로 실제 화면 상태 교차 확인.

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1→5 순서대로 실행
  2. FAIL 항목 → 원인 분석 (특히 3번의 분기 정확성)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 수정 범위는 `crawling_relay_cs_fdam.py` 단일 파일로 한정. DAG 파일 변경 금지.
- 본 파일은 기존 전체가 `print` 기반 → **print 유지**, logging 신규 도입 금지.
- 기존 헬퍼(`_is_login_page`, `_collect_menu_nodes`, `_has_login_marker_text`,
  `_safe_body_text`, `_safe_current_url`)를 **재사용**. 동일 기능 신규 함수 금지.
- 함수 시그니처(`_wait_for_login_success(driver, pre_url, timeout)`) 유지 —
  호출부(line 692, 711) 변경 없이 동작해야 함.
- 폴더 구조/파일명 변경 금지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- 디바운스 연속 횟수(N): 기본 3 (약 2.4초). 정상 케이스 막히면 2로 낮춤.
