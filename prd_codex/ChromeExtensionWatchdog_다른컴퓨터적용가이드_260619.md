# ChromeExtensionWatchdog 다른 컴퓨터 적용 가이드 프롬프트

아래 프롬프트를 다른 컴퓨터의 Codex에 그대로 전달하세요.

```text
C:\airflow 저장소에 ChromeExtensionWatchdog 외부 감시 재시도 스크립트를 적용해줘.

목표:
- scripts/coupang_host_chrome.ps1은 수정하지 않는다.
- 신규 파일 scripts/coupang_extension_watchdog.py를 만든다.
- 이 스크립트는 이미 떠 있는 Chrome 디버그 포트 localhost:9222에 Selenium으로 attach한다.
- runner.html 탭을 찾아 배치 완료를 기다린다.
- runner에 attach한 직후 최초 수집 대상 row 목록을 스냅샷으로 잡는다.
- 배치 완료 후 최초 수집 대상 안의 실패/경고/건너뜀 행만 선택해서 startBtn을 눌러 재시도한다.
- 최대 5회 반복한다.
- b-ok 완료 행은 절대 클릭하지 않는다.
- b-wait 중 텍스트가 "건너뜀"인 경우만 재시도한다. "대기"는 클릭하지 않는다.
- 상위 50%만 수집하는 운영이므로, 최초 스냅샷 시점부터 이미 "건너뜀"인 하위 50% 행은 재시도 대상에서 제외한다.
- 재시도 루프가 끝나면 `DB_CoupangMacro_Load_Dags`를 트리거해 `E:\down\coupangeats_*` 원본을 `ANALYTICS_DB/coupang_macro`로 적재한다.
- 쿠팡 매크로 적재와 실패 이메일 알림은 `DB_CoupangMacro_Load_Dags` 하나로 통일한다.

저장소 규칙:
- 항상 AGENTS.md를 먼저 읽고 따른다.
- scripts 폴더는 분석/자동화용 단독 실행 스크립트로만 사용한다.
- logging은 logger = logging.getLogger(__name__) 패턴을 사용한다.
- print는 사용하지 않는다.
- 파라미터는 argparse를 사용한다.
- 문서/파이썬 파일은 UTF-8로 유지한다.
- 임시 파일은 저장소 루트에 만들지 않는다.

구현 파일:
- 새 파일: C:\airflow\scripts\coupang_extension_watchdog.py
- 수정 금지: C:\airflow\scripts\coupang_host_chrome.ps1

필수 동작:
1. Chrome attach
   - from selenium import webdriver
   - from selenium.webdriver.chrome.options import Options
   - opts = Options()
   - opts.debugger_address = "localhost:9222"
   - webdriver.Chrome(options=opts)

2. runner.html 탭 탐색
   - driver.window_handles를 순회한다.
   - 각 탭으로 switch_to.window(handle) 한다.
   - current_url이 chrome-extension:// 로 시작하고 /runner.html 로 끝나면 runner 탭으로 판단한다.
   - 없으면 DevTools endpoint http://127.0.0.1:9222/json 을 확인하고, runner.html 탭을 새로 연다.
   - fallback URL은 chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html 이다.

3. 배치 완료 대기
   - driver.execute_script("return window.running === true") 로 실행 중 여부를 확인한다.
   - true이면 poll 간격으로 대기한다.
   - false이면 배치 완료로 본다.
   - 탭이 사라지거나 WebDriverException이 나면 runner 탭을 다시 찾거나 다시 연다.
   - 기본 poll_sec은 30초, batch_timeout은 5400초로 둔다.

4. 최초 수집 대상 스냅샷
   - runner.html 탭 확보 직후 row 목록을 읽는다.
   - 안정화를 위해 기본 5초 정도 기다린 뒤 스냅샷을 잡는다.
   - rows = driver.find_elements(By.CSS_SELECTOR, "tbody#rows tr")
   - 각 row의 id가 row-숫자 형식인지 확인한다.
   - 이 시점에 class가 b-wait이고 text.strip() == "건너뜀"인 행은 초기 제외 행으로 본다.
   - 초기 제외 행은 상위 50% 수집 대상 밖의 하위 50%로 간주하고 이후 재시도에서 제외한다.
   - 나머지 row index만 최초 수집 대상 set으로 보관한다.

5. 실패/건너뜀 행 필터링
   - rows = driver.find_elements(By.CSS_SELECTOR, "tbody#rows tr")
   - 각 row의 id가 row-숫자 형식인지 확인한다.
   - 최초 수집 대상 set에 들어있는 row만 검사한다.
   - row 안의 span.badge들을 검사한다.
   - class에 b-fail 또는 b-warn이 있으면 재시도 대상이다.
   - class에 b-wait이 있고 text.strip() == "건너뜀"이면 재시도 대상이다.
   - b-ok는 재시도 대상이 아니다.
   - b-wait 텍스트가 "대기"이면 재시도 대상이 아니다.
   - 중복 index는 순서를 유지해서 제거한다.

6. 재시도 클릭
   - 실패/건너뜀 index에 대해서만 driver.find_element(By.ID, f"row-{idx}").click() 한다.
   - 각 클릭 사이에는 0.3초 정도 쉰다.
   - 이후 driver.find_element(By.ID, "startBtn").click() 한다.
   - 재시도 시작 로그를 남긴다.

7. CLI
   - --max-retry 기본값 5
   - --poll-sec 기본값 30
   - --batch-timeout 기본값 5400
   - --target-settle-sec 기본값 5
   - --load-dag-id 기본값 DB_CoupangMacro_Load_Dags
   - --scheduler-container 기본값 airflow-airflow-scheduler-1
   - --skip-load-dag 옵션을 주면 적재 DAG 트리거를 생략한다.
   - main()은 int exit code를 반환하고, __main__에서 raise SystemExit(main()) 한다.

8. 적재 DAG 트리거
   - runner 배치 완료를 한 번 이상 확인한 뒤에만 트리거한다.
   - 기본 명령은 docker scheduler 컨테이너에서 실행한다.
     - docker exec airflow-airflow-scheduler-1 airflow dags trigger DB_CoupangMacro_Load_Dags
   - docker 명령이 실패하면 로컬 airflow CLI를 fallback으로 시도한다.
   - 이 DAG가 `E:\down\coupangeats_orders_*.csv`, `coupangeats_cmg_*.csv`, `coupangeats_options_*.csv`를 읽어서 아래 경로에 저장한다.
     - `C:\Users\민준\OneDrive - 주식회사 도리당\data\analytics\coupang_macro\orders`
     - `C:\Users\민준\OneDrive - 주식회사 도리당\data\analytics\coupang_macro\cmg`
     - `C:\Users\민준\OneDrive - 주식회사 도리당\data\analytics\coupang_macro\options`
   - 같은 DAG 안의 `send_failure_email_if_any` 태스크가 dag_run.conf.failures가 있을 때만 실패 이메일을 보낸다.

검증:
1. 문법 검사
   python -m py_compile C:\airflow\scripts\coupang_extension_watchdog.py

2. UTF-8 검사
   python -c "from pathlib import Path; Path(r'C:\airflow\scripts\coupang_extension_watchdog.py').read_text(encoding='utf-8'); print('utf8 ok')"

3. 도움말 검사
   python C:\airflow\scripts\coupang_extension_watchdog.py --help

4. 실제 Chrome 실행 없이 시뮬레이션 검사
   - 가짜 driver/row/span 객체를 만들어 find_failed_row_indices()와 click_retry()를 호출한다.
   - 입력 행 예시:
     - row-1: badge b-ok, "완료"
     - row-2: badge b-fail, "실패"
     - row-3: badge b-wait, "대기"
     - row-4: badge b-wait, "건너뜀"
     - row-5: badge b-warn, "경고"
     - row-6: 최초 스냅샷부터 badge b-wait, "건너뜀"인 하위 50% 제외 행
   - 기대 결과:
     - 최초 대상 set 밖의 row-6은 이후에도 클릭되지 않음
     - failed_indices는 최초 대상 안의 ['2', '4', '5']
     - 클릭된 행은 row-2, row-4, row-5
     - row-1, row-3, row-6은 클릭되지 않음
     - startBtn은 클릭됨

완료 후 보고:
- 생성/수정 파일 목록
- 검증 명령 결과
- 실제 Chrome attach 테스트를 하지 않았다면 그 사실을 명확히 말한다.
```

## 다른 컴퓨터에서 수동 실행할 때

1. 전용 Chrome 실행:

```powershell
powershell -ExecutionPolicy Bypass -File C:\airflow\scripts\coupang_host_chrome.ps1
```

2. watchdog 실행:

```powershell
python C:\airflow\scripts\coupang_extension_watchdog.py --max-retry 5
```

이 실행이 끝나면 기본적으로 `DB_CoupangMacro_Load_Dags`가 트리거되어 `coupang_macro` 적재가 시작됩니다.

## 적용 후 확인 포인트

- `coupang_host_chrome.ps1`은 변경되면 안 됩니다.
- 쿠팡 매크로 적재와 실패 알림은 `DB_CoupangMacro_Load_Dags` 하나가 담당합니다.
- 완료 행(`b-ok`)은 클릭되면 안 됩니다.
- 대기 행(`b-wait`, 텍스트 `대기`)은 클릭되면 안 됩니다.
- 최초 수집 대상 안에서 나중에 건너뜀 처리된 행(`b-wait`, 텍스트 `건너뜀`)은 클릭되어야 합니다.
- 최초 스냅샷부터 이미 건너뜀이었던 하위 50% 행은 클릭되면 안 됩니다.
- 실패/경고 행(`b-fail`, `b-warn`)은 클릭되어야 합니다.
