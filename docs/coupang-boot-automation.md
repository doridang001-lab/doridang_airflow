# 쿠팡이츠 매크로 runner 자동 실행 가이드

## 목표
- Windows 부팅 시 디버그 크롬을 기동하고(`coupang_host_chrome.ps1`)
- `runner.html` 자동화 스크립트를 실행해 `topHalfBtn` 클릭
- 수집 클릭 완료 후 적재 DAG를 즉시 트리거하고, 수집 파일 안정화 후 처리

## 실행 스크립트
- `C:\airflow\scripts\coupang_host_chrome.ps1`
- `C:\airflow\scripts\coupang_boot_autostart.ps1`

## 작업 스케줄러 등록(로그온 트리거)
`CoupangAutoCollect` 작업은 `coupang_boot_autostart.ps1`를 로그온 1분 지연 후 실행하도록 등록한다.
Chrome 확장 UI를 사용하므로 로그인 전 부팅 트리거가 아니라 `민준` 계정 로그온 트리거를 사용한다.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File C:\airflow\scripts\register_coupang_autocollect_task.ps1
```

권장 순서:
- 최초 등록 후 터미널에서 `coupang_boot_autostart.ps1` 단독 실행 확인
- 실행 정책이 막히는 경우 `-ExecutionPolicy Bypass` 유지
- Chrome 프로필 경로(`C:\coupang_chrome_profile`) 및 runner 확장 경로가 유효한지 확인
- 작업 스케줄러의 실행 제한 시간이 5분으로 남아 있으면 안 됨. 등록 스크립트는 기본 45분으로 설정한다.

## 체크리스트
- `http://127.0.0.1:9222/json` 접속 가능
- `runner.html`은 고정 확장 ID가 아니라 `chrome.runtime.getURL('runner.html')`로 열린 실제 확장 URL이어야 함
- `runner.html` 탭에서 `#topHalfBtn`가 60초 내 활성화
- 버튼 클릭 후 로그에 `clicked topHalfBtn` 출력
- 실행 로그는 `C:\airflow\.tmp\coupang_boot_autostart\*.log`에서 확인
- 수집 파일 패턴(`coupangeats_*.csv`)이 수집 디렉터리에 쓰인 뒤
- `airflow dags trigger DB_CoupangMacro_Load_Dags`로 적재 DAG 시작

## 동작 방식
- `coupang_boot_autostart.ps1`는 우선 host chrome을 실행하고 `coupang_runner_autoclick.py`로 버튼을 클릭한다.
- 그 다음 수집 결과 파일(`E:\down`, `Collect_Data/...`) 증가/안정화를 감시한 뒤
  `DB_CoupangMacro_Load_Dags`를 트리거한다.
- 스케줄 시간(09:00)은 보조 스케줄로 유지되어, 수동/오류 보정 처리에도 대응한다.
