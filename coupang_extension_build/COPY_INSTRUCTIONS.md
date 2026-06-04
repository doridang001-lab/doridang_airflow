# 쿠팡 전체 자동수집 — Phase 0 설치/테스트 안내

OneDrive 확장 폴더를 Claude가 직접 못 고치게 막혀 있어, 완성본을 여기(`C:\airflow\coupang_extension_build\`)에 만들었습니다.
**아래 파일들을 확장 폴더로 복사(덮어쓰기/추가)** 하면 됩니다.

확장 폴더: `C:\Users\민준\OneDrive - 주식회사 도리당\Extention\doridang_collector_1.3\`

## 1. 복사할 파일

| 빌드 파일 | → 확장 폴더 위치 | 종류 |
|-----------|------------------|------|
| `manifest.json` | `manifest.json` | 덮어쓰기 (권한·content script 추가) |
| `background.js` | `background.js` | 덮어쓰기 (배치 알림·알람 추가) |
| `popup.html` | `popup.html` | 덮어쓰기 (대시보드 버튼 추가) |
| `popup.js` | `popup.js` | 덮어쓰기 (대시보드 버튼 핸들러) |
| `runner.html` | `runner.html` | **신규** |
| `runner.js` | `runner.js` | **신규** |
| `content/06_batch.js` | `content/06_batch.js` | **신규** |

> 기존 `content/00~05_*.js`, `accounts.js`, `02_baemin.js`, `03_coupangeats.js`, `04_auto_login.js`, `05_main.js`는 **그대로 둡니다** (수집 로직 손대지 않음).

PowerShell 한 줄 복사 예시:
```powershell
$src = "C:\airflow\coupang_extension_build"
$dst = "C:\Users\민준\OneDrive - 주식회사 도리당\Extention\doridang_collector_1.3"
Copy-Item "$src\manifest.json","$src\background.js","$src\popup.html","$src\popup.js","$src\runner.html","$src\runner.js" $dst -Force
Copy-Item "$src\content\06_batch.js" "$dst\content" -Force
```

## 2. 확장 리로드
`chrome://extensions` → 개발자 모드 → 이 확장 **새로고침(↻)**. 오류 배지 없는지 확인.

## 3. Phase 0 테스트 (테스트 매장 1개)
1. 크롬 툴바의 확장 아이콘 클릭 → **🐔 쿠팡 전체 자동수집 (대시보드 열기)** 클릭
   - (또는 주소창에 `chrome-extension://<확장ID>/runner.html` 직접 입력)
2. 대시보드 상단 라벨이 `테스트 모드 · 도리당 송파삼전점` 인지 확인
   - ⚠ `'도리당 송파삼전점' 미발견` 이 뜨면 → 그 매장이 `accounts.js` 하드코딩 또는
     `...\Repository\sales_employee.csv`(쿠팡이츠 행)에 **id/pw가 있어야** 합니다. 없으면 추가.
   - 다른 매장으로 테스트하려면 `runner.js` 상단 `TEST_STORE` 값을 바꾸세요.
3. **▶ 수집 시작** 클릭 → 새 작업 탭이 열리며:
   - 매장 행 상태가 `세션 초기화 → 로그인중 → 수집중 → 완료` 로 실시간 변함
   - 우측 라이브 로그에 단계별 메시지 스트리밍
   - 완료 시 주문 CSV + `coupangeats_batch_manifest_YYYYMMDD.csv` 다운로드
4. 잘 되면 전체 모드: 대시보드를 `runner.html?all=1` 로 열면 모든 쿠팡 매장이 큐에 올라갑니다.

## 4. 알아둘 점 / 주의
- **세션 초기화 체크박스(기본 ON):** 매장마다 쿠팡 쿠키를 지워 정확한 계정으로 로그인합니다.
  지금 크롬에서 쿠팡에 수동 로그인해 둔 게 있으면 로그아웃됩니다(정상). 끄면 현재 세션 유지.
- **수집 날짜:** 기존 로직 그대로 "주문관리 페이지에 표시되는 기간"을 수집합니다. 특정 날짜 고정이
  필요하면 다음 단계(A1-4: 날짜필터 세팅)에서 추가합니다.
- **로그아웃 방식:** 현재는 쿠키 삭제(browsingData)로 확실히 처리합니다. DOM 로그아웃 버튼 클릭은
  보조(`06_batch.js`)로만 두었습니다.

## 5. 다음 단계 (이번 빌드 이후)
- **Part B:** 크롬 다운로드 폴더를 OneDrive 동기화 폴더로 지정 → Airflow가 그 경로에서 CSV 인제스트.
- **Part C:** `dags/db/DB_Coupang_Macro_Dags.py`의 Selenium 수집 태스크를 "확장 CSV 인제스트 +
  매니페스트 검증"으로 교체, Selenium 경로는 env 플래그 폴백으로 격리.
- **무인화:** 윈도우 작업 스케줄러가 매일 `chrome.exe ...runner.html?run=coupang&auto=1` 실행
  (또는 background.js의 `chrome.alarms` 'daily_coupang' 활성화).
