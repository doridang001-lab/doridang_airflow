# update_log

## 2026-07-20 AirflowCodexTelegramAutoHeal PowerShell 팝업 제거
- 대상: `scripts/start_codex_autoheal_hidden.ps1`, Windows 작업 스케줄러 `AirflowCodexTelegramAutoHeal`, `docs/codex/airflow-codex-telegram-autoheal.md`
- 변경: 기존 5분 PowerShell wrapper는 heartbeat 최신 및 WSL tmux 창 존재 상태에서 즉시 종료하도록 guard 추가, 별도 WSL 직접 실행 작업 `AirflowCodexTelegramAutoHealWSL`을 30분 반복으로 등록.
- 검증: wrapper 직접 실행 성공, tmux 창이 없으면 WSL `codex-autoheal`을 재기동함을 로그와 `tmux list-windows`로 확인, 새 작업이 heartbeat 갱신함을 확인.
- 남은 위험: 보호된 기존 `AirflowCodexTelegramAutoHeal` 작업은 권한 거부로 비활성화하지 못해 XML상 PowerShell wrapper/5분 반복이 유지됨.

## 2026-07-16 쿠팡이츠 다음 페이지 이동 실패 경고 수정
- 대상: `coupang_extension_build/content/03_coupangeats.js`, `AGENTS.md`, `prd_codex/쿠팡이츠다음페이지없음이동실패경고원인수정_260716.md`
- 변경: 다음 페이지 이동 결과를 `moved/stalled`로 구분하고, 정체 시 1회 재시도 후 transient error로 백필 이월하도록 구현.
- 검증: `node --check` 통과, `_clickNextOrderPage()` boolean 반환 제거 확인, 호출부 `nextResult` 분기 확인, `remaining` 조건 유지 확인.
- 남은 위험: 실제 쿠팡이츠 SPA 정체 여부는 Chrome 확장 재로드 후 수동 수집으로만 최종 확인 가능.
- OneDrive: 승인 전이므로 개발용 OneDrive 파일은 수정하지 않음.

## 2026-07-17 쿠팡이츠 로그인 쓰로틀 배치 중단 수정
- 대상: `coupang_extension_build/runner.js`, OneDrive 개발용 확장 `runner.js`
- 변경: 로그인 단계 쓰로틀 2연속 감지 시 배치 전체를 중단하던 처리를 제거하고, 해당 계정/매장만 백필 대상으로 표시한 뒤 쿨다운 후 다음 계정으로 진행하도록 수정.
- 검증: 저장소/OneDrive `runner.js` 모두 `node --check` 통과, UTF-8 읽기 검증 통과, 두 파일 바이트 동일 확인.
- 남은 위험: 실제 쿠팡이츠 로그인 제한 빈도와 10분 쿨다운 적정성은 다음 상/하위 배치 실행 로그로 확인 필요.
- OneDrive: 사용자 승인 후 개발용 확장 파일에 동일 변경 반영.

## 2026-07-19 DB_FinProduct_Dags 장기 실행 복구
- 대상: `dags/db/DB_FinProduct_Dags.py`, Airflow `DB_FinProduct_Dags`
- 변경: `classify_pending_master` LLM pending 분류 단위를 `limit=1000`에서 `PENDING_MASTER_LIMIT=3`으로 축소해 장시간 running 고착을 방지.
- 검증: py_compile 통과, DAG import error 없음, `scheduled__2026-07-17T23:35:00+00:00` run의 모든 task success 확인.
- 남은 위험: pending 최신 미분류 상품이 13,896건이라 매 스케줄마다 소량 누적 처리되며, LLM 응답 속도는 건당 약 2.5~3.5분 수준.

## 2026-07-19 DB_FinProduct_Dags pending 수동분류 task 제거
- 대상: `dags/db/DB_FinProduct_Dags.py`
- 변경: 수동분류 pending 처리는 `DB_FinProduct_Map_Dags` 담당으로 보고 `classify_pending_master` task, import, limit 설정을 제거.
- 검증: py_compile 통과, DAG import error 없음, Airflow task 목록에서 `classify_pending_master` 제거 확인.
- 재실행: 사용자 승인 후 `manual__2026-07-19T11:39:58+00:00` 실행, 전체 task success 확인.
- 남은 위험: 기존 과거 run 이력에는 제거 전 task 상태가 남을 수 있으나 신규 run에는 포함되지 않음.

## 2026-07-20 AirflowCodexTelegramAutoHeal 큐 적재 누락 수정
- 대상: `dags/db/DB_Beamin_Macro_Upload_Dags.py`, `docs/codex/airflow-codex-telegram-autoheal.md`
- 변경: `DB_Beamin_Macro_Upload_Dags` 실패가 `heal_queue.jsonl`에 적재되도록 공통 `on_failure_callback`을 연결하고, auto-heal 문서를 실제 스케줄러 wrapper 기준으로 정정.
- 검증: 컨테이너 Airflow 환경에서 DAG import/task 목록 확인, `default_args.on_failure_callback` 연결 확인, 임시 queue 경로로 `enqueue_heal_task` JSONL 적재 확인, UTF-8 읽기 확인.
- 남은 위험: 현재 세션의 `wsl.exe --list --verbose`가 배포판 없음으로 응답해 WSL 계정/배포 상태는 별도 복구 필요.

## 2026-07-20 Sales_Employee_Extract_Dags 토더 누락 알림 전환
- 대상: `dags/sales/Sales_Employee_Extract_Dags.py`
- 변경: 토더 ID/PW 누락 알림의 Gmail SMTP 메일 발송을 제거하고 공용 `send_telegram_chunks` 텔레그램 발송으로 전환, 누락 매장은 `[신규 매장 / 양도양수 매장 / 해지 매장]` 양식으로 정리, 핸드폰번호는 `전화번호` 컬럼 우선 사용, 플랫폼 계정은 공백 제거 컬럼 매칭으로 보강, 담당자 없는 비운영 매장은 알림 대상에서 제외, DAG 스케줄을 매일 02:30으로 변경.
- 검증: ast 구문 확인, 임시 Airflow 홈 기반 DAG import 및 스케줄 확인, 잔여 메일 참조 제거, monkeypatch 기반 토더 누락/컬럼 없음 단위 동작, UTF-8 읽기 확인.
- 남은 위험: 텔레그램 실제 수신은 Airflow Variable(`TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`) 설정과 운영 실행 환경에 의존.

## 2026-07-20 쿠팡 지정날짜 주문서 수집 버튼 추가
- 대상: OneDrive 개발용 확장 `runner.html`, `runner.js`, `content/05_main.js`, `content/03_coupangeats.js`
- 변경: 대시보드에 매장/시작일/종료일 기반 `지정날짜 수집` UI를 추가하고 `customOrdersRange` manual 경로로 주문서만 수집하도록 분리.
- 검증: `node --check` 3개 JS 통과, custom 블록 체크포인트/beacon 0건 확인, CMG/메뉴 URL보다 early return 선행 확인, DOM id 유지 확인, UTF-8 읽기 확인.
- 남은 위험: Chrome 확장 새로고침과 실제 쿠팡이츠 지정날짜 수집은 브라우저에서 수동 확인 필요.
- OneDrive: 사용자 승인 후 개발용 확장 파일 수정.

## 2026-07-20 DB_Beamin_Macro_Upload_Dags upload inbox 다중 날짜 정체 수정
- 대상: `modules/transform/pipelines/db/DB_Beamin_pc2_distribute.py`, `tests/test_beamin_upload_inbox_distribute.py`
- 변경: `_baemin_upload_inbox` 다중 `target_date` 사전 차단을 제거하고, 모든 폴더 분배/cleanup 후 최신 날짜 meta만 downstream XCom 병합 대상으로 필터링.
- 검증: 전용 pytest 3건 통과, distributor import 통과, 임시 `AIRFLOW_HOME` 기반 `DB_Beamin_Macro_Upload_Dags` import 통과, `_validate_single_target_date` 참조 없음 확인.
- 운영 적재: 사용자 요청 후 `ingest_baemin_upload_inbox()` 단독 실행, 8개 중 7개 폴더 cleanup, files=191 rows=38323 적재.
- 검증 결과: 송파삼전점 2026-07-16~19 주문이 analytics에 반영됨(일별 rows/orders: 31/4, 52/9, 89/13, 64/10).
- 남은 위험: `_meta.json`만 있는 `manual__manual_songpa_woori_only_20260716_1007` 폴더 1개는 대상 파일이 없어 cleanup 보류됨.
- OneDrive: 사용자 요청에 따라 운영 analytics/inbox 적재 및 cleanup 수행.

## 2026-07-20 DB_Beamin upload inbox 비주문 파일 포맷 점검
- 대상: `modules/transform/pipelines/db/DB_Beamin_pc2_distribute.py`, `tests/test_beamin_upload_inbox_distribute.py`
- 원인: upload inbox 분배가 subtype 구분 없이 공통 `write_table()`을 호출해 `orders` 외 CSV 기준 파일까지 parquet로 저장하고 기존 CSV를 삭제함.
- 변경: `orders`만 parquet/upsert를 유지하고, `orders` 외 subtype은 UTF-8-SIG CSV 저장 및 동일 stem parquet 제거로 분리.
- 검증: 전용 pytest 4건 통과, 컨테이너 import 및 코드 반영 확인.
- 운영 복원: 사용자 승인 후 `orders` 제외 비주문 parquet 133개를 CSV로 변환하고 parquet 삭제, 기존 CSV가 있으면 중복 제거 병합.
- 검증 결과: `orders`만 parquet 308개 유지, 비주문 subtype parquet 0개 확인, 우가클/광고/변경이력 샘플 CSV 재읽기 통과.
- 남은 위험: 없음. 향후 upload inbox 적재는 코드상 `orders` 외 CSV로 저장됨.

## 2026-07-20 delivery_commission 신규 DAG 구현
- 대상: `modules/transform/pipelines/db/DB_DeliveryCommission.py`, `dags/db/DB_DeliveryCommission_Dags.py`, `modules/transform/utility/paths.py`, `modules/transform/utility/schedule.py`
- 변경: 배민·쿠팡 전체 저장 원천을 매번 재집계해 `delivery_commission.parquet` 단일 마트로 overwrite하는 DAG와 경로/스케줄 상수 추가, 실행 시간은 매일 08:10으로 설정.
- 계산: 배민은 주문번호 dedup 후 `입금예정금액-광고지출`, 쿠팡은 취소금액을 재차 차감하지 않고 `매출액` 기준으로 정산 차이를 계산.
- 검증: py_compile/import 및 로더 단독 실행 통과, 배민 주문 5361행·광고 4856행·쿠팡 주문 3162행 집계 확인.
- 운영 생성: 사용자 승인 후 `delivery_commission.parquet` overwrite 실행, 9053행 생성(배달의민족 5361행, 쿠팡이츠 3692행).
- 검증 결과: 결과 parquet 재읽기 통과, 음수 매출 0건, `total_amt==0` 28건 확인.
- 남은 위험: 일부 0원 행은 원천 0원/광고비 선차감 케이스일 수 있어 필요 시 별도 샘플 점검.

## 2026-07-20 delivery_commission 수수료율 컬럼 제거
- 대상: `modules/transform/pipelines/db/DB_DeliveryCommission.py`, OneDrive mart `delivery_commission.parquet`
- 변경: 출력 스키마에서 수수료율 컬럼 제거, 최종 컬럼을 `sale_date/store/platform/total_amt/settlement_amount/diff_amt` 6개로 축소.
- 운영 생성: 사용자 요청 후 mart parquet overwrite 실행, 9063행 생성(배달의민족 5361행, 쿠팡이츠 3702행).
- 검증 결과: py_compile 통과, 결과 parquet 재읽기 통과, 수수료율 컬럼 없음 확인.
- 남은 위험: 없음.

## 2026-07-20 쿠팡 확장 로그인 재시도/타이핑/discard 방지
- 대상: `coupang_extension_build/runner.js`, `coupang_extension_build/content/04_auto_login.js`, `coupang_extension_build/background.js`
- 변경: 원인불명 로그인 에러는 최대 2회 내 F5 재로그인으로 복구하고, 쓰로틀성 로그인 에러는 즉시 스킵 유지. 쿠팡 ID/PW 입력을 글자별 타이핑 이벤트로 변경하고 러너/작업/수동수집 탭에 `autoDiscardable:false` 설정 추가.
- 검증 결과: `node --check` 3개 JS 통과, `autoDiscardable:false` runner 4곳/background 1곳 및 `simulateTyping` 정의/호출 확인.
- 남은 위험: 실제 Chrome 확장 폴더 복사/리로드 후 로그인 재시도와 장시간 탭 응답성은 수동 확인 필요.

## 2026-07-20 쿠팡 수동수집 밴 방지 가드 추가
- 대상: `coupang_extension_build/background.js`, `coupang_extension_build/content/05_main.js`
- 변경: 쿠팡 수동 아이콘/단축키 수집에 10분 쿨다운을 추가하고, `10057`/`ACCESS_DENIED` 등 제한 신호 수신 시 수동수집 2시간 잠금을 저장하도록 보강.
- 검증 결과: `node --check` 2개 JS 통과, 아이콘/단축키 수동 가드와 `MULTISTORE_COMPLETE` 제한 신호 락 패턴 확인.
- 남은 위험: 실제 밴 감지 후 락 동작과 운영 쿨다운 길이는 Chrome 확장 리로드 후 수동 확인 필요.

## 2026-07-20 Saleslab Power BI 카카오톡 전송 순서 수정
- 대상: `scripts/saleslab_powerbi_kakao_daily.py`, 작업 스케줄러 `SaleslabPowerBIKakaoDaily`
- 변경: 자동화 초반 카카오톡 실행 후 창을 최대화하도록 변경해 `카카오톡 실행 → 카카오톡 최대화 → Power BI 캡처 → 프담CS방 검색/전송` 순서로 고정. 채팅 탭 좌표를 `39,140`으로 교정하고 `Ctrl+F` 직후 방 이름만 입력하도록 단순화.
- 검증 결과: `py_compile` 통과, 스케줄러 액션이 기존 `saleslab_powerbi_kakao_daily.ps1 -Send -SendMode enter` 호출을 유지함 확인. UI 단독 테스트에서 카카오톡 전면 유지 및 `[전략기획부] 프담CS` 검색 입력 실행 확인.
- 남은 위험: 실제 전송은 UI/모니터 배치 의존이라 다음 예약 실행 또는 수동 1회 실행으로 최종 화면 동작 확인 필요.

## 2026-07-21 쿠팡 확장 중단 프로토콜 강화
- 대상: `coupang_extension_build/runner.js`, `content/05_main.js`, 전용 회귀 테스트, OneDrive 개발용 확장 대응 파일.
- 변경: 현재 워크탭 STOP 전달, 영속 중단 래치, F5/건수불일치 재개 차단, 부분 저장 10초 유예 후 탭 종료, 중단 후 runner 자동 재시작 차단을 추가.
- 검증: 전용 pytest 2건과 저장소/OneDrive JS `node --check`, UTF-8 엄격 읽기, 워크탭 추적 경로 3개/4개 및 핵심 로직 동일성 확인.
- 남은 위험: 실제 쿠팡이츠 로그인 상태에서 수집 중단·부분 파일 저장·F5 전환 중 중단은 Chrome 확장 reload 후 수동 확인 필요.
- OneDrive: 사용자 승인 후 개발용 `runner.js`, `content/05_main.js`에 반영.

## 2026-07-21 배민수동 가격형 item_name 유입 교정
- 대상: 배민 확장 옵션 파서·복사 지침, `DB_UnifiedSales_baemin.py`, 4개 매장 2026-07 배민 원본과 영향 날짜 UnifiedSales.
- 변경: 취소선 정가 제거·가격형 메뉴명 폴백과 변환 가드를 추가하고, 확장 배포 대상에 `content/02_baemin.js`를 명시했으며 원본 34행·UnifiedSales 교정과 상품 ID 마스터 정상 메뉴 키 8행 추가를 완료.
- 검증 결과: 원본/UnifiedSales 숫자형 이름 각 0건, 정상 메뉴 키 10개 존재, 원본 행 수·금액 컬럼 및 15개 매장-날짜 매출 합계 불변, 신규 회귀 테스트 3건 통과.
- 남은 위험: 확장 파일의 실제 Chrome 복사·리로드와 운영 화면 수집은 사용자 수동 확인 필요. 기존 관련 테스트 실패 5건은 이번 범위 밖 기준선으로 유지.

## 2026-07-21 Codex auto-heal 예약 작업 창 숨김
- 대상: `scripts/start_codex_autoheal_wsl_hidden.vbs`, 작업 스케줄러 `AirflowCodexTelegramAutoHeal*`.
- 변경: 5분 주기 상태 점검을 `wscript.exe` 기반 완전 숨김 실행으로 전환하고, 중복된 WSL 직접 실행 작업을 비활성화.
- 검증 결과: `AirflowCodexTelegramAutoHeal`은 `wscript.exe` 액션·숨김·5분 주기·결과 0, 직접 WSL 작업은 비활성화 상태로 재조회 확인.
- 남은 위험: 다음 예약 시각에 실제 화면 깜빡임이 없는지 사용자 세션에서 최종 확인 필요.

## 2026-07-21 배민 매크로 안정성·PC2 테스트매장 우선순위 보완
- 대상: `beamin_stability.py`, `DB_Beamin_Macro_Pc2_Dags.py`.
- 변경: `safe_daily`의 드라이버 재시작·로그인 재시도 대기·세션 복구 한도를 조정하고, PC2 분할 결과에서 unified 테스트매장을 먼저 수집하도록 정렬.
- 검증 결과: 프로필·홀짝 분할·정확 매칭·None/미인식 범위·UTF-8·문법 검사와 컨테이너 DAG import 통과, Airflow import 오류 없음.
- 남은 위험: 기존 배민 회귀 테스트 16건 중 이번 변경 밖 기준선 4건 실패, 안정성 개선 효과는 다음 예약 실행 로그에서 확인 필요.

## 2026-07-21 배민 변경이력 토요일 수집 전환
- 대상: `DB_Beamin_combined.py`, 변경이력 요일 회귀 테스트.
- 변경: 한국시간 기준 토요일에만 매장 변경이력 단계를 실행하고, 나머지 요일에는 우리가게·주문·광고 수집만 계속하도록 가드 추가.
- 검증 결과: 토요일/금요일/일요일 판정과 실제 단계 호출·스킵 테스트 4건, 문법·UTF-8·메인/PC2 DAG 컨테이너 import 통과.
- 남은 위험: 다음 토요일 예약 실행에서 변경이력 파일 갱신과 평일 스킵 로그를 운영 확인할 필요가 있음.

## 2026-07-21 Airflow DAG 전용 Harness 확장
- 대상: `C:\dags_harness_framework`, Airflow 실행기, `harness/registry.json`, 하네스 운영 문서.
- 변경: 5개 DAG 그룹과 9개 특수 DAG override, inventory/validate/scaffold CLI, `dag_targets` 자동 문서 주입, 해시 기반 dry-run/apply 배포를 추가.
- 검증 결과: 프레임워크 테스트 81건 통과, Airflow 배포본 테스트 15건 통과, 현재 DAG 후보 73개 registry 검증 통과, managed payload lock 등록 완료.
- 남은 위험: Harness Monitoring 대시보드의 registry 진단 표시는 후속 범위이며, 실제 phase Codex 실행은 운영 변경을 피하기 위해 수행하지 않음.

## 2026-07-21 Airflow Harness 불필요 자산 정리
- 대상: `phases/airflow-harness-smoke`, 하네스 문서 예시, 모니터링의 빈 phase 진단, registry 검증기.
- 변경: 일회성 smoke phase를 삭제하고 빈 `phases/index.json`을 정상 idle 상태로 처리했으며, 미등록 phase 디렉터리 탐지를 추가.
- 검증 결과: 원본 프레임워크 82건, Airflow 실행기·모니터링 29건 통과, DAG 후보 73개 registry 검증 및 배포 dry-run 정상.
- 남은 위험: 실제 모니터링 DAG 실행과 OneDrive 대시보드 재생성은 승인 없이 수행하지 않음.

## 2026-07-21 수삼 일별 리포트 Flow 댓글 업로드 구현
- 대상: `scripts/analysis/susam_report.py`, 전용 회귀 테스트.
- 변경: 일별 수삼 XLSX 4시트, Flow 드래그앤드롭·Enter 댓글 등록, DOM 성공 확인, 상태파일 중복 방지와 Docker 원격 Chrome 연결 CLI를 추가.
- 검증 결과: 전용 pytest 11건과 UTF-8·문법 검사 통과. Windows에서 07-01, Airflow worker 컨테이너에서 07-02 실제 댓글·첨부 등록 및 재실행 skip 확인.
- 남은 위험: worker 런타임 설치 패키지는 컨테이너 재생성 시 사라지므로 requirements 반영 이미지 재빌드가 필요하며, Docker 업로드 시 승인된 DevTools 포트를 작업 중에만 열어야 함.

## 2026-07-21 네이버 스마트플레이스 자동수집 대시보드 추가
- 대상: `doridang_collector_개발용`의 네이버 콘텐츠 모듈·전용 러너·manifest·popup.
- 변경: 송파삼전점 날짜별 URL 이동, 유입·예약 파싱, 요청 상관관계 보호, 진행 테이블·라이브 로그·부분 CSV 저장을 추가.
- 검증 결과: 신규/수정 JS 문법, manifest JSON·등록 계약, 대시보드 필수 ID, UTF-8 검사 통과.
- 남은 위험: 네이버 실제 화면의 라벨 구조와 값 대응, 로그인 상태, 다운로드 동작은 확장 리로드 후 수동 E2E 확인 필요.

## 2026-07-21 네이버 로딩 전 오수집 방지·수집 버튼 분리
- 대상: `content/07_naver.js`, `runner_naver.html`, `runner_naver.js`.
- 변경: 어제/기간 수집 버튼을 분리하고, 목표 날짜가 속한 서로 다른 지표 카드·로딩 종료·3회 값 안정화를 모두 확인한 뒤에만 성공하도록 보강.
- 검증 결과: JS 문법, 날짜 파서, 버튼·재시도·오류코드 정적 계약, manifest JSON, UTF-8 검사 통과.
- 남은 위험: 네이버 실제 DOM에서 카드 범위와 로딩 요소 선택자가 맞는지는 확장 리로드 후 수동 E2E로 최종 확인 필요.

## 2026-07-21 네이버 예약 지표 안전 순서 폴백 추가
- 대상: `content/07_naver.js`.
- 변경: 라벨 연결 실패 시 표시 숫자가 정확히 2개이고 서로 다른 `ReportSummary_info` 카드일 때만 첫째를 유입, 둘째를 예약으로 읽도록 보강.
- 검증 결과: JS 문법, `344/1` 폴백 매핑, 숫자 3개 거부, probe 추출 방식, UTF-8·payload 계약 검사 통과.
- 남은 위험: 실제 화면에서 DOM 순서가 유입·예약 순서인지 확장 리로드 후 화면값과 CSV를 최종 대조해야 함.

## 2026-07-21 네이버 기간 수집 날짜 이동 간격 추가
- 대상: `runner_naver.js`.
- 변경: 기간 수집에서 날짜 처리 완료 후 다음 날짜 URL 이동 전 2.0~5.0초 무작위 대기를 추가하고 중단 요청을 100ms 단위로 반영.
- 검증 결과: JS 문법, 지연 범위·기간 모드·마지막 날짜 제외 계약, UTF-8 검사 통과.
- 남은 위험: 실제 기간 수집 로그에서 날짜별 대기 시간이 범위 안에 표시되는지 확장 리로드 후 확인 필요.

## 2026-07-21 일별 상품 매출 리포트 집계 기준 보완
- 대상: `scripts/analysis/susam_report.py`, `Strategy_SusamReport_01_FlowUpload_Dags.py`, 전용 회귀 테스트.
- 변경: 전체 상품을 `order_date, store, item_name`으로 묶고 `qty`, `총매출액`을 합산하도록 수정했으며 DAG에 명시적 `force: true` 재업로드 옵션을 추가.
- 검증 결과: Docker 임시 데이터 기능 검증 통과. 07-03 수정본은 3,836행·64개 매장·수량 15,582·총매출액 57,308,360원으로 원본 합계 일치, Flow 드래그앤드롭·Enter·DOM 확인 후 DAG 성공.
- 남은 위험: 기존 07-01·07-02 및 07-03 최초 댓글의 구형 첨부는 Flow에서 자동 교체되지 않아 별도 정리가 필요함.

## 2026-07-21 일별 상품 매출 07-01~07-19 Flow 순차 업로드
- 대상: `scripts/analysis/susam_report.py`, `Strategy_SusamReport_01_FlowUpload_Dags.py`.
- 변경: DAG start/end 범위 실행, 날짜순 단일 브라우저 세션 업로드, ChromeDriver 연결 종료 시 새 세션 1회 재시도와 성공 날짜 재개 기능을 추가.
- 검증 결과: 전용 테스트 12건·Docker 문법 검사 통과. 19개 XLSX 구조·날짜·그룹 중복 검증 통과, Flow 댓글 19건 DOM 확인 및 최종 재개 DAG 성공.
- 남은 위험: 첫 범위 실행은 ChromeDriver 종료로 07-05 이후 실패 기록이 Airflow 이력에 남았으나, 07-06~19 재개 실행은 전부 성공했고 디버그 포트는 종료함.

## 2026-07-21 네이버 통계 카드 범위 및 매장명 표시 보완
- 대상: `content/07_naver.js`, `runner_naver.html`, `runner_naver.js`.
- 변경: 페이지 전체 8개 숫자 대신 플레이스 유입 카드와 같은 그룹의 바로 다음 요약 카드를 예약으로 연결하고, 대시보드에 송파삼전점 매장명을 표시했으며 중단 후 지연 라이브 로그를 차단.
- 검증 결과: 두 JavaScript 파일 문법 검사, 숫자 8개 화면의 `344/1` 선택 모형, 공통 영역 숫자 8개 오수집 거부, UTF-8 검사 통과.
- 남은 위험: 실제 네이버 화면에서 유입 344·예약 1로 수집되는지는 확장 리로드 후 1회 확인 필요.

## 2026-07-21 네이버 예약 카드 인접 순서 폴백 보완
- 대상: `content/07_naver.js`.
- 변경: 실제 DOM에서 유입·예약 카드의 공통 영역에 다른 요약 카드도 포함되는 점을 반영해, 라벨로 확정한 유입 숫자의 바로 다음 별도 `ReportSummary_info` 숫자를 예약으로 읽도록 수정.
- 검증 결과: 표시 숫자 8개 모형에서 유입 344·예약 1 추출, JavaScript 문법과 UTF-8 검사 통과.
- 남은 위험: 확장 리로드 후 실제 화면값과 CSV를 1회 대조해야 함.

## 2026-07-21 네이버 실제 지표 라벨 및 URL 날짜 검증 적용
- 대상: `content/07_naver.js`.
- 변경: 순서 기반 폴백을 제거하고 같은 통계 목록의 `플레이스 유입`·`예약·주문 신청` 라벨로만 값을 읽으며, 카드 내부 날짜 대신 URL의 `startDate/endDate/term`으로 요청 날짜를 검증.
- 검증 결과: 제공 DOM 모형에서 유입 344·예약 1·안정화 3회 성공, 날짜 불일치·예약 누락·중복 목록·로딩 상태 실패 경로와 JS 문법·UTF-8 검사 통과.
- 남은 위험: 크롬 확장 리로드 후 실제 네이버 화면과 생성 CSV를 1회 대조해야 함.

## 2026-07-21 수삼 일별 리포트 09시 자동 업로드
- 대상: `Strategy_SusamReport_01_FlowUpload_Dags.py`, `schedule.py`, Flow Chrome Windows 실행기와 전용 테스트.
- 변경: 매일 09:00 KST에 전일을 기본 대상으로 삼고, UnifiedSales 당일 갱신·10분 안정화와 당일 DAG 종료를 기다린 뒤 Docker에서 Flow 업로드하도록 구성. Windows 작업은 08:55 Chrome을 열고 완료 마커 또는 11:10에 닫음.
- 검증 결과: 전용 테스트 19건, Docker DAG import와 하네스 검증 통과. 07-20 예약 실행 실제 업로드 성공(3,181행·54개 매장·수량 13,881·49,326,166원), 재검증 실행은 업로드 건너뛰기 및 Chrome 즉시 종료 확인.
- 남은 위험: Windows 사용자가 로그인된 상태여야 하며 Flow 전용 Chrome 프로필의 로그인 세션이 만료되면 수동 재로그인이 필요함.

## 2026-07-22 배민 수집 Telegram 최종 알림 단일화
- 대상: 배민 Main·PC2·Upload·Retry DAG, Upload/Retry 파이프라인, 공통 notifier와 전용 테스트.
- 변경: 수집·검증·중간 Retry Telegram을 제거하고, Upload 무재시도 또는 terminal Retry에서만 대상·완료·잔여 실패와 미해결 매장을 1건으로 발송하도록 수정. ToOrder 상세의 잘못된 0원 표시 키도 교정.
- 검증 결과: 알림·검증·Retry·inbox 테스트 34건 통과, 관련 Python 문법 검사와 scheduler 컨테이너의 4개 DAG import 통과.
- 남은 위험: 기존 driver recovery 테스트 중 알림과 무관한 실패 2건과 30초 초과 1건이 남아 있으며, 다음 예약 실행에서 Telegram이 실제 1건만 오는지 운영 확인 필요.

## 2026-07-22 쿠팡 로그인 스로틀 회로차단기
- 대상: OneDrive 개발본과 `coupang_extension_build/runner.js`.
- 변경: 로그인 스로틀이 3회 연속 발생하면 백필·정규·재시도 루프를 중단하고, 미완료 매장을 기존 백필 및 2~3시간 자동재시도 경로로 이관하도록 별도 상태를 추가.
- 검증 결과: 두 JavaScript 파일 문법 검사, 세 루프 삽입·초기화·자동재시도 게이트 보존 및 UTF-8 검사 통과.
- 남은 위험: 확장 리로드 후 실제 연속 스로틀 상황에서 3회째 즉시 중단되고 자동재시도가 예약되는지 운영 확인 필요.

## 2026-07-22 쿠팡 로그인 일시 오류 확인 재시도
- 대상: OneDrive 개발본과 `coupang_extension_build/runner.js` 로그인 판정 흐름.
- 변경: 첫 권한 오류는 스로틀로 집계하지 않고 3초 후 같은 페이지에서 1회 재클릭하며, 기존 오류 스팬을 5초간 무시한 뒤 동일 오류가 재확인될 때만 스로틀로 확정.
- 검증 결과: 두 JavaScript 파일 문법 검사, 재클릭·오류 유예·확정 판정 블록 동기화와 UTF-8 검사 통과.
- 남은 위험: 확장 리로드 후 첫 자동 클릭 실패·두 번째 클릭 성공 및 두 번째도 실패하는 두 운영 경로를 각각 확인해야 함.

## 2026-07-22 UnifiedSales 월별 검증 알림 압축
- 대상: `DB_UnifiedSales_validate.py` 월별 Telegram 메시지와 전용 회귀 테스트.
- 변경: 월별 알림은 대상월·비교범위·CSV 경로·오차 매장 순위까지만 표시하고 채널별 상세내역을 제외하며, 일별 알림 상세내역은 유지.
- 검증 결과: 월별 압축·일별 유지 테스트 2건, 모듈 임포트, scheduler 컨테이너 DAG 파싱 및 UTF-8 검사 통과.
- 남은 위험: 다음 월별 예약 실행에서 실제 Telegram 메시지가 한 건의 요약 형태로 수신되는지 운영 확인 필요.

## 2026-07-22 delivery_commission 도리당·나홀로 합산 복구
- 대상: `DB_DeliveryCommission.py`, 전용 회귀 테스트, OneDrive 쿠팡 주문 원천과 delivery_commission 마트.
- 변경: 브랜드별 주문 중복 제거 후 지점 단위 합산, 나홀로 광고비 매장 키 보정, 원천 읽기 실패 시 기존 마트 보존과 검증 후 교체를 적용.
- 운영 복구: 쿠팡 주문 parquet 264개를 로컬 유지로 전환하고 마트를 9,731행·최신일 2026-07-21로 재생성했으며 DAG를 활성화.
- 검증 결과: 테스트 2건, 원천 전체 읽기, 키 중복·NULL·금액 산식 검사와 예정/수동 Airflow 실행 2건 모두 통과.
- 남은 위험: OneDrive 신규 원천이 온라인 전용 상태로 생성되면 DAG가 부분 overwrite 대신 실패하므로 실패 로그에서 대상 파일 동기화가 필요.

## 2026-07-22 수삼 리포트 Flow 댓글 붙여넣기 대기 계획 보완
- 대상: `prd_codex/수삼리포트CSV댓글업로드_260722.md`.
- 변경: 긴 표 본문을 브라우저 클립보드에서 Ctrl+V한 뒤 전체 문자열 일치를 최대 30초 확인하고 Enter를 보내도록 구현·테스트 지시를 보완.
- 검증 결과: 계획서의 입력 순서, 지연·타임아웃 테스트, 실패 시 Enter·상태 저장 금지 조건과 UTF-8을 확인.
- 남은 위험: 실제 Flow Clipboard API 권한과 대용량 붙여넣기 반영은 최초 라이브 Chrome 실행에서 육안 확인 필요.

## 2026-07-22 수삼 CSV 댓글 업로드 구현 및 07-01 단건 등록
- 대상: 수삼 리포트 스크립트·DAG 문구·전용 테스트와 Flow 2026-07-01 댓글.
- 변경: 수삼 필터 CSV/표 본문 생성, 브라우저 Ctrl+V 후 전체 반영 대기, 첨부 제거, 누락 ChromeDriver 동일 버전 자동 복구를 구현.
- 운영 결과: 2026-07-01 수삼 행 0건을 확인하고 `수삼 판매 내역 없음` 댓글 1건 등록 완료. OneDrive CSV는 승인 없이 생성하지 않음.
- 검증 결과: 전용 단위 테스트 15건과 문법·UTF-8 검사 통과, 라이브 Flow DOM 댓글 증가 확인.
- 남은 위험: 다음 예약 실행 전용 Chrome 로그인 세션 유지 여부와 다건 대용량 본문은 운영 확인 필요.

## 2026-07-22 수삼 Flow 댓글 07-02~07-20 순차 등록
- 대상: Flow 수삼 리포트 고정 게시글의 2026-07-02~2026-07-20 댓글 19건.
- 운영 결과: 날짜순으로 Ctrl+V·전체 본문 반영·Enter·신규 DOM 증가를 확인하며 19건 모두 등록, 실패 0건.
- 검증 결과: 총 리포트 200행, 날짜별 본문 22~3,299자이며 가장 긴 07-15도 전체 3,299자 반영 후 등록 확인.
- 남은 위험: 없음. OneDrive CSV는 승인 없이 생성하지 않고 원천 parquet만 읽음.

## 2026-07-22 수삼 DAG worker 원격 입력 복구 및 07-21 적재
- 대상: `susam_report.py`, 전용 테스트, `Strategy_SusamReport_01_FlowUpload_Dags` 2026-07-21 실행.
- 원인/수정: worker→Windows Chrome의 클립보드·키 포커스가 무효여서 입력과 Enter가 반영되지 않음. Ctrl+V/일반 Enter 우선 후 DOM setter·KeyboardEvent fallback과 전체 본문/신규 댓글 검증을 추가.
- 검증 결과: 단위 테스트 16건 통과, 최종 DAG 네 태스크 모두 성공, CSV 12행·컬럼·날짜·수삼 필터와 업로드 상태 및 Flow 완료 로그 확인.
- 산출물: OneDrive `mart/Flow/Susam_2026-07-21.csv`, Flow 2026-07-21 댓글 1건.
- 남은 위험: 원격 Chrome 창이 여러 감시 프로세스에서 재사용되면 이전 제한시간에 닫힐 수 있으므로 실행 전 새 창 여부 확인 필요.

## 2026-07-22 배민 매크로 2대 PC 상·하위 인박스 분리
- 대상: 배민 staging/export, Main DAG, Compose 역할 전달, 팀 설정 문서와 인박스 회귀 테스트.
- 변경: PC 역할별 매장 절반 분할과 `manual__top__*`/`manual__bottom__*` 폴더 분리, 현재 중앙 PC `top` 환경 적용.
- 검증 결과: 전용 테스트 16건, 컨테이너 Main/Pc2/Upload DAG import, 상·하위 무중복 분할, scheduler·worker 역할값과 healthy 상태 확인.
- 남은 위험: 두 번째 PC에는 `BAEMIN_MACRO_ROLE=bottom` 설정과 컨테이너 재생성이 별도로 필요하며 OneDrive 실적재는 미실행.

## 2026-07-22 배민 analytics `.bak` 재발 방지
- 대상: 가격형 옵션 교정·주문 월 재파티션 일회성 스크립트와 관련 계획·회귀 테스트.
- 변경: OneDrive 원본 옆 백업 생성을 제거하고 `LOCAL_DB/temp/<script>/<run_id>`로 격리하며 검증 성공 또는 정상 롤백 후 정리하도록 수정.
- 검증 결과: 관련 테스트 12건, Python 문법·UTF-8 검사와 코드 검색에서 analytics sidecar 백업 생성 경로 제거 확인.
- 남은 위험: 재파티션을 `--verify` 없이 적용하면 복구용 로컬 백업은 의도적으로 유지되므로 필요 시 `LOCAL_DB/temp`를 수동 정리해야 함.

## 2026-07-22 Codex 백업 산출물 전역 정책 추가
- 대상: 루트 `AGENTS.md` 작업 산출물 규칙.
- 변경: 백업 기본 금지, 불가피한 백업의 `LOCAL_DB/temp/backups/<작업>/<실행ID>` 격리, 성공 후 정리와 OneDrive 영구 백업 별도 승인 원칙을 추가.
- 검증 결과: 루트 정책 단일 원본 유지, 하위 guidance 무변경, UTF-8·중복 검색·diff 검사를 통과.
- 남은 위험: 기존 코드의 과거 백업 구현은 개별 작업 시 새 전역 규칙에 맞춰 점진적으로 교정해야 함.
