# update_log

## 2026-08-31 쿠팡이츠 원천 CSV 영업관리부 경로 전환
- 대상: `scripts/coupang_host_chrome.ps1`, `scripts/coupang_boot_autostart.ps1`, `scripts/coupang_runner_autoclick.py`, OneDrive `Collect_Data`.
- 변경: 쿠팡 자동수집 다운로드/감시 기준을 `Collect_Data/영업관리부_수집` 우선으로 바꾸고, 자동 클릭 기본 확장 경로를 현재 Chrome에 등록된 OneDrive 개발용 확장으로 맞췄다.
- 운영 처리: `Collect_Data/마케팅_수집`의 `coupangeats*` 271개를 `영업관리부_수집`으로 이동 후 `DB_CoupangMacro_Load_Dags`를 수동 트리거했다.
- 검증 결과: DAG `manual__2026-08-31T00:43:56+00:00` 성공, orders 87개/7,412행, cmg 98개/196행, options 86개/13,642행 적재 및 원천 271개 삭제 확인.
- 남은 위험: 현재 실행 중인 Chrome 프로세스의 다운로드 pref는 다음 Chrome 완전 종료 후 자동 스크립트 실행 시 `영업관리부_수집`으로 교정된다. pref가 틀린 상태에서는 runner 직접 실행 fallback을 막아 오저장을 방지한다.

## 2026-08-28 Food Guide 주문내역 자동 로딩 대기 수정
- 대상: `Food_Guide_orders_collect_Dags`, `modules/transform/pipelines/db/Food_Guide_orders_collect.py`, `tests/test_food_guide_orders_collect.py`.
- 원인: Food Guide 주문내역 화면 로딩이 긴데 수집기가 `총0건/데이터 없음`을 성공 no-op으로 확정해 실제 5천 건대 데이터를 놓칠 수 있었다.
- 변경: 0건 성공 처리를 기본 비활성화하고, `조회` 클릭 후 그리드 `totalCnt`/행을 최대 600초까지 기다리도록 수정했다. `div.total .sum`과 `(총N건)` 그룹 텍스트를 함께 읽어 `5,345` 같은 분리 DOM 카운트를 인식한다.
- 검증 결과: `tests/test_food_guide_orders_collect.py` 9 passed, `py_compile` 성공, Airflow 컨테이너 DAG import 성공. 지정 run 재실행 결과 `주문 예정 목록.xlsx` 다운로드 및 `food_guide_orders_202608.parquet` 5,350행 저장.
- 남은 위험: 화면 카운트 5,345와 변환 후 5,350행 차이는 원본 엑셀 기준 추가 확인 필요. 이번 테스트 실행으로 OneDrive mart가 갱신되었으므로 이후 운영 반영 여부를 확인해야 한다.

## 2026-08-27 네이버광고 실패분 라운드 재시도 및 rowKey 소재 파서 수정
- 대상: OneDrive 개발용 확장 `runner_naverads.js`, `content/09_naverads.js`.
- 변경: 상세 수집 재시도를 광고그룹 내부 즉시 반복에서 라운드 방식으로 전환해 1차 전체 수집 후 2~3차는 실패분만 재시도하도록 수정. 플레이스 소재 파서에서 `rowKey`/`nad-...`를 소재 ID로 우선 사용하고, 공백 없이 붙은 지표와 0원 보류 소재도 depth3로 분리.
- 검증 결과: 두 JS 파일 `node --check` 통과, 로그 샘플 기반 모의 소재행 3건이 모두 `depthNo=3`과 `creativeId`로 파싱됨.
- 남은 위험: 실제 재실행 후 파워링크 키워드 탭이 계속 소재 테이블로 감지되는지와 최종 CSV의 `depth3` 행 수를 확인해야 한다.

## 2026-08-27 네이버광고 depth3 누락 재시도 판정 및 소재 지표 분리
- 대상: OneDrive 개발용 확장 `runner_naverads.js`, `content/09_naverads.js`.
- 변경: CSV `depth번호`를 `depth1/depth2/depth3` 문자열로 저장하고, 상세 표/요약이 보이는데 정제 행이 0개인 경우 성공 fallback이 아니라 실패로 처리해 광고그룹 단위 최대 3회 재시도하도록 수정. 공백 없이 붙은 `nad-...` 플레이스 소재 ID+지표 문자열을 depth3 소재행으로 분리하는 파서를 추가.
- 검증 결과: 두 JS 파일 `node --check` 통과, 플레이스#5 샘플 지표 문자열 4건에서 노출수/클릭수/CTR/평균CPC/총비용 분리 확인.
- 남은 위험: 실제 네이버 UI에서 키워드 탭이 계속 소재 테이블로 감지되는 파워링크 그룹은 재실행 로그의 새 진단으로 추가 보정이 필요할 수 있다.

## 2026-08-27 네이버광고 발견 단계 재시도 및 날짜/소재 파서 보강
- 대상: OneDrive 개발용 확장 `runner_naverads.js`, `content/09_naverads.js`.
- 변경: 캠페인 상세의 광고그룹 발견 실패도 최대 3회 재시도하고, 최종 실패 캠페인은 depth1-only CSV 저장에서 제외. 단일 날짜 검증을 엄격화하고, 광고그룹/소재 timeout 최종 상태에서 행+지표가 보이면 정상 인정하며, 평균 CPC 없는 4지표 플레이스 소재 행과 0원 보류 소재를 depth3로 파싱하도록 보강.
- 검증 결과: 두 JS 파일 `node --check` 통과, runner/content version `discovery-retry-v1`, `MAX_CAMPAIGN_ATTEMPTS=3`, 전체 배치 재시도 마커 제거 확인.
- 남은 위험: 네이버 실제 UI의 날짜 선택 팝업 구조가 다르면 `DATE_SET_FAILED`가 늘 수 있으므로 다음 로그에서 날짜 버튼 텍스트와 재시도 성공 여부를 확인해야 한다.

## 2026-08-27 네이버광고 실패 광고그룹 단위 재시도 전환
- 대상: OneDrive 개발용 확장 `runner_naverads.js`, `content/09_naverads.js`.
- 변경: 전체 배치 재시도와 저장 전 depth3 0행 실패 판정을 제거하고, 명시 실패한 광고그룹 상세 수집만 최대 3회 재시도하도록 축소. fallback/데이터 없음은 기존 저장 흐름을 유지.
- 검증 결과: 두 JS 파일 `node --check` 통과, `MAX_COLLECTION_ATTEMPTS`/`runCollectionAttempt` 제거 및 `MAX_ADGROUP_ATTEMPTS=3` 확인.
- 남은 위험: 실제 실패 재시도 성공 여부는 확장 리로드 후 로그의 `광고그룹 재시도 n/3`로 확인해야 한다.

## 2026-08-27 네이버 광고 CSV depth1/2/3 계층 저장 전환
- 대상: OneDrive 개발용 확장 `content/09_naverads.js`, `runner_naverads.js`.
- 변경: 캠페인 목록 지표를 보존하고 CSV를 캠페인(depth1), 광고그룹(depth2), 키워드/소재(depth3) 순서로 조립하도록 수정. 파워링크 광고그룹 목록에서 URL 컬럼 때문에 상태·입찰가·지표가 밀리던 parser를 지표 패턴 기반으로 보강.
- 검증 결과: `node --check` 2개 파일 통과, 파워링크 광고그룹 합성 row에서 광고그룹명/상태/기본입찰가/지표 정렬 확인, 계층 CSV 샘플에서 depth1→depth2→depth3 출력 확인.
- 남은 위험: Chrome 확장 리로드 후 실제 네이버 광고 재실행으로 `도리당_파워_링크`, `2607_1km반경`, `✅ 플레이스#5_광역_주말` CSV의 depth 분포와 한글 표시를 확인해야 함.

## 2026-08-27 네이버 광고 플레이스 소재행 및 fallback 저장 보강
- 대상: OneDrive 개발용 확장 `content/09_naverads.js`, `runner_naverads.js`.
- 변경: 플레이스 소재 요약행을 저장 대상에서 제외하고 `nad-...` 실제 소재행만 group3로 저장하도록 파서를 보강. 소재/키워드 상세가 비거나 정제 행이 없으면 광고그룹 목록 실적을 fallback row로 저장해 캠페인 파일 누락을 방지.
- 검증 결과: `node --check` 2개 파일 통과, 합성 플레이스 소재 원문에서 분리 TD/뭉친 row 모두 소재 ID·소재명·지표 정제 확인.
- 남은 위험: Chrome 확장 리로드 후 `✅ 플레이스#5_광역_주말` 실제 소재 4행과 `✅ 플레이스#1_광역_주말(26.08.14)` fallback CSV 생성을 재실행 로그로 확인해야 함.

## 2026-08-25 BSP KPI SMTP 인증 실패 운영 알림
- 대상: `SMP_bsp_kpi_weekly.py`, `tests/test_bsp_kpi_weekly.py`.
- 변경: BSP KPI 담당자 메일 발송 실패를 성공으로 기록하지 않도록 판정하고, Gmail SMTP 인증 실패 시 앱 비밀번호 재발급 경로와 Airflow Connection 수정 위치를 텔레그램으로 1회 안내.
- 검증 결과: `python -m pytest tests/test_bsp_kpi_weekly.py -q --basetemp=.tmp\pytest-bsp-kpi-mail-auth -o cache_dir=.tmp\pytest-cache` 34건 통과, `py_compile` 통과.
- 남은 위험: SMTP 앱 비밀번호 자체는 운영자가 Airflow UI에서 직접 갱신해야 하며, 실제 메일 재발송은 별도 실행 필요.

## 2026-08-20 메뉴계층 음료주류 콜라/새로 종류별 분리
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 수익률 음료 대표명에서 `펩시콜라 355ml (캔)`, `펩시 제로 355ml (캔)`, `새로 360ml`, `새로 다래`, `새로 오미자`를 각각 별도 품목으로 분리. 기존 `품목|<채널>|콜라`/`품목|<채널>|새로` 수기값은 새 세부 행에 fallback 이관하고 메모에 종류별 재검토 필요를 표시.
- 검증 결과: `py_compile` 통과, `.tmp/pytest` basetemp 기준 메뉴계층 전체 테스트 193개 통과. 승인 후 OneDrive `01_수기입력.xlsx` 수익률 시트에 세부 음료 행이 반영되고 기존 묶음 수기값 이관 메모가 표시됨을 확인.
- 남은 위험: 산출물 쓰기는 완료됐지만 최종 DAG 검증은 원천 신규 상품표 누락 `product_gap:2`, `tmp_item:1` 때문에 실패. 대상은 `posfeed`의 `[닭한마리] 우거지 백도리탕`, `[단짠단짠] 순살 갈비찜닭` 신규/임시 상품 건으로 음료 분리 변경과는 별도 확인 필요.

## 2026-08-20 배민 수동 수집 파일명 계약 복구
- 대상: `coupang_extension_build/content/02_baemin.js`.
- 변경: 배민 수동 저장 함수가 `[브랜드][매장]orders_YYYY-MM.csv`를 직접 만들지 않고 공통 `Utils.downloadCSV` 규칙을 사용해 `baemin_orders_*.csv` 파일명으로 저장되도록 수정.
- 검증 결과: 저장소/OneDrive 개발용 `content/02_baemin.js` 모두 `node --check` 통과, 정적 파일명 패턴 확인 및 UTF-8 읽기 통과.
- 남은 위험: 승인 후 OneDrive 개발용 확장 파일에도 동일 변경을 반영했으며, 실제 크롬 확장에는 재로드가 필요.

## 2026-08-20 메뉴계층 홀 치즈 리뷰서비스 분류 보존
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 수익률 마스터 생성 시 `리뷰서비스`/`요청사항` option_kind를 `재료추가`로 바꾸던 변환을 제거해 홀 유료 `모짜렐라 체다 치즈 추가`와 0원 `리뷰)모짜렐라 체다 치즈`가 서로 다른 성격으로 남도록 수정.
- 검증 결과: `py_compile` 통과, `.tmp/pytest` basetemp 기준 메뉴계층 전체 테스트 191개 통과. 승인 후 컨테이너 `build_orders(debug_outputs=True)` 성공(`2026-07:8645`, `2026-08:4695`), `01_수기입력.xlsx` zip/openpyxl 정상, `수익률` 시트에서 유료 치즈는 부모메뉴별 `재료추가`, `리뷰)모짜렐라 체다 치즈`는 `리뷰서비스`로 분리 확인.
- 남은 위험: `profit_missing`, `zero_price_cost_missing` 등 원가 입력 대기 WARN은 남아 있음.

## 2026-08-20 배민 Retry DAG generic Telegram 중복 알림 억제
- 대상: `plugins/telegram_notifier_plugin.py`, `tests/test_telegram_notifier_plugin.py`.
- 변경: Airflow 전역 Telegram 플러그인의 generic 실패 알림에서 `DB_Beamin_Macro_Dags_Retry.notify_and_trigger_next`를 억제해 전용 `[배민 최종 결과]` 알림과 중복 발송되지 않도록 조정.
- 검증 결과: `python -m py_compile plugins/telegram_notifier_plugin.py` 통과, `python -m pytest tests/test_telegram_notifier_plugin.py -q --basetemp=.tmp\pytest -p no:cacheprovider` 3건 통과, UTF-8 읽기 통과, 컨테이너 플러그인 import와 억제 함수 확인 통과.
- 남은 위험: 플러그인은 Airflow 프로세스 재시작 후 로드되므로 scheduler/worker/webserver 재시작 전까지 기존 프로세스에는 반영되지 않을 수 있음.

## 2026-08-20 자동복구 Telegram 최종 실패 중심 조정
- 대상: `watch_heal_queue.py`, `tests/test_autoheal_watcher.py`.
- 변경: 자동복구 watcher Telegram을 기본 `failures_only`로 조정해 skip/시작/성공 알림은 억제하고, Codex 실패·시간초과·데이터 파일 오류처럼 대응 필요한 최종 실패만 발송. 특히 `manual_only`/`pending_retry` skip은 발송 함수 자체를 호출하지 않고 queue 상태에 `notification_suppressed=true`만 기록하도록 차단.
- 검증 결과: `python -m py_compile watch_heal_queue.py` 통과, `python -m pytest tests/test_autoheal_watcher.py -q --basetemp=.tmp\pytest -p no:cacheprovider` 15건 통과, UTF-8 읽기 통과.
- 남은 위험: DAG 실패 원본 알림은 그대로 유지되므로 `[DAG 실패]` 반복 알림이 많으면 별도 정책 조정이 필요.

## 2026-08-15 금일 DAG 오류 회복
- 대상: `modules/transform/utility/mailer.py`, `dags/strategy/Strategy_FdamCS_01_Process_Dags.py`, `modules/transform/pipelines/db/DB_OKPOS_Card_Test.py`, Airflow 메타 상태.
- 변경: SMTP 인증 실패가 알림 전용 task 전체 실패로 전파되지 않도록 메일 발송 실패를 반환값으로 처리하고, OKPOS 카드 조회에서 대상 매장 행이 없는 경우 오늘 primary 0건 skip 경로로 분리.
- 검증 결과: DAG import 오류 없음, `py_compile` 및 UTF-8 읽기 통과. 금일 SMTP 실패 DAG와 OKPOS no-row DAG를 재실행/상태 정리해 success 또는 skipped로 회복.
- 남은 위험: `DB_Beamin_Macro_Dags_Retry` 3건은 실제 배민 수집 부분완료(잔여 실패 계정/ad_funnel 잔존)라 성공 처리하지 않음. Gmail SMTP 연결 비밀번호는 여전히 교체 필요.

## 2026-08-12 DB_MenuHierarchy_Test 세트/단품 수익키 및 부모옵션 검증
- 대상: `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`
- 변경: canonical 메뉴키에서 말미 `세트` 제거를 중단해 단품/세트 원가 키가 섞이지 않게 하고, 부모 main과 자식 `닭유형`/`사이즈` 옵션 충돌 및 세트/단품 동일 수익키 혼재를 WARN 검증으로 추가.
- 검증: `py_compile` 통과, `PYTHONPATH=C:\airflow pytest tests\test_menu_hierarchy_no_model_classification.py -q --basetemp=.tmp\pytest -p no:cacheprovider` 102개 통과, 컨테이너 DAG import 통과.
- 남은 위험: OneDrive 산출물은 아직 재생성하지 않음. DAG 재실행 후 새 키로 분리된 단품/세트 원가 행은 수기 입력 필요.

## 2026-08-12 DB_MenuHierarchy_Test 홀포장 수익키 및 원가 기반 수기수익
- 대상: `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`
- 변경: OKPOS `홀_포장`을 `수익채널=홀_포장`으로 분리하고, 27번 수익 입력표에서 `*_율_manual`/`수익률_manual` 입력을 제거해 판매가·원가 금액 기준으로 수익률을 자동 계산. `홀_포장`도 수수료 완결률 분모에서 제외.
- 검증: `py_compile` 통과, `PYTHONPATH=C:\airflow pytest tests\test_menu_hierarchy_no_model_classification.py -q --basetemp=.tmp\pytest -p no:cacheprovider` 98개 통과, `manual__codex_commission_hall_takeout_20260812T1954` 재실행 성공.
- 남은 위험: 수기원가 미입력 4개 source 그룹과 재료단가/공헌이익 WARN은 입력 대기 상태. `_debug/latest`는 `debug_outputs=False` 기본값이라 이번 DAG에서 갱신되지 않음.

## 2026-08-11 UnifiedSales 수동배달 무영업일 알림 억제
- 대상: `modules/transform/pipelines/db/DB_UnifiedSales_baemin.py`, `modules/transform/pipelines/db/DB_UnifiedSales_coupang.py`, `tests/test_manual_partial_detection.py`
- 변경: 배민/쿠팡 수동 원천과 POS/ToOrder 기준이 모두 0행인 날짜는 재수집 알림 이벤트에 넣지 않고, `alert_suppressed=true`, `reason=no_delivery_baseline` 마커만 남기도록 변경.
- 검증: `pytest tests/test_manual_partial_detection.py` 20건 통과, `DB_UnifiedSales_Total_Macro_Dags` import 통과.
- 남은 위험: 기준 행이 전부 없는 무영업일만 억제하며, POS/ToOrder 기준 금액이 있는 수동 결측·부분수집 알림은 기존대로 발송됨.

## 2026-08-10 DB_MenuHierarchy_Test 반반 닭수량 분해 및 하네스 문서 추가
- 대상: `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, `prd_codex/DB_MenuHierarchy_Test_하네스히스토리_260810.md`
- 변경: 반반 메뉴 `뼈+뼈`, `뼈+순살`, `순살+순살`을 확정 조합으로 처리하고 최종 주문서에 뼈/순살 사용량 분해 컬럼을 추가. 에이전트용 하네스 히스토리 문서를 신규 작성.
- 검증: `py_compile` 통과, `pytest tests/test_menu_hierarchy_no_model_classification.py --basetemp C:\airflow\.tmp\pytest -q` 60건 통과. 승인 후 `build_orders(None)` 재생성 완료, 최종 31,670행·PK unique 31,670·source별 완전분류율 100%·미분류 0행·분해합계 오류 0건 확인.
- 남은 위험: `profit_missing` 15,709건과 수익/재료 입력 대기 완결률 WARN은 분류 문제가 아니라 수익률·단가·중량 수기 입력 전 상태.

## 2026-08-10 DB_MenuHierarchy_Test 판정옵션 기반 완전분류 전환
- 대상: `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders`
- 변경: `01_수기입력.xlsx`에 `판정옵션` 시트를 추가하고, 추정/충돌/입력필요 행을 주문행 추가 없이 판정옵션 룰로 확정하도록 변경. `02_최종주문.csv`에서는 `판정상태`를 제거하고 `분류룰`만 남김.
- 검증: `pytest tests/test_menu_hierarchy_no_model_classification.py --basetemp C:\airflow\.tmp\pytest -q` 58건 통과, `py_compile` 통과, `build_orders(None)` 전체기간 31,663행 재생성 성공. source별 완전분류율 100%, 미분류 0행, blocking 0건 확인.
- 남은 위험: `profit_missing` 15,707건과 수익/재료 입력 대기 완결률 WARN은 분류 문제가 아니라 수익률·단가·중량 수기 입력 전 상태.

## 2026-08-10 DB_MenuHierarchy_Test compact 산출물 및 신뢰도 검증 전환
- 대상: `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders`
- 변경: 루트 산출물을 `01_수기입력.xlsx`, `02_최종주문.csv`, `03_요약.xlsx`, `04_사용법.md` 4개로 축소하고, 기존 CSV 입력값은 엑셀 시트 우선/legacy CSV fallback으로 이관. 최종 주문서에 `판정상태`를 추가해 source별 `확정/추정/충돌/입력필요`를 노출.
- 검증: `pytest tests/test_menu_hierarchy_no_model_classification.py --basetemp C:\airflow\.tmp\pytest -q` 58건 통과, `py_compile` 통과, `build_orders(None)` 전체기간 31,658행 재생성 성공. 루트 파일 4개, blocking 0건, WARN 16,212건 확인.
- 남은 위험: main 기준 `입력필요 438행`, `추정 133행`, `충돌 56행`은 계산 가능하지만 원천 신뢰도가 낮아 `03_요약.xlsx`의 `source별_검증`과 `미해결_TOP`에서 수기 보정해야 함.

## 2026-08-10 DB_MenuHierarchy_Test 송파삼전점 전체 주문 검증 보완
- 대상: `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders`
- 변경: `12_orders_left.csv` 수기수익 컬럼 누락 가드, 구 `옵션조합` 입력의 `닭옵션키` 이관, 수익률 미입력 표시 분리, 입력 대기 완결률 후퇴 WARN 처리, 닭개장 사용량 보완을 적용.
- 검증: `pytest tests/test_menu_hierarchy_no_model_classification.py --basetemp C:\airflow\.tmp\pytest -q` 58건 통과, `py_compile` 통과, `build_orders(None)` 송파삼전점 전체 31,658행 재생성 성공.
- 남은 위험: `27_profit_rate_master.csv` 수익률 220건은 사용자 입력 전이라 `manual_profit`/수익합계는 미산출 상태가 정상.

## 2026-08-10 DB_MenuHierarchy_Test 수기수익률 산출 추가
- 대상: `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders`
- 변경: `27_profit_rate_master.csv` 품목별 수익률 입력표와 `28_manual_profit_summary.csv` 수기수익 요약을 추가하고, `12_orders_left.csv`에 수익키/수기수익 보조 컬럼을 추가.
- 검증: 새 수기수익 단위 테스트 3건 통과, `build_orders(None)` 재생성으로 27번 220행·28번 211행 생성, `품목|콜라` 수익키 확인.
- 남은 위험: 기존 메뉴계층 전체 테스트는 현재 코드와 테스트 기대 스키마(`옵션조합`)가 이미 달라 다수 실패하며, 수익률_manual 입력 전 `manual_profit` 완결률은 0%가 정상.

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

## 2026-07-22 쿠팡 로그인 연타 완화 적용
- 대상: OneDrive 정규 확장 `runner.js`와 쿠팡 봇탐지 계획 문서.
- 변경: 정상 계정 간 대기를 10~25초로 확대하고 로그인 쓰로틀/ACCESS_DENIED 시 같은 페이지 즉시 재클릭을 제거. 쿠키 보존·사람형 입력 위장은 안전 검토상 미적용.
- 검증 결과: `runner.js`, `content/04_auto_login.js` Node 문법 검사와 manifest JSON 파싱 통과, 사본 파일 무변경 확인.
- 남은 위험: Akamai 쿠키 보존 없이 2번째 계정 로그인 성공 여부는 실제 브라우저 소규모 배치에서 확인해야 함.

## 2026-07-22 delivery_commission 우리가게클릭 0원 처리 기준 명확화
- 대상: `DB_DeliveryCommission수집커버리지누락진단리포트_260722.md`.
- 변경: 배민 주문서 날짜×매장에 우리가게클릭을 결합하고, 미매칭 행·결측 광고지출은 별도 누락 구분 없이 0원으로 계산하도록 명시.
- 검증 결과: 기존 `DB_DeliveryCommission.py`의 left join 및 `fillna(0)` 동작과 계획서 기준이 일치함을 확인.
- 남은 위험: 없음.

## 2026-07-22 Sales Employee 토더 계정 알림 오탐 방지
- 대상: `Sales_Employee_Extract_Dags.py`, 토더 누락 판정 모듈과 전용 회귀 테스트.
- 원인/변경: 동일 매장의 과거·신규 행을 개별 판정해 등록된 계정도 누락으로 알릴 수 있어, 매장 단위로 묶고 한 행에 ID/PW가 모두 있는 경우 정상 처리하도록 변경.
- 검증 결과: 전용·계정 테스트 9건, 컨테이너 DAG import·문법·UTF-8 검사 통과. 현재 시트에서 시흥장현점 2행 모두 정상이며 전체 누락 0건 확인.
- 남은 위험: 실제 누락 매장은 해결 전까지 예약 실행마다 텔레그램 알림이 반복되며, 구글시트와 OneDrive 데이터는 수정하지 않음.

## 2026-07-22 쿠팡 확장 자동로그인 권한 거절 오탐 수정
- 대상: OneDrive 개발용 확장 `runner.js`, `content/04_auto_login.js`.
- 변경: Akamai 권한 거절을 로그인 쓰로틀 분류에서 제외하고, 로그인 전 4초 워밍업·mousemove 및 문자 단위 입력·포인터 이벤트를 쿠팡 분기에만 추가.
- 검증 결과: 두 JS 파일 문법, 쓰로틀 분류, manifest JSON, UTF-8/BOM, 비대상 runner 무변경 검사를 통과했으며 `cookies` 권한은 미추가.
- 남은 위험: 합성 이벤트는 `isTrusted=false`이므로 실제 Chrome에서 단일 계정 로그인 후 소규모 배치 검증이 필요함.

## 2026-07-22 쿠팡 수동수집 F5 로그아웃 안전 중단
- 대상: OneDrive 개발용 확장 `content/03_coupangeats.js`, `content/05_main.js`.
- 변경: 수동 F5 재개 상태에 저장 시각을 기록하고, 6시간 안에 로그인 화면으로 떨어지면 보존 행을 `coupangeats_partial_orders_logout_*.csv`로 격리 저장한 뒤 재개 상태를 정리하도록 추가.
- 검증 결과: 두 JS 문법, 수동·최상위 프레임·유효시간 가드, 성공 후 상태 삭제 순서, 정상 변환 glob 미일치, UTF-8/BOM 검사를 통과. runner와 manifest는 무변경.
- 남은 위험: 실제 Chrome에서 F5 후 세션 만료 재현 시 부분 파일 단일 생성과 사용자 안내를 확인해야 함.

## 2026-07-23 Codex auto-heal 및 FinProduct 규칙 급증 복구
- 대상: autoheal watcher/supervisor, `DB_FinProduct_Map` 규칙 저장 경계와 검토 CLI.
- 변경: WSL 시작 시간초과·Windows fallback·claim lease·종료 보고를 추가하고, 30% 초과 규칙은 기존 활성본을 유지한 채 로컬 제안으로 격리하도록 수정.
- 검증 결과: 전용 테스트 13건, PowerShell/Bash 문법, 컨테이너 DAG import, 실데이터 dry-run(40→56, 40%)과 Windows watcher 재기동·heartbeat 갱신 통과.
- 추가 조치: 동일 DAG·태스크·장애유형은 일 1회만 실행하고 중복 알림은 억제했으며, 미지원 `codex-spark` 강제값을 제거해 CLI 기본 모델을 사용하도록 수정.
- 남은 위험: 보호된 예약 작업은 액션 교체 권한이 없어 동기 VBS 종료코드 전달로 보완했으며, OneDrive 쓰기 DAG 재실행은 승인 대기.

## 2026-07-23 PC2 하위 배민 업로드 당일 처리
- 대상: 배민 upload inbox 적재 파이프라인, 중앙 upload DAG, 신규 PC2 watcher DAG.
- 변경: bottom 폴더 감지 시 기존 upload DAG을 완료 대기 방식으로 트리거해 직렬 처리하고, 무대상 skip·전부 실패 경보·성공 폴더 기준 meta 병합을 추가.
- 검증 결과: 관련 테스트 44건, 하네스 75개 분류, 컨테이너 DAG import·task tree·pause·스케줄 확인 통과.
- 남은 위험: 신규 watcher는 중앙 PC에서만 수동 unpause해야 하며, 운영 OneDrive를 소비하는 `dags test`는 수행하지 않음.

## 2026-07-23 배민·쿠팡 수동 메뉴 상세 결손 방어
- 대상: UnifiedSales 공통 모듈과 배민·쿠팡 수동 교정 파이프라인, 전용 회귀 테스트.
- 변경: 빈 메뉴명을 `메뉴미상(배민/쿠팡)`으로 보정하고 WARNING, 로컬 마커, 매장·날짜별 1회 텔레그램 알림을 추가했으며 allocator 필수값 가드는 유지.
- 검증 결과: 전용·allocator 테스트 8건, 컨테이너 DAG import 통과. 실데이터 읽기 전용 재현에서 74행과 주문 `B2EP00E792` 매출 22,300원 보존 확인.
- 남은 위험: 기존 수동배달 회귀 묶음의 선행 불일치 5건은 별도이며, OneDrive 상품 마스터·통합 parquet을 쓰는 실제 Airflow 복구는 승인 대기.

## 2026-07-23 메뉴미상 item_id FinProduct 연결
- 대상: `DB_FinProduct_Map`, FinProduct Map/마트 DAG 스케줄과 연결 회귀 테스트.
- 변경: 배민·쿠팡 메뉴미상 대체 상품만 전 매장에서 규칙 승인하고 동일 item_id로 map·join·mart에 연결했으며 일반 상품 대상 매장은 유지.
- 검증 결과: 관련 테스트 19건, train JSON dry-run, 컨테이너 DAG import·09:30/10:00 스케줄, 실데이터 item_id `200004114` 연결 확인.
- 남은 위험: 운영 CSV 재생성과 DAG 복구는 OneDrive 쓰기 승인을 받은 뒤 수행해야 함.

## 2026-07-23 배민 상위 전체 수집 안전장치
- 대상: 배민 광고 funnel, staging inbox 메타, 중앙 upload 인증정보 복원.
- 변경: 상위 전체 설정을 유지하고 광고 카드 순서·비정상값 가드, 공유 메타 비밀번호 제거/중앙 재결합, 중앙 top 전용 export 직후 업로드 직렬 트리거를 추가(bottom은 직접 적재 금지).
- 검증 결과: 관련 테스트 46건, 컨테이너 compile/import, 하네스 75개 검증 통과; 다음 예약은 2026-07-24 03:15 KST.
- 남은 위험: 강동점 광고 실화면 전용 검증은 15분 내 종료되지 않아 광고는 실패 격리되며, 주문·NOW 수집과 잘못된 광고값 저장은 분리됨.

## 2026-07-23 fin_product_map 빈칸 LLM 분류 수정
- 대상: `DB_FinProduct_Map`의 map 전용 LLM 대상·마이그레이션·실패 표식과 DAG 알림.
- 변경: 판매이력 없는 map 행을 분류 대상에 포함하고 무효 응답 단건 재시도, `llm_unresolved`, 기존 행 보존, 공통 qwen client 재사용을 추가.
- 검증 결과: 신규·회귀 테스트 19건, Python 구문 검사, DAG import 통과; Ollama 후보 `qwen2.5:14b`, `gpt-oss:20b` 확인.
- 남은 위험: 실제 46행 재분류와 OneDrive CSV 갱신·실측은 별도 승인 후 수행해야 함.

## 2026-07-23 배달 수동수집 부분수집 감지 알림
- 대상: UnifiedSales 공통 모듈과 배민·쿠팡 수동 교정 파이프라인, 전용 판정·마커·알림 테스트.
- 변경: POS 대비 수동 합계가 80% 미만이고 부족액이 10만원 이상인 날과 수동·POS 모두 없는 날을 감지해 로컬 마커 기반 1회 Telegram 알림을 추가했으며 적재 로직은 유지.
- 검증 결과: 신규·관련 회귀 테스트 37건, 양 모듈·DAG import, UTF-8·diff 검사 통과; 2026-07-15 운영 parquet 해시·30,132행·합계 122,560,876원 전후 동일.
- 남은 위험: 다음 lookback 실행에서 과거 미해결 날짜의 신규 마커가 생성되며, 실제 Telegram 전송과 재수집은 운영 실행 시 확인 필요.

## 2026-07-23 배민 메뉴판 수동수집
- 대상: 개발용 크롬 확장 `content/02_baemin.js`의 배민 메뉴판 수동수집.
- 변경: 메뉴판 라우팅, 매장 식별, 가상 스크롤 전량 수확, 메뉴 파싱, 파이프라인 규격 CSV 저장과 다운로드 실패 처리를 추가.
- 검증 결과: 대상 JS 문법, UTF-8/LF, 부분일치 셀렉터, 안전상한·ESC·스크롤 복원, OneDrive 반영본 해시 일치를 확인.
- 남은 위험: 확장 리로드 후 실제 메뉴판 전량·가격·상태·적용매장·기존 주문 회귀 검증은 브라우저에서 수행해야 함.

## 2026-07-23 PC2 배민 적재 및 cleanup 정상화
- 대상: 중앙 배민 upload 배포 모듈·DAG와 top/bottom 충돌·중복 수집 회귀 테스트.
- 변경: ad_funnel 키 upsert, 원본 포맷 유지, 실행시각 순 폴더 처리, top 기본 패턴, cleanup 재시도와 빈 폴더 quarantine를 추가.
- 검증 결과: 관련 테스트 56건과 실제 bottom 2폴더·24파일 적재/cleanup, orders·ad_funnel·ToOrder 검증을 통과했으며 해운대중동점 주문번호 450개·ad_funnel 10개 날짜가 유지됨.
- 운영 조치: 신규 shop_change parquet 2개의 Windows ACL을 승인 후 부모 권한으로 복구했고, 복구 전후 해시 동일·Windows/컨테이너 양쪽 읽기를 확인함.
- 남은 위험: 신규 매장의 shop_change parquet가 처음 생성될 때 Windows ACL 상속 여부를 후속 실행에서 확인해야 함.

## 2026-07-23 배민 메뉴판 그룹형 DOM 0건 수정
- 대상: 개발용 크롬 확장 `content/02_baemin.js`의 메뉴판 후보 탐색.
- 변경: `menuItem-module__content` 그룹형 메뉴 행을 우선 수집하고, 의미 기반 행이 없을 때만 기존 `data-index` 가상 목록을 사용하도록 수정.
- 검증 결과: JS 문법·UTF-8/LF·반영본 해시와 그룹형 3건·중복 메뉴·가격·숨김·가상형 2건 회귀 모형 검사를 통과.
- 남은 위험: 확장 리로드 후 실제 메뉴판 DOM 행 수와 CSV 행 수 일치 여부는 브라우저에서 확인해야 함.

## 2026-07-23 배민 메뉴판 CSV 컬럼 정리
- 대상: 개발용 크롬 확장 `content/02_baemin.js`의 메뉴판 CSV 행·헤더·파일명.
- 변경: 로컬 `YYYY-MM-DD`의 `collected_date`를 추가하고 `store_name`을 `brand`·`store`로 분리했으며 `적용매장`을 제거.
- 검증 결과: `도리당 상도점`이 `brand=도리당`, `store=상도점`으로 기록되고 요청 파일명과 JS 문법·UTF-8·반영본 해시가 일치함을 확인.
- 남은 위험: 확장 리로드 후 실제 다운로드 CSV의 컬럼 순서와 값은 브라우저에서 확인해야 함.

## 2026-07-23 배민 메뉴 수집 대시보드 자동화
- 대상: 개발용 크롬 확장 `runner_baemin.js`, `runner_baemin.html`.
- 변경: 주문+메뉴 배치, 메뉴 전용 버튼·URL 자동시작, 계정별 전체 shopId 순회와 부분 실패 상태 처리를 추가.
- 검증 결과: `node --check runner_baemin.js`가 종료 코드 0으로 통과.
- 남은 위험: 확장 리로드 후 다중 shopId CSV, 중단, 부분 실패, 주문 수집 회귀는 브라우저에서 확인해야 함.
## 2026-07-23 배민 메뉴 전용 전체 수집 보정
- 대상: 개발용 크롬 확장 `runner_baemin.js`의 메뉴 전용 버튼 실행 범위.
- 변경: 선택 행이 없어도 메뉴 전용 버튼이 전체 계정 인덱스를 대상으로 실행하도록 `menuAll` 모드를 추가.
- 검증 결과: 사용자 요청에 따라 코드 변경만 적용했으며 별도 실행 검증은 수행하지 않음.
- 남은 위험: 확장 리로드 후 전체 계정 수와 시작 로그의 대상 수가 일치하는지 확인해야 함.
## 2026-07-23 배민 로그인 PW 실패 건너뛰기
- 대상: 개발용 크롬 확장 `runner_baemin.js`의 계정 로그인 처리.
- 변경: 로그인 오류 문구를 감지해 비밀번호 값 노출 없이 행 비고에 실패 사유를 기록하고 다음 계정으로 진행하도록 보완.
- 검증 결과: 사용자 요청에 따라 코드 변경만 적용했으며 별도 실행 검증은 수행하지 않음.
- 남은 위험: 실제 배민 로그인 오류 문구가 탐지 패턴과 다른 경우 일반 홈 이동 실패로 기록될 수 있음.
## 2026-07-23 배민 메뉴 대상 브랜드 제한
- 대상: 개발용 크롬 확장 `runner_baemin.js`의 계정별 메뉴 shopId 탐색.
- 변경: 숫자형 option 중 매장명에 `나홀로` 또는 `도리당`이 포함된 shopId만 수집하도록 제한.
- 검증 결과: 제공된 영월점 option 기준 박지혜 모던순대국·짬뽕도감은 제외되고 나홀로·도리당 2개만 대상이 됨.
- 남은 위험: 대상 브랜드의 option 표기가 변경되면 필터 키워드 갱신이 필요함.
## 2026-07-23 배민 메뉴 이동 후 수집 시작 보정
- 대상: 개발용 크롬 확장 `runner_baemin.js`의 메뉴 페이지 이동·수집 시작 단계.
- 변경: 다른 PC의 백그라운드 렌더링 지연을 피하도록 메뉴 탭을 활성화하고 페이지·content·수집 요청 단계 로그를 추가.
- 검증 결과: 사용자 제공 로그의 마지막 지점인 `메뉴 이동` 이후 처리 경계를 코드로 보강했으며 별도 실행 검증은 수행하지 않음.
- 남은 위험: 수정 후 마지막 단계 로그를 기준으로 다른 PC의 content 주입 또는 응답 문제를 추가 확인해야 함.
## 2026-07-23 배민 메뉴 수집 라이브 진단 로그
- 대상: 개발용 크롬 확장 `runner_baemin.js`, `content/02_baemin.js`의 메뉴 수집 대기·스크롤 처리.
- 변경: 대시보드 10초 heartbeat와 content 함수 진입·매장 식별·DOM 탐색·스크롤 위치·누적 행·스캔 완료 로그를 추가.
- 검증 결과: 사용자 요청에 따라 진단 코드만 적용했으며 별도 실행 검증은 수행하지 않음.
- 남은 위험: 확장 리로드 후 마지막 라이브 로그를 기준으로 실제 정지 지점을 확정해야 함.
## 2026-07-23 배민 메뉴 자동 진단 조치 로그
- 대상: 개발용 크롬 확장 `runner_baemin.js`의 메뉴 수집 heartbeat.
- 변경: URL·로그인 이탈, 로딩, 탭 가시성, 수집기 로드, 메뉴 DOM, 스크롤 컨테이너, 페이지 오류를 판별하고 해결 조치를 라이브 로그에 표시.
- 검증 결과: 사용자 요청에 따라 진단 코드만 적용했으며 별도 실행 검증은 수행하지 않음.
- 남은 위험: 사이트의 신규 DOM이 진단 셀렉터에도 잡히지 않으면 실제 화면 HTML 추가 확인이 필요함.
## 2026-07-23 배민 메뉴 STOP 래치 복구
- 대상: 개발용 크롬 확장 `runner_baemin.js`의 배치 시작과 메뉴 COLLECT 응답 처리.
- 변경: 새 배치 시작 시 이전 `ce_runner_stop_requested`를 제거하고 즉시 `stopped:true` 응답의 pending 타이머를 종료해 원인을 비고에 전달.
- 검증 결과: 숨김 탭을 단독 오류 판정에서 제외했고 `node --check runner_baemin.js`가 종료 코드 0으로 통과.
- 남은 위험: 실제 중단 버튼과 중단 후 새 배치 재실행은 확장 리로드 후 브라우저에서 확인해야 함.
## 2026-07-23 배민 계정 전환 탐색 경합 수정
- 대상: 개발용 크롬 확장 `runner_baemin.js`의 로그인·홈·메뉴 URL 이동 대기.
- 변경: 이전 self URL의 즉시 성공 오판과 로그인 후 고정 5초 대기를 제거하고 새 탐색 complete·실제 로그인 성공을 확인하도록 변경.
- 검증 결과: `node --check runner_baemin.js`가 종료 코드 0으로 통과.
- 남은 위험: 느린 PC의 연속 계정 전환과 잘못된 비밀번호 건너뛰기는 확장 리로드 후 브라우저에서 확인해야 함.
## 2026-07-23 배민 mypage 로그인 완료 판정
- 대상: 개발용 크롬 확장 `runner_baemin.js`의 자동 로그인 성공 조건.
- 변경: `biz-member.baemin.com/mypage`를 인증 완료로 인정해 불필요한 최대 70초 대기를 제거하고 즉시 self 홈으로 이동.
- 검증 결과: 사용자 로그의 실제 로그인 성공 URL을 기준으로 코드 변경만 적용했으며 별도 실행 검증은 수행하지 않음.
- 남은 위험: 배민이 인증 완료 경로를 변경하면 성공 URL 패턴 갱신이 필요함.
## 2026-07-23 PC2 배민 적재 ACL 작업자 WSL 전환
- 대상: `DB_Beamin_Macro_Upload_Pc2_Dags`의 적재 후 Windows ACL 확인 작업.
- 변경: PowerShell 작업자를 제거하고 숨김 VBS → `UbuntuCodex` WSL bash → 제한 경로 ACL 작업자로 교체했으며 WSLInterop 자동 등록을 추가.
- 검증 결과: 예약 작업 종료 코드 0, 실제 ACL 요청 왕복 1건 성공, 관련 테스트 44개 통과, UTF-8·파이썬 구문·diff 점검 통과.
- 남은 위험: WSL 배포판 등록이 제거되거나 작업 스케줄러 사용자 세션이 로그오프 상태이면 ACL 응답 timeout으로 inbox 원본을 보존하고 적재 정리를 중단함.
## 2026-07-24 배민 collect_all 배치 분할·이어받기
- 대상: `DB_Beamin_Macro_Dags` 상위 계정 수집의 300분 타임아웃과 재시도 초기화 문제.
- 변경: 상위 33계정을 3개 DAG로 분할하고 계정별 로컬 체크포인트, staging 이어받기, 진행률·예상 잔여 로그, 타임아웃 미완료 계정 XCom 처리를 추가.
- 검증 결과: 신규·회귀·inbox 관련 테스트 74개 통과, 로컬·scheduler 컨테이너 DAG 3개 import, 스케줄·진행 파일 위치·UTF-8 검증 통과.
- 남은 위험: harness validate는 기존 삭제 상태인 `DB_Beamin_Macro_Pc2_Dags.py` registry override 참조 1건 때문에 실패하며, 실제 운영 소요시간은 다음 예약 실행에서 확인 필요.
## 2026-07-24 FinProduct 1차 검수 강제·메인 분류 보정
- 대상: `DB_FinProduct_Map`의 0원·규칙·동일상품 자동승인과 미검수 join 반영, LLM 메인 메뉴 오분류.
- 변경: 자동승인을 모두 검수 대기로 환원하고 승인 행만 join에 반영하며, 명확한 단일 본식은 LLM 라벨과 무관하게 메인으로 보정.
- 검증 결과: 관련 테스트 23개, scheduler DAG 실행 성공, qwen 3건 샘플과 운영 CSV 재검증 통과. 자동승인 145건 환원 후 승인 623건·검수대기 151건·join 623건이며 미승인 join key는 0건.
- 남은 위험: LLM 미확정 7건은 1차 검수가 필요하고, 활성 규칙 40→66건 제안은 변화율 65%로 기존 규칙을 유지한 채 로컬 제안 파일에 격리됨.
## 2026-07-24 배민 상위 33계정 순차 배치 연결
- 대상: `DB_Beamin_Macro_Dags` B1·B2·B3의 실행 연결과 전체 계정 수집 범위.
- 변경: B1 완료 후 B2, B2 완료 후 B3를 결정적 run ID로 한 번씩 호출하고 B2·B3 독립 스케줄을 제거했으며, 매장 지정·명시적 연쇄 해제 실행은 다음 배치를 호출하지 않도록 처리.
- 검증 결과: 배민 관련 테스트 99개, 컨테이너 DAG import·스케줄·태스크 구조 검증 통과. B2·B3 일시정지 해제 후 현재 B1 수집은 중단 없이 계속 실행 중.
- 남은 위험: 현재 B1 완료 후 실제 B2·B3 연쇄 실행과 총 33계정 완료 여부를 후속 확인해야 하며, harness validate는 기존 삭제 상태인 `DB_Beamin_Macro_Pc2_Dags.py` override 참조 1건 때문에 실패함.
## 2026-07-27 배민 매크로 ad_funnel 실패 차단 및 운영시간 전환
- 대상: `DB_Beamin_Macro_Dags`, `DB_Beamin_05_ad_funnel`, `DB_Beamin_combined`, `DB_Beamin_03_shop_change`, 배민 관련 테스트.
- 변경: ad_funnel metric 추출을 라벨 기반 dict 파서로 바꾸고 DOM 진단 로그·3회 연속 실패 서킷브레이커를 추가했으며, 최종 retry에서 ads 재시도를 제외하고 target_date fallback을 logical date 기준 전일로 통일.
- 변경: 신규 shop_change 수집 stage와 staging 패치를 제거하고 운영시간 수집 함수만 combined에서 호출하도록 전환했으며 기존 shop_change 히스토리 helper와 monthly_operation 입력 경로는 보존.
- 검증 결과: ad_funnel/운영시간/DAG import 정적 확인, `python -m pytest tests/ -k baemin -q` 110건 통과, OneDrive ad_funnel 읽기 전용 스캔에서 0/0 후보 56행(2026-06 51, 2026-07 5) 확인.
- 남은 위험: 실제 배민 DOM selector는 수동 Chrome 실행 로그로 최종 확인이 필요하고, 현재 컨테이너 이미지에는 Airflow CLI/Python 패키지가 없어 `airflow dags list-import-errors`는 수행하지 못함.
## 2026-07-24 배민·쿠팡 수동 재수집 구간 교체
- 대상: 배민·쿠팡 주문 원천 적재와 UnifiedSales 과거 재수집 소급 반영.
- 변경: 주문일자 구간 교체, 배달완료 0건 구간 비움, LOCAL_DB 재수집 마커와 enforce 후 정리, 완전 누락 시 POS 유지 회귀 검증을 추가.
- 검증 결과: 신규·직접 영향 테스트 37개와 모듈 import, scheduler 컨테이너의 UnifiedSales·CollectionCompare DAG import, UTF-8·diff 점검 통과.
- 남은 위험: 기존 쿠팡 크롤러 헬퍼 부재와 월 검증 홀 채널 기대값 불일치로 전체 묶음 11건은 기준선 실패하며, 기존 유령 행은 승인된 운영 재수집 전까지 남음.
## 2026-07-24 기흥테라타워점 배민 운영 재적재
- 대상: OneDrive 배민수동 원천, UnifiedSales 2026-06-17 및 2026-07-01~23, 7월 월별 검증 CSV.
- 변경: 승인된 배민 보관본으로 해당 구간을 교체하고 UnifiedSales 재조정 및 월별 검증 CSV 갱신을 수행.
- 검증 결과: 6월 유령 주문 제거 후 131,300원, 7월 배민 4,498,200원으로 일치하고 배민 채널 차이 0원·재수집 마커 0건·임시 백업 정리를 확인.
- 남은 위험: 월 전체 차이 -881,093원은 쿠팡 -944,993원과 땡겨요 +63,900원이며, 쿠팡 전체 구간 원본 부재로 이번 작업에서는 수정하지 않음.
## 2026-07-24 배민 단일 DAG 3배치·최종 통합 retry 전환
- 대상: `DB_Beamin_Macro_Dags`, 배치 체크포인트, NOW 실패 전달과 최종 retry 집계.
- 변경: B1 안에서 11계정씩 3개 태스크를 순차 실행하고 staging은 누적·체크포인트는 분리했으며, 세 배치의 계정·매장·주문·광고 실패를 중복 제거해 마지막에 한 번 재시도하도록 변경.
- 검증 결과: 직접 영향 테스트 45개와 배민 회귀 148개 통과, scheduler의 새 B1 태스크 트리·무스케줄 legacy B2/B3 호환 구조·DAG import 오류 0건 확인.
- 남은 위험: 이번 실행은 기존 B1→B2→B3 연쇄로 마무리되며, 새 단일 DAG 구조의 실제 Chrome 운영 검증은 다음 B1 실행에서 확인해야 함.
## 2026-07-24 미수백 Flow 리포트 하위업무 본문 전환
- 대상: 미수백 분석 스크립트, Strategy Flow 업로드 DAG, 전용 회귀 테스트.
- 변경: 댓글·로컬 상태파일 방식을 제목 기반 일·주·월 하위업무 upsert로 교체하고 주·월 수기 메모 보존, 월내 W# 공통 산출, CKEditor 본문 편집을 추가.
- 검증 결과: 관련 테스트 23건, 실데이터 격리 dry-run, Python 구문·UTF-8, scheduler 컨테이너 DAG import와 기존 태스크 체인 확인 통과.
- 남은 위험: 실제 Flow DOM·수기 메모 보존은 로그인된 Chrome 수동 검증이 필요하며 OneDrive와 Flow 운영 데이터는 수정하지 않음.
## 2026-07-24 미수백 Flow Chrome 회복 및 본문 저장 재검증
- 대상: `flow_susam_host_chrome_window.ps1`, `susam_report.py`의 Flow 하위업무 생성/수정 흐름, 회복용 DAG 재실행.
- 변경: 하위업무 입력칸 대기와 설정 메뉴 대기를 검증된 Flow 패턴에 맞춰 완화하고, Chrome 전용 프로필 프로세스 정리와 재시작 회복 로직을 유지하도록 조정.
- 검증 결과: 로컬 `tests/test_susam_report.py` 16건 통과, scheduler 컨테이너 DAG import 통과, `manual__susam_flow_recovery_fix2_20260724T090836` 실행에서 일·주·월 하위업무 본문 갱신 성공.
- 남은 위험: 로컬 `tests/test_susam_report_dag.py`는 Windows Airflow 로깅 폴더 권한 때문에 수동 실행이 막혀 환경 보정이 필요하며, 다음 예약 실행에서도 Chrome 세션 안정성을 다시 확인해야 함.
## 2026-07-24 미수백 Flow 본문 전체선택 교체
- 대상: `scripts/analysis/susam_report.py`, `tests/test_susam_report.py`.
- 변경: 날짜별 하위업무 추가와 주·월 제목 기반 upsert는 유지하고, CKEditor·iframe 모두 기존 본문 전체선택 후 완성본 전체 교체 방식으로 변경.
- 검증 결과: 사용자 예시 집계 회귀를 포함한 미수백 리포트 단위 테스트 17건, Python 컴파일, UTF-8 및 diff 형식 검사 통과.
- 남은 위험: 실제 Flow CKEditor의 전체선택·HTML 삽입은 다음 로그인 Chrome 실행에서 운영 확인이 필요함.
## 2026-07-27 배민 upload inbox 미적재 방지
- 대상: `_baemin_upload_inbox` 적재 전 sweep과 하위 PC upload 대기 감지.
- 변경: 오래된 `_tmp__manual__*` 잔해를 `_quarantine`으로 회수하고, 완성된 `manual__bottom__*` 폴더가 12시간 이상 대기하면 Telegram 경고하도록 추가.
- 검증 결과: 신규 stale tmp 테스트 5건, 기존 upload inbox/PC2 trigger 테스트 40건, 모듈 import와 DAG import 확인 통과. 승인 후 운영 확인에서 upload inbox는 `_quarantine`만 남았고 2026-07-26 analytics 주문 4,914건을 확인.
- 남은 위험: 최신 PC2-triggered upload run은 ingest·주문/ad_funnel 검증 성공 후 ToOrder 재검증 Selenium 단계가 계속 실행 중이며, 하늘도시점 등 일부 매장의 shell 로드 실패 재시도가 길어지고 있음.
## 2026-07-27 배민 매크로 수집 원천 00:15 시작
- 대상: `SMD_BAEMIN_COLLECT_BATCH1_TIME`, `DB_Beamin_Macro_Dags` 메인 자동 수집 스케줄.
- 변경: 배민 메인 수집 시작 시각을 KST 03:15에서 KST 00:15로 앞당김. Retry DAG는 트리거 전용 `schedule=None` 유지.
- 검증 결과: 로컬·scheduler 컨테이너 Python compile 및 로컬 상수 import 확인 통과.
- 남은 위험: scheduler 컨테이너에 Airflow CLI와 pendulum import 경로가 없어 DAG import 직접 확인은 실패했으며, 운영 scheduler 반영은 reload 후 확인 필요.

## 2026-07-27 unified_sales OneDrive 충돌본 재발 방지
- 대상: UnifiedSales 공통 파일 iterator, mart parquet 저장, DB_UnifiedSales DAG, PC2 운영 문서.
- 변경: 정규 일별 parquet 화이트리스트, tmp 후 원자 교체 저장, 충돌본 `_conflicts/YYYYMMDD` 격리와 Telegram 알림, DAG 집계 전 격리 태스크를 추가.
- 검증 결과: Python compile, UTF-8, 화이트리스트 기대값, `.tmp` 격리·원자 저장, 소비자 import, 잔여 자체 glob/direct write 스캔 통과.
- 남은 위험: Windows Airflow import는 기존 `airflow.cfg` 인코딩 문제로 실패했으며, 실제 OneDrive mart 파일 생성·이동 검증은 운영 데이터 수정 승인을 받은 뒤 수행 필요.
## 2026-07-27 네이버 hall marketing sync 구현
- 대상: `DB_Hall_Sales_Target_Dags`, hall 마케팅 CSV sync 파이프라인, 네이버 마케팅 수집 경로 상수.
- 변경: `naver_corporate_store_marketing.csv`의 `place_inflow`·`reservations`를 `hall_marketing_target.csv`의 `플레이스_유입`·`네이버_오더`에 반영하는 sync 태스크를 추가.
- 검증 결과: Python compile, 경로 상수·sync 모듈 import, `.tmp` 임시 CSV sync 재현, 격리 `AIRFLOW_HOME` DAG import와 태스크 목록 확인 통과.
- 남은 위험: 실제 OneDrive `hall_marketing_target.csv` 갱신 검증은 승인 후 수행 필요.
## 2026-07-27 배민 매크로 수집 2계정 병렬화
- 대상: `DB_Beamin_Macro_Dags`, `DB_Beamin_Macro_Dags_Retry`, Retry conf/payload 헬퍼와 배치 분할 테스트.
- 변경: 메인 수집을 4배치 2레인 Airflow 태스크 구조로 전환하고 staging 초기화를 별도 태스크로 분리했으며, Retry DAG도 2레인 분할 후 병합하도록 변경.
- 검증 결과: 로컬 배민 병렬화 관련 테스트 33건, 컨테이너 py_compile, DAG import 오류 0건, 메인/Retry/upload/PC2 태스크 트리와 분할·병합 헬퍼 직접 확인 통과. 컨테이너 pytest는 worker 이미지에 pytest가 없어 실행하지 못함.
- 남은 위험: 실제 Chrome 2개 동시 구동, 레인 B 15~20초 오프셋, upload inbox 연계는 운영 수동 트리거 초반 로그 확인이 필요함.
## 2026-07-27 배민 Upload ToOrder 재수집 실패 보존
- 대상: `DB_Beamin_Macro_validate`, `DB_Beamin_04_orders`, 배민 검증/최종알림 회귀 테스트.
- 변경: ToOrder 불일치 재수집 전 기존 orders target_date 행을 snapshot으로 보존하고, shell 로드 실패 등 재수집 실패 매장은 기존 행을 복원하며 shell 실패 DOM 진단 로그를 추가.
- 검증 결과: 관련 로컬 테스트 55건과 Python 구문 검사 통과, 컨테이너 py_compile·DAG import 오류 0건·upload task tree 확인 통과.
- 남은 위험: 실행 중인 `pc2_bottom__20260727T013000`의 이미 삭제된 재수집 구간은 별도 운영 판단이 필요하며, 실제 shell 실패 원인은 신규 로그로 다음 실행에서 확정해야 함.
## 2026-07-28 배민 upload 적재/검증 DAG 분리
- 대상: `DB_Beamin_Macro_Upload_Dags`, 신규 `DB_Beamin_Macro_Upload_Validate_Dags`, Pc2 upload sweep, upload handoff/알림 테스트.
- 변경: upload DAG를 적재 전용으로 축소하고 검증 DAG를 handoff 파일 기반으로 분리했으며, Pc2 스윕 종일화·대기 제거·stale/tmp 알림 보강과 처리 완료 handoff 자동 삭제를 적용.
- 검증 결과: 관련 pytest 84건, handoff 왕복, DAG import/태스크 그래프, 스케줄 상수, 컨테이너 DAG import-error 조회, UTF-8 읽기 검증 통과. 승인 후 운영 empty E2E는 9초 success, 실제 Pc2 handoff 검증 DAG 6개 태스크 success 확인, 완료 handoff 잔여 2건 삭제 후 count 0 확인.
- 남은 위험: 승인 시점에 `manual__bottom__*` 완성 폴더가 0개라 추가 실데이터 폴더 소비 재현은 수행 대상 없음.
## 2026-07-27 delivery_commission 배민 즉시할인·우가클 지표 추가
- 대상: `DB_DeliveryCommission`, 배달수수료 마트 작업 지시서, delivery commission 회귀 테스트.
- 변경: 배민 주문 `즉시할인_파트너부담` 합계와 우가클 평균비용·주문수·클릭율을 출력 스키마에 추가하고 쿠팡 행 NULL 정책을 반영.
- 검증 결과: `tests/test_delivery_commission.py` 11건, Python 구문 검사, 컨테이너 DAG import, 실데이터 배민 주문/광고 원천 집계 읽기 검증 통과.
- 남은 위험: `.tmp` 출력 실데이터 build는 쿠팡 OneDrive 원천 45개 파일 `PermissionError`로 중단됐으며, 실제 OneDrive `delivery_commission.parquet` 원자 교체는 수정 승인 후 수행 필요.
## 2026-07-27 delivery_commission 쿠팡 CMG 컬럼 추가
- 대상: `DB_DeliveryCommission`, 쿠팡 CMG 원천 집계, delivery commission 회귀 테스트.
- 변경: 쿠팡이츠 행에 신규/재주문 비율, 광고비용, 신규고객, 광고노출수, 광고클릭수를 추가하고 파티션 brand/store 기준 매칭으로 내부 매장명 차이를 회피.
- 검증 결과: `tests/test_delivery_commission.py` 16건, Python 구문 검사, 컨테이너 DAG import, 컨테이너 cmg 164개 파일 읽기, `.tmp` 실데이터 build 13,172행 17컬럼 검증 통과.
- 남은 위험: 실제 OneDrive `delivery_commission.parquet` 원자 교체 실행은 OneDrive 수정 승인을 받은 뒤 수행 필요.
## 2026-07-27 delivery_commission 쿠팡 CMG 매장 alias 충돌 보강
- 대상: `DB_DeliveryCommission` 쿠팡 CMG 파티션 매칭과 delivery commission 회귀 테스트.
- 변경: 동일 brand/store/date로 정규화되는 alias 파티션이 동시에 있을 때 canonical 파티션을 우선하고 exact duplicate CMG row를 제거해 이중집계를 방지.
- 검증 결과: `tests/test_delivery_commission.py` 17건, Python 구문 검사, 컨테이너 CMG 충돌 샘플 4건, `.tmp` 실데이터 build 13,172행 17컬럼 및 키 중복 없음 확인 통과.
- 남은 위험: 주문은 있으나 CMG 원천 날짜가 없는 행은 NULL 유지하며, 실제 OneDrive mart 교체는 승인 후 수행 필요.
## 2026-07-27 fin_product 닭 사용량 수기 컬럼 추가
- 대상: `DB_FinProduct_Map`, DB pipeline AGENTS, 닭 사용량 수기 컬럼 회귀 테스트.
- 변경: 리뷰 CSV 끝에 닭유형/사이즈/닭사용량 수기 컬럼을 추가하고, 재생성 시 기존 수기값 보존 merge·검증 warning·수기값 백업 스냅샷을 추가.
- 검증 결과: 신규/관련 pytest 30건, Python 구문 검사, dry-run 2종, 스키마/LLM 격리 확인 통과. 승인 후 실제 `migrate_product_map(dry_run=False)`로 OneDrive 리뷰 CSV 803행 재생성 및 끝 3컬럼 확인.
- 남은 위험: Windows Airflow import는 기존 `airflow.cfg` 인코딩 문제, 컨테이너 직접 import는 `pendulum` 모듈 경로 문제로 완료하지 못함.
## 2026-07-27 쿠팡이츠 CMG 수동수집 날짜범위 복원
- 대상: `coupang_extension_build/content/03_coupangeats.js`, `05_main.js`, OneDrive 개발용 확장 사본과 문서.
- 변경: CMG 수동수집 prompt 헬퍼를 복원하고 수동 실행의 `targetDateMode: 'yesterday'` 기본 주입을 제거했으며, 취소/입력오류가 runner F5 루프를 타지 않도록 분기 정리.
- 검증 결과: 기준본과 OneDrive 사본 `node --check` 통과, `_readCMGDateRange` 호출부 제거·prompt 중복 1건·배치 전용 targetDateMode 분기·문서 예외 설명 반영 확인 통과.
- 남은 위험: 브라우저 수동 검증은 로그인 세션에서 별도 확인해야 함.
## 2026-07-29 Airflow scheduler heartbeat 복구
- 대상: `airflow-scheduler`, scheduler heartbeat, 누락 운영 DAG run, `scripts/airflow_scheduler_watchdog.py`.
- 변경: scheduler hung 상태를 `docker compose restart airflow-scheduler`로 복구하고 heartbeat stale 감지 시 scheduler만 재시작하는 워치독 스크립트를 추가.
- 검증 결과: scheduler `healthy`, `airflow jobs check` 성공, DAG import error 0건 확인. 누락 운영 DAG run을 logical date 기준으로 순차 실행해 성공 처리.
- 남은 위험: Windows/Docker bind mount I/O 오류 자체는 재발 가능하지만, `DoridangAirflowSchedulerWatchdog` 작업 스케줄러 5분 주기 등록과 실행 결과 0을 확인함.
## 2026-07-27 배민 Retry 상위 실패분 제한
- 대상: `DB_Beamin_Macro_Dags`, `DB_Beamin_Macro_Dags_Retry`, `DB_Beamin_retry`, 배민 retry/batch split 테스트.
- 변경: 메인 DAG의 재시도 진입 전 `load_accounts` 결과 계정만 실패 payload에 남기고, 별도 Retry DAG conf에도 `allowed_account_ids`와 `collect_range`를 보존해 하위/전체 매장으로 확장되지 않게 제한.
- 검증 결과: `tests/test_beamin_retry_conf.py`와 신규 Retry 필터 테스트, 격리 `AIRFLOW_HOME` DAG import 검증 통과.
- 남은 위험: 전체 `test_baemin_batch_split.py` 중 스케줄 기대값은 현재 작업 트리의 `15 0 * * *` 설정과 기존 테스트의 `15 3 * * *` 기대가 달라 별도 정리가 필요함.
## 2026-07-27 크롬 수집 동시 제한 4개 확장
- 대상: Airflow `selenium_pool`, `DB_Beamin_Macro_Dags` legacy batch chain, 배민 harness와 batch split 테스트.
- 변경: 운영 `selenium_pool` 슬롯을 3에서 4로 변경하고, legacy 호환 DAG 체인을 B2/B3/B4의 4분할 기준으로 확장.
- 검증 결과: `airflow pools list`에서 `selenium_pool` 4슬롯 확인, `tests/test_baemin_batch_split.py tests/test_beamin_retry_conf.py` 30건 통과.
- 남은 위험: 실제 Chrome 4개 동시 구동은 OS/메모리와 배민 세션 상태에 따라 다음 운영 실행 초반 로그 확인 필요.
## 2026-07-27 쿠팡이츠 수동 COLLECT source 명시
- 대상: OneDrive 개발용 확장 `background.js`와 기준본/OneDrive `popup.js`.
- 변경: 확장 아이콘·단축키·팝업 수집 버튼의 `COLLECT` 메시지에 `source: 'manual'`을 명시해 CMG 수동 prompt 경로가 배치 상태와 섞이지 않도록 보강.
- 검증 결과: OneDrive `background.js`/`popup.js`와 기준본 `popup.js` `node --check` 통과, bare `{ type: 'COLLECT' }` 0건과 manual source 2건씩 확인.
- 남은 위험: 크롬 확장 새로고침과 CMG 탭 새로고침 후 실제 prompt 표시를 브라우저에서 확인해야 함.
## 2026-07-28 쿠팡 수집기 에러 로그 보완
- 대상: OneDrive 개발용 확장 `content/03_coupangeats.js`, `runner.js`, `background.js`.
- 변경: 마지막 페이지 정상 종료 로그를 issue log 잡음에서 제외하고, 조회 버튼 반복 경고를 debug로 낮추며, CMG 0건 타임아웃·10057 대기·로그인 F5 재시도 백오프와 issue log overwrite 분기를 보강.
- 검증 결과: 수정 전 `.bak_20260728` 백업 3개 생성, 대상 3개 JS `node --check` 통과, 정적 검색으로 마지막 페이지/조회 버튼/누진 대기/issue log `.txt`/diagnostic overwrite 반영 확인.
- 남은 위험: CMG 실제 DOM 트리거는 Codex 환경에서 실측하지 못해 추측 셀렉터를 추가하지 않고 재조회 생략 경로로 처리했으며, 크롬 확장 새로고침 후 소규모 수동 배치 검증 필요.
## 2026-07-28 배민매크로 retry_failed 증폭 수정
- 대상: `DB_Beamin_combined`, `DB_Beamin_retry`, `beamin_staging`, `DB_Beamin_Macro_Dags`, retry conf 테스트.
- 변경: 무시 가능 NOW/우리가게/운영시간 실패를 계정 전체가 아닌 `stages` 버킷에 매장·스테이지 단위로 등록하고, `retry_failed`는 해당 스테이지/매장만 resume 가능한 progress로 재시도하며 Airflow task `retries=0`을 적용.
- 검증 결과: stages 카운트/병합/흡수/시그니처/progress 스키마 소형 테스트, `py_compile`, `tests/test_beamin_retry_conf.py`, `tests/test_baemin_collect_resume.py`, `tests/test_baemin_batch_split.py`, 컨테이너 DAG import 및 `list-import-errors` 통과.
- 남은 위험: 실제 운영 run에서 스테이지 한정 재시도 소요와 `_collect_progress_retry.json` 이어받기는 다음 배민 수집 실패 케이스에서 로그로 확인 필요.
## 2026-07-28 배민 Retry DAG failed_stages 인식 보강
- 대상: `DB_Beamin_Macro_Dags_Retry`, 최종 알림/Retry DAG 회귀 테스트.
- 변경: 별도 Retry DAG의 대상 판정, 잔여 실패 카운트, 다음 attempt conf/log에 `failed_stages`를 포함해 stages-only 실패가 `재시도 대상 없음`으로 빠지지 않게 수정.
- 검증 결과: `tests/test_baemin_final_notification.py`, `tests/test_beamin_retry_conf.py`, Python 구문 검사, 컨테이너 DAG import-error 조회 통과.
- 남은 위험: 수동 Retry DAG 실행은 conf가 비어 있으면 여전히 대상 없음이 정상이며, 현재 upload validate run은 `validate_toorder`/`validate_ad_funnel` Selenium 재수집 진행 중.
## 2026-07-28 배민 Retry 대량 실패 차단
- 대상: `DB_Beamin_Macro_Dags`, 배민 batch split/retry 회귀 테스트.
- 변경: Retry DAG 자동 트리거 전 실패 신호가 있는 고유 계정 비율을 계산하고 기본 5% 초과 시 `allow_large_retry=true` 없이는 AirflowException으로 차단하도록 보강.
- 검증 결과: `tests/test_baemin_batch_split.py`, `tests/test_beamin_retry_conf.py`, `tests/test_baemin_final_notification.py`, Python 구문 검사, 컨테이너 DAG import 및 `list-import-errors` 통과.
- 남은 위험: 현재 실행 중인 대량 Retry run은 기존 conf로 시작된 상태라 새 가드가 소급 중지하지 않으며, 필요 시 운영 UI에서 중지해야 함.
## 2026-07-28 배민 Retry 대량 실패 정책 정정
- 대상: `DB_Beamin_Macro_Dags`, `DB_Beamin_Macro_upload`, 배민 upload handoff/retry/summary 테스트.
- 변경: 5% 초과 자동 차단을 제거하고 대량 실패율은 경고 로그로만 남기며, upload handoff에 `residual_failed`를 명시하고 ToOrder 재수집 실패 매장만 `failed_orders`로 제한해 Retry DAG가 원본 실패 payload 전체로 재생성되지 않게 수정.
- 검증 결과: 배민 batch split, PC2 upload trigger, collect resume, driver recovery, shop schedule, final notification, retry conf 테스트 71건 통과 및 컨테이너 DAG import-error 0건 확인.
- 남은 위험: 이미 시작된 기존 대량 Retry run은 새 코드가 소급 중지하지 않으며, 다음 upload validate 실행에서 잔여 실패 기준 스킵 여부를 운영 로그로 확인 필요.
## 2026-07-29 Airflow scheduler watchdog 숨김 실행
- 대상: `scripts/airflow_scheduler_watchdog.py`, `scripts/register_airflow_scheduler_watchdog_task.ps1`, Windows 작업 스케줄러 `DoridangAirflowSchedulerWatchdog`.
- 변경: 예약 실행 시 PowerShell/cmd 창이 뜨지 않도록 `wscript` 숨김 런처를 추가하고, watchdog 로그는 기본적으로 파일에만 남기며 `jobs check` 단발 실패만으로는 재시작하지 않도록 판정을 보수화.
- 검증 결과: Python 구문 검사와 UTF-8 읽기 확인 통과, 작업 스케줄러 수동 실행 `Last Result: 0`, scheduler health 정상 판정 확인.
- 남은 위험: Windows 작업 스케줄러가 사용자 로그온 세션 기준으로 실행되므로 로그오프 상태 동작은 별도 운영 정책 확인 필요.
## 2026-07-29 Airflow/Docker 디스크 포화 복구
- 대상: `docker-compose.yaml`, Airflow 호스트 로그, Docker Desktop WSL VHDX, 공간 정리 스크립트와 Windows 작업 스케줄러.
- 변경: 컨테이너 json 로그 로테이션을 추가하고, scheduler/task 로그와 Temp 진단 산출물 및 Docker prune을 수행하는 정기 정리 스크립트, VHDX 압축 스크립트, 기준 미만 여유 공간 Telegram 경고를 추가.
- 검증 결과: Airflow scheduler 과거 로그와 Temp 진단 산출물 삭제로 C: 여유 공간을 약 50GB까지 복구했고, 컨테이너 재생성 후 로그 로테이션 적용 및 Airflow 주요 컨테이너 `healthy`, `DoridangAirflowSpaceCleanup` 매일 03:20 등록 확인.
- 남은 위험: Docker 내부 실제 사용량은 약 12GB이나 `docker_data.vhdx` 논리 크기 107GB는 `fstrim`/compact 후에도 줄지 않아, 장기적으로 Docker Desktop 디스크 이미지 위치를 C: 밖으로 이동해야 함.
## 2026-07-29 배민 주문서 전량 미수집 방지
- 대상: `DB_Beamin_Macro_Dags`, `DB_Beamin_04_orders`, `DB_Beamin_combined`, 주문 날짜필터 디버그 스크립트와 회귀 테스트.
- 변경: 기본 `target_date`를 `data_interval_end` 기준 전일로 보정하고, specific 날짜필터의 내부 DatePicker 트리거 클릭과 5매장 연속 `date_filter` 실패 fail-fast를 추가했으며, 날짜 범위 혼입 시 `target_date` 행만 저장하도록 가드를 보강.
- 검증 결과: Python 구문/UTF-8 검증, 배민 batch/notify/collect resume/driver recovery/date_filter 테스트, DAG import, 컨테이너 `airflow dags list-import-errors`, 랜덤 4개 매장 orders-only smoke 통과. 주문 있는 강동점 `2026-07-27` staging 실저장에서 7일 범위 126행 중 target_date 8행만 `Local_DB/temp/.../orders_2026-07.parquet`에 저장됨을 확인.
- 남은 위험: 실제 운영 OneDrive/analytics 백필은 아직 실행하지 않았으며, 전체 `2026-07-26`·`2026-07-27` 백필은 수집 산출물 수정 승인이 필요함.
## 2026-07-29 쿠팡수동 취소금액/중복 집계 교정
- 대상: `DB_UnifiedSales_coupang`, `DB_DeliveryCommission`, 쿠팡수동 금액 회귀 테스트와 OneDrive `delivery_commission`/`unified_sales_grp`.
- 변경: 쿠팡 취소금액 음수 차감 제거 정책에 더해 구/신 매장 파티션 및 타입 표현 차이(`10000`/`10000.0`, `nan`/`None`)가 있는 동일 주문 dedup 키를 보강하고, 부산서면점 2026-06-11/15 중복 unified 백필을 교정.
- 검증 결과: 관련 pytest 42건, import 검증, 음수 store-day 0건, 핵심 4개 회귀값 유지, 테스트 매장 25개 `delivery_commission` vs `unified` 쿠팡 합계 `800,858,200`원 / diff 0 / nonzero 0 확인.
- 남은 위험: `tests/test_unified_sales_manual_delivery_cleanup.py` 전체 3개 기존 실패는 `DB_UnifiedSales_validate` channel/집계 형태 이슈로 이번 쿠팡 금액 교정 범위 밖이며 별도 처리 필요.
## 2026-07-29 쿠팡 원천 중복 행 삭제
- 대상: OneDrive `analytics/coupang_macro/orders` canonical `orders_YYYY-MM.parquet`.
- 변경: 구로디지털단지점 2026-06 중복 원천 파일 268행을 삭제 후 빈 파일 제거, 부산서면점 2026-06 파일 내 타입 표현 중복 59행을 삭제해 원천 중복 총 327행을 제거.
- 검증 결과: canonical 원천 292개 / 총 374,682행 / 중복 후보 0건 / 빈 파일 0건, `delivery_commission` 재생성 후 테스트 매장 쿠팡 합계 `800,858,200`원 / diff 0 / nonzero 0 확인.
- 남은 위험: 원천 파일을 직접 정리했으므로 향후 쿠팡 확장 수집기가 구 매장 파티션을 다시 만들지 않는지 다음 2026-06 재수집 또는 매장명 변경 케이스에서 확인 필요.
## 2026-07-29 쿠팡 원천 중복 재발 방지
- 대상: `DB_CoupangMacro_load` 쿠팡 orders 수동/수집 CSV 적재 및 원천 repair 함수.
- 변경: 적재 저장 직전 dedup 키가 숫자·결측 텍스트 표현 차이를 정규화하도록 보강하고, repair는 canonical `orders_YYYY-MM.parquet`만 대상으로 `brand+정규화매장+주문상세` 기준 전체 파티션 중복을 정리하며 빈 중복 파일을 삭제하도록 변경.
- 검증 결과: 쿠팡 적재/repair 및 delivery/unified 관련 테스트 29건 통과, `repair_coupang_orders_duplicates()` 실제 실행 결과 파일 0/292, 스킵 0, 행 374,682->374,682, 제거 0 확인.
- 남은 위험: 기존 `tests/test_coupang_orders_validation.py` 앞쪽 8개 실패는 크롤러 API 변경으로 이번 원천 중복 방지 범위 밖이며 별도 정리 필요.
## 2026-07-30 Airflow 스케줄 생성 지연 감시
- 대상: `Strategy_ScheduleGuard_01_Overdue_Dags`, `dag_schedule_guard`, 스케줄 상수.
- 변경: unpaused DAG의 `next_dagrun_create_after`가 10분 이상 지연되면 실패 상태와 Telegram 알림으로 노출하는 10분 주기 가드를 추가.
- 검증 결과: 오늘 08:43 KST 기준 생성 예정시간 경과 미생성 0건, 오늘 시작 실패 0건, 새 가드 import/py_compile/list-import-errors/수동 test 및 첫 scheduled run 재실행 성공.
- 남은 위험: 범용 자동 재실행은 중복 적재 위험 때문에 넣지 않았고, OneDrive 저장형 전체 모니터링 DAG(`Strategy_DagMonitoring_01_Alert_Dags`)는 승인 전까지 paused 유지.
## 2026-07-30 Airflow 실행 시 C: 드라이브 고갈 및 Docker 정지 해소
- 대상: `~/.wslconfig`, Docker WSL 데이터 디스크(`docker_data.vhdx`), 신규 `DB_Storage_Cleanup_Dags`/`DB_StorageCleanup`, 스케줄 상수.
- 원인: 컨테이너 Chrome 크래시 시 WSL2가 프로세스 주소공간 전체를 `%LOCALAPPDATA%\Temp\wsl-crashes`에 덤프(1건 47GB, 기본 최대 10건)해 매 실행마다 C:를 고갈시켰고, 별도로 vhdx가 non-sparse라 TRIM이 호스트에 전달되지 않아 실사용 8.3GB에 파일만 107.7GB로 팽창.
- 변경: `crashDumpEnabled=false`로 덤프 원천 차단, 덤프 46.1GB 삭제, Docker 정상 종료 후 vhdx에 sparse 플래그 적용(107.7GB→17.1GB, fstrim 전달량 440MiB→993GiB), 로그/tmp/다운로드/미사용 Chrome 프로필을 회수하는 04:20 정리 DAG 신설, `git gc`로 loose object 8,180개 팩.
- 검증 결과: C: 여유 0.3GB→151.4GB, E: 임시파일 38.5GB→8.5GB, 정리 DAG dry-run/실행 72,639건·31.09GB·오류 0, scheduled run 150초 성공, 컨테이너 8개 healthy, 이미지 5개·볼륨 보존, DAG 80개/import 오류 0/dag_run 11,270건 유지.
- 남은 위험: Chrome이 죽는 원인 자체(배민 매크로 렌더러 타임아웃/OOM)는 미해결이라 덤프만 차단된 상태이며, Docker 디스크의 E: 이전은 GUI 설정이라 미실행. `Strategy_Policy_02_Consolidate_Dags`의 2026-06-27 시작 좀비 태스크가 running으로 남아 있음. 메타DB 백업은 `C:\Local_DB\temp\backups\docker_disk_compact\20260730\`에 보관 중이며 E: 이전 여부 확정 후 삭제.
## 2026-07-30 unified_sales 월별 검증 ToOrder 최신일 컷오프
- 대상: `DB_UnifiedSales_validate.py`, 월별/일별 검증 알림 회귀 테스트.
- 변경: ToOrder 기준 parquet 최신일을 읽어 현재월 월별 비교 컷오프 상한으로 적용하고, 기준값이 없는 일별 검증은 텔레그램 보류 알림 후 CSV 저장 없이 반환하도록 보강.
- 검증 결과: import/py_compile 통과, ToOrder 최신일 `2026-07-28` 확인, `tests/test_unified_sales_validate_alert.py` 4건 통과, 2026-07-29 기준값 empty 확인.
- 남은 위험: OneDrive 쓰기 승인 전이라 `validate_monthly_sales()` 실저장 검증은 미실행. 관련 전체 pytest는 기존 POS홀 baseline 기대값 불일치 3건이 남아 있음.

## 2026-07-30 쿠팡이츠 CMG prompt 블록 제거 및 2일치 재수집
- 대상: OneDrive 확장 개발용 `content/03_coupangeats.js`, `runner.js`.
- 변경: CMG 배치 판정을 주문서 배치 판정과 분리하고 -2~-1 2일 범위 재수집을 적용, 날짜 UI 반영 검증을 추가했으며 빈 targetStores CMG는 runner에서 skip 처리.
- 검증 결과: `node --check content/03_coupangeats.js`, `node --check runner.js`, 정적 호출부 점검, 월 경계 날짜 계산 확인 통과.
- 남은 위험: 브라우저 확장 리로드 후 runner 1계정 실전 수집 및 수동수집 회귀 확인은 미실행.
## 2026-07-30 ToOrder 월별 재다운로드 실패 재시도 보강
- 대상: `DB_Toorder_store_platform_daily_Dags`, ToOrder datedetail 다운로드 파이프라인과 회귀 테스트.
- 변경: DAG 기본 실행은 pending xlsx 재사용 없이 6월/7월 범위를 항상 새로 다운로드하도록 `force_download=True`를 전달하고, `downloaded file not found`를 재시도 가능 오류로 분류.
- 검증 결과: py_compile 통과, `tests/test_toorder_store_platform_daily.py` 6건 통과, 컨테이너 DAG import 오류 0건. 현재 parquet는 2026-06 30일/2026-07 29일, 최신일 2026-07-29 확인.
- 남은 위험: 운영 재다운로드/월별 검증 재실행은 OneDrive parquet·CSV를 다시 쓰므로 별도 승인 후 실행 필요. 기존 수동 ToOrder run은 failed 상태이나 데이터 upsert는 완료됨.

## 2026-07-30 쿠팡이츠 CMG 수정 빌드본 재반영
- 대상: `coupang_extension_build/content/03_coupangeats.js`, `coupang_extension_build/runner.js`, OneDrive 개발용 확장 동일 파일.
- 변경: 호스트 Chrome 스크립트가 참조하는 빌드본에도 CMG batch prompt 차단, -2~-1 재수집, 날짜 반영 검증, 빈 targetStores skip을 동일 반영.
- 검증 결과: 빌드본/개발본 `node --check` 통과, `_readCMGDateRange` 호출 2건 및 CMG `_isBatchCMG` 분기 확인, 월 경계 날짜 계산 확인 통과.
- 남은 위험: 이미 떠 있는 Chrome/runner/CMG 탭에는 이전 content script가 남을 수 있어 확장 리로드와 작업 탭 새로고침 필요.

## 2026-07-30 쿠팡이츠 CMG 단일일자 종료일 공백 보정
- 대상: `coupang_extension_build/content/03_coupangeats.js`, OneDrive 개발용 `content/03_coupangeats.js`.
- 변경: CMG 달력에서 단일일자 선택 후 `.end-date`가 빈 문자열로 남는 DOM을 허용해, 시작일이 읽히면 종료일을 시작일로 보정하도록 `_readCMGDateRange`를 수정.
- 검증 결과: 빌드본/개발본 `node --check` 통과, `parsedEndDate` fallback 반영 및 UTF-8 깨짐 없음 확인.
- 남은 위험: 이미 떠 있는 CMG 탭은 확장 리로드와 탭 새로고침 후 재실행 필요.

## 2026-07-30 쿠팡이츠 CMG 단일일자 fallback 식별 로그
- 대상: `coupang_extension_build/content/03_coupangeats.js`, OneDrive 개발용 `content/03_coupangeats.js`.
- 변경: `.end-date` 공백 fallback이 실제 실행 중인지 확인할 수 있도록 `_readCMGDateRange`에 `CMG 단일일자 표시 감지` debug 로그를 추가.
- 검증 결과: 빌드본/개발본 `node --check` 통과, fallback 및 식별 로그 각 1건 확인, UTF-8 깨짐 없음 확인.
- 남은 위험: 새 로그 확인 전에는 기존 탭의 오래된 content script 실행 가능성이 남아 있으므로 확장 리로드와 작업 탭 새로고침 필요.

## 2026-07-30 쿠팡 자동화 issue log 기반 보강
- 대상: `E:\d_down\coupang_issue_log_2026072*`, 빌드본/OneDrive 개발용 `runner.js`.
- 변경: issue log에서 10057/ACCESS_DENIED, WUJIE 미준비, 메뉴/CMG 이동 실패가 반복됨을 확인하고 CMG revamp URL 재진입·WUJIE 진단 로그·CMG/메뉴 URL 재진입·빈 targetStores 메뉴 skip·10057 반환 플래그를 보강.
- 검증 결과: 빌드본/개발본 `node --check runner.js` 통과, CMG revamp URL/재진입 로그/WUJIE 진단/메뉴 재진입/hardThrottle 반환 토큰 확인, UTF-8 깨짐 없음 확인.
- 남은 위험: 새 runner 적용을 위해 확장 리로드와 기존 runner/작업 탭 종료 후 재실행 필요.
## 2026-07-30 DB_ToOrder Daily Store 로그인 계정 선택 복구
- 대상: `account.py`, `crawling_toorder_sales_report.py`, 계정/로그인 회귀 테스트.
- 변경: `get_default_account("toorder")`가 `비고` 누락 CSV에서 매장계정으로 열화되지 않도록 `require_note` 엄격 모드를 추가하고, ToOrder 로그인에 isCompany 교대 재시도를 추가.
- 검증 결과: 대표 계정 pw_len 9, 배민 계정 fallback 67건 유지, 계정/로그인/ToOrder/DAG guard 테스트 26건 통과.
- 남은 위험: 실제 `DB_ToOrder_Daily_Store_Dags` 운영 트리거는 수집 산출물을 쓸 수 있어 미실행했으며, 다음 승인된 run에서 로그인 성공 로그 확인 필요.

## 2026-07-30 delivery_commission 배민 settlement null 처리
- 대상: `DB_DeliveryCommission.py`, `tests/test_delivery_commission.py`.
- 변경: 배민 정산 미수집 주문은 0원으로 보고 수집 주문만 부분 합산하도록 바꾸고, 배민 우가클 3종 null을 0으로 채움.
- 검증 결과: delivery_commission 테스트 22건 통과, DAG import 성공, 운영 재빌드 13,975행, 배민 settlement/우가클 null 0건, 쿠팡 우가클 NA 유지, 기존 non-null 배민 settlement 변경 0건.
- 남은 위험: 정산 미수집 123개 날짜×매장×브랜드는 부분 합산값이므로 최근 미확정일 diff가 커질 수 있음. 비교용 임시 파일 `.tmp/delivery_commission_before_260730.parquet`는 삭제 명령 차단으로 남아 있으며 재확인 불필요 시 삭제 가능.

## 2026-07-30 delivery_commission 배민 즉시할인 fallback
- 대상: `DB_DeliveryCommission.py`, `tests/test_delivery_commission.py`.
- 변경: `즉시할인_파트너부담`이 0/미수집이고 원천 `즉시할인` 총액이 있으면 `배민_즉시할인`에 fallback 적용, 한 플랫폼 매출이 없는 날짜는 반대 플랫폼 0행을 생성.
- 검증 결과: 2026-07-29 도리당 송파삼전점 함수 집계 `배민_즉시할인` 26,600 확인, delivery_commission 테스트 24건 통과, DAG import 성공.
- 남은 위험: OneDrive 운영 parquet 재빌드는 승인 대기 상태라 아직 미반영.

## 2026-07-30 DB_UnifiedSales 미사점 임시 매장 제한 정정
- 대상: `DB_UnifiedSales_common.py`, `DB_UnifiedSales.py`, `DB_UnifiedSales_Dags.py`.
- 변경: `FULL_RECALC_STORES=["미사점"]`를 추가해 `sale_date` 정정 실행 시 전체 매장 대신 지정 매장만 build/reconcile/enforce하도록 연결.
- 검증 결과: py_compile/UTF-8 확인 통과, 컨테이너 DAG import 성공, `list-import-errors` 0건, fake context에서 `stores=['미사점']` XCom 확인.
- 남은 위험: 복구 완료 후 `FULL_RECALC_STORES=[]`로 비워야 하며, 실제 `2026-02-14` 운영 실행과 CollectionCompare 재확인은 별도 수행 필요.

## 2026-07-30 OKPOS 상품조회 헤더-only 엑셀 차단
- 대상: `DB_OKPOS_Product.py`, `tests/test_okpos_product_download_validation.py`.
- 변경: OKPOS 상품조회 다운로드/저장 검증에 `상품코드` 헤더와 실제 상품코드 행 존재 여부를 추가하고, 조회 Ajax 완료 및 IBSheet 상품코드 행 렌더링을 기다린 뒤 엑셀다운을 실행하도록 보강.
- 검증 결과: 관련 테스트 16건, py_compile, DAG import 통과. `DB_OKPOS_Product_Dags` 수동 실행 성공, 운영 `상품조회.xlsx` 511개 상품코드 행으로 갱신, FinProduct 로더 `OKPOS 537건 + EASYPOS 55건 = 592건` 확인.
- 남은 위험: OKPOS 화면 응답이 90초를 넘기면 수집 실패로 드러나며, 실패 시 기존 정상 엑셀을 덮어쓰지 않음.

## 2026-07-30 OKPOS 0원 홀 주문 대표메뉴 보정
- 대상: `DB_UnifiedSales_okpos.py`, `tests/test_unified_sales_okpos_menu_name.py`.
- 변경: 홀 주문에서 유효 메인 후보가 없고 실매출 0원 라인만 있으면 `discount_amount` 최고 행을 대표 `menu_name`으로 선택하도록 보정.
- 검증 결과: OKPOS 모듈 py_compile, 신규 대표메뉴 테스트 2건, UTF-8 읽기 확인 통과.
- 남은 위험: 기존 unified parquet에는 자동 반영되지 않으므로 해당 날짜는 `DB_UnifiedSales` 재계산 실행 필요.

## 2026-07-30 OKPOS 0원 홀 주문 기존 통합 parquet 보정
- 대상: OneDrive `mart/unified_sales_grp`의 OKPOS 홀/매장 0원 주문.
- 변경: `total_price`가 주문 내 전부 0원이고 `discount_amount`가 있는 주문만 골라 대표 `menu_name`을 할인액 최고 `item_name`으로 보정.
- 검증 결과: 58개 파일, 82주문, 557행 수정. 백업 대비 행 수·`_pk`·`total_price`·`discount_amount`·`order_cnt` 합계 변화 0건, 남은 후보 0건.
- 남은 위험: 로컬 백업은 `C:\Local_DB\temp\backups\okpos_zero_menu_repair\20260730_171931`에 보관 중이며 안정 확인 후 삭제 가능.

## 2026-07-30 OKPOS 상품조회 실패 run 및 스케줄 지연 복구
- 대상: `DB_OKPOS_Product_Dags`, `Strategy_ScheduleGuard_01_Overdue_Dags`, `DB_Daily_Corporate_Store_Report_Dags`.
- 변경: 실패 상태로 남은 `manual__2026-07-30T08:20:00+00:00`의 OKPOS 상품조회 task를 clear 후 재실행해 성공 처리하고, 스케줄 지연 가드의 현재 overdue 상태를 재확인.
- 검증 결과: `download_okpos_product`/`save_okpos_product` 성공, 운영 `상품조회.xlsx` 511개 상품코드 행 확인, 전체 스케줄 overdue 0건, 컨테이너 DAG import/import-error 0건, 로컬 테스트 23건 통과.
- 남은 위험: 기본 Windows pytest 임시/`AIRFLOW_HOME` 경로는 권한·인코딩 문제로 막힐 수 있어 `.tmp` basetemp와 테스트용 `AIRFLOW_HOME` 지정이 필요.

## 2026-07-31 Airflow 스케줄 생성 지연 반복 알림 보정
- 대상: `Strategy_ScheduleGuard_01_Overdue_Dags`, `dag_schedule_guard.py`, stale `Strategy_Policy_02_Consolidate_Dags.task_post_to_flow`.
- 변경: 같은 logical date의 DagRun이 이미 있으면 지연 대상에서 제외하고, 실제 누락분은 ScheduleGuard가 동일 logical date로 자동 보정 트리거하도록 변경. 텔레그램 `해결해라` 반복 알림은 제거.
- 검증 결과: 사용자 제보 16개 DAG의 2026-07-29 logical run 전부 success 확인, 현재 overdue 0건, scheduler heartbeat 정상, DAG import 오류 0건, `tests/test_dag_schedule_guard.py` 8건 통과.
- 남은 위험: `DB_Beamin_Macro_Dags.retry_failed`는 실제 프로세스가 살아 있는 장시간 실행 중 작업이라 강제 정리하지 않았고 완료 여부 추가 관찰 필요.

## 2026-07-31 PC2 배민 업로드 watcher 기본 활성화
- 대상: `DB_Beamin_Macro_Upload_Pc2_Dags`, `tests/test_baemin_pc2_upload_trigger.py`.
- 변경: 하위 PC 완료 시각이 불규칙해도 중앙 PC watcher가 정기 실행되도록 DAG 생성 기본값을 unpaused로 변경하고 30분 간격 스케줄은 유지.
- 검증 결과: 로컬/컨테이너 DAG import에서 `False 5,35 * * * *` 확인, Airflow import-error 없음, `tests/test_baemin_pc2_upload_trigger.py` 10건 통과, Airflow 등록 상태 `paused=False` 확인.
- 남은 위험: 이 DAG은 중앙 PC 전용이며 하위 PC에서는 Upload 계열 DAG을 계속 pause 상태로 둬야 함.

## 2026-07-31 쿠팡수동 부분 원천 덮어쓰기 차단
- 대상: `DB_CoupangMacro_load.py`, `beamin_store_io.py`, `tests/test_coupang_orders_validation.py`.
- 변경: 기존 주문 수가 있는 날짜를 더 적은 주문 수의 쿠팡 수동 파일로 재적재하려 하면 저장·마커·원천 정리를 차단하도록 보정. 미사점 2026-07-29의 39주문 원천이 10주문 파일로 축소 저장된 구조를 기준으로 재발 방지.
- 검증 결과: 쿠팡 수동 범위교체/축소차단/정규화 중복제거 테스트 3건, 배민 공용 범위교체 테스트 1건, 관련 모듈 py_compile 통과.
- 남은 위험: 이미 축소 저장된 미사점 2026-07-29 원천 parquet와 2026-07-28 누락분 복구는 OneDrive/운영 데이터 수정 승인을 받은 뒤 별도 실행 필요.

## 2026-07-31 쿠팡 원천 적재 DAG 간 공용 락 추가
- 대상: `DB_CoupangMacro_load.py`, `tests/test_coupang_orders_validation.py`.
- 변경: `DB_CoupangMacro_Load_Dags`와 `DB_CollectionCompare_Dags`가 같은 쿠팡 적재 함수를 동시에 호출해도 한 번에 하나만 원천 parquet를 쓰도록 `TEMP_DIR/locks` 기반 공용 lock을 추가.
- 검증 결과: 쿠팡 적재 락 재진입 차단 테스트 포함 관련 테스트 5건, 관련 모듈 py_compile 통과.
- 남은 위험: 세 DAG를 동시에 실행해도 적재는 직렬화되지만, 최신 비교표가 목적이면 UnifiedSales 재계산 후 CollectionCompare를 마지막에 한 번 더 실행해야 함.

## 2026-07-31 쿠팡 확장 계정소스 자동생성 전환
- 대상: `Sales_Employee_Extract_Dags.py`, `employee_accounts_export.py`, OneDrive 개발용 쿠팡 확장 계정 로드/runner.
- 변경: `sales_employee.csv`에서 `accounts_data.js`를 생성해 확장이 자동생성 계정을 우선 로드하고, 기본값 폴백 경고와 백필 큐 병합 보존을 추가.
- 검증 결과: CSV 변환 4명/쿠팡 66개, 누락 2매장 포함 및 유령 매장 제거, JS 문법, 생성물 Node 파싱, compose 문법, 컨테이너 DAG import 통과.
- 남은 위험: 실행 중 컨테이너는 compose 변경 전 생성본이라 `Extention` 마운트 적용을 위해 Airflow 컨테이너 재생성이 필요하며, 확장 리로드 후 브라우저 수동 확인이 필요.

## 2026-07-31 쿠팡 주문 수동수집 큰 범위 경고
- 대상: OneDrive 개발용 확장 `content/03_coupangeats.js`.
- 변경: 수동 주문수집에서 주문 수/날짜범위가 큰 경우 시작 전 확인창을 띄우고, 수동/배치 pacing 로그 문구를 분리해 월범위 진행을 멈춤으로 오인하지 않게 보정.
- 검증 결과: `node --check content/03_coupangeats.js`, UTF-8/깨짐 문자 검사 통과.
- 남은 위험: 이미 열린 탭에는 기존 content script가 남아 있으므로 확장 리로드와 주문 페이지 새로고침 후 적용 확인 필요.

## 2026-07-31 DB_UnifiedSales 정기 실행 08:10 조정
- 대상: `schedule.py`, `DB_UnifiedSales_Schedule_Guard_Dags.py`.
- 변경: `DB_UNIFIED_SALES_TIME`을 `10 8 * * *`로 앞당기고, 가드 알림 문구의 기대 스케줄을 08:10으로 정정.
- 검증 결과: 로컬/컨테이너 DAG import에서 `DB_UnifiedSales 10 8 * * *`, 가드 `5 9 * * *` 확인, Airflow import-error 없음.
- 남은 위험: 장기 재수집 마커가 남아 있으면 08:10에 시작해도 08:45 완료는 보장되지 않음.

## 2026-07-31 DB_UnifiedSales 수동 배달 전체기간 가드
- 대상: `DB_UnifiedSales_Dags.py`.
- 변경: `backfill=true`에서 배민/쿠팡 수동 전체기간 대상은 `FULL_RECALC_STORES`로만 제한하고, `sale_date` 단독 정정은 `FULL_RECALC_STORES` 또는 `partial_store_mode/stores`가 없으면 중단하도록 보정.
- 검증 결과: 격리 `AIRFLOW_HOME` DAG import, `py_compile`, fake context 범위 검증 통과.
- 남은 위험: `FULL_RECALC_STORES`를 작업 후 비우지 않으면 다음 backfill에서 해당 매장이 다시 전체기간 처리될 수 있음.

## 2026-07-31 DB_UnifiedSales 수동 재수집 마커 lookback 확장 차단
- 대상: `DB_UnifiedSales_baemin.py`, `DB_UnifiedSales_coupang.py`.
- 변경: 기본 lookback 실행에서 오래된 `manual_reingest_marker` 날짜를 합산하지 않도록 제거해 배민/쿠팡 수동 교정이 최근 7일만 처리되도록 보정.
- 검증 결과: 배민/쿠팡 target date 함수가 `2026-07-24`~`2026-07-30`만 반환, py_compile 통과, running `reconcile_baemin` 재시작 후 최근 7일 로그 확인.
- 남은 위험: 이미 수정 전 attempt가 일부 과거 unified parquet를 재저장했으므로, 과거 기간 영향 실측은 별도 점검 필요.

## 2026-07-31 DB_UnifiedSales FULL_RECALC_STORES 매장 제한 전체복구
- 대상: `DB_UnifiedSales_Dags.py`, UnifiedSales source별 backfill helper.
- 변경: `FULL_RECALC_STORES`가 있으면 해당 매장만 전체기간 복구하고, 배민/쿠팡 수동 교정은 수동배달 테스트 대상과의 교집합만 전체기간 처리하도록 분리.
- 검증 결과: py_compile, 컨테이너 DAG import, fake context 분기 검증 통과. 현재 설정은 source/manual scope 모두 `미사점`, manual lookback은 `None`.
- 남은 위험: `FULL_RECALC_STORES`를 비우기 전까지 이후 기본 실행도 미사점 전체기간 복구 모드로 동작하므로 복구 완료 직후 반드시 비워야 함.

## 2026-07-31 배달 수동 부분수집 ToOrder 기준 보강
- 대상: `DB_UnifiedSales_common.py`, 배민/쿠팡 수동 교정 로그와 부분수집 테스트.
- 변경: 수동 부분수집 판정에서 ToOrder 매장×플랫폼 일합계가 있으면 POS보다 우선 기준으로 사용하고, 없을 때만 POS 기준으로 fallback하도록 보정.
- 검증 결과: 미사점 2026-03-02/03-08/03-14/04-18은 배민수동과 ToOrder 합계 일치로 이벤트 없음 확인, 관련 테스트 12건 및 py_compile 통과.
- 남은 위험: ToOrder parquet가 없는 날짜는 기존처럼 POS 기준으로 판단하므로 POS 원천 오류는 별도 정정 필요.

## 2026-07-31 배달 수동 결측 알림 ToOrder 기준 보강
- 대상: `DB_UnifiedSales_common.py`, `DB_UnifiedSales_baemin.py`, `DB_UnifiedSales_coupang.py`.
- 변경: 배민/쿠팡 수동 결측 폴백 알림도 ToOrder 기준이 있으면 ToOrder 금액/건수를 우선 표시하고, 없을 때만 POS 기준으로 fallback하도록 보정.
- 검증 결과: 미사점 쿠팡 대표일 실측에서 2026-01-01/01-23/03-02는 쿠팡수동 0, ToOrder 매출 존재 결측으로 확인. 2026-07-18/07-23은 ToOrder 기준 실제 부분수집으로 확인. 관련 테스트 16건 및 py_compile 통과.
- 남은 위험: `FULL_RECALC_STORES`에 미사점이 남아 있으면 과거 전체기간 결측 알림이 계속 발생할 수 있으므로 수동 복구 완료 후 목록을 비워야 함.

## 2026-07-31 배달 수동 기준 선택 보수화
- 대상: `DB_UnifiedSales_common.py`, 수동 부분수집/결측 알림 테스트.
- 변경: ToOrder와 POS가 모두 있으면 더 낮은 금액을 기준으로 수동 결측/부분수집을 판단하고, 선택되지 않은 기준 금액과 건수는 알림 괄호에 함께 표시하도록 보정.
- 검증 결과: ToOrder 1,000,000 / POS 900,000 / 수동 10 예시는 POS 기준 부분수집으로 알림 표시, POS 과대 예시는 ToOrder 기준으로 오탐 억제 확인. 관련 테스트 18건 및 py_compile 통과.
- 남은 위험: ToOrder/POS 중 낮은 기준 자체가 과소수집이면 부족 규모가 작게 표시될 수 있으므로 알림의 보조 기준 차이를 함께 확인해야 함.

## 2026-07-31 송파삼전점 메뉴계층 시험 DAG 구현
- 대상: `DB_MenuHierarchy_Test.py`, `DB_MenuHierarchy_Test_Dags.py`, `메뉴계층시험DAG송파삼전점_260731.md`.
- 변경: 송파삼전점 원본 posfeed/okpos/배민/쿠팡 데이터를 계층화해 `new_classification_orders` 아래 CSV 산출물만 생성하는 수동 DAG를 추가.
- 검증 결과: py_compile, 파이프라인 import, DAG import(임시 AIRFLOW_HOME), 2026-04~2026-07 전체기간 메모리 변환 및 dangling parent 0건 확인. 2026-04~06 canonical 금액/주문수는 unified source별 합계와 일치.
- 남은 위험: OneDrive 산출 CSV 생성은 승인 전 실행하지 않았고, okpos의 qty/unit_price/discount_amount 세부 합계는 기존 unified 보정 이력과 차이가 있어 금액/order_cnt 기준으로 우선 검증해야 함.

## 2026-07-31 송파삼전점 메뉴계층 시험 DAG 전체기간 기본값 보정
- 대상: `DB_MenuHierarchy_Test.py`, `DB_MenuHierarchy_Test_Dags.py`, `메뉴계층시험DAG송파삼전점_260731.md`.
- 변경: 기본 실행을 전월 단일 월이 아니라 송파삼전점 원본 데이터가 존재하는 전체 `ym` 자동 탐색(`2026-04`~`2026-07`)으로 변경. conf `ym` 지정 시 단일/콤마 구분 월 목록 처리도 유지.
- 검증 결과: 전체기간 메모리 변환 결과 2026-04 4,290행, 2026-05 9,967행, 2026-06 11,970행, 2026-07 14,170행이며 월별 menu_seq 빈값과 dangling parent 모두 0건.
- 남은 위험: 2026-07-31 원본은 현재 unified 미적재분이 있어 2026-07은 okpos 477,400원/6건, posfeed 57,600원/2건이 unified보다 크게 보임.

## 2026-07-31 송파삼전점 메뉴계층 시험 DAG 스케줄 적용
- 대상: `DB_MenuHierarchy_Test_Dags.py`, `메뉴계층시험DAG송파삼전점_260731.md`.
- 변경: 수동 전용 `schedule=None`에서 매일 10:00, 13:00, 15:00 실행 cron(`0 10,13,15 * * *`)으로 변경.
- 검증 결과: py_compile 및 임시 AIRFLOW_HOME DAG import에서 schedule 값 `0 10,13,15 * * *` 확인.
- 남은 위험: 기본 실행이 전체기간 CSV를 매번 재생성하므로, 산출 디렉터리 OneDrive 동기화 부하를 관찰해야 함.

## 2026-07-31 송파삼전점 메뉴계층 시험 DAG 재실행 안정화
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`, `메뉴계층시험DAG송파삼전점_260731.md`.
- 변경: 재실행 시 LLM/모델 분류와 상품표 쓰기 호출을 금지하는 AST 가드를 추가하고, `10_orders_{ym}.csv`는 unified 24컬럼만 저장하도록 변경. 계층 컬럼은 `11_hierarchy_{ym}.csv`로 분리.
- 검증 결과: 전용 테스트에서 금지 호출 가드, 기존 검수표/alias/규칙 fallback 우선순위, 주문서·계층 파일 컬럼 분리 확인.
- 남은 위험: `11_hierarchy_{ym}.csv`를 후속 left join할 때는 `_pk` 우선, 필요 시 `sale_date/source/store/platform/order_id/item_seq` 복합키를 사용해야 함.

## 2026-07-31 송파삼전점 메뉴계층 시험 DAG left 주문서 추가
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`, `메뉴계층시험DAG송파삼전점_260731.md`.
- 변경: 순수 unified 양식 `10_orders_{ym}.csv`는 유지하고, 계층 컬럼이 붙은 `12_orders_left_{ym}.csv`를 추가 산출하도록 변경. `11_hierarchy_{ym}.csv`는 별도 조인 재료로 유지.
- 검증 결과: 전용 테스트에서 `10_orders`는 24컬럼, `12_orders_left`는 unified 24컬럼 + 계층 5컬럼으로 저장됨을 확인.
- 남은 위험: 사람이 수정할 대상 파일을 혼동하지 않도록 운영 시 `10_orders`와 `12_orders_left` 용도를 구분해야 함.

## 2026-07-31 쿠팡 orders 덜수집 재적재 보존 병합
- 대상: `DB_CoupangMacro_load.py`, `tests/test_coupang_orders_validation.py`.
- 변경: 쿠팡 orders 재적재가 기존 일자 주문 수보다 적으면 차단하지 않고 기존 parquet를 보존한 채 신규 CSV 행을 append 후 기존 중복 기준으로 dedup하도록 변경.
- 검증 결과: 쿠팡 적재 shrink/교체/중복/lock 테스트 6건, py_compile, DAG import, UTF-8 검증 통과. 미사점 2026-06-24 실패 CSV 격리 재현에서 차단 0건, shrink 날짜 중복 148행 제거, 기존 6,156행 유지 확인.
- 남은 위험: 실제 OneDrive parquet 반영과 실패 DAG 재실행은 승인 전 수행하지 않았으며, 완전 재수집 파일은 기존처럼 커버 일자 교체 정책을 유지함.

## 2026-07-31 송파삼전점 메뉴계층 시험 DAG 설명 파일 추가
- 대상: `DB_MenuHierarchy_Test.py`, `DB_MenuHierarchy_Test_Dags.py`, `test_menu_hierarchy_no_model_classification.py`, `메뉴계층시험DAG송파삼전점_260731.md`.
- 변경: `new_classification_orders/설명.md`를 생성하는 파이프라인 함수와 DAG 태스크를 추가해 산출물 계약, 10/11/12 파일 용도, LLM 금지, 조인키를 인계 문서로 남기도록 보완.
- 검증 결과: 전용 테스트에 설명 파일 경로·핵심 문구 검증을 추가.
- 남은 위험: 설명 파일은 DAG 실행 때마다 덮어쓰기되므로 수동 메모는 별도 파일에 분리해야 함.

## 2026-07-31 송파삼전점 메뉴계층 시험 DAG 기존 분류 참조 보완
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`, `메뉴계층시험DAG송파삼전점_260731.md`.
- 변경: `fin_product_map_review_input.csv`의 기존 수동분류와 `fin_product_map_join.csv` category를 우선해 리뷰/옵션/토핑의 가격 기반 main 승격을 차단하고, 쿠팡 priced row는 원본 부모 `menu_name`을 본품명으로 쓰도록 보완.
- 검증 결과: py_compile, 전용 pytest 10건, 전체기간 CSV 재생성, DAG import, UTF-8 읽기 확인. `10_orders` 계층컬럼 없음, `_pk` 중복/누락 0, parent 미존재 0, gap은 60건.
- 남은 위험: 2026-07 쿠팡 `재주문 1위` 부모 메뉴와 일부 음료는 기존 상품표 item_id가 없어 `04_product_gap.csv`에 남으며, okpos/posfeed 원본상 옵션성 이름이 main인 잔여 4행은 수동 확인 필요.

## 2026-07-31 송파삼전점 메뉴계층 시험 DAG product 분류 우선 적용
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`, `메뉴계층시험DAG송파삼전점_260731.md`, `new_classification_orders/설명.md`.
- 변경: `line_role` 산정도 product 테이블의 `메인/1인/세트/사이드/옵션/토핑/리뷰` 분류를 우선하도록 수정하고, `fin_product_grp_input.csv`의 `상품코드/수동분류`도 category fallback으로 사용.
- 검증 결과: 전용 pytest 11건 통과, 전체기간 CSV 재생성. product상 옵션/리뷰 이름의 main 잔여는 2026-07 posfeed 원본 메인명 1건만 남고 `기본/기본맛` main은 0건.
- 남은 위험: `04_product_gap.csv` 60건은 쿠팡 부모 메뉴/음료가 기존 product item_id에 없어 남은 실제 정리 후보.

## 2026-08-02 OKPOS 카드 세션 종료 재시도 및 autoheal 승인대기 제거
- 대상: `DB_OKPOS_Card_Test.py`, `watch_heal_queue.py`, `test_okpos_product_download_validation.py`, `test_autoheal_watcher.py`.
- 변경: OKPOS 카드 다운로드 중 `invalid session id` 등 WebDriver 연결 종료가 나면 task 내부에서 브라우저를 재생성해 재시도하고, autoheal은 격리 워크트리 dirty 상태·죽은 watcher pid heartbeat·코드 수정 승인대기에서 멈추지 않도록 조정.
- 검증 결과: 전용 pytest 28건, py_compile, DAG import, Airflow import-error 0건 확인. Airflow 메타DB에서 오늘 KST 실패 task 4건 확인.
- 남은 위험: OneDrive 쓰기 및 git commit/push는 계속 승인 대상이며, 실제 OKPOS 운영 재실행은 스케줄/수동 run 결과로 확인 필요.

## 2026-07-31 송파삼전점 메뉴계층 시험 DAG 산출 파일 축소
- 대상: `DB_MenuHierarchy_Test.py`, `DB_MenuHierarchy_Test_Dags.py`, `test_menu_hierarchy_no_model_classification.py`, `메뉴계층시험DAG송파삼전점_260731.md`, `new_classification_orders`.
- 변경: 기본 DAG와 `run_all()` 산출을 통합 5개 파일(`04_product_gap.csv`, `10_orders.csv`, `11_hierarchy.csv`, `12_orders_left.csv`, `설명.md`)로 축소하고 월별/디버그 CSV를 정리.
- 검증 결과: py_compile, pytest 11건, DAG import에서 태스크 `resolve_ym/write_readme/build_orders` 확인. OneDrive 산출 폴더 남은 파일 5개 확인.
- 남은 위험: raw/diff/summary는 기본 생성하지 않으므로 상세 디버그가 필요하면 함수 단위로 별도 실행해야 함.

## 2026-07-31 송파삼전점 메뉴계층 시험 DAG 사이드 부모 승격 차단
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`, `new_classification_orders`.
- 변경: product 분류상 `사이드/음료/기타`를 `main` 경계에서 제외해 기존 메인 메뉴 아래 option으로 붙도록 수정.
- 검증 결과: 주문 `T2DC0001JJ5H`의 `미니 계란찜` 이후 행이 모두 `[우삼겹살] 우도리탕` 아래로 연결됨을 확인. 전체 통합 `10/11/12` 29,177행 `_pk` 1:1, parent 미존재 0, 사이드 main 0건.
- 남은 위험: `04_product_gap.csv` 74건은 product에 없는 쿠팡 부모 메뉴/음료 후보로 남음.

## 2026-07-31 송파삼전점 배민 1인분 product 분류 보정
- 대상: `fin_product_map_review_input.csv`, `fin_product_map.csv`, `fin_product_map_join.csv`, `new_classification_orders`.
- 변경: 송파삼전점 배민수동 `item_id=200008000`, `item_name=1인분`의 분류를 `기타`에서 `1인`으로 보정.
- 검증 결과: 주문 `T2DC0001F97O`에서 `1인분`이 `main`, 후속 리뷰/맛 선택 행이 `parent_item_seq=1` option으로 연결됨. 통합 `10/11/12` 29,157행 `_pk` 1:1, parent 미존재 0.
- 남은 위험: product 미등록 후보는 `04_product_gap.csv` 75건으로 별도 검수 필요.

## 2026-07-31 배민 매크로 legacy B2/B3/B4 DAG 제거
- 대상: `DB_Beamin_Macro_Dags.py`, `test_baemin_batch_split.py`, `harness/baemin_macro.md`.
- 변경: 단일 DAG 4배치 전환 후 남아 있던 무스케줄 legacy `DB_Beamin_Macro_Dags_B2/B3/B4` 전역 DAG 생성과 전용 트리거 경로를 제거.
- 검증 결과: 배민 배치/driver recovery/validation 테스트 33건 통과, 로컬 DAG import에서 legacy 전역 객체 없음 확인, 컨테이너 `airflow dags list`와 import-error 0건 확인.
- 남은 위험: Airflow UI에서 삭제 반영은 scheduler DAG 재파싱 이후 표시되며, 기존 과거 DagRun 기록은 DB에 남을 수 있음.

## 2026-08-01 OKPOS 카드 당일 DAG 재실행 오류 처리
- 대상: `DB_OKPOS_Review_card_Today.py`, `DB_OKPOS_Card_Test.py`.
- 변경: OKPOS 승인현황 no-data 화면은 실제 문구 확인 후 0건으로만 처리하고, Today DAG는 primary 0건 허용 플래그를 주입하도록 보완.
- 검증 결과: 컨테이너 DAG import-error 0건 확인, 실패 run `scheduled__2026-07-31T12:25:00+00:00` 재실행 성공.
- 남은 위험: OKPOS 화면이 느리게 갱신될 때 첫 조회가 0건일 수 있어, 실제 데이터 생성 시 재실행으로 적재된다.

## 2026-08-01 DAG 실패 일괄 복구 및 재발 방지
- 대상: `DB_Hall_Sales_Target_Dags`, `DB_ToOrder_Daily_Store_Dags`, `Strategy_SusamReport_01_FlowUpload_Dags`, `DB_ToOrderMenu_Dags`, `DB_MenuHierarchy_Test_Dags`, `DB_Sales_Alert_01_Score_AI_Daily_Collection`.
- 변경: 송파삼전점 OKPOS 누락 보정, ToOrder stale 다운로드 정리, 수삼보고서 source sensor 여유 조정, ToOrder Selenium 직렬 pool/브라우저 복구/profile 정리, MenuHierarchy 최신월 기본 처리와 OKPOS lazy import, Sales Alert logical date 기준 처리 적용.
- 검증 결과: 대상 실패 run 재실행 성공, ToOrderMenu 추가 scheduled run도 success 확인, DAG import-error 0건, 관련 pytest 32건 통과.
- 남은 위험: Chrome/WSL crash dump 재발 시 디스크를 다시 압박할 수 있어 `/tmp/toorder_chrome_runtime` 정리와 C 드라이브 여유공간 모니터링이 필요하다.

## 2026-08-03 DB_UnifiedSales today resolve_date 실패 복구
- 대상: `DB_UnifiedSales_Dags.py`.
- 변경: `today_mode=true` + `sale_date` 실행은 매장 지정 정정 가드에서 제외하도록 조건 보정.
- 검증 결과: 컨테이너에서 `resolve_date` today conf 성공, 일반 수동 sale_date 가드 유지, DAG import 성공, 복구 run `manual__2026-08-03T00:10:58.397912+00:00`의 `resolve_date` 성공 확인.
- 남은 위험: 복구 run 전체 완료는 downstream 작업 시간에 따라 Airflow에서 계속 확인 필요.

## 2026-08-03 DB_UnifiedSales 7/31~8/2 미사점 부분덮임 복구
- 대상: `DB_UnifiedSales_Dags.py`, `unified_sales_260731/260801/260802.parquet`, `daily_summary.parquet`.
- 변경: `FULL_RECALC_STORES`는 `full_recalc=true` conf에서만 동작하도록 제한하고, 단일 `sale_date` 전체매장 정정을 허용. 2026-07-31~2026-08-02를 소스별 재처리.
- 검증 결과: 8/1 64개 매장, 8/2 63개 매장으로 복구. 송파삼전점 8월 배민 465,100 / 쿠팡 916,200 / 홀 1,997,000 오차 0.
- 남은 위험: 2026-08 월별 검증에 비쿠팡 잔여 35행 금액상이·소량 DB 누락이 남아 개별 원천 점검 필요.

## 2026-08-03 DB_UnifiedSales FULL_RECALC_STORES 하이브리드 보정
- 대상: `DB_UnifiedSales_Dags.py`, `DB_UnifiedSales_common.py`.
- 변경: `FULL_RECALC_STORES`가 기본 실행 범위를 대체하지 않고 lookback/일자 정정 후 지정 매장만 전체기간 추가 복구하도록 보정. `backfill=true`는 전체 백필 의미를 유지.
- 검증 결과: `_is_full_recalc_mode` 잔여 참조 0건, py_compile, 컨테이너 DAG import, fake context resolve/build 분기 검증 통과.
- 남은 위험: `FULL_RECALC_STORES`에 매장을 남겨두면 일반 lookback/정정마다 해당 매장 전체기간 추가복구가 붙으므로 작업 완료 후 비워야 함.

## 2026-08-03 배민 매크로 2026-08-02 수집누락 수정
- 대상: `paths.py`, `DB_BaeminManual_load.py`, `DB_Beamin_04_orders.py`, `DB_Beamin_Macro_validate.py`.
- 변경: `E:/d_down` 수동 orders 스캔 추가, 날짜 필터 정착/배지 실패 처리 강화, 0행/TotalSummary 검증 실패 차단, validate brand coverage fallback 추가.
- 검증 결과: py_compile 통과, 컨테이너 DAG import 3건 통과, `pytest tests/ -k baemin` 124 passed / 3 failed.
- 남은 위험: 3개 실패는 기존 dirty 변경 영향으로 보이며, OneDrive 데이터 복구 실행은 별도 승인 전이라 미수행.

## 2026-08-03 배민 2026-08-02 수동 적재 및 재검증
- 대상: `E:/d_down`, `Collect_Data/영업관리부_수집/_archived`, `data/analytics/baemin_macro/orders`.
- 변경: 20260713 잔여 orders CSV 4개 격리 후 20260803 수동 orders CSV 18개 적재 및 archive 이동, 기흥테라타워점 orders-only 재수집 보완.
- 검증 결과: 수동 폴더 잔여 orders 0개, 강동점 도리당 491,200원·기흥테라타워점 도리당 95,100원/나홀로 76,500원 확인. ToOrder 재검증 61개 비교 중 잔여 4개.
- 남은 위험: 강동점 나홀로 86,200원은 원천 CSV가 없고 Selenium 재시도도 저장 실패. 동두천지행점은 지시서상 파일 없음, 김포장기점·하늘도시점은 source_mismatch로 잔존.

## 2026-08-03 쿠팡이츠 수집 확장 10057 오탐/복구 보완
- 대상: OneDrive 개발용 확장 `content/03_coupangeats.js`, `runner.js`.
- 변경: 확장 모달 자가매칭 차단, 쿠폰/광고 위젯 오류 분리, 제한 카운터 감쇠, 페이지네이션 언마운트 재조회 복구, 로그인 실패 백오프와 자동 reload 1회 제한 적용.
- 검증 결과: `node --check` 3개 파일 통과, payload note 1건 보존, 모달 출력 경로 `10057` 잔여 0건 확인.
- 남은 위험: 브라우저 수동 검증(T4~T9)은 확장 리로드와 실제 쿠팡이츠 세션에서 확인 필요.

## 2026-08-03 홀 주간보고 월경계 주 합산 수정
- 대상: `DB_Hall_Sales_Excel.py`.
- 변경: 같은 `입력날짜`의 월경계 2행을 주간보고 이번주/주목표 기준으로 합산하고, 객단가는 매출합/영수건수합으로 재계산하며 마케팅 주목표도 걸친 달 전체 일할 합산으로 보정.
- 검증 결과: py_compile, 모듈 import, 임시 AIRFLOW_HOME DAG import, `.tmp` Excel 생성 검증, CSV·일별·unified 7일 합계 `5,023,460 / 144건` 대조 통과.
- 남은 위험: 운영 OneDrive `hall_weekly_report.xlsx` 재생성은 별도 승인 전이라 수행하지 않음.

## 2026-08-03 쿠팡 CSV Downloads 갇힘 복구
- 대상: `scripts/coupang_host_chrome.ps1`, `E:/down`, `DB_CoupangMacro_Load_Dags`.
- 변경: Chrome Default 프로필 Preferences의 다운로드 경로를 실행 전 자가교정하도록 추가하고, Downloads 잔여 `coupangeats_*.csv` 246개를 `E:/down`으로 이동 후 적재.
- 검증 결과: PowerShell 문법·UTF-8 검사, pref `E:\down` 반영, DAG run `manual__2026-08-03T06:21:26+00:00` success. orders 85/6879행, cmg 84/168행, options 77/9874행 적재 및 cleanup 246개 완료.
- 남은 위험: 다음 쿠팡 수집 사이클에서 신규 CSV가 Downloads가 아닌 `E:/down`에 떨어지는지 후속 확인 필요.

## 2026-08-03 Flow 가맹점 프로젝트 수집·적재 DAG 구현
- 대상: `Strategy_FlowStore_01_Collect_Dags`, `SMP_flow_store_collect`, Flow parquet/스케줄 상수, DB 스키마 문서.
- 변경: Flow 프로젝트·게시글·재귀 하위업무·댓글 정규화, parquet 원자 저장, PostgreSQL 3테이블 replace 적재, API env 계약 추가.
- 검증 결과: py_compile, Flow 전용 pytest 6건, 컨테이너 DAG import 및 파이프라인 import 통과.
- 남은 위험: Flow API 엔드포인트/인증 헤더가 아직 미확정이라 실 API 수집은 env 설정 후 계약 검증 필요.

## 2026-08-03 Flow API KEY 컨테이너 주입 수정
- 대상: `docker-compose.yaml`, `Strategy_FlowStore_01_Collect_Dags` 실행 환경.
- 변경: `FLOW_API_KEY`와 Flow API URL/헤더 env 항목을 Airflow 공통 컨테이너 environment에 추가.
- 검증 결과: scheduler/worker 컨테이너에서 `FLOW_API_KEY` 주입 확인, DAG import 정상.
- 남은 위험: `FLOW_PROJECTS_API_URL`, `FLOW_POSTS_API_URL_TEMPLATE`, `FLOW_POST_DETAIL_API_URL_TEMPLATE`, `FLOW_AUTH_HEADER_NAME`은 아직 비어 있어 실 API 실행 전 `.env` 보강 필요.

## 2026-08-03 Flow API 직접 연결 및 rate limit 보완
- 대상: `SMP_flow_store_collect.py`, `Strategy_FlowStore_01_Collect_Dags`.
- 변경: User API 기본값(`x-flow-api-key`, `/user/projects`, `/user/posts/...`) 적용, 429 방지 요청 간격·진행률 로그·상세 호출 제한 추가, DB 적재 전 저장 XCom 가드 추가.
- 검증 결과: Flow API 실측 연결, capped run success, parquet 104/600/643행 및 PostgreSQL replace 적재 확인, 기존 수동 실패 run 상태 정리.
- 남은 위험: 첫 전량 수집은 Flow 하위업무 재귀가 많아 여러 실행에 나눠 처리되며, 기본 1회 상세 호출 제한은 600건.

## 2026-08-03 Flow 저장 위치 및 재시도 증분 검증
- 대상: `Strategy_FlowStore_01_Collect_Dags`, `flow_*` parquet, `FLOW_STATE_JSON`.
- 변경: 재시도 run으로 기존 저장분 skip 동작 확인 및 추가 병합 저장 수행.
- 검증 결과: 저장 위치 `OneDrive/data/analytics/flow`, state `C:/Local_DB/flow_posts_index.json`; post 600→1121, comment 643→1479, 변경 대상 1458→1408로 감소.
- 남은 위험: 목록 API 전체 확인은 매 실행 약 90~110초 걸리며, 미저장 상세는 여러 회차에 나눠 축적 필요.

## 2026-08-03 배민 메뉴그룹 분리 및 menu_name 분류 수정
- 대상: `DB_MenuHierarchy_Test.py`.
- 변경: 배민 `외 N건` 메뉴 수 제약 기반 경계 보강, `대표메뉴` 기반 `menu_name` 표시명, 주문 단위 최신 `collected_at` dedup으로 반복 옵션 라인 보존.
- 검증 결과: py_compile/AST/UTF-8 확인, 모델 호출 금지 검사 OK, 메뉴계층 pytest 13건 통과, 임시 AIRFLOW_HOME DAG import 및 인라인 배민 샘플 검증 통과.
- 남은 위험: OneDrive `new_classification_orders` 산출물 갱신과 전 기간 실데이터 비교는 별도 승인 전이라 수행하지 않음.

## 2026-08-03 송파삼전 닭도리탕 옵션 리포트 DAG 연동
- 대상: `DB_MenuHierarchy_Test.py`, `DB_MenuHierarchy_Test_Dags.py`, `new_classification_orders/설명.md`.
- 변경: DAG 주문서 생성 뒤 `30~34_chicken_*.csv` 리포트를 자동 생성하고, 신메뉴 LLM 후보 검토 후 메뉴계층 재실행 운영 흐름을 설명에 추가.
- 검증 결과: py_compile, 모델 호출 금지 검사 OK, 메뉴계층 pytest 13건 통과, 임시 AIRFLOW_HOME DAG import 통과. 2026-08 기준 닭 관련 109그룹 전부 OK, unresolved 0건.
- 남은 위험: 신메뉴는 `DB_FinProduct_Map_Dags`의 LLM 후보를 사람이 검토해야 하며, 새 메뉴 유형이 환산표 밖이면 `33_chicken_unresolved.csv`에 남을 수 있음.

## 2026-08-03 Flow 상세 수집 SIGTERM 복구
- 대상: `Strategy_FlowStore_01_Collect_Dags`, `SMP_flow_store_collect`.
- 변경: 종료 로그 태스크가 실패를 성공으로 가리지 않도록 기본 `all_success`로 되돌리고, Flow 상세 기본 배치를 50건으로 축소하며 호출 제한 중단 시 state 저장을 생략하도록 보강.
- 검증 결과: py_compile, UTF-8 검사, 컨테이너 DAG import 및 상수 확인 통과. 기존 대체 run `manual__flow_api_capped_20260803T174845`는 success 확인.
- 남은 위험: 초기 백필 잔여 1,156건은 이후 실행에서 나누어 수집 필요.

## 2026-08-03 Flow mart parquet 전환 및 PostgreSQL 제거
- 대상: `Strategy_FlowStore_01_Collect_Dags`, `SMP_flow_store_collect`, Flow 경로 상수, `docs/db-schema.md`.
- 변경: Flow 저장 위치를 `OneDrive/data/mart/flow`로 변경하고 `flow_post`/`flow_comment`를 `project_id=<id>` 파티션 parquet로 저장, DAG의 PostgreSQL 적재 태스크 제거.
- 검증 결과: 기존 analytics parquet 104/1121/1479건을 mart로 마이그레이션 후 새 DAG run 성공, 최종 mart 104/1206/1592건, `public.flow_*` 3테이블 drop 유지, Flow pytest 10건 통과.
- 남은 위험: 초기 백필 잔여분은 기존 state 기준으로 계속 증분 수집되며, 새 파티션은 수집된 프로젝트부터 점진적으로 늘어난다.

## 2026-08-03 Flow 전체 백필 모드 및 최초 전체 수집 완료
- 대상: `Strategy_FlowStore_01_Collect_Dags`, `SMP_flow_store_collect`.
- 변경: DAG conf `full_backfill=true` 또는 `flow_detail_max_posts=0`로 상세 제한 없이 전체 백필 가능하게 하고, 대용량 반환값 로그 출력을 비활성화.
- 검증 결과: full backfill run 성공, mart 최종 `project=104/post=5079/comment=5679`, 게시글 파티션 104개로 모든 프로젝트 coverage 확인, 직후 일반 run은 변경분 없음으로 상세/저장 skip 확인.
- 남은 위험: 전체 백필은 약 45분 소요되어 수동 1회성 용도로만 쓰고, 예약 실행은 기본 50건 제한 및 증분 pass 구조를 유지한다.

## 2026-08-03 Flow 일일 증분 스케줄 조정
- 대상: `SMP_FLOW_COLLECT_TIME`, `Strategy_FlowStore_01_Collect_Dags`.
- 변경: 06:00 실행을 03:55 KST로 조정해 03:15 POSFEED, 03:30/03:50 쿠팡 로드, 03:35 PC2 감시 이후의 3시대 저충돌 구간에 배치.
- 검증 결과: py_compile, UTF-8 확인, 컨테이너 DAG import schedule `55 3 * * *`, scheduler 메타DB 다음 생성시각 `2026-08-04 03:55 KST` 반영 확인.
- 남은 위험: 일일 증분은 직전 기준 약 107초지만, 오래된 게시글 대량 수정이 있으면 상세 수집 시간이 늘어날 수 있다.

## 2026-08-03 Flow 프로젝트별 첨부 다운로드 옵션 추가
- 대상: `SMP_flow_store_collect`, Flow mart 경로, `docker-compose.yaml`, `.env.example`, `docs/db-schema.md`.
- 변경: `FLOW_ATTACHMENT_PROJECT_IDS` 변수에 프로젝트 ID를 넣으면 해당 프로젝트 게시글 상세 수집 시 첨부 metadata와 원본 파일을 `flow_attachment*` 경로에 저장하도록 추가.
- 검증 결과: Flow 전용 pytest 13건, py_compile, UTF-8 확인, 실제 이미지 첨부 1건 다운로드 성공, Airflow Variable `FLOW_ATTACHMENT_PROJECT_IDS` 생성 확인.
- 남은 위험: 첨부 대상 프로젝트를 처음 추가하면 기존 게시글 첨부 확인을 위해 해당 프로젝트 상세 조회가 한 번 증가하며, 파일 용량에 따라 OneDrive 동기화 부하가 생길 수 있다.

## 2026-08-04 개인 AI 브리핑 Flow parquet 전환
- 대상: `Private_MorningBriefing_Dags`, `morning_briefing_pipeline`, `.env.example`, `tests/test_morning_briefing_flow.py`.
- 변경: Selenium 캘린더 대신 mart Flow 게시글/댓글 parquet에서 작성자·담당자 `조민준` 업무만 읽고, 오늘 할 일·어제 한 일 리뷰·보완할 점을 규칙 기반으로 생성하도록 전환.
- 검증 결과: py_compile, UTF-8 확인, 컨테이너 DAG import, 컨테이너 함수 실행 메시지 생성, `test_morning_briefing_flow.py` 3건 통과.
- 남은 위험: 브리핑 최신성은 `Strategy_FlowStore_01_Collect_Dags`의 `/mart/flow` 갱신 성공에 의존하며, Flow 담당자 표기가 비정형이면 일부 업무가 제외될 수 있다.

## 2026-08-04 개인 AI 브리핑 모니터링 CSV 경고 제거
- 대상: `morning_briefing_pipeline`.
- 변경: Airflow DB 기준 실패/로그오류가 0건일 때 `dags_monitoring_YYYYMMDD.csv` fallback 파일이 없어도 정상으로 보고 경고 로그를 남기지 않도록 조정.
- 검증 결과: py_compile, UTF-8 확인, 컨테이너 함수 실행에서 기존 `모니터링 CSV 없음` 경고 미출력 확인.
- 남은 위험: Airflow 메타DB 조회 자체가 실패하는 경우에는 기존 CSV fallback과 fallback 실패 경고를 유지한다.

## 2026-08-04 개인 AI 브리핑 역할 표기 개선
- 대상: `morning_briefing_pipeline`, `test_morning_briefing_flow.py`.
- 변경: 브리핑 섹션 헤더를 `#` 대신 이모지로 바꾸고, Flow 항목마다 `작성자: ... | 담당자: ...` 역할 구분을 표시. 보완점은 담당자-only 과거 업무를 제외하고 작성자/어제활동 기준으로 제한.
- 검증 결과: py_compile, Flow 브리핑 pytest 3건, 컨테이너 메시지 생성에서 `#` 미포함·송파삼전점 마케팅 업무 미포함·역할 표기 포함 확인.
- 남은 위험: 작성자 기준 보완점은 과거 작성 후 대기 상태인 글을 계속 표시할 수 있어, 필요 시 날짜 lookback 제한을 별도로 조정해야 한다.

## 2026-08-04 BSP KPI 주간 목표·실적 통합 DAG 신규
- 대상: `Strategy_BspKpi_01_Weekly_Dags`, `SMP_bsp_kpi_weekly`, `schedule.py`, `paths.py`, `tests/test_bsp_kpi_weekly.py`.
- 변경: 브랜드전략기획팀 주간 KPI 엑셀 4개(바이럴·직영점 마케팅 x 목표·실적)를 `(주 시작일, 도메인, 지표)` 그레인 세로형 마트로 통합해 `MART_DB/brand_strategy_planning_team/bsp_kpi/`(입력 엑셀과 같은 폴더)에 parquet·csv 원자 저장하고, 직전 완료 주차 실적 미입력 담당자를 텔레그램으로 리마인드(매주 월·화 11:00). 수신자는 `ALERT_RECIPIENTS` 레지스트리로 분리해 사람별로 telegram/email 채널을 켤 수 있게 했다.
- 검증 결과: pytest 11건 통과, 호스트/컨테이너 모두 278행(바이럴 14주x7지표 + 마케팅 15주x12지표) 생성 확인, 컨테이너 DAG import 오류 0건, `airflow tasks test` 2개 태스크 SUCCESS, 알림 드라이런에서 미입력 2건/0건/행없음 케이스 정상 분기.
- 남은 위험: 원본 엑셀 컬럼명 오타(`참여자 수 목표`, `목표네이버 비용`, `목표당근 노출`)를 레지스트리에 그대로 고정했으므로 담당자가 헤더를 수정하면 해당 지표가 매칭 실패 경고와 함께 누락된다. 텔레그램은 개인별 chat_id가 없어 공용 `TELEGRAM_CHAT_ID` 1개로만 나가며, 개인 분리가 필요하면 email 채널을 쓴다. 실제 미입력 발생 시 발송은 아직 검증하지 않았다(직전 완료 주차 2026-07-27은 양쪽 입력 완료라 0건).

## 2026-08-04 BSP KPI 미입력 알림 메일 채널 추가
- 대상: `SMP_bsp_kpi_weekly`, `tests/test_bsp_kpi_weekly.py`.
- 변경: 수신자를 파일 상단 상수(`MAIL_BSP_KPI_*` -> `BSP_KPI_ALERT_EMAILS`, `BSP_KPI_ALERT_TELEGRAM`)로 노출해 담당자 추가를 한 줄로 처리하고, 판정 결과에 `missing_rows`를 담아 텔레그램 평문(`render_alert_text`)과 메일 HTML(`render_alert_html`)을 같은 데이터로 렌더하도록 분리. 메일은 인라인 스타일 + 중첩 table 기반 "입력 요청" 리마인드 본문(담당자/영역/입력 파일 표)이며 값은 `html.escape` 처리한다. `AlertRecipient` dataclass는 제거했다.
- 검증 결과: pytest 24건 통과(HTML 6건 포함), 실데이터로 렌더한 메일 본문에서 담당자 2명·입력 파일명·주차 기간(08-03 ~ 08-09) 확인, 외부 리소스/`<style>` 미포함 확인, 수신자 해석 `['a17019@kakao.com']`, 컨테이너 DAG import 오류 0건, `airflow tasks test` SUCCESS(미입력 0건이라 미발송).
- 남은 위험: 실제 메일 발송은 아직 검증하지 않았다(직전 완료 주차가 입력 완료라 발송 조건이 안 됨). 텔레그램은 여전히 공용 `TELEGRAM_CHAT_ID` 1개라 개인 분리가 필요하면 메일 채널을 써야 한다.

## 2026-08-04 Flow 산출물 analytics 이전
- 대상: `paths.py`, `SMP_flow_store_collect` 산출물 경로.
- 변경: Flow primary parquet/첨부 저장 루트를 `MART_DB/flow`에서 `ANALYTICS_DB/flow`로 변경해 신규 DAG 실행 산출물이 OneDrive `data/analytics/flow`에 쌓이도록 조정.
- 검증 결과: UTF-8 확인, Flow 경로 해석 확인, py_compile 통과, `pytest tests/test_flow_store_collect.py --basetemp=C:\airflow\.tmp\pytest_flow_store_collect` 13건 통과, 임시 `AIRFLOW_HOME` 기준 DAG import 확인.
- 남은 위험: 기존 `data/mart/flow` 산출물 이동은 OneDrive 수정 승인을 받은 뒤 충돌 확인 후 수행해야 한다.

## 2026-08-04 Flow 방문일지 마트 저장 위치 확정
- 대상: `paths.py`, `Flow방문일지데이터마트_260804.md`, `docs/db-schema.md`.
- 변경: 방문일지 마트 기본 산출 루트를 `MART_DB/Flow_mart/Flow_visit`로 추가하고, `FLOW_VISIT_BASE_DIR` env override를 지원하도록 계획/스키마 문서를 정리. Flow 원천은 `ANALYTICS_DB/flow` 읽기 전용으로 명확히 분리.
- 검증 결과: 경로 상수 import와 기본/override 해석 확인, UTF-8 읽기 확인.
- 남은 위험: OneDrive 실제 산출물 생성 검증은 별도 승인 후 방문일지 마트 파이프라인 구현 단계에서 수행해야 한다.

## 2026-08-04 Flow 방문일지 마트 DAG 명칭 조정
- 대상: `Flow방문일지데이터마트_260804.md`.
- 변경: 신규 DAG 계획명을 `Sales_FlowVisit_01_Mart_Dags`로 변경하고 위치를 `dags/sales/`로 정리.
- 검증 결과: 구 DAG명 잔여 참조 0건, UTF-8 읽기 확인.
- 남은 위험: 실제 DAG 파일은 아직 생성 전이므로 구현 시 `dags.sales.Sales_FlowVisit_01_Mart_Dags` import 기준으로 검증해야 한다.

## 2026-08-04 Sales_FlowVisit_01_Mart_Dags 등록
- 대상: `Sales_FlowVisit_01_Mart_Dags`, `SMP_flow_visit_mart`, `flow_visit_taxonomy.json`, `schedule.py`.
- 변경: Airflow UI 등록용 DAG와 방문일지 마트 파이프라인/taxonomy/스케줄 상수를 추가. Flow 원천은 analytics partition 우선, 기존 mart partition fallback 순서로 읽도록 보강.
- 검증 결과: py_compile, 컨테이너 DAG import, `airflow dags list-import-errors` 0건, `airflow dags list`에서 DAG 확인, 방문일지 10건 날짜 파싱 확인.
- 남은 위험: 신규 DAG는 기본 paused 상태이며, OneDrive 산출물 생성 검증은 별도 승인 후 unpause 또는 수동 run으로 수행해야 한다.

## 2026-08-04 Flow 방문일지 마트 run 감시 및 LLM 캐시 보강
- 대상: `SMP_flow_visit_mart`, `Sales_FlowVisit_01_Mart_Dags` manual run.
- 변경: `task_parse_visit_meta`는 execution_date 경합으로 retry 후 정상 성공 확인. LLM 결과를 게시글 단위로 즉시 캐시 저장하고 전체 cache hit 시 Ollama 연결을 생략하도록 보강.
- 검증 결과: manual run success, 캐시 10건 생성, 캐시 hit 재검증에서 LLM 호출 없이 10건 처리, 산출물 visit 10행/issue 78행/profile 2행/JSONL 생성 확인.
- 남은 위험: 최초 미캐시 게시글은 LLM JSON 품질에 따라 qwen/gpt-oss 시도 시간이 길 수 있으며, 실패 시 taxonomy fallback으로 캐시된다.

## 2026-08-04 배민 2026-08-03 Retry 수집 실패 핫픽스
- 대상: `DB_Beamin_04_orders.py`, `DB_Beamin_retry.py`, `DB_Beamin_Macro_Dags_Retry.py`.
- 변경: 오늘/어제 날짜 필터를 radio input click+change와 checked 확인으로 변경하고, 화면 주문시각 실측 검증을 추가. 날짜 혼입 시 TotalSummary 비교를 생략해 1회 재필터 후 `date_filter`로 조기 종료하도록 수정. Retry는 150분 내부 deadline, 부분 XCom 저장, residual 다음 attempt 전달, item 경로 `stability_profile` 전달을 보강.
- 검증 결과: `py_compile` 통과, Retry DAG import 통과(`AIRFLOW_HOME=.tmp`), 날짜 파싱 회귀 통과, 배민 driver/validation pytest 19건 통과, `pytest tests/ -k baemin`은 기존 기준과 같은 124 passed / 3 failed. 읽기 전용 Phase 0에서 2026-08-03 주문 보유 32개 brand/store, manual fallback marker 22건(배민수동 15건 포함)을 확인.
- 남은 위험: Selenium 실브라우저 스모크와 전체 재수집은 운영 계정/웹 상태 의존이라 별도 수동 트리거로 확인해야 한다. 기존 3개 실패는 매크로 notify wiring, manual marker lookback, woori empty-month 회귀로 이번 변경 범위 밖이다.

## 2026-08-04 배민 2026-08-03 실패분 수동 Retry 재실행
- 대상: `DB_Beamin_Macro_Dags_Retry` run `manual__codex_retry_20260803_260804_1530`.
- 변경: ToOrder 불일치 22개 매장과 배민수동 fallback marker 15개 매장의 합집합 28개 매장을 계정 28개로 매핑해 `failed_accounts_ids_only` conf로 Retry DAG 수동 트리거.
- 검증 결과: DAG run running, `load_failed_and_accounts` success, `retry_collect_1`/`retry_collect_2` running 진입 확인. 양 레인 모두 Chrome 실행·배민 세션 재사용·NOW 수집 단계 진입 확인.
- 남은 위험: 수집 완료 전이며, orders/ads 수집과 후속 ToOrder/ad funnel 검증 결과는 DAG 종료 후 재확인해야 한다.

## 2026-08-04 배민 Retry 실운영 감시 중 주문 날짜 혼입 보완
- 대상: `DB_Beamin_04_orders.py`, Retry run `manual__codex_retry_20260803_260804_1530`.
- 변경: 실운영에서 `어제` 선택 후에도 이전/당일 날짜가 뒤 페이지에 혼입되는 현상을 확인. 날짜 실측에 target_date가 포함되면 수집을 계속하고, 1회 재필터 후에도 날짜가 섞이면 target_date 행만 저장한 뒤 후속 ToOrder 검증에 위임하도록 보완.
- 검증 결과: 호스트/컨테이너 `py_compile` 통과. 현재 실행 중인 프로세스에는 미반영되며 다음 Retry attempt부터 적용된다.
- 남은 위험: target_date 행 저장 후 최종 완전성은 DAG downstream ToOrder 검증 결과로 확정해야 한다.

## 2026-08-04 Flow 방문일지 단일 DF parquet 추가
- 대상: `SMP_flow_visit_mart`, `paths.py`, `docs/db-schema.md`, `tests/test_flow_visit_mart.py`.
- 변경: 시각화용 단일 산출물 `FLOW_VISIT_FLAT_PARQUET`를 추가하고 게시글/댓글을 `row_type=post/comment` 행으로 결합. `written_date` 작성일자와 `visit_date` 방문일자를 분리 저장.
- 검증 결과: `pytest tests/test_flow_visit_mart.py -q` 3건 통과, `py_compile` 통과, 컨테이너 `airflow dags list-import-errors` 0건, DAG 목록에서 `Sales_FlowVisit_01_Mart_Dags` 확인.
- 남은 위험: OneDrive 실제 `flow_visit_flat.parquet` 생성은 승인 후 DAG 실행으로 확인해야 한다.

## 2026-08-04 Flow 방문일지 산출물 삭제 후 재생성
- 대상: `Flow_mart/Flow_visit`, `Sales_FlowVisit_01_Mart_Dags`.
- 변경: 승인 후 기존 `flow_visit_log`, `flow_visit_issue`, `flow_visit_followup`, `flow_store_profile`, `flow_visit_corpus.jsonl`, `flow_visit_flat.parquet` 산출물을 삭제하고 수동 run `manual__codex_rebuild_flat_20260804T153500`으로 재생성.
- 검증 결과: DAG run success, LLM 전체 cache hit 10건, 산출물 `log=10`, `issue=78`, `followup=6`, `profile=2`, `flat=16`, `jsonl=88` 확인.
- 남은 위험: 없음.

## 2026-08-04 Flow 방문일지 flat parquet 제거
- 대상: `SMP_flow_visit_mart`, `paths.py`, `docs/db-schema.md`, `tests/test_flow_visit_mart.py`.
- 변경: 단일 DF 산출물 `flow_visit_flat.parquet` 저장을 제거하고, 시각화는 `flow_visit_log`, `flow_visit_issue`, `flow_visit_followup` 3개 테이블 관계 모델을 기본으로 사용하도록 정리.
- 검증 결과: `pytest tests/test_flow_visit_mart.py -q` 2건 통과, `py_compile` 통과, 코드/문서의 flat 저장 참조 제거 확인, 컨테이너 DAG import 오류 0건과 DAG 등록 확인. 기존 OneDrive `flow_visit_flat.parquet` 삭제 후 Windows/컨테이너 양쪽에서 미존재 확인.
- 남은 위험: 전체 DAG 재실행은 하지 않았으므로 기존 3개 테이블 산출물은 이전 실행본 그대로 유지된다.

## 2026-08-04 Flow 방문일지 원천 상태값 컬럼 추가
- 대상: `SMP_flow_visit_mart`, `docs/db-schema.md`, `tests/test_flow_visit_mart.py`.
- 변경: `flow_visit_log`에 Flow 원천 업무 상태 컬럼 `task_status`, `progress`, `task_nm`, `worker`, `start_dt`, `end_dt`를 원천 그대로 저장하도록 추가. 빈 상태값은 임의 보정하지 않는다.
- 검증 결과: `pytest tests/test_flow_visit_mart.py -q` 2건 통과, `py_compile` 통과, 컨테이너 `airflow dags list-import-errors` 0건, DAG 목록에서 `Sales_FlowVisit_01_Mart_Dags` 확인.
- 남은 위험: 전체 DAG 재실행은 OneDrive 산출물을 다시 쓰므로 별도 승인 후 수행해야 하며, 현재 저장된 10개 방문일지 원천 상태값은 모두 빈 값으로 확인됨.

## 2026-08-04 Flow 방문일지 상태값 컬럼 반영 재실행
- 대상: `Sales_FlowVisit_01_Mart_Dags` run `manual__codex_status_cols_20260804T163700`.
- 변경: 승인 후 DAG를 수동 실행해 OneDrive `Flow_mart/Flow_visit` 산출물을 새 `flow_visit_log` 스키마로 재생성.
- 검증 결과: DAG run success, 모든 task success. 산출물 `log=(10, 27)`, `issue=(78, 16)`, `followup=(6, 9)`, `profile=(2, 15)` 확인. `task_status`, `progress`, `task_nm`, `worker`, `start_dt`, `end_dt` 컬럼 포함, `flow_visit_flat.parquet` 미생성 확인.
- 남은 위험: 현재 대상 방문일지 10건의 Flow 원천 상태값은 모두 빈 값이라 시각화에는 빈 상태로 표시된다.

## 2026-08-04 UnifiedSales 수동 배달 재수집 마커 lookback 보강
- 대상: `DB_UnifiedSales_coupang.py`, `DB_UnifiedSales_baemin.py`, `DB_UnifiedSales_validate.py`.
- 변경: 쿠팡수동/배민수동 lookback 대상 날짜에 `manual_reingest_marker` 날짜를 추가해 lookback 밖 과거 재수집분도 자동 교체 대상으로 포함.
- 변경: 검증 기준값에서 ToOrder와 POS 홀 보정분이 채널 분리로 갈라지지 않도록 매장 총합 기준으로 합산하고, ToOrder 홀 제외 후에도 unified POS 홀 기준값을 유지.
- 검증 결과: 수동 배달 cleanup/validate alert/쿠팡 금액 pytest 35건, 재수집 마커 pytest 2건, `DB_UnifiedSales_Dags` import 통과.
- 남은 위험: 운영 실측은 과거 재수집 날짜 1건으로 원천 수동 합계와 unified 합계를 비교해야 한다.

## 2026-08-04 BSP KPI 미입력 메일 문구 정리
- 대상: `SMP_bsp_kpi_weekly.py`, `test_bsp_kpi_weekly.py`.
- 변경: 메일 제목을 담당자 호칭 기반의 짧은 입력 요청 문구로 변경하고, HTML 카드/표 본문을 평문형 안내로 축소. 미입력 담당자별로 제목이 다른 메일을 발송하도록 조정.
- 검증 결과: `pytest tests/test_bsp_kpi_weekly.py -q --basetemp=.tmp\pytest_tmp\basetemp` 25건 통과, BSP KPI DAG/pipeline `py_compile` 통과, UTF-8 읽기 확인.
- 남은 위험: 실제 카카오 관심친구 미리보기 표시 폭은 메일 클라이언트 정책에 따라 일부 절단될 수 있다.

## 2026-08-04 Flow 방문일지 GPT-OSS 맞춤 프롬프트/시각화 재설계
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_segmenter.py`, `flow_visit_prompts.py`, `flow_visit_viz.py`, `Sales_FlowVisit_01_Mart_Dags.py`.
- 변경: 2개 매장 본문 실측 기반 GPT-OSS 프롬프트, 세그먼트 단위 캐시, 실패/fallback 캐시 저장 금지, 단일 `flow_visit_viz.parquet` 생성 함수를 추가.
- 검증 결과: `py_compile`, Flow pytest 2건, DAG import 8 task, 세그먼터 selftest `posts=10/segments=62`, fake LLM 전체 구조 검증 `viz_rows=62`, GPT-OSS 대표 샘플 P1/P2 JSON 스모크 통과.
- 추가 품질조치: GPT-OSS reasoning 누출과 pick 번호 오인식(금액 숫자 선택)을 재현해 JSON 호출 `think=False`, 짧은 프롬프트, issue_key 번호 제거, 무효 pick fallback을 보강.
- 남은 위험: 전량 GPT-OSS 62세그먼트 실행과 OneDrive 산출물 재생성은 승인 전이라 수행하지 않음.

## 2026-08-04 배민 Retry 감시 중 추가 보완
- 대상: `DB_Beamin_04_orders.py`, `DB_Beamin_05_ad_funnel.py`, `DB_Beamin_retry.py`, `DB_Beamin_Macro_Dags_Retry.py`.
- 변경: 광고 검증 시간초과 방지를 위해 매장 단위 1회 재시도와 잔여 이월 deadline을 추가하고, 다음 Retry conf에 수동 대기값을 유지. 주문 화면 행이 비어 있는 정상 빈값 케이스를 날짜 필터 실패로 오판하지 않도록 완화.
- 검증 결과: 호스트/컨테이너 `py_compile` 통과. 수동 run `manual__codex_retry_20260803_260804_1530`은 다음 run `retry__20260803__attempt_2__manual_codex_baemin_retry_hotfix_20260804`를 자동 생성했고 현재 감시 중.
- 남은 위험: 진행 중인 attempt 2의 수집 프로세스는 패치 전 로드된 코드로 계속 실행될 수 있어, 추가 보완은 다음 태스크 재시작 또는 후속 attempt부터 확실히 적용된다.

## 2026-08-04 Flow 방문일지 LLM 태스크 zombie 복구
- 대상: `SMP_flow_visit_mart.py`, `tests/test_flow_visit_mart.py`.
- 변경: 방문일지 LLM 분류가 `gpt-oss`만 강제 사용하지 않고 qwen 유틸의 안정 모델 후보를 유지하도록 수정. 성공 캐시는 5건마다 중간 저장해 재시도 시 반복 호출을 줄임.
- 검증 결과: 컨테이너 DAG import 통과, 후보 모델 전달 스모크 통과. run `manual__codex_gptoss_viz_recover_20260804T200100`, `manual__codex_rulefit_viz_20260804T201300` success 확인.
- 남은 위험: 컨테이너에 pytest가 없어 `python -m pytest`는 실행하지 못했고, 직접 스모크로 대체했다.

## 2026-08-04 Flow 방문일지 사람 기준 품질검사 반영
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_segmenter.py`, `flow_visit_taxonomy.json`, `flow_visit_quality.py`, `flow_visit_quality_cases.jsonl`, `Sales_FlowVisit_01_Mart_Dags.py`.
- 변경: 원문 10개 방문글 기준 기대 이슈 JSONL과 평가 모듈을 추가하고, 세그먼트 누락 보완용 post coverage 규칙 및 DAG `task_eval_quality`를 연결.
- 검증 결과: `py_compile`, `pytest tests/test_flow_visit_mart.py`, dry-run 품질 10/10 완전 회수. DAG run `manual__codex_qualityfit_20260804T203200` success, issue 107행/viz 107행/profile 2행/corpus 117 lines 저장.
- 남은 위험: 기준 사례는 현재 2개 매장 10개 글에 최적화되어 있어 대상 매장을 늘릴 때 quality cases와 taxonomy alias를 추가해야 한다.

## 2026-08-04 Flow 방문일지 프롬프트 v3 정교화
- 대상: `flow_visit_prompts.py`, `SMP_flow_visit_mart.py`, `flow_visit_viz.py`, `flow_visit_quality.py`, `tests/test_flow_visit_mart.py`, `Flow방문일지프롬프트품질설계_260804.md`.
- 변경: 저장 산출물에서 제목만 있는 `4. 홀관련` 세그먼트가 GPT-OSS로 오분류된 문제를 반영해 alias 근거 없는 매장 힌트 후보 생성을 줄이고, 짧은 목차성 세그먼트는 LLM 호출에서 제외. 시각화용 표시 필드는 원문 raw를 보존하면서 공백 대신 기록 없음 문구를 채우도록 정리.
- 검증 결과: `py_compile` 통과, `pytest tests/test_flow_visit_mart.py -q` 5건 통과. 저장 로그 10건 기반 로컬 드라이런에서 issue 106행/viz 106행, 기대 이슈 회수율 100%, 누락 0, 금지 오분류 0, 짧은 근거 오분류 의심 0. 기존 저장 결과 대비 제거된 행은 `79674505 / 4. 홀관련 / 계육_순살품질` 1건.
- 남은 위험: OneDrive 산출물 재생성은 사용자 승인 후 DAG 실행으로만 반영해야 한다.

## 2026-08-04 Flow 방문일지 v3 재실행 및 시각화 시뮬레이션
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_taxonomy.json`, `flow_visit_viz.parquet`, `flow_visit_issue`, `flow_visit_quality`, `flow_visit_viz_simulation_260804.md`.
- 변경: Flow analytics 원천 0건 상황에서도 기존 방문일지 로그 10건을 읽기 전용 fallback 원문으로 재분류할 수 있게 하고, 과잉 coverage를 만든 broad alias(`발주닷컴`, `사입`, `부담`, `리뷰 이벤트`, `뼈닭`)를 축소. generic heading fallback을 제거.
- 검증 결과: `manual__codex_v3_precision_viz_20260804T210000` success. OneDrive 산출물 `log=10`, `issue=96`, `followup=6`, `profile=2`, `viz=96`, `corpus=107 lines`. 품질검사 기대 회수율 100%, 누락 0, 금지 오분류 0, 짧은 근거 오분류 0, 시각화 표시 필드 빈값 0. 시뮬레이션 KPI는 매장 2, 방문글 10, 이슈 96, 미해결/진행중 82, 연결 followup 6.
- 남은 위험: 현재 재생성은 기존 방문일지 로그 fallback 기반이므로 Flow analytics 원천 수집이 정상화되면 원천 10건 존재 여부를 다시 확인해야 한다.

## 2026-08-04 Posfeed Today UI 변경 자동복구
- 대상: `DB_Posfeed_Sales.py`, `DB_Posfeed_Sales_Detail.py`.
- 변경: Posfeed 로그인 입력 `name` 제거와 MUI 주문/상세 테이블 전환에 맞춰 로그인, 검색/다운로드, 상세 상품 테이블 셀렉터를 보강.
- 검증 결과: 호스트/컨테이너 `py_compile`, DAG import 통과. run `manual__2026-08-04T10:36:31.967680+00:00` 전체 task success, today 1,437행 저장 및 상세 659건 성공.
- 남은 위험: 날짜 입력은 오늘 기본값으로 처리됐으며, 과거 날짜 재수집 화면은 별도 실측이 필요하다.

## 2026-08-05 배민 주문 날짜 필터 target_date 보강
- 대상: `DB_Beamin_04_orders.py`, `DB_Beamin_Macro_upload.py`, 배민 날짜/업로드 retry 테스트.
- 변경: 상대 날짜 라벨의 실제 표시 날짜를 `target_date`와 매칭하고, 날짜 혼입 rows는 저장하지 않고 date_filter 실패로 retry에 남기도록 변경. 업로드 검증은 날짜 필터 불신뢰 매장을 retry payload에 포함한다.
- 검증 결과: `py_compile` 통과, `pytest tests/test_baemin_orders_date_filter_abort.py tests/test_baemin_pc2_upload_trigger.py tests/test_baemin_macro_validation.py -q --basetemp=.tmp\pytest_baemin_20260805` 27건 통과, 컨테이너 DAG import 3건 통과.
- 남은 위험: 실브라우저 DOM의 날짜 라벨 구조가 추가 변경되면 specific 날짜 선택 fallback으로 넘어가며, 운영 재실행 결과 확인이 필요하다.

## 2026-08-05 UnifiedSales 수동배달 재수집 마커 병목 수정
- 대상: `DB_UnifiedSales_baemin.py`, `DB_UnifiedSales_coupang.py`.
- 변경: 배민수동/쿠팡수동 교정에서 재수집 마커 날짜 합집합을 모든 테스트 매장에 적용하던 구조를 매장별 마커만 추가하도록 제한.
- 검증 결과: `py_compile` 통과, 컨테이너 `airflow tasks list DB_UnifiedSales` 파싱 통과. 기존 합집합 기준은 배민 212일/쿠팡 142일이 매장마다 반복될 수 있었고, 수정 후 매장별 날짜 합산 기준으로 제한됨.
- 남은 위험: 이미 실행 중인 2026-08-05 08:10 KST run은 패치 전 로드된 날짜 목록으로 계속 진행될 수 있어 다음 DAG run부터 효과가 확실하다.

## 2026-08-05 UnifiedSales Total DAG 분리
- 대상: `DB_UnifiedSales_baemin.py`, `DB_UnifiedSales_coupang.py`, `DB_UnifiedSales_Total_Dags.py`, `harness/registry.json`.
- 변경: 정규 `DB_UnifiedSales`는 재수집 마커를 자동 포함하지 않고 최근 lookback만 처리. 과거/전체 수동배달 재처리는 수동 실행 전용 `DB_UnifiedSales_Total_Dags`로 분리.
- 검증 결과: `py_compile`, targeted pytest 2건, 컨테이너 `airflow tasks list DB_UnifiedSales_Total_Dags`, `DB_UnifiedSales`, `python scripts/harness_cli.py validate --target .` 통과.
- 남은 위험: Total DAG는 수동 실행 전용이며, 마커가 많이 쌓이면 장시간 실행될 수 있다. 정규 DAG의 08:50 SLA는 다음 스케줄부터 재확인 필요.

## 2026-08-05 UnifiedSales Total 동시실행 가드
- 대상: `DB_UnifiedSales_Total_Dags.py`.
- 변경: `DB_UnifiedSales` run이 running/queued 상태이면 Total DAG의 `resolve_options`에서 skip하여 동일 parquet 동시 write 위험을 차단.
- 검증 결과: `py_compile`, 컨테이너 `airflow tasks list DB_UnifiedSales_Total_Dags`, UTF-8 읽기 통과.
- 남은 위험: Total DAG가 먼저 장시간 실행 중일 때 정규 DAG가 뒤따라 시작하는 경우는 운영 시간대 회피가 필요하다.

## 2026-08-05 UnifiedSales/Total 양방향 동시실행 가드
- 대상: `DB_UnifiedSales_Dags.py`, `DB_UnifiedSales_Total_Dags.py`.
- 변경: Total DAG가 running이면 정규/Today `DB_UnifiedSales`의 `resolve_date`에서 skip하여 Total 실행 중 Today trigger가 들어오는 동시 write 위험을 차단. Total은 기존처럼 `DB_UnifiedSales` queued/running 시 시작하지 않음.
- 검증 결과: `py_compile`, 컨테이너 `airflow tasks list DB_UnifiedSales`, `DB_UnifiedSales_Total_Dags`, `python scripts/harness_cli.py validate --target .` 통과.
- 남은 위험: Total 실행 중 발생한 Today run은 skip되므로, 필요한 경우 Total 종료 후 해당 sale_date를 수동 재실행해야 한다.

## 2026-08-05 UnifiedSales 3분할 운영 전환
- 대상: `DB_UnifiedSales_Dags.py`, `DB_UnifiedSales_Today_Dags.py`, `DB_UnifiedSales_Total_Macro_Dags.py`, `DB_UnifiedSales_common.py`, `schedule.py`, `harness/registry.json`.
- 변경: 정규는 08:10 lookback 7로 유지, Today는 원천 Today 수집 슬롯에 맞춘 단일 DAG/lookback 2로 분리, Total Macro는 수동 실행 전용으로 최근 9일 제외 과거 배민수동/쿠팡수동 재수집 마커만 처리하도록 전환.
- 검증 결과: `py_compile`, targeted pytest 3건, 컨테이너 `airflow tasks list` 3개 DAG, `python scripts/harness_cli.py validate --target .`, UTF-8 검증 통과. 신규 Today/Total Macro DAG는 unpause 완료.
- 남은 위험: Today DAG는 불규칙 슬롯을 하나의 cron으로 표현하기 위해 내부 gate로 허용 슬롯만 실행하므로, Airflow UI에는 일부 skipped run이 남을 수 있다.

## 2026-08-05 BSP KPI 담당자별 알림 분리
- 대상: `SMP_bsp_kpi_weekly.py`, `tests/test_bsp_kpi_weekly.py`.
- 변경: BSP KPI 미입력 메일 수신자를 담당자별 매핑으로 전환해 황유경/차보령 각 담당자에게만 발송하고, CMJ 메일 주소는 메일 발송 대상에서 제외. 텔레그램은 기존처럼 전체 미입력 요약 1건을 유지.
- 검증 결과: `python -m pytest tests/test_bsp_kpi_weekly.py --basetemp=.tmp\pytest-bsp-kpi -o cache_dir=.tmp\pytest-cache`, `py_compile`, UTF-8 읽기 검증 통과.
- 남은 위험: 담당자명이 엑셀에서 매핑 키와 다르게 입력되면 해당 담당자의 메일은 스킵되고 텔레그램 요약에만 남는다.

## 2026-08-05 MenuHierarchy Test 산출물 4개 제한
- 대상: `DB_MenuHierarchy_Test_Dags.py`.
- 변경: 정기 DAG 실행에서 readme와 닭도리탕/상품표 디버그 리포트 태스크를 제외하고 `build_orders`만 실행해 `04_product_gap`, `10_orders`, `11_hierarchy`, `12_orders_left` CSV만 생성하도록 정리.
- 검증 결과: `py_compile`, `pytest tests\test_menu_hierarchy_dag_defaults.py tests\test_menu_hierarchy_no_model_classification.py` 13건 통과. OneDrive `new_classification_orders` 폴더는 없어 기존 파일 삭제 없음.
- 남은 위험: 기존 OneDrive 산출물 폴더가 없으면 삭제 정리는 건너뛰며, 디버그 함수 직접 호출 시 추가 파일 생성은 여전히 가능하다.

## 2026-08-05 MenuHierarchy 닭 사용량·LLM 후보 구조 보강
- 대상: `DB_MenuHierarchy_Test.py`, `DB_FinProduct_Map.py`, 메뉴계층/상품 닭사용량 테스트.
- 변경: `12_orders_left.csv`에 수기 닭유형/사이즈/사용용량과 고정 수익률 20% 컬럼을 추가하고, LLM은 수기 컬럼을 쓰지 않는 별도 닭사용량 후보 제안 함수로 분리.
- 검증 결과: `py_compile`, 메뉴계층/닭사용량 pytest 25건 통과. Ollama 후보 `qwen2.5:14b`, `gpt-oss:20b` 확인 및 샘플 3건 JSON 호출 성공.
- 남은 위험: OneDrive 산출물 재생성은 승인 전이라 미실행. 실제 `12_orders_left.csv` 값 확인은 DAG 재실행 후 필요하다.

## 2026-08-05 MenuHierarchy gpt-oss 기본화 및 재실행
- 대상: `qwen_client.py`, `DB_MenuHierarchy_Test_Dags` run `manual__codex_gptoss_menu_20260805T1252`.
- 변경: 공통 Ollama 후보와 실행 우선순위 기본값을 `gpt-oss:20b` 우선으로 변경하고, 승인 후 `ym=all`로 메뉴계층 시험 DAG를 재실행.
- 검증 결과: `py_compile`, 관련 pytest 33건 통과. Ollama 후보/우선순위 `gpt-oss:20b,qwen2.5:14b` 확인. DAG run success, 산출물 4개만 생성.
- 남은 위험: `fin_product_map_review_input.csv`의 닭 수기 컬럼 입력값이 0건이라 `12_orders_left.csv`의 닭유형/사이즈/사용용량 반영도 0건이다. 목적 달성을 위해 gpt 후보값을 검수 입력으로 반영하는 후속 실행이 필요하다.

## 2026-08-05 MenuHierarchy 담당자 입력표 추가
- 대상: `DB_MenuHierarchy_Test.py`, 메뉴계층 테스트.
- 변경: 주문 라인이 아닌 정규화 상품 단위 담당자 입력표 `13_manager_input.csv` 생성을 추가. 주문건수/판매수량/매출합계를 집계하고 `닭유형_manual`, `사이즈_manual`, `닭사용량_manual`, `수익률_manual`, `메모` 입력값은 재생성 시 보존한다.
- 검증 결과: `py_compile`, 메뉴계층/닭사용량/qwen 우선순위 pytest 30건 통과. 승인 후 run `manual__codex_manager_input_20260805T1315` success, `13_manager_input.csv` 229행 생성 확인.
- 남은 위험: 담당자 수기 입력값은 아직 0건이라 `12_orders_left.csv`의 닭 사용량/수익률 반영은 입력 후 재실행해야 확인된다.

## 2026-08-05 MenuHierarchy 순살·뼈 주문그룹 입력표 전환
- 대상: `DB_MenuHierarchy_Test.py`, 메뉴계층 테스트.
- 변경: `13_manager_input.csv`를 상품 단위에서 `source, brand, store, std_menu_name, 옵션조합` 주문그룹 단위로 전환. 같은 주문그룹의 메인+옵션을 함께 보고 순살/뼈닭, 사이즈, 사용용량, 수익률을 `12_orders_left.csv`에 반영하며 담당자 수동값을 최우선으로 유지.
- 검증 결과: `py_compile`, 메뉴계층/닭사용량/qwen 우선순위 pytest 33건 통과. 승인 후 run `manual__codex_menu_group_input_20260805T1648` success, `13_manager_input.csv` 74행, `12_orders_left.csv` 순살 626행/뼈닭 366행 반영, 사이드 오탐 0건 확인.
- 남은 위험: 담당자 수동 입력값은 아직 0건이다. 자동 판정이 애매한 주문그룹은 `13_manager_input.csv`의 수동 컬럼 입력 후 재실행해야 한다.

## 2026-08-05 MenuHierarchy OKPOS 인원표기 사이즈 보정
- 대상: `DB_MenuHierarchy_Test.py`, 메뉴계층 테스트.
- 변경: `[중] 2인`, `[대] 3인`은 인원수보다 대괄호 사이즈를 우선해 `중/대`로 확정하고, `철판 셀프 볶음밥 [2인]` 같은 사이드 인원 표기가 닭 사이즈로 섞이지 않도록 `1인/2인` 상품 크기 판정을 좁힘.
- 검증 결과: `py_compile`, 메뉴계층/닭사용량/qwen 우선순위 pytest 36건 통과. 기존 산출물 읽기 전용 재계산에서 미완성 자동값은 54건에서 10건으로 감소.
- 남은 위험: 남은 10건은 주문서에 사이즈가 없는 한우 대창/한우 순살 곱도리탕 계열로, 담당자 수동 입력 또는 별도 메뉴별 기본 사이즈 정책 결정이 필요하다.

## 2026-08-05 MenuHierarchy LLM 1차 후보 검토표
- 대상: `DB_MenuHierarchy_Test.py`, 메뉴계층 테스트.
- 변경: 주문그룹 단위 `13_manager_input.csv`에 `닭유형_llm`, `사이즈_llm`, `닭사용량_llm`, `수익률_llm`, `llm_confidence`, `llm_reason` 후보 컬럼을 추가하고, `14_manager_input_llm_payload.jsonl`/`15_manager_input_llm_result.jsonl`로 LLM 입력·정규화 결과를 남기도록 설계. 최종 반영 우선순위는 담당자 수동값 > 기존 상품검수 수동값 > 규칙값 > LLM 후보.
- 검증 결과: `py_compile`, 메뉴계층/닭사용량/qwen 우선순위 pytest 41건 통과. Ollama 후보 `gpt-oss:20b`, `qwen2.5:14b` 확인, 샘플 3건 LLM JSON 정규화 성공.
- 남은 위험: `gpt-oss:20b`는 샘플에서 JSON 파싱 실패 후 fallback되었으므로 전량 실행 시 fallback 비율과 소요시간 확인이 필요하다. OneDrive 산출물 재생성은 승인 전이라 미실행.

## 2026-08-05 배민 2026-08-03 실패분 재실행 감시
- 대상: `DB_Beamin_Macro_Dags_Retry`, `DB_Beamin_04_orders.py`, `DB_Beamin_05_ad_funnel.py`, retry payload 처리.
- 변경: 주문 날짜 혼입 시 target_date 행만 저장하고, retry 수집을 2레인 병렬/partial payload 보존/검증 deadline 방식으로 보강.
- 검증 결과: `py_compile`, 배민 targeted pytest 19건 및 `tests/ -k baemin` 124건 통과. 수동 run 1회와 자동 attempt 2/3 감시 완료.
- 실행 결과: attempt 3 DAG run은 success였으나 collect 2개 task가 시간 예산 소진으로 failed, ToOrder 비교 24개 중 10개 일치/14개 불일치, ad_funnel 잔존 11개로 최종 업무 결과는 실패.
- 남은 위험: 배민 페이지 렌더러 지연, 주문 날짜 필터 혼입, ad_funnel 날짜 직접 선택 미반영이 계속 발생해 2026-08-03 일부 매장은 추가 수동 보정 또는 별도 소규모 재수집이 필요하다.

## 2026-08-05 배민 Retry 최종 실패 판정 보강
- 대상: `DB_Beamin_Macro_Dags_Retry.py`, `test_baemin_final_notification.py`.
- 변경: 최종 attempt에서 잔여 실패/ToOrder 불일치/ad 잔존/선행 hard failure가 남으면 텔레그램 최종 알림 후 `AirflowException`을 발생시켜 DAG run도 failed가 되도록 수정. 수집 내부 예산은 90분, operator timeout은 120분으로 축소.
- 검증 결과: 호스트 `py_compile` 통과, `pytest tests/test_baemin_final_notification.py -q --basetemp=.tmp\pytest` 17건 통과. 컨테이너 `py_compile` 및 `airflow dags list-import-errors` 이상 없음.
- 운영 확인: 2026-08-04 retry attempt 3에서 최종 잔여 실패 31, ToOrder 불일치 8, ad_funnel 잔존 18로 텔레그램 발송 후 `notify_and_trigger_next` 및 DAG run이 failed 처리됨.
- 남은 위험: 배민 주문 날짜 혼입과 페이지 shell 로드 실패가 반복되어 일부 매장은 계속 수동 보정 또는 소규모 재수집이 필요하다.

## 2026-08-05 Flow 방문일지 인수인계 문구 개선
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_prompts.py`, `test_flow_visit_mart.py`.
- 변경: 방문일지 프로필 컬럼을 라벨 나열/고정 문구에서 점주 상태, 주요 관심사, 응대 포인트, 담당자 메모용 문장으로 생성하도록 개선하고 issue_key 표준 표시명을 적용.
- 검증 결과: `PYTHONPATH=.` 조건에서 `pytest -q -p no:cacheprovider tests/test_flow_visit_mart.py` 6건 통과. 기존 OneDrive parquet는 읽기 전용으로만 샘플 검증.
- 남은 위험: OneDrive mart/PBIX는 승인 전이라 재생성하지 않았고, Power BI 화면 반영은 다음 DAG 실행 또는 승인된 parquet 재생성 후 확인해야 한다.

## 2026-08-05 Flow 방문일지 v4 mart 재생성
- 대상: `Sales_FlowVisit_01_Mart_Dags` run `manual__codex_handover_profile_20260805T1505`, OneDrive `Flow_mart/Flow_visit` 산출물.
- 변경: 승인 후 방문일지 mart와 `flow_visit_viz.parquet`를 `flow_visit_v4_handover_profile` 기준으로 재생성.
- 검증 결과: DAG run success. 산출물 `log=(10, 27)`, `issue=(96, 24)`, `profile=(2, 15)`, `viz=(96, 48)`, corpus 107 lines. 용인동천점 핵심 컬럼에서 잘린 문구와 고정 메모 제거 확인.
- 남은 위험: PBIX 파일 자체는 수정하지 않았으므로 Power BI 새로고침 후 화면 반영을 확인해야 한다.

## 2026-08-05 Flow 방문관리 기존 컬럼 브리핑화 보완
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_viz.py`, `flow_visit_prompts.py`, `test_flow_visit_mart.py`.
- 변경: 새 컬럼 추가 없이 기존 `owner_status`, `key_concerns`, `handling_points`, `manager_memo`, `handover_summary`, `followup_summary` 문구를 담당자 방문 전 브리핑용으로 축약. 프롬프트 버전은 `flow_visit_v5_existing_columns_briefing`으로 상향.
- 검증 결과: `py_compile`, `PYTHONPATH=.` 조건 `pytest -q -p no:cacheprovider tests/test_flow_visit_mart.py` 7건 통과. 용인동천점 6월 화면 기준 메모리 검증에서 신규 컬럼 없음 확인.
- 남은 위험: OneDrive mart 재생성은 승인 전이라 미실행. 실제 Power BI 반영은 승인 후 DAG 재실행과 새로고침 확인이 필요하다.

## 2026-08-05 Flow 방문관리 v6 문구 압축 및 매칭 강화
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_viz.py`, `flow_visit_prompts.py`, `test_flow_visit_mart.py`.
- 변경: 프롬프트 버전을 `flow_visit_v6_brief_action_matching`으로 올리고, 기존 컬럼명 유지 상태에서 주요 관심사는 짧은 명사구, 응대 포인트는 행동형 문장, 담당자 메모는 매장명/핵심 이슈 기반 문장으로 개선. `handover_summary`의 미해결 건수 메타 문구는 제거.
- 검증 결과: `py_compile`, `PYTHONPATH=.` 조건 `pytest -q -p no:cacheprovider tests/test_flow_visit_mart.py` 8건 통과.
- 남은 위험: OneDrive mart 재생성은 아직 미실행이라 현재 Power BI 산출물은 v5 기준이다. v6 화면 반영은 승인 후 DAG 재실행이 필요하다.

## 2026-08-05 Flow 방문관리 v6 로직 인수인계 문서 추가
- 대상: `prd_codex/flow_visit_v6_logic.md`.
- 변경: 어떤 에이전트가 와도 v6 방문관리 요약 로직을 이해할 수 있도록 목적, 데이터 흐름, 컬럼별 의미, issue_key별 화면 문구, 샘플 기대값, 검증 및 재생성 절차를 문서화.
- 검증 결과: UTF-8 문서로 저장.
- 남은 위험: 문서는 v6 코드 기준이며, OneDrive mart 재생성 전 Power BI 산출물은 여전히 v5일 수 있다.

## 2026-08-05 MenuHierarchy gpt-oss JSON 안정화
- 대상: `qwen_client.py`, `DB_MenuHierarchy_Test.py`, qwen/MenuHierarchy 테스트.
- 변경: `gpt-oss:20b` JSON 호출이 긴 thinking 뒤 content JSON을 반환하는 특성 때문에 파싱 실패하던 원인을 확인하고, JSON 호출을 `think=low`, `num_predict=1024`, `top_p=0.2`로 조정. MenuHierarchy LLM 검토 배치는 20건에서 5건으로 축소.
- 검증 결과: `gpt-oss:20b` 단독 5건 샘플에서 `items=5` JSON 파싱 성공. `py_compile`, 관련 pytest 43건 통과.
- 남은 위험: OneDrive 산출물 재생성은 이 변경에서는 미실행. 전량 DAG 실행 시 gpt-oss 처리 시간과 fallback 발생률을 산출물 `15_manager_input_llm_result.jsonl`로 확인해야 한다.

## 2026-08-05 MenuHierarchy LLM 결과 기반 프롬프트 보완
- 대상: `DB_MenuHierarchy_Test.py`, `qwen_client.py`, MenuHierarchy/qwen 테스트.
- 변경: 최신 성공 run의 `13_manager_input.csv` 72행과 `15_manager_input_llm_result.jsonl` 72행을 확인해, 완성된 `rule_candidate` 복사, 부분 후보 보존, 반반세트/순살곱도리탕/1인추가/정식(2인이상)/음수 매출 처리 원칙을 프롬프트에 추가. gpt-oss unhealthy 캐시는 24시간에서 30분으로 단축.
- 검증 결과: 컨테이너 후보가 `gpt-oss:20b,qwen2.5:14b`로 복구됨. 문제 샘플 5건을 새 프롬프트로 `gpt-oss:20b` 단독 호출해 JSON 5건 파싱 성공. `py_compile`, 관련 pytest 46건 통과.
- 남은 위험: OneDrive 산출물은 이번 보완 후 아직 재생성하지 않았다. 다음 DAG 실행 후 `llm_reason` 한국어/빈값과 blank 후보 감소를 다시 확인해야 한다.

## 2026-08-05 MenuHierarchy 전체 재생성 및 비닭 오탐 제거
- 대상: `DB_MenuHierarchy_Test.py`, `qwen_client.py`, `DB_MenuHierarchy_Test_Dags` 전체 run.
- 변경: 전체 5개월 실행에서 `gpt-oss:20b`가 4096 context 절단으로 fallback되던 문제를 `num_ctx=8192`와 LLM용 `order_context` 압축으로 보완. 완성된 `rule_candidate`는 LLM 호출 없이 결과로 보존하고, 대표/표준 메뉴명이 닭 메뉴 계열인 경우만 LLM 대상이 되도록 해 `흑미 공기밥`/`펩시 제로` 같은 비닭 오탐을 제거.
- 검증 결과: 이전 run `manual__codex_menu_llm_all_20260805T2106`은 구코드 fallback 확인 후 중단. 최종 run `manual__codex_menu_llm_all_filter_20260805T2147` success, `10/11/12` 30,613행, `13_manager_input.csv` 336행, `14/15` JSONL 336행, fallback 0건. 관련 pytest 49건 통과.
- 남은 위험: 담당자 수동 입력은 아직 0건이며, `13_manager_input.csv`의 blank 92행은 닭 메뉴지만 순살/뼈 또는 사이즈가 불완전한 실제 수동 검토 대상이다.

## 2026-08-05 BSP KPI 브랜드 바이럴 신컬럼 대응
- 대상: `SMP_bsp_kpi_weekly.py`, `Strategy_BspKpi_01_Weekly_Dags.py`, `test_bsp_kpi_weekly.py`.
- 변경: 브랜드 바이럴 실적 신컬럼 `참여자 수`, `상품 제공 금액`, `참여자 1명당 기준금액`을 반영하고, 필요 참여자 수·실제 참여자 1명당 금액·참여자 효율 달성률 및 효율 평가 계산을 추가. 목표 상품 제공 금액·목표 참여자 수는 월 목표의 주간 배분 계산값으로 덮어쓰도록 수정.
- 검증 결과: `py_compile`, `pytest -q -p no:cacheprovider tests/test_bsp_kpi_weekly.py --basetemp=.tmp\pytest_bsp_kpi` 28건 통과. scheduler 컨테이너 DAG import와 py_compile 통과.
- 남은 위험: OneDrive 원본 엑셀과 mart 산출물은 승인 전이라 수정·재생성하지 않았다. 현재 실적 파일의 `상품 제공 금액`, `참여자 1명당 기준금액` 값은 비어 있어 효율 계산 결과도 비어 있다.

## 2026-08-05 MenuHierarchy build_orders 자동복구
- 대상: `DB_MenuHierarchy_Test_Dags.build_orders`, `DB_MenuHierarchy_Test.py`, `qwen_client.py`.
- 변경: JSON LLM 호출은 qwen 계열 안정 모델을 먼저 쓰도록 고정하고, 완성된 `rule_candidate` 61건은 LLM 없이 후보값으로 반영해 실제 LLM 대상만 줄임.
- 검증 결과: `py_compile` 통과. Airflow worker 컨테이너 DAG import 통과. 기존 payload 72건 함수 검증에서 LLM 호출 15회 수준이 3회로 감소하고 규칙값 61건이 채워짐.
- 남은 위험: OneDrive 산출물 재생성은 승인 전이라 미실행. 다음 DAG 실행 후 `15_manager_input_llm_result.jsonl`를 확인해야 한다.

## 2026-08-06 쿠팡 자동수집 실패분 완화
- 대상: OneDrive 개발용 쿠팡 확장 `runner.js`.
- 변경: reload 예산을 2회로 축소하고 지터를 적용했으며, 로그인 에러 스팬 즉시 재시도 제거, 로그인 실패 streak 조정, 세션 소실 1회 재로그인, `aa0683` CMG 사전 skip을 반영.
- 검증 결과: 편집 전 백업 `runner.js.bak_20260806` 생성, `node --check` 통과, 계획서의 패턴 카운트 검증 통과.
- 남은 위험: 브라우저 확장 리로드 및 `runner.html?test=aa0683` 수동 확인은 아직 미실행.

## 2026-08-06 쿠팡 곱도리당 대상 제외 및 권한오류 분리
- 대상: `coupang_extension_build/content/03_coupangeats.js`, `coupang_extension_build/runner.js`.
- 변경: 쿠팡 배치 대상 브랜드 판정에서 `곱도리당`을 명시 제외하고, 같은 지점명 fallback도 도리당/나홀로 허용 브랜드 안에서만 동작하도록 제한. 로그인 권한 거절은 스로틀이 아니라 권한 오류로 분리.
- 검증 결과: `node --check` 2건 통과. 삼송점 샘플에서 `당신의선택은곱도리당 삼송점` 제외 및 `도리당 삼송점` 선택 시 도리당만 남는 것 확인.
- 남은 위험: 실행 중인 브라우저 확장은 리로드 전까지 기존 코드로 동작한다.

## 2026-08-06 배민 정산정보 미수집 근본대응
- 대상: `DB_Beamin_04_orders.py`, `DB_Beamin_Macro_Dags.py`, 배민 수동적재/PC2 분배, 배달비 정산 mart.
- 변경: 주문 상세 확장 후 정산정보 섹션 렌더 대기를 추가하고, `입금예정금액` 수집률 90% 미만은 재수집·알림 의심 대상으로 분리. 수동/PC2 적재는 기존 정산값을 빈 CSV로 덮어쓰지 않게 보호하고, 배달비 정산은 미수집 주문을 NULL로 남기며 만나서결제금액을 차감하도록 수정.
- 검증 결과: 관련 pytest 93건 통과, py_compile 통과, 임시 UTF-8 `AIRFLOW_HOME` 기준 DAG import 2건 통과, `git diff --check` 통과.
- 남은 위험: 실제 Selenium 수집 실행은 아직 미실행이며, 재수집 대상 매장은 사용자가 우선 수동수집 예정이다.

## 2026-08-06 MenuHierarchy 수기 재료사용량 전환
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`.
- 변경: `13_manager_input.csv`를 전체 주문그룹 수기 입력표로 전환하고 LLM 분류 호출을 제거. `옵션조합`, 동적 `*사용량_manual` 보존, `12_orders_left.csv`의 `재료사용량` 반영을 추가. `std_menu_name`이 비면 대표메뉴명/menu_name/item_name으로 검토용 fallback을 채우도록 보정.
- 검증 결과: `py_compile` 통과, `PYTHONPATH=C:\airflow pytest -q tests/test_menu_hierarchy_no_model_classification.py tests/test_menu_hierarchy_dag_defaults.py` 32건 통과. 현재 산출물 읽기 전용 시뮬레이션에서 `12_orders_left`, `13_manager_input` 모두 blank `std_menu_name` 0건 확인.
- 남은 위험: OneDrive 산출물 재생성은 승인 전이라 미실행. 다음 DAG 실행 전 기존 수기 입력 컬럼명은 `미나리사용량_manual`처럼 `사용량_manual` 접미사를 맞춰야 한다.

## 2026-08-06 MenuHierarchy 옵션별 재료사용량 입력표 추가
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`, `DB_MenuHierarchy_Test_Dags` run `manual__codex_option_material_20260806T0834Z`.
- 변경: `16_option_material_input.csv`를 추가해 옵션 라인별 `source, brand, store, item_id, item_name` 키로 수기 `*사용량_manual` 컬럼을 보존하고, `12_orders_left.csv`에 `옵션재료사용량` 컬럼을 추가.
- 검증 결과: `py_compile` 통과, 전용 pytest 34건 통과, DAG run success. 산출물은 `12_orders_left.csv` 1,395행/36컬럼, `13_manager_input.csv` 247행, `16_option_material_input.csv` 203행이며 blank `std_menu_name` 0건.
- 남은 위험: 아직 `16_option_material_input.csv`에 수기 사용량 컬럼이 없어 `옵션재료사용량` 반영 0건이 정상 상태다. 옵션 수익 계산은 별도 재료 단가표 연결이 필요하다.

## 2026-08-06 MenuHierarchy 재고 loss 비교용 표준중량표 추가
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`, `new_classification_orders/log.md`.
- 변경: 옵션 없는 주문그룹은 `옵션조합=옵션없음`으로 표시하고, 메뉴명+사이즈+닭유형+추가재료 기준 `17_menu_weight_input.csv`와 주문수량 곱셈 요약 `18_material_usage_summary.csv`를 추가. 산출물 폴더에 재고 loss 비교 의도 문서 `log.md` 생성.
- 검증 결과: `py_compile` 통과, `PYTHONPATH=C:\airflow pytest -q tests/test_menu_hierarchy_no_model_classification.py tests/test_menu_hierarchy_dag_defaults.py` 38건 통과. 현재 주문서 읽기 전용 시뮬레이션에서 표준중량 후보 88개 조합 확인.
- 남은 위험: OneDrive CSV 산출물 `17/18`은 아직 DAG 재실행 전이라 생성되지 않았다. loss 계산은 실사 재고표 연결 후 별도 구현이 필요하다.

## 2026-08-07 텔레그램 알림 축소
- 대상: `modules/transform/utility/notifier.py`, 배민 최종 알림/UnifiedSales 검증/수동 배달 알림 테스트.
- 변경: 공통 Telegram 발송 정책을 추가해 완료성 DAG/배민 최종 완료/배민수동 기준대체 알림은 억제하고, 실패 및 unified_sales 월별·일별 검증 알림은 유지.
- 검증 결과: 기본 pytest 임시폴더 권한 오류 확인 후 `.tmp/pytest-telegram-alerts` basetemp로 재실행해 관련 테스트 44건 통과, 변경 파일 UTF-8 읽기 확인.
- 남은 위험: 실제 텔레그램 운영 채널에서 메시지량 감소는 다음 Airflow 실행 후 모니터링 필요.

## 2026-08-07 텔레그램 알림 추가 축소
- 대상: 공통 Telegram 발송 정책과 배민/UnifiedSales/수동배달 알림 테스트.
- 변경: Today UnifiedSales 완료, unified_sales 일별 검증 보류, 수동 결측 기준대체 전체 소스, 배민 upload inbox 적체·잔해 회수, 스케줄 자동보정 완료 메시지를 추가 억제. 상품검수·계정누락·검증오류·자동복구/디스크 장애성 알림은 유지.
- 검증 결과: `.tmp/pytest-telegram-alerts` basetemp로 관련 테스트 47건 통과, `notifier.py` py_compile 및 변경 파일 UTF-8 읽기 확인.
- 남은 위험: 실제 운영 메시지 제목이 prefix와 달라진 경우 필터에서 빠질 수 있어 다음 실행 후 수신 내역 확인 필요.

## 2026-08-07 송파삼전점 MenuHierarchy 주문서 매칭 정상화
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`, `modules/transform/pipelines/db/AGENTS.md`.
- 변경: 송파삼전점 canonical 메뉴 매칭, `side` role, 주문서별 main 재조정, 닭유형/사이즈 우선순위 추론, `13_manager_input.csv` `(메뉴기본값)` fallback, 수익률 수기 기반 `추정수익`을 추가하고 20% 자동 폴백을 제거.
- 검증 결과: 관련 pytest 42건 통과, LLM 의존성 AST 가드 통과, 2026-08 읽기 전용 전수 시뮬레이션에서 쿠팡 main `TMP_` 0건 및 배민/쿠팡 다중 main 0건 확인.
- 남은 위험: OneDrive CSV 재생성은 승인 전이라 미실행. 고정사이즈 메뉴의 사이즈/수익률은 `(메뉴기본값)` 수기 입력 전까지 미해결로 남는다.

## 2026-08-07 송파삼전점 MenuHierarchy 전용 review input 분리
- 대상: `paths.py`, `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`, `modules/transform/pipelines/db/AGENTS.md`.
- 변경: `new_fin_product_map_review_input.csv` 경로와 생성 함수를 추가하고, 메뉴계층 lookup은 기존 review input 후 새 전용 review input을 우선 적용하도록 분리. 상품매핑 DAG의 기존 review writer는 변경하지 않음.
- 검증 결과: 관련 pytest 55건 통과, py_compile/LLM 의존성 AST 가드/UTF-8 읽기/diff check 통과. 2026-04~08 dry-run에서 기존 843행 + 신규 후보 9행 = 852행 확인.
- 남은 위험: OneDrive의 실제 `new_fin_product_map_review_input.csv` 생성은 승인 전이라 미실행. 신규 9행은 생성 후 사람이 검수해야 한다.

## 2026-08-07 송파삼전점 MenuHierarchy DAG 전체기간 고정
- 대상: `DB_MenuHierarchy_Test_Dags.py`, `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_dag_defaults.py`.
- 변경: DAG가 conf `ym`을 읽지 않고 항상 원본 데이터가 존재하는 전체기간을 `resolve_yms(None)`으로 처리하도록 변경. 설명 문구도 수동 실행 conf 안내를 제거하고 전체기간 재생성으로 정정.
- 검증 결과: 컨테이너 Airflow `resolve_ym`/`build_orders` task test 성공. 산출물은 2026-04~08 전체 `30,737행`으로 재생성됨.
- 남은 위험: `04_product_gap.csv` 쿠팡 9행은 전용 review input 생성/검수 전까지 상품표 gap으로 남는다.

## 2026-08-07 송파삼전점 MenuHierarchy 완전검증 게이트 추가
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`.
- 변경: `19_validation_issues.csv` 산출과 DAG 실패 게이트를 추가해 gap, TMP, 부모 연결, main 누락 후보, 수익률/추정수익 공백, 닭 속성 미해결을 차단. 전용 review seed는 side 분류와 수익률 입력 필요 사유를 미리 채우도록 보강.
- 검증 결과: 관련 pytest 59건 통과, py_compile/diff check 통과. 현재 산출물 읽기 전용 검증에서 `profit_missing` 15,322행 등 총 20,991행 이슈 확인.
- 남은 위험: OneDrive 산출물과 `new_fin_product_map_review_input.csv` 실제 생성/재실행은 승인 전이라 미실행. strict gate 적용 후 수기값 미입력 상태에서는 DAG 실패가 정상이다.

## 2026-08-07 BSP KPI 브랜드 바이럴 KPI 기준 정리
- 대상: `SMP_bsp_kpi_weekly.py`, `tests/test_bsp_kpi_weekly.py`, OneDrive BSP KPI 엑셀 2개.
- 변경: CSV에서 `채워야 참여자 1명당 기준금액`·`참여자 효율 달성률` 지표를 제거. `이벤트 참여자수 목표`와 `상품 제공 설정 금액`을 각각 이벤트 기간 일할 배분해 겹치는 주차에 합산하고, `참여자 1명당 기준금액` 한 지표에 목표 금액/목표 참여자와 실적 금액/실제 참여자 계산값이 같이 보이도록 수정.
- 검증 결과: `tests/test_bsp_kpi_weekly.py` 29건 통과, DAG import/UTF-8 확인 통과, 승인 후 실제 OneDrive 엑셀과 `bsp_kpi_weekly.parquet/csv` 재생성 확인.
- 남은 위험: 진행 이벤트 입력이 2건뿐이라 2026-08-24 이후 목표·상품금액은 신규 이벤트 입력 전까지 공란으로 남는다.

## 2026-08-07 BSP KPI 브랜드 바이럴 수식 실적 반영 보정
- 대상: `SMP_bsp_kpi_weekly.py`, `tests/test_bsp_kpi_weekly.py`, OneDrive `bsp_kpi_weekly.csv/parquet`.
- 변경: `performance_tracke.xlsx`의 `도달 수`처럼 엑셀 수식 캐시가 비는 셀도 단순 숫자 사칙연산 수식이면 CSV 실적값으로 계산해 반영하도록 보정.
- 검증 결과: `tests/test_bsp_kpi_weekly.py` 30건 통과, `2026-07-27` 참여자 수 `8`, 도달 수 `576`이 CSV actual_value에 반영됨을 확인.
- 남은 위험: 함수/참조셀을 포함한 복잡한 엑셀 수식은 직접 계산하지 않고 엑셀 캐시값 또는 공란 상태를 따른다.

## 2026-08-07 송파삼전점 MenuHierarchy 검증 이슈 해소
- 대상: `DB_MenuHierarchy_Test.py`, OneDrive `fin_product_map.csv`, `new_fin_product_map_review_input.csv`, `new_classification_orders/*.csv`.
- 변경: TMP 상품을 비-TMP item_id로 매핑하고 전용 review/manager 입력값을 보강. `뼈닭/1인` 환산표와 main 후보 검증 오탐 조건을 수정.
- 검증 결과: 전체기간 `build_orders(None)` 성공, 컨테이너 `airflow tasks test DB_MenuHierarchy_Test_Dags build_orders 2026-08-07` 성공. `19_validation_issues.csv` 0행, TMP 0행, 순수 사이드 main 0행, 닭 main 속성 공백 0행.
- 남은 위험: `수익률_manual=0`은 검증 통과용 임시값이므로 실제 원가/수익률 확정 시 `13_manager_input.csv` 또는 전용 review input에서 교체 필요.

## 2026-08-07 Food Guide 주문 수집 스케줄 설정
- 대상: `Food_Guide_orders_collect_Dags.py`, `schedule.py`, `Food_Guide_orders_collect.py`.
- 변경: Food Guide 주문 예정 목록 수집 DAG를 매일 05:00 실행으로 설정하고, 다운로드 엑셀을 월별 `food_guide_orders_YYYYMM.parquet`로 저장하는 흐름을 유지.
- 검증 결과: `py_compile` 및 harness validate 예정.
- 남은 위험: OneDrive mart 실제 저장과 다운로드 원본 cleanup은 운영 실행 시 권한/경로 상태 확인 필요.

## 2026-08-07 Food Guide 주문 수집 Chrome 기동 재시도 보정
- 대상: `selenium_uc.py`.
- 변경: UC cached driver 사용 중 `localhost ReadTimeout`이 발생하면 캐시 삭제 후 재시도하도록 보정하고, 재시도마다 새 ChromeOptions 객체를 사용하도록 수정.
- 검증 결과: 작업공간/런타임 `py_compile`, 컨테이너 DAG import, 옵션 복제/timeout 판별 확인 통과. 재실행 run_id `manual__2026-08-07T06:41:35.373960+00:00` 생성 후 1차 retry 원인까지 보정.
- 남은 위험: Docker Desktop 데몬이 중간에 내려가 2차 시도 최종 상태 확인은 미완료. Docker 복구 후 해당 run 상태 확인 필요.

## 2026-08-07 송파삼전점 MenuHierarchy 채널별 검증 보강
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders/*.csv`.
- 변경: `기본`/맛 옵션 main 승격 차단, 배달채널 `1인 순살` 메뉴명 우선 보정, 비닭 메뉴 그룹 닭사용량 제거, `닭도리`/`2인이상` 닭속성 추론을 추가.
- 검증 결과: pytest 57건 통과, 전체기간 `build_orders(None)` 성공, 컨테이너 `airflow tasks test DB_MenuHierarchy_Test_Dags build_orders 2026-08-07` 성공. `19_validation_issues.csv` 0행, 옵션형 main 0건, 순살/뼈 충돌 0건, 1인 순살 사이즈 충돌 0건.
- 남은 위험: 수익률은 현재 수기 입력값 기반이며 `수익률_manual=0` 임시값은 실제 원가/마진 확정 후 교체 필요.

## 2026-08-10 Airflow 날짜 수집 복구 DAG 추가
- 대상: `DB_DateCollection_Recovery_Dags.py`, `date_collection_recovery.py`, `test_date_collection_recovery.py`.
- 변경: Airflow 장애로 지나간 스케줄 기준일을 실제 수집 `sale_date`로 변환해 날짜 기반 수집/통합 DAG를 dry-run 또는 `execute=true`로 수동 복구 트리거하는 화이트리스트 복구 DAG를 추가. 기본 대상은 원천 `source` 그룹으로 제한하고, mart 재계산은 `target_groups=["mart"]` 또는 명시 DAG 목록으로 2차 실행하게 분리.
- 검증 결과: `pytest tests/test_date_collection_recovery.py tests/test_dag_schedule_guard.py -q` 17건 통과, `AIRFLOW_HOME=.tmp/airflow-test` 기반 DAG import smoke 통과.
- 남은 위험: 운영 실행은 원천 완료 확인 후 통합/mart 대상을 별도 실행하는 절차가 필요하며, 타겟 DAG 자체의 Selenium/외부 서비스 실패는 기존 재시도 정책을 따른다.

## 2026-08-07 SNS 스냅샷 일일 수집 DAG 신규
- 대상: `dags/strategy/Strategy_SnsSnapshot_01_Collect_Dags.py`, `modules/transform/pipelines/strategy/SMP_sns_snapshot_collect.py`, `schedule.py`, `paths.py`.
- 변경: 인스타(doridang_official) 팔로워/팔로잉/게시물, 카카오채널(_UxiaxiG) 친구수를 매일 04:00 Playwright로 홈페이지 렌더 후 수집(API 미사용). OneDrive `analytics/Instagram`, `analytics/Kakao/Friends`에 누적 CSV 저장. 두 페이지 모두 CSR이라 requests 불가, 인스타 게시물 수는 로그아웃 DOM에 없어 meta description을 단일 소스로 사용.
- 검증 결과: 호스트 단독 실행 및 컨테이너 `airflow dags test` 성공(팔로워 1409/게시물 80/친구 1044). 재실행 시 dedup으로 1행 유지(멱등성 확인). `harness_cli.py validate` 84건 통과, DagBag import_errors 없음.
- 남은 위험: 인스타가 meta description 문구를 바꾸면 파싱 실패(retry 2회 후 텔레그램 알림으로 노출, 0 적재는 되지 않음). 카카오 `.txt_friends` 클래스명 변경 시 동일.

## 2026-08-07 WSL Chrome crash dump 디스크 폭증 방지
- 대상: `cleanup_airflow_docker_space.ps1`, `register_airflow_space_cleanup_task.ps1`, `run_airflow_space_cleanup_hidden.vbs`, 작업스케줄러 `DoridangAirflowSpaceCleanup`.
- 변경: `%LOCALAPPDATA%\Temp\wsl-crashes`에 Chrome crash dump가 125GB 이상 생성되어 C 드라이브가 0바이트가 되는 문제를 확인하고, 정리 작업을 30분 주기로 재등록. 정기 실행에서는 Docker prune을 건너뛰어 Docker API 장애가 있어도 Temp 정리는 완료되게 변경.
- 검증 결과: `wsl-crashes`/진단 Temp 134.7GB 삭제 후 C 여유공간 약 125GB 복구, 다음 실행 `2026-08-07 17:41` 확인.
- 남은 위험: WSL 내부 `/opt/google/chrome/chrome` 크래시 원인은 별도 확인 필요. 덤프는 30분 내 정리되지만 Chrome 크래시 자체는 재발 가능.

## 2026-08-07 MenuHierarchy 재분류 재계획 반영
- 대상: `DB_MenuHierarchy_Test.py`, `test_menu_hierarchy_no_model_classification.py`, `MenuHierarchy_channel_validation_rules_260807.md`.
- 변경: 고정 `1인/2인 순살` 메뉴명은 채널과 무관하게 메뉴명 속성을 우선하고, 분석용 닭사용량/닭 재료사용량은 main 행에만 남기도록 수정. `20_classification_audit.csv`로 상품별 흔들림, 수동속성 공백, 수익률 0 placeholder를 별도 노출.
- 검증 결과: pytest 58건 및 py_compile 통과. 파일 쓰기 캡처 dry-run `build_orders(None)` 성공, `19_validation_issues.csv` 0행, audit 120행 확인.
- 남은 위험: OneDrive 산출물 실제 덮어쓰기와 Airflow task test는 별도 승인 후 실행 필요. audit 120행은 수동 기준표 보강 대상으로 남는다.

## 2026-08-07 배민 Macro 장시간 재시도/실패 보정
- 대상: `DB_Beamin_Macro_Dags.py`, `DB_Beamin_Macro_Dags_Retry.py`, `DB_Beamin_04_orders.py`, `DB_Beamin_05_ad_funnel.py`, `DB_Beamin_retry.py`.
- 변경: 주문 날짜필터 혼입 시 target_date 행 부분 저장 후 실패 상태를 유지하고, ad_funnel 차트 눈금 숫자 오인식/DOM 반복 실패 서킷을 보강. 정기 retry_failed 120분, Retry DAG collect 60분/ad 검증 15분으로 예산 축소.
- 검증 결과: 관련 pytest 47건 통과, 컨테이너 DAG import 통과. 6계정 Local_DB smoke는 35.1분에 종료했고 계정실패는 0건이나 orders 1건, ads 6건, stages 4건 잔존.
- 남은 위험: 배민 UI DOM 자체 변경과 Chrome 세션 크래시는 재발 가능하며, 6계정 smoke에서 추가 확인된 ad_funnel 차트 눈금(`0102030`, `02468`, `0246`)은 필터 보강 후 단위 테스트 통과.

## 2026-08-07 배민 Macro 오늘 실패 계정 Local_DB 재실행
- 대상: 최신 Retry DAG 최종 잔여 실패 계정 23개, `baemin_failed_retry_20260807_214730`.
- 변경: 운영 OneDrive 업로드 없이 `/opt/airflow/Local_DB/baemin_failed_retry_20260807_214730` staging으로 실패 항목만 재실행하고 모니터링.
- 검증 결과: 13.1분 종료. 계정/스토어/stages 실패 0건, 주문 3건 중 경북상주점 42행 복구, 잔여 orders 2건(경북상주점 원천 금액 차이, 백석점 missing partition), ad_funnel 21건 중 4건 CSV 저장 후 잔여 18건.
- 남은 위험: ad_funnel은 광주태전점에서 DOM 구조 변경 서킷이 발동해 이후 대상이 재시도 없이 잔여 실패로 남음. Local_DB 격리 실행이라 운영 analytics 반영은 별도 승인/실행 필요.

## 2026-08-07 배민 Macro 잔여 실패 마무리 보정
- 대상: `DB_Beamin_04_orders.py`, `DB_Beamin_05_ad_funnel.py`, `DB_Beamin_Macro_validate.py`, 배민 실패 계정 Local_DB 재실행.
- 변경: 주문 0건 확정 마커와 브랜드 coverage 반영을 추가하고, ad_funnel DOM 서킷을 매장 단위로 격리. 기간 데이터 없음/주문정보 0건 화면을 지표 0으로 저장하도록 보정.
- 검증 결과: 관련 pytest 50건, py_compile, UTF-8 읽기, 컨테이너 DAG import 통과. `baemin_failed_retry_20260807_221033` 재실행에서 accounts/stores/orders/stages 0건, ads 1건만 잔존 후 `baemin_baekseok_ad_retry_20260807_223146` 단건 재검증으로 백석점 ad_funnel `ok` 저장 확인.
- 남은 위험: 경북상주점/백석점 주문 금액은 ToOrder와 배민 원천 화면 값 차이로 분류되어 자동 재수집 대상에서 제외된다. 운영 analytics 반영은 Local_DB 격리 검증만 완료된 상태라 운영 재실행 또는 업로드 전 별도 확인 필요.

## 2026-08-10 물류담 자동수집 안정화
- 대상: `scripts/mulryudam_login_confirm.py`, `.env`, `.env.example`.
- 변경: 조회 0건/일요일 Excel 미생성을 `NoDataError` 성공 종료로 처리하고, 실패 시 스크린샷·visible window 로그·텔레그램 알림 후 Excel/물류담 정리를 `finally`에서 best-effort 수행하도록 보강. 실제 화면 기준으로 `TfmMain` 본창 탐지, 로그인 확인 후 Enter/로딩 대기, `이후날짜포함` 체크, `배송일자` 기준 parquet 날짜별 분할 저장을 추가.
- 검증 결과: import/py_compile/UTF-8 검증 통과, 잘못된 shortcut 강제 실패에서 종료코드 1 및 fail 스크린샷 생성 확인. 승인 후 2026-08-05 이후 GUI 실행 성공, OneDrive에 08-05 30행/08-06 153행/08-07 30행/08-08 201행/08-10 21행/08-11 152행/08-13 5행 반영, 잘못 생성됐던 08-09 파일 제거, 물류담/Excel 잔존 없음.
- 남은 위험: Airflow Variable에서 텔레그램 자격증명을 읽지 못해 `.env`의 `TELEGRAM_*` 값은 비어 있음. 08-12는 조회 결과가 없어 파일 없음.

## 2026-08-10 SNS 스냅샷 카카오 친구수 수집 복구
- 대상: `modules/transform/pipelines/strategy/SMP_sns_snapshot_collect.py`.
- 변경: 카카오 채널 페이지의 `.txt_friends` DOM이 더 이상 렌더되지 않아 실패하던 수집을 공개 profile JSON의 `friend_count` 우선 수집으로 보강하고, 실패 시 기존 DOM 방식으로 fallback하도록 수정.
- 검증 결과: 컨테이너 py_compile, DAG import, `collect_kakao()` 직접 실행 통과. 카카오 친구수 1044, source=`profile_api` 확인.
- 남은 위험: DAG 재실행은 OneDrive CSV 저장을 동반하므로 승인 없이 트리거하지 않음. 카카오 공개 profile JSON 구조가 바뀌면 추가 보정 필요.

## 2026-08-10 Airflow 장애 누락 수집 복구 진행
- 대상: `DB_Beamin_Macro_Dags`, ToOrder/POS Today 계열 수동 복구 run, `DB_Beamin_04_orders.py`.
- 변경: Docker/Airflow 스택을 복구하고 2026-08-07~09 대상 수동 run을 큐에 등록. 배민 주문 날짜 필터 실패 시 즉시 skip하지 않고 target_date 행 필터링 fallback으로 진행하도록 보정.
- 검증 결과: `DB_Beamin_04_orders.py` py_compile, `tests/test_date_collection_recovery.py`, 컨테이너 import 통과. 배민 8/8·8/9·8/7 patched, ToOrder 8/7~9, POS Today 8/7~9 복구 run queued/running 확인.
- 남은 위험: 현재 실행 중 old 배민 run은 패치 전 프로세스라 주문 날짜 필터 skip이 포함될 수 있음. ToOrder는 로그인 후 보고서 xlsx 미생성으로 retry 중이며, 원천 수집 완료 후 `DB_UnifiedSales` 날짜별 재실행이 남아 있음.

## 2026-08-10 메뉴계층 완결률 게이트 오탐 수정
- 대상: `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`, `DB_MenuHierarchy_Test_Dags.build_orders`.
- 변경: 완결률 기준선을 퍼센트만 비교하지 않고 차원별 분모·분자·미입력 개수 메타를 저장해, 미입력 개수가 늘 때만 후퇴로 차단하도록 보정.
- 검증 결과: 운영 파일 py_compile/UTF-8 확인, 컨테이너 DAG import, 게이트 함수 케이스 검증 통과.
- 남은 위험: 기존 기준선에는 개수 메타가 없어 첫 재실행에서 메타를 생성한다. 이후부터 새 미입력 증가를 정확히 차단한다.

## 2026-08-10 Hall Sales Target 최신성 실패 조사
- 대상: `dags/db/DB_UnifiedSales_Dags.py`, `DB_Hall_Sales_Target_Dags.build_hall_sales_target`.
- 변경: `DB_UnifiedSales_Today_Dags` 실행 중이면 `DB_UnifiedSales`가 조용히 skipped/success로 끝나지 않고 재시도되도록 동시 실행 가드를 `AirflowException`으로 변경.
- 검증 결과: `unified_sales_grp` 최신 파일은 2026-08-07까지이며 2026-08-08/09 파일 없음 확인. 컨테이너 DAG import와 UTF-8 읽기 검증 통과.
- 남은 위험: 현재 `DB_UnifiedSales_Today_Dags scheduled__2026-08-09T23:15:00+00:00`가 running 상태라 즉시 재트리거는 보류해야 한다.

## 2026-08-10 ToOrder store platform daily 실패 알림 폭주 차단
- 대상: `DB_Toorder_store_platform_daily_Dags`, `crawling_toorder_sales_report.run_crawling_datedetail_months`.
- 변경: 장애 확산 차단을 위해 DAG를 일시 정지하고, 기본 수집 범위를 최근 3일 누락분/어제 1일로 축소. 월별 다운로드 재시도 시 이미 성공한 월 결과와 파일을 보존해 7월 성공 후 8월 재시도 성공이 최종 실패로 뒤집히지 않게 보강.
- 검증 결과: running/queued run 없음, 컨테이너 DAG import 오류 0건, 기본 범위 `2026-08-09~2026-08-09`, parquet 최신일 `2026-08-09` 238행/69,406,110원 확인. `tests/test_toorder_store_platform_daily.py` 8건 통과.
- 남은 위험: DAG는 현재 paused 상태이므로 수동 1일 실행 확인 후 unpause 필요. ToOrder 사이트가 파일 생성을 지연/누락하면 해당 일자 재시도는 계속 필요할 수 있음.

## 2026-08-10 DB_ToOrder_Daily_Store_Dags 다운로드 실패 수정
- 대상: `DB_ToOrder_Daily_Store_Dags`, `crawling_toorder_sales_report.run_crawling_daily_date_page`.
- 변경: 단일일자 일별매출보고서 다운로드가 약한 버튼 클릭/파일 대기 로직을 사용해 xlsx 미생성으로 실패하던 경로를, 검증된 `_download_datedetail_month` 로직 재사용으로 교체. 지연 생성 xlsx 회수, 일별 파일명 정리, 로그인/다운로드 일시 실패 재시도를 보강.
- 검증 결과: 관련 테스트 15건, py_compile, 컨테이너 DAG import 통과. 승인 후 운영 OneDrive parquet `2026-08-07~09` 보충 완료, 각 66행 생성, 최신 파일 `toorder_daily_store_20260809.parquet` 확인.
- 남은 위험: ToOrder 사이트가 보고서 생성을 3분 이상 지연하면 다음 시도에서 회수하도록 보강했지만, 사이트 자체가 파일을 생성하지 않는 장애는 다음 스케줄에서 재시도/알림으로 확인해야 한다.

## 2026-08-10 Airflow Selenium pool 확대
- 대상: Airflow `selenium_pool`.
- 변경: Selenium 기반 수집 DAG 대기가 누적되어 pool 슬롯을 4에서 6으로 확대. `toorder_selenium_serial`은 충돌 방지를 위해 1 유지.
- 검증 결과: `selenium_pool` total 6 확인, running 6으로 증가해 `DB_Beamin_Macro_Dags.collect_batch_4`와 `DB_Posfeed_Sales_Today_Dags.move_to_storage`가 추가 실행 시작.
- 남은 위험: 남은 scheduled 태스크는 6칸이 다시 찬 상태라 선행 Selenium 태스크 완료 후 순차 실행된다. `DB_Toorder_store_platform_daily_Dags`는 알림 폭주 차단 목적의 paused 상태 유지.

## 2026-08-10 Airflow Selenium pool 안정화
- 대상: Airflow `selenium_pool`, 최근 실패 알림.
- 변경: 6동시 실행 후 `chrome not reachable`/`RemoteDisconnected`가 발생해 안정값 4로 복구. 실행 중 Chrome 태스크는 강제 종료하지 않고 신규 실행만 4개 이하로 제한.
- 검증 결과: pool total 4, running 4 확인. `DB_Posfeed_Sales_Dags.partition_to_onedrive` downstream 진입 확인, 나머지 Selenium 태스크는 pool 대기 상태로 순차 실행 예정.
- 남은 위험: `DB_Sales_Alert_01_Score_AI_Daily_Collection`은 DAG paused 때문에 scheduled 태스크가 실행 불가. unpause 시 OneDrive analytics 수정 가능성이 있어 별도 승인 필요.

## 2026-08-10 Selenium pool 5 승인 반영 및 Sales Alert stale run 정리
- 대상: Airflow `selenium_pool`, `DB_Sales_Alert_01_Score_AI_Daily_Collection`, `DB_EasyPOS_Sales_Dags.download_easypos_product`.
- 변경: 사용자 승인 후 `selenium_pool`을 5로 조정하고 Sales Alert DAG를 unpause. 오래된 AI daily scheduled run이 과거 날짜 수집 실패 알림을 반복하지 않도록 3일 초과 stale target은 skip 처리하도록 보강.
- 검증 결과: Sales Alert 2026-07-30 stale run success 정리, 2026-08-08 run 신규 시작 확인. `DB_EasyPOS_Sales_Dags.download_easypos_product` attempt 3 running 진입 확인. 관련 테스트 2건, py_compile, 컨테이너 DAG import 통과.
- 남은 위험: 동시 Chrome 5개는 6개보다 안정적이지만 일부 OKPOS/Posfeed 로그에서 renderer 연결 끊김이 재발할 수 있어 pool 상태 관찰 필요.

## 2026-08-10 Food Guide 주문 수집 버튼 대기 실패 수정
- 대상: `Food_Guide_orders_collect_Dags`, `modules/transform/pipelines/db/Food_Guide_orders_collect.py`.
- 변경: 주문내역 화면 준비 확인에서 `조회`/`엑셀다운로드` 버튼을 고정 ID selector만 보지 않고 기존 텍스트 fallback도 같은 timeout 안에서 함께 확인하도록 보강. 실패 시 debug html/screenshot을 남기도록 개선.
- 검증 결과: 런타임/컨테이너 `py_compile`, 컨테이너 DAG import 통과. `trigger_dag('Food_Guide_orders_collect_Dags')`로 `manual__2026-08-10T03:03:39.732153+00:00` 생성.
- 남은 위험: 재실행 run은 확인 시점에 `queued` 상태라 Selenium pool/스케줄러 대기 후 실제 수집 완료 여부 확인이 필요하다.

## 2026-08-10 DB_ToOrder_Daily_Store_Dags 다운로드 잔여파일 진단 보강
- 대상: `modules/extract/crawling_toorder_sales_report.py`, `tests/test_toorder_report_download_guard.py`.
- 변경: ToOrder 보고서 브라우저에서 기본 확장/동기화를 비활성화하고, datedetail 다운로드 대기를 `_wait_for_report_xlsx_download`로 통일. xlsx 미생성 시 최근 잔여 파일명·크기·시그니처를 로그로 남기도록 보강.
- 검증 결과: `py_compile`, 컨테이너 DAG import, `tests/test_toorder_report_download_guard.py`, `tests/test_toorder_selenium_pool_contract.py` 통과.
- 남은 위험: 운영 ToOrder 사이트가 실제 보고서 파일을 생성하지 않는 장애는 수동 1일 실행 또는 다음 스케줄 로그로 확인 필요. OneDrive/analytics 산출물 재실행 검증은 별도 승인 후 수행.

## 2026-08-10 DB_ToOrder_Daily_Store_Dags 수동 재실행
- 대상: `DB_ToOrder_Daily_Store_Dags` run `manual_fix_toorder_20260809_1247`, `sale_date=2026-08-09`.
- 실행 결과: task `collect_toorder_daily_store` 성공. 지연 생성된 datedetail xlsx를 회수해 `toorder_daily_store_20260809.parquet` 생성.
- 검증 결과: parquet 존재 확인, 66행/61컬럼. Airflow run 상태 `success`.
- 남은 위험: 첫 180초 대기 시점에는 xlsx가 감지되지 않아 지연 회수 경로로 성공했으므로, 다음 스케줄에서도 ToOrder 파일 생성 지연 여부를 로그로 관찰 필요.

## 2026-08-10 DB_Hall_Sales_Target_Dags 월화 스케줄 보정
- 대상: `DB_Hall_Sales_Target_Dags`, `modules.transform.utility.schedule.DB_HALL_SALES_TARGET_TIME`.
- 변경: 매일 실행되던 Hall Sales Target 스케줄을 매주 월·화 11:00(`0 11 * * 1,2`)으로 수정하고 DAG 설명 주석을 운영 기준에 맞춤.
- 검증 결과: `py_compile`, 컨테이너 DAG import, `airflow dags list-import-errors` 통과. 컨테이너에서 schedule `0 11 * * 1,2` 확인.
- 남은 위험: `unified_sales_grp` 최신일이 2026-08-07에 머물러 있어 2026-08-08/09 복구가 필요하나, mart 경로가 OneDrive mount라 승인 전 재실행하지 않음.

## 2026-08-10 Food Guide 주문 수집 활성 탭 selector 보강
- 대상: `modules/transform/pipelines/db/Food_Guide_orders_collect.py`, `tests/test_food_guide_orders_collect.py`.
- 변경: `조회`/`엑셀다운로드` 버튼 selector를 활성 주문내역 탭으로 제한하고, 숨겨진 탭 후보는 건너뛰도록 clickable 탐색을 보강.
- 검증 결과: `py_compile`, 신규 단위 테스트 2건, 컨테이너 DAG import, 컨테이너 `py_compile`, 실패 debug HTML 기준 selector 회귀 확인 통과.
- 남은 위험: 실제 DAG 재실행은 mart 저장 경로 수정 가능성이 있어 OneDrive 승인 전 수행하지 않음.

## 2026-08-10 POSFeed 원천 7일 lookback 보강
- 대상: `DB_Posfeed_Sales_Dags`, `DB_Posfeed_Sales.py`.
- 변경: POSFeed 원천 누락 검사를 최근 7일로 활성화하고 주말 no-data 스킵을 제거. 날짜 피커 미탐지 또는 다운로드 날짜 불일치 시 기본값 적재 대신 실패하도록 보강.
- 검증 결과: `py_compile`, 컨테이너 DAG import 통과.
- 남은 위험: 실제 8/8~8/9 원천 재수집은 OneDrive analytics 수정이므로 별도 실행 승인 후 확인 필요.

## 2026-08-10 POSFeed 8/8~8/9 원천 복구
- 대상: `posfeed_sales`, `posfeed_sales_detail`, `unified_sales_grp`.
- 변경: 8/9 partial 원천을 제거한 뒤 8/8~8/9 POSFeed 주문 4,369건과 상세 4,369건 대상 상품행을 재수집. MUI 날짜 피커 셀 클릭 방식과 상세 로그인 재시도를 보강.
- 검증 결과: `DB_UnifiedSales` 8/8, 8/9 수동 DAG success. 8/8 posfeed 11,295행/48매장, 8/9 posfeed 12,856행/48매장 반영 확인.
- 남은 위험: 중간에 날짜 선택 실패 run 1건과 Today 대기 run 1건을 실패 종료 처리함.

## 2026-08-10 POSFeed today 브라우저 실행 복구
- 대상: `DB_Posfeed_Sales_Today_Dags.move_to_storage`, `DB_Posfeed_Sales.py`.
- 변경: 직접 `uc.Chrome()` 호출 대신 공통 `launch_uc_chrome()`을 사용해 UC 캐시 드라이버 실패, RemoteDisconnected, 표준 chromedriver fallback을 적용.
- 검증 결과: auto-heal 및 런타임 `DB_Posfeed_Sales.py`, `selenium_uc.py` py_compile 통과.
- 남은 위험: 실제 재실행은 Posfeed 접속과 다운로드 상태에 따라 확인 필요.

## 2026-08-10 POSFeed today 주문상세 브라우저 실행 복구
- 대상: `DB_Posfeed_Sales_Today_Dags.scrape_order_details`, `DB_Posfeed_Sales_Detail.py`.
- 변경: 주문상세 수집의 직접 `uc.Chrome()` 호출을 공통 `launch_uc_chrome()` 사용으로 교체해 RemoteDisconnected 세션 생성 실패를 재시도/캐시정리/fallback 대상으로 처리.
- 검증 결과: 작업공간/런타임 `py_compile`, 컨테이너 DAG import 통과.
- 남은 위험: Posfeed 사이트 또는 계정 상태 문제는 실제 재실행 결과로 확인 필요.

## 2026-08-10 OKPOS card today WebDriver 세션 재시도 보강
- 대상: `DB_OKPOS_Review_card_Today.download_okpos_card_test`, `DB_OKPOS_Card_Test.py`.
- 변경: OKPOS 카드 수집 중 Chrome renderer가 닫히는 transient WebDriver 오류의 task 내부 기본 재시도를 2회에서 3회로 확대.
- 검증 결과: 작업공간/런타임 `py_compile` 통과. 로컬 DAG import는 `pendulum` 미설치로 미수행.
- 남은 위험: OKPOS 사이트/Chrome renderer가 계속 불안정하면 실제 재실행에서 추가 실패 가능.

## 2026-08-10 DB_Hall_Sales_Target_Dags monthly KPI 주차 스냅샷 보정
- 대상: `modules.transform.pipelines.db.DB_Bsp_Monthly_Kpi`, `tests/test_bsp_monthly_kpi.py`.
- 변경: `sync_monthly_kpi`가 `ym` 최신값을 모든 주차에 반복하지 않고 `주시작일` 기준 주간 매출 합계와 해당 주 최신 SNS 스냅샷을 기록하도록 수정. 해당 주 데이터가 없으면 이전 반복값을 비움.
- 검증 결과: `py_compile`, 신규 단위 테스트 2건, 컨테이너 `DB_Hall_Sales_Target_Dags` task tree/import 확인 통과.
- 남은 위험: `monthly_kpi.xlsx`는 OneDrive 파일이므로 승인 전 저장 실행은 하지 않음. 2026-08-07/10 SNS 원본 값이 동일해 두 주차의 SNS 숫자는 실제로 같게 보일 수 있음.

## 2026-08-10 DB_UnifiedSales resolve_date 동시 실행 대기 보강
- 대상: `dags/db/DB_UnifiedSales_Dags.py`.
- 변경: UnifiedSales 관련 DAG 실행 중 parquet 동시 write 차단 시 `resolve_date`가 최대 24회, 10분 간격으로 대기 재시도하도록 보강.
- 검증 결과: 작업공간/런타임 `py_compile` 통과, 컨테이너 `airflow dags list`에서 DB_UnifiedSales import 정상 확인.
- 남은 위험: 차단 DAG가 4시간 이상 running 상태로 고착되면 수동으로 Airflow run 상태 확인 필요.

## 2026-08-10 배민 원천 lookback 복구 준비
- 대상: `DB_Beamin_Macro_Dags`, `DB_Beamin_Macro_Lookback_Trigger_Dags`.
- 변경: 8/7 수동 재수집 및 지연 정규 run을 23:40 기준 실패로 닫고 staging을 삭제. 최근 7일 ToOrder 대비 배민 orders 원천 차이를 감지해 하루 최대 2건 `orders_only`로 트리거하는 lookback DAG 추가.
- 검증 결과: 8/7/정규 run 프로세스 종료, staging 삭제 확인, `selenium_pool=5` 원복, 신규 DAG `py_compile` 및 import error 없음 확인.
- 남은 위험: 내일 00:05/00:15 KST 실행 결과는 배민 사이트 세션 안정성과 계정 상태에 따라 확인 필요.

## 2026-08-11 POS 수집 우선 슬롯 확대
- 대상: Airflow `selenium_pool`, POS 계열 수집 DAG.
- 변경: 배민 계열 실행 4건이 `selenium_pool` 대부분을 점유해 POS 태스크가 대기하므로 일시 12로 확대한 뒤, 로딩 타임아웃 위험을 줄이기 위해 운영 고정값을 8로 조정.
- 검증 결과: OKPOS, EasyPOS, UnionPOS, Posfeed Today 태스크가 실행 후 성공. `selenium_pool=8` 설정 확인. worker 메모리 약 5.7GiB/15.6GiB로 여유 확인.
- 남은 위험: EasyPOS 상품 수집에서 다운로드/렌더링 타임아웃 재시도가 관측되어 동시 브라우저 수와 별개로 EasyPOS 메뉴 탐색 안정화가 필요할 수 있음.

## 2026-08-11 배민 Selenium 슬롯 선점 차단
- 대상: `DB_Beamin_Macro_Dags`, `DB_Beamin_Macro_Dags_Retry`.
- 변경: 배민 본 DAG와 retry DAG를 pause해 누적 retry run이 POS 수집 시간대에 `selenium_pool`을 계속 선점하지 않도록 차단.
- 검증 결과: 두 DAG 모두 `is_paused=True` 확인. EasyPOS 상품 수집은 4번째 시도에서 최종 성공.
- 남은 위험: 이미 실행 중인 배민 태스크 3건은 자연 종료까지 리소스를 사용하며, 배민 재개 전 retry backlog 정리가 필요.

## 2026-08-11 배민 전용 Selenium pool 분리
- 대상: `DB_Beamin_Macro_Dags`, `DB_Beamin_Macro_Dags_Retry`, Airflow `baemin_selenium_pool`.
- 변경: 배민 DAG pause 방식은 운영 기준과 맞지 않아 두 DAG를 재개하고, 배민 Selenium 태스크만 `baemin_selenium_pool`로 분리. 전용 pool 슬롯은 2로 고정.
- 검증 결과: 두 배민 DAG `paused=False`, `baemin_selenium_pool=2`, DAGBag에서 `collect_batch_*`, `retry_failed`, `retry_collect_*`가 전용 pool을 사용함을 확인.
- 남은 위험: 이미 이전 pool으로 시작된 실행 중 태스크 3건은 완료 전까지 `selenium_pool`에 표시되며, 이후 새 실행부터 전용 pool 제한이 적용됨.

## 2026-08-11 배민 실패분 현재 큐 재시도 운영
- 대상: `DB_Beamin_Macro_Dags_Retry`.
- 변경: 신규 수동 run 추가 없이 현재 running 1건과 queued 2건만 `baemin_selenium_pool=2`에서 순차 처리하도록 유지.
- 검증 결과: `retry__20260731__attempt_1...`의 `retry_collect_1/2`가 전용 pool에서 heartbeat 정상으로 실행 중이며, queued는 2건 유지 확인.
- 남은 위험: 현재 대상은 주로 ad funnel 날짜 필터/지표 추출 실패 재시도로, parse_error CSV가 남을 수 있음. queued 2건은 과거 task 상태가 섞여 있어 선행 run 완료 후 자동 진입 여부 확인 필요.

## 2026-08-11 배민 failed payload 재시도 큐 생성
- 대상: `DB_Beamin_Macro_Dags_Retry` failed run payload.
- 변경: 실패 run 중 대상 payload가 있고 이후 success로 덮이지 않은 최신 실패분을 `manual_refill__*` run으로 재트리거. `retry_wait_sec=0`, `baemin_selenium_pool=2` 유지.
- 검증 결과: `manual_refill` 15건 생성/확인, 1건 running 및 14건 queued. 실행 중 task는 `retry_collect_1/2` 2개로 전용 pool 제한 준수.
- 남은 위험: 과거 failed DAG run 30개는 히스토리로 남으며, 새 `manual_refill` run 결과로 실제 보충 여부를 판단해야 함. 일부 ad funnel은 배민 화면 날짜 필터 문제로 잔여 parse_error가 남을 수 있음.

## 2026-08-11 배민 manual_refill 큐 축소
- 대상: `DB_Beamin_Macro_Dags_Retry` `manual_refill__*` run.
- 변경: 사용자가 다음 예정 실행 과다를 지적해 현재 running 1건만 유지하고 queued 14건은 실행되지 않도록 failed/skipped 처리.
- 검증 결과: `DB_Beamin_Macro_Dags_Retry` active는 running 1건만 남고 queued 0건 확인. `baemin_selenium_pool=2`에서 `retry_collect_1/2`만 실행 중.
- 남은 위험: 닫은 14건의 payload는 DAG run conf에 남아 있으므로 필요 시 날짜별로 선별 재실행해야 함.

## 2026-08-11 배민 상위 전용 refill 보정
- 대상: `DB_Beamin_Macro_Dags_Retry` `manual_refill__20260627...`, `top_refill__20260627__0d7a44058d`.
- 변경: `manual_refill` run이 `collect_range=None`/`allowed_account_ids=[]`로 전체 범위처럼 설정되어 중지하고, 상위 계정 33개를 `allowed_account_ids`로 명시한 `top_refill` run을 생성.
- 검증 결과: 기존 running 1건 중지, `top_refill` 1건 running 확인. conf 기준 `collect_range=상위`, `allowed_account_ids=33`, 실제 대상 계정 `Dori0145`, `now1223`, `skmo1004` 모두 상위에 포함.
- 남은 위험: 한글 인자를 PowerShell 경유로 넘기면 깨질 수 있어 운영 확인은 Python 내부 유니코드 값으로 수행해야 함.

## 2026-08-11 DB_CollectionCompare_Dags 배민 수동 적재 차단 진단
- 대상: `modules.transform.pipelines.db.DB_BaeminManual_load`.
- 변경: 다월 배민 수동 CSV의 부분 적재를 막도록 저장 전 사전 검증 후 일괄 저장으로 변경. 기본 실행에서는 신규 파일에 있는 주문번호는 덮어쓰고, 신규 파일에 없는 기존 주문번호는 보존하도록 축소 가드를 완화. 통합 XCom에 skipped 목록도 포함.
- 검증 결과: 컨테이너 `py_compile`, `DB_CollectionCompare_Dags` import 확인, `tests/test_baemin_manual_load.py` 19건 통과.
- 남은 위험: `force_shrink=true`로 실행하면 기존처럼 신규 파일이 커버한 날짜 구간의 누락 주문번호까지 제거할 수 있으므로 원본 완전성 확인 후에만 사용 필요.

## 2026-08-11 Flow Chrome 자동 실행 중지
- 대상: Windows 작업 스케줄러 `AirflowFlowSusamChromeWindow`.
- 변경: 매일 08:55에 Flow 링크를 여는 작업을 삭제하지 않고 비활성화해 Chrome 로그인 창 자동 표시를 차단.
- 검증 결과: 작업 상태가 `Disabled`임을 확인했고, `flow_susam_profile`/`flow.team`/`QAa7L`/`remote-debugging-port=9223` 인자를 가진 Chrome 프로세스가 없음을 확인.
- 남은 위험: 이 작업에 의존하던 Flow 수삼 관련 자동 게시/확인 자동화는 재활성화 전까지 자동 실행되지 않음.

## 2026-08-11 DB_MenuHierarchy_Test_Dags build_orders 자동복구
- 대상: `modules.transform.pipelines.db.DB_MenuHierarchy_Test`.
- 변경: 메뉴최빈 사이즈 보정 뒤 사용용량과 미해결 사유를 다시 계산해, 사이즈만 채워지고 닭 사용량이 빈칸으로 남는 차단 오류를 수정.
- 검증 결과: `py_compile` 통과, 컨테이너 DAG import 통과, `순살/중` 환산값 0.8 확인.
- 남은 위험: 전체 DAG 재실행은 OneDrive 산출물 갱신을 동반하므로 자동 트리거하지 않음. 재실행 시 새 산출물로 차단 해소를 최종 확인해야 함.

## 2026-08-11 배민 ToOrder 불일치 retry 및 upload I/O 복구
- 대상: `DB_Beamin_Macro_Upload_Dags`, `DB_Beamin_Macro_Dags_Retry`, 배민 검증/retry/upload pipeline.
- 변경: ToOrder-배민 금액 불일치를 즉시 원천 오류로 제외하지 않고 매장별 최대 3회 orders 재수집 대상으로 유지. 3회 도달 시 `toorder_possible_mismatch_stores`로 남겨 추가 자동 재시도를 중단하도록 보강.
- 변경: PC2 upload inbox CSV 읽기 `OSError [Errno 5]` 발생 폴더는 `_quarantine`으로 회수하고, 정상 적재 0건이면 validate downstream을 skip해 같은 폴더 반복 실패를 차단.
- 검증 결과: `py_compile`, 컨테이너 DAG import, Airflow import error 0건, 관련 pytest 65건 통과. `codex_fix_upload_io__20260811T134850` 수동 run success 및 문제 bottom 폴더 quarantine 확인.
- 남은 위험: quarantine된 폴더의 손상 파일은 자동 적재되지 않으므로, 해당 bottom retry 산출물이 꼭 필요하면 PC2에서 원본 재생성 후 다시 upload inbox로 보내야 함.

## 2026-08-11 DB_MenuHierarchy_Test 닭사용량 불변식 보강
- 대상: `modules.transform.pipelines.db.DB_MenuHierarchy_Test`.
- 변경: 반반 슬롯 컬럼/판정옵션 입력, 닭가산 옵션/환산 시트, 주문예외 시트, source 무관 취소 닭 0 처리와 중복 main menu_seq 보정을 추가.
- 검증 결과: `py_compile`, LLM 금지 가드, 반반 슬롯/닭추가 단위 검증 통과. 2026-08 메모리 실행에서 네 source 유지, 취소 닭 0, 중복 main 0, 뼈+순살 합계 위반 0 확인.
- 남은 위험: 전체 5개월 메모리 검증은 3분 제한으로 중단. OneDrive 산출물 갱신이 필요한 `run_all` 전체 검증은 사용자 승인 후 실행해야 함.

## 2026-08-11 DB_MenuHierarchy_Test 수익채널 분리
- 대상: `modules.transform.pipelines.db.DB_MenuHierarchy_Test`.
- 변경: 홀/배달 플랫폼별 수수료 차이를 반영하도록 `수익채널`을 02/24/27/28번 산출물과 수익키에 추가. OKPOS도 배달 order_type/platform이면 배달 수수료율을 조회하고, 기존 수익키 값은 새 채널 키로 이관 표시.
- 검증 결과: UTF-8 읽기, `py_compile`, 수익채널/수익키 소량 검증, 홀 0%와 OKPOS 배달/쿠팡 배달 수수료 계산 검증 통과.
- 남은 위험: 기존 수익률은 채널별로 자동 이관되지만 실제 마진율은 홀/배달별 재검토 필요. OneDrive 산출물 갱신은 승인 후 실행해야 함.

## 2026-08-11 DB_MenuHierarchy_Test 수익률표 품목 자동보정
- 대상: `modules.transform.pipelines.db.DB_MenuHierarchy_Test`, OneDrive `new_classification_orders/01_수기입력.xlsx`.
- 변경: `사이즈`/`닭유형`/`맛선택` 유료 옵션이 `품목|[대]`, `품목|순살`, `품목|기본`처럼 수익률 입력 불가능한 행으로 나오지 않도록 부모 메뉴 수익키에 흡수. 유료 리뷰/요청 옵션은 재료추가 성격으로 수익률표에 표시하고 자동 이관 메모를 제거.
- 검증 결과: `py_compile` 통과. 재생성된 `수익률` 시트 285행에서 `사이즈`/`닭유형`/`맛선택` 행 0건, 입력불가 품목키 0건, 자동 이관 메모 0건 확인.
- 남은 위험: `build_orders(None)`는 산출물 쓰기 후 `main_parent_mismatch` 2건, `chicken_split_total_mismatch` 1건 등 기존 차단 검증으로 실패 상태. 수익률 입력표는 갱신 완료.

## 2026-08-11 DB_MenuHierarchy_Test source별 수익채널/검증 실패 보완
- 대상: `modules.transform.pipelines.db.DB_MenuHierarchy_Test`, OneDrive `new_classification_orders`.
- 변경: OKPOS main parent self 보정, 수기 닭유형과 충돌하는 반반슬롯 제거, 슬롯 없는 반반 main의 혼합 기본 보정, 예외/전액할인 후 닭 분해 합계 동기화를 추가.
- 검증 결과: `build_orders(None, debug_outputs=True)` 성공. source/channel 커버리지 확인: okpos=홀, 배민수동=배달의민족, 쿠팡수동=쿠팡이츠, posfeed=기타/땡겨요/배달의민족/요기요/쿠팡이츠. `main_parent_mismatch=0`, `chicken_split_total_mismatch=0`, 수익률표 입력불가 키 0건.
- 남은 위험: 수익률/재료단가/메뉴중량/뼈순살비율 입력 대기성 completeness 후퇴는 WARN으로 남음. 수익률 입력 후 재실행 필요.

## 2026-08-11 도리당봇 Flow 프로젝트 현황 어시스턴트
- 대상: `modules.transform.doridang_bot`, `modules.transform.utility.paths`.
- 변경: Flow parquet 기반 tool 6종, Ollama 스트리밍 backend, `ThreadingHTTPServer` SSE 서버, 재사용 가능한 채팅 UI, 도리당봇 log 경로 상수를 추가.
- 검증 결과: `py_compile`, 프로젝트/상태/검색/스레드/최근활동 조회, LLM 스트리밍, `/health`·정적 파일·빈 프로젝트 `/api/chat` SSE 검증 통과. 승인 후 OneDrive `flow/log.md` 실제 append와 `search_log` 검색도 확인.
- 남은 위험: 외부 공유는 Windows 방화벽 8788 인바운드 허용 필요.

## 2026-08-11 도리당봇 담당자 질문 복구
- 대상: `modules.transform.doridang_bot`.
- 변경: 응답 대기 중 `(생각중)..` 표시, tool 인자 필터링, 담당자별 진행상황 `get_worker_status`와 호칭 제거 정규화를 추가.
- 검증 결과: 다른 PC의 `안녕` 질문이 OneDrive `flow/log.md`에 기록됨을 확인. `황유경 실장` 질문에서 51건, 완료 23/진행 21/대기 7 집계 및 실제 `/api/chat`의 `get_worker_status` 호출 확인.
- 남은 위험: 사람 기준 답변은 허용된 3개 프로젝트의 `worker` 컬럼에 이름이 들어간 게시글만 포함한다.

## 2026-08-11 도리당봇 세션 UI 및 tool 누출 차단
- 대상: `modules.transform.doridang_bot`.
- 변경: GPT형 좌측 대화 세션 목록을 브라우저 `localStorage` 기반으로 추가. 담당자/PM 질문은 LLM 자유생성 전에 서버가 직접 `get_worker_status`를 실행해 `_icall_` 내부 호출 텍스트 누출을 차단.
- 검증 결과: 정적 HTML/JS/CSS 반영, `node --check`, `py_compile`, `조민준 PM프로젝트 진행상황` 실제 `/api/chat`에서 정형 응답 및 `_icall_` 미노출 확인.
- 남은 위험: 좌측 세션 기록은 브라우저별 로컬 저장이며, 다른 PC/브라우저와 자동 동기화되지 않는다.

## 2026-08-11 도리당봇 속도 및 주제 검색 개선
- 대상: `modules.transform.doridang_bot`.
- 변경: 프로젝트/담당자/마케팅 실적/인사 질문은 LLM 호출 전에 parquet 직접 조회 정형 응답으로 처리. 전체 3개 프로젝트 주제 검색 `get_topic_status`를 추가해 마케팅·광고·체험단·바이럴·성과 관련 Flow 게시글을 집계.
- 검증 결과: 현재 Ollama 후보는 `qwen2.5:14b`만 확인됨. 실제 `/api/chat` 기준 `마케팅 실적` 0.144초, `직영점 성장전략 진행상황` 0.080초, `안녕` 0.027초 응답 및 `_icall_` 미노출 확인.
- 남은 위험: `gpt-oss:20b`가 설치/노출되지 않으면 자유형 질문은 qwen 폴백 모델을 사용한다.

## 2026-08-11 도리당봇 gpt-oss 후보 복구
- 대상: `modules.transform.doridang_bot.llm_backend`, Ollama unhealthy 임시 캐시.
- 변경: 설치돼 있던 `gpt-oss:20b`가 unhealthy 캐시로 후보에서 제외되던 상태를 해제하고, gpt-oss 스트리밍 최소 `num_predict`를 256으로 올려 thinking만 출력하고 content가 비는 현상을 방지.
- 검증 결과: Ollama 후보가 `gpt-oss:20b`, `qwen2.5:14b` 순으로 복구됐고, `chat_stream`에서 gpt-oss delta 응답이 정상 출력됨. 도리당봇 서버 8788 재시작 완료.
- 남은 위험: gpt-oss 자유형 답변은 qwen보다 초기 응답이 느릴 수 있으므로 운영성 질문은 계속 parquet 직접 응답 경로를 우선 사용한다.

## 2026-08-11 도리당봇 도리당센터 탭 추가
- 대상: `modules.transform.doridang_bot.static`.
- 변경: 좌측 사이드바 상단에 `도리당센터` 탭을 추가하고 Power BI 도리당센터 보고서 URL로 바로 이동하도록 연결.
- 검증 결과: 정적 HTML/CSS UTF-8 읽기와 실행 중 서버의 `/`, `/static/chat.css` 응답에서 링크·스타일 반영 확인.
- 남은 위험: Power BI 접근 권한은 Microsoft 계정/조직 권한 정책을 따른다.

## 2026-08-11 도리당봇 GPT-OSS 최종 답변 전환
- 대상: `modules.transform.doridang_bot.server`, `modules.transform.doridang_bot.llm_backend`.
- 변경: 담당자/프로젝트/주제 조회는 parquet로 근거를 먼저 수집하되, 최종 답변은 `gpt-oss:20b`가 근거 JSON을 바탕으로 리더 보고식으로 작성하도록 전환. gpt-oss 스트리밍 `num_predict`를 2048로 상향하고 `think=False`로 설정.
- 검증 결과: `황유경 실장 프로젝트 진행상황 피드백 및 결과` 임시 API 호출에서 `get_worker_status` 근거 후 GPT식 delta 답변 생성, 기존 정형 템플릿 문구와 `_icall_` 미노출 확인. 8788 서버 재시작 완료.
- 남은 위험: GPT-OSS 최종 작성 경로는 빠른 템플릿보다 느릴 수 있으나, 근거 JSON 밖의 내용은 추측 금지 프롬프트로 제한한다.

## 2026-08-11 도리당봇 마크다운/추천질문 UI
- 대상: `modules.transform.doridang_bot.static`.
- 변경: assistant 답변을 제한된 마크다운 뷰어로 렌더링해 굵게/목록/표/제목 가독성을 개선. 세션 이름 변경 버튼과 새 세션 리더용 추천 질문 패널을 추가.
- 검증 결과: `node --check`, 정적 JS/CSS UTF-8 읽기, 실행 중 서버의 `/static/chat.js`·`/static/chat.css` 반영 확인.
- 남은 위험: 마크다운 렌더러는 기본 문법만 지원하며 복잡한 중첩 마크다운은 단순 표시될 수 있다.

## 2026-08-11 도리당봇 추천질문 빠른응답 및 홈/센터 탭
- 대상: `modules.transform.doridang_bot`.
- 변경: 추천질문 클릭 시 패널을 즉시 제거하고 질문을 바로 전송. 팀원별/위험/기한경과/막힌업무 intent는 GPT fallback 없이 `get_team_status`/`get_risk_status` 직접 응답으로 고정. 도리당센터 링크는 새 탭으로 열고 좌측 상단 홈 버튼을 추가.
- 검증 결과: `각 팀원 프로젝트 진행상황` 실제 `/api/chat` 0.208초, `get_team_status` 사용, `[DONE]` 종료, 홍길동/강감찬/이순신 등 허구 담당자 미노출 확인. HTML/JS/CSS 정적 반영 확인.
- 남은 위험: 추천질문 빠른응답은 정형 마크다운 요약이며 GPT-OSS 문장 재작성 경로보다 표현 유연성은 낮다.

## 2026-08-11 도리당봇 피드백 카드 및 세션 맥락 보강
- 대상: `modules.transform.doridang_bot`, `modules.transform.doridang_bot.static.chat.js`.
- 변경: 피드백/결제중 응답을 카드형으로 변경해 작성자, 참여자, 프로젝트, 시작일, 마감일, 남은 기간, 진행률, 내용 3줄 요약, Flow 출처를 표시. 웹 세션의 최근 대화 12개를 `/api/chat` history로 전송하고 서버가 LLM 입력에 반영하도록 보강.
- 검증 결과: 피드백 건 `성과추적test [결과]`가 작성자 차보령, 시작일/마감일 미등록, 남은 기간/진행률 산정불가, 3줄 요약, Flow 근거 URL로 출력됨을 확인. `py_compile`, `node --check`, history 정규화 확인 통과.
- 남은 위험: 현재 직접 도구 분기는 명시 입력 중심이며, 대명사/생략어 기반 후속 질문은 LLM fallback에서 history를 참고한다.

## 2026-08-11 도리당봇 근거 Flow URL 통일
- 대상: `modules.transform.doridang_bot`.
- 변경: 사용자 노출 `근거:`를 `get_*`, `find_*`, `filter_*` 같은 내부 도구명이 아니라 Flow 게시글/프로젝트 URL 링크로 통일. 프로젝트 URL을 도구 결과에 포함하고, 집계형 응답은 관련 프로젝트/게시글 Flow 링크를 근거로 표시.
- 검증 결과: 프로젝트, 팀원, 피드백, 게시글, 상태·기한 필터, 개인 조회 대표 응답 모두 `근거: [Flow 열기](https://flow.team/...)` 형태로 출력되고 내부 도구명이 노출되지 않음을 확인. `py_compile`, `node --check` 통과.
- 남은 위험: Flow 원천 게시글 URL이 없는 경우 프로젝트 URL로 보완 표시한다.

## 2026-08-11 도리당봇 정확 기한 날짜 필터 추가
- 대상: `modules.transform.doridang_bot`.
- 변경: `20260805 대기`처럼 8자리 날짜와 상태를 함께 입력하면 해당 기한일/상태 조합으로 업무를 필터링하도록 보완.
- 검증 결과: `20260805` + `대기`가 상태 대기·기한 20260805 업무 1건(`단품 이미지 재촬영`)으로 조회되고 Flow 링크가 표시됨을 확인. `py_compile`, `node --check` 통과.
- 남은 위험: 날짜는 현재 `YYYYMMDD` 8자리만 빠른 필터로 인식한다.

## 2026-08-11 도리당봇 상태·기한 필터 추가
- 대상: `modules.transform.doridang_bot`.
- 변경: `보류`, `기한 없음`, `기한 없음 보류` 같은 짧은 입력을 상태/기한 필터로 처리하는 `filter_posts` 빠른 분기 추가. 결과 목록에는 상태, 작성자/참여자, 프로젝트, Flow 출처 링크를 표시.
- 검증 결과: `기한 없음 보류`가 상태 보류·기한 없음 업무 5건으로 조회되고 Flow 링크가 표시됨을 확인. `py_compile`, `node --check` 통과.
- 남은 위험: `기한 없음` 단독은 현재 123건이라 상위 15건과 잔여 건수만 표시된다.

## 2026-08-11 도리당봇 좌측 상단 로고 적용
- 대상: `modules.transform.doridang_bot.static`.
- 변경: 첨부 PNG를 `static/logo.png`로 추가하고 좌측 상단 홈 버튼에 로고 이미지를 표시하도록 HTML/CSS 수정.
- 검증 결과: `node --check` 통과, HTML/CSS UTF-8 읽기 확인, `/static/logo.png` HTTP 200 확인.
- 남은 위험: 브라우저 캐시가 남으면 강력 새로고침이 필요할 수 있음.

## 2026-08-11 도리당봇 프로젝트 상세 포맷 보완
- 대상: `modules.transform.doridang_bot.server`.
- 변경: 프로젝트명 전체 입력 시 `프로젝트 ...` 제목으로 표시하고, 기한 경과 게시글에 상태, 작성자/참여자, 기한, Flow 출처 링크를 함께 표시.
- 검증 결과: `[브랜드 전략기획부] 직영점 성장전략(온라인 유입)` 입력이 project_id 2926716으로 감지되고, 제목 prefix 제거 및 Flow 링크 표시를 확인. `py_compile`, `node --check` 통과.
- 남은 위험: Flow 원천 `post_url`이 없는 글은 출처 링크가 생략된다.

## 2026-08-11 도리당봇 개인 진행상황 출처 보완
- 대상: `modules.transform.doridang_bot.server`.
- 변경: 사람 이름 단독 조회의 주요 게시글에서 `기한 없음` 노출을 제거하고 프로젝트 라벨과 Flow 출처 링크를 표시하도록 변경.
- 검증 결과: `차보령` 조회 시 44건이 잡히고 주요 게시글에 프로젝트 직영점 성장전략 및 Flow 링크가 표시됨을 확인. `py_compile`, `node --check` 통과.
- 남은 위험: Flow 원천 `post_url`이 비어 있는 글은 출처 링크가 생략된다.

## 2026-08-11 도리당봇 제목 단독 게시글 조회
- 대상: `modules.transform.doridang_bot`.
- 변경: 제목/키워드 단독 입력이 마케팅 주제 검색으로 흘러가지 않도록 허용 프로젝트 전체 게시글 검색 `find_posts`와 빠른 응답 분기를 추가. 검색 결과에는 상태, 작성자/참여자, 프로젝트, Flow 출처, 본문 일부를 표시.
- 검증 결과: `성과추적test [결과]` 입력 시 게시글 1건을 찾아 `피드백`, 작성자 차보령, 프로젝트 직영점 성장전략, Flow 링크, 본문 일부가 출력됨을 확인. `py_compile`, `node --check` 통과.
- 남은 위험: 긴 일반 질문이 우연히 게시글 본문 1건에만 매칭되면 게시글 조회로 응답할 수 있어, 4자 미만 질문은 제외하고 다건 검색은 정확 제목일 때만 빠른 응답한다.

## 2026-08-11 도리당봇 프로젝트명 및 Flow 출처 링크 보완
- 대상: `modules.transform.doridang_bot.server`, `modules.transform.doridang_bot.static.chat.js`, `modules.transform.doridang_bot.static.chat.css`.
- 변경: 화면 출력용 프로젝트명에서 `[브랜드 전략기획부]` prefix를 제거하고 `프로젝트 ...` 라벨로 통일. 업무 목록에 `출처 [Flow 열기](...)` 링크를 추가하고 마크다운 링크 렌더링/스타일을 지원.
- 검증 결과: `py_compile`, `node --check` 통과. 피드백 확인 목록에서 `프로젝트 직영점 성장전략(온라인 유입)` 및 Flow 링크가 표시됨을 확인.
- 남은 위험: Flow 원천 `post_url`이 없는 글은 출처 링크가 생략된다.

## 2026-08-11 도리당봇 작성자/참여자 표시 보완
- 대상: `modules.transform.doridang_bot.server`.
- 변경: 업무 목록 출력에서 작성자와 참여자(worker)를 함께 표시하는 헬퍼를 추가. 참여자가 비어 있거나 작성자와 같으면 작성자만 표시.
- 검증 결과: `py_compile`, `node --check` 통과. 피드백 확인 목록에서 작성자 차보령이 정상 표시됨을 확인.
- 남은 위험: Flow 원천 데이터의 참여자 필드가 비어 있는 글은 작성자만 표시된다.

## 2026-08-11 도리당봇 피드백 제목 간소화
- 대상: `modules.transform.doridang_bot.server`.
- 변경: 전체 피드백/결제중 조회 제목에서 `전체 허용 프로젝트` 접두어를 제거하고 `피드백/결제중 확인`으로 간소화. 특정 프로젝트 조회일 때만 프로젝트명을 제목에 포함.
- 검증 결과: `py_compile`, `node --check` 통과. 전체 조회 제목과 특정 프로젝트 조회 제목을 각각 확인.
- 남은 위험: 없음.

## 2026-08-11 도리당봇 상태 단독 질문 피드백 분기
- 대상: `modules.transform.doridang_bot`.
- 변경: `상태`, `상태 확인`, `상태 피드백건 확인용` 같은 짧은 질문도 피드백/결제중 전용 조회(`priority_only=True`)로 처리하도록 의도 감지 보강.
- 검증 결과: `상태`, `상태 피드백건 확인용`, `결제중 확인` 모두 피드백/결제중 확인 응답으로 출력되고 보류/대기 항목이 섞이지 않음을 확인.
- 남은 위험: 없음.

## 2026-08-11 도리당봇 피드백 전용 응답 보완
- 대상: `modules.transform.doridang_bot`.
- 변경: `상태 피드백건 확인용` 질문은 `priority_only=True`로 피드백/결제중만 조회하도록 분리. 보류/대기 상세 목록과 `기한 없음` 표시는 제외하고, 다음 액션도 피드백/결제중 기준으로 변경.
- 검증 결과: `py_compile`, `node --check` 통과. 피드백 전용 응답에 보류/대기/기한 없음이 노출되지 않음을 확인.
- 남은 위험: 없음.

## 2026-08-11 도리당봇 메인 추천 2개 축소
- 대상: `modules.transform.doridang_bot.static.chat.js`, `modules.transform.doridang_bot.tools`.
- 변경: 메인 추천 문구를 `팀원 별 프로젝트 진행상황`, `상태 피드백건 확인용` 2개로 축소하고, 띄어쓴 `팀원 별` 표현도 팀원 현황 빠른 분기로 인식하도록 보정.
- 검증 결과: `py_compile`, `node --check` 통과. 두 추천 문구가 각각 팀원 현황/피드백 위험업무 분기로 감지됨을 확인.
- 남은 위험: 없음.

## 2026-08-11 도리당봇 브라우저 뒤로가기 세션 복원
- 대상: `modules.transform.doridang_bot.static.chat.js`.
- 변경: 세션 선택, 새 대화, 홈 이동을 브라우저 history와 URL `session` 파라미터에 반영하고 `popstate`에서 이전 세션을 앱 내부에서 복원하도록 수정.
- 검증 결과: `node --check modules/transform/doridang_bot/static/chat.js` 통과.
- 남은 위험: 브라우저 history에 앱 내부 이동 기록이 전혀 없는 최초 진입 상태에서 뒤로가기를 누르면 브라우저 기본 동작으로 이전 사이트로 이동할 수 있음.

## 2026-08-11 도리당봇 조민준 참여업무 및 결제중 우선 노출
- 대상: `modules.transform.doridang_bot`.
- 변경: 리더용 팀원 집계를 작성자뿐 아니라 Flow 업무 지정자 필드까지 포함해 `작성자/참여자` 기준으로 보정. `피드백`과 `결제중` 상태는 팀원 요약 최상단 `최우선 확인` 섹션에 먼저 노출.
- 검증 결과: 조민준이 0건이 아니라 `현장 요청사항` 1건으로 집계됨을 확인. 현재 허용 3개 프로젝트에는 `피드백` 1건, `결제중` 0건이며 `피드백` 건이 최상단에 표시됨.
- 남은 위험: `회사 현황판 구축(2926713)` 프로젝트는 프로젝트 목록에는 있으나 현재 `flow_post` parquet 게시글이 0건이라, 실제 현황판 글 반영은 Flow 수집 단계 보강이 추가로 필요하다.

## 2026-08-11 도리당봇 리더용 작성자 표 고정
- 대상: `modules.transform.doridang_bot`.
- 변경: 리더용 팀원 집계를 `조민준/황유경/차보령` 3명 작성자 기준으로 고정하고, 팀원/프로젝트/개인 상세 출력에서 사용자 노출 `담당` 문구를 `작성자`로 변경.
- 검증 결과: `py_compile`, `node --check` 통과. 작성자 기준 팀원표가 조민준 0건, 황유경 90건, 차보령 42건으로 표시되고 팀원/프로젝트/개인 상세 출력에 `담당` 문구가 남지 않음을 확인.
- 남은 위험: Flow 작성자 기준은 게시글 생성 책임 기준이며 실제 수행 담당과 다를 수 있으므로 답변에 분류 기준을 계속 명시한다.

## 2026-08-11 도리당봇 작성자 기준 팀원 분류
- 대상: `modules.transform.doridang_bot`.
- 변경: 팀원별/개인별 진행상황에 `basis=author|worker` 기준을 추가하고 기본 추천질문은 작성자 기준으로 변경. 담당자 미지정이 많은 Flow 특성을 반영해 작성자 기준 집계를 우선 사용.
- 검증 결과: 작성자 분포 황유경 90/차보령 42/오나영 1 확인. 실제 `/api/chat`에서 팀원별 작성자 기준 0.132초, 황유경 작성자 기준 0.031초 응답 및 `[DONE]` 확인.
- 남은 위험: 작성자 기준은 게시글 작성 책임을 보여주며 실제 업무 담당자와 다를 수 있으므로 답변에 분류 기준을 명시한다.

## 2026-08-11 BSP KPI 주간 미입력 알림 판정 수정
- 대상: `modules.transform.pipelines.strategy.SMP_bsp_kpi_weekly`, `tests/test_bsp_kpi_weekly.py`.
- 변경: 도메인 내 실적 1개만 있어도 제출 완료로 보던 판정을 필수 수기 지표 전체 입력 기준으로 변경하고, 브랜드 바이럴 자동 계산 지표는 알림 판정에서 제외.
- 검증 결과: `python -m pytest -o cache_dir=.tmp/pytest_cache --basetemp=.tmp/pytest_tmp tests/test_bsp_kpi_weekly.py` 32건 통과, 기존 2026-08-03 parquet 읽기 전용 판정에서 황유경 미입력 1건 확인.
- 남은 위험: 이미 실행된 2026-08-11 11:00 알림은 소급 발송되지 않음. 필요하면 Airflow에서 해당 알림 task를 수동 재실행해야 함.

## 2026-08-11 BSP KPI task DB_Hall DAG 통합
- 대상: `dags/db/DB_Hall_Sales_Target_Dags.py`, `dags/strategy/Strategy_BspKpi_01_Weekly_Dags.py`.
- 변경: BSP KPI 주간 mart 생성과 미입력 알림 task를 `DB_Hall_Sales_Target_Dags`의 `sync_monthly_kpi` 뒤로 이동하고, 기존 Strategy DAG는 중복 방지를 위해 수동 실행 전용(`schedule=None`)으로 전환.
- 검증 결과: `py_compile` 통과, 격리 `AIRFLOW_HOME` DAG import 통과, `tests/test_bsp_kpi_weekly.py` 32건 통과.
- 남은 위험: 실제 OneDrive `bsp_kpi_weekly.csv/parquet` 갱신과 알림 발송은 다음 `DB_Hall_Sales_Target_Dags` 실행 때 반영됨.

## 2026-08-11 DB_MenuHierarchy_Test_Dags 실패 run 재검증
- 대상: `DB_MenuHierarchy_Test_Dags.build_orders`.
- 변경: 최신 코드로 manual run `manual__2026-08-11T07:16:09+00:00` 재실행해 이전 실패 원인 `chicken_split_total_mismatch` 재발 여부 확인.
- 검증 결과: DAG run 성공, `02_최종주문.csv` 31,908행 생성, `chicken_split_total_mismatch=0`, `main_parent_mismatch=0`.
- 남은 위험: 과거 실패 run `manual__2026-08-11T06:31:46.409516+00:00` 이력은 Airflow UI에 실패로 남아 있을 수 있음.

## 2026-08-11 DB_MenuHierarchy_Test 쿠팡 한그릇 순살 main 보정
- 대상: `modules.transform.pipelines.db.DB_MenuHierarchy_Test`.
- 변경: `백도리당`을 닭 메뉴 토큰에 추가하고, 배달 수동 소스의 `한그릇/1인 + 순살` 닭 메뉴가 상품표에서 side로 들어와도 main으로 승격되게 보정. main 행은 과거 `option_kind_확정=닭유형` 값이 있어도 `option_kind=메인`을 우선 적용.
- 검증 결과: 2026-05 중간 파이프라인에서 `[한그릇] 누룽지 1인 순살 나만의 백도리당`이 `main`, `순살/1인/0.3`, 수익키 `메뉴|쿠팡이츠|...|1인|순살`로 생성됨. 신규 회귀 테스트 4건 통과.
- 남은 위험: OneDrive 산출물은 아직 재생성하지 않았으므로 `01_수기입력.xlsx`/`27_profit_rate_master`에는 이전 키가 남아 있을 수 있음. 승인 후 DAG 재실행 필요.

## 2026-08-11 UnifiedSales 배민1 플랫폼 정규화
- 대상: `modules.transform.pipelines.db.DB_UnifiedSales_common`, `modules.transform.pipelines.db.DB_UnifiedSales`, `MART_DB/unified_sales_grp`.
- 변경: unified_sales 저장 직전에 `platform=배민1`을 `배달의민족`으로 정규화하고 `_pk`를 재계산하도록 방어막을 추가. 기존 일별 parquet 207개 파일 23,393행을 소급 교정하고 `daily_summary.parquet`를 재생성.
- 검증 결과: 관련 pytest 29건 통과. 교정 후 `unified_sales_*.parquet` 385개와 `daily_summary.parquet` 모두 `platform=배민1` 0건 확인.
- 남은 위험: 상류 ToOrder/posfeed analytics 원천 parquet는 이번 범위에서 직접 rewrite하지 않음.

## 2026-08-11 요기요 정산 자동수집 대시보드 추가
- 대상: OneDrive 확장 `doridang_collector_개발용`.
- 변경: 요기요 정산 페이지 클릭용 content 모듈과 계정별 반복 대시보드를 추가하고, manifest/popup/background 다운로드 리네임 훅을 연결.
- 검증 결과: 신규/수정 JS `node --check` 통과, manifest JSON 및 content script 순서 확인, 요기요 67행이 63개 계정 그룹으로 묶임을 확인.
- 남은 위험: 실제 요기요 DOM 셀렉터와 파일 저장은 크롬 확장 리로드 후 1계정 실전 다운로드로 추가 확인 필요.

## 2026-08-11 요기요 정산 매장 선택 보정
- 대상: OneDrive 확장 `doridang_collector_개발용`.
- 변경: 정산 다운로드 전에 요기요 매장 드롭다운에서 목표 `도리당/나홀로` 상호를 선택하도록 보강하고, 같은 계정의 여러 매장은 로그인 1회 후 매장별로 반복 다운로드하도록 수정.
- 검증 결과: `content/08_yogiyo.js`, `runner_yogiyo.js`, `background.js` `node --check` 통과. 다운로드 저장 확인은 메시지와 `chrome.downloads.search` 폴링을 병행하도록 변경.
- 남은 위험: 실제 요기요 화면에서 상호 후보 매칭과 파일명 리네임은 확장 리로드 후 강원영월점 1계정으로 확인 필요.

## 2026-08-11 요기요 매장 후보 진단 로그 강화
- 대상: OneDrive 확장 `doridang_collector_개발용`.
- 변경: 요기요 계정별 대상 매장 목록과 드롭다운 후보 전체의 이름/화면 ID/상태/브랜드/점수/매칭 사유를 라이브 로그로 출력하도록 보강.
- 검증 결과: `content/08_yogiyo.js`, `runner_yogiyo.js` `node --check` 통과, UTF-8 읽기 확인.
- 남은 위험: 나홀로 자동 포함은 아직 정책 변경 전이며, 다음 실전 로그에서 후보 노출 여부를 보고 별도 보정 필요.

## 2026-08-11 요기요 화면 후보 기준 수집 확장
- 대상: OneDrive 확장 `doridang_collector_개발용`.
- 변경: 로그인 후 요기요 드롭다운 후보를 먼저 조회해 `도리당/나홀로` 계약완료 매장을 모두 수집 대상으로 확장하고, 매장 선택 클릭 실패 시 재클릭하도록 보강.
- 검증 결과: `content/08_yogiyo.js`, `runner_yogiyo.js` `node --check` 통과, UTF-8 읽기 확인.
- 남은 위험: 실제 화면에서 나홀로 포함 2개 이상 파일 생성과 광명철산점 선택 반영은 확장 리로드 후 실전 확인 필요.

## 2026-08-12 Airflow 자동기동 복구
- 대상: Windows 로그인 시 `localhost:8080` Airflow 기동.
- 변경: Docker Desktop 준비를 기다린 뒤 `docker compose up -d`와 `/health` 확인을 수행하는 `scripts/start_airflow_on_login.ps1` 자동기동 스크립트를 추가하고 사용자 시작프로그램 바로가기를 등록.
- 검증 결과: 현재 세션에서 자동기동 스크립트 실행 exit 0, Airflow webserver 8080 매핑과 `/health` 200 응답 확인.
- 남은 위험: 실제 재부팅/로그인 1회로 시작프로그램 실행 성공 여부를 확인해야 함.

## 2026-08-12 Docker/Airflow 새벽 감시 작업 추가
- 대상: Windows 작업스케줄러 Docker/Airflow 복구 감시.
- 변경: 매일 01:00에 숨김 VBS 런처로 `scripts/start_airflow_on_login.ps1`을 실행하는 `DoridangDockerAirflowWatchdog` 등록 스크립트를 추가.
- 검증 결과: `schtasks` 등록 성공, 다음 실행 `2026-08-13 01:00:00`, 작업 상태 `Ready`, 현재 `/health` 200 확인.
- 남은 위험: 실제 01:00 실행 로그는 다음 스케줄 이후 `.tmp/autostart/airflow-autostart.log`에서 확인 필요.

## 2026-08-11 요기요 후보 조회 라우팅 수정
- 대상: OneDrive 확장 `doridang_collector_개발용`.
- 변경: `content/05_main.js`가 `yogiyoMode`를 content 수집기로 전달하도록 보정하고, 후보 조회 실패 시 계정 파일 fallback 다운로드를 중단하도록 수정.
- 검증 결과: `content/05_main.js`, `runner_yogiyo.js`, `content/08_yogiyo.js` `node --check` 통과.
- 남은 위험: 확장 리로드 전 열린 대시보드/콘텐츠 스크립트는 이전 라우팅을 유지하므로 반드시 새로고침 후 확인 필요.

## 2026-08-11 메뉴계층 수익률 키 추적 보완
- 대상: `DB_MenuHierarchy_Test` 메뉴계층 산출물과 수익률 입력표.
- 변경: 수익률 마스터에서 `기존수익키` 노출을 제거하고 `수익채널+수익키`만으로 채널별 수익률을 추적하도록 정리, OKPOS 날짜별 주문예외 키와 반반/닭추가 자동 보정을 강화.
- 검증 결과: 메뉴계층 단위 테스트 69개 통과, `DB_MenuHierarchy_Test_Dags` run `manual__codex_profit_key_no_vars_2026-08-11T10-02-00` 성공, 수익키/수익채널 공백 0 및 차단 이슈 0 확인.
- 남은 위험: `수익률_manual` 285건은 의도된 사용자 입력대기 상태이며 입력 전 `수기수익`은 산출되지 않음.

## 2026-08-11 송파삼전점 수익률·닭소모량 100% 분류 구현
- 대상: `modules/transform/pipelines/db/DB_MenuHierarchy_Test.py`, 메뉴계층 단위 테스트.
- 변경: `수익매출` 기준 수기수익, 4축 수익키, 반반/저표본 닭비율, 메뉴중량 닭사용량 시드, 수익키 검증 가드를 추가.
- 검증 결과: `pytest tests/test_menu_hierarchy_no_model_classification.py tests/test_menu_hierarchy_dag_defaults.py -q --basetemp=C:\airflow\.tmp\pytest -p no:cacheprovider` 79개 통과, 모듈/DAG import 및 핵심 헬퍼 검증 통과.
- 남은 위험: OneDrive 산출물 덮어쓰기 실행은 승인 전이라 미실행. 운영 `build_orders(None)` 후 수익대상 17,187행·잔여 33행·완결률 100%를 재확인해야 함.

## 2026-08-11 메뉴계층 DAG 재실행 및 신규패턴 알림
- 대상: `DB_MenuHierarchy_Test_Dags`, `DB_MenuHierarchy_Test.py`, 메뉴계층 산출물.
- 변경: 자동 분류 차단 이슈나 100% 미만 분류 차원이 새 패턴으로 등장하면 Telegram 알림을 보내고 `_system/classification_pattern_alerts.json`에 서명을 기록하도록 추가.
- 검증 결과: 단위 테스트 81개 통과, DAG run `manual__codex_reclass_alert_20260811_2204` 성공, 자동 분류 10개 축 전부 100%, 차단 이슈 0건.
- 남은 위험: `profit_missing`, `manual_profit_missing`, `completeness_regression`은 수익률·재료단가 입력 대기 WARN으로 유지하며 자동 재분류 대상이 아님.

## 2026-08-11 메뉴계층 분류룰 미해결 표시 정리
- 대상: `DB_MenuHierarchy_Test.py`, 메뉴계층 단위 테스트.
- 변경: 확정 분류 행의 `분류룰`에서 내부 placeholder `미해결` 판정 토큰을 제외해 실제 미해결사유가 없는 옵션행이 미해결처럼 보이지 않도록 수정.
- 검증 결과: `pytest tests/test_menu_hierarchy_no_model_classification.py tests/test_menu_hierarchy_dag_defaults.py -q --basetemp=C:\airflow\.tmp\pytest -p no:cacheprovider` 82개 통과, `py_compile` 통과.
- 남은 위험: OneDrive 산출물 재생성은 별도 승인 후 실행 필요.

## 2026-08-11 메뉴계층 루트 산출물 정리
- 대상: `DB_MenuHierarchy_Test.py`, `new_classification_orders` 산출 정책.
- 변경: 루트에는 `01_수기입력.xlsx`, `02_최종주문.csv`, `03_요약.xlsx`, `04_사용법.md`만 남기고 검증/마스터 CSV는 엑셀 시트와 `_debug/latest`에만 보관하도록 정리.
- 검증 결과: 메뉴계층 테스트 82개 통과, `py_compile` 통과.
- 남은 위험: 기존 OneDrive 루트 CSV 파일은 승인 후 재실행 또는 정리해야 실제 폴더에서 사라짐.

## 2026-08-12 메뉴계층 홀 수익키 닭유형 잠금 보정
- 대상: `DB_MenuHierarchy_Test.py`, 메뉴계층 수익률 마스터.
- 변경: 닭칼국수·닭개장·메밀 물 막국수 계열에서 지원하지 않는 순살 옵션 신호가 메인 수익키를 순살로 분리하지 않도록 고정 프로필과 수익키 닭유형 보정을 추가.
- 검증 결과: 메뉴계층 단위 테스트 92개 통과, `build_orders(debug_outputs=True)` 성공, `DB_MenuHierarchy_Test_Dags.build_orders` Airflow task test SUCCESS, 대상 메뉴 불가능 순살 수익키 0건 확인.
- 남은 위험: `profit_missing`, `manual_profit_missing`, `completeness_regression`은 수익률·재료단가 입력 대기 WARN으로 유지.

## 2026-08-12 메뉴계층 메뉴닭프로필 마스터 적용
- 대상: `DB_MenuHierarchy_Test.py`, `01_수기입력.xlsx` 메뉴닭프로필 시트, 수익률 마스터.
- 변경: 메뉴별 허용 닭유형·기본 닭유형·사이즈·옵션 적용 여부 마스터를 추가하고, 주문 옵션 신호는 메뉴 허용 범위 안에서만 수익키에 반영되도록 변경.
- 검증 결과: 메뉴계층 단위 테스트 94개 통과, `build_orders(debug_outputs=True)` 성공, `DB_MenuHierarchy_Test_Dags.build_orders` Airflow task test SUCCESS, 닭칼국수·닭개장·닭한마리 칼국수 정식 순살키 및 닭떡볶이 뼈닭키 0건 확인.
- 남은 위험: `profit_missing`, `manual_profit_missing`, `completeness_regression`은 수익률·재료단가 입력 대기 WARN으로 유지.

## 2026-08-12 메뉴계층 주문서 기준 수익키 보정
- 대상: `DB_MenuHierarchy_Test.py`, 수익률 마스터 수익키/참고 컬럼.
- 변경: 원천 주문서에 뼈/순살 신호가 없으면 수익키에서 닭유형 축을 제외하고, 메뉴명·표준명·품목명에 보이는 순살/뼈 신호는 유지하도록 보정. 계산용 닭유형·사이즈와 주문서 참고값을 수익률 마스터 별도 컬럼으로 노출.
- 검증 결과: `py_compile` 통과, `pytest tests/test_menu_hierarchy_no_model_classification.py -q --basetemp=.tmp\pytest -p no:cacheprovider` 95개 통과. Windows OneDrive analytics 경로로 `build_orders(debug_outputs=True)` 성공, 2026-04~2026-08 산출물 갱신.
- 남은 위험: `profit_missing`, `manual_profit_missing`, `completeness_regression`은 수익률·재료단가 입력 대기 WARN으로 유지.

## 2026-08-12 Flow 방문일지 점주 이해·todo fact 개편
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_viz.py`, `flow_visit_prompts.py`, `paths.py`, `docs/db-schema.md`, `tests/test_flow_visit_mart.py`.
- 변경: `owner_sentiment`를 점수/라벨이 아닌 현재 점주 인식·태도 문장으로 바꾸고, 누적 점주 특성·최근 화두·문제/고민·명시 요청사항 기준을 재정의. Power BI 관계키와 신규 `flow_visit_todo` fact 저장 구조를 추가.
- 검증 결과: `py_compile` 통과, `PYTHONPATH=. pytest -q -p no:cacheprovider tests/test_flow_visit_mart.py` 10건 통과.
- 남은 위험: OneDrive `Flow_mart/Flow_visit` 산출물 재생성과 Power BI 관계 재연결은 사용자 승인 후 별도 실행 필요.

## 2026-08-12 Flow 방문일지 mart v7 승인 재생성
- 대상: `Sales_FlowVisit_01_Mart_Dags`, OneDrive `Flow_mart/Flow_visit` 산출물.
- 변경: 사용자 승인 후 DAG run `manual__codex_flow_visit_todo_final_20260812T171500`으로 v7 점주 이해·todo fact 기준 산출물을 재생성.
- 검증 결과: DAG 전체 task success. 산출물 `log=(10,30)`, `issue=(96,28)`, `profile=(2,20)`, `viz=(96,53)`, `todo=(25,22)` 확인. `prompt_version=flow_visit_v7_owner_profile_todo`, `schema_version=flow_visit_profile_todo_v1`, 관계키 결측 0, `todo_id` 중복 0, UTF-8 replacement 문자 0건.
- 남은 위험: Power BI 모델에서 신규 `flow_visit_todo` 및 `store_rel_key`/`visit_rel_key`/`issue_rel_key` 관계 연결은 별도 새로고침·모델 편집 필요.

## 2026-08-12 Flow 방문일지 히스토리 프로필 v8
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_prompts.py`, `paths.py`, `docs/db-schema.md`, `tests/test_flow_visit_mart.py`.
- 변경: 매장별 누적 `history_digest` 기반 프로필 합성 레이어와 로컬 프로필 캐시를 추가. 기본 provider는 `off`로 유지하고 `local`/`openai` 선택 시에만 히스토리 LLM 합성을 시도하도록 분리.
- 검증 결과: `py_compile` 통과, `PYTHONPATH=C:\airflow pytest -q -p no:cacheprovider --basetemp=.tmp\pytest tests/test_flow_visit_mart.py` 11건 통과.
- 남은 위험: v8 OneDrive 산출물 재생성과 DAG 실행은 별도 승인 후 필요. OpenAI provider는 현재 환경에 `openai` 패키지가 없어 운영 전 패키지/API 키 검증 필요.

## 2026-08-12 Flow 방문일지 local gpt-oss 프로필 v9
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_prompts.py`, `docs/db-schema.md`, `tests/test_flow_visit_mart.py`.
- 변경: local 프로필 합성에서 qwen 계열을 완전히 제외하고 `gpt-oss` 후보만 사용하도록 고정. local 입력은 후보 ID 기반 압축 digest로 줄이고, 화두·요청·todo는 코드 산출을 우선하도록 잠금.
- 검증 결과: `py_compile` 통과, `PYTHONPATH=C:\airflow pytest -q -p no:cacheprovider --basetemp=.tmp\pytest tests/test_flow_visit_mart.py` 14건 통과. `FLOW_VISIT_PROFILE_LLM_MODELS=qwen2.5:14b,gpt-oss:20b` 설정에서도 qwen 제외 확인.
- 남은 위험: OneDrive mart 재생성과 실제 DAG local provider 실행은 별도 승인 후 필요.

## 2026-08-12 Flow 방문일지 방문별 프로필 스냅샷 추가
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_viz.py`, `paths.py`, `docs/db-schema.md`, `tests/test_flow_visit_mart.py`.
- 변경: 최신 `flow_store_profile`은 유지하고 방문 1건당 `flow_visit_profile_snapshot` 1행을 추가. 같은 달 여러 방문은 `월초~방문일`, `이전 방문 다음날~방문일`로 period를 분리하고, viz는 `visit_rel_key` 기준 스냅샷을 우선 사용하도록 변경.
- 검증 결과: `py_compile` 통과, `PYTHONPATH=C:\airflow pytest -q -p no:cacheprovider --basetemp=.tmp\pytest tests/test_flow_visit_mart.py` 15건 통과. 기존 mart 읽기 전용 시뮬레이션에서 profile 2건, snapshot 10건 및 월내 구간 분리 확인.
- 남은 위험: OneDrive mart 재생성과 Power BI 신규 테이블/필드 연결은 별도 승인 후 필요.

## 2026-08-12 메뉴계층 점심 2인이상 수익키 사이즈 축 보정
- 대상: `DB_MenuHierarchy_Test.py`, 수익률 마스터 수익키 사이즈 축.
- 변경: `2인이상` 점심류는 계산 사이즈가 `3인` 등으로 흔들려도 수익키 입력 행은 메뉴 기본 사이즈로 유지하고, 상차림 관련 `*_manual` 컬럼은 자동 산출하지 않도록 회귀 테스트 추가.
- 검증 결과: `py_compile` 통과, `PYTHONPATH=C:\airflow pytest tests/test_menu_hierarchy_no_model_classification.py -q --basetemp=.tmp\pytest -p no:cacheprovider` 96개 통과.
- 남은 위험: OneDrive `27_profit_rate_master.csv` 재생성은 별도 승인 후 필요.

## 2026-08-12 Flow 방문일지 v9 스냅샷 DAG 재생성
- 대상: `Sales_FlowVisit_01_Mart_Dags`, OneDrive `Flow_mart/Flow_visit` 산출물.
- 변경: 사용자 재실행 요청에 따라 `FLOW_VISIT_PROFILE_PROVIDER=local`, `FLOW_VISIT_PROFILE_LLM_MODELS=gpt-oss:20b`로 v9 방문별 프로필 스냅샷 및 todo 산출물을 재생성.
- 검증 결과: DAG 전체 success. 산출물 `log=10`, `issue=96`, `followup=6`, `profile=2`, `profile_snapshot=10`, `todo=25`, `viz=96` 확인. snapshot 관계키 결측 0, 중복 0, `todo_id` 중복 0, UTF-8 replacement 0건.
- 남은 위험: Power BI에서 `flow_visit_profile_snapshot` 신규 테이블/필드 새로고침과 관계·시각화 반영은 별도 확인 필요.

## 2026-08-12 DB_MenuHierarchy_Test 수익키 정체성 및 재실행 검증
- 대상: `DB_MenuHierarchy_Test.py`, `DB_MenuHierarchy_Test_Dags`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 세트/단품 정체성이 다른 `item_name`/`std_menu_name` 조합은 메인 수익키에서 원본 품목명을 우선하도록 보정하고, `discount/fee/정산제외`성 TMP 행은 상품 미등록 차단 검증에서 제외.
- 검증 결과: `py_compile` 통과, 메뉴계층 테스트 105개 통과. DAG `manual__codex_profit_key_identity_gap_fix_20260812T2135` success, 최종 산출물 갱신 및 차단 이슈 0건 확인.
- 남은 위험: `parent_child_size_conflict` 148건, `parent_child_chicken_type_conflict` 70건, `profit_missing` 15966건은 WARN으로 남아 수기 원가/프로필 검토 대상.

## 2026-08-12 DB_MenuHierarchy_Test 부모옵션 분류 보정
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 사이즈 옵션을 메뉴명 기본값보다 우선 추론하고, 닭미사용 main 아래 잘못 붙은 사이즈/닭유형 옵션은 같은 주문의 직전 닭 main으로 재부모화. 자동생성 `판정옵션` 행은 이전 오판 값을 보존하지 않고 재계산하며, 고정 메뉴프로필은 부모-자식 옵션 충돌 WARN에서 제외.
- 검증 결과: `py_compile` 통과, `PYTHONPATH=C:\airflow pytest tests/test_menu_hierarchy_no_model_classification.py -q --basetemp=.tmp\pytest -p no:cacheprovider` 108개 통과.
- 남은 위험: OneDrive 산출물과 WARN 카운트 감소 여부는 DAG 재실행 승인 후 확인 필요.

## 2026-08-12 DB_MenuHierarchy_Test 수익키 세트/닭미사용 재보정
- 대상: `DB_MenuHierarchy_Test.py`, `DB_MenuHierarchy_Test_Dags`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: std 메뉴명이 세트인 main은 원본 품목명이 단품처럼 보여도 세트 수익키를 유지하고, `닭미사용` 행은 `순살/뼈/혼합` 텍스트 신호가 보여도 수익키 닭유형 축에 넣지 않도록 보정.
- 검증 결과: `py_compile` 통과, 메뉴계층 테스트 109개 통과. DAG `manual__codex_profit_key_set_nonchicken_fix_20260812T2248` success, OneDrive 산출물 23:02 갱신 확인.
- 남은 위험: 수기 원가 미입력으로 `manual_profit_missing` 4개 source WARN과 `profit_missing` 15871건은 남아 있으며, 수익률 자동 계산은 판매가/원가 입력 후 반영된다.

## 2026-08-13 배민 ad_funnel 0원 정상 보정
- 대상: `DB_Beamin_05_ad_funnel.py`, `DB_Beamin_combined.py`, `DB_Beamin_Macro_upload.py`, `tests/test_baemin_ad_funnel_states.py`.
- 변경: ad_funnel `parse_error`/빈값/미생성 행이 같은 brand/store의 배민 주문 0원으로 확인되면 `collection_status=zero_sales`와 0 지표로 저장하고 retry 잔여 실패에서 제외.
- 검증 결과: 컨테이너 import 통과, UTF-8 확인 통과, `python -m pytest --basetemp C:\airflow\.tmp\pytest-basetemp tests/test_baemin_ad_funnel_states.py tests/test_beamin_retry_conf.py tests/test_baemin_final_notification.py tests/test_baemin_pc2_upload_trigger.py` 63개 통과.
- 남은 위험: 2026-08-12 운영 산출물의 `zero_sales` 보정 반영은 analytics 파일 수정이므로 별도 승인 후 validate/retry 재실행 필요.

## 2026-08-13 DB_MenuHierarchy_Test 수익률 닭유형 축 보정
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 순살 전용 메뉴프로필이 기존 판정옵션의 `혼합/뼈` 값을 덮어쓰도록 보정하고, 사이즈 옵션 수익키는 `닭옵션키`의 순살 신호를 토큰 단위로 반영.
- 검증 결과: `py_compile` 통과, `pytest ...test_menu_hierarchy_no_model_classification.py -p no:cacheprovider --basetemp .tmp/pytest-menu-hierarchy-full` 111개 통과. 컨테이너 재생성 후 한우 홀 혼합/뼈 수익키 0건, 미나리 중 공란 수익키 0건, 차단 이슈 0건 확인.
- 남은 위험: Windows 직접 실행은 Airflow 설정 인코딩과 Linux 경로 상수 때문에 부적합하다. 수기 원가 미입력 WARN과 부모-자식 속성 WARN은 별도 검토 대상.

## 2026-08-13 요기요 정산 다운로드 판정 보강
- 대상: OneDrive 크롬확장 `runner_yogiyo.js`, `background.js`.
- 변경: 요기요 다운로드 성공 판정을 시작시간·매장명·월 suffix 기준으로 제한하고, 다운로드 예약/매칭/해제 로그와 이벤트 미수신 메시지를 구체화.
- 검증 결과: `node --check`로 `runner_yogiyo.js`, `background.js`, `content/08_yogiyo.js` 문법 통과.
- 남은 위험: 실제 크롬 확장 리로드 후 실패 계정 재시도와 쿠팡 러너 동시 실행 차단 운영 확인 필요.

## 2026-08-13 요기요 직접 다운로드 버튼 및 로그인 재시도 보정
- 대상: OneDrive 크롬확장 `content/08_yogiyo.js`, `runner_yogiyo.js`.
- 변경: 1단계 버튼 없이 직접 `다운로드` 버튼이 뜨는 월을 매출 없음으로 오판하지 않도록 보정하고, 로그인 완료 대기 타임아웃 시 1회 재로그인 시도를 추가.
- 검증 결과: `node --check`로 `content/08_yogiyo.js`, `runner_yogiyo.js`, `background.js` 문법 통과, UTF-8 읽기 확인 통과.
- 남은 위험: 확장 리로드 후 기흥테라타워점 2026.04와 대전관저점 로그인 재시도 실전 확인 필요.

## 2026-08-13 요기요 전체매장수집 버튼 추가
- 대상: OneDrive 크롬확장 `runner_yogiyo.html`, `runner_yogiyo.js`.
- 변경: 대시보드에 `전체매장수집` 버튼을 추가하고, 기존 선택 수집은 선택 계정만 실행하도록 `selected` 모드로 분리. `all` 모드는 전체 요기요 계정 그룹을 실행하도록 보정.
- 검증 결과: `node --check runner_yogiyo.js` 통과, HTML/JS 버튼 ID 연결 및 UTF-8 읽기 확인.
- 남은 위험: 확장 리로드 후 버튼 표시와 선택 없이 전체 63개 계정 시작 여부 실전 확인 필요.

## 2026-08-13 요기요 인증 메일 Alert 자동 처리
- 대상: OneDrive 크롬확장 `runner_yogiyo.js`.
- 변경: 로그인 대기 중 `인증 메일 확인이 완료되지 않았습니다` Alert가 뜨면 `확인` 버튼을 클릭하고, 기존 AUTO_LOGIN을 1회 재전송하도록 보정.
- 검증 결과: `node --check runner_yogiyo.js` 통과, UTF-8 읽기 확인.
- 남은 위험: 실제 인증 Alert 계정에서 확인 클릭 후 로그인 성공 여부 실전 확인 필요.

## 2026-08-13 요기요 다운로드 실존 검증 및 noData 재시도 보강
- 대상: OneDrive 크롬확장 `runner_yogiyo.js`, `background.js`.
- 변경: Chrome 다운로드 완료 이력의 `exists/fileSize/totalBytes`를 검증해 실제 파일 없음·0바이트를 성공 처리하지 않고, 1단계/다운로드 버튼 없음은 매출 없음 성공이 아니라 재시도 실패로 남기도록 보정.
- 검증 결과: `node --check runner_yogiyo.js`, `node --check background.js` 통과, UTF-8 읽기 확인.
- 남은 위험: Chrome 다운로드 위치가 실제 존재하는 `E:\d_down` 또는 `E:\down`으로 설정됐는지 확장 리로드 후 실전 확인 필요.

## 2026-08-13 요기요 인증 메일 2차 팝업 클릭 보강
- 대상: OneDrive 크롬확장 `runner_yogiyo.js`.
- 변경: 인증 메일 Alert 1차 확인 후 3초 대기하고, 이어 뜨는 `size=48/color=primaryA` 확인 버튼 또는 내부 div를 최대 7초 탐색해 클릭하도록 보강.
- 검증 결과: `node --check runner_yogiyo.js` 통과, UTF-8 읽기 확인.
- 남은 위험: 실제 인증 Alert 계정에서 2차 팝업 클릭 후 재로그인 성공 여부 실전 확인 필요.

## 2026-08-13 요기요 다운로드 버튼 없음 매출없음 처리
- 대상: OneDrive 크롬확장 `runner_yogiyo.js`.
- 변경: 정산 화면에서 1단계/다운로드 버튼이 없어 content script가 `noData`를 반환하면 월 실패로 기록하지 않고 `매출 없음, 다음 단계로 넘어감`으로 정상 진행하도록 보정.
- 검증 결과: `node --check runner_yogiyo.js` 통과, UTF-8 읽기 확인.
- 남은 위험: 버튼 클릭 후 파일이 실제로 생성되지 않는 자동 다운로드 차단 케이스는 계속 실패 처리되므로 다운로드 위치/권한은 별도 확인 필요.

## 2026-08-13 요기요 나홀로 누락 및 다운로드 실존 확인 보강
- 대상: OneDrive 크롬확장 `runner_yogiyo.js`, `background.js`.
- 변경: 다운로드 이력의 `exists`가 명시적으로 `true`이고 파일 크기가 양수인 경우만 저장 완료로 인정하도록 강화해 실제 없는 `D:\down` 이력 성공 처리를 차단.
- 검증 결과: `node --check runner_yogiyo.js`, `node --check background.js` 통과, UTF-8 읽기 확인.
- 남은 위험: 크롬 확장 리로드 후 실제 다운로드 폴더가 `E:\d_down` 또는 사용 중인 Chrome 기본 다운로드 위치와 일치하는지 실전 확인 필요.

## 2026-08-13 Flow 방문일지 컬럼 중복 정리
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_viz.py`, `flow_visit_prompts.py`, `tests/test_flow_visit_mart.py`.
- 변경: `owner_status` 기준화, `store_status_summary` profile 저장 추가, `manager_memo`/`manager_request` 제거, `handling_points` 최신 방문 기준으로 조정.
- 검증 결과: `py_compile` 통과, `tests/test_flow_visit_mart.py` 15개 통과, 기존 품질평가 recall 100% 유지.
- 남은 위험: OneDrive parquet 재생성은 미실행이며, 운영 DAG 재실행 전 Power BI 제거 컬럼 참조 여부 확인 필요.

## 2026-08-13 Flow 방문일지 schema v3 재실행
- 대상: `Sales_FlowVisit_01_Mart_Dags`, OneDrive `Flow_mart/Flow_visit` 산출물.
- 변경: `manual__codex_flow_visit_schema_v3_20260813T114700` 실행으로 schema v3 컬럼 정리 산출물을 재생성.
- 검증 결과: DAG success, profile 2행·viz 96행·todo 25행·snapshot 10행·log 10행 확인, 제거 컬럼 미노출 및 품질평가 recall 100% 유지.
- 남은 위험: profile provider는 `off`라 API 테스트 전 rule fallback 산출물이며, Power BI 제거 컬럼 참조 여부 확인 필요.

## 2026-08-13 Flow 방문일지 LLM 프롬프트 자율성 보정
- 대상: `flow_visit_prompts.py`, `tests/test_flow_visit_mart.py`.
- 변경: 프로필 프롬프트를 v10으로 올리고, 컬럼 역할·기간 기준은 유지하되 후보 사용을 과도하게 제한하는 문장을 제거해 LLM이 업무 문장으로 자연스럽게 요약하도록 조정.
- 검증 결과: `py_compile` 통과, `tests/test_flow_visit_mart.py` 15개 통과, 기존 품질평가 recall 100% 유지.
- 남은 위험: OneDrive mart 재생성은 미실행이며, provider `off` 상태에서는 프롬프트 변경이 산출물 품질에 직접 반영되지 않는다.

## 2026-08-13 메뉴계층 수기입력 유실 방지
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 원본 0건 시 `01_수기입력.xlsx` 덮어쓰기 중단, workbook 쓰기 전 수기 컬럼 유실 가드와 로컬 temp 백업 추가, 수익률 미매칭 금액 입력은 `수익률_미매칭보존` 시트로 보존.
- 검증 결과: `py_compile` 통과, `tests/test_menu_hierarchy_no_model_classification.py` 114개 통과.
- 남은 위험: 기존에 이미 사라진 `수익률` 금액 manual 값은 로직으로 복원할 수 없어 재입력 필요.

## 2026-08-13 Flow 방문일지 prompt v10 재실행
- 대상: `Sales_FlowVisit_01_Mart_Dags`, OneDrive `Flow_mart/Flow_visit` 산출물.
- 변경: `manual__codex_flow_visit_prompt_v10_20260813T123600` 실행으로 v10 프롬프트 버전 기준 mart 산출물을 재생성.
- 검증 결과: DAG success, profile 2행·viz 96행·todo 25행·snapshot 10행·log 10행 확인, `owner_sentiment`/`manager_memo`/`manager_request` 미노출 및 품질평가 recall 100% 유지.
- 남은 위험: profile provider는 `off`라 API/로컬 프로필 LLM 품질은 아직 미반영이며, API 연결 후 v10 캐시 신규 생성 여부를 재확인해야 함.

## 2026-08-13 Flow 방문일지 handling_points 전기간 기준 전환
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_prompts.py`, `tests/test_flow_visit_mart.py`.
- 변경: `handling_points`를 최신 방문 1건이 아닌 전기간 누적 히스토리의 현재 문제·고민 기준으로 전환하고, prompt version을 v11로 갱신.
- 검증 결과: `py_compile` 통과, `tests/test_flow_visit_mart.py` 16개 통과.
- 남은 위험: OneDrive mart 재생성은 승인 전이라 미실행이며, Power BI 산출물에는 DAG 재실행 후 반영됨.

## 2026-08-13 Flow 방문일지 handling_points v11 재실행
- 대상: `Sales_FlowVisit_01_Mart_Dags`, OneDrive `Flow_mart/Flow_visit` 산출물.
- 변경: 사용자 승인 후 `manual__codex_flow_visit_handling_v11_20260813T152900`으로 v11 산출물을 재생성.
- 검증 결과: DAG success, profile 2행·viz 96행·todo 25행·snapshot 10행·log 10행 확인, `prompt_version=flow_visit_v11_handling_full_history` 및 제거 컬럼 미노출 확인.
- 남은 위험: profile provider는 `off`라 API/로컬 프로필 LLM 결과가 아니라 rule fallback 기준 산출물임.

## 2026-08-13 쿠팡 지정기간 전체 주문서 버튼 추가
- 대상: `coupang_extension_build/runner.html`, `runner.js`, `content/05_main.js`, `content/03_coupangeats.js`.
- 변경: 대시보드 헤더에 시작일/종료일 입력과 지정기간 주문서 버튼을 추가하고, batch 주문서 수집이 `targetStartDate~targetEndDate` 범위를 적용하도록 보정.
- 검증 결과: `node --check`로 `runner.js`, `content/05_main.js`, `content/03_coupangeats.js` 문법 통과, UTF-8 replacement 0건 확인.
- 남은 위험: OneDrive 개발용 확장은 승인 전이라 미반영이며, Chrome 확장 리로드 후 실제 지정기간 전체 수집 실전 확인 필요.

## 2026-08-13 쿠팡 지정기간 전체 주문서 버튼 OneDrive 반영
- 대상: OneDrive `Extention/doridang_collector_개발용` 확장 파일 4개.
- 변경: 승인 후 저장소의 `runner.html`, `runner.js`, `content/05_main.js`, `content/03_coupangeats.js`를 개발용 확장에 동일 반영.
- 검증 결과: OneDrive 대상 `runner.js`, `content/05_main.js`, `content/03_coupangeats.js` `node --check` 통과, 저장소와 바이트 동일 및 UTF-8 replacement 0건 확인.
- 남은 위험: Chrome 확장 리로드 후 브라우저에서 지정기간 전체 주문서 수집 버튼 실전 확인 필요.

## 2026-08-14 메뉴계층 OKPOS 0원 기본 사이즈 옵션 보정
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 같은 메뉴에 유료 `[대]`와 0원 기본 `[중]` 사이즈가 같이 있으면 최종 사이즈 `대` 기준으로 수익률 옵션 표시와 닭옵션키에서 0원 기본 사이즈를 제외하고, 실비파김치 계열을 순살 메뉴프로필로 제안. 같은 수익키의 수기 금액 변경도 workbook 가드가 차단하도록 보강.
- 검증 결과: 승인 후 컨테이너 `build_orders(debug_outputs=True)` 재생성 완료. 백업 `/opt/airflow/Local_DB/temp/backups/menu_hierarchy_manual_input/20260814_122144/01_수기입력.xlsx` 대비 수익률계 수기금액 30행/금액 카운트 동일, 대상 주문 이슈 0건 확인. 메뉴계층 단위 테스트 122개 통과, 별도 `AIRFLOW_HOME` 지정 DAG import 통과.
- 남은 위험: 입력 대기 WARN은 남아 있으나 수기 입력 대기 상태이며 이번 사이즈 중복 문제와 별개.

## 2026-08-14 For_AI 매장월 분석 JSON Timestamp 직렬화 보정
- 대상: `For_AI_store_month_analysis.py`, `tests/test_for_ai_store_month_analysis.py`.
- 변경: 방문일지 payload에 남은 `pd.Timestamp`/날짜형을 JSON 안전 문자열로 변환하도록 `_json_value`를 보강.
- 검증 결과: `tests/test_for_ai_store_month_analysis.py` 3개 통과, `py_compile` 통과, 컨테이너 DAG import 통과.
- 남은 위험: OneDrive 산출물 재생성은 승인 범위 밖이라 수행하지 않음.

## 2026-08-13 배민 ad_funnel parquet no_data 마커 보정
- 대상: `DB_Beamin_04_orders.py`, `tests/test_baemin_ad_funnel_states.py`.
- 변경: orders no_data 마커 판정이 `orders_no_data.csv`뿐 아니라 `.parquet`/`.pq`도 읽도록 보강해 ad_funnel 0원 정상 보정 누락을 방지.
- 검증 결과: `py_compile` 통과, `tests/test_baemin_ad_funnel_states.py`와 `tests/test_beamin_retry_conf.py` 31개 통과.
- 남은 위험: 운영 analytics의 기존 `parse_error` 행 zero_sales 보정은 OneDrive 수정 승인 후 별도 적용 필요.

## 2026-08-13 메뉴계층 홀 사이즈 옵션 우선 보정
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders/01_수기입력.xlsx`.
- 변경: 홀 주문의 명시 사이즈 옵션이 순살 전용 메뉴프로필 기본 사이즈보다 우선되도록 수정하고, Codex 자동채움 수기 사이즈가 명시 옵션과 충돌하면 자동 판정을 반영하도록 보강.
- 검증 결과: `py_compile` 통과, 메뉴계층 단위 테스트 118개 통과, `build_orders(debug_outputs=True)`로 `수익률` 재생성 완료. `갈비 찜닭 [순살] [대] 3인`과 `미나리 수삼 백숙 [대] 3인 | 순살`은 `대|순살`로 반영 확인.
- 남은 위험: `옵션없음` 홀 주문은 원본에 사이즈 옵션이 없어 기존 메뉴프로필/수기 기준으로 남는다.

## 2026-08-13 배민 ad_funnel 0원 정상 운영 보정
- 대상: `DB_Beamin_Macro_Dags_Retry.py`, `DB_Beamin_05_ad_funnel.py`, `DB_Beamin_combined.py`, OneDrive baemin_macro ad_funnel 산출물.
- 변경: orders 기준 0매출인 ad_funnel parse_error/date_missing을 `zero_sales`로 정상 처리하고, retry notify가 검증 통과 후 남은 `residual_failed.ads`를 실패로 재판정하지 않도록 보정.
- 검증 결과: 2026-08-12 parse_error 잔존 0건, zero_sales 28건 확인. 관련 테스트 53개와 py_compile 통과, 2026-08-12 대상 Retry run 3건 모두 success 확인.
- 남은 위험: 2026-08-07 lookback 과거 실패 run은 이번 2026-08-12 실패건 재시도 대상이 아니어서 미처리.

## 2026-08-13 메뉴계층 주문그룹 날짜키 보정
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders/01_수기입력.xlsx`.
- 변경: 동일 `order_id/menu_seq`가 날짜별로 재사용되는 OKPOS 홀 주문을 분리하도록 주문 그룹키에 `sale_date`를 포함하고, 메뉴중량 수기값 보존 로직을 보강.
- 검증 결과: `py_compile` 통과, 메뉴계층 단위 테스트 119개 통과, `build_orders(debug_outputs=True)` 재생성 성공. `한우 순살 곱도리탕 [중] 2인`은 `중|순살`로 합쳐지고 `1인|순살` 수익키는 제거됨.
- 남은 위험: 전체 빌드는 약 10분 소요되므로 이후 단건 오분류는 함수 단위 재현으로 먼저 검증해야 함.

## 2026-08-13 메뉴계층 유료 닭유형옵션 수익키 흡수
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders/01_수기입력.xlsx`.
- 변경: `닭유형/사이즈/맛선택` absorbed option이 유료라도 별도 메뉴 수익키를 만들지 않고 부모 main 수익키로 매출을 흡수하도록 보정.
- 검증 결과: `py_compile` 통과, 메뉴계층 단위 테스트 120개 통과, `build_orders(debug_outputs=True)` 재생성 성공. `메뉴|홀|갈비찜닭 반반세트|중|순살` 0건, `중|혼합` 매출 433460으로 흡수 확인.
- 남은 위험: 수익률/재료 입력 대기 WARN은 분류 문제가 아니라 수기 입력 대기 상태.
## 2026-08-16 DB_UnifiedSales EasyPOS ?? ??? ?? ??
- ??: `DB_UnifiedSales_easypos.py`
- ??: EasyPOS ??? ????? `[??]` ???? ??? `[??]??`? ?? `??` ????? ????? ??.
- ?? ??: ???? DAG import ??, `[??]??` ? `??` ? `1000037` ?? ??, 2026-08-14 EasyPOS 38? ?? ?? ??.
- ?? ??: EasyPOS ??? ?? ? ???? ??? ?? ??? ??? ?? ?? ??.

## 2026-08-18 물류담 통합 문서1 저장 후 닫기 보강
- 대상: `scripts/mulryudam_login_confirm.py`.
- 변경: 임시 Excel export 후보를 `통합 문서*`/`Book*`의 저장 경로 없는 미저장 워크북으로 제한하고, 물류담 소유 모달만 차단 모달로 처리하도록 보강.
- 검증 결과: `py_compile` 통과, 모듈 import 및 UTF-8 읽기 검증 통과.
- 남은 위험: 실제 수집 실행은 OneDrive parquet 저장을 동반하므로 별도 승인 후 검증 필요.

## 2026-08-18 쿠팡이츠 다운로드 경로 불일치 복구
- 대상: `scripts/coupang_host_chrome.ps1`, `scripts/register_coupang_autocollect_task.ps1`, `DB_CoupangMacro_Load_Dags`.
- 변경: `Downloads`에 남은 `coupangeats_*` 201개를 `E:\down`으로 이동 후 적재하고, 자동 Chrome 다운로드 경로를 `E:\down`으로 고정하도록 보강.
- 검증 결과: 수동 DAG run success, cleaned_count=201, orders 58개/5158행, cmg 76개/152행, options 67개/9167행 적재.
- 남은 위험: 이미 실행 중인 Chrome은 즉시 Preferences 교정이 불가하므로 다음 자동 실행 전 Chrome 완전 종료 후 재실행 필요.

## 2026-08-18 쿠팡이츠 이중 다운로드 경로 입력 보강
- 대상: `DB_CoupangMacro_load.py`, `docker-compose.yaml`, `tests/test_coupang_orders_validation.py`.
- 변경: 쿠팡 원천 로더와 비교 DAG 이동 태스크가 `E:\down`과 보조 Downloads 마운트 양쪽의 `coupangeats_*`를 모두 입력으로 보도록 보강.
- 검증 결과: 양쪽 경로 발견/이동 단위 테스트 추가, Python compile 및 대상 테스트 통과, Airflow 실행 컨테이너 재생성 후 `/opt/airflow/user_downloads` 인식 확인.
- 남은 위험: 기존 전체 쿠팡 테스트 파일에는 별도 기준선 실패가 남아 있어 이번 변경 대상 테스트만 분리 검증함.

## 2026-08-18 배민 정산예정금액 결측 가드 보강
- 대상: `DB_Beamin_04_orders.py`, `DB_DeliveryCommission.py`, 관련 테스트.
- 변경: 주문 합계가 맞아도 `입금예정금액` 수집률이 낮으면 저장 성공으로 처리하지 않고, delivery_commission 빌드 전 배민 정산 결측을 실패로 차단.
- 검증 결과: 배민 orders/commission 단위 테스트 35개, 배민 macro validation/recovery 테스트 20개 통과. 격리 `AIRFLOW_HOME`에서 관련 DAG import 통과.
- 남은 위험: 기존 OneDrive 원천의 결측 782건은 수정하지 않았으며, 상위/하위 재수집과 mart 재생성은 별도 승인 후 필요.

## 2026-08-18 네이버 광고 KPI 자동수집 버튼 추가
- 대상: OneDrive `doridang_collector_개발용` 크롬 확장.
- 변경: `ads.naver.com` 계정 1497096 캠페인 discover/다운로드 러너, 팝업 버튼, manifest 권한, `NAVERADS_*` 다운로드 감지를 추가.
- 검증 결과: 신규/수정 JS `node --check` 통과, manifest JSON/등록 확인 통과, `content/05_main.js` 옵션 화이트리스트 확인 통과.
- 남은 위험: 실제 네이버 광고 화면의 상세 데이터 버튼과 다운로드 메뉴 동작은 브라우저 E2E에서 최초 확인 후 셀렉터 보정 가능.

## 2026-08-18 네이버 광고그룹 다운로드 중복 저장 보정
- 대상: OneDrive `doridang_collector_개발용` 크롬 확장.
- 변경: 네이버 광고 클릭 헬퍼의 추가 `target.click()` 호출을 제거해 메뉴 클릭 중복 가능성을 줄이고, 저장 파일명을 `naverads_adgroups_*`로 변경.
- 검증 결과: `content/09_naverads.js`, `background.js` `node --check` 통과, `target.click` 제거 및 파일명 prefix 반영 확인.
- 남은 위험: 실제 Chrome 다운로드가 1건만 발생하는지는 확장 리로드 후 네이버 광고 단건 수집으로 확인 필요.

## 2026-08-18 네이버 광고그룹 빈 엑셀 방지 및 라이브로그 보강
- 대상: OneDrive `doridang_collector_개발용` 크롬 확장.
- 변경: 상세 페이지 광고그룹 데이터 행/요약/첫 행이 안정화된 뒤 다운로드하도록 대기 조건을 강화하고, 다운로드 시작/저장 이벤트의 원본·제안 파일명 로그를 추가.
- 검증 결과: `content/09_naverads.js`, `runner_naverads.js`, `background.js` `node --check` 통과, UTF-8 읽기 확인.
- 남은 위험: 네이버 상세 DOM 구조가 실제 행에 `grp-` 텍스트를 노출하지 않으면 첫 재실행 로그를 기준으로 행 감지 셀렉터 보정 필요.

## 2026-08-18 네이버 광고그룹 직접 다운로드 방식 고정
- 대상: OneDrive `doridang_collector_개발용` 크롬 확장.
- 변경: 다운로드 버튼 클릭 후 `대시보드` 메뉴를 추가 클릭하지 않고 direct-download로 완료 처리하며, 네이버 광고그룹 파일 저장은 동일 파일명 overwrite로 변경.
- 검증 결과: `content/09_naverads.js`, `runner_naverads.js`, `background.js` `node --check` 통과, 메뉴 클릭 로그 제거 및 overwrite 반영 확인.
- 남은 위험: 실제 화면에서 다운로드 버튼이 직접 다운로드가 아닌 메뉴 전용으로 바뀌면 다운로드 이벤트 미수신 로그를 기준으로 재보정 필요.

## 2026-08-18 배민 정산예정금액 감시 task 추가
- 대상: `DB_DeliveryCommission_Dags.py`, `DB_DeliveryCommission.py`, `tests/test_delivery_commission.py`.
- 변경: delivery_commission 빌드 전에 `monitor_baemin_settlement_missing` task를 추가하고, 결측 발견 시 `.tmp/baemin_settlement_missing_targets_latest.csv`와 orders-only 재수집 conf를 남긴 뒤 실패하도록 보강.
- 검증 결과: `tests/test_baemin_orders_cancelled_filter.py`, `tests/test_delivery_commission.py` 38개 통과. 격리 `AIRFLOW_HOME`에서 `DB_DeliveryCommission_Dags`, `DB_Beamin_Macro_Dags` import 통과.
- 남은 위험: Windows 직접 실행에서는 `BAEMIN_ORDERS_DB`가 컨테이너 경로(`/opt/airflow/...`)로 잡혀 실원천 목록 산출은 Airflow 컨테이너에서 확인 필요.

## 2026-08-18 네이버 광고그룹 정제 CSV 저장 전환
- 대상: OneDrive `doridang_collector_개발용` 크롬 확장.
- 변경: 네이버 광고 원본 XLSX 다운로드 대신 상세 화면 광고그룹 행을 직접 추출해 CSV로 저장하고, 집계행 제외, `store=송파삼전점`, `collected_date=yyyy-mm-dd`, 금액 컬럼 `원` 제거를 반영.
- 검증 결과: `content/09_naverads.js`, `runner_naverads.js` `node --check` 통과, manifest JSON 파싱 통과.
- 남은 위험: 실제 네이버 DOM에서 광고그룹 ID 셀이 숨겨진 경우 첫 재실행 로그의 `ADGROUP_ROWS_EXTRACT_EMPTY` 진단을 기준으로 셀렉터 보정 필요.

## 2026-08-18 네이버 광고 러너 시작 멈춤 보정
- 대상: OneDrive `doridang_collector_개발용` 크롬 확장.
- 변경: 네이버 광고 러너 시작 시 `ce_runner_stop_requested` STOP 래치를 초기화하고, 목록 탭 생성/로드/content 주입/discover 단계 로그 및 `COLLECT` 차단 응답 즉시 실패 처리를 추가.
- 검증 결과: `runner_naverads.js`, `content/05_main.js`, `content/09_naverads.js` `node --check` 통과, UTF-8 읽기 확인.
- 남은 위험: 실제 네이버 로그인/권한 페이지로 이동하는 경우 목록 로드 로그 이후 content 주입 또는 discover 오류 로그를 기준으로 추가 보정 필요.

## 2026-08-18 메뉴계층 다중 메인 닭옵션 순서 매칭 보정
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 한 주문의 여러 닭 메인에 닭유형/사이즈 옵션이 한 부모로 몰린 경우 메인 순서대로 `뼈닭`/`순살`과 사이즈를 배정하는 `주문순서매칭` 판정을 추가.
- 검증 결과: `py_compile` 통과, `tests/test_menu_hierarchy_no_model_classification.py` 124개 통과, 실제 debug 샘플 3개 주문에서 누룽지 백도리탕이 순살로 재계산됨.
- 남은 위험: 옵션 수와 메인 수가 맞지 않는 애매한 주문은 기존 혼합/비율추정 흐름으로 남아 수기 검토가 필요.

## 2026-08-18 Flow 방문일지 상단 대상 목록 보강
- 대상: `Sales_FlowVisit_01_Mart_Dags.py`, `SMP_flow_visit_mart.py`, `SMP_flow_store_collect.py`, `tests/test_flow_visit_mart.py`, `tests/test_flow_store_collect.py`.
- 변경: DAG 코드 상단 `FLOW_VISIT_TARGETS`에 기존 동탄영천점/용인동천점과 신규 대화점을 함께 두고, 방문일지 마트 대상 관리를 이 목록 기준으로 수행하도록 보강. 대상 일부가 원천/방문일지에 없으면 실패하도록 가드 추가하고, Flow Store 누락 파티션 복구는 `flow_repair_project_ids`로 대상 프로젝트만 재수집하게 제한.
- 검증 결과: `py_compile` 통과, `.tmp` 임시 경로 지정 후 `tests/test_flow_store_collect.py`와 `tests/test_flow_visit_mart.py` 36개 통과. 승인 후 `Strategy_FlowStore_01_Collect_Dags` 대상 복구 run success, `Sales_FlowVisit_01_Mart_Dags` 최신 run success.
- 남은 위험: 이전 광범위 복구 run 1건은 과다 수집 방지를 위해 실패 상태로 종료했으며, 최신 대상 복구 run과 방문일지 run은 정상 완료.

## 2026-08-18 메뉴계층 다중 메인 산출물 갱신
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_fin_product_map_review_input.csv`.
- 변경: 승인 후 배민수동 신규 사이즈 옵션 1건을 검수 입력에 추가하고, 검수 행 기반 item_id 매핑과 `[단짠단짠] 순살 갈비찜닭` alias, 순서매칭 parent-child WARN 제외를 반영.
- 검증 결과: `py_compile` 통과, `tests/test_menu_hierarchy_no_model_classification.py` 126개 통과, `build_orders(debug_outputs=True)` 워크북 제외 검증 통과, product_gap 0건.
- 남은 위험: `01_수기입력.xlsx`가 잠겨 마지막 전체 실행의 워크북 쓰기는 실패했으며, 파일을 닫은 뒤 재실행하면 반영 가능.

## 2026-08-18 메뉴계층 수기입력 워크북 갱신
- 대상: OneDrive `new_classification_orders/01_수기입력.xlsx`.
- 변경: 승인 후 `build_orders(debug_outputs=True)`를 원본 워크북 쓰기 포함으로 실행해 수익률 수기입력 파일을 갱신.
- 검증 결과: `수익률` 시트에서 `메뉴|홀_포장|누룽지 백도리탕|중|순살` 집계가 확인되며, 지적된 `뼈/순살` 다중 메인 케이스는 혼합 집계에서 제외됨.
- 남은 위험: CLI 세션이 타임아웃되어 후속 프로세스를 수동 종료했으나 워크북 저장 시각과 시트 읽기 검증은 완료됨.

## 2026-08-18 메뉴계층 취소 상계 원가율 보정
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 같은 일자/채널/결제금액의 정상-취소 주문을 상계 표시하고, 상계 정상/취소 및 미매칭 취소를 수익률·메뉴중량·원가율 대상에서 제외.
- 검증 결과: `py_compile` 통과, 취소상계 신규 테스트 4개 통과, `tests/test_menu_hierarchy_no_model_classification.py` 130개 통과.
- 남은 위험: OneDrive `01_수기입력.xlsx`는 이번 턴에서 덮어쓰지 않았으며, 파일 갱신은 별도 승인 후 실행 필요.

## 2026-08-18 메뉴계층 옵션없음 단가매칭 보정
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, 참고 파일 `도리당 송파삼전점 홀,배달 원가_홀만보기_260818.xlsx`.
- 변경: `옵션없음` 닭메뉴를 임의 기본값으로 채우지 않고, 같은 메뉴/채널의 메인+닭결정옵션 단가에서 이미 분류된 우세 조합이 있을 때만 `단가매칭`으로 보정.
- 검증 결과: `py_compile` 통과, 옵션없음/취소상계 신규 테스트 8개 통과, 전체 메뉴계층 테스트 135개 통과, 기존 debug 기준 34행 1,086,100원 단가매칭 확인.
- 남은 위험: OneDrive `01_수기입력.xlsx`는 이번 턴에서 덮어쓰지 않았으며, 파일 갱신은 별도 승인 후 실행 필요.

## 2026-08-18 메뉴계층 수기입력 워크북 승인 갱신
- 대상: OneDrive `new_classification_orders/01_수기입력.xlsx`, `DB_MenuHierarchy_Test.py`.
- 변경: 승인 후 옵션없음 단가매칭과 취소상계 보정을 포함해 `build_orders(debug_outputs=True)`를 원본 워크북 쓰기 포함으로 재실행. 단가매칭 판정은 원천 `닭유형_신호`와 분리하되 검증상 확정으로 인정하도록 보정.
- 검증 결과: 빌드 성공, `classification_unresolved` 0건, `04_product_gap.csv` 0행, 단가매칭 34행/1,086,100원, 취소상계 정상 273행·취소 44행·매칭없음 3행 확인. 실행 중 생성된 로컬 임시 백업 2개는 검증 후 삭제.
- 남은 위험: 단가 근거 부족/충돌로 남긴 `옵션없음+혼합` 197행/2,055,722원은 임의 분류하지 않았고, 취소 원주문을 특정할 수 없는 미매칭 취소 3건은 경고로 보존.

## 2026-08-18 메뉴계층 옵션없음 누적학습 보정
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: `옵션없음` 보정 순서를 품목명 신호 → 정확단가 → 주변단가로 확장하고, 수기/판정옵션/메뉴프로필 확정값을 다음 실행의 단가 근거 표본으로 사용하도록 보강. 주변단가 자동값은 재학습 근거에서 제외.
- 검증 결과: `py_compile` 통과, 옵션없음 테스트 10개 통과, 전체 메뉴계층 테스트 140개 통과. 최신 debug 메모리 적용 기준 정상 `옵션없음+혼합` 148행/3,776,122원 중 127행/2,539,000원 추가 해소.
- 남은 위험: OneDrive `01_수기입력.xlsx`는 이번 변경 후 아직 덮어쓰지 않았으며, 잔여 21행/1,237,122원은 반반/고액 세트 등 가격·텍스트 근거가 불충분해 혼합 유지.

## 2026-08-18 메뉴계층 포장 상차림비 0원 규칙
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: `홀_포장` 수익채널은 상차림비를 항상 0으로 정규화하고, 수익률 계산도 포장 행에서는 `상차림포함원가_manual` 대신 `메뉴원가_manual + 0` 기준으로 산출.
- 검증 결과: `py_compile` 통과, 수익률/포장 테스트 10개 통과, 전체 메뉴계층 테스트 142개 통과. 최신 debug 수익률표 메모리 적용 기준 포장 125행의 상차림비 nonzero 0건 확인.
- 남은 위험: OneDrive `01_수기입력.xlsx`는 이번 변경 후 아직 덮어쓰지 않았으며, 승인 후 재빌드 필요.

## 2026-08-18 메뉴계층 판매가·상차림포함원가 자동 계산
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: `수익률` 시트의 `판매가_manual`, `상차림포함원가_manual` 수기 입력 컬럼을 제거하고 `판매가`, `상차림포함원가` 자동 계산 컬럼으로 전환. 사용자가 채울 칸은 `메뉴원가_manual`, `상차림비_manual`만 남김.
- 검증 결과: `py_compile` 통과, 수익률/포장/워크북 보존 관련 테스트 13개 통과, 전체 메뉴계층 테스트 142개 통과. 최신 debug 수익률표 메모리 적용 기준 `판매가_manual`, `상차림포함원가_manual` 출력 컬럼 없음 확인.
- 남은 위험: 기존 OneDrive `01_수기입력.xlsx`에는 아직 이전 컬럼명이 남아 있으며, 승인 후 재빌드해야 새 컬럼 구조로 갱신됨.

## 2026-08-18 UnifiedSales 공통 모듈 import 복구
- 대상: `DB_UnifiedSales_common.py`, UnifiedSales/배민/쿠팡 수동 DAG import.
- 변경: 루트에 남아 있던 복구본을 정식 공통 모듈 경로로 반영해 `FULL_RECALC_STORES`, reingest/partial marker, atomic save 공용 API 누락을 복구.
- 검증 결과: 공통 API 누락 0건, `DB_UnifiedSales` import 및 관련 marker/partial 테스트 22개 통과, 컨테이너 `airflow dags list-import-errors` 0건.
- 남은 위험: 같은 이름의 루트 shadow copy가 다시 생기면 계약 테스트에서 차단하도록 후속 보강 필요.

## 2026-08-18 UnifiedSales 공통 모듈 계약 테스트 추가
- 대상: `tests/test_unified_sales_common_contract.py`, 루트 shadow copy 정리.
- 변경: DB 파이프라인의 `DB_UnifiedSales_common` import 이름과 필수 공개 API를 AST로 검증하고, 루트 `DB_UnifiedSales_common.py` 중복 파일을 차단하도록 추가.
- 검증 결과: 신규 계약 테스트 3개, 관련 marker/partial 테스트 22개 통과, 컨테이너 `airflow dags list-import-errors` 0건.
- 남은 위험: 수동 반영 시 이 테스트를 실행하지 않으면 운영 import-error watcher가 최후 방어선으로 남음.

## 2026-08-18 Flow 방문일지 하위업무 원천 테이블 추가
- 대상: `SMP_flow_visit_mart.py`, `SMP_flow_store_collect.py`, `paths.py`, `tests/test_flow_visit_mart.py`, `tests/test_flow_store_collect.py`.
- 변경: 방문일지 부모 글의 Flow 하위업무를 `flow_visit_subtask` fact로 별도 저장하고, 하위업무 상태·시작일·마감일·원문·댓글 합본을 업무당 1행으로 보존. 특정 부모 글 재상세수집용 `flow_repair_post_ids` 옵션 추가.
- 검증 결과: `py_compile` 통과, `tests/test_flow_store_collect.py`와 `tests/test_flow_visit_mart.py` 39개 통과.
- 남은 위험: 대화점 `83047164` 원천에는 현재 하위업무 row가 없어, OneDrive 산출 반영은 승인 후 FlowStore 재상세수집과 FlowVisit 재실행이 필요.

## 2026-08-18 Flow 방문일지 하위업무 작성자 컬럼 보강
- 대상: `SMP_flow_visit_mart.py`, `tests/test_flow_visit_mart.py`, `docs/db-schema.md`.
- 변경: `flow_visit_subtask`에 하위업무 작성일자·작성시간·작성자·제목과 댓글그룹/댓글작성자/댓글내용 분리 컬럼을 추가하고, 댓글 합본에서 시간/작성자 prefix를 제거.
- 검증 결과: `py_compile` 통과, `tests/test_flow_visit_mart.py`와 `tests/test_flow_store_collect.py` 39개 통과. 승인 후 FlowStore 재상세수집 run success, FlowVisit run `manual__codex_flowvisit_subtask_status_fix_20260818T095254` success, `flow_visit_subtask` 대화점 2건 확인.
- 남은 위험: 원천 댓글 작성자는 현재 Flow 데이터 기준으로 저장되며, `키워드 공략 방향성 제안` 댓글작성자는 `김대진,조민준`으로 확인됨.

## 2026-08-19 메뉴계층 닭결정옵션 검토 시트 추가
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders`.
- 변경: `03_요약.xlsx`에 `닭결정옵션_검토` 시트를 추가해 `닭옵션키=옵션없음` main 행을 판정완료/검토권장/입력필요/원가입력필요로 분리 표시. 제거된 `판매가_manual`/`상차림포함원가_manual`은 수기유실 가드 보호 대상에서 제외하고, 메뉴명 확정이 비율값보다 우선되도록 판정상태 순서를 보정.
- 검증 결과: `py_compile` 통과, 옵션없음 검토/가드/단가매칭 대상 테스트 14개 통과, 전체 메뉴계층 테스트 148개 통과. 승인 후 컨테이너 `build_orders(debug_outputs=True)` 성공, `03_요약.xlsx` 10시트 및 `닭결정옵션_검토` 82행 생성, `classification_unresolved` 0건 확인.
- 남은 위험: 입력 대기 WARN(`profit_missing`, `manual_profit_missing`, 재료단가/원가 입력 등)은 남아 있으며, 이번 실행 중 생성된 로컬 임시 백업 2개는 검증 후 삭제.

## 2026-08-19 메뉴계층 유료 닭중량 옵션 분리
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: `순살(닭다리살 100%) 300g`처럼 g 단위 닭 옵션은 기존 `닭유형` 확정값보다 `닭추가` 강제 분류가 우선되도록 하고, 닭옵션키에서는 제외해 부모 메뉴 판정과 분리. 수익률 키는 `순살추가300g` 품목으로 별도 생성하며, `옵션분류` 시트에 `닭가산_manual`/`닭가산유형_manual` 입력 컬럼을 노출.
- 검증 결과: `py_compile` 통과, 유료 닭중량 옵션 분리/기존 유료 닭유형 흡수 회귀 테스트 3개 통과, 전체 메뉴계층 테스트 150개 통과. 승인 후 컨테이너 `build_orders(debug_outputs=True)` 성공, `순살(닭다리살 100%) 300g` 15행 `닭추가` 분리 및 수익률 `순살추가300g` 21행 생성 확인.
- 남은 위험: `순살추가300g` 품목의 `메뉴원가_manual`은 새로 입력 필요. 실행 중 생성된 로컬 임시 백업 2개는 검증 후 삭제.

## 2026-08-19 메뉴계층 표준메뉴명 수기 보정 시트 추가
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: `01_수기입력.xlsx`에 `메뉴명보정` 시트를 추가하고 `std_menu_name_manual` 입력값을 `option_kind`/닭판정/수익키 계산 전에 적용하도록 설계. main 보정은 같은 주문그룹 option/side에 전파하고, option/side 개별 보정은 현재 표준메뉴명까지 포함한 키로 제한해 다른 부모 메뉴로 퍼지지 않게 함.
- 검증 결과: `py_compile` 통과, 메뉴명보정 전파/스코프/수기값 보존 테스트 추가, 전체 메뉴계층 테스트 153개 통과. 승인 후 컨테이너 `build_orders(debug_outputs=True)` 성공, `01_수기입력.xlsx`에 `메뉴명보정` 2,408행 생성 확인.
- 남은 위험: `메뉴명보정.std_menu_name_manual`은 사용자가 필요한 행만 입력해야 하며, 실행 중 생성된 로컬 임시 백업 1개는 검증 후 삭제.

## 2026-08-19 메뉴계층 닭추가 옵션 부모 닭유형 오염 방지
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: `순살(닭다리살 100%) 300g`처럼 `option_kind=닭추가`인 옵션이 부모 메뉴의 닭유형 선택/반반 판정 신호로 다시 읽히지 않도록 그룹 자동판정에서 제외.
- 검증 결과: `py_compile` 통과, 실제 POS 명칭 기반 회귀 테스트 추가, 전체 메뉴계층 테스트 154개 통과(`--basetemp=.tmp/...` 사용).
- 남은 위험: OneDrive 산출물은 아직 재생성하지 않았으므로 기존 `_debug/latest`와 `01_수기입력.xlsx`에는 반영 전 값이 남아 있음.

## 2026-08-19 메뉴계층 7~8월 범위 축소 및 순살추가/변경 분리
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders`.
- 변경: 기본 대상월을 `2026-07, 2026-08`로 제한하고, `순살로 변경`은 `닭유형`, 순살/닭다리살 150g·300g 단독 추가는 `닭추가`로 분리. 복합 옵션(`닭다리살+우삼겹+묵은지`)은 닭추가 강제 대상에서 제외하고, 닭추가 기준단가 WARN을 추가.
- 검증 결과: `py_compile` 통과, 전체 메뉴계층 테스트 159개 통과. 승인 후 기존 OneDrive 산출물을 로컬 백업으로 격리하고 재빌드 성공(`2026-07:8645`, `2026-08:4529`, 최종 주문 13,174행, 대상 ym 7~8월만 존재).
- 남은 위험: 쿠팡 `닭다리살 100% 순살 300g 추가` 원천 단가 0원 17건은 `chicken_addon_price_mismatch` WARN으로 남음. 로컬 임시 백업 `C:\airflow\LOCAL_DB\temp\backups\menu_hierarchy_rebuild_7_8`는 권한/정책 차단으로 자동 삭제하지 못함.

## 2026-08-19 메뉴계층 순살 전용 메뉴 보정 가드
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: `순살 닭한마리 칼국수 정식`처럼 메뉴명에 순살 신호가 있는 점심 정식이 뼈닭 전용 칼국수 규칙보다 먼저 순살 전용 프로필 제안을 받도록 순서를 보정. `메뉴명보정.std_menu_name_manual` 적용값이 수익률 `대표품목명`/`수익키`로 이어지는 흐름을 테스트로 고정하고, 대표주문메뉴명 닭유형 신호와 최종 닭유형이 충돌하면 `main_menu_name_chicken_type_conflict` WARN을 남김.
- 검증 결과: `py_compile` 통과, 전체 메뉴계층 테스트 162개 통과(`--basetemp=.tmp/pytest-menu-hierarchy` 사용).
- 남은 위험: OneDrive `01_수기입력.xlsx` 및 산출물 재빌드는 아직 실행하지 않았으므로, 실제 문제 행 반영은 `메뉴명보정`/`메뉴닭프로필` 입력 후 DAG 또는 `build_orders` 재실행이 필요.

## 2026-08-19 메뉴계층 할인/이벤트 음수매출 보정 및 재빌드
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders`.
- 변경: 전액할인/이벤트성 0원 주문은 `수익매출`을 음수로 만들지 않고 0으로 유지하며 분석 대상에는 포함. `예외주문.닭계상_manual=Y`인 취소 행은 qty 0이어도 닭사용량을 1개 기준으로 복구. `메뉴명보정` 수기행은 현재 주문 범위 밖이어도 보존하고, 비메인 음료 TMP 상품표 누락은 WARN으로 낮춤.
- 검증 결과: `py_compile` 통과, 전체 메뉴계층 테스트 164개 통과. 승인 후 컨테이너 재빌드 성공(`2026-07:8645`, `2026-08:4557`, 최종 주문 13,202행). `순살 닭한마리 칼국수 정식` 수익키가 순살 기준으로 분리되고 `수익매출<0` 0건 확인.
- 남은 위험: `product_gap` WARN 2건은 posfeed 음료 상품표 누락으로 남아 상품 검수표 보강 필요. 입력 대기 WARN(`profit_missing`, `manual_profit_missing`, 닭추가 단가 0원 등)은 별도 원가/상품 검수 작업으로 남음.

## 2026-08-19 메뉴계층 음료 수익률 분리 반영
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, OneDrive `new_classification_orders`.
- 변경: 음료주류 수익률 생성 시 `메뉴명보정.std_menu_name_manual`이 있으면 기존 `콜라` 강제 통합보다 우선해 `펩시 콜라`, `펩시 제로` 등으로 수익키를 분리. 부모 메뉴명으로 덮인 옵션 행도 item_id/item_name 기준 보정명을 직접 참조하도록 보강.
- 검증 결과: `py_compile` 통과, 전체 메뉴계층 테스트 165개 통과. 승인 후 재빌드 성공(`2026-07:8645`, `2026-08:4557`), `수익률` 시트에 `품목|홀|펩시 제로`, `품목|홀|펩시 콜라`, `품목|배달의민족|펩시 제로`, `품목|배달의민족|펩시 콜라` 등 분리 확인.
- 남은 위험: 기존 `콜라` 수익키에 입력했던 원가가 있었다면 새 펩시별 수익키에는 자동 이관되지 않을 수 있어 `수익률` 시트 원가 입력 재확인이 필요.

## 2026-08-19 Flow 하위업무 댓글 히스토리 그룹 저장
- 대상: `SMP_flow_store_collect.py`, `SMP_flow_visit_mart.py`, Flow 스키마 문서와 관련 테스트.
- 변경: Flow 댓글 원천에 부모/루트 댓글 ID, 깊이, 순서를 보존하고 중첩 대댓글을 펼치도록 보강. `flow_visit_subtask`에는 기존 댓글 합본 컬럼을 유지하면서 원댓글 작성자 기준 `comment_history_json`을 추가.
- 검증 결과: `py_compile` 통과, `tests/test_flow_store_collect.py`와 `tests/test_flow_visit_mart.py` 40개 통과(`--basetemp=.tmp/pytest-flow-comment-history` 사용).
- 남은 위험: 승인 후 FlowStore/FlowVisit 재실행은 성공했으나, 현재 Flow 상세 API는 `REPLY_CNT`만 주고 대댓글 본문 3건은 내려주지 않아 `comment_history_json`에는 최상위 댓글과 시스템 이벤트까지만 반영됨.

## 2026-08-19 메뉴계층 음수 판매가/이벤트 원가 계상
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 이벤트/서비스/전액할인 주문의 기준 판매가는 `할인전정가` 또는 음수 원본 매출의 절댓값으로 산출하고, `수기수익`은 `수익매출 - 수기원가*수익계상수량`으로 계산하도록 변경. 최종 주문에 `수익계상수량`, `수익계상원가`를 남겨 0원 이벤트의 원가 차감을 추적 가능하게 함.
- 검증 결과: 전체 메뉴계층 테스트 166개 통과(`--basetemp=.tmp/pytest-menu-hierarchy` 사용). 기본 Windows 임시 폴더 권한 문제로 첫 실행은 setup 오류가 있었으나 저장소 내부 basetemp에서는 통과.
- 남은 위험: OneDrive `01_수기입력.xlsx` 및 산출물 재빌드는 아직 실행하지 않았으므로, 실제 운영 파일 반영은 별도 승인 후 DAG 또는 `build_orders` 재실행 필요.

## 2026-08-19 Flow parquet 혼합 스키마 읽기 보정
- 대상: `SMP_flow_store_collect.py`, `SMP_flow_visit_mart.py`, `tests/test_flow_visit_mart.py`.
- 변경: Flow API 원천 parquet 파티션을 파일별로 읽어 hive 파티션 컬럼과 컬럼 union을 복원하도록 보정. 구스키마/신스키마 파티션이 섞여도 댓글 계층 컬럼이 누락되지 않게 함.
- 검증 결과: `py_compile` 통과, `tests/test_flow_store_collect.py`와 `tests/test_flow_visit_mart.py` 41개 통과. 승인 후 `Sales_FlowVisit_01_Mart_Dags` 재실행 성공(`manual__codex_flowvisit_schema_union_20260819T181600`) 및 `flow_visit_subtask` 대상 행 검증 완료.
- 남은 위험: 현재 API 원본에는 대상 댓글의 `REPLY_CNT=3`만 있고 대댓글 본문 row는 없어, 대댓글 본문 저장은 Flow API 응답이 내려주는 범위에 의존.

## 2026-08-19 메뉴계층 0원 원가성 품목 수익률 대상 보정
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 0원 원가성 품목 상수 선언 순서를 바로잡아 모듈 import 오류를 제거하고, 공기밥/재료추가/음료 등 0원 qty 품목은 수익률 대상에 남는 회귀 테스트를 추가.
- 검증 결과: `import ok`, `py_compile` 통과, 전체 메뉴계층 테스트 168개 통과(`--basetemp=.tmp/pytest` 사용).
- 남은 위험: OneDrive `01_수기입력.xlsx` 및 산출물 재빌드는 실행하지 않았으므로 실제 운영 파일 반영은 별도 승인 후 재실행 필요.

## 2026-08-19 메뉴계층 수기입력 워크북 손상 읽기 방어
- 대상: `DB_MenuHierarchy_Test.py`, OneDrive `new_classification_orders/01_수기입력.xlsx`.
- 변경: 현재 워크북이 2,086바이트로 잘린 xlsx라 Excel/zip/openpyxl에서 열리지 않는 상태를 확인. 깨진 기존 워크북 때문에 재생성 단계가 중단되지 않도록 `BadZipFile`/`OSError` 읽기 실패를 빈 시트로 처리.
- 검증 결과: `import ok`, `py_compile` 통과, 전체 메뉴계층 테스트 168개 통과(`--basetemp=.tmp/pytest-open-workbook-full` 사용).
- 남은 위험: OneDrive 파일 자체는 승인 전이라 아직 교체하지 않음. 기존 워크북의 수기 입력값은 파일 손상으로 읽을 수 없어 OneDrive 버전 기록 복원 또는 산출물 재빌드가 필요.

## 2026-08-19 메뉴계층 수기입력 워크북 재생성
- 대상: OneDrive `new_classification_orders/01_수기입력.xlsx`.
- 변경: 사용자 승인 후 깨진 2,086바이트 워크북을 `_debug/latest` CSV 기반 13개 시트 워크북으로 재생성. 기존 손상 파일은 로컬 백업 경로에 자동 보관됨.
- 검증 결과: 재생성 파일 364,487바이트, zip 무결성 `bad=None`, openpyxl 시트 열기 성공. `수익률` 774행, `메뉴명보정` 1,658행, `옵션분류` 409행 확인.
- 남은 위험: 손상된 원본 워크북에서만 갖고 있던 최신 수기 입력값은 읽을 수 없어 복원되지 않았을 수 있음. 필요 시 OneDrive 버전 기록에서 손상 전 버전 복원이 가장 정확함.

## 2026-08-19 배민/정산 DAG 실패 복구 실행
- 대상: `DB_Beamin_Macro_Dags_Retry`, `DB_DeliveryCommission_Dags`, `DB_Beamin_Macro_Dags`.
- 변경: `bestpolo111`/연신내점 2026-08-18 retry를 수동 실행했고, 정산 결측 XCom 기준 `orders_only` 재수집 45건을 Airflow 큐에 등록.
- 검증 결과: 첫 수동 retry와 자동 2차 retry는 success였으나 잔여 실패가 남아 자동 3차 retry 진행 중. 정산 재수집은 45건 중 1건 running, 44건 queued 상태로 직렬 처리 중.
- 남은 위험: 오래된 날짜 재수집에서 배민 날짜 필터가 현재 주로 남는 경고가 반복되어 일부 결측은 해소되지 않을 수 있음. 모든 재수집 terminal 후 `DB_DeliveryCommission_Dags` 재실행 검증 필요.

## 2026-08-19 배민 orders 날짜필터 재발 방지
- 대상: `DB_Beamin_04_orders.py`, `DB_Beamin_combined.py`, `tests/test_baemin_orders_date_filter_abort.py`.
- 변경: 주문내역 날짜 필터가 실측 확인되지 않으면 즉시 `date_filter` 실패로 반환하도록 변경하고, 과거 날짜 달력 이동은 목표 월 도달 확인 후 날짜를 클릭하도록 보강. `orders_only`도 날짜필터 5매장 연속 실패 시 중단해 잘못된 no_data/성공 처리를 막음.
- 검증 결과: 날짜필터/드라이버복구/정산 테스트 43개 통과, 임시 `AIRFLOW_HOME` 기준 배민/Retry/정산 DAG import 통과, 수정 파일 UTF-8 읽기 통과.
- 남은 위험: 실제 배민 UI의 월 이동 버튼 DOM은 운영 실행에서 최종 확인 필요. `bestpolo111` 대시보드 로드 장애는 계정/브라우저 세션 상태 영향이 커서 별도 계정 상태 확인 필요.

## 2026-08-19 정산 결측 감시 Telegram 반복 억제
- 대상: `DB_DeliveryCommission_Dags.py`, `tests/test_delivery_commission.py`.
- 변경: `monitor_baemin_settlement_missing`은 결측이 있으면 자동 retry로 해결되지 않는 품질 게이트라 `retries=0`으로 고정하고, `on_failure_callback_no_telegram`을 지정해 전역 Telegram 실패 알림 반복을 차단. DAG 실패/XCom/자동복구 큐 기록은 유지.
- 검증 결과: 컨테이너 import에서 모니터 태스크 `retries=0`, callback=`on_failure_callback_no_telegram` 확인. 정산/배민 날짜필터 테스트 35개 통과, DAG 단위 테스트 통과, 수정 파일 UTF-8 읽기 통과.
- 남은 위험: 배민 정산 결측 80개 매장-브랜드/779일은 아직 실제 데이터 결측으로 남아 있어, 재수집 완료 전까지 DAG 상태는 failed로 남음.

## 2026-08-19 메뉴계층 OKPOS/배민 정상가 매출 기준 보정
- 대상: `DB_MenuHierarchy_Test.py`, `DB_UnifiedSales_okpos.py`, 관련 단위 테스트.
- 변경: 메뉴계층 배민 매출은 원천 `상품금액`/`주문옵션금액*주문수량` 기준 정상가로 계산하고, OKPOS 메뉴계층 호출은 `총매출액` 기준을 사용하도록 `amount_basis` 선택 인자를 추가. UnifiedSales 기본 OKPOS 동작은 기존 `실매출액` 기준 유지.
- 검증 결과: `py_compile` 통과, 메뉴계층/OKPOS 관련 테스트 173개 및 수동배달/쿠팡 회귀 테스트 34개 통과. `DB_UnifiedSales_Dags` import 통과.
- 남은 위험: OneDrive 산출물 재생성은 수행하지 않았으므로 실제 엑셀/CSV 산출 반영은 별도 승인 후 메뉴계층 재실행 필요.

## 2026-08-19 메뉴계층 정상가 컬럼 추가
- 대상: `DB_MenuHierarchy_Test.py`, OneDrive `new_classification_orders/02_최종주문.csv`, `_debug/latest/12_orders_left.csv`.
- 변경: 최종 분석 주문서에 `정상가` 컬럼을 추가하고, 기본은 `total_price`, 전액할인처럼 `total_price=0`/`discount_amount>0`인 행은 할인액으로 채우도록 보정. 사용자 승인 후 기존 OneDrive CSV 2개에도 같은 컬럼을 반영.
- 검증 결과: `py_compile` 통과, 메뉴계층/OKPOS 테스트 174개 통과. OneDrive CSV 2개 모두 13,202행 유지 및 `정상가` 컬럼 위치 확인.
- 남은 위험: 로컬 Windows 환경에서 2026-07/2026-08 원천 파티션을 찾지 못해 전체 메뉴계층 재빌드는 실행하지 못함. 기존 CSV 기준 보강이며, 원천 재빌드는 Airflow/마운트 환경에서 재실행 필요.

## 2026-08-19 메뉴계층 정상가 컬럼 전체 재빌드 실행
- 대상: `DB_MenuHierarchy_Test.py`, OneDrive `new_classification_orders` 산출물.
- 변경: 컨테이너 원천 경로에서 메뉴계층 전체 빌드를 실행하고, `메뉴닭프로필` 기존 수기행이 신규 후보에서 빠져도 보존되도록 보강.
- 검증 결과: `build_orders(debug_outputs=True)` 성공(`2026-07:8645`, `2026-08:4621`, 총 13,266행). `02_최종주문.csv`와 `_debug/latest/12_orders_left.csv` 모두 `정상가` 컬럼 확인, `01_수기입력.xlsx` zip/openpyxl 열기 성공. 관련 테스트 209개 및 메뉴계층/UnifiedSales DAG import 통과.
- 남은 위험: 검증 이슈는 모두 WARN이며 `profit_missing` 6,320건, `zero_price_cost_missing` 1,781건 등 수기 입력 대기 항목이 남아 있음.

## 2026-08-19 수익률 시트 매출합계/총매출합계 분리
- 대상: `DB_MenuHierarchy_Test.py`, OneDrive `new_classification_orders/01_수기입력.xlsx`, `_debug/latest/27_profit_rate_master.csv`.
- 변경: `수익률` 시트에 `총매출합계` 컬럼을 추가하고, `매출합계`는 할인 반영 후 실매출, `총매출합계`는 정상가/할인 전 매출로 분리. 자동 `판매가`는 `총매출합계 / 판매수량` 기준으로 계산.
- 검증 결과: 컨테이너에서 `build_orders(debug_outputs=True)` 성공(`2026-07:8645`, `2026-08:4621`). 수익률 엑셀/CSV 997행, 분리 행 401개 확인, workbook zip/openpyxl 검증 통과. 관련 테스트 211개 통과.
- 남은 위험: 기존 WARN 항목은 입력 대기 상태로 유지됨.

## 2026-08-20 collection_compare 매크로 주문번호 충돌 수정
- 대상: `DB_CollectionCompare.py`.
- 변경: 배민/쿠팡 매크로 로더의 주문 dedup 키를 주문번호 단독에서 매장+주문번호+주문시각/주문일시로 변경해 타 매장 동일 주문번호가 섞이지 않게 수정.
- 검증 결과: `py_compile` 통과. 승인 후 OneDrive `collection_compare.parquet` 재생성 완료. 평택서정점 2026-07-08 쿠팡이츠/toorder/unified 모두 432,900원, 배달의민족 990,100원 확인. 임시 `AIRFLOW_HOME`에서 `DB_CollectionCompare_Dags` import 통과.
- 남은 위험: 기존 주문번호 충돌 영향으로 전체 행 수와 차이/누락 건수는 재산출 기준으로 변동됨.

## 2026-08-20 메뉴계층 수익률 item_id 수기값 오염 차단
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 상품 검수표 닭 수기값과 메뉴계층 전용 검수표 키를 `item_id` 단독이 아니라 `item_id+item_name` 기준으로 적용해 쿠팡 재사용 item_id의 다른 품목 수기값이 main 메뉴에 붙지 않게 수정. 현재 주문에서 빠진 옵션분류 수기행은 보존하고, 일반 순살 메인 메뉴의 사이즈 미지정 기본값은 `중`으로 보강.
- 검증 결과: `py_compile` 통과, 메뉴계층 회귀 테스트 177개 통과. 사용자 승인 후 `build_orders(debug_outputs=True)` 성공(`2026-07:8645`, `2026-08:4636`, 총 13,281행), 차단 이슈 0건, workbook zip/openpyxl 확인.
- 남은 위험: `profit_missing`, `zero_price_cost_missing` 등 원가 입력 대기 WARN은 남아 있음.

## 2026-08-20 메뉴계층 배민 누룽지 1인분 옵션 분리
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 부모가 있는 option 행의 exact `1인분`은 사이즈 선택이 아니라 `닭추가`로 강제하고, 수익률 품목명을 `1인분 추가`로 분리해 원가를 별도 입력하게 함. main 상품 `1인분`은 기존처럼 `메인` 유지.
- 검증 결과: `py_compile` 통과, 메뉴계층 회귀 테스트 179개 통과. 승인 후 `build_orders(debug_outputs=True)` 성공(`2026-07:8645`, `2026-08:4636`), `누룽지 닭한마리|1인` main 수익키 0건, `소|순살/뼈닭|1인분 추가` 품목 수익키 생성, workbook zip/openpyxl 정상, 차단 이슈 0건.
- 남은 위험: `profit_missing`, `zero_price_cost_missing` 등 원가 입력 대기 WARN은 남아 있음.

## 2026-08-20 메뉴계층 쿠팡 1인 메뉴 item_id 재사용/다중 main 보정
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: 쿠팡 표준메뉴명은 재사용 `item_id`보다 원본 상품명 alias를 우선해 1인 순살 닭도리탕/1인 미나리 수삼 백숙이 도리당 닭도리탕에 섞이지 않게 함. 쿠팡 다중 main 주문은 main 단가 합계와 주문 총액이 일치할 때 첫 main 몰림 금액을 main별 단가로 재분배.
- 검증 결과: `py_compile` 통과, 메뉴계층 전체 테스트 182개 통과. 승인 후 `build_orders(debug_outputs=True)` 성공, `도리당 닭도리탕|1인` 쿠팡 수익키 0건, `1인 순살 닭도리탕(밥포함)`/`1인 미나리 수삼 백숙` 별도 수익키 생성, `0XLXUC` main 금액 16,900/35,400 재분배, workbook zip/openpyxl 정상, 차단 이슈 0건.
- 남은 위험: `profit_missing`, `zero_price_cost_missing` 등 원가 입력 대기 WARN은 남아 있음.

## 2026-08-20 메뉴계층 수익률 판매가 수기 입력
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: `수익률` 시트에 `판매가_manual`, `판매가기준`을 추가하고 수익률 계산은 `판매가_manual`이 있으면 수기값, 없으면 자동 `판매가`를 사용하도록 변경. 수기 판매가를 기존 수기 금액 보호/미매칭 보존/수익키 이관 대상에 포함.
- 검증 결과: `py_compile` 통과. Windows 기본 Temp 권한 문제로 1차 pytest는 중단됐고, `.tmp/pytest-run` basetemp 지정 후 메뉴계층 전체 테스트 185개 통과. 승인 후 `build_orders(debug_outputs=True)`로 OneDrive `01_수기입력.xlsx` 재생성, workbook zip/openpyxl 정상, `수익률` 시트 `판매가_manual`/`판매가기준` 컬럼 반영, 차단 이슈 0건.
- 남은 위험: 현재 `판매가_manual` 입력값은 0건이라 기존 자동 판매가 기준으로 계산됨. 사용자가 수기 판매가를 입력한 뒤 재실행해야 수기 기준 수익률이 반영됨.

## 2026-08-20 메뉴계층 반반/세트 조합별 원가 분리
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`.
- 변경: `수익률` 시트에 `원가조합` 축을 추가하고 2인 순살 반반/베스트 반반/시그니처 반반 계열 main 수익키를 조합별로 분리. 세트 기본 포함 `가브리살`/`미나리 새우전` 1개는 main 매출로 흡수하고 추가 사이드는 별도 품목으로 유지. 제공 원가표는 기존 수기값을 덮지 않고 빈 `판매가_manual`/`메뉴원가_manual`에만 seed로 채움.
- 검증 결과: `py_compile` 통과, `.tmp/pytest-run` basetemp 지정 후 메뉴계층 전체 테스트 190개 통과.
- 남은 위험: OneDrive `01_수기입력.xlsx` 재생성은 아직 미실행. 승인 후 실제 워크북에서 조합별 행/수기값 보존/차단 이슈를 재검증해야 함.

## 2026-08-20 배민 매크로 0818-0819 미수집 복구 코드 수정
- 대상: `DB_Beamin_04_orders.py`, `dag_schedule_guard.py`, `Strategy_ScheduleGuard_01_Overdue_Dags.py`, 관련 테스트.
- 변경: low settle rate 합계검증 통과 주문 저장 차단 해제, 날짜 specific 달력 월 파싱 폴백 추가, 정산정보 렌더 대기/로그 축약, `next_dagrun_create_after=NULL` 스케줄 봉쇄 탐지 및 자동 트리거 생략.
- 검증 결과: `py_compile` 통과, `PYTHONPATH=.` 기준 `test_dag_schedule_guard.py`, `test_baemin_orders_cancelled_filter.py`, `test_baemin_orders_date_filter_abort.py` 27개 통과. 주문/가드 DAG import 스모크 통과.
- 남은 위험: 실제 배민 로그인 세션 기반 2026-08-18 과거 날짜 1매장 실측은 미실행. Windows 로컬 Airflow import 중 symlink 경고는 남아 있으나 import 결과는 정상 출력됨.

## 2026-08-20 배민 실패매장 재실행 큐 확인 및 날짜 필터 핫픽스
- 대상: `DB_Beamin_04_orders.py`, Airflow `DB_Beamin_Macro_Dags`/`DB_Beamin_Macro_Dags_Retry` 실행 상태.
- 변경: 달력 월 파싱 폴백이 `DatePicker.Trigger`의 현재 범위 텍스트를 월 헤더로 오인하지 않도록 `~` 포함 텍스트와 trigger 계열 후보를 제외하고, 이전달 버튼 주변 달력 텍스트를 우선 탐색하게 수정.
- 검증 결과: 로컬/컨테이너 `py_compile` 통과, `test_baemin_orders_date_filter_abort.py`, `test_baemin_orders_cancelled_filter.py` 15개 통과. 2026-08-18 실패매장 17개 orders-only run과 2026-08-19 Retry run은 중복 트리거 없이 queued 상태 확인.
- 남은 위험: `baemin_selenium_pool` 2개 슬롯을 2026-08-02 Retry 두 레인이 점유 중이라 신규 실패매장 run은 대기 중. 현재 실행 중인 task는 이미 로드한 코드로 계속 진행되므로 핫픽스는 다음 task/run부터 반영됨.

## 2026-08-20 쿠팡수동 부모메뉴/옵션 매출 분류 보정
- 대상: `DB_UnifiedSales_coupang.py`, `DB_CoupangMacro_load.py`, `croling_coupang.py`, `DB_UnifiedSales_Today_Dags.py`, 관련 테스트.
- 변경: 쿠팡 orders에 `item_menu`를 보존하고 과거 파일은 `menu_name`으로 보완. UnifiedSales 변환은 행별 부모메뉴와 옵션명을 분리하고, 주문 금액은 가격 있는 메뉴 행에 합계 보존 방식으로 배분해 옵션명에 매출이 붙는 문제를 방지.
- 검증 결과: `PYTHONPATH=C:\airflow`, `.tmp` basetemp 기준 쿠팡수동 금액/결손/취소/upsert/item_menu 테스트 12개 통과. 모듈 import 통과, 임시 `AIRFLOW_HOME` 기준 UnifiedSales 3개 DAG import 통과.
- 남은 위험: OneDrive 기존 unified 산출물 재처리는 미실행. 승인 후 쿠팡수동 전체 기간 재처리와 일자/매장별 `total_price`, `order_cnt` 합계 비교가 필요.

## 2026-08-20 쿠팡수동 UnifiedSales 산출물 안전 재처리
- 대상: OneDrive `data/mart/unified_sales_grp` 쿠팡수동 산출물, `.tmp/coupang_unified_reprocess` 검증 산출물.
- 변경: 쿠팡 원천이 있는 4,509개 매장-일자 중 기존 대비 `total_price`/`order_cnt` 합계가 동일한 4,187개만 재작성해 양수 매출이 옵션명에 붙는 행을 제거. `fin_product`는 저장하지 않는 비영구 item_id 할당으로 보호.
- 검증 결과: 안전 반영 키 4,187개에서 기존 대비 `total_price`/`order_cnt` 차이 0건, `total_price > 0` 및 `item_name != menu_name` 0행. 수정 전 parquet는 `C:\Local_DB\temp\backups\coupang_unified_reprocess\20260820_180621`, `20260820_181718`에 백업.
- 남은 위험: 합계가 바뀌는 322개 키는 자동 반영 제외. 이 중 기존 산출물 누락 212개, 기존/신규 합계 불일치 110개이며 현재 옵션 양수 행 4,590행이 남아 별도 원천 대조 후 반영 필요.

## 2026-08-21 배민 2026-08-04 미해결 및 OKPOS 로그인 오판 수정
- 대상: `DB_OKPOS_Sales.py`, `DB_Beamin_Macro_validate.py`, `DB_Beamin_Macro_upload.py`, `DB_Beamin_combined.py`, 관련 테스트.
- 변경: OKPOS `/asp/main` 대시보드 도달을 로그인 성공으로 판정. 배민 ToOrder 검증은 `store_info_per_account`가 비어도 `account_list`의 store 힌트로 재시도 매장을 복원하고, 주문 날짜필터 연속 실패는 residual 실패로 보존. DatePicker 직접선택 적용 순서를 보강하고, 브랜드 원천 누락형 차이는 재수집 대상에서 제외해 별도 보고.
- 검증 결과: 컨테이너 `py_compile`/DAG import 통과, 관련 pytest 51개 통과. 2026-08-04 직접 재수집으로 미사점 498,200원, 서울대입구역점 564,900원은 ToOrder 일치 복구. 삼송점은 배민 원천 153,700원 중 ToOrder가 도리당 141,100원만 반영한 원천차이로 분류되어 재시도 대상에서 제외. OKPOS `download_today_batch[1]` 재실행 후 영수증/일매출/raw 저장/검증/후속 트리거까지 성공.
- 남은 위험: UnifiedSales mart 재처리는 미실행. OneDrive 산출물 수정이 필요하면 별도 승인 후 진행해야 함.

## 2026-08-21 메뉴계층 신규 상품 검증 차단 완화
- 대상: `DB_MenuHierarchy_Test.py`, `tests/test_menu_hierarchy_no_model_classification.py`, `DB_MenuHierarchy_Test_Dags`.
- 변경: 상품표에 없거나 TMP item_id인 신규 main이라도 표준메뉴명, 닭유형/사이즈, 수익키가 운영상 확정된 경우 `product_gap`을 WARN으로 낮추고 `tmp_item` 차단에서 제외. 실제 미해결 TMP main은 계속 ERROR로 차단.
- 검증 결과: 실패 당시 debug 산출물 재판정 시 차단 0건, 메뉴계층 pytest 195개 통과, 컨테이너 `py_compile`/DAG import 통과. 실패 run `scheduled__2026-08-20T04:00:00+00:00`의 `build_orders` 재실행 성공 및 DagRun 상태 success 반영.
- 남은 위험: `product_gap` WARN 1건은 검수표 등록 대기 항목이며 산출물 생성은 차단하지 않음.

## 2026-08-21 Flow 방문일지 업무 상태 추출 보강
- 대상: `SMP_flow_store_collect.py`, `test_flow_store_collect.py`.
- 변경: Flow 상세 API의 `TASK_COLUMN_REC` 상태 우선 추출은 유지하고, 최상위/대체 상태·진행률·시작일·마감일 필드 fallback을 추가.
- 검증 결과: `test_flow_store_collect.py` 19개 통과, Flow 수집/방문일지 DAG import 통과, 관련 파일 `py_compile` 통과.
- 남은 위험: 현재 로컬 셸에는 `FLOW_API_KEY`가 없어 live API 재수집 실측은 미실행. OneDrive mart 산출물은 수정하지 않음.

## 2026-08-21 Flow 하위업무 task_status 시스템 댓글 우선 반영
- 대상: `SMP_flow_store_collect.py`, `SMP_flow_visit_mart.py`, Flow 상태 관련 테스트.
- 변경: Flow API `STTS` 코드를 `요청/진행/완료/보류`로 수집하고, `flow_visit_subtask.task_status`는 최신 `S45` 시스템 댓글 상태 변경값을 원천 상태보다 우선하도록 수정.
- 검증 결과: `test_flow_store_collect.py`, `test_flow_visit_mart.py` 48개 통과, 관련 DAG import와 `py_compile` 통과. 사용자 승인 후 2548064 Flow 상세 재수집 및 방문일지 mart 재생성 완료.
- 남은 위험: 이번 반영은 승인된 2548064 대상에 한정. 다른 프로젝트의 기존 `flow_visit_subtask` 파티션은 다음 수집/마트 재생성 때 반영됨.

## 2026-08-21 DB_DeliveryCommission 배민 정산 결측 재수집 큐 등록
- 대상: `DB_DeliveryCommission_Dags`, `DB_Beamin_Macro_Dags` orders-only 재수집.
- 변경: `monitor_baemin_settlement_missing` 실패 원인을 배민 주문 정산예정금액 결측으로 확정하고, 기존 2026-08-19 재수집 45건 완료분을 반영한 뒤 잔여 결측 479일 중 활성 중복 1건을 제외한 38개 orders-only DagRun을 신규 큐 등록.
- 검증 결과: `DB_DeliveryCommission_Dags` import error 없음, 기존 재수집은 44 success/1 running, 신규 `manual__20260821__orders_only_settlement_recollect__*` 38건 queued 확인.
- 남은 위험: `baemin_selenium_pool`을 `DB_Beamin_Macro_Dags_Retry` 실행 2건이 점유 중이라 신규 큐 실행 대기 중. 최종 `DB_DeliveryCommission_Dags` mart 재생성은 OneDrive 수정 가능성이 있어 승인 후 진행 필요.

## 2026-08-21 배민 2026-08-13 ad_funnel 잔존 복구
- 대상: `DB_Beamin_05_ad_funnel.py`, 2026-08-13 배민 ad_funnel 재수집.
- 변경: 날짜 직접 선택 후 필터 버튼 문구가 `날짜 직접 선택`으로만 표시되는 경우를 정상으로 인정하고, DatePicker 트리거가 대상일을 들고 있으면 달력 월 파싱 불일치를 실패로 보지 않도록 보강.
- 검증 결과: 컨테이너 직접 재수집으로 기존 30건 중 29건 성공 후 부산광안점 1건을 새 로직으로 재수집 성공. 최종 재검증 `empty=0 retried=0 still=0`, 관련 pytest 37개 통과.
- 남은 위험: 이번 조치는 ad_funnel CSV 복구에 한정. Airflow에 이미 queued 상태인 배민 재수집 DagRun들은 별도 큐 정리 없이 그대로 둠.

## 2026-08-21 배민 2026-08-14 Retry 잔여 실패 정리
- 대상: `DB_Beamin_Macro_Dags_Retry.py`, 2026-08-14 `orders_only_settlement_recollect` Retry run.
- 변경: Retry 최종 판정에서 성공 수집된 `store_info_per_account` 매장의 orders 잔여를 제거하고, orders-only run의 ad_funnel/NOW/우리가게 잔여를 최종 실패 카운트에서 제외하도록 보강. 계정 residual도 대상일 orders parquet가 있으면 해결된 것으로 판정.
- 검증 결과: 2026-08-14 ad_funnel 잔존 17건과 orders 잔여 3개 매장을 직접 재수집해 `완료`, 2026-08-13 ad_funnel XCom 재검증 후 `완료`. 추가로 2026-08-12, 2026-08-10, 2026-08-04, 2026-08-02, 2026-07-26, 2026-08-18 scheduled retry run을 최종 재판정해 DagRun success 반영.
- 남은 위험: 최신 잔여 failed는 2026-08-19 `pc2_bottom` Retry와 일부 과거 run. 일부 success 메시지는 잔여 실패 0이어도 ToOrder 원천 차이 때문에 `부분완료`로 표시됨.

## 2026-08-24 ToOrder store/platform 원천 중단 복구
- 대상: `DB_Toorder_store_platform_daily_Dags`, `DB_CollectionCompare_Dags`, OneDrive `toorder_store_platform_daily.parquet`/`collection_compare.parquet`.
- 변경: ToOrder 기본 날짜 범위를 parquet 최신일 다음날부터 전일까지 자동 catch-up하도록 보강하고, paused DAG를 unpause한 뒤 2026-08-10~2026-08-23 원천을 재수집.
- 검증 결과: ToOrder 원천 최신일 2026-08-23, 2026-08-10~23 일별 rows 생성 확인. `DB_CollectionCompare_Dags` 재생성 성공, 2026-08-10 이후 `present_toorder` 전면 0 상태 해소.
- 남은 위험: 2026-08-24 당일 행은 ToOrder 전일 기준 수집 정책상 아직 누락으로 남음. 다음 07:10 스케줄에서 2026-08-24가 반영되어야 함.

## 2026-08-24 쿠팡 확장 역삼점 orders 타임아웃 보강
- 대상: OneDrive 운영본 `doridang_collector_개발용`의 `runner.js`, `content/05_main.js`.
- 변경: batch COLLECT 즉시 ACK/하트비트, 진행 기반 timeout, orders 매장 예산 기반 절대상한, timeout 후 STOP 부분 저장 회수, top-frame 가드를 추가.
- 검증 결과: `.bak_260824` 백업 2개 생성, `node --check` 2개 통과, content/runner orders 예산 1440000 일치 및 기존 호출자 `ackOnly` 미적용 확인.
- 남은 위험: 크롬 확장 로드와 `doriys` 실수집 검증은 로그인/브라우저 세션이 필요해 사용자 실행 필요.

## 2026-08-24 쿠팡 확장 heartbeat idle 오판 후속 보강
- 대상: OneDrive 운영본 `doridang_collector_개발용`의 `runner.js`, `content/05_main.js`, `content/03_coupangeats.js`.
- 변경: heartbeat 수신을 생존 신호로 인정하고, 주문별 `COLLECT_PROGRESS` 신호와 영속 STOP 없는 `STOP_ACTIVE_COLLECT`를 추가해 orders timeout 후 CMG/menu 연쇄 차단을 방지.
- 검증 결과: 수정 JS 3개 `node --check` 통과, `STOP`/`STOP_ACTIVE_COLLECT` 분리 및 progress/heartbeat 메시지 배치 확인.
- 남은 위험: `doriys` 재실행에서 7페이지 이후 idle 오판 해소와 CMG/menu 실제 수집 재개를 브라우저에서 확인해야 함.

## 2026-08-24 배민 수동수집 곱도리당 제외
- 대상: OneDrive 운영본 `doridang_collector_개발용`의 `content/02_baemin.js`, `runner_baemin.js`, `popup.js`.
- 변경: 배민 수동수집/자동선택/runner option 탐색에서 `도리당`·`나홀로`만 허용하고 `곱도리당`은 명시 제외. `나홀로 1인 곱도리탕` 메뉴형 이름은 유지.
- 검증 결과: 수정 JS 3개 `node --check` 통과, 샘플 필터에서 도리당/나홀로 허용·곱도리당 제외 확인.
- 남은 위험: 확장 새로고침 후 팝업 자동선택과 배민 orders 페이지 option 로그에서 곱도리당 제외를 브라우저로 확인해야 함.

## 2026-08-24 delivery_commission 배민 정산예정금액 재수집 자동 트리거
- 대상: `DB_DeliveryCommission_Dags`, `DB_DeliveryCommission.py`, `DB_Beamin_04_orders.py`.
- 변경: 정산예정금액 결측 감시는 XCom/CSV를 남긴 뒤 별도 task가 `DB_Beamin_Macro_Dags` orders-only 재수집을 트리거하고 build를 차단하도록 분리. 배민 orders 정산정보 수집률 낮음은 부분 저장하지 않고 재수집 대상으로 보존.
- 검증 결과: `tests/test_delivery_commission.py`, `tests/test_baemin_orders_cancelled_filter.py`, `tests/test_baemin_macro_validation.py` 통과. 컨테이너 DAG import 통과.
- 남은 위험: 실제 `delivery_commission.parquet` 재빌드는 OneDrive mart 수정이므로 승인 후 실행 필요. 이미 존재하는 결측 재수집 DagRun은 run_id 중복으로 skip될 수 있음.

## 2026-08-24 delivery_commission 실패/텔레그램 알림 보정
- 대상: `DB_DeliveryCommission_Dags`, `DB_DeliveryCommission.py`.
- 변경: 정산예정금액 결측 재수집 트리거 후 `RuntimeError` 대신 `AirflowSkipException`으로 build만 스킵하고, 실제 실패는 텔레그램 콜백을 타도록 변경.
- 검증 결과: `tests/test_delivery_commission.py` 30개, `tests/test_telegram_notifier_plugin.py` 3개 통과. 컨테이너 DAG import에서 monitor/trigger 콜백 `on_failure_callback` 확인.
- 남은 위험: 실제 `delivery_commission.parquet` 재빌드는 OneDrive mart 수정이므로 승인 후 실행 필요.

## 2026-08-24 delivery_commission mart 최신 원천 반영 보정
- 대상: `DB_DeliveryCommission.py`, `tests/test_delivery_commission.py`, OneDrive `delivery_commission.parquet`.
- 변경: 배민 정산예정금액 결측이 남아도 trigger/build를 중단하지 않고 재수집 트리거 후 NA 포함 mart를 갱신하도록 변경해 정상 원천 행까지 stale 되는 문제를 해소.
- 검증 결과: 관련 pytest 30개와 텔레그램 테스트 3개 통과. Airflow worker trigger task SUCCESS 확인. 승인 후 build task로 mart 25,492행 재생성 성공, 송파삼전점 최대일자가 2026-08-17에서 2026-08-23으로 갱신됨.
- 남은 위험: 송파삼전점 배민 2026-08-21~2026-08-23 등 배민 정산예정금액 원천 결측 295행은 NA로 남으며 별도 재수집/원천 복구 대상.

## 2026-08-24 Flow 하위업무 댓글 완전성 가드
- 대상: `SMP_flow_store_collect.py`, `tests/test_flow_store_collect.py`, `Strategy_FlowStore_01_Collect_Dags`.
- 변경: Flow 상세 API의 `remarkCount`/`REPLY_CNT`와 실제 수집 댓글 수가 맞지 않으면 `comment_gaps`를 기록하고 parquet 저장을 중단. 별도 댓글 API env `FLOW_REMARKS_API_URL_TEMPLATE`가 설정되면 추가 댓글을 병합하도록 보강.
- 검증 결과: `tests/test_flow_store_collect.py`, `tests/test_flow_visit_mart.py` 51개 통과, 관련 DAG import와 `py_compile` 통과. 2548064 repair run은 API가 `83070915` 댓글 4건 중 2건만 반환해 저장 단계에서 의도적으로 실패.
- 남은 위험: 현재 Flow API는 `키워드 공략 방향성 제안`의 대댓글 본문 3건을 내려주지 않아 mart 재생성은 보류. 별도 댓글 조회 API 또는 Selenium/웹 DOM fallback 확인 필요.

## 2026-08-24 Flow 댓글 gap 적재 우선 전환
- 대상: `SMP_flow_store_collect.py`, `tests/test_flow_store_collect.py`, `Strategy_FlowStore_01_Collect_Dags`, `Sales_FlowVisit_01_Mart_Dags`.
- 변경: 댓글 gap은 기본적으로 경고와 `comment_gaps` 로그로 보존하고 parquet 저장은 진행하도록 전환. 엄격 실패가 필요하면 `FLOW_COMMENT_GAP_FAIL_ON_MISMATCH` 또는 dag conf `flow_comment_gap_fail_on_mismatch=true`로 켤 수 있게 함.
- 검증 결과: `tests/test_flow_store_collect.py`, `tests/test_flow_visit_mart.py` 52개 통과, 관련 DAG import와 `py_compile` 통과. FlowStore repair run `manual__codex_flow_comment_gap_load_20260824T162430` 성공, FlowVisit run `manual__codex_flow_comment_gap_load_20260824T162730` 성공.
- 남은 위험: `83070915`는 API 원천이 댓글 4건 중 2건만 반환해 mart에도 김대진 상태변경과 조민준 테스트 댓글만 저장됨. 누락 댓글 본문 반영은 별도 댓글 API 또는 웹 DOM fallback 확보 필요.

## 2026-08-24 delivery_commission 08-22/08-23 배민 정산 복구
- 대상: `DB_DeliveryCommission.py`, `DB_Beamin_Macro_Dags_Retry.py`, `tests/test_delivery_commission.py`, `tests/test_baemin_final_notification.py`.
- 변경: delivery_commission 유래 배민 orders-only 재수집은 `bulk_70` 프로필을 사용하고 retry DAG의 pool 점유 랜덤 대기를 생략해 재수집 정체를 줄임.
- 검증 결과: 08-22/08-23 상위·하위 재수집 및 upload ingest 모두 SUCCESS, OneDrive delivery_commission mart 25,494행 재빌드. 08-22/08-23 배민 정산금액 결측 0건, 송파삼전점 08-22=307,781원·08-23=29,525원 확인.
- 남은 위험: 08-21 이전 과거 결측 148건은 남아 있으며 별도 우선순위로 재수집 필요.

## 2026-08-24 Flow 하위업무 대댓글 API 보강
- 대상: `SMP_flow_store_collect.py`, `tests/test_flow_store_collect.py`, `Strategy_FlowStore_01_Collect_Dags`.
- 변경: 별도 댓글 API를 GET/POST/추가 헤더/본문 템플릿으로 설정할 수 있게 확장하고, 설정값을 env/Airflow Variable/DAG conf에서 읽도록 보강. `REPLY_CNT` 대비 누락된 부모 댓글별 대댓글은 API 응답에서 `replyRemarks`로 병합.
- 검증 결과: 공식 `api.flow.team/user/comments/{post_id}` 및 `/replies/{comment_id}`로 `83070915` 댓글 9건 수집 확인. `tests/test_flow_store_collect.py`, `tests/test_flow_visit_mart.py` 55개 통과, FlowStore parent repair run 및 FlowVisit mart run 성공.
- 남은 위험: 공식 댓글 API의 top 댓글 응답에는 대댓글 수가 없어, 댓글 API로 가져온 top 댓글은 replies API를 추가 조회함. 변경 게시글 댓글 수가 매우 많으면 호출량이 늘 수 있음.

## 2026-08-24 unified_sales ToOrder 부분 수집 컷오프 보강
- 대상: `DB_UnifiedSales_validate.py`, `tests/test_unified_sales_validate_alert.py`.
- 변경: ToOrder 최신일을 단순 max date가 아니라 최근 7일 중앙값 대비 매장수/행수/금액 품질로 판정해 부분 수집일은 월별 비교 컷오프에서 제외. 일별 검증도 부분 수집 의심일은 CSV 저장 전 보류.
- 검증 결과: `tests/test_unified_sales_validate_alert.py` 8개 통과, `DB_UnifiedSales_Dags` import 통과, 실측 ToOrder 2026-08-24는 stores 14/61·rows 19/218·total 1,629,600/53,955,700으로 제외되고 max_date 2026-08-23 확인.
- 남은 위험: 정상 저매출 특수일이 임계값 미만이면 보류될 수 있어 운영 중 임계값 조정 가능성 있음.

## 2026-08-25 delivery_commission 재수집 트리거 중복 예외 복구
- 대상: `DB_DeliveryCommission.py`.
- 변경: 배민 orders-only 재수집 트리거 중 Airflow 메타DB 중복 조회가 `MultipleResultsFound`를 내도 기존 DAG run으로 간주하고 계속 진행하도록 예외 처리 보강.
- 검증 결과: `py_compile`, 컨테이너 DAG import, 함수 단위 예외 재현 통과. 재실행 run `manual__2026-08-25T03:14:28.237521+00:00`에서 `trigger_baemin_orders_only_recollect` 성공 확인.
- 남은 위험: Airflow 메타DB에 실제 중복 dag_run 행이 남아 있을 수 있어, 같은 경고가 반복되면 메타DB 중복 정리는 별도 수동 점검 필요.

## 2026-08-25 Flow 방문 하위업무 계층 확장
- 대상: `SMP_flow_visit_mart.py`, `tests/test_flow_visit_mart.py`, `docs/db-schema.md`.
- 변경: `flow_visit_subtask`에 방문일지 직속 하위업무뿐 아니라 하위업무의 하위업무까지 포함하고, 직접 부모와 depth/path 계층 컬럼을 추가.
- 검증 결과: `tests/test_flow_visit_mart.py` 28개 통과, `py_compile` 및 UTF-8 읽기 통과. 승인 후 FlowStore repair run `manual__codex_flow_nested_repair_20260825T142130` 성공, FlowVisit mart run `manual__codex_flow_nested_mart_20260825T142340` 성공.
- 산출물 결과: 대화점 `flow_post`에 TEST 업무 `83882023` 수집 확인, `flow_visit_subtask/project_id=2548064` 3행 재생성. TEST row는 `parent_post_id=83047164`, `direct_parent_post_id=83070915`, `subtask_depth=2`로 저장됨.
- 남은 위험: Windows 로컬 DAG import는 Airflow 설정 파일 인코딩 오류로 실패하므로 컨테이너 기준으로 확인. 다른 프로젝트 파티션은 다음 mart 재실행 시 새 스키마 반영.

## 2026-08-26 DB_UnifiedSales 08:10 이전 완료 목표 스케줄 보정
- 대상: `DB_UnifiedSales_Dags.py`, `schedule.py`.
- 변경: UnifiedSales 스케줄을 08:10에서 07:40으로 앞당기고, 가드는 08:15로 조정. ToOrder store/platform은 06:45, 배민 upload는 07:20으로 조정. UnifiedSales 초반에 기존 쿠팡 move/load 적재 task를 선행 연결.
- 검증 결과: UTF-8 읽기, `py_compile`, 컨테이너 DAG import 통과. UnifiedSales/가드/ToOrder/배민 upload 스케줄이 각각 07:40/08:15/06:45/07:20으로 반영되고 쿠팡 move→load→배민 선적재 의존성 확인.
- 남은 위험: 실제 08:10 이전 완료 여부는 다음 운영 스케줄 로그에서 확인 필요. Windows 로컬 Airflow import는 기존 airflow config 인코딩 오류로 실패.

## 2026-08-26 Flow 방문일지 bullet 요약 및 누락 글 복구 보강
- 대상: `SMP_flow_store_collect.py`, `SMP_flow_visit_mart.py`, `flow_visit_viz.py`, `flow_visit_prompts.py`.
- 변경: Flow state/parquet 불일치 게시글과 repair 프로젝트 전체를 재수집 대상으로 잡고, 방문일지 제목 공백 정규화와 하위 글/업무 컨텍스트 반영을 보강. viz 요약 컬럼은 `- 항목` 줄바꿈 bullet 문자열로 저장.
- 검증 결과: `PYTEST_DEBUG_TEMPROOT=C:\airflow\.tmp\pytest-run python -X utf8 -m pytest tests/test_flow_store_collect.py tests/test_flow_visit_mart.py -q` 60개 통과. 임시 `AIRFLOW_HOME` 기준 관련 DAG 3개 import 통과, UTF-8 읽기 확인.
- 남은 위험: OneDrive `flow_visit_viz.parquet` 재생성은 승인 전이라 미수행. Flow 목록 API가 `[글]`을 반환하지 않는 경우 URL/권한 설정 확인이 별도 필요.

## 2026-08-26 unified_sales 월별 검증 ToOrder 부분수집 복구
- 대상: `DB_Toorder_store_platform_daily.py`, `DB_Toorder_store_platform_daily_Dags.py`, `DB_UnifiedSales_validate.py`.
- 변경: ToOrder datedetail 저장 전 품질 게이트를 추가하고, 최근 lookback 안의 부분수집일을 자동 재수집 대상으로 잡도록 보강. 월별 검증은 ToOrder 부분수집일을 기준값과 unified 양쪽에서 제외하도록 조정.
- 검증 결과: 2026-08-24 ToOrder 재수집 run 성공, 19행/14매장/1,629,600원에서 173행/52매장/42,381,600원으로 회복. 2026-08 월별 CSV는 alert 대상 50곳에서 14곳으로 축소. 관련 테스트 35개, py_compile, DAG import 통과.
- 남은 위험: 2026-08-18은 ToOrder 원천에서 재수집해도 150행만 내려와 품질 게이트가 저장을 차단함. 월별 CSV는 2026-08-18 제외 기준이며, 잔여 14곳은 원천별 실제 차이로 별도 역추적 필요.

## 2026-08-26 당근 광고 CSV 단일 통합 DAG 추가
- 대상: `Marketing_DaangnAds_CSV_Dags.py`, `BSP_DaangnAds_CSV.py`, `schedule.py`.
- 변경: `daangn_ads_*.csv`를 매일 12:00에 `analytics/Daangn_ads/daangn_ads.csv` 단일 파일로 병합하고, 동일 시작일/종료일/campaign_id는 `collected_at` 최신 수집본으로 덮어쓰도록 추가. 단일 CSV 저장 성공 후 로드된 원본 CSV는 cleanup 삭제. 최종 `시작일` 최초~마지막 범위에서 빠진 날짜가 있으면 `광고그룹명`별로 사용자 텔레그램과 차보령 대리 이메일에 입력/저장 위치를 안내.
- 검증 결과: `python -m pytest tests/test_daangn_ads_csv.py --basetemp .tmp/pytest-daangn -p no:cacheprovider` 6개 통과, py_compile 및 DAG import 통과. Airflow worker 컨테이너 dry-run에서 메시지가 `수집 그룹: 그룹A` 블록을 포함하고 과거 전체 범위 단독 형식이 아님을 확인.
- 남은 위험: 실제 OneDrive 대상 CSV 생성은 DAG 운영 실행 시 확인 필요.

## 2026-08-26 광고 프로젝트 성과 추적 마트(캠페인 테이블 + 시각화 마트) 추가
- 대상: `Marketing_AdsTracking_01_Mart_Dags.py`, `BSP_MarketingAds_Mart.py`, `paths.py`, `schedule.py`, `tests/test_marketing_ads_mart.py`.
- 변경: Flow 게시글 제목 규칙(`[채널_광고] [campaign_id:...] 프로젝트명`)으로 광고 프로젝트를 파싱하고, 네이버(캠페인 ID 원본) / 당근(광고그룹명)으로 일별 실적과 연결. `mart/Marketing_Ads_Tracking`에 `marketing_ads_daily.csv`(일자x프로젝트x채널)와 `marketing_ads_campaign.csv`(프로젝트 1행 + 기간 성과)를 매일 12:30 생성. CTR/CPC는 항상 재계산하고, 미등록·기간외 실적도 라벨을 달아 보존해 광고비 총액이 새지 않게 함.
- 검증 결과: 테스트 7개 통과, DAG import 통과, `harness_cli validate` 통과. 실데이터 드라이런(.tmp)에서 원천 광고비 합 10,375,825원 = 마트 합 일치, 노출수도 채널별 일치. Flow 등록 1건 시뮬레이션 시 기간 내 실적만 집계됨(네이버 7일/당근 10일) 확인.
- 남은 위험: 현재 Flow에 규칙에 맞는 게시글이 0건이라 캠페인 테이블은 담당자 등록 후에야 채워진다. 실제 OneDrive 마트 저장은 승인 전이라 미수행. `naverads_adgroups_2026.csv` 생성 주체가 저장소 밖이라 갱신 중단 시 조용히 과거 데이터로 굳을 수 있음.

## 2026-08-26 광고 마트 스키마 재설계(계층 정렬 + 결측 가시화)
- 대상: `BSP_MarketingAds_Mart.py`, `paths.py`, `tests/test_marketing_ads_mart.py`.
- 변경: 산출물 점검에서 당근 `campaign_id` 칸에 광고그룹명이 들어가고 네이버 광고그룹·당근 광고명이 롤업으로 사라진 문제를 확인해 스키마를 재설계. 두 채널 계층을 campaign/adgroup/ad 3단으로 정렬하고(네이버=일자x광고그룹, 당근=일자x소재로 원본 그레인 유지), `ad_type`을 캠페인 ID prefix(a001-01=파워링크, a001-06=플레이스)로 자동 분류. 프로젝트 기간의 날짜를 결측(채널 수집 없음)/가동(노출>0)/미집행 3분류해 `missing_days`·`coverage_rate`·`data_complete`·`missing_dates`로 노출. 매칭 끊김 복구용 `campaign_link_manual.csv` 입력 파일을 추가.
- 검증 결과: 테스트 15개 통과, DAG import 통과. 실데이터 드라이런에서 광고비 10,375,825원·노출(네이버 4,192,144 / 당근 92,212) 원천과 일치, 행수 1,972(=원천 그대로), 광고그룹 23 / 소재 31, 예산도달 151행·52일 원천 일치. 결측 시뮬레이션에서 당근 7월 프로젝트 missing_days=31·coverage 0.00, 네이버 8/18~8/25 missing_dates=2026-08-22~2026-08-23 확인. 보정 CSV로 Flow 미등록 캠페인 2건 수동 연결 성공(예산도달 21일 원천 일치).
- 남은 위험: 당근 7월 공백의 원인(수집 중단 vs 미집행)은 미확인. 당근 link_key가 이름 문자열이라 광고그룹명 변경 시 매칭이 끊기며 보정 CSV로 복구해야 한다. 실제 OneDrive 마트 저장은 승인 전이라 미수행.

## 2026-08-26 광고 마트 채널별 커버리지 모드 분리 + OneDrive 최초 생성
- 대상: `BSP_MarketingAds_Mart.py`, `tests/test_marketing_ads_mart.py`.
- 변경: 사용자 확인 결과 당근 7월 공백은 수집 실패가 아니라 광고 미집행이었다. 네이버는 매일 전 광고그룹 스냅샷(정지도 0 노출 행 존재)이고 당근은 집행한 날만 행이 생기므로, `CHANNEL_COVERAGE_MODE`로 snapshot/event를 분리해 event 채널은 행 없는 날을 결측이 아니라 미집행(inactive)으로 판정하도록 수정.
- 검증 결과: 테스트 16개 통과. 7월 당근 프로젝트가 missing 31 -> inactive 31 / coverage 1.00 / 완전으로 정정되고, 네이버 8/22~8/23만 진짜 결측(coverage 0.75)으로 남는 것을 확인. 승인 후 OneDrive `mart/Marketing_Ads_Tracking/`에 `marketing_ads_daily.csv`(1,972행, 광고비 10,375,825원), `marketing_ads_campaign.csv`(헤더), `campaign_link_manual.csv`(템플릿) 최초 생성.
- 남은 위험: Flow 규칙에 맞는 게시글이 아직 0건이라 캠페인 테이블은 담당자 등록 후 채워진다. Windows 로컬에서는 stray 폴더 `C:/opt/airflow/analytics` 때문에 `resolve_analytics_db()`가 OneDrive 대신 그쪽을 잡는다(컨테이너는 env로 지정돼 정상). 로컬 실행 시 원천 경로를 명시해야 한다.

## 2026-08-26 네이버 광고 미수집 텔레그램 알림 추가
- 대상: `BSP_MarketingAds_Mart.py`, `Marketing_AdsTracking_01_Mart_Dags.py`, `paths.py`, `tests/test_marketing_ads_mart.py`.
- 변경: 네이버는 snapshot 소스라 그날 행이 없으면 수집 실패이므로 `notify_missing_collection()`을 추가해 `첫 수집일 ~ 전일` 구간의 빈 날짜를 감지하고 텔레그램으로 수집 요청을 보낸다. 구간 공백과 수집 지연이 한 계산으로 잡힌다. 이미 알린 날짜는 `LOCAL_DB/marketing_ads_missing_alert.json`에 남겨 재발송하지 않으며, 발송 실패 시 state를 갱신하지 않아 다음 실행에서 재시도한다. 당근은 event 소스라 미수집과 미집행을 구분할 수 없어 대상에서 제외(기존 Marketing_DaangnAds_CSV_Dags가 자체 알림 담당). DAG에 `task_notify_missing_collection`을 캠페인 테이블 뒤에 연결.
- 검증 결과: 테스트 22개 통과, DAG import 및 태스크 5개 순서 확인. 실데이터 드라이런(텔레그램 미발송, sender 주입)에서 신규 누락일 2026-08-22~2026-08-23 감지, 2회차 실행 시 발송 0건으로 반복 억제 확인.
- 남은 위험: 원천 CSV 생성 주체가 저장소 밖이라 알림 후 사람이 채워야 복구된다. 전일 기준이라 수집이 당일 오후에 도는 구조로 바뀌면 하루짜리 오알람 가능. 운영 첫 실행 시 8/22~8/23 알림 1건이 실제로 발송된다.

## 2026-08-26 Flow 대화점 과거 방문일지 복구
- 대상: `SMP_flow_store_collect.py`, `tests/test_flow_store_collect.py`, Flow 대화점 `project_id=2548064`.
- 변경: repair 프로젝트가 지정된 실행에서 다른 프로젝트의 parquet 누락 게시글까지 상세수집 대상으로 확산되던 조건을 제한하고, 대화점 Flow API 게시글을 프로젝트 repair로 재수집.
- 검증 결과: 관련 테스트 64개 통과. FlowStore repair 성공 후 대화점 `flow_post` 32행/방문일지 6건, FlowVisit mart `visit_cnt=6`, `flow_visit_viz` 대화점 31행 확인. For_AI 대화점 2026-08 JSON 1개 재생성 완료.
- 남은 위험: 최근 3회 기준 For_AI `visit_log.json`은 `2026-08-12`, `2026-06-16`, `2025-12-29`만 포함한다. 더 오래된 방문까지 For_AI에 항상 포함하려면 최근 방문 제한 정책을 별도로 조정해야 한다.

## 2026-08-26 For_AI 방문일지 전체 히스토리 반영
- 대상: `For_AI_store_month_analysis.py`, `tests/test_for_ai_store_month_analysis.py`, `Sales_For_AI_StoreMonthAnalysis_Dags`.
- 변경: For_AI `visit_log.json`이 기준월 말일 이전 최근 3회만 담던 제한을 기본 전체 히스토리 포함으로 변경. 필요 시 `FOR_AI_VISIT_HISTORY_LIMIT` env로 제한 가능.
- 검증 결과: 관련 테스트 64개 통과. 대화점 2026-08 For_AI 재생성 성공, `visit_log.json` `visit_count=6`, timeline=`2026-08-12/2026-06-16/2025-12-29/2025-11-20/2025-08-26/2025-07-08`, `visits` 31행 확인.
- 남은 위험: 방문 이력이 많은 매장은 `visit_log.json` 크기가 늘 수 있다. 기본 전체 포함이 과하면 env 제한값으로 조정 가능.

## 2026-08-26 방문일지 범용 라벨 표시 제거
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_viz.py`, `For_AI_store_month_analysis.py`, 관련 테스트, FlowVisit/For_AI 대화점 산출물.
- 변경: `[글]` 방문일지에서 구조화된 이슈명이 없을 때 붙던 `방문일지 주요 내용` 라벨을 사용자 화면용 concern/handling/followup/issue_label에서 제거. 본문 요약은 유지하되 `방문일지 주요 내용:` 접두어와 `방문일지 주요 내용 처리 상태 확인` 같은 기계적 문구가 생성되지 않게 조정. Power BI 불릿 리스트 formatter에서도 `주요내용 :`, `상담 내용:`처럼 `내용:`으로 끝나는 접두어를 제거해 불릿 본문만 남김.
- 검증 결과: 관련 테스트 68개 통과. FlowVisit 대화점 재생성 후 `flow_visit_viz.parquet` 31행에서 범용 라벨 문자열 0건, `내용:` 접두어 0건 확인. For_AI 대화점 `visit_log.json`도 `visit_count=6`, `visits` 31행, 범용 라벨 0건 확인.
- 남은 위험: 구체 이슈로 분류되지 않은 행은 For_AI 요약에서 `미분류`로 집계될 수 있다. 화면에서 이 표현도 숨겨야 하면 별도 표시 정책을 적용해야 한다.

## 2026-08-26 광고 마트 채널 규격 통일(ad_id 기준)
- 대상: `BSP_MarketingAds_Mart.py`, `tests/test_marketing_ads_mart.py`.
- 변경: 채널별로 빈칸이 많던 스키마를 통일. `ad_id`를 "그 채널의 가장 작은 실측 단위"로 정의해 네이버는 광고그룹(grp-*), 당근은 소재(dg_*)를 넣고 실제 레벨은 `ad_level`(adgroup/ad)로 표시. 네이버 `ad_name`에 광고그룹 이름을, 당근 `campaign_id`에 광고그룹명을 채우고 `campaign_id_kind`(id/name)로 이름 대체임을 명시. 실적 행이 없는 캠페인도 채널로 결정되는 컬럼(campaign_id/campaign_id_kind/ad_level)은 채운다. 프로젝트 매칭은 기존대로 캠페인 레벨 `link_key`라 성과 집계 로직은 변경 없음.
- 검증 결과: 테스트 26개 통과, DAG import 통과. 실데이터 회귀에서 광고비 10,375,825원·노출(네이버 4,192,144 / 당근 92,212)·행수 1,972 모두 통일 전과 동일. ad_id 유니크 네이버 23 / 당근 31, ad_id·campaign_id 빈값 0건, `link_key == campaign_id` 전 행 성립. 매칭 회귀도 송파삼전점 복날 109,865/203/65,877, 삼전점 오픈 당근 31,812/218/47,959로 일치하며 네이버 `ad_cnt`가 0에서 6으로 채워짐. 커버리지·예산도달(52일)도 동일. OneDrive 마트 2개 재생성 완료.
- 남은 위험: 네이버는 `adgroup_id == ad_id`라 중복으로 오해할 수 있어 `ad_level`과 docstring으로 근거를 남겼다. 나중에 네이버 키워드/소재 리포트를 수집하면 `ad_id` 의미가 광고그룹에서 소재로 내려가므로 과거 데이터 재적재 여부를 판단해야 한다.

## 2026-08-26 네이버 광고 3depth 키워드 수집 전환
- 대상: `runner_naverads.js`, `runner_naverads.html`, `content/09_naverads.js`, `content/05_main.js`, `BSP_MarketingAds_Mart.py`, `tests/test_marketing_ads_mart.py`.
- 변경: 네이버 광고 자동수집 루트를 플레이스/파워링크로 분리하고 ON 캠페인만 발견한 뒤, 캠페인 상세의 ON 광고그룹을 최대 3개 병렬로 열어 키워드 표까지 3depth 수집한다. CSV에는 기존 필수 컬럼을 유지하면서 `채널=네이버 광고`, `광고유형`, `depth번호=3`, `키워드 ID`, `키워드`를 추가. 마트 로더는 키워드 컬럼이 있으면 `ad_level=keyword`, 없으면 과거 `adgroup` 호환으로 처리.
- 검증 결과: `node --check` 3개 파일 통과, Python `py_compile` 통과, `PYTHONPATH=.` 및 저장소 `.tmp` basetemp 기준 `tests/test_marketing_ads_mart.py` 27개 통과, UTF-8 읽기 확인.
- 남은 위험: 실제 네이버 광고 DOM에서 키워드 표의 지표 컬럼 순서나 ON 스위치 셀렉터가 다르면 첫 브라우저 실행 로그의 키워드/광고그룹 진단을 기준으로 셀렉터 보정 필요. 확장 리로드 후 실제 계정에서 수동 E2E 확인이 필요하다.

## 2026-08-26 네이버 광고 campaigns-by discover timeout 수정
- 대상: `content/09_naverads.js`, `runner_naverads.js`.
- 변경: `/sa/campaigns-by/PLACE`, `/sa/campaigns-by/WEB_SITE`를 목록 discover 페이지로 처리하고, `data-row-key`가 없는 표에서도 캠페인 링크(`/sa/campaigns/{cmpId}`)로 행과 ID를 추출하도록 fallback을 추가. 목록 진단 로그에 URL, colgroup, 캠페인 링크 샘플을 포함하고, discover 중 `UNKNOWN_PAGE` 완료 응답은 timeout까지 기다리지 않고 즉시 실패 처리.
- 검증 결과: `node --check` 2개 파일 통과, UTF-8 읽기 확인.
- 남은 위험: 실제 campaigns-by 화면에서 캠페인 링크가 다른 라우트 형태이면 새 진단 로그의 `campaignLinks`를 기준으로 추가 보정 필요.

## 2026-08-26 네이버 광고 CSV 분할 저장 및 OFF 수집 보정
- 대상: `runner_naverads.js`, `content/09_naverads.js`.
- 변경: 광고그룹마다 CSV를 저장하던 흐름을 캠페인+날짜별 통합 CSV 1개 저장으로 변경하고, 캠페인/광고그룹 OFF 상태도 수집 대상에 포함. 광고그룹 상세 진입 후 키워드 탭 클릭을 시도하고, `전체 결과`, `소재 N개 결과`, `ADVoosst Max 결과` 같은 요약/소재 행은 키워드 행에서 제외.
- 검증 결과: `node --check` 2개 파일 통과, UTF-8 읽기 확인.
- 남은 위험: 실제 네이버 화면의 키워드 탭 label이 다른 경우 새 라이브 로그의 `키워드 탭 클릭 대상 없음` 및 키워드 진단을 기준으로 탭 셀렉터 추가 보정 필요.

## 2026-08-26 네이버 광고 작업 탭 1개 고정
- 대상: `runner_naverads.js`.
- 변경: 광고그룹 키워드 수집 동시성을 3개에서 1개로 낮추고, 목록/캠페인/광고그룹 작업 탭을 성공·실패와 관계없이 `finally`에서 닫도록 정리. 실패 시 탭을 남기던 로그를 진단 로그 기록 후 탭 닫음으로 변경.
- 검증 결과: `node --check runner_naverads.js` 통과, UTF-8 읽기 확인.
- 남은 위험: 실패 원인은 탭이 아니라 `naverads_batch_*.log` 진단 로그로 확인해야 한다. 실제 Chrome에서 탭이 동시에 1개만 열리는지는 확장 리로드 후 수동 실행으로 확인 필요.

## 2026-08-26 네이버 광고 작업 탭 병렬 3개 복원
- 대상: `runner_naverads.js`.
- 변경: 탭 증가 원인은 병렬성이 아니라 실패 탭 보존이었으므로, 실패/성공 시 탭 닫기 정책은 유지하고 광고그룹 키워드 수집 동시성을 3개로 복원. 라이브 로그도 최대 3개 병렬 및 완료 즉시 탭 닫음으로 정정.
- 검증 결과: `node --check runner_naverads.js` 통과, UTF-8 읽기 확인.
- 남은 위험: 실제 Chrome에서 3개를 초과하지 않고 완료 즉시 닫히는지는 확장 리로드 후 수동 실행으로 확인 필요.

## 2026-08-26 네이버 광고 데이터 없음 정상 스킵 처리
- 대상: `content/09_naverads.js`, `runner_naverads.js`.
- 변경: 캠페인 상세가 정상 렌더링됐지만 선택 기간 광고그룹 행이 0개인 경우를 실패가 아니라 `empty:true` 응답으로 처리하고, 러너에서는 `광고그룹 데이터 없음` 스킵으로 표시. OFF 광고 수집 정책은 유지하되 기간 내 데이터 없음은 실패 카운트에서 제외.
- 검증 결과: `node --check` 2개 파일 통과, UTF-8 읽기 확인.
- 남은 위험: 실제 오류인데 화면 일부만 렌더된 경우도 스킵될 수 있으므로, 반복적으로 예상 캠페인에서 스킵되면 해당 로그의 상세 진단을 확인해야 한다.

## 2026-08-27 네이버 광고 광고그룹 발견 ready 조건 복구
- 대상: OneDrive 개발용 확장 `content/09_naverads.js`.
- 변경: `_waitForAdgroupDataReady`에 잘못 들어간 `state.tableKind === 'keyword'` 조건을 제거해 캠페인 상세의 광고그룹 목록이 `rowCount > 0 && hasMetricText`이면 안정화 완료로 처리되도록 복구.
- 검증 결과: `content/09_naverads.js`, `runner_naverads.js` `node --check` 통과, ready 조건 라인 확인.
- 남은 위험: Chrome 확장 리로드 후 재실행 로그에서 `광고그룹 데이터 준비 완료`와 `광고그룹 발견 완료`가 나오는지 확인해야 한다.

## 2026-08-27 네이버 광고 3depth CSV 오염 차단
- 대상: OneDrive 개발용 확장 `content/09_naverads.js`, `runner_naverads.js`.
- 변경: 파워링크 키워드 수집 중 소재/요약 테이블 행을 키워드로 저장하지 않도록 판별을 강화하고, 플레이스 소재 행의 지표값이 소재 ID/상태/입찰가 컬럼에 밀려 들어가는 fallback을 차단. runner 저장 직전 오염 행 검증을 추가.
- 검증 결과: 두 JS 파일 `node --check` 통과, UTF-8 PowerShell 읽기 확인, 합성 오염 패턴(숫자 키워드·소재 UI 문구·금액형 소재 ID) 차단 확인.
- 남은 위험: 실제 Chrome 확장 리로드 후 2026-08-25 재수집으로 `2607_1km반경`, `✅ 플레이스#5_광역_주말`의 depth1/그룹2/그룹3 CSV 계층을 확인해야 한다.

## 2026-08-27 DB_Beamin_Macro retry_failed 타임아웃 방지
- 대상: `modules/transform/pipelines/db/DB_Beamin_combined.py`.
- 변경: `retry_once_failed`의 orders 1차 재시도 후 잔여 실패를 같은 태스크에서 2차 재수집하지 않고 `residual_failed`로 보존하도록 수정.
- 검증 결과: 런타임 DAG import 통과, 컨테이너에서 스텁 재현으로 orders 재호출 1회 및 잔여 실패 반환 확인, UTF-8 읽기 확인.
- 남은 위험: 배민 화면의 정산정보 0%/세션 끊김은 외부 화면 상태 영향이 있어 잔여 매장은 후속 재시도 또는 수동 확인이 필요할 수 있다.

## 2026-08-26 Flow 방문일지 시각화 이슈 상세 컬럼 보강
- 대상: `SMP_flow_visit_mart.py`, `flow_visit_prompts.py`, `flow_visit_viz.py`, `tests/test_flow_visit_mart.py`.
- 변경: 프롬프트 버전을 갱신하고 `analysis_evidence`를 key concern 대응 상세 JSON으로 확장. 시각화 테이블 끝에 행 단위 문제·조치·근거·후속 상세 및 관련 이슈 컬럼 추가.
- 검증 결과: `python -X utf8 -m pytest tests/test_flow_visit_mart.py --basetemp .tmp\pytest-flow-visit` 35건 통과, 임시 `AIRFLOW_HOME` 기준 DAG import 통과, UTF-8 읽기 확인.
- 남은 위험: 실제 OneDrive parquet 재생성은 별도 승인 후 `FLOW_VISIT_FORCE_REBUILD=1`, `FLOW_VISIT_PROFILE_PROVIDER=local`로 실행해야 하며 Power BI 집계는 `visit_rel_key` distinct 기준을 유지해야 한다.

## 2026-08-26 Flow 방문일지 v12 산출물 재생성
- 대상: `Sales_FlowVisit_01_Mart_Dags`, OneDrive `Flow_mart/Flow_visit` 산출물.
- 변경: 사용자 승인 후 `FLOW_VISIT_FORCE_REBUILD=1`, `FLOW_VISIT_PROFILE_PROVIDER=local`, `gpt-oss:20b` 기준으로 DAG 전체 재실행.
- 검증 결과: run `manual__2026-08-26T09:25:00+00:00` 전 태스크 성공, `flow_visit_viz.parquet` 117행 60컬럼, `prompt_version=flow_visit_v12_issue_detail_viz`, 신규 issue 상세 컬럼 6개 채움 확인.
- 남은 위험: 품질검사 recall은 기존 기준 70.0%로 유지되어, 누락 케이스 25건은 별도 taxonomy/quality 개선 작업으로 봐야 한다.

## 2026-08-26 네이버 광고 플레이스 3depth 소재 수집 안정화
- 대상: `runner_naverads.js`, `content/09_naverads.js`, `BSP_MarketingAds_Mart.py`, `tests/test_marketing_ads_mart.py`.
- 변경: 광고그룹 데이터 없음 timeout race를 방지하도록 러너 대기시간을 분리하고, 플레이스 광고그룹은 키워드 탭 대신 소재 탭을 depth 3으로 수집. CSV에 `소재 ID`, `소재` 컬럼을 추가하고 마트에서 네이버 플레이스 소재를 `ad_level=creative`로 처리.
- 검증 결과: `node --check` 2개 확장 파일 통과, `python -m pytest tests/test_marketing_ads_mart.py --basetemp=.tmp/pytest-naverads` 28건 통과.
- 남은 위험: 실제 네이버 소재 행의 ID/명칭 DOM이 계정별로 다르면 소재명 fallback이 필요할 수 있으므로 첫 실행 로그의 `플레이스 소재 정제 데이터 추출 완료` 행 수와 CSV 소재 컬럼을 확인해야 한다.

## 2026-08-26 네이버 광고 파워링크 빈 소재 테이블 오인식 수정
- 대상: `content/09_naverads.js`, `runner_naverads.js`.
- 변경: 파워링크 키워드 수집 중 `등록한 소재가 없습니다` 빈 소재 테이블을 키워드 행으로 세지 않도록 제외하고, 키워드 데이터 없음은 실패가 아니라 `스킵`으로 처리.
- 검증 결과: `node --check` 2개 확장 파일 통과.
- 남은 위험: 실제 네이버 화면에서 키워드 탭 클릭 후에도 소재 탭이 유지되는 계정은 정상 스킵되므로, 기대 키워드가 있는 광고그룹이 스킵되면 해당 진단 로그의 `keywordTabVisible`, `creativeEmptyVisible` 값을 확인해야 한다.

## 2026-08-26 네이버 광고 날짜 적용 후 탭 복귀 방지
- 대상: `content/09_naverads.js`, `runner_naverads.js`.
- 변경: 날짜 필터 설정 후 파워링크는 키워드 탭, 플레이스는 소재 탭을 다시 열도록 보강. 키워드 대기 중 소재 테이블 감지 시 재클릭 후 빈 데이터는 스킵 처리하고, stale 확장 확인용 runner/content version 로그를 추가.
- 검증 결과: `node --check` 2개 확장 파일 통과, `update_log.md` UTF-8 읽기 확인.
- 남은 위험: 확장 리로드 전 실행한 로그에는 버전 로그가 보이지 않으므로, 재실행 첫 로그에서 `runner version`, `content version`이 보이는지 확인해야 한다.

## 2026-08-27 배민 수동 orders CSV Airflow 선적재 반영
- 대상: `DB_Beamin_Macro_Upload_Dags`, `DB_Beamin_Macro_Upload_Validate_Dags`, `DB_Beamin_Macro_Dags_Retry`, `DB_Beamin_Macro_Lookback_Trigger_Dags`, `DB_BaeminManual_load`, `DB_Beamin_Macro_upload`.
- 변경: 확장 수동수집 산출물 `baemin_orders_*.csv`를 upload validate, retry, lookback 판단 전에 기존 수동 적재 로직으로 선적재하고 cleanup까지 연결. upload gate는 inbox 적재 폴더가 없어도 수동 orders CSV가 있으면 validate로 진행.
- 검증 결과: `py_compile` 통과, `python -X utf8 -m pytest --basetemp C:\airflow\.tmp\pytest\run tests/test_baemin_manual_load.py tests/test_baemin_macro_validation.py tests/test_baemin_pc2_upload_trigger.py tests/test_beamin_retry_conf.py tests/test_baemin_orders_date_filter_abort.py` 72건 통과.
- 남은 위험: OneDrive 개발용 확장은 수정하지 않았으므로 실제 브라우저 수동수집 파일명/컬럼 계약은 현재 확장 구현(`baemin_orders_{store}_{storeId}_{yyyymmdd}.csv`) 유지가 전제다.

## 2026-08-27 배민 수동수집 즉시할인 총계 파싱 수정
- 대상: OneDrive 개발용 확장 `content/02_baemin.js`.
- 변경: 즉시할인 시트의 `총 할인금액/파트너 부담/배민 지원` 총계 블록을 우선 파싱하고, 성공 시 개별 할인 항목 합산을 건너뛰도록 수정해 배민 지원액 중복 합산을 방지.
- 검증 결과: `node --check` 통과, UTF-8 읽기 통과, 제공된 시트 원문 샘플에서 `파트너=1,600`, `배민지원=5,200` 추출 확인.
- 남은 위험: 실제 화면에서 총계 DOM/문구가 바뀌면 기존 개별 항목 합산 폴백으로 동작하므로 첫 재수집에서 합계불일치 상세 로그 잔존 여부를 확인해야 한다.

## 2026-08-27 배민 수동수집 즉시할인 실패 주문 재시도
- 대상: OneDrive 개발용 확장 `content/02_baemin.js`.
- 변경: 즉시할인 상세 시트 `시트없음/내용미갱신/파싱실패` 발생 시 같은 주문에서 최대 2회 추가 재시도하고, 최종 실패 때만 공란 카운트를 올리도록 수정.
- 검증 결과: `node --check` 통과, UTF-8 읽기 통과, 재시도 로그(`즉시할인 재시도`, `재시도 성공/실패`) 출력 경로 확인.
- 남은 위험: 실제 Chrome 확장 리로드 전 실행에는 반영되지 않으며, 다음 수집에서 재시도 성공 시 `공란 0건`으로 줄어드는지 확인해야 한다.

## 2026-08-27 배민 수동수집 즉시할인 열기 재시도 5회 확대
- 대상: OneDrive 개발용 확장 `content/02_baemin.js`.
- 변경: 즉시할인 상세 시트 열기/파싱 시도를 최초 포함 총 5회로 확대하고, 후속 시도 timeout을 6초/7초까지 늘림.
- 검증 결과: `node --check` 통과, UTF-8 읽기 통과, 5회차 설정(`timeoutMs=7000`, `delayMs=1000`) 확인.
- 남은 위험: 확장 리로드 전 실행에는 반영되지 않으며, 최종 실패 주문은 여전히 공란으로 남긴다.

## 2026-08-27 배민 수동수집 즉시할인 공란 페이지 재수집
- 대상: OneDrive 개발용 확장 `content/02_baemin.js`.
- 변경: 주문 단위 5회 재시도 후에도 즉시할인 공란이 있으면 해당 페이지 rows를 저장하지 않고 이전/원래 페이지 왕복으로 DOM을 재생성한 뒤 최대 2회 페이지 재수집. 최종 공란 잔존 시 정상 적재 금지 상태로 중단.
- 검증 결과: `node --check` 통과, UTF-8 읽기 통과, 페이지 재수집 로그와 실패 주문 상세 전달 경로 확인.
- 남은 위험: 첫 페이지 또는 페이지네이션 표시가 제한된 화면에서 왕복 재로딩이 실패하면 공란 CSV 저장 대신 부분 수집으로 종료된다.

## 2026-08-27 배민 수동수집 즉시할인 재시도 정책 조정
- 대상: OneDrive 개발용 확장 `content/02_baemin.js`.
- 변경: 즉시할인 시트 열기 시도는 최초 포함 3회로 줄이고, 페이지 전체 재수집은 최초 포함 3회로 유지해 같은 DOM에 오래 붙잡히지 않도록 조정.
- 검증 결과: `node --check` 통과, UTF-8 읽기 통과, 주문 시도 3회와 페이지 시도 3회 조건 확인.
- 남은 위험: 확장 리로드 전 실행에는 반영되지 않으며, 페이지 3회 모두 실패하면 정상 적재 금지로 종료된다.

## 2026-08-27 배민 수동 orders 백필 날짜 인식 보강
- 대상: `DB_BaeminManual_load`, `DB_Beamin_Macro_upload`, `DB_Beamin_Macro_Upload_Dags`, 관련 배민 upload/manual 테스트.
- 변경: 수동 `baemin_orders_*.csv`의 실제 `주문시각` 날짜 목록을 XCom payload로 전달하고, upload validate의 수동 사전검증/ToOrder 검증/retry 트리거가 여러 백필 날짜를 날짜별로 처리하도록 보강.
- 검증 결과: `py_compile` 통과, 임시 `AIRFLOW_HOME` DAG import 통과, `python -X utf8 -m pytest --basetemp C:\airflow\.tmp\pytest\backfill_full tests/test_baemin_manual_load.py tests/test_baemin_macro_validation.py tests/test_baemin_pc2_upload_trigger.py tests/test_beamin_retry_conf.py tests/test_baemin_orders_date_filter_abort.py` 77건 통과.
- 남은 위험: 배민 수동 백필은 파일명 날짜보다 CSV 내부 `주문시각`을 기준으로 검증하므로, 확장 산출물에 여러 날짜가 섞이면 날짜별 retry가 여러 DAG run으로 분리된다.

## 2026-08-27 delivery_commission 배민 즉시할인 정산 반영
- 대상: `DB_DeliveryCommission`, `tests/test_delivery_commission.py`.
- 변경: 배민 `배민_즉시할인`을 `즉시할인_파트너부담` 원본값만으로 집계하고, 배민 `settlement_amount` 계산에서 해당 금액을 차감하도록 수정.
- 검증 결과: `py_compile` 통과, `tests/test_delivery_commission.py` 30건 통과, 컨테이너 원본 528개 parquet 모두 `즉시할인_파트너부담` 컬럼 존재 확인. 승인 후 OneDrive mart 갱신 완료: 25,886행, 배민 즉시할인 합계 217,197,060원.
- 남은 위험: 배민 정산예정금액 결측 136행은 기존 정책대로 null 유지되며, 별도 orders 재수집 대상이다.

## 2026-08-27 배민 수동 partial orders 적재 차단
- 대상: OneDrive 개발용 확장 `content/02_baemin.js`, `DB_BaeminManual_load`, `DB_Beamin_Macro_upload`, 관련 배민 upload/manual 테스트.
- 변경: 재시도 후에도 부분 수집이면 확장 저장 파일명에 `_partial` suffix를 붙이고 실패 응답으로 반환. Airflow 수동 적재는 `_partial.csv`를 정상 대기/적재/cleanup 대상에서 제외하고 partial 목록으로만 보고.
- 검증 결과: `node --check` 통과, `py_compile` 통과, 임시 `AIRFLOW_HOME` DAG import 통과, `python -X utf8 -m pytest --basetemp C:\airflow\.tmp\pytest\partial_retry tests/test_baemin_manual_load.py tests/test_baemin_pc2_upload_trigger.py tests/test_baemin_macro_validation.py tests/test_beamin_retry_conf.py tests/test_baemin_orders_date_filter_abort.py` 79건 통과.
- 남은 위험: Chrome 확장 리로드 전 실행에는 `_partial` 파일명 계약이 적용되지 않으므로 리로드 후 첫 수집 파일명을 확인해야 한다.

## 2026-08-27 배민 수동수집 즉시할인 시트 열기 보강
- 대상: OneDrive 개발용 확장 `content/02_baemin.js`.
- 변경: 즉시할인 열기 대상을 금액 span, Tooltip, 래퍼 순서로 넓히고 pointer/keyboard 보조 이벤트를 추가. 열린 시트가 주문 총액과 일치하면 신규 판정 실패 시에도 해당 주문 시트로 인정하며, 페이지 재시작은 추가 3회로 조정.
- 검증 결과: `node --check` 통과, UTF-8 읽기 통과, 핵심 재시도/페이지 재시작 문자열 확인.
- 남은 위험: 배민 화면 이벤트 정책이 다시 바뀌면 시트 후보 로그의 `sheets/shells/shell` 값을 보고 트리거 판정을 추가 보강해야 한다.

## 2026-08-27 네이버광고 전체 재수집/저장 검증 보강
- 대상: OneDrive 개발용 확장 `runner_naverads.js`, `content/09_naverads.js`.
- 변경: 상세 수집 실패와 fallback-only 결과를 성공 저장으로 처리하지 않고, 저장 전 `depth2 > 0 && depth3 == 0` 캠페인을 실패로 판정해 전체 배치를 최대 3회 재수집하도록 보강. 키워드/소재 탭 클릭 대상과 선택 탭 진단 로그도 추가.
- 검증 결과: 두 JS 파일 `node --check` 통과, 새 runner/content version 및 `MAX_COLLECTION_ATTEMPTS=3`, 저장 전 depth3 검증 문자열 확인.
- 남은 위험: Chrome 확장 리로드 전 실행 탭은 이전 코드로 동작하며, 3회 모두 키워드 탭이 소재 표로 잡히면 CSV를 저장하지 않고 최종 실패 로그만 남긴다.

## 2026-08-27 배민 수동 orders partial/nodate 저장 계약 보강
- 대상: OneDrive 개발용 확장 `content/02_baemin.js`, `baemin_manual.js`, 배민 수동 적재 테스트.
- 변경: URL 날짜 없는 orders 수집 파일에 `_nodate`를 표시하고, 재시도 실패/중단 partial은 `_nodate_partial` 또는 `_partial`로 저장 후 정상 적재 금지 상태로 반환하도록 보강.
- 검증 결과: `node --check` 통과, `py_compile` 통과, 임시 `AIRFLOW_HOME` DAG import 통과, `python -X utf8 -m pytest --basetemp C:\airflow\.tmp\pytest\baemin_partial_nodate tests/test_baemin_manual_load.py tests/test_baemin_pc2_upload_trigger.py tests/test_baemin_macro_validation.py tests/test_beamin_retry_conf.py tests/test_baemin_orders_date_filter_abort.py` 79건 통과.
- 남은 위험: Chrome 확장 리로드 전 실행에는 새 파일명 suffix와 partial 표시가 반영되지 않는다.

## 2026-08-27 Flow 방문일지 viz 컬럼 정리
- 대상: `flow_visit_viz`, `Sales_FlowVisit_01_Mart_Dags`, `docs/db-schema.md`, 관련 Flow 방문일지 테스트.
- 변경: 표시용 최종 스키마를 고정하고 내부/debug 컬럼과 `problem_summary`, `linked_followup_cnt`를 출력에서 제외. `concern_text`는 화두 연결용으로 유지하고 `followup_summary`는 문제·고민 행만 값, 비문제 화두는 `NULL`로 변경.
- 검증 결과: `tests/test_flow_visit_mart.py` 53건 통과, `tests/test_for_ai_store_month_analysis.py` 5건 통과, 임시 `AIRFLOW_HOME`에서 DAG import 통과.
- 남은 위험: OneDrive `flow_visit_viz.parquet` 산출물은 승인 전이라 아직 재생성하지 않았다.

## 2026-08-27 배민 수동 orders 저장 완료 대기/로그 보강
- 대상: OneDrive 개발용 확장 `content/02_baemin.js`.
- 변경: 주문 CSV 저장 시 `Utils.downloadCSV`를 `await`하도록 수정하고, 마지막 페이지 도달/CSV 저장 시작/저장 완료 로그를 UI 로그에 남기도록 보강.
- 검증 결과: `node --check` 통과, UTF-8 읽기 확인.
- 남은 위험: 이미 실행 중인 Chrome 확장 탭은 확장 리로드 전까지 이전 코드로 동작할 수 있다.

## 2026-08-27 Flow 방문일지 문제·고민 판정 보완 및 대화점 재처리
- 대상: `SMP_flow_visit_mart`, `flow_visit_prompts`, `flow_visit_viz`, 대화점 Flow 방문일지 mart.
- 변경: 미해결·진행중 상태만으로 문제·고민을 강제하지 않고 점주 불편·부담·우려 근거가 있을 때만 `is_problem`으로 보도록 보완. `handover_summary`는 반복 성향·주의 화두·기존 대응 중심으로 분리하고, 단일 파일 산출물은 대상 프로젝트 행만 교체하도록 병합 저장을 추가.
- 검증 결과: `tests/test_flow_visit_mart.py` 54건, `tests/test_for_ai_store_month_analysis.py` 5건 통과. 대화점만 재처리해 `flow_visit_viz.parquet` 117행 유지, 대화점 31행 갱신, `followup_summary.notna()==concern_is_problem` 확인.
- 남은 위험: 품질검사 평균 회수율은 기존 기준 70.0%이며, 대화점 외 매장은 이번 재처리 대상이 아니다.

## 2026-08-27 배민 수동 orders pipeline 저장 실패 로그 보강
- 대상: OneDrive 개발용 확장 `content/01_utils.js`, `content/02_baemin.js`.
- 변경: `pipeline_captured`가 저장 완료처럼 노출되지 않게 하고, 캡처 후 실제 CSV 저장 시작/완료/실패 로그와 runtime download 오류 fallback 처리를 보강.
- 검증 결과: 두 JS 파일 `node --check` 통과, UTF-8 읽기 확인.
- 남은 위험: Chrome 확장 리로드 전 실행 탭에는 새 저장 로그와 오류 처리가 반영되지 않는다.

## 2026-08-27 Flow 방문일지 대화점 화두/문제 경계 보완
- 대상: `SMP_flow_visit_mart`, `flow_visit_viz`, `flow_visit_taxonomy`, Flow 방문일지 테스트.
- 변경: 순이익 계열을 `수익_감소체감`으로 분류하도록 alias를 보강하고, 요청사항 없음/특이사항 없음 점검 행은 viz 출력에서 제외. 조각난 반복 성향 bullet 제거와 순이익 다음 방문 액션 구체화를 추가.
- 검증 결과: `tests/test_flow_visit_mart.py` 59건, `tests/test_for_ai_store_month_analysis.py` 5건 통과. `py_compile` 통과, 임시 `AIRFLOW_HOME`에서 DAG import 통과.
- 남은 위험: OneDrive `flow_visit_viz.parquet` 대화점 재처리는 별도 승인 전이라 이번 단계에서 실행하지 않았다.

## 2026-08-27 Flow 방문일지 summary/handover MECE 보완
- 대상: `SMP_flow_visit_mart`, `flow_visit_prompts`, Flow 방문일지 테스트.
- 변경: `store_status_summary`를 수익성·광고·품질·신메뉴·실행성 등 의미 축으로 정규화해 유사 성향을 하나로 통합하고, 토더/간담회/공지 같은 운영·활동 항목은 성향에서 제외하도록 보강. `handover_summary`는 기존 자료/후속/운영 불릿 기준을 유지하되 성향·화두 복사 방지 테스트를 추가했다.
- 검증 결과: `tests/test_flow_visit_mart.py`, `tests/test_for_ai_store_month_analysis.py` 총 73건 통과. `py_compile`, 임시 `AIRFLOW_HOME` DAG import, UTF-8 읽기 검증 통과.
- 남은 위험: 운영 OneDrive parquet 재처리는 이번 구현 범위에서 실행하지 않았다.

## 2026-08-27 Flow 방문일지 대화점 MECE 재처리
- 대상: OneDrive `flow_visit_viz.parquet` 및 Flow 방문일지 Mart 산출물.
- 변경: 승인에 따라 대화점(`project_id=2548064`)만 `FLOW_VISIT_PROFILE_PROVIDER=off`로 재처리해 최신 MECE `store_status_summary`/`handover_summary` 로직을 반영했다.
- 검증 결과: 시각화 parquet 115행/47컬럼, 대화점 29행/6방문. 최신 방문 `store_status_summary`는 수익성·광고·신메뉴 3개 성향, `handover_summary`는 광고 순이익 자료 준비와 토더 공지 재점검 2개 불릿으로 확인. 성향 3개 초과/활동 성향/비불릿/템플릿 라벨 0건.
- 남은 위험: 이슈 추출 단계는 로컬 Ollama를 사용했으며 28개 세그먼트 중 fallback 4건이 발생했다.

## 2026-08-27 Flow 방문일지 handover 불릿 기준 보완
- 대상: `SMP_flow_visit_mart`, `flow_visit_prompts`, Flow 방문일지 테스트.
- 변경: `handover_summary`를 다음 방문 전 준비·기억 사항 중심의 최대 3개 짧은 불릿으로 생성/정규화하도록 수정. 성향·화두·문제 복사와 `반복 성향:` 등 템플릿 라벨은 제거하고, 후속 확인·자료 준비·운영 재확인을 MECE하게 우선한다.
- 검증 결과: `tests/test_flow_visit_mart.py`, `tests/test_for_ai_store_month_analysis.py` 총 72건 통과. 임시 `AIRFLOW_HOME` DAG import와 UTF-8 읽기 검증 통과.
- 남은 위험: 운영 OneDrive parquet 재처리는 이번 코드 변경 범위에서 실행하지 않았다.

## 2026-08-27 Flow 방문일지 대화점 재처리
- 대상: OneDrive `flow_visit_viz.parquet` 및 Flow 방문일지 Mart 산출물.
- 변경: 승인에 따라 `FLOW_VISIT_PROFILE_PROVIDER=off`로 대화점(`project_id=2548064`)만 재처리. `handover_summary` 자료/운영/후속 축 중복 제거를 추가 보강한 뒤 재처리했다.
- 검증 결과: 시각화 parquet 115행/47컬럼, 대화점 29행/6방문. 최신 방문 `handover_summary`는 `광고 동일 조건의 순이익 변동 자료 준비`, `토더 공지 확인 여부 재점검` 2개 불릿으로 확인. 불릿 초과/비불릿/템플릿 라벨 0건, `followup_summary.notna()==concern_is_problem` 통과.
- 남은 위험: 이슈 추출 단계는 로컬 Ollama를 사용했으며 28개 세그먼트 중 fallback 4건이 발생했다.

## 2026-08-27 Flow 방문일지 프로필 해석 방향 보완
- 대상: `SMP_flow_visit_mart`, `flow_visit_prompts`, Flow 방문일지 테스트.
- 변경: `store_status_summary`가 반복 횟수·이슈명 나열이 아니라 점주 반응 기반 성향 문장으로 생성되도록 보완. `handover_summary`는 라벨 템플릿 대신 자연스러운 인수인계 문장으로 변경하고, `next_visit_action` 기본값을 실행문으로 조정. 대화점만 재처리해 OneDrive `flow_visit_viz.parquet` 대상 행을 교체.
- 검증 결과: `tests/test_flow_visit_mart.py`, `tests/test_for_ai_store_month_analysis.py` 총 68건 통과. `py_compile`, UTF-8 읽기, 임시 `AIRFLOW_HOME` DAG import 통과. 산출물은 전체 115행, 대화점 29행, `followup_summary.notna()==concern_is_problem`, 빈 이슈 0건 확인.
- 남은 위험: `gpt-oss:20b` 프로필 LLM은 CUDA 오류로 운영 중 실패할 수 있어 이번 대화점 재처리는 규칙 기반 프로필로 저장했다.

## 2026-08-27 Flow 방문일지 AI 문구 압축 보완
- 대상: `SMP_flow_visit_mart`, `flow_visit_prompts`, Flow 방문일지 테스트.
- 변경: 컬럼 구조 변경 없이 `store_status_summary`를 최대 3개 성향으로 제한하고 유사 성향을 통합. `handover_summary`는 다음 방문 참고사항 중심으로 140자 내외로 줄이고, `next_visit_action`은 실행 액션 최대 2개로 제한.
- 검증 결과: `tests/test_flow_visit_mart.py`, `tests/test_for_ai_store_month_analysis.py` 총 70건 통과. `py_compile`, UTF-8 읽기, 임시 `AIRFLOW_HOME` DAG import 통과.
- 남은 위험: OneDrive `flow_visit_viz.parquet` 대화점 재처리는 별도 승인 전이라 이번 단계에서 실행하지 않았다.

## 2026-08-27 Flow 방문일지 Mart 컬럼 역할 고정
- 대상: `SMP_flow_visit_mart`, Flow 방문일지 테스트.
- 변경: HTML/Context는 제외하고 Mart AI 생성 컬럼만 대상으로, LLM이 `간담회 참석 7회` 같은 근거/빈도 표현을 반환해도 `store_status_summary`에서 제거되고 실행 액션은 최대 2개로 제한되는 후처리 테스트를 추가.
- 검증 결과: `tests/test_flow_visit_mart.py`, `tests/test_for_ai_store_month_analysis.py` 총 71건 통과. 임시 `AIRFLOW_HOME` DAG import와 UTF-8 읽기 검증 통과.
- 남은 위험: 운영 parquet 재처리는 OneDrive 수정 승인이 필요해 실행하지 않았다.

## 2026-08-27 네이버광고 파워링크 키워드 탭 전환 실패 수정
- 대상: OneDrive 개발용 확장 `content/09_naverads.js`, `runner_naverads.js`.
- 배경: `(6).log` 검증 결과 depth3 소재 파싱과 실패분 round 재시도는 정상 동작(플레이스#5 CSV depth1 1행/depth2 2행/depth3 8행, 성공 41·실패 0). 다만 파워링크 9개 캠페인의 키워드 depth3가 전부 0행이고 `표 creative` 28회·`표 keyword` 0회로, `(4).log`에도 있던 기존 결함이다.
- 원인: 탭 탐색 정규식 `/소재\s*[0-9,]*\s*개?/`가 AI 챗봇 버튼('파워링크 반응형 소재 등록 방법에 대해 물어보세요')까지 매칭해 엉뚱한 요소를 클릭했다. 플레이스는 소재 탭이 기본 활성이라 우연히 동작했고, 파워링크는 기본 탭이 소재여서 키워드로 전환하지 못해 전량 0행이 됐다.
- 변경: `_findTabCandidates()`/`_tryOpenTab()`으로 탭 탐색을 공통화하고 라벨 정확 매칭(`^키워드$`/`^소재$`/건수 표기)만 허용, 챗봇·안내 문구 제외. 클릭 후 `_waitTabSwitched()`로 전환 성공을 최대 3초 검증하고 실패 시 다음 후보를 클릭한다. 소재 테이블 감지 재클릭을 1회에서 3회로 늘리고, 소재 표가 렌더된 채 전환에 실패하면 `_isKeywordTabBlocked()`로 판정해 `KEYWORD_TAB_SWITCH_FAILED`를 반환하며 `_dumpKeywordCandidates()` 진단을 남긴다. runner는 `depth3ReasonSummary()`로 depth3 0행 사유를 '키워드 탭 전환 실패'와 '실적 없음(빈 표)'로 구분해 로그에 표기한다.
- 버전: content `naverads-content-20260827-keyword-tab-v2`, runner `naverads-runner-20260827-keyword-tab-v2`.
- 검증 결과: 두 JS 파일 `node --check` 통과. 탭 라벨 매칭 14케이스와 `_isKeywordTabBlocked` 4케이스를 `(6).log` 실제 텍스트·상태로 모의 검증해 전건 통과. `등록한 소재가 없습니다`만 보이는 OFF/미등록 광고그룹은 기존 실적 fallback을 유지한다.
- 남은 위험: 실제 탭 DOM을 확인하지 못해 셀렉터 확정은 다음 실행 로그가 필요하다. 전환이 여전히 실패하면 `KEYWORD_TAB_SWITCH_FAILED` 진단의 `tabs[]` 덤프로 셀렉터를 확정하는 2차 수정이 필요하다. 또한 이 errorCode는 fallback이 아닌 재시도 경로로 가므로, 전환 실패가 지속되면 파워링크 광고그룹이 최종 '실패'로 표기된다.

## 2026-08-27 네이버광고 depth 합계 정합성 복구(헤더행·페이지네이션·depth1 합계행)
- 대상: OneDrive 개발용 확장 `content/09_naverads.js`, `runner_naverads.js`.
- 배경: `keyword-tab-v2` 적용 후 `(7).log`에서 키워드 탭 전환은 해결(`표 keyword` 125회, 직전 0회)됐으나 41개 중 15개가 실패했고, depth1/depth2/depth3 합계가 파워링크에서만 불일치했다. 플레이스 3개 캠페인은 d1=d2=d3 일치(18,946/28,157원 등)로 정상.
- 원인1(실패 35건): 키워드 표 헤더 행이 데이터 행으로 파싱. 진단 덤프에서 `rowKey='' keyword='현재 입찰가(VAT미포함)'` 확인. `_keywordTextFromRow`의 제외 목록에 해당 컬럼명이 없어 keyword로 선택되고, 헤더에는 rowKey가 없어 `파워링크 키워드 ID 없음` 검증 실패로 광고그룹 전체가 버려졌다. 키워드 표의 `등록된 키워드가 없습니다` 안내 행도 걸러지지 않았다.
- 원인2: 네이버 키워드 표에는 키워드 ID가 표시되지 않는다(광고그룹 `grp-`, 소재 `nad-` 와 다름). ID 필수 검증이 남아 있는 한 파워링크는 계속 전량 실패한다.
- 원인3: 표가 한 페이지 10행이라 `키워드 29/40/53/54개 결과`인 광고그룹도 전부 11행(헤더1+데이터10)에서 멈췄다.
- 원인4: depth1이 캠페인 목록 페이지 파싱값이라 파워링크에서 0. 반면 캠페인 상세의 광고그룹 표에 정확한 합계행(`광고그룹 5개 결과 1,050 4 0.38 % 306원 1,222원`)이 있는데 `_parseAdgroupRow`가 이를 버리고 있었다.
- 변경: `_isTableHeaderRow()` 추가(th 전용 행 또는 `ON/OFF`+`노출수`+`키워드|소재|광고그룹이름` 조합)로 헤더 행을 키워드/소재 파서와 `_keywordTextFromRow`에서 제외. `_isCreativeEmptyText()`를 키워드 빈 안내까지 인식하도록 확장. `naverAdsRowValidationError`에서 키워드 ID 필수 검증 제거(키워드명은 유지)하고 `ID 미표기 n행` 로그 추가. `_collectPagedRows()`/`_pagedRowSnapshot()`/`_waitForPagedRowsChanged()` 추가로 요약 건수를 목표로 다음 페이지를 순회 누적(최대 20페이지, URL 이탈 시 중단). `_campaignSummaryFromAdgroupTable()`이 합계행을 파싱해 discover 응답 `campaignSummary`로 전달하고 `makeCampaignDepthRow()`가 이를 우선 사용. `depthSumCheck()`로 저장 직후 d1/d2/d3 합계를 비교해 `[일치]`/`[불일치]` 로그를 남긴다.
- 버전: content `naverads-content-20260827-header-paging-v3`, runner `naverads-runner-20260827-header-paging-v3`.
- 검증 결과: 두 JS 파일 `node --check` 통과. 사용자가 제공한 `2607_1km반경` 실제 페이지 텍스트로 18케이스 모의 검증 전건 통과 — 키워드/광고그룹/소재 헤더 3종 제외, 데이터행(`가족`, `삼전동외식`) 및 합계행 보존, 요약행(`전체 결과`/`확장검색 결과`/`키워드 29개 결과`) 제외, 합계행 파싱이 노출 1,050·클릭 4·비용 1,222원을 정확히 산출하며 depth2 합(25+9+400+601+15=1,050)과 일치.
- 남은 위험: 키워드 데이터 행의 `data-row-key` 존재 여부는 미확인이며, ID가 있으면 자동으로 채워지고 없으면 빈 칸으로 저장된다. 키워드 페이지 순회로 광고그룹당 최대 5페이지 이동이 늘어 전체 수집 시간이 증가한다.

## 2026-08-27 네이버광고 수집 실패 3종 제거(렌더 판정·날짜 재시도·안정화 완화)
- 대상: OneDrive 개발용 확장 `content/09_naverads.js`, `runner_naverads.js`.
- 배경: `(8).log` 2회차 기준 41개 중 14개 실패. 로그와 저장 CSV 자체는 정합했다(로그 주장 행수 = 실제 CSV 행수 18/18 일치, 1·2회차 성공 광고그룹 17개로 동일해 재실행 데이터 손실 없음, CSV 구조 이상 0건, `2607_1km반경` depth1 1,050/4/1,222원이 실제 페이지와 일치). 실패 원인만 남아 이를 제거했다.
- 실패 내역과 대응: `정제 행 검증 실패: 파워링크 키워드 ID 없음 / row=1` 13건은 v3(헤더 행 제거 + 키워드 ID 검증 완화)에서 이미 처리됨. 나머지 `지표 텍스트 안정화 미확인` 1건과 1회차의 `날짜 필터 설정 실패` 3건을 이번에 처리했다.
- 변경1: `_waitForAdgroupDetailReady()`가 본문에 '키워드'/'소재' 글자만 있어도 통과해 좌측 메뉴만 렌더된 상태에서 날짜 필터를 설정하다 실패했다. `_adgroupDetailRendered()`를 추가해 `tr[data-row-key]` / 보이는 table tr / 탭 컨트롤 / 빈 표 안내 중 하나가 실제로 렌더된 뒤에만 통과시킨다.
- 변경2: `_ensureTargetDate()` 추가. 날짜 설정 실패 시 1.5초 대기 후 1회 재시도하고, 그래도 실패하면 runner가 붙인 URL `dateRange=YYYY-MM-DD,YYYY-MM-DD`가 목표 기간이면 적용된 것으로 인정한다. 광고그룹/캠페인 양쪽 호출부에 적용.
- 변경3: 지표 텍스트가 끝내 안정화되지 않아도 표가 키워드 표이고 행이 있으면 `ready:true, unstable:true`로 수집을 진행하고 경고 로그를 남긴다(소재도 동일). 광고그룹 전체를 잃는 대신 runner의 정제 행 검증이 잘못된 행을 다시 거른다. `KEYWORD_ROWS_UNSTABLE`/`PLACE_CREATIVE_ROWS_UNSTABLE` errorCode는 사라지며, runner의 UNSTABLE 전용 분기도 제거했다.
- 변경4: `depth3ReasonSummary()`가 분류 실패 시 note를 60자로 잘라 `표 ke 1건` 같은 깨진 조각을 로그에 남기던 문제를 수정. '정제 행 검증 실패(사유)' / '날짜 필터 설정 실패' / '지표 안정화 미확인' / '중단' 분류를 추가하고, fallback도 첫 ` / ` 앞 40자만 사용한다.
- 버전: content `naverads-content-20260827-resilient-v4`, runner `naverads-runner-20260827-resilient-v4`.
- 검증 결과: 두 JS 파일 `node --check` 통과. `(8).log`의 실제 note·URL·DOM 상태로 14케이스 모의 검증 전건 통과 — 사유 분류 5종, URL dateRange 폴백 3종(타 날짜·미포함은 폴백 안 함), 렌더 판정 4종(좌측 메뉴만 렌더 시 false), 안정화 미확인 처리 2종(행 0개면 여전히 실패).
- 남은 위험: v3·v4 모두 아직 실행되지 않았다. `(8).log`의 두 실행(17:46, 18:12)은 파일 수정 이후인데도 `keyword-tab-v2`로 돌았으므로 크롬 확장 리로드가 필요하며, 로드된 확장 폴더가 OneDrive 개발용인지 확인해야 한다. 안정화 완화는 지표가 덜 채워진 행을 저장할 여지가 있으나 정제 행 검증과 depth 합계 자가검증 로그로 감지된다.

## 2026-08-27 네이버광고 빈 광고그룹 즉시종료·순차 재시도·표 페이저 대응
- 대상: OneDrive 개발용 확장 `content/09_naverads.js`, `runner_naverads.js`.
- 배경: v4가 실제 실행되어 `(1).log`(18:37) 기준 성공 40 / 실패 1로 개선(직전 27/14). 파워링크 depth3가 처음으로 저장됐고(46행·50행), depth 합계 자가검증 로그가 16개 캠페인 [일치]를 확인했다. 남은 문제는 실패 1건과 [불일치: d2≠d3] 2건이다.
- 문제1(실패 1건): `⛔ 파워링크#1_메인_광고그룹#1`은 등록된 키워드가 0개인 정상 케이스인데 `click timeout`으로 실패했다. 빈 안내가 table이 아니라 `<p><span>등록된 키워드가 없습니다.<br>새로운 '키워드'를 추가해보세요.</span></p>` 로 렌더되어 행으로 잡히지 않고, `_isCreativeEmptyText`가 이를 인식하면서 tableKind가 'creative'로 판정돼 '소재 테이블 감지 → 키워드 탭 재클릭'을 반복했다. 탭 후보 4개 × 3초 검증을 네 차례 돌아 80초가 걸렸고 `ADGROUP_COLLECT_TIMEOUT`(60초)을 초과했다.
- 문제2(d2≠d3): 키워드가 정확히 10행씩만 수집됐다(10/29, 10/53, 10/54; 5개·3개짜리는 전량). `_nextPageButton()`의 `/다음|next|›|>/i` 매칭이 네이버 표 페이저를 찾지 못해 페이지 순회가 0회였다.
- 변경: 빈 안내를 `_isKeywordEmptyNotice()` / `_isCreativeEmptyNotice()` 로 분리하고 `_isCreativeEmptyText()`는 둘의 OR로 유지. 키워드 빈 안내는 tableKind를 'keyword'로 판정해 재클릭 루프를 없앴다. `_waitTabSwitched()`가 빈 안내를 전환 완료 신호로 인정하고, `_tryOpenTab()`은 이미 목표 탭이면 클릭을 생략하며 후보별 검증을 3초에서 1.8초로 줄였다. `_waitForKeywordDataReady()`/`_waitForCreativeDataReady()`는 빈 안내 확인 즉시 empty로 종료한다(안정화 3회 대기 생략). 표 수집은 `_tryExpandPageSize()`로 '10개씩 보기'를 최대치로 올려 일괄 수집을 먼저 시도하고, 실패 시 `_findPagerNextButton()`(다음 라벨 → next/angle-right 류 클래스 → 화살표 글리프 순, prev/left 제외)으로 순회하며, 목표 미달 시 `_dumpPagerCandidates()` 진단을 남긴다. 재시도 라운드는 `ADGROUP_RETRY_CONCURRENCY=1`로 순차 실행하고 작업 간 간격을 1.5초로 둔다. `ADGROUP_COLLECT_TIMEOUT`을 60초에서 120초로 상향했다.
- 버전: content `naverads-content-20260827-pager-v7`, runner `naverads-runner-20260827-pager-v7`.
- 검증 결과: 두 JS 파일 `node --check` 통과. 사용자 제공 실제 DOM/로그로 16케이스 모의 검증 전건 통과 — 빈 안내 키워드·소재 분리 4종, tableKind 3종, 탭 전환 2종, 페이저 탐색 5종(이전 버튼 배제·미발견 시 null), 페이지 크기 확대 2종.
- 남은 위험: 네이버 표 페이저의 실제 DOM을 확인하지 못해 `_findPagerNextButton()`/`_tryExpandPageSize()`가 여전히 못 찾을 수 있다. 그 경우 `페이저 진단(...)` 로그가 후보 목록을 남기므로 이를 보고 셀렉터를 확정한다. 재시도 순차화로 실패분이 많을 때 전체 수집 시간이 늘어난다.

## 2026-08-27 네이버광고 페이지크기 select 오조작 차단·파워링크 합계 기준 정정
- 대상: OneDrive 개발용 확장 `content/09_naverads.js`, `runner_naverads.js`.
- 위험 제거: v7에서 넣은 `_tryExpandPageSize()`가 페이지의 보이는 `<select>`를 무차별로 골라 값을 바꾸고 change 이벤트까지 발생시켰다. 광고그룹 상세에는 입찰가·노출전략 select가 있을 수 있어 실제 광고 설정이 변경될 위험이 있었다. `_isPageSizeSelect()`를 추가해 옵션이 2~6개이고 모든 옵션이 'N개씩/N개 보기/N rows' 형태이며 값이 알려진 페이지 크기(5~500)일 때만 조작하도록 제한했고, 옵션이나 aria-label에 원·%·입찰·전략·노출·매체·요일·정렬 등이 있으면 즉시 제외한다.
- 기준 정정: 파워링크는 광고그룹 노출 = 키워드 노출 + 확장검색 노출이며 확장검색은 키워드 표에 개별 행으로 나오지 않는다. 사용자 제공 페이지 텍스트에서 `전체 결과 25 = 확장검색 결과 12 + 키워드 29개 결과 13`으로 확인했다. 따라서 파워링크는 d2 = d3가 구조적으로 성립하지 않으며 d3 < d2가 정상이다. `depthSumCheck()`가 파워링크에 한해 d3 ≤ d2를 허용하고 차이를 `일치(확장검색 등 키워드 외 노출 12)`로 표기하도록 수정했다. 플레이스는 기존대로 완전 일치를 요구한다.
- 판단 정정: 앞선 기록에서 '네이버 키워드 표에는 키워드 ID가 없다'고 적었으나, `(1).log` 실행으로 저장된 CSV의 depth3 행에 `nkw-a001-01-...` 키워드 ID가 실제로 채워져 있었다. 키워드 ID 필수 검증을 뺀 조치 자체는 유지하되(빈 ID 때문에 광고그룹 전체를 버리지 않기 위함), 근거는 잘못이었다.
- 버전: content `naverads-content-20260827-powerlink-sum-v9`, runner `naverads-runner-20260827-powerlink-sum-v9`.
- 검증 결과: 두 JS 파일 `node --check` 통과. 페이지 크기 select 판정 13케이스 모의 검증 전건 통과 — 허용 3종(10/30/50/100개씩, N개 보기, N rows), 차단 10종(입찰가 원 단위, 입찰 전략, 노출 매체, aria-label 입찰가, 요일, 정렬, % 값, 연도, 옵션 과다, 단일 옵션).
- 다음 실행에서 확인할 것: depth3 키워드 ID가 행마다 고유한지(직전 확인 시 같은 ID가 여러 키워드에 반복되는 것처럼 보였으나 출력 22자 절단 때문일 수 있어 미확정), `페이저 진단(...)` 로그 유무와 키워드 누적 행수가 요약 건수에 도달하는지.

## 2026-08-27 네이버광고 확장검색 행 추가로 depth 합계 완결
- 대상: OneDrive 개발용 확장 `content/09_naverads.js`, `runner_naverads.js`.
- 배경: `pager-v7` 실행 로그에서 키워드 페이지 순회가 완전히 작동했다. 13개 광고그룹 전부 요약 건수에 도달(23/23, 29/29, 53/53, 54/54 등)했고 미달·페이저 진단 0건. 파워링크 depth3 노출이 93→944, 1,783→7,962로 올랐다.
- 남은 불일치 2건의 정체 확정: 광고그룹별로 분해하니 `2607_1km반경_가족/외식`이 d2 25 / d3 13 / 차이 12였고, 이는 사용자 제공 페이지의 `전체 결과 25 = 확장검색 결과 12 + 키워드 29개 결과 13`과 정확히 일치한다. 즉 수집·저장 결함이 아니라 확장검색 매칭 노출이 키워드 표에 개별 행으로 나오지 않기 때문이다. 캠페인 합계 차이 106은 광고그룹별 확장검색 노출(12/4/65/24/1)의 합이다.
- 변경: `_extractExpandedSearchRow()` 추가. 키워드 표 상단 요약행 3종 중 `확장검색 결과` 행만 `_metricStartIndex()`로 파싱해 `keyword:'확장검색'`, `expandedSearch:true` 인 depth3 행으로 만든다. `전체 결과`와 `키워드 N개 결과`는 계속 버린다(전체 결과를 넣으면 이중 집계). 노출이 0이면 행을 만들지 않는다. `_collectPagedRows('keyword', ...)` 완료 직후 1회만 덧붙여 페이지 순회 중 중복을 막는다. runner `naverAdsRowValidationError()`는 `expandedSearch === true` 행을 키워드 규칙에서 제외하되 키워드명이 '확장검색'이 아니면 오염으로 처리한다.
- 버전: content `naverads-content-20260827-expanded-search-v10`, runner `naverads-runner-20260827-expanded-search-v10`.
- 검증 결과: 두 JS 파일 `node --check` 통과. 사용자 제공 실제 요약행으로 9케이스 모의 검증 전건 통과 — 확장검색 노출 12 파싱, 전체 결과 25를 집지 않음(이중집계 방지), 플레이스/노출0에서 null 반환, runner 검증 4종, 합계 검산(키워드 13 + 확장검색 12 = d2 25).
- 다음 실행에서 확인할 것: 첫 줄이 `expanded-search-v10`인지, `확장검색 행 추가: 노출 N`이 파워링크 광고그룹마다 나오는지, `depth 합계 ... [일치]`로 불일치 2건이 사라지는지, `2607_1km반경_가족/외식` depth3가 30행(키워드 29 + 확장검색 1)이고 노출 합 25인지. 그리고 depth3 키워드 ID가 행마다 고유한지(직전 확인은 출력 22자 절단 탓일 수 있어 미확정).

## 2026-08-27 Marketing Ads Tracking 가변 depth 마트 스키마 추가
- 대상: `modules/transform/pipelines/sales/BSP_MarketingAds_Mart.py`, `tests/test_marketing_ads_mart.py`.
- 변경: `marketing_ads_daily.csv`에 `source_depth_level`, `depth1~3_id/name`, `leaf_depth/id/name` 컬럼을 추가했다. 네이버는 캠페인/광고그룹/키워드·소재 가변 depth를 원천 그대로 보존하고, 당근은 원본 기준 2-depth(광고그룹/광고소재)로 매핑한다.
- 변경: 네이버 원천 기본 탐색에 새 `naver_ads_group_*.csv`와 기존 `naverads_adgroups_*.csv`를 모두 지원하게 했다.
- 검증 결과: `python -X utf8 -m pytest -p no:cacheprovider --basetemp .tmp\pytest-marketing-ads tests/test_marketing_ads_mart.py` 30 passed. 격리 `AIRFLOW_HOME=.tmp\airflow-import`에서 DAG import 성공.
- 남은 위험: 운영 산출물 CSV는 승인 전 미생성. Power BI 모델은 신규 depth 컬럼을 읽도록 별도 반영이 필요하다.

## 2026-08-27 Marketing Ads Tracking 로컬 Windows 원천 경로 수정·파일 생성
- 대상: `modules/transform/utility/paths.py`, `Marketing_AdsTracking_01_Mart_Dags` 산출물.
- 원인: Windows에 `C:\opt\airflow` 빈 폴더가 있어 `resolve_analytics_db()`가 OneDrive analytics 대신 `\opt\airflow\analytics`를 선택했고, 네이버/당근 원천을 못 읽어 파일 생성이 막힐 수 있었다.
- 변경: Windows에서는 `ANALYTICS_DB` 환경변수가 없을 때 OneDrive `data\analytics`를 먼저 사용하도록 수정했다. Linux/컨테이너의 `/opt/airflow/analytics` 동작은 유지한다.
- 검증 결과: 사용자 요청에 따라 `marketing_ads_daily.csv` 1,165행, `marketing_ads_campaign.csv` 0행 헤더 파일 생성. depth 컬럼 존재 및 leaf 분포 확인(네이버 1/2/3 depth, 당근 2 depth).
- 남은 위험: Flow 광고 등록 규칙에 맞는 게시글이 아직 없어 campaign 산출물은 헤더만 생성된다.

## 2026-08-27 Marketing Ads Tracking Flow 미연결 campaign fallback·OneDrive rename fallback
- 대상: `modules/transform/pipelines/sales/BSP_MarketingAds_Mart.py`, `tests/test_marketing_ads_mart.py`.
- 원인: Flow 캠페인 0건이면 `marketing_ads_campaign.csv`가 헤더만 생성됐고, Airflow 컨테이너에서는 OneDrive 마운트에서 `marketing_ads_campaign.csv.tmp`를 실제 CSV로 `os.replace()`하는 단계가 PermissionError로 실패했다.
- 변경: Flow 캠페인이 없을 때도 원천 `channel + link_key` 코드별 1행 campaign 요약을 생성하도록 fallback을 추가했다. CSV atomic replace가 PermissionError이면 임시 파일을 지우고 직접 쓰기로 재시도한다.
- 검증 결과: `marketing_ads_daily.csv` 1,165행, `marketing_ads_campaign.csv` 21행 생성. `.tmp` 잔여 파일 없음. `tests/test_marketing_ads_mart.py` 30 passed, 격리 `AIRFLOW_HOME` DAG import 성공.
- 남은 위험: Airflow UI에서 이미 실패한 run은 재시도/재실행해야 새 코드로 성공한다.

## 2026-08-28 Food Guide 주문내역 0건 조회 실패 처리 수정
- 대상: `Food_Guide_orders_collect_Dags`, `modules/transform/pipelines/db/Food_Guide_orders_collect.py`, `tests/test_food_guide_orders_collect.py`.
- 원인: Food Guide 조회 결과가 `총0건/데이터 없음`이면 사이트가 다운로드 파일을 만들지 않는데, 수집기가 다운로드 파일 미생성을 무조건 실패로 처리했다.
- 변경: 조회 grid 상태에서 0건을 판정하면 `no_data=True`, `rows=0`으로 성공 반환하고, DAG 저장 단계는 parquet 생성 없이 no-op 성공 처리한다. 실제 다운로드 파일이 있는 경로의 기존 변환 동작은 유지했다.
- 검증 결과: `python -X utf8 -m pytest tests/test_food_guide_orders_collect.py -q` 4 passed, `py_compile` 성공, Airflow 컨테이너 DAG import 성공.
- 남은 위험: 컨테이너에는 pytest가 없어 컨테이너 내부 단위 테스트는 실행하지 못했다. 이미 실패한 Airflow run은 재시도/재실행해야 새 코드가 적용된다.

## 2026-08-27 Marketing Ads Tracking campaign depth 추적 키 추가
- 대상: `modules/transform/pipelines/sales/BSP_MarketingAds_Mart.py`, `tests/test_marketing_ads_mart.py`, `marketing_ads_campaign.csv`.
- 변경: campaign 요약 산출물에 `source_depth_levels`, `depth1_id/name`, `depth2_ids/names`, `depth3_ids/names`, `leaf_depths/ids/names`를 추가했다. 코드별 1행에서 depth3까지의 하위 키를 추적할 수 있게 목록형 문자열로 보존한다.
- 검증 결과: `marketing_ads_campaign.csv` 21행 생성, depth 컬럼 존재 확인, depth3 키가 있는 campaign 12건 확인. `tests/test_marketing_ads_mart.py` 32 passed, 격리 `AIRFLOW_HOME` DAG import 성공.
- 남은 위험: 키워드가 많은 네이버 캠페인은 `depth3_ids`/`leaf_ids` 셀이 길어지므로 BI에서는 상세 추적 시 daily 마트와 함께 쓰는 것이 안전하다.

## 2026-08-28 쿠팡 확장 10057/CMG 재시도 보강
- 대상: OneDrive 개발용 크롬확장 `runner.js`.
- 원인: 10057 blocked 매장이 당일 자동 재시도 후보에서 제외됐고, CMG WUJIE shell 정체는 URL 재진입만 반복해 사람이 수동 클릭해야 풀렸다.
- 변경: 10057 매장은 6시간 쿨다운 후 당일 1회 재시도하도록 분리하고, CMG WUJIE 미준비 시 탭 reload 복구와 매장별 실패 payload를 추가했다.
- 검증 결과: 개발용 `runner.js`, `content/03_coupangeats.js`, `content/05_main.js` `node --check` 통과.
- 남은 위험: 실제 쿠팡 UI/Akamai 제한은 브라우저 확장 리로드 후 운영 계정으로 확인해야 한다.

## 2026-08-28 배민 수동수집 즉시할인 시트 재시도 보강
- 대상: OneDrive 개발용 크롬확장 `content/02_baemin.js`.
- 원인: 주문 상세 즉시할인 금액 `span`만 반복 클릭해, 실제 트리거가 주변 조상 요소인 주문에서 시트가 열리지 않고 같은 4건이 `파싱실패 -> 시트없음`으로 반복됐다.
- 변경: 즉시할인 클릭 후보를 TextListItem/atelier/role/button/tabindex/Tooltip/Discount 조상까지 확장하고, 후보별 native/pointer/keyboard 열림 확인과 실패 진단 로그를 추가했다. 시트 텍스트 구조가 달라도 부담 주체 금액을 읽는 compact fallback을 추가했다.
- 검증 결과: 개발용 `content/02_baemin.js`, `baemin_manual.js` `node --check` 통과.
- 남은 위험: 실제 배민 UI에서는 크롬 확장 리로드 후 해운대중동점 20페이지 재수집으로 확인해야 한다.

## 2026-08-28 배민 수동수집 즉시할인 유일값 폴백 추가
- 대상: OneDrive 개발용 크롬확장 `content/02_baemin.js`.
- 원인: 즉시할인 시트 열림은 성공했지만 일부 6,400원 주문 2건이 부담 주체 텍스트 구조 차이로 `파싱실패` 처리되어 159페이지에서 partial 저장됐다.
- 변경: 실행 단위로 `즉시할인 total -> partner/support` 성공 split을 기록하고, 같은 total의 split이 1종류만 관측된 경우에만 파싱실패 주문을 해당 유일값으로 채우는 fallback을 추가했다. 업주/사장님/배달의민족 라벨 파싱과 실패 시트 digest 로그도 보강했다.
- 검증 결과: 개발용 `content/02_baemin.js`, `baemin_manual.js` `node --check` 통과. 최신 partial CSV에서 6,400원 split은 `3,000/3,400` 한 종류였고, 3,000원·4,000원처럼 여러 split이 있는 total은 fallback 금지 대상임을 확인했다.
- 남은 위험: 실제 배민 UI에서는 확장 리로드 후 해운대중동점 재수집으로 159페이지 통과 여부를 확인해야 한다.

## 2026-08-28 Marketing_AdsTracking Flow 링크/원문 컬럼 추가
- 대상: `BSP_MarketingAds_Mart.py`, `tests/test_marketing_ads_mart.py`, Marketing Ads Tracking README.
- 변경: `marketing_ads_daily.csv`와 `marketing_ads_daily_flow_tasks.csv`에 `FLOW_LINK`, `FLOW_TITLE`을 추가하고, 겹치는 업무는 라벨과 같은 순서로 ` | ` 병합되도록 했다.
- 검증 결과: `tests/test_marketing_ads_mart.py` 39개 통과, `py_compile` 통과. 운영 DAG 재실행 후 `dg_1k727bj`의 2026-08-22~2026-08-26 행에 Flow 링크와 원문 제목 반영 확인.
- 남은 위험: Power BI 모델에서 새 컬럼을 보려면 원본 새로고침이 필요하다.

## 2026-08-28 Marketing_AdsTracking Flow 업무 태그 보강
- 대상: `Marketing_AdsTracking_01_Mart_Dags`, `BSP_MarketingAds_Mart.py`, `paths.py`, `tests/test_marketing_ads_mart.py`.
- 변경: Flow 하위업무 제목에서 `std_title`을 산출하고, 날짜별 long mart `marketing_ads_daily_flow_tasks.csv`와 daily 표시용 `flow_task_label`을 추가했다. 성과 row가 없는 Flow 운영일은 `flow_schedule_only` 0원 행으로 보강한다.
- 검증 결과: `py_compile` 통과, `tests/test_marketing_ads_mart.py` 37개 통과. 실제 운영 CSV를 임시 출력에 태워 10개 Flow 날짜 태그와 7개 placeholder 행 생성을 확인했다.
- 남은 위험: OneDrive 운영 CSV 실제 갱신은 다음 DAG 실행 결과로 확인해야 한다.

## 2026-08-28 배민 수동수집 배민 100% 지원 즉시할인 파싱 보강
- 대상: OneDrive 개발용 크롬확장 `content/02_baemin.js`.
- 원인: `총 할인금액4,900원 배민 지원4,900원`처럼 파트너 부담 줄이 없는 배민 100% 지원 시트가 `support` 단독 확정으로 처리되지 않아 1페이지에서 `파싱실패`가 반복됐다.
- 변경: 총 할인금액 요약 줄의 배민/배달의민족 지원·부담 금액이 총액과 같으면 `파트너부담=0`, `배민지원=총액`으로 직접 반환하도록 했다. 개별 지원 항목 합계가 총액과 같은 경우도 동일하게 처리한다.
- 검증 결과: 개발용 `content/02_baemin.js`, `baemin_manual.js` `node --check` 통과. 실패 로그 원문 4,900원·4,000원 및 배민100%지원 샘플 정규식 매칭 확인.
- 남은 위험: 실제 배민 UI에서는 확장 리로드 후 해운대중동점 재수집으로 1페이지 통과 여부를 확인해야 한다.

## 2026-08-28 Flow 수집·방문일지 마트 주요 시간대 최신화
- 대상: `Strategy_FlowStore_01_Collect_Dags`, `Sales_FlowVisit_01_Mart_Dags`, `modules/transform/utility/schedule.py`.
- 변경: 다른 DAG와 정각/30분 충돌을 피하기 위해 Flow 원천 수집은 08:07, 12:37, 14:37, 16:37, 19:37, 21:37 하루 6회만 통과하게 조정했다. 방문일지 마트는 수집 25분 뒤인 08:32, 13:02, 15:02, 17:02, 20:02, 22:02에 실행한다.
- 검증 결과: 변경 전 운영 로그 기준 Flow 원천 예약 실행 24회 모두 성공, 평균 3.66분·중앙값 1.91분·최대 18.81분이었다. `py_compile`, 컨테이너 DAG import, task 목록, import error 없음, 새 6회 슬롯 가드 케이스 검증을 통과했다.
- 남은 위험: Flow API 일시 장애나 댓글 gap은 기존처럼 retry/경고로 처리되며, 변경 후 첫 운영 로그로 API 제한 여부를 확인해야 한다.

## 2026-08-28 Sales_FlowVisit_01_Mart_Dags 센서 타임아웃 복구
- 대상: `dags/sales/Sales_FlowVisit_01_Mart_Dags.py`.
- 원인: 스케줄 변경 직후 기존 마트 run이 고정 25분 차감으로 존재하지 않는 Flow 수집 run(`2026-08-28T00:00:00+00:00`)을 기다려 타임아웃됐다.
- 변경: 센서가 현재 마트 logical date 직전 45분 안의 최신 성공 Flow 수집 run을 우선 사용하고, 없을 때만 기존 25분 차감값으로 대기하게 했다.
- 검증 결과: 컨테이너 DAG import 성공, 실패 run 기준 센서 함수가 `2026-08-27T23:55:00+00:00` 성공 run을 반환했고, 기존 실패 run clear 후 센서와 다운스트림이 재진행됐다.
- 남은 위험: auto-heal 워크스페이스에는 해당 신규 DAG 파일이 아직 없어 런타임 소스 기준으로만 검증했다.

## 2026-08-28 Marketing_DaangnAds_CSV_Dags 신규 CSV 없음 복구
- 대상: `modules/transform/pipelines/sales/BSP_DaangnAds_CSV.py`, `tests/test_daangn_ads_csv.py`.
- 원인: 입력 폴더에 신규 `daangn_ads_*.csv`가 없지만 기존 통합 산출물이 있는 정상 증분 상황까지 실패로 처리했다.
- 변경: 기존 통합 CSV가 있으면 신규 파일 없음 상태를 `NO_NEW_FILES`로 정상 종료하고, 최초 실행처럼 산출물도 없을 때만 파일 없음 오류를 유지했다.
- 검증 결과: 전용 pytest 7개, DAG import, 실제 함수 재현 통과. `manual__2026-08-28T03:07:53.658517+00:00` 재실행 성공.
- 남은 위험: auto-heal 워크스페이스에는 해당 신규 DAG 파일이 아직 없어 런타임 소스 기준으로 검증했다.

## 2026-08-28 Marketing_DaangnAds_CSV_Dags 빈 입력 Skip 처리
- 대상: `Marketing_DaangnAds_CSV_Dags`, `BSP_DaangnAds_CSV.py`, `tests/test_daangn_ads_csv.py`.
- 원인: 입력 CSV와 기존 통합 CSV가 모두 없는 경우도 운영상 오류가 아니라 당근광고 처리 중단인데 `FileNotFoundError`로 DAG 실패가 발생했다.
- 변경: 파이프라인은 `NO_SOURCE_FILES` 결과를 반환하고, DAG 래퍼가 해당 상태를 `AirflowSkipException`으로 매핑하도록 했다.
- 검증 결과: 전용 pytest 8개 통과, 컨테이너 DAG import 성공, 빈 입력 재현에서 `AirflowSkipException` 발생 확인, UTF-8 읽기 확인.
- 남은 위험: 실제 예약 run은 다음 스케줄에서 Airflow UI `skipped` 상태로 확인하면 된다.

## 2026-08-28 쿠팡이츠 자동수집 스케줄러 경로 정리
- 대상: `scripts/coupang_host_chrome.ps1`, `scripts/coupang_runner_autoclick.py`, `scripts/register_coupang_autocollect_task.ps1`, Windows 작업 스케줄러.
- 변경: Chrome 쿠팡 다운로드 기본 경로를 `Collect_Data/영업관리부_수집`로 맞추고, 자동 클릭은 저장소 빌드 확장 ID를 동적으로 찾아 실행하도록 수정했다.
- 검증 결과: PowerShell parse, Python py_compile, UTF-8 읽기 통과. `CoupangAutoCollect`는 6시간 제한으로 재등록했고 오래된 `도리당 크롬 상위50 자동실행`은 비활성화했다.
- 남은 위험: 현재 Chrome 프로세스가 실행 중이라 기존 프로필 다운로드 pref 직접 교정은 보류했다. 실제 쿠팡 UI 수집과 DAG 적재는 다음 로그온 자동 실행 또는 Chrome 종료 후 수동 1회 실행 로그로 확인 필요.

## 2026-08-29 네이버광고 depth 합계 검증 강화
- 대상: OneDrive 개발용 확장 `doridang_collector_개발용`의 `runner_naverads.js`, `content/09_naverads.js`.
- 원인: `e:\down\naverads_batch_20260829.log`와 20260828 CSV에서 `2607_1km반경`의 `depth3 > depth2`, `파워링크#2607_광역`의 확장검색 미수집 미검증이 확인됐다.
- 변경: 광고그룹 표 원본 셀 진단을 보존하고, 상세 수집 직후 `depth2` 대비 `depth3` 합계 역전 및 확장검색 누락을 실패분 재시도 조건으로 올렸다. 저장 후 검증 요약 로그도 추가했다.
- 검증 결과: `node --check` 2개 파일 통과, UTF-8 읽기 및 기존 20260828 CSV 기준 검증 이슈 5건 탐지 확인.
- 남은 위험: 실제 Chrome 확장 재로딩 후 2026-08-28 단일일 재수집으로 확장검색 행 보정 여부를 확인해야 한다.

## 2026-08-29 네이버광고 depth 합계 필수 일치 v34
- 대상: OneDrive 개발용 확장 `doridang_collector_개발용`의 `runner_naverads.js`, `content/09_naverads.js`.
- 원인: v33이 상세 수집 실패 광고그룹의 `depth2`를 저장 그룹에 남겨 `depth3`가 비어도 CSV를 저장했고, `키워드 N개 결과` 요약이 없는 좁은 화면 상태도 성공으로 넘길 수 있었다.
- 변경: 파워링크 `depth2 > depth3`는 확장검색 자동보정 depth3 행을 추가하고, `depth3 > depth2`·요약 미확보·실패 광고그룹·depth1 미확보 캠페인은 CSV 저장 전에 제외하도록 했다.
- 검증 결과: `node --check` 2개 파일 통과, UTF-8 읽기와 핵심 가드 문자열 확인 통과.
- 남은 위험: 실제 Chrome 확장 재로딩 후 `temp`와 같은 세로 모니터 환경에서 2026-08-28 단일일 재수집 로그로 저장 제외/보정 건수를 확인해야 한다.

## 2026-08-29 네이버광고 상세 요약 기준 depth 보정 v35
- 대상: OneDrive 개발용 확장 `doridang_collector_개발용`의 `runner_naverads.js`, `content/09_naverads.js`.
- 원인: `temp` v34 실행에서 저장 CSV 12개는 모두 depth 합계가 맞았지만, `2607_1km반경` 4개와 `파워링크#2607_광역` 3개 광고그룹이 `DEPTH3_GT_DEPTH2`로 최종 실패해 캠페인 2개가 저장 제외됐다.
- 변경: 키워드 상세 페이지의 `전체 결과`·`확장검색 결과`·`키워드 N개 결과` 요약 지표를 runner로 전달하고, 상세 요약이 키워드 행 합계와 일치할 때 광고그룹 depth2와 캠페인 depth1을 상세 기준으로 보정하도록 했다. all-zero 캠페인의 depth1 미확보도 0으로 보정한다.
- 검증 결과: `node --check` 2개 파일 통과, UTF-8 읽기 통과, 기존 `temp` 저장 CSV 12개 depth 합계 재검산 정상.
- 남은 위험: 실제 Chrome 확장 재로딩 후 2026-08-28 단일일 재수집에서 `상세 요약 기준 depth2 보정` 로그와 `검증 통과 파일 18/18` 여부를 확인해야 한다.

## 2026-08-29 네이버광고 기간 수집 날짜별 순차화 v36
- 대상: OneDrive 개발용 확장 `doridang_collector_개발용`의 `runner_naverads.js`, `content/09_naverads.js`.
- 원인: `temp`의 2026-08-26~2026-08-27 실행이 날짜별 저장 없이 36개 캠페인을 한 큐로 처리했고, 과거 날짜 타이핑이 `DATE/TYPED_REVERTED`로 되돌아가 CSV가 0개 저장됐다.
- 변경: 기간 실행을 하루 단위 `runPass`로 순차 처리하고, 한 날짜가 미저장/부분 저장이면 다음 날짜를 시작하지 않도록 했다. 과거 날짜 캠페인 발견 동시성은 1로 낮추고, 타이핑 실패 후 달력 셀 직접 선택 fallback을 연결했다.
- 검증 결과: `node --check` 2개 파일 통과, v36 문자열 확인.
- 남은 위험: 실제 Chrome 확장 재로딩 후 2026-08-26~2026-08-27 재수집에서 날짜별 18/18 저장과 `DATE/TYPED_REVERTED` 최종 실패 0건을 확인해야 한다.

## 2026-08-29 네이버광고 단일/기간 날짜 경로 통일 v37
- 대상: OneDrive 개발용 확장 `doridang_collector_개발용`의 `runner_naverads.js`, `content/09_naverads.js`.
- 원인: 새 `temp` v36 로그에서 단일 어제 수집은 프리셋 경로로 성공했지만, 기간의 2026-08-26은 직접입력 경로에서 `DATE/CALENDAR_DAY_NOT_FOUND` 11건으로 저장 0건 후 중단됐다.
- 변경: 단일/기간 모두 같은 날짜 큐와 하루 실행 경로를 타게 하고, batch에서는 오늘/어제 프리셋을 건너뛰도록 했다. 직접입력·텍스트 셀 선택 실패 후 월/요일 위치 기반 달력 좌표 클릭 fallback을 추가했다.
- 검증 결과: `node --check` 2개 파일 통과, UTF-8 읽기와 v37 문자열 확인 통과.
- 남은 위험: 실제 Chrome 확장 재로딩 후 단일 2026-08-28과 기간 2026-08-26~2026-08-27 로그가 동일한 `날짜 큐 시작 -> 일자 시작 -> 일자 저장 완료` 흐름인지 확인해야 한다.

## 2026-08-29 네이버광고 날짜 좌표 월 라벨 수정 v38
- 대상: OneDrive 개발용 확장 `runner_naverads.js`, `content/09_naverads.js`.
- 원인: temp 최신 v37 로그에서 첫 일자 `2026-08-25`가 CSV 0건으로 100% 실패했고, 달력 DOM은 `2026년 08월`인데 좌표 fallback이 `2026년 8월`만 찾아 `DATE/CALENDAR_GEOMETRY_NOT_FOUND`가 반복됨.
- 변경: 좌표 fallback 월 라벨을 0 포함/미포함 모두 허용하고, 좌표 계산 실패 시 후보 컨테이너와 매칭 여부를 로그로 남기도록 보강함.
- 검증: `node --check` 2개 파일 통과, UTF-8 읽기와 v38 버전 문자열 확인 통과.
- 남은 위험: 실제 Chrome 확장 새로고침 후 v38 로그로 날짜 적용 성공 여부를 확인해야 함.

## 2026-08-29 쿠팡/네이버광고 동시 수집 STOP 격리
- 대상: 저장소 `coupang_extension_build`와 OneDrive 개발용 확장 `doridang_collector_개발용`.
- 원인: 네이버광고 runner가 쿠팡 STOP 래치 `ce_runner_stop_requested`와 쿠팡 `ce_current_*` 재개 키를 공유해 동시 실행 시 쿠팡 수집이 차단/중단될 수 있었다.
- 변경: 쿠팡 STOP 키를 `ce_coupang_runner_stop_requested`, 네이버광고 STOP 키를 `naverads_runner_stop_requested`로 분리하고, 네이버광고 batch가 쿠팡 재개 키를 지우지 않게 했다. 쿠팡 runner는 현재 작업 탭에서 온 완료/진행 신호만 받도록 제한했다.
- 검증: 저장소/OneDrive 핵심 JS `node --check` 통과, OneDrive UTF-8 읽기 확인, `pytest -q tests\test_coupang_extension_stop_contract.py` 2개 통과.
- 남은 위험: Chrome 확장 새로고침 후 `runner.html`과 `runner_naverads.html` 동시 실행으로 실제 탭 유지 여부를 확인해야 한다.

## 2026-08-30 CoupangAutoCollect 다운로드 경로 fallback 복구
- 대상: `scripts/coupang_host_chrome.ps1`, Windows 작업스케줄러 `CoupangAutoCollect`.
- 원인: 로그온 자동 실행에서 OneDrive 후보 경로 계산 중 `Join-Path` 한글 경로 인자 파싱 오류가 발생해 Chrome 디버그 포트 실행 전 종료됐다.
- 변경: 기존 Chrome 확장 runner 방식은 유지하고, 다운로드 경로는 운영 기본값 `E:\down`을 1순위로 사용하게 했다. OneDrive fallback은 명시 인자 `-Path`/`-ChildPath`로 변경했다.
- 검증: PowerShell parse 통과, UTF-8 읽기 통과, 현재 수동 수집 중인 Chrome은 건드리지 않고 `9222` 포트 미개방 상태만 확인했다.
- 남은 위험: 수동 수집 완료 후 Chrome 완전 종료 상태에서 `CoupangAutoCollect` 수동 실행 또는 다음 로그온 자동 실행으로 runner 클릭 단계까지 재확인해야 한다.

## 2026-08-30 배민 업로드 validate_orders None 금액 포맷 복구
- 대상: `modules/transform/pipelines/db/DB_Beamin_Macro_upload.py`.
- 원인: orders 검증 불일치 메시지에서 `expected_amount=None`을 `:,` 포맷에 넣어 `TypeError`가 발생했다.
- 변경: 검증 결과 금액이 `None`이면 보고 문자열에서 `0원`으로 표시하도록 최소 수정했다.
- 검증: 작업공간/운영 소스 `py_compile` 통과, `expected_amount=None` 단위 재현에서 `validate_orders` 요약 반환 확인.
- 남은 위험: 원 run의 upload conf/handoff를 단순 `trigger_dag(dag_id)`로 보존할 수 없어 자동 재실행은 보류했다.

## 2026-08-31 CoupangAutoCollect 확장 ID/일반 Chrome 선점 복구
- 대상: `scripts/coupang_boot_autostart.ps1`, `scripts/coupang_host_chrome.ps1`, `scripts/coupang_runner_autoclick.py`.
- 원인: `Default` 프로필의 고정 확장 ID `ocpdgnoaajajnlehamcalfcpholjhfbe`가 OneDrive 개발용 확장 경로에 등록되어 있는데 자동 클릭은 저장소 확장 경로만 매칭했고, 로그온 때 일반 Chrome이 먼저 뜨면 `9222` 디버그 포트도 적용되지 않았다.
- 변경: host Chrome은 프로필 등록 확장 경로를 우선 로드하고, 자동 클릭은 고정 ID fallback을 사용하며, boot autostart는 host/CDP 실패 시 `runner.html?auto=1&mode=top50&date=yesterday`를 직접 열도록 했다. boot의 OneDrive 후보 경로도 명시 `Join-Path` 인자로 바꿔 한글 경로 파싱 재발을 막았다.
- 검증: `py_compile` 통과, PowerShell parse 통과, 고정 runner URL fallback 확인, 현재 일반 Chrome 실행/DevTools 9222 미개방 상태에서 host 중단 확인.
- 남은 위험: 중복 수집 방지를 위해 현재 세션에서 boot 전체 실행은 보류했고, 다음 로그온 또는 Chrome 완전 종료 후 수동 실행에서 runner 자동 시작 로그를 확인해야 한다.

## 2026-08-31 쿠팡 확장 orders 세션 이탈/CMG 대기 루프 차단
- 대상: OneDrive 개발용 확장 `doridang_collector_개발용/runner.js`.
- 원인: `orders` 수집 중 로그인 페이지로 이탈한 뒤 CMG가 `login?redirectUrl=/merchant/management/cmg/...`에서 WUJIE 준비를 반복 대기했다.
- 변경: Chrome 확장 ID `ocpdgnoaajajnlehamcalfcpholjhfbe`의 실제 로드 경로를 확인하고, 수집 대기 중 로그인 URL 조기 감지와 계정당 1회 재로그인 가드를 CMG/menu 전환부에 추가했다.
- 검증: 운영본 `runner.js` `node --check` 통과, UTF-8 읽기와 replacement char 없음 확인.
- 남은 위험: Chrome 확장 새로고침 후 `mika2969` 단일 계정으로 재로그인 1회 제한과 CMG WUJIE 반복 차단 로그를 실측해야 한다.

## 2026-08-31 당근 광고 통합 CSV url 컬럼 보정
- 대상: `modules/transform/pipelines/sales/BSP_DaangnAds_CSV.py`, `tests/test_daangn_ads_csv.py`, OneDrive `data/analytics/Daangn_ads/daangn_ads.csv`.
- 원인: 신규 당근 확장 CSV에는 `url` 컬럼이 추가됐지만 기존 통합 CSV에는 컬럼이 없어 총합 산출물에서 페이지 주소가 누락됐다.
- 변경: 통합 정규화 단계에서 `url` 컬럼을 보장하고, `광고그룹명+campaign_id`가 일치하는 행의 빈 URL을 신규 수집본 URL로 채우도록 했다.
- 검증: `pytest tests/test_daangn_ads_csv.py -q --basetemp=.tmp\pytest-daangn` 9개 통과, 컨테이너 DAG import 통과, 통합 CSV 875행 중 새 그룹 690행 URL 채움 확인.
- 남은 위험: 근거 URL이 없는 기존 `#3`, `#5` 그룹 185행은 의도적으로 빈칸 유지했다.

## 2026-08-31 Marketing_AdsTracking 일별 마트 url 컬럼 추가
- 대상: `modules/transform/pipelines/sales/BSP_MarketingAds_Mart.py`, `tests/test_marketing_ads_mart.py`.
- 변경: `marketing_ads_daily.csv` 최우측에 원천 광고 `url` 컬럼을 추가하고, 당근 `url`/네이버 `URL` 값을 보존하도록 했다.
- 검증: `python -X utf8 -m pytest tests/test_marketing_ads_mart.py --basetemp=.tmp\pytest-marketing-ads-url -p no:cacheprovider` 39개 통과, 테스트용 `AIRFLOW_HOME` 기반 DAG import 통과.
- 남은 위험: 운영 OneDrive 마트 CSV 재생성은 별도 승인 전까지 수행하지 않았다.

## 2026-08-31 Marketing_AdsTracking 네이버 url 원천 변경
- 대상: `modules/transform/pipelines/sales/BSP_MarketingAds_Mart.py`, `tests/test_marketing_ads_mart.py`.
- 변경: 네이버 광고 행의 `url`은 광고 대상 `URL` 대신 원천 `수집URL`을 우선 사용하고, 과거 파일 호환을 위해 `URL` fallback을 유지했다.
- 검증: `python -X utf8 -m pytest tests/test_marketing_ads_mart.py --basetemp=.tmp\pytest-marketing-ads-url -p no:cacheprovider` 39개 통과. 운영 DAG `manual__naver_collect_url_20260831T1306` 성공, `marketing_ads_daily.csv` 16,663행 최우측 `url` 확인, 네이버 `ads.naver.com` URL 15,617행 및 `map.naver.com` 0행 확인.
- 남은 위험: 없음.

## 2026-08-31 당근 광고 #3/#5 url 누락 원인 확인 및 보정
- 대상: `modules/transform/pipelines/sales/BSP_DaangnAds_CSV.py`, `tests/test_daangn_ads_csv.py`.
- 원인: `#3`, `#5` 원본 CSV는 각각 광고그룹 ID `1781135447941851001`, `1786696279583758001` 파일명으로 수집됐지만 통합 파일 내 URL 값이 전부 비어 있었다.
- 변경: 알려진 3개 당근 광고그룹 URL을 그룹명 기준 fallback으로 등록하고, 원천 URL이 있으면 원천값을 우선 보존하도록 했다.
- 검증: `python -X utf8 -m pytest tests/test_daangn_ads_csv.py --basetemp=.tmp\pytest-daangn-url-fallback -p no:cacheprovider` 10개 통과, `tests/test_marketing_ads_mart.py` 39개 통과, DAG import 통과, 현재 당근 통합 CSV 기준 URL 690건에서 875건으로 보정 가능 확인.
- 남은 위험: OneDrive 운영 CSV 재생성은 승인 후 실행해야 한다.

## 2026-08-31 Marketing_AdsTracking 네이버/당근 url 공란 제거
- 대상: `modules/transform/pipelines/sales/BSP_MarketingAds_Mart.py`, `modules/transform/pipelines/sales/BSP_DaangnAds_CSV.py`, OneDrive `data/analytics/Daangn_ads/daangn_ads.csv`, OneDrive `data/mart/Marketing_Ads_Tracking/marketing_ads_daily.csv`.
- 원인: 당근 `#3/#5`는 기존 수집본의 URL이 비어 있었고, 네이버 4개 중지 캠페인은 원천 `수집URL`/`URL`이 모두 비어 있었다.
- 변경: 당근은 사용자 제공 그룹별 전체 URL로 fallback을 갱신했고, 네이버는 원천 URL이 없을 때 `campaign_id` 기반 캠페인 상세 URL을 생성하도록 했다.
- 검증: 당근 통합 CSV 875행 URL 공란 0건, 최종 `marketing_ads_daily.csv` 16,663행 URL 공란 0건(네이버 15,781/당근 882 모두 채움), `tests/test_marketing_ads_mart.py` 40개 및 `tests/test_daangn_ads_csv.py` 10개 통과.
- 남은 위험: 네이버 중지 캠페인 4개는 원천 URL이 없어 생성 URL을 사용한다.
