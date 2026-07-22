# 쿠팡이츠 크롬확장 배치 수집

## 대전제
- Selenium 접근은 **Akamai 봇탐지로 차단** → **크롬확장 배치 방식**으로 수집(60매장).
- 빌드/구조: `C:\airflow\coupang_extension_build\`(background.js, runner.js)
- 관련 MEMORY: `[[coupang-macro-failures]]`, `[[coupang-orders-f5-resume]]`, `[[coupang-extension-batch]]`

## 알려진 함정과 해법
1. **봇탐지(연타 실행)** — 짧은 간격 연속 실행이 탐지 유발 → 실행 간격/속도 조절, 연타 금지.
2. **shell-ready 오판** — 페이지 미준비인데 준비됐다고 판단 → 실제 요소/상태로 준비 확인.
3. **F5 reload-resume** — 막힌 페이지를 새로고침으로 재개하되 **manual 한정·reload 2회 한도·재개 후 날짜필터 재적용 필수**(안 하면 잘못된 범위 수집).
4. **watchdog** — 확장이 멈추면 외부에서 감시·재시도(ChromeExtensionWatchdog). 다른 PC 적용 가이드 존재.

## 참조
- `coupang_extension_build/background.js`, `runner.js`
- `harness/coupang_macro.md`
- `docs/coupang-boot-automation.md`
- prd_codex: `ChromeExtensionWatchdog_*`, `COLLECT_RANGE매장분할_*`
- DAG: `dags/db/DB_CoupangMacro_Load_Dags.py`, `DB_Coupang_Notify_Dags.py`, `dags/sales/Sales_CoupangEats_CMG_Partition_Dags.py`
