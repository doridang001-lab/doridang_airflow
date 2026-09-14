# OKPOS daily/order mismatch 알람 억제 + lookback 탐지·리포트 개선

## Task
`[OKPOS daily 교차검증] daily/order mismatch` 텔레그램이 매장별로 매 실행 반복 발송되는 스팸을 멈춘다. 근본 원인은 `okpos_order.csv`가 daily 대비 부분 수집(예: 삼송점 2026-06-27 daily=458,600/영수10건인데 order=229,300/5건)되는 데이터 갭이며, 게이트가 lookback 14일 × 매장별로 dedup 없이 재발송한다. (1) 게이트 알람에 **지문 dedup + 최근일만** 스로틀을 넣고, (2) 부분 수집 갭을 **탐지·리포트**로 가시화한다. **크롤러 재다운로드 로직은 이번 범위에서 변경 금지(탐지·리포트만).**

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import RAW_OKPOS_SALES` (하드코딩 금지)
- 금액 파싱/합산: `_sum_csv_amount_for_date`(반품 음수 그대로 합산 = 순매출), CSV 읽기: `_read_okpos_csv`

## Files to Create / Modify
- **수정** `modules/transform/pipelines/db/DB_UnifiedSales_okpos.py` — 게이트 알람 스로틀(핵심).
- **수정** `modules/transform/pipelines/db/DB_OKPOS_Sales.py` — `report_missing_daily` 확장 + 부분수집 리포트 파일 + 단일 digest.

## Implementation Steps

### Part A — 게이트 알람 스로틀 (핵심): `DB_UnifiedSales_okpos.py`

대상: `_okpos_daily_gate`(약 852~935행)와 `_send_okpos_daily_gate_alert`(약 843행).
이미 import됨: `hashlib`, `os`, `datetime`, `pandas as pd`. 상수 `OKPOS_BRAND_ROOT = RAW_OKPOS_SALES / "brand=도리당"`(32행) 사용.

1. 모듈 상단(`OKPOS_BRAND_ROOT` 정의 인근)에 상수 추가:
   ```python
   _GATE_ALERT_RECENT_DAYS = int(os.getenv("OKPOS_GATE_ALERT_RECENT_DAYS", "2") or "2")
   ```

2. KST now 헬퍼가 이 모듈에 없으면 로컬 헬퍼 추가(있으면 재사용):
   ```python
   def _kst_today():
       from zoneinfo import ZoneInfo
       return datetime.now(ZoneInfo("Asia/Seoul")).date()
   ```

3. 헬퍼 `_should_send_gate_alert(store_short: str, date_str: str, fingerprint: str) -> bool` 추가:
   - 최근일만: `(_kst_today() - datetime.strptime(date_str, "%Y-%m-%d").date()).days > _GATE_ALERT_RECENT_DAYS` → `return False`.
   - 마커 파일: `OKPOS_BRAND_ROOT / f"store={store_short}" / f"ym={date_str[:7]}" / ".gate_alert__okpos.txt"`.
   - 파일에서 기존 지문 집합을 읽어(`\t` 구분 첫 토큰 아님 — 아래 포맷 참고), `fingerprint`가 있으면 `return False` (dedup).
   - 없으면 `f"{date_str}\t{fingerprint}\t{ts}\n"` append 후 `return True`.
   - **모든 파일 IO는 try/except로 감싸** 실패해도 `False` 반환하지 말고 적재를 막지 않게 처리(예외 시 로그만).
   - 마커 포맷은 `date\tfingerprint\ttimestamp`. 지문 집합 추출은 각 줄 `split("\t")` 후 index 1.

4. `_okpos_daily_gate`의 **텔레그램 3개 분기**(0원 적재제외 약 884행, order_total_zero 약 904행, mismatch 약 921행)를 다음처럼 변경:
   - 각 분기의 `reason` 문자열을 명확히 둔다(`daily_zero_order_zero` / `order_total_zero` / `mismatch`).
   - 지문 계산:
     ```python
     fp = hashlib.md5(f"{date_str}|{store_short}|{daily_net}|{order_total}|{reason}".encode()).hexdigest()
     ```
   - `logger.warning(msg.replace("\n", " | "))`는 **항상 유지**(로그는 남김).
   - `_send_okpos_daily_gate_alert(msg)` 호출을 감싼다:
     ```python
     if _should_send_gate_alert(store_short, date_str, fp):
         _send_okpos_daily_gate_alert(msg)
     else:
         logger.info("gate alert 스로틀 스킵: %s | %s | reason=%s", date_str, store_short, reason)
     ```
   - `strict_gate` drop 동작(약 929~930행)과 `keep.loc[group.index] = False` 적재 게이트 로직은 **그대로 유지**. 알람만 억제.

### Part B — 부분 수집 탐지·리포트 (탐지/리포트만): `DB_OKPOS_Sales.py`

크롤러 재다운로드/`_filter_missing_keys`(약 908행) 판정 로직은 **변경 금지**. 탐지·가시화만 추가.

5. `report_missing_daily`(약 2485행, DAG t1b에서 이미 실행됨)에 **부분 수집 섹션** 추가:
   - lookback `sale_dates` × `STORES` 순회. 각 매장 `store_short = store["name"].replace("도리당 ", "", 1)`.
   - daily/order 경로:
     ```python
     base = RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={sale_date[:7]}"
     daily_net, dreason = _sum_csv_amount_for_date(base / "okpos_daily.csv", sale_date, "실매출액")
     order_sum, oreason = _sum_csv_amount_for_date(base / "okpos_order.csv", sale_date, "실매출액")
     ```
   - `dreason is None and daily_net` (daily 비영) 이고 `order_sum is not None` 이며
     `abs(daily_net - order_sum) > tol` (`tol = int(os.getenv("OKPOS_DAILY_VALIDATE_TOLERANCE", "0"))`) 인 경우만
     partial 목록에 `(sale_date, store["name"], daily_net, order_sum, daily_net - order_sum)` 추가.
   - daily가 0/부재(`dreason is not None or not daily_net`)면 스킵(당일 미완성; 기존 게이트/reconcile 정책과 일치).
   - `context["ti"].xcom_push(key="partial_collection", value=partial_list)` + partial 있으면 `logger.warning`로 총건수/상위예시 요약.

6. 영속 리포트 파일 1개 생성(partial이 있을 때):
   - 경로: `RAW_OKPOS_SALES / "_reports" / f"okpos_partial_{datetime.now():%Y%m%d}.csv"` (부모 mkdir).
   - 컬럼: `date, store, daily_net, order_sum, diff`. `pd.DataFrame(...).to_csv(path, index=False, encoding="utf-8-sig")`.

7. (권장) 단일 digest 텔레그램 **하루 1회만**:
   - `from modules.transform.utility.notifier import send_telegram` (try/except).
   - 일자 마커 `RAW_OKPOS_SALES / "_reports" / f".partial_digest_{YYYYMMDD}.sent"` 존재 시 스킵, 없으면 발송 후 마커 생성.
   - 본문: `f"[OKPOS 부분수집] {len(partial)}건\n" + 상위 N건 요약`. 매장별 개별 발송 금지.
   - 기존 반환 문자열/`missing_daily` XCom 동작은 유지하고 partial 섹션을 덧붙임.

## Reference Code

### DB_OKPOS_Sales.py — `_sum_csv_amount_for_date` (약 800행)
```python
def _sum_csv_amount_for_date(csv_path: Path, sale_date: str, amount_col: str) -> tuple[int | None, str | None]:
    """특정 CSV의 sale_date/amount_col 합계를 반환한다."""
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return None, "missing"
    try:
        df = _read_okpos_csv(csv_path)
    except Exception as e:
        return None, f"read_error({type(e).__name__})"
    if "sale_date" not in df.columns:
        return None, "no_sale_date"
    df = df[df["sale_date"].astype(str).str.strip() == sale_date].copy()
    if df.empty:
        return None, "empty_date"
    if amount_col not in df.columns:
        return None, f"no_{amount_col}"
    return int(_to_int_series(df[amount_col]).sum()), None
```

### DB_OKPOS_Sales.py — `report_missing_daily` (약 2485행, 확장 대상)
```python
def report_missing_daily(**context) -> str:
    """기간 내 okpos_daily.csv 누락 (date, store) 조합을 점검용으로 로그로 출력한다."""
    sale_dates = _load_sale_dates_from_xcom(context)
    missing = _filter_missing_keys(sale_dates, "okpos_daily")
    missing_pairs = [(sd, st["name"]) for sd, st in missing]
    context["ti"].xcom_push(key="missing_daily", value=missing_pairs)
    if not missing_pairs:
        logger.info(f"daily 누락 없음: {sale_dates[0]} ~ {sale_dates[-1]}")
        return f"daily 누락 없음 ({sale_dates[0]} ~ {sale_dates[-1]})"
    preview_n = int(os.getenv("OKPOS_MISSING_REPORT_PREVIEW", "50"))
    preview = ", ".join([f"{sd}:{name}" for sd, name in missing_pairs[:preview_n]])
    logger.warning(f"daily 누락 {len(missing_pairs)}건 ({sale_dates[0]} ~ {sale_dates[-1]}). 예: {preview}")
    return f"daily 누락 {len(missing_pairs)}건 ({sale_dates[0]} ~ {sale_dates[-1]})"
```

### DB_UnifiedSales_okpos.py — `_okpos_daily_gate` 텔레그램 분기 + `_send_okpos_daily_gate_alert` (약 843~935행)
```python
def _send_okpos_daily_gate_alert(message: str) -> None:
    try:
        from modules.transform.utility.notifier import send_telegram
        send_telegram(message)
    except Exception as e:
        logger.warning("OKPOS daily 교차검증 텔레그램 발송 실패(무시): %s", e)

# 게이트 mismatch 분기 (약 920~930행) — 여기의 _send 호출을 _should_send_gate_alert로 감싼다
        else:
            msg = (
                "[OKPOS daily 교차검증] daily/order mismatch\n"
                f"date={date_str}\nstore={store_short}\n"
                f"daily_net={daily_net}\norder_total={order_total}\ndiff={diff}\n"
                f"strict_gate={int(strict_gate)}"
            )
            logger.warning(msg.replace("\n", " | "))
            _send_okpos_daily_gate_alert(msg)   # ← if _should_send_gate_alert(...) 로 감쌈
            if strict_gate:
                keep.loc[group.index] = False
# 모듈 상단: OKPOS_BRAND_ROOT = RAW_OKPOS_SALES / "brand=도리당" (32행)
# import: hashlib, os, datetime 이미 존재
```

### notifier.py — `send_telegram` (약 103행)
```python
def send_telegram(text: str) -> bool:
    token, chat_id = _get_telegram_creds()
    if not token or not chat_id:
        logger.warning("Telegram credentials missing; skip send")
        return False
    ...  # POST sendMessage, 실패 시 warning + False
```

## Test Cases
1. [import] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_okpos import _okpos_daily_gate, _should_send_gate_alert"` → 기대: ImportError 없음
2. [import] `python -c "from modules.transform.pipelines.db.DB_OKPOS_Sales import report_missing_daily, _sum_csv_amount_for_date"` → 기대: ImportError 없음
3. [DAG import] `python -c "from dags.db.DB_UnifiedSales_Dags import dag"` → 기대: ImportError 없음
4. [DAG import] `python -c "from dags.db.DB_OKPOS_Sales_Dags import dag"` → 기대: ImportError 없음
5. [최근일 아님 → 스팸 종료] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_okpos import _should_send_gate_alert; print(_should_send_gate_alert('삼송점','2026-06-27','x'))"` → 기대: `False` (오늘 기준 11일 전 → 텔레그램 미발송)
6. [dedup] 최근 날짜(오늘-1)로 동일 `fp` 2회 호출: 1회차 `True`, 2회차 `False`. 마커 `store=.../ym=.../.gate_alert__okpos.txt`에 지문 1줄만 존재
7. [리포트] OKPOS DAG `report_missing_daily`를 lookback 범위(2026-06-27 포함)로 실행 → XCom `partial_collection`에 06-27 삼송/송파/평택/동두천 partial 포함 + `_reports/okpos_partial_*.csv` 생성
8. [게이트 회귀] `OKPOS_UNIFIED_STRICT_GATE=1`에서 order_total_zero 케이스가 여전히 drop되는지(적재 로직 불변) 로그로 확인

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~8 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- **알람만 억제.** `_okpos_daily_gate`의 `keep`/`strict_gate` drop 등 적재 게이트 로직은 절대 변경 금지.
- **크롤러 재다운로드/`_filter_missing_keys` 판정 로직 변경 금지.** Part B는 탐지·리포트(로그/XCom/CSV/단일 digest)까지만.
- daily가 0/부재인 날짜는 partial 판정에서 제외(당일 미완성).
- 마커/리포트/텔레그램 IO 실패는 try/except로 감싸 적재·DAG 흐름을 막지 말 것.
- 텔레그램은 최근일 dedup(게이트) 또는 하루 1회 digest(리포트)로만. 매장별 개별 반복 발송 금지.
- 경로는 `RAW_OKPOS_SALES`/`OKPOS_BRAND_ROOT` 상수 사용(하드코딩 금지).

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
