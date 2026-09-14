# OKPOS 0원 daily 방지 + UnifiedSales daily 교차검증

## Task
`okpos_daily.csv`는 수집 시각마다 append 되는데, 당일(same-day) 수집은 매출 마감 전이라 `0`으로, 익일(next-day) 수집은 실제값으로 들어와 **같은 sale_date에 0행과 실제값 행이 둘 다 남는 버그**가 있다. 근본 원인은 `save_to_raw`의 `_pk` 재계산 블록이 daily에도 적용되어 `총매출액`을 _pk에 섞어 dedup을 무력화하기 때문이다. (A) 이 버그를 고쳐 daily.csv가 sale_date당 1행(비영·최신)만 남게 하고, (B) UnifiedSales의 okpos 적재 시 order/item 합을 daily 실매출액과 교차검증해 0/부분 데이터가 unified에 적재되지 않게 한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 금액 파싱: `_to_int_series` (콤마 제거 → int), CSV 읽기: `_read_okpos_csv` (utf-8-sig 우선)

## Files to Create / Modify
- **수정** `modules/transform/pipelines/db/DB_OKPOS_Sales.py` — `save_to_raw`의 `_pk` 재계산 블록(약 2887~2906행) daily 분기 + daily dedup 헬퍼 추가 + 마이그레이션 헬퍼 추가
- **수정** `modules/transform/pipelines/db/DB_UnifiedSales_okpos.py` — `run_okpos`/`run_lookback_okpos`/`backfill_okpos`에 daily 교차검증 게이트 추가

## Implementation Steps

### Part A — okpos_daily.csv 0행 dedup 수정 (핵심)

1. **`save_to_raw`의 `_pk` 재계산 블록(약 2887~2906행)을 page_type 분기 처리.**
   - 현재 코드는 `n_orig_ex = len(existing_df.columns) - 5` 로 첫 N개 컬럼을 md5 → order/item 전용 가정. daily(9컬럼)는 `9-5=4` → `sale_date|ym|매장명|총매출액` 이 되어 총매출액이 _pk에 섞임.
   - `page_type == "daily"` 일 때는 daily 전용 공식으로 재계산:
     ```python
     existing_df["_pk"] = existing_df.apply(
         lambda r: hashlib.md5(f"{r['sale_date']}|{r['매장명']}|daily".encode()).hexdigest(),
         axis=1,
     )
     ```
   - 그 외(order/order_item)는 기존 `len(cols)-5` 방식 유지.

2. **daily dedup은 비영(非零) 우선 + 최신 collected_at 우선으로 collapse.**
   - 기존 `drop_duplicates(subset=["_pk"], keep="last")` 는 파일 행 순서 의존. daily는 아래처럼 정렬 후 유지:
     ```python
     # daily: 같은 (sale_date, 매장명)에서 실매출액≠0 우선, 그다음 collected_at 최신 우선
     existing_df["_net"] = _to_int_series(existing_df["실매출액"])
     existing_df["_nz"] = (existing_df["_net"] != 0).astype(int)
     existing_df = (
         existing_df.sort_values(["_nz", "collected_at"])
         .drop_duplicates(subset=["_pk"], keep="last")
         .drop(columns=["_net", "_nz"])
     )
     ```
   - 결과: 익일 실제값 수집 시 새 행의 _pk가 기존 행과 동일 공식으로 맞춰져 0행이 자동 대체됨. 아직 실제값이 없는 날짜(예: 07-05)는 0행 1개만 유지되고 익일 수집 때 대체됨(정상).

3. **마이그레이션 헬퍼 추가** (기존 `purge_okpos_daily` 인근).
   - 함수 `dedup_okpos_daily(dry_run: bool = True) -> str`:
     - `RAW_OKPOS_SALES / "brand=도리당"` 하위 `store=*/ym=*/okpos_daily.csv` 전부 glob.
     - 각 파일: `_read_okpos_csv` → `_pk`를 daily 공식으로 재계산 → step 2 정렬 규칙으로 (sale_date, 매장명)별 1행 유지.
     - dry_run=True: 삭제 예정(중복/0) 행 수와 예시만 로그 출력, 저장 안 함.
     - dry_run=False: `onedrive_csv_save(df=deduped, file_path=str(path), pk_col="_pk", timestamp_col="collected_at", if_exists="replace")` 로 저장.
   - **먼저 dry_run=True로 실행해 대상 확인 후 dry_run=False 실행** ([[partition-migrate]] 원칙).

### Part B — UnifiedSales okpos 적재 시 daily 교차검증

4. **`DB_UnifiedSales_okpos.py`에 게이트 헬퍼 추가.**
   - `_sum_csv_amount_for_date` 를 `DB_OKPOS_Sales` 에서 import (또는 동등 로직 로컬 구현). daily.csv 경로는 `RAW_OKPOS_SALES / "brand=도리당" / f"store={store_short}" / f"ym={ym}" / "okpos_daily.csv"`.
   - 함수 `_okpos_daily_gate(df: pd.DataFrame, date_str: str) -> pd.DataFrame`:
     - `tol = int(os.getenv("OKPOS_DAILY_VALIDATE_TOLERANCE", "0"))`
     - df를 (brand, store) 그룹으로 순회. 각 store의 `store_short` 로 daily.csv 경로 구성.
     - `daily_net, reason = _sum_csv_amount_for_date(daily_csv, date_str, "실매출액")`
     - `order_total = int(그 store/date 그룹의 total_price 합)`
     - 판정:
       - `reason is None and daily_net != 0` (daily 비영):
         - `abs(order_total) <= tol` (order/item 0/near-0) → **해당 store 행 제거(drop) + logger.warning + send_telegram** (0을 unified에 적재하지 않음).
         - `abs(order_total - daily_net) <= tol` → 정상 유지.
         - 그 외 mismatch(둘 다 비영, 오차 초과) → logger.warning + send_telegram, 기본은 유지(하드 스킵은 `OKPOS_UNIFIED_STRICT_GATE=1` env일 때만 drop).
       - daily 0/부재(`reason is not None or daily_net == 0`) → 게이트 미적용(당일 미완성), order/item 값 있으면 그대로 유지 + logger.info.
     - drop 대상 store 행을 제외한 df 반환.
5. **`run_okpos`, `run_lookback_okpos`, `backfill_okpos` 에서 `_transform_okpos_df` 직후 `_save_unified_daily` 전에 게이트 호출:**
   ```python
   df = _transform_okpos_df(order_df, item_df)
   if df.empty: ...
   df = _okpos_daily_gate(df, date_str)
   if df.empty:
       return f"스킵 (daily 교차검증 후 전량 제외 | {date_str})"
   saved = _save_unified_daily(df, date_str, overwrite=overwrite)
   ```

## Reference Code

### DB_OKPOS_Sales.py — 수정 대상 _pk 재계산 블록 (약 2887~2906행)
```python
# 기존 CSV의 _pk가 hash()로 생성된 경우 → MD5로 재계산 후 중복 제거
if dest.exists():
    try:
        existing_df = pd.read_csv(dest)
        if "_pk" in existing_df.columns:
            n_orig_ex = len(existing_df.columns) - 5  # 파생 컬럼 5개 제외
            existing_df["_pk"] = existing_df.iloc[:, :n_orig_ex].apply(
                lambda r: hashlib.md5("|".join(r.astype(str).tolist()).encode()).hexdigest(),
                axis=1,
            )
            before = len(existing_df)
            existing_df.drop_duplicates(subset=["_pk"], keep="last", inplace=True)
            existing_df.to_csv(dest, index=False, encoding="utf-8-sig")
            logger.info(f"기존 CSV _pk 재계산: {dest.name} | 중복제거={before - len(existing_df)}건")
    except Exception:
        logger.warning(f"기존 CSV _pk 재계산 실패 (무시): {dest}")
```
→ 이 블록은 page_map 루프 `for page_type, (files, csv_name) in page_map.items():` 안에 있음. `page_type` 변수 사용 가능. daily 분기 추가.

### DB_OKPOS_Sales.py — daily _pk 공식 (약 2489행) 및 금액/읽기 헬퍼
```python
# daily _pk 공식 (_transform_okpos_daily_df 내부)
out["_pk"] = out.apply(
    lambda r: hashlib.md5(f"{r['sale_date']}|{r['매장명']}|daily".encode()).hexdigest(),
    axis=1,
)

def _read_okpos_csv(csv_path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(csv_path, dtype=str, encoding="utf-8")

def _sum_csv_amount_for_date(csv_path: Path, sale_date: str, amount_col: str) -> tuple[int | None, str | None]:
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

### DB_UnifiedSales_okpos.py — 게이트 삽입 지점 (run_okpos / run_lookback_okpos / backfill_okpos)
```python
def run_okpos(date_str: str, overwrite: bool = False) -> str:
    try:
        order_df, item_df = _load_okpos_by_date(date_str)
    except FileNotFoundError as e:
        logger.warning("스킵: %s", e)
        return f"스킵 (okpos CSV 없음 | {date_str})"
    df = _transform_okpos_df(order_df, item_df)
    if df.empty:
        return f"스킵 (데이터 없음 | {date_str})"
    saved = _save_unified_daily(df, date_str, overwrite=overwrite)   # ← 여기 앞에 게이트 삽입
    _flush_unmatched_alert()
    result = f"unified_sales(okpos) 저장 | {date_str} | {saved}행"
    logger.info(result)
    return result
# OKPOS_BRAND_ROOT = RAW_OKPOS_SALES / "brand=도리당"  (module top)
# store_short = 매장명.replace("도리당 ", "", 1)
# send_telegram: from modules.transform.utility.notifier import send_telegram
```

## Test Cases

1. [_pk 공식 검증] `python -c "import hashlib; s='도리당 송파삼전점'; print(hashlib.md5(f'2026-07-01|{s}|daily'.encode()).hexdigest())"` → 기대: `b38b7c5fd443d5a8f13ff9e7b27160cf` (daily 공식 값, 총매출액 미포함)
2. [Part A 마이그레이션 dry-run] `python -X utf8 -c "from modules.transform.pipelines.db.DB_OKPOS_Sales import dedup_okpos_daily; print(dedup_okpos_daily(dry_run=True))"` → 기대: 삭제 예정 0행 목록 출력(송파삼전점 2026-07: 07-01/07-02 0행 등), 파일 미변경
3. [Part A 실행 후 검증] dedup_okpos_daily(dry_run=False) 실행 후 `store=송파삼전점/ym=2026-07/okpos_daily.csv` 재확인 → 기대: sale_date당 1행, 07-01=966700 / 07-02=1244700, 0행 없음(07-05는 실제값 수집 전이면 0행 1개 유지 허용)
4. [DAG import] `python -c "from dags.db.DB_OKPOS_Sales_Dags import dag"` → 기대: ImportError 없음
5. [DAG import] `python -c "from dags.db.DB_UnifiedSales_Dags import dag"` → 기대: ImportError 없음
6. [Part B 게이트 함수] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_okpos import _okpos_daily_gate"` → 기대: ImportError 없음
7. [Part B 동작] `DB_UnifiedSales`를 `conf={"sale_date":"2026-07-04"}`(daily·order 일치일)로 트리거 → 기대: 정상 적재, 로그에 교차검증 PASS. order 합=0 강제 조작 케이스 → 해당 store/date 미적재 + 경고

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~7 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- **daily에만** 새 _pk 공식/dedup 규칙 적용. order/order_item 의 기존 `len(cols)-5` 재계산·dedup 로직은 절대 변경 금지.
- 마이그레이션은 반드시 dry_run=True 선행 → 대상 확인 후 dry_run=False. 데이터 유실 방지.
- Part B 게이트는 보수적으로: "order/item 0/near-0 while daily 비영" 에만 하드 drop. 일반 mismatch는 경고 위주(하드 스킵은 `OKPOS_UNIFIED_STRICT_GATE=1` env 일 때만).
- daily가 0/부재(당일 미완성)면 게이트를 적용하지 말 것 — order/item 값이 있으면 그대로 적재(daily 보정은 OKPOS DAG `reconcile_zero_daily_against_sales_detail` 담당).
- `send_telegram` import 실패/예외는 try/except로 감싸 적재 흐름을 막지 말 것.
- UnifiedSales는 okpos_daily.csv를 적재 소스로 쓰지 않음. daily는 오직 교차검증 기준값으로만 참조.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
