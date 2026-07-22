# OKPOS daily/order 오차 근본 해결

## Task
`[OKPOS 부분수집]` 텔레그램이 매 실행마다 같은 매장/날짜로 반복 발송된다. raw CSV `okpos_order.csv`에 **중복 적재**(재수집 시 append 누적, `_pk` 불안정으로 dedup 실패)가 발생해 `order = 정수배 × daily`가 되고, 다운스트림 `DB_UnifiedSales_okpos`(order를 실매출 소스, daily를 게이트로 사용)의 숫자가 어긋난다. **재발 방지(`_pk` 안정화 + upsert-by-date 쓰기)** 와 **기존 오염 데이터 일회성 dedup 정리**를 수행한다.

실데이터 12건 검증 결과 원인이 3가지로 갈리며, 이번 범위는 **A(중복)만 완전 해결**, B/C는 범위 밖:
- **A. order 중복** (06-24 4매장: 동두천/삼송/평택 2×, 송파 4×) → 이번에 해결.
- **B. order 부분수집** (06-27 4매장, order ≈ ½ daily) → 범위 밖.
- **C. daily 롤업 지연** (07-09 등, daily=0/미완성) → 기존 `reconcile_zero_daily_against_sales_detail`이 전담, 범위 밖.

**A의 근본 원인(코드 확인):**
- `_transform_okpos_df`의 `_pk`가 다운로드마다 변함:
  - 선행 0 정규화가 존재하지 않는 컬럼명 `영수증번호`를 대상으로 함 → 실제 컬럼 `영수번호`가 `3` vs `0003`로 매번 달라짐.
  - 숫자 컬럼(`할인내역`)이 `2000.0` vs `2000`으로 렌더링 → 문자열 캐스팅 값 상이.
  - tie-breaker로 **전역 row index** 사용 → 행 하나만 밀려도 전체 `_pk` 변동.
- `save_to_raw` 기본 `if_exists="append"` → 불안정 `_pk`가 dedup 무력화 → 같은 날짜 통째 재적재 → 2×/4×.

**검증 완료(dry-run):** canonical 서명(선행 0 제거 + 숫자 정규화) + collected_at(배치)별 occurrence 카운터로 dedup 시 06-24 4매장 order가 daily와 **정확히 일치**(송파 92행/2,723,200 → 23행/680,800 = daily).

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import RAW_OKPOS_SALES` (하드코딩 금지)
- CSV 저장: `from modules.load.load_onedrive import onedrive_csv_save` (pk_col="_pk", timestamp_col="collected_at")

## Files to Create / Modify
- **수정** `modules/transform/pipelines/db/DB_OKPOS_Sales.py` — `_transform_okpos_df` `_pk` 안정화, `save_to_raw`/`check_and_fill_missing_today` upsert-by-date 전환, `repair_okpos_order_duplicates` 신규.
- **수정(필요 시)** `dags/db/DB_OKPOS_Sales_Dags.py` — 일회성 repair task 배선(선택).

## Implementation Steps

### Part A — 재발 방지 (근본 원인)

1. **`_pk` canonical 정규화 헬퍼 추가** (`_transform_okpos_df` 인근, `DB_OKPOS_Sales.py:2829`)
   - 원본 컬럼 값을 다운로드 불변 문자열로 정규화하는 `_canon_pk_value(v) -> str`:
     - `str(v).strip()`; `nan`/`None`/`""` → `""`.
     - `re.fullmatch(r"-?\d+\.0+", s)` 이면 소수부 제거(`2000.0`→`2000`).
     - 선행 0 제거(`0003`→`3`, 전부 0이면 `"0"`).
   - **주의:** 이 정규화는 **`_pk` 계산용 서명에만** 적용하고, 저장되는 원본 컬럼 값은 그대로 둔다(표시/분석 호환).

2. **선행 0 정규화 대상 컬럼 교정** (`DB_OKPOS_Sales.py:2863~2866`)
   - `("포스번호", "영수증번호")` → `("포스번호", "영수번호", "영수증번호")` (존재하는 것만 처리). 실제 데이터 컬럼은 `영수번호`.

3. **tie-breaker: 전역 row_idx → 배치 내 occurrence** (`DB_OKPOS_Sales.py:2892~2898`)
   - 파생 제외 원본 컬럼(`n_orig` 이전 컬럼)을 `_canon_pk_value`로 매핑한 `sig_df` 생성.
   - occurrence = `sig_df.groupby(list(sig_df.columns)).cumcount()` (같은 서명 내 순번; 같은 영수증 내 동일 상품 중복 정상 보존).
   - `_pk = md5("|".join(canon_signature) + "|" + str(occ))`. 전역 index 제거.
   - 결과: 동일 내용 재다운로드 → 동일 `_pk`.

4. **`save_to_raw` 기본 쓰기 = upsert-by-date** (`DB_OKPOS_Sales.py:3623~3630`)
   - 기본 `if_exists="append"` 경로를 **날짜 단위 replace**로 전환: 대상 CSV에서 `sale_date` 행 제거 후 새 df concat → `onedrive_csv_save(..., if_exists="replace")`.
   - 이미 있는 `replace_by_date` 분기(3604~3622) / `_upsert_by_date`(4063) 패턴 재사용. save_to_raw는 (날짜,매장) 단위 처리이므로 날짜 replace가 안전하고 재수집이 절대 중복되지 않음.

5. **`check_and_fill_missing_today` 동일 전환** (`DB_OKPOS_Sales.py:~3972`)
   - append 저장을 upsert-by-date로 교체.

### Part B — 기존 오염 데이터 일회성 정리 (120일 윈도우)

6. **`repair_okpos_order_duplicates(dry_run: bool = True, **context) -> str` 신규**
   - `RAW_OKPOS_SALES` 하위 모든 `okpos_order.csv` / `okpos_order_item.csv` glob 순회.
   - 각 파일을 `sale_date`별 그룹 후, **canonical 서명 + collected_at(배치)별 occurrence** 기준 중복 제거(검증된 로직, 아래 Reference Code `dedup_order` 참고).
   - `dry_run=True`: 매장/날짜별 `before→after` 행수·금액 리포트 로그 + `RAW_OKPOS_SALES/_reports/okpos_order_dedup_{YYYYMMDD}.csv` 저장. 실제 적용은 `dry_run=False` 또는 conf/env 게이트(예: `purge_okpos_daily` 안전장치 패턴).
   - **daily=0 인 (date,매장)은 판정/보정에서 제외**(최근 롤업 지연 케이스는 기존 zero-daily reconcile 전담).

7. **`okpos_daily` 중복 행 정리**
   - 기존 `dedup_okpos_daily(dry_run)`(`DB_OKPOS_Sales.py:536`) / `_dedup_okpos_daily_df`(118) 재사용 — (date, 매장) 1행 보장, 비영·최신 우선.

### 기준 소스 정책 (사용자 결정)
- **daily 요약을 실매출 진실값으로 신뢰.** 단 **daily=0/미완성(최근·어제자 롤업 지연)** 은 예외 → 기존 zero-daily reconcile 전담, repair는 건드리지 않음.
- order **중복 dedup**은 daily 값과 무관하게 항상 수행(06-24형 결정론적 해결).
- order를 daily로 덮어쓰거나 부분수집을 daily로 보정하는 동작은 **하지 않음**(06-27/07-09 자동보정은 범위 밖).

## Reference Code

### 검증된 dedup 로직 (repair_okpos_order_duplicates 핵심)
```python
import re, pandas as pd

def _canon(v):
    s = str(v).strip()
    if s in ("", "nan", "None"):
        return ""
    if re.fullmatch(r"-?\d+\.0+", s):
        s = s.split(".")[0]
    s2 = s.lstrip("0")
    return s2 if s2 != "" else "0"

def dedup_order(d: pd.DataFrame) -> pd.DataFrame:
    derived = {"매장명", "sale_date", "ym", "_pk"}   # collected_at은 배치 구분용으로 남김
    orig = [c for c in d.columns if c not in derived and c != "collected_at"]
    sig = d[orig].map(_canon)
    occ = sig.assign(_ca=d["collected_at"].values).groupby(orig + ["_ca"]).cumcount()  # 배치 내 순번
    key = sig.assign(_occ=occ.values)
    keep = ~key.duplicated(keep="first")
    return d[keep]
# 검증: 06-24 송파 92행/2,723,200 -> 23행/680,800 (=daily). 동두천 96->48, 삼송 6->3, 평택 28->14 전부 daily 일치.
```

### DB_OKPOS_Sales.py — `_transform_okpos_df` 현재 `_pk` 부분 (line 2861~2898, 교체 대상)
```python
# 선행 0 정규화 — 실제 컬럼은 '영수번호' (현재 '영수증번호'라 미동작)
for _norm_col in ("포스번호", "영수증번호"):
    if _norm_col in df.columns:
        _normed = df[_norm_col].astype(str).str.lstrip("0")
        df[_norm_col] = _normed.where(_normed != "", "0")
_sort_cols = [c for c in ("영수증번호", "상품코드", "총매출액") if c in df.columns]
if _sort_cols:
    df = df.sort_values(_sort_cols, kind="stable").reset_index(drop=True)
else:
    df.reset_index(drop=True, inplace=True)
ym = sale_date[:7]
n_orig = len(df.columns)
df["매장명"] = store_name; df["sale_date"] = sale_date; df["ym"] = ym
df["collected_at"] = _kst_now().strftime("%Y-%m-%d %H:%M:%S")
row_idx = df.index.astype(str)                                   # ← 전역 index tie-breaker (제거 대상)
df["_pk"] = df.iloc[:, :n_orig].apply(
    lambda r: hashlib.md5("|".join(r.astype(str).tolist()).encode()).hexdigest(), axis=1)
df["_pk"] = df["_pk"].astype(str) + "|" + row_idx
df["_pk"] = df["_pk"].map(lambda s: hashlib.md5(s.encode()).hexdigest())
return df, ym
```

### DB_OKPOS_Sales.py — upsert-by-date 재사용 패턴 (`_upsert_by_date`, line 4063)
```python
def _upsert_by_date(dest: Path, df_new: pd.DataFrame, sale_date: str) -> None:
    from modules.load.load_onedrive import onedrive_csv_save
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        try:
            existing = pd.read_csv(dest, dtype=str, encoding="utf-8-sig")
        except Exception:
            existing = pd.read_csv(dest, dtype=str)
        if "sale_date" in existing.columns:
            existing = existing[existing["sale_date"].astype(str).str.strip() != sale_date].copy()
        merged = pd.concat([existing, df_new], ignore_index=True)
    else:
        merged = df_new.copy()
    onedrive_csv_save(df=merged, file_path=str(dest), pk_col="_pk",
                      timestamp_col="collected_at", if_exists="replace")
```

### DB_OKPOS_Sales.py — daily dedup 재사용 (`_dedup_okpos_daily_df`, line 118)
```python
def _dedup_okpos_daily_df(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """daily는 같은 일자/매장에서 비영 실매출·최신 수집분 우선."""
    if df.empty:
        return df.copy(), 0
    out = df.copy()
    if not {"sale_date", "매장명"}.issubset(out.columns):
        return out, 0
    out["_pk"] = out.apply(_okpos_daily_pk, axis=1)
    out["_net"] = _to_int_series(out["실매출액"] if "실매출액" in out.columns else pd.Series(0, index=out.index))
    out["_nz"] = (out["_net"] != 0).astype(int)
    if "collected_at" not in out.columns:
        out["collected_at"] = ""
    before = len(out)
    out = (out.sort_values(["_nz", "collected_at"], kind="mergesort")
             .drop_duplicates(subset=["_pk"], keep="last")
             .drop(columns=["_net", "_nz"], errors="ignore").reset_index(drop=True))
    return out, before - len(out)
```

## Test Cases
1. [import] `python -c "from modules.transform.pipelines.db.DB_OKPOS_Sales import save_to_raw, _transform_okpos_df, repair_okpos_order_duplicates"` → 기대: ImportError 없음
2. [DAG import] `python -c "from dags.db.DB_OKPOS_Sales_Dags import dag"` → 기대: ImportError 없음
3. [`_pk` 안정성] 같은 xlsx를 `_transform_okpos_df`로 2회 변환 → `set(df1['_pk']) == set(df2['_pk'])`; 값이 `2000.0`/`0003`로 달라도 동일해야 함.
4. [dedup 정확성 dry-run] `repair_okpos_order_duplicates(dry_run=True)` 실행 → 06-24 동두천 96→48/1,352,900, 삼송 6→3/196,100, 평택 28→14/439,900, 송파 92→23/680,800 (전부 daily와 diff=0) 리포트 확인.
5. [upsert 멱등성] save_to_raw 경로로 같은 (날짜,매장)을 2회 저장 → 대상 CSV 해당 날짜 행수·`실매출액` 합 불변.
6. [daily=0 스킵] daily=0 인 (date,매장)이 repair 대상에서 제외되는지 로그 확인.
7. [12건 재계산] repair 적용 후 06-24 4매장 diff=0. 06-27/07-09는 변화 없음(범위 밖, daily=0 스킵).

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
- print 금지 → logging. 경로는 `RAW_OKPOS_SALES` 상수(하드코딩 금지).
- `_pk` canonical 정규화는 **서명 계산에만** 적용, 저장되는 원본 컬럼 값은 변형 금지.
- 파괴적 정리(Part B)는 **dry-run 기본** + 명시적 게이트로만 실제 실행(`purge_okpos_daily` 안전장치 패턴).
- 파일 IO/텔레그램 실패는 try/except로 감싸 적재/DAG 흐름 차단 금지.
- 기존 reconcile/게이트 로직(`reconcile_against_daily_summary`, `reconcile_zero_daily_against_sales_detail`, `_okpos_daily_gate`) 동작 변경 금지 — 이번 범위는 `_pk`·쓰기·dedup만.
- 같은 영수증 내 동일 상품 중복(정상)은 보존해야 함 → occurrence 카운터로 구분.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
