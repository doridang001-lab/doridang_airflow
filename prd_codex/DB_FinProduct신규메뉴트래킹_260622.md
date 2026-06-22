# DB_FinProduct 신규 메뉴 트래킹

## Task
`fin_product_mart.csv`에 상품명·출시일·is_main_candidate를 추가하고, 출시 후 30일 매출 성과를 집계하는
`fin_product_launch_tracking.csv`를 신설한다.
unified_sales와 source+상품코드(=item_id) 키로 left join해 신규 메인 메뉴가 잘 나갔는지 추적한다.

---

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- `MART_DB`, `FIN_PRODUCT_MART_CSV_PATH` 등 기존 paths.py 상수 사용

---

## Files to Create / Modify

- **수정**: `modules/transform/pipelines/db/DB_FinProduct.py`
  - `build_fin_product_mart()` — 컬럼 추가 (Phase 1)
  - `build_launch_tracking()` — 신규 함수 추가 (Phase 2)
- **수정**: `dags/db/DB_FinProduct_Dags.py`
  - import에 `build_launch_tracking` 추가
  - `t9` task 추가, `t8 >> t9` 연결

---

## Implementation Steps

### Phase 1 — `build_fin_product_mart()` 컬럼 추가

**파일**: `modules/transform/pipelines/db/DB_FinProduct.py`

**수정 위치**: `build_fin_product_mart()` 내 mart 생성 구간 (line 1038~1047)

**현재 코드**:
```python
# 마트 생성: source+상품코드 기준 중복 제거 (수동분류 충돌 시 첫 번째 값 유지)
df["중복_수동분류"] = df.apply(
    lambda r: "Y" if (str(r["source"]), str(r["상품코드"])) in conflict_keys else "N", axis=1
)
mart = (
    df[["source", "상품코드", "수동분류", "중복_수동분류"]]
    .drop_duplicates(subset=["source", "상품코드"], keep="first")
    .sort_values(["source", "상품코드"])
    .reset_index(drop=True)
)
```

**수정 후** (중복_수동분류 할당 블록 바로 뒤에 교체):
```python
df["중복_수동분류"] = df.apply(
    lambda r: "Y" if (str(r["source"]), str(r["상품코드"])) in conflict_keys else "N", axis=1
)
df["상품명"] = df["상품명"].astype(str).str.strip() if "상품명" in df.columns else ""
df["is_main_candidate"] = (
    df["is_main_candidate"].astype(str).str.strip().str.upper()
    if "is_main_candidate" in df.columns
    else "N"
)

# launch_date: 전체 이력(is_latest 무관)에서 source+상품코드별 min(updated_at)
_master_for_launch = master.copy()
_master_for_launch["source"] = _master_for_launch["source"].astype(str).str.strip()
_master_for_launch["상품코드"] = _master_for_launch["상품코드"].astype(str).str.strip()
_master_for_launch["_ts"] = _master_for_launch["updated_at"].apply(_normalize_updated_at)
launch_dates = (
    _master_for_launch[_master_for_launch["_ts"] != ""]
    .groupby(["source", "상품코드"])["_ts"]
    .min()
    .reset_index(name="launch_date")
)

mart = (
    df[["source", "상품코드", "상품명", "is_main_candidate", "수동분류", "중복_수동분류"]]
    .drop_duplicates(subset=["source", "상품코드"], keep="first")
    .merge(launch_dates, on=["source", "상품코드"], how="left")
    .sort_values(["source", "상품코드"])
    .reset_index(drop=True)
)
```

**결과 컬럼 순서**:
`source | 상품코드 | 상품명 | is_main_candidate | 수동분류 | 중복_수동분류 | launch_date`

> `_normalize_updated_at` 함수는 버그 패치에서 이미 추가됨. 없으면 아래 함수를 helpers 섹션에 추가:
> ```python
> def _normalize_updated_at(ts: str) -> str:
>     import re
>     m = re.match(r'^(\d{4}-\d{2}-\d{2}) (\d):(\d{2})$', ts)
>     if m:
>         return f"{m.group(1)} 0{m.group(2)}:{m.group(3)}:00"
>     return ts
> ```

---

### Phase 2 — `build_launch_tracking()` 신규 함수

**파일**: `modules/transform/pipelines/db/DB_FinProduct.py` (파일 맨 끝에 추가)

```python
def build_launch_tracking(**context) -> str:
    """신규 메인 메뉴의 출시 후 30일 매출 성과 집계.

    fin_product_mart.csv(launch_date 포함) + unified_sales parquet
    → fin_product_launch_tracking.csv
    조인 키: source + 상품코드(fin_product) = source + item_id(unified_sales)
    """
    if not FIN_PRODUCT_MART_CSV_PATH.exists():
        logger.warning("fin_product_mart.csv 없음 - 스킵")
        return "스킵: mart 없음"

    mart = pd.read_csv(FIN_PRODUCT_MART_CSV_PATH, dtype=str).fillna("")
    # 메인 메뉴 + launch_date 있는 행만
    mart = mart[
        (mart.get("is_main_candidate", pd.Series(["N"] * len(mart))).str.upper() == "Y")
        & (mart.get("launch_date", pd.Series([""] * len(mart))) != "")
    ]
    if mart.empty:
        return "추적 대상 없음 (is_main_candidate=Y + launch_date 있는 메뉴 없음)"

    mart["launch_date"] = pd.to_datetime(mart["launch_date"], errors="coerce")
    mart = mart.dropna(subset=["launch_date"])
    mart["source"] = mart["source"].astype(str).str.strip()
    mart["상품코드"] = mart["상품코드"].astype(str).str.strip()

    # unified_sales parquet 로드
    parquet_dir = MART_DB / "unified_sales_grp"
    files = sorted(parquet_dir.glob("*.parquet")) if parquet_dir.exists() else []
    if not files:
        logger.warning("unified_sales parquet 없음: %s", parquet_dir)
        return "스킵: unified_sales 없음"

    sales = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    sales["sale_date"] = pd.to_datetime(sales["sale_date"], errors="coerce")
    sales["source"] = sales["source"].astype(str).str.strip()
    sales["item_id"] = sales["item_id"].astype(str).str.strip()
    sales["qty"] = pd.to_numeric(sales.get("qty", 0), errors="coerce").fillna(0)
    sales["total_price"] = pd.to_numeric(sales.get("total_price", 0), errors="coerce").fillna(0)

    # LEFT JOIN: fin_product_mart → unified_sales
    merged = mart.merge(
        sales[["source", "item_id", "sale_date", "qty", "total_price"]],
        left_on=["source", "상품코드"],
        right_on=["source", "item_id"],
        how="left",
    )

    # 출시 후 30일 이내 매출만 (날짜 없는 행은 0으로 집계에 포함)
    in_window = (merged["sale_date"] >= merged["launch_date"]) & (
        merged["sale_date"] <= merged["launch_date"] + pd.Timedelta(days=30)
    )
    merged = merged[in_window | merged["sale_date"].isna()]

    result = (
        merged.groupby(["source", "상품코드", "상품명", "수동분류", "launch_date"])
        .agg(orders_30d=("qty", "sum"), revenue_30d=("total_price", "sum"))
        .reset_index()
    )
    result["launch_date"] = result["launch_date"].dt.strftime("%Y-%m-%d")
    result["orders_30d"] = result["orders_30d"].astype(int)
    result["revenue_30d"] = result["revenue_30d"].astype(int)

    out_path = FIN_PRODUCT_MART_CSV_PATH.parent / "fin_product_launch_tracking.csv"
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("launch_tracking 저장: %d건", len(result))
    return f"launch_tracking {len(result)}건 저장"
```

**필요 import 확인**: `MART_DB`가 paths.py에서 import되는지 확인. 파일 상단 import 블록:
```python
from modules.transform.utility.paths import (
    ANALYTICS_DB,
    FIN_PRODUCT_CSV_PATH,
    FIN_PRODUCT_REVIEW_CSV_PATH,
    FIN_PRODUCT_ALIAS_CSV_PATH,
    FIN_PRODUCT_MART_CSV_PATH,
    MART_DB,   # ← 없으면 추가
)
```

---

### Phase 2b — DAG 수정

**파일**: `dags/db/DB_FinProduct_Dags.py`

```python
# import 블록에 추가
from modules.transform.pipelines.db.DB_FinProduct import (
    load_okpos_product_xlsx,
    detect_product_changes,
    classify_with_llm,
    update_product_master,
    send_alert_email,
    finalize_unionpos_pending,
    apply_review_approvals,
    build_fin_product_mart,
    build_launch_tracking,   # ← 추가
)

# DAG 내 task 추가 (t8 뒤에)
t9 = PythonOperator(
    task_id="build_launch_tracking",
    python_callable=build_launch_tracking,
    trigger_rule="all_done",
)

# 의존성 체인 수정
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9
```

---

## Reference Code

### `build_fin_product_mart()` 현재 코드 (수정 대상 구간)

```python
def build_fin_product_mart(**context) -> str:
    master = _read_master()
    # ... (중복 감지 로직 생략)

    # 마트 생성: source+상품코드 기준 중복 제거 (수동분류 충돌 시 첫 번째 값 유지)
    df["중복_수동분류"] = df.apply(
        lambda r: "Y" if (str(r["source"]), str(r["상품코드"])) in conflict_keys else "N", axis=1
    )
    mart = (
        df[["source", "상품코드", "수동분류", "중복_수동분류"]]   # ← 여기 교체
        .drop_duplicates(subset=["source", "상품코드"], keep="first")
        .sort_values(["source", "상품코드"])
        .reset_index(drop=True)
    )

    FIN_PRODUCT_MART_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_MART_CSV_PATH.with_suffix(".tmp")
    mart.to_csv(tmp, index=False, encoding="utf-8-sig")
    _safe_replace(tmp, FIN_PRODUCT_MART_CSV_PATH)
```

### `dags/db/DB_FinProduct_Dags.py` 현재 task 체인

```python
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
# → t9 추가 후: t1 >> ... >> t8 >> t9
```

---

## Test Cases

1. **Phase 1: mart 컬럼 확인**
```python
python -c "
import pandas as pd, os
from pathlib import Path
onedrive = Path(os.environ.get('OneDriveCommercial', Path.home() / 'OneDrive - 주식회사 도리당'))
df = pd.read_csv(onedrive / 'data/mart/fin_product/fin_product_mart.csv', dtype=str)
print('컬럼:', df.columns.tolist())
assert '상품명' in df.columns, '상품명 없음'
assert 'launch_date' in df.columns, 'launch_date 없음'
assert 'is_main_candidate' in df.columns, 'is_main_candidate 없음'
print('샘플:')
print(df[['source','상품코드','상품명','launch_date','is_main_candidate']].head(5).to_string(index=False))
"
```
→ 기대: 컬럼 3개 모두 존재, 상품명·launch_date 값 있음

2. **Phase 1: launch_date 정합성**
```python
python -c "
import pandas as pd, os
from pathlib import Path
onedrive = Path(os.environ.get('OneDriveCommercial', Path.home() / 'OneDrive - 주식회사 도리당'))
mart = pd.read_csv(onedrive / 'data/mart/fin_product/fin_product_mart.csv', dtype=str)
grp = pd.read_csv(onedrive / 'data/mart/fin_product/fin_product_grp.csv', dtype=str).fillna('')
# 샘플 1개 검증: launch_date == min(updated_at) for that (source, code)
sample = mart.dropna(subset=['launch_date']).iloc[0]
history = grp[(grp['source']==sample['source']) & (grp['상품코드']==sample['상품코드'])]
expected_min = history['updated_at'].min()
print(f'mart launch_date: {sample[\"launch_date\"]}')
print(f'grp min updated_at: {expected_min}')
"
```
→ 기대: 두 값이 동일하거나 근접

3. **Phase 2: import 검증**
```
python -c "from modules.transform.pipelines.db.DB_FinProduct import build_launch_tracking, build_fin_product_mart; print('OK')"
```
→ 기대: `OK` 출력, ImportError 없음

4. **Phase 2b: DAG import 검증**
```
python -c "from dags.db.DB_FinProduct_Dags import dag; print('tasks:', [t.task_id for t in dag.tasks])"
```
→ 기대: `build_launch_tracking` task 포함

5. **Phase 2: tracking 파일 생성**
```python
python -c "
import os
from pathlib import Path
onedrive = Path(os.environ.get('OneDriveCommercial', Path.home() / 'OneDrive - 주식회사 도리당'))
p = onedrive / 'data/mart/fin_product/fin_product_launch_tracking.csv'
assert p.exists(), f'파일 없음: {p}'
import pandas as pd
df = pd.read_csv(p)
print('컬럼:', df.columns.tolist())
print('행수:', len(df))
print(df.head(3).to_string(index=False))
"
```
→ 기대: `orders_30d`, `revenue_30d`, `launch_date` 컬럼 포함

---

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

---

## Constraints

- `build_fin_product_mart()`의 기존 중복 감지·충돌 알림 로직은 변경 금지
- `launch_date`는 `_normalize_updated_at()` 적용 후 min → 정규화 없이 계산하면 `"0:49"` 포맷이 먼저 정렬될 수 있음
- `build_launch_tracking()`에서 unified_sales가 없으면 에러 내지 말고 warning + 스킵
- DAG `t9`의 `trigger_rule="all_done"` 유지 (t8 실패해도 tracking 시도)
- print 금지, logger 사용
- `MART_DB`가 paths.py import에 없으면 추가 (기존 paths.py에 이미 정의된 상수)

---

## Do Not Ask — Decide Yourself
- `_normalize_updated_at` 이미 있으면 재정의 금지 — 그대로 사용
- `MART_DB` import 누락 시 `from modules.transform.utility.paths import MART_DB` 추가
- `mart.get(col, ...)` 패턴: pandas DataFrame엔 `.get()` 없음 → `mart[col] if col in mart.columns else ...` 사용
- unified_sales 컬럼 `item_id` 없을 경우: `item_id` 대신 `상품코드` 또는 `menu_name` 조인 시도 후 로그 남기기
- 타입 힌트: 기존 함수와 동일 (`**context`) 형식
- 주석: 최소화, WHY만
