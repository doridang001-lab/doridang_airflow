# fin_product_map: PK를 item_key → item_id로 교정 + 검수 큐 축소

## Task

`fin_product_map`의 PK가 `item_key`(정규화된 상품명)라서, 같은 상품명에 서로 다른 상품코드를 부여하는 OKPOS의 코드가 조용히 유실되고 있다. 실측 결과 매출에 존재하는 okpos `item_id` 174개 중 **39개가 map/join에 부재**하여 **1,597개 매출 라인(okpos의 9.4%, 최소 904,293원)이 영구 미분류**다. downstream(`build_join_map`, `DB_FinProduct._load_map_enrichment_df`, `DB_OrderCrossAnalysis._load_map_join_overlay`)은 전부 `item_id`로 join 하므로, map의 PK를 `item_id`로 교정한다. 동시에 사람이 손대야 할 검수 행 수는 늘리지 않는다(pending 38행 유지).

**중요: `item_id` 개념 자체는 정상이다.** okpos는 `base 10_000_000 + store_seq*10_000 + 원본코드하위4자리`로 원본 상품코드를 충실히 보존한다. 정보를 버리는 것은 `item_key`다. `DB_ItemIdAllocator.py`는 **수정하지 않는다.**

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- DAG에는 오케스트레이션만, 비즈니스 로직은 `modules/transform/pipelines/`
- 런타임 스위치는 모듈 상단에 `NAME: bool = True` 형태 상수로 (기존 `DB_FinProduct_Dags.py:43`의 `ENABLE_LLM` 관례)

## Files to Create / Modify

**Modify (신규 파일 없음):**
- `modules/transform/pipelines/db/DB_FinProduct_Map.py` — 주 변경
- `modules/transform/pipelines/db/DB_FinProduct.py` — `대표메뉴` 재소스 (mart 회귀 방지)
- `dags/db/DB_FinProduct_Map_Dags.py` — 텔레그램 알림 조건

**절대 수정 금지:**
- `modules/transform/pipelines/db/DB_ItemIdAllocator.py`
- `modules/transform/pipelines/db/DB_UnifiedSales_*.py`

## Implementation Steps

### 1. `DB_FinProduct_Map.py` — PK 상수 교체

```python
# 기존 (line 104)
KEY_COLUMNS = ["store", "source", "brand", "item_key"]
# 변경
KEY_COLUMNS = ["store", "source", "brand", "item_id"]
```

`KEY_COLUMNS`는 이미 아래 전부에서 공유되므로 상수 한 줄로 전파된다. 각 호출부는 그대로 두고 동작만 확인:
- `build_initial_map` (line 338)
- `migrate_product_map` (lines 1256, 1269, 1279)
- `llm_product_map` (line 1354)
- `build_review_rows` (line 811)
- `apply_review_edits` (line 827)
- `find_llm_targets` (lines 874, 876-878)

### 2. `scan_target_items` 조정 (line 274~322)

groupby 키가 `item_id`가 되므로:

- groupby **전에** 필터 추가: `item_id`가 빈 값인 행, 그리고 `item_id == OKPOS_ADJUSTMENT_ITEM_ID`(`"19999999"`) 행 제외. sentinel은 여러 상품이 공유하므로 PK가 될 수 없다. (`OKPOS_ADJUSTMENT_ITEM_ID`는 이미 이 파일에 import 되어 있음)
- agg에서 `item_id=("item_id", _pick_common_value)` **제거** (이제 키)
- agg에 `item_key=("item_key", _pick_common_value)` **추가** (allocator·recently map이 계속 사용)
- `_fill_item_identity_columns(grouped, persist=persist_identity)` 호출 **직후** `.drop_duplicates(subset=KEY_COLUMNS, keep="last")` 추가 — 재할당이 id를 바꾸는 경우에 대한 보험

`unitprice`는 계속 키가 아니다(표시 전용, join map 미사용). 코드가 분리되면 가격 모호성이 자연 해소된다.

### 3. `_seed_split_rows_from_siblings(map_df)` 신규 함수

분리되어 살아난 39행을 형제 행에서 자동 복사한다.

- `(store, source, brand, item_key)`로 group
- donor = 그룹 내 `review_status_edit == REVIEW_APPROVED` 이고 `표준_메뉴명_edit != ""` 이고 `수동분류_edit in VALID_CATEGORIES` 인 행. `classified_by == "human"` 인 행을 우선 선택
- 그룹 내에서 `표준_메뉴명_edit`이 비었거나 `수동분류_edit`이 `VALID_CATEGORIES`에 없는 행에 donor 값 복사:
  - `표준_메뉴명_edit`, `수동분류_edit` ← donor
  - `review_status_edit = REVIEW_APPROVED`
  - `classified_by = "human_sibling"`
  - `updated_at = TODAY`
- donor가 없으면(형제도 pending) **건드리지 않고 pending 유지**
- 몇 행을 seed 했는지 `logger.info`로 기록

**호출 위치:** `migrate_product_map` 안, concat/dedup 직후(line 1272 뒤), `apply_review_edits`(line 1273) **앞**.

### 4. `_review_reason` 분기 추가 (line 754)

함수 **맨 앞**에 `classified_by` 기반 분기를 둔다 (승인 상태여도 사유가 남아야 나중에 눈으로 훑을 수 있다):

```python
classified_by = _strip_text(row.get("classified_by"))
if classified_by == "human_sibling":
    return "자동복사(동일상품명 코드분리)"
if classified_by == "auto_zero_price":
    return "자동승인(0원 라인)"
# 이하 기존 로직 (검수완료 분기부터)
```

### 5. `apply_review_edits` — `classified_by` 덮어쓰기 조건부화 (line 855)

사람이 실제로 고치지 않았는데 `human_sibling` / `auto_zero_price` 마커가 지워지는 것을 막는다.

```python
# 기존
result.loc[idx, "classified_by"] = "human"
# 변경
if std_changed or label_changed:
    result.loc[idx, "classified_by"] = "human"
elif is_approved and _strip_text(result.loc[idx, "classified_by"].iloc[-1]) == "":
    result.loc[idx, "classified_by"] = "human"
```

`result.loc[idx, REVIEW_STATUS_COLUMN]`과 `updated_at` 갱신은 기존대로 유지.

### 6. 0원 라인 자동 분류·검수 스킵

모듈 상단(로거 위, 상수 블록)에 스위치 추가:

```python
AUTO_APPROVE_ZERO_PRICE: bool = True
```

`_auto_approve_zero_price(map_df)` 신규 함수:
- `AUTO_APPROVE_ZERO_PRICE`가 False면 그대로 반환
- 대상 mask: `pd.to_numeric(unitprice, errors="coerce").fillna(0) == 0` **AND** `수동분류_edit in VALID_CATEGORIES` (LLM/규칙이 이미 분류함) **AND** `review_status_edit != REVIEW_APPROVED`
- `표준_메뉴명_edit`이 비면 `item_name`으로 채움
- `review_status_edit = REVIEW_APPROVED`, `classified_by = "auto_zero_price"`, `updated_at = TODAY`
- 승인 행 수를 `logger.info`로 기록

**호출 위치 2곳** (분류가 채워진 뒤여야 함):
- `migrate_product_map` — `_apply_classification_overrides(result)` 직후 (line 1275 뒤)
- `llm_product_map` — `_apply_classification_overrides(map_df)` 직후 (line 1357 뒤). 새로 LLM 분류된 0원 행이 같은 run에서 승인되어 알림을 트리거하지 않게 한다.

### 7. `llm_product_map` summary에 `new_pending` 추가

`new_rows`(classify_unmapped 결과)를 map에 merge하고 `_auto_approve_zero_price`를 통과시킨 **뒤**, 그 신규 행 중 여전히 `review_status_edit == REVIEW_PENDING`인 개수를 세어 `summary["new_pending"]`에 담는다.

신규 행 식별은 `new_rows`의 `KEY_COLUMNS` 튜플 집합으로 최종 `map_df`를 필터링해 계산한다.

### 8. `dags/db/DB_FinProduct_Map_Dags.py` — 알림 조건 교체 (line 86~99)

```python
# 기존
new_count = int(result.get("new_classified") or 0)
if not dry_run and new_count > 0:
# 변경
new_pending = int(result.get("new_pending") or 0)
if not dry_run and new_pending > 0:
```

메시지 본문의 `신규 추가: {new_count}건`도 `신규 검수 필요: {new_pending}건`으로 교체. `pending_count`, `samples` 로직은 유지하되 `new_item_samples`는 pending 행 위주로 나오도록 `llm_product_map`에서 채운다. 0원 자동승인 행이 매번 "검수 필요" 텔레그램을 쏘는 것을 막는 것이 목적.

### 9. `대표메뉴`를 검수 CSV에서 제거

`대표메뉴`는 `_representative_menu_name`(line 266) = 주문 라인 `menu_name`의 **최빈값**이다. `menu_name`은 "이 주문에서 이 라인이 어느 메인에 붙었나"이므로 공유 옵션은 부모가 여럿이다. 실측 311개 group 중 **243개(78%)가 부모 2개 이상**. 검수자가 잘못된 부모 메뉴를 보고 오분류하는 것을 막는다.

**`DB_FinProduct_Map.py`:**
- `REVIEW_COLUMNS`(line 81)에서 `"대표메뉴"` 제거
- `MAP_COLUMNS`(line 65)에는 **유지** (기계용 파생값, `fin_product_map.csv`에 계속 씀)

**`DB_FinProduct.py` (mart 회귀 방지 — 필수):**

`_load_map_enrichment_df`(line 66)가 review CSV에서 읽던 `대표메뉴`는 `fin_product_mart.csv`로 흘러간다(line 1443). 그대로 두면 mart 컬럼이 전부 NA가 된다.

→ `fin_product_map.csv`(`FIN_PRODUCT_MAP_CSV_PATH`)에서 `(source, item_id) → 대표메뉴` lookup을 읽는 헬퍼를 추가하고, `_load_map_enrichment_df`가 `df["대표메뉴"]`를 그 lookup으로 채우게 한다. 승인/`표준상품명`/`수동분류_edit`은 계속 review CSV에서 읽는다. 파일이 없거나 읽기 실패하면 빈 문자열로 degrade (`logger.warning`).

`_review_few_shot_rows`(line 639)와 `_load_map_enrichment_df`(line 84)는 이미 `if col not in df.columns: df[col] = ""` 가드가 있어 크래시하지 않는다. few-shot 프롬프트(line 713)도 `if main:` 조건부라 안전하게 생략된다. → **`_review_few_shot_rows`는 수정 불필요.**

## Reference Code

### modules/transform/pipelines/db/DB_FinProduct_Map.py (현재 상태, 핵심부)

```python
REVIEW_STATUS_COLUMN = "review_status_edit"      # line 48
REVIEW_STATUS_DISPLAY_COLUMN = "검수유무"
REVIEW_APPROVED = "1"
REVIEW_PENDING = "0"
DUP_LABEL_COLUMN = "중복_수동분류"
MAP_COLUMNS = [
    "item_id", "item_key", "store_seq", "item_seq", "store", "source", "brand",
    "item_name", "unitprice", "표준_메뉴명_edit", "수동분류_edit", "대표메뉴",
    REVIEW_STATUS_COLUMN, "classified_by", "updated_at",
]
REVIEW_COLUMNS = [
    "item_id", "item_key", "store", "source", "brand", "item_name", "unitprice",
    "표준_메뉴명_edit", "수동분류_edit", DUP_LABEL_COLUMN, "대표메뉴",
    REVIEW_STATUS_DISPLAY_COLUMN, "검수사유",
]
REVIEW_INTERNAL_COLUMNS = [
    REVIEW_STATUS_COLUMN if col == REVIEW_STATUS_DISPLAY_COLUMN else col
    for col in REVIEW_COLUMNS
]
JOIN_COLUMNS = ["item_id", "store", "source", "brand", "standard_menu_name", "category"]
KEY_COLUMNS = ["store", "source", "brand", "item_key"]        # ← line 104, 교체 대상
VALID_CATEGORIES = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
TODAY = str(date.today())


def _pick_common_value(series: pd.Series) -> str:              # line 159
    values = series.fillna("").astype(str).str.strip()
    values = values[values != ""]
    if values.empty:
        return ""
    return values.value_counts(sort=True).index[0]


def scan_target_items(*, persist_identity: bool = True) -> pd.DataFrame:   # line 274
    frames = []
    for file_path in iter_unified_sales_files():
        df = pd.read_parquet(file_path, columns=["item_id","store","source","brand","item_name","unit_price","menu_name"])
        for col in (...):
            df[col] = df[col].fillna("").astype(str).str.strip()
        df["source"] = df["source"].map(canonical_source)
        df["item_key"] = df["item_name"].map(normalize_item_key)
        scoped = df[df["store"].isin(TARGET_STORE_SET) & (df["item_name"] != "")]
        frames.append(scoped[[...]])
    grouped = (
        pd.concat(frames, ignore_index=True)
        .groupby(KEY_COLUMNS)
        .agg(
            item_id=("item_id", _pick_common_value),        # ← 제거 대상
            item_name=("item_name", _pick_common_value),
            unitprice=("unit_price", _pick_common_value),
            대표메뉴=("menu_name", _representative_menu_name),
        )
        .reset_index()
    )
    grouped = _fill_item_identity_columns(grouped, persist=persist_identity)
    return grouped.sort_values(["store","source","item_name"]).reset_index(drop=True)


def _review_reason(row: pd.Series) -> str:                     # line 754
    if _normalize_review_status(row.get(REVIEW_STATUS_COLUMN)) == REVIEW_APPROVED:
        return "검수완료"
    reasons = []
    std_name = _strip_text(row.get("표준_메뉴명_edit"))
    label = _strip_text(row.get("수동분류_edit"))
    classified_by = _strip_text(row.get("classified_by"))
    if not std_name:
        reasons.append("표준명 미입력")
    if label not in VALID_CATEGORIES:
        reasons.append("수동분류 미입력")
    elif label == "기타":
        reasons.append("기타 분류 확인")
    if classified_by.startswith("rule/llm_conflict"):
        reasons.append(classified_by.replace("rule/llm_conflict", "규칙/LLM 불일치", 1))
    if not reasons and classified_by == "llm":
        reasons.append("LLM 분류 확인")
    return ", ".join(dict.fromkeys(reasons))


def apply_review_edits(map_df, review_df) -> pd.DataFrame:     # line 818
    ...
    reviews = reviews.drop_duplicates(subset=KEY_COLUMNS, keep="last")
    for _, review_row in reviews.iterrows():
        key_mask = pd.Series(True, index=result.index)
        for col in KEY_COLUMNS:
            key_mask = key_mask & (result[col].fillna("").astype(str).str.strip() == _strip_text(review_row.get(col)))
        if not bool(key_mask.any()):
            continue
        idx = result.index[key_mask]
        rv_std = _strip_text(review_row.get("표준_메뉴명_edit"))
        rv_label = _strip_text(review_row.get("수동분류_edit"))
        cur_std = _strip_text(result.loc[idx, "표준_메뉴명_edit"].iloc[-1])
        cur_label = _strip_text(result.loc[idx, "수동분류_edit"].iloc[-1])
        is_approved = _normalize_review_status(review_row.get(REVIEW_STATUS_COLUMN)) == REVIEW_APPROVED
        std_changed = rv_std != "" and rv_std != cur_std
        label_changed = rv_label in VALID_CATEGORIES and rv_label != cur_label
        if not (is_approved or std_changed or label_changed):
            continue
        std_name = rv_std or (_strip_text(review_row.get("item_name")) if is_approved else "")
        label = _strip_text(review_row.get("수동분류_edit"))
        if std_name != "":
            result.loc[idx, "표준_메뉴명_edit"] = std_name
        if label in VALID_CATEGORIES:
            result.loc[idx, "수동분류_edit"] = label
        result.loc[idx, REVIEW_STATUS_COLUMN] = REVIEW_APPROVED if is_approved else REVIEW_PENDING
        result.loc[idx, "classified_by"] = "human"             # ← 조건부화 대상 (line 855)
        result.loc[idx, "updated_at"] = TODAY
    return result


def migrate_product_map(dry_run: bool = False, **context) -> dict:   # line 1248
    result = build_initial_map(persist_identity=not dry_run)
    existing = load_map()
    review_edits = load_review_map()
    if not result.empty and not existing.empty:
        current_value_cols = ["item_id","store_seq","item_seq","item_name","unitprice","대표메뉴"]
        current_values = result[KEY_COLUMNS + current_value_cols].drop_duplicates(subset=KEY_COLUMNS, keep="last")
        existing = existing.merge(current_values, on=KEY_COLUMNS, how="inner", suffixes=("", "_current"))
        ...
        result = (
            pd.concat([result, existing], ignore_index=True)
            .reindex(columns=MAP_COLUMNS, fill_value="")
            .drop_duplicates(subset=KEY_COLUMNS, keep="last")
            .sort_values(["store","source","item_name"])
            .reset_index(drop=True)
        )
    # ← 여기에 _seed_split_rows_from_siblings(result) 삽입
    result = apply_review_edits(result, review_edits)
    result = apply_recently_edits(result, load_recently_map())
    result = _apply_classification_overrides(result)
    # ← 여기에 result = _auto_approve_zero_price(result) 삽입
    result[REVIEW_STATUS_COLUMN] = result[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
    review_rows = build_review_rows(result)
    join_rows, join_conflicts = build_join_map(result)
    ...
```

### modules/transform/pipelines/db/DB_FinProduct.py (대표메뉴 소비부)

```python
_MAP_APPROVED = {"1", "1.0", "y", "yes", "true", "approved", "승인", "완료", "검수완료"}
_MAP_DUP_LABEL_COL = "중복_수동분류"
_MAP_ENRICHMENT_COLS = ["source", "item_id", _MAP_DUP_LABEL_COL, "표준상품명", "수동분류_edit", "대표메뉴", "검수유무"]


def _load_map_enrichment_df() -> pd.DataFrame:        # line 66
    """fin_product_map_review_input.csv에서 검수 완료된 분석용 표준명/분류만 로드."""
    empty = pd.DataFrame(columns=_MAP_ENRICHMENT_COLS)
    if not FIN_PRODUCT_MAP_REVIEW_CSV_PATH.exists():
        return empty
    df = pd.read_csv(FIN_PRODUCT_MAP_REVIEW_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    required_cols = ("source","item_id","표준_메뉴명_edit","수동분류_edit",_MAP_DUP_LABEL_COL,"대표메뉴","검수유무")
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""                              # ← 가드 있음, 크래시 안 함
    df["source"] = df["source"].astype(str).str.strip()
    df["item_id"] = df["item_id"].astype(str).str.strip()
    df["표준상품명"] = df["표준_메뉴명_edit"].astype(str).str.strip()
    ...
    approved = df["검수유무"].str.lower().isin(_MAP_APPROVED)
    df = df[approved & (df["source"] != "") & (df["item_id"] != "") & (df["표준상품명"] != "")].copy()
    if df.empty:
        return empty
    df["대표메뉴"] = df["대표메뉴"].astype(str).str.strip()    # ← fin_product_map.csv 에서 채워야 함
    for col in ["표준상품명","수동분류_edit","대표메뉴","검수유무"]:
        df[col] = df[col].replace("", pd.NA)
    return df.drop_duplicates(subset=["source","item_id"], keep="last")[_MAP_ENRICHMENT_COLS]


# line 1428 — enrichment 가 mart 로 흘러감
enrichment = _load_map_enrichment_df()
mart = mart.merge(enrichment.rename(columns={"item_id": "상품코드"}), on=["source","상품코드"], how="left")
mart = mart[key_cols + [_MAP_DUP_LABEL_COL, "상품명", "표준상품명", "수동분류_edit", "대표메뉴", "검수유무", "exclude_check"]]
```

### dags/db/DB_FinProduct_Map_Dags.py (알림부, line 79~100)

```python
def run_llm(**context) -> dict:
    conf = _dag_conf(context)
    dry_run = _conf_bool(conf, "dry_run", DRY_RUN_BY_DEFAULT)
    default_limit = DRY_RUN_LLM_LIMIT if dry_run else None
    limit = _conf_int(conf, "limit", default_limit)
    logger.info("fin_product_map llm 시작: dry_run=%s limit=%s", dry_run, limit)
    result = llm_product_map(dry_run=dry_run, limit=limit)
    new_count = int(result.get("new_classified") or 0)          # ← new_pending 으로 교체
    if not dry_run and new_count > 0:
        pending_count = int(result.get("pending") or 0)
        samples = result.get("new_item_samples") or []
        sample_text = "\n".join(f"- {item}" for item in samples[:5])
        if sample_text:
            sample_text = f"\n신규 샘플:\n{sample_text}\n"
        send_telegram(
            "[상품 매핑] 신규 상품 검수 필요\n"
            f"신규 추가: {new_count}건\n"
            f"미검수(검수유무=0): {pending_count}건\n"
            f"{sample_text}"
            "fin_product_map_review_input.csv에서 표준명과 분류를 확인해주세요."
        )
    return result
```

## Test Cases

실행 전 백업:
```powershell
$d = "C:\Users\민준\OneDrive - 주식회사 도리당\data\mart\fin_product"
Copy-Item "$d\fin_product_map.csv" "$d\_backup\fin_product_map.pre_pk_fix.csv"
Copy-Item "$d\fin_product_map_review_input.csv" "$d\_backup\fin_product_map_review_input.pre_pk_fix.csv"
```

1. **모듈 import** — `python -c "from modules.transform.pipelines.db.DB_FinProduct_Map import migrate_product_map, llm_product_map, _seed_split_rows_from_siblings, _auto_approve_zero_price"` → 기대: ImportError 없음

2. **DAG import** — `python -c "from dags.db.DB_FinProduct_Map_Dags import dag; from dags.db.DB_FinProduct_Dags import dag as d2"` → 기대: ImportError 없음

3. **dry-run 수치** — `python -c "from modules.transform.pipelines.db.DB_FinProduct_Map import migrate_product_map; import json; print(json.dumps(migrate_product_map(dry_run=True), ensure_ascii=False, indent=2))"`
   → 기대:
   - `duplicate_keys == 0`
   - `join_conflict_keys == 0`
   - `review_rows` ≈ 386 (현재 347, +39)
   - `join_rows` ≈ 385 (현재 346)
   - `pending` ≈ 38 (현재 38에서 **크게 늘지 않아야 함**). 크게 늘었다면 donor 없는 형제 그룹이 많은 것이므로 로그로 목록 출력 후 원인 분석

4. **39개 코드 복구 확인** — 매출 parquet의 okpos `item_id` 집합과 dry-run map의 `item_id` 집합 차집합이 비어야 한다:
```python
import pandas as pd, sys
sys.path.insert(0, ".")
from modules.transform.pipelines.db.DB_UnifiedSales_common import iter_unified_sales_files
from modules.transform.pipelines.db.DB_ItemIdAllocator import canonical_source
from modules.transform.pipelines.db.DB_FinProduct_Map import build_initial_map, _seed_split_rows_from_siblings

frames = []
for f in iter_unified_sales_files():
    d = pd.read_parquet(f, columns=["item_id","store","source","item_name"])
    frames.append(d[d["store"] == "송파삼전점"])
s = pd.concat(frames, ignore_index=True)
s["source"] = s["source"].map(canonical_source)
s = s[(s["source"] == "okpos") & (s["item_name"].fillna("").str.strip() != "")]
sales_ids = set(s["item_id"].astype(str)) - {"19999999", ""}

m = build_initial_map(persist_identity=False)
map_ids = set(m[m["source"] == "okpos"]["item_id"].astype(str))
print("missing:", sorted(sales_ids - map_ids))   # 기대: []
print("map okpos rows:", len(map_ids))           # 기대: 174
```

5. **`10030976` 자동복사 확인** — dry-run map에서 해당 행이
   `표준_메뉴명_edit != ""`, `수동분류_edit in VALID_CATEGORIES`, `review_status_edit == "1"`, `classified_by == "human_sibling"` 인지 확인.
   `build_review_rows` 결과에서 `검수사유 == "자동복사(동일상품명 코드분리)"` 인지 확인.

6. **0원 자동승인 확인** — dry-run map에서 `unitprice == "0"` 이고 `수동분류_edit in VALID_CATEGORIES` 인 행 중 `review_status_edit == "0"` 인 행이 0개인지 확인.
   `AUTO_APPROVE_ZERO_PRICE = False`로 두고 재실행하면 승인되지 않는지도 확인(스위치 동작).

7. **실제 실행 후 CSV 확인** — `migrate_product_map(dry_run=False)` 실행 후 `fin_product_map_review_input.csv` 헤더에 `대표메뉴`가 없고, `fin_product_map.csv` 헤더에는 `대표메뉴`가 있는지. `검수사유` 컬럼에 자동복사/자동승인 행이 보이는지.

8. **mart 회귀 확인** — `python -c "from modules.transform.pipelines.db.DB_FinProduct import build_fin_product_mart; build_fin_product_mart()"` 실행 후 `fin_product_mart.csv`의 `대표메뉴` 컬럼이 **여전히 채워져 있는지**(전부 NA가 아니어야 함), 그리고 `"mart review 표준명/분류 합침: N/M행 매칭"` 로그의 매칭률이 변경 전보다 **상승**했는지.

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~8 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

특히 Test Case 3의 `pending`이 38에서 크게 늘면 **중단하고 원인을 보고**한다. 목표는 "매출 유실 복구 + 사람 작업량 불변"이므로 pending 증가는 설계 실패다.

## Constraints

- `DB_ItemIdAllocator.py`, `DB_UnifiedSales_*.py`는 **절대 수정하지 않는다.** `item_id` 생성 로직은 정상이다.
- `unitprice`는 PK에 넣지 않는다. 표시 전용이며 join map이 쓰지 않는다.
- `MAP_COLUMNS`에서 `대표메뉴`를 제거하지 않는다. `REVIEW_COLUMNS`에서만 제거한다.
- `OKPOS_ADJUSTMENT_ITEM_ID`(`"19999999"`)는 여러 상품이 공유하는 sentinel이므로 절대 PK 그룹에 넣지 않는다.
- `_seed_split_rows_from_siblings`는 donor가 없으면 **아무것도 하지 않는다.** 추측으로 분류를 채우지 않는다.
- `AUTO_APPROVE_ZERO_PRICE`는 스위치 한 줄로 끌 수 있어야 한다. 0원→옵션 추정은 휴리스틱이며 `2인 순살 닭도리탕`(0원, 반반 구성품)처럼 실제로는 메인인 라인이 있다.
- `print` 금지. `logger.info` / `logger.warning` 사용.
- 기존 함수 시그니처(`migrate_product_map(dry_run=False, **context)` 등)를 바꾸지 않는다. DAG가 `PythonOperator`로 직접 호출한다.
- `_review_few_shot_rows`(`DB_FinProduct.py:630`)는 수정 불필요. 이미 가드가 있다.

### 범위 외 (후속 과제로만 기록, 이번에 손대지 않음)

`item_id`는 두 종류가 한 컬럼에 섞여 있다:
- `okpos`/`unionpos`/`easypos` → 실제 POS 코드 기반, 이름이 바뀌어도 안정
- `배민수동`/`쿠팡수동`/`posfeed` → `normalize_item_key(item_name)` 기반(`DB_ItemIdAllocator.py:211`). 상품명이 바뀌면 새 `item_id`가 발급되어 과거 매출과 끊긴다

이 4개 소스에서는 `item_id ≡ item_key`라 이번 PK 변경의 영향이 없다(그래서 충돌 27건이 전부 okpos였다). 이름 변경 내성은 별도 이슈이며 이번 변경으로 악화되지 않는다.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`def f(df: pd.DataFrame) -> pd.DataFrame:`)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, private helper는 `_` prefix, 기존 파일과 동일하게
- 헬퍼 함수 배치: 호출하는 함수 바로 위
