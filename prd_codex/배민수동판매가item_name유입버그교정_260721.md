# 배민수동 판매가→item_name 유입 버그: 원본 데이터 교정 + 근본 수집 수정

## Task
배민 크롬확장 수집 시, 할인/프로모션 메뉴("[복날한정] 미나리 수삼 백숙" 등)의 기본 옵션 라인에서 `주문옵션상세`에 옵션명 대신 **정가(판매가) 숫자**가 유입된다. 이 값이 그대로 unified_sales의 `item_name`으로 들어가 순수 숫자 item_name(=가격)이 된다. **수집(확장) 근본 수정 + 변환단 방어 가드 + 기존 원본/unified 데이터 교정**을 모두 수행한다.

- 오염 필드는 `menu_name`이 아니라 **`item_name`**(=`주문옵션상세`)이다. `menu_name`(=`주문내역`)은 전부 정상.
- 실측 스코프: unified `item_name`이 순수숫자인 배민수동 행 = **34행 / 4개 매장 / 2026-07**
  - 송파삼전점 17, 전주전북대점 7, 중랑면목점 6, 강원영월점 4 (모두 `DELIVERY_MANUAL_TEST_STORES`)
  - 원본 `baemin_macro/orders` 2026-07 parquet의 `주문옵션상세` 순수숫자도 동일 34건.
- 예) 주문 `T2EE00009H5W`: `주문내역="[복날한정] 1인 미나리 수삼 백숙"`, `주문옵션상세="16900"`(정가), `주문옵션금액="12000"`(실결제) → unified `item_name="16900"`.
- 2026-07-13 PRD(`fin_product_map가격item_name제외_260713.md`)는 fin_product_map 레이어에서 **숨기기만** 했고 원천/수집/원본은 미수정 → 이번에 근본 교정.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- **수정(수집 근본)**: `coupang_extension_build/content/02_baemin.js` (옵션 파싱 루프)
- **수정(변환 방어)**: `modules/transform/pipelines/db/DB_UnifiedSales_baemin.py` (`_transform_to_unified`)
- **생성(1회성 원본 교정)**: 임시 스크립트 (예: `scripts/fix_baemin_price_optname_260721.py`)
- **실행(unified 재생성)**: `reconcile_baemin_for_test_stores(...)` 호출
- **(선택) 테스트 추가**: `tests/test_baemin_manual_load.py`

## Implementation Steps

### 1. 변환단 방어 가드 — `DB_UnifiedSales_baemin.py`
`_transform_to_unified`에서 `out["item_name"] = df["주문옵션상세"]...`(현재 **523줄**) 를 아래로 교체.
순수 숫자(가격) `주문옵션상세`는 옵션명이 아니므로 `menu_name`으로 폴백(coupang 패턴 이식). `unit_price`(=`주문옵션금액`, 실결제)는 불변.

```python
opt = df["주문옵션상세"].fillna("").astype(str).str.strip()
is_price = opt.str.fullmatch(r"[\d,]+")
out["item_name"] = opt.mask(is_price, out["menu_name"])
```
- 확장이 뚫려도 unified에는 가격이 item_name으로 절대 못 들어가는 영구 방어막.

### 2. 근본 수집 수정 — `content/02_baemin.js` (옵션 파싱 `~421-446`)
옵션명 스팬에서 취소선 정가(`.DetailInfo-module__t8S5`)를 제거 후 텍스트를 읽고, 그래도 순수 가격이면 메뉴명으로 폴백한다.

- (a) `const optName = spans[0]?.textContent.trim() || '';` 를, 가격 스팬 처리(430-437줄)와 동일하게 **정가 요소를 clone-후-remove** 하고 읽도록 변경:
  ```js
  let optName = '';
  const nameSpan = spans[0];
  if (nameSpan) {
    if (nameSpan.querySelector('.DetailInfo-module__t8S5')) {
      const c = nameSpan.cloneNode(true);
      c.querySelector('.DetailInfo-module__t8S5')?.remove();
      optName = c.textContent.trim();
    } else {
      optName = nameSpan.textContent.trim();
    }
  }
  ```
- (b) 방어 가드: 정리 후에도 `/^[\d,]+원?$/.test(optName)` 이면 옵션명으로 쓰지 않고 `menuName`으로 폴백:
  ```js
  if (/^[\d,]+원?$/.test(optName)) optName = menuName;
  ```
- (c) `base` 폴백(`options.length===0` → `name: menuName`)은 이미 안전, 변경 없음.

### 3. 기존 원본 parquet 교정 (1회성 스크립트)
4개 매장 `baemin_macro/orders/brand=*/store={store}/ym=2026-07/orders_2026-07.parquet`에서
`주문옵션상세`가 순수숫자(`^[\d,]+$`)인 행을 같은 행의 메뉴명(`주문내역`에서 ` 외 N건` 제거)으로 치환.
`주문옵션금액`/`결제금액` 등 금액은 불변. 저장 전 백업은 원본 옆이 아니라
`LOCAL_DB/temp/fix_baemin_price_optname/<run_id>/`에 만들고 성공 또는 정상 롤백 후 정리한다.

```python
import re, glob, shutil
import pandas as pd
from modules.transform.utility.paths import BAEMIN_ORDERS_DB

STORES = ["송파삼전점", "전주전북대점", "중랑면목점", "강원영월점"]
suffix = re.compile(r"\s*외\s*\d+건$")
for store in STORES:
    for f in glob.glob(str(BAEMIN_ORDERS_DB / "brand=*" / f"store={store}" / "ym=2026-07" / "orders_2026-07.parquet")):
        df = pd.read_parquet(f)
        opt = df["주문옵션상세"].fillna("").astype(str).str.strip()
        mask = opt.str.fullmatch(r"[\d,]+")
        if not mask.any():
            continue
        menu = df["주문내역"].fillna("").astype(str).str.replace(suffix, "", regex=True).str.strip()
        # 실제 구현은 _backup_paths()로 LOCAL_DB/temp 아래에 백업한다.
        df.loc[mask, "주문옵션상세"] = menu[mask]
        df.to_parquet(f, index=False, engine="pyarrow")
        print(f"fixed {int(mask.sum())} rows -> {f}")
```

### 4. unified 재생성
Step 1의 가드가 적용된 상태에서 교정된 원본으로 재적재:
```python
from modules.transform.pipelines.db.DB_UnifiedSales_baemin import reconcile_baemin_for_test_stores
print(reconcile_baemin_for_test_stores(
    stores=["송파삼전점", "전주전북대점", "중랑면목점", "강원영월점"],
    sale_date=None, lookback_days=None,   # 원천/기존 배민수동 날짜 전체 재교정
))
```

### 5. fin_product_map 정합 (선택)
2026-07-13 PRD의 순수숫자 제외 가드는 유지. 원본 정상화 후 `migrate_product_map()` 재실행 시 잔여 가격행(`200008080`/`200008081`류) 자연 소거.

## Reference Code

### modules/transform/pipelines/db/DB_UnifiedSales_baemin.py (`_transform_to_unified` 현재)
```python
BAEMIN_SOURCE = "배민수동"
BAEMIN_PLATFORM = "배달의민족"

def _transform_to_unified(df, store, brand, store_map):
    df = df[df["주문상태"].fillna("").astype(str).str.strip().eq("배달완료")].copy()
    if df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
    out = pd.DataFrame(index=df.index)
    out["sale_date"] = df["sale_date"]
    ...
    out["menu_name"] = (
        df["주문내역"].fillna("").astype(str)
        .str.replace(r"\s*외\s*\d+건$", "", regex=True).str.strip()
    )
    out["item_name"] = df["주문옵션상세"].fillna("").astype(str).str.strip()   # ← 523줄, 교체 대상
    out["qty"] = _to_int_series(df["주문수량"]).replace(0, 1)
    out["unit_price"] = _to_int_series(df["주문옵션금액"])
    ...
```

### modules/transform/pipelines/db/DB_UnifiedSales_coupang.py (정상 mask 폴백 패턴 — 이식 참고)
```python
out["menu_name"] = df["order_summary"].fillna("").astype(str).str.replace(r"\s*외\s*\d+건$", "", regex=True).str.strip()
item_name = df["menu_options"].fillna("").astype(str).str.strip()
item_name = item_name.mask(item_name.eq("") | item_name.eq("nan"), df["menu_name"].fillna("").astype(str).str.strip())
out["item_name"] = item_name
```

### coupang_extension_build/content/02_baemin.js (옵션 파싱 현재 ~421-446)
```javascript
if (optionsContainer?.classList.contains('DetailInfo-module__J1rX')) {
  const optDivs = optionsContainer.querySelectorAll('.DetailInfo-module__n2Ro');
  for (const opt of optDivs) {
    const spans = opt.querySelectorAll('span');
    const optName = spans[0]?.textContent.trim() || '';   // ← 정가요소 미제거 → 숫자 유입
    let optPrice = '';
    const priceSpan = spans[1];
    if (priceSpan) {
      const originalPriceEl = priceSpan.querySelector('.DetailInfo-module__t8S5');
      if (originalPriceEl) {
        const clone = priceSpan.cloneNode(true);
        clone.querySelector('.DetailInfo-module__t8S5')?.remove();
        optPrice = Utils.cleanPrice(clone.textContent);
      } else {
        optPrice = Utils.cleanPrice(priceSpan.textContent);
      }
    }
    if (optName) options.push({ name: optName, price: optPrice });
  }
}
if (options.length === 0) {
  options.push({ name: menuName, price: menuPrice });   // base 폴백 — 안전
}
```

### dags/db/DB_UnifiedSales_Dags.py (재적재 진입점)
```python
def reconcile_baemin(**context) -> str:
    ...
    from modules.transform.pipelines.db.DB_UnifiedSales_baemin import reconcile_baemin_for_test_stores
    sale_date = context["ti"].xcom_pull(task_ids="resolve_date", key="sale_date")
    return reconcile_baemin_for_test_stores(
        stores=stores, sale_date=sale_date, lookback_days=_manual_delivery_lookback_days(context),
    )
```

## Test Cases
1. [import 정상] `python -c "import modules.transform.pipelines.db.DB_UnifiedSales_baemin"` → 기대: ImportError 없음
2. [변환 가드 단위] 순수숫자 `주문옵션상세` → `item_name`이 메뉴명으로 폴백:
   ```
   python -c "import pandas as pd; from modules.transform.pipelines.db.DB_UnifiedSales_baemin import _transform_to_unified; df=pd.DataFrame({'주문상태':['배달완료'],'주문번호':['T1'],'수령방법':['배달'],'주문내역':['[복날한정] 1인 미나리 수삼 백숙'],'주문옵션상세':['16900'],'주문수량':['1'],'주문옵션금액':['12000'],'결제금액':['12000'],'sale_date':['2026-07-08'],'order_time':['12:00:00']}); o=_transform_to_unified(df,'송파삼전점','도리당',{}); print(o['item_name'].tolist())"
   ```
   → 기대: `['[복날한정] 1인 미나리 수삼 백숙']` (숫자 '16900' 아님)
3. [원본 스캔 0건] 교정 스크립트 실행 후 4개 매장 2026-07 `주문옵션상세` 순수숫자 = 0
   ```
   python -c "import glob,pandas as pd; from modules.transform.utility.paths import BAEMIN_ORDERS_DB; n=0
   for f in glob.glob(str(BAEMIN_ORDERS_DB/'brand=*'/'store=*'/'ym=2026-07'/'orders_2026-07.parquet')):
       d=pd.read_parquet(f); s=d['주문옵션상세'].fillna('').astype(str).str.strip(); n+=int(s.str.fullmatch(r'[\d,]+').sum())
   print('pure=',n)"
   ```
   → 기대: `pure= 0`
4. [unified 스캔 0건] 재적재 후 배민수동 `item_name` 순수숫자 = 0
   ```
   python -c "import glob,pandas as pd; from modules.transform.pipelines.db.DB_UnifiedSales_common import UNIFIED_ROOT; n=0
   for f in glob.glob(str(UNIFIED_ROOT/'unified_sales_*.parquet')):
       d=pd.read_parquet(f,columns=['source','item_name']); m=d[d['source']=='배민수동']
       n+=int(m['item_name'].fillna('').astype(str).str.strip().str.fullmatch(r'[\d,]+').sum())
   print('pure=',n)"
   ```
   → 기대: `pure= 0`
5. [금액 보존] 교정 전후 4개 매장 2026-07 배민수동 `total_price` 합계 불변 (±0)
6. [DAG import] `python -c "from dags.db.DB_UnifiedSales_Dags import dag"` → 기대: ImportError 없음

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 순서대로 실행 (1→2: 코드 가드, 3: 원본교정, 4~5: 재적재 후, 6: DAG)
  2. FAIL 항목 → 원인 분석
  3. 코드/스크립트 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `unit_price`(=`주문옵션금액`, 실결제금액)와 `결제금액`/`total_price` 등 **금액은 절대 변경 금지**. 이번 교정은 `주문옵션상세`(=item_name) 텍스트 정상화만 대상.
- 순수숫자 판별은 `str.fullmatch(r"[\d,]+")` (콤마 허용, 소수점/기타 확장 금지).
- 원본 parquet 교정 전 반드시 `LOCAL_DB/temp`에 임시 백업하되 OneDrive 원본 폴더에는 `.bak`을 만들지 말 것.
- fin_product_map의 기존 `\d+` 제외 가드는 제거하지 말 것(중복 무해).
- 확장(02_baemin.js) 배포는 사용자 수동 복사 방식 — 코드 수정만 하고 배포는 사용자에게 안내.
- 폴더 구조/파일명 변경 금지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
