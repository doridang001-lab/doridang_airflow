# 쿠팡수동 order_type 포장 정규화 (배달_포장)

## Task
unified_sales mart(`unified_sales_grp/unified_sales_*`)에 무효값 `order_type = '포장'`이 존재한다. 이 값은 쿠팡수동(쿠팡이츠) 직수집 경로에서만 발생하며, 쿠팡 확장이 스크랩한 raw `delivery_type`(`"배달"`/`"포장"`)을 그대로 흘려보내기 때문이다. 다른 배달 파이프라인(배민·posfeed·okpos)은 이미 `포장`을 `배달_포장`으로 정규화하므로, 쿠팡도 동일하게 맞추고 기존 적재 데이터도 전체 소급 교정한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지 (scripts는 print 허용이나 logger 우선)
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- unified 공통 상수: `from modules.transform.pipelines.db.DB_UnifiedSales_common import ...`
- scripts는 분석/교정용: `main()`이 요약 dict 반환, `argparse` 사용

## Files to Create / Modify
- **수정**: `modules/transform/pipelines/db/DB_UnifiedSales_coupang.py` (line 256, `order_type` 매핑)
- **생성**: `scripts/fix_coupang_order_type_pojang.py` (기존 unified_sales parquet 전체 소급 교정)

## Implementation Steps

### 1. 파이프라인 근본 수정 — DB_UnifiedSales_coupang.py line 256
현재:
```python
out["order_type"] = df.get("delivery_type", "").fillna("").astype(str).str.strip().replace("", "배달")
```
→ 배민 패턴과 동일하게 변경:
```python
delivery_type = df.get("delivery_type", "").fillna("").astype(str).str.strip()
out["order_type"] = delivery_type.map(lambda v: "배달_포장" if "포장" in v else "배달")
```
- 빈 값/`"배달"` → `"배달"`, `"포장"` 포함 → `"배달_포장"`.
- `_transform_to_unified` 함수 내부. 다른 라인 변경 금지.

### 2. 소급 교정 스크립트 — scripts/fix_coupang_order_type_pojang.py 신규
- `DB_UnifiedSales_common.UNIFIED_ROOT`의 `unified_sales_*.parquet` 전체 순회
- 치환 조건: `source == "쿠팡수동"` **AND** `order_type == "포장"` 인 행만 `order_type = "배달_포장"`
  - 다른 source / 다른 order_type(`홀_포장` 등)은 절대 건드리지 않는다 (오염 방지)
- 변경 행이 존재하는 파일만 재저장 (`to_parquet(path, index=False, engine="pyarrow")`)
- 파일별 교정 행수 logger 출력
- `main()` → `{"files_changed": int, "rows_fixed": int}` 반환
- 경로 하드코딩 금지: `UNIFIED_ROOT` 상수 사용

## Reference Code

### modules/transform/pipelines/db/DB_UnifiedSales_baemin.py (order_type 규약 원본)
```python
out["order_type"] = df["수령방법"].map(
    lambda v: "배달_포장" if str(v).strip() == "포장" else "배달"
)
```

### modules/transform/pipelines/db/DB_UnifiedSales_coupang.py (수정 대상, 발췌)
```python
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS, UNIFIED_ROOT, _make_unified_pk, _unified_daily_path, iter_unified_sales_files,
)

COUPANG_SOURCE = "쿠팡수동"
COUPANG_PLATFORM = "쿠팡이츠"

def _transform_to_unified(df, store, brand, store_map):
    out = pd.DataFrame(index=df.index)
    ...
    out["source"] = COUPANG_SOURCE
    out["platform"] = COUPANG_PLATFORM
    # ↓ 이 라인이 수정 대상
    out["order_type"] = df.get("delivery_type", "").fillna("").astype(str).str.strip().replace("", "배달")
    ...
    return out.reindex(columns=UNIFIED_COLUMNS, fill_value="")
```

### modules/transform/pipelines/db/DB_UnifiedSales_common.py (경로/컬럼 상수)
```python
# UNIFIED_ROOT: .../data/mart/unified_sales_grp
# unified_sales_*.parquet
# UNIFIED_COLUMNS: [..., "source", "platform", "order_type", ...]
# iter_unified_sales_files(): unified_sales parquet 경로 이터레이터
```

## Test Cases
1. [매핑 로직] `python -c "import pandas as pd; s=pd.Series(['배달','포장','','배달포장']); print(s.map(lambda v:'배달_포장' if '포장' in v else '배달').tolist())"` → 기대: `['배달', '배달_포장', '배달', '배달_포장']`
2. [모듈 import] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_coupang import reconcile_coupang_for_test_stores"` → 기대: ImportError 없음
3. [스크립트 import] `python -c "from scripts.fix_coupang_order_type_pojang import main"` → 기대: ImportError 없음
4. [소급 실행] `python scripts/fix_coupang_order_type_pojang.py` → 기대: 교정 행수 로그 출력, `{"files_changed":N,"rows_fixed":M}` 요약
5. [최종 검증] 전체 unified_sales parquet에서 `order_type == "포장"` 행 0건 → 기대: `쿠팡수동` 잔여 `포장` 0

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 소급 스크립트는 `source == "쿠팡수동"` AND `order_type == "포장"` 행만 치환. 그 외 source/order_type(`홀_포장`, `배달` 등)은 불변.
- 파이프라인 수정은 line 256 한 줄만. 함수 시그니처·다른 컬럼 로직 유지.
- 경로 하드코딩 금지 — `UNIFIED_ROOT` 상수 사용.
- parquet 저장 시 `engine="pyarrow"`, `index=False` 유지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
