# 배민 orders·shop_change CSV→Parquet 적재 전환

## Task
배민 수집 데이터 중 용량이 큰 `orders`(17MB, 약 23× 압축)와 `shop_change`(약 4.3× 압축)를 OneDrive에 적재할 때 CSV 대신 parquet(zstd)로 저장한다. 수집/다운로드 단계는 CSV 그대로 두고 **적재(저장) 시점에만 parquet로 변환**한다. 기존에 쌓인 CSV는 일괄 변환 스크립트로 옮기고, 앞으로 수집되는 것도 parquet로 적재한다. `shop_operation`은 절감폭이 작아 CSV 유지(읽기만 호환).

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 파티션 구조 유지: `brand={brand}/store={store}/ym={YYYY-MM}/{name}.{ext}` — 확장자만 csv→parquet
- 모든 셀은 **문자열(dtype=str)** 로 다룬다(주문번호 앞자리 0, 금액 문자열 보존)
- parquet 압축: zstd, `engine="pyarrow"`

## Files to Create / Modify
- 생성: `modules/transform/pipelines/db/beamin_store_io.py` — 공용 IO 헬퍼
- 생성: `scripts/migrate_baemin_csv_to_parquet.py` — 기존 CSV 일괄 변환(dry-run/apply/verify)
- 수정: `modules/transform/pipelines/db/DB_Beamin_04_orders.py` — 쓰기 1(`_save_orders_csv`), 읽기 1(`_csv_already_covers`)
- 수정: `modules/transform/pipelines/db/DB_Beamin_Macro_validate.py` — 쓰기 1(`import_manual_baemin_csvs`), 읽기 4
- 수정: `modules/transform/pipelines/db/DB_Beamin_03_shop_change.py` — 쓰기 1(live `_save_csv`), 읽기 2
- 수정: `modules/transform/pipelines/db/DB_Beamin_monthly_operation.py` — 읽기(shop_change/shop_operation)
- 범위 외(그대로): 수집 다운로드 CSV(`영업관리부_수집`), `shop_operation` 쓰기, `monthly_operation.csv`, 대시보드(데이터 파일 안 읽고 로그만 파싱)

## Implementation Steps

1. **공용 헬퍼 `beamin_store_io.py` 생성** (아래 Reference Code 그대로):
   - `write_table(df, stem_path)`: parquet(zstd) 저장 + 같은 위치 레거시 CSV 자동 삭제. `stem_path`는 **확장자 없는 경로**.
   - `read_table(stem_path, columns=None)`: parquet 우선·CSV 폴백, 없으면 `None`. 전부 문자열 복원.
   - `read_file(path, columns=None)`: 확장자 있는 실제 파일을 suffix로 분기해 읽음.
   - `find_tables(base, rel_glob_stem)`: parquet·csv 양쪽 glob, 같은 stem이면 parquet 우선.

2. **orders 쓰기** — `DB_Beamin_04_orders.py:_save_orders_csv`:
   - `out_path = out_dir / f"orders_{ym}.csv"` → `stem = out_dir / f"orders_{ym}"`
   - `out_path.exists()`+`pd.read_csv` → `existing = read_table(stem); if existing is not None:`
   - `combined.to_csv(...)` → `out_path = write_table(combined, stem)`
   - import 추가: `from modules.transform.pipelines.db.beamin_store_io import read_table, write_table`

3. **orders 읽기** — 같은 파일 `_csv_already_covers`: `path=.../orders_{ym}.csv`+`pd.read_csv` → `stem=.../orders_{ym}`; `df = read_table(stem); if df is None: return False`.

4. **manual import 쓰기** — `DB_Beamin_Macro_validate.py:import_manual_baemin_csvs`: orders upsert을 2번과 동일 패턴으로 `read_table`/`write_table` 전환.

5. **orders 읽기 4곳** — `DB_Beamin_Macro_validate.py`:
   - `_baemin_orders_by_store`, `_inspect_brand_coverage`, `delete_baemin_orders_for_date`: `BAEMIN_ORDERS_DB.glob(".../orders_{ym}.csv")` → `find_tables(BAEMIN_ORDERS_DB, ".../orders_{ym}")`, `pd.read_csv(p,...)` → `read_file(p)`.
   - `_delete_orders_for_stores`: 읽고 행 삭제 후 재기록 — `read_file(p)` 후 `write_table(df, p.with_suffix(""))`.
   - import 추가: `from modules.transform.pipelines.db.beamin_store_io import find_tables, read_file, read_table, write_table`

6. **shop_change 쓰기** — `DB_Beamin_03_shop_change.py` **live `_save_csv`(파일 내 두 번째 정의, `_row_signature` dedup 버전)**: `out_path/"shop_change.csv"` → `stem/"shop_change"`, `read_table(stem)` 폴백, `write_table(combined, stem)`. (첫 번째 dead `_save_csv`는 호출 안 되므로 변경 불필요.)

7. **shop_change 읽기 2곳** — `_last_collected_date`, `_existing_change_keys`: `BAEMIN_SHOP_CHANGE_DB.glob(".../shop_change.csv")` → `find_tables(..., ".../shop_change")`, `pd.read_csv(p, usecols=[...])` → `read_file(p, columns=[...])`.

8. **monthly_operation 읽기** — `DB_Beamin_monthly_operation.py`: `_load_change_df_for_ym`, `_get_daily_hours_by_weekday`, `_extract_weekday_hours_from_change_csv`의 shop_change/shop_operation 읽기를 `read_table`/`read_file`/`find_tables`로 전환. (shop_operation은 CSV 유지지만 헬퍼가 폴백 처리. `find_tables(base, "ym=*/shop_change")[::-1]`로 최신 ym 우선 정렬.)

9. **마이그레이션 스크립트 `scripts/migrate_baemin_csv_to_parquet.py`**: orders + shop_change CSV glob → parquet 저장 → **행수 검증** 통과 시 원본 CSV 삭제. `--apply`(미지정 시 dry-run), `--keep-csv`(보존), `--include-operation`(옵션).

## Reference Code

### modules/transform/pipelines/db/beamin_store_io.py (신규 — 이 내용 그대로 생성)
```python
from __future__ import annotations
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)
PARQUET_COMPRESSION = "zstd"

def _as_str_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.fillna("").astype(str)

def read_table(stem_path: Path, columns: list[str] | None = None) -> pd.DataFrame | None:
    pq = stem_path.with_suffix(".parquet"); csv = stem_path.with_suffix(".csv")
    if pq.exists():
        return _as_str_df(pd.read_parquet(pq, columns=columns))
    if csv.exists():
        return _as_str_df(pd.read_csv(csv, dtype=str, encoding="utf-8-sig", usecols=columns))
    return None

def read_file(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return _as_str_df(pd.read_parquet(path, columns=columns))
    return _as_str_df(pd.read_csv(path, dtype=str, encoding="utf-8-sig", usecols=columns))

def write_table(df: pd.DataFrame, stem_path: Path) -> Path:
    stem_path.parent.mkdir(parents=True, exist_ok=True)
    pq = stem_path.with_suffix(".parquet")
    _as_str_df(df).to_parquet(pq, engine="pyarrow", compression=PARQUET_COMPRESSION, index=False)
    csv = stem_path.with_suffix(".csv")
    if csv.exists():
        try: csv.unlink()
        except OSError as exc: logger.warning("레거시 CSV 삭제 실패: %s / %s", csv, exc)
    return pq

def find_tables(base: Path, rel_glob_stem: str) -> list[Path]:
    found: dict[Path, Path] = {}
    for suffix in (".csv", ".parquet"):   # parquet를 뒤에 둬 같은 stem이면 덮어씀(우선)
        for p in base.glob(rel_glob_stem + suffix):
            found[p.with_suffix("")] = p
    return sorted(found.values())
```

### 기존 쓰기 패턴 (orders, 전환 전→후 예시)
```python
# 전: out_path=.../orders_{ym}.csv ; if out_path.exists(): existing=pd.read_csv(...) ; combined.to_csv(...)
# 후:
stem = BAEMIN_ORDERS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}" / f"orders_{ym}"
existing = read_table(stem)
if existing is not None:
    existing = existing[~existing["주문번호"].isin(new_order_ids)]
    combined = pd.concat([existing, new_df], ignore_index=True)
else:
    combined = new_df
out_path = write_table(combined, stem)
```

### scripts/migrate_baemin_csv_to_parquet.py 핵심 로직 (행수 검증 후 CSV 삭제)
```python
import argparse, sys
from pathlib import Path
import pandas as pd
from modules.transform.utility.paths import (
    BAEMIN_ORDERS_DB, BAEMIN_SHOP_CHANGE_DB, BAEMIN_SHOP_OPERATION_DB)
from modules.transform.pipelines.db.beamin_store_io import PARQUET_COMPRESSION

def _convert_one(csv_path: Path, apply: bool, keep_csv: bool):
    pq = csv_path.with_suffix(".parquet")
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig").fillna("").astype(str)
    if not apply:
        return True, f"DRY {csv_path.name}: {len(df)}행 예정"
    df.to_parquet(pq, engine="pyarrow", compression=PARQUET_COMPRESSION, index=False)
    if len(pd.read_parquet(pq)) != len(df):              # 행수 검증
        return False, f"검증 실패: {csv_path}"
    if not keep_csv:
        csv_path.unlink()
    return True, f"OK {csv_path.name}"
# targets: orders(brand=*/store=*/ym=*/orders_*.csv), shop_change(.../shop_change.csv)
# --apply 미지정 시 dry-run, --keep-csv 보존, --include-operation 으로 shop_operation 추가
```

## Test Cases
1. [import] `python -c "import importlib; [importlib.import_module(m) for m in ['modules.transform.pipelines.db.beamin_store_io','modules.transform.pipelines.db.DB_Beamin_04_orders','modules.transform.pipelines.db.DB_Beamin_03_shop_change','modules.transform.pipelines.db.DB_Beamin_monthly_operation','modules.transform.pipelines.db.DB_Beamin_Macro_validate','scripts.migrate_baemin_csv_to_parquet']]; print('OK')"` → 기대: `OK`
2. [헬퍼 라운드트립] 임시폴더에 `write_table` → `read_table` 후 `주문번호==['001','002']`(앞자리 0 보존), parquet 파일 존재, `find_tables`가 parquet 우선 반환 → 기대: assert 통과
3. [CSV 폴백] parquet 없는 위치에 `.csv`만 두고 `read_table`이 `None` 아님; 이후 `write_table` 호출 시 `.csv` 삭제·`.parquet` 생성 → 기대: assert 통과
4. [마이그레이션 dry-run] `python -m scripts.migrate_baemin_csv_to_parquet` → 기대: `[DRY-RUN] ... fail=0`
5. [압축률] 가장 큰 orders 샘플 1개 parquet(zstd) 변환 비율 → 기대: orders ≈ 20× 이상

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 수집/다운로드 CSV(`영업관리부_수집/baemin_orders_*.csv`)는 **외부 입력**이라 전환 대상 아님.
- `shop_operation` **쓰기**는 CSV 유지(절감폭 작음). 단 읽기는 헬퍼로 parquet/csv 모두 호환.
- `dtype=str` 일관성 유지 — parquet도 전 컬럼 문자열로 저장/복원.
- 파티션 폴더 구조·파일 stem 이름 변경 금지(확장자만 변경).
- 읽기 글로브/`usecols`(→`columns=`)를 쓰기와 **동시에** 전환(반쪽 전환 시 빈 결과).
- 마이그레이션 `--apply`는 OneDrive 운영 데이터를 수정/삭제 → 행수 검증 통과 시에만 원본 CSV 삭제. 먼저 `--keep-csv`로 보존 변환 권장.
- 대시보드(`db_beamin_macro_dashboard.py`)는 데이터 파일을 읽지 않으므로 수정 금지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code 패턴 따라가기
- 타입 힌트·주석: 기존 파일과 동일, 주석은 WHY만 최소화
- 변수명·함수명: snake_case, 기존 파일과 동일
- 헬퍼 위치 모호 시: `modules/transform/pipelines/db/beamin_store_io.py` 고정
