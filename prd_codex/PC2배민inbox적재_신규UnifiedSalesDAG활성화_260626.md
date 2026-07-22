# PC2 배민 inbox 적재 (중복 없이 추가) + 신규 UnifiedSales DAG 활성화

## Task
PC2 머신의 배민 매크로가 OneDrive `영업관리부_수집/_baemin_pc2_inbox/manual__<run_id>/baemin_macro/...` 로 수집물을 떨구지만, 이를 메인 머신의 `ANALYTICS_DB/baemin_macro/...` 파티션으로 옮기는 코드가 없어 unified_sales에 반영되지 않고 폴더에 누적된다. inbox→파티션 분배 모듈을 **중복 없이 신규만 추가**되도록 구현하고, 처리한 inbox 폴더는 정리하며, 적재 흐름을 신규 `DB_UnifiedSales` DAG 실행으로 일원화한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 셀은 문자열(dtype=str)로 다룬다 (주문번호 앞자리 0, 금액 문자열 보존)

## Files to Create / Modify
- **생성**: `modules/transform/pipelines/db/DB_Beamin_pc2_distribute.py`
- **수정**: `dags/.airflowignore` (한 줄 교체)
- 변경 없음(참고만): `dags/db/DB_UnifiedSales_Dags.py` (이미 `ingest_baemin_pc2_inbox` 태스크 작성됨), `dags/db/DB_CollectionCompare_Dags.py`

## inbox 구조 (디스크 확인 완료)
```
_baemin_pc2_inbox/manual__<run_id>/
├── manifest.json                         # {상대경로: 행수} 목록 (참고용, 신뢰 안 해도 됨)
└── baemin_macro/<subtype>/brand=*/store=*/ym=*/<file>.parquet
    subtype: orders, ad_funnel, metrics_now, metrics_our_store_clicks,
             monthly_operation, shop_change, shop_operation
```
- orders 파일은 **월 누적 풀파일** (예: 189행 / 주문번호 nunique 28). 그래서 주문번호 upsert가 안전.
- 핵심: `ANALYTICS_DB / "baemin_macro" == BAEMIN_ORDERS_DETAIL_DB`. 즉 inbox 폴더 기준 상대경로
  `baemin_macro/...` 를 그대로 `ANALYTICS_DB / rel_path` 에 매핑하면 목적지 파티션과 정확히 일치.

## Implementation Steps

### 1. `DB_Beamin_pc2_distribute.py` 생성
함수 `ingest_baemin_pc2_inbox(**context) -> str` 구현.

상수:
```python
from modules.transform.utility.paths import COLLECT_DB, ANALYTICS_DB
INBOX_DIR = COLLECT_DB / "영업관리부_수집" / "_baemin_pc2_inbox"
ARCHIVE_DIR = INBOX_DIR / "_archived"
ORDER_KEY = "주문번호"
```

로직:
1. `INBOX_DIR` 없으면 `"pc2 inbox 없음: 스킵"` 반환.
2. `sorted(p for p in INBOX_DIR.glob("manual__*") if p.is_dir())` 순회.
3. 각 폴더에서 `folder.rglob("*")` 중 `suffix in {".parquet", ".csv"}` 이고 경로에 `baemin_macro` 가 포함된 파일만 처리.
4. 파일별 분배(아래 `_distribute_one_file`). 폴더의 **모든 파일 성공 시에만** 폴더를 `_archived/` 로 이동(`_archive_path` 로 중복명 회피). 에러 발생 시 그 폴더는 보존하고 다음 폴더 계속(재시도). manifest.json 등 비대상 파일은 폴더 이동 시 함께 따라감.
5. 처리 폴더/파일/행수 요약 문자열 반환. 진행상황은 `logger.info` 로 기록.

`_distribute_one_file(folder: Path, src_file: Path) -> tuple[str, int]`:
```python
rel = src_file.relative_to(folder)          # baemin_macro/orders/brand=../ym=../orders_2026-06.parquet
subtype = rel.parts[1]                       # 'orders', 'ad_funnel', ...
dst_stem = (ANALYTICS_DB / rel).with_suffix("")  # 확장자 제거 stem
new_df = read_file(src_file)                 # parquet/csv 모두 str DataFrame
if subtype == "orders" and ORDER_KEY in new_df.columns:
    existing = read_table(dst_stem)
    if existing is not None and ORDER_KEY in existing.columns:
        new_ids = set(new_df[ORDER_KEY].unique())
        existing = existing[~existing[ORDER_KEY].isin(new_ids)]   # 동일 주문번호 기존행 제거
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
    write_table(combined, dst_stem)
    return subtype, len(new_df)
# 그 외 snapshot subtype: PC2 풀스냅샷으로 파티션 replace
write_table(new_df, dst_stem)
return subtype, len(new_df)
```

`_archive_path(folder: Path) -> Path`: `DB_BaeminManual_load._archive_path` 와 동일 패턴(`name`, 충돌 시 `.1`, `.2` ... suffix).

### 2. `dags/.airflowignore` 수정
현재 내용(1줄):
```
db/DB_UnifiedSales_Dags.py
```
→ 아래로 교체:
```
db/DB_UnifiedSales.py
```
구버전 DAG(pc2 태스크 없음)를 막고, 신규 `DB_UnifiedSales_Dags.py`(dag_id=`DB_UnifiedSales`, 첫 태스크 `ingest_baemin_pc2_inbox`)를 활성화. 두 파일 dag_id가 동일하므로 동시 활성 금지 — 반드시 한쪽만 ignore.

신규 DAG 태스크 순서(이미 작성됨):
`resolve_date → ingest_baemin_pc2_inbox → wait_for_upstream_pos → build_okpos → ... → reconcile_baemin → ...`
→ inbox가 `baemin_macro/orders` 에 먼저 적재된 뒤 `reconcile_baemin` 이 읽어 unified 교정.

## Reference Code

### modules/transform/pipelines/db/beamin_store_io.py (재사용 — 직접 import)
```python
def read_table(stem_path: Path, columns: list[str] | None = None) -> pd.DataFrame | None:
    """stem(확장자 없음) → parquet 우선·CSV 폴백. 둘 다 없으면 None."""
    pq = stem_path.with_suffix(".parquet"); csv = stem_path.with_suffix(".csv")
    if pq.exists(): return _as_str_df(pd.read_parquet(pq, columns=columns))
    if csv.exists(): return _as_str_df(pd.read_csv(csv, dtype=str, encoding="utf-8-sig", usecols=columns))
    return None

def read_file(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    """확장자 있는 실제 파일(parquet/csv)을 str DataFrame으로."""
    if path.suffix == ".parquet": return _as_str_df(pd.read_parquet(path, columns=columns))
    return _as_str_df(pd.read_csv(path, dtype=str, encoding="utf-8-sig", usecols=columns))

def write_table(df: pd.DataFrame, stem_path: Path) -> Path:
    """str DataFrame을 parquet(zstd) 저장, 같은 위치 레거시 CSV 제거."""
    stem_path.parent.mkdir(parents=True, exist_ok=True)
    pq = stem_path.with_suffix(".parquet")
    _as_str_df(df).to_parquet(pq, engine="pyarrow", compression="zstd", index=False)
    csv = stem_path.with_suffix(".csv")
    if csv.exists():
        try: csv.unlink()
        except OSError as exc: logger.warning("레거시 CSV 삭제 실패: %s / %s", csv, exc)
    return pq
```

### modules/transform/pipelines/db/DB_Beamin_04_orders.py (주문번호 upsert 패턴 참조)
```python
def _save_orders_csv(rows, brand, store, target_date) -> Path:
    """월별 파일로 upsert. 같은 주문번호의 기존 행은 전부 교체."""
    ym = target_date[:7]
    stem = BAEMIN_ORDERS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}" / f"orders_{ym}"
    new_df = pd.DataFrame(rows, columns=_COLUMNS).astype(str)
    new_order_ids = set(new_df["주문번호"].unique())
    existing = read_table(stem)
    if existing is not None:
        existing = existing[~existing["주문번호"].isin(new_order_ids)]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
    return write_table(combined, stem)
```

### modules/transform/pipelines/db/DB_BaeminManual_load.py (archive dedup 네이밍 참조)
```python
ARCHIVE_DIR = COLLECT_SRC / "_archived"
def _archive_path(path: Path) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    target = ARCHIVE_DIR / path.name
    if not target.exists(): return target
    stem, suffix = path.stem, path.suffix
    for idx in range(1, 1000):
        candidate = ARCHIVE_DIR / f"{stem}.{idx}{suffix}"
        if not candidate.exists(): return candidate
    raise RuntimeError(f"cannot create archive path for {path}")
```
※ inbox 분배에서는 `_archive_path` 가 **파일이 아니라 폴더**(`manual__*`)를 받도록 적용하고, 이동은 `shutil.move(str(folder), str(_archive_path(folder)))`.

## Test Cases
1. [import] `python -c "from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import ingest_baemin_pc2_inbox"` → 기대: 에러 없음
2. [분배 실행] `python -c "from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import ingest_baemin_pc2_inbox; print(ingest_baemin_pc2_inbox())"`
   → 기대: 요약 문자열 출력. `ANALYTICS_DB/baemin_macro/orders/brand=도리당/store=송파점/ym=2026-06/orders_2026-06.parquet` 에 병합, 나머지 subtype 파일 생성, inbox `manual__*` → `_baemin_pc2_inbox/_archived/` 이동
3. [중복/멱등 검증] 분배 전 orders 목적지 주문번호 nunique 기록 → 같은 inbox 데이터를 2회 분배(아카이브 폴더를 다시 inbox로 되돌려 재실행) → orders 행수·주문번호 nunique **동일**(중복 0) 확인
4. [DAG 로드] `python -c "from airflow.models import DagBag; b=DagBag('dags'); print([k for k in b.dags if k=='DB_UnifiedSales']); print(b.import_errors)"`
   → 기대: `DB_UnifiedSales` 1건 등록, import_errors 비어있음 (구버전/신버전 dag_id 충돌 없음)

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1→4 순서 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- orders는 **주문번호 upsert**(동일 주문번호 기존 행 제거 후 concat), 그 외 subtype은 **파티션 replace**. orders를 절대 단순 append 하지 말 것(중복 발생).
- IO는 반드시 `beamin_store_io.read_table/read_file/write_table` 재사용(parquet 우선·csv 폴백·zstd·레거시 csv 정리). 직접 `pd.read_parquet`/`to_parquet` 재구현 금지.
- 모든 셀 str 처리(주문번호 0 손실·금액 변형 방지).
- `manual__*` 폴더는 **모든 파일 성공 후에만** archive 이동. 부분 실패 시 폴더 보존(upsert/replace가 멱등이라 재실행 안전).
- `.airflowignore` 는 반드시 한쪽 DAG만 ignore — 두 줄 다 넣거나 둘 다 빼면 dag_id 충돌/누락.
- `DB_CollectionCompare_Dags.py` 는 수정 금지. inbox는 UnifiedSales DAG 단독 소유(두 DAG 동시 처리 시 중복/레이스).
- 스케줄(`DB_UNIFIED_SALES_TIME = "40 8 * * *"`) 변경 금지. PC2가 08:40 전 inbox 미도착 시 그날 스킵, 다음 실행에서 누적분 멱등 처리.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
