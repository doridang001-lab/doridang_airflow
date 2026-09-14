# PC2 배민 적재 + cleanup 정상화 (중앙 Upload DAG 충돌 제거)

## Task

하위 PC(bottom)가 `_baemin_upload_inbox`로 내보낸 배민 수집분을 중앙에서 적재하는 경로에 3가지 결함이 있다.
(1) ad_funnel 월 파일이 하루치로 덮어써져 기존 이력이 삭제되고, (2) 중앙 스케줄 run의 기본 glob `manual__*`이
bottom 폴더까지 선점해 bottom meta가 조용히 버려지며, (3) cleanup(rmtree)이 한 번 실패하면 파이프라인이 영구히 막힌다.
이 3가지를 고쳐 "적재 → 검증 → inbox 폴더 cleanup"이 충돌 없이 끝나게 만든다.

실측 대상 폴더(2매장 테스트, 12파일):
`C:\Users\민준\OneDrive - 주식회사 도리당\Collect_Data\영업관리부_수집\_baemin_upload_inbox\manual__bottom__codex_bottom_two_stores_no_inference__20260723_144446`

실측 결과 — ad_funnel `store=해운대중동점/ym=2026-07`은 기존 10일치(07-01~07-22)인데 export는 07-22 1행뿐이라
현재 로직으로는 **9일치가 삭제된다**. orders/shop_change/clicks/monthly_operation은 유실 0으로 확인됨.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 배민 파티션 IO는 `modules/transform/pipelines/db/beamin_store_io.py`의
  `read_file` / `read_table` / `write_table` / `find_tables`만 사용 (직접 read_csv/to_parquet 금지)

## Files to Create / Modify

- 수정: `modules/transform/pipelines/db/DB_Beamin_pc2_distribute.py`
- 수정: `dags/db/DB_Beamin_Macro_Upload_Dags.py`
- 수정: `tests/test_beamin_upload_inbox_distribute.py`
- 변경 없음: `dags/db/DB_Beamin_Macro_Upload_Pc2_Dags.py` (현재 구성이 맞음)

## Implementation Steps

### 1. ad_funnel upsert (blocker — 데이터 손실 방지)

`DB_Beamin_pc2_distribute.py` 상단에 상수 추가:

```python
CSV_UPSERT_KEYS = {"ad_funnel": "target_date"}
```

기존 `_upsert_orders(dst_stem, new_df)`를 키 파라미터화한 헬퍼로 일반화한다
(기존 `_upsert_orders`는 `_upsert_by_key(dst_stem, new_df, ORDER_KEY)` 호출로 대체 가능):

```python
def _upsert_by_key(dst_stem: Path, new_df: pd.DataFrame, key: str) -> pd.DataFrame:
    existing = read_table(dst_stem)
    if existing is None or key not in existing.columns or key not in new_df.columns:
        return new_df
    new_keys = set(new_df[key].fillna("").astype(str).unique())
    if "" in new_keys:
        logger.warning("%s 빈 키 값 포함: %s", key, dst_stem)
    existing = existing[~existing[key].fillna("").astype(str).isin(new_keys)]
    return pd.concat([existing, new_df], ignore_index=True)
```

`_distribute_one_file`에서 subtype이 `CSV_UPSERT_KEYS`에 있으면
`_upsert_by_key(dst_stem, new_df, CSV_UPSERT_KEYS[subtype])` 결과를 저장한다.
기존 파일 없음 / 키 컬럼 없음이면 new_df 그대로(현행 동작 유지).

> 배경: 중앙 수집기 `DB_Beamin_05_ad_funnel._save_ad_funnel_csv`는 `target_date` 기준 upsert로 월 파일을
> 누적하는데, PC2 경로만 통째 덮어쓰기였다. 하위 PC staging은 매 run `init_empty_staging`으로 비워지므로
> export되는 ad_funnel은 항상 그날 1행뿐이다.

### 2. 저장 포맷을 원본 확장자에 맞추기

현재 `_distribute_one_file`은 orders가 아니면 무조건 `_write_csv_table`(CSV 저장 + 같은 위치 parquet 삭제)을 쓴다.
그런데 shop_change는 중앙 수집기가 `write_table`(parquet+zstd)로 저장하므로 적재 주체마다 포맷이 뒤집힌다.

→ 비-orders 저장 시 **소스 파일 확장자를 따른다**:

```python
if src_file.suffix.lower() == ".parquet":
    out_path = write_table(combined, dst_stem)
else:
    out_path = _write_csv_table(combined, dst_stem)
```

결과: ad_funnel·metrics_our_store_clicks·monthly_operation → CSV 유지, shop_change → parquet 유지.

### 3. 폴더 패턴 분리 (중앙 07:40 run이 bottom 폴더를 선점하지 못하게)

`DB_Beamin_pc2_distribute.py`:

```python
DEFAULT_FOLDER_PATTERN = "manual__*"
TOP_FOLDER_PATTERN = "manual__top__*"
BOTTOM_FOLDER_PATTERN = "manual__bottom__*"
ALLOWED_UPLOAD_FOLDER_PATTERNS = frozenset({
    DEFAULT_FOLDER_PATTERN, TOP_FOLDER_PATTERN, BOTTOM_FOLDER_PATTERN,
})
```

`dags/db/DB_Beamin_Macro_Upload_Dags.py`의 `t_ingest` op_kwargs 기본값을 변경:

```python
op_kwargs={"folder_pattern": "{{ dag_run.conf.get('folder_pattern', 'manual__top__*') }}"},
```

`manual__*`는 수동 전체 처리용으로 허용 목록에 남긴다.

> 배경: `manual__*` glob은 `manual__bottom__*`도 매치한다. 중앙 upload는 07:40(`SMD_BAEMIN_UPLOAD_TIME`),
> PC2 sweep은 08:00~12:30(`SMD_BAEMIN_UPLOAD_PC2_TIME`)이라 항상 중앙이 먼저 가져간다. 이때 top/bottom의
> `target_date`가 다르면 대표 날짜 로직이 bottom meta를 버려 orders 검증·ToOrder 교차검증·최종 알림에서 누락된다.

### 4. `_validate_upload_folder_pattern` 완화 (top 트리거 ValueError 수정)

`dags/db/DB_Beamin_Macro_Dags.py`의 `trigger_upload_after_export`는 `folder_pattern`에 폴더 **실명**
(`manual__top__<run_id>`)을 넘긴다. 현재 고정 목록 검증이라 ValueError로 ingest가 죽는다.

검증을 아래 규칙으로 완화한다 (`ALLOWED_UPLOAD_FOLDER_PATTERNS`는 그대로 두고 추가 허용):

```python
def _validate_upload_folder_pattern(folder_pattern: str) -> None:
    pattern = str(folder_pattern or "")
    if pattern in ALLOWED_UPLOAD_FOLDER_PATTERNS:
        return
    if (
        pattern.startswith("manual__")
        and "/" not in pattern
        and "\\" not in pattern
        and ".." not in pattern
        and "*" not in pattern
        and "?" not in pattern
    ):
        return
    raise ValueError(f"허용되지 않은 upload inbox 폴더 패턴: {folder_pattern}")
```

`"*"`, `"**"`, 절대경로는 계속 거부되어야 한다(기존 테스트 유지).

### 5. cleanup 견고화 (요구사항의 핵심)

**5-1. rmtree 재시도** — `_cleanup_processed_folder`에서 OneDrive 동기화 잠금 대비:

```python
for attempt in range(3):
    try:
        shutil.rmtree(folder)
        return
    except OSError as exc:
        if attempt == 2:
            raise
        logger.warning("inbox cleanup 재시도(%d/3): %s / %s", attempt + 1, folder, exc)
        time.sleep(2 * (attempt + 1))
```

기존 안전 가드(부모가 inbox인지, 폴더명이 `manual__`로 시작하는지, 실제 디렉터리인지)는 반드시 유지한다.

**5-2. 껍데기 폴더 quarantine** — `ingest_inbox`에서 대상 파일이 0개인 폴더는 현재 `skipped_folders`로만
집계돼 영원히 남고, `has_ingested_folders`가 `folders>0 & cleaned==0`으로 매 run
AirflowException을 던져 **영구히 막힌다**. 아래로 바꾼다:

```python
QUARANTINE_DIR_NAME = "_quarantine"
```

- 대상 파일 0개인 폴더는 `inbox_dir / QUARANTINE_DIR_NAME / folder.name`으로 `shutil.move`
- 이동 실패는 warning 로그만 남기고 진행 (파이프라인을 막지 않는다)
- quarantine 이동 건수를 `stats["quarantined"]`로 집계하고 summary 문자열에도 포함
- `_empty_ingest_stats()`에도 `"quarantined": 0` 추가
- `inbox_dir.glob(folder_pattern)` 결과에서 `_quarantine` 폴더 자체는 제외 (이름이 `manual__`로 시작하지 않아
  현재 패턴에는 안 걸리지만, 방어적으로 `p.name != QUARANTINE_DIR_NAME` 조건 추가)

적재 자체는 멱등(orders upsert, ad_funnel upsert, 나머지는 월 전체 재수집)이므로 cleanup 실패 후
재실행해도 데이터는 안전하다.

### 6. 테스트 추가

`tests/test_beamin_upload_inbox_distribute.py`에 추가:

- `test_ad_funnel_keeps_existing_dates` — 대상 파티션에 07-01·07-02 행이 있는 상태에서 07-22 1행을 적재하면 3행이 남고 07-22만 갱신
- `test_top_pattern_does_not_match_bottom_folder` — `manual__top__*`로 ingest 시 `manual__bottom__*` 폴더가 남아 있음
- `test_exact_folder_name_pattern_allowed` — `manual__top__run1` 같은 실명 허용, `"*"`·`"../x"`는 여전히 ValueError
- `test_parquet_source_stays_parquet` — parquet 소스 shop_change 적재 후 `.parquet` 존재·`.csv` 미생성
- `test_folder_without_target_files_is_quarantined` — 대상 파일 0개 폴더가 `_quarantine/`로 이동하고 stats에 반영

## Reference Code

### modules/transform/pipelines/db/DB_Beamin_pc2_distribute.py (현행 핵심부)

```python
INBOX_DIR = COLLECT_DB / "영업관리부_수집" / "_baemin_pc2_inbox"
UPLOAD_INBOX_DIR = COLLECT_DB / "영업관리부_수집" / "_baemin_upload_inbox"
ORDER_KEY = "주문번호"
SUPPORTED_SUFFIXES = {".parquet", ".csv"}
ROOT_PART = "baemin_macro"
DEFAULT_FOLDER_PATTERN = "manual__*"
BOTTOM_FOLDER_PATTERN = "manual__bottom__*"
ALLOWED_UPLOAD_FOLDER_PATTERNS = frozenset({DEFAULT_FOLDER_PATTERN, BOTTOM_FOLDER_PATTERN})

def _distribute_one_file(folder: Path, src_file: Path) -> tuple[str, int]:
    rel = src_file.relative_to(folder)
    if len(rel.parts) < 3 or rel.parts[0] != ROOT_PART:
        raise ValueError(f"inbox 대상 경로 아님: {src_file}")
    subtype = rel.parts[1]
    dst_stem = (ANALYTICS_DB / rel).with_suffix("")
    new_df = read_file(src_file)
    if new_df.empty:
        raise ValueError(f"inbox 빈 파일: {src_file}")
    if subtype == "orders":
        if ORDER_KEY in new_df.columns:
            combined = _upsert_orders(dst_stem, new_df)
        else:
            combined = new_df
        out_path = write_table(combined, dst_stem)
    else:
        combined = new_df
        out_path = _write_csv_table(combined, dst_stem)
    logger.info("inbox write: %s -> %s | rows=%d", src_file, out_path, len(combined))
    return subtype, len(new_df)

def _cleanup_processed_folder(folder: Path, inbox_dir: Path) -> None:
    folder = folder.resolve()
    inbox = inbox_dir.resolve()
    if folder.parent != inbox:
        raise ValueError(f"inbox cleanup 대상 부모 경로 불일치: {folder}")
    if not folder.name.startswith("manual__"):
        raise ValueError(f"inbox cleanup 대상 폴더명 아님: {folder}")
    if not folder.is_dir():
        raise ValueError(f"inbox cleanup 대상 폴더 없음: {folder}")
    shutil.rmtree(folder)
```

### modules/transform/pipelines/db/beamin_store_io.py (IO 계약)

```python
def read_table(stem_path: Path, columns=None) -> pd.DataFrame | None:
    """확장자 없는 stem을 parquet 우선·CSV 폴백으로 읽는다. 둘 다 없으면 None."""

def read_file(path: Path, columns=None) -> pd.DataFrame:
    """확장자가 있는 실제 파일(parquet/csv)을 문자열 DataFrame으로 읽는다."""

def write_table(df: pd.DataFrame, stem_path: Path) -> Path:
    """문자열 DataFrame을 parquet(zstd)로 저장하고 같은 위치 레거시 CSV는 제거한다."""

def find_tables(base: Path, rel_glob_stem: str) -> list[Path]:
    """parquet·csv 양쪽 glob, 같은 stem이면 parquet 우선."""
```

### modules/transform/pipelines/db/DB_Beamin_05_ad_funnel.py (참고할 upsert 패턴)

```python
def _save_ad_funnel_csv(metrics, brand, store, target_date, *, status="ok") -> Path:
    ym = target_date[:7]
    out_dir = BAEMIN_AD_FUNNEL_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
    out_path = out_dir / "baemin_ad_funnel.csv"
    new_df = pd.DataFrame([row], columns=_COLUMNS)
    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        existing = existing[existing["target_date"] != target_date]   # 같은 날짜 제거 후 append
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
    combined.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path
```

### dags/db/DB_Beamin_Macro_Upload_Dags.py (수정 대상 라인)

```python
    t_ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
        op_kwargs={"folder_pattern": "{{ dag_run.conf.get('folder_pattern', 'manual__*') }}"},
        execution_timeout=timedelta(minutes=60),
    )
```

### tests/test_beamin_upload_inbox_distribute.py (기존 헬퍼 — 재사용할 것)

```python
from modules.transform.pipelines.db import DB_Beamin_pc2_distribute as dist

def _meta(target_date: str, account_id: str) -> dict: ...
def _write_upload_folder(inbox: Path, folder_name: str, target_date: str, account_id: str) -> Path:
    """inbox/<folder_name>/baemin_macro/orders/brand=도리당/store=.../ym=2026-07/orders_2026-07.csv 생성"""
def _write_metric_file(folder: Path) -> Path:
    """.../metrics_our_store_clicks/.../woori_shop_click.csv 생성"""

def test_upload_wrapper_rejects_unapproved_folder_pattern():
    with pytest.raises(ValueError):
        dist.ingest_baemin_upload_inbox(folder_pattern="*")
```

## Test Cases

1. [단위 테스트] `python -m pytest tests/test_beamin_upload_inbox_distribute.py -q` → 기대: 전부 PASS (기존 + 신규 5건)
2. [모듈 import] `python -c "from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import CSV_UPSERT_KEYS, TOP_FOLDER_PATTERN, QUARANTINE_DIR_NAME; print(CSV_UPSERT_KEYS, TOP_FOLDER_PATTERN)"` → 기대: `{'ad_funnel': 'target_date'} manual__top__*`
3. [패턴 검증] `python -c "from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import _validate_upload_folder_pattern as v; v('manual__top__run1'); v('manual__bottom__*'); print('ok')"` → 기대: `ok`
4. [패턴 거부] `python -c "from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import _validate_upload_folder_pattern as v; import sys; [sys.exit('FAIL') for p in ['*','../x','manual__a/b'] if not (lambda: [False for _ in [0]][0])]"` → 대신 pytest 케이스로 검증(위 1번에 포함)
5. [DAG import] `python -c "import ast,sys; ast.parse(open(r'dags/db/DB_Beamin_Macro_Upload_Dags.py',encoding='utf-8').read()); print('ok')"` → 기대: `ok`
6. [DAG 기본 패턴] `python -c "print('manual__top__*' in open(r'dags/db/DB_Beamin_Macro_Upload_Dags.py',encoding='utf-8').read())"` → 기대: `True`
7. [실데이터 dry-run — ad_funnel 이력 보존] 아래 스크립트 실행 → 기대: `merged_dates=10`, `2026-07-22` 포함

```python
import sys; sys.path.insert(0, r"C:\airflow")
import pandas as pd
from pathlib import Path
from modules.transform.pipelines.db.beamin_store_io import read_file, read_table
from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import UPLOAD_INBOX_DIR, _upsert_by_key
f = UPLOAD_INBOX_DIR / "manual__bottom__codex_bottom_two_stores_no_inference__20260723_144446"
rel = Path("baemin_macro/ad_funnel/brand=도리당/store=해운대중동점/ym=2026-07/baemin_ad_funnel.csv")
new = read_file(f / rel)
merged = _upsert_by_key((ANALYTICS_DB / rel).with_suffix(""), new, "target_date")
print("merged_dates=", merged["target_date"].nunique(), sorted(merged["target_date"].unique()))
```

8. [실제 적재] Airflow에서 `DB_Beamin_Macro_Upload_Dags`를 conf `{"folder_pattern": "manual__bottom__*", "skip_if_empty": true}`로 수동 트리거
   → 기대 로그: `upload inbox 적재 완료(pattern=manual__bottom__*) | folders=1 cleaned=1 skipped=0 failed=0 files=12`
9. [적재 결과 확인] `ad_funnel/brand=도리당/store=해운대중동점/ym=2026-07/baemin_ad_funnel.csv` → 10개 날짜 유지,
   `orders/.../store=해운대중동점/ym=2026-07/orders_2026-07.parquet` → 주문번호 450건 유지,
   `shop_change/.../ym=2026-07/` → `.parquet` 유지·`.csv` 미생성
10. [cleanup 확인] inbox에서 `manual__bottom__codex_bottom_two_stores_no_inference__20260723_144446` 폴더 **삭제됨**
11. [최종 알림] Telegram: `대상 계정 2 / 수집 완료 2 / 잔여 실패 0`, orders 검증 2건 일치

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~7 순서대로 실행 (코드 레벨)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
  5. 1~7 전부 PASS면 8~11(실적재) 진행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- **데이터 유실 금지**: ad_funnel은 반드시 기존 `target_date` 행을 보존해야 한다. 실측상 해운대중동점 07월 10일치가 걸려 있다.
- `_cleanup_processed_folder`의 안전 가드(부모 경로 == inbox, 폴더명 `manual__` 접두어, 디렉터리 존재)는 **절대 제거하지 않는다**. rmtree 대상이 넓어지면 OneDrive 원본이 날아간다.
- orders는 기존 `주문번호` upsert 동작을 그대로 유지한다(해운대중동점 450건 중 25건만 교체돼야 함).
- clicks·shop_change·monthly_operation은 수집기가 월 전체를 재수집하므로 **덮어쓰기가 정상**이다. upsert 대상에 넣지 말 것.
- `dags/db/DB_Beamin_Macro_Upload_Pc2_Dags.py`는 수정하지 않는다.
- `DEFAULT_FOLDER_PATTERN = "manual__*"`는 수동 전체 처리용으로 허용 목록에 남긴다(제거 금지).
- inbox에 남아있는 `manual__manual_songpa_woori_only_20260716_1007`(07-16)은 이번 변경 후 어떤 스케줄 패턴에도
  안 걸린다. 코드로 처리하지 말고, 작업 완료 후 `{"folder_pattern": "manual__*"}` conf로 1회 수동 트리거하거나 직접 삭제한다.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (`from __future__ import annotations` 사용 중)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- `_upsert_orders`를 남길지 `_upsert_by_key`로 통합할지: 통합하되 기존 호출부 시그니처는 깨지 않게
