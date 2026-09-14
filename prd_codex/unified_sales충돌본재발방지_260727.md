# unified_sales OneDrive 충돌본(-JSFNE-K) 재발 방지

## Task

`MART_DB/unified_sales_grp/` 에 `unified_sales_260720-JSFNE-K.parquet`, `unified_sales_260721-JSFNE-K.parquet`, `unified_sales_260722-JSFNE-K.parquet` 3개가 생성됐다. `-JSFNE-K` 는 OneDrive 충돌본(conflict copy) 접미사로, 이 폴더를 함께 동기화하는 다른 PC(PC2, 컴퓨터명 `JSFNE-K`)와 중앙 PC가 같은 파일을 동시에 들고 있을 때 OneDrive가 자동 생성한다.

가장 큰 문제는 `iter_unified_sales_files()` 가 `glob("unified_sales_*.parquet")` 에서 `.bak_` 만 제외하기 때문에 이 충돌본이 **그대로 집계에 매치되어 해당 3일치 매출이 이중 집계**된다는 점이다. 이 함수는 `build_daily_summary`, `hall_viz`, `DB_OrderCrossAnalysis`, `DB_Hall_Daily_Excel`, `DB_Hall_Sales_Target`, `DB_FinProduct_Map`, `validate` 등 20+ 지점의 단일 입력 경로다.

목표: (1) 비정규 파일명이 집계에 절대 섞이지 않게 하고, (2) 원자적 쓰기로 충돌 생성 자체를 막고, (3) 그래도 생기면 격리 + 텔레그램 알림.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- DB_UnifiedSales 공통 스키마/저장 로직은 반드시 `DB_UnifiedSales_common.py` 에 둔다
- 파이프라인 함수는 문자열 XCom 메시지를 반환한다

## Files to Create / Modify

**수정:**
- `modules/transform/pipelines/db/DB_UnifiedSales_common.py` — 화이트리스트 정규식, `save_unified_parquet()`, `quarantine_conflict_copies()` 추가 + 기존 `to_parquet` 교체
- `modules/transform/pipelines/db/DB_UnifiedSales_baemin.py` — `to_parquet` 교체 (316, 724, 740)
- `modules/transform/pipelines/db/DB_UnifiedSales_coupang.py` — `to_parquet` 교체 (473)
- `modules/transform/pipelines/db/DB_UnifiedSales_hall_viz.py` — `to_parquet` 교체 (120, 191)
- `modules/transform/pipelines/db/DB_UnifiedSales_posfeed.py` — `to_parquet` 교체 (783)
- `modules/transform/pipelines/db/DB_UnifiedSales_validate.py` — `to_parquet` 교체 (1207, `daily_summary.parquet`)
- `modules/transform/pipelines/db/DB_CollectionCompare.py` — 자체 glob(101) → `iter_unified_sales_files()` 사용
- `modules/transform/pipelines/sales/SMD_store_sales_daily_actuals.py` — 자체 glob(120) → `iter_unified_sales_files()` 사용
- `dags/db/DB_UnifiedSales_Dags.py` — `quarantine_conflicts` 태스크 추가
- `harness/baemin_macro.md` — PC2 운영 규칙 1줄 추가

**생성 없음** (신규 파일 만들지 않는다)

## Implementation Steps

### 1. 정규 파일명 화이트리스트 (최우선 — 이중 집계 차단)

`DB_UnifiedSales_common.py` 상단(`UNIFIED_ROOT` 정의 근처)에 추가:

```python
import re

UNIFIED_DAILY_RE = re.compile(r"^unified_sales_\d{6}\.parquet$")


def is_canonical_unified_file(path) -> bool:
    """정규 일별 파일명(unified_sales_YYMMDD.parquet)만 True."""
    return bool(UNIFIED_DAILY_RE.match(path.name))
```

기존 `iter_unified_sales_files()` 를 `.bak_` 문자열 제외 방식에서 화이트리스트 방식으로 교체한다:

```python
def iter_unified_sales_files() -> list:
    """정규 unified_sales 일별 parquet만 반환한다.

    - 백필 백업(`unified_sales_YYMMDD.bak_*.parquet`)
    - OneDrive 충돌본(`unified_sales_YYMMDD-<PCNAME>.parquet`)
    - 원자적 쓰기 임시 파일(`*.parquet.tmp`)
    이 모두 단순 glob에 걸리므로 화이트리스트로 걸러낸다.
    """
    if not UNIFIED_ROOT.exists():
        return []
    return sorted(
        path
        for path in UNIFIED_ROOT.glob("unified_sales_*.parquet")
        if is_canonical_unified_file(path)
    )
```

### 2. 자체 glob 쓰는 outlier 2곳 정리

`DB_CollectionCompare.py:101` 과 `SMD_store_sales_daily_actuals.py:120` 의
`root.glob("unified_sales_*.parquet") if ".bak_" not in path.name` 패턴을
`from modules.transform.pipelines.db.DB_UnifiedSales_common import iter_unified_sales_files` 로 대체한다.
(두 파일 모두 대상 root가 `MART_DB / "unified_sales_grp"` 로 동일한지 확인 후 교체. 다르면 화이트리스트 필터만 적용.)

나머지 소비자(hall_viz, OrderCrossAnalysis, Hall_Daily_Excel, Hall_Sales_Target, FinProduct_Map, posfeed, validate, baemin, coupang)는 이미 `iter_unified_sales_files()` 를 쓰므로 자동 보호된다.

### 3. 원자적 쓰기 헬퍼 추가

`DB_UnifiedSales_common.py` 에 추가:

```python
def save_unified_parquet(df: pd.DataFrame, path) -> None:
    """같은 폴더에 .tmp로 쓴 뒤 os.replace로 원자 교체.

    OneDrive 동기화 폴더에 직접 to_parquet 하면 쓰기 중간 상태를 OneDrive가
    스냅샷해 충돌본(`<name>-<PCNAME>.parquet`)을 만든다. tmp → replace 로
    최종 경로가 항상 완성본만 갖도록 한다.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_parquet(tmp, index=False, engine="pyarrow")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
```

`os` import가 없으면 추가한다. `.tmp` 는 1번 화이트리스트에 걸려 집계에 들어가지 않는다.

### 4. 모든 unified/mart parquet 쓰기를 헬퍼로 교체

패턴은 전부 동일: `X.to_parquet(P, index=False, engine="pyarrow")` → `save_unified_parquet(X, P)`

| 파일 | 라인 |
|---|---|
| `DB_UnifiedSales_common.py` | 953, 1293, 1338, 1391, 1431 |
| `DB_UnifiedSales_baemin.py` | 316, 724, 740 |
| `DB_UnifiedSales_coupang.py` | 473 |
| `DB_UnifiedSales_hall_viz.py` | 120, 191 |
| `DB_UnifiedSales_posfeed.py` | 783 |
| `DB_UnifiedSales_validate.py` | 1207 |

`coupang.py:473` 은 `df.reindex(...).to_parquet(path, ...)` 형태이므로 중간 결과를 변수로 받아 넘긴다.
각 파일에서 `from modules.transform.pipelines.db.DB_UnifiedSales_common import save_unified_parquet` 를 기존 import 블록에 추가한다.

### 5. 충돌본 격리 + 텔레그램 알림

`DB_UnifiedSales_common.py` 에 추가:

```python
CONFLICT_QUARANTINE_DIR = UNIFIED_ROOT / "_conflicts"


def quarantine_conflict_copies() -> str:
    """비정규 unified_sales_* 파일을 _conflicts/<YYYYMMDD>/ 로 격리하고 알린다.

    OneDrive 충돌본(`unified_sales_260722-JSFNE-K.parquet`)이 집계에 섞이면
    해당 일자가 이중 집계된다. 자동 병합/삭제는 하지 않고 격리 + 알림만 한다.
    """
```

동작:
1. `UNIFIED_ROOT.glob("unified_sales_*")` 순회
2. `is_canonical_unified_file(path)` 가 False이고 `.bak_` 아니고 `.tmp` 로 끝나지 않는 파일만 대상
3. `CONFLICT_QUARANTINE_DIR / pendulum.now("Asia/Seoul").strftime("%Y%m%d")` 로 `shutil.move`
4. 1건 이상이면 `send_telegram()` 으로 건수 + 파일명 목록 통지 — 알림 실패는 무시(try/except, logger.warning)
5. 반환: `"충돌본 없음"` 또는 `f"OK: 충돌본 격리 {n}건 | {names}"`

import: `from modules.transform.utility.notifier import send_telegram` (모듈 최상단이 아니라 함수 내부 import로 순환참조 회피 — 기존 파일 패턴 확인 후 결정)

### 6. DAG 태스크 배선

`dags/db/DB_UnifiedSales_Dags.py`:

```python
def quarantine_conflicts(**context) -> str:
    """OneDrive 충돌본을 집계 전에 격리한다."""
    return pipeline_quarantine_conflict_copies()
```

Operator 추가:

```python
t_quarantine = PythonOperator(
    task_id="quarantine_conflicts",
    python_callable=quarantine_conflicts,
)
```

체인 삽입 위치 — `resolve_date`(t1) 직후, `ingest_baemin_pc2_inbox`(t_ingest_pc2) 직전:

```
t_ingest_manual_baemin >> t_cleanup_manual_baemin >> t1 >> t_quarantine >> t_ingest_pc2 >> t3 >> ...
```

기존 직렬 체인(`# 순차 실행: 같은 날짜 parquet에 동시 write 방지`) 형식과 순서 규칙을 유지한다.

### 7. repartition 경고 추가 (쓰기 증폭 축소)

`DB_UnifiedSales_common.py:1110` `repartition_unified_sales_by_sale_date()` 는 전 이력을 `overwrite=True` 로 재기록해 충돌 유발 위험이 가장 크다. DAG 태스크가 아닌 **수동 복구 전용**임을 docstring에 명시하고, 함수 진입 직후 아래 로그를 남긴다:

```python
logger.warning(
    "repartition: unified_sales 전체 재기록 시작 — OneDrive 동기화 중이면 충돌본 발생 위험"
)
```

`refresh_store_meta_in_unified_sales` / `enforce_manual_delivery_sources_for_test_stores` /
`purge_manual_delivery_sources_for_non_test_stores` 는 이미 no-op 스킵 가드
(`if removed <= 0: continue`, `if before.equals(after): continue`)가 있으므로 **로직을 변경하지 않는다.**

### 8. 운영 규칙 문서화

`harness/baemin_macro.md` 에 한 줄 추가:

> PC2(`JSFNE-K`)는 OneDrive 선택적 동기화에서 `data/mart/` 를 **제외**한다. PC2에 필요한 건 `Collect_Data/영업관리부_수집/_baemin_pc2_inbox/` 뿐이며(설계상 mart는 중앙 PC 전용), mart를 동기화하면 `unified_sales_YYMMDD-JSFNE-K.parquet` 충돌본이 생겨 매출이 이중 집계된다.

## Reference Code

### modules/transform/utility/airflow_api.py (원자적 쓰기 기존 패턴)

```python
    _HEAL_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _HEAL_QUEUE_PATH.with_suffix(_HEAL_QUEUE_PATH.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        for kind, row in rows:
            if kind == "json":
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            else:
                f.write(row + "\n")
    tmp_path.replace(_HEAL_QUEUE_PATH)
    return True
```

### modules/transform/utility/notifier.py (텔레그램 알림)

```python
def send_telegram(text: str) -> bool:
    token, chat_id = _get_telegram_creds()
    if not token or not chat_id:
        logger.warning("Telegram credentials missing; skip send")
        return False
    try:
        payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=10):
            pass
        logger.info("Telegram alert sent")
        return True
    except Exception as e:
        logger.warning("Telegram send failed (ignored): %s", e)
        return False
```

### modules/transform/pipelines/db/DB_UnifiedSales_common.py (수정 전 현재 상태)

```python
UNIFIED_ROOT = MART_DB / "unified_sales_grp"


def iter_unified_sales_files() -> list:
    """실제 unified_sales parquet만 반환한다.

    백필 백업 파일은 `unified_sales_YYMMDD.bak_*.parquet` 형태라 단순 glob에
    같이 잡힌다. 운영 집계/검증/정리에서는 반드시 제외해야 한다.
    """
    if not UNIFIED_ROOT.exists():
        return []
    return sorted(
        path
        for path in UNIFIED_ROOT.glob("unified_sales_*.parquet")
        if ".bak_" not in path.name
    )


def _unified_daily_path(date_str: str):
    ymd = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")
    return UNIFIED_ROOT / f"unified_sales_{ymd}.parquet"
```

## Test Cases

1. [화이트리스트] `python -c "from pathlib import Path; from modules.transform.pipelines.db.DB_UnifiedSales_common import is_canonical_unified_file as f; print([f(Path(n)) for n in ['unified_sales_260722.parquet','unified_sales_260722-JSFNE-K.parquet','unified_sales_260722.bak_1.parquet','unified_sales_260722.parquet.tmp','daily_summary.parquet']])"`
   → 기대: `[True, False, False, False, False]`

2. [기존 파일 인식] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_common import iter_unified_sales_files; fs=iter_unified_sales_files(); print(len(fs), all(p.name.startswith('unified_sales_') for p in fs))"`
   → 기대: 375 내외 (변경 전 개수와 동일), `True`

3. [격리 동작] `UNIFIED_ROOT` 에 더미 `unified_sales_260722-JSFNE-K.parquet` 를 만든 뒤
   `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_common import quarantine_conflict_copies; print(quarantine_conflict_copies())"`
   → 기대: `OK: 충돌본 격리 1건 | ...` 출력 + `_conflicts/<YYYYMMDD>/` 로 이동 + 텔레그램 수신

4. [집계 불변] 3번의 더미 충돌본이 **있는 상태에서** `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_validate import build_daily_summary; print(build_daily_summary())"` 실행 후 `daily_summary.parquet` 의 2026-07-22 `total_price` 합계를 기록 → 충돌본 제거 후 재실행 → 합계가 **동일**해야 함 (이중 집계 차단 증명)

5. [원자적 쓰기] `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_okpos import run_lookback_okpos; print(run_lookback_okpos(days=3))"` 실행 후
   `python -c "from modules.transform.pipelines.db.DB_UnifiedSales_common import UNIFIED_ROOT; print(list(UNIFIED_ROOT.glob('*.tmp')))"`
   → 기대: `[]` (잔여 tmp 없음) + 대상 parquet 정상 read

6. [DAG import] `python -c "from dags.db.DB_UnifiedSales_Dags import dag"` → 기대: ImportError 없음

7. [소비자 import] `python -c "import modules.transform.pipelines.db.DB_CollectionCompare, modules.transform.pipelines.sales.SMD_store_sales_daily_actuals, modules.transform.pipelines.db.DB_Hall_Daily_Excel, modules.transform.pipelines.db.DB_OrderCrossAnalysis"` → 기대: 에러 없음

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~7 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- **파일 신규 생성 금지** — 모든 변경은 기존 파일 수정으로 처리한다.
- **충돌본 자동 병합/삭제 금지** — 격리 + 알림만. `_conflicts/` 안의 파일은 사람이 판단한다.
- **mart 경로 변경 금지** — `MART_DB` 는 OneDrive 아래 그대로 둔다. 원자적 쓰기로만 해결한다.
- `refresh_store_meta_in_unified_sales` / `enforce_manual_delivery_sources_for_test_stores` / `purge_manual_delivery_sources_for_non_test_stores` 의 no-op 스킵 가드 **로직 변경 금지** (이미 정상 동작 중).
- `DB_UnifiedSales` DAG의 **스케줄(`DB_UNIFIED_SALES_TIME`) 및 기존 태스크 순서 변경 금지**. `quarantine_conflicts` 만 `resolve_date` 뒤에 삽입한다.
- `.tmp` 접미사는 `is_canonical_unified_file()` 에서 반드시 False여야 한다 (중간 파일이 집계에 섞이면 안 됨).
- `save_unified_parquet` 의 `finally` 블록에서 tmp 정리 시 예외를 삼키지 말고 `unlink(missing_ok=True)` 만 쓴다.
- print 금지, `logger` 사용.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- `send_telegram` import 위치(모듈 최상단 vs 함수 내부): 순환참조 나면 함수 내부로
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
