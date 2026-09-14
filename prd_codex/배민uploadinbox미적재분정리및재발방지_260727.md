# 배민 upload inbox 미적재분 정리 및 재발 방지

## Task

`_baemin_upload_inbox`에 2컴(bottom) 산출물이 적재되지 않고 쌓이는 문제를 해결한다. 원인은 두 가지다. (1) `_tmp__manual__*` 폴더는 export가 중단된 잔해라 glob 패턴(`manual__*`)에 매칭되지 않고 영원히 남는다 — 회수 로직이 없다. (2) 정상 완료된 `manual__bottom__*` 폴더가 아무도 적재하지 않은 채 대기해도 아무 알림이 없다. stale tmp 자동 quarantine과 미적재 대기 감지 알림을 추가한다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify

**Modify**
- `modules/transform/pipelines/db/DB_Beamin_pc2_distribute.py` — stale `_tmp__` 폴더 quarantine sweep 추가
- `modules/transform/pipelines/db/DB_Beamin_Macro_upload.py` — `has_upload_inbox_folders`에 미적재 체류 경고 추가

**Create**
- `tests/test_beamin_stale_tmp_sweep.py` — sweep 동작 테스트

**운영 조작 (코드 변경 아님, 사람이 수행)**
- `DB_Beamin_Macro_Upload_Dags`를 conf `{"folder_pattern": "manual__bottom__*", "skip_if_empty": true}` 로 수동 트리거 → 대기 중인 `manual__bottom__scheduled__2026-07-25T18_15_00_00_00`(target_date=2026-07-26, 150 파일) 적재
- `DB_Beamin_Macro_Upload_Pc2_Dags` unpause 상태 확인 (`is_paused_upon_creation=True`)

## Implementation Steps

### 1. `DB_Beamin_pc2_distribute.py` — stale tmp sweep 상수 추가

기존 상수 블록(`QUARANTINE_DIR_NAME = "_quarantine"` 근처)에 추가:

```python
TMP_FOLDER_PREFIX = "_tmp__manual__"
STALE_TMP_MAX_AGE_HOURS = 24
```

### 2. `_sweep_stale_tmp_folders` helper 신규 작성

`_quarantine_empty_folder` 바로 아래에 배치. 삭제가 아니라 기존 `_quarantine` 디렉터리로 이동한다.

```python
def _sweep_stale_tmp_folders(
    inbox_dir: Path,
    max_age_hours: float = STALE_TMP_MAX_AGE_HOURS,
) -> int:
    """중단된 export 잔해(_tmp__manual__*)를 quarantine으로 회수한다.

    export_staging_to_inbox는 복사 완료 후에만 _tmp__ 접두어를 떼므로,
    임계 시간이 지나도 _tmp__로 남아 있으면 실패한 export로 간주한다.
    """
    if not inbox_dir.exists():
        return 0
    cutoff = time.time() - max_age_hours * 3600
    moved = 0
    for folder in sorted(inbox_dir.glob(f"{TMP_FOLDER_PREFIX}*")):
        if not folder.is_dir():
            continue
        try:
            if folder.stat().st_mtime > cutoff:
                continue
        except OSError as exc:
            logger.warning("stale tmp mtime 확인 실패: %s / %s", folder, exc)
            continue
        if _quarantine_empty_folder(folder, inbox_dir):
            moved += 1
            logger.warning(
                "중단된 export 잔해 quarantine: %s (age>%sh)", folder.name, max_age_hours
            )
    return moved
```

`_quarantine_empty_folder`를 재사용하므로 이동 실패 시 예외를 던지지 않고 WARNING 후 건너뛴다(기존 동작 유지).

### 3. `ingest_inbox`에서 sweep 호출

`inbox_dir.exists()` 가드 통과 직후, `folders = sorted(...)` glob **직전**에 삽입:

```python
    stale_tmp = _sweep_stale_tmp_folders(inbox_dir)

    folders = sorted(
        (
            p
            for p in inbox_dir.glob(folder_pattern)
            if p.is_dir() and p.name != QUARANTINE_DIR_NAME
        ),
        key=_folder_sort_key,
    )
```

주의: `folder_pattern`은 절대 `_tmp__*`를 매칭하지 않으므로 sweep이 정상 대상 폴더를 잡아먹을 위험은 없다.

### 4. `stale_tmp`를 stats에 반영

- `_empty_ingest_stats()`의 dict에 `"stale_tmp": 0` 키 추가
- `ingest_inbox`의 조기 반환 2곳(inbox 없음 / 대상 없음)은 `_empty_ingest_stats()`를 쓰지만, **"대상 없음" 반환 경로는 sweep 이후이므로** 해당 반환의 stats에 `stale_tmp` 실제값을 반영해야 한다:

```python
    if not folders:
        logger.info("%s(pattern=%s) 처리 대상 없음: %s", label, folder_pattern, inbox_dir)
        stats = _empty_ingest_stats()
        stats["stale_tmp"] = stale_tmp
        return {
            "summary": f"{label} 처리 대상 없음: 스킵",
            "meta": _empty_meta() if read_meta else {},
            "stats": stats,
        }
```

- 최종 반환 stats dict에도 `"stale_tmp": stale_tmp` 추가
- `summary` 문자열에 `stale_tmp={stale_tmp}` 를 `quarantined=...` 뒤에 이어붙인다

`has_ingested_folders`(`DB_Beamin_Macro_upload.py`)는 `folders`/`cleaned` 키만 읽으므로 키 추가로 인한 downstream 파손은 없다.

### 5. `has_upload_inbox_folders` — 체류 경고 추가

`modules/transform/pipelines/db/DB_Beamin_Macro_upload.py`의 기존 함수를 확장한다. 현재는 개수만 세고 ShortCircuit이 조용히 False로 끝나므로, bottom 폴더가 오래 남아 있어도 아무도 모른다.

```python
UPLOAD_INBOX_STALE_HOURS = 12


def has_upload_inbox_folders(
    folder_pattern: str = BOTTOM_FOLDER_PATTERN,
    **context,
) -> bool:
    """지정 패턴의 완성된 upload inbox 폴더가 있는지 확인한다."""
    count = count_baemin_upload_inbox_folders(folder_pattern)
    logger.info("upload inbox 감지: pattern=%s folders=%d", folder_pattern, count)
    if count:
        _warn_if_upload_inbox_stale(folder_pattern)
    return bool(count)


def _warn_if_upload_inbox_stale(folder_pattern: str) -> None:
    """완성 폴더가 임계 시간 넘게 적재되지 않고 남아 있으면 경고한다."""
    from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import (
        QUARANTINE_DIR_NAME,
        UPLOAD_INBOX_DIR,
    )

    cutoff = time.time() - UPLOAD_INBOX_STALE_HOURS * 3600
    stale = [
        path.name
        for path in UPLOAD_INBOX_DIR.glob(folder_pattern)
        if path.is_dir() and path.name != QUARANTINE_DIR_NAME and path.stat().st_mtime < cutoff
    ]
    if not stale:
        return
    body = (
        f"[배민 upload inbox 적체] pattern={folder_pattern} "
        f"{len(stale)}개 폴더가 {UPLOAD_INBOX_STALE_HOURS}시간 넘게 미적재\n"
        + "\n".join(f"- {name}" for name in sorted(stale)[:10])
    )
    logger.warning(body)
    try:
        send_telegram(body)
    except Exception as exc:
        logger.warning("적체 알림 실패(무시): %s", exc)
```

`import time`을 파일 상단에 추가한다. `send_telegram`은 이미 import되어 있다.

### 6. `_tmp__` 잔해 수동 정리

sweep은 다음 DAG run부터 동작하므로, 현재 남은 4개는 즉시 수동 정리해도 되고 sweep에 맡겨도 된다(전부 24h 초과라 다음 run에서 자동 quarantine된다). analytics에 07-23(6,592건)/07-24(5,086건)이 이미 정상 적재돼 있으므로 **재적재하지 말 것.**

```
_tmp__manual__bottom__retry__20260723__attempt_2__...   (47 files)
_tmp__manual__bottom__retry__20260723__attempt_4__...   (1 file)
_tmp__manual__bottom__retry__20260723__attempt_5__...   (1 file)
_tmp__manual__bottom__scheduled__2026-07-23T18_15...    (243 files)
```

## Reference Code

### modules/transform/pipelines/db/beamin_staging.py (export — `_tmp__` 규약의 출처)

```python
def export_staging_to_inbox(
    local_baemin: Path,
    run_id: str | None,
    inbox_dir: Path,
    meta: dict | None = None,
    *,
    replace_existing: bool = False,
    folder_prefix: str = "",
) -> Path:
    if not local_baemin.exists():
        raise RuntimeError(f"staging 경로 없음: {local_baemin}")

    run_part = safe_run_id_part(run_id)
    prefix = safe_run_id_part(folder_prefix) if folder_prefix else ""
    name = f"manual__{prefix}__{run_part}" if prefix else f"manual__{run_part}"
    inbox_run = inbox_dir / name
    tmp_run = inbox_run.with_name(f"_tmp__{inbox_run.name}")
    if tmp_run.exists():
        shutil.rmtree(tmp_run)
    if inbox_run.exists():
        if not replace_existing:
            raise FileExistsError(f"inbox run 폴더가 이미 존재함: {inbox_run}")
        shutil.rmtree(inbox_run)

    dst_root = tmp_run / "baemin_macro"
    file_count = 0
    for src in local_baemin.rglob("*"):
        if not src.is_file():
            continue
        rel = src.relative_to(local_baemin)
        dst = dst_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        file_count += 1

    tmp_run.mkdir(parents=True, exist_ok=True)
    if meta is not None:
        (tmp_run / "_meta.json").write_text(
            json.dumps(sanitize_export_meta(meta), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    tmp_run.rename(inbox_run)
    logger.info("배민 inbox export 완료: %s (%d files)", inbox_run, file_count)
    return inbox_run
```

### modules/transform/pipelines/db/DB_Beamin_pc2_distribute.py (수정 대상 — 상수 / quarantine 패턴)

```python
import json
import logging
import re
import shutil
import time
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from modules.transform.pipelines.db.beamin_store_io import read_file, read_table, write_table
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB, LOCAL_DB

logger = logging.getLogger(__name__)

INBOX_DIR = COLLECT_DB / "영업관리부_수집" / "_baemin_pc2_inbox"
UPLOAD_INBOX_DIR = COLLECT_DB / "영업관리부_수집" / "_baemin_upload_inbox"
DEFAULT_FOLDER_PATTERN = "manual__*"
TOP_FOLDER_PATTERN = "manual__top__*"
BOTTOM_FOLDER_PATTERN = "manual__bottom__*"
ALLOWED_UPLOAD_FOLDER_PATTERNS = frozenset(
    {DEFAULT_FOLDER_PATTERN, TOP_FOLDER_PATTERN, BOTTOM_FOLDER_PATTERN}
)
QUARANTINE_DIR_NAME = "_quarantine"


def _quarantine_empty_folder(folder: Path, inbox_dir: Path) -> bool:
    quarantine_dir = inbox_dir / QUARANTINE_DIR_NAME
    destination = quarantine_dir / folder.name
    try:
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            raise FileExistsError(f"quarantine 대상이 이미 존재함: {destination}")
        shutil.move(str(folder), str(destination))
    except OSError as exc:
        logger.warning("빈 inbox 폴더 quarantine 실패: %s / %s", folder, exc)
        return False
    logger.info("빈 inbox 폴더 quarantine 완료: %s -> %s", folder, destination)
    return True
```

### modules/transform/pipelines/db/DB_Beamin_Macro_upload.py (수정 대상 함수)

```python
def has_upload_inbox_folders(
    folder_pattern: str = BOTTOM_FOLDER_PATTERN,
    **context,
) -> bool:
    """지정 패턴의 완성된 upload inbox 폴더가 있는지 확인한다."""
    count = count_baemin_upload_inbox_folders(folder_pattern)
    logger.info("upload inbox 감지: pattern=%s folders=%d", folder_pattern, count)
    return bool(count)


def has_ingested_folders(**context) -> bool:
    """무대상 PC2 run만 skip하고 전부 실패한 적재는 실패로 노출한다."""
    stats = context["ti"].xcom_pull(task_ids="ingest", key="ingest_stats") or {}
    folders = int(stats.get("folders") or 0)
    cleaned = int(stats.get("cleaned") or 0)
    ...
```

### tests/test_beamin_upload_inbox_distribute.py (기존 테스트 패턴 — 깨뜨리지 말 것)

```python
def test_ingest_inbox_filters_by_folder_pattern(monkeypatch, tmp_path):
    ...
        folder_pattern=dist.BOTTOM_FOLDER_PATTERN,
    ...

def test_upload_wrapper_rejects_unapproved_folder_pattern():
    with pytest.raises(ValueError):
        dist.ingest_baemin_upload_inbox(folder_pattern="*")


def test_count_baemin_upload_inbox_folders(...):
    assert dist.count_baemin_upload_inbox_folders(dist.BOTTOM_FOLDER_PATTERN) == 0
    ...
    assert dist.count_baemin_upload_inbox_folders(dist.BOTTOM_FOLDER_PATTERN) == 1
```

## Test Cases

1. **기존 테스트 무회귀**
   `python -m pytest tests/test_beamin_upload_inbox_distribute.py tests/test_baemin_pc2_upload_trigger.py -q`
   → 기대: 전부 PASS, 실패 0

2. **모듈 import**
   `python -c "from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import _sweep_stale_tmp_folders, TMP_FOLDER_PREFIX, STALE_TMP_MAX_AGE_HOURS; print('ok')"`
   → 기대: `ok`

3. **stale tmp만 quarantine 되는지** (`tests/test_beamin_stale_tmp_sweep.py` 신규 작성)
   - tmp_path에 `_tmp__manual__bottom__old`(mtime 48h 전), `_tmp__manual__bottom__new`(방금), `manual__bottom__keep`(방금) 3개 생성
   - `_sweep_stale_tmp_folders(tmp_path)` 호출
   - 기대: 반환값 `1` / `_quarantine/_tmp__manual__bottom__old` 존재 / `_tmp__manual__bottom__new` 원위치 유지 / `manual__bottom__keep` 원위치 유지
   `python -m pytest tests/test_beamin_stale_tmp_sweep.py -q` → 전부 PASS

4. **stats에 stale_tmp 키 존재**
   `python -c "from modules.transform.pipelines.db.DB_Beamin_pc2_distribute import _empty_ingest_stats; assert 'stale_tmp' in _empty_ingest_stats(); print('ok')"`
   → 기대: `ok`

5. **DAG import 무결성**
   `python -c "from dags.db.DB_Beamin_Macro_Upload_Dags import dag; from dags.db.DB_Beamin_Macro_Upload_Pc2_Dags import dag as d2; print(dag.dag_id, d2.dag_id)"`
   → 기대: `DB_Beamin_Macro_Upload_Dags DB_Beamin_Macro_Upload_Pc2_Dags`, ImportError 없음

6. **적체 경고 함수 동작** (`send_telegram`을 monkeypatch로 캡처)
   - `UPLOAD_INBOX_DIR`를 tmp_path로 monkeypatch, 24h 전 mtime의 `manual__bottom__x` 생성
   - `_warn_if_upload_inbox_stale("manual__bottom__*")` 호출 → 기대: 캡처된 메시지에 `적체` 및 폴더명 포함
   - 방금 만든 폴더만 있을 때 → 기대: 알림 미발송

7. **실물 확인 (운영 조작 후, 사람이 수행)**
   - `_baemin_upload_inbox`에 `manual__*` / `_tmp__*` 잔여 없음
   - analytics 07-26 주문 정상 회복:
     `python -c "import glob,pandas as pd; f=glob.glob(r'C:\Users\민준\OneDrive - 주식회사 도리당\data\analytics\baemin_macro\orders\**\ym=2026-07\*.parquet',recursive=True); d=pd.concat([pd.read_parquet(x) for x in f]); print(d['주문시각'].astype(str).str[:12].value_counts().sort_index().tail(5))"`
     → 기대: `2026. 07. 26` 이 4,000건 이상 (현재 2,010건)

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~6 순서대로 실행 (7번은 운영 조작 후 사람이 확인)
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 1~6 전체 PASS + Constraints 위반 없음
```

## Constraints

- **`_tmp__` 폴더의 내용을 재적재하지 말 것.** 07-23/24는 analytics에 이미 완전 적재됨. 부분 복사본 투입 시 데이터 오염.
- **`export_staging_to_inbox`의 rename 방식을 변경하지 말 것.** atomic 보장이 목적이고 정상 동작 중이다.
- sweep은 **삭제가 아니라 quarantine 이동**이어야 한다. `_quarantine_empty_folder`를 재사용할 것.
- `_validate_upload_folder_pattern`의 허용 패턴 집합을 넓히지 말 것 (경로 탈출 방어용).
- `_cleanup_processed_folder`의 `startswith("manual__")` 가드를 완화하지 말 것.
- sweep은 `folder_pattern` glob과 완전히 독립이어야 한다 — 정상 `manual__*` 폴더를 절대 건드리지 않는다.
- `ingest_stats` 딕셔너리는 XCom으로 넘어간다. 키를 추가만 하고 기존 키(`folders`, `cleaned`, `skipped`, `quarantined`, `failed`, `files`, `acl_verified_files`, `rows`, `subtypes`)를 제거·개명하지 말 것.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게 (전부 `from __future__ import annotations` + 힌트 있음)
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, private helper는 `_` 접두어
- 로그 메시지 언어: 한국어 (기존 파일과 동일)
