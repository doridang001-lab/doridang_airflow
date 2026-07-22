# DB_FinProduct_Map_Dags 복원

## Task
`송파삼전점` 대상 `fin_product_map`을 갱신하던 DAG와 파이프라인 모듈이 프로젝트에서 사라졌다(git 커밋 전 소실, 이력 없음). 백업 번들이 `E:\d_down\DB_FinProduct_Map_Dags_connected_20260626\` 에 남아 있다. 이 번들에서 코드 파일 2개를 복원하고, 현재 `paths.py`/`schedule.py`에 빠진 상수를 보강해 DAG가 정상 import·실행되고 출력 파일 3개(`fin_product_map.csv`, `fin_product_map_review.csv`, `fin_product_map_train.json`)가 mart에 저장되게 한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
**복원(번들에서 그대로 복사):**
- `C:\airflow\dags\db\DB_FinProduct_Map_Dags.py` ← `E:\d_down\DB_FinProduct_Map_Dags_connected_20260626\dags\db\DB_FinProduct_Map_Dags.py`
- `C:\airflow\modules\transform\pipelines\db\DB_FinProduct_Map.py` ← `E:\d_down\DB_FinProduct_Map_Dags_connected_20260626\modules\transform\pipelines\db\DB_FinProduct_Map.py`

**수정(상수만 추가, 통째 덮어쓰기 금지):**
- `C:\airflow\modules\transform\utility\paths.py`
- `C:\airflow\modules\transform\utility\schedule.py`

**손대지 않음:** `notifier.py`(on_failure_callback, send_telegram 이미 존재), `mailer.py`. 번들의 `paths.py`/`schedule.py`는 현재 프로젝트와 구조가 달라(lazy `_get` dict 방식) 복사 금지.

## Implementation Steps

1. **DAG 파일 복원** — 번들의 `dags\db\DB_FinProduct_Map_Dags.py`를 `C:\airflow\dags\db\DB_FinProduct_Map_Dags.py`로 **내용 변경 없이 그대로** 복사. 태스크 흐름: `migrate_product_map >> build_fin_product_map_train_json >> llm_product_map`. conf 옵션 `dry_run`(기본 False), `limit`(dry_run 시 20) 지원.

2. **파이프라인 모듈 복원** — 번들의 `modules\transform\pipelines\db\DB_FinProduct_Map.py`를 `C:\airflow\modules\transform\pipelines\db\DB_FinProduct_Map.py`로 **그대로** 복사. `TARGET_STORES = ["송파삼전점"]` 고정. 공개 함수: `migrate_product_map`, `build_fin_product_map_train_json`, `llm_product_map`.

3. **paths.py 상수 추가** — 현재 프로젝트의 직접대입 스타일로, `POSFEED_WHITELIST_CSV_PATH = MART_DB / "fin_product" / "fin_product_posfeed_whitelist.csv"` 줄 바로 뒤에 추가:
   ```python
   FIN_PRODUCT_MAP_CSV_PATH = MART_DB / "fin_product" / "fin_product_map.csv"
   FIN_PRODUCT_MAP_REVIEW_CSV_PATH = MART_DB / "fin_product" / "fin_product_map_review.csv"
   FIN_PRODUCT_MAP_TRAIN_JSON_PATH = MART_DB / "fin_product" / "fin_product_map_train.json"
   ```
   → 이 경로가 mart에 이미 존재하는 출력 파일 3개와 정확히 일치해야 한다.

4. **schedule.py 상수 추가** — `DB_FIN_PRODUCT_TIME = "35 8 * * *"` 줄 바로 뒤에 추가:
   ```python
   DB_FIN_PRODUCT_MAP_TIME      = "50 8 * * *" # 매일 08:50 실행
   ```

## Reference Code

### dags/db/DB_FinProduct_Map_Dags.py (번들 — 그대로 복원)
```python
"""
fin_product_map 송파삼전점 표준화 DAG
매일 자동 실행하며 송파삼전점 대상 fin_product_map CSV/JSON을 갱신한다.
사람은 fin_product_map_review.csv만 검수하고, 전체 map은 파이프라인이 병합한다.
수동 테스트 실행은 DAG 실행 conf에 {"dry_run": true}를 명시한다.
"""
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from modules.transform.pipelines.db.DB_FinProduct_Map import (
    llm_product_map,
    migrate_product_map,
    build_fin_product_map_train_json,
)
from modules.transform.utility.notifier import on_failure_callback, send_telegram
from modules.transform.utility.schedule import DB_FIN_PRODUCT_MAP_TIME

dag_id = Path(__file__).stem
DRY_RUN_BY_DEFAULT = False
DRY_RUN_LLM_LIMIT = 20
# ... (이하 번들 원본 전체를 그대로 복사)
```

### modules/transform/pipelines/db/DB_FinProduct_Map.py (번들 — 그대로 복원)
```python
"""fin_product_map 생성 및 내부 LLM 증분 분류."""
import json
import logging
import os
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import (
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
    FIN_PRODUCT_MAP_TRAIN_JSON_PATH,
    MART_DB,
)

TARGET_STORES = ["송파삼전점"]
TARGET_STORE_SET = {s.strip() for s in TARGET_STORES if s.strip()}
UNIFIED_SALES_GRP_DIR = MART_DB / "unified_sales_grp"
# 공개 함수: migrate_product_map, build_fin_product_map_train_json, llm_product_map
# ... (이하 번들 원본 전체를 그대로 복사)
```

> **중요:** 위 Reference는 헤더 발췌일 뿐이다. 실제 복원은 두 파일 모두 번들 원본 파일 전체를 한 글자도 바꾸지 말고 그대로 복사한다.

## Test Cases
1. [경로 상수] `python -c "from modules.transform.utility.paths import FIN_PRODUCT_MAP_CSV_PATH, FIN_PRODUCT_MAP_REVIEW_CSV_PATH, FIN_PRODUCT_MAP_TRAIN_JSON_PATH; print(FIN_PRODUCT_MAP_CSV_PATH)"` → 기대: ImportError 없음, 경로 끝이 `fin_product\fin_product_map.csv`
2. [스케줄 상수] `python -c "from modules.transform.utility.schedule import DB_FIN_PRODUCT_MAP_TIME; print(DB_FIN_PRODUCT_MAP_TIME)"` → 기대: `50 8 * * *`
3. [파이프라인 import] `python -c "from modules.transform.pipelines.db.DB_FinProduct_Map import migrate_product_map, build_fin_product_map_train_json, llm_product_map"` → 기대: ImportError 없음
4. [DAG import] `python -c "from dags.db.DB_FinProduct_Map_Dags import dag; print(dag.dag_id)"` → 기대: `DB_FinProduct_Map_Dags`
5. [출력 파일 경로 일치] 1번에서 출력된 3개 경로가 실제 mart(`...\data\mart\fin_product\`)의 `fin_product_map.csv` / `fin_product_map_review.csv` / `fin_product_map_train.json`과 동일한지 확인

> 모든 명령은 `C:\airflow`를 작업 디렉터리(또는 PYTHONPATH)로 두고 실행.

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석 (import 누락 상수 / 경로 불일치)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 번들의 `paths.py`/`schedule.py`/`notifier.py`/`mailer.py`로 현재 프로젝트 파일을 덮어쓰지 말 것. 상수만 추가.
- `paths.py`는 현재 프로젝트의 직접대입(`X = MART_DB / ...`) 스타일을 따른다. 번들의 lazy `_get` dict 패턴 사용 금지.
- 출력 경로 3개는 mart에 이미 존재하는 동일 파일명을 가리켜야 한다(신규 경로 만들지 말 것).
- DAG 파일과 파이프라인 파일은 번들 원본을 변형 없이 복원.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
