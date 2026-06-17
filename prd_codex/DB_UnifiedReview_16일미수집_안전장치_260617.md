# DB_UnifiedReview 16일 미수집 추적 & 안전장치

## Task
`DB_UnifiedReview` 마트가 작성일자 6/16을 누락한 원인을 추적한 결과, **ToOrder VOC 플랫폼의 리뷰 노출(승인) 지연**으로 확인됐다(코드 버그·네트워크 끊김 아님). 데이터는 익일 수집분에서 자동 회복되지만, "노출 지연"과 "진짜 부분 실패"를 시스템이 구분하지 못하는 위험이 있어 **수집 스냅샷 완전성 검증**과 **수동 재수집 절차**를 추가한다.

## 추적 결과 (근거)
- 원천 `toorder_voc_YYYYMMDD.parquet`는 정상 크기로 저장됨. 총행수 단조 증가(14,609→14,886→14,936).
- 겹치는 작성일자 건수가 파일 간 **동일**(6/8~6/13). 끊겨서 잘렸다면 들쭉날쭉해야 함.
- 파일명 날짜 D 스냅샷은 작성일자 **D-1까지만** 포함. "최신일 0건 → 다음날 완전체 등장" 계단 패턴이 매일 정확히 반복(6/14: f0614=0 → f0615=695 고정).
- 결론: 6/16 리뷰는 6/18 아침 수집분(`toorder_voc_20260617.parquet`)에 처음 포함 → 그날 lookback 재집계로 `unified_review_260616.parquet` 자동 생성.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- 수정: `modules/transform/pipelines/sales/Sales_ToOrder_Review_collect.py` — 스냅샷 완전성 검증 helper `_validate_snapshot` 추가 + `t3_save`(또는 신규 `t4_validate`)에서 호출
- 수정: `dags/sales/Sales_ToOrder_Review_Dags.py` — `t4_validate` task 추가 시 wiring(`save >> validate`)
- 수정(문서): `dags/db/DB_UnifiedReview_Dags.py`, `modules/transform/pipelines/db/DB_UnifiedReview.py`, `modules/transform/pipelines/sales/Sales_ToOrder_Review_collect.py` docstring에 ~1일 노출 지연 명시

## Implementation Steps

### 1. 스냅샷 완전성 검증 helper (핵심)
`Sales_ToOrder_Review_collect.py`에 추가:
```python
def _validate_snapshot(new_path: Path, prev_path: Path, drop_tol: float = 0.05) -> dict:
    """직전 스냅샷 대비 겹치는 작성일자 건수가 급감하면 truncation(부분 실패) 의심.
    최신일(target_date) 0건은 플랫폼 노출 지연으로 정상 간주."""
    new = pd.read_parquet(new_path, columns=["작성일자"])
    prev = pd.read_parquet(prev_path, columns=["작성일자"])
    nc = new["작성일자"].astype(str).str[:10].value_counts()
    pc = prev["작성일자"].astype(str).str[:10].value_counts()

    suspicious = []
    for d in pc.index.intersection(nc.index):  # 겹치는 날짜만
        if nc[d] < pc[d] * (1 - drop_tol):
            suspicious.append((d, int(pc[d]), int(nc[d])))

    total_shrink = len(new) < len(prev)
    ok = (not suspicious) and (not total_shrink)
    return {"ok": ok, "suspicious": suspicious, "total_new": len(new),
            "total_prev": len(prev), "total_shrink": total_shrink}
```

### 2. t3_save 직후 검증 호출 (또는 신규 t4_validate task)
- 방금 저장한 파일 경로 = `output_dir / f"toorder_voc_{target_date}.parquet"`.
- 직전 스냅샷 = `output_dir / f"toorder_voc_{(target_date-1일)}.parquet"` (존재할 때만).
- `result["ok"] is False`면 `logger.warning(...)` + `AirflowException`으로 기존 `on_failure_callback`(텔레그램/이메일/heal) 재사용.
- 최신일 0건은 절대 실패로 처리하지 않음(겹치는 날짜만 비교하므로 자연히 제외됨).

### 3. 지연 특성 문서화
각 docstring 상단에 1줄: `# ToOrder VOC는 작성일자 기준 ~1일 노출 지연. 최신 1~2일치는 후속 lookback 재집계로 채워짐.`

### 4. 16일 즉시 재수집(수동 복구) — 운영 절차
파일명이 `target_date` 기준이고 `t1_prepare`는 **이미 존재하는 파일을 skip**한다. 따라서 작성일자 6/16은 새 스냅샷 `toorder_voc_20260617.parquet`로 들어온다.
```bash
# (1) 수동 수집 트리거 (현재 노출 전체분)
airflow dags trigger Sales_ToOrder_Review_Dags --conf '{"start_date":"2026-06-17","end_date":"2026-06-17"}'

# (2) 6/16 포함 검증
python -c "import pandas as pd; from modules.transform.utility.paths import TOORDER_REVIEW_ANALYTICS_DIR as D; s=pd.read_parquet(D/'toorder_voc_20260617.parquet')['작성일자'].astype(str).str[:10]; print('6/16건수:', (s=='2026-06-16').sum())"

# (3) 마트 재집계 (lookback이 작성일자별 덮어쓰기)
airflow dags trigger DB_UnifiedReview_Dags
# 또는 특정일: --conf '{"sale_date":"2026-06-16"}'
```
6/16 건수 0이면 플랫폼 미노출 → 익일 스케줄이 자동 처리(2번 검증으로 지연/실패 구분).

## Reference Code

### modules/transform/pipelines/sales/Sales_ToOrder_Review_collect.py
```python
import logging
import pandas as pd
import pendulum
from pathlib import Path
from modules.transform.utility.paths import ANALYTICS_DB, TEMP_DIR

def _resolve_output_dir() -> Path:
    override = os.getenv("TOORDER_VOC_OUTPUT_DIR")
    if override:
        return Path(override)
    return ANALYTICS_DB / "toorder_review"

def t3_save(**context):
    ti = context["ti"]
    temp_parquet_map = ti.xcom_pull(task_ids="t2_collect", key="temp_parquet_map") or {}
    output_dir = Path(ti.xcom_pull(task_ids="t1_prepare", key="output_dir"))
    saved_paths = []
    for target_date, tmp_path_str in temp_parquet_map.items():
        final_path = output_dir / f"toorder_voc_{target_date.replace('-', '')}.parquet"
        shutil.move(tmp_path_str, str(final_path))
        saved_paths.append(str(final_path))
    # ↑ 여기서 _validate_snapshot(final_path, 직전파일) 호출 추가
```

### modules/transform/pipelines/db/DB_UnifiedReview.py
```python
from modules.transform.utility.paths import (
    TOORDER_REVIEW_ANALYTICS_DIR, UNIFIED_REVIEW_MART_DIR,
)
_GROUP_KEYS = ["작성일자", "매장명", "토픽", "감정수준"]

def _save_mart(df) -> int:  # 작성일자별 unified_review_YYMMDD.parquet 덮어쓰기 저장
    for date_val, group in df.groupby("작성일자", dropna=False):
        short = str(date_val).replace("-", "")[2:]
        group.to_parquet(UNIFIED_REVIEW_MART_DIR / f"unified_review_{short}.parquet", index=False)

def run_review(mode="lookback", days=30, target_date=None) -> str:
    files = _collect_files(mode=mode, days=days, target_date=target_date)
    # lookback: 오늘 기준 days일 내 파일 → 작성일자별 재집계 → _save_mart
```

## Test Cases
1. [helper 정상판정] f0615/f0616를 `_validate_snapshot`에 입력 → 기대: `ok=True`, `suspicious=[]` (겹침 안정, 최신일 자동 제외)
   `python -c "from modules.transform.pipelines.sales.Sales_ToOrder_Review_collect import _validate_snapshot; from modules.transform.utility.paths import TOORDER_REVIEW_ANALYTICS_DIR as D; print(_validate_snapshot(D/'toorder_voc_20260616.parquet', D/'toorder_voc_20260615.parquet'))"`
2. [helper 이상감지] f0616에서 임의 날짜 건수를 절반으로 깎은 더미 vs 원본 → 기대: `ok=False`, `suspicious` 비어있지 않음
3. [pipeline import] `python -c "from modules.transform.pipelines.sales.Sales_ToOrder_Review_collect import _validate_snapshot, t3_save"` → 기대: ImportError 없음
4. [DAG import] `python -c "from dags.sales.Sales_ToOrder_Review_Dags import dag; print(dag.task_ids)"` → 기대: 에러 없음, t4_validate 추가 시 목록에 포함
5. [DAG import] `python -c "from dags.db.DB_UnifiedReview_Dags import dag"` → 기대: ImportError 없음

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
- `target_date = 어제` 로직, lookback 30일 재집계 로직은 정상이므로 **변경 금지**.
- 기존 `toorder_voc_20260616.parquet`는 정상 파일 → 삭제·재수집 금지(t1 skip 정상 동작).
- 검증은 **겹치는 작성일자만** 비교한다. 최신일(target_date) 0건을 실패로 처리하면 매일 오탐 발생 → 절대 금지.
- 알림은 기존 `on_failure_callback`(텔레그램/이메일/heal) 재사용. 새 알림 채널 만들지 말 것.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- t4_validate를 별도 task로 둘지 t3_save 내부에 넣을지: 실패 알림 분리가 명확한 **별도 task** 권장
