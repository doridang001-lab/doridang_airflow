# DB_FinProduct OKPOS 신규 상품 누락 버그 수정

## Task
`fin_product_grp.csv`에 OKPOS 신규/변경 상품이 매일 중복 탐지되거나 누락되는 현상을 수정한다.
실제 데이터(2975행) 분석으로 두 개의 확정 버그 발견. OKPOS 우선순위 1번.

---

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

---

## 수집 소스 경로 (참고)

| 소스 | 경로 |
|------|------|
| OKPOS xlsx 입력 | `ANALYTICS_DB / "okpos_product" / "상품조회.xlsx"` (476행) |
| EASYPOS xlsx 입력 | `ANALYTICS_DB / "easypos_product" / "상품조회.xlsx"` (55행) |
| 출력 CSV | `FIN_PRODUCT_CSV_PATH` = `MART_DB / "fin_product" / "fin_product_grp.csv"` |

---

## Files to Modify

- `modules/transform/pipelines/db/DB_FinProduct.py`
  - `_read_master()` 함수 끝부분에 `updated_at` 정규화 헬퍼 추가
  - `detect_product_changes()` 함수에서 `iloc[-1]` → `is_latest=Y` 비교로 수정

---

## Implementation Steps

### 1. `_normalize_updated_at` 헬퍼 추가 (모듈 상단 private 함수로)

`_read_master()` 위 또는 helpers 섹션에 아래 함수를 추가한다:

```python
def _normalize_updated_at(ts: str) -> str:
    """'YYYY-MM-DD H:MM' 형식을 'YYYY-MM-DD HH:MM:00'으로 패딩 (문자열 정렬 안전성 확보)."""
    import re
    m = re.match(r'^(\d{4}-\d{2}-\d{2}) (\d):(\d{2})$', ts)
    if m:
        return f"{m.group(1)} 0{m.group(2)}:{m.group(3)}:00"
    return ts
```

### 2. `_read_master()` 끝부분에 updated_at 정규화 적용

현재 `_read_master()`의 return 직전(line 342 근처)에 아래 코드를 추가한다:

```python
# updated_at 형식 정규화: "H:MM" → "HH:MM:00" (is_latest 정렬 안전성)
if "updated_at" in df.columns:
    df["updated_at"] = df["updated_at"].apply(_normalize_updated_at)

return df
```

### 3. `detect_product_changes()` — `iloc[-1]` → `is_latest=Y` 비교로 교체

**현재 코드** (`modules/transform/pipelines/db/DB_FinProduct.py`, line 541-542):
```python
matching = df_master[(master_src == src) & (master_code == code)]
prev = matching.iloc[-1]
```

**수정 후**:
```python
matching = df_master[(master_src == src) & (master_code == code)]
if "is_latest" in matching.columns:
    latest_rows = matching[matching["is_latest"].str.upper() == "Y"]
    prev = latest_rows.iloc[-1] if not latest_rows.empty else matching.iloc[-1]
else:
    prev = matching.iloc[-1]
```

---

## Reference Code

### `_read_master()` 현재 코드 (line 310-342)

```python
def _read_master() -> pd.DataFrame:
    """fin_product_grp.csv 로드. 없으면 빈 DataFrame."""
    if not FIN_PRODUCT_CSV_PATH.exists():
        return pd.DataFrame(columns=["source", "구분", "대메뉴", "중메뉴", "상품코드", "상품명",
                                     "판매단가", "수동분류", "is_main_candidate", "llm_check",
                                     "exclude_check", "updated_at", "is_latest"])
    df = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str).fillna("")

    # Backward compatibility
    if "exclude_check" not in df.columns:
        df["exclude_check"] = "N"
    if "updated_at" not in df.columns:
        df["updated_at"] = ""
    if "구분" not in df.columns:
        df["구분"] = ""
    if "is_latest" not in df.columns:
        df["is_latest"] = "N"
    if "표준_메뉴명" in df.columns:
        df = df.drop(columns=["표준_메뉴명"], errors="ignore")
    if "중복_수동분류" not in df.columns:
        df["중복_수동분류"] = "N"

    df["exclude_check"] = df["exclude_check"].fillna("").astype(str).str.strip().str.upper().replace({"": "N"})
    # ... (flags normalization)

    return df   # ← 이 return 직전에 updated_at 정규화 삽입
```

### `detect_product_changes()` 현재 코드 (line 518-550)

```python
def detect_product_changes(**context) -> str:
    okpos_json = context["ti"].xcom_pull(task_ids="load_okpos_product_xlsx", key=_XCOM_OKPOS)
    df_new = pd.DataFrame(json.loads(okpos_json))

    df_master = _read_master()
    changes = []

    master_src = df_master["source"].fillna("").astype(str).str.strip()
    master_code = df_master["상품코드"].fillna("").astype(str).str.strip()
    existing_keys = set(zip(master_src, master_code))

    for _, row in df_new.iterrows():
        code = str(row.get("상품코드", "")).strip()
        src = str(row.get("source", "")).strip()
        key = (src, code)

        if key not in existing_keys:
            d = row.to_dict()
            d["_change_type"] = "신규"
            changes.append(d)
        else:
            matching = df_master[(master_src == src) & (master_code == code)]
            prev = matching.iloc[-1]   # ← 여기를 수정
            if any(str(row.get(k, "")).strip() != str(prev.get(k, "")).strip() for k in _CHANGE_KEYS):
                d = row.to_dict()
                d["_change_type"] = "변경"
                changes.append(d)

    context["ti"].xcom_push(key=_XCOM_CHANGES, value=json.dumps(changes, ensure_ascii=False))
    logger.info("변경 감지: %d건 (신규+변경)", len(changes))
    return f"변경 감지 {len(changes)}건"
```

---

## Test Cases

1. **버그 1 수정 검증** — iloc[-1] vs is_latest=Y 불일치 케이스가 0이어야 함:
```python
python -c "
import pandas as pd
from pathlib import Path
import os

onedrive = Path(os.environ.get('OneDriveCommercial', Path.home() / 'OneDrive - 주식회사 도리당'))
csv_path = onedrive / 'data' / 'mart' / 'fin_product' / 'fin_product_grp.csv'
df = pd.read_csv(csv_path, dtype=str).fillna('')

# updated_at 정규화 적용 후 재마킹 시뮬레이션
import re
def norm(ts):
    m = re.match(r'(\d{4}-\d{2}-\d{2}) (\d):(\d{2})$', ts)
    return f'{m.group(1)} 0{m.group(2)}:{m.group(3)}:00' if m else ts
df['updated_at'] = df['updated_at'].apply(norm)

master_src = df['source'].fillna('').astype(str).str.strip()
master_code = df['상품코드'].fillna('').astype(str).str.strip()
issues = []
for (src, code), group in df.groupby([master_src, master_code]):
    latest_rows = group[group['is_latest'].str.upper() == 'Y']
    if not latest_rows.empty:
        latest_idx = latest_rows.index[-1]
        last_idx = group.index[-1]
        if latest_idx != last_idx:
            issues.append((src, code))
print(f'불일치 케이스: {len(issues)}건 (목표: 0건에 근접)')
"
```
→ 기대: 수정 전 51건 → 수정 후 크게 감소 (0건 이상적)

2. **버그 2 수정 검증** — updated_at 정규화 후 is_latest 재마킹 정합성:
```python
python -c "
import pandas as pd, re, os
from pathlib import Path
onedrive = Path(os.environ.get('OneDriveCommercial', Path.home() / 'OneDrive - 주식회사 도리당'))
csv_path = onedrive / 'data' / 'mart' / 'fin_product' / 'fin_product_grp.csv'
df = pd.read_csv(csv_path, dtype=str).fillna('')
# H:MM 형식 행 수 확인
bad_fmt = df['updated_at'].str.match(r'^\d{4}-\d{2}-\d{2} \d:\d{2}$', na=False).sum()
print(f'정규화 필요 행: {bad_fmt}건')
"
```
→ 기대: `2775건` 출력 (수정 전). 수정 후 `_read_master()` 통해 로드 시 자동 정규화.

3. **import 검증**:
```
python -c "from modules.transform.pipelines.db.DB_FinProduct import detect_product_changes, _read_master"
```
→ 기대: ImportError 없음

4. **Airflow DAG 트리거** (수동):
   - Airflow UI → `DB_FinProduct_Dags` → Trigger DAG
   - `detect_product_changes` 로그 확인: "변경 감지 N건" → 51개 오탐지 상품이 더 이상 매일 감지되지 않아야 함

---

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~3 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: Test 1 불일치 케이스 0 또는 현저히 감소 + ImportError 없음
```

---

## Constraints

- `_normalize_updated_at`은 `_read_master()` 내부에서만 호출 (외부 노출 불필요)
- `_read_master()` 반환 직전에 삽입 → 이 함수를 쓰는 모든 downstream 자동 적용
- `_mark_is_latest` 자체는 건드리지 않음 (정규화를 upstream에서 처리)
- DAG 파일(`dags/db/DB_FinProduct_Dags.py`) 변경 없음
- 기존 CSV 파일 자체는 건드리지 않음 (코드 수정만, 데이터 마이그레이션 없음)
- print 금지, logger 사용

---

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- `_normalize_updated_at` 위치: helpers 섹션 (`_read_xlsx()` 위쪽, 기존 헬퍼들과 함께)
- import re: 이미 파일 상단에 있음 (중복 추가 불필요)
- 타입 힌트: 기존 파일과 동일하게 (`str -> str`)
- 주석: 최소화, WHY만
