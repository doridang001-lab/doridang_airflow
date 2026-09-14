# 네이버 자동수집 → hall_marketing_target 자동 반영

## Task
`DB_Hall_Sales_Target_Dags.py` 리포트가 쓰는 `hall_marketing_target.csv` 의 `플레이스_유입`·`네이버_오더` 두 컬럼은 지금까지 매주 수동 입력이었다. 크롬 확장이 이미 자동수집해 적재하는 `naver_corporate_store_marketing.csv`(컬럼 `date/place_inflow/reservations`)를 소스로 이 두 컬럼을 자동 채워, 수동 입력을 없애고 주간/일일 리포트에도 그대로 반영한다. 소스 파일은 **다른 경로에서 이미 갱신**되므로 이 작업은 **읽기 전용 소비 + sync**만 한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- DAG 파일 위치: `dags/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- 파이프라인은 Airflow 비의존(순수 pandas)로 작성 — 로컬 실행/테스트 가능하게

## Files to Create / Modify
**생성**
- `modules/transform/pipelines/db/DB_Hall_Marketing_Sync.py` — `sync_naver_marketing()` sync 로직

**수정**
- `modules/transform/utility/paths.py` — 소스 CSV 경로 상수 1줄 추가
- `dags/db/DB_Hall_Sales_Target_Dags.py` — import + 태스크 + 의존성 추가

## 컬럼 매핑 (확정)
| naver_corporate_store_marketing.csv | → | hall_marketing_target.csv |
|---|---|---|
| `date` | → | `입력날짜` (기준일자는 공란 유지 — 소비단이 입력날짜로 fallback) |
| `place_inflow` | → | `플레이스_유입` |
| `reservations` | → | `네이버_오더` |

## 확정된 동작
- 소스(`naver_corporate_store_marketing.csv`)는 이미 별도 갱신됨 → **읽기만**.
- 덮어쓰기: 네이버 수집값이 **정답 소스**. 해당 날짜의 `플레이스_유입`/`네이버_오더`는 항상 네이버 값으로 갱신.
- **나머지 마케팅 컬럼(`홍보물_배포`, `쿠폰_회수건수`, `인스타_노출`, `당근_노출`)은 절대 손대지 않음(보존).**
- 소스에만 있고 target에 없는 날짜(예: `2026-07-20`)는 **새 행 추가**(나머지 컬럼 `""`).

## Implementation Steps

### 1. `modules/transform/utility/paths.py` — 경로 상수 추가
`MART_DB = resolve_mart_db()` (라인 265) **아래**, 다른 `MART_DB / ...` 상수들과 같은 블록에 추가:
```python
NAVER_CORP_STORE_MKT_CSV_PATH = (
    MART_DB / "naver_corporate_store_marketing" / "naver_corporate_store_marketing.csv"
)
```

### 2. `modules/transform/pipelines/db/DB_Hall_Marketing_Sync.py` — 신규 생성
```python
"""
네이버 자동수집(naver_corporate_store_marketing.csv) → hall_marketing_target.csv sync.

플레이스_유입 / 네이버_오더 두 컬럼만 네이버 수집값으로 덮어쓴다.
나머지 마케팅 컬럼은 보존. 소스에만 있는 날짜는 새 행으로 추가한다.
소스 파일은 다른 경로에서 이미 갱신되므로 이 모듈은 읽기 전용 소비만 한다.
"""

import logging

import pandas as pd

from modules.transform.utility.paths import NAVER_CORP_STORE_MKT_CSV_PATH
from modules.transform.pipelines.db.DB_Hall_Sales_Excel import MKT_CSV  # hall_marketing_target.csv

logger = logging.getLogger(__name__)

STORE_NAME = "송파삼전점"


def _norm_date(value) -> str:
    ts = pd.to_datetime(str(value).strip(), errors="coerce")
    return "" if pd.isna(ts) else ts.strftime("%Y-%m-%d")


def sync_naver_marketing() -> None:
    if not NAVER_CORP_STORE_MKT_CSV_PATH.exists():
        logger.warning("네이버 수집 소스 없음, sync 생략: %s", NAVER_CORP_STORE_MKT_CSV_PATH)
        return
    if not MKT_CSV.exists():
        logger.warning("hall_marketing_target.csv 없음, sync 생략: %s", MKT_CSV)
        return

    src = pd.read_csv(NAVER_CORP_STORE_MKT_CSV_PATH, dtype=str,
                      keep_default_na=False, encoding="utf-8-sig")
    if "store" in src.columns:
        src = src[src["store"].astype(str).str.strip() == STORE_NAME]
    # date -> (place_inflow, reservations) — 같은 날짜 중복 시 마지막 값 우선
    by_date: dict[str, tuple[str, str]] = {}
    for _, r in src.iterrows():
        d = _norm_date(r.get("date", ""))
        if d:
            by_date[d] = (str(r.get("place_inflow", "")).strip(),
                          str(r.get("reservations", "")).strip())

    tgt = pd.read_csv(MKT_CSV, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    columns = list(tgt.columns)  # 원본 컬럼 순서 보존
    tgt["입력날짜"] = tgt["입력날짜"].map(_norm_date)

    updated = 0
    existing_dates = set(tgt["입력날짜"])
    for i in tgt.index:
        d = tgt.at[i, "입력날짜"]
        if d in by_date:
            place, naver = by_date[d]
            tgt.at[i, "플레이스_유입"] = place
            tgt.at[i, "네이버_오더"] = naver
            updated += 1

    new_rows = []
    for d, (place, naver) in by_date.items():
        if d not in existing_dates:
            row = {c: "" for c in columns}
            row["입력날짜"] = d
            row["플레이스_유입"] = place
            row["네이버_오더"] = naver
            new_rows.append(row)

    if new_rows:
        tgt = pd.concat([tgt, pd.DataFrame(new_rows, columns=columns)], ignore_index=True)

    tgt = tgt.sort_values("입력날짜").reset_index(drop=True)
    tgt = tgt[columns]
    tgt.to_csv(MKT_CSV, index=False, encoding="utf-8-sig")
    logger.info("네이버 마케팅 sync 완료: 갱신 %d건 / 추가 %d건 → %s",
                updated, len(new_rows), MKT_CSV)
```

### 3. `dags/db/DB_Hall_Sales_Target_Dags.py` — 태스크 추가
import 블록에 추가:
```python
from modules.transform.pipelines.db.DB_Hall_Marketing_Sync import sync_naver_marketing
```
`with DAG(...) as dag:` 안, 기존 `t_csv` 정의 근처에 태스크 추가:
```python
    t_sync_mkt = PythonOperator(
        task_id="sync_naver_marketing",
        python_callable=sync_naver_marketing,
    )
```
의존성 — 기존 마지막 두 줄:
```python
    t_csv >> [t_excel, t_daily_excel, t_daily_csv]
    t_excel >> t_llm_log
```
아래에 추가(마케팅 CSV를 읽는 세 태스크가 sync 완료 후 실행되도록. `t_sync_mkt`는 `t_csv`와 병렬 가능):
```python
    t_sync_mkt >> [t_excel, t_daily_excel, t_daily_csv]
```

## Reference Code

### modules/transform/pipelines/db/DB_Hall_Sales_Excel.py (MKT_CSV 정의 + _load_mkt 소비)
```python
from modules.transform.utility.paths import MART_DB

SALE_CSV  = MART_DB / "hall_sales_target" / "hall_sale_target.csv"
MKT_CSV   = MART_DB / "hall_sales_target" / "hall_marketing_target.csv"

def _to_number(series: pd.Series) -> pd.Series:
    cleaned = series.fillna("").astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(cleaned, errors="coerce").fillna(0)

def _load_mkt():
    df = pd.read_csv(MKT_CSV, dtype=str)
    df = df.rename(columns={"인스타_노출_traget": "인스타_노출_target"})
    df["입력날짜"] = pd.to_datetime(df["입력날짜"], errors="coerce")
    df["기준일자"] = pd.to_datetime(df["기준일자"], errors="coerce")
    for c in df.columns:
        if c not in ("입력날짜", "기준일자"):
            df[c] = _to_number(df[c])
    # 기준일자가 비어있으면 입력날짜로 fallback
    df["_기준일"] = df["기준일자"].fillna(df["입력날짜"])
    # ... 이후 _year/_month 파생 후 플레이스_유입/네이버_오더 사용
```

### modules/transform/pipelines/db/DB_Hall_Daily_Excel.py (_load_marketing — 두 컬럼 join 소비)
```python
MKT_CSV = MART_DB / "hall_sales_target" / "hall_marketing_target.csv"

def _load_marketing(ym: str) -> pd.DataFrame:
    if not MKT_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(MKT_CSV, dtype=str)
    df["입력날짜"] = pd.to_datetime(df["입력날짜"], errors="coerce")
    df["기준일자"] = pd.to_datetime(df["기준일자"], errors="coerce")
    df["_기준일"] = df["기준일자"].fillna(df["입력날짜"])
    for col in ["플레이스_유입", "홍보물_배포", "쿠폰_회수건수", "인스타_노출", "당근_노출", "네이버_오더"]:
        if col in df.columns:
            df[col] = _to_number(df[col])
    # ... "place": int(m["플레이스_유입"]), "naver": int(m["네이버_오더"]) 로 사용
```

### hall_marketing_target.csv 헤더/형식 (BOM + 콤마값 존재 — 반드시 보존)
```
﻿입력날짜,기준일자,플레이스_유입,홍보물_배포,쿠폰_회수건수,인스타_노출,당근_노출,네이버_오더
2026-05-15,,277,,,941,"1,811",
2026-07-19,,357,,,0,0,0
```
- BOM(`﻿`) 유지 → read/write 모두 `encoding="utf-8-sig"`.
- 콤마 포함 값(`"1,811"`)은 dtype=str + keep_default_na=False 로 문자열 보존, to_csv가 자동 quoting.

### naver_corporate_store_marketing.csv 소스 형식
```
﻿store,date,ym,place_inflow,reservations
송파삼전점,2026-07-20,2026_07,344,1
```

## Test Cases
1. [모듈 import] `python -c "from modules.transform.pipelines.db.DB_Hall_Marketing_Sync import sync_naver_marketing"` → 기대: 에러 없음
2. [경로 상수] `python -c "from modules.transform.utility.paths import NAVER_CORP_STORE_MKT_CSV_PATH; print(NAVER_CORP_STORE_MKT_CSV_PATH)"` → 기대: `...\data\mart\naver_corporate_store_marketing\naver_corporate_store_marketing.csv`
3. [DAG import] `python -c "import dags.db.DB_Hall_Sales_Target_Dags"` → 기대: ImportError 없음
4. [sync 드라이런] `python -c "from modules.transform.pipelines.db.DB_Hall_Marketing_Sync import sync_naver_marketing; sync_naver_marketing()"` → 기대: 로그 `네이버 마케팅 sync 완료: 갱신 N건 / 추가 M건`, 예외 없음
5. [결과 검증] sync 후 `hall_marketing_target.csv` 확인:
   - 새 행 `2026-07-20` 추가되고 `플레이스_유입=344`, `네이버_오더=1`
   - 기존 날짜의 `플레이스_유입`/`네이버_오더`가 naver 값과 일치
   - `홍보물_배포/쿠폰_회수건수/인스타_노출/당근_노출` 기존 값(특히 `"1,811"` 같은 콤마값) **그대로 보존**
   ```
   python -c "import pandas as pd; from modules.transform.pipelines.db.DB_Hall_Sales_Excel import MKT_CSV; d=pd.read_csv(MKT_CSV,dtype=str,keep_default_na=False,encoding='utf-8-sig'); r=d[d['입력날짜']=='2026-07-20']; print(r.to_dict('records'))"
   ```
   → 기대: place `344`, 네이버_오더 `1` 포함 1행
6. [리포트 반영] `python scripts/run_hall_weekly_report_local.py` → 기대: 에러 없이 `hall_weekly_report.xlsx` 재생성, 표2 마케팅 현황에 갱신값 반영

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~6 순서대로 실행
  2. FAIL 항목 → 원인 분석 (import 경로 / BOM·인코딩 / 컬럼 보존 / 날짜 매칭)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 소스 CSV(`naver_corporate_store_marketing.csv`)는 **읽기 전용** — 절대 수정/생성하지 않는다(다른 경로에서 이미 갱신됨).
- 덮어쓰기 대상은 `플레이스_유입`, `네이버_오더` **두 컬럼 한정**. 나머지 마케팅 컬럼은 코드가 건드리지 않는다.
- CSV read/write 모두 `encoding="utf-8-sig"`, `dtype=str`, `keep_default_na=False` — BOM·콤마값·빈칸 보존.
- 원본 컬럼 순서 유지(`columns = list(tgt.columns)` 로 고정 후 마지막에 재적용).
- 소스 파일 부재 시 sync는 no-op(warning) — 기존 리포트 파이프라인을 절대 막지 않는다.
- 폴더/파일명 변경 금지. 기존 import 경로 그대로.
- print 금지 → logging 사용.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
- 날짜 매칭이 애매하면: 양쪽 모두 `YYYY-MM-DD` 문자열로 normalize 후 매칭
