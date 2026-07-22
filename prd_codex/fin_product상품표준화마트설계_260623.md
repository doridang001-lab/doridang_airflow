# fin_product 상품 표준화 마트 설계

## Task
unified_sales 거래 데이터의 `item_name`은 매장·POS별로 동일 메뉴도 표기가 제각각이다.
`item_name` → `표준_메뉴명` 매핑 마스터 테이블(`fin_product_map.csv`)을 신규 생성하고,
LLM(Claude API) 자동분류 + 사람 검토 워크플로우로 채운 뒤 unified_sales LEFT JOIN에 활용한다.

---

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- scripts는 `_base.py`를 첫 줄에 import해 `sys.path` 세팅 및 `save_summary` 활용
- 경로 상수: `from modules.transform.utility.paths import MART_DB, FIN_PRODUCT_CSV_PATH, FIN_PRODUCT_MART_CSV_PATH, POSFEED_WHITELIST_CSV_PATH`
- `unified_sales_grp` 경로: `MART_DB / "unified_sales_grp"` (상수 없음 → 직접 조합)
- `fin_product_map` 경로: `MART_DB / "fin_product" / "fin_product_map.csv"` (상수 없음 → 직접 조합)
- 스크립트 결과는 `save_summary(name, dict)` → `scripts/output/{name}_{ts}.json`
- argparse로 파라미터 처리

---

## Files to Create / Modify

- `C:\airflow\scripts\product_map_migrate.py` — NEW (1회성 마이그레이션)
- `C:\airflow\scripts\product_map_llm.py` — NEW (증분 LLM 자동분류)
- `C:\airflow\modules\transform\utility\paths.py` — MODIFY (경로 상수 2개 추가)

---

## fin_product_map.csv 스키마

| 컬럼 | 설명 | 예시 |
|---|---|---|
| `item_name` | unified_sales 원본 상품명 (자연키) | "묵은지닭도리탕(중)" |
| `source` | POS 종류 | okpos / easypos / unipos / posfeed |
| `표준_메뉴명` | 정규화된 표준명 | "묵은지 닭도리탕" |
| `수동분류` | 카테고리 | 메인 / 사이드 / 주류 / 음료 / 토핑 |
| `review_status` | 검토 상태 | pending / approved |
| `classified_by` | 분류 주체 | llm / human |
| `updated_at` | 최종 수정일 | 2026-06-23 |

---

## Implementation Steps

### Step 0 — paths.py에 상수 2개 추가

`modules/transform/utility/paths.py` 276번 줄 아래에 추가:

```python
FIN_PRODUCT_MAP_CSV_PATH = MART_DB / "fin_product" / "fin_product_map.csv"
FIN_PRODUCT_MAP_JSON_PATH = MART_DB / "fin_product" / "fin_product_map.json"
```

---

### Step 1 — 마이그레이션 스크립트 (`product_map_migrate.py`)

**목적**: 기존 grp/whitelist에서 이미 분류된 데이터를 fin_product_map으로 옮기기 (1회 실행)

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
import pandas as pd
from datetime import date
from scripts._base import save_summary
from modules.transform.utility.paths import (
    FIN_PRODUCT_CSV_PATH,          # fin_product_grp.csv
    POSFEED_WHITELIST_CSV_PATH,    # fin_product_posfeed_whitelist.csv
    FIN_PRODUCT_MAP_CSV_PATH,      # fin_product_map.csv (신규)
)

logger = logging.getLogger(__name__)
TODAY = str(date.today())

SOURCE_PRIORITY = {"okpos": 0, "easypos": 1, "unipos": 2, "posfeed": 3}


def load_grp() -> pd.DataFrame:
    """fin_product_grp.csv에서 표준_메뉴명이 채워진 행만 추출."""
    df = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str).fillna("")
    df = df[df["표준_메뉴명"].str.strip() != ""]
    return df[["source", "상품명", "표준_메뉴명", "수동분류"]].rename(
        columns={"상품명": "item_name"}
    )


def load_whitelist() -> pd.DataFrame:
    """fin_product_posfeed_whitelist.csv에서 is_valid=Y 행 추출."""
    df = pd.read_csv(POSFEED_WHITELIST_CSV_PATH, dtype=str).fillna("")
    df = df[df["is_valid"].str.upper() == "Y"]
    df["source"] = "posfeed"
    df["표준_메뉴명"] = ""   # whitelist는 표준명 없음 → pending 처리
    df["수동분류"] = ""
    return df[["source", "item_name", "표준_메뉴명", "수동분류"]]


def build_map(grp: pd.DataFrame, whitelist: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([grp, whitelist], ignore_index=True)
    combined["_priority"] = combined["source"].map(SOURCE_PRIORITY).fillna(99)
    combined = combined.sort_values("_priority")
    # item_name 기준 중복 제거 (우선순위 높은 source 유지)
    combined = combined.drop_duplicates(subset=["item_name"], keep="first").drop(columns=["_priority"])
    combined["review_status"] = combined["표준_메뉴명"].apply(
        lambda x: "approved" if x.strip() else "pending"
    )
    combined["classified_by"] = "human"
    combined["updated_at"] = TODAY
    return combined.reset_index(drop=True)


def main():
    logger.info("마이그레이션 시작")
    grp = load_grp()
    whitelist = load_whitelist()
    result = build_map(grp, whitelist)

    FIN_PRODUCT_MAP_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(FIN_PRODUCT_MAP_CSV_PATH, index=False, encoding="utf-8-sig")
    logger.info("저장 완료: %s (%d행)", FIN_PRODUCT_MAP_CSV_PATH, len(result))

    summary = {
        "total": len(result),
        "approved": int((result["review_status"] == "approved").sum()),
        "pending": int((result["review_status"] == "pending").sum()),
    }
    save_summary("product_map_migrate", summary)
    return summary


if __name__ == "__main__":
    main()
```

---

### Step 2 — LLM 자동분류 스크립트 (`product_map_llm.py`)

**목적**: unified_sales에서 미매핑 item_name을 찾아 Claude API로 자동 분류 후 fin_product_map에 추가 (증분 방식, 반복 실행 가능)

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import json
import logging
import pandas as pd
from datetime import date
from scripts._base import save_summary
from modules.transform.utility.paths import (
    MART_DB,
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_JSON_PATH,
)

logger = logging.getLogger(__name__)
TODAY = str(date.today())
UNIFIED_SALES_GRP_DIR = MART_DB / "unified_sales_grp"
BATCH_SIZE = 20
CATEGORIES = ["메인", "사이드", "주류", "음료", "토핑"]


def scan_unique_items() -> pd.DataFrame:
    """unified_sales_grp/*.parquet 전체 스캔 → 고유 (source, item_name)."""
    frames = []
    for f in sorted(UNIFIED_SALES_GRP_DIR.glob("unified_sales_*.parquet")):
        df = pd.read_parquet(f, columns=["source", "item_name"])
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["source", "item_name"])
    all_items = pd.concat(frames, ignore_index=True)
    return all_items.drop_duplicates(subset=["item_name"]).reset_index(drop=True)


def find_unmapped(all_items: pd.DataFrame, map_df: pd.DataFrame) -> pd.DataFrame:
    known = set(map_df["item_name"].str.strip())
    return all_items[~all_items["item_name"].isin(known)].reset_index(drop=True)


def build_prompt(batch: list[dict], examples: list[dict]) -> str:
    example_text = "\n".join(
        f'  - "{r["item_name"]}" → 표준명: "{r["표준_메뉴명"]}", 분류: {r["수동분류"]}'
        for r in examples
    )
    items_text = "\n".join(
        f'  {i+1}. source={r["source"]}, item_name="{r["item_name"]}"'
        for i, r in enumerate(batch)
    )
    return f"""도리당 F&B 프렌차이즈 상품명 정규화 전문가입니다.
아래 상품명들을 표준명으로 정규화하고 카테고리를 분류하세요.

카테고리: {", ".join(CATEGORIES)}

기존 승인된 매핑 예시:
{example_text}

분류할 상품 목록:
{items_text}

반드시 JSON 배열로만 응답하세요 (다른 텍스트 없이):
[
  {{"item_name": "...", "표준_메뉴명": "...", "수동분류": "..."}}
]"""


def call_claude(prompt: str) -> list[dict]:
    import anthropic
    client = anthropic.Anthropic()
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    text = msg.content[0].text.strip()
    # JSON 파싱
    start = text.find("[")
    end = text.rfind("]") + 1
    return json.loads(text[start:end])


def export_json(map_df: pd.DataFrame) -> None:
    approved = map_df[map_df["review_status"] == "approved"]
    result = {}
    for _, row in approved.iterrows():
        std = row["표준_메뉴명"].strip()
        if not std:
            continue
        result.setdefault(std, [])
        result[std].append(row["item_name"])
    FIN_PRODUCT_MAP_JSON_PATH.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info("JSON 저장: %s (%d 표준명)", FIN_PRODUCT_MAP_JSON_PATH, len(result))


def main(dry_run: bool = False):
    logger.info("LLM 분류 시작")

    all_items = scan_unique_items()
    logger.info("unified_sales 고유 item_name: %d건", len(all_items))

    map_df = pd.read_csv(FIN_PRODUCT_MAP_CSV_PATH, dtype=str).fillna("") \
        if FIN_PRODUCT_MAP_CSV_PATH.exists() else pd.DataFrame(
            columns=["item_name", "source", "표준_메뉴명", "수동분류", "review_status", "classified_by", "updated_at"]
        )

    unmapped = find_unmapped(all_items, map_df)
    logger.info("미매핑: %d건", len(unmapped))

    if unmapped.empty:
        logger.info("신규 분류 대상 없음. 종료.")
        export_json(map_df)
        return {"new_classified": 0}

    examples = (
        map_df[map_df["review_status"] == "approved"]
        .dropna(subset=["표준_메뉴명"])
        .query("표준_메뉴명 != ''")
        [["item_name", "표준_메뉴명", "수동분류"]]
        .sample(min(20, len(map_df[map_df["review_status"] == "approved"])), random_state=42)
        .to_dict("records")
    )

    new_rows = []
    batches = [unmapped.iloc[i:i+BATCH_SIZE].to_dict("records") for i in range(0, len(unmapped), BATCH_SIZE)]

    for idx, batch in enumerate(batches):
        logger.info("배치 %d/%d 처리 중 (%d건)", idx+1, len(batches), len(batch))
        if dry_run:
            for item in batch:
                new_rows.append({
                    "item_name": item["item_name"],
                    "source": item["source"],
                    "표준_메뉴명": "[DRY_RUN]",
                    "수동분류": "메인",
                    "review_status": "pending",
                    "classified_by": "llm",
                    "updated_at": TODAY,
                })
            continue
        try:
            prompt = build_prompt(batch, examples)
            results = call_claude(prompt)
            item_map = {r["item_name"]: r for r in results}
            for item in batch:
                classified = item_map.get(item["item_name"], {})
                new_rows.append({
                    "item_name": item["item_name"],
                    "source": item["source"],
                    "표준_메뉴명": classified.get("표준_메뉴명", ""),
                    "수동분류": classified.get("수동분류", ""),
                    "review_status": "pending",
                    "classified_by": "llm",
                    "updated_at": TODAY,
                })
        except Exception as e:
            logger.warning("배치 %d 실패: %s", idx+1, e)

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        map_df = pd.concat([map_df, new_df], ignore_index=True)
        map_df.to_csv(FIN_PRODUCT_MAP_CSV_PATH, index=False, encoding="utf-8-sig")
        logger.info("fin_product_map.csv 업데이트: 총 %d행", len(map_df))

    export_json(map_df)

    summary = {
        "total_unique_items": len(all_items),
        "previously_mapped": len(all_items) - len(unmapped),
        "new_classified": len(new_rows),
        "approved": int((map_df["review_status"] == "approved").sum()),
        "pending": int((map_df["review_status"] == "pending").sum()),
    }
    save_summary("product_map_llm", summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="LLM 호출 없이 구조만 테스트")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
```

---

### Step 3 — LEFT JOIN 활용 패턴

downstream 파이프라인에서 아래 패턴으로 합류:

```python
from modules.transform.utility.paths import FIN_PRODUCT_MAP_CSV_PATH

map_df = (
    pd.read_csv(FIN_PRODUCT_MAP_CSV_PATH, dtype=str)
    .fillna("")
    .query("review_status == 'approved'")
    [["item_name", "표준_메뉴명", "수동분류"]]
)
unified = unified.merge(map_df, on="item_name", how="left")
```

---

## Reference Code

### scripts/_base.py
```python
import json, logging, sys, traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUTPUT_DIR = PROJECT_ROOT / "scripts" / "output"
KST = timezone(timedelta(hours=9))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger(__name__)

def save_summary(name: str, data: dict[str, Any]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"{name}_{ts}.json"
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("결과 저장: %s", out_path)
    return out_path
```

### modules/transform/utility/paths.py (관련 상수)
```python
# 기존 상수
FIN_PRODUCT_CSV_PATH = MART_DB / "fin_product" / "fin_product_grp.csv"
FIN_PRODUCT_MART_CSV_PATH = MART_DB / "fin_product" / "fin_product_mart.csv"
POSFEED_WHITELIST_CSV_PATH = MART_DB / "fin_product" / "fin_product_posfeed_whitelist.csv"

# 추가할 상수 (Step 0)
FIN_PRODUCT_MAP_CSV_PATH = MART_DB / "fin_product" / "fin_product_map.csv"
FIN_PRODUCT_MAP_JSON_PATH = MART_DB / "fin_product" / "fin_product_map.json"
```

---

## Test Cases

1. **paths 상수 확인**
   ```
   python -c "from modules.transform.utility.paths import FIN_PRODUCT_MAP_CSV_PATH, FIN_PRODUCT_MAP_JSON_PATH; print(FIN_PRODUCT_MAP_CSV_PATH)"
   ```
   → 기대: OneDrive/.../fin_product/fin_product_map.csv 경로 출력, ImportError 없음

2. **마이그레이션 dry 실행**
   ```
   python scripts/product_map_migrate.py
   ```
   → 기대: `scripts/output/product_map_migrate_*.json`에 `{"total": N, "approved": M, "pending": P}` 저장, fin_product_map.csv 생성

3. **LLM 분류 dry-run**
   ```
   python scripts/product_map_llm.py --dry-run
   ```
   → 기대: LLM 호출 없이 미매핑 건수 로그 출력, `[DRY_RUN]` 행 추가

4. **LLM 분류 실제 실행**
   ```
   python scripts/product_map_llm.py
   ```
   → 기대: pending 행 추가, fin_product_map.json 생성

5. **null 비율 검증**
   ```python
   import pandas as pd
   from modules.transform.utility.paths import MART_DB, FIN_PRODUCT_MAP_CSV_PATH
   unified = pd.read_parquet(MART_DB / "unified_sales_grp" / "unified_sales_260101.parquet")
   map_df = pd.read_csv(FIN_PRODUCT_MAP_CSV_PATH).query("review_status == 'approved'")
   merged = unified.merge(map_df[["item_name","표준_메뉴명"]], on="item_name", how="left")
   null_pct = merged["표준_메뉴명"].isna().mean() * 100
   print(f"null 비율: {null_pct:.1f}%")  # 목표: 5% 미만
   ```

---

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~5 순서대로 실행
  2. FAIL 항목 → 원인 분석 (import 오류, 경로 오류, JSON 파싱 오류 순으로 확인)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + null 비율 5% 미만
```

---

## Constraints

- 기존 파일(fin_product_grp.csv, fin_product_mart.csv, fin_product_posfeed_whitelist.csv) **삭제 금지** — 방치
- `fin_product_map.csv`는 `item_name` 기준 중복 없이 유지 (upsert 방식)
- LLM 응답 파싱 실패 시 해당 배치 skip, 로그만 남기고 전체 중단 금지
- Claude API key는 환경변수 `ANTHROPIC_API_KEY`로 주입 (하드코딩 금지)
- `review_status=pending` 행은 LEFT JOIN 집계에서 제외 (approved만 사용)
- `수동분류` 값은 반드시 `["메인", "사이드", "주류", "음료", "토핑"]` 중 하나

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
