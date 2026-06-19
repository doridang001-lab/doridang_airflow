# OKPOS daily 0원 버그 Fix A 완성

## Task

`download_daily` 태스크가 2026-06-10 이후 모든 매장에서 0원을 저장하는 무음 버그 수정.  
Fix B(shopOpen 언체크)와 6월 CSV 삭제는 완료됨. 남은 작업은 **Fix A 진단 로깅 완성** 1건.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Modify

- `modules/transform/pipelines/db/DB_OKPOS_Sales.py` — `_transform_okpos_daily_df` 함수 내부 (line ~2125)

## Implementation Steps

### Fix A — `if out_amount_sum == 0:` 직후에 무조건 logger.warning 추가

**현재 코드 (line 2125):**
```python
    out_amount_sum = int(out[["총매출액", "총할인액", "실매출액", "영수건수"]].sum(axis=1).iloc[0])
    if out_amount_sum == 0:
        amount_cols = [c for c in ("총매출액", "총할인액", "실매출액", "영수건수") if c in data.columns]
```

**변경 후:**
```python
    out_amount_sum = int(out[["총매출액", "총할인액", "실매출액", "영수건수"]].sum(axis=1).iloc[0])
    if out_amount_sum == 0:
        logger.warning(
            f"daily 0원 감지 | {sale_date} | {store_name} | "
            f"columns={list(data.columns)} | "
            f"selected_row={row_series.to_dict()} | "
            f"data_head=\n{data.head(10).to_string()}"
        )
        amount_cols = [c for c in ("총매출액", "총할인액", "실매출액", "영수건수") if c in data.columns]
```

**변경 위치:** `amount_cols = [c for c in ...` 바로 위에 logger.warning 블록 삽입.

## Reference Code

### DB_OKPOS_Sales.py — 수정 대상 전체 블록 (현재 상태)

```python
    out_amount_sum = int(out[["총매출액", "총할인액", "실매출액", "영수건수"]].sum(axis=1).iloc[0])
    if out_amount_sum == 0:
        amount_cols = [c for c in ("총매출액", "총할인액", "실매출액", "영수건수") if c in data.columns]
        nonzero_source_found = False
        if amount_cols:
            dt = pd.to_datetime(data["일자"].astype(str).str.strip(), errors="coerce")
            same_date_rows = data[dt.dt.strftime("%Y-%m-%d") == sale_date].copy()
            scan_rows = same_date_rows if not same_date_rows.empty else data
            for _, src_row in scan_rows.iterrows():
                if any(_to_amount(src_row.get(c)) != 0 for c in amount_cols):
                    nonzero_source_found = True
                    break
        if nonzero_source_found:
            logger.warning(
                f"daily 0원 의심 | {sale_date} | {store_name} | "
                f"columns={list(data.columns)} | "
                f"selected_row={row_series.to_dict()} | "
                f"data_head=\\n{data.head(10).to_string()}"
            )
            raise ValueError(
                "daily 엑셀 파싱 실패: 선택된 행은 0원이지만 같은 날짜 원본에 0이 아닌 금액이 있습니다 "
                f"(sale_date={sale_date}, store={store_name}, selected={row_series.to_dict()})"
            )
```

## Test Cases

1. **문법 오류 없음** — `python -c "import ast; ast.parse(open('modules/transform/pipelines/db/DB_OKPOS_Sales.py').read()); print('OK')"` → 기대: `OK`

2. **Airflow 재수집 후 로그 확인** — Airflow에서 `DB_OKPOS_Sales` DAG Trigger 후 태스크 로그 검색:
   - `daily 0원 감지` 메시지 **없으면** Fix B 정상 동작 (0원 자체가 해결됨)
   - `daily 0원 감지` 메시지 **있으면** 로그의 `columns` / `selected_row` / `data_head` 분석

3. **CSV 값 확인** — 재수집 완료 후:
   ```python
   from modules.transform.utility.paths import RAW_OKPOS_SALES
   import pandas as pd
   p = RAW_OKPOS_SALES / "brand=도리당" / "store=송파삼전점" / "ym=2026-06" / "okpos_daily.csv"
   df = pd.read_csv(p, dtype=str)
   print(df[df["sale_date"] >= "2026-06-10"][["sale_date","총매출액","실매출액","영수건수"]])
   ```
   기대: 총매출액/실매출액 > 0, 영수건수 > 0

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 로그의 columns / selected_row / data_head 확인 후 원인 분석
  3. 코드 수정 (aliases 추가 또는 행 선택 로직 보완)
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS
```

## Constraints

- 진단 로깅은 버그 수정 후에도 유지 (다음 이상 징후 조기 감지용)
- `nonzero_source_found` 분기와 `raise ValueError`는 건드리지 않음 — Fix A는 그 앞에 무조건 logging만 추가
- `shopOpen` 변경은 이미 완료(Fix B) — 재수정 금지

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- logger.warning 레벨 — 기본 `warning`; 로그에 보이지 않으면 `error`로 레벨 업
- 주석 여부: WHY만 한 줄, WHAT 설명 금지
