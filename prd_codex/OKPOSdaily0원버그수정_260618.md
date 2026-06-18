# OKPOS daily 0원 버그 수정

## Task

`download_daily` 태스크가 2026-06-10 이후 모든 매장/날짜에서 총매출액·총할인액·실매출액·영수건수 전부 0을 저장하는 버그를 수정한다.  
에러 없이 0이 저장되므로 **잘못된 Excel 뷰 또는 행 선택** 문제로 추정 — `shopOpen` 체크박스가 POS 단말기 분리 뷰를 켜서 매장명 매칭이 실패하는 가설이 가장 유력하다.  
진단 로깅을 먼저 추가하고, shopOpen 동작을 반전(언체크)한 뒤, 6/10~6/17 데이터를 강제 재수집한다.

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...`

## Files to Modify

- `modules/transform/pipelines/db/DB_OKPOS_Sales.py`
  - **Fix A** (line ~2081): `_transform_okpos_daily_df` — 0원 감지 시 진단 로깅 추가
  - **Fix B** (line ~1331): `_download_excel_for_store` — `shopOpen` 체크박스 언체크로 반전

## Implementation Steps

### Fix A — 진단 로깅 (line 2081)

`if out_amount_sum == 0:` 블록 **맨 앞**에 아래 `logger.warning` 한 줄 삽입.

현재 코드:
```python
    out_amount_sum = int(out[["총매출액", "총할인액", "실매출액", "영수건수"]].sum(axis=1).iloc[0])
    if out_amount_sum == 0:
        amount_cols = [c for c in ("총매출액", "총할인액", "실매출액", "영수건수") if c in data.columns]
```

변경 후:
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

### Fix B — shopOpen 언체크 (line 1327~1337)

현재 코드 (`if not checked` → 체크 강제):
```python
            if "shop_open_checkbox_id" in page_cfg:
                try:
                    cb = driver.find_element(By.ID, page_cfg["shop_open_checkbox_id"])
                    # ensure checked
                    checked = driver.execute_script("return arguments[0].checked === true;", cb)
                    if not checked:
                        driver.execute_script("arguments[0].click();", cb)
                        time.sleep(0.2)
                except Exception:
                    # 없어도 진행
                    pass
```

변경 후 (`if checked` → 언체크 강제, 즉 POS 단말기 분리 OFF):
```python
            if "shop_open_checkbox_id" in page_cfg:
                try:
                    cb = driver.find_element(By.ID, page_cfg["shop_open_checkbox_id"])
                    # ensure unchecked: 체크 시 POS 단말기 분리 뷰가 켜져 매장명 매칭 실패 발생
                    checked = driver.execute_script("return arguments[0].checked === true;", cb)
                    if checked:
                        driver.execute_script("arguments[0].click();", cb)
                        time.sleep(0.2)
                except Exception:
                    pass
```

## Reference Code

### DB_OKPOS_Sales.py — 수정 대상 컨텍스트

```python
# _transform_okpos_daily_df 내부 (line 2069~2101)
# out 생성 후 0원 검증 블록
    out = pd.DataFrame([{
        "sale_date": sale_date,
        "ym": sale_date[:7],
        "매장명": store_name,
        "총매출액": _amount_from_row(row_series, "총매출액"),
        "총할인액": _amount_from_row(row_series, "총할인액"),
        "실매출액": _amount_from_row(row_series, "실매출액"),
        "영수건수": _amount_from_row(row_series, "영수건수"),
        "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }])

    out_amount_sum = int(out[["총매출액", "총할인액", "실매출액", "영수건수"]].sum(axis=1).iloc[0])
    if out_amount_sum == 0:
        # ← Fix A: 이 위치에 logger.warning 추가
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
            raise ValueError(
                "daily 엑셀 파싱 실패: 선택된 행은 0원이지만 같은 날짜 원본에 0이 아닌 금액이 있습니다 "
                f"(sale_date={sale_date}, store={store_name}, selected={row_series.to_dict()})"
            )

# _download_excel_for_store 내부 (line 1326~1337)
# ← Fix B: 이 블록 전체 교체
            if "shop_open_checkbox_id" in page_cfg:
                try:
                    cb = driver.find_element(By.ID, page_cfg["shop_open_checkbox_id"])
                    checked = driver.execute_script("return arguments[0].checked === true;", cb)
                    if not checked:   # ← 여기를 "if checked:" 로 변경
                        driver.execute_script("arguments[0].click();", cb)
                        time.sleep(0.2)
                except Exception:
                    pass
```

## Test Cases

Fix 적용 후 Airflow에서 `download_daily` 태스크를 아래 conf로 강제 재실행:

```json
{"force_redownload_daily": true, "sale_date_from": "2026-06-10", "sale_date_to": "2026-06-17"}
```

검증 순서:

1. **로그 확인** — `daily 0원 감지` 메시지가 없으면 0값 미발생 → Fix B 효과 확인  
2. **CSV 확인** (Python):
   ```python
   import pandas as pd
   from modules.transform.utility.paths import RAW_OKPOS_SALES
   p = RAW_OKPOS_SALES / "brand=도리당" / "store=송파삼전점" / "ym=2026-06" / "okpos_daily.csv"
   df = pd.read_csv(p, dtype=str)
   print(df[df["sale_date"] >= "2026-06-10"][["sale_date","총매출액","실매출액","영수건수"]])
   ```
   기대: 총매출액/실매출액 > 0, 영수건수 > 0

3. **reconcile 태스크** — `reconcile_against_daily_summary` 정상 완료 확인

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL → 로그의 columns / selected_row 확인 후 원인 분석
  3. 코드 수정 (aliases 추가 또는 행 선택 로직 보완)
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS
```

## Constraints

- **0 값이 저장된 기존 행 반드시 교체** — `force_redownload_daily` conf 사용, `_upsert_by_date`/`replace_by_date` 동작 확인
- `shopOpen` 변경은 daily 페이지 전용 (`PAGE_TYPES["daily"]`에만 `shop_open_checkbox_id` 키 존재)
- today/receipt 페이지는 `shop_open_checkbox_id` 키 없음 → 미영향
- 진단 로깅은 버그 수정 후에도 유지 (다음 이상 징후 조기 감지용)

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- `if checked:` vs `if not checked:` 확신 없으면: **Fix B 먼저 적용 → 로그 확인 → 아니면 되돌리기**
- Fix A 로그가 Airflow 로그에 보이지 않으면: `logger.warning` → `logger.error`로 레벨 업
- 주석 여부: WHY만 한 줄, WHAT 금지
