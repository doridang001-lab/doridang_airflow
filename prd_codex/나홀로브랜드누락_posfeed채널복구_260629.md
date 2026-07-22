# 나홀로 브랜드 누락 수정 (posfeed 채널 복구)

## Task
`DB_UnifiedSales` DAG가 "나홀로" 브랜드를 수집하지 못한다. "나홀로" 매출은 **오직 posfeed 채널**로만 unified_sales에 진입하는데, `build_posfeed` 태스크가 `_save_exclusion_log()`에서 깨진 누적 로그 CSV를 방어 없이 `pd.read_csv`로 읽다가 `ParserError`로 매번 실패해 posfeed 전체(나홀로 포함)가 적재되지 않는다. `_save_exclusion_log()`를 방어적으로 고쳐 로깅용 부가 산출물이 메인 적재 파이프라인을 죽이지 않게 한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)
- CSV 입출력: `encoding="utf-8-sig"`, `dtype=str`

## Files to Create / Modify
- **수정**: `modules/transform/pipelines/db/DB_UnifiedSales_posfeed.py` — `_save_exclusion_log()` 함수 (현재 359~379행) 단 하나. 그 외 함수/파일 변경 금지.

## Implementation Steps

1. **기존 로그 read 방어** (현재 375~377행)
   - `pd.read_csv(...)` 호출에 `on_bad_lines="skip"`를 추가하고, 전체를 `try/except`로 감싼다.
   - 읽기 실패(또는 빈 결과) 시 `logger.warning(...)`만 남기고 **기존 데이터는 빈 DataFrame으로 대체**해 신규 행만 저장한다. 이 함수는 절대 예외를 상위로 전파하지 않는다(로깅용 부가 산출물).
   - `on_bad_lines="skip"`는 깨진 행 1개만 드롭하고 나머지(265,778행)는 정상 로드됨을 이미 검증함.

2. **write 경화 (재오염 방지)** (현재 373행 `save_df` 생성 직후 ~ 379행 저장 전)
   - 저장 직전 문자열 컬럼 `menu_name`, `item_name`(존재하는 컬럼만)의 `\r`, `\n`을 공백으로 치환한다. 개행이 섞인 메뉴명이 들어와도 CSV가 깨지지 않게 한다.
   - 예: `save_df[c] = save_df[c].astype(str).str.replace(r"[\r\n]+", " ", regex=True)`

3. **수정 후 형태 (참고 구현)**
   ```python
   def _save_exclusion_log(excluded: pd.DataFrame, date_str: str, menu_map: dict) -> None:
       """블랙리스트로 제외된 행을 월별 CSV에 누적 저장 (append+dedup).

       로깅용 부가 산출물이므로 어떤 경우에도 예외를 전파해 메인 적재를 죽이지 않는다.
       """
       ym = date_str[:7]
       log_dir = EXCLUSION_LOG_BASE / f"ym={ym}"
       log_dir.mkdir(parents=True, exist_ok=True)
       log_path = log_dir / "exclusion_log.csv"

       exc = excluded.copy()
       exc["menu_name"] = (
           exc["order_id"].map(menu_map)
           .fillna(exc["메뉴명"].fillna("").astype(str) if "메뉴명" in exc.columns else "")
       )

       cols = ["sale_date", "brand", "store", "menu_name", "item_name", "qty", "unit_price", "total_price"]
       save_df = exc[[c for c in cols if c in exc.columns]].copy()

       # 개행 정화: 메뉴/상품명에 섞인 \r\n이 CSV를 깨뜨리지 않게 공백 치환
       for c in ("menu_name", "item_name"):
           if c in save_df.columns:
               save_df[c] = save_df[c].astype(str).str.replace(r"[\r\n]+", " ", regex=True)

       if log_path.exists():
           try:
               existing = pd.read_csv(
                   str(log_path), dtype=str, encoding="utf-8-sig", on_bad_lines="skip"
               )
               save_df = pd.concat([existing, save_df], ignore_index=True).drop_duplicates()
           except Exception as exc_read:
               logger.warning("exclusion_log 읽기 실패, 신규 행만 저장: %s | %s", log_path, exc_read)

       save_df.to_csv(str(log_path), index=False, encoding="utf-8-sig")
   ```

## Reference Code
### DB_UnifiedSales_posfeed.py (현재 _save_exclusion_log, 359~379행)
```python
def _save_exclusion_log(excluded: pd.DataFrame, date_str: str, menu_map: dict) -> None:
    """블랙리스트로 제외된 행을 월별 CSV에 누적 저장 (append+dedup)."""
    ym = date_str[:7]
    log_dir = EXCLUSION_LOG_BASE / f"ym={ym}"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "exclusion_log.csv"

    exc = excluded.copy()
    exc["menu_name"] = (
        exc["order_id"].map(menu_map)
        .fillna(exc["메뉴명"].fillna("").astype(str) if "메뉴명" in exc.columns else "")
    )

    cols = ["sale_date", "brand", "store", "menu_name", "item_name", "qty", "unit_price", "total_price"]
    save_df = exc[[c for c in cols if c in exc.columns]].copy()

    if log_path.exists():
        existing = pd.read_csv(str(log_path), dtype=str, encoding="utf-8-sig")
        save_df = pd.concat([existing, save_df], ignore_index=True).drop_duplicates()

    save_df.to_csv(str(log_path), index=False, encoding="utf-8-sig")
```

### 호출 지점 (현재 343~344행, 변경하지 말 것 — 참고용)
```python
if not _bl_excluded.empty:
    _save_exclusion_log(_bl_excluded, date_str, menu_map)
```

## Test Cases

1. [transform 크래시 해소 + 나홀로 적재] — 현재는 ParserError로 죽음, 수정 후 정상:
   ```
   python -c "from modules.transform.pipelines.db import DB_UnifiedSales_posfeed as P; o=P._load_orders_for_ym('2026-06'); i=P._load_items_for_ym('2026-06'); out=P._transform_df(o,i,'2026-06-27'); print('rows',len(out),'나홀로',int((out['brand']=='나홀로').sum()))"
   ```
   → 기대: 크래시 없이 `rows 2259 나홀로 238` 내외 출력 (나홀로 > 0)

2. [깨진 로그 방어 읽기] `on_bad_lines="skip"`로 깨진 ym=2026-06 로그가 정상 로드되는지:
   ```
   python -c "import pandas as pd; from modules.transform.pipelines.db.DB_UnifiedSales_posfeed import EXCLUSION_LOG_BASE; f=EXCLUSION_LOG_BASE/'ym=2026-06'/'exclusion_log.csv'; df=pd.read_csv(str(f),dtype=str,encoding='utf-8-sig',on_bad_lines='skip'); print('rows',len(df))"
   ```
   → 기대: `rows 265778` 내외 (예외 없음)

3. [회귀 - 정상 달] 깨지지 않은 다른 달(2026-05) transform도 멀쩡한지 1일자 확인:
   ```
   python -c "from modules.transform.pipelines.db import DB_UnifiedSales_posfeed as P; o=P._load_orders_for_ym('2026-05'); i=P._load_items_for_ym('2026-05'); print('ok' if not o.empty else 'no-data', len(P._transform_df(o,i,'2026-05-27')))"
   ```
   → 기대: 예외 없이 행 수 출력

4. [모듈 import] 문법/구문 오류 없는지:
   ```
   python -c "import modules.transform.pipelines.db.DB_UnifiedSales_posfeed; print('import ok')"
   ```
   → 기대: `import ok`

## Verification Loop
구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 1~4 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `_save_exclusion_log()` **단 하나만** 수정한다. `_transform_df`, 호출 지점(343~344행), 다른 함수는 건드리지 않는다.
- `_save_exclusion_log`는 어떤 경우에도 예외를 상위로 전파하지 않는다 (메인 적재 보호).
- `report_posfeed_exclusions`(470~474행)는 이미 try/except 방어되어 있으니 수정 불필요.
- 누락 데이터 복구(6월 posfeed/나홀로 재적재)는 **사용자가 DAG 백필 재실행**으로 처리 — Codex는 코드 수정 + 정적 검증까지만. (현재 `LOOKBACK_DAYS=None`이라 conf 없이 트리거하면 `backfill_posfeed()` 전체 재적재됨)
- CSV 인코딩은 기존대로 `utf-8-sig` 유지.

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
