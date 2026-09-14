# OKPOS sale_date 밀림 수정

## Task

`DB_OKPOS_Sales_Today_Dags`의 08시(KST) 슬롯이 **전일 영업일 데이터에 오늘 날짜(`sale_date`)를 붙여** 저장하고 있다.
Today DAG는 `OKPOS_USE_DEFAULT_QUERY_DATE=1`로 today/receipt 페이지에 날짜 필터를 세팅하지 않고 POS 기본 조회일자를 그대로 긁는데,
KST 08시엔 OKPOS 화면이 아직 전일 마감분을 보여준다. 그런데 `sale_date`는 `pendulum.now("Asia/Seoul")`(=오늘)로 스탬핑된다.

결과: 전일 매출이 오늘 날짜로 새고, 전일은 21시 슬롯 시점의 부분 스냅샷에서 영구히 멈춘다.
`DB_OKPOS_Sales`(lookback 120일)는 이미 파일이 있는 (날짜, 매장)을 skip하므로 정정되지 않는다.

**해결: 날짜 필터를 세팅하지 않을 때, 화면의 조회일자 입력 필드 값을 읽어 실제 영업일을 `sale_date`로 채택한다.**

### 근거 (실측)

- `sale_date=2026-07-09` order 24행의 `collected_at`이 전부 `2026-07-08 23:05` (컨테이너 UTC → KST 7/9 08:05)
- `sale_date=2026-07-08` order 21행이 7/9의 24행에 **완전 포함** (21/21 일치)
- 동일 반품 영수증 14번(`-73,900`, 결제시각 `18:38:59`)이 7/8·7/9 양일 중복
- 4개 매장(동두천지행·삼송·송파삼전·평택비전) 전부 7/9 `collected_at` 동일

송파삼전점 7월 피해:

| 날짜 | 적재 | 실제 | 차이 |
|---|---|---|---|
| 7/7 | 676,800 | 778,600 | −101,800 (23:02:38 결제 영수증 23번, order엔 없고 item엔 존재) |
| 7/8 | 672,900 | 719,800 | −46,900 |
| 7/9 | 719,800 | 7/8 복제본 | 허수 |

### 설계 판단 (중요)

엑셀에서 날짜 역산은 **불가능**하다. `okpos_order.csv`에 날짜 컬럼이 없고 `결제시각`은 `HH:MM:SS`뿐이다.
(`_infer_sale_date_from_xlsx`는 daily 리포트 전용 — 상단 "조회일자" 문자열 파싱)

today/receipt는 `PAGE_TYPES`에 `date_input_id`를 갖고 있다(`datepicker-input` / `saleDate`).
**그 필드의 현재 값을 읽는 것이 POS가 실제로 보여주는 데이터와 일치하는 유일한 권위 있는 값이다.**

## Project Conventions

- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify

- **수정** `modules/transform/pipelines/db/DB_OKPOS_Sales.py`
  - `_read_query_date` 신설 (`_select_date_tui` 바로 아래, 약 1300행대)
  - `_download_excel_for_store` 날짜 분기(2059-2080행) + rename(2171-2177행)
  - `download_today_stores._session`(2320-2336행)
  - `download_receipt_stores._session`(2379-2395행)
  - `_transform_okpos_df`의 `collected_at`(2773행)
- **수정** `dags/db/DB_OKPOS_Sales_Today_Dags.py` — `resolve_today` 주석 보강 (로직 변경 없음)

## Implementation Steps

### 1. `_read_query_date` 신설 — `DB_OKPOS_Sales.py`

`_select_date_tui`(1304행)의 역함수. 같은 셀렉터 목록을 재사용한다.

```python
def _read_query_date(driver: uc.Chrome, date_input_id: str) -> str | None:
    """날짜 입력 필드의 현재 값을 YYYY-MM-DD로 읽는다. 실패 시 None."""
    input_el = None
    for sel in [
        (By.ID, date_input_id),
        (By.NAME, date_input_id),
        (By.CSS_SELECTOR, f"[id='{date_input_id}']"),
    ]:
        try:
            input_el = WebDriverWait(driver, 5).until(EC.presence_of_element_located(sel))
            break
        except TimeoutException:
            continue
    if input_el is None:
        return None

    raw = input_el.get_attribute("value") or ""
    if not raw.strip():
        try:
            raw = driver.execute_script(
                "var e=document.getElementById(arguments[0]); return e ? e.value : '';",
                date_input_id,
            ) or ""
        except Exception:
            raw = ""

    m = re.search(r"(\d{4})[./-](\d{2})[./-](\d{2})", str(raw))
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None
```

### 2. 실제 영업일 채택 — `_download_excel_for_store`

`use_default_query_date`(2061-2064행)가 참인 분기에서 `_read_query_date`를 호출한다.
**함수 시그니처는 건드리지 않는다.** 반환하는 파일 경로의 이름만 실제 날짜로 바뀐다.

```python
effective_sale_date = sale_date
if use_default_query_date:
    probed = _read_query_date(driver, page_cfg["date_input_id"])
    if probed:
        effective_sale_date = probed
        if probed != sale_date:
            logger.warning(
                "%s POS 기본 조회일자가 요청일과 다름: requested=%s actual=%s",
                attempt_tag, sale_date, probed,
            )
    else:
        logger.warning("%s 조회일자 읽기 실패 → 요청일 사용: %s", attempt_tag, sale_date)
    logger.info("%s Today DAG 기본 조회일자 사용: page=%s, sale_date=%s",
                attempt_tag, page_type_key, effective_sale_date)
elif ...  # 기존 daily/today 분기 그대로
```

2174행 rename을 `effective_sale_date`로 교체:

```python
renamed = download_dir / f"{effective_sale_date}__{page_type_key}__{store_slug}{suffix}"
```

`_quarantine_empty_okpos_download`(2155행) 등 디버그 경로는 `sale_date` 그대로 둔다 (요청 추적용).

### 3. 호출부 XCom key를 실제 영업일로

`download_today_stores._session`(2320-2336행)과 `download_receipt_stores._session`(2379-2395행)은
`key = f"{sale_date}__{store['name']}"`를 **다운로드 전에** 만든다.
`_current_key`(retry 추적용)는 요청 날짜 그대로 두고, `results`에 넣는 key만 실제 날짜로 바꾼다.

```python
downloaded = _download_excel_for_store(
    driver, wait, "today", page_cfg, sale_date, store, download_dir
)
_current_key[0] = None
if downloaded == "__NO_DATA__":
    results[key] = "__NO_DATA__"
elif downloaded:
    actual_date = Path(downloaded).name.split("__", 1)[0]   # {sale_date}__{page}__{slug}{suffix}
    results[f"{actual_date}__{store['name']}"] = str(downloaded)
```

`receipt`도 동일 패턴 (page_type만 `"receipt"`).

`save_to_raw`가 key를 `sale_date__store`로 파싱하므로(3411행),
이 한 곳만 맞추면 저장 경로 / `ym` / `replace_by_date` / `_transform_okpos_df`의 `df["sale_date"]`가 전부 따라온다.

### 4. `collected_at`을 KST로 통일 — 2773행

```python
df["collected_at"] = _kst_now().strftime("%Y-%m-%d %H:%M:%S")
```

`_transform_okpos_daily_df`(2792행~) 등 다른 `datetime.now()` 사용처도 함께 점검해 `_kst_now()`로 교체.
기존 CSV의 `collected_at`은 UTC로 남아 있으나 **소급 변환하지 않는다** (로그성 컬럼, `_pk` 계산에 미포함).

### 5. Today DAG 주석 보강 — `dags/db/DB_OKPOS_Sales_Today_Dags.py`

`resolve_today`(49-54행)의 `sale_date`는 이제 "요청 힌트"이고 실제 저장 날짜는 화면 조회일자라는 점을 주석으로 남긴다.
**로직 변경 없음.**

## Reference Code

### modules/transform/pipelines/db/DB_OKPOS_Sales.py — PAGE_TYPES (69-90행)

```python
PAGE_TYPES = {
    "today": {
        "url": "https://my.okpos.co.kr/asp/sales/v2/today",
        "date_input_id": "datepicker-input",
        "excel_js": "exportDetailSheet()",
    },
    "receipt": {
        "url": "https://my.okpos.co.kr/asp/sales/v2/receipt/details",
        "date_input_id": "saleDate",
        "excel_js": "exportReceiptDetailExcel()",
    },
    # 일자별 종합매출 (실매출 기준의 공식 집계 리포트)
    "daily": {
        "url": "https://my.okpos.co.kr/asp/sales/v2/daily",
        # daily는 기간 조회: startDate/endDate 두 입력을 각각 세팅
        "start_date_input_id": "startDate",
        "end_date_input_id": "endDate",
        "shop_open_checkbox_id": "shopOpen",
        "excel_js": "exportSheet()",
    },
}
```

### `_kst_now` (43-44행)

```python
def _kst_now() -> datetime:
    return datetime.now(ZoneInfo("Asia/Seoul")).replace(tzinfo=None)
```

### `_select_date_tui` — 셀렉터 패턴 재사용 대상 (1304-1330행)

```python
def _select_date_tui(driver: uc.Chrome, wait: WebDriverWait, sale_date: str, date_input_id: str) -> None:
    """OKPOS 날짜 입력 필드에 날짜 설정 (다양한 방식 순서대로 시도)"""
    year, month, day = sale_date.split("-")
    date_slash = f"{year}/{month}/{day}"    # YYYY/MM/DD
    date_dot   = f"{year}.{month}.{day}"    # YYYY.MM.DD
    date_short = f"{year}-{month}-{day}"    # YYYY-MM-DD

    input_el = None
    for sel in [
        (By.ID, date_input_id),
        (By.NAME, date_input_id),
        (By.CSS_SELECTOR, f"[id='{date_input_id}']"),
    ]:
        try:
            input_el = WebDriverWait(driver, 5).until(EC.presence_of_element_located(sel))
            break
        except TimeoutException:
            continue
    if input_el is None:
        raise TimeoutException(f"날짜 입력 필드를 찾을 수 없습니다: id={date_input_id}")

    el_type  = input_el.get_attribute("type") or ""
    el_value = input_el.get_attribute("value") or ""
    logger.info(f"날짜 입력 필드 발견: id={date_input_id}, type={el_type}, current_value={el_value}")
```

### `_download_excel_for_store` — 날짜 분기 (2059-2080행)

```python
            # ── 1. 날짜 선택 ─────────────────────────────────────────────────
            logger.info(f"{attempt_tag} 날짜 선택 시작")
            use_default_query_date = (
                page_type_key in {"today", "receipt"}
                and os.getenv("OKPOS_USE_DEFAULT_QUERY_DATE", "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}
            )
            if use_default_query_date:
                logger.info(
                    "%s Today DAG 기본 조회일자 사용: page=%s, sale_date=%s",
                    attempt_tag,
                    page_type_key,
                    sale_date,
                )
            # daily는 기간 조회(start/end)로 동작
            elif "start_date_input_id" in page_cfg and "end_date_input_id" in page_cfg:
                _select_date_tui(driver, wait, sale_date, page_cfg["start_date_input_id"])
                _dismiss_alert(driver)
                _select_date_tui(driver, wait, sale_date, page_cfg["end_date_input_id"])
            else:
                _select_date_tui(driver, wait, sale_date, page_cfg["date_input_id"])
            _dismiss_alert(driver)
            logger.info(f"{attempt_tag} 날짜 선택 완료")
```

### `_download_excel_for_store` — rename (2171-2177행)

```python
            # 즉시 rename — 다음 매장 다운로드 시 동일 파일명 충돌 방지
            # (Chrome은 같은 파일명으로 덮어쓰므로 existing_files에서 새 파일로 감지 불가)
            suffix = downloaded.suffix.lower()
            renamed = download_dir / f"{sale_date}__{page_type_key}__{store_slug}{suffix}"
            shutil.move(str(downloaded), str(renamed))
            logger.info(f"{attempt_tag} 다운로드 완료: {renamed.name}")
            return renamed
```

### `download_today_stores._session` (2320-2336행)

```python
    def _session(driver, wait, pending, results):
        _login(driver, wait)
        _setup_download_dir(driver, download_dir)
        driver.get(page_cfg["url"])
        time.sleep(2)
        for sale_date, store in pending:
            key = f"{sale_date}__{store['name']}"
            _current_key[0] = key
            downloaded = _download_excel_for_store(
                driver, wait, "today", page_cfg, sale_date, store, download_dir
            )
            _current_key[0] = None
            if downloaded == "__NO_DATA__":
                results[key] = "__NO_DATA__"
            elif downloaded:
                results[key] = str(downloaded)
```

`download_receipt_stores._session`(2379-2395행)도 `"today"` → `"receipt"` 외 완전히 동일.

### `save_to_raw` — key 파싱 (3406-3414행)

```python
    for page_type, (files, csv_name) in page_map.items():
        for key, src_str in files.items():
            # key format: {sale_date}__{store_name}
            try:
                sale_date, store_name = key.split("__", 1)
            except Exception:
                sale_date, store_name = "unknown", key
            store_short = store_name.replace("도리당 ", "", 1)
            ym_guess = sale_date[:7] if sale_date and sale_date != "unknown" else None
```

### `_transform_okpos_df` — 스탬핑 (2767-2773행)

```python
    ym = sale_date[:7]  # YYYY-MM
    n_orig = len(df.columns)

    df["매장명"]       = store_name
    df["sale_date"]    = sale_date
    df["ym"]           = ym
    df["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
```

## Test Cases

1. [모듈 import] `python -c "from modules.transform.pipelines.db.DB_OKPOS_Sales import _read_query_date; print('ok')"` → 기대: `ok`
2. [DAG import] `python -c "from dags.db.DB_OKPOS_Sales_Today_Dags import dag; print(dag.dag_id)"` → 기대: `DB_OKPOS_Sales_Today_Dags`, ImportError 없음
3. [DAG import] `python -c "from dags.db.DB_OKPOS_Sales_Dags import dag; print(dag.dag_id)"` → 기대: `DB_OKPOS_Sales_Dags`
4. [DAG 파싱] `airflow dags list-import-errors` → 기대: OKPOS 관련 에러 없음
5. [날짜 파싱 단위] `python -c "import re; m=re.search(r'(\d{4})[./-](\d{2})[./-](\d{2})', '2026/07/08'); print(f'{m.group(1)}-{m.group(2)}-{m.group(3)}')"` → 기대: `2026-07-08`
6. [collected_at TZ] `python -c "from modules.transform.pipelines.db.DB_OKPOS_Sales import _kst_now; import datetime; print((_kst_now()-datetime.datetime.utcnow()).total_seconds()>28800-60)"` → 기대: `True`
7. [print 금지] `python -c "import re,pathlib; s=pathlib.Path('modules/transform/pipelines/db/DB_OKPOS_Sales.py').read_text(encoding='utf-8'); print([l for l in s.splitlines() if re.match(r'^\s*print\(', l)])"` → 기대: `[]`
8. [datetime.now 잔존 확인] `python -c "import pathlib; s=pathlib.Path('modules/transform/pipelines/db/DB_OKPOS_Sales.py').read_text(encoding='utf-8'); print(s.count('datetime.now()'))"` → 기대: 수정 전보다 감소, `collected_at` 라인엔 0건

## Verification Loop

구현 완료 후 아래 루프를 **모든 Test Cases PASS까지** 반복한다.

```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

### 운영 검증 (코드 수정 후, 사람이 수행)

1. Today DAG를 08시대에 수동 실행 → 로그에 `POS 기본 조회일자가 요청일과 다름: requested=... actual=...` warning 확인
2. 저장 경로의 `sale_date`가 **전일**인지 확인
3. 재수집 후 `okpos_order.csv`의 `sale_date` × `max(collected_at)` 교차표 → 각 `sale_date`의 최대 `collected_at`이 익일 새벽(KST)
4. 송파삼전점 7/7 영수증 23번이 `okpos_order.csv`에 존재하고 그날 합계가 **778,600**
5. 반품 영수증 14번(`-73,900`)이 **7/8에만 1건** 존재
6. 7/1~7/9 전 날짜에서 `okpos_order` 합계 == `okpos_daily.실매출액` == unified `total_price`(source=okpos)
7. 4개 매장(동두천지행·삼송·송파삼전·평택비전) 전부에 대해 3~6 반복

## Data Repair (코드 수정 후, 별도 실행 — 자동 실행 금지)

1. **오염된 7/9 행 제거** — 4개 매장 `ANALYTICS_DB/okpos_sales_raw/brand=도리당/store=*/ym=2026-07/`의
   `okpos_order.csv`, `okpos_order_item.csv`에서 `sale_date == "2026-07-09"` 행 삭제 (7/8 복제본).
   `scripts/` 하위 일회성 스크립트로, **반드시 dry-run으로 삭제 대상 행 수를 먼저 출력**한 뒤 실행.
2. **7/7·7/8 재수집** — `DB_OKPOS_Sales` DAG (Today가 아닌 일반 DAG. 명시적 날짜 필터 사용)
   ```json
   {"sale_date_from":"2026-07-07","sale_date_to":"2026-07-08","force_redownload":true}
   ```
3. **stale sentinel 삭제** — `ym=2026-07/.no_data__okpos_order.txt`, `.no_data__okpos_order_item.txt`
   (7/6자로 남아 있으나 실제 CSV는 채워져 있음)
4. **unified 정정 재적재** — `DB_UnifiedSales` DAG를 날짜별로 실행
   `{"sale_date":"2026-07-07"}`, `{"sale_date":"2026-07-08"}`, `{"sale_date":"2026-07-09"}`
5. 7/9는 코드 수정 후 다음 Today 슬롯이 실제 7/9 데이터로 채운다.

## Constraints

- `_download_excel_for_store`의 **함수 시그니처를 바꾸지 말 것.** 반환 파일명만 실제 날짜로 바뀐다.
- `_current_key`(retry 추적)는 **요청 날짜 그대로** 유지. `results` key만 실제 날짜를 쓴다.
- `_quarantine_empty_okpos_download` 등 디버그 파일명은 `sale_date`(요청일) 유지.
- `daily` page_type은 이 변경의 영향을 받지 않는다 (`start_date_input_id`/`end_date_input_id` 기간조회).
- 기존 CSV의 `collected_at`(UTC)을 소급 변환하지 말 것. `_pk` 계산에 포함되지 않는 로그성 컬럼이다.
- `dags/db/DB_OKPOS_Sales_Today_Dags.py`는 **주석만** 수정. `resolve_today` 로직 변경 금지.
- Data Repair 섹션은 코드 수정과 분리해 실행한다. 코드 작업 중 OneDrive 실데이터를 건드리지 말 것.
- 테스트매장 금일/과거일 배달 source 분기(`DB_UnifiedSales_common.py:796-800`)는 **의도된 설계**다. 손대지 말 것.
- `제휴사주문`은 배달이 아니라 테이블번호가 붙은 홀 주문이다. platform 분류를 바꾸지 말 것.

## Do Not Ask — Decide Yourself

- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
