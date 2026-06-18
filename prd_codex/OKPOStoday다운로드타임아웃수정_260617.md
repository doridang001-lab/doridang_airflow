# OKPOStoday다운로드타임아웃수정

## Task
`DB_OKPOS_Sales_Dags`의 `download_today` task가 100% 실패하고 있다.
원인은 OKPOS 페이지가 "조회된 데이터가 없습니다."를 표시할 때 JS `exportDetailSheet()`를 호출해도 다운로드가 시작되지 않는데, 현재 코드가 이를 사전 체크하지 않아 30초 no-activity 타임아웃이 발생하는 것이다.
빈 조회 결과를 JS 실행 전에 감지해 즉시 `"__NO_DATA__"`로 반환하도록 수정한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- NO_DATA 반환 규칙: `"__NO_DATA__"` 문자열 반환 → 호출부에서 `results[key] = "__NO_DATA__"` 처리
- 주석: WHY만, WHAT 설명 금지

## Files to Create / Modify
- **수정**: `C:\airflow\modules\transform\pipelines\db\DB_OKPOS_Sales.py`
  - 함수: `_download_excel_for_store`
  - Step 3 완료(line 1367 `logger.info(f"{attempt_tag} 조회 완료")`) 직후,
    Step 4 시작(line 1369 `# ── 4. 엑셀 다운로드`) 직전에 3줄 삽입

## Implementation Steps
1. `C:\airflow\modules\transform\pipelines\db\DB_OKPOS_Sales.py` 열기
2. `_download_excel_for_store` 함수에서 아래 라인을 찾는다:
   ```python
   logger.info(f"{attempt_tag} 조회 완료")

           # ── 4. 엑셀 다운로드 ─────────────────────────────────────────────
   ```
3. 두 블록 사이에 아래 3줄을 삽입한다:
   ```python
   logger.info(f"{attempt_tag} 조회 완료")

           # ── 3.5: 빈 조회 결과 사전 감지 ──────────────────────────────────
           if "조회된 데이터가 없습니다" in driver.page_source:
               logger.info(f"{attempt_tag} 조회 데이터 없음 → NO_DATA 처리")
               return "__NO_DATA__"

           # ── 4. 엑셀 다운로드 ─────────────────────────────────────────────
   ```
4. 파일 저장

## Reference Code
### DB_OKPOS_Sales.py — 삽입 위치 전후 (line 1339~1395)
```python
            # ── 3. 조회 버튼 클릭 ────────────────────────────────────────────
            logger.info(f"{attempt_tag} 조회 버튼 클릭")
            _dismiss_alert(driver)
            _click_search_btn(driver, wait)
            time.sleep(2)
            alert = _dismiss_alert(driver)
            if alert:
                logger.error(f"{attempt_tag} 조회 후 alert 발생: {alert!r}")
                if "매장" in alert:
                    # ... 팝업 닫기 처리 ...
                    pass
                raise TimeoutException(f"조회 alert: {alert}")
            logger.info(f"{attempt_tag} 조회 완료")

            # ▼▼▼ 여기에 삽입 ▼▼▼
            # ── 3.5: 빈 조회 결과 사전 감지 ──────────────────────────────────
            if "조회된 데이터가 없습니다" in driver.page_source:
                logger.info(f"{attempt_tag} 조회 데이터 없음 → NO_DATA 처리")
                return "__NO_DATA__"
            # ▲▲▲ 여기까지 ▲▲▲

            # ── 4. 엑셀 다운로드 ─────────────────────────────────────────────
            existing = {
                p for p in download_dir.iterdir()
                if p.is_file() and p.suffix.lower() not in {".crdownload", ".tmp", ".part"}
            }
            logger.info(f"{attempt_tag} 엑셀 다운로드 JS 실행: {page_cfg['excel_js']}")
            _dismiss_alert(driver)
            driver.execute_script(page_cfg["excel_js"])
            time.sleep(1)
            _dismiss_alert(driver)

            # ── 5. 다운로드 완료 대기 ─────────────────────────────────────────
            downloaded = _wait_for_download(download_dir, existing)
            if downloaded is None:
                raise TimeoutException("다운로드 타임아웃")

            suffix = downloaded.suffix.lower()
            renamed = download_dir / f"{sale_date}__{page_type_key}__{store_slug}{suffix}"
            shutil.move(str(downloaded), str(renamed))
            logger.info(f"{attempt_tag} 다운로드 완료: {renamed.name}")
            return renamed
```

## Test Cases
1. **구문 검증** `python -c "import ast; ast.parse(open('modules/transform/pipelines/db/DB_OKPOS_Sales.py').read()); print('OK')"` → 기대: `OK`
2. **import 검증** `python -c "from modules.transform.pipelines.db.DB_OKPOS_Sales import download_today_stores; print('OK')"` → 기대: `OK` (ImportError 없음)
3. **DAG import** `python -c "from dags.db.DB_OKPOS_Sales_Dags import dag; print('OK')"` → 기대: `OK`
4. **Airflow 실행**: Airflow UI → `DB_OKPOS_Sales_Dags` → `download_today` task 재실행
   - 기대 로그: `조회 데이터 없음 → NO_DATA 처리` 메시지 포함
   - 기대 완료 메시지: `today 다운로드 완료: X/5건 (데이터없음: Y건)` Y > 0, RuntimeError 없음
5. **후속 task**: `check_and_fill_missing_today` 정상 완료 확인

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1~3 순서대로 실행 (정적 검증)
  2. FAIL 항목 → 원인 분석 후 코드 수정
  3. Test Case 4~5 Airflow 실행 검증
종료 조건: 전체 PASS + RuntimeError 없음
```

## Constraints
- `_download_excel_for_store` 함수의 기존 Step 1~5 구조와 번호 주석 변경 금지
- `"__NO_DATA__"` 반환값 문자열은 기존 컨벤션 그대로 유지 (대소문자, 언더스코어 포함)
- `driver.page_source` 체크는 `time.sleep(2)` 이후이므로 페이지 로드 대기 불필요
- 기존 Step 3의 alert 처리 로직(`TimeoutException(f"조회 alert: {alert}")`) 변경 금지
- `download_today_stores`, `_download_with_retry` 등 호출부 변경 불필요 (기존에 `__NO_DATA__` 처리 로직 있음)

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
