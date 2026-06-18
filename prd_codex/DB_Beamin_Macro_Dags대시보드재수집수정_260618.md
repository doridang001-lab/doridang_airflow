# DB_Beamin_Macro_Dags 대시보드 + 재수집 수정

## Task
`DB_Beamin_Macro_Dags` DAG 관련 두 가지 버그를 수정한다.
① 대시보드 결과를 HTML 파일로 디스크에 저장하지 않고 localhost 서버에서만 서빙하도록 변경.
② `retry_once_failed()` 에서 orders·ads 재시도 후 여전히 실패한 항목에 대해 2차 재시도가 없는 문제를 수정.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 대시보드 파일 위치: `modules/transform/dashboard/`
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Modify
- `modules/transform/dashboard/db_beamin_macro_dashboard.py` — `SnapshotStore.persist()` HTML 저장 제거
- `modules/transform/pipelines/db/DB_Beamin_combined.py` — `retry_once_failed()` 2차 재시도 추가

## Implementation Steps

### 1. HTML 파일 저장 제거
**파일**: `modules/transform/dashboard/db_beamin_macro_dashboard.py`

`SnapshotStore.persist()` (라인 2222~2230) 에서 아래 두 곳을 수정:

**제거할 줄** (라인 2223):
```python
self.paths.html_path.write_text(str(snapshot.get("html") or ""), encoding="utf-8")
```

**제거할 블록** (라인 2229~2230):
```python
        if html_payload is None:
            logger.warning("Snapshot persisted without HTML payload.")
```

수정 후 `persist()` 최종 형태:
```python
def persist(self, snapshot: dict[str, Any]) -> None:
    payload = dict(snapshot)
    payload.pop("html", None)
    self.paths.json_path.write_text(_safe_json(payload), encoding="utf-8")
    self.paths.store_progress_path.write_text(_safe_json(payload.get("stores", [])), encoding="utf-8")
    self.paths.live_log_path.write_text(_safe_json(payload.get("live_logs", [])), encoding="utf-8")
```

> `html_payload` 변수 자체도 더 이상 필요 없으므로 제거.
> 서버의 `/db-beamin-macro` 핸들러(라인 2318~2321)는 메모리 snapshot["html"]을 그대로 반환하므로 동작에 영향 없음.

---

### 2. orders 2차 재시도 추가
**파일**: `modules/transform/pipelines/db/DB_Beamin_combined.py`

`retry_once_failed()` 함수 내 orders 처리 블록 (라인 686~698)을 수정:

**현재 코드**:
```python
    for item in failed.get("orders", []):
        account = item["account"]
        stores = item["stores"]
        try:
            result = collect_orders_for_account(
                account["account_id"], account["password"], stores, target_date=target_date
            )
            still_failed = result.get("failed", []) if isinstance(result, dict) else result
            logger.info(
                "orders 재시도 완료: %s (실패 %d건)", account["account_id"], len(still_failed)
            )
        except Exception as e:
            logger.error("orders 재시도 실패 [%s]: %s", account["account_id"], e)
```

**수정 후**:
```python
    for item in failed.get("orders", []):
        account = item["account"]
        stores = item["stores"]
        try:
            result = collect_orders_for_account(
                account["account_id"], account["password"], stores, target_date=target_date
            )
            still_failed = result.get("failed", []) if isinstance(result, dict) else result
            logger.info(
                "orders 재시도 완료: %s (실패 %d건)", account["account_id"], len(still_failed)
            )
            if still_failed:
                logger.info("orders 2차 재시도 %d건 (30초 대기)", len(still_failed))
                time.sleep(30)
                result2 = collect_orders_for_account(
                    account["account_id"], account["password"], still_failed, target_date=target_date
                )
                still2 = result2.get("failed", []) if isinstance(result2, dict) else result2
                logger.info(
                    "orders 2차 재시도 완료 %s: 잔여실패 %d건", account["account_id"], len(still2)
                )
        except Exception as e:
            logger.error("orders 재시도 실패 [%s]: %s", account["account_id"], e)
```

---

### 3. ads 2차 재시도 추가
**파일**: `modules/transform/pipelines/db/DB_Beamin_combined.py`

`retry_once_failed()` 함수 내 ads 처리 블록 (라인 700~711)을 수정:

**현재 코드**:
```python
    for item in failed.get("ads", []):
        account = item["account"]
        stores = item["stores"]
        try:
            collect_ad_funnel_for_account(
                account["account_id"], account["password"], stores, target_date=target_date
            )
            logger.info("ads 재시도 성공: %s", account["account_id"])
        except Exception as e:
            logger.error("ads 재시도 실패 [%s]: %s", account["account_id"], e)
```

**수정 후**:
```python
    for item in failed.get("ads", []):
        account = item["account"]
        stores = item["stores"]
        try:
            collect_ad_funnel_for_account(
                account["account_id"], account["password"], stores, target_date=target_date
            )
            logger.info("ads 재시도 성공: %s", account["account_id"])
        except Exception as e:
            logger.warning("ads 재시도 1차 실패, 30초 후 2차 시도 [%s]: %s", account["account_id"], e)
            time.sleep(30)
            try:
                collect_ad_funnel_for_account(
                    account["account_id"], account["password"], stores, target_date=target_date
                )
                logger.info("ads 2차 재시도 성공: %s", account["account_id"])
            except Exception as e2:
                logger.error("ads 2차 재시도도 실패 [%s]: %s", account["account_id"], e2)
```

## Reference Code

### SnapshotStore.persist() — 현재 코드 (db_beamin_macro_dashboard.py:2217-2231)
```python
class SnapshotStore:
    def __init__(self, root: Path | str = DASHBOARD_DB):
        self.paths = DashboardPaths(Path(root))
        self.paths.root.mkdir(parents=True, exist_ok=True)

    def persist(self, snapshot: dict[str, Any]) -> None:
        self.paths.html_path.write_text(str(snapshot.get("html") or ""), encoding="utf-8")
        payload = dict(snapshot)
        html_payload = payload.pop("html", None)
        self.paths.json_path.write_text(_safe_json(payload), encoding="utf-8")
        self.paths.store_progress_path.write_text(_safe_json(payload.get("stores", [])), encoding="utf-8")
        self.paths.live_log_path.write_text(_safe_json(payload.get("live_logs", [])), encoding="utf-8")
        if html_payload is None:
            logger.warning("Snapshot persisted without HTML payload.")
```

### retry_once_failed() — 현재 코드 (DB_Beamin_combined.py:633-713)
```python
def retry_once_failed(failed: dict, target_date: str | None = None) -> str:
    """실패한 계정/매장만 1회 재시도."""
    n_accounts = len(failed.get("accounts", []))
    n_stores   = len(failed.get("stores", []))
    n_orders   = len(failed.get("orders", []))
    n_ads      = len(failed.get("ads", []))
    logger.info("재시도 시작: accounts=%d stores=%d orders=%d", n_accounts, n_stores, n_orders)

    # ... accounts / stores 처리 (변경 없음) ...

    for item in failed.get("orders", []):
        account = item["account"]
        stores  = item["stores"]
        try:
            result = collect_orders_for_account(
                account["account_id"], account["password"], stores, target_date=target_date
            )
            still_failed = result.get("failed", []) if isinstance(result, dict) else result
            logger.info("orders 재시도 완료: %s (실패 %d건)", account["account_id"], len(still_failed))
        except Exception as e:
            logger.error("orders 재시도 실패 [%s]: %s", account["account_id"], e)

    for item in failed.get("ads", []):
        account = item["account"]
        stores  = item["stores"]
        try:
            collect_ad_funnel_for_account(
                account["account_id"], account["password"], stores, target_date=target_date
            )
            logger.info("ads 재시도 성공: %s", account["account_id"])
        except Exception as e:
            logger.error("ads 재시도 실패 [%s]: %s", account["account_id"], e)

    return f"재시도 완료: accounts={n_accounts} stores={n_stores} orders={n_orders} ads={n_ads}"
```

## Test Cases
1. **HTML 파일 미생성 확인**: 대시보드 서버를 기동하거나 DAG 트리거 후 `DASHBOARD_DB/` 경로에 `db_beamin_macro_dashboard.html` 파일이 생성되지 않는지 확인
   ```python
   from modules.transform.utility.paths import ANALYTICS_DB
   # 또는 DASHBOARD_DB 경로를 직접 확인
   ```
2. **JSON 3개 파일은 정상 생성**: `db_beamin_macro_snapshot.json`, `db_beamin_macro_store_progress.json`, `db_beamin_macro_live_log.json` 존재 확인
3. **localhost 서빙 정상**: `http://localhost:8787/db-beamin-macro` 접속 시 대시보드 HTML 정상 반환
4. **2차 재시도 로그 확인**: 다음 DAG 실행 로그에서 `orders 2차 재시도` 또는 `ads 재시도 1차 실패, 30초 후 2차 시도` 로그라인이 출력되는지 확인
5. **import 오류 없음**:
   ```bash
   python -c "from modules.transform.dashboard.db_beamin_macro_dashboard import SnapshotStore; print('ok')"
   python -c "from modules.transform.pipelines.db.DB_Beamin_combined import retry_once_failed; print('ok')"
   ```

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 5번 (import 오류) 먼저 확인
  2. 대시보드 서버 기동 → HTML 파일 미생성 확인
  3. FAIL 항목 → 원인 분석 후 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- `html_path` 관련 코드는 `DashboardPaths.html_path` 프로퍼티는 **삭제하지 말 것** (다른 곳에서 참조 가능)
- `payload.pop("html", None)` 은 JSON 저장에서 HTML 키를 제외하기 위해 **유지**
- `time` 모듈은 `DB_Beamin_combined.py` 상단에 이미 import 되어 있음 — 추가 import 불필요
- stores 레벨 재시도는 변경하지 않음 (드라이버 내부 재시도 이미 존재)
- 기존 반환 타입 `str` 유지 — `retry_once_failed()` 시그니처 변경 금지

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
