# BeaminMacroDAG버그수정

## Task
`DB_Beamin_Macro_Dags.py`에서 두 가지 버그를 수정한다.
1. `load_accounts` 함수 186-187줄 한글 깨짐 (EUC-KR 바이트가 UTF-8 파일에 혼입)
2. `collect_all` 전 계정 Chrome `session not created: cannot connect to chrome` — Chrome 148 업데이트 후 UC 폴백 로직 미처리 케이스 발생

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/{domain}/`
- DAG 파일 위치: `dags/{domain}/`
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify
- `dags/db/DB_Beamin_Macro_Dags.py` — lines 186-187 한글 교정
- `modules/transform/utility/selenium_uc.py` — `session not created` 폴백 케이스 추가 (lines ~343 이후)

## Implementation Steps

### Fix 1 — 한글 깨짐 (`dags/db/DB_Beamin_Macro_Dags.py`)

`load_accounts` 함수 내 lines 186-187을 아래와 같이 교체한다.

```python
# 현재 (깨진 텍스트 — EUC-KR 혼입)
logger.info("怨꾩젙 濡쒕뱶 ?꾨즺: %d媛?-> %s", len(accounts), stores)
return f"怨꾩젙 {len(accounts)}媛? {stores}"

# 수정 후
logger.info("계정 로드 완료: %d개 -> %s", len(accounts), stores)
return f"계정 {len(accounts)}개, {stores}"
```

`old_string` 기준으로 Edit 툴 사용 — 두 줄만 변경, 나머지 함수 구조 보존.

---

### Fix 2 — Chrome 148 `session not created` (`modules/transform/utility/selenium_uc.py`)

현재 폴백 로직(line ~343)은 `RemoteDisconnected`, `Connection aborted`, `Current browser version is X` 만 처리한다.
`session not created` / `cannot connect to chrome` 에러는 처리하지 않아 standard chromedriver fallback으로 바로 넘어간다 — 이것도 버전 불일치로 실패.

**삽입 위치**: `RemoteDisconnected` 폴백 블록(lines 343-362) 바로 **다음**, `_emit_uc_log("browser launch failed ...")` 호출 **이전**.

삽입할 코드:
```python
        # session not created / cannot connect: UC 캐시+버전파일 삭제 후 재다운로드
        if "session not created" in err_str or "cannot connect to chrome" in err_str:
            _emit_uc_log(log_fn, "session not created: UC driver 캐시+버전파일 삭제 후 재시도")
            if data_dir:
                for fname in ("undetected_chromedriver", "chrome_version.txt"):
                    target = Path(data_dir) / fname
                    if target.exists():
                        try:
                            target.unlink()
                        except Exception:
                            pass
            configure_uc_data_path()
            retry_kwargs = {k: v for k, v in kwargs.items() if k != "driver_executable_path"}
            try:
                with _uc_launch_lock():
                    driver = uc.Chrome(**retry_kwargs)
                _apply_failfast_client(driver, log_fn=log_fn)
                return driver
            except Exception:
                pass
```

`old_string`으로 사용할 앵커 (현재 파일 line 364):
```python
        _emit_uc_log(
            log_fn,
            "browser launch failed "
```

이 앵커 **바로 앞**에 위 블록을 삽입한다.

## Reference Code

### selenium_uc.py — 기존 폴백 블록 전체 (lines 321-382)
```python
    # 네트워크 재시도로도 해결 안 됨 → 기존 폴백(version mismatch / 바이너리 오염) 처리
    if True:
        exc = last_exc
        err_str = str(exc)

        # version mismatch: 에러 메시지에서 올바른 버전 추출 후 재시도
        match = re.search(r"Current browser version is (\d+)", err_str)
        if match:
            retry_version = int(match.group(1))
            _emit_uc_log(
                log_fn,
                f"chromedriver/browser version mismatch detected; retry with version_main={retry_version}",
            )
            configure_uc_data_path()
            try:
                with _uc_launch_lock():
                    driver = uc.Chrome(options=options, version_main=retry_version)
                _apply_failfast_client(driver, log_fn=log_fn)
                return driver
            except Exception:
                pass

        # RemoteDisconnected / ProtocolError: 바이너리 오염 → 삭제 후 재시도(재다운로드)
        if "RemoteDisconnected" in err_str or "Connection aborted" in err_str:
            _emit_uc_log(log_fn, "RemoteDisconnected: UC driver binary 강제 삭제 후 재시도")
            if data_dir:
                driver_bin = Path(data_dir) / "undetected_chromedriver"
                if driver_bin.exists():
                    try:
                        driver_bin.unlink()
                    except Exception:
                        pass
            configure_uc_data_path()
            # 캐시 삭제 후 재다운로드해야 하므로 driver_executable_path 제거
            retry_kwargs = {k: v for k, v in kwargs.items() if k != "driver_executable_path"}
            try:
                with _uc_launch_lock():
                    driver = uc.Chrome(**retry_kwargs)
                _apply_failfast_client(driver, log_fn=log_fn)
                return driver
            except Exception:
                pass

        _emit_uc_log(
            log_fn,
            "browser launch failed "
            f"binary path={chrome_binary or 'auto'} "
            f"detected version={detected_version or 'auto'} "
            f"original exception={exc}",
        )
        try:
            return _launch_standard_chrome(
                options=options,
                chrome_binary=chrome_binary,
                detected_version=detected_version,
                log_fn=log_fn,
            )
        except Exception as fallback_exc:
            _emit_uc_log(log_fn, f"standard chromedriver fallback failed: {fallback_exc}")
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("browser launch failed (no exception captured)")
```

### DB_Beamin_Macro_Dags.py — load_accounts 함수 (lines 173-187)
```python
def load_accounts(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    stores_override = conf.get("stores")
    target = stores_override if stores_override else TARGET_STORES
    accounts = pipeline_load_accounts(target_stores=target, exact=True)

    if not stores_override:
        collect_range = conf.get("collect_range", COLLECT_RANGE)
        accounts = _split_accounts_by_range(accounts, collect_range)

    context["ti"].xcom_push(key="account_list", value=accounts)
    stores = [a["store_name"] for a in accounts]
    logger.info("怨꾩젙 濡쒕뱶 ?꾨즺: %d媛?-> %s", len(accounts), stores)  # ← 수정 대상
    return f"怨꾩젙 {len(accounts)}媛? {stores}"                             # ← 수정 대상
```

## Test Cases

1. **Fix 1 한글 확인**: 파일을 열어 lines 186-187에 `怨` 등 깨진 문자가 없고 `계정`, `개` 가 보이는지 확인
   ```bash
   python -c "
   import ast, sys
   with open('dags/db/DB_Beamin_Macro_Dags.py', encoding='utf-8') as f:
       src = f.read()
   assert '怨꾩젙' not in src, 'FAIL: 한글 깨짐 잔존'
   assert '계정 로드 완료' in src, 'FAIL: 수정 텍스트 없음'
   print('PASS')
   "
   ```

2. **Fix 2 코드 삽입 확인**:
   ```bash
   python -c "
   with open('modules/transform/utility/selenium_uc.py', encoding='utf-8') as f:
       src = f.read()
   assert 'session not created' in src, 'FAIL: 폴백 케이스 없음'
   assert 'chrome_version.txt' in src, 'FAIL: 버전파일 삭제 로직 없음'
   print('PASS')
   "
   ```

3. **Import 검증**:
   ```bash
   python -c "from modules.transform.utility.selenium_uc import launch_uc_chrome; print('PASS')"
   python -c "from dags.db.DB_Beamin_Macro_Dags import dag; print('PASS')"
   ```

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 1-3 순서대로 실행
  2. FAIL 항목 → 원인 분석 (인코딩 / 삽입 위치 / import 오류)
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS
```

## Constraints
- `DB_Beamin_Macro_Dags.py` 186-187줄 **두 줄만** 변경. 나머지 함수/DAG 구조 보존
- `selenium_uc.py` 삽입 위치는 `RemoteDisconnected` 블록 다음, `_emit_uc_log("browser launch failed ...")` 이전이어야 함
- 들여쓰기는 `if True:` 블록 내부이므로 **8칸(공백 8개)** 유지
- `chrome_version.txt` 삭제는 `undetected_chromedriver` 바이너리와 함께 삭제해야 다음 실행에서 재다운로드가 트리거됨 — 둘 중 하나만 삭제하면 안 됨

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 기존 파일과 동일하게
- 주석 여부: 최소화 (WHY만, WHAT 설명 금지)
- 변수명·함수명 스타일: snake_case, 기존 파일과 동일하게
