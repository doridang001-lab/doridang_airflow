# 홀 주간보고 xlsx 잠김 PermissionError 수정

## Task
`DB_Hall_Sales_Target_Dags.build_weekly_report_excel` 이 고정 참조 파일 `hall_weekly_report.xlsx` 를 저장할 때 `PermissionError`(Errno 13) 로 실패한다. 이 파일은 Excel/PowerBI 가 Windows 호스트에서 열어두는 파일이라 OneDrive bind 마운트를 통해 CIFS 공유 위반(EACCES)이 발생한다. 고정 참조 파일 저장을 **비치명적(non-fatal)** 으로 만들어, 파일이 잠겨 있어도 태스크가 실패하지 않게 한다(날짜별 파일은 이미 저장됨).

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지 (해당 파일에 logger 이미 존재)
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 파이프라인 파일 위치: `modules/transform/pipelines/db/`
- 수정 대상 파일: `modules/transform/pipelines/db/DB_Hall_Sales_Excel.py`

## Files to Create / Modify
- 수정: `modules/transform/pipelines/db/DB_Hall_Sales_Excel.py` (단일 파일, `build_weekly_report_excel` 함수 내 저장 블록)

## Root Cause (배경)
- 라인 439 (날짜별 파일 `hall_weekly_report_YYMMDD.xlsx`): `allow_unique_fallback=True` → 잠김 시 타임스탬프 파일명으로 우회 저장 → 성공.
- 라인 440 (고정 파일 `hall_weekly_report.xlsx`): `allow_unique_fallback` 없음 → 잠김 시 `_save_workbook_replace` 내부 `path.unlink()`(라인 131) 에서 `PermissionError` 재발생 → 태스크 실패.
- 라인 126 의 임시 파일 저장은 성공(디렉터리는 쓰기 가능) → 문제는 **파일 자체 잠김**, 폴더 권한 아님.
- `retries: 3` 이라 15분 간격 3회 재시도 모두 실패 → ~45분 후 DAG 실패 + 이메일/텔레그램 실패 알림.

## Implementation Steps

1. `DB_Hall_Sales_Excel.py` 의 `build_weekly_report_excel` 함수 저장 블록에서 라인 440 을 아래처럼 교체한다.

교체 전:
```python
    output_path = _save_workbook_replace(wb, output_path, allow_unique_fallback=True)
    _save_workbook_replace(wb, latest_path)   # 고정 파일명 — Excel/PowerBI 참조용
```

교체 후:
```python
    output_path = _save_workbook_replace(wb, output_path, allow_unique_fallback=True)
    try:
        _save_workbook_replace(wb, latest_path)   # 고정 파일명 — Excel/PowerBI 참조용
    except PermissionError:
        logger.warning(
            "고정 참조 파일 잠김(열려 있음) — 갱신 건너뜀: %s (최신 데이터는 %s)",
            latest_path, output_path,
        )
        from modules.transform.utility.notifier import send_telegram
        send_telegram(
            f"[홀 주간보고] {latest_path.name} 이 열려 있어 갱신하지 못했습니다.\n"
            f"최신본: {output_path.name} (파일을 닫으면 다음 실행에 자동 갱신)"
        )
```

2. 변경 원칙:
   - `PermissionError` 만 삼킨다. 그 외 예외는 그대로 전파.
   - 라인 439(날짜별 파일)는 수정하지 않는다 — 이미 `allow_unique_fallback` 로 잠김 처리됨.
   - `_save_workbook_replace` 함수 자체는 수정하지 않는다.

## Reference Code

### modules/transform/pipelines/db/DB_Hall_Sales_Excel.py (저장 헬퍼 + 호출부)
```python
def _save_workbook_replace(wb: Workbook, path, allow_unique_fallback: bool = False):
    tmp_path = path.with_name(f".{path.stem}.tmp{path.suffix}")
    tmp_path.unlink(missing_ok=True)
    wb.save(tmp_path)
    tmp_path.chmod(0o666)
    try:
        if path.exists():
            path.chmod(0o666)
            path.unlink()               # ← 잠김 시 여기서 PermissionError
        tmp_path.replace(path)
        saved_path = path
    except PermissionError:
        if not allow_unique_fallback:
            raise                        # ← 라인 440 호출은 여기서 재발생
        saved_path = path.with_name(
            f"{path.stem}_{datetime.now().strftime('%H%M%S')}{path.suffix}"
        )
        tmp_path.replace(saved_path)
    saved_path.chmod(0o666)
    return saved_path

# ... build_weekly_report_excel() 저장 블록 ...
    today_str = date.today().strftime("%y%m%d")
    output_path = XLSX_DIR / f"hall_weekly_report_{today_str}.xlsx"
    latest_path = XLSX_DIR / "hall_weekly_report.xlsx"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path = _save_workbook_replace(wb, output_path, allow_unique_fallback=True)
    _save_workbook_replace(wb, latest_path)   # ← 수정 대상
```

### modules/transform/utility/notifier.py (send_telegram — 이미 존재, 재사용)
```python
def send_telegram(text: str) -> bool:
    token, chat_id = _get_telegram_creds()
    if not token or not chat_id:
        logger.warning("Telegram credentials missing; skip send")
        return False   # 크리덴셜 없으면 안전하게 no-op
    ...
```

## Test Cases
1. [문법] `python -c "import ast; ast.parse(open(r'C:/airflow/modules/transform/pipelines/db/DB_Hall_Sales_Excel.py',encoding='utf-8').read())"` → 기대: 예외 없음
2. [잠김 재현] `hall_weekly_report.xlsx` 를 다른 프로세스로 열어(또는 파일 핸들 유지) `build_weekly_report_excel` 실행 → 기대: 날짜별 파일 저장 성공, 태스크 정상 반환, `logger.warning` 출력 + 텔레그램 알림 시도, 기존 고정 파일 그대로 유지
3. [정상 경로] 파일 잠기지 않은 상태로 `build_weekly_report_excel` 실행 → 기대: `hall_weekly_report.xlsx` 기존처럼 덮어쓰기 성공
4. [예외 격리] `PermissionError` 외 다른 예외는 여전히 전파되는지 코드 검토로 확인

## Verification Loop
```
LOOP until all PASS:
  1. Test Cases 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints
- 오직 `PermissionError` 만 catch. 광범위한 `except Exception` 금지.
- 라인 439(날짜별 파일) 및 `_save_workbook_replace` 함수 시그니처는 변경 금지.
- `send_telegram` 은 새로 만들지 말고 `modules.transform.utility.notifier` 의 기존 함수 재사용.
- print 금지 — 기존 `logger` 사용.
