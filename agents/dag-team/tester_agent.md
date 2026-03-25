# tester_agent — dag-team (Sonnet)

coder가 생성한 DAG/파이프라인 코드를 정적 분석으로 컨벤션 준수 여부를 검증한다.

```python
import re
from pathlib import Path


def main(coder_result: dict) -> dict:
    """
    생성된 코드를 정적 분석해 컨벤션 준수 여부를 반환한다.

    Args:
        coder_result: coder_agent.main()의 반환 dict
            {dag_code, pipeline_code, dag_file_path, pipeline_file_path}

    Returns:
        {
            "checks": {...},
            "overall": "PASS|CAUTION|FAIL",
            "issues": [...]
        }
    """
    dag_code = coder_result.get("dag_code", "")
    pipeline_code = coder_result.get("pipeline_code", "")

    dag_checks = _check_dag(dag_code)
    pipeline_checks = _check_pipeline(pipeline_code)

    all_checks = {**dag_checks, **pipeline_checks}
    issues = [rule for rule, passed in all_checks.items() if not passed]

    # 판정
    fail_rules = {
        "no_print",
        "no_hardcode_path",
        "has_logging",
        "dag_id_from_filename",
        "with_dag_style",
    }
    caution_rules = {
        "schedule_constant",
        "pendulum_start_date",
        "xcom_pattern",
        "paths_constants",
    }

    failed = [r for r in issues if r in fail_rules]
    cautioned = [r for r in issues if r in caution_rules]

    if failed:
        overall = "FAIL"
    elif cautioned:
        overall = "CAUTION"
    else:
        overall = "PASS"

    return {
        "checks": all_checks,
        "overall": overall,
        "issues": issues,
        "detail": _build_detail(issues)
    }


def _check_dag(code: str) -> dict:
    return {
        "dag_id_from_filename": bool(
            re.search(r'dag_id\s*=\s*Path\(__file__\)\.stem', code) or
            re.search(r'dag_id\s*=\s*os\.path\.basename', code)
        ),
        "with_dag_style": bool(re.search(r'with DAG\(', code)),
        "schedule_constant": not bool(re.search(r"schedule\s*=\s*['\"][\d\s\*]+['\"]", code)),
        "pendulum_start_date": bool(re.search(r'pendulum\.datetime', code)),
        "xcom_pattern": bool(
            re.search(r"op_kwargs\s*=\s*\{", code) or
            re.search(r'xcom_pull', code)
        ),
        "no_email_on_failure_true": not bool(re.search(r"'email_on_failure'\s*:\s*True", code)),
        "has_default_args": bool(re.search(r'default_args', code)),
        "no_hardcode_schedule": not bool(re.search(r"schedule_interval\s*=\s*['\"]", code)),
    }


def _check_pipeline(code: str) -> dict:
    hardcode_patterns = [
        r'["\']C:\\\\',
        r'["\']C:/',
        r'["\'/]opt/airflow',
        r'["\']E:/',
        r'["\']D:/',
    ]
    has_hardcode = any(re.search(p, code) for p in hardcode_patterns)

    paths_constants = [
        "COLLECT_DB", "LOCAL_DB", "ONEDRIVE_DB",
        "ANALYTICS_DB", "TEMP_DIR", "DOWN_DIR"
    ]
    uses_paths_const = any(c in code for c in paths_constants)

    return {
        "no_print": not bool(re.search(r'\bprint\s*\(', code)),
        "no_hardcode_path": not has_hardcode,
        "has_logging": bool(re.search(r'logging\.getLogger\(__name__\)', code)),
        "logger_not_print": not bool(re.search(r'\bprint\s*\(', code)),
        "paths_constants": uses_paths_const,
        "context_param": bool(re.search(r'def \w+\(\*\*context\)', code)),
    }


def _build_detail(issues: list) -> dict:
    descriptions = {
        "dag_id_from_filename": "dag_id는 Path(__file__).stem으로 자동 추출해야 합니다",
        "with_dag_style": "with DAG(...) as dag: 스타일을 사용해야 합니다",
        "schedule_constant": "schedule은 schedule.py 상수를 사용해야 합니다 (cron 문자열 하드코딩 금지)",
        "pendulum_start_date": "start_date는 pendulum.datetime(tz='Asia/Seoul')을 사용해야 합니다",
        "xcom_pattern": "태스크 간 데이터는 XCom op_kwargs 패턴으로 전달해야 합니다",
        "no_print": "print() 사용 금지 — logging.getLogger(__name__)를 사용하세요",
        "no_hardcode_path": "경로 하드코딩 금지 — paths.py 상수를 사용하세요",
        "has_logging": "logger = logging.getLogger(__name__) 선언이 필요합니다",
        "paths_constants": "경로는 COLLECT_DB, LOCAL_DB 등 paths.py 상수를 사용해야 합니다",
        "context_param": "파이프라인 함수는 **context 파라미터를 받아야 합니다",
        "no_hardcode_schedule": "schedule_interval= 대신 schedule= 을 사용하세요",
    }
    return {issue: descriptions.get(issue, "컨벤션 위반") for issue in issues}
```
