# planner_agent — dag-team (Haiku)

사용자의 자연어 계획을 받아 최적의 기술적 코딩 계획을 수립한다.

```python
import json
import re
from pathlib import Path

def main(user_request: str) -> dict:
    """
    사용자 요청을 분석해 DAG 코딩 계획 dict를 반환한다.

    Args:
        user_request: 자연어로 된 DAG 생성 요청

    Returns:
        plan dict:
        {
            "dag_file": "dags/sales/Sales_XXX_Dags.py",
            "pipeline_module": "modules/transform/pipelines/sales/SMD_XXX.py",
            "dag_id": "Sales_XXX_Dags",
            "team": "sales | strategy",
            "schedule_constant": "SMD_ORDERS_TIME",
            "tasks": [
                {"task_id": "...", "function": "...", "xcom_out": "..."}
            ],
            "external_sensor": None,
            "reference_dag": "dags/sales/Sales_Orders_01_Extract_Dags.py",
            "rationale": "..."
        }
    """
    import subprocess
    import os

    airflow_root = Path("/opt/airflow")
    if not airflow_root.exists():
        airflow_root = Path("C:/airflow")

    # 1. 팀 분류
    team = _classify_team(user_request)
    dag_dir = airflow_root / "dags" / team
    pipeline_dir = airflow_root / "modules" / "transform" / "pipelines" / team

    # 2. 기존 DAG 목록 스캔
    existing_dags = sorted(dag_dir.glob("*.py")) if dag_dir.exists() else []
    dag_names = [f.stem for f in existing_dags]

    # 3. 스케줄 상수 확인
    schedule_path = airflow_root / "modules" / "transform" / "utility" / "schedule.py"
    schedule_constants = _read_schedule_constants(schedule_path)

    # 4. paths.py 상수 확인
    paths_path = airflow_root / "modules" / "transform" / "utility" / "paths.py"
    paths_constants = _read_paths_constants(paths_path)

    # 5. 네이밍 결정
    dag_name, module_name = _decide_naming(user_request, team, dag_names)
    dag_file = f"dags/{team}/{dag_name}.py"
    pipeline_module = f"modules/transform/pipelines/{team}/{module_name}.py"

    # 6. 스케줄 상수 선택
    schedule_constant = _select_schedule(user_request, schedule_constants)

    # 7. 태스크 설계
    tasks = _design_tasks(user_request, team)

    # 8. ExternalTaskSensor 필요 여부
    external_sensor = _needs_external_sensor(user_request, dag_names)

    # 9. 가장 유사한 reference DAG 선정
    reference_dag = _find_reference_dag(user_request, existing_dags, tasks)

    return {
        "dag_file": dag_file,
        "pipeline_module": pipeline_module,
        "dag_id": dag_name,
        "team": team,
        "schedule_constant": schedule_constant,
        "tasks": tasks,
        "external_sensor": external_sensor,
        "reference_dag": str(reference_dag) if reference_dag else None,
        "paths_constants": paths_constants,
        "rationale": _build_rationale(user_request, team, schedule_constant, reference_dag)
    }


def _classify_team(request: str) -> str:
    strategy_keywords = ["전략", "쿠폰", "voc", "cs", "플로우", "coupang", "fdam", "toorder", "strategy"]
    request_lower = request.lower()
    for kw in strategy_keywords:
        if kw in request_lower:
            return "strategy"
    return "sales"


def _read_schedule_constants(path: Path) -> dict:
    if not path.exists():
        return {}
    content = path.read_text(encoding="utf-8")
    constants = {}
    for line in content.splitlines():
        m = re.match(r'^(\w+)\s*=\s*["\'](.+)["\']', line.strip())
        if m:
            constants[m.group(1)] = m.group(2)
    return constants


def _read_paths_constants(path: Path) -> list:
    if not path.exists():
        return []
    content = path.read_text(encoding="utf-8")
    return re.findall(r'^([A-Z_]+)\s*=', content, re.MULTILINE)


def _decide_naming(request: str, team: str, existing: list) -> tuple:
    """파일명 결정 — 기존 번호 체계에서 다음 번호 사용"""
    prefix = "Sales" if team == "sales" else "Strategy"

    # 기존 번호 추출
    nums = []
    for name in existing:
        m = re.search(r'_(\d{2})_', name)
        if m:
            nums.append(int(m.group(1)))
    next_num = f"{(max(nums) + 1):02d}" if nums else "01"

    # 요청에서 키워드 추출
    keyword = _extract_keyword(request)

    dag_name = f"{prefix}_{keyword}_Dags"
    module_prefix = "SMD" if team == "sales" else "SMP"
    module_name = f"{module_prefix}_{keyword}"
    return dag_name, module_name


def _extract_keyword(request: str) -> str:
    """요청에서 핵심 키워드 추출 (CamelCase)"""
    stopwords = {"dag", "파이프라인", "만들어", "추가", "생성", "데이터", "처리", "the", "a", "an"}
    words = re.findall(r'[A-Za-z가-힣]+', request)
    keywords = [w for w in words if w.lower() not in stopwords and len(w) > 1]
    if not keywords:
        return "New"
    # 첫 2개 영단어 또는 한글 키워드를 CamelCase로
    result = []
    for w in keywords[:2]:
        if re.match(r'[A-Za-z]', w):
            result.append(w.capitalize())
        else:
            result.append(w)
    return "_".join(result) if result else "New"


def _select_schedule(request: str, constants: dict) -> str:
    request_lower = request.lower()
    if "매일" in request or "daily" in request_lower:
        for k, v in constants.items():
            if "* * *" in v and "1,3,5" not in v:
                return k
    if "월" in request or "weekly" in request_lower or "monday" in request_lower:
        for k, v in constants.items():
            if "* * 1" in v:
                return k
    # 기본값
    return list(constants.keys())[0] if constants else "SMD_ORDERS_TIME"


def _design_tasks(request: str, team: str) -> list:
    """요청 키워드 기반 태스크 설계"""
    tasks = []
    request_lower = request.lower()

    if "추출" in request or "extract" in request_lower or "로드" in request or "load" in request_lower:
        tasks.append({"task_id": "extract_data", "function": "extract_data", "xcom_out": "raw_parquet_path"})
    if "변환" in request or "transform" in request_lower or "전처리" in request:
        tasks.append({"task_id": "transform_data", "function": "transform_data", "xcom_out": "processed_path"})
    if "저장" in request or "save" in request_lower or "업로드" in request:
        tasks.append({"task_id": "save_data", "function": "save_data", "xcom_out": "output_path"})
    if "gsheet" in request_lower or "구글" in request or "시트" in request:
        tasks.append({"task_id": "upload_to_gsheet", "function": "upload_to_gsheet", "xcom_out": None})
    if "onedrive" in request_lower or "원드라이브" in request:
        tasks.append({"task_id": "upload_to_onedrive", "function": "upload_to_onedrive", "xcom_out": None})

    if not tasks:
        tasks = [
            {"task_id": "load_data", "function": "load_data", "xcom_out": "raw_path"},
            {"task_id": "process_data", "function": "process_data", "xcom_out": "processed_path"},
            {"task_id": "save_result", "function": "save_result", "xcom_out": "output_path"},
        ]
    return tasks


def _needs_external_sensor(request: str, dag_names: list) -> dict | None:
    if "완료 후" in request or "다음에" in request or "sensor" in request.lower():
        # 가장 최근 DAG를 대기 대상으로 추정
        if dag_names:
            return {
                "wait_for_dag": dag_names[-1],
                "wait_for_task": "save_data",
                "mode": "reschedule",
                "timeout": 3600
            }
    return None


def _find_reference_dag(request: str, existing: list, tasks: list) -> Path | None:
    if not existing:
        return None
    task_ids = [t["task_id"] for t in tasks]
    # XCom 패턴이 있으면 Extract DAG를 참조
    if any("extract" in t or "load" in t for t in task_ids):
        for f in existing:
            if "01" in f.stem or "Extract" in f.stem:
                return f
    # sensor 패턴이 있으면 02번 DAG 참조
    if any("sensor" in t or "review" in t for t in task_ids):
        for f in existing:
            if "02" in f.stem:
                return f
    return existing[0] if existing else None


def _build_rationale(request: str, team: str, schedule: str, ref: Path | None) -> str:
    lines = [
        f"팀: {team} (요청 키워드 기반 분류)",
        f"스케줄: {schedule}",
    ]
    if ref:
        lines.append(f"참조 DAG: {ref.name} (유사 패턴)")
    return " | ".join(lines)
```
