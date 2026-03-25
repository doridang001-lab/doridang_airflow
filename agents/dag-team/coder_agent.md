# coder_agent — dag-team (Sonnet)

planner의 plan dict를 받아 프로젝트 컨벤션에 맞는 DAG 파일과 파이프라인 모듈 코드를 생성한다.

```python
import json
import textwrap
from pathlib import Path


def main(plan: dict) -> dict:
    """
    plan dict를 받아 DAG 파일과 파이프라인 모듈 코드를 생성한다.

    Args:
        plan: planner_agent.main()의 반환 dict

    Returns:
        {
            "dag_code": "...",
            "pipeline_code": "...",
            "dag_file_path": "dags/sales/Sales_XXX_Dags.py",
            "pipeline_file_path": "modules/transform/pipelines/sales/SMD_XXX.py"
        }
    """
    airflow_root = Path("/opt/airflow")
    if not airflow_root.exists():
        airflow_root = Path("C:/airflow")

    # reference DAG 읽어서 스타일 확인
    reference_content = ""
    if plan.get("reference_dag"):
        ref_path = airflow_root / plan["reference_dag"]
        if ref_path.exists():
            reference_content = ref_path.read_text(encoding="utf-8")

    # schedule.py 읽어서 import 경로 확인
    schedule_path = airflow_root / "modules" / "transform" / "utility" / "schedule.py"
    schedule_import = _resolve_schedule_import(schedule_path, plan["schedule_constant"])

    dag_code = _generate_dag_code(plan, schedule_import, reference_content)
    pipeline_code = _generate_pipeline_code(plan)

    return {
        "dag_code": dag_code,
        "pipeline_code": pipeline_code,
        "dag_file_path": plan["dag_file"],
        "pipeline_file_path": plan["pipeline_module"]
    }


def _resolve_schedule_import(schedule_path: Path, constant: str) -> str:
    """schedule.py에서 실제 import 경로 확인"""
    if schedule_path.exists():
        content = schedule_path.read_text(encoding="utf-8")
        if constant in content:
            return f"from modules.transform.utility.schedule import {constant}"
    return f"from modules.transform.utility.io import {constant}"


def _generate_dag_code(plan: dict, schedule_import: str, reference: str) -> str:
    dag_id = plan["dag_id"]
    tasks = plan["tasks"]
    team = plan.get("team", "sales")
    schedule_const = plan["schedule_constant"]
    module_path = plan["pipeline_module"].replace("/", ".").replace(".py", "")
    module_name = Path(plan["pipeline_module"]).stem

    # 파이프라인 함수 import 목록
    func_names = [t["function"] for t in tasks]
    func_import = ", ".join(func_names)

    # ExternalTaskSensor 필요 여부
    sensor = plan.get("external_sensor")
    sensor_import = "from airflow.sensors.external_task import ExternalTaskSensor\n" if sensor else ""

    # 태스크 정의 생성
    task_defs = _build_task_definitions(tasks, sensor)

    # 태스크 체인 생성
    task_chain = _build_task_chain(tasks, sensor)

    code = textwrap.dedent(f"""\
        import os
        from pathlib import Path
        import pendulum
        from datetime import timedelta
        from airflow import DAG
        from airflow.operators.python import PythonOperator
        {sensor_import}
        {schedule_import}
        from {module_path} import {func_import}

        dag_id = Path(__file__).stem

        default_args = {{
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'email_on_failure': False,
        }}

        with DAG(
            dag_id=dag_id,
            schedule={schedule_const},
            start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
            catchup=False,
            max_active_runs=1,
            default_args=default_args,
        ) as dag:

        {task_defs}

        {task_chain}
    """)
    return code


def _build_task_definitions(tasks: list, sensor: dict | None) -> str:
    lines = []
    indent = "    "

    if sensor:
        lines.append(f"{indent}wait_task = ExternalTaskSensor(")
        lines.append(f"{indent}    task_id='wait_for_{sensor[\"wait_for_dag\"]}',")
        lines.append(f"{indent}    external_dag_id='{sensor[\"wait_for_dag\"]}',")
        lines.append(f"{indent}    external_task_id='{sensor[\"wait_for_task\"]}',")
        lines.append(f"{indent}    mode='{sensor[\"mode\"]}',")
        lines.append(f"{indent}    timeout={sensor['timeout']},")
        lines.append(f"{indent}    soft_fail=True,")
        lines.append(f"{indent})")
        lines.append("")

    for i, task in enumerate(tasks):
        task_var = f"{task['task_id']}_task"
        lines.append(f"{indent}{task_var} = PythonOperator(")
        lines.append(f"{indent}    task_id='{task['task_id']}',")
        lines.append(f"{indent}    python_callable={task['function']},")

        # XCom 연결 (첫 번째 태스크 제외)
        if i > 0 and tasks[i - 1].get("xcom_out"):
            prev_task = tasks[i - 1]
            lines.append(f"{indent}    op_kwargs={{")
            lines.append(f"{indent}        'input_task_id': '{prev_task['task_id']}',")
            lines.append(f"{indent}        'input_xcom_key': '{prev_task['xcom_out']}',")
            if task.get("xcom_out"):
                lines.append(f"{indent}        'output_xcom_key': '{task['xcom_out']}',")
            lines.append(f"{indent}    }},")

        lines.append(f"{indent})")
        lines.append("")

    return "\n".join(lines)


def _build_task_chain(tasks: list, sensor: dict | None) -> str:
    indent = "    "
    task_vars = [f"{t['task_id']}_task" for t in tasks]
    if sensor:
        task_vars = ["wait_task"] + task_vars
    chain = " >> ".join(task_vars)
    return f"{indent}{chain}"


def _generate_pipeline_code(plan: dict) -> str:
    module_name = Path(plan["pipeline_module"]).stem
    team = plan.get("team", "sales")
    tasks = plan["tasks"]
    func_names = [t["function"] for t in tasks]

    # paths 상수 결정
    paths_import = "COLLECT_DB, LOCAL_DB"
    if "onedrive" in plan["pipeline_module"].lower() or any("onedrive" in t["task_id"] for t in tasks):
        paths_import += ", ONEDRIVE_DB"

    func_bodies = []
    for i, task in enumerate(tasks):
        fname = task["function"]
        xcom_out = task.get("xcom_out")

        if i == 0:
            # 첫 번째: 데이터 로드
            body = textwrap.dedent(f"""\
                def {fname}(**context) -> str:
                    \"\"\"데이터 로드 후 parquet 경로를 XCom에 저장한다.\"\"\"
                    import pandas as pd
                    import tempfile

                    logger.info("Starting {fname}")

                    # TODO: 실제 데이터 소스에서 로드
                    df = pd.DataFrame()

                    # parquet으로 임시 저장
                    tmp = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
                    df.to_parquet(tmp.name, index=False)
                    logger.info(f"Saved {{len(df)}} rows to {{tmp.name}}")

                    if context.get('output_xcom_key') or {repr(xcom_out)}:
                        context['ti'].xcom_push(
                            key=context.get('output_xcom_key', {repr(xcom_out)}),
                            value=tmp.name
                        )
                    return tmp.name
            """)
        elif i == len(tasks) - 1:
            # 마지막: 저장
            body = textwrap.dedent(f"""\
                def {fname}(**context) -> str:
                    \"\"\"처리된 데이터를 최종 저장한다.\"\"\"
                    import pandas as pd

                    input_task_id = context.get('input_task_id', '{tasks[i-1]["task_id"] if i > 0 else ""}')
                    input_key = context.get('input_xcom_key', '{tasks[i-1].get("xcom_out", "") if i > 0 else ""}')
                    parquet_path = context['ti'].xcom_pull(task_ids=input_task_id, key=input_key)

                    logger.info(f"Loading from {{parquet_path}}")
                    df = pd.read_parquet(parquet_path)

                    # TODO: 저장 경로 및 로직 구현
                    output_path = str(LOCAL_DB / "{module_name}" / "output.csv")
                    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                    df.to_csv(output_path, index=False, encoding='utf-8-sig')
                    logger.info(f"Saved {{len(df)}} rows to {{output_path}}")
                    return output_path
            """)
        else:
            # 중간: 변환
            body = textwrap.dedent(f"""\
                def {fname}(**context) -> str:
                    \"\"\"데이터 변환 처리 후 parquet 경로를 XCom에 저장한다.\"\"\"
                    import pandas as pd
                    import tempfile

                    input_task_id = context.get('input_task_id', '{tasks[i-1]["task_id"] if i > 0 else ""}')
                    input_key = context.get('input_xcom_key', '{tasks[i-1].get("xcom_out", "") if i > 0 else ""}')
                    parquet_path = context['ti'].xcom_pull(task_ids=input_task_id, key=input_key)

                    logger.info(f"Loading from {{parquet_path}}")
                    df = pd.read_parquet(parquet_path)

                    # TODO: 변환 로직 구현
                    logger.info(f"Processed {{len(df)}} rows")

                    tmp = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
                    df.to_parquet(tmp.name, index=False)

                    output_key = context.get('output_xcom_key', {repr(xcom_out)})
                    context['ti'].xcom_push(key=output_key, value=tmp.name)
                    return tmp.name
            """)
        func_bodies.append(body)

    all_funcs = "\n\n".join(func_bodies)

    code = textwrap.dedent(f"""\
        import logging
        from pathlib import Path

        import pandas as pd

        from modules.transform.utility.paths import {paths_import}

        logger = logging.getLogger(__name__)


        {all_funcs}
    """)
    return code
```
