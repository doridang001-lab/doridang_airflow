"""Flow visit log mart DAG."""

from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from modules.transform.utility.notifier import on_failure_callback

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)

dag_file_stem = Path(__file__).stem
pipeline_module_path = "modules.transform.pipelines.strategy.SMP_flow_visit_mart"
pipeline_module = importlib.import_module(pipeline_module_path)

# Flow 방문일지 마트 대상은 여기에서만 관리한다.
FLOW_VISIT_TARGETS = [
    {"store_name": "평택비전점", "project_id": "2896580"},    
    {"store_name": "하늘도시점", "project_id": "2408780"},
    {"store_name": "대전장대점", "project_id": "2560963"},
    {"store_name": "대전용전점", "project_id": "2401767"},
    {"store_name": "의정부점", "project_id": "2321844"},
    {"store_name": "수유점", "project_id": "2539491"},
    {"store_name": "교대점", "project_id": "2321817"},
    {"store_name": "용인동천점", "project_id": "2742104"},
    {"store_name": "대화점", "project_id": "2548064"},
    {"store_name": "법흥리점", "project_id": "2321821"},
    {"store_name": "동탄영천점", "project_id": "2466857"},
    {"store_name": "송파점", "project_id": "2321816"},
    {"store_name": "삼송점", "project_id": "2342207"},
    {"store_name": "강동점", "project_id": "2321820"},
    {"store_name": "중랑면목점", "project_id": "2896575"},
    {"store_name": "수원화서점", "project_id": "2848682"},
    {"store_name": "광주태전점", "project_id": "2548056"},
    {"store_name": "성신여대점", "project_id": "2525213"},
    {"store_name": "용인마북점", "project_id": "2944898"},
    {"store_name": "남양주진접점", "project_id": "2941806"},
    {"store_name": "광주경안점", "project_id": "2471056"},
    {"store_name": "강원영월점", "project_id": "2801897"},
    {"store_name": "부산대신점", "project_id": "2656621"},
    {"store_name": "백석점", "project_id": "2956190"},
    {"store_name": "동인천점", "project_id": "2954006"},
    {"store_name": "동두천지행점", "project_id": "2729660"},
    {"store_name": "청라점", "project_id": "2361858"},
    {"store_name": "연신내점", "project_id": "2676688"},
    {"store_name": "미사점", "project_id": "2344833"},
    {"store_name": "기흥테라타워점", "project_id": "2664367"},
    {"store_name": "부산장림점", "project_id": "2460014"},
    {"store_name": "창원내서점", "project_id": "2731269"},
    {"store_name": "부산서면점", "project_id": "2769736"},
    {"store_name": "천안성정점", "project_id": "2413519"},
    
    {"store_name": "응암점", "project_id": "2373040"},
    {"store_name": "화성봉담점", "project_id": "2526804"},
    {"store_name": "광명철산점", "project_id": "2733771"},
    {"store_name": "세종나성점", "project_id": "2700460"},
    {"store_name": "해운대중동점", "project_id": "2852733"},
    {"store_name": "전주전북대점", "project_id": "2785861"},
    {"store_name": "상도점", "project_id": "2434558"},
    {"store_name": "시흥배곧점", "project_id": "2808861"},
    {"store_name": "대전중구점", "project_id": "2586609"},
    {"store_name": "대전관평점", "project_id": "2767498"},
    {"store_name": "부산재송센텀점", "project_id": "2531198"},
    {"store_name": "경북상주점", "project_id": "2731249"},
    {"store_name": "구로디지털점", "project_id": "2475090"},
    {"store_name": "대전관저점", "project_id": "2741392"}, 
    {"store_name": "대전둔산점", "project_id": "2326855"},
    {"store_name": "부산광안점", "project_id": "2676689"},
    {"store_name": "인천간석중앙점", "project_id": "2526812"},
    {"store_name": "김포장기점", "project_id": "2363307"},
    {"store_name": "김포풍무점", "project_id": "2531200"},
    {"store_name": "서울대입구역점", "project_id": "2321847"},
    {"store_name": "부천옥길점", "project_id": "2398936"},
    {"store_name": "김해구산점", "project_id": "2749493"},
    {"store_name": "시흥장현점", "project_id": "2927811"},
    {"store_name": "용현점", "project_id": "2321837"},
    {"store_name": "행신점", "project_id": "2419636"},
    {"store_name": "초월점", "project_id": "2321846"},
    {"store_name": "구리다산점", "project_id": "2518266"},
    {"store_name": "오산시청점", "project_id": "2764957"},
    {"store_name": "평택서정점", "project_id": "2524592"},
    {"store_name": "역삼점", "project_id": "2321834"},


    {"store_name": "양주옥정점", "project_id": "2495366"},
    {"store_name": "익산영등점", "project_id": "2629242"},
]

extract_visit_logs = pipeline_module.extract_visit_logs
parse_visit_meta = pipeline_module.parse_visit_meta
llm_extract_issues = pipeline_module.llm_extract_issues
build_store_profile = pipeline_module.build_store_profile
save_visit_mart = pipeline_module.save_visit_mart
build_visit_viz_table = pipeline_module.build_visit_viz_table
export_llm_corpus = pipeline_module.export_llm_corpus
eval_quality = pipeline_module.eval_quality


def task_extract_visit_logs(**context):
    context["flow_visit_targets"] = FLOW_VISIT_TARGETS
    payload = extract_visit_logs(**context)
    if not payload.get("posts"):
        raise AirflowSkipException("Flow 방문일지 게시글이 없습니다.")
    context["ti"].xcom_push(key="visit_payload", value=payload)
    logger.info("Flow 방문일지 원천 XCom 저장 완료: posts=%s", len(payload.get("posts") or []))
    return payload


def task_parse_visit_meta(**context):
    payload = context["ti"].xcom_pull(task_ids="task_extract_visit_logs", key="visit_payload")
    if not payload:
        raise AirflowSkipException("Flow 방문일지 원천 페이로드가 없습니다.")
    payload = parse_visit_meta(payload, **context)
    context["ti"].xcom_push(key="visit_payload", value=payload)
    return payload


def task_llm_extract_issues(**context):
    payload = context["ti"].xcom_pull(task_ids="task_parse_visit_meta", key="visit_payload")
    if not payload:
        raise AirflowSkipException("Flow 방문일지 메타 페이로드가 없습니다.")
    payload = llm_extract_issues(payload, **context)
    context["ti"].xcom_push(key="visit_payload", value=payload)
    return payload


def task_build_store_profile(**context):
    payload = context["ti"].xcom_pull(task_ids="task_llm_extract_issues", key="visit_payload")
    if not payload:
        raise AirflowSkipException("Flow 방문일지 이슈 페이로드가 없습니다.")
    payload = build_store_profile(payload, **context)
    context["ti"].xcom_push(key="visit_payload", value=payload)
    return payload


def task_save_visit_mart(**context):
    payload = context["ti"].xcom_pull(task_ids="task_build_store_profile", key="visit_payload")
    if not payload:
        raise AirflowSkipException("Flow 방문일지 프로필 페이로드가 없습니다.")
    saved_message = save_visit_mart(payload, **context)
    context["ti"].xcom_push(key="saved_message", value=saved_message)
    return saved_message


def task_export_llm_corpus(**context):
    payload = context["ti"].xcom_pull(task_ids="task_build_store_profile", key="visit_payload")
    if not payload:
        raise AirflowSkipException("Flow 방문일지 프로필 페이로드가 없습니다.")
    corpus_message = export_llm_corpus(payload, **context)
    context["ti"].xcom_push(key="corpus_message", value=corpus_message)
    return corpus_message


def task_build_visit_viz(**context):
    payload = context["ti"].xcom_pull(task_ids="task_build_store_profile", key="visit_payload")
    if not payload:
        raise AirflowSkipException("Flow 방문일지 프로필 페이로드가 없습니다.")
    viz_message = build_visit_viz_table(payload, **context)
    context["ti"].xcom_push(key="viz_message", value=viz_message)
    return viz_message


def task_write_log(**context):
    ti = context["ti"]
    saved_message = ti.xcom_pull(task_ids="task_save_visit_mart", key="saved_message")
    viz_message = ti.xcom_pull(task_ids="task_build_visit_viz", key="viz_message")
    corpus_message = ti.xcom_pull(task_ids="task_export_llm_corpus", key="corpus_message")
    quality_message = ti.xcom_pull(task_ids="task_eval_quality", key="quality_message")
    message = (
        f"{saved_message or '마트 저장 없음'} / {viz_message or '시각화 저장 없음'} / "
        f"{corpus_message or 'JSONL 저장 없음'} / {quality_message or '품질검사 없음'}"
    )
    logger.info("Flow 방문일지 마트 DAG 종료: %s", message)
    return message


def task_eval_quality(**context):
    quality_message = eval_quality(**context)
    context["ti"].xcom_push(key="quality_message", value=quality_message)
    return quality_message


with DAG(
    dag_id=dag_file_stem,
    description="Flow 방문일지 이슈 추출 및 매장 프로필 마트 생성",
    schedule=None,  # Strategy_FlowStore_01_Collect_Dags 수집 완료 시 트리거
    start_date=pendulum.datetime(2026, 8, 4, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["02_transform", "flow", "visit_log", "llm", "event_driven"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "email_on_failure": False,
        "on_failure_callback": on_failure_callback,
    },
) as dag:
    t1 = PythonOperator(
        task_id="task_extract_visit_logs",
        python_callable=task_extract_visit_logs,
        show_return_value_in_logs=False,
    )
    t2 = PythonOperator(
        task_id="task_parse_visit_meta",
        python_callable=task_parse_visit_meta,
        show_return_value_in_logs=False,
    )
    t3 = PythonOperator(
        task_id="task_llm_extract_issues",
        python_callable=task_llm_extract_issues,
        show_return_value_in_logs=False,
    )
    t4 = PythonOperator(
        task_id="task_build_store_profile",
        python_callable=task_build_store_profile,
        show_return_value_in_logs=False,
    )
    t5 = PythonOperator(
        task_id="task_save_visit_mart",
        python_callable=task_save_visit_mart,
    )
    t6 = PythonOperator(
        task_id="task_build_visit_viz",
        python_callable=task_build_visit_viz,
    )
    t7 = PythonOperator(
        task_id="task_export_llm_corpus",
        python_callable=task_export_llm_corpus,
    )
    t8 = PythonOperator(
        task_id="task_eval_quality",
        python_callable=task_eval_quality,
    )
    t9 = PythonOperator(
        task_id="task_write_log",
        python_callable=task_write_log,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9
