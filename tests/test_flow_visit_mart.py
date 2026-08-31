import json
from pathlib import Path

import pandas as pd
import pytest

from modules.transform.pipelines.strategy import SMP_flow_visit_mart as flow_visit
from modules.transform.pipelines.strategy import flow_visit_prompts
from modules.transform.pipelines.strategy import flow_visit_viz
from modules.transform.utility import qwen_client


class _DagRun:
    def __init__(self, conf):
        self.conf = conf


def test_query_flow_json_keeps_stable_model_candidates(monkeypatch):
    captured = {}

    def fake_query_qwen_json(prompt, **kwargs):
        captured.update(kwargs)
        return {"issue_key": "기타", "severity": "낮음", "status": "미해결", "is_request": False}

    monkeypatch.delenv("FLOW_VISIT_LLM_MODELS", raising=False)
    monkeypatch.setattr(qwen_client, "query_qwen_json", fake_query_qwen_json)

    result = flow_visit._query_flow_json(
        "prompt",
        "system",
        client=object(),
        model_candidates=["qwen2.5:14b", "gpt-oss:20b"],
    )

    assert result["issue_key"] == "기타"
    assert captured["preferred_models"] is None
    assert captured["model_candidates"] == ["qwen2.5:14b", "gpt-oss:20b"]


def test_parse_post_date_accepts_flow_compact_timestamp():
    assert flow_visit._parse_post_date("20260804130509") == "2026-08-04"
    assert flow_visit._parse_post_date("20260804") == "2026-08-04"
    assert flow_visit._parse_post_date("2026-08-04") == "2026-08-04"


def test_read_parquet_dataset_preserves_mixed_partition_schema(tmp_path):
    base = tmp_path / "flow_comment"
    old_dir = base / "project_id=1"
    new_dir = base / "project_id=2"
    old_dir.mkdir(parents=True)
    new_dir.mkdir(parents=True)
    pd.DataFrame([{
        "comment_id": "old",
        "post_id": "100",
        "content_text": "old schema",
    }]).to_parquet(old_dir / "part.parquet", index=False)
    pd.DataFrame([{
        "comment_id": "new",
        "post_id": "200",
        "content_text": "new schema",
        "parent_comment_id": "root",
        "root_comment_id": "root",
    }]).to_parquet(new_dir / "part.parquet", index=False)

    df = flow_visit._read_parquet_dataset(base)

    assert set(["project_id", "parent_comment_id", "root_comment_id"]).issubset(df.columns)
    row = df[df["comment_id"].eq("new")].iloc[0]
    assert row["project_id"] == "2"
    assert row["parent_comment_id"] == "root"


def test_flow_visit_target_project_ids_returns_store_projects_when_env_empty(monkeypatch):
    monkeypatch.setattr(flow_visit, "TARGET_PROJECT_IDS", "")
    monkeypatch.delenv("FLOW_VISIT_PROJECT_IDS", raising=False)

    df = pd.DataFrame([
        {"project_id": "1", "is_store": True},
        {"project_id": "2", "is_store": False},
    ])

    assert flow_visit._target_project_ids(df) == {"1"}


def test_flow_visit_target_project_ids_accepts_conf_targets():
    df = pd.DataFrame([
        {"project_id": "1", "store_name": "용인동천점", "project_name": "경기_용인동천점"},
    ])
    context = {
        "dag_run": _DagRun({
            "flow_visit_targets": [
                {"store_name": "대화점", "project_id": "2548064"},
            ],
        })
    }

    assert flow_visit._target_project_ids(df, context=context) == {"2548064"}


def test_flow_visit_target_project_ids_accepts_dag_top_targets():
    df = pd.DataFrame([
        {"project_id": "1", "store_name": "용인동천점", "project_name": "경기_용인동천점"},
    ])
    context = {
        "flow_visit_targets": [
            {"store_name": "동탄영천점", "project_id": "2466857"},
            {"store_name": "용인동천점", "project_id": "2742104"},
            {"store_name": "대화점", "project_id": "2548064"},
        ],
        "dag_run": _DagRun({
            "flow_visit_targets": [
                {"store_name": "무시대상", "project_id": "9999999"},
            ],
        }),
    }

    assert flow_visit._target_project_ids(df, context=context) == {"2466857", "2742104", "2548064"}


def test_flow_visit_target_project_ids_matches_conf_store_name():
    df = pd.DataFrame([
        {"project_id": "2548064", "store_name": "대화점", "project_name": "경기_대화점"},
        {"project_id": "2742104", "store_name": "용인동천점", "project_name": "경기_용인동천점"},
    ])
    context = {"dag_run": _DagRun({"flow_visit_store_names": "도리당 대화점"})}

    assert flow_visit._target_project_ids(df, context=context) == {"2548064"}


def test_extract_visit_logs_reports_missing_explicit_project(monkeypatch):
    post = pd.DataFrame([
        {
            "project_id": "2742104",
            "post_id": "100",
            "title": "방문일지",
            "content_text": "",
        }
    ])
    projects = pd.DataFrame([
        {"project_id": "2742104", "store_name": "용인동천점", "project_name": "경기_용인동천점"},
    ])

    monkeypatch.setattr(
        flow_visit,
        "_read_parquet_first",
        lambda paths, required=True: projects if not required else post,
    )

    context = {"dag_run": _DagRun({"flow_visit_targets": [{"store_name": "대화점", "project_id": "2548064"}]})}

    with pytest.raises(RuntimeError, match="Flow 방문일지 대상 프로젝트 원천 누락: 2548064"):
        flow_visit.extract_visit_logs(**context)


def test_extract_visit_logs_reports_partially_missing_top_targets(monkeypatch):
    post = pd.DataFrame([
        {
            "project_id": "2742104",
            "post_id": "100",
            "title": "방문일지",
            "content_text": "",
        }
    ])
    projects = pd.DataFrame([
        {"project_id": "2742104", "store_name": "용인동천점", "project_name": "경기_용인동천점"},
        {"project_id": "2466857", "store_name": "동탄영천점", "project_name": "경기_동탄영천점"},
    ])

    monkeypatch.setattr(
        flow_visit,
        "_read_parquet_first",
        lambda paths, required=True: projects if not required else post,
    )

    context = {
        "flow_visit_targets": [
            {"store_name": "동탄영천점", "project_id": "2466857"},
            {"store_name": "용인동천점", "project_id": "2742104"},
        ]
    }

    with pytest.raises(RuntimeError, match="Flow 방문일지 대상 프로젝트 원천 누락: 2466857"):
        flow_visit.extract_visit_logs(**context)


def test_extract_visit_logs_includes_visit_subtasks_and_comments(monkeypatch):
    post = pd.DataFrame([
        {
            "project_id": "2548064",
            "post_id": "83047164",
            "parent_post_id": "",
            "title": "26.08.12 (수)대화점 방문일지",
            "content_text": "방문일자: 2026-08-12",
            "store_name": "대화점",
        },
        {
            "project_id": "2548064",
            "post_id": "83047165",
            "parent_post_id": "83047164",
            "title": "신규 메뉴 (닭발, 찜닭) 희망",
            "content_text": "신규 메뉴 검토 요청",
            "store_name": "대화점",
        },
        {
            "project_id": "2548064",
            "post_id": "83047166",
            "parent_post_id": "83047165",
            "title": "답변에 따른 가맹점 2차 확인 요청(TEST)",
            "content_text": "가맹점에 추가 확인 요청",
            "store_name": "대화점",
        },
    ])
    comments = pd.DataFrame([
        {
            "project_id": "2548064",
            "post_id": "83047165",
            "comment_id": "1",
            "author_name": "담당자",
            "content_text": "검토하겠습니다.",
            "written_at": "20260813090000",
            "is_system": False,
        },
        {
            "project_id": "2548064",
            "post_id": "83047166",
            "comment_id": "2",
            "author_name": "담당자",
            "content_text": "추가 확인했습니다.",
            "written_at": "20260814090000",
            "is_system": False,
        },
    ])
    projects = pd.DataFrame([
        {"project_id": "2548064", "store_name": "대화점", "project_name": "경기_대화점"},
    ])
    frames = iter([post, comments, projects])
    monkeypatch.setattr(flow_visit, "_read_parquet_first", lambda paths, required=True: next(frames))

    payload = flow_visit.extract_visit_logs(
        dag_run=_DagRun({"flow_visit_targets": [{"store_name": "대화점", "project_id": "2548064"}]})
    )

    assert [row["post_id"] for row in payload["posts"]] == ["83047164"]
    assert [row["post_id"] for row in payload["subtasks"]] == ["83047165", "83047166"]
    nested = payload["subtasks"][1]
    assert nested["visit_parent_post_id"] == "83047164"
    assert nested["direct_parent_post_id"] == "83047165"
    assert nested["direct_parent_title"] == "신규 메뉴 (닭발, 찜닭) 희망"
    assert nested["subtask_depth"] == 2
    assert nested["subtask_path_titles"] == "신규 메뉴 (닭발, 찜닭) 희망 > 답변에 따른 가맹점 2차 확인 요청(TEST)"
    assert [row["post_id"] for row in payload["comments"]] == ["83047165", "83047166"]


def test_extract_visit_logs_matches_visit_title_with_nonbreaking_spaces(monkeypatch):
    post = pd.DataFrame([
        {
            "project_id": "2548064",
            "post_id": "83047164",
            "parent_post_id": "",
            "title": "26.08.12\xa0(수)대화점\xa0방문일지",
            "content_text": "",
            "store_name": "대화점",
        },
    ])
    comments = pd.DataFrame([])
    projects = pd.DataFrame([
        {"project_id": "2548064", "store_name": "대화점", "project_name": "경기_대화점"},
    ])
    frames = iter([post, comments, projects])
    monkeypatch.setattr(flow_visit, "_read_parquet_first", lambda paths, required=True: next(frames))

    payload = flow_visit.extract_visit_logs(
        dag_run=_DagRun({"flow_visit_targets": [{"store_name": "대화점", "project_id": "2548064"}]})
    )

    assert [row["post_id"] for row in payload["posts"]] == ["83047164"]


def test_parse_visit_meta_appends_subtask_context_to_visit_body():
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "post_id": "83047164",
                "title": "26.08.12 (수)대화점 방문일지",
                "post_date": "2026-08-12",
                "content_text": "방문일자26.08.12\n방문목적 정기 점검",
            }
        ],
        "subtasks": [
            {
                "project_id": "2548064",
                "post_id": "83070915",
                "visit_parent_post_id": "83047164",
                "parent_post_id": "83047164",
                "title": "키워드 공략 방향성 제안",
                "content_text": "누룽지 닭한마리 키워드 효과 검토 요청",
            }
        ],
        "comments": [
            {
                "project_id": "2548064",
                "post_id": "83070915",
                "comment_id": "c1",
                "content_text": "브랜드 차원 키워드 활용 예정으로 답변",
                "written_at": "20260813100000",
                "is_system": False,
            }
        ],
    }

    result = flow_visit.parse_visit_meta(payload)
    content = result["posts"][0]["content_clean"]

    assert "[하위 글/업무]" in content
    assert "키워드 공략 방향성 제안" in content
    assert "브랜드 차원 키워드 활용 예정" in content


def test_flow_visit_todo_due_date_uses_only_explicit_deadline():
    assert flow_visit._parse_todo_due_date("다음 주 금요일까지 확인해주세요", "2026-08-12") == "2026-08-21"
    assert flow_visit._parse_todo_due_date("확인해주세요", "2026-08-12") is None


def test_save_visit_mart_writes_normalized_parquets_without_flat(monkeypatch):
    captured_parquet = {}
    captured_partitions = {}

    monkeypatch.setattr(
        flow_visit,
        "_write_project_partitions",
        lambda df, path, cols, project_ids: captured_partitions.setdefault(path, df.copy()),
    )
    monkeypatch.setattr(
        flow_visit,
        "_write_parquet_atomic",
        lambda df, path: captured_parquet.setdefault(path, df.copy()),
    )
    monkeypatch.setattr(flow_visit, "FLOW_STORE_PROFILE_PARQUET", Path("flow_store_profile.parquet"))

    payload = {
        "posts": [
            {
                "project_id": "2751521",
                "store_name": "양천목동점",
                "post_id": "100",
                "title": "26. 08. 04 (화) 양천목동점 방문일지",
                "visit_date": "2026-08-04",
                "registered_at": "20260804100000",
                "author_name": "심성준",
                "content_clean": "asd",
                "task_status": "진행",
                "progress": "50",
                "task_nm": "방문 후속 업무",
                "worker": "김대진",
                "start_dt": "2026-08-04",
                "end_dt": "2026-08-05",
                "issues": [],
            }
        ],
        "comments": [
            {
                "post_id": "100",
                "comment_id": "200",
                "author_name": "김대진",
                "content_text": "넵",
                "written_at": "20260804103000",
                "is_system": False,
            }
        ],
        "subtasks": [],
        "profiles": [],
    }

    message = flow_visit.save_visit_mart(payload)

    assert "flat=" not in message
    assert Path("flow_visit_flat.parquet") not in captured_parquet
    assert Path("flow_store_profile.parquet") in captured_parquet

    log_df = captured_partitions[flow_visit.FLOW_VISIT_LOG_PARQUET]
    profile_df = captured_parquet[Path("flow_store_profile.parquet")]
    todo_df = captured_partitions[flow_visit.FLOW_VISIT_TODO_PARQUET]
    snapshot_df = captured_partitions[flow_visit.FLOW_VISIT_PROFILE_SNAPSHOT_PARQUET]
    assert flow_visit.FLOW_VISIT_TODO_PARQUET in captured_partitions
    assert flow_visit.FLOW_VISIT_PROFILE_SNAPSHOT_PARQUET in captured_partitions
    assert "store_rel_key" in log_df.columns
    assert "visit_rel_key" in log_df.columns
    assert "owner_sentiment" not in log_df.columns
    assert "store_status_summary" in profile_df.columns
    assert "manager_memo" not in profile_df.columns
    assert "manager_memo" not in snapshot_df.columns
    assert "manager_request" not in todo_df.columns
    assert "task_status" in log_df.columns
    assert log_df.loc[0, "task_status"] == "진행"
    assert log_df.loc[0, "progress"] == "50"
    assert log_df.loc[0, "task_nm"] == "방문 후속 업무"
    assert log_df.loc[0, "worker"] == "김대진"
    assert log_df.loc[0, "start_dt"] == "2026-08-04"
    assert log_df.loc[0, "end_dt"] == "2026-08-05"


def test_save_visit_mart_writes_source_subtask_table(monkeypatch):
    captured_partitions = {}

    monkeypatch.setattr(
        flow_visit,
        "_write_project_partitions",
        lambda df, path, cols, project_ids: captured_partitions.setdefault(path, df.copy()),
    )
    monkeypatch.setattr(flow_visit, "_write_parquet_atomic", lambda df, path: None)

    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "post_id": "83047164",
                "title": "26.08.12 (수)대화점 방문일지",
                "visit_date": "2026-08-12",
                "issues": [],
                "hq_followups": [],
            }
        ],
        "subtasks": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "post_id": "83047165",
                "parent_post_id": "83047164",
                "post_url": "https://flow.team/l/subtask",
                "registered_at": "20260813094200",
                "author_name": "김대진 팀장",
                "title": "키워드 공략 방향성 제안",
                "task_nm": "키워드 공략 방향성 제안",
                "task_status": "",
                "worker": "상품 담당",
                "start_dt": "20260813",
                "end_dt": "20260818",
                "content_text": "오픈 후 복날을 2번 정도 경험을 했는데 작년 복날에는 큰 기대를 했으나 매출의 빛을 보지 못했습니다.",
            },
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "post_id": "83047166",
                "parent_post_id": "83047165",
                "visit_parent_post_id": "83047164",
                "direct_parent_post_id": "83047165",
                "direct_parent_title": "키워드 공략 방향성 제안",
                "subtask_depth": 2,
                "subtask_path_titles": "키워드 공략 방향성 제안 > 답변에 따른 가맹점 2차 확인 요청(TEST)",
                "post_url": "https://flow.team/l/nested",
                "registered_at": "20260814100000",
                "author_name": "조민준",
                "title": "답변에 따른 가맹점 2차 확인 요청(TEST)",
                "task_nm": "답변에 따른 가맹점 2차 확인 요청(TEST)",
                "task_status": "요청",
                "worker": "SV 담당",
                "start_dt": "20260814",
                "end_dt": "",
                "content_text": "답변에 따른 가맹점 2차 확인 요청",
            }
        ],
        "comments": [
            {
                "post_id": "83047165",
                "comment_id": "c1",
                "author_name": "오나영",
                "content_text": "현재 키워드와 관련하여 연계되어 있는 업무가 다수 있습니다.",
                "written_at": "20260813100000",
                "is_system": False,
                "sys_code": "",
                "reply_cnt": "1",
                "root_comment_id": "c1",
                "parent_comment_id": "",
                "comment_depth": "0",
                "comment_order": "0",
            },
            {
                "post_id": "83047165",
                "comment_id": "c1-1",
                "author_name": "김대진",
                "content_text": "해당 내용 토대로 가맹점 안내하고 추가 답변 있는지 확인 바랍니다.",
                "written_at": "20260818142300",
                "is_system": False,
                "sys_code": "",
                "reply_cnt": "0",
                "root_comment_id": "c1",
                "parent_comment_id": "c1",
                "comment_depth": "1",
                "comment_order": "1",
            },
            {
                "post_id": "83047165",
                "comment_id": "c3",
                "author_name": "조민준",
                "content_text": "테스트용 댓글입니다 1",
                "written_at": "20260818184300",
                "is_system": False,
                "sys_code": "",
                "reply_cnt": "2",
                "root_comment_id": "c3",
                "parent_comment_id": "",
                "comment_depth": "0",
                "comment_order": "3",
            },
            {
                "post_id": "83047165",
                "comment_id": "c3-1",
                "author_name": "조민준",
                "content_text": "테스트용 댓글입니다 1-2",
                "written_at": "20260818184400",
                "is_system": False,
                "sys_code": "",
                "reply_cnt": "0",
                "root_comment_id": "c3",
                "parent_comment_id": "c3",
                "comment_depth": "1",
                "comment_order": "4",
            },
            {
                "post_id": "83047165",
                "comment_id": "c2",
                "author_name": "황유경",
                "content_text": "'진행' → '완료', 상태를 변경하였습니다. '2026-08-18', 마감일을 추가하였습니다.",
                "written_at": "20260813103000",
                "is_system": True,
                "sys_code": "S45^^1^^2@$%S47^^2026-08-13@$%S48^^2026-08-18@$%",
                "reply_cnt": "0",
                "root_comment_id": "c2",
                "parent_comment_id": "",
                "comment_depth": "0",
                "comment_order": "2",
            },
        ],
        "profiles": [],
        "profile_snapshots": [],
        "todos": [],
    }

    message = flow_visit.save_visit_mart(payload)

    subtask_df = captured_partitions[flow_visit.FLOW_VISIT_SUBTASK_PARQUET]
    assert "subtask=2" in message
    assert len(subtask_df) == 2
    assert subtask_df.loc[0, "parent_post_id"] == "83047164"
    assert subtask_df.loc[0, "direct_parent_post_id"] == "83047164"
    assert subtask_df.loc[0, "subtask_depth"] == 1
    assert subtask_df.loc[0, "subtask_post_id"] == "83047165"
    assert subtask_df.loc[0, "visit_date"] == "2026-08-12"
    assert subtask_df.loc[0, "registered_date"] == "2026-08-13"
    assert subtask_df.loc[0, "registered_time"] == "09:42"
    assert subtask_df.loc[0, "author_name"] == "김대진 팀장"
    assert subtask_df.loc[0, "title"] == "키워드 공략 방향성 제안"
    assert subtask_df.loc[0, "task_status"] == "완료"
    assert subtask_df.loc[0, "start_dt"] == "2026-08-13"
    assert subtask_df.loc[0, "end_dt"] == "2026-08-18"
    assert "오픈 후 복날을 2번 정도 경험" in subtask_df.loc[0, "content_text"]
    assert subtask_df.loc[0, "comment_group"] == "일반댓글/대댓글있음,상태변경/시작일변경/마감일변경"
    assert subtask_df.loc[0, "comment_author"] == "오나영,황유경,김대진,조민준"
    assert "현재 키워드와 관련하여 연계되어 있는 업무가 다수 있습니다." in subtask_df.loc[0, "comment_text"]
    assert "테스트용 댓글입니다 1-2" in subtask_df.loc[0, "comment_text"]
    assert "'진행' → '완료', 상태를 변경하였습니다." in subtask_df.loc[0, "comment_text"]
    assert "'2026-08-18', 마감일을 추가하였습니다." in subtask_df.loc[0, "comment_text"]
    history = json.loads(subtask_df.loc[0, "comment_history_json"])
    assert [group["group_author"] for group in history["groups"]] == ["오나영", "조민준"]
    assert [comment["author_name"] for comment in history["groups"][0]["comments"]] == ["오나영", "김대진"]
    assert [comment["content_text"] for comment in history["groups"][1]["comments"]] == [
        "테스트용 댓글입니다 1",
        "테스트용 댓글입니다 1-2",
    ]
    assert len(history["system_events"]) == 1
    assert history["system_events"][0]["author_name"] == "황유경"
    nested = subtask_df[subtask_df["subtask_post_id"].eq("83047166")].iloc[0]
    assert nested["parent_post_id"] == "83047164"
    assert nested["direct_parent_post_id"] == "83047165"
    assert nested["direct_parent_title"] == "키워드 공략 방향성 제안"
    assert nested["subtask_depth"] == 2
    assert nested["subtask_path_titles"] == "키워드 공략 방향성 제안 > 답변에 따른 가맹점 2차 확인 요청(TEST)"
    assert nested["visit_date"] == "2026-08-12"


def test_latest_status_from_comments_uses_latest_system_event_text():
    comments = [
        {
            "content_text": "'요청' → '진행', 상태를 변경하였습니다.",
            "written_at": "20260813100000",
            "is_system": True,
            "sys_code": "S45^^0^^1",
        },
        {
            "content_text": "'진행' → '완료', 상태를 변경하였습니다.",
            "written_at": "20260818100000",
            "is_system": True,
            "sys_code": "S45^^1^^2",
        },
    ]

    assert flow_visit._latest_status_from_comments(comments) == "완료"


def test_latest_status_from_comments_falls_back_to_s45_sys_code():
    comments = [
        {
            "content_text": "",
            "written_at": "20260813100000",
            "is_system": True,
            "sys_code": "S45^^0^^1@$%S47^^2026-08-13",
        }
    ]

    assert flow_visit._latest_status_from_comments(comments) == "진행"


def test_subtask_status_prefers_system_comment_over_source_status():
    subtask = {"task_status": "진행", "STTS": "1"}
    comments = [
        {
            "content_text": "'진행' → '완료', 상태를 변경하였습니다.",
            "written_at": "20260818100000",
            "is_system": True,
            "sys_code": "S45^^1^^2",
        }
    ]

    assert flow_visit._task_status_from_subtask(subtask, comments) == "완료"


def test_subtask_status_falls_back_to_stts_code_without_comments():
    assert flow_visit._task_status_from_subtask({"task_status": "", "STTS": "2"}, []) == "완료"


def test_flow_visit_short_heading_is_not_classified_from_store_hints():
    taxonomy = [
        {"key": "계육_순살품질", "category": "사입", "aliases": ["순살 퀄리티"]},
        {"key": "매출_홀부진", "category": "매출", "aliases": ["홀매출"]},
        {"key": "기타", "category": "미분류", "aliases": []},
    ]
    segment = {
        "topic": "홀관련",
        "raw_text": "4. 홀관련",
        "owner_voice_raw": "",
        "sv_action_raw": "",
    }

    candidates = flow_visit_prompts.select_issue_candidates("동탄영천점", segment, taxonomy)

    assert [row["key"] for row in candidates] == ["기타"]
    assert not flow_visit._segment_has_issue_signal(segment, candidates)


def test_flow_visit_profile_prompt_builds_json_only_history_prompt():
    prompt, system_prompt = flow_visit_prompts.build_profile_prompt({
        "store_name": "테스트점",
        "latest_visit": {"visit_date": "2026-08-12", "issues": []},
    })

    assert "자연스럽게 요약" in system_prompt
    assert "점수" in prompt
    assert "handling_points: key_concerns 중 점주가 곤란해하고 담당자가 해결·대응해야 하는 현재 문제·고민만" in prompt
    assert "미해결·진행중 상태만으로 문제·고민으로 보지 않는다" in prompt
    assert "owner_status" in prompt
    assert "todos" in prompt
    assert "테스트점" in prompt


def test_flow_visit_local_profile_uses_only_gpt_oss_models(monkeypatch):
    captured = {}

    def fake_client():
        return object(), ["qwen2.5:14b", "gpt-oss:20b"]

    def fake_query(prompt, **kwargs):
        captured.update(kwargs)
        return {"owner_status": "후속 확인을 기다리는 상태"}

    monkeypatch.setattr(flow_visit, "PROFILE_LLM_PROVIDER", "local")
    monkeypatch.setenv("FLOW_VISIT_PROFILE_LLM_MODELS", "qwen2.5:14b,gpt-oss:20b")
    monkeypatch.setattr(qwen_client, "get_ollama_client_with_candidates", fake_client)
    monkeypatch.setattr(qwen_client, "query_qwen_json", fake_query)

    result, model = flow_visit._query_history_profile_json({
        "store_name": "테스트점",
        "latest_visit": {"issues": []},
        "recurring_patterns": [],
        "unresolved_issues": [],
        "explicit_requests": [],
    })

    assert result["owner_status"] == "후속 확인을 기다리는 상태"
    assert model == "local_gpt_oss_profile:gpt-oss:20b"
    assert captured["model_candidates"] == ["gpt-oss:20b"]
    assert captured["preferred_models"] == ["gpt-oss:20b"]


def test_flow_visit_local_profile_digest_is_compact():
    digest = {
        "store_name": "테스트점",
        "project_id": "1",
        "last_visit_date": "2026-08-12",
        "visit_count": 10,
        "latest_visit": {
            "issues": [
                {"issue_key": f"k{i}", "issue_label": f"라벨{i}", "owner_voice": "가" * 300, "post_id": f"p{i}", "issue_seq": i}
                for i in range(20)
            ]
        },
        "recurring_patterns": [
            {"issue_key": f"r{i}", "issue_label": f"반복{i}", "cnt": i, "dates": ["2026-01-01", "2026-02-01"]}
            for i in range(20)
        ],
        "unresolved_issues": [
            {"issue_key": f"u{i}", "issue_label": f"미해결{i}", "owner_voice": "나" * 300, "post_id": f"u{i}", "issue_seq": i}
            for i in range(20)
        ],
        "explicit_requests": [
            {"issue_key": f"q{i}", "issue_label": f"요청{i}", "owner_voice": "다" * 300, "post_id": f"q{i}", "issue_seq": i, "is_request": True}
            for i in range(20)
        ],
        "visit_history": [{"content": "라" * 10000}],
    }

    compact = flow_visit._compact_local_profile_digest(digest)
    prompt, _ = flow_visit_prompts.build_profile_prompt(compact)

    assert "visit_history" not in compact
    assert len(compact["local_candidates"]["latest"]) == 6
    assert len(compact["local_candidates"]["recurring"]) == 6
    assert len(compact["local_candidates"]["unresolved"]) == 6
    assert len(compact["local_candidates"]["requests"]) == 5
    assert len(prompt) <= 6000


def test_flow_visit_store_traits_do_not_promote_activity_frequency():
    issues = [
        {
            "issue_key": "기타",
            "issue_label": "간담회참석",
            "visit_date": f"2026-0{i}-01",
            "owner_voice": "간담회 참석 가능 여부를 확인함",
            "raw_text": "간담회 참석 안내",
        }
        for i in range(1, 4)
    ]

    traits = flow_visit._store_trait_texts(issues, {"기타": 3})

    assert traits == ["누적 히스토리에서 반복 특성을 더 확인해야 함"]
    assert not any("간담회" in item or "3회" in item for item in traits)


def test_flow_visit_store_traits_interpret_repeated_owner_reaction():
    issues = [
        {
            "issue_key": "광고_우가클단가",
            "issue_label": "우가클 단가",
            "visit_date": "2026-06-01",
            "owner_voice": "우가클 광고비 대비 주문 증가 효과가 있는지 확인하고 싶다고 함",
            "raw_text": "광고 효율과 주문 전환 확인",
        },
        {
            "issue_key": "수익_감소체감",
            "issue_label": "순이익",
            "visit_date": "2026-07-01",
            "owner_voice": "도리당만 분리한 순이익과 광고비 제외 후 남는 금액을 부담스러워함",
            "raw_text": "순이익 20% 미만 우려",
        },
        {
            "issue_key": "광고_우가클단가",
            "issue_label": "우가클 단가",
            "visit_date": "2026-08-01",
            "owner_voice": "광고를 줄이고 싶어하며 실제 수익 근거를 요청",
            "raw_text": "광고 효과 확인 요청",
        },
    ]

    traits = flow_visit._store_trait_texts(
        issues,
        {"광고_우가클단가": 2, "수익_감소체감": 1},
    )

    joined = " ".join(traits)
    assert "광고" in joined
    assert "주문" in joined or "수익" in joined
    assert len(traits) <= 3
    assert not any("광고_우가클단가" in item or "2회" in item for item in traits)


def test_flow_visit_profile_digest_carries_problem_context():
    issue = {
        "visit_date": "2026-08-12",
        "post_id": "83047164",
        "issue_seq": 1,
        "issue_key": "수익_감소체감",
        "issue_label": "순이익",
        "category": "매출",
        "owner_voice": "도리당만 분리한 순이익이 20% 미만일까 우려",
        "status": "미해결",
        "raw_text": "순이익과 광고비 제외 후 남는 금액 확인 요청",
    }
    post = {
        "store_name": "대화점",
        "project_id": "2548064",
        "visit_date": "2026-08-12",
        "post_id": "83047164",
        "issues": [issue],
    }

    row = flow_visit._history_issue_row(issue)
    digest = flow_visit._build_store_history_digest("대화점", [post], [issue], [])
    compact = flow_visit._compact_local_profile_digest(digest)
    latest = compact["local_candidates"]["latest"][0]

    assert row["is_problem"] is True
    assert "순이익" in row["problem_detail"]
    assert latest["problem"] is True
    assert latest["category"] == "매출"
    assert "순이익" in latest["problem_detail"]


def test_flow_visit_handover_summary_is_short_mece_bullets():
    summary = flow_visit._build_handover_summary(
        ["광고 집행 여부를 실제 주문 증가와 수익 근거를 보고 판단하는 편"],
        [
            {
                "issue_key": "수익_감소체감",
                "issue_label": "순이익",
                "owner_voice": "도리당 단독 순이익 20% 미만 우려",
                "sv_action": "광고세팅값 동일 시 순이익 변동 체감 확인 요청",
                "status": "미해결",
            }
        ],
        ["도리당 단독 순이익 산출 가능 여부 확인"],
    )
    rows = summary.splitlines()

    assert "반복 성향:" not in summary
    assert "주의 화두:" not in summary
    assert "기존 대응:" not in summary
    assert "가맹점이다" not in summary
    assert 1 <= len(rows) <= 3
    assert all(row.startswith("- ") for row in rows)
    assert "광고 집행 여부를 실제 주문 증가와 수익 근거를 보고 판단하는 편" not in summary
    assert "순이익 변동 자료 준비" in summary


def test_flow_visit_handover_summary_dedupes_action_by_axis():
    summary = flow_visit._build_handover_summary(
        [],
        [
            {
                "issue_key": "수익_감소체감",
                "issue_label": "순이익",
                "owner_voice": "순이익 부담",
                "status": "미해결",
            },
            {
                "issue_key": "토더_설정공지",
                "issue_label": "토더 설정/공지",
                "owner_voice": "공지 확인을 소홀히 한다",
                "status": "미해결",
            },
        ],
        ["광고세팅값 조건 동일시 순이익 데이터 회신", "공지사항 확인 여부 재확인"],
    )
    rows = summary.splitlines()

    assert 1 <= len(rows) <= 3
    assert all(row.startswith("- ") for row in rows)
    assert sum("순이익" in row or "광고" in row or "데이터" in row for row in rows) == 1
    assert sum("토더" in row or "공지" in row for row in rows) == 1


def test_flow_visit_next_action_fallback_is_executable_sentence():
    result = flow_visit._build_next_visit_action([], [])

    assert result.endswith("확인한다")
    assert result != "확인 필요"


def test_flow_visit_profile_copy_keeps_top_three_traits_and_two_actions():
    issues = [
        {
            "issue_key": "수익_감소체감",
            "issue_label": "순이익",
            "visit_date": "2026-06-01",
            "owner_voice": "순이익과 남는 금액 부담을 확인하고 싶다고 함",
            "raw_text": "순이익 부담",
            "status": "미해결",
        },
        {
            "issue_key": "수익_감소체감",
            "issue_label": "순이익",
            "visit_date": "2026-07-01",
            "owner_voice": "광고비 제외 후 남는 금액을 부담스러워함",
            "raw_text": "수익 부담",
            "status": "미해결",
        },
        {
            "issue_key": "광고_우가클단가",
            "issue_label": "우가클 단가",
            "visit_date": "2026-07-01",
            "owner_voice": "광고비 대비 주문 증가 효과를 확인하고 싶다고 함",
            "raw_text": "광고 효과 확인",
            "status": "미해결",
        },
        {
            "issue_key": "광고_우가클단가",
            "issue_label": "우가클 단가",
            "visit_date": "2026-08-01",
            "owner_voice": "광고를 줄이고 싶어하며 실제 수익 근거를 요청",
            "raw_text": "광고 수익 근거 요청",
            "status": "미해결",
        },
        {
            "issue_key": "메뉴_신메뉴도입",
            "issue_label": "신메뉴 도입",
            "visit_date": "2026-07-01",
            "owner_voice": "신메뉴 출시와 판매 키워드 확대에 관심",
            "raw_text": "신메뉴 관심",
            "status": "미해결",
        },
        {
            "issue_key": "메뉴_신메뉴도입",
            "issue_label": "신메뉴 도입",
            "visit_date": "2026-08-01",
            "owner_voice": "신메뉴 추가 가능성과 키워드 활용을 확인",
            "raw_text": "키워드 확장 관심",
            "status": "미해결",
        },
        {
            "issue_key": "기타",
            "issue_label": "간담회참석",
            "visit_date": "2026-08-01",
            "owner_voice": "간담회 참석 여부 확인",
            "raw_text": "간담회 참석 안내",
            "status": "미해결",
        },
    ]

    traits = flow_visit._store_trait_texts(
        issues,
        {
            "수익_감소체감": 2,
            "광고_우가클단가": 2,
            "메뉴_신메뉴도입": 2,
            "기타": 7,
        },
    )
    action = flow_visit._build_next_visit_action(
        [{"todo_list": "순이익 자료 확인"}, {"todo_list": "광고 전후 주문 비교"}, {"todo_list": "토더 설정 재시연"}],
        [],
    )

    assert len(traits) <= 3
    assert not any("간담회" in item or "7회" in item for item in traits)
    assert len(action.split(" → ")) == 2


def test_flow_visit_store_status_summary_compacts_traits_by_mece_axis():
    traits = flow_visit._compact_store_trait_texts(
        [
            "비용을 제외한 실제 수익성과 남는 금액을 중요하게 확인하는 편",
            "실제 남는 금액과 수익성 근거를 확인한 뒤 판단하는 편",
            "광고 집행 여부를 실제 주문 증가와 수익 근거를 보고 판단하는 편",
            "광고는 비용 대비 효과를 확인한 뒤 판단하는 편",
            "신규 메뉴와 판매 키워드 확장에 관심을 두고 진행 가능성을 확인하는 편",
            "신규 메뉴와 판매 확대에는 비교적 적극적인 편",
            "토더 공지는 현장에서 다시 확인시키는 안내가 필요한 편",
            "간담회 참석 7회",
        ],
        3,
    )

    assert traits == [
        "수익성과 실제 남는 금액을 중요하게 확인하는 편",
        "광고는 실제 주문·수익 효과를 확인한 뒤 판단하는 편",
        "신규 메뉴와 판매 확대에는 비교적 적극적인 편",
    ]


def test_flow_visit_normalized_profile_treats_llm_outputs_as_judgment_results():
    fallback = {
        "owner_status": "최근 방문 이슈를 기준으로 추가 확인이 필요한 상태",
        "store_status_summary": "광고 집행 여부를 실제 주문 증가와 수익 근거를 보고 판단하는 편\n신규 메뉴와 판매 키워드 확장에 관심을 두고 진행 가능성을 확인하는 편",
        "key_concerns": ["순이익", "토더 설정 교육"],
        "handling_points": ["순이익"],
        "handover_summary": "- 광고 동일 조건의 순이익 변동 자료 준비",
        "next_visit_action": "순이익 자료 확인 → 토더 설정 완료 여부 확인",
        "analysis_evidence": [
            {
                "concern": "순이익",
                "issue_key": "수익_감소체감",
                "issue_label": "순이익",
                "problem_detail": "순이익 부담",
                "owner_voice": "순이익 부담",
                "status": "미해결",
                "evidence": "순이익 부담",
                "is_problem": True,
            }
        ],
    }
    result = {
        "owner_status": "광고 효과를 확인하려는 상태",
        "store_status_summary": [
            "간담회 참석 7회",
            "광고 집행 여부를 실제 주문 증가와 수익 근거를 보고 판단하는 편",
            "광고는 비용 대비 효과를 확인한 뒤 판단하는 편",
            "신규 메뉴와 판매 키워드 확장에 관심을 두고 진행 가능성을 확인하는 편",
            "토더 공지는 현장에서 다시 확인시키는 안내가 필요한 편",
        ],
        "key_concerns": ["순이익", "토더 설정 교육"],
        "handling_points": ["순이익"],
        "handover_summary": (
            "다음 방문 전 광고 성과 자료와 토더 설정 안내 이력을 확인한다.\n"
            "- 순이익\n"
            "- 토더 설정 교육"
        ),
        "next_visit_action": ["순이익 자료 확인", "토더 설정 완료 여부 확인", "신메뉴 의견 확인"],
        "analysis_evidence": fallback["analysis_evidence"],
    }

    profile = flow_visit._normalize_history_profile_result(result, fallback)
    traits = profile["store_status_summary"].splitlines()

    assert len(traits) <= 3
    assert not any("간담회" in item or "7회" in item for item in traits)
    assert not any("토더" in item for item in traits)
    assert sum("광고" in item for item in traits) == 1
    assert len(profile["next_visit_action"].split(" → ")) == 2
    assert "신메뉴 의견 확인" not in profile["next_visit_action"]
    handover_rows = profile["handover_summary"].splitlines()
    assert 1 <= len(handover_rows) <= 3
    assert all(row.startswith("- ") for row in handover_rows)
    assert "- 순이익" not in handover_rows
    assert "- 토더 설정 교육" not in handover_rows


def test_flow_visit_viz_display_fields_do_not_render_blank_actions():
    payload = {
        "posts": [
            {
                "project_id": "2466857",
                "store_name": "동탄영천점",
                "visit_date": "2026-06-23",
                "post_id": "1",
                "issues": [
                    {
                        "issue_key": "계육_뼈닭내장",
                        "issue_label": "뼈닭 내장",
                        "category": "사입",
                        "owner_voice": "내장 제거 개선 요청",
                        "sv_action": "",
                        "status": "미해결",
                    }
                ],
            }
        ],
        "profiles": [
            {
                "project_id": "2466857",
                "analysis_evidence": [
                    {
                        "concern": "뼈닭 내장 제거",
                        "issue_key": "계육_뼈닭내장",
                        "issue_rel_key": "2466857|1|1",
                        "action_hint": "계육 품질 개선 가능 여부 확인",
                        "status": "미해결",
                        "is_problem": True,
                    }
                ],
            }
        ],
    }

    df = flow_visit_viz.build_visit_viz_table(payload, "2026-08-04T00:00:00", "test")

    # 없다는 사실을 글자로 채우지 않는다.
    assert df.loc[0, "sv_summary"] == ""
    assert bool(df.loc[0, "concern_is_problem"]) is True
    # 후속 상황은 따옴표 문자열이 아니라 컬럼으로 나뉜다.
    assert df.loc[0, "followup_reply"] == ""
    assert df.loc[0, "followup_state"] == "미해결"
    assert df.loc[0, "followup_next"] == "계육 품질 개선 가능 여부 확인"
    # followup_summary는 문제·고민 행에서만 문제 상세 요약을 담는다.
    assert df.loc[0, "followup_summary"] == df.loc[0, "issue_problem_detail"]
    assert df.loc[0, "issue_problem_detail"] == "내장 제거 개선 요청"
    assert df.loc[0, "issue_action_detail"] == "계육 품질 개선 가능 여부 확인"
    assert df.loc[0, "issue_evidence_text"] == "내장 제거 개선 요청"
    assert "issue_followup_detail" not in df.columns
    assert "owner_sentiment" not in df.columns
    assert "manager_memo" not in df.columns
    assert "visit_checklist" not in df.columns
    assert "response_status_summary" not in df.columns


def test_flow_visit_profile_compresses_dongtan_store_specific_concerns():
    payload = {
        "posts": [
            {
                "project_id": "2466857",
                "store_name": "동탄영천점",
                "visit_date": "2026-06-23",
                "post_id": "1",
                "issues": [
                    {
                        "issue_key": "계육_순살품질",
                        "issue_label": "순살 품질",
                        "category": "사입",
                        "owner_voice": "순살 퀄리티가 이취 발생했다가 발생하지 않았다가 들쑥날쑥해 개선 요청",
                        "raw_text": "닭이 메인 식재료라 퀄리티가 좋아야 한다고 하심",
                        "opinion_source": "점주직접",
                        "is_request": True,
                        "severity": "보통",
                        "status": "미해결",
                    },
                    {
                        "issue_key": "인테리어_내부디자인",
                        "issue_label": "매장 내부 디자인",
                        "category": "가맹점의견",
                        "owner_voice": "홀 내부 디자인 요청했으나 지속적으로 틀리게 나와 보완 요청",
                        "raw_text": "내부 디자인 추가 보완 진행 예정",
                        "opinion_source": "점주직접",
                        "is_request": True,
                        "severity": "보통",
                        "status": "진행중",
                    },
                    {
                        "issue_key": "매출_홀부진",
                        "issue_label": "홀 매출 부진",
                        "category": "매출",
                        "owner_voice": "홀매출 20만원으로 월세와 관리비 부담이 큼",
                        "raw_text": "월세 250만원 관리비 55만원 고정비 지출",
                        "opinion_source": "점주직접",
                        "is_request": False,
                        "severity": "높음",
                        "status": "미해결",
                    },
                ],
            }
        ]
    }

    result = flow_visit.build_store_profile(payload)
    profile = result["profiles"][0]

    assert profile["key_concerns"] == [
        "순살 품질",
        "매장 내부 디자인 보완",
        "홀 매출 부진·고정비 부담",
    ]
    # 문제·고민은 점주가 곤란해하는 것만이다.
    # 순살 품질(품질 불만)과 홀 매출 부진·고정비 부담(부담)은 들어오고,
    # 매장 내부 디자인 보완(요청)은 화두로만 남는다.
    assert profile["handling_points"] == ["순살 품질", "홀 매출 부진·고정비 부담"]
    assert set(profile["handling_points"]) < set(profile["key_concerns"])
    assert "품질 개선 가능 여부 확인" in profile["handover_summary"]
    assert "manager_memo" not in profile
    assert profile["store_rel_key"] == "2466857|동탄영천점"
    assert len(result["todos"]) == 2
    assert "순살 품질" in result["todos"][0]["todo_list"]
    assert "manager_request" not in result["todos"][0]
    assert result["todos"][0]["visit_rel_key"] == "2466857|1"
    assert result["todos"][0]["todo_status"] in {"대기", "진행"}
    assert all(len(row) <= 24 for row in profile["key_concerns"])


def test_flow_visit_profile_builds_handover_copy_for_store_visit():
    payload = {
        "posts": [
            {
                "project_id": "2742104",
                "store_name": "용인동천점",
                "visit_date": "2026-06-01",
                "post_id": "78555117",
                "llm_model": "rule_storefit",
                "issues": [
                    {
                        "issue_key": "매출_배달정체",
                        "issue_label": "매장현황 - 매출이 오",
                        "category": "매출",
                        "owner_voice": "매출이 1400~1500만원 선을 유지하고 쿠폰을 빼면 바로 빠져 답답하시다고 함",
                        "raw_text": "매출이 너무 오르지 않아서 답답하시다고 함",
                        "opinion_source": "점주직접",
                        "is_request": False,
                        "severity": "보통",
                        "status": "미해결",
                    },
                    {
                        "issue_key": "우거지_질김",
                        "issue_label": "우거지는 현재 상태도",
                        "category": "사입",
                        "owner_voice": "우거지가 예전보다 질긴 것 같아서 조금 더 삶아서 작업했으면 좋겠다고 하심",
                        "raw_text": "우거지는 현재 상태도 좋으나 예전보다 질긴것 같아서 삶고 있음",
                        "opinion_source": "점주직접",
                        "is_request": True,
                        "severity": "보통",
                        "status": "미해결",
                    },
                        {
                            "issue_key": "메뉴_1인메뉴확대",
                            "issue_label": "1인 묵은지 도리탕 쿠",
                            "category": "가맹점의견",
                            "owner_voice": "쿠팡에 1인 묵은지 도리탕 등록하길 희망",
                            "raw_text": "쿠팡에 1인 묵은지 도리탕 등록 요청",
                            "opinion_source": "점주직접",
                            "is_request": True,
                            "severity": "보통",
                            "status": "진행중",
                        },
                        {
                            "issue_key": "수익_감소체감",
                            "issue_label": "순수익 감소 체감",
                            "category": "매출",
                            "owner_voice": "순수익이 월 300~400만원 정도로 체감되어 생활이 걱정된다고 함",
                            "raw_text": "많아야 300~400만원 정도 남는 것 같다고 함",
                            "opinion_source": "점주직접",
                            "is_request": False,
                            "severity": "높음",
                            "status": "미해결",
                        },
                    ],
                },
            {
                "project_id": "2742104",
                "store_name": "용인동천점",
                "visit_date": "2026-03-18",
                "post_id": "75060833",
                "llm_model": "rule_storefit",
                "issues": [
                    {
                        "issue_key": "광고_우가클단가",
                        "issue_label": "우가클",
                        "category": "광고",
                        "owner_voice": "우가클 800원에서 600원으로 낮춰 흐름을 보기로 함",
                        "raw_text": "우가클 노출수 대비 전환율이 낮음",
                        "opinion_source": "담당자판단",
                        "is_request": False,
                        "severity": "보통",
                        "status": "미해결",
                    }
                ],
            },
        ]
    }

    result = flow_visit.build_store_profile(payload)
    profile = result["profiles"][0]

    assert "점" not in profile["owner_status"]
    assert "수익성" in profile["owner_status"]
    assert "배달 매출 1,400~1,500만원 정체" in profile["key_concerns"]
    assert "묵은지·우거지 품질" in profile["key_concerns"]
    assert "쿠팡 1인 메뉴 등록" in profile["key_concerns"]
    assert len(profile["key_concerns"]) <= 10
    assert any("수익" in row or "실적" in row or "자료" in row for row in profile["handling_points"])
    assert "반복 성향:" not in profile["handover_summary"]
    assert "주의 화두:" not in profile["handover_summary"]
    assert "기존 대응:" not in profile["handover_summary"]
    assert "가맹점이다" not in profile["handover_summary"]
    handover_rows = profile["handover_summary"].splitlines()
    assert 1 <= len(handover_rows) <= 3
    assert all(row.startswith("- ") for row in handover_rows)
    assert len(profile["store_status_summary"].splitlines()) <= 3
    assert len(profile["next_visit_action"].split(" → ")) <= 2
    assert "미해결/진행중" not in profile["handover_summary"]
    assert "manager_memo" not in profile
    assert len(result["todos"]) == 2
    assert {row["todo_owner"] for row in result["todos"]} == {"상품 담당", "SV 담당"}
    assert all(row["todo_due_date"] is None for row in result["todos"])
    assert any("쿠팡 1인 메뉴 등록 현황" in row["todo_list"] for row in result["todos"])
    assert all("현재 상태도" not in row for row in profile["key_concerns"])
    assert all("1인 묵은지 도리탕 쿠" not in row for row in profile["key_concerns"])
    assert payload["posts"][0]["issues"][1]["issue_label"] == "우거지 질김"


def test_flow_visit_profile_hides_generic_visit_issue_label_from_display_lists():
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "83047164",
                "issues": [
                    {
                        "issue_key": "기타",
                        "issue_label": "방문일지 주요 내용",
                        "category": "미분류",
                        "owner_voice": "도리당만 분리해서 계산을 해보진 않았으며 두 브랜드 합쳐서 9천만원 정도 유지",
                        "raw_text": "도리당만 분리해서 계산을 해보진 않았으며 두 브랜드 합쳐서 9천만원 정도 유지",
                        "opinion_source": "담당자판단",
                        "is_request": False,
                        "severity": "보통",
                        "status": "미해결",
                    },
                    {
                        "issue_key": "토더_설정공지",
                        "issue_label": "토더 설정/공지",
                        "category": "가맹점의견",
                        "owner_voice": "토더 공지를 제대로 확인하지 않는 경우가 많음",
                        "raw_text": "토더 공지 확인 방법 현장 안내 필요",
                        "opinion_source": "점주직접",
                        "is_request": True,
                        "severity": "보통",
                        "status": "미해결",
                    },
                ],
            }
        ]
    }

    result = flow_visit.build_store_profile(payload)
    profile = result["profiles"][0]
    joined = "\n".join(profile["key_concerns"] + profile["handling_points"])

    assert "방문일지 주요 내용:" not in joined
    assert "방문일지 주요 내용 처리 상태 확인" not in joined
    assert any("도리당만 분리" in row for row in profile["key_concerns"])
    assert "토더 공지 확인과 설정 교육 필요" in profile["key_concerns"]
    # 본사가 먼저 꺼낸 안내·교육 과제는 화두이지 점주의 고민이 아니다.
    assert "토더 공지 확인과 설정 교육 필요" not in profile["handling_points"]
    assert set(profile["handling_points"]) <= set(profile["key_concerns"])


def test_flow_visit_profile_snapshot_splits_month_periods_and_uses_visit_asof():
    payload = {
        "posts": [
            {
                "project_id": "1",
                "store_name": "테스트점",
                "visit_date": "2026-06-18",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_key": "광고_즉시할인",
                        "issue_label": "즉시할인 광고",
                        "category": "광고",
                        "owner_voice": "광고 효과를 확인 중",
                        "raw_text": "광고 효과를 확인 중",
                        "opinion_source": "점주직접",
                        "is_request": False,
                        "severity": "보통",
                        "status": "해결",
                    }
                ],
            },
            {
                "project_id": "1",
                "store_name": "테스트점",
                "visit_date": "2026-06-20",
                "post_id": "p2",
                "issues": [
                    {
                        "issue_key": "수익_감소체감",
                        "issue_label": "순수익 감소 체감",
                        "category": "매출",
                        "owner_voice": "실제 남는 금액이 적어 걱정",
                        "raw_text": "실제 남는 금액이 적어 걱정",
                        "opinion_source": "점주직접",
                        "is_request": False,
                        "severity": "높음",
                        "status": "미해결",
                    }
                ],
            },
        ]
    }

    result = flow_visit.build_store_profile(payload)
    snapshots = sorted(result["profile_snapshots"], key=lambda row: row["visit_date"])

    assert [(row["period_start"], row["period_end"]) for row in snapshots] == [
        ("2026-06-01", "2026-06-18"),
        ("2026-06-19", "2026-06-20"),
    ]
    assert snapshots[0]["visit_cnt_as_of"] == 1
    assert snapshots[1]["visit_cnt_as_of"] == 2
    assert snapshots[0]["key_concerns"] == ["즉시할인 광고 효율"]
    assert snapshots[1]["key_concerns"] == ["순수익 감소 체감"]
    assert all("광고 효율 불확실" not in row for row in snapshots[1]["handling_points"])


def test_flow_visit_handling_points_stay_subset_of_latest_concerns():
    payload = {
        "posts": [
            {
                "project_id": "1",
                "store_name": "테스트점",
                "visit_date": "2026-06-01",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_key": "계육_뼈닭내장",
                        "issue_label": "뼈닭 내장 제거",
                        "category": "사입",
                        "owner_voice": "뼈닭 내장과 손질 상태가 계속 불편함",
                        "raw_text": "뼈닭 내장과 손질 상태가 계속 불편함",
                        "opinion_source": "점주직접",
                        "is_request": False,
                        "severity": "보통",
                        "status": "해결",
                    }
                ],
            },
            {
                "project_id": "1",
                "store_name": "테스트점",
                "visit_date": "2026-06-10",
                "post_id": "p2",
                "issues": [
                    {
                        "issue_key": "계육_뼈닭내장",
                        "issue_label": "뼈닭 내장 제거",
                        "category": "사입",
                        "owner_voice": "요청은 아니지만 뼈닭 손질 상태를 다시 신경 쓰고 있음",
                        "raw_text": "요청은 아니지만 뼈닭 손질 상태를 다시 신경 쓰고 있음",
                        "opinion_source": "점주직접",
                        "is_request": False,
                        "severity": "보통",
                        "status": "안내완료",
                    }
                ],
            },
            {
                "project_id": "1",
                "store_name": "테스트점",
                "visit_date": "2026-06-20",
                "post_id": "p3",
                "issues": [
                    {
                        "issue_key": "메뉴_홀등록요청",
                        "issue_label": "홀 메뉴 등록",
                        "category": "가맹점의견",
                        "owner_voice": "홀 메뉴 등록을 요청",
                        "raw_text": "홀 메뉴 등록 요청 건을 확인",
                        "opinion_source": "점주직접",
                        "is_request": True,
                        "severity": "보통",
                        "status": "미해결",
                    }
                ],
            },
        ]
    }

    result = flow_visit.build_store_profile(payload)
    profile = result["profiles"][0]

    assert profile["key_concerns"] == ["홀 메뉴 등록"]
    # 과거 방문의 해결/안내완료 이슈는 최근 화두가 아니므로 문제·고민에도 들어오지 않는다.
    assert all("내장" not in row for row in profile["handling_points"])
    # 홀 메뉴 등록은 요청이므로 화두로만 남는다.
    assert profile["handling_points"] == []
    assert len(result["todos"]) == 1
    assert "홀 메뉴 등록" in result["todos"][0]["todo_list"]


def test_flow_visit_profile_analysis_evidence_contains_issue_detail_fields():
    payload = {
        "posts": [
            {
                "project_id": "1",
                "store_name": "테스트점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_key": "토더_설정공지",
                        "issue_label": "토더 설정/공지",
                        "category": "가맹점의견",
                        "owner_voice": "공지를 제대로 확인하지 않는 경우가 많다고 함",
                        "sv_action": "공지 확인 방법을 다시 안내",
                        "raw_text": "공지 확인을 잘 안 해서 토더 설정을 놓치는 경우가 있음",
                        "opinion_source": "점주직접",
                        "is_request": False,
                        "severity": "보통",
                        "status": "미해결",
                    }
                ],
            }
        ]
    }

    result = flow_visit.build_store_profile(payload)
    evidence = result["profiles"][0]["analysis_evidence"][0]

    assert evidence["concern"] == result["profiles"][0]["key_concerns"][0]
    assert evidence["issue_key"] == "토더_설정공지"
    assert evidence["issue_label"] == "토더 설정/공지"
    # problem_detail은 점주 요지 한 줄이다. "문제:/점주 의견:" 라벨을 다시 붙이지 않는다.
    assert evidence["problem_detail"] == "공지를 제대로 확인하지 않는 경우가 많다고 함"
    assert "문제:" not in evidence["problem_detail"]
    assert evidence["owner_voice"] == "공지를 제대로 확인하지 않는 경우가 많다고 함"
    assert evidence["sv_action"] == "공지 확인 방법을 다시 안내"


def test_flow_visit_history_profile_llm_removes_scores_and_reuses_cache(monkeypatch, tmp_path):
    cache_path = tmp_path / "flow_visit_profile_cache.json"
    calls = {"count": 0}

    def fake_query(history_digest):
        calls["count"] += 1
        return {
            "owner_status": "2/10 · 불만\n수익성과 품질에 대한 우려가 강한 상태",
            "store_status_summary": ["비용 대비 실제 수익성을 확인하는 편"],
            "key_concerns": ["묵은지 품질"],
            "handling_points": ["상품 품질 우려\n묵은지 상태 개선 가능성을 확인 중"],
            "next_visit_action": ["묵은지 품질 확인 결과 안내"],
            "analysis_evidence": [
                {
                    "concern": "묵은지_숙성경도",
                    "issue_key": "묵은지_숙성경도",
                    "visit_date": "2026-08-12",
                    "post_id": "p1",
                    "problem_detail": "짧음",
                    "evidence": "묵은지 품질 확인 요청",
                }
            ],
            "todos": [
                {
                    "post_id": "p1",
                    "issue_key": "묵은지_숙성경도",
                    "issue_seq": 1,
                    "todo_list": "묵은지 품질 이슈를 관련 부서에 전달하고 개선 가능 여부 확인",
                    "todo_due_date": None,
                    "todo_owner": "상품 담당",
                    "todo_status": "대기",
                    "analysis_evidence": [
                        {"visit_date": "2026-08-12", "post_id": "p1", "evidence": "묵은지 품질 확인 요청"}
                    ],
                }
            ],
        }, "unit_profile_llm"

    payload = {
        "posts": [
            {
                "project_id": "1",
                "store_name": "테스트점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_key": "묵은지_숙성경도",
                        "issue_label": "묵은지 숙성도",
                        "category": "사입",
                        "owner_voice": "묵은지 품질을 본사에서 확인해주세요",
                        "raw_text": "묵은지 품질을 본사에서 확인해주세요",
                        "opinion_source": "점주직접",
                        "is_request": True,
                        "severity": "보통",
                        "status": "미해결",
                    }
                ],
            }
        ]
    }

    monkeypatch.setattr(flow_visit, "PROFILE_LLM_PROVIDER", "local")
    monkeypatch.setattr(flow_visit, "FLOW_VISIT_PROFILE_CACHE", cache_path)
    monkeypatch.setattr(flow_visit, "_query_history_profile_json", fake_query)

    first = flow_visit.build_store_profile({"posts": [dict(payload["posts"][0], issues=[dict(payload["posts"][0]["issues"][0])])]})
    second = flow_visit.build_store_profile({"posts": [dict(payload["posts"][0], issues=[dict(payload["posts"][0]["issues"][0])])]})

    assert calls["count"] == 1
    assert "/10" not in first["profiles"][0]["owner_status"]
    assert "점" not in first["profiles"][0]["owner_status"]
    assert first["profiles"][0]["llm_model"] == "unit_profile_llm"
    evidence = first["profiles"][0]["analysis_evidence"][0]
    assert evidence["visit_date"] == "2026-08-12"
    assert evidence["concern"] == first["profiles"][0]["key_concerns"][0]
    assert evidence["issue_label"] == "묵은지 숙성도"
    assert "점주 의견:" not in evidence["problem_detail"]
    assert evidence["owner_voice"] == "묵은지 품질을 본사에서 확인해주세요"
    assert "evidence" in evidence
    assert first["todos"][0]["visit_rel_key"] == "1|p1"
    assert first["todos"][0]["issue_rel_key"] == "1|p1|1"
    assert first["todos"][0]["todo_owner"] == "상품 담당"
    assert second["todos"][0]["todo_id"] == first["todos"][0]["todo_id"]


def test_flow_visit_viz_followup_summary_keeps_existing_columns_with_action_context():
    payload = {
        "posts": [
            {
                "project_id": "2742104",
                "store_name": "용인동천점",
                "visit_date": "2026-06-01",
                "post_id": "78555117",
                "issues": [
                    {
                        "issue_key": "상권_악화",
                        "issue_label": "상권 악화",
                        "category": "매출",
                        "owner_voice": "재개발 공사 영향으로 매출 하락을 우려",
                        "sv_action": "",
                        "status": "미해결",
                    }
                ],
                "hq_followups": [
                    {
                        "responder": "유지성",
                        "reply_text": "1~4월까지는 배달플랫폼 전체 수요가 빠져 신규는 올리기가 쉽지 않습니다. 성수기 광고를 준비해야 합니다.",
                        "linked_issue_key": "상권_악화",
                        "written_at": "2026-06-02 08:47:56",
                    }
                ],
            }
        ],
        "profiles": [
            {
                "project_id": "2742104",
                "handling_points": ["상권 악화"],
                "analysis_evidence": [
                    {
                        "concern": "상권 악화",
                        "issue_key": "상권_악화",
                        "issue_rel_key": "2742104|78555117|1",
                        "action_hint": "이전 상담 기준 자료 준비",
                        "status": "미해결",
                        "is_problem": True,
                    }
                ],
            }
        ],
    }

    df = flow_visit_viz.build_visit_viz_table(payload, "2026-08-05T00:00:00", "test")

    assert df.loc[0, "followup_reply"].startswith("1~4월까지는 배달플랫폼")
    assert df.loc[0, "followup_state"] == "미해결"
    assert df.loc[0, "followup_next"] == "이전 상담 기준 자료 준비"
    # 문제 요약 칸에는 상태 문자열이 아니라 내용이 들어간다.
    assert df.loc[0, "issue_problem_detail"].startswith("재개발 공사 영향")
    assert df.loc[0, "followup_summary"] == df.loc[0, "issue_problem_detail"]
    # 따옴표·라벨·자동생성 문구가 남아 있으면 안 된다.
    for col in ["followup_summary", "followup_reply", "followup_next", "issue_problem_detail"]:
        assert '문제 "' not in df.loc[0, col]
        assert '상태 "' not in df.loc[0, col]
        assert "남은확인" not in df.loc[0, col]
        assert "처리 상태 확인" not in df.loc[0, col]
    assert "issue_followup_detail" not in df.columns
    assert "visit_checklist" not in df.columns
    assert "response_status_summary" not in df.columns


def test_flow_visit_viz_adds_issue_detail_columns_at_end_for_overlapped_raw_text():
    shared_raw = "공지 확인이 잘 안 되고 신메뉴 도입 일정도 같이 확인이 필요함"
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "83047164",
                "issues": [
                    {
                        "issue_seq": 1,
                        "seg_id": "83047164#1",
                        "issue_key": "토더_설정공지",
                        "issue_label": "토더 설정/공지",
                        "category": "가맹점의견",
                        "owner_voice": "공지 확인을 제대로 안 하는 경우가 많음",
                        "sv_action": "확인 방법을 다시 안내",
                        "raw_text": shared_raw,
                        "status": "미해결",
                    },
                    {
                        "issue_seq": 2,
                        "seg_id": "83047164#1",
                        "issue_key": "메뉴_신메뉴도입",
                        "issue_label": "신메뉴 도입",
                        "category": "가맹점의견",
                        "owner_voice": "신메뉴 접수 일정 확인 필요",
                        "sv_action": "",
                        "raw_text": shared_raw,
                        "status": "진행중",
                    },
                ],
            }
        ],
        "profiles": [
            {
                "project_id": "2548064",
                "handling_points": ["토더 공지 확인과 설정 교육 필요", "신메뉴 도입 관심"],
                "analysis_evidence": [
                    {
                        "concern": "토더 공지 확인과 설정 교육 필요",
                        "issue_rel_key": "2548064|83047164|1",
                        "action_hint": "토더 공지 확인 방법 현장 재시연",
                        "status": "미해결",
                        "is_problem": True,
                    },
                    {
                        "concern": "신메뉴 도입 관심",
                        "issue_rel_key": "2548064|83047164|2",
                        "action_hint": "신메뉴 도입 처리 상태 확인",
                        "status": "진행중",
                        "is_problem": True,
                    },
                ],
            }
        ],
    }

    df = flow_visit_viz.build_visit_viz_table(payload, "2026-08-26T00:00:00", "test")

    assert list(df.columns[-5:]) == ["visit_rel_key", "post_id", "issue_seq", "issue_key", "issue_rel_key"]
    assert "related_issue_count" not in df.columns
    assert "related_issue_labels" not in df.columns
    assert len(df) == 2
    assert "토더 공지 확인 방법 현장 재시연" in df.loc[0, "issue_action_detail"]
    assert "신메뉴 도입 처리 상태 확인" in df.loc[1, "issue_action_detail"]


def test_flow_visit_issue_cache_key_is_independent_of_profile_prompt_version():
    # 프로필 프롬프트 개정으로 PROMPT_VERSION이 올라가도 이슈 분류 캐시는 살아 있어야 한다.
    # 같이 무효화되면 전량 재분류 -> LLM_MAX_SEGMENTS 상한 -> fallback 가드 초과로 태스크가 죽는다.
    segment = {"seg_id": "p1#1", "raw_text": "본문", "sv_action_raw": "", "owner_voice_raw": ""}
    key = flow_visit._cache_key("p1", segment, [])

    assert flow_visit.ISSUE_PROMPT_VERSION in key
    assert flow_visit.PROMPT_VERSION not in key
    assert flow_visit.PROMPT_VERSION != flow_visit.ISSUE_PROMPT_VERSION


def test_flow_visit_concerns_map_one_to_one_with_analysis_evidence():
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_seq": 1,
                        "issue_key": "토더_설정공지",
                        "issue_label": "토더 설정/공지",
                        "category": "가맹점의견",
                        "owner_voice": "공지 확인을 제대로 안 하는 경우가 많음",
                        "raw_text": "공지 확인을 제대로 안 하는 경우가 많음",
                        "status": "미해결",
                    },
                    {
                        "issue_seq": 2,
                        "issue_key": "메뉴_신메뉴도입",
                        "issue_label": "신메뉴 도입",
                        "category": "가맹점의견",
                        "owner_voice": "신메뉴 도입은 숙지 완료했고 진행하신다고 함",
                        "raw_text": "신메뉴 도입은 숙지 완료했고 진행하신다고 함",
                        "status": "해결",
                    },
                ],
            }
        ]
    }

    profile = flow_visit.build_store_profile(payload)["profiles"][0]

    assert [row["concern"] for row in profile["analysis_evidence"]] == profile["key_concerns"]
    assert set(profile["handling_points"]) <= set(profile["key_concerns"])
    # 공지 확인 소홀은 본사 과제, 신메뉴 도입은 관심 -> 둘 다 고민은 아니다.
    assert profile["handling_points"] == []
    assert "신메뉴 도입 관심" in profile["key_concerns"]
    assert profile["analysis_evidence"][0]["issue_rel_key"] == "2548064|p1|1"


def test_flow_visit_no_open_issue_yields_empty_handling_points_and_blank_followup():
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_seq": 1,
                        "issue_key": "메뉴_신메뉴도입",
                        "issue_label": "신메뉴 도입",
                        "category": "가맹점의견",
                        "owner_voice": "신메뉴 도입은 숙지 완료했고 진행하신다고 함",
                        "raw_text": "신메뉴 도입은 숙지 완료했고 진행하신다고 함",
                        "status": "해결",
                    }
                ],
            }
        ]
    }

    result = flow_visit.build_store_profile(payload)
    profile = result["profiles"][0]

    assert profile["key_concerns"] == ["신메뉴 도입 관심"]
    assert profile["handling_points"] == []

    df = flow_visit_viz.build_visit_viz_table(result, "2026-08-26T00:00:00", "test")

    # 문제·고민이 아니어도 내용 요약은 보여야 한다.
    assert df.loc[0, "issue_problem_detail"] != ""
    # 규약: 후속 3컬럼이 공란이면 현재 문제·고민이 아니다.
    assert df.loc[0, "followup_state"] == ""
    assert df.loc[0, "followup_next"] == ""
    assert df.loc[0, "followup_reply"] == ""
    assert bool(df.loc[0, "concern_is_problem"]) is False


def test_flow_visit_duplicate_concern_text_merges_into_one_evidence_row():
    payload = {
        "posts": [
            {
                "project_id": "2742104",
                "store_name": "용인동천점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_seq": 1,
                        "issue_key": "묵은지_숙성경도",
                        "issue_label": "묵은지 숙성도",
                        "category": "사입",
                        "owner_voice": "묵은지가 너무 단단해 조리 시간이 길어짐",
                        "raw_text": "묵은지가 너무 단단해 조리 시간이 길어짐",
                        "status": "미해결",
                    },
                    {
                        "issue_seq": 2,
                        "issue_key": "우거지_질김",
                        "issue_label": "우거지 질김",
                        "category": "사입",
                        "owner_voice": "우거지가 질겨서 손질에 시간이 더 걸림",
                        "raw_text": "우거지가 질겨서 손질에 시간이 더 걸림",
                        "status": "미해결",
                    },
                    {
                        "issue_seq": 3,
                        "issue_key": "토더_설정공지",
                        "issue_label": "토더 설정/공지",
                        "category": "가맹점의견",
                        "owner_voice": "토더 공지를 제대로 확인하지 않는 경우가 많음",
                        "raw_text": "토더 공지를 제대로 확인하지 않는 경우가 많음",
                        "status": "미해결",
                    },
                ],
            }
        ]
    }

    profile = flow_visit.build_store_profile(payload)["profiles"][0]
    evidence = profile["analysis_evidence"]

    # 두 이슈가 같은 화두 문구로 접히지만 근거는 1행으로 병합되고 순서는 밀리지 않는다.
    assert profile["key_concerns"] == ["묵은지·우거지 품질", "토더 공지 확인과 설정 교육 필요"]
    assert [row["concern"] for row in evidence] == profile["key_concerns"]
    assert "우거지" in evidence[0]["problem_detail"]
    assert "묵은지" in evidence[0]["problem_detail"]
    assert evidence[1]["issue_key"] == "토더_설정공지"


def _summary_payload(owner="공지 확인이 잘 안 된다는 점주 의견", sv="확인 방법 재안내", label="토더 공지", concern=True):
    return {"owner_summary": owner, "sv_summary": sv, "issue_label": label, "is_concern": concern}


def _one_issue_payload(**issue_overrides):
    issue = {
        "issue_seq": 1,
        "issue_key": "토더_설정공지",
        "issue_label": "토더 설정/공지",
        "category": "가맹점의견",
        "owner_voice": "공지 확인을 제대로 안 하는 경우가 많음",
        "raw_text": "공지 확인을 제대로 안 하는 경우가 많음",
        "status": "미해결",
    }
    issue.update(issue_overrides)
    return {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "issues": [issue],
            }
        ]
    }


def test_flow_visit_summary_llm_writes_short_summary_and_caches(monkeypatch, tmp_path):
    calls = []

    def fake_query(prompt, system_prompt, client, model_candidates):
        calls.append(prompt)
        if "owner_summary" in prompt:
            return _summary_payload()
        return {"issue_key": "토더_설정공지", "severity": "보통", "status": "미해결", "is_request": False}

    monkeypatch.setattr(flow_visit, "FLOW_VISIT_LLM_CACHE", tmp_path / "llm_cache.json")
    monkeypatch.setattr(flow_visit, "_query_flow_json", fake_query)
    monkeypatch.setattr(
        flow_visit, "_rule_class_result",
        lambda segment, candidates, comments: {
            "issue_key": "토더_설정공지", "severity": "보통", "status": "미해결", "is_request": False,
        },
    )
    monkeypatch.setattr(
        "modules.transform.utility.qwen_client.get_ollama_client_with_candidates",
        lambda: (object(), ["gpt-oss:20b"]),
    )

    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "content_clean": "3. 토더 공지 - 공지 확인을 제대로 안 하는 경우가 많음",
                "content_text": "3. 토더 공지 - 공지 확인을 제대로 안 하는 경우가 많음",
            }
        ],
        "comments": [],
    }

    first = flow_visit.llm_extract_issues(dict(payload))
    issues = first["posts"][0]["issues"]

    assert issues, "세그먼트가 하나도 안 잡혔습니다"
    assert issues[0]["owner_voice"] == "공지 확인이 잘 안 된다는 점주 의견"
    assert issues[0]["sv_action"] == "확인 방법 재안내"
    assert issues[0]["is_concern"] is True
    assert "llm_summary" in issues[0]["llm_model"]
    summary_calls = len([p for p in calls if "owner_summary" in p])
    assert summary_calls >= 1

    # 두 번째 실행은 캐시를 타서 요약 LLM을 다시 부르지 않는다.
    flow_visit.llm_extract_issues(dict(payload))
    assert len([p for p in calls if "owner_summary" in p]) == summary_calls


def test_flow_visit_summary_falls_back_to_rule_without_counting_as_fallback(monkeypatch, tmp_path):
    monkeypatch.setattr(flow_visit, "FLOW_VISIT_LLM_CACHE", tmp_path / "llm_cache.json")
    monkeypatch.setattr(
        flow_visit, "_rule_class_result",
        lambda segment, candidates, comments: {
            "issue_key": "토더_설정공지", "severity": "보통", "status": "미해결", "is_request": False,
        },
    )
    monkeypatch.setattr(flow_visit, "_query_flow_json", lambda *a, **k: {"parse_error": "boom"})
    monkeypatch.setattr(
        "modules.transform.utility.qwen_client.get_ollama_client_with_candidates",
        lambda: (object(), ["gpt-oss:20b"]),
    )

    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "content_clean": "3. 토더 공지 - 공지 확인을 제대로 안 하는 경우가 많음",
                "content_text": "3. 토더 공지 - 공지 확인을 제대로 안 하는 경우가 많음",
            }
        ],
        "comments": [],
    }

    # 요약 실패는 MAX_FALLBACK_RATIO 가드를 건드리지 않는다 (예외 없이 통과해야 한다).
    result = flow_visit.llm_extract_issues(payload)
    issues = result["posts"][0]["issues"]
    assert issues
    assert issues[0]["owner_voice"]
    assert len(issues[0]["owner_voice"]) <= 80


def test_flow_visit_normalize_segment_issue_supports_legacy_pick_cache():
    segment = {
        "seg_id": "p1#1",
        "raw_text": "묵은지가 단단하다는 의견. 개선 확인 예정.",
        "owner_voice_raw": "묵은지가 단단하다는 의견. 개선 확인 예정.",
        "sv_action_raw": "",
        "topic": "묵은지",
    }
    issue = flow_visit._normalize_segment_issue(
        segment,
        {"issue_key": "묵은지_숙성경도", "severity": "보통", "status": "미해결", "is_request": False},
        {"owner_pick": [1], "sv_pick": [], "issue_label": "묵은지 숙성도"},
        "gpt-oss:20b",
    )

    assert issue["owner_voice"].startswith("묵은지가 단단하다는 의견")
    assert issue["is_concern"] is None


def test_flow_visit_empty_content_segment_is_not_a_concern():
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_seq": 1,
                        "issue_key": "기타",
                        "issue_label": "방문일지 주요 내용",
                        "category": "미분류",
                        "owner_voice": "5. 점주님 요청사항-없음",
                        "sv_action": "5. 점주님 요청사항-없음. ALL",
                        "raw_text": "5. 점주님 요청사항-없음",
                        "status": "미해결",
                    },
                    {
                        "issue_seq": 2,
                        "issue_key": "매장이전_양도양수",
                        "issue_label": "매장이전/양도양수",
                        "category": "매출",
                        "owner_voice": "매장 이전을 고민 중이라 부담이 크다고 하심",
                        "raw_text": "매장 이전을 고민 중이라 부담이 크다고 하심",
                        "status": "미해결",
                    },
                ],
            }
        ]
    }

    profile = flow_visit.build_store_profile(payload)["profiles"][0]

    joined = " ".join(profile["key_concerns"] + profile["handling_points"])
    assert "요청사항-없음" not in joined
    assert "5." not in joined
    assert profile["key_concerns"] == ["매장이전 검토"]


def test_flow_visit_no_issue_check_result_is_filtered_even_when_classified():
    issue = {
        "issue_key": "용기_불량",
        "issue_label": "용기 불량",
        "category": "사입",
        "owner_voice": "용기 크게 다른점 특이사항 없음",
        "sv_action": "소, 중, 대 용기 불량 현장 확인",
        "raw_text": "용기 불량 체크 결과 용기 크게 다른점 특이사항 없음.",
        "status": "미해결",
    }

    assert flow_visit._is_empty_content_issue(issue) is True


def test_flow_visit_profit_alias_maps_to_profit_issue():
    _, taxonomy_issues = flow_visit._issue_maps()
    segment = {
        "topic": "순이익",
        "owner_voice_raw": "도리당만 분리하면 순이익이 20% 미만일 것 같아 부담",
        "sv_action_raw": "광고세팅값 동일 조건에서 순이익 변동 체감 확인 요청",
        "raw_text": "도리당과 파스타 매출 9천만원, 도리당 순이익 20% 미만 우려",
    }

    result = flow_visit._rule_class_result(segment, taxonomy_issues, [])

    assert result is not None
    assert result["issue_key"] == "수익_감소체감"


def test_flow_visit_store_traits_drop_fragmented_llm_bullets():
    traits = flow_visit._profile_list_texts(
        ["신규 메뉴", "판매 채널 확대에는 비교적 적극적인 편", "광고는 실제 주문", "광고는 비용 대비 효과를 확인한 뒤 판단하는 편"],
        [],
        limit=5,
    )

    assert "신규 메뉴" not in traits
    assert "광고는 실제 주문" not in traits
    assert traits == ["광고는 비용 대비 효과를 확인한 뒤 판단하는 편"]


def test_flow_visit_menu_problem_concern_label_is_not_interest_only():
    issue = {
        "issue_key": "메뉴_신메뉴도입",
        "issue_label": "신메뉴 도입",
        "owner_voice": "신메뉴 조리시 불조절 필수와 물 부족 우려",
        "raw_text": "조리시 불조절해도 조금 짠 느낌이 있고 화구 화력이 강하다보니 물 1600ml 부족할 것 같다고 함",
        "status": "미해결",
    }

    assert flow_visit._concern_text(issue) == "신메뉴 조리·판매 우려"


def test_flow_visit_profit_next_action_is_specific():
    issue = {
        "issue_key": "수익_감소체감",
        "issue_label": "순이익",
        "owner_voice": "도리당만 분리하면 순이익이 20% 미만일 것 같아 부담",
        "sv_action": "광고세팅값 동일 조건에서 순이익 변동 체감 확인 요청",
    }

    action = flow_visit._handling_text(issue, allow_generic=False)

    assert "도리당 단독 순이익" in action
    assert "자료 준비" in action


def test_flow_visit_positive_status_report_is_concern_but_not_problem():
    payload = _one_issue_payload(
        issue_key="기타",
        issue_label="방문일지 주요 내용",
        owner_voice="매출이 오픈 이후 지속적으로 상승 중이며 성실영업도 잘지켜주심",
        raw_text="1. 매장현황 - 매출이 오픈 이후 지속적으로 상승 중이며 성실영업도 잘지켜주심",
        status="미해결",
    )

    profile = flow_visit.build_store_profile(payload)["profiles"][0]

    assert len(profile["key_concerns"]) == 1
    assert not profile["key_concerns"][0].startswith("1.")
    assert "매장현황" not in profile["key_concerns"][0]
    assert profile["handling_points"] == []


def test_flow_visit_llm_concern_flag_overrides_signal_rule():
    problem = flow_visit._is_problem_issue(
        {"owner_voice": "중립적인 문장", "raw_text": "중립적인 문장", "status": "미해결", "is_concern": True}
    )
    not_problem = flow_visit._is_problem_issue(
        {"owner_voice": "부담이 크다고 하심", "raw_text": "부담이 크다고 하심", "status": "미해결", "is_concern": False}
    )
    resolved = flow_visit._is_problem_issue(
        {"owner_voice": "부담이 크다고 하심", "raw_text": "부담이 크다고 하심", "status": "해결", "is_concern": True}
    )

    assert problem is True
    assert not_problem is False
    # 상태가 먼저다. 해결된 건은 LLM이 고민이라 해도 문제·고민이 아니다.
    assert resolved is False


def test_flow_visit_viz_does_not_fall_back_to_status_for_concern_flag():
    """화두로 안 잡힌 이슈는 status가 미해결이어도 문제·고민이 아니다.

    status 기본값이 "미해결"이라 폴백을 두면 전 행이 문제·고민이 되어
    부분집합 관계가 다시 무의미해진다.
    """
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-06-16",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_seq": 1,
                        "issue_key": "매장이전_양도양수",
                        "issue_label": "매장이전/양도양수",
                        "category": "매출",
                        "owner_voice": "이전을 검토 중",
                        "status": "미해결",
                    },
                    {
                        "issue_seq": 2,
                        "issue_key": "기타",
                        "issue_label": "점주님 요청사항-없음",
                        "category": "미분류",
                        "owner_voice": "없음",
                        "status": "미해결",
                    },
                ],
            }
        ],
        "profiles": [
            {
                "project_id": "2548064",
                "analysis_evidence": [
                    {
                        "concern": "매장이전 검토",
                        "issue_key": "매장이전_양도양수",
                        "issue_rel_key": "2548064|p1|1",
                        "status": "미해결",
                        "is_problem": False,
                    }
                ],
            }
        ],
    }

    df = flow_visit_viz.build_visit_viz_table(payload, "2026-08-27T00:00:00", "test")

    # 화두이긴 하나 문제·고민은 아님 -> concern_text는 남고 followup_summary만 NULL이다.
    assert df.loc[0, "concern_text"] == "매장이전 검토"
    assert bool(df.loc[0, "concern_is_problem"]) is False
    assert df.loc[0, "issue_problem_detail"] == "이전을 검토 중"
    assert df.loc[0, "followup_state"] == ""
    assert df.loc[0, "followup_next"] == ""

    # 화두로 잡히지도 않는 "요청사항-없음" 이슈는 viz 행 자체에서 제외한다.
    assert len(df) == 1


def test_flow_visit_llm_next_action_replaces_generated_filler(monkeypatch, tmp_path):
    """LLM이 쓴 next_action이 "OO 처리 상태 확인" 자동생성을 대체한다."""
    monkeypatch.setattr(flow_visit, "FLOW_VISIT_LLM_CACHE", tmp_path / "llm_cache.json")
    monkeypatch.setattr(
        flow_visit, "_rule_class_result",
        lambda segment, candidates, comments: {
            "issue_key": "토더_설정공지", "severity": "보통", "status": "미해결", "is_request": True,
        },
    )
    monkeypatch.setattr(
        flow_visit, "_query_flow_json",
        lambda *a, **k: {
            "owner_summary": "공지를 토더로 받다보니 중요성을 인식하지 못해 확인을 소홀히 함",
            "sv_summary": "",
            "issue_label": "토더 공지",
            "is_concern": True,
            "next_action": "공지 확인 방법 현장 재시연 일정 회신",
        },
    )
    monkeypatch.setattr(
        "modules.transform.utility.qwen_client.get_ollama_client_with_candidates",
        lambda: (object(), ["gpt-oss:20b"]),
    )

    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "content_clean": "3. 토더 공지 - 공지 확인을 제대로 안 하는 경우가 많음",
                "content_text": "3. 토더 공지 - 공지 확인을 제대로 안 하는 경우가 많음",
            }
        ],
        "comments": [],
    }

    result = flow_visit.build_store_profile(flow_visit.llm_extract_issues(payload))
    df = flow_visit_viz.build_visit_viz_table(result, "2026-08-27T00:00:00", "test")

    issues = result["posts"][0]["issues"]
    assert issues, "세그먼트가 하나도 안 잡혔습니다"
    assert issues[0]["next_action"] == "공지 확인 방법 현장 재시연 일정 회신"
    # LLM이 쓴 next_action이 매핑 액션문("토더 공지 확인 방법 현장 재시연")보다 우선한다.
    assert df.loc[0, "followup_next"] == "공지 확인 방법 현장 재시연 일정 회신"
    assert df.loc[0, "issue_problem_detail"].startswith("공지를 토더로")
    for col in df.columns:
        if df[col].dtype == object:
            assert not df[col].fillna("").astype(str).str.contains("처리 상태 확인", regex=False).any(), col


def test_flow_visit_unmapped_issue_without_llm_action_leaves_next_blank():
    """근거가 없으면 남은 확인은 공란이다. 라벨 반복 문구를 만들지 않는다."""
    issue = {"issue_key": "메뉴_신메뉴도입", "issue_label": "신메뉴 도입", "status": "미해결"}

    assert flow_visit._handling_text(issue, allow_generic=False) == ""
    assert flow_visit._handling_text(issue) == "신메뉴 도입 처리 상태 확인"
    # 매핑된 이슈는 allow_generic과 무관하게 실제 액션문을 유지한다.
    mapped = {"issue_key": "토더_설정공지", "issue_label": "토더 설정/공지", "status": "미해결"}
    assert flow_visit._handling_text(mapped, allow_generic=False) == "토더 공지 확인 방법 현장 재시연"


def test_flow_visit_viz_omits_no_data_filler_text():
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_seq": 1,
                        "issue_key": "토더_설정공지",
                        "issue_label": "토더 설정/공지",
                        "category": "가맹점의견",
                        "owner_voice": "공지 확인이 잘 안 된다는 의견",
                        "sv_action": "",
                        "status": "미해결",
                    }
                ],
            }
        ],
        "profiles": [
            {
                "project_id": "2548064",
                "analysis_evidence": [
                    {
                        "concern": "토더 공지 확인과 설정 교육 필요",
                        "issue_key": "토더_설정공지",
                        "issue_rel_key": "2548064|p1|1",
                        "action_hint": "토더 공지 확인 방법 현장 재시연",
                        "status": "미해결",
                        "is_problem": True,
                    }
                ],
            }
        ],
    }

    df = flow_visit_viz.build_visit_viz_table(payload, "2026-08-27T00:00:00", "test")

    assert df.loc[0, "sv_summary"] == ""
    assert df.loc[0, "followup_reply"] == ""
    assert df.loc[0, "followup_responder_list"] == ""
    for filler in ["기록 없음", "본사 답변 없음", "후속 댓글 없음", "후속 확인사항 없음"]:
        for col in df.columns:
            if df[col].dtype == object:
                assert not df[col].fillna("").astype(str).str.contains(filler, regex=False).any(), (col, filler)


def test_flow_visit_next_action_must_belong_to_its_own_topic():
    """한 문단이 여러 주제로 쪼개질 때 옆 주제의 할 일이 붙는 것을 막는다.

    실제로 토더 공지 행에 순이익 회신 문구가 붙은 적이 있다.
    """
    toder = {
        "issue_label": "토더 설정/공지",
        "issue_key": "토더_설정공지",
        "owner_voice": "공지사항을 토더로 받다보니 중요성을 인식하지 못해 확인 안함",
        "sv_action": "미확인 가맹점 재안내",
    }
    profit = {
        "issue_label": "순이익확인",
        "issue_key": "기타",
        "owner_voice": "도리당만 분리하면 순이익률이 20% 미만일 것으로",
        "sv_action": "",
    }

    # 주제와 어휘가 하나도 안 겹치면 버린다
    assert flow_visit._anchored_next_action("광고세팅 동일 조건 순이익 데이터 회신", toder) == ""
    # 제 주제의 할 일은 그대로 남는다
    assert flow_visit._anchored_next_action("공지 확인 방법 현장 재시연", toder) == "공지 확인 방법 현장 재시연"
    # 조사·접미가 붙어도 부분 문자열로 인정한다 ("순이익" vs "순이익확인")
    assert (
        flow_visit._anchored_next_action("광고세팅값 동일 조건 순이익 데이터 회신", profit)
        == "광고세팅값 동일 조건 순이익 데이터 회신"
    )
    assert flow_visit._anchored_next_action("", toder) == ""


def test_flow_visit_request_is_concern_but_not_a_problem():
    """요청·희망은 화두이되 문제·고민이 아니다. 점주가 곤란해하는 것만 고민이다."""
    worry = {"owner_voice": "순살 품질 편차가 커서 부담된다고 하심", "raw_text": "순살 품질 편차가 커서 부담",
             "status": "미해결", "is_request": True}
    request = {"owner_voice": "닭발과 찜닭 메뉴 추가를 희망하심", "raw_text": "닭발과 찜닭 추가 희망",
               "status": "미해결", "is_request": True}
    hq_task = {"owner_voice": "공지를 토더로 받다보니 확인을 소홀히 한다고 함",
               "raw_text": "미확인 가맹점 재안내 필요", "status": "미해결", "is_request": False}

    assert flow_visit._is_problem_issue(worry) is True
    # is_request가 True여도 그것만으로는 고민이 아니다.
    assert flow_visit._is_problem_issue(request) is False
    assert flow_visit._is_problem_issue(hq_task) is False


def test_flow_visit_profile_invariants_do_not_promote_status_only_concern_to_problem():
    profile = {
        "analysis_evidence": [
            {
                "concern": "신메뉴 도입 관심",
                "issue_key": "메뉴_신메뉴도입",
                "status": "진행중",
                "is_problem": False,
                "evidence": "신메뉴 추가를 희망",
            }
        ],
        "handling_points": [],
    }

    result = flow_visit._enforce_concern_invariants(profile)

    assert result["key_concerns"] == ["신메뉴 도입 관심"]
    assert result["handling_points"] == []
    assert result["analysis_evidence"][0]["is_problem"] is False


def test_flow_visit_viz_followup_summary_is_blank_when_not_a_problem():
    """대시보드가 followup_summary IS NOT NULL로 문제·고민을 거른다.

    그 규약이 성립하려면 문제·고민 행에만 값이 있어야 한다.
    issue_problem_detail은 화두 행에도 채워지므로 판정에 쓰면 안 된다.
    """
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_seq": 1,
                        "issue_key": "수익_감소체감",
                        "issue_label": "순이익",
                        "category": "매출",
                        "owner_voice": "도리당만 분리하면 순이익이 20% 미만일 것 같아 부담",
                        "status": "미해결",
                    },
                    {
                        "issue_seq": 2,
                        "issue_key": "메뉴_신메뉴도입",
                        "issue_label": "신메뉴 도입",
                        "category": "가맹점의견",
                        "owner_voice": "닭발과 찜닭 추가를 희망하심",
                        "status": "미해결",
                    },
                ],
            }
        ],
        "profiles": [
            {
                "project_id": "2548064",
                "analysis_evidence": [
                    {
                        "concern": "순수익 감소 체감",
                        "issue_key": "수익_감소체감",
                        "issue_rel_key": "2548064|p1|1",
                        "action_hint": "수수료·광고비 제외 순수익 기준 설명",
                        "status": "미해결",
                        "is_problem": True,
                    },
                    {
                        "concern": "신메뉴 도입 관심",
                        "issue_key": "메뉴_신메뉴도입",
                        "issue_rel_key": "2548064|p1|2",
                        "status": "미해결",
                        "is_problem": False,
                    },
                ],
            }
        ],
    }

    df = flow_visit_viz.build_visit_viz_table(payload, "2026-08-27T00:00:00", "test")

    # 문제·고민 행: 내용이 있고 필터에 걸린다
    assert bool(df.loc[0, "concern_is_problem"]) is True
    assert pd.notna(df.loc[0, "followup_summary"])
    assert df.loc[0, "followup_summary"] == df.loc[0, "issue_problem_detail"]

    # 화두이지만 고민은 아닌 행: followup_summary는 NULL, 내용 요약은 남는다
    assert bool(df.loc[1, "concern_is_problem"]) is False
    assert pd.isna(df.loc[1, "followup_summary"])
    assert df.loc[1, "issue_problem_detail"] != ""

    # 두 기준이 항상 일치해야 한다
    assert (df.followup_summary.notna() == df.concern_is_problem).all()


def test_flow_visit_viz_formats_profile_lists_as_bullets():
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "83047164",
                "store_status_summary": "부정적이다\n광고에 부정적이다",
                "issues": [],
            }
        ],
        "profiles": [
            {
                "project_id": "2548064",
                "owner_status": "후속 확인을 기다리는 상태",
                "key_concerns": ["방문일지 주요내용 : 신규 메뉴 희망", "키워드 공략 방향성"],
                "handling_points": ["상담 내용: 신규 메뉴 출시 가능 여부 확인", "키워드 효과 자료 준비"],
            }
        ],
    }

    df = flow_visit_viz.build_visit_viz_table(payload, "2026-08-26T00:00:00", "test")

    assert df.loc[0, "store_status_summary"] == "- 부정적이다\n- 광고에 부정적이다"
    assert df.loc[0, "key_concerns"] == "- 신규 메뉴 희망\n- 키워드 공략 방향성"
    assert df.loc[0, "handling_points"] == "- 신규 메뉴 출시 가능 여부 확인\n- 키워드 효과 자료 준비"
    assert "내용" not in df.loc[0, "key_concerns"]
    assert "내용" not in df.loc[0, "handling_points"]


def test_flow_visit_viz_hides_generic_issue_label():
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "83047164",
                "issues": [
                    {
                        "issue_seq": 1,
                        "issue_key": "기타",
                        "issue_label": "방문일지 주요 내용",
                        "category": "미분류",
                        "owner_voice": "점주 의견",
                    }
                ],
            }
        ],
        "profiles": [{"project_id": "2548064"}],
    }

    df = flow_visit_viz.build_visit_viz_table(payload, "2026-08-26T00:00:00", "test")

    assert pd.isna(df.loc[0, "issue_label"])


def test_flow_visit_viz_uses_final_display_schema_and_linked_followup_count():
    payload = {
        "posts": [
            {
                "project_id": "2548064",
                "store_name": "대화점",
                "visit_date": "2026-08-12",
                "post_id": "p1",
                "issues": [
                    {
                        "issue_seq": 1,
                        "issue_key": "수익_감소체감",
                        "issue_label": "순이익",
                        "category": "매출",
                        "owner_voice": "순이익이 낮아 부담",
                        "status": "미해결",
                    }
                ],
                "hq_followups": [
                    {"linked_issue_key": "수익_감소체감", "reply_text": "순이익 기준 자료 준비", "responder": "유지성"},
                    {"linked_issue_key": "다른_이슈", "reply_text": "다른 이슈 답변", "responder": "조민준"},
                    {"linked_issue_key": "수익_감소체감", "reply_text": "노이즈", "is_noise": True},
                ],
            }
        ],
        "profiles": [
            {
                "project_id": "2548064",
                "key_concerns": ["순수익 감소 체감", "신메뉴 도입 관심"],
                "analysis_evidence": [
                    {
                        "concern": "순수익 감소 체감",
                        "issue_key": "수익_감소체감",
                        "issue_rel_key": "2548064|p1|1",
                        "is_problem": True,
                    }
                ],
            }
        ],
    }

    df = flow_visit_viz.build_visit_viz_table(payload, "2026-08-27T00:00:00", "test")

    assert list(df.columns) == flow_visit_viz.VIZ_OUTPUT_COLUMNS
    for col in [
        "problem_summary",
        "linked_followup_cnt",
        "profile_period_start",
        "profile_period_end",
        "profile_as_of_date",
        "llm_model",
        "visit_date_source",
        "prompt_version",
        "generated_at",
        "seg_id",
        "opinion_source",
        "is_request",
        "is_fallback",
        "grounding_flag",
        "source_kind",
        "followup_json",
        "related_issue_count",
        "related_issue_labels",
    ]:
        assert col not in df.columns
    assert df.loc[0, "concern_text"] == "순수익 감소 체감"
    assert df.loc[0, "followup_cnt"] == 1
