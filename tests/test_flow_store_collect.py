import json
from pathlib import Path

import pandas as pd
import pytest

from modules.transform.pipelines.strategy import SMP_flow_store_collect as flow


DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def _load_detail(post_id: str) -> dict:
    payload = json.loads((DATA_DIR / "posts" / "2742104" / f"{post_id}.json").read_text(encoding="utf-8"))
    return flow._unwrap_response_data(payload)


def _fake_detail(post_id: str, title: str, child_tasks=None) -> dict:
    return {
        "projectId": "2742104",
        "postId": post_id,
        "title": title,
        "outContent": f"{title} body",
        "registerName": "tester",
        "registerId": "tester@example.com",
        "registeredDateTime": "20260101010101",
        "editedDateTime": "20260101010101",
        "remarkCount": "0",
        "subTaskCount": str(len(child_tasks or [])),
        "remarks": [],
        "tasks": [],
        "subTasks": child_tasks or [],
        "attachments": [],
        "imageAttachments": [],
        "connectUrl": "",
    }


def _row(columns, **values):
    return {column: values.get(column, "") for column in columns}


def test_unwrap_projects_dump():
    payload = json.loads((DATA_DIR / "projects.json").read_text(encoding="utf-8"))
    data = flow._unwrap_response_data(payload)

    assert len(data["projects"]) == 101
    first = flow._normalize_project(data["projects"][0], "2026-08-03T00:00:00+09:00")
    assert set(first) == set(flow._PROJECT_COLS)


def test_project_store_inference_keeps_parenthesized_store_names():
    assert flow._infer_project_parts("경기_시흥장현점(양수)(68호점)")[:3] == (
        True,
        "경기",
        "시흥장현점(양수)(68호점)",
    )
    assert flow._infer_project_parts("TF_송파삼전점 가맹 사업 모델 고도화")[0] is False


def test_unwrap_post_list_dump_handles_outer_list():
    payload = json.loads((DATA_DIR / "posts" / "2742104" / "_list.json").read_text(encoding="utf-8"))
    data = flow._unwrap_response_data(payload)

    assert data["projectId"] == "2742104"
    assert len(data["posts"]) == 10


def test_normalize_post_extracts_task_status_from_column_rec():
    detail = _fake_detail(
        "post-1",
        "상태 컬럼 테스트",
        child_tasks=[
            {
                "COLABO_COMMT_SRNO": "child-1",
                "TASK_NM": "하위업무",
                "TASK_COLUMN_REC": [
                    {
                        "COLUMN_TYPE": "STATUS",
                        "COLUMN_DATA_REC": [{"OPTION_NAME": "완료"}],
                    }
                ],
                "PROGRESS": "100",
                "START_DT": "2026-08-13",
                "END_DT": "2026-08-18",
                "WORKER_REC": [{"USER_NM": "오나영"}],
            }
        ],
    )
    task = detail["subTasks"][0]

    row = flow._normalize_post(
        detail,
        {
            "project_id": "2742104",
            "project_name": "경기_용인동천점",
            "store_name": "용인동천점",
        },
        parent_post_id="post-1",
        depth=1,
        task_meta=task,
        collected_at="2026-08-21T00:00:00+09:00",
    )

    assert row["task_status"] == "완료"
    assert row["progress"] == "100"
    assert row["start_dt"] == "2026-08-13"
    assert row["end_dt"] == "2026-08-18"
    assert row["worker"] == "오나영"


def test_normalize_post_extracts_task_status_from_stts_code():
    detail = _fake_detail("post-1", "STTS 코드 테스트")
    detail["tasks"] = [
        {
            "COLABO_COMMT_SRNO": "post-1",
            "TASK_NM": "STTS 코드 업무",
            "STTS": "1",
            "TASK_COLUMN_REC": [
                {
                    "COLUMN_TYPE": "STTS",
                    "COLUMN_DATA_REC": [
                        {
                            "OPTION_NAME": "",
                            "OPTION_CATEGORY": "1",
                            "CUSTOM_COLUMN_DATA": "1",
                        }
                    ],
                }
            ],
            "PROGRESS": "0",
            "WORKER_REC": [],
        }
    ]

    row = flow._normalize_post(
        detail,
        {
            "project_id": "2742104",
            "project_name": "경기_용인동천점",
            "store_name": "용인동천점",
        },
        parent_post_id="",
        depth=0,
        task_meta=None,
        collected_at="2026-08-21T00:00:00+09:00",
    )

    assert row["task_status"] == "진행"
    assert row["progress"] == "0"


def test_normalize_post_extracts_task_status_from_fallback_fields():
    detail = _fake_detail("post-1", "상태 fallback 테스트")
    detail.update(
        {
            "status": {"optionName": "진행"},
            "progressRate": 50,
            "startDate": "2026-08-13",
            "dueDate": "2026-08-18",
        }
    )

    row = flow._normalize_post(
        detail,
        {
            "project_id": "2742104",
            "project_name": "경기_용인동천점",
            "store_name": "용인동천점",
        },
        parent_post_id="",
        depth=0,
        task_meta=None,
        collected_at="2026-08-21T00:00:00+09:00",
    )

    assert row["task_status"] == "진행"
    assert row["progress"] == "50"
    assert row["start_dt"] == "2026-08-13"
    assert row["end_dt"] == "2026-08-18"


def test_collect_post_details_recurses_subtasks(monkeypatch):
    root = _load_detail("69590268")
    child_tasks = root["subTasks"]
    grandchild_tasks = [
        {
            "COLABO_COMMT_SRNO": "90000001",
            "TASK_NM": "손자 업무 1",
            "SUB_TASK_CNT": "0",
            "PROGRESS": "0",
            "TASK_COLUMN_REC": [],
            "WORKER_REC": [],
        },
        {
            "COLABO_COMMT_SRNO": "90000002",
            "TASK_NM": "손자 업무 2",
            "SUB_TASK_CNT": "0",
            "PROGRESS": "0",
            "TASK_COLUMN_REC": [],
            "WORKER_REC": [],
        },
    ]

    def fake_fetch(project_id, post_id):
        if post_id == "69590268":
            return root
        if post_id == "69590269":
            return _fake_detail(post_id, "매장 정보 플로우 생성", grandchild_tasks)
        if post_id in {task["COLABO_COMMT_SRNO"] for task in child_tasks}:
            return _fake_detail(post_id, f"하위업무 {post_id}")
        if post_id in {"90000001", "90000002"}:
            return _fake_detail(post_id, f"손자업무 {post_id}")
        raise AssertionError(post_id)

    monkeypatch.setattr(flow, "_fetch_post_detail", fake_fetch)
    details = flow.collect_post_details(
        [
            {
                "project_id": "2742104",
                "project_name": "경기_용인동천점",
                "project_url": "https://flow.team/main.act?projectId=2742104",
                "is_store": True,
                "region": "경기",
                "store_name": "용인동천점",
                "status_tag": "",
                "post_id": "69590268",
                "updated_at": "LIST_UPDATED_AT",
            }
        ]
    )

    posts = details["posts"]
    assert sum(1 for post in posts if post["depth"] == 0) == 1
    assert sum(1 for post in posts if post["depth"] == 1) == 9
    assert sum(1 for post in posts if post["depth"] == 2) == 2

    child = next(post for post in posts if post["post_id"] == "69590269")
    assert child["parent_post_id"] == "69590268"
    assert child["task_nm"] == "매장 정보 플로우 생성"
    assert child["task_status"] == "완료"
    assert child["progress"] == "100"
    assert details["state_updates"]["69590268"] == "LIST_UPDATED_AT"


def test_collect_post_details_full_backfill_conf_disables_post_limit(monkeypatch):
    monkeypatch.setattr(flow, "DETAIL_MAX_POSTS", 1)
    fetched = []

    def fake_fetch(project_id, post_id):
        fetched.append(post_id)
        return _fake_detail(post_id, f"title {post_id}")

    class DagRun:
        conf = {"full_backfill": True}

    monkeypatch.setattr(flow, "_fetch_post_detail", fake_fetch)
    details = flow.collect_post_details(
        [
            {
                "project_id": "2742104",
                "project_name": "경기_용인동천점",
                "project_url": "",
                "is_store": True,
                "region": "경기",
                "store_name": "용인동천점",
                "status_tag": "",
                "post_id": str(post_id),
                "updated_at": "LIST_UPDATED_AT",
            }
            for post_id in range(3)
        ],
        dag_run=DagRun(),
    )

    assert fetched == ["0", "1", "2"]
    assert [post["post_id"] for post in details["posts"]] == ["0", "1", "2"]
    assert details["complete"] is True


def test_collect_post_details_downloads_attachments_for_configured_project(monkeypatch, tmp_path):
    detail = _fake_detail("post-1", "첨부 테스트")
    detail["attachments"] = [
        {
            "ATCH_SRNO": "file-1",
            "FILE_NAME": "테스트 문서.pdf",
            "FILE_SIZE": "123",
            "ATCH_URL": "https://flow.team/download/file-1",
        }
    ]
    detail["imageAttachments"] = [
        {
            "ATCH_SRNO": "image-1",
            "ORCP_FILE_NM": "image.png",
            "FILE_SIZE": "456",
            "ATCH_URL": "https://flow.team/flowImg/image.png",
            "THUM_IMG_PATH": "https://flow.team/flowImg/thumb.png",
            "WIDTH": "10",
            "HEIGHT": "20",
        }
    ]

    class DagRun:
        conf = {"flow_attachment_project_ids": "2742104"}

    downloaded = []

    def fake_download(url, path):
        downloaded.append((url, path))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"attachment")
        return "hash"

    monkeypatch.setattr(flow, "_fetch_post_detail", lambda project_id, post_id: detail)
    monkeypatch.setattr(flow, "_download_attachment_file", fake_download)
    monkeypatch.setattr(flow, "FLOW_ATTACHMENT_FILES_DIR", tmp_path / "files")

    details = flow.collect_post_details(
        [
            {
                "project_id": "2742104",
                "project_name": "경기_용인동천점",
                "project_url": "",
                "is_store": True,
                "region": "경기",
                "store_name": "용인동천점",
                "status_tag": "",
                "post_id": "post-1",
                "updated_at": "LIST_UPDATED_AT",
            }
        ],
        dag_run=DagRun(),
    )

    assert len(details["attachments"]) == 2
    assert len(downloaded) == 2
    assert details["attachment_failures"] == []
    assert details["attachment_checked_post_ids"] == ["post-1"]
    assert {row["attachment_type"] for row in details["attachments"]} == {"file", "image"}
    assert all(row["downloaded"] is True for row in details["attachments"])


def test_detect_changed_posts_includes_attachment_unchecked_project(monkeypatch, tmp_path):
    state_path = tmp_path / "flow_posts_index.json"
    state_path.write_text(
        json.dumps(
            {
                "2742104:post-1": {
                    "project_id": "2742104",
                    "post_id": "post-1",
                    "updated_at": "SAME",
                }
            }
        ),
        encoding="utf-8",
    )

    class DagRun:
        conf = {"flow_attachment_project_ids": "2742104"}

    post_dir = tmp_path / "flow_post"
    (post_dir / "project_id=2742104").mkdir(parents=True)
    pd.DataFrame([{"post_id": "post-1"}]).to_parquet(
        post_dir / "project_id=2742104" / "part.parquet",
        index=False,
    )

    monkeypatch.setattr(flow, "FLOW_STATE_JSON", state_path)
    monkeypatch.setattr(flow, "FLOW_POST_PARQUET", post_dir)
    changed = flow.detect_changed_posts(
        [
            {
                "project_id": "2742104",
                "post_id": "post-1",
                "registeredDateTime": "SAME",
                "project_name": "경기_용인동천점",
            }
        ],
        dag_run=DagRun(),
    )

    assert len(changed) == 1
    assert changed[0]["attachment_check_only"] is True


def test_detect_changed_posts_repairs_missing_post_project_partition(monkeypatch, tmp_path):
    state_path = tmp_path / "flow_posts_index.json"
    state_path.write_text(
        json.dumps(
            {
                "2466857:post-1": {
                    "project_id": "2466857",
                    "post_id": "post-1",
                    "updated_at": "SAME",
                }
            }
        ),
        encoding="utf-8",
    )
    post_dir = tmp_path / "flow_post"
    (post_dir / "project_id=2742104").mkdir(parents=True)
    pd.DataFrame([{"post_id": "other"}]).to_parquet(
        post_dir / "project_id=2742104" / "part.parquet",
        index=False,
    )

    monkeypatch.setattr(flow, "FLOW_STATE_JSON", state_path)
    monkeypatch.setattr(flow, "FLOW_POST_PARQUET", post_dir)

    class DagRun:
        conf = {"flow_repair_project_ids": "2466857"}

    changed = flow.detect_changed_posts(
        [
            {
                "project_id": "2466857",
                "post_id": "post-1",
                "registeredDateTime": "SAME",
                "project_name": "경기_동탄영천점",
            }
        ],
        dag_run=DagRun(),
    )

    assert len(changed) == 1
    assert changed[0]["project_id"] == "2466857"
    assert changed[0]["attachment_check_only"] is False


def test_detect_changed_posts_repairs_project_even_when_partition_exists(monkeypatch, tmp_path):
    state_path = tmp_path / "flow_posts_index.json"
    state_path.write_text(
        json.dumps(
            {
                "2548064:post-1": {
                    "project_id": "2548064",
                    "post_id": "post-1",
                    "updated_at": "SAME",
                }
            }
        ),
        encoding="utf-8",
    )
    post_dir = tmp_path / "flow_post"
    (post_dir / "project_id=2548064").mkdir(parents=True)
    pd.DataFrame([{"project_id": "2548064", "post_id": "post-1"}]).to_parquet(
        post_dir / "project_id=2548064" / "part.parquet",
        index=False,
    )

    class DagRun:
        conf = {"flow_repair_project_ids": "2548064"}

    monkeypatch.setattr(flow, "FLOW_STATE_JSON", state_path)
    monkeypatch.setattr(flow, "FLOW_POST_PARQUET", post_dir)

    changed = flow.detect_changed_posts(
        [
            {
                "project_id": "2548064",
                "post_id": "post-1",
                "registeredDateTime": "SAME",
                "project_name": "경기_대화점",
            }
        ],
        dag_run=DagRun(),
    )

    assert len(changed) == 1
    assert changed[0]["attachment_check_only"] is False


def test_detect_changed_posts_limits_missing_parquet_repair_to_configured_project(monkeypatch, tmp_path):
    state_path = tmp_path / "flow_posts_index.json"
    state_path.write_text(
        json.dumps(
            {
                "2548064:target-post": {
                    "project_id": "2548064",
                    "post_id": "target-post",
                    "updated_at": "SAME",
                },
                "9999999:other-missing": {
                    "project_id": "9999999",
                    "post_id": "other-missing",
                    "updated_at": "SAME",
                },
            }
        ),
        encoding="utf-8",
    )
    post_dir = tmp_path / "flow_post"
    (post_dir / "project_id=2548064").mkdir(parents=True)
    pd.DataFrame([{"project_id": "2548064", "post_id": "target-kept"}]).to_parquet(
        post_dir / "project_id=2548064" / "part.parquet",
        index=False,
    )

    class DagRun:
        conf = {"flow_repair_project_ids": "2548064"}

    monkeypatch.setattr(flow, "FLOW_STATE_JSON", state_path)
    monkeypatch.setattr(flow, "FLOW_POST_PARQUET", post_dir)

    changed = flow.detect_changed_posts(
        [
            {
                "project_id": "2548064",
                "post_id": "target-post",
                "registeredDateTime": "SAME",
                "project_name": "경기_대화점",
            },
            {
                "project_id": "9999999",
                "post_id": "other-missing",
                "registeredDateTime": "SAME",
                "project_name": "경기_다른점",
            },
        ],
        dag_run=DagRun(),
    )

    assert [row["project_id"] for row in changed] == ["2548064"]
    assert changed[0]["attachment_check_only"] is False


def test_detect_changed_posts_repairs_post_missing_from_existing_parquet(monkeypatch, tmp_path):
    state_path = tmp_path / "flow_posts_index.json"
    state_path.write_text(
        json.dumps(
            {
                "2548064:missing-post": {
                    "project_id": "2548064",
                    "post_id": "missing-post",
                    "updated_at": "SAME",
                }
            }
        ),
        encoding="utf-8",
    )
    post_dir = tmp_path / "flow_post"
    (post_dir / "project_id=2548064").mkdir(parents=True)
    pd.DataFrame([{"project_id": "2548064", "post_id": "kept-post"}]).to_parquet(
        post_dir / "project_id=2548064" / "part.parquet",
        index=False,
    )

    monkeypatch.setattr(flow, "FLOW_STATE_JSON", state_path)
    monkeypatch.setattr(flow, "FLOW_POST_PARQUET", post_dir)

    changed = flow.detect_changed_posts(
        [
            {
                "project_id": "2548064",
                "post_id": "missing-post",
                "registeredDateTime": "SAME",
                "project_name": "경기_대화점",
            }
        ]
    )

    assert len(changed) == 1
    assert changed[0]["attachment_check_only"] is False


def test_detect_changed_posts_repairs_missing_partition_when_state_says_collected(monkeypatch, tmp_path):
    state_path = tmp_path / "flow_posts_index.json"
    state_path.write_text(
        json.dumps(
            {
                "2466857:post-1": {
                    "project_id": "2466857",
                    "post_id": "post-1",
                    "updated_at": "SAME",
                }
            }
        ),
        encoding="utf-8",
    )
    post_dir = tmp_path / "flow_post"
    (post_dir / "project_id=2742104").mkdir(parents=True)
    pd.DataFrame([{"post_id": "other"}]).to_parquet(
        post_dir / "project_id=2742104" / "part.parquet",
        index=False,
    )

    monkeypatch.setattr(flow, "FLOW_STATE_JSON", state_path)
    monkeypatch.setattr(flow, "FLOW_POST_PARQUET", post_dir)

    changed = flow.detect_changed_posts(
        [
            {
                "project_id": "2466857",
                "post_id": "post-1",
                "registeredDateTime": "SAME",
                "project_name": "경기_동탄영천점",
            }
        ]
    )

    assert len(changed) == 1
    assert changed[0]["project_id"] == "2466857"


def test_detect_changed_posts_repairs_explicit_post_id_even_when_state_same(monkeypatch, tmp_path):
    state_path = tmp_path / "flow_posts_index.json"
    state_path.write_text(
        json.dumps(
            {
                "2548064:83047164": {
                    "project_id": "2548064",
                    "post_id": "83047164",
                    "updated_at": "SAME",
                }
            }
        ),
        encoding="utf-8",
    )
    post_dir = tmp_path / "flow_post"
    (post_dir / "project_id=2548064").mkdir(parents=True)
    pd.DataFrame([{"project_id": "2548064", "post_id": "83047164"}]).to_parquet(
        post_dir / "project_id=2548064" / "part.parquet",
        index=False,
    )

    class DagRun:
        conf = {"flow_repair_post_ids": ["83047164"]}

    monkeypatch.setattr(flow, "FLOW_STATE_JSON", state_path)
    monkeypatch.setattr(flow, "FLOW_POST_PARQUET", post_dir)

    changed = flow.detect_changed_posts(
        [
            {
                "project_id": "2548064",
                "post_id": "83047164",
                "registeredDateTime": "SAME",
                "project_name": "경기_대화점",
            }
        ],
        dag_run=DagRun(),
    )

    assert len(changed) == 1
    assert changed[0]["post_id"] == "83047164"
    assert changed[0]["attachment_check_only"] is False


def test_comments_are_plain_text_and_system_flagged():
    checks = {
        "69590085": False,
        "69590268": True,
        "75060833": False,
        "78555117": False,
    }
    for post_id, expected_system in checks.items():
        detail = _load_detail(post_id)
        assert detail["remarks"]
        row = flow._normalize_comment(detail["remarks"][0], post_id, "2026-08-03T00:00:00+09:00")
        assert row["is_system"] is expected_system
        assert "<a" not in row["content_text"]
        assert "onClick" not in row["content_text"]
        assert "\xa0" not in row["content_text"]


def test_nested_comment_replies_are_flattened_with_history_keys():
    comments = [
        {
            "COLABO_REMARK_SRNO": "c1",
            "COLABO_COMMT_SRNO": "p1",
            "COLABO_SRNO": "project1",
            "RGSR_NM": "오나영",
            "RGSN_DTTM": "20260813125000",
            "REMARK_CNTN": "원댓글",
            "SYSTEM_REMARK_YN": "N",
            "REPLY_CNT": "1",
            "replyRemarks": [
                {
                    "COLABO_REMARK_SRNO": "c1-1",
                    "COLABO_COMMT_SRNO": "p1",
                    "COLABO_SRNO": "project1",
                    "RGSR_NM": "김덕기",
                    "RGSN_DTTM": "20260818144700",
                    "REMARK_CNTN": "답글",
                    "SYSTEM_REMARK_YN": "N",
                }
            ],
        }
    ]

    rows = list(flow._iter_comment_tree(comments, "p1", "2026-08-18T00:00:00+09:00"))

    assert [row["comment_id"] for row in rows] == ["c1", "c1-1"]
    assert rows[0]["root_comment_id"] == "c1"
    assert rows[0]["parent_comment_id"] == ""
    assert rows[0]["comment_depth"] == "0"
    assert rows[1]["root_comment_id"] == "c1"
    assert rows[1]["parent_comment_id"] == "c1"
    assert rows[1]["comment_depth"] == "1"


def test_collect_post_details_records_comment_gap_when_api_hides_replies(monkeypatch):
    detail = _fake_detail("p1", "댓글 누락 업무")
    detail["projectId"] = "project1"
    detail["remarkCount"] = "4"
    detail["remarks"] = [
        {
            "COLABO_REMARK_SRNO": "c1",
            "COLABO_COMMT_SRNO": "p1",
            "COLABO_SRNO": "project1",
            "RGSR_NM": "조민준",
            "RGSN_DTTM": "20260818184348",
            "REMARK_CNTN": "테스트용 댓글입니다 1",
            "SYSTEM_REMARK_YN": "N",
            "REPLY_CNT": "3",
        }
    ]

    monkeypatch.setattr(flow, "_fetch_post_detail", lambda *_args: detail)

    details = flow.collect_post_details(
        [
            {
                "project_id": "project1",
                "project_name": "경기_대화점",
                "project_url": "https://flow.team/main.act?projectId=project1",
                "is_store": True,
                "region": "경기",
                "store_name": "대화점",
                "status_tag": "",
                "post_id": "p1",
                "updated_at": "LIST_UPDATED_AT",
            }
        ]
    )

    assert len(details["comments"]) == 1
    assert details["comment_gaps"] == [
        {
            "project_id": "project1",
            "post_id": "p1",
            "title": "댓글 누락 업무",
            "expected_comments": 4,
            "actual_comments": 1,
            "reply_gaps": [
                {
                    "comment_id": "c1",
                    "expected_replies": 3,
                    "actual_replies": 0,
                }
            ],
        }
    ]


def test_collect_post_details_merges_configured_extra_remarks(monkeypatch):
    detail = _fake_detail("p1", "댓글 보강 업무")
    detail["projectId"] = "project1"
    detail["remarkCount"] = "2"
    detail["remarks"] = [
        {
            "COLABO_REMARK_SRNO": "c1",
            "COLABO_COMMT_SRNO": "p1",
            "COLABO_SRNO": "project1",
            "RGSR_NM": "오나영",
            "RGSN_DTTM": "20260813125000",
            "REMARK_CNTN": "현재 키워드와 관련하여 연계되어 있는 업무가 다수 있습니다.",
            "SYSTEM_REMARK_YN": "N",
            "REPLY_CNT": "0",
        }
    ]
    extra = [
        {
            "COLABO_REMARK_SRNO": "c2",
            "COLABO_COMMT_SRNO": "p1",
            "COLABO_SRNO": "project1",
            "RGSR_NM": "김덕기",
            "RGSN_DTTM": "20260818144700",
            "REMARK_CNTN": "전달완료했습니다. 추가 답변따로 없으십니다.",
            "SYSTEM_REMARK_YN": "N",
            "REPLY_CNT": "0",
        }
    ]

    monkeypatch.setattr(flow, "_fetch_post_detail", lambda *_args: detail)
    monkeypatch.setattr(flow, "_fetch_extra_remarks", lambda *_args, **_kwargs: extra)

    details = flow.collect_post_details(
        [
            {
                "project_id": "project1",
                "project_name": "경기_대화점",
                "project_url": "https://flow.team/main.act?projectId=project1",
                "is_store": True,
                "region": "경기",
                "store_name": "대화점",
                "status_tag": "",
                "post_id": "p1",
                "updated_at": "LIST_UPDATED_AT",
            }
        ]
    )

    assert [row["comment_id"] for row in details["comments"]] == ["c1", "c2"]
    assert details["comment_gaps"] == []


def test_collect_post_details_hydrates_missing_reply_remarks(monkeypatch):
    detail = _fake_detail("p1", "대댓글 보강 업무")
    detail["projectId"] = "project1"
    detail["remarkCount"] = "2"
    detail["remarks"] = [
        {
            "COLABO_REMARK_SRNO": "c1",
            "COLABO_COMMT_SRNO": "p1",
            "COLABO_SRNO": "project1",
            "RGSR_NM": "조민준",
            "RGSN_DTTM": "20260818184348",
            "REMARK_CNTN": "삭제된 부모 댓글",
            "SYSTEM_REMARK_YN": "N",
            "DELETE_YN": "Y",
            "REPLY_CNT": "1",
        }
    ]
    replies = [
        {
            "COLABO_REMARK_SRNO": "r1",
            "RGSR_NM": "김덕기",
            "RGSN_DTTM": "20260818144700",
            "REMARK_CNTN": "전달완료했습니다. 추가 답변따로 없으십니다.",
            "SYSTEM_REMARK_YN": "N",
            "REPLY_CNT": "0",
        }
    ]

    def fake_fetch_extra(project_id, post_id, comment_id="", context=None):
        assert project_id == "project1"
        assert post_id == "p1"
        return replies if comment_id == "c1" else []

    monkeypatch.setattr(flow, "FLOW_REMARKS_API_URL_TEMPLATE", "https://flow.team/COLABO2_REMARK_R101.jct")
    monkeypatch.setattr(flow, "_fetch_post_detail", lambda *_args: detail)
    monkeypatch.setattr(flow, "_fetch_extra_remarks", fake_fetch_extra)

    details = flow.collect_post_details(
        [
            {
                "project_id": "project1",
                "project_name": "경기_대화점",
                "project_url": "https://flow.team/main.act?projectId=project1",
                "is_store": True,
                "region": "경기",
                "store_name": "대화점",
                "status_tag": "",
                "post_id": "p1",
                "updated_at": "LIST_UPDATED_AT",
            }
        ]
    )

    assert [row["comment_id"] for row in details["comments"]] == ["c1", "r1"]
    reply = details["comments"][1]
    assert reply["parent_comment_id"] == "c1"
    assert reply["root_comment_id"] == "c1"
    assert reply["comment_depth"] == "1"
    assert details["comment_gaps"] == []


def test_collect_post_details_normalizes_v2_comments_and_replies(monkeypatch):
    detail = _fake_detail("p1", "공식 댓글 API 업무")
    detail["projectId"] = "project1"
    detail["remarkCount"] = "2"
    detail["remarks"] = []
    v2_comments = [
        {
            "projectId": "project1",
            "postId": "p1",
            "commentId": "c1",
            "contents": "공식 댓글입니다.",
            "registerName": "오나영",
            "registerId": "owner@example.com",
            "registeredDateTime": "20260813125014",
            "systemCode": "",
        }
    ]
    v2_replies = [
        {
            "projectId": "project1",
            "postId": "p1",
            "replyId": "r1",
            "parentCommentId": "c1",
            "contents": "공식 대댓글입니다.",
            "registerName": "김덕기",
            "registerId": "reply@example.com",
            "registeredDateTime": "20260818144700",
            "systemCode": "",
        }
    ]

    def fake_fetch_extra(project_id, post_id, comment_id="", context=None):
        return v2_replies if comment_id == "c1" else v2_comments

    monkeypatch.setattr(flow, "FLOW_REMARKS_API_URL_TEMPLATE", "https://api.flow.team/user/comments/{post_id}")
    monkeypatch.setattr(flow, "FLOW_REPLIES_API_URL_TEMPLATE", "https://api.flow.team/user/comments/{post_id}/replies/{comment_id}")
    monkeypatch.setattr(flow, "_fetch_post_detail", lambda *_args: detail)
    monkeypatch.setattr(flow, "_fetch_extra_remarks", fake_fetch_extra)

    details = flow.collect_post_details(
        [
            {
                "project_id": "project1",
                "project_name": "경기_대화점",
                "project_url": "https://flow.team/main.act?projectId=project1",
                "is_store": True,
                "region": "경기",
                "store_name": "대화점",
                "status_tag": "",
                "post_id": "p1",
                "updated_at": "LIST_UPDATED_AT",
            }
        ]
    )

    assert [row["comment_id"] for row in details["comments"]] == ["c1", "r1"]
    assert details["comments"][0]["content_text"] == "공식 댓글입니다."
    assert details["comments"][1]["parent_comment_id"] == "c1"
    assert details["comment_gaps"] == []


def test_fetch_extra_remarks_posts_flow_jct_body(monkeypatch):
    captured = {}

    def fake_request_json(url, method="GET", body=None, extra_headers=None):
        captured["url"] = url
        captured["method"] = method
        captured["body"] = body
        captured["extra_headers"] = extra_headers
        return {"REPLY_REMARK_REC": [{"COLABO_REMARK_SRNO": "r1"}]}

    monkeypatch.setattr(flow, "FLOW_REMARKS_API_URL_TEMPLATE", "https://flow.team/COLABO2_REMARK_R101.jct")
    monkeypatch.setattr(flow, "FLOW_REPLIES_API_URL_TEMPLATE", "https://flow.team/COLABO2_REMARK_R101.jct")
    monkeypatch.setattr(flow, "FLOW_REMARKS_API_METHOD", "POST")
    monkeypatch.setattr(flow, "FLOW_REMARKS_API_BODY_TEMPLATE", "")
    monkeypatch.setattr(flow, "FLOW_REMARKS_API_BODY_FORMAT", "json")
    monkeypatch.setattr(flow, "FLOW_REMARKS_API_HEADERS_JSON", '{"Cookie":"flowLogin=test-session"}')
    monkeypatch.setattr(flow, "_request_json", fake_request_json)

    rows = flow._fetch_extra_remarks("project1", "p1", comment_id="c1")

    assert rows == [{"COLABO_REMARK_SRNO": "r1"}]
    assert captured["url"] == "https://flow.team/COLABO2_REMARK_R101.jct"
    assert captured["method"] == "POST"
    assert "_JSON_" in captured["body"]
    assert captured["extra_headers"]["Cookie"] == "flowLogin=test-session"
    assert captured["extra_headers"]["Content-Type"].startswith("application/x-www-form-urlencoded")


def test_save_flow_parquet_preserves_comment_gap_by_default(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, "FLOW_PROJECT_PARQUET", tmp_path / "flow_project" / "flow_project.parquet")
    monkeypatch.setattr(flow, "FLOW_POST_PARQUET", tmp_path / "flow_post")
    monkeypatch.setattr(flow, "FLOW_COMMENT_PARQUET", tmp_path / "flow_comment")
    monkeypatch.setattr(flow, "FLOW_ATTACHMENT_PARQUET", tmp_path / "flow_attachment")
    monkeypatch.setattr(flow, "FLOW_LEGACY_PROJECT_PARQUET", tmp_path / "legacy_project.parquet")
    monkeypatch.setattr(flow, "FLOW_LEGACY_POST_PARQUET", tmp_path / "legacy_post.parquet")
    monkeypatch.setattr(flow, "FLOW_LEGACY_COMMENT_PARQUET", tmp_path / "legacy_comment.parquet")
    monkeypatch.setattr(flow, "FLOW_STATE_JSON", tmp_path / "flow_posts_index.json")

    new_post = _row(
        flow._POST_COLS,
        project_id="project1",
        post_id="p1",
        title="댓글 누락 업무",
        edited_at="20260102020202",
        registered_at="20260102020202",
        content_hash="new-hash",
        collected_at="new",
    )

    message = flow.save_flow_parquet(
        {
            "projects": [],
            "posts": [new_post],
            "comments": [],
            "comment_gaps": [
                {
                    "project_id": "project1",
                    "post_id": "p1",
                    "expected_comments": 4,
                    "actual_comments": 1,
                    "reply_gaps": [],
                }
            ],
        }
    )

    assert "comment_gaps=1" in message
    assert (tmp_path / "flow_post" / "project_id=project1" / "part.parquet").exists()


def test_save_flow_parquet_fails_when_comment_gap_strict_conf_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, "FLOW_PROJECT_PARQUET", tmp_path / "flow_project" / "flow_project.parquet")
    monkeypatch.setattr(flow, "FLOW_POST_PARQUET", tmp_path / "flow_post")
    monkeypatch.setattr(flow, "FLOW_COMMENT_PARQUET", tmp_path / "flow_comment")
    monkeypatch.setattr(flow, "FLOW_ATTACHMENT_PARQUET", tmp_path / "flow_attachment")
    monkeypatch.setattr(flow, "FLOW_LEGACY_PROJECT_PARQUET", tmp_path / "legacy_project.parquet")
    monkeypatch.setattr(flow, "FLOW_LEGACY_POST_PARQUET", tmp_path / "legacy_post.parquet")
    monkeypatch.setattr(flow, "FLOW_LEGACY_COMMENT_PARQUET", tmp_path / "legacy_comment.parquet")
    monkeypatch.setattr(flow, "FLOW_STATE_JSON", tmp_path / "flow_posts_index.json")

    new_post = _row(
        flow._POST_COLS,
        project_id="project1",
        post_id="p1",
        title="댓글 누락 업무",
        edited_at="20260102020202",
        registered_at="20260102020202",
        content_hash="new-hash",
        collected_at="new",
    )

    class DagRun:
        conf = {"flow_comment_gap_fail_on_mismatch": True}

    with pytest.raises(RuntimeError, match="Flow 댓글 수집 불완전"):
        flow.save_flow_parquet(
            {
                "projects": [],
                "posts": [new_post],
                "comments": [],
                "comment_gaps": [
                    {
                        "project_id": "project1",
                        "post_id": "p1",
                        "expected_comments": 4,
                        "actual_comments": 1,
                        "reply_gaps": [],
                    }
                ],
            },
            dag_run=DagRun(),
        )

    assert not (tmp_path / "flow_post").exists()


def test_api_config_missing_fails(monkeypatch):
    monkeypatch.setattr(flow, "FLOW_API_KEY", "")

    with pytest.raises(RuntimeError, match="Flow API 설정 누락"):
        flow._require_api_config()


def test_default_api_contract_uses_user_api_without_empty_cursor():
    assert flow.FLOW_AUTH_HEADER_NAME == "x-flow-api-key"
    assert flow.FLOW_PROJECTS_API_URL == "https://api.flow.team/user/projects"
    base_url = flow._format_api_url(flow.FLOW_POSTS_API_URL_TEMPLATE, project_id="2742104", cursor="")
    assert base_url == "https://api.flow.team/user/posts/projects/2742104"
    assert flow._append_query(base_url, {"cursor": ""}) == base_url
    assert flow._append_query(base_url, {"cursor": "1"}).endswith("?cursor=1")


def test_save_flow_parquet_migrates_legacy_to_project_partitions(tmp_path, monkeypatch):
    legacy_dir = tmp_path / "analytics" / "flow"
    mart_dir = tmp_path / "mart" / "flow"
    legacy_dir.mkdir(parents=True)
    state_path = tmp_path / "Local_DB" / "flow_posts_index.json"

    old_project = _row(flow._PROJECT_COLS, project_id="111", project_name="old", collected_at="old")
    old_post = _row(
        flow._POST_COLS,
        project_id="111",
        post_id="old-post",
        title="old title",
        content_hash="old-hash",
        collected_at="old",
    )
    old_comment = _row(
        flow._COMMENT_COLS,
        project_id="111",
        post_id="old-post",
        comment_id="old-comment",
        content_text="old comment",
        collected_at="old",
    )
    pd.DataFrame([old_project]).to_parquet(legacy_dir / "flow_project.parquet", index=False)
    pd.DataFrame([old_post]).to_parquet(legacy_dir / "flow_post.parquet", index=False)
    pd.DataFrame([old_comment]).to_parquet(legacy_dir / "flow_comment.parquet", index=False)

    monkeypatch.setattr(flow, "FLOW_PROJECT_PARQUET", mart_dir / "flow_project" / "flow_project.parquet")
    monkeypatch.setattr(flow, "FLOW_POST_PARQUET", mart_dir / "flow_post")
    monkeypatch.setattr(flow, "FLOW_COMMENT_PARQUET", mart_dir / "flow_comment")
    monkeypatch.setattr(flow, "FLOW_ATTACHMENT_PARQUET", mart_dir / "flow_attachment")
    monkeypatch.setattr(flow, "FLOW_LEGACY_PROJECT_PARQUET", legacy_dir / "flow_project.parquet")
    monkeypatch.setattr(flow, "FLOW_LEGACY_POST_PARQUET", legacy_dir / "flow_post.parquet")
    monkeypatch.setattr(flow, "FLOW_LEGACY_COMMENT_PARQUET", legacy_dir / "flow_comment.parquet")
    monkeypatch.setattr(flow, "FLOW_STATE_JSON", state_path)

    new_project = _row(flow._PROJECT_COLS, project_id="222", project_name="new", collected_at="new")
    new_post = _row(
        flow._POST_COLS,
        project_id="222",
        post_id="new-post",
        title="new title",
        edited_at="20260102020202",
        registered_at="20260102020202",
        content_hash="new-hash",
        collected_at="new",
    )
    new_comment = _row(
        flow._COMMENT_COLS,
        project_id="222",
        post_id="new-post",
        comment_id="new-comment",
        content_text="new comment",
        collected_at="new",
    )

    message = flow.save_flow_parquet(
        {
            "projects": [new_project],
            "posts": [new_post],
            "comments": [new_comment],
            "state_updates": {"new-post": "LIST_UPDATED_AT"},
            "complete": True,
        }
    )

    assert "posts=2 comments=2" in message
    assert (mart_dir / "flow_post" / "project_id=111" / "part.parquet").exists()
    assert (mart_dir / "flow_post" / "project_id=222" / "part.parquet").exists()
    assert (mart_dir / "flow_comment" / "project_id=111" / "part.parquet").exists()
    assert (mart_dir / "flow_comment" / "project_id=222" / "part.parquet").exists()

    posts = flow._read_parquet_or_empty(mart_dir / "flow_post", flow._POST_COLS)
    comments = flow._read_parquet_or_empty(mart_dir / "flow_comment", flow._COMMENT_COLS)
    assert set(posts["post_id"]) == {"old-post", "new-post"}
    assert set(comments["comment_id"]) == {"old-comment", "new-comment"}

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["222:new-post"]["updated_at"] == "LIST_UPDATED_AT"


def test_flow_pipeline_does_not_expose_postgres_loader():
    assert not hasattr(flow, "load_to_postgres")


def test_new_flow_files_do_not_use_print():
    repo = Path(__file__).resolve().parents[1]
    for rel_path in [
        "modules/transform/pipelines/strategy/SMP_flow_store_collect.py",
        "dags/strategy/Strategy_FlowStore_01_Collect_Dags.py",
    ]:
        assert "print(" not in (repo / rel_path).read_text(encoding="utf-8")
