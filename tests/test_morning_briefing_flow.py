import pandas as pd

from modules.transform.pipelines.strategy import morning_briefing_pipeline as briefing


def test_today_items_filter_target_author_worker_and_done_status():
    posts = pd.DataFrame(
        [
            {
                "post_id": "1",
                "title": "작성자 업무",
                "author_name": "조민준 PM",
                "worker": "",
                "task_status": "피드백",
                "end_dt": "20260804",
                "start_dt": "",
            },
            {
                "post_id": "2",
                "title": "담당자 업무",
                "author_name": "김대진",
                "worker": "김대진, 조민준",
                "task_status": "진행",
                "end_dt": "",
                "start_dt": "20260804",
            },
            {
                "post_id": "3",
                "title": "타인 업무",
                "author_name": "김대진",
                "worker": "차보령",
                "task_status": "진행",
                "end_dt": "20260804",
                "start_dt": "",
            },
            {
                "post_id": "4",
                "title": "완료 업무",
                "author_name": "조민준",
                "worker": "",
                "task_status": "완료",
                "end_dt": "20260804",
                "start_dt": "",
            },
        ]
    )

    items = briefing._collect_today_flow_items(posts, "2026-08-04", "조민준")

    assert [item["title"] for item in items] == ["작성자 업무", "담당자 업무"]
    formatted = briefing._format_flow_today_sections(items)
    assert "작성자: 조민준 PM | 담당자: 미지정" in formatted
    assert "작성자: 김대진 | 담당자: 김대진, 조민준" in formatted


def test_yesterday_review_includes_target_comment_context():
    posts = pd.DataFrame(
        [
            {
                "post_id": "10",
                "title": "댓글 남긴 업무",
                "project_name": "전략기획",
                "author_name": "김대진",
                "worker": "차보령",
                "task_status": "진행",
                "post_date": "20260801",
                "registered_at": "20260801100000",
                "edited_at": "20260801100000",
            }
        ]
    )
    comments = pd.DataFrame(
        [
            {
                "comment_id": "c1",
                "post_id": "10",
                "author_name": "조민준",
                "written_at": "20260803120000",
                "content_text": "확인했습니다.",
                "is_system": False,
                "sys_code": "",
            }
        ]
    )

    items = briefing._collect_yesterday_review_items(posts, comments, "2026-08-03", "조민준")

    assert len(items) == 1
    assert items[0]["title"] == "댓글 남긴 업무"
    assert items[0]["events"] == ["댓글 작성"]
    assert items[0]["comment_samples"] == ["확인했습니다."]
    formatted = briefing._format_flow_review_sections(items)
    assert "작성자: 김대진 | 담당자: 차보령" in formatted


def test_improvements_ignore_worker_only_old_posts():
    posts = pd.DataFrame(
        [
            {
                "post_id": "old",
                "title": "오래된 문의글",
                "project_name": "Flow 문의",
                "author_name": "조민준 PM",
                "worker": "",
                "task_status": "",
                "end_dt": "",
                "start_dt": "",
            },
            {
                "post_id": "assigned",
                "title": "마감 지난 담당업무",
                "project_name": "전략기획",
                "author_name": "김대진",
                "worker": "조민준",
                "task_status": "진행",
                "end_dt": "20260801",
                "start_dt": "",
            },
            {
                "post_id": "authored",
                "title": "마감 지난 작성업무",
                "project_name": "전략기획",
                "author_name": "조민준",
                "worker": "김대진",
                "task_status": "진행",
                "end_dt": "20260801",
                "start_dt": "",
            },
        ]
    )
    comments = pd.DataFrame()

    improvements = briefing._collect_flow_improvements(
        posts,
        comments,
        "2026-08-04",
        yesterday_items=[],
        target_name="조민준",
    )

    assert all("오래된 문의글" not in item for item in improvements)
    assert all("마감 지난 담당업무" not in item for item in improvements)
    assert any("마감 지난 작성업무" in item for item in improvements)
