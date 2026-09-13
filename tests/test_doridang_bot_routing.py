"""도리당봇 라우팅 / 사람 기준(basis) 회귀 테스트.

봇에는 테스트가 하나도 없었다. 여기서 고정하는 것은 두 가지다.
1. get_team_status와 get_worker_status가 같은 사람에 대해 같은 숫자를 낸다
2. 룰 라우팅이 tests/fixtures/doridang_eval.jsonl 기준선 아래로 떨어지지 않는다
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from modules.transform.doridang_bot import router, tools
from modules.transform.utility import flow_task_status as status_rules

REPO_ROOT = Path(__file__).resolve().parents[1]
EVAL_PATH = REPO_ROOT / "tests" / "fixtures" / "doridang_eval.jsonl"

# 2026-09-02 측정한 룰 라우팅 baseline(38문항 기준). 이 아래로 내려가면 회귀다.
# LLM 라우팅(실제 동작)은 같은 평가셋에서 37/38이지만, Ollama가 필요하므로 pytest에서는 재지 않는다.
#   측정: python scripts/eval_doridang_bot.py --mode llm
ROUTING_BASELINE = 26


def _post(**overrides):
    row = {
        "project_id": "2926716",
        "project_name": "[브랜드 전략기획부] 직영점 성장전략(온라인 유입)",
        "post_id": "p1",
        "parent_post_id": "",
        "depth": 0,
        "title": "테스트 업무",
        "post_date": "2026-08-01",
        "author_name": "조민준",
        "content_text": "본문",
        "task_status": "진행",
        "progress": "50",
        "worker": "",
        "start_dt": "20260801",
        "end_dt": "20260810",
        "remark_cnt": 0,
        "child_cnt": 0,
        "post_url": "https://flow.team/l/test",
        "collected_at": "2026-09-01T19:38:55.136097+09:00",
    }
    row.update(overrides)
    return row


@pytest.fixture
def posts(monkeypatch):
    """담당자/작성자가 엇갈리는 표본. 기준마다 다른 숫자가 나오도록 구성했다."""
    df = pd.DataFrame(
        [
            # 담당자 지정 있음 -> auto/worker 모두 황유경
            _post(post_id="p1", author_name="조민준", worker="황유경", task_status="진행"),
            # 담당자 비어 있음 -> auto는 작성자 차보령, worker 기준에서는 빠진다
            _post(post_id="p2", author_name="차보령", worker="", task_status="대기"),
            # 담당자 다중값
            _post(post_id="p3", author_name="조민준", worker="황유경, 차보령", task_status="보류"),
            # 작성자와 담당자가 같은 사람
            _post(post_id="p4", author_name="조민준", worker="조민준", task_status="완료"),
            # 허용 범위 밖 프로젝트 -> 어떤 집계에도 들어가면 안 된다
            _post(post_id="p9", project_id="2499984", author_name="황유경", worker="황유경"),
        ]
    )
    monkeypatch.setattr(tools, "_load_posts", lambda: df.copy())
    return df


@pytest.mark.parametrize("basis", [tools.BASIS_AUTO, tools.BASIS_AUTHOR, tools.BASIS_WORKER])
@pytest.mark.parametrize("name", ["조민준", "황유경", "차보령"])
def test_team_status_and_worker_status_agree(posts, basis, name):
    """같은 기준이면 팀 집계와 개인 조회의 숫자가 반드시 같아야 한다."""
    team = tools.get_team_status(basis=basis)
    member = next((m for m in team["members"] if m["worker"] == name), None)
    worker = tools.get_worker_status(name, basis=basis)

    expected = member["post_count"] if member else 0
    assert worker.get("post_count", 0) == expected
    assert worker.get("overdue_count", 0) == (member["overdue_count"] if member else 0)


def test_auto_basis_falls_back_to_author(posts):
    """담당자가 비어 있으면 작성자를 담당자로 본다."""
    auto = tools.get_worker_status("차보령", basis=tools.BASIS_AUTO)
    worker_only = tools.get_worker_status("차보령", basis=tools.BASIS_WORKER)
    # p2(담당자 공란, 작성자 차보령) + p3(담당자 다중값) = 2건
    assert auto["post_count"] == 2
    # 담당자 지정만 세면 p3 하나
    assert worker_only["post_count"] == 1


def test_out_of_scope_project_is_excluded(posts):
    """허용 3개 프로젝트 밖 게시글은 사람 집계에 절대 섞이면 안 된다."""
    result = tools.get_worker_status("황유경", basis=tools.BASIS_AUTO)
    for post in result["posts"]:
        assert post["project_id"] in tools.ALLOWED_PROJECTS


def test_out_of_scope_project_status_is_denied():
    denied = tools.get_project_status("2499984")
    assert denied.get("allowed") is False
    assert "posts" not in denied


def test_basis_normalization():
    assert tools._normalize_basis("") == tools.BASIS_AUTO
    assert tools._normalize_basis("작성자") == tools.BASIS_AUTHOR
    assert tools._normalize_basis("담당자") == tools.BASIS_WORKER
    assert tools._normalize_basis("아무말") == tools.BASIS_AUTO
    assert tools.basis_label(tools.BASIS_AUTHOR) == "작성자"
    assert tools.basis_note(tools.BASIS_AUTO)


def test_find_posts_is_registered():
    """스키마에 광고해놓고 등록을 빠뜨려 '알 수 없는 tool입니다'가 나던 버그."""
    advertised = {schema["function"]["name"] for schema in tools.tool_schemas()}
    assert advertised <= set(tools.TOOL_FUNCTIONS), advertised - set(tools.TOOL_FUNCTIONS)
    assert "find_posts" in tools.TOOL_FUNCTIONS


def test_execute_tool_rejects_unknown_name():
    assert "error" in tools.execute_tool("nope", {})


@pytest.mark.parametrize(
    "question, expected_tool",
    [
        ("안녕", router.LIGHT_CHAT_TOOL),
        ("팀원 별 프로젝트 진행상황", "get_team_status"),
        ("황유경 실장의 진행상황을 알려줘", "get_worker_status"),
        ("직영점 성장전략 진행상황 알려줘", "get_project_status"),
        ("몇시야?", None),
    ],
)
def test_route_question(question, expected_tool):
    route = router.route_question(question)
    assert (route.tool if route else None) == expected_tool


def test_routing_accuracy_does_not_regress():
    """평가셋 기준 라우팅 정확도가 baseline 아래로 떨어지면 실패한다."""
    cases = [json.loads(line) for line in EVAL_PATH.read_text(encoding="utf-8").splitlines() if line.strip()]
    scored = [case for case in cases if not case.get("requires_context")]

    passed = 0
    for case in scored:
        # use_llm=False: Ollama 없이도 도는 룰 경로만 회귀선으로 고정한다
        resolution = router.resolve_route(case["question"], history=case.get("history"), use_llm=False)
        actual_tool = resolution.route.tool if resolution.route else None
        actual_args = resolution.route.arguments if resolution.route else {}
        if actual_tool != case.get("expect_tool"):
            continue
        if all(actual_args.get(k) == v for k, v in (case.get("expect_args") or {}).items()):
            passed += 1

    assert passed >= ROUTING_BASELINE, f"라우팅 회귀: {passed}/{len(scored)} (baseline {ROUTING_BASELINE})"

# --- 1B: LLM 라우팅 보정 장치 ---


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("조민jun", "조민준"),          # qwen이 한글 이름을 깨뜨리는 실제 사례
        ("황유경 실장", "황유경"),
        ("차보령", "차보령"),
        ("전혀없는사람", "전혀없는사람"),  # 못 찾으면 원본을 그대로 둔다
    ],
)
def test_snap_worker_name(raw, expected):
    assert tools.snap_worker_name(raw) == expected


def test_sanitize_drops_unstated_basis():
    """모델이 근거 없이 basis를 붙이면 통일한 기본 기준이 흔들린다."""
    assert "basis" not in router._sanitize_arguments("황유경 피드백 알려줘", {"basis": "worker"})
    assert router._sanitize_arguments("작성자 기준으로 알려줘", {"basis": "author"})["basis"] == "author"


def test_sanitize_drops_disallowed_project():
    cleaned = router._sanitize_arguments("아무거나", {"project_id": "2499984", "status": "진행"})
    assert "project_id" not in cleaned
    assert cleaned["status"] == "진행"


def test_route_from_text_salvages_written_tool_call():
    """모델이 tool을 호출하지 않고 문장으로 써버리는 경우를 건진다."""
    route = router._route_from_text('filter_posts(status="진행")')
    assert route is not None
    assert route.tool == "filter_posts"
    assert route.arguments == {"status": "진행"}
    assert router._route_from_text("그냥 문장입니다 (괄호 포함)") is None
    assert router._route_from_text("os.system(rm)") is None


def test_filter_posts_accepts_worker(posts):
    """'그 중에 기한 지난 건' 같은 후속 질문을 한 번에 답하기 위한 조건."""
    everyone = tools.filter_posts(status="보류")
    only_one = tools.filter_posts(status="보류", worker="황유경")
    assert only_one["post_count"] <= everyone["post_count"]
    assert only_one["worker_filter"] == "황유경"
    for post in only_one["posts"]:
        assert "황유경" in f"{post['worker']}{post['author_name']}"


def test_resolve_route_prefers_rule_when_llm_declines(monkeypatch):
    """LLM이 '범위 밖'이라 해도 룰이 아는 질문이면 룰을 믿는다."""
    monkeypatch.setattr(router, "route_with_llm",
                        lambda question, history=None: router.LlmRouting(ok=True, text="답변 범위 밖입니다."))
    resolution = router.resolve_route("황유경")
    assert resolution.source == "rule"
    assert resolution.route.tool == "get_worker_status"


def test_resolve_route_falls_back_to_rules_on_llm_failure(monkeypatch):
    monkeypatch.setattr(router, "route_with_llm",
                        lambda question, history=None: router.LlmRouting(ok=False))
    resolution = router.resolve_route("팀원 별 프로젝트 진행상황")
    assert resolution.source == "rule"
    assert resolution.route.tool == "get_team_status"


def test_resolve_route_returns_scope_reply(monkeypatch):
    monkeypatch.setattr(router, "route_with_llm",
                        lambda question, history=None: router.LlmRouting(ok=True, text="답변 범위 밖입니다."))
    resolution = router.resolve_route("몇시야?")
    assert resolution.route is None
    assert resolution.source == "llm-scope"
    assert resolution.text

# --- 1C: 업무 상태 분류 (업무단위/회의록/액션은 업무가 아니다) ---


@pytest.fixture
def mixed_posts(monkeypatch):
    """업무와 기록이 섞인 표본. 기한 있음/없음도 섞어 정렬을 검증한다."""
    df = pd.DataFrame(
        [
            _post(post_id="t1", task_status="진행", end_dt="20200101", worker="황유경"),   # 기한 지남
            _post(post_id="t2", task_status="대기", end_dt="21000101", worker="황유경"),   # 기한 멀었음
            _post(post_id="t3", task_status="진행", end_dt="", worker="황유경"),           # 기한 없음
            _post(post_id="t4", task_status="완료", end_dt="20200101", worker="황유경"),   # 완료는 지나도 경과 아님
            _post(post_id="r1", task_status="업무단위", end_dt="20200101", worker="황유경"),
            _post(post_id="r2", task_status="회의록", end_dt="", worker="황유경"),
            _post(post_id="r3", task_status="액션", end_dt="20200101", worker="황유경"),
            _post(post_id="u1", task_status="실행중", end_dt="20200101", worker="황유경"),  # 모르는 라벨
        ]
    )
    monkeypatch.setattr(tools, "_load_posts", lambda: df.copy())
    return df


def test_status_classification_basics():
    assert status_rules.is_task("진행") and status_rules.is_open("진행")
    assert status_rules.is_task("완료") and status_rules.is_done("완료")
    assert not status_rules.is_open("완료")
    for record in ("업무단위", "회의록", "액션"):
        assert not status_rules.is_task(record), record
        assert not status_rules.is_open(record), record


def test_unknown_status_is_treated_as_open_task():
    """라벨이 Flow 자유 입력이라 새 값이 생긴다. 비업무로 숨기면 리더 시야에서 사라진다."""
    assert status_rules.is_task("실행중")
    assert status_rules.is_open("실행중")
    assert not status_rules.is_done("실행중")
    assert status_rules.unknown_statuses(["진행", "실행중", "완료"]) == {"실행중"}
    assert status_rules.unknown_statuses(None) == set()


def test_overdue_excludes_records_and_done(mixed_posts):
    """조민준의 '기한 경과 2건' 중 하나가 [업무단위]였던 버그."""
    overdue = tools._overdue_posts(mixed_posts)
    assert set(overdue["post_id"]) == {"t1", "u1"}


def test_progress_sort_puts_open_tasks_first_and_no_due_last(mixed_posts):
    """기한 없는 글이 빈 문자열이라 오름차순 최상단을 차지하던 버그."""
    ordered = list(tools._sort_for_progress(tools._task_posts(mixed_posts))["post_id"])
    # t1/u1은 기한이 같아 순서가 갈릴 수 있다. 의미 있는 순서만 고정한다.
    assert set(ordered[:2]) == {"t1", "u1"}   # 기한 지난 열린 업무가 맨 앞
    assert ordered[2] == "t2"                 # 그 다음 기한 남은 열린 업무
    assert ordered[3] == "t3"                 # 기한 없는 열린 업무는 뒤
    assert ordered[4] == "t4"                 # 완료는 맨 뒤


def test_end_sort_key_pushes_missing_due_to_the_end(mixed_posts):
    keys = tools._end_sort_key(mixed_posts)
    assert keys.loc[mixed_posts["post_id"].eq("t3").idxmax()] == status_rules.NO_DUE_SORT_KEY


def test_task_and_record_counts_add_up(mixed_posts):
    result = tools.get_worker_status("황유경")
    assert result["task_count"] + result["record_count"] == result["post_count"]
    assert result["record_count"] == 3
    assert result["record_counts"] == {"업무단위": 1, "회의록": 1, "액션": 1}
    # 업무 상태 집계에 기록이 섞이면 안 된다
    assert set(result["status_counts"]) == {"진행", "대기", "완료", "실행중"}


def test_worker_posts_list_contains_no_records(mixed_posts):
    """'진행상황'을 물었는데 회의록 목록이 나오던 문제."""
    result = tools.get_worker_status("황유경")
    for post in result["posts"]:
        assert status_rules.is_task(post["task_status"]), post


def test_filter_posts_excludes_records_by_default(mixed_posts):
    overdue = tools.filter_posts(due="overdue")
    assert set(post["post_id"] for post in overdue["posts"]) == {"t1", "u1"}


def test_filter_posts_returns_records_when_explicitly_asked(mixed_posts):
    """'회의록 보여줘'는 명시 지정이므로 조회돼야 한다."""
    result = tools.filter_posts(status="회의록")
    assert result["post_count"] == 1
    assert result["posts"][0]["post_id"] == "r2"


def test_team_status_separates_records(mixed_posts):
    team = tools.get_team_status()
    assert team["task_count"] + team["record_count"] == team["post_count"]
    member = next(m for m in team["members"] if m["worker"] == "황유경")
    assert member["record_count"] == 3
    # 열린 업무: t1, t2, t3, u1
    assert member["active_count"] == 4


def test_status_detection_order_is_deterministic():
    """set 순회는 순서가 고정되지 않는다. 질문 매칭은 튜플 순서를 따라야 한다."""
    assert isinstance(status_rules.DETECTION_ORDER, tuple)
    assert router.status_due_filters("회의록 보여줘") == {"status": "회의록"}
    assert router.status_due_filters("진행중만 찾아줘") == {"status": "진행"}
    # 캘린더 어휘였던 결제중은 flow_post에 없어 제거했다
    assert "결제중" not in status_rules.DETECTION_ORDER

# --- 1D: 조건 유실 차단 + 모니터링 세 번째 버킷 ---


@pytest.fixture
def monitoring_posts(monkeypatch):
    """모니터링은 집계에서 빠지지만 기한은 감시한다."""
    df = pd.DataFrame(
        [
            _post(post_id="m1", task_status="모니터링", end_dt="20200101", worker="조민준"),  # 기한 지남
            _post(post_id="m2", task_status="모니터링", end_dt="21000101", worker="조민준"),  # 기한 남음
            _post(post_id="p1", task_status="진행", end_dt="21000101", worker="조민준"),
            _post(post_id="d1", task_status="완료", end_dt="20200101", worker="조민준"),
            _post(post_id="r1", task_status="회의록", end_dt="20200101", worker="조민준"),   # 기한 있어도 무시
        ]
    )
    monkeypatch.setattr(tools, "_load_posts", lambda: df.copy())
    return df


def test_monitoring_is_excluded_from_tasks_but_tracks_due():
    assert not status_rules.is_task("모니터링")
    assert not status_rules.is_open("모니터링")
    assert status_rules.tracks_due("모니터링")          # 기한은 계속 본다
    # 묶음·기록은 기한도 보지 않는다
    for record in ("업무단위", "회의록", "액션"):
        assert not status_rules.tracks_due(record), record
    # 완료는 기한이 지나도 경과가 아니다
    assert not status_rules.tracks_due("완료")
    # 모니터링은 위험 순위가 없다 — 기한 경과로만 위험에 들어온다
    assert status_rules.risk_rank("모니터링") == status_rules.UNRANKED


def test_overdue_includes_monitoring_but_not_records(monitoring_posts):
    overdue = tools._overdue_posts(monitoring_posts)
    assert set(overdue["post_id"]) == {"m1"}


def test_monitoring_with_remaining_due_is_not_a_risk(monitoring_posts):
    risky = set(tools._risk_posts(monitoring_posts)["post_id"])
    assert "m2" not in risky   # 기한 남은 모니터링은 위험이 아니다
    assert "m1" in risky       # 기한 지난 모니터링은 위험이다


def test_monitoring_is_reported_outside_task_counts(monitoring_posts):
    result = tools.get_worker_status("조민준")
    assert result["task_count"] == 2          # 진행 1 + 완료 1
    assert result["record_count"] == 3        # 모니터링 2 + 회의록 1
    assert result["record_counts"]["모니터링"] == 2
    assert "모니터링" not in result["status_counts"]


def test_progress_list_has_only_open_work(monitoring_posts):
    """'진행상황'을 물었는데 완료와 모니터링이 목록에 섞이던 문제."""
    result = tools.get_worker_status("조민준")
    assert [post["post_id"] for post in result["posts"]] == ["p1"]
    assert result["open_count"] == 1


def test_empty_progress_list_says_so(monkeypatch):
    df = pd.DataFrame([_post(post_id="d1", task_status="완료", worker="조민준")])
    monkeypatch.setattr(tools, "_load_posts", lambda: df.copy())
    from modules.transform.doridang_bot import server

    text = server._format_worker_status(tools.get_worker_status("조민준"))
    assert "진행 중인 업무가 없습니다." in text


# --- 조건이 조용히 버려지던 문제 ---


def test_accepted_arguments_reflects_signature():
    assert tools.accepted_arguments("get_worker_status") == {"worker", "basis"}
    assert {"status", "due", "worker"} <= tools.accepted_arguments("filter_posts")
    assert tools.accepted_arguments("없는tool") == set()


def test_filter_arguments_warns_when_dropping(caplog):
    """말없이 버리면 사용자의 조건이 흔적 없이 사라진다."""
    with caplog.at_level("WARNING", logger="modules.transform.doridang_bot.tools"):
        kept = tools._filter_arguments(tools.get_worker_status, {"worker": "조민준", "status": "진행"})
    assert kept == {"worker": "조민준"}
    assert "status" in caplog.text


@pytest.mark.parametrize(
    "tool, arguments, expected_tool, expected_args",
    [
        # "진행만 정리해줘" 가 전체를 답하던 실제 버그
        ("get_worker_status", {"worker": "조민준", "status": "진행"},
         "filter_posts", {"worker": "조민준", "status": "진행"}),
        ("get_project_status", {"project_id": "2926713", "due": "overdue"},
         "filter_posts", {"project_id": "2926713", "due": "overdue"}),
        # 조건이 없으면 그대로 둔다
        ("get_worker_status", {"worker": "조민준"},
         "get_worker_status", {"worker": "조민준"}),
        # 이미 조건을 받는 tool이면 그대로
        ("filter_posts", {"worker": "조민준", "status": "진행"},
         "filter_posts", {"worker": "조민준", "status": "진행"}),
        # 전환하면 keyword를 잃으므로 전환하지 않는다
        ("get_topic_status", {"keyword": "마케팅", "status": "진행"},
         "get_topic_status", {"keyword": "마케팅", "status": "진행"}),
    ],
)
def test_redirect_to_filter(tool, arguments, expected_tool, expected_args):
    assert router._redirect_to_filter(tool, dict(arguments)) == (expected_tool, expected_args)


def test_redirected_route_actually_filters(monitoring_posts):
    """재라우팅 결과가 실제로 조건을 적용하는지 끝까지 확인한다."""
    name, arguments = router._redirect_to_filter("get_worker_status", {"worker": "조민준", "status": "진행"})
    result = tools.execute_tool(name, arguments)
    assert result["post_count"] == 1
    assert result["status_counts"] == {"진행": 1}


def test_is_known_person_rejects_non_names():
    assert tools.is_known_person("조민준")
    assert tools.is_known_person("황유경 실장")
    assert not tools.is_known_person("승인검증")
    assert not tools.is_known_person("")


def test_route_falls_back_to_rules_when_worker_is_not_a_person(monkeypatch):
    """'...진행상황 승인검증'을 worker='승인검증'으로 읽어 0건을 답하던 문제."""
    monkeypatch.setattr(
        router, "route_with_llm",
        lambda question, history=None: router.LlmRouting(
            ok=True, route=router.Route("get_worker_status", {"worker": "승인검증"})
        ),
    )
    resolution = router.resolve_route("회사 현황판 구축 진행상황 승인검증")
    assert resolution.source == "rule"
    assert resolution.route.tool == "get_project_status"
    assert resolution.route.arguments["project_id"] == "2926713"


def test_plausible_route_with_known_person_is_kept(monkeypatch):
    monkeypatch.setattr(
        router, "route_with_llm",
        lambda question, history=None: router.LlmRouting(
            ok=True, route=router.Route("get_worker_status", {"worker": "조민준"})
        ),
    )
    resolution = router.resolve_route("조민준 진행상황")
    assert resolution.source == "llm"
    assert resolution.route.arguments["worker"] == "조민준"

# --- 1E: 직전 결과 기억(대화) + 낡은 데이터 경고 ---


@pytest.fixture
def bot_server():
    from modules.transform.doridang_bot import server

    with server._SESSION_LOCK:
        server._SESSION_MEMORY.clear()
    return server


@pytest.fixture
def last_turn():
    return {
        "question": "조민준 진행상황",
        "tool": "get_worker_status",
        "arguments": {"worker": "조민준"},
        "posts": [
            {"post_id": "p1", "title": "김덕기 과장 교육", "task_status": "진행",
             "end_dt": "20260828", "author_name": "조민준", "worker": "", "content_text": "",
             "project_id": "2926713", "project_name": "회사 현황판 구축", "post_url": ""},
            {"post_id": "p2", "title": "현장 요청사항", "task_status": "진행",
             "end_dt": "", "author_name": "차보령", "worker": "", "content_text": "본문 내용",
             "project_id": "2926716", "project_name": "직영점 성장전략", "post_url": ""},
        ],
        "result": {"post_count": 2},
        "answer": "",
    }


@pytest.mark.parametrize(
    "question, expected",
    [
        ("진행2건은 뭐야?", True),        # 사용자가 실제로 물어 문제를 발견한 질문
        ("그 2건은?", True),
        ("첫번째 거 자세히", True),
        ("그거 무슨 내용이야", True),
        ("보류 3건 보여줘", False),        # 건수가 안 맞고 되묻지도 않는다 -> 새 조회
        ("그 중에 기한 지난 건만", False),  # 조건이 붙으면 filter_posts 가 낫다
        ("황유경 진행상황", False),         # 새 사람
        ("브랜드 바이럴 진행상황", False),   # 새 프로젝트
    ],
)
def test_refers_to_last(bot_server, last_turn, question, expected):
    assert bot_server._refers_to_last(question, last_turn) is expected


def test_refers_to_last_needs_previous_result(bot_server):
    assert bot_server._refers_to_last("그 2건은?", None) is False
    assert bot_server._refers_to_last("그 2건은?", {"posts": []}) is False


def test_session_memory_roundtrip(bot_server, last_turn):
    bot_server._remember_turn("sess-1", last_turn)
    assert bot_server._recall_turn("sess-1")["tool"] == "get_worker_status"
    assert bot_server._recall_turn("없는세션") is None


def test_session_memory_skips_without_session_id(bot_server, last_turn):
    """구 클라이언트가 session_id를 안 보내면 그냥 건너뛴다."""
    bot_server._remember_turn("", last_turn)
    assert bot_server._recall_turn("") is None
    assert len(bot_server._SESSION_MEMORY) == 0


def test_session_memory_is_bounded(bot_server, last_turn):
    for index in range(bot_server.SESSION_MEMORY_MAX + 10):
        bot_server._remember_turn(f"s{index}", last_turn)
    assert len(bot_server._SESSION_MEMORY) == bot_server.SESSION_MEMORY_MAX
    assert bot_server._recall_turn("s0") is None            # 오래된 것부터 밀려난다
    assert bot_server._recall_turn("s59") is not None


@pytest.mark.parametrize(
    "question, expected",
    [("첫번째 거 자세히", 1), ("두번째", 2), ("3번째 알려줘", 3), ("그 2건은?", None)],
)
def test_requested_ordinal(bot_server, question, expected):
    assert bot_server._requested_ordinal(question) == expected


def test_last_result_detail_renders_all(bot_server, last_turn):
    text = bot_server._format_last_result_detail("그 2건은?", last_turn)
    assert "2건" in text
    assert "김덕기 과장 교육" in text and "현장 요청사항" in text
    assert "⚠지남" in text                                   # 기한 지난 건 표시가 남는다


def test_last_result_detail_picks_one(bot_server, last_turn):
    text = bot_server._format_last_result_detail("첫번째 거 자세히", last_turn)
    assert "1번째 건" in text
    assert "김덕기 과장 교육" in text
    assert "현장 요청사항" not in text


def test_worker_status_defaults_to_leader_brief(bot_server, mixed_posts):
    text = bot_server._format_worker_status(tools.get_worker_status("황유경"))
    assert "결론:" in text
    assert "## 지금 볼 업무" in text
    assert "## 다음에 바로 물어볼 질문" in text
    assert "진행률" not in text


def test_team_status_defaults_to_leader_brief(bot_server, posts):
    text = bot_server._format_team_status(tools.get_team_status())
    assert "## 결론" in text
    assert "## 팀원별 한눈에 보기" in text
    assert "## 다음에 바로 물어볼 질문" in text
    assert "주요 프로젝트" not in text


def test_detail_request_keeps_project_breakdown(bot_server, posts):
    text = bot_server._format_team_status(tools.get_team_status(), detailed=True)
    assert "주요 프로젝트" in text


def test_last_result_cause_uses_evidence_without_asserting_fact(bot_server, last_turn, monkeypatch):
    monkeypatch.setattr(
        tools,
        "get_post_thread",
        lambda post_id: {
            "posts": [
                {"post_id": post_id, "task_status": "진행", "title": "김덕기 과장 교육"},
                {"post_id": "c1", "task_status": "진행", "title": "교육일정 수립"},
            ],
            "comments": [
                {
                    "is_system": False,
                    "author_name": "조민준",
                    "written_at": "20260820110000",
                    "content_text": "매주 월요일에 있는 인원과 협의해서 교육일정 확정하겠습니다.",
                }
            ],
        },
    )
    text = bot_server._format_last_result_detail("기한이 있는 원인이 뭐야?", last_turn)
    assert "## 원인 확인" in text
    assert "확인된 근거" in text
    assert "추정되는 병목" in text
    assert "확인 질문" in text
    assert "근거 기반 추정" in text


def test_history_context_adds_worker_to_filter_route(monkeypatch):
    monkeypatch.setattr(router, "route_with_llm", lambda question, history=None: router.LlmRouting(ok=False))
    resolution = router.resolve_route(
        "그 중에 기한 지난 건만 다시 보여줘",
        history=[
            {"role": "user", "text": "황유경 실장의 진행상황을 알려줘"},
            {"role": "assistant", "text": "## 황유경 진행상황"},
        ],
    )
    assert resolution.route.tool == "filter_posts"
    assert resolution.route.arguments["worker"] == "황유경"
    assert resolution.route.arguments["due"] == "overdue"


# --- 데이터 신선도 ---


@pytest.fixture
def mixed_freshness_posts(monkeypatch):
    """답변 대상 3개는 오래됐고, 답변하지 않는 프로젝트만 최신인 상황."""
    df = pd.DataFrame(
        [
            _post(post_id="a1", project_id="2926716", collected_at="2026-08-26T13:26:00+09:00"),
            _post(post_id="a2", project_id="2926713", collected_at="2026-08-28T10:15:00+09:00"),
            # 봇이 답하지 않는 프로젝트 — 여기에 속아 "오늘 데이터"라고 표시하던 버그
            _post(post_id="x1", project_id="2499984", collected_at="2026-09-02T16:38:00+09:00"),
        ]
    )
    monkeypatch.setattr(tools, "_load_posts", lambda: df.copy())
    return df


def test_data_freshness_only_looks_at_answered_projects(mixed_freshness_posts):
    freshness = tools.data_freshness()
    assert freshness["latest"].startswith("2026-08-28")     # 09-02 가 아니다
    assert freshness["oldest"].startswith("2026-08-26")
    assert freshness["stale"] is True
    assert freshness["age_days"] >= 1


def test_stale_note_warns(bot_server, mixed_freshness_posts):
    note = bot_server._data_as_of_note()
    assert "⚠" in note
    assert "2026-08-28" in note


def test_fresh_note_has_no_warning(monkeypatch, bot_server):
    from datetime import datetime, timedelta

    just_now = (datetime.now().astimezone() - timedelta(minutes=5)).isoformat()
    df = pd.DataFrame([_post(post_id="a1", project_id="2926716", collected_at=just_now)])
    monkeypatch.setattr(tools, "_load_posts", lambda: df.copy())

    freshness = tools.data_freshness()
    assert freshness["stale"] is False
    assert "⚠" not in bot_server._data_as_of_note()
