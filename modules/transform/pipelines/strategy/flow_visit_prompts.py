"""Flow 방문일지 GPT-OSS 프롬프트."""

from __future__ import annotations

import json
import re
from typing import Any

GPT_OSS_MODELS = ["gpt-oss:20b", "gpt-oss:latest", "gpt-oss"]
PROMPT_VERSION = "flow_visit_v16_profile_brief_copy"
# 이슈 분류/요약 프롬프트 전용 버전. 이슈 단위 LLM 캐시 키에 들어가므로
# build_issue_prompt / build_summary_prompt를 실제로 바꿀 때만 올린다.
# PROMPT_VERSION과 같이 올리면 전체 이슈 분류 캐시가 날아가 재분류가 강제되고,
# LLM_MAX_SEGMENTS 상한에 걸려 fallback 비율 가드가 터진다.
ISSUE_PROMPT_VERSION = "flow_visit_v17_topic_anchored"
# 요약 결과 최대 길이. 표의 "핵심 요약" 칸에 그대로 들어간다.
SUMMARY_MAX_CHARS = 60
# 다음 확인 사항 최대 길이. 표의 "남은 확인" 칸에 들어간다.
NEXT_ACTION_MAX_CHARS = 30

STORE_ISSUE_HINTS = {
    "동탄영천점": [
        "매출_홀부진",
        "고정비_부담",
        "매장이전_양도양수",
        "계육_순살품질",
        "계육_뼈닭내장",
        "인테리어_내부디자인",
        "판촉물_현수막배너",
        "메뉴_홀등록요청",
        "소스_용량표기",
        "용기_규격도입",
        "부자재_1인봉투",
        "사입_전용상품준수",
        "운영_홀배달동시한계",
        "발주_마켓봄전환",
        "발주_마감시한",
        "밑반찬_자율화",
        "메뉴판_설명물",
        "메뉴_중량과다",
        "리뷰이벤트_사리품목",
        "POS_전환",
    ],
    "용인동천점": [
        "매출_배달정체",
        "광고_우가클단가",
        "광고_즉시할인",
        "광고_한그릇하나만",
        "광고_쿠팡노출률",
        "상권_악화",
        "매장이전_양도양수",
        "수익_감소체감",
        "정책_가격인상",
        "정책_지원금종료",
        "묵은지_숙성경도",
        "우거지_질김",
        "메뉴_1인메뉴확대",
        "대창_외관",
        "파김치_소포장",
        "토더_설정공지",
    ],
}

STORE_GUIDES = {
    "동탄영천점": (
        "동탄영천점은 배달 매출 안정과 홀 매출 부진이 같이 나온다. "
        "월세/관리비/권리금은 고정비 또는 이전 이슈로 분리한다. "
        "계육은 순살 품질과 뼈닭 내장/손질을 분리한다."
    ),
    "용인동천점": (
        "용인동천점은 배달 매출 정체, 광고 효율, 상권 악화, 수익 체감이 핵심이다. "
        "우가클/즉시할인/한그릇/쿠팡은 광고 이슈로 보고, 재개발/공사는 상권 악화로 분리한다."
    ),
}

NEGATIVE_GUIDES = {
    "동탄영천점": [
        "홀 매출이 낮다는 내용을 단순 배달정체로 분류하지 않는다.",
        "순살 이취/중량 부족과 뼈닭 내장 제거 불량을 같은 issue_key로 합치지 않는다.",
    ],
    "용인동천점": [
        "매출이 낮다는 이유만으로 모든 광고 문단을 매출_배달정체로 합치지 않는다.",
        "재개발, 공사, 상권 표현이 있으면 광고 문제가 아니라 상권_악화를 우선 검토한다.",
    ],
}


def _as_text(value: Any) -> str:
    return "" if value is None else str(value)


def _short(value: Any, limit: int = 700) -> str:
    text = re.sub(r"\s+", " ", _as_text(value)).strip()
    return text[:limit]


def split_sentences(text: Any, max_items: int = 6) -> list[str]:
    value = re.sub(r"\s+", " ", _as_text(text)).strip()
    if not value:
        return []
    parts = re.split(r"(?<=[.!?。])\s+|ㄴ\s*|-\s*", value)
    rows = [_short(part.strip(" ."), 180) for part in parts if len(part.strip()) >= 4]
    return rows[:max_items]


# "5. ", "1. 매장현황 - ", "4. 담당자 의견 - " 같은 목차 번호와 제목 접두.
_HEADING_PREFIX_RE = re.compile(
    r"^\s*(?:\d{1,2}\s*[.)]\s*)+(?:[^\s\-:：][^\-:：]{0,14}?\s*[-:：]\s*)?"
)
# 내용이 사실상 비어 있는 세그먼트. 화두로 만들면 안 된다.
_EMPTY_CONTENT_RE = re.compile(
    r"^(?:[^가-힣]*)?(?:[가-힣\s]{0,12}?)?"
    r"(?:요청\s*사항|특이\s*사항|건의\s*사항|불편\s*사항|문의\s*사항|사항|내용|없음|해당\s*없음)"
    r"\s*[-:：]?\s*(?:없음|없습니다|없으심|무|N/?A|ALL)?\s*[.\s]*$",
    re.IGNORECASE,
)


def strip_heading_prefix(text: Any) -> str:
    """목차 번호와 제목 접두를 벗긴다. 전부 접두뿐이면 원문을 그대로 돌려준다."""
    value = re.sub(r"\s+", " ", _as_text(text)).strip()
    stripped = _HEADING_PREFIX_RE.sub("", value).strip(" -:：")
    return stripped or value


def is_empty_content(text: Any) -> bool:
    """'요청사항-없음', '특이사항 없음', '없음. ALL' 처럼 내용이 없는 문구인지."""
    value = re.sub(r"\s+", " ", _as_text(text)).strip()
    if not value:
        return True
    value = strip_heading_prefix(value)
    # 꼬리 채움말("없음. ALL", "없음 N/A")까지 털어내야 내용 없음으로 잡힌다.
    value = re.sub(r"(?:[.\s]|\bALL\b|\bN/?A\b)+$", "", value, flags=re.IGNORECASE).strip()
    if not value:
        return True
    if len(value) > 30:
        return False
    if re.fullmatch(r"(?:없음|없습니다|무|N/?A|ALL|-)+", value, re.IGNORECASE):
        return True
    return bool(_EMPTY_CONTENT_RE.match(value))


def select_issue_candidates(
    store_name: str,
    segment: dict[str, Any],
    taxonomy_issues: list[dict[str, Any]],
    comments: list[dict[str, Any]] | None = None,
    limit: int = 8,
) -> list[dict[str, Any]]:
    topic_text = _as_text(segment.get("topic"))
    action_text = _as_text(segment.get("sv_action_raw"))
    owner_text = _as_text(segment.get("owner_voice_raw"))
    raw_text = _as_text(segment.get("raw_text"))
    comment_text = " ".join(_as_text(row.get("content_text")) for row in comments or [])
    text = " ".join([topic_text, action_text, owner_text, raw_text, comment_text])
    compact_text = re.sub(r"\s+", "", text)
    by_key = {issue["key"]: issue for issue in taxonomy_issues}
    scored: list[tuple[int, int, dict[str, Any]]] = []
    store_hints = STORE_ISSUE_HINTS.get(store_name, [])
    for idx, issue in enumerate(taxonomy_issues):
        key = issue.get("key")
        if key == "기타":
            continue
        score = 0
        for alias in issue.get("aliases") or []:
            if not alias:
                continue
            if alias in topic_text:
                score += 60
            if alias in owner_text:
                score += 45
            if alias in action_text:
                score += 30
            if alias in raw_text:
                score += 25
            if comment_text and alias in comment_text:
                score += 8
        if score and key in store_hints:
            score += 10
        if score:
            scored.append((score, -idx, issue))
    scored.sort(reverse=True)
    selected = [row[2] for row in scored[:limit]]
    if not selected and len(compact_text) >= 40:
        selected = [by_key[key] for key in store_hints[: min(3, limit)] if key in by_key]
    if "기타" in by_key:
        selected = selected[:limit] + [by_key["기타"]]
    return selected


def build_issue_prompt(
    store_name: str,
    segment: dict[str, Any],
    issue_candidates: list[dict[str, Any]],
    comments: list[dict[str, Any]] | None = None,
) -> str:
    issue_lines = []
    for idx, issue in enumerate(issue_candidates, 1):
        aliases = ", ".join((issue.get("aliases") or [])[:6])
        issue_lines.append(f"{idx}. issue_key=\"{issue['key']}\" | category={issue.get('category')} | aliases={aliases}")
    comment_lines = [
        f"- {row.get('author_name')}: {_short(row.get('content_text'), 350)}"
        for row in comments or []
        if _as_text(row.get("content_text")).strip()
    ]
    negatives = "\n".join(f"- {line}" for line in NEGATIVE_GUIDES.get(store_name, [])) or "- 없음"
    evidence = _short(
        " ".join([
            _as_text(segment.get("topic")),
            _as_text(segment.get("sv_action_raw")),
            _as_text(segment.get("owner_voice_raw") or segment.get("raw_text")),
        ]),
        1200,
    )
    return f"""출력 첫 글자는 반드시 {{ 이어야 한다. 분석문/설명/마크다운 금지. JSON 객체 1개만 출력.
Do not explain. Do not reason. Start with {{ and end with }}.

[매장 맞춤 힌트]
{STORE_GUIDES.get(store_name, "매장별 힌트 없음")}

[오분류 금지]
{negatives}
- 발췌가 제목/목차 수준(예: "홀관련", "매장현황", "계육")이고 구체 불만/요청/조치/수치가 없으면 issue_key는 반드시 "기타"다.
- issue_key는 아래 후보 중 원문 발췌에 직접 근거가 있는 값만 고른다. 매장 힌트만으로 고르지 않는다.
- 본사 댓글은 같은 글의 보조 근거일 뿐이며, 발췌와 연결되는 표현이 없으면 분류 근거로 쓰지 않는다.

[발췌]
매장: {store_name}
{evidence}

[본사 댓글]
{chr(10).join(comment_lines) if comment_lines else "없음"}

[issue_key - 반드시 issue_key 따옴표 안 값만 그대로 복사. 번호는 복사 금지]
{chr(10).join(issue_lines)}

[status] 해결=조치완료, 진행중=검토/확인/예정, 안내완료=불가/어려움/현행유지 안내, 미해결=답변근거 없음
[severity] 높음=안전/법적/매출 직접 타격, 보통=운영/품질/광고 개선, 낮음=단순 문의

[출력 JSON 스키마]
{{"issue_key":"", "severity":"보통", "status":"미해결", "is_request":false}}
"""


def build_summary_prompt(store_name: str, segment: dict[str, Any]) -> str:
    """세그먼트를 점주 요지 / 담당자 조치 / 이슈명 짧은 요약으로 만드는 프롬프트.

    이전 버전은 문장 번호를 고르게 했으나, 한국어 방문일지는 마침표가 거의 없어
    split_sentences가 문단을 못 쪼갠다. 그 결과 "첫 문장"이 문단 전체(180자)가 되어
    요약이라는 이름과 달리 원문이 그대로 실렸다. 그래서 직접 쓰게 바꾼다.
    """
    owner_text = _short(segment.get("owner_voice_raw") or segment.get("raw_text"), 900) or "없음"
    sv_text = _short(segment.get("sv_action_raw"), 900) or "없음"
    topic = _short(segment.get("topic"), 60) or "없음"
    return f"""Return exactly one minified JSON object. No prose. No reasoning. First char must be {{.
Fill this schema only: {{"owner_summary":"","sv_summary":"","issue_label":"","is_concern":false,"next_action":""}}

store={store_name}
topic={topic}

[점주 발언 원문]
{owner_text}

[담당자 조치 원문]
{sv_text}

rules:
- 모든 값은 한국어다.
- **모든 값은 위 topic에 대한 것이어야 한다.** 원문 한 문단에 다른 주제가 섞여 있어도
  topic과 무관한 내용은 쓰지 않는다. topic이 "토더 공지"인데 순이익 이야기를 쓰면 안 된다.
- owner_summary: 점주 발언의 요지를 {SUMMARY_MAX_CHARS}자 이내 한 문장으로 쓴다. 원문이 "없음"이면 "".
- sv_summary: 담당자가 실제로 한 조치를 {SUMMARY_MAX_CHARS}자 이내 한 문장으로 쓴다. 원문이 "없음"이면 "".
- issue_label: 12자 이내 한국어 명사구. 문장 종결어미를 쓰지 않는다.
- 원문에 없는 사실과 숫자를 만들지 않는다. 금액·비율·기간은 원문에 있는 값만 그대로 쓴다.
- 목차 번호와 제목을 그대로 옮기지 않는다. "1. 매장현황", "5. 점주님 요청사항-없음" 같은 형태 금지.
- 내용이 "없음", "특이사항 없음", "해당없음"뿐이면 세 값을 모두 ""로 둔다.
- 원문 문장을 통째로 복사하지 말고 요지만 남긴다.
- is_concern: 점주가 직접 표현한 불편·불만·우려·부담·걱정이면 true, 아니면 false.
  판단 기준은 "점주가 곤란해하는가"이지 "본사가 할 일이 있는가"가 아니다.
  true 예 - 품질이 나쁘다, 매출·수익이 걱정된다, 비용이 부담된다, 불편해서 개선해달라고 불만을 말했다.
  false 예 - 신메뉴 추가처럼 바라는 것을 말한 요청·희망·관심,
  본사가 먼저 꺼낸 안내·교육·재공지·점검 같은 내부 과제,
  단순 현황 보고, 매출이 오르고 있다는 긍정 서술, 점주가 이미 정한 운영 계획,
  숙지 완료처럼 이미 정리된 사안, 요청사항 없음.
  담당자 조치가 "재안내", "교육", "공지", "점검"으로 시작하는 건은 본사가 먼저 꺼낸 것이므로,
  점주가 그 일로 불편·불만을 말하지 않는 한 false다.
  점주가 무언가를 "제대로 안 한다"는 서술은 본사 관점의 지적이지 점주의 고민이 아니므로 false다.
- next_action: 담당자가 다음에 확인·회신·안내해야 할 일을 {NEXT_ACTION_MAX_CHARS}자 이내 한 줄로 쓴다.
  **반드시 topic과 owner_summary에 대한 일이어야 한다.** 원문에 섞인 다른 주제의 할 일을 쓰지 않는다.
  원문에 근거가 없으면 "". 이슈명을 되풀이하는 빈 문구를 만들지 않는다.
  금지 예 - "순이익확인 처리 상태 확인", "신메뉴 도입 처리 상태 확인", "OO 확인".
  좋은 예 - "광고세팅 동일 조건 순이익 데이터 회신", "닭발·찜닭 추가 가능 여부 확인".
"""


def build_profile_prompt(history_digest: dict[str, Any]) -> tuple[str, str]:
    """매장별 누적 히스토리 digest를 점주 이해 JSON으로 합성하는 프롬프트."""
    if isinstance(history_digest, dict) and "local_candidates" in history_digest:
        system_prompt = (
            "너는 프랜차이즈 SV 방문일지에서 점주 이해 정보를 정리하는 분석가다. "
            "원문 근거 안에서 자연스럽게 요약하되, 출력은 JSON 객체만 반환한다."
        )
        prompt = (
            "JSON만 출력. 설명 금지.\n"
            "의도: 담당자가 인수인계 없이도 현재 점주 상태, 최근 화두, 현재 고민, 다음 대응을 바로 이해하게 만든다. "
            "사람이 별도 작성한 평가가 아니라 방문 히스토리에서 자동으로 뽑는다.\n"
            "스키마={"
            "\"owner_status\":\"현재 점주 상태 문장\","
            "\"store_status_summary\":[\"전기간 누적 특징·성향 최대3\"],"
            "\"key_concerns\":[\"최근 방문 화두 최대10\"],"
            "\"handling_points\":[\"key_concerns 중 현재 문제·고민인 문장만 그대로\"],"
            "\"handover_summary\":\"- 다음 방문 전 핵심 참고사항 최대3개 불릿\","
            "\"next_visit_action\":[\"다음 방문 실행 행동 최대2\"],"
            "\"analysis_evidence\":[{\"concern\":\"\",\"issue_key\":\"\",\"issue_label\":\"\",\"visit_date\":\"\",\"post_id\":\"\",\"problem_detail\":\"\",\"owner_voice\":\"\",\"sv_action\":\"\",\"status\":\"\",\"evidence\":\"\",\"is_problem\":false}],"
            "\"todos\":[]"
            "}\n"
            "기준: owner_status는 지금 이 가맹점의 현재 상태, store_status_summary는 여러 번 만나보니 보이는 반복 성향, "
            "key_concerns는 이번 방문에서 실제로 대화한 화두, handling_points는 그 화두 중 점주가 곤란해하고 담당자가 해결해야 하는 현재 문제·고민, "
            "handover_summary는 다음 방문 전에 담당자가 준비하거나 기억해야 할 핵심 사항, next_visit_action은 다음 방문에서 실제로 할 행동이다. "
            "todos는 미처리 할 일이다. "
            "반복 횟수는 보조 근거일 뿐 최종 성향이 아니다. 간담회 참석, 정책 안내, 신메뉴 단순 안내처럼 점주 반응이 없는 활동은 민감 포인트로 쓰지 않는다. "
            "민감 포인트는 recurring의 cnt만 보지 말고 issue_label, category, is_problem, problem_detail, owner_voice, evidence를 함께 보고 판단한다. "
            "비용 부담, 품질 문제 제기, 수익·매출 변화 확인, 거부감, 동일 문제의 처리 결과 요구처럼 점주 반응이 반복될 때만 민감 포인트 후보로 본다. "
            "store_status_summary는 유사 성향을 합쳐 가장 중요한 3개만 짧게 쓴다. 판단 방식, 행동 특성, 관심 성향을 MECE하게 나누고 '광고 7회', '간담회 7회', '반복 성향:' 같은 집계 표현은 쓰지 않는다. "
            "handover_summary는 문자열 하나로 쓰되 각 줄을 '- '로 시작하는 짧은 불릿 최대 3개만 쓴다. "
            "성향·화두·문제를 그대로 복사하지 말고 후속 확인, 자료 준비, 운영 재확인을 MECE하게 쓴다. "
            "next_visit_action은 확인한다/비교한다/설명한다/재시연한다/자료를 준비한다/회신한다 같은 짧은 실행문 최대 2개만 쓴다. "
            "analysis_evidence는 key_concerns와 1:1로 대응하는 상세 근거이며 행 수와 순서가 key_concerns와 같아야 한다. "
            "handling_points는 key_concerns에 있는 문장만 그대로 골라 담는다. 새 문장을 만들지 않는다. "
            "미해결·진행중이어도 단순 관심·희망·본사 내부 과제는 문제·고민이 아니다. 점주 불편·부담·우려·손해가 근거로 보이는 화두만 is_problem=true로 둔다. "
            "점수·등급·N/10 표현은 쓰지 말고, 근거가 약하면 단정하지 않는다. "
            "후보는 참고 자료이며 같은 근거 안에서 업무적으로 읽히는 표현으로 다듬어도 된다.\n"
            "데이터="
            + json.dumps(history_digest, ensure_ascii=False)
        )
        return prompt, system_prompt

    schema = {
        "owner_status": "",
        "store_status_summary": [],
        "key_concerns": [],
        "handling_points": [],
        "handover_summary": "",
        "next_visit_action": [],
        "analysis_evidence": [],
        "todos": [
            {
                "post_id": "",
                "issue_key": "",
                "issue_seq": None,
                "todo_list": "",
                "todo_due_date": None,
                "todo_owner": None,
                "todo_status": "대기",
                "analysis_evidence": [
                    {
                        "concern": "",
                        "issue_key": "",
                        "issue_label": "",
                        "visit_date": "",
                        "post_id": "",
                        "problem_detail": "",
                        "owner_voice": "",
                        "sv_action": "",
                        "status": "",
                        "evidence": "",
                        "is_problem": False,
                    }
                ],
            }
        ],
    }
    system_prompt = (
        "방문일지 누적 기록에서 점주 이해 정보와 할 일을 정리하는 분석가다. "
        "원문 근거 안에서 자연스럽게 요약하되 JSON 객체만 반환한다."
    )
    prompt = f"""출력은 JSON 객체 1개만 반환한다. 첫 글자는 반드시 {{ 이어야 한다.

[핵심 원칙]
- owner_status: 최근 기준 점주의 현재 인식·태도 상태. 점수, 등급 숫자, N/10 표현 금지.
- store_status_summary: 전체 누적 히스토리에서 반복 확인되는 판단 방식·행동 특성·관심 성향. 유사 성향을 통합해 최대 3개만 쓴다. 1회성 이슈 목록 금지.
- key_concerns: 가장 최근 방문 1건에서 실제 대화한 화두 최대 10개.
- handling_points: key_concerns 중 점주가 곤란해하고 담당자가 해결·대응해야 하는 현재 문제·고민만 그대로 골라 담는다. 단순 관심·희망·본사 내부 과제는 제외한다.
- handover_summary: 다음 방문 전 준비·기억 사항만 '- ' 짧은 불릿 최대 3개. 성향·화두·문제 반복 금지.
- next_visit_action: 다음 방문 시 확인·비교·설명·설정·자료 준비 등 실행 단위 행동 최대 2개.
- todos: 명시 요청 또는 명확한 후속 약속이 있는 경우만 생성한다. 단순 고민만으로 만들지 않는다.
- todo_due_date는 원천에 명시된 기한만 YYYY-MM-DD로 쓰고, 없으면 null.
- todo_owner는 근거가 없으면 "담당자 미정".
- analysis_evidence는 key_concerns와 1:1로 대응한다. 행 수와 순서가 key_concerns와 같아야 하며 concern, issue_key, issue_label, visit_date, post_id, problem_detail, owner_voice, sv_action, status, evidence, is_problem을 남긴다.
- is_problem은 그 화두가 현재 문제·고민이면 true다. handling_points는 is_problem이 true인 행의 concern과 같아야 한다.

[해석 가이드]
- 담당자가 인수인계 없이 방문 전에 이해할 수 있게 자연스러운 업무 문장으로 쓴다.
- 현재 상태와 최근 화두는 최신 방문을 우선하고, 전기간 누적 성향은 반복 히스토리를 본다.
- 반복 횟수는 근거일 뿐 결론이 아니다. 같은 이슈가 여러 번 나와도 점주 반응·문제 여부·근거 문장이 없으면 성향으로 쓰지 않는다.
- 민감 포인트 판단은 issue_label, category, is_problem, problem_detail, owner_voice, evidence를 함께 본다. cnt가 높아도 문제 맥락이나 점주 반응이 없으면 제외한다.
- 비용 부담, 품질 문제 제기, 수익·매출 변화 확인, 특정 정책·광고 거부감, 동일 문제 처리 결과 요구, 특정 운영 방식 우려가 반복될 때만 민감 포인트 후보로 삼는다.
- 간담회 참석, 정책 안내, 신메뉴 단순 안내, 매장현황처럼 활동이나 사건이 반복된 것만으로 민감 포인트라고 쓰지 않는다.
- store_status_summary에는 이슈명과 횟수를 나열하지 말고 "이 가맹점은 무엇을 중요하게 보고 어떻게 판단하는가"를 업무 문장으로 해석한다. 수익성, 광고 효과, 품질, 판매 확대, 실행 성향 등 의미 축별로 비슷한 문장은 하나로 합친다.
- handover_summary에는 "반복 성향:", "주의 화두:", "기존 대응:" 같은 라벨을 쓰지 않는다. 이미 다른 컬럼에 있는 성향·화두·문제를 그대로 반복하지 않는다.
- handover_summary는 이전 방문 후속 확인사항, 준비해야 할 자료, 재확인이 필요한 운영사항을 MECE하게 나눈다. 각 항목은 짧은 1줄 불릿이며 최대 3개다.
- next_visit_action은 "광고", "확인 필요" 같은 키워드가 아니라 담당자가 실제로 할 짧은 행동 문장으로 쓴다.
- owner_status는 "지금 어떤 상태인가", store_status_summary는 "여러 번 만나보니 어떤 특징인가", key_concerns는 "이번 방문에서 무슨 이야기를 했는가", handling_points는 "그중 지금 해결해야 할 문제는 무엇인가", handover_summary는 "다른 담당자가 무엇을 알아야 하는가", next_visit_action은 "그래서 다음 방문에서 무엇을 해야 하는가"에 답한다.
- 같은 원문 조각에 여러 문제가 겹치면 대표 1개로 뭉치지 말고 이슈별로 분리해 해석한다.
- handling_points는 화두 문구를 그대로 쓴다. 액션문으로 바꾸지 않는다. 다음 방문 액션은 next_visit_action에만 쓴다.
- 미해결·진행중 상태만으로 문제·고민으로 보지 않는다. 점주의 불편·부담·우려·손해 근거가 있어야 한다.
- todos는 과거~현재 남아 있는 미처리 할 일이며, 요청 원문을 반복하지 말고 실행문으로 쓴다.
- 원문에 없는 사실은 만들지 않되, 같은 근거 안에서 의미가 분명하면 표현을 다듬어도 된다.

[출력 스키마 예시]
{json.dumps(schema, ensure_ascii=False)}

[히스토리 digest]
{json.dumps(history_digest, ensure_ascii=False)}
"""
    return prompt, system_prompt


def assemble_summary(sentences: list[str], picks: Any) -> str:
    if not isinstance(picks, list):
        return ""
    selected: list[str] = []
    for pick in picks[:2]:
        try:
            idx = int(pick) - 1
        except Exception:
            continue
        if 0 <= idx < len(sentences):
            selected.append(sentences[idx])
    if not selected and sentences:
        selected.append(sentences[0])
    return " / ".join(selected)


def normalize_summary(value: Any, limit: int = SUMMARY_MAX_CHARS) -> str:
    """LLM이 쓴 요약을 표시용으로 정리한다. 값이 없거나 내용 없음이면 빈 문자열."""
    text = re.sub(r"\s+", " ", _as_text(value)).strip().strip(" .,-/")
    text = strip_heading_prefix(text)
    if not text or is_empty_content(text):
        return ""
    return text[:limit].strip()


def normalize_label(value: Any, fallback: str) -> str:
    label = re.sub(r"\s+", " ", _as_text(value)).strip().strip(".")
    if not label or len(label) > 20:
        return fallback
    if re.search(r"(합니다|했습니다|된다|됩니다|라고|다고|이다|입니다)$", label):
        return fallback
    return label[:20]
