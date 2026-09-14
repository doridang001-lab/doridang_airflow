"""Flow 게시글 업무 상태(task_status) 분류.

`task_status` 는 Flow 사용자가 프로젝트마다 직접 만드는 자유 라벨이다.
수집단(`SMP_flow_store_collect.py`)이 `COLUMN_TYPE=='STATUS'` 의 `OPTION_NAME` 을 그대로 가져오므로
코드 매핑(요청/진행/완료/보류 4종)으로는 실제 값을 다 덮지 못한다.

허용 프로젝트 실측(2026-09-02, 243건):
    완료 99 / 진행 68 / 대기 41 / 업무단위 10 / 회의록 9 / 보류 5 / 모니터링 4 / 액션 4 / 피드백 2 / 보완 1

이 중 `업무단위`(자식 평균 4.3개를 가진 묶음), `회의록`(자식·기한 없는 기록),
`액션`(자식을 가진 계층 중간 노드)은 업무가 아니다. 업무 집계에 넣으면
자식 업무가 이중 집계되고, 진행상황 목록이 기록으로 채워진다.

라벨이 자유 입력이므로 **모르는 값은 열린 업무로 본다.** 비업무로 처리하면
새로 만들어진 상태가 리더 시야에서 조용히 사라진다.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# 업무가 아닌 것 — 묶음(부모)이거나 기록. 집계에서도 기한 감시에서도 뺀다.
NON_TASK_STATUS = frozenset({"업무단위", "회의록", "액션"})

# 진행 업무 집계에서는 빼지만 기한은 계속 본다.
# 남에게 맡겨 지켜보는 항목이라 "진행 중"으로 세면 안 되지만,
# 기한이 있는 실제 항목이라 완전히 버리면 기한을 넘겨도 리더 눈에 안 띈다.
WATCH_ONLY_STATUS = frozenset({"모니터링"})

# 진행 업무 집계에서 빠지는 전체
EXCLUDED_FROM_TASKS = NON_TASK_STATUS | WATCH_ONLY_STATUS

# 종료된 업무
DONE_STATUS = frozenset({"완료"})

# 아직 열려 있는 업무
OPEN_STATUS = frozenset({"진행", "대기", "보류", "피드백", "보완"})

KNOWN_STATUS = EXCLUDED_FROM_TASKS | DONE_STATUS | OPEN_STATUS

# 리더가 먼저 봐야 하는 순서. 낮을수록 급하다.
# 모니터링은 여기 없다 — 기한을 넘겼을 때만 위험 목록에 들어온다.
RISK_RANK = {
    "피드백": 0,
    "보류": 1,
    "보완": 2,
    "대기": 3,
}
UNRANKED = 9

# 리더가 즉시 확인해야 하는 상태 (get_risk_status(priority_only=True))
PRIORITY_STATUS = frozenset({"피드백", "보완"})

# 기한 없는 글이 오름차순 정렬에서 맨 앞으로 오는 것을 막는다.
NO_DUE_SORT_KEY = "99999999"

# 질문에서 상태 단어를 찾을 때 쓰는 순서. 긴 라벨을 먼저 봐야 부분일치로 잘못 잡히지 않는다.
# set은 순회 순서가 고정되지 않으므로 별도 튜플로 둔다.
DETECTION_ORDER = (
    "업무단위",
    "모니터링",
    "회의록",
    "피드백",
    "보완",
    "완료",
    "진행",
    "대기",
    "보류",
    "액션",
)


def normalize(status: object) -> str:
    return str(status or "").strip()


def is_task(status: object) -> bool:
    """진행 업무 집계 대상인가. 모르는 라벨은 업무로 본다 — 새 상태가 조용히 사라지면 안 된다."""
    return normalize(status) not in EXCLUDED_FROM_TASKS


def is_done(status: object) -> bool:
    return normalize(status) in DONE_STATUS


def is_open(status: object) -> bool:
    """아직 열려 있는 업무인가. 집계 제외 대상도 완료도 아니면 열린 업무다."""
    value = normalize(status)
    return value not in EXCLUDED_FROM_TASKS and value not in DONE_STATUS


def tracks_due(status: object) -> bool:
    """기한을 감시할 대상인가. 모니터링은 집계에서 빠지지만 기한은 본다."""
    value = normalize(status)
    return value not in NON_TASK_STATUS and value not in DONE_STATUS


def is_priority(status: object) -> bool:
    return normalize(status) in PRIORITY_STATUS


def risk_rank(status: object) -> int:
    return RISK_RANK.get(normalize(status), UNRANKED)


def unknown_statuses(statuses: object) -> set[str]:
    """수집 데이터에 새로 생긴 라벨. 분류 갱신이 필요한지 보려고 쓴다."""
    if statuses is None:
        return set()
    # pandas Series / numpy array는 진리값 판정이 안 되므로 직접 순회한다
    found = {normalize(value) for value in statuses}
    unknown = {value for value in found if value and value not in KNOWN_STATUS}
    if unknown:
        logger.info("분류에 없는 task_status: %s", sorted(unknown))
    return unknown
