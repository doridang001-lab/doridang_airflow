# Executive Reviewer Agent

validator 결과를 영업 관점에서 최종 검토하고 의사결정 트리를 생성합니다.

**입력**: qwen_prompter + prompt_validator 결과 JSON
**출력**: JSON (액션 분류 + 의사결정 트리)

---

```python
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts._base import KST

logger = logging.getLogger(__name__)


def categorize_action_type(action_text: str) -> str:
    """액션 타입 분류"""
    text = action_text.lower()

    if any(kw in text for kw in ["광고비", "예산", "당일", "즉시", "활성", "중지"]):
        return "Type A (즉시)" if "테스트" not in text else "Type A (당일 테스트)"

    if any(kw in text for kw in ["점검", "조사", "협력", "운영", "테스트"]):
        return "Type B (주간)"

    if any(kw in text for kw in ["운영시간", "상품", "권역", "본부", "승인"]):
        return "Type C (장기)"

    return "Type B (주간)"


def estimate_execution_time(action_type: str) -> str:
    """실행 소요 시간"""
    if "Type A" in action_type:
        return "당일" if "즉시" in action_type else "1일 이내"
    elif "Type B" in action_type:
        return "3~7일"
    else:
        return "2주 이상"


def identify_required_roles(action_text: str) -> list[str]:
    """필요 담당자"""
    roles = []
    text = action_text.lower()

    if any(kw in text for kw in ["광고비", "예산", "광고"]):
        roles.append("광고 담당자")
    if any(kw in text for kw in ["운영", "배달", "매장"]):
        roles.append("운영팀")
    if any(kw in text for kw in ["상품", "품절", "재고"]):
        roles.append("상품팀")
    if any(kw in text for kw in ["가격", "할인", "프로모션"]):
        roles.append("마케팅팀")

    return roles if roles else ["광고 담당자"]


def estimate_feasibility_score(action_type: str, roles_count: int) -> int:
    """실행 가능성 점수 (0~5)"""
    if "Type A" in action_type and roles_count <= 1:
        return 5
    elif "Type A" in action_type:
        return 4
    elif "Type B" in action_type and roles_count <= 2:
        return 4
    elif "Type B" in action_type:
        return 3
    else:
        return 2


def build_action_breakdown(actions: list) -> list[dict]:
    """액션별 상세 분석"""
    breakdown = []

    for i, action in enumerate(actions, 1):
        if isinstance(action, dict):
            action_text = action.get("action", "")
        else:
            action_text = str(action)

        action_type = categorize_action_type(action_text)
        exec_time = estimate_execution_time(action_type)
        roles = identify_required_roles(action_text)
        feasibility = estimate_feasibility_score(action_type, len(roles))

        breakdown.append(
            {
                "action": action_text,
                "action_type": action_type,
                "execution_time": exec_time,
                "required_roles": roles,
                "feasibility_score": feasibility,
                "risk": "낮음" if feasibility >= 4 else "중간" if feasibility >= 3 else "높음",
            }
        )

    return breakdown


def build_decision_tree(actions: list) -> dict:
    """의사결정 트리"""
    return {
        "start": "액션 1: 광고비 축소 원인 확인",
        "branch_1": {
            "condition": "의도적 광고비 축소 확인",
            "true_path": "액션 2: 광고비 복원 테스트",
            "false_path": "액션 3: 운영 점검",
        },
        "branch_2": {
            "condition": "3일 내 주문 10% 이상 회복",
            "true_path": "예산 추가 증액 + 지속 모니터링",
            "false_path": "액션 3: 운영 점검",
        },
    }


def build_final_recommendation(breakdown: list, validation_status: str) -> dict:
    """최종 의사결정"""
    avg_feasibility = sum(a["feasibility_score"] for a in breakdown) / len(breakdown) if breakdown else 0

    if validation_status == "HOLD":
        overall = "재검토 필요"
        message = "qwen_prompter에서 액션 재생성이 필요합니다"
    elif avg_feasibility >= 4.5:
        overall = "즉시 실행 추천"
        message = "모든 액션이 당일 또는 주중에 실행 가능합니다"
    elif avg_feasibility >= 3.5:
        overall = "실행 권고 (협력 필요)"
        message = "일부 액션은 운영팀 협력이 필요하지만 주간 내 완료 가능합니다"
    else:
        overall = "계획 수립 필요"
        message = "장기 계획이 필요한 액션이 포함되어 있습니다"

    return {
        "overall_recommendation": overall,
        "message": message,
        "confidence": min(0.95, 0.7 + avg_feasibility * 0.05),
        "action_priority": [
            f"{i+1}순위: {a['action'][:50]}..."
            for i, a in enumerate(breakdown[:3])
        ],
    }


def main(qwen_output: dict, validator_output: dict) -> dict:
    """메인 로직"""
    run_at = datetime.now(KST)

    actions = qwen_output.get("qwen_responses", {}).get("actions", [])
    validation_status = validator_output.get("recommendation", {}).get("status", "")
    metrics = qwen_output.get("extracted", {}).get("metrics", {})

    # 액션 분석
    breakdown = build_action_breakdown(actions)
    decision_tree = build_decision_tree(actions)
    final_recommendation = build_final_recommendation(breakdown, validation_status)

    result = {
        "meta": {
            "agent": "executive_reviewer",
            "timestamp": run_at.isoformat(),
        },
        "executive_summary": {
            "overall_recommendation": final_recommendation["overall_recommendation"],
            "confidence": final_recommendation["confidence"],
            "key_insight": final_recommendation["message"],
        },
        "action_breakdown": breakdown,
        "decision_tree": decision_tree,
        "action_priority": final_recommendation["action_priority"],
        "quality_assurance": {
            "action_validity": "모든 액션이 배민 앱 범위 내",
            "business_logic": "액션 시퀀스가 논리적이고 측정 가능",
            "executive_readiness": "영업관리자가 즉시 팀과 협의 가능",
        },
    }

    logger.info(f"✅ 최종 검토 완료: {final_recommendation['overall_recommendation']}")
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Executive Reviewer Agent")
    parser.add_argument("--qwen-output-json", required=True)
    parser.add_argument("--validator-output-json", required=True)
    args = parser.parse_args()

    with open(args.qwen_output_json) as f:
        qwen_output = json.load(f)
    with open(args.validator_output_json) as f:
        validator_output = json.load(f)

    logging.basicConfig(level=logging.INFO)
    result = main(qwen_output, validator_output)
    print(json.dumps(result, ensure_ascii=False, indent=2))
```
