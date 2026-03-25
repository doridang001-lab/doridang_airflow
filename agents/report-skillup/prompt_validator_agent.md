# Prompt Validator Agent

qwen_prompter 결과를 검증하고 품질을 평가합니다.

**입력**: qwen_prompter 결과 JSON
**출력**: JSON (검증 점수 + 권고)

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


def validate_judgment(judgment: str) -> dict:
    """판단 항목 검증"""
    score = {
        "clarity": 0,
        "relevance": 0,
        "issues": [],
    }

    if not judgment or len(judgment) == 0:
        score["issues"].append("판단 텍스트가 비어있음")
        return score

    # 길이 검증
    if len(judgment) > 50:
        score["clarity"] = 3
        score["issues"].append(f"너무 길음 ({len(judgment)}자)")
    elif len(judgment) > 30:
        score["clarity"] = 4
    else:
        score["clarity"] = 5

    # 한글 검증
    if any(ord(c) > 127 for c in judgment) or "효율" in judgment or "부족" in judgment:
        score["relevance"] = 5
    else:
        score["relevance"] = 3
        score["issues"].append("한글 키워드 부족")

    return score


def validate_actions(actions: list) -> dict:
    """액션 항목 검증"""
    score = {
        "count": len(actions),
        "executability": [],
        "specificity": [],
        "issues": [],
    }

    if not actions:
        score["issues"].append("액션이 없음")
        return score

    for i, action in enumerate(actions):
        if isinstance(action, dict):
            action_text = action.get("action", "")
        else:
            action_text = str(action)

        # 실행 가능성
        exec_score = _evaluate_executability(action_text)
        score["executability"].append(exec_score)

        # 구체성
        spec_score = _evaluate_specificity(action_text)
        score["specificity"].append(spec_score)

    avg_exec = sum(score["executability"]) / len(score["executability"])
    if avg_exec < 3:
        score["issues"].append("실행 불가능한 액션 포함")

    return score


def _evaluate_executability(action_text: str) -> int:
    """실행 가능성 (0~5)"""
    text = action_text.lower()

    # Type A: 즉시 실행
    immediate_keywords = ["광고비", "예산", "활성", "중지", "타겟팅", "당일"]
    if any(kw in text for kw in immediate_keywords):
        return 5

    # Type B: 주중 실행
    weekly_keywords = ["테스트", "점검", "협력", "확인", "조사"]
    if any(kw in text for kw in weekly_keywords):
        return 4 if "주" not in text or "주중" in text else 3

    # Type C: 장기 과제
    long_keywords = ["운영시간", "상품", "권역", "본부", "승인"]
    if any(kw in text for kw in long_keywords):
        return 2

    return 3


def _evaluate_specificity(action_text: str) -> int:
    """구체성 (0~5)"""
    score = 3
    if any(c.isdigit() for c in action_text):
        score += 1
    if "%" in action_text or "배" in action_text or "x" in action_text:
        score += 1
    return min(5, score)


def validate_causes(causes: list) -> dict:
    """원인 분석 검증"""
    score = {
        "count": len(causes),
        "plausibility": [],
        "issues": [],
    }

    if not causes:
        score["issues"].append("원인이 없음")
        return score

    for cause in causes:
        if isinstance(cause, dict):
            cause_text = cause.get("cause", "")
        else:
            cause_text = str(cause)

        plaus = 5 if any(kw in cause_text for kw in ["광고비", "예산", "운영"]) else 3
        score["plausibility"].append(plaus)

    return score


def generate_recommendation(judgment_score: dict, action_score: dict) -> dict:
    """통합 권고"""
    avg_exec = (
        sum(action_score["executability"]) / len(action_score["executability"])
        if action_score["executability"]
        else 0
    )

    if avg_exec >= 4.5 and not action_score["issues"]:
        return {
            "status": "PASS",
            "message": "액션이 실행 가능하며 구체적입니다",
        }
    elif avg_exec >= 3.5:
        return {
            "status": "CAUTION",
            "message": "일부 액션은 협력이 필요합니다",
            "issues": action_score.get("issues", []),
        }
    else:
        return {
            "status": "HOLD",
            "message": "액션의 실행 가능성이 낮습니다",
            "issues": action_score.get("issues", []),
        }


def main(qwen_output: dict) -> dict:
    """메인 로직"""
    run_at = datetime.now(KST)
    qwen_responses = qwen_output.get("qwen_responses", {})

    validation = {
        "judgment": validate_judgment(qwen_responses.get("judgment", "")),
        "actions": validate_actions(qwen_responses.get("actions", [])),
        "causes": validate_causes(qwen_responses.get("causes", [])),
    }

    recommendation = generate_recommendation(validation["judgment"], validation["actions"])

    result = {
        "meta": {
            "agent": "prompt_validator",
            "timestamp": run_at.isoformat(),
        },
        "validation": validation,
        "recommendation": recommendation,
    }

    logger.info(f"✅ 검증 완료: {recommendation['status']}")
    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Prompt Validator Agent")
    parser.add_argument("--qwen-output-json", required=True)
    args = parser.parse_args()

    with open(args.qwen_output_json) as f:
        qwen_output = json.load(f)

    logging.basicConfig(level=logging.INFO)
    result = main(qwen_output)
    print(json.dumps(result, ensure_ascii=False, indent=2))
```
