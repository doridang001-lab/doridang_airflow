# Report Skillup Orchestrator

3개 에이전트(qwen_prompter, prompt_validator, executive_reviewer)를 순차 실행하고 결과를 병합합니다.

**입력**: PDF 경로
**출력**: JSON (통합 리포트)

---

```python
import json
import logging
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts._base import OUTPUT_DIR, KST

logger = logging.getLogger(__name__)


def run_agent(agent_name: str, agent_file: str, args: dict) -> tuple[str, dict]:
    """단일 에이전트 실행"""
    logger.info(f"▶️  {agent_name} 시작...")

    agent_path = Path(__file__).parent / agent_file
    cmd = ["python", "-c", f"import sys; sys.path.insert(0, str(Path('{agent_path}').parent.parent.parent)); exec(open('{agent_path}').read())"]

    # 간단한 버전: 직접 import 후 실행
    try:
        if agent_name == "qwen_prompter":
            from agents.report_skillup.qwen_prompter_agent import main as agent_main
            result = agent_main(args.get("pdf_path"))
        elif agent_name == "prompt_validator":
            from agents.report_skillup.prompt_validator_agent import main as agent_main
            with open(args.get("qwen_output_json")) as f:
                qwen_output = json.load(f)
            result = agent_main(qwen_output)
        elif agent_name == "executive_reviewer":
            from agents.report_skillup.executive_reviewer_agent import main as agent_main
            with open(args.get("qwen_output_json")) as f:
                qwen_output = json.load(f)
            with open(args.get("validator_output_json")) as f:
                validator_output = json.load(f)
            result = agent_main(qwen_output, validator_output)
        else:
            raise ValueError(f"Unknown agent: {agent_name}")

        logger.info(f"✅ {agent_name} 완료")
        return agent_name, result

    except Exception as e:
        logger.error(f"❌ {agent_name} 오류: {e}")
        raise


def merge_results(results: dict, pdf_path: str) -> dict:
    """결과 병합"""
    run_at = datetime.now(KST)

    merged = {
        "meta": {
            "orchestrator": "report-skillup",
            "timestamp": run_at.isoformat(),
            "pdf_source": pdf_path,
            "agents_executed": list(results.keys()),
        },
        "agents": results,
        "summary": {
            "qwen_judgment": results.get("qwen_prompter", {})
            .get("qwen_responses", {})
            .get("judgment", ""),
            "validator_status": results.get("prompt_validator", {})
            .get("recommendation", {})
            .get("status", ""),
            "executive_recommendation": results.get("executive_reviewer", {})
            .get("executive_summary", {})
            .get("overall_recommendation", ""),
        },
        "action_items": results.get("executive_reviewer", {})
        .get("action_breakdown", []),
        "decision_tree": results.get("executive_reviewer", {})
        .get("decision_tree", {}),
    }

    return merged


def save_report(merged_result: dict, pdf_path: str) -> Path:
    """리포트 저장"""
    timestamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    store_keyword = Path(pdf_path).stem.split("_")[1] if "_" in Path(pdf_path).stem else "unknown"

    output_path = OUTPUT_DIR / f"report-skillup_{store_keyword}_{timestamp}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged_result, f, ensure_ascii=False, indent=2)

    logger.info(f"📁 리포트 저장: {output_path}")
    return output_path


def main(pdf_path: str) -> dict:
    """메인 오케스트레이터"""
    logger.info("🚀 Report Skillup Orchestrator 시작")
    logger.info(f"📄 PDF: {pdf_path}")
    logger.info("=" * 60)

    results = {}

    # 1단계: qwen_prompter
    logger.info("📊 1단계: Qwen Prompter (PDF → 프롬프트 생성)")
    logger.info("=" * 60)
    _, qwen_result = run_agent("qwen_prompter", "qwen_prompter_agent.md", {"pdf_path": pdf_path})
    results["qwen_prompter"] = qwen_result

    # qwen 결과 임시 저장
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(qwen_result, f)
        qwen_json_path = f.name

    # 2단계: prompt_validator
    logger.info("=" * 60)
    logger.info("📋 2단계: Prompt Validator (검증 및 품질 평가)")
    logger.info("=" * 60)
    _, validator_result = run_agent(
        "prompt_validator",
        "prompt_validator_agent.md",
        {"qwen_output_json": qwen_json_path},
    )
    results["prompt_validator"] = validator_result

    # validator 결과 임시 저장
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(validator_result, f)
        validator_json_path = f.name

    # 3단계: executive_reviewer
    logger.info("=" * 60)
    logger.info("👔 3단계: Executive Reviewer (영업 관점 최종 검토)")
    logger.info("=" * 60)
    _, executive_result = run_agent(
        "executive_reviewer",
        "executive_reviewer_agent.md",
        {
            "qwen_output_json": qwen_json_path,
            "validator_output_json": validator_json_path,
        },
    )
    results["executive_reviewer"] = executive_result

    # 결과 병합
    logger.info("=" * 60)
    logger.info("🔗 결과 병합...")
    merged = merge_results(results, pdf_path)

    # 리포트 저장
    logger.info("=" * 60)
    logger.info("💾 리포트 저장...")
    report_path = save_report(merged, pdf_path)

    # 최종 요약
    logger.info("=" * 60)
    logger.info("✨ 최종 결과 요약")
    logger.info("=" * 60)
    logger.info(f"🎯 판단: {merged['summary']['qwen_judgment']}")
    logger.info(f"✅ 검증: {merged['summary']['validator_status']}")
    logger.info(f"👔 권고: {merged['summary']['executive_recommendation']}")
    logger.info(f"📄 보고서: {report_path}")

    return merged


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Report Skillup Orchestrator")
    parser.add_argument("--pdf-path", required=True, help="PDF 파일 경로")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    result = main(args.pdf_path)
    print(json.dumps(result, ensure_ascii=False, indent=2))
```
