# Qwen Prompter Agent

로컬 Ollama qwen을 이용해 PDF 리포트에서 프롬프트를 생성합니다.

**입력**: PDF 경로
**출력**: JSON (판단/액션/원인)

---

```python
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.transform.utility.qwen_client import query_qwen_json
from scripts._base import KST

logger = logging.getLogger(__name__)


def extract_pdf_data(pdf_path: Path) -> dict:
    """PDF에서 데이터 추출"""
    try:
        import pypdf
        with open(pdf_path, "rb") as f:
            reader = pypdf.PdfReader(f)
            text = "".join([page.extract_text() for page in reader.pages])
        return {
            "text": text,
            "page_count": len(reader.pages),
            "extracted_at": datetime.now(KST).isoformat(),
        }
    except Exception as e:
        logger.warning(f"PDF 텍스트 추출 실패: {e}")
        return {"text": "", "error": str(e)}


def create_judgment_prompt(metrics: dict) -> str:
    """핵심 판단 프롬프트"""
    return f"""당신은 배민 광고 전문가입니다.

이번 주 주문수 변화율: {metrics.get('order_delta', 0):.1f}%
이번 주 CVR: {metrics.get('cvr', 0):.1f}%
이번 주 ROAS: {metrics.get('roas', 0):.1f}x

30자 이내 핵심 판단을 한 문장으로 주세요.
반드시 JSON 형식으로:
{{"judgment": "..."}}"""


def create_action_prompt(metrics: dict) -> str:
    """액션 제안 프롬프트"""
    return f"""당신은 배민 광고 운영 전문가입니다.

이번주 매장 상황:
- 주문: {metrics.get('order_num', 0)}건 (전주 대비 {metrics.get('order_delta', 0):.1f}%)
- CTR: {metrics.get('ctr', 0):.2f}%
- CVR: {metrics.get('cvr', 0):.2f}%
- ROAS: {metrics.get('roas', 0):.1f}x

영업관리자가 당장 실행 가능한 액션 4개를 구체적으로 제시하세요.
반드시 JSON 형식으로:
{{
  "actions": [
    {{"priority": 1, "action": "...", "expected_effect": "..."}},
    ...
  ]
}}"""


def create_cause_prompt(metrics: dict) -> str:
    """원인 분석 프롬프트"""
    return f"""당신은 데이터 분석 전문가입니다.

변화 신호:
- 주문 {metrics.get('order_delta', 0):.1f}% 변화
- CTR {metrics.get('ctr_delta', 0):.1f}%p 변화
- CVR {metrics.get('cvr_delta', 0):.1f}%p 변화
- ROAS {metrics.get('roas_delta', 0):.1f}x 변화

3가지 가능한 원인을 우선순위와 함께 제시하세요.
반드시 JSON 형식으로:
{{
  "causes": [
    {{"priority": 1, "cause": "...", "likelihood": "높음"}},
    ...
  ]
}}"""


def main(pdf_path: str, metrics: dict = None) -> dict:
    """메인 로직"""
    run_at = datetime.now(KST)
    pdf_p = Path(pdf_path)

    # 1. PDF 데이터 추출
    pdf_data = extract_pdf_data(pdf_p)
    logger.info(f"PDF 추출: {pdf_p.name} ({pdf_data.get('page_count', '?')}페이지)")

    # 2. metrics 기본값 설정
    if metrics is None:
        metrics = {
            "order_num": 150,
            "order_delta": -15.5,
            "ctr": 2.1,
            "ctr_delta": -0.3,
            "cvr": 12.5,
            "cvr_delta": 2.0,
            "roas": 6.2,
            "roas_delta": 0.5,
        }

    logger.info(f"주문수: {metrics['order_num']}건 ({metrics['order_delta']:.1f}%)")

    # 3. qwen 프롬프트 생성 및 호출
    qwen_responses = {}
    prompts_used = []

    try:
        # 판단
        prompt_judgment = create_judgment_prompt(metrics)
        resp_judgment = query_qwen_json(prompt_judgment)
        qwen_responses["judgment"] = resp_judgment.get("judgment", "분석 불가")
        prompts_used.append({"name": "judgment"})
        logger.info(f"✅ 판단 생성")

        # 액션
        prompt_action = create_action_prompt(metrics)
        resp_action = query_qwen_json(prompt_action)
        qwen_responses["actions"] = resp_action.get("actions", [])
        prompts_used.append({"name": "actions"})
        logger.info(f"✅ 액션 생성: {len(qwen_responses['actions'])}개")

        # 원인
        prompt_cause = create_cause_prompt(metrics)
        resp_cause = query_qwen_json(prompt_cause)
        qwen_responses["causes"] = resp_cause.get("causes", [])
        prompts_used.append({"name": "causes"})
        logger.info(f"✅ 원인 생성: {len(qwen_responses['causes'])}개")

    except Exception as e:
        logger.error(f"❌ qwen 호출 실패: {e}")
        raise

    result = {
        "meta": {
            "agent": "qwen_prompter",
            "timestamp": run_at.isoformat(),
            "model": "qwen2.5:7b",
            "pdf_path": str(pdf_p),
        },
        "extracted": {
            "pdf_page_count": pdf_data.get("page_count", 0),
            "metrics": metrics,
        },
        "qwen_responses": qwen_responses,
    }

    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Qwen Prompter Agent")
    parser.add_argument("--pdf-path", required=True, help="PDF 파일 경로")
    parser.add_argument("--metrics-json", help="metrics JSON 파일 경로 (옵션)")
    args = parser.parse_args()

    metrics = None
    if args.metrics_json:
        with open(args.metrics_json) as f:
            metrics = json.load(f)

    logging.basicConfig(level=logging.INFO)
    result = main(args.pdf_path, metrics)
    print(json.dumps(result, ensure_ascii=False, indent=2))
```
