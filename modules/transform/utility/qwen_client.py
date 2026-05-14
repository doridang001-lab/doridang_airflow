"""
로컬 Ollama LLM 클라이언트 래퍼 (gpt-oss 메인)
"""
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

OLLAMA_HOST_CANDIDATES = [
    "http://host.docker.internal:11434",  # Docker 컨테이너 내부 → 호스트
    "http://localhost:11434",             # Windows/WSL 직접 실행
    "http://127.0.0.1:11434",            # localhost 대체
]
LLM_MODEL_CANDIDATES = [
    "gpt-oss:20b",
    "gpt-oss:latest",
    "gpt-oss",
    "qwen2.5:7b",
    "qwen2.5:latest",
    "qwen2.5",
    "gemma2:2b",
]


def get_ollama_client():
    """Ollama 클라이언트 반환 (호스트 자동 탐색 + 모델 자동 선택)"""
    try:
        import ollama
    except ImportError:
        raise ImportError("ollama 패키지 필요: pip install ollama")

    last_error = None
    for host in OLLAMA_HOST_CANDIDATES:
        try:
            client = ollama.Client(host=host)
            models = client.list()
            model_names = [m["model"] for m in models.get("models", [])]

            for candidate in LLM_MODEL_CANDIDATES:
                if any(candidate in m for m in model_names):
                    logger.info(f"✅ Ollama 연결 성공: {host} / 모델: {candidate}")
                    return client, candidate

            last_error = Exception(f"사용 가능한 Ollama 모델 없음. 설치된 모델: {model_names}")
            break  # 연결은 됐으나 모델이 없는 경우 더 이상 다른 host 시도 불필요
        except Exception as e:
            logger.debug(f"Ollama 연결 시도 실패: {host} → {e}")
            last_error = e
            continue

    logger.error(f"❌ Ollama 연결 실패 (모든 호스트 시도): {last_error}")
    raise last_error


def query_qwen(prompt: str, system_prompt: Optional[str] = None) -> str:
    """
    qwen에 쿼리 실행 및 응답 반환

    Args:
        prompt: 사용자 입력
        system_prompt: 시스템 프롬프트 (옵션)

    Returns:
        str: qwen 응답 텍스트
    """
    try:
        client, model_name = get_ollama_client()
    except Exception as e:
        logger.error(f"Ollama 초기화 실패: {e}")
        raise

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    try:
        response = client.chat(model=model_name, messages=messages, stream=False)

        # ollama 라이브러리 버전에 따라 dict 또는 객체로 반환됨
        if hasattr(response, "message"):
            msg = response.message
            content = getattr(msg, "content", "") or ""
            # gpt-oss 등 reasoning 모델: content가 빈 경우 thinking/reasoning fallback
            if not content.strip():
                content = (
                    getattr(msg, "thinking", "")
                    or getattr(msg, "reasoning", "")
                    or ""
                )
        else:
            msg = response.get("message", {})
            content = msg.get("content", "") or ""
            if not content.strip():
                content = msg.get("thinking", "") or msg.get("reasoning", "") or ""

        if not content.strip():
            logger.warning(f"[LLM] {model_name} 응답이 비어 있습니다. 원본: {str(response)[:200]}")

        logger.debug(f"LLM 응답 길이: {len(content)}")
        return content
    except Exception as e:
        logger.error(f"LLM 쿼리 실패: {e}")
        raise


def query_qwen_json(prompt: str, system_prompt: Optional[str] = None) -> dict:
    """
    qwen에 JSON 응답 요청

    Args:
        prompt: 사용자 입력
        system_prompt: 시스템 프롬프트 (옵션)

    Returns:
        dict: JSON 파싱된 응답
    """
    response_text = query_qwen(prompt, system_prompt)

    # JSON 블록 추출 (```json ... ``` 형식)
    if "```json" in response_text:
        json_part = response_text.split("```json")[1].split("```")[0].strip()
    elif "```" in response_text:
        json_part = response_text.split("```")[1].split("```")[0].strip()
    else:
        json_part = response_text.strip()

    try:
        return json.loads(json_part)
    except json.JSONDecodeError as e:
        logger.error(f"JSON 파싱 실패: {e}\n응답: {response_text[:200]}")
        # 파싱 실패 시 원본 텍스트를 반환
        return {"raw_response": response_text, "parse_error": str(e)}


def batch_query_qwen(prompts: list[tuple[str, Optional[str]]]) -> list[str]:
    """
    여러 프롬프트를 순차 실행 (병렬은 호출자가 담당)

    Args:
        prompts: [(prompt, system_prompt), ...] 리스트

    Returns:
        list[str]: 응답 리스트
    """
    results = []
    for prompt, system_prompt in prompts:
        try:
            result = query_qwen(prompt, system_prompt)
            results.append(result)
        except Exception as e:
            logger.error(f"배치 쿼리 실패 (prompt={prompt[:50]}...): {e}")
            results.append(f"ERROR: {e}")
    return results
