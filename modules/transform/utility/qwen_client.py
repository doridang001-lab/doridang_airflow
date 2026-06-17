"""
로컬 Ollama LLM 클라이언트 래퍼 (qwen 메인)
"""
import json
import logging
import re
import tempfile
import time
from typing import Optional
from pathlib import Path

logger = logging.getLogger(__name__)

UNHEALTHY_MODEL_CACHE = Path(tempfile.gettempdir()) / "codex_qwen_unhealthy_models.json"
UNHEALTHY_MODEL_TTL_SEC = 24 * 60 * 60

OLLAMA_HOST_CANDIDATES = [
    "http://host.docker.internal:11434",  # Docker 컨테이너 내부 → 호스트
    "http://localhost:11434",             # Windows/WSL 직접 실행
    "http://127.0.0.1:11434",            # localhost 대체
]
LLM_MODEL_CANDIDATES = [
    "qwen2.5:7b",
    "qwen2.5:latest",
    "qwen2.5",
    "qwen:latest",
    "qwen",
    "gpt-oss:20b",
    "gpt-oss:latest",
    "gpt-oss",
    "gemma2:2b",
]
LLM_CHAT_OPTIONS = {
    "num_predict": 220,
}
GPT_OSS_OPTIONS = {
    "num_predict": 128,
    "temperature": 0,
    "top_p": 0.8,
}

MODEL_UNHEALTHY_HINTS = (
    "500 internal server error",
    "cuda error",
    "shared object initialization failed",
    "stack-based buffer",
    "llama-server process has terminated",
    "connection reset",
    "connection aborted",
    "broken pipe",
    "terminated",
)


def _model_name(model) -> str:
    if isinstance(model, dict):
        return model.get("model", "")
    return getattr(model, "model", "") or ""


def _available_model_candidates(model_names: list[str]) -> list[str]:
    unhealthy = _load_unhealthy_models()
    candidates = []
    seen = set()
    for candidate in LLM_MODEL_CANDIDATES:
        if candidate in model_names:
            matched = candidate
        else:
            matched = next((name for name in model_names if candidate in name), None)
        if matched and matched not in seen and not _is_model_unhealthy(matched, unhealthy):
            candidates.append(matched)
            seen.add(matched)
    return candidates


def _load_unhealthy_models() -> dict[str, float]:
    try:
        if not UNHEALTHY_MODEL_CACHE.exists():
            return {}
        payload = json.loads(UNHEALTHY_MODEL_CACHE.read_text(encoding="utf-8"))
        now = time.time()
        return {
            str(model): float(ts)
            for model, ts in payload.items()
            if now - float(ts) < UNHEALTHY_MODEL_TTL_SEC
        }
    except Exception:
        return {}


def _save_unhealthy_models(models: dict[str, float]) -> None:
    try:
        UNHEALTHY_MODEL_CACHE.write_text(json.dumps(models), encoding="utf-8")
    except Exception:
        pass


def _is_model_unhealthy(model_name: str, unhealthy: dict[str, float] | None = None) -> bool:
    unhealthy = unhealthy or _load_unhealthy_models()
    ts = unhealthy.get(model_name)
    return bool(ts and (time.time() - ts) < UNHEALTHY_MODEL_TTL_SEC)


def _mark_model_unhealthy(model_name: str) -> None:
    unhealthy = _load_unhealthy_models()
    unhealthy[model_name] = time.time()
    _save_unhealthy_models(unhealthy)


def _should_mark_model_unhealthy(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(hint in message for hint in MODEL_UNHEALTHY_HINTS)


def _prioritize_primary_models(model_candidates: list[str], prefer_stable: bool = True) -> list[str]:
    gpt_oss = [m for m in model_candidates if "gpt-oss" in m]
    stable = [m for m in model_candidates if "gpt-oss" not in m]
    if prefer_stable:
        return stable + gpt_oss
    return gpt_oss + stable


def _chat_options_for_model(model_name: str, is_json: bool = False) -> dict:
    if "gpt-oss" in model_name:
        options = dict(GPT_OSS_OPTIONS)
        if is_json:
            options["num_predict"] = 256
            options["top_p"] = 0.5
        return options
    options = dict(LLM_CHAT_OPTIONS)
    if is_json:
        options["num_predict"] = max(options.get("num_predict", 220), 512)
        options["temperature"] = 0
    return options


def get_ollama_client_with_candidates():
    """Ollama 클라이언트와 실행 가능한 후보 모델 목록 반환."""
    try:
        import ollama
    except ImportError:
        raise ImportError("ollama 패키지 필요: pip install ollama")

    last_error = None
    for host in OLLAMA_HOST_CANDIDATES:
        try:
            client = ollama.Client(host=host)
            models = client.list()
            if hasattr(models, "models"):
                raw_models = models.models
            else:
                raw_models = models.get("models", [])
            model_names = [_model_name(m) for m in raw_models]

            candidates = _available_model_candidates(model_names)
            if candidates:
                logger.info("✅ Ollama 연결 성공: %s / 후보 모델: %s", host, candidates)
                return client, candidates

            last_error = Exception(f"사용 가능한 Ollama 모델 없음. 설치된 모델: {model_names}")
            break  # 연결은 됐으나 모델이 없는 경우 더 이상 다른 host 시도 불필요
        except Exception as e:
            logger.debug(f"Ollama 연결 시도 실패: {host} → {e}")
            last_error = e
            continue

    logger.error(f"❌ Ollama 연결 실패 (모든 호스트 시도): {last_error}")
    raise last_error


def get_ollama_client():
    """Ollama 클라이언트 반환 (호스트 자동 탐색 + 첫 후보 모델 선택)."""
    client, candidates = get_ollama_client_with_candidates()
    return client, candidates[0]


def _response_content(response, model_name: str) -> str:
    # ollama 라이브러리 버전에 따라 dict 또는 객체로 반환됨
    if hasattr(response, "message"):
        msg = response.message
        content = getattr(msg, "content", "") or ""
        thinking = getattr(msg, "thinking", "") or ""
    else:
        msg = response.get("message", {})
        content = msg.get("content", "") or ""
        thinking = msg.get("thinking", "") or ""

    if not content.strip() and thinking.strip():
        content = thinking

    if not content.strip():
        logger.warning("[LLM] %s 응답이 비어 있습니다. 원본: %s", model_name, str(response)[:200])
        raise ValueError(f"{model_name} 응답 content가 비어 있습니다")

    return content


def _generate_content(response, model_name: str) -> str:
    if hasattr(response, "response"):
        content = response.response or ""
    else:
        content = response.get("response", "") or ""

    if not content.strip():
        logger.warning("[LLM] %s generate 응답이 비어 있습니다. 원본: %s", model_name, str(response)[:200])
        raise ValueError(f"{model_name} generate 응답이 비어 있습니다")

    return content


def _extract_from_pos(text: str, start: int) -> str | None:
    depth = 0
    in_str = False
    escape = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
            continue

        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:idx + 1]

    return None


def _extract_json_object(text: str) -> str:
    """텍스트 안의 JSON 객체를 추출한다. reasoning text가 앞에 있어도 처리한다."""
    text = (text or "").strip()
    if not text:
        raise ValueError("LLM 응답이 비어 있습니다")

    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()

    if text.startswith("{") and text.endswith("}"):
        return text

    last_brace = text.rfind("{")
    if last_brace != -1:
        candidate = _extract_from_pos(text, last_brace)
        if candidate:
            return candidate

    start = text.find("{")
    if start == -1:
        raise ValueError("JSON 형식을 찾지 못했습니다")
    candidate = _extract_from_pos(text, start)
    if candidate:
        return candidate

    raise ValueError("JSON 객체 경계를 찾지 못했습니다")


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
        client, model_candidates = get_ollama_client_with_candidates()
    except Exception as e:
        logger.error(f"Ollama 초기화 실패: {e}")
        raise

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    last_error = None
    for model_name in _prioritize_primary_models(model_candidates):
        try:
            response = client.chat(
                model=model_name,
                messages=messages,
                stream=False,
                think=False,
                options=_chat_options_for_model(model_name),
            )
            content = _response_content(response, model_name)
            logger.debug("LLM 응답 길이: %d (%s)", len(content), model_name)
            return content
        except Exception as e:
            last_error = e
            if "gpt-oss" in model_name and _should_mark_model_unhealthy(e):
                _mark_model_unhealthy(model_name)
            logger.warning("LLM 모델 실행 실패, 다음 후보 재시도: %s / %s", model_name, e)

    logger.error("LLM 쿼리 실패 (모든 후보 모델 실패): %s", last_error)
    raise last_error


def query_qwen_json(
    prompt: str,
    system_prompt: Optional[str] = None,
    preferred_models: Optional[list] = None,
    client=None,
    model_candidates: Optional[list] = None,
) -> dict:
    """
    qwen에 JSON 응답 요청

    Args:
        prompt: 사용자 입력
        system_prompt: 시스템 프롬프트 (옵션)
        preferred_models: 사용할 모델 목록 (지정 시 전역 후보 대신 이 목록만 사용)

    Returns:
        dict: JSON 파싱된 응답
    """
    if client is None or model_candidates is None:
        try:
            client, model_candidates = get_ollama_client_with_candidates()
        except Exception as e:
            logger.error(f"Ollama 초기화 실패: {e}")
            raise

    if preferred_models:
        available = {m for m in model_candidates}
        model_candidates = [m for m in preferred_models if m in available] or model_candidates

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    last_error = None
    last_response_text = ""

    for model_name in _prioritize_primary_models(model_candidates):
        try:
            attempts = 2 if "gpt-oss" in model_name else 1
            for attempt in range(1, attempts + 1):
                request_messages = messages
                if "gpt-oss" in model_name:
                    strict_system = (
                        "반드시 유효한 JSON 객체만 출력하세요. "
                        "설명, 마크다운, 코드블록, 추가 문장은 금지합니다. "
                        "문자열 값은 짧게 작성하고 모든 따옴표와 중괄호를 닫으세요."
                    )
                    request_messages = []
                    if system_prompt:
                        request_messages.append({
                            "role": "system",
                            "content": f"{system_prompt}\n{strict_system}",
                        })
                    else:
                        request_messages.append({"role": "system", "content": strict_system})
                    request_messages.append({"role": "user", "content": prompt})

                chat_kwargs = {
                    "model": model_name,
                    "messages": request_messages,
                    "stream": False,
                    "think": "gpt-oss" in model_name,
                    "options": _chat_options_for_model(model_name, is_json=True),
                }
                # gpt-oss는 Ollama format=json에서 CUDA/stack buffer 오류가 나는 경우가 있어
                # 일반 chat + 엄격 JSON 프롬프트로 JSON 텍스트를 받는다.
                if "gpt-oss" not in model_name:
                    chat_kwargs["format"] = "json"

                response = client.chat(**chat_kwargs)
                response_text = _response_content(response, model_name)
                last_response_text = response_text
                try:
                    json_part = _extract_json_object(response_text)
                    json_clean = re.sub(r'[\x00-\x1f]', '', json_part)
                    try:
                        return json.loads(json_clean)
                    except json.JSONDecodeError:
                        json_flat = json_part.replace('\n', '').replace('\r', '')
                        return json.loads(json_flat)
                except Exception as parse_exc:
                    last_error = parse_exc
                    log_func = logger.info if attempt < attempts else logger.warning
                    log_func(
                        "JSON 모델 %s 파싱 실패(%s/%s): %s",
                        model_name,
                        attempt,
                        attempts,
                        parse_exc,
                    )
                    if attempt < attempts:
                        continue
                    raise parse_exc
        except Exception as e:
            last_error = e
            if "gpt-oss" in model_name and _should_mark_model_unhealthy(e):
                _mark_model_unhealthy(model_name)
            logger.warning("JSON 모델 실행/파싱 실패, 다음 후보 재시도: %s / %s", model_name, e)

    logger.warning(f"JSON 파싱 실패: {last_error}\n응답: {last_response_text[:300]}")
    return {"raw_response": last_response_text, "parse_error": str(last_error)}


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
