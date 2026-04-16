"""
요기요 정책 수집 파이프라인

요기요 CEO 공지사항에서 정책 변경 사항을 수집하고 LLM 기반 분석을 수행합니다.

처리 흐름:
1. REST API로 공지 목록 수집 (ceo-api.yogiyo.co.kr)
2. title+policy_date 중복 판정 및 신규 목록 생성
3. REST API로 공지 본문 수집 및 LLM(gpt-oss-20b) 기반 정책행 생성
4. policy_type 표준 코드 매핑 (8종)
5. 정책행 누적 저장 및 중복제거, 정렬
"""

import os
import re
import json
import logging
import requests
import pandas as pd
import pendulum
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from modules.transform.utility.paths import YOGIYO_POLICY_CSV_PATH

logger = logging.getLogger(__name__)

# 요기요 CEO API (CSR 페이지 - REST API 직접 호출)
YOGIYO_API_BASE = "https://ceo-api.yogiyo.co.kr"
YOGIYO_LIST_API = (
    f"{YOGIYO_API_BASE}/announcements/"
    "?page=1&page_size=10&search=&display_channels__id=ceo_site"
)
YOGIYO_DETAIL_API = f"{YOGIYO_API_BASE}/announcements/{{notice_id}}/"
YOGIYO_VIEW_URL = "https://ceo.yogiyo.co.kr/announcement/view/{notice_id}/"

# 정책 타입 표준 코드 (8종)
POLICY_TYPES = ["할인", "광고", "노출", "수수료", "정산", "운영", "기능변경", "기타"]
DEFAULT_POLICY_TYPE = "기타"

# LLM 엔드포인트 (환경변수)
GPT_ENDPOINT = os.getenv("LOCAL_GPT_OSS_20B_ENDPOINT")
GPT_MODEL = os.getenv("LOCAL_GPT_OSS_20B_MODEL", "gpt-oss:20b")

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR",
    "Referer": "https://ceo.yogiyo.co.kr/",
}


def _extract_llm_json(content: str) -> Dict[str, Any]:
    """LLM 응답 문자열에서 JSON 객체를 최대한 안정적으로 추출한다."""
    text = (content or "").strip()
    if not text:
        raise ValueError("LLM 응답이 비어 있습니다.")

    # 1) ```json ... ``` 코드블록 우선
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL | re.IGNORECASE)
    if fenced:
        return json.loads(fenced.group(1))

    # 2) 전체가 JSON 객체인 경우
    if text.startswith("{") and text.endswith("}"):
        return json.loads(text)

    # 3) 텍스트 내 첫 JSON 객체 추출(중괄호 밸런싱)
    start = text.find("{")
    if start == -1:
        raise ValueError("JSON 형식을 찾지 못했습니다.")

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
                return json.loads(text[start:idx + 1])

    raise ValueError("JSON 객체 경계를 찾지 못했습니다.")


def _extract_assistant_text(response_data: Dict[str, Any]) -> str:
    """OpenAI 호환 응답에서 assistant 텍스트를 최대한 호환성 있게 추출한다."""
    choice = response_data.get("choices", [{}])[0]

    message = choice.get("message", {})
    content = message.get("content", "")

    if isinstance(content, str) and content.strip():
        return content

    if isinstance(content, list):
        parts: List[str] = []
        for part in content:
            if isinstance(part, str):
                if part.strip():
                    parts.append(part)
                continue
            if isinstance(part, dict):
                txt = part.get("text") or part.get("content") or ""
                if isinstance(txt, str) and txt.strip():
                    parts.append(txt)
        joined = "\n".join(parts).strip()
        if joined:
            return joined

    text_fallback = choice.get("text", "")
    if isinstance(text_fallback, str) and text_fallback.strip():
        return text_fallback

    finish_reason = choice.get("finish_reason")
    if finish_reason == "length":
        raise ValueError("LLM 응답이 비어 있습니다(토큰 상한 도달).")
    raise ValueError("LLM 응답이 비어 있습니다.")


def _clean_text(text: str) -> str:
    """공백/개행을 정리한다."""
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _infer_policy_type(text: str) -> str:
    """제목/요약 텍스트에서 정책유형을 우선순위 기반으로 판정한다."""
    t = _clean_text(text)

    if any(k in t for k in ["정산", "지급", "입금", "무이자", "할부", "이자", "결제일"]):
        return "정산"
    if any(k in t for k in ["할인", "즉시할인", "쿠폰", "배달비 지원", "이벤트", "프로모션"]):
        return "할인"
    if any(k in t for k in ["수수료", "중개이용료", "요금제", "요금", "비용"]):
        return "수수료"
    if any(k in t for k in ["광고", "마케팅"]):
        return "광고"
    if any(k in t for k in ["기능", "업데이트", "개선", "도입", "설정", "추가", "변경", "픽업", "포장"]):
        return "기능변경"
    if any(k in t for k in ["운영", "휴무", "영업", "정책", "연휴"]):
        return "운영"
    if any(k in t for k in ["노출", "순위", "검색", "배너", "랭킹"]):
        return "노출"

    return DEFAULT_POLICY_TYPE


def _refine_summary(title: str, summary: str, content_text: str) -> str:
    """요약 문장을 품질 기준에 맞게 정제한다."""
    s = _clean_text(summary)
    title_clean = _clean_text(re.sub(r"^\[[^\]]+\]\s*", "", title))

    low_quality_markers = ["안녕하세요", "공지입니다", "안내드립니다", "요기요입니다"]
    broken_spacing = re.search(r"(?:[가-힣]\s){3,}[가-힣]", s) is not None
    if (not s) or any(m in s for m in low_quality_markers) or len(s) < 10 or broken_spacing:
        s = title_clean

    s = s.replace("  ", " ").strip(" .")
    if len(s) > 50:
        s = s[:50].rstrip()

    if len(s) < 12:
        first_line = _clean_text(content_text)[:50]
        if first_line:
            s = first_line
    return s


def _build_action(policy_type: str, title: str) -> str:
    """정책유형/제목 기반 실행 제안 문장을 생성한다."""
    t = _clean_text(title)

    # 정산 관련
    if "무이자" in t or "할부" in t:
        return "무이자/할부 혜택을 메인 배너에 노출하고 고단가 상품 판매에 집중하세요"
    if "정산" in t and ("지급" in t or "지급일" in t):
        return "정산 지급 일정을 확인하고 주간 자금 계획을 재수립하세요"
    if "정산" in t and ("연휴" in t or "휴무" in t):
        return "연휴 기간 정산 일정을 미리 파악하고 현금 흐름을 계획하세요"

    # 할인 관련
    if "즉시할인" in t:
        return "즉시할인 메뉴를 설정하고 배너 노출로 객단가 증대를 유도하세요"
    if "배달비" in t and ("지원" in t or "면제" in t):
        return "배달비 지원 기간에 배달 주문을 유도하는 프로모션을 강화하세요"
    if "이벤트" in t or "프로모션" in t:
        return "할인 이벤트 기간에 신메뉴 및 고수익 상품을 집중 판매하세요"

    # 수수료 관련
    if "수수료" in t or "중개이용료" in t or "요금제" in t:
        return "수수료 변경을 반영해 상품 가격과 마진 구조를 즉시 조정하세요"

    # 기능변경 관련
    if "픽업" in t or "포장" in t:
        return "픽업/포장 관련 설정을 확인하고 픽업 상품 가격 전략을 수립하세요"
    if "기능" in t and ("도입" in t or "추가" in t):
        return "신규 기능을 즉시 활성화하고 1주일 성과를 점검하세요"

    # 운영 관련
    if "휴무" in t or "휴일" in t or "연휴" in t:
        return "휴무일 설정을 즉시 완료하고 배달시간을 명확히 안내하세요"

    # 폴백: 타입별 제안
    action_map = {
        "할인": "할인 지원 조건을 반영해 프로모션 예산을 확대하세요",
        "광고": "광고 지원 혜택 구간에 예산을 집중 투자하세요",
        "노출": "노출 개선 기능을 활성화하고 대표메뉴를 재정렬하세요",
        "수수료": "수수료 변경 기준에 맞춰 가격과 마진 구조를 조정하세요",
        "정산": "정산 일정 변동에 맞춰 주간 자금 계획을 다시 세우세요",
        "운영": "운영 정책 변경사항을 매장 운영시간과 공지에 즉시 반영하세요",
        "기능변경": "신규 기능 설정을 적용하고 1주일 성과를 점검하세요",
        "기타": "공지 핵심 변경점을 체크리스트로 만들어 바로 적용하세요",
    }
    return action_map.get(policy_type, action_map["기타"])


def _build_fallback_llm_result(title: str, content_text: str) -> Dict[str, str]:
    """LLM 실패 시 규칙 기반으로 최소 정책행을 생성한다."""
    inferred_type = _infer_policy_type(f"{title}\n{content_text}")
    return {
        "content_summary": _refine_summary(title, "", content_text),
        "policy_type": inferred_type,
        "recommended_action": _build_action(inferred_type, title),
    }


def _extract_ollama_base_url(endpoint: str) -> str:
    """OpenAI 호환 endpoint에서 Ollama base URL을 추출한다."""
    if not endpoint:
        return ""
    marker = "/v1/chat/completions"
    if marker in endpoint:
        return endpoint.split(marker)[0]
    return endpoint.rstrip("/")


def _request_llm_json(prompt: str) -> Dict[str, Any]:
    """OpenAI 호환 호출. content 비어있으면 토큰을 늘려 1회 재시도한다."""
    last_error = None
    for max_tokens, temperature in ((500, 0.3), (1200, 0.0)):
        try:
            llm_response = requests.post(
                GPT_ENDPOINT,
                json={
                    "model": GPT_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "response_format": {"type": "json_object"},
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                },
                timeout=90,
            )
            llm_response.raise_for_status()
            response_data = llm_response.json()
            assistant_message = _extract_assistant_text(response_data)
            return _extract_llm_json(assistant_message)
        except Exception as e:
            last_error = e
    raise last_error if last_error else ValueError("LLM 요청 실패")


def _request_llm_json_via_ollama_native(prompt: str) -> Dict[str, Any]:
    """Ollama /api/chat으로 JSON 응답을 직접 요청한다."""
    base_url = _extract_ollama_base_url(GPT_ENDPOINT)
    if not base_url:
        raise ValueError("Ollama base URL을 찾을 수 없습니다.")

    native_url = f"{base_url}/api/chat"
    resp = requests.post(
        native_url,
        json={
            "model": GPT_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "format": "json",
            "stream": False,
            "options": {"temperature": 0.0},
        },
        timeout=90,
    )
    resp.raise_for_status()
    data = resp.json()
    message = (data.get("message") or {}).get("content", "")
    return _extract_llm_json(message)


def _can_use_llm() -> bool:
    """LLM 엔드포인트 설정 여부만 우선 확인한다."""
    if not GPT_ENDPOINT:
        logger.warning("LLM 엔드포인트 미설정 - 규칙 기반 분석으로 대체합니다.")
        return False
    return True


def _normalize_policy_date(raw_date: str) -> str:
    """공지 날짜 텍스트를 YYYY-MM-DD로 정규화한다."""
    text = (raw_date or "").strip()
    m = re.search(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", text)
    if m:
        year, month, day = m.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    return pendulum.now("Asia/Seoul").format("YYYY-MM-DD")


def extract_notice_list(**context) -> List[Dict[str, Any]]:
    """
    요기요 CEO REST API로 공지 목록 수집

    Returns:
        List[Dict]: [{"notice_id": str, "title": str, "policy_date": str, "url": str, "platform": "yogiyo"}, ...]
    """
    logger.info("=" * 60)
    logger.info("[1단계] 요기요 공지 목록 수집 시작 (REST API)")
    logger.info(f"URL: {YOGIYO_LIST_API}")

    try:
        resp = requests.get(YOGIYO_LIST_API, headers=REQUEST_HEADERS, timeout=30)
        if not resp.ok:
            logger.warning(f"공지 목록 API {resp.status_code}: {resp.text[:300]}")
            resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"공지 목록 API 호출 실패: {e}")
        return []

    results = data.get("results", [])
    if not results:
        logger.warning("API 응답에 공지 항목이 없습니다.")
        return []

    logger.info(f"API 응답: 총 {data.get('count', '?')}건 중 {len(results)}건 수신")

    notices = []
    for item in results:
        try:
            notice_id = str(item.get("id", ""))
            if not notice_id:
                continue

            title = _clean_text(item.get("title", ""))
            if not title:
                continue

            # publish_datetime: "2026-04-02 11:56:45" 형식
            policy_date = _normalize_policy_date(
                item.get("publish_datetime") or item.get("created_datetime", "")
            )
            url = YOGIYO_VIEW_URL.format(notice_id=notice_id)

            notices.append({
                "notice_id": notice_id,
                "title": title,
                "policy_date": policy_date,
                "url": url,
                "platform": "yogiyo",
            })

        except Exception as e:
            logger.warning(f"공지 항목 파싱 오류: {e}")
            continue

    logger.info(f"✅ 파싱 완료: {len(notices)}개 공지")
    return notices


def detect_new_notices(notice_list: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
    """
    기존 CSV와 비교하여 신규 공지 판정

    Args:
        notice_list: extract_notice_list에서 반환한 공지 목록

    Returns:
        List[Dict]: 신규 공지 목록
    """
    logger.info("=" * 60)
    logger.info("[2단계] 신규 공지 판정 시작")

    if not notice_list:
        logger.warning("파싱된 공지가 없습니다.")
        return []

    # 기존 CSV 로드
    if YOGIYO_POLICY_CSV_PATH.exists():
        try:
            existing_df = pd.read_csv(YOGIYO_POLICY_CSV_PATH, encoding='utf-8-sig')
            logger.info(f"기존 정책 데이터: {len(existing_df)}행")

            existing_keys = set(
                existing_df.apply(lambda row: f"{row['title']}|{row['policy_date']}", axis=1).tolist()
            )
        except Exception as e:
            logger.warning(f"기존 CSV 로드 실패 (신규 파일로 간주): {e}")
            existing_keys = set()
    else:
        logger.info("기존 CSV 파일 없음 (첫 실행)")
        existing_keys = set()

    # 신규 공지 필터링 (최신순 목록 기준, 첫 중복 발견 시 중단)
    new_notices = []
    for notice in notice_list:
        key = f"{notice['title']}|{notice['policy_date']}"
        if key in existing_keys:
            logger.info(f"중복 공지 발견으로 스캔 중단: {notice['title']} ({notice['policy_date']})")
            break
        new_notices.append(notice)

    logger.info(f"✅ 신규 공지: {len(new_notices)}개 / 전체: {len(notice_list)}개")

    if new_notices:
        for i, notice in enumerate(new_notices, 1):
            logger.info(f"  [{i}] {notice['title']} ({notice['policy_date']})")

    return new_notices


def collect_policy_rows(new_notices: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
    """
    신규 공지 본문 수집 및 LLM 기반 정책행 생성

    Args:
        new_notices: detect_new_notices에서 반환한 신규 공지 목록

    Returns:
        List[Dict]: 정책행 목록
    """
    logger.info("=" * 60)
    logger.info("[3단계] 정책 본문 수집 및 LLM 분석 시작")

    if not new_notices:
        logger.info("처리할 신규 공지가 없습니다.")
        return []

    llm_enabled = _can_use_llm()

    policy_rows = []
    collected_at = pendulum.now('Asia/Seoul').format('YYYY-MM-DD HH:mm:ss')

    for i, notice in enumerate(new_notices, 1):
        logger.info(f"\n[{i}/{len(new_notices)}] {notice['title']}")

        # 본문 수집 (REST API - content 필드에 HTML 본문 포함)
        try:
            detail_url = YOGIYO_DETAIL_API.format(notice_id=notice['notice_id'])
            resp = requests.get(detail_url, headers=REQUEST_HEADERS, timeout=30)
            resp.raise_for_status()
            detail_data = resp.json()

            content_html = detail_data.get("content", "")
            if not content_html:
                logger.warning("API content 필드가 비어 있습니다. 스킵합니다.")
                continue

            content_text = BeautifulSoup(content_html, 'html.parser').get_text(
                separator='\n', strip=True
            )

            if len(content_text) < 20:
                logger.warning(f"본문이 너무 짧습니다({len(content_text)}자). 스킵합니다.")
                continue

            logger.info(f"  본문 수집 완료: {len(content_text)}자")

        except Exception as e:
            logger.error(f"본문 수집 실패: {e}")
            continue

        prompt = f"""다음은 요기요 공지사항입니다. 아래 3가지를 JSON 형식으로 추출하세요:

1. content_summary: 핵심 내용을 1줄로 요약 (50자 이내)
2. policy_type: 다음 중 하나로 분류 [{', '.join(POLICY_TYPES)}]
3. recommended_action: 사업자가 바로 실행할 수 있는 투자/운영 제안 문장으로 작성 (15~45자)
    예시: "수수료 변경을 반영해 상품 가격과 마진 구조를 즉시 조정하세요"

공지 제목: {notice['title']}
공지 본문:
{content_text[:2000]}

JSON 형식으로만 응답하세요:
{{"content_summary": "...", "policy_type": "...", "recommended_action": "..."}}

중요: 설명/근거/사고과정 없이 JSON 객체 1개만 반환하세요.
"""

        # LLM 분석 (OpenAI 호환 -> Ollama 네이티브 순으로 시도)
        llm_result = None
        if llm_enabled:
            for attempt in range(2):
                try:
                    if attempt == 0:
                        llm_result = _request_llm_json(prompt)
                    else:
                        llm_result = _request_llm_json_via_ollama_native(prompt)

                    if not isinstance(llm_result, dict):
                        raise ValueError("JSON 객체가 아닙니다.")

                    raw_summary = str(llm_result.get('content_summary', '')).strip()
                    title_for_type = _clean_text(notice['title'])

                    inferred_type = _infer_policy_type(title_for_type)
                    if inferred_type == DEFAULT_POLICY_TYPE:
                        inferred_type = _infer_policy_type(raw_summary)
                        if inferred_type == DEFAULT_POLICY_TYPE:
                            inferred_type = _infer_policy_type(f"{title_for_type} {raw_summary}")

                    llm_result['policy_type'] = inferred_type if inferred_type in POLICY_TYPES else DEFAULT_POLICY_TYPE
                    llm_result['content_summary'] = _refine_summary(notice['title'], raw_summary, content_text)
                    llm_result['recommended_action'] = _build_action(llm_result['policy_type'], notice['title'])

                    logger.info(f"  ✅ policy_type={llm_result['policy_type']}")
                    logger.info(f"  LLM 분석 완료 (시도 {attempt + 1})")
                    break

                except Exception as e:
                    logger.info(f"  LLM 응답 미수신 (시도 {attempt + 1}/2): {e}")
                    if attempt == 1:
                        logger.info("  LLM 미응답 지속. 규칙 기반 분석으로 대체합니다.")
                        llm_result = _build_fallback_llm_result(notice['title'], content_text)
                        break

        if not llm_enabled:
            llm_result = _build_fallback_llm_result(notice['title'], content_text)
            logger.info("  LLM 미설정 상태여서 규칙 기반 분석을 사용합니다.")

        if not llm_result:
            logger.warning("LLM/규칙기반 결과를 생성하지 못해 해당 공지를 건너뜁니다.")
            continue

        policy_row = {
            'policy_id': f"yogiyo_{notice['notice_id']}",
            'platform': 'yogiyo',
            'collected_at': collected_at,
            'policy_date': notice['policy_date'],
            'title': notice['title'],
            'content_summary': llm_result.get('content_summary', ''),
            'policy_type': llm_result.get('policy_type', DEFAULT_POLICY_TYPE),
            'recommended_action': llm_result.get('recommended_action', ''),
            'source_url': notice['url'],
        }

        policy_rows.append(policy_row)
        logger.info(f"  ✅ 정책행 생성 완료")

    logger.info(f"\n총 {len(policy_rows)}개 정책행 생성 완료")
    return policy_rows


def normalize_policy_types(policy_rows: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
    """
    policy_type을 표준 코드로 매핑

    Args:
        policy_rows: collect_policy_rows에서 반환한 정책행 목록

    Returns:
        List[Dict]: 정규화된 정책행 목록
    """
    logger.info("=" * 60)
    logger.info("[4단계] policy_type 정규화 시작")

    if not policy_rows:
        logger.info("정규화할 정책행이 없습니다.")
        return []

    type_mapping = {
        '할인': ['할인', '프로모션', '이벤트', '혜택'],
        '광고': ['광고', '마케팅'],
        '노출': ['노출', '순위', '검색', '배너'],
        '정산': ['정산', '수익', '지급', '무이자', '할부'],
        '수수료': ['수수료', '요금', '비용', '중개이용료', '요금제'],
        '운영': ['운영', '관리', '정책', '규정'],
        '기능변경': ['기능', '변경', '업데이트', '개선'],
        '기타': ['기타', '안내', '공지'],
    }

    normalized_rows = []
    for row in policy_rows:
        original_type = row['policy_type']
        normalized_type = DEFAULT_POLICY_TYPE

        for standard_type, keywords in type_mapping.items():
            if any(keyword in original_type for keyword in keywords):
                normalized_type = standard_type
                break

        if normalized_type not in POLICY_TYPES:
            normalized_type = DEFAULT_POLICY_TYPE

        row['policy_type'] = normalized_type
        normalized_rows.append(row)

        if original_type != normalized_type:
            logger.info(f"  정규화: '{original_type}' → '{normalized_type}'")

    logger.info(f"✅ 정규화 완료: {len(normalized_rows)}개 정책행")
    return normalized_rows


def save_policy_csv(normalized_rows: List[Dict[str, Any]], **context) -> str:
    """
    정책행을 CSV에 누적 저장 (중복제거 + 정렬)

    Args:
        normalized_rows: normalize_policy_types에서 반환한 정규화된 정책행 목록

    Returns:
        str: 저장된 CSV 파일 경로
    """
    logger.info("=" * 60)
    logger.info("[5단계] 정책 CSV 저장 시작")

    if not normalized_rows:
        logger.info("저장할 정책행이 없습니다.")
        return str(YOGIYO_POLICY_CSV_PATH)

    new_df = pd.DataFrame(normalized_rows)

    if YOGIYO_POLICY_CSV_PATH.exists():
        try:
            existing_df = pd.read_csv(YOGIYO_POLICY_CSV_PATH, encoding='utf-8-sig')
            logger.info(f"기존 데이터: {len(existing_df)}행")
            merged_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception as e:
            logger.warning(f"기존 CSV 로드 실패: {e}. 신규 데이터만 저장합니다.")
            merged_df = new_df
    else:
        logger.info("기존 CSV 없음. 신규 파일 생성")
        merged_df = new_df

    before_count = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=['title', 'policy_date'], keep='last')
    removed_count = before_count - len(merged_df)

    if removed_count > 0:
        logger.info(f"중복 제거: {removed_count}행")

    merged_df = merged_df.sort_values('policy_date', ascending=False)

    YOGIYO_POLICY_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(YOGIYO_POLICY_CSV_PATH, index=False, encoding='utf-8-sig')

    logger.info(f"✅ 저장 완료: {YOGIYO_POLICY_CSV_PATH}")
    logger.info(f"   총 {len(merged_df)}행 (신규 {len(new_df)}행 추가)")

    return str(YOGIYO_POLICY_CSV_PATH)


def write_policy_log(**context) -> str:
    """policy/log.parquet에 실행 이력 기록 (yogiyo)"""
    from modules.transform.utility.paths import POLICY_LOG_PATH

    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="task_save_policy_csv") or str(YOGIYO_POLICY_CSV_PATH)
    normalized_rows = ti.xcom_pull(task_ids="task_normalize_policy_types", key="normalized_rows") or []
    inserted = len(normalized_rows)
    result = "success" if inserted > 0 else "skipped"

    try:
        total = len(pd.read_csv(YOGIYO_POLICY_CSV_PATH, encoding="utf-8-sig")) if YOGIYO_POLICY_CSV_PATH.exists() else 0
    except Exception:
        total = 0

    run_date = context.get("ds") or pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d")
    new_df = pd.DataFrame([{
        "run_at": pd.Timestamp.now(tz="Asia/Seoul"),
        "platform": "yogiyo",
        "run_date": run_date,
        "inserted": inserted,
        "total": total,
        "result": result,
        "file_path": file_path,
    }])

    try:
        POLICY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        if POLICY_LOG_PATH.exists() and POLICY_LOG_PATH.stat().st_size > 0:
            prev_df = pd.read_parquet(POLICY_LOG_PATH)
            out_df = pd.concat([prev_df, new_df], ignore_index=True)
        else:
            out_df = new_df
        out_df["run_at"] = pd.to_datetime(out_df["run_at"], errors="coerce")
        out_df = out_df.sort_values("run_at", ascending=False).reset_index(drop=True)
        out_df.to_parquet(POLICY_LOG_PATH, index=False)
        logger.info(f"log.parquet 기록 완료: {POLICY_LOG_PATH} | yogiyo inserted={inserted} result={result}")
        return f"log.parquet 기록 완료: yogiyo inserted={inserted} result={result}"
    except Exception as e:
        logger.error(f"log.parquet 쓰기 실패 (DAG 계속 진행): {e}")
        return f"log.parquet 쓰기 실패: {e}"
