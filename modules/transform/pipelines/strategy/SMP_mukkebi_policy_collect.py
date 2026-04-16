"""
먹깨비 정책 수집 파이프라인

먹깨비(newboss.mukkebi.com) 이벤트/공지 게시판에서 정책 변경 사항을 수집합니다.
CSR(Next.js) 페이지 → REST API 직접 호출로 수집 (Playwright 불필요)
LLM 분석 없이 목록 메타데이터만 수집하는 단일 단계 구조입니다.

처리 흐름:
1. REST API로 이벤트 게시글 목록 전 페이지 수집
2. bd_idx 중복 판정 및 신규 목록 생성
3. 기존 CSV 병합 후 OneDrive 저장
"""

import logging
import requests
import pandas as pd
import pendulum
import math
from typing import List, Dict, Any, Set
from modules.transform.utility.paths import MUKKEBI_POLICY_CSV_PATH

logger = logging.getLogger(__name__)

# 먹깨비 REST API (CSR - Next.js, 인증 불필요)
API_URL = "https://newboss.mukkebi.com/main/cmk014_borad_list_pageing"
BC_CODE = "event"   # 이벤트 게시판
PER_PAGE = 10
PLATFORM = "mukkebi"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://newboss.mukkebi.com/customer?type=%EC%9D%B4%EB%B2%A4%ED%8A%B8&p=1",
    "Origin": "https://newboss.mukkebi.com",
}

# 타겟 스키마 컬럼 (기존 플랫폼과 동일)
SCHEMA_COLUMNS = [
    "policy_id", "platform", "collected_at", "policy_date",
    "title", "category", "content", "content_summary",
    "policy_type", "recommended_action", "source_url",
]


def _fetch_page(page: int) -> Dict[str, Any]:
    """단일 페이지 API 호출"""
    resp = requests.get(
        API_URL,
        headers=REQUEST_HEADERS,
        params={"bc_code": BC_CODE, "page": page, "per_page": PER_PAGE, "stx": "", "sca": "bd"},
        timeout=30,
    )
    if not resp.ok:
        logger.warning(f"API page={page} {resp.status_code}: {resp.text[:300]}")
        resp.raise_for_status()
    return resp.json()


def _parse_policy_date(item: Dict[str, Any]) -> str:
    """이벤트 기간 또는 등록일을 policy_date 문자열로 변환"""
    s_date = (item.get("bd_s_date") or "").strip()
    e_date = (item.get("bd_e_date") or "").strip()

    # 유효한 기간 날짜(0000-00-00 제외)
    if s_date and s_date != "0000-00-00" and e_date and e_date != "0000-00-00":
        return f"{s_date} ~ {e_date}"

    # 폴백: 등록일 앞 10자 (YYYY-MM-DD)
    regdate = (item.get("bd_regdate") or "")[:10]
    return regdate


def _coerce_int(value: Any, default: int = 0) -> int:
    """API 숫자 필드를 정수로 정규화한다."""
    if value is None:
        return default

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    text = str(value).strip().replace(",", "")
    if not text:
        return default

    try:
        return int(text)
    except ValueError:
        logger.warning(f"정수 변환 실패: value={value!r}, default={default}")
        return default


def extract_notice_list(**context) -> List[Dict[str, Any]]:
    """
    먹깨비 이벤트 게시판 전 페이지 수집

    Returns:
        List[Dict]: 수집된 공지 목록 (최신순)
    """
    logger.info("=" * 60)
    logger.info(f"[1단계] 먹깨비 공지 목록 수집 시작 (bc_code={BC_CODE})")

    # 1페이지로 total_count 확인
    try:
        first = _fetch_page(1)
    except Exception as e:
        logger.error(f"첫 번째 페이지 호출 실패: {e}")
        return []

    first_data = first.get("data") or {}
    first_items = list(first_data.get("list") or [])
    total_count = _coerce_int(first_data.get("total_count"), default=len(first_items))
    total_pages = max(1, math.ceil(total_count / PER_PAGE)) if total_count else 1
    logger.info(f"총 {total_count}건 / {total_pages}페이지")

    all_items: List[Dict[str, Any]] = first_items

    for page in range(2, total_pages + 1):
        try:
            data = _fetch_page(page)
            all_items.extend(data["data"]["list"])
            logger.info(f"페이지 {page}/{total_pages}: {len(data['data']['list'])}건 수신")
        except Exception as e:
            logger.warning(f"페이지 {page} 수집 실패: {e}")
            break

    # 정규화
    notices = []
    for item in all_items:
        bd_idx = str(item.get("bd_idx", "")).strip()
        if not bd_idx:
            continue
        notices.append({
            "bd_idx": int(bd_idx),
            "bd_num": str(item.get("bd_num", "")).strip(),
            "title": str(item.get("bd_title", "")).strip(),
            "policy_date": _parse_policy_date(item),
            "category": BC_CODE,
        })

    logger.info(f"총 {len(notices)}건 파싱 완료")
    return notices


def detect_new_notices(notice_list: List[Dict[str, Any]], **context) -> List[Dict[str, Any]]:
    """
    기존 CSV와 비교하여 신규 공지 판정 (bd_idx 기반)

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

    existing_ids: Set[int] = set()
    if MUKKEBI_POLICY_CSV_PATH.exists():
        try:
            existing_df = pd.read_csv(MUKKEBI_POLICY_CSV_PATH, encoding="utf-8-sig")
            logger.info(f"기존 정책 데이터: {len(existing_df)}행")
            # policy_id 형식: "mukkebi_{bd_idx}"
            existing_ids = set(
                int(pid.split("_")[-1])
                for pid in existing_df["policy_id"].astype(str).tolist()
                if "_" in pid and pid.split("_")[-1].isdigit()
            )
        except Exception as e:
            logger.warning(f"기존 CSV 로드 실패 (신규 파일로 간주): {e}")
    else:
        logger.info("기존 CSV 파일 없음 (첫 실행)")

    new_notices = [n for n in notice_list if n["bd_idx"] not in existing_ids]

    logger.info(f"신규 공지: {len(new_notices)}건 / 전체: {len(notice_list)}건")
    for i, n in enumerate(new_notices, 1):
        logger.info(f"  [{i}] bd_idx={n['bd_idx']} {n['title']} ({n['policy_date']})")

    return new_notices


def save_policy_csv(new_notices: List[Dict[str, Any]], **context) -> str:
    """
    신규 공지를 CSV에 누적 저장 (중복제거 + 정렬)

    Args:
        new_notices: detect_new_notices에서 반환한 신규 공지 목록

    Returns:
        str: 저장된 CSV 파일 경로
    """
    logger.info("=" * 60)
    logger.info("[3단계] 정책 CSV 저장 시작")

    if not new_notices:
        logger.info("저장할 신규 공지가 없습니다.")
        return str(MUKKEBI_POLICY_CSV_PATH)

    collected_at = pendulum.now("Asia/Seoul").format("YYYY-MM-DD HH:mm:ss")

    # 스키마 맞춰 DataFrame 생성 (없는 필드는 None)
    rows = []
    for n in new_notices:
        rows.append({
            "policy_id": f"mukkebi_{n['bd_idx']}",
            "platform": PLATFORM,
            "collected_at": collected_at,
            "policy_date": n["policy_date"],
            "title": n["title"],
            "category": n["category"],
            "content": None,
            "content_summary": None,
            "policy_type": None,
            "recommended_action": None,
            "source_url": (
                f"https://newboss.mukkebi.com/customer"
                f"?type=%EC%9D%B4%EB%B2%A4%ED%8A%B8&p=1"
            ),
        })

    new_df = pd.DataFrame(rows, columns=SCHEMA_COLUMNS)

    if MUKKEBI_POLICY_CSV_PATH.exists():
        try:
            existing_df = pd.read_csv(MUKKEBI_POLICY_CSV_PATH, encoding="utf-8-sig")
            logger.info(f"기존 데이터: {len(existing_df)}행")
            merged_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception as e:
            logger.warning(f"기존 CSV 로드 실패: {e}. 신규 데이터만 저장합니다.")
            merged_df = new_df
    else:
        logger.info("기존 CSV 없음. 신규 파일 생성")
        merged_df = new_df

    before_count = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=["policy_id"], keep="last")
    removed_count = before_count - len(merged_df)
    if removed_count > 0:
        logger.info(f"중복 제거: {removed_count}행")

    merged_df = merged_df.sort_values("policy_date", ascending=False)

    MUKKEBI_POLICY_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(MUKKEBI_POLICY_CSV_PATH, index=False, encoding="utf-8-sig")

    logger.info(f"저장 완료: {MUKKEBI_POLICY_CSV_PATH}")
    logger.info(f"   총 {len(merged_df)}행 (신규 {len(new_df)}행 추가)")
    return str(MUKKEBI_POLICY_CSV_PATH)


def write_policy_log(**context) -> str:
    """policy/log.parquet에 실행 이력 기록 (mukkebi)"""
    from modules.transform.utility.paths import POLICY_LOG_PATH

    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="task_save_policy_csv") or str(MUKKEBI_POLICY_CSV_PATH)
    new_notices = ti.xcom_pull(task_ids="task_detect_new_notices", key="new_notices") or []
    inserted = len(new_notices)
    result = "success" if inserted > 0 else "skipped"

    try:
        total = (
            len(pd.read_csv(MUKKEBI_POLICY_CSV_PATH, encoding="utf-8-sig"))
            if MUKKEBI_POLICY_CSV_PATH.exists()
            else 0
        )
    except Exception:
        total = 0

    run_date = context.get("ds") or pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d")
    new_df = pd.DataFrame([{
        "run_at": pd.Timestamp.now(tz="Asia/Seoul"),
        "platform": PLATFORM,
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
        logger.info(f"log.parquet 기록 완료: mukkebi inserted={inserted} result={result}")
        return f"log.parquet 기록 완료: mukkebi inserted={inserted} result={result}"
    except Exception as e:
        logger.error(f"log.parquet 쓰기 실패 (DAG 계속 진행): {e}")
        return f"log.parquet 쓰기 실패: {e}"
