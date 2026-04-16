"""
땡겨요 정책 수집 파이프라인

boss.ddangyo.com에 Playwright로 로그인하여 공지사항 목록과 본문을 수집하고
정책 행으로 변환하여 누적 CSV로 저장합니다.

처리 흐름:
1. OneDrive sales_employee.csv에서 땡겨요 계정 로드
2. Playwright 로그인 및 공지사항 목록 파싱
3. 각 공지 본문 수집 (재로그인 후 상세 페이지 접근)
4. 정책 행 생성 (8종 분류, 실행 제안 생성)
5. CSV 누적 저장 (기존 파일과 병합, 중복제거, 정렬)
"""

import re
import hashlib
import logging
import time
import pandas as pd
import pendulum
from pathlib import Path
from typing import List, Dict, Any, Optional

from modules.transform.utility.paths import DDANGYO_POLICY_CSV_PATH, ONEDRIVE_DB, POLICY_LOG_PATH

logger = logging.getLogger(__name__)

# 땡겨요 사장님 포털
DDANGYO_URL = "https://boss.ddangyo.com/"

# 정책 타입 표준 코드 (8종)
POLICY_TYPES = ["할인", "광고", "노출", "수수료", "정산", "운영", "기능변경", "기타"]
DEFAULT_POLICY_TYPE = "기타"

# sales_employee.csv 경로 및 컬럼
EMPLOYEE_CSV_PATH = ONEDRIVE_DB / "sales_employee.csv"
PLATFORM_COL = "플랫폼"
PLATFORM_NAME = "땡겨요"
ACCOUNT_ID_COL = "계정ID"
ACCOUNT_PW_COL = "계정PW"


# ============================================================
# 헬퍼 함수
# ============================================================

def _clean_text(text: str) -> str:
    """공백/개행을 정리한다."""
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _normalize_policy_date(raw_date: str) -> str:
    """공지 날짜 텍스트를 YYYY-MM-DD로 정규화한다."""
    text = (raw_date or "").strip()
    m = re.search(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", text)
    if m:
        year, month, day = m.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    return pendulum.now("Asia/Seoul").format("YYYY-MM-DD")


def _infer_policy_type(text: str) -> str:
    """키워드 기반 8종 정책유형 분류."""
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


def _login_playwright(page, accounts: list) -> bool:
    """Playwright 로그인 공통 로직 (최대 3회 재시도)."""
    for account in accounts[:3]:
        try:
            page.goto(DDANGYO_URL)
            page.wait_for_selector("#mf_ibx_mbrId", timeout=10000)
            page.fill("#mf_ibx_mbrId", account["account_id"])
            page.fill("#mf_sct_pwd", account["account_pw"])
            page.click("#mf_btn_webLogin")
            page.wait_for_selector(
                "#mf_wfm_side_gen_menuParent_5_gen_menuSub_1_btn_child",
                timeout=10000,
            )
            logger.info(f"로그인 성공: {account['account_id']}")
            return True
        except Exception as e:
            logger.warning(f"로그인 실패: {account['account_id']} - {e}")
            continue
    return False


# ============================================================
# 공개 함수
# ============================================================

def load_ddangyo_accounts(**context) -> List[Dict[str, str]]:
    """
    OneDrive sales_employee.csv에서 땡겨요 계정 로드.

    Returns:
        List[Dict]: [{"account_id": str, "account_pw": str}, ...]
    """
    logger.info("=" * 60)
    logger.info("[계정 로드] sales_employee.csv에서 땡겨요 계정 조회")
    logger.info(f"경로: {EMPLOYEE_CSV_PATH}")

    if not EMPLOYEE_CSV_PATH.exists():
        logger.warning(f"sales_employee.csv 파일이 없습니다: {EMPLOYEE_CSV_PATH}")
        return []

    try:
        df = pd.read_csv(EMPLOYEE_CSV_PATH, encoding="utf-8-sig", dtype=str)
    except Exception as e:
        logger.warning(f"sales_employee.csv 로드 실패: {e}")
        return []

    if PLATFORM_COL not in df.columns:
        logger.warning(f"컬럼 '{PLATFORM_COL}'이 없습니다. 컬럼 목록: {df.columns.tolist()}")
        return []

    filtered = df[df[PLATFORM_COL].str.strip() == PLATFORM_NAME]
    if filtered.empty:
        logger.warning(f"플랫폼='{PLATFORM_NAME}' 계정이 없습니다.")
        return []

    accounts = []
    for _, row in filtered.iterrows():
        account_id = str(row.get(ACCOUNT_ID_COL, "")).strip()
        account_pw = str(row.get(ACCOUNT_PW_COL, "")).strip()
        if account_id and account_pw:
            accounts.append({"account_id": account_id, "account_pw": account_pw})

    logger.info(f"땡겨요 계정 {len(accounts)}개 로드 완료")
    return accounts


def login_and_collect_notices(**context) -> List[Dict[str, Any]]:
    """
    Playwright 로그인 및 공지사항 목록 파싱.

    Returns:
        List[Dict]: [{"bord_ser_no": str, "category": str, "title": str, "policy_date": str}, ...]
    """
    from playwright.sync_api import sync_playwright

    logger.info("=" * 60)
    logger.info("[1단계] 땡겨요 로그인 및 공지 목록 수집 시작")

    accounts = load_ddangyo_accounts(**context)
    if not accounts:
        logger.warning("사용 가능한 계정이 없습니다.")
        return []

    notices = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            login_ok = _login_playwright(page, accounts)
            if not login_ok:
                logger.warning("모든 계정으로 로그인 실패.")
                return []

            # 공지사항 메뉴 클릭
            page.click("#mf_wfm_side_gen_menuParent_5_gen_menuSub_1_btn_child")
            page.wait_for_selector(
                "#mf_wfm_contents_wfm_tabcontents_gen_generator1 li",
                timeout=15000,
            )
            time.sleep(1)

            li_elements = page.query_selector_all(
                "#mf_wfm_contents_wfm_tabcontents_gen_generator1 li"
            )
            logger.info(f"공지 목록 li 개수: {len(li_elements)}")

            for li in li_elements:
                try:
                    bord_ser_no_el = li.query_selector("input[id*='ibx_bordSerNo']")
                    category_el = li.query_selector("span[id*='tbx_catIdNm']")
                    title_el = li.query_selector("span[id*='tbx_ttle']")
                    date_el = li.query_selector("span[id*='tbx_notiStaDt']")

                    bord_ser_no = bord_ser_no_el.get_attribute("value").strip() if bord_ser_no_el else ""
                    category = _clean_text(category_el.inner_text()) if category_el else ""
                    title = _clean_text(title_el.inner_text()) if title_el else ""
                    raw_date = _clean_text(date_el.inner_text()) if date_el else ""
                    policy_date = _normalize_policy_date(raw_date)

                    if not title:
                        continue

                    notices.append({
                        "bord_ser_no": bord_ser_no,
                        "category": category,
                        "title": title,
                        "policy_date": policy_date,
                    })
                except Exception as e:
                    logger.warning(f"공지 li 파싱 오류: {e}")
                    continue

        except Exception as e:
            logger.error(f"공지 목록 수집 중 오류: {e}")
        finally:
            browser.close()

    logger.info(f"공지 목록 수집 완료: {len(notices)}개")
    context["ti"].xcom_push(key="notice_list", value=notices)
    return notices


def collect_notice_bodies(**context) -> List[Dict[str, Any]]:
    """
    각 공지 본문 수집 (재로그인 후 상세 페이지 접근).

    Returns:
        List[Dict]: notice_list 각 항목에 'content' 필드 추가
    """
    from playwright.sync_api import sync_playwright

    logger.info("=" * 60)
    logger.info("[2단계] 공지 본문 수집 시작")

    notice_list: List[Dict[str, Any]] = context["ti"].xcom_pull(
        task_ids="task_login_and_collect", key="notice_list"
    ) or []

    if not notice_list:
        logger.warning("수집된 공지 목록이 없습니다.")
        context["ti"].xcom_push(key="notices_with_content", value=[])
        return []

    accounts = load_ddangyo_accounts(**context)
    if not accounts:
        logger.warning("사용 가능한 계정이 없습니다.")
        context["ti"].xcom_push(key="notices_with_content", value=[])
        return []

    notices_with_content = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            login_ok = _login_playwright(page, accounts)
            if not login_ok:
                logger.warning("본문 수집용 로그인 실패.")
                browser.close()
                context["ti"].xcom_push(key="notices_with_content", value=[])
                return []

            for i, notice in enumerate(notice_list):
                logger.info(f"[{i + 1}/{len(notice_list)}] 본문 수집: {notice['title']}")
                try:
                    # 공지사항 메뉴 재진입 (IBSheet Duplicate 방지)
                    page.click("#mf_wfm_side_gen_menuParent_5_gen_menuSub_1_btn_child")
                    page.wait_for_selector(
                        "#mf_wfm_contents_wfm_tabcontents_gen_generator1 li",
                        timeout=15000,
                    )
                    time.sleep(1)

                    # i번째 li의 공지 제목 링크 클릭
                    li_elements = page.query_selector_all(
                        "#mf_wfm_contents_wfm_tabcontents_gen_generator1 li"
                    )
                    if i >= len(li_elements):
                        logger.warning(f"인덱스 {i} 범위 초과 (li 개수: {len(li_elements)})")
                        continue

                    link_el = li_elements[i].query_selector("a[id*='grp_notice_title']")
                    if not link_el:
                        logger.warning(f"공지 링크 요소를 찾지 못했습니다 (인덱스: {i})")
                        continue

                    link_el.click()
                    page.wait_for_selector("#mf_wfm_contents_tbx_cont", timeout=10000)
                    time.sleep(0.5)

                    content_el = page.query_selector("#mf_wfm_contents_tbx_cont")
                    content = _clean_text(content_el.inner_text()) if content_el else ""

                    if not content:
                        logger.warning(f"본문이 비어 있습니다: {notice['title']}")

                    notice_copy = dict(notice)
                    notice_copy["content"] = content
                    notices_with_content.append(notice_copy)

                    logger.info(f"  본문 수집 완료: {len(content)}자")

                    page.go_back()
                    page.wait_for_selector(
                        "#mf_wfm_contents_wfm_tabcontents_gen_generator1 li",
                        timeout=10000,
                    )
                    time.sleep(0.5)

                except Exception as e:
                    logger.warning(f"본문 수집 오류 [{notice['title']}]: {e}")
                    continue

        except Exception as e:
            logger.error(f"본문 수집 세션 오류: {e}")
        finally:
            browser.close()

    logger.info(f"본문 수집 완료: {len(notices_with_content)}개")
    context["ti"].xcom_push(key="notices_with_content", value=notices_with_content)
    return notices_with_content


def build_policy_rows(**context) -> List[Dict[str, Any]]:
    """
    정책 행 생성 (정책유형 8종 분류, 실행 제안 생성).

    Returns:
        List[Dict]: policy_id, platform, collected_at, policy_date, title,
                    category, content, content_summary, policy_type,
                    recommended_action, source_url
    """
    logger.info("=" * 60)
    logger.info("[3단계] 정책 행 생성 시작")

    notices_with_content: List[Dict[str, Any]] = context["ti"].xcom_pull(
        task_ids="task_collect_notice_bodies", key="notices_with_content"
    ) or []

    if not notices_with_content:
        logger.warning("본문이 수집된 공지가 없습니다.")
        context["ti"].xcom_push(key="policy_rows", value=[])
        return []

    collected_at = pendulum.now("Asia/Seoul").format("YYYY-MM-DD HH:mm:ss")
    policy_rows = []

    for notice in notices_with_content:
        title = notice.get("title", "")
        content = notice.get("content", "")
        category = notice.get("category", "")
        policy_date = notice.get("policy_date", "")
        bord_ser_no = notice.get("bord_ser_no", "")

        if not title:
            continue

        # content_summary: 제목에서 카테고리 태그 제거 후 50자 이내
        content_summary = re.sub(r"^\[[^\]]+\]\s*", "", title)[:50].strip()

        # 정책유형 추론 (제목 + 본문 조합)
        combined_text = f"{title} {content[:200]}"
        policy_type = _infer_policy_type(combined_text)

        recommended_action = _build_action(policy_type, title)

        # policy_id: platform_bordSerNo (없으면 title 해시)
        if bord_ser_no:
            policy_id = f"ddangyo_{bord_ser_no}"
        else:
            policy_id = "ddangyo_" + hashlib.md5(f"{title}{policy_date}".encode()).hexdigest()[:8]

        source_url = DDANGYO_URL

        policy_rows.append({
            "policy_id": policy_id,
            "platform": "ddangyo",
            "collected_at": collected_at,
            "policy_date": policy_date,
            "title": title,
            "category": category,
            "content": content,
            "content_summary": content_summary,
            "policy_type": policy_type,
            "recommended_action": recommended_action,
            "source_url": source_url,
        })

        logger.info(f"  policy_id={policy_id} | type={policy_type} | {title[:30]}")

    logger.info(f"정책 행 생성 완료: {len(policy_rows)}개")
    context["ti"].xcom_push(key="policy_rows", value=policy_rows)
    return policy_rows


def save_policy_csv(**context) -> str:
    """
    CSV 누적 저장 (기존 파일과 병합, 중복제거, 정렬).

    Returns:
        str: 저장된 CSV 파일 경로
    """
    logger.info("=" * 60)
    logger.info("[4단계] 정책 CSV 저장 시작")

    policy_rows: List[Dict[str, Any]] = context["ti"].xcom_pull(
        task_ids="task_build_policy_rows", key="policy_rows"
    ) or []

    if not policy_rows:
        logger.info("저장할 정책 행이 없습니다.")
        return str(DDANGYO_POLICY_CSV_PATH)

    new_df = pd.DataFrame(policy_rows)

    if DDANGYO_POLICY_CSV_PATH.exists():
        try:
            existing_df = pd.read_csv(DDANGYO_POLICY_CSV_PATH, encoding="utf-8-sig")
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

    DDANGYO_POLICY_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(DDANGYO_POLICY_CSV_PATH, index=False, encoding="utf-8-sig")

    logger.info(f"저장 완료: {DDANGYO_POLICY_CSV_PATH}")
    logger.info(f"  총 {len(merged_df)}행 (신규 {len(new_df)}행 추가)")

    return str(DDANGYO_POLICY_CSV_PATH)


def write_policy_log(**context) -> str:
    """policy/log.parquet에 실행 이력 기록 (ddangyo)."""
    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="task_save_policy_csv") or str(DDANGYO_POLICY_CSV_PATH)
    policy_rows = ti.xcom_pull(task_ids="task_build_policy_rows", key="policy_rows") or []
    inserted = len(policy_rows)
    result = "success" if inserted > 0 else "skipped"

    try:
        total = (
            len(pd.read_csv(DDANGYO_POLICY_CSV_PATH, encoding="utf-8-sig"))
            if DDANGYO_POLICY_CSV_PATH.exists()
            else 0
        )
    except Exception:
        total = 0

    run_date = context.get("ds") or pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d")
    new_df = pd.DataFrame([{
        "run_at": pd.Timestamp.now(tz="Asia/Seoul"),
        "platform": "ddangyo",
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
        logger.info(f"log.parquet 기록 완료: {POLICY_LOG_PATH} | ddangyo inserted={inserted} result={result}")
        return f"log.parquet 기록 완료: ddangyo inserted={inserted} result={result}"
    except Exception as e:
        logger.error(f"log.parquet 쓰기 실패 (DAG 계속 진행): {e}")
        return f"log.parquet 쓰기 실패: {e}"
