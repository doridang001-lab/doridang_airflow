"""
SNS 스냅샷 일일 수집 파이프라인

수집 대상:
- 인스타그램 프로필 (doridang_official) → 팔로워/팔로잉/게시물 수
- 카카오톡 채널 (_UxiaxiG, 도리당) → 친구 수

두 페이지 모두 CSR(클라이언트 렌더링)이라 requests로는 껍데기만 받는다.
인스타그램은 Playwright로 렌더한 뒤 meta를 읽고, 카카오는 공개 profile JSON을 우선 사용한다.

처리 흐름:
1. 인스타 프로필 렌더 → meta[name=description] 파싱
2. 카카오 채널 공개 profile JSON → friend_count 파싱
3. 각각 INSTAGRAM_SNAPSHOT_CSV_PATH / KAKAO_FRIENDS_CSV_PATH 에 1행 누적 저장
"""

import json
import logging
import re
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
import pendulum
from airflow.exceptions import AirflowSkipException

from modules.transform.utility.paths import (
    INSTAGRAM_SNAPSHOT_CSV_PATH,
    KAKAO_FRIENDS_CSV_PATH,
)
from modules.transform.utility.playwright_launcher import launch_chromium

logger = logging.getLogger(__name__)

# ============================================================
# 수집 대상 상수
# ============================================================

INSTAGRAM_ACCOUNT = "doridang_official"
INSTAGRAM_URL = f"https://www.instagram.com/{INSTAGRAM_ACCOUNT}/"

KAKAO_CHANNEL_ID = "_UxiaxiG"
KAKAO_CHANNEL_NAME = "도리당"
KAKAO_URL = f"https://pf.kakao.com/{KAKAO_CHANNEL_ID}"
KAKAO_PROFILE_API_URL = f"https://pf.kakao.com/rocket-web/web/v2/profiles/{KAKAO_CHANNEL_ID}"

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# 인스타 meta description 예시:
#   "팔로워 1,409명, 팔로잉 1명, 게시물 80개 - 도리당(@doridang_official)님의 Instagram ..."
# 게시물 수는 로그아웃 상태 DOM에 없고 meta에만 있어서 meta를 단일 소스로 쓴다.
_INSTA_KO = re.compile(r"팔로워\s*([\d,]+)명,\s*팔로잉\s*([\d,]+)명,\s*게시물\s*([\d,]+)개")
_INSTA_EN = re.compile(
    r"([\d,]+)\s*Followers,\s*([\d,]+)\s*Following,\s*([\d,]+)\s*Posts", re.IGNORECASE
)

# 카카오 친구수 예시: "친구 1,044"
_KAKAO_FRIENDS = re.compile(r"([\d,]+)")


# ============================================================
# 내부 유틸
# ============================================================

def _to_int(text: Optional[str]) -> Optional[int]:
    """'1,044' / '1408' → int. 변환 불가 시 None."""
    if text is None:
        return None
    try:
        return int(str(text).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _new_context(browser):
    """인스타 meta를 한글로 받기 위해 locale/Accept-Language를 ko-KR로 고정한다."""
    return browser.new_context(
        user_agent=BROWSER_UA,
        locale="ko-KR",
        extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9"},
    )


def _collected_at() -> str:
    return pendulum.now("Asia/Seoul").format("YYYY-MM-DD HH:mm:ss")


def _collect_date() -> str:
    """스냅샷은 '지금 이 순간의 값'이라 논리 날짜(ds)가 아닌 실제 수집일을 쓴다."""
    return pendulum.now("Asia/Seoul").to_date_string()


def _append_snapshot(row: Dict[str, Any], csv_path, dedup_key) -> None:
    """스냅샷 1행을 CSV에 누적 저장 (dedup keep='last'로 재실행 멱등성 확보)."""
    df_new = pd.DataFrame([row])
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    if csv_path.exists():
        try:
            df_all = pd.concat(
                [pd.read_csv(csv_path, encoding="utf-8-sig"), df_new], ignore_index=True
            )
        except Exception as exc:
            logger.warning(f"기존 CSV 로드 실패({csv_path}): {exc}. 신규 데이터만 저장합니다.")
            df_all = df_new
    else:
        df_all = df_new

    df_all = df_all.drop_duplicates(subset=dedup_key, keep="last")
    df_all = df_all.sort_values("collect_date").reset_index(drop=True)
    df_all.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"저장 완료: {csv_path} (총 {len(df_all)}행)")


def _find_kakao_friend_count(value: Any) -> Optional[int]:
    """카카오 profile JSON에서 friend_count를 재귀적으로 찾는다."""
    if isinstance(value, dict):
        if "friend_count" in value:
            return _to_int(value.get("friend_count"))
        for child in value.values():
            found = _find_kakao_friend_count(child)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_kakao_friend_count(child)
            if found is not None:
                return found
    return None


def _collect_kakao_from_profile_api() -> int:
    """카카오 채널 공개 profile JSON에서 친구 수를 수집한다."""
    request = Request(
        KAKAO_PROFILE_API_URL,
        headers={
            "User-Agent": BROWSER_UA,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ko-KR,ko;q=0.9",
            "Referer": KAKAO_URL,
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"카카오 profile JSON 요청 실패: {exc}") from exc

    friends = _find_kakao_friend_count(payload)
    if friends is None:
        raise RuntimeError("카카오 profile JSON에서 friend_count를 찾지 못했습니다.")
    return friends


def _collect_kakao_from_dom() -> int:
    """카카오톡 채널 홈의 친구 수를 DOM에서 수집한다."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = launch_chromium(p, headless=True)
        try:
            ctx = _new_context(browser)
            page = ctx.new_page()
            page.goto(KAKAO_URL, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_selector(".txt_friends", timeout=30000)
            friends_text = page.inner_text(".txt_friends")
        finally:
            browser.close()

    match = _KAKAO_FRIENDS.search(friends_text or "")
    friends = _to_int(match.group(1)) if match else None
    if friends is None:
        logger.warning(f"카카오 친구수 DOM 파싱 실패. 원문: {friends_text!r}")
        raise RuntimeError("카카오 채널 DOM에서 친구 수를 찾지 못했습니다.")
    return friends


# ============================================================
# 수집
# ============================================================

def collect_instagram(**context) -> Dict[str, Any]:
    """인스타 프로필의 팔로워/팔로잉/게시물 수를 meta description에서 수집한다."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = launch_chromium(p, headless=True)
        try:
            ctx = _new_context(browser)
            page = ctx.new_page()
            page.goto(INSTAGRAM_URL, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_selector("meta[name='description']", state="attached", timeout=30000)
            meta_text = page.get_attribute("meta[name='description']", "content") or ""
        finally:
            browser.close()

    match = _INSTA_KO.search(meta_text) or _INSTA_EN.search(meta_text)
    if not match:
        # 인스타가 meta 문구를 바꾸면 이 로그가 유일한 단서다.
        logger.warning(f"인스타 meta 파싱 실패. 원문: {meta_text[:300]!r}")
        raise RuntimeError("인스타 meta description에서 팔로워/게시물 수를 찾지 못했습니다.")

    followers, following, posts = (_to_int(g) for g in match.groups())
    if followers is None or posts is None:
        logger.warning(f"인스타 숫자 변환 실패. 원문: {meta_text[:300]!r}")
        raise RuntimeError("인스타 지표를 숫자로 변환하지 못했습니다.")

    result = {
        "collect_date": _collect_date(),
        "account": INSTAGRAM_ACCOUNT,
        "followers": followers,
        "following": following,
        "posts": posts,
        "collected_at": _collected_at(),
        "source": "meta",
    }
    logger.info(f"인스타 수집 완료: 팔로워 {followers}, 팔로잉 {following}, 게시물 {posts}")
    return result


def collect_kakao(**context) -> Dict[str, Any]:
    """카카오톡 채널 홈의 친구 수를 공개 profile JSON에서 우선 수집한다."""
    source = "profile_api"
    try:
        friends = _collect_kakao_from_profile_api()
    except RuntimeError as exc:
        logger.warning(f"카카오 profile JSON 수집 실패. DOM fallback을 시도합니다: {exc}")
        friends = _collect_kakao_from_dom()
        source = "dom"

    result = {
        "collect_date": _collect_date(),
        "channel_id": KAKAO_CHANNEL_ID,
        "channel_name": KAKAO_CHANNEL_NAME,
        "friends": friends,
        "collected_at": _collected_at(),
        "source": source,
    }
    logger.info(f"카카오 수집 완료: 친구 {friends} (source={source})")
    return result


# ============================================================
# 저장
# ============================================================

def save_snapshots(**context) -> str:
    """두 수집 결과를 각자 경로에 누적 저장한다. 한쪽만 성공해도 그쪽은 저장한다."""
    ti = context["ti"]
    insta = ti.xcom_pull(task_ids="collect_instagram")
    kakao = ti.xcom_pull(task_ids="collect_kakao")

    saved = []

    if insta:
        _append_snapshot(insta, INSTAGRAM_SNAPSHOT_CSV_PATH, ["collect_date", "account"])
        saved.append(f"instagram(팔로워 {insta['followers']}, 게시물 {insta['posts']})")
    else:
        logger.warning("인스타 수집 결과가 없습니다. (저장 스킵)")

    if kakao:
        _append_snapshot(kakao, KAKAO_FRIENDS_CSV_PATH, ["collect_date", "channel_id"])
        saved.append(f"kakao(친구 {kakao['friends']})")
    else:
        logger.warning("카카오 수집 결과가 없습니다. (저장 스킵)")

    if not saved:
        raise AirflowSkipException("저장할 스냅샷이 없습니다. (인스타/카카오 모두 수집 실패)")

    message = " / ".join(saved)
    logger.info(f"스냅샷 저장 완료: {message}")
    return message
