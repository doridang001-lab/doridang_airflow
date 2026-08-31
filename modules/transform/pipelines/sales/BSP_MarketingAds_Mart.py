"""
광고 프로젝트 성과 추적 마트 파이프라인.

Flow에 등록된 광고 프로젝트와 채널별 광고 실적을 연결해 Power BI가 바로 읽는 CSV 2개를 만든다.

    - marketing_ads_daily.csv   : 원본 상세 그레인 팩트
                             (네이버 = 일자x가변 depth, 당근 = 일자x광고소재)
- marketing_ads_campaign.csv: 광고 계층 ID 매핑 테이블

두 채널의 계층이 달라 통합 스키마에서 아래처럼 맞춘다.

    컬럼              네이버                          당근
    campaign_id       캠페인 ID(cmp-...)              광고그룹명(ID가 없어 이름을 사용)
    campaign_id_kind  id                              name
    campaign_name     캠페인 이름                     광고그룹명
    adgroup_id/name   광고그룹 ID(grp-...) + 이름     (빈값 - 그 레벨이 실제로 없음)
    ad_id/ad_name     키워드 ID/키워드명,             소재 ID(dg_*) + 광고명
                      플레이스 소재 ID/소재명 또는
                      과거 광고그룹 ID + 이름
    ad_level          keyword, creative 또는 adgroup  ad
    depth1            캠페인                         광고그룹
    depth2            광고그룹                       광고소재
    depth3            키워드/소재                    (빈값)
    leaf              실제 행의 최하위 depth          실제 행의 최하위 depth

    * ad_id는 "그 채널의 가장 작은 실측 단위"다. 네이버 파워링크 신규 원본은 키워드,
      플레이스 신규 원본은 소재가 최소 단위이며, 과거 광고그룹 CSV는 adgroup 레벨로 호환 처리한다.
    * 당근 원본 CSV의 `campaign_id` 컬럼명은 잘못된 이름이며 실제로는 광고(소재) ID다.

Flow 등록 규칙(담당자 안내 기준)
    네이버 : [네이버_광고] [campaign_id:cmp-a001-01-000000010894818] 송파삼전점 복날
    당근   : [당근_광고] [campaign_id:도리당 송파삼전점 #3] 삼전점 오픈 이벤트
    기간 : Flow 업무의 시작일/마감일 필드 (비어 있으면 본문의 "시작일:", "마감일:" fallback)
    상태 : Flow 업무의 상태 필드 (진행/완료/대기)

담당자가 넣는 값(link_key)은 네이버는 캠페인 ID 원본, 당근은 광고그룹명이다.
오타나 광고그룹명 변경으로 매칭이 끊기면 campaign_link_manual.csv로 보정한다.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd
import pendulum

from modules.transform.utility.paths import (
    DAANGN_ADS_CSV_PATH,
    FLOW_POST_PARQUET,
    MARKETING_ADS_ALERT_STATE_JSON,
    MARKETING_ADS_CAMPAIGN_CSV,
    MARKETING_ADS_DAILY_CSV,
    MARKETING_ADS_DAILY_FLOW_TASKS_CSV,
    MARKETING_ADS_FLOW_COMPARE_CSV,
    MARKETING_ADS_LINK_MANUAL_CSV,
    NAVER_ADS_DIR,
    NAVER_ADS_FILE_PATTERN,
)

logger = logging.getLogger(__name__)

CHANNEL_NAVER = "네이버"
CHANNEL_DAANGN = "당근"

UNREGISTERED_LABEL = "(미등록)"
OUT_OF_PERIOD_LABEL = "(기간외)"

STATUS_ACTIVE = "가동"
STATUS_BUDGET_CAPPED = "예산도달"
STATUS_PAUSED = "중지"

MATCH_SOURCE_FLOW = "flow"
MATCH_SOURCE_MANUAL = "manual"
MATCH_SOURCE_SOURCE = "source"

LINK_LEVEL_CAMPAIGN = "campaign"

# ad_id는 "그 채널의 가장 작은 실측 단위"로 통일한다.
#   네이버 = 키워드 CSV면 키워드, 과거 CSV면 광고그룹, 당근 = 광고 소재
# 실제 레벨은 ad_level로 표시해 두 값을 혼동하지 않게 한다.
AD_LEVEL_KEYWORD = "keyword"
AD_LEVEL_CREATIVE = "creative"
AD_LEVEL_ADGROUP = "adgroup"
AD_LEVEL_CAMPAIGN = "campaign"
AD_LEVEL_AD = "ad"

# campaign_id가 진짜 ID인지(네이버) 이름을 대신 쓴 것인지(당근) 구분한다.
ID_KIND_ID = "id"
ID_KIND_NAME = "name"

# 실적 행이 없는 캠페인도 채널만 알면 결정되는 값이라 빈칸으로 두지 않는다.
CHANNEL_AD_LEVEL = {CHANNEL_NAVER: AD_LEVEL_KEYWORD, CHANNEL_DAANGN: AD_LEVEL_AD}
CHANNEL_ID_KIND = {CHANNEL_NAVER: ID_KIND_ID, CHANNEL_DAANGN: ID_KIND_NAME}

# 채널별 커버리지 판정 방식
#   snapshot: 매일 전체 광고그룹을 내려준다(정지 상태도 0 노출 행으로 존재).
#             그날 행이 없으면 수집 실패 -> 결측
#   event   : 광고를 집행한 날만 행이 생긴다.
#             그날 행이 없으면 미집행이며 결측으로 볼 수 없다
COVERAGE_SNAPSHOT = "snapshot"
COVERAGE_EVENT = "event"
CHANNEL_COVERAGE_MODE = {
    CHANNEL_NAVER: COVERAGE_SNAPSHOT,
    CHANNEL_DAANGN: COVERAGE_EVENT,
}

READ_ENCODINGS = ("utf-8-sig", "utf-8", "cp949")
NAVER_ADS_FILE_PATTERNS = ("naver_ads_group_*.csv", NAVER_ADS_FILE_PATTERN)

TITLE_CHANNEL_RE = re.compile(r"\[\s*(네이버|당근)\s*[_\s]*광고\s*\]")
CAMPAIGN_ID_RE = re.compile(r"\[\s*campaign[_\s]*id\s*[:：]\s*([^\]]+?)\s*\]", re.IGNORECASE)
BRACKET_RE = re.compile(r"\[[^\]]*\]")
BODY_START_RE = re.compile(r"시작일\s*[:：]?\s*(\d{4})[-./]?(\d{2})[-./]?(\d{2})")
BODY_END_RE = re.compile(r"(?:마감일|종료일)\s*[:：]?\s*(\d{4})[-./]?(\d{2})[-./]?(\d{2})")
STORE_RE = re.compile(r"([가-힣A-Za-z0-9]+점)")

# 네이버 캠페인 ID prefix가 광고 유형을 담고 있다: cmp-a001-01-* = 파워링크, cmp-a001-06-* = 플레이스
NAVER_CAMPAIGN_PREFIX_RE = re.compile(r"^cmp-a\d+-(\d+)-")
NAVER_AD_TYPE_BY_PREFIX = {"01": "파워링크", "06": "플레이스"}
NAVER_AD_ACCOUNT_ID = "1497096"
BUDGET_CAPPED_KEYWORD = "예산 도달"
FLOW_AD_PROJECT_ID = "2960298"
FLOW_AD_PROJECT_NAME = "DB 광고 프로젝트 성과"
FLOW_AD_PARENT_BY_CHANNEL = {
    CHANNEL_DAANGN: {"title": "당근광고", "url_code": "QqxSI", "post_id": "84107449"},
    CHANNEL_NAVER: {"title": "네이버광고", "url_code": "QqxSN", "post_id": "84107459"},
}

DAILY_COLS = [
    "stat_date",
    "channel",
    "ad_type",
    "campaign_id",
    "campaign_id_kind",
    "campaign_name",
    "adgroup_id",
    "adgroup_name",
    "ad_id",
    "ad_name",
    "ad_level",
    "source_depth_level",
    "depth1_id",
    "depth1_name",
    "depth2_id",
    "depth2_name",
    "depth3_id",
    "depth3_name",
    "leaf_depth",
    "leaf_id",
    "leaf_name",
    "status_raw",
    "status_class",
    "link_level",
    "link_key",
    "campaign_key",
    "project_name",
    "store_name",
    "project_status",
    "start_date",
    "end_date",
    "in_period",
    "overlap_flag",
    "impressions",
    "clicks",
    "ctr",
    "cpc",
    "cost",
    "flow_url",
    "collected_at",
    "flow_task_label",
    "FLOW_LINK",
    "FLOW_TITLE",
    "url",
]

CAMPAIGN_COLS = [
    "channel",
    "id",
    "std_name",
    "name_01",
    "name_02",
    "name_03",
    "leaf_depths",
    "ad_type",
    "start_date",
    "end_date",
]

FLOW_COMPARE_COLS = [
    "project_id",
    "project_name",
    "parent_post_id",
    "parent_title",
    "flow_post_id",
    "flow_url",
    "channel",
    "id",
    "std_name",
    "std_title",
    "task_title",
    "channel_warning",
    "start_date",
    "end_date",
    "prev_start_date",
    "prev_end_date",
    "impressions",
    "clicks",
    "ctr",
    "cpc",
    "cost",
    "prev_impressions",
    "prev_clicks",
    "prev_ctr",
    "prev_cpc",
    "prev_cost",
    "diff_impressions",
    "diff_clicks",
    "diff_cost",
    "pct_impressions",
    "pct_clicks",
    "pct_cost",
    "matched_daily_rows",
    "collected_at",
]

DAILY_FLOW_TASK_COLS = [
    "stat_date",
    "channel",
    "id",
    "std_name",
    "flow_post_id",
    "flow_url",
    "parent_title",
    "std_title",
    "flow_task_label",
    "FLOW_LINK",
    "FLOW_TITLE",
    "start_date",
    "end_date",
]

# 원천 로드 결과의 공통 컬럼 (source_store는 프로젝트 미매칭 행의 매장명 fallback용 내부 컬럼)
SOURCE_COLS = [
    "stat_date",
    "channel",
    "ad_type",
    "campaign_id",
    "campaign_id_kind",
    "campaign_name",
    "adgroup_id",
    "adgroup_name",
    "ad_id",
    "ad_name",
    "ad_level",
    "source_depth_level",
    "depth1_id",
    "depth1_name",
    "depth2_id",
    "depth2_name",
    "depth3_id",
    "depth3_name",
    "leaf_depth",
    "leaf_id",
    "leaf_name",
    "status_raw",
    "status_class",
    "link_level",
    "link_key",
    "source_store",
    "impressions",
    "clicks",
    "cost",
    "url",
]

MANUAL_LINK_COLS = [
    "channel",
    "link_key",
    "flow_post_id",
    "project_name",
    "start_date",
    "end_date",
    "status",
    "store_name",
    "memo",
]

NAVER_REQUIRED_COLS = ["collected_date", "캠페인 ID", "광고그룹 ID", "노출수", "클릭수", "총비용"]
DAANGN_REQUIRED_COLS = ["시작일", "campaign_id", "광고그룹명", "노출수", "클릭수", "지출"]


# ------------------------------------------------------------------
# 공통 헬퍼
# ------------------------------------------------------------------
def _now_iso() -> str:
    return pendulum.now("Asia/Seoul").isoformat()


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if value is pd.NaT:
        return ""
    return str(value).strip()


def _to_number(value: Any) -> float:
    """'1,192', '1.26 %', '' 같은 문자열을 숫자로 정규화한다."""
    text = _text(value)
    if not text:
        return 0.0
    text = text.replace(",", "").replace("%", "").replace("₩", "").replace(" ", "")
    if not text or text in {"-", "--"}:
        return 0.0
    try:
        return float(text)
    except ValueError:
        logger.warning("숫자 변환 실패: %r", value)
        return 0.0


def _to_date(value: Any) -> str:
    """YYYYMMDD / YYYY-MM-DD / ISO 문자열을 YYYY-MM-DD로 정규화한다."""
    text = _text(value)
    if not text:
        return ""
    digits = re.sub(r"\D", "", text)
    if len(digits) < 8:
        return ""
    return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"


def _guess_store(text: Any) -> str:
    match = STORE_RE.search(_text(text))
    return match.group(1) if match else ""


def _safe_div(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return numerator / denominator


def _read_csv_with_fallback(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in READ_ENCODINGS:
        try:
            return pd.read_csv(path, dtype=str, encoding=encoding).fillna("")
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
    raise ValueError(f"CSV 인코딩 판별 실패: {path} ({last_error})")


def _validate_columns(df: pd.DataFrame, required: Iterable[str], label: str) -> None:
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"{label} 필수 컬럼 누락: {missing}")


def _naver_source_files(naver_dir: Path, file_pattern: str | Iterable[str]) -> list[Path]:
    patterns = [file_pattern] if isinstance(file_pattern, str) else list(file_pattern)
    files: dict[str, Path] = {}
    for pattern in patterns:
        for path in sorted(naver_dir.glob(pattern)):
            files[str(path.resolve())] = path
    return sorted(files.values())


def _normalize_depth_level(value: Any) -> str:
    text = _text(value).lower()
    match = re.search(r"([1-3])", text)
    return match.group(1) if match else ""


def _leaf_from_depths(*pairs: tuple[str, str]) -> tuple[str, str, str]:
    for level, value in reversed(list(pairs)):
        if _text(value):
            return level, _text(value), _text(value)
    return "", "", ""


def _naver_depth_fields(row: dict[str, Any]) -> dict[str, str]:
    campaign_id = _text(row.get("캠페인 ID"))
    campaign_name = _text(row.get("캠페인 이름"))
    adgroup_id = _text(row.get("광고그룹 ID"))
    adgroup_name = _text(row.get("광고그룹 이름"))
    keyword_id = _text(row.get("키워드 ID"))
    keyword_name = _text(row.get("키워드"))
    creative_id = _text(row.get("소재 ID"))
    creative_name = _text(row.get("소재"))

    depth3_id = keyword_id or creative_id
    depth3_name = keyword_name or creative_name
    if depth3_name and not depth3_id:
        depth3_id = "::".join(part for part in [campaign_id, adgroup_id, depth3_name] if part)
    leaf_depth, leaf_id, leaf_name = _leaf_from_depths(
        ("1", campaign_id),
        ("2", adgroup_id),
        ("3", depth3_id or depth3_name),
    )
    if leaf_depth == "1":
        leaf_name = campaign_name or leaf_id
    elif leaf_depth == "2":
        leaf_name = adgroup_name or leaf_id
    elif leaf_depth == "3":
        leaf_name = depth3_name or leaf_id

    return {
        "source_depth_level": _normalize_depth_level(row.get("depth번호")) or leaf_depth,
        "depth1_id": campaign_id,
        "depth1_name": campaign_name,
        "depth2_id": adgroup_id,
        "depth2_name": adgroup_name,
        "depth3_id": depth3_id,
        "depth3_name": depth3_name,
        "leaf_depth": leaf_depth,
        "leaf_id": leaf_id,
        "leaf_name": leaf_name,
    }


def _daangn_depth_fields(row: dict[str, Any]) -> dict[str, str]:
    group_name = _text(row.get("광고그룹명"))
    ad_id = _text(row.get("campaign_id"))
    ad_name = _text(row.get("광고명"))
    return {
        "source_depth_level": "2",
        "depth1_id": group_name,
        "depth1_name": group_name,
        "depth2_id": ad_id,
        "depth2_name": ad_name or ad_id,
        "depth3_id": "",
        "depth3_name": "",
        "leaf_depth": "2" if ad_id or ad_name else "1",
        "leaf_id": ad_id or group_name,
        "leaf_name": ad_name or ad_id or group_name,
    }


def _write_csv_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(temp_path, index=False, encoding="utf-8-sig")
    try:
        os.replace(temp_path, path)
    except PermissionError as exc:
        logger.warning("CSV atomic replace 실패, 직접 쓰기로 재시도: %s -> %s (%s)", temp_path, path, exc)
        try:
            temp_path.unlink(missing_ok=True)
        except OSError as cleanup_exc:
            logger.warning("임시 CSV 삭제 실패: %s (%s)", temp_path, cleanup_exc)
        df.to_csv(path, index=False, encoding="utf-8-sig")


def _with_rates(df: pd.DataFrame) -> pd.DataFrame:
    """CTR/CPC는 원본 평균값을 합산하지 않고 항상 재계산한다."""
    df = df.copy()
    df["ctr"] = [
        round(_safe_div(clicks, impressions) * 100, 2)
        for clicks, impressions in zip(df["clicks"], df["impressions"])
    ]
    df["cpc"] = [round(_safe_div(cost, clicks), 2) for cost, clicks in zip(df["cost"], df["clicks"])]
    return df


def _naver_ad_type(campaign_id: str) -> str:
    match = NAVER_CAMPAIGN_PREFIX_RE.match(campaign_id)
    if not match:
        return ""
    prefix = match.group(1)
    ad_type = NAVER_AD_TYPE_BY_PREFIX.get(prefix)
    if ad_type:
        return ad_type
    logger.warning("알 수 없는 네이버 캠페인 유형 prefix: %s (campaign_id=%s)", prefix, campaign_id)
    return f"기타({prefix})"


def _naver_collect_url(row: dict[str, Any], campaign_id: str, stat_date: str) -> str:
    source_url = _text(row.get("수집URL")) or _text(row.get("URL"))
    if source_url or not campaign_id or not stat_date:
        return source_url
    return (
        f"https://ads.naver.com/manage/ad-accounts/{NAVER_AD_ACCOUNT_ID}/sa/campaigns/{campaign_id}"
        f"?startDate={stat_date}&endDate={stat_date}&dateRange={stat_date}%2C{stat_date}"
    )


def _status_class(status_raw: str, impressions: float) -> str:
    """가동/예산도달/중지 분류.

    당근 `상태`는 소재별 현재 스냅샷이라 날짜별 가동 여부로 쓸 수 없으므로
    노출 실적을 기준으로 판정하고, 예산 도달만 네이버 상태 문자열에서 읽는다.
    """
    if BUDGET_CAPPED_KEYWORD in status_raw:
        return STATUS_BUDGET_CAPPED
    return STATUS_ACTIVE if impressions > 0 else STATUS_PAUSED


def _date_range(start_date: str, end_date: str) -> list[str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        return []
    days = (end - start).days + 1
    return [(start + timedelta(days=offset)).isoformat() for offset in range(days)]


def _compress_dates(dates: Iterable[str]) -> str:
    """연속된 날짜를 'a~b' 범위로 압축한다."""
    ordered = sorted(set(dates))
    if not ordered:
        return ""
    ranges: list[tuple[str, str]] = []
    start = previous = ordered[0]
    for current in ordered[1:]:
        if date.fromisoformat(current) - date.fromisoformat(previous) == timedelta(days=1):
            previous = current
            continue
        ranges.append((start, previous))
        start = previous = current
    ranges.append((start, previous))
    return ", ".join(begin if begin == end else f"{begin}~{end}" for begin, end in ranges)


# ------------------------------------------------------------------
# 1. Flow 캠페인 파싱 + 수동 보정 병합
# ------------------------------------------------------------------
def parse_flow_campaigns(
    post_dir: Path | None = None,
    manual_csv: Path | None = None,
    **context: Any,
) -> dict[str, Any]:
    """Flow 게시글에서 광고 등록 규칙에 맞는 프로젝트를 뽑고 수동 보정을 얹는다."""

    post_dir = Path(post_dir or FLOW_POST_PARQUET)
    campaigns = _parse_flow_posts(post_dir)
    campaigns, manual_stats = _merge_manual_links(campaigns, manual_csv)

    campaigns.sort(key=lambda item: (item["channel"], item["link_key"], item["start_date"]))
    if not campaigns:
        logger.warning(
            "Flow 광고 등록 규칙([채널_광고] [campaign_id:...] 프로젝트명)에 맞는 게시글이 없습니다. "
            "실적은 %s로 적재됩니다.",
            UNREGISTERED_LABEL,
        )
    else:
        logger.info(
            "광고 캠페인 %s건 (flow=%s, 수동보정 override=%s, 수동추가=%s)",
            len(campaigns),
            sum(1 for item in campaigns if item["match_source"] == MATCH_SOURCE_FLOW),
            manual_stats["overridden"],
            manual_stats["added"],
        )

    return {
        "campaigns": campaigns,
        "post_dir": str(post_dir),
        "manual_overridden": manual_stats["overridden"],
        "manual_added": manual_stats["added"],
        "collected_at": _now_iso(),
    }


def _parse_flow_posts(post_dir: Path) -> list[dict[str, Any]]:
    if not post_dir.exists():
        logger.warning("Flow 게시글 경로 없음: %s", post_dir)
        return []

    posts = pd.read_parquet(post_dir)
    if posts.empty:
        logger.warning("Flow 게시글 0건: %s", post_dir)
        return []

    columns = [
        "post_id",
        "project_id",
        "project_name",
        "store_name",
        "title",
        "content_text",
        "task_status",
        "start_dt",
        "end_dt",
        "post_url",
        "author_name",
        "worker",
    ]
    posts = posts.reindex(columns=columns, fill_value="")

    campaigns: list[dict[str, Any]] = []
    for row in posts.to_dict("records"):
        campaign = _parse_campaign_row(row)
        if campaign:
            campaigns.append(campaign)
    return campaigns


def _parse_campaign_row(row: dict[str, Any]) -> dict[str, Any] | None:
    title = _text(row.get("title"))
    if not title:
        return None

    channel_match = TITLE_CHANNEL_RE.search(title)
    id_match = CAMPAIGN_ID_RE.search(title)
    if not channel_match or not id_match:
        return None

    link_key = _text(id_match.group(1))
    if not link_key:
        return None

    project_name = _text(BRACKET_RE.sub(" ", title))
    project_name = re.sub(r"\s+", " ", project_name).strip()

    content = _text(row.get("content_text"))
    start_date = _to_date(row.get("start_dt"))
    end_date = _to_date(row.get("end_dt"))
    if not start_date:
        body = BODY_START_RE.search(content)
        start_date = _to_date("".join(body.groups())) if body else ""
    if not end_date:
        body = BODY_END_RE.search(content)
        end_date = _to_date("".join(body.groups())) if body else ""

    post_id = _text(row.get("post_id"))
    channel = channel_match.group(1)

    return {
        "campaign_key": post_id or f"{channel}|{link_key}|{start_date}",
        "channel": channel,
        "link_key": link_key,
        "project_name": project_name or link_key,
        "store_name": _text(row.get("store_name")) or _guess_store(project_name),
        "status": _text(row.get("task_status")),
        "start_date": start_date,
        "end_date": end_date,
        "flow_post_id": post_id,
        "flow_url": _text(row.get("post_url")),
        "owner": _text(row.get("worker")) or _text(row.get("author_name")),
        "match_source": MATCH_SOURCE_FLOW,
        "project_id": _text(row.get("project_id")),
    }


def _load_manual_links(manual_csv: Path | None = None) -> list[dict[str, str]]:
    manual_csv = Path(manual_csv or MARKETING_ADS_LINK_MANUAL_CSV)
    if not manual_csv.exists():
        logger.info("수동 보정 파일 없음(정상): %s", manual_csv)
        return []

    raw = _read_csv_with_fallback(manual_csv)
    if raw.empty:
        return []
    _validate_columns(raw, ["channel", "link_key"], "캠페인 보정 CSV")
    raw = raw.reindex(columns=MANUAL_LINK_COLS, fill_value="")
    return [{column: _text(row.get(column)) for column in MANUAL_LINK_COLS} for row in raw.to_dict("records")]


def _merge_manual_links(
    campaigns: list[dict[str, Any]],
    manual_csv: Path | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """수동 보정 행을 Flow 파싱 결과에 덮어쓰거나 새 캠페인으로 추가한다."""

    manual_rows = _load_manual_links(manual_csv)
    stats = {"overridden": 0, "added": 0}
    if not manual_rows:
        return campaigns, stats

    by_post_id = {campaign["flow_post_id"]: campaign for campaign in campaigns if campaign["flow_post_id"]}
    for row in manual_rows:
        channel = row["channel"]
        link_key = row["link_key"]
        if not channel or not link_key:
            logger.warning("보정 행 건너뜀(channel/link_key 누락): %s", row)
            continue

        target = by_post_id.get(row["flow_post_id"]) if row["flow_post_id"] else None
        if target is not None:
            target["channel"] = channel
            target["link_key"] = link_key
            for source_key, target_key in (
                ("project_name", "project_name"),
                ("start_date", "start_date"),
                ("end_date", "end_date"),
                ("status", "status"),
                ("store_name", "store_name"),
            ):
                value = row[source_key]
                if value:
                    target[target_key] = _to_date(value) if "date" in source_key else value
            target["match_source"] = MATCH_SOURCE_MANUAL
            stats["overridden"] += 1
            continue

        start_date = _to_date(row["start_date"])
        campaign_key = row["flow_post_id"] or f"manual|{channel}|{link_key}|{start_date}"
        campaigns.append(
            {
                "campaign_key": campaign_key,
                "channel": channel,
                "link_key": link_key,
                "project_name": row["project_name"] or link_key,
                "store_name": row["store_name"] or _guess_store(row["project_name"] or link_key),
                "status": row["status"],
                "start_date": start_date,
                "end_date": _to_date(row["end_date"]),
                "flow_post_id": row["flow_post_id"],
                "flow_url": "",
                "owner": "",
                "match_source": MATCH_SOURCE_MANUAL,
                "project_id": "",
            }
        )
        stats["added"] += 1

    return campaigns, stats


# ------------------------------------------------------------------
# 2. 채널별 일별 실적 로드 (원본 상세 그레인 유지)
# ------------------------------------------------------------------
def _load_naver_daily(
    naver_dir: Path | None = None,
    file_pattern: str | Iterable[str] = NAVER_ADS_FILE_PATTERNS,
) -> pd.DataFrame:
    """네이버: 신규 CSV는 일자 x 키워드, 과거 CSV는 일자 x 광고그룹 그레인."""

    naver_dir = Path(naver_dir or NAVER_ADS_DIR)
    files = _naver_source_files(naver_dir, file_pattern)
    if not files:
        logger.warning("네이버 광고 CSV 없음: %s / %s", naver_dir, file_pattern)
        return _empty_source_frame()

    raw = pd.concat([_read_csv_with_fallback(path) for path in files], ignore_index=True)
    _validate_columns(raw, NAVER_REQUIRED_COLS, "네이버 광고 CSV")

    rows: list[dict[str, Any]] = []
    for row in raw.to_dict("records"):
        stat_date = _to_date(row.get("collected_date"))
        campaign_id = _text(row.get("캠페인 ID"))
        if not stat_date or not campaign_id:
            continue
        adgroup_id = _text(row.get("광고그룹 ID"))
        adgroup_name = _text(row.get("광고그룹 이름"))
        keyword_id = _text(row.get("키워드 ID"))
        keyword_name = _text(row.get("키워드"))
        creative_id = _text(row.get("소재 ID"))
        creative_name = _text(row.get("소재"))
        depth_fields = _naver_depth_fields(row)
        if keyword_name:
            ad_id = keyword_id or "::".join(part for part in [campaign_id, adgroup_id, keyword_name] if part)
            ad_name = keyword_name
            ad_level = AD_LEVEL_KEYWORD
        elif creative_name or creative_id:
            ad_id = creative_id or "::".join(part for part in [campaign_id, adgroup_id, creative_name] if part)
            ad_name = creative_name or creative_id
            ad_level = AD_LEVEL_CREATIVE
        else:
            # 과거 네이버 광고그룹 CSV 호환: 키워드 컬럼이 없으면 광고그룹을 최소 단위로 유지한다.
            ad_id = adgroup_id or campaign_id
            ad_name = adgroup_name or _text(row.get("캠페인 이름"))
            ad_level = AD_LEVEL_ADGROUP if adgroup_id else AD_LEVEL_CAMPAIGN
        impressions = _to_number(row.get("노출수"))
        status_raw = _text(row.get("상태"))
        source_row = {
            "stat_date": stat_date,
            "channel": CHANNEL_NAVER,
            "ad_type": _text(row.get("광고유형")) or _naver_ad_type(campaign_id),
            "campaign_id": campaign_id,
            "campaign_id_kind": ID_KIND_ID,
            "campaign_name": _text(row.get("캠페인 이름")),
            "adgroup_id": adgroup_id,
            "adgroup_name": adgroup_name,
            "ad_id": ad_id,
            "ad_name": ad_name,
            "ad_level": ad_level,
            "status_raw": status_raw,
            "status_class": _status_class(status_raw, impressions),
            "link_level": LINK_LEVEL_CAMPAIGN,
            "link_key": campaign_id,
            "source_store": _text(row.get("store")),
            "impressions": impressions,
            "clicks": _to_number(row.get("클릭수")),
            "cost": _to_number(row.get("총비용")),
            "url": _naver_collect_url(row, campaign_id, stat_date),
        }
        source_row.update(depth_fields)
        rows.append(source_row)

    daily = pd.DataFrame(rows, columns=SOURCE_COLS)
    # 동일 일자/캠페인/광고그룹/최소단위 중복 수집분은 마지막 행만 남긴다.
    daily = daily.drop_duplicates(subset=["stat_date", "campaign_id", "adgroup_id", "ad_id"], keep="last")
    # 위 drop_duplicates는 stat_date가 같은 중복만 잡는다.
    # 날짜 필터 미적용으로 stat_date만 다른 복제본은 여기서 경고한다.
    _warn_duplicate_metric_dates(daily, "네이버 광고 원본")
    logger.info("네이버 광고 일별 실적 %s행 (파일 %s개)", len(daily), len(files))
    return daily


def _load_daangn_daily(csv_path: Path | None = None) -> pd.DataFrame:
    """당근: 일자 x 광고소재(dg_*) 그레인. 광고그룹명을 캠페인 레벨로 올린다."""

    csv_path = Path(csv_path or DAANGN_ADS_CSV_PATH)
    if not csv_path.exists():
        logger.warning("당근 광고 CSV 없음: %s", csv_path)
        return _empty_source_frame()

    raw = _read_csv_with_fallback(csv_path)
    _validate_columns(raw, DAANGN_REQUIRED_COLS, "당근 광고 CSV")

    rows: list[dict[str, Any]] = []
    for row in raw.to_dict("records"):
        stat_date = _to_date(row.get("시작일"))
        group_name = _text(row.get("광고그룹명"))
        if not stat_date or not group_name:
            continue
        impressions = _to_number(row.get("노출수"))
        status_raw = _text(row.get("상태"))
        depth_fields = _daangn_depth_fields(row)
        source_row = {
            "stat_date": stat_date,
            "channel": CHANNEL_DAANGN,
            "ad_type": _text(row.get("게재위치")),
            # 당근은 캠페인 ID를 노출하지 않아 광고그룹명이 캠페인 레벨의 식별자 겸 이름이다.
            # 이름을 ID 자리에 쓰는 것이므로 campaign_id_kind로 명시한다.
            "campaign_id": group_name,
            "campaign_id_kind": ID_KIND_NAME,
            "campaign_name": group_name,
            # 당근에는 네이버식 광고그룹 레벨이 없어 기존 adgroup 컬럼은 비워 둔다.
            "adgroup_id": "",
            "adgroup_name": "",
            # 원본 CSV의 campaign_id는 실제로는 광고(소재) ID다.
            "ad_id": _text(row.get("campaign_id")),
            "ad_name": _text(row.get("광고명")),
            "ad_level": AD_LEVEL_AD,
            "status_raw": status_raw,
            "status_class": _status_class(status_raw, impressions),
            "link_level": LINK_LEVEL_CAMPAIGN,
            "link_key": group_name,
            "source_store": _guess_store(group_name),
            "impressions": impressions,
            "clicks": _to_number(row.get("클릭수")),
            "cost": _to_number(row.get("지출")),
            "url": _text(row.get("url")),
        }
        source_row.update(depth_fields)
        rows.append(source_row)

    daily = pd.DataFrame(rows, columns=SOURCE_COLS)
    # 동일 일자/소재 중복 수집분은 마지막 행만 남긴다.
    daily = daily.drop_duplicates(subset=["stat_date", "ad_id"], keep="last")
    logger.info("당근 광고 일별 실적 %s행", len(daily))
    return daily


def _empty_source_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=SOURCE_COLS)


# ------------------------------------------------------------------
# 3. 실적 - 프로젝트 연결
# ------------------------------------------------------------------
def _attach_project(daily: pd.DataFrame, campaigns: list[dict[str, Any]]) -> pd.DataFrame:
    index: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for campaign in campaigns:
        index.setdefault((campaign["channel"], campaign["link_key"]), []).append(campaign)
    for bucket in index.values():
        bucket.sort(key=lambda item: (item["start_date"] or "0000-00-00", item["campaign_key"]))

    overlap_keys: set[str] = set()
    rows: list[dict[str, Any]] = []
    for row in daily.to_dict("records"):
        key = (row["channel"], row["link_key"])
        bucket = index.get(key, [])
        matched = [item for item in bucket if _in_period(row["stat_date"], item)]
        if len(matched) > 1:
            overlap_keys.add(f"{key[0]}|{key[1]}")
        campaign = matched[0] if matched else None

        if campaign:
            project_name = campaign["project_name"]
            store_name = campaign["store_name"] or row["source_store"]
        elif bucket:
            project_name = OUT_OF_PERIOD_LABEL
            store_name = row["source_store"] or bucket[0]["store_name"]
        else:
            project_name = UNREGISTERED_LABEL
            store_name = row["source_store"]

        merged = {column: row[column] for column in SOURCE_COLS if column != "source_store"}
        merged.update(
            {
                "campaign_key": campaign["campaign_key"] if campaign else "",
                "project_name": project_name,
                "store_name": store_name,
                "project_status": campaign["status"] if campaign else "",
                "start_date": campaign["start_date"] if campaign else "",
                "end_date": campaign["end_date"] if campaign else "",
                "in_period": bool(campaign),
                "overlap_flag": len(matched) > 1,
                "flow_url": campaign["flow_url"] if campaign else "",
            }
        )
        rows.append(merged)

    if overlap_keys:
        logger.warning(
            "기간이 겹치는 캠페인 등록 발견(가장 빠른 시작일 프로젝트에만 연결): %s",
            ", ".join(sorted(overlap_keys)),
        )

    columns = [column for column in DAILY_COLS if column not in {"ctr", "cpc", "collected_at"}]
    return pd.DataFrame(rows, columns=columns)


def _in_period(stat_date: str, campaign: dict[str, Any]) -> bool:
    start = campaign["start_date"]
    end = campaign["end_date"]
    if start and stat_date < start:
        return False
    if end and stat_date > end:
        return False
    return True


# ------------------------------------------------------------------
# 4. 시각화용 마트 (원본 상세 그레인)
# ------------------------------------------------------------------
def build_ads_daily_mart(
    payload: dict[str, Any] | None = None,
    naver_dir: Path | None = None,
    daangn_csv: Path | None = None,
    output_path: Path | None = None,
    **context: Any,
) -> str:
    payload = payload or {}
    campaigns = payload.get("campaigns") or []
    output_path = Path(output_path or MARKETING_ADS_DAILY_CSV)

    daily = pd.concat([_load_naver_daily(naver_dir), _load_daangn_daily(daangn_csv)], ignore_index=True)
    if daily.empty:
        raise ValueError("네이버/당근 광고 실적 원천이 모두 비어 있습니다.")

    attached = _with_rates(_attach_project(daily, campaigns))
    attached["collected_at"] = payload.get("collected_at") or _now_iso()
    attached["flow_task_label"] = ""
    attached = attached[DAILY_COLS].sort_values(
        ["stat_date", "channel", "campaign_id", "adgroup_id", "ad_id"]
    )
    _write_csv_atomic(attached, output_path)

    result = {
        "dataset_name": "marketing_ads_daily",
        "output_path": str(output_path),
        "rows": len(attached),
        "campaigns": len(campaigns),
        "matched_rows": int(attached["in_period"].sum()),
        "unmatched_rows": int((~attached["in_period"]).sum()),
        "overlap_rows": int(attached["overlap_flag"].sum()),
        "data_start": attached["stat_date"].min(),
        "data_end": attached["stat_date"].max(),
        "cost_total": round(float(attached["cost"].sum()), 2),
        "unmatched_cost": round(
            float(attached.loc[~attached["in_period"], "cost"].sum()), 2
        ),
    }
    logger.info("광고 일별 마트 저장 완료: %s", json.dumps(result, ensure_ascii=False))
    return json.dumps(result, ensure_ascii=False)


# ------------------------------------------------------------------
# 5. 캠페인 테이블 (Flow 매칭용 광고 계층 ID 매핑)
# ------------------------------------------------------------------
def build_campaign_table(
    payload: dict[str, Any] | None = None,
    daily_path: Path | None = None,
    output_path: Path | None = None,
    **context: Any,
) -> str:
    payload = payload or {}
    campaigns = payload.get("campaigns") or []
    daily_path = Path(daily_path or MARKETING_ADS_DAILY_CSV)
    output_path = Path(output_path or MARKETING_ADS_CAMPAIGN_CSV)
    collected_at = payload.get("collected_at") or _now_iso()

    daily = _read_daily_for_campaign(daily_path)
    if not campaigns and not daily.empty:
        campaigns = _source_campaigns_from_daily(daily, collected_at)
    rows = []
    for campaign in campaigns:
        rows.extend(_campaign_mapping_rows(campaign, daily, collected_at))
    table = pd.DataFrame(rows, columns=CAMPAIGN_COLS)
    if len(table):
        table = table[CAMPAIGN_COLS].sort_values(["start_date", "channel", "id", "name_01", "name_02", "name_03"])
    _write_csv_atomic(table, output_path)

    result = {
        "dataset_name": "marketing_ads_campaign",
        "output_path": str(output_path),
        "rows": len(table),
        "ids": int(table["id"].nunique()) if len(table) else 0,
        "campaigns": len(campaigns),
    }
    if not len(table):
        logger.warning("광고 ID 매핑 대상이 없어 캠페인 테이블을 헤더만 저장했습니다: %s", output_path)
    logger.info("광고 ID 매핑 테이블 저장 완료: %s", json.dumps(result, ensure_ascii=False))
    return json.dumps(result, ensure_ascii=False)


def _campaign_mapping_rows(
    campaign: dict[str, Any],
    daily: pd.DataFrame,
    collected_at: str,
) -> list[dict[str, Any]]:
    if daily.empty:
        rows = daily
    else:
        mask = (daily["channel"] == campaign["channel"]) & (daily["link_key"] == campaign["link_key"])
        if campaign.get("match_source") != MATCH_SOURCE_SOURCE:
            mask = mask & (daily["campaign_key"] == campaign["campaign_key"])
        rows = daily[mask]

    if rows.empty:
        return [_fallback_campaign_mapping_row(campaign)]

    mapped: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    for row in rows.to_dict("records"):
        depth = _max_depth(row)
        if not depth:
            continue

        item = {
            "channel": _text(row.get("channel")),
            "id": _text(row.get(f"depth{depth}_id")),
            "std_name": _text(row.get(f"depth{depth}_name")),
            "name_01": _text(row.get("depth1_name")),
            "name_02": _text(row.get("depth2_name")),
            "name_03": _text(row.get("depth3_name")),
            "leaf_depths": str(depth),
            "ad_type": _text(row.get("ad_type")),
            "start_date": _text(campaign.get("start_date")) or _text(row.get("stat_date")),
            "end_date": _text(campaign.get("end_date")) or _text(row.get("stat_date")),
        }
        if not item["id"]:
            continue

        key = (
            item["channel"],
            item["id"],
            item["name_01"],
            item["name_02"],
            item["name_03"],
            item["ad_type"],
        )
        existing = mapped.get(key)
        if existing is None:
            mapped[key] = item
            continue
        if not campaign.get("start_date"):
            existing["start_date"] = min(existing["start_date"], _text(row.get("stat_date")))
        if not campaign.get("end_date"):
            existing["end_date"] = max(existing["end_date"], _text(row.get("stat_date")))

    if not mapped:
        return [_fallback_campaign_mapping_row(campaign)]
    return list(mapped.values())


def _max_depth(row: dict[str, Any]) -> int:
    for depth in (3, 2, 1):
        if _text(row.get(f"depth{depth}_id")):
            return depth
    return 0


def _fallback_campaign_mapping_row(campaign: dict[str, Any]) -> dict[str, Any]:
    link_key = _text(campaign.get("link_key"))
    return {
        "channel": _text(campaign.get("channel")),
        "id": link_key,
        "std_name": _text(campaign.get("project_name")) or link_key,
        "name_01": _text(campaign.get("project_name")) or link_key,
        "name_02": "",
        "name_03": "",
        "leaf_depths": "1",
        "ad_type": "",
        "start_date": _text(campaign.get("start_date")),
        "end_date": _text(campaign.get("end_date")),
    }


def build_flow_ad_performance_mart(
    flow_post_dir: Path | None = None,
    campaign_path: Path | None = None,
    daily_path: Path | None = None,
    output_path: Path | None = None,
    project_id: str = FLOW_AD_PROJECT_ID,
    **context: Any,
) -> str:
    flow_post_dir = Path(flow_post_dir or FLOW_POST_PARQUET)
    campaign_path = Path(campaign_path or MARKETING_ADS_CAMPAIGN_CSV)
    daily_path = Path(daily_path or MARKETING_ADS_DAILY_CSV)
    output_path = Path(output_path or MARKETING_ADS_FLOW_COMPARE_CSV)
    collected_at = _now_iso()

    flow_posts = _read_flow_posts_for_project(flow_post_dir, project_id)
    campaigns = _read_campaign_id_map(campaign_path)
    daily = _daily_with_flow_match_id(_read_daily_for_campaign(daily_path))

    tasks = _flow_ad_tasks(flow_posts, campaigns, project_id)
    rows = [
        _flow_compare_row(task, campaigns[task["id"]], daily, collected_at)
        for task in tasks
    ]
    table = pd.DataFrame(rows, columns=FLOW_COMPARE_COLS)
    if len(table):
        table = table[FLOW_COMPARE_COLS].sort_values(["start_date", "channel", "id", "flow_post_id"])
    _write_csv_atomic(table, output_path)

    result = {
        "dataset_name": "marketing_ads_flow_compare",
        "output_path": str(output_path),
        "rows": len(table),
        "project_id": project_id,
        "ids": int(table["id"].nunique()) if len(table) else 0,
    }
    logger.info("Flow 광고 성과 비교 마트 저장 완료: %s", json.dumps(result, ensure_ascii=False))
    return json.dumps(result, ensure_ascii=False)


def annotate_ads_daily_with_flow_tasks(
    daily_path: Path | None = None,
    flow_compare_path: Path | None = None,
    campaign_path: Path | None = None,
    daily_flow_tasks_path: Path | None = None,
    output_path: Path | None = None,
    **context: Any,
) -> str:
    daily_path = Path(daily_path or MARKETING_ADS_DAILY_CSV)
    flow_compare_path = Path(flow_compare_path or MARKETING_ADS_FLOW_COMPARE_CSV)
    campaign_path = Path(campaign_path or MARKETING_ADS_CAMPAIGN_CSV)
    daily_flow_tasks_path = Path(daily_flow_tasks_path or MARKETING_ADS_DAILY_FLOW_TASKS_CSV)
    output_path = Path(output_path or daily_path)
    collected_at = _now_iso()

    daily = _read_daily_for_campaign(daily_path)
    flow_compare = _read_csv_with_fallback(flow_compare_path) if flow_compare_path.exists() else pd.DataFrame(columns=FLOW_COMPARE_COLS)
    campaigns = _read_campaign_id_map(campaign_path)

    flow_tasks = _expand_flow_tasks_by_date(flow_compare)
    _write_csv_atomic(flow_tasks, daily_flow_tasks_path)

    annotated = _annotate_daily_rows(daily, flow_tasks, campaigns, collected_at)
    _write_csv_atomic(annotated, output_path)

    result = {
        "dataset_name": "marketing_ads_daily_flow_tasks",
        "output_path": str(daily_flow_tasks_path),
        "daily_output_path": str(output_path),
        "task_rows": len(flow_tasks),
        "daily_rows": len(annotated),
        "placeholder_rows": int((annotated["status_class"] == "flow_schedule_only").sum()) if len(annotated) else 0,
        "tagged_rows": int(_series_text(annotated, "flow_task_label").ne("").sum()) if len(annotated) else 0,
    }
    logger.info("광고 일별 Flow 업무 태그 저장 완료: %s", json.dumps(result, ensure_ascii=False))
    return json.dumps(result, ensure_ascii=False)


def _read_flow_posts_for_project(flow_post_dir: Path, project_id: str) -> pd.DataFrame:
    if not flow_post_dir.exists():
        raise RuntimeError(f"Flow 게시글 경로가 없습니다: {flow_post_dir}")
    posts = pd.read_parquet(flow_post_dir)
    if posts.empty:
        raise RuntimeError(f"Flow 게시글 데이터가 비어 있습니다: {flow_post_dir}")
    posts = posts.reindex(columns=[
        "project_id",
        "project_name",
        "post_id",
        "parent_post_id",
        "depth",
        "title",
        "task_nm",
        "task_status",
        "start_dt",
        "end_dt",
        "post_url",
        "collected_at",
    ], fill_value="")
    project_posts = posts[_series_text(posts, "project_id").eq(project_id)].copy()
    if project_posts.empty:
        raise RuntimeError(f"Flow 광고 프로젝트 게시글이 없습니다: project_id={project_id}")
    return project_posts


def _read_campaign_id_map(campaign_path: Path) -> dict[str, dict[str, Any]]:
    if not campaign_path.exists():
        raise RuntimeError(f"광고 ID 매핑 CSV가 없습니다: {campaign_path}")
    raw = _read_csv_with_fallback(campaign_path)
    _validate_columns(raw, ["channel", "id", "std_name"], "광고 ID 매핑 CSV")
    campaigns: dict[str, dict[str, Any]] = {}
    for row in raw.fillna("").to_dict("records"):
        ad_id = _text(row.get("id"))
        if not ad_id or ad_id in campaigns:
            continue
        campaigns[ad_id] = {key: _text(value) for key, value in row.items()}
    if not campaigns:
        raise RuntimeError(f"광고 ID 매핑 CSV에 id가 없습니다: {campaign_path}")
    return campaigns


def _daily_with_flow_match_id(daily: pd.DataFrame) -> pd.DataFrame:
    daily = daily.copy()
    if daily.empty:
        daily["_flow_match_id"] = ""
        return daily
    daily["_flow_match_id"] = daily.apply(lambda row: _flow_match_id(row.to_dict()), axis=1)
    return daily


def _flow_match_id(row: dict[str, Any]) -> str:
    for depth in (3, 2, 1):
        value = _text(row.get(f"depth{depth}_id"))
        if value:
            return value
    return ""


def _flow_ad_tasks(
    posts: pd.DataFrame,
    campaigns: dict[str, dict[str, Any]],
    project_id: str,
) -> list[dict[str, Any]]:
    parent_channels = _flow_ad_parent_channels(posts)
    if not parent_channels:
        raise RuntimeError(
            f"Flow 광고 대분류 업무를 찾지 못했습니다: project_id={project_id} "
            f"parents={json.dumps(FLOW_AD_PARENT_BY_CHANNEL, ensure_ascii=False)}"
        )

    errors: list[str] = []
    tasks: list[dict[str, Any]] = []
    valid_ids = set(campaigns)
    for row in posts.to_dict("records"):
        parent_post_id = _text(row.get("parent_post_id"))
        channel = parent_channels.get(parent_post_id)
        if not channel:
            continue

        title = _text(row.get("title")) or _text(row.get("task_nm"))
        ad_id, candidates = _extract_flow_ad_id(title, valid_ids)
        start_date = _to_date(row.get("start_dt"))
        end_date = _to_date(row.get("end_dt"))
        missing_fields = []
        if not ad_id:
            missing_fields.append(f"id 미매칭(candidates={candidates})")
        elif _text(campaigns[ad_id].get("channel")) != channel:
            missing_fields.append(f"id 채널 불일치(id={ad_id}, campaign_channel={_text(campaigns[ad_id].get('channel'))}, parent_channel={channel})")
        if not start_date:
            missing_fields.append("start_dt")
        if not end_date:
            missing_fields.append("end_dt")
        if missing_fields:
            errors.append(
                f"post_id={_text(row.get('post_id'))} title={title!r} missing={','.join(missing_fields)}"
            )
            continue

        parent = parent_channels[parent_post_id]
        channel_warning = _flow_channel_warning(title, parent)
        tasks.append(
            {
                "project_id": project_id,
                "project_name": _text(row.get("project_name")) or FLOW_AD_PROJECT_NAME,
                "parent_post_id": parent_post_id,
                "parent_title": _flow_parent_title(posts, parent_post_id),
                "flow_post_id": _text(row.get("post_id")),
                "flow_url": _text(row.get("post_url")),
                "channel": channel,
                "id": ad_id,
                "std_title": _flow_std_title(title, ad_id),
                "task_title": title,
                "channel_warning": channel_warning,
                "start_date": start_date,
                "end_date": end_date,
            }
        )

    if errors:
        raise RuntimeError("Flow 광고 하위업무 필수값 오류: " + " | ".join(errors[:10]))
    if not tasks:
        raise RuntimeError(f"Flow 광고 하위업무가 없습니다: project_id={project_id}")
    return tasks


def _flow_ad_parent_channels(posts: pd.DataFrame) -> dict[str, str]:
    channels: dict[str, str] = {}
    for row in posts.to_dict("records"):
        if _text(row.get("parent_post_id")):
            continue
        post_id = _text(row.get("post_id"))
        title = _text(row.get("title")) or _text(row.get("task_nm"))
        url = _text(row.get("post_url"))
        for channel, meta in FLOW_AD_PARENT_BY_CHANNEL.items():
            if (
                post_id == meta["post_id"]
                or meta["url_code"] in url
                or _normalize_flow_label(title) == _normalize_flow_label(meta["title"])
            ):
                channels[post_id] = channel
    return channels


def _flow_parent_title(posts: pd.DataFrame, parent_post_id: str) -> str:
    parent = posts[_series_text(posts, "post_id").eq(parent_post_id)]
    if parent.empty:
        return ""
    row = parent.iloc[0]
    return _text(row.get("title")) or _text(row.get("task_nm"))


def _extract_flow_ad_id(title: str, valid_ids: set[str]) -> tuple[str, list[str]]:
    candidates = [_text(candidate) for candidate in re.findall(r"[\[(（]\s*([^\]\)）]+?)\s*[\])）]", title)]
    for candidate in candidates:
        if candidate in valid_ids:
            return candidate, candidates
    for ad_id in sorted(valid_ids, key=len, reverse=True):
        if ad_id and ad_id in title:
            return ad_id, candidates
    return "", candidates


def _flow_channel_warning(title: str, parent_channel: str) -> str:
    title_channel = _title_channel_hint(title)
    if title_channel and title_channel != parent_channel:
        return f"제목 채널={title_channel}, 부모 채널={parent_channel}"
    return ""


def _title_channel_hint(title: str) -> str:
    label = _normalize_flow_label(title)
    if "당근" in label:
        return CHANNEL_DAANGN
    if "네이버" in label or "네ㅇ버" in label:
        return CHANNEL_NAVER
    return ""


def _normalize_flow_label(value: Any) -> str:
    return re.sub(r"[\s_\[\]\(\)（）]", "", _text(value))


def _flow_compare_row(
    task: dict[str, Any],
    campaign: dict[str, Any],
    daily: pd.DataFrame,
    collected_at: str,
) -> dict[str, Any]:
    prev_start, prev_end = _previous_period(task["start_date"], task["end_date"])
    current = _summarize_flow_period(daily, task["channel"], task["id"], task["start_date"], task["end_date"])
    previous = _summarize_flow_period(daily, task["channel"], task["id"], prev_start, prev_end)
    return {
        "project_id": task["project_id"],
        "project_name": task["project_name"],
        "parent_post_id": task["parent_post_id"],
        "parent_title": task["parent_title"],
        "flow_post_id": task["flow_post_id"],
        "flow_url": task["flow_url"],
        "channel": task["channel"],
        "id": task["id"],
        "std_name": _text(campaign.get("std_name")),
        "std_title": task["std_title"],
        "task_title": task["task_title"],
        "channel_warning": task["channel_warning"],
        "start_date": task["start_date"],
        "end_date": task["end_date"],
        "prev_start_date": prev_start,
        "prev_end_date": prev_end,
        "impressions": current["impressions"],
        "clicks": current["clicks"],
        "ctr": current["ctr"],
        "cpc": current["cpc"],
        "cost": current["cost"],
        "prev_impressions": previous["impressions"],
        "prev_clicks": previous["clicks"],
        "prev_ctr": previous["ctr"],
        "prev_cpc": previous["cpc"],
        "prev_cost": previous["cost"],
        "diff_impressions": current["impressions"] - previous["impressions"],
        "diff_clicks": current["clicks"] - previous["clicks"],
        "diff_cost": current["cost"] - previous["cost"],
        "pct_impressions": _pct_change(current["impressions"], previous["impressions"]),
        "pct_clicks": _pct_change(current["clicks"], previous["clicks"]),
        "pct_cost": _pct_change(current["cost"], previous["cost"]),
        "matched_daily_rows": current["matched_daily_rows"],
        "collected_at": collected_at,
    }


def _previous_period(start_date: str, end_date: str) -> tuple[str, str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    days = (end - start).days + 1
    previous_end = start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=days - 1)
    return previous_start.isoformat(), previous_end.isoformat()


def _flow_std_title(title: str, ad_id: str) -> str:
    text = _text(title)
    text = re.sub(r"^[\s\[\(\（]*\s*(?:네이버|네ㅇ버|당근)\s*[_\s]*광고\s*[\]\)\）]*", "", text).strip()
    text = re.sub(rf"^[\s\[\(\（]*\s*{re.escape(ad_id)}\s*[\]\)\）]*", "", text).strip()
    text = re.sub(r"^[\s\[\]\(\)（）]+", "", text).strip()
    return text or _text(title)


def _flow_task_label(start_date: str, end_date: str, ad_id: str, std_title: str) -> str:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start.year == end.year and start.month == end.month:
        period = f"{start:%d}-{end:%d}"
    else:
        period = f"{start:%m-%d}~{end:%m-%d}"
    return f"{period} [{ad_id}] {_text(std_title)}".strip()


def _expand_flow_tasks_by_date(flow_compare: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if flow_compare.empty:
        return pd.DataFrame(rows, columns=DAILY_FLOW_TASK_COLS)
    flow_compare = flow_compare.reindex(columns=FLOW_COMPARE_COLS, fill_value="")
    for row in flow_compare.fillna("").to_dict("records"):
        start_date = _to_date(row.get("start_date"))
        end_date = _to_date(row.get("end_date"))
        ad_id = _text(row.get("id"))
        if not start_date or not end_date or not ad_id:
            continue
        std_title = _text(row.get("std_title")) or _flow_std_title(_text(row.get("task_title")), ad_id)
        label = _flow_task_label(start_date, end_date, ad_id, std_title)
        for stat_date in pd.date_range(start_date, end_date, freq="D"):
            rows.append(
                {
                    "stat_date": stat_date.strftime("%Y-%m-%d"),
                    "channel": _text(row.get("channel")),
                    "id": ad_id,
                    "std_name": _text(row.get("std_name")),
                    "flow_post_id": _text(row.get("flow_post_id")),
                    "flow_url": _text(row.get("flow_url")),
                    "parent_title": _text(row.get("parent_title")),
                    "std_title": std_title,
                    "flow_task_label": label,
                    "FLOW_LINK": _text(row.get("flow_url")),
                    "FLOW_TITLE": _text(row.get("task_title")),
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )
    table = pd.DataFrame(rows, columns=DAILY_FLOW_TASK_COLS)
    if len(table):
        table = table.sort_values(["stat_date", "channel", "id", "start_date", "end_date", "flow_post_id"])
    return table


def _annotate_daily_rows(
    daily: pd.DataFrame,
    flow_tasks: pd.DataFrame,
    campaigns: dict[str, dict[str, Any]],
    collected_at: str,
) -> pd.DataFrame:
    daily = daily.reindex(columns=DAILY_COLS, fill_value="").copy()
    if daily.empty and flow_tasks.empty:
        return daily
    for column in ("impressions", "clicks", "ctr", "cpc", "cost"):
        if column in daily.columns:
            daily[column] = daily[column].map(_to_number)

    daily = _daily_with_flow_match_id(daily)
    placeholder_rows = _flow_placeholder_rows(daily, flow_tasks, campaigns, collected_at)
    if placeholder_rows:
        daily = pd.concat([daily, pd.DataFrame(placeholder_rows)], ignore_index=True)
        daily = _daily_with_flow_match_id(daily)

    labels = _flow_task_labels_by_daily_key(flow_tasks)
    links = _flow_task_links_by_daily_key(flow_tasks)
    titles = _flow_task_titles_by_daily_key(flow_tasks)
    daily["flow_task_label"] = [
        labels.get((_text(row.get("stat_date")), _text(row.get("channel")), _text(row.get("_flow_match_id"))), "")
        for row in daily.to_dict("records")
    ]
    daily["FLOW_LINK"] = [
        links.get((_text(row.get("stat_date")), _text(row.get("channel")), _text(row.get("_flow_match_id"))), "")
        for row in daily.to_dict("records")
    ]
    daily["FLOW_TITLE"] = [
        titles.get((_text(row.get("stat_date")), _text(row.get("channel")), _text(row.get("_flow_match_id"))), "")
        for row in daily.to_dict("records")
    ]
    daily = daily.drop(columns=["_flow_match_id"], errors="ignore")
    daily = daily.reindex(columns=DAILY_COLS, fill_value="")
    if len(daily):
        daily = daily.sort_values(["stat_date", "channel", "campaign_id", "adgroup_id", "ad_id", "flow_task_label"])
    return daily


def _flow_task_labels_by_daily_key(flow_tasks: pd.DataFrame) -> dict[tuple[str, str, str], str]:
    labels: dict[tuple[str, str, str], str] = {}
    if flow_tasks.empty:
        return labels
    flow_tasks = flow_tasks.sort_values(["stat_date", "channel", "id", "start_date", "end_date", "flow_post_id"])
    for key, rows in flow_tasks.groupby(["stat_date", "channel", "id"], sort=False):
        values = []
        seen = set()
        for label in rows["flow_task_label"]:
            text = _text(label)
            if text and text not in seen:
                seen.add(text)
                values.append(text)
        labels[tuple(_text(part) for part in key)] = " | ".join(values)
    return labels


def _flow_task_links_by_daily_key(flow_tasks: pd.DataFrame) -> dict[tuple[str, str, str], str]:
    links: dict[tuple[str, str, str], str] = {}
    if flow_tasks.empty:
        return links
    flow_tasks = flow_tasks.sort_values(["stat_date", "channel", "id", "start_date", "end_date", "flow_post_id"])
    for key, rows in flow_tasks.groupby(["stat_date", "channel", "id"], sort=False):
        values = []
        seen = set()
        for link in rows["FLOW_LINK"]:
            text = _text(link)
            if text and text not in seen:
                seen.add(text)
                values.append(text)
        links[tuple(_text(part) for part in key)] = " | ".join(values)
    return links


def _flow_task_titles_by_daily_key(flow_tasks: pd.DataFrame) -> dict[tuple[str, str, str], str]:
    titles: dict[tuple[str, str, str], str] = {}
    if flow_tasks.empty:
        return titles
    flow_tasks = flow_tasks.sort_values(["stat_date", "channel", "id", "start_date", "end_date", "flow_post_id"])
    for key, rows in flow_tasks.groupby(["stat_date", "channel", "id"], sort=False):
        values = []
        seen = set()
        for title in rows["FLOW_TITLE"]:
            text = _text(title)
            if text and text not in seen:
                seen.add(text)
                values.append(text)
        titles[tuple(_text(part) for part in key)] = " | ".join(values)
    return titles


def _flow_placeholder_rows(
    daily: pd.DataFrame,
    flow_tasks: pd.DataFrame,
    campaigns: dict[str, dict[str, Any]],
    collected_at: str,
) -> list[dict[str, Any]]:
    if flow_tasks.empty:
        return []
    existing = {
        (_text(row.get("stat_date")), _text(row.get("channel")), _text(row.get("_flow_match_id")))
        for row in daily.to_dict("records")
    }
    placeholders: list[dict[str, Any]] = []
    for task in flow_tasks.to_dict("records"):
        key = (_text(task.get("stat_date")), _text(task.get("channel")), _text(task.get("id")))
        if key in existing:
            continue
        row = _flow_placeholder_row(daily, campaigns.get(key[2], {}), task, collected_at)
        placeholders.append(row)
        existing.add(key)
    return placeholders


def _flow_placeholder_row(
    daily: pd.DataFrame,
    campaign: dict[str, Any],
    task: dict[str, Any],
    collected_at: str,
) -> dict[str, Any]:
    channel = _text(task.get("channel"))
    ad_id = _text(task.get("id"))
    template = daily[(daily["channel"] == channel) & (daily["_flow_match_id"] == ad_id)]
    if not template.empty:
        row = {column: _text(value) for column, value in template.iloc[0].to_dict().items() if column in DAILY_COLS}
    else:
        row = {column: "" for column in DAILY_COLS}
        depth = int(_to_number(campaign.get("leaf_depths"))) if _text(campaign.get("leaf_depths")) else 1
        row.update(
            {
                "channel": channel,
                "ad_type": _text(campaign.get("ad_type")),
                "depth1_id": _text(campaign.get("id")) if depth == 1 else _text(campaign.get("name_01")),
                "depth1_name": _text(campaign.get("name_01")) or _text(campaign.get("std_name")),
                "depth2_id": _text(campaign.get("id")) if depth == 2 else "",
                "depth2_name": _text(campaign.get("name_02")) if depth >= 2 else "",
                "depth3_id": _text(campaign.get("id")) if depth == 3 else "",
                "depth3_name": _text(campaign.get("name_03")) if depth >= 3 else "",
                "leaf_depth": str(depth),
                "leaf_id": ad_id,
                "leaf_name": _text(campaign.get("std_name")) or ad_id,
            }
        )
        if channel == CHANNEL_DAANGN:
            row["campaign_id_kind"] = "name"
            row["campaign_id"] = row["depth1_id"]
            row["campaign_name"] = row["depth1_name"]
            row["ad_id"] = ad_id
            row["ad_name"] = row["leaf_name"]
            row["ad_level"] = "ad"
        else:
            row["campaign_id_kind"] = "id"
            row["campaign_id"] = row["depth1_id"]
            row["campaign_name"] = row["depth1_name"]
            row["ad_id"] = ad_id
            row["ad_name"] = row["leaf_name"]
    row.update(
        {
            "stat_date": _text(task.get("stat_date")),
            "channel": channel,
            "project_name": FLOW_AD_PROJECT_NAME,
            "start_date": _text(task.get("start_date")),
            "end_date": _text(task.get("end_date")),
            "in_period": "True",
            "overlap_flag": "False",
            "status_raw": "flow_schedule_only",
            "status_class": "flow_schedule_only",
            "impressions": 0.0,
            "clicks": 0.0,
            "ctr": 0.0,
            "cpc": 0.0,
            "cost": 0.0,
            "flow_url": _text(task.get("flow_url")),
            "collected_at": collected_at,
            "flow_task_label": _text(task.get("flow_task_label")),
            "FLOW_LINK": _text(task.get("FLOW_LINK")),
            "FLOW_TITLE": _text(task.get("FLOW_TITLE")),
        }
    )
    return {column: row.get(column, "") for column in DAILY_COLS}


def _summarize_flow_period(
    daily: pd.DataFrame,
    channel: str,
    ad_id: str,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    if daily.empty:
        rows = daily
    else:
        rows = daily[
            (daily["channel"] == channel)
            & (daily["_flow_match_id"] == ad_id)
            & (daily["stat_date"] >= start_date)
            & (daily["stat_date"] <= end_date)
        ]
    impressions = round(float(rows["impressions"].sum()), 2) if len(rows) else 0.0
    clicks = round(float(rows["clicks"].sum()), 2) if len(rows) else 0.0
    cost = round(float(rows["cost"].sum()), 2) if len(rows) else 0.0
    return {
        "impressions": impressions,
        "clicks": clicks,
        "ctr": round(_safe_div(clicks, impressions) * 100, 2),
        "cpc": round(_safe_div(cost, clicks), 2),
        "cost": cost,
        "matched_daily_rows": int(len(rows)),
    }


def _pct_change(current: float, previous: float) -> float | str:
    if previous == 0:
        return ""
    return round((current - previous) / previous * 100, 2)


def _series_text(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype=str)
    return df[column].astype("string").fillna("").astype(str)


def _source_campaigns_from_daily(daily: pd.DataFrame, collected_at: str) -> list[dict[str, Any]]:
    """Flow 등록 전에도 원천 광고 코드별 1행 campaign 테이블을 만든다."""
    campaigns: list[dict[str, Any]] = []
    if daily.empty:
        return campaigns

    grouped = daily.sort_values(["stat_date", "channel", "link_key"]).groupby(["channel", "link_key"], sort=True)
    for (channel, link_key), rows in grouped:
        if not _text(link_key):
            continue
        data_start = _text(rows["stat_date"].min())
        data_end = _text(rows["stat_date"].max())
        project_name = _last_value(rows, "campaign_name") or link_key
        campaigns.append(
            {
                "campaign_key": f"source|{channel}|{link_key}",
                "channel": channel,
                "link_key": link_key,
                "project_name": project_name,
                "store_name": _last_value(rows, "store_name") or _last_value(rows, "source_store"),
                "status": "",
                "start_date": data_start,
                "end_date": data_end,
                "flow_post_id": "",
                "flow_url": "",
                "owner": "",
                "match_source": MATCH_SOURCE_SOURCE,
                "project_id": "",
                "collected_at": collected_at,
            }
        )
    return campaigns


def _read_daily_for_campaign(daily_path: Path) -> pd.DataFrame:
    if not daily_path.exists():
        logger.warning("일별 마트 파일 없음: %s", daily_path)
        return pd.DataFrame(columns=DAILY_COLS)

    daily = pd.read_csv(daily_path, dtype=str, encoding="utf-8-sig").fillna("")
    if daily.empty:
        return daily
    for column in ("impressions", "clicks", "cost"):
        daily[column] = daily[column].map(_to_number)
    return daily


def _channel_calendar(daily: pd.DataFrame) -> dict[str, set[str]]:
    """채널별로 수집이 존재하는 날짜 집합. 결측(수집 없음) 판정의 기준이 된다."""
    if daily.empty:
        return {}
    if "status_class" in daily.columns:
        daily = daily[_series_text(daily, "status_class") != "flow_schedule_only"]
    return {
        channel: set(group["stat_date"])
        for channel, group in daily.groupby("channel")
    }


def _campaign_row(
    campaign: dict[str, Any],
    daily: pd.DataFrame,
    channel_calendar: dict[str, set[str]],
    collected_at: str,
) -> dict[str, Any]:
    channel = campaign["channel"]
    channel_dates = channel_calendar.get(channel, set())

    if daily.empty:
        rows = daily
    else:
        mask = (daily["channel"] == channel) & (daily["link_key"] == campaign["link_key"])
        if campaign.get("match_source") != MATCH_SOURCE_SOURCE:
            mask = mask & (daily["campaign_key"] == campaign["campaign_key"])
        rows = daily[mask]

    impressions = float(rows["impressions"].sum()) if len(rows) else 0.0
    clicks = float(rows["clicks"].sum()) if len(rows) else 0.0
    cost = float(rows["cost"].sum()) if len(rows) else 0.0

    period_dates = _period_dates(campaign, channel_dates)
    active_dates = set(rows.loc[rows["impressions"] > 0, "stat_date"]) if len(rows) else set()
    budget_dates = (
        set(rows.loc[rows["status_class"] == STATUS_BUDGET_CAPPED, "stat_date"]) if len(rows) else set()
    )

    if CHANNEL_COVERAGE_MODE.get(channel, COVERAGE_SNAPSHOT) == COVERAGE_SNAPSHOT:
        missing_dates = [day for day in period_dates if day not in channel_dates]
    else:
        # event 채널은 행이 없는 날을 미집행으로 본다(결측 판정 불가)
        missing_dates = []
    active_days = sum(1 for day in period_dates if day in active_dates)
    missing_days = len(missing_dates)
    period_days = len(period_dates)
    inactive_days = period_days - active_days - missing_days
    budget_capped_days = sum(1 for day in period_dates if day in budget_dates)

    matched_dates = set(rows["stat_date"]) if len(rows) else set()
    return {
        "campaign_key": campaign["campaign_key"],
        "channel": channel,
        "link_key": campaign["link_key"],
        # link_key는 캠페인 레벨 키라 실적이 없어도 campaign_id를 채울 수 있다.
        "campaign_id": _last_value(rows, "campaign_id") or campaign["link_key"],
        "campaign_id_kind": _last_value(rows, "campaign_id_kind") or CHANNEL_ID_KIND.get(channel, ""),
        "campaign_name": _last_value(rows, "campaign_name"),
        "ad_type": _last_value(rows, "ad_type"),
        "ad_level": _last_value(rows, "ad_level") or CHANNEL_AD_LEVEL.get(channel, ""),
        "source_depth_levels": _unique_join(rows, "source_depth_level"),
        "depth1_id": _first_unique(rows, "depth1_id") or campaign["link_key"],
        "depth1_name": _first_unique(rows, "depth1_name") or _last_value(rows, "campaign_name") or campaign["link_key"],
        "depth2_ids": _unique_join(rows, "depth2_id"),
        "depth2_names": _unique_join(rows, "depth2_name"),
        "depth3_ids": _unique_join(rows, "depth3_id"),
        "depth3_names": _unique_join(rows, "depth3_name"),
        "leaf_depths": _unique_join(rows, "leaf_depth"),
        "leaf_ids": _unique_join(rows, "leaf_id"),
        "leaf_names": _unique_join(rows, "leaf_name"),
        "project_name": campaign["project_name"],
        "store_name": campaign["store_name"],
        "status": campaign["status"],
        "start_date": campaign["start_date"],
        "end_date": campaign["end_date"],
        "period_days": period_days,
        "active_days": active_days,
        "inactive_days": inactive_days,
        "missing_days": missing_days,
        "budget_capped_days": budget_capped_days,
        "coverage_rate": round(_safe_div(period_days - missing_days, period_days), 4) if period_days else "",
        "data_complete": ("완전" if missing_days == 0 else "불완전") if period_days else "",
        "missing_dates": _compress_dates(missing_dates),
        "data_start": min(matched_dates) if matched_dates else "",
        "data_end": max(matched_dates) if matched_dates else "",
        "adgroup_cnt": _nunique_non_empty(rows, "adgroup_id"),
        "ad_cnt": _nunique_non_empty(rows, "ad_id"),
        "impressions": impressions,
        "clicks": clicks,
        "ctr": 0.0,
        "cpc": 0.0,
        "cost": cost,
        "cost_per_day": round(_safe_div(cost, active_days), 2),
        "match_status": "매칭됨" if matched_dates else "실적없음",
        "match_source": campaign["match_source"],
        "flow_post_id": campaign["flow_post_id"],
        "flow_url": campaign["flow_url"],
        "owner": campaign["owner"],
        "collected_at": collected_at,
    }


def _period_dates(campaign: dict[str, Any], channel_dates: set[str]) -> list[str]:
    """프로젝트 기간 날짜 목록. 마감일이 비어 있으면 채널의 마지막 수집일까지로 본다."""
    start_date = campaign["start_date"]
    end_date = campaign["end_date"]
    if not start_date:
        return []
    if not end_date:
        if not channel_dates:
            return []
        end_date = max(channel_dates)
        if end_date < start_date:
            return []
    return _date_range(start_date, end_date)


def _last_value(rows: pd.DataFrame, column: str) -> str:
    if not len(rows):
        return ""
    values = [value for value in rows[column].map(_text) if value]
    return values[-1] if values else ""


def _unique_join(rows: pd.DataFrame, column: str) -> str:
    if not len(rows) or column not in rows.columns:
        return ""
    values = sorted({value for value in rows[column].map(_text) if value})
    return " | ".join(values)


def _first_unique(rows: pd.DataFrame, column: str) -> str:
    if not len(rows) or column not in rows.columns:
        return ""
    values = [value for value in rows[column].map(_text) if value]
    return values[0] if values else ""


def _nunique_non_empty(rows: pd.DataFrame, column: str) -> int:
    if not len(rows):
        return 0
    return len({value for value in rows[column].map(_text) if value})


# ------------------------------------------------------------------
# 6. 네이버 미수집 알림
# ------------------------------------------------------------------
def notify_missing_collection(
    daily_path: Path | None = None,
    state_path: Path | None = None,
    naver_dir: Path | None = None,
    alert_sender: Callable[[str], bool] | None = None,
    today: str | None = None,
    **context: Any,
) -> str:
    """네이버 수집 누락일을 감지해 텔레그램으로 알린다.

    네이버는 매일 전체 광고그룹을 내려주는 snapshot 소스라 그날 행이 없으면 수집 실패다.
    당근은 집행한 날만 행이 생기는 event 소스라 미수집과 미집행을 구분할 수 없어 대상이 아니며,
    Marketing_DaangnAds_CSV_Dags가 자체 누락일 알림을 담당한다.

    이미 알린 날짜는 state 파일에 남겨 재발송하지 않는다.
    """

    daily_path = Path(daily_path or MARKETING_ADS_DAILY_CSV)
    state_path = Path(state_path or MARKETING_ADS_ALERT_STATE_JSON)
    naver_dir = Path(naver_dir or NAVER_ADS_DIR)

    daily = _read_daily_for_campaign(daily_path)
    collected_dates = _channel_calendar(daily).get(CHANNEL_NAVER, set())
    expected_end = today or _yesterday()

    # 날짜가 다 차 있어도 내용이 복제본이면 "수집 정상"이 아니다.
    naver_daily = (
        daily[_series_text(daily, "channel") == CHANNEL_NAVER]
        if not daily.empty and "channel" in daily.columns
        else daily
    )
    duplicate_dates = _warn_duplicate_metric_dates(naver_daily, "네이버 광고 마트")

    missing_dates = _missing_collection_dates(collected_dates, expected_end)
    notified = set(_load_alert_state(state_path).get(CHANNEL_NAVER, {}).get("notified_dates", []))
    new_missing = [day for day in missing_dates if day not in notified]

    result: dict[str, Any] = {
        "channel": CHANNEL_NAVER,
        "expected_end": expected_end,
        "total_missing": len(missing_dates),
        "new_missing": new_missing,
        "duplicate_metric_dates": duplicate_dates,
        "telegram_sent": None,
    }
    if not new_missing:
        # 누락일이 없다고 정상이라는 뜻은 아니다. 복제본이 있으면 함께 남긴다.
        if duplicate_dates:
            logger.warning(
                "네이버 광고 수집 신규 누락일은 없으나 지표가 동일한 날짜 묶음 %s건 발견: %s",
                len(duplicate_dates),
                json.dumps(result, ensure_ascii=False),
            )
        else:
            logger.info("네이버 광고 수집 신규 누락일 없음: %s", json.dumps(result, ensure_ascii=False))
        return json.dumps(result, ensure_ascii=False)

    message = _missing_collection_message(
        collected_dates, expected_end, new_missing, missing_dates, naver_dir, daily_path
    )
    sender = alert_sender or _send_telegram_chunks
    try:
        sent = bool(sender(message))
    except Exception as exc:
        logger.warning("네이버 광고 수집 누락 텔레그램 발송 실패: %s", exc)
        sent = False

    result["telegram_sent"] = sent
    if sent:
        # 발송에 성공한 날짜만 기록해 실패 시 다음 실행에서 다시 시도한다.
        _save_alert_state(state_path, CHANNEL_NAVER, sorted(notified | set(new_missing)))
    logger.warning("네이버 광고 수집 누락 알림: %s", json.dumps(result, ensure_ascii=False))
    return json.dumps(result, ensure_ascii=False)


def _yesterday() -> str:
    return pendulum.now("Asia/Seoul").subtract(days=1).to_date_string()


def _duplicate_metric_dates(daily: pd.DataFrame) -> list[list[str]]:
    """지표까지 완전히 동일한 stat_date 묶음.

    날짜 필터가 걸리지 않은 채 같은 기간을 반복 수집하면 stat_date만 다른 복제본이 생긴다.
    stat_date가 서로 달라 drop_duplicates로는 잡히지 않고, 수집 누락 점검도
    "날짜가 다 있다"며 통과시키므로 여기서 따로 탐지한다.
    (2026-08 네이버 기간수집에서 26일치가 같은 데이터로 채워진 사고)
    """
    if daily.empty or "stat_date" not in daily.columns:
        return []
    metric_cols = [col for col in ("impressions", "clicks", "cost") if col in daily.columns]
    key_cols = [col for col in ("campaign_id", "adgroup_id", "ad_id") if col in daily.columns]
    if not metric_cols or not key_cols:
        return []

    fingerprints: dict[str, Any] = {}
    for stat_date, group in daily.groupby("stat_date"):
        metrics = group[metric_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        # 전부 0인 날은 서로 같아도 이상하지 않다(캠페인 중지 등).
        if metrics.to_numpy().sum() == 0:
            continue
        keyed = group[key_cols].astype(str).join(metrics).sort_values(key_cols)
        fingerprints[str(stat_date)] = pd.util.hash_pandas_object(keyed, index=False).sum()

    buckets: dict[Any, list[str]] = {}
    for stat_date, fingerprint in fingerprints.items():
        buckets.setdefault(fingerprint, []).append(stat_date)
    return sorted(
        (sorted(days) for days in buckets.values() if len(days) >= 2),
        key=lambda days: days[0],
    )


def _warn_duplicate_metric_dates(daily: pd.DataFrame, label: str) -> list[list[str]]:
    duplicates = _duplicate_metric_dates(daily)
    for days in duplicates:
        logger.warning(
            "%s: %s일이 지표까지 완전히 동일합니다(%s). "
            "수집 시 날짜 필터가 적용되지 않았을 수 있으니 해당 날짜를 재수집하세요.",
            label,
            len(days),
            _compress_dates(days),
        )
    return duplicates


def _missing_collection_dates(collected_dates: set[str], expected_end: str) -> list[str]:
    """첫 수집일 ~ 전일 사이에서 수집이 없는 날짜. 구간 공백과 수집 지연이 함께 잡힌다."""
    if not collected_dates:
        return []
    start = min(collected_dates)
    if expected_end < start:
        return []
    return [day for day in _date_range(start, expected_end) if day not in collected_dates]


def _missing_collection_message(
    collected_dates: set[str],
    expected_end: str,
    new_missing: list[str],
    missing_dates: list[str],
    naver_dir: Path,
    daily_path: Path,
) -> str:
    patterns = ", ".join(NAVER_ADS_FILE_PATTERNS)
    return "\n".join(
        [
            "[네이버 광고 수집 누락]",
            "",
            f"확인 범위: {min(collected_dates)} ~ {expected_end} (전일 기준)",
            f"신규 누락일: {_compress_dates(new_missing)}",
            f"누적 누락일: {len(missing_dates)}일",
            "",
            "네이버 검색광고에서 해당 날짜 실적을 받아 아래 경로에 채워주세요.",
            f"입력 위치: {naver_dir} ({patterns})",
            f"마트 위치: {daily_path}",
        ]
    )


def _load_alert_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        logger.warning("알림 state 파일 읽기 실패(빈 상태로 진행): %s - %s", state_path, exc)
        return {}


def _save_alert_state(state_path: Path, channel: str, notified_dates: list[str]) -> None:
    state = _load_alert_state(state_path)
    state[channel] = {"notified_dates": notified_dates, "updated_at": _now_iso()}
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = state_path.with_suffix(state_path.suffix + ".tmp")
    temp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp_path, state_path)


def _send_telegram_chunks(text: str) -> bool:
    from modules.transform.utility.notifier import send_telegram_chunks

    return send_telegram_chunks(text)
