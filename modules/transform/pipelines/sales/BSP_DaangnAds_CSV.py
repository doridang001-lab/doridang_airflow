"""
당근 광고 CSV 통합 파이프라인.

Collect_Data/마케팅_수집/daangn_ads_*.csv 파일을 모아
analytics/Daangn_ads/daangn_ads.csv 단일 파일로 저장한다.
동일 시작일/종료일/campaign_id 행은 collected_at 최신 수집본으로 덮어쓴다.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB

logger = logging.getLogger(__name__)

SOURCE_DIR = COLLECT_DB / "마케팅_수집"
FILE_PATTERN = "daangn_ads_*.csv"
OUTPUT_DIR = ANALYTICS_DB / "Daangn_ads"
OUTPUT_FILE = "daangn_ads.csv"
READ_ENCODINGS = ("utf-8-sig", "utf-8", "cp949")
MAIL_DAANGN_ADS_CHA = "melanie0204@kakao.com"

COLLECTED_AT_COL = "collected_at"
GROUP_COL = "광고그룹명"
AD_NAME_COL = "광고명"
URL_COL = "url"
IMAGE_URL_COL = "image_url"
DEDUP_COLS = ["시작일", "종료일", "campaign_id"]
KNOWN_AD_GROUP_URLS = {
    "도리당 송파삼전점 #3": "https://ads-lite.business.daangn.com/ad-groups/QWRHcm91cDoxNzgxMTM1NDQ3OTQxODUxMDAx/?filterType=ALL&startAt=2025-07-30T15%3A00%3A00.000Z&endAt=2026-08-31T14%3A59%3A59.999Z&groupConnectionId=client%3AQWR2ZXJ0aXNlcjozMDA0Mzc3%3A__adGroupList_adGroups_connection%28filter%3A%7B%22placementFilter%22%3A%5B%22ALL%22%5D%2C%22statusFilter%22%3A%5B%22ALL%22%5D%7D%29",
    "도리당 송파삼전점 #5": "https://ads-lite.business.daangn.com/ad-groups/QWRHcm91cDoxNzg2Njk2Mjc5NTgzNzU4MDAx/?filterType=ALL&startAt=2025-07-30T15%3A00%3A00.000Z&endAt=2026-08-31T14%3A59%3A59.999Z&groupConnectionId=client%3AQWR2ZXJ0aXNlcjozMDA0Mzc3%3A__adGroupList_adGroups_connection%28filter%3A%7B%22placementFilter%22%3A%5B%22ALL%22%5D%2C%22statusFilter%22%3A%5B%22ALL%22%5D%7D%29",
    "웹사이트 - 도리당 송파삼전점 플레이스 #1(08.06)": "https://ads-lite.business.daangn.com/ad-groups/QWRHcm91cDoxNzg1OTE3NjA1MjUxNzAxMDAx/",
}


def load_daangn_ads_csv(
    source_dir: Path | None = None,
    output_path: Path | None = None,
    file_pattern: str = FILE_PATTERN,
    cleanup_source: bool = True,
    alert_sender: Callable[[str], bool] | None = None,
    email_sender: Callable[[str, str, list[str]], object] | None = None,
) -> str:
    """당근 광고 CSV를 단일 CSV로 멱등 병합 저장한다."""

    source_dir = source_dir or SOURCE_DIR
    output_path = output_path or (OUTPUT_DIR / OUTPUT_FILE)

    result = _process_daangn_ads(
        source_dir,
        output_path,
        file_pattern,
        cleanup_source,
        alert_sender,
        email_sender,
    )
    logger.info("당근 광고 CSV 통합 완료: %s", json.dumps(result, ensure_ascii=False))
    return json.dumps(result, ensure_ascii=False)


def _process_daangn_ads(
    source_dir: Path,
    output_path: Path,
    file_pattern: str = FILE_PATTERN,
    cleanup_source: bool = True,
    alert_sender: Callable[[str], bool] | None = None,
    email_sender: Callable[[str, str, list[str]], object] | None = None,
) -> dict[str, Any]:
    files = sorted(source_dir.glob(file_pattern))
    if not files:
        if output_path.exists():
            existing = _read_csv_with_fallback(output_path)
            duplicate_diagnostics = _duplicate_diagnostics(existing)
            logger.info("당근 광고 신규 CSV 없음. 기존 통합 파일 유지: %s", output_path)
            return {
                "dataset_name": "Daangn_ads",
                "source_dir": str(source_dir),
                "output_path": str(output_path),
                "source_files": 0,
                "loaded_files": 0,
                "cleaned_files": [],
                "cleanup_source": cleanup_source,
                "skipped_files": [],
                "input_rows": 0,
                "output_rows": len(existing),
                "deduplicated_rows": 0,
                "dedup_cols": DEDUP_COLS,
                **duplicate_diagnostics,
                "missing_dates": [],
                "missing_by_group": [],
                "telegram_sent": None,
                "email_sent": None,
                "email_recipients": [],
                "status": "NO_NEW_FILES",
            }
        logger.info("당근 광고 CSV 없음. 통합 파일도 없어 처리 중단: %s", source_dir / file_pattern)
        return {
            "dataset_name": "Daangn_ads",
            "source_dir": str(source_dir),
            "output_path": str(output_path),
            "source_files": 0,
            "loaded_files": 0,
            "cleaned_files": [],
            "cleanup_source": cleanup_source,
            "skipped_files": [],
            "input_rows": 0,
            "output_rows": 0,
            "deduplicated_rows": 0,
            "dedup_cols": DEDUP_COLS,
            "duplicate_key_rows": 0,
            "same_name_cross_group_rows": 0,
            "same_name_cross_group_groups": 0,
            "missing_dates": [],
            "missing_by_group": [],
            "telegram_sent": None,
            "email_sent": None,
            "email_recipients": [],
            "status": "NO_SOURCE_FILES",
        }

    frames: list[pd.DataFrame] = []
    loaded_source_files: list[Path] = []
    skipped_files: list[dict[str, str]] = []
    for csv_path in files:
        try:
            df = _read_csv_with_fallback(csv_path)
        except Exception as exc:
            logger.warning("당근 광고 CSV 로드 실패: %s - %s", csv_path, exc)
            skipped_files.append({"file": str(csv_path), "reason": str(exc)})
            continue

        df["_source_file"] = csv_path.name
        frames.append(df)
        loaded_source_files.append(csv_path)

    if not frames:
        raise ValueError("읽을 수 있는 당근 광고 CSV 파일이 없습니다.")

    existing_output_loaded = output_path.exists()
    if existing_output_loaded:
        existing = _read_csv_with_fallback(output_path)
        existing["_source_file"] = existing.get("_source_file", "existing_output")
        frames.append(existing)

    combined = pd.concat(frames, ignore_index=True).fillna("")
    _validate_required_columns(combined)

    before_rows = len(combined)
    normalized = _normalize_and_deduplicate(combined)
    duplicate_rows = before_rows - len(normalized)
    duplicate_diagnostics = _duplicate_diagnostics(normalized)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(output_path, index=False, encoding="utf-8-sig")
    alert_result = _notify_missing_dates(normalized, source_dir, output_path, alert_sender, email_sender)
    cleaned_files = _cleanup_source_files(loaded_source_files, output_path) if cleanup_source else []

    return {
        "dataset_name": "Daangn_ads",
        "source_dir": str(source_dir),
        "output_path": str(output_path),
        "source_files": len(files),
        "loaded_files": len(frames) - (1 if existing_output_loaded else 0),
        "cleaned_files": [str(path) for path in cleaned_files],
        "cleanup_source": cleanup_source,
        "skipped_files": skipped_files,
        "input_rows": before_rows,
        "output_rows": len(normalized),
        "deduplicated_rows": duplicate_rows,
        "dedup_cols": DEDUP_COLS,
        **duplicate_diagnostics,
        "missing_dates": alert_result["missing_dates"],
        "missing_by_group": alert_result["missing_by_group"],
        "telegram_sent": alert_result["telegram_sent"],
        "email_sent": alert_result["email_sent"],
        "email_recipients": alert_result["email_recipients"],
        "status": "SUCCESS",
    }


def _cleanup_source_files(source_files: list[Path], output_path: Path) -> list[Path]:
    cleaned: list[Path] = []
    output_resolved = output_path.resolve()
    for source_file in source_files:
        if source_file.resolve() == output_resolved:
            logger.warning("출력 파일과 같은 경로라 cleanup 제외: %s", source_file)
            continue
        source_file.unlink()
        cleaned.append(source_file)
        logger.info("당근 광고 원본 CSV cleanup: %s", source_file)
    return cleaned


def _notify_missing_dates(
    df: pd.DataFrame,
    source_dir: Path,
    output_path: Path,
    alert_sender: Callable[[str], bool] | None = None,
    email_sender: Callable[[str, str, list[str]], object] | None = None,
) -> dict[str, Any]:
    alert = _build_missing_date_alert(df, source_dir, output_path)
    if not alert["missing_dates"]:
        return {
            "missing_dates": [],
            "missing_by_group": [],
            "telegram_sent": None,
            "email_sent": None,
            "email_recipients": [],
        }

    telegram_sender = alert_sender or _send_telegram_chunks
    try:
        telegram_sent = bool(telegram_sender(alert["message"]))
    except Exception as exc:
        logger.warning("당근 광고 누락일 텔레그램 발송 실패: %s", exc)
        telegram_sent = False

    email_recipients = [MAIL_DAANGN_ADS_CHA]
    try:
        subject = "차보령 대리님 당근 광고 수집 누락일 확인 부탁드립니다"
        email_result = (email_sender or _send_missing_date_email)(subject, alert["email_html"], email_recipients)
        email_sent = not str(email_result or "").startswith(("메일 발송 실패", "메일 수신자 없음"))
    except Exception as exc:
        logger.warning("당근 광고 누락일 이메일 발송 실패: %s", exc)
        email_sent = False

    return {
        "missing_dates": alert["missing_dates"],
        "missing_by_group": alert["missing_by_group"],
        "telegram_sent": telegram_sent,
        "email_sent": email_sent,
        "email_recipients": email_recipients,
    }


def _build_missing_date_alert(df: pd.DataFrame, source_dir: Path, output_path: Path) -> dict[str, Any]:
    work = df.copy()
    work["_start_date"] = pd.to_datetime(work["시작일"], errors="coerce")
    work = work.dropna(subset=["_start_date"])
    if work.empty:
        return {"missing_dates": [], "missing_by_group": [], "message": ""}

    if GROUP_COL not in work.columns:
        work[GROUP_COL] = ""
    work["_group"] = work[GROUP_COL].fillna("").astype(str).str.strip().replace("", "미확인 그룹")

    missing_by_group: list[dict[str, Any]] = []
    for group, group_df in work.groupby("_group", sort=True):
        dates = group_df["_start_date"].dropna()
        if dates.empty:
            continue
        first = dates.min().normalize()
        last = dates.max().normalize()
        observed = {value.normalize() for value in dates}
        expected = pd.date_range(first, last, freq="D")
        missing = [value.strftime("%Y-%m-%d") for value in expected if value not in observed]
        if not missing:
            continue
        missing_by_group.append(
            {
                "group": str(group),
                "first_date": first.strftime("%Y-%m-%d"),
                "last_date": last.strftime("%Y-%m-%d"),
                "missing_dates": missing,
            }
        )

    missing_dates = sorted({date for item in missing_by_group for date in item["missing_dates"]})
    if not missing_dates:
        return {"missing_dates": [], "missing_by_group": [], "message": ""}

    lines = ["[당근 광고 수집 누락]"]
    for item in missing_by_group:
        lines.extend(
            [
                "",
                f"수집 그룹: {item['group']}",
                f"범위: {item['first_date']} ~ {item['last_date']}",
                f"누락일: {', '.join(item['missing_dates'])}",
            ]
        )
    lines.extend(
        [
            "",
            "누락된 날짜 파일을 Collect_Data/마케팅_수집에 채워 넣어주세요.",
            f"입력 위치: {source_dir}",
            f"저장 위치: {output_path}",
        ]
    )
    message = "\n".join(lines)
    return {
        "missing_dates": missing_dates,
        "missing_by_group": missing_by_group,
        "message": message,
        "email_html": _missing_date_email_html(message),
    }


def _send_telegram_chunks(text: str) -> bool:
    from modules.transform.utility.notifier import send_telegram_chunks

    return send_telegram_chunks(text)


def _send_missing_date_email(subject: str, html: str, recipients: list[str]) -> object:
    from modules.transform.utility.mailer import send_email

    return send_email(subject=subject, html_content=html, to_emails=recipients)


def _missing_date_email_html(text: str) -> str:
    from modules.transform.utility.mailer import text_to_html

    return text_to_html(text)


def _read_csv_with_fallback(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in READ_ENCODINGS:
        try:
            return pd.read_csv(path, dtype=str, encoding=encoding)
        except Exception as exc:
            last_error = exc
    raise ValueError(f"CSV 읽기 실패: {path} ({last_error})")


def _validate_required_columns(df: pd.DataFrame) -> None:
    required = [COLLECTED_AT_COL, *DEDUP_COLS]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"당근 광고 CSV 필수 컬럼 누락: {missing}")


def _duplicate_diagnostics(df: pd.DataFrame) -> dict[str, int]:
    duplicate_key_rows = 0
    if all(col in df.columns for col in DEDUP_COLS):
        duplicate_key_rows = int(df.duplicated(subset=DEDUP_COLS, keep=False).sum())

    same_name_cross_group_rows = 0
    same_name_cross_group_groups = 0
    same_name_cols = ["시작일", AD_NAME_COL]
    if all(col in df.columns for col in [*same_name_cols, GROUP_COL]):
        name_group_counts = (
            df.groupby(same_name_cols, dropna=False)[GROUP_COL]
            .nunique()
            .reset_index(name="_ad_group_count")
        )
        cross_group_keys = name_group_counts[name_group_counts["_ad_group_count"] > 1][same_name_cols]
        if not cross_group_keys.empty:
            cross_group_rows = df.merge(cross_group_keys, on=same_name_cols, how="inner")
            same_name_cross_group_rows = len(cross_group_rows)
            same_name_cross_group_groups = len(cross_group_keys)

    return {
        "duplicate_key_rows": duplicate_key_rows,
        "same_name_cross_group_rows": same_name_cross_group_rows,
        "same_name_cross_group_groups": same_name_cross_group_groups,
    }


def _normalize_and_deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if URL_COL not in work.columns:
        work[URL_COL] = ""

    work["_collected_at_sort"] = pd.to_datetime(work[COLLECTED_AT_COL], errors="coerce", utc=True)
    work = work.dropna(subset=["_collected_at_sort"])
    work = work[work[DEDUP_COLS].astype(str).ne("").all(axis=1)]

    work = work.sort_values("_collected_at_sort", ascending=False)
    work = _fill_missing_urls(work)
    work = _fill_missing_image_url(work)
    work = work.drop_duplicates(subset=DEDUP_COLS, keep="first")
    work = work.sort_values(DEDUP_COLS).reset_index(drop=True)
    work = work.drop(columns=[col for col in ("_source_file", "_collected_at_sort") if col in work.columns])
    return _move_url_column_last(work)


def _fill_missing_urls(df: pd.DataFrame) -> pd.DataFrame:
    if GROUP_COL not in df.columns:
        return df

    work = df.copy()
    work[URL_COL] = work[URL_COL].fillna("").astype(str)

    group_values = work[GROUP_COL].fillna("").astype(str).str.strip()
    url_values = work[URL_COL].fillna("").astype(str).str.strip()

    reference_mask = group_values.ne("") & url_values.ne("")
    if reference_mask.any():
        refs = pd.DataFrame(
            {
                GROUP_COL: group_values[reference_mask],
                URL_COL: work.loc[reference_mask, URL_COL],
            }
        )
        url_by_group = refs.drop_duplicates(GROUP_COL, keep="first").set_index(GROUP_COL)[URL_COL]
    else:
        url_by_group = pd.Series(dtype=str)

    known_group_urls = pd.Series(KNOWN_AD_GROUP_URLS)
    url_by_group = pd.concat([known_group_urls, url_by_group])
    url_by_group = url_by_group[~url_by_group.index.duplicated(keep="last")]

    fill_mask = group_values.ne("") & url_values.eq("") & group_values.isin(url_by_group.index)
    if fill_mask.any():
        work.loc[fill_mask, URL_COL] = group_values[fill_mask].map(url_by_group)

    return work


def _fill_missing_image_url(df: pd.DataFrame) -> pd.DataFrame:
    """같은 campaign_id(광고 단위 결정론적 ID)에 image_url이 있는 행이 하나라도 있으면
    같은 campaign_id의 빈 image_url 행을 채운다. 확장이 image_url을 수집하기 전에 쌓인
    과거 행도, 이후 같은 광고가 한 번이라도 수집되면 채워진다.

    url과 달리 그룹(광고그룹명)이 아니라 campaign_id로 채운다 - 한 광고그룹 안에도 소재별로
    이미지가 다르기 때문에 그룹 단위로 채우면 서로 다른 광고의 썸네일이 뒤섞인다.
    """
    work = df.copy()
    if IMAGE_URL_COL not in work.columns:
        work[IMAGE_URL_COL] = ""
    work[IMAGE_URL_COL] = work[IMAGE_URL_COL].fillna("").astype(str)

    campaign_values = work["campaign_id"].fillna("").astype(str).str.strip()
    image_values = work[IMAGE_URL_COL].str.strip()

    reference_mask = campaign_values.ne("") & image_values.ne("")
    if not reference_mask.any():
        return work

    refs = pd.DataFrame(
        {
            "campaign_id": campaign_values[reference_mask],
            IMAGE_URL_COL: work.loc[reference_mask, IMAGE_URL_COL],
        }
    )
    image_by_campaign = refs.drop_duplicates("campaign_id", keep="first").set_index("campaign_id")[IMAGE_URL_COL]

    fill_mask = campaign_values.ne("") & image_values.eq("") & campaign_values.isin(image_by_campaign.index)
    if fill_mask.any():
        work.loc[fill_mask, IMAGE_URL_COL] = campaign_values[fill_mask].map(image_by_campaign)

    return work


def _move_url_column_last(df: pd.DataFrame) -> pd.DataFrame:
    if URL_COL not in df.columns:
        return df
    columns = [col for col in df.columns if col != URL_COL] + [URL_COL]
    return df.loc[:, columns]
