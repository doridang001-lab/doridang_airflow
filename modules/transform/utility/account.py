import logging
import re
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import LOCAL_DB, ONEDRIVE_DB

logger = logging.getLogger(__name__)

AUTOMATION_NOTE_COLUMN = "비고"
AUTOMATION_NOTE_VALUE = "자동화 연결"
SALES_EMPLOYEE_CSV = ONEDRIVE_DB / "sales_employee.csv"
LOCAL_SALES_EMPLOYEE_CSV = LOCAL_DB / "영업관리부_DB" / "sales_employee.csv"

CHANNEL_TO_PLATFORM = {
       "baemin": "배달의 민족",
       "coupangeats": "쿠팡이츠",
       "ddangyo": "땡겨요",
       "toorder": "토더",
}

account_df = pd.DataFrame({
       "platform": ["아임웹", "카페24", "어도비_구글", "toorder", "포스피드"],
       "channel": ["aimeweb", "cafe24", "adobe_google", "toorder", "posspeed"],
       "id": [
              "doridangofficial@naver.com",
              "doridangofficial@naver.com",
              "doridang001@gmail.com",
              "doridang15",
              "siw2222@naver.com",
       ],
       "pw": [
              "Ehfl0613!!",
              "Ehfl0613!!",
              "ehfl0907!!",
              "ehfl1819!",
              "ehfl8877!!",
       ],
})


def get_pw(channel: str, account_id: str | None = None) -> str:
    """계정 비밀번호 단일 소스 조회.

    channel(및 선택적으로 account_id)에 해당하는 pw를 account_df에서 반환한다.
    비밀번호 변경 시 이 파일의 account_df 한 줄만 수정하면 된다.
    """
    platform = CHANNEL_TO_PLATFORM.get(str(channel).strip())
    if platform:
        df = load_automation_account_df(platform=platform)
        if account_id is not None and "계정ID" in df.columns:
            df = df[df["계정ID"].astype(str).str.strip() == str(account_id).strip()]
        if not df.empty:
            return str(df.iloc[0]["계정PW"]).strip()
        return _get_legacy_pw(channel, account_id)

    return _get_legacy_pw(channel, account_id)


def _get_legacy_pw(channel: str, account_id: str | None = None) -> str:
    df = account_df[account_df["channel"] == channel]
    if account_id is not None:
        df = df[df["id"] == account_id]
    return "" if df.empty else str(df.iloc[0]["pw"]).strip()


def _default_sales_employee_csv_path() -> Path:
    if LOCAL_SALES_EMPLOYEE_CSV.exists():
        return LOCAL_SALES_EMPLOYEE_CSV
    return SALES_EMPLOYEE_CSV


def _read_sales_employee_csv(path: Path | None = None) -> pd.DataFrame:
    """Read sales_employee.csv using the encodings seen in team exports."""
    if path is None:
        path = _default_sales_employee_csv_path()

    if not path.exists():
        logger.warning("sales_employee.csv 없음: %s", path)
        return pd.DataFrame()

    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            df = pd.read_csv(path, dtype=str, encoding=encoding).fillna("")
            df.columns = [str(col).strip() for col in df.columns]
            return df
        except Exception as exc:
            last_error = exc
    logger.warning("sales_employee.csv 로드 실패: %s (%s)", path, last_error)
    return pd.DataFrame()


def _normalize_targets(target_stores: list[str] | None) -> list[str]:
    return [str(store).strip() for store in (target_stores or []) if str(store).strip()]


def load_automation_account_df(
    *,
    platform: str,
    target_stores: list[str] | None = None,
    exact: bool = True,
    csv_path: Path | None = None,
    require_note: bool = False,
) -> pd.DataFrame:
    """Return rows explicitly marked as automation-linked in sales_employee.csv."""
    df = _read_sales_employee_csv(csv_path)
    required = {"플랫폼", "계정ID", "계정PW", AUTOMATION_NOTE_COLUMN}
    missing = sorted(required - set(df.columns))
    if missing:
        logger.warning("자동화 계정 필터 컬럼 누락: %s columns=%s", missing, list(df.columns))
        if AUTOMATION_NOTE_COLUMN in missing:
            if require_note:
                logger.warning(
                    "비고 컬럼 없음 - require_note=True 이므로 platform-only fallback 미적용: columns=%s",
                    list(df.columns),
                )
                return pd.DataFrame(columns=list(required) + ["매장명"])
            required_without_note = {"플랫폼", "계정ID", "계정PW"}
            if required_without_note.issubset(df.columns):
                logger.warning(
                    "비고 컬럼 없음 - platform/store 필터 fallback 적용"
                )
            else:
                return pd.DataFrame(columns=list(required) + ["매장명"])
        else:
            return pd.DataFrame(columns=list(required) + ["매장명"])

    platform_name = str(platform).strip()
    platform_mask = df["플랫폼"].astype(str).str.strip().eq(platform_name)
    if AUTOMATION_NOTE_COLUMN in df.columns:
        note_mask = df[AUTOMATION_NOTE_COLUMN].astype(str).str.strip().eq(AUTOMATION_NOTE_VALUE)
        filtered = df[platform_mask & note_mask].copy()
    else:
        filtered = df[platform_mask].copy()

    targets = _normalize_targets(target_stores)
    if targets and "매장명" in filtered.columns:
        store_series = filtered["매장명"].astype(str).str.strip()
        if exact:
            filtered = filtered[store_series.isin(targets)].copy()
        else:
            pattern = "|".join(re.escape(store) for store in targets)
            filtered = filtered[store_series.str.contains(pattern, na=False, regex=True)].copy()

    logger.info(
        "자동화 연결 계정 로드: platform=%s total=%d stores=%s",
        platform_name,
        len(filtered),
        filtered["매장명"].astype(str).str.strip().tolist() if "매장명" in filtered.columns else [],
    )
    return filtered


def _legacy_account_row(channel: str) -> pd.Series | None:
    legacy_df = account_df[account_df["channel"] == str(channel).strip()]
    return None if legacy_df.empty else legacy_df.iloc[0]


def get_default_account(channel: str) -> tuple[str, str]:
    """채널 대표(기업) 계정을 반환한다."""
    channel_key = str(channel).strip()
    platform = CHANNEL_TO_PLATFORM.get(channel_key)
    if not platform:
        return "", ""

    legacy_row = _legacy_account_row(channel_key)
    legacy_id = str(legacy_row.get("id", "")).strip() if legacy_row is not None else ""

    df = load_automation_account_df(platform=platform, require_note=True)
    if not df.empty:
        matched = df
        if legacy_id and "계정ID" in df.columns:
            preferred = df[df["계정ID"].astype(str).str.strip() == legacy_id]
            if not preferred.empty:
                matched = preferred
            else:
                logger.warning(
                    "자동화 연결 행에 대표 계정ID 없음 - 첫 행 사용: channel=%s expected=%s",
                    channel_key,
                    legacy_id,
                )
        row = matched.iloc[0]
        return str(row.get("계정ID", "")).strip(), str(row.get("계정PW", "")).strip()

    if legacy_row is None:
        logger.warning("기본 계정 없음: channel=%s platform=%s", channel_key, platform)
        return "", ""

    password = get_pw(channel_key, legacy_id) or str(legacy_row.get("pw", "")).strip()
    logger.warning(
        "자동화 연결(비고) 기본 계정 없음 - 내장 대표 계정 사용: channel=%s account_id=%s",
        channel_key,
        legacy_id,
    )
    return legacy_id, password
