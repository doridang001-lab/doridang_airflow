"""브랜드전략기획팀 주간 KPI 목표·실적 통합 파이프라인.

처리 흐름:
1. 도메인별 목표/실적 엑셀 Sheet1을 읽는다.
2. 지표 레지스트리로 wide -> long 변환 후 (주 시작일, 도메인, 지표) 기준으로 병합한다.
3. 달성률/갭/입력 여부를 계산해 세로형 마트 parquet·csv로 원자 저장한다.
4. 지정 주차의 실적 미입력 담당자를 판정해 알림 본문을 만든다.

컬럼명은 원본 엑셀에 오타(`참여자 수 목표`, `목표네이버 비용`, `목표당근 노출`)가 있어
접두어 규칙으로 추론하지 않고 레지스트리에 실제 문자열을 그대로 선언한다.
"""

from __future__ import annotations

import logging
import os
import re
from copy import copy
from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_HALF_UP
from html import escape
from datetime import date, datetime, timedelta
from math import ceil
from pathlib import Path
from typing import Callable, Sequence

import pandas as pd
import pendulum
from openpyxl import load_workbook

from modules.transform.utility.paths import (
    BSP_KPI_DIR,
    BSP_KPI_WEEKLY_CSV,
    BSP_KPI_WEEKLY_PARQUET,
)

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")
TEAM = "brand_strategy_planning"
SHEET_NAME = "Sheet1"

WEEK_START_COLUMNS = ("주 시작일", "주시작일", "주 시작 일")
YM_COLUMNS = ("ym", "YM", "년월")
WEEK_LABEL_COLUMNS = ("월 주차", "주차", "월주차")
OWNER_COLUMNS = ("담당자", "담당")

OUTPUT_COLUMNS = [
    "week_start",
    "ym",
    "week_label",
    "team",
    "domain",
    "domain_name",
    "owner",
    "metric_key",
    "metric_name",
    "metric_order",
    "target_value",
    "actual_value",
    "achievement_rate",
    "gap",
    "evaluation",
    "has_target",
    "has_actual",
    "alert_required",
    "is_submitted",
    "is_closed_week",
    "updated_at",
]


@dataclass(frozen=True)
class MetricSpec:
    """지표 1개의 영문 키와 원본 엑셀 컬럼명 매핑."""

    key: str
    name: str
    actual_column: str | tuple[str, ...] | None
    target_column: str | tuple[str, ...] | None
    alert_required: bool = True


@dataclass(frozen=True)
class DomainSpec:
    """KPI 도메인 1개(= 실적 파일 1개 + 목표 파일 1개)."""

    key: str
    name: str
    actual_file: str
    target_file: str
    metrics: tuple[MetricSpec, ...]
    in_progress_file: str | None = None


@dataclass(frozen=True)
class BspKpiMissingRow:
    """미입력 도메인 1건. 평문·HTML 본문이 이 데이터를 공유한다."""

    owner: str
    domain: str
    domain_name: str
    actual_file: str


@dataclass(frozen=True)
class BspKpiAlertResult:
    """주간 실적 미입력 판정 결과와 운영 로그용 집계."""

    message: str
    target_week_start: str = ""
    week_label: str = ""
    checked_count: int = 0
    missing_count: int = 0
    missing_rows: tuple[BspKpiMissingRow, ...] = ()
    skipped_reason: str = ""
    sent_telegram: bool = False
    sent_emails: tuple[str, ...] = ()


# ============================================================
# 지표 레지스트리
# 신규 KPI 파일이 늘어나면 여기에 DomainSpec 한 줄만 추가한다.
# ============================================================

BRAND_VIRAL_METRICS = (
    MetricSpec("product_cost", "상품 제공 금액", "상품 제공 금액", "목표 상품 제공 금액", alert_required=False),
    # 신컬럼 `참여자 수`를 우선하고, 기존 오타 컬럼 `참여자 수 목표`도 fallback으로 유지한다.
    MetricSpec("participants", "참여자 수", ("참여자 수", "참여자 수 목표"), "목표 참여자 수"),
    MetricSpec("posts", "게시물 수", "게시물 수", "목표 게시물 수"),
    MetricSpec("reach", "도달 수", "도달 수", "목표 도달 수"),
    MetricSpec("saves", "저장 수", "저장 수", None),
    MetricSpec("shares", "공유 수", "공유 수", None),
    MetricSpec("likes", "좋아요 수", "좋아요 수", None),
    MetricSpec("participant_unit_amount", "참여자 1명당 기준금액", "참여자 1명당 기준금액", None, alert_required=False),
    MetricSpec("required_participants", "필요 참여자 수", None, None, alert_required=False),
    MetricSpec("actual_cost_per_participant", "실제 참여자 1명당 금액", None, None, alert_required=False),
)

BRAND_VIRAL_EVENT_TARGET_KEYS = ("product_cost", "participants", "participant_unit_amount")
BRAND_VIRAL_TARGET_WORKBOOK_COLUMNS = {
    "product_cost": "목표 상품 제공 금액",
    "participants": "목표 참여자 수",
    "participant_unit_amount": "채워야 참여자 1명당 기준금액",
}

DIRECT_STORE_METRICS = (
    MetricSpec("place_daily_visit", "플레이스 일평균 유입수", "플레이스 일평균 유입수", "목표 플레이스 일평균 유입수"),
    MetricSpec("naver_impression", "네이버 노출", "네이버 노출", "목표 네이버 노출"),
    MetricSpec("naver_click", "네이버 클릭", "네이버 클릭", "목표 네이버 클릭"),
    # 목표 시트 컬럼명 띄어쓰기 누락
    MetricSpec("naver_cost", "네이버 비용", "네이버 비용", "목표네이버 비용"),
    MetricSpec("danggeun_impression", "당근 노출", "당근 노출", "목표당근 노출"),
    MetricSpec("danggeun_click", "당근 클릭", "당근 클릭", "목표 당근 클릭"),
    MetricSpec("danggeun_cost", "당근 비용", "당근 비용", "목표 당근 비용"),
    MetricSpec("insta_impression", "인스타 노출", "인스타 노출", "목표 인스타 노출"),
    MetricSpec("insta_landing_view", "인스타 랜딩 조회", "인스타 랜딩 조회", "목표 인스타 랜딩 조회"),
    MetricSpec("insta_cost", "인스타 비용", "인스타 비용", "목표 인스타 비용"),
    MetricSpec("internal_content", "내부 콘텐츠", "내부 콘텐츠", "목표 내부 콘텐츠"),
    MetricSpec("external_content", "외부 콘텐츠", "외부 콘텐츠", "목표 외부 콘텐츠"),
)

# ============================================================
# 알림 수신자
# 메일 받을 사람이 늘면 아래에 변수를 추가하고 BSP_KPI_ALERT_EMAILS 매핑에 넣는다.
# 값을 None으로 두면 그 사람만 발송에서 자동 제외된다(mail_recipients.py와 같은 관례).
# MAIL_BSP_KPI_CMJ는 운영 확인용 변수로만 두고, 메일은 각 담당자에게만 발송한다.
# 사내 주소는 mail_recipients.py 상수를 import해서 쓰는 것을 권장한다.
#   from modules.transform.utility.mail_recipients import MAIL_CMJ_PM
# ============================================================

MAIL_BSP_KPI_CMJ = "a17019@kakao.com"   # 조민준 PM
MAIL_BSP_KPI_HWANG = "syd662@kakao.com"               # 황유경 (브랜드 바이럴 담당)
MAIL_BSP_KPI_CHA = "melanie0204@kakao.com"            # 차보령 (직영점 마케팅 담당)

BSP_KPI_ALERT_EMAILS = {
    "황유경": MAIL_BSP_KPI_HWANG,
    "차보령": MAIL_BSP_KPI_CHA,
}

BSP_KPI_ALERT_TELEGRAM = True  # False로 두면 메일만 나간다

ALERT_INPUT_LOCATION = "data/mart/brand_strategy_planning_team/bsp_kpi/"

OWNER_EMAIL_DISPLAY_NAMES = {
    "황유경": "황유경 실장님",
    "차보령": "차보령 대리님",
}

DOMAINS: tuple[DomainSpec, ...] = (
    DomainSpec(
        key="brand_viral",
        name="브랜드 바이럴",
        actual_file="weekly_brand_viral_performance_tracke.xlsx",
        target_file="weekly_brand_viral_performance_tracke_Target_Cal.xlsx",
        metrics=BRAND_VIRAL_METRICS,
        in_progress_file="weekly_brand_viral_in_progress_tracke.xlsx",
    ),
    DomainSpec(
        key="direct_store_marketing",
        name="직영점 마케팅",
        actual_file="weekly_direct_store_marketing_growth.xlsx",
        target_file="weekly_direct_store_marketing_growth_Target_Cal.xlsx",
        metrics=DIRECT_STORE_METRICS,
    ),
)


# ============================================================
# 내부 유틸
# ============================================================


def _squash(value: object) -> str:
    return re.sub(r"\s+", "", str(value))


def _find_column(df: pd.DataFrame, *candidates: str):
    """공백 차이를 무시하고 컬럼을 찾는다. `주 시작일`/`주시작일` 흡수용."""
    lookup = {_squash(column): column for column in df.columns}
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
        found = lookup.get(_squash(candidate))
        if found is not None:
            return found
    return None


def _column_candidates(value: str | tuple[str, ...] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def _to_number(value: object) -> float:
    """쉼표·공백이 섞인 엑셀 값을 float으로 바꾸고 실패하면 NaN."""
    if value is None:
        return float("nan")
    if isinstance(value, bool):
        return float("nan")
    if isinstance(value, (int, float)):
        return float("nan") if pd.isna(value) else float(value)
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "nan", "None", "null"}:
        return float("nan")
    try:
        return float(text)
    except ValueError:
        return float("nan")


def _evaluate_simple_excel_formula(value: object) -> float:
    """숫자와 사칙연산만 있는 엑셀 수식 캐시가 비었을 때 직접 계산한다."""
    if not isinstance(value, str) or not value.startswith("="):
        return float("nan")
    expression = value[1:].strip()
    if not expression or not re.fullmatch(r"[0-9,\.\s\+\-\*/\(\)]+", expression):
        return float("nan")
    try:
        result = eval(expression.replace(",", ""), {"__builtins__": {}}, {})
    except Exception:
        return float("nan")
    return _to_number(result)


def _fill_simple_formula_values(path: Path, df: pd.DataFrame, *, sheet_name: str) -> pd.DataFrame:
    """openpyxl 저장 후 사라진 수식 캐시를 단순 산식에 한해 DataFrame에 보정한다."""
    try:
        workbook = load_workbook(path, data_only=False, read_only=True)
    except Exception as exc:
        logger.warning("BSP KPI 엑셀 수식 확인 실패 | path=%s error=%r", path, exc)
        return df
    if sheet_name not in workbook.sheetnames:
        return df

    working = df.copy()
    worksheet = workbook[sheet_name]
    max_row = min(len(working) + 1, worksheet.max_row)
    max_column = min(len(working.columns), worksheet.max_column)
    filled = 0
    for row_number in range(2, max_row + 1):
        df_index = row_number - 2
        for column_number in range(1, max_column + 1):
            column_name = working.columns[column_number - 1]
            if not pd.isna(working.iat[df_index, column_number - 1]):
                continue
            value = _evaluate_simple_excel_formula(worksheet.cell(row=row_number, column=column_number).value)
            if pd.isna(value):
                continue
            working.at[df_index, column_name] = value
            filled += 1
    if filled:
        logger.info("BSP KPI 엑셀 단순 수식 보정 | path=%s sheet=%s cells=%s", path.name, sheet_name, filled)
    return working


def _read_kpi_sheet(path: Path, *, sheet_name: str = SHEET_NAME) -> pd.DataFrame:
    """KPI 엑셀 시트를 읽는다. 파일이 없으면 빈 DataFrame을 돌려주고 계속 진행한다.

    Target_Cal 파일의 Sheet2는 주차 라벨이 병합된 계산용 작업 시트이므로 읽지 않는다.
    """
    if not path.is_file():
        logger.warning("BSP KPI 엑셀 없음 | path=%s", path)
        return pd.DataFrame()
    try:
        df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    except Exception as exc:
        logger.warning("BSP KPI 엑셀 읽기 실패 | path=%s error=%r", path, exc)
        return pd.DataFrame()
    df = _fill_simple_formula_values(path, df, sheet_name=sheet_name)
    logger.info("BSP KPI 엑셀 로드 | path=%s rows=%s cols=%s", path.name, len(df), len(df.columns))
    return df


def _normalize_keys(df: pd.DataFrame) -> pd.DataFrame:
    """키 컬럼을 week_start/ym/week_label/owner로 정규화한다."""
    if df.empty:
        return pd.DataFrame(columns=["week_start", "ym", "week_label", "owner"])

    week_column = _find_column(df, *WEEK_START_COLUMNS)
    if week_column is None:
        logger.warning("BSP KPI 주 시작일 컬럼 없음 | columns=%s", list(df.columns))
        return pd.DataFrame(columns=["week_start", "ym", "week_label", "owner"])

    ym_column = _find_column(df, *YM_COLUMNS)
    label_column = _find_column(df, *WEEK_LABEL_COLUMNS)
    owner_column = _find_column(df, *OWNER_COLUMNS)

    working = df.copy()
    working["week_start"] = pd.to_datetime(working[week_column], errors="coerce").dt.date
    working["ym"] = working[ym_column].map(_clean_text) if ym_column else ""
    working["week_label"] = working[label_column].map(_clean_text) if label_column else ""
    working["owner"] = working[owner_column].map(_clean_text) if owner_column else ""
    working = working[working["week_start"].notna()].copy()
    return working


def _to_long(df: pd.DataFrame, metrics: Sequence[MetricSpec], *, value_kind: str) -> pd.DataFrame:
    """지표 레지스트리를 사용해 wide -> long 변환한다. value_kind는 target/actual."""
    value_column = f"{value_kind}_value"
    empty = pd.DataFrame(
        columns=["week_start", "ym", "week_label", "owner", "metric_key", "metric_order", value_column]
    )
    working = _normalize_keys(df)
    if working.empty:
        return empty

    attribute = "target_column" if value_kind == "target" else "actual_column"
    frames: list[pd.DataFrame] = []
    for order, metric in enumerate(metrics):
        source_names = _column_candidates(getattr(metric, attribute))
        if not source_names:
            continue
        source_column = _find_column(working, *source_names)
        if source_column is None:
            logger.warning(
                "BSP KPI 컬럼 매칭 실패 | kind=%s metric=%s expected=%s",
                value_kind,
                metric.key,
                source_names,
            )
            continue
        piece = working[["week_start", "ym", "week_label", "owner"]].copy()
        piece["metric_key"] = metric.key
        piece["metric_order"] = order
        piece[value_column] = working[source_column].map(_to_number)
        frames.append(piece)

    if not frames:
        return empty
    return pd.concat(frames, ignore_index=True)


def _clean_text(value: object) -> str:
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def _pick_text(primary: object, fallback: object) -> str:
    text = str(primary or "").strip()
    if text and text.lower() not in {"nan", "none"}:
        return text
    text = str(fallback or "").strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def _merge_domain(domain: DomainSpec, base_dir: Path) -> pd.DataFrame:
    """도메인 1개의 목표/실적을 세로형으로 병합한다."""
    metric_names = {metric.key: metric.name for metric in domain.metrics}

    actual_df = _read_kpi_sheet(base_dir / domain.actual_file)
    target_df = _read_kpi_sheet(base_dir / domain.target_file)
    in_progress_df = (
        _read_kpi_sheet(base_dir / domain.in_progress_file)
        if domain.in_progress_file
        else pd.DataFrame()
    )
    actual_long = _to_long(actual_df, domain.metrics, value_kind="actual")
    target_long = _to_long(target_df, domain.metrics, value_kind="target")
    if domain.key == "brand_viral":
        actual_long = _append_brand_viral_actual_calculations(
            actual_df,
            actual_long,
            domain.metrics,
        )
        target_long = _apply_brand_viral_target_calculations(
            target_df,
            in_progress_df,
            target_long,
            domain.metrics,
        )
    if actual_long.empty and target_long.empty:
        logger.warning("BSP KPI 도메인 데이터 없음 | domain=%s", domain.key)
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    merged = pd.merge(
        target_long,
        actual_long,
        on=["week_start", "metric_key"],
        how="outer",
        suffixes=("_target", "_actual"),
    )

    # 실적 시트 값을 우선하고 비어 있으면 목표 시트 값으로 채운다.
    for column in ("ym", "week_label", "owner"):
        merged[column] = [
            _pick_text(actual, target)
            for actual, target in zip(merged[f"{column}_actual"], merged[f"{column}_target"])
        ]
    merged["metric_order"] = (
        merged["metric_order_target"].fillna(merged["metric_order_actual"]).astype(int)
    )
    merged["metric_name"] = merged["metric_key"].map(metric_names)
    merged["team"] = TEAM
    merged["domain"] = domain.key
    merged["domain_name"] = domain.name

    has_target_metric = {
        metric.key: bool(metric.target_column)
        or metric.key == "participant_unit_amount"
        for metric in domain.metrics
    }
    alert_required_metric = {metric.key: metric.alert_required for metric in domain.metrics}
    merged["has_target"] = merged["metric_key"].map(has_target_metric).fillna(False).astype(bool)
    merged["has_actual"] = merged["actual_value"].notna()
    merged["alert_required"] = (
        merged["metric_key"].map(alert_required_metric).fillna(True).astype(bool)
    )

    return merged


def _finalize(df: pd.DataFrame, *, today: date) -> pd.DataFrame:
    """달성률·갭·입력 여부를 계산하고 출력 컬럼 순서로 정리한다."""
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    target = df["target_value"]
    actual = df["actual_value"]
    ratio_base = target.where(target.notna() & (target != 0))
    df["achievement_rate"] = actual / ratio_base
    df["gap"] = actual - target
    df["evaluation"] = ""
    required = df["alert_required"].fillna(True).astype(bool)
    required_missing = required & ~df["has_actual"].fillna(False).astype(bool)
    required_count = required.groupby([df["week_start"], df["domain"]]).transform("sum")
    missing_count = required_missing.groupby([df["week_start"], df["domain"]]).transform("sum")
    df["is_submitted"] = (required_count.gt(0) & missing_count.eq(0)).astype(bool)
    df["is_closed_week"] = df["week_start"].map(lambda value: value + timedelta(days=7) <= today)
    df["updated_at"] = pd.Timestamp(pendulum.now(KST).naive())

    df = df.sort_values(["week_start", "domain", "metric_order"], kind="stable")
    return df.reindex(columns=OUTPUT_COLUMNS).reset_index(drop=True)


def _write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_parquet(tmp, index=False, engine="pyarrow")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def _write_csv_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def _as_date(value: object) -> date | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _excel_round(value: Decimal) -> int:
    """Excel ROUND(value, 0)와 같은 반올림을 적용한다."""
    return int(value.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _series_number(working: pd.DataFrame, column_name: str | None) -> pd.Series:
    if column_name is None:
        return pd.Series(float("nan"), index=working.index, dtype="float64")
    return working[column_name].map(_to_number)


def _calculated_actual_frame(
    working: pd.DataFrame,
    *,
    metric_key: str,
    metric_order: int,
    values: pd.Series,
) -> pd.DataFrame:
    piece = working[["week_start", "ym", "week_label", "owner"]].copy()
    piece["metric_key"] = metric_key
    piece["metric_order"] = metric_order
    piece["actual_value"] = values
    return piece


def _append_brand_viral_actual_calculations(
    source_df: pd.DataFrame,
    actual_long: pd.DataFrame,
    metrics: Sequence[MetricSpec],
) -> pd.DataFrame:
    """브랜드 바이럴 실적 파일의 추가 입력값으로 효율 계산 지표를 만든다."""
    working = _normalize_keys(source_df)
    if working.empty:
        return actual_long

    metric_order = {metric.key: order for order, metric in enumerate(metrics)}
    product = _series_number(working, _find_column(working, "상품 제공 금액"))
    participants = _series_number(working, _find_column(working, "참여자 수", "참여자 수 목표"))
    unit_amount = _series_number(working, _find_column(working, "참여자 1명당 기준금액"))

    required = pd.Series(float("nan"), index=working.index, dtype="float64")
    valid_required = product.notna() & unit_amount.notna() & (unit_amount > 0)
    required.loc[valid_required] = [
        float(ceil(product_value / unit_value))
        for product_value, unit_value in zip(product[valid_required], unit_amount[valid_required])
    ]

    actual_cost = pd.Series(float("nan"), index=working.index, dtype="float64")
    valid_cost = product.notna() & participants.notna() & (participants > 0)
    actual_cost.loc[valid_cost] = [
        float(_excel_round(Decimal(str(product_value)) / Decimal(str(participant_value))))
        for product_value, participant_value in zip(product[valid_cost], participants[valid_cost])
    ]

    frames = [
        actual_long,
        _calculated_actual_frame(
            working,
            metric_key="required_participants",
            metric_order=metric_order["required_participants"],
            values=required,
        ),
        _calculated_actual_frame(
            working,
            metric_key="actual_cost_per_participant",
            metric_order=metric_order["actual_cost_per_participant"],
            values=actual_cost,
        ),
    ]
    return pd.concat(frames, ignore_index=True)


def _apply_brand_viral_target_calculations(
    source_df: pd.DataFrame,
    in_progress_df: pd.DataFrame,
    target_long: pd.DataFrame,
    metrics: Sequence[MetricSpec],
) -> pd.DataFrame:
    """진행 이벤트 기간과 겹친 일수로 브랜드 바이럴 주차 목표를 계산한다."""
    metric_order = {metric.key: order for order, metric in enumerate(metrics)}
    if not target_long.empty:
        target_long = target_long.copy()
        target_long = target_long[~target_long["metric_key"].isin(BRAND_VIRAL_EVENT_TARGET_KEYS)]

    working = _normalize_keys(source_df)
    if working.empty:
        return target_long

    event_targets = _build_brand_viral_event_target_frame(working, in_progress_df, metric_order)
    return pd.concat([target_long, event_targets], ignore_index=True)


def _decimal_number(value: object) -> Decimal | None:
    number = _to_number(value)
    if pd.isna(number):
        return None
    return Decimal(str(number))


def _normalize_brand_viral_events(source_df: pd.DataFrame) -> pd.DataFrame:
    """진행 이벤트 입력 파일에서 일할 목표 계산에 필요한 유효 이벤트만 남긴다."""
    empty = pd.DataFrame(columns=["event_name", "start_date", "end_date", "target_participants", "product_cost"])
    if source_df.empty:
        logger.warning("BSP KPI 브랜드 바이럴 진행 이벤트 입력 없음")
        return empty

    start_column = _find_column(source_df, "시작일", "이벤트 시작일")
    end_column = _find_column(source_df, "종료일", "이벤트 종료일")
    event_column = _find_column(source_df, "이벤트명", "이벤트 명")
    participant_column = _find_column(source_df, "이벤트 참여자수 목표", "이벤트 참여자 수 목표", "참여자수 목표")
    cost_column = _find_column(source_df, "상품 제공 설정 금액", "상품제공 설정금액", "상품 제공 금액")
    missing = [
        name
        for name, column in {
            "시작일": start_column,
            "종료일": end_column,
            "이벤트명": event_column,
            "이벤트 참여자수 목표": participant_column,
            "상품 제공 설정 금액": cost_column,
        }.items()
        if column is None
    ]
    if missing:
        logger.warning("BSP KPI 브랜드 바이럴 진행 이벤트 컬럼 없음 | missing=%s", missing)
        return empty

    working = pd.DataFrame(index=source_df.index)
    working["event_name"] = source_df[event_column].map(_clean_text)
    working["start_date"] = source_df[start_column].map(_as_date)
    working["end_date"] = source_df[end_column].map(_as_date)
    working["target_participants"] = source_df[participant_column].map(_decimal_number)
    working["product_cost"] = source_df[cost_column].map(_decimal_number)

    valid = (
        working["event_name"].ne("")
        & working["start_date"].notna()
        & working["end_date"].notna()
        & working["target_participants"].notna()
        & working["product_cost"].notna()
    )
    valid &= working["end_date"] >= working["start_date"]
    invalid_count = int((~valid).sum())
    if invalid_count:
        logger.info("BSP KPI 브랜드 바이럴 진행 이벤트 제외 | rows=%s", invalid_count)
    return working[valid].reset_index(drop=True)


def _overlap_days(start: date, end: date, week_start: date) -> int:
    week_end = week_start + timedelta(days=6)
    overlap_start = max(start, week_start)
    overlap_end = min(end, week_end)
    if overlap_end < overlap_start:
        return 0
    return (overlap_end - overlap_start).days + 1


def _build_brand_viral_event_target_frame(
    target_weeks: pd.DataFrame,
    in_progress_df: pd.DataFrame,
    metric_order: dict[str, int],
) -> pd.DataFrame:
    events = _normalize_brand_viral_events(in_progress_df)
    rows: list[dict[str, object]] = []
    for _, week_row in target_weeks.iterrows():
        week = _as_date(week_row["week_start"])
        product_total = Decimal("0")
        participant_total = Decimal("0")
        if week is not None and not events.empty:
            for _, event in events.iterrows():
                days = _overlap_days(event["start_date"], event["end_date"], week)
                if days <= 0:
                    continue
                event_days = (event["end_date"] - event["start_date"]).days + 1
                product_total += event["product_cost"] * Decimal(days) / Decimal(event_days)
                participant_total += event["target_participants"] * Decimal(days) / Decimal(event_days)

        values = {
            "product_cost": float(_excel_round(product_total)) if product_total else float("nan"),
            "participants": float(_excel_round(participant_total)) if participant_total else float("nan"),
            "participant_unit_amount": (
                float(_excel_round(product_total / participant_total))
                if product_total and participant_total
                else float("nan")
            ),
        }
        for metric_key in BRAND_VIRAL_EVENT_TARGET_KEYS:
            rows.append(
                {
                    "week_start": week_row["week_start"],
                    "ym": week_row["ym"],
                    "week_label": week_row["week_label"],
                    "owner": week_row["owner"],
                    "metric_key": metric_key,
                    "metric_order": metric_order[metric_key],
                    "target_value": values[metric_key],
                }
            )
    return pd.DataFrame(
        rows,
        columns=["week_start", "ym", "week_label", "owner", "metric_key", "metric_order", "target_value"],
    )


def _copy_cell_style(source, target) -> None:
    if not getattr(source, "has_style", False):
        return
    target.font = copy(source.font)
    target.fill = copy(source.fill)
    target.border = copy(source.border)
    target.alignment = copy(source.alignment)
    target.number_format = source.number_format
    target.protection = copy(source.protection)


def _worksheet_column_lookup(ws) -> dict[str, int]:
    return {
        _squash(cell.value): cell.column
        for cell in ws[1]
        if cell.value is not None and str(cell.value).strip()
    }


def _find_worksheet_column(ws, *candidates: str) -> int | None:
    lookup = _worksheet_column_lookup(ws)
    for candidate in candidates:
        found = lookup.get(_squash(candidate))
        if found is not None:
            return found
    return None


def _ensure_worksheet_column(ws, header: str) -> tuple[int, bool]:
    existing = _find_worksheet_column(ws, header)
    if existing is not None:
        return existing, False

    source_column = ws.max_column
    new_column = source_column + 1
    header_cell = ws.cell(row=1, column=new_column, value=header)
    _copy_cell_style(ws.cell(row=1, column=source_column), header_cell)
    if ws.column_dimensions[ws.cell(row=1, column=source_column).column_letter].width:
        ws.column_dimensions[header_cell.column_letter].width = ws.column_dimensions[
            ws.cell(row=1, column=source_column).column_letter
        ].width
    return new_column, True


def _excel_write_value(value: object) -> int | float | None:
    if value is None or pd.isna(value):
        return None
    numeric = float(value)
    return int(numeric) if numeric.is_integer() else numeric


def _sync_brand_viral_target_workbook(base_dir: Path) -> int:
    """DAG 실행 시 Target_Cal.xlsx Sheet1의 브랜드 바이럴 계산 목표를 채운다."""
    domain = next(domain for domain in DOMAINS if domain.key == "brand_viral")
    target_path = base_dir / domain.target_file
    progress_path = base_dir / str(domain.in_progress_file)
    if not target_path.is_file():
        logger.warning("BSP KPI 브랜드 바이럴 목표 엑셀 없음 | path=%s", target_path)
        return 0

    target_df = _read_kpi_sheet(target_path)
    target_weeks = _normalize_keys(target_df)
    if target_weeks.empty:
        logger.warning("BSP KPI 브랜드 바이럴 목표 엑셀 주차 없음 | path=%s", target_path)
        return 0

    progress_df = _read_kpi_sheet(progress_path)
    metric_order = {metric.key: order for order, metric in enumerate(domain.metrics)}
    event_targets = _build_brand_viral_event_target_frame(target_weeks, progress_df, metric_order)
    target_values = event_targets.pivot(index="week_start", columns="metric_key", values="target_value")

    wb = load_workbook(target_path)
    if SHEET_NAME not in wb.sheetnames:
        logger.warning("BSP KPI 브랜드 바이럴 목표 시트 없음 | path=%s sheet=%s", target_path, SHEET_NAME)
        return 0
    ws = wb[SHEET_NAME]

    week_column = _find_worksheet_column(ws, *WEEK_START_COLUMNS)
    if week_column is None:
        logger.warning("BSP KPI 브랜드 바이럴 목표 엑셀 주 시작일 컬럼 없음 | path=%s", target_path)
        return 0

    value_columns: dict[str, int] = {}
    added_columns: set[int] = set()
    for metric_key, header in BRAND_VIRAL_TARGET_WORKBOOK_COLUMNS.items():
        column, added = _ensure_worksheet_column(ws, header)
        value_columns[metric_key] = column
        if added:
            added_columns.add(column)

    changed = 0
    for row_number in range(2, ws.max_row + 1):
        week = _as_date(ws.cell(row=row_number, column=week_column).value)
        if week is None:
            continue
        for metric_key, column in value_columns.items():
            raw_value = (
                target_values.at[week, metric_key]
                if week in target_values.index and metric_key in target_values.columns
                else float("nan")
            )
            value = _excel_write_value(raw_value)
            cell = ws.cell(row=row_number, column=column)
            if column in added_columns:
                _copy_cell_style(ws.cell(row=row_number, column=column - 1), cell)
            if cell.value != value:
                cell.value = value
                changed += 1

    if changed or added_columns:
        wb.save(target_path)
    logger.info(
        "BSP KPI 브랜드 바이럴 목표 엑셀 동기화 | path=%s changed=%s added_columns=%s",
        target_path,
        changed,
        len(added_columns),
    )
    return changed


def _sync_brand_viral_actual_workbook(base_dir: Path) -> int:
    """DAG 실행 시 실적 파일 Sheet1의 주차별 상품 제공 금액·기준금액을 채운다."""
    domain = next(domain for domain in DOMAINS if domain.key == "brand_viral")
    actual_path = base_dir / domain.actual_file
    progress_path = base_dir / str(domain.in_progress_file)
    if not actual_path.is_file():
        logger.warning("BSP KPI 브랜드 바이럴 실적 엑셀 없음 | path=%s", actual_path)
        return 0

    actual_df = _read_kpi_sheet(actual_path)
    actual_weeks = _normalize_keys(actual_df)
    if actual_weeks.empty:
        logger.warning("BSP KPI 브랜드 바이럴 실적 엑셀 주차 없음 | path=%s", actual_path)
        return 0

    progress_df = _read_kpi_sheet(progress_path)
    metric_order = {metric.key: order for order, metric in enumerate(domain.metrics)}
    event_targets = _build_brand_viral_event_target_frame(actual_weeks, progress_df, metric_order)
    event_values = event_targets.pivot(index="week_start", columns="metric_key", values="target_value")
    participant_column = _find_column(actual_weeks, "참여자 수", "참여자 수 목표")

    wb = load_workbook(actual_path)
    if SHEET_NAME not in wb.sheetnames:
        logger.warning("BSP KPI 브랜드 바이럴 실적 시트 없음 | path=%s sheet=%s", actual_path, SHEET_NAME)
        return 0
    ws = wb[SHEET_NAME]

    week_column = _find_worksheet_column(ws, *WEEK_START_COLUMNS)
    if week_column is None:
        logger.warning("BSP KPI 브랜드 바이럴 실적 엑셀 주 시작일 컬럼 없음 | path=%s", actual_path)
        return 0

    sync_columns = {
        "product_cost": _ensure_worksheet_column(ws, "상품 제공 금액"),
        "participant_unit_amount": _ensure_worksheet_column(ws, "참여자 1명당 기준금액"),
    }
    changed = 0
    for row_number in range(2, ws.max_row + 1):
        week = _as_date(ws.cell(row=row_number, column=week_column).value)
        if week is None:
            continue
        product_value = (
            event_values.at[week, "product_cost"]
            if week in event_values.index and "product_cost" in event_values.columns
            else float("nan")
        )
        participant_value = (
            _to_number(ws.cell(row=row_number, column=_find_worksheet_column(ws, "참여자 수", "참여자 수 목표")).value)
            if participant_column is not None and _find_worksheet_column(ws, "참여자 수", "참여자 수 목표") is not None
            else float("nan")
        )
        actual_unit_value = (
            float(_excel_round(Decimal(str(product_value)) / Decimal(str(participant_value))))
            if not pd.isna(product_value) and not pd.isna(participant_value) and participant_value > 0
            else float("nan")
        )
        actual_values = {
            "product_cost": product_value,
            "participant_unit_amount": actual_unit_value,
        }
        for metric_key, (column, added) in sync_columns.items():
            raw_value = actual_values[metric_key]
            value = _excel_write_value(raw_value)
            cell = ws.cell(row=row_number, column=column)
            if added:
                _copy_cell_style(ws.cell(row=row_number, column=column - 1), cell)
            if cell.value != value:
                cell.value = value
                changed += 1

    if changed or any(added for _, added in sync_columns.values()):
        wb.save(actual_path)
    logger.info(
        "BSP KPI 브랜드 바이럴 실적 엑셀 동기화 | path=%s changed=%s added_columns=%s",
        actual_path,
        changed,
        sum(1 for _, added in sync_columns.values() if added),
    )
    return changed


# ============================================================
# 공개 함수
# ============================================================


def build_kpi_dataframe(base_dir: Path | None = None, *, today: date | None = None) -> pd.DataFrame:
    """엑셀을 읽어 세로형 통합 DataFrame을 만든다(저장하지 않음)."""
    base = Path(base_dir) if base_dir else BSP_KPI_DIR
    reference_day = today or pendulum.now(KST).date()

    frames = [_merge_domain(domain, base) for domain in DOMAINS]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    return _finalize(combined, today=reference_day)


def build_kpi_weekly(**context) -> str:
    """4개 엑셀을 세로형 마트로 통합해 parquet·csv로 저장하고 parquet 경로를 반환한다."""
    _sync_brand_viral_actual_workbook(BSP_KPI_DIR)
    _sync_brand_viral_target_workbook(BSP_KPI_DIR)
    df = build_kpi_dataframe()
    if df.empty:
        logger.warning("BSP KPI 통합 결과 없음 | base_dir=%s", BSP_KPI_DIR)
    _write_parquet_atomic(df, BSP_KPI_WEEKLY_PARQUET)
    _write_csv_atomic(df, BSP_KPI_WEEKLY_CSV)

    submitted_weeks = int(df[df["is_submitted"]].groupby(["week_start", "domain"]).ngroups) if not df.empty else 0
    logger.info(
        "BSP KPI 통합 완료 | rows=%s weeks=%s domains=%s submitted=%s → %s",
        len(df),
        df["week_start"].nunique() if not df.empty else 0,
        df["domain"].nunique() if not df.empty else 0,
        submitted_weeks,
        BSP_KPI_WEEKLY_PARQUET,
    )
    return str(BSP_KPI_WEEKLY_PARQUET)


def resolve_target_week_start(conf: dict | None = None, *, now: object = None) -> str:
    """알림 대상 주차(직전 완료 주차의 월요일)를 YYYY-MM-DD로 돌려준다.

    conf에 week_start가 있으면 그 값을 우선한다.
    """
    conf = conf if isinstance(conf, dict) else {}
    raw = str(conf.get("week_start") or "").strip()
    if raw:
        parsed = _as_date(raw)
        if parsed is None:
            raise ValueError("dag_run.conf.week_start는 YYYY-MM-DD 형식이어야 합니다.")
        return parsed.strftime("%Y-%m-%d")

    base = pendulum.instance(now).in_timezone(KST) if now else pendulum.now(KST)
    this_monday = base.date() - timedelta(days=base.date().weekday())
    return (this_monday - timedelta(days=7)).strftime("%Y-%m-%d")


def _week_period_text(target_week_start: str) -> str:
    """'2026-08-03' -> '08-03 ~ 08-09'."""
    week = _as_date(target_week_start)
    if week is None:
        return ""
    return f"{week:%m-%d} ~ {week + timedelta(days=6):%m-%d}"


def _display_owner_name(owner: str) -> str:
    """메일에서 사용할 담당자 호칭."""
    owner = _clean_text(owner)
    if not owner:
        return "담당자님"
    if owner in OWNER_EMAIL_DISPLAY_NAMES:
        return OWNER_EMAIL_DISPLAY_NAMES[owner]
    if owner.endswith("님"):
        return owner
    return f"{owner}님"


def _group_missing_rows_by_owner(
    rows: Sequence[BspKpiMissingRow],
) -> list[tuple[str, tuple[BspKpiMissingRow, ...]]]:
    grouped: dict[str, list[BspKpiMissingRow]] = {}
    for row in rows:
        grouped.setdefault(row.owner, []).append(row)
    return [(owner, tuple(owner_rows)) for owner, owner_rows in grouped.items()]


def _select_email_rows(
    result: BspKpiAlertResult,
    *,
    owner: str | None = None,
    missing_rows: Sequence[BspKpiMissingRow] | None = None,
) -> tuple[BspKpiMissingRow, ...]:
    if missing_rows is not None:
        return tuple(missing_rows)
    if owner:
        owner_rows = tuple(row for row in result.missing_rows if row.owner == owner)
        if owner_rows:
            return owner_rows
    if not result.missing_rows:
        return ()
    first_owner = result.missing_rows[0].owner
    return tuple(row for row in result.missing_rows if row.owner == first_owner)


def render_alert_text(result: BspKpiAlertResult) -> str:
    """텔레그램용 평문 본문."""
    period = _week_period_text(result.target_week_start)
    header = f"대상 주차: {result.target_week_start}"
    if result.week_label:
        header += f" ({result.week_label})"
    if period:
        header += f" · {period}"

    lines = [f"- {row.domain_name} : {row.owner}" for row in result.missing_rows]
    return "\n".join(
        [
            "[BSP KPI 주간 미입력]",
            header,
            f"미입력 {result.missing_count}건 / 점검 {result.checked_count}건",
            "",
            *lines,
            "",
            "지난주 실적을 아직 입력하지 않았습니다. 이번 주 내로 입력 부탁드립니다.",
            f"입력 위치: {ALERT_INPUT_LOCATION}",
        ]
    )


def render_alert_email_text(
    result: BspKpiAlertResult,
    *,
    owner: str | None = None,
    missing_rows: Sequence[BspKpiMissingRow] | None = None,
) -> str:
    """메일용 짧은 평문 본문."""
    rows = _select_email_rows(result, owner=owner, missing_rows=missing_rows)
    owner_text = _display_owner_name(owner or (rows[0].owner if rows else ""))
    period = _week_period_text(result.target_week_start)
    week_text = result.week_label or result.target_week_start
    if period:
        week_text += f" ({period})"
    domains = ", ".join(dict.fromkeys(row.domain_name for row in rows))
    if not domains:
        domains = "확인 필요"

    return "\n".join(
        [
            f"{owner_text}",
            "",
            "지난주 실적이 아직 입력되지 않았습니다.",
            "브랜드 전략기획 KPI 주간 실적 입력 부탁드립니다.",
            f"대상 주차: {week_text}",
            f"미입력 영역: {domains}",
            "",
            "입력될 때까지 매주 월·화 오전에 알림이 발송됩니다.",
            f"입력 위치: {ALERT_INPUT_LOCATION}",
        ]
    )


def render_alert_html(
    result: BspKpiAlertResult,
    *,
    owner: str | None = None,
    missing_rows: Sequence[BspKpiMissingRow] | None = None,
) -> str:
    """메일용 본문. 메일러 제약상 HTML로 감싸되 화면에는 평문처럼 보이게 한다."""
    text = render_alert_email_text(result, owner=owner, missing_rows=missing_rows)
    body = escape(text).replace("\n", "<br>\n")

    return f"""<!DOCTYPE html>
<html lang="ko">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:'Malgun Gothic',Arial,sans-serif; margin:20px; line-height:1.6; color:#222; font-size:14px;">
{body}
<p style="color:#999; font-size:12px; margin-top:20px;">본 메일은 자동으로 발송되었습니다.</p>
</body>
</html>"""


def render_alert_subject(result: BspKpiAlertResult, *, owner: str | None = None) -> str:
    """메일 제목. 관심친구 미리보기에서 담당자와 요청 내용이 바로 보이게 한다."""
    if owner is None and result.missing_rows:
        owner = result.missing_rows[0].owner
    prefix = f"{_display_owner_name(owner)} " if owner else ""
    return f"{prefix}브랜드 전략기획 KPI 주간입력 부탁드립니다"


def build_missing_alert(df: pd.DataFrame, *, target_week_start: str) -> BspKpiAlertResult:
    """지정 주차의 실적 미입력 도메인·담당자를 판정해 알림 본문을 만든다.

    해당 주차 행이 있는 도메인만 점검하고, 그 도메인의 지표가 전부 비어 있으면 미입력이다.
    부분 입력은 통과시킨다.
    """
    week = _as_date(target_week_start)
    if week is None:
        return BspKpiAlertResult(message="", skipped_reason="대상 주차 형식 오류")
    week_text = week.strftime("%Y-%m-%d")

    if df is None or df.empty:
        return BspKpiAlertResult(message="", target_week_start=week_text, skipped_reason="통합 데이터 없음")

    working = df.copy()
    working["_week"] = working["week_start"].map(_as_date)
    working = working[working["_week"] == week]
    if working.empty:
        return BspKpiAlertResult(
            message="",
            target_week_start=week_text,
            skipped_reason=f"대상 주차 행 없음: {week_text}",
        )

    week_label = _pick_text(working["week_label"].iloc[0], "")
    actual_files = {domain.key: domain.actual_file for domain in DOMAINS}

    missing_rows: list[BspKpiMissingRow] = []
    checked = 0
    for domain_key, group in working.groupby("domain", sort=False):
        checked += 1
        required = group.get("alert_required")
        if required is None:
            required_mask = pd.Series(True, index=group.index)
        else:
            required_mask = required.fillna(True).astype(bool)
        required_group = group[required_mask]
        if not required_group.empty and bool(required_group["has_actual"].fillna(False).all()):
            continue
        missing_rows.append(
            BspKpiMissingRow(
                owner=_pick_text(group["owner"].iloc[0], "담당자 미지정"),
                domain=str(domain_key),
                domain_name=_pick_text(group["domain_name"].iloc[0], str(domain_key)),
                actual_file=actual_files.get(str(domain_key), ""),
            )
        )

    result = BspKpiAlertResult(
        message="",
        target_week_start=week_text,
        week_label=week_label,
        checked_count=checked,
        missing_count=len(missing_rows),
        missing_rows=tuple(missing_rows),
    )
    if not missing_rows:
        return result
    return replace(result, message=render_alert_text(result))


def dispatch_missing_alert(
    df: pd.DataFrame,
    sender: Callable[[str], object],
    *,
    target_week_start: str,
) -> BspKpiAlertResult:
    """미입력 본문이 있을 때만 주입된 발송 함수를 한 번 호출한다."""
    result = build_missing_alert(df, target_week_start=target_week_start)
    if result.message:
        sender(result.message)
    return result


def _resolve_email_values(*items: object) -> list[str]:
    from modules.transform.utility.mail_recipients import resolve_mail_recipients

    return resolve_mail_recipients(*items)


def _normalize_owner_for_email(owner: object) -> str:
    from modules.transform.utility.mail_recipients import normalize_manager_name

    normalized = normalize_manager_name(owner)
    return normalized or _clean_text(owner)


def resolve_owner_alert_emails(owner: str) -> list[str]:
    """담당자 1명에게 발송할 메일 주소만 반환한다."""
    configured = BSP_KPI_ALERT_EMAILS
    if not isinstance(configured, dict):
        return _resolve_email_values(configured)

    owner_key = _normalize_owner_for_email(owner)
    if owner_key in configured:
        return _resolve_email_values(configured[owner_key])

    owner_text = _clean_text(owner)
    if owner_text in configured:
        return _resolve_email_values(configured[owner_text])
    return []


def resolve_alert_emails() -> list[str]:
    """상단 담당자별 수신자 설정에서 None·빈값·중복을 걸러낸 전체 메일 주소 목록."""
    configured = BSP_KPI_ALERT_EMAILS
    if isinstance(configured, dict):
        return _resolve_email_values(*configured.values())
    return _resolve_email_values(configured)


def _default_telegram_sender(message: str) -> object:
    from modules.transform.utility.notifier import send_telegram_chunks

    return send_telegram_chunks(message)


def _default_email_sender(subject: str, html_content: str, emails: list[str]) -> object:
    from modules.transform.utility.mailer import send_email

    return send_email(subject=subject, html_content=html_content, to_emails=emails, raise_on_error=True)


def _email_send_succeeded(send_result: object) -> bool:
    """메일러가 실패를 문자열로 반환하는 구 구현도 실패로 판정한다."""
    if send_result is False:
        return False
    text = str(send_result or "")
    if not text:
        return True
    lowered = text.lower()
    return "메일 발송 실패" not in text and "smtp_auth_fail" not in lowered and "failed" not in lowered


def _is_smtp_auth_error(error: object) -> bool:
    text = str(error or "").lower()
    auth_markers = (
        "535",
        "5.7.8",
        "username and password not accepted",
        "badcredentials",
        "authentication",
        "smtp_auth_fail",
    )
    return any(marker in text for marker in auth_markers)


def render_smtp_auth_failure_text(*, owner: str, emails: Sequence[str], error: object) -> str:
    """SMTP 앱 비밀번호 갱신이 필요한 운영 알림 본문."""
    owner_text = _display_owner_name(owner)
    recipients = ", ".join(emails) if emails else "수신자 없음"
    error_text = str(error or "").splitlines()[0][:300]
    return "\n".join(
        [
            "[BSP KPI 메일 발송 실패] SMTP 인증 필요",
            "doridang_conn_smtp_gmail Gmail SMTP 인증이 실패했습니다.",
            f"실패 대상: {owner_text} ({recipients})",
            f"오류: {error_text}",
            "",
            "조치:",
            "1. https://myaccount.google.com/apppasswords 에서 앱 비밀번호를 새로 발급",
            "2. Airflow UI > Admin > Connections > doridang_conn_smtp_gmail > Password 교체",
            "3. 새 앱 비밀번호는 채팅/문서에 공유하지 말고 UI에 직접 입력",
        ]
    )


def notify_missing_alert(
    df: pd.DataFrame,
    *,
    target_week_start: str,
    telegram_sender: Callable[[str], object] | None = None,
    email_sender: Callable[[str, str, list[str]], object] | None = None,
) -> BspKpiAlertResult:
    """미입력 본문을 한 번 만들어 설정된 채널로 발송한다.

    텔레그램은 평문, 메일은 HTML로 나간다. 한 채널이 실패해도 다른 채널 발송은 계속한다.
    """
    result = build_missing_alert(df, target_week_start=target_week_start)
    if not result.message:
        return result

    sent_telegram = False
    sent_emails: tuple[str, ...] = ()
    smtp_auth_alert_sent = False

    if BSP_KPI_ALERT_TELEGRAM:
        sender = telegram_sender or _default_telegram_sender
        try:
            sender(render_alert_text(result))
            sent_telegram = True
        except Exception as exc:
            logger.error("BSP KPI 텔레그램 발송 실패: %r", exc)

    sender = email_sender or _default_email_sender
    sent_email_values: list[str] = []
    for owner, owner_rows in _group_missing_rows_by_owner(result.missing_rows):
        owner_emails = resolve_owner_alert_emails(owner)
        if not owner_emails:
            logger.info("BSP KPI 메일 수신자 없음 | owner=%s", owner)
            continue
        try:
            send_result = sender(
                render_alert_subject(result, owner=owner),
                render_alert_html(result, owner=owner, missing_rows=owner_rows),
                owner_emails,
            )
            if _email_send_succeeded(send_result):
                sent_email_values.extend(owner_emails)
                continue
            logger.error("BSP KPI 메일 발송 실패 반환 | owner=%s to=%s result=%r", owner, owner_emails, send_result)
            if _is_smtp_auth_error(send_result) and not smtp_auth_alert_sent:
                try:
                    (telegram_sender or _default_telegram_sender)(
                        render_smtp_auth_failure_text(owner=owner, emails=owner_emails, error=send_result)
                    )
                    smtp_auth_alert_sent = True
                except Exception as alert_exc:
                    logger.error("BSP KPI SMTP 인증 실패 텔레그램 발송 실패: %r", alert_exc)
        except Exception as exc:
            logger.error("BSP KPI 메일 발송 실패 | owner=%s to=%s error=%r", owner, owner_emails, exc)
            if _is_smtp_auth_error(exc) and not smtp_auth_alert_sent:
                try:
                    (telegram_sender or _default_telegram_sender)(
                        render_smtp_auth_failure_text(owner=owner, emails=owner_emails, error=exc)
                    )
                    smtp_auth_alert_sent = True
                except Exception as alert_exc:
                    logger.error("BSP KPI SMTP 인증 실패 텔레그램 발송 실패: %r", alert_exc)
    if sent_email_values:
        sent_emails = tuple(_resolve_email_values(sent_email_values))

    return replace(result, sent_telegram=sent_telegram, sent_emails=sent_emails)


__all__ = [
    "BspKpiAlertResult",
    "BspKpiMissingRow",
    "DomainSpec",
    "MetricSpec",
    "BSP_KPI_ALERT_EMAILS",
    "BSP_KPI_ALERT_TELEGRAM",
    "DOMAINS",
    "build_kpi_dataframe",
    "build_kpi_weekly",
    "build_missing_alert",
    "dispatch_missing_alert",
    "notify_missing_alert",
    "render_alert_email_text",
    "render_alert_html",
    "render_alert_subject",
    "render_alert_text",
    "resolve_alert_emails",
    "resolve_owner_alert_emails",
    "resolve_target_week_start",
]
