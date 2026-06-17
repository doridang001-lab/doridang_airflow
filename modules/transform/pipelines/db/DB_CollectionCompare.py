"""Build collection comparison mart for delivery sales sources."""

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

import pandas as pd

from modules.transform.utility.paths import (
    ANALYTICS_DB,
    BAEMIN_ORDERS_DB,
    COLLECTION_COMPARE_PATH,
    COUPANG_ORDERS_DB,
    MART_DB,
)

logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")

PLATFORM_GROUPS = {
    "배달의민족": {"배달의민족", "배민1", "배민 포장", "배민 사장"},
    "쿠팡이츠": {"쿠팡이츠", "쿠팡 포장"},
}
PLATFORM_TO_GROUP = {
    platform: group
    for group, platforms in PLATFORM_GROUPS.items()
    for platform in platforms
}
GAP_THRESHOLD = 2.0


@dataclass(frozen=True)
class SourceSpec:
    key: str
    label: str
    loader: Callable[[], pd.DataFrame]
    supported_groups: set[str]


def _normalize_store_name(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.replace(r"\s+", "", regex=True)


def _normalize_grouped(
    df: pd.DataFrame,
    date_col: str,
    store_col: str,
    platform_col: str,
    amt_col: str,
) -> pd.DataFrame:
    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    out["store"] = _normalize_store_name(df[store_col])
    out["platform_group"] = df[platform_col].astype(str).str.strip().map(PLATFORM_TO_GROUP)
    out["amt"] = pd.to_numeric(df[amt_col], errors="coerce").fillna(0)
    out = out.dropna(subset=["date", "platform_group"])
    out = out[out["store"] != ""]
    out = out.groupby(["date", "store", "platform_group"], as_index=False)["amt"].sum()
    out["amt"] = out["amt"].round().astype(int)
    return out


def _empty_long() -> pd.DataFrame:
    return pd.DataFrame(columns=["date", "store", "platform_group", "amt"])


def _read_parquet_parts(files: list[Path], columns: list[str]) -> pd.DataFrame:
    parts = []
    for path in files:
        try:
            parts.append(pd.read_parquet(path, columns=columns))
        except Exception as exc:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, exc)
    if not parts:
        return pd.DataFrame(columns=columns)
    return pd.concat(parts, ignore_index=True)


def load_toorder() -> pd.DataFrame:
    path = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"
    if not path.exists():
        logger.warning("toorder parquet 없음: %s", path)
        return _empty_long()
    df = pd.read_parquet(path, columns=["date", "store", "platform", "price"])
    return _normalize_grouped(df, "date", "store", "platform", "price")


def load_unified() -> pd.DataFrame:
    root = MART_DB / "unified_sales_grp"
    files = sorted(root.glob("unified_sales_*.parquet")) if root.exists() else []
    if not files:
        logger.warning("unified_sales parquet 없음: %s", root)
        return _empty_long()
    df = _read_parquet_parts(files, ["sale_date", "store", "platform", "total_price"])
    if df.empty:
        return _empty_long()
    return _normalize_grouped(df, "sale_date", "store", "platform", "total_price")


def _partition_value(path: Path, prefix: str) -> str:
    token = f"{prefix}="
    for part in path.parts:
        if part.startswith(token):
            return part[len(token):].strip()
    return ""


def _baemin_order_date(series: pd.Series) -> pd.Series:
    raw = series.astype(str)
    extracted = raw.str.extract(r"(\d{4}\.\s*\d{2}\.\s*\d{2}\.)", expand=False)
    return pd.to_datetime(extracted, format="%Y. %m. %d.", errors="coerce").dt.strftime("%Y-%m-%d")


def _coupang_order_date(series: pd.Series) -> pd.Series:
    raw = series.astype(str)
    extracted = raw.str.extract(r"(\d{4}\.\d{2}\.\d{2})", expand=False)
    return pd.to_datetime(extracted, format="%Y.%m.%d", errors="coerce").dt.strftime("%Y-%m-%d")


def load_baemin_macro() -> pd.DataFrame:
    files = sorted(BAEMIN_ORDERS_DB.glob("brand=*/store=*/ym=*/orders_*.parquet"))
    if not files:
        logger.warning("baemin orders parquet 없음: %s", BAEMIN_ORDERS_DB)
        return _empty_long()

    parts = []
    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("baemin parquet 로드 실패, 스킵: %s | %s", path, exc)
            continue
        if df.empty:
            continue
        if "store" not in df.columns:
            df = df.copy()
            df["store"] = _partition_value(path, "store") or df.get("store_name", "")
        parts.append(df)

    if not parts:
        return _empty_long()

    df = pd.concat(parts, ignore_index=True)
    required = {"주문번호", "주문시각", "store", "총결제금액"}
    missing = required - set(df.columns)
    if missing:
        logger.warning("baemin 필수 컬럼 없음: %s", sorted(missing))
        return _empty_long()

    amt_text = df["총결제금액"].astype(str).str.replace(",", "", regex=False).str.strip()
    df = df[amt_text.ne("") & amt_text.ne("nan")].copy()
    if df.empty:
        return _empty_long()

    df["총결제금액"] = pd.to_numeric(
        df["총결제금액"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    ).fillna(0)
    grouped = df.groupby("주문번호", as_index=False).agg(
        주문시각=("주문시각", "max"),
        store=("store", "max"),
        총결제금액=("총결제금액", "max"),
    )
    grouped["date"] = _baemin_order_date(grouped["주문시각"])
    grouped["store"] = _normalize_store_name(grouped["store"])
    grouped["platform_group"] = "배달의민족"
    grouped["amt"] = grouped["총결제금액"]
    grouped = grouped.dropna(subset=["date"])
    grouped = grouped[grouped["store"] != ""]
    if grouped.empty:
        return _empty_long()
    grouped = grouped.groupby(["date", "store", "platform_group"], as_index=False)["amt"].sum()
    grouped["amt"] = grouped["amt"].round().astype(int)
    return grouped[["date", "store", "platform_group", "amt"]]


def load_coupang_macro() -> pd.DataFrame:
    files = sorted(COUPANG_ORDERS_DB.glob("brand=*/store=*/ym=*/orders_*.parquet"))
    if not files:
        logger.warning("coupang orders parquet 없음: %s", COUPANG_ORDERS_DB)
        return _empty_long()

    parts = []
    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("coupang parquet 로드 실패, 스킵: %s | %s", path, exc)
            continue
        if df.empty:
            continue
        df = df.copy()
        df["store"] = _partition_value(path, "store") or df.get("store", df.get("store_name", ""))
        parts.append(df)

    if not parts:
        return _empty_long()

    df = pd.concat(parts, ignore_index=True)
    required = {"order_id", "order_date", "store", "매출액"}
    missing = required - set(df.columns)
    if missing:
        logger.warning("coupang 필수 컬럼 없음: %s", sorted(missing))
        return _empty_long()

    amt_text = df["매출액"].astype(str).str.replace(",", "", regex=False).str.strip()
    df = df[amt_text.ne("") & amt_text.ne("nan")].copy()
    if df.empty:
        return _empty_long()

    df["매출액"] = pd.to_numeric(
        df["매출액"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    ).fillna(0)
    grouped = df.groupby("order_id", as_index=False).agg(
        order_date=("order_date", "max"),
        store=("store", "max"),
        매출액=("매출액", "max"),
    )
    grouped["date"] = _coupang_order_date(grouped["order_date"])
    grouped["store"] = _normalize_store_name(grouped["store"])
    grouped["platform_group"] = "쿠팡이츠"
    grouped["amt"] = grouped["매출액"]
    grouped = grouped.dropna(subset=["date"])
    grouped = grouped[grouped["store"] != ""]
    if grouped.empty:
        return _empty_long()
    grouped = grouped.groupby(["date", "store", "platform_group"], as_index=False)["amt"].sum()
    grouped["amt"] = grouped["amt"].round().astype(int)
    return grouped[["date", "store", "platform_group", "amt"]]


SOURCES = [
    SourceSpec("toorder", "토더", load_toorder, {"배달의민족", "쿠팡이츠"}),
    SourceSpec("unified", "통합", load_unified, {"배달의민족", "쿠팡이츠"}),
    SourceSpec("baemin_macro", "배민매크로", load_baemin_macro, {"배달의민족"}),
    SourceSpec("coupang_macro", "쿠팡매크로", load_coupang_macro, {"쿠팡이츠"}),
]
SOURCE_LABELS = {spec.key: spec.label for spec in SOURCES}


def _expected_keys(platform_group: str) -> list[str]:
    return [spec.key for spec in SOURCES if platform_group in spec.supported_groups]


def _row_metrics(row: pd.Series) -> pd.Series:
    expected = _expected_keys(str(row["platform_group"]))
    present = [key for key in expected if bool(row.get(f"present_{key}", False))]
    missing = [key for key in expected if not bool(row.get(f"present_{key}", False))]
    amounts_by_key = {key: int(row.get(f"amt_{key}", 0)) for key in expected}
    amounts = list(amounts_by_key.values())

    amt_max = max(amounts) if amounts else 0
    amt_min = min(amounts) if amounts else 0
    gap = amt_max - amt_min
    gap_rate = round((gap / amt_max * 100), 2) if amt_max else 0.0
    if missing:
        status = "누락"
    elif gap_rate >= GAP_THRESHOLD:
        status = "차이"
    else:
        status = "정상"

    baseline_source = ""
    for key in ("unified", "toorder", "baemin_macro", "coupang_macro"):
        if key in present:
            baseline_source = key
            break
    baseline_amt = int(row.get(f"amt_{baseline_source}", 0)) if baseline_source else 0

    issue_by_key = {}
    for spec in SOURCES:
        key = spec.key
        if key not in expected:
            issue_by_key[key] = "비교대상아님"
        elif key in missing:
            issue_by_key[key] = "누락"
        elif gap_rate >= GAP_THRESHOLD and int(row.get(f"amt_{key}", 0)) < amt_max:
            issue_by_key[key] = "차이_낮음"
        elif gap_rate >= GAP_THRESHOLD and int(row.get(f"amt_{key}", 0)) > amt_min:
            issue_by_key[key] = "차이_높음"
        else:
            issue_by_key[key] = "정상"

    issue_summary = []
    for spec in SOURCES:
        issue = issue_by_key[spec.key]
        if issue in {"정상", "비교대상아님"}:
            continue
        issue_summary.append(f"{spec.label} {issue}")

    return pd.Series(
        {
            "n_present": len(present),
            "n_expected": len(expected),
            "baseline_source": SOURCE_LABELS.get(baseline_source, baseline_source),
            "baseline_amt": baseline_amt,
            "amt_max": amt_max,
            "amt_min": amt_min,
            "gap": gap,
            "gap_rate": gap_rate,
            "status": status,
            "missing_sources": ",".join(SOURCE_LABELS.get(key, key) for key in missing),
            "issue_summary": ",".join(issue_summary),
        }
        | {f"issue_{spec.key}": issue_by_key[spec.key] for spec in SOURCES}
    )


def _empty_output() -> pd.DataFrame:
    cols = ["date", "store", "platform_group"]
    for spec in SOURCES:
        cols.append(f"amt_{spec.key}")
        cols.append(f"present_{spec.key}")
        cols.append(f"issue_{spec.key}")
    cols.extend([
        "n_present",
        "n_expected",
        "baseline_source",
        "baseline_amt",
        "amt_max",
        "amt_min",
        "gap",
        "gap_rate",
        "status",
        "missing_sources",
        "issue_summary",
        "checked_at",
    ])
    return pd.DataFrame(columns=cols)


def build_collection_compare() -> str:
    frames = []
    for spec in SOURCES:
        try:
            frame = spec.loader()
            if frame.empty:
                logger.warning("소스 데이터 없음: %s", spec.key)
                continue
            frame = frame.copy()
            frame["source"] = spec.key
            frames.append(frame)
        except Exception as exc:
            logger.warning("소스 로드 실패: %s | %s", spec.key, exc, exc_info=True)

    if not frames:
        wide = _empty_output()
    else:
        long_df = pd.concat(frames, ignore_index=True)
        wide = long_df.pivot_table(
            index=["date", "store", "platform_group"],
            columns="source",
            values="amt",
            aggfunc="sum",
        ).reset_index()

        for spec in SOURCES:
            key = spec.key
            if key in wide.columns:
                wide[f"amt_{key}"] = wide[key].fillna(0).round().astype(int)
                wide[f"present_{key}"] = wide[key].notna()
            else:
                wide[f"amt_{key}"] = 0
                wide[f"present_{key}"] = False

        wide = wide.drop(columns=[spec.key for spec in SOURCES if spec.key in wide.columns])
        metrics = wide.apply(_row_metrics, axis=1)
        wide = pd.concat([wide, metrics], axis=1)
        wide["checked_at"] = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

        ordered = _empty_output().columns.tolist()
        wide = wide.reindex(columns=ordered)
        wide = wide.sort_values(["date", "store", "platform_group"]).reset_index(drop=True)

    COLLECTION_COMPARE_PATH.parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(COLLECTION_COMPARE_PATH, index=False, engine="pyarrow")
    diff_count = int((wide["status"] == "차이").sum()) if "status" in wide.columns else 0
    missing_count = int((wide["status"] == "누락").sum()) if "status" in wide.columns else 0
    return (
        f"수집비교 {len(wide)}행 -> {COLLECTION_COMPARE_PATH} "
        f"(차이 {diff_count}, 누락 {missing_count})"
    )
