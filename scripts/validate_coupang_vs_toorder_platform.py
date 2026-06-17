"""Validate collected Coupang orders against ToOrder store-platform totals."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pendulum

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.transform.utility.paths import ANALYTICS_DB, COUPANG_ORDERS_DB
from modules.transform.utility.store_normalize import normalize_for_join

logger = logging.getLogger(__name__)

TOORDER_PLATFORM_PARQUET = (
    ANALYTICS_DB
    / "toorder_daily_store_platform"
    / "toorder_store_platform_daily.parquet"
)
COUPANG_PLATFORMS = ("쿠팡이츠", "쿠팡 포장")


def _default_target_date() -> str:
    return pendulum.now("Asia/Seoul").subtract(days=1).format("YYYY-MM-DD")


def _read_toorder_platform() -> pd.DataFrame:
    if not TOORDER_PLATFORM_PARQUET.exists():
        logger.warning("ToOrder parquet 없음: %s", TOORDER_PLATFORM_PARQUET)
        return pd.DataFrame(columns=["date", "store", "platform", "price"])
    try:
        return pd.read_parquet(TOORDER_PLATFORM_PARQUET)
    except Exception as exc:
        logger.warning("ToOrder parquet 읽기 실패: %s / %s", TOORDER_PLATFORM_PARQUET, exc)
        return pd.DataFrame(columns=["date", "store", "platform", "price"])


def _latest_available_date(df: pd.DataFrame) -> str | None:
    if df.empty or "date" not in df.columns:
        return None
    dates = df["date"].dropna().astype(str)
    return None if dates.empty else str(dates.max())


def _toorder_coupang_by_store(target_date: str) -> dict[str, int]:
    df = _read_toorder_platform()
    latest = _latest_available_date(df)
    if df.empty:
        logger.warning("⚠️ %s 기준 데이터 없음 - 최신 가용일: %s", target_date, latest)
        return {}

    sub = df[
        (df["date"].astype(str) == target_date)
        & (df["platform"].isin(COUPANG_PLATFORMS))
    ].copy()
    if sub.empty:
        logger.warning("⚠️ %s 기준 데이터 없음 - 최신 가용일: %s", target_date, latest)
        return {}

    sub["_key"] = normalize_for_join(sub["store"])
    result = sub.groupby("_key")["price"].sum().astype(int).to_dict()
    logger.info(
        "ToOrder 쿠팡 합계: %s / %d매장 / %s원",
        target_date,
        len(result),
        f"{sum(result.values()):,}",
    )
    return result


def _numeric_price(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    ).fillna(0)


def _coupang_collected_by_store(target_date: str, price_col: str) -> dict[str, int]:
    ym = target_date[:7]
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    date_patterns = [
        target_date.replace("-", "."),
        f"{dt.year}.{dt.month}.{dt.day}",
        f"{dt.year}.{dt.month:02d}.{dt.day:02d}",
    ]
    csv_paths = list(COUPANG_ORDERS_DB.glob(f"brand=*/store=*/ym={ym}/orders_{ym}.csv"))
    if not csv_paths:
        logger.warning("쿠팡 orders CSV 없음: ym=%s / dir=%s", ym, COUPANG_ORDERS_DB)
        return {}

    result: dict[str, int] = {}
    for csv_path in csv_paths:
        store = csv_path.parts[-3][len("store=") :]
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
        except Exception as exc:
            logger.warning("CSV 읽기 실패: %s / %s", csv_path, exc)
            continue

        if df.empty:
            continue
        if "order_date" not in df.columns or price_col not in df.columns:
            logger.warning("필수 컬럼 없음: %s / price_col=%s", csv_path, price_col)
            continue

        mask = df["order_date"].apply(lambda x: any(p in str(x) for p in date_patterns))
        daily = df[mask]
        if daily.empty:
            continue

        price = _numeric_price(daily[price_col])
        if "is_cancelled" in daily.columns:
            cancel = daily["is_cancelled"].astype(str).str.strip().str.upper() == "Y"
        else:
            cancel = pd.Series(False, index=daily.index)

        net_sales = int(price[~cancel].sum()) - int(price[cancel].sum())
        key = normalize_for_join(pd.Series([store])).iloc[0]
        result[key] = result.get(key, 0) + net_sales

    logger.info(
        "수집 쿠팡 합계: %s / %d매장 / %s원 / price_col=%s",
        target_date,
        len(result),
        f"{sum(result.values()):,}",
        price_col,
    )
    return result


def compare(target_date: str, price_col: str = "total_price", tol: int = 0) -> dict:
    toorder = _toorder_coupang_by_store(target_date)
    collected = _coupang_collected_by_store(target_date, price_col)

    rows = []
    for store in sorted(set(toorder) | set(collected)):
        toorder_amount = int(toorder.get(store, 0))
        collected_amount = int(collected.get(store, 0))
        diff = toorder_amount - collected_amount
        matched = abs(diff) <= tol
        gap = (toorder_amount == 0) ^ (collected_amount == 0)
        row = {
            "store": store,
            "toorder": toorder_amount,
            "collected": collected_amount,
            "diff": diff,
            "matched": matched,
            "gap": gap,
        }
        rows.append(row)

    toorder_total = int(sum(toorder.values()))
    collected_total = int(sum(collected.values()))
    diff_total = toorder_total - collected_total
    mismatches = [r for r in rows if not r["matched"]]
    gaps = [r for r in rows if r["gap"]]

    if not toorder:
        logger.warning("⚠️ %s ToOrder 기준 데이터 없음 - 비교 스킵", target_date)
    if not collected:
        logger.warning("⚠️ %s 수집 데이터 없음 - 전체 gap으로 비교", target_date)

    logger.info(
        "총액 비교: ToOrder=%s원 / 수집=%s원 / 차이=%s원 / 매칭=%d/%d",
        f"{toorder_total:,}",
        f"{collected_total:,}",
        f"{diff_total:,}",
        len(rows) - len(mismatches),
        len(rows),
    )
    for row in mismatches:
        logger.warning(
            "불일치: %s / ToOrder=%s / 수집=%s / 차이=%s / gap=%s",
            row["store"],
            f"{row['toorder']:,}",
            f"{row['collected']:,}",
            f"{row['diff']:,}",
            row["gap"],
        )

    return {
        "target_date": target_date,
        "price_col": price_col,
        "tol": tol,
        "toorder_total": toorder_total,
        "collected_total": collected_total,
        "diff_total": diff_total,
        "matched": len(mismatches) == 0,
        "matched_count": len(rows) - len(mismatches),
        "store_count": len(rows),
        "mismatched_stores": [r["store"] for r in mismatches],
        "gap_stores": [r["store"] for r in gaps],
        "rows": rows,
    }


def _save_csv(result: dict) -> Path:
    out_dir = ANALYTICS_DB / "unified_sales_validation"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"coupang_toorder_platform_{result['target_date']}.csv"
    pd.DataFrame(result["rows"]).to_csv(out_path, index=False, encoding="utf-8-sig")
    logger.info("CSV 저장: %s", out_path)
    return out_path


def show_latest() -> None:
    df = _read_toorder_platform()
    latest = _latest_available_date(df)
    if not latest:
        logger.warning("최신 가용일 없음")
        return

    sub = df[
        (df["date"].astype(str) == latest)
        & (df["platform"].isin(COUPANG_PLATFORMS))
    ].copy()
    if sub.empty:
        logger.warning("최신 가용일 %s 쿠팡 데이터 없음", latest)
        return

    sub["_key"] = normalize_for_join(sub["store"])
    by_store = sub.groupby("_key")["price"].sum().astype(int).sort_index()
    logger.info("최신 가용일: %s / %d매장 / 합계 %s원", latest, len(by_store), f"{int(by_store.sum()):,}")
    for store, amount in by_store.items():
        logger.info("%s: %s원", store, f"{int(amount):,}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Coupang collected orders against ToOrder platform parquet."
    )
    parser.add_argument("--date", default=_default_target_date(), help="Target date YYYY-MM-DD")
    parser.add_argument(
        "--price-col",
        default="total_price",
        choices=("total_price", "매출액"),
        help="Collected orders price column to compare",
    )
    parser.add_argument("--tol", type=int, default=0, help="Allowed per-store amount difference")
    parser.add_argument("--show-latest", action="store_true", help="Show latest ToOrder Coupang totals")
    parser.add_argument("--csv", action="store_true", help="Save per-store comparison CSV")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    args = parse_args()
    if args.show_latest:
        show_latest()
        return

    result = compare(args.date, price_col=args.price_col, tol=args.tol)
    if args.csv:
        _save_csv(result)


if __name__ == "__main__":
    main()
