# -*- coding: utf-8 -*-
r"""배민 즉시할인 분해 미수집(공란) 현황을 날짜/매장별로 집계한다 (읽기 전용).

`즉시할인 > 0` 인데 `즉시할인_파트너부담` 이 공란인 주문은 상세시트 파싱에 실패한
것이다. DB_DeliveryCommission 이 이 경우를 전액 파트너부담으로 폴백하므로
`배민_즉시할인` 마트가 틀어진다.

수집기 수정(DB_Beamin_04_orders `_get_discount_popup_detail`) 전후를 비교하거나,
백필 대상 날짜를 고르는 데 쓴다.

사용:
    python scripts/analyze/baemin_instant_discount_gap.py            # 기본 경로
    python scripts/analyze/baemin_instant_discount_gap.py <orders_dir> [--days 30]

주의: Windows 호스트에서 `C:\opt\airflow` 가 존재하면 paths.py 가 컨테이너 경로를
잡아 빈 결과가 나온다. 그때는 orders_dir 을 직접 넘긴다.
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

_EMPTY_TOKENS = {"", "nan", "NaN", "None", "<NA>", "null", "NULL"}


def _blank(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().isin(_EMPTY_TOKENS)


def _money(series: pd.Series) -> pd.Series:
    cleaned = (
        series.fillna("").astype(str).str.replace(r"[^\d-]", "", regex=True).replace("", "0")
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0)


def _order_date(series: pd.Series) -> pd.Series:
    parts = series.fillna("").astype(str).str.extract(r"(\d{4})\.\s*(\d{2})\.\s*(\d{2})")
    return parts.apply(lambda row: "-".join(row) if row.notna().all() else "", axis=1)


def _default_orders_dir() -> Path:
    from modules.transform.utility.paths import BAEMIN_ORDERS_DB

    return BAEMIN_ORDERS_DB


def scan(orders_dir: Path, days: int | None) -> str:
    files = sorted(orders_dir.glob("brand=*/store=*/ym=*/orders_*.parquet"))
    by_date: dict[str, dict] = defaultdict(lambda: {"orders": 0, "blank": 0, "stores": set()})
    total_rows = total_blank = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:  # noqa: BLE001 - 스캔은 한 파티션 실패로 멈추지 않는다
            print(f"read fail: {path} / {exc}", file=sys.stderr)
            continue
        if df.empty or "즉시할인" not in df.columns:
            continue

        store = path.parent.parent.name.removeprefix("store=")
        brand = path.parent.parent.parent.name.removeprefix("brand=")
        df = df[df.get("주문상태", pd.Series(dtype=str)).astype(str).str.strip().eq("배달완료")]
        if df.empty:
            continue

        dates = _order_date(df["주문시각"])
        has_discount = _money(df["즉시할인"]).gt(0)
        is_blank = (
            _blank(df["즉시할인_파트너부담"])
            if "즉시할인_파트너부담" in df.columns
            else pd.Series(True, index=df.index)
        )
        bad = has_discount & is_blank
        total_rows += int(has_discount.sum())
        total_blank += int(bad.sum())

        for date in sorted({value for value in dates[bad].tolist() if value}):
            by_date[date]["blank"] += int((dates.eq(date) & bad).sum())
            by_date[date]["orders"] += int((dates.eq(date) & has_discount).sum())
            by_date[date]["stores"].add(f"{brand} {store}")

    rate = 100.0 * total_blank / total_rows if total_rows else 0.0
    lines = [
        f"orders_dir={orders_dir}",
        f"partitions={len(files)}  즉시할인>0 행={total_rows:,}  "
        f"분해공란 행={total_blank:,} ({rate:.1f}%)",
        "",
        "날짜별 (최근순)",
    ]
    dates_desc = sorted(by_date, reverse=True)
    if days:
        dates_desc = dates_desc[:days]
    for date in dates_desc:
        item = by_date[date]
        lines.append(
            f"  {date}  공란={item['blank']:5d} / 할인주문={item['orders']:5d}  "
            f"매장={len(item['stores'])}"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("orders_dir", nargs="?", help="baemin_macro/orders 경로")
    parser.add_argument("--days", type=int, default=30, help="출력할 날짜 수 (0=전체)")
    args = parser.parse_args()

    orders_dir = Path(args.orders_dir) if args.orders_dir else _default_orders_dir()
    if not orders_dir.exists():
        raise SystemExit(f"orders 경로 없음: {orders_dir}")
    print(scan(orders_dir, args.days or None))


if __name__ == "__main__":
    main()
