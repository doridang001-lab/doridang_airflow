"""
OKPOS Raw 매출(OKPOS Sales) 수집 누락 체크 리포트.

사용 예)
  python -X utf8 scripts/okpos_sales_missing_report.py 2026-01-01 2026-01-31

출력:
  - 매장(store) × 페이지(today/receipt)별로 보유 날짜(have) / 누락 날짜(missing) 요약
  - "중간 누락" (앞뒤 날짜는 있는데 특정 날짜만 비는 경우)도 잡을 수 있도록 missing 샘플을 보여줌
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _iter_dates(d1: date, d2: date) -> list[str]:
    cur = d1
    out: list[str] = []
    while cur <= d2:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _ym(d: str) -> str:
    # d: YYYY-MM-DD
    return d[:7]


def _read_sale_date_set(csv_path: Path) -> set[str]:
    """okpos_order(.csv) / okpos_order_item(.csv)에서 sale_date 집합을 구성한다."""
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return set()
    dates: set[str] = set()
    for chunk in pd.read_csv(
        csv_path,
        dtype=str,
        usecols=["sale_date"],
        encoding="utf-8-sig",
        chunksize=200_000,
    ):
        ser = chunk["sale_date"].astype(str).str.strip()
        for v in ser.unique().tolist():
            if not v or v == "nan":
                continue
            dates.add(v[:10])
    return dates


def _read_no_data_marker(marker_path: Path) -> set[str]:
    if not marker_path.exists() or marker_path.stat().st_size == 0:
        return set()
    out: set[str] = set()
    for line in marker_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = (line or "").strip()
        if not line:
            continue
        out.add(line.split("\t", 1)[0].strip())
    return out


@dataclass(frozen=True)
class PageSpec:
    key: str
    filename: str


PAGES = [
    PageSpec("today", "okpos_order.csv"),
    PageSpec("receipt", "okpos_order_item.csv"),
    PageSpec("daily", "okpos_daily.csv"),
]


def main() -> int:
    # Ensure repo root is on PYTHONPATH when run from anywhere.
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    parser = argparse.ArgumentParser()
    parser.add_argument("date_from", help="YYYY-MM-DD")
    parser.add_argument("date_to", help="YYYY-MM-DD")
    parser.add_argument("--brand", default="도리당", help="brand partition value (default: 도리당)")
    parser.add_argument(
        "--pages",
        default="today,receipt,daily",
        help="comma-separated pages to check: today,receipt,daily (default: today,receipt,daily)",
    )
    parser.add_argument(
        "--emit_airflow_confs",
        action="store_true",
        help="print suggested monthly dag_run.conf JSONs that would cover missing dates (does not run Airflow)",
    )
    args = parser.parse_args()

    d1 = _parse_date(args.date_from)
    d2 = _parse_date(args.date_to)
    if d1 > d2:
        raise SystemExit(f"date_from({args.date_from}) > date_to({args.date_to})")

    # Windows 로컬 실행 시에도 동일하게 OneDrive analytics 경로를 따라가도록 paths 사용
    from modules.transform.utility.paths import ONEDRIVE_DB, RAW_OKPOS_SALES
    from modules.transform.pipelines.db.DB_OKPOS_Sales import STORES

    brand_root = RAW_OKPOS_SALES / f"brand={args.brand}"
    want_dates_all = _iter_dates(d1, d2)

    expected_stores = [s["name"].replace("도리당 ", "", 1) for s in STORES]

    print(f"RAW_OKPOS_SALES={RAW_OKPOS_SALES}")
    print(f"brand_root={brand_root}")
    print(f"range={want_dates_all[0]}..{want_dates_all[-1]} ({len(want_dates_all)} days)")

    # Open date map (optional)
    open_csv = ONEDRIVE_DB / "sales_employee.csv"
    open_map: dict[str, date] = {}
    if open_csv.exists():
        try:
            emp = pd.read_csv(open_csv, encoding="utf-8-sig", dtype=str)
        except Exception:
            emp = pd.read_csv(open_csv, encoding="cp949", dtype=str)
        if "매장명" in emp.columns and "실오픈일" in emp.columns:
            emp["매장명"] = emp["매장명"].astype(str).str.strip()
            emp["_open"] = pd.to_datetime(emp["실오픈일"], errors="coerce").dt.date
            emp = emp.dropna(subset=["_open"])
            for store_name, g in emp.groupby("매장명", dropna=True):
                od = g["_open"].min()
                if pd.isna(od):
                    continue
                open_map[str(store_name).strip()] = od

    want_pages = {p.strip() for p in str(args.pages or "").split(",") if p.strip()}
    valid_pages = {p.key for p in PAGES}
    unknown_pages = sorted(want_pages - valid_pages)
    if unknown_pages:
        raise SystemExit(f"unknown --pages values: {unknown_pages} (valid={sorted(valid_pages)})")

    pages = [p for p in PAGES if p.key in want_pages]

    # Union missing dates across all stores/pages (for suggested backfill runs)
    missing_union_by_page: dict[str, set[str]] = {p.key: set() for p in pages}

    for store_short in expected_stores:
        store_dir = brand_root / f"store={store_short}"
        full_name = f"도리당 {store_short}"
        od = open_map.get(full_name)
        if od is None:
            # whitespace-tolerant fallback
            compact = full_name.replace(" ", "")
            for k, v in open_map.items():
                if k.replace(" ", "") == compact:
                    od = v
                    break

        print(f"\n# store={store_short} open_date={od}")

        # Ignore pre-open range (no need to collect)
        effective_from = max(d1, od) if od else d1
        if effective_from > d2:
            print("  - not_open_in_range: skip")
            continue
        want_dates = _iter_dates(effective_from, d2)
        want_set = set(want_dates)

        if not store_dir.exists():
            print("  - store dir missing")
            continue

        # 월별 캐시: (ym, page.key) -> sale_date_set
        cache: dict[tuple[str, str], set[str]] = {}

        for page in pages:
            have_data_dates: set[str] = set()
            no_data_dates: set[str] = set()
            for d in want_dates:
                ym = _ym(d)
                k = (ym, page.key)
                if k not in cache:
                    csv_path = store_dir / f"ym={ym}" / page.filename
                    cache[k] = _read_sale_date_set(csv_path)
                if d in cache[k]:
                    have_data_dates.add(d)

                marker_path = store_dir / f"ym={ym}" / f".no_data__{Path(page.filename).stem}.txt"
                mk = (ym, f"marker::{page.key}")
                if mk not in cache:
                    cache[mk] = _read_no_data_marker(marker_path)
                if d in cache[mk]:
                    no_data_dates.add(d)

            present = have_data_dates | no_data_dates
            missing = sorted(want_set - present)
            print(f"  - {page.key}: have_data={len(have_data_dates)} no_data={len(no_data_dates)} missing={len(missing)}")
            if missing:
                print("    missing_sample:", ", ".join(missing[:15]))
                missing_union_by_page[page.key].update(missing)

    if args.emit_airflow_confs and missing_union_by_page:
        # Suggested strategy: run monthly backfills for months that contain any missing date.
        months: set[str] = set()
        for miss in missing_union_by_page.values():
            for d in miss:
                months.add(_ym(d))
        months = set(sorted(months))
        if months:
            print("\n# Suggested monthly dag_run.conf payloads (run DB_OKPOS_Sales_Dags with these)")
            for ym in sorted(months):
                y, m = ym.split("-", 1)
                # Bound to the requested range.
                first = date(int(y), int(m), 1)
                # last day of month
                next_month = date(int(y) + (1 if int(m) == 12 else 0), 1 if int(m) == 12 else int(m) + 1, 1)
                last = next_month - timedelta(days=1)
                run_from = max(d1, first).isoformat()
                run_to = min(d2, last).isoformat()
                payload = {"sale_date_from": run_from, "sale_date_to": run_to}
                print(json.dumps(payload, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
