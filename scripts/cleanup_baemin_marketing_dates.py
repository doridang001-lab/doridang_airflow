"""
Baemin marketing 파티션 CSV 정리 스크립트.

목표
- 날짜 컬럼 값을 'YYYY-MM-DD 12:00:00 AM' 형태로 통일
- store_id + 날짜 기준으로 1건만 남김(collected_at 최신 유지)
- 파일 내 brand/store/ym 값도 파티션 경로 기준으로 통일

실행:
  cd C:/airflow
  python scripts/cleanup_baemin_marketing_dates.py
  python scripts/cleanup_baemin_marketing_dates.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.utility.paths import BAEMIN_MARKETING_DB  # noqa: E402
from modules.transform.pipelines.db.db_baemin_marketing import (  # noqa: E402
    BAEMIN_MARKETING_DATE_COL_CANDIDATES,
    _first_existing_column,
    _normalize_date_col_as_midnight_ampm,
)


def _parse_partition_keys(csv_path: Path) -> tuple[str, str, str]:
    # .../baemin_marketing/brand=X/store=Y/ym=Z/baemin_marketing_data.csv
    ym = csv_path.parent.name[len("ym=") :] if csv_path.parent.name.startswith("ym=") else "unknown"
    store_dir = csv_path.parent.parent
    store = store_dir.name[len("store=") :] if store_dir.name.startswith("store=") else "unknown"
    brand_dir = store_dir.parent
    brand = brand_dir.name[len("brand=") :] if brand_dir.name.startswith("brand=") else "unknown"
    return brand, store, ym


def _cleanup_one(csv_path: Path, dry_run: bool) -> tuple[int, int]:
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    before = len(df)

    date_col = _first_existing_column(df, BAEMIN_MARKETING_DATE_COL_CANDIDATES)
    if date_col and date_col != "날짜":
        df = df.rename(columns={date_col: "날짜"})

    if "날짜" in df.columns:
        _normalize_date_col_as_midnight_ampm(df, "날짜")

    brand, store, ym = _parse_partition_keys(csv_path)
    df["brand"] = brand
    df["store"] = store
    df["ym"] = ym

    dedup_cols = [c for c in ["store_id", "날짜"] if c in df.columns]
    if dedup_cols:
        if "collected_at" in df.columns:
            df["collected_at"] = pd.to_datetime(df["collected_at"], errors="coerce", format="mixed")
            df = df.sort_values("collected_at")
        df = df.drop_duplicates(subset=dedup_cols, keep="last")

    after = len(df)

    if not dry_run:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    return before, after


def main():
    parser = argparse.ArgumentParser(description="baemin_marketing 파티션 CSV 날짜/중복 정리")
    parser.add_argument("--dry-run", action="store_true", help="파일을 수정하지 않고 결과만 출력")
    args = parser.parse_args()

    base = BAEMIN_MARKETING_DB
    if not base.is_dir():
        print(f"[오류] 경로 없음: {base}")
        return 1

    csv_files = sorted(base.glob("brand=*/store=*/ym=*/baemin_marketing_data.csv"))
    if not csv_files:
        print(f"[정보] 대상 파일 없음: {base}")
        return 0

    total_before = 0
    total_after = 0
    changed_files = 0

    for csv_path in csv_files:
        try:
            before, after = _cleanup_one(csv_path, dry_run=args.dry_run)
            total_before += before
            total_after += after
            if before != after:
                changed_files += 1
            print(f"[정리] {csv_path} — {before:,} → {after:,}")
        except Exception as e:
            print(f"[실패] {csv_path}: {e}")

    print(f"\n[합계] 파일 {len(csv_files)}개, 행 {total_before:,} → {total_after:,} (변경 파일 {changed_files}개)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

