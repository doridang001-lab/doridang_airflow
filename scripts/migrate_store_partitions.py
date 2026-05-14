"""
migrate_store_partitions.py — store= 파티션 폴더명 정규화 마이그레이션

대상 채널:
  - analytics/posfeed_sales
  - analytics/posfeed_sales_detail
  - analytics/baemin_marketing

처리 방식:
  1. STORE_NAME_MAP 기준으로 구버전 폴더명 탐지
  2. 신규 폴더로 CSV 이동 + 내부 store 컬럼 값도 함께 수정
  3. 목적지에 같은 ym 이미 존재하면 concat + dedup(_pk 기준)
  4. 구버전 폴더 삭제

사용법:
  python scripts/migrate_store_partitions.py --dry-run   # 확인만
  python scripts/migrate_store_partitions.py             # 실제 실행
"""

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.store_normalize import normalize, strip_brand


# ──────────────────────────────────────────────
# 채널별 설정
# ──────────────────────────────────────────────
def _new_store_for_posfeed(old_store: str) -> str:
    """posfeed: store 폴더값이 '브랜드 지점명' 형태 → 정규화된 공식명"""
    return normalize(pd.Series([old_store])).iloc[0]


def _new_store_for_baemin(old_store: str, brand: str) -> str:
    """baemin_marketing: store 폴더값이 지점명만 있는 형태 → 정규화 후 브랜드 접두어 제거"""
    full = f"{brand} {old_store}"
    normalized_full = normalize(pd.Series([full])).iloc[0]
    return strip_brand(pd.Series([normalized_full])).iloc[0]


DATASETS = [
    {
        "name": "posfeed_sales",
        "root": ANALYTICS_DB / "posfeed_sales",
        "filename": "posfeed_orders.csv",
        "pk_col": "_pk",
        "new_store_fn": lambda old, brand: _new_store_for_posfeed(old),
        "csv_store_col": "store",  # CSV 내부에서 바꿀 컬럼
    },
    {
        "name": "posfeed_sales_detail",
        "root": ANALYTICS_DB / "posfeed_sales_detail",
        "filename": "posfeed_order_item.csv",
        "pk_col": "_pk",
        "new_store_fn": lambda old, brand: _new_store_for_posfeed(old),
        "csv_store_col": "store",
    },
    {
        "name": "baemin_marketing",
        "root": ANALYTICS_DB / "baemin_marketing",
        "filename": "baemin_marketing_data.csv",
        "pk_col": "store_id",
        "new_store_fn": _new_store_for_baemin,
        "csv_store_col": "store",
    },
]


# ──────────────────────────────────────────────
# 핵심 로직
# ──────────────────────────────────────────────
def _merge_and_save(src_csv: Path, dst_csv: Path, pk_col: str, csv_store_col: str,
                    new_store_val: str, dry_run: bool) -> int:
    """src → dst 머지 (중복제거). 실행 건수 반환."""
    src_df = pd.read_csv(src_csv, dtype=str, encoding="utf-8-sig")

    # store 컬럼 값 업데이트
    if csv_store_col in src_df.columns:
        src_df[csv_store_col] = new_store_val

    if dst_csv.exists():
        dst_df = pd.read_csv(dst_csv, dtype=str, encoding="utf-8-sig")
        combined = pd.concat([dst_df, src_df], ignore_index=True)
    else:
        combined = src_df

    # 중복 제거
    if pk_col in combined.columns:
        before = len(combined)
        combined = combined.drop_duplicates(subset=[pk_col], keep="last")
        dedup_count = before - len(combined)
        if dedup_count > 0:
            print(f"      dedup {dedup_count}건 제거")
    else:
        # pk 컬럼 없으면 전체 행 기준
        combined = combined.drop_duplicates()

    if not dry_run:
        dst_csv.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(dst_csv, index=False, encoding="utf-8-sig")

    return len(combined)


def migrate_dataset(cfg: dict, dry_run: bool) -> tuple[int, int]:
    """한 채널의 구버전 폴더 전체를 마이그레이션. (파일수, 폴더수) 반환."""
    root: Path = cfg["root"]
    filename: str = cfg["filename"]
    pk_col: str = cfg["pk_col"]
    csv_store_col: str = cfg["csv_store_col"]
    new_store_fn = cfg["new_store_fn"]

    file_count = 0
    folder_count = 0

    for store_dir in sorted(root.glob("brand=*/store=*")):
        brand = store_dir.parent.name.replace("brand=", "")
        old_store = store_dir.name.replace("store=", "")
        new_store = new_store_fn(old_store, brand)

        if old_store == new_store:
            continue  # 이미 정규화된 폴더

        new_store_dir = store_dir.parent / f"store={new_store}"

        print(f"  [{brand}] {old_store!r:40s} → {new_store!r}")

        # ym 단위로 처리
        for ym_dir in sorted(store_dir.glob("ym=*")):
            ym = ym_dir.name
            src_csv = ym_dir / filename
            if not src_csv.exists():
                print(f"    {ym} — CSV 없음, 건너뜀")
                continue

            dst_csv = new_store_dir / ym / filename
            dst_exists = dst_csv.exists()
            print(f"    {ym} — {'머지' if dst_exists else '이동'} → {dst_csv.relative_to(root)}")

            rows = _merge_and_save(src_csv, dst_csv, pk_col, csv_store_col, new_store, dry_run)
            print(f"      저장 행수: {rows}")
            file_count += 1

        # 구버전 폴더 삭제 (dry-run 아닐 때)
        if not dry_run:
            shutil.rmtree(store_dir)
            print(f"    구버전 폴더 삭제: {store_dir.name}")
        else:
            print(f"    [dry-run] 구버전 폴더 삭제 예정: {store_dir.name}")

        folder_count += 1

    return file_count, folder_count


# ──────────────────────────────────────────────
# 엔트리포인트
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="실제 변경 없이 계획만 출력")
    parser.add_argument(
        "--dataset",
        choices=["posfeed_sales", "posfeed_sales_detail", "baemin_marketing", "all"],
        default="all",
    )
    args = parser.parse_args()

    if args.dry_run:
        print("=" * 60)
        print("DRY-RUN 모드 — 실제 파일은 변경되지 않습니다")
        print("=" * 60)

    total_files = 0
    total_folders = 0

    for cfg in DATASETS:
        if args.dataset != "all" and cfg["name"] != args.dataset:
            continue
        print(f"\n▶ {cfg['name']}")
        files, folders = migrate_dataset(cfg, dry_run=args.dry_run)
        total_files += files
        total_folders += folders
        if folders == 0:
            print("  (변경 대상 없음)")

    print()
    if args.dry_run:
        print(f"[dry-run 완료] 변환 예정 폴더 {total_folders}개 / 파일 {total_files}개")
    else:
        print(f"[완료] 폴더 {total_folders}개 마이그레이션, 파일 {total_files}개 처리")


if __name__ == "__main__":
    main()
