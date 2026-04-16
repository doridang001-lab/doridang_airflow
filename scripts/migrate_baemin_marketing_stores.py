"""
배민 마케팅 store 폴더 일회성 마이그레이션 스크립트

변환:
  store=[음식배달]_닭도리탕_전문_도리당_강동점/ym=.../data.csv
  → store=강동점/ym=.../baemin_marketing_data.csv

실행:
  cd C:/airflow
  python scripts/migrate_baemin_marketing_stores.py

  # 실제 이동 없이 미리 보기만:
  python scripts/migrate_baemin_marketing_stores.py --dry-run
"""

import argparse
import re
import shutil
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# 경로 설정
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from modules.transform.utility.paths import BAEMIN_MARKETING_DB


# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------

def _extract_branch(store_name: str) -> str:
    name = re.sub(r"\[.*?\]\s*", "", str(store_name)).strip()
    tokens = name.split()
    return tokens[-1] if tokens else name


def _branch_from_folder(folder_value: str) -> str:
    """'[음식배달]_닭도리탕_전문_도리당_강동점' → '강동점'"""
    return _extract_branch(folder_value.replace("_", " "))


def _is_legacy(folder_value: str) -> bool:
    """폴더명이 이미 정규화된 지점명(단일 토큰)이면 False."""
    normalized = _branch_from_folder(folder_value)
    return folder_value != normalized


def _merge_and_save(src_csv: Path, dst_csv: Path, dry_run: bool) -> int:
    """src_csv 를 dst_csv 에 merge(중복제거 후 append). 저장 행 수 반환."""
    src_df = pd.read_csv(src_csv, encoding="utf-8-sig")

    if dst_csv.exists():
        dst_df = pd.read_csv(dst_csv, encoding="utf-8-sig")
        combined = pd.concat([dst_df, src_df], ignore_index=True)
    else:
        combined = src_df

    dedup_cols = [c for c in ["store_id", "날짜"] if c in combined.columns]
    if dedup_cols and "collected_at" in combined.columns:
        combined["collected_at"] = pd.to_datetime(combined["collected_at"], errors="coerce")
        combined = combined.sort_values("collected_at")
        combined = combined.drop_duplicates(subset=dedup_cols, keep="last")

    if not dry_run:
        dst_csv.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(dst_csv, index=False, encoding="utf-8-sig")

    return len(combined)


# ---------------------------------------------------------------------------
# 마이그레이션
# ---------------------------------------------------------------------------

def migrate(dry_run: bool = False):
    base = BAEMIN_MARKETING_DB
    if not base.is_dir():
        print(f"[오류] 경로 없음: {base}")
        sys.exit(1)

    total_moved = 0
    total_skipped = 0

    for brand_dir in sorted(base.iterdir()):
        if not brand_dir.is_dir() or not brand_dir.name.startswith("brand="):
            continue
        brand = brand_dir.name[len("brand="):]

        for store_dir in sorted(brand_dir.iterdir()):
            if not store_dir.is_dir() or not store_dir.name.startswith("store="):
                continue
            folder_value = store_dir.name[len("store="):]

            if not _is_legacy(folder_value):
                continue  # 이미 정규화된 폴더

            new_store = _branch_from_folder(folder_value)
            new_store_dir = brand_dir / f"store={new_store}"

            print(f"\n[변환 대상] brand={brand}")
            print(f"  구형: store={folder_value}")
            print(f"  신형: store={new_store}")

            for ym_dir in sorted(store_dir.iterdir()):
                if not ym_dir.is_dir() or not ym_dir.name.startswith("ym="):
                    continue
                ym = ym_dir.name

                new_ym_dir = new_store_dir / ym

                for csv_file in sorted(ym_dir.glob("*.csv")):
                    # 파일명 정규화: data.csv → baemin_marketing_data.csv
                    new_filename = (
                        "baemin_marketing_data.csv"
                        if csv_file.name == "data.csv"
                        else csv_file.name
                    )
                    dst_csv = new_ym_dir / new_filename

                    print(f"  {ym}/{csv_file.name} → {ym}/{new_filename}  ", end="")

                    if dry_run:
                        print("[DRY-RUN]")
                    else:
                        rows = _merge_and_save(csv_file, dst_csv, dry_run=False)
                        print(f"[완료 {rows:,}행]")
                        total_moved += 1

            # 구형 store 디렉터리 삭제 (dry-run이면 생략)
            if not dry_run:
                shutil.rmtree(store_dir, ignore_errors=True)
                print(f"  [삭제] {store_dir}")
            else:
                total_skipped += 1

    if dry_run:
        print(f"\n[DRY-RUN 완료] 변환 대상 store 폴더: {total_skipped}개 (실제 변경 없음)")
    else:
        print(f"\n[완료] 파일 이동: {total_moved}건")


# ---------------------------------------------------------------------------
# 진입점
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="배민 마케팅 store 폴더 마이그레이션")
    parser.add_argument("--dry-run", action="store_true", help="실제 변경 없이 미리 보기")
    args = parser.parse_args()

    migrate(dry_run=args.dry_run)
