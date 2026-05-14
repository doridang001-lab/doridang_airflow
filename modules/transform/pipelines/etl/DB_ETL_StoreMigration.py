"""
ETL_StoreMigration.py — store= 파티션 폴더명 정규화 마이그레이션

STORE_NAME_MAP 기준으로 구버전 store= 폴더명을 감지하고
공식명 폴더로 CSV를 머지/이동한 뒤 구버전 폴더를 삭제한다.

대상 채널:
  - analytics/posfeed_sales         (posfeed_orders.csv,     pk=_pk)
  - analytics/posfeed_sales_detail  (posfeed_order_item.csv, pk=_pk)
  - analytics/baemin_marketing      (baemin_marketing_data.csv, pk=store_id)
"""

import logging
import shutil
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.store_normalize import normalize, strip_brand

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────
# 채널 설정
# ──────────────────────────────────────────────────────────
def _new_store_posfeed(old_store: str, brand: str) -> str:
    return normalize(pd.Series([old_store])).iloc[0]


def _new_store_baemin(old_store: str, brand: str) -> str:
    full = f"{brand} {old_store}"
    normalized = normalize(pd.Series([full])).iloc[0]
    return strip_brand(pd.Series([normalized])).iloc[0]


DATASETS = [
    {
        "name": "posfeed_sales",
        "root": ANALYTICS_DB / "posfeed_sales",
        "filename": "posfeed_orders.csv",
        "pk_col": "_pk",
        "new_store_fn": _new_store_posfeed,
        "csv_store_col": "store",
    },
    {
        "name": "posfeed_sales_detail",
        "root": ANALYTICS_DB / "posfeed_sales_detail",
        "filename": "posfeed_order_item.csv",
        "pk_col": "_pk",
        "new_store_fn": _new_store_posfeed,
        "csv_store_col": "store",
    },
    {
        "name": "baemin_marketing",
        "root": ANALYTICS_DB / "baemin_marketing",
        "filename": "baemin_marketing_data.csv",
        "pk_col": "store_id",
        "new_store_fn": _new_store_baemin,
        "csv_store_col": "store",
    },
]


# ──────────────────────────────────────────────────────────
# 내부 유틸
# ──────────────────────────────────────────────────────────
def _merge_and_save(
    src_csv: Path,
    dst_csv: Path,
    pk_col: str,
    csv_store_col: str,
    new_store_val: str,
    dry_run: bool,
) -> dict:
    src_df = pd.read_csv(src_csv, dtype=str, encoding="utf-8-sig")

    if csv_store_col in src_df.columns:
        src_df[csv_store_col] = new_store_val

    if dst_csv.exists():
        dst_df = pd.read_csv(dst_csv, dtype=str, encoding="utf-8-sig")
        combined = pd.concat([dst_df, src_df], ignore_index=True)
        action = "merge"
    else:
        combined = src_df
        action = "move"

    before = len(combined)
    if pk_col in combined.columns:
        combined = combined.drop_duplicates(subset=[pk_col], keep="last")
    else:
        combined = combined.drop_duplicates()
    dedup = before - len(combined)

    if not dry_run:
        dst_csv.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(dst_csv, index=False, encoding="utf-8-sig")

    return {"action": action, "rows": len(combined), "dedup": dedup}


# ──────────────────────────────────────────────────────────
# 공개 함수 (DAG task로 호출)
# ──────────────────────────────────────────────────────────
def scan_legacy_partitions(**context) -> str:
    """구버전 파티션 폴더를 스캔해 XCom으로 목록 push."""
    legacy = []
    for cfg in DATASETS:
        root: Path = cfg["root"]
        new_store_fn = cfg["new_store_fn"]
        for store_dir in sorted(root.glob("brand=*/store=*")):
            brand = store_dir.parent.name.replace("brand=", "")
            old_store = store_dir.name.replace("store=", "")
            new_store = new_store_fn(old_store, brand)
            if old_store == new_store:
                continue
            for ym_dir in sorted(store_dir.glob("ym=*")):
                csv = ym_dir / cfg["filename"]
                if csv.exists():
                    legacy.append({
                        "dataset": cfg["name"],
                        "brand": brand,
                        "old_store": old_store,
                        "new_store": new_store,
                    })
                    break  # 대표 1건만 확인

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="legacy_count", value=len(legacy))

    if not legacy:
        logger.info("구버전 파티션 없음 — 마이그레이션 불필요")
        return "구버전 파티션 없음"

    summary = "\n".join(
        f"  [{r['dataset']}] {r['brand']} | {r['old_store']!r} → {r['new_store']!r}"
        for r in legacy
    )
    logger.info("구버전 파티션 %d개 발견:\n%s", len(legacy), summary)
    return f"구버전 파티션 {len(legacy)}개 발견"


def migrate_partitions(dry_run: bool = False, **context) -> str:
    """구버전 파티션을 공식명 폴더로 머지/이동하고 구버전 폴더를 삭제한다."""
    total_files = 0
    total_folders = 0
    total_dedup = 0

    for cfg in DATASETS:
        root: Path = cfg["root"]
        filename: str = cfg["filename"]
        pk_col: str = cfg["pk_col"]
        csv_store_col: str = cfg["csv_store_col"]
        new_store_fn = cfg["new_store_fn"]

        for store_dir in sorted(root.glob("brand=*/store=*")):
            brand = store_dir.parent.name.replace("brand=", "")
            old_store = store_dir.name.replace("store=", "")
            new_store = new_store_fn(old_store, brand)

            if old_store == new_store:
                continue

            new_store_dir = store_dir.parent / f"store={new_store}"
            logger.info("[%s] %r → %r", cfg["name"], old_store, new_store)

            folder_had_files = False
            for ym_dir in sorted(store_dir.glob("ym=*")):
                src_csv = ym_dir / filename
                if not src_csv.exists():
                    continue
                dst_csv = new_store_dir / ym_dir.name / filename
                result = _merge_and_save(
                    src_csv, dst_csv, pk_col, csv_store_col, new_store, dry_run
                )
                logger.info(
                    "  %s → %s | action=%s rows=%d dedup=%d",
                    ym_dir.name, dst_csv.parent.relative_to(root),
                    result["action"], result["rows"], result["dedup"],
                )
                total_files += 1
                total_dedup += result["dedup"]
                folder_had_files = True

            if folder_had_files:
                if not dry_run:
                    shutil.rmtree(store_dir)
                    logger.info("  구버전 폴더 삭제: %s", store_dir.name)
                else:
                    logger.info("  [dry-run] 삭제 예정: %s", store_dir.name)
                total_folders += 1

    msg = (
        f"{'[dry-run] ' if dry_run else ''}"
        f"폴더 {total_folders}개 마이그레이션 / 파일 {total_files}개 처리 / dedup {total_dedup}건 제거"
    )
    logger.info(msg)
    return msg
