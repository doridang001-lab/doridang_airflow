"""쿠팡수동 unified_sales order_type 포장 값을 배달_포장으로 소급 교정."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.transform.pipelines.db.DB_UnifiedSales_common import (  # noqa: E402
    UNIFIED_COLUMNS,
    UNIFIED_ROOT,
    iter_unified_sales_files,
)

logger = logging.getLogger(__name__)

COUPANG_SOURCE = "쿠팡수동"
OLD_ORDER_TYPE = "포장"
NEW_ORDER_TYPE = "배달_포장"


def _target_mask(df: pd.DataFrame) -> pd.Series:
    source = df.get("source", "").fillna("").astype(str).str.strip()
    order_type = df.get("order_type", "").fillna("").astype(str).str.strip()
    return source.eq(COUPANG_SOURCE) & order_type.eq(OLD_ORDER_TYPE)


def main() -> dict[str, int]:
    files_changed = 0
    rows_fixed = 0

    files = iter_unified_sales_files()
    if not files:
        logger.info("unified_sales parquet 없음, 스킵 | %s", UNIFIED_ROOT)
        return {"files_changed": 0, "rows_fixed": 0}

    for path in files:
        df = pd.read_parquet(path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        mask = _target_mask(df)
        count = int(mask.sum())
        if count == 0:
            continue

        df.loc[mask, "order_type"] = NEW_ORDER_TYPE
        df.to_parquet(path, index=False, engine="pyarrow")
        files_changed += 1
        rows_fixed += count
        logger.info("쿠팡수동 order_type 교정: file=%s rows=%d", path.name, count)

    summary = {"files_changed": files_changed, "rows_fixed": rows_fixed}
    logger.info("쿠팡수동 order_type 교정 완료: %s", summary)
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="unified_sales parquet의 쿠팡수동 order_type=포장 값을 배달_포장으로 교정합니다."
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    _parse_args()
    main()
