"""fin_product_posfeed_whitelist.csv 분류를 fin_product_grp.csv posfeed 행으로 이관."""

import logging
from datetime import datetime

import pandas as pd

from modules.transform.pipelines.db.DB_FinProduct import _safe_replace
from modules.transform.pipelines.db.DB_UnifiedSales_common import _normalize_item_key
from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, POSFEED_WHITELIST_CSV_PATH

logger = logging.getLogger(__name__)


def _read_csv(path):
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")


def main() -> dict[str, int]:
    """whitelist 확정값을 grp posfeed exclude_check로 복원한다."""
    if not FIN_PRODUCT_CSV_PATH.exists():
        raise FileNotFoundError(f"fin_product_grp.csv 없음: {FIN_PRODUCT_CSV_PATH}")
    if not POSFEED_WHITELIST_CSV_PATH.exists():
        raise FileNotFoundError(f"posfeed whitelist CSV 없음: {POSFEED_WHITELIST_CSV_PATH}")

    whitelist = _read_csv(POSFEED_WHITELIST_CSV_PATH)
    grp = _read_csv(FIN_PRODUCT_CSV_PATH)

    if not {"item_name", "is_valid"}.issubset(whitelist.columns):
        raise ValueError("whitelist 필수 컬럼 없음: item_name, is_valid")
    required_grp_cols = {"source", "상품명", "상품코드", "exclude_check", "updated_at"}
    missing_grp_cols = required_grp_cols - set(grp.columns)
    if missing_grp_cols:
        raise ValueError(f"fin_product_grp.csv 필수 컬럼 없음: {sorted(missing_grp_cols)}")

    whitelist_map: dict[str, str] = {}
    for _, row in whitelist.iterrows():
        key = _normalize_item_key(str(row["item_name"]).strip())
        is_valid = str(row["is_valid"]).strip().upper()
        if key and is_valid in {"Y", "N"}:
            whitelist_map[key] = is_valid

    posfeed_mask = grp["source"].fillna("").astype(str).str.strip().str.lower() == "posfeed"
    posfeed_count = int(posfeed_mask.sum())
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    matched_count = 0
    unmatched_count = 0
    overwritten_count = 0
    blank_code_count = 0

    for idx in grp.index[posfeed_mask]:
        key = _normalize_item_key(str(grp.at[idx, "상품명"]).strip())
        is_valid = whitelist_map.get(key)
        if is_valid in {"Y", "N"}:
            next_exclude = "Y" if is_valid == "N" else "N"
            if str(grp.at[idx, "exclude_check"]).strip().upper() != next_exclude:
                overwritten_count += 1
            grp.at[idx, "exclude_check"] = next_exclude
            matched_count += 1
        else:
            if str(grp.at[idx, "exclude_check"]).strip().upper() != "N":
                overwritten_count += 1
            grp.at[idx, "exclude_check"] = "N"
            unmatched_count += 1

        if str(grp.at[idx, "상품코드"]).strip():
            blank_code_count += 1
        grp.at[idx, "상품코드"] = ""
        grp.at[idx, "updated_at"] = now

    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        grp.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    final_posfeed = grp[posfeed_mask]
    exclude_counts = (
        final_posfeed["exclude_check"].fillna("").astype(str).str.strip().str.upper().value_counts().to_dict()
    )
    logger.info(
        "posfeed grp 마이그레이션 완료: posfeed=%d, whitelist_match=%d, whitelist_unmatched=%d, overwritten=%d, blank_code=%d, exclude=%s",
        posfeed_count,
        matched_count,
        unmatched_count,
        overwritten_count,
        blank_code_count,
        exclude_counts,
    )
    return {
        "posfeed_rows": posfeed_count,
        "matched_rows": matched_count,
        "unmatched_rows": unmatched_count,
        "overwritten_rows": overwritten_count,
        "blank_code_rows": blank_code_count,
        "exclude_y": int(exclude_counts.get("Y", 0)),
        "exclude_n": int(exclude_counts.get("N", 0)),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    main()
