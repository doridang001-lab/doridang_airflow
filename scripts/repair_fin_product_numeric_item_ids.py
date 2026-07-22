"""Repair fin_product_grp.csv after numeric manual item_id migration.

Default mode is dry-run. Pass --apply to write fin_product_grp.csv.
This script uses backup/legacy files as classification evidence only; it never
restores legacy hash item_id values.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.transform.pipelines.db.DB_ItemIdAllocator import (  # noqa: E402
    allocate_manual_item_ids,
    normalize_item_key,
    validate_fin_product_codes,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import _normalize_item_key  # noqa: E402
from modules.transform.utility.paths import (  # noqa: E402
    FIN_PRODUCT_CSV_PATH,
    POSFEED_WHITELIST_CSV_PATH,
)


MANUAL_SOURCES = {"posfeed", "배민수동", "쿠팡수동"}


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")


def _backup(path: Path) -> Path:
    backup = path.with_name(f"{path.stem}.repair_backup_{datetime.now():%Y%m%d_%H%M%S}{path.suffix}")
    shutil.copy2(path, backup)
    return backup


def _find_legacy_posfeed_backup() -> Path | None:
    base = FIN_PRODUCT_CSV_PATH.parent
    preferred = base / "fin_product_grp.blank_code_posfeed_legacy_20260630_103128.csv"
    if preferred.exists():
        return preferred
    candidates = sorted(base.glob("fin_product_grp.blank_code_posfeed_legacy_*.csv"))
    return candidates[-1] if candidates else None


def _legacy_exclude_keys() -> set[str]:
    keys: set[str] = set()
    if POSFEED_WHITELIST_CSV_PATH.exists():
        wl = _read_csv(POSFEED_WHITELIST_CSV_PATH)
        if {"item_name", "is_valid"}.issubset(wl.columns):
            invalid = wl["is_valid"].astype(str).str.strip().str.upper().eq("N")
            keys.update(
                _normalize_item_key(v)
                for v in wl.loc[invalid, "item_name"].astype(str)
                if _normalize_item_key(v)
            )

    legacy_path = _find_legacy_posfeed_backup()
    if legacy_path and legacy_path.exists():
        legacy = _read_csv(legacy_path)
        if {"상품명", "exclude_check"}.issubset(legacy.columns):
            excluded = legacy["exclude_check"].astype(str).str.strip().str.upper().eq("Y")
            keys.update(
                _normalize_item_key(v)
                for v in legacy.loc[excluded, "상품명"].astype(str)
                if _normalize_item_key(v)
            )
    return keys


def _source_series(df: pd.DataFrame) -> pd.Series:
    if "source" not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return df["source"].fillna("").astype(str).str.strip()


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in [
        "source",
        "brand",
        "store",
        "item_key",
        "상품코드",
        "상품명",
        "판매단가",
        "llm_check",
        "exclude_check",
        "is_latest",
        "updated_at",
    ]:
        if col not in out.columns:
            out[col] = ""
    return out


def _repair_blank_manual_codes(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    out = _ensure_columns(df)
    source = _source_series(out)
    blank_code = out["상품코드"].astype(str).str.strip().eq("")
    target = source.isin(MANUAL_SOURCES) & blank_code
    if not target.any():
        return out, 0

    rows = pd.DataFrame(
        {
            "source": source.loc[target].values,
            "brand": out.loc[target, "brand"].astype(str).str.strip().values,
            "store": out.loc[target, "store"].astype(str).str.strip().values,
            "item_name": out.loc[target, "상품명"].astype(str).str.strip().values,
            "unit_price": out.loc[target, "판매단가"].astype(str).str.strip().values,
        },
        index=out.loc[target].index,
    )
    usable = (
        rows["source"].astype(str).str.strip().ne("")
        & rows["store"].astype(str).str.strip().ne("")
        & rows["item_name"].astype(str).str.strip().ne("")
    )
    if not usable.any():
        return out, 0

    codes = allocate_manual_item_ids(rows.loc[usable], persist=False)
    update_idx = rows.loc[usable].index
    out.loc[update_idx, "상품코드"] = codes.astype(str).values
    out.loc[update_idx, "item_key"] = rows.loc[usable, "item_name"].map(normalize_item_key).values
    out.loc[update_idx, "llm_check"] = "N"
    out.loc[update_idx, "is_latest"] = out.loc[update_idx, "is_latest"].replace("", "Y")
    return out, len(update_idx)


def _drop_orphan_blank_manual_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    out = _ensure_columns(df)
    source = _source_series(out)
    blank_code = out["상품코드"].astype(str).str.strip().eq("")
    no_identity = (
        out["brand"].astype(str).str.strip().eq("")
        & out["store"].astype(str).str.strip().eq("")
        & out["item_key"].astype(str).str.strip().eq("")
    )
    not_latest = out["is_latest"].astype(str).str.strip().str.upper().ne("Y")
    target = source.isin(MANUAL_SOURCES) & blank_code & no_identity & not_latest
    if not target.any():
        return out, 0
    return out.loc[~target].reset_index(drop=True), int(target.sum())


def repair(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    out = _ensure_columns(df)
    stats: dict[str, int] = {}

    out, stats["blank_manual_codes_repaired"] = _repair_blank_manual_codes(out)
    out, stats["orphan_blank_manual_rows_dropped"] = _drop_orphan_blank_manual_rows(out)

    source = _source_series(out)
    manual = source.isin(MANUAL_SOURCES)
    llm_y = out["llm_check"].astype(str).str.strip().str.upper().eq("Y")
    stats["manual_llm_y_cleared"] = int((manual & llm_y).sum())
    out.loc[manual & llm_y, "llm_check"] = "N"

    posfeed = source.str.lower().eq("posfeed")
    item_key = out["item_key"].astype(str).str.strip()
    fallback_key = out["상품명"].astype(str).map(_normalize_item_key)
    keys = item_key.where(item_key.ne(""), fallback_key)
    exclude_keys = _legacy_exclude_keys()
    should_exclude = posfeed & keys.isin(exclude_keys)
    currently_excluded = out["exclude_check"].astype(str).str.strip().str.upper().eq("Y")
    stats["posfeed_exclude_marked"] = int((should_exclude & ~currently_excluded).sum())
    out.loc[should_exclude, "exclude_check"] = "Y"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    changed = should_exclude | (manual & llm_y)
    out.loc[changed, "updated_at"] = out.loc[changed, "updated_at"].replace("", now)

    return out, stats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="실제 fin_product_grp.csv를 수정합니다.")
    parser.add_argument(
        "--apply-legacy-excludes",
        action="store_true",
        help="구 whitelist/백업 exclude_check=Y를 현재 posfeed exclude_check에 반영합니다. 기본값은 반영하지 않습니다.",
    )
    args = parser.parse_args()

    df = _read_csv(FIN_PRODUCT_CSV_PATH)
    repaired, stats = repair(df)
    if not args.apply_legacy_excludes:
        stats["posfeed_exclude_marked"] = 0
        source = repaired["source"].fillna("").astype(str).str.strip().str.lower()
        # repair()가 legacy exclude를 적용한 결과를 기본 실행에서는 되돌린다.
        original = df.reindex(repaired.index).fillna("") if len(df) == len(repaired) else repaired.copy()
        if "exclude_check" in original.columns:
            restored = original["exclude_check"].astype(str).str.strip().str.upper()
            mask = source.eq("posfeed")
            repaired.loc[mask, "exclude_check"] = restored.loc[mask].where(restored.loc[mask].isin(["Y", "N"]), "N")

    print(f"file={FIN_PRODUCT_CSV_PATH}")
    print(f"rows_before={len(df)} rows_after={len(repaired)}")
    for key, value in stats.items():
        print(f"{key}={value}")

    try:
        validate_fin_product_codes(repaired, allow_legacy_blank=False)
        print("validate=OK")
    except Exception as exc:
        print(f"validate=FAIL {type(exc).__name__}: {exc}")
        return 2

    if args.apply:
        backup = _backup(FIN_PRODUCT_CSV_PATH)
        repaired.to_csv(FIN_PRODUCT_CSV_PATH, index=False, encoding="utf-8-sig")
        print(f"applied=1 backup={backup}")
    else:
        print("applied=0 dry_run=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
