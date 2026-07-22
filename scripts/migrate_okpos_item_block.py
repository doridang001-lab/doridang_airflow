"""Migrate OKPOS scoped item_id values from 1000 to 10000 item blocks.

Default mode is dry-run. Pass --apply to rewrite CSV files after reviewing the
summary in scripts/output.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.transform.pipelines.db.DB_ItemIdAllocator import (  # noqa: E402
    SOURCE_ITEM_BASE,
    canonical_source,
    item_block_size,
    normalize_item_key,
    validate_fin_product_codes,
)
from modules.transform.utility.paths import (  # noqa: E402
    FIN_PRODUCT_CSV_PATH,
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_JOIN_CSV_PATH,
    FIN_PRODUCT_MAP_RECENTLY_CSV_PATH,
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
    FIN_PRODUCT_MAP_TRAIN_JSON_PATH,
    FIN_PRODUCT_MART_CSV_PATH,
    RAW_OKPOS_SALES,
)
from scripts._base import run_script  # noqa: E402


SOURCE = "okpos"
BASE = SOURCE_ITEM_BASE[SOURCE]
OLD_BLOCK_SIZE = 1000
NEW_BLOCK_SIZE = item_block_size(SOURCE)
CODE_COLUMNS = ("상품코드", "item_id")
REVIEW_COLUMNS = ("llm_check", "수동분류", "exclude_check", "launch_date")


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    tmp = path.with_suffix(".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        try:
            tmp.replace(path)
        except (PermissionError, OSError):
            path.write_bytes(tmp.read_bytes())
    finally:
        tmp.unlink(missing_ok=True)


def _backup(path: Path) -> Path:
    backup_dir = path.parent / "_backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / f"{path.stem}.okpos_block_bak_{_timestamp()}{path.suffix}"
    shutil.copy2(path, backup)
    return backup


def _source_series(df: pd.DataFrame) -> pd.Series:
    if "source" not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return df["source"].fillna("").astype(str).str.strip().map(canonical_source)


def _code_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index)
    return pd.to_numeric(df[column].fillna("").astype(str).str.strip(), errors="coerce")


def _okpos_code_mask(df: pd.DataFrame, column: str) -> pd.Series:
    return _source_series(df).eq(SOURCE) & _code_series(df, column).ge(BASE) & _code_series(df, column).lt(BASE + 10_000_000)


def _store_from_path(path: Path) -> str:
    for part in path.parts:
        if part.startswith("store="):
            return part.split("=", 1)[1].strip()
    return ""


def _first_existing(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _load_raw_offsets() -> pd.DataFrame:
    brand_root = RAW_OKPOS_SALES / "brand=도리당"
    rows: list[dict[str, str]] = []
    if not brand_root.exists():
        return pd.DataFrame(columns=["store", "item_key", "raw_code", "raw_offset"])

    code_candidates = ("상품코드", "item_id", "상품번호", "상품ID")
    name_candidates = ("상품명", "item_name", "메뉴명", "상품")
    for path in sorted(brand_root.glob("store=*/ym=*/okpos_order_item.csv")):
        try:
            df = _read_csv(path)
        except Exception:
            continue
        code_col = _first_existing(list(df.columns), code_candidates)
        name_col = _first_existing(list(df.columns), name_candidates)
        if not code_col or not name_col:
            continue
        store = _store_from_path(path)
        work = df[[code_col, name_col]].copy()
        work[code_col] = work[code_col].fillna("").astype(str).str.strip()
        work[name_col] = work[name_col].fillna("").astype(str).str.strip()
        work = work[(work[code_col] != "") & (work[name_col] != "")]
        for _, row in work.drop_duplicates().iterrows():
            code_text = str(row[code_col]).strip()
            try:
                raw_code = int(code_text)
            except ValueError:
                continue
            if raw_code < BASE:
                continue
            rows.append({
                "store": store,
                "item_key": normalize_item_key(row[name_col]),
                "raw_code": str(raw_code),
                "raw_offset": str(raw_code - BASE),
            })
    if not rows:
        return pd.DataFrame(columns=["store", "item_key", "raw_code", "raw_offset"])
    return pd.DataFrame(rows).drop_duplicates()


def _build_mapping(master: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    raw = _load_raw_offsets()
    okpos = master[_source_series(master).eq(SOURCE)].copy()
    if okpos.empty:
        return {"code": {}, "identity": []}, {"okpos_master_rows": 0, "raw_identity_rows": len(raw), "mapped_rows": 0}

    for col in ("store", "상품명", "item_key", "store_seq", "상품코드"):
        if col not in okpos.columns:
            okpos[col] = ""
        okpos[col] = okpos[col].fillna("").astype(str).str.strip()
    okpos["item_key"] = okpos["item_key"].where(okpos["item_key"] != "", okpos["상품명"].map(normalize_item_key))
    okpos["_old_code"] = okpos["상품코드"]

    merged = okpos.merge(raw, how="left", on=["store", "item_key"])
    merged["_store_seq_num"] = pd.to_numeric(merged["store_seq"], errors="coerce")
    merged["_raw_offset_num"] = pd.to_numeric(merged["raw_offset"], errors="coerce")
    mapped = merged[merged["_store_seq_num"].notna() & merged["_raw_offset_num"].notna()].copy()
    exact_raw_code = mapped["raw_code"].fillna("").astype(str).str.strip().eq(mapped["_old_code"])
    exact_old_codes = set(mapped.loc[exact_raw_code, "_old_code"])
    if exact_old_codes:
        mapped = mapped[exact_raw_code | ~mapped["_old_code"].isin(exact_old_codes)].copy()
    mapped["_new_code"] = (
        BASE
        + mapped["_store_seq_num"].astype("int64") * NEW_BLOCK_SIZE
        + mapped["_raw_offset_num"].astype("int64")
    ).astype(str)
    mapped = mapped[mapped["_old_code"].ne("") & mapped["_old_code"].ne(mapped["_new_code"])]

    identity_cols = ["_old_code", "store", "item_key"]
    conflict_counts = (
        mapped.groupby(identity_cols)["_new_code"]
        .nunique()
        .loc[lambda s: s > 1]
    )
    conflict_keys = set(conflict_counts.index.tolist())
    if conflict_keys:
        mapped = mapped[
            ~mapped[identity_cols].apply(lambda row: tuple(row.tolist()) in conflict_keys, axis=1)
        ].copy()

    identity_mapping = (
        mapped[identity_cols + ["_new_code"]]
        .drop_duplicates()
        .rename(columns={"_old_code": "old_code", "_new_code": "new_code"})
    )
    code_counts = identity_mapping.groupby("old_code")["new_code"].nunique()
    unambiguous_codes = set(code_counts[code_counts.eq(1)].index)
    code_mapping = (
        identity_mapping[identity_mapping["old_code"].isin(unambiguous_codes)]
        .drop_duplicates("old_code")
        .set_index("old_code")["new_code"]
        .to_dict()
    )
    mapping = {
        "code": code_mapping,
        "identity": identity_mapping.to_dict("records"),
    }
    stats = {
        "okpos_master_rows": int(len(okpos)),
        "raw_identity_rows": int(len(raw)),
        "mapped_identity_rows": int(len(identity_mapping)),
        "mapped_unambiguous_old_codes": int(len(code_mapping)),
        "unmapped_okpos_rows": int(okpos[~okpos["_old_code"].isin(set(identity_mapping["old_code"]))].shape[0]),
        "ambiguous_identity_keys": int(len(conflict_keys)),
        "ambiguous_samples": [list(key) for key in list(conflict_keys)[:20]],
    }
    return mapping, stats


def _snapshot_review_values(df: pd.DataFrame) -> pd.DataFrame:
    cols = [col for col in ("source", "상품코드", *REVIEW_COLUMNS) if col in df.columns]
    return df.loc[_source_series(df).eq(SOURCE), cols].copy()


def _row_item_key_frame(df: pd.DataFrame) -> pd.Series:
    if "item_key" in df.columns:
        item_key = df["item_key"].fillna("").astype(str).str.strip()
    else:
        item_key = pd.Series([""] * len(df), index=df.index)
    if (item_key == "").any():
        for name_col in ("상품명", "item_name"):
            if name_col in df.columns:
                name_key = df[name_col].fillna("").astype(str).map(normalize_item_key)
                item_key = item_key.where(item_key != "", name_key)
                break
    return item_key


def _rewrite_codes(df: pd.DataFrame, mapping: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, int]]:
    out = df.copy()
    code_mapping = mapping["code"]
    identity_mapping = pd.DataFrame(mapping["identity"])
    stats: dict[str, int] = {}
    for column in CODE_COLUMNS:
        if column not in out.columns:
            stats[column] = 0
            continue
        values = out[column].fillna("").astype(str).str.strip()
        source_mask = _source_series(out).eq(SOURCE)
        changed = pd.Series(False, index=out.index)

        if not identity_mapping.empty and "store" in out.columns:
            key_df = pd.DataFrame({
                "_idx": out.index,
                "old_code": values,
                "store": out["store"].fillna("").astype(str).str.strip(),
                "item_key": _row_item_key_frame(out),
            })
            merged = key_df.merge(identity_mapping, how="left", on=["old_code", "store", "item_key"])
            has_identity = source_mask & merged["new_code"].fillna("").ne("").set_axis(out.index)
            if has_identity.any():
                out.loc[has_identity, column] = merged.loc[has_identity.to_numpy(), "new_code"].values
                changed |= has_identity

        remaining = source_mask & ~changed & values.isin(code_mapping)
        if remaining.any():
            out.loc[remaining, column] = values[remaining].map(code_mapping).values
            changed |= remaining
        stats[column] = int(changed.sum())
    if {"상품코드", "store_seq", "item_seq"}.issubset(out.columns):
        mask = _okpos_code_mask(out, "상품코드")
        codes = _code_series(out, "상품코드")
        offset = codes - BASE
        out.loc[mask, "store_seq"] = (offset[mask] // NEW_BLOCK_SIZE).astype("int64").astype(str).values
        out.loc[mask, "item_seq"] = (offset[mask] % NEW_BLOCK_SIZE).astype("int64").astype(str).values
    return out, stats


def _migrate_csv(path: Path, mapping: dict[str, str], apply: bool, validate_master: bool = False) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "changed": 0}
    before = _read_csv(path)
    review_before = _snapshot_review_values(before) if validate_master else pd.DataFrame()
    after, stats = _rewrite_codes(before, mapping)
    changed = sum(stats.values())
    result: dict[str, Any] = {
        "path": str(path),
        "exists": True,
        "changed": int(changed),
        "columns": stats,
    }
    if validate_master:
        validate_fin_product_codes(after, allow_legacy_blank=True)
        review_after = _snapshot_review_values(after)
        result["review_rows_before"] = int(len(review_before))
        result["review_rows_after"] = int(len(review_after))
        result["validate"] = "OK"
    if apply and changed:
        result["backup"] = str(_backup(path))
        _write_csv(path, after)
    return result


def _migrate_train_json(path: Path, mapping: dict[str, str], apply: bool) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "changed": 0}
    text = path.read_text(encoding="utf-8")
    changed = 0
    new_text = text
    for old, new in mapping["code"].items():
        count = new_text.count(old)
        if count:
            changed += count
            new_text = new_text.replace(old, new)
    result: dict[str, Any] = {"path": str(path), "exists": True, "changed": changed}
    if apply and changed:
        result["backup"] = str(_backup(path))
        path.write_text(new_text, encoding="utf-8")
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="실제 CSV/JSON 파일을 백업 후 수정")
    return parser.parse_args()


def main() -> dict[str, Any]:
    args = _parse_args()
    master = _read_csv(FIN_PRODUCT_CSV_PATH)
    mapping, mapping_stats = _build_mapping(master)
    targets = [
        (FIN_PRODUCT_CSV_PATH, True),
        (FIN_PRODUCT_MART_CSV_PATH, False),
        (FIN_PRODUCT_MAP_CSV_PATH, False),
        (FIN_PRODUCT_MAP_REVIEW_CSV_PATH, False),
        (FIN_PRODUCT_MAP_RECENTLY_CSV_PATH, False),
        (FIN_PRODUCT_MAP_JOIN_CSV_PATH, False),
    ]
    files = [_migrate_csv(path, mapping, args.apply, validate_master) for path, validate_master in targets]
    files.append(_migrate_train_json(FIN_PRODUCT_MAP_TRAIN_JSON_PATH, mapping, args.apply))
    return {
        "meta": {
            "script": "migrate_okpos_item_block",
            "mode": "apply" if args.apply else "dry-run",
            "old_block_size": OLD_BLOCK_SIZE,
            "new_block_size": NEW_BLOCK_SIZE,
        },
        "summary": {
            "result": "OK",
            "mapping": mapping_stats,
            "changed_total": int(sum(item.get("changed", 0) for item in files)),
        },
        "files": files,
    }


if __name__ == "__main__":
    run_script(main, "migrate_okpos_item_block")
