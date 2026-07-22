"""Migrate manual delivery item_id bands from 6 digits to 9 digits.

Default mode is dry-run. Pass --apply to write MART_DB files.
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
    MANUAL_SOURCE_BASE,
    SOURCE_CODE_RANGES,
    canonical_source,
    validate_fin_product_codes,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (  # noqa: E402
    UNIFIED_COLUMNS,
    iter_unified_sales_files,
)
from modules.transform.utility.paths import (  # noqa: E402
    FIN_PRODUCT_CSV_PATH,
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_RECENTLY_CSV_PATH,
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
    FIN_PRODUCT_MAP_TRAIN_JSON_PATH,
)


OLD_MANUAL_BASE = {
    "posfeed": 100_000,
    "배민수동": 200_000,
    "쿠팡수동": 300_000,
}
MANUAL_SOURCES = tuple(OLD_MANUAL_BASE)
DELTAS = {
    source: MANUAL_SOURCE_BASE[source] - old_base
    for source, old_base in OLD_MANUAL_BASE.items()
}


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _backup(path: Path) -> Path:
    backup_dir = path.parent / "_backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / f"{path.stem}.bak_{_timestamp()}{path.suffix}"
    shutil.copy2(path, backup)
    return backup


def _safe_replace(tmp: Path, target: Path) -> None:
    try:
        tmp.replace(target)
    except (PermissionError, OSError):
        target.write_bytes(tmp.read_bytes())


def _source_series(df: pd.DataFrame) -> pd.Series:
    if "source" not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return df["source"].fillna("").astype(str).str.strip().map(canonical_source)


def _numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index)
    return pd.to_numeric(df[column].fillna("").astype(str).str.strip(), errors="coerce")


def _format_code(value: int) -> str:
    return str(int(value))


def _manual_source_mask(source: pd.Series, src: str) -> pd.Series:
    return source.eq(src)


def _migration_mask(df: pd.DataFrame, code_column: str, src: str) -> pd.Series:
    source = _source_series(df)
    code = _numeric_series(df, code_column)
    old_base = OLD_MANUAL_BASE[src]
    new_base = MANUAL_SOURCE_BASE[src]
    return _manual_source_mask(source, src) & code.ge(old_base) & code.lt(new_base)


def _range_issue_count(df: pd.DataFrame, code_column: str) -> int:
    source = _source_series(df)
    code = _numeric_series(df, code_column)
    issues = pd.Series(False, index=df.index)
    for src in MANUAL_SOURCES:
        start, end = SOURCE_CODE_RANGES[src]
        mask = source.eq(src) & code.notna()
        issues |= mask & ~code.between(start, end)
    return int(issues.sum())


def _summarize(df: pd.DataFrame, code_column: str) -> dict[str, dict[str, int | None]]:
    source = _source_series(df)
    code = _numeric_series(df, code_column)
    summary: dict[str, dict[str, int | None]] = {}
    for src in MANUAL_SOURCES:
        mask = source.eq(src) & code.notna()
        values = code[mask]
        summary[src] = {
            "rows": int(mask.sum()),
            "min": int(values.min()) if not values.empty else None,
            "max": int(values.max()) if not values.empty else None,
            "to_migrate": int(_migration_mask(df, code_column, src).sum()),
        }
    return summary


def _empty_summary() -> dict[str, dict[str, int | None]]:
    return {
        src: {"rows": 0, "min": None, "max": None, "to_migrate": 0}
        for src in MANUAL_SOURCES
    }


def _merge_summary(
    total: dict[str, dict[str, int | None]],
    part: dict[str, dict[str, int | None]],
) -> None:
    for src in MANUAL_SOURCES:
        total[src]["rows"] = int(total[src]["rows"] or 0) + int(part[src]["rows"] or 0)
        total[src]["to_migrate"] = int(total[src]["to_migrate"] or 0) + int(part[src]["to_migrate"] or 0)
        part_min = part[src]["min"]
        part_max = part[src]["max"]
        if part_min is not None:
            total[src]["min"] = part_min if total[src]["min"] is None else min(int(total[src]["min"]), int(part_min))
        if part_max is not None:
            total[src]["max"] = part_max if total[src]["max"] is None else max(int(total[src]["max"]), int(part_max))


def _migrate_code_column(df: pd.DataFrame, code_column: str) -> tuple[pd.DataFrame, dict[str, int]]:
    out = df.copy()
    stats: dict[str, int] = {}
    if code_column not in out.columns:
        return out, {src: 0 for src in MANUAL_SOURCES}
    numeric = _numeric_series(out, code_column)
    for src in MANUAL_SOURCES:
        mask = _migration_mask(out, code_column, src)
        stats[src] = int(mask.sum())
        if mask.any():
            updated = numeric.loc[mask].astype("int64") + DELTAS[src]
            out.loc[mask, code_column] = updated.map(_format_code).values
    return out, stats


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    tmp = path.with_suffix(".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _migrate_csv(path: Path, code_column: str, apply: bool) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "changed": 0}
    df = _read_csv(path)
    before = _summarize(df, code_column)
    migrated, stats = _migrate_code_column(df, code_column)
    changed = sum(stats.values())
    result = {
        "path": str(path),
        "exists": True,
        "changed": changed,
        "before": before,
        "after": _summarize(migrated, code_column),
        "range_issues_after": _range_issue_count(migrated, code_column),
    }
    if apply and changed:
        backup = _backup(path)
        _write_csv(path, migrated)
        result["backup"] = str(backup)
    return result


def _migrate_fin_product_grp(apply: bool) -> dict[str, Any]:
    result = _migrate_csv(FIN_PRODUCT_CSV_PATH, "상품코드", apply)
    if result.get("exists"):
        migrated = _read_csv(FIN_PRODUCT_CSV_PATH) if apply and result.get("changed") else _migrate_code_column(_read_csv(FIN_PRODUCT_CSV_PATH), "상품코드")[0]
        validate_fin_product_codes(migrated)
        result["validate"] = "OK"
    return result


def _migrate_unified_sales(apply: bool, *, verbose: bool = False) -> dict[str, Any]:
    files = []
    total_changed = 0
    changed_files = 0
    before_total = _empty_summary()
    after_total = _empty_summary()
    range_issues_after = 0
    for path in iter_unified_sales_files():
        df = pd.read_parquet(path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        before = _summarize(df, "item_id")
        migrated, stats = _migrate_code_column(df, "item_id")
        changed = sum(stats.values())
        total_changed += changed
        if changed:
            changed_files += 1
        after = _summarize(migrated, "item_id")
        file_range_issues = _range_issue_count(migrated, "item_id")
        _merge_summary(before_total, before)
        _merge_summary(after_total, after)
        range_issues_after += file_range_issues
        file_result = {
            "path": str(path),
            "changed": changed,
            "before": before,
            "after": after,
            "range_issues_after": file_range_issues,
        }
        if apply and changed:
            backup = _backup(path)
            migrated.to_parquet(path, index=False, engine="pyarrow")
            file_result["backup"] = str(backup)
        if verbose:
            files.append(file_result)
    result = {
        "file_count": len(iter_unified_sales_files()),
        "changed_files": changed_files,
        "changed": total_changed,
        "before": before_total,
        "after": after_total,
        "range_issues_after": range_issues_after,
    }
    if verbose:
        result["files"] = files
    return result


def _migrate_train_value(value: Any) -> tuple[Any, int]:
    if isinstance(value, list):
        changed = 0
        items = []
        for item in value:
            new_item, item_changed = _migrate_train_value(item)
            items.append(new_item)
            changed += item_changed
        return items, changed
    if isinstance(value, dict):
        out = dict(value)
        changed = 0
        source = canonical_source(out.get("source", ""))
        if source in MANUAL_SOURCES and "item_id" in out:
            code = pd.to_numeric(pd.Series([str(out.get("item_id", "")).strip()]), errors="coerce").iloc[0]
            if pd.notna(code) and int(code) < MANUAL_SOURCE_BASE[source] and int(code) >= OLD_MANUAL_BASE[source]:
                out["item_id"] = _format_code(int(code) + DELTAS[source])
                changed += 1
        for key, item in list(out.items()):
            if key == "item_id":
                continue
            new_item, item_changed = _migrate_train_value(item)
            out[key] = new_item
            changed += item_changed
        return out, changed
    return value, 0


def _migrate_train_json(apply: bool) -> dict[str, Any]:
    path = FIN_PRODUCT_MAP_TRAIN_JSON_PATH
    if not path.exists():
        return {"path": str(path), "exists": False, "changed": 0}
    payload = json.loads(path.read_text(encoding="utf-8"))
    migrated, changed = _migrate_train_value(payload)
    result = {"path": str(path), "exists": True, "changed": changed}
    if apply and changed:
        backup = _backup(path)
        tmp = path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(migrated, ensure_ascii=False, indent=2), encoding="utf-8")
            _safe_replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)
        result["backup"] = str(backup)
    return result


def _migrate_map_files(apply: bool) -> list[dict[str, Any]]:
    targets = [
        (FIN_PRODUCT_MAP_CSV_PATH, "item_id"),
        (FIN_PRODUCT_MAP_RECENTLY_CSV_PATH, "상품코드"),
        (FIN_PRODUCT_MAP_REVIEW_CSV_PATH, "item_id"),
    ]
    results = [_migrate_csv(path, column, apply) for path, column in targets]
    results.append(_migrate_train_json(apply))
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="실제 MART_DB 파일을 수정합니다.")
    parser.add_argument("--verbose", action="store_true", help="unified_sales 파일별 상세 결과를 출력합니다.")
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    result = {
        "mode": mode,
        "deltas": DELTAS,
        "fin_product_grp": _migrate_fin_product_grp(args.apply),
        "unified_sales": _migrate_unified_sales(args.apply, verbose=args.verbose),
        "map_files": _migrate_map_files(args.apply),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
