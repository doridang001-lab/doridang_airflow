"""배민 주문 파티션을 주문시각 월 기준으로 재정리한다.

기본은 dry-run이다. OneDrive analytics 파일 변경은 --apply 지정 시에만 수행한다.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.db.DB_Beamin_04_orders import _COLUMNS
from modules.transform.pipelines.db.DB_BaeminManual_load import _upsert_manual_orders
from modules.transform.pipelines.db.beamin_store_io import find_tables, order_ym, read_file, write_table
from modules.transform.utility.paths import BAEMIN_ORDERS_DB, LOCAL_DB
from scripts._base import save_summary

logger = logging.getLogger(__name__)

SONGPA_BRAND = "도리당"
SONGPA_STORE = "송파삼전점"
MANUAL_SOURCE_DIR = Path(r"E:\d_down")
SONGPA_MANUAL_FILES = {
    "2026-05": "baemin_orders_[음식배달]_닭도리탕_전문_도리당_송파삼전점_unknown_20260713.csv",
    "2026-06": "baemin_orders_[음식배달]_닭도리탕_전문_도리당_송파삼전점_unknown_20260713 (1).csv",
    "2026-07": "baemin_orders_[음식배달]_닭도리탕_전문_도리당_송파삼전점_unknown_20260713 (2).csv",
}


def _partition_value(path: Path, prefix: str) -> str:
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix):]
    return ""


def _source_ym(path: Path) -> str:
    return _partition_value(path, "ym=")


def _stem(brand: str, store: str, ym: str) -> Path:
    return BAEMIN_ORDERS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}" / f"orders_{ym}"


def _as_order_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in _COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out[_COLUMNS].fillna("").astype(str)


def _backup_path(path: Path, backup_root: Path) -> Path:
    try:
        relative = path.relative_to(BAEMIN_ORDERS_DB)
    except ValueError as exc:
        raise RuntimeError(f"백업 대상이 배민 orders 범위를 벗어남: {path}") from exc
    return backup_root / relative.parent / f"{path.name}.bak"


def _backup_existing(stem: Path, apply: bool, backup_root: Path) -> list[str]:
    backed_up: list[str] = []
    for path in (stem.with_suffix(".parquet"), stem.with_suffix(".csv")):
        if not path.exists():
            continue
        backup = _backup_path(path, backup_root)
        if apply:
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, backup)
        backed_up.append(str(backup))
    return backed_up


def _amount_sum(df: pd.DataFrame) -> int:
    if "결제금액" not in df.columns:
        return 0
    amount = pd.to_numeric(
        df["결제금액"].fillna("").astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    ).fillna(0)
    return int(amount.sum())


def _order_count(df: pd.DataFrame) -> int:
    if "주문번호" not in df.columns:
        return 0
    return int(df["주문번호"].fillna("").astype(str).str.strip().replace("", pd.NA).nunique())


def _load_store_files(files: list[Path]) -> pd.DataFrame:
    frames = []
    for path in files:
        df = _as_order_columns(read_file(path))
        parsed_ym = order_ym(df["주문시각"])
        df["_target_ym"] = parsed_ym.where(parsed_ym.ne(""), _source_ym(path))
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=[*_COLUMNS, "_target_ym"])
    return pd.concat(frames, ignore_index=True).fillna("").astype(str)


def _dedup_with_target_ym(df: pd.DataFrame) -> pd.DataFrame:
    deduped = _upsert_manual_orders(None, df)
    parsed_ym = order_ym(deduped["주문시각"])
    deduped["_target_ym"] = parsed_ym.where(parsed_ym.ne(""), deduped["_target_ym"])
    return deduped


def repartition_other_stores(apply: bool, backup_root: Path) -> dict[str, Any]:
    files_by_store: dict[tuple[str, str], list[Path]] = defaultdict(list)
    for path in find_tables(BAEMIN_ORDERS_DB, "brand=*/store=*/ym=*/orders_*"):
        brand = _partition_value(path, "brand=")
        store = _partition_value(path, "store=")
        if brand == SONGPA_BRAND and store == SONGPA_STORE:
            continue
        files_by_store[(brand, store)].append(path)

    stats: dict[str, Any] = {
        "stores": len(files_by_store),
        "source_files": sum(len(paths) for paths in files_by_store.values()),
        "written_partitions": 0,
        "cleared_partitions": 0,
        "backups": 0,
        "moves": [],
        "issues": [],
    }

    for (brand, store), paths in sorted(files_by_store.items()):
        loaded = _load_store_files(paths)
        if loaded.empty:
            continue

        deduped = _dedup_with_target_ym(loaded)
        groups = {ym: group for ym, group in deduped.groupby("_target_ym", sort=True) if ym}
        source_yms = {_source_ym(path) for path in paths if _source_ym(path)}
        for ym in sorted(source_yms | set(groups)):
            if not ym:
                stats["issues"].append({"brand": brand, "store": store, "issue": "empty_target_ym"})
                continue

            group = groups.get(ym)
            if group is None:
                out_df = pd.DataFrame(columns=_COLUMNS, dtype=str)
                stats["cleared_partitions"] += 1
            else:
                out_df = group.drop(columns=["_target_ym"], errors="ignore")[_COLUMNS]
            target_stem = _stem(brand, store, ym)
            backups = _backup_existing(target_stem, apply, backup_root)
            if apply:
                write_table(out_df, target_stem)
            stats["written_partitions"] += 1
            stats["backups"] += len(backups)
            stats["moves"].append(
                {
                    "brand": brand,
                    "store": store,
                    "ym": ym,
                    "rows": len(out_df),
                    "orders": _order_count(out_df),
                    "amount": _amount_sum(out_df),
                    "backups": backups,
                }
            )

    logger.info(
        "재파티션 %s: stores=%d source_files=%d partitions=%d cleared=%d backups=%d",
        "APPLY" if apply else "DRY-RUN",
        stats["stores"],
        stats["source_files"],
        stats["written_partitions"],
        stats["cleared_partitions"],
        stats["backups"],
    )
    return stats


def replace_songpa_manual(apply: bool, backup_root: Path) -> dict[str, Any]:
    stats: dict[str, Any] = {"partitions": [], "issues": []}
    for ym, filename in SONGPA_MANUAL_FILES.items():
        csv_path = MANUAL_SOURCE_DIR / filename
        if not csv_path.exists():
            raise FileNotFoundError(f"송파삼전점 수동 CSV 없음: {csv_path}")

        df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
        out_df = _as_order_columns(df)
        parsed = order_ym(out_df["주문시각"])
        bad = out_df[parsed.ne(ym)]
        if not bad.empty:
            sample = bad[["주문번호", "주문시각"]].head(5).to_dict("records")
            raise RuntimeError(f"송파삼전점 CSV 월 불일치: ym={ym} rows={len(bad)} sample={sample}")

        target_stem = _stem(SONGPA_BRAND, SONGPA_STORE, ym)
        backups = _backup_existing(target_stem, apply, backup_root)
        if apply:
            write_table(out_df, target_stem)
        stats["partitions"].append(
            {
                "ym": ym,
                "source": str(csv_path),
                "rows": len(out_df),
                "orders": _order_count(out_df),
                "amount": _amount_sum(out_df),
                "backups": backups,
            }
        )

    logger.info("송파삼전점 수동 교체 %s: %d partitions", "APPLY" if apply else "DRY-RUN", len(stats["partitions"]))
    return stats


def verify_invariants() -> dict[str, Any]:
    mismatches = []
    cross_month: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for path in find_tables(BAEMIN_ORDERS_DB, "brand=*/store=*/ym=*/orders_*"):
        brand = _partition_value(path, "brand=")
        store = _partition_value(path, "store=")
        ym = _source_ym(path)
        df = _as_order_columns(read_file(path))
        parsed = order_ym(df["주문시각"])
        bad = df[parsed.ne("") & parsed.ne(ym)]
        if not bad.empty:
            mismatches.append(
                {
                    "path": str(path),
                    "ym": ym,
                    "bad_rows": len(bad),
                    "sample": bad[["주문번호", "주문시각"]].head(3).to_dict("records"),
                }
            )
        ids = df["주문번호"].fillna("").astype(str).str.strip()
        for order_id in ids[ids.ne("")].drop_duplicates():
            cross_month[(brand, store, order_id)].add(ym)

    duplicated = [
        {"brand": brand, "store": store, "주문번호": order_id, "ym": sorted(yms)}
        for (brand, store, order_id), yms in cross_month.items()
        if len(yms) > 1
    ]
    return {
        "partition_month_mismatches": len(mismatches),
        "cross_month_duplicate_orders": len(duplicated),
        "mismatch_samples": mismatches[:20],
        "duplicate_samples": duplicated[:20],
    }


def run(apply: bool, skip_repartition: bool, skip_songpa: bool, verify: bool) -> dict[str, Any]:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_root = LOCAL_DB / "temp" / "repartition_baemin_orders" / run_id
    result: dict[str, Any] = {
        "meta": {
            "script": "repartition_baemin_orders",
            "mode": "apply" if apply else "dry-run",
            "base": str(BAEMIN_ORDERS_DB),
            "backup_root": str(backup_root),
        },
        "summary": {"status": "ok"},
        "stats": {},
    }
    if not skip_repartition:
        result["stats"]["repartition"] = repartition_other_stores(apply, backup_root)
    if not skip_songpa:
        result["stats"]["songpa_manual_replace"] = replace_songpa_manual(apply, backup_root)
    if verify:
        result["stats"]["verify"] = verify_invariants()
        verify_stats = result["stats"]["verify"]
        if apply and not (
            verify_stats.get("partition_month_mismatches")
            or verify_stats.get("cross_month_duplicate_orders")
        ):
            shutil.rmtree(backup_root, ignore_errors=True)
            result["meta"]["backup_cleaned"] = True
    if apply and "backup_cleaned" not in result["meta"]:
        result["meta"]["backup_cleaned"] = False
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="실제 백업/저장 수행")
    parser.add_argument("--skip-repartition", action="store_true", help="송파삼전점 외 재파티션 생략")
    parser.add_argument("--skip-songpa", action="store_true", help="송파삼전점 수동 교체 생략")
    parser.add_argument("--verify", action="store_true", help="현재 orders 전체 불변식 검증")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    args = parse_args()
    result = run(
        apply=args.apply,
        skip_repartition=args.skip_repartition,
        skip_songpa=args.skip_songpa,
        verify=args.verify,
    )
    out_path = save_summary("repartition_baemin_orders", result)
    logger.info("summary 저장: %s", out_path)
    verify_stats = result.get("stats", {}).get("verify")
    if verify_stats and (
        verify_stats.get("partition_month_mismatches") or verify_stats.get("cross_month_duplicate_orders")
    ):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
