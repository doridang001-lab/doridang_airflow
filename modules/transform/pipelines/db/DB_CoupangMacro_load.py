"""Load Coupang macro raw CSV files into analytics partition storage."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
from collections.abc import Callable
from glob import glob
from pathlib import Path

import pandas as pd
import pendulum

from modules.transform.utility.paths import (
    COLLECT_DB,
    COUPANG_ORDERS_DB,
    COUPANG_ORDERS_DETAIL_DB,
    DOWN_DIR,
)
from modules.transform.utility.store_normalize import lookup_store_key

logger = logging.getLogger(__name__)

KNOWN_BRANDS = ["도리당", "나홀로"]
CMG_DIR = COUPANG_ORDERS_DETAIL_DB / "cmg"
OPTIONS_DIR = COUPANG_ORDERS_DETAIL_DB / "options"
COLLECT_SRC = COLLECT_DB / "영업관리부_수집"
ARCHIVE_DIR = COLLECT_SRC / "_archived"


def _iter_source_files(prefix: str) -> list[dict[str, Path]]:
    """Collect files from `E:/down` and `Collect_Data/...`, with source tags."""
    down_pattern = str(DOWN_DIR / f"coupangeats_{prefix}_*.csv")
    collect_pattern = str(COLLECT_SRC / f"coupangeats_{prefix}_*.csv")

    items: list[dict[str, Path]] = []
    seen: set[Path] = set()
    for item in sorted(glob(down_pattern)):
        path = Path(item)
        if path in seen:
            continue
        seen.add(path)
        items.append({"path": path, "source": "down"})

    for item in sorted(glob(collect_pattern)):
        path = Path(item)
        if path in seen:
            continue
        seen.add(path)
        items.append({"path": path, "source": "collect"})

    return items


def _resolve_brand_store(display_name: str) -> tuple[str, str]:
    """Resolve `(brand, store)` from display name and normalize store via store_normalize."""
    name = re.sub(r"\(\d+\)\s*$", "", str(display_name)).strip()
    name = re.sub(r"\s*_\d+\s*$", "", name).strip()
    if not name:
        logger.warning("display_name missing when resolving brand/store: %s", display_name)
        return "", ""

    brand = next((b for b in KNOWN_BRANDS if b in name), "")
    if not brand:
        logger.warning("brand not matched (KNOWN_BRANDS=%s): %s", KNOWN_BRANDS, name)
        return "", ""

    store_token = name.split()[-1] if name.split() else ""
    if not store_token:
        logger.warning("store token missing for display name: %s", name)
        return brand, ""

    store = lookup_store_key(brand, store_token) or store_token
    if not store:
        logger.warning("store normalizing failed: brand=%s display=%s", brand, name)
        return brand, ""

    return brand, store


def _ym_from_order_date(value: str) -> str:
    if not value:
        return ""
    text = str(value).strip()
    if len(text) < 7:
        return ""
    return text[:7].replace(".", "-")


def _ym_from_iso(value: str) -> str:
    if not value:
        return ""
    text = str(value).strip()
    return text[:7]


def _ym_from_filename(path: Path) -> str:
    matches = re.findall(r"\d{8}", path.name)
    if not matches:
        logger.warning("ym token not found in filename: %s", path)
        return ""

    token = matches[-1]
    if len(token) != 8:
        logger.warning("invalid ym token %s in filename: %s", token, path)
        return ""
    return f"{token[:4]}-{token[4:6]}"


def _row_hash_from_series(row: pd.Series) -> str:
    values = ["" if pd.isna(v) else str(v) for v in row.to_list()]
    return hashlib.md5("|".join(values).encode("utf-8")).hexdigest()[:12]


def _orders_partition_dir(brand: str, store: str, ym: str) -> Path:
    return COUPANG_ORDERS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"


def _dataset_partition_dir(root_dir: Path, brand: str, store: str, ym: str) -> Path:
    return root_dir / f"brand={brand}" / f"store={store}" / f"ym={ym}"


def _archive_path(path: Path) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    target = ARCHIVE_DIR / path.name
    if not target.exists():
        return target

    stem = path.stem
    suffix = path.suffix
    idx = 1
    while True:
        candidate = ARCHIVE_DIR / f"{stem}.{idx}{suffix}"
        if not candidate.exists():
            return candidate
        idx += 1


def _cleanup_sources(loaded_files: list[dict[str, Path]]) -> None:
    for item in loaded_files:
        path = Path(item["path"])
        if not path.exists():
            continue

        source = str(item["source"])
        if source == "down":
            try:
                path.unlink()
                logger.info("deleted from DOWN_DIR: %s", path)
            except Exception as exc:  # pragma: no cover - 운영 환경 처리
                logger.warning("failed to delete %s: %s", path, exc)
        elif source == "collect":
            target = _archive_path(path)
            try:
                shutil.move(path, target)
                logger.info("archived collect data: %s -> %s", path, target)
            except Exception as exc:  # pragma: no cover - 운영 환경 처리
                logger.warning("failed to archive %s: %s", path, exc)
        else:
            logger.warning("unknown source tag for %s: %s", path, source)


COUPANG_ORDER_DEDUP_COLUMNS = [
    "store_id",
    "order_id",
    "order_date",
    "menu_name",
    "menu_qty",
    "menu_price",
    "menu_options",
    "total_price",
    "is_cancelled",
]


def _canonical_coupang_order_keys(df: pd.DataFrame) -> pd.DataFrame:
    keys = pd.DataFrame(index=df.index)
    numeric_cols = {"menu_qty", "menu_price", "total_price", "매출액", "취소금액"}
    for col in COUPANG_ORDER_DEDUP_COLUMNS:
        if col not in df.columns:
            keys[col] = ""
            continue
        if col in numeric_cols:
            values = pd.to_numeric(df[col], errors="coerce")
            keys[col] = values.round(6).astype("string").fillna("")
        else:
            keys[col] = df[col].fillna("").astype(str).str.strip()
    return keys


def _deduplicate_coupang_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if df.empty:
        return df, 0

    before = len(df)
    work = df.copy()
    if "collected_at" in work.columns:
        work["_collected_at_sort"] = pd.to_datetime(work["collected_at"], errors="coerce", utc=True)
        work = work.sort_values("_collected_at_sort", ascending=False, na_position="last", kind="mergesort")

    keys = _canonical_coupang_order_keys(work)
    deduped = work.loc[~keys.duplicated(keep="first")].drop(columns=["_collected_at_sort"], errors="ignore")
    deduped = deduped.reset_index(drop=True)
    return deduped, before - len(deduped)


def repair_coupang_orders_parquet(
    dry_run: bool = True,
    stores: list[str] | None = None,
    ym: str | None = None,
    backup: bool = True,
) -> str:
    """쿠팡 orders parquet 중복을 정규화된 주문 라인 기준으로 제거한다."""
    files = sorted(COUPANG_ORDERS_DB.rglob("orders_*.parquet"))
    if stores:
        store_set = {str(store).strip() for store in stores if str(store).strip()}
        files = [path for path in files if any(f"store={store}" in str(path) for store in store_set)]
    if ym:
        ym_token = str(ym).strip()
        files = [path for path in files if f"ym={ym_token}" in str(path)]

    if not files:
        return f"수정 대상 없음 | {COUPANG_ORDERS_DB}"

    total_before = 0
    total_after = 0
    changed_files = 0
    examples: list[str] = []
    backup_root = COUPANG_ORDERS_DETAIL_DB / "_repair_backup" / pendulum.now("Asia/Seoul").format("YYYYMMDD_HHmmss")
    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, exc)
            continue

        before = len(df)
        deduped, removed = _deduplicate_coupang_orders(df)
        after = len(deduped)
        total_before += before
        total_after += after
        if removed:
            changed_files += 1
            if len(examples) < 10:
                examples.append(f"{path.name}:{before}->{after}(-{removed})")
            if not dry_run:
                if backup:
                    backup_path = backup_root / path.relative_to(COUPANG_ORDERS_DB)
                    backup_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, backup_path)
                deduped.to_parquet(path, index=False, engine="pyarrow")
                logger.info("repair 저장: %s | %d -> %d행 (%d 제거)", path, before, after, removed)
            else:
                logger.info("repair dry-run: %s | %d -> %d행 (%d 제거)", path, before, after, removed)

    mode = "DRY-RUN" if dry_run else "수정 완료"
    removed_total = total_before - total_after
    sample = " | ".join(examples)
    suffix = f" | 예시={sample}" if sample else ""
    backup_msg = f" | 백업={backup_root}" if (backup and not dry_run and changed_files) else ""
    return f"{mode} | 파일={len(files)} 변경파일={changed_files} 제거={removed_total}행 ({total_before}->{total_after}){backup_msg}{suffix}"


def migrate_jungdong_partition(dry_run: bool = True, ym: str | None = "2026-06", backup: bool = True) -> str:
    """store=중동점 orders 파티션을 store=해운대중동점으로 병합한다."""
    src_base = COUPANG_ORDERS_DB / "brand=도리당" / "store=중동점"
    dst_base = COUPANG_ORDERS_DB / "brand=도리당" / "store=해운대중동점"

    if not src_base.exists():
        return f"중동점 파티션 없음, 스킵 | {src_base}"

    src_files = sorted(src_base.rglob("orders_*.parquet"))
    if ym:
        ym_token = str(ym).strip()
        src_files = [path for path in src_files if path.parent.name == f"ym={ym_token}"]

    if not src_files:
        target = f"ym={ym}" if ym else "전체 ym"
        return f"수정 대상 없음 | {src_base} | {target}"

    backup_root = COUPANG_ORDERS_DETAIL_DB / "_partition_migration_backup" / pendulum.now("Asia/Seoul").format("YYYYMMDD_HHmmss")
    operations: list[dict] = []
    affected_ym_dirs: set[Path] = set()
    results: list[str] = []

    for src_path in src_files:
        ym_part = src_path.parent.name
        dst_dir = dst_base / ym_part
        dst_path = dst_dir / src_path.name

        src_df = pd.read_parquet(src_path)
        if "order_id" not in src_df.columns:
            return f"중단: source order_id 컬럼 없음 | {src_path}"

        if dst_path.exists():
            dst_df = pd.read_parquet(dst_path)
            if "order_id" not in dst_df.columns:
                return f"중단: target order_id 컬럼 없음 | {dst_path}"
            new_ids = set(src_df["order_id"].dropna().astype(str).unique())
            keep_old = dst_df[~dst_df["order_id"].astype(str).isin(new_ids)]
            merged = pd.concat([keep_old, src_df], ignore_index=True)
            before_msg = f"dst={len(dst_df)}행/{dst_df['order_id'].nunique()}건"
        else:
            dst_df = pd.DataFrame()
            merged = src_df
            before_msg = "dst=없음"

        operations.append({"src_path": src_path, "dst_path": dst_path, "merged": merged})
        affected_ym_dirs.add(src_path.parent)
        results.append(
            f"{ym_part}: src={len(src_df)}행/{src_df['order_id'].nunique()}건, "
            f"{before_msg}, merged={len(merged)}행/{merged['order_id'].nunique()}건, "
            f"delete={src_path.parent}"
        )

    if dry_run:
        return f"DRY-RUN | 백업예정={backup_root} | " + " | ".join(results)

    for operation in operations:
        src_path = operation["src_path"]
        dst_path = operation["dst_path"]
        merged = operation["merged"]

        if backup:
            src_backup = backup_root / src_path.relative_to(COUPANG_ORDERS_DB)
            src_backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, src_backup)
            if dst_path.exists():
                dst_backup = backup_root / dst_path.relative_to(COUPANG_ORDERS_DB)
                dst_backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(dst_path, dst_backup)

        dst_path.parent.mkdir(parents=True, exist_ok=True)
        merged.astype(str).to_parquet(dst_path, index=False)
        logger.info("migrated jungdong orders: %s -> %s (%d행)", src_path, dst_path, len(merged))

    for ym_dir in sorted(affected_ym_dirs, key=lambda path: str(path), reverse=True):
        shutil.rmtree(ym_dir)
        logger.info("deleted source ym partition: %s", ym_dir)

    try:
        src_base.rmdir()
        logger.info("deleted empty source store partition: %s", src_base)
    except OSError:
        logger.info("source store partition remains because it is not empty: %s", src_base)

    backup_msg = f" | 백업={backup_root}" if backup else ""
    return f"완료{backup_msg} | " + " | ".join(results)


def _load_orders(files: list[dict[str, Path]]) -> dict:
    loaded_files: list[dict[str, Path]] = []
    outputs: list[str] = []
    grouped_rows: dict[tuple[str, str, str], list[dict]] = {}
    total_rows = 0

    for item in files:
        path = Path(item["path"])
        source = item.get("source", "unknown")
        try:
            df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
        except Exception as exc:
            logger.error("failed to read orders file %s: %s", path, exc)
            continue

        if df.empty:
            logger.warning("empty orders file: %s", path)
            continue

        file_rows = 0
        for _, row in df.iterrows():
            brand, store = _resolve_brand_store(row.get("store_name", ""))
            if not brand or not store:
                continue

            ym = _ym_from_order_date(row.get("order_date", ""))
            if not ym:
                logger.warning("invalid order_date for %s: %s", path, row.get("order_date"))
                continue

            row_dict = row.astype(str).to_dict()
            row_dict["_row_hash"] = _row_hash_from_series(row.astype(str))
            grouped_rows.setdefault((brand, store, ym), []).append(row_dict)
            file_rows += 1

        if file_rows > 0:
            loaded_files.append({"path": path, "source": source})
            total_rows += file_rows

    for (brand, store, ym), rows in grouped_rows.items():
        out_dir = _orders_partition_dir(brand, store, ym)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"orders_{ym}.parquet"

        new_df = pd.DataFrame(rows)
        new_df, removed_new = _deduplicate_coupang_orders(new_df)
        if removed_new:
            logger.warning("orders csv 내부 중복 제거: %s | %d행", out_path, removed_new)

        if out_path.exists():
            try:
                existing = pd.read_parquet(out_path)
            except Exception as exc:
                logger.error("failed to read existing orders parquet %s: %s", out_path, exc)
                existing = pd.DataFrame()

            if not existing.empty and "order_id" in existing.columns and "order_id" in new_df.columns:
                new_ids = set(new_df["order_id"].dropna().astype(str).unique())
                keep = existing[~existing["order_id"].astype(str).isin(new_ids)]
                combined = pd.concat([keep, new_df], ignore_index=True)
            else:
                combined = pd.concat([existing, new_df], ignore_index=True)
                combined, removed = _deduplicate_coupang_orders(combined)
                if removed:
                    logger.warning("orders parquet 중복 제거: %s | %d행", out_path, removed)
        else:
            combined = new_df

        combined = combined.astype(str)
        combined.to_parquet(out_path, index=False)
        outputs.append(str(out_path))
        logger.info("saved orders parquet: %s (%d rows)", out_path, len(combined))

    return {
        "files_found": len(files),
        "files_loaded": len(loaded_files),
        "rows_loaded": total_rows,
        "outputs": outputs,
        "loaded_files": loaded_files,
    }


def _load_csv_dataset(
    files: list[dict[str, Path]],
    out_dir: Path,
    out_name: str,
    ym_fn: Callable[[pd.Series, Path], str],
    dedup_subset: list[str],
) -> dict:
    loaded_files: list[dict[str, Path]] = []
    outputs: list[str] = []
    grouped_rows: dict[tuple[str, str, str], list[dict]] = {}
    total_rows = 0

    for item in files:
        path = Path(item["path"])
        source = item.get("source", "unknown")
        try:
            df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
        except Exception as exc:
            logger.error("failed to read %s file %s: %s", out_name, path, exc)
            continue

        if df.empty:
            logger.warning("empty %s file: %s", out_name, path)
            continue

        file_rows = 0
        for _, row in df.iterrows():
            brand, store = _resolve_brand_store(row.get("매장명", ""))
            if not brand or not store:
                continue

            ym = ym_fn(row, path)
            if not ym:
                logger.warning("invalid ym for %s: %s", out_name, path)
                continue

            row_dict = row.astype(str).to_dict()
            grouped_rows.setdefault((brand, store, ym), []).append(row_dict)
            file_rows += 1

        if file_rows > 0:
            loaded_files.append({"path": path, "source": source})
            total_rows += file_rows

    for (brand, store, ym), rows in grouped_rows.items():
        target_dir = _dataset_partition_dir(out_dir, brand, store, ym)
        target_dir.mkdir(parents=True, exist_ok=True)
        out_path = target_dir / f"{out_name}.csv"

        new_df = pd.DataFrame(rows)
        if out_path.exists():
            try:
                existing = pd.read_csv(out_path, dtype=str)
            except Exception as exc:
                logger.error("failed to read existing %s: %s", out_path, exc)
                existing = pd.DataFrame()
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df

        combined = combined.astype(str)
        missing = [col for col in dedup_subset if col not in combined.columns]
        for col in missing:
            combined[col] = ""
        if dedup_subset:
            combined = combined.drop_duplicates(subset=dedup_subset, keep="last")

        combined.to_csv(out_path, index=False, encoding="utf-8-sig")
        outputs.append(str(out_path))
        logger.info("saved %s partition: %s (%d rows)", out_name, out_path, len(combined))

    return {
        "files_found": len(files),
        "files_loaded": len(loaded_files),
        "rows_loaded": total_rows,
        "outputs": outputs,
        "loaded_files": loaded_files,
    }


def load_coupang_macro_partition() -> str:
    """Load Coupang macro raw CSV files from DOWN_DIR/Collect_Data and clean sources."""
    order_files = _iter_source_files("orders")
    cmg_files = _iter_source_files("cmg")
    options_files = _iter_source_files("options")

    if not order_files and not cmg_files and not options_files:
        message = "처리할 파일이 없습니다."
        logger.info(message)
        return json.dumps(
            {
                "status": "ok",
                "message": message,
                "orders": {"files_found": 0, "files_loaded": 0, "rows_loaded": 0, "outputs": []},
                "cmg": {"files_found": 0, "files_loaded": 0, "rows_loaded": 0, "outputs": []},
                "options": {"files_found": 0, "files_loaded": 0, "rows_loaded": 0, "outputs": []},
            },
            ensure_ascii=False,
        )

    order_result = _load_orders(order_files)
    cmg_result = _load_csv_dataset(
        cmg_files,
        CMG_DIR,
        "cmg",
        lambda row, _path: _ym_from_iso(row.get("조회일자", "")),
        ["매장명", "조회일자"],
    )
    options_result = _load_csv_dataset(
        options_files,
        OPTIONS_DIR,
        "options",
        lambda row, path: _ym_from_filename(path),
        ["매장명", "옵션그룹", "옵션명", "적용메뉴"],
    )

    loaded_files = []
    loaded_files.extend(order_result["loaded_files"])
    loaded_files.extend(cmg_result["loaded_files"])
    loaded_files.extend(options_result["loaded_files"])
    _cleanup_sources(loaded_files)

    return json.dumps(
        {
            "status": "ok",
            "orders": {
                "files_found": order_result["files_found"],
                "files_loaded": order_result["files_loaded"],
                "rows_loaded": order_result["rows_loaded"],
                "outputs": order_result["outputs"],
            },
            "cmg": {
                "files_found": cmg_result["files_found"],
                "files_loaded": cmg_result["files_loaded"],
                "rows_loaded": cmg_result["rows_loaded"],
                "outputs": cmg_result["outputs"],
            },
            "options": {
                "files_found": options_result["files_found"],
                "files_loaded": options_result["files_loaded"],
                "rows_loaded": options_result["rows_loaded"],
                "outputs": options_result["outputs"],
            },
            "cleaned_count": len(loaded_files),
            "cleaned_files": [str(item["path"]) for item in loaded_files],
        },
        ensure_ascii=False,
    )
