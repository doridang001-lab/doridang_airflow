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
ORDER_DEDUP_COLUMNS = [
    "order_date",
    "order_id",
    "delivery_type",
    "order_status",
    "order_summary",
    "total_price",
    "is_cancelled",
    "menu_name",
    "menu_qty",
    "menu_price",
    "menu_options",
]


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


def _deduplicate_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """동일 주문/메뉴옵션 행은 원본 저장 단계에서 1건만 남긴다."""
    if df.empty:
        return df, 0

    before = len(df)
    subset = [col for col in ORDER_DEDUP_COLUMNS if col in df.columns]
    if subset:
        out = df.drop_duplicates(subset=subset, keep="last").copy()
    elif "_row_hash" in df.columns:
        out = df.drop_duplicates(subset=["_row_hash"], keep="last").copy()
    else:
        out = df.drop_duplicates(keep="last").copy()
    return out.reset_index(drop=True), before - len(out)


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
        if out_path.exists():
            try:
                existing = pd.read_parquet(out_path)
            except Exception as exc:
                logger.error("failed to read existing orders parquet %s: %s", out_path, exc)
                existing = pd.DataFrame()
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df

        combined, dropped = _deduplicate_orders(combined)
        if dropped:
            logger.warning("dropped duplicate coupang order rows: %s (%d rows)", out_path, dropped)

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


def repair_coupang_orders_duplicates() -> str:
    """기존 쿠팡 orders parquet의 중복 행을 저장 기준과 동일하게 정리한다."""
    files = sorted(COUPANG_ORDERS_DB.glob("brand=*/store=*/ym=*/orders_*.parquet"))
    if not files:
        return f"쿠팡 orders parquet 없음 | {COUPANG_ORDERS_DB}"

    changed = 0
    skipped = 0
    total_before = 0
    total_after = 0
    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            skipped += 1
            logger.warning("쿠팡 orders parquet 로드 실패, 스킵: %s | %s", path, exc)
            continue

        fixed, dropped = _deduplicate_orders(df)
        total_before += len(df)
        total_after += len(fixed)
        if dropped <= 0:
            continue

        fixed.astype(str).to_parquet(path, index=False)
        changed += 1
        logger.warning("쿠팡 orders 중복 정리: %s 제거=%d", path, dropped)

    return (
        f"쿠팡 orders 중복 정리 완료 | 파일={changed}/{len(files)} "
        f"스킵={skipped} 행={total_before}->{total_after} 제거={total_before - total_after}"
    )


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
