"""Load Coupang macro raw CSV files into analytics partition storage."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import time
from collections.abc import Callable
from contextlib import contextmanager
from glob import glob
from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    record_manual_reingest_marker,
)
from modules.transform.pipelines.db.beamin_store_io import replace_covered_date_range
from modules.transform.utility.paths import (
    COLLECT_DB,
    COUPANG_ORDERS_DB,
    COUPANG_ORDERS_DETAIL_DB,
    DOWN_DIR,
    TEMP_DIR,
)
from modules.transform.utility.store_normalize import lookup_store_key

logger = logging.getLogger(__name__)

KNOWN_BRANDS = ["도리당", "나홀로"]
CMG_DIR = COUPANG_ORDERS_DETAIL_DB / "cmg"
OPTIONS_DIR = COUPANG_ORDERS_DETAIL_DB / "options"
COLLECT_SRC = COLLECT_DB / "영업관리부_수집"
MISPLACED_MARKETING_SRC = COLLECT_DB / "마케팅_수집"
ARCHIVE_DIR = COLLECT_SRC / "_archived"
COUPANG_RAW_PREFIXES = ("orders", "cmg", "options")
ORDER_DEDUP_COLUMNS = [
    "order_date",
    "order_id",
    "delivery_type",
    "order_status",
    "order_summary",
    "total_price",
    "is_cancelled",
    "item_menu",
    "menu_name",
    "menu_qty",
    "menu_price",
    "menu_options",
]
ORDER_DEDUP_NUMERIC_COLUMNS = {"total_price", "menu_qty", "menu_price"}
ORDER_INTERNAL_COLUMNS = ["_ingest_source_path", "_ingest_source"]
COUPANG_LOAD_LOCK_DIR = TEMP_DIR / "locks" / "coupang_macro_load.lock"
COUPANG_EXTRA_DOWNLOAD_DIRS_ENV = "COUPANG_EXTRA_DOWNLOAD_DIRS"


def _remove_stale_coupang_lock(lock_dir: Path) -> bool:
    """Remove only our known stale lock shape."""
    if not lock_dir.exists():
        return True
    try:
        children = list(lock_dir.iterdir())
    except OSError:
        return False

    for child in children:
        if child.name != "owner.json" or not child.is_file():
            return False
    for child in children:
        child.unlink(missing_ok=True)
    try:
        lock_dir.rmdir()
        return True
    except OSError:
        return False


@contextmanager
def _coupang_load_lock(
    timeout_sec: float = 900,
    stale_sec: float = 7200,
    wait_interval_sec: float = 5,
):
    """Serialize Coupang source ingestion across DAGs that share the same loader."""
    lock_dir = COUPANG_LOAD_LOCK_DIR
    owner_path = lock_dir / "owner.json"
    lock_dir.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    acquired = False

    while True:
        try:
            lock_dir.mkdir()
            owner_path.write_text(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "created_at": pd.Timestamp.now(tz="Asia/Seoul").isoformat(),
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            acquired = True
            logger.info("쿠팡 원천 적재 lock 획득: %s", lock_dir)
            break
        except FileExistsError:
            try:
                age = time.time() - lock_dir.stat().st_mtime
            except OSError:
                age = 0
            if stale_sec > 0 and age > stale_sec and _remove_stale_coupang_lock(lock_dir):
                logger.warning("stale 쿠팡 원천 적재 lock 제거: %s", lock_dir)
                continue
            if time.monotonic() - started >= timeout_sec:
                raise TimeoutError(f"쿠팡 원천 적재 lock 대기 시간 초과: {lock_dir}")
            time.sleep(wait_interval_sec)

    try:
        yield
    finally:
        if acquired:
            try:
                owner_path.unlink(missing_ok=True)
                lock_dir.rmdir()
                logger.info("쿠팡 원천 적재 lock 해제: %s", lock_dir)
            except OSError as exc:
                logger.warning("쿠팡 원천 적재 lock 해제 실패: %s | %s", lock_dir, exc)


def _path_key(path: Path) -> str:
    try:
        return str(path.resolve())
    except OSError:
        return str(path.absolute())


def _extra_download_dirs() -> list[Path]:
    dirs: list[Path] = []
    raw = os.getenv(COUPANG_EXTRA_DOWNLOAD_DIRS_ENV, "").strip()
    if raw:
        dirs.extend(Path(part.strip()) for part in raw.split(";") if part.strip())

    if os.name == "nt":
        dirs.append(Path.home() / "Downloads")
    else:
        dirs.append(Path("/opt/airflow/user_downloads"))

    out: list[Path] = []
    seen: set[str] = set()
    for path in dirs:
        key = _path_key(path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def _raw_source_dirs(include_collect: bool = True) -> list[tuple[str, Path]]:
    dirs: list[tuple[str, Path]] = [("down", DOWN_DIR)]
    if include_collect:
        dirs.append(("collect", COLLECT_SRC))

    known = {_path_key(path) for _, path in dirs}
    extra_idx = 1
    for path in _extra_download_dirs():
        key = _path_key(path)
        if key in known:
            continue
        known.add(key)
        source = "downloads" if extra_idx == 1 else f"downloads_{extra_idx}"
        dirs.append((source, path))
        extra_idx += 1
    return dirs


def _iter_source_files(prefix: str) -> list[dict[str, Path]]:
    """Collect files from raw download dirs and `Collect_Data/...`, with source tags."""
    pattern = f"coupangeats_{prefix}_*.csv"

    items: list[dict[str, Path]] = []
    seen: set[str] = set()
    for source, source_dir in _raw_source_dirs(include_collect=True):
        for item in sorted(glob(str(source_dir / pattern))):
            path = Path(item)
            key = _path_key(path)
            if key in seen:
                continue
            seen.add(key)
            items.append({"path": path, "source": source})

    return items


def _collect_dest_path(src: Path) -> Path:
    dest = COLLECT_SRC / src.name
    if not dest.exists():
        return dest

    stem = src.stem
    suffix = src.suffix
    idx = 1
    while True:
        candidate = COLLECT_SRC / f"{stem}.{idx}{suffix}"
        if not candidate.exists():
            return candidate
        idx += 1


def move_misplaced_coupang_marketing_to_collect() -> list[dict[str, str]]:
    """Move Coupang raw CSVs accidentally saved in marketing collection dir."""
    try:
        if _path_key(MISPLACED_MARKETING_SRC) == _path_key(COLLECT_SRC):
            return []
    except OSError:
        return []

    if not MISPLACED_MARKETING_SRC.exists():
        return []

    COLLECT_SRC.mkdir(parents=True, exist_ok=True)
    moved: list[dict[str, str]] = []
    for prefix in COUPANG_RAW_PREFIXES:
        pattern = f"coupangeats_{prefix}_*.csv"
        for src in sorted(MISPLACED_MARKETING_SRC.glob(pattern)):
            if not src.is_file():
                continue
            dest = _collect_dest_path(src)
            try:
                shutil.move(str(src), str(dest))
                moved.append({"source": str(src), "dest": str(dest)})
                logger.warning("misplaced coupang csv moved to collect: %s -> %s", src, dest)
            except Exception as exc:
                logger.warning("failed to move misplaced coupang csv %s: %s", src, exc)
    return moved


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


def _partition_value(path: Path, prefix: str) -> str:
    token = f"{prefix}="
    for part in path.parts:
        if part.startswith(token):
            return part[len(token):].strip()
    return ""


def _is_canonical_orders_file(path: Path) -> bool:
    return bool(re.fullmatch(r"orders_\d{4}-\d{2}\.parquet", path.name))


def _coupang_sale_date(series: pd.Series) -> pd.Series:
    matched = series.fillna("").astype(str).str.extract(r"(\d{4})\.(\d{2})\.(\d{2})")
    return (matched[0] + "-" + matched[1] + "-" + matched[2]).fillna("")


def _deduplicate_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """동일 주문/메뉴옵션 행은 원본 저장 단계에서 1건만 남긴다."""
    if df.empty:
        return df, 0

    before = len(df)
    subset = [col for col in ORDER_DEDUP_COLUMNS if col in df.columns]
    if subset:
        key = _normalized_dedup_key(df, subset)
        out = df[~key.duplicated(keep="last")].copy()
    elif "_row_hash" in df.columns:
        out = df.drop_duplicates(subset=["_row_hash"], keep="last").copy()
    else:
        out = df.drop_duplicates(keep="last").copy()
    return out.reset_index(drop=True), before - len(out)


def _ensure_item_menu_column(df: pd.DataFrame) -> pd.DataFrame:
    """과거 쿠팡 orders 입력은 item_menu가 없으므로 menu_name으로 보완한다."""
    if df.empty:
        return df
    out = df.copy()
    if "item_menu" not in out.columns:
        out["item_menu"] = ""
    if "menu_name" not in out.columns:
        out["menu_name"] = ""
    item_menu = out["item_menu"].fillna("").astype(str).str.strip()
    menu_name = out["menu_name"].fillna("").astype(str).str.strip()
    out["item_menu"] = item_menu.mask(item_menu.eq("") | item_menu.eq("nan"), menu_name)
    return out


def _normalized_dedup_key(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    key = pd.DataFrame(index=df.index)
    for col in columns:
        if col not in df.columns:
            key[col] = ""
            continue
        values = df[col]
        if col in ORDER_DEDUP_NUMERIC_COLUMNS:
            key[col] = pd.to_numeric(
                values.astype(str).str.replace(",", "", regex=False).str.strip(),
                errors="coerce",
            ).astype("Float64").astype(str)
        else:
            key[col] = values.fillna("").astype(str).str.strip()
            key[col] = key[col].replace({"nan": "", "None": "", "<NA>": ""})
    return key


def _record_reingest_dates(store: str, new_df: pd.DataFrame, info: dict) -> None:
    row_dates = (
        _coupang_sale_date(new_df["order_date"]).reset_index(drop=True)
        if "order_date" in new_df.columns
        else pd.Series(dtype=str)
    )
    for date in info.get("covered_dates", []):
        record_manual_reingest_marker(
            "쿠팡수동",
            store,
            date,
            {
                "rows": int(row_dates.eq(date).sum()),
                "removed": int(info.get("removed", 0)),
            },
        )


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
        if source in {"down", "collect"} or source.startswith("downloads"):
            try:
                path.unlink()
                logger.info("deleted from %s: %s", source, path)
            except Exception as exc:  # pragma: no cover - 운영 환경 처리
                logger.warning("failed to delete %s: %s", path, exc)
        else:
            logger.warning("unknown source tag for %s: %s", path, source)


def _load_orders(files: list[dict[str, Path]]) -> dict:
    loaded_files: list[dict[str, Path]] = []
    outputs: list[str] = []
    blocked_outputs: list[dict] = []
    blocked_source_paths: set[str] = set()
    append_only_outputs: list[dict] = []
    grouped_rows: dict[tuple[str, str, str], list[dict]] = {}
    total_rows = 0
    blocked_rows = 0

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
            row_dict["_ingest_source_path"] = str(path)
            row_dict["_ingest_source"] = str(source)
            grouped_rows.setdefault((brand, store, ym), []).append(row_dict)
            file_rows += 1

        if file_rows > 0:
            loaded_files.append({"path": path, "source": source})
            total_rows += file_rows

    for (brand, store, ym), rows in grouped_rows.items():
        out_dir = _orders_partition_dir(brand, store, ym)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"orders_{ym}.parquet"

        raw_new_df = pd.DataFrame(rows).fillna("").astype(str)
        source_paths = set(raw_new_df.get("_ingest_source_path", pd.Series(dtype=str)).tolist())
        new_df = raw_new_df.drop(columns=ORDER_INTERNAL_COLUMNS, errors="ignore")
        new_df = _ensure_item_menu_column(new_df)
        if out_path.exists():
            try:
                existing = _ensure_item_menu_column(pd.read_parquet(out_path))
            except Exception as exc:
                logger.error("failed to read existing orders parquet %s: %s", out_path, exc)
                existing = pd.DataFrame()
        else:
            existing = pd.DataFrame()

        new_dates = _coupang_sale_date(new_df["order_date"])
        existing_dates = (
            _coupang_sale_date(existing["order_date"])
            if not existing.empty and "order_date" in existing.columns
            else pd.Series(dtype=str)
        )
        combined, info = replace_covered_date_range(
            existing,
            new_df,
            existing_dates,
            new_dates,
            order_key="order_id",
        )
        shrunk_dates = sorted(
            set(info.get("shrunk_dates", []))
            | set(info.get("partial_shrunk_dates", []))
        )
        if shrunk_dates:
            logger.warning(
                "coupang reingest order counts shrank; preserving existing rows and "
                "appending only non-duplicates: %s/%s ym=%s "
                "dates=%s details=%s sources=%s",
                brand,
                store,
                ym,
                shrunk_dates,
                info.get("shrink_details", {}),
                sorted(source_paths),
            )
            existing_for_append = existing.reset_index(drop=True)
            existing_date_values = (
                existing_dates.fillna("").astype(str).str.strip().reset_index(drop=True)
            )
            preserved_existing = existing_for_append.loc[
                existing_date_values.isin(shrunk_dates)
            ].copy()
            combined = pd.concat([combined, preserved_existing], ignore_index=True)
            append_only_outputs.append(
                {
                    "path": str(out_path),
                    "brand": brand,
                    "store": store,
                    "ym": ym,
                    "dates": shrunk_dates,
                    "details": info.get("shrink_details", {}),
                    "sources": sorted(source_paths),
                }
            )
            info = {
                **info,
                "removed": 0,
                "append_only": True,
                "append_only_dates": shrunk_dates,
            }

        combined = _ensure_item_menu_column(combined)
        combined, dropped = _deduplicate_orders(combined)
        if dropped:
            logger.warning("dropped duplicate coupang order rows: %s (%d rows)", out_path, dropped)

        combined = combined.astype(str)
        combined.to_parquet(out_path, index=False)
        _record_reingest_dates(store, new_df, info)
        outputs.append(str(out_path))
        logger.info("saved orders parquet: %s (%d rows)", out_path, len(combined))

    if blocked_source_paths:
        loaded_files = [
            item
            for item in loaded_files
            if str(Path(item["path"])) not in blocked_source_paths
        ]

    return {
        "files_found": len(files),
        "files_loaded": len(loaded_files),
        "rows_loaded": max(0, total_rows - blocked_rows),
        "outputs": outputs,
        "loaded_files": loaded_files,
        "blocked_files": sorted(blocked_source_paths),
        "blocked_outputs": blocked_outputs,
        "rows_blocked": blocked_rows,
        "append_only_outputs": append_only_outputs,
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
    files = sorted(
        path
        for path in COUPANG_ORDERS_DB.glob("brand=*/store=*/ym=*/orders_*.parquet")
        if _is_canonical_orders_file(path)
    )
    if not files:
        return f"쿠팡 orders parquet 없음 | {COUPANG_ORDERS_DB}"

    skipped = 0
    frames: list[pd.DataFrame] = []
    originals: dict[str, pd.DataFrame] = {}
    for path in files:
        try:
            df = _ensure_item_menu_column(pd.read_parquet(path))
        except Exception as exc:
            skipped += 1
            logger.warning("쿠팡 orders parquet 로드 실패, 스킵: %s | %s", path, exc)
            continue
        originals[str(path)] = df
        if df.empty:
            continue
        brand = _partition_value(path, "brand")
        store = _partition_value(path, "store")
        work = df.copy()
        work["_path"] = str(path)
        work["_rel"] = str(path.relative_to(COUPANG_ORDERS_DB))
        work["_row"] = range(len(work))
        work["_brand"] = brand
        work["_partition_store"] = store
        work["_norm_store"] = (lookup_store_key(brand, store) or store).replace(" ", "")
        work["_canonical_store_partition"] = (
            work["_partition_store"].astype(str).str.replace(r"\s+", "", regex=True).eq(
                work["_norm_store"]
            )
        )
        frames.append(work)

    total_before = sum(len(df) for df in originals.values())
    if not frames:
        return (
            f"쿠팡 orders 중복 정리 완료 | 파일=0/{len(files)} "
            f"스킵={skipped} 행={total_before}->{total_before} 제거=0"
        )

    all_rows = pd.concat(frames, ignore_index=True)
    key_columns = ["_brand", "_norm_store", *ORDER_DEDUP_COLUMNS]
    key = _normalized_dedup_key(all_rows, key_columns)
    rank = pd.DataFrame(
        {
            "_canonical_store_partition": all_rows["_canonical_store_partition"].astype(int),
            "_rel": all_rows["_rel"].astype(str),
            "_row": all_rows["_row"].astype(int),
        },
        index=all_rows.index,
    )
    ordered = rank.sort_values(
        ["_canonical_store_partition", "_rel", "_row"],
        kind="stable",
    ).index
    keep_ordered = ~key.loc[ordered].duplicated(keep="last")
    keep = pd.Series(False, index=all_rows.index)
    keep.loc[ordered[keep_ordered.to_numpy()]] = True
    remove = all_rows[~keep]

    changed = 0
    total_after = total_before - len(remove)
    if remove.empty:
        return (
            f"쿠팡 orders 중복 정리 완료 | 파일=0/{len(files)} "
            f"스킵={skipped} 행={total_before}->{total_after} 제거=0"
        )

    for path_text, group in remove.groupby("_path"):
        path = Path(path_text)
        df = originals[path_text]
        drop_rows = set(group["_row"].astype(int).tolist())
        fixed = df.loc[[idx for idx in range(len(df)) if idx not in drop_rows]].reset_index(
            drop=True
        )
        if fixed.empty:
            path.unlink(missing_ok=True)
        else:
            fixed.astype(str).to_parquet(path, index=False)
        changed += 1
        logger.warning("쿠팡 orders 중복 정리: %s 제거=%d", path, len(drop_rows))

    return (
        f"쿠팡 orders 중복 정리 완료 | 파일={changed}/{len(files)} "
        f"스킵={skipped} 행={total_before}->{total_after} 제거={total_before - total_after}"
    )


def move_coupang_down_to_collect() -> str:
    """Move coupang CSV files from download dirs → COLLECT_SRC (영업관리부_수집).

    DB_CollectionCompare_Dags에서만 호출. 적재는 DB_CoupangMacro_Load_Dags가 담당.
    """
    COLLECT_SRC.mkdir(parents=True, exist_ok=True)
    moved = 0
    for prefix in COUPANG_RAW_PREFIXES:
        pattern = f"coupangeats_{prefix}_*.csv"
        for _, source_dir in _raw_source_dirs(include_collect=False):
            for item in sorted(glob(str(source_dir / pattern))):
                src = Path(item)
                dest = _collect_dest_path(src)
                try:
                    shutil.move(str(src), str(dest))
                    logger.info("moved to collect: %s -> %s", src.name, dest)
                    moved += 1
                except Exception as exc:
                    logger.warning("failed to move %s: %s", src, exc)
    return f"이동 완료: {moved}개"


def load_coupang_macro_partition() -> str:
    """Load Coupang macro raw CSV files from configured source dirs and clean sources."""
    with _coupang_load_lock():
        return _load_coupang_macro_partition_unlocked()


def _load_coupang_macro_partition_unlocked() -> str:
    misplaced_moved = move_misplaced_coupang_marketing_to_collect()
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
                "misplaced_moved_count": len(misplaced_moved),
                "misplaced_moved_files": misplaced_moved,
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

    if order_result.get("blocked_outputs"):
        raise RuntimeError(
            "쿠팡 orders 재적재 차단: 기존 주문 수보다 적은 원천 파일이 감지되었습니다. "
            + json.dumps(
                {
                    "blocked_outputs": order_result.get("blocked_outputs", []),
                    "blocked_files": order_result.get("blocked_files", []),
                    "rows_blocked": order_result.get("rows_blocked", 0),
                },
                ensure_ascii=False,
            )
        )

    return json.dumps(
        {
            "status": "ok",
            "orders": {
                "files_found": order_result["files_found"],
                "files_loaded": order_result["files_loaded"],
                "rows_loaded": order_result["rows_loaded"],
                "outputs": order_result["outputs"],
                "blocked_outputs": order_result.get("blocked_outputs", []),
                "blocked_files": order_result.get("blocked_files", []),
                "rows_blocked": order_result.get("rows_blocked", 0),
                "append_only_outputs": order_result.get("append_only_outputs", []),
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
            "misplaced_moved_count": len(misplaced_moved),
            "misplaced_moved_files": misplaced_moved,
        },
        ensure_ascii=False,
    )
