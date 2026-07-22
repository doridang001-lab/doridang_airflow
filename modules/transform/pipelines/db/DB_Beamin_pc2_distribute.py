"""Baemin macro inbox files distributor."""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from modules.transform.pipelines.db.beamin_store_io import read_file, read_table, write_table
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB

logger = logging.getLogger(__name__)

INBOX_DIR = COLLECT_DB / "영업관리부_수집" / "_baemin_pc2_inbox"
UPLOAD_INBOX_DIR = COLLECT_DB / "영업관리부_수집" / "_baemin_upload_inbox"
ORDER_KEY = "주문번호"
SUPPORTED_SUFFIXES = {".parquet", ".csv"}
ROOT_PART = "baemin_macro"
FAILED_KEYS = ("accounts", "stores", "orders", "ads")


def ingest_baemin_pc2_inbox(**context) -> str:
    """PC2 inbox의 manual__* 폴더를 analytics baemin_macro 파티션으로 적재한다."""
    return ingest_inbox(INBOX_DIR)["summary"]


def ingest_baemin_upload_inbox(**context) -> dict[str, Any]:
    """Upload inbox의 manual__* 폴더를 적재하고 폴더 meta를 함께 반환한다."""
    return ingest_inbox(UPLOAD_INBOX_DIR, read_meta=True)


def ingest_inbox(inbox_dir: Path, read_meta: bool = False) -> dict[str, Any]:
    label = inbox_dir.name
    if not inbox_dir.exists():
        logger.info("%s 없음: %s", label, inbox_dir)
        return {
            "summary": f"{label} 없음: 스킵",
            "meta": _empty_meta() if read_meta else {},
            "stats": _empty_ingest_stats(),
        }

    folders = sorted(p for p in inbox_dir.glob("manual__*") if p.is_dir())
    if not folders:
        logger.info("%s 처리 대상 없음: %s", label, inbox_dir)
        return {
            "summary": f"{label} 처리 대상 없음: 스킵",
            "meta": _empty_meta() if read_meta else {},
            "stats": _empty_ingest_stats(),
        }
    if read_meta:
        folder_meta_by_folder = {folder: _read_meta(folder) for folder in folders}
        target_dates = sorted(
            str(meta.get("target_date"))
            for meta in folder_meta_by_folder.values()
            if meta.get("target_date")
        )
        representative_date = target_dates[-1] if target_dates else None
        logger.info("%s 대표 target_date=%s (폴더 %d개 중)", label, representative_date, len(folders))
    else:
        folder_meta_by_folder = {}
        representative_date = None

    processed_folders = 0
    cleaned_folders = 0
    skipped_folders = 0
    failed_folders = 0
    processed_files = 0
    processed_rows = 0
    subtype_counts: dict[str, int] = {}
    meta_items: list[dict[str, Any]] = []

    for folder in folders:
        processed_folders += 1
        files = _target_files(folder)
        folder_files = 0
        folder_rows = 0
        folder_counts: dict[str, int] = {}
        logger.info("%s 폴더 처리 시작: %s | 대상파일=%d", label, folder, len(files))
        if not files:
            skipped_folders += 1
            logger.warning("%s 대상 파일 없음, cleanup 보류: %s", label, folder)
            continue

        try:
            folder_meta = folder_meta_by_folder.get(folder, {}) if read_meta else {}
            for src_file in files:
                subtype, rows = _distribute_one_file(folder, src_file)
                folder_files += 1
                folder_rows += rows
                folder_counts[subtype] = folder_counts.get(subtype, 0) + rows
                logger.info(
                    "%s 파일 분배 완료: folder=%s file=%s subtype=%s rows=%d",
                    label,
                    folder.name,
                    src_file.relative_to(folder),
                    subtype,
                    rows,
                )
        except Exception:
            failed_folders += 1
            logger.exception("%s 폴더 처리 실패, cleanup 보류: %s", label, folder)
            continue

        _cleanup_processed_folder(folder, inbox_dir)
        cleaned_folders += 1
        processed_files += folder_files
        processed_rows += folder_rows
        if read_meta:
            folder_date = str(folder_meta.get("target_date")) if folder_meta.get("target_date") else None
            if representative_date is None or folder_date == representative_date:
                meta_items.append(folder_meta)
            else:
                logger.warning(
                    "%s 지난 날짜 meta는 downstream 검증에서 제외: folder=%s target_date=%s (대표날짜=%s)",
                    label,
                    folder.name,
                    folder_date,
                    representative_date,
                )
        for subtype, rows in folder_counts.items():
            subtype_counts[subtype] = subtype_counts.get(subtype, 0) + rows
        logger.info("%s 폴더 cleanup 완료: %s | files=%d rows=%d", label, folder, folder_files, folder_rows)

    meta = _merge_meta(meta_items) if read_meta else {}
    subtype_summary = ", ".join(f"{k}={v}" for k, v in sorted(subtype_counts.items())) or "없음"
    summary = (
        f"{label} 적재 완료 | "
        f"folders={processed_folders} cleaned={cleaned_folders} skipped={skipped_folders} failed={failed_folders} "
        f"files={processed_files} rows={processed_rows} subtypes={subtype_summary}"
    )
    logger.info(summary)
    return {
        "summary": summary,
        "meta": meta,
        "stats": {
            "folders": processed_folders,
            "cleaned": cleaned_folders,
            "skipped": skipped_folders,
            "failed": failed_folders,
            "files": processed_files,
            "rows": processed_rows,
            "subtypes": dict(sorted(subtype_counts.items())),
        },
    }


def _empty_ingest_stats() -> dict[str, Any]:
    return {
        "folders": 0,
        "cleaned": 0,
        "skipped": 0,
        "failed": 0,
        "files": 0,
        "rows": 0,
        "subtypes": {},
    }


def _empty_meta() -> dict[str, Any]:
    return {
        "target_date": None,
        "account_list": [],
        "validation": [],
        "ad_stores": [],
        "store_info_per_account": [],
        "original_failed": _empty_failed(),
        "failed": _empty_failed(),
    }


def _empty_failed() -> dict[str, list]:
    return {key: [] for key in FAILED_KEYS}


def _read_meta(folder: Path) -> dict[str, Any]:
    meta_path = folder / "_meta.json"
    if not meta_path.exists():
        logger.warning("upload inbox meta 없음: %s", meta_path)
        return {}
    return json.loads(meta_path.read_text(encoding="utf-8"))


def _merge_meta(items: list[dict[str, Any]]) -> dict[str, Any]:
    merged = _empty_meta()
    target_dates = sorted({str(item.get("target_date")) for item in items if item.get("target_date")})
    if len(target_dates) > 1:
        raise ValueError(f"upload inbox target_date 혼재: {target_dates}")
    if target_dates:
        merged["target_date"] = target_dates[0]

    seen_ad_stores: set[str] = set()
    for item in items:
        for key in ("account_list", "validation", "store_info_per_account"):
            values = item.get(key) or []
            if isinstance(values, list):
                merged[key].extend(values)
        for ad_store in item.get("ad_stores") or []:
            marker = json.dumps(ad_store, ensure_ascii=False, sort_keys=True, default=str)
            if marker in seen_ad_stores:
                continue
            seen_ad_stores.add(marker)
            merged["ad_stores"].append(ad_store)
        for failed_key in ("original_failed", "failed"):
            failed = item.get(failed_key) or {}
            for key in FAILED_KEYS:
                values = failed.get(key) or []
                if isinstance(values, list):
                    merged[failed_key][key].extend(values)
    return merged


def _target_files(folder: Path) -> list[Path]:
    return sorted(
        p
        for p in folder.rglob("*")
        if p.is_file()
        and p.suffix.lower() in SUPPORTED_SUFFIXES
        and _is_baemin_macro_relpath(folder, p)
    )


def _is_baemin_macro_relpath(folder: Path, src_file: Path) -> bool:
    try:
        rel = src_file.relative_to(folder)
    except ValueError:
        return False
    return len(rel.parts) >= 3 and rel.parts[0] == ROOT_PART


def _distribute_one_file(folder: Path, src_file: Path) -> tuple[str, int]:
    rel = src_file.relative_to(folder)
    if len(rel.parts) < 3 or rel.parts[0] != ROOT_PART:
        raise ValueError(f"inbox 대상 경로 아님: {src_file}")

    subtype = rel.parts[1]
    dst_stem = (ANALYTICS_DB / rel).with_suffix("")
    new_df = read_file(src_file)
    if new_df.empty:
        raise ValueError(f"inbox 빈 파일: {src_file}")

    if subtype == "orders":
        if ORDER_KEY in new_df.columns:
            combined = _upsert_orders(dst_stem, new_df)
        else:
            combined = new_df
        out_path = write_table(combined, dst_stem)
    else:
        combined = new_df
        out_path = _write_csv_table(combined, dst_stem)

    logger.info("inbox write: %s -> %s | rows=%d", src_file, out_path, len(combined))
    return subtype, len(new_df)


def _write_csv_table(df: pd.DataFrame, stem_path: Path) -> Path:
    stem_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path = stem_path.with_suffix(".csv")
    df.fillna("").astype(str).to_csv(csv_path, index=False, encoding="utf-8-sig")
    pq_path = stem_path.with_suffix(".parquet")
    if pq_path.exists():
        try:
            pq_path.unlink()
        except OSError as exc:
            logger.warning("비주문 parquet 삭제 실패: %s / %s", pq_path, exc)
    return csv_path


def _upsert_orders(dst_stem: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    existing = read_table(dst_stem)
    if existing is None or ORDER_KEY not in existing.columns:
        return new_df

    new_ids = set(new_df[ORDER_KEY].fillna("").astype(str).unique())
    if "" in new_ids:
        logger.warning("orders 주문번호 빈 값 포함: %s", dst_stem)
    existing = existing[~existing[ORDER_KEY].fillna("").astype(str).isin(new_ids)]
    return pd.concat([existing, new_df], ignore_index=True)


def _cleanup_processed_folder(folder: Path, inbox_dir: Path) -> None:
    """성공 처리된 manual 폴더만 삭제한다."""
    folder = folder.resolve()
    inbox = inbox_dir.resolve()
    if folder.parent != inbox:
        raise ValueError(f"inbox cleanup 대상 부모 경로 불일치: {folder}")
    if not folder.name.startswith("manual__"):
        raise ValueError(f"inbox cleanup 대상 폴더명 아님: {folder}")
    if not folder.is_dir():
        raise ValueError(f"inbox cleanup 대상 폴더 없음: {folder}")
    shutil.rmtree(folder)
