"""Ingest Baemin PC2 inbox runs into analytics baemin_macro partitions."""

from __future__ import annotations

import json
import logging
import shutil
from collections import Counter
from pathlib import Path

import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_Beamin_03_shop_change import CSV_COLUMNS, _row_signature
from modules.transform.pipelines.db.beamin_store_io import read_file, read_table, write_table
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB

logger = logging.getLogger(__name__)
KST = pendulum.timezone("Asia/Seoul")

INBOX = COLLECT_DB / "영업관리부_수집" / "_baemin_pc2_inbox"
PROCESSING = INBOX / "_processing"
LOCK_DIR = INBOX / "_lock"
MIN_AGE_MINUTES = 30

_CSV_DATASETS = {
    "metrics_now": ("baemin_now.csv", ["date"]),
    "metrics_our_store_clicks": ("woori_shop_click.csv", None),
    "shop_operation": ("shop_operation.csv", ["수집일시"]),
    "monthly_operation": ("monthly_operation.csv", None),
    "ad_funnel": ("baemin_ad_funnel.csv", ["target_date"]),
}


def _as_str_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.fillna("").astype(str)


def _acquire_lock() -> bool:
    INBOX.mkdir(parents=True, exist_ok=True)
    try:
        LOCK_DIR.mkdir()
    except FileExistsError:
        return False
    payload = {"locked_at": pendulum.now(KST).to_iso8601_string()}
    (LOCK_DIR / "lock.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return True


def _release_lock() -> None:
    try:
        lock_file = LOCK_DIR / "lock.json"
        if lock_file.exists():
            lock_file.unlink()
        LOCK_DIR.rmdir()
    except OSError as exc:
        logger.warning("PC2 inbox lock 해제 실패(무시): %s", exc)


def _load_manifest(run_dir: Path) -> dict[str, int] | None:
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        logger.info("PC2 inbox run skip: manifest 없음 %s", run_dir)
        return None
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("PC2 inbox run skip: manifest 읽기 실패 %s (%s)", run_dir, exc)
        return None
    if not isinstance(data, dict):
        logger.warning("PC2 inbox run skip: manifest 형식 오류 %s", run_dir)
        return None
    return {str(k): int(v) for k, v in data.items()}


def _manifest_ready(run_dir: Path, manifest: dict[str, int]) -> bool:
    newest_mtime = 0.0
    for rel, expected_size in manifest.items():
        path = run_dir / rel
        if not path.exists() or not path.is_file():
            logger.info("PC2 inbox run skip: 파일 미동기화 %s", path)
            return False
        actual_size = path.stat().st_size
        if actual_size != expected_size:
            logger.info(
                "PC2 inbox run skip: size 불일치 %s expected=%s actual=%s",
                path,
                expected_size,
                actual_size,
            )
            return False
        newest_mtime = max(newest_mtime, path.stat().st_mtime)

    if newest_mtime:
        newest = pendulum.from_timestamp(newest_mtime, tz=KST)
        age_minutes = (pendulum.now(KST) - newest).total_seconds() / 60
        if age_minutes < MIN_AGE_MINUTES:
            logger.info(
                "PC2 inbox run skip: mtime %.1f분 경과, 최소 %d분 필요 %s",
                age_minutes,
                MIN_AGE_MINUTES,
                run_dir,
            )
            return False
    return True


def _candidate_runs() -> list[Path]:
    if not INBOX.exists():
        return []
    return sorted(
        p
        for p in INBOX.iterdir()
        if p.is_dir() and not p.name.startswith("_") and p.name != PROCESSING.name
    )


def _analytics_path(incoming: Path, baemin_root: Path) -> Path:
    rel = incoming.relative_to(baemin_root)
    return ANALYTICS_DB / "baemin_macro" / rel


def _merge_csv(incoming: Path, target: Path, key: list[str] | None) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    incoming_df = _as_str_df(read_file(incoming))
    if key is None:
        combined = incoming_df
    elif target.exists():
        existing = _as_str_df(pd.read_csv(target, dtype=str, encoding="utf-8-sig"))
        for col in incoming_df.columns:
            if col not in existing.columns:
                existing[col] = ""
        for col in existing.columns:
            if col not in incoming_df.columns:
                incoming_df[col] = ""
        combined = pd.concat([existing[incoming_df.columns], incoming_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=key, keep="last")
    else:
        combined = incoming_df
    combined.to_csv(target, index=False, encoding="utf-8-sig")
    return len(incoming_df)


def _merge_orders(incoming: Path, target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    stem = target.with_suffix("")
    incoming_df = _as_str_df(read_file(incoming))
    existing = read_table(stem)
    if existing is not None and "주문번호" in incoming_df.columns:
        existing = _as_str_df(existing)
        existing = existing[~existing["주문번호"].isin(set(incoming_df["주문번호"]))]
        combined = pd.concat([existing, incoming_df], ignore_index=True)
    else:
        combined = incoming_df
    write_table(combined, stem)
    return len(incoming_df)


def _merge_shop_change(incoming: Path, target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    stem = target.with_suffix("")
    incoming_df = _as_str_df(read_file(incoming))
    for col in CSV_COLUMNS:
        if col not in incoming_df.columns:
            incoming_df[col] = ""
    incoming_df = incoming_df[CSV_COLUMNS]

    existing = read_table(stem)
    if existing is not None:
        existing = _as_str_df(existing)
        for col in CSV_COLUMNS:
            if col not in existing.columns:
                existing[col] = ""
        existing = existing[CSV_COLUMNS]
        combined = pd.concat([existing, incoming_df], ignore_index=True)
    else:
        combined = incoming_df

    combined["_sig"] = combined.apply(lambda row: str(_row_signature(row)), axis=1)
    combined = combined.drop_duplicates(subset=["_sig"], keep="last").drop(columns=["_sig"])
    write_table(combined, stem)
    return len(incoming_df)


def _merge_file(incoming: Path, baemin_root: Path) -> tuple[str, int]:
    dataset = incoming.relative_to(baemin_root).parts[0]
    target = _analytics_path(incoming, baemin_root)

    if dataset in _CSV_DATASETS:
        expected_name, key = _CSV_DATASETS[dataset]
        if incoming.name != expected_name:
            return dataset, 0
        return dataset, _merge_csv(incoming, target, key)

    if dataset == "orders" and incoming.stem.startswith("orders_"):
        target = target.with_suffix(".parquet")
        return dataset, _merge_orders(incoming, target)

    if dataset == "shop_change" and incoming.stem == "shop_change":
        target = target.with_suffix(".parquet")
        return dataset, _merge_shop_change(incoming, target)

    logger.info("PC2 inbox 미지원 파일 skip: %s", incoming)
    return dataset, 0


def _process_run(run_dir: Path) -> Counter:
    baemin_root = run_dir / "baemin_macro"
    counts: Counter = Counter()
    if not baemin_root.exists():
        logger.warning("PC2 inbox run skip: baemin_macro 없음 %s", run_dir)
        return counts

    for incoming in sorted(p for p in baemin_root.rglob("*") if p.is_file()):
        dataset, rows = _merge_file(incoming, baemin_root)
        if rows:
            counts[dataset] += rows
    return counts


def ingest_baemin_pc2_inbox(**context) -> str:
    if not INBOX.exists():
        return f"PC2 inbox 없음: {INBOX}"
    if not _acquire_lock():
        logger.info("PC2 inbox ingest skip: 다른 ingest 실행 중")
        return "PC2 inbox ingest skip: locked"

    processed = 0
    total_counts: Counter = Counter()
    try:
        PROCESSING.mkdir(parents=True, exist_ok=True)
        for run_dir in _candidate_runs():
            manifest = _load_manifest(run_dir)
            if manifest is None or not _manifest_ready(run_dir, manifest):
                continue

            claimed = PROCESSING / run_dir.name
            try:
                shutil.move(str(run_dir), str(claimed))
            except Exception as exc:
                logger.info("PC2 inbox run claim 실패, skip: %s (%s)", run_dir, exc)
                continue

            try:
                counts = _process_run(claimed)
                total_counts.update(counts)
                processed += 1
                shutil.rmtree(claimed)
                logger.info("PC2 inbox run 처리 완료: %s %s", claimed.name, dict(counts))
            except Exception:
                logger.exception("PC2 inbox run 처리 실패, 보존: %s", claimed)
                raise
    finally:
        _release_lock()

    summary = f"PC2 inbox 처리 run={processed}, rows={dict(total_counts)}"
    logger.info(summary)
    return summary
