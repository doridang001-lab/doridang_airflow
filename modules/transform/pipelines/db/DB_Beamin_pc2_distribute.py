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
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB, LOCAL_DB

logger = logging.getLogger(__name__)
KST = pendulum.timezone("Asia/Seoul")

INBOX = COLLECT_DB
MANIFEST_PREFIX = "baemin_pc2_manifest__"
PROCESSING = LOCAL_DB / "baemin_pc2_inbox_processing"
LOCK_DIR = LOCAL_DB / "baemin_pc2_inbox_lock"
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
    LOCK_DIR.parent.mkdir(parents=True, exist_ok=True)
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


def _age_minutes(path: Path) -> float:
    return (pendulum.now(KST) - pendulum.from_timestamp(path.stat().st_mtime, tz=KST)).total_seconds() / 60


def _load_manifest(manifest_path: Path) -> dict | None:
    if not manifest_path.exists():
        logger.info("PC2 inbox run skip: manifest 없음 %s", manifest_path)
        return None
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("PC2 inbox run skip: manifest 읽기 실패 %s (%s)", manifest_path, exc)
        return None
    if not isinstance(data, dict) or not isinstance(data.get("files"), dict):
        logger.warning("PC2 inbox run skip: manifest 형식 오류 %s", manifest_path)
        return None
    return data


def _flat_manifest_ready(manifest_path: Path, manifest: dict) -> bool:
    manifest_age = _age_minutes(manifest_path)
    if manifest_age < MIN_AGE_MINUTES:
        logger.info(
            "PC2 inbox manifest skip: mtime %.1f분 경과, 최소 %d분 필요 %s",
            manifest_age,
            MIN_AGE_MINUTES,
            manifest_path,
        )
        return False

    for filename, info in manifest["files"].items():
        if not isinstance(info, dict) or not info.get("rel"):
            logger.warning("PC2 inbox manifest skip: 파일 메타 형식 오류 %s %s", manifest_path, filename)
            return False
        path = INBOX / filename
        if not path.exists() or not path.is_file():
            logger.info("PC2 inbox run skip: 파일 미동기화 %s", path)
            return False
        actual_size = path.stat().st_size
        expected_size = int(info.get("size", -1))
        if actual_size != expected_size:
            logger.info(
                "PC2 inbox run skip: size 불일치 %s expected=%s actual=%s",
                path,
                expected_size,
                actual_size,
            )
            return False
        file_age = _age_minutes(path)
        if file_age < MIN_AGE_MINUTES:
            logger.info(
                "PC2 inbox file skip: mtime %.1f분 경과, 최소 %d분 필요 %s",
                file_age,
                MIN_AGE_MINUTES,
                path,
            )
            return False
    return True


def _candidate_runs() -> list[Path]:
    if not INBOX.exists():
        return []
    return sorted(
        p
        for p in INBOX.iterdir()
        if p.is_file()
        and p.suffix.lower() == ".json"
        and p.name.startswith(MANIFEST_PREFIX)
        and not p.name.startswith("_tmp_")
    )


def _claim_flat_run(manifest_path: Path, manifest: dict) -> Path:
    run_name = str(manifest.get("run") or manifest_path.stem.removeprefix(MANIFEST_PREFIX))
    run_name = "".join(ch if ch.isalnum() or ch in "._=-" else "_" for ch in run_name)[:160]
    run_dir = PROCESSING / run_name
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(manifest_path), str(run_dir / "manifest.json"))
    for filename, info in manifest["files"].items():
        rel = Path(str(info["rel"]))
        src = INBOX / filename
        dst = run_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
    return run_dir


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
        for manifest_path in _candidate_runs():
            manifest = _load_manifest(manifest_path)
            if manifest is None or not _flat_manifest_ready(manifest_path, manifest):
                continue

            try:
                run_dir = _claim_flat_run(manifest_path, manifest)
            except Exception as exc:
                logger.info("PC2 inbox flat run claim 실패, skip: %s (%s)", manifest_path, exc)
                continue

            try:
                counts = _process_run(run_dir)
                total_counts.update(counts)
                processed += 1
                shutil.rmtree(run_dir)
                logger.info("PC2 inbox flat run 처리 완료: %s %s", run_dir.name, dict(counts))
            except Exception:
                logger.exception("PC2 inbox flat run 처리 실패, 보존: %s", run_dir)
                raise
    finally:
        _release_lock()

    summary = f"PC2 inbox 처리 run={processed}, rows={dict(total_counts)}"
    logger.info(summary)
    return summary
