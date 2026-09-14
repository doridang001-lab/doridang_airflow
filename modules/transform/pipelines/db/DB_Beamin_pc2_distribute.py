"""Baemin macro inbox files distributor."""

from __future__ import annotations

import json
import logging
import re
import shutil
import time
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from modules.transform.pipelines.db.beamin_store_io import read_file, read_table, write_table
from modules.transform.utility.paths import ANALYTICS_DB, COLLECT_DB, LOCAL_DB

logger = logging.getLogger(__name__)

INBOX_DIR = COLLECT_DB / "영업관리부_수집" / "_baemin_pc2_inbox"
UPLOAD_INBOX_DIR = COLLECT_DB / "영업관리부_수집" / "_baemin_upload_inbox"
ORDER_KEY = "주문번호"
CSV_UPSERT_KEYS = {"ad_funnel": "target_date"}
ORDER_SETTLEMENT_COLUMNS = [
    "주문중개",
    "고객할인비용",
    "배달",
    "그외",
    "부가세",
    "만나서결제금액",
    "입금예정금액",
]
# 즉시할인 분해는 상세시트를 열어야만 읽히므로 재수집에서 통째로 비는 일이 잦다.
# 공란으로 덮어써 기존 정상값을 잃지 않도록 보존 대상에 포함한다.
# (수집기가 0원을 '0'으로 명시 기록하므로 `_is_filled` 기준에서 0은 값으로 취급된다)
ORDER_DISCOUNT_COLUMNS = [
    "즉시할인",
    "즉시할인_파트너부담",
    "즉시할인_배민지원",
    "배민부담_쿠폰할인",
]
ORDER_PRESERVE_COLUMNS = [*ORDER_SETTLEMENT_COLUMNS, *ORDER_DISCOUNT_COLUMNS]
_EMPTY_TOKENS = {"", "nan", "NaN", "None", "<NA>", "null", "NULL"}
SUPPORTED_SUFFIXES = {".parquet", ".csv"}
ROOT_PART = "baemin_macro"
FAILED_KEYS = ("accounts", "stores", "orders", "ads")
DEFAULT_FOLDER_PATTERN = "manual__*"
TOP_FOLDER_PATTERN = "manual__top__*"
BOTTOM_FOLDER_PATTERN = "manual__bottom__*"
ALLOWED_UPLOAD_FOLDER_PATTERNS = frozenset(
    {DEFAULT_FOLDER_PATTERN, TOP_FOLDER_PATTERN, BOTTOM_FOLDER_PATTERN}
)
QUARANTINE_DIR_NAME = "_quarantine"
TMP_FOLDER_PREFIX = "_tmp__manual__"
STALE_TMP_MAX_AGE_HOURS = 24
_FOLDER_TIMESTAMP_RE = re.compile(r"__(\d{8}_\d{6})$")
ACL_REPAIR_QUEUE_DIR = LOCAL_DB / "baemin_acl_repair_queue"
ACL_ACK_TIMEOUT_SECONDS = 180
ACL_ACK_POLL_SECONDS = 2
ACL_REQUEST_SCHEMA_VERSION = 1


class SourceFileReadError(OSError):
    """Source inbox file could not be read reliably."""


def ingest_baemin_pc2_inbox(**context) -> str:
    """PC2 inbox의 manual__* 폴더를 analytics baemin_macro 파티션으로 적재한다."""
    return ingest_inbox(INBOX_DIR)["summary"]


def ingest_baemin_upload_inbox(
    folder_pattern: str = DEFAULT_FOLDER_PATTERN,
    **context,
) -> dict[str, Any]:
    """Upload inbox의 manual__* 폴더를 적재하고 폴더 meta를 함께 반환한다."""
    _validate_upload_folder_pattern(folder_pattern)
    return ingest_inbox(
        UPLOAD_INBOX_DIR,
        read_meta=True,
        folder_pattern=folder_pattern,
        require_acl_ack=True,
    )


def count_baemin_upload_inbox_folders(
    folder_pattern: str = DEFAULT_FOLDER_PATTERN,
) -> int:
    """완성된 upload inbox 폴더 수를 반환한다."""
    _validate_upload_folder_pattern(folder_pattern)
    if not UPLOAD_INBOX_DIR.exists():
        return 0
    return sum(
        1
        for path in UPLOAD_INBOX_DIR.glob(folder_pattern)
        if path.is_dir() and path.name != QUARANTINE_DIR_NAME
    )


def ingest_inbox(
    inbox_dir: Path,
    read_meta: bool = False,
    folder_pattern: str = DEFAULT_FOLDER_PATTERN,
    require_acl_ack: bool = False,
) -> dict[str, Any]:
    label = inbox_dir.name
    if not inbox_dir.exists():
        logger.info("%s(pattern=%s) 없음: %s", label, folder_pattern, inbox_dir)
        return {
            "summary": f"{label} 없음: 스킵",
            "meta": _empty_meta() if read_meta else {},
            "stats": _empty_ingest_stats(),
        }

    stale_tmp = _sweep_stale_tmp_folders(inbox_dir)

    folders = sorted(
        (
            p
            for p in inbox_dir.glob(folder_pattern)
            if p.is_dir() and p.name != QUARANTINE_DIR_NAME
        ),
        key=_folder_sort_key,
    )
    if not folders:
        logger.info("%s(pattern=%s) 처리 대상 없음: %s", label, folder_pattern, inbox_dir)
        stats = _empty_ingest_stats()
        stats["stale_tmp"] = stale_tmp
        return {
            "summary": f"{label} 처리 대상 없음: 스킵",
            "meta": _empty_meta() if read_meta else {},
            "stats": stats,
        }
    processed_folders = 0
    cleaned_folders = 0
    skipped_folders = 0
    quarantined_folders = 0
    failed_folders = 0
    processed_files = 0
    processed_rows = 0
    acl_verified_files = 0
    subtype_counts: dict[str, int] = {}
    successful_meta: list[tuple[Path, dict[str, Any]]] = []

    for folder in folders:
        files = _target_files(folder)
        folder_files = 0
        folder_rows = 0
        folder_counts: dict[str, int] = {}
        folder_output_paths: list[Path] = []
        logger.info("%s 폴더 처리 시작: %s | 대상파일=%d", label, folder, len(files))
        if not files:
            skipped_folders += 1
            if _quarantine_empty_folder(folder, inbox_dir):
                quarantined_folders += 1
            logger.warning("%s 대상 파일 없음, quarantine 처리: %s", label, folder)
            continue

        processed_folders += 1
        try:
            folder_meta = _read_meta(folder) if read_meta else {}
            for src_file in files:
                subtype, rows, out_path = _distribute_one_file(folder, src_file)
                folder_files += 1
                folder_rows += rows
                folder_counts[subtype] = folder_counts.get(subtype, 0) + rows
                folder_output_paths.append(out_path)
                logger.info(
                    "%s 파일 분배 완료: folder=%s file=%s subtype=%s rows=%d",
                    label,
                    folder.name,
                    src_file.relative_to(folder),
                    subtype,
                    rows,
                )
            folder_acl_verified = (
                _request_and_wait_acl_repair(folder, folder_output_paths)
                if require_acl_ack
                else 0
            )
            _cleanup_processed_folder(folder, inbox_dir)
        except SourceFileReadError:
            if _quarantine_empty_folder(folder, inbox_dir):
                quarantined_folders += 1
                logger.exception("%s 소스 파일 읽기 실패, quarantine 처리: %s", label, folder)
            else:
                failed_folders += 1
                logger.exception("%s 소스 파일 읽기 실패, quarantine 실패: %s", label, folder)
            continue
        except Exception:
            failed_folders += 1
            logger.exception("%s 폴더 처리 실패, cleanup 보류: %s", label, folder)
            continue

        cleaned_folders += 1
        processed_files += folder_files
        processed_rows += folder_rows
        acl_verified_files += folder_acl_verified
        if read_meta:
            successful_meta.append((folder, folder_meta))
        for subtype, rows in folder_counts.items():
            subtype_counts[subtype] = subtype_counts.get(subtype, 0) + rows
        logger.info("%s 폴더 cleanup 완료: %s | files=%d rows=%d", label, folder, folder_files, folder_rows)

    meta_items: list[dict[str, Any]] = []
    if read_meta:
        successful_dates = sorted(
            str(item.get("target_date"))
            for _, item in successful_meta
            if item.get("target_date")
        )
        representative_date = successful_dates[-1] if successful_dates else None
        logger.info(
            "%s(pattern=%s) 대표 target_date=%s (성공 폴더 %d개 중)",
            label,
            folder_pattern,
            representative_date,
            len(successful_meta),
        )
        for folder, folder_meta in successful_meta:
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

    meta = _merge_meta(meta_items) if read_meta else {}
    subtype_summary = ", ".join(f"{k}={v}" for k, v in sorted(subtype_counts.items())) or "없음"
    summary = (
        f"{label} 적재 완료(pattern={folder_pattern}) | "
        f"folders={processed_folders} cleaned={cleaned_folders} skipped={skipped_folders} "
        f"quarantined={quarantined_folders} stale_tmp={stale_tmp} failed={failed_folders} "
        f"files={processed_files} acl_verified_files={acl_verified_files} "
        f"rows={processed_rows} subtypes={subtype_summary}"
    )
    logger.info(summary)
    return {
        "summary": summary,
        "meta": meta,
        "stats": {
            "folders": processed_folders,
            "cleaned": cleaned_folders,
            "skipped": skipped_folders,
            "quarantined": quarantined_folders,
            "stale_tmp": stale_tmp,
            "failed": failed_folders,
            "files": processed_files,
            "acl_verified_files": acl_verified_files,
            "rows": processed_rows,
            "subtypes": dict(sorted(subtype_counts.items())),
        },
    }


def _validate_upload_folder_pattern(folder_pattern: str) -> None:
    pattern = str(folder_pattern or "")
    if pattern in ALLOWED_UPLOAD_FOLDER_PATTERNS:
        return
    if (
        pattern.startswith("manual__")
        and "/" not in pattern
        and "\\" not in pattern
        and ".." not in pattern
        and "*" not in pattern
        and "?" not in pattern
    ):
        return
    raise ValueError(f"허용되지 않은 upload inbox 폴더 패턴: {folder_pattern}")


def _empty_ingest_stats() -> dict[str, Any]:
    return {
        "folders": 0,
        "cleaned": 0,
        "skipped": 0,
        "quarantined": 0,
        "stale_tmp": 0,
        "failed": 0,
        "files": 0,
        "acl_verified_files": 0,
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


def _distribute_one_file(folder: Path, src_file: Path) -> tuple[str, int, Path]:
    rel = src_file.relative_to(folder)
    if len(rel.parts) < 3 or rel.parts[0] != ROOT_PART:
        raise ValueError(f"inbox 대상 경로 아님: {src_file}")

    subtype = rel.parts[1]
    dst_stem = (ANALYTICS_DB / rel).with_suffix("")
    try:
        new_df = read_file(src_file)
    except OSError as exc:
        raise SourceFileReadError(f"inbox 소스 파일 읽기 실패: {src_file}") from exc
    if new_df.empty:
        raise ValueError(f"inbox 빈 파일: {src_file}")

    if subtype == "orders":
        if ORDER_KEY in new_df.columns:
            combined = _upsert_orders(dst_stem, new_df)
        else:
            combined = new_df
    elif subtype == "metrics_now":
        combined = _upsert_metrics_now(dst_stem, new_df)
    elif subtype in CSV_UPSERT_KEYS:
        combined = _upsert_by_key(dst_stem, new_df, CSV_UPSERT_KEYS[subtype])
    else:
        combined = new_df

    if subtype == "orders" or src_file.suffix.lower() == ".parquet":
        out_path = write_table(combined, dst_stem)
    else:
        out_path = _write_csv_table(combined, dst_stem)

    logger.info("inbox write: %s -> %s | rows=%d", src_file, out_path, len(combined))
    return subtype, len(new_df), out_path


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
    return _upsert_by_key(dst_stem, new_df, ORDER_KEY)


def _upsert_metrics_now(dst_stem: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    from modules.transform.pipelines.db.DB_BaeminManual_load import (
        normalize_baemin_now_schema,
    )

    normalized_new = normalize_baemin_now_schema(new_df)
    existing = read_table(dst_stem)
    if existing is None or existing.empty:
        return normalized_new

    normalized_existing = normalize_baemin_now_schema(existing)
    if "date" not in normalized_new.columns or "date" not in normalized_existing.columns:
        return normalize_baemin_now_schema(
            pd.concat([normalized_existing, normalized_new], ignore_index=True)
        )

    new_dates = set(normalized_new["date"].fillna("").astype(str).str.strip())
    new_dates.discard("")
    if new_dates:
        normalized_existing = normalized_existing[
            ~normalized_existing["date"].fillna("").astype(str).str.strip().isin(new_dates)
        ]
    return normalize_baemin_now_schema(
        pd.concat([normalized_existing, normalized_new], ignore_index=True)
    )


def _upsert_by_key(dst_stem: Path, new_df: pd.DataFrame, key: str) -> pd.DataFrame:
    existing = read_table(dst_stem)
    if existing is None or key not in existing.columns or key not in new_df.columns:
        return new_df

    missing_columns = [col for col in existing.columns if col not in new_df.columns]
    if missing_columns:
        logger.warning(
            "업로드 컬럼 누락, 기존 스키마로 정렬: dst=%s missing=%s",
            dst_stem,
            missing_columns,
        )
    new_df = new_df.reindex(columns=existing.columns, fill_value="")
    if key == ORDER_KEY:
        new_df = _coalesce_existing_order_settlement(existing, new_df, key)

    new_keys = set(new_df[key].fillna("").astype(str).unique())
    if "" in new_keys:
        logger.warning("%s 빈 키 값 포함: %s", key, dst_stem)
    existing = existing[~existing[key].fillna("").astype(str).isin(new_keys)]
    return pd.concat([existing, new_df], ignore_index=True)


def _is_filled(value: object) -> bool:
    return str(value if value is not None else "").strip() not in _EMPTY_TOKENS


def _coalesce_existing_order_settlement(
    existing: pd.DataFrame,
    new_df: pd.DataFrame,
    key: str,
) -> pd.DataFrame:
    settlement_cols = [
        col for col in ORDER_PRESERVE_COLUMNS if col in existing.columns and col in new_df.columns
    ]
    if not settlement_cols:
        return new_df

    existing_lookup: dict[str, dict[str, str]] = {}
    for row in existing[[key, *settlement_cols]].itertuples(index=False):
        order_key = str(getattr(row, key) if hasattr(row, key) else row[0] or "").strip()
        if not order_key:
            continue
        values = existing_lookup.setdefault(order_key, {})
        for idx, col in enumerate(settlement_cols, start=1):
            raw = row[idx]
            if col not in values and _is_filled(raw):
                values[col] = str(raw)

    if not existing_lookup:
        return new_df

    out = new_df.copy()
    preserved = 0
    for idx, row in out.iterrows():
        order_key = str(row.get(key, "") or "").strip()
        values = existing_lookup.get(order_key)
        if not values:
            continue
        for col, value in values.items():
            if not _is_filled(row.get(col, "")):
                out.at[idx, col] = value
                preserved += 1
    if preserved:
        logger.warning("orders 업로드 정산값 기존값 보존: %s cells", preserved)
    return out


def _request_and_wait_acl_repair(
    folder: Path,
    output_paths: list[Path],
    *,
    queue_dir: Path = ACL_REPAIR_QUEUE_DIR,
    timeout_seconds: float = ACL_ACK_TIMEOUT_SECONDS,
    poll_seconds: float = ACL_ACK_POLL_SECONDS,
) -> int:
    relative_files = _acl_relative_output_paths(output_paths)
    if not relative_files:
        return 0

    request_id = f"{folder.name}__{uuid.uuid4().hex}"
    queue_dir.mkdir(parents=True, exist_ok=True)
    request_path = queue_dir / f"{request_id}.request.json"
    ack_path = queue_dir / f"{request_id}.done.json"
    temp_path = queue_dir / f".{request_id}.request.tmp"
    request = {
        "schema_version": ACL_REQUEST_SCHEMA_VERSION,
        "request_id": request_id,
        "folder": folder.name,
        "files": relative_files,
        "created_at": pd.Timestamp.now(tz="Asia/Seoul").isoformat(),
    }
    temp_path.write_text(
        json.dumps(request, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(request_path)
    logger.info(
        "Windows ACL 복구 요청: folder=%s files=%d request=%s",
        folder.name,
        len(relative_files),
        request_path,
    )

    deadline = time.monotonic() + timeout_seconds
    while True:
        if ack_path.exists():
            ack = json.loads(ack_path.read_text(encoding="utf-8"))
            _validate_acl_ack(ack, request_id, relative_files)
            request_path.unlink(missing_ok=True)
            ack_path.unlink(missing_ok=True)
            logger.info(
                "Windows ACL 복구 확인: folder=%s files=%d",
                folder.name,
                len(relative_files),
            )
            return len(relative_files)
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Windows ACL 복구 응답 timeout: folder={folder.name} "
                f"request={request_path} timeout={timeout_seconds}s"
            )
        time.sleep(poll_seconds)


def _acl_relative_output_paths(output_paths: list[Path]) -> list[str]:
    analytics_root = ANALYTICS_DB.resolve()
    relative_files: list[str] = []
    for output_path in output_paths:
        resolved = output_path.resolve()
        try:
            relative = resolved.relative_to(analytics_root)
        except ValueError as exc:
            raise ValueError(f"ACL 복구 대상이 analytics 밖임: {output_path}") from exc
        if (
            len(relative.parts) < 2
            or relative.parts[0] != ROOT_PART
            or resolved.suffix.lower() not in SUPPORTED_SUFFIXES
        ):
            raise ValueError(f"ACL 복구 허용 대상 아님: {output_path}")
        relative_files.append(relative.as_posix())
    return sorted(set(relative_files))


def _validate_acl_ack(
    ack: dict[str, Any],
    request_id: str,
    relative_files: list[str],
) -> None:
    if ack.get("schema_version") != ACL_REQUEST_SCHEMA_VERSION:
        raise ValueError(f"Windows ACL 응답 schema 불일치: {ack}")
    if ack.get("request_id") != request_id or ack.get("ok") is not True:
        raise ValueError(f"Windows ACL 응답 상태 불일치: {ack}")
    ack_files = ack.get("files")
    if not isinstance(ack_files, list) or sorted(str(path) for path in ack_files) != relative_files:
        raise ValueError(f"Windows ACL 응답 파일 불일치: {ack}")


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
    for attempt in range(3):
        try:
            shutil.rmtree(folder)
            return
        except OSError as exc:
            if attempt == 2:
                raise
            logger.warning(
                "inbox cleanup 재시도(%d/3): %s / %s",
                attempt + 1,
                folder,
                exc,
            )
            time.sleep(2 * (attempt + 1))


def _folder_sort_key(folder: Path) -> tuple[int, str, int, str]:
    match = _FOLDER_TIMESTAMP_RE.search(folder.name)
    if match:
        return (1, match.group(1), 0, folder.name)
    try:
        modified_ns = folder.stat().st_mtime_ns
    except OSError:
        modified_ns = 0
    return (0, "", modified_ns, folder.name)


def _quarantine_empty_folder(folder: Path, inbox_dir: Path) -> bool:
    quarantine_dir = inbox_dir / QUARANTINE_DIR_NAME
    destination = quarantine_dir / folder.name
    try:
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            raise FileExistsError(f"quarantine 대상이 이미 존재함: {destination}")
        shutil.move(str(folder), str(destination))
    except OSError as exc:
        logger.warning("빈 inbox 폴더 quarantine 실패: %s / %s", folder, exc)
        return False
    logger.info("inbox 폴더 quarantine 완료: %s -> %s", folder, destination)
    return True


def _sweep_stale_tmp_folders(
    inbox_dir: Path,
    max_age_hours: float = STALE_TMP_MAX_AGE_HOURS,
) -> int:
    """중단된 export 잔해(_tmp__manual__*)를 quarantine으로 회수한다."""
    if not inbox_dir.exists():
        return 0
    cutoff = time.time() - max_age_hours * 3600
    moved = 0
    for folder in sorted(inbox_dir.glob(f"{TMP_FOLDER_PREFIX}*")):
        if not folder.is_dir():
            continue
        try:
            if folder.stat().st_mtime > cutoff:
                continue
        except OSError as exc:
            logger.warning("stale tmp mtime 확인 실패: %s / %s", folder, exc)
            continue
        if _quarantine_empty_folder(folder, inbox_dir):
            moved += 1
            logger.warning(
                "중단된 export 잔해 quarantine: %s (age>%sh)",
                folder.name,
                max_age_hours,
            )
    if moved:
        try:
            from modules.transform.utility.notifier import send_telegram

            send_telegram(
                f"[배민 inbox 잔해 회수] {inbox_dir.name}에서 중단된 export {moved}건을 "
                f"quarantine으로 옮김 (age>{max_age_hours}h). 해당 수집분은 적재되지 않음."
            )
        except Exception as exc:
            logger.warning("잔해 회수 알림 실패(무시): %s", exc)
    return moved
