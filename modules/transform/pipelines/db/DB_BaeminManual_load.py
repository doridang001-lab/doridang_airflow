"""Baemin manual order CSV manual upload ingestion."""

from __future__ import annotations

import json
import logging
import re
import shutil
from pathlib import Path

import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_Beamin_Macro_validate import (
    _manual_baemin_filename_fallback,
    _manual_baemin_store_meta,
)
from modules.transform.pipelines.db.DB_Beamin_04_orders import _COLUMNS
from modules.transform.pipelines.db.beamin_store_io import read_table, write_table
from modules.transform.utility.paths import COLLECT_DB, BAEMIN_ORDERS_DB

logger = logging.getLogger(__name__)

COLLECT_SRC = COLLECT_DB / "영업관리부_수집"
ARCHIVE_DIR = COLLECT_SRC / "_archived"

_REQUIRED = {"주문상태", "주문번호", "주문시각", "결제금액"}
_STATUS_DELIVERED = "배달완료"


def _ym_from_order_time(value: str) -> str:
    """'2026. 06. 17.' -> '2026-06'"""
    if not value:
        return ""
    m = re.match(r"(\d{4})\.\s*(\d{1,2})\.", str(value).strip())
    if not m:
        return ""
    month = m.group(2).zfill(2)
    return f"{m.group(1)}-{month}"


def _archive_path(path: Path) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    target = ARCHIVE_DIR / path.name
    if not target.exists():
        return target
    stem, suffix = path.stem, path.suffix
    for idx in range(1, 1000):
        candidate = ARCHIVE_DIR / f"{stem}.{idx}{suffix}"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"cannot create archive path for {path}")


def _load_one_file(csv_path: Path) -> tuple[int, bool]:
    """Return (loaded_rows, should_cleanup)."""
    df = None
    for enc in ("utf-8-sig", "cp949"):
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding=enc)
            break
        except Exception:
            df = None

    if df is None or df.empty:
        logger.warning("파일 읽기 실패 또는 빈 파일: %s", csv_path.name)
        return 0, False

    missing = _REQUIRED - set(df.columns)
    if missing:
        logger.warning("필수컬럼 누락: %s / %s", csv_path.name, sorted(missing))
        return 0, False

    fallback = _manual_baemin_filename_fallback(csv_path)
    raw_store = ""
    if "store_name" in df.columns and not df["store_name"].dropna().empty:
        raw_store = str(df["store_name"].dropna().astype(str).iloc[0])
    store_key, brand = _manual_baemin_store_meta(raw_store, fallback)
    if not store_key or not brand:
        logger.warning("가게 파싱 실패: %s / raw=%s", csv_path.name, raw_store)
        return 0, False

    target_df = df[
        (df["주문상태"].astype(str) == _STATUS_DELIVERED)
        & df["결제금액"].astype(str).str.strip().ne("")
    ].copy()
    if target_df.empty:
        logger.info("처리할 배달완료 건 없음: %s", csv_path.name)
        return 0, True

    target_df["collected_at"] = pendulum.now("Asia/Seoul").isoformat()
    target_df["store_name"] = store_key

    total_rows = 0
    for ym, group in target_df.groupby(target_df["주문시각"].apply(_ym_from_order_time)):
        if not ym:
            logger.warning("ym 파싱 실패, 건 수: %d / 파일: %s", len(group), csv_path.name)
            continue

        new_df = pd.DataFrame("", index=group.index, columns=_COLUMNS, dtype=str)
        for col in _COLUMNS:
            if col in group.columns:
                new_df[col] = group[col].astype(str).values
        new_df = new_df.fillna("").astype(str)

        stem = BAEMIN_ORDERS_DB / f"brand={brand}" / f"store={store_key}" / f"ym={ym}" / f"orders_{ym}"
        new_order_ids = set(new_df["주문번호"].unique())
        existing = read_table(stem)
        if existing is not None and not existing.empty:
            existing = existing[~existing["주문번호"].isin(new_order_ids)]
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        out_path = write_table(combined, stem)
        total_rows += len(new_df)
        logger.info("ingest complete: %s/%s ym=%s rows=%d -> %s", brand, store_key, ym, len(new_df), out_path)

    return total_rows, True


def load_manual_baemin_orders(**context) -> str:
    """Load `baemin_orders_*.csv` files under COLLECT_SRC into BAEMIN_ORDERS_DB."""
    files = sorted(COLLECT_SRC.glob("baemin_orders_*.csv"))
    if not files:
        logger.info("대상 파일 없음: %s", COLLECT_SRC)
        return json.dumps({"loaded_files": [], "skipped_files": []}, ensure_ascii=False)

    loaded: list[str] = []
    skipped: list[str] = []
    for csv_path in files:
        rows, ok = _load_one_file(csv_path)
        if ok:
            loaded.append(str(csv_path))
            logger.info("로드 성공: %s (%d rows)", csv_path.name, rows)
        else:
            skipped.append(str(csv_path))
            logger.warning("로드 실패(skip): %s", csv_path.name)

    return json.dumps({"loaded_files": loaded, "skipped_files": skipped}, ensure_ascii=False)


def cleanup_manual_baemin_orders(**context) -> str:
    """Move only loaded files to _archived."""
    ti = context.get("task_instance")
    if not ti:
        logger.info("XCom context 없음, cleanup skipped")
        return "cleanup skipped: no task instance"

    raw = ti.xcom_pull(task_ids="ingest_manual_baemin_orders", key="return_value")
    if not raw:
        logger.info("XCom 없음, cleanup skipped")
        return "cleanup skipped: no xcom payload"

    try:
        payload = json.loads(raw)
    except Exception:
        logger.warning("XCom payload 파싱 실패: %s", raw)
        return "cleanup skipped: invalid payload"

    loaded_files = payload.get("loaded_files", [])
    moved = 0
    for path_str in loaded_files:
        path = Path(path_str)
        if not path.exists():
            logger.info("이미 이동됨 또는 삭제됨: %s", path.name)
            continue
        target = _archive_path(path)
        try:
            shutil.move(str(path), str(target))
            logger.info("보관됨: %s -> %s", path.name, target)
            moved += 1
        except Exception as exc:
            logger.warning("보관 실패: %s / %s", path.name, exc)

    return f"cleanup complete: {moved} files"
