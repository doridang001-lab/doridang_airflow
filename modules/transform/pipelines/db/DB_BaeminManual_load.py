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
from modules.transform.pipelines.db.beamin_store_io import order_ym, read_table, write_table
from modules.transform.pipelines.db.db_baemin_marketing import (
    _extract_branch,
    _extract_brand_from_filename,
)
from modules.transform.utility.paths import (
    BAEMIN_MARKETING_DB,
    BAEMIN_METRICS_DB,
    BAEMIN_ORDERS_DB,
    COLLECT_DB,
    DOWN_DIR,
)

logger = logging.getLogger(__name__)

COLLECT_SRC = COLLECT_DB / "영업관리부_수집"
ARCHIVE_DIR = COLLECT_SRC / "_archived"
DOWN_ARCHIVE_DIR = COLLECT_SRC

_REQUIRED = {"주문상태", "주문번호", "주문시각", "결제금액"}
_STATUS_DELIVERED = "배달완료"
_ORDER_KEY = "주문번호"
_ORDER_LINE_KEY = [
    "주문번호",
    "주문시각",
    "주문내역",
    "주문옵션상세",
    "주문수량",
    "주문옵션금액",
    "결제금액",
]
_KNOWN_BRANDS = ("나홀로", "도리당")


def _upsert_manual_orders(existing: pd.DataFrame | None, new_df: pd.DataFrame) -> pd.DataFrame:
    """기존 월 파티션과 새 수동 CSV를 주문번호 단위 최신 수집본으로 병합한다."""
    if existing is None or existing.empty:
        combined = new_df.copy()
    else:
        combined = pd.concat([existing, new_df], ignore_index=True)

    combined = combined.fillna("").astype(str)
    if _ORDER_KEY not in combined.columns:
        return combined.drop_duplicates(keep="last").reset_index(drop=True)

    if "collected_at" not in combined.columns:
        combined["collected_at"] = ""

    order_latest = (
        combined[[ _ORDER_KEY, "collected_at"]]
        .assign(
            **{
                _ORDER_KEY: combined[_ORDER_KEY].fillna("").astype(str).str.strip(),
                "collected_at": combined["collected_at"].fillna("").astype(str),
            }
        )
        .groupby(_ORDER_KEY, dropna=False)["collected_at"]
        .max()
        .to_dict()
    )
    order_ids = combined[_ORDER_KEY].fillna("").astype(str).str.strip()
    latest_collected = order_ids.map(lambda oid: order_latest.get(oid, ""))
    keep_latest = order_ids.eq("") | combined["collected_at"].fillna("").astype(str).eq(latest_collected)
    out = combined[keep_latest].copy()

    dedup_cols = [col for col in _ORDER_LINE_KEY if col in out.columns]
    if dedup_cols:
        out = out.drop_duplicates(subset=dedup_cols, keep="last")
    else:
        out = out.drop_duplicates(keep="last")

    return out.reset_index(drop=True)


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


def _down_collect_path(path: Path) -> Path:
    DOWN_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    target = DOWN_ARCHIVE_DIR / path.name
    if not target.exists():
        return target
    stem, suffix = path.stem, path.suffix
    for idx in range(1, 1000):
        candidate = DOWN_ARCHIVE_DIR / f"{stem}.{idx}{suffix}"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"cannot create down collect path for {path}")


def _iter_manual_files(prefix: str) -> list[dict[str, str]]:
    """Collect manual Baemin files from download and collect folders with source tags."""
    items: list[dict[str, str]] = []
    seen: set[Path] = set()
    source_dirs = (
        (DOWN_DIR, "down"),
        (DOWN_DIR / "업로드_temp", "down"),
        (COLLECT_SRC, "collect"),
    )
    for source_dir, source_name in source_dirs:
        if not source_dir.exists():
            continue
        for csv_path in sorted(source_dir.glob(f"baemin_{prefix}_*.csv")):
            resolved = csv_path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            items.append({"path": str(csv_path), "source": source_name})
    return items


def _iter_manual_order_files() -> list[dict[str, str]]:
    return _iter_manual_files("orders")


def _baemin_now_brand_store(store_name: str) -> tuple[str, str]:
    text = re.sub(r"\[.*?\]\s*", "", str(store_name or "")).strip()
    brand = next((b for b in _KNOWN_BRANDS if b in text), "")
    matches = re.findall(r"[가-힣A-Za-z0-9]+(?:점|지점|분점|직영점)", text)
    store = matches[-1] if matches else (text.split()[-1] if text.split() else "")
    return brand, store


def _date_from_metrics(path: Path, collected_at: str) -> str:
    tokens = re.findall(r"\d{8}", path.name)
    if tokens:
        token = tokens[-1]
        return f"{token[:4]}-{token[4:6]}-{token[6:]}"
    return str(collected_at or "")[:10]


def _ym_from_date(value: str) -> str:
    text = str(value or "").strip()
    if len(text) < 7:
        return ""
    return text[:7].replace(".", "-")


def _read_utf8_sig_csv(path: Path, dataset: str) -> pd.DataFrame | None:
    try:
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    except Exception as exc:
        logger.warning("%s 파일 읽기 실패: %s / %s", dataset, path.name, exc)
        return None


def _read_existing_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    except Exception as exc:
        logger.warning("기존 CSV 읽기 실패, 신규 데이터만 저장: %s / %s", path, exc)
        return pd.DataFrame()


def _normalize_marketing_date(df: pd.DataFrame) -> pd.DataFrame:
    if "날짜" not in df.columns or df.empty:
        return df
    out = df.copy()
    parsed = pd.to_datetime(out["날짜"], errors="coerce", format="mixed").dt.floor("D")
    normalized = parsed.dt.strftime("%Y-%m-%d %I:%M:%S %p")
    mask = normalized.notna()
    out.loc[mask, "날짜"] = normalized[mask]
    return out


def _save_marketing_groups(grouped_rows: dict[tuple[str, str, str], list[dict]]) -> list[str]:
    outputs: list[str] = []
    for (brand, store, ym), rows in grouped_rows.items():
        out_dir = BAEMIN_MARKETING_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "baemin_marketing_data.csv"

        new_df = pd.DataFrame(rows).fillna("").astype(str)
        existing = _read_existing_csv(out_path)
        combined = pd.concat([existing, new_df], ignore_index=True) if not existing.empty else new_df
        combined = combined.fillna("").astype(str)
        if "collected_at" not in combined.columns:
            combined["collected_at"] = ""
        for col in ("store_id", "날짜"):
            if col not in combined.columns:
                combined[col] = ""
        combined = _normalize_marketing_date(combined)
        combined = combined.sort_values("collected_at").drop_duplicates(
            subset=["store_id", "날짜"],
            keep="last",
        )
        combined.to_csv(out_path, index=False, encoding="utf-8-sig")
        outputs.append(str(out_path))
        logger.info("manual baemin marketing saved: %s rows=%d", out_path, len(combined))
    return outputs


def _save_metrics_groups(grouped_rows: dict[tuple[str, str, str], list[dict]]) -> list[str]:
    outputs: list[str] = []
    for (brand, store, ym), rows in grouped_rows.items():
        out_dir = BAEMIN_METRICS_DB / f"brand={brand}" / f"store={store}" / f"ym={ym}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "baemin_now.csv"

        new_df = pd.DataFrame(rows).fillna("").astype(str)
        existing = _read_existing_csv(out_path)
        if not existing.empty and "date" in existing.columns and "date" in new_df.columns:
            dates = set(new_df["date"].fillna("").astype(str))
            existing = existing[~existing["date"].fillna("").astype(str).isin(dates)]
        combined = pd.concat([existing, new_df], ignore_index=True) if not existing.empty else new_df
        combined = combined.fillna("").astype(str)
        combined.to_csv(out_path, index=False, encoding="utf-8-sig")
        outputs.append(str(out_path))
        logger.info("manual baemin metrics saved: %s rows=%d", out_path, len(combined))
    return outputs


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

    target_df = df[df["주문상태"].astype(str).str.strip() == _STATUS_DELIVERED].copy()
    if target_df.empty:
        logger.info("처리할 배달완료 건 없음: %s", csv_path.name)
        return 0, True

    target_df["collected_at"] = pendulum.now("Asia/Seoul").isoformat()
    target_df["store_name"] = store_key

    total_rows = 0
    for ym, group in target_df.groupby(order_ym(target_df["주문시각"])):
        if not ym:
            logger.warning("ym 파싱 실패, 건 수: %d / 파일: %s", len(group), csv_path.name)
            continue

        new_df = pd.DataFrame("", index=group.index, columns=_COLUMNS, dtype=str)
        for col in _COLUMNS:
            if col in group.columns:
                new_df[col] = group[col].astype(str).values
        new_df = new_df.fillna("").astype(str)

        stem = BAEMIN_ORDERS_DB / f"brand={brand}" / f"store={store_key}" / f"ym={ym}" / f"orders_{ym}"
        existing = read_table(stem)
        if existing is not None and not existing.empty:
            logger.info(
                "manual baemin partition latest-upsert: %s/%s ym=%s existing=%d new=%d",
                brand,
                store_key,
                ym,
                len(existing),
                len(new_df),
            )
        combined = _upsert_manual_orders(existing, new_df)
        out_path = write_table(combined, stem)
        total_rows += len(new_df)
        logger.info("ingest complete: %s/%s ym=%s rows=%d -> %s", brand, store_key, ym, len(new_df), out_path)

    return total_rows, True


def load_manual_baemin_orders(**context) -> str:
    """Load `baemin_orders_*.csv` files under DOWN_DIR/COLLECT_SRC into BAEMIN_ORDERS_DB."""
    files = _iter_manual_order_files()
    if not files:
        logger.info("대상 파일 없음: %s, %s", DOWN_DIR, COLLECT_SRC)
        return json.dumps({"loaded_files": [], "skipped_files": []}, ensure_ascii=False)

    loaded: list[dict[str, str]] = []
    skipped: list[str] = []
    for item in files:
        csv_path = Path(item["path"])
        rows, ok = _load_one_file(csv_path)
        if ok:
            loaded.append(item)
            logger.info("로드 성공: %s (%d rows)", csv_path.name, rows)
        else:
            skipped.append(str(csv_path))
            logger.warning("로드 실패(skip): %s", csv_path.name)

    return json.dumps({"loaded_files": loaded, "skipped_files": skipped}, ensure_ascii=False)


def load_manual_baemin_marketing(**context) -> str:
    files = _iter_manual_files("marketing")
    grouped_rows: dict[tuple[str, str, str], list[dict]] = {}
    loaded: list[dict[str, str]] = []
    skipped: list[str] = []

    for item in files:
        path = Path(item["path"])
        df = _read_utf8_sig_csv(path, "marketing")
        if df is None or df.empty:
            skipped.append(str(path))
            continue

        brand = _extract_brand_from_filename(path.name)
        file_rows = 0
        for _, row in df.iterrows():
            store = _extract_branch(row.get("store_name", ""))
            ym = _ym_from_date(row.get("날짜", ""))
            if not brand or brand == "unknown" or not store or not ym:
                continue
            row_dict = row.fillna("").astype(str).to_dict()
            grouped_rows.setdefault((brand, store, ym), []).append(row_dict)
            file_rows += 1

        if file_rows:
            loaded.append(item)
            logger.info("manual baemin marketing loaded: %s rows=%d", path.name, file_rows)
        else:
            skipped.append(str(path))
            logger.warning("manual baemin marketing rows skipped: %s", path.name)

    outputs = _save_marketing_groups(grouped_rows)
    return json.dumps(
        {
            "loaded_files": loaded,
            "skipped_files": skipped,
            "outputs": outputs,
        },
        ensure_ascii=False,
    )


def load_manual_baemin_metrics(**context) -> str:
    files = _iter_manual_files("metrics")
    grouped_rows: dict[tuple[str, str, str], list[dict]] = {}
    loaded: list[dict[str, str]] = []
    skipped: list[str] = []

    for item in files:
        path = Path(item["path"])
        df = _read_utf8_sig_csv(path, "metrics")
        if df is None or df.empty:
            skipped.append(str(path))
            continue

        file_rows = 0
        for _, row in df.iterrows():
            brand, store = _baemin_now_brand_store(row.get("store_name", ""))
            date = _date_from_metrics(path, row.get("collected_at", ""))
            ym = _ym_from_date(date)
            if not brand or not store or not date or not ym:
                continue
            row_dict = row.fillna("").astype(str).to_dict()
            row_dict["date"] = date
            grouped_rows.setdefault((brand, store, ym), []).append(row_dict)
            file_rows += 1

        if file_rows:
            loaded.append(item)
            logger.info("manual baemin metrics loaded: %s rows=%d", path.name, file_rows)
        else:
            skipped.append(str(path))
            logger.warning("manual baemin metrics rows skipped: %s", path.name)

    outputs = _save_metrics_groups(grouped_rows)
    return json.dumps(
        {
            "loaded_files": loaded,
            "skipped_files": skipped,
            "outputs": outputs,
        },
        ensure_ascii=False,
    )


def load_manual_baemin_files(**context) -> str:
    orders = json.loads(load_manual_baemin_orders(**context))
    marketing = json.loads(load_manual_baemin_marketing(**context))
    metrics = json.loads(load_manual_baemin_metrics(**context))
    return json.dumps(
        {
            "orders": orders.get("loaded_files", []),
            "marketing": marketing.get("loaded_files", []),
            "metrics": metrics.get("loaded_files", []),
        },
        ensure_ascii=False,
    )


def _cleanup_loaded_files(loaded_files: list) -> int:
    moved = 0
    for item in loaded_files:
        if isinstance(item, dict):
            path = Path(item.get("path", ""))
            source = item.get("source", "collect")
        else:
            path = Path(str(item))
            source = "collect"
        if not path.exists():
            logger.info("이미 이동됨 또는 삭제됨: %s", path.name)
            continue
        target = _down_collect_path(path) if source == "down" else _archive_path(path)
        try:
            shutil.move(str(path), str(target))
            logger.info("보관됨: %s -> %s", path.name, target)
            moved += 1
        except Exception as exc:
            logger.warning("보관 실패: %s / %s", path.name, exc)

    return moved


def cleanup_manual_baemin_orders(**context) -> str:
    """Move only loaded order files to _archived."""
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

    moved = _cleanup_loaded_files(payload.get("loaded_files", []))
    return f"cleanup complete: {moved} files"


def cleanup_manual_baemin_files(**context) -> str:
    """Move only loaded manual Baemin files from the unified ingest payload."""
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

    loaded_files = []
    if "loaded_files" in payload:
        loaded_files.extend(payload.get("loaded_files") or [])
    for key in ("orders", "marketing", "metrics"):
        loaded_files.extend(payload.get(key) or [])

    moved = _cleanup_loaded_files(loaded_files)
    return f"cleanup complete: {moved} files"
