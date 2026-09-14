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
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    record_manual_reingest_marker,
)
from modules.transform.pipelines.db.beamin_store_io import (
    order_date,
    order_ym,
    read_table,
    replace_covered_date_range,
    write_table,
)
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
    MANUAL_DOWN_DIR,
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
_EMPTY_TOKENS = {"", "nan", "none", "<na>", "null"}
BAEMIN_NOW_SCHEMA_COLUMNS = [
    "account_id",
    "platform",
    "collected_at",
    "store_id",
    "store_name",
    "cardIndex",
    "url",
    "조리소요시간",
    "조리소요시간_순위구분",
    "조리소요시간_순위비율",
    "주문접수시간",
    "주문접수시간_순위구분",
    "주문접수시간_순위비율",
    "최근재주문율",
    "조리시간준수율",
    "조리시간준수율_순위구분",
    "조리시간준수율_순위비율",
    "주문접수율",
    "주문접수율_순위구분",
    "주문접수율_순위비율",
    "최근별점",
    "date",
    "영업시간운영률",
    "영업시간운영률_상태",
    "주문취소율",
    "주문취소율_상태",
    "주문취소율_상세",
    "준비시간정확도",
    "준비시간정확도_상태",
    "주문접수시간_상태",
    "최근재주문율_상태",
    "최근별점_상태",
    "collection_note",
    "brand",
    "store",
    "brand_store",
]
BAEMIN_NOW_NEW_COLUMNS = [
    "영업시간운영률",
    "영업시간운영률_상태",
    "주문취소율",
    "주문취소율_상태",
    "주문취소율_상세",
    "준비시간정확도",
    "준비시간정확도_상태",
    "주문접수시간_상태",
    "최근재주문율_상태",
    "최근별점_상태",
]
BAEMIN_NOW_STATUS_COLUMNS = [
    "영업시간운영률_상태",
    "주문취소율_상태",
    "준비시간정확도_상태",
    "주문접수시간_상태",
    "최근재주문율_상태",
    "최근별점_상태",
]
BAEMIN_NOW_ID_COLUMNS = ["brand", "store", "brand_store"]


def _replace_manual_orders(
    existing: pd.DataFrame | None,
    new_df: pd.DataFrame,
    coverage_dates: pd.Series,
) -> tuple[pd.DataFrame, dict]:
    existing_dates = (
        order_date(existing["주문시각"])
        if existing is not None and not existing.empty and "주문시각" in existing.columns
        else pd.Series(dtype=str)
    )
    combined, info = replace_covered_date_range(
        existing,
        new_df,
        existing_dates,
        coverage_dates,
    )
    combined = combined.fillna("").astype(str)
    dedup_cols = [col for col in _ORDER_LINE_KEY if col in combined.columns]
    if dedup_cols:
        combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
    else:
        combined = combined.drop_duplicates(keep="last")

    out = combined.reindex(columns=_COLUMNS, fill_value="").reset_index(drop=True)
    return out, info


def _preserve_missing_existing_orders(
    existing: pd.DataFrame | None,
    new_df: pd.DataFrame,
    coverage_dates: pd.Series,
    combined: pd.DataFrame,
    info: dict,
) -> tuple[pd.DataFrame, dict]:
    if existing is None or existing.empty or _ORDER_KEY not in existing.columns:
        return combined, info
    if _ORDER_KEY not in new_df.columns or "주문시각" not in existing.columns:
        return combined, info

    covered_dates = set(info.get("covered_dates") or [])
    if not covered_dates:
        return combined, info

    existing_data = existing.fillna("").astype(str).reset_index(drop=True)
    existing_dates = order_date(existing_data["주문시각"]).reset_index(drop=True)
    new_data = new_df.fillna("").astype(str).reset_index(drop=True)
    new_dates = (
        order_date(new_data["주문시각"]).reset_index(drop=True)
        if "주문시각" in new_data.columns
        else coverage_dates.fillna("").astype(str).str.strip().reset_index(drop=True)
    )
    new_keys_by_date = (
        pd.DataFrame(
            {
                "date": new_dates,
                "key": new_data[_ORDER_KEY].fillna("").astype(str).str.strip(),
            }
        )
        .loc[lambda frame: frame["date"].isin(covered_dates) & frame["key"].ne("")]
        .groupby("date")["key"]
        .apply(set)
        .to_dict()
    )

    preserve_mask = pd.Series(False, index=existing_data.index)
    details = {}
    for date in sorted(covered_dates):
        existing_mask = existing_dates.eq(date)
        if not existing_mask.any():
            continue
        existing_keys = existing_data.loc[existing_mask, _ORDER_KEY].fillna("").astype(str).str.strip()
        missing_keys = sorted(set(existing_keys[existing_keys.ne("")]) - new_keys_by_date.get(date, set()))
        if not missing_keys:
            continue
        preserve_mask |= existing_mask & existing_data[_ORDER_KEY].fillna("").astype(str).str.strip().isin(missing_keys)
        details[date] = {
            "preserved_orders": len(missing_keys),
            "sample": missing_keys[:5],
        }

    if not preserve_mask.any():
        return combined, info

    preserved = existing_data.loc[preserve_mask].copy()
    out = pd.concat([combined.fillna("").astype(str), preserved], ignore_index=True)
    info = {
        **info,
        "removed": max(0, int(info.get("removed", 0)) - len(preserved)),
        "preserved_missing_orders": details,
        "preserved_missing_rows": int(len(preserved)),
    }
    return out, info


def _upsert_manual_orders(existing: pd.DataFrame | None, new_df: pd.DataFrame) -> pd.DataFrame:
    """기존 월 파티션의 새 데이터 주문일자 구간을 교체한다."""
    coverage_dates = (
        order_date(new_df["주문시각"])
        if "주문시각" in new_df.columns
        else pd.Series(dtype=str)
    )
    out, _ = _replace_manual_orders(existing, new_df, coverage_dates)
    return out


def _record_reingest_dates(
    store: str,
    new_df: pd.DataFrame,
    info: dict,
) -> None:
    row_dates = (
        order_date(new_df["주문시각"]).reset_index(drop=True)
        if "주문시각" in new_df.columns
        else pd.Series(dtype=str)
    )
    for date in info.get("covered_dates", []):
        record_manual_reingest_marker(
            "배민수동",
            store,
            date,
            {
                "rows": int(row_dates.eq(date).sum()),
                "removed": int(info.get("removed", 0)),
            },
        )


def _truthy_conf(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _filled_settlement(series: pd.Series) -> pd.Series:
    return ~series.fillna("").astype(str).str.strip().isin(_EMPTY_TOKENS)


def _regression_dates(
    existing: pd.DataFrame | None,
    new_df: pd.DataFrame,
    column: str,
) -> dict[str, dict[str, int]]:
    """해당 날짜에서 `column` 이 기존엔 채워져 있었는데 새 데이터는 전부 빈 경우를 찾는다."""
    if (
        existing is None
        or existing.empty
        or new_df.empty
        or "주문시각" not in existing.columns
        or "주문시각" not in new_df.columns
        or column not in existing.columns
        or column not in new_df.columns
    ):
        return {}

    existing_dates = order_date(existing["주문시각"])
    new_dates = order_date(new_df["주문시각"])
    details: dict[str, dict[str, int]] = {}
    for date in sorted({value for value in new_dates.tolist() if value}):
        existing_mask = existing_dates.eq(date)
        new_mask = new_dates.eq(date)
        if not existing_mask.any() or not new_mask.any():
            continue
        existing_settled = int(_filled_settlement(existing.loc[existing_mask, column]).sum())
        new_settled = int(_filled_settlement(new_df.loc[new_mask, column]).sum())
        if existing_settled > 0 and new_settled == 0:
            details[date] = {
                "existing_settled_rows": existing_settled,
                "new_settled_rows": new_settled,
                "new_rows": int(new_mask.sum()),
            }
    return details


def _settlement_regression_dates(
    existing: pd.DataFrame | None,
    new_df: pd.DataFrame,
) -> dict[str, dict[str, int]]:
    return _regression_dates(existing, new_df, "입금예정금액")


def _discount_regression_dates(
    existing: pd.DataFrame | None,
    new_df: pd.DataFrame,
) -> dict[str, dict[str, int]]:
    """즉시할인 분해가 통째로 비어 들어오는 재적재를 감지한다.

    정산정보와 달리 적재를 막지는 않는다. 즉시할인은 상세시트를 열어야 읽히는
    값이라 정상적으로 비어 있는 파일도 있어서, 경고로만 남기고 보존은
    `_preserve_missing_existing_orders` / upsert 보존 목록에 맡긴다.
    """
    return _regression_dates(existing, new_df, "즉시할인_파트너부담")


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
    if prefix == "orders":
        source_dirs = (*source_dirs, (MANUAL_DOWN_DIR, "manual"))
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


def _is_partial_order_file(path: Path) -> bool:
    return path.name.lower().endswith("_partial.csv")


def count_pending_manual_baemin_order_files() -> int:
    """Return pending manual Baemin order CSV count without loading files."""
    return sum(1 for item in _iter_manual_order_files() if not _is_partial_order_file(Path(item["path"])))


def count_partial_manual_baemin_order_files() -> int:
    """Return pending partial Baemin order CSV count excluded from ingest."""
    return sum(1 for item in _iter_manual_order_files() if _is_partial_order_file(Path(item["path"])))




def _order_dates_from_frame(df: pd.DataFrame | None) -> list[str]:
    if df is None or df.empty or "주문시각" not in df.columns:
        return []
    return sorted({value for value in order_date(df["주문시각"]).tolist() if value})


def _scan_manual_order_dates(csv_path: Path) -> list[str]:
    for enc in ("utf-8-sig", "cp949"):
        try:
            return _order_dates_from_frame(pd.read_csv(csv_path, dtype=str, encoding=enc))
        except Exception:
            continue
    logger.warning("수동 배민 주문일 스캔 실패: %s", csv_path.name)
    return []


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


def normalize_baemin_now_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Keep existing NOW values and guarantee new schema columns."""
    out = df.fillna("").astype(str).copy()
    out = out.drop(columns=["collection_status"], errors="ignore")
    out = out.mask(out.apply(lambda col: col.str.strip().str.lower().isin(_EMPTY_TOKENS)))
    out = out.fillna("")
    for col in BAEMIN_NOW_SCHEMA_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    for col in BAEMIN_NOW_NEW_COLUMNS:
        out.loc[out[col].fillna("").astype(str).str.strip().str.lower().isin(_EMPTY_TOKENS), col] = ""
    out = _normalize_baemin_now_brand_store(out)

    ordered = [
        col
        for col in BAEMIN_NOW_SCHEMA_COLUMNS
        if col in out.columns and col not in BAEMIN_NOW_ID_COLUMNS
    ]
    extras = [col for col in out.columns if col not in ordered and col not in BAEMIN_NOW_ID_COLUMNS]
    ids = [col for col in BAEMIN_NOW_ID_COLUMNS if col in out.columns]
    return out[ordered + extras + ids]


def _normalize_baemin_now_brand_store(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("brand", "store", "brand_store"):
        if col not in out.columns:
            out[col] = ""

    if "store_name" in out.columns:
        inferred = out["store_name"].map(_baemin_now_brand_store)
        brand_blank = out["brand"].fillna("").astype(str).str.strip().eq("")
        store_blank = out["store"].fillna("").astype(str).str.strip().eq("")
        out.loc[brand_blank, "brand"] = inferred.loc[brand_blank].map(lambda item: item[0])
        out.loc[store_blank, "store"] = inferred.loc[store_blank].map(lambda item: item[1])

    brand = out["brand"].fillna("").astype(str).str.strip()
    store = out["store"].fillna("").astype(str).str.strip()
    combined = brand + "|" + store
    out["brand_store"] = combined.where(brand.ne("") & store.ne(""), "")
    return out


def backfill_baemin_now_schema_only(dry_run: bool = True) -> dict:
    """Add new NOW columns to stored baemin_now.csv files without changing rows."""
    files = sorted(BAEMIN_METRICS_DB.glob("brand=*/store=*/ym=*/baemin_now.csv"))
    scanned = len(files)
    targets: list[str] = []
    changed = 0
    errors: list[str] = []

    for path in files:
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", keep_default_na=False)
            normalized = normalize_baemin_now_schema(df)
            missing = [col for col in BAEMIN_NOW_NEW_COLUMNS if col not in df.columns]
            if not missing and list(normalized.columns) == list(df.columns):
                continue
            targets.append(str(path))
            if dry_run:
                continue
            tmp = path.with_suffix(".csv.tmp")
            normalized.to_csv(tmp, index=False, encoding="utf-8-sig")
            tmp.replace(path)
            changed += 1
        except Exception as exc:
            errors.append(f"{path}: {exc}")
            logger.warning("baemin_now schema backfill failed: %s / %s", path, exc)

    result = {
        "dry_run": dry_run,
        "scanned": scanned,
        "targets": len(targets),
        "changed": changed,
        "errors": errors,
    }
    logger.info("baemin_now schema backfill result: %s", result)
    return result


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
        combined = normalize_baemin_now_schema(combined)
        combined.to_csv(out_path, index=False, encoding="utf-8-sig")
        outputs.append(str(out_path))
        logger.info("manual baemin metrics saved: %s rows=%d", out_path, len(combined))
    return outputs


def _load_one_file(csv_path: Path, force: bool = False) -> tuple[int, bool]:
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

    raw_ym = order_ym(df["주문시각"])
    raw_dates = order_date(df["주문시각"])
    valid_months = sorted({ym for ym in raw_ym.tolist() if ym})
    if not valid_months:
        logger.warning("유효 주문일자 없음, 구간 교체 스킵: %s", csv_path.name)
        return 0, True

    target_df = df[df["주문상태"].astype(str).str.strip() == _STATUS_DELIVERED].copy()
    target_df["collected_at"] = pendulum.now("Asia/Seoul").isoformat()
    target_df["store_name"] = store_key
    target_ym = order_ym(target_df["주문시각"])
    if target_df.empty:
        logger.warning("배달완료 0건 파일로 구간 비움: %s", csv_path.name)

    total_rows = 0
    pending_writes: list[tuple[str, pd.DataFrame, pd.DataFrame, dict, Path]] = []
    for ym in valid_months:
        coverage_dates = raw_dates[raw_ym.eq(ym)].reset_index(drop=True)
        group = target_df[target_ym.eq(ym)].reset_index(drop=True)
        new_df = pd.DataFrame("", index=range(len(group)), columns=_COLUMNS, dtype=str)
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
        combined, info = _replace_manual_orders(existing, new_df, coverage_dates)
        shrink_dates = sorted(set(info.get("shrunk_dates") or []) | set(info.get("partial_shrunk_dates") or []))
        settlement_regressions = _settlement_regression_dates(existing, new_df)
        discount_regressions = _discount_regression_dates(existing, new_df)
        if discount_regressions:
            logger.warning(
                "즉시할인 분해 후퇴 감지(적재는 계속): %s/%s ym=%s details=%s",
                brand,
                store_key,
                ym,
                discount_regressions,
            )
        if not force and shrink_dates:
            combined, info = _preserve_missing_existing_orders(
                existing,
                new_df,
                coverage_dates,
                combined,
                info,
            )
            logger.warning(
                "재수집 구간 축소 감지, 누락 주문번호는 기존값 보존 후 적재: "
                "%s/%s ym=%s dates=%s details=%s preserved=%s",
                brand,
                store_key,
                ym,
                shrink_dates,
                info.get("shrink_details", {}),
                info.get("preserved_missing_orders", {}),
            )
        if not force and settlement_regressions:
            logger.warning(
                "정산정보 후퇴 의심으로 적재 차단: %s/%s ym=%s details=%s written_before=%s",
                brand,
                store_key,
                ym,
                settlement_regressions,
                [item[0] for item in pending_writes],
            )
            return 0, False
        if force and (shrink_dates or settlement_regressions):
            logger.warning(
                "force_shrink=true로 재수집 가드 우회: %s/%s ym=%s shrink=%s settlement_regression=%s",
                brand,
                store_key,
                ym,
                shrink_dates,
                settlement_regressions,
            )
        pending_writes.append((f"{brand}/{store_key}/{ym}", combined, new_df, info, stem))

    for partition, combined, new_df, info, stem in pending_writes:
        out_path = write_table(combined, stem)
        _record_reingest_dates(store_key, new_df, info)
        total_rows += len(new_df)
        logger.info("ingest complete: %s rows=%d -> %s", partition, len(new_df), out_path)

    return total_rows, True


def load_manual_baemin_orders(**context) -> str:
    """Load `baemin_orders_*.csv` files under DOWN_DIR/COLLECT_SRC into BAEMIN_ORDERS_DB."""
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    force = _truthy_conf(conf.get("force_shrink"))
    files = _iter_manual_order_files()
    if not files:
        logger.info("대상 파일 없음: %s, %s", DOWN_DIR, COLLECT_SRC)
        return json.dumps({"loaded_files": [], "skipped_files": []}, ensure_ascii=False)

    loaded: list[dict[str, str]] = []
    skipped: list[str] = []
    partial_files: list[str] = []
    order_dates: set[str] = set()
    for item in files:
        csv_path = Path(item["path"])
        if _is_partial_order_file(csv_path):
            partial_files.append(str(csv_path))
            logger.warning("partial 수동 배민 orders 파일 적재 제외: %s", csv_path.name)
            continue
        rows, ok = _load_one_file(csv_path, force=force)
        if ok:
            dates = _scan_manual_order_dates(csv_path)
            order_dates.update(dates)
            loaded.append({**item, "dates": dates})
            logger.info("로드 성공: %s (%d rows)", csv_path.name, rows)
        else:
            skipped.append(str(csv_path))
            logger.warning("로드 실패(skip): %s", csv_path.name)

    return json.dumps(
        {
            "loaded_files": loaded,
            "skipped_files": skipped,
            "partial_files": partial_files,
            "order_dates": sorted(order_dates),
        },
        ensure_ascii=False,
    )


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
            "skipped_orders": orders.get("skipped_files", []),
            "skipped_marketing": marketing.get("skipped_files", []),
            "skipped_metrics": metrics.get("skipped_files", []),
            "partial_orders": orders.get("partial_files", []),
            "order_dates": orders.get("order_dates", []),
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
