"""Build compact store-month JSON payloads for GPT-facing analysis.

The output is intentionally aggregated. Downstream GPT bots should read these
JSON files instead of scanning raw CSV/parquet sources.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import re
import time
from contextvars import ContextVar
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import pyarrow.parquet as pq

from modules.transform.utility.paths import (
    ANALYTICS_DB,
    DELIVERY_COMMISSION_PATH,
    FLOW_VISIT_VIZ_PARQUET,
    MART_DB,
)

logger = logging.getLogger(__name__)

FOR_AI_ROOT = MART_DB / "For_AI"
STORE_SALES_TARGET_CSV = ANALYTICS_DB / "store_sales_target" / "target.csv"
STORE_SALES_DAILY_ACTUALS_CSV = ANALYTICS_DB / "store_sales_target" / "daily_actuals.csv"
UNIFIED_SALES_DIR = MART_DB / "unified_sales_grp"
UNIFIED_REVIEW_DIR = MART_DB / "unified_review"
BAEMIN_WOORI_DIR = ANALYTICS_DB / "baemin_macro" / "metrics_our_store_clicks"
BAEMIN_NOW_DIR = ANALYTICS_DB / "baemin_macro" / "metrics_now"
BAEMIN_AD_FUNNEL_DIR = ANALYTICS_DB / "baemin_macro" / "ad_funnel"
BAEMIN_MARKETING_DIR = ANALYTICS_DB / "baemin_marketing"
COUPANG_MARKETING_DIR = ANALYTICS_DB / "coupang_marketing"
DELIVERY_COMMISSION_NUMERIC_COLS = [
    "total_amt",
    "settlement_amount",
    "diff_amt",
    "배민_즉시할인",
    "우가클_평균비용",
    "우가클_주문수",
    "우가클_클릭율",
    "쿠팡_신규비율",
    "쿠팡_재주문비율",
    "쿠팡_광고비용",
    "쿠팡_신규고객",
    "쿠팡_광고노출수",
    "쿠팡_광고클릭수",
]

NUMERIC_SALES_COLS = [
    "홀매출",
    "홀_테이블_매출",
    "홀_포장_매출",
    "배달매출",
    "총매출",
    "테이블수",
    "테이블단가",
]
TARGET_NUMERIC_COLS = [
    "홀_월목표매출",
    "홀_테이블_월목표매출",
    "홀_포장_월목표매출",
    "배달_월목표매출",
    "전체_월목표매출",
    "평일_일목표매출",
    "주말_일목표매출",
    "목표_테이블단가",
    "테이블수",
]
MARKETING_NUMERIC_COLS = ["광고지출", "노출수", "클릭수", "주문수", "주문금액", "광고효과"]
COUPANG_MARKETING_NUMERIC_COLS = [
    "광고비용",
    "신규고객",
    "광고주문수",
    "광고매출",
    "전체매출",
    "광고클릭수",
    "광고노출수",
]
NOW_NUMERIC_COLS = ["조리소요시간", "주문접수시간", "최근재주문율", "조리시간준수율", "주문접수율", "최근별점"]
MENU_NAME_COL_CANDIDATES = ["menu_name", "product_name", "item_name", "product_nm", "name", "상품명", "메뉴명"]
MENU_TOP_N = 20
VISIT_HISTORY_LIMIT = int(os.getenv("FOR_AI_VISIT_HISTORY_LIMIT", "0"))
SPLIT_JSON_FILES = {
    "orders": "orders.json",
    "ads": "ads.json",
    "visit_log": "visit_log.json",
}
PARQUET_BATCH_SIZE = 8192
_CSV_PATH_CACHE: ContextVar[dict[tuple[Path, str], list[Path]] | None] = ContextVar(
    "for_ai_csv_path_cache", default=None
)


def build_for_ai_store_month_analysis(
    *,
    brand: str | None = None,
    ym: str | None = None,
    store: str | None = None,
    output_root: str | Path | None = None,
    write: bool = True,
    lookback: int | None = None,
    target_source: str = "daily_actuals",
) -> str:
    """Generate compact JSON payloads.

    With daily_actuals target source, only the latest month is generated when
    no filters are passed. With orders target source, saved unified_sales orders
    decide the brand/store/month targets.
    """
    output_dir = Path(output_root) if output_root is not None else FOR_AI_ROOT
    daily = _load_daily_actuals()
    if daily.empty and target_source != "orders":
        return "SKIP: daily_actuals.csv 없음 또는 데이터 없음"

    if target_source == "orders":
        targets = _target_keys_from_orders(brand=brand, ym=ym, store=store, lookback=lookback)
        if not targets and not daily.empty:
            logger.warning("unified_sales 주문 기준 생성 대상 없음, daily_actuals 기준으로 대체합니다.")
            targets = _target_keys(daily, brand=brand, ym=ym, store=store)
    else:
        targets = _target_keys(daily, brand=brand, ym=ym, store=store)
    if not targets:
        return "SKIP: 생성 대상 없음"

    target_df = _load_target()
    index_items: list[dict[str, Any]] = []
    started = time.monotonic()
    logger.info("For_AI 분석 시작: 대상 %d개, write=%s", len(targets), write)
    cache_token = _CSV_PATH_CACHE.set({})
    try:
        for count, item in enumerate(targets, 1):
            split_payloads = build_store_month_payloads(
                brand=item["brand"],
                ym=item["ym"],
                store=item["store"],
                daily=daily,
                target=target_df,
            )
            payload = split_payloads["analysis"]
            if write:
                path = _analysis_path(output_dir, item["brand"], item["ym"], item["store"])
                _write_json(path, payload)
                _write_json(path.with_name(SPLIT_JSON_FILES["orders"]), split_payloads["orders"])
                _write_json(path.with_name(SPLIT_JSON_FILES["ads"]), split_payloads["ads"])
                _write_json(path.with_name(SPLIT_JSON_FILES["visit_log"]), split_payloads["visit_log"])
                _write_store_readme(path.with_name("README.md"), payload)
                index_items.append(_index_item(output_dir, payload))
            del payload, split_payloads
            if count == 1 or count % 25 == 0 or count == len(targets):
                logger.info("For_AI 분석 진행: %d/%d, %.1f초", count, len(targets), time.monotonic() - started)
    finally:
        _CSV_PATH_CACHE.reset(cache_token)

    if write:
        _write_indexes(output_dir, index_items)
        return f"OK: For_AI JSON {len(index_items)}개 생성 -> {output_dir}"
    return f"OK: For_AI JSON payload {len(targets)}개 생성(dry-run)"


def build_store_month_payload(
    *,
    brand: str,
    ym: str,
    store: str,
    daily: pd.DataFrame | None = None,
    target: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Return the GPT entrypoint analysis payload for compatibility."""
    return build_store_month_payloads(brand=brand, ym=ym, store=store, daily=daily, target=target)["analysis"]


def build_store_month_payloads(
    *,
    brand: str,
    ym: str,
    store: str,
    daily: pd.DataFrame | None = None,
    target: pd.DataFrame | None = None,
) -> dict[str, dict[str, Any]]:
    daily_df = _ensure_daily_actuals_schema(_load_daily_actuals() if daily is None else daily.copy())
    target_df = _load_target() if target is None else target.copy()
    brand = _clean_text(brand)
    ym = _normalize_ym(ym)
    store = _clean_text(store)

    month_daily = daily_df[
        daily_df["브랜드"].eq(brand)
        & daily_df["기준월"].eq(ym)
        & daily_df["매장명"].eq(store)
    ].copy()
    target_row = _month_target(target_df, brand=brand, ym=ym, store=store)
    unified = _load_unified_month(brand=brand, ym=ym, store=store)
    missing_sources: list[str] = []

    if month_daily.empty:
        missing_sources.append("store_sales_target/daily_actuals")
    if target_row is None:
        missing_sources.append("store_sales_target/target")
    if unified.empty:
        missing_sources.append("unified_sales_grp")

    baemin_woori = _load_partitioned_csv(BAEMIN_WOORI_DIR, brand=brand, ym=ym, store=store, filename="woori_shop_click.csv")
    baemin_now = _load_partitioned_csv(BAEMIN_NOW_DIR, brand=brand, ym=ym, store=store, filename="baemin_now.csv")
    baemin_ad = _load_partitioned_csv(BAEMIN_AD_FUNNEL_DIR, brand=brand, ym=ym, store=store, filename="baemin_ad_funnel.csv")
    baemin_marketing = _load_partitioned_csv(BAEMIN_MARKETING_DIR, brand=brand, ym=ym, store=store, filename="baemin_marketing_data.csv")
    coupang_marketing = _load_partitioned_csv(COUPANG_MARKETING_DIR, brand=brand, ym=ym, store=store, filename="data.csv")
    reviews = _load_review_month(ym=ym, store=store)
    commission = _load_delivery_commission_month(brand=brand, ym=ym, store=store)
    visits = _load_flow_visit_month(ym=ym, store=store)

    for source_name, df in [
        ("baemin_macro/metrics_our_store_clicks", baemin_woori),
        ("baemin_macro/metrics_now", baemin_now),
        ("baemin_macro/ad_funnel", baemin_ad),
        ("baemin_marketing", baemin_marketing),
        ("coupang_marketing", coupang_marketing),
        ("unified_review", reviews),
        ("delivery_commission", commission),
        ("flow_visit_viz", visits),
    ]:
        if df.empty:
            missing_sources.append(source_name)

    month_summary = _month_summary(month_daily, target_row)
    channel_summary = _channel_summary(month_daily, unified)
    week_summary = _week_summary(month_daily, target_row)
    risk_days = _risk_days(month_daily, target_row)
    marketing_summary = {
        "baemin_woori_click": _marketing_summary(baemin_woori, date_col="날짜", numeric_cols=MARKETING_NUMERIC_COLS),
        "baemin_ad_funnel": _marketing_summary(baemin_ad, date_col="target_date", numeric_cols=["노출수", "클릭수", "주문수", "주문금액"]),
        "baemin_marketing": _marketing_summary(baemin_marketing, date_col="날짜", numeric_cols=MARKETING_NUMERIC_COLS),
        "coupang_marketing": _marketing_summary(coupang_marketing, date_col="조회일자", numeric_cols=COUPANG_MARKETING_NUMERIC_COLS),
    }
    operation_summary = _operation_summary(baemin_now)
    review_summary = _review_summary(reviews)
    commission_summary = _commission_summary(commission)
    visit_summary = _visit_summary(visits)
    visit_summary["current_month_visit_count"] = _visit_count(_current_month_visit_rows(visits, ym))
    visit_summary["visit_history_limit"] = VISIT_HISTORY_LIMIT or None
    visit_summary["visit_history_scope"] = "all_until_month_end" if VISIT_HISTORY_LIMIT <= 0 else "limited_until_month_end"
    orders_payload = _orders_payload(
        brand=brand,
        store=store,
        ym=ym,
        month_daily=month_daily,
        unified=unified,
        missing_sources=missing_sources,
    )
    ads_payload = _ads_payload(
        brand=brand,
        store=store,
        ym=ym,
        marketing_summary=marketing_summary,
        commission_summary=commission_summary,
    )
    visit_log_payload = _visit_log_payload(
        brand=brand,
        store=store,
        ym=ym,
        visits=visits,
        visit_summary=visit_summary,
    )
    source_coverage = _source_coverage(
        {
            "daily_actuals": month_daily,
            "unified_sales": unified,
            "baemin_woori_click": baemin_woori,
            "baemin_now": baemin_now,
            "baemin_ad_funnel": baemin_ad,
            "baemin_marketing": baemin_marketing,
            "coupang_marketing": coupang_marketing,
            "unified_review": reviews,
            "delivery_commission": commission,
            "flow_visit_viz": visits,
        }
    )
    file_manifest = {
        "analysis": {
            "path": "analysis.json",
            "description": "월간 핵심 요약과 GPT 진입점",
        },
        "orders": {
            "path": SPLIT_JSON_FILES["orders"],
            "description": "일별/채널별/메뉴별 주문수, 객단가, 매출 집계",
            "rows": _payload_rows(orders_payload),
        },
        "ads": {
            "path": SPLIT_JSON_FILES["ads"],
            "description": "광고와 수수료성 광고 지표 집계",
            "rows": _payload_rows(ads_payload),
        },
        "visit_log": {
            "path": SPLIT_JSON_FILES["visit_log"],
            "description": "월별 방문일지 요약과 방문별 이슈",
            "rows": _payload_rows(visit_log_payload),
        },
    }

    analysis_payload = {
        "meta": {
            "brand": brand,
            "store": store,
            "ym": ym,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_coverage": source_coverage,
            "missing_sources": sorted(set(missing_sources)),
            "files": file_manifest,
        },
        "month_summary": month_summary,
        "week_summary": week_summary,
        "channel_summary": channel_summary,
        "marketing_summary": marketing_summary,
        "commission_summary": commission_summary,
        "operation_summary": operation_summary,
        "review_summary": review_summary,
        "visit_summary": visit_summary,
        "risk_days": risk_days,
        "gpt_context": _gpt_context(
            brand=brand,
            store=store,
            ym=ym,
            month_summary=month_summary,
            channel_summary=channel_summary,
            marketing_summary=marketing_summary,
            commission_summary=commission_summary,
            operation_summary=operation_summary,
            review_summary=review_summary,
            visit_summary=visit_summary,
            missing_sources=missing_sources,
        ),
        "recommended_prompt": _recommended_prompt(),
    }
    return {
        "analysis": analysis_payload,
        "orders": orders_payload,
        "ads": ads_payload,
        "visit_log": visit_log_payload,
    }


def _load_daily_actuals() -> pd.DataFrame:
    if not STORE_SALES_DAILY_ACTUALS_CSV.exists():
        return _ensure_daily_actuals_schema(pd.DataFrame())
    df = pd.read_csv(STORE_SALES_DAILY_ACTUALS_CSV, encoding="utf-8-sig", dtype=str)
    df.columns = df.columns.map(lambda c: str(c).strip())
    df = _normalize_text_cols(df, ["기준월", "매장명", "브랜드", "요일구분"])
    df["기준월"] = df["기준월"].map(_normalize_ym)
    if "매출일자" in df.columns:
        df["매출일자"] = _parse_dates(df["매출일자"]).dt.strftime("%Y-%m-%d")
    return _ensure_daily_actuals_schema(_parse_numeric_cols(df, NUMERIC_SALES_COLS))


def _ensure_daily_actuals_schema(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["브랜드", "기준월", "매장명", "매출일자", "요일구분", *NUMERIC_SALES_COLS]:
        if col not in out.columns:
            out[col] = "" if col not in NUMERIC_SALES_COLS else 0
    return out


def _load_target() -> pd.DataFrame:
    if not STORE_SALES_TARGET_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(STORE_SALES_TARGET_CSV, encoding="utf-8-sig", dtype=str)
    df.columns = df.columns.map(lambda c: str(c).strip())
    df = _normalize_text_cols(df, ["기준월", "매장명", "브랜드"])
    df["기준월"] = df["기준월"].map(_normalize_ym)
    return _parse_numeric_cols(df, TARGET_NUMERIC_COLS)


def _target_keys(daily: pd.DataFrame, *, brand: str | None, ym: str | None, store: str | None) -> list[dict[str, str]]:
    work = daily.copy()
    if brand:
        work = work[work["브랜드"].eq(_clean_text(brand))]
    if ym:
        work = work[work["기준월"].eq(_normalize_ym(ym))]
    elif not work.empty:
        latest_ym = sorted(work["기준월"].dropna().unique())[-1]
        work = work[work["기준월"].eq(latest_ym)]
    if store:
        store_text = _clean_text(store)
        work = work[work["매장명"].eq(store_text) | work["매장명"].str.contains(store_text, regex=False, na=False)]

    keys = (
        work[["브랜드", "기준월", "매장명"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["브랜드", "기준월", "매장명"])
    )
    return [
        {"brand": str(row["브랜드"]), "ym": str(row["기준월"]), "store": str(row["매장명"])}
        for _, row in keys.iterrows()
    ]


def _raise_if_memory_error(exc: Exception) -> None:
    if isinstance(exc, MemoryError) or isinstance(exc, OSError) and exc.errno == errno.ENOMEM:
        raise exc


def _unified_batches(path: Path, *, keys_only: bool = False) -> Iterator[pd.DataFrame]:
    columns = ["brand", "ym", "store", "sale_date"]
    if not keys_only:
        columns += ["source", "platform", "order_type", "order_id", "total_price", "order_cnt"]
        columns += MENU_NAME_COL_CANDIDATES
    with pq.ParquetFile(path) as parquet:
        selected = [name for name in parquet.schema_arrow.names if name.strip() in columns]
        for batch in parquet.iter_batches(batch_size=PARQUET_BATCH_SIZE, columns=selected, use_threads=False):
            df = batch.to_pandas(use_threads=False)
            df.columns = df.columns.map(lambda name: str(name).strip())
            yield df


def _target_keys_from_orders(
    *,
    brand: str | None,
    ym: str | None,
    store: str | None,
    lookback: int | None,
) -> list[dict[str, str]]:
    files = sorted(UNIFIED_SALES_DIR.glob("unified_sales_*.parquet")) if UNIFIED_SALES_DIR.exists() else []
    if not files:
        return []
    all_keys: set[tuple[str, str, str]] = set()
    for path in files:
        try:
            file_keys: set[tuple[str, str, str]] = set()
            for df in _unified_batches(path, keys_only=True):
                for col in ["brand", "ym", "store", "sale_date"]:
                    if col not in df.columns:
                        df[col] = ""
                work = _normalize_text_cols(df, ["ym", "brand", "store", "sale_date"])
                missing_ym = work["ym"].eq("")
                if missing_ym.any():
                    work.loc[missing_ym, "ym"] = _parse_dates(work.loc[missing_ym, "sale_date"]).dt.strftime("%Y-%m")
                keys = work[["brand", "ym", "store"]].replace("", pd.NA).dropna().drop_duplicates()
                file_keys.update(keys.itertuples(index=False, name=None))
            all_keys.update(file_keys)
        except Exception as exc:
            _raise_if_memory_error(exc)
            logger.warning("unified_sales 대상 키 읽기 실패: %s | %s", path, exc)
            continue
    if not all_keys:
        return []

    work = pd.DataFrame(sorted(all_keys), columns=["brand", "ym", "store"])
    if brand:
        work = work[work["brand"].eq(_clean_text(brand))]
    if ym:
        work = work[work["ym"].eq(_normalize_ym(ym))]
    elif lookback is not None:
        recent_months = sorted(work["ym"].dropna().unique())[-int(lookback) :]
        work = work[work["ym"].isin(recent_months)]
    if store:
        store_text = _clean_text(store)
        work = work[work["store"].eq(store_text) | work["store"].str.contains(store_text, regex=False, na=False)]

    work = work.sort_values(["brand", "ym", "store"])
    return [
        {"brand": str(row["brand"]), "ym": str(row["ym"]), "store": str(row["store"])}
        for _, row in work.iterrows()
    ]


def _month_target(target: pd.DataFrame, *, brand: str, ym: str, store: str) -> dict[str, Any] | None:
    if target.empty:
        return None
    matched = target[target["브랜드"].eq(brand) & target["기준월"].eq(ym) & target["매장명"].eq(store)]
    if matched.empty:
        return None
    row = matched.iloc[-1].to_dict()
    return {key: _json_value(value) for key, value in row.items()}


def _ensure_unified_sales_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.map(lambda c: str(c).strip())
    for col in ["sale_date", "ym", "source", "brand", "store", "platform", "order_type", "order_id", "total_price", "order_cnt"]:
        if col not in out.columns:
            out[col] = "" if col not in ["total_price", "order_cnt"] else 0
    menu_col = next((col for col in MENU_NAME_COL_CANDIDATES if col in out.columns), None)
    out["menu_name"] = out[menu_col] if menu_col else ""
    return out[
        ["sale_date", "ym", "source", "brand", "store", "platform", "order_type", "order_id", "menu_name", "total_price", "order_cnt"]
    ]


def _load_unified_month(*, brand: str, ym: str, store: str) -> pd.DataFrame:
    files = _month_parquet_files(UNIFIED_SALES_DIR, "unified_sales_", ym)
    parts = []
    for path in files:
        try:
            file_parts = []
            for df in _unified_batches(path):
                df = _ensure_unified_sales_cols(df)
                df = _normalize_text_cols(df, ["ym", "brand", "store"])
                matched = df[df["brand"].eq(brand) & df["ym"].eq(ym) & df["store"].eq(store)].copy()
                if not matched.empty:
                    file_parts.append(_normalize_text_cols(matched, ["source", "platform", "order_type", "sale_date", "order_id", "menu_name"]))
            # Preserve file-wide numeric inference and all-or-nothing read failures.
            if file_parts:
                parts.append(_parse_numeric_cols(pd.concat(file_parts, ignore_index=True), ["total_price", "order_cnt"]))
        except Exception as exc:
            _raise_if_memory_error(exc)
            logger.warning("unified_sales 읽기 실패: %s | %s", path, exc)
            continue
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _load_review_month(*, ym: str, store: str) -> pd.DataFrame:
    files = _month_parquet_files(UNIFIED_REVIEW_DIR, "unified_review_", ym)
    parts = []
    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            _raise_if_memory_error(exc)
            logger.warning("unified_review 읽기 실패: %s | %s", path, exc)
            continue
        store_col = "매장명" if "매장명" in df.columns else "store" if "store" in df.columns else None
        if not store_col:
            continue
        df = _normalize_text_cols(df, [store_col])
        matched = df[df[store_col].eq(store)].copy()
        if not matched.empty:
            parts.append(matched)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _load_delivery_commission_month(*, brand: str, ym: str, store: str) -> pd.DataFrame:
    if not DELIVERY_COMMISSION_PATH.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(DELIVERY_COMMISSION_PATH)
    except Exception as exc:
        _raise_if_memory_error(exc)
        logger.warning("delivery_commission 읽기 실패: %s | %s", DELIVERY_COMMISSION_PATH, exc)
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df = _normalize_text_cols(df, ["brand", "store", "platform", "sale_date"])
    df["ym"] = _parse_dates(df["sale_date"]).dt.strftime("%Y-%m")
    matched = df[df["brand"].eq(brand) & df["ym"].eq(ym) & df["store"].eq(store)].copy()
    return _parse_numeric_cols(matched, DELIVERY_COMMISSION_NUMERIC_COLS)


def _load_flow_visit_month(*, ym: str, store: str) -> pd.DataFrame:
    if not FLOW_VISIT_VIZ_PARQUET.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(FLOW_VISIT_VIZ_PARQUET)
    except Exception as exc:
        _raise_if_memory_error(exc)
        logger.warning("flow_visit_viz 읽기 실패: %s | %s", FLOW_VISIT_VIZ_PARQUET, exc)
        return pd.DataFrame()
    if df.empty or "store_name" not in df.columns:
        return pd.DataFrame()
    df = _normalize_text_cols(df, ["store_name", "visit_ym"])
    matched = df[df["store_name"].eq(store)].copy()
    if matched.empty or "visit_date" not in matched.columns:
        return pd.DataFrame()
    matched["_visit_date"] = _parse_dates(matched["visit_date"])
    cutoff = pd.Timestamp(f"{ym}-01") + pd.offsets.MonthEnd(0)
    matched = matched[matched["_visit_date"].notna() & (matched["_visit_date"] <= cutoff)].copy()
    if matched.empty:
        return pd.DataFrame()
    combined = _visit_history_rows(matched, limit=VISIT_HISTORY_LIMIT)
    dedup_cols = [col for col in ["visit_rel_key", "post_id", "issue_rel_key", "issue_label", "visit_date"] if col in combined.columns]
    if dedup_cols:
        combined = combined.drop_duplicates(subset=dedup_cols)
    return combined.drop(columns=["_visit_date"], errors="ignore")


def _load_partitioned_csv(root: Path, *, brand: str, ym: str, store: str, filename: str) -> pd.DataFrame:
    path = root / f"brand={brand}" / f"store={store}" / f"ym={ym}" / filename
    candidates = [path] if path.exists() else []
    if not candidates and root.exists():
        cache = _CSV_PATH_CACHE.get()
        key = (root, filename)
        if cache is not None:
            if key not in cache:
                cache[key] = list(root.rglob(filename))
            paths = cache[key]
        else:
            paths = root.rglob(filename)
        candidates = [
            candidate
            for candidate in paths
            if f"ym={ym}" in str(candidate) and _store_matches_path(candidate, store)
        ]
    parts = []
    for candidate in candidates:
        try:
            df = pd.read_csv(candidate, encoding="utf-8-sig", dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(candidate, encoding="cp949", dtype=str)
        except Exception as exc:
            _raise_if_memory_error(exc)
            logger.warning("CSV 읽기 실패: %s | %s", candidate, exc)
            continue
        df["_source_path"] = str(candidate)
        parts.append(df)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _month_parquet_files(root: Path, prefix: str, ym: str) -> list[Path]:
    if not root.exists():
        return []
    start = pd.Timestamp(f"{ym}-01")
    end = start + pd.offsets.MonthEnd(0)
    names = []
    current = start
    while current <= end:
        names.append(root / f"{prefix}{current.strftime('%y%m%d')}.parquet")
        current += pd.Timedelta(days=1)
    return [path for path in names if path.exists()]


def _visit_history_rows(df: pd.DataFrame, *, limit: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    work = df.copy()
    if "_visit_date" not in work.columns:
        work["_visit_date"] = _parse_dates(work["visit_date"]) if "visit_date" in work.columns else pd.NaT
    key_col = next((col for col in ["visit_rel_key", "post_id", "visit_date"] if col in work.columns), None)
    if limit <= 0:
        return work.sort_values("_visit_date", ascending=False).copy()
    if key_col is None:
        return work.sort_values("_visit_date", ascending=False).head(limit)
    visits = (
        work[[key_col, "_visit_date"]]
        .dropna(subset=["_visit_date"])
        .drop_duplicates()
        .sort_values("_visit_date", ascending=False)
        .head(limit)
    )
    return work[work[key_col].isin(visits[key_col])].copy()


def _current_month_visit_rows(df: pd.DataFrame, ym: str) -> pd.DataFrame:
    if df.empty or "visit_ym" not in df.columns:
        return pd.DataFrame()
    return df[df["visit_ym"].map(_clean_text).eq(ym)].copy()


def _month_summary(daily: pd.DataFrame, target_row: dict[str, Any] | None) -> dict[str, Any]:
    actual = _sum(daily, "총매출")
    hall = _sum(daily, "홀매출")
    table = _sum(daily, "홀_테이블_매출")
    takeout = _sum(daily, "홀_포장_매출")
    delivery = _sum(daily, "배달매출")
    target_total = int(target_row.get("전체_월목표매출") or 0) if target_row else 0
    days = int(daily["매출일자"].nunique()) if not daily.empty and "매출일자" in daily.columns else 0
    table_orders = _sum(daily, "테이블수")
    return {
        "target_sales": target_total,
        "actual_sales": actual,
        "sales_gap": actual - target_total if target_total else None,
        "achievement_rate": _ratio(actual, target_total),
        "sales_days": days,
        "daily_average_sales": round(actual / days) if days else 0,
        "hall_sales": hall,
        "table_sales": table,
        "takeout_sales": takeout,
        "delivery_sales": delivery,
        "table_orders": table_orders,
        "average_table_price": round(actual / table_orders) if table_orders else 0,
    }


def _week_summary(daily: pd.DataFrame, target_row: dict[str, Any] | None) -> list[dict[str, Any]]:
    if daily.empty:
        return []
    work = daily.copy()
    work["_date"] = pd.to_datetime(work["매출일자"], errors="coerce")
    work = work[work["_date"].notna()]
    if work.empty:
        return []
    work["week"] = work["_date"].dt.isocalendar().week.astype(int)
    rows = []
    prev_sales: int | None = None
    for week, group in work.groupby("week", sort=True):
        sales = _sum(group, "총매출")
        target = int(group.apply(lambda row: _target_for_day(row, target_row), axis=1).sum()) if target_row else 0
        rows.append(
            {
                "week": int(week),
                "date_from": group["_date"].min().strftime("%Y-%m-%d"),
                "date_to": group["_date"].max().strftime("%Y-%m-%d"),
                "actual_sales": sales,
                "target_sales": target,
                "achievement_rate": _ratio(sales, target),
                "wow_sales_change": None if prev_sales is None else sales - prev_sales,
            }
        )
        prev_sales = sales
    return rows


def _channel_summary(daily: pd.DataFrame, unified: pd.DataFrame) -> dict[str, Any]:
    actual = _sum(daily, "총매출")
    by_platform: dict[str, int] = {}
    by_source: dict[str, int] = {}
    if not unified.empty:
        platform_group = unified.groupby("platform", dropna=False)["total_price"].sum()
        source_group = unified.groupby("source", dropna=False)["total_price"].sum()
        by_platform = {str(k): int(v) for k, v in platform_group.items()}
        by_source = {str(k): int(v) for k, v in source_group.items()}
    summary = {
        "total_sales": actual,
        "hall_sales": _sum(daily, "홀매출"),
        "table_sales": _sum(daily, "홀_테이블_매출"),
        "takeout_sales": _sum(daily, "홀_포장_매출"),
        "delivery_sales": _sum(daily, "배달매출"),
        "by_platform": by_platform,
        "by_source": by_source,
    }
    for key in ["hall_sales", "takeout_sales", "delivery_sales"]:
        summary[f"{key}_share"] = _ratio(int(summary[key]), actual)
    return summary


def _orders_payload(
    *,
    brand: str,
    store: str,
    ym: str,
    month_daily: pd.DataFrame,
    unified: pd.DataFrame,
    missing_sources: list[str],
) -> dict[str, Any]:
    warnings = []
    if unified.empty:
        warnings.append("unified_sales_grp 원천이 없어 주문수와 메뉴별 집계를 만들 수 없습니다.")
    elif "menu_name" not in unified.columns or not unified["menu_name"].astype(str).str.strip().any():
        warnings.append("unified_sales_grp에 메뉴명 컬럼이 없어 메뉴별 집계는 '메뉴명 없음'으로 묶었습니다.")

    daily_summary = _orders_daily_summary(month_daily, unified)
    daily_channel_summary = _orders_group_summary(unified, ["sale_date", "brand", "platform", "order_type"])
    monthly_channel_summary = _orders_group_summary(unified, ["brand", "platform", "order_type"])
    daily_menu_summary = _compress_menu_rows(
        _orders_group_summary(_with_menu_name(unified), ["sale_date", "brand", "platform", "order_type", "menu_name"]),
        group_cols=["sale_date", "brand", "platform", "order_type"],
        top_n=MENU_TOP_N,
    )
    monthly_menu_summary = _compress_menu_rows(
        _orders_group_summary(_with_menu_name(unified), ["brand", "platform", "order_type", "menu_name"]),
        group_cols=["brand", "platform", "order_type"],
        top_n=MENU_TOP_N,
    )
    return {
        "meta": {
            "brand": brand,
            "store": store,
            "ym": ym,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "menu_top_n": MENU_TOP_N,
            "missing_sources": sorted({name for name in missing_sources if name in ["unified_sales_grp", "store_sales_target/daily_actuals"]}),
            "warnings": warnings,
        },
        "daily_summary": daily_summary,
        "daily_channel_summary": daily_channel_summary,
        "monthly_channel_summary": monthly_channel_summary,
        "daily_menu_summary": daily_menu_summary,
        "monthly_menu_summary": monthly_menu_summary,
    }


def _orders_daily_summary(month_daily: pd.DataFrame, unified: pd.DataFrame) -> list[dict[str, Any]]:
    if month_daily.empty and unified.empty:
        return []
    sales_by_day: dict[str, int] = {}
    if not month_daily.empty and "매출일자" in month_daily.columns:
        sales_by_day = {
            str(k): int(v)
            for k, v in month_daily.groupby("매출일자", dropna=False)["총매출"].sum().items()
        }
    order_rows = _orders_group_summary(unified, ["sale_date"])
    orders_by_day = {row["sale_date"]: int(row["order_count"] or 0) for row in order_rows}
    unified_sales_by_day = {row["sale_date"]: int(row["sales"] or 0) for row in order_rows}
    dates = sorted(set(sales_by_day) | set(orders_by_day))
    return [
        {
            "sale_date": date,
            "order_count": orders_by_day.get(date, 0),
            "average_order_value": _average_order_value(sales_by_day.get(date, unified_sales_by_day.get(date, 0)), orders_by_day.get(date, 0)),
            "sales": sales_by_day.get(date, unified_sales_by_day.get(date, 0)),
        }
        for date in dates
    ]


def _orders_group_summary(df: pd.DataFrame, group_cols: list[str]) -> list[dict[str, Any]]:
    if df.empty:
        return []
    work = _with_menu_name(df.copy())
    work = _parse_numeric_cols(work, ["total_price", "order_cnt"])
    for col in group_cols:
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].map(_clean_text)
    rows = []
    for keys, group in work.groupby(group_cols, dropna=False, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        sales = _sum(group, "total_price")
        order_count = _order_count(group)
        row = {col: _json_value(value) for col, value in zip(group_cols, keys)}
        row.update(
            {
                "order_count": order_count,
                "average_order_value": _average_order_value(sales, order_count),
                "sales": sales,
            }
        )
        rows.append(row)
    return rows


def _compress_menu_rows(rows: list[dict[str, Any]], *, group_cols: list[str], top_n: int) -> list[dict[str, Any]]:
    if not rows:
        return []
    df = pd.DataFrame(rows)
    out = []
    for _, group in df.groupby(group_cols, dropna=False, sort=True):
        sorted_group = group.sort_values("sales", ascending=False)
        top = sorted_group.head(top_n).copy()
        rest = sorted_group.iloc[top_n:].copy()
        total_sales = int(sorted_group["sales"].sum())
        for record in top.to_dict("records"):
            record["sales_share"] = _ratio(int(record.get("sales") or 0), total_sales)
            out.append({key: _json_value(value) for key, value in record.items()})
        if not rest.empty:
            rest_sales = int(rest["sales"].sum())
            rest_orders = int(rest["order_count"].sum())
            other = {col: _json_value(rest.iloc[0].get(col)) for col in group_cols}
            other.update(
                {
                    "menu_name": "기타",
                    "order_count": rest_orders,
                    "average_order_value": _average_order_value(rest_sales, rest_orders),
                    "sales": rest_sales,
                    "sales_share": _ratio(rest_sales, total_sales),
                }
            )
            out.append(other)
    return out


def _with_menu_name(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "menu_name" not in out.columns:
        out["menu_name"] = ""
    out["menu_name"] = out["menu_name"].map(_clean_text).replace("", "메뉴명 없음")
    return out


def _order_count(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    if "order_id" in df.columns and df["order_id"].map(_clean_text).replace("", pd.NA).notna().any():
        return int(df["order_id"].map(_clean_text).replace("", pd.NA).dropna().nunique())
    if "order_cnt" in df.columns:
        return _sum(df, "order_cnt")
    return 0


def _average_order_value(sales: int | float, order_count: int | float) -> int:
    return round(float(sales or 0) / float(order_count or 0)) if float(order_count or 0) else 0


def _ads_payload(
    *,
    brand: str,
    store: str,
    ym: str,
    marketing_summary: dict[str, Any],
    commission_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "meta": {
            "brand": brand,
            "store": store,
            "ym": ym,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "monthly_summary": marketing_summary,
        "commission_ad_summary": {
            "coverage": commission_summary.get("coverage", {}),
            "coupang_ad_cost": commission_summary.get("totals", {}).get("coupang_ad_cost"),
            "coupang_new_customers": commission_summary.get("totals", {}).get("coupang_new_customers"),
            "coupang_ad_impressions": commission_summary.get("totals", {}).get("coupang_ad_impressions"),
            "coupang_ad_clicks": commission_summary.get("totals", {}).get("coupang_ad_clicks"),
            "baemin_instant_discount": commission_summary.get("totals", {}).get("baemin_instant_discount"),
        },
    }


def _visit_log_payload(
    *,
    brand: str,
    store: str,
    ym: str,
    visits: pd.DataFrame,
    visit_summary: dict[str, Any],
) -> dict[str, Any]:
    current_month = _current_month_visit_rows(visits, ym)
    history = _visit_history_rows(visits, limit=VISIT_HISTORY_LIMIT)
    current_month_rows = _visit_records(current_month)
    history_rows = _visit_records(history)
    return {
        "meta": {
            "brand": brand,
            "store": store,
            "ym": ym,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "visit_history_limit": VISIT_HISTORY_LIMIT or None,
            "visit_history_scope": "all_until_month_end" if VISIT_HISTORY_LIMIT <= 0 else "limited_until_month_end",
        },
        "summary": visit_summary,
        "current_month_summary": _visit_summary(current_month),
        "recent_summary": _visit_summary(history),
        "history_summary": _visit_summary(history),
        "issue_summary": {
            "by_category": visit_summary.get("issue_counts", {}),
            "top_issues": visit_summary.get("top_issues", []),
        },
        "visit_timeline": _visit_timeline(visits),
        "current_month_visits": current_month_rows,
        "recent_visits": history_rows,
        "visits": history_rows,
    }


def _payload_rows(payload: dict[str, Any]) -> int:
    total = 0
    for value in payload.values():
        if isinstance(value, list):
            total += len(value)
        elif isinstance(value, dict):
            total += _payload_rows(value)
    return total


def _risk_days(daily: pd.DataFrame, target_row: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    if daily.empty:
        return {"lowest_achievement_days": [], "largest_sales_drop_days": []}
    work = daily.copy()
    work["target_sales"] = work.apply(lambda row: _target_for_day(row, target_row), axis=1)
    work["achievement_rate"] = work.apply(lambda row: _ratio(int(row.get("총매출") or 0), int(row.get("target_sales") or 0)), axis=1)
    work["sales_gap"] = pd.to_numeric(work["총매출"], errors="coerce").fillna(0).astype(int) - work["target_sales"].fillna(0).astype(int)
    lowest = work.sort_values(["achievement_rate", "총매출"], ascending=[True, True]).head(5)
    work["_prev_sales"] = pd.to_numeric(work["총매출"], errors="coerce").fillna(0).astype(int).shift(1)
    work["sales_drop"] = pd.to_numeric(work["총매출"], errors="coerce").fillna(0).astype(int) - work["_prev_sales"].fillna(0).astype(int)
    drops = work[work["_prev_sales"].notna()].sort_values("sales_drop", ascending=True).head(5)
    cols = ["매출일자", "요일구분", "target_sales", "총매출", "achievement_rate", "sales_gap"]
    return {
        "lowest_achievement_days": [_clean_day_record(row) for row in lowest.reindex(columns=cols).to_dict("records")],
        "largest_sales_drop_days": [_clean_day_record(row) for row in drops.reindex(columns=cols + ["sales_drop"]).to_dict("records")],
    }


def _marketing_summary(df: pd.DataFrame, *, date_col: str, numeric_cols: list[str]) -> dict[str, Any]:
    if df.empty:
        return {"coverage": {"rows": 0}, "totals": {}, "derived": {}}
    work = df.copy()
    work.columns = work.columns.map(lambda c: str(c).strip())
    work = _parse_numeric_cols(work, numeric_cols)
    dates = _date_bounds(work, date_col)
    totals = {col: _sum(work, col) for col in numeric_cols if col in work.columns and col != "광고효과"}
    ad_cost = int(totals.get("광고지출", totals.get("광고비용", 0)) or 0)
    ad_sales = int(totals.get("주문금액", totals.get("광고매출", 0)) or 0)
    impressions = int(totals.get("노출수", totals.get("광고노출수", 0)) or 0)
    clicks = int(totals.get("클릭수", totals.get("광고클릭수", 0)) or 0)
    orders = int(totals.get("주문수", totals.get("광고주문수", 0)) or 0)
    return {
        "coverage": {"rows": int(len(work)), **dates},
        "totals": totals,
        "derived": {
            "ctr": _ratio(clicks, impressions),
            "conversion_rate": _ratio(orders, clicks),
            "roas": _ratio(ad_sales, ad_cost),
        },
    }


def _operation_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"coverage": {"rows": 0}, "averages": {}, "minimums": {}, "latest": {}}
    work = df.copy()
    work.columns = work.columns.map(lambda c: str(c).strip())
    work = _parse_numeric_cols(work, NOW_NUMERIC_COLS)
    averages = {col: _mean(work, col) for col in NOW_NUMERIC_COLS if col in work.columns}
    minimums = {col: _min(work, col) for col in NOW_NUMERIC_COLS if col in work.columns}
    latest = {}
    if "date" in work.columns:
        sorted_work = work.sort_values("date")
        latest = {
            col: _json_value(sorted_work.iloc[-1].get(col))
            for col in NOW_NUMERIC_COLS
            if col in sorted_work.columns
        }
    return {
        "coverage": {"rows": int(len(work)), **_date_bounds(work, "date")},
        "averages": averages,
        "minimums": minimums,
        "latest": latest,
    }


def _review_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"coverage": {"rows": 0}, "sentiment_counts": {}, "top_topics": []}
    work = df.copy()
    work.columns = work.columns.map(lambda c: str(c).strip())
    count_col = "언급수" if "언급수" in work.columns else None
    if count_col:
        work = _parse_numeric_cols(work, [count_col])
    else:
        work["언급수"] = 1
        count_col = "언급수"
    sentiment_col = "감정수준" if "감정수준" in work.columns else None
    topic_col = "토픽" if "토픽" in work.columns else None
    sentiment_counts = {}
    if sentiment_col:
        sentiment_counts = {str(k): int(v) for k, v in work.groupby(sentiment_col)[count_col].sum().items()}
    top_topics = []
    if topic_col:
        grouped = work.groupby(topic_col, dropna=False)[count_col].sum().sort_values(ascending=False).head(5)
        top_topics = [{"topic": str(k), "mentions": int(v)} for k, v in grouped.items()]
    return {
        "coverage": {"rows": int(len(work)), **_date_bounds(work, "작성일자")},
        "sentiment_counts": sentiment_counts,
        "top_topics": top_topics,
    }


def _commission_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"coverage": {"rows": 0}, "totals": {}, "by_platform": []}
    work = _parse_numeric_cols(df.copy(), DELIVERY_COMMISSION_NUMERIC_COLS)
    totals = {
        "delivery_gross_sales": _sum(work, "total_amt"),
        "settlement_amount": _sum(work, "settlement_amount"),
        "commission_and_discount": _sum(work, "diff_amt"),
        "baemin_instant_discount": _sum(work, "배민_즉시할인"),
        "coupang_ad_cost": _sum(work, "쿠팡_광고비용"),
        "coupang_new_customers": _sum(work, "쿠팡_신규고객"),
        "coupang_ad_impressions": _sum(work, "쿠팡_광고노출수"),
        "coupang_ad_clicks": _sum(work, "쿠팡_광고클릭수"),
    }
    gross = totals["delivery_gross_sales"]
    totals["commission_rate"] = _ratio(totals["commission_and_discount"], gross)
    by_platform = []
    if "platform" in work.columns:
        for platform, group in work.groupby("platform", dropna=False):
            platform_gross = _sum(group, "total_amt")
            platform_diff = _sum(group, "diff_amt")
            by_platform.append(
                {
                    "platform": str(platform),
                    "delivery_gross_sales": platform_gross,
                    "settlement_amount": _sum(group, "settlement_amount"),
                    "commission_and_discount": platform_diff,
                    "commission_rate": _ratio(platform_diff, platform_gross),
                }
            )
    return {
        "coverage": {"rows": int(len(work)), **_date_bounds(work, "sale_date")},
        "totals": totals,
        "by_platform": by_platform,
    }


def _visit_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"coverage": {"rows": 0}, "visit_count": 0, "issue_counts": {}, "top_issues": [], "latest_visit": {}}
    work = df.copy()
    work.columns = work.columns.map(lambda c: str(c).strip())
    visit_count = _visit_count(work)
    issue_counts: dict[str, int] = {}
    if "category" in work.columns:
        issue_counts = {str(k): int(v) for k, v in work["category"].fillna("미분류").value_counts().head(8).items()}
    top_issues = []
    label_col = "issue_label" if "issue_label" in work.columns else "category" if "category" in work.columns else None
    if label_col:
        labels = work[label_col].fillna("미분류").astype(str).map(_clean_text)
        labels = labels[~labels.map(_is_generic_visit_issue_label)]
        label_counts = labels.value_counts().head(5)
        top_issues = [{"issue": str(k), "count": int(v)} for k, v in label_counts.items()]
    request_count = int(work["is_request"].fillna(False).astype(bool).sum()) if "is_request" in work.columns else 0
    unresolved_count = 0
    if "status" in work.columns:
        status = work["status"].fillna("").astype(str).str.strip()
        unresolved_count = int(status.isin(["미해결", "진행", "대기", "보류"]).sum())
    latest_visit = {}
    if "visit_date" in work.columns:
        sorted_work = work.sort_values("visit_date")
        latest = sorted_work.iloc[-1]
        latest_visit = {
            "visit_date": _format_date(latest.get("visit_date")),
            "post_url": _clean_text(latest.get("post_url")),
            "author_name": _clean_text(latest.get("author_name")),
            "owner_status": _truncate(_clean_text(latest.get("owner_status")), 180),
            "key_concerns": _truncate(_clean_text(latest.get("key_concerns")), 220),
            "next_visit_action": _truncate(_clean_text(latest.get("next_visit_action")), 220),
        }
    return {
        "coverage": {"rows": int(len(work)), **_date_bounds(work, "visit_date")},
        "visit_count": visit_count,
        "issue_counts": issue_counts,
        "top_issues": top_issues,
        "request_count": request_count,
        "unresolved_count": unresolved_count,
        "latest_visit": latest_visit,
    }


def _is_generic_visit_issue_label(value: Any) -> bool:
    return re.sub(r"\s+", "", _clean_text(value)) in {"방문일지주요내용"}


def _visit_count(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    if "visit_rel_key" in df.columns:
        return int(df["visit_rel_key"].dropna().map(_clean_text).replace("", pd.NA).dropna().nunique())
    if "post_id" in df.columns:
        return int(df["post_id"].dropna().map(_clean_text).replace("", pd.NA).dropna().nunique())
    if "visit_date" in df.columns:
        return int(df["visit_date"].dropna().map(_clean_text).replace("", pd.NA).dropna().nunique())
    return int(len(df))


def _visit_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    work = df.copy()
    work.columns = work.columns.map(lambda c: str(c).strip())
    cols = [
        "visit_date",
        "visit_ym",
        "author_name",
        "issue_label",
        "category",
        "severity",
        "status",
        "key_concerns",
        "next_visit_action",
    ]
    rows = []
    for _, row in work.sort_values("visit_date", ascending=False).iterrows():
        record = {}
        for col in cols:
            if col not in work.columns:
                continue
            if col == "issue_label" and _is_generic_visit_issue_label(row.get(col)):
                continue
            if col == "key_concerns":
                # handling_points는 key_concerns의 부분집합이라 따로 싣지 않고 표시만 남긴다.
                record[col] = _truncate(
                    _mark_problem_concerns(row.get("key_concerns"), row.get("handling_points")), 260
                )
                continue
            record[col] = (
                _truncate(_clean_text(row.get(col)), 260)
                if col == "next_visit_action"
                else _json_value(row.get(col))
            )
        rows.append(record)
    return rows


def _bullet_lines(value: Any) -> list[str]:
    return [line.lstrip("-• ").strip() for line in _clean_text(value).splitlines() if line.strip()]


def _mark_problem_concerns(key_concerns: Any, handling_points: Any) -> str:
    """방문 화두 목록에 현재 문제·고민을 표시한다."""
    problems = {re.sub(r"\s+", "", line) for line in _bullet_lines(handling_points)}
    lines = []
    for line in _bullet_lines(key_concerns):
        mark = " (문제·고민)" if re.sub(r"\s+", "", line) in problems else ""
        lines.append(f"- {line}{mark}")
    return "\n".join(lines)


def _visit_timeline(df: pd.DataFrame) -> list[str]:
    if df.empty or "visit_date" not in df.columns:
        return []
    dates = _parse_dates(df["visit_date"]).dropna().dt.strftime("%Y-%m-%d").drop_duplicates()
    return sorted(dates.tolist(), reverse=True)


def _source_coverage(sources: dict[str, pd.DataFrame]) -> dict[str, dict[str, Any]]:
    return {name: {"rows": int(len(df)), "available": bool(not df.empty)} for name, df in sources.items()}


def _gpt_context(
    *,
    brand: str,
    store: str,
    ym: str,
    month_summary: dict[str, Any],
    channel_summary: dict[str, Any],
    marketing_summary: dict[str, Any],
    commission_summary: dict[str, Any],
    operation_summary: dict[str, Any],
    review_summary: dict[str, Any],
    visit_summary: dict[str, Any],
    missing_sources: list[str],
) -> dict[str, Any]:
    return {
        "summary_text": (
            f"{brand} {store} {ym} 월간 매출 분석용 압축 집계입니다. "
            f"실제매출 {month_summary.get('actual_sales', 0):,}원, "
            f"목표 {month_summary.get('target_sales', 0):,}원, "
            f"달성률 {month_summary.get('achievement_rate')}%, "
            f"배달매출 {channel_summary.get('delivery_sales', 0):,}원, "
            f"홀매출 {channel_summary.get('hall_sales', 0):,}원입니다."
        ),
        "key_numbers": {
            "actual_sales": month_summary.get("actual_sales"),
            "target_sales": month_summary.get("target_sales"),
            "achievement_rate": month_summary.get("achievement_rate"),
            "delivery_share": channel_summary.get("delivery_sales_share"),
            "hall_share": channel_summary.get("hall_sales_share"),
            "baemin_woori_roas": marketing_summary.get("baemin_woori_click", {}).get("derived", {}).get("roas"),
            "coupang_ad_rows": marketing_summary.get("coupang_marketing", {}).get("coverage", {}).get("rows"),
            "delivery_commission_rate": commission_summary.get("totals", {}).get("commission_rate"),
            "baemin_latest_rating": operation_summary.get("latest", {}).get("최근별점"),
            "review_top_topics": review_summary.get("top_topics", []),
            "visit_top_issues": visit_summary.get("top_issues", []),
            "visit_count": visit_summary.get("visit_count", 0),
        },
        "missing_sources": sorted(set(missing_sources)),
    }


def _recommended_prompt() -> str:
    return (
        "아래 analysis.json만 근거로 매장 월간 매출 부진 원인을 분석하세요. "
        "결론, 핵심 원인 3개, 수치 근거, 결측 데이터, 다음 액션 순서로 답하세요. "
        "JSON에 없는 수치와 외부 요인은 추측하지 말고, 결측 데이터는 원인으로 단정하지 마세요."
    )


def _index_item(output_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    meta = payload["meta"]
    return {
        "brand": meta["brand"],
        "ym": meta["ym"],
        "store": meta["store"],
        "path": str(_analysis_path(output_root, meta["brand"], meta["ym"], meta["store"]).relative_to(output_root)),
        "readme_path": str(_analysis_path(output_root, meta["brand"], meta["ym"], meta["store"]).with_name("README.md").relative_to(output_root)),
        "files": {
            name: str(_analysis_path(output_root, meta["brand"], meta["ym"], meta["store"]).with_name(info["path"]).relative_to(output_root))
            for name, info in (meta.get("files") or {}).items()
            if isinstance(info, dict) and info.get("path")
        },
        "actual_sales": payload["month_summary"].get("actual_sales"),
        "achievement_rate": payload["month_summary"].get("achievement_rate"),
        "missing_sources": meta.get("missing_sources", []),
    }


def _write_indexes(output_root: Path, items: list[dict[str, Any]]) -> None:
    root_items = _read_index_items(output_root / "index.json")
    by_month: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in items:
        root_items = _upsert_index_item(root_items, item)
        by_month.setdefault((item["brand"], item["ym"]), []).append(item)
    root_items = sorted(root_items, key=lambda x: (str(x.get("brand")), str(x.get("ym")), str(x.get("store"))))
    _write_json(output_root / "index.json", {"generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "items": root_items})
    for (brand, ym), items in by_month.items():
        month_index = output_root / f"brand={brand}" / f"ym={ym}" / "index.json"
        month_items = _read_index_items(month_index)
        for item in items:
            month_items = _upsert_index_item(month_items, item)
        month_items = sorted(month_items, key=lambda x: str(x.get("store")))
        _write_json(month_index, {"brand": brand, "ym": ym, "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "items": month_items})


def _read_index_items(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        _raise_if_memory_error(exc)
        logger.warning("index 읽기 실패, 새로 생성합니다: %s | %s", path, exc)
        return []
    items = payload.get("items") if isinstance(payload, dict) else None
    return items if isinstance(items, list) else []


def _upsert_index_item(items: list[dict[str, Any]], item: dict[str, Any]) -> list[dict[str, Any]]:
    key = (str(item.get("brand")), str(item.get("ym")), str(item.get("store")))
    kept = [
        existing
        for existing in items
        if (str(existing.get("brand")), str(existing.get("ym")), str(existing.get("store"))) != key
    ]
    kept.append(item)
    return kept


def _analysis_path(output_root: Path, brand: str, ym: str, store: str) -> Path:
    return output_root / f"brand={brand}" / f"ym={ym}" / f"store={store}" / "analysis.json"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_store_readme(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_store_readme_markdown(payload), encoding="utf-8")


def _store_readme_markdown(payload: dict[str, Any]) -> str:
    meta = payload.get("meta") or {}
    month = payload.get("month_summary") or {}
    missing = meta.get("missing_sources") or []
    coverage = meta.get("source_coverage") or {}
    files = meta.get("files") or {}
    lines = [
        f"# {meta.get('brand', '')} {meta.get('store', '')} {meta.get('ym', '')} For_AI 데이터 설명서",
        "",
        "## 목적",
        "",
        "이 폴더는 GPT봇이 원천 CSV/parquet를 직접 읽지 않고 압축 집계 JSON만 읽어 매장 월간 매출 부진 원인을 분석하도록 만든 산출물입니다.",
        "",
        "## 핵심 요약",
        "",
        f"- 실제매출: {int(month.get('actual_sales') or 0):,}원",
        f"- 목표매출: {int(month.get('target_sales') or 0):,}원",
        f"- 달성률: {month.get('achievement_rate')}%",
        f"- 영업일수: {int(month.get('sales_days') or 0)}일",
        "",
        "## JSON 파일",
        "",
        "- `analysis.json`: 월간 핵심 요약, 파일 목차, 결측 원천, GPT 권장 프롬프트",
        "- `orders.json`: 일별 주문수·객단가·매출, platform/order_type/menu_name별 압축 집계",
        "- `ads.json`: 배민/쿠팡 광고와 광고성 비용·성과 지표",
        "- `visit_log.json`: 해당 월 방문일지와 기준월 말일 이전 전체 방문 이슈",
        "",
        "## 파일 매니페스트",
        "",
    ]
    if files:
        for name, info in files.items():
            lines.append(f"- `{name}`: `{(info or {}).get('path')}` - {(info or {}).get('description', '')}")
    else:
        lines.append("- 파일 매니페스트 없음")
    lines.extend(
        [
            "",
            "## analysis.json 주요 섹션",
            "",
            "- `meta`: 브랜드, 매장, 기준월, 생성시각, 파일 목차, 원천별 사용 가능 여부, 결측 원천 목록",
            "- `month_summary`: 월 목표/실제/부족액/달성률/일평균/홀·포장·배달 요약",
            "- `week_summary`: 주차별 목표·실적·달성률·전주 대비 매출 변화",
            "- `channel_summary`: 홀·포장·배달 및 플랫폼/source별 매출 압축 집계",
            "- `marketing_summary`: 광고 월간 요약. 상세 광고 파일은 `ads.json`",
            "- `commission_summary`: 배달 플랫폼별 총매출, 정산액, 수수료·할인성 차감액, 차감률",
            "- `visit_summary`: 방문일지 핵심 요약. 해당 월 방문이 없어도 기준월 이전 방문 히스토리를 포함할 수 있습니다.",
            "- `risk_days`: 달성률 하위 일자와 전일 대비 하락 일자 상위 5개",
            "- `gpt_context`: GPT에 바로 넘길 압축 한국어 요약과 핵심 숫자",
            "- `recommended_prompt`: 이 JSON을 기준으로 분석하게 하는 권장 프롬프트",
            "",
            "## orders.json 집계 기준",
            "",
            "- `daily_summary`: `sale_date`별 주문수, 객단가, 매출",
            "- `daily_channel_summary`: `sale_date`, `brand`, `platform`, `order_type` 기준 주문수, 객단가, 매출",
            "- `daily_menu_summary`: `sale_date`, `brand`, `platform`, `order_type`, `menu_name` 기준 주문수, 객단가, 매출",
            "- 메뉴는 각 일자/platform/order_type별 매출 상위 메뉴를 남기고 나머지는 `기타`로 묶습니다.",
            "- 주문수는 `order_id` 고유 개수를 우선 사용하고, 없으면 `order_cnt` 합계를 사용합니다.",
            "",
        ]
    )
    lines.extend(
        [
            "## 원천 데이터",
            "",
            "- 매출/목표: `analytics/store_sales_target/target.csv`, `analytics/store_sales_target/daily_actuals.csv`, `mart/unified_sales_grp/*.parquet`",
            "- 배민 광고/운영: `analytics/baemin_macro/metrics_our_store_clicks`, `metrics_now`, `ad_funnel`, `analytics/baemin_marketing`",
            "- 쿠팡 광고: `analytics/coupang_marketing`",
            "- 수수료/정산: `mart/delivery_commission/delivery_commission.parquet`",
            "- 리뷰: `mart/unified_review/*.parquet`",
            "- 방문일지: `mart/Flow_mart/Flow_visit/flow_visit_viz.parquet`",
            "",
            "## 원천 커버리지",
            "",
        ]
    )
    if coverage:
        for name, info in sorted(coverage.items()):
            rows = int((info or {}).get("rows") or 0)
            available = "있음" if (info or {}).get("available") else "없음"
            lines.append(f"- `{name}`: {available}, {rows:,}행")
    else:
        lines.append("- 커버리지 정보 없음")
    lines.extend(["", "## 결측 원천", ""])
    if missing:
        lines.extend(f"- `{name}`" for name in missing)
    else:
        lines.append("- 결측 원천 없음")
    lines.extend(
        [
            "",
            "## GPT 사용 규칙",
            "",
            "- `analysis.json`에 없는 수치와 외부 요인은 추측하지 않습니다.",
            "- 먼저 `analysis.json`을 읽고, 주문/메뉴 원인이 필요하면 `orders.json`, 광고 원인이 필요하면 `ads.json`, 현장 이슈가 필요하면 `visit_log.json`을 추가로 읽습니다.",
            "- 방문일지는 분석월 방문과 기준월 말일 이전 방문 히스토리를 함께 봅니다.",
            "- 결측 원천은 원인으로 단정하지 않고 확인 필요로 표시합니다.",
            "- 광고, 수수료, 리뷰, 방문일지 이슈는 매출 하락과 같은 기간에 악화된 경우에만 원인 후보로 봅니다.",
            "- 상세 원천 행이 필요하면 이 JSON이 아니라 위 원천 경로를 별도 조회해야 합니다.",
            "",
        ]
    )
    return "\n".join(lines)


def _normalize_text_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = out[col].map(_clean_text)
    return out


def _parse_numeric_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            continue
        out[col] = (
            out[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.strip()
        )
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)
    return out


def _normalize_ym(value: Any) -> str:
    text = _clean_text(value)
    parsed = _parse_dates(pd.Series([text])).iloc[0]
    if pd.notna(parsed):
        return parsed.strftime("%Y-%m")
    matched = re.search(r"(20\d{2})[-./년\s]*(\d{1,2})", text)
    if matched:
        return f"{matched.group(1)}-{int(matched.group(2)):02d}"
    return text[:7]


def _clean_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if pd.isna(value):
        return ""
    return str(value).strip()


def _store_matches_path(path: Path, store: str) -> bool:
    normalized_path = str(path).replace("_", "").replace(" ", "")
    normalized_store = store.replace("_", "").replace(" ", "")
    return normalized_store in normalized_path


def _sum(df: pd.DataFrame, col: str) -> int:
    if df.empty or col not in df.columns:
        return 0
    return int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def _mean(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df.columns:
        return None
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if series.empty else round(float(series.mean()), 3)


def _min(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df.columns:
        return None
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if series.empty else round(float(series.min()), 3)


def _ratio(numerator: int | float, denominator: int | float) -> float | None:
    denominator = float(denominator or 0)
    if denominator == 0:
        return None
    return round(float(numerator or 0) / denominator * 100, 1)


def _target_for_day(row: pd.Series, target_row: dict[str, Any] | None) -> int:
    if not target_row:
        return 0
    key = "주말_일목표매출" if str(row.get("요일구분") or "") == "주말" else "평일_일목표매출"
    return int(target_row.get(key) or 0)


def _clean_day_record(record: dict[str, Any]) -> dict[str, Any]:
    mapping = {
        "매출일자": "date",
        "요일구분": "day_type",
        "총매출": "actual_sales",
    }
    return {mapping.get(k, k): _json_value(v) for k, v in record.items()}


def _date_bounds(df: pd.DataFrame, date_col: str) -> dict[str, str | None]:
    if df.empty or date_col not in df.columns:
        return {"date_from": None, "date_to": None}
    parsed = _parse_dates(df[date_col])
    parsed = parsed.dropna()
    if parsed.empty:
        return {"date_from": None, "date_to": None}
    return {"date_from": parsed.min().strftime("%Y-%m-%d"), "date_to": parsed.max().strftime("%Y-%m-%d")}


def _format_date(value: Any) -> str | None:
    parsed = _parse_dates(pd.Series([value])).iloc[0]
    if pd.isna(parsed):
        return None
    return parsed.strftime("%Y-%m-%d")


def _truncate(text: str, limit: int) -> str:
    return text if len(text) <= limit else f"{text[:limit].rstrip()}..."


def _parse_dates(value: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(value, errors="coerce", format="mixed")
    except (TypeError, ValueError):
        return pd.to_datetime(value, errors="coerce")


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_value(v) for v in value]
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.strftime("%Y-%m-%d") if value.time() == datetime.min.time() else value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value
