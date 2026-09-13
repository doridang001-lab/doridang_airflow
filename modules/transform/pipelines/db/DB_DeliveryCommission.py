"""Build delivery commission mart from Baemin and Coupang macro sources."""

import logging
import hashlib
import re
from pathlib import Path

import pandas as pd

from modules.transform.utility.account import load_automation_account_df
from modules.transform.utility.paths import (
    BAEMIN_ORDERS_DB,
    BAEMIN_OUR_STORE_CLICKS_DB,
    COUPANG_ORDERS_DETAIL_DB,
    COUPANG_ORDERS_DB,
    DDANGYO_FEE_RATIO_BASELINE_CSV,
    DDANGYO_FEE_RATIO_MONTHLY_CSV,
    DELIVERY_COMMISSION_PATH,
    DELIVERY_REVENUE_PATH,
    MART_DB,
    ONEDRIVE_DB,
    STORE_COST_ALLOCATION_PATH,
    YOGIYO_SETTLEMENT_MONTHLY_FEE_CSV_PATH,
)
from modules.transform.utility.store_normalize import lookup_store_key, strip_brand

logger = logging.getLogger(__name__)
BAEMIN_DISCOUNT_INCLUDED_ATTR = "baemin_partner_discount_included"

OUTPUT_COLUMNS = [
    "sale_date",
    "store",
    "platform",
    "total_amt",
    "settlement_amount",
    "diff_amt",
    "brand",
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
REVENUE_OUTPUT_COLUMNS = [
    "sale_date",
    "brand",
    "store",
    "platform",
    "total_price",
    "settlement_price",
    "revenue",
    "fin_revenue",
    "store_expected_month_fin_revenue",
    "rent_cost",
]
REVENUE_BASE_COLUMNS = [
    column
    for column in REVENUE_OUTPUT_COLUMNS
    if not column.endswith("_cost")
    and column not in {"fin_revenue", "store_expected_month_fin_revenue"}
]
COST_OUTPUT_COLUMNS = [
    "sale_date",
    "brand",
    "store",
    "platform",
    "total_price",
    "monthly_rent",
    "rent_cost",
]
ADDITIONAL_REVENUE_FEE_RATIOS = {
    "먹깨비": 0.015,
    "배달특급": 0.010,
    "인천이음": 0.020,
}
ADDITIONAL_REVENUE_PLATFORM_ALIASES = {
    "땡배달": "땡겨요",
}
YOGIYO_REVENUE_PLATFORM_ALIASES = {
    "요기요": "요기요",
    "요기배달": "요기요",
}
ADDITIONAL_REVENUE_PLATFORMS = {
    "땡겨요",
    *ADDITIONAL_REVENUE_FEE_RATIOS,
    *ADDITIONAL_REVENUE_PLATFORM_ALIASES,
}
UNIFIED_SALES_ROOT = MART_DB / "unified_sales_grp"
BAEMIN_SETTLEMENT_MISSING_COLUMNS = [
    "scope",
    "store",
    "brand",
    "missing_days",
    "total_amt_sum",
    "first_date",
    "last_date",
    "missing_periods",
    "missing_dates",
]
REQUIRED_BRANDS = {"도리당", "나홀로"}
COUPANG_CMG_DB = COUPANG_ORDERS_DETAIL_DB / "cmg"
BAEMIN_SETTLEMENT_MISSING_LATEST_CSV = (
    Path(".tmp") / "baemin_settlement_missing_targets_latest.csv"
)
BAEMIN_RECOLLECT_SOURCE_DAG_ID = "DB_DeliveryCommission_Dags"
_COUPANG_DEDUP_COLS = [
    "brand",
    "store",
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
_COUPANG_DEDUP_NUMERIC_COLS = {"total_price", "menu_qty", "menu_price"}


def _partition_value(path: Path, prefix: str) -> str:
    token = f"{prefix}="
    for part in path.parts:
        if part.startswith(token):
            return part[len(token):].strip()
    return ""


def _normalize_store_name(brands: pd.Series, stores: pd.Series) -> pd.Series:
    stripped_stores = strip_brand(stores.astype(str).str.strip())
    normalized = [
        lookup_store_key(str(brand).strip(), str(store).strip())
        for brand, store in zip(brands, stripped_stores)
    ]
    return pd.Series(normalized, index=stores.index, dtype=str).str.replace(r"\s+", "", regex=True)


def _validate_source_brands(paths: list[Path], source_name: str) -> None:
    brands = {_partition_value(path, "brand") for path in paths}
    missing = REQUIRED_BRANDS - brands
    if missing:
        raise RuntimeError(f"{source_name} 필수 브랜드 원천 없음: {sorted(missing)}")


def _raise_read_errors(source_name: str, errors: list[tuple[Path, Exception]]) -> None:
    if not errors:
        return
    samples = ", ".join(str(path) for path, _ in errors[:3])
    raise RuntimeError(
        f"{source_name} 원천 파일 {len(errors)}개 읽기 실패; 기존 마트 보존 | sample={samples}"
    ) from errors[0][1]


def _money(series: pd.Series) -> pd.Series:
    return _nullable_money(series).fillna(0)


def _nullable_money(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )


def _baemin_order_date(series: pd.Series) -> pd.Series:
    raw = series.astype(str)
    extracted = raw.str.extract(r"(\d{4}\.\s*\d{2}\.\s*\d{2}\.)", expand=False)
    return pd.to_datetime(extracted, format="%Y. %m. %d.", errors="coerce").dt.strftime("%Y-%m-%d")


def _baemin_payment_amount(series: pd.Series) -> int:
    values = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .str.extract(r"(-?\d+)", expand=False)
    )
    amounts = pd.to_numeric(values, errors="coerce").fillna(0)
    nonzero = amounts[amounts.ne(0)]
    return int(nonzero.iloc[0] if not nonzero.empty else amounts.max())


def _scope_key(value: object) -> str:
    return re.sub(r"\s+", "", strip_brand(pd.Series([str(value or "")])).iloc[0])


def _baemin_store_scope_lookup() -> dict[str, str]:
    try:
        accounts = load_automation_account_df(platform="배달의 민족")
    except Exception as exc:
        logger.warning("배민 계정 scope 로드 실패: %s", exc)
        return {}
    if accounts.empty or "매장명" not in accounts.columns:
        return {}

    store_names = sorted(
        {
            str(store).strip()
            for store in accounts["매장명"].dropna().tolist()
            if str(store).strip()
        }
    )
    mid = len(store_names) // 2
    lookup: dict[str, str] = {}
    for index, store_name in enumerate(store_names):
        scope = "상위" if index < mid else "하위"
        for key in {store_name, strip_brand(pd.Series([store_name])).iloc[0], _scope_key(store_name)}:
            if key:
                lookup.setdefault(key, scope)
    return lookup


def _format_date_periods(dates: list[str]) -> str:
    parsed = sorted(pd.to_datetime(pd.Series(dates), errors="coerce").dropna().dt.date.unique())
    if not parsed:
        return ""

    periods: list[str] = []
    start = prev = parsed[0]
    for current in parsed[1:]:
        if (current - prev).days == 1:
            prev = current
            continue
        periods.append(start.isoformat() if start == prev else f"{start.isoformat()}~{prev.isoformat()}")
        start = prev = current
    periods.append(start.isoformat() if start == prev else f"{start.isoformat()}~{prev.isoformat()}")
    return "|".join(periods)


def find_baemin_settlement_missing_targets() -> pd.DataFrame:
    baemin_orders = _load_baemin_orders_agg()
    missing = baemin_orders[
        baemin_orders["total_amt"].fillna(0).gt(0)
        & baemin_orders["baemin_deposit_amt"].isna()
    ].copy()
    if missing.empty:
        return pd.DataFrame(columns=BAEMIN_SETTLEMENT_MISSING_COLUMNS)

    scope_lookup = _baemin_store_scope_lookup()
    missing["scope"] = missing["store"].map(lambda store: scope_lookup.get(_scope_key(store), "미분류"))
    grouped = (
        missing.groupby(["scope", "store", "brand"], dropna=False)
        .agg(
            missing_days=("date", "nunique"),
            total_amt_sum=("total_amt", "sum"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            missing_dates=("date", lambda values: "|".join(sorted({str(value) for value in values}))),
        )
        .reset_index()
    )
    grouped["missing_periods"] = grouped["missing_dates"].map(
        lambda value: _format_date_periods(str(value).split("|"))
    )
    grouped["total_amt_sum"] = grouped["total_amt_sum"].round().astype("int64")
    grouped = grouped[BAEMIN_SETTLEMENT_MISSING_COLUMNS]
    return grouped.sort_values(
        ["scope", "missing_days", "last_date", "store", "brand"],
        ascending=[True, False, False, True, True],
    ).reset_index(drop=True)


def build_baemin_orders_only_recollect_confs(targets: pd.DataFrame) -> list[dict]:
    if targets.empty:
        return []

    rows: list[dict[str, str]] = []
    for row in targets.itertuples(index=False):
        for date in str(row.missing_dates).split("|"):
            date = date.strip()
            if date:
                rows.append({"scope": str(row.scope), "target_date": date, "store": str(row.store)})
    if not rows:
        return []

    exploded = pd.DataFrame(rows).drop_duplicates()
    confs: list[dict] = []
    for (scope, target_date), group in exploded.groupby(["scope", "target_date"], sort=True):
        stores = sorted(group["store"].dropna().astype(str).unique().tolist())
        confs.append(
            {
                "dag_id": "DB_Beamin_Macro_Backfill_Dags",
                "conf": {
                    "orders_only": True,
                    "target_date": str(target_date),
                    "stores": stores,
                    "collect_range": None if scope == "미분류" else str(scope),
                    "run_all_batches": False,
                    "stability_profile": "bulk_70",
                },
            }
        )
    return confs


def _safe_run_id_part(value: object, limit: int = 80) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "")).strip("_")
    return (cleaned or "none")[:limit]


def build_baemin_orders_only_recollect_triggers(
    recollect_confs: list[dict],
    *,
    source_run_id: str | None = None,
) -> list[dict]:
    triggers: list[dict] = []
    for item in recollect_confs:
        dag_id = str(item.get("dag_id") or "DB_Beamin_Macro_Dags")
        conf = dict(item.get("conf") or {})
        target_date = str(conf.get("target_date") or "")
        scope = conf.get("collect_range")
        stores = sorted(str(store) for store in (conf.get("stores") or []) if str(store).strip())
        store_digest = hashlib.sha1("|".join(stores).encode("utf-8")).hexdigest()[:10]
        run_id = (
            "delivery_commission_settlement_recollect__"
            f"{_safe_run_id_part(target_date.replace('-', ''))}__"
            f"{_safe_run_id_part(scope or 'all')}__"
            f"{store_digest}"
        )
        if source_run_id:
            conf.setdefault("source_dag_id", BAEMIN_RECOLLECT_SOURCE_DAG_ID)
            conf.setdefault("source_run_id", source_run_id)
        triggers.append({"dag_id": dag_id, "run_id": run_id, "conf": conf})
    return triggers


def monitor_baemin_settlement_missing(**context) -> str:
    targets = find_baemin_settlement_missing_targets()
    if targets.empty:
        return "배민 정산예정금액 결측 없음"

    output_path = BAEMIN_SETTLEMENT_MISSING_LATEST_CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    targets.to_csv(output_path, index=False, encoding="utf-8-sig")

    recollect_confs = build_baemin_orders_only_recollect_confs(targets)
    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(
            key="baemin_settlement_missing_targets",
            value=targets.to_dict(orient="records"),
        )
        ti.xcom_push(key="orders_only_recollect_confs", value=recollect_confs)

    by_scope = targets.groupby("scope", dropna=False)["missing_days"].sum().to_dict()
    samples = ", ".join(
        f"{row.scope}/{row.store}/{row.brand}/{row.missing_days}일/{row.first_date}~{row.last_date}"
        for row in targets.head(10).itertuples(index=False)
    )
    logger.error(
        "배민 정산예정금액 결측 감지: targets=%d missing_days=%d by_scope=%s csv=%s sample=%s",
        len(targets),
        int(targets["missing_days"].sum()),
        by_scope,
        output_path,
        samples,
    )
    return (
        "배민 정산예정금액 결측 감지 "
        f"{len(targets)}개 매장-브랜드/{int(targets['missing_days'].sum())}일; "
        f"orders_only 재수집 필요 | csv={output_path} | sample={samples}"
    )


def trigger_baemin_orders_only_recollect(**context) -> str:
    ti = context.get("ti")
    recollect_confs = []
    if ti is not None:
        recollect_confs = (
            ti.xcom_pull(
                task_ids="monitor_baemin_settlement_missing",
                key="orders_only_recollect_confs",
            )
            or []
        )
    if not recollect_confs:
        return "배민 정산예정금액 결측 재수집 대상 없음"

    dag_run = context.get("dag_run")
    source_run_id = getattr(dag_run, "run_id", None) or context.get("run_id")
    triggers = build_baemin_orders_only_recollect_triggers(
        recollect_confs,
        source_run_id=source_run_id,
    )

    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.exceptions import DagRunAlreadyExists
    from sqlalchemy.exc import MultipleResultsFound

    triggered: list[str] = []
    existing: list[str] = []
    deferred: list[str] = []
    for item in triggers:
        try:
            from modules.transform.utility.workload import route_trigger
            result = route_trigger(trigger_dag,
                dag_id=item["dag_id"],
                run_id=item["run_id"],
                conf=item["conf"],
            )
            if result == "cancelled":
                logger.info("사용자 취소로 배민 재수집 요청 제외: %s", item["run_id"])
            elif result == "deferred":
                deferred.append(item["run_id"])
            elif result == "existing":
                existing.append(item["run_id"])
            else:
                triggered.append(item["run_id"])
        except (DagRunAlreadyExists, MultipleResultsFound):
            existing.append(item["run_id"])
            logger.info("배민 정산예정금액 재수집 DAG run 이미 존재: %s", item["run_id"])

    summary = (
        "배민 정산예정금액 결측 재수집 트리거 "
        f"triggered={len(triggered)} existing={len(existing)} total={len(triggers)} deferred={len(deferred)}"
    )
    logger.warning("%s; build_delivery_commission은 NA 포함 갱신 계속 진행", summary)
    return f"{summary}; build_delivery_commission NA 포함 갱신 계속 진행"


def _coupang_order_date(series: pd.Series) -> pd.Series:
    raw = series.astype(str)
    extracted = raw.str.extract(r"(\d{4}\.\d{2}\.\d{2})", expand=False)
    return pd.to_datetime(extracted, format="%Y.%m.%d", errors="coerce").dt.strftime("%Y-%m-%d")


def _parse_coupang_ad_ratio(value: object) -> tuple[float | None, float | None]:
    text = str(value or "").strip()
    if not text:
        return None, None

    matches = dict(
        (label, float(number) / 100)
        for label, number in re.findall(r"(전체|신규|재주문)\s*(\d+(?:\.\d+)?)\s*%", text)
    )
    if "전체" in matches:
        return matches["전체"], matches["전체"]
    if "신규" in matches or "재주문" in matches:
        return matches.get("신규"), matches.get("재주문")
    raise ValueError(f"쿠팡 광고비율 파싱 실패: {text}")


def _unique_nullable_ratio(values: pd.Series) -> float | None:
    unique = values.dropna().unique()
    if len(unique) > 1:
        raise ValueError(f"쿠팡 광고비율 충돌: {sorted(unique)}")
    return None if len(unique) == 0 else float(unique[0])


def _deduplicate_coupang_raw(df: pd.DataFrame) -> pd.DataFrame:
    cols = [col for col in _COUPANG_DEDUP_COLS if col in df.columns]
    before = len(df)
    key = pd.DataFrame(index=df.index)
    for col in cols:
        values = df[col]
        if col in _COUPANG_DEDUP_NUMERIC_COLS:
            key[col] = pd.to_numeric(
                values.astype(str).str.replace(",", "", regex=False).str.strip(),
                errors="coerce",
            ).astype("Float64").astype(str)
        else:
            key[col] = values.fillna("").astype(str).str.strip()
            key[col] = key[col].replace({"nan": "", "None": "", "<NA>": ""})
    out = df[~key.duplicated(keep="last")].copy()
    dropped = before - len(out)
    if dropped:
        logger.warning("쿠팡 원천 중복 제거: %d행", dropped)
    return out


def _is_canonical_coupang_orders_file(path: Path) -> bool:
    return bool(re.fullmatch(r"orders_\d{4}-\d{2}\.parquet", path.name))


def _read_csv_with_fallback(path: Path) -> pd.DataFrame:
    last_exc: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=encoding, dtype=str)
        except Exception as exc:
            last_exc = exc
    raise RuntimeError(f"csv 로드 실패: {path}") from last_exc


def _load_baemin_orders_agg() -> pd.DataFrame:
    columns = [
        "date",
        "store",
        "brand",
        "total_amt",
        "baemin_deposit_amt",
        "baemin_cash_amt",
        "baemin_partner_instant_discount",
    ]
    files = sorted(BAEMIN_ORDERS_DB.glob("brand=*/store=*/ym=*/orders_*.parquet"))
    if not files:
        raise RuntimeError(f"baemin orders parquet 없음: {BAEMIN_ORDERS_DB}")
    _validate_source_brands(files, "baemin orders")

    parts = []
    read_errors: list[tuple[Path, Exception]] = []
    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            read_errors.append((path, exc))
            continue
        if df.empty:
            continue
        df = df.copy()
        df["brand"] = _partition_value(path, "brand")
        df["store"] = _partition_value(path, "store") or df.get("store", df.get("store_name", ""))
        parts.append(df)
    _raise_read_errors("baemin orders", read_errors)

    if not parts:
        raise RuntimeError("baemin orders 유효 데이터 없음; 기존 마트 보존")

    df = pd.concat(parts, ignore_index=True)
    required = {
        "주문번호",
        "주문시각",
        "주문상태",
        "brand",
        "store",
        "결제금액",
        "총결제금액",
        "즉시할인_파트너부담",
        "만나서결제금액",
        "입금예정금액",
    }
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"baemin 필수 컬럼 없음: {sorted(missing)}")

    status = df["주문상태"].fillna("").astype(str).str.strip()
    excluded = ~status.eq("배달완료")
    if excluded.any():
        logger.info("배민 취소/미완료 주문 제외: %d행", int(excluded.sum()))
        df = df[~excluded].copy()
    if df.empty:
        raise RuntimeError("baemin orders 배달완료 데이터 없음; 기존 마트 보존")

    df["총결제금액"] = _money(df["총결제금액"])
    df["즉시할인_파트너부담"] = _nullable_money(df["즉시할인_파트너부담"])
    df["만나서결제금액"] = _money(df["만나서결제금액"])
    df["입금예정금액"] = _nullable_money(df["입금예정금액"])
    grouped = df.groupby(["brand", "주문번호"], as_index=False).agg(
        주문시각=("주문시각", "max"),
        store=("store", "max"),
        결제금액=("결제금액", _baemin_payment_amount),
        총결제금액=("총결제금액", "max"),
        즉시할인_파트너부담=("즉시할인_파트너부담", "max"),
        만나서결제금액=("만나서결제금액", "max"),
        입금예정금액=("입금예정금액", "max"),
    )
    grouped["date"] = _baemin_order_date(grouped["주문시각"])
    grouped["store"] = _normalize_store_name(grouped["brand"], grouped["store"])
    grouped = grouped.dropna(subset=["date"])
    grouped = grouped[grouped["store"] != ""]

    unsettled = grouped["총결제금액"].gt(0) & grouped["입금예정금액"].isna()
    if unsettled.any():
        unsettled_keys = grouped.loc[
            unsettled, ["date", "store", "brand"]
        ].drop_duplicates()
        samples = ", ".join(
            f"{row.date}/{row.store}/{row.brand}"
            for row in unsettled_keys.head(10).itertuples(index=False)
        )
        logger.warning(
            "baemin 정산정보 미수집 주문 NULL 처리: 날짜×매장×브랜드 %s개, 주문 %s/%s건 | sample=%s",
            len(unsettled_keys),
            int(unsettled.sum()),
            len(grouped),
            samples,
        )

    grouped["_unsettled"] = unsettled
    grouped["즉시할인_파트너부담"] = grouped["즉시할인_파트너부담"].fillna(0)
    if grouped.empty:
        raise RuntimeError("baemin orders 집계 결과 없음; 기존 마트 보존")

    grouped = grouped.groupby(["date", "store", "brand"], as_index=False).agg(
        결제금액=("결제금액", "sum"),
        즉시할인_파트너부담=("즉시할인_파트너부담", "sum"),
        만나서결제금액=("만나서결제금액", "sum"),
        입금예정금액=("입금예정금액", "sum"),
        _unsettled=("_unsettled", "max"),
    )
    grouped = grouped.rename(
        columns={
            "결제금액": "total_amt",
            "즉시할인_파트너부담": "baemin_partner_instant_discount",
            "만나서결제금액": "baemin_cash_amt",
            "입금예정금액": "baemin_deposit_amt",
        }
    )
    grouped["total_amt"] = grouped["total_amt"].round().astype(int)
    grouped["baemin_partner_instant_discount"] = (
        grouped["baemin_partner_instant_discount"].round().astype(int)
    )
    grouped["baemin_cash_amt"] = grouped["baemin_cash_amt"].round().astype(int)
    grouped["baemin_deposit_amt"] = grouped["baemin_deposit_amt"].round()
    grouped.loc[grouped["_unsettled"], "baemin_deposit_amt"] = pd.NA
    grouped["baemin_deposit_amt"] = grouped["baemin_deposit_amt"].astype("Int64")
    return grouped[columns]


def _load_baemin_ad_spend_agg() -> pd.DataFrame:
    columns = ["date", "store", "brand", "ad_spend", "wgc_avg_cost", "wgc_orders", "wgc_ctr"]
    files = sorted(BAEMIN_OUR_STORE_CLICKS_DB.rglob("*.csv"))
    if not files:
        raise RuntimeError(f"baemin 광고 csv 없음: {BAEMIN_OUR_STORE_CLICKS_DB}")
    _validate_source_brands(files, "baemin ads")

    parts = []
    read_errors: list[tuple[Path, Exception]] = []
    for path in files:
        try:
            df = _read_csv_with_fallback(path)
        except Exception as exc:
            read_errors.append((path, exc))
            continue
        if df.empty:
            continue
        missing = {"날짜", "store_name", "광고지출", "노출수", "클릭수", "주문수"} - set(df.columns)
        if missing:
            read_errors.append((path, ValueError(f"필수 컬럼 없음: {sorted(missing)}")))
            continue
        out = pd.DataFrame()
        out["date"] = pd.to_datetime(df["날짜"], errors="coerce").dt.strftime("%Y-%m-%d")
        brands = pd.Series(_partition_value(path, "brand"), index=df.index, dtype=str)
        out["brand"] = brands
        out["store"] = _normalize_store_name(brands, df["store_name"])
        out["ad_spend"] = _money(df["광고지출"])
        out["impressions"] = _money(df["노출수"])
        out["clicks"] = _money(df["클릭수"])
        out["wgc_orders"] = _money(df["주문수"])
        parts.append(out)
    _raise_read_errors("baemin ads", read_errors)

    if not parts:
        raise RuntimeError("baemin ads 유효 데이터 없음; 기존 마트 보존")

    result = pd.concat(parts, ignore_index=True)
    result = result.dropna(subset=["date"])
    result = result[result["store"] != ""]
    if result.empty:
        raise RuntimeError("baemin ads 집계 결과 없음; 기존 마트 보존")
    result = result.groupby(["date", "store", "brand"], as_index=False)[
        ["ad_spend", "impressions", "clicks", "wgc_orders"]
    ].sum()
    result["ad_spend"] = result["ad_spend"].round().astype(int)
    clicks = result["clicks"]
    impressions = result["impressions"]
    result["wgc_avg_cost"] = (result["ad_spend"] / clicks.where(clicks.ne(0))).round(0)
    result["wgc_orders"] = result["wgc_orders"].round().astype("Int64")
    result["wgc_ctr"] = (clicks / impressions.where(impressions.ne(0))).round(4)
    return result[columns]


def _load_coupang_orders_agg() -> pd.DataFrame:
    columns = ["date", "store", "brand", "total_amt", "coupang_settlement_amt"]
    files = sorted(
        path
        for path in COUPANG_ORDERS_DB.glob("brand=*/store=*/ym=*/orders_*.parquet")
        if _is_canonical_coupang_orders_file(path)
    )
    if not files:
        raise RuntimeError(f"coupang orders parquet 없음: {COUPANG_ORDERS_DB}")
    _validate_source_brands(files, "coupang orders")

    parts = []
    read_errors: list[tuple[Path, Exception]] = []
    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            read_errors.append((path, exc))
            continue
        if df.empty:
            continue
        df = df.copy()
        df["brand"] = _partition_value(path, "brand")
        df["store"] = _partition_value(path, "store") or df.get("store", df.get("store_name", ""))
        df["_src_path"] = str(path)
        parts.append(df)
    _raise_read_errors("coupang orders", read_errors)

    if not parts:
        raise RuntimeError("coupang orders 유효 데이터 없음; 기존 마트 보존")

    df = pd.concat(parts, ignore_index=True)
    required = {
        "order_id",
        "order_date",
        "brand",
        "store",
        "매출액",
        "정산_예정_금액",
        "취소금액",
        "is_cancelled",
    }
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"coupang 필수 컬럼 없음: {sorted(missing)}")

    df["store"] = _normalize_store_name(df["brand"], df["store"])
    df = _deduplicate_coupang_raw(df)
    df["매출액"] = _nullable_money(df["매출액"])
    df["정산_예정_금액"] = _money(df["정산_예정_금액"])
    df["_fallback"] = _money(df["total_price"]) if "total_price" in df.columns else 0
    df["_is_cancelled"] = (
        df["is_cancelled"].fillna("").astype(str).str.strip().str.upper().eq("Y")
    )
    grouped = df.groupby(["brand", "store", "order_id"], as_index=False).agg(
        order_date=("order_date", "max"),
        매출액=("매출액", "sum"),
        _sales_rows=("매출액", "count"),
        _fallback=("_fallback", "max"),
        정산_예정_금액=("정산_예정_금액", "sum"),
        is_cancelled=("_is_cancelled", "max"),
    )
    # 정산 블록 미파싱 정상주문만 total_price로 폴백한다.
    no_sales = grouped["_sales_rows"].eq(0) & ~grouped["is_cancelled"]
    grouped.loc[no_sales, "매출액"] = grouped.loc[no_sales, "_fallback"]
    grouped["date"] = _coupang_order_date(grouped["order_date"])
    grouped = grouped.dropna(subset=["date"])
    grouped = grouped[grouped["store"] != ""]
    if grouped.empty:
        raise RuntimeError("coupang orders 집계 결과 없음; 기존 마트 보존")

    grouped = grouped.groupby(["date", "store", "brand"], as_index=False)[
        ["매출액", "정산_예정_금액"]
    ].sum()
    grouped = grouped.rename(
        columns={"매출액": "total_amt", "정산_예정_금액": "coupang_settlement_amt"}
    )
    grouped["total_amt"] = grouped["total_amt"].round().astype(int)
    grouped["coupang_settlement_amt"] = grouped["coupang_settlement_amt"].round().astype(int)
    return grouped[columns]


def _load_coupang_cmg_agg() -> pd.DataFrame:
    columns = [
        "date",
        "store",
        "brand",
        "coupang_new_ratio",
        "coupang_reorder_ratio",
        "coupang_ad_cost",
        "coupang_new_customers",
        "coupang_ad_impressions",
        "coupang_ad_clicks",
    ]
    files = sorted(COUPANG_CMG_DB.glob("brand=*/store=*/ym=*/cmg.csv"))
    if not files:
        raise RuntimeError(f"coupang cmg csv 없음: {COUPANG_CMG_DB}")
    _validate_source_brands(files, "coupang cmg")

    parts = []
    read_errors: list[tuple[Path, Exception]] = []
    for path in files:
        try:
            df = _read_csv_with_fallback(path)
        except Exception as exc:
            read_errors.append((path, exc))
            continue
        if df.empty:
            continue
        required = {"조회일자", "광고비율", "광고비용", "신규고객", "광고노출수", "광고클릭수"}
        missing = required - set(df.columns)
        if missing:
            read_errors.append((path, ValueError(f"필수 컬럼 없음: {sorted(missing)}")))
            continue

        out = pd.DataFrame()
        out["date"] = pd.to_datetime(df["조회일자"], errors="coerce").dt.strftime("%Y-%m-%d")
        brand = _partition_value(path, "brand")
        partition_store = _partition_value(path, "store")
        store = lookup_store_key(brand, partition_store)
        out["brand"] = pd.Series(brand, index=df.index, dtype=str)
        out["store"] = pd.Series(store, index=df.index, dtype=str).str.replace(
            r"\s+", "", regex=True
        )
        out["_partition_store"] = pd.Series(partition_store, index=df.index, dtype=str)
        out["_is_canonical_partition"] = (
            out["_partition_store"].astype(str).str.replace(r"\s+", "", regex=True).eq(out["store"])
        )
        try:
            ratios = df["광고비율"].map(_parse_coupang_ad_ratio)
        except Exception as exc:
            read_errors.append((path, exc))
            continue
        out["coupang_new_ratio"] = pd.to_numeric(ratios.map(lambda item: item[0]), errors="coerce")
        out["coupang_reorder_ratio"] = pd.to_numeric(ratios.map(lambda item: item[1]), errors="coerce")
        out["coupang_ad_cost"] = _money(df["광고비용"])
        out["coupang_new_customers"] = _money(df["신규고객"])
        out["coupang_ad_impressions"] = _money(df["광고노출수"])
        out["coupang_ad_clicks"] = _money(df["광고클릭수"])
        parts.append(out)
    _raise_read_errors("coupang cmg", read_errors)

    if not parts:
        raise RuntimeError("coupang cmg 유효 데이터 없음; 기존 마트 보존")

    result = pd.concat(parts, ignore_index=True)
    result = result.dropna(subset=["date"])
    result = result[result["store"] != ""]
    if result.empty:
        raise RuntimeError("coupang cmg 집계 결과 없음; 기존 마트 보존")

    result = result.drop_duplicates(
        subset=[
            "date",
            "store",
            "brand",
            "_partition_store",
            "coupang_new_ratio",
            "coupang_reorder_ratio",
            "coupang_ad_cost",
            "coupang_new_customers",
            "coupang_ad_impressions",
            "coupang_ad_clicks",
        ],
        keep="last",
    )
    collision_keys = ["date", "store", "brand"]
    has_canonical = result.groupby(collision_keys)["_is_canonical_partition"].transform("max")
    result = result[~(has_canonical & ~result["_is_canonical_partition"])].copy()

    try:
        result = result.groupby(["date", "store", "brand"], as_index=False).agg(
            coupang_new_ratio=("coupang_new_ratio", _unique_nullable_ratio),
            coupang_reorder_ratio=("coupang_reorder_ratio", _unique_nullable_ratio),
            coupang_ad_cost=("coupang_ad_cost", "sum"),
            coupang_new_customers=("coupang_new_customers", "sum"),
            coupang_ad_impressions=("coupang_ad_impressions", "sum"),
            coupang_ad_clicks=("coupang_ad_clicks", "sum"),
        )
    except Exception as exc:
        raise RuntimeError("coupang cmg 광고비율 집계 실패; 기존 마트 보존") from exc

    result["coupang_ad_cost"] = result["coupang_ad_cost"].round().astype("Int64")
    result["coupang_new_customers"] = result["coupang_new_customers"].round().astype("Int64")
    result["coupang_ad_impressions"] = result["coupang_ad_impressions"].round().astype("Int64")
    result["coupang_ad_clicks"] = result["coupang_ad_clicks"].round().astype("Int64")
    return result[columns]


def _finalize(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if frames:
        result = pd.concat(frames, ignore_index=True)
    else:
        result = pd.DataFrame(
            columns=[
                "date",
                "store",
                "platform",
                "total_amt",
                "settlement_amount",
                "brand",
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
        )
    if result.empty:
        raise RuntimeError("delivery_commission 결과 없음; 기존 마트 보존")

    existing_keys = set(result[["date", "store", "platform", "brand"]].itertuples(index=False, name=None))
    zero_rows = []
    for date, store, brand in result[["date", "store", "brand"]].drop_duplicates().itertuples(index=False):
        if (date, store, "배달의민족", brand) not in existing_keys:
            zero_rows.append(
                {
                    "date": date,
                    "store": store,
                    "platform": "배달의민족",
                    "total_amt": 0,
                    "settlement_amount": 0,
                    "brand": brand,
                    "배민_즉시할인": 0,
                    "우가클_평균비용": 0.0,
                    "우가클_주문수": 0,
                    "우가클_클릭율": 0.0,
                    "쿠팡_신규비율": float("nan"),
                    "쿠팡_재주문비율": float("nan"),
                    "쿠팡_광고비용": pd.NA,
                    "쿠팡_신규고객": pd.NA,
                    "쿠팡_광고노출수": pd.NA,
                    "쿠팡_광고클릭수": pd.NA,
                }
            )
        if (date, store, "쿠팡이츠", brand) not in existing_keys:
            zero_rows.append(
                {
                    "date": date,
                    "store": store,
                    "platform": "쿠팡이츠",
                    "total_amt": 0,
                    "settlement_amount": 0,
                    "brand": brand,
                    "배민_즉시할인": pd.NA,
                    "우가클_평균비용": float("nan"),
                    "우가클_주문수": pd.NA,
                    "우가클_클릭율": float("nan"),
                    "쿠팡_신규비율": 0.0,
                    "쿠팡_재주문비율": 0.0,
                    "쿠팡_광고비용": 0,
                    "쿠팡_신규고객": 0,
                    "쿠팡_광고노출수": 0,
                    "쿠팡_광고클릭수": 0,
                }
            )
    if zero_rows:
        result = pd.concat([result, pd.DataFrame(zero_rows)], ignore_index=True)

    result["diff_amt"] = result["total_amt"] - result["settlement_amount"]
    result = result.rename(columns={"date": "sale_date"})
    result = result.sort_values(["sale_date", "store", "platform", "brand"]).reset_index(drop=True)
    result = result[OUTPUT_COLUMNS]
    if result.duplicated(["sale_date", "store", "platform", "brand"]).any():
        raise RuntimeError("delivery_commission 결과 키 중복 발생")
    if set(result["platform"]) != {"배달의민족", "쿠팡이츠"}:
        raise RuntimeError(f"delivery_commission 플랫폼 누락: {sorted(set(result['platform']))}")
    if set(result["brand"]) != REQUIRED_BRANDS:
        raise RuntimeError(f"delivery_commission 브랜드 누락: {sorted(set(result['brand']))}")
    return result


def _write_parquet_atomic(
    result: pd.DataFrame,
    path: Path,
    expected_columns: list[str] | None = None,
) -> None:
    expected_columns = expected_columns or OUTPUT_COLUMNS
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    try:
        result.to_parquet(temp_path, index=False, engine="pyarrow")
        verified = pd.read_parquet(temp_path)
        if list(verified.columns) != expected_columns or len(verified) != len(result):
            raise RuntimeError(f"{path.name} 임시 parquet 검증 실패")
        if verified.attrs.get(BAEMIN_DISCOUNT_INCLUDED_ATTR) != result.attrs.get(
            BAEMIN_DISCOUNT_INCLUDED_ATTR
        ):
            raise RuntimeError(f"{path.name} 본인부담 할인 반영 메타데이터 검증 실패")
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _warn_if_baemin_settlement_missing(baemin: pd.DataFrame) -> None:
    if baemin.empty:
        return
    missing = baemin[
        baemin["total_amt"].fillna(0).gt(0)
        & baemin["settlement_amount"].isna()
    ].copy()
    if missing.empty:
        return

    samples = ", ".join(
        f"{row.date}/{row.store}/{row.brand}/total={int(row.total_amt):,}"
        for row in missing[["date", "store", "brand", "total_amt"]]
        .head(10)
        .itertuples(index=False)
    )
    summary = (
        missing.groupby(["store", "brand"], dropna=False)
        .agg(days=("date", "nunique"), first=("date", "min"), last=("date", "max"))
        .sort_values(["days", "last"], ascending=[False, False])
        .head(10)
    )
    logger.warning(
        "delivery_commission 배민 정산예정금액 결측, mart는 결측값 포함 갱신: rows=%d stores=%d sample=%s top=%s",
        len(missing),
        missing["store"].nunique(),
        samples,
        summary.to_dict(orient="index"),
    )


def build_delivery_commission() -> str:
    wgc_columns = {
        "wgc_avg_cost": "우가클_평균비용",
        "wgc_orders": "우가클_주문수",
        "wgc_ctr": "우가클_클릭율",
    }
    coupang_cmg_columns = {
        "coupang_new_ratio": "쿠팡_신규비율",
        "coupang_reorder_ratio": "쿠팡_재주문비율",
        "coupang_ad_cost": "쿠팡_광고비용",
        "coupang_new_customers": "쿠팡_신규고객",
        "coupang_ad_impressions": "쿠팡_광고노출수",
        "coupang_ad_clicks": "쿠팡_광고클릭수",
    }
    baemin_orders = _load_baemin_orders_agg()
    baemin_ads = _load_baemin_ad_spend_agg()
    baemin = baemin_orders.merge(baemin_ads, on=["date", "store", "brand"], how="left")
    if not baemin.empty:
        # 원천 결제금액에 본인부담 할인만 복원한다. 수익 마트에서는 재가산하지 않는다.
        baemin["total_amt"] = (
            baemin["total_amt"] + baemin["baemin_partner_instant_discount"]
        ).round().astype("int64")
        baemin["ad_spend"] = baemin["ad_spend"].fillna(0).round().astype(int)
        baemin["wgc_avg_cost"] = baemin["wgc_avg_cost"].fillna(0)
        baemin["wgc_orders"] = baemin["wgc_orders"].fillna(0).astype("Int64")
        baemin["wgc_ctr"] = baemin["wgc_ctr"].fillna(0)
        baemin["settlement_amount"] = (
            baemin["baemin_deposit_amt"]
            - baemin["ad_spend"]
        ).round().astype("Int64")
        baemin["platform"] = "배달의민족"
        baemin = baemin.rename(
            columns={"baemin_partner_instant_discount": "배민_즉시할인", **wgc_columns}
        )
        baemin["쿠팡_신규비율"] = pd.Series(float("nan"), index=baemin.index, dtype="float64")
        baemin["쿠팡_재주문비율"] = pd.Series(float("nan"), index=baemin.index, dtype="float64")
        baemin["쿠팡_광고비용"] = pd.Series(pd.NA, index=baemin.index, dtype="Int64")
        baemin["쿠팡_신규고객"] = pd.Series(pd.NA, index=baemin.index, dtype="Int64")
        baemin["쿠팡_광고노출수"] = pd.Series(pd.NA, index=baemin.index, dtype="Int64")
        baemin["쿠팡_광고클릭수"] = pd.Series(pd.NA, index=baemin.index, dtype="Int64")
        baemin = baemin[
            [
                "date",
                "store",
                "platform",
                "total_amt",
                "settlement_amount",
                "brand",
                "배민_즉시할인",
                *wgc_columns.values(),
                *coupang_cmg_columns.values(),
            ]
        ]
        _warn_if_baemin_settlement_missing(baemin)

    coupang = _load_coupang_orders_agg()
    coupang_cmg = _load_coupang_cmg_agg()
    coupang = coupang.merge(coupang_cmg, on=["date", "store", "brand"], how="left")
    if not coupang.empty:
        coupang["settlement_amount"] = coupang["coupang_settlement_amt"].round().astype("int64")
        coupang["platform"] = "쿠팡이츠"
        coupang["배민_즉시할인"] = pd.Series(pd.NA, index=coupang.index, dtype="Int64")
        coupang["우가클_평균비용"] = pd.Series(float("nan"), index=coupang.index, dtype="float64")
        coupang["우가클_주문수"] = pd.Series(pd.NA, index=coupang.index, dtype="Int64")
        coupang["우가클_클릭율"] = pd.Series(float("nan"), index=coupang.index, dtype="float64")
        coupang = coupang.rename(columns=coupang_cmg_columns)
        coupang = coupang[
            [
                "date",
                "store",
                "platform",
                "total_amt",
                "settlement_amount",
                "brand",
                "배민_즉시할인",
                *wgc_columns.values(),
                *coupang_cmg_columns.values(),
            ]
        ]

    result = _finalize([df for df in (baemin, coupang) if not df.empty])
    result.attrs[BAEMIN_DISCOUNT_INCLUDED_ATTR] = True
    _write_parquet_atomic(result, DELIVERY_COMMISSION_PATH)
    logger.info("delivery_commission mart 저장 완료: %s rows=%s", DELIVERY_COMMISSION_PATH, len(result))
    return f"delivery_commission {len(result)}행 -> {DELIVERY_COMMISSION_PATH}"


def _revenue_from_delivery_commission(source: pd.DataFrame | None = None) -> pd.DataFrame:
    if source is None:
        if not DELIVERY_COMMISSION_PATH.exists():
            raise RuntimeError(f"delivery_commission parquet 없음: {DELIVERY_COMMISSION_PATH}")
        source = pd.read_parquet(DELIVERY_COMMISSION_PATH)
    required = {"sale_date", "brand", "store", "platform", "total_amt", "settlement_amount", "diff_amt"}
    missing = required - set(source.columns)
    if missing:
        raise RuntimeError(f"delivery_commission 필수 컬럼 없음: {sorted(missing)}")

    work = source.rename(
        columns={
            "total_amt": "total_price",
            "settlement_amount": "settlement_price",
            "diff_amt": "revenue",
        }
    )[REVENUE_BASE_COLUMNS].copy()
    for column in ["total_price", "settlement_price", "revenue"]:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    return work


def _unified_sales_files() -> list[Path]:
    if not UNIFIED_SALES_ROOT.exists():
        raise RuntimeError(f"unified_sales parquet 없음: {UNIFIED_SALES_ROOT}")
    return sorted(
        path
        for path in UNIFIED_SALES_ROOT.glob("unified_sales_*.parquet")
        if re.fullmatch(r"unified_sales_\d{6}\.parquet", path.name)
    )


def _normalize_fee_month(series: pd.Series) -> pd.Series:
    raw = series.fillna("").astype(str).str.strip()
    return raw.str.replace("-", "_", regex=False)


def _load_ddangyo_monthly_fee_ratio() -> pd.DataFrame:
    if not DDANGYO_FEE_RATIO_MONTHLY_CSV.exists():
        raise RuntimeError(f"땡겨요 월별 수수료율 csv 없음: {DDANGYO_FEE_RATIO_MONTHLY_CSV}")
    df = _read_csv_with_fallback(DDANGYO_FEE_RATIO_MONTHLY_CSV)
    required = {"ym", "store", "fee_ratio"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"땡겨요 월별 수수료율 필수 컬럼 없음: {sorted(missing)}")

    result = df[list(required)].copy()
    result["ym"] = _normalize_fee_month(result["ym"])
    store = result["store"].fillna("").astype(str).str.strip()
    result["brand"] = store.str.extract(r"^(도리당|나홀로)\s+", expand=False).fillna("")
    result["store"] = strip_brand(store).str.replace(r"\s+", "", regex=True)
    result["fee_ratio"] = pd.to_numeric(result["fee_ratio"], errors="coerce")
    result = result[
        result["ym"].ne("")
        & result["brand"].ne("")
        & result["store"].ne("")
        & result["fee_ratio"].notna()
    ].copy()
    result["fee_ratio"] = result["fee_ratio"].clip(lower=0, upper=1)
    return result[["ym", "brand", "store", "fee_ratio"]].drop_duplicates(
        ["ym", "brand", "store"],
        keep="last",
    )


def _load_ddangyo_baseline_fee_ratio() -> pd.DataFrame:
    if not DDANGYO_FEE_RATIO_BASELINE_CSV.exists():
        raise RuntimeError(f"땡겨요 기준 수수료율 csv 없음: {DDANGYO_FEE_RATIO_BASELINE_CSV}")
    df = _read_csv_with_fallback(DDANGYO_FEE_RATIO_BASELINE_CSV)
    required = {"ym", "fee_ratio"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"땡겨요 기준 수수료율 필수 컬럼 없음: {sorted(missing)}")

    result = df[list(required)].copy()
    result["ym"] = _normalize_fee_month(result["ym"])
    result["fee_ratio"] = pd.to_numeric(result["fee_ratio"], errors="coerce")
    result = result[result["ym"].ne("") & result["fee_ratio"].notna()].copy()
    result["fee_ratio"] = result["fee_ratio"].clip(lower=0, upper=1)
    return result[["ym", "fee_ratio"]].drop_duplicates("ym", keep="last")


def _load_additional_delivery_revenue() -> pd.DataFrame:
    files = _unified_sales_files()
    if not files:
        return pd.DataFrame(columns=REVENUE_BASE_COLUMNS)

    parts: list[pd.DataFrame] = []
    required = ["sale_date", "brand", "store", "platform", "total_price"]
    read_errors: list[tuple[Path, Exception]] = []
    for path in files:
        try:
            df = pd.read_parquet(path, columns=required)
        except Exception as exc:
            read_errors.append((path, exc))
            continue
        if df.empty:
            continue
        platform = df["platform"].fillna("").astype(str).str.strip()
        matched = df[platform.isin(ADDITIONAL_REVENUE_PLATFORMS)].copy()
        if matched.empty:
            continue
        parts.append(matched)
    _raise_read_errors("unified_sales delivery revenue", read_errors)
    if not parts:
        return pd.DataFrame(columns=REVENUE_BASE_COLUMNS)

    work = pd.concat(parts, ignore_index=True)
    work["sale_date"] = work["sale_date"].fillna("").astype(str).str.strip()
    work["brand"] = work["brand"].fillna("").astype(str).str.strip()
    work["store"] = (
        work["store"].fillna("").astype(str).str.strip().str.replace(r"\s+", "", regex=True)
    )
    work["platform"] = work["platform"].fillna("").astype(str).str.strip()
    work["platform"] = work["platform"].replace(ADDITIONAL_REVENUE_PLATFORM_ALIASES)
    work["total_price"] = pd.to_numeric(
        work["total_price"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    ).fillna(0)
    work = work[
        work["sale_date"].ne("")
        & work["brand"].ne("")
        & work["store"].ne("")
        & work["platform"].isin(ADDITIONAL_REVENUE_PLATFORMS)
    ].copy()
    if work.empty:
        return pd.DataFrame(columns=REVENUE_BASE_COLUMNS)

    work = work.groupby(["sale_date", "brand", "store", "platform"], as_index=False)["total_price"].sum()
    work["ym"] = pd.to_datetime(work["sale_date"], errors="coerce").dt.strftime("%Y_%m")
    work = work.dropna(subset=["ym"]).copy()

    work["fee_ratio"] = work["platform"].map(ADDITIONAL_REVENUE_FEE_RATIOS)
    ddangyo_mask = work["platform"].eq("땡겨요")
    if ddangyo_mask.any():
        monthly = _load_ddangyo_monthly_fee_ratio()
        baseline = _load_ddangyo_baseline_fee_ratio().rename(
            columns={"fee_ratio": "_baseline_fee_ratio"}
        )
        ddangyo = work.loc[ddangyo_mask].drop(columns=["fee_ratio"]).merge(
            monthly,
            on=["ym", "brand", "store"],
            how="left",
        )
        ddangyo = ddangyo.merge(baseline, on="ym", how="left")
        missing_baseline = ddangyo["fee_ratio"].isna() & ddangyo["_baseline_fee_ratio"].isna()
        if missing_baseline.any():
            months = sorted(ddangyo.loc[missing_baseline, "ym"].dropna().unique().tolist())
            raise RuntimeError(f"땡겨요 기준 수수료율 누락: {months}")
        ddangyo["fee_ratio"] = ddangyo["fee_ratio"].fillna(ddangyo["_baseline_fee_ratio"])
        work = pd.concat(
            [work.loc[~ddangyo_mask], ddangyo.drop(columns=["_baseline_fee_ratio"])],
            ignore_index=True,
        )

    work["revenue"] = (work["total_price"] * work["fee_ratio"]).round()
    work["settlement_price"] = work["total_price"] - work["revenue"]
    result = work[REVENUE_BASE_COLUMNS].copy()
    for column in ["total_price", "settlement_price", "revenue"]:
        result[column] = result[column].round().astype("Int64")
    return result


def _load_yogiyo_monthly_gap_rates() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not YOGIYO_SETTLEMENT_MONTHLY_FEE_CSV_PATH.exists():
        raise RuntimeError(f"요기요 월별 수수료율 csv 없음: {YOGIYO_SETTLEMENT_MONTHLY_FEE_CSV_PATH}")

    df = _read_csv_with_fallback(YOGIYO_SETTLEMENT_MONTHLY_FEE_CSV_PATH)
    required = {"ym", "brand", "store", "sales_tot", "settlement_gap_rate"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"요기요 월별 수수료율 필수 컬럼 없음: {sorted(missing)}")

    work = df[list(required)].copy()
    work["ym"] = _normalize_fee_month(work["ym"])
    work["brand"] = work["brand"].fillna("").astype(str).str.strip()
    work["store"] = _normalize_store_name(work["brand"], work["store"])
    work["sales_tot"] = _money(work["sales_tot"])
    work["settlement_gap_rate"] = pd.to_numeric(work["settlement_gap_rate"], errors="coerce")
    work = work[
        work["ym"].ne("")
        & work["brand"].ne("")
        & work["store"].ne("")
        & work["sales_tot"].gt(0)
        & work["settlement_gap_rate"].notna()
    ].copy()
    over_limit = work["settlement_gap_rate"].gt(1)
    if over_limit.any():
        samples = ", ".join(
            f"{row.ym}/{row.brand}/{row.store}/{row.settlement_gap_rate}"
            for row in work.loc[over_limit, ["ym", "brand", "store", "settlement_gap_rate"]]
            .head(10)
            .itertuples(index=False)
        )
        raise RuntimeError(f"요기요 settlement_gap_rate 1 초과: {samples}")

    exact = work[["ym", "brand", "store", "settlement_gap_rate"]].drop_duplicates(
        ["ym", "brand", "store"],
        keep="last",
    )
    baseline = (
        work.assign(_weighted_gap=work["sales_tot"] * work["settlement_gap_rate"])
        .groupby("ym", as_index=False)
        .agg(_weighted_gap=("_weighted_gap", "sum"), sales_tot=("sales_tot", "sum"))
    )
    baseline["baseline_gap_rate"] = baseline["_weighted_gap"] / baseline["sales_tot"]
    baseline = baseline[["ym", "baseline_gap_rate"]]
    return exact, baseline


def _load_yogiyo_delivery_revenue() -> pd.DataFrame:
    files = _unified_sales_files()
    if not files:
        return pd.DataFrame(columns=REVENUE_BASE_COLUMNS)

    parts: list[pd.DataFrame] = []
    required = ["sale_date", "brand", "store", "platform", "total_price"]
    read_errors: list[tuple[Path, Exception]] = []
    for path in files:
        try:
            df = pd.read_parquet(path, columns=required)
        except Exception as exc:
            read_errors.append((path, exc))
            continue
        if df.empty:
            continue
        platform = df["platform"].fillna("").astype(str).str.strip()
        matched = df[platform.isin(YOGIYO_REVENUE_PLATFORM_ALIASES)].copy()
        if not matched.empty:
            parts.append(matched)
    _raise_read_errors("unified_sales yogiyo revenue", read_errors)
    if not parts:
        return pd.DataFrame(columns=REVENUE_BASE_COLUMNS)

    work = pd.concat(parts, ignore_index=True)
    work["sale_date"] = work["sale_date"].fillna("").astype(str).str.strip()
    work["brand"] = work["brand"].fillna("").astype(str).str.strip()
    work["store"] = _normalize_store_name(work["brand"], work["store"])
    work["platform"] = work["platform"].fillna("").astype(str).str.strip().replace(
        YOGIYO_REVENUE_PLATFORM_ALIASES
    )
    work["total_price"] = pd.to_numeric(
        work["total_price"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    ).fillna(0)
    work = work[
        work["sale_date"].ne("")
        & work["brand"].ne("")
        & work["store"].ne("")
        & work["platform"].eq("요기요")
    ].copy()
    if work.empty:
        return pd.DataFrame(columns=REVENUE_BASE_COLUMNS)

    work = (
        work.groupby(["sale_date", "brand", "store", "platform"], as_index=False)["total_price"]
        .sum()
        .reset_index(drop=True)
    )
    work["ym"] = pd.to_datetime(work["sale_date"], errors="coerce").dt.strftime("%Y_%m")
    work = work.dropna(subset=["ym"]).copy()

    exact, baseline = _load_yogiyo_monthly_gap_rates()
    work = work.merge(exact, on=["ym", "brand", "store"], how="left")
    work = work.merge(baseline, on="ym", how="left")
    missing = work["settlement_gap_rate"].isna() & work["baseline_gap_rate"].isna()
    if missing.any():
        months = sorted(work.loc[missing, "ym"].dropna().unique().tolist())
        raise RuntimeError(f"요기요 settlement_gap_rate 누락: {months}")
    work["settlement_gap_rate"] = work["settlement_gap_rate"].fillna(work["baseline_gap_rate"])
    work["revenue"] = (work["total_price"] * work["settlement_gap_rate"]).round()
    work["settlement_price"] = work["total_price"] - work["revenue"]
    result = work[REVENUE_BASE_COLUMNS].copy()
    for column in ["total_price", "settlement_price", "revenue"]:
        result[column] = result[column].round().astype("Int64")
    return result


def _finalize_delivery_revenue(frames: list[pd.DataFrame]) -> pd.DataFrame:
    work = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)

    keys = ["sale_date", "brand", "store", "platform"]
    positive = work["total_price"].fillna(0).gt(0)
    work["_missing_settlement"] = positive & work["settlement_price"].isna()
    work["_missing_revenue"] = positive & work["revenue"].isna()
    result = (
        work.groupby(keys, dropna=False)
        .agg(
            total_price=("total_price", "sum"),
            settlement_price=("settlement_price", "sum"),
            revenue=("revenue", "sum"),
            _missing_settlement=("_missing_settlement", "max"),
            _missing_revenue=("_missing_revenue", "max"),
        )
        .reset_index()
    )
    result.loc[result["_missing_settlement"], "settlement_price"] = pd.NA
    result.loc[result["_missing_revenue"], "revenue"] = pd.NA
    result = result.drop(columns=["_missing_settlement", "_missing_revenue"])
    result[["total_price", "settlement_price", "revenue"]] = result[
        ["total_price", "settlement_price", "revenue"]
    ].round().astype("Int64")
    result = result.sort_values(keys).reset_index(drop=True)
    result = result[REVENUE_BASE_COLUMNS]
    if result.empty:
        raise RuntimeError("delivery_revenue 결과 없음; 기존 마트 보존")
    if result.duplicated(keys).any():
        raise RuntimeError("delivery_revenue 결과 키 중복 발생")
    return result


def _load_store_monthly_rent() -> pd.DataFrame:
    path = ONEDRIVE_DB / "sales_employee.csv"
    if not path.exists():
        raise RuntimeError(f"sales_employee.csv 없음: {path}")

    df = _read_csv_with_fallback(path)
    required = {"매장명", "임대료"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"sales_employee.csv 임대료 필수 컬럼 없음: {sorted(missing)}")

    work = df[["매장명", "임대료"]].copy()
    work["monthly_rent"] = _nullable_money(work["임대료"])
    work = work[work["monthly_rent"].notna() & work["monthly_rent"].gt(0)].copy()
    if work.empty:
        return pd.DataFrame(columns=["brand", "store", "monthly_rent"])

    store_name = work["매장명"].fillna("").astype(str).str.strip()
    work["brand"] = store_name.str.extract(r"^(도리당|나홀로)\s+", expand=False).fillna("")
    work["store"] = _normalize_store_name(work["brand"], store_name)
    work = work[work["brand"].ne("") & work["store"].ne("")].copy()
    if work.empty:
        return pd.DataFrame(columns=["brand", "store", "monthly_rent"])

    distinct = (
        work.groupby(["brand", "store"], dropna=False)["monthly_rent"]
        .nunique(dropna=True)
        .reset_index(name="rent_count")
    )
    conflicts = distinct[distinct["rent_count"].gt(1)]
    if not conflicts.empty:
        samples = ", ".join(
            f"{row.brand}/{row.store}" for row in conflicts.head(10).itertuples(index=False)
        )
        raise RuntimeError(f"매장별 임대료 값 중복 충돌: {samples}")

    result = (
        work.groupby(["brand", "store"], as_index=False)["monthly_rent"]
        .max()
        .sort_values(["brand", "store"])
        .reset_index(drop=True)
    )
    result["monthly_rent"] = result["monthly_rent"].round().astype("Int64")
    return result


def _load_unified_sales_daily_platform_totals() -> pd.DataFrame:
    files = _unified_sales_files()
    if not files:
        return pd.DataFrame(columns=["sale_date", "brand", "store", "platform", "total_price"])

    parts: list[pd.DataFrame] = []
    required = ["sale_date", "brand", "store", "platform", "total_price"]
    read_errors: list[tuple[Path, Exception]] = []
    for path in files:
        try:
            df = pd.read_parquet(path, columns=required)
        except Exception as exc:
            read_errors.append((path, exc))
            continue
        if not df.empty:
            parts.append(df)
    _raise_read_errors("unified_sales cost allocation", read_errors)
    if not parts:
        return pd.DataFrame(columns=required)

    work = pd.concat(parts, ignore_index=True)
    work["sale_date"] = work["sale_date"].fillna("").astype(str).str.strip()
    work["brand"] = work["brand"].fillna("").astype(str).str.strip()
    work["store"] = _normalize_store_name(work["brand"], work["store"])
    work["platform"] = work["platform"].fillna("").astype(str).str.strip()
    work["total_price"] = pd.to_numeric(
        work["total_price"].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    ).fillna(0)
    work = work[
        work["sale_date"].ne("")
        & work["brand"].ne("")
        & work["store"].ne("")
        & work["platform"].ne("")
    ].copy()
    if work.empty:
        return pd.DataFrame(columns=required)

    result = (
        work.groupby(["sale_date", "brand", "store", "platform"], as_index=False)["total_price"]
        .sum()
        .sort_values(["sale_date", "brand", "store", "platform"])
        .reset_index(drop=True)
    )
    result["total_price"] = result["total_price"].round().astype("Int64")
    return result


def _allocate_daily_rent(group: pd.DataFrame) -> pd.Series:
    monthly_rent = int(group["monthly_rent"].iloc[0])
    total = group["total_price"].astype(float).sum()
    if monthly_rent <= 0 or total <= 0:
        return pd.Series([pd.NA] * len(group), index=group.index, dtype="Int64")

    sale_date = pd.to_datetime(group["sale_date"].iloc[0], errors="coerce")
    if pd.isna(sale_date):
        return pd.Series([pd.NA] * len(group), index=group.index, dtype="Int64")
    days_in_month = int(sale_date.days_in_month)
    daily_base = monthly_rent // days_in_month
    daily_remainder = monthly_rent - (daily_base * days_in_month)
    daily_rent = daily_base + (1 if int(sale_date.day) <= daily_remainder else 0)

    raw = group["total_price"].astype(float) / total * daily_rent
    base = (raw // 1).astype(int)
    remainder = daily_rent - int(base.sum())
    allocated = base.copy()
    if remainder > 0:
        fractions = (raw - base).sort_values(ascending=False)
        allocated.loc[fractions.index[:remainder]] += 1
    return allocated.astype("Int64")


def _finalize_store_cost_allocation() -> pd.DataFrame:
    rents = _load_store_monthly_rent()
    if rents.empty:
        raise RuntimeError("store_cost_allocation 임대료 원천 없음; 기존 마트 보존")

    sales = _load_unified_sales_daily_platform_totals()
    if sales.empty:
        raise RuntimeError("store_cost_allocation unified_sales 원천 없음; 기존 마트 보존")

    work = sales.merge(rents, on=["brand", "store"], how="inner")
    if work.empty:
        raise RuntimeError("store_cost_allocation 매칭 결과 없음; 기존 마트 보존")

    work["ym"] = pd.to_datetime(work["sale_date"], errors="coerce").dt.strftime("%Y-%m")
    work = work.dropna(subset=["ym"]).copy()
    work = work[work["total_price"].fillna(0).gt(0)].copy()
    if work.empty:
        raise RuntimeError("store_cost_allocation 양수 매출 결과 없음; 기존 마트 보존")

    keys = ["brand", "store", "ym", "sale_date"]
    work["rent_cost"] = pd.NA
    for _, index in work.groupby(keys).groups.items():
        allocated = _allocate_daily_rent(work.loc[index])
        work.loc[index, "rent_cost"] = allocated.to_numpy()
    result = work[COST_OUTPUT_COLUMNS].copy()
    result[["total_price", "monthly_rent", "rent_cost"]] = result[
        ["total_price", "monthly_rent", "rent_cost"]
    ].round().astype("Int64")
    result = result.sort_values(["sale_date", "brand", "store", "platform"]).reset_index(drop=True)
    if result.empty:
        raise RuntimeError("store_cost_allocation 결과 없음; 기존 마트 보존")
    if result.duplicated(["sale_date", "brand", "store", "platform"]).any():
        raise RuntimeError("store_cost_allocation 결과 키 중복 발생")

    check = result.copy()
    check["ym"] = pd.to_datetime(check["sale_date"], errors="coerce").dt.strftime("%Y-%m")
    check["_days_in_month"] = pd.to_datetime(check["sale_date"], errors="coerce").dt.days_in_month
    check["_day"] = pd.to_datetime(check["sale_date"], errors="coerce").dt.day
    check["_daily_base"] = check["monthly_rent"] // check["_days_in_month"]
    check["_daily_remainder"] = check["monthly_rent"] - (
        check["_daily_base"] * check["_days_in_month"]
    )
    check["_daily_rent"] = check["_daily_base"] + (
        check["_day"].le(check["_daily_remainder"]).astype(int)
    )
    diff = (
        check.groupby(["brand", "store", "ym", "sale_date"], as_index=False)
        .agg(daily_rent=("_daily_rent", "max"), rent_cost=("rent_cost", "sum"))
    )
    mismatch = diff[diff["daily_rent"].ne(diff["rent_cost"])]
    if not mismatch.empty:
        samples = ", ".join(
            f"{row.sale_date}/{row.brand}/{row.store}" for row in mismatch.head(10).itertuples(index=False)
        )
        raise RuntimeError(f"store_cost_allocation 일 임대료 합계 불일치: {samples}")
    return result


def _revenue_rent_basis(revenue: pd.DataFrame, commission: pd.DataFrame) -> pd.DataFrame:
    """임대료는 본인부담 할인 복원 전 매출 비중으로 배분한다."""
    keys = ["sale_date", "brand", "store", "platform"]
    if commission.attrs.get(BAEMIN_DISCOUNT_INCLUDED_ATTR) is not True:
        # 코드 전환 중 구 계산식의 마트를 읽으면 기존 매출을 그대로 사용한다.
        return revenue[keys + ["total_price"]].copy()
    baemin = commission.loc[
        commission["platform"].eq("배달의민족"), keys + ["배민_즉시할인"]
    ]
    work = revenue[keys + ["total_price"]].merge(
        baemin, on=keys, how="left", validate="one_to_one", indicator=True
    )
    missing = work["platform"].eq("배달의민족") & work["_merge"].ne("both")
    if missing.any():
        raise RuntimeError("delivery_revenue 임대료 기준의 배민 수수료 키 누락")
    work["total_price"] = (
        work["total_price"] - _money(work["배민_즉시할인"])
    ).round().astype("Int64")
    return work[keys + ["total_price"]]


def _attach_revenue_costs(
    revenue: pd.DataFrame, *, rent_basis: pd.DataFrame | None = None
) -> pd.DataFrame:
    result = revenue.copy()
    try:
        rents = _load_store_monthly_rent()
    except RuntimeError as exc:
        logger.warning("delivery_revenue rent_cost 생략: %s", exc)
        result["rent_cost"] = 0
        return _apply_fin_revenue(result)

    if rents.empty:
        result["rent_cost"] = 0
        return _apply_fin_revenue(result)

    result = result.drop(columns=["rent_cost"], errors="ignore")
    keys = ["sale_date", "brand", "store", "platform"]
    basis = result[keys + ["total_price"]] if rent_basis is None else rent_basis
    work = basis.merge(rents, on=["brand", "store"], how="inner")
    work["ym"] = pd.to_datetime(work["sale_date"], errors="coerce").dt.strftime("%Y-%m")
    work = work.dropna(subset=["ym"]).copy()
    work = work[work["total_price"].fillna(0).gt(0)].copy()
    if work.empty:
        result["rent_cost"] = 0
        return _apply_fin_revenue(result)

    work["rent_cost"] = pd.NA
    for _, index in work.groupby(["brand", "store", "ym", "sale_date"]).groups.items():
        allocated = _allocate_daily_rent(work.loc[index])
        work.loc[index, "rent_cost"] = allocated.to_numpy()

    rent = work[keys + ["rent_cost"]].copy()
    if rent.duplicated(keys).any():
        raise RuntimeError("delivery_revenue rent_cost 결과 키 중복 발생")
    result = result.merge(rent, on=keys, how="left")
    result["rent_cost"] = result["rent_cost"].fillna(0).round().astype("Int64")
    return _apply_fin_revenue(result)


def _apply_fin_revenue(revenue: pd.DataFrame) -> pd.DataFrame:
    result = revenue.copy()
    cost_columns = [column for column in result.columns if column.endswith("_cost")]
    if cost_columns:
        total_cost = result[cost_columns].fillna(0).sum(axis=1)
    else:
        total_cost = 0
    result["fin_revenue"] = (
        pd.to_numeric(result["revenue"], errors="coerce").fillna(0) - total_cost
    ).round().astype("Int64")
    return _order_revenue_columns(result)


def _order_revenue_columns(revenue: pd.DataFrame) -> pd.DataFrame:
    result = revenue.copy()
    cost_columns = [column for column in result.columns if column.endswith("_cost")]
    non_cost_columns = [
        column
        for column in REVENUE_OUTPUT_COLUMNS
        if not column.endswith("_cost") and column in result.columns
    ]
    extra_non_cost = [
        column
        for column in result.columns
        if not column.endswith("_cost") and column not in non_cost_columns
    ]
    return result[non_cost_columns + extra_non_cost + cost_columns]


def _store_month_fin_fallback(
    actual_fin: float,
    actual_days: int,
    days_in_month: int,
) -> int:
    if actual_days <= 0:
        return 0
    return int(round(actual_fin / actual_days * days_in_month))


def _build_store_month_forecast_model(completed_daily: pd.DataFrame):
    if len(completed_daily) < 14:
        return None
    try:
        from sklearn.ensemble import RandomForestRegressor
    except ImportError:
        logger.warning("scikit-learn 미설치로 store expected fin_revenue 모델 대신 fallback 사용")
        return None

    features = _store_forecast_features(completed_daily)
    model = RandomForestRegressor(
        n_estimators=120,
        random_state=42,
        min_samples_leaf=2,
        n_jobs=1,
    )
    model.fit(features, completed_daily["fin_revenue"].astype(float))
    return model, list(features.columns)


def _store_forecast_features(
    daily: pd.DataFrame,
    feature_columns: list[str] | None = None,
) -> pd.DataFrame:
    work = daily.copy()
    base = pd.DataFrame(
        {
            "day": work["sale_dt"].dt.day.astype(int),
            "month": work["sale_dt"].dt.month.astype(int),
            "dow": work["sale_dt"].dt.dayofweek.astype(int),
            "is_weekend": work["sale_dt"].dt.dayofweek.isin([5, 6]).astype(int),
            "days_in_month": work["sale_dt"].dt.days_in_month.astype(int),
            "days_left": (work["sale_dt"].dt.days_in_month - work["sale_dt"].dt.day).astype(int),
            "store_daily_mean": work["store_daily_mean"].fillna(0).astype(float),
            "store_dow_mean": work["store_dow_mean"].fillna(0).astype(float),
            "brand_dow_mean": work["brand_dow_mean"].fillna(0).astype(float),
            "global_dow_mean": work["global_dow_mean"].fillna(0).astype(float),
        }
    )
    encoded = pd.get_dummies(work[["brand", "store"]].fillna(""), prefix=["brand", "store"])
    result = pd.concat([base, encoded], axis=1)
    if feature_columns is not None:
        result = result.reindex(columns=feature_columns, fill_value=0)
    return result


def _add_store_forecast_stats(target: pd.DataFrame, completed_daily: pd.DataFrame) -> pd.DataFrame:
    result = target.copy()
    global_mean = float(completed_daily["fin_revenue"].mean()) if not completed_daily.empty else 0.0
    global_dow = (
        completed_daily.groupby("dow", as_index=False)["fin_revenue"]
        .mean()
        .rename(columns={"fin_revenue": "global_dow_mean"})
    )
    brand_dow = (
        completed_daily.groupby(["brand", "dow"], as_index=False)["fin_revenue"]
        .mean()
        .rename(columns={"fin_revenue": "brand_dow_mean"})
    )
    store_daily = (
        completed_daily.groupby(["brand", "store"], as_index=False)["fin_revenue"]
        .mean()
        .rename(columns={"fin_revenue": "store_daily_mean"})
    )
    store_dow = (
        completed_daily.groupby(["brand", "store", "dow"], as_index=False)["fin_revenue"]
        .mean()
        .rename(columns={"fin_revenue": "store_dow_mean"})
    )
    result = result.merge(global_dow, on="dow", how="left")
    result = result.merge(brand_dow, on=["brand", "dow"], how="left")
    result = result.merge(store_daily, on=["brand", "store"], how="left")
    result = result.merge(store_dow, on=["brand", "store", "dow"], how="left")
    for column in ["global_dow_mean", "brand_dow_mean", "store_daily_mean", "store_dow_mean"]:
        result[column] = result[column].fillna(global_mean)
    return result


def _apply_store_expected_month_fin_revenue(revenue: pd.DataFrame) -> pd.DataFrame:
    result = revenue.copy()
    result = result.drop(columns=["store_expected_month_fin_revenue"], errors="ignore")
    if result.empty:
        result["store_expected_month_fin_revenue"] = pd.Series(dtype="Int64")
        return _order_revenue_columns(result)

    work = result[["sale_date", "brand", "store", "fin_revenue"]].copy()
    work["sale_dt"] = pd.to_datetime(work["sale_date"], errors="coerce")
    work["fin_revenue"] = pd.to_numeric(work["fin_revenue"], errors="coerce").fillna(0)
    work = work[work["sale_dt"].notna()].copy()
    if work.empty:
        result["store_expected_month_fin_revenue"] = 0
        result["store_expected_month_fin_revenue"] = result[
            "store_expected_month_fin_revenue"
        ].astype("Int64")
        return _order_revenue_columns(result)

    daily = (
        work.groupby(["brand", "store", "sale_dt"], as_index=False)["fin_revenue"]
        .sum()
        .sort_values(["brand", "store", "sale_dt"])
        .reset_index(drop=True)
    )
    daily["ym"] = daily["sale_dt"].dt.to_period("M").astype(str)
    daily["dow"] = daily["sale_dt"].dt.dayofweek
    current_period = daily["sale_dt"].max().to_period("M")
    current_ym = str(current_period)
    completed_daily = daily[daily["sale_dt"].dt.to_period("M") < current_period].copy()
    completed_daily = _add_store_forecast_stats(completed_daily, completed_daily)
    model_info = _build_store_month_forecast_model(completed_daily)

    monthly = (
        daily.groupby(["brand", "store", "ym"], as_index=False)
        .agg(
            actual_fin=("fin_revenue", "sum"),
            actual_days=("sale_dt", "nunique"),
            max_sale_dt=("sale_dt", "max"),
        )
        .reset_index(drop=True)
    )
    monthly["days_in_month"] = monthly["max_sale_dt"].dt.days_in_month
    monthly["store_expected_month_fin_revenue"] = monthly["actual_fin"].round().astype("Int64")

    current_mask = monthly["ym"].eq(current_ym)
    if current_mask.any():
        for index, row in monthly.loc[current_mask].iterrows():
            actual_fin = float(row["actual_fin"])
            actual_days = int(row["actual_days"])
            days_in_month = int(row["days_in_month"])
            max_sale_dt = row["max_sale_dt"]
            if actual_days >= days_in_month or max_sale_dt.day >= days_in_month:
                expected = int(round(actual_fin))
            elif model_info is None or completed_daily.empty:
                expected = _store_month_fin_fallback(actual_fin, actual_days, days_in_month)
            else:
                model, feature_columns = model_info
                future_dates = pd.date_range(
                    max_sale_dt + pd.Timedelta(days=1),
                    max_sale_dt + pd.offsets.MonthEnd(0),
                    freq="D",
                )
                future = pd.DataFrame(
                    {
                        "brand": row["brand"],
                        "store": row["store"],
                        "sale_dt": future_dates,
                    }
                )
                future["dow"] = future["sale_dt"].dt.dayofweek
                future = _add_store_forecast_stats(future, completed_daily)
                predicted = model.predict(_store_forecast_features(future, feature_columns))
                expected = int(round(actual_fin + float(pd.Series(predicted).sum())))
            monthly.loc[index, "store_expected_month_fin_revenue"] = expected

    expected_map = monthly[
        ["brand", "store", "ym", "store_expected_month_fin_revenue"]
    ].copy()
    result["ym"] = pd.to_datetime(result["sale_date"], errors="coerce").dt.to_period("M").astype(str)
    result = result.merge(expected_map, on=["brand", "store", "ym"], how="left")
    result["store_expected_month_fin_revenue"] = (
        result["store_expected_month_fin_revenue"].fillna(0).round().astype("Int64")
    )
    result = result.drop(columns=["ym"])
    return _order_revenue_columns(result)


def build_store_cost_allocation() -> str:
    result = _finalize_store_cost_allocation()

    _write_parquet_atomic(result, STORE_COST_ALLOCATION_PATH, COST_OUTPUT_COLUMNS)
    logger.info(
        "store_cost_allocation mart 저장 완료: %s rows=%s",
        STORE_COST_ALLOCATION_PATH,
        len(result),
    )
    return f"store_cost_allocation {len(result)}행 -> {STORE_COST_ALLOCATION_PATH}"


def build_delivery_revenue() -> str:
    if not DELIVERY_COMMISSION_PATH.exists():
        raise RuntimeError(f"delivery_commission parquet 없음: {DELIVERY_COMMISSION_PATH}")
    commission = pd.read_parquet(DELIVERY_COMMISSION_PATH)
    result = _finalize_delivery_revenue(
        [
            _revenue_from_delivery_commission(commission),
            _load_additional_delivery_revenue(),
            _load_yogiyo_delivery_revenue(),
        ]
    )
    result = _attach_revenue_costs(result, rent_basis=_revenue_rent_basis(result, commission))
    result = _apply_store_expected_month_fin_revenue(result)

    _write_parquet_atomic(result, DELIVERY_REVENUE_PATH, REVENUE_OUTPUT_COLUMNS)
    logger.info("delivery_revenue mart 저장 완료: %s rows=%s", DELIVERY_REVENUE_PATH, len(result))
    return f"delivery_revenue {len(result)}행 -> {DELIVERY_REVENUE_PATH}"
