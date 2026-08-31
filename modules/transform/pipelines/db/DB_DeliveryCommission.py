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
    DELIVERY_COMMISSION_PATH,
)
from modules.transform.utility.store_normalize import lookup_store_key, strip_brand

logger = logging.getLogger(__name__)

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
                "dag_id": "DB_Beamin_Macro_Dags",
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
    for item in triggers:
        try:
            trigger_dag(
                dag_id=item["dag_id"],
                run_id=item["run_id"],
                conf=item["conf"],
            )
            triggered.append(item["run_id"])
        except (DagRunAlreadyExists, MultipleResultsFound):
            existing.append(item["run_id"])
            logger.info("배민 정산예정금액 재수집 DAG run 이미 존재: %s", item["run_id"])

    summary = (
        "배민 정산예정금액 결측 재수집 트리거 "
        f"triggered={len(triggered)} existing={len(existing)} total={len(triggers)}"
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


def _write_parquet_atomic(result: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    try:
        result.to_parquet(temp_path, index=False, engine="pyarrow")
        verified = pd.read_parquet(temp_path)
        if list(verified.columns) != OUTPUT_COLUMNS or len(verified) != len(result):
            raise RuntimeError("delivery_commission 임시 parquet 검증 실패")
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
        baemin["ad_spend"] = baemin["ad_spend"].fillna(0).round().astype(int)
        baemin["wgc_avg_cost"] = baemin["wgc_avg_cost"].fillna(0)
        baemin["wgc_orders"] = baemin["wgc_orders"].fillna(0).astype("Int64")
        baemin["wgc_ctr"] = baemin["wgc_ctr"].fillna(0)
        baemin["settlement_amount"] = (
            baemin["baemin_deposit_amt"]
            - baemin["baemin_partner_instant_discount"]
            - baemin["ad_spend"]
            - baemin["baemin_cash_amt"]
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
    _write_parquet_atomic(result, DELIVERY_COMMISSION_PATH)
    logger.info("delivery_commission mart 저장 완료: %s rows=%s", DELIVERY_COMMISSION_PATH, len(result))
    return f"delivery_commission {len(result)}행 -> {DELIVERY_COMMISSION_PATH}"
