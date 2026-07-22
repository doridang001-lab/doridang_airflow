"""Build delivery commission mart from Baemin and Coupang macro sources."""

import logging
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import (
    BAEMIN_ORDERS_DB,
    BAEMIN_OUR_STORE_CLICKS_DB,
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
]
REQUIRED_BRANDS = {"도리당", "나홀로"}


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
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    ).fillna(0)


def _baemin_order_date(series: pd.Series) -> pd.Series:
    raw = series.astype(str)
    extracted = raw.str.extract(r"(\d{4}\.\s*\d{2}\.\s*\d{2}\.)", expand=False)
    return pd.to_datetime(extracted, format="%Y. %m. %d.", errors="coerce").dt.strftime("%Y-%m-%d")


def _coupang_order_date(series: pd.Series) -> pd.Series:
    raw = series.astype(str)
    extracted = raw.str.extract(r"(\d{4}\.\d{2}\.\d{2})", expand=False)
    return pd.to_datetime(extracted, format="%Y.%m.%d", errors="coerce").dt.strftime("%Y-%m-%d")


def _read_csv_with_fallback(path: Path) -> pd.DataFrame:
    last_exc: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=encoding, dtype=str)
        except Exception as exc:
            last_exc = exc
    raise RuntimeError(f"csv 로드 실패: {path}") from last_exc


def _load_baemin_orders_agg() -> pd.DataFrame:
    columns = ["date", "store", "total_amt", "baemin_deposit_amt"]
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
    required = {"주문번호", "주문시각", "brand", "store", "총결제금액", "입금예정금액"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"baemin 필수 컬럼 없음: {sorted(missing)}")

    df["총결제금액"] = _money(df["총결제금액"])
    df["입금예정금액"] = _money(df["입금예정금액"])
    grouped = df.groupby(["brand", "주문번호"], as_index=False).agg(
        주문시각=("주문시각", "max"),
        store=("store", "max"),
        총결제금액=("총결제금액", "max"),
        입금예정금액=("입금예정금액", "max"),
    )
    grouped["date"] = _baemin_order_date(grouped["주문시각"])
    grouped["store"] = _normalize_store_name(grouped["brand"], grouped["store"])
    grouped = grouped.dropna(subset=["date"])
    grouped = grouped[grouped["store"] != ""]
    if grouped.empty:
        raise RuntimeError("baemin orders 집계 결과 없음; 기존 마트 보존")

    grouped = grouped.groupby(["date", "store"], as_index=False)[["총결제금액", "입금예정금액"]].sum()
    grouped = grouped.rename(
        columns={"총결제금액": "total_amt", "입금예정금액": "baemin_deposit_amt"}
    )
    grouped["total_amt"] = grouped["total_amt"].round().astype(int)
    grouped["baemin_deposit_amt"] = grouped["baemin_deposit_amt"].round().astype(int)
    return grouped[columns]


def _load_baemin_ad_spend_agg() -> pd.DataFrame:
    columns = ["date", "store", "ad_spend"]
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
        missing = {"날짜", "store_name", "광고지출"} - set(df.columns)
        if missing:
            read_errors.append((path, ValueError(f"필수 컬럼 없음: {sorted(missing)}")))
            continue
        out = pd.DataFrame()
        out["date"] = pd.to_datetime(df["날짜"], errors="coerce").dt.strftime("%Y-%m-%d")
        brands = pd.Series(_partition_value(path, "brand"), index=df.index, dtype=str)
        out["store"] = _normalize_store_name(brands, df["store_name"])
        out["ad_spend"] = _money(df["광고지출"])
        parts.append(out)
    _raise_read_errors("baemin ads", read_errors)

    if not parts:
        raise RuntimeError("baemin ads 유효 데이터 없음; 기존 마트 보존")

    result = pd.concat(parts, ignore_index=True)
    result = result.dropna(subset=["date"])
    result = result[result["store"] != ""]
    if result.empty:
        raise RuntimeError("baemin ads 집계 결과 없음; 기존 마트 보존")
    result = result.groupby(["date", "store"], as_index=False)["ad_spend"].sum()
    result["ad_spend"] = result["ad_spend"].round().astype(int)
    return result[columns]


def _load_coupang_orders_agg() -> pd.DataFrame:
    columns = ["date", "store", "total_amt", "coupang_settlement_amt"]
    files = sorted(COUPANG_ORDERS_DB.glob("brand=*/store=*/ym=*/orders_*.parquet"))
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
        parts.append(df)
    _raise_read_errors("coupang orders", read_errors)

    if not parts:
        raise RuntimeError("coupang orders 유효 데이터 없음; 기존 마트 보존")

    df = pd.concat(parts, ignore_index=True)
    required = {"order_id", "order_date", "brand", "store", "매출액", "정산_예정_금액"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"coupang 필수 컬럼 없음: {sorted(missing)}")

    df["매출액"] = _money(df["매출액"])
    df["정산_예정_금액"] = _money(df["정산_예정_금액"])
    grouped = df.groupby(["brand", "order_id"], as_index=False).agg(
        order_date=("order_date", "max"),
        store=("store", "max"),
        매출액=("매출액", "max"),
        정산_예정_금액=("정산_예정_금액", "max"),
    )
    grouped["date"] = _coupang_order_date(grouped["order_date"])
    grouped["store"] = _normalize_store_name(grouped["brand"], grouped["store"])
    grouped = grouped.dropna(subset=["date"])
    grouped = grouped[grouped["store"] != ""]
    if grouped.empty:
        raise RuntimeError("coupang orders 집계 결과 없음; 기존 마트 보존")

    grouped = grouped.groupby(["date", "store"], as_index=False)[["매출액", "정산_예정_금액"]].sum()
    grouped = grouped.rename(
        columns={"매출액": "total_amt", "정산_예정_금액": "coupang_settlement_amt"}
    )
    grouped["total_amt"] = grouped["total_amt"].round().astype(int)
    grouped["coupang_settlement_amt"] = grouped["coupang_settlement_amt"].round().astype(int)
    return grouped[columns]


def _finalize(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if frames:
        result = pd.concat(frames, ignore_index=True)
    else:
        result = pd.DataFrame(columns=["date", "store", "platform", "total_amt", "settlement_amount"])
    if result.empty:
        raise RuntimeError("delivery_commission 결과 없음; 기존 마트 보존")

    result["diff_amt"] = result["total_amt"] - result["settlement_amount"]
    result = result.rename(columns={"date": "sale_date"})
    result = result.sort_values(["sale_date", "store", "platform"]).reset_index(drop=True)
    result = result[OUTPUT_COLUMNS]
    if result.duplicated(["sale_date", "store", "platform"]).any():
        raise RuntimeError("delivery_commission 결과 키 중복 발생")
    if set(result["platform"]) != {"배달의민족", "쿠팡이츠"}:
        raise RuntimeError(f"delivery_commission 플랫폼 누락: {sorted(set(result['platform']))}")
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


def build_delivery_commission() -> str:
    baemin_orders = _load_baemin_orders_agg()
    baemin_ads = _load_baemin_ad_spend_agg()
    baemin = baemin_orders.merge(baemin_ads, on=["date", "store"], how="left")
    if not baemin.empty:
        baemin["ad_spend"] = baemin["ad_spend"].fillna(0)
        baemin["settlement_amount"] = baemin["baemin_deposit_amt"] - baemin["ad_spend"]
        baemin["platform"] = "배달의민족"
        baemin = baemin[["date", "store", "platform", "total_amt", "settlement_amount"]]

    coupang = _load_coupang_orders_agg()
    if not coupang.empty:
        coupang["settlement_amount"] = coupang["coupang_settlement_amt"]
        coupang["platform"] = "쿠팡이츠"
        coupang = coupang[["date", "store", "platform", "total_amt", "settlement_amount"]]

    result = _finalize([df for df in (baemin, coupang) if not df.empty])
    _write_parquet_atomic(result, DELIVERY_COMMISSION_PATH)
    logger.info("delivery_commission mart 저장 완료: %s rows=%s", DELIVERY_COMMISSION_PATH, len(result))
    return f"delivery_commission {len(result)}행 -> {DELIVERY_COMMISSION_PATH}"
