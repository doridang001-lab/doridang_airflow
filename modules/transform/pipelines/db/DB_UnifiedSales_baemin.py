"""
배민 직수집(baemin_macro/orders) -> unified_sales 교정 파이프라인.

TEST_STORES에 한해 posfeed 배달의민족 행을 제거하고 직수집 데이터로 대체한다.
"""

import hashlib
import logging
import re
from datetime import timedelta
from glob import glob

import pandas as pd
import pendulum

from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS,
    UNIFIED_ROOT,
    _apply_posfeed_blacklist,
    _load_store_map,
    _lookup_store_meta,
    _make_unified_pk,
    _to_int_series,
    _unified_daily_path,
)

logger = logging.getLogger(__name__)

BAEMIN_SOURCE = "배민수동"
BAEMIN_PLATFORM = "배달의민족"


def reconcile_baemin_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int = 7,
) -> str:
    """
    TEST_STORES의 배달의민족 행을 baemin_macro 직수집 기준으로 교정.

    - 직수집 파일 없는 날짜: 기존 배달의민족 행 제거
    - 직수집 파일은 있지만 해당 날짜 행이 0건인 경우: 기존 배달의민족 행 제거
    - 배민 원천은 주문상태=배달완료, 주문금액=결제금액 기준으로 교정
    """
    if sale_date:
        dates = [str(sale_date)]
    else:
        kst_now = pendulum.now("Asia/Seoul")
        dates = [
            (kst_now - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(lookback_days)
        ]
    ym_list = sorted({d[:7] for d in dates})

    total_added = 0
    total_removed = 0
    store_map = _load_store_map()

    for store in stores:
        for ym in ym_list:
            baemin_files = _find_baemin_files(store, ym)
            if not baemin_files:
                for date in dates:
                    if date[:7] != ym:
                        continue
                    removed, added = _upsert_daily(pd.DataFrame(columns=UNIFIED_COLUMNS), date, store)
                    total_removed += removed
                    total_added += added
                    logger.info(
                        "배민 수집 파일 없음, 기존 배달의민족 행 제거: store=%s date=%s | 제거=%d 추가=%d",
                        store,
                        date,
                        removed,
                        added,
                    )
                continue

            df_raw = _read_baemin_files(baemin_files)
            if df_raw.empty:
                for date in dates:
                    if date[:7] != ym:
                        continue
                    removed, added = _upsert_daily(pd.DataFrame(columns=UNIFIED_COLUMNS), date, store)
                    total_removed += removed
                    total_added += added
                    logger.info(
                        "배민 수집 데이터 없음, 기존 배달의민족 행 제거: store=%s date=%s | 제거=%d 추가=%d",
                        store,
                        date,
                        removed,
                        added,
                    )
                continue

            _assert_required_columns(df_raw, store, ym)
            brand = _parse_brand_from_path(baemin_files[0])

            parsed = df_raw["주문시각"].map(_parse_baemin_datetime)
            df_raw = df_raw.copy()
            df_raw["sale_date"] = parsed.map(lambda v: v[0])
            df_raw["order_time"] = parsed.map(lambda v: v[1])

            for date in dates:
                if date[:7] != ym:
                    continue
                df_day = df_raw[df_raw["sale_date"] == date].copy()
                if df_day.empty:
                    removed, added = _upsert_daily(pd.DataFrame(columns=UNIFIED_COLUMNS), date, store)
                    total_removed += removed
                    total_added += added
                    logger.info(
                        "배민 데이터 없음, 기존 배달의민족 행 제거: store=%s date=%s | 제거=%d 추가=%d",
                        store,
                        date,
                        removed,
                        added,
                    )
                    continue

                df_unified = _transform_to_unified(df_day, store, brand, store_map)
                df_unified = _apply_posfeed_blacklist(df_unified)
                if df_unified.empty:
                    removed, added = _upsert_daily(df_unified, date, store)
                    total_removed += removed
                    total_added += added
                    logger.info(
                        "배민 교정 데이터 없음, 기존 배민 매출 제거: store=%s date=%s | 제거=%d 추가=%d",
                        store,
                        date,
                        removed,
                        added,
                    )
                    continue
                df_unified = _recalculate_order_fields(df_unified)

                removed, added = _upsert_daily(df_unified, date, store)
                total_removed += removed
                total_added += added
                logger.info(
                    "배민 교정 완료: store=%s date=%s | 제거=%d 추가=%d",
                    store,
                    date,
                    removed,
                    added,
                )

    return f"배민수동 교정 완료 | 제거={total_removed}행 추가={total_added}행"


def enforce_baemin_manual_only_for_test_stores(
    stores: list[str],
    sale_date: str | None = None,
    lookback_days: int = 7,
) -> str:
    """TEST_STORES의 배달의민족 최종 방어막.

    모든 채널 적재/재분류가 끝난 뒤, 테스트 매장에 대해
    platform=배달의민족 행은 source=배민수동만 남기고 나머지 source는 제거한다.
    """
    if not stores:
        return "TEST_STORES 없음 - 배민수동 최종 정리 스킵"

    if sale_date:
        dates = [str(sale_date)]
    else:
        kst_now = pendulum.now("Asia/Seoul")
        dates = [
            (kst_now - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(lookback_days)
        ]

    store_set = {str(store).strip() for store in stores if str(store).strip()}
    total_removed = 0
    changed_files = 0

    for date in dates:
        daily_path = _unified_daily_path(date)
        if not daily_path.exists():
            logger.info("배민수동 최종 정리 스킵, 파일 없음: %s", daily_path)
            continue

        df = pd.read_parquet(daily_path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        remove_mask = (
            df["store"].fillna("").astype(str).str.strip().isin(store_set)
            & df["platform"].fillna("").astype(str).str.strip().eq(BAEMIN_PLATFORM)
            & ~df["source"].fillna("").astype(str).str.strip().eq(BAEMIN_SOURCE)
        )
        removed = int(remove_mask.sum())
        if removed == 0:
            logger.info("배민수동 최종 정리 변경 없음: %s", daily_path.name)
            continue

        df_out = df[~remove_mask].reset_index(drop=True)
        for col in ("qty", "unit_price", "total_price", "discount_amount", "order_cnt"):
            if col in df_out.columns:
                df_out[col] = pd.to_numeric(df_out[col], errors="coerce").fillna(0).astype(int)
        df_out = df_out.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        df_out.to_parquet(daily_path, index=False, engine="pyarrow")

        total_removed += removed
        changed_files += 1
        logger.warning(
            "배민수동 최종 정리: %s | 제거=%d | stores=%s",
            daily_path.name,
            removed,
            sorted(df.loc[remove_mask, "store"].dropna().astype(str).unique().tolist()),
        )

    return f"배민수동 최종 정리 완료 | 파일={changed_files} 제거={total_removed}행"


def _find_baemin_files(store: str, ym: str) -> list[str]:
    """brand=* glob으로 baemin_macro orders 파일 탐색 (.parquet 우선, .csv 폴백)."""
    base = ANALYTICS_DB / "baemin_macro" / "orders"
    for ext in ("parquet", "csv"):
        pattern = str(base / "brand=*" / f"store={store}" / f"ym={ym}" / f"orders_{ym}.{ext}")
        files = sorted(glob(pattern))
        if files:
            return files
    return []


def _read_baemin_files(files: list[str]) -> pd.DataFrame:
    dfs = []
    for file_path in files:
        try:
            if file_path.endswith(".parquet"):
                df = pd.read_parquet(file_path)
            else:
                df = pd.read_csv(file_path, dtype=str, encoding="utf-8-sig")
            df["_src_path"] = file_path
            dfs.append(df)
        except Exception as exc:
            logger.warning("배민 파일 읽기 실패: %s | %s", file_path, exc)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _assert_required_columns(df: pd.DataFrame, store: str, ym: str) -> None:
    required = {
        "주문시각",
        "주문번호",
        "주문상태",
        "수령방법",
        "주문내역",
        "주문옵션상세",
        "주문수량",
        "주문옵션금액",
        "결제금액",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise KeyError(f"배민 수집 컬럼 누락: store={store} ym={ym} missing={missing}")


def _parse_brand_from_path(file_path: str) -> str:
    """'...brand=도리당/store=...' 경로에서 brand 추출."""
    match = re.search(r"brand=([^/\\]+)", file_path)
    return match.group(1) if match else ""


def _parse_baemin_datetime(value: str) -> tuple[str, str]:
    """'2026. 06. 15. (월) 오후 10:48:04' -> ('2026-06-15', '22:48:04')."""
    if not isinstance(value, str):
        return "", ""
    match = re.match(
        r"\s*(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.\s*\(.+?\)\s*(오전|오후)\s*(\d{1,2}):(\d{2}):(\d{2})",
        value,
    )
    if not match:
        return "", ""

    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    ampm = match.group(4)
    hour = int(match.group(5))
    minute = int(match.group(6))
    second = int(match.group(7))

    if ampm == "오후" and hour != 12:
        hour += 12
    elif ampm == "오전" and hour == 12:
        hour = 0

    return f"{year:04d}-{month:02d}-{day:02d}", f"{hour:02d}:{minute:02d}:{second:02d}"


def _transform_to_unified(
    df: pd.DataFrame,
    store: str,
    brand: str,
    store_map: dict,
) -> pd.DataFrame:
    """baemin_macro rows -> UNIFIED_COLUMNS DataFrame."""
    df = df[df["주문상태"].fillna("").astype(str).str.strip().eq("배달완료")].copy()
    if df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    out = pd.DataFrame(index=df.index)

    out["sale_date"] = df["sale_date"]
    out["ym"] = df["sale_date"].str[:7]
    out["source"] = BAEMIN_SOURCE
    out["brand"] = brand
    out["store"] = store
    out["platform"] = BAEMIN_PLATFORM
    out["order_id"] = df["주문번호"].fillna("").astype(str).str.strip()
    out["order_time"] = df["order_time"]
    out["sale_type"] = df["주문상태"].map(lambda v: "취소" if "취소" in str(v) else "정상")
    out["order_type"] = df["수령방법"].map(
        lambda v: "배달_포장" if str(v).strip() == "포장" else "배달"
    )
    out["menu_name"] = (
        df["주문내역"].fillna("").astype(str)
        .str.replace(r"\s*외\s*\d+건$", "", regex=True)
        .str.strip()
    )
    out["item_name"] = df["주문옵션상세"].fillna("").astype(str).str.strip()
    out["qty"] = _to_int_series(df["주문수량"]).replace(0, 1)
    out["unit_price"] = _to_int_series(df["주문옵션금액"])
    out["discount_amount"] = 0

    out["item_seq"] = out.groupby("order_id").cumcount().add(1).astype(int).astype(str)
    out["item_total"] = out["unit_price"] * out["qty"]

    order_item_sum = out.groupby("order_id")["item_total"].transform("sum")
    order_amount = _first_nonzero_by_order(out["order_id"], _to_int_series(df["결제금액"]))
    out["total_price"] = (
        (out["item_total"] / order_item_sum.replace(0, 1)) * order_amount
    ).round().astype(int)
    out["item_id"] = out["item_name"].map(
        lambda value: hashlib.md5(value.encode()).hexdigest() if value else ""
    )
    out["담당자"] = _lookup_store_meta(store_map, store, "담당자")
    out["region"] = _lookup_store_meta(store_map, store, "region")
    out["실오픈일"] = _lookup_store_meta(store_map, store, "실오픈일")
    out["collected_at"] = pendulum.now("Asia/Seoul").isoformat()

    out = out.drop(columns=["item_total"], errors="ignore")
    out = _recalculate_order_fields(out)
    return out.reindex(columns=UNIFIED_COLUMNS, fill_value="")


def _first_nonzero_by_order(order_id: pd.Series, values: pd.Series) -> pd.Series:
    """주문 첫 행에만 들어오는 상품금액을 주문 전체 행으로 확산한다."""
    tmp = pd.DataFrame({"order_id": order_id, "value": values})
    order_amounts = (
        tmp[tmp["value"] != 0]
        .groupby("order_id")["value"]
        .first()
        .to_dict()
    )
    fallback_amounts = tmp.groupby("order_id")["value"].max().to_dict()
    return tmp["order_id"].map(lambda key: order_amounts.get(key, fallback_amounts.get(key, 0))).astype(int)


def _recalculate_order_fields(df: pd.DataFrame) -> pd.DataFrame:
    """대표행/order_cnt/_pk를 현재 행 기준으로 재계산한다."""
    if df.empty:
        return df

    df = df.copy()
    df["order_cnt"] = 0

    representative_indexes = []
    for _, group in df.groupby("order_id", sort=False):
        menu_match = group["item_name"].fillna("").astype(str).str.strip().eq(
            group["menu_name"].fillna("").astype(str).str.strip()
        )
        if menu_match.any():
            representative_indexes.append(group[menu_match].index[0])
            continue
        unit_price = pd.to_numeric(group["unit_price"], errors="coerce").fillna(0)
        representative_indexes.append(unit_price.idxmax())

    if representative_indexes:
        df.loc[representative_indexes, "order_cnt"] = df.loc[representative_indexes, "sale_type"].map(
            lambda value: 0 if str(value) == "취소" else 1
        )

    df["_pk"] = _make_unified_pk(df)
    return df


def _upsert_daily(df_new: pd.DataFrame, date: str, store: str) -> tuple[int, int]:
    """
    unified_sales 일별 parquet에서 해당 store의 배달의민족 행을 교체.

    Returns:
        (removed_count, added_count)
    """
    UNIFIED_ROOT.mkdir(parents=True, exist_ok=True)
    daily_path = _unified_daily_path(date)

    if daily_path.exists():
        df_existing = pd.read_parquet(daily_path).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        remove_mask = (
            df_existing["store"].fillna("").astype(str).str.strip().eq(store)
            & df_existing["platform"].fillna("").astype(str).str.strip().eq(BAEMIN_PLATFORM)
        )
        removed_count = int(remove_mask.sum())
        df_existing = df_existing[~remove_mask]
    else:
        df_existing = pd.DataFrame(columns=UNIFIED_COLUMNS)
        removed_count = 0

    df_out = pd.concat([df_existing, df_new], ignore_index=True)
    if "_pk" in df_out.columns:
        before = len(df_out)
        df_out = df_out.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)
        dropped = before - len(df_out)
        if dropped:
            logger.warning("배민 교정 저장 중 _pk 중복 %d행 제거", dropped)

    for col in ("qty", "unit_price", "total_price", "discount_amount", "order_cnt"):
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce").fillna(0).astype(int)

    df_out = df_out.reindex(columns=UNIFIED_COLUMNS, fill_value="")
    df_out.to_parquet(daily_path, index=False, engine="pyarrow")
    return removed_count, len(df_new)
