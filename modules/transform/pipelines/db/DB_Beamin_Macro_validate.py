"""배민 주문내역을 토더(ToOrder) 매출과 교차검증한다.

흐름:
  1. OneDrive toorder_daily_sales CSV → 매장별 배민 플랫폼 매출액
  2. 배민 orders DB → 매장별 배달완료 결제금액 (브랜드 합산, 주문번호 dedup)
  3. 공통 매장 per-store 비교
  4. 불일치 매장 → 해당 매장 CSV 행 삭제 → 재수집 → 재비교
  5. 여전히 불일치 → 알림 (DAG 레이어에서 처리)

공개 함수:
    validate_toorder_orders  - 전체 오케스트레이션
"""

import logging
import os
import re
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_Beamin_04_orders import (
    SUSPECT_ZERO_REASON,
    collect_orders_for_account,
    has_orders_no_data_marker,
    orders_no_data_marker_reason,
    _COLUMNS,
)
from modules.transform.utility.paths import ANALYTICS_DB, BAEMIN_ORDERS_DB
from modules.transform.pipelines.db.beamin_store_io import (
    find_tables,
    order_date,
    read_file,
    read_table,
    write_table,
)
from modules.transform.utility.store_normalize import normalize as normalize_store_names, strip_brand

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")

TOORDER_PARQUET_PATH = ANALYTICS_DB / "toorder_daily_store_platform" / "toorder_store_platform_daily.parquet"

# 재수집 전체 시간 예산(초). validate_toorder 태스크의 execution_timeout(30분)보다
# 짧게 잡아, 예산 초과 시 AirflowTaskTimeout으로 죽는 대신 남은 매장을 다음 실행으로
# 넘긴다. 계정 1건 재수집이 최대 ~5분 걸리므로 기본값은 여유를 둔 18분.
_RECOLLECT_BUDGET_SEC = int(os.getenv("BAEMIN_RECOLLECT_BUDGET_SEC", "1080"))
# 계정 1건 재수집에 필요한 최소 잔여 시간(초). 이보다 적게 남으면 시작하지 않는다.
_RECOLLECT_ACCOUNT_RESERVE_SEC = int(os.getenv("BAEMIN_RECOLLECT_ACCOUNT_RESERVE_SEC", "360"))


# ToOrder price / 배민 페이지 TotalSummary 와 같은 기준은 총결제금액이다.
# 결제금액은 만나서결제 주문에서 공란이라 단독으로 쓰면 그 주문이 0원으로 집계된다.
_AMOUNT_COLUMNS: tuple[str, ...] = ("총결제금액", "결제금액")


def _order_amount_series(df: pd.DataFrame) -> pd.Series:
    """행별 주문 금액 (총결제금액 우선, 없으면 결제금액 폴백). 값이 없으면 NA."""
    amount = pd.Series(pd.NA, index=df.index, dtype="Float64")
    for col in _AMOUNT_COLUMNS:
        if col not in df.columns:
            continue
        values = pd.to_numeric(
            df[col].astype(str).str.replace(",", "", regex=False).str.strip(),
            errors="coerce",
        )
        amount = amount.fillna(pd.Series(values.values, index=df.index, dtype="Float64"))
    return amount


def _order_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """주문번호별 금액 프레임(주문번호, amount)으로 정규화한다.

    금액은 주문 첫 행에만 채워지므로 주문번호 단위 max로 집약한다.
    """
    empty = pd.DataFrame({"주문번호": pd.Series(dtype=str), "amount": pd.Series(dtype="int64")})
    if df is None or df.empty or "주문번호" not in df.columns:
        return empty
    frame = pd.DataFrame(
        {
            "주문번호": df["주문번호"].astype(str).str.strip().values,
            "amount": _order_amount_series(df).values,
        }
    )
    frame = frame[frame["주문번호"].ne("")]
    if frame.empty:
        return empty
    grouped = frame.groupby("주문번호", as_index=False)["amount"].max()
    grouped["amount"] = grouped["amount"].fillna(0).astype("int64")
    return grouped


def _merge_order_amounts(frames: list) -> pd.DataFrame:
    """여러 브랜드/파일의 주문 금액 프레임을 주문번호 dedup으로 합친다."""
    usable = [frame for frame in frames if frame is not None and not frame.empty]
    if not usable:
        return pd.DataFrame({"주문번호": pd.Series(dtype=str), "amount": pd.Series(dtype="int64")})
    combined = pd.concat(usable, ignore_index=True)
    merged = combined.groupby("주문번호", as_index=False)["amount"].max()
    merged["amount"] = merged["amount"].fillna(0).astype("int64")
    return merged


def _manual_baemin_store_meta(raw_store_name: str, fallback_name: str = "") -> tuple[str, str]:
    """Returns (store_key, brand) parsed from a manual Baemin CSV source name."""
    text = str(raw_store_name or fallback_name or "").strip()
    if not text:
        return "", ""
    text = re.sub(r"\[.*?\]\s*", "", text).strip()
    brand = "나홀로" if "나홀로" in text else ("도리당" if "도리당" in text else "")
    matches = re.findall(r"[가-힣A-Za-z0-9]+(?:점|지점|분점|직영점)", text)
    branch = matches[-1] if matches else (text.split()[-1] if text.split() else text)
    normalized = f"{brand} {branch}".strip() if brand else branch
    normalized_series = normalize_store_names(pd.Series([normalized]))
    branch_series = strip_brand(normalized_series)
    return str(branch_series.iloc[0]).strip(), brand


def _manual_baemin_filename_fallback(csv_path: Path) -> str:
    stem = csv_path.stem
    stem = re.sub(r"^baemin_orders_", "", stem)
    stem = re.sub(r"_unknown_\d{8}$", "", stem)
    return stem.replace("_", " ").strip()


def import_manual_baemin_csvs(
    target_date: str,
    base_dir: Path,
    file_pattern: str | None = None,
) -> dict[str, int]:
    """Import manual Baemin order CSVs into the monthly orders DB partition."""
    ym = target_date[:7]
    date_prefix = target_date.replace("-", ". ") + "."
    now_str = pendulum.now(KST).isoformat()
    imported: dict[str, int] = {}

    pattern = str(file_pattern or "baemin_orders_*.csv")
    for csv_path in sorted(base_dir.glob(pattern)):
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
        except Exception as exc:
            logger.warning("manual import read failed: %s / %s", csv_path.name, exc)
            continue
        if df.empty:
            continue

        fallback_name = _manual_baemin_filename_fallback(csv_path)
        raw_store = ""
        if "store_name" in df.columns and not df["store_name"].dropna().empty:
            raw_store = str(df["store_name"].dropna().astype(str).iloc[0])
        store_key, brand = _manual_baemin_store_meta(raw_store, fallback_name)
        if not store_key or not brand:
            logger.warning("manual import store parse failed: %s / raw=%s", csv_path.name, raw_store)
            continue

        required = {"주문상태", "주문번호", "주문시각", "결제금액"}
        if not required.issubset(df.columns):
            logger.warning("manual import missing required columns: %s", csv_path.name)
            continue

        target_df = df[
            (df["주문상태"].astype(str) == "배달완료")
            & df["주문시각"].astype(str).str.startswith(date_prefix, na=False)
            & df["결제금액"].astype(str).str.strip().ne("")
        ].copy()
        if target_df.empty:
            continue

        target_df["collected_at"] = now_str
        target_df["store_name"] = store_key
        target_df = target_df.reset_index(drop=True)

        new_df = pd.DataFrame("", index=target_df.index, columns=_COLUMNS, dtype=str)
        for col in _COLUMNS:
            if col in target_df.columns:
                new_df[col] = target_df[col].astype(str).values
        new_df = new_df.fillna("").astype(str)

        stem = BAEMIN_ORDERS_DB / f"brand={brand}" / f"store={store_key}" / f"ym={ym}" / f"orders_{ym}"

        new_order_ids = set(new_df["주문번호"].unique())
        existing = read_table(stem)
        if existing is not None:
            existing = existing[~existing["주문번호"].isin(new_order_ids)]
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        out_path = write_table(combined, stem)

        key = f"{brand}/{store_key}"
        imported[key] = imported.get(key, 0) + len(new_df)
        logger.info("manual import 완료: %s -> %d행 (%s)", key, len(new_df), out_path)

    return imported


# ============================================================
# Step 1: ToOrder CSV → 매장별 배민 합계
# ============================================================

def _toorder_baemin_stats_by_store(target_date: str) -> dict[str, dict[str, int]]:
    """ToOrder parquet에서 매장별 배민 플랫폼 매출액/건수를 반환한다.

    Returns:
        {store_name: {"amount": int, "receipts": int}}  파일 없으면 {}
    """
    if not TOORDER_PARQUET_PATH.exists():
        logger.warning("ToOrder parquet 없음: %s", TOORDER_PARQUET_PATH)
        return {}
    try:
        df = pd.read_parquet(TOORDER_PARQUET_PATH)
        df = df[df["date"].astype(str) == target_date]
        mask = df["platform"].isin(["배달의민족", "배민1", "배민 포장"])
        rows = df.loc[mask].copy()
        if "receipts_num" not in rows.columns:
            rows["receipts_num"] = 0
        grouped = rows.groupby("store").agg(
            amount=("price", "sum"),
            receipts=("receipts_num", "sum"),
        )
        by_store = {
            str(store): {
                "amount": int(pd.to_numeric(row["amount"], errors="coerce") or 0),
                "receipts": int(pd.to_numeric(row["receipts"], errors="coerce") or 0),
            }
            for store, row in grouped.iterrows()
        }
        logger.info("ToOrder 배민 매장 %d개 (target_date=%s)", len(by_store), target_date)
        return by_store
    except Exception as exc:
        logger.error("ToOrder parquet 읽기 실패: %s / %s", TOORDER_PARQUET_PATH, exc)
        return {}


def _toorder_baemin_by_store(target_date: str) -> dict:
    """toorder parquet에서 매장별 배민 플랫폼 매출액 합계를 반환한다.

    Returns:
        {store_name: amount(int)}  파일 없으면 {}
    """
    return {
        store: stats["amount"]
        for store, stats in _toorder_baemin_stats_by_store(target_date).items()
    }


def sum_toorder_baemin_from_csv(target_date: str) -> int:
    """toorder_daily_sales CSV에서 배민 플랫폼 전체 합계를 반환한다 (로깅용)."""
    by_store = _toorder_baemin_by_store(target_date)
    total = sum(by_store.values())
    logger.info("ToOrder 배민 합계: %d원 (target_date=%s)", total, target_date)
    return total


def _normalize_retry_store_names(stores: list | set | tuple | None) -> list[str]:
    return sorted({str(store or "").strip() for store in stores or [] if str(store or "").strip()})


# ============================================================
# Step 2: 배민 orders DB → 매장별 배달완료 합계
# ============================================================

def _baemin_orders_stats_by_store(target_date: str) -> dict[str, dict[str, int]]:
    """배민 orders DB에서 매장별 배달완료 금액/건수를 반환한다.

    금액 기준은 총결제금액(없으면 결제금액 폴백)이다. ToOrder price 및 배민 페이지
    TotalSummary와 같은 기준이라 만나서결제 주문도 누락 없이 집계된다.

    같은 매장이 brand=도리당/brand=나홀로에 모두 저장된 경우:
    - 주문번호가 동일하면 (같은 배민 계정) → dedup 후 1건
    - 주문번호가 다르면 (별도 배민 계정, 예: 역삼점) → 브랜드 합산

    Returns:
        {store_name: {"amount": int, "count": int}}  해당 날짜 금액>0인 매장만
    """
    ym = target_date[:7]
    date_prefix = target_date.replace("-", ". ") + "."

    csv_paths = find_tables(BAEMIN_ORDERS_DB, f"brand=*/store=*/ym={ym}/orders_{ym}")
    logger.info("배민 orders 파일: %d개 (ym=%s)", len(csv_paths), ym)

    store_frames: dict = defaultdict(list)
    for csv_path in csv_paths:
        store_name = csv_path.parts[-3][len("store="):]
        try:
            df = read_file(csv_path)
            if df.empty:
                continue
            mask = (
                (df["주문상태"] == "배달완료")
                & df["주문시각"].astype(str).str.startswith(date_prefix, na=False)
            )
            store_frames[store_name].append(_order_amounts(df.loc[mask]))
        except Exception as exc:
            logger.warning("CSV 읽기 실패: %s / %s", csv_path, exc)

    result: dict[str, dict[str, int]] = {}
    for store_name, frames in store_frames.items():
        combined = _merge_order_amounts(frames)
        total = int(combined["amount"].sum())
        if total > 0:
            result[store_name] = {"amount": total, "count": int(len(combined))}

    logger.info("배민 orders 수집 매장: %d개 (target_date=%s)", len(result), target_date)
    return result


def _baemin_orders_by_store(target_date: str) -> dict:
    """배민 orders DB에서 매장별 배달완료 금액 합계를 반환한다."""
    return {
        store: stats["amount"]
        for store, stats in _baemin_orders_stats_by_store(target_date).items()
    }


def _baemin_orders_brand_totals_by_store(target_date: str) -> dict[str, dict[str, int]]:
    """배민 orders DB에서 매장별/브랜드별 배달완료 금액 합계를 반환한다 (총결제금액 기준)."""
    ym = target_date[:7]
    date_prefix = target_date.replace("-", ". ") + "."
    tables = find_tables(BAEMIN_ORDERS_DB, f"brand=*/store=*/ym={ym}/orders_{ym}")
    totals: dict[str, dict[str, int]] = defaultdict(dict)
    for table_path in tables:
        parts = table_path.parts
        brand = next((part[len("brand="):] for part in parts if part.startswith("brand=")), "")
        store_name = next((part[len("store="):] for part in parts if part.startswith("store=")), "")
        if not brand or not store_name:
            continue
        try:
            df = read_file(table_path)
            if df.empty:
                continue
            mask = (
                (df["주문상태"] == "배달완료")
                & df["주문시각"].astype(str).str.startswith(date_prefix, na=False)
            )
            total = int(_order_amounts(df.loc[mask])["amount"].sum())
            if total > 0:
                totals[store_name][brand] = total
        except Exception as exc:
            logger.warning("브랜드별 배민 orders 합계 읽기 실패: %s / %s", table_path, exc)
    return {store: dict(brand_totals) for store, brand_totals in totals.items()}


def _brand_total_source_mismatch_reason(
    toorder_amount: int,
    baemin_amount: int,
    brand_totals: dict[str, int],
) -> str | None:
    """ToOrder 금액이 특정 브랜드 합계와만 일치하면 원천 브랜드 누락으로 본다."""
    if toorder_amount <= 0 or not brand_totals or len(brand_totals) < 2:
        return None
    matched_brands = [brand for brand, amount in brand_totals.items() if int(amount or 0) == toorder_amount]
    if not matched_brands:
        return None
    missing_amount = int(baemin_amount or 0) - int(toorder_amount or 0)
    if missing_amount <= 0:
        return None
    return (
        "toorder_missing_brand_amount:"
        f"matched={','.join(sorted(matched_brands))};"
        f"brand_totals={brand_totals};missing_amount={missing_amount}"
    )


def sum_baemin_orders_delivered(target_date: str) -> int:
    """배민 orders DB의 배달완료 결제금액 전체 합계 (로깅용)."""
    by_store = _baemin_orders_by_store(target_date)
    return sum(by_store.values())


def _expected_brands_from_orders_history(target_date: str) -> dict[str, set[str]]:
    ym = target_date[:7]
    expected: dict[str, set[str]] = defaultdict(set)
    latest_dates: list[str] = []
    for csv_path in find_tables(BAEMIN_ORDERS_DB, f"brand=*/store=*/ym={ym}/orders_{ym}"):
        try:
            brand = csv_path.parts[-4][len("brand="):]
            store = csv_path.parts[-3][len("store="):]
        except Exception:
            continue
        try:
            df = read_file(csv_path)
        except Exception as exc:
            logger.warning("expected brand fallback read failed: %s / %s", csv_path, exc)
            continue
        if df.empty or "주문시각" not in df.columns:
            continue
        dates = order_date(df["주문시각"])
        valid_dates = sorted({date for date in dates.tolist() if date and date < target_date})
        if not valid_dates:
            continue
        expected[store].add(brand)
        latest_dates.append(valid_dates[-1])

    if not expected:
        return {}

    logger.info(
        "store_info 비어 brand 기대값을 orders 직전 유효 데이터에서 복원: latest_date=%s stores=%d",
        max(latest_dates),
        len(expected),
    )
    return expected


def _expected_brands_by_store(
    store_info_per_account: list,
    target_date: str | None = None,
) -> dict[str, set[str]]:
    expected: dict[str, set[str]] = defaultdict(set)
    for item in store_info_per_account or []:
        for store_info in item.get("stores", []) or []:
            store = str(store_info.get("store") or "").strip()
            brand = str(store_info.get("brand") or "").strip()
            if store and brand:
                expected[store].add(brand)
    if not expected and target_date:
        return _expected_brands_from_orders_history(target_date)
    return expected


def store_info_from_account_list(account_list: list | None) -> list[dict]:
    """account_list의 store_name/store_id 힌트로 최소 store_info_per_account를 복원한다."""
    grouped: dict[str, dict] = {}
    for account in account_list or []:
        if not isinstance(account, dict):
            continue
        account_id = str(account.get("account_id") or "").strip()
        store_id = str(account.get("store_id") or "").strip()
        store_name = str(
            account.get("store")
            or account.get("store_name")
            or account.get("name")
            or ""
        ).strip()
        if not account_id or not store_id or not store_name:
            continue
        normalized = normalize_store_names(pd.Series([store_name]))
        store = str(strip_brand(normalized).iloc[0]).strip()
        brand = str(account.get("brand") or "").strip()
        if not brand:
            brand = "나홀로" if store_name.startswith("나홀로") else "도리당"
        grouped.setdefault(account_id, {"account_id": account_id, "stores": []})["stores"].append(
            {"store_id": store_id, "brand": brand, "store": store}
        )
    return [item for item in grouped.values() if item.get("stores")]


def _collected_store_names(store_info_per_account: list) -> set[str]:
    """이번 DAG 실행에서 실제 수집 대상으로 전달된 매장명 집합."""
    return {
        str(store_info.get("store") or "").strip()
        for item in (store_info_per_account or [])
        for store_info in item.get("stores", []) or []
        if str(store_info.get("store") or "").strip()
    }


def _inspect_brand_coverage(
    target_date: str,
    store_names: set[str],
    expected_brands: dict[str, set[str]] | None = None,
) -> dict[str, dict]:
    """매장별 brand 파티션 존재 여부와 target_date 데이터 존재 여부를 점검한다."""
    ym = target_date[:7]
    date_prefix = target_date.replace("-", ". ") + "."
    result: dict[str, dict] = {}

    for store_name in sorted(store_names):
        expected = set((expected_brands or {}).get(store_name) or [])
        existing: set[str] = set()
        active: set[str] = set()
        no_data: set[str] = set()
        for csv_path in find_tables(BAEMIN_ORDERS_DB, f"brand=*/store={store_name}/ym={ym}/orders_{ym}"):
            brand = csv_path.parts[-4][len("brand="):]
            existing.add(brand)
            try:
                df = read_file(csv_path)
            except Exception as exc:
                logger.warning("brand coverage read failed: %s / %s", csv_path, exc)
                continue
            if "주문시각" not in df.columns:
                continue
            if df["주문시각"].astype(str).str.startswith(date_prefix, na=False).any():
                active.add(brand)

        suspect_zero: set[str] = set()
        for brand in expected:
            if not has_orders_no_data_marker(brand, store_name, target_date):
                continue
            existing.add(brand)
            if orders_no_data_marker_reason(brand, store_name, target_date) == SUSPECT_ZERO_REASON:
                # 직전 영업일 데이터가 있는데 0건으로 읽힌 케이스 → 빈값으로 신뢰하지 않는다.
                suspect_zero.add(brand)
                continue
            active.add(brand)
            no_data.add(brand)

        missing = sorted(expected - existing)
        stale = sorted((expected & existing) - active)
        issue_type = None
        if missing:
            issue_type = "missing_partition"
        elif suspect_zero:
            issue_type = "suspect_no_data"
        elif stale:
            issue_type = "stale_partition"

        result[store_name] = {
            "expected_brands": sorted(expected),
            "existing_brands": sorted(existing),
            "active_brands": sorted(active),
            "missing_brands": missing,
            "stale_brands": stale,
            "no_data_brands": sorted(no_data),
            "suspect_zero_brands": sorted(suspect_zero),
            "issue_type": issue_type,
        }

    return result


# ============================================================
# Step 3: 불일치 매장 삭제 / 재수집
# ============================================================

def _delete_orders_for_stores(target_date: str, store_names: list) -> int:
    """지정 매장들의 orders CSV에서 target_date 행을 제거한다 (전 브랜드).

    Returns:
        삭제된 총 행 수
    """
    ym = target_date[:7]
    date_prefix = target_date.replace("-", ". ") + "."
    deleted_total = 0

    for store_name in store_names:
        csv_paths = find_tables(
            BAEMIN_ORDERS_DB, f"brand=*/store={store_name}/ym={ym}/orders_{ym}"
        )
        for csv_path in csv_paths:
            try:
                df = read_file(csv_path)
                before = len(df)
                df = df[~df["주문시각"].str.startswith(date_prefix, na=False)]
                deleted = before - len(df)
                if deleted > 0:
                    write_table(df, csv_path.with_suffix(""))
                    logger.info(
                        "행 삭제: %s / %s → %d행",
                        csv_path.parts[-4], store_name, deleted,
                    )
                    deleted_total += deleted
            except Exception as exc:
                logger.warning("orders 행 삭제 실패: %s / %s", csv_path, exc)

    logger.info("총 삭제 행: %d (stores=%s)", deleted_total, store_names)
    return deleted_total


def _snapshot_orders_for_stores(target_date: str, store_names: list) -> dict[Path, pd.DataFrame]:
    ym = target_date[:7]
    date_prefix = target_date.replace("-", ". ") + "."
    snapshot: dict[Path, pd.DataFrame] = {}

    for store_name in store_names:
        for csv_path in find_tables(
            BAEMIN_ORDERS_DB, f"brand=*/store={store_name}/ym={ym}/orders_{ym}"
        ):
            try:
                df = read_file(csv_path)
                rows = df[df["주문시각"].str.startswith(date_prefix, na=False)].copy()
                if not rows.empty:
                    snapshot[csv_path] = rows
            except Exception as exc:
                logger.warning("orders snapshot 실패: %s / %s", csv_path, exc)
    logger.info("orders snapshot 완료: stores=%s files=%d", store_names, len(snapshot))
    return snapshot


def _snapshot_partition_key(path: Path) -> tuple[str, str]:
    """스냅샷 경로에서 (brand, store)를 뽑는다."""
    brand_part = next((part for part in path.parts if part.startswith("brand=")), "")
    store_part = next((part for part in path.parts if part.startswith("store=")), "")
    return (
        brand_part[len("brand="):] if brand_part else "",
        store_part[len("store="):] if store_part else "",
    )


def _partition_has_target_rows(path: Path, target_date: str) -> bool:
    date_prefix = target_date.replace("-", ". ") + "."
    try:
        if not path.exists():
            return False
        df = read_file(path)
        if df.empty or "주문시각" not in df.columns:
            return False
        return bool(df["주문시각"].astype(str).str.startswith(date_prefix, na=False).any())
    except Exception as exc:
        logger.warning("재수집 결과 확인 실패: %s / %s", path, exc)
        return False


def _partitions_emptied_after_recollect(
    target_date: str,
    snapshot: dict[Path, pd.DataFrame],
) -> set[tuple[str, str]]:
    """삭제 전에는 데이터가 있었는데 재수집 후 0건이 된 (brand, store)."""
    emptied: set[tuple[str, str]] = set()
    for path, rows in snapshot.items():
        if not isinstance(path, Path) or not isinstance(rows, pd.DataFrame) or rows.empty:
            continue
        if not _partition_has_target_rows(path, target_date):
            emptied.add(_snapshot_partition_key(path))
    return emptied


def _restore_orders_snapshot(
    target_date: str,
    snapshot: dict[Path, pd.DataFrame],
    store_names: set,
) -> int:
    """삭제 전 스냅샷을 되돌린다.

    store_names 원소는 매장명(str) 또는 (brand, store) 튜플 둘 다 허용한다.
    같은 매장이라도 브랜드별로 재수집 성패가 갈리므로 튜플 지정이 정확하다.
    """
    if not snapshot or not store_names:
        return 0

    plain_names = {value for value in store_names if isinstance(value, str)}
    keyed_names = {tuple(value) for value in store_names if not isinstance(value, str)}

    date_prefix = target_date.replace("-", ". ") + "."
    restored = 0
    for path, rows in snapshot.items():
        if not isinstance(path, Path):
            continue
        brand_name, store_name = _snapshot_partition_key(path)
        if store_name not in plain_names and (brand_name, store_name) not in keyed_names:
            continue
        try:
            current = read_file(path) if path.exists() else pd.DataFrame(columns=rows.columns)
            current = current[~current["주문시각"].str.startswith(date_prefix, na=False)].copy()
            combined = pd.concat([current, rows], ignore_index=True)
            write_table(combined, path.with_suffix(""))
            restored += len(rows)
            logger.warning(
                "재수집 실패로 기존 orders snapshot 복원: %s/%s rows=%d",
                brand_name,
                store_name,
                len(rows),
            )
        except Exception as exc:
            logger.error("orders snapshot 복원 실패: %s / %s", path, exc, exc_info=True)
    return restored


# 이전 전체 삭제 함수 (호환성 유지)
def delete_baemin_orders_for_date(target_date: str) -> int:
    ym = target_date[:7]
    date_prefix = target_date.replace("-", ". ") + "."
    csv_paths = find_tables(BAEMIN_ORDERS_DB, f"brand=*/store=*/ym={ym}/orders_{ym}")
    all_stores = list({p.parts[-3][len("store="):] for p in csv_paths})
    return _delete_orders_for_stores(target_date, all_stores)


def _recollect_stores(
    store_info_per_account: list,
    account_list: list,
    target_date: str,
    store_names: set,
) -> dict[str, set[str]]:
    """지정된 매장들에 대해서만 배민 주문내역을 재수집한다 (전 브랜드).

    store_info_per_account 내 각 항목의 stores 중
    store["store"] 이 store_names에 포함된 것만 재수집한다.
    """
    pw_map = {a["account_id"]: a["password"] for a in account_list}
    attempted: set[str] = set()
    succeeded: set[str] = set()
    failed: set[str] = set()
    skipped: set[str] = set()
    seen_targets: set[tuple[str, str]] = set()
    deadline = time.monotonic() + _RECOLLECT_BUDGET_SEC

    for item in store_info_per_account:
        account_id = item["account_id"]
        all_stores = item.get("stores", [])
        target_stores = []
        for store in all_stores:
            store_name = str(store.get("store") or "").strip()
            store_id = str(store.get("store_id") or store_name).strip()
            key = (account_id, store_id)
            if store_name not in store_names or key in seen_targets:
                continue
            seen_targets.add(key)
            target_stores.append(store)
        if not target_stores:
            continue
        password = pw_map.get(account_id)
        if not password:
            logger.warning("비밀번호 없음: %s", account_id)
            continue
        target_store_names = {
            str(s.get("store") or "").strip()
            for s in target_stores
            if s.get("store")
        }

        remaining = deadline - time.monotonic()
        if remaining < _RECOLLECT_ACCOUNT_RESERVE_SEC:
            skipped.update(target_store_names)
            logger.warning(
                "재수집 시간 예산 소진(잔여 %.0fs < %ds), 다음 실행으로 이월: %s → %s",
                remaining,
                _RECOLLECT_ACCOUNT_RESERVE_SEC,
                account_id,
                sorted(target_store_names),
            )
            continue

        logger.info(
            "재수집: %s → %s (잔여 예산 %.0fs)",
            account_id,
            [s["store"] for s in target_stores],
            remaining,
        )
        attempted.update(target_store_names)
        try:
            result = collect_orders_for_account(account_id, password, target_stores, target_date=target_date)
            failed_names = {
                str(item.get("store") or "").strip()
                for item in (result or {}).get("failed") or []
                if item.get("store")
            }
            failed.update(failed_names & target_store_names)
            succeeded.update(target_store_names - failed_names)
        except Exception as exc:
            logger.error("재수집 실패 [%s]: %s", account_id, exc, exc_info=True)
            failed.update(target_store_names)

    if skipped:
        logger.warning(
            "시간 예산으로 재수집하지 못한 매장 %d개 (기존 orders 복원 대상): %s",
            len(skipped),
            sorted(skipped),
        )
    return {
        "attempted": attempted,
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
    }


def _normalize_recollect_result(value, eligible_retry: set[str]) -> dict[str, set[str]]:
    if isinstance(value, dict):
        attempted = set(value.get("attempted") or [])
        succeeded = set(value.get("succeeded") or [])
        failed = set(value.get("failed") or [])
        skipped = set(value.get("skipped") or [])
        # skipped 매장은 삭제만 되고 재수집되지 않았으므로 failed에 포함해
        # 기존 orders snapshot이 반드시 복원되게 한다.
        return {
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": (failed | skipped | (attempted - succeeded)) - succeeded,
            "skipped": skipped,
        }
    succeeded = set(value or [])
    return {
        "attempted": succeeded,
        "succeeded": succeeded,
        "failed": set(eligible_retry) - succeeded,
        "skipped": set(),
    }


def _recollectable_store_names(
    store_info_per_account: list,
    account_list: list,
    store_names: set,
) -> set[str]:
    """비밀번호가 있어 실제 재수집을 시도할 수 있는 매장명만 반환한다."""
    pw_map = {a["account_id"]: a["password"] for a in account_list}
    recollectable: set[str] = set()

    for item in store_info_per_account:
        account_id = item["account_id"]
        if not pw_map.get(account_id):
            continue
        for store in item.get("stores", []):
            store_name = str(store.get("store") or "").strip()
            if store_name in store_names:
                recollectable.add(store_name)

    return recollectable


# 이전 전체 재수집 함수 (호환성 유지)
def recollect_baemin_orders(
    store_info_per_account: list,
    account_list: list,
    target_date: str,
) -> None:
    all_stores = {
        s.get("store")
        for item in store_info_per_account
        for s in item.get("stores", [])
    }
    _recollect_stores(store_info_per_account, account_list, target_date, all_stores)


def _is_amount_only_mismatch(
    store: str,
    toorder_stats: dict[str, dict[str, int]],
    baemin_stats: dict[str, dict[str, int]],
) -> bool:
    """재수집해도 좁혀지지 않는 금액 기준 차이인지.

    배민 건수가 ToOrder 건수 이상이면 배민 쪽에 빠진 주문이 없다는 뜻이다.
    수집 결과는 이미 배민 페이지 TotalSummary와 대조돼 있으므로 다시 받아도
    같은 값이 나온다 → 삭제·재수집 대상에서 뺀다.

    재수집이 의미 있는 경우(= False)는 다음 셋뿐이다.
      - 배민 금액 0 (파티션 소실/미수집)
      - ToOrder 건수 > 배민 건수 (배민 쪽 주문 누락)
      - ToOrder 건수 정보 없음 (판단 근거가 없어 안전하게 재수집)
    """
    toorder_receipts = int((toorder_stats.get(store) or {}).get("receipts") or 0)
    baemin_count = int((baemin_stats.get(store) or {}).get("count") or 0)
    return bool(baemin_count > 0 and toorder_receipts > 0 and baemin_count >= toorder_receipts)


# ============================================================
# 공개 오케스트레이션
# ============================================================

def validate_toorder_orders(
    account_list: list,
    store_info_per_account: list,
    target_date: str,
) -> dict:
    """ToOrder CSV 매출과 배민 orders CSV를 매장별로 교차검증한다.

    비교 기준:
      - 배민 orders DB가 보유한 매장 중 ToOrder CSV에도 존재하는 매장만 비교
      - 같은 매장이 brand=도리당/나홀로 양쪽에 있으면 브랜드 합산 (주문번호 dedup)

    불일치 매장 처리:
      - 해당 매장(전 브랜드)의 target_date 행 삭제 → 재수집 → 재비교

    Parameters:
        account_list:           배민 계정 목록 (XCom from load_accounts)
        store_info_per_account: 매장 메타데이터 (XCom from collect_all)
        target_date:            검증 대상 날짜 "YYYY-MM-DD"

    Returns:
        {
            "store_results":      {store: {"toorder": int, "baemin": int, "matched": bool}},
            "mismatched_stores":  [str],   # 최종 불일치 매장
            "retried_stores":     [str],   # 재수집 시도한 매장
            "matched":            bool,    # 비교 대상 전체 일치 여부
            "compared_count":     int,     # 비교한 매장 수
        }
    """
    result: dict = {
        "store_results": {},
        "mismatched_stores": [],
        "missing_brand_stores": [],
        "source_mismatch_stores": [],
        "possible_source_mismatch_stores": [],
        "amount_only_mismatch_stores": [],
        "retried_stores": [],
        "retry_failed_stores": [],
        "retry_skipped_stores": [],
        "restored_stores": [],
        "matched": False,
        "compared_count": 0,
    }

    if not store_info_per_account:
        fallback_store_info = store_info_from_account_list(account_list)
        if fallback_store_info:
            store_info_per_account = fallback_store_info
            result["store_info_fallback"] = "account_list"
            logger.info("account_list에서 store_info 복원: accounts=%d", len(fallback_store_info))

    # 1. 양쪽 데이터 수집 (금액은 공개 래퍼, 건수는 stats 보조 조회)
    toorder_by_store = _toorder_baemin_by_store(target_date)
    baemin_by_store = _baemin_orders_by_store(target_date)
    toorder_stats = _toorder_baemin_stats_by_store(target_date)
    baemin_stats = _baemin_orders_stats_by_store(target_date)
    baemin_brand_totals_by_store = _baemin_orders_brand_totals_by_store(target_date)

    if not toorder_by_store:
        logger.warning("ToOrder CSV 없음 또는 비어있음 — 검증 건너뜀")
        return result

    # 2. 비교 범위 = 이번 실행에서 실제 수집한 매장 ∩ ToOrder 매장
    #    (수집 대상이 아닌 매장을 배민=0과 비교해 허위 불일치/불필요 재수집하는 것을 방지)
    collected_stores = _collected_store_names(store_info_per_account)
    baemin_has_data = {store for store, amount in baemin_by_store.items() if amount > 0}
    compare_scope = collected_stores or baemin_has_data
    compare_stores = set(toorder_by_store) & compare_scope
    expected_brands = _expected_brands_by_store(store_info_per_account, target_date)
    brand_coverage = _inspect_brand_coverage(target_date, compare_stores, expected_brands)
    result["compared_count"] = len(compare_stores)
    logger.info(
        "brand coverage snapshot before retry: %s",
        {
            store: {
                "expected": info.get("expected_brands", []),
                "active": info.get("active_brands", []),
                "missing": info.get("missing_brands", []),
                "stale": info.get("stale_brands", []),
                "issue": info.get("issue_type"),
            }
            for store, info in brand_coverage.items()
        },
    )
    logger.info(
        "비교 매장 수: %d개 (수집=%d, ToOrder=%d, 배민=%d)",
        len(compare_stores), len(collected_stores), len(toorder_by_store), len(baemin_by_store),
    )

    mismatched_first: list[str] = []
    amount_only_stores: list[str] = []
    missing_brand_stores: list[str] = []
    source_mismatch_stores: list[str] = []
    possible_source_mismatch_stores: list[str] = []
    toorder_gap_stores: list[str] = []          # ToOrder 계정 연결 끊김 의심 매장
    for store in sorted(compare_stores):
        coverage = brand_coverage.get(store, {})
        t = toorder_by_store[store]
        b = baemin_by_store.get(store, 0)       # 배민에 없으면 0으로 처리

        # 둘 다 0 → 당일 배달 없음 → 비교 건너뜀
        if t == 0 and b == 0:
            logger.info("당일 배달 없음 (skip): %s", store)
            continue

        # ToOrder=0 이지만 배민 실데이터 있으면 → ToOrder 갭 (계정연결 문제)
        if t == 0 and b > 0:
            toorder_gap_stores.append(store)
            result["store_results"][store] = {
                "toorder": t, "baemin": b, "matched": False, "toorder_gap": True,
            }
            logger.warning("ToOrder 갭 의심 (계정연결?): %s ToOrder=0 / 배민=%d", store, b)
            continue

        matched = t == b
        result["store_results"][store] = {
            "toorder": t,
            "baemin": b,
            "matched": matched,
            "toorder_gap": False,
            "brand_issue": coverage.get("issue_type"),
            "missing_brands": coverage.get("missing_brands", []),
            "stale_brands": coverage.get("stale_brands", []),
            "no_data_brands": coverage.get("no_data_brands", []),
            "expected_brands": coverage.get("expected_brands", []),
            "active_brands": coverage.get("active_brands", []),
        }
        if not matched:
            mismatched_first.append(store)
            if not coverage.get("issue_type") and _is_amount_only_mismatch(
                store, toorder_stats, baemin_stats
            ):
                amount_only_stores.append(store)
                result["store_results"][store]["amount_only"] = True
                logger.warning(
                    "건수 동일·금액만 차이(재수집 제외): %s ToOrder=%d(%d건) / 배민=%d(%d건)",
                    store,
                    t,
                    int((toorder_stats.get(store) or {}).get("receipts") or 0),
                    b,
                    int((baemin_stats.get(store) or {}).get("count") or 0),
                )
            if coverage.get("issue_type"):
                missing_brand_stores.append(store)
                logger.warning(
                    "brand coverage issue: %s type=%s expected=%s active=%s missing=%s stale=%s",
                    store,
                    coverage.get("issue_type"),
                    coverage.get("expected_brands", []),
                    coverage.get("active_brands", []),
                    coverage.get("missing_brands", []),
                    coverage.get("stale_brands", []),
                )
            else:
                source_reason = _brand_total_source_mismatch_reason(
                    t,
                    b,
                    baemin_brand_totals_by_store.get(store, {}),
                )
                if source_reason:
                    source_mismatch_stores.append(store)
                    result["store_results"][store]["source_mismatch"] = True
                    result["store_results"][store]["source_mismatch_reason"] = source_reason
                    logger.warning(
                        "원천 브랜드 누락형 금액 차이, 재수집 제외: %s reason=%s",
                        store,
                        source_reason,
                    )
            if (
                store not in source_mismatch_stores
                and not coverage.get("issue_type")
                and t > 0
                and (b > 0 or coverage.get("no_data_brands"))
            ):
                possible_source_mismatch_stores.append(store)
                result["store_results"][store]["possible_source_mismatch"] = True
                logger.warning(
                    "원천 금액 차이 가능성, 자동 재수집 후보 유지: %s ToOrder=%d / 배민=%d (차이=%d)",
                    store,
                    t,
                    b,
                    t - b,
                )
            logger.warning("불일치: %s ToOrder=%d / 배민=%d (차이=%d)", store, t, b, t - b)
        else:
            logger.info("일치: %s %d원", store, t)

    result["toorder_gap_stores"] = toorder_gap_stores
    result["amount_only_mismatch_stores"] = sorted(set(amount_only_stores))
    result["missing_brand_stores"] = sorted(set(missing_brand_stores))
    result["source_mismatch_stores"] = sorted(set(source_mismatch_stores))
    result["possible_source_mismatch_stores"] = sorted(set(possible_source_mismatch_stores))

    # 3. 전부 일치 → 종료
    if not mismatched_first:
        result["matched"] = True
        return result

    # 4. 불일치 매장 → 삭제 → 재수집
    # 이번 실행의 수집 대상이 아닌 매장은 삭제하지 않는다. 삭제 후 재수집 대상에서
    # 빠지면 기존 정상 배민 금액이 0으로 보이는 허위 불일치가 발생한다.
    amount_only_set = set(amount_only_stores)
    retry_candidates = [
        store for store in mismatched_first
        if (not collected_stores or store in collected_stores)
        and store not in source_mismatch_stores
        and store not in amount_only_set
    ]
    source_mismatch_set = set(source_mismatch_stores)
    skipped_retry = sorted(
        set(mismatched_first) - set(retry_candidates) - source_mismatch_set - amount_only_set
    )
    if skipped_retry:
        logger.warning("재수집 범위 밖 불일치 매장 보존: %s", skipped_retry)
    if source_mismatch_stores:
        logger.warning("원천 금액 차이 매장 재수집 제외: %s", sorted(source_mismatch_set))
    if amount_only_set:
        logger.warning(
            "건수 동일·금액만 차이라 재수집해도 좁혀지지 않음, 재수집 제외: %s",
            sorted(amount_only_set),
        )

    attempted_retry: set[str] = set()
    blocked_retry: list[str] = []
    if retry_candidates:
        retry_candidate_set = set(retry_candidates)
        eligible_retry = sorted(
            _recollectable_store_names(store_info_per_account, account_list, retry_candidate_set)
        )
        blocked_retry = sorted(retry_candidate_set - set(eligible_retry))
        if blocked_retry:
            logger.warning("재수집 계정/비밀번호 없음, 기존 orders 보존: %s", blocked_retry)
            result["retry_failed_stores"] = blocked_retry

        logger.warning("불일치 %d개 매장 재수집: %s", len(eligible_retry), eligible_retry)
        result["retried_stores"] = eligible_retry

        if eligible_retry:
            retry_snapshot = _snapshot_orders_for_stores(target_date, eligible_retry)
            _delete_orders_for_stores(target_date, eligible_retry)
            recollect_result = _normalize_recollect_result(
                _recollect_stores(
                    store_info_per_account,
                    account_list,
                    target_date,
                    set(eligible_retry),
                ),
                set(eligible_retry),
            )
            attempted_retry = recollect_result["attempted"]
            succeeded_retry = recollect_result["succeeded"]
            failed_retry = recollect_result["failed"]
            skipped_retry = recollect_result.get("skipped") or set()
            if failed_retry:
                _restore_orders_snapshot(target_date, retry_snapshot, failed_retry)
                logger.warning(
                    "재수집 실패 매장 기존 orders 보존: %s",
                    sorted(failed_retry),
                )
            # 성공으로 보고됐더라도 삭제 전 데이터가 있던 파티션이 0건이 됐으면 소실이다.
            emptied = _partitions_emptied_after_recollect(target_date, retry_snapshot)
            if emptied:
                restored_rows = _restore_orders_snapshot(target_date, retry_snapshot, emptied)
                emptied_stores = {store for _brand, store in emptied}
                failed_retry |= emptied_stores
                succeeded_retry -= emptied_stores
                result["restored_stores"] = sorted(f"{brand}/{store}" for brand, store in emptied)
                logger.error(
                    "재수집 후 0건 파티션 감지, 삭제 전 orders 복원: %s (rows=%d)",
                    result["restored_stores"],
                    restored_rows,
                )
        else:
            succeeded_retry = set()
            failed_retry = set()
            skipped_retry = set()
    else:
        result["retried_stores"] = []
        succeeded_retry = set()
        failed_retry = set()
        skipped_retry = set()

    result["retry_succeeded_stores"] = sorted(succeeded_retry)
    result["retry_failed_stores"] = _normalize_retry_store_names(set(failed_retry) | set(blocked_retry))
    result["retry_skipped_stores"] = sorted(skipped_retry)

    # 5. 재비교
    baemin_after = _baemin_orders_by_store(target_date)
    _baemin_stats_after = _baemin_orders_stats_by_store(target_date)
    baemin_brand_totals_after = _baemin_orders_brand_totals_by_store(target_date)
    brand_coverage_after = _inspect_brand_coverage(target_date, set(mismatched_first), expected_brands)
    logger.info(
        "brand coverage snapshot after retry: %s",
        {
            store: {
                "expected": info.get("expected_brands", []),
                "active": info.get("active_brands", []),
                "missing": info.get("missing_brands", []),
                "stale": info.get("stale_brands", []),
                "issue": info.get("issue_type"),
            }
            for store, info in brand_coverage_after.items()
        },
    )
    mismatched_final: list[str] = []
    for store in mismatched_first:
        t = toorder_by_store[store]
        if store in succeeded_retry:
            b = baemin_after.get(store, 0)
        else:
            b = baemin_by_store.get(store, 0)
        matched = t == b
        coverage = brand_coverage_after.get(store, {})
        result["store_results"][store] = {
            "toorder": t,
            "baemin": b,
            "matched": matched,
            "brand_issue": coverage.get("issue_type"),
            "missing_brands": coverage.get("missing_brands", []),
            "stale_brands": coverage.get("stale_brands", []),
            "no_data_brands": coverage.get("no_data_brands", []),
            "expected_brands": coverage.get("expected_brands", []),
            "active_brands": coverage.get("active_brands", []),
        }
        if not matched:
            mismatched_final.append(store)
            if not coverage.get("issue_type") and _is_amount_only_mismatch(
                store, toorder_stats, _baemin_stats_after
            ):
                amount_only_stores.append(store)
                result["store_results"][store]["amount_only"] = True
            if coverage.get("issue_type"):
                missing_brand_stores.append(store)
            else:
                source_reason = _brand_total_source_mismatch_reason(
                    t,
                    b,
                    baemin_brand_totals_after.get(store) or baemin_brand_totals_by_store.get(store, {}),
                )
                if source_reason:
                    source_mismatch_stores.append(store)
                    result["store_results"][store]["source_mismatch"] = True
                    result["store_results"][store]["source_mismatch_reason"] = source_reason
            if (
                store not in source_mismatch_stores
                and not coverage.get("issue_type")
                and t > 0
                and (b > 0 or coverage.get("no_data_brands"))
            ):
                possible_source_mismatch_stores.append(store)
                result["store_results"][store]["possible_source_mismatch"] = True
            if store in succeeded_retry:
                logger.warning("재수집 후 불일치: %s ToOrder=%d / 배민=%d", store, t, b)
            elif store in failed_retry:
                logger.warning("재수집 실패, 기존 금액 유지: %s ToOrder=%d / 배민=%d", store, t, b)
            else:
                logger.warning("원천 금액 차이 유지: %s ToOrder=%d / 배민=%d", store, t, b)
        else:
            if store in succeeded_retry:
                logger.info("재수집 후 일치: %s %d원", store, t)
            else:
                logger.info("일치 유지: %s %d원", store, t)

    source_mismatch_set = set(source_mismatch_stores)
    actionable_mismatches = [store for store in mismatched_final if store not in source_mismatch_set]
    result["mismatched_stores"] = actionable_mismatches
    result["amount_only_mismatch_stores"] = sorted(set(amount_only_stores) - source_mismatch_set)
    result["missing_brand_stores"] = sorted(set(missing_brand_stores) - source_mismatch_set)
    result["source_mismatch_stores"] = sorted(source_mismatch_set)
    result["possible_source_mismatch_stores"] = sorted(set(possible_source_mismatch_stores))
    result["matched"] = len(actionable_mismatches) == 0
    return result
