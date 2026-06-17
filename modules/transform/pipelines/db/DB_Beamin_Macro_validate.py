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
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_Beamin_04_orders import collect_orders_for_account, _COLUMNS
from modules.transform.utility.paths import ANALYTICS_DB, BAEMIN_ORDERS_DB
from modules.transform.pipelines.db.beamin_store_io import (
    find_tables,
    read_file,
    read_table,
    write_table,
)
from modules.transform.utility.store_normalize import normalize as normalize_store_names, strip_brand

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")

TOORDER_DAILY_DIR = ANALYTICS_DB / "toorder_daily_sales"


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

def _toorder_baemin_by_store(target_date: str) -> dict:
    """toorder_daily_sales CSV에서 매장별 배민 플랫폼 매출액 합계를 반환한다.

    Returns:
        {store_name: amount(int)}  파일 없으면 {}
    """
    date_str = target_date.replace("-", "")
    csv_path = TOORDER_DAILY_DIR / f"toorder_daily_sales_{date_str}.csv"
    if not csv_path.exists():
        logger.warning("ToOrder CSV 없음: %s", csv_path)
        return {}
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        mask = df["플랫폼명"].isin(["배달의민족", "배민1", "배민 포장"])
        by_store = (
            df.loc[mask]
            .groupby("매장명")["매출액"]
            .sum()
            .apply(int)
            .to_dict()
        )
        logger.info("ToOrder 배민 매장 %d개 (target_date=%s)", len(by_store), target_date)
        return by_store
    except Exception as exc:
        logger.error("ToOrder CSV 읽기 실패: %s / %s", csv_path, exc)
        return {}


def sum_toorder_baemin_from_csv(target_date: str) -> int:
    """toorder_daily_sales CSV에서 배민 플랫폼 전체 합계를 반환한다 (로깅용)."""
    by_store = _toorder_baemin_by_store(target_date)
    total = sum(by_store.values())
    logger.info("ToOrder 배민 합계: %d원 (target_date=%s)", total, target_date)
    return total


# ============================================================
# Step 2: 배민 orders DB → 매장별 배달완료 합계
# ============================================================

def _baemin_orders_by_store(target_date: str) -> dict:
    """배민 orders DB에서 매장별 배달완료 결제금액 합계를 반환한다.

    같은 매장이 brand=도리당/brand=나홀로에 모두 저장된 경우:
    - 주문번호가 동일하면 (같은 배민 계정) → dedup 후 1건
    - 주문번호가 다르면 (별도 배민 계정, 예: 역삼점) → 브랜드 합산

    Returns:
        {store_name: amount(int)}  해당 날짜 배달완료 건수>0인 매장만
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
                & df["주문시각"].str.startswith(date_prefix, na=False)
                & df["결제금액"].str.strip().ne("")
            )
            store_frames[store_name].append(df.loc[mask, ["주문번호", "결제금액"]])
        except Exception as exc:
            logger.warning("CSV 읽기 실패: %s / %s", csv_path, exc)

    result = {}
    for store_name, frames in store_frames.items():
        combined = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["주문번호"])
        total = int(
            combined["결제금액"]
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0)
            .sum()
        )
        if total > 0:
            result[store_name] = total

    logger.info("배민 orders 수집 매장: %d개 (target_date=%s)", len(result), target_date)
    return result


def sum_baemin_orders_delivered(target_date: str) -> int:
    """배민 orders DB의 배달완료 결제금액 전체 합계 (로깅용)."""
    by_store = _baemin_orders_by_store(target_date)
    return sum(by_store.values())


def _expected_brands_by_store(store_info_per_account: list) -> dict[str, set[str]]:
    expected: dict[str, set[str]] = defaultdict(set)
    for item in store_info_per_account or []:
        for store_info in item.get("stores", []) or []:
            store = str(store_info.get("store") or "").strip()
            brand = str(store_info.get("brand") or "").strip()
            if store and brand:
                expected[store].add(brand)
    return expected


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

        missing = sorted(expected - existing)
        stale = sorted((expected & existing) - active)
        issue_type = None
        if missing:
            issue_type = "missing_partition"
        elif stale:
            issue_type = "stale_partition"

        result[store_name] = {
            "expected_brands": sorted(expected),
            "existing_brands": sorted(existing),
            "active_brands": sorted(active),
            "missing_brands": missing,
            "stale_brands": stale,
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
) -> None:
    """지정된 매장들에 대해서만 배민 주문내역을 재수집한다 (전 브랜드).

    store_info_per_account 내 각 항목의 stores 중
    store["store"] 이 store_names에 포함된 것만 재수집한다.
    """
    pw_map = {a["account_id"]: a["password"] for a in account_list}

    for item in store_info_per_account:
        account_id = item["account_id"]
        all_stores = item.get("stores", [])
        target_stores = [s for s in all_stores if s.get("store") in store_names]
        if not target_stores:
            continue
        password = pw_map.get(account_id)
        if not password:
            logger.warning("비밀번호 없음: %s", account_id)
            continue
        logger.info(
            "재수집: %s → %s",
            account_id,
            [s["store"] for s in target_stores],
        )
        try:
            collect_orders_for_account(account_id, password, target_stores, target_date=target_date)
        except Exception as exc:
            logger.error("재수집 실패 [%s]: %s", account_id, exc, exc_info=True)


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
        "retried_stores": [],
        "matched": False,
        "compared_count": 0,
    }

    # 1. 양쪽 데이터 수집
    toorder_by_store = _toorder_baemin_by_store(target_date)
    baemin_by_store = _baemin_orders_by_store(target_date)

    if not toorder_by_store:
        logger.warning("ToOrder CSV 없음 또는 비어있음 — 검증 건너뜀")
        return result

    # 2. 비교 범위 = 이번 실행에서 실제 수집한 매장 ∩ ToOrder 매장
    #    (수집 대상이 아닌 매장을 배민=0과 비교해 허위 불일치/불필요 재수집하는 것을 방지)
    collected_stores = {
        s.get("store")
        for item in (store_info_per_account or [])
        for s in item.get("stores", [])
        if s.get("store")
    }
    baemin_has_data = {store for store, amount in baemin_by_store.items() if amount > 0}
    compare_stores = set(toorder_by_store) & (collected_stores | baemin_has_data)
    expected_brands = _expected_brands_by_store(store_info_per_account)
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
    missing_brand_stores: list[str] = []
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
            "expected_brands": coverage.get("expected_brands", []),
            "active_brands": coverage.get("active_brands", []),
        }
        if not matched:
            mismatched_first.append(store)
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
            logger.warning("불일치: %s ToOrder=%d / 배민=%d (차이=%d)", store, t, b, t - b)
        else:
            logger.info("일치: %s %d원", store, t)

    result["toorder_gap_stores"] = toorder_gap_stores
    result["missing_brand_stores"] = sorted(set(missing_brand_stores))

    # 3. 전부 일치 → 종료
    if not mismatched_first:
        result["matched"] = True
        return result

    # 4. 불일치 매장 → 삭제 → 재수집
    logger.warning("불일치 %d개 매장 재수집: %s", len(mismatched_first), mismatched_first)
    result["retried_stores"] = mismatched_first

    _delete_orders_for_stores(target_date, mismatched_first)
    _recollect_stores(store_info_per_account, account_list, target_date, set(mismatched_first))

    # 5. 재비교
    baemin_after = _baemin_orders_by_store(target_date)
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
        b = baemin_after.get(store, 0)
        matched = t == b
        coverage = brand_coverage_after.get(store, {})
        result["store_results"][store] = {
            "toorder": t,
            "baemin": b,
            "matched": matched,
            "brand_issue": coverage.get("issue_type"),
            "missing_brands": coverage.get("missing_brands", []),
            "stale_brands": coverage.get("stale_brands", []),
            "expected_brands": coverage.get("expected_brands", []),
            "active_brands": coverage.get("active_brands", []),
        }
        if not matched:
            mismatched_final.append(store)
            if coverage.get("issue_type"):
                missing_brand_stores.append(store)
            logger.warning("재수집 후 불일치: %s ToOrder=%d / 배민=%d", store, t, b)
        else:
            logger.info("재수집 후 일치: %s %d원", store, t)

    result["mismatched_stores"] = mismatched_final
    result["missing_brand_stores"] = sorted(set(missing_brand_stores))
    result["matched"] = len(mismatched_final) == 0
    return result
