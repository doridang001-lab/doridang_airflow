"""
unified_sales 채널별 파이프라인 공통 모듈.

- 스키마/저장 로직
- store 메타 로드(담당자/region/실오픈일)
- 공통 정규화 유틸
- 기존 parquet 재저장/재분류 유틸
"""

import logging
import hashlib
import json
import os
import re
from datetime import datetime

import pandas as pd
import pendulum

from modules.transform.utility.paths import (
    FIN_PRODUCT_CSV_PATH,
    LOCAL_DB,
    MART_DB,
    ONEDRIVE_DB,
    POSFEED_WHITELIST_CSV_PATH,
    existing_fin_product_csv_path,
)
from modules.transform.pipelines.db.DB_ItemIdAllocator import canonical_source

logger = logging.getLogger(__name__)

UNIFIED_ROOT = MART_DB / "unified_sales_grp"
MANUAL_FALLBACK_MARKER_ROOT = LOCAL_DB / "manual_fallback_markers"


def _kst_today_str() -> str:
    return pendulum.now("Asia/Seoul").strftime("%Y-%m-%d")


def iter_unified_sales_files() -> list:
    """실제 unified_sales parquet만 반환한다.

    백필 백업 파일은 `unified_sales_YYMMDD.bak_*.parquet` 형태라 단순 glob에
    같이 잡힌다. 운영 집계/검증/정리에서는 반드시 제외해야 한다.
    """
    if not UNIFIED_ROOT.exists():
        return []
    return sorted(
        path
        for path in UNIFIED_ROOT.glob("unified_sales_*.parquet")
        if ".bak_" not in path.name
    )


def pos_delivery_summary(
    date: str,
    store: str,
    platforms: set[str],
    manual_source: str,
) -> tuple[int, int, int]:
    """수동 결측 시 유지될 비수동 배달행의 금액/주문수/행수를 반환한다."""
    path = _unified_daily_path(date)
    if not path.exists():
        return 0, 0, 0

    try:
        df = pd.read_parquet(
            path,
            columns=["store", "platform", "source", "total_price", "order_cnt"],
        )
    except Exception as exc:
        logger.warning("수동 폴백 요약 로드 실패: %s | %s", path, exc)
        return 0, 0, 0

    if df.empty:
        return 0, 0, 0

    store_s = df["store"].fillna("").astype(str).str.strip()
    platform_s = df["platform"].fillna("").astype(str).str.strip()
    source_s = df["source"].fillna("").astype(str).str.strip()
    mask = (
        store_s.eq(str(store).strip())
        & platform_s.isin(platforms)
        & ~source_s.eq(str(manual_source).strip())
    )
    if not mask.any():
        return 0, 0, 0

    total_price = pd.to_numeric(df.loc[mask, "total_price"], errors="coerce").fillna(0).sum()
    order_cnt = pd.to_numeric(df.loc[mask, "order_cnt"], errors="coerce").fillna(0).sum()
    return int(total_price), int(order_cnt), int(mask.sum())


def record_manual_fallback_marker(
    source: str,
    store: str,
    date: str,
    meta: dict,
) -> bool:
    """폴백 마커를 기록하고 신규 생성 여부를 반환한다."""
    try:
        path = MANUAL_FALLBACK_MARKER_ROOT / str(source).strip() / str(store).strip() / f"{date}.json"
        is_new = not path.exists()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(meta or {})
        payload.update(
            {
                "source": str(source).strip(),
                "store": str(store).strip(),
                "date": str(date).strip(),
                "updated_at": pendulum.now("Asia/Seoul").isoformat(),
            }
        )
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return is_new
    except Exception as exc:
        logger.warning("수동 폴백 마커 기록 실패: source=%s store=%s date=%s | %s", source, store, date, exc)
        return False


def clear_manual_fallback_marker(source: str, store: str, date: str) -> bool:
    """수동 복원 시 폴백 마커를 삭제한다."""
    try:
        path = MANUAL_FALLBACK_MARKER_ROOT / str(source).strip() / str(store).strip() / f"{date}.json"
        if not path.exists():
            return False
        path.unlink()
        return True
    except Exception as exc:
        logger.warning("수동 폴백 마커 삭제 실패: source=%s store=%s date=%s | %s", source, store, date, exc)
        return False


def notify_manual_fallback(source_label: str, events: list[dict]) -> None:
    """수동 결측 POS 폴백 신규 이벤트를 Telegram으로 1회 알린다."""
    if not events:
        return
    try:
        from modules.transform.utility.notifier import send_telegram

        lines = [f"[도리당] 배달 수동 결측→POS 대체({source_label})"]
        for event in events:
            amount = int(event.get("total_price") or 0)
            order_cnt = int(event.get("order_cnt") or 0)
            platform = str(event.get("platform") or "").strip()
            lines.append(
                f"- {event.get('store')} {event.get('date')} {platform} {amount:,}/{order_cnt}건"
            )
        lines.append("재수집 요망")
        send_telegram("\n".join(lines))
    except Exception as exc:
        logger.warning("수동 폴백 알림 실패: source_label=%s | %s", source_label, exc)

# 테스트매장
_BASE_DELIVERY_MANUAL_TEST_STORES = [
    "해운대중동점",
    "법흥리점",
    "송파삼전점",
    "동탄영천점",
    "중랑면목점",
    "시흥배곧점",
    "강원영월점",
    "평택비전점",
    "부산장림점",
    "경북상주점",
    "창원내서점",
    "행신점",
    "전주전북대점",
    "구로디지털점",
    "부천옥길점"
]

# 사용법:
# 1. 새 테스트매장을 임시로 적용할 때
#    - ADD_TEST_STORES에 매장명을 넣는다.
#    - 예: ADD_TEST_STORES = ["창원내서점"]
#    - DAG의 partial_store_mode에서 stores를 생략하면 이 목록까지 자동 포함된다.
# 2. 수집 오류 등으로 테스트매장 정책에서 잠시 빼야 할 때
#    - EXCLUDE_TEST_STORES에 매장명을 넣는다.
#    - 예: EXCLUDE_TEST_STORES = ["창원내서점"]
#    - 기본 목록이나 ADD_TEST_STORES에 있어도 제외 목록에 있으면 최종 대상에서 빠진다.
# 3. 테스트가 끝나고 정식 운영 매장으로 확정할 때
#    - ADD_TEST_STORES의 매장을 _BASE_DELIVERY_MANUAL_TEST_STORES로 옮긴다.
#    - 옮긴 뒤 ADD_TEST_STORES에서는 삭제한다.
# 4. 최종 적용 대상
#    - DELIVERY_MANUAL_TEST_STORES = 기본 목록 + 임시 추가 목록 - 임시 제외 목록.
#    - 운영 로직과 DAG는 이 최종 목록만 참조한다.

# 임시추가
ADD_TEST_STORES = [ "삼송점"
]

# 제외시 사용
EXCLUDE_TEST_STORES = [
]

_EXCLUDED_TEST_STORE_SET = {str(store).strip() for store in EXCLUDE_TEST_STORES if str(store).strip()}

DELIVERY_MANUAL_TEST_STORES = [
    store
    for store in dict.fromkeys(_BASE_DELIVERY_MANUAL_TEST_STORES + ADD_TEST_STORES)
    if str(store).strip() and str(store).strip() not in _EXCLUDED_TEST_STORE_SET
]

# POS 원천이 없어 나머지 채널을 toorder로 보충하는 매장.
TOORDER_MANUAL_STORES = ["해운대중동점"]

DELIVERY_PLATFORM_FAMILIES = {
    "배민수동": {"배달의민족", "배민1", "배민 포장", "배민 사장"},
    "쿠팡수동": {"쿠팡이츠", "쿠팡 포장"},
}

PLATFORM_TO_MANUAL_SOURCE = {
    platform: source
    for source, platforms in DELIVERY_PLATFORM_FAMILIES.items()
    for platform in platforms
}

UNIFIED_COLUMNS = [
    "sale_date",
    "ym",
    "source",
    "brand",
    "store",
    "region",
    "담당자",
    "실오픈일",
    "platform",
    "order_type",
    "order_id",
    "order_time",
    "menu_name",
    "item_seq",
    "item_id",
    "item_name",
    "qty",
    "unit_price",
    "total_price",
    "discount_amount",
    "sale_type",
    "_pk",
    "collected_at",
    "order_cnt",
]


# 모듈 레벨 캐시 (DAG 실행 단위로 재사용)
_STORE_MAP_CACHE: dict | None = None
_STORE_MAP_CACHE_MTIME: float | None = None

_FIN_PRODUCT_CACHE: pd.DataFrame | None = None
_FIN_PRODUCT_CACHE_MTIME: float | None = None

_POSFEED_WHITELIST_CACHE: dict[str, set[str] | None] | None = None
_POSFEED_WHITELIST_CACHE_MTIME: float | None = None


def _load_fin_product_latest() -> pd.DataFrame:
    """fin_product_grp_input.csv 로드 후 source+brand+store+상품코드별 최신 1행으로 정규화.

    - updated_at 컬럼이 있으면(updated_at 파싱 가능한 경우) 최신 기준으로 dedupe
    - 없으면 파일 내 마지막 행(last)을 최신으로 가정
    """
    global _FIN_PRODUCT_CACHE, _FIN_PRODUCT_CACHE_MTIME
    try:
        source_path = existing_fin_product_csv_path()
        mtime = source_path.stat().st_mtime
    except FileNotFoundError:
        _FIN_PRODUCT_CACHE = pd.DataFrame(columns=["상품코드", "상품명", "updated_at"])
        _FIN_PRODUCT_CACHE_MTIME = None
        return _FIN_PRODUCT_CACHE

    if _FIN_PRODUCT_CACHE is not None and _FIN_PRODUCT_CACHE_MTIME == mtime:
        return _FIN_PRODUCT_CACHE

    try:
        df = pd.read_csv(source_path, dtype=str).fillna("")
    except Exception:
        df = pd.DataFrame(columns=["상품코드", "상품명", "updated_at"])

    for c in ("source", "brand", "store", "상품코드", "상품명"):
        if c not in df.columns:
            df[c] = ""
    # optional canonical name columns
    for c in ("표준_메뉴명", "상품명_표준", "메뉴명"):
        if c not in df.columns:
            df[c] = ""
    if "updated_at" not in df.columns:
        df["updated_at"] = ""

    df["source"] = df["source"].fillna("").astype(str).str.strip().map(canonical_source)
    df["brand"] = df["brand"].fillna("").astype(str).str.strip()
    df["store"] = df["store"].fillna("").astype(str).str.strip()
    df["상품코드"] = df["상품코드"].fillna("").astype(str).str.strip()
    df["상품명"] = df["상품명"].fillna("").astype(str).str.strip()
    df["표준_메뉴명"] = df["표준_메뉴명"].fillna("").astype(str).str.strip()
    df["상품명_표준"] = df["상품명_표준"].fillna("").astype(str).str.strip()
    df["메뉴명"] = df["메뉴명"].fillna("").astype(str).str.strip()
    df["updated_at"] = df["updated_at"].fillna("").astype(str).str.strip()

    df = df[df["상품코드"] != ""].copy()
    if df.empty:
        _FIN_PRODUCT_CACHE = df
        _FIN_PRODUCT_CACHE_MTIME = mtime
        return _FIN_PRODUCT_CACHE

    key_cols = ["source", "brand", "store", "상품코드"]
    if df["updated_at"].astype(str).str.strip().ne("").any():
        ts = pd.to_datetime(df["updated_at"], errors="coerce")
        df["_updated_at_ts"] = ts.fillna(pd.Timestamp.min)
        df = df.sort_values(key_cols + ["_updated_at_ts"], na_position="last").groupby(key_cols, as_index=False).last()
        df = df.drop(columns=["_updated_at_ts"], errors="ignore")
    else:
        df = df.sort_values(key_cols).groupby(key_cols, as_index=False).last()

    _FIN_PRODUCT_CACHE = df
    _FIN_PRODUCT_CACHE_MTIME = mtime
    return _FIN_PRODUCT_CACHE


def _fin_code_to_name_map() -> dict[tuple[str, str, str, str], str]:
    df = _load_fin_product_latest()
    if df.empty or "상품코드" not in df.columns or "상품명" not in df.columns:
        return {}
    # Prefer canonical name if present: 표준_메뉴명 -> 상품명_표준 -> 메뉴명 -> 상품명
    canon0 = df["표준_메뉴명"].fillna("").astype(str).str.strip()
    canon = df["상품명_표준"].fillna("").astype(str).str.strip()
    menu = df["메뉴명"].fillna("").astype(str).str.strip() if "메뉴명" in df.columns else pd.Series([""] * len(df))
    raw = df["상품명"].fillna("").astype(str).str.strip()
    name_s = canon0.where(canon0 != "", canon.where(canon != "", menu.where(menu != "", raw)))
    keys = list(zip(
        df["source"].fillna("").astype(str).str.strip().map(canonical_source),
        df["brand"].fillna("").astype(str).str.strip(),
        df["store"].fillna("").astype(str).str.strip(),
        df["상품코드"].fillna("").astype(str).str.strip(),
    ))
    return dict(zip(keys, name_s.fillna("").astype(str)))


def _apply_fin_item_name(df: pd.DataFrame) -> pd.DataFrame:
    """unified_sales df의 scoped item_id 기준으로 item_name을 fin_product_grp 최신 상품명으로 정합."""
    if df.empty or "item_id" not in df.columns:
        return df
    code_to_name = _fin_code_to_name_map()
    if not code_to_name:
        return df
    for col in ("source", "brand", "store"):
        if col not in df.columns:
            df[col] = ""
    keys = list(zip(
        df["source"].fillna("").astype(str).str.strip().map(canonical_source),
        df["brand"].fillna("").astype(str).str.strip(),
        df["store"].fillna("").astype(str).str.strip(),
        df["item_id"].fillna("").astype(str).str.strip(),
    ))
    mapped = pd.Series([code_to_name.get(key, "") for key in keys], index=df.index)
    mask = mapped.fillna("").astype(str).str.strip() != ""
    if "item_name" not in df.columns:
        df["item_name"] = ""
    df.loc[mask, "item_name"] = mapped.loc[mask].astype(str)
    return df


def _normalize_item_key(name: str) -> str:
    """item_name 비교용 정규화 키 생성 (저장값이 아닌 비교 전용).

    1. [...] 기호만 제거 (내용 보존)
    2. (...) 기호만 제거 (내용 보존)
    3. 한글·영문·숫자 이외 문자(공백 포함) 제거
    """
    s = re.sub(r'[\[\]]', '', name)
    s = re.sub(r'[()]', '', s)
    s = re.sub(r'[^가-힣a-zA-Z0-9一-鿿]', '', s)
    return s


def _merge_blacklist_scope(
    result: dict[str, set[str] | None],
    key: str,
    stores: set[str] | None,
) -> None:
    if not key:
        return
    if key not in result:
        result[key] = stores
        return
    if result[key] is None or stores is None:
        result[key] = None
        return
    result[key] = set(result[key] or set()) | stores


def _load_posfeed_blacklist() -> dict[str, set[str] | None]:
    """posfeed 제외 상품을 반환.

    반환값: {normalize(item_name): None | set[str]}
    - None → 전체 매장에서 제외
    - set[str] → 해당 매장에서만 제외 (store 컬럼의 쉼표 구분 값)

    기본 소스는 fin_product_grp_input.csv의 source=posfeed & exclude_check=Y이다.
    구 whitelist에는 옵션/배달비까지 N으로 남아 있어 기본 blacklist로 쓰지 않는다.
    긴급 진단 시에만 POSFEED_USE_LEGACY_BLACKLIST=1로 합산한다.
    """
    global _POSFEED_WHITELIST_CACHE, _POSFEED_WHITELIST_CACHE_MTIME
    try:
        source_path = existing_fin_product_csv_path()
        grp_mtime = source_path.stat().st_mtime
    except FileNotFoundError:
        source_path = FIN_PRODUCT_CSV_PATH
        grp_mtime = None
    try:
        legacy_mtime = POSFEED_WHITELIST_CSV_PATH.stat().st_mtime
    except FileNotFoundError:
        legacy_mtime = None

    mtime = (grp_mtime, legacy_mtime)

    if _POSFEED_WHITELIST_CACHE is not None and _POSFEED_WHITELIST_CACHE_MTIME == mtime:
        return _POSFEED_WHITELIST_CACHE

    result: dict[str, set[str] | None] = {}

    try:
        df = pd.read_csv(source_path, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as e:
        logger.warning("posfeed grp 블랙리스트 로드 실패: %s", e)
        df = pd.DataFrame()

    if not df.empty:
        if {"source", "상품명", "exclude_check"}.issubset(df.columns):
            source_mask = df["source"].fillna("").astype(str).str.strip().str.lower() == "posfeed"
            exclude_mask = df["exclude_check"].fillna("").astype(str).str.strip().str.upper() == "Y"
            for _, row in df[source_mask & exclude_mask].iterrows():
                key = _normalize_item_key(str(row["상품명"]).strip())
                _merge_blacklist_scope(result, key, None)
        else:
            logger.warning("posfeed grp 블랙리스트 컬럼 오류 (필요: source, 상품명, exclude_check)")

    if os.getenv("POSFEED_USE_LEGACY_BLACKLIST", "").strip().lower() in {"1", "true", "y", "yes"}:
        try:
            legacy = pd.read_csv(POSFEED_WHITELIST_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
        except FileNotFoundError:
            legacy = pd.DataFrame()
        except Exception as e:
            logger.warning("posfeed legacy whitelist 로드 실패: %s", e)
            legacy = pd.DataFrame()

        if not legacy.empty:
            if {"item_name", "is_valid"}.issubset(legacy.columns):
                invalid = legacy["is_valid"].fillna("").astype(str).str.strip().str.upper() == "N"
                for _, row in legacy[invalid].iterrows():
                    key = _normalize_item_key(str(row["item_name"]).strip())
                    store_val = str(row.get("store", "")).strip()
                    if store_val:
                        stores = {v.strip() for v in store_val.split(",") if v.strip()}
                        _merge_blacklist_scope(result, key, stores or None)
                    else:
                        _merge_blacklist_scope(result, key, None)
            else:
                logger.warning("posfeed legacy whitelist 컬럼 오류 (필요: item_name, is_valid)")

    logger.info("posfeed 블랙리스트 로드: %d 항목 (grp+legacy 정규화 키)", len(result))
    _POSFEED_WHITELIST_CACHE = result
    _POSFEED_WHITELIST_CACHE_MTIME = mtime
    return _POSFEED_WHITELIST_CACHE


def _apply_posfeed_blacklist(df: pd.DataFrame) -> pd.DataFrame:
    """posfeed df에서 grp 블랙리스트(exclude_check=Y) item_name 행 제거.

    - grp posfeed 제외 행은 전체 매장 제외로 적용
    - grp에 없는 item_name은 자동 통과
    """
    blacklist = _load_posfeed_blacklist()
    if not blacklist or df.empty:
        return df

    items = df["item_name"].fillna("").astype(str).str.strip()
    stores_col = df["store"].fillna("").astype(str).str.strip() if "store" in df.columns else pd.Series("", index=df.index)
    item_keys = items.map(_normalize_item_key)

    def _is_bl(key: str, store: str) -> bool:
        if key not in blacklist:
            return False
        r = blacklist[key]
        return r is None or store in r

    remove_mask = pd.Series(
        [_is_bl(k, s) for k, s in zip(item_keys, stores_col)],
        index=df.index,
        dtype=bool,
    )

    if not remove_mask.any():
        return df

    out = df.copy()
    if "order_id" in out.columns and "total_price" in out.columns:
        order_keys = out["order_id"].fillna("").astype(str).str.strip()
        amounts = pd.to_numeric(out["total_price"], errors="coerce").fillna(0)
        # 주문별로 제외 금액을 잔존행에 합산한다.
        # 잔존행이 있으면 첫 행에 합산, 주문 전체가 블랙리스트면 대표행(최고 매출) 1개를
        # 살려서 주문 매출을 보존한다(전량 제외 시 매출 누락 방지).
        for order_key, grp_idx in amounts.groupby(order_keys).groups.items():
            if not order_key:
                continue
            removed_idx = [i for i in grp_idx if remove_mask.at[i]]
            if not removed_idx:
                continue
            removed_sum = amounts.loc[removed_idx].sum()
            if removed_sum == 0:
                continue
            kept_idx = [i for i in grp_idx if not remove_mask.at[i]]
            if kept_idx:
                target_idx = kept_idx[0]
                out.at[target_idx, "total_price"] = int(round(amounts.at[target_idx] + removed_sum))
            else:
                rep_idx = amounts.loc[removed_idx].abs().idxmax()
                out.at[rep_idx, "total_price"] = int(round(removed_sum))
                remove_mask.at[rep_idx] = False

    logger.warning(
        "posfeed 블랙리스트 적용: %d행 제거 | 항목: %s",
        int(remove_mask.sum()),
        out.loc[remove_mask, "item_name"].unique().tolist()[:10],
    )
    return out[~remove_mask].copy()


# ============================================================
# 공통 내부 유틸
# ============================================================

def _load_store_map() -> dict[str, dict]:
    """sales_employee.csv → {지점명: {담당자, region, 실오픈일}} 맵 (캐시)."""
    global _STORE_MAP_CACHE, _STORE_MAP_CACHE_MTIME
    csv_path = ONEDRIVE_DB / "sales_employee.csv"
    try:
        mtime = csv_path.stat().st_mtime
    except FileNotFoundError:
        mtime = None

    # Airflow worker가 장시간 살아있으면 모듈 캐시가 다음 DAG 실행에도 남을 수 있어,
    # 파일 수정시간(mtime)이 동일할 때만 캐시를 재사용한다.
    if _STORE_MAP_CACHE is not None and _STORE_MAP_CACHE_MTIME is not None and mtime is not None:
        if _STORE_MAP_CACHE_MTIME == mtime:
            return _STORE_MAP_CACHE

    try:
        try:
            df = pd.read_csv(csv_path, dtype=str, usecols=["매장명", "담당자", "상세주소", "실오픈일"])
        except ValueError:
            df = pd.read_csv(csv_path, dtype=str, usecols=["매장명", "담당자", "상세주소"])
            df["실오픈일"] = ""
    except FileNotFoundError:
        logger.warning("sales_employee.csv 없음: %s", csv_path)
        _STORE_MAP_CACHE = {}
        _STORE_MAP_CACHE_MTIME = None
        return _STORE_MAP_CACHE

    df["_key"] = df["매장명"].str.strip().str.split().str[-1]
    df = df.drop_duplicates(subset=["_key"], keep="first")
    _STORE_MAP_CACHE = {
        row["_key"]: {
            "담당자": str(row.get("담당자", "")).strip(),
            "region": str(row.get("상세주소", "")).strip()[:2],
            "실오픈일": str(row.get("실오픈일", "")).strip(),
        }
        for _, row in df.iterrows()
    }
    _STORE_MAP_CACHE_MTIME = mtime
    return _STORE_MAP_CACHE


def _lookup_store_meta(store_map: dict[str, dict], key, field: str) -> str:
    if pd.isna(key):
        return ""
    return str(store_map.get(str(key), {}).get(field, "")).strip()


def _unified_daily_path(date_str: str):
    ymd = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")
    return UNIFIED_ROOT / f"unified_sales_{ymd}.parquet"


def _make_unified_pk(df: pd.DataFrame) -> pd.Series:
    """unified_sales 전용 PK 생성 (소스/스키마 공통).

    PK 구성:
    - sale_date | source | store | platform | order_id | item_seq

    채널별로 일부 값이 비어있을 수 있으나(예: toorder는 order_id/item_seq 없음),
    동일 채널 내에서 행 그레인이 유지되는 한 안정적으로 동작한다.
    """
    for col in ("sale_date", "source", "store", "platform", "order_id", "item_seq"):
        if col not in df.columns:
            df[col] = ""
    key = (
        df["sale_date"].fillna("").astype(str).str.strip()
        + "|" + df["source"].fillna("").astype(str).str.strip()
        + "|" + df["store"].fillna("").astype(str).str.strip()
        + "|" + df["platform"].fillna("").astype(str).str.strip()
        + "|" + df["order_id"].fillna("").astype(str).str.strip()
        + "|" + df["item_seq"].fillna("").astype(str).str.strip()
    )
    return key.map(lambda s: hashlib.md5(s.encode()).hexdigest())


def _save_unified_daily(
    df: pd.DataFrame,
    date_str: str,
    overwrite: bool = False,
    replace_stores: list[str] | None = None,
) -> int:
    """일별 unified_sales 저장.

    overwrite=False(기본): 동일 source 행을 교체(source-aware replace). 다른 source 행은 유지.
    overwrite=True: 기존 파일 전체 교체 (정정용).
    replace_stores: 지정된 매장의 동일 source 행만 교체. 금일 부분 재적재용.
    """
    UNIFIED_ROOT.mkdir(parents=True, exist_ok=True)
    daily_path = _unified_daily_path(date_str)

    # Backward compatibility: older parquet / upstream may still have "구분".
    if "sale_type" not in df.columns and "구분" in df.columns:
        df = df.copy()
        df["sale_type"] = df["구분"]

    if "order_cnt" not in df.columns:
        df = df.copy()
        df["order_cnt"] = 0

    # Normalize source so source-aware replace is stable across re-runs.
    # (Avoids accidental duplication when historical rows had different casing/whitespace.)
    if "source" in df.columns:
        df = df.copy()
        df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()

    df = df.reindex(columns=UNIFIED_COLUMNS, fill_value="")
    df = filter_manual_delivery_sources_for_test_stores(df)

    if daily_path.exists():
        existing = pd.read_parquet(daily_path)
        existing = existing.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        if "source" in existing.columns:
            existing = existing.copy()
            existing["source"] = existing["source"].fillna("").astype(str).str.strip().str.lower()
        sources = df["source"].dropna().unique().tolist()
        store_scope = {
            str(store).strip()
            for store in (replace_stores or [])
            if str(store).strip()
        }
        if sources and store_scope:
            existing_store = existing["store"].fillna("").astype(str).str.strip()
            replace_mask = existing["source"].isin(sources) & existing_store.isin(store_scope)
            existing_same_src = existing[replace_mask]
            existing_other_src = existing[~replace_mask]
        else:
            existing_same_src = existing[existing["source"].isin(sources)] if sources else existing.iloc[:0]
            existing_other_src = existing[~existing["source"].isin(sources)] if sources else existing
        # no-op 체크: overwrite=False일 때만 적용 (overwrite=True면 강제 재기록)
        if not overwrite and (
            set(existing_same_src["_pk"]) == set(df["_pk"])
            and not existing_same_src["_pk"].duplicated().any()
            and len(existing_same_src) == len(df)
        ):
            logger.info("변경 없음, 스킵: %s", daily_path)
            return 0
        merged = pd.concat([existing_other_src, df], ignore_index=True)
        # overwrite=True 로 기존 same-source rows 를 교체할 때 기존 행 수가 더 많으면
        # 단순 차분은 음수가 될 수 있다. 반환값은 "이번 저장으로 반영된 행 수"로만
        # 사용하므로 음수는 0으로 클램프한다.
        new_count = max(0, len(df) - len(existing_same_src))
    else:
        merged = df.copy()
        new_count = len(merged)

    merged = filter_manual_delivery_sources_for_test_stores(merged)

    # Final safety: keep latest row per PK (idempotency across re-runs / partial loads).
    if "_pk" in merged.columns:
        before = len(merged)
        merged = merged.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)
        dropped = before - len(merged)
        if dropped:
            logger.warning("unified_sales %s: _pk 중복 %d행 제거(방어)", date_str, dropped)

    merged["qty"] = pd.to_numeric(merged["qty"], errors="coerce").fillna(0).astype(int)
    merged["unit_price"] = pd.to_numeric(merged["unit_price"], errors="coerce").fillna(0).astype(int)
    if "total_price" in merged.columns:
        merged["total_price"] = pd.to_numeric(merged["total_price"], errors="coerce").fillna(0).astype(int)
    if "discount_amount" in merged.columns:
        merged["discount_amount"] = pd.to_numeric(merged["discount_amount"], errors="coerce").fillna(0).astype(int)
    merged.to_parquet(daily_path, index=False, engine="pyarrow")
    logger.info("저장(일별): %s | 전체 %d행 (신규 %d행)", daily_path, len(merged), new_count)
    return new_count


# ============================================================
# 공통 시각 정규화
# ============================================================

def _normalize_time(raw) -> str:
    s = str(raw).strip()
    if not s or s == "nan":
        return ""

    # Prefer explicit HH:MM:SS anywhere in the string (e.g. "2026-04-01 12:05:00")
    m = re.findall(r"\b(\d{2}:\d{2}:\d{2})\b", s)
    if m:
        return m[-1]

    # Fallback: HH:MM -> HH:MM:00 (e.g. "2026-04-01 12:05")
    m2 = re.findall(r"\b(\d{2}:\d{2})\b", s)
    if m2:
        return f"{m2[-1]}:00"

    # Last resort: keep as-is
    return s


def _strip_and_coalesce_columns(df: pd.DataFrame) -> pd.DataFrame:
    """CSV/HTML 소스에서 컬럼명 앞뒤 공백이 섞이는 케이스 방어 + 중복 컬럼 병합.

    월 단위로 여러 매장을 concat 할 때, 매장별로 컬럼명이 미세하게 다르면(공백 포함)
    동일한 의미의 컬럼이 2개로 늘어나며 한쪽이 전부 NaN이 되는 케이스가 생긴다.

    예) 어떤 매장은 '실매출액', 다른 매장은 ' 실매출액 ' → concat 후 2개 컬럼 공존
        → strip 후 둘 다 '실매출액'이 되므로, "첫 non-null" 기준으로 한 컬럼으로 병합한다.
    """
    if df is None or df.empty:
        return df

    bases: dict[str, list[str]] = {}
    order: list[str] = []
    for c in df.columns:
        base = str(c).strip()
        if base not in bases:
            bases[base] = []
            order.append(base)
        bases[base].append(c)

    if all(len(cols) == 1 for cols in bases.values()):
        # strip만 적용
        out = df.copy()
        out.columns = order
        return out

    out = pd.DataFrame(index=df.index)
    for base in order:
        cols = bases[base]
        if len(cols) == 1:
            out[base] = df[cols[0]]
            continue
        s = df[cols[0]]
        for c in cols[1:]:
            s = s.combine_first(df[c])
        out[base] = s
    return out


def _to_int_series(s: pd.Series) -> pd.Series:
    """금액/수량 컬럼을 안전하게 int로 변환 (콤마/공백 포함 대응)."""
    if s is None:
        return pd.Series(0)
    v = s.astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(v, errors="coerce").fillna(0).astype(int)


def _normalize_id_col(s: pd.Series) -> pd.Series:
    """포스번호/영수번호 leading-zero 정규화: '0001'→'1', '01'→'1'.

    OKPOS CSV 수집 시 order와 item 파일 간에 동일 번호가 '1' vs '01' 형식으로
    불일치하는 경우가 있어 join 키를 정수 문자열로 통일한다.
    비숫자 값(알파벳 포함 등)은 원본 그대로 보존한다.
    """
    stripped = s.astype(str).str.strip()
    numeric = pd.to_numeric(stripped, errors="coerce")
    result = stripped.copy()
    mask = numeric.notna()
    result[mask] = numeric[mask].astype(int).astype(str)
    return result


# ============================================================
# Admin / Repair API
# ============================================================

def resave_existing_unified_sales() -> str:
    """저장된 모든 unified_sales parquet을 sale_date 기준으로 재편성 저장.

    - 모든 파일을 한 번에 읽어 concat 후 _pk 기준 dedup
    - sale_date별 groupby → 날짜당 1회 overwrite 저장
    - sale_date 없거나 비어있는 행은 제외
    """
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    parts: list[pd.DataFrame] = []
    skipped = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as e:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, e)
            skipped += 1
            continue

        if "source" in df.columns:
            df = df.copy()
            df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()

        parts.append(df)

    if not parts:
        raise RuntimeError("읽을 수 있는 unified_sales parquet 없음")

    all_df = pd.concat(parts, ignore_index=True)
    before_dedup = len(all_df)
    if "_pk" in all_df.columns:
        all_df = all_df.drop_duplicates(subset=["_pk"], keep="last")
    logger.info("전체 %d행 로드 (dedup 후 %d행)", before_dedup, len(all_df))

    if "sale_date" not in all_df.columns:
        raise RuntimeError("sale_date 컬럼이 없어 재편성 불가")

    all_df["sale_date"] = all_df["sale_date"].fillna("").astype(str).str.strip()
    targets = sorted({d for d in all_df["sale_date"].unique().tolist() if d and d.lower() != "nan"})
    if not targets:
        return "SKIP: 유효한 sale_date 없음"

    total_targets = 0
    total_rows = 0
    for sale_date in targets:
        grp = all_df[all_df["sale_date"] == sale_date]
        saved = _save_unified_daily(grp, sale_date, overwrite=True)
        total_targets += 1
        total_rows += saved
        logger.info("재저장: %s | %d행", sale_date, saved)

    result = (
        f"unified_sales 재저장 완료 | 소스파일 {len(parts)}개 (스킵 {skipped}개) | "
        f"날짜 {total_targets}개 | 신규행 {total_rows}행"
    )
    logger.info(result)
    return result


def repartition_unified_sales_by_sale_date() -> str:
    """Repartition existing unified_sales parquet by `sale_date` (YYYY-MM-DD).

    Some repair flows may leave files that are grouped by collected_at, which can contain multiple sale_date
    values. If you later read all files via glob and group by sale_date, those rows get double-counted.

    This function loads every unified_sales_*.parquet, globally deduplicates by `_pk`, then overwrites
    per-sale_date files so filename date matches the sale_date.
    """
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    parts: list[pd.DataFrame] = []
    skipped = 0
    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, exc)
            skipped += 1
            continue
        if "source" in df.columns:
            df = df.copy()
            df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()
        parts.append(df)

    if not parts:
        raise RuntimeError("읽을 수 있는 unified_sales parquet 없음")

    all_df = pd.concat(parts, ignore_index=True)
    if "_pk" in all_df.columns:
        before = len(all_df)
        all_df = all_df.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)
        logger.info("global dedup by _pk: %d -> %d", before, len(all_df))

    if "sale_date" not in all_df.columns:
        raise RuntimeError("sale_date 컬럼이 없어 repartition 불가")

    all_df["sale_date"] = all_df["sale_date"].fillna("").astype(str).str.strip()
    targets = sorted({d for d in all_df["sale_date"].unique().tolist() if d and d.lower() != "nan"})
    if not targets:
        return "SKIP: 유효한 sale_date 없음"

    total_saved = 0
    for d in targets:
        grp = all_df[all_df["sale_date"] == d]
        saved = _save_unified_daily(grp, d, overwrite=True)
        total_saved += saved

    result = f"OK: repartition by sale_date | files={len(parts)} skip={skipped} days={len(targets)} saved={total_saved}"
    logger.info(result)
    return result


def filter_manual_delivery_sources_for_test_stores(
    df: pd.DataFrame,
    stores: list[str] | None = None,
) -> pd.DataFrame:
    """테스트 매장 배달행을 platform 기준으로 정리한다.

    - 과거일(어제 이하): 같은 매장/일자/플랫폼 패밀리에 수동(배민수동/쿠팡수동)
      행이 실제로 있을 때만 비수동(POS/posfeed/okpos) 행을 제거한다.
      수동 수집이 비어 있으면 POS 계열 행을 fallback으로 유지해 매출 누락을 막는다.
    - 오늘: 테스트 매장 배달은 자동(POS/posfeed)만 사용 (수동 행 제거)
    - sale_date가 비어있는 행은 today/past 어느 쪽으로도 판정하지 않고 그대로 둔다.
    """
    if df.empty or not {"store", "platform", "source"}.issubset(df.columns):
        return df

    store_set = {
        str(store).strip()
        for store in (stores or DELIVERY_MANUAL_TEST_STORES)
        if str(store).strip()
    }
    if not store_set:
        return df

    out = df.copy()
    store = out["store"].fillna("").astype(str).str.strip()
    platform = out["platform"].fillna("").astype(str).str.strip()
    source = out["source"].fillna("").astype(str).str.strip()
    date = (
        out["sale_date"].fillna("").astype(str).str.strip()
        if "sale_date" in out.columns
        else pd.Series("", index=out.index)
    )
    today = _kst_today_str()
    has_date = date.ne("")

    remove_mask = pd.Series(False, index=out.index)
    for manual_src, family in DELIVERY_PLATFORM_FAMILIES.items():
        in_family = store.isin(store_set) & platform.isin(family)
        if not in_family.any():
            continue

        is_manual = source.eq(manual_src)
        is_today = has_date & date.eq(today)
        is_past = has_date & date.ne(today)

        # 오늘: 테스트 매장도 자동수집(POS/posfeed)만 사용 → 수동 행 제거.
        remove_mask |= in_family & is_today & is_manual

        # 과거: 같은 매장/일자/플랫폼군에 수동 행이 실제로 있을 때만 비수동 행 제거.
        manual_keys = set(
            zip(
                store[in_family & is_past & is_manual],
                date[in_family & is_past & is_manual],
            )
        )
        if manual_keys:
            row_keys = pd.Series(list(zip(store, date)), index=out.index)
            has_manual_family = row_keys.isin(manual_keys)
            remove_mask |= in_family & is_past & ~is_manual & has_manual_family

    removed = int(remove_mask.sum())
    if removed:
        logger.warning("테스트 매장 수동 우선 배달 정리: %d행 제거", removed)
    return out[~remove_mask].reset_index(drop=True)


def filter_manual_delivery_sources_for_non_test_stores(
    df: pd.DataFrame,
    stores: list[str] | None = None,
) -> pd.DataFrame:
    """비테스트 매장에 남은 배민수동/쿠팡수동 행을 제거한다."""
    if df.empty or not {"store", "source"}.issubset(df.columns):
        return df

    store_set = {
        str(store).strip()
        for store in (stores or DELIVERY_MANUAL_TEST_STORES)
        if str(store).strip()
    }
    manual_sources = set(DELIVERY_PLATFORM_FAMILIES)

    out = df.copy()
    store = out["store"].fillna("").astype(str).str.strip()
    source = out["source"].fillna("").astype(str).str.strip()
    remove_mask = source.isin(manual_sources) & ~store.isin(store_set)

    removed = int(remove_mask.sum())
    if removed:
        logger.warning("비테스트 매장 수동 배달 source 정리: %d행 제거", removed)
    return out[~remove_mask].reset_index(drop=True)


def enforce_manual_delivery_sources_for_test_stores(
    stores: list[str] | None = None,
) -> str:
    """기존 unified_sales parquet에서 테스트 매장 배달 플랫폼 중복 source를 제거."""
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    changed_files = 0
    total_removed = 0
    skipped = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("수동 source 강제 parquet 로드 실패, 스킵: %s | %s", path, exc)
            skipped += 1
            continue

        before = len(df)
        df_out = filter_manual_delivery_sources_for_test_stores(df, stores=stores)
        removed = before - len(df_out)
        if removed <= 0:
            continue

        df_out = df_out.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        for col in ("qty", "unit_price", "total_price", "discount_amount", "order_cnt"):
            if col in df_out.columns:
                df_out[col] = pd.to_numeric(df_out[col], errors="coerce").fillna(0).astype(int)
        df_out.to_parquet(path, index=False, engine="pyarrow")
        changed_files += 1
        total_removed += removed
        logger.warning("테스트 매장 배달 수동 source 강제: %s | 제거=%d", path.name, removed)

    result = (
        f"테스트 매장 배달 수동 source 강제 완료 | 파일={changed_files} "
        f"제거={total_removed} 스킵={skipped}"
    )
    logger.info(result)
    return result


def purge_manual_delivery_sources_for_non_test_stores(
    stores: list[str] | None = None,
) -> str:
    """기존 unified_sales parquet에서 비테스트 매장의 수동 배달 source를 제거."""
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    changed_files = 0
    total_removed = 0
    skipped = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("비테스트 수동 source parquet 로드 실패, 스킵: %s | %s", path, exc)
            skipped += 1
            continue

        before = len(df)
        df_out = filter_manual_delivery_sources_for_non_test_stores(df, stores=stores)
        removed = before - len(df_out)
        if removed <= 0:
            continue

        df_out = df_out.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        for col in ("qty", "unit_price", "total_price", "discount_amount", "order_cnt"):
            if col in df_out.columns:
                df_out[col] = pd.to_numeric(df_out[col], errors="coerce").fillna(0).astype(int)
        df_out.to_parquet(path, index=False, engine="pyarrow")
        changed_files += 1
        total_removed += removed
        logger.warning("비테스트 매장 수동 배달 source 정리: %s | 제거=%d", path.name, removed)

    result = (
        f"비테스트 매장 수동 배달 source 정리 완료 | 파일={changed_files} "
        f"제거={total_removed} 스킵={skipped}"
    )
    logger.info(result)
    return result


def refresh_store_meta_in_unified_sales() -> str:
    """현재 sales_employee.csv 기준으로 unified_sales 매장 메타를 일괄 갱신."""
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    store_map = _load_store_map()
    total_rows = 0
    changed_files = 0
    skipped = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("매장 메타 갱신 parquet 로드 실패, 스킵: %s | %s", path, exc)
            skipped += 1
            continue

        if df.empty or "store" not in df.columns:
            continue

        df = df.copy()
        before = df.reindex(columns=["담당자", "region", "실오픈일"], fill_value="")
        store_key = df["store"].fillna("").astype(str).str.strip().str.split().str[-1]
        known_mask = store_key.isin(store_map)
        for field in ("담당자", "region", "실오픈일"):
            if field not in df.columns:
                df[field] = ""
            df.loc[known_mask, field] = store_key.loc[known_mask].map(
                lambda key: _lookup_store_meta(store_map, key, field)
            )

        after = df[["담당자", "region", "실오픈일"]]
        if before.equals(after):
            continue

        df = df.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        df.to_parquet(path, index=False, engine="pyarrow")
        changed_files += 1
        total_rows += len(df)

    result = (
        f"unified_sales 매장 메타 갱신 완료 | 파일={changed_files} "
        f"행={total_rows} 스킵={skipped}"
    )
    logger.info(result)
    return result


def purge_source_from_unified_sales(source: str = "toorder") -> str:
    """Remove all rows of target source from all unified_sales parquet files."""
    src = str(source).strip().lower()
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    total_removed = 0
    changed_files = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, exc)
            continue
        if "source" not in df.columns:
            continue

        mask = df["source"].fillna("").astype(str).str.strip().str.lower() == src
        removed = int(mask.sum())
        if removed == 0:
            continue

        try:
            df = df[~mask].reset_index(drop=True)
            df.to_parquet(path, index=False, engine="pyarrow")
        except Exception as exc:
            logger.warning("unified_sales parquet 저장 실패, 스킵: %s | %s", path, exc)
            continue

        total_removed += removed
        changed_files += 1
        logger.info("%s 행 제거: %s (%d행)", src, path.name, removed)

    result = f"OK: purge source={src} | files={changed_files} removed={total_removed}"
    logger.info(result)
    return result


def reclassify_hall_platform(date_str: str, overwrite: bool = True) -> str:
    """unified_sales parquet의 포스/제휴사주문 행을 테이블명 기반으로 재분류."""
    path = _unified_daily_path(date_str)
    if not path.exists():
        return f"파일 없음: {path}"

    df = pd.read_parquet(path)
    if "테이블명" not in df.columns:
        return f"{date_str}: 테이블명 컬럼 없음, 스킵"

    target = df["platform"].isin(["포스", "제휴사주문"])
    table_v = df.loc[target, "테이블명"].fillna("").astype(str).str.strip()
    is_pack = table_v.str.contains("포장", regex=False).reindex(df.index, fill_value=False)
    is_num = table_v.str.fullmatch(r"\d+").fillna(False).reindex(df.index, fill_value=False)

    df.loc[target & is_pack, "order_type"] = "홀_포장"
    df.loc[target & is_pack, "platform"] = "홀"
    df.loc[target & is_num, "order_type"] = "홀_테이블"
    df.loc[target & is_num, "platform"] = "홀"

    saved = _save_unified_daily(df, date_str, overwrite=overwrite)
    result = f"{date_str}: {saved}행 재분류 저장"
    logger.info(result)
    return result
