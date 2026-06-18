"""
unified_sales 채널별 파이프라인 공통 모듈.

- 스키마/저장 로직
- store 메타 로드(담당자/region/실오픈일)
- 공통 정규화 유틸
- 기존 parquet 재저장/재분류 유틸
"""

import logging
import hashlib
import re
from datetime import datetime

import pandas as pd

from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, MART_DB, ONEDRIVE_DB, POSFEED_WHITELIST_CSV_PATH

logger = logging.getLogger(__name__)

UNIFIED_ROOT = MART_DB / "unified_sales_grp"

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
    """fin_product_grp.csv 로드 후 상품코드별 최신 1행으로 정규화.

    - updated_at 컬럼이 있으면(updated_at 파싱 가능한 경우) 최신 기준으로 dedupe
    - 없으면 파일 내 마지막 행(last)을 최신으로 가정
    """
    global _FIN_PRODUCT_CACHE, _FIN_PRODUCT_CACHE_MTIME
    try:
        mtime = FIN_PRODUCT_CSV_PATH.stat().st_mtime
    except FileNotFoundError:
        _FIN_PRODUCT_CACHE = pd.DataFrame(columns=["상품코드", "상품명", "updated_at"])
        _FIN_PRODUCT_CACHE_MTIME = None
        return _FIN_PRODUCT_CACHE

    if _FIN_PRODUCT_CACHE is not None and _FIN_PRODUCT_CACHE_MTIME == mtime:
        return _FIN_PRODUCT_CACHE

    try:
        df = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str).fillna("")
    except Exception:
        df = pd.DataFrame(columns=["상품코드", "상품명", "updated_at"])

    for c in ("상품코드", "상품명"):
        if c not in df.columns:
            df[c] = ""
    # optional canonical name columns
    for c in ("표준_메뉴명", "상품명_표준", "메뉴명"):
        if c not in df.columns:
            df[c] = ""
    if "updated_at" not in df.columns:
        df["updated_at"] = ""

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

    if df["updated_at"].astype(str).str.strip().ne("").any():
        ts = pd.to_datetime(df["updated_at"], errors="coerce")
        df["_updated_at_ts"] = ts.fillna(pd.Timestamp.min)
        df = df.sort_values(["상품코드", "_updated_at_ts"], na_position="last").groupby("상품코드", as_index=False).last()
        df = df.drop(columns=["_updated_at_ts"], errors="ignore")
    else:
        df = df.sort_values(["상품코드"]).groupby("상품코드", as_index=False).last()

    _FIN_PRODUCT_CACHE = df
    _FIN_PRODUCT_CACHE_MTIME = mtime
    return _FIN_PRODUCT_CACHE


def _fin_code_to_name_map() -> dict[str, str]:
    df = _load_fin_product_latest()
    if df.empty or "상품코드" not in df.columns or "상품명" not in df.columns:
        return {}
    # Prefer canonical name if present: 표준_메뉴명 -> 상품명_표준 -> 메뉴명 -> 상품명
    canon0 = df["표준_메뉴명"].fillna("").astype(str).str.strip()
    canon = df["상품명_표준"].fillna("").astype(str).str.strip()
    menu = df["메뉴명"].fillna("").astype(str).str.strip() if "메뉴명" in df.columns else pd.Series([""] * len(df))
    raw = df["상품명"].fillna("").astype(str).str.strip()
    name_s = canon0.where(canon0 != "", canon.where(canon != "", menu.where(menu != "", raw)))
    return pd.Series(name_s.values, index=df["상품코드"].astype(str)).fillna("").astype(str).to_dict()


def _apply_fin_item_name(df: pd.DataFrame) -> pd.DataFrame:
    """unified_sales df의 item_id 기준으로 item_name을 fin_product_grp 최신 상품명으로 '정합'."""
    if df.empty or "item_id" not in df.columns:
        return df
    code_to_name = _fin_code_to_name_map()
    if not code_to_name:
        return df
    mapped = df["item_id"].fillna("").astype(str).str.strip().map(lambda c: code_to_name.get(c, ""))
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


def _load_posfeed_blacklist() -> dict[str, set[str] | None]:
    """fin_product_posfeed_whitelist.csv에서 is_valid=N 항목을 store-aware dict로 반환.

    반환값: {normalize(item_name): None | set[str]}
    - None → 전체 매장에서 제외
    - set[str] → 해당 매장에서만 제외 (store 컬럼의 쉼표 구분 값)

    - 필수 컬럼: item_name, is_valid
    - store 컬럼 없거나 값이 비면 전체 매장 제외
    - 파일 없거나 N 항목 없으면 빈 dict 반환 → 필터링 비활성.
    """
    global _POSFEED_WHITELIST_CACHE, _POSFEED_WHITELIST_CACHE_MTIME
    try:
        mtime = POSFEED_WHITELIST_CSV_PATH.stat().st_mtime
    except FileNotFoundError:
        _POSFEED_WHITELIST_CACHE = {}
        _POSFEED_WHITELIST_CACHE_MTIME = None
        return _POSFEED_WHITELIST_CACHE

    if _POSFEED_WHITELIST_CACHE is not None and _POSFEED_WHITELIST_CACHE_MTIME == mtime:
        return _POSFEED_WHITELIST_CACHE

    try:
        df = pd.read_csv(POSFEED_WHITELIST_CSV_PATH, dtype=str).fillna("")
    except Exception as e:
        logger.warning("posfeed whitelist 로드 실패: %s", e)
        _POSFEED_WHITELIST_CACHE = {}
        _POSFEED_WHITELIST_CACHE_MTIME = mtime
        return _POSFEED_WHITELIST_CACHE

    if not {"item_name", "is_valid"}.issubset(df.columns):
        logger.warning("posfeed whitelist 컬럼 오류 (필요: item_name, is_valid)")
        _POSFEED_WHITELIST_CACHE = {}
        _POSFEED_WHITELIST_CACHE_MTIME = mtime
        return _POSFEED_WHITELIST_CACHE

    result: dict[str, set[str] | None] = {}
    for _, row in df[df["is_valid"].str.strip().str.upper() == "N"].iterrows():
        key = _normalize_item_key(str(row["item_name"]).strip())
        if not key:
            continue
        store_val = str(row["store"]).strip() if "store" in df.columns else ""
        if not store_val or store_val == "nan":
            result[key] = None  # 전체 매장
        else:
            stores = {s.strip() for s in store_val.split(",") if s.strip()}
            if key in result:
                if result[key] is not None:
                    result[key].update(stores)
                # already None (global) → stays global
            else:
                result[key] = stores

    logger.info("posfeed 블랙리스트 로드: %d 항목 (정규화 키)", len(result))
    _POSFEED_WHITELIST_CACHE = result
    _POSFEED_WHITELIST_CACHE_MTIME = mtime
    return _POSFEED_WHITELIST_CACHE


def _apply_posfeed_blacklist(df: pd.DataFrame) -> pd.DataFrame:
    """posfeed df에서 블랙리스트(is_valid=N) item_name 행 제거.

    - store 컬럼 비어있으면 전체 매장, 값 있으면 해당 매장에서만 제거
    - CSV에 없는 item_name은 자동 통과
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

    logger.warning(
        "posfeed 블랙리스트 적용: %d행 제거 | 항목: %s",
        int(remove_mask.sum()),
        df.loc[remove_mask, "item_name"].unique().tolist()[:10],
    )
    return df[~remove_mask].copy()


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


def _save_unified_daily(df: pd.DataFrame, date_str: str, overwrite: bool = False) -> int:
    """일별 unified_sales 저장.

    overwrite=False(기본): 동일 source 행을 교체(source-aware replace). 다른 source 행은 유지.
    overwrite=True: 기존 파일 전체 교체 (정정용).
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

    if daily_path.exists():
        existing = pd.read_parquet(daily_path)
        existing = existing.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        if "source" in existing.columns:
            existing = existing.copy()
            existing["source"] = existing["source"].fillna("").astype(str).str.strip().str.lower()
        sources = df["source"].dropna().unique().tolist()
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
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")) if UNIFIED_ROOT.exists() else []
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
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")) if UNIFIED_ROOT.exists() else []
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


def purge_source_from_unified_sales(source: str = "toorder") -> str:
    """Remove all rows of target source from all unified_sales parquet files."""
    src = str(source).strip().lower()
    files = sorted(UNIFIED_ROOT.glob("unified_sales_*.parquet")) if UNIFIED_ROOT.exists() else []
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
