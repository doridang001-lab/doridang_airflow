"""
unified_sales - UnionPOS 채널 전용 모듈.

입력:
- ANALYTICS_DB/unionpos_sales_raw/brand=도리당/store=*/ym=YYYY-MM/
  - unionpos_receipt_list.csv  : 영수증 헤더
  - unionpos_receipt_items.csv : 영수증 상세품목

출력:
- MART_DB/unified_sales_grp/unified_sales_YYMMDD.parquet (공통 _save_unified_daily 사용)

추가:
- fin_product_grp.csv(ANALYTICS_DB/fin_product/fin_product_grp.csv)를 사용해 menu_name(대표메뉴) 산정
"""

import hashlib
import logging
import os
import re
from datetime import datetime
from functools import lru_cache

import pandas as pd

from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, RAW_UNIONPOS_SALES
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_COLUMNS,
    _apply_fin_item_name,
    _make_unified_pk,
    _load_store_map,
    _lookup_store_meta,
    _normalize_id_col,
    _normalize_time,
    _save_unified_daily,
    _strip_and_coalesce_columns,
    _to_int_series,
)
from modules.transform.pipelines.db.DB_FinProduct import _mark_is_latest

logger = logging.getLogger(__name__)

UNIONPOS_BRAND_ROOT = RAW_UNIONPOS_SALES / "brand=도리당"
UNIONPOS_SOURCE = "unionpos"
UNIONPOS_BRAND = "도리당"

# 월 단위 캐시 (DAG 실행 단위로 재사용)
_UNIONPOS_MONTH_CACHE: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}


@lru_cache(maxsize=1)
def _load_fin_product() -> pd.DataFrame:
    """fin_product_grp.csv 로드 (menu_name / 메인 경계 판단용)."""
    try:
        df = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str).fillna("")
    except Exception as e:
        logger.warning("fin_product_grp 로드 실패, fallback 사용: %s", e)
        return pd.DataFrame(columns=["상품코드", "상품명", "is_main_candidate", "exclude_check"])

    for col in ("상품코드", "상품명", "is_main_candidate", "exclude_check"):
        if col not in df.columns:
            df[col] = ""

    df["상품코드"] = df["상품코드"].fillna("").astype(str).str.strip()
    df["is_main_candidate"] = df["is_main_candidate"].fillna("N").astype(str).str.strip().str.upper().replace({"": "N"})
    df["exclude_check"] = df["exclude_check"].fillna("N").astype(str).str.strip().str.upper().replace({"": "N"})

    # 코드별 최신 1행만 사용 (상품명 변경 등 히스토리 append 구조 대응)
    if "updated_at" in df.columns:
        ts = pd.to_datetime(df["updated_at"], errors="coerce")
        df["_updated_at_ts"] = ts.fillna(pd.Timestamp.min)
        df = df.sort_values(["상품코드", "_updated_at_ts"], na_position="last").groupby("상품코드", as_index=False).last()
        df = df.drop(columns=["_updated_at_ts"], errors="ignore")
    else:
        df = df.sort_values(["상품코드"]).groupby("상품코드", as_index=False).last()

    return df


def _parse_store_from_path(path) -> str:
    """.../store=동탄영천점/ym=YYYY-MM/file.csv → 동탄영천점"""
    s = str(path)
    # windows/linux 공통 대응
    parts = s.replace("\\", "/").split("/")
    store_part = next((p for p in parts if p.startswith("store=")), "")
    return store_part.split("=", 1)[1] if "=" in store_part else ""


def _load_unionpos_month(ym: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    global _UNIONPOS_MONTH_CACHE
    if ym in _UNIONPOS_MONTH_CACHE:
        return _UNIONPOS_MONTH_CACHE[ym]

    list_dfs: list[pd.DataFrame] = []
    item_dfs: list[pd.DataFrame] = []

    for path in sorted(UNIONPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/unionpos_receipt_list.csv")):
        store = _parse_store_from_path(path)
        try:
            df = pd.read_csv(path, dtype=str)
            df["_store_partition"] = store
            list_dfs.append(df)
        except Exception as e:
            logger.warning("unionpos_receipt_list 로드 실패: %s | %s", path, e)

    for path in sorted(UNIONPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/unionpos_receipt_items.csv")):
        store = _parse_store_from_path(path)
        try:
            df = pd.read_csv(path, dtype=str)
            df["_store_partition"] = store
            # 파일 내 원본 행순서를 item_seq tie-breaker로 사용
            df = df.reset_index(drop=True)
            df["_row_in_file"] = pd.RangeIndex(len(df)).astype(int)
            item_dfs.append(df)
        except Exception as e:
            logger.warning("unionpos_receipt_items 로드 실패: %s | %s", path, e)

    list_df = pd.concat(list_dfs, ignore_index=True) if list_dfs else pd.DataFrame()
    item_df = pd.concat(item_dfs, ignore_index=True) if item_dfs else pd.DataFrame()

    logger.info("UnionPOS 월 로드: list %d행, items %d행 | ym=%s", len(list_df), len(item_df), ym)
    _UNIONPOS_MONTH_CACHE[ym] = (list_df, item_df)
    return list_df, item_df


def _load_unionpos_by_date(date_str: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    ym = date_str[:7]
    list_df, item_df = _load_unionpos_month(ym)
    if list_df.empty:
        raise FileNotFoundError(f"unionpos_receipt_list CSV 없음 | ym={ym}")
    if item_df.empty:
        raise FileNotFoundError(f"unionpos_receipt_items CSV 없음 | ym={ym}")

    list_f = list_df[list_df["sale_date"].astype(str).str.strip() == date_str].reset_index(drop=True)
    item_f = item_df[item_df["sale_date"].astype(str).str.strip() == date_str].reset_index(drop=True)

    if list_f.empty:
        raise FileNotFoundError(f"unionpos_receipt_list 데이터 없음 | {date_str}")
    if item_f.empty:
        raise FileNotFoundError(f"unionpos_receipt_items 데이터 없음 | {date_str}")
    return list_f, item_f


def _normalize_unionpos_item_code(s: pd.Series) -> pd.Series:
    """상품코드 정규화: '00196'→'196', 숫자 아닌 값은 그대로."""
    stripped = s.fillna("").astype(str).str.strip()
    numeric = pd.to_numeric(stripped, errors="coerce")
    out = stripped.copy()
    mask = numeric.notna()
    out[mask] = numeric[mask].astype(int).astype(str)
    return out


_UNIONPOS_CHILD_PREFIX_RE = re.compile(r"^\s*┗\s*")
_UNIONPOS_PAREN_ONLY_RE = re.compile(r"^\(\s*[^()]+\s*\)$")


def _normalize_unionpos_item_name(raw: str) -> str:
    """UnionPOS 옵션/하위행 표기 정규화.

    예)
    - '┗ 낙지200g' -> '낙지200g'
    - '┗ (소)' -> ''  (의미 없는 괄호 옵션 행 제거)
    """
    s = "" if raw is None else str(raw)
    s = s.strip()
    if not s or s.lower() == "nan":
        return ""

    s = _UNIONPOS_CHILD_PREFIX_RE.sub("", s).strip()

    # '(소)' 같은 괄호 단독 옵션명은 제거
    if _UNIONPOS_PAREN_ONLY_RE.fullmatch(s or ""):
        return ""
    return s


def _resolve_menu_name_unionpos(df: pd.DataFrame) -> pd.Series:
    """order_id 기준 menu_name(대표메뉴) 산정.

    - fin_product_grp.csv의 is_main_candidate=Y를 메인 경계로 사용
    - Y 후보가 없으면 금액(절대값) 최대 품목을 대표메뉴로 사용
    """
    prod_df = _load_fin_product()
    main_set = set(
        prod_df.loc[
            (prod_df["is_main_candidate"] == "Y")
            & (prod_df["exclude_check"] != "Y"),
            "상품코드",
        ].astype(str)
    )

    code_s = df["item_id"].fillna("").astype(str)
    df["_is_main"] = code_s.isin(main_set)
    df["_unit_abs"] = df["unit_price"].abs().fillna(0).astype(int)

    fee_like = df["item_name"].fillna("").astype(str).str.contains(
        r"(?:배달비|배달팁|포장비|쿠폰|할인|서비스|수수료)", na=False, regex=True
    )
    df["_fee_like"] = fee_like

    out = pd.Series("", index=df.index, dtype=str)

    for order_id, group in df.groupby("order_id", sort=False):
        grp = group.sort_values(["_row_in_file", "item_seq_num"], na_position="last")
        valid = grp[grp["item_name"].fillna("").astype(str).str.strip() != ""]
        if valid.empty:
            continue

        main_rows = valid[valid["_is_main"] & (valid["_unit_abs"] > 0)]
        if not main_rows.empty:
            current = str(main_rows.iloc[0]["item_name"])
            for idx, row in grp.iterrows():
                if row["_is_main"] and str(row.get("item_name", "")).strip() and int(row.get("_unit_abs", 0)) > 0:
                    current = str(row["item_name"])
                out.at[idx] = current
        else:
            pool = valid[~valid["_fee_like"] & (valid["_unit_abs"] > 0)]
            if pool.empty:
                pool = valid
            best_idx = pool["_unit_abs"].idxmax()
            name = str(pool.loc[best_idx, "item_name"])
            out.loc[group.index] = name

    return out


def _transform_unionpos_df(list_df: pd.DataFrame, item_df: pd.DataFrame) -> pd.DataFrame:
    order = _strip_and_coalesce_columns(list_df.copy())
    item = _strip_and_coalesce_columns(item_df.copy())

    for col in ("판매일시", "영수증번호", "포스번호", "판매타입", "매장명"):
        if col not in order.columns:
            order[col] = ""
        order[col] = order[col].fillna("").astype(str).str.strip()

    for col in ("영수증번호", "판매일시", "상품코드", "상품명", "수량", "합계", "단가", "판매타입", "분류명"):
        if col not in item.columns:
            item[col] = ""
        item[col] = item[col].fillna("").astype(str).str.strip()

    if "_store_partition" not in order.columns:
        order["_store_partition"] = order["매장명"].astype(str).str.strip().str.split().str[-1]
    if "_store_partition" not in item.columns:
        # store 파티션이 누락된 경우(비정상): 빈값으로 처리
        item["_store_partition"] = ""
    if "_row_in_file" not in item.columns:
        item = item.reset_index(drop=True)
        item["_row_in_file"] = pd.RangeIndex(len(item)).astype(int)

    # ID 정규화 (leading-zero 방지)
    order["포스번호"] = _normalize_id_col(order["포스번호"])
    order["영수증번호"] = _normalize_id_col(order["영수증번호"])
    item["영수증번호"] = _normalize_id_col(item["영수증번호"])

    # 전취(전취소) 영수증 제외 + 유효 판매타입만 남김
    st_o = order["판매타입"].fillna("").astype(str).str.strip()
    order = order[st_o.isin({"판매", "판매(반)", "반품"})].reset_index(drop=True)
    if order.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    st_i = item["판매타입"].fillna("").astype(str).str.strip()
    item_keep = st_i.isin({"", "판매", "판매(반)", "반품"})
    item = item[item_keep].reset_index(drop=True)

    # line amount: 판매/반품만 합계 반영, 옵션/하위행(판매타입 공백)은 0으로 둠
    st_i2 = item["판매타입"].fillna("").astype(str).str.strip()
    amt = _to_int_series(item["합계"])
    is_amount_row = st_i2.isin({"판매", "판매(반)", "반품"})
    item["_line_amt"] = amt.where(is_amount_row, 0)

    # qty
    item["_qty_int"] = _to_int_series(item["수량"])

    # item code
    item["상품코드"] = _normalize_unionpos_item_code(item["상품코드"])

    # merge 시 컬럼 충돌 방지 (판매타입/번호/sale_date 등)
    item = item.rename(
        columns={
            "번호": "item_line_no",
            "판매타입": "item_sale_type",
            "sale_date": "item_sale_date",
            "ym": "item_ym",
            "collected_at": "item_collected_at",
            "_pk": "item_pk",
        }
    )

    merged = pd.merge(
        order,
        item,
        on=["_store_partition", "영수증번호", "판매일시"],
        how="left",
    )

    # store/brand/source
    merged["source"] = UNIONPOS_SOURCE
    merged["brand"] = UNIONPOS_BRAND
    merged["store"] = merged["_store_partition"].fillna("").astype(str).str.strip()
    if "sale_date" not in merged.columns:
        # receipt_list에 없는 경우에만 fallback
        merged["sale_date"] = merged["판매일시"].astype(str).str[:10]
    merged["sale_date"] = merged["sale_date"].fillna("").astype(str).str.strip()
    merged["ym"] = merged["sale_date"].astype(str).str[:7]

    # store meta
    store_map = _load_store_map()
    merged["_skey"] = merged["store"].str.strip().str.split().str[-1]
    merged["담당자"] = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "담당자"))
    merged["region"] = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "region"))
    merged["실오픈일"] = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "실오픈일"))
    merged = merged.drop(columns=["_skey"])

    # order_time
    merged["order_time"] = merged["판매일시"].map(_normalize_time)

    # order_id
    merged["order_id"] = (
        merged["포스번호"].astype(str) + "_" + merged["영수증번호"].astype(str) + "_" + merged["판매일시"].astype(str)
    )

    # platform / order_type (포장 여부)
    cat_s = merged["분류명"].fillna("").astype(str)
    name_s = merged["상품명"].fillna("").astype(str)
    is_pack_row = cat_s.str.contains("포장", na=False) | name_s.str.contains("포장", na=False)
    is_pack = is_pack_row.groupby(merged["order_id"]).transform("any")
    merged["platform"] = "홀"
    merged["order_type"] = is_pack.map(lambda v: "홀_포장" if v else "홀_테이블")

    # item fields
    merged["item_id"] = merged["상품코드"].fillna("").astype(str).str.strip()
    merged["item_name"] = merged["상품명"].fillna("").astype(str).map(_normalize_unionpos_item_name)
    merged["qty"] = merged.get("_qty_int", 0).fillna(0).astype(int).astype(str)
    merged["unit_price"] = merged.get("_line_amt", 0).fillna(0).astype(int)
    # unionpos는 unit_price가 이미 라인 총액
    merged["total_price"] = merged["unit_price"]
    merged["discount_amount"] = 0

    # sale_type (정상/취소)
    st = merged["판매타입"].fillna("").astype(str).str.strip()
    merged["sale_type"] = st.map(lambda v: "취소" if "반품" in v else "정상")

    # 취소 라인은 total_price를 음수로 저장(집계 시 CASE 불필요)
    refund_mask = merged["sale_type"] == "취소"
    merged.loc[refund_mask, "total_price"] = -merged.loc[refund_mask, "total_price"].abs()
    merged.loc[~refund_mask, "total_price"] = merged.loc[~refund_mask, "total_price"].abs()

    # item_seq: 파일 내 원본 순서 기준
    merged["item_seq_num"] = pd.to_numeric(merged.get("item_line_no", ""), errors="coerce").fillna(9999)
    merged = merged.sort_values(["order_id", "_row_in_file", "item_seq_num"], na_position="last").reset_index(drop=True)
    merged["item_seq"] = merged.groupby("order_id").cumcount().add(1).astype(int).astype(str)

    # menu_name
    merged["menu_name"] = _resolve_menu_name_unionpos(
        merged.rename(columns={"_row_in_file": "_row_in_file"})
        .assign(
            _row_in_file=merged["_row_in_file"].fillna(0).astype(int),
        )
    ).fillna("").astype(str)

    # collected_at / _pk
    if "collected_at" not in merged.columns:
        merged["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    merged["collected_at"] = merged["collected_at"].fillna("").astype(str).str.strip()

    merged["_pk"] = _make_unified_pk(merged)

    # order_cnt: order_id별 대표행 1(정상) / -1(반품)
    merged["order_cnt"] = 0
    merged["_abs_amt"] = merged["unit_price"].abs().fillna(0).astype(int)
    valid_for_cnt = (merged["item_name"].fillna("").astype(str).str.strip() != "") & (merged["_abs_amt"] > 0)
    tmp = merged[valid_for_cnt].copy()
    if tmp.empty:
        idx = merged.groupby("order_id").head(1).index
    else:
        idx = tmp.groupby("order_id")["_abs_amt"].idxmax()
    merged.loc[idx.to_numpy(), "order_cnt"] = merged.loc[idx.to_numpy(), "sale_type"].map(lambda v: -1 if v == "취소" else 1)
    merged = merged.drop(columns=["_abs_amt"])

    # '(소)' 등 제거된 옵션행은 unified에서 제외 (분석 노이즈 방지)
    drop_meaningless = (merged["item_name"].fillna("").astype(str).str.strip() == "") & (merged["unit_price"].fillna(0).astype(int) == 0)
    merged = merged[~drop_meaningless].reset_index(drop=True)

    # 빈 item_id/item_name 행은 제외 (header-only merge 방지)
    keep_item = (merged["item_id"].fillna("").astype(str).str.strip() != "") | (merged["item_name"].fillna("").astype(str).str.strip() != "")
    merged = merged[keep_item].reset_index(drop=True)

    # cleanup
    for c in ("_line_amt", "_qty_int", "_fee_like", "_is_main", "_unit_abs"):
        if c in merged.columns:
            merged = merged.drop(columns=[c])

    merged = merged.reindex(columns=UNIFIED_COLUMNS, fill_value="")
    # item_id(상품코드) 기준으로 item_name을 fin 상품명으로 정합(LEFT JOIN 100% 목표)
    merged = _apply_fin_item_name(merged)
    return merged


def upsert_fin_product_grp_from_unionpos(
    ym: str | None = None,
    dry_run: bool = False,
) -> str:
    """UnionPOS receipt_items 기반으로 fin_product_grp.csv에 미등록 상품코드를 append.

    - 신규 항목은 llm_check=Y로 추가 (추후 DB_FinProduct/LLM로 분류 보강)
    - 대메뉴: unionpos_receipt_list.csv의 '매장명' 사용
    - 중메뉴: unionpos_receipt_items.csv의 '분류명' 사용
    """
    # 소스 로드
    if ym:
        yms = [ym]
    else:
        yms = sorted({p.parent.name.split("=", 1)[1] for p in UNIONPOS_BRAND_ROOT.glob("store=*/ym=*/unionpos_receipt_items.csv")})

    if not yms:
        raise FileNotFoundError(f"unionpos_receipt_items.csv 없음 | {UNIONPOS_BRAND_ROOT}")

    parts: list[pd.DataFrame] = []
    for _ym in yms:
        try:
            list_df, item_df = _load_unionpos_month(_ym)
        except Exception as e:
            logger.warning("월 로드 실패, 스킵: ym=%s | %s", _ym, e)
            continue
        if list_df.empty or item_df.empty:
            continue
        df = _transform_unionpos_df(list_df, item_df)
        if df.empty:
            continue
        parts.append(df)

    if not parts:
        return "스킵 (unionpos 데이터 없음)"

    uni = pd.concat(parts, ignore_index=True)

    # 상품 테이블로 변환
    prod = pd.DataFrame({
        "source": UNIONPOS_SOURCE,
        "대메뉴": uni.get("store", "").map(lambda s: f"{UNIONPOS_BRAND} {s}".strip() if str(s).strip() else UNIONPOS_BRAND),
        "중메뉴": "",  # 아래에서 채움
        "상품코드": uni.get("item_id", "").fillna("").astype(str).str.strip(),
        "상품명": uni.get("item_name", "").fillna("").astype(str).str.strip(),
        "표준_메뉴명": uni.get("item_name", "").fillna("").astype(str).str.strip(),
        "판매단가": 0,
        "수동분류": "기타",
        "is_main_candidate": "N",
        "llm_check": "Y",
        "exclude_check": "N",
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })

    # 중메뉴/판매단가: transform 결과에서 유추 불가 → 원본 item_df에서 채움
    # (store 파티션 + 영수증번호 + 판매일시 + 상품코드 조인)
    item = item_df.copy()
    item = _strip_and_coalesce_columns(item)
    for col in ("상품코드", "분류명", "단가", "영수증번호", "판매일시"):
        if col not in item.columns:
            item[col] = ""
    if "_store_partition" not in item.columns:
        item["_store_partition"] = ""
    item["상품코드"] = _normalize_unionpos_item_code(item["상품코드"])
    item["_판매단가_int"] = _to_int_series(item["단가"])
    item["_중메뉴"] = item["분류명"].fillna("").astype(str).str.strip()

    # item_id별 대표 단가/분류명 (가장 최근 행 우선)
    latest = (
        item[item["상품코드"].fillna("").astype(str).str.strip() != ""]
        .reset_index()
        .sort_values("index")
        .groupby("상품코드", as_index=False)
        .last()
    )
    price_map = latest.set_index("상품코드")["_판매단가_int"].to_dict()
    mid_map = latest.set_index("상품코드")["_중메뉴"].to_dict()

    prod["판매단가"] = prod["상품코드"].map(lambda c: int(price_map.get(str(c), 0)))
    prod["중메뉴"] = prod["상품코드"].map(lambda c: str(mid_map.get(str(c), "")).strip())

    prod = prod[prod["상품코드"] != ""].drop_duplicates(subset=["상품코드"], keep="last")

    # 기존 마스터 로드
    if FIN_PRODUCT_CSV_PATH.exists():
        master = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str).fillna("")
    else:
        master = pd.DataFrame(columns=list(prod.columns))

    if "상품코드" not in master.columns:
        master["상품코드"] = ""
    if "updated_at" not in master.columns:
        master["updated_at"] = ""
    master["상품코드"] = master["상품코드"].fillna("").astype(str).str.strip()

    existing = set(master["상품코드"].tolist())
    to_add = prod[~prod["상품코드"].isin(existing)].copy()

    # 기존 상품코드인데 상품명/중메뉴/판매단가가 바뀐 경우:
    # fin_product_grp.csv는 "코드별 마지막 행(last)"을 최신으로 가정하므로 overwrite가 아니라 append(히스토리)로 누적한다.
    to_change = pd.DataFrame(columns=list(prod.columns))
    if not master.empty:
        master_latest = (
            master.copy()
            .assign(상품코드=master["상품코드"].fillna("").astype(str).str.strip())
            .groupby("상품코드", as_index=False)
            .last()
        )

        # Backward compatibility: 일부 컬럼이 누락된 CSV도 존재
        for c in ("상품명", "중메뉴", "판매단가", "is_main_candidate", "exclude_check", "llm_check"):
            if c not in master_latest.columns:
                master_latest[c] = ""

        cur = prod[prod["상품코드"].isin(existing)].copy()
        if not cur.empty:
            cur["_code_norm"] = cur["상품코드"].fillna("").astype(str).str.strip()
            ml = master_latest.copy()
            ml["_code_norm"] = ml["상품코드"].fillna("").astype(str).str.strip()
            ml = ml.set_index("_code_norm")

            def _ml(col: str) -> pd.Series:
                return (
                    cur["_code_norm"]
                    .map(lambda k: str(ml.at[k, col]) if k in ml.index else "")
                    .fillna("")
                    .astype(str)
                )

            cur["_ml_name"] = _ml("상품명").str.strip()
            cur["_ml_mid"] = _ml("중메뉴").str.strip()
            cur["_ml_price"] = pd.to_numeric(_ml("판매단가"), errors="coerce").fillna(0).astype(int)

            cur_name = cur["상품명"].fillna("").astype(str).str.strip()
            cur_mid = cur["중메뉴"].fillna("").astype(str).str.strip()
            cur_price = pd.to_numeric(cur["판매단가"], errors="coerce").fillna(0).astype(int)

            changed = (cur_name != cur["_ml_name"]) | (cur_mid != cur["_ml_mid"]) | (cur_price != cur["_ml_price"])
            ch = cur[changed].copy()

            if not ch.empty:
                base = (
                    master_latest.set_index("상품코드")
                    .reindex(ch["상품코드"].tolist())
                    .reset_index()
                )
                for c in prod.columns:
                    if c not in base.columns:
                        base[c] = ""
                base = base.reindex(columns=list(prod.columns), fill_value="")

                base["상품명"] = ch["상품명"].tolist()
                base["중메뉴"] = ch["중메뉴"].tolist()
                base["판매단가"] = ch["판매단가"].tolist()
                # 변경분은 LLM 분류 대상이 아니므로 llm_check=N으로 둔다
                base["llm_check"] = "N"
                base["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                to_change = base.copy()

    if to_add.empty and to_change.empty:
        return "fin_product_grp.csv 변경 없음 (미등록/변경 상품코드 없음)"

    if dry_run:
        msg = []
        if not to_add.empty:
            msg.append(f"+{len(to_add)}행(신규)")
        if not to_change.empty:
            msg.append(f"+{len(to_change)}행(변경)")
        return f"dry_run: fin_product_grp.csv {' '.join(msg)} 추가 예정"

    out = pd.concat([master, to_add, to_change], ignore_index=True)
    out = _mark_is_latest(out)

    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        out.to_csv(tmp, index=False, encoding="utf-8-sig")
        try:
            os.replace(tmp, FIN_PRODUCT_CSV_PATH)
        except (PermissionError, OSError):
            FIN_PRODUCT_CSV_PATH.write_bytes(tmp.read_bytes())
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    parts = []
    if not to_add.empty:
        parts.append(f"+{len(to_add)}행(신규)")
    if not to_change.empty:
        parts.append(f"+{len(to_change)}행(변경)")
    return f"fin_product_grp.csv 업데이트: {' '.join(parts)} (unionpos)"


def run_unionpos(date_str: str, overwrite: bool = False) -> str:
    """특정 일자의 UnionPOS raw CSV를 unified_sales로 append/overwrite."""
    try:
        list_df, item_df = _load_unionpos_by_date(date_str)
    except FileNotFoundError as e:
        logger.warning("스킵: %s", e)
        return f"스킵 (unionpos CSV 없음 | {date_str})"

    df = _transform_unionpos_df(list_df, item_df)
    if df.empty:
        return f"스킵 (데이터 없음 | {date_str})"

    saved = _save_unified_daily(df, date_str, overwrite=overwrite)
    result = f"unified_sales(unionpos) 저장 | {date_str} | {saved}행"
    logger.info(result)
    return result


def run_lookback_unionpos(days: int = 7) -> str:
    """최근 N일 unionpos 데이터를 append-only로 적재."""
    try:
        import pendulum  # type: ignore
    except ModuleNotFoundError:
        from datetime import timedelta
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo("Asia/Seoul"))
        date_iter = ((now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, days + 1))
    else:
        kst = pendulum.timezone("Asia/Seoul")
        date_iter = (pendulum.now(kst).subtract(days=i).format("YYYY-MM-DD") for i in range(1, days + 1))

    total_saved = 0
    for date_str in date_iter:
        try:
            list_df, item_df = _load_unionpos_by_date(date_str)
        except FileNotFoundError:
            logger.info("unionpos CSV 없음, 스킵: %s", date_str)
            continue
        df = _transform_unionpos_df(list_df, item_df)
        if df.empty:
            continue
        saved = _save_unified_daily(df, date_str)
        total_saved += saved
        if saved:
            logger.info("lookback unionpos %s | 추가 %d행", date_str, saved)

    result = f"unified_sales(unionpos) lookback({days}일) | 추가 {total_saved}행"
    logger.info(result)
    return result


def backfill_unionpos() -> str:
    """unionpos_sales_raw 전체를 훑어서 일자별로 unified_sales로 적재."""
    paths = sorted(UNIONPOS_BRAND_ROOT.glob("store=*/ym=*/unionpos_receipt_list.csv"))
    if not paths:
        raise FileNotFoundError(f"unionpos_receipt_list.csv 없음 | {UNIONPOS_BRAND_ROOT}")

    date_set: set[str] = set()
    for p in paths:
        try:
            tmp = pd.read_csv(p, dtype=str, usecols=["sale_date"])
            date_set.update(tmp["sale_date"].astype(str).str.strip().dropna().unique())
        except Exception as e:
            logger.warning("sale_date 스캔 실패: %s | %s", p, e)

    total_targets = 0
    for date_str in sorted(date_set):
        if not date_str or date_str.lower() == "nan":
            continue
        try:
            run_unionpos(date_str)
            total_targets += 1
        except Exception as e:
            logger.warning("run_unionpos 실패: %s | %s", date_str, e)

    result = f"unified_sales(unionpos) backfill 완료 | {total_targets}일"
    logger.info(result)
    return result
