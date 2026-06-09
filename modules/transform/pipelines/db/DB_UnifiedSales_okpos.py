"""
unified_sales - OKPOS 채널 전용 모듈.
"""

import hashlib
import logging
from datetime import datetime
from functools import lru_cache

import pandas as pd

from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, RAW_OKPOS_SALES
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

logger = logging.getLogger(__name__)

OKPOS_BRAND_ROOT = RAW_OKPOS_SALES / "brand=도리당"

# 도리당 OKPOS "상품 미매칭" 알림은 운영상 불필요하여 기본 비활성화.
# (필요 시 수신자 이메일을 지정하면 다시 활성화됨)
UNMATCHED_ALERT_TO_EMAILS: str | None = None

# 모듈 레벨 캐시 (DAG 실행 단위로 재사용)
_OKPOS_MONTH_CACHE: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}

# 미매칭 상품코드 누적 (run 종료 시 1회 이메일 발송)
_PENDING_UNMATCHED: set[str] = set()


def _dedup_okpos_df_by_stable_key(df: pd.DataFrame, kind: str) -> pd.DataFrame:
    """OKPOS 원천 CSV의 중복 방어.

    수집 재시도/append/다운로드 포맷 변화 등으로 `_pk`가 달라져도
    실질적으로 동일 영수증(또는 영수증-상품라인)이 중복 적재되는 케이스가 있어
    영수증 기반 안정키로 한 번 더 dedup 한다.

    kind:
      - "order": okpos_order.csv
      - "item" : okpos_order_item.csv
    """
    if df.empty:
        return df

    df = df.copy()

    # 최신 수집분 우선(keep="last")을 보장하기 위해 collected_at으로 정렬
    if "collected_at" in df.columns:
        ts = pd.to_datetime(df["collected_at"], errors="coerce")
        df["_collected_at_ts"] = ts.fillna(pd.Timestamp.min)
        df = df.sort_values("_collected_at_ts", kind="mergesort").drop(columns=["_collected_at_ts"])

    if kind == "order":
        candidate_cols = [
            "sale_date",
            "매장명",
            "포스번호",
            "영수번호",
            "구분",
            "실매출액",
            "총매출액",
        ]
    elif kind == "item":
        # IMPORTANT:
        # okpos_order_item(영수증별매출) 리포트는 동일 영수증 내에
        # 동일 상품/수량/금액 행이 "여러 번" 등장하는 것이 정상일 수 있다(동일 메뉴 2회 주문 등).
        # 컬럼만으로는 이를 구분할 안정적인 line id가 없으므로,
        # 안정키 기반 drop_duplicates는 정상 데이터를 누락시킬 위험이 크다.
        return df
    else:
        return df

    subset = [c for c in candidate_cols if c in df.columns]
    # 키 컬럼이 충분하지 않으면 dedup 하지 않음(오탐 방지)
    if len(subset) < (4 if kind == "order" else 6):
        return df

    before = len(df)
    df = df.drop_duplicates(subset=subset, keep="last").reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        logger.info("okpos_%s 안정키 중복 제거: -%d행 | keys=%s", kind, dropped, subset)
    return df


@lru_cache(maxsize=1)
def _load_fin_product() -> pd.DataFrame:
    try:
        df = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str)
        df["상품코드"] = df["상품코드"].fillna("").str.strip()
        df["is_main_candidate"] = df["is_main_candidate"].fillna("N").str.strip().str.upper()
        if "exclude_check" not in df.columns:
            df["exclude_check"] = "N"
        df["exclude_check"] = df["exclude_check"].fillna("N").astype(str).str.strip().str.upper()
        # 코드별 최신 1행만 사용 (상품명 변경 등 히스토리 append 구조 대응)
        if "updated_at" in df.columns:
            ts = pd.to_datetime(df["updated_at"], errors="coerce")
            df["_updated_at_ts"] = ts.fillna(pd.Timestamp.min)
            df = df.sort_values(["상품코드", "_updated_at_ts"], na_position="last").groupby("상품코드", as_index=False).last()
            df = df.drop(columns=["_updated_at_ts"], errors="ignore")
        else:
            df = df.sort_values(["상품코드"]).groupby("상품코드", as_index=False).last()
        return df
    except Exception as e:
        logger.warning("fin_product_grp 로드 실패, fallback 사용: %s", e)
        return pd.DataFrame(columns=["상품코드", "is_main_candidate", "exclude_check"])


def _build_unmatched_report(codes: set[str]) -> str:
    """미매칭 상품코드별 원인을 분류해 리포트 문자열 반환."""
    prod_df = _load_fin_product()
    all_in_product = set(prod_df["상품코드"])
    name_map = prod_df.set_index("상품코드")["상품명"].to_dict() if "상품명" in prod_df.columns else {}
    candidate_map = prod_df.set_index("상품코드")["is_main_candidate"].to_dict()
    exclude_map = prod_df.set_index("상품코드")["exclude_check"].to_dict() if "exclude_check" in prod_df.columns else {}

    not_registered: list[str] = []
    marked_n: list[str] = []
    excluded: list[str] = []

    for code in sorted(codes):
        if code not in all_in_product:
            not_registered.append(code)
        else:
            if exclude_map.get(code, "N") == "Y":
                excluded.append(code)
            elif candidate_map.get(code, "N") != "Y":
                marked_n.append(code)

    lines: list[str] = []
    if not_registered:
        lines.append(f"■ 미등록 상품 ({len(not_registered)}건) — fin_product_grp.csv 추가 필요")
        for c in not_registered:
            lines.append(f"  · {c}")
    if excluded:
        lines.append(f"\n■ exclude_check=Y 상품 ({len(excluded)}건) — 제외 처리됨")
        for c in excluded:
            pname = name_map.get(c, "")
            lines.append(f"  · {c}  {pname}  [exclude_check=Y]")
    if marked_n:
        lines.append(f"\n■ is_main_candidate=N 상품 ({len(marked_n)}건) — 분류 검토")
        for c in marked_n:
            pname = name_map.get(c, "")
            flag = candidate_map.get(c, "N")
            lines.append(f"  · {c}  {pname}  [{flag}]")

    return "\n".join(lines) if lines else "(분류 불가)"


def _flush_unmatched_alert() -> None:
    """누적된 미매칭 상품코드를 이메일 1통으로 발송 후 초기화."""
    global _PENDING_UNMATCHED
    if not _PENDING_UNMATCHED:
        return
    codes = _PENDING_UNMATCHED.copy()
    _PENDING_UNMATCHED = set()
    if not UNMATCHED_ALERT_TO_EMAILS:
        logger.info("OKPOS 상품 미매칭 알림 스킵 (%d건)", len(codes))
        return
    from modules.transform.utility.mailer import send_email, text_to_html
    try:
        report = _build_unmatched_report(codes)
        body = text_to_html(
            f"OKPOS menu_name 보정 중 is_main_candidate=Y 후보 없는 주문 발생 ({len(codes)}개 상품코드)\n\n"
            f"{report}\n\n"
            f"→ fallback: item_seq 최소 상품명으로 대체 적용됨"
        )
        send_email(
            subject=f"[도리당] OKPOS 상품 미매칭 알림 ({len(codes)}건)",
            html_content=body,
            to_emails=UNMATCHED_ALERT_TO_EMAILS,
        )
    except Exception as e:
        logger.warning("미매칭 알림 이메일 발송 실패: %s", e)


def _resolve_menu_name(merged: pd.DataFrame) -> pd.Series:
    """order_key + 메인 경계 기준 행별 menu_name 결정.

    - is_main_candidate=Y 있음: item_seq 순 스캔, Y 항목마다 current_name 갱신 (메인 경계 분할)
    - is_main_candidate=Y 없음:
      - 홀: 배달비 제외 최고가 1개를 대표 메뉴로 전체 할당 + 알림 누적
      - 배달: 판매 상품명(=상품명) + 금액 기반 휴리스틱으로 메인 경계 추정해 분할
    - 반품 (상품명 NaN): 같은 날·매장 abs(_item_amt) 일치 주문 menu_name 상속
    """
    prod_df = _load_fin_product()
    if "exclude_check" not in prod_df.columns:
        prod_df = prod_df.assign(exclude_check="N")
    main_set = set(
        prod_df.loc[
            (prod_df["is_main_candidate"] == "Y")
            & (prod_df["exclude_check"].fillna("N").astype(str).str.strip().str.upper() != "Y"),
            "상품코드",
        ]
    )

    merged["_is_main"] = merged["상품코드"].fillna("").astype(str).isin(main_set)
    qty_col = "수량_y" if "수량_y" in merged.columns else "수량" if "수량" in merged.columns else None
    qty_num = pd.to_numeric(merged[qty_col], errors="coerce").fillna(0) if qty_col else pd.Series(0, index=merged.index)
    amt_num = pd.to_numeric(merged["_item_amt"], errors="coerce").fillna(0)
    safe_qty = qty_num.where(qty_num != 0, 1)
    unit_each = (amt_num / safe_qty).where(qty_num > 0, amt_num)
    merged["_unit_price_num"] = unit_each.fillna(0)
    merged["_item_seq_num"] = pd.to_numeric(merged["item_seq"], errors="coerce").fillna(9999)

    result = pd.Series("", index=merged.index, dtype=str)
    unmatched_codes: set[str] = set()
    refund_keys: list[str] = []

    for order_key, group in merged.groupby("_order_key"):
        grp = group.sort_values("_item_seq_num")
        valid = grp[grp["상품명"].notna() & (grp["상품명"].astype(str).str.strip() != "")]

        if valid.empty:
            refund_keys.append(order_key)
            continue

        # 0원 이하 Y항목은 메인 경계 판단에서 제외 (부재료/서비스 상품 방지)
        main_rows = valid[valid["_is_main"] & (valid["_unit_price_num"] > 0)]

        if not main_rows.empty:
            # Y 항목 경계마다 current_name 갱신 → 행별 분할 할당
            current_name = str(main_rows.iloc[0]["상품명"])
            for idx, row in grp.iterrows():
                if row["_is_main"] and pd.notna(row["상품명"]) and str(row["상품명"]).strip() and row["_unit_price_num"] > 0:
                    current_name = str(row["상품명"])
                result.at[idx] = current_name
        else:
            # 미등록 또는 유효 Y항목 없음:
            # - 홀: 대표 메뉴 1개로 전체 할당 + 알림
            # - 배달: 상품명/금액 기반으로 메인 경계 추정해 여러 메뉴로 분할 (한 주문 내 2메뉴 이상 대응)
            order_type_sample = ""
            if "order_type" in grp.columns:
                _ot = grp["order_type"].dropna().astype(str).map(str.strip)
                _ot = _ot[_ot != ""]
                order_type_sample = _ot.iloc[0] if len(_ot) else ""

            table_sample = ""
            table_col = next((c for c in ("테이블명_x", "테이블명") if c in grp.columns), None)
            if table_col:
                _tv = grp[table_col].dropna().astype(str).map(str.strip)
                _tv = _tv[_tv != ""]
                table_sample = _tv.iloc[0] if len(_tv) else ""

            is_hall_order = False
            if table_sample:
                if "배달" in table_sample:
                    is_hall_order = False
                elif table_sample.isdigit() or ("포장" in table_sample):
                    is_hall_order = True
            if not is_hall_order and order_type_sample:
                # 주문유형 쪽에도 "홀/포장/매장" 표기가 있는 케이스가 있어 보조 신호로 사용
                if any(token in order_type_sample for token in ("홀", "포장", "매장")) and ("배달" not in order_type_sample):
                    is_hall_order = True

            name_s = valid["상품명"].fillna("").astype(str).str.strip()
            fee_like = name_s.str.contains(r"(?:배달비|배달팁|포장비|쿠폰|할인|서비스|수수료)", na=False, regex=True)

            if is_hall_order:
                # 홀 주문만 알림 대상 (배달 미등록은 정상 케이스)
                if "order_type" in valid.columns:
                    hall = valid[valid["order_type"].astype(str).str.contains("홀", na=False)]
                    unmatched_codes.update(hall["상품코드"].dropna().astype(str).unique())
                else:
                    unmatched_codes.update(valid["상품코드"].dropna().astype(str).unique())

                pool = valid[~fee_like]
                if pool.empty:
                    pool = valid
                best_idx = pool["_unit_price_num"].idxmax()
                name = str(pool.loc[best_idx, "상품명"])
                result.loc[group.index] = name
            else:
                # 배달: 메인 후보를 금액/텍스트로 추정해 경계 분할
                # - 유료 옵션(토핑/추가/사리 등)은 메인으로 오인하기 쉬워 제외
                option_like = name_s.str.contains(r"(?:추가|옵션|토핑|사리|변경|선택|곱빼기|맵기)", na=False, regex=True)
                max_price = float(valid["_unit_price_num"].max())
                # 주문 내 최고가 대비 비율(여러 메인 메뉴 주문 대응) + 최소금액(옵션 방지)
                threshold = max(2000.0, max_price * 0.25)

                mainish = valid[
                    (valid["_unit_price_num"] >= threshold)
                    & (~fee_like)
                    & (~option_like)
                ]

                if mainish.empty:
                    # 휴리스틱도 실패하면 기존처럼 대표 메뉴 1개 선택
                    pool = valid[~fee_like]
                    if pool.empty:
                        pool = valid
                    best_idx = pool["_unit_price_num"].idxmax()
                    name = str(pool.loc[best_idx, "상품명"])
                    result.loc[group.index] = name
                else:
                    main_idxs = set(mainish.index)
                    current_name = str(mainish.iloc[0]["상품명"])
                    for idx, row in grp.iterrows():
                        if idx in main_idxs and pd.notna(row.get("상품명")) and str(row.get("상품명", "")).strip():
                            # 0원/음수 라인은 메인 경계에서 제외 (할인/서비스/쿠폰 등)
                            if float(row.get("_unit_price_num", 0) or 0) > 0:
                                current_name = str(row.get("상품명"))
                        result.at[idx] = current_name

    # 반품 후처리
    if refund_keys:
        refund_set = set(refund_keys)
        pos_lookup: dict[tuple, str] = {}   # (store_day, amt) → name
        id_lookup: dict[tuple, str] = {}    # (매장명, 포스번호, 영수번호) → name
        for order_key, group in merged.groupby("_order_key"):
            if order_key in refund_set:
                continue
            names = result.loc[group.index]
            order_name = names[names != ""].iloc[0] if (names != "").any() else ""
            if not order_name:
                continue
            parts = order_key.split("|")
            store_day = "|".join(parts[:2])
            grp_total = abs(int(group["_item_amt"].sum()))
            if grp_total > 0:
                pos_lookup.setdefault((store_day, grp_total), order_name)
            # ID 기반 룩업: 금액 0 반품 대비 (매장명, 포스번호, 영수번호)
            if len(parts) >= 4:
                id_lookup.setdefault((parts[1], parts[2], parts[3]), order_name)

        for key in refund_keys:
            refund_grp = merged[merged["_order_key"] == key]
            store_day = "|".join(key.split("|")[:2])
            refund_amt = abs(int(refund_grp["_item_amt"].sum()))

            matched = ""
            if refund_amt > 0:
                matched = pos_lookup.get((store_day, refund_amt), "")
            if not matched:
                # 금액 매칭 실패(0원 반품 등) → 동일 매장·포스·영수번호로 2차 매칭
                parts = key.split("|")
                if len(parts) >= 4:
                    matched = id_lookup.get((parts[1], parts[2], parts[3]), "")
            if matched:
                result.loc[refund_grp.index] = matched

    if unmatched_codes:
        _PENDING_UNMATCHED.update(unmatched_codes)

    return result


def _load_okpos_month(ym: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """ym='YYYY-MM' 전체 월 데이터 로드 (모듈 레벨 캐시).
    같은 ym을 여러 날짜에서 호출해도 1회만 읽음.
    """
    global _OKPOS_MONTH_CACHE
    if ym in _OKPOS_MONTH_CACHE:
        return _OKPOS_MONTH_CACHE[ym]

    order_dfs: list[pd.DataFrame] = []
    item_dfs: list[pd.DataFrame] = []

    for path in sorted(OKPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/okpos_order.csv")):
        try:
            df = pd.read_csv(path, dtype=str)
            # 수집 재시도/append 등으로 동일 _pk가 중복되는 케이스 방지
            if "_pk" in df.columns:
                before = len(df)
                df = df.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)
                dropped = before - len(df)
                if dropped:
                    logger.info("okpos_order 중복 제거: -%d행 | %s", dropped, path)
            df = _dedup_okpos_df_by_stable_key(df, kind="order")
            order_dfs.append(df)
        except Exception as e:
            logger.warning("okpos_order 로드 실패: %s | %s", path, e)

    for path in sorted(OKPOS_BRAND_ROOT.glob(f"store=*/ym={ym}/okpos_order_item.csv")):
        try:
            df = pd.read_csv(path, dtype=str)
            # 수집 재시도/append 등으로 동일 _pk가 중복되는 케이스 방지
            if "_pk" in df.columns:
                before = len(df)
                df = df.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)
                dropped = before - len(df)
                if dropped:
                    logger.info("okpos_order_item 중복 제거: -%d행 | %s", dropped, path)
            df = _dedup_okpos_df_by_stable_key(df, kind="item")
            item_dfs.append(df)
        except Exception as e:
            logger.warning("okpos_order_item 로드 실패: %s | %s", path, e)

    order_df = pd.concat(order_dfs, ignore_index=True) if order_dfs else pd.DataFrame()
    item_df = pd.concat(item_dfs, ignore_index=True) if item_dfs else pd.DataFrame()
    # 최종 방어 (concat 이후에도 중복 남을 수 있음)
    if not order_df.empty and "_pk" in order_df.columns:
        order_df = order_df.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)
    if not item_df.empty and "_pk" in item_df.columns:
        item_df = item_df.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)

    logger.info("OKPOS 월 로드: order %d행, item %d행 | ym=%s", len(order_df), len(item_df), ym)
    _OKPOS_MONTH_CACHE[ym] = (order_df, item_df)
    return order_df, item_df


def _load_okpos_by_date(date_str: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """지정 날짜의 okpos_order + okpos_order_item (월별 캐시에서 날짜 필터)."""
    ym = date_str[:7]
    order_df, item_df = _load_okpos_month(ym)

    if order_df.empty:
        raise FileNotFoundError(f"okpos_order CSV 없음 | ym={ym}")
    if item_df.empty:
        raise FileNotFoundError(f"okpos_order_item CSV 없음 | ym={ym}")

    order_f = order_df[order_df["sale_date"].str.strip() == date_str].reset_index(drop=True)
    item_f = item_df[item_df["sale_date"].str.strip() == date_str].reset_index(drop=True)

    if order_f.empty:
        raise FileNotFoundError(f"okpos_order 데이터 없음 | {date_str}")
    if item_f.empty:
        raise FileNotFoundError(f"okpos_order_item 데이터 없음 | {date_str}")

    logger.info("OKPOS 필터: order %d행, item %d행 | %s", len(order_f), len(item_f), date_str)
    return order_f, item_f


def _make_okpos_pk(sale_date: str, store: str, platform: str, order_id: str, item_seq: str) -> str:
    key = f"{sale_date}|okpos|{store}|{platform}|{order_id}|{item_seq}"
    return hashlib.md5(key.encode()).hexdigest()


def _transform_okpos_df(order_df: pd.DataFrame, item_df: pd.DataFrame) -> pd.DataFrame:
    """OKPOS order + order_item → unified_sales 스키마 DataFrame."""
    order = order_df.copy()
    item = item_df.copy()

    # OKPOS order_item 내 행 순서를 PK 안정화용 tie-breaker로 사용
    # (pandas merge 결과 행 순서가 런마다 달라질 수 있어 cumcount 기반 item_seq가 변동 → 재실행 시 중복 적재 발생)
    item = item.reset_index(drop=True)
    item["_item_row"] = pd.RangeIndex(len(item))

    order = _strip_and_coalesce_columns(order)
    item = _strip_and_coalesce_columns(item)

    for col in ["총매출액", "실매출액"]:
        if col in order.columns:
            order[col] = _to_int_series(order[col])
    for col in ["총매출액", "실매출액"]:
        if col in item.columns:
            item[col] = _to_int_series(item[col])

    # 포스번호/영수번호 leading-zero 정규화: order↔item 간 '1' vs '01' 불일치 방지
    for _col in ["포스번호"]:
        if _col in order.columns:
            order[_col] = _normalize_id_col(order[_col])
        if _col in item.columns:
            item[_col] = _normalize_id_col(item[_col])
    if "영수번호" in order.columns:
        order["영수번호"] = _normalize_id_col(order["영수번호"])
    if "영수증번호" in item.columns:
        item["영수증번호"] = _normalize_id_col(item["영수증번호"])

    # OKPOS는 "영수번호"(order) vs "영수증번호"(item)로 컬럼명이 달라짐.
    # 영수번호/포스번호는 매장 간에도 재사용될 수 있어 "매장명"까지 포함해서 join 하는 것이 안전.
    if (
        {"매장명", "포스번호", "영수번호", "sale_date"}.issubset(order.columns)
        and {"매장명", "포스번호", "영수증번호", "sale_date"}.issubset(item.columns)
    ):
        merged = pd.merge(
            order,
            item,
            left_on=["매장명", "포스번호", "영수번호", "sale_date"],
            right_on=["매장명", "포스번호", "영수증번호", "sale_date"],
            how="left",
        )
    elif (
        {"포스번호", "영수번호", "sale_date"}.issubset(order.columns)
        and {"포스번호", "영수증번호", "sale_date"}.issubset(item.columns)
    ):
        merged = pd.merge(
            order,
            item,
            left_on=["포스번호", "영수번호", "sale_date"],
            right_on=["포스번호", "영수증번호", "sale_date"],
            how="left",
        )
    else:
        merged = pd.merge(
            order,
            item,
            left_on=["영수번호", "sale_date"],
            right_on=["영수증번호", "sale_date"],
            how="left",
        )

    channel_col = "주문채널" if "주문채널" in merged.columns else None

    actual_col = (
        "실매출액_y" if "실매출액_y" in merged.columns
        else "실매출액" if "실매출액" in merged.columns
        else None
    )
    total_col = (
        "총매출액_y" if "총매출액_y" in merged.columns
        else "총매출액" if "총매출액" in merged.columns
        else None
    )

    actual_y = _to_int_series(merged[actual_col]) if actual_col else pd.Series(0, index=merged.index)
    total_y = _to_int_series(merged[total_col]) if total_col else pd.Series(0, index=merged.index)

    # item join 미매칭(반품 등) → order의 _x 컬럼으로 fallback
    if actual_col == "실매출액_y" and "실매출액_x" in merged.columns:
        no_match = merged["실매출액_y"].isna()
        actual_y = actual_y.where(~no_match, _to_int_series(merged["실매출액_x"]))
    if total_col == "총매출액_y" and "총매출액_x" in merged.columns:
        no_match = merged["총매출액_y"].isna()
        total_y = total_y.where(~no_match, _to_int_series(merged["총매출액_x"]))

    # total_price 기준은 "실매출액" (할인 반영 후 금액)으로 고정한다.
    # (OKPOS 화면/일자별 종합매출의 '실매출'과 일치해야 함)
    merged["_item_amt"] = actual_y

    # total_price: 원천 리포트 기준 라인 총액(반올림 없는 합계용)
    merged["total_price"] = pd.to_numeric(merged["_item_amt"], errors="coerce").fillna(0).astype(int)

    # discount_amount: 원천 할인액(라인 기준). (없으면 0)
    disc_col = "할인액_y" if "할인액_y" in merged.columns else "할인액" if "할인액" in merged.columns else None
    if disc_col:
        merged["discount_amount"] = _to_int_series(merged[disc_col])
    else:
        merged["discount_amount"] = 0

    # 영수증 단위로 grouping 하기 위한 안정 키(매장/포스/영수번호 기반).
    # 기존 _pk_x(행 hash)는 다운로드/정렬/표기 변화에 취약할 수 있어 가능하면 사용하지 않음.
    merged["_order_key"] = merged["_pk_x"].fillna("").astype(str)
    pos_col = "포스번호_x" if "포스번호_x" in merged.columns else "포스번호" if "포스번호" in merged.columns else None
    rcpt_col = "영수번호" if "영수번호" in merged.columns else "영수증번호" if "영수증번호" in merged.columns else None
    store_name_col = "매장명_x" if "매장명_x" in merged.columns else "매장명" if "매장명" in merged.columns else None
    if pos_col and rcpt_col and store_name_col and "sale_date" in merged.columns:
        merged["_order_key"] = (
            merged["sale_date"].fillna("").astype(str).str.strip()
            + "|" + merged[store_name_col].fillna("").astype(str).str.strip()
            + "|" + merged[pos_col].fillna("").astype(str).str.strip()
            + "|" + merged[rcpt_col].fillna("").astype(str).str.strip()
        )
    else:
        logger.warning("okpos: pos_col/rcpt_col 미존재 → _order_key fallback to _pk_x (pos=%s, rcpt=%s)", pos_col, rcpt_col)
    _empty_key = (merged["_order_key"] == "").sum()
    if _empty_key:
        logger.warning("okpos: _order_key 빈값 %d행 — order_cnt 오집계 위험", _empty_key)

    # 그룹 내 정렬을 고정해 item_seq / 대표메뉴(menu_name) 산정이 재실행에도 동일하도록 보장
    if "_item_row" in merged.columns:
        merged["_item_row"] = pd.to_numeric(merged["_item_row"], errors="coerce")
        merged = merged.sort_values(["_order_key", "_item_row"], kind="mergesort").reset_index(drop=True)
    else:
        merged = merged.sort_values(["_order_key"], kind="mergesort").reset_index(drop=True)

    # order_type 기본값 미리 할당 (_resolve_menu_name 내 홀/배달 구분에 사용)
    ot_col = "주문유형_x" if "주문유형_x" in merged.columns else "주문유형" if "주문유형" in merged.columns else None
    merged["order_type"] = (merged[ot_col].fillna("").astype(str).str.strip() if ot_col else "")

    merged["item_seq"] = (merged.groupby("_order_key").cumcount() + 1).astype(str)
    merged["menu_name"] = _resolve_menu_name(merged).fillna("").astype(str)
    merged.drop(columns=["_is_main", "_unit_price_num", "_item_seq_num"], errors="ignore", inplace=True)
    # sale_type 확정 후 order_cnt 계산하기 위해 _item_seq_int만 미리 계산해 둠
    merged["_item_seq_int"] = pd.to_numeric(merged["item_seq"], errors="coerce").fillna(0).astype(int)

    if store_name_col:
        merged["brand"] = merged[store_name_col].str.split(" ").str[0].fillna("")
        merged["store"] = merged[store_name_col].str.split(" ").str[1].fillna("")
    else:
        merged["brand"] = ""
        merged["store"] = ""

    store_map = _load_store_map()
    merged["_skey"] = merged["store"].str.strip().str.split().str[-1]
    merged["담당자"] = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "담당자"))
    merged["region"] = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "region"))
    merged["실오픈일"] = merged["_skey"].map(lambda k: _lookup_store_meta(store_map, k, "실오픈일"))

    merged["source"] = "okpos"
    merged["ym"] = merged["sale_date"].str[:7]
    merged["platform"] = (merged[channel_col].fillna("").astype(str).str.strip() if channel_col else "")

    # order_time: OKPOS는 주문서의 "최초주문시각"을 우선 사용하고, 없으면 결제시각으로 fallback.
    # 병합 후 suffix가 붙은 주문서(_x) 컬럼을 먼저 봐야 item 미매칭 반품도 00:00:00으로 떨어지지 않는다.
    time_candidates = [
        c for c in (
            "최초주문시각_x", "최초주문시각",
            "최초주문_x", "최초주문",
            "결제시각_x", "결제시각",
            "결제시각_y",
        )
        if c in merged.columns
    ]
    merged["order_time"] = ""
    for time_col in time_candidates:
        normalized = merged[time_col].apply(_normalize_time)
        empty_mask = merged["order_time"] == ""
        merged.loc[empty_mask, "order_time"] = normalized[empty_mask]

    # item-level 시각은 영수증 내 행마다 달라질 수 있어 _order_key 기준 첫 값으로 통일한다.
    merged["order_time"] = merged.groupby("_order_key", sort=False)["order_time"].transform(
        lambda s: next((v for v in s if v), "")
    )

    # order_id: {포스번호-영수번호}_{order_time} 형식 (예: 1-14_11:00:10).
    # order_time 이 비어있으면 구분자를 '_'로 바꾸고 00:00:00 으로 대체한다 (예: 1_14_00:00:00).
    # (unified PK에는 sale_date/store가 포함되므로 order_id 자체는 매장 간 유일성까지 요구하지 않음)
    has_time = merged["order_time"] != ""
    time_suffix = merged["order_time"].where(has_time, "00:00:00")
    if pos_col and rcpt_col:
        pos_s = merged[pos_col].fillna("").astype(str).str.strip()
        rcpt_s = merged[rcpt_col].fillna("").astype(str).str.strip()
        sep = has_time.map({True: "-", False: "_"})
        store_s = (
            merged[store_name_col].fillna("").astype(str).str.split(" ").str[-1].str.strip()
            if store_name_col
            else pd.Series("", index=merged.index)
        )
        merged["order_id"] = store_s + "_" + pos_s + sep + rcpt_s + "_" + time_suffix
    else:
        merged["order_id"] = merged["_order_key"].fillna("").astype(str) + "_" + time_suffix

    # 테이블명 기반 보정: OKPOS의 주문유형이 "홀(매장)"으로 고정되는 경우가 있어 포장 여부를 추가 반영
    table_col = "테이블명_x" if "테이블명_x" in merged.columns else "테이블명" if "테이블명" in merged.columns else None
    if table_col:
        table_v = merged[table_col].fillna("").astype(str).str.strip()
        is_pack = table_v.str.contains("포장", regex=False)
        is_num = table_v.str.fullmatch(r"\d+").fillna(False)

        # 테이블명 규칙: "포스"/"제휴사주문" 주문에만 적용 (배달 채널 보호)
        # - 테이블명에 "포장" 포함 → order_type="홀_포장",   platform="홀 포장"
        # - 테이블명이 순수 숫자    → order_type="홀_테이블", platform="홀"
        platform_v = merged["platform"].fillna("").astype(str).str.strip() if "platform" in merged.columns else ""
        is_hall_platform = platform_v.isin(["포스", "제휴사주문"]) if hasattr(platform_v, "isin") else False
        cond = is_hall_platform if hasattr(is_hall_platform, "__and__") else None

        is_delivery_table = table_v.str.contains("배달", regex=False)

        if cond is not None:
            merged.loc[cond & is_pack, "order_type"] = "홀_포장"
            merged.loc[cond & is_pack, "platform"] = "홀"
            merged.loc[cond & (~is_pack) & is_num, "order_type"] = "홀_테이블"
            merged.loc[cond & (~is_pack) & is_num, "platform"] = "홀"
            # 테이블명에 "배달" 포함 (배달1, 배달-01 등) → 배달 주문
            merged.loc[cond & (~is_pack) & (~is_num) & is_delivery_table, "order_type"] = "배달"
        else:
            merged.loc[is_pack, "order_type"] = "홀_포장"
            merged.loc[is_pack, "platform"] = "홀"
            merged.loc[(~is_pack) & is_num, "order_type"] = "홀_테이블"
            merged.loc[(~is_pack) & is_num, "platform"] = "홀"
            merged.loc[(~is_pack) & (~is_num) & is_delivery_table, "order_type"] = "배달"

    # 최종 order_type 정규화: 배달 채널 포장 주문 및 미분류 값 처리
    _HALL_CH = {"포스", "제휴사주문", "홀"}
    _VALID_OT = {"홀_테이블", "홀_포장", "배달_포장", "배달"}
    pf_v = merged["platform"].fillna("").astype(str).str.strip()
    ot_v = merged["order_type"].fillna("").astype(str).str.strip()
    # 포스전화(CID) 포장 → 홀_포장 (전화 픽업은 홀 포장으로 처리)
    merged.loc[(pf_v == "포스전화(CID)") & (ot_v == "포장"), "order_type"] = "홀_포장"
    ot_v = merged["order_type"].fillna("").astype(str).str.strip()
    # 배달 채널(배달의민족/쿠팡이츠 등) 포장 주문 → 배달_포장
    merged.loc[~pf_v.isin(_HALL_CH) & (ot_v == "포장"), "order_type"] = "배달_포장"
    # 나머지 미분류(홀(매장) 등) → 홀_테이블
    ot_v2 = merged["order_type"].fillna("").astype(str).str.strip()
    merged.loc[~ot_v2.isin(_VALID_OT), "order_type"] = "홀_테이블"

    merged["item_id"] = merged["상품코드"].fillna("").astype(str)
    merged["item_name"] = merged["상품명"].fillna("").astype(str)

    qty_col = "수량_y" if "수량_y" in merged.columns else "수량"
    qty_source = merged[qty_col] if qty_col in merged.columns else pd.Series(0, index=merged.index)
    qty_num = pd.to_numeric(qty_source, errors="coerce").fillna(0).astype(int)
    merged["qty"] = qty_num.astype(str)

    # unit_price는 "총액"(_item_amt)이 아니라 "단가"를 유지 (unit_price / qty 이슈 방지)
    amt_num = pd.to_numeric(merged["_item_amt"], errors="coerce").fillna(0)
    safe_qty = qty_num.where(qty_num != 0, 1)
    unit_each = (amt_num / safe_qty).where(qty_num > 0, amt_num)
    merged["unit_price"] = unit_each.round().astype(int)

    gbn_col = "구분_x" if "구분_x" in merged.columns else "구분" if "구분" in merged.columns else None
    if gbn_col:
        merged["sale_type"] = merged[gbn_col].str.strip().map({"매출": "정상", "반품": "취소"}).fillna("정상")
    else:
        merged["sale_type"] = "정상"

    # order_cnt: 영수증(order_key)당 마지막 item 행에만 1(정상) 또는 -1(취소) 할당.
    # 취소 주문도 마지막 item 행만 -1로 처리해야 합산 시 1건 차감이 된다.
    # (모든 취소 item 행에 -1을 쓰면 item 수만큼 중복 차감됨)
    _is_last = merged["_item_seq_int"] == merged.groupby("_order_key")["_item_seq_int"].transform("max")
    merged["order_cnt"] = 0
    merged.loc[_is_last & (merged["sale_type"] != "취소"), "order_cnt"] = 1
    merged.loc[_is_last & (merged["sale_type"] == "취소"), "order_cnt"] = -1
    merged = merged.drop(columns=["_item_seq_int"])
    # 취소 라인은 total_price를 음수로 저장(집계 시 CASE 불필요)
    refund_mask = merged["sale_type"] == "취소"
    merged.loc[refund_mask, "total_price"] = -merged.loc[refund_mask, "total_price"].abs()
    merged.loc[~refund_mask, "total_price"] = merged.loc[~refund_mask, "total_price"].abs()
    merged.loc[refund_mask, "discount_amount"] = -merged.loc[refund_mask, "discount_amount"].abs()
    merged.loc[~refund_mask, "discount_amount"] = merged.loc[~refund_mask, "discount_amount"].abs()

    # NOTE:
    # okpos_order(today)와 okpos_order_item(receipt_details) 합계가 일치하지 않는 케이스가 존재한다.
    # unified_sales는 원천 item 리포트(영수증별매출) 그레인을 유지하므로,
    # 정합성 체크/재수집은 OKPOS 수집 DAG(validate_today_vs_receipt)에서 수행한다.

    merged["collected_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    merged["_pk"] = _make_unified_pk(merged)

    # 마지막 방어: 병합/정렬 이슈로 동일 PK가 중복되는 경우 제거
    if "_pk" in merged.columns:
        before = len(merged)
        merged = merged.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)
        dropped = before - len(merged)
        if dropped:
            logger.info("unified_sales(okpos) PK 중복 제거: -%d행", dropped)

    # ============================================================
    # OKPOS 리포트 불일치(주문서 vs 영수증별) 조정 라인 추가
    # ============================================================
    # 목적: unified_sales의 일자/매장 total_price 합계가 okpos_order(today) 합계와 일치하도록 보장.
    # - item 리포트(okpos_order_item)가 누락/정의 차이로 합계가 다를 수 있음.
    # - 실제 아이템 행을 변경하지 않고, 영수증(_order_key)별 차액을 1개 조정 행으로 별도 기록한다.
    #
    # 조정 행 규칙:
    # - item_id: "__OKPOS_ADJ__"
    # - item_name: "정산차액(OKPOS)"
    # - qty: "1"
    # - unit_price: total_price와 동일(차액)
    # - total_price: 차액(반품 포함 signed)
    #
    # NOTE: 조정 행이 들어간 주문은 item 합계가 order 합계와 맞아야 정상.
    try:
        channel_col2 = "주문채널" if "주문채널" in merged.columns else None
        order_actual_col2 = "실매출액_x" if "실매출액_x" in merged.columns else "실매출액" if "실매출액" in merged.columns else None
        order_total_col2 = "총매출액_x" if "총매출액_x" in merged.columns else "총매출액" if "총매출액" in merged.columns else None
        if channel_col2 and order_actual_col2 and "_order_key" in merged.columns:
            order_actual2 = _to_int_series(merged[order_actual_col2])
            order_amt = order_actual2.astype(int)

            # 실매출액_x는 CSV 저장 시 반품 행이 이미 음수로 저장됨 → 직접 사용 (sign flip 금지)
            order_amt_signed = order_amt.astype(int)

            tmp = pd.DataFrame({
                "_order_key": merged["_order_key"].astype(str),
                "__order_amt_signed": order_amt_signed.to_numpy(),
                "__item_total_price": pd.to_numeric(merged["total_price"], errors="coerce").fillna(0).astype(int).to_numpy(),
            })
            tmp["__order_amt_first"] = tmp.groupby("_order_key", dropna=False)["__order_amt_signed"].transform("first")
            tmp["__item_sum"] = tmp.groupby("_order_key", dropna=False)["__item_total_price"].transform("sum")
            tmp["__diff"] = (tmp["__order_amt_first"] - tmp["__item_sum"]).fillna(0).astype(int)

            diff_map = tmp.groupby("_order_key", dropna=False)["__diff"].first()
            diff_map = diff_map[diff_map != 0]
            if not diff_map.empty:
                adj_rows: list[pd.Series] = []
                # representative row: abs(total_price) 최대
                merged["_abs_tp"] = merged["total_price"].abs()
                rep_idx = merged.groupby("_order_key", dropna=False)["_abs_tp"].idxmax()
                rep = merged.loc[rep_idx].copy()
                rep = rep.set_index("_order_key", drop=False)

                # item_seq 다음 번호 생성
                seq_max = merged.groupby("_order_key", dropna=False)["item_seq"].apply(lambda s: pd.to_numeric(s, errors="coerce").fillna(0).max())
                for ok, diff_val in diff_map.items():
                    if ok not in rep.index:
                        continue
                    row = rep.loc[ok].copy()
                    next_seq = int(seq_max.get(ok, 0) or 0) + 1
                    row["item_seq"] = str(next_seq)
                    row["item_id"] = "__OKPOS_ADJ__"
                    row["item_name"] = "정산차액(OKPOS)"
                    row["menu_name"] = row.get("menu_name", "")
                    row["qty"] = "1"
                    row["unit_price"] = int(abs(diff_val))
                    row["total_price"] = int(diff_val)
                    row["discount_amount"] = 0
                    row["sale_type"] = "정상" if diff_val >= 0 else "취소"
                    row["order_cnt"] = 0
                    row["_pk"] = ""  # 재계산
                    adj_rows.append(row)

                if adj_rows:
                    adj_df = pd.DataFrame(adj_rows)
                    # pk 재계산을 위해 필요한 컬럼 보장
                    adj_df["_pk"] = _make_unified_pk(adj_df)
                    merged = pd.concat([merged.drop(columns=["_abs_tp"], errors="ignore"), adj_df], ignore_index=True)
                else:
                    merged = merged.drop(columns=["_abs_tp"], errors="ignore")
            else:
                merged = merged.drop(columns=["_abs_tp"], errors="ignore")
    except Exception as e:
        logger.warning("OKPOS 조정 라인 생성 실패(무시): %s", e)

    # NaN 방지: 반품 등 item join 미매칭 행의 문자열 컬럼 null → ""
    for _col in UNIFIED_COLUMNS:
        if _col in merged.columns and merged[_col].dtype == object:
            merged[_col] = merged[_col].fillna("").astype(str)

    # item_id(상품코드) 기준으로 item_name을 fin 상품명으로 정합(LEFT JOIN 100% 목표)
    merged = _apply_fin_item_name(merged)
    return merged[UNIFIED_COLUMNS]


def run_okpos(date_str: str, overwrite: bool = False) -> str:
    """특정 날짜 OKPOS order+order_item → unified_sales 저장.

    overwrite=True: 기존 데이터 교체 (정정용).
    """
    try:
        order_df, item_df = _load_okpos_by_date(date_str)
    except FileNotFoundError as e:
        logger.warning("스킵: %s", e)
        return f"스킵 (okpos CSV 없음 | {date_str})"
    df = _transform_okpos_df(order_df, item_df)
    if df.empty:
        return f"스킵 (데이터 없음 | {date_str})"
    saved = _save_unified_daily(df, date_str, overwrite=overwrite)
    _flush_unmatched_alert()
    result = f"unified_sales(okpos) 저장 | {date_str} | {saved}행"
    logger.info(result)
    return result


def run_lookback_okpos(days: int = 7) -> str:
    """최근 N일 okpos 누락분 보충 (append-only, 월별 캐시).

    같은 달 날짜를 반복 처리해도 월간 CSV는 1회만 로드.
    """
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
            order_df, item_df = _load_okpos_by_date(date_str)
        except FileNotFoundError:
            logger.info("okpos CSV 없음, 스킵: %s", date_str)
            continue
        df = _transform_okpos_df(order_df, item_df)
        if df.empty:
            continue
        saved = _save_unified_daily(df, date_str)
        total_saved += saved
        if saved:
            logger.info("lookback okpos %s | 신규 %d행", date_str, saved)
    _flush_unmatched_alert()
    result = f"unified_sales(okpos) lookback({days}일) | 신규 {total_saved}행"
    logger.info(result)
    return result


def backfill_okpos() -> str:
    """전체 OKPOS order CSV → unified_sales 일별 일괄 변환."""
    date_set: set[str] = set()
    for order_path in sorted(OKPOS_BRAND_ROOT.glob("store=*/ym=*/okpos_order.csv")):
        try:
            df = pd.read_csv(order_path, dtype=str, usecols=["sale_date"])
            date_set.update(df["sale_date"].str.strip().dropna().unique())
        except Exception as e:
            logger.warning("날짜 스캔 실패: %s | %s", order_path, e)

    total = 0
    for date_str in sorted(date_set):
        if not date_str or date_str.lower() == "nan":
            continue
        try:
            order_df, item_df = _load_okpos_by_date(date_str)
            df = _transform_okpos_df(order_df, item_df)
            if not df.empty:
                _save_unified_daily(df, date_str)
                total += 1
        except Exception as e:
            logger.warning("backfill 실패: %s | %s", date_str, e)

    _flush_unmatched_alert()
    result = f"unified_sales(okpos) backfill 완료 | {total}일"
    logger.info(result)
    return result
