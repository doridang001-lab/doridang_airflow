"""동탄영천점 unionpos merge 폭증(이중집계) 검증 (read-only)."""

import pandas as pd
from modules.transform.utility.paths import RAW_UNIONPOS_SALES

YM = "2026-06"
STORE = "동탄영천점"
base = RAW_UNIONPOS_SALES / "brand=도리당" / f"store={STORE}" / f"ym={YM}"
list_p = base / "unionpos_receipt_list.csv"
item_p = base / "unionpos_receipt_items.csv"


def norm(s):
    return s.fillna("").astype(str).str.strip()


def main():
    order = pd.read_csv(list_p, dtype=str, encoding="utf-8-sig").fillna("")
    item = pd.read_csv(item_p, dtype=str, encoding="utf-8-sig").fillna("")
    print(f"raw receipt_list 행: {len(order)} | receipt_items 행: {len(item)}")

    for c in ("영수증번호", "판매일시", "포스번호"):
        if c in order.columns:
            order[c] = norm(order[c])
    for c in ("영수증번호", "판매일시", "합계", "판매타입"):
        if c in item.columns:
            item[c] = norm(item[c])

    # raw item 합계 (판매/반품 라인) — 정답 매출 근사
    amt = pd.to_numeric(item["합계"].str.replace(",", "", regex=False), errors="coerce").fillna(0)
    keep = item["판매타입"].isin(["", "판매", "판매(반)", "반품"]) if "판매타입" in item.columns else pd.Series(True, index=item.index)
    print(f"raw item 합계 합(판매라인): {amt[keep].sum():,.0f}")

    # 헤더 (영수증번호,판매일시) 중복도
    dup = order.groupby(["영수증번호", "판매일시"]).size()
    print(f"\n헤더 (영수증번호,판매일시) 고유: {len(dup)} | 중복키(>1): {(dup>1).sum()}건")
    if (dup > 1).any():
        print("중복 헤더 예시(같은 영수증번호+판매일시 여러 행):")
        print(dup[dup > 1].head(10).to_string())
        # 포스번호가 다른가?
        multi = order.groupby(["영수증번호", "판매일시"])["포스번호"].nunique()
        print(f"  그 중 포스번호가 2개 이상인 키: {(multi>1).sum()}건")

    # 실제 머지(현재 로직: 포스번호 제외) 행수
    order["_sp"] = STORE
    item["_sp"] = STORE
    merged = pd.merge(order, item, on=["_sp", "영수증번호", "판매일시"], how="left", suffixes=("_o", "_i"))
    col_amt = "합계_i" if "합계_i" in merged.columns else "합계"
    m_amt = pd.to_numeric(merged[col_amt].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0)
    keep_m = merged.get("판매타입_i", merged.get("판매타입", "")).isin(["", "판매", "판매(반)", "반품"])
    print(f"\n현재 머지(포스번호 제외) 행: {len(merged)} | 합계 합: {m_amt[keep_m].sum():,.0f}")

    # 포스번호 포함 머지(정상)
    if "포스번호" in item.columns:
        item["포스번호"] = norm(item["포스번호"])
        merged2 = pd.merge(order, item, on=["_sp", "영수증번호", "판매일시", "포스번호"], how="left", suffixes=("_o", "_i"))
        col2 = "합계_i" if "합계_i" in merged2.columns else "합계"
        m2 = pd.to_numeric(merged2[col2].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0)
        print(f"포스번호 포함 머지 행: {len(merged2)} | 합계 합: {m2.sum():,.0f}")
    else:
        print("item에 포스번호 컬럼 없음 → 포스 단위 분리 불가(헤더로만 매핑해야)")
        print("item 컬럼:", list(item.columns))


if __name__ == "__main__":
    main()
