import pandas as pd
from pathlib import Path

home = Path.home()
base = home / "OneDrive - 주식회사 도리당" / "data" / "analytics" / "okpos_sales_raw" / "brand=도리당" / "store=송파삼전점"

dates = ["2026-04-23", "2026-04-29", "2026-05-04"]

for sale_date in dates:
    ym = sale_date[:7]
    d = base / f"ym={ym}"
    print(f"\n=== {sale_date} ===")
    for fname, col in [("okpos_order.csv", "실매출액"), ("okpos_order_item.csv", "실매출액")]:
        csv = d / fname
        if not csv.exists():
            continue
        df = pd.read_csv(csv, dtype=str)
        df2 = df[df["sale_date"].astype(str).str.strip() == sale_date].copy()
        amt = pd.to_numeric(df2[col].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0).astype(int)

        gbn_col = "구분" if "구분" in df2.columns else None
        if gbn_col:
            groups = df2[gbn_col].fillna("").astype(str).str.strip().value_counts().to_dict()
            # 반품 행 합계
            refund_mask = df2[gbn_col].fillna("").astype(str).str.strip() == "반품"
            refund_amt = amt[refund_mask].sum()
            sale_amt = amt[~refund_mask].sum()
            print(f"  {fname}: 구분분포={groups} | 판매합={sale_amt:,} | 반품합={refund_amt:,} | 합계(반품음수)={sale_amt+refund_amt:,} | 합계(반품양수)={sale_amt-refund_amt:,}")
        else:
            print(f"  {fname}: 구분 컬럼 없음, 합계={int(amt.sum()):,}")

        # 실매출액 컬럼에 음수 있는지
        neg = (amt < 0).sum()
        print(f"    음수 행수={neg}, 최소값={int(amt.min())}, 최대값={int(amt.max())}")
