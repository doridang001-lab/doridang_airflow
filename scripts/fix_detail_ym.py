"""잘못된 ym 파티션에 저장된 detail 행을 올바른 ym으로 재배치."""
import sys
sys.path.insert(0, r"C:\airflow")

import pandas as pd
from pathlib import Path
from modules.transform.utility.paths import ANALYTICS_DB

sales_root = ANALYTICS_DB / "posfeed_sales"
detail_root = ANALYTICS_DB / "posfeed_sales_detail"

print("posfeed_sales 로드 중...")
code_to_date: dict[str, str] = {}
for csv_path in sales_root.rglob("posfeed_orders.csv"):
    try:
        df = pd.read_csv(csv_path, dtype=str, usecols=["주문 코드", "등록날짜"])
        for _, row in df.iterrows():
            code = str(row["주문 코드"]).strip()
            date = str(row["등록날짜"]).strip()
            if code and date and date != "nan":
                code_to_date[code] = date
    except Exception as e:
        print(f"  WARN: {csv_path.name} — {e}")

print(f"매핑 완료: {len(code_to_date)}건")

moved_total = 0
for detail_csv in sorted(detail_root.rglob("ym=2026-04/posfeed_order_item.csv")):
    store_dir = detail_csv.parent.parent

    try:
        df = pd.read_csv(detail_csv, dtype=str)
    except Exception as e:
        print(f"  WARN: {detail_csv} — {e}")
        continue

    if "주문코드" not in df.columns:
        continue

    def get_ym(code: str) -> str:
        date = code_to_date.get(str(code).strip(), "")
        return date[:7] if date else ""

    df["_correct_ym"] = df["주문코드"].map(get_ym)

    # 매핑 없으면 collected_at fallback
    mask_no_map = df["_correct_ym"] == ""
    if mask_no_map.any() and "collected_at" in df.columns:
        df.loc[mask_no_map, "_correct_ym"] = pd.to_datetime(
            df.loc[mask_no_map, "collected_at"], errors="coerce"
        ).dt.strftime("%Y-%m").fillna("2026-04")

    wrong = df[df["_correct_ym"] != "2026-04"].copy()
    correct = df[df["_correct_ym"] == "2026-04"].copy()

    if wrong.empty:
        continue

    store = store_dir.name[len("store="):]
    print(f"\n{store}: 잘못된 행 {len(wrong)}건")

    for ym, grp in wrong.groupby("_correct_ym"):
        target_path = store_dir / f"ym={ym}" / "posfeed_order_item.csv"
        target_path.parent.mkdir(parents=True, exist_ok=True)
        grp = grp.drop(columns=["_correct_ym"])
        if target_path.exists():
            existing = pd.read_csv(target_path, dtype=str)
            merged = pd.concat([existing, grp], ignore_index=True).drop_duplicates(subset=["_pk"])
            merged.to_csv(target_path, index=False, encoding="utf-8-sig")
            print(f"  → ym={ym}: 기존 {len(existing)}+추가 {len(grp)} → {len(merged)}행")
        else:
            grp.to_csv(target_path, index=False, encoding="utf-8-sig")
            print(f"  → ym={ym}: 신규 {len(grp)}행")
        moved_total += len(grp)

    correct = correct.drop(columns=["_correct_ym"])
    if correct.empty:
        detail_csv.unlink()
        print(f"  ym=2026-04 파티션 삭제 (April 데이터 없음)")
    else:
        correct.to_csv(detail_csv, index=False, encoding="utf-8-sig")
        print(f"  ym=2026-04 유지: {len(correct)}행")

print(f"\n완료: 총 {moved_total}행 재배치")
