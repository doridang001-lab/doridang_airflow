import pandas as pd
from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH

df = pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str).fillna("")
df = df[df["is_latest"].str.upper() == "Y"]
df = df[df["수동분류"].str.strip() != ""]

print(f"is_latest=Y + 수동분류 있는 행: {len(df)}")
print(f"source 종류: {df['source'].unique().tolist()}")

# source+상품코드 기준 수동분류 종류 수
conflict = (
    df.groupby(["source", "상품코드"])["수동분류"]
    .nunique()
    .reset_index(name="분류종류수")
)
print("\n== source+상품코드 기준 수동분류 종류 수 분포 ==")
print(conflict["분류종류수"].value_counts().sort_index())

dup = conflict[conflict["분류종류수"] > 1]
if not dup.empty:
    print(f"\n== 충돌 {len(dup)}건 ==")
    for _, r in dup.iterrows():
        labels = df[(df["source"] == r["source"]) & (df["상품코드"] == r["상품코드"])]["수동분류"].unique()
        print(f"  {r['source']} {r['상품코드']} -> {list(labels)}")
else:
    print("\n충돌 없음")

# 동일 source+상품코드에 행이 여러 개인 경우 (분류는 같은데 행 중복)
row_cnt = df.groupby(["source", "상품코드"]).size().reset_index(name="행수")
print("\n== 동일 source+상품코드 행수 분포 ==")
print(row_cnt["행수"].value_counts().sort_index())
multi = row_cnt[row_cnt["행수"] > 1]
if not multi.empty:
    print(f"\n행 중복 {len(multi)}건 예시:")
    for _, r in multi.head(5).iterrows():
        subset = df[(df["source"] == r["source"]) & (df["상품코드"] == r["상품코드"])][["source","상품코드","상품명","수동분류","대메뉴"]]
        print(subset.to_string(index=False))
