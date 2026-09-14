import _base  # noqa: F401  sys.path 설정
import pandas as pd
from modules.transform.utility.paths import (
    TOORDER_REVIEW_ANALYTICS_DIR,
    UNIFIED_REVIEW_MART_DIR,
)

print("ANALYTICS DIR:", TOORDER_REVIEW_ANALYTICS_DIR)
print("MART DIR:", UNIFIED_REVIEW_MART_DIR)

# 1) 원천 parquet 확인
src = TOORDER_REVIEW_ANALYTICS_DIR / "toorder_voc_202607.parquet"
print("\n=== SOURCE:", src, "exists?", src.exists())
if src.exists():
    df = pd.read_parquet(src)
    print("행수:", len(df), "컬럼:", list(df.columns))
    # 기흥 매장 필터
    m = df["매장명"].astype(str).str.contains("기흥", na=False)
    k = df[m]
    print("\n기흥 매장 행수:", len(k))
    if "작성일자" in df.columns:
        print("기흥 작성일자 목록:", sorted(k["작성일자"].astype(str).unique())[-10:])
    # 국물이 달아요 검색
    if "리뷰내용" in df.columns:
        hit = df[df["리뷰내용"].astype(str).str.contains("국물이 달", na=False)]
        print("\n'국물이 달' 리뷰 검색 행수:", len(hit))
        for _, r in hit.iterrows():
            print("  ->", dict((c, r.get(c)) for c in ["작성일자","매장명","채널","작성자","토픽","감정수준"] if c in df.columns))
    # 7/18 전체
    if "작성일자" in df.columns:
        d18 = df[df["작성일자"].astype(str).str.contains("2026-07-18|20260718|07-18", na=False)]
        print("\n7/18 전체 행수:", len(d18))
        print("7/18 매장 목록:", sorted(d18["매장명"].astype(str).unique()))

# 2) 마트 확인
print("\n=== MART FILES ===")
for f in sorted(UNIFIED_REVIEW_MART_DIR.glob("unified_review_2607*.parquet")):
    dfm = pd.read_parquet(f)
    print(f.name, "행수:", len(dfm), "컬럼:", list(dfm.columns))
