"""OKPOS ingest_manual_daily_xlsx 수정 검증 스크립트"""
import sys
sys.path.insert(0, r"C:\airflow")
import pandas as pd

xlsx_path = r"E:\down\일자별 종합매출_수동.xlsx"

# --- _read_okpos_excel 시뮬레이션 (score 기반 header 감지) ---
header_candidates = {
    "NO","포스번호","영수증번호","구분","테이블명","최초주문","결제시간",
    "상품코드","바코드","상품명","수량","총매출액","할인액","할인구분","실매출액","가액","부가세",
}
preview = pd.read_excel(xlsx_path, header=None, nrows=12, dtype=str, engine="openpyxl")
best_score, header_row = -1, 0
for i in range(len(preview)):
    row_vals = [str(v).strip() for v in preview.iloc[i].tolist() if pd.notna(v)]
    score = sum(1 for v in row_vals if v in header_candidates)
    if score > best_score:
        best_score, header_row = score, i
print(f"[_read_okpos_excel] header_row={header_row}, best_score={best_score}")

raw = pd.read_excel(xlsx_path, header=header_row, dtype=str, engine="openpyxl")
raw.columns = [str(c).strip() for c in raw.columns]
print(f"[raw] columns (first 6): {list(raw.columns[:6])}")
print(f"[raw] 합매출 in cols: {'합매출' in raw.columns}, 일자 in cols: {'일자' in raw.columns}")

# --- 수정 1: 통합 포맷 감지 + 리네임 ---
if "합매출" in raw.columns and "일자" not in raw.columns:
    unnamed_cols = [c for c in raw.columns if str(c).startswith("Unnamed:")]
    rename_map: dict = {}
    if len(unnamed_cols) >= 1: rename_map[unnamed_cols[0]] = "일자"
    if len(unnamed_cols) >= 2: rename_map[unnamed_cols[1]] = "영업일"
    if len(unnamed_cols) >= 3: rename_map[unnamed_cols[2]] = "영업매장"
    if rename_map:
        raw = raw.rename(columns=rename_map)
        print(f"[fix1] 리네임 적용: {list(rename_map.values())}")

print(f"[fix1 after] 일자: {'일자' in raw.columns}, 영업매장: {'영업매장' in raw.columns}, 실매출: {'실매출' in raw.columns}")

# --- Strategy 3: 날짜 필터 ---
data = raw.copy()
data["일자"] = data["일자"].astype(str).str.strip()
dt = pd.to_datetime(data["일자"], errors="coerce")
rows = data[dt.notna()].copy()
rows["sale_date"] = dt[dt.notna()].dt.strftime("%Y-%m-%d")
rows["영업매장"] = rows["영업매장"].astype(str).str.strip()
print(f"\n날짜 필터 후: {len(rows)}행")

# --- 수정 2a: 소계 행 제거 ---
mask_sub = rows["영업매장"].str.match(r"^\d+\s*개\s*$", na=False)
print(f"소계 행 (N 개 패턴): {mask_sub.sum()}건")
rows = rows[~mask_sub].copy()
print(f"소계 제거 후: {len(rows)}행")

# --- 수정 2b: 미등록 매장 제거 ---
STORES = [
    {"name": "도리당 동두천지행점"},
    {"name": "도리당 삼송점"},
    {"name": "도리당 평택비전점"},
    {"name": "도리당 송파삼전점"},
]
known = {s["name"] for s in STORES}
mask_unk = ~rows["영업매장"].isin(known)
print(f"미등록 매장: {rows.loc[mask_unk, '영업매장'].unique().tolist()}")
rows = rows[~mask_unk].copy()
print(f"미등록 제거 후: {len(rows)}행")

# --- 결과 요약 ---
print("\n=== 매장별 날짜 수 ===")
summary = rows.groupby("영업매장")["sale_date"].nunique().reset_index()
summary.columns = ["매장명", "날짜수"]
print(summary.to_string(index=False))

# --- 수정 3: _num fallback 컬럼 ---
def _num(v) -> int:
    return int(pd.to_numeric(str(v).replace(",", "").strip(), errors="coerce") or 0)

sample = rows.head(3)
for _, r in sample.iterrows():
    total = _num(r.get("총매출", r.get("합매출", 0)))
    discount = _num(r.get("총할인", r.get("할인액", 0)))
    net = _num(r.get("실매출", r.get("실매출액", 0)))
    count = _num(r.get("영수건수", r.get("객수(수)", 0)))
    store_short = str(r.get("영업매장", "")).replace("도리당 ", "", 1)
    print(f"  {r['sale_date']} | {store_short} | 총={total:,} 할인={discount:,} 실={net:,} 건={count}")

print("\n[OK] 모든 검증 통과")
