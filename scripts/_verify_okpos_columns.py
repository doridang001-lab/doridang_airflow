"""실제 컬럼명 확인"""
import sys
sys.path.insert(0, r"C:\airflow")
import pandas as pd

xlsx_path = r"E:\down\일자별 종합매출_수동.xlsx"
header_candidates = {"NO","포스번호","영수증번호","구분","테이블명","최초주문","결제시간","상품코드","바코드","상품명","수량","총매출액","할인액","할인구분","실매출액","가액","부가세"}
preview = pd.read_excel(xlsx_path, header=None, nrows=12, dtype=str, engine="openpyxl")
best_score, header_row = -1, 0
for i in range(len(preview)):
    row_vals = [str(v).strip() for v in preview.iloc[i].tolist() if pd.notna(v)]
    score = sum(1 for v in row_vals if v in header_candidates)
    if score > best_score:
        best_score, header_row = score, i

raw = pd.read_excel(xlsx_path, header=header_row, dtype=str, engine="openpyxl")
raw.columns = [str(c).strip() for c in raw.columns]

print("=== 전체 컬럼 목록 ===")
for i, c in enumerate(raw.columns):
    print(f"  [{i:02d}] {c!r}")

# 첫 번째 실제 데이터 행
dt = pd.to_datetime(raw.iloc[:, 0].astype(str).str.strip(), errors="coerce")
first_data = raw[dt.notna()].iloc[0]
print("\n=== 첫 데이터 행 (재무 컬럼 위주) ===")
for i, (col, val) in enumerate(first_data.items()):
    if i >= 15: break
    print(f"  [{i:02d}] {col!r}: {val!r}")

# 기존 코드 _num 체크
def _num(v) -> int:
    return int(pd.to_numeric(str(v).replace(",", "").strip(), errors="coerce") or 0)

def _get_val(r, *keys):
    for k in keys:
        v = r.get(k)
        if v is not None and str(v).strip() not in ("", "nan", "None"):
            return v
    return 0

r = first_data
print("\n=== _num 결과 ===")
print(f"  총매출: r.get('총매출')={r.get('총매출')!r} => _num={_num(r.get('총매출', 0))}")
print(f"  합매출: r.get('합매출')={r.get('합매출')!r} => _num={_num(r.get('합매출', 0))}")
print(f"  총할인: r.get('총할인')={r.get('총할인')!r} => _num={_num(r.get('총할인', 0))}")
print(f"  할인액: r.get('할인액')={r.get('할인액')!r} => _num={_num(r.get('할인액', 0))}")
print(f"  실매출: r.get('실매출')={r.get('실매출')!r} => _num={_num(r.get('실매출', 0))}")
print(f"  영수건수: r.get('영수건수')={r.get('영수건수')!r} => _num={_num(r.get('영수건수', 0))}")
print(f"  객수(수): r.get('객수(수)')={r.get('객수(수)')!r} => _num={_num(r.get('객수(수)', 0))}")
