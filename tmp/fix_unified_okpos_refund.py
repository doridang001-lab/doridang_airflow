"""
04-23, 04-29, 05-04 송파삼전점 unified_sales 재처리 (overwrite)
반품 double-flip 버그 수정 후 실행
"""
import sys, os
sys.path.insert(0, r"C:\airflow")
os.chdir(r"C:\airflow")

import importlib
import modules.transform.pipelines.db.DB_UnifiedSales_okpos as mod
importlib.reload(mod)

from modules.transform.pipelines.db.DB_UnifiedSales_okpos import run_okpos

dates = ["2026-04-23", "2026-04-29", "2026-05-04"]

for d in dates:
    print(f"\n>>> {d} 재처리 중...")
    result = run_okpos(d, overwrite=True)
    print(f"    결과: {result}")

print("\n✅ 완료")
