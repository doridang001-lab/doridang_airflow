"""수동 xlsx 직접 적재 스크립트 (Airflow 없이 실행)"""
import sys
sys.path.insert(0, r"C:\airflow")

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

from modules.transform.pipelines.db.DB_OKPOS_Sales import ingest_manual_daily_xlsx

result = ingest_manual_daily_xlsx(
    manual_xlsx_path=r"E:\down\일자별 종합매출_수동.xlsx",
    store_name="",  # 전체 매장 적재
)
print("\n=== 결과 ===")
print(result)
