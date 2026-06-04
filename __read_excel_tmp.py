import sys
sys.path.insert(0, "C:/airflow")
from pathlib import Path
from modules.transform.pipelines.db.DB_OKPOS_Card_Test import _parse_okpos_card_workbook

result = _parse_okpos_card_workbook(Path(r"e:\d_down\매장현황.xlsx"), "2026-04-17")
with open(r"c:\airflow\__excel_out.txt", "w", encoding="utf-8") as f:
    f.write(f"shape: {result.shape}\n")
    f.write(f"columns: {result.columns.tolist()}\n")
    f.write(result.head(5).to_string())
