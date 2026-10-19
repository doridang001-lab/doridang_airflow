"""월별 CSV를 대화 서비스가 기존 조회 계약으로 읽는지 확인한다."""
import json
from pathlib import Path

import pandas as pd
import pytest

from modules.transform.pipelines.db.samjeon_material_agent.monthly import (
    TABLE_FILES, load_monthly, publish_months, stage_months,
)
from modules.transform.pipelines.db.samjeon_material_service.config import Settings
from modules.transform.pipelines.db.samjeon_material_service.repository import Repository
from modules.transform.pipelines.db.samjeon_material_service.analysis_tools import AnalysisTools
from test_samjeon_material_mart import _fixture
from test_samjeon_material_mart_v3 import build


def test_service_reads_monthly_csv_and_rejects_corruption(tmp_path):
    plan, _ = _fixture(tmp_path)
    stage = build(plan)
    tables = {name: pd.read_parquet(stage / f"{name}.parquet") for name in TABLE_FILES if name in {
        "sales_lines", "purchase_lines", "sales_daily", "purchase_daily", "item_daily", "quality_daily"}}
    for name in ("review_candidates", "coverage_candidates"):
        tables[name] = pd.read_csv(stage / f"{name}.csv", encoding="utf-8-sig", keep_default_na=False)
    context = json.loads((stage / "agent_context.json").read_text(encoding="utf-8"))
    monthly_plan = {**plan, "start_date": "2026-10-01", "summary_only_enabled": True,
                    "output_root": str(tmp_path / "monthly"),
                    "publication_index_path": str(tmp_path / "publication_index.json")}
    months = stage_months(monthly_plan, stage, tables, context)
    assert months == ["2026_10"]
    publish_months(monthly_plan, stage, months)
    root = Path(monthly_plan["output_root"])
    repo = Repository(Settings(snapshot_root=str(root)))
    path, loaded = repo.current()
    assert loaded["format"] == "monthly_csv_v3"
    assert repo.query(path, "summary", "2026-10-11", "2026-10-11")["result"]["observed_sales_won"] == 110000
    assert repo.query(path, "evidence", "2026-10-11", "2026-10-11")["unsupported"]
    assert repo.search_items(path, "쌀") == []
    tool = AnalysisTools(Settings(snapshot_root=str(root), state_root=str(tmp_path / "state")), repo)
    catalog = tool.call("data_catalog", {})
    assert list(catalog["tables"]) == ["daily_summary"]
    assert tool.call("query_data", {"sql": 'SELECT "관측매출원" FROM daily_summary WHERE "날짜" = ?',
                                    "parameters": ["2026-10-11"]})["rows"] == [[110000]]
    assert tool.call("query_data", {"sql": "SELECT count(*) FROM purchase_lines"})["error"] == "unknown_table"
    assert tool.call("get_evidence", {"evidence_ids": ["x"]})["error"] == "unsupported_detail"
    target = root / "2026_10" / "2026_10.csv"
    target.write_bytes(b"broken\n")
    with pytest.raises(ValueError, match="요약"):
        load_monthly(root)
