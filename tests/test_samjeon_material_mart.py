"""송파삼전점 mart의 분류, 대사, 게시 경계를 검증한다."""

import json
import shutil
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from modules.transform.pipelines.db.samjeon_material_agent.mart import (
    FOOD_MAIN, FOOD_MEAL, STORE, _forecast, plan_build, publish_snapshot,
    stage_aggregate, stage_purchases, stage_sales, validate_snapshot,
)


def _fixture(tmp_path: Path):
    sales = tmp_path / "sales"
    food = tmp_path / "food"
    dam = tmp_path / "dam"
    for directory in (sales, food, dam):
        directory.mkdir()
    pd.DataFrame([
        {"sale_date": "2026-10-11", "store": STORE, "source": "okpos", "total_price": 100000, "_pk": "1"},
        {"sale_date": "2026-10-11", "store": STORE, "source": "okpos", "total_price": -10000, "_pk": "2"},
        {"sale_date": "2026-10-11", "store": STORE, "source": "posfeed", "total_price": 20000, "_pk": "4"},
        {"sale_date": "2026-10-11", "store": "다른점", "source": "okpos", "total_price": 999999, "_pk": "3"},
    ]).to_parquet(sales / "unified_sales_261011.parquet")
    pd.DataFrame([
        {"배송일": "2026-10-11", "사업장": FOOD_MAIN, "상품코드": "A", "상품명": "쌀",
         "규격": "1kg", "단위": "봉", "수량": 2, "합계": 30000},
        {"배송일": "2026-10-11", "사업장": FOOD_MEAL, "상품코드": "A", "상품명": "쌀",
         "규격": "1kg", "단위": "봉", "수량": 1, "합계": 5000},
        {"배송일": "2026-10-12", "사업장": FOOD_MAIN, "상품코드": "B", "상품명": "미래",
         "규격": "1kg", "단위": "봉", "수량": 1, "합계": 9000},
    ]).to_parquet(food / "food_guide_orders_202610.parquet")
    class_path = tmp_path / "classification.json"
    class_path.write_text(json.dumps({"version": "reviewed-1", "items": [{
        "source": "food_guide", "product_code": "A", "item_name": "쌀",
        "spec": "1kg", "unit": "봉", "category": "원재료",
    }]}, ensure_ascii=False), encoding="utf-8")
    plan = plan_build("2026-10-11", sales_root=sales, food_root=food, dam_root=dam,
                      classification_path=class_path, coverage_path=tmp_path / "coverage.json",
                      work_root=tmp_path / "work", output_root=tmp_path / "output")
    return plan, class_path


def test_staff_meal_and_reconciliation(tmp_path):
    plan, _ = _fixture(tmp_path)
    stage_sales(plan)
    stage_purchases(plan)
    stage_aggregate(plan)
    assert validate_snapshot(plan)["purchase_rows"] == 2
    stage = Path(plan["stage_dir"])
    context = json.loads((stage / "agent_context.json").read_text(encoding="utf-8"))
    lines = pd.read_parquet(stage / "purchase_lines.parquet")
    assert dict(zip(lines["account"], lines["category"])) == {
        FOOD_MAIN: "원재료", FOOD_MEAL: "현장 직원식대"}
    assert context["daily"]["observed_sales_won"] == 110000
    assert context["daily"]["hall_sales_won"] == 90000
    assert context["month_to_date"]["hall_sales_won"] == 90000
    assert context["daily"]["observed_purchase_won"] == 30000
    assert context["daily"]["staff_meal_won"] == 5000
    assert context["daily"]["purchase_rate_pct"] is None
    assert context["forecast"]["status"] == "보류"
    manifest = json.loads((stage / "manifest.json").read_text(encoding="utf-8"))
    assert (stage / "agent_guide.md").is_file()
    assert context["guide"] == {"file": "agent_guide.md",
                                "sha256": manifest["files"]["agent_guide.md"]}
    assert publish_snapshot(plan)["latest_updated"] in {True, False}
    assert publish_snapshot(plan)["purchase_rows"] == 2
    assert (Path(plan["output_root"]) / "snapshots" / plan["snapshot_id"] / "agent_guide.md").is_file()


def test_source_change_blocks_publication(tmp_path):
    plan, _ = _fixture(tmp_path)
    stage_sales(plan)
    stage_purchases(plan)
    stage_aggregate(plan)
    sales_path = Path(plan["sales_files"][-1]["path"])
    pd.DataFrame([{"sale_date": "2026-10-11", "store": STORE, "source": "okpos",
                   "total_price": 1, "_pk": "changed"}]).to_parquet(sales_path)
    with pytest.raises(RuntimeError, match="판본 변경"):
        publish_snapshot(plan)
    assert not (Path(plan["output_root"]) / "latest.json").exists()


def test_conflicting_sales_key_fails(tmp_path):
    plan, _ = _fixture(tmp_path)
    path = Path(plan["sales_files"][-1]["path"])
    frame = pd.read_parquet(path)
    frame.loc[1, "_pk"] = "1"
    frame.to_parquet(path)
    plan = plan_build("2026-10-11", sales_root=path.parent,
                      food_root=Path(plan["food_files"][-1]["path"]).parent,
                      dam_root=tmp_path / "dam", work_root=tmp_path / "work",
                      output_root=tmp_path / "output")
    with pytest.raises(ValueError, match="상충"):
        stage_sales(plan)


def test_month_end_forecast_only_with_closed_samples():
    start = date(2026, 8, 16)
    days = [(start + timedelta(days=i)).isoformat() for i in range(57)]
    sales = pd.DataFrame({"sale_date": days, "amount_won": [1000] * 57})
    purchases = pd.DataFrame({"purchase_date": days, "category": ["원재료"] * 57,
                              "amount_won": [370] * 57})
    quality = pd.DataFrame([{"date": day, "source": source, "status": "정상"}
                            for day in days for source in ("sales", "food_guide", "logistics_dam")])
    result = _forecast({"as_of_date": "2026-10-11"}, sales, purchases, quality, True)
    assert result["status"] == "정상"
    assert result["projected_month_sales_won"] == 31000
    assert result["projected_purchase_rate_pct"] == "37.0"
    quality.loc[(quality["date"] == "2026-10-10") & quality["source"].eq("food_guide"), "status"] = "잠정"
    assert _forecast({"as_of_date": "2026-10-11"}, sales, purchases, quality, False)["status"] == "보류"


def test_guide_revision_changes_snapshot_and_blocks_stale_publish(tmp_path):
    original, _ = _fixture(tmp_path)
    guide = tmp_path / "agent_guide.md"
    shutil.copy2(original["guide_path"], guide)
    paths = {"sales_root": Path(original["sales_files"][-1]["path"]).parent,
             "food_root": Path(original["food_files"][-1]["path"]).parent,
             "dam_root": tmp_path / "dam", "guide_path": guide,
             "work_root": tmp_path / "work", "output_root": tmp_path / "output"}
    first = plan_build("2026-10-11", **paths)
    stage_sales(first)
    stage_purchases(first)
    stage_aggregate(first)
    guide.write_text(guide.read_text(encoding="utf-8") + "\n판본 검증용 변경\n", encoding="utf-8")
    second = plan_build("2026-10-11", **paths)
    assert first["snapshot_id"] != second["snapshot_id"]
    with pytest.raises(RuntimeError, match="가이드 판본 변경"):
        publish_snapshot(first)
    assert not (Path(first["output_root"]) / "latest.json").exists()
