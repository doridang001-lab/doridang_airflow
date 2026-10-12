"""PRD 계산, 마감·분류 검수, 근거 조회 및 게시 회귀 시험."""
import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from modules.transform.pipelines.db.samjeon_material_agent import mart
from modules.transform.pipelines.db.samjeon_material_agent.analytics import build_context, forecast, period_result
from modules.transform.pipelines.db.samjeon_material_agent.classification import import_review, review_candidates
from modules.transform.pipelines.db.samjeon_material_agent.contracts import (
    FOOD_MAIN, FOOD_MEAL, SOURCE_NAMES, atomic_json, digest, json_bytes, percent, validate_context,
)
from modules.transform.pipelines.db.samjeon_material_agent.quality import coverage_candidates, import_coverage, quality_daily
from modules.transform.pipelines.db.samjeon_material_agent.query import query_snapshot
from modules.transform.pipelines.db.samjeon_material_agent.settings import validate_settings
from modules.transform.pipelines.db.samjeon_material_agent.sources import load_sales_lines, resolve_source_ref
from test_samjeon_material_mart import _fixture


def replan(plan, **overrides):
    paths = {"sales_root": Path(plan["sales_files"][-1]["path"]).parent,
             "food_root": Path(plan["food_files"][-1]["path"]).parent,
             "dam_root": Path(plan["dam_files"][-1]["path"]).parent,
             "work_root": Path(plan["stage_dir"]).parent.parent,
             "output_root": plan["output_root"],
             **{f"{key}_path": plan[f"{key}_path"] for key in ("classification", "coverage", "settings", "recommendation", "guide")}}
    paths.update(overrides)
    if "date_from" in paths or "date_to" in paths:
        return mart.plan_build(**paths)
    return mart.plan_build(plan["as_of_date"], **paths)


def build(plan):
    mart.stage_sales(plan)
    mart.stage_purchases(plan)
    mart.stage_aggregate(plan)
    mart.validate_snapshot(plan)
    return Path(plan["stage_dir"])


def analytical_tables(end="2026-10-11", sales_won=1000, purchase_won=370):
    target = date.fromisoformat(end)
    days = [str(target - timedelta(days=i)) for i in reversed(range(57))]
    sales = pd.DataFrame({"sale_date": days, "source": ["okpos"] * 57, "amount_won": [sales_won] * 57, "row_count": [1] * 57})
    purchase = pd.DataFrame({"purchase_date": days, "source": ["food_guide"] * 57, "account": [FOOD_MAIN] * 57,
                             "category": ["원재료"] * 57, "amount_won": [purchase_won] * 57, "row_count": [1] * 57})
    quality = pd.DataFrame([{"date": day, "source": source, "status": "정상", "reason": "합성 마감"}
                            for day in days for source in SOURCE_NAMES])
    tables = {"sales_daily": sales, "purchase_daily": purchase, "quality_daily": quality, "item_daily": pd.DataFrame()}
    plan = {"as_of_date": end, "start_date": days[0], "query_start": end[:7] + "-01", "snapshot_id": "test", "guide_sha256": "test"}
    return plan, tables


def test_json_types_are_preserved_and_unknown_types_rejected():
    result = json.loads(json_bytes({"complete": np.bool_(False), "amount_won": np.int64(10)}))
    assert result == {"complete": False, "amount_won": 10}
    with pytest.raises(TypeError):
        json_bytes({"unsupported": object()})
    with pytest.raises(ValueError):
        json_bytes({"amount": float("nan")})
    with pytest.raises(ValueError, match="boolean"):
        validate_context({"schema_version": 3, "complete": "False"})
    assert percent(Decimal("37.05")) == "37.1"


def test_prd_reference_calculation_and_effective_target():
    plan, tables = analytical_tables(sales_won=50000000, purchase_won=19600000)
    meals = tables["purchase_daily"].iloc[[-1]].copy()
    meals["category"], meals["account"], meals["amount_won"] = "현장 직원식대", FOOD_MEAL, 500000
    tables["purchase_daily"] = pd.concat([tables["purchase_daily"], meals], ignore_index=True)
    settings = validate_settings({"target_start_date": "2026-10-01", "amount_basis_confirmed": True})
    result = period_result(plan["as_of_date"], plan["as_of_date"], tables, settings)
    assert result["purchase_rate_pct"] == "39.2"
    assert result["target_gap_pp"] == "2.2"
    assert result["target_excess_purchase_won"] == 1100000
    assert result["staff_meal_won"] == 500000
    assert result["target_exceeded"] is True
    result = period_result("2026-09-01", "2026-09-30", tables, settings)
    assert result["target_comparison_status"] == "참고"
    assert result["target_exceeded"] is None


def test_target_threshold_uses_unrounded_value():
    plan, tables = analytical_tables(sales_won=1000000, purchase_won=370001)
    context = build_context(plan, tables, "test", {"target_start_date": "2026-10-01", "amount_basis_confirmed": True})
    assert context["month_to_date"]["target_gap_pp"] == "0.0"
    assert any(row["type"] == "purchase_target_exceeded" for row in context["alerts"])


@pytest.mark.parametrize("sales_won", [0, -100])
def test_nonpositive_sales_does_not_produce_rate(sales_won):
    plan, tables = analytical_tables(sales_won=sales_won)
    result = period_result(plan["as_of_date"], plan["as_of_date"], tables, validate_settings({}))
    assert result["purchase_rate_pct"] is None
    assert result["provisional_purchase_rate_pct"] is None


def test_unclassified_zero_net_remains_incomplete():
    plan, tables = analytical_tables()
    extra = pd.DataFrame([{"purchase_date": plan["as_of_date"], "source": "food_guide", "account": FOOD_MAIN,
                           "category": "미분류", "amount_won": value, "row_count": 1} for value in (100, -100)])
    tables["purchase_daily"] = pd.concat([tables["purchase_daily"], extra], ignore_index=True)
    result = period_result(plan["as_of_date"], plan["as_of_date"], tables, validate_settings({}))
    assert result["unclassified_won"] == 0
    assert result["unclassified_absolute_amount_won"] == 200
    assert result["unclassified_row_count"] == 2
    assert result["complete"] is False
    assert result["purchase_rate_pct"] is None
    assert result["provisional_purchase_rate_pct"] is None


def test_provisional_ratio_and_independent_staff_meals():
    plan, tables = analytical_tables()
    q = tables["quality_daily"]
    q.loc[q["source"].eq("sales"), "status"] = "잠정"
    result = period_result(plan["as_of_date"], plan["as_of_date"], tables, validate_settings({}))
    assert result["purchase_rate_pct"] is None
    assert result["provisional_purchase_rate_pct"] == "37.0"
    assert result["staff_meal_complete"] is True
    q.loc[q["source"].eq("sales"), "status"] = "누락"
    assert period_result(plan["as_of_date"], plan["as_of_date"], tables, validate_settings({}))["provisional_purchase_rate_pct"] is None


def test_forecast_uses_fixed_eight_week_window():
    plan, tables = analytical_tables(end="2026-10-01")
    result = forecast(plan, tables["sales_daily"], tables["purchase_daily"], tables["quality_daily"], True)
    assert result["status"] == "정상"
    assert {row["sample_count"] for row in result["days"]} == {8}
    assert result["projected_month_sales_won"] == 31000
    assert all(max(row["sales_sample_dates"]) <= plan["as_of_date"] for row in result["days"])


def test_forecast_confirmed_zero_days_and_month_end_status():
    plan, tables = analytical_tables(end="2026-10-01")
    tables["sales_daily"] = tables["sales_daily"].iloc[-1:].copy()
    tables["purchase_daily"] = tables["purchase_daily"].iloc[-1:].copy()
    result = forecast(plan, tables["sales_daily"], tables["purchase_daily"], tables["quality_daily"], True)
    assert result["status"] == "정상"
    assert {row["sample_count"] for row in result["days"]} == {8}
    plan, tables = analytical_tables(end="2026-09-30")
    assert forecast(plan, tables["sales_daily"], tables["purchase_daily"], tables["quality_daily"], False)["status"] == "보류"
    assert forecast(plan, tables["sales_daily"], tables["purchase_daily"], tables["quality_daily"], True)["status"] == "실적"


def test_forecast_excludes_incomplete_and_unclassified_samples():
    plan, tables = analytical_tables(end="2026-10-01")
    q = tables["quality_daily"]
    friday = pd.to_datetime(q["date"]).dt.weekday.eq(4)
    q.loc[friday & q["source"].eq("food_guide"), "status"] = "누락"
    result = forecast(plan, tables["sales_daily"], tables["purchase_daily"], q, True)
    assert result["status"] == "보류"
    assert result["sample_counts"]["purchase"] == 0
    assert result["sample_counts"]["sales"] == 8


def test_plan_range_isolation_and_small_reference(tmp_path):
    original, _ = _fixture(tmp_path)
    again = replan(original)
    assert original["snapshot_id"] == again["snapshot_id"]
    assert original["stage_dir"] != again["stage_dir"]
    plan = replan(original, date_from="2026-02-01", date_to="2026-10-11")
    assert plan["days"][0] == "2026-02-01"
    assert len(plan["days"]) > 57
    reference = mart.save_plan(plan)
    assert len(json_bytes(reference)) < 1000
    assert mart.read_plan(reference) == plan
    Path(reference["plan_path"]).write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="해시"):
        mart.read_plan(reference)


@pytest.mark.parametrize("kwargs", [
    {"as_of_date": "2026-10-01", "date_from": "2026-09-01", "date_to": "2026-10-01"},
    {"date_from": "2026-10-01"}, {"date_from": "2026-10-02", "date_to": "2026-10-01"},
    {"as_of_date": "2099-01-01"}, {"as_of_date": "20260201"},
])
def test_invalid_plan_dates(kwargs):
    with pytest.raises(ValueError):
        mart.plan_build(**kwargs)


def test_leap_year_window(tmp_path):
    original, _ = _fixture(tmp_path)
    plan = replan(original, date_from="2024-02-01", date_to="2024-03-01")
    assert "2024-02-29" in plan["days"]
    assert len(plan["days"]) == 57


def test_identical_sales_duplicate_kept_once_with_physical_evidence(tmp_path):
    plan, _ = _fixture(tmp_path)
    path = Path(plan["sales_files"][-1]["path"])
    frame = pd.read_parquet(path)
    pd.concat([frame, frame.iloc[[0]]], ignore_index=True).to_parquet(path)
    plan = replan(plan)
    lines, counts = load_sales_lines(plan)
    assert len(lines) == 3
    assert int(lines["amount_won"].sum()) == 110000
    assert lines["source_row"].tolist() == [0, 1, 2]
    assert all(value.startswith("sales/") for value in lines["source_ref"])


def test_review_suggestions_require_explicit_approval(tmp_path):
    plan, classification = _fixture(tmp_path)
    classification.unlink()
    food = Path(plan["food_files"][-1]["path"])
    frame = pd.read_parquet(food)
    frame["대분류"] = "양곡"
    frame.to_parquet(food)
    plan = replan(plan)
    stage = build(plan)
    review = pd.read_csv(stage / "review_candidates.csv", encoding="utf-8-sig", dtype=str).fillna("")
    assert len(review) == 1
    assert review.iloc[0]["suggested_category"] == "원재료"
    assert review.iloc[0]["approved"] == ""
    assert import_review(stage / "review_candidates.csv", classification, version="r1", expected_hash="")["updated_items"] == 0
    assert not classification.exists()
    review["approved"], review["approved_category"] = "Y", "원재료"
    review_path = tmp_path / "review.csv"
    review.to_csv(review_path, index=False, encoding="utf-8-sig")
    assert import_review(review_path, classification, version="r1", expected_hash="")["updated_items"] == 1
    stage = build(replan(plan))
    lines = pd.read_parquet(stage / "purchase_lines.parquet")
    assert dict(zip(lines["account"], lines["category"])) == {FOOD_MAIN: "원재료", FOOD_MEAL: "현장 직원식대"}


def test_review_import_rejects_duplicate_approval(tmp_path):
    plan, path = _fixture(tmp_path)
    item = json.loads(path.read_text(encoding="utf-8"))["items"][0]
    from modules.transform.pipelines.db.samjeon_material_agent.classification import item_key
    from modules.transform.pipelines.db.samjeon_material_agent.contracts import product_key
    row = {**item, "product_key": product_key(item_key(item)), "approved": "Y", "approved_category": "포장재"}
    review = tmp_path / "review.csv"
    pd.DataFrame([row, row]).to_csv(review, index=False, encoding="utf-8-sig")
    before = path.read_bytes()
    with pytest.raises(ValueError, match="중복"):
        import_review(review, path, version="r2", expected_hash=digest(path))
    assert path.read_bytes() == before


def confirm_day(plan, tmp_path, day="2026-10-11"):
    sales, _ = load_sales_lines(plan)
    q = quality_daily(plan, sales, mart.load_purchases(plan))
    review = coverage_candidates(q.loc[q["date"].eq(day)]).copy()
    review["approved"], review["confirmation_evidence"], review["confirmed_by"] = "Y", "합성 조회 완료 근거", "시험 담당자"
    review["no_transactions"] = review["row_count"].eq(0).map({True: "Y", False: "N"})
    path = tmp_path / "coverage_review.csv"
    review.to_csv(path, index=False, encoding="utf-8-sig")
    return import_coverage(path, Path(plan["coverage_path"]), expected_hash=plan["config_hashes"]["coverage"], current_quality=q)


def test_coverage_day_slice_ignores_other_day_revision(tmp_path):
    plan, _ = _fixture(tmp_path)
    assert confirm_day(plan, tmp_path)["confirmed_days_sources"] == 3
    plan = replan(plan)
    path = Path(plan["food_files"][-1]["path"])
    frame = pd.read_parquet(path)
    frame.loc[2, "합계"] = 99999
    frame.to_parquet(path)
    plan = replan(plan)
    sales, _ = load_sales_lines(plan)
    q = quality_daily(plan, sales, mart.load_purchases(plan))
    assert q.loc[q["date"].eq("2026-10-11"), "status"].eq("정상").all()
    frame.loc[0, "합계"] = 30001
    frame.to_parquet(path)
    plan = replan(plan)
    q = quality_daily(plan, sales, mart.load_purchases(plan))
    assert q.loc[q["date"].eq("2026-10-11") & q["source"].eq("food_guide"), "status"].item() == "오래됨"


def test_missing_dam_is_not_zero_without_evidence(tmp_path):
    plan, _ = _fixture(tmp_path)
    stage = build(plan)
    q = pd.read_parquet(stage / "quality_daily.parquet")
    assert q.loc[q["source"].eq("logistics_dam"), "status"].eq("누락").all()
    confirm_day(plan, tmp_path)
    stage = build(replan(plan))
    context = json.loads((stage / "agent_context.json").read_text(encoding="utf-8"))
    assert context["daily"]["complete"] is True
    assert context["daily"]["purchase_rate_pct"] == "27.3"
    assert context["daily"]["target_comparison_status"] == "참고"


def test_reconciliation_catches_offsetting_group_corruption(tmp_path):
    plan, _ = _fixture(tmp_path)
    stage = build(plan)
    path = stage / "purchase_daily.parquet"
    daily = pd.read_parquet(path)
    daily.loc[0, "amount_won"] += 1
    daily.loc[1, "amount_won"] -= 1
    daily.to_parquet(path, index=False)
    manifest = json.loads((stage / "manifest.json").read_text(encoding="utf-8"))
    manifest["files"][path.name] = digest(path)
    atomic_json(stage / "manifest.json", manifest)
    with pytest.raises(ValueError, match="그룹별"):
        mart.validate_snapshot(plan)


def test_query_evidence_pagination_range_and_cross_platform_ref(tmp_path):
    plan, _ = _fixture(tmp_path)
    stage = build(plan)
    result = query_snapshot(stage, intent="evidence", date_from="2026-10-11", limit=2)
    assert result["result"]["total"] == 5
    assert result["result"]["next_offset"] == 2
    evidence = result["result"]["rows"][0]
    only = query_snapshot(stage, intent="evidence", date_from="2026-10-11", evidence_id=evidence["evidence_id"])
    assert only["result"]["total"] == 1
    out = query_snapshot(stage, date_from="2020-01-01")
    assert out["status"] == "범위 밖"
    assert resolve_source_ref("sales/unified_sales_261011.parquet", sales_root=tmp_path, food_root=tmp_path, dam_root=tmp_path) == tmp_path / "unified_sales_261011.parquet"
    with pytest.raises(ValueError):
        resolve_source_ref("sales/../secret", sales_root=tmp_path, food_root=tmp_path, dam_root=tmp_path)


def test_range_publication_does_not_update_latest(tmp_path):
    original, _ = _fixture(tmp_path)
    plan = replan(original, date_from="2026-10-01", date_to="2026-10-11")
    build(plan)
    root = Path(plan["output_root"])
    root.mkdir()
    pointer = root / "latest.json"
    pointer.write_text('{"previous":true}', encoding="utf-8")
    before = pointer.read_bytes()
    assert mart.publish_snapshot(plan)["latest_updated"] is False
    assert pointer.read_bytes() == before


def test_copy_failure_preserves_pointer_and_cleans_pending(tmp_path, monkeypatch):
    plan, _ = _fixture(tmp_path)
    build(plan)
    root = Path(plan["output_root"])
    root.mkdir()
    pointer = root / "latest.json"
    pointer.write_text('{"previous":true}', encoding="utf-8")
    def fail(*args, **kwargs):
        raise OSError("합성 복사 실패")
    monkeypatch.setattr(mart.shutil, "copy2", fail)
    with pytest.raises(OSError):
        mart.publish_snapshot(plan)
    assert pointer.read_text(encoding="utf-8") == '{"previous":true}'
    assert list((root / "snapshots").iterdir()) == []


def test_empty_sources_produce_missing_not_normal(tmp_path):
    plan, _ = _fixture(tmp_path)
    Path(plan["sales_files"][-1]["path"]).unlink()
    Path(plan["food_files"][-1]["path"]).unlink()
    stage = build(replan(plan))
    context = json.loads((stage / "agent_context.json").read_text(encoding="utf-8"))
    assert context["status"] == "누락"
    assert context["daily"]["complete"] is False
    assert context["daily"]["purchase_rate_pct"] is None


def test_dry_run_and_doctor_do_not_write(tmp_path, monkeypatch):
    from modules.transform.pipelines.db.samjeon_material_agent import __main__ as cli
    plan, _ = _fixture(tmp_path)
    monkeypatch.setattr(cli, "plan_build", lambda *args, **kwargs: plan)
    before = set(tmp_path.rglob("*"))
    result = cli.main(["build", "--date", "2026-10-11", "--dry-run"])
    assert result["files_written"] is False
    assert result["published"] is False
    assert set(tmp_path.rglob("*")) == before
    assert cli.main(["doctor", "--date", "2026-10-11"])["files_written"] is False
    assert set(tmp_path.rglob("*")) == before


def test_dag_conf_and_disabled_publish_gate(tmp_path):
    import ast
    from types import SimpleNamespace
    path = Path(__file__).parents[1] / "dags/db/DB_Samjeon_Material_Mart_Dags.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    functions = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef)], type_ignores=[])
    plan, _ = _fixture(tmp_path)
    received = []
    def resolve(*args, **kwargs):
        received.append((args, kwargs))
        return plan
    class Skip(Exception):
        pass
    def unexpected_read(*args, **kwargs):
        pytest.fail("비활성 게시 태스크에서 계산·게시가 실행됨")
    namespace = {"plan_build": resolve, "save_plan": mart.save_plan, "read_plan": unexpected_read,
                 "Variable": SimpleNamespace(get=lambda *args, **kwargs: "false"), "AirflowSkipException": Skip}
    exec(compile(functions, str(path), "exec"), namespace)
    reference = namespace["_plan"](dag_run=SimpleNamespace(conf={"date_from": "2026-09-01", "date_to": "2026-10-11"}, run_id="run-1"))
    assert received[0][1]["date_from"] == "2026-09-01"
    assert len(json_bytes(reference)) < 1000
    with pytest.raises(ValueError):
        namespace["_plan"](dag_run=SimpleNamespace(conf={"unsupported": True}, run_id="run-1"))
    with pytest.raises(Skip):
        namespace["_run"]("publish", ti=SimpleNamespace(xcom_pull=lambda **kwargs: reference))


def test_config_guards_and_version_change(tmp_path):
    with pytest.raises(ValueError):
        validate_settings({"amount_basis_confirmed": "false"})
    with pytest.raises(ValueError):
        validate_settings({"minimum_weekday_samples": 3})
    with pytest.raises(ValueError):
        validate_settings({"target_rate": "NaN"})
    plan, _ = _fixture(tmp_path)
    stage = build(plan)
    atomic_json(Path(plan["settings_path"]), {"target_start_date": "2026-10-01", "amount_basis_confirmed": True})
    with pytest.raises(RuntimeError, match="설정"):
        mart.publish_snapshot(plan)
    assert plan["snapshot_id"] != replan(plan)["snapshot_id"]


def test_review_master_exact_match_and_nonfood_stays_reviewable(tmp_path):
    plan, classification = _fixture(tmp_path)
    classification.unlink()
    food = Path(plan["food_files"][-1]["path"])
    frame = pd.read_parquet(food)
    frame["대분류"] = "비식품"
    frame.to_parquet(food)
    plan = replan(plan)
    stage = build(plan)
    review = pd.read_csv(stage / "review_candidates.csv", encoding="utf-8-sig").fillna("")
    assert review.iloc[0]["suggested_category"] == ""
    master = review[["source", "product_code", "item_name", "spec", "unit"]].copy()
    master["item_category"] = "용기_수저_봉투"
    master.to_csv(plan["recommendation_path"], index=False, encoding="utf-8-sig")
    stage = build(replan(plan))
    review = pd.read_csv(stage / "review_candidates.csv", encoding="utf-8-sig").fillna("")
    assert review.iloc[0]["suggested_category"] == "포장재"
    assert review.iloc[0]["approved_category"] == ""


def test_unclosed_dam_cannot_be_confirmed(tmp_path):
    plan, _ = _fixture(tmp_path)
    path = Path(plan["dam_files"][-1]["path"])
    pd.DataFrame([{"sale_date": "2026-10-11", "배송일자": "2026-10-11", "거래처명": FOOD_MAIN,
                   "코드": 1.0, "품명": "닭", "규격": "1kg", "단위": "EA", "수량": 1,
                   "금액": 100, "마감됨": False}]).to_parquet(path)
    plan = replan(plan)
    with pytest.raises(ValueError, match="미마감"):
        confirm_day(plan, tmp_path)


@pytest.mark.parametrize("field,value,match", [("합계", 1.5, "정수 원"), ("배송일", "잘못된날짜", "날짜")])
def test_invalid_purchase_values_fail(tmp_path, field, value, match):
    plan, _ = _fixture(tmp_path)
    path = Path(plan["food_files"][-1]["path"])
    frame = pd.read_parquet(path)
    frame[field] = frame[field].astype(object)
    frame.loc[0, field] = value
    if field == "합계":
        frame[field] = frame[field].astype(float)
    frame.to_parquet(path)
    plan = replan(plan)
    with pytest.raises(ValueError, match=match):
        mart.stage_purchases(plan)


def test_missing_quality_source_never_yields_provisional_ratio():
    plan, tables = analytical_tables()
    q = tables["quality_daily"]
    missing = q.loc[q["source"].ne("logistics_dam")]
    duplicated = q.loc[q["source"].eq("sales")]
    tables["quality_daily"] = pd.concat([missing, duplicated], ignore_index=True)
    result = period_result(plan["as_of_date"], plan["as_of_date"], tables, validate_settings({}))
    assert result["complete"] is False
    assert result["provisional_purchase_rate_pct"] is None


def test_large_amount_reconciliation_has_no_float_tolerance():
    actual = pd.DataFrame({"key": ["a", "b"], "amount_won": [19600001, 19599999]})
    expected = pd.DataFrame({"key": ["a", "b"], "amount_won": [19600000, 19600000]})
    with pytest.raises(ValueError, match="대사"):
        mart._equal_frame(actual, expected, ["key"], "원 단위")


def test_staff_meal_query_is_independent_of_missing_sales(tmp_path):
    plan, _ = _fixture(tmp_path)
    confirm_day(plan, tmp_path)
    Path(plan["sales_files"][-1]["path"]).unlink()
    stage = build(replan(plan))
    result = query_snapshot(stage, intent="staff_meal", date_from="2026-10-11", date_to="2026-10-11")
    assert result["status"] == "정상"
    assert result["result"]["staff_meal_won"] == 5000


def test_purchase_identical_rows_and_refund_keep_signs(tmp_path):
    plan, _ = _fixture(tmp_path)
    path = Path(plan["food_files"][-1]["path"])
    frame = pd.read_parquet(path)
    refund = frame.iloc[[0]].copy()
    refund["합계"] = -3000
    pd.concat([frame, frame.iloc[[0]], refund], ignore_index=True).to_parquet(path)
    stage = build(replan(plan))
    lines = pd.read_parquet(stage / "purchase_lines.parquet")
    assert len(lines) == 4
    assert int(lines.loc[lines["account"].eq(FOOD_MAIN), "amount_won"].sum()) == 57000
    assert lines["evidence_id"].nunique() == 4


@pytest.mark.parametrize("before", [0, -100])
def test_nonpositive_comparison_base_returns_difference_only(before):
    from modules.transform.pipelines.db.samjeon_material_agent.analytics import compare_periods
    common = {"date_from": "2026-10-01", "date_to": "2026-10-07", "purchase_complete": True}
    result = compare_periods({**common, "observed_purchase_won": 300},
                             {**common, "observed_purchase_won": before}, "observed_purchase_won", "purchase_complete")
    assert result["increase_won"] == 300 - before
    assert result["increase_rate_pct"] is None
    assert "0 이하" in result["reason"]
    if before < 0:
        assert "환불" in result["reason"]
