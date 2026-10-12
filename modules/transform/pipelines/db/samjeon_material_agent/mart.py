"""삼전점 마트 실행계획, 스테이징, 대사 및 게시 진입점."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import uuid
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pendulum

from modules.transform.pipelines.db.samjeon_material_agent.locations import ANALYTICS_DB, LOCAL_DB, MART_DB
from modules.transform.pipelines.db.samjeon_material_agent.contracts import (
    ALL_CATEGORIES, CATEGORIES, DAM_STORE, FOOD_MAIN, FOOD_MEAL, STORE, RULE_VERSION, SCHEMA_VERSION,
    SOURCE_NAMES, atomic_json, digest as _digest, json_bytes as _json_bytes,
    read_config as _read_config, validate_context,
)
from modules.transform.pipelines.db.samjeon_material_agent.sources import (
    aggregate_sales, file_entry, load_purchases, load_sales, load_sales_lines,
)
from modules.transform.pipelines.db.samjeon_material_agent.classification import (
    classify as _classify, load_classification as _classification, review_candidates,
)
from modules.transform.pipelines.db.samjeon_material_agent.quality import coverage_candidates, quality_daily
from modules.transform.pipelines.db.samjeon_material_agent.analytics import (
    build_context, forecast as _forecast, quality_issues,
)
from modules.transform.pipelines.db.samjeon_material_agent.settings import load_settings

logger = logging.getLogger(__name__)
DEFAULT_GUIDE = Path(__file__).with_name("agent_guide.md")
DEFAULT_OUTPUT = MART_DB / "CEO_DB" / "samjeon_agent_DB"
DEFAULT_WORK = LOCAL_DB / "temp" / "samjeon_material_mart"
DEFAULT_CLASSIFICATION = LOCAL_DB / "samjeon_material_agent" / "classification.json"
DEFAULT_COVERAGE = LOCAL_DB / "samjeon_material_agent" / "coverage.json"
DEFAULT_SETTINGS = LOCAL_DB / "samjeon_material_agent" / "config.json"
DEFAULT_RECOMMENDATION = MART_DB / "Logistics" / "Logistics_product" / "product_master_input.csv"
OUTPUT_TABLES = ("purchase_lines", "sales_lines", "sales_daily", "purchase_daily", "item_daily", "quality_daily")
OUTPUT_NAMES = tuple(f"{name}.parquet" for name in OUTPUT_TABLES) + (
    "review_candidates.csv", "coverage_candidates.csv", "agent_context.json", "issues.json", "agent_guide.md", "manifest.json")
PURCHASE_GROUPS = ["purchase_date", "source", "account", "category"]
ITEM_GROUPS = [*PURCHASE_GROUPS, "product_key", "product_code", "item_name", "spec", "unit"]


def _month_names(start: date, end: date) -> list[str]:
    months, cursor = [], start.replace(day=1)
    while cursor <= end:
        months.append(cursor.strftime("%Y%m"))
        cursor = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
    return months


def _iso(value: str) -> date:
    result = date.fromisoformat(value)
    if str(result) != value:
        raise ValueError("날짜는 YYYY-MM-DD 형식이어야 합니다")
    return result


def _implementation_hash() -> str:
    return hashlib.sha256(_json_bytes({p.name: _digest(p) for p in sorted(Path(__file__).parent.glob("*.py"))})).hexdigest()


def plan_build(as_of_date: str | None = None, *, date_from: str | None = None, date_to: str | None = None,
               sales_root=None, food_root=None, dam_root=None, output_root=None, work_root=None,
               classification_path=None, coverage_path=None, guide_path=None, settings_path=None,
               recommendation_path=None, run_id: str | None = None) -> dict:
    """파일 생성 없이 원천과 기준일을 확정한다. 각 실행은 고유 작업 공간을 갖는다."""
    if as_of_date is not None and (date_from is not None or date_to is not None):
        raise ValueError("sale_date와 date_from/date_to를 혼용할 수 없습니다")
    range_request = date_from is not None or date_to is not None
    if range_request and (date_from is None or date_to is None):
        raise ValueError("date_from과 date_to를 함께 지정해야 합니다")
    explicit = date_to if date_to is not None else as_of_date
    target = _iso(explicit) if explicit is not None else pendulum.now("Asia/Seoul").date() - timedelta(days=1)
    if target >= pendulum.now("Asia/Seoul").date():
        raise ValueError("기준일은 한국시간 전일 이전이어야 합니다")
    requested = _iso(date_from) if range_request else target.replace(day=1)
    if requested > target:
        raise ValueError("조회 시작일이 종료일보다 늦습니다")
    start = min(target - timedelta(days=56), requested, target.replace(day=1))
    roots = [Path(sales_root) if sales_root is not None else MART_DB / "unified_sales_grp",
             Path(food_root) if food_root is not None else ANALYTICS_DB / "Food_Guide_Raw",
             Path(dam_root) if dam_root is not None else ANALYTICS_DB / "Logistics_Dam"]
    configs = {"classification": Path(classification_path) if classification_path is not None else DEFAULT_CLASSIFICATION,
               "coverage": Path(coverage_path) if coverage_path is not None else DEFAULT_COVERAGE,
               "settings": Path(settings_path) if settings_path is not None else DEFAULT_SETTINGS,
               "recommendation": Path(recommendation_path) if recommendation_path is not None else DEFAULT_RECOMMENDATION}
    guide = Path(guide_path) if guide_path is not None else DEFAULT_GUIDE
    days = [str(start + timedelta(days=i)) for i in range((target - start).days + 1)]
    sales = [file_entry(roots[0] / f"unified_sales_{day[2:].replace('-', '')}.parquet", "sales", day) for day in days]
    food = [file_entry(roots[1] / f"food_guide_orders_{month}.parquet", "food_guide", month) for month in _month_names(start, target)]
    dam = [file_entry(roots[2] / f"Dam_order_details_{day[2:].replace('-', '')}.parquet", "logistics_dam", day) for day in days]
    hashes = {key: _digest(path) if path.is_file() else "" for key, path in configs.items()}
    guide_hash, implementation = _digest(guide), _implementation_hash()
    signature = hashlib.sha256(_json_bytes({"date": str(target), "query_start": str(requested), "range_request": range_request,
                                            "files": sales + food + dam, "config": hashes, "guide": guide_hash,
                                            "implementation": implementation, "rule": RULE_VERSION})).hexdigest()[:16]
    snapshot_id = f"{target}-{signature}"
    work = Path(work_root) if work_root is not None else DEFAULT_WORK
    execution = hashlib.sha256(run_id.encode()).hexdigest()[:20] if run_id else uuid.uuid4().hex
    plan = {"schema_version": SCHEMA_VERSION, "as_of_date": str(target), "start_date": str(start), "days": days,
            "query_start": str(requested), "query_end": str(target), "range_request": range_request,
            "sales_files": sales, "food_files": food, "dam_files": dam, "config_hashes": hashes,
            **{f"{key}_path": str(path) for key, path in configs.items()}, "snapshot_id": snapshot_id,
            "guide_path": str(guide), "guide_sha256": guide_hash, "implementation_hash": implementation,
            "stage_dir": str(work / snapshot_id / execution),
            "output_root": str(Path(output_root) if output_root is not None else DEFAULT_OUTPUT)}
    load_settings(plan)
    _classification(plan)
    return plan


def save_plan(plan: dict) -> dict:
    path = Path(plan["stage_dir"]) / "plan.json"
    atomic_json(path, plan)
    return {"plan_path": str(path), "sha256": _digest(path), "snapshot_id": plan["snapshot_id"]}


def read_plan(reference: dict) -> dict:
    path = Path(reference["plan_path"])
    if _digest(path) != reference["sha256"]:
        raise ValueError("실행계획 해시 불일치")
    plan = json.loads(path.read_text(encoding="utf-8"))
    if plan["snapshot_id"] != reference["snapshot_id"] or _digest(path) != reference["sha256"]:
        raise ValueError("실행계획 판본 불일치")
    return plan


def _stage(plan: dict) -> Path:
    path = Path(plan["stage_dir"])
    path.mkdir(parents=True, exist_ok=True)
    return path


def stage_sales(plan: dict) -> dict:
    lines, counts = load_sales_lines(plan)
    stage = _stage(plan)
    lines.to_parquet(stage / "sales_lines.parquet", index=False)
    aggregate_sales(lines).to_parquet(stage / "sales_daily.parquet", index=False)
    receipt = {name: _digest(stage / name) for name in ("sales_lines.parquet", "sales_daily.parquet")}
    atomic_json(stage / "sales_receipt.json", receipt)
    return {"rows": len(lines), "source_rows": sum(counts.values()), "sha256": receipt["sales_lines.parquet"]}


def stage_purchases(plan: dict) -> dict:
    lines = load_purchases(plan)
    stage = _stage(plan)
    path = stage / "purchase_lines_raw.parquet"
    lines.to_parquet(path, index=False)
    receipt = {path.name: _digest(path)}
    atomic_json(stage / "purchases_receipt.json", receipt)
    return {"rows": len(lines), "sha256": receipt[path.name]}


def _group(lines: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if lines.empty:
        return pd.DataFrame(columns=[*keys, "amount_won", "row_count"])
    return lines.groupby(keys, dropna=False).agg(amount_won=("amount_won", "sum"), row_count=("evidence_id", "size")).reset_index()


def _agent_context(plan: dict, tables: dict, classification_version: str) -> dict:
    return build_context(plan, tables, classification_version, load_settings(plan))


def _aggregate_tables(plan: dict, sales: pd.DataFrame, purchases: pd.DataFrame, sales_lines: pd.DataFrame | None = None) -> tuple[dict, dict]:
    mapping, version = _classification(plan)
    lines = _classify(purchases, mapping)
    if sales_lines is None:
        sales_lines, _ = load_sales_lines(plan)
    quality = quality_daily(plan, sales_lines, lines)
    tables = {"purchase_lines": lines, "sales_lines": sales_lines, "sales_daily": sales,
              "purchase_daily": _group(lines, PURCHASE_GROUPS), "item_daily": _group(lines, ITEM_GROUPS),
              "quality_daily": quality,
              "review_candidates": review_candidates(lines, plan["recommendation_path"], plan["config_hashes"]["recommendation"]),
              "coverage_candidates": coverage_candidates(quality)}
    return tables, _agent_context(plan, tables, version)


def _check_source_versions(plan: dict) -> None:
    for entry in plan["sales_files"] + plan["food_files"] + plan["dam_files"]:
        path = Path(entry["path"])
        if (_digest(path) if path.is_file() else "") != entry["sha256"]:
            raise RuntimeError(f"원천 판본 변경으로 게시 중단: {path}")
    for name in ("classification", "coverage", "settings"):
        _read_config(plan[f"{name}_path"], plan["config_hashes"][name])
    recommendation = Path(plan["recommendation_path"])
    if (_digest(recommendation) if recommendation.is_file() else "") != plan["config_hashes"]["recommendation"]:
        raise RuntimeError("품목 추천 원천 판본 변경")
    if _digest(Path(plan["guide_path"])) != plan["guide_sha256"]:
        raise RuntimeError("계획 이후 LLM 가이드 판본 변경")
    if _implementation_hash() != plan["implementation_hash"]:
        raise RuntimeError("실행 도중 계산 코드 판본 변경")


def stage_aggregate(plan: dict) -> dict:
    stage = _stage(plan)
    for receipt_name in ("sales_receipt.json", "purchases_receipt.json"):
        for name, expected in json.loads((stage / receipt_name).read_text(encoding="utf-8")).items():
            if _digest(stage / name) != expected:
                raise ValueError(f"중간 파일 해시 불일치: {name}")
    sales, purchases, sales_lines = (pd.read_parquet(stage / name) for name in
                                   ("sales_daily.parquet", "purchase_lines_raw.parquet", "sales_lines.parquet"))
    tables, context = _aggregate_tables(plan, sales, purchases, sales_lines)
    _check_source_versions(plan)
    validate_context(context)
    for name in OUTPUT_TABLES:
        tables[name].to_parquet(stage / f"{name}.parquet", index=False)
    for name in ("review_candidates", "coverage_candidates"):
        tables[name].to_csv(stage / f"{name}.csv", index=False, encoding="utf-8-sig")
    shutil.copy2(plan["guide_path"], stage / "agent_guide.md")
    if _digest(stage / "agent_guide.md") != plan["guide_sha256"]:
        raise RuntimeError("LLM 가이드 복사 검증 실패")
    atomic_json(stage / "agent_context.json", context)
    atomic_json(stage / "issues.json", quality_issues(plan, tables))
    manifest = {"schema_version": SCHEMA_VERSION, "snapshot_id": plan["snapshot_id"], "as_of_date": plan["as_of_date"],
                "window_start": plan["start_date"], "query_start": plan["query_start"], "range_request": plan["range_request"],
                "rule_version": RULE_VERSION, "implementation_hash": plan["implementation_hash"],
                "sources": plan["sales_files"] + plan["food_files"] + plan["dam_files"],
                "config_hashes": plan["config_hashes"], "guide_sha256": plan["guide_sha256"],
                "files": {name: _digest(stage / name) for name in OUTPUT_NAMES if name != "manifest.json"},
                "rows": {name: len(tables[name]) for name in OUTPUT_TABLES}, "status": context["status"]}
    atomic_json(stage / "manifest.json", manifest)
    logger.info("삼전점 마트 스테이징: snapshot=%s status=%s rows=%s", plan["snapshot_id"], context["status"], manifest["rows"])
    return {"snapshot_id": plan["snapshot_id"], "status": context["status"], "rows": manifest["rows"]}


def _equal_frame(actual: pd.DataFrame, expected: pd.DataFrame, keys: list[str], label: str) -> None:
    if set(actual.columns) != set(expected.columns):
        raise ValueError(f"{label} 스키마 대사 실패")
    left = actual.sort_values(keys).reset_index(drop=True)[expected.columns]
    right = expected.sort_values(keys).reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(left, right, check_dtype=False, check_exact=True)
    except AssertionError as exc:
        raise ValueError(f"{label} 그룹별 대사 실패") from exc


def validate_snapshot(plan: dict) -> dict:
    _check_source_versions(plan)
    stage = Path(plan["stage_dir"])
    manifest = json.loads((stage / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest["snapshot_id"] != plan["snapshot_id"]:
        raise ValueError("스냅샷 ID 또는 스키마 버전 불일치")
    expected_metadata = {"as_of_date": plan["as_of_date"], "window_start": plan["start_date"],
                         "query_start": plan["query_start"], "range_request": plan["range_request"],
                         "config_hashes": plan["config_hashes"], "implementation_hash": plan["implementation_hash"],
                         "rule_version": RULE_VERSION,
                         "sources": plan["sales_files"] + plan["food_files"] + plan["dam_files"]}
    if any(manifest.get(key) != value for key, value in expected_metadata.items()):
        raise ValueError("manifest와 실행계획 판본 불일치")
    if set(manifest["files"]) != set(OUTPUT_NAMES) - {"manifest.json"}:
        raise ValueError("스냅샷 필수 파일 목록 불일치")
    for name, expected in manifest["files"].items():
        if _digest(stage / name) != expected:
            raise ValueError(f"스냅샷 파일 해시 불일치: {name}")
    context = json.loads((stage / "agent_context.json").read_text(encoding="utf-8"))
    validate_context(context)
    if context.get("guide") != {"file": "agent_guide.md", "sha256": plan["guide_sha256"]} or manifest["files"]["agent_guide.md"] != plan["guide_sha256"]:
        raise ValueError("에이전트 문맥과 가이드 판본 불일치")
    tables = {name: pd.read_parquet(stage / f"{name}.parquet") for name in OUTPUT_TABLES}
    lines, sales_lines, quality = (tables[name] for name in ("purchase_lines", "sales_lines", "quality_daily"))
    sales_receipt = json.loads((stage / "sales_receipt.json").read_text(encoding="utf-8"))
    if _digest(stage / "sales_lines.parquet") != sales_receipt["sales_lines.parquet"]:
        raise ValueError("매출 원천 행 스테이징 판본 변경")
    purchase_receipt = json.loads((stage / "purchases_receipt.json").read_text(encoding="utf-8"))
    if _digest(stage / "purchase_lines_raw.parquet") != purchase_receipt["purchase_lines_raw.parquet"]:
        raise ValueError("구매 원천 행 스테이징 판본 변경")
    raw_purchases = pd.read_parquet(stage / "purchase_lines_raw.parquet")
    raw_fields = [key for key in raw_purchases.columns if key not in {"category", "classification_status"}]
    _equal_frame(lines[raw_fields], raw_purchases[raw_fields], ["evidence_id"], "구매 원천 행")
    for name, frame in tables.items():
        if len(frame) != manifest["rows"][name]:
            raise ValueError(f"manifest 행수 불일치: {name}")
    expected_pairs = {(day, source) for day in plan["days"] for source in SOURCE_NAMES}
    if set(zip(quality["date"], quality["source"])) != expected_pairs or quality.duplicated(["date", "source"]).any():
        raise ValueError("품질 날짜·원천 매트릭스가 불완전합니다")
    entries = {entry["source_ref"]: entry for entry in manifest["sources"]}
    for frame, date_column in ((lines, "purchase_date"), (sales_lines, "sale_date")):
        if frame["evidence_id"].duplicated().any():
            raise ValueError("근거 ID 중복")
        if not frame[date_column].between(plan["start_date"], plan["as_of_date"]).all():
            raise ValueError("계산 기간 밖의 행이 포함됐습니다")
        for row in frame.to_dict("records"):
            entry = entries.get(row["source_ref"])
            if not entry or entry["sha256"] != row["source_sha256"] or row["source_row"] < 0:
                raise ValueError("원천 근거 연결 불일치")
            expected_id = hashlib.sha256(_json_bytes([row["source_ref"], row["source_sha256"], row["source_row"]])).hexdigest()[:24]
            if row["evidence_id"] != expected_id:
                raise ValueError("근거 ID 판본 불일치")
    if sales_lines["_pk"].duplicated().any():
        raise ValueError("매출 키 중복")
    if not lines["category"].isin(ALL_CATEGORIES).all() or not lines.loc[lines["account"].eq(FOOD_MEAL), "category"].eq("현장 직원식대").all():
        raise ValueError("구매 비용 분류 오류")
    _equal_frame(tables["sales_daily"], aggregate_sales(sales_lines), ["sale_date", "source"], "매출")
    _equal_frame(tables["purchase_daily"], _group(lines, PURCHASE_GROUPS), PURCHASE_GROUPS, "구매")
    _equal_frame(tables["item_daily"], _group(lines, ITEM_GROUPS), ITEM_GROUPS, "품목")
    mapping, version = _classification(plan)
    _equal_frame(lines, _classify(lines, mapping), ["evidence_id"], "분류")
    _equal_frame(quality, quality_daily(plan, sales_lines, lines), ["date", "source"], "품질")
    if _json_bytes(context) != _json_bytes(_agent_context(plan, tables, version)) or manifest["status"] != context["status"]:
        raise ValueError("에이전트 문맥 계산 대사 실패")
    if json.loads((stage / "issues.json").read_text(encoding="utf-8")) != quality_issues(plan, tables):
        raise ValueError("품질 문제 목록 대사 실패")
    return {"snapshot_id": plan["snapshot_id"], "status": manifest["status"],
            "purchase_rows": len(lines), "sales_rows": len(sales_lines)}


def publish_snapshot(plan: dict) -> dict:
    """게시 승인은 호출자(DAG Variable)가 검사한다. 완성본만 원자적으로 교체한다."""
    summary = validate_snapshot(plan)
    stage, root = Path(plan["stage_dir"]), Path(plan["output_root"])
    snapshots = root / "snapshots"
    snapshots.mkdir(parents=True, exist_ok=True)
    target = snapshots / plan["snapshot_id"]
    if not target.exists():
        pending = snapshots / f".{plan['snapshot_id']}.{uuid.uuid4().hex}.pending"
        pending.mkdir()
        try:
            for name in OUTPUT_NAMES:
                shutil.copy2(stage / name, pending / name)
                if _digest(pending / name) != _digest(stage / name):
                    raise RuntimeError(f"게시 복사 검증 실패: {name}")
            _check_source_versions(plan)
            os.replace(pending, target)
        except Exception:
            if pending.exists() and pending.resolve().parent == snapshots.resolve():
                shutil.rmtree(pending)
            raise
    else:
        for name in OUTPUT_NAMES:
            if _digest(target / name) != _digest(stage / name):
                raise RuntimeError(f"같은 ID의 기존 스냅샷과 내용이 다릅니다: {name}")
    _check_source_versions(plan)
    yesterday = str(pendulum.now("Asia/Seoul").date() - timedelta(days=1))
    summary["latest_updated"] = False
    if plan["as_of_date"] == yesterday and not plan["range_request"]:
        atomic_json(root / "latest.json", {"snapshot_id": plan["snapshot_id"], "as_of_date": plan["as_of_date"],
                                          "schema_version": SCHEMA_VERSION,
                                          "manifest": f"snapshots/{plan['snapshot_id']}/manifest.json"})
        summary["latest_updated"] = True
    return summary
