"""검증된 단일 스냅샷에 대한 제한된 읽기 전용 조회."""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db.samjeon_material_agent.analytics import build_context, period_result
from modules.transform.pipelines.db.samjeon_material_agent.contracts import SCHEMA_VERSION, digest, validate_context
from modules.transform.pipelines.db.samjeon_material_agent.mart import OUTPUT_NAMES, OUTPUT_TABLES
from modules.transform.pipelines.db.samjeon_material_agent.settings import validate_settings
from modules.transform.pipelines.db.samjeon_material_agent.monthly import load_monthly, csv_evidence, summary_period

INTENTS = {"summary", "compare", "anomalies", "forecast", "staff_meal", "evidence", "status"}


def load_snapshot(directory: Path) -> tuple[dict, dict, dict]:
    directory = Path(directory)
    if directory.is_dir() and not (directory / "manifest.json").is_file():
        month = directory.name
        is_month = (directory / f"{month}.csv").is_file()
        return load_monthly(directory.parent if is_month else directory)
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("이 스냅샷은 v3 조회 계약이 아닙니다. v3 로컬 재계산이 필요합니다")
    if set(manifest["files"]) != set(OUTPUT_NAMES) - {"manifest.json"}:
        raise ValueError("스냅샷 필수 파일 목록 불일치")
    for name, expected in manifest["files"].items():
        if digest(directory / name) != expected:
            raise ValueError(f"스냅샷 파일 해시 불일치: {name}")
    context = json.loads((directory / "agent_context.json").read_text(encoding="utf-8"))
    validate_context(context)
    if context["snapshot_id"] != manifest["snapshot_id"] or context["guide"] != {
        "file": "agent_guide.md", "sha256": manifest["files"]["agent_guide.md"]
    }:
        raise ValueError("문맥·가이드·manifest 판본 불일치")
    tables = {name: pd.read_parquet(directory / f"{name}.parquet") for name in OUTPUT_TABLES}
    return manifest, context, tables


def query_snapshot(directory: Path, *, intent: str = "summary", date_from: str | None = None,
                   date_to: str | None = None, evidence_id: str | None = None, product_key: str | None = None,
                   source: str | None = None, offset: int = 0, limit: int = 100) -> dict:
    if intent not in INTENTS:
        raise ValueError(f"지원하지 않는 조회 종류: {intent}")
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError("근거 조회 offset은 0 이상, limit은 1~500이어야 합니다")
    manifest, context, tables = load_snapshot(Path(directory))
    end = date_to or manifest["as_of_date"]
    start = date_from or end[:7] + "-01"
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if str(first) != start or str(last) != end or first > last:
        raise ValueError("조회 날짜 또는 기간 오류")
    extra = {"compare": 13, "anomalies": 56, "forecast": 55}.get(intent, 0)
    required_start = min(first, last - timedelta(days=extra))
    if intent in {"compare", "anomalies", "forecast"}:
        required_start = min(required_start, last.replace(day=1))
    identity = {"snapshot_id": manifest["snapshot_id"], "schema_version": SCHEMA_VERSION,
                "intent": intent, "date_from": start, "date_to": end}
    if manifest.get("format") == "monthly_csv_v3":
        if start < manifest["window_start"] or end > manifest["as_of_date"]:
            return {**identity, "status": "범위 밖", "reason": "해당 기간의 월별 CSV가 필요",
                    "required_date_from": start, "required_date_to": end}
        if intent not in {"summary", "status", "staff_meal"}:
            raise ValueError("집계 전용 자료에서 지원하지 않는 조회입니다")
        period = summary_period(tables["daily_summary"], start, end)
        result = period if intent == "summary" else (
            {"staff_meal_won": period["staff_meal_won"],
             "staff_meal_complete": period["staff_meal_complete"]} if intent == "staff_meal" else
            {"period": period, "quality": tables["daily_summary"].loc[
                tables["daily_summary"]["날짜"].between(start, end),
                ["날짜", "자료상태", "매출원천상태", "푸드가이드상태", "물류담상태", "확인필요"]].to_dict("records")})
        return {**identity, "status": period["status"], "result": result}
    if str(required_start) < manifest["window_start"] or end > manifest["as_of_date"]:
        return {**identity, "status": "범위 밖", "reason": "해당 기간의 월별 CSV가 필요",
                "required_date_from": str(required_start), "required_date_to": end}
    settings = validate_settings(context["settings"])
    period = period_result(start, end, tables, settings)
    result_status = period["status"]
    if intent in {"summary", "staff_meal", "status"}:
        result = period
        if intent == "staff_meal":
            result = {key: period[key] for key in ("staff_meal_won", "staff_meal_complete", "evidence")}
            statuses = set(tables["quality_daily"].loc[
                tables["quality_daily"]["date"].between(start, end) & tables["quality_daily"]["source"].eq("food_guide"), "status"])
            result_status = ("정상" if period["staff_meal_complete"] else
                             next((status for status in ("검증 실패", "누락", "오래됨") if status in statuses), "잠정"))
        if intent == "status":
            result = {"period": period, "quality": tables["quality_daily"].loc[
                tables["quality_daily"]["date"].between(start, end)].to_dict("records")}
    elif intent == "evidence":
        records = []
        for kind, column in (("sales_lines", "sale_date"), ("purchase_lines", "purchase_date")):
            rows = tables[kind]
            rows = rows.loc[rows[column].between(start, end)]
            for field, value in (("evidence_id", evidence_id), ("product_key", product_key), ("source", source)):
                if value is not None:
                    rows = rows.loc[rows[field].eq(value)] if field in rows else rows.iloc[:0]
            records.extend({"kind": kind, **row} for row in rows.to_dict("records"))
        records.sort(key=lambda row: (row.get("sale_date", row.get("purchase_date")), row["evidence_id"]))
        result = {"total": len(records), "offset": offset, "limit": limit, "rows": records[offset:offset + limit],
                  "next_offset": offset + limit if offset + limit < len(records) else None}
    else:
        plan = {"as_of_date": end, "start_date": manifest["window_start"], "query_start": start,
                "snapshot_id": manifest["snapshot_id"], "guide_sha256": manifest["guide_sha256"]}
        calculated = build_context(plan, tables, context["classification_version"], settings)
        result = ({"seven_day": calculated["seven_day"], "staff_meal_seven_day": calculated["staff_meal_seven_day"],
                   "top_increase_items": calculated["top_increase_items"]} if intent == "compare" else
                  calculated["alerts"] if intent == "anomalies" else calculated["forecast"])
    return {**identity, "status": result_status,
            "result": csv_evidence(result) if str(manifest.get("format", "")).startswith("monthly_csv_") else result}
