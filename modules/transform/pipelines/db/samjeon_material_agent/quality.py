"""날짜·원천 범위의 수집 확인과 내용 판본을 검사한다."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db.samjeon_material_agent.contracts import (
    SCOPES, SOURCE_NAMES, atomic_json, json_bytes, read_config, text,
)


def slice_hash(day: str, source: str, rows: pd.DataFrame, present: bool) -> str:
    fields = (["sale_date", "source", "_pk", "amount_won", "row_content_hash"] if source == "sales" else
              ["purchase_date", "source", "account", "product_code", "item_name", "spec", "unit",
               "qty", "amount_won", "closed"])
    # 물리적 파일 해시·행 위치·다른 날짜 변경은 날짜별 마감 판본과 분리한다.
    records = [{key: text(row.get(key)) for key in fields} for row in rows.to_dict("records")]
    ordered = sorted(records, key=lambda row: json_bytes(row))
    return hashlib.sha256(json_bytes({"date": day, "source": source, "scope": SCOPES[source],
                                     "present": present, "rows": ordered})).hexdigest()


def quality_daily(plan: dict, sales_lines: pd.DataFrame, purchases: pd.DataFrame) -> pd.DataFrame:
    coverage = read_config(plan["coverage_path"], plan["config_hashes"]["coverage"])
    records = []
    sales_files = {entry["date"]: entry for entry in plan["sales_files"]}
    food_files = {entry["date"]: entry for entry in plan["food_files"]}
    dam_files = {entry["date"]: entry for entry in plan["dam_files"]}
    for day in plan["days"]:
        entries = {"sales": sales_files[day], "food_guide": food_files[day[:7].replace("-", "")],
                   "logistics_dam": dam_files[day]}
        for source, entry in entries.items():
            rows = (sales_lines.loc[sales_lines["sale_date"].eq(day)] if source == "sales" else
                    purchases.loc[purchases["purchase_date"].eq(day) & purchases["source"].eq(source)])
            count, present = len(rows), bool(entry["sha256"])
            current_hash = slice_hash(day, source, rows, present)
            confirmation = coverage.get("days", {}).get(day, {}).get(source, {})
            evidence = text(confirmation.get("evidence"))
            no_transactions = confirmation.get("no_transactions") is True
            has_slice = bool(confirmation.get("slice_hash"))
            matches = (confirmation.get("slice_hash") == current_hash and confirmation.get("scope") == SCOPES[source]
                       if has_slice else confirmation.get("source_hash") == entry["sha256"] and present)
            if evidence and not matches:
                status, reason = "오래됨", "마감 확인 후 해당 날짜 원천 판본 또는 계정 범위 변경"
            elif not present and not (evidence and matches and no_transactions):
                status, reason = "누락", "필요한 원천 파일 또는 무거래 수집 완료 근거 없음"
            elif source == "logistics_dam" and count and not rows["closed"].fillna(False).all():
                status, reason = "잠정", "마감되지 않은 물류담 행"
            elif evidence and matches and (count > 0 or no_transactions):
                status, reason = "정상", evidence
            else:
                status, reason = "잠정", "날짜별 수집·마감 확인 필요" if count else "무거래 또는 미수집 여부 확인 필요"
            records.append({"date": day, "source": source, "status": status, "reason": reason,
                            "row_count": count, "source_hash": entry["sha256"], "slice_hash": current_hash,
                            "source_present": present, "scope": SCOPES[source], "evidence": evidence})
    return pd.DataFrame(records)


def coverage_candidates(quality: pd.DataFrame) -> pd.DataFrame:
    result = quality.copy()
    result["approved"] = ""
    result["confirmation_evidence"] = ""
    result["confirmed_by"] = ""
    result["no_transactions"] = ""
    return result


def import_coverage(review_path: Path, config_path: Path, *, expected_hash: str, current_quality: pd.DataFrame) -> dict:
    current = read_config(str(config_path), expected_hash)
    frame = pd.read_csv(review_path, encoding="utf-8-sig", dtype=str).fillna("")
    required = {"date", "source", "slice_hash", "scope", "approved", "confirmation_evidence", "confirmed_by", "no_transactions"}
    if not required.issubset(frame.columns):
        raise ValueError("마감 검수표 필수 컬럼 누락")
    lookup = current_quality.set_index(["date", "source"]).to_dict("index")
    changes = {}
    for row in frame.to_dict("records"):
        flag = text(row["approved"]).lower()
        if flag not in {"", "n", "false", "0", "y", "true", "1"}:
            raise ValueError("마감 승인값 오류")
        if flag not in {"y", "true", "1"}:
            continue
        key = (row["date"], row["source"])
        if key in changes or key not in lookup:
            raise ValueError("마감 검수표 날짜·원천 중복 또는 범위 오류")
        actual = lookup[key]
        if row["slice_hash"] != actual["slice_hash"] or row["scope"] != actual["scope"]:
            raise ValueError("마감 검수 이후 원천 판본 또는 계정 범위 변경")
        if not text(row["confirmation_evidence"]) or not text(row["confirmed_by"]):
            raise ValueError("마감 확인 근거와 확인자가 필요합니다")
        no_transactions = text(row["no_transactions"]).lower() in {"y", "true", "1"}
        if no_transactions != (actual["row_count"] == 0):
            raise ValueError("마감 무거래 확인과 관측 행수가 일치하지 않습니다")
        if actual["reason"] == "마감되지 않은 물류담 행":
            raise ValueError("미마감 물류담 행은 마감 확인할 수 없습니다")
        changes[key] = {"slice_hash": actual["slice_hash"], "source_hash": actual["source_hash"],
                        "scope": actual["scope"], "no_transactions": no_transactions,
                        "evidence": text(row["confirmation_evidence"]), "confirmed_by": text(row["confirmed_by"]),
                        "confirmed_at": datetime.now(timezone.utc).isoformat()}
    if changes:
        days = current.setdefault("days", {})
        for (day, source), confirmation in changes.items():
            days.setdefault(day, {})[source] = confirmation
        current["version"] = "coverage-v3"
        read_config(str(config_path), expected_hash)
        atomic_json(config_path, current)
    return {"confirmed_days_sources": len(changes)}


def source_complete(quality: pd.DataFrame, start: str, end: str, sources=SOURCE_NAMES) -> bool:
    rows = quality.loc[quality["date"].between(start, end) & quality["source"].isin(sources)]
    expected = len(pd.date_range(start, end)) * len(sources)
    return bool(len(rows) == expected and not rows.duplicated(["date", "source"]).any() and rows["status"].eq("정상").all())
