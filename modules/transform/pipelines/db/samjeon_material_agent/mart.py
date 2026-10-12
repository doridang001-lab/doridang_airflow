"""송파삼전점 원재료·직원식대 근거형 데이터마트.

Airflow 태스크 사이에는 작은 계획 딕셔너리만 전달한다. 원천은 읽기 전용이고,
중간 파일은 LOCAL_DB에 만든 뒤 검증된 스냅샷만 MART_DB에 게시한다.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum

from modules.transform.utility.paths import ANALYTICS_DB, LOCAL_DB, MART_DB


STORE = "송파삼전점"
FOOD_MAIN = "도리당 송파삼전점"
FOOD_MEAL = "도리당 송파삼전점(부식)"
DAM_STORE = "도리당 송파삼전점"
CATEGORIES = {"원재료", "포장재", "기타"}
RULE_VERSION = "samjeon-mart-v1"
DEFAULT_OUTPUT = MART_DB / "CEO_DB" / "samjeon_agent_DB"
DEFAULT_WORK = LOCAL_DB / "temp" / "samjeon_material_mart"
DEFAULT_CLASSIFICATION = LOCAL_DB / "samjeon_material_agent" / "classification.json"
DEFAULT_COVERAGE = LOCAL_DB / "samjeon_material_agent" / "coverage.json"
SOURCE_NAMES = ("sales", "food_guide", "logistics_dam")
PURCHASE_COLUMNS = [
    "purchase_date", "source", "account", "product_key", "product_code",
    "item_name", "spec", "unit", "qty", "amount_won", "category",
    "classification_status", "source_file", "source_sha256", "source_row",
    "evidence_id", "closed",
]
SALES_COLUMNS = ["sale_date", "source", "amount_won", "row_count"]


def _digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")


def _text(value: Any) -> str:
    if pd.isna(value):
        return ""
    result = str(value).strip()
    return "" if result.lower() in {"nan", "none", "nat"} else result


def _code(value: Any) -> str:
    result = _text(value)
    return result[:-2] if result.endswith(".0") else result


def _won(value: Any, *, label: str) -> int:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(f"{label}: 금액을 읽을 수 없습니다: {value!r}") from exc
    if not number.is_finite() or number != number.to_integral_value():
        raise ValueError(f"{label}: 정수 원 금액이 아닙니다: {value!r}")
    return int(number)


def _date(value: Any, *, label: str) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"{label}: 날짜를 읽을 수 없습니다: {value!r}")
    return parsed.strftime("%Y-%m-%d")


def _closed(value: Any) -> bool:
    if pd.isna(value):
        return False
    return _text(value).lower() in {"true", "1", "yes", "y"}


def _file_entry(path: Path, source: str, file_date: str) -> dict[str, Any]:
    return {
        "source": source,
        "date": file_date,
        "path": str(path),
        "sha256": _digest(path) if path.is_file() else "",
    }


def _read_checked(entry: dict[str, Any], required: list[str]) -> pd.DataFrame:
    path = Path(entry["path"])
    if not entry["sha256"]:
        raise FileNotFoundError(path)
    if _digest(path) != entry["sha256"]:
        raise RuntimeError(f"읽기 전 원천 판본 변경: {path}")
    frame = pd.read_parquet(path)
    if _digest(path) != entry["sha256"]:
        raise RuntimeError(f"읽는 중 원천 판본 변경: {path}")
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"{path.name}: 필수 컬럼 누락 {missing}")
    return frame


def _month_names(start: date, end: date) -> list[str]:
    months = []
    cursor = start.replace(day=1)
    while cursor <= end:
        months.append(cursor.strftime("%Y%m"))
        cursor = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
    return months


def plan_build(
    as_of_date: str | None = None,
    *,
    sales_root: str | Path | None = None,
    food_root: str | Path | None = None,
    dam_root: str | Path | None = None,
    output_root: str | Path | None = None,
    work_root: str | Path | None = None,
    classification_path: str | Path | None = None,
    coverage_path: str | Path | None = None,
) -> dict[str, Any]:
    """기준일과 읽을 파일을 확정한다. 디렉터리·파일은 만들지 않는다."""
    target = date.fromisoformat(as_of_date) if as_of_date else pendulum.now("Asia/Seoul").date() - timedelta(days=1)
    if target >= pendulum.now("Asia/Seoul").date():
        raise ValueError("기준일은 한국시간 전일 이전이어야 합니다")
    start = target - timedelta(days=56)
    sales_dir = Path(sales_root) if sales_root else MART_DB / "unified_sales_grp"
    food_dir = Path(food_root) if food_root else ANALYTICS_DB / "Food_Guide_Raw"
    dam_dir = Path(dam_root) if dam_root else ANALYTICS_DB / "Logistics_Dam"
    class_path = Path(classification_path) if classification_path else DEFAULT_CLASSIFICATION
    close_path = Path(coverage_path) if coverage_path else DEFAULT_COVERAGE
    days = [(start + timedelta(days=i)).isoformat() for i in range(57)]
    sales = [_file_entry(sales_dir / f"unified_sales_{day[2:].replace('-', '')}.parquet", "sales", day) for day in days]
    food = [_file_entry(food_dir / f"food_guide_orders_{month}.parquet", "food_guide", month) for month in _month_names(start, target)]
    dam = [_file_entry(dam_dir / f"Dam_order_details_{day[2:].replace('-', '')}.parquet", "logistics_dam", day) for day in days]
    inputs = sales + food + dam
    config = {"classification": _digest(class_path) if class_path.is_file() else "",
              "coverage": _digest(close_path) if close_path.is_file() else ""}
    signature = hashlib.sha256(_json_bytes({"date": str(target), "files": inputs, "config": config, "rule": RULE_VERSION})).hexdigest()[:16]
    snapshot_id = f"{target}-{signature}"
    work = Path(work_root) if work_root else DEFAULT_WORK
    return {
        "as_of_date": str(target), "start_date": str(start), "days": days,
        "sales_files": sales, "food_files": food, "dam_files": dam,
        "config_hashes": config, "classification_path": str(class_path),
        "coverage_path": str(close_path), "snapshot_id": snapshot_id,
        "stage_dir": str(work / snapshot_id),
        "output_root": str(Path(output_root) if output_root else DEFAULT_OUTPUT),
    }


def _product_key(source: str, code: str, name: str, spec: str, unit: str) -> str:
    return hashlib.sha256(_json_bytes([source, code, name, spec, unit])).hexdigest()[:20]


def _purchase_record(
    entry: dict[str, Any], row_number: int, day: str, source: str, account: str,
    code: str, name: str, spec: str, unit: str, qty: Any, amount: Any, closed: bool,
) -> dict[str, Any]:
    key = _product_key(source, code, name, spec, unit)
    evidence = hashlib.sha256(_json_bytes([source, entry["path"], entry["sha256"], row_number])).hexdigest()[:24]
    return {
        "purchase_date": day, "source": source, "account": account,
        "product_key": key, "product_code": code, "item_name": name,
        "spec": spec, "unit": unit, "qty": str(qty) if not pd.isna(qty) else "",
        "amount_won": _won(amount, label=evidence), "category": "",
        "classification_status": "", "source_file": entry["path"],
        "source_sha256": entry["sha256"], "source_row": row_number,
        "evidence_id": evidence, "closed": closed,
    }


def load_sales(plan: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, int]]:
    rows = []
    counts = {}
    for entry in plan["sales_files"]:
        if not entry["sha256"]:
            continue
        frame = _read_checked(entry, ["sale_date", "store", "source", "total_price", "_pk"])
        frame = frame.loc[frame["store"].fillna("").astype(str).str.strip().eq(STORE)].copy()
        if frame.empty:
            counts[entry["date"]] = 0
            continue
        if frame["sale_date"].astype(str).ne(entry["date"]).any():
            raise ValueError(f"{entry['path']}: 파일 날짜와 삼전점 sale_date 불일치")
        frame["_pk"] = frame["_pk"].map(_text)
        if frame["_pk"].eq("").any():
            raise ValueError(f"{entry['path']}: 삼전점 _pk 누락")
        rows.append(frame)
        counts[entry["date"]] = len(frame)
    if rows:
        combined = pd.concat(rows, ignore_index=True)
        duplicated = combined.loc[combined["_pk"].duplicated(keep=False)]
        if not duplicated.empty:
            if any(len(part.astype(str).drop_duplicates()) > 1 for _, part in duplicated.groupby("_pk", sort=False)):
                raise ValueError("동일 _pk의 상충 매출 행")
            combined = combined.drop_duplicates("_pk", keep="first")
        combined["amount_won"] = [_won(v, label="매출") for v in combined["total_price"]]
        result = (combined.groupby(["sale_date", "source"], dropna=False)
                  .agg(amount_won=("amount_won", "sum"), row_count=("_pk", "size"))
                  .reset_index())
        result["source"] = result["source"].map(_text)
        result = result[SALES_COLUMNS]
    else:
        result = pd.DataFrame(columns=SALES_COLUMNS)
    return result, counts


def load_purchases(plan: dict[str, Any]) -> pd.DataFrame:
    start, end = plan["start_date"], plan["as_of_date"]
    records: list[dict[str, Any]] = []
    for entry in plan["food_files"]:
        if not entry["sha256"]:
            continue
        frame = _read_checked(entry, ["배송일", "사업장", "상품코드", "상품명", "규격", "단위", "수량", "합계"])
        for row_number, (_, row) in enumerate(frame.iterrows()):
            account = _text(row["사업장"])
            if account not in {FOOD_MAIN, FOOD_MEAL}:
                continue
            day = _date(row["배송일"], label="푸드가이드 배송일")
            if not start <= day <= end:
                continue
            records.append(_purchase_record(
                entry, row_number, day, "food_guide", account,
                _code(row["상품코드"]), _text(row["상품명"]), _text(row["규격"]),
                _text(row["단위"]), row["수량"], row["합계"], True,
            ))
    for entry in plan["dam_files"]:
        if not entry["sha256"]:
            continue
        frame = _read_checked(entry, ["sale_date", "배송일자", "거래처명", "코드", "품명", "규격", "단위", "수량", "금액", "마감됨"])
        for row_number, (_, row) in enumerate(frame.iterrows()):
            if _text(row["거래처명"]) != DAM_STORE:
                continue
            day = _date(row["배송일자"], label="물류담 배송일자")
            if day != _date(row["sale_date"], label="물류담 sale_date") or day != entry["date"]:
                raise ValueError(f"{entry['path']}: 물류담 배송일자·sale_date·파일 날짜 불일치")
            records.append(_purchase_record(
                entry, row_number, day, "logistics_dam", DAM_STORE,
                _code(row["코드"]), _text(row["품명"]), _text(row["규격"]),
                _text(row["단위"]), row["수량"], row["금액"], _closed(row["마감됨"]),
            ))
    return pd.DataFrame(records, columns=PURCHASE_COLUMNS)


def _read_config(path_text: str, expected_hash: str) -> dict[str, Any]:
    path = Path(path_text)
    if not expected_hash:
        if path.exists():
            raise RuntimeError(f"계획 이후 설정 파일 생성: {path}")
        return {}
    if _digest(path) != expected_hash:
        raise RuntimeError(f"계획 이후 설정 파일 변경: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"설정 파일의 최상위 값은 객체여야 합니다: {path}")
    return value


def _classification(plan: dict[str, Any]) -> tuple[dict[tuple[str, str, str, str, str], str], str]:
    config = _read_config(plan["classification_path"], plan["config_hashes"]["classification"])
    mapping = {}
    for item in config.get("items", []):
        key = tuple(_text(item.get(field)) for field in
                    ("source", "product_code", "item_name", "spec", "unit"))
        category = _text(item.get("category"))
        if category not in CATEGORIES:
            raise ValueError(f"품목 분류값 오류: {key} → {category}")
        if key in mapping and mapping[key] != category:
            raise ValueError(f"품목 분류 키 충돌: {key}")
        mapping[key] = category
    return mapping, _text(config.get("version"))


def _classify(lines: pd.DataFrame, mapping: dict[tuple[str, str, str, str, str], str]) -> pd.DataFrame:
    lines = lines.copy()
    for index, row in lines.iterrows():
        if row["source"] == "food_guide" and row["account"] == FOOD_MEAL:
            lines.at[index, "category"] = "현장 직원식대"
            lines.at[index, "classification_status"] = "계정확정"
            continue
        key = (row["source"], row["product_code"], row["item_name"], row["spec"], row["unit"])
        category = mapping.get(key)
        lines.at[index, "category"] = category or "미분류"
        lines.at[index, "classification_status"] = "검수완료" if category else "검수필요"
    return lines


def _quality(plan: dict[str, Any], sales: pd.DataFrame, lines: pd.DataFrame) -> pd.DataFrame:
    coverage = _read_config(plan["coverage_path"], plan["config_hashes"]["coverage"])
    records = []
    sales_counts = sales.groupby("sale_date")["row_count"].sum().to_dict() if not sales.empty else {}
    purchase_counts = lines.groupby(["purchase_date", "source"]).size().to_dict() if not lines.empty else {}
    sales_files = {entry["date"]: entry for entry in plan["sales_files"]}
    food_files = {entry["date"]: entry for entry in plan["food_files"]}
    dam_files = {entry["date"]: entry for entry in plan["dam_files"]}
    for day in plan["days"]:
        entries = {
            "sales": sales_files[day],
            "food_guide": food_files[day[:4] + day[5:7]],
            "logistics_dam": dam_files[day],
        }
        for source, entry in entries.items():
            count = int(sales_counts.get(day, 0)) if source == "sales" else int(purchase_counts.get((day, source), 0))
            confirmation = coverage.get("days", {}).get(day, {}).get(source, {})
            evidence = _text(confirmation.get("evidence"))
            confirmed_hash = _text(confirmation.get("source_hash"))
            source_hash = entry["sha256"]
            if not source_hash and source != "logistics_dam":
                status, reason = "누락", "필요한 원천 파일 없음"
            elif source == "logistics_dam" and count and not lines.loc[
                (lines["purchase_date"] == day) & (lines["source"] == source), "closed"
            ].all():
                status, reason = "잠정", "마감되지 않은 물류담 행"
            elif evidence and confirmed_hash != source_hash:
                status, reason = "오래됨", "마감 확인 후 원천 판본 변경"
            elif evidence and confirmed_hash == source_hash:
                status, reason = "정상", evidence
            else:
                status, reason = "잠정", "날짜별 수집·마감 확인 필요" if count else "무거래 또는 미수집 여부 확인 필요"
            records.append({
                "date": day, "source": source, "status": status, "reason": reason,
                "row_count": count, "source_hash": source_hash,
                "evidence": evidence,
            })
    return pd.DataFrame(records)


def _aggregate_tables(
    plan: dict[str, Any], sales: pd.DataFrame, purchases: pd.DataFrame,
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    mapping, classification_version = _classification(plan)
    lines = _classify(purchases, mapping)
    coverage = _quality(plan, sales, lines)
    if lines.empty:
        purchase_daily = pd.DataFrame(columns=["purchase_date", "source", "category", "amount_won", "row_count"])
        item_daily = pd.DataFrame(columns=["purchase_date", "source", "account", "category", "product_key",
                                           "product_code", "item_name", "spec", "unit", "amount_won", "row_count"])
        review = pd.DataFrame(columns=["source", "product_key", "product_code", "item_name", "spec", "unit",
                                       "amount_won", "row_count"])
    else:
        purchase_daily = (lines.groupby(["purchase_date", "source", "category"], dropna=False)
                          .agg(amount_won=("amount_won", "sum"), row_count=("evidence_id", "size"))
                          .reset_index())
        item_daily = (lines.groupby(["purchase_date", "source", "account", "category", "product_key",
                                     "product_code", "item_name", "spec", "unit"], dropna=False)
                      .agg(amount_won=("amount_won", "sum"), row_count=("evidence_id", "size"))
                      .reset_index())
        unknown = lines.loc[lines["category"].eq("미분류")]
        review = (unknown.groupby(["source", "product_key", "product_code", "item_name", "spec", "unit"],
                                  dropna=False)
                  .agg(amount_won=("amount_won", "sum"), row_count=("evidence_id", "size"))
                  .reset_index().sort_values("amount_won", ascending=False))
    tables = {
        "purchase_lines": lines,
        "sales_daily": sales,
        "purchase_daily": purchase_daily,
        "item_daily": item_daily,
        "quality_daily": coverage,
    }
    context = _agent_context(plan, tables, classification_version)
    tables["review_candidates"] = review
    return tables, context


def _sum(frame: pd.DataFrame, *, start: str, end: str, date_col: str, categories: set[str] | None = None) -> int:
    if frame.empty:
        return 0
    selected = frame.loc[frame[date_col].between(start, end)]
    if categories is not None:
        selected = selected.loc[selected["category"].isin(categories)]
    return int(selected["amount_won"].sum())


def _agent_context(plan: dict[str, Any], tables: dict[str, pd.DataFrame], classification_version: str) -> dict[str, Any]:
    end = date.fromisoformat(plan["as_of_date"])
    month = end.replace(day=1).isoformat()
    today = end.isoformat()
    sales, purchase = tables["sales_daily"], tables["purchase_daily"]
    quality, items = tables["quality_daily"], tables["item_daily"]
    material = {"원재료", "포장재"}

    def period(start: str, finish: str) -> dict[str, Any]:
        total_sales = _sum(sales, start=start, end=finish, date_col="sale_date")
        amounts = {
            category: _sum(purchase, start=start, end=finish, date_col="purchase_date", categories={category})
            for category in ("원재료", "포장재", "현장 직원식대", "기타", "미분류")
        }
        statuses = quality.loc[quality["date"].between(start, finish), "status"]
        complete = bool(len(statuses)) and statuses.eq("정상").all() and amounts["미분류"] == 0
        purchase_amount = amounts["원재료"] + amounts["포장재"]
        rate = None
        gap = None
        excess = None
        if complete and total_sales > 0:
            ratio = Decimal(purchase_amount) / Decimal(total_sales)
            rate = str((ratio * 100).quantize(Decimal("0.1")))
            gap = str(((ratio - Decimal("0.37")) * 100).quantize(Decimal("0.1")))
            excess = str(Decimal(purchase_amount) - Decimal(total_sales) * Decimal("0.37"))
        return {
            "observed_sales_won": total_sales, "observed_purchase_won": purchase_amount,
            "ingredient_won": amounts["원재료"], "packaging_won": amounts["포장재"],
            "staff_meal_won": amounts["현장 직원식대"], "other_won": amounts["기타"],
            "unclassified_won": amounts["미분류"], "complete": complete,
            "purchase_rate_pct": rate, "target_gap_pp": gap,
            "target_excess_purchase_won": excess,
        }

    daily = period(today, today)
    mtd = period(month, today)
    recent_start = (end - timedelta(days=6)).isoformat()
    prior_start = (end - timedelta(days=13)).isoformat()
    prior_end = (end - timedelta(days=7)).isoformat()
    recent = period(recent_start, today)
    prior = period(prior_start, prior_end)
    comparison_ready = recent["complete"] and prior["complete"]
    top_items = []
    if comparison_ready and not items.empty:
        candidates = items.loc[items["category"].isin(material)].copy()
        now = candidates.loc[candidates["purchase_date"].between(recent_start, today)]
        before = candidates.loc[candidates["purchase_date"].between(prior_start, prior_end)]
        keys = ["source", "product_key", "item_name"]
        current = now.groupby(keys)["amount_won"].sum()
        previous = before.groupby(keys)["amount_won"].sum()
        for key, amount in current.sub(previous, fill_value=0).sort_values(ascending=False).head(3).items():
            if amount > 0:
                top_items.append({"source": key[0], "product_key": key[1],
                                  "item_name": key[2], "increase_won": int(amount)})
    issues = quality.loc[
        quality["date"].between(month, today) & quality["status"].ne("정상"),
        ["date", "source", "status", "reason"],
    ].tail(12).to_dict("records")
    if mtd["unclassified_won"]:
        issues.append({"status": "잠정", "reason": "미분류 구매 금액 존재",
                       "amount_won": mtd["unclassified_won"]})
    alerts = []
    if comparison_ready and prior["observed_purchase_won"] > 0:
        increase = recent["observed_purchase_won"] - prior["observed_purchase_won"]
        if increase >= 100000 and Decimal(increase) / Decimal(prior["observed_purchase_won"]) >= Decimal("0.30"):
            alerts.append({"type": "seven_day_purchase_increase", "increase_won": increase,
                           "previous_won": prior["observed_purchase_won"]})
    if daily["complete"]:
        samples = []
        for week in range(1, 9):
            historic = (end - timedelta(days=week * 7)).isoformat()
            historic_quality = quality.loc[quality["date"].eq(historic), "status"]
            unknown = _sum(purchase, start=historic, end=historic, date_col="purchase_date",
                           categories={"미분류"})
            if len(historic_quality) == len(SOURCE_NAMES) and historic_quality.eq("정상").all() and unknown == 0:
                samples.append(_sum(purchase, start=historic, end=historic, date_col="purchase_date",
                                    categories=material))
        if len(samples) >= 4:
            baseline = Decimal(sum(samples)) / Decimal(len(samples))
            increase = Decimal(daily["observed_purchase_won"]) - baseline
            if baseline > 0 and increase >= 100000 and increase / baseline >= Decimal("0.30"):
                alerts.append({"type": "weekday_purchase_increase", "date": today,
                               "increase_won": int(increase), "sample_count": len(samples)})
    if mtd["complete"] and mtd["target_gap_pp"] is not None and Decimal(mtd["target_gap_pp"]) > 0:
        alerts.append({"type": "purchase_target_exceeded", "target_gap_pp": mtd["target_gap_pp"]})
    forecast = _forecast(plan, sales, purchase, quality, mtd["complete"])
    month_quality = quality.loc[quality["date"].between(month, today)]
    overall = "누락" if month_quality["status"].eq("누락").any() else (
        "오래됨" if month_quality["status"].eq("오래됨").any() else (
            "정상" if mtd["complete"] else "잠정"))
    return {
        "as_of_date": today, "window_start": plan["start_date"],
        "snapshot_id": plan["snapshot_id"], "rule_version": RULE_VERSION,
        "classification_version": classification_version,
        "status": overall,
        "daily": daily, "month_to_date": mtd,
        "seven_day": {
            "current_observed_purchase_won": recent["observed_purchase_won"],
            "previous_observed_purchase_won": prior["observed_purchase_won"],
            "complete": comparison_ready,
            "increase_won": recent["observed_purchase_won"] - prior["observed_purchase_won"]
            if comparison_ready else None,
        },
        "top_increase_items": top_items,
        "alerts": alerts,
        "forecast": forecast,
        "issues": issues[-3:],
    }


def _forecast(plan: dict[str, Any], sales: pd.DataFrame, purchase: pd.DataFrame,
              quality: pd.DataFrame, mtd_complete: bool) -> dict[str, Any]:
    """완료된 동일 요일의 최근 8주 매출·매입으로 월말을 전망한다."""
    end = date.fromisoformat(plan["as_of_date"])
    month_end = (end.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    if end == month_end:
        return {"status": "확정일", "reason": "월말 이후에는 월 실적을 사용"}
    if not mtd_complete:
        return {"status": "보류", "reason": "월 누적 매출·매입의 마감 또는 분류 미완료"}
    daily_sales = sales.groupby("sale_date")["amount_won"].sum().to_dict() if not sales.empty else {}
    counted = purchase.loc[purchase["category"].isin({"원재료", "포장재"})]
    daily_purchase = counted.groupby("purchase_date")["amount_won"].sum().to_dict() if not counted.empty else {}
    unknown = purchase.loc[purchase["category"].eq("미분류")]
    unknown_days = set(unknown["purchase_date"])
    statuses = quality.pivot(index="date", columns="source", values="status")
    forecast_days = []
    for offset in range(1, (month_end - end).days + 1):
        future = end + timedelta(days=offset)
        sales_samples = []
        purchase_samples = []
        for week in range(1, 9):
            historic = future - timedelta(days=week * 7)
            key = historic.isoformat()
            if key >= future.isoformat() or key not in statuses.index or key in unknown_days:
                continue
            if statuses.loc[key].eq("정상").all() and key in daily_sales:
                sales_samples.append(int(daily_sales[key]))
                purchase_samples.append(int(daily_purchase.get(key, 0)))
        if len(sales_samples) < 4:
            return {"status": "보류", "reason": "잔여일의 동일 요일 마감 표본 4개 미만",
                    "date": future.isoformat(), "sample_count": len(sales_samples)}
        forecast_days.append({
            "date": future.isoformat(), "sample_count": len(sales_samples),
            "expected_sales_won": int(round(sum(sales_samples) / len(sales_samples))),
            "expected_purchase_won": int(round(sum(purchase_samples) / len(purchase_samples))),
        })
    month = end.replace(day=1).isoformat()
    projected_sales = _sum(sales, start=month, end=end.isoformat(), date_col="sale_date") + sum(x["expected_sales_won"] for x in forecast_days)
    projected_purchase = _sum(purchase, start=month, end=end.isoformat(), date_col="purchase_date",
                              categories={"원재료", "포장재"}) + sum(x["expected_purchase_won"] for x in forecast_days)
    if projected_sales <= 0:
        return {"status": "보류", "reason": "예상 월매출 0 이하"}
    return {
        "status": "정상", "assumption": "최근 패턴 유지", "method": "최근 8주 동일 요일 마감 매출·매입 평균",
        "days": forecast_days, "projected_month_sales_won": projected_sales,
        "projected_month_purchase_won": projected_purchase,
        "projected_purchase_rate_pct": str((Decimal(projected_purchase) / Decimal(projected_sales) * 100).quantize(Decimal("0.1"))),
        "remaining_purchase_capacity_won": str(Decimal(projected_sales) * Decimal("0.37") -
                                                Decimal(projected_purchase - sum(x["expected_purchase_won"] for x in forecast_days))),
    }


def _stage(plan: dict[str, Any]) -> Path:
    stage = Path(plan["stage_dir"])
    stage.mkdir(parents=True, exist_ok=True)
    return stage


def stage_sales(plan: dict[str, Any]) -> dict[str, Any]:
    sales, counts = load_sales(plan)
    path = _stage(plan) / "sales_daily.parquet"
    sales.to_parquet(path, index=False)
    return {"rows": len(sales), "source_rows": sum(counts.values()), "sha256": _digest(path)}


def stage_purchases(plan: dict[str, Any]) -> dict[str, Any]:
    purchases = load_purchases(plan)
    path = _stage(plan) / "purchase_lines_raw.parquet"
    purchases.to_parquet(path, index=False)
    return {"rows": len(purchases), "sha256": _digest(path)}


OUTPUT_TABLES = ("purchase_lines", "sales_daily", "purchase_daily", "item_daily", "quality_daily")
OUTPUT_NAMES = tuple(f"{name}.parquet" for name in OUTPUT_TABLES) + (
    "review_candidates.csv", "agent_context.json", "manifest.json")


def _check_source_versions(plan: dict[str, Any]) -> None:
    for entry in plan["sales_files"] + plan["food_files"] + plan["dam_files"]:
        path = Path(entry["path"])
        actual = _digest(path) if path.is_file() else ""
        if actual != entry["sha256"]:
            raise RuntimeError(f"원천 판본 변경으로 게시 중단: {path}")
    for name, path_key in (("classification", "classification_path"), ("coverage", "coverage_path")):
        _read_config(plan[path_key], plan["config_hashes"][name])


def stage_aggregate(plan: dict[str, Any]) -> dict[str, Any]:
    stage = _stage(plan)
    sales = pd.read_parquet(stage / "sales_daily.parquet")
    purchases = pd.read_parquet(stage / "purchase_lines_raw.parquet")
    tables, context = _aggregate_tables(plan, sales, purchases)
    for name in OUTPUT_TABLES:
        tables[name].to_parquet(stage / f"{name}.parquet", index=False)
    tables["review_candidates"].to_csv(stage / "review_candidates.csv", index=False, encoding="utf-8-sig")
    (stage / "agent_context.json").write_bytes(_json_bytes(context))
    files = {name: _digest(stage / name) for name in OUTPUT_NAMES if name != "manifest.json"}
    manifest = {
        "snapshot_id": plan["snapshot_id"], "as_of_date": plan["as_of_date"],
        "window_start": plan["start_date"], "rule_version": RULE_VERSION,
        "sources": plan["sales_files"] + plan["food_files"] + plan["dam_files"],
        "config_hashes": plan["config_hashes"], "files": files,
        "rows": {name: len(tables[name]) for name in OUTPUT_TABLES},
        "status": context["status"],
    }
    (stage / "manifest.json").write_bytes(_json_bytes(manifest))
    return {"snapshot_id": plan["snapshot_id"], "status": context["status"], "rows": manifest["rows"]}


def validate_snapshot(plan: dict[str, Any]) -> dict[str, Any]:
    _check_source_versions(plan)
    stage = Path(plan["stage_dir"])
    manifest = json.loads((stage / "manifest.json").read_text(encoding="utf-8"))
    if manifest["snapshot_id"] != plan["snapshot_id"]:
        raise ValueError("스냅샷 ID 불일치")
    for name, expected in manifest["files"].items():
        if _digest(stage / name) != expected:
            raise ValueError(f"스냅샷 파일 해시 불일치: {name}")
    lines = pd.read_parquet(stage / "purchase_lines.parquet")
    purchase_daily = pd.read_parquet(stage / "purchase_daily.parquet")
    item_daily = pd.read_parquet(stage / "item_daily.parquet")
    sales_daily = pd.read_parquet(stage / "sales_daily.parquet")
    quality = pd.read_parquet(stage / "quality_daily.parquet")
    if len(quality) != 57 * len(SOURCE_NAMES) or quality.duplicated(["date", "source"]).any():
        raise ValueError("품질 날짜·원천 매트릭스가 불완전합니다")
    if len(lines) != lines["evidence_id"].nunique():
        raise ValueError("구매 근거 ID 중복")
    if not lines.empty:
        if lines["purchase_date"].gt(plan["as_of_date"]).any():
            raise ValueError("기준일 이후 구매가 포함됐습니다")
        if not lines.loc[lines["account"].eq(FOOD_MEAL), "category"].eq("현장 직원식대").all():
            raise ValueError("부식 계정 분류 오류")
    if not sales_daily.empty and sales_daily["sale_date"].gt(plan["as_of_date"]).any():
        raise ValueError("기준일 이후 매출이 포함됐습니다")
    if int(lines["amount_won"].sum()) != int(purchase_daily["amount_won"].sum()) or int(lines["amount_won"].sum()) != int(item_daily["amount_won"].sum()):
        raise ValueError("구매 합계 대사 실패")
    return {"snapshot_id": plan["snapshot_id"], "status": manifest["status"],
            "purchase_rows": len(lines), "sales_rows": int(sales_daily["row_count"].sum())}


def publish_snapshot(plan: dict[str, Any]) -> dict[str, Any]:
    """검증 완료 스냅샷을 한 폴더로 게시하고, 최신 포인터는 마지막에 교체한다."""
    summary = validate_snapshot(plan)
    stage = Path(plan["stage_dir"])
    root = Path(plan["output_root"])
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
            os.replace(pending, target)
        except Exception:
            # 실패한 임시 디렉터리는 증거로 남기고 최신 포인터는 유지한다.
            raise
    else:
        for name in OUTPUT_NAMES:
            if _digest(target / name) != _digest(stage / name):
                raise RuntimeError(f"같은 ID의 기존 스냅샷과 내용이 다릅니다: {name}")
    yesterday = pendulum.now("Asia/Seoul").date() - timedelta(days=1)
    if plan["as_of_date"] == yesterday.isoformat():
        pointer = {"snapshot_id": plan["snapshot_id"], "as_of_date": plan["as_of_date"],
                   "manifest": f"snapshots/{plan['snapshot_id']}/manifest.json"}
        tmp = root / f".latest.{uuid.uuid4().hex}.tmp"
        tmp.write_bytes(_json_bytes(pointer))
        os.replace(tmp, root / "latest.json")
        summary["latest_updated"] = True
    else:
        summary["latest_updated"] = False
    return summary
