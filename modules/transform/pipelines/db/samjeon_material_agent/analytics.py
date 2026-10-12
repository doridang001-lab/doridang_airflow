"""확정·잠정 지표와 비교·전망. 숫자 판정은 반올림 전에 수행한다."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pandas as pd

from modules.transform.pipelines.db.samjeon_material_agent.contracts import (
    ALL_CATEGORIES, RULE_VERSION, SCHEMA_VERSION, SOURCE_NAMES, percent, rounded_won,
)
from modules.transform.pipelines.db.samjeon_material_agent.quality import source_complete
from modules.transform.pipelines.db.samjeon_material_agent.settings import validate_settings

MATERIAL = {"원재료", "포장재"}
PURCHASE_SOURCES = ("food_guide", "logistics_dam")


def amount(frame, start, end, date_col, categories=None) -> int:
    rows = frame.loc[frame[date_col].between(start, end)]
    if categories is not None:
        rows = rows.loc[rows["category"].isin(categories)]
    return int(rows["amount_won"].sum())


def unknown_rows(purchase, start, end) -> int:
    rows = purchase.loc[purchase["purchase_date"].between(start, end) & purchase["category"].eq("미분류")]
    return int(rows["row_count"].sum()) if "row_count" in rows else len(rows)


def period_result(start: str, end: str, tables: dict, settings: dict) -> dict:
    sales, purchases, quality = (tables[name] for name in ("sales_daily", "purchase_daily", "quality_daily"))
    total = amount(sales, start, end, "sale_date")
    hall = amount(sales.loc[sales["source"].eq("okpos")], start, end, "sale_date")
    values = {category: amount(purchases, start, end, "purchase_date", {category}) for category in ALL_CATEGORIES}
    unknown_count = unknown_rows(purchases, start, end)
    unknown = tables.get("purchase_lines", purchases)
    unknown = unknown.loc[unknown["purchase_date"].between(start, end) & unknown["category"].eq("미분류")]
    sales_complete = source_complete(quality, start, end, ("sales",))
    purchases_complete = source_complete(quality, start, end, PURCHASE_SOURCES) and unknown_count == 0
    meal_complete = source_complete(quality, start, end, ("food_guide",))
    complete = bool(sales_complete and purchases_complete)
    selected_quality = quality.loc[quality["date"].between(start, end)]
    expected = len(pd.date_range(start, end)) * len(SOURCE_NAMES)
    statuses = set(selected_quality["status"])
    status = next((s for s in ("검증 실패", "누락", "오래됨") if s in statuses), "정상" if complete else "잠정")
    if len(selected_quality) != expected:
        status = "누락"
    purchase_won = values["원재료"] + values["포장재"]
    expected_pairs = {(str(day.date()), source) for day in pd.date_range(start, end) for source in SOURCE_NAMES}
    matrix_valid = (len(selected_quality) == expected and
                    set(zip(selected_quality["date"], selected_quality["source"])) == expected_pairs)
    provisional_ready = matrix_valid and statuses <= {"정상", "잠정"} and unknown_count == 0
    ratio = Decimal(purchase_won) / Decimal(total) if total > 0 else None
    rate = percent(ratio * 100) if complete and ratio is not None else None
    provisional = percent(ratio * 100) if not complete and provisional_ready and ratio is not None else None
    effective = settings["target_start_date"]
    applicable = bool(effective and start >= effective and settings["amount_basis_confirmed"])
    target = Decimal(settings["target_rate"])
    reasons = []
    if not complete:
        reasons.append("기간 내 원천 수집·마감 또는 분류 미완료")
    if unknown_count:
        reasons.append(f"미분류 {unknown_count}행: 순금액이 0이어도 검수 필요")
    if total <= 0:
        reasons.append("매출 0 이하: 매입률 계산 불가")
    if purchase_won < 0:
        reasons.append("음수 순매입: 반품 가능성 확인 필요")
    if not applicable:
        reasons.append("목표 적용일 이전이거나 목표 적용일·금액 기준 미확인: 참고 비교")
    return {
        "date_from": start, "date_to": end, "status": status, "complete": complete,
        "sales_complete": bool(sales_complete), "purchase_complete": bool(purchases_complete),
        "staff_meal_complete": bool(meal_complete), "observed_sales_won": total, "hall_sales_won": hall,
        "observed_purchase_won": purchase_won, "ingredient_won": values["원재료"],
        "packaging_won": values["포장재"], "staff_meal_won": values["현장 직원식대"],
        "other_won": values["기타"], "unclassified_won": values["미분류"],
        "unclassified_row_count": unknown_count, "unclassified_absolute_amount_won": int(unknown["amount_won"].abs().sum()),
        "purchase_rate_pct": rate, "provisional_purchase_rate_pct": provisional,
        "target_rate_pct": percent(target * 100), "target_applicable": applicable,
        "target_comparison_status": "보류" if rate is None else "공식" if applicable else "참고",
        "target_gap_pp": percent((ratio - target) * 100) if rate is not None else None,
        "target_excess_purchase_won": rounded_won(Decimal(purchase_won) - Decimal(total) * target) if rate is not None else None,
        "target_exceeded": bool(ratio > target) if rate is not None and applicable else None,
        "reasons": reasons,
        "evidence": {"sales": "sales_lines.parquet", "purchases": "purchase_lines.parquet",
                     "quality": "quality_daily.parquet", "date_from": start, "date_to": end},
    }


def compare_periods(current: dict, previous: dict, field: str, complete_field: str) -> dict:
    ready = bool(current[complete_field] and previous[complete_field])
    now, before = current[field], previous[field]
    change = now - before
    reasons = []
    if not ready:
        reasons.append("비교 기간 자료 미완료")
    elif before <= 0:
        reasons.append("비교 기간 금액 0 이하")
    if now < 0 or before < 0:
        reasons.append("음수 순매입·식대: 반품 또는 환불 가능성 확인 필요")
    return {"date_from": current["date_from"], "date_to": current["date_to"],
            "previous_date_from": previous["date_from"], "previous_date_to": previous["date_to"],
            "current_observed_purchase_won": now, "previous_observed_purchase_won": before,
            "complete": ready, "increase_won": change if ready else None,
            "increase_rate_pct": percent(Decimal(change) / Decimal(before) * 100) if ready and before > 0 else None,
            "reason": "; ".join(reasons)}


def forecast(plan: dict, sales: pd.DataFrame, purchase: pd.DataFrame, quality: pd.DataFrame,
             mtd_complete: bool, settings: dict | None = None) -> dict:
    settings = validate_settings(settings or {})
    end = date.fromisoformat(plan["as_of_date"])
    month = end.replace(day=1).isoformat()
    month_end = (end.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    # 호출자의 완료 플래그뿐 아니라 입력 기간의 마감·분류도 다시 확인한다.
    complete = bool(mtd_complete and source_complete(quality, month, str(end)) and unknown_rows(purchase, month, str(end)) == 0)
    if not complete:
        return {"status": "보류", "reason": "월 누적 매출·매입의 마감 또는 분류 미완료"}
    mtd_sales = amount(sales, month, str(end), "sale_date")
    mtd_purchase = amount(purchase, month, str(end), "purchase_date", MATERIAL)
    if end == month_end:
        return {"status": "실적", "reason": "월말 마감 완료: 전망 대신 월 실적",
                "actual_month_sales_won": mtd_sales, "actual_month_purchase_won": mtd_purchase,
                "actual_purchase_rate_pct": percent(Decimal(mtd_purchase) / Decimal(mtd_sales) * 100) if mtd_sales > 0 else None}
    first = end - timedelta(days=55)
    pools = {weekday: {"sales": [], "purchase": []} for weekday in range(7)}
    for offset in range(56):
        day = first + timedelta(days=offset)
        key = str(day)
        if source_complete(quality, key, key, ("sales",)):
            pools[day.weekday()]["sales"].append((key, amount(sales, key, key, "sale_date")))
        if source_complete(quality, key, key, PURCHASE_SOURCES) and unknown_rows(purchase, key, key) == 0:
            pools[day.weekday()]["purchase"].append((key, amount(purchase, key, key, "purchase_date", MATERIAL)))
    forecast_days = []
    for offset in range(1, (month_end - end).days + 1):
        future = end + timedelta(days=offset)
        samples = pools[future.weekday()]
        counts = {key: len(values) for key, values in samples.items()}
        if min(counts.values()) < settings["minimum_weekday_samples"]:
            return {"status": "보류", "reason": "잔여일의 동일 요일 마감 표본 4개 이상 필요",
                    "date": str(future), "sample_counts": counts}
        forecast_days.append({"date": str(future), "sample_count": min(counts.values()),
                              "sales_sample_count": counts["sales"], "purchase_sample_count": counts["purchase"],
                              "sales_sample_dates": [d for d, _ in samples["sales"]],
                              "purchase_sample_dates": [d for d, _ in samples["purchase"]],
                              "expected_sales_won": rounded_won(Decimal(sum(v for _, v in samples["sales"])) / counts["sales"]),
                              "expected_purchase_won": rounded_won(Decimal(sum(v for _, v in samples["purchase"])) / counts["purchase"])})
    projected_sales = mtd_sales + sum(row["expected_sales_won"] for row in forecast_days)
    projected_purchase = mtd_purchase + sum(row["expected_purchase_won"] for row in forecast_days)
    if projected_sales <= 0:
        return {"status": "보류", "reason": "예상 월매출 0 이하"}
    applicable = bool(settings["target_start_date"] and month >= settings["target_start_date"] and settings["amount_basis_confirmed"])
    return {"status": "정상", "assumption": "최근 패턴 유지", "method": "기준일 이전 최근 8주 동일 요일 평균",
            "sample_window_start": str(first), "sample_window_end": str(end), "days": forecast_days,
            "projected_month_sales_won": projected_sales, "projected_month_purchase_won": projected_purchase,
            "projected_purchase_rate_pct": percent(Decimal(projected_purchase) / Decimal(projected_sales) * 100),
            "remaining_purchase_capacity_won": rounded_won(Decimal(projected_sales) * Decimal(settings["target_rate"]) - Decimal(mtd_purchase)),
            "target_applicable": applicable, "capacity_status": "공식" if applicable else "참고"}


def quality_issues(plan: dict, tables: dict) -> list[dict]:
    start, end = plan.get("query_start", plan["as_of_date"][:7] + "-01"), plan["as_of_date"]
    q = tables["quality_daily"]
    issues = q.loc[q["date"].between(start, end) & q["status"].ne("정상"), ["date", "source", "status", "reason"]].to_dict("records")
    lines = tables.get("purchase_lines", tables["purchase_daily"])
    unknown = lines.loc[lines["purchase_date"].between(start, end) & lines["category"].eq("미분류")]
    for (day, source), rows in unknown.groupby(["purchase_date", "source"]):
        issues.append({"date": day, "source": source, "status": "잠정", "reason": "미분류 구매 검수 필요",
                       "amount_won": int(rows["amount_won"].sum()), "absolute_amount_won": int(rows["amount_won"].abs().sum()),
                       "row_count": int(rows["row_count"].sum()) if "row_count" in rows else len(rows)})
    rank = {"검증 실패": 0, "누락": 1, "오래됨": 2, "잠정": 3}
    return sorted(issues, key=lambda row: (rank[row["status"]], -row.get("absolute_amount_won", 0), row["date"], row["source"]))


def build_context(plan: dict, tables: dict, classification_version: str, settings: dict | None = None) -> dict:
    settings = validate_settings(settings or {})
    end = date.fromisoformat(plan["as_of_date"])
    today, month = str(end), str(end.replace(day=1))
    daily, mtd = (period_result(start, today, tables, settings) for start in (today, month))
    recent_start, prior_start, prior_end = (str(end - timedelta(days=n)) for n in (6, 13, 7))
    recent, prior = period_result(recent_start, today, tables, settings), period_result(prior_start, prior_end, tables, settings)
    comparison = compare_periods(recent, prior, "observed_purchase_won", "purchase_complete")
    meals = compare_periods(recent, prior, "staff_meal_won", "staff_meal_complete")
    meals["current_staff_meal_won"] = meals.pop("current_observed_purchase_won")
    meals["previous_staff_meal_won"] = meals.pop("previous_observed_purchase_won")
    top = []
    items = tables["item_daily"]
    if comparison["complete"] and not items.empty:
        selected = items.loc[items["category"].isin(MATERIAL)]
        keys = ["source", "product_key", "item_name"]
        now = selected.loc[selected["purchase_date"].between(recent_start, today)].groupby(keys)["amount_won"].sum()
        before = selected.loc[selected["purchase_date"].between(prior_start, prior_end)].groupby(keys)["amount_won"].sum()
        for key, change in now.sub(before, fill_value=0).sort_values(ascending=False).head(3).items():
            if change > 0:
                top.append({"source": key[0], "product_key": key[1], "item_name": key[2],
                            "current_won": int(now.get(key, 0)), "previous_won": int(before.get(key, 0)), "increase_won": int(change),
                            "evidence": {"file": "purchase_lines.parquet", "source": key[0], "product_key": key[1],
                                         "date_from": prior_start, "date_to": today}})
    alerts = []
    threshold, minimum = Decimal(settings["increase_rate"]), settings["increase_won"]
    if comparison["complete"] and prior["observed_purchase_won"] > 0:
        increase = comparison["increase_won"]
        if increase >= minimum and Decimal(increase) / prior["observed_purchase_won"] >= threshold:
            alerts.append({"type": "seven_day_purchase_increase", **comparison, "items": top})
    if daily["purchase_complete"]:
        samples = []
        for week in range(1, 9):
            historic = str(end - timedelta(days=7 * week))
            if historic < plan["start_date"]:
                continue
            value = period_result(historic, historic, tables, settings)
            if value["purchase_complete"]:
                samples.append((historic, value["observed_purchase_won"]))
        if len(samples) >= settings["minimum_weekday_samples"]:
            baseline = Decimal(sum(value for _, value in samples)) / len(samples)
            increase = Decimal(daily["observed_purchase_won"]) - baseline
            if baseline > 0 and increase >= minimum and increase / baseline >= threshold:
                alerts.append({"type": "weekday_purchase_increase", "date": today,
                               "increase_won": rounded_won(increase), "baseline_won": rounded_won(baseline),
                               "sample_count": len(samples), "sample_dates": [day for day, _ in samples]})
    if mtd["target_exceeded"] is True:
        alerts.append({"type": "purchase_target_exceeded", "target_gap_pp": mtd["target_gap_pp"], "evidence": mtd["evidence"]})
    issues = quality_issues(plan, tables)
    requested = period_result(plan.get("query_start", month), today, tables, settings)
    return {"schema_version": SCHEMA_VERSION, "as_of_date": today, "window_start": plan["start_date"],
            "snapshot_id": plan["snapshot_id"], "rule_version": RULE_VERSION,
            "guide": {"file": "agent_guide.md", "sha256": plan["guide_sha256"]},
            "classification_version": classification_version, "settings": settings,
            "status": requested["status"], "daily": daily, "month_to_date": mtd, "requested_period": requested,
            "seven_day": comparison, "staff_meal_seven_day": meals, "top_increase_items": top, "alerts": alerts,
            "forecast": forecast(plan, tables["sales_daily"], tables["purchase_daily"], tables["quality_daily"], mtd["complete"], settings),
            "issues": issues[:3], "issues_total": len(issues), "issues_file": "issues.json"}
