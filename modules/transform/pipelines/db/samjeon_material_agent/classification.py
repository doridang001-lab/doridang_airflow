"""추천은 참고이며 명시적으로 검수된 분류만 계산에 반영한다."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db.samjeon_material_agent.contracts import (
    CATEGORIES, FOOD_MEAL, PRODUCT_FIELDS, atomic_json, code, digest, product_key, read_config, text,
)

MASTER_CATEGORIES = {
    "토핑": "원재료", "계육": "원재료", "기본반찬": "원재료", "소스": "원재료", "사이드": "원재료",
    "용기_수저_봉투": "포장재", "물티슈_스티커": "포장재", "유니폼_앞치마": "기타",
}
FOOD_CATEGORIES = {name: "원재료" for name in ("가공식품", "농산물", "수산물", "양곡", "축산물")}


def item_key(item: dict) -> tuple[str, ...]:
    return tuple(code(item.get(field)) if field == "product_code" else text(item.get(field)) for field in PRODUCT_FIELDS)


def mapping_from_config(config: dict) -> dict:
    mapping = {}
    for item in config.get("items", []):
        key, category = item_key(item), text(item.get("category"))
        if key[0] not in {"food_guide", "logistics_dam"} or not key[1] or not key[2]:
            raise ValueError(f"품목 분류 키 오류: {key}")
        if category not in CATEGORIES:
            raise ValueError(f"품목 분류값 오류: {category}")
        if key in mapping and mapping[key] != category:
            raise ValueError(f"품목 분류 키 충돌: {key}")
        mapping[key] = category
    return mapping


def load_classification(plan: dict) -> tuple[dict, str]:
    config = read_config(plan["classification_path"], plan["config_hashes"]["classification"])
    return mapping_from_config(config), text(config.get("version"))


def classify(lines: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    result = lines.copy()
    for index, row in lines.iterrows():
        meal = row["source"] == "food_guide" and row["account"] == FOOD_MEAL
        category = "현장 직원식대" if meal else mapping.get(item_key(row))
        result.at[index, "category"] = category or "미분류"
        result.at[index, "classification_status"] = "계정확정" if meal else "검수완료" if category else "검수필요"
    return result


def review_candidates(lines: pd.DataFrame, master_path: str | None = None, master_hash: str = "") -> pd.DataFrame:
    columns = [*PRODUCT_FIELDS, "product_key", "amount_won", "absolute_amount_won", "row_count",
               "suggested_category", "suggestion_basis", "approved_category", "approved"]
    unknown = lines.loc[lines["category"].eq("미분류")].copy()
    if unknown.empty:
        return pd.DataFrame(columns=columns)
    master = {}
    if master_path and master_hash:
        path = Path(master_path)
        if digest(path) != master_hash:
            raise RuntimeError("품목 추천 원천 판본 변경")
        frame = pd.read_csv(path, encoding="utf-8-sig", dtype=str).fillna("")
        required = {*PRODUCT_FIELDS, "item_category"}
        if not required.issubset(frame.columns):
            raise ValueError("물류 품목 마스터 필수 컬럼 누락")
        for row in frame.to_dict("records"):
            row["source"] = {"Dam_order": "logistics_dam", "food_guide": "food_guide"}.get(row["source"], row["source"])
            if text(row["item_category"]):
                master.setdefault(item_key(row), set()).add(text(row["item_category"]))
        if digest(path) != master_hash:
            raise RuntimeError("읽는 중 품목 추천 원천 판본 변경")
    records = []
    for key, group in unknown.groupby(list(PRODUCT_FIELDS), dropna=False, sort=True):
        categories = master.get(key, set())
        suggestion, basis = "", "검수 필요"
        if len(categories) == 1:
            category = next(iter(categories))
            suggestion = MASTER_CATEGORIES.get(category, "")
            basis = f"물류 품목 마스터: {category}"
        elif len(categories) > 1:
            basis = "물류 품목 마스터 분류 충돌"
        elif key[0] == "food_guide":
            majors = set(group["major_category"].map(text)) - {""}
            if len(majors) == 1:
                major = next(iter(majors))
                suggestion = FOOD_CATEGORIES.get(major, "")
                basis = f"푸드가이드 대분류: {major}" + ("; 포장재·기타 구분 검수 필요" if major == "비식품" else "")
        records.append({**dict(zip(PRODUCT_FIELDS, key)), "product_key": product_key(key),
                        "amount_won": int(group["amount_won"].sum()),
                        "absolute_amount_won": int(group["amount_won"].abs().sum()), "row_count": len(group),
                        "suggested_category": suggestion, "suggestion_basis": basis,
                        "approved_category": "", "approved": ""})
    return pd.DataFrame(records, columns=columns).sort_values("absolute_amount_won", ascending=False, kind="stable")


def import_review(review_path: Path, config_path: Path, *, version: str, expected_hash: str) -> dict:
    """검수자가 승인한 행만 병합한다. 기존 설정 변경은 해시로 충돌 검사한다."""
    if not version.strip():
        raise ValueError("검수 버전이 필요합니다")
    current = read_config(str(config_path), expected_hash)
    mapping = mapping_from_config(current)
    frame = pd.read_csv(review_path, encoding="utf-8-sig", dtype=str).fillna("")
    if not {*PRODUCT_FIELDS, "product_key", "approved", "approved_category"}.issubset(frame.columns):
        raise ValueError("검수표 필수 컬럼 누락")
    changes = {}
    for row in frame.to_dict("records"):
        approved = text(row["approved"]).lower()
        if approved not in {"", "n", "false", "0", "y", "true", "1"}:
            raise ValueError("검수 승인값은 Y/N 또는 boolean 표기여야 합니다")
        if approved not in {"y", "true", "1"}:
            continue
        key, category = item_key(row), text(row["approved_category"])
        if row["product_key"] != product_key(key):
            raise ValueError("검수표 품목 키가 변경되었습니다")
        mapping_from_config({"items": [{**dict(zip(PRODUCT_FIELDS, key)), "category": category}]})
        if key in changes:
            raise ValueError("검수표에 중복 승인 품목이 있습니다")
        changes[key] = category
    if not changes:
        return {"updated_items": 0, "version": current.get("version", "")}
    mapping.update(changes)
    result = {"version": version, "items": [{**dict(zip(PRODUCT_FIELDS, key)), "category": category}
                                            for key, category in sorted(mapping.items())]}
    read_config(str(config_path), expected_hash)
    atomic_json(config_path, result)
    return {"updated_items": len(changes), "total_items": len(mapping), "version": version}
