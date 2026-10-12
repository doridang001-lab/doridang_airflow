"""마트의 타입, 판본, 직렬화 및 공통 값 계약."""
from __future__ import annotations

import hashlib
import json
import math
import os
import uuid
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = 3
OUTPUT_TABLES = ('purchase_lines', 'sales_lines', 'sales_daily', 'purchase_daily', 'item_daily', 'quality_daily')
OUTPUT_NAMES = tuple(f'{name}.parquet' for name in OUTPUT_TABLES) + (
    'review_candidates.csv', 'coverage_candidates.csv', 'agent_context.json', 'issues.json',
    'agent_guide.md', 'manifest.json')
RULE_VERSION = "samjeon-mart-v3"
STORE = "송파삼전점"
FOOD_MAIN = "도리당 송파삼전점"
FOOD_MEAL = "도리당 송파삼전점(부식)"
DAM_STORE = FOOD_MAIN
SOURCE_NAMES = ("sales", "food_guide", "logistics_dam")
CATEGORIES = {"원재료", "포장재", "기타"}
ALL_CATEGORIES = ("원재료", "포장재", "현장 직원식대", "기타", "미분류")
PRODUCT_FIELDS = ("source", "product_code", "item_name", "spec", "unit")
SCOPES = {"sales": STORE, "food_guide": f"{FOOD_MAIN}|{FOOD_MEAL}", "logistics_dam": DAM_STORE}
SALES_COLUMNS = ["sale_date", "source", "amount_won", "row_count"]
SALES_LINE_COLUMNS = ["sale_date", "source", "amount_won", "_pk", "source_ref", "source_file",
                      "source_sha256", "source_row", "evidence_id", "row_content_hash"]
PURCHASE_COLUMNS = [
    "purchase_date", "source", "account", "original_account", "product_key", "product_code",
    "item_name", "spec", "unit", "qty", "amount_won", "category", "classification_status",
    "source_ref", "source_file", "source_sha256", "source_row", "evidence_id", "closed", "major_category",
]


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def json_value(value: Any) -> Any:
    if isinstance(value, np.generic):
        return json_value(value.item())
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON에 비유한 숫자를 저장할 수 없습니다")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("JSON에 비유한 Decimal을 저장할 수 없습니다")
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("JSON 객체 키는 문자열이어야 합니다")
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    raise TypeError(f"지원하지 않는 JSON 타입: {type(value).__name__}")


def json_bytes(value: Any) -> bytes:
    return json.dumps(json_value(value), ensure_ascii=False, sort_keys=True, allow_nan=False).encode("utf-8")


def atomic_json(path: Path, value: Any) -> None:
    data = json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_bytes(data)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def text(value: Any) -> str:
    if pd.isna(value):
        return ""
    result = str(value).strip()
    return "" if result.lower() in {"nan", "none", "nat"} else result


def code(value: Any) -> str:
    result = text(value)
    return result[:-2] if result.endswith(".0") else result


def won(value: Any, *, label: str) -> int:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(f"{label}: 금액을 읽을 수 없습니다") from exc
    if not number.is_finite() or number != number.to_integral_value():
        raise ValueError(f"{label}: 정수 원 금액이 아닙니다")
    return int(number)


def rounded_won(value: Decimal) -> int:
    return int(value.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def percent(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))


def product_key(values: list[str] | tuple[str, ...]) -> str:
    return hashlib.sha256(json_bytes(values)).hexdigest()[:20]


def read_config(path_text: str, expected_hash: str) -> dict:
    path = Path(path_text)
    if not expected_hash:
        if path.exists():
            raise RuntimeError(f"계획 이후 설정 파일 생성: {path}")
        return {}
    if not path.is_file() or digest(path) != expected_hash:
        raise RuntimeError(f"계획 이후 설정 파일 변경: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("설정 파일의 최상위 값은 객체여야 합니다")
    if digest(path) != expected_hash:
        raise RuntimeError(f"읽는 중 설정 판본 변경: {path}")
    return value


def validate_context(value: dict) -> None:
    if value.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("지원하지 않는 문맥 스키마 버전")

    def visit(node):
        if isinstance(node, dict):
            for key, item in node.items():
                if key.endswith("complete") or key in {"target_applicable", "target_exceeded"}:
                    if item is not None and type(item) is not bool:
                        raise ValueError(f"boolean 타입 오류: {key}")
                if key.endswith("_won") and item is not None and type(item) is not int:
                    raise ValueError(f"정수 원 타입 오류: {key}")
                if key.endswith(("_pct", "_pp")) and item is not None:
                    if not isinstance(item, str) or not Decimal(item).is_finite():
                        raise ValueError(f"비율 타입 오류: {key}")
                visit(item)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(value)
    json_bytes(value)
