"""FinProduct 수동분류 기반 규칙 채굴 및 적용."""

import json
import logging
import os
import re
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

from modules.transform.pipelines.db.DB_ItemIdAllocator import normalize_item_key
from modules.transform.utility.paths import (
    FIN_PRODUCT_RULES_JSON_PATH,
    FIN_PRODUCT_RULES_MANUAL_JSON_PATH,
)

logger = logging.getLogger(__name__)

CATEGORIES = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
DEFAULT_IGNORE_WORDS = ["추가", "곱빼기", "선택", "맛있는", "변경", "기본", "많이", "적게"]
MIN_SUPPORT = 5
MIN_LABEL_RATIO = 0.9
MIN_CONFIDENCE = 0.9
MIN_DISTINCT_PATTERNS = 3
MIN_VALIDATION_ACCURACY = 0.9
MAX_RULES_PER_LABEL = 30
MAX_PROMPT_RULES = 80
MAX_ACTIVE_RULE_CHANGE_RATIO = 0.3
RULE_STATUS_ACTIVE = "active"
RULE_STATUS_CANDIDATE = "candidate"
RULE_STATUS_BLOCKED = "blocked"

_WORD_RE = re.compile(r"[0-9A-Za-z가-힣一-龥]+")
_GENERIC_TOKENS = {
    "메뉴",
    "선택",
    "기본",
    "추가",
    "변경",
    "맛있는",
    "많이",
    "적게",
    "보통",
    "단품",
    "대표",
}
_MEANINGFUL_SUFFIXES = {
    "사리",
    "토핑",
    "음료",
    "주류",
    "세트",
    "리뷰",
    "콜라",
    "맥주",
    "소주",
}


def _safe_replace(src: Path, dst: Path, retries: int = 3, delay_sec: float = 1.0) -> None:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            os.replace(src, dst)
            return
        except (PermissionError, OSError) as exc:
            last_error = exc
            if attempt < retries:
                logger.warning("규칙 JSON 교체 재시도: %s (%d/%d)", dst, attempt, retries)
                time.sleep(delay_sec)
    if last_error is not None:
        raise last_error


def _first_existing(row: pd.Series, candidates: tuple[str, ...]) -> str:
    for col in candidates:
        if col in row.index:
            value = str(row.get(col, "") or "").strip()
            if value and value.lower() != "nan":
                return value
    return ""


def _normalize_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows is None or rows.empty:
        return pd.DataFrame(
            columns=[
                "item_name",
                "label",
                "represent_menu",
                "exclude_check",
                "category_large",
                "category_middle",
            ]
        )

    normalized = []
    for _, row in rows.fillna("").iterrows():
        item_name = _first_existing(row, ("item_name", "상품명", "메뉴명", "name"))
        label = _first_existing(row, ("수동분류", "수동분류_edit", "category", "label"))
        if label not in CATEGORIES:
            continue
        if not item_name:
            continue
        normalized.append(
            {
                "item_name": item_name,
                "label": label,
                "represent_menu": _first_existing(
                    row,
                    ("represent_menu", "대표메뉴", "표준_메뉴명_edit", "표준명", "표준_메뉴명"),
                ),
                "exclude_check": _first_existing(row, ("exclude_check", "제외", "exclude")),
                "category_large": _first_existing(row, ("category_large", "대메뉴")),
                "category_middle": _first_existing(row, ("category_middle", "중메뉴")),
            }
        )
    return pd.DataFrame(normalized)


def _token_candidates(item_name: str) -> set[str]:
    key = normalize_item_key(item_name)
    tokens = {token.strip().lower() for token in _WORD_RE.findall(str(item_name or ""))}
    candidates: set[str] = set()
    if len(key) >= 2:
        candidates.add(key)
    for token in tokens:
        norm = normalize_item_key(token)
        if len(norm) >= 2 and norm not in _GENERIC_TOKENS and norm not in DEFAULT_IGNORE_WORDS:
            candidates.add(norm)
            for suffix in _MEANINGFUL_SUFFIXES:
                if norm.endswith(suffix) and norm != suffix:
                    candidates.add(suffix)

    parts = [normalize_item_key(token) for token in tokens]
    parts = [part for part in parts if len(part) >= 2 and part not in _GENERIC_TOKENS]
    for idx in range(len(parts) - 1):
        joined = "".join(parts[idx:idx + 2])
        if len(joined) >= 4:
            candidates.add(joined)
    return candidates


def _display_keyword(keyword: str, examples: list[str]) -> str:
    for example in examples:
        for token in _WORD_RE.findall(str(example or "")):
            if normalize_item_key(token) == keyword:
                return token.strip()
    return keyword


def _mode(values: pd.Series, default: str = "") -> str:
    cleaned = [str(value).strip() for value in values.fillna("") if str(value).strip()]
    if not cleaned:
        return default
    return Counter(cleaned).most_common(1)[0][0]


def _validation_accuracy(keyword: str, label: str, rows: pd.DataFrame) -> tuple[float, int, list[str]]:
    matched = []
    negative_examples = []
    for _, row in rows.iterrows():
        keywords = _token_candidates(row["item_name"])
        if keyword not in keywords:
            continue
        matched.append(row)
        if row["label"] != label and len(negative_examples) < 5:
            negative_examples.append(str(row["item_name"]))
    if not matched:
        return 0.0, 0, negative_examples
    correct = sum(1 for row in matched if row["label"] == label)
    return round(correct / len(matched), 4), len(matched), negative_examples


def _rule_status(
    *,
    support: int,
    ratio: float,
    distinct_patterns: int,
    validation_accuracy: float,
    label_counts: Counter,
) -> tuple[str, str]:
    if len(label_counts) > 1:
        others = sum(count for label, count in label_counts.items()) - support
        if others > 0:
            return RULE_STATUS_BLOCKED, "동일 키워드가 다른 라벨에도 존재"
    failed = []
    if support < MIN_SUPPORT:
        failed.append(f"support<{MIN_SUPPORT}")
    if ratio < MIN_LABEL_RATIO:
        failed.append(f"label_ratio<{MIN_LABEL_RATIO}")
    if distinct_patterns < MIN_DISTINCT_PATTERNS:
        failed.append(f"distinct_patterns<{MIN_DISTINCT_PATTERNS}")
    if validation_accuracy < MIN_VALIDATION_ACCURACY:
        failed.append(f"validation_accuracy<{MIN_VALIDATION_ACCURACY}")
    if failed:
        return RULE_STATUS_CANDIDATE, ", ".join(failed)
    return RULE_STATUS_ACTIVE, ""


def build_rules_from_manual(rows: pd.DataFrame) -> list[dict]:
    normalized = _normalize_rows(rows)
    if normalized.empty:
        return []

    keyword_labels: dict[str, Counter] = defaultdict(Counter)
    row_keywords: list[set[str]] = []
    for _, row in normalized.iterrows():
        keywords = _token_candidates(row["item_name"])
        row_keywords.append(keywords)
        for keyword in keywords:
            keyword_labels[keyword][row["label"]] += 1

    candidates: list[dict[str, Any]] = []
    for keyword, label_counts in keyword_labels.items():
        total = sum(label_counts.values())
        label, support = label_counts.most_common(1)[0]
        ratio = support / total if total else 0
        confidence = round(ratio, 4)
        matched_idx = [
            idx
            for idx, keywords in enumerate(row_keywords)
            if keyword in keywords and normalized.iloc[idx]["label"] == label
        ]
        matched = normalized.iloc[matched_idx].copy()
        examples = matched["item_name"].drop_duplicates().head(5).astype(str).tolist()
        distinct_patterns = int(matched["item_name"].map(normalize_item_key).nunique())
        validation_accuracy, validation_samples, negative_examples = _validation_accuracy(keyword, label, normalized)
        status, blocked_reason = _rule_status(
            support=int(support),
            ratio=ratio,
            distinct_patterns=distinct_patterns,
            validation_accuracy=validation_accuracy,
            label_counts=label_counts,
        )
        candidates.append(
            {
                "수동분류": label,
                "include_keywords": [_display_keyword(keyword, examples)],
                "ignore_words": DEFAULT_IGNORE_WORDS,
                "represent_menu": _mode(matched["represent_menu"], default=_display_keyword(keyword, examples)),
                "category_large": _mode(matched["category_large"]),
                "category_middle": _mode(matched["category_middle"]),
                "exclude_check": _mode(matched["exclude_check"], default="N").upper() or "N",
                "examples": examples,
                "negative_examples": negative_examples,
                "support": int(support),
                "confidence": confidence,
                "status": status,
                "blocked_reason": blocked_reason,
                "source_labels": dict(label_counts),
                "validation": {
                    "accuracy": validation_accuracy,
                    "samples": validation_samples,
                    "distinct_patterns": distinct_patterns,
                },
                "_keyword_key": keyword,
            }
        )

    candidates.sort(
        key=lambda rule: (
            CATEGORIES.index(rule["수동분류"]),
            0 if rule.get("status") == RULE_STATUS_ACTIVE else 1 if rule.get("status") == RULE_STATUS_CANDIDATE else 2,
            -float(rule["confidence"]),
            -int(rule["support"]),
            -len(normalize_item_key(rule["include_keywords"][0])),
            rule["include_keywords"][0],
        )
    )

    rules: list[dict] = []
    per_label_count: Counter = Counter()
    for candidate in candidates:
        label = candidate["수동분류"]
        if per_label_count[label] >= MAX_RULES_PER_LABEL:
            continue
        per_label_count[label] += 1
        candidate.pop("_keyword_key", None)
        candidate["priority"] = len(rules) + 1
        candidate["rule_name"] = f"{candidate['include_keywords'][0]} 분류"
        rules.append(candidate)

    return rules


def _load_json_rules(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("규칙 JSON 로드 실패: %s | %s", path, exc)
        return []
    if isinstance(data, dict):
        data = data.get("rules", [])
    return data if isinstance(data, list) else []


def _valid_rule(rule: dict) -> bool:
    label = str(rule.get("수동분류", "")).strip()
    keywords = rule.get("include_keywords") or []
    return label in CATEGORIES and isinstance(keywords, list) and any(str(k).strip() for k in keywords)


def load_rules() -> list[dict]:
    auto_rules = [rule for rule in _load_json_rules(FIN_PRODUCT_RULES_JSON_PATH) if isinstance(rule, dict)]
    manual_rules = [rule for rule in _load_json_rules(FIN_PRODUCT_RULES_MANUAL_JSON_PATH) if isinstance(rule, dict)]

    merged: list[dict] = []
    for idx, rule in enumerate(manual_rules, start=1):
        if not _valid_rule(rule):
            continue
        item = dict(rule)
        item["priority"] = int(item.get("priority") or idx)
        item["manual_override"] = True
        item["status"] = RULE_STATUS_ACTIVE
        merged.append(item)

    offset = max([int(rule.get("priority") or 0) for rule in merged] or [0])
    for idx, rule in enumerate(auto_rules, start=1):
        if not _valid_rule(rule):
            continue
        item = dict(rule)
        item["priority"] = offset + int(item.get("priority") or idx)
        item["status"] = item.get("status") or RULE_STATUS_CANDIDATE
        merged.append(item)

    return sorted(merged, key=lambda rule: int(rule.get("priority") or 999999))


def save_rules(rules: list[dict]) -> None:
    existing = _load_json_rules(FIN_PRODUCT_RULES_JSON_PATH)
    old_active = sum(1 for rule in existing if isinstance(rule, dict) and rule.get("status") == RULE_STATUS_ACTIVE)
    new_active = sum(1 for rule in rules if rule.get("status") == RULE_STATUS_ACTIVE)
    if old_active and abs(new_active - old_active) / old_active > MAX_ACTIVE_RULE_CHANGE_RATIO:
        raise RuntimeError(
            "active 규칙 수 변화가 30%를 초과해 저장 중단: "
            f"old={old_active}, new={new_active}"
        )
    FIN_PRODUCT_RULES_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_RULES_JSON_PATH.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")
        _safe_replace(tmp, FIN_PRODUCT_RULES_JSON_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def summarize_rules(rules: list[dict]) -> dict[str, int]:
    counts = Counter(str(rule.get("status") or RULE_STATUS_CANDIDATE) for rule in rules)
    return {
        "active_rule_count": int(counts.get(RULE_STATUS_ACTIVE, 0)),
        "candidate_rule_count": int(counts.get(RULE_STATUS_CANDIDATE, 0)),
        "blocked_rule_count": int(counts.get(RULE_STATUS_BLOCKED, 0)),
        "conflict_count": int(counts.get(RULE_STATUS_BLOCKED, 0)),
        "rule_count": int(len(rules)),
    }


def _normalized_match_text(item: dict, ignore_words: list[str]) -> str:
    parts = [
        item.get("category_large", ""),
        item.get("category_middle", ""),
        item.get("대메뉴", ""),
        item.get("중메뉴", ""),
        item.get("item_name", ""),
        item.get("상품명", ""),
    ]
    text = normalize_item_key(" ".join(str(part or "") for part in parts))
    for word in ignore_words:
        key = normalize_item_key(word)
        if key:
            text = text.replace(key, "")
    return text


def classify_by_rules(item: dict, rules: list[dict]) -> dict | None:
    if not item or not rules:
        return None
    for rule in sorted(rules, key=lambda value: int(value.get("priority") or 999999)):
        label = str(rule.get("수동분류", "")).strip()
        if label not in CATEGORIES:
            continue
        if not rule.get("manual_override") and rule.get("status") != RULE_STATUS_ACTIVE:
            continue
        confidence = float(rule.get("confidence", 1.0) or 0)
        if confidence < MIN_CONFIDENCE and not rule.get("manual_override"):
            continue
        keywords = [str(keyword).strip() for keyword in rule.get("include_keywords") or [] if str(keyword).strip()]
        if not keywords:
            continue
        text = _normalized_match_text(item, rule.get("ignore_words") or DEFAULT_IGNORE_WORDS)
        if any(normalize_item_key(keyword) and normalize_item_key(keyword) in text for keyword in keywords):
            return {
                "수동분류": label,
                "수동분류_edit": label,
                "represent_menu": str(rule.get("represent_menu", "") or "").strip(),
                "표준_메뉴명_edit": str(rule.get("represent_menu", "") or "").strip(),
                "exclude_check": str(rule.get("exclude_check", "N") or "N").strip().upper() or "N",
                "rule_name": str(rule.get("rule_name", "") or "").strip(),
                "confidence": confidence,
                "classified_by": "rule",
            }
    return None


def rules_to_prompt_block(rules: list[dict]) -> str:
    selected = [
        rule for rule in rules
        if str(rule.get("수동분류", "")).strip() in CATEGORIES
        and (rule.get("manual_override") or rule.get("status") == RULE_STATUS_ACTIVE)
        and float(rule.get("confidence", 1.0) or 0) >= MIN_CONFIDENCE
    ]
    if not selected:
        return "(자동 키워드 규칙 없음)"
    lines = []
    for rule in selected[:MAX_PROMPT_RULES]:
        keywords = ", ".join(str(k).strip() for k in rule.get("include_keywords", []) if str(k).strip())
        examples = ", ".join(str(e).strip() for e in rule.get("examples", [])[:3] if str(e).strip())
        suffix = f" | 예시: {examples}" if examples else ""
        lines.append(f'- 키워드 [{keywords}] -> 수동분류 "{rule["수동분류"]}"{suffix}')
    return "\n".join(lines)


def reconcile(rule_result: dict | None, llm_result: dict) -> dict:
    llm = dict(llm_result or {})
    rule_label = str((rule_result or {}).get("수동분류") or (rule_result or {}).get("수동분류_edit") or "").strip()
    llm_label = str(llm.get("수동분류") or llm.get("수동분류_edit") or "").strip()

    if rule_result and rule_label and llm_label and rule_label == llm_label:
        result = {**llm, **{k: v for k, v in rule_result.items() if k not in {"수동분류", "수동분류_edit"}}}
        result["수동분류"] = llm_label
        result["수동분류_edit"] = llm_label
        result["classified_by"] = "rule+llm"
        result["검수유무"] = "0"
        return result

    if rule_result and rule_label and llm_label and rule_label != llm_label:
        result = dict(llm)
        result["수동분류"] = llm_label
        result["수동분류_edit"] = llm_label
        result["classified_by"] = f"rule/llm_conflict(규칙={rule_label}, LLM={llm_label})"
        result["검수사유"] = f"규칙/LLM 불일치(규칙={rule_label}, LLM={llm_label})"
        result["검수유무"] = "0"
        return result

    if rule_result and rule_label:
        result = dict(rule_result)
        result["수동분류"] = rule_label
        result["수동분류_edit"] = rule_label
        result["classified_by"] = "rule"
        result["검수유무"] = "0"
        return result

    result = dict(llm)
    if llm_label:
        result["수동분류"] = llm_label
        result["수동분류_edit"] = llm_label
    result["classified_by"] = result.get("classified_by") or "llm"
    result["검수유무"] = "0"
    return result
