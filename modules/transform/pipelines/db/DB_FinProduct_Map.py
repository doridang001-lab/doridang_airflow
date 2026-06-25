"""fin_product_map 생성 및 내부 LLM 증분 분류."""

import json
import logging
import os
import time
from datetime import date
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import (
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_JSON_PATH,
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
    MART_DB,
)

TARGET_STORES = ["송파삼전점"]
TARGET_STORE_SET = {s.strip() for s in TARGET_STORES if s.strip()}
UNIFIED_SALES_GRP_DIR = MART_DB / "unified_sales_grp"
MAP_COLUMNS = [
    "store",
    "source",
    "brand",
    "item_name",
    "unitprice",
    "표준_메뉴명_edit",
    "수동분류_edit",
    "main_menu_edit",
    "메뉴대분류_edit",
    "메뉴중분류_edit",
    "부모메뉴_확정방식",
    "대표_menu_name",
    "menu_name_후보수",
    "menu_name_샘플",
    "main_menu_confidence",
    "review_status_edit",
    "classified_by",
    "updated_at",
]
REVIEW_COLUMNS = [
    "store",
    "source",
    "brand",
    "item_name",
    "unitprice",
    "표준_메뉴명_edit",
    "수동분류_edit",
    "review_status_edit",
    "검수사유",
    "menu_name_샘플",
]
KEY_COLUMNS = ["store", "source", "item_name"]
EDIT_COLUMN_ALIASES = {
    "표준_메뉴명": "표준_메뉴명_edit",
    "수동분류": "수동분류_edit",
    "main_menu": "main_menu_edit",
    "메뉴대분류": "메뉴대분류_edit",
    "메뉴중분류": "메뉴중분류_edit",
    "review_status": "review_status_edit",
}
VALID_CATEGORIES = ["메인", "1인", "사이드", "기타"]
VALID_MENU_MAJOR = ["메인메뉴", "추가메뉴", "사이드", "기타"]
VALID_PARENT_METHODS = ["상품 단독 분류", "menu_name 필요", "주문번호 기준 추론", ""]
MODEL_CANDIDATES = ["gpt-oss:20b", "gpt-oss:latest", "gpt-oss", "qwen2.5:7b", "qwen2.5:latest"]
OLLAMA_HOSTS = [
    os.getenv("OLLAMA_HOST", "").strip(),
    "http://host.docker.internal:11434",
    "http://localhost:11434",
]
BATCH_SIZE = 5
TODAY = str(date.today())

logger = logging.getLogger(__name__)


def _safe_replace(tmp: Path, target: Path, retries: int = 3, delay_sec: float = 1.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            os.replace(tmp, target)
            return
        except PermissionError:
            if attempt == retries:
                raise
            logger.warning("파일 교체 재시도: %s (%d/%d)", target, attempt, retries)
            time.sleep(delay_sec)


def _empty_map() -> pd.DataFrame:
    return pd.DataFrame(columns=MAP_COLUMNS)


def _normalize_review_status(value: object) -> str:
    text = str(value or "").strip()
    if text.lower() == "approved" or text.upper() == "Y":
        return "Y"
    return "N"


def _pick_common_value(series: pd.Series) -> str:
    values = series.fillna("").astype(str).str.strip()
    values = values[values != ""]
    if values.empty:
        return ""
    return values.value_counts(sort=True).index[0]


def _apply_edit_column_aliases(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for old_col, edit_col in EDIT_COLUMN_ALIASES.items():
        if edit_col not in result.columns:
            result[edit_col] = ""
        if old_col in result.columns:
            old_values = result[old_col].fillna("").astype(str)
            empty_edit = result[edit_col].fillna("").astype(str).str.strip() == ""
            result.loc[empty_edit, edit_col] = old_values[empty_edit]
            result = result.drop(columns=[old_col])
    return result


def _strip_text(value: object) -> str:
    return str(value or "").strip()


def _default_menu_major(label: str) -> str:
    if label in {"메인", "1인"}:
        return "메인메뉴"
    if label == "사이드":
        return "사이드"
    return "기타"


def _classification_override(item_name: str) -> dict[str, str]:
    name = item_name.strip()
    if name == "1인 추가":
        return {
            "표준_메뉴명_edit": "1인 추가",
            "수동분류_edit": "사이드",
            "main_menu_edit": "",
            "메뉴대분류_edit": "추가메뉴",
            "메뉴중분류_edit": "인원추가",
            "부모메뉴_확정방식": "menu_name 필요",
        }
    return {}


def _normalize_classification(item: dict, classified: dict) -> dict[str, str]:
    item_name = _strip_text(item.get("item_name"))
    representative_menu = _strip_text(item.get("대표_menu_name"))
    try:
        menu_count = int(float(_strip_text(item.get("menu_name_후보수")) or "0"))
    except ValueError:
        menu_count = 0
    values = dict(classified)
    values.update(_classification_override(item_name))

    label = _strip_text(values.get("수동분류_edit") or values.get("수동분류"))
    if label not in VALID_CATEGORIES:
        label = "기타"

    std_name = _strip_text(values.get("표준_메뉴명_edit") or values.get("표준_메뉴명")) or item_name
    main_menu = _strip_text(values.get("main_menu_edit") or values.get("main_menu"))
    if menu_count == 1:
        main_menu = representative_menu or main_menu
    elif menu_count > 1:
        main_menu = ""
    menu_major = _strip_text(values.get("메뉴대분류_edit") or values.get("메뉴대분류")) or _default_menu_major(label)
    if menu_major not in VALID_MENU_MAJOR:
        menu_major = "기타"
    menu_middle = _strip_text(values.get("메뉴중분류_edit") or values.get("메뉴중분류"))
    parent_method = _strip_text(values.get("부모메뉴_확정방식")) or "상품 단독 분류"
    if menu_count > 1 and not main_menu:
        parent_method = "menu_name 필요"
    if parent_method not in VALID_PARENT_METHODS:
        parent_method = "상품 단독 분류"

    return {
        "표준_메뉴명_edit": std_name,
        "수동분류_edit": label,
        "main_menu_edit": main_menu,
        "메뉴대분류_edit": menu_major,
        "메뉴중분류_edit": menu_middle,
        "부모메뉴_확정방식": parent_method,
    }


def _apply_classification_overrides(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "item_name" not in df.columns:
        return df
    result = df.copy()
    for idx, item_name in result["item_name"].fillna("").astype(str).str.strip().items():
        override = _classification_override(item_name)
        if not override:
            continue
        for col, value in override.items():
            if col in result.columns:
                result.at[idx, col] = value
    return result


def _apply_menu_evidence_rules(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    result = df.copy()
    for col in ("대표_menu_name", "menu_name_후보수", "menu_name_샘플", "main_menu_confidence"):
        if col not in result.columns:
            result[col] = ""
    menu_count = pd.to_numeric(result["menu_name_후보수"], errors="coerce").fillna(0).astype(int)
    result["main_menu_confidence"] = result["main_menu_confidence"].fillna("").astype(str).str.strip()
    result.loc[menu_count == 1, "main_menu_confidence"] = "high"
    result.loc[menu_count > 1, "main_menu_confidence"] = "low"
    result.loc[menu_count == 1, "main_menu_edit"] = result.loc[menu_count == 1, "대표_menu_name"].fillna("").astype(str).str.strip()
    result.loc[menu_count > 1, "main_menu_edit"] = ""
    result.loc[menu_count > 1, "부모메뉴_확정방식"] = "menu_name 필요"
    result.loc[(menu_count == 1) & (result["부모메뉴_확정방식"].fillna("").astype(str).str.strip() == ""), "부모메뉴_확정방식"] = "상품 단독 분류"
    return result


def _menu_name_summary(series: pd.Series) -> pd.Series:
    values = series.fillna("").astype(str).str.strip()
    values = values[values != ""]
    if values.empty:
        return pd.Series({
            "대표_menu_name": "",
            "menu_name_후보수": "0",
            "menu_name_샘플": "",
            "main_menu_confidence": "",
        })
    counts = values.value_counts(sort=True)
    menu_count = len(counts)
    representative = counts.index[0]
    return pd.Series({
        "대표_menu_name": representative,
        "menu_name_후보수": str(menu_count),
        "menu_name_샘플": " | ".join(counts.head(5).index.tolist()),
        "main_menu_confidence": "high" if menu_count == 1 else "low",
    })


def scan_target_items() -> pd.DataFrame:
    if not TARGET_STORE_SET:
        raise ValueError("TARGET_STORES가 비어 있습니다.")

    frames = []
    for file_path in sorted(UNIFIED_SALES_GRP_DIR.glob("unified_sales_*.parquet")):
        try:
            df = pd.read_parquet(file_path, columns=["store", "source", "brand", "item_name", "unit_price", "menu_name"])
        except Exception as e:
            try:
                df = pd.read_parquet(file_path)
            except Exception as fallback_error:
                logger.warning("parquet 로드 실패: %s | %s | %s", file_path, e, fallback_error)
                continue
            for col in ("store", "source", "brand", "item_name", "unit_price", "menu_name"):
                if col not in df.columns:
                    df[col] = ""
            df = df[["store", "source", "brand", "item_name", "unit_price", "menu_name"]]
        if df.empty:
            continue
        for col in ("store", "source", "brand", "item_name", "unit_price", "menu_name"):
            df[col] = df[col].fillna("").astype(str).str.strip()
        scoped = df[df["store"].isin(TARGET_STORE_SET) & (df["item_name"] != "")]
        if not scoped.empty:
            frames.append(scoped[["store", "source", "brand", "item_name", "unit_price", "menu_name"]])

    if not frames:
        return pd.DataFrame(columns=[
            "store", "source", "brand", "item_name", "unitprice",
            "대표_menu_name", "menu_name_후보수", "menu_name_샘플", "main_menu_confidence",
        ])
    grouped = (
        pd.concat(frames, ignore_index=True)
        .groupby(KEY_COLUMNS)
        .agg(
            brand=("brand", _pick_common_value),
            unitprice=("unit_price", _pick_common_value),
            menu_names=("menu_name", lambda s: list(s)),
        )
        .reset_index()
    )
    evidence = grouped["menu_names"].apply(lambda values: _menu_name_summary(pd.Series(values)))
    return (
        pd.concat([grouped.drop(columns=["menu_names"]), evidence], axis=1)
        .sort_values(["store", "source", "item_name"])
        .reset_index(drop=True)
    )


def build_initial_map() -> pd.DataFrame:
    target_items = scan_target_items()
    if target_items.empty:
        return _empty_map()

    result = target_items.copy()
    result["표준_메뉴명_edit"] = result["item_name"]
    result["수동분류_edit"] = ""
    menu_count = pd.to_numeric(result["menu_name_후보수"], errors="coerce").fillna(0).astype(int)
    result["main_menu_edit"] = result["대표_menu_name"].where(menu_count == 1, "")
    result["메뉴대분류_edit"] = ""
    result["메뉴중분류_edit"] = ""
    result["부모메뉴_확정방식"] = "상품 단독 분류"
    result.loc[menu_count > 1, "부모메뉴_확정방식"] = "menu_name 필요"
    result["review_status_edit"] = "N"
    result["classified_by"] = ""
    result["updated_at"] = TODAY
    result = _apply_menu_evidence_rules(result)
    result = _apply_classification_overrides(result)
    return result[MAP_COLUMNS].drop_duplicates(subset=KEY_COLUMNS, keep="last").reset_index(drop=True)


def load_map() -> pd.DataFrame:
    if not FIN_PRODUCT_MAP_CSV_PATH.exists():
        return _empty_map()
    df = pd.read_csv(FIN_PRODUCT_MAP_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    df = _apply_edit_column_aliases(df)
    df = df.reindex(columns=MAP_COLUMNS, fill_value="")
    df = _apply_menu_evidence_rules(df)
    df = _apply_classification_overrides(df)
    df["review_status_edit"] = df["review_status_edit"].apply(_normalize_review_status)
    return df


def _empty_review_map() -> pd.DataFrame:
    return pd.DataFrame(columns=REVIEW_COLUMNS)


def load_review_map() -> pd.DataFrame:
    if not FIN_PRODUCT_MAP_REVIEW_CSV_PATH.exists():
        return _empty_review_map()
    df = pd.read_csv(FIN_PRODUCT_MAP_REVIEW_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    df = _apply_edit_column_aliases(df)
    df = df.reindex(columns=REVIEW_COLUMNS, fill_value="")
    df["review_status_edit"] = df["review_status_edit"].apply(_normalize_review_status)
    return df


def write_map(map_df: pd.DataFrame) -> None:
    FIN_PRODUCT_MAP_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_MAP_CSV_PATH.with_suffix(".tmp")
    try:
        map_df.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_MAP_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def write_review_map(review_df: pd.DataFrame) -> None:
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_MAP_REVIEW_CSV_PATH.with_suffix(".tmp")
    try:
        review_df.reindex(columns=REVIEW_COLUMNS, fill_value="").to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_MAP_REVIEW_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def _review_reason(row: pd.Series) -> str:
    if _normalize_review_status(row.get("review_status_edit")) == "Y":
        return ""
    reasons = []
    std_name = _strip_text(row.get("표준_메뉴명_edit"))
    label = _strip_text(row.get("수동분류_edit"))
    try:
        menu_count = int(float(_strip_text(row.get("menu_name_후보수")) or "0"))
    except ValueError:
        menu_count = 0

    if not std_name:
        reasons.append("표준명 미입력")
    if label not in VALID_CATEGORIES:
        reasons.append("수동분류 미입력")
    elif label == "기타":
        reasons.append("기타 분류 확인")
    if menu_count > 1 and label in {"메인", "1인"}:
        reasons.append("부모메뉴 후보 다수")
    return ", ".join(dict.fromkeys(reasons))


def build_review_rows(map_df: pd.DataFrame) -> pd.DataFrame:
    if map_df.empty:
        return _empty_review_map()
    result = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    result["검수사유"] = result.apply(_review_reason, axis=1)
    result = result[result["검수사유"].fillna("").astype(str).str.strip() != ""]
    if result.empty:
        return _empty_review_map()
    return (
        result.reindex(columns=REVIEW_COLUMNS, fill_value="")
        .drop_duplicates(subset=KEY_COLUMNS, keep="last")
        .sort_values(["store", "source", "검수사유", "item_name"])
        .reset_index(drop=True)
    )


def apply_review_edits(map_df: pd.DataFrame, review_df: pd.DataFrame) -> pd.DataFrame:
    if map_df.empty or review_df.empty:
        return map_df

    result = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    approved = review_df[
        review_df["review_status_edit"].fillna("").astype(str).str.strip().apply(_normalize_review_status) == "Y"
    ].copy()
    if approved.empty:
        return result

    approved = approved.drop_duplicates(subset=KEY_COLUMNS, keep="last")
    for _, review_row in approved.iterrows():
        key_mask = pd.Series(True, index=result.index)
        for col in KEY_COLUMNS:
            key_mask = key_mask & (result[col].fillna("").astype(str).str.strip() == _strip_text(review_row.get(col)))
        if not bool(key_mask.any()):
            continue

        idx = result.index[key_mask]
        std_name = _strip_text(review_row.get("표준_메뉴명_edit")) or _strip_text(review_row.get("item_name"))
        label = _strip_text(review_row.get("수동분류_edit"))
        if label not in VALID_CATEGORIES:
            continue

        result.loc[idx, "표준_메뉴명_edit"] = std_name
        result.loc[idx, "수동분류_edit"] = label
        result.loc[idx, "메뉴대분류_edit"] = result.loc[idx, "메뉴대분류_edit"].where(
            result.loc[idx, "메뉴대분류_edit"].fillna("").astype(str).str.strip() != "",
            _default_menu_major(label),
        )
        result.loc[idx, "review_status_edit"] = "Y"
        result.loc[idx, "classified_by"] = "human"
        result.loc[idx, "updated_at"] = TODAY
    return result


def find_llm_targets(all_items: pd.DataFrame, map_df: pd.DataFrame) -> pd.DataFrame:
    if all_items.empty:
        return all_items.copy()
    if map_df.empty:
        return all_items.copy()

    status = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    for col in (
        "store", "source", "brand", "item_name", "unitprice", "표준_메뉴명_edit", "수동분류_edit",
        "main_menu_edit", "메뉴대분류_edit", "메뉴중분류_edit", "부모메뉴_확정방식",
        "대표_menu_name", "menu_name_후보수", "menu_name_샘플", "main_menu_confidence",
        "review_status_edit", "classified_by",
    ):
        status[col] = status[col].fillna("").astype(str).str.strip()
    status["review_status_edit"] = status["review_status_edit"].apply(_normalize_review_status)
    status = status.drop_duplicates(subset=KEY_COLUMNS, keep="last")

    merged = all_items.merge(
        status[KEY_COLUMNS + ["표준_메뉴명_edit", "수동분류_edit", "review_status_edit", "classified_by"]],
        on=KEY_COLUMNS,
        how="left",
    )
    classified_by = merged["classified_by"].fillna("").astype(str).str.strip()
    review_status = merged["review_status_edit"].fillna("").astype(str).str.strip()
    has_manual_value = (
        (merged["표준_메뉴명_edit"].fillna("").astype(str).str.strip() != "")
        & (merged["수동분류_edit"].fillna("").astype(str).str.strip().isin(VALID_CATEGORIES))
    )
    return merged[
        ~(classified_by.isin(["llm", "human"]) | (review_status == "Y") | has_manual_value)
    ][[
        "store", "source", "brand", "item_name", "unitprice",
        "대표_menu_name", "menu_name_후보수", "menu_name_샘플", "main_menu_confidence",
    ]].reset_index(drop=True)


def build_prompt(batch: list[dict], examples: list[dict]) -> str:
    example_text = "\n".join(
        f'- "{r["item_name"]}" => 표준명 "{r["표준_메뉴명_edit"]}", '
        f'분류 "{r["수동분류_edit"]}", 메뉴대분류 "{r.get("메뉴대분류_edit", "")}", '
        f'메뉴중분류 "{r.get("메뉴중분류_edit", "")}", main_menu "{r.get("main_menu_edit", "")}"'
        for r in examples
    ) or "(예시 없음)"
    items_text = "\n".join(
        f'{i + 1}. store={r["store"]}, source={r["source"]}, brand={r.get("brand", "")}, '
        f'unitprice={r.get("unitprice", "")}, item_name="{r["item_name"]}", '
        f'대표_menu_name="{r.get("대표_menu_name", "")}", menu_name_후보수={r.get("menu_name_후보수", "")}, '
        f'menu_name_샘플="{r.get("menu_name_샘플", "")}"'
        for i, r in enumerate(batch)
    )
    return f"""도리당 F&B 상품명 정규화 작업입니다.
아래 상품명을 표준_메뉴명_edit, 수동분류_edit, main_menu_edit, 메뉴대분류_edit, 메뉴중분류_edit, 부모메뉴_확정방식으로 분류하세요.

허용 수동분류: {", ".join(VALID_CATEGORIES)}
허용 메뉴대분류: {", ".join(VALID_MENU_MAJOR)}
허용 부모메뉴_확정방식: 상품 단독 분류, menu_name 필요, 주문번호 기준 추론

규칙:
- 상품명만 보고 확정할 수 있는 분류는 부모메뉴_확정방식="상품 단독 분류"로 둡니다.
- menu_name_후보수=1이면 대표_menu_name을 main_menu_edit로 사용할 수 있습니다.
- menu_name_후보수>1인 추가/옵션 상품은 main_menu_edit를 비우고, 부모메뉴_확정방식="menu_name 필요"로 둡니다.
- "1인 추가"는 표준_메뉴명_edit="1인 추가", 수동분류_edit="사이드", 메뉴대분류_edit="추가메뉴", 메뉴중분류_edit="인원추가", main_menu_edit="", 부모메뉴_확정방식="menu_name 필요"로 분류합니다.

기존 승인 예시:
{example_text}

분류 대상:
{items_text}

반드시 JSON 배열만 응답하세요.
각 원소는 item_name, 표준_메뉴명_edit, 수동분류_edit, main_menu_edit, 메뉴대분류_edit, 메뉴중분류_edit, 부모메뉴_확정방식 키를 가져야 합니다.
수동분류_edit는 허용값 중 하나만 사용하세요.
"""


def _get_ollama_client():
    import ollama

    errors = []
    for host in dict.fromkeys(h for h in OLLAMA_HOSTS if h):
        try:
            probe_client = ollama.Client(host=host, timeout=5)
            model_names = [m["model"] for m in probe_client.list().get("models", [])]
        except Exception as e:
            errors.append(f"{host}: {e}")
            continue
        for candidate in MODEL_CANDIDATES:
            if any(candidate in model for model in model_names):
                logger.info("LLM 모델 선택: %s (%s)", candidate, host)
                return ollama.Client(host=host, timeout=120), candidate
        errors.append(f"{host}: 후보 모델 없음 {model_names}")
    raise RuntimeError("사용 가능한 LLM 모델 없음. " + " | ".join(errors))


def call_llm(prompt: str) -> list[dict]:
    client, model = _get_ollama_client()
    response = client.chat(
        model=model,
        messages=[
            {"role": "system", "content": "반드시 JSON만 응답하세요."},
            {"role": "user", "content": prompt},
        ],
        format="json",
        options={"num_predict": 4096},
        stream=False,
    )
    text = response.get("message", {}).get("content", "").strip()
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            for key in ("items", "results", "data"):
                value = parsed.get(key)
                if isinstance(value, list):
                    return value
            if {"item_name", "표준_메뉴명_edit", "수동분류_edit"}.issubset(parsed) or {"item_name", "표준_메뉴명", "수동분류"}.issubset(parsed):
                return [parsed]
    except json.JSONDecodeError:
        pass
    if "```json" in text:
        text = text.split("```json", 1)[1].split("```", 1)[0].strip()
    elif "```" in text:
        text = text.split("```", 1)[1].split("```", 1)[0].strip()
    start = text.find("[")
    end = text.rfind("]") + 1
    if start < 0 or end <= start:
        raise ValueError(f"JSON 배열을 찾지 못했습니다: {text[:200]}")
    parsed = json.loads(text[start:end])
    if isinstance(parsed, dict):
        for key in ("items", "results", "data"):
            value = parsed.get(key)
            if isinstance(value, list):
                return value
    if not isinstance(parsed, list):
        raise ValueError(f"JSON 배열 형식이 아닙니다: {text[:200]}")
    return parsed


def build_examples(map_df: pd.DataFrame) -> list[dict]:
    if map_df.empty:
        return []
    approved = map_df[
        (map_df["review_status_edit"].fillna("").astype(str).str.strip().apply(_normalize_review_status) == "Y")
        & (map_df["표준_메뉴명_edit"].fillna("").astype(str).str.strip() != "")
        & (map_df["수동분류_edit"].fillna("").astype(str).str.strip().isin(VALID_CATEGORIES))
    ]
    if approved.empty:
        return []
    return (
        approved[["item_name", "표준_메뉴명_edit", "수동분류_edit", "main_menu_edit", "메뉴대분류_edit", "메뉴중분류_edit"]]
        .drop_duplicates()
        .head(20)
        .to_dict("records")
    )


def _classify_batch(batch: list[dict], examples: list[dict]) -> list[dict]:
    results = call_llm(build_prompt(batch, examples))
    by_name = {str(r.get("item_name", "")).strip(): r for r in results}
    rows = []
    for item_idx, item in enumerate(batch):
        classified = by_name.get(item["item_name"], {})
        if not classified and item_idx < len(results):
            classified = results[item_idx]
        normalized = _normalize_classification(item, classified)
        rows.append({
            "store": item["store"],
            "source": item["source"],
            "brand": item.get("brand", ""),
            "item_name": item["item_name"],
            "unitprice": item.get("unitprice", ""),
            **normalized,
            "대표_menu_name": item.get("대표_menu_name", ""),
            "menu_name_후보수": item.get("menu_name_후보수", ""),
            "menu_name_샘플": item.get("menu_name_샘플", ""),
            "main_menu_confidence": item.get("main_menu_confidence", ""),
            "review_status_edit": "N",
            "classified_by": "llm",
            "updated_at": TODAY,
        })
    return rows


def classify_unmapped(unmapped: pd.DataFrame, map_df: pd.DataFrame, limit: int | None, dry_run: bool) -> list[dict]:
    if limit is not None:
        unmapped = unmapped.head(limit)
    if unmapped.empty:
        return []

    examples = build_examples(map_df)
    new_rows = []
    batches = [
        unmapped.iloc[i:i + BATCH_SIZE].to_dict("records")
        for i in range(0, len(unmapped), BATCH_SIZE)
    ]

    for idx, batch in enumerate(batches, start=1):
        logger.info("배치 처리: %d/%d (%d건)", idx, len(batches), len(batch))
        if dry_run:
            continue
        try:
            new_rows.extend(_classify_batch(batch, examples))
        except Exception as e:
            logger.warning("배치 %d 실패: %s", idx, e)
            if len(batch) <= 1:
                continue
            for item in batch:
                try:
                    new_rows.extend(_classify_batch([item], examples))
                except Exception as item_error:
                    logger.warning("단건 분류 실패: %s | %s", item["item_name"], item_error)
    return new_rows


def export_json(map_df: pd.DataFrame, dry_run: bool = False) -> None:
    if dry_run:
        logger.info("dry-run: JSON 저장 생략")
        return
    status = map_df["review_status_edit"].fillna("").astype(str).str.strip().apply(_normalize_review_status)
    valid = (
        (map_df["표준_메뉴명_edit"].fillna("").astype(str).str.strip() != "")
        & (map_df["수동분류_edit"].fillna("").astype(str).str.strip().isin(VALID_CATEGORIES))
    )
    needs_review = map_df.apply(_review_reason, axis=1).fillna("").astype(str).str.strip() != ""
    approved = map_df[valid & ((status == "Y") | ~needs_review)]
    result: dict[str, list[str]] = {}
    for _, row in approved.iterrows():
        std = str(row["표준_메뉴명_edit"]).strip()
        item = str(row["item_name"]).strip()
        if not std or not item:
            continue
        result.setdefault(std, [])
        if item not in result[std]:
            result[std].append(item)
    FIN_PRODUCT_MAP_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    FIN_PRODUCT_MAP_JSON_PATH.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info("JSON 저장: %s (%d 표준명)", FIN_PRODUCT_MAP_JSON_PATH, len(result))


def migrate_product_map(dry_run: bool = False, **context) -> dict:
    result = build_initial_map()
    existing = load_map()
    review_edits = load_review_map()
    if not result.empty and not existing.empty:
        current_value_cols = [
            "brand", "unitprice", "대표_menu_name", "menu_name_후보수",
            "menu_name_샘플", "main_menu_confidence",
        ]
        current_values = result[KEY_COLUMNS + current_value_cols].drop_duplicates(subset=KEY_COLUMNS, keep="last")
        existing = existing.merge(current_values, on=KEY_COLUMNS, how="inner", suffixes=("", "_current"))
        for col in current_value_cols:
            current_col = f"{col}_current"
            if current_col in existing.columns:
                if col in {"brand", "unitprice"}:
                    existing[col] = existing[col].where(existing[col].astype(str).str.strip() != "", existing[current_col])
                else:
                    existing[col] = existing[current_col]
                existing = existing.drop(columns=[current_col])
        result = (
            pd.concat([result, existing], ignore_index=True)
            .reindex(columns=MAP_COLUMNS, fill_value="")
            .drop_duplicates(subset=KEY_COLUMNS, keep="last")
            .sort_values(["store", "source", "item_name"])
            .reset_index(drop=True)
        )
    result = apply_review_edits(result, review_edits)
    result = _apply_menu_evidence_rules(result)
    result = _apply_classification_overrides(result)
    result["review_status_edit"] = result["review_status_edit"].apply(_normalize_review_status)
    review_rows = build_review_rows(result)
    duplicate_count = int(result.duplicated(subset=KEY_COLUMNS).sum())
    summary = {
        "target_stores": TARGET_STORES,
        "target_rows": int(len(result)),
        "approved": int((result["review_status_edit"] == "Y").sum()) if not result.empty else 0,
        "pending": int((result["review_status_edit"] == "N").sum()) if not result.empty else 0,
        "review_rows": int(len(review_rows)),
        "duplicate_keys": duplicate_count,
        "dry_run": dry_run,
        "output_path": str(FIN_PRODUCT_MAP_CSV_PATH),
        "review_output_path": str(FIN_PRODUCT_MAP_REVIEW_CSV_PATH),
    }

    if result.empty:
        logger.warning("대상 매장 데이터 없음: %s", TARGET_STORES)
    elif dry_run:
        logger.info("dry-run: CSV 저장 생략 (%d행, review %d행)", len(result), len(review_rows))
    else:
        write_map(result)
        write_review_map(review_rows)
        logger.info("fin_product_map.csv 저장: %s (%d행)", FIN_PRODUCT_MAP_CSV_PATH, len(result))
        logger.info("fin_product_map_review.csv 저장: %s (%d행)", FIN_PRODUCT_MAP_REVIEW_CSV_PATH, len(review_rows))
    return summary


def llm_product_map(dry_run: bool = False, limit: int | None = None, **context) -> dict:
    all_items = scan_target_items()
    map_df = apply_review_edits(load_map(), load_review_map())
    llm_targets = find_llm_targets(all_items, map_df)
    summary = {
        "target_stores": TARGET_STORES,
        "target_rows": int(len(all_items)),
        "already_llm_classified": int(len(all_items) - len(llm_targets)),
        "llm_targets": int(len(llm_targets)),
        "new_classified": 0,
        "dry_run": dry_run,
        "limit": limit,
        "output_path": str(FIN_PRODUCT_MAP_CSV_PATH),
        "review_output_path": str(FIN_PRODUCT_MAP_REVIEW_CSV_PATH),
    }

    if all_items.empty:
        logger.warning("대상 매장 데이터 없음: %s", TARGET_STORES)
        return summary

    new_rows = classify_unmapped(llm_targets, map_df, limit=limit, dry_run=dry_run)
    summary["new_classified"] = len(new_rows)

    if new_rows:
        new_df = pd.DataFrame(new_rows, columns=MAP_COLUMNS)
        map_df = pd.concat([map_df, new_df], ignore_index=True)
        map_df = (
            map_df.reindex(columns=MAP_COLUMNS, fill_value="")
            .drop_duplicates(subset=KEY_COLUMNS, keep="last")
            .reset_index(drop=True)
        )
    map_df = _apply_menu_evidence_rules(map_df)
    map_df = _apply_classification_overrides(map_df)
    map_df["review_status_edit"] = map_df["review_status_edit"].apply(_normalize_review_status)
    review_rows = build_review_rows(map_df)
    summary["review_rows"] = int(len(review_rows))

    if dry_run:
        logger.info("dry-run: LLM 호출 및 CSV 저장 생략")
    elif not map_df.empty:
        write_map(map_df)
        write_review_map(review_rows)
        logger.info("fin_product_map.csv 업데이트: %s (%d행)", FIN_PRODUCT_MAP_CSV_PATH, len(map_df))
        logger.info("fin_product_map_review.csv 저장: %s (%d행)", FIN_PRODUCT_MAP_REVIEW_CSV_PATH, len(review_rows))
    else:
        logger.info("신규 분류 대상 없음")

    export_json(map_df, dry_run=dry_run)
    return summary
