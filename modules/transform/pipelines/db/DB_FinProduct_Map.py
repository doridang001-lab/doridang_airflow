"""fin_product_map 생성 및 내부 LLM 증분 분류."""

import json
import logging
import os
import re
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import (
    FIN_PRODUCT_CSV_PATH,
    FIN_PRODUCT_MAP_JOIN_CSV_PATH,
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_RECENTLY_CSV_PATH,
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
    FIN_PRODUCT_MAP_TRAIN_JSON_PATH,
    MART_DB,
    existing_fin_product_csv_path,
    existing_fin_product_map_review_csv_path,
)
from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.qwen_client import (
    get_ollama_client_with_candidates,
    query_qwen_json,
)
from modules.transform.pipelines.db.DB_FinProduct_Rules import (
    build_rules_from_manual,
    classify_by_rules,
    evaluate_rule_change,
    load_rules,
    reconcile,
    rules_to_prompt_block,
    save_rule_proposal,
    save_rules,
    summarize_rules,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import iter_unified_sales_files
from modules.transform.pipelines.db.DB_ItemIdAllocator import (
    MANUAL_SOURCE_BASE,
    OKPOS_ADJUSTMENT_ITEM_ID,
    allocate_manual_item_ids,
    canonical_source,
    ensure_allocator_columns,
    is_manual_allocated_source,
    item_block_size,
    normalize_item_key,
)

TARGET_STORES = ["송파삼전점", "역삼점"]
TARGET_STORE_SET = {s.strip() for s in TARGET_STORES if s.strip()}
UNIFIED_SALES_GRP_DIR = MART_DB / "unified_sales_grp"
REVIEW_STATUS_COLUMN = "review_status_edit"
REVIEW_STATUS_DISPLAY_COLUMN = "검수유무"
REVIEW_APPROVED = "1"
REVIEW_PENDING = "0"
DUP_LABEL_COLUMN = "중복_수동분류"
MANUAL_CHICKEN_COLUMNS = ["닭유형_manual", "사이즈_manual", "닭사용량_manual"]
VALID_CHICKEN_TYPES = ("뼈닭", "순살")
VALID_CHICKEN_SIZES = ("소", "중", "대", "1인", "2인")
CHICKEN_USAGE_TABLE = {
    ("뼈닭", "소"): 0.5,
    ("뼈닭", "중"): 1.0,
    ("뼈닭", "대"): 1.5,
    ("순살", "소"): 0.4,
    ("순살", "중"): 0.8,
    ("순살", "대"): 1.2,
    ("순살", "1인"): 0.3,
    ("순살", "2인"): 0.6,
}
CHICKEN_USAGE_CANDIDATE_COLUMNS = [
    "store",
    "source",
    "brand",
    "item_id",
    "item_name",
    "닭유형_candidate",
    "사이즈_candidate",
    "닭사용량_candidate",
    "닭분류_근거",
    "닭분류_confidence",
]
MAP_COLUMNS = [
    "item_id",
    "item_key",
    "store_seq",
    "item_seq",
    "store",
    "source",
    "brand",
    "item_name",
    "unitprice",
    "표준_메뉴명_edit",
    "수동분류_edit",
    "대표메뉴",
    REVIEW_STATUS_COLUMN,
    "classified_by",
    "updated_at",
]
REVIEW_COLUMNS = [
    "item_id",
    "item_key",
    "store",
    "source",
    "brand",
    "item_name",
    "unitprice",
    "표준_메뉴명_edit",
    "수동분류_edit",
    DUP_LABEL_COLUMN,
    REVIEW_STATUS_DISPLAY_COLUMN,
    "검수사유",
    *MANUAL_CHICKEN_COLUMNS,
]
REVIEW_INTERNAL_COLUMNS = [
    REVIEW_STATUS_COLUMN if col == REVIEW_STATUS_DISPLAY_COLUMN else col
    for col in REVIEW_COLUMNS
]
RECENTLY_COLUMNS = [
    "source",
    "구분",
    "대메뉴",
    "중메뉴",
    "상품코드",
    "상품명",
    "판매단가",
    "표준_메뉴명",
    "exclude_check",
    "brand",
    "store",
    REVIEW_STATUS_COLUMN,
]
JOIN_COLUMNS = ["item_id", "store", "source", "brand", "standard_menu_name", "category"]
KEY_COLUMNS = ["store", "source", "brand", "item_id"]
EDIT_COLUMN_ALIASES = {
    "표준_메뉴명": "표준_메뉴명_edit",
    "수동분류": "수동분류_edit",
    "review_status": REVIEW_STATUS_COLUMN,
}
VALID_CATEGORIES = ["메인", "1인", "사이드", "기타", "주류", "음료", "토핑", "옵션", "세트", "리뷰"]
BATCH_SIZE = 5
TODAY = str(date.today())
_TRAIN_EXAMPLES_PER_LABEL = 10
MANUAL_UNKNOWN_ITEM_NAMES = frozenset({"메뉴미상(배민)", "메뉴미상(쿠팡)"})
MANUAL_UNKNOWN_SOURCES = frozenset({"배민수동", "쿠팡수동"})
AUTOMATIC_APPROVAL_CLASSIFIERS = frozenset({"auto_zero_price", "human_sibling", "rule"})
SET_COMPOSITE_MARKERS = ("+", "&", "콤보", "구성")
MAIN_MENU_KEYWORDS = ("도리탕", "우도리탕", "곱도리탕", "닭한마리", "삼계탕", "정식", "탕")
MAIN_MENU_EXCLUSION_KEYWORDS = ("추가", "토핑", "사리", "변경", "선택")

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


def _drop_price_item_names(df: pd.DataFrame) -> pd.DataFrame:
    """item_name이 순수 숫자(가격)인 행을 제거한다."""
    if df.empty or "item_name" not in df.columns:
        return df
    names = df["item_name"].fillna("").astype(str).str.strip()
    is_price = names.str.fullmatch(r"\d+")
    if bool(is_price.any()):
        logger.info("가격 item_name 행 제외: %d행", int(is_price.sum()))
    return df[~is_price].copy()


def _normalize_review_status(value: object) -> str:
    text = str(value or "").strip()
    normalized = text.lower()
    if normalized in {"1", "1.0", "y", "yes", "true", "approved", "승인", "완료", "검수완료"}:
        return REVIEW_APPROVED
    return REVIEW_PENDING


def _apply_review_status_aliases(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if REVIEW_STATUS_DISPLAY_COLUMN in result.columns:
        result[REVIEW_STATUS_COLUMN] = result[REVIEW_STATUS_DISPLAY_COLUMN]
    elif "review_status" in result.columns:
        result[REVIEW_STATUS_COLUMN] = result["review_status"]
    elif REVIEW_STATUS_COLUMN not in result.columns:
        result[REVIEW_STATUS_COLUMN] = ""
    return result


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


def _source_base_for_item_id(source: object) -> int | None:
    return MANUAL_SOURCE_BASE.get(canonical_source(source))


def _fill_item_identity_columns(df: pd.DataFrame, *, persist: bool = True) -> pd.DataFrame:
    result = df.copy()
    for col in ("item_id", "item_key", "store_seq", "item_seq", "source", "brand", "store", "item_name"):
        if col not in result.columns:
            result[col] = ""
        result[col] = result[col].fillna("").astype(str).str.strip()
    result["source"] = result["source"].map(canonical_source)
    legacy_okpos_adj = result["source"].eq("okpos") & result["item_id"].eq("__OKPOS_ADJ__")
    if legacy_okpos_adj.any():
        result.loc[legacy_okpos_adj, "item_id"] = OKPOS_ADJUSTMENT_ITEM_ID
    result["item_key"] = result["item_key"].where(
        result["item_key"] != "",
        result["item_name"].map(normalize_item_key),
    )

    allocated_mask = result["source"].map(is_manual_allocated_source)
    if allocated_mask.any():
        result.loc[allocated_mask, "item_id"] = allocate_manual_item_ids(
            result.loc[allocated_mask, ["source", "brand", "store", "item_id", "item_name", "unitprice"]]
            .rename(columns={"unitprice": "unit_price"}),
            persist=persist,
        )
        for idx in result[allocated_mask].index:
            base = _source_base_for_item_id(result.at[idx, "source"])
            if base is None:
                continue
            try:
                offset = int(result.at[idx, "item_id"]) - base
            except ValueError:
                continue
            block_size = item_block_size(result.at[idx, "source"])
            result.at[idx, "store_seq"] = str(offset // block_size)
            result.at[idx, "item_seq"] = str(offset % block_size)

    return result


def _classification_override(item_name: str) -> dict[str, str]:
    name = item_name.strip()
    if name in MANUAL_UNKNOWN_ITEM_NAMES:
        return {
            "표준_메뉴명_edit": name,
            "수동분류_edit": "기타",
            "classified_by": "rule",
        }
    if name == "1인 추가":
        return {
            "표준_메뉴명_edit": "1인 추가",
            "수동분류_edit": "사이드",
        }
    return {}


def _normalize_classification(item: dict, classified: dict) -> dict[str, str]:
    item_name = _strip_text(item.get("item_name"))
    values = dict(classified)
    values.update(_classification_override(item_name))

    label = _strip_text(values.get("수동분류_edit") or values.get("수동분류"))
    if label not in VALID_CATEGORIES:
        label = ""

    std_name = _strip_text(values.get("표준_메뉴명_edit") or values.get("표준_메뉴명")) or item_name

    return {
        "표준_메뉴명_edit": std_name,
        "수동분류_edit": label,
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
                if col == "classified_by" and _strip_text(result.at[idx, col]) not in {"", "rule"}:
                    continue
                result.at[idx, col] = value
    return result


def _representative_menu_name(series: pd.Series) -> str:
    values = series.fillna("").astype(str).str.strip()
    values = values[values != ""]
    if values.empty:
        return ""
    return str(values.value_counts(sort=True).index[0]).strip()


def scan_target_items(*, persist_identity: bool = True) -> pd.DataFrame:
    if not TARGET_STORE_SET:
        raise ValueError("TARGET_STORES가 비어 있습니다.")

    frames = []
    for file_path in iter_unified_sales_files():
        try:
            df = pd.read_parquet(file_path, columns=["item_id", "store", "source", "brand", "item_name", "unit_price", "menu_name"])
        except Exception as e:
            try:
                df = pd.read_parquet(file_path)
            except Exception as fallback_error:
                logger.warning("parquet 로드 실패: %s | %s | %s", file_path, e, fallback_error)
                continue
            for col in ("item_id", "store", "source", "brand", "item_name", "unit_price", "menu_name"):
                if col not in df.columns:
                    df[col] = ""
            df = df[["item_id", "store", "source", "brand", "item_name", "unit_price", "menu_name"]]
        if df.empty:
            continue
        for col in ("item_id", "store", "source", "brand", "item_name", "unit_price", "menu_name"):
            df[col] = df[col].fillna("").astype(str).str.strip()
        df["source"] = df["source"].map(canonical_source)
        df["item_key"] = df["item_name"].map(normalize_item_key)
        manual_unknown = (
            df["source"].isin(MANUAL_UNKNOWN_SOURCES)
            & df["item_name"].isin(MANUAL_UNKNOWN_ITEM_NAMES)
        )
        scoped = df[
            (df["store"].isin(TARGET_STORE_SET) | manual_unknown)
            & (df["item_name"] != "")
            & ~df["item_name"].str.fullmatch(r"\d+")
            & (df["item_id"] != "")
            & (df["item_id"] != OKPOS_ADJUSTMENT_ITEM_ID)
        ]
        if not scoped.empty:
            frames.append(scoped[["item_id", "item_key", "store", "source", "brand", "item_name", "unit_price", "menu_name"]])

    if not frames:
        return pd.DataFrame(columns=[
            "item_id", "item_key", "store_seq", "item_seq", "store", "source", "brand", "item_name", "unitprice", "대표메뉴",
        ])
    grouped = (
        pd.concat(frames, ignore_index=True)
        .groupby(KEY_COLUMNS)
        .agg(
            item_key=("item_key", _pick_common_value),
            item_name=("item_name", _pick_common_value),
            unitprice=("unit_price", _pick_common_value),
            대표메뉴=("menu_name", _representative_menu_name),
        )
        .reset_index()
    )
    grouped = _fill_item_identity_columns(grouped, persist=persist_identity)
    return (
        grouped
        .drop_duplicates(subset=KEY_COLUMNS, keep="last")
        .sort_values(["store", "source", "item_name"])
        .reset_index(drop=True)
    )


def build_initial_map(*, persist_identity: bool = True) -> pd.DataFrame:
    target_items = scan_target_items(persist_identity=persist_identity)
    if target_items.empty:
        return _empty_map()

    result = target_items.copy()
    result["표준_메뉴명_edit"] = result["item_name"]
    result["수동분류_edit"] = ""
    result["대표메뉴"] = result["대표메뉴"].fillna("").astype(str).str.strip()
    result[REVIEW_STATUS_COLUMN] = REVIEW_PENDING
    result["classified_by"] = ""
    result["updated_at"] = TODAY
    result = _apply_classification_overrides(result)
    return result[MAP_COLUMNS].drop_duplicates(subset=KEY_COLUMNS, keep="last").reset_index(drop=True)


def load_map() -> pd.DataFrame:
    if not FIN_PRODUCT_MAP_CSV_PATH.exists():
        return _empty_map()
    df = pd.read_csv(FIN_PRODUCT_MAP_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    df = _apply_review_status_aliases(df)
    df = _apply_edit_column_aliases(df)
    df = df.reindex(columns=MAP_COLUMNS, fill_value="")
    df = _drop_price_item_names(df)
    df = _fill_item_identity_columns(df, persist=False)
    df = _apply_classification_overrides(df)
    df[REVIEW_STATUS_COLUMN] = df[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
    return df


def _empty_review_map() -> pd.DataFrame:
    return pd.DataFrame(columns=REVIEW_INTERNAL_COLUMNS)


def _normalize_manual_chicken_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in MANUAL_CHICKEN_COLUMNS:
        if col not in result.columns:
            result[col] = ""
        result[col] = result[col].fillna("").astype(str).str.strip()
    return result


def _manual_chicken_values_exist(df: pd.DataFrame) -> bool:
    if df.empty:
        return False
    manual = _normalize_manual_chicken_columns(df.reindex(columns=MANUAL_CHICKEN_COLUMNS, fill_value=""))
    return bool(manual.ne("").any(axis=1).any())


def _restore_manual_chicken_columns(output_df: pd.DataFrame) -> pd.DataFrame:
    """DAG 재생성 시 사람이 엑셀로 입력한 닭 사용량 컬럼을 디스크에서 되살린다."""
    result = _normalize_manual_chicken_columns(output_df)
    path = existing_fin_product_map_review_csv_path()
    if not path.exists():
        return result
    try:
        prev = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as e:
        logger.warning("기존 검수 파일 로드 실패, 수기 컬럼 보존 생략: %s", e)
        return result
    if not any(col in prev.columns for col in MANUAL_CHICKEN_COLUMNS):
        return result

    prev = prev.reindex(columns=[*KEY_COLUMNS, *MANUAL_CHICKEN_COLUMNS], fill_value="")
    for col in KEY_COLUMNS:
        prev[col] = prev[col].fillna("").astype(str).str.strip()
    prev["source"] = prev["source"].map(canonical_source)
    prev = _normalize_manual_chicken_columns(prev)

    manual_by_key = {
        tuple(_strip_text(row.get(col)) for col in KEY_COLUMNS): {
            col: _strip_text(row.get(col)) for col in MANUAL_CHICKEN_COLUMNS
        }
        for row in prev.to_dict("records")
    }

    restored = 0
    for idx in result.index:
        key = tuple(_strip_text(result.at[idx, col]) for col in KEY_COLUMNS)
        values = manual_by_key.get(key)
        if not values or not any(values.values()):
            continue
        for col in MANUAL_CHICKEN_COLUMNS:
            result.at[idx, col] = values[col]
        restored += 1

    logger.info(
        "닭 사용량 수기 보존: %d행 (신규 %d행)",
        restored,
        int(len(result) - restored),
    )
    return result


def _backup_manual_chicken_columns() -> None:
    path = existing_fin_product_map_review_csv_path()
    if not path.exists():
        return
    try:
        prev = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")
        prev = prev.reindex(columns=[*KEY_COLUMNS, *MANUAL_CHICKEN_COLUMNS], fill_value="")
        prev = _normalize_manual_chicken_columns(prev)
        if not _manual_chicken_values_exist(prev):
            return
        backup_dir = FIN_PRODUCT_MAP_REVIEW_CSV_PATH.parent / "_backup"
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_dir / f"chicken_usage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        prev.to_csv(backup_path, index=False, encoding="utf-8-sig")
        logger.info("닭 사용량 수기 백업 스냅샷 저장: %s", backup_path)
    except Exception as e:
        logger.warning("닭 사용량 수기 백업 스냅샷 실패: %s", e)


def _validate_chicken_usage(df: pd.DataFrame) -> int:
    warnings = 0
    if df.empty:
        return warnings

    work = _normalize_manual_chicken_columns(df)
    for idx, row in work.iterrows():
        chicken_type = _strip_text(row.get("닭유형_manual"))
        size = _strip_text(row.get("사이즈_manual"))
        usage_text = _strip_text(row.get("닭사용량_manual"))
        values = [chicken_type, size, usage_text]
        filled_count = sum(value != "" for value in values)
        if filled_count == 0:
            continue

        item_id = _strip_text(row.get("item_id"))
        item_name = _strip_text(row.get("item_name"))
        row_has_warning = False
        if filled_count != len(MANUAL_CHICKEN_COLUMNS):
            logger.warning(
                "닭 사용량 부분 입력: item_id=%s item_name=%s 값=%s/%s/%s",
                item_id,
                item_name,
                chicken_type,
                size,
                usage_text,
            )
            warnings += 1
            row_has_warning = True
        if chicken_type and chicken_type not in VALID_CHICKEN_TYPES:
            logger.warning("닭 유형 허용값 불일치: item_id=%s item_name=%s 입력=%s", item_id, item_name, chicken_type)
            warnings += 1
            row_has_warning = True
        if size and size not in VALID_CHICKEN_SIZES:
            logger.warning("닭 사이즈 허용값 불일치: item_id=%s item_name=%s 입력=%s", item_id, item_name, size)
            warnings += 1
            row_has_warning = True

        usage = None
        if usage_text:
            try:
                usage = float(usage_text)
            except ValueError:
                logger.warning("닭 사용량 숫자 변환 실패: item_id=%s item_name=%s 입력=%s", item_id, item_name, usage_text)
                warnings += 1
                row_has_warning = True

        expected = CHICKEN_USAGE_TABLE.get((chicken_type, size))
        if filled_count == len(MANUAL_CHICKEN_COLUMNS) and not row_has_warning and usage is not None and expected is None:
            logger.warning(
                "닭 사용량 환산표 불일치: item_id=%s item_name=%s 입력=%s 기대=%s",
                item_id,
                item_name,
                usage_text,
                "",
            )
            warnings += 1
        elif filled_count == len(MANUAL_CHICKEN_COLUMNS) and not row_has_warning and usage is not None and usage != expected:
            logger.warning(
                "닭 사용량 환산표 불일치: item_id=%s item_name=%s 입력=%s 기대=%s",
                item_id,
                item_name,
                usage_text,
                expected,
            )
            warnings += 1
    return warnings


def load_review_map() -> pd.DataFrame:
    review_path = existing_fin_product_map_review_csv_path()
    if not review_path.exists():
        return _empty_review_map()
    df = pd.read_csv(review_path, dtype=str, encoding="utf-8-sig").fillna("")
    df = _apply_review_status_aliases(df)
    df = _apply_edit_column_aliases(df)
    df = df.reindex(columns=REVIEW_INTERNAL_COLUMNS, fill_value="")
    df = _normalize_manual_chicken_columns(df)
    if "source" in df.columns:
        df["source"] = df["source"].fillna("").astype(str).str.strip().map(canonical_source)
    if "item_key" in df.columns and "item_name" in df.columns:
        df["item_key"] = df["item_key"].fillna("").astype(str).str.strip().where(
            df["item_key"].fillna("").astype(str).str.strip() != "",
            df["item_name"].fillna("").astype(str).map(normalize_item_key),
        )
    df[REVIEW_STATUS_COLUMN] = df[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
    return df


def write_map(map_df: pd.DataFrame) -> None:
    FIN_PRODUCT_MAP_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_MAP_CSV_PATH.with_suffix(".tmp")
    try:
        map_df = _fill_item_identity_columns(map_df.reindex(columns=MAP_COLUMNS, fill_value=""), persist=False)
        map_df.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_MAP_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def write_review_map(review_df: pd.DataFrame) -> int:
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_MAP_REVIEW_CSV_PATH.with_suffix(".tmp")
    try:
        output_df = review_df.reindex(columns=REVIEW_INTERNAL_COLUMNS, fill_value="").copy()
        output_df = _restore_manual_chicken_columns(output_df)
        if "item_key" in output_df.columns and "item_name" in output_df.columns:
            output_df["item_key"] = output_df["item_key"].fillna("").astype(str).str.strip().where(
                output_df["item_key"].fillna("").astype(str).str.strip() != "",
                output_df["item_name"].fillna("").astype(str).map(normalize_item_key),
            )
        output_df = _mark_duplicate_labels(output_df)
        output_df[REVIEW_STATUS_DISPLAY_COLUMN] = output_df[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
        warning_count = _validate_chicken_usage(output_df)
        output_df.reindex(columns=REVIEW_COLUMNS, fill_value="").to_csv(tmp, index=False, encoding="utf-8-sig")
        _backup_manual_chicken_columns()
        _safe_replace(tmp, FIN_PRODUCT_MAP_REVIEW_CSV_PATH)
        return warning_count
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def _recently_key_columns(df: pd.DataFrame) -> list[str]:
    return [col for col in ("store", "source", "brand", "상품코드") if col in df.columns]


def load_recently_map() -> pd.DataFrame:
    if not FIN_PRODUCT_MAP_RECENTLY_CSV_PATH.exists():
        return pd.DataFrame(columns=RECENTLY_COLUMNS)
    df = pd.read_csv(FIN_PRODUCT_MAP_RECENTLY_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
    df = df.reindex(columns=RECENTLY_COLUMNS, fill_value="")
    for col in RECENTLY_COLUMNS:
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["source"] = df["source"].map(canonical_source)
    df[REVIEW_STATUS_COLUMN] = df[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
    return df


def _label_from_recently(row: pd.Series, current_label: str = "") -> str:
    if current_label in VALID_CATEGORIES:
        return current_label
    major = _strip_text(row.get("대메뉴"))
    if major == "메인메뉴":
        return "메인"
    if major == "사이드":
        return "사이드"
    if major == "추가메뉴":
        return "토핑"
    return ""


def apply_recently_edits(map_df: pd.DataFrame, recently_df: pd.DataFrame) -> pd.DataFrame:
    if recently_df.empty:
        return map_df

    result = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    result = _fill_item_identity_columns(result, persist=False)
    approved = recently_df[
        recently_df[REVIEW_STATUS_COLUMN].fillna("").astype(str).str.strip().apply(_normalize_review_status) == REVIEW_APPROVED
    ].copy()
    if approved.empty:
        return result

    approved = approved.drop_duplicates(subset=_recently_key_columns(approved), keep="last")
    new_rows = []
    for _, recent_row in approved.iterrows():
        source = canonical_source(recent_row.get("source"))
        item_id = _strip_text(recent_row.get("상품코드"))
        item_name = _strip_text(recent_row.get("상품명"))
        store = _strip_text(recent_row.get("store"))
        brand = _strip_text(recent_row.get("brand"))
        std_name = _strip_text(recent_row.get("표준_메뉴명")) or item_name
        if not item_id or not item_name or not std_name:
            continue

        key_mask = (
            result["source"].fillna("").astype(str).str.strip().map(canonical_source).eq(source)
            & result["item_id"].fillna("").astype(str).str.strip().eq(item_id)
        )
        if store:
            key_mask &= result["store"].fillna("").astype(str).str.strip().eq(store)
        if brand:
            key_mask &= result["brand"].fillna("").astype(str).str.strip().eq(brand)

        if bool(key_mask.any()):
            idx = result.index[key_mask]
            approved_map_value = (
                result.loc[idx, REVIEW_STATUS_COLUMN]
                .fillna("")
                .astype(str)
                .str.strip()
                .apply(_normalize_review_status)
                .eq(REVIEW_APPROVED)
                & (result.loc[idx, "표준_메뉴명_edit"].fillna("").astype(str).str.strip() != "")
            )
            if bool(approved_map_value.any()):
                continue
            current_label = _strip_text(result.loc[idx, "수동분류_edit"].iloc[-1])
            label = _label_from_recently(recent_row, current_label)
            result.loc[idx, "표준_메뉴명_edit"] = std_name
            if label in VALID_CATEGORIES:
                result.loc[idx, "수동분류_edit"] = label
                result.loc[idx, REVIEW_STATUS_COLUMN] = REVIEW_APPROVED
                result.loc[idx, "classified_by"] = "human"
            else:
                result.loc[idx, REVIEW_STATUS_COLUMN] = REVIEW_PENDING
                result.loc[idx, "classified_by"] = ""
            result.loc[idx, "updated_at"] = TODAY
            continue

        item_key = normalize_item_key(item_name)
        label = _label_from_recently(recent_row)
        new_rows.append({
            "item_id": item_id,
            "item_key": item_key,
            "store_seq": "",
            "item_seq": "",
            "store": store,
            "source": source,
            "brand": brand,
            "item_name": item_name,
            "unitprice": _strip_text(recent_row.get("판매단가")),
            "표준_메뉴명_edit": std_name,
            "수동분류_edit": label if label in VALID_CATEGORIES else "",
            "대표메뉴": "",
            REVIEW_STATUS_COLUMN: REVIEW_APPROVED if label in VALID_CATEGORIES else REVIEW_PENDING,
            "classified_by": "human" if label in VALID_CATEGORIES else "",
            "updated_at": TODAY,
        })

    if new_rows:
        result = pd.concat([result, pd.DataFrame(new_rows, columns=MAP_COLUMNS)], ignore_index=True)
    return result.reindex(columns=MAP_COLUMNS, fill_value="").drop_duplicates(
        subset=KEY_COLUMNS,
        keep="last",
    ).reset_index(drop=True)


def _load_recently_source_rows() -> pd.DataFrame:
    source_path = existing_fin_product_csv_path()
    if not source_path.exists():
        return pd.DataFrame(columns=RECENTLY_COLUMNS)
    df = pd.read_csv(source_path, dtype=str, encoding="utf-8-sig").fillna("")
    for col in ("source", "구분", "대메뉴", "중메뉴", "상품코드", "상품명", "판매단가", "exclude_check", "brand", "store", "is_latest", "updated_at"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["source"] = df["source"].map(canonical_source)
    latest = df["is_latest"].str.lower().isin({"1", "1.0", "y", "yes", "true"})
    if latest.any():
        df = df[latest].copy()
    if "updated_at" in df.columns:
        df["_updated_at_ts"] = pd.to_datetime(df["updated_at"], errors="coerce")
        df = df.sort_values(["_updated_at_ts", "source", "상품명"], ascending=[False, True, True], na_position="last")
    return df[["source", "구분", "대메뉴", "중메뉴", "상품코드", "상품명", "판매단가", "exclude_check", "brand", "store"]].drop_duplicates(
        subset=["store", "source", "brand", "상품코드"],
        keep="first",
    )


def build_recently_rows(map_df: pd.DataFrame) -> pd.DataFrame:
    source_rows = _load_recently_source_rows()
    map_rows = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    map_rows = _fill_item_identity_columns(map_rows, persist=False)
    map_rows = map_rows.drop_duplicates(subset=["store", "source", "brand", "item_id"], keep="last")
    std_cols = [
        "store", "source", "brand", "item_id",
        "표준_메뉴명_edit", REVIEW_STATUS_COLUMN,
    ]

    if source_rows.empty:
        out = map_rows.rename(columns={
            "item_id": "상품코드",
            "item_name": "상품명",
            "unitprice": "판매단가",
            "표준_메뉴명_edit": "표준_메뉴명",
        })
        out["구분"] = ""
        out["대메뉴"] = ""
        out["중메뉴"] = ""
        out["exclude_check"] = ""
        return out.reindex(columns=RECENTLY_COLUMNS, fill_value="")

    out = source_rows.merge(
        map_rows[std_cols].rename(columns={"item_id": "상품코드"}),
        on=["store", "source", "brand", "상품코드"],
        how="left",
    )
    out["표준_메뉴명"] = out["표준_메뉴명_edit"].fillna("").astype(str).str.strip()
    out[REVIEW_STATUS_COLUMN] = out[REVIEW_STATUS_COLUMN].fillna("").astype(str).str.strip()
    return out.reindex(columns=RECENTLY_COLUMNS, fill_value="")


def write_recently_map(map_df: pd.DataFrame) -> None:
    FIN_PRODUCT_MAP_RECENTLY_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_MAP_RECENTLY_CSV_PATH.with_suffix(".tmp")
    try:
        build_recently_rows(map_df).to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_MAP_RECENTLY_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def build_join_map(map_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if map_df.empty:
        return pd.DataFrame(columns=JOIN_COLUMNS), pd.DataFrame(columns=JOIN_COLUMNS + ["item_name"])

    work = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    for col in (
        "item_id",
        "store",
        "source",
        "brand",
        "item_name",
        "표준_메뉴명_edit",
        "수동분류_edit",
        REVIEW_STATUS_COLUMN,
    ):
        work[col] = work[col].fillna("").astype(str).str.strip()
    work[REVIEW_STATUS_COLUMN] = work[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
    work["source"] = work["source"].map(canonical_source)
    work = work.rename(columns={
        "표준_메뉴명_edit": "standard_menu_name",
        "수동분류_edit": "category",
    })
    work = work[
        (work[REVIEW_STATUS_COLUMN] == REVIEW_APPROVED)
        & (work["item_id"] != "")
        & (work["store"] != "")
        & (work["source"] != "")
        & (work["brand"] != "")
        & (work["standard_menu_name"] != "")
        & (work["category"] != "")
    ].copy()
    if work.empty:
        return pd.DataFrame(columns=JOIN_COLUMNS), pd.DataFrame(columns=JOIN_COLUMNS + ["item_name"])

    key_cols = ["item_id", "store", "source", "brand"]
    standard_counts = (
        work.groupby(key_cols)["standard_menu_name"]
        .nunique()
        .reset_index(name="standard_menu_name_count")
    )
    conflict_keys = standard_counts[standard_counts["standard_menu_name_count"] > 1][key_cols]
    if not conflict_keys.empty:
        conflicts = work.merge(conflict_keys, on=key_cols, how="inner")[
            key_cols + ["standard_menu_name", "category", "item_name"]
        ].sort_values(key_cols + ["standard_menu_name", "category", "item_name"])
        work = work.merge(conflict_keys.assign(_join_conflict="1"), on=key_cols, how="left")
        work = work[work["_join_conflict"].fillna("") == ""].drop(columns=["_join_conflict"])
    else:
        conflicts = pd.DataFrame(columns=JOIN_COLUMNS + ["item_name"])

    join_rows = (
        work[JOIN_COLUMNS]
        .drop_duplicates(subset=key_cols + ["standard_menu_name"], keep="last")
        .drop_duplicates(subset=key_cols, keep="last")
        .sort_values(["store", "source", "brand", "item_id"])
        .reset_index(drop=True)
    )
    return join_rows, conflicts.reset_index(drop=True)


def _count_join_excluded_pending(map_df: pd.DataFrame) -> int:
    if map_df.empty:
        return 0
    work = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    for col in (
        "item_id",
        "store",
        "source",
        "brand",
        "표준_메뉴명_edit",
        "수동분류_edit",
        REVIEW_STATUS_COLUMN,
    ):
        work[col] = work[col].fillna("").astype(str).str.strip()
    complete = (
        work["item_id"].ne("")
        & work["store"].ne("")
        & work["source"].ne("")
        & work["brand"].ne("")
        & work["표준_메뉴명_edit"].ne("")
        & work["수동분류_edit"].isin(VALID_CATEGORIES)
    )
    pending = work[REVIEW_STATUS_COLUMN].apply(_normalize_review_status).ne(REVIEW_APPROVED)
    return int((complete & pending).sum())


def _format_join_conflicts(conflicts: pd.DataFrame, limit: int = 10) -> str:
    if conflicts.empty:
        return ""
    lines = []
    key_cols = ["item_id", "store", "source", "brand"]
    grouped = conflicts.groupby(key_cols, dropna=False)
    for key, rows in list(grouped)[:limit]:
        item_id, store, source, brand = key
        standard_names = ", ".join(sorted(rows["standard_menu_name"].dropna().astype(str).unique().tolist()))
        categories = ", ".join(sorted(rows["category"].dropna().astype(str).unique().tolist()))
        samples = ", ".join(rows["item_name"].dropna().astype(str).unique().tolist()[:3])
        lines.append(
            f"- item_id={item_id}, store={store}, source={source}, brand={brand} | "
            f"standard_menu_name={standard_names} | category={categories} | samples={samples}"
        )
    remaining = len(grouped) - len(lines)
    if remaining > 0:
        lines.append(f"- 외 {remaining}개 key")
    return "\n".join(lines)


def _format_duplicate_labels(review_rows: pd.DataFrame, limit: int = 10) -> str:
    if review_rows.empty or DUP_LABEL_COLUMN not in review_rows.columns:
        return ""
    dup = review_rows[review_rows[DUP_LABEL_COLUMN].fillna("").astype(str).str.strip() == "Y"]
    if dup.empty:
        return ""
    lines = []
    grouped = dup.groupby(["source", "item_id"], dropna=False)
    for (source, item_id), rows in list(grouped)[:limit]:
        labels = ", ".join(
            sorted(
                rows["수동분류_edit"]
                .dropna()
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .unique()
                .tolist()
            )
        )
        std_names = ", ".join(
            sorted(
                rows["표준_메뉴명_edit"]
                .dropna()
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .unique()
                .tolist()
            )
        )
        samples = ", ".join(rows["item_name"].dropna().astype(str).unique().tolist()[:3])
        lines.append(
            f"- item_id={item_id}, source={source} | 수동분류={labels} | "
            f"표준명={std_names} | samples={samples}"
        )
    remaining = len(grouped) - len(lines)
    if remaining > 0:
        lines.append(f"- 외 {remaining}개 key")
    return "\n".join(lines)


def _notify_duplicate_labels(review_rows: pd.DataFrame) -> int:
    if review_rows.empty or DUP_LABEL_COLUMN not in review_rows.columns:
        return 0
    dup = review_rows[review_rows[DUP_LABEL_COLUMN].fillna("").astype(str).str.strip() == "Y"]
    dup_key_count = int(dup.drop_duplicates(subset=["source", "item_id"]).shape[0])
    if dup_key_count:
        detail = _format_duplicate_labels(review_rows)
        send_telegram(
            "[상품 매핑] 수동분류 중복 감지\n"
            f"중복 상품: {dup_key_count}개 (같은 상품에 수동분류 2개 이상)\n"
            "fin_product_map_review_input.csv의 중복_수동분류=Y 행을 확인해주세요.\n"
            f"{detail}"
        )
    return dup_key_count


def write_join_map(map_df: pd.DataFrame) -> dict:
    FIN_PRODUCT_MAP_JOIN_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    join_rows, conflicts = build_join_map(map_df)
    tmp = FIN_PRODUCT_MAP_JOIN_CSV_PATH.with_suffix(".tmp")
    try:
        join_rows.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_MAP_JOIN_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
    conflict_key_count = int(conflicts.drop_duplicates(subset=["item_id", "store", "source", "brand"]).shape[0])
    if conflict_key_count:
        detail = _format_join_conflicts(conflicts)
        send_telegram(
            "[상품 매핑] 주문서 join 표준명 충돌\n"
            f"충돌 key: {conflict_key_count}개\n"
            "해당 key는 fin_product_map_join.csv에서 제외했습니다.\n"
            f"{detail}"
        )
    return {
        "join_rows": int(len(join_rows)),
        "join_conflict_keys": conflict_key_count,
        "join_output_path": str(FIN_PRODUCT_MAP_JOIN_CSV_PATH),
    }


def _review_reason(row: pd.Series) -> str:
    classified_by = _strip_text(row.get("classified_by"))
    if classified_by == "human_sibling":
        return "자동복사(동일상품명 코드분리)"
    if classified_by == "auto_zero_price":
        return "기존 0원 자동분류, 1차 검수 필요"
    if classified_by == "main_set_guard":
        return "메인/세트 기준 자동보정, 1차 검수 필요"
    if classified_by == "llm_unresolved":
        return "LLM 분류 실패, 수동분류 미입력"
    if _normalize_review_status(row.get(REVIEW_STATUS_COLUMN)) == REVIEW_APPROVED:
        return "검수완료"
    reasons = []
    std_name = _strip_text(row.get("표준_메뉴명_edit"))
    label = _strip_text(row.get("수동분류_edit"))

    if not std_name:
        reasons.append("표준명 미입력")
    if label not in VALID_CATEGORIES:
        reasons.append("수동분류 미입력")
    elif label == "기타":
        reasons.append("기타 분류 확인")
    if classified_by.startswith("rule/llm_conflict"):
        reasons.append(classified_by.replace("rule/llm_conflict", "규칙/LLM 불일치", 1))
    if not reasons and classified_by.startswith("llm"):
        reasons.append("LLM 분류 확인")
    return ", ".join(dict.fromkeys(reasons))


def _mark_duplicate_labels(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in ["source", "item_id", "수동분류_edit"]:
        if col not in result.columns:
            result[col] = ""
        result[col] = result[col].fillna("").astype(str).str.strip()

    if result.empty:
        result[DUP_LABEL_COLUMN] = "N"
        return result

    work = result[(result["source"] != "") & (result["item_id"] != "") & (result["수동분류_edit"] != "")]
    conflict_keys = set()
    if not work.empty:
        conflict_df = (
            work.groupby(["source", "item_id"])["수동분류_edit"]
            .nunique()
            .reset_index(name="분류_종류수")
        )
        conflicts = conflict_df[conflict_df["분류_종류수"] > 1]
        conflict_keys = set(zip(conflicts["source"], conflicts["item_id"]))

    keys = list(zip(result["source"], result["item_id"]))
    result[DUP_LABEL_COLUMN] = ["Y" if key in conflict_keys else "N" for key in keys]
    return result


def build_review_rows(map_df: pd.DataFrame) -> pd.DataFrame:
    if map_df.empty:
        return _empty_review_map()
    result = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    result["검수사유"] = result.apply(_review_reason, axis=1)
    if result.empty:
        return _empty_review_map()
    result = (
        result.reindex(columns=REVIEW_INTERNAL_COLUMNS, fill_value="")
        .drop_duplicates(subset=KEY_COLUMNS, keep="last")
        .sort_values(["store", "source", "검수사유", "item_name"])
        .reset_index(drop=True)
    )
    return _mark_duplicate_labels(result)


def apply_review_edits(map_df: pd.DataFrame, review_df: pd.DataFrame) -> pd.DataFrame:
    if map_df.empty or review_df.empty:
        return map_df

    result = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    reviews = review_df.reindex(columns=REVIEW_INTERNAL_COLUMNS, fill_value="").copy()
    if reviews.empty:
        return result

    reviews = reviews.drop_duplicates(subset=KEY_COLUMNS, keep="last")
    for _, review_row in reviews.iterrows():
        key_mask = pd.Series(True, index=result.index)
        for col in KEY_COLUMNS:
            key_mask = key_mask & (result[col].fillna("").astype(str).str.strip() == _strip_text(review_row.get(col)))
        if not bool(key_mask.any()):
            continue

        idx = result.index[key_mask]
        rv_std = _strip_text(review_row.get("표준_메뉴명_edit"))
        rv_label = _strip_text(review_row.get("수동분류_edit"))
        cur_std = _strip_text(result.loc[idx, "표준_메뉴명_edit"].iloc[-1])
        cur_label = _strip_text(result.loc[idx, "수동분류_edit"].iloc[-1])
        is_approved = _normalize_review_status(review_row.get(REVIEW_STATUS_COLUMN)) == REVIEW_APPROVED
        std_changed = rv_std != "" and rv_std != cur_std
        label_changed = rv_label in VALID_CATEGORIES and rv_label != cur_label

        if not (is_approved or std_changed or label_changed):
            continue

        std_name = rv_std or (_strip_text(review_row.get("item_name")) if is_approved else "")
        label = _strip_text(review_row.get("수동분류_edit"))
        if std_name != "":
            result.loc[idx, "표준_메뉴명_edit"] = std_name
        if label in VALID_CATEGORIES:
            result.loc[idx, "수동분류_edit"] = label

        result.loc[idx, REVIEW_STATUS_COLUMN] = REVIEW_APPROVED if is_approved else REVIEW_PENDING
        if is_approved or std_changed or label_changed:
            result.loc[idx, "classified_by"] = "human"
        result.loc[idx, "updated_at"] = TODAY
    return result


def _reset_automatic_approvals(
    map_df: pd.DataFrame,
    review_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, int, set[tuple[str, ...]]]:
    result = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    reviews = review_df.reindex(columns=REVIEW_INTERNAL_COLUMNS, fill_value="").copy()
    if result.empty:
        return result, reviews, 0, set()

    classified_by = result["classified_by"].fillna("").astype(str).str.strip()
    review_status = result[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
    reset_mask = (
        classified_by.isin(AUTOMATIC_APPROVAL_CLASSIFIERS)
        & review_status.eq(REVIEW_APPROVED)
    )
    reset_count = int(reset_mask.sum())
    if not reset_count:
        return result, reviews, 0, set()

    reset_keys = {
        tuple(_strip_text(row.get(col)) for col in KEY_COLUMNS)
        for row in result.loc[reset_mask, KEY_COLUMNS].to_dict("records")
    }
    result.loc[reset_mask, REVIEW_STATUS_COLUMN] = REVIEW_PENDING

    if not reviews.empty:
        review_keys = reviews.reindex(columns=KEY_COLUMNS, fill_value="").apply(
            lambda row: tuple(_strip_text(row.get(col)) for col in KEY_COLUMNS),
            axis=1,
        )
        reviews.loc[review_keys.isin(reset_keys), REVIEW_STATUS_COLUMN] = REVIEW_PENDING

    logger.info("기존 자동승인 1차 검수 대기로 환원: %d행", reset_count)
    return result, reviews, reset_count, reset_keys


def _align_review_rows_for_forced_review(
    review_df: pd.DataFrame,
    map_df: pd.DataFrame,
    forced_keys: set[tuple[str, ...]],
) -> pd.DataFrame:
    reviews = review_df.reindex(columns=REVIEW_INTERNAL_COLUMNS, fill_value="").copy()
    if reviews.empty or not forced_keys:
        return reviews

    current = map_df.reindex(columns=MAP_COLUMNS, fill_value="").drop_duplicates(
        subset=KEY_COLUMNS,
        keep="last",
    )
    current_by_key = {
        tuple(_strip_text(row.get(col)) for col in KEY_COLUMNS): row
        for row in current.to_dict("records")
    }
    for idx, review_row in reviews.iterrows():
        key = tuple(_strip_text(review_row.get(col)) for col in KEY_COLUMNS)
        if key not in forced_keys:
            continue
        map_row = current_by_key.get(key, {})
        reviews.at[idx, REVIEW_STATUS_COLUMN] = REVIEW_PENDING
        reviews.at[idx, "표준_메뉴명_edit"] = _strip_text(map_row.get("표준_메뉴명_edit"))
        reviews.at[idx, "수동분류_edit"] = _strip_text(map_row.get("수동분류_edit"))
    return reviews


def _suppress_recently_approvals(
    recently_df: pd.DataFrame,
    forced_keys: set[tuple[str, ...]],
) -> pd.DataFrame:
    if recently_df.empty or not forced_keys:
        return recently_df
    result = recently_df.copy()
    for col in ("store", "source", "brand", "상품코드", REVIEW_STATUS_COLUMN):
        if col not in result.columns:
            result[col] = ""
        result[col] = result[col].fillna("").astype(str).str.strip()
    recent_keys = result.apply(
        lambda row: (
            _strip_text(row.get("store")),
            canonical_source(row.get("source")),
            _strip_text(row.get("brand")),
            _strip_text(row.get("상품코드")),
        ),
        axis=1,
    )
    result.loc[recent_keys.isin(forced_keys), REVIEW_STATUS_COLUMN] = REVIEW_PENDING
    return result


def _seed_split_rows_from_siblings(map_df: pd.DataFrame) -> pd.DataFrame:
    if map_df.empty:
        return map_df

    result = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    for col in (
        "store", "source", "brand", "item_key", "표준_메뉴명_edit",
        "수동분류_edit", REVIEW_STATUS_COLUMN, "classified_by",
    ):
        result[col] = result[col].fillna("").astype(str).str.strip()
    result[REVIEW_STATUS_COLUMN] = result[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)

    seeded = 0
    for _, group in result.groupby(["store", "source", "brand", "item_key"], dropna=False):
        donors = group[
            (group[REVIEW_STATUS_COLUMN] == REVIEW_APPROVED)
            & (group["표준_메뉴명_edit"] != "")
            & group["수동분류_edit"].isin(VALID_CATEGORIES)
        ].copy()
        if donors.empty:
            continue

        donors["_rank"] = donors["classified_by"].eq("human").map({True: 0, False: 1})
        donor = donors.sort_values(["_rank", "updated_at"]).iloc[0]
        targets = group[
            (group["표준_메뉴명_edit"] == "")
            | (~group["수동분류_edit"].isin(VALID_CATEGORIES))
        ]
        if targets.empty:
            continue

        idx = targets.index
        result.loc[idx, "표준_메뉴명_edit"] = donor["표준_메뉴명_edit"]
        result.loc[idx, "수동분류_edit"] = donor["수동분류_edit"]
        result.loc[idx, REVIEW_STATUS_COLUMN] = REVIEW_PENDING
        result.loc[idx, "classified_by"] = "human_sibling"
        result.loc[idx, "updated_at"] = TODAY
        seeded += len(idx)

    if seeded:
        logger.info("동일 상품명 코드분리 행 자동복사: %d행", seeded)
    return result


def _set_policy_text(row: pd.Series | dict) -> str:
    return " ".join(
        _strip_text(row.get(col))
        for col in ("item_name", "대표메뉴", "표준_메뉴명_edit")
        if _strip_text(row.get(col))
    )


def _has_composite_set_evidence(value: pd.Series | dict | str) -> bool:
    text = value if isinstance(value, str) else _set_policy_text(value)
    return any(marker in _strip_text(text) for marker in SET_COMPOSITE_MARKERS)


def _has_main_menu_evidence(value: pd.Series | dict | str) -> bool:
    text = value if isinstance(value, str) else _strip_text(value.get("item_name"))
    normalized = _strip_text(text)
    return (
        any(keyword in normalized for keyword in MAIN_MENU_KEYWORDS)
        and not any(keyword in normalized for keyword in MAIN_MENU_EXCLUSION_KEYWORDS)
    )


def _apply_main_set_policy(
    map_df: pd.DataFrame,
) -> tuple[pd.DataFrame, int, int, set[tuple[str, ...]]]:
    if map_df.empty:
        return map_df, 0, 0, set()

    result = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    for col in (
        "item_name",
        "대표메뉴",
        "표준_메뉴명_edit",
        "수동분류_edit",
        REVIEW_STATUS_COLUMN,
        "classified_by",
    ):
        result[col] = result[col].fillna("").astype(str).str.strip()
    result[REVIEW_STATUS_COLUMN] = result[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)

    target_mask = (
        result[REVIEW_STATUS_COLUMN].ne(REVIEW_APPROVED)
        & result["classified_by"].ne("human")
    )
    corrected = 0
    unresolved = 0
    changed_keys: set[tuple[str, ...]] = set()
    for idx in result.index[target_mask]:
        text = _set_policy_text(result.loc[idx])
        has_composite = _has_composite_set_evidence(text)
        if _has_main_menu_evidence(result.loc[idx]) and not has_composite:
            if result.at[idx, "수동분류_edit"] == "메인":
                continue
            result.at[idx, "수동분류_edit"] = "메인"
            result.at[idx, "classified_by"] = "main_set_guard"
            corrected += 1
        elif result.at[idx, "수동분류_edit"] != "세트":
            continue
        elif has_composite:
            continue
        else:
            result.at[idx, "수동분류_edit"] = ""
            result.at[idx, "classified_by"] = "llm_unresolved"
            unresolved += 1
        result.at[idx, "updated_at"] = TODAY
        changed_keys.add(tuple(_strip_text(result.at[idx, col]) for col in KEY_COLUMNS))

    if corrected or unresolved:
        logger.info(
            "메인/세트 정책 보정: 메인=%d행, 수동확인=%d행",
            corrected,
            unresolved,
        )
    return result, corrected, unresolved, changed_keys


def find_llm_targets(all_items: pd.DataFrame, map_df: pd.DataFrame) -> pd.DataFrame:
    target_columns = [
        "item_id", "item_key", "store_seq", "item_seq",
        "store", "source", "brand", "item_name", "unitprice", "대표메뉴",
    ]
    if map_df.empty:
        return all_items.reindex(columns=target_columns, fill_value="").copy()

    status = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    for col in (
        "item_id", "item_key", "store_seq", "item_seq",
        "store", "source", "brand", "item_name", "unitprice", "표준_메뉴명_edit", "수동분류_edit", "대표메뉴",
        REVIEW_STATUS_COLUMN, "classified_by",
    ):
        status[col] = status[col].fillna("").astype(str).str.strip()
    status[REVIEW_STATUS_COLUMN] = status[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
    status = status.drop_duplicates(subset=KEY_COLUMNS, keep="last")

    if all_items.empty:
        parquet_targets = pd.DataFrame(columns=target_columns)
    else:
        merged = all_items.merge(
            status[KEY_COLUMNS + ["표준_메뉴명_edit", "수동분류_edit", REVIEW_STATUS_COLUMN, "classified_by"]],
            on=KEY_COLUMNS,
            how="left",
        )
        classified_by = merged["classified_by"].fillna("").astype(str).str.strip()
        review_status = merged[REVIEW_STATUS_COLUMN].fillna("").astype(str).str.strip()
        manual_label = merged["수동분류_edit"].fillna("").astype(str).str.strip()
        item_name = merged["item_name"].fillna("").astype(str).str.strip()
        source = merged["source"].fillna("").astype(str).str.strip().map(canonical_source)
        invalid_item_name = item_name.str.fullmatch(r"\d+")
        manual_unknown = (
            source.isin(MANUAL_UNKNOWN_SOURCES)
            & item_name.isin(MANUAL_UNKNOWN_ITEM_NAMES)
        )
        is_pending_other = manual_label.eq("기타") & review_status.ne(REVIEW_APPROVED)
        has_valid_label = manual_label.isin(VALID_CATEGORIES)
        has_manual_value = (
            (merged["표준_메뉴명_edit"].fillna("").astype(str).str.strip() != "")
            & has_valid_label
            & ~is_pending_other
        )
        llm_done = classified_by.eq("llm") & has_valid_label
        human_done = classified_by.eq("human")
        classified_done = (llm_done | human_done) & ~is_pending_other
        parquet_targets = merged[
            ~invalid_item_name
            & ~manual_unknown
            & ~(classified_done | (review_status == REVIEW_APPROVED) | has_manual_value)
        ][target_columns]

    map_classified_by = status["classified_by"]
    map_review_status = status[REVIEW_STATUS_COLUMN]
    map_manual_label = status["수동분류_edit"]
    map_item_name = status["item_name"]
    map_item_id = status["item_id"]
    map_source = status["source"].map(canonical_source)
    map_manual_unknown = (
        map_source.isin(MANUAL_UNKNOWN_SOURCES)
        & map_item_name.isin(MANUAL_UNKNOWN_ITEM_NAMES)
    )
    map_pending_other = map_manual_label.eq("기타") & map_review_status.ne(REVIEW_APPROVED)
    map_has_valid_label = map_manual_label.isin(VALID_CATEGORIES)
    map_classified_done = (
        ((map_classified_by.eq("llm") & map_has_valid_label) | map_classified_by.eq("human"))
        & ~map_pending_other
    )
    map_only = status[
        map_review_status.ne(REVIEW_APPROVED)
        & (~map_has_valid_label | map_pending_other)
        & ~map_classified_done
        & ~map_manual_unknown
        & map_item_name.ne("")
        & ~map_item_name.str.fullmatch(r"\d+")
        & map_item_id.ne("")
        & map_item_id.ne(OKPOS_ADJUSTMENT_ITEM_ID)
    ].reindex(columns=target_columns, fill_value="")

    return (
        pd.concat([parquet_targets, map_only], ignore_index=True)
        .drop_duplicates(subset=KEY_COLUMNS, keep="first")
        .reset_index(drop=True)
    )


def build_prompt(batch: list[dict], examples: list[dict], rules: list[dict] | None = None) -> str:
    example_text = "\n".join(
        f'- "{r["item_name"]}" => 표준명 "{r["표준_메뉴명_edit"]}", '
        f'분류 "{r["수동분류_edit"]}"'
        for r in examples
    ) or "(예시 없음)"
    rule_text = rules_to_prompt_block(rules or [])
    items_text = "\n".join(
        f'{i + 1}. store={r["store"]}, source={r["source"]}, brand={r.get("brand", "")}, '
        f'unitprice={r.get("unitprice", "")}, item_name="{r["item_name"]}", '
        f'대표메뉴="{r.get("대표메뉴", "")}"'
        for i, r in enumerate(batch)
    )
    return f"""도리당 F&B 상품명 정규화 작업입니다.
아래 상품명을 표준_메뉴명_edit, 수동분류_edit로 분류하세요.

허용 수동분류: {", ".join(VALID_CATEGORIES)}

규칙:
- 아래 자동 키워드 규칙과 승인 예시를 우선 참고하되, 상품 문맥과 다르면 JSON 결과에 더 적절한 분류를 응답하세요.
- "1인 추가"는 표준_메뉴명_edit="1인 추가", 수동분류_edit="사이드"로 분류합니다.
- 서로 다른 메뉴가 명확히 묶인 복합 구성만 "세트"로 분류합니다.
- 상품명에 "세트"라는 단어만 있거나 대괄호 수식어가 있다는 이유로 "세트"로 분류하지 마세요.
- 탕, 도리탕, 우도리탕, 곱도리탕, 닭한마리, 삼계탕, 정식 등 단일 본식은 기본적으로 "메인"입니다.

자동 키워드 규칙:
{rule_text}

기존 승인 예시:
{example_text}

분류 대상:
{items_text}

반드시 {{"items": [...]}} 형태의 JSON 객체만 응답하세요.
items의 각 원소는 item_name, 표준_메뉴명_edit, 수동분류_edit 키를 가져야 합니다.
수동분류_edit는 허용값 중 하나만 사용하세요.
"""


def _load_map_examples() -> list[dict]:
    if FIN_PRODUCT_MAP_TRAIN_JSON_PATH.exists():
        try:
            data = json.loads(FIN_PRODUCT_MAP_TRAIN_JSON_PATH.read_text(encoding="utf-8"))
            examples = []
            for label_data in data.get("label_rules", {}).values():
                examples.extend(label_data.get("examples", []))
            examples = [
                example
                for example in examples
                if _strip_text(example.get("수동분류_edit")) != "세트"
                or _has_composite_set_evidence(example)
            ]
            if examples:
                logger.info("train JSON에서 예시 %d건 로드", len(examples))
                return examples
        except Exception as e:
            logger.warning("train JSON 로드 실패, map_df fallback: %s", e)
    return []


def call_llm(prompt: str) -> list[dict]:
    client, model_list = get_ollama_client_with_candidates()
    parsed = query_qwen_json(
        prompt,
        system_prompt="반드시 유효한 JSON 객체만 응답하세요.",
        client=client,
        model_candidates=model_list,
    )
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for key in ("items", "results", "data"):
            value = parsed.get(key)
            if isinstance(value, list):
                return value
        if (
            {"item_name", "표준_메뉴명_edit", "수동분류_edit"}.issubset(parsed)
            or {"item_name", "표준_메뉴명", "수동분류"}.issubset(parsed)
        ):
            return [parsed]
    logger.warning("LLM JSON 결과에 분류 목록 없음: %s", str(parsed)[:500])
    return []


def build_chicken_usage_prompt(batch: list[dict]) -> str:
    usage_table = ", ".join(
        f"{chicken_type}/{size}={usage:g}"
        for (chicken_type, size), usage in sorted(CHICKEN_USAGE_TABLE.items())
    )
    items_text = "\n".join(
        f'{i + 1}. store={_strip_text(r.get("store"))}, source={_strip_text(r.get("source"))}, '
        f'brand={_strip_text(r.get("brand"))}, item_id={_strip_text(r.get("item_id"))}, '
        f'unitprice={_strip_text(r.get("unitprice"))}, item_name="{_strip_text(r.get("item_name"))}", '
        f'대표메뉴="{_strip_text(r.get("대표메뉴"))}", 표준명="{_strip_text(r.get("표준_메뉴명_edit"))}", '
        f'수동분류="{_strip_text(r.get("수동분류_edit"))}", 주문옵션맥락="{_strip_text(r.get("order_context"))}"'
        for i, r in enumerate(batch)
    )
    return f"""도리당 닭 사용량 검수 후보 생성 작업입니다.
아래 상품/옵션이 닭 사용량을 결정하는 행인지 판단하고 후보값만 제안하세요.

허용 닭유형: {", ".join(VALID_CHICKEN_TYPES)}
허용 사이즈: {", ".join(VALID_CHICKEN_SIZES)}
사용량 기준표: {usage_table}

규칙:
- 닭도리탕, 곱도리탕, 우도리탕, 닭한마리, 삼계탕의 본식 또는 사이즈/순살/뼈닭 옵션만 후보를 채웁니다.
- 리뷰서비스, 음료, 주류, 공기밥, 토핑, 배달비, 할인, 소스, 맵기 옵션은 빈 후보로 둡니다.
- 사이즈가 옵션 행에만 있으면 해당 옵션 행에 사이즈와 사용량 후보를 제안합니다.
- 확실하지 않으면 닭유형_candidate, 사이즈_candidate, 닭사용량_candidate를 빈 문자열로 둡니다.
- 닭사용량_candidate는 사용량 기준표의 숫자만 사용합니다.
- confidence는 0부터 1 사이 숫자 문자열로 답하세요.

분류 대상:
{items_text}

반드시 {{"items": [...]}} 형태의 JSON 객체만 응답하세요.
items의 각 원소는 item_name, 닭유형_candidate, 사이즈_candidate, 닭사용량_candidate, 닭분류_근거, 닭분류_confidence 키를 가져야 합니다.
"""


def call_chicken_usage_llm(prompt: str) -> list[dict]:
    client, model_list = get_ollama_client_with_candidates()
    parsed = query_qwen_json(
        prompt,
        system_prompt="반드시 유효한 JSON 객체만 응답하세요.",
        client=client,
        model_candidates=model_list,
    )
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for key in ("items", "results", "data"):
            value = parsed.get(key)
            if isinstance(value, list):
                return value
        if {"item_name", "닭유형_candidate", "사이즈_candidate"}.issubset(parsed):
            return [parsed]
    logger.warning("LLM 닭 사용량 JSON 결과에 후보 목록 없음: %s", str(parsed)[:500])
    return []


def _blank_chicken_usage_candidate(item: dict) -> dict[str, str]:
    base = {
        col: _strip_text(item.get(col))
        for col in ["store", "source", "brand", "item_id", "item_name"]
    }
    base.update({
        "닭유형_candidate": "",
        "사이즈_candidate": "",
        "닭사용량_candidate": "",
        "닭분류_근거": "",
        "닭분류_confidence": "",
    })
    return base


def _normalize_chicken_usage_candidate(item: dict, candidate: dict) -> dict[str, str]:
    row = _blank_chicken_usage_candidate(item)
    chicken_type = _strip_text(candidate.get("닭유형_candidate") or candidate.get("닭유형"))
    size = _strip_text(candidate.get("사이즈_candidate") or candidate.get("사이즈"))
    if chicken_type not in VALID_CHICKEN_TYPES or size not in VALID_CHICKEN_SIZES:
        return row

    usage_text = _strip_text(candidate.get("닭사용량_candidate") or candidate.get("닭사용량"))
    expected_usage = CHICKEN_USAGE_TABLE.get((chicken_type, size))
    if expected_usage is None:
        return row
    if usage_text:
        try:
            usage = float(usage_text)
        except ValueError:
            usage = expected_usage
    else:
        usage = expected_usage

    confidence = _strip_text(candidate.get("닭분류_confidence") or candidate.get("confidence"))
    if confidence:
        try:
            value = float(confidence)
            confidence = str(max(0.0, min(1.0, value)))
        except ValueError:
            confidence = ""

    row.update({
        "닭유형_candidate": chicken_type,
        "사이즈_candidate": size,
        "닭사용량_candidate": f"{usage:g}",
        "닭분류_근거": _strip_text(candidate.get("닭분류_근거") or candidate.get("reason")),
        "닭분류_confidence": confidence,
    })
    return row


def suggest_chicken_usage_candidates(items: list[dict]) -> list[dict[str, str]]:
    batch = [dict(item) for item in items if _strip_text(item.get("item_name"))]
    if not batch:
        return []

    results = call_chicken_usage_llm(build_chicken_usage_prompt(batch))
    by_key: dict[str, dict] = {}
    for result in results:
        if not isinstance(result, dict):
            continue
        for key_name in ("item_id", "item_name"):
            raw = _strip_text(result.get(key_name))
            if raw:
                by_key.setdefault(raw, result)
                by_key.setdefault(normalize_item_key(raw), result)

    positional_ok = len(results) == len(batch)
    rows = []
    for pos, item in enumerate(batch):
        item_id = _strip_text(item.get("item_id"))
        item_name = _strip_text(item.get("item_name"))
        candidate = (
            by_key.get(item_id)
            or by_key.get(item_name)
            or by_key.get(normalize_item_key(item_name))
            or (results[pos] if positional_ok and isinstance(results[pos], dict) else {})
        )
        rows.append(_normalize_chicken_usage_candidate(item, candidate))
    return rows


def build_examples(map_df: pd.DataFrame) -> list[dict]:
    if map_df.empty:
        return []
    approved = map_df[
        (map_df[REVIEW_STATUS_COLUMN].fillna("").astype(str).str.strip().apply(_normalize_review_status) == REVIEW_APPROVED)
        & (map_df["표준_메뉴명_edit"].fillna("").astype(str).str.strip() != "")
        & (map_df["수동분류_edit"].fillna("").astype(str).str.strip().isin(VALID_CATEGORIES))
    ]
    if approved.empty:
        return []
    rows = []
    for label in VALID_CATEGORIES:
        label_df = approved[approved["수동분류_edit"].fillna("").astype(str).str.strip() == label]
        if label == "세트" and not label_df.empty:
            label_df = label_df[
                label_df.apply(_has_composite_set_evidence, axis=1)
            ]
        rows.extend(
            label_df[["item_name", "표준_메뉴명_edit", "수동분류_edit"]]
            .drop_duplicates()
            .head(_TRAIN_EXAMPLES_PER_LABEL * 2)
            .to_dict("records")
        )
    return rows


def _build_map_train_payload(map_df: pd.DataFrame, rules: list[dict] | None = None) -> dict:
    approved = map_df[
        (map_df[REVIEW_STATUS_COLUMN].fillna("").astype(str).str.strip().apply(_normalize_review_status) == REVIEW_APPROVED)
        & (map_df["수동분류_edit"].fillna("").astype(str).str.strip().isin(VALID_CATEGORIES))
        & (map_df["item_name"].fillna("").astype(str).str.strip() != "")
    ].copy()

    counts = approved["수동분류_edit"].value_counts().to_dict() if not approved.empty else {}
    label_rules: dict[str, dict] = {}
    for label in VALID_CATEGORIES:
        label_df = approved[approved["수동분류_edit"] == label].copy()
        if label == "세트" and not label_df.empty:
            label_df = label_df[
                label_df.apply(_has_composite_set_evidence, axis=1)
            ]
        label_rule_rows = [rule for rule in (rules or []) if rule.get("수동분류") == label]
        base_rule = {
            "include_keywords": [
                keyword
                for rule in label_rule_rows
                for keyword in (rule.get("include_keywords") or [])
            ][:50],
            "ignore_words": sorted({
                word
                for rule in label_rule_rows
                for word in (rule.get("ignore_words") or [])
                if str(word).strip()
            }),
            "represent_menu": next(
                (str(rule.get("represent_menu", "")).strip() for rule in label_rule_rows if str(rule.get("represent_menu", "")).strip()),
                "",
            ),
        }
        if label_df.empty:
            label_rules[label] = {**base_rule, "examples": []}
            continue
        label_df = (
            label_df
            .drop_duplicates(subset=["item_name"], keep="last")
            .head(_TRAIN_EXAMPLES_PER_LABEL)
        )
        label_rules[label] = {
            **base_rule,
            "examples": [
                {
                    "item_name": str(row["item_name"]).strip(),
                    "표준_메뉴명_edit": str(row.get("표준_메뉴명_edit", "")).strip(),
                    "수동분류_edit": str(row.get("수동분류_edit", "")).strip(),
                }
                for _, row in label_df.iterrows()
            ]
        }

    return {
        "version": "1.0",
        "source": str(FIN_PRODUCT_MAP_CSV_PATH),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "allowed_categories": VALID_CATEGORIES,
        "label_counts": {label: int(counts.get(label, 0)) for label in VALID_CATEGORIES},
        "label_rules": label_rules,
    }


def _confirmed_master_rows_for_rules() -> pd.DataFrame:
    source_path = existing_fin_product_csv_path()
    if not source_path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(source_path, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as exc:
        logger.warning("fin_product 확정행 규칙 소스 로드 실패: %s", exc)
        return pd.DataFrame()
    for col in ("상품명", "수동분류", "exclude_check", "대메뉴", "중메뉴", "llm_check", "approve", "classified_by", "검수사유"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()
    confirmed = df[
        (df["상품명"] != "")
        & df["수동분류"].isin(VALID_CATEGORIES)
        & (df["llm_check"].str.upper() != "Y")
        & (~df["classified_by"].str.lower().isin({"rule", "llm", "rule+llm"}))
        & (~df["classified_by"].str.startswith("rule/llm_conflict"))
        & (~df["검수사유"].str.contains("충돌|미입력|확인|불일치", regex=True, na=False))
    ].copy()
    if confirmed.empty:
        return confirmed
    if "대표메뉴" not in confirmed.columns:
        confirmed["대표메뉴"] = confirmed["상품명"]
    return confirmed


def _rule_source_rows(map_df: pd.DataFrame) -> pd.DataFrame:
    classified_by = map_df["classified_by"].fillna("").astype(str).str.strip() if "classified_by" in map_df.columns else pd.Series([""] * len(map_df), index=map_df.index)
    approved = map_df[
        (map_df[REVIEW_STATUS_COLUMN].fillna("").astype(str).str.strip().apply(_normalize_review_status) == REVIEW_APPROVED)
        & (map_df["수동분류_edit"].fillna("").astype(str).str.strip().isin(VALID_CATEGORIES))
        & (map_df["item_name"].fillna("").astype(str).str.strip() != "")
        & (~classified_by.str.lower().isin({"rule", "llm", "rule+llm"}))
        & (~classified_by.str.startswith("rule/llm_conflict"))
    ].copy()
    if not approved.empty:
        approved_rules = pd.DataFrame({
            "상품명": approved["item_name"].fillna("").astype(str).str.strip(),
            "수동분류": approved["수동분류_edit"].fillna("").astype(str).str.strip(),
            "대표메뉴": approved["표준_메뉴명_edit"].fillna("").astype(str).str.strip(),
            "exclude_check": "",
            "대메뉴": "",
            "중메뉴": "",
        })
    else:
        approved_rules = pd.DataFrame()
    master_confirmed = _confirmed_master_rows_for_rules()
    return pd.concat([approved_rules, master_confirmed], ignore_index=True, sort=False)


def build_fin_product_map_train_json(dry_run: bool = False, **context) -> dict:
    map_df = apply_recently_edits(load_map(), load_recently_map())
    rules = build_rules_from_manual(_rule_source_rows(map_df))
    rule_summary = summarize_rules(rules)
    change_report = evaluate_rule_change(rules)
    payload = _build_map_train_payload(map_df, rules=rules)
    counts = payload.get("label_counts", {})
    count_msg = ", ".join(f"{label}={counts.get(label, 0)}" for label in VALID_CATEGORIES)
    rule_update_status = "dry_run" if dry_run else "saved"
    proposal_path = ""

    if dry_run:
        logger.info(
            "dry-run: train/rules JSON 저장 생략 | %s | %s | change=%s",
            count_msg,
            rule_summary,
            change_report,
        )
    else:
        FIN_PRODUCT_MAP_TRAIN_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = FIN_PRODUCT_MAP_TRAIN_JSON_PATH.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            _safe_replace(tmp, FIN_PRODUCT_MAP_TRAIN_JSON_PATH)
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
        if change_report["review_required"]:
            ti = context.get("task_instance") or context.get("ti")
            run_id = context.get("run_id") or getattr(ti, "run_id", None) or "manual"
            proposal, created, change_report = save_rule_proposal(rules, run_id=str(run_id))
            proposal_path = str(proposal)
            rule_update_status = "review_required"
            logger.warning(
                "활성 규칙 급증으로 기존 규칙 유지 및 제안 격리: old=%d new=%d ratio=%.1f%% path=%s",
                change_report["old_active_count"],
                change_report["new_active_count"],
                change_report["active_change_ratio"] * 100,
                proposal,
            )
            if created:
                send_telegram(
                    "[상품 매핑] 규칙 변경 검토 필요\n"
                    f"활성 규칙: {change_report['old_active_count']} → {change_report['new_active_count']}\n"
                    f"변화율: {change_report['active_change_ratio'] * 100:.1f}%\n"
                    "기존 활성 규칙은 유지했습니다.\n"
                    f"검토 파일: {proposal}"
                )
        else:
            save_rules(rules)
            logger.info(
                "fin_product_map_train.json 및 rules 저장: %s | %s | %s",
                FIN_PRODUCT_MAP_TRAIN_JSON_PATH,
                rule_summary,
                count_msg,
            )

    return {
        "label_counts": counts,
        **rule_summary,
        "dry_run": dry_run,
        "rule_update_status": rule_update_status,
        "rule_change_ratio": change_report["active_change_ratio"],
        "rule_proposal_path": proposal_path,
    }


def _classify_batch(
    batch: list[dict],
    examples: list[dict],
    rules: list[dict],
    allow_retry: bool = True,
) -> list[dict]:
    results = call_llm(build_prompt(batch, examples, rules=rules))
    by_name: dict[str, dict] = {}
    for result in results:
        if not isinstance(result, dict):
            continue
        raw = str(result.get("item_name", "")).strip()
        if raw:
            by_name.setdefault(raw, result)
        normalized_key = normalize_item_key(raw)
        if normalized_key:
            by_name.setdefault(normalized_key, result)
    rows = []
    unresolved_positions = []
    unmatched_count = 0
    positional_ok = len(results) == len(batch)
    for pos, item in enumerate(batch):
        raw_item = str(item.get("item_name", "")).strip()
        classified = by_name.get(raw_item) or by_name.get(normalize_item_key(raw_item))
        if not classified and positional_ok and isinstance(results[pos], dict):
            classified = results[pos]
        if not classified:
            classified = {}
            unmatched_count += 1
        llm_normalized = _normalize_classification(item, classified)
        rule_result = classify_by_rules(item, rules)
        resolved = reconcile(rule_result, llm_normalized)
        normalized = _normalize_classification(item, resolved)
        row = {
            "item_id": item.get("item_id", ""),
            "item_key": item.get("item_key", ""),
            "store_seq": item.get("store_seq", ""),
            "item_seq": item.get("item_seq", ""),
            "store": item["store"],
            "source": item["source"],
            "brand": item.get("brand", ""),
            "item_name": item["item_name"],
            "unitprice": item.get("unitprice", ""),
            "대표메뉴": item.get("대표메뉴", ""),
            **normalized,
            REVIEW_STATUS_COLUMN: REVIEW_PENDING,
            "classified_by": resolved.get("classified_by", "llm"),
            "updated_at": TODAY,
        }
        if normalized["수동분류_edit"] not in VALID_CATEGORIES and not rule_result:
            row["classified_by"] = "llm_unresolved"
            unresolved_positions.append((pos, item))
        rows.append(row)
    if unmatched_count:
        logger.warning("LLM 응답 item_name 미매칭: %d/%d건", unmatched_count, len(batch))
    if allow_retry and len(batch) > 1 and unresolved_positions:
        logger.warning("LLM 무효 응답 단건 재시도: %d건", len(unresolved_positions))
        for pos, item in unresolved_positions:
            try:
                retry_rows = _classify_batch([item], examples, rules, allow_retry=False)
                if retry_rows:
                    rows[pos] = retry_rows[0]
            except Exception as retry_error:
                logger.warning("LLM 무효 응답 단건 재시도 실패: %s | %s", item.get("item_name", ""), retry_error)
    final_unresolved = sum(row.get("classified_by") == "llm_unresolved" for row in rows)
    if allow_retry and final_unresolved:
        logger.warning("LLM 분류 최종 실패: %d/%d건", final_unresolved, len(batch))
    return rows


def classify_unmapped(unmapped: pd.DataFrame, map_df: pd.DataFrame, limit: int | None, dry_run: bool) -> list[dict]:
    if limit is not None:
        unmapped = unmapped.head(limit)
    if unmapped.empty:
        return []

    examples = _load_map_examples() or build_examples(map_df)
    rules = load_rules()
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
            new_rows.extend(_classify_batch(batch, examples, rules))
        except Exception as e:
            logger.warning("배치 %d 실패: %s", idx, e)
            if len(batch) <= 1:
                continue
            for item in batch:
                try:
                    new_rows.extend(_classify_batch([item], examples, rules))
                except Exception as item_error:
                    logger.warning("단건 분류 실패: %s | %s", item["item_name"], item_error)
    return new_rows


def migrate_product_map(dry_run: bool = False, **context) -> dict:
    result = build_initial_map(persist_identity=not dry_run)
    existing = load_map()
    review_edits = load_review_map()
    if not result.empty and not existing.empty:
        current_value_cols = [col for col in [
            "item_id", "store_seq", "item_seq", "item_name", "unitprice", "대표메뉴",
        ] if col not in KEY_COLUMNS]
        current_values = result[KEY_COLUMNS + current_value_cols].drop_duplicates(subset=KEY_COLUMNS, keep="last")
        existing = existing.merge(current_values, on=KEY_COLUMNS, how="left", suffixes=("", "_current"))
        for col in current_value_cols:
            current_col = f"{col}_current"
            if current_col in existing.columns:
                current = existing[current_col]
                if col in {"brand", "unitprice"}:
                    existing[col] = existing[col].where(existing[col].astype(str).str.strip() != "", current)
                else:
                    has_current = current.notna() & current.astype(str).str.strip().ne("")
                    existing[col] = current.where(has_current, existing[col])
                existing = existing.drop(columns=[current_col])
        result = (
            pd.concat([result, existing], ignore_index=True)
            .reindex(columns=MAP_COLUMNS, fill_value="")
            .drop_duplicates(subset=KEY_COLUMNS, keep="last")
            .sort_values(["store", "source", "item_name"])
            .reset_index(drop=True)
        )
    result = _seed_split_rows_from_siblings(result)
    result, review_edits, auto_approval_reset, auto_reset_keys = _reset_automatic_approvals(
        result,
        review_edits,
    )
    result, main_set_corrected, main_set_unresolved, policy_keys = _apply_main_set_policy(result)
    forced_review_keys = auto_reset_keys | policy_keys
    review_edits = _align_review_rows_for_forced_review(
        review_edits,
        result,
        forced_review_keys,
    )
    result = apply_review_edits(result, review_edits)
    recently_edits = _suppress_recently_approvals(
        load_recently_map(),
        forced_review_keys,
    )
    result = apply_recently_edits(result, recently_edits)
    result = _apply_classification_overrides(result)
    result, post_corrected, post_unresolved, _ = _apply_main_set_policy(result)
    main_set_corrected += post_corrected
    main_set_unresolved += post_unresolved
    result[REVIEW_STATUS_COLUMN] = result[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
    review_rows = build_review_rows(result)
    join_rows, join_conflicts = build_join_map(result)
    duplicate_count = int(result.duplicated(subset=KEY_COLUMNS).sum())
    join_conflict_count = int(join_conflicts.drop_duplicates(subset=["item_id", "store", "source", "brand"]).shape[0])
    duplicate_label_count = int(
        review_rows[review_rows[DUP_LABEL_COLUMN].fillna("").astype(str).str.strip() == "Y"]
        .drop_duplicates(subset=["source", "item_id"])
        .shape[0]
    )
    summary = {
        "target_stores": TARGET_STORES,
        "target_rows": int(len(result)),
        "approved": int((result[REVIEW_STATUS_COLUMN] == REVIEW_APPROVED).sum()) if not result.empty else 0,
        "pending": int((result[REVIEW_STATUS_COLUMN] == REVIEW_PENDING).sum()) if not result.empty else 0,
        "review_rows": int(len(review_rows)),
        "join_rows": int(len(join_rows)),
        "join_excluded_pending": _count_join_excluded_pending(result),
        "join_conflict_keys": join_conflict_count,
        "duplicate_keys": duplicate_count,
        "duplicate_label_keys": duplicate_label_count,
        "auto_approval_reset": auto_approval_reset,
        "main_set_corrected": main_set_corrected,
        "main_set_unresolved": main_set_unresolved,
        "chicken_usage_warnings": 0,
        "dry_run": dry_run,
        "output_path": str(FIN_PRODUCT_MAP_CSV_PATH),
        "review_output_path": str(FIN_PRODUCT_MAP_REVIEW_CSV_PATH),
        "recently_output_path": str(FIN_PRODUCT_MAP_RECENTLY_CSV_PATH),
        "join_output_path": str(FIN_PRODUCT_MAP_JOIN_CSV_PATH),
    }

    if result.empty:
        logger.warning("대상 매장 데이터 없음: %s", TARGET_STORES)
    elif dry_run:
        logger.info("dry-run: CSV 저장 생략 (%d행, review %d행)", len(result), len(review_rows))
    else:
        write_map(result)
        summary["chicken_usage_warnings"] = write_review_map(review_rows)
        write_recently_map(result)
        summary.update(write_join_map(result))
        _notify_duplicate_labels(review_rows)
        logger.info("fin_product_map.csv 저장: %s (%d행)", FIN_PRODUCT_MAP_CSV_PATH, len(result))
        logger.info("fin_product_map_review_input.csv 저장: %s (%d행)", FIN_PRODUCT_MAP_REVIEW_CSV_PATH, len(review_rows))
        logger.info("fin_product_map_recently.csv 저장: %s", FIN_PRODUCT_MAP_RECENTLY_CSV_PATH)
        logger.info("fin_product_map_join.csv 저장: %s (%d행)", FIN_PRODUCT_MAP_JOIN_CSV_PATH, summary["join_rows"])
    return summary


def llm_product_map(dry_run: bool = False, limit: int | None = None, **context) -> dict:
    all_items = scan_target_items(persist_identity=not dry_run)
    map_df, review_edits, auto_approval_reset, auto_reset_keys = _reset_automatic_approvals(
        load_map(),
        load_review_map(),
    )
    map_df, main_set_corrected, main_set_unresolved, policy_keys = _apply_main_set_policy(map_df)
    forced_review_keys = auto_reset_keys | policy_keys
    review_edits = _align_review_rows_for_forced_review(
        review_edits,
        map_df,
        forced_review_keys,
    )
    map_df = apply_review_edits(map_df, review_edits)
    recently_edits = _suppress_recently_approvals(
        load_recently_map(),
        forced_review_keys,
    )
    map_df = apply_recently_edits(map_df, recently_edits)
    # 메뉴 상세 결손 대체 상품은 규칙으로 채우되 사람 승인 전에는 join에 반영하지 않는다.
    map_df = _apply_classification_overrides(map_df)
    map_df, post_corrected, post_unresolved, _ = _apply_main_set_policy(map_df)
    main_set_corrected += post_corrected
    main_set_unresolved += post_unresolved
    llm_targets = find_llm_targets(all_items, map_df)
    all_item_keys = {
        tuple(_strip_text(row.get(col)) for col in KEY_COLUMNS)
        for row in all_items.reindex(columns=KEY_COLUMNS, fill_value="").to_dict("records")
    }
    map_only_target_count = sum(
        tuple(_strip_text(row.get(col)) for col in KEY_COLUMNS) not in all_item_keys
        for row in llm_targets.to_dict("records")
    )
    target_rows = int(len(all_items) + map_only_target_count)
    summary = {
        "target_stores": TARGET_STORES,
        "target_rows": target_rows,
        "already_llm_classified": max(0, target_rows - int(len(llm_targets))),
        "llm_targets": int(len(llm_targets)),
        "new_classified": 0,
        "new_pending": 0,
        "llm_unresolved": int((map_df["classified_by"] == "llm_unresolved").sum()) if not map_df.empty else 0,
        "auto_approval_reset": auto_approval_reset,
        "main_set_corrected": main_set_corrected,
        "main_set_unresolved": main_set_unresolved,
        "chicken_usage_warnings": 0,
        "dry_run": dry_run,
        "limit": limit,
        "output_path": str(FIN_PRODUCT_MAP_CSV_PATH),
        "review_output_path": str(FIN_PRODUCT_MAP_REVIEW_CSV_PATH),
        "recently_output_path": str(FIN_PRODUCT_MAP_RECENTLY_CSV_PATH),
    }

    if all_items.empty and map_df.empty:
        logger.warning("대상 매장 데이터 없음: %s", TARGET_STORES)
        return summary

    new_rows = classify_unmapped(llm_targets, map_df, limit=limit, dry_run=dry_run)
    summary["new_classified"] = len(new_rows)
    summary["new_item_samples"] = [
        f"{row.get('store', '')}/{row.get('source', '')}/{row.get('item_name', '')}"
        for row in new_rows[:10]
    ]

    new_keys = {tuple(_strip_text(row.get(col)) for col in KEY_COLUMNS) for row in new_rows}
    if new_rows:
        new_df = pd.DataFrame(new_rows, columns=MAP_COLUMNS)
        map_df = pd.concat([map_df, new_df], ignore_index=True)
        map_df = (
            map_df.reindex(columns=MAP_COLUMNS, fill_value="")
            .drop_duplicates(subset=KEY_COLUMNS, keep="last")
            .reset_index(drop=True)
        )
    map_df = _apply_classification_overrides(map_df)
    map_df, new_main_set_corrected, new_main_set_unresolved, _ = _apply_main_set_policy(map_df)
    summary["main_set_corrected"] += new_main_set_corrected
    summary["main_set_unresolved"] += new_main_set_unresolved
    map_df[REVIEW_STATUS_COLUMN] = map_df[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
    if new_keys:
        key_frame = map_df[KEY_COLUMNS].fillna("").astype(str).apply(lambda col: col.str.strip())
        is_new_row = key_frame.apply(lambda row: tuple(row[col] for col in KEY_COLUMNS) in new_keys, axis=1)
        new_pending_rows = map_df[is_new_row & (map_df[REVIEW_STATUS_COLUMN] == REVIEW_PENDING)]
        summary["new_pending"] = int(len(new_pending_rows))
        summary["new_item_samples"] = [
            f"{row.get('store', '')}/{row.get('source', '')}/{row.get('item_name', '')}"
            for row in new_pending_rows.head(10).to_dict("records")
        ]
    review_rows = build_review_rows(map_df)
    join_rows, join_conflicts = build_join_map(map_df)
    summary["review_rows"] = int(len(review_rows))
    summary["join_rows"] = int(len(join_rows))
    summary["join_excluded_pending"] = _count_join_excluded_pending(map_df)
    summary["join_conflict_keys"] = int(join_conflicts.drop_duplicates(subset=["item_id", "store", "source", "brand"]).shape[0])
    summary["duplicate_label_keys"] = int(
        review_rows[review_rows[DUP_LABEL_COLUMN].fillna("").astype(str).str.strip() == "Y"]
        .drop_duplicates(subset=["source", "item_id"])
        .shape[0]
    )
    summary["join_output_path"] = str(FIN_PRODUCT_MAP_JOIN_CSV_PATH)
    summary["approved"] = int((map_df[REVIEW_STATUS_COLUMN] == REVIEW_APPROVED).sum()) if not map_df.empty else 0
    summary["pending"] = int((map_df[REVIEW_STATUS_COLUMN] == REVIEW_PENDING).sum()) if not map_df.empty else 0
    summary["llm_unresolved"] = int((map_df["classified_by"] == "llm_unresolved").sum()) if not map_df.empty else 0

    if dry_run:
        logger.info("dry-run: LLM 호출 및 CSV 저장 생략")
    elif not map_df.empty:
        write_map(map_df)
        summary["chicken_usage_warnings"] = write_review_map(review_rows)
        write_recently_map(map_df)
        summary.update(write_join_map(map_df))
        _notify_duplicate_labels(review_rows)
        logger.info("fin_product_map.csv 업데이트: %s (%d행)", FIN_PRODUCT_MAP_CSV_PATH, len(map_df))
        logger.info("fin_product_map_review_input.csv 저장: %s (%d행)", FIN_PRODUCT_MAP_REVIEW_CSV_PATH, len(review_rows))
        logger.info("fin_product_map_recently.csv 저장: %s", FIN_PRODUCT_MAP_RECENTLY_CSV_PATH)
        logger.info("fin_product_map_join.csv 저장: %s (%d행)", FIN_PRODUCT_MAP_JOIN_CSV_PATH, summary["join_rows"])
    else:
        logger.info("신규 분류 대상 없음")

    return summary
