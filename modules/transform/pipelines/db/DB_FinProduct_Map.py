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
from modules.transform.pipelines.db.DB_FinProduct_Rules import (
    build_rules_from_manual,
    classify_by_rules,
    load_rules,
    reconcile,
    rules_to_prompt_block,
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
MODEL_CANDIDATES = ["qwen2.5:14b", "qwen2.5:7b", "qwen2.5:latest", "gpt-oss:20b", "gpt-oss:latest", "gpt-oss"]
OLLAMA_HOSTS = [
    os.getenv("OLLAMA_HOST", "").strip(),
    "http://host.docker.internal:11434",
    "http://localhost:11434",
]
BATCH_SIZE = 5
TODAY = str(date.today())
_TRAIN_EXAMPLES_PER_LABEL = 10
AUTO_APPROVE_ZERO_PRICE: bool = True

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
        scoped = df[
            df["store"].isin(TARGET_STORE_SET)
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


def load_review_map() -> pd.DataFrame:
    review_path = existing_fin_product_map_review_csv_path()
    if not review_path.exists():
        return _empty_review_map()
    df = pd.read_csv(review_path, dtype=str, encoding="utf-8-sig").fillna("")
    df = _apply_review_status_aliases(df)
    df = _apply_edit_column_aliases(df)
    df = df.reindex(columns=REVIEW_INTERNAL_COLUMNS, fill_value="")
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


def write_review_map(review_df: pd.DataFrame) -> None:
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_MAP_REVIEW_CSV_PATH.with_suffix(".tmp")
    try:
        output_df = review_df.reindex(columns=REVIEW_INTERNAL_COLUMNS, fill_value="").copy()
        if "item_key" in output_df.columns and "item_name" in output_df.columns:
            output_df["item_key"] = output_df["item_key"].fillna("").astype(str).str.strip().where(
                output_df["item_key"].fillna("").astype(str).str.strip() != "",
                output_df["item_name"].fillna("").astype(str).map(normalize_item_key),
            )
        output_df = _mark_duplicate_labels(output_df)
        output_df[REVIEW_STATUS_DISPLAY_COLUMN] = output_df[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
        output_df.reindex(columns=REVIEW_COLUMNS, fill_value="").to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_MAP_REVIEW_CSV_PATH)
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
    for col in ("item_id", "store", "source", "brand", "item_name", "표준_메뉴명_edit", "수동분류_edit"):
        work[col] = work[col].fillna("").astype(str).str.strip()
    work["source"] = work["source"].map(canonical_source)
    work = work.rename(columns={
        "표준_메뉴명_edit": "standard_menu_name",
        "수동분류_edit": "category",
    })
    work = work[
        (work["item_id"] != "")
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
        return "자동승인(0원 라인)"
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
    if not reasons and classified_by == "llm":
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
        current_classified_by = _strip_text(result.loc[idx, "classified_by"].iloc[-1])
        if std_changed or label_changed:
            result.loc[idx, "classified_by"] = "human"
        elif is_approved and current_classified_by == "":
            result.loc[idx, "classified_by"] = "human"
        result.loc[idx, "updated_at"] = TODAY
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
        result.loc[idx, REVIEW_STATUS_COLUMN] = REVIEW_APPROVED
        result.loc[idx, "classified_by"] = "human_sibling"
        result.loc[idx, "updated_at"] = TODAY
        seeded += len(idx)

    if seeded:
        logger.info("동일 상품명 코드분리 행 자동복사: %d행", seeded)
    return result


def _auto_approve_zero_price(map_df: pd.DataFrame) -> pd.DataFrame:
    if not AUTO_APPROVE_ZERO_PRICE or map_df.empty:
        return map_df

    result = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    for col in ("unitprice", "수동분류_edit", "표준_메뉴명_edit", "item_name", REVIEW_STATUS_COLUMN):
        result[col] = result[col].fillna("").astype(str).str.strip()
    result[REVIEW_STATUS_COLUMN] = result[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)

    unitprice = pd.to_numeric(result["unitprice"], errors="coerce").fillna(0)
    mask = (
        unitprice.eq(0)
        & result["수동분류_edit"].isin(VALID_CATEGORIES)
        & result[REVIEW_STATUS_COLUMN].ne(REVIEW_APPROVED)
    )
    if not bool(mask.any()):
        return result

    empty_std = mask & result["표준_메뉴명_edit"].eq("")
    result.loc[empty_std, "표준_메뉴명_edit"] = result.loc[empty_std, "item_name"]
    result.loc[mask, REVIEW_STATUS_COLUMN] = REVIEW_APPROVED
    result.loc[mask, "classified_by"] = "auto_zero_price"
    result.loc[mask, "updated_at"] = TODAY
    logger.info("0원 분류 완료 행 자동승인: %d행", int(mask.sum()))
    return result


def find_llm_targets(all_items: pd.DataFrame, map_df: pd.DataFrame) -> pd.DataFrame:
    if all_items.empty:
        return all_items.copy()
    if map_df.empty:
        return all_items.copy()

    status = map_df.reindex(columns=MAP_COLUMNS, fill_value="").copy()
    for col in (
        "item_id", "item_key", "store_seq", "item_seq",
        "store", "source", "brand", "item_name", "unitprice", "표준_메뉴명_edit", "수동분류_edit", "대표메뉴",
        REVIEW_STATUS_COLUMN, "classified_by",
    ):
        status[col] = status[col].fillna("").astype(str).str.strip()
    status[REVIEW_STATUS_COLUMN] = status[REVIEW_STATUS_COLUMN].apply(_normalize_review_status)
    status = status.drop_duplicates(subset=KEY_COLUMNS, keep="last")

    merged = all_items.merge(
        status[KEY_COLUMNS + ["표준_메뉴명_edit", "수동분류_edit", REVIEW_STATUS_COLUMN, "classified_by"]],
        on=KEY_COLUMNS,
        how="left",
    )
    classified_by = merged["classified_by"].fillna("").astype(str).str.strip()
    review_status = merged[REVIEW_STATUS_COLUMN].fillna("").astype(str).str.strip()
    manual_label = merged["수동분류_edit"].fillna("").astype(str).str.strip()
    item_name = merged["item_name"].fillna("").astype(str).str.strip()
    invalid_item_name = item_name.str.fullmatch(r"\d+")
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
    return merged[
        ~invalid_item_name & ~(classified_done | (review_status == REVIEW_APPROVED) | has_manual_value)
    ][[
        "item_id", "item_key", "store_seq", "item_seq",
        "store", "source", "brand", "item_name", "unitprice", "대표메뉴",
    ]].reset_index(drop=True)


def build_prompt(batch: list[dict], examples: list[dict], rules: list[dict] | None = None) -> str:
    example_text = "\n".join(
        f'- "{r["item_name"]}" => 표준명 "{r["표준_메뉴명_edit"]}", '
        f'분류 "{r["수동분류_edit"]}"'
        for r in examples
    ) or "(예시 없음)"
    rule_text = rules_to_prompt_block(rules or [])
    items_text = "\n".join(
        f'{i + 1}. store={r["store"]}, source={r["source"]}, brand={r.get("brand", "")}, '
        f'unitprice={r.get("unitprice", "")}, item_name="{r["item_name"]}"'
        for i, r in enumerate(batch)
    )
    return f"""도리당 F&B 상품명 정규화 작업입니다.
아래 상품명을 표준_메뉴명_edit, 수동분류_edit로 분류하세요.

허용 수동분류: {", ".join(VALID_CATEGORIES)}

규칙:
- 아래 자동 키워드 규칙과 승인 예시를 우선 참고하되, 상품 문맥과 다르면 JSON 결과에 더 적절한 분류를 응답하세요.
- "1인 추가"는 표준_메뉴명_edit="1인 추가", 수동분류_edit="사이드"로 분류합니다.

자동 키워드 규칙:
{rule_text}

기존 승인 예시:
{example_text}

분류 대상:
{items_text}

반드시 JSON 배열만 응답하세요.
각 원소는 item_name, 표준_메뉴명_edit, 수동분류_edit 키를 가져야 합니다.
수동분류_edit는 허용값 중 하나만 사용하세요.
"""


def _load_map_examples() -> list[dict]:
    if FIN_PRODUCT_MAP_TRAIN_JSON_PATH.exists():
        try:
            data = json.loads(FIN_PRODUCT_MAP_TRAIN_JSON_PATH.read_text(encoding="utf-8"))
            examples = []
            for label_data in data.get("label_rules", {}).values():
                examples.extend(label_data.get("examples", []))
            if examples:
                logger.info("train JSON에서 예시 %d건 로드", len(examples))
                return examples
        except Exception as e:
            logger.warning("train JSON 로드 실패, map_df fallback: %s", e)
    return []


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
        think=False,
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
        (map_df[REVIEW_STATUS_COLUMN].fillna("").astype(str).str.strip().apply(_normalize_review_status) == REVIEW_APPROVED)
        & (map_df["표준_메뉴명_edit"].fillna("").astype(str).str.strip() != "")
        & (map_df["수동분류_edit"].fillna("").astype(str).str.strip().isin(VALID_CATEGORIES))
    ]
    if approved.empty:
        return []
    rows = []
    for label in VALID_CATEGORIES:
        label_df = approved[approved["수동분류_edit"].fillna("").astype(str).str.strip() == label]
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
    payload = _build_map_train_payload(map_df, rules=rules)
    counts = payload.get("label_counts", {})
    count_msg = ", ".join(f"{label}={counts.get(label, 0)}" for label in VALID_CATEGORIES)

    if dry_run:
        logger.info("dry-run: train/rules JSON 저장 생략 | %s | %s", count_msg, rule_summary)
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
        save_rules(rules)
        logger.info(
            "fin_product_map_train.json 및 rules 저장: %s | %s | %s",
            FIN_PRODUCT_MAP_TRAIN_JSON_PATH,
            rule_summary,
            count_msg,
        )

    return {"label_counts": counts, **rule_summary, "dry_run": dry_run}


def _classify_batch(batch: list[dict], examples: list[dict], rules: list[dict]) -> list[dict]:
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
        rows.append({
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
        })
    if unmatched_count:
        logger.warning("LLM 응답 item_name 미매칭: %d/%d건", unmatched_count, len(batch))
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
    result = _seed_split_rows_from_siblings(result)
    result = apply_review_edits(result, review_edits)
    result = apply_recently_edits(result, load_recently_map())
    result = _apply_classification_overrides(result)
    result = _auto_approve_zero_price(result)
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
        "join_conflict_keys": join_conflict_count,
        "duplicate_keys": duplicate_count,
        "duplicate_label_keys": duplicate_label_count,
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
        write_review_map(review_rows)
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
    map_df = apply_review_edits(load_map(), load_review_map())
    map_df = apply_recently_edits(map_df, load_recently_map())
    llm_targets = find_llm_targets(all_items, map_df)
    summary = {
        "target_stores": TARGET_STORES,
        "target_rows": int(len(all_items)),
        "already_llm_classified": int(len(all_items) - len(llm_targets)),
        "llm_targets": int(len(llm_targets)),
        "new_classified": 0,
        "new_pending": 0,
        "dry_run": dry_run,
        "limit": limit,
        "output_path": str(FIN_PRODUCT_MAP_CSV_PATH),
        "review_output_path": str(FIN_PRODUCT_MAP_REVIEW_CSV_PATH),
        "recently_output_path": str(FIN_PRODUCT_MAP_RECENTLY_CSV_PATH),
    }

    if all_items.empty:
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
    map_df = _auto_approve_zero_price(map_df)
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
    summary["join_conflict_keys"] = int(join_conflicts.drop_duplicates(subset=["item_id", "store", "source", "brand"]).shape[0])
    summary["duplicate_label_keys"] = int(
        review_rows[review_rows[DUP_LABEL_COLUMN].fillna("").astype(str).str.strip() == "Y"]
        .drop_duplicates(subset=["source", "item_id"])
        .shape[0]
    )
    summary["join_output_path"] = str(FIN_PRODUCT_MAP_JOIN_CSV_PATH)
    summary["approved"] = int((map_df[REVIEW_STATUS_COLUMN] == REVIEW_APPROVED).sum()) if not map_df.empty else 0
    summary["pending"] = int((map_df[REVIEW_STATUS_COLUMN] == REVIEW_PENDING).sum()) if not map_df.empty else 0

    if dry_run:
        logger.info("dry-run: LLM 호출 및 CSV 저장 생략")
    elif not map_df.empty:
        write_map(map_df)
        write_review_map(review_rows)
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
