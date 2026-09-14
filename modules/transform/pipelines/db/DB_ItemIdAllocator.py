"""Numeric item_id allocator shared by product/manual sales pipelines."""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import FIN_PRODUCT_CSV_PATH, existing_fin_product_csv_path

logger = logging.getLogger(__name__)

DEFAULT_ITEM_BLOCK_SIZE = 1000
SOURCE_ITEM_BLOCK_SIZE = {"okpos": 10000}
MANUAL_ITEM_BLOCK_SIZE = DEFAULT_ITEM_BLOCK_SIZE
MAX_ITEM_BLOCK_SIZE = max(SOURCE_ITEM_BLOCK_SIZE.values(), default=DEFAULT_ITEM_BLOCK_SIZE)

SOURCE_CODE_RANGES: dict[str, tuple[int, int]] = {
    "easypos": (1_000_000, 1_999_999),
    "unipos": (2_000_000, 2_999_999),
    "unionpos": (2_000_000, 2_999_999),
    "posfeed": (100_000_000, 199_999_999),
    "배민수동": (200_000_000, 299_999_999),
    "쿠팡수동": (300_000_000, 399_999_999),
    "okpos": (10_000_000, 19_999_999),
}

SOURCE_ITEM_BASE: dict[str, int] = {
    "easypos": 1_000_000,
    "unipos": 2_000_000,
    "okpos": 10_000_000,
    "posfeed": 100_000_000,
    "배민수동": 200_000_000,
    "쿠팡수동": 300_000_000,
}

MANUAL_SOURCE_BASE = SOURCE_ITEM_BASE
MANUAL_SOURCES = set(MANUAL_SOURCE_BASE)
OKPOS_ADJUSTMENT_ITEM_ID = "19999999"

ALLOCATOR_COLUMNS = ["brand", "store", "item_key", "store_seq", "item_seq"]

_ITEM_KEY_RE = re.compile(r"[^0-9A-Za-z가-힣一-龥]+")


def normalize_item_key(name: object) -> str:
    text = "" if name is None else str(name)
    text = text.strip().lower()
    if not text or text == "nan":
        return ""
    key = _ITEM_KEY_RE.sub("", text)
    if key:
        return key
    return re.sub(r"\s+", "", text)


def canonical_source(source: object) -> str:
    text = "" if source is None else str(source)
    text = text.strip()
    lowered = text.lower()
    if lowered == "unionpos":
        return "unipos"
    return lowered if lowered in {"okpos", "easypos", "unipos", "posfeed"} else text


def is_manual_allocated_source(source: object) -> bool:
    return canonical_source(source) in MANUAL_SOURCES


def item_block_size(source: object) -> int:
    return SOURCE_ITEM_BLOCK_SIZE.get(canonical_source(source), DEFAULT_ITEM_BLOCK_SIZE)


def ensure_allocator_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ALLOCATOR_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out


def _read_master() -> pd.DataFrame:
    source_path = existing_fin_product_csv_path()
    if source_path.exists():
        df = pd.read_csv(source_path, dtype=str, encoding="utf-8-sig").fillna("")
    else:
        df = pd.DataFrame()
    return ensure_allocator_columns(df)


def _safe_replace(src: Path, dst: Path) -> None:
    # OneDrive/FUSE/Windows 공유 드라이브에서 동시 쓰기나 lock으로 os.replace가 실패하는 경우가 있어
    # 재시도 + 파일 권한 보정 후 덮어쓰기까지 시도한다.
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            os.replace(src, dst)
            return
        except (PermissionError, OSError) as exc:
            last_error = exc
            logger.warning(
                "fin_product_grp_input 교체 재시도: target=%s attempt=%d/3 error=%s",
                dst,
                attempt,
                exc,
            )
            if attempt < 3:
                time.sleep(attempt * 1.2)

    try:
        if dst.exists():
            try:
                mode = dst.stat().st_mode | 0o200  # ensure 사용자 쓰기 비트
                dst.chmod(mode)
            except Exception as chmod_exc:
                logger.debug("dst chmod 실패(무시): target=%s err=%s", dst, chmod_exc)
        with src.open("rb") as src_fp, dst.open("wb") as dst_fp:
            dst_fp.write(src_fp.read())
        return
    except Exception as overwrite_exc:
        pending = dst.with_name(f"{dst.name}.pending_{int(time.time())}.csv")
        try:
            os.replace(src, pending)
            logger.error(
                "fin_product_grp_input 최종 쓰기 실패, pending 저장: target=%s pending=%s replace_err=%s write_err=%s",
                dst,
                pending,
                last_error,
                overwrite_exc,
            )
        except Exception as pending_exc:
            logger.error(
                "fin_product_grp_input 최종 쓰기 실패 및 pending 저장 실패: target=%s pending=%s"
                " replace_err=%s write_err=%s pending_err=%s",
                dst,
                pending,
                last_error,
                overwrite_exc,
                pending_exc,
            )
            raise
        logger.warning(
            "fin_product_grp_input 저장 지연: pending=%s. 재실행 시 병합/반영 로직을 검토하세요.",
            pending,
        )


def _write_master(df: pd.DataFrame) -> None:
    FIN_PRODUCT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = FIN_PRODUCT_CSV_PATH.with_suffix(".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        _safe_replace(tmp, FIN_PRODUCT_CSV_PATH)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def _clean_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return df[col].fillna("").astype(str).str.strip()


def _coerce_float(text: str) -> float:
    try:
        return float(text)
    except (TypeError, ValueError):
        return float("nan")


def _item_seq_token(value: object) -> str:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return ""
    numeric = _coerce_float(text)
    if pd.isna(numeric):
        return ""
    seq = int(numeric)
    if 0 <= seq < MAX_ITEM_BLOCK_SIZE:
        return str(seq)
    return ""


def _item_seq_token_series(values: pd.Series) -> pd.Series:
    text = values.fillna("").astype(str).str.strip()
    numeric = pd.to_numeric(text, errors="coerce")
    valid = numeric.notna() & numeric.ge(0) & numeric.lt(MAX_ITEM_BLOCK_SIZE)
    result = pd.Series("", index=values.index, dtype="object")
    result.loc[valid] = numeric.loc[valid].astype("int64").astype(str)
    return result


def _raw_item_token(row: pd.Series) -> str:
    for col in ("item_id", "상품코드", "item_seq"):
        value = row.get(col, "")
        source = canonical_source(row.get("source", ""))
        if col in ("item_id", "상품코드") and _code_in_source_range(source, value):
            token = str((int(str(value).strip()) - _source_base(source)) % item_block_size(source))
        else:
            token = _item_seq_token(value)
        if token:
            return token
    return normalize_item_key(row.get("item_name", row.get("상품명", "")))


def _manual_key_tuple(source: object, brand: object, store: object, item_name: object) -> tuple[str, str, str, str]:
    return (
        canonical_source(source),
        str(brand or "").strip(),
        str(store or "").strip(),
        normalize_item_key(item_name),
    )


def _manual_master_key_series(master: pd.DataFrame) -> pd.Series:
    source = _clean_series(master, "source").map(canonical_source)
    brand = _clean_series(master, "brand")
    store = _clean_series(master, "store")
    item_seq = _item_seq_token_series(_clean_series(master, "item_seq"))
    if "item_key" in master.columns:
        item_key = _clean_series(master, "item_key")
    else:
        item_key = pd.Series([""] * len(master), index=master.index)
    name_key = _clean_series(master, "상품명").map(normalize_item_key)
    item_key = item_key.where(item_key != "", name_key)
    token = item_seq.where(item_seq != "", item_key)
    return pd.Series(list(zip(source, brand, store, token)), index=master.index)


def _source_base(source: str) -> int:
    base = SOURCE_ITEM_BASE.get(canonical_source(source))
    if base is None:
        raise ValueError(f"item_id 배정 대상이 아닌 source: {source}")
    return base


def _code_in_source_range(source: str, code: object) -> bool:
    text = str(code or "").strip()
    if not text:
        return False
    numeric = _coerce_float(text)
    if pd.isna(numeric):
        return False
    start, end = SOURCE_CODE_RANGES.get(canonical_source(source), (None, None))
    if start is None:
        return False
    return start <= int(numeric) <= end


def _numeric_value(value: object) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    numeric = _coerce_float(text)
    if pd.isna(numeric):
        return None
    return int(numeric)


def _allocator_state(master: pd.DataFrame) -> tuple[
    dict[tuple[str, str, str], int],
    dict[str, int],
    dict[tuple[str, int], set[int]],
]:
    src = _clean_series(master, "source").map(canonical_source)
    brand = _clean_series(master, "brand")
    store = _clean_series(master, "store")
    store_seq_s = pd.to_numeric(_clean_series(master, "store_seq"), errors="coerce")
    item_seq_s = pd.to_numeric(_clean_series(master, "item_seq"), errors="coerce")

    store_seq_by_key: dict[tuple[str, str, str], int] = {}
    max_store_seq_by_source: dict[str, int] = {}
    used_item_seq: dict[tuple[str, int], set[int]] = {}

    valid_store = src.isin(MANUAL_SOURCES) & store_seq_s.notna()
    for idx in master[valid_store].index:
        source = src.at[idx]
        store_seq = int(store_seq_s.at[idx])
        key = (source, brand.at[idx], store.at[idx])
        store_seq_by_key[key] = store_seq
        max_store_seq_by_source[source] = max(max_store_seq_by_source.get(source, -1), store_seq)

    for source in MANUAL_SOURCES:
        consistent = _consistent_allocator_mask(master, source)
        valid_item = consistent & store_seq_s.notna() & item_seq_s.notna()
        for idx in master[valid_item].index:
            store_seq = int(store_seq_s.at[idx])
            item_seq = int(item_seq_s.at[idx])
            used_item_seq.setdefault((source, store_seq), set()).add(item_seq)

    return store_seq_by_key, max_store_seq_by_source, used_item_seq


def _pick_item_seq_from_state(
    used_item_seq: dict[tuple[str, int], set[int]],
    source: str,
    store_seq: int,
    preferred: str,
) -> int:
    block_size = item_block_size(source)
    used = used_item_seq.setdefault((source, store_seq), set())
    if preferred:
        seq = int(preferred)
        if 0 <= seq < block_size and seq not in used:
            return seq
    next_seq = 0 if not used else max(used) + 1
    if next_seq >= block_size:
        for seq in range(block_size):
            if seq not in used:
                return seq
    if next_seq >= block_size:
        raise ValueError(f"{source} store_seq={store_seq} 상품 슬롯 초과: {next_seq + 1}/{block_size}")
    return next_seq


def _allocator_code_consistent(source: object, code: object, store_seq: object, item_seq: object) -> bool:
    source_key = canonical_source(source)
    code_num = _numeric_value(code)
    store_num = _numeric_value(store_seq)
    item_num = _numeric_value(item_seq)
    if code_num is None or store_num is None or item_num is None:
        return True
    if source_key not in SOURCE_ITEM_BASE:
        return True
    expected = _source_base(source_key) + store_num * item_block_size(source_key) + item_num
    return code_num == expected


def _consistent_allocator_mask(master: pd.DataFrame, source: str) -> pd.Series:
    src = _clean_series(master, "source").map(canonical_source)
    mask = src.eq(source)
    if not mask.any():
        return mask
    code = pd.to_numeric(_clean_series(master, "상품코드"), errors="coerce")
    store_seq = pd.to_numeric(_clean_series(master, "store_seq"), errors="coerce")
    item_seq = pd.to_numeric(_clean_series(master, "item_seq"), errors="coerce")
    consistent = pd.Series(True, index=master.index)
    comparable = mask & code.notna() & store_seq.notna() & item_seq.notna()
    if comparable.any():
        expected = _source_base(source) + store_seq[comparable].astype("int64") * item_block_size(source) + item_seq[comparable].astype("int64")
        consistent.loc[comparable] = code[comparable].astype("int64").eq(expected)
    skipped = int((mask & ~consistent).sum())
    if skipped:
        logger.debug("legacy %s item_id 배정 이력 제외: %d행", source, skipped)
    return mask & consistent


def _existing_manual_assignments(master: pd.DataFrame, source: str) -> dict[tuple[str, str, str, str], str]:
    if master.empty or "source" not in master.columns or "상품코드" not in master.columns:
        return {}
    out = master.copy()
    keys = _manual_master_key_series(out)
    src = _clean_series(out, "source").map(canonical_source)
    brand = _clean_series(out, "brand")
    store = _clean_series(out, "store")
    item_key = _clean_series(out, "item_key") if "item_key" in out.columns else pd.Series([""] * len(out), index=out.index)
    name_key = _clean_series(out, "상품명").map(normalize_item_key)
    code = _clean_series(out, "상품코드")
    mask = _consistent_allocator_mask(out, source) & code.ne("")
    code_set = set(code[mask])
    assignments: dict[tuple[str, str, str, str], str] = {}
    for idx in out[mask].index:
        key = keys.at[idx]
        if not all(key):
            continue
        if not _code_in_source_range(source, code.at[idx]):
            continue
        assignments[key] = code.at[idx]
        product_key = item_key.at[idx] or name_key.at[idx]
        by_name_key = (source, brand.at[idx], store.at[idx], product_key)
        if all(by_name_key):
            assignments.setdefault(by_name_key, code.at[idx])

    legacy_mask = src.eq(source) & code.ne("") & ~mask
    if legacy_mask.any():
        store_seq = pd.to_numeric(_clean_series(out, "store_seq"), errors="coerce")
        item_seq = pd.to_numeric(_clean_series(out, "item_seq"), errors="coerce")
        for idx in out[legacy_mask & store_seq.notna() & item_seq.notna()].index:
            product_key = item_key.at[idx] or name_key.at[idx]
            if not product_key:
                continue
            candidate = str(_source_base(source) + int(store_seq.at[idx]) * item_block_size(source) + int(item_seq.at[idx]))
            if candidate not in code_set:
                continue
            by_name_key = (source, brand.at[idx], store.at[idx], product_key)
            if all(by_name_key):
                assignments.setdefault(by_name_key, candidate)
    return assignments


def _next_store_seq(master: pd.DataFrame, source: str, brand: str, store: str) -> int:
    src = _clean_series(master, "source").map(canonical_source)
    brand_s = _clean_series(master, "brand")
    store_s = _clean_series(master, "store")
    store_seq_s = pd.to_numeric(_clean_series(master, "store_seq"), errors="coerce")

    same_store = src.eq(source) & brand_s.eq(brand) & store_s.eq(store) & store_seq_s.notna()
    if same_store.any():
        return int(store_seq_s[same_store].iloc[-1])

    used = store_seq_s[src.eq(source) & store_seq_s.notna()]
    return 0 if used.empty else int(used.max()) + 1


def _next_item_seq(master: pd.DataFrame, source: str, store_seq: int) -> int:
    block_size = item_block_size(source)
    src = _clean_series(master, "source").map(canonical_source)
    store_seq_s = pd.to_numeric(_clean_series(master, "store_seq"), errors="coerce")
    item_seq_s = pd.to_numeric(_clean_series(master, "item_seq"), errors="coerce")
    consistent = _consistent_allocator_mask(master, source)
    used = item_seq_s[consistent & src.eq(source) & store_seq_s.eq(store_seq) & item_seq_s.notna()]
    next_seq = 0 if used.empty else int(used.max()) + 1
    if next_seq >= block_size:
        raise ValueError(f"{source} store_seq={store_seq} 상품 슬롯 초과: {next_seq + 1}/{block_size}")
    return next_seq


def _pick_item_seq(master: pd.DataFrame, source: str, store_seq: int, preferred: str) -> int:
    block_size = item_block_size(source)
    src = _clean_series(master, "source").map(canonical_source)
    store_seq_s = pd.to_numeric(_clean_series(master, "store_seq"), errors="coerce")
    item_seq_s = pd.to_numeric(_clean_series(master, "item_seq"), errors="coerce")
    consistent = _consistent_allocator_mask(master, source)
    used = set(
        item_seq_s[consistent & src.eq(source) & store_seq_s.eq(store_seq) & item_seq_s.notna()]
        .astype(int)
        .tolist()
    )
    if preferred:
        seq = int(preferred)
        if 0 <= seq < block_size and seq not in used:
            return seq
    return _next_item_seq(master, source, store_seq)


def _make_manual_master_row(columns: list[str], row: pd.Series, code: str, store_seq: int, item_seq: int) -> dict[str, str]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result = {col: "" for col in columns}
    item_name = str(row.get("item_name", "")).strip()
    source = canonical_source(row.get("source", ""))
    result.update({
        "source": source,
        "brand": str(row.get("brand", "")).strip(),
        "store": str(row.get("store", "")).strip(),
        "item_key": normalize_item_key(item_name),
        "store_seq": str(store_seq),
        "item_seq": str(item_seq),
        "구분": str(row.get("구분", "배달") or "배달"),
        "상품코드": code,
        "상품명": item_name,
        "판매단가": str(row.get("unit_price", row.get("unitprice", "")) or ""),
        "is_main_candidate": "N",
        "llm_check": "N",
        "exclude_check": "N",
        "updated_at": now,
        "is_latest": "Y",
    })
    return result


def allocate_manual_item_ids(
    rows: pd.DataFrame,
    *,
    persist: bool = True,
) -> pd.Series:
    """Return numeric, store-scoped item_id for POS/manual sources.

    Required row columns: source, brand, store, item_name. Existing master keys are reused.
    New keys are appended to fin_product_grp_input.csv when persist=True.
    """
    if rows.empty:
        return pd.Series([], dtype=str, index=rows.index)

    required = {"source", "brand", "store", "item_name"}
    missing = required - set(rows.columns)
    if missing:
        raise ValueError(f"item_id 배정 필수 컬럼 누락: {sorted(missing)}")

    master = _read_master()
    master = ensure_allocator_columns(master)
    if "source" not in master.columns:
        master["source"] = ""
    if "상품코드" not in master.columns:
        master["상품코드"] = ""
    if "상품명" not in master.columns:
        master["상품명"] = ""

    out = pd.Series([""] * len(rows), index=rows.index, dtype=str)
    new_rows: list[dict[str, str]] = []
    columns = list(master.columns)
    for col in ["source", "구분", "대메뉴", "중메뉴", "상품코드", "상품명", "판매단가", "is_main_candidate", "llm_check", "exclude_check", "updated_at", "is_latest", *ALLOCATOR_COLUMNS]:
        if col not in columns:
            columns.append(col)

    work_master = master.reindex(columns=columns, fill_value="").copy()
    assignment_cache: dict[str, dict[tuple[str, str, str, str], str]] = {
        source: _existing_manual_assignments(work_master, source)
        for source in MANUAL_SOURCES
    }
    store_seq_by_key, max_store_seq_by_source, used_item_seq = _allocator_state(work_master)

    for idx, row in rows.iterrows():
        source = canonical_source(row.get("source", ""))
        if source not in SOURCE_ITEM_BASE:
            raise ValueError(f"item_id 배정 대상이 아닌 source: {row.get('source', '')}")

        brand = str(row.get("brand", "")).strip()
        store = str(row.get("store", "")).strip()
        item_name = str(row.get("item_name", "")).strip()
        item_token = _raw_item_token(row)
        if not store or not item_token:
            raise ValueError(f"{source} item_id 배정 실패: store/item_name 필수")

        key = (source, brand, store, item_token)
        existing = assignment_cache[source].get(key)
        if existing:
            out.at[idx] = existing
            continue

        store_key = (source, brand, store)
        if store_key in store_seq_by_key:
            store_seq = store_seq_by_key[store_key]
        else:
            store_seq = max_store_seq_by_source.get(source, -1) + 1
            store_seq_by_key[store_key] = store_seq
            max_store_seq_by_source[source] = store_seq

        preferred_seq = item_token if item_token.isdigit() else ""
        item_seq = _pick_item_seq_from_state(used_item_seq, source, store_seq, preferred_seq)
        code = str(_source_base(source) + store_seq * item_block_size(source) + item_seq)
        row_for_master = row.copy()
        row_for_master["source"] = source
        master_row = _make_manual_master_row(columns, row_for_master, code, store_seq, item_seq)
        new_rows.append(master_row)
        used_item_seq.setdefault((source, store_seq), set()).add(item_seq)
        assignment_cache[source][key] = code
        out.at[idx] = code

    if new_rows and persist:
        work_master = pd.concat([work_master, pd.DataFrame(new_rows, columns=columns)], ignore_index=True)
        validate_fin_product_codes(work_master, allow_legacy_blank=True)
        _write_master(work_master.reindex(columns=columns, fill_value=""))
        logger.info("item_id 신규 배정: +%d행", len(new_rows))

    return out


def validate_fin_product_codes(df: pd.DataFrame, *, allow_legacy_blank: bool = False) -> None:
    if df.empty:
        return
    if "source" not in df.columns or "상품코드" not in df.columns:
        raise ValueError("fin_product code 검증 필수 컬럼 누락: source/상품코드")

    source = _clean_series(df, "source").map(canonical_source)
    code = _clean_series(df, "상품코드")
    blank = code.eq("")
    if blank.any() and not allow_legacy_blank:
        raise ValueError(f"빈 상품코드 {int(blank.sum())}건")
    if "is_latest" in df.columns:
        latest = _clean_series(df, "is_latest").str.upper().eq("Y")
        blank_latest = blank & latest
        if blank_latest.any():
            raise ValueError(f"is_latest=Y 빈 상품코드 {int(blank_latest.sum())}건")

    target = ~blank
    numeric = pd.to_numeric(code[target], errors="coerce")
    bad_numeric = numeric.isna()
    if bad_numeric.any():
        samples = code[target][bad_numeric].head(10).tolist()
        raise ValueError(f"비숫자 상품코드 {int(bad_numeric.sum())}건: {samples}")

    num_full = pd.to_numeric(code.where(target, "0"), errors="coerce").fillna(0).astype(int)
    for src, (start, end) in SOURCE_CODE_RANGES.items():
        mask = target & source.eq(src)
        if not mask.any():
            continue
        outside = mask & ~num_full.between(start, end)
        if outside.any():
            samples = df.loc[outside, ["source", "상품코드"]].head(10).to_dict("records")
            raise ValueError(f"{src} 상품코드 대역 이탈 {int(outside.sum())}건: {samples}")

    manual_target = target & source.isin(MANUAL_SOURCES)
    if manual_target.any():
        brand = _clean_series(df, "brand")
        store = _clean_series(df, "store")
        item_seq_token = _item_seq_token_series(_clean_series(df, "item_seq"))
        item_key = _clean_series(df, "item_key").where(
            _clean_series(df, "item_key") != "",
            _clean_series(df, "상품명").map(normalize_item_key),
        )
        token = item_seq_token.where(item_seq_token != "", item_key)
        identity = pd.Series(list(zip(source, brand, store, token)), index=df.index)
        conflict_codes = []
        for code_value, idxs in df[manual_target].groupby(code[manual_target]).groups.items():
            identities = {identity.at[i] for i in idxs}
            if len(identities) > 1:
                conflict_codes.append(code_value)
            if len(conflict_codes) >= 20:
                break
        if conflict_codes:
            raise ValueError(f"상품코드가 서로 다른 상품키에 중복 사용됨: {conflict_codes}")

    for src in MANUAL_SOURCES:
        mask = source.eq(src)
        if not mask.any():
            continue
        store = _clean_series(df, "store")
        item_seq_token = _item_seq_token_series(_clean_series(df, "item_seq"))
        item_key = _clean_series(df, "item_key").where(
            _clean_series(df, "item_key") != "",
            _clean_series(df, "상품명").map(normalize_item_key),
        )
        token = item_seq_token.where(item_seq_token != "", item_key)
        missing_key = mask & (store.eq("") | token.eq(""))
        if missing_key.any() and not allow_legacy_blank:
            raise ValueError(f"{src} store/item_seq 누락 {int(missing_key.sum())}건")

        item_seq = pd.to_numeric(_clean_series(df, "item_seq"), errors="coerce")
        block_size = item_block_size(src)
        overflow = mask & item_seq.notna() & item_seq.ge(block_size)
        if overflow.any():
            raise ValueError(f"{src} 매장별 {block_size}개 슬롯 초과 {int(overflow.sum())}건")
