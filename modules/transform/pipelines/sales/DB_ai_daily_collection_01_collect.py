"""
toorder 종합보고서(일별매출보고서) 수집 파이프라인.

다운로드 받은 엑셀을 그대로 보관(`*_raw.xlsx`)하고,
`종합보고서_일별매출보고서_grp`(grp 통합테이블) 기준으로 '일별', '매장별' 표를 재생성해서 저장한다.
"""

import logging
import os
import re
import shutil
import zipfile
from pathlib import Path

import pandas as pd
import pendulum

from modules.extract.crawling_toorder_sales_report_daily_date import run_crawling_single_date
from modules.transform.utility.paths import ANALYTICS_DB, DOWN_DIR
from modules.transform.utility.store_normalize import normalize as _normalize_store_names
from modules.transform.utility.account import get_default_account

logger = logging.getLogger(__name__)

DEFAULT_TOORDER_ID, DEFAULT_TOORDER_PW = get_default_account("toorder")
TOORDER_ID = DEFAULT_TOORDER_ID
TOORDER_PW = DEFAULT_TOORDER_PW

DEFAULT_DEST_DIR = ANALYTICS_DB / "ai_daily_collection"
DEFAULT_DOWNLOAD_DIR = DOWN_DIR / "toorder_sales_report_date"
DEST_DIR = DEFAULT_DEST_DIR
DOWNLOAD_DIR = DEFAULT_DOWNLOAD_DIR
CONSOLIDATED_XLSX = DEST_DIR / "종합보고서_일별매출보고서_통합.xlsx"
DEFAULT_CONSOLIDATED_FILENAME = "종합보고서_일별매출보고서_통합.xlsx"
CONSOLIDATED_XLSX = DEFAULT_DEST_DIR / DEFAULT_CONSOLIDATED_FILENAME

def _xlsx_validation_error(path: Path) -> str | None:
    try:
        with zipfile.ZipFile(path) as zf:
            names = set(zf.namelist())
    except (OSError, zipfile.BadZipFile) as exc:
        return f"xlsx zip open failed: {type(exc).__name__}: {exc}"

    missing = [name for name in ("[Content_Types].xml", "xl/workbook.xml") if name not in names]
    if missing:
        preview = ", ".join(sorted(names)[:5])
        return f"xlsx required parts missing: {missing}; members={preview}"
    return None


def _quarantine_invalid_xlsx(path: Path, reason: str) -> Path:
    quarantine_dir = path.parent / "_invalid_xlsx"
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    target = quarantine_dir / path.name
    if target.exists():
        target = quarantine_dir / f"{path.stem}_invalid_{pendulum.now('Asia/Seoul').format('YYYYMMDD_HHmmss')}{path.suffix}"
    path.rename(target)
    marker = target.with_suffix(target.suffix + ".reason.txt")
    marker.write_text(reason, encoding="utf-8")
    return target

GRP_SHEET_CANDIDATES = [
    "종합보고서_일별매출보고서_grp",
    "종합보고서_일별매출보고서_grp 통합테이블",
    "종합보고서_일별매출보고서(1)",
    "종합보고서_일별매출보고서(2)",
]

def _is_ingest_target_xlsx(path: Path, consolidated_xlsx: Path) -> bool:
    if not path.is_file():
        return False
    if path.suffix.lower() != ".xlsx":
        return False
    if path.name.startswith("~$"):  # Excel lock/temp files
        return False
    try:
        if path.resolve() == consolidated_xlsx.resolve():
            return False
    except Exception:
        # resolve 실패 시에도 이름으로 한번 더 방어
        if path.name == consolidated_xlsx.name:
            return False
    if path.name.endswith("_raw.xlsx"):
        return False
    return True


def _ingest_existing_files(dest_dir: Path, consolidated_xlsx: Path) -> None:
    """
    DEST_DIR에 사용자가 수동으로 넣어둔 xlsx가 있으면 통합본에 같이 적재한다.
    적재가 성공한 파일은 `_raw`로 rename하여 재처리를 방지한다.
    """
    try:
        candidates = sorted(dest_dir.glob("*.xlsx"))
    except Exception as exc:
        logger.warning("수동 파일 스캔 실패(%s): %s", dest_dir, exc)
        return

    for src_xlsx in candidates:
        if not _is_ingest_target_xlsx(src_xlsx, consolidated_xlsx):
            continue

        logger.info("수동 파일 적재 시작: %s", src_xlsx)
        try:
            _upsert_ai_daily_collection_excel(src_xlsx, consolidated_xlsx)
        except Exception as exc:
            logger.exception("수동 파일 적재 실패(건너뜀): %s (%s)", src_xlsx, exc)
            continue

        raw_path = src_xlsx.with_name(f"{src_xlsx.stem}_raw{src_xlsx.suffix}")
        try:
            if raw_path.exists():
                raw_bak = raw_path.with_name(
                    f"{raw_path.stem}_bak_{pendulum.now('Asia/Seoul').format('HHmmss')}{raw_path.suffix}"
                )
                raw_path.rename(raw_bak)
            src_xlsx.rename(raw_path)
            logger.info("수동 파일 보관 처리: %s", raw_path)
        except Exception as exc:
            logger.warning("수동 파일 rename 실패(재처리 가능성 있음): %s (%s)", src_xlsx, exc)

def _resolve_target_date(target_date: str | None) -> str:
    if target_date:
        return target_date
    return pendulum.now("Asia/Seoul").format("YYYY-MM-DD")


def _detect_date_column_auto(df: pd.DataFrame) -> str | None:
    """컬럼을 순회하면서 날짜처럼 보이는 첫 컬럼을 반환 (auto-detection fallback)."""
    for col in df.columns:
        col_name = str(col).strip().lower()
        # 컬럼 이름에 "일", "날짜", "date" 등이 포함되어 있는지 확인
        if any(kw in col_name for kw in ["일", "날짜", "date", "day"]):
            return col
    
    # 컬럼 이름으로 찾을 수 없으면, 첫 10개 데이터를 보고 날짜 형식인지 확인
    for col in df.columns:
        try:
            samples = df[col].dropna().head(10).astype(str)
            # 모든 샘플이 날짜 형식처럼 보이는지 확인
            valid_dates = sum(1 for s in samples if _normalize_parquet_date(s) is not None)
            if valid_dates > 0 and valid_dates == len(samples):
                return col
        except Exception:
            pass
    return None


def _load_ai_daily_collection_grp_df(src_xlsx: Path) -> tuple[pd.DataFrame, str | None, str | None]:
    if not src_xlsx.exists():
        raise FileNotFoundError(f"원본 입력 파일이 없습니다: {src_xlsx}")

    grp_df = _load_grp_df_from_xls(src_xlsx)

    date_col = _pick_first_existing_col(
        grp_df,
        ["일자", "날짜", "주문일자", "판매일자", "매출일자", "일자(YYYY-MM-DD)"],
    )
    # 명시적 후보에서 찾을 수 없으면 자동 감지 시도
    if not date_col:
        date_col = _detect_date_column_auto(grp_df)
    
    store_col = _pick_first_existing_col(
        grp_df,
        ["매장", "매장명", "지점", "점포", "브랜드"],
    )
    normalized = _coerce_numeric_columns(
        grp_df,
        keep_cols={c for c in [date_col, store_col] if c},
    )
    if store_col and store_col in normalized.columns:
        normalized[store_col] = _normalize_store_names(normalized[store_col])
    return normalized, date_col, store_col


def _normalize_parquet_date(value) -> str | None:
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return pd.to_datetime(text).strftime("%Y-%m-%d")
    except Exception:
        if len(text) >= 10:
            return text[:10]
        return text


def _daily_parquet_path(dest_dir: Path, target_date: str) -> Path:
    return dest_dir / f"toorder_daily_store_{target_date.replace('-', '')}.parquet"


def _merge_daily_rows(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    *,
    date_col: str | None,
    store_col: str | None,
) -> pd.DataFrame:
    all_cols = list(dict.fromkeys([*existing_df.columns.tolist(), *new_df.columns.tolist()]))
    existing2 = existing_df.reindex(columns=all_cols)
    new2 = new_df.reindex(columns=all_cols)

    key_cols: list[str] = []
    if date_col and date_col in all_cols:
        key_cols.append(date_col)
    if store_col and store_col in all_cols:
        key_cols.append(store_col)
    if not key_cols:
        key_cols = [c for c in all_cols if not pd.api.types.is_numeric_dtype(new2[c])]

    if not key_cols:
        return pd.concat([existing2, new2], ignore_index=True).drop_duplicates().reset_index(drop=True)

    for col in key_cols:
        existing2[col] = existing2[col].astype(str).str.strip()
        new2[col] = new2[col].astype(str).str.strip()

    new_keys = new2[key_cols].drop_duplicates()
    if len(new_keys) > 0:
        existing2 = existing2.merge(new_keys.assign(_new=1), on=key_cols, how="left")
        existing2 = existing2.loc[existing2["_new"].isna()].drop(columns=["_new"])

    return pd.concat([existing2, new2], ignore_index=True).reset_index(drop=True)


def _upsert_daily_parquets(
    normalized_df: pd.DataFrame,
    *,
    dest_dir: Path,
    date_col: str | None,
    store_col: str | None,
) -> list[Path]:
    if not date_col or date_col not in normalized_df.columns:
        available_cols = ", ".join(sorted(normalized_df.columns.tolist()))
        raise ValueError(
            f"일별 parquet 저장에 필요한 날짜 컬럼이 없습니다. "
            f"찾은 컬럼: [{available_cols}]. "
            f"다음 중 하나를 포함해야 합니다: 일자, 날짜, 주문일자, 판매일자, 매출일자, 일자(YYYY-MM-DD)"
        )

    work_df = normalized_df.copy()
    work_df[date_col] = work_df[date_col].apply(_normalize_parquet_date)
    work_df = work_df.loc[work_df[date_col].notna()].reset_index(drop=True)
    if work_df.empty:
        return []

    saved_paths: list[Path] = []
    for target_date, day_df in work_df.groupby(date_col, dropna=False):
        parquet_path = _daily_parquet_path(dest_dir, str(target_date))
        merged_df = day_df.reset_index(drop=True)
        if parquet_path.exists():
            existing_df = pd.read_parquet(parquet_path, engine="pyarrow")
            merged_df = _merge_daily_rows(
                existing_df=existing_df,
                new_df=merged_df,
                date_col=date_col,
                store_col=store_col,
            )

        temp_path = parquet_path.with_name(f"{parquet_path.stem}._tmp{parquet_path.suffix}")
        merged_df.to_parquet(temp_path, index=False, engine="pyarrow")
        temp_path.replace(parquet_path)
        saved_paths.append(parquet_path)

    return saved_paths


def _ingest_xlsx_to_daily_parquet(src_xlsx: Path, dest_dir: Path) -> list[Path]:
    normalized_df, date_col, store_col = _load_ai_daily_collection_grp_df(src_xlsx)
    return _upsert_daily_parquets(
        normalized_df,
        dest_dir=dest_dir,
        date_col=date_col,
        store_col=store_col,
    )


def _rename_to_raw(src_xlsx: Path) -> Path:
    raw_path = src_xlsx.with_name(f"{src_xlsx.stem}_raw{src_xlsx.suffix}")
    if raw_path.exists():
        raw_bak = raw_path.with_name(
            f"{raw_path.stem}_bak_{pendulum.now('Asia/Seoul').format('HHmmss')}{raw_path.suffix}"
        )
        raw_path.rename(raw_bak)
    src_xlsx.rename(raw_path)
    return raw_path


def _ingest_existing_files_to_daily_parquet(dest_dir: Path) -> list[Path]:
    saved_paths: list[Path] = []
    for src_xlsx in sorted(dest_dir.glob("*.xlsx")):
        if src_xlsx.name.startswith("~$") or src_xlsx.name.endswith("_raw.xlsx"):
            continue
        invalid_reason = _xlsx_validation_error(src_xlsx)
        if invalid_reason:
            quarantined = _quarantine_invalid_xlsx(src_xlsx, invalid_reason)
            logger.warning("Invalid ToOrder daily xlsx quarantined: %s -> %s | %s", src_xlsx, quarantined, invalid_reason)
            continue
        logger.info("수동 xlsx parquet 변환 시작: %s", src_xlsx)
        day_paths = _ingest_xlsx_to_daily_parquet(src_xlsx, dest_dir)
        saved_paths.extend(day_paths)
        _rename_to_raw(src_xlsx)
    return saved_paths


def run_ai_daily_collection_to_daily_parquet_dir(
    *,
    dest_dir: str | Path,
    target_date: str | None = None,
    download_dir: str | Path | None = None,
    toorder_id: str | None = None,
    toorder_pw: str | None = None,
) -> list[str]:
    kst = pendulum.timezone("Asia/Seoul")
    resolved_target_date = _resolve_target_date(target_date)
    resolved_dest_dir = Path(dest_dir)
    resolved_download_dir = Path(download_dir) if download_dir else DEFAULT_DOWNLOAD_DIR

    resolved_dest_dir.mkdir(parents=True, exist_ok=True)
    resolved_download_dir.mkdir(parents=True, exist_ok=True)

    saved_paths = _ingest_existing_files_to_daily_parquet(resolved_dest_dir)

    result = run_crawling_single_date(
        toorder_id=toorder_id or DEFAULT_TOORDER_ID,
        toorder_pw=toorder_pw or DEFAULT_TOORDER_PW,
        target_date=resolved_target_date,
        download_dir=resolved_download_dir,
    )
    if not result.get("success"):
        raise RuntimeError(
            f"toorder 일별매출보고서 다운로드 실패({resolved_target_date}): {result.get('error')}"
        )

    src = Path(result["file"])
    if not src.exists():
        raise FileNotFoundError(f"다운로드 파일이 없습니다: {src}")

    dest = resolved_dest_dir / src.name
    if dest.exists():
        bak = dest.with_name(
            f"{dest.stem}_bak_{pendulum.now(kst).format('HHmmss')}{dest.suffix}"
        )
        dest.rename(bak)

    shutil.move(str(src), str(dest))
    logger.info("다운로드 xlsx 이동: %s", dest)

    invalid_reason = _xlsx_validation_error(dest)
    if invalid_reason:
        quarantined = _quarantine_invalid_xlsx(dest, invalid_reason)
        raise RuntimeError(f"Downloaded ToOrder report is not a valid xlsx: {quarantined} | {invalid_reason}")

    saved_paths.extend(_ingest_xlsx_to_daily_parquet(dest, resolved_dest_dir))
    raw_dest = _rename_to_raw(dest)
    logger.info("처리 완료 xlsx 보관: %s", raw_dest)
    return [str(path) for path in saved_paths]


def run_ai_daily_collection_to_dir(
    *,
    dest_dir: str | Path,
    target_date: str | None = None,
    download_dir: str | Path | None = None,
    consolidated_filename: str = DEFAULT_CONSOLIDATED_FILENAME,
    toorder_id: str | None = None,
    toorder_pw: str | None = None,
) -> str:
    kst = pendulum.timezone("Asia/Seoul")
    resolved_target_date = _resolve_target_date(target_date)
    resolved_dest_dir = Path(dest_dir)
    resolved_download_dir = Path(download_dir) if download_dir else DEFAULT_DOWNLOAD_DIR
    consolidated_xlsx = resolved_dest_dir / consolidated_filename

    resolved_dest_dir.mkdir(parents=True, exist_ok=True)
    resolved_download_dir.mkdir(parents=True, exist_ok=True)

    _ingest_existing_files(resolved_dest_dir, consolidated_xlsx)

    result = run_crawling_single_date(
        toorder_id=toorder_id or DEFAULT_TOORDER_ID,
        toorder_pw=toorder_pw or DEFAULT_TOORDER_PW,
        target_date=resolved_target_date,
        download_dir=resolved_download_dir,
    )

    if not result.get("success"):
        raise RuntimeError(
            f"toorder ?쇰퀎留ㅼ텧蹂닿퀬???ㅼ슫濡쒕뱶 ?ㅽ뙣({resolved_target_date}): {result.get('error')}"
        )

    src = Path(result["file"])
    if not src.exists():
        raise FileNotFoundError(f"?ㅼ슫濡쒕뱶 ?뚯씪 ?놁쓬: {src}")

    dest = resolved_dest_dir / src.name
    if dest.exists():
        bak = dest.with_name(
            f"{dest.stem}_bak_{pendulum.now(kst).format('HHmmss')}{dest.suffix}"
        )
        dest.rename(bak)

    raw_dest = dest.with_name(f"{dest.stem}_raw{dest.suffix}")
    if raw_dest.exists():
        raw_bak = raw_dest.with_name(
            f"{raw_dest.stem}_bak_{pendulum.now(kst).format('HHmmss')}{raw_dest.suffix}"
        )
        raw_dest.rename(raw_bak)

    shutil.move(str(src), str(raw_dest))
    logger.info("?먮낯 ?뚯씪 ?대룞: %s", raw_dest)

    _upsert_ai_daily_collection_excel(raw_dest, consolidated_xlsx)
    logger.info("?듯빀蹂???? %s", consolidated_xlsx)
    return str(consolidated_xlsx)


def run_ai_daily_collection(**context) -> str:
    kst = pendulum.timezone("Asia/Seoul")
    today = pendulum.now(kst).format("YYYY-MM-DD")

    DEST_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # 수동으로 넣어둔 파일도 함께 적재
    _ingest_existing_files(DEST_DIR, CONSOLIDATED_XLSX)

    result = run_crawling_single_date(
        toorder_id=TOORDER_ID,
        toorder_pw=TOORDER_PW,
        target_date=today,
        download_dir=DOWNLOAD_DIR,
    )

    if not result.get("success"):
        raise RuntimeError(
            f"toorder 일별매출보고서 다운로드 실패({today}): {result.get('error')}"
        )

    src = Path(result["file"])
    if not src.exists():
        raise FileNotFoundError(f"다운로드 파일 없음: {src}")

    dest = DEST_DIR / src.name
    if dest.exists():
        bak = dest.with_name(
            f"{dest.stem}_bak_{pendulum.now(kst).format('HHmmss')}{dest.suffix}"
        )
        dest.rename(bak)

    raw_dest = dest.with_name(f"{dest.stem}_raw{dest.suffix}")
    if raw_dest.exists():
        raw_bak = raw_dest.with_name(
            f"{raw_dest.stem}_bak_{pendulum.now(kst).format('HHmmss')}{raw_dest.suffix}"
        )
        raw_dest.rename(raw_bak)

    shutil.move(str(src), str(raw_dest))
    logger.info("원본 파일 이동: %s", raw_dest)

    _upsert_ai_daily_collection_excel(raw_dest, CONSOLIDATED_XLSX)
    logger.info("통합본 저장: %s", CONSOLIDATED_XLSX)
    return str(CONSOLIDATED_XLSX)


def _upsert_ai_daily_collection_excel(src_xlsx: Path, dst_xlsx: Path) -> None:
    """
    다운로드 엑셀을 정리해서 다시 저장한다.

    요구사항:
    - grp 시트를 통합테이블로 사용
    - 합계(총합) 행/열 제거
    - 같은 키(일자/매장 등)는 덮어쓰기, 신규 일자는 append
    - 일별/매장별 표로 저장(통합 grp 기준 재집계)
    """
    if not src_xlsx.exists():
        raise FileNotFoundError(f"원본 엑셀 파일이 없습니다: {src_xlsx}")

    grp_df = _load_grp_df_from_xls(src_xlsx)

    date_col = _pick_first_existing_col(
        grp_df,
        ["일자", "날짜", "주문일자", "판매일자", "매출일자", "일자(YYYY-MM-DD)"],
    )
    store_col = _pick_first_existing_col(grp_df, ["매장", "매장명", "지점", "점포", "브랜드"])

    normalized = _coerce_numeric_columns(
        grp_df, keep_cols={c for c in [date_col, store_col] if c}
    )

    merged_grp = _merge_into_consolidated(
        new_df=normalized,
        dst_xlsx=dst_xlsx,
        date_col=date_col,
        store_col=store_col,
    )

    dst_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(dst_xlsx, engine="openpyxl") as writer:
        merged_grp.to_excel(writer, index=False, sheet_name="grp")

        daily_df = _group_sum(merged_grp, group_col=date_col, drop_group_name="합계")
        store_df = _group_sum(merged_grp, group_col=store_col, drop_group_name="합계")
        if daily_df is not None:
            daily_df.to_excel(writer, index=False, sheet_name="일별")
        if store_df is not None:
            store_df.to_excel(writer, index=False, sheet_name="매장별")


def _pick_grp_sheet(sheet_names: list[str]) -> list[str]:
    for cand in GRP_SHEET_CANDIDATES:
        if cand in sheet_names:
            return [cand]
    for name in sheet_names:
        if "grp" in str(name).lower():
            return [name]
    채널_sheets = sorted(
        [n for n in sheet_names if "채널별(일)매출보고서" in str(n)],
        key=str,
    )
    if 채널_sheets:
        return 채널_sheets
    raise ValueError(f"grp 시트를 찾지 못했습니다. sheet_names={sheet_names}")


def _load_channel_daily_df(src_xlsx: Path, sheets: list[str]) -> pd.DataFrame:
    """
    Parse '채널별(일)매출보고서' sheet format (single-date report).

    Structure:
      Row 0: report title
      Row 1: date range  (e.g. "2026-06-08 ~ 2026-06-08")
      Row 2: column headers
      Row 3+: one row per store

    Only sheet (1) is used — sheet (2) is a transposed pivot.
    A '일자' column is injected from the date in row 1.
    """
    target_sheets = [s for s in sheets if "(1)" in s] or [sheets[0]]

    frames = []
    for sheet in target_sheets:
        df_raw = pd.read_excel(src_xlsx, sheet_name=sheet, engine="openpyxl", header=None)
        if len(df_raw) < 4:
            continue

        date_text = str(df_raw.iloc[1, 0]).strip()
        m = re.search(r"\d{4}-\d{2}-\d{2}", date_text)
        extracted_date = m.group(0) if m else None

        seen: dict[str, int] = {}
        headers = []
        for i, v in enumerate(df_raw.iloc[2]):
            col = str(v).strip() if pd.notna(v) else f"_col_{i}"
            if col in seen:
                seen[col] += 1
                col = f"{col}_{seen[col]}"
            else:
                seen[col] = 0
            headers.append(col)

        df_data = df_raw.iloc[3:].copy()
        df_data.columns = headers
        df_data = df_data.reset_index(drop=True)
        df_data = _clean_excel_table(_drop_total_rows_and_cols(df_data))

        if extracted_date:
            df_data["일자"] = extracted_date

        frames.append(df_data)

    if not frames:
        raise ValueError(f"채널별(일) 시트에서 데이터를 읽지 못했습니다: {src_xlsx}")

    return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]


def _load_datedetail_daily_df(src_xlsx: Path, sheet_names: list[str]) -> pd.DataFrame:
    target_sheets = [s for s in sheet_names if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(s))]
    if not target_sheets:
        target_sheets = [s for s in sheet_names if str(s) != "종합"]
    if not target_sheets:
        raise ValueError(f"일별상세 날짜 시트를 찾지 못했습니다: {sheet_names}")

    frames = []
    for sheet in target_sheets:
        raw = pd.read_excel(src_xlsx, sheet_name=sheet, engine="openpyxl", header=None)
        if raw.shape[0] < 7 or raw.shape[1] < 6:
            continue

        date_text = str(raw.iloc[1, 0]).strip()
        m = re.search(r"\d{4}-\d{2}-\d{2}", date_text) or re.search(
            r"\d{4}-\d{2}-\d{2}", str(sheet)
        )
        extracted_date = m.group(0) if m else None

        store_names = raw.iloc[2].ffill()
        store_tags = raw.iloc[3].ffill()
        metric_names = raw.iloc[5]
        channel_rows = raw.iloc[6:].copy()
        channel_rows = channel_rows.loc[channel_rows.iloc[:, 0].notna()]

        rows: list[dict[str, object]] = []
        seen_store_blocks: set[tuple[str, int]] = set()
        for start_col in range(1, raw.shape[1] - 3, 4):
            store_name = str(store_names.iloc[start_col]).strip()
            if not store_name or store_name.lower() == "nan" or store_name == "합계":
                continue

            metric_block = [str(v).strip() for v in metric_names.iloc[start_col:start_col + 4]]
            if metric_block[:4] != ["총 매출", "매출액", "배달료", "영수 건수"]:
                continue

            block_key = (store_name, start_col)
            if block_key in seen_store_blocks:
                continue
            seen_store_blocks.add(block_key)

            tag_value = store_tags.iloc[start_col]
            tag = None if pd.isna(tag_value) else str(tag_value).strip()
            row: dict[str, object] = {
                "매장": store_name,
                "매장 태그": tag,
                "매장_1": store_name,
                "매장 태그_1": tag,
                "매장_2": store_name,
                "매장 태그_2": tag,
                "매장_3": store_name,
                "매장 태그_3": tag,
            }

            for _, channel_row in channel_rows.iterrows():
                channel = str(channel_row.iloc[0]).strip()
                if not channel or channel.lower() == "nan" or channel == "합계":
                    continue
                values = channel_row.iloc[start_col:start_col + 4].tolist()
                suffixes = ["", "_1", "_2", "_3"]
                for suffix, value in zip(suffixes, values):
                    row[f"{channel}{suffix}"] = value

            if extracted_date:
                row["일자"] = extracted_date
            rows.append(row)

        if rows:
            frames.append(pd.DataFrame(rows))

    if not frames:
        raise ValueError(f"일별상세 시트에서 데이터를 읽지 못했습니다: {src_xlsx}")
    return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]


def _load_grp_df_from_xls(src_xlsx: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(src_xlsx, engine="openpyxl")
    if "종합" in xls.sheet_names and any(
        re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(name)) for name in xls.sheet_names
    ):
        preview = pd.read_excel(src_xlsx, sheet_name="종합", engine="openpyxl", header=None, nrows=1)
        title = str(preview.iloc[0, 0]).strip() if not preview.empty else ""
        if "일별상세" in title:
            return _load_datedetail_daily_df(src_xlsx, xls.sheet_names)

    sheets = _pick_grp_sheet(xls.sheet_names)

    if all("채널별(일)매출보고서" in str(s) for s in sheets):
        return _load_channel_daily_df(src_xlsx, sheets)

    frames = [
        _clean_excel_table(
            _drop_total_rows_and_cols(
                pd.read_excel(src_xlsx, sheet_name=s, engine="openpyxl")
            )
        )
        for s in sheets
    ]
    return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]


def _clean_excel_table(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    out = out.dropna(axis=0, how="all").dropna(axis=1, how="all")
    return out


def _drop_total_rows_and_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # 컬럼명이 '합계'인 경우 제거
    out = out.loc[:, [c for c in out.columns if str(c).strip() != "합계"]]

    # 어떤 셀이라도 '합계'면 행 제거
    def _row_has_total(row: pd.Series) -> bool:
        for v in row.values:
            if isinstance(v, str) and v.strip() == "합계":
                return True
        return False

    if len(out) > 0:
        out = out.loc[~out.apply(_row_has_total, axis=1)]
    return out


def _pick_first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {str(c).strip(): c for c in df.columns}
    for cand in candidates:
        if cand in cols:
            return cols[cand]
    return None


def _coerce_numeric_columns(df: pd.DataFrame, keep_cols: set[str]) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if col in keep_cols:
            continue
        series = out[col]
        if series.dtype == "O":
            cleaned = (
                series.astype(str)
                .str.replace(",", "", regex=False)
                .str.replace(r"\s+", "", regex=True)
            )
            numeric = pd.to_numeric(cleaned, errors="coerce")
            non_na_ratio = float(numeric.notna().mean()) if len(numeric) else 0.0
            if non_na_ratio >= 0.6:
                out[col] = numeric
    return out


def _group_sum(
    df: pd.DataFrame, group_col: str | None, drop_group_name: str
) -> pd.DataFrame | None:
    if not group_col or group_col not in df.columns:
        return None

    tmp = df.copy()
    tmp[group_col] = tmp[group_col].astype(str).str.strip()
    tmp = tmp.loc[tmp[group_col] != drop_group_name]

    numeric_cols = [
        c for c in tmp.columns if c != group_col and pd.api.types.is_numeric_dtype(tmp[c])
    ]
    if not numeric_cols:
        return (
            tmp[[group_col]]
            .drop_duplicates()
            .sort_values(group_col)
            .reset_index(drop=True)
        )

    grouped = tmp.groupby(group_col, dropna=False)[numeric_cols].sum().reset_index()
    return grouped.sort_values(group_col).reset_index(drop=True)


def _merge_into_consolidated(
    new_df: pd.DataFrame,
    dst_xlsx: Path,
    date_col: str | None,
    store_col: str | None,
) -> pd.DataFrame:
    """
    통합 파일이 있으면 grp 시트를 읽어서 new_df를 upsert 한다.
    - key 후보: (일자, 매장) -> 없으면 '숫자형이 아닌 컬럼 전체'
    - 덮어쓰기 규칙: new_df에 존재하는 key 조합은 기존에서 제거 후 append
    """
    if not dst_xlsx.exists():
        return new_df.reset_index(drop=True)

    try:
        existing = pd.read_excel(dst_xlsx, sheet_name="grp", engine="openpyxl")
    except Exception as exc:
        logger.warning("기존 통합본 읽기 실패(%s). 새로 생성합니다: %s", dst_xlsx, exc)
        return new_df.reset_index(drop=True)

    existing = _clean_excel_table(existing)
    existing = _drop_total_rows_and_cols(existing)

    # 컬럼 맞추기(신규 컬럼은 추가, 누락 컬럼은 NaN)
    all_cols = list(dict.fromkeys([*existing.columns.tolist(), *new_df.columns.tolist()]))
    existing2 = existing.reindex(columns=all_cols)
    new2 = new_df.reindex(columns=all_cols)

    key_cols: list[str] = []
    if date_col and date_col in all_cols:
        key_cols.append(date_col)
    if store_col and store_col in all_cols:
        key_cols.append(store_col)
    if not key_cols:
        key_cols = [c for c in all_cols if not pd.api.types.is_numeric_dtype(existing2[c])]

    if key_cols:
        existing2 = existing2.copy()
        new2 = new2.copy()
        for c in key_cols:
            existing2[c] = existing2[c].astype(str).str.strip()
            new2[c] = new2[c].astype(str).str.strip()

        new_keys = new2[key_cols].drop_duplicates()
        if len(new_keys) > 0:
            existing2 = existing2.merge(new_keys.assign(_new=1), on=key_cols, how="left")
            existing2 = existing2.loc[existing2["_new"].isna()].drop(columns=["_new"])

        merged = pd.concat([existing2, new2], ignore_index=True)
        return merged.reset_index(drop=True)

    merged = pd.concat([existing2, new2], ignore_index=True).drop_duplicates()
    return merged.reset_index(drop=True)
