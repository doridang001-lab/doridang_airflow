"""Build monthly Ddangyo fee ratio CSV from manual settlement xls files."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import (
    DDANGYO_FEE_RATIO_BASELINE_CSV,
    DDANGYO_FEE_RATIO_MONTHLY_CSV,
    MANUAL_DOWN_DIR,
    ONEDRIVE_DB,
)

logger = logging.getLogger(__name__)

OUTPUT_COLUMNS = ["ym", "store", "fee_ratio"]
BASELINE_COLUMNS = ["ym", "fee_ratio"]
PLATFORM_NAME = "땡겨요"
SOURCE_PATTERN = "땡겨요 정산내역(일별)*.xls"
START_YM = "2026_01"
END_YM = "2026_12"
UNMAPPED_LATEST_CSV = Path(".tmp") / "ddangyo_fee_unmapped_latest.csv"
_DORIDANG_TERM = "도리당".encode("utf-16le")
_STORE_PATTERN = re.compile(r"(?:닭도리탕 전문\s*)?(도리당\s*[가-힣A-Za-z0-9()\- ]{1,30}?점)")


@dataclass(frozen=True)
class DdangyoSettlementRow:
    path: Path
    ym: str
    store: str | None
    total_amt: float
    deposit_amt: float


def _read_sales_employee(path: Path = ONEDRIVE_DB / "sales_employee.csv") -> pd.DataFrame:
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=encoding)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig")


def _load_ddangyo_store_names(accounts_path: Path = ONEDRIVE_DB / "sales_employee.csv") -> list[str]:
    frame = _read_sales_employee(accounts_path)
    required = {"매장명", "플랫폼"}
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(f"sales_employee.csv 필수 컬럼 없음: {sorted(missing)}")
    stores = (
        frame[frame["플랫폼"].fillna("").astype(str).str.strip().eq(PLATFORM_NAME)]["매장명"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    return sorted({store for store in stores if store})


def _store_key(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "").strip())


def _canonical_store(raw_store: str | None, known_stores: list[str]) -> str | None:
    if not raw_store:
        return None
    raw = re.sub(r"\s+", " ", raw_store).strip()
    known_by_key = {_store_key(store): store for store in known_stores}
    return known_by_key.get(_store_key(raw), raw)


def _extract_store_from_binary(path: Path, known_stores: list[str]) -> str | None:
    data = path.read_bytes()
    candidates: list[str] = []
    for known in known_stores:
        if known.encode("utf-16le") in data:
            candidates.append(known)
    if candidates:
        return sorted(candidates, key=len, reverse=True)[0]

    index = data.find(_DORIDANG_TERM)
    if index < 0:
        return None
    chunk = data[max(0, index - 200): min(len(data), index + 400)]
    for offset in (0, 1):
        text = chunk[offset:].decode("utf-16le", errors="ignore")
        for match in _STORE_PATTERN.finditer(text):
            candidate = re.sub(r"\s+", " ", match.group(1)).strip()
            if candidate:
                candidates.append(candidate)
    if not candidates:
        return None
    return _canonical_store(candidates[0], known_stores)


def _parse_ym(value: object) -> str:
    numbers = re.findall(r"\d+", str(value or ""))
    if len(numbers) < 2:
        raise ValueError(f"정산 월 파싱 실패: {value!r}")
    return f"{numbers[0]}_{int(numbers[1]):02d}"


def _find_summary_amounts(frame: pd.DataFrame) -> tuple[float, float]:
    text = frame.fillna("").astype(str)
    total_pos = deposit_pos = None
    for row_idx, row in text.iterrows():
        for col_idx, value in row.items():
            normalized = re.sub(r"\s+", "", value)
            if normalized == "(A)주문결제":
                total_pos = (row_idx, col_idx)
            if normalized == "입금금액":
                deposit_pos = (row_idx, col_idx)
        if total_pos and deposit_pos:
            amount_row = row_idx + 1
            try:
                total = pd.to_numeric(frame.loc[amount_row, total_pos[1]], errors="raise")
                deposit = pd.to_numeric(frame.loc[amount_row, deposit_pos[1]], errors="raise")
            except Exception as exc:
                raise ValueError("정산 요약 금액 파싱 실패") from exc
            return float(total), float(deposit)
    raise ValueError("정산 요약 헤더를 찾지 못함")


def _read_settlement_file(path: Path, known_stores: list[str]) -> DdangyoSettlementRow:
    try:
        frame = pd.read_excel(path, sheet_name=0, header=None, engine="xlrd")
    except ImportError as exc:
        raise RuntimeError("땡겨요 xls 읽기에는 xlrd>=2.0.1 설치가 필요합니다") from exc
    except Exception as exc:
        raise RuntimeError(f"땡겨요 정산 파일 읽기 실패: {path}") from exc
    ym = _parse_ym(frame.iloc[0, 0])
    total_amt, deposit_amt = _find_summary_amounts(frame)
    store = _extract_store_from_binary(path, known_stores)
    return DdangyoSettlementRow(
        path=path,
        ym=ym,
        store=_canonical_store(store, known_stores),
        total_amt=total_amt,
        deposit_amt=deposit_amt,
    )


def _month_range(start_ym: str = START_YM, end_ym: str = END_YM) -> list[str]:
    start = pd.Period(start_ym.replace("_", "-"), freq="M")
    end = pd.Period(end_ym.replace("_", "-"), freq="M")
    return [period.strftime("%Y_%m") for period in pd.period_range(start, end, freq="M")]


def _rows_to_source_frame(
    rows: list[DdangyoSettlementRow],
    known_stores: list[str],
) -> pd.DataFrame:
    source = pd.DataFrame(
        [
            {
                "source_path": str(row.path),
                "ym": row.ym,
                "store": _canonical_store(row.store, known_stores),
                "total_amt": row.total_amt,
                "deposit_amt": row.deposit_amt,
            }
            for row in rows
        ]
    )
    if source.empty:
        source = pd.DataFrame(columns=["source_path", "ym", "store", "total_amt", "deposit_amt"])

    source["total_amt"] = pd.to_numeric(source["total_amt"], errors="coerce").fillna(0.0)
    source["deposit_amt"] = pd.to_numeric(source["deposit_amt"], errors="coerce").fillna(0.0)
    return source


def _actual_ratios(source: pd.DataFrame) -> pd.DataFrame:
    mapped = source[
        source["store"].notna()
        & ~source["store"].astype(str).str.strip().eq("")
    ].copy()
    actual = (
        mapped.groupby(["ym", "store"], as_index=False)[["total_amt", "deposit_amt"]]
        .sum()
    )
    actual = actual[actual["total_amt"].gt(0)].copy()
    actual["fee_ratio"] = (actual["total_amt"] - actual["deposit_amt"]) / actual["total_amt"]
    return actual


def _build_monthly_baseline(
    rows: list[DdangyoSettlementRow],
    known_stores: list[str],
    *,
    start_ym: str = START_YM,
    end_ym: str = END_YM,
) -> pd.DataFrame:
    source = _rows_to_source_frame(rows, known_stores)
    actual = _actual_ratios(source)
    months = _month_range(start_ym, end_ym)
    if actual.empty:
        return pd.DataFrame({"ym": months, "fee_ratio": [0.0] * len(months)})[BASELINE_COLUMNS]

    global_total = float(actual["total_amt"].sum())
    global_ratio = (
        float((actual["total_amt"] - actual["deposit_amt"]).sum()) / global_total
        if global_total > 0
        else 0.0
    )
    monthly = actual.groupby("ym", as_index=False)[["total_amt", "deposit_amt"]].sum()
    monthly["fee_ratio"] = (monthly["total_amt"] - monthly["deposit_amt"]) / monthly["total_amt"]
    result = pd.DataFrame({"ym": months}).merge(monthly[BASELINE_COLUMNS], on="ym", how="left")
    result["fee_ratio"] = (
        pd.to_numeric(result["fee_ratio"], errors="coerce")
        .fillna(global_ratio)
        .clip(lower=0, upper=1)
        .round(6)
    )
    return result[BASELINE_COLUMNS]


def _build_monthly_result(
    rows: list[DdangyoSettlementRow],
    known_stores: list[str],
    *,
    start_ym: str = START_YM,
    end_ym: str = END_YM,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    source = _rows_to_source_frame(rows, known_stores)
    unmapped = source[source["store"].isna() | source["store"].astype(str).str.strip().eq("")].copy()
    mapped = source.drop(unmapped.index).copy()

    store_order = list(dict.fromkeys([*known_stores, *mapped["store"].dropna().astype(str).tolist()]))
    months = _month_range(start_ym, end_ym)
    if not store_order:
        return pd.DataFrame(columns=OUTPUT_COLUMNS), unmapped

    actual = _actual_ratios(source)

    global_total = float(actual["total_amt"].sum())
    global_ratio = (
        float((actual["total_amt"] - actual["deposit_amt"]).sum()) / global_total
        if global_total > 0
        else 0.0
    )
    store_totals = actual.groupby("store")[["total_amt", "deposit_amt"]].sum()
    store_ratios = {
        store: (float(row["total_amt"] - row["deposit_amt"]) / float(row["total_amt"]))
        for store, row in store_totals.iterrows()
        if float(row["total_amt"]) > 0
    }

    grid = pd.MultiIndex.from_product([months, store_order], names=["ym", "store"]).to_frame(index=False)
    result = grid.merge(actual[["ym", "store", "fee_ratio"]], on=["ym", "store"], how="left")
    result["fee_ratio"] = [
        ratio if pd.notna(ratio) else store_ratios.get(store, global_ratio)
        for ratio, store in zip(result["fee_ratio"], result["store"])
    ]
    result["fee_ratio"] = pd.to_numeric(result["fee_ratio"], errors="coerce").fillna(0).clip(lower=0, upper=1)
    result["fee_ratio"] = result["fee_ratio"].round(6)
    return result[OUTPUT_COLUMNS], unmapped


def _write_csv_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    frame.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    tmp_path.replace(path)


def build_ddangyo_fee_ratio_monthly(
    *,
    source_dir: Path = MANUAL_DOWN_DIR,
    source_pattern: str = SOURCE_PATTERN,
    accounts_path: Path = ONEDRIVE_DB / "sales_employee.csv",
    output_path: Path = DDANGYO_FEE_RATIO_MONTHLY_CSV,
    baseline_path: Path = DDANGYO_FEE_RATIO_BASELINE_CSV,
    unmapped_path: Path = UNMAPPED_LATEST_CSV,
    start_ym: str = START_YM,
    end_ym: str = END_YM,
) -> str:
    known_stores = _load_ddangyo_store_names(accounts_path)
    paths = sorted(source_dir.glob(source_pattern), key=lambda path: path.stat().st_mtime)
    if not paths:
        raise RuntimeError(f"땡겨요 정산 원천 파일 없음: {source_dir / source_pattern}")

    rows: list[DdangyoSettlementRow] = []
    errors: list[tuple[Path, Exception]] = []
    for path in paths:
        try:
            rows.append(_read_settlement_file(path, known_stores))
        except Exception as exc:
            errors.append((path, exc))
    if errors:
        sample = ", ".join(str(path) for path, _ in errors[:3])
        raise RuntimeError(f"땡겨요 정산 원천 파일 {len(errors)}개 읽기 실패; sample={sample}") from errors[0][1]

    result, unmapped = _build_monthly_result(rows, known_stores, start_ym=start_ym, end_ym=end_ym)
    baseline = _build_monthly_baseline(rows, known_stores, start_ym=start_ym, end_ym=end_ym)
    _write_csv_atomic(result, output_path)
    _write_csv_atomic(baseline, baseline_path)
    if not unmapped.empty:
        _write_csv_atomic(unmapped, unmapped_path)
        logger.warning("땡겨요 매장명 미확정 파일 %d개: %s", len(unmapped), unmapped_path)
    elif unmapped_path.exists():
        unmapped_path.unlink()

    logger.info("ddangyo fee ratio monthly 저장 완료: %s rows=%s", output_path, len(result))
    return (
        f"ddangyo_fee_ratio_monthly {len(result)}행 -> {output_path} | "
        f"baseline {len(baseline)}행 -> {baseline_path}"
    )
