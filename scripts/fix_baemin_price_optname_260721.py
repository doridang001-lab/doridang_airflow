"""배민 가격형 주문옵션상세를 교정하고 영향 날짜 UnifiedSales를 재생성한다.

기본 모드는 dry-run이다. ``--apply``를 지정한 경우에만 원본과 후속 데이터를
백업한 뒤 수정한다.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.transform.pipelines.db.DB_ItemIdAllocator import (  # noqa: E402
    allocate_manual_item_ids,
    canonical_source,
    normalize_item_key,
)
from modules.transform.pipelines.db.DB_UnifiedSales_baemin import (  # noqa: E402
    BAEMIN_SOURCE,
    reconcile_baemin_for_test_stores,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (  # noqa: E402
    UNIFIED_ROOT,
    _unified_daily_path,
    iter_unified_sales_files,
)
from modules.transform.utility.paths import (  # noqa: E402
    BAEMIN_ORDERS_DB,
    FIN_PRODUCT_CSV_PATH,
    LOCAL_DB,
)

logger = logging.getLogger(__name__)

TARGET_YM = "2026-07"
EXPECTED_BAD_ROWS = 34
TARGET_STORE_DATES = {
    "송파삼전점": (
        "2026-07-08",
        "2026-07-09",
        "2026-07-10",
        "2026-07-12",
        "2026-07-15",
        "2026-07-17",
        "2026-07-19",
    ),
    "전주전북대점": ("2026-07-12", "2026-07-14", "2026-07-15"),
    "중랑면목점": ("2026-07-12", "2026-07-15", "2026-07-18"),
    "강원영월점": ("2026-07-11", "2026-07-15"),
}
TARGET_STORES = tuple(TARGET_STORE_DATES)
OPTION_COLUMN = "주문옵션상세"
MENU_COLUMN = "주문내역"
MONEY_COLUMNS = ("주문옵션금액", "결제금액")
MENU_SUFFIX_RE = re.compile(r"\s*외\s*\d+건$")


def _price_mask(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.fullmatch(r"[\d,]+")


def _partition_value(path: Path, prefix: str) -> str:
    for part in path.parts:
        if part.startswith(prefix):
            return part.split("=", 1)[1]
    return ""


def _target_files(root: Path | None = None) -> list[Path]:
    root = root or BAEMIN_ORDERS_DB
    files: list[Path] = []
    for store in TARGET_STORES:
        files.extend(
            root.glob(
                f"brand=*/store={store}/ym={TARGET_YM}/orders_{TARGET_YM}.parquet"
            )
        )
    return sorted(set(files))


def _correct_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    missing = {OPTION_COLUMN, MENU_COLUMN, *MONEY_COLUMNS} - set(df.columns)
    if missing:
        raise KeyError(f"배민 원본 필수 컬럼 누락: {sorted(missing)}")

    out = df.copy()
    mask = _price_mask(out[OPTION_COLUMN])
    count = int(mask.sum())
    if count == 0:
        return out, 0

    menu = (
        out[MENU_COLUMN]
        .fillna("")
        .astype(str)
        .str.replace(MENU_SUFFIX_RE, "", regex=True)
        .str.strip()
    )
    invalid_menu = menu.eq("") | menu.str.fullmatch(r"[\d,]+")
    if bool((mask & invalid_menu).any()):
        rows = out.index[mask & invalid_menu].tolist()
        raise ValueError(f"가격형 옵션 교정용 메뉴명이 유효하지 않음: rows={rows}")

    out.loc[mask, OPTION_COLUMN] = menu.loc[mask]
    pd.testing.assert_frame_equal(
        df.drop(columns=[OPTION_COLUMN]),
        out.drop(columns=[OPTION_COLUMN]),
        check_dtype=True,
        check_exact=True,
    )
    for column in MONEY_COLUMNS:
        pd.testing.assert_series_equal(df[column], out[column], check_exact=True)
    return out, count


def _scan_raw(root: Path | None = None) -> dict:
    root = root or BAEMIN_ORDERS_DB
    files: list[dict] = []
    allocation_rows: list[dict[str, str]] = []
    corrected_keys: set[tuple[str, str, str, str]] = set()
    total = 0
    by_store = {store: 0 for store in TARGET_STORES}

    for path in _target_files(root):
        df = pd.read_parquet(path)
        mask = _price_mask(df[OPTION_COLUMN])
        count = int(mask.sum())
        store = _partition_value(path, "store=")
        brand = _partition_value(path, "brand=")
        by_store[store] = by_store.get(store, 0) + count
        total += count

        if count:
            menu = (
                df.loc[mask, MENU_COLUMN]
                .fillna("")
                .astype(str)
                .str.replace(MENU_SUFFIX_RE, "", regex=True)
                .str.strip()
            )
            corrected_keys.update(
                (
                    canonical_source(BAEMIN_SOURCE),
                    brand,
                    store,
                    normalize_item_key(item_name),
                )
                for item_name in menu
            )
            allocation_rows.extend(
                {
                    "source": BAEMIN_SOURCE,
                    "brand": brand,
                    "store": store,
                    "item_name": item_name,
                    "unit_price": str(df.at[index, "주문옵션금액"] or ""),
                }
                for index, item_name in menu.items()
            )

        files.append({"path": path, "store": store, "brand": brand, "bad_rows": count})

    return {
        "files": files,
        "bad_rows": total,
        "by_store": by_store,
        "corrected_keys": corrected_keys,
        "allocation_rows": allocation_rows,
    }


def _count_unified_numeric() -> tuple[int, dict[str, int]]:
    total = 0
    by_store = {store: 0 for store in TARGET_STORES}
    for path in iter_unified_sales_files():
        df = pd.read_parquet(path, columns=["source", "store", "item_name"])
        mask = (
            df["source"].fillna("").astype(str).str.strip().eq(BAEMIN_SOURCE)
            & _price_mask(df["item_name"])
        )
        total += int(mask.sum())
        if mask.any():
            counts = df.loc[mask].groupby("store").size()
            for store, count in counts.items():
                by_store[str(store)] = by_store.get(str(store), 0) + int(count)
    return total, by_store


def _affected_pairs() -> list[tuple[str, str]]:
    return [
        (store, sale_date)
        for store, dates in TARGET_STORE_DATES.items()
        for sale_date in dates
    ]


def _pair_amounts() -> dict[tuple[str, str], int]:
    amounts: dict[tuple[str, str], int] = {}
    for store, sale_date in _affected_pairs():
        path = _unified_daily_path(sale_date)
        if not path.exists():
            amounts[(store, sale_date)] = 0
            continue
        df = pd.read_parquet(
            path,
            columns=["sale_date", "store", "source", "total_price"],
        )
        mask = (
            df["sale_date"].fillna("").astype(str).str.strip().eq(sale_date)
            & df["store"].fillna("").astype(str).str.strip().eq(store)
            & df["source"].fillna("").astype(str).str.strip().eq(BAEMIN_SOURCE)
        )
        amounts[(store, sale_date)] = int(
            pd.to_numeric(df.loc[mask, "total_price"], errors="coerce").fillna(0).sum()
        )
    return amounts


def _read_master() -> pd.DataFrame:
    if not FIN_PRODUCT_CSV_PATH.exists():
        return pd.DataFrame()
    return pd.read_csv(FIN_PRODUCT_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")


def _master_keys(df: pd.DataFrame) -> set[tuple[str, str, str, str]]:
    if df.empty:
        return set()
    required = {"source", "brand", "store"}
    if not required.issubset(df.columns):
        return set()
    item_key = (
        df["item_key"]
        if "item_key" in df.columns
        else df.get("상품명", pd.Series("", index=df.index))
    )
    return set(
        zip(
            df["source"].map(canonical_source),
            df["brand"].astype(str).str.strip(),
            df["store"].astype(str).str.strip(),
            item_key.astype(str).map(normalize_item_key),
        )
    )


def _backup_paths(
    paths: list[Path],
    run_id: str,
    backup_root: Path | None = None,
) -> dict[Path, Path | None]:
    backup_root = backup_root or LOCAL_DB / "temp" / "fix_baemin_price_optname" / run_id
    backups: dict[Path, Path | None] = {}
    for index, path in enumerate(dict.fromkeys(paths)):
        if not path.exists():
            backups[path] = None
            continue
        backup = backup_root / f"{index:04d}_{path.name}.bak"
        if backup.exists():
            raise FileExistsError(f"백업 파일이 이미 존재함: {backup}")
        backup.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, backup)
        backups[path] = backup
        logger.info("백업 생성: %s", backup)
    return backups


def _cleanup_backup_root(backup_root: Path) -> None:
    if backup_root.exists():
        shutil.rmtree(backup_root)
        logger.info("로컬 임시 백업 정리: %s", backup_root)


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _restore_backups(backups: dict[Path, Path | None]) -> list[str]:
    errors: list[str] = []
    for path, backup in backups.items():
        try:
            if backup is None:
                path.unlink(missing_ok=True)
            elif path.exists() and _file_digest(path) == _file_digest(backup):
                logger.info("롤백 생략, 백업과 동일: %s", path)
                continue
            else:
                shutil.copy2(backup, path)
            logger.warning("롤백 복원: %s", path)
        except Exception as exc:
            errors.append(f"{path}: {exc}")
            logger.exception("롤백 복원 실패: %s", path)
    return errors


def _write_parquet_atomic(df: pd.DataFrame, path: Path, run_id: str) -> None:
    temp = path.with_name(f"{path.name}.{run_id}.tmp")
    try:
        df.to_parquet(temp, index=False, engine="pyarrow")
        check = pd.read_parquet(temp)
        pd.testing.assert_frame_equal(df.reset_index(drop=True), check.reset_index(drop=True))
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _apply_raw(scan: dict, run_id: str) -> int:
    changed = 0
    for entry in scan["files"]:
        if entry["bad_rows"] == 0:
            continue
        path = entry["path"]
        before = pd.read_parquet(path)
        after, count = _correct_frame(before)
        _write_parquet_atomic(after, path, run_id)
        changed += count
        logger.info("원본 교정: rows=%d path=%s", count, path)
    return changed


def _allocate_corrected_items(scan: dict) -> None:
    rows = pd.DataFrame(scan["allocation_rows"])
    if rows.empty:
        return
    rows = rows.drop_duplicates(
        subset=["source", "brand", "store", "item_name"],
        keep="last",
    ).reset_index(drop=True)
    allocate_manual_item_ids(rows, persist=True)

    missing = scan["corrected_keys"] - _master_keys(_read_master())
    if missing:
        raise RuntimeError(f"상품 ID 마스터 사전 반영 실패: {sorted(missing)}")
    logger.info("상품 ID 마스터 사전 반영 완료: keys=%d", len(rows))


def _reconcile_affected_pairs() -> None:
    for store, sale_date in _affected_pairs():
        result = reconcile_baemin_for_test_stores(
            stores=[store],
            sale_date=sale_date,
            lookback_days=None,
        )
        logger.info("UnifiedSales 재생성: store=%s date=%s result=%s", store, sale_date, result)


def run(*, apply: bool) -> dict:
    raw_before = _scan_raw()
    unified_before, unified_by_store = _count_unified_numeric()
    logger.info(
        "교정 사전 점검: raw=%d raw_by_store=%s unified=%d unified_by_store=%s",
        raw_before["bad_rows"],
        raw_before["by_store"],
        unified_before,
        unified_by_store,
    )

    if raw_before["bad_rows"] not in (0, EXPECTED_BAD_ROWS):
        raise RuntimeError(
            f"예상하지 못한 원본 오염 건수: expected=0 또는 {EXPECTED_BAD_ROWS} "
            f"actual={raw_before['bad_rows']}"
        )
    if not apply:
        return {
            "mode": "dry-run",
            "raw_bad_rows": raw_before["bad_rows"],
            "unified_bad_rows": unified_before,
            "raw_by_store": raw_before["by_store"],
        }
    if raw_before["bad_rows"] == 0 and unified_before == 0:
        logger.info("교정 대상 없음: 이미 정상 상태")
        return {"mode": "apply", "changed_rows": 0, "new_master_rows": 0}

    amounts_before = _pair_amounts()
    master_before = _read_master()
    master_keys_before = _master_keys(master_before)
    expected_new_keys = raw_before["corrected_keys"] - master_keys_before
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    raw_targets = [entry["path"] for entry in raw_before["files"] if entry["bad_rows"]]
    unified_targets = sorted({_unified_daily_path(date) for _, date in _affected_pairs()})
    backup_targets = [*raw_targets, *unified_targets, FIN_PRODUCT_CSV_PATH]
    pending_before = set(FIN_PRODUCT_CSV_PATH.parent.glob(f"{FIN_PRODUCT_CSV_PATH.name}.pending_*"))
    backup_root = LOCAL_DB / "temp" / "fix_baemin_price_optname" / run_id
    backups = _backup_paths(backup_targets, run_id, backup_root)

    try:
        _allocate_corrected_items(raw_before)
        changed_rows = _apply_raw(raw_before, run_id)
        _reconcile_affected_pairs()

        raw_after = _scan_raw()
        unified_after, _ = _count_unified_numeric()
        amounts_after = _pair_amounts()
        master_after = _read_master()
        master_keys_after = _master_keys(master_after)

        if raw_after["bad_rows"] != 0:
            raise RuntimeError(f"원본 가격형 옵션 잔존: {raw_after['bad_rows']}행")
        if unified_after != 0:
            raise RuntimeError(f"UnifiedSales 가격형 item_name 잔존: {unified_after}행")
        if amounts_after != amounts_before:
            changed = {
                key: (amounts_before[key], amounts_after[key])
                for key in amounts_before
                if amounts_before[key] != amounts_after[key]
            }
            raise RuntimeError(f"대상 매출 합계 변경 감지: {changed}")
        missing_keys = raw_before["corrected_keys"] - master_keys_after
        if missing_keys:
            raise RuntimeError(f"상품 ID 마스터 키 누락: {sorted(missing_keys)}")

        new_master_rows = len(master_after) - len(master_before)
        if raw_before["bad_rows"] == EXPECTED_BAD_ROWS and new_master_rows != len(expected_new_keys):
            raise RuntimeError(
                "상품 ID 마스터 신규 행 불일치: "
                f"expected={len(expected_new_keys)} actual={new_master_rows}"
            )
        logger.info(
            "교정 완료: raw_fixed=%d unified_bad=0 new_master_rows=%d backups=%d",
            changed_rows,
            new_master_rows,
            len(backups),
        )
        _cleanup_backup_root(backup_root)
        return {
            "mode": "apply",
            "changed_rows": changed_rows,
            "new_master_rows": new_master_rows,
            "backup_count": len(backups),
        }
    except Exception:
        logger.exception("교정 실패, 백업에서 복원합니다")
        rollback_errors = _restore_backups(backups)
        try:
            pending_after = set(
                FIN_PRODUCT_CSV_PATH.parent.glob(f"{FIN_PRODUCT_CSV_PATH.name}.pending_*")
            )
            for pending in pending_after - pending_before:
                pending.unlink(missing_ok=True)
        except Exception as exc:
            rollback_errors.append(f"pending 정리: {exc}")
            logger.exception("신규 pending 파일 정리 실패")
        if rollback_errors:
            logger.error("롤백 미완료 항목: %s", rollback_errors)
        else:
            _cleanup_backup_root(backup_root)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="배민 가격형 주문옵션상세와 영향 UnifiedSales를 교정합니다."
    )
    parser.add_argument("--apply", action="store_true", help="백업 후 실제 데이터를 수정합니다.")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    args = parse_args()
    run(apply=args.apply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
