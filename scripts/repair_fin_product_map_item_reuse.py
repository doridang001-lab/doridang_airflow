"""fin_product_map의 item_id 재사용(실제 상품 변경) 오염 행 1회성 복구.

배경 (2026-09-11): scan_target_items()가 item_key를 item_name과 독립적으로
최빈값 집계하던 버그로 인해, 쿠팡 상품코드가 재사용된 아래 5개 행이
전혀 무관한 예전 상품(소주/음료)의 표준_메뉴명_edit/수동분류_edit를 그대로
물려받고 있었다 (예: item_id=300004078의 실제 item_name은
"[1인] 순살 닭도리탕 (밥포함) 1인분"인데 표준_메뉴명_edit="참이슬 후레쉬",
수동분류_edit="음료"로 남아있었음). 코드 자체의 재발 방지책은
DB_FinProduct_Map.py의 _fill_item_identity_columns()/scan_target_items()/
_reset_reused_item_classifications()에 반영했고, 이 스크립트는 이미
오염되어 디스크에 저장된 값만 1회성으로 초기화한다.

기본은 dry-run이며, --apply를 줘야 실제로 파일을 덮어쓴다.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime, date
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.transform.pipelines.db.DB_ItemIdAllocator import normalize_item_key  # noqa: E402
from modules.transform.utility.paths import (  # noqa: E402
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_JOIN_CSV_PATH,
    FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
)
from scripts._base import run_script  # noqa: E402

# (store, source, brand, item_id) - item_name이 실제로는 무관한 상품인데
# 표준_메뉴명_edit/수동분류_edit가 예전(재사용 전) 상품 값으로 남아있던 행.
TARGETS: set[tuple[str, str, str, str]] = {
    ("송파삼전점", "쿠팡수동", "도리당", "300004078"),  # 실제: [1인] 순살 닭도리탕(밥포함) 1인분
    ("송파삼전점", "쿠팡수동", "도리당", "300004076"),  # 실제: [들깨] 우거지 닭도리탕
    ("송파삼전점", "쿠팡수동", "도리당", "300004083"),  # 실제: [매운辛]실비 파김치 곱도리탕 + 계란찜
    ("송파삼전점", "쿠팡수동", "도리당", "300004080"),  # 실제: [한그릇] 누룽지 1인 순살 나만의 백도리당
    ("송파삼전점", "쿠팡수동", "도리당", "300004094"),  # 실제: [단짠단짠] 순살 갈비찜닭
}
KEY_COLUMNS = ("store", "source", "brand", "item_id")
TODAY = str(date.today())


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna("")


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    tmp = path.with_suffix(".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        try:
            tmp.replace(path)
        except (PermissionError, OSError):
            path.write_bytes(tmp.read_bytes())
    finally:
        tmp.unlink(missing_ok=True)


def _backup(path: Path) -> Path:
    backup_dir = path.parent / "_backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / f"{path.stem}.item_reuse_repair_bak_{_timestamp()}{path.suffix}"
    shutil.copy2(path, backup)
    return backup


def _key_mask(df: pd.DataFrame) -> pd.Series:
    for col in KEY_COLUMNS:
        if col not in df.columns:
            return pd.Series([False] * len(df), index=df.index)
    keys = list(zip(
        df["store"].fillna("").astype(str).str.strip(),
        df["source"].fillna("").astype(str).str.strip(),
        df["brand"].fillna("").astype(str).str.strip(),
        df["item_id"].fillna("").astype(str).str.strip(),
    ))
    return pd.Series([k in TARGETS for k in keys], index=df.index)


def _repair_map(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    result = df.copy()
    mask = _key_mask(result)
    changes = []
    for idx in result.index[mask]:
        row = result.loc[idx]
        changes.append({
            "item_id": row.get("item_id", ""),
            "item_name": row.get("item_name", ""),
            "old_표준_메뉴명_edit": row.get("표준_메뉴명_edit", ""),
            "old_수동분류_edit": row.get("수동분류_edit", ""),
        })
    result.loc[mask, "표준_메뉴명_edit"] = ""
    result.loc[mask, "수동분류_edit"] = ""
    if "classified_by" in result.columns:
        result.loc[mask, "classified_by"] = ""
    if "review_status_edit" in result.columns:
        result.loc[mask, "review_status_edit"] = "0"
    if "item_key" in result.columns and "item_name" in result.columns:
        result.loc[mask, "item_key"] = result.loc[mask, "item_name"].map(normalize_item_key)
    if "updated_at" in result.columns:
        result.loc[mask, "updated_at"] = TODAY
    return result, changes


def _repair_review_input(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    mask = _key_mask(result)
    result.loc[mask, "표준_메뉴명_edit"] = ""
    result.loc[mask, "수동분류_edit"] = ""
    if "검수유무" in result.columns:
        result.loc[mask, "검수유무"] = "0"
    if "검수사유" in result.columns:
        result.loc[mask, "검수사유"] = "item_id 재사용으로 상품 변경 - 재분류 필요"
    if "중복_수동분류" in result.columns:
        result.loc[mask, "중복_수동분류"] = "N"
    if "형제분류_불일치" in result.columns:
        result.loc[mask, "형제분류_불일치"] = "N"
    if "item_key" in result.columns and "item_name" in result.columns:
        result.loc[mask, "item_key"] = result.loc[mask, "item_name"].map(normalize_item_key)
    return result


def _repair_join(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    mask = _key_mask(df)
    removed = int(mask.sum())
    return df[~mask].reset_index(drop=True), removed


def main() -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="실제로 파일을 덮어쓴다 (기본은 dry-run)")
    args = parser.parse_args()

    result: dict = {"dry_run": not args.apply, "targets": sorted(TARGETS)}

    map_df = _read_csv(FIN_PRODUCT_MAP_CSV_PATH)
    review_df = _read_csv(FIN_PRODUCT_MAP_REVIEW_CSV_PATH)
    join_df = _read_csv(FIN_PRODUCT_MAP_JOIN_CSV_PATH)

    new_map, changes = _repair_map(map_df)
    new_review = _repair_review_input(review_df)
    new_join, join_removed = _repair_join(join_df)

    result["map_rows_changed"] = len(changes)
    result["map_changes"] = changes
    result["review_rows_changed"] = int(_key_mask(review_df).sum())
    result["join_rows_removed"] = join_removed

    if not args.apply:
        print(f"[dry-run] map.csv 변경 예정: {len(changes)}행")
        for c in changes:
            print(f"  - {c['item_id']} | {c['item_name']!r} : "
                  f"{c['old_표준_메뉴명_edit']!r}/{c['old_수동분류_edit']!r} -> ''/''")
        print(f"[dry-run] review_input.csv 변경 예정: {result['review_rows_changed']}행")
        print(f"[dry-run] join.csv 제거 예정: {join_removed}행")
        print("--apply 없이 실행했으므로 파일은 수정하지 않았습니다.")
        return result

    result["backups"] = [
        str(_backup(FIN_PRODUCT_MAP_CSV_PATH)),
        str(_backup(FIN_PRODUCT_MAP_REVIEW_CSV_PATH)),
        str(_backup(FIN_PRODUCT_MAP_JOIN_CSV_PATH)),
    ]
    _write_csv(FIN_PRODUCT_MAP_CSV_PATH, new_map)
    _write_csv(FIN_PRODUCT_MAP_REVIEW_CSV_PATH, new_review)
    _write_csv(FIN_PRODUCT_MAP_JOIN_CSV_PATH, new_join)
    print(f"완료: map {len(changes)}행 / review {result['review_rows_changed']}행 초기화, "
          f"join {join_removed}행 제거")
    return result


if __name__ == "__main__":
    run_script(main, "repair_fin_product_map_item_reuse")
