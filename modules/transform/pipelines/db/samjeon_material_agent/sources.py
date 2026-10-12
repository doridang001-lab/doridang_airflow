"""읽기 전용 원천 로더. 정규화한 행과 물리적 출처를 함께 보존한다."""
from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db.samjeon_material_agent.contracts import (
    DAM_STORE, FOOD_MAIN, FOOD_MEAL, STORE, PURCHASE_COLUMNS, SALES_COLUMNS,
    SALES_LINE_COLUMNS, code, digest, json_bytes, product_key, text, won,
)


def file_entry(path: Path, source: str, file_date: str) -> dict:
    return {"source": source, "date": file_date, "path": str(path),
            "source_ref": f"{source}/{path.name}", "sha256": digest(path) if path.is_file() else ""}


def read_checked(entry: dict, required: list[str]) -> pd.DataFrame:
    path = Path(entry["path"])
    if not entry["sha256"]:
        raise FileNotFoundError(path)
    if not path.is_file() or digest(path) != entry["sha256"]:
        raise RuntimeError(f"읽기 전 원천 판본 변경: {path}")
    frame = pd.read_parquet(path)
    if digest(path) != entry["sha256"]:
        raise RuntimeError(f"읽는 중 원천 판본 변경: {path}")
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"{path.name}: 필수 컬럼 누락 {missing}")
    return frame


def source_date(value, label: str) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"{label}: 날짜를 읽을 수 없습니다")
    return parsed.strftime("%Y-%m-%d")


def lineage(entry: dict, row: int) -> dict:
    ref = entry["source_ref"]
    return {"source_ref": ref, "source_file": entry["path"], "source_sha256": entry["sha256"],
            "source_row": row,
            "evidence_id": hashlib.sha256(json_bytes([ref, entry["sha256"], row])).hexdigest()[:24]}


def load_sales_lines(plan: dict) -> tuple[pd.DataFrame, dict[str, int]]:
    records, counts, seen = [], {}, {}
    for entry in plan["sales_files"]:
        if not entry["sha256"]:
            continue
        frame = read_checked(entry, ["sale_date", "store", "source", "total_price", "_pk"])
        counts[entry["date"]] = 0
        # 행 번호는 필터 전 물리 위치이며 취소·반품도 원천 부호대로 보존한다.
        for position, row in enumerate(frame.to_dict("records")):
            if text(row["store"]) != STORE:
                continue
            day = source_date(row["sale_date"], "매출 sale_date")
            if day != entry["date"]:
                raise ValueError(f"{entry['path']}: 파일 날짜와 삼전점 sale_date 불일치")
            key, source = text(row["_pk"]), text(row["source"])
            if not key or not source:
                raise ValueError("삼전점 _pk 또는 source 누락")
            content = {str(k): text(v) for k, v in row.items()}
            content_hash = hashlib.sha256(json_bytes(content)).hexdigest()
            if key in seen:
                if seen[key] != content_hash:
                    raise ValueError("동일 _pk의 상충 매출 행")
                continue
            seen[key] = content_hash
            counts[day] += 1
            records.append({"sale_date": day, "source": source, "_pk": key,
                            "amount_won": won(row["total_price"], label="매출"),
                            "row_content_hash": content_hash, **lineage(entry, position)})
    return pd.DataFrame(records, columns=SALES_LINE_COLUMNS), counts


def aggregate_sales(lines: pd.DataFrame) -> pd.DataFrame:
    if lines.empty:
        return pd.DataFrame(columns=SALES_COLUMNS)
    return lines.groupby(["sale_date", "source"], dropna=False).agg(
        amount_won=("amount_won", "sum"), row_count=("_pk", "size")
    ).reset_index()[SALES_COLUMNS]


def load_sales(plan: dict) -> tuple[pd.DataFrame, dict[str, int]]:
    lines, counts = load_sales_lines(plan)
    return aggregate_sales(lines), counts


def load_purchases(plan: dict) -> pd.DataFrame:
    records = []
    for source, entries in (("food_guide", plan["food_files"]), ("logistics_dam", plan["dam_files"])):
        food = source == "food_guide"
        required = (["배송일", "사업장", "상품코드", "상품명", "규격", "단위", "수량", "합계"] if food else
                    ["sale_date", "배송일자", "거래처명", "코드", "품명", "규격", "단위", "수량", "금액", "마감됨"])
        for entry in entries:
            if not entry["sha256"]:
                continue
            frame = read_checked(entry, required)
            for position, row in enumerate(frame.to_dict("records")):
                original = row["사업장" if food else "거래처명"]
                account = text(original)
                if account not in ({FOOD_MAIN, FOOD_MEAL} if food else {DAM_STORE}):
                    continue
                day = source_date(row["배송일" if food else "배송일자"], "구매 배송일")
                if not food and (day != source_date(row["sale_date"], "물류담 sale_date") or day != entry["date"]):
                    raise ValueError(f"{entry['path']}: 물류담 배송일자·sale_date·파일 날짜 불일치")
                if not plan["start_date"] <= day <= plan["as_of_date"]:
                    continue
                product = code(row["상품코드" if food else "코드"])
                name, spec, unit = text(row["상품명" if food else "품명"]), text(row["규격"]), text(row["단위"])
                records.append({
                    "purchase_date": day, "source": source, "account": account, "original_account": str(original),
                    "product_key": product_key([source, product, name, spec, unit]), "product_code": product,
                    "item_name": name, "spec": spec, "unit": unit, "qty": text(row["수량"]),
                    "amount_won": won(row["합계" if food else "금액"], label="구매"),
                    "category": "", "classification_status": "", **lineage(entry, position),
                    "closed": None if food else text(row["마감됨"]).lower() in {"true", "1", "yes", "y"},
                    "major_category": text(row.get("대분류")),
                })
    return pd.DataFrame(records, columns=PURCHASE_COLUMNS)


def resolve_source_ref(ref: str, *, sales_root: Path, food_root: Path, dam_root: Path) -> Path:
    source, filename = ref.split("/", 1)
    if not filename or "/" in filename or "\\" in filename or filename in {".", ".."}:
        raise ValueError("잘못된 원천 참조 경로")
    return {"sales": sales_root, "food_guide": food_root, "logistics_dam": dam_root}[source] / filename
