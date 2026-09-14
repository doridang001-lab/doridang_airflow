from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db import DB_Baemin_now_grp as now_grp


def _write_now(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


def _write_unified(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def test_build_baemin_now_grp_merges_and_dedupes(tmp_path):
    source_root = tmp_path / "metrics_now"
    output_path = tmp_path / "mart" / "Baemin_now_grp" / "baemin_now_grp.parquet"
    unified_path = tmp_path / "mart" / "unified_sales_grp" / "unified_sales_260801.parquet"
    old_path = source_root / "brand=도리당" / "store=송파삼전점" / "ym=2026-07" / "baemin_now.csv"
    new_path = source_root / "brand=도리당" / "store=송파삼전점" / "ym=2026-08" / "baemin_now.csv"
    other_path = source_root / "brand=나홀로" / "store=송파삼전점" / "ym=2026-08" / "baemin_now.csv"
    missing_sales_path = source_root / "brand=도리당" / "store=미매칭점" / "ym=2026-08" / "baemin_now.csv"

    _write_now(
        old_path,
        [
            {
                "date": "2026-08-01",
                "collected_at": "2026-08-01T00:10:00Z",
                "store_id": "1",
                "store_name": "[음식배달] 닭도리탕 전문 도리당 송파삼전점",
                "최근별점": "4.7",
                "collection_status": "ok",
            }
        ],
    )
    _write_now(
        new_path,
        [
            {
                "date": "2026-08-01",
                "collected_at": "2026-08-01T00:20:00Z",
                "store_id": "1",
                "store_name": "[음식배달] 닭도리탕 전문 도리당 송파삼전점",
                "최근별점": "4.9",
                "준비시간정확도_상태": "개선해요",
            }
        ],
    )
    _write_now(
        other_path,
        [
            {
                "date": "2026-08-01",
                "collected_at": "2026-08-02T00:20:00Z",
                "store_id": "2",
                "store_name": "[음식배달] 닭도리탕 전문 나홀로 송파삼전점",
                "최근별점": "null",
            }
        ],
    )
    _write_now(
        missing_sales_path,
        [
            {
                "date": "2026-08-03",
                "collected_at": "2026-08-03T00:20:00Z",
                "store_id": "3",
                "store_name": "[음식배달] 닭도리탕 전문 도리당 미매칭점",
                "최근별점": "4.1",
            }
        ],
    )
    unified_file = _write_unified(
        unified_path,
        [
            {"sale_date": "2026-08-01", "store": "송파삼전점", "total_price": "10000"},
            {"sale_date": "2026-08-01", "store": "송파삼전점", "total_price": "25,000"},
            {"sale_date": "2026-08-01", "store": "다른점", "total_price": "999999"},
        ],
    )

    message = now_grp.build_baemin_now_grp(
        source_root=source_root,
        output_path=output_path,
        unified_sales_files=[unified_file],
    )
    result = pd.read_parquet(output_path).fillna("").astype(str)

    assert "files=4" in message
    assert len(result) == 3
    latest = result[result["brand_store"].eq("도리당|송파삼전점")].iloc[0]
    assert latest["최근별점"] == "4.9"
    assert latest["sales_total"] == "35000"
    assert "collection_status" not in result.columns
    assert result[result["brand_store"].eq("나홀로|송파삼전점")]["최근별점"].iloc[0] == ""
    assert result[result["brand_store"].eq("나홀로|송파삼전점")]["sales_total"].iloc[0] == "35000"
    assert result[result["store"].eq("미매칭점")]["sales_total"].iloc[0] == "0"
    assert result.columns[-4] == "sales_total"
    assert list(result.columns[-3:]) == ["brand", "store", "brand_store"]
