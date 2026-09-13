from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db import DB_DdangyoFee as ddangyo


def _row(path: str, ym: str, store: str | None, total: int, deposit: int) -> ddangyo.DdangyoSettlementRow:
    return ddangyo.DdangyoSettlementRow(
        path=Path(path),
        ym=ym,
        store=store,
        total_amt=total,
        deposit_amt=deposit,
    )


def test_find_summary_amounts_uses_header_position() -> None:
    frame = pd.DataFrame(
        [
            ["2026년09월 정산 내역", "", ""],
            ["정산정보", "입금금액", ""],
            ["(A)주문결제", "입금금액", ""],
            [100_000, 94_000, ""],
        ]
    )

    assert ddangyo._parse_ym(frame.iloc[0, 0]) == "2026_09"
    assert ddangyo._find_summary_amounts(frame) == (100_000.0, 94_000.0)


def test_build_monthly_result_sums_duplicate_store_month_before_ratio() -> None:
    result, unmapped = ddangyo._build_monthly_result(
        [
            _row("a.xls", "2026_09", "도리당 송파점", 100_000, 94_000),
            _row("b.xls", "2026_09", "도리당 송파점", 50_000, 47_500),
        ],
        ["도리당 송파점"],
        start_ym="2026_09",
        end_ym="2026_09",
    )

    assert unmapped.empty
    assert list(result.columns) == ddangyo.OUTPUT_COLUMNS
    assert len(result) == 1
    assert result.iloc[0].to_dict() == {
        "ym": "2026_09",
        "store": "도리당 송파점",
        "fee_ratio": 0.056667,
    }


def test_build_monthly_result_fills_missing_months_with_store_weighted_average() -> None:
    result, _ = ddangyo._build_monthly_result(
        [_row("a.xls", "2026_01", "도리당 송파점", 100_000, 95_000)],
        ["도리당 송파점"],
        start_ym="2026_01",
        end_ym="2026_03",
    )

    assert result["ym"].tolist() == ["2026_01", "2026_02", "2026_03"]
    assert result["fee_ratio"].tolist() == [0.05, 0.05, 0.05]


def test_build_monthly_result_includes_source_only_new_store() -> None:
    result, _ = ddangyo._build_monthly_result(
        [_row("a.xls", "2026_09", "도리당 신규점", 100_000, 90_000)],
        ["도리당 송파점"],
        start_ym="2026_09",
        end_ym="2026_09",
    )

    assert set(result["store"]) == {"도리당 송파점", "도리당 신규점"}
    keyed = result.set_index("store")
    assert keyed.loc["도리당 신규점", "fee_ratio"] == 0.1
    assert keyed.loc["도리당 송파점", "fee_ratio"] == 0.1


def test_build_monthly_result_keeps_unmapped_out_of_result() -> None:
    result, unmapped = ddangyo._build_monthly_result(
        [
            _row("mapped.xls", "2026_09", "도리당 송파점", 100_000, 95_000),
            _row("unknown.xls", "2026_09", None, 100_000, 80_000),
        ],
        ["도리당 송파점"],
        start_ym="2026_09",
        end_ym="2026_09",
    )

    assert result["store"].tolist() == ["도리당 송파점"]
    assert len(unmapped) == 1
    assert unmapped.iloc[0]["source_path"] == "unknown.xls"


def test_build_monthly_baseline_uses_month_weighted_ratio_then_global_fallback() -> None:
    baseline = ddangyo._build_monthly_baseline(
        [
            _row("a.xls", "2026_01", "도리당 송파점", 100_000, 95_000),
            _row("b.xls", "2026_01", "도리당 강동점", 300_000, 270_000),
            _row("c.xls", "2026_02", "도리당 송파점", 200_000, 190_000),
            _row("unknown.xls", "2026_02", None, 100_000, 80_000),
        ],
        ["도리당 송파점", "도리당 강동점"],
        start_ym="2026_01",
        end_ym="2026_03",
    )

    assert list(baseline.columns) == ddangyo.BASELINE_COLUMNS
    assert baseline.to_dict("records") == [
        {"ym": "2026_01", "fee_ratio": 0.0875},
        {"ym": "2026_02", "fee_ratio": 0.05},
        {"ym": "2026_03", "fee_ratio": 0.075},
    ]


def test_extract_store_from_binary_prefers_known_store(tmp_path: Path) -> None:
    path = tmp_path / "sample.xls"
    path.write_bytes("noise 닭도리탕 전문 도리당 송파점 more".encode("utf-16le"))

    assert ddangyo._extract_store_from_binary(path, ["도리당 송파점"]) == "도리당 송파점"
