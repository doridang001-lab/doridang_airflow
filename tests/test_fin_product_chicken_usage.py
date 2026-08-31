import logging

import pandas as pd

from modules.transform.pipelines.db import DB_FinProduct_Map as M


def _review_row(item_id: str = "9001", **overrides) -> dict[str, str]:
    row = {
        "item_id": item_id,
        "item_key": f"상품{item_id}",
        "store": "송파삼전점",
        "source": "posfeed",
        "brand": "도리당",
        "item_name": f"상품{item_id}",
        "unitprice": "12000",
        "표준_메뉴명_edit": f"상품{item_id}",
        "수동분류_edit": "메인",
        M.DUP_LABEL_COLUMN: "N",
        M.REVIEW_STATUS_COLUMN: M.REVIEW_PENDING,
        "검수사유": "LLM 분류 확인",
    }
    row.update(overrides)
    return row


def _patch_review_path(monkeypatch, path):
    monkeypatch.setattr(M, "FIN_PRODUCT_MAP_REVIEW_CSV_PATH", path)
    monkeypatch.setattr(M, "existing_fin_product_map_review_csv_path", lambda: path)


def test_review_schema_adds_manual_chicken_columns_at_end():
    assert M.REVIEW_COLUMNS[-3:] == M.MANUAL_CHICKEN_COLUMNS


def test_manual_chicken_columns_are_isolated_from_llm_map_columns():
    exposed = [
        col
        for col in M.MANUAL_CHICKEN_COLUMNS
        if col in M.MAP_COLUMNS + M.JOIN_COLUMNS + M.RECENTLY_COLUMNS
    ]

    assert exposed == []


def test_chicken_usage_candidate_columns_are_isolated_from_persisted_schemas():
    exposed = [
        col
        for col in M.CHICKEN_USAGE_CANDIDATE_COLUMNS
        if col in M.MAP_COLUMNS + M.JOIN_COLUMNS + M.RECENTLY_COLUMNS + M.REVIEW_COLUMNS
    ]

    assert exposed == ["store", "source", "brand", "item_id", "item_name"]


def test_suggest_chicken_usage_candidates_returns_llm_review_candidates(monkeypatch):
    items = [
        _review_row(
            "9001",
            item_name="[한우 대창] 순살 곱도리탕",
            대표메뉴="[한우 대창] 순살 곱도리탕",
            표준_메뉴명_edit="[한우 대창] 순살 곱도리탕",
            수동분류_edit="메인",
        )
    ]

    monkeypatch.setattr(
        M,
        "call_chicken_usage_llm",
        lambda prompt: [
            {
                "item_name": "[한우 대창] 순살 곱도리탕",
                "닭유형_candidate": "순살",
                "사이즈_candidate": "2인",
                "닭사용량_candidate": "0.6",
                "닭분류_근거": "순살과 2인 메뉴명",
                "닭분류_confidence": "0.9",
            }
        ],
    )

    result = M.suggest_chicken_usage_candidates(items)

    assert result == [
        {
            "store": "송파삼전점",
            "source": "posfeed",
            "brand": "도리당",
            "item_id": "9001",
            "item_name": "[한우 대창] 순살 곱도리탕",
            "닭유형_candidate": "순살",
            "사이즈_candidate": "2인",
            "닭사용량_candidate": "0.6",
            "닭분류_근거": "순살과 2인 메뉴명",
            "닭분류_confidence": "0.9",
        }
    ]


def test_suggest_chicken_usage_candidates_falls_back_to_blank_for_invalid_llm(monkeypatch):
    items = [_review_row("9001", item_name="제로콜라", 수동분류_edit="음료")]
    monkeypatch.setattr(
        M,
        "call_chicken_usage_llm",
        lambda prompt: [
            {
                "item_name": "제로콜라",
                "닭유형_candidate": "치킨",
                "사이즈_candidate": "특대",
                "닭사용량_candidate": "9.9",
            }
        ],
    )

    result = M.suggest_chicken_usage_candidates(items)

    assert result[0]["닭유형_candidate"] == ""
    assert result[0]["사이즈_candidate"] == ""
    assert result[0]["닭사용량_candidate"] == ""


def test_write_review_map_preserves_existing_manual_chicken_values(tmp_path, monkeypatch):
    review_path = tmp_path / "fin_product_map_review_input.csv"
    _patch_review_path(monkeypatch, review_path)
    pd.DataFrame([
        _review_row("9001", 닭유형_manual="뼈닭", 사이즈_manual="대", 닭사용량_manual="1.5")
    ]).reindex(columns=M.REVIEW_COLUMNS, fill_value="").to_csv(
        review_path,
        index=False,
        encoding="utf-8-sig",
    )

    M.write_review_map(pd.DataFrame([_review_row("9001")]).reindex(columns=M.REVIEW_INTERNAL_COLUMNS, fill_value=""))

    result = pd.read_csv(review_path, dtype=str, encoding="utf-8-sig").fillna("")
    row = result.iloc[0]
    assert row["닭유형_manual"] == "뼈닭"
    assert row["사이즈_manual"] == "대"
    assert row["닭사용량_manual"] == "1.5"
    backups = list((tmp_path / "_backup").glob("chicken_usage_*.csv"))
    assert len(backups) == 1


def test_write_review_map_keeps_new_rows_manual_chicken_blank(tmp_path, monkeypatch):
    review_path = tmp_path / "fin_product_map_review_input.csv"
    _patch_review_path(monkeypatch, review_path)
    pd.DataFrame([
        _review_row("9001", 닭유형_manual="순살", 사이즈_manual="소", 닭사용량_manual="0.4")
    ]).reindex(columns=M.REVIEW_COLUMNS, fill_value="").to_csv(
        review_path,
        index=False,
        encoding="utf-8-sig",
    )
    review_df = pd.DataFrame([
        _review_row("9001"),
        _review_row("9002"),
    ]).reindex(columns=M.REVIEW_INTERNAL_COLUMNS, fill_value="")

    M.write_review_map(review_df)

    result = pd.read_csv(review_path, dtype=str, encoding="utf-8-sig").fillna("").set_index("item_id")
    assert result.loc["9001", "닭유형_manual"] == "순살"
    assert result.loc["9002", "닭유형_manual"] == ""
    assert result.loc["9002", "사이즈_manual"] == ""
    assert result.loc["9002", "닭사용량_manual"] == ""


def test_validate_chicken_usage_warns_for_conversion_table_mismatch(caplog):
    df = pd.DataFrame([
        _review_row("9001", 닭유형_manual="뼈닭", 사이즈_manual="대", 닭사용량_manual="0.4")
    ])

    with caplog.at_level(logging.WARNING):
        warning_count = M._validate_chicken_usage(df)

    assert warning_count == 1
    assert "환산표 불일치" in caplog.text
    assert df.iloc[0]["닭사용량_manual"] == "0.4"


def test_validate_chicken_usage_warns_for_partial_input(caplog):
    df = pd.DataFrame([
        _review_row("9001", 닭유형_manual="뼈닭", 사이즈_manual="", 닭사용량_manual="")
    ])

    with caplog.at_level(logging.WARNING):
        warning_count = M._validate_chicken_usage(df)

    assert warning_count == 1
    assert "부분 입력" in caplog.text


def test_load_review_map_accepts_legacy_schema_without_manual_chicken_columns(tmp_path, monkeypatch):
    review_path = tmp_path / "fin_product_map_review_input.csv"
    _patch_review_path(monkeypatch, review_path)
    legacy_columns = [col for col in M.REVIEW_COLUMNS if col not in M.MANUAL_CHICKEN_COLUMNS]
    pd.DataFrame([_review_row("9001")]).reindex(columns=legacy_columns, fill_value="").to_csv(
        review_path,
        index=False,
        encoding="utf-8-sig",
    )

    result = M.load_review_map()

    for col in M.MANUAL_CHICKEN_COLUMNS:
        assert col in result.columns
        assert result.iloc[0][col] == ""
