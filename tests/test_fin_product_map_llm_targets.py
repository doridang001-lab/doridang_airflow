import pandas as pd

from modules.transform.pipelines.db import DB_FinProduct_Map as M


def _item(item_id: str = "9001", **overrides) -> dict[str, str]:
    row = {
        "item_id": item_id,
        "item_key": f"상품{item_id}",
        "store_seq": "411",
        "item_seq": item_id,
        "store": "송파삼전점",
        "source": "posfeed",
        "brand": "도리당",
        "item_name": f"상품{item_id}",
        "unitprice": "12000",
        "대표메뉴": "",
    }
    row.update(overrides)
    return row


def _map_row(item_id: str = "9001", **overrides) -> dict[str, str]:
    row = {
        **_item(item_id),
        "표준_메뉴명_edit": "",
        "수동분류_edit": "",
        M.REVIEW_STATUS_COLUMN: M.REVIEW_PENDING,
        "classified_by": "",
        "updated_at": M.TODAY,
    }
    row.update(overrides)
    return row


def _map_df(*rows: dict[str, str]) -> pd.DataFrame:
    return pd.DataFrame(rows).reindex(columns=M.MAP_COLUMNS, fill_value="")


def test_find_llm_targets_includes_map_only_unclassified_row():
    all_items = pd.DataFrame(columns=[
        "item_id", "item_key", "store_seq", "item_seq", "store",
        "source", "brand", "item_name", "unitprice", "대표메뉴",
    ])

    result = M.find_llm_targets(all_items, _map_df(_map_row()))

    assert result["item_id"].tolist() == ["9001"]


def test_find_llm_targets_excludes_approved_and_valid_completed_rows():
    approved = _map_row("9001", **{M.REVIEW_STATUS_COLUMN: M.REVIEW_APPROVED})
    human = _map_row(
        "9002",
        표준_메뉴명_edit="승인 상품",
        수동분류_edit="메인",
        classified_by="human",
    )
    llm = _map_row(
        "9003",
        표준_메뉴명_edit="LLM 상품",
        수동분류_edit="사이드",
        classified_by="llm",
    )

    result = M.find_llm_targets(pd.DataFrame(), _map_df(approved, human, llm))

    assert result.empty


def test_llm_product_map_classifies_map_only_when_parquet_is_empty(monkeypatch):
    pending = _map_df(_map_row())
    saved = {}
    monkeypatch.setattr(M, "scan_target_items", lambda persist_identity=True: pd.DataFrame())
    monkeypatch.setattr(M, "load_map", lambda: pending.copy())
    monkeypatch.setattr(M, "load_review_map", M._empty_review_map)
    monkeypatch.setattr(M, "load_recently_map", lambda: pd.DataFrame(columns=M.RECENTLY_COLUMNS))
    monkeypatch.setattr(M, "_load_map_examples", lambda: [])
    monkeypatch.setattr(M, "load_rules", lambda: [])
    monkeypatch.setattr(
        M,
        "call_llm",
        lambda _prompt: [{
            "item_name": "상품9001",
            "표준_메뉴명_edit": "상품9001",
            "수동분류_edit": "메인",
        }],
    )
    monkeypatch.setattr(M, "write_map", lambda df: saved.setdefault("map", df.copy()))
    monkeypatch.setattr(M, "write_review_map", lambda _df: None)
    monkeypatch.setattr(M, "write_recently_map", lambda _df: None)
    monkeypatch.setattr(M, "write_join_map", lambda _df: {"join_rows": 1, "join_conflict_keys": 0})
    monkeypatch.setattr(M, "_notify_duplicate_labels", lambda _df: None)

    summary = M.llm_product_map(dry_run=False)

    assert summary["target_rows"] == 1
    assert summary["llm_targets"] == 1
    assert summary["new_classified"] == 1
    assert saved["map"].iloc[0]["수동분류_edit"] == "메인"


def test_invalid_batch_response_retries_once_and_marks_unresolved(monkeypatch):
    calls = []

    def fake_call(_prompt):
        calls.append(1)
        if len(calls) == 1:
            return [
                {"item_name": "상품9001", "표준_메뉴명_edit": "상품9001", "수동분류_edit": "없는분류"},
                {"item_name": "상품9002", "표준_메뉴명_edit": "상품9002", "수동분류_edit": "메인"},
            ]
        return [{"item_name": "상품9001", "표준_메뉴명_edit": "상품9001", "수동분류_edit": "없는분류"}]

    monkeypatch.setattr(M, "call_llm", fake_call)

    rows = M._classify_batch([_item("9001"), _item("9002")], [], [])

    assert len(calls) == 2
    assert rows[0]["수동분류_edit"] == ""
    assert rows[0]["classified_by"] == "llm_unresolved"
    assert rows[1]["수동분류_edit"] == "메인"


def test_invalid_batch_response_uses_successful_single_retry(monkeypatch):
    calls = []

    def fake_call(_prompt):
        calls.append(1)
        if len(calls) == 1:
            return [
                {"item_name": "상품9001", "표준_메뉴명_edit": "상품9001", "수동분류_edit": "없는분류"},
                {"item_name": "상품9002", "표준_메뉴명_edit": "상품9002", "수동분류_edit": "메인"},
            ]
        return [{"item_name": "상품9001", "표준_메뉴명_edit": "상품9001", "수동분류_edit": "사이드"}]

    monkeypatch.setattr(M, "call_llm", fake_call)

    rows = M._classify_batch([_item("9001"), _item("9002")], [], [])

    assert len(calls) == 2
    assert rows[0]["수동분류_edit"] == "사이드"
    assert rows[0]["classified_by"] == "llm"


def test_migrate_preserves_map_only_row_and_old_value_when_current_is_blank(monkeypatch):
    current = pd.DataFrame([_item("9001", item_name="")])
    existing = _map_df(
        _map_row("9001", item_name="기존상품", 표준_메뉴명_edit="기존상품", 수동분류_edit="메인"),
        _map_row("9002", item_name="판매이력없음"),
    )
    captured = {}
    monkeypatch.setattr(M, "build_initial_map", lambda persist_identity=True: current.copy())
    monkeypatch.setattr(M, "load_map", lambda: existing.copy())
    monkeypatch.setattr(M, "load_review_map", M._empty_review_map)
    monkeypatch.setattr(M, "load_recently_map", lambda: pd.DataFrame(columns=M.RECENTLY_COLUMNS))

    def capture(df):
        captured["result"] = df.copy()
        return df

    monkeypatch.setattr(M, "_seed_split_rows_from_siblings", capture)

    summary = M.migrate_product_map(dry_run=True)

    assert summary["target_rows"] == 2
    assert set(captured["result"]["item_id"]) == {"9001", "9002"}
    assert captured["result"].set_index("item_id").loc["9001", "item_name"] == "기존상품"


def test_review_reason_exposes_llm_failure():
    reason = M._review_reason(pd.Series({
        "classified_by": "llm_unresolved",
        "수동분류_edit": "",
        M.REVIEW_STATUS_COLUMN: M.REVIEW_PENDING,
    }))

    assert reason == "LLM 분류 실패, 수동분류 미입력"


def test_call_llm_reuses_qwen_client_and_unwraps_items(monkeypatch):
    client = object()
    seen = {}
    monkeypatch.setattr(M, "get_ollama_client_with_candidates", lambda: (client, ["qwen2.5:14b"]))

    def fake_query(prompt, **kwargs):
        seen.update({"prompt": prompt, **kwargs})
        return {"items": [{"item_name": "상품9001"}]}

    monkeypatch.setattr(M, "query_qwen_json", fake_query)

    result = M.call_llm("분류")

    assert result == [{"item_name": "상품9001"}]
    assert seen["client"] is client
    assert seen["model_candidates"] == ["qwen2.5:14b"]


def test_zero_price_llm_result_stays_pending(monkeypatch):
    monkeypatch.setattr(
        M,
        "call_llm",
        lambda _prompt: [{
            "item_name": "기본맛",
            "표준_메뉴명_edit": "기본맛",
            "수동분류_edit": "옵션",
        }],
    )

    row = M._classify_batch([_item(unitprice="0", item_name="기본맛")], [], [])[0]

    assert row["unitprice"] == "0"
    assert row[M.REVIEW_STATUS_COLUMN] == M.REVIEW_PENDING
    assert row["classified_by"] == "llm"


def test_existing_automatic_approvals_reset_once_and_human_reapproval_persists():
    automatic = _map_row(
        "9001",
        표준_메뉴명_edit="자동 상품",
        수동분류_edit="옵션",
        classified_by="auto_zero_price",
        **{M.REVIEW_STATUS_COLUMN: M.REVIEW_APPROVED},
    )
    human = _map_row(
        "9002",
        표준_메뉴명_edit="사람 승인",
        수동분류_edit="메인",
        classified_by="human",
        **{M.REVIEW_STATUS_COLUMN: M.REVIEW_APPROVED},
    )
    review = pd.DataFrame([
        {col: automatic.get(col, "") for col in M.REVIEW_INTERNAL_COLUMNS},
        {col: human.get(col, "") for col in M.REVIEW_INTERNAL_COLUMNS},
    ])

    reset_map, reset_review, reset_count, reset_keys = M._reset_automatic_approvals(
        _map_df(automatic, human),
        review,
    )

    by_id = reset_map.set_index("item_id")
    assert reset_count == 1
    assert len(reset_keys) == 1
    assert by_id.loc["9001", M.REVIEW_STATUS_COLUMN] == M.REVIEW_PENDING
    assert by_id.loc["9002", M.REVIEW_STATUS_COLUMN] == M.REVIEW_APPROVED
    assert reset_review.set_index("item_id").loc["9001", M.REVIEW_STATUS_COLUMN] == M.REVIEW_PENDING

    reset_review.loc[
        reset_review["item_id"].eq("9001"),
        M.REVIEW_STATUS_COLUMN,
    ] = M.REVIEW_APPROVED
    reapplied = M.apply_review_edits(reset_map, reset_review)
    reapplied_by_id = reapplied.set_index("item_id")
    assert reapplied_by_id.loc["9001", M.REVIEW_STATUS_COLUMN] == M.REVIEW_APPROVED
    assert reapplied_by_id.loc["9001", "classified_by"] == "human"

    final_map, _, second_reset_count, _ = M._reset_automatic_approvals(
        reapplied,
        reset_review,
    )
    assert second_reset_count == 0
    assert final_map.set_index("item_id").loc["9001", M.REVIEW_STATUS_COLUMN] == M.REVIEW_APPROVED


def test_main_set_policy_corrects_only_machine_pending_rows():
    rows = _map_df(
        _map_row(
            "9001",
            item_name="[들깨] 우거지 닭도리탕",
            표준_메뉴명_edit="들깨 우거지 닭도리탕",
            수동분류_edit="옵션",
            classified_by="llm",
        ),
        _map_row(
            "9002",
            item_name="대새 추가 (대창 150g+새우8마리)",
            표준_메뉴명_edit="대새 추가",
            수동분류_edit="세트",
            classified_by="llm",
        ),
        _map_row(
            "9003",
            item_name="선택 상품",
            표준_메뉴명_edit="선택 상품",
            수동분류_edit="세트",
            classified_by="llm",
        ),
        _map_row(
            "9004",
            item_name="사람 승인 닭도리탕",
            표준_메뉴명_edit="사람 승인 닭도리탕",
            수동분류_edit="세트",
            classified_by="human",
            **{M.REVIEW_STATUS_COLUMN: M.REVIEW_APPROVED},
        ),
    )

    result, corrected, unresolved, changed_keys = M._apply_main_set_policy(rows)
    by_id = result.set_index("item_id")

    assert corrected == 1
    assert unresolved == 1
    assert len(changed_keys) == 2
    assert by_id.loc["9001", "수동분류_edit"] == "메인"
    assert by_id.loc["9001", "classified_by"] == "main_set_guard"
    assert by_id.loc["9002", "수동분류_edit"] == "세트"
    assert by_id.loc["9003", "수동분류_edit"] == ""
    assert by_id.loc["9003", "classified_by"] == "llm_unresolved"
    assert by_id.loc["9004", "수동분류_edit"] == "세트"


def test_prompt_and_examples_enforce_composite_set_policy():
    prompt = M.build_prompt(
        [_item(item_name="묵은지 도리탕", 대표메뉴="대표 닭도리탕")],
        [],
        [],
    )
    approved = _map_df(
        _map_row(
            "9001",
            item_name="단일 닭도리탕",
            표준_메뉴명_edit="단일 닭도리탕",
            수동분류_edit="세트",
            classified_by="human",
            **{M.REVIEW_STATUS_COLUMN: M.REVIEW_APPROVED},
        ),
        _map_row(
            "9002",
            item_name="닭도리탕+계란찜",
            표준_메뉴명_edit="닭도리탕+계란찜",
            수동분류_edit="세트",
            classified_by="human",
            **{M.REVIEW_STATUS_COLUMN: M.REVIEW_APPROVED},
        ),
    )

    examples = M.build_examples(approved)

    assert '대표메뉴="대표 닭도리탕"' in prompt
    assert '단일 본식은 기본적으로 "메인"' in prompt
    assert [row["item_name"] for row in examples] == ["닭도리탕+계란찜"]
