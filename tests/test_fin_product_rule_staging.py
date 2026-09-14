import json
from pathlib import Path

import pytest

from modules.transform.pipelines.db import DB_FinProduct_Map as mapping
from modules.transform.pipelines.db import DB_FinProduct_Rules as rules


def _rule(keyword: str, *, status: str = "active", label: str = "메인") -> dict:
    return {
        "수동분류": label,
        "include_keywords": [keyword],
        "support": 5,
        "confidence": 1.0,
        "status": status,
        "validation": {"accuracy": 1.0},
    }


def test_rule_change_report_uses_strictly_over_thirty_percent():
    existing = [_rule(str(index)) for index in range(10)]
    exactly_thirty = existing + [_rule(str(index)) for index in range(10, 13)]
    over_thirty = exactly_thirty + [_rule("13")]

    assert rules.build_rule_change_report(existing, exactly_thirty)["review_required"] is False
    assert rules.build_rule_change_report(existing, over_thirty)["review_required"] is True


def test_save_rule_proposal_is_deduplicated(tmp_path, monkeypatch):
    active_path = tmp_path / "fin_product_rules.json"
    existing = [_rule(str(index)) for index in range(10)]
    proposed = existing + [_rule(str(index)) for index in range(10, 14)]
    active_path.write_text(json.dumps(existing, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(rules, "FIN_PRODUCT_RULES_JSON_PATH", active_path)

    first_path, first_created, first_report = rules.save_rule_proposal(
        proposed,
        run_id="scheduled__2026-07-21T23:20:00+00:00",
        proposal_dir=tmp_path / "proposals",
    )
    second_path, second_created, second_report = rules.save_rule_proposal(
        proposed,
        run_id="scheduled__2026-07-21T23:20:00+00:00",
        proposal_dir=tmp_path / "proposals",
    )

    assert first_created is True
    assert second_created is False
    assert first_path == second_path
    assert first_report == second_report
    assert json.loads(first_path.read_text(encoding="utf-8"))["change_report"]["review_required"] is True


def test_promote_rule_proposal_rejects_changed_baseline(tmp_path, monkeypatch):
    active_path = tmp_path / "fin_product_rules.json"
    existing = [_rule(str(index)) for index in range(10)]
    proposed = existing + [_rule(str(index)) for index in range(10, 14)]
    active_path.write_text(json.dumps(existing, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(rules, "FIN_PRODUCT_RULES_JSON_PATH", active_path)
    monkeypatch.setattr(rules, "TEMP_DIR", tmp_path / "temp")
    proposal, _, _ = rules.save_rule_proposal(proposed, proposal_dir=tmp_path / "proposals")

    active_path.write_text(json.dumps(existing + [_rule("changed")], ensure_ascii=False), encoding="utf-8")
    with pytest.raises(RuntimeError, match="제안 생성 후 변경"):
        rules.promote_rule_proposal(proposal)


def test_promote_rule_proposal_applies_only_explicit_call(tmp_path, monkeypatch):
    active_path = tmp_path / "fin_product_rules.json"
    existing = [_rule(str(index)) for index in range(10)]
    proposed = existing + [_rule(str(index)) for index in range(10, 14)]
    active_path.write_text(json.dumps(existing, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(rules, "FIN_PRODUCT_RULES_JSON_PATH", active_path)
    monkeypatch.setattr(rules, "TEMP_DIR", tmp_path / "temp")
    proposal, _, report = rules.save_rule_proposal(proposed, proposal_dir=tmp_path / "proposals")

    result = rules.promote_rule_proposal(proposal)

    saved = json.loads(active_path.read_text(encoding="utf-8"))
    assert result["proposal_sha256"] == report["proposal_sha256"]
    assert len(saved) == 14
    assert not list((tmp_path / "temp" / "backups").rglob("fin_product_rules.json"))


def test_build_train_stages_large_change_and_keeps_active_rules(tmp_path, monkeypatch):
    train_path = tmp_path / "fin_product_map_train.json"
    proposed = [_rule(str(index)) for index in range(14)]
    proposal_path = tmp_path / "proposal.json"
    sent = []
    saved = []
    monkeypatch.setattr(mapping, "FIN_PRODUCT_MAP_TRAIN_JSON_PATH", train_path)
    monkeypatch.setattr(mapping, "load_map", lambda: object())
    monkeypatch.setattr(mapping, "load_recently_map", lambda: object())
    monkeypatch.setattr(mapping, "apply_recently_edits", lambda *_args: object())
    monkeypatch.setattr(mapping, "_rule_source_rows", lambda _value: object())
    monkeypatch.setattr(mapping, "build_rules_from_manual", lambda _value: proposed)
    monkeypatch.setattr(
        mapping,
        "summarize_rules",
        lambda _value: {"active_rule_count": 14, "candidate_rule_count": 0, "blocked_rule_count": 0, "conflict_count": 0, "rule_count": 14},
    )
    monkeypatch.setattr(
        mapping,
        "evaluate_rule_change",
        lambda _value: {"old_active_count": 10, "new_active_count": 14, "active_change_ratio": 0.4, "review_required": True},
    )
    monkeypatch.setattr(
        mapping,
        "_build_map_train_payload",
        lambda *_args, **_kwargs: {"label_counts": {"메인": 14}},
    )
    monkeypatch.setattr(
        mapping,
        "save_rule_proposal",
        lambda *_args, **_kwargs: (
            proposal_path,
            True,
            {"old_active_count": 10, "new_active_count": 14, "active_change_ratio": 0.4, "review_required": True},
        ),
    )
    monkeypatch.setattr(mapping, "save_rules", lambda value: saved.append(value))
    monkeypatch.setattr(mapping, "send_telegram", sent.append)

    result = mapping.build_fin_product_map_train_json(run_id="scheduled-test")

    assert result["rule_update_status"] == "review_required"
    assert result["rule_proposal_path"] == str(proposal_path)
    assert saved == []
    assert len(sent) == 1
    assert json.loads(train_path.read_text(encoding="utf-8"))["label_counts"]["메인"] == 14
