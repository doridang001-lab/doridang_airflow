from modules.transform.utility import qwen_client


def test_gpt_oss_20b_is_default_model_candidate():
    assert qwen_client.LLM_MODEL_CANDIDATES[0] == "gpt-oss:20b"


def test_prioritize_primary_models_prefers_gpt_oss_by_default():
    models = ["qwen2.5:14b", "gpt-oss:20b"]

    assert qwen_client._prioritize_primary_models(models)[0] == "gpt-oss:20b"


def test_prioritize_primary_models_can_still_prefer_stable_models():
    models = ["qwen2.5:14b", "gpt-oss:20b"]

    assert qwen_client._prioritize_primary_models(models, prefer_stable=True)[0] == "qwen2.5:14b"


def test_gpt_oss_json_options_allow_short_batched_json():
    options = qwen_client._chat_options_for_model("gpt-oss:20b", is_json=True)

    assert options["num_predict"] >= 1024
    assert options["num_ctx"] >= 8192
    assert options["temperature"] == 0
    assert options["top_p"] <= 0.2


def test_query_json_uses_low_thinking_for_gpt_oss():
    calls = []

    class FakeMessage:
        content = '{"items":[]}'
        thinking = "분석"

    class FakeResponse:
        message = FakeMessage()

    class FakeClient:
        def chat(self, **kwargs):
            calls.append(kwargs)
            return FakeResponse()

    parsed = qwen_client.query_qwen_json(
        "JSON만 응답",
        client=FakeClient(),
        model_candidates=["gpt-oss:20b"],
    )

    assert parsed == {"items": []}
    assert calls[0]["model"] == "gpt-oss:20b"
    assert calls[0]["think"] == "low"
    assert calls[0]["options"]["num_predict"] >= 1024
    assert calls[0]["options"]["num_ctx"] >= 8192


def test_gpt_oss_unhealthy_cache_expires_faster(monkeypatch):
    now = 2_000_000.0
    monkeypatch.setattr(qwen_client.time, "time", lambda: now)

    unhealthy = {
        "gpt-oss:20b": now - qwen_client.GPT_OSS_UNHEALTHY_TTL_SEC - 1,
        "qwen2.5:14b": now - qwen_client.GPT_OSS_UNHEALTHY_TTL_SEC - 1,
    }

    assert not qwen_client._is_model_unhealthy("gpt-oss:20b", unhealthy)
    assert qwen_client._is_model_unhealthy("qwen2.5:14b", unhealthy)
