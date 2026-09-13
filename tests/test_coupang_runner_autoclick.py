from __future__ import annotations

import importlib.util
from pathlib import Path


def load_autoclick_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "coupang_runner_autoclick.py"
    spec = importlib.util.spec_from_file_location("coupang_runner_autoclick", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_runner_tab_url_accepts_query_and_hash():
    mod = load_autoclick_module()
    expected = "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html"
    actual = expected + "?auto=1&mode=top50&date=yesterday#log"

    assert mod._is_runner_tab_url(actual, expected)


def test_runner_tab_url_rejects_other_extension_id():
    mod = load_autoclick_module()

    assert not mod._is_runner_tab_url(
        "chrome-extension://otherextensionid/runner.html?auto=1",
        "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html",
    )


def test_loaded_runner_url_normalizes_query_url_to_base_runner_url():
    mod = load_autoclick_module()
    tabs = [
        {
            "url": "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html?auto=1&runTs=20260901001348",
            "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/1",
        }
    ]

    assert (
        mod._runner_url_from_loaded_extensions(tabs)
        == "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html"
    )


def test_existing_runner_websockets_accepts_query_runner_tabs():
    mod = load_autoclick_module()
    tabs = [
        {
            "url": "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html?auto=1",
            "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/1",
        },
        {
            "url": "https://store.coupangeats.com/merchant/login",
            "webSocketDebuggerUrl": "ws://127.0.0.1/devtools/page/2",
        },
    ]

    assert mod._existing_runner_websockets(
        tabs,
        "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html",
    ) == ["ws://127.0.0.1/devtools/page/1"]


def test_blocked_runner_page_detects_chrome_error_document():
    mod = load_autoclick_module()

    assert mod._is_blocked_runner_page(
        {
            "locationHref": "chrome-error://chromewebdata/",
            "bodyText": "ocpdgnoaajajnlehamcalfcpholjhfbe이(가) 차단됨\nERR_BLOCKED_BY_CLIENT",
        }
    )


def test_blocked_runner_page_ignores_real_runner_document():
    mod = load_autoclick_module()

    assert not mod._is_blocked_runner_page(
        {
            "locationHref": "chrome-extension://ocpdgnoaajajnlehamcalfcpholjhfbe/runner.html",
            "bodyText": "쿠팡 전체 자동수집",
        }
    )
