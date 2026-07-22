from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "coupang_extension_build" / "runner.js"
CONTENT_MAIN = ROOT / "coupang_extension_build" / "content" / "05_main.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_runner_tracks_every_managed_work_tab_and_keeps_stop_fallbacks():
    source = _read(RUNNER)

    assert "const STOP_GRACE_MS = 10000;" in source
    assert "const RUNNER_STOP_KEY = 'ce_runner_stop_requested';" in source
    assert "async function requestCurrentWorkStop()" in source
    assert "chrome.tabs.sendMessage(tabId, { type: 'STOP' })" in source
    assert "Date.now() + STOP_GRACE_MS" in source
    assert "await chrome.storage.local.set({ [RUNNER_STOP_KEY]: true })" in source
    assert "if (!stopRequested && c.done > 0" in source

    managed_creates = "chrome.tabs.create({url:'about:blank', active:true})"
    tracked_creates = f"trackWorkTab(await {managed_creates})"
    assert source.count(managed_creates) == 3
    assert source.count(tracked_creates) == 3
    assert source.count("await closeWorkTab(") == 3


def test_content_stop_contract_blocks_collect_and_reload_resume():
    source = _read(CONTENT_MAIN)

    assert "if (msg?.type === 'STOP')" in source
    assert "Sites['coupangeats']._stopFlag = true" in source
    assert "Sites['baemin']._stopFlag = true" in source
    assert "sendResponse({ success: true, stopped: true })" in source
    assert "const RUNNER_RELOAD_KEYS" in source
    assert "const runnerManaged = source === 'batch' || msg.runnerManaged === true" in source
    assert "runnerManaged && await isRunnerStopRequested()" in source
    assert source.count("runRunnerCollectorUnlessStopped") == 4
    assert "if (await isRunnerStopRequested()) return;" in source
