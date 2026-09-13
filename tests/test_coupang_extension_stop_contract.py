from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "coupang_extension_build" / "runner.js"
CONTENT_MAIN = ROOT / "coupang_extension_build" / "content" / "05_main.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_runner_tracks_every_managed_work_tab_and_keeps_stop_fallbacks():
    source = _read(RUNNER)

    assert "const STOP_GRACE_MS = 10000;" in source
    assert "const RUNNER_STOP_KEY = 'ce_coupang_runner_stop_requested';" in source
    assert "async function requestCurrentWorkStop()" in source
    assert "chrome.tabs.sendMessage(tabId, { type: 'STOP', runnerName: 'coupang' })" in source
    assert "Date.now() + STOP_GRACE_MS" in source
    assert "await chrome.storage.local.set({ [RUNNER_STOP_KEY]: true })" in source
    assert "function isCurrentWorkTabMessage(sender)" in source
    assert "if (!isCurrentWorkTabMessage(sender)) return;" in source
    assert "runnerName:'coupang'" in source
    assert "if (!stopRequested && c.done > 0" in source

    managed_creates = "chrome.tabs.create({url:'about:blank', active:true})"
    tracked_creates = f"trackWorkTab(await {managed_creates})"
    assert source.count(managed_creates) == 3
    assert source.count(tracked_creates) == 3
    assert source.count("await closeWorkTab(") == 3


def test_content_stop_contract_blocks_collect_and_reload_resume():
    source = _read(CONTENT_MAIN)

    assert "const COUPANG_RUNNER_STOP_KEY = 'ce_coupang_runner_stop_requested';" in source
    assert "const NAVERADS_RUNNER_STOP_KEY = 'naverads_runner_stop_requested';" in source
    assert "function stopKeyForMessage(msg = {})" in source
    assert "if (msg?.type === 'STOP')" in source
    assert "Sites['coupangeats']._stopFlag = true" in source
    assert "Sites['baemin']._stopFlag = true" in source
    assert "sendResponse({ success: true, stopped: true })" in source
    assert "const RUNNER_RELOAD_KEYS" in source
    assert "const runnerManaged = source === 'batch' || msg.runnerManaged === true" in source
    assert "runnerManaged && await isRunnerStopRequested(stopKey)" in source
    assert source.count("runRunnerCollectorUnlessStopped") == 4
    assert "if (await isRunnerStopRequested()) return;" in source
    assert "if (!naverAdsCollect && source === 'batch' && targetStores && targetStores.length > 0)" in source
    assert "} else if (!naverAdsCollect) {" in source


def test_runner_reclicks_same_login_page_for_permission_error_text():
    source = _read(RUNNER)

    assert "const permissionErr = isLoginPermissionError(lastLoginErrText);" in source
    assert "permissionErr || (!isLoginCredError(lastLoginErrText) && !throttleErr)" in source
    assert "retryLoginWithoutReload = true;" in source
    assert "권한 오류 문구' : '로그인 에러 문구'" in source
    assert "같은 페이지 마우스형 재클릭으로 확인" in source
    assert "로그인 권한 거절 — 재클릭 후에도 동일 문구, 다음 계정 진행" in source
