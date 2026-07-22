// ===================================================================================
// ================= 05_main.js : 초기화 / 메시지 핸들러 / 리다이렉트 =================
// ===================================================================================

var SHORTCUT_KEY = '/';

chrome.storage.sync.get(['shortcutKey', 'downloadPath'], (result) => {
  SHORTCUT_KEY = result.shortcutKey ?? '/';
  DOWNLOAD_PATH = result.downloadPath || '';
  console.log(`[Collector] 단축키: ${SHORTCUT_KEY || '없음'}, 경로: ${DOWNLOAD_PATH || '기본'}`);
});

const MANUAL_COLLECT_MIN_INTERVAL_MS = 10 * 60 * 1000;
const MANUAL_LOCK_KEY = 'ce_manual_collect_lock_until';
const MANUAL_LAST_START_KEY = 'ce_manual_collect_last_started_at';
const RUNNER_STOP_KEY = 'ce_runner_stop_requested';
const RUNNER_RELOAD_KEYS = ['ce_orders_restart', 'ce_orders_resume', 'ce_orders_batch_reload_count'];

function isRunnerStopRequested() {
  return new Promise((resolve) => {
    chrome.storage.local.get([RUNNER_STOP_KEY], (result) => resolve(result[RUNNER_STOP_KEY] === true));
  });
}

async function runRunnerCollectorUnlessStopped(opts, label) {
  if (await isRunnerStopRequested()) {
    console.warn(`[Collector] STOP 래치로 ${label} 재개를 차단했습니다.`);
    return false;
  }
  runCollector({ ...opts, runnerManaged: true });
  return true;
}

function isCoupangPage() {
  return location.href.includes('store.coupangeats.com');
}

function getLocalStorage(keys) {
  return new Promise(resolve => chrome.storage.local.get(keys, resolve));
}

function setLocalStorage(values) {
  return new Promise(resolve => chrome.storage.local.set(values, resolve));
}

async function canStartManualCoupangCollect() {
  if (!isCoupangPage()) return true;
  const now = Date.now();
  const state = await getLocalStorage([MANUAL_LOCK_KEY, MANUAL_LAST_START_KEY]);
  const lockUntil = Number(state[MANUAL_LOCK_KEY] || 0);
  if (lockUntil > now) {
    const remainMin = Math.ceil((lockUntil - now) / 60000);
    Utils.showError('쿠팡 수동수집 잠금', `쿠팡 제한 감지 후 보호 잠금 중입니다.\n${remainMin}분 후 다시 시도하세요.`);
    return false;
  }

  const lastStartedAt = Number(state[MANUAL_LAST_START_KEY] || 0);
  if (lastStartedAt && now - lastStartedAt < MANUAL_COLLECT_MIN_INTERVAL_MS) {
    const remainMin = Math.ceil((MANUAL_COLLECT_MIN_INTERVAL_MS - (now - lastStartedAt)) / 60000);
    Utils.showError('쿠팡 수동수집 대기', `연속 수동수집은 밴 위험이 큽니다.\n${remainMin}분 후 다시 시도하세요.`);
    return false;
  }

  await setLocalStorage({ [MANUAL_LAST_START_KEY]: now });
  return true;
}

async function runCollector(opts = {}) {
  const url = location.href;
  for (const [key, site] of Object.entries(Sites)) {
    if (site.match(url)) {
      console.log(`[Collector] ${site.name} 수집 시작 (source: ${opts.source || 'manual'})`);
      await site.collect(opts);
      return true;
    }
  }
  Utils.showError('미지원 사이트', '이 사이트는 아직 지원되지 않습니다.');
  return false;
}

document.addEventListener('keydown', (e) => {
  if (!SHORTCUT_KEY) return;
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.isContentEditable) return;
  if (e.key === SHORTCUT_KEY) {
    e.preventDefault();
    sessionStorage.removeItem('__collector_auto_run');
    chrome.storage.local.remove(['ce_current_target_stores', 'ce_current_target_date', 'ce_current_target_date_mode', 'ce_orders_restart', 'ce_orders_resume', RUNNER_STOP_KEY], async () => {
      if (!await canStartManualCoupangCollect()) return;
      runCollector({ source: 'manual', targetStores: null });
    });
  }
});

// 페이지 로드 시 자동 워크플로우 체크
window.addEventListener('load', () => {
  const url = location.href;

  // F5 새로고침 후 자동 재시작 (무한루프 감지로 인한 reload)
  if (url.includes('store.coupangeats.com/merchant/management/orders/')) {
    chrome.storage.local.get(['ce_orders_restart', 'ce_orders_resume', 'ce_current_target_stores', 'ce_current_target_date', 'ce_current_target_date_mode', RUNNER_STOP_KEY], (result) => {
      const today = new Date().toISOString().slice(0, 10).replace(/-/g, '');

      if (result[RUNNER_STOP_KEY] === true) {
        sessionStorage.removeItem('__collector_auto_run');
        chrome.storage.local.remove(RUNNER_RELOAD_KEYS);
        console.warn('[Collector] STOP 래치로 F5 자동 재개를 차단했습니다.');
        return;
      }

      // F5 새로고침 기반 페이지 복귀 재개 (안정화/로딩 타임아웃 폴백)
      const resume = result.ce_orders_resume;
      if (resume && resume.date === today) {
        // 키를 먼저 제거 → 재개 중 예외가 나도 무한 재개 루프 방지(데이터는 in-memory 보존).
        // 재개 도중 다시 막히면 _hardReloadResume 가 키를 새로 저장(reloadCount++)한다.
        chrome.storage.local.remove(['ce_orders_resume']);
        setTimeout(async () => {
          if (await isRunnerStopRequested()) return;
          console.log('[Collector] F5 재개 - 막힌 페이지부터 이어서 수집');
          Sites['coupangeats']._resumeOrdersFromReload(resume);
        }, 3000);
        return;
      }

      const restart = result.ce_orders_restart;
      if (restart && restart.date === today) {
        chrome.storage.local.remove(['ce_orders_restart']);
        setTimeout(() => {
          console.log('[Collector] F5 재시작 - 체크포인트 기반 수집 재개');
          const targetStores = Array.isArray(result.ce_current_target_stores) ? result.ce_current_target_stores : [];
          if (targetStores.length > 0) runRunnerCollectorUnlessStopped({ source: 'batch', targetStores, targetDate: result.ce_current_target_date, targetDateMode: result.ce_current_target_date_mode }, 'F5 배치');
        }, 3000);
        return;
      }

      // 건수 불일치로 인한 재수집 자동 실행
      if (sessionStorage.getItem('__collector_auto_run') === '1') {
        sessionStorage.removeItem('__collector_auto_run');
        setTimeout(() => {
          console.log('[Collector] 건수 불일치 - 자동 재수집 시작...');
          const targetStores = Array.isArray(result.ce_current_target_stores) ? result.ce_current_target_stores : [];
          if (targetStores.length > 0) runRunnerCollectorUnlessStopped({ source: 'batch', targetStores, targetDate: result.ce_current_target_date, targetDateMode: result.ce_current_target_date_mode }, '건수 불일치');
        }, 2500);
      }
    });
    return;
  }

  // 건수 불일치로 인한 재수집 자동 실행 (orders 아닌 페이지)
  if (sessionStorage.getItem('__collector_auto_run') === '1') {
    sessionStorage.removeItem('__collector_auto_run');
    setTimeout(() => {
      console.log('[Collector] 건수 불일치 - 자동 재수집 시작...');
      chrome.storage.local.get(['ce_current_target_stores', 'ce_current_target_date', 'ce_current_target_date_mode'], (result) => {
        const targetStores = Array.isArray(result.ce_current_target_stores) ? result.ce_current_target_stores : [];
          if (targetStores.length > 0) runRunnerCollectorUnlessStopped({ source: 'batch', targetStores, targetDate: result.ce_current_target_date, targetDateMode: result.ce_current_target_date_mode }, '건수 불일치');
      });
    }, 2500);
  }

  // 쿠팡: home 페이지에서 자동으로 orders 페이지로 이동
  if (url.includes('store.coupangeats.com/merchant/management/home/')) {
    const storeIdMatch = url.match(/\/home\/(\d+)/);
    if (storeIdMatch) {
      const storeId = storeIdMatch[1];
      const nextUrl = `https://store.coupangeats.com/merchant/management/orders/${storeId}`;
      
      setTimeout(() => {
        const confirmMsg = `🔄 쿠팡이츠 자동 워크플로우\n\n매출 관리 페이지로 이동하시겠습니까?\n(매장 ID: ${storeId})`;
        
        Utils.navigateWithConfirm(nextUrl, confirmMsg);
      }, 1500);
    }
  }
});

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg?.type === 'GET_SHOP_INFO') {
    try {
      sendResponse({ success: true, shopInfo: Sites['baemin']._getShopInfo() });
    } catch (e) {
      sendResponse({ success: false, error: String(e) });
    }
    return true;
  }

  if (msg?.type === 'STOP') {
    try {
      if (Sites['coupangeats']) Sites['coupangeats']._stopFlag = true;
      if (Sites['baemin']) Sites['baemin']._stopFlag = true;
      sessionStorage.removeItem('__collector_auto_run');
      chrome.storage.local.set({ [RUNNER_STOP_KEY]: true }, () => {
        chrome.storage.local.remove(RUNNER_RELOAD_KEYS, () => sendResponse({ success: true, stopped: true }));
      });
    } catch (e) {
      sendResponse({ success: false, stopped: false, error: String(e) });
    }
    return true;
  }

  if (msg?.type === 'COLLECT') {
    const targetStores = Array.isArray(msg.targetStores) ? msg.targetStores : null;
    const source = msg.source || 'manual';
    (async () => {
      const runnerManaged = source === 'batch' || msg.runnerManaged === true;
      if (runnerManaged && await isRunnerStopRequested()) {
        sendResponse({ success: false, source, stopped: true });
        return;
      }
      if (!runnerManaged) await chrome.storage.local.remove([RUNNER_STOP_KEY]);
      if (source === 'batch' && targetStores && targetStores.length > 0) {
        chrome.storage.local.set({
          ce_current_target_stores: targetStores,
          ce_current_target_date: msg.targetDate || '',
          ce_current_target_date_mode: msg.targetDateMode || 'yesterday'
        });
      } else {
        chrome.storage.local.remove(['ce_current_target_stores', 'ce_current_target_date', 'ce_current_target_date_mode', 'ce_orders_restart']);
      }
      if (source === 'manual' && !msg.manualGuardChecked && !await canStartManualCoupangCollect()) {
        sendResponse({ success: false, source, blocked: true });
        return;
      }
      runCollector({
        source,
        targetStores,
        targetDate: msg.targetDate || '',
        targetDateMode: msg.targetDateMode || 'yesterday',
        expectedStore: msg.expectedStore || '',
        expectedStoreId: msg.expectedStoreId || '',
        runnerManaged
      });
      sendResponse({ success: true, source });
    })();
    return true;
  }
  if (msg?.type === 'UPDATE_SHORTCUT') {
    SHORTCUT_KEY = msg.key;
    console.log(`[Collector] 단축키 변경됨: ${SHORTCUT_KEY || '없음'}`);
    sendResponse({ success: true });
  }
  if (msg?.type === 'UPDATE_DOWNLOAD_PATH') {
    DOWNLOAD_PATH = msg.path;
    console.log(`[Collector] 다운로드 경로 변경됨: ${DOWNLOAD_PATH || '기본'}`);
    sendResponse({ success: true });
  }
  if (msg?.type === 'UPDATE_NAVIGATE_CONFIRM') {
    console.log(`[Collector] 자동 이동 설정 변경: ${msg.showConfirm ? 'ON (확인창 표시)' : 'OFF (바로 이동)'}`);
    sendResponse({ success: true });
  }
  if (msg?.type === 'AUTO_LOGIN') {
    console.log('[Content] 메시지 받음:', {
      platform: msg.platform,
      storeName: msg.storeName,
      autoClick: msg.autoClick,
      autoClickType: typeof msg.autoClick
    });
    handleAutoLogin(msg.platform, msg.id, msg.pw, msg.storeName, msg.autoClick);
    sendResponse({ success: true });
  }
  if (msg?.type === 'PING') {
    sendResponse({ success: true });
  }

  // 배민 파이프라인 규격 수동수집
  if (msg?.type === 'COLLECT_PIPELINE_ORDERS') {
    sendResponse({ ack: true });
    if (window.BaeminPipelineSave) {
      window.BaeminPipelineSave.collectAndSaveOrders()
        .then(result => chrome.runtime.sendMessage({ type: 'BAEMIN_PIPELINE_COMPLETE', result }))
        .catch(err => chrome.runtime.sendMessage({ type: 'BAEMIN_PIPELINE_COMPLETE', result: { success: false, error: String(err) } }));
    } else {
      chrome.runtime.sendMessage({ type: 'BAEMIN_PIPELINE_COMPLETE', result: { success: false, error: 'BaeminPipelineSave 미로드' } });
    }
  }

  if (msg?.type === 'COLLECT_PIPELINE_ADFUNNEL') {
    const targetDate = msg.targetDate;
    sendResponse({ ack: true });
    if (window.BaeminPipelineSave) {
      window.BaeminPipelineSave.collectAndSaveAdFunnel(targetDate)
        .then(result => chrome.runtime.sendMessage({ type: 'BAEMIN_PIPELINE_COMPLETE', result }))
        .catch(err => chrome.runtime.sendMessage({ type: 'BAEMIN_PIPELINE_COMPLETE', result: { success: false, error: String(err) } }));
    } else {
      chrome.runtime.sendMessage({ type: 'BAEMIN_PIPELINE_COMPLETE', result: { success: false, error: 'BaeminPipelineSave 미로드' } });
    }
  }

  return true;
});



console.log('[Collector] 로드 완료.');


// ===================================================================================
// [추가 로직] 쿠팡이츠 로그인 후 매출(주문) 페이지로 강제 리다이렉트 (보완판)
// ===================================================================================
(function() {
  const redirectToOrders = () => {
    const url = window.location.href;
    // 주소에 'management/home'이 포함되어 있다면 'management/orders'로 교체
    if (url.includes('store.coupangeats.com/merchant/management/home/')) {
      const orderUrl = url.replace('/management/home/', '/management/orders/');
      console.log('[자동 이동] 홈 감지 -> 매출 페이지로 이동:', orderUrl);
      window.location.replace(orderUrl);
    }
  };

  // 문서가 준비되었을 때 실행
  const init = () => {
    // 1. 처음 페이지 로드 시 체크
    redirectToOrders();

    // 2. 주소창 감시 (SPA 대응)
    let lastUrl = location.href;
    const observer = new MutationObserver(() => {
      const currentUrl = location.href;
      if (currentUrl !== lastUrl) {
        lastUrl = currentUrl;
        redirectToOrders();
      }
    });

    if (document.body) {
      observer.observe(document.body, { childList: true, subtree: true });
    }
  };

  // 실행 시점 조절
  if (document.readyState === 'complete') {
    init();
  } else {
    window.addEventListener('load', init);
  }
})();


