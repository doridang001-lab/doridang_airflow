// 아이콘 클릭 시 시각적 피드백 제공
async function showClickFeedback(tabId) {
  try {
    await chrome.action.setBadgeText({ text: '✓', tabId: tabId });
    await chrome.action.setBadgeBackgroundColor({ color: '#28a745', tabId: tabId });

    setTimeout(async () => {
      await chrome.action.setBadgeText({ text: '', tabId: tabId });
    }, 500);
  } catch (e) {
    console.log('[Feedback] 피드백 표시 실패:', e);
  }
}

const MANUAL_COLLECT_MIN_INTERVAL_MS = 10 * 60 * 1000;
const MANUAL_THROTTLE_LOCK_MS = 2 * 60 * 60 * 1000;
const MANUAL_LOCK_KEY = 'ce_manual_collect_lock_until';
const MANUAL_LAST_START_KEY = 'ce_manual_collect_last_started_at';

function isCoupangTab(tab) {
  return String(tab?.url || '').includes('store.coupangeats.com');
}

function throttleSignalFromPayload(payload) {
  const text = JSON.stringify(payload || {});
  return /10056|10057|ACCESS_DENIED|ACCESS_DENY|ACCESS_DENIDE|과도한 요청|일시적으로 제한|단시간 내 반복 요청|권한이 존재하지 않/.test(text);
}

async function showBlockedBadge(tabId, text = 'WAIT') {
  try {
    await chrome.action.setBadgeText({ text, tabId });
    await chrome.action.setBadgeBackgroundColor({ color: '#dc3545', tabId });
    setTimeout(() => {
      chrome.action.setBadgeText({ text: '', tabId }).catch(() => {});
    }, 3000);
  } catch (_) {}
}

async function canStartManualCoupangCollect(tabId) {
  const now = Date.now();
  const state = await chrome.storage.local.get([MANUAL_LOCK_KEY, MANUAL_LAST_START_KEY]);
  const lockUntil = Number(state[MANUAL_LOCK_KEY] || 0);
  if (lockUntil > now) {
    const remainMin = Math.ceil((lockUntil - now) / 60000);
    console.warn(`[Manual Collect] 쿠팡 제한 락 중 — ${remainMin}분 후 재시도 가능`);
    await showBlockedBadge(tabId, 'LOCK');
    return false;
  }

  const lastStartedAt = Number(state[MANUAL_LAST_START_KEY] || 0);
  if (lastStartedAt && now - lastStartedAt < MANUAL_COLLECT_MIN_INTERVAL_MS) {
    const remainMin = Math.ceil((MANUAL_COLLECT_MIN_INTERVAL_MS - (now - lastStartedAt)) / 60000);
    console.warn(`[Manual Collect] 쿠팡 수동수집 쿨다운 — ${remainMin}분 후 재시도 가능`);
    await showBlockedBadge(tabId, 'WAIT');
    return false;
  }

  await chrome.storage.local.set({ [MANUAL_LAST_START_KEY]: now });
  return true;
}

async function lockManualCoupangCollect(reason) {
  const until = Date.now() + MANUAL_THROTTLE_LOCK_MS;
  await chrome.storage.local.set({ [MANUAL_LOCK_KEY]: until });
  console.warn(`[Manual Collect] 쿠팡 제한 감지 — 수동수집 2시간 잠금 (${reason || 'throttle'})`);
}

// 아이콘 클릭 시 수집 실행
chrome.action.onClicked.addListener(async (tab) => {
  try { await chrome.tabs.update(tab.id, { autoDiscardable: false }); } catch (_) {}
  if (isCoupangTab(tab) && !await canStartManualCoupangCollect(tab.id)) return;
  await showClickFeedback(tab.id);

  try {
    await chrome.tabs.sendMessage(tab.id, { type: 'COLLECT', source: 'manual', manualGuardChecked: true });
  } catch (e) {
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
    await chrome.tabs.sendMessage(tab.id, { type: 'COLLECT', source: 'manual', manualGuardChecked: true });
  }
});

const RUNNER_PATH = 'runner.html';

async function openRunnerDashboard() {
  const runnerUrl = chrome.runtime.getURL(RUNNER_PATH);
  try {
    const tabs = await chrome.tabs.query({ url: runnerUrl });
    if (tabs.length > 0) {
      await chrome.tabs.update(tabs[0].id, { active: true });
      return;
    }
    await chrome.tabs.create({ url: runnerUrl, active: true });
  } catch (e) {
    console.log('[Runner] 대시보드 자동 열기 실패:', e);
  }
}

chrome.runtime.onStartup.addListener(() => {
  // 자동수집은 Windows 작업 스케줄러의 doridang 프로필 Chrome에서만 시작한다.
});

chrome.runtime.onInstalled.addListener(() => {
  // 설치/업데이트 시 일반 Chrome 또는 게스트 프로필에서 runner가 열리지 않게 한다.
});

function pad2(n) {
  return String(n).padStart(2, '0');
}

function fallbackDownloadFilename(msg) {
  const derived = deriveCoupangOrdersFilename(msg?.content);
  if (derived) return derived;

  const d = new Date();
  const ymd = `${d.getFullYear()}${pad2(d.getMonth() + 1)}${pad2(d.getDate())}`;
  const hms = `${pad2(d.getHours())}${pad2(d.getMinutes())}${pad2(d.getSeconds())}`;
  const ext = String(msg?.mimeType || '').includes('text/plain') ? 'txt' : 'csv';
  return `multi_site_collector_${ymd}_${hms}.${ext}`;
}

function safeFilenamePart(value) {
  return String(value || '')
    .replace(/[\\/:*?"<>|]+/g, '_')
    .replace(/^\.+/, '')
    .replace(/\s+/g, '_')
    .trim();
}

function parseCsvLine(line) {
  const values = [];
  let current = '';
  let quoted = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    const next = line[i + 1];

    if (ch === '"' && quoted && next === '"') {
      current += '"';
      i += 1;
    } else if (ch === '"') {
      quoted = !quoted;
    } else if (ch === ',' && !quoted) {
      values.push(current);
      current = '';
    } else {
      current += ch;
    }
  }

  values.push(current);
  return values;
}

function deriveCoupangOrdersFilename(content) {
  const text = String(content || '').replace(/^\uFEFF/, '');
  const lines = text.split(/\r?\n/).filter(line => line.trim());
  if (lines.length < 2 || !lines[0].includes('store_id') || !lines[0].includes('order_date')) {
    return null;
  }

  const header = parseCsvLine(lines[0]);
  const row = parseCsvLine(lines[1]);
  const idx = (name) => header.indexOf(name);
  const storeId = row[idx('store_id')];
  const storeName = row[idx('store_name')];
  const orderDate = row[idx('order_date')];
  const ymdMatch = String(orderDate || '').match(/(\d{4})[.-](\d{2})[.-](\d{2})/);

  if (!storeId || !storeName || !ymdMatch) return null;

  const ymd = `${ymdMatch[1]}${ymdMatch[2]}${ymdMatch[3]}`;
  return `coupangeats_orders_${safeFilenamePart(storeName)}_${safeFilenamePart(storeId)}_${ymd}.csv`;
}

function sanitizeDownloadFilename(filename, msg) {
  const raw = String(filename || '').trim();
  const safe = raw
    .replace(/[\\/:*?"<>|]+/g, '_')
    .replace(/^\.+/, '')
    .replace(/\s+/g, ' ')
    .trim();

  if (!safe) return fallbackDownloadFilename(msg);
  if (!/\.[A-Za-z0-9]{1,8}$/.test(safe)) {
    const ext = String(msg?.mimeType || '').includes('text/plain') ? 'txt' : 'csv';
    return `${safe}.${ext}`;
  }
  return safe;
}

const pendingDownloadNamesByUrl = new Map();

chrome.downloads.onDeterminingFilename.addListener((downloadItem, suggest) => {
  const filename = pendingDownloadNamesByUrl.get(downloadItem.url);
  if (!filename) return;

  suggest({
    filename,
    conflictAction: 'uniquify'
  });
});

// 다운로드 요청 처리
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg?.type === 'MULTISTORE_COMPLETE' && throttleSignalFromPayload(msg.payload)) {
    lockManualCoupangCollect('MULTISTORE_COMPLETE').catch(() => {});
  }

  if (msg?.type === 'DOWNLOAD_CSV') {
    const content = msg.content ?? '';
    const filename = sanitizeDownloadFilename(msg.filename, msg);
    const mimeType = msg.mimeType || 'text/csv;charset=utf-8';

    const blob = new Blob(['\uFEFF' + content], { type: mimeType });
    const reader = new FileReader();

    reader.onloadend = () => {
      const dataUrl = reader.result;
      pendingDownloadNamesByUrl.set(dataUrl, filename);

      chrome.downloads.download({
        url: dataUrl,
        filename: filename,
        saveAs: false
      }, (downloadId) => {
        if (chrome.runtime.lastError) {
          console.error('[Download Error]', chrome.runtime.lastError);
          sendResponse({ success: false, error: chrome.runtime.lastError.message });
        } else {
          sendResponse({ success: true, downloadId, filename: filename });

          // ── 배치 연동: 주문 CSV 저장 시 runner 대시보드로 알림 ──
          // 파일명 패턴: coupangeats_orders_{store}_{storeId}_{date}.csv
          try {
            if (typeof filename === 'string' && filename.indexOf('coupangeats_orders_') !== -1) {
              chrome.runtime.sendMessage({ type: 'BATCH_FILE_SAVED', filename: filename }).catch(() => {});
            }
          } catch (_) { /* ignore */ }
        }
        setTimeout(() => pendingDownloadNamesByUrl.delete(dataUrl), 30000);
      });
    };

    reader.readAsDataURL(blob);
    return true;
  }

  if (msg?.type === 'COLLECTION_COMPLETE') {
    if (throttleSignalFromPayload(msg)) {
      lockManualCoupangCollect('COLLECTION_COMPLETE').catch(() => {});
    }
    const tabId = sender.tab?.id;
    if (tabId) {
      chrome.action.setBadgeText({ text: '✓', tabId: tabId });
      chrome.action.setBadgeBackgroundColor({ color: '#28a745', tabId: tabId });

      setTimeout(() => {
        chrome.action.setBadgeText({ text: '', tabId: tabId });
      }, 3000);
    }
    sendResponse({ success: true });
    return true;
  }
});
