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

// 아이콘 클릭 시 수집 실행
chrome.action.onClicked.addListener(async (tab) => {
  await showClickFeedback(tab.id);

  try {
    await chrome.tabs.sendMessage(tab.id, { type: 'COLLECT' });
  } catch (e) {
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
    await chrome.tabs.sendMessage(tab.id, { type: 'COLLECT' });
  }
});

// 다운로드 요청 처리
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg?.type === 'DOWNLOAD_CSV') {
    const { content, filename } = msg;

    const blob = new Blob(['﻿' + content], { type: 'text/csv;charset=utf-8' });
    const reader = new FileReader();

    reader.onloadend = () => {
      const dataUrl = reader.result;

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
      });
    };

    reader.readAsDataURL(blob);
    return true;
  }

  if (msg?.type === 'COLLECTION_COMPLETE') {
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

// ===================================================================================
// 무인 자동 시작용 알람 (선택): 윈도우 작업 스케줄러 대신 크롬이 항상 켜져 있을 때 사용.
// alarm 발화 시 runner 페이지를 열어 자동 수집을 시작한다.
// (기본 권장 트리거는 윈도우 작업 스케줄러 → runner.html?run=coupang&auto=1)
// ===================================================================================
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === 'daily_coupang') {
    const url = chrome.runtime.getURL('runner.html?run=coupang&auto=1');
    chrome.tabs.create({ url, active: true });
  }
});
