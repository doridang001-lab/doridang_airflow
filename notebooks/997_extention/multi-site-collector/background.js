// 아이콘 클릭 시 수집 실행
chrome.action.onClicked.addListener(async (tab) => {
  try {
    await chrome.tabs.sendMessage(tab.id, { type: 'COLLECT' });
  } catch (e) {
    // content.js 없으면 주입 후 재시도
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
    await chrome.tabs.sendMessage(tab.id, { type: 'COLLECT' });
  }
});
