const shortcutInput = document.getElementById('shortcutInput');
const statusEl = document.getElementById('status');
const runBtn = document.getElementById('runBtn');

// 저장된 단축키 불러오기
chrome.storage.sync.get(['shortcutKey'], (result) => {
  const key = result.shortcutKey ?? '/';
  shortcutInput.value = key;
  updateStatus(key);
});

function updateStatus(key) {
  if (key) {
    statusEl.textContent = `현재 단축키: ${key}`;
  } else {
    statusEl.textContent = `단축키 없음 (팝업에서만 실행)`;
  }
}

// 단축키 입력 시 저장
shortcutInput.addEventListener('input', (e) => {
  const key = e.target.value;
  
  chrome.storage.sync.set({ shortcutKey: key }, () => {
    statusEl.textContent = key ? `✓ 저장됨: ${key}` : `✓ 단축키 비활성화됨`;
    statusEl.className = 'status saved';
    
    // 모든 탭에 알림
    chrome.tabs.query({}, (tabs) => {
      tabs.forEach(tab => {
        chrome.tabs.sendMessage(tab.id, { type: 'UPDATE_SHORTCUT', key }).catch(() => {});
      });
    });
    
    setTimeout(() => {
      updateStatus(key);
      statusEl.className = 'status';
    }, 1500);
  });
});

// 입력창 포커스 시 전체 선택
shortcutInput.addEventListener('focus', () => {
  shortcutInput.select();
});

// 수집 실행 버튼
runBtn.addEventListener('click', async () => {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  
  if (!tab?.id) {
    alert('탭을 찾을 수 없습니다.');
    return;
  }
  
  try {
    await chrome.tabs.sendMessage(tab.id, { type: 'COLLECT' });
    window.close();
  } catch (err) {
    // content script가 로드되지 않은 경우 직접 주입
    try {
      await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        files: ['content.js']
      });
      // 잠시 대기 후 다시 시도
      setTimeout(async () => {
        await chrome.tabs.sendMessage(tab.id, { type: 'COLLECT' });
        window.close();
      }, 500);
    } catch (err2) {
      alert('이 페이지에서는 수집을 실행할 수 없습니다.\n(배민 또는 쿠팡이츠 페이지에서 실행하세요)');
    }
  }
});