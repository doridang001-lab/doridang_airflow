const shortcutInput = document.getElementById('shortcutInput');
const statusEl = document.getElementById('status');
const runBtn = document.getElementById('runBtn');
const normalView = document.getElementById('normalView');
const loginView = document.getElementById('loginView');
const platformTitle = document.getElementById('platformTitle');
const managerList = document.getElementById('managerList');

// 페이지 로드 시 현재 탭 확인
(async function init() {
  console.log('[Popup] 초기화 시작');

  // accounts.js가 로드될 때까지 약간 대기
  await new Promise(resolve => setTimeout(resolve, 100));

  if (window.accountsReady && typeof window.accountsReady.then === 'function') {
    try {
      await window.accountsReady;
    } catch (error) {
      console.warn('[Popup] 계정 로드 실패:', error);
    }
  }

  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

  console.log('[Popup] 현재 탭:', tab?.url);

  if (!tab?.url) {
    console.log('[Popup] URL 없음, 일반 뷰 표시');
    showNormalView();
    return;
  }

  // detectPlatform 함수 확인
  if (typeof window.detectPlatform !== 'function') {
    console.error('[Popup] detectPlatform 함수가 로드되지 않음!');
    showNormalView();
    return;
  }

  const platform = window.detectPlatform(tab.url);
  console.log('[Popup] 감지된 플랫폼:', platform);

  if (platform) {
    // 로그인 페이지인 경우
    console.log('[Popup] 로그인 페이지 감지, 로그인 뷰 표시');
    showLoginView(platform, tab.id);
  } else {
    // 일반 페이지인 경우
    console.log('[Popup] 일반 페이지, 일반 뷰 표시');
    showNormalView();
  }
})();

function showNormalView() {
  normalView.style.display = 'block';
  loginView.style.display = 'none';

  // 저장된 설정 불러오기
  chrome.storage.sync.get(['shortcutKey', 'showConfirmOnNavigate'], (result) => {
    const key = result.shortcutKey ?? '/';
    shortcutInput.value = key;
    updateStatus(key);

    // 자동 이동 확인창 설정 로드 (기본값: false = 확인창 표시 안 함)
    const showConfirm = result.showConfirmOnNavigate ?? false;
    const showConfirmToggle = document.getElementById('showConfirmToggle');
    if (showConfirmToggle) {
      showConfirmToggle.checked = showConfirm;
    }
  });
}

function showLoginView(platform, tabId) {
  console.log('[Popup] showLoginView 호출:', platform);

  normalView.style.display = 'none';
  loginView.style.display = 'block';

  // 플랫폼 타이틀 설정
  const platformNames = { baemin: '배달의민족', coupang: '쿠팡이츠', yogiyo: '요기요', ddangyo: '땡겨요' };
  const platformName = platformNames[platform] || platform;
  platformTitle.textContent = `${platformName} 자동 로그인`;

  // getStoresByPlatform 함수 확인
  if (typeof window.getStoresByPlatform !== 'function') {
    console.error('[Popup] getStoresByPlatform 함수가 로드되지 않음!');
    managerList.innerHTML = '<div style="padding: 20px; text-align: center; color: #dc3545;">계정 정보를 불러올 수 없습니다.</div>';
    return;
  }

  // 매장 목록 렌더링
  const storesByManager = window.getStoresByPlatform(platform);
  console.log('[Popup] 매장 목록:', storesByManager);

  renderManagerList(storesByManager, platform, tabId);
}

function renderManagerList(storesByManager, platform, tabId) {
  managerList.innerHTML = '';

  for (const [manager, stores] of Object.entries(storesByManager)) {
    const section = document.createElement('div');
    section.className = 'manager-section';

    const managerName = document.createElement('div');
    managerName.className = 'manager-name';
    managerName.textContent = manager;
    section.appendChild(managerName);

    const storeList = document.createElement('div');
    storeList.className = 'store-list';

    stores.forEach(store => {
      const storeItem = document.createElement('div');
      storeItem.className = 'store-item';
      storeItem.textContent = store.store;
      storeItem.dataset.id = store.id;
      storeItem.dataset.pw = store.pw;
      storeItem.dataset.platform = platform;

      storeItem.addEventListener('click', () => {
        handleStoreClick(store, platform, tabId);
      });

      storeList.appendChild(storeItem);
    });

    section.appendChild(storeList);
    managerList.appendChild(section);
  }
}

const CONTENT_SCRIPT_FILES = [
  'content/00_config.js',
  'content/01_utils.js',
  'content/02_baemin.js',
  'content/03_coupangeats.js',
  'content/04_auto_login.js',
  'content/05_main.js',
  'content/06_batch.js'
];

async function ensureContentScript(tabId) {
  try {
    await chrome.tabs.sendMessage(tabId, { type: 'PING' });
    return true; // 이미 로드됨
  } catch (e) {
    // content script 없음 → 수동 주입
    console.log('[Popup] content script 없음, 주입 시도...');
    await chrome.scripting.executeScript({ target: { tabId }, files: CONTENT_SCRIPT_FILES });
    await new Promise(resolve => setTimeout(resolve, 300));
    return true;
  }
}

async function handleStoreClick(store, platform, tabId) {
  try {
    const autoClickCheckbox = document.getElementById('autoClickBtn');
    const autoClick = autoClickCheckbox ? autoClickCheckbox.checked : false;

    await ensureContentScript(tabId);

    await chrome.tabs.sendMessage(tabId, {
      type: 'AUTO_LOGIN',
      platform: platform,
      id: store.id,
      pw: store.pw,
      storeName: store.store,
      autoClick: autoClick
    });

    chrome.action.setBadgeText({ text: '✓', tabId: tabId });
    chrome.action.setBadgeBackgroundColor({ color: '#28a745', tabId: tabId });
    setTimeout(() => {
      chrome.action.setBadgeText({ text: '', tabId: tabId });
      window.close();
    }, 800);

  } catch (err) {
    console.error('Auto login failed:', err);
    alert('ID/PW 입력 실패. 페이지를 새로고침 후 다시 시도해주세요.');
  }
}

function updateStatus(key) {
  if (key) {
    statusEl.textContent = `현재 단축키: ${key}`;
  } else {
    statusEl.textContent = `단축키 없음 (팝업에서만 실행)`;
  }
}

// 단축키 입력 시 저장
if (shortcutInput) {
  shortcutInput.addEventListener('input', (e) => {
    const key = e.target.value;

    chrome.storage.sync.set({ shortcutKey: key }, () => {
      statusEl.textContent = key ? `✓ 저장됨: ${key}` : `✓ 단축키 비활성화됨`;
      statusEl.className = 'status saved';

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

  shortcutInput.addEventListener('focus', () => {
    shortcutInput.select();
  });
}

// 자동 이동 확인창 토글
const showConfirmToggle = document.getElementById('showConfirmToggle');
if (showConfirmToggle) {
  showConfirmToggle.addEventListener('change', (e) => {
    const showConfirm = e.target.checked;

    chrome.storage.sync.set({ showConfirmOnNavigate: showConfirm }, () => {
      console.log('[Popup] 자동 이동 설정 저장:', showConfirm ? 'ON (확인창 표시)' : 'OFF (바로 이동)');

      // 모든 탭에 설정 변경 알림
      chrome.tabs.query({}, (tabs) => {
        tabs.forEach(tab => {
          chrome.tabs.sendMessage(tab.id, {
            type: 'UPDATE_NAVIGATE_CONFIRM',
            showConfirm: showConfirm
          }).catch(() => {});
        });
      });

      // 간단한 피드백
      const originalText = e.target.nextElementSibling.textContent;
      e.target.nextElementSibling.textContent = showConfirm ? '✓ 확인창 표시 모드' : '✓ 자동 이동 모드';
      setTimeout(() => {
        e.target.nextElementSibling.textContent = originalText;
      }, 1500);
    });
  });
}

// 쿠팡 전체 자동수집 대시보드 열기
const batchBtn = document.getElementById('batchBtn');
if (batchBtn) {
  batchBtn.addEventListener('click', () => {
    chrome.tabs.create({ url: chrome.runtime.getURL('runner.html') });
    window.close();
  });
}

// 수집 실행 버튼
if (runBtn) {
  runBtn.addEventListener('click', async () => {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

    if (!tab?.id) {
      alert('탭을 찾을 수 없습니다.');
      return;
    }

    runBtn.textContent = '⏳ 실행 중...';
    runBtn.style.background = '#ffa500';
    runBtn.disabled = true;

    chrome.action.setBadgeText({ text: '▶', tabId: tab.id });
    chrome.action.setBadgeBackgroundColor({ color: '#4a90d9', tabId: tab.id });

    try {
      await chrome.tabs.sendMessage(tab.id, { type: 'COLLECT' });

      chrome.action.setBadgeText({ text: '✓', tabId: tab.id });
      chrome.action.setBadgeBackgroundColor({ color: '#28a745', tabId: tab.id });

      setTimeout(() => {
        chrome.action.setBadgeText({ text: '', tabId: tab.id });
      }, 1000);

      window.close();
    } catch (err) {
      try {
        await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          files: ['content.js']
        });

        setTimeout(async () => {
          try {
            await chrome.tabs.sendMessage(tab.id, { type: 'COLLECT' });

            chrome.action.setBadgeText({ text: '✓', tabId: tab.id });
            chrome.action.setBadgeBackgroundColor({ color: '#28a745', tabId: tab.id });

            setTimeout(() => {
              chrome.action.setBadgeText({ text: '', tabId: tab.id });
            }, 1000);

            window.close();
          } catch (err3) {
            showError(tab.id);
          }
        }, 500);
      } catch (err2) {
        showError(tab.id);
      }
    }
  });
}

function showError(tabId) {
  chrome.action.setBadgeText({ text: '✗', tabId: tabId });
  chrome.action.setBadgeBackgroundColor({ color: '#dc3545', tabId: tabId });

  setTimeout(() => {
    chrome.action.setBadgeText({ text: '', tabId: tabId });
  }, 2000);

  runBtn.textContent = '▶ 지금 수집 실행';
  runBtn.style.background = '#4a90d9';
  runBtn.disabled = false;

  alert('이 페이지에서는 수집을 실행할 수 없습니다.\n(배민 또는 쿠팡이츠 페이지에서 실행하세요)');
}
