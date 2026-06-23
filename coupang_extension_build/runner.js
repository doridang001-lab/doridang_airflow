// ===================================================================================
// runner.js — 쿠팡 계정 레벨 오케스트레이터 + 라이브 대시보드
// ===================================================================================
//
// 흐름 (계정당):
//   세션 초기화 → 로그인 URL 이동 → AUTO_LOGIN → orders 도달 대기
//   → COLLECT → MULTISTORE_COMPLETE(orders) 대기
//   → advertising CMG URL 이동 → COLLECT → MULTISTORE_COMPLETE(cmg) 대기
//   → 다음 계정
//
// 기존 _collectOrdersMultiStore() / _collectCMGMultiStore() 는 건드리지 않는다.
// runner는 계정 레벨 루프만 담당한다.

(function () {
  'use strict';

  // ── 상수 ──
  const TEST_ACCOUNT_ID = null; // null = URL ?test=<id> 로 지정, 없으면 첫 계정
  const LOGIN_URL  = 'https://store.coupangeats.com/merchant/login';
  // CMG는 store 도메인 안의 /cmg 경로가 기본 (이미 로그인된 세션 재사용, 빠름)
  // advertising.coupangeats.com 은 여기서 iframe으로 임베딩됨
  const CMG_URL    = 'https://store.coupangeats.com/merchant/management/cmg';
  const MENU_URL   = 'https://store.coupangeats.com/merchant/management/menu';
  const RE_LOGIN   = /store\.coupangeats\.com\/merchant\/login/;
  const RE_ORDERS  = /store\.coupangeats\.com\/merchant\/management\/(orders|home)/;
  // /cmg 경로 OR advertising 직접 접근 모두 허용
  const RE_CMG     = /store\.coupangeats\.com\/merchant\/management\/cmg|advertising\.coupangeats\.com/;
  const RE_MENU    = /store\.coupangeats\.com\/merchant\/management\/menu/;
  const CONTENT_FILES = [
    'content/00_config.js','content/01_utils.js','content/02_baemin.js',
    'content/03_coupangeats.js','content/04_auto_login.js','content/05_main.js'
  ];
  const LOGIN_TIMEOUT   = 90000;   // 로그인 + orders 도달 대기 (ms)
  const COLLECT_TIMEOUT = 300000;  // 수집 완료 대기 — 30개 매장 처리 여유 (ms)
  const CMG_TIMEOUT    = 300000;   // CMG는 6일치 × 4초 = 정상 1-2분 → 5분으로 충분
  const MENU_TIMEOUT   = 60000;    // 메뉴/옵션 DOM 스크랩 — 1분으로 충분
  const DELAY_MIN = 3000;
  const DELAY_MAX = 6000;

  // ── 상태 ──
  let queue        = [];    // [{id, pw, stores:[...], _tr}] — 항상 전체 목록
  let running      = false;
let stopRequested= false;
let startTs      = 0;
let elapsedTimer = null;
let pending         = null;       // 현재 MULTISTORE_COMPLETE 대기 {resolve, settled, payload, _to, _poll}
let autoRetryTimer  = null;       // 배치 완료 후 자동 재시도 대기 타이머
  let retryRound      = 0;          // 실제 재시도 실행 회차
  const MAX_AUTO_RETRY = 10;
  let selectedIndices = new Set();  // 클릭으로 선택된 계정 인덱스 집합 (비어있으면 = 전체)
  let runMode = 'all';
  let runTargetStores = [];
  const storeStatusRows = new Map();
  const manifestRows = [];

  // ── DOM ──
  const $  = (id) => document.getElementById(id);
  const el = {
    rows:$('rows'), log:$('log'), fill:$('progressFill'),
    mProg:$('mProgress'), mOk:$('mOk'), mWarn:$('mWarn'), mFail:$('mFail'),
    mElapsed:$('mElapsed'), mCurrent:$('mCurrent'), modeLabel:$('modeLabel'),
    startBtn:$('startBtn'), topHalfBtn:$('topHalfBtn'), bottomHalfBtn:$('bottomHalfBtn'),
    stopBtn:$('stopBtn'), clearChk:$('clearSessionChk'),
    summary:$('summary'),
    sTotal:$('sTotal'), sOk:$('sOk'), sWarn:$('sWarn'), sFail:$('sFail'), sElapsed:$('sElapsed'),
    manifestBtn:$('manifestBtn'), logBtn:$('logBtn'), retryBtn:$('retryBtn'), menuBtn:$('menuBtn')
  };

  // ── 유틸 ──
  const sleep  = (ms) => new Promise(r => setTimeout(r, ms));
  const rand   = (a,b) => Math.floor(Math.random()*(b-a+1))+a;
  const pad2   = (n) => String(n).padStart(2,'0');
  const fmtMs  = (ms) => { const s=Math.floor(ms/1000); return `${pad2(Math.floor(s/60))}:${pad2(s%60)}`; };
  const nowStr = () => { const d=new Date(); return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`; };
  const getDateStr = (d) => { return `${d.getFullYear()}${pad2(d.getMonth()+1)}${pad2(d.getDate())}`; };
  const getTodayStr = () => getDateStr(new Date());
  const getYesterdayStr = () => { const d=new Date(); d.setDate(d.getDate()-1); return getDateStr(d); };
  const normalizeStoreName = (name) => String(name || '').replace(/\s+/g, ' ').trim();
  const uniq = (arr) => [...new Set(arr.map(normalizeStoreName).filter(Boolean))];

  function log(msg, cls) {
    const div = document.createElement('div');
    div.className = 'log-line' + (cls ? ' ' + cls : '');
    div.innerHTML = `<span class="t">${nowStr()}</span>  ${String(msg).replace(/</g,'&lt;')}`;
    el.log.appendChild(div);
    el.log.scrollTop = el.log.scrollHeight;
  }

  function allStoreNamesSorted() {
    const names = [];
    queue.forEach(acc => acc.stores.forEach(store => names.push(store)));
    return uniq(names).sort((a, b) => a.localeCompare(b, 'ko'));
  }

  function pickTargetStores(mode) {
    const stores = allStoreNamesSorted();
    const mid = Math.ceil(stores.length / 2);
    if (mode === 'top50') return stores.slice(0, mid);
    if (mode === 'bottom50') return stores.slice(mid);
    return stores;
  }

  function modeText(mode) {
    if (mode === 'top50') return '상위 50%';
    if (mode === 'bottom50') return '하위 50%';
    return '전체';
  }

  function accountTargetStores(acc, targetStores = runTargetStores) {
    const targetSet = new Set((targetStores || []).map(normalizeStoreName));
    if (!targetSet.size) return uniq(acc.stores);
    return uniq(acc.stores).filter(store => targetSet.has(normalizeStoreName(store)));
  }

  function targetRunIndices(targetStores = runTargetStores) {
    const picked = [];
    queue.forEach((acc, i) => {
      if (accountTargetStores(acc, targetStores).length > 0) picked.push(i);
    });
    return picked;
  }

  function resetStoreStatus(targetStores) {
    storeStatusRows.clear();
    targetStores.forEach(store => {
      const acc = queue.find(q => q.stores.some(s => normalizeStoreName(s) === normalizeStoreName(store)));
      storeStatusRows.set(normalizeStoreName(store), {
        store,
        account_id: acc?.id || '',
        ordersOk: false,
        cmgOk: false,
        menuOk: false,
        ordersTried: false,
        cmgTried: false,
        menuTried: false,
        note: '',
        collected_at: ''
      });
    });
  }

  function ensureStoreStatus(store, acc) {
    const key = normalizeStoreName(store);
    if (!storeStatusRows.has(key)) {
      storeStatusRows.set(key, {
        store: key,
        account_id: acc?.id || '',
        ordersOk: false,
        cmgOk: false,
        menuOk: false,
        ordersTried: false,
        cmgTried: false,
        menuTried: false,
        note: '',
        collected_at: ''
      });
    }
    return storeStatusRows.get(key);
  }

  function recordCategoryResults(category, acc, targetStores, payload, fallbackOk, note) {
    const rows = Array.isArray(payload?.storeResults) ? payload.storeResults : [];
    const byName = new Map(rows.map(r => [normalizeStoreName(r.storeName), r]));
    const emptyTargetMiss = payload && Array.isArray(payload.storeResults) && rows.length === 0 && payload.storeCount === 0 && targetStores.length > 0;
    targetStores.forEach(store => {
      const row = ensureStoreStatus(store, acc);
      const storeKey = normalizeStoreName(store);
      const result = byName.get(storeKey) || rows.find(r => {
        const resultKey = normalizeStoreName(r.storeName);
        return resultKey.includes(storeKey) || storeKey.includes(resultKey);
      });
      const completed = result ? !!result.completed : (emptyTargetMiss ? false : !!fallbackOk);
      row[`${category}Tried`] = true;
      row[`${category}Ok`] = completed;
      row.note = result?.note || note || row.note || '';
      row.collected_at = new Date().toISOString();
    });
  }

  function markAccountStoresTried(acc, targetStores, note) {
    targetStores.forEach(store => {
      const row = ensureStoreStatus(store, acc);
      ['orders', 'cmg', 'menu'].forEach(category => {
        row[`${category}Tried`] = true;
      });
      row.note = note || row.note || '';
      row.collected_at = new Date().toISOString();
    });
  }

  function incompleteStores() {
    return [...storeStatusRows.values()]
      .filter(r => !(r.ordersOk && r.cmgOk && r.menuOk))
      .map(r => r.store);
  }

  function storeRowsToManifestRows() {
    return [...storeStatusRows.values()].map(r => {
      const status = r.ordersOk && r.cmgOk && r.menuOk
        ? 'ok'
        : (r.ordersOk || r.cmgOk || r.menuOk ? 'warn' : 'fail');
      return {
        account_id: r.account_id,
        stores: r.store,
        status,
        ordersOk: r.ordersOk,
        cmgOk: r.cmgOk,
        menuOk: r.menuOk,
        note: r.note || '',
        duration: '-',
        collected_at: r.collected_at || new Date().toISOString()
      };
    });
  }

  // ── 선택 상태 요약 텍스트 ──
  function selectionLabel() {
    const n = selectedIndices.size;
    if (n === 0) return null;
    if (n === 1) {
      const i = [...selectedIndices][0];
      return queue[i]?.stores.length === 1
        ? queue[i].stores[0]
        : `${queue[i].stores[0]} 외 ${queue[i].stores.length - 1}개`;
    }
    return `${n}개 매장`;
  }

  function applySelectionUI() {
    const label = selectionLabel();
    if (!label) {
      el.startBtn.textContent = '▶ 전체 수집 시작';
      el.modeLabel.textContent = `전체 모드 · ${queue.length}개 계정`;
    } else {
      el.startBtn.textContent = `▶ ${label} 테스트`;
      el.modeLabel.textContent = `선택: ${label} (${selectedIndices.size}개 계정)`;
    }
    // 테이블 행 강조
    queue.forEach((acc, i) => {
      if (!acc._tr) return;
      acc._tr.classList.toggle('selected', selectedIndices.has(i));
    });
    // 필터 패널 강조
    syncFilterSelection();
  }

  // ── 매장 필터 패널 ──
  function buildFilterList(searchText) {
    const filterList = document.getElementById('filterList');
    if (!filterList) return;
    const q = (searchText ?? document.getElementById('filterSearch')?.value ?? '').trim();

    // 전체 매장 → [{store, accIdx}] 가나다순
    const items = [];
    queue.forEach((acc, i) => {
      acc.stores.forEach(store => items.push({ store, accIdx: i }));
    });
    items.sort((a, b) => a.store.localeCompare(b.store, 'ko'));

    const filtered = q ? items.filter(s => s.store.includes(q)) : items;

    filterList.innerHTML = '';
    filtered.forEach(({ store, accIdx }) => {
      const div = document.createElement('div');
      div.className = 'fi' + (selectedIndices.has(accIdx) ? ' active' : '');
      div.textContent = store;
      div.title = queue[accIdx]?.id || '';
      div.dataset.accIdx = accIdx;
      div.addEventListener('click', () => { if (!running) selectAccount(accIdx); });
      filterList.appendChild(div);
    });
  }

  // 필터 패널 강조만 갱신 (전체 재빌드 없이)
  function syncFilterSelection() {
    const filterList = document.getElementById('filterList');
    if (!filterList) return;
    filterList.querySelectorAll('.fi').forEach(div => {
      div.classList.toggle('active', selectedIndices.has(Number(div.dataset.accIdx)));
    });
  }

  // ── 테이블 ──
  function buildRows() {
    el.rows.innerHTML = '';
    queue.forEach((acc, i) => {
      const tr = document.createElement('tr');
      tr.id = `row-${i}`;
      const storeLabel = acc.stores.length === 1
        ? acc.stores[0]
        : `${acc.stores[0]} 외 ${acc.stores.length - 1}개`;
      const storeTip = acc.stores.join('\n');
      tr.innerHTML =
        `<td style="color:#6a7488;font-size:12px">${i+1}</td>` +
        `<td title="${storeTip}">` +
          `<div style="font-size:13px;font-weight:600">${storeLabel}</div>` +
          `<div style="font-size:11px;color:#6a7488;margin-top:2px">${acc.id}</div>` +
        `</td>` +
        `<td><span class="badge b-wait" id="ro-${i}">대기</span></td>` +
        `<td><span class="badge b-wait" id="rc-${i}">대기</span></td>` +
        `<td><span class="badge b-wait" id="rm-${i}">대기</span></td>` +
        `<td id="rd-${i}" style="font-size:12px">-</td>` +
        `<td id="rn-${i}" style="font-size:12px;color:#6a7488"></td>`;
      // 행 클릭 → 해당 계정 테스트 선택
      tr.addEventListener('click', () => { if (!running) selectAccount(i); });
      el.rows.appendChild(tr);
      acc._tr = tr;
    });
    // 이전 선택 상태 복원
    selectedIndices.forEach(i => {
      if (queue[i]?._tr) queue[i]._tr.classList.add('selected');
    });
    updateMeta();
  }

  // 토글 방식 — 이미 선택된 계정이면 해제, 아니면 추가
  function selectAccount(i) {
    if (selectedIndices.has(i)) {
      selectedIndices.delete(i);
      log(`선택 해제: ${queue[i].stores[0]}`, 'info');
    } else {
      selectedIndices.add(i);
      queue[i]._tr?.scrollIntoView({ block: 'nearest' });
      log(`선택 추가: ${queue[i].stores[0]}`, 'info');
    }
    applySelectionUI();
  }

  const BADGES = {
    wait  : ['b-wait','대기'],   run:['b-run','수집중'],
    ok    : ['b-ok',  '완료'],   warn:['b-warn','경고'],
    fail  : ['b-fail', '실패'],  skip:['b-wait','건너뜀']
  };
  function setBadge(id, key) {
    const el2 = document.getElementById(id);
    if (!el2) return;
    const [cls, txt] = BADGES[key] || BADGES.wait;
    el2.className = 'badge ' + cls;
    el2.textContent = txt;
  }
  function setRow(i, {orders, cmg, menu, dur, note, current, promote}={}) {
    if (orders) setBadge(`ro-${i}`, orders);
    if (cmg)    setBadge(`rc-${i}`, cmg);
    if (menu)   setBadge(`rm-${i}`, menu);
    if (dur != null) { const d = $(`rd-${i}`); if(d) d.textContent = dur; }
    if (note != null) { const n = $(`rn-${i}`); if(n) n.textContent = note; }
    el.rows.querySelectorAll('tr.current').forEach(r => r.classList.remove('current'));
    if (current && queue[i]?._tr) queue[i]._tr.classList.add('current');
    // 수집 완료 → 행을 테이블 최상단으로 이동
    if (promote && queue[i]?._tr) el.rows.prepend(queue[i]._tr);
  }

  function counts() {
    if (storeStatusRows.size > 0) {
      let ok=0,warn=0,fail=0,done=0;
      storeStatusRows.forEach(r => {
        const tried = r.ordersTried || r.cmgTried || r.menuTried;
        if (!tried) return;
        done++;
        if (r.ordersOk && r.cmgOk && r.menuOk) ok++;
        else if (r.ordersOk || r.cmgOk || r.menuOk) warn++;
        else fail++;
      });
      return {ok,warn,fail,done};
    }
    let ok=0,warn=0,fail=0;
    manifestRows.forEach(r => { if(r.status==='ok') ok++; else if(r.status==='warn') warn++; else fail++; });
    return {ok,warn,fail,done:manifestRows.length};
  }
  function updateMeta() {
    const c = counts(), n = storeStatusRows.size || queue.length;
    el.mProg.textContent = `${c.done} / ${n}`;
    el.mOk.textContent = c.ok; el.mWarn.textContent = c.warn; el.mFail.textContent = c.fail;
    el.fill.style.width = n ? `${Math.round(c.done/n*100)}%` : '0%';
  }

  // ── 탭 유틸 ──
  async function getUrl(tabId) {
    try { const t = await chrome.tabs.get(tabId); return t.url||''; } catch(_) { return ''; }
  }
  async function waitUrl(tabId, regexMap, ms) {
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      if (stopRequested) return null;
      const url = await getUrl(tabId);
      for (const [key, re] of Object.entries(regexMap)) {
        const m = url.match(re);
        if (m) return {key, match:m, url};
      }
      await sleep(800);
    }
    return null;
  }

  // [Bug2/3 fix] URL 매칭 후 실제 document.readyState === 'complete' 까지 대기
  // executeScript가 페이지 로딩 중 실패해도 try/catch로 재시도
  async function waitForPageLoad(tabId, ms = 10000) {
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      if (stopRequested) return false;
      try {
        const res = await chrome.scripting.executeScript({
          target: { tabId },
          func: () => document.readyState
        });
        if (res?.[0]?.result === 'complete') return true;
      } catch(_) {}
      await sleep(600);
    }
    return false;
  }

  // reload 시작 보장 후 readyState 변화 감지 (구 페이지 false positive 방지)
  async function waitForReload(tabId, ms = 15000) {
    const deadline = Date.now() + ms;
    let sawLoading = false;
    const earlyDeadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      if (stopRequested) return false;
      try {
        const res = await chrome.scripting.executeScript({
          target: { tabId },
          func: () => document.readyState
        });
        const st = res?.[0]?.result;
        if (st === 'loading' || st === 'interactive') sawLoading = true;
        if (sawLoading && st === 'complete') return true;
        // 5초 지나도 loading 감지 못하면 그냥 통과 (이미 로드 완료)
        if (!sawLoading && st === 'complete' && Date.now() > earlyDeadline) return true;
      } catch(_) {}
      await sleep(400);
    }
    return false;
  }

  // 로그인 폼(ID + PW 인풋)이 실제로 DOM에 나타날 때까지 대기
  // 팝업에서 사용자가 직접 클릭하는 시점 = 눈으로 폼이 보일 때 → 동일 조건
  // readyState만으로는 React 초기화 전일 수 있어 실제 요소 존재 여부로 판단
  async function waitForLoginReady(tabId, maxRetries = 10, perRetryMs = 10000) {
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      if (stopRequested) return false;
      if (attempt > 0) {
        log(`[F5 ${attempt}/${maxRetries}] 로그인 폼 미감지 — 새로고침...`);
        try { await chrome.tabs.reload(tabId); } catch(_) {}
        await sleep(2000);
      }
      const deadline = Date.now() + perRetryMs;
      while (Date.now() < deadline) {
        if (stopRequested) return false;
        try {
          const res = await chrome.scripting.executeScript({
            target: { tabId },
            func: () => {
              const pw = document.querySelector('input[type="password"]');
              const id = document.querySelector('input[type="text"], input[type="email"]');
              // offsetParent !== null → 실제로 화면에 렌더링됨 (hidden 아님)
              return !!(pw && id && pw.offsetParent !== null);
            }
          });
          if (res?.[0]?.result === true) return true;
        } catch(_) {}
        await sleep(800);
      }
    }
    log('로그인 폼 대기 타임아웃', 'err');
    return false;
  }

  // 로딩이 느릴 때 F5 새로고침 후 재시도 — 최대 maxRetries번
  // 각 시도마다 perRetryMs 동안 readyState=complete 를 기다리고,
  // 그래도 안 되면 chrome.tabs.reload() → 다시 대기
  async function waitForPageLoadWithRetry(tabId, label, maxRetries = 10, perRetryMs = 10000) {
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      if (stopRequested) return false;
      if (attempt > 0) {
        log(`[F5 ${attempt}/${maxRetries}] ${label} 로딩 지연 — 새로고침...`);
        try { await chrome.tabs.reload(tabId); } catch(_) {}
        await sleep(2000); // 재로딩 시작 대기
      }
      const loaded = await waitForPageLoad(tabId, perRetryMs);
      if (loaded) return true;
      if (stopRequested) return false;
    }
    log(`${label} F5 ${maxRetries}회 시도 후 로딩 실패`, 'err');
    return false;
  }

  async function injectContent(tabId) {
    try { await chrome.tabs.sendMessage(tabId, {type:'PING'}); return true; }
    catch(_) {
      try {
        await chrome.scripting.executeScript({target:{tabId}, files:CONTENT_FILES});
        await sleep(500);
        return true;
      } catch(e) { log(`content 주입 실패: ${e.message}`, 'err'); return false; }
    }
  }
  // WUJIE-APP(CMG 광고 iframe)이 shadowRoot 안에 드롭다운+달력 모두 렌더링할 때까지 폴링
  // form AND calendar 둘 다 필요 — OR이면 form만 로드돼도 통과 → _readCMGDateRange null → prompt() 10분 블록
  async function waitForWujieApp(tabId, ms = 30000) {
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      if (stopRequested) return false;
      try {
        const res = await chrome.scripting.executeScript({
          target: { tabId },
          func: () => {
            const wa = document.querySelector('WUJIE-APP');
            if (!wa || !wa.shadowRoot) return false;
            // form.cmgeats-store-dropdown이 실제로 있어야 true (없으면 _getCMGMatchingStores가 [] 반환해 false positive 발생)
            // AND 조건: form(매장드롭다운) + calendar(날짜선택) 모두 확인
            return !!(wa.shadowRoot.querySelector('form.cmgeats-store-dropdown') &&
                      wa.shadowRoot.querySelector('.calendar-dropdown-input'));
          }
        });
        if (res && res[0] && res[0].result) return true;
      } catch(_) {}
      await sleep(1200);
    }
    return false;
  }

  async function clearSession() {
    return new Promise(resolve => {
      try {
        chrome.browsingData.remove(
          {origins:['https://store.coupangeats.com','https://advertising.coupangeats.com']},
          {cookies:true, localStorage:true, cacheStorage:true},
          () => resolve(true)
        );
      } catch(e) { log(`세션 초기화 오류(무시): ${e.message}`); resolve(false); }
    });
  }

  // ── MULTISTORE_COMPLETE 수신 ──
  // background.js가 content→runner 탭으로 중계한다.
  // runner 탭은 확장 페이지이므로 chrome.runtime.onMessage로 바로 받는다.
  chrome.runtime.onMessage.addListener((msg) => {
    if (msg?.type === 'LIVE_LOG') { log(msg.msg); return; }
    if (!pending || pending.settled) return;
    if (msg?.type === 'MULTISTORE_COMPLETE') {
      pending.settled = true;
      pending.payload = msg.payload || {};
      clearTimeout(pending._to);
      clearInterval(pending._poll);  // [Bug1 fix] 폴 인터벌도 정리
      pending.resolve();
    }
  });

  // [Bug1 fix] waitMultistore: stopRequested를 500ms마다 폴링해 즉시 resolve
  // 기존: MULTISTORE_COMPLETE 도착 or 600초 타임아웃만 → 중단 불가
  // 수정: setInterval로 stopRequested 감지 → 즉각 중단
  function waitMultistore(category, timeoutMs = COLLECT_TIMEOUT) {
    return new Promise(resolve => {
      const obj = { settled: false, payload: {}, resolve, _to: null, _poll: null };
      pending = obj;
      // 500ms마다 중단 요청 확인
      obj._poll = setInterval(() => {
        if (stopRequested && !obj.settled) {
          obj.settled = true;
          obj.payload = { category, stopped: true };
          clearInterval(obj._poll);
          clearTimeout(obj._to);
          resolve();
        }
      }, 500);
      obj._to = setTimeout(() => {
        if (!obj.settled) {
          obj.settled = true;
          obj.payload = { category, timeout: true };
          clearInterval(obj._poll);
          resolve();
        }
      }, timeoutMs);
    });
  }

  // 로그인 결과 감지: orders URL 도달→'ok' / 에러 스팬→'error' / 타임아웃→'timeout'
  // Coupang 로그인 실패 시 <span class="login-error-text"> 가 DOM에 나타남
  async function waitForLoginResult(tabId, ms) {
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      if (stopRequested) return 'stop';
      const url = await getUrl(tabId);
      if (RE_ORDERS.test(url)) return 'ok';
      if (RE_LOGIN.test(url)) {
        try {
          const res = await chrome.scripting.executeScript({
            target: { tabId },
            func: () => {
              const s = document.querySelector('span.login-error-text');
              return !!(s && s.offsetParent !== null && s.textContent.trim().length > 0);
            }
          });
          if (res?.[0]?.result) return 'error';
        } catch(_) {}
      }
      await sleep(800);
    }
    return 'timeout';
  }

  // ── 계정 1개 처리 ──
  // skip.orders=true → orders 수집 건너뜀(이미 완료)
  // skip.cmg=true    → CMG   수집 건너뜀(이미 완료)
  // skip.menu=true   → 메뉴  수집 건너뜀(이미 완료)
  // 반환: {status, dur, ordersOk, cmgOk, menuOk}
  async function processAccount(i, workTabId, skip = {}, targetStoresArg = null) {
    const acc = queue[i];
    const targetStores = accountTargetStores(acc, targetStoresArg || runTargetStores);
    if (targetStores.length === 0) {
      return {status:'skip', note:'대상 매장 없음', ordersOk:true, cmgOk:true, menuOk:true};
    }
    const t0 = Date.now();
    const storeDisplay = acc.stores.length === 1 ? acc.stores[0] : `${acc.stores[0]} 외 ${acc.stores.length-1}개`;
    el.mCurrent.textContent = storeDisplay;
    const retryTag = (skip.orders || skip.cmg) ? ' [재시도]' : '';
    log(`──── [${i+1}/${queue.length}] ${storeDisplay} (${acc.id})${retryTag} ────`, 'info');
    if (acc.stores.length > 1) log('[' + acc.id + '] 수집 매장: ' + acc.stores.join(' / '), 'info');
    log('[' + acc.id + '] 이번 대상: ' + targetStores.join(' / '), 'info');

    // 0) 세션 초기화
    if (el.clearChk.checked) {
      setRow(i, {orders:'wait', note:'세션 초기화중', current:true, promote:true});
      await clearSession();
      await sleep(500);
    }

    // 1) 로그인 페이지 이동
    setRow(i, {orders:'run', note:'로그인 페이지', current:true, promote:true});
    await chrome.tabs.update(workTabId, {url: LOGIN_URL});
    let r = await waitUrl(workTabId, {login:RE_LOGIN, orders:RE_ORDERS}, 25000);
    if (stopRequested) {
      markAccountStoresTried(acc, targetStores, '중단됨');
      return {status:'fail', note:'중단됨', ordersOk:false, cmgOk:false};
    }
    if (!r) {
      log(`[${acc.id}] 로그인 페이지 로딩 타임아웃`, 'err');
      markAccountStoresTried(acc, targetStores, '로그인 페이지 타임아웃');
      return {status:'fail', note:'로그인 페이지 타임아웃', ordersOk:false, cmgOk:false};
    }

    // 2) 자동 로그인 (이미 orders면 생략)
    if (r.key === 'login') {
      setRow(i, {orders:'run', note:'로그인중', current:true});
      await waitForLoginReady(workTabId);

      // inject + sendMessage + 에러 스팬 감지 → 최대 5회 F5 재시도
      const MAX_LOGIN = 5;
      let loginDone = false;
      for (let attempt = 1; attempt <= MAX_LOGIN; attempt++) {
        if (stopRequested) {
          markAccountStoresTried(acc, targetStores, '중단됨');
          return {status:'fail', note:'중단됨', ordersOk:false, cmgOk:false};
        }
        if (attempt > 1) {
          if (RE_ORDERS.test(await getUrl(workTabId))) { loginDone = true; break; }
          log(`[${acc.id}] 로그인 재시도 (${attempt}/${MAX_LOGIN}) — F5...`);
          try { await chrome.tabs.reload(workTabId); } catch(_) {}
          await sleep(2000);  // reload 시작 대기 — 구 페이지 form false-positive 방지
          await waitForLoginReady(workTabId);
        }
        await injectContent(workTabId);
        await sleep(300);
        try {
          await chrome.tabs.sendMessage(workTabId, {
            type:'AUTO_LOGIN', platform:'coupang',
            id:acc.id, pw:acc.pw, storeName:acc.stores[0]||'', autoClick:true
          });
        } catch(e) { log(`[${acc.id}] AUTO_LOGIN 오류 (${attempt}/${MAX_LOGIN}): ${e.message}`, 'err'); }

        // orders 도달 또는 에러 스팬 감지 대기 (30초)
        const result = await waitForLoginResult(workTabId, 30000);
        if (result === 'ok')  { loginDone = true; break; }
        if (result === 'error') {
          log(`[${acc.id}] 로그인 에러 스팬 감지 — 재시도 (${attempt}/${MAX_LOGIN})`,'err');
          continue;  // → F5 at top of loop
        }
        // 'timeout' or 'stop'
        if (RE_ORDERS.test(await getUrl(workTabId))) { loginDone = true; break; }
      }

      if (!loginDone) {
        if (stopRequested) {
          markAccountStoresTried(acc, targetStores, '중단됨');
          return {status:'fail', note:'중단됨', ordersOk:false, cmgOk:false};
        }
        const curUrl = await getUrl(workTabId);
        const note = RE_LOGIN.test(curUrl) ? '로그인 실패(ID/PW 또는 Akamai)' : 'orders 페이지 미도달';
        log(`[${acc.id}] ${note}`, 'err');
        markAccountStoresTried(acc, targetStores, note);
        return {status:'fail', note, ordersOk:false, cmgOk:false};
      }
    }
    log(`[${acc.id}] orders 페이지 도달`, 'ok');

    // 3) 주문서(orders) 수집
    let ordersOk = false;
    if (!skip.orders) {
      setRow(i, {orders:'run', note:'orders 페이지로 이동', current:true});
      // home 착지 케이스 대비: /orders 경로가 아니면 명시 이동 후 대기
      const curUrl = await getUrl(workTabId);
      if (!curUrl.includes('/merchant/management/orders')) {
        await chrome.tabs.update(workTabId, {url: 'https://store.coupangeats.com/merchant/management/orders'});
        const ordersNav = await waitUrl(workTabId, {orders: /merchant\/management\/orders/}, 20000);
        if (!ordersNav || stopRequested) {
          const note = stopRequested ? '중단됨' : 'orders 페이지 이동 실패';
          log(`[${acc.id}] ${note}`, 'err');
          setRow(i, {orders:'fail', note});
          recordCategoryResults('orders', acc, targetStores, null, false, note);
          return {status:'fail', note, ordersOk:false, cmgOk:false};
        }
      }
      await waitForPageLoadWithRetry(workTabId, 'orders 페이지');
      setRow(i, {orders:'run', note:'orders 수집중', current:true});
      await injectContent(workTabId);
      await sleep(600);
      try { await chrome.tabs.sendMessage(workTabId, {type:'COLLECT', source:'batch', targetStores}); }
      catch(e) { log(`[${acc.id}] COLLECT 전송 실패: ${e.message}`, 'err'); }

      let ordersResult = await waitMultistore('orders');
      const op = (pending && pending.payload) || ordersResult || {};
      if (op.skipped) {
        // targetStores가 있는데 건너뜀 → 재시도 대상 (매장명 탐지 실패 등)
        // targetStores가 없고 'not_target_brand' → 진짜 브랜드 아님 → 건너뜀=성공
        const isGenuineSkip = op.reason === 'not_target_brand' && targetStores.length === 0;
        ordersOk = isGenuineSkip;
        if (isGenuineSkip) {
          log('[' + acc.id + '] orders 건너뜀(브랜드 아님): ' + (op.curStoreName || '미탐지'), 'info');
          setRow(i, {orders:'skip', note: op.curStoreName ? '브랜드 아님: ' + op.curStoreName : '매장명 미탐지'});
        } else {
          log('[' + acc.id + '] orders 건너뜀→재시도: ' + (op.reason || '') + ' (' + (op.curStoreName || '미탐지') + ')', 'info');
          setRow(i, {orders:'warn', note: '건너뜀→재시도'});
        }
      } else {
        ordersOk = !op.timeout && !op.stopped;
        log('[' + acc.id + '] orders ' + (ordersOk ? '완료' : (op.timeout ? '타임아웃' : '중단')) + ' — ' + (op.completedCount||0) + '/' + (op.storeCount||0) + '개',
            ordersOk ? 'ok' : 'err');
        setRow(i, {orders: ordersOk ? 'ok' : (op.timeout ? 'fail' : 'warn')});
      }
      recordCategoryResults('orders', acc, targetStores, op, ordersOk, op.timeout ? 'orders 타임아웃' : '');
      if (stopRequested) return {status:'fail', note:'중단됨', ordersOk, cmgOk:false};
    } else {
      ordersOk = true;
      setRow(i, {orders:'skip'});
      recordCategoryResults('orders', acc, targetStores, null, true, 'orders 이전 완료');
      log(`[${acc.id}] orders 건너뜀 (이전 수집 완료)`, 'info');
    }

    // 4) CMG(마케팅) 수집 — 실패 시 F5 새로고침 후 최대 3회 시도
    let cmgOk = false;
    if (!skip.cmg) {
      setRow(i, {cmg:'run', note:'CMG 이동중', current:true});
      await chrome.tabs.update(workTabId, {url: CMG_URL});
      const cmgNav = await waitUrl(workTabId, {cmg:RE_CMG}, 45000);
      if (stopRequested) return {status:'fail', note:'중단됨', ordersOk, cmgOk:false};

      if (!cmgNav) {
        log(`[${acc.id}] CMG 페이지 이동 실패 — 건너뜀`, 'err');
        setRow(i, {cmg:'fail', note:'CMG 이동 실패'});
        recordCategoryResults('cmg', acc, targetStores, null, false, 'CMG 이동 실패');
      } else {
        const MAX_CMG = 3;
        let lastCp = {};
        for (let attempt = 1; attempt <= MAX_CMG && !cmgOk && !stopRequested; attempt++) {
          if (attempt > 1) {
            log(`[${acc.id}] CMG F5 재시도 (${attempt}/${MAX_CMG})...`);
            try { await chrome.tabs.reload(workTabId, {bypassCache: true}); } catch(_) {}
            await sleep(3000);
            await waitForReload(workTabId, 15000);
            await waitForPageLoadWithRetry(workTabId, `CMG F5 재로딩 (${attempt}/${MAX_CMG})`);
          } else {
            setRow(i, {cmg:'run', note:'CMG 로딩 대기중', current:true});
            await waitForPageLoadWithRetry(workTabId, 'CMG 페이지');
          }

          log(`[${acc.id}] CMG WUJIE-APP 로딩 대기... (${attempt}/${MAX_CMG})`);
          const wujieReady = await waitForWujieApp(workTabId);
          if (!wujieReady) {
            log(`[${acc.id}] ⚠️ WUJIE-APP 미준비 (${attempt}/${MAX_CMG})`, 'err');
            lastCp = { category: 'cmg', error: true };
            continue;
          }

          log(`[${acc.id}] WUJIE-APP 로딩 확인 (${attempt}/${MAX_CMG})`, 'ok');
           await sleep(600);
          setRow(i, {cmg:'run', note: attempt > 1 ? `CMG 재시도 ${attempt}/${MAX_CMG}` : 'CMG 수집중', current:true});
          await injectContent(workTabId);
           await sleep(200);
          try { await chrome.tabs.sendMessage(workTabId, {type:'COLLECT', source:'batch', targetStores}); }
          catch(e) { log(`[${acc.id}] CMG COLLECT 전송 실패: ${e.message}`, 'err'); lastCp = {category:'cmg', error:true}; continue; }

          const cmgResult = await waitMultistore('cmg', CMG_TIMEOUT);
          lastCp = (pending && pending.payload) || cmgResult || {};

          if (lastCp.skipped) {
            const isGenuineCmgSkip = lastCp.reason === 'not_target_brand' && targetStores.length === 0;
            cmgOk = isGenuineCmgSkip;
            if (isGenuineCmgSkip) {
              log(`[${acc.id}] CMG 건너뜀(브랜드 아님): ${lastCp.curStoreName||'미탐지'}`, 'info');
              setRow(i, {cmg:'skip', note: lastCp.curStoreName ? '브랜드 아님: ' + lastCp.curStoreName : '매장명 미탐지'});
            } else {
              log(`[${acc.id}] CMG 건너뜀→재시도: ${lastCp.reason||''} (${lastCp.curStoreName||'미탐지'})`, 'info');
              setRow(i, {cmg:'warn', note: '건너뜀→재시도'});
            }
          } else {
            cmgOk = !lastCp.timeout && !lastCp.stopped && !lastCp.error;
            const rs = cmgOk ? '완료' : (lastCp.error ? '매장목록 없음(광고 미집행)' : lastCp.timeout ? '타임아웃' : '중단');
            log(`[${acc.id}] CMG ${attempt}/${MAX_CMG} ${rs} — ${lastCp.completedCount||0}/${lastCp.storeCount||0}개`,
                cmgOk ? 'ok' : 'err');
            if (cmgOk) {
              setRow(i, {cmg:'ok'});
            } else {
              setRow(i, {cmg: lastCp.error ? 'warn' : 'fail'});
              if (lastCp.stopped) break;
            }
          }
        }
        recordCategoryResults('cmg', acc, targetStores, lastCp, cmgOk, lastCp?.error ? 'CMG 오류' : '');
        if (stopRequested) return {status:'fail', note:'중단됨', ordersOk, cmgOk};
      }
    } else {
      cmgOk = true;
      setRow(i, {cmg:'skip'});
      recordCategoryResults('cmg', acc, targetStores, null, true, 'CMG 이전 완료');
      log(`[${acc.id}] CMG 건너뜀 (이전 수집 완료)`, 'info');
    }

    // 4.5) orders 즉시 재시도 — CMG 완료 후 로그인 세션 유지 상태
    if (!skip.orders && !ordersOk && !stopRequested) {
      log(`[${acc.id}] orders 실패 → CMG 완료 직후 즉시 재시도 (로그인 유지)`, 'info');
      setRow(i, {orders:'run', note:'orders 즉시 재시도', current:true});
      await chrome.tabs.update(workTabId, {url: 'https://store.coupangeats.com/merchant/management/orders'});
      const ordersNav = await waitUrl(workTabId, {orders: RE_ORDERS}, 25000);
      if (ordersNav && !stopRequested) {
        await waitForPageLoadWithRetry(workTabId, 'orders 재시도 페이지');
        await injectContent(workTabId);
        await sleep(1500);
        try { await chrome.tabs.sendMessage(workTabId, {type:'COLLECT', source:'batch', targetStores}); }
        catch(e) { log(`[${acc.id}] orders 재시도 COLLECT 실패: ${e.message}`, 'err'); }
        const retryResult = await waitMultistore('orders', CMG_TIMEOUT);
        const rp = (pending && pending.payload) || retryResult || {};
        if (rp.skipped) {
          ordersOk = true;
          log(`[${acc.id}] orders 재시도 건너뜀: ${rp.reason||'브랜드 아님'}`, 'info');
          setRow(i, {orders:'skip'});
        } else {
          ordersOk = !rp.timeout && !rp.stopped;
          log(`[${acc.id}] orders 즉시재시도 ${ordersOk ? '완료' : (rp.timeout ? '타임아웃' : '중단')} — ${rp.completedCount||0}/${rp.storeCount||0}개`,
              ordersOk ? 'ok' : 'err');
          setRow(i, {orders: ordersOk ? 'ok' : (rp.timeout ? 'fail' : 'warn')});
        }
        recordCategoryResults('orders', acc, targetStores, rp, ordersOk, rp.timeout ? 'orders 재시도 타임아웃' : '');
      } else {
        log(`[${acc.id}] orders 재시도 페이지 이동 실패`, 'err');
      }
    }

    // 5) 상품관리(메뉴+옵션) 수집
    let menuOk = false;
    if (!skip.menu) {
      setRow(i, {menu:'run', note:'메뉴 이동중', current:true});
      await chrome.tabs.update(workTabId, {url: MENU_URL});
      const menuNav = await waitUrl(workTabId, {menu: RE_MENU}, 15000);
      if (!menuNav || stopRequested) {
        log(`[${acc.id}] 메뉴 페이지 이동 실패`, 'err');
        setRow(i, {menu:'fail', note:'메뉴 이동 실패'});
        recordCategoryResults('menu', acc, targetStores, null, false, '메뉴 이동 실패');
      } else {
        await waitForPageLoadWithRetry(workTabId, '메뉴 페이지');
        await injectContent(workTabId);
        await sleep(1000);
        setRow(i, {menu:'run', note:'상품 수집중', current:true});
        try { await chrome.tabs.sendMessage(workTabId, {type:'COLLECT', source:'batch', targetStores}); }
        catch(e) { log(`[${acc.id}] MENU COLLECT 실패: ${e.message}`, 'err'); }
        const menuResult = await waitMultistore('menu', MENU_TIMEOUT);
        const mp = (pending && pending.payload) || menuResult || {};
        if (mp.skipped) {
          const isGenuineMenuSkip = mp.reason === 'not_target_brand' && targetStores.length === 0;
          menuOk = isGenuineMenuSkip;
          if (isGenuineMenuSkip) {
            log(`[${acc.id}] 메뉴 건너뜀(브랜드 아님): ${mp.curStoreName||'미탐지'}`, 'info');
            setRow(i, {menu:'skip', note: mp.curStoreName ? '브랜드 아님: ' + mp.curStoreName : '매장명 미탐지'});
          } else {
            log(`[${acc.id}] 메뉴 건너뜀→재시도: ${mp.reason||''} (${mp.curStoreName||'미탐지'})`, 'info');
            setRow(i, {menu:'warn', note: '건너뜀→재시도'});
          }
        } else {
          menuOk = !mp.timeout && !mp.stopped && !mp.error;
          log(`[${acc.id}] 메뉴 ${menuOk ? '완료' : (mp.timeout ? '타임아웃' : '실패')} — ${mp.completedCount||0}/${mp.storeCount||0}개`,
              menuOk ? 'ok' : 'err');
          setRow(i, {menu: menuOk ? 'ok' : (mp.timeout ? 'fail' : 'warn')});
        }
        recordCategoryResults('menu', acc, targetStores, mp, menuOk, mp.timeout ? '메뉴 타임아웃' : '');
      }
      if (stopRequested) return {status:'fail', note:'중단됨', ordersOk, cmgOk, menuOk:false};
    } else {
      menuOk = true;
      setRow(i, {menu:'skip'});
      recordCategoryResults('menu', acc, targetStores, null, true, '메뉴 이전 완료');
      log(`[${acc.id}] 메뉴 건너뜀 (이전 수집 완료)`, 'info');
    }

    const dur = fmtMs(Date.now() - t0);
    const status = ordersOk && cmgOk && menuOk ? 'ok' : (ordersOk || cmgOk || menuOk ? 'warn' : 'fail');
    setRow(i, {dur, note:'', current:false, promote:true});  // 완료 → 최상단 이동
    return {status, dur, ordersOk, cmgOk, menuOk};
  }

  // ── 배치 실행 ──
  async function runBatch(mode = 'all') {
    if (running || !queue.length) return;
    running = true; stopRequested = false;
    runMode = mode;
    if (autoRetryTimer) { clearInterval(autoRetryTimer); autoRetryTimer = null; }
    retryRound = 0;
    manifestRows.length = 0;
    if (mode === 'all' && selectedIndices.size > 0) {
      runTargetStores = uniq([...selectedIndices].flatMap(i => queue[i]?.stores || []));
    } else {
      runTargetStores = pickTargetStores(mode);
    }
    resetStoreStatus(runTargetStores);
    el.startBtn.disabled = true; el.topHalfBtn.disabled = true; el.bottomHalfBtn.disabled = true; el.stopBtn.disabled = false;
    el.summary.classList.remove('show');
    startTs = Date.now();
    elapsedTimer = setInterval(() => { el.mElapsed.textContent = fmtMs(Date.now()-startTs); }, 1000);

    // 어제 실패 계정 백필 큐 확인
    const bfqResult = await chrome.storage.local.get(['ce_backfill_queue']);
    const bfq = bfqResult.ce_backfill_queue;
    const bfStores = Array.isArray(bfq?.failedStores) ? bfq.failedStores : [];
    const bfAccIds = Array.isArray(bfq?.failedAccIds) ? bfq.failedAccIds : [];
    if (bfq && bfq.enqueuedDate === getYesterdayStr() && (bfStores.length > 0 || bfAccIds.length > 0)) {
      const backfillTargets = bfStores.length > 0
        ? bfStores
        : uniq(queue.filter(q => bfAccIds.includes(q.id)).flatMap(q => q.stores));
      log(`📋 어제(${bfq.targetDate}) 미완료 ${backfillTargets.length}개 매장 백필 시작...`, 'info');
      await chrome.storage.local.remove(['ce_backfill_queue']);
      resetStoreStatus(backfillTargets);
      let bfWorkTab;
      try {
        bfWorkTab = await chrome.tabs.create({url:'about:blank', active:true});
        const bfRunIndices = targetRunIndices(backfillTargets);
        for (const qi of bfRunIndices) {
          if (stopRequested) break;
          const targetStores = accountTargetStores(queue[qi], backfillTargets);
          try { await processAccount(qi, bfWorkTab.id, {}, targetStores); }
          catch(e) {
            log(`[${queue[qi].id}] 백필 예외: ${e.message}`, 'err');
            markAccountStoresTried(queue[qi], targetStores, e.message);
          }
          updateMeta();
          if (!stopRequested) await sleep(rand(DELAY_MIN, DELAY_MAX));
        }
      } catch(e) { log(`백필 예외: ${e.message}`, 'err'); }
      finally { if (bfWorkTab) { try { await chrome.tabs.remove(bfWorkTab.id); } catch(_) {} } }
      log('백필 완료 — 정규 배치 시작', 'info');
      manifestRows.length = 0;
      storeStatusRows.clear();
    }
    resetStoreStatus(runTargetStores);

    const runIndices = mode === 'all' && selectedIndices.size > 0
      ? [...selectedIndices].sort((a, b) => a - b)
      : targetRunIndices(runTargetStores);
    log(`${modeText(mode)} 배치 시작 — ${runTargetStores.length}개 매장 / ${runIndices.length}개 계정${selectedIndices.size > 0 && mode === 'all' ? ' (선택 테스트)' : ''}`, 'info');

    // [Bug5 fix] try/finally로 예외 발생 시에도 반드시 finishBatch 호출
    let workTab;
    try {
      workTab = await chrome.tabs.create({url:'about:blank', active:true});

      // [Bug4 fix] runIndices 기준 인덱스(ri) 사용 — queue.length 아닌 runIndices.length 비교
      for (let ri = 0; ri < runIndices.length; ri++) {
        const i = runIndices[ri];
        if (stopRequested) { log('중단 요청 — 종료', 'err'); break; }
        let res;
        const targetStores = accountTargetStores(queue[i], runTargetStores);
        try { res = await processAccount(i, workTab.id, {}, targetStores); }
        catch(e) {
          log(`[${queue[i].id}] 예외: ${e.message}`, 'err');
          markAccountStoresTried(queue[i], targetStores, e.message);
          res = {status:'fail', note:e.message};
        }

        manifestRows.push({
          account_id: queue[i].id,
          stores: targetStores.join('|'),
          status: res.status,
          ordersOk: res.ordersOk ?? false,
          cmgOk:    res.cmgOk    ?? false,
          menuOk:   res.menuOk   ?? false,
          note: res.note||'',
          duration: res.dur||'-',
          collected_at: new Date().toISOString()
        });
        updateMeta();

        // 마지막 계정이 아닐 때만 대기 (runIndices 기준)
        if (ri < runIndices.length - 1 && !stopRequested) {
          const d = rand(DELAY_MIN, DELAY_MAX);
          log(`다음 계정까지 ${Math.round(d/1000)}초 대기...`);
          await sleep(d);
        }
      }
    } catch(e) {
      log(`배치 예외: ${e.message}`, 'err');
    } finally {
      if (workTab) { try { await chrome.tabs.remove(workTab.id); } catch(_) {} }
      finishBatch();
    }
  }

  function finishBatch() {
    running = false;
    clearInterval(elapsedTimer);
    manifestRows.splice(0, manifestRows.length, ...storeRowsToManifestRows());
    el.startBtn.disabled = false; el.topHalfBtn.disabled = false; el.bottomHalfBtn.disabled = false; el.stopBtn.disabled = true;
    el.mCurrent.textContent = '-';

    const c = counts();
    el.sTotal.textContent = storeStatusRows.size || queue.length; el.sOk.textContent = c.ok;
    el.sWarn.textContent = c.warn; el.sFail.textContent = c.fail;
    el.sElapsed.textContent = fmtMs(Date.now()-startTs);
    el.summary.classList.add('show');
    el.manifestBtn.style.display = ''; el.logBtn.style.display = '';
    // 경고/실패 있으면 5초 후 자동 재시도 (retryBtn 클릭 = 취소)
    // 단, 사용자가 중단(stopRequested)한 경우엔 자동 재시도 금지 — finishRetry와 동일 가드
    if (c.warn + c.fail > 0 && !stopRequested) {
      if (autoRetryTimer) { clearInterval(autoRetryTimer); autoRetryTimer = null; }
      el.retryBtn.style.display = '';
      el.retryBtn.textContent = '↩ 재시도 (5초 후 자동 시작)';
      let countdown = 5;
      autoRetryTimer = setInterval(() => {
        countdown--;
        if (countdown > 0) {
          el.retryBtn.textContent = '↩ 재시도 (' + countdown + '초 후 자동 시작)';
        } else {
          clearInterval(autoRetryTimer); autoRetryTimer = null;
          el.retryBtn.textContent = '↩ 실패 재시도';
          runRetry();
        }
      }, 1000);
    }
    log(`배치 완료 — 성공 ${c.ok} / 경고 ${c.warn} / 실패 ${c.fail}`, c.fail ? 'err' : 'ok');

    // 중단으로 인해 아예 실행 안 된 계정 백필 큐 저장
    // (미완료는 finishRetry에서 덮어씀 — 재시도 없이 탭 닫힌 경우 대비)
    const allIncomplete = incompleteStores();
    if (allIncomplete.length > 0) {
      const targetDate = getYesterdayStr();
      chrome.storage.local.set({ ce_backfill_queue: { enqueuedDate: getTodayStr(), targetDate, failedStores: allIncomplete } });
      log(`📋 백필 큐 저장 — 미완료 매장 ${allIncomplete.length}개`, 'info');
    } else {
      chrome.storage.local.remove(['ce_backfill_queue']);
    }

    downloadLog();
  }

  // ── 실패 재시도 ──
  // manifestRows에서 (ordersOk=false OR cmgOk=false OR menuOk=false) 항목 재실행 (중단됨 포함)
  // skip 플래그로 이미 성공한 카테고리는 건너뜀
  async function runRetry() {
    if (running) return;
    const failures = incompleteStores();
    if (!failures.length) { log('재시도할 실패 항목 없음', 'info'); return; }

    running = true; stopRequested = false;
    if (autoRetryTimer) { clearInterval(autoRetryTimer); autoRetryTimer = null; }
    retryRound++;
    el.retryBtn.disabled = true; el.stopBtn.disabled = false;
    el.startBtn.disabled = true; el.topHalfBtn.disabled = true; el.bottomHalfBtn.disabled = true;
    startTs = Date.now();
    elapsedTimer = setInterval(() => { el.mElapsed.textContent = fmtMs(Date.now()-startTs); }, 1000);
    const failureSet = new Set(failures.map(normalizeStoreName));
    const retryIndices = targetRunIndices(failures);
    const oFail = [...storeStatusRows.values()].filter(r => failureSet.has(normalizeStoreName(r.store)) && !r.ordersOk).length;
    const cFail = [...storeStatusRows.values()].filter(r => failureSet.has(normalizeStoreName(r.store)) && !r.cmgOk).length;
    const mFail = [...storeStatusRows.values()].filter(r => failureSet.has(normalizeStoreName(r.store)) && !r.menuOk).length;
    log(`재시도 ${retryRound}/${MAX_AUTO_RETRY}회차 시작 — ${failures.length}개 매장 / ${retryIndices.length}개 계정 (orders:${oFail} / CMG:${cFail} / 메뉴:${mFail})`, 'info');

    let workTab;
    try {
      workTab = await chrome.tabs.create({url:'about:blank', active:true});
      for (let ri = 0; ri < retryIndices.length; ri++) {
        if (stopRequested) { log('중단 요청 — 재시도 종료', 'err'); break; }
        const queueIdx = retryIndices[ri];
        const acc = queue[queueIdx];
        const targetStores = accountTargetStores(acc, failures);
        if (!targetStores.length) continue;
        const relatedRows = targetStores.map(store => storeStatusRows.get(normalizeStoreName(store))).filter(Boolean);
        const skip = {
          orders: relatedRows.every(r => r.ordersOk),
          cmg: relatedRows.every(r => r.cmgOk),
          menu: relatedRows.every(r => r.menuOk)
        };
        // 실패 카테고리 배지 초기화
        if (!skip.orders) setBadge(`ro-${queueIdx}`, 'wait');
        if (!skip.cmg)    setBadge(`rc-${queueIdx}`, 'wait');
        if (!skip.menu)   setBadge(`rm-${queueIdx}`, 'wait');
        try {
          await processAccount(queueIdx, workTab.id, skip, targetStores);
        } catch(e) {
          log(`[${acc.id}] 재시도 예외: ${e.message}`, 'err');
          markAccountStoresTried(acc, targetStores, e.message);
        }
        updateMeta();
        if (ri < retryIndices.length - 1 && !stopRequested) {
          const d = rand(DELAY_MIN, DELAY_MAX);
          log(`다음 계정까지 ${Math.round(d/1000)}초 대기...`);
          await sleep(d);
        }
      }
    } catch(e) {
      log(`재시도 예외: ${e.message}`, 'err');
    } finally {
      if (workTab) { try { await chrome.tabs.remove(workTab.id); } catch(_) {} }
      finishRetry();
    }
  }

  function finishRetry() {
    running = false;
    clearInterval(elapsedTimer);
    manifestRows.splice(0, manifestRows.length, ...storeRowsToManifestRows());
    el.stopBtn.disabled = true;
    el.startBtn.disabled = false; el.topHalfBtn.disabled = false; el.bottomHalfBtn.disabled = false;
    el.mCurrent.textContent = '-';
    const c = counts();
    el.sOk.textContent = c.ok; el.sWarn.textContent = c.warn; el.sFail.textContent = c.fail;
    el.retryBtn.disabled = false;
    const stillFailing = incompleteStores().length > 0;
    el.retryBtn.style.display = stillFailing ? '' : 'none';
    if (stillFailing && !stopRequested) {
      if (retryRound < MAX_AUTO_RETRY) {
        if (autoRetryTimer) { clearInterval(autoRetryTimer); autoRetryTimer = null; }
        el.retryBtn.textContent = `↩ 재시도 ${retryRound}/${MAX_AUTO_RETRY} (5초 후 자동 시작)`;
        let countdown = 5;
        autoRetryTimer = setInterval(() => {
          countdown--;
          if (countdown > 0) {
            el.retryBtn.textContent = `↩ 재시도 ${retryRound}/${MAX_AUTO_RETRY} (${countdown}초 후 자동 시작)`;
          } else {
            clearInterval(autoRetryTimer); autoRetryTimer = null;
            el.retryBtn.textContent = '↩ 실패 재시도';
            runRetry();
          }
        }, 1000);
      } else {
        el.retryBtn.textContent = '↩ 실패 재시도';
        notifyFailures();
      }
    }
    log(`재시도 완료 — 성공 ${c.ok} / 경고 ${c.warn} / 실패 ${c.fail}`, c.fail ? 'err' : 'ok');

    // 최종 실패 계정을 백필 큐에 저장 → 다음 날 배치 시작 시 자동 재수집
    // 중단됨 포함, 아예 실행 안 된 계정도 포함
    const allIncompleteR = incompleteStores();
    if (allIncompleteR.length > 0) {
      const targetDate = getYesterdayStr();
      chrome.storage.local.set({ ce_backfill_queue: { enqueuedDate: getTodayStr(), targetDate, failedStores: allIncompleteR } });
      log(`📋 백필 큐 저장 — 미완료 매장 ${allIncompleteR.length}개 (내일 배치 시 자동 재수집)`, 'info');
    } else {
      chrome.storage.local.remove(['ce_backfill_queue']);
      log('✅ 전체 성공 — 백필 큐 없음', 'ok');
    }

    downloadLog();
  }

  // ── 로그 MD 다운로드 ──
  function downloadLog() {
    const lines = [...el.log.querySelectorAll('.log-line')].map(div => {
      const t = div.querySelector('.t')?.textContent || '';
      const msg = div.textContent.replace(t, '').trim();
      return t + '  ' + msg;
    });
    const ymd = new Date().toISOString().slice(0, 10);
    const md = '# 쿠팡 수집 로그 (' + ymd + ')\n\n`\n' + lines.join('\n') + '\n`\n';
    const filename = 'coupang_log_' + ymd.replace(/-/g, '') + '.md';
    chrome.runtime.sendMessage({type:'DOWNLOAD_CSV', content:md, filename, mimeType:'text/plain'}, resp => {
      log(resp?.success ? ('로그 저장: ' + filename) : '로그 저장 실패', resp?.success ? 'ok' : 'err');
    });
  }

  // ── 실패 알림 (Airflow REST API → 이메일) ──
  async function notifyFailures() {
    const failures = manifestRows
      .filter(r => r.note !== '중단됨' && (!r.ordersOk || !r.cmgOk || !r.menuOk))
      .map(r => ({ account: r.account_id, stores: r.stores, ordersOk: r.ordersOk, cmgOk: r.cmgOk, menuOk: r.menuOk, note: r.note }));
    if (!failures.length) return;
    log('📧 Airflow 알림 전송 중 — ' + failures.length + '개 실패 항목...', 'info');
    try {
      const resp = await fetch('http://localhost:8080/api/v1/dags/DB_Coupang_Notify/dagRuns', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Basic ' + btoa('airflow:airflow')
        },
        body: JSON.stringify({
          conf: {
            failures,
            collected_at: new Date().toISOString(),
            notify_email: 'a17019@kakao.com'
          }
        })
      });
      if (resp.ok) {
        log('📧 이메일 알림 DAG 트리거 완료', 'ok');
      } else {
        const txt = await resp.text().catch(() => '');
        log('📧 Airflow 응답 오류 (' + resp.status + '): ' + txt.slice(0, 100), 'err');
      }
    } catch(e) {
      log('📧 Airflow 연결 실패 (' + e.message + ') — localhost:8080 접근 불가', 'err');
    }
  }

  // ── 매니페스트 CSV ──
  function downloadManifest() {
    if (!manifestRows.length) return;
    const headers = ['collected_at','account_id','stores','status','duration','note'];
    const esc = v => { const s=String(v??''); return /[",\n]/.test(s)?'"'+s.replace(/"/g,'""')+'"':s; };
    const lines = [headers.join(','), ...manifestRows.map(r => headers.map(h=>esc(r[h])).join(','))];
    const csv = lines.join('\n');
    const ymd = new Date().toISOString().slice(0,10).replace(/-/g,'');
    const filename = `coupangeats_batch_manifest_${ymd}.csv`;
    chrome.runtime.sendMessage({type:'DOWNLOAD_CSV', content:csv, filename}, (resp) => {
      log(resp?.success ? `매니페스트 저장: ${filename}` : '매니페스트 저장 실패', resp?.success?'ok':'err');
    });
  }

  // ── 계정 목록 로드 ──
  // 항상 전체 목록 표시. 테스트 대상은 행 클릭으로 선택.
  async function loadQueue() {
    if (window.accountsReady?.then) {
      try { await window.accountsReady; } catch(e) { log('계정 로드 경고: '+e.message,'err'); }
    }
    const acc = window.ACCOUNTS || {};
    const idMap = new Map();
    for (const stores of Object.values(acc)) {
      for (const s of stores) {
        if (s.platform !== 'coupang') continue;
        if (!idMap.has(s.id)) idMap.set(s.id, {id:s.id, pw:s.pw, stores:[]});
        idMap.get(s.id).stores.push(s.store);
      }
    }
    queue = [...idMap.values()];
    selectedIndices.clear();
    el.startBtn.textContent = '▶ 전체 수집 시작';
    el.modeLabel.textContent = `총 ${queue.length}개 계정 — 행/필터 클릭으로 복수 선택`;
    buildRows();
    buildFilterList('');
    log(`계정 로드 완료: 쿠팡 ${queue.length}개 계정 (행 클릭 → 개별 테스트)`, 'info');
  }

  // ── 초기화 ──
  async function init() {
    await loadQueue();
    // 검색 입력 → 필터 실시간 갱신
    const filterSearch = document.getElementById('filterSearch');
    if (filterSearch) {
      filterSearch.addEventListener('input', (e) => buildFilterList(e.target.value));
    }
  }

  el.menuBtn.addEventListener('click', () => {
    chrome.tabs.create({ url: 'https://store.coupangeats.com/merchant/management/menu/', active: true });
  });
  el.startBtn.addEventListener('click', () => runBatch('all'));
  el.topHalfBtn.addEventListener('click', () => runBatch('top50'));
  el.bottomHalfBtn.addEventListener('click', () => runBatch('bottom50'));
  el.stopBtn.addEventListener('click', () => {
    if (autoRetryTimer) { clearInterval(autoRetryTimer); autoRetryTimer = null; }
    stopRequested = true; log('중단 요청됨 — 현재 수집 완료 후 멈춥니다.','err');
  });
  el.manifestBtn.addEventListener('click', downloadManifest);
  el.retryBtn.addEventListener('click', () => {
    if (autoRetryTimer) {
      clearInterval(autoRetryTimer); autoRetryTimer = null;
      el.retryBtn.textContent = '↩ 실패 재시도';
      log('자동 재시도 취소 — 수동으로 다시 누르면 즉시 시작합니다.', 'info');
    } else {
      runRetry();
    }
  });
  el.logBtn.addEventListener('click', downloadLog);

  // F5 / 탭 닫기 방지 — 배치 실행 중일 때 경고
  window.addEventListener('beforeunload', (e) => {
    if (!running) return;
    e.preventDefault();
    e.returnValue = '배치 수집이 진행 중입니다. 페이지를 떠나면 수집이 중단됩니다.';
  });

  // auto 파라미터: 윈도우 작업 스케줄러가 runner.html?auto=1 로 열 때 자동 시작 (전체)
  const params = new URLSearchParams(location.search);

  // 인터넷 연결 확인 — coupangeats 서버에 HEAD 요청, 실패 시 2초 간격 재시도
  async function waitForInternet(maxMs) {
    const deadline = Date.now() + maxMs;
    while (Date.now() < deadline) {
      try {
        await fetch('https://store.coupangeats.com/merchant/login', { method: 'HEAD', cache: 'no-store' });
        return true;
      } catch (_) {
        await sleep(2000);
      }
    }
    return false;
  }

  init().then(async () => {
    if (params.get('auto') !== '1') return;
    log('자동 시작 모드 (auto=1) — 인터넷 연결 확인 중...', 'info');

    async function tryAutoRun() {
      const online = await waitForInternet(30000);
      if (!online) {
        log('인터넷 연결 없음 — 10초 후 페이지 새로고침 후 재시도...', 'err');
        setTimeout(() => location.reload(), 10000);
        return;
      }
      log('인터넷 연결 확인 — 3초 후 전체 수집 시작', 'info');
      await sleep(3000);
      await runBatch();

      // 성공 0 + 경고 0 이면 인터넷 오류로 판단 → 10초 후 새로고침 재시도
      const c = counts();
      if (c.done > 0 && c.ok === 0 && c.warn === 0) {
        log('전체 수집 실패 — 인터넷 오류 의심, 10초 후 재시도...', 'err');
        setTimeout(() => location.reload(), 10000);
      }
    }

    tryAutoRun();
  });
})();
