// ===================================================================================
// ================= runner.js : 쿠팡 전체 자동수집 오케스트레이터 + 라이브 대시보드 =================
// ===================================================================================
//
// 동작:
//   1) accounts.js에서 쿠팡 매장 목록을 읽는다 (테스트 단계는 TEST_STORE 1개만).
//   2) 작업 탭 1개를 만들어 매장마다:
//        세션 초기화 → 로그인 페이지 이동 → 자동 로그인 → orders 페이지 도달 대기
//        → COLLECT 전송 → 완료 신호 대기 → 결과 기록 → (다른 계정이면 로그아웃) → 지연
//   3) 진행 상황을 대시보드에 실시간 표시, 끝나면 매니페스트 CSV 다운로드.
//
// 핵심: 로그인·수집은 "진짜 크롬·진짜 세션"의 content script가 수행 → Akamai 안전.
//       이 페이지(러너 탭)는 오래 살아있으므로 MV3 service worker 강제종료에 영향받지 않는다.

(function () {
  'use strict';

  // ── 설정 ──
  const TEST_STORE = '도리당 송파삼전점';          // Phase 0 테스트 매장 (URL ?all=1 이면 전체)
  const LOGIN_URL = 'https://store.coupangeats.com/merchant/login';
  const RE_LOGIN = /store\.coupangeats\.com\/merchant\/login/;
  const RE_ORDERS = /store\.coupangeats\.com\/merchant\/management\/orders\/(\d+)/;
  const CONTENT_FILES = [
    'content/00_config.js', 'content/01_utils.js', 'content/02_baemin.js',
    'content/03_coupangeats.js', 'content/04_auto_login.js', 'content/05_main.js',
    'content/06_batch.js'
  ];
  const DELAY_MIN = 5000, DELAY_MAX = 15000;     // 매장 간 지연(ms)
  const LOGIN_TIMEOUT = 70000;                   // 로그인+리다이렉트 대기(ms)
  const COLLECT_TIMEOUT = 240000;                // 수집 완료 대기(ms)

  // ── 상태 ──
  let queue = [];            // [{store, id, pw, _row}]
  let running = false;
  let stopRequested = false;
  let startTs = 0;
  let elapsedTimer = null;
  let pending = null;        // 현재 매장 완료 대기 {resolve, settled, fileSaved, filename, error, storeId}
  const manifestRows = [];   // 최종 매니페스트

  // ── DOM ──
  const $ = (id) => document.getElementById(id);
  const els = {
    rows: $('rows'), log: $('log'), fill: $('progressFill'),
    mProgress: $('mProgress'), mOk: $('mOk'), mEmpty: $('mEmpty'), mFail: $('mFail'),
    mElapsed: $('mElapsed'), mCurrent: $('mCurrent'),
    startBtn: $('startBtn'), stopBtn: $('stopBtn'), clearChk: $('clearSessionChk'),
    modeLabel: $('modeLabel'), summary: $('summary'),
    sTotal: $('sTotal'), sOk: $('sOk'), sEmpty: $('sEmpty'), sFail: $('sFail'), sElapsed: $('sElapsed')
  };

  // ── 유틸 ──
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const rand = (a, b) => Math.floor(Math.random() * (b - a + 1)) + a;
  const pad2 = (n) => String(n).padStart(2, '0');
  function fmtDur(ms) {
    const s = Math.floor(ms / 1000);
    return `${pad2(Math.floor(s / 60))}:${pad2(s % 60)}`;
  }
  function nowStr() {
    const d = new Date();
    return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
  }
  function log(msg, cls) {
    const line = document.createElement('div');
    line.className = 'log-line' + (cls ? ' ' + cls : '');
    line.innerHTML = `<span class="t">${nowStr()}</span>  ${msg}`;
    els.log.appendChild(line);
    els.log.scrollTop = els.log.scrollHeight;
    console.log('[Runner]', msg);
  }

  // ── 대시보드 렌더 ──
  const STATUS = {
    wait:  { cls: 'b-wait',  txt: '대기' },
    login: { cls: 'b-run',   txt: '로그인중' },
    nav:   { cls: 'b-run',   txt: '페이지 이동' },
    collect:{cls: 'b-run',   txt: '수집중' },
    ok:    { cls: 'b-ok',    txt: '완료' },
    empty: { cls: 'b-empty', txt: '빈수집' },
    fail:  { cls: 'b-fail',  txt: '실패' }
  };
  function buildRows() {
    els.rows.innerHTML = '';
    queue.forEach((item, i) => {
      const tr = document.createElement('tr');
      tr.id = `row-${i}`;
      tr.innerHTML = `<td>${i + 1}</td><td>${item.store}</td><td>${item.id}</td>` +
        `<td><span class="badge b-wait">대기</span></td><td>-</td><td>-</td><td></td>`;
      els.rows.appendChild(tr);
      item._tr = tr;
    });
    updateMeta();
  }
  function setRowStatus(i, statusKey, opts = {}) {
    const tr = queue[i] && queue[i]._tr;
    if (!tr) return;
    const tds = tr.children;
    const st = STATUS[statusKey] || STATUS.wait;
    tds[3].innerHTML = `<span class="badge ${st.cls}">${st.txt}</span>`;
    if (opts.count !== undefined) tds[4].textContent = opts.count;
    if (opts.dur !== undefined) tds[5].textContent = opts.dur;
    if (opts.note !== undefined) tds[6].textContent = opts.note;
    els.rows.querySelectorAll('tr.current').forEach((r) => r.classList.remove('current'));
    if (['login', 'nav', 'collect'].includes(statusKey)) tr.classList.add('current');
  }
  function counts() {
    let ok = 0, empty = 0, fail = 0, done = 0;
    manifestRows.forEach((r) => {
      if (r.status === 'ok') ok++;
      else if (r.status === 'empty') empty++;
      else if (r.status === 'fail') fail++;
    });
    done = manifestRows.length;
    return { ok, empty, fail, done };
  }
  function updateMeta() {
    const c = counts();
    const total = queue.length;
    els.mProgress.textContent = `${c.done} / ${total}`;
    els.mOk.textContent = c.ok; els.mEmpty.textContent = c.empty; els.mFail.textContent = c.fail;
    els.fill.style.width = total ? `${Math.round((c.done / total) * 100)}%` : '0%';
  }

  // ── 탭 제어 ──
  async function getTabUrl(tabId) {
    try { const t = await chrome.tabs.get(tabId); return t.url || ''; }
    catch (_) { return ''; }
  }
  // url이 어느 정규식에 매칭될 때까지 폴링. 반환: {key} 또는 null(타임아웃)
  async function waitTabUrl(tabId, regexMap, timeoutMs) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (stopRequested) return null;
      const url = await getTabUrl(tabId);
      for (const [key, re] of Object.entries(regexMap)) {
        const m = url.match(re);
        if (m) return { key, match: m, url };
      }
      await sleep(700);
    }
    return null;
  }
  async function ensureContent(tabId) {
    try {
      await chrome.tabs.sendMessage(tabId, { type: 'PING' });
      return true;
    } catch (_) {
      try {
        await chrome.scripting.executeScript({ target: { tabId }, files: CONTENT_FILES });
        await sleep(400);
        return true;
      } catch (e) {
        log(`content script 주입 실패: ${e.message}`, 'err');
        return false;
      }
    }
  }
  async function clearCoupangSession() {
    return new Promise((resolve) => {
      try {
        chrome.browsingData.remove(
          { origins: ['https://store.coupangeats.com', 'https://advertising.coupangeats.com'] },
          { cookies: true, localStorage: true, cacheStorage: true },
          () => resolve(true)
        );
      } catch (e) {
        log(`세션 초기화 실패(무시): ${e.message}`);
        resolve(false);
      }
    });
  }

  // ── 완료 신호 수신 (content/06_batch.js, background.js → 여기로) ──
  chrome.runtime.onMessage.addListener((msg) => {
    if (!pending) return;
    if (msg && msg.type === 'BATCH_FILE_SAVED') {
      pending.fileSaved = true;
      pending.filename = msg.filename || '';
    }
    if (msg && msg.type === 'COLLECTION_COMPLETE') {
      if (pending.settled) return;
      pending.settled = true;
      pending.error = msg.payload && msg.payload.error ? msg.payload.error : null;
      if (msg.payload && msg.payload.store_id) pending.storeId = msg.payload.store_id;
      pending.resolve();
    }
  });
  function waitCollection() {
    return new Promise((resolve) => {
      pending.resolve = resolve;
      // 안전 타임아웃
      pending._to = setTimeout(() => {
        if (!pending.settled) { pending.settled = true; pending.error = 'timeout'; resolve(); }
      }, COLLECT_TIMEOUT);
    });
  }

  // ── 매장 1개 처리 ──
  async function processStore(i, workTabId) {
    const item = queue[i];
    const t0 = Date.now();
    els.mCurrent.textContent = item.store;
    log(`──── [${i + 1}/${queue.length}] ${item.store} (${item.id}) 시작 ────`);

    // 0) 세션 초기화 (다른 계정 로그인 보장)
    if (els.clearChk.checked) {
      setRowStatus(i, 'nav', { note: '세션 초기화' });
      await clearCoupangSession();
    }

    // 1) 로그인 페이지 이동
    setRowStatus(i, 'nav', { note: '로그인 페이지' });
    await chrome.tabs.update(workTabId, { url: LOGIN_URL });
    let r = await waitTabUrl(workTabId, { login: RE_LOGIN, orders: RE_ORDERS }, 30000);
    if (stopRequested) return { status: 'fail', note: '중단됨' };
    if (!r) { log(`${item.store}: 로그인 페이지 로딩 실패`, 'err'); return { status: 'fail', note: '로그인 페이지 로딩 실패', dur: t0 }; }

    // 2) 자동 로그인 (이미 orders면 생략)
    if (r.key === 'login') {
      setRowStatus(i, 'login');
      await ensureContent(workTabId);
      await sleep(800);
      try {
        await chrome.tabs.sendMessage(workTabId, {
          type: 'AUTO_LOGIN', platform: 'coupang', id: item.id, pw: item.pw,
          storeName: item.store, autoClick: true
        });
      } catch (e) {
        log(`${item.store}: 자동로그인 메시지 실패 ${e.message}`, 'err');
      }
      log(`${item.store}: 로그인 시도 → orders 페이지 대기...`);
      r = await waitTabUrl(workTabId, { orders: RE_ORDERS }, LOGIN_TIMEOUT);
      if (stopRequested) return { status: 'fail', note: '중단됨' };
      if (!r) {
        const cur = await getTabUrl(workTabId);
        const note = RE_LOGIN.test(cur) ? '로그인 실패(Akamai/계정)' : '주문페이지 도달 실패';
        log(`${item.store}: ${note}`, 'err');
        return { status: 'fail', note, dur: t0 };
      }
    }
    const storeId = r.match[1];
    log(`${item.store}: orders 페이지 도달 (storeId=${storeId})`, 'ok');

    // 3) 수집 실행
    setRowStatus(i, 'collect', { note: `storeId ${storeId}` });
    await ensureContent(workTabId);
    await sleep(1200);
    pending = { settled: false, fileSaved: false, filename: '', error: null, storeId };
    try {
      await chrome.tabs.sendMessage(workTabId, { type: 'COLLECT' });
    } catch (e) {
      log(`${item.store}: COLLECT 전송 실패 ${e.message}`, 'err');
    }
    await waitCollection();
    if (pending._to) clearTimeout(pending._to);
    const result = pending; pending = null;

    // 4) 결과 판정
    if (result.error) {
      log(`${item.store}: 수집 오류 - ${result.error}`, 'err');
      return { status: 'fail', note: result.error, storeId, dur: t0 };
    }
    if (result.fileSaved) {
      log(`${item.store}: 수집 완료 → ${result.filename}`, 'ok');
      return { status: 'ok', note: result.filename, storeId, filename: result.filename, dur: t0 };
    }
    log(`${item.store}: 수집 완료(저장 없음=주문 0건)`, '');
    return { status: 'empty', note: '주문 0건', storeId, dur: t0 };
  }

  // ── 배치 실행 ──
  async function runBatch() {
    if (running) return;
    running = true; stopRequested = false;
    manifestRows.length = 0;
    els.startBtn.disabled = true; els.stopBtn.disabled = false;
    els.summary.classList.remove('show');
    startTs = Date.now();
    elapsedTimer = setInterval(() => { els.mElapsed.textContent = fmtDur(Date.now() - startTs); }, 1000);

    log(`배치 시작: 대상 ${queue.length}개 매장`);
    const workTab = await chrome.tabs.create({ url: 'about:blank', active: true });

    for (let i = 0; i < queue.length; i++) {
      if (stopRequested) { log('사용자 중단 요청 — 루프 종료', 'err'); break; }
      let res;
      try {
        res = await processStore(i, workTab.id);
      } catch (e) {
        log(`${queue[i].store}: 예외 ${e.message}`, 'err');
        res = { status: 'fail', note: e.message };
      }
      const dur = res.dur ? fmtDur(Date.now() - res.dur) : '-';
      setRowStatus(i, res.status, { count: res.status === 'ok' ? '저장됨' : (res.status === 'empty' ? '0' : '-'), dur, note: res.note || '' });
      manifestRows.push({
        store: queue[i].store, account_id: queue[i].id, store_id: res.storeId || '',
        status: res.status, filename: res.filename || '', note: res.note || '',
        collected_at: new Date().toISOString()
      });
      updateMeta();

      // 다음 매장 전 지연 (다른 계정이면 세션 초기화는 processStore 시작부에서 처리)
      if (i < queue.length - 1 && !stopRequested) {
        const d = rand(DELAY_MIN, DELAY_MAX);
        log(`다음 매장까지 ${Math.round(d / 1000)}초 대기...`);
        await sleep(d);
      }
    }

    // 작업 탭 정리
    try { await chrome.tabs.remove(workTab.id); } catch (_) {}

    finishBatch();
  }

  function finishBatch() {
    running = false;
    clearInterval(elapsedTimer);
    els.startBtn.disabled = false; els.stopBtn.disabled = true;
    els.mCurrent.textContent = '-';

    const c = counts();
    els.sTotal.textContent = queue.length; els.sOk.textContent = c.ok;
    els.sEmpty.textContent = c.empty; els.sFail.textContent = c.fail;
    els.sElapsed.textContent = fmtDur(Date.now() - startTs);
    els.summary.classList.add('show');
    log(`배치 종료 — 성공 ${c.ok} / 빈수집 ${c.empty} / 실패 ${c.fail}`, c.fail ? 'err' : 'ok');

    downloadManifest();
  }

  // ── 매니페스트 CSV 다운로드 (Airflow 검증/적재용) ──
  function downloadManifest() {
    if (!manifestRows.length) return;
    const headers = ['collected_at', 'store', 'account_id', 'store_id', 'status', 'filename', 'note'];
    const esc = (v) => {
      const s = String(v == null ? '' : v);
      return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
    };
    const lines = [headers.join(',')];
    manifestRows.forEach((r) => lines.push(headers.map((h) => esc(r[h])).join(',')));
    const csv = lines.join('\n');
    const ymd = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const filename = `coupangeats_batch_manifest_${ymd}.csv`;
    chrome.runtime.sendMessage({ type: 'DOWNLOAD_CSV', content: csv, filename }, (resp) => {
      if (resp && resp.success) log(`매니페스트 저장: ${filename}`, 'ok');
      else log(`매니페스트 저장 실패`, 'err');
    });
  }

  // ── 매장 목록 로드 ──
  async function loadQueue(useAll) {
    if (window.accountsReady && typeof window.accountsReady.then === 'function') {
      try { await window.accountsReady; } catch (e) { log('계정 로드 경고: ' + e.message, 'err'); }
    }
    const acc = window.ACCOUNTS || {};
    const all = [];
    for (const stores of Object.values(acc)) {
      for (const s of stores) {
        if (s.platform === 'coupang') all.push({ store: s.store, id: s.id, pw: s.pw });
      }
    }
    if (useAll) {
      queue = all;
      els.modeLabel.textContent = `전체 모드 · ${all.length}개 매장`;
    } else {
      queue = all.filter((s) => s.store === TEST_STORE || s.store.replace(/\s/g, '') === TEST_STORE.replace(/\s/g, ''));
      if (queue.length === 0) {
        els.modeLabel.textContent = `⚠ 테스트 매장 '${TEST_STORE}' 미발견 — accounts/CSV 확인 필요`;
        log(`테스트 매장 '${TEST_STORE}'을(를) 계정목록에서 찾지 못했습니다. (전체 ${all.length}개 쿠팡 매장 로드됨)`, 'err');
      } else {
        els.modeLabel.textContent = `테스트 모드 · ${TEST_STORE}`;
      }
    }
    buildRows();
    log(`매장 목록 로드 완료: ${queue.length}개 (전체 쿠팡 ${all.length}개)`);
  }

  // ── 초기화 ──
  els.startBtn.addEventListener('click', runBatch);
  els.stopBtn.addEventListener('click', () => {
    stopRequested = true;
    log('중단 요청됨 — 현재 매장 종료 후 멈춥니다.', 'err');
  });

  const params = new URLSearchParams(location.search);
  const useAll = params.get('all') === '1';
  const auto = params.get('auto') === '1' || params.get('run') === 'coupang';

  loadQueue(useAll).then(() => {
    if (auto && queue.length > 0) {
      log('자동 시작 모드 (윈도우 작업 스케줄러/알람) — 3초 후 시작');
      setTimeout(runBatch, 3000);
    }
  });
})();
