// ===================================================================================
// ================= 02_baemin.js : 배민 수집 로직 =================
// ===================================================================================

Sites['baemin'] = {
  name: '배민',
  match: (url) => url.includes('self.baemin.com'),
  _stopFlag: false,
  
  async collect(opts = {}) {
    this._stopFlag = false;
    this._batchMode = opts.source === 'batch';
    this._setupEscListener();

    const shopInfo = this._getShopInfo();
    const expectedStore = opts.expectedStore || '';
    const expectedStoreId = opts.expectedStoreId || '';
    let category = 'now';
    let success = true;
    let error = '';
    let result = null;

    try {
      if (location.href.includes('/orders/history')) category = 'orders';
      else if (location.href.includes('/stat/marketing/woori-shop-click')) category = 'woori';
      else if (location.href.includes('/stat/advertisement')) category = 'ad';
      else if (location.href.includes('/history/change/shop')) category = 'change';
      else if (location.href.includes('/history/change/ad')) category = 'change';
      else if (location.href.includes('/menupan')) category = 'menupan';

      // 메인 페이지에서 수집 시 매장명 확인. 배치 모드는 대시보드가 계정을 통제한다.
      if (!this._batchMode &&
          !location.href.includes('/orders/') &&
          !location.href.includes('/stat/') &&
           !location.href.includes('/history/') &&
           !location.href.includes('/menupan')) {
        const storeName = shopInfo.store_name || '';
        const isDoridang = this._isAllowedDoridangFamilyStoreName(storeName);

        if (!isDoridang) {
          const confirmMsg = `⚠️ 매장명 확인\n\n현재 선택된 매장:\n"${storeName}"\n\n도리당/나홀로 매장이 아닌 것 같습니다.\n이 매장으로 수집하시겠습니까?`;

          if (!confirm(confirmMsg)) {
            console.log('[배민 수집] 사용자가 수집 취소');
            Utils.showError('수집 취소', '매장을 확인하고 다시 시도해주세요.');
            throw new Error('사용자가 수집 취소');
          }
        }
      }

      if (category === 'orders' && opts.source === 'manual' && !expectedStore && !expectedStoreId) {
        result = await this.collectManualOrdersTargetOptions();
      }
      else if (category === 'orders') {
        let ordersShopInfo = shopInfo;
        if ((ordersShopInfo.needFilter || expectedStore || expectedStoreId) && (expectedStore || expectedStoreId)) {
          const filterResult = await this.applyOrdersStoreFilter(expectedStore, expectedStoreId);
          if (!filterResult.success) {
            throw new Error(filterResult.error || 'orders 가게 필터 적용 실패');
          }
          ordersShopInfo = this._getShopInfo();
        }
        result = await this._collectOrdersAllPages(ordersShopInfo);
      }
      else if (category === 'woori') result = await this._collectMarketingData(shopInfo);
      else if (category === 'ad') result = await this._collectAdvertisementData(shopInfo);
      else if (category === 'menupan') result = await this._collectMenupan(shopInfo);
      else if (location.href.includes('/history/change/ad')) result = await this._collectAdChangeHistory(shopInfo);
      else if (category === 'change') result = await this._collectChangeHistory(shopInfo);
      else {
        const currentInfo = this._getShopInfo();
        if (this._batchMode && expectedStore && !this._isSameStoreName(currentInfo.store_name, expectedStore) && currentInfo.store_id !== expectedStoreId) {
          throw new Error(`now content 매장 불일치: 현재=${currentInfo.store_name || '미탐지'}(${currentInfo.store_id || 'id없음'}) / 목표=${expectedStore}(${expectedStoreId || 'id없음'})`);
        }
        result = await this._collectMetrics(currentInfo);
      }

      if (result && result.success === false) {
        success = false;
        error = result.error || '수집 실패';
      }
    } catch (e) {
      success = false;
      error = e?.message || String(e);
      console.error('[배민 배치 수집] 실패:', e);
    } finally {
      this._removeEscListener();
      if (this._batchMode) {
        try {
          chrome.runtime.sendMessage({
            type: 'BAEMIN_CATEGORY_COMPLETE',
            payload: {
              category,
              success,
              error,
              store_id: (result && result.store_id) || shopInfo.store_id || '',
              store_name: (result && result.store_name) || shopInfo.store_name || '',
              filename: (result && result.filename) || '',
              rows: (result && result.rows) || 0
            }
          });
        } catch (_) {}
      }
      this._batchMode = false;
    }

    return result || { success, error };
  },

  _escHandler: null,
  _suppressEsc: false,
  _setupEscListener() {
    this._escHandler = (e) => {
      if (e.key !== 'Escape') return;
      // _closeDiscountSheet()가 상세시트를 닫으려고 쏘는 합성 ESC를
      // 자기가 받아 수집을 중단시키던 버그. dispatchEvent 이벤트는 isTrusted=false.
      if (!e.isTrusted || this._suppressEsc) return;
      this._stopFlag = true;
      Utils.showError('수집 중단', 'ESC 눌림 - 현재까지 수집된 데이터를 저장합니다.');
    };
    document.addEventListener('keydown', this._escHandler);
  },
  _removeEscListener() {
    if (this._escHandler) {
      document.removeEventListener('keydown', this._escHandler);
      this._escHandler = null;
    }
  },

  _randomDelay(min, max) {
    const ms = Math.floor(Math.random() * (max - min + 1)) + min;
    return new Promise(r => setTimeout(r, ms));
  },

  async _waitForPageLoad(prevFirstOrderId, timeout = 10000) {
    const startTime = Date.now();
    while (Date.now() - startTime < timeout) {
      if (this._stopFlag) return false;
      await new Promise(r => setTimeout(r, 200));
      const firstRow = document.querySelector('tr.Table_b_r4ax_1dwbr4on[data-index]');
      if (!firstRow) continue;
      const orderIdCell = firstRow.querySelector('td[data-td-index="1"]');
      const currentOrderId = orderIdCell?.textContent.trim() || '';
      if (currentOrderId && currentOrderId !== prevFirstOrderId) return true;
    }
    return false;
  },

  _getFirstOrderId() {
    const firstRow = document.querySelector('tr.Table_b_r4ax_1dwbr4on[data-index]');
    const orderIdCell = firstRow?.querySelector('td[data-td-index="1"]');
    return orderIdCell?.textContent.trim() || '';
  },

  _extractAreaName(storeName) {
    const areaMatch = storeName.match(/([가-힣]+(?:점|지점|매장|본점|분점|직영점))$/);
    return areaMatch ? areaMatch[1] : '';
  },

  _storeKey(name) {
    return String(name || '')
      .replace(/\[[^\]]*\]/g, '')
      .replace(/닭도리탕전문/g, '')
      .replace(/구로디지털단지점/g, '구로디지털점')
      .replace(/[\s·ㆍ.,_\-()\[\]]+/g, '')
      .trim();
  },

  _isSameStoreName(actual, expected) {
    const a = this._storeKey(actual);
    const e = this._storeKey(expected);
    return !!a && !!e && (a === e || a.includes(e));
  },

  _parseShopOption(option) {
    if (!option) return null;
    const value = option.value || '';
    let text = (option.textContent || '').replace(/\s+/g, ' ').trim();
    text = text.replace(/^\[[^\]]+\]\s*/, '').trim();
    text = text.split('/')[0].trim();
    text = text.replace(/\s+\d+$/, '').trim();
    text = text.replace(/^닭도리탕\s*전문\s*/, '').trim();
    return { store_name: text, store_id: value, needFilter: false };
  },

  _getShopInfo() {
    const cleanText = value => (value || '').replace(/\s+/g, ' ').trim();
    const visible = el => {
      if (!el) return false;
      const r = el.getBoundingClientRect();
      const st = getComputedStyle(el);
      return r.width > 0 && r.height > 0 && st.visibility !== 'hidden' && st.display !== 'none';
    };

    const nameEl = [...document.querySelectorAll('.ShopSelect-module__b8Mn, .ShopSelect-module__JWCr h3, h3')].find(visible);
    const infoEl = [...document.querySelectorAll('.ShopSelect-module__j4Qm')].find(visible);
    const visibleName = cleanText(nameEl?.textContent);
    const visibleStoreId = infoEl?.textContent.match(/(\d+)/)?.[1] || '';
    if (visibleName || visibleStoreId) {
      return { store_name: visibleName, store_id: visibleStoreId, needFilter: false };
    }

    const pathStoreId = location.pathname.match(/\/shops\/(\d+)/)?.[1] || '';
    const selectBox = document.querySelector('select.Select-module__a623, select.ShopSelect-module___pC1, .ShopSelect-module__JWCr select');
    if (selectBox) {
      const selectedOption = pathStoreId
        ? [...selectBox.options].find(option => option.value === pathStoreId)
        : selectBox.options[selectBox.selectedIndex];
      const parsed = this._parseShopOption(selectedOption);
      if (parsed?.store_name || parsed?.store_id) return parsed;
    }

    if (location.href.includes('/history/change/shop') || location.href.includes('/history/change/ad')) {
      return { store_name: '', store_id: pathStoreId, needFilter: false };
    }
    
    if (location.href.includes('/orders/history')) {
      const filterContainer = document.querySelector('button.FilterContainer-module__ccrG .FilterContainer-module__Axi6');
      if (filterContainer) {
        const firstBadge = filterContainer.querySelector('.Badge_b_r4ax_19agxiso');
        const text = firstBadge?.textContent.trim() || '';
        if (text === '음식배달 가게 전체') {
          return { store_name: '', store_id: pathStoreId, needFilter: true };
        }
        return { store_name: text, store_id: pathStoreId, needFilter: false };
      }
    }

    return {
      store_name: '',
      store_id: pathStoreId,
      needFilter: false
    };
  },

  _ordersFilterDebug() {
    const textOf = el => (el?.innerText || el?.textContent || '').replace(/\s+/g, ' ').trim();
    const filterButtons = [...document.querySelectorAll('button')]
      .filter(btn => textOf(btn).includes('가게') || textOf(btn).includes('음식배달'));
    const selects = [...document.querySelectorAll('select')];
    return {
      url: location.href,
      shopInfo: this._getShopInfo(),
      filterButtons: filterButtons.slice(0, 8).map(btn => textOf(btn)),
      selectCount: selects.length,
      options: selects.flatMap(select =>
        [...select.options].map(opt => `${opt.value}:${textOf(opt)}`)
      ).slice(0, 40)
    };
  },

  _cleanOrdersStoreLabel(label) {
    return String(label || '')
      .replace(/^\[[^\]]+\]\s*/, '')
      .replace(/\s+\d{4,}$/, '')
      .replace(/^닭도리탕\s*전문\s*/, '')
      .replace(/\s+/g, ' ')
      .trim();
  },

  _isAllowedDoridangFamilyStoreName(name) {
    const text = String(name || '').replace(/\s+/g, '');
    if (!text || text.includes('곱도리당')) return false;
    return text.includes('도리당') || text.includes('나홀로');
  },

  _isTargetOrdersStoreOption(option) {
    if (!option) return false;
    const value = String(option.value || '').trim();
    const text = String(option.textContent || '').trim();
    if (!value || value === 'ALL_FOOD_SHOP') return false;
    return this._isAllowedDoridangFamilyStoreName(text);
  },

  async _openOrdersStoreFilter() {
    const textOf = el => (el?.innerText || el?.textContent || '').replace(/\s+/g, ' ').trim();
    const existingSelect = [...document.querySelectorAll('select')]
      .find(select => [...select.options].some(option => String(option.value || '') === 'ALL_FOOD_SHOP'));
    if (existingSelect) return true;

    const filterButtons = [...document.querySelectorAll('button')];
    const storeFilterBtn = filterButtons.find(btn => {
      const text = textOf(btn);
      return btn.querySelector('.Badge_b_r4ax_19agxiso') &&
        (text.includes('가게') || text.includes('음식배달'));
    }) || filterButtons.find(btn => textOf(btn).includes('음식배달 가게 전체'));
    if (!storeFilterBtn) return false;
    storeFilterBtn.scrollIntoView?.({ block: 'center', inline: 'center' });
    storeFilterBtn.click();
    await new Promise(resolve => setTimeout(resolve, 700));
    return true;
  },

  async getOrdersStoreOptions() {
    if (!location.href.includes('/orders/history')) {
      return { success: false, error: 'orders 페이지가 아님', options: [], debug: this._ordersFilterDebug() };
    }

    await this._openOrdersStoreFilter();
    const textOf = el => (el?.innerText || el?.textContent || '').replace(/\s+/g, ' ').trim();
    const seen = new Map();
    for (const select of document.querySelectorAll('select')) {
      for (const option of select.options) {
        if (!this._isTargetOrdersStoreOption(option)) continue;
        const value = String(option.value || '').trim();
        if (seen.has(value)) continue;
        const text = textOf(option);
        seen.set(value, {
          value,
          text,
          store_id: value,
          store_name: this._cleanOrdersStoreLabel(text),
          brand: this._isAllowedDoridangFamilyStoreName(text) && text.includes('도리당') ? '도리당' : '나홀로'
        });
      }
    }
    const options = [...seen.values()];
    return { success: true, options, debug: this._ordersFilterDebug() };
  },

  async applyOrdersStoreFilter(expectedStore = '', expectedStoreId = '', optionValue = '', optionText = '') {
    if (!location.href.includes('/orders/history')) {
      return { success: false, error: 'orders 페이지가 아님', debug: this._ordersFilterDebug() };
    }

    const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
    const textOf = el => (el?.innerText || el?.textContent || '').replace(/\s+/g, ' ').trim();
    const expectedOptionName = this._cleanOrdersStoreLabel(optionText);
    const matchesExpected = info => {
      if (!info) return false;
      if (optionValue && info.store_id && String(info.store_id) === String(optionValue)) return true;
      if (expectedOptionName && this._isSameStoreName(info.store_name, expectedOptionName)) return true;
      if (expectedStoreId && info.store_id && String(info.store_id) === String(expectedStoreId)) return true;
      if (expectedStore && this._isSameStoreName(info.store_name, expectedStore)) return true;
      return !info.needFilter && !expectedStore && !expectedStoreId && !optionValue && !optionText;
    };

    let info = this._getShopInfo();
    if (matchesExpected(info)) {
      // /orders/history 에는 /shops/{id} 경로가 없어 info.store_id 가 비어 있다.
      // 그대로 두면 파일명이 ..._unknown_... 으로 저장된다.
      return {
        success: true,
        already: true,
        shopInfo: {
          ...info,
          store_id: optionValue || expectedStoreId || info.store_id || '',
          store_name: expectedOptionName || info.store_name || expectedStore || ''
        },
        debug: this._ordersFilterDebug()
      };
    }

    for (let attempt = 1; attempt <= 4; attempt++) {
      await this._openOrdersStoreFilter();

      const selects = [...document.querySelectorAll('select')];
      let selected = null;
      for (const select of selects) {
        const options = [...select.options];
        const option = optionValue
          ? options.find(opt => String(opt.value) === String(optionValue))
          : expectedStoreId
            ? options.find(opt => String(opt.value) === String(expectedStoreId)) ||
              options.find(opt => this._isSameStoreName(opt.textContent, expectedStore))
            : options.find(opt => this._isSameStoreName(opt.textContent, expectedStore));
        if (!option) continue;

        const setter = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, 'value')?.set;
        if (setter) setter.call(select, option.value);
        else select.value = option.value;
        select.selectedIndex = option.index;
        option.selected = true;
        select.dispatchEvent(new Event('input', { bubbles: true, composed: true }));
        select.dispatchEvent(new Event('change', { bubbles: true, composed: true }));
        selected = { value: option.value, text: textOf(option) };
        break;
      }

      if (!selected) {
        await sleep(600);
        continue;
      }

      await sleep(500);
      const applyBtn = [...document.querySelectorAll('button')].find(btn => {
        const text = textOf(btn);
        return text === '적용' || text === '조회' || text.includes('적용');
      });
      if (applyBtn) {
        applyBtn.scrollIntoView?.({ block: 'center', inline: 'center' });
        applyBtn.click();
      }

      for (let i = 0; i < 12; i++) {
        await sleep(500);
        info = this._getShopInfo();
        if (matchesExpected(info)) {
          return {
            success: true,
            attempt,
            selected,
            shopInfo: {
              ...info,
              store_id: optionValue || info.store_id || selected.value,
              store_name: expectedOptionName || info.store_name || this._cleanOrdersStoreLabel(selected.text)
            },
            debug: this._ordersFilterDebug()
          };
        }
      }
    }

    return {
      success: false,
      error: 'orders 가게 필터 적용 실패',
      debug: this._ordersFilterDebug()
    };
  },

  _manualOrdersPanel: null,

  _ensureManualOrdersPanel() {
    let panel = document.getElementById('doridang-baemin-manual-orders-log');
    if (panel) {
      this._manualOrdersPanel = panel;
      return panel;
    }

    panel = document.createElement('div');
    panel.id = 'doridang-baemin-manual-orders-log';
    panel.style.cssText = [
      'position:fixed',
      'right:16px',
      'bottom:16px',
      'z-index:2147483647',
      'width:420px',
      'max-height:52vh',
      'background:#101318',
      'color:#e8edf5',
      'border:1px solid #2d3440',
      'border-radius:8px',
      'box-shadow:0 12px 32px rgba(0,0,0,.38)',
      'font-family:Malgun Gothic,Segoe UI,sans-serif',
      'font-size:12px',
      'overflow:hidden'
    ].join(';');
    panel.innerHTML = `
      <div style="display:flex;align-items:center;gap:8px;padding:10px 12px;background:#171c24;border-bottom:1px solid #2d3440;">
        <strong style="font-size:13px;">배민 수동 orders 로그</strong>
        <span data-role="status" style="margin-left:auto;color:#8fa2bd;">실행중</span>
        <button type="button" data-role="copy" style="border:0;background:#2d3440;color:#e8edf5;border-radius:4px;padding:3px 7px;cursor:pointer;">복사</button>
        <button type="button" data-role="close" style="border:0;background:#2d3440;color:#e8edf5;border-radius:4px;padding:3px 7px;cursor:pointer;">닫기</button>
      </div>
      <div data-role="body" style="padding:10px 12px;max-height:calc(52vh - 42px);overflow:auto;white-space:pre-wrap;line-height:1.55;"></div>
    `;
    panel.querySelector('[data-role="close"]')?.addEventListener('click', () => panel.remove());

    const copyBtn = panel.querySelector('[data-role="copy"]');
    copyBtn?.addEventListener('click', async () => {
      const text = panel.querySelector('[data-role="body"]')?.innerText || '';
      let ok = false;
      try {
        await navigator.clipboard.writeText(text);
        ok = true;
      } catch (_) {
        // content script 에서 clipboard 권한이 막히는 경우의 폴백
        try {
          const ta = document.createElement('textarea');
          ta.value = text;
          ta.style.cssText = 'position:fixed;left:-9999px;top:0;';
          document.body.appendChild(ta);
          ta.select();
          ok = document.execCommand('copy');
          ta.remove();
        } catch (_) {}
      }
      copyBtn.textContent = ok ? '복사됨' : '복사실패';
      setTimeout(() => { copyBtn.textContent = '복사'; }, 1500);
    });
    document.body.appendChild(panel);
    this._manualOrdersPanel = panel;
    return panel;
  },

  _manualOrdersLog(message, level = 'info') {
    const panel = this._ensureManualOrdersPanel();
    const body = panel.querySelector('[data-role="body"]');
    const line = document.createElement('div');
    const now = new Date().toTimeString().slice(0, 8);
    const color = level === 'ok' ? '#61d394' : level === 'err' ? '#ff7b7b' : level === 'warn' ? '#ffd166' : '#b8c7dc';
    line.style.color = color;
    line.textContent = `[${now}] ${message}`;
    body.appendChild(line);
    body.scrollTop = body.scrollHeight;
    console.log(`[배민 수동 orders] ${message}`);
  },

  _manualOrdersSetStatus(status, level = 'info') {
    const panel = this._ensureManualOrdersPanel();
    const statusEl = panel.querySelector('[data-role="status"]');
    if (!statusEl) return;
    statusEl.textContent = status;
    statusEl.style.color = level === 'ok' ? '#61d394' : level === 'err' ? '#ff7b7b' : '#8fa2bd';
  },

  async collectManualOrdersTargetOptions() {
    this._ensureManualOrdersPanel();
    this._manualOrdersSetStatus('실행중');
    this._manualOrdersLog(`시작: ${location.href}`);
    this._manualOrdersLog(`환경: ${new Date().toLocaleString('ko-KR')} / 뷰포트 ${window.innerWidth}x${window.innerHeight} / UA ${navigator.userAgent.slice(0, 60)}`);

    const optResult = await this.getOrdersStoreOptions();
    if (!optResult.success) {
      const detail = optResult.error || JSON.stringify(optResult.debug || {}).slice(0, 700);
      this._manualOrdersLog(`option 조회 실패: ${detail}`, 'err');
      this._manualOrdersSetStatus('실패', 'err');
      return { success: false, error: detail };
    }

    const options = (optResult.options || []).filter(opt =>
      opt?.value && opt.value !== 'ALL_FOOD_SHOP' &&
      this._isAllowedDoridangFamilyStoreName(String(opt.text || ''))
    );

    if (!options.length) {
      this._manualOrdersLog('도리당/나홀로 option 없음. ALL_FOOD_SHOP은 수집하지 않습니다.', 'err');
      this._manualOrdersLog(`debug=${JSON.stringify(optResult.debug || {}).slice(0, 900)}`, 'warn');
      this._manualOrdersSetStatus('실패', 'err');
      return { success: false, error: '도리당/나홀로 option 없음' };
    }

    this._manualOrdersLog(`도리당/나홀로 option ${options.length}개 발견: ${options.map(opt => opt.store_name || opt.text).join(' / ')}`, 'ok');
    let okCount = 0;
    let failCount = 0;
    const files = [];

    for (let i = 0; i < options.length; i++) {
      if (this._stopFlag) break;
      const opt = options[i];
      const label = opt.store_name || opt.text || opt.value;
      this._manualOrdersLog(`[${i + 1}/${options.length}] 필터 선택: ${label} (${opt.value})`);

      const filterResult = await this.applyOrdersStoreFilter('', '', opt.value, opt.text);
      if (!filterResult.success) {
        failCount++;
        const detail = filterResult.error || JSON.stringify(filterResult.debug || {}).slice(0, 800);
        this._manualOrdersLog(`필터 적용 실패: ${label} / ${detail}`, 'err');
        continue;
      }

      this._manualOrdersLog(`적용 완료: ${filterResult.shopInfo?.store_name || label}`, 'ok');
      this._manualOrdersLog(`수집 시작: ${label}`);
      const result = await window.BaeminPipelineSave.collectAndSaveOrders('', '', opt.value, opt.text);
      if (result?.success) {
        okCount++;
        files.push(result.filename || '');
        if (result.partial) {
          this._manualOrdersLog(`부분 수집 저장 — 적재 금지: ${result.filename || label}`, 'err');
        } else {
          this._manualOrdersLog(`저장 완료: ${result.filename || label}`, 'ok');
        }
      } else {
        failCount++;
        this._manualOrdersLog(`수집 실패: ${label} / ${result?.error || '알 수 없는 실패'}`, 'err');
      }
      await new Promise(resolve => setTimeout(resolve, 900));
    }

    const success = okCount > 0;
    this._manualOrdersLog(`완료: 성공 ${okCount}, 실패 ${failCount}`, success ? 'ok' : 'err');
    this._manualOrdersSetStatus(success ? '완료' : '실패', success ? 'ok' : 'err');
    return { success, rows: okCount, files, error: success ? '' : '저장 성공 0건' };
  },

  _menupanLiveLog(message) {
    const msg = `[메뉴 content] ${message}`;
    console.log(`[Collector] ${msg}`);
    try {
      const sent = chrome.runtime.sendMessage({ type:'LIVE_LOG', msg });
      if (sent?.catch) sent.catch(() => {});
    } catch (_) {}
  },

  _getMenupanShopInfo() {
    const shopId = location.pathname.match(/\/shops\/(\d+)/)?.[1] || '';
    if (shopId) {
      for (const select of document.querySelectorAll('select')) {
        const option = [...select.options].find(item => String(item.value) === shopId);
        if (!option) continue;
        const parsed = this._parseShopOption(option);
        if (parsed?.store_name || parsed?.store_id) return parsed;
      }
    }

    const fallback = this._getShopInfo();
    return {
      store_name: fallback.store_name || '',
      store_id: shopId || fallback.store_id || '',
      needFilter: false
    };
  },

  _findMenuScrollElement() {
    let el = document.querySelector('[class*="menuList-module__container"]');
    while (el && el !== document.body) {
      const style = getComputedStyle(el);
      if (/(auto|scroll)/.test(style.overflowY) && el.scrollHeight > el.clientHeight + 50) {
        return el;
      }
      el = el.parentElement;
    }
    return document.scrollingElement || document.documentElement;
  },

  _parseMenuItem(el, info, iso, collectedDate) {
    const text = node => (node?.textContent || '').replace(/\s+/g, ' ').trim();
    const 메뉴명 = text(el.querySelector('[class*="menuInfo-module__name"]'));
    if (!메뉴명) return null;

    let 배달판매가 = '';
    for (const row of el.querySelectorAll('[class*="priceListItem-module__row"]')) {
      const spans = [...row.querySelectorAll('span')].filter(span => !span.querySelector('span'));
      if (text(spans[0]) !== '배달') continue;
      const priceSpan = spans.find(span =>
        !(span.className || '').includes('cancelLine') &&
        /[\d,]+원/.test(text(span)) &&
        !/^\d+%$/.test(text(span))
      );
      if (priceSpan) 배달판매가 = Utils.cleanPrice(text(priceSpan));
      break;
    }

    const statusBoxText = text(el.querySelector('[class*="menuList-module__statusBox"]'));
    const hidden = !!el.querySelector('[class*="menuInfo-module__hide"]');
    const 상태 = statusBoxText || (hidden ? '숨김' : '판매중');

    return {
      collected_at: iso,
      collected_date: collectedDate,
      brand: info.brand || '',
      store: info.store || '',
      store_id: info.store_id || '',
      메뉴명,
      배달판매가,
      상태
    };
  },

  async _scrollCollectMenuItems(info) {
    this._menupanLiveLog(`메뉴 DOM 탐색 시작: url=${location.href}`);
    const collectedAt = new Date();
    const iso = collectedAt.toISOString();
    const collectedDate = `${collectedAt.getFullYear()}-${String(collectedAt.getMonth() + 1).padStart(2, '0')}-${String(collectedAt.getDate()).padStart(2, '0')}`;
    const collected = new Map();
    const container = this._findMenuScrollElement();
    const documentScroller = document.scrollingElement || document.documentElement;
    const usesWindow = container === documentScroller;
    const scrollTopOf = () => usesWindow ? window.scrollY : container.scrollTop;
    const viewportOf = () => usesWindow ? window.innerHeight : container.clientHeight;
    const maxScrollOf = () => Math.max(0, container.scrollHeight - viewportOf());
    const scrollTo = y => {
      if (usesWindow) window.scrollTo(0, y);
      else container.scrollTop = y;
    };
    const originalTop = scrollTopOf();

    const harvest = () => {
      let added = 0;
      const root = document.querySelector('[class*="menuList-module__container"]') || document;
      const semanticNodes = root.querySelectorAll(
        'li[class*="menuItem-module__content"], [class*="menuList-module__listItem"]'
      );
      const nodes = semanticNodes.length
        ? semanticNodes
        : root.querySelectorAll('[data-index]');

      nodes.forEach((el, position) => {
        const indexKey = el.getAttribute('data-index');
        if (indexKey !== null && collected.has(`index-${indexKey}`)) return;

        const row = this._parseMenuItem(el, info, iso, collectedDate);
        if (!row) return;

        const positionKey = el.style.transform || '';
        const key = indexKey !== null
          ? `index-${indexKey}`
          : positionKey
            ? `position-${positionKey}`
            : `semantic-${position}|${row.메뉴명}|${row.배달판매가}|${row.상태}`;
        if (collected.has(key)) return;
        collected.set(key, row);
        added++;
      });
      return added;
    };

    let idleRounds = 0;
    try {
      scrollTo(0);
      await this._randomDelay(350, 550);

      for (let guard = 0; guard < 200; guard++) {
        if (this._stopFlag) break;

        const added = harvest();
        Utils.showProgress('메뉴판 수집 중...', `${collected.size}개 메뉴`);

        const currentTop = scrollTopOf();
        const maxScroll = maxScrollOf();
        if (currentTop >= maxScroll - 4) {
          idleRounds = added === 0 ? idleRounds + 1 : 0;
          if (idleRounds >= 3) break;
        } else {
          idleRounds = 0;
        }

        const nextTop = Math.min(currentTop + viewportOf() * 0.7, maxScroll);
        scrollTo(nextTop);
        if (!this._menupanLastLiveLogAt || Date.now() - this._menupanLastLiveLogAt >= 3000) {
          this._menupanLastLiveLogAt = Date.now();
          this._menupanLiveLog(`스크롤 진행: ${Math.round(nextTop)}/${Math.round(maxScroll)}, 수집 ${collected.size}행, idle ${idleRounds}`);
        }
        await this._randomDelay(350, 550);
      }
    } finally {
      scrollTo(originalTop);
    }

    return [...collected.values()];
  },

  async _collectMenupan(shopInfo) {
    try {
      this._menupanLiveLog(`수집 함수 진입: url=${location.href}`);
      const info = this._getMenupanShopInfo();
      const storeName = info.store_name || shopInfo.store_name || '알수없는매장';
      const storeId = info.store_id || shopInfo.store_id || '';
      this._menupanLiveLog(`매장 식별: ${storeName} (${storeId || 'id없음'})`);
      const brand = window.BaeminPipelineSave._getBrand(storeName);
      const store = storeName.startsWith(`${brand} `)
        ? storeName.slice(brand.length).trim()
        : storeName;

      Utils.showProgress('메뉴판 수집 중...', '메뉴 목록 스캔');
      this._menupanLiveLog(`메뉴 목록 스캔 시작: ${brand}/${store}`);
      const rows = await this._scrollCollectMenuItems({
        ...info,
        brand,
        store,
        store_id: storeId
      });
      this._menupanLiveLog(`메뉴 목록 스캔 완료: ${rows.length}행`);

      if (!rows.length) {
        Utils.showError('데이터 없음', '수집할 메뉴 데이터가 없습니다.');
        return { success: false, error: '메뉴 데이터 없음', store_name: storeName, store_id: storeId };
      }

      const csv = Utils.toCSV(window.BaeminPipelineSave.MENU_HEADERS, rows);
      const now = new Date();
      const today = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
      const filename = `[${brand}][${storeName}]baemin_menu_${today}.csv`;
      const downloadResult = await window.BaeminPipelineSave._download(csv, filename);

      if (downloadResult?.success !== true) {
        const downloadError = downloadResult?.error || '다운로드 응답 없음';
        Utils.showError('메뉴판 저장 실패', downloadError);
        return {
          success: false,
          error: downloadError,
          filename,
          rows: rows.length,
          store_name: storeName,
          store_id: storeId
        };
      }

      console.log(`[Collector] 메뉴판 ${rows.length}건 저장: ${filename}`);
      Utils.showSuccess('메뉴판 수집 완료', `${rows.length}건 → ${filename}`);
      return {
        success: true,
        filename,
        rows: rows.length,
        store_name: storeName,
        store_id: storeId
      };
    } catch (error) {
      const message = error?.message || String(error);
      console.warn('[Collector] 메뉴판 수집 실패:', error);
      Utils.showError('메뉴판 수집 실패', message);
      return { success: false, error: message };
    }
  },

  // 수동/배치 공통 로그: 수동 로그 패널이 떠 있으면 패널로, 아니면 콘솔로만 남긴다.
  _ordersLog(message, level = 'info') {
    if (document.getElementById('doridang-baemin-manual-orders-log')) {
      this._manualOrdersLog(message, level);
      return;
    }
    console.log(`[배민 orders] ${message}`);
  },

  _ordersPageCursor: 1,

  _getCurrentPage() {
    const activeDiv = document.querySelector('.Pagination_b_r4ax_pb5p4va');
    const parsed = parseInt(activeDiv?.textContent.trim() || '', 10);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
    const current = document.querySelector('[aria-current="page"], [aria-current="true"]');
    const fromAria = parseInt(current?.textContent.trim() || '', 10);
    if (Number.isFinite(fromAria) && fromAria > 0) return fromAria;
    return this._ordersPageCursor || 1;
  },

  _clickPage(pageNum) {
    const target = String(pageNum);

    const scoped = [...document.querySelectorAll('ul.Pagination_b_r4ax_pb5p4v5 button.Pagination_b_r4ax_pb5p4vb')];
    let btn = scoped.find(el => el.textContent.trim() === target);

    if (!btn) {
      // 배민 해시 클래스(vanilla-extract)는 재배포마다 회전 -> 텍스트/조상 기준 폴백
      const numeric = [...document.querySelectorAll('button')]
        .filter(el => el.textContent.trim() === target);
      btn = numeric.find(el => el.closest('[class*="Pagination"]')) || numeric[numeric.length - 1];
      if (btn) this._ordersLog(`페이지네이션 폴백 셀렉터 사용 -> ${pageNum}페이지`, 'warn');
    }

    if (!btn) return false;
    btn.click();
    this._ordersPageCursor = pageNum;
    return true;
  },

  _clickNextPage(currentPage) {
    return this._clickPage(currentPage + 1);
  },

  _orderRows() {
    return [...document.querySelectorAll('tr.Table_b_r4ax_1dwbr4on[data-index]')];
  },

  _detailSectionCount() {
    return document.querySelectorAll('td[colspan="9"] .DetailInfo-module__pZYe').length;
  },

  // '모두 펼쳐보기' / '모두 접기' 토글. 클래스가 회전해도 텍스트로 찾는다.
  _findExpandAllButton() {
    for (const btn of document.querySelectorAll('button')) {
      const text = (btn.textContent || '').replace(/\s+/g, '');
      if (text.includes('모두펼쳐보기')) return { button: btn, expanded: false };
      if (text.includes('모두접기')) return { button: btn, expanded: true };
    }
    return null;
  },

  async _expandAllOrders() {
    const rowCount = this._orderRows().length;
    const found = this._findExpandAllButton();

    if (found?.expanded) {
      this._ordersLog(`펼치기: 이미펼침 (행 ${rowCount}개 / 상세 ${this._detailSectionCount()}개)`);
      return;
    }

    if (found) {
      found.button.click();
      // 배민은 상세 섹션을 지연 렌더한다. 실제로 펼쳐질 때까지 최대 8초 확인.
      const deadline = Date.now() + 8000;
      while (Date.now() < deadline) {
        if (this._stopFlag) return;
        await new Promise(resolve => setTimeout(resolve, 200));
        if (this._findExpandAllButton()?.expanded || this._detailSectionCount() > 0) break;
      }
      await this._randomDelay(400, 700);
      const detailCount = this._detailSectionCount();
      this._ordersLog(
        `펼치기: 버튼클릭 (행 ${rowCount}개 / 상세 ${detailCount}개)`,
        detailCount > 0 ? 'ok' : 'warn'
      );
      return;
    }

    for (const row of this._orderRows()) {
      if (this._stopFlag) return;
      const svg = row.querySelector('td[data-td-index="0"] svg');
      if (svg?.getAttribute('aria-label') === '컨텐츠 펼치기') {
        svg.closest('td')?.click();
        await this._randomDelay(150, 300);
      }
    }
    this._ordersLog(`펼치기: 폴백(행별 클릭) (행 ${rowCount}개 / 상세 ${this._detailSectionCount()}개)`, 'warn');
  },

  // ───── 즉시할인 파트너부담/배민지원 분해 ─────
  // 배민의 해시 클래스(vanilla-extract)는 재배포마다 회전하므로
  // 텍스트 / data-atelier-component 기준 폴백을 함께 둔다.
  // 가장 중요한 점: 상세 시트를 항상 닫고, 닫힐 때까지 기다린다.
  // (이전 주문의 열린 시트를 다시 읽어 값이 복제되던 버그)

  _isSheetVisible(el) {
    if (!el || !el.isConnected) return false;
    if (el.closest('[aria-hidden="true"], [inert]')) return false;
    // checkVisibility 는 opacity/visibility/content-visibility 를 한 번에 판정한다 (Chrome 105+)
    if (typeof el.checkVisibility === 'function' &&
        !el.checkVisibility({ opacityProperty: true, visibilityProperty: true, contentVisibilityAuto: true })) {
      return false;
    }
    if (!el.getClientRects().length) return false;
    const st = getComputedStyle(el);
    if (st.visibility === 'hidden' || st.display === 'none') return false;
    if (parseFloat(st.opacity || '1') < 0.05) return false;
    // transform 으로 뷰포트 밖으로 밀어낸 시트는 닫힌 것으로 본다
    const r = el.getBoundingClientRect();
    const vh = window.innerHeight || document.documentElement.clientHeight;
    const vw = window.innerWidth || document.documentElement.clientWidth;
    if (r.bottom <= 0 || r.top >= vh || r.right <= 0 || r.left >= vw) return false;
    return true;
  },

  // 시트는 행마다 포털로 별도 mount 된다. 하나만 고르면 가장 오래된(이전 주문) 시트를 계속 읽게 된다.
  _findAllDiscountSheets() {
    const candidates = [...document.querySelectorAll(
      '.InstantDiscountDetailPageSheet-module__IbXh,' +
      '[role="dialog"],' +
      '[data-atelier-component="PageSheet"],' +
      '[class*="InstantDiscountDetailPageSheet-module__"]'
    )].filter((el) => {
      if (!this._isSheetVisible(el)) return false;
      const t = el.textContent || '';
      return /즉시할인/.test(t)
        && /(파트너\s*부담|가게\s*부담|점주\s*부담|배민\s*지원)/.test(t);
    });
    // 중첩된 후보는 바깥 것만 남긴다
    return candidates.filter((el) => !candidates.some((o) => o !== el && o.contains(el)));
  },

  // 폴백용. 여러 개면 가장 나중에 붙은 것을 쓴다.
  _findDiscountSheet() {
    const all = this._findAllDiscountSheets();
    return all.length ? all[all.length - 1] : null;
  },

  _resetDiscountStats() {
    this._discountSplitCount = 0;
    this._discountBlankCount = 0;
    this._discountBlank = { 시트없음: 0, 내용미갱신: 0, 파싱실패: 0, 합계불일치: 0 };
    this._lastAmountEl = null;
  },

  _countDiscountBlank(reason) {
    if (!this._discountBlank) this._resetDiscountStats();
    if (this._discountBlank[reason] === undefined) this._discountBlank[reason] = 0;
    this._discountBlank[reason]++;
    this._discountBlankCount++;
  },

  _sheetSignature(sheet) {
    if (!sheet) return { el: null, text: '' };
    return { el: sheet, text: (sheet.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 200) };
  },

  // prev(클릭 직전 시트)와 같은 시트를 새 시트로 오인하면 이전 주문 값이 복제된다.
  // 정상적으로 닫혔다면 prev.el 이 null 이라 곧바로 통과한다.
  // 트리거 클릭 전에 있던 시트 집합(before)에 없는 '신규' 시트를 우선 집는다.
  // 신규가 없으면 기존 시트 중 내용이 바뀐 것(시트를 재사용하는 화면 대비)을 집는다.
  async _waitNewDiscountSheet(before, prevSig, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    let sawSheet = false;
    while (Date.now() < deadline) {
      const all = this._findAllDiscountSheets();
      if (all.length) sawSheet = true;

      const fresh = all.find((el) => !before.has(el));
      if (fresh) return { sheet: fresh, reason: '신규' };

      if (prevSig?.el) {
        const updated = all.find((el) => el === prevSig.el && this._sheetSignature(el).text !== prevSig.text);
        if (updated) return { sheet: updated, reason: '갱신' };
      } else if (all.length) {
        return { sheet: all[all.length - 1], reason: '신규' };
      }

      await new Promise((r) => setTimeout(r, 100));
    }
    return { sheet: null, reason: sawSheet ? '내용미갱신' : '시트없음' };
  },

  _isSheetGone(sheet) {
    return !sheet || !sheet.isConnected || !this._isSheetVisible(sheet);
  },

  // 시트가 쌓이는 구조라 '문서에 시트가 하나도 없을 것'을 조건으로 삼으면
  // 닫기가 성공해도 항상 실패로 보고된다. 대상 요소 하나로만 판정한다.
  async _waitSheetGone(sheet, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (this._isSheetGone(sheet)) return true;
      await new Promise((r) => setTimeout(r, 100));
    }
    return this._isSheetGone(sheet);
  },

  _maxSheetCloseAttempt: -1,
  _discountSplitCount: 0,
  _discountBlankCount: 0,
  _discountBlank: null,
  _lastAmountEl: null,
  _sheetCloseHopeless: false,

  // React 의 바깥클릭 닫기 핸들러는 대부분 mousedown/pointerdown 에 붙는다.
  // el.click() 은 click 이벤트 하나만 쏘므로 시트가 영원히 안 닫혔다.
  _dispatchPointerSequence(target, clientX, clientY) {
    const el = target || document.body;
    const r = el.getBoundingClientRect();
    const x = Number.isFinite(clientX) ? clientX : Math.max(1, Math.round(r.left + 2));
    const y = Number.isFinite(clientY) ? clientY : Math.max(1, Math.round(r.top + 2));
    const opts = { bubbles: true, cancelable: true, composed: true, clientX: x, clientY: y, button: 0 };
    try { el.dispatchEvent(new PointerEvent('pointerdown', { ...opts, pointerId: 1, isPrimary: true })); } catch (_) {}
    el.dispatchEvent(new MouseEvent('mousedown', opts));
    try { el.dispatchEvent(new PointerEvent('pointerup', { ...opts, pointerId: 1, isPrimary: true })); } catch (_) {}
    el.dispatchEvent(new MouseEvent('mouseup', opts));
    el.dispatchEvent(new MouseEvent('click', opts));
  },

  // 툴팁/팝오버는 트리거에서 마우스가 벗어날 때 닫힌다.
  _dispatchHoverExit(el) {
    if (!el) return;
    const r = el.getBoundingClientRect();
    const opts = { bubbles: false, cancelable: true, composed: true, clientX: Math.round(r.left), clientY: Math.round(r.top) };
    const bubbling = { ...opts, bubbles: true, relatedTarget: document.body };
    try { el.dispatchEvent(new PointerEvent('pointerleave', { ...opts, pointerId: 1, isPrimary: true })); } catch (_) {}
    try { el.dispatchEvent(new PointerEvent('pointerout', { ...bubbling, pointerId: 1, isPrimary: true })); } catch (_) {}
    el.dispatchEvent(new MouseEvent('mouseleave', opts));
    el.dispatchEvent(new MouseEvent('mouseout', bubbling));
    try { el.blur?.(); } catch (_) {}
    // 다른 곳으로 포인터가 이동했음을 알린다
    try {
      document.body.dispatchEvent(new MouseEvent('mouseover', { bubbles: true, cancelable: true, composed: true, clientX: 1, clientY: 1 }));
      document.body.dispatchEvent(new MouseEvent('mousemove', { bubbles: true, cancelable: true, composed: true, clientX: 1, clientY: 1 }));
    } catch (_) {}
  },

  // 시트 박스 바깥의 실제 좌표를 골라, 그 지점의 최상위 요소(=딤)에 이벤트를 쏜다.
  // 사용자가 딤을 클릭하는 것과 가장 가깝고 딤의 클래스/구조가 바뀌어도 따라간다.
  _outsideSheetPoints(sheet) {
    const r = sheet.getBoundingClientRect();
    const vw = window.innerWidth || document.documentElement.clientWidth;
    const vh = window.innerHeight || document.documentElement.clientHeight;
    return [
      [Math.round(vw / 2), Math.max(4, Math.round(r.top / 2))],
      [Math.round(vw / 2), Math.min(vh - 4, Math.round((r.bottom + vh) / 2))],
      [Math.max(4, Math.round(r.left / 2)), Math.round(vh / 2)],
      [Math.min(vw - 4, Math.round((r.right + vw) / 2)), Math.round(vh / 2)]
    ].filter(([x, y]) =>
      x >= 1 && y >= 1 && x < vw && y < vh &&
      !(x >= r.left && x <= r.right && y >= r.top && y <= r.bottom)
    );
  },

  _clickOutsideSheet(sheet) {
    for (const [x, y] of this._outsideSheetPoints(sheet)) {
      const target = document.elementFromPoint(x, y);
      if (!target || sheet.contains(target)) continue;
      this._dispatchPointerSequence(target, x, y);
      return target;
    }
    return null;
  },

  // 딤은 시트의 부모다 (PageSheet_b_r4ax_… fixed z300).
  // 이전 구현은 'sheet 를 포함하지 않는 요소'만 찾아 정답을 걸러냈다.
  _findSheetOverlay(sheet) {
    let el = sheet.parentElement;
    while (el && el !== document.body) {
      if (getComputedStyle(el).position === 'fixed') return el;
      el = el.parentElement;
    }
    return this._findSheetBackdrop(sheet);
  },

  // 조상에서 못 찾았을 때의 보조 (딤이 형제인 구조 대비)
  _findSheetBackdrop(sheet) {
    const vh = window.innerHeight || document.documentElement.clientHeight;
    const vw = window.innerWidth || document.documentElement.clientWidth;
    return [...document.querySelectorAll('div')].find(el => {
      if (el === sheet || el.contains(sheet)) return false;
      const st = getComputedStyle(el);
      if (st.position !== 'fixed' && st.position !== 'absolute') return false;
      if (st.pointerEvents === 'none') return false;
      const r = el.getBoundingClientRect();
      return r.width >= vw * 0.9 && r.height >= vh * 0.9;
    }) || null;
  },

  _findSheetCloseButton(sheet) {
    const buttons = [...sheet.querySelectorAll('button, [role="button"]')];
    const attrOf = btn => `${btn.getAttribute('aria-label') || ''} ${btn.getAttribute('title') || ''}`;

    const labeled = buttons.find(btn => /닫기|close/i.test(attrOf(btn)));
    if (labeled) return labeled;

    const svgLabeled = sheet.querySelector('svg[aria-label*="닫기"], svg[aria-label*="close" i]');
    if (svgLabeled) return svgLabeled.closest('button, [role="button"]') || svgLabeled;

    const texted = buttons.find(btn => ['닫기', '확인'].includes(btn.textContent.trim()));
    if (texted) return texted;

    const iconBtn = sheet.querySelector('button[data-atelier-component="IconButton"]');
    if (iconBtn) return iconBtn;

    // 텍스트 없이 svg 하나만 가진 아이콘 버튼
    return buttons.find(btn => !btn.textContent.trim() && btn.querySelector('svg')) || null;
  },

  async _closeDiscountSheet(target, amountEl) {
    const sheet = target || this._findDiscountSheet();
    if (this._isSheetGone(sheet)) return true;

    for (let attempt = 0; attempt < 5; attempt++) {
      if (this._isSheetGone(sheet)) return true;

      if (attempt === 0) {
        // 백드롭이 없는 팝오버/툴팁으로 확인됐다 -> 트리거 호버 이탈이 1순위
        if (amountEl?.isConnected) this._dispatchHoverExit(amountEl);
      } else if (attempt === 1) {
        // 트리거 재클릭 토글
        if (amountEl?.isConnected) this._dispatchPointerSequence(amountEl);
      } else if (attempt === 2) {
        // 문서 레벨 바깥클릭 리스너용
        this._dispatchPointerSequence(document.body);
        for (const type of ['pointerdown', 'mousedown']) {
          try {
            document.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, composed: true, clientX: 1, clientY: 1 }));
          } catch (_) {}
        }
      } else if (attempt === 3) {
        this._clickOutsideSheet(sheet)
          || this._dispatchPointerSequence(this._findSheetOverlay(sheet) || document.documentElement);
        const btn = this._findSheetCloseButton(sheet);
        if (btn) this._dispatchPointerSequence(btn);
      } else {
        // 합성 ESC. 수집기 자신의 ESC 중단 리스너가 이걸 받아 멈추던 버그가 있어 억제 플래그로 감싼다.
        this._suppressEsc = true;
        try {
          for (const type of ['keydown', 'keyup']) {
            const ev = new KeyboardEvent(type, {
              key: 'Escape', code: 'Escape', keyCode: 27, which: 27, bubbles: true, cancelable: true
            });
            document.dispatchEvent(ev);
            window.dispatchEvent(new KeyboardEvent(type, {
              key: 'Escape', code: 'Escape', keyCode: 27, which: 27, bubbles: true, cancelable: true
            }));
          }
        } finally {
          this._suppressEsc = false;
        }
      }

      if (await this._waitSheetGone(sheet, 300)) {
        this._maxSheetCloseAttempt = Math.max(this._maxSheetCloseAttempt, attempt);
        return true;
      }
    }
    this._maxSheetCloseAttempt = 99;
    // 닫기는 부가 기능(DOM 정리)으로 강등됐다. 값의 정확성은 '신규 시트 감지'가 보장한다.
    this._sheetCloseHopeless = true;
    return this._isSheetGone(sheet);
  },

  // 행마다 시트가 쌓이면 DOM 이 무거워지고 화면을 가린다. 페이지 끝에서 한 번 훑어 닫는다.
  async _sweepDiscountSheets() {
    const sheets = this._findAllDiscountSheets();
    for (const sheet of sheets) {
      if (this._stopFlag) break;
      this._dispatchPointerSequence(document.body);
      this._clickOutsideSheet(sheet);
      await this._waitSheetGone(sheet, 200);
    }
    return this._findAllDiscountSheets().length;
  },

  _sheetDumped: false,

  // 닫기 4회가 모두 실패했을 때 남아 있는 시트의 실체를 한 번만 남긴다.
  // (어떤 방식으로 숨겨지는지 모르면 셀렉터를 계속 추측하게 된다)
  _dumpDiscountSheet() {
    if (this._sheetDumped) return;
    const sheet = this._findDiscountSheet();
    if (!sheet) return;
    this._sheetDumped = true;

    const cls = (el, take = 2) =>
      String(el?.className || '').trim().split(/\s+/).filter(Boolean).slice(0, take).join(' ')
      || (el?.tagName || '?').toLowerCase();
    const box = el => {
      const r = el.getBoundingClientRect();
      return `${Math.round(r.width)}x${Math.round(r.height)}`;
    };

    this._ordersLog('───── 즉시할인 시트 진단 (복사 버튼으로 전달) ─────', 'warn');

    // 시트가 행마다 쌓이는 구조인지 한 줄로 확인
    const allSheets = this._findAllDiscountSheets();
    const digest = allSheets.map((el, i) =>
      `#${i} "${(el.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 40)}"`
    );
    this._ordersLog(`시트 수: ${allSheets.length}\n${digest.join('\n')}`, 'warn');

    // 1) 시트 하위 구조
    const lines = [];
    const walk = (el, depth) => {
      if (lines.length >= 25 || depth > 4) return;
      const st = getComputedStyle(el);
      const text = (el.childNodes[0]?.nodeType === 3 ? el.childNodes[0].textContent : '').trim().slice(0, 20);
      lines.push(`${'  '.repeat(depth)}${el.tagName.toLowerCase()}.${cls(el)} [${box(el)}] cur=${st.cursor}${text ? ` "${text}"` : ''}`);
      for (const child of el.children) walk(child, depth + 1);
    };
    walk(sheet, 0);
    this._ordersLog(`시트 구조:\n${lines.join('\n')}`, 'warn');

    // 2) 부모 체인
    const chain = [];
    let node = sheet.parentElement;
    while (node && node !== document.body && chain.length < 8) {
      const st = getComputedStyle(node);
      chain.push(`${cls(node, 1)} ${st.position}/z${st.zIndex} [${box(node)}]`);
      node = node.parentElement;
    }
    this._ordersLog(`시트 부모체인: ${chain.join(' < ')}`, 'warn');

    // 3) svg (버튼이 아닌 닫기 요소 탐지)
    const svgs = [...sheet.querySelectorAll('svg')].slice(0, 5).map(svg =>
      `${cls(svg.parentElement, 1)}>svg[${svg.getAttribute('aria-label') || '-'}] vb=${svg.getAttribute('viewBox') || '-'} cur=${getComputedStyle(svg.parentElement || svg).cursor}`
    );
    this._ordersLog(`시트 svg: ${svgs.join(' | ') || '없음'}`, 'warn');

    // 4) 트리거 — 툴팁/팝오버 판별 결정타
    const trigger = this._lastAmountEl;
    if (trigger?.isConnected) {
      const st = getComputedStyle(trigger);
      const data = [...trigger.attributes].map(a => a.name).filter(n => n.startsWith('data-') || n.startsWith('aria-') || n === 'title' || n === 'role');
      const attrs = data.map(n => `${n}=${trigger.getAttribute(n)}`).join(' ');
      this._ordersLog(`트리거: ${trigger.tagName.toLowerCase()}.${cls(trigger)} [${box(trigger)}] cur=${st.cursor} ${attrs || '속성없음'}`, 'warn');
    } else {
      this._ordersLog('트리거: 없음(언마운트)', 'warn');
    }

    // 5) 바깥 클릭 좌표에서 실제로 잡히는 요소
    const hits = this._outsideSheetPoints(sheet).map(([x, y]) => {
      const el = document.elementFromPoint(x, y);
      return `(${x},${y})->${el ? el.tagName.toLowerCase() + '.' + cls(el) : 'null'}`;
    });
    this._ordersLog(`클릭지점: ${hits.join(' ') || '없음'}`, 'warn');

    // 6) 원문 HTML — 위 요약으로 안 풀리면 이걸로 확정
    const html = (sheet.outerHTML || '').replace(/\s+/g, ' ');
    this._ordersLog(`시트 HTML(1200자):\n${html.slice(0, 1200)}`, 'warn');
    this._ordersLog('───── 진단 끝 ─────', 'warn');

    console.warn('[baemin] 즉시할인 시트 진단', {
      sheet,
      sheetClass: String(sheet.className || ''),
      trigger,
      structure: lines,
      parents: chain,
      html: html.slice(0, 2500)
    });
  },

  _parseDiscountSheet(sheet) {
    const out = { partner: null, support: null };
    const items = sheet.querySelectorAll('[data-atelier-component="TextListItem"], li');

    for (const item of items) {
      const label = item.textContent || '';
      const isPartner = /(파트너|가게|점주)\s*부담/.test(label);
      const isSupport = /배민\s*지원/.test(label);
      if (!isPartner && !isSupport) continue;
      if (isPartner && out.partner !== null) continue;
      if (isSupport && out.support !== null) continue;

      const valueBox = item.querySelector('.TextListItem_b_r4ax_n197m77') || item.lastElementChild;
      if (!valueBox) continue;
      const spans = valueBox.querySelectorAll('span');
      let raw = spans.length ? spans[spans.length - 1].textContent : '';
      if (!/\d/.test(raw || '')) raw = valueBox.textContent || '';
      const value = Utils.cleanPrice(raw);
      if (value === '') continue;

      if (isPartner) out.partner = value;
      else out.support = value;
    }
    return out;
  },

  _findInstantDiscountAmountEl(detailSection) {
    if (!detailSection) return null;

    const container = detailSection.querySelector('.InstantDiscountDetailPageSheet-module__u8LB');
    if (container) {
      const el = container.querySelector('.InstantDiscountDetailPageSheet-module__siYJ');
      if (el) return el;
    }

    const labelEl = [...detailSection.querySelectorAll('[data-atelier-component="Typography"], span')]
      .find((el) => (el.textContent || '').trim() === '즉시할인');
    if (!labelEl) return null;

    let scope = labelEl.parentElement;
    for (let depth = 0; depth < 4 && scope; depth++) {
      const amount = [...scope.querySelectorAll('span')]
        .filter((el) => el !== labelEl && !el.contains(labelEl)
          && /\d/.test(el.textContent || '') && /원/.test(el.textContent || ''))
        .pop();
      if (amount) return amount;
      scope = scope.parentElement;
    }
    return null;
  },

  // 즉시할인 3컬럼을 한 번에 산출.
  // 분해 파싱에 성공하면 0은 반드시 '0' 으로 기록하고, 공란은 오직 '수집 실패' 만
  // 의미하게 한다 (DB_DeliveryCommission 폴백이 이 구분에 의존).
  async _readInstantDiscount(detailSection, orderNo) {
    const result = { 즉시할인: '', 파트너부담: '', 배민지원: '' };

    const amountEl = this._findInstantDiscountAmountEl(detailSection);
    if (!amountEl) return result;

    const total = Utils.cleanPrice(amountEl.textContent || '');
    if (total === '') return result;

    const T = parseInt(total, 10) || 0;
    result.즉시할인 = String(T);
    if (T <= 0) {
      result.파트너부담 = '0';
      result.배민지원 = '0';
      this._discountSplitCount++;
      return result;
    }

    this._lastAmountEl = amountEl;


    // 클릭 전 시트 집합. 이 안에 없는 시트가 이번 주문의 것이다.
    const before = new Set(this._findAllDiscountSheets());
    const prev = this._sheetSignature(before.size ? [...before][before.size - 1] : null);
    // 여는 쪽은 기존대로 click. _dispatchPointerSequence 는 mousedown 까지 쏘므로
    // 핸들러가 mousedown 이면 열자마자 click 으로 다시 닫힐 수 있다.
    amountEl.click();
    const found = await this._waitNewDiscountSheet(before, prev, 2500);
    const sheet = found.sheet;

    if (!sheet) {
      console.warn(`[baemin] 즉시할인 시트 ${found.reason} order=${orderNo} total=${T}`);
      this._countDiscountBlank(found.reason);
      this._dumpDiscountSheet();
      return result;
    }

    const { partner, support } = this._parseDiscountSheet(sheet);

    // 닫기는 실패해도 무방하다. 다음 주문은 '신규 시트'로 구분되기 때문이다.
    if (!this._sheetCloseHopeless) await this._closeDiscountSheet(sheet, amountEl);

    const p = partner === null ? null : (parseInt(partner, 10) || 0);
    const s = support === null ? null : (parseInt(support, 10) || 0);

    // 합이 맞지 않으면 틀린 숫자를 쓰느니 공란으로 둔다.
    // 공란은 오직 '수집 실패'만 의미한다 (DB_DeliveryCommission 폴백이 이 구분에 의존).
    if (p !== null && s !== null && p + s === T) {
      result.파트너부담 = String(p);
      result.배민지원 = String(s);
    } else if (p === null && s !== null && s >= 0 && s <= T) {
      result.파트너부담 = String(T - s);
      result.배민지원 = String(s);
    } else if (s === null && p !== null && p >= 0 && p <= T) {
      result.파트너부담 = String(p);
      result.배민지원 = String(T - p);
    } else {
      const reason = (p === null && s === null) ? '파싱실패' : '합계불일치';
      console.warn(`[baemin] 즉시할인 ${reason} order=${orderNo} total=${T} partner=${p} support=${s}`);
      this._countDiscountBlank(reason);
      this._dumpDiscountSheet();
      return result;
    }

    this._discountSplitCount++;
    return result;
  },

  _findOrdersScrollElement() {
    // self.baemin.com 주문내역은 페이지 전체가 스크롤된다. 문서 스크롤러를 우선한다.
    const documentScroller = document.scrollingElement || document.documentElement;
    if (documentScroller && documentScroller.scrollHeight > documentScroller.clientHeight + 50) {
      return documentScroller;
    }
    let el = this._orderRows()[0] || document.querySelector('table');
    while (el && el !== document.body) {
      const style = getComputedStyle(el);
      if (/(auto|scroll)/.test(style.overflowY) && el.scrollHeight > el.clientHeight + 50) {
        return el;
      }
      el = el.parentElement;
    }
    return document.scrollingElement || document.documentElement;
  },

  // 펼침 직후 상세 섹션은 지연 렌더된다. 해당 행의 상세 tr이 붙을 때까지 잠깐 기다린다.
  async _waitRowDetail(row, timeoutMs = 2500) {
    const hasDetail = () => {
      let sibling = row.nextElementSibling;
      while (sibling?.tagName === 'TR') {
        if (sibling.querySelector('td[colspan="9"] .DetailInfo-module__pZYe')) return true;
        sibling = sibling.nextElementSibling;
      }
      return false;
    };
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (hasDetail()) return true;
      await new Promise(resolve => setTimeout(resolve, 120));
    }
    return false;
  },

  // 배민 주문 테이블은 tr[data-index] 가상 스크롤이라 화면에 걸친 행만 DOM에 존재한다.
  // '모두 펼쳐보기'로 행 높이가 커지면 2~3행만 렌더돼 그만큼만 수집되던 버그가 있었다.
  // 메뉴판의 _scrollCollectMenuItems와 동일하게 스크롤하며 data-index 기준으로 dedupe 수집한다.
  async _collectCurrentPageData(iso) {
    const container = this._findOrdersScrollElement();
    const documentScroller = document.scrollingElement || document.documentElement;
    const usesWindow = container === documentScroller;
    const scrollTopOf = () => usesWindow ? window.scrollY : container.scrollTop;
    const viewportOf = () => usesWindow ? window.innerHeight : container.clientHeight;
    const maxScrollOf = () => Math.max(0, container.scrollHeight - viewportOf());
    const scrollTo = y => {
      if (usesWindow) window.scrollTo(0, y);
      else container.scrollTop = y;
    };

    const processed = new Set();
    const pageRows = [];
    let maxIndex = -1;
    let minIndex = -1;
    let lastLogAt = 0;
    this._maxSheetCloseAttempt = -1;
    this._sheetCloseHopeless = false;
    this._resetDiscountStats();

    scrollTo(0);
    await this._randomDelay(350, 550);

    let idleRounds = 0;
    for (let guard = 0; guard < 60; guard++) {
      if (this._stopFlag) break;

      let added = 0;
      for (const row of this._orderRows()) {
        if (this._stopFlag) break;
        const idx = row.getAttribute('data-index');
        const orderNo = row.querySelector('td[data-td-index="1"]')?.textContent.trim() || '';
        const key = idx !== null ? `index-${idx}` : `order-${orderNo}`;
        if (key === 'order-' || processed.has(key)) continue;
        processed.add(key);
        if (idx !== null) {
          const idxNum = parseInt(idx, 10) || 0;
          maxIndex = maxIndex < 0 ? idxNum : Math.max(maxIndex, idxNum);
          minIndex = minIndex < 0 ? idxNum : Math.min(minIndex, idxNum);
        }

        // 즉시할인 상세시트가 열리면 스크롤이 튄다 -> 행 단위로 위치를 복원한다.
        const keepTop = scrollTopOf();
        const live = idx !== null
          ? document.querySelector(`tr.Table_b_r4ax_1dwbr4on[data-index="${idx}"]`) || row
          : row;
        if (this._findExpandAllButton()?.expanded) await this._waitRowDetail(live);
        pageRows.push(...await this._collectRowsData([live], iso));
        if (Math.abs(scrollTopOf() - keepTop) > 2) scrollTo(keepTop);
        added++;
      }

      const currentTop = scrollTopOf();
      const maxScroll = maxScrollOf();

      if (Date.now() - lastLogAt >= 3000) {
        lastLogAt = Date.now();
        this._ordersLog(`수집 진행: 주문 ${processed.size}건 / ${pageRows.length}행 (scroll ${Math.round(currentTop)}/${Math.round(maxScroll)})`);
      }
      Utils.updateProgressModal({ rowCount: pageRows.length });

      // 가상 리스트라면 한 화면 스크롤은 반드시 새 행을 드러낸다.
      // 두 라운드 연속 새 행이 없으면 더 볼 게 없다(비가상 테이블은 1라운드에 끝난다).
      if (added === 0) {
        idleRounds++;
        if (idleRounds >= 2) break;
      } else {
        idleRounds = 0;
      }

      if (currentTop >= maxScroll - 4) break;

      scrollTo(Math.min(currentTop + viewportOf() * 0.7, maxScroll));
      await this._randomDelay(350, 550);

      // 상세시트가 body 스크롤 락을 남기면 스크롤이 먹지 않는다 -> 무한 루프 방지
      if (added === 0 && Math.abs(scrollTopOf() - currentTop) < 4) break;
    }

    const leftoverSheets = await this._sweepDiscountSheets();

    if (this._discountSplitCount || this._discountBlankCount || this._maxSheetCloseAttempt >= 1) {
      const bad = this._discountBlankCount > 0 || this._maxSheetCloseAttempt === 99;
      const attemptText = this._maxSheetCloseAttempt === 99
        ? '실패'
        : this._maxSheetCloseAttempt < 0 ? '해당없음' : String(this._maxSheetCloseAttempt);
      const reasons = Object.entries(this._discountBlank || {})
        .map(([key, value]) => `${key} ${value}`).join(', ');
      this._ordersLog(
        `즉시할인: 분해 ${this._discountSplitCount}건 / 공란 ${this._discountBlankCount}건(${reasons})` +
        ` / 닫기 attempt ${attemptText}` +
        (this._sheetCloseHopeless ? ' / 닫기불가' : '') +
        ` / 잔존시트 ${leftoverSheets}개`,
        bad ? 'err' : 'ok'
      );
    }

    scrollTo(0);
    const indexSpan = maxIndex >= 0 ? maxIndex - minIndex + 1 : -1;
    return { rows: pageRows, orderCount: processed.size, maxIndex, minIndex, indexSpan };
  },

  async _collectRowsData(rows, iso) {
    const pageRows = [];

    for (const row of rows) {
      if (this._stopFlag) break;
      
      const base = {
        collected_at: iso,
        store_name: this._currentShopInfo.store_name,
        주문상태: '', 주문번호: '', 주문시각: '', 광고상품: '', 캠페인ID: '',
        결제타입: '', 수령방법: '', 결제금액: '',
        상품금액: '', 즉시할인: '', 즉시할인_파트너부담: '', 즉시할인_배민지원: '', 배민부담_쿠폰할인: '', 총결제금액: '',
        주문중개: '', 고객할인비용: '', 배달: '', 그외: '', 부가세: '', 만나서결제금액: '', 입금예정금액: ''
      };

      for (const cell of row.querySelectorAll('td')) {
        const idx = cell.getAttribute('data-td-index');
        const text = cell.textContent.trim();
        if (idx === '1') {
          const badge = cell.querySelector('.Badge_b_r4ax_19agxiso, [data-atelier-component="Badge"] span');
          base.주문상태 = badge?.textContent.trim() || '';
          base.주문번호 = text.replace(base.주문상태, '').trim();
        }
        else if (idx === '2') base.주문시각 = text.replace(/\s+/g, ' ');
        else if (idx === '3') base.광고상품 = text;
        else if (idx === '4') base.캠페인ID = text;
        else if (idx === '6') base.결제타입 = text;
        else if (idx === '7') base.수령방법 = text;
        else if (idx === '8') base.결제금액 = Utils.cleanPrice(text);
      }

      let detailSection = null;
      let sibling = row.nextElementSibling;
      while (sibling?.tagName === 'TR') {
        const section = sibling.querySelector('td[colspan="9"] .DetailInfo-module__pZYe');
        if (section) {
          detailSection = section;
          break;
        }
        sibling = sibling.nextElementSibling;
      }

      if (!detailSection) {
        pageRows.push({ ...base, 주문내역: '', 주문수량: '', 주문옵션상세: '', 주문옵션금액: '' });
        continue;
      }

      const allSections = detailSection.querySelectorAll('section.DetailInfo-module__Sopx');
      let orderSection = null;
      let settleSection = null;
      
      for (const sec of allSections) {
        const headerText = sec.querySelector('.DetailInfo-module__bKQt')?.textContent || '';
        if (headerText.includes('정산정보')) {
          settleSection = sec;
        } else if (headerText.includes('주문정보')) {
          orderSection = sec;
        }
      }
      
      if (!orderSection && allSections.length > 0) {
        orderSection = allSections[0];
      }

      if (settleSection) {
        const allFieldItems = settleSection.querySelectorAll('.FieldItem-module__gYJs');
        for (const item of allFieldItems) {
          const labelEl = item.querySelector('.FieldItem-module__YCcw');
          const valueEl = item.querySelector('.FieldItem-module__rb57');
          if (!labelEl || !valueEl) continue;
          
          const label = labelEl.textContent.trim();
          const value = Utils.cleanPriceWithSign(valueEl.textContent);
          
          if (label.includes('(A)')) base.주문중개 = value;
          else if (label.includes('(B)')) base.배달 = value;
          else if (label.includes('(C)')) base.그외 = value;
          else if (label.includes('(D)')) base.부가세 = value;
          else if (label.includes('(E)') || label.includes('만나서결제금액')) base.만나서결제금액 = value;
          else if (label.includes('입금예정금액')) base.입금예정금액 = value;
        }
        
        const settleContents = settleSection.querySelectorAll('.SettleContent-module__Bji5 li');
        for (const li of settleContents) {
          const labelText = li.textContent || '';
          if (labelText.includes('고객할인비용')) {
            const valueEl = li.closest('p')?.querySelector('.SettleContent-module__E2ID');
            base.고객할인비용 = Utils.cleanPrice(valueEl?.textContent || '');
            break;
          }
        }
      }

      base.배민부담_쿠폰할인 = '';

      const discountSplit = await this._readInstantDiscount(detailSection, base.주문번호);
      base.즉시할인 = discountSplit.즉시할인;
      base.즉시할인_파트너부담 = discountSplit.파트너부담;
      base.즉시할인_배민지원 = discountSplit.배민지원;

      const couponDiscountEl = detailSection?.querySelector('.CouponDiscount-module__rTI9');
      if (couponDiscountEl) {
        const parentLi = couponDiscountEl.closest('li');
        if (parentLi) {
          const valueEl = parentLi.querySelector('.TextListItem_b_r4ax_n197m77 span, .TextListItem_b_r4ax_n197m76 p');
          base.배민부담_쿠폰할인 = Utils.cleanPrice(valueEl?.textContent || '');
        }
      }

      const totalEl = orderSection?.querySelector('.DetailInfo-module__PmTR .FieldItem-module__LyiN');
      if (totalEl) base.총결제금액 = Utils.cleanPrice(totalEl.textContent);

      const menuContainer = orderSection?.querySelector('.DetailInfo-module__j9yH');
      const allMenuData = [];

      if (menuContainer) {
        const menuBlocks = menuContainer.querySelectorAll('.DetailInfo-module__pC_2');
        
        for (const menuBlock of menuBlocks) {
          const menuInfoEl = menuBlock.querySelector('.DetailInfo-module__nV94');
          const menuNameEl = menuInfoEl?.querySelector('span:first-child');
          const menuQtyEl = menuInfoEl?.querySelector('.DetailInfo-module__QGJz');
          const menuPriceEl = menuBlock.querySelector('.FieldItem-module__rb57');
          
          const menuName = menuNameEl?.textContent.trim() || '';
          const menuQty = menuQtyEl?.textContent.replace(/[^\d]/g, '') || '1';
          const menuPrice = Utils.cleanPrice(menuPriceEl?.textContent || '');

          let optionsContainer = menuBlock.nextElementSibling;
          const options = [];
          
          if (optionsContainer?.classList.contains('DetailInfo-module__J1rX')) {
            const optDivs = optionsContainer.querySelectorAll('.DetailInfo-module__n2Ro');
            for (const opt of optDivs) {
              const spans = opt.querySelectorAll('span');
              const optName = spans[0]?.textContent.trim() || '';
              
              let optPrice = '';
              const priceSpan = spans[1];
              if (priceSpan) {
                const originalPriceEl = priceSpan.querySelector('.DetailInfo-module__t8S5');
                if (originalPriceEl) {
                  const clone = priceSpan.cloneNode(true);
                  clone.querySelector('.DetailInfo-module__t8S5')?.remove();
                  optPrice = Utils.cleanPrice(clone.textContent);
                } else {
                  optPrice = Utils.cleanPrice(priceSpan.textContent);
                }
              }
              
              if (optName) options.push({ name: optName, price: optPrice });
            }
          }

          if (options.length === 0) {
            options.push({ name: menuName, price: menuPrice });
          }

          if (menuName) {
            allMenuData.push({ menuName, menuQty, menuPrice, options });
          }
        }
      }

      const totalMenuPrice = allMenuData.reduce((sum, m) => sum + (parseInt(m.menuPrice) || 0), 0);
      if (totalMenuPrice > 0) {
        base.상품금액 = totalMenuPrice.toString();
      }

      const orderSummary = allMenuData.length > 0 
        ? allMenuData[0].menuName + (allMenuData.length > 1 ? ` 외 ${allMenuData.length - 1}건` : '')
        : '';

      let isFirstRow = true;

      for (const menu of allMenuData) {
        let isFirstOptionOfMenu = true;
        
        for (let i = 0; i < menu.options.length; i++) {
          const opt = menu.options[i];
          
          let optionName = opt.name;
          if (i === 0 && opt.name === '기본') {
            optionName = menu.menuName;
          }
          
          const rowData = {
            collected_at: base.collected_at,
            store_name: base.store_name,
            주문상태: base.주문상태,
            주문번호: base.주문번호,
            주문시각: base.주문시각,
            광고상품: base.광고상품,
            캠페인ID: base.캠페인ID,
            주문내역: orderSummary,
            주문수량: menu.menuQty,
            결제타입: base.결제타입,
            수령방법: base.수령방법,
            결제금액: isFirstRow ? base.결제금액 : '',
            상품금액: isFirstRow ? base.상품금액 : '',
            즉시할인: isFirstRow ? base.즉시할인 : '',
            즉시할인_파트너부담: isFirstRow ? base.즉시할인_파트너부담 : '',
            즉시할인_배민지원: isFirstRow ? base.즉시할인_배민지원 : '',
            배민부담_쿠폰할인: isFirstRow ? base.배민부담_쿠폰할인 : '',
            총결제금액: isFirstRow ? base.총결제금액 : '',
            주문옵션상세: optionName,
            주문옵션금액: opt.price,
            주문중개: isFirstRow ? base.주문중개 : '',
            고객할인비용: isFirstRow ? base.고객할인비용 : '',
            배달: isFirstRow ? base.배달 : '',
            그외: isFirstRow ? base.그외 : '',
            부가세: isFirstRow ? base.부가세 : '',
            만나서결제금액: isFirstRow ? base.만나서결제금액 : '',
            입금예정금액: isFirstRow ? base.입금예정금액 : ''
          };
          pageRows.push(rowData);
          isFirstRow = false;
          isFirstOptionOfMenu = false;
        }
      }

      if (allMenuData.length === 0) {
        pageRows.push({ ...base, 주문내역: '', 주문수량: '', 주문옵션상세: '', 주문옵션금액: '' });
      }
    }
    return pageRows;
  },

  _currentShopInfo: null,

  // 동작은 바꾸지 않는다. URL에 날짜가 없으면 배민 기본 범위로 수집되므로 로그로만 남긴다.
  _ordersDateRangeText() {
    const start = location.href.match(/startDate=(\d{4}-\d{2}-\d{2})/)?.[1] || '';
    const end = location.href.match(/endDate=(\d{4}-\d{2}-\d{2})/)?.[1] || '';
    if (start || end) return `${start || '?'} ~ ${end || '?'} (URL)`;
    const badge = [...document.querySelectorAll('button')]
      .map(btn => (btn.innerText || btn.textContent || '').replace(/\s+/g, ' ').trim())
      .find(text => /\d{4}[.\-\/]\d{1,2}[.\-\/]\d{1,2}/.test(text));
    return badge ? `${badge} (화면 필터)` : 'URL에 날짜 없음 - 배민 기본 범위';
  },

  async _collectOrdersAllPages(shopInfo) {
    if (shopInfo.needFilter) {
      return { success: false, error: '가게 필터 필요', shopInfo };
    }

    this._currentShopInfo = shopInfo;
    this._ordersPageCursor = 1;
    this._sheetDumped = false;

    Utils.showProgressModal('주문 내역 수집', '첫 페이지 로딩 중...');
    this._ordersLog(`수집 범위: ${this._ordersDateRangeText()}`);

    // 화면에 떠 있는 페이지부터 수집하면 앞 페이지가 통째로 빠진다.
    const startPage = this._getCurrentPage();
    if (startPage > 1) {
      this._ordersLog(`현재 ${startPage}페이지 -> 1페이지로 이동 후 수집`, 'warn');
      const prevFirstOrderId = this._getFirstOrderId();
      if (this._clickPage(1)) {
        // 이미 1페이지인데 판정이 틀렸을 수도 있으므로 짧게만 기다린다.
        const deadline = Date.now() + 5000;
        while (Date.now() < deadline) {
          await new Promise(resolve => setTimeout(resolve, 200));
          if (this._getCurrentPage() === 1 && this._getFirstOrderId() !== prevFirstOrderId) break;
        }
        await this._randomDelay(500, 800);
      }
    }
    
    const allRows = [];
    const iso = new Date().toISOString();
    let pageCount = 0;
    let hasError = false;
    let errorMessage = '';

    while (!this._stopFlag) {
      const currentPage = this._getCurrentPage();
      pageCount++;
      
      Utils.updateProgressModal({
        currentPage: pageCount,
        rowCount: allRows.length,
        message: `${currentPage}페이지 펼치는 중...`
      });
      
      await this._expandAllOrders();
      if (this._stopFlag) {
        console.log('[배민] ESC로 중단됨');
        break;
      }
      
      await this._randomDelay(300, 500);
      
      Utils.updateProgressModal({ message: `${currentPage}페이지 수집 중...` });
      
      const pageResult = await this._collectCurrentPageData(iso);
      allRows.push(...pageResult.rows);

      Utils.updateProgressModal({ rowCount: allRows.length });

      const missing = pageResult.indexSpan > pageResult.orderCount;
      this._ordersLog(
        `${currentPage}페이지: 주문 ${pageResult.orderCount}건 / ${pageResult.rows.length}행` +
        (pageResult.maxIndex >= 0 ? ` (data-index ${pageResult.minIndex}~${pageResult.maxIndex})` : '') +
        (missing ? ` — 누락 의심 (${pageResult.indexSpan - pageResult.orderCount}건)` : ''),
        missing ? 'warn' : 'ok'
      );
      console.log(`[배민] ${currentPage}페이지: ${pageResult.rows.length}행 수집 (총 ${allRows.length}행)`);
      
      const prevFirstOrderId = this._getFirstOrderId();
      const hasNext = this._clickNextPage(currentPage);
      if (!hasNext) {
        console.log('[배민] 마지막 페이지 도달');
        break;
      }
      
      Utils.updateProgressModal({ message: `${currentPage + 1}페이지 로딩 대기...` });
      
      const loaded = await this._waitForPageLoad(prevFirstOrderId, 15000);
      if (!loaded) {
        if (this._stopFlag) {
          console.log('[배민] ESC로 중단됨');
        } else {
          console.log(`[배민] ${currentPage + 1}페이지 로딩 타임아웃`);
          hasError = true;
          errorMessage = `${currentPage + 1}페이지 로딩 타임아웃`;
        }
        break;
      }
      await this._randomDelay(500, 800);
    }

    if (this._stopFlag) {
      this._ordersLog('ESC 중단 감지 — 부분 수집 상태로 종료', 'err');
    } else if (hasError) {
      this._ordersLog(`중단: ${errorMessage}`, 'warn');
    }

    if (allRows.length === 0) {
      Utils.showSuccessModal('데이터 없음', '추출할 주문이 없습니다.', { status: 'error' });
      this._ordersLog('수집된 주문 없음', 'err');
      return { success: false, error: '수집된 주문 없음', rows: 0 };
    }

    const headers = [
      'collected_at', 'store_name', '주문상태', '주문번호', '주문시각',
      '광고상품', '캠페인ID', '주문내역', '주문수량', '결제타입', '수령방법', 
      '결제금액', '상품금액', '즉시할인', '즉시할인_파트너부담', '즉시할인_배민지원', '배민부담_쿠폰할인', '총결제금액', 
      '주문옵션상세', '주문옵션금액',
      '주문중개', '고객할인비용', '배달', '그외', '부가세', '만나서결제금액', '입금예정금액'
    ];
    const csv = Utils.toCSV(headers, allRows);
    
    const dateMatch = location.href.match(/startDate=(\d{4}-\d{2}-\d{2})/);
    const dateStr = dateMatch ? dateMatch[1].replace(/-/g, '') : Utils.getTodayStr();
    
    const filename = Utils.downloadCSV(csv, {
      channel: 'baemin',
      purpose: 'orders',
      storeName: shopInfo.store_name,
      storeId: shopInfo.store_id,
      dateStr: dateStr
    });
    
    const uniqueOrders = new Set(allRows.map(r => r.주문번호)).size;
    
    let status = 'success';
    let statusMsg = '';
    if (this._stopFlag) {
      status = 'warning';
      statusMsg = '(ESC 중단됨)';
    } else if (hasError) {
      status = 'warning';
      statusMsg = `(${errorMessage})`;
    }
    
    const details = `📊 수집 결과:
- ${pageCount}페이지 처리
- ${uniqueOrders}건 주문
- 총 ${allRows.length.toLocaleString()}행

📁 파일명:
${filename}`;
    
    Utils.showSuccessModal(`주문 내역 수집 완료 ${statusMsg}`, details, { status });
    return { success: true, filename, rows: allRows.length, partial: this._stopFlag || hasError };
  },

  async _collectMarketingData(shopInfo) {
    const storeIdMatch = location.pathname.match(/\/shops\/(\d+)\//);
    const monthMatch = location.href.match(/initialMonth=(\d{4}-\d{2})/);
    
    const store_id = storeIdMatch?.[1] || shopInfo.store_id || '';
    const yearMonth = monthMatch?.[1] || '';
    const [year, month] = yearMonth.split('-');
    
    if (!year || !month) {
      Utils.showError('날짜 정보 없음', 'URL에서 월 정보를 찾을 수 없습니다.');
      return { success: false, error: '날짜 정보 없음' };
    }
    
    // StoreRegistry에서 매장명 조회
    let store_name = shopInfo.store_name;
    if (!store_name && store_id) {
      store_name = await Utils.StoreRegistry.get('baemin', store_id);
      console.log(`[마케팅 수집] StoreRegistry에서 조회: ${store_id} → ${store_name}`);
    }
    
    Utils.showProgress('마케팅 데이터 수집 중...', '테이블 로딩 대기');

    let table = null;
    let rows = [];
    const waitStart = Date.now();
    while (Date.now() - waitStart < 45000) {
      const expandBtn = [...document.querySelectorAll('button[data-atelier-component="Button"], button')]
        .find(btn => (btn.textContent || '').includes('전체보기'));

      if (expandBtn) {
        expandBtn.click();
        await this._randomDelay(500, 800);
      }

      table = document.querySelector('table[data-atelier-component="Table"]') || document.querySelector('table');
      rows = table ? [...table.querySelectorAll('tbody tr.Table_b_r4ax_1dwbr4on, tbody tr')] : [];
      if (table && rows.length > 0) break;

      Utils.showProgress('마케팅 데이터 수집 중...', `테이블 로딩 대기 ${Math.floor((Date.now() - waitStart) / 1000)}초`);
      await this._randomDelay(800, 1200);
    }

    if (!table) {
      Utils.showError('테이블 없음', '마케팅 데이터 테이블을 찾을 수 없습니다.');
      return { success: false, error: `마케팅 데이터 테이블 없음 (${location.href})` };
    }
    if (!rows.length) {
      Utils.showSuccess('마케팅 데이터 없음', `우가클 ${yearMonth} 테이블은 비어 있습니다.`);
      return { success: true, empty: true, rows: 0 };
    }
    const allRows = [];
    const iso = new Date().toISOString();
    
    for (const row of rows) {
      const cells = row.querySelectorAll('td');
      if (cells.length < 7) continue;
      
      const rawDate = cells[0]?.querySelector('.styles-module__x4Tk')?.textContent.trim() || '';
      const dayMatch = rawDate.match(/(\d+)\.(\d+)/);
      let formattedDate = '';
      if (dayMatch) {
        const pageMonth = dayMatch[1].padStart(2, '0');
        const day = dayMatch[2].padStart(2, '0');
        formattedDate = `${year}-${pageMonth}-${day}`;
      }
      
      const getValue = (cell) => {
        const text = cell?.querySelector('.styles-module__x4Tk')?.textContent.trim() || '';
        return text.replace(/[,원회건배]/g, '').trim();
      };
      
      allRows.push({
        collected_at: iso,
        store_id: store_id,
        store_name: store_name,  // 매장명 추가
        날짜: formattedDate,
        광고지출: getValue(cells[1]),
        노출수: getValue(cells[2]),
        클릭수: getValue(cells[3]),
        주문수: getValue(cells[4]),
        주문금액: getValue(cells[5]),
        광고효과: getValue(cells[6])
      });
    }
    
    if (!allRows.length) {
      Utils.showSuccess('마케팅 데이터 없음', `우가클 ${yearMonth} 수집 결과가 비어 있습니다.`);
      return { success: true, empty: true, rows: 0 };
    }
    
    const headers = [
      'collected_at', 'store_id', 'store_name', '날짜',
      '광고지출', '노출수', '클릭수', '주문수', '주문금액', '광고효과'
    ];
    
    const csv = Utils.toCSV(headers, allRows);
    const filename = await Utils.downloadCSV(csv, {
      channel: 'baemin',
      purpose: 'marketing',
      storeName: store_name,
      storeId: store_id,
      dateStr: yearMonth.replace('-', '')
    });
    
    // 다음 페이지 URL
    const nextUrl = `https://self.baemin.com/orders/history`;
    
    // 자동 이동
    const confirmMsg = `✅ 마케팅 데이터 수집 완료!\n\n📁 파일: ${filename}\n📊 ${allRows.length}건 수집\n\n⏭️ 주문 내역 페이지로 이동하시겠습니까?`;
    
    if (!this._batchMode) {
      await Utils.navigateWithConfirm(nextUrl, confirmMsg);
    }
    return { success: true, filename, rows: allRows.length };
  },

  async _collectAdvertisementData(shopInfo) {
    const storeIdMatch = location.pathname.match(/\/shops\/(\d+)\//);
    const monthMatch = location.href.match(/initialMonth=(\d{4}-\d{2})/);
    
    const store_id = storeIdMatch?.[1] || shopInfo.store_id || '';
    const yearMonth = monthMatch?.[1] || '';
    
    if (!yearMonth) {
      Utils.showError('날짜 정보 없음', 'URL에서 월 정보를 찾을 수 없습니다.');
      return { success: false, error: '날짜 정보 없음' };
    }
    
    const iso = new Date().toISOString();
    const allData = {};
    
    const adEffectTabs = ['노출수', '클릭수'];
    const orderInfoTabs = ['주문수', '주문금액'];
    
    Utils.showProgress('광고 성과 수집 중...', '광고효과 데이터');
    for (const tabName of adEffectTabs) {
      const tabBtn = [...document.querySelectorAll('button[role="tab"]')]
        .find(btn => btn.textContent.trim() === tabName && btn.id?.includes('rdp-trigger'));
      
      if (tabBtn) {
        tabBtn.click();
        await this._randomDelay(400, 600);
        
        const chartData = this._extractChartData();
        if (chartData) allData[tabName] = chartData;
      }
    }
    
    Utils.showProgress('광고 성과 수집 중...', '주문정보 데이터');
    for (const tabName of orderInfoTabs) {
      const tabBtn = [...document.querySelectorAll('button[role="tab"]')]
        .find(btn => btn.textContent.trim() === tabName && btn.id?.includes('rf5-trigger'));
      
      if (tabBtn) {
        tabBtn.click();
        await this._randomDelay(400, 600);
        
        const chartData = this._extractChartDataWithCategories();
        if (chartData) allData[tabName] = chartData;
      }
    }
    
    const rows = this._mergeAdvertisementData(allData, store_id, iso);
    
    if (!rows.length) {
      Utils.showError('데이터 없음', '수집할 광고 성과 데이터가 없습니다.');
      return { success: false, error: '광고 성과 데이터 없음' };
    }
    
    const headers = [
      'collected_at', 'store_id', '월',
      '노출수', '클릭수', 
      '주문수_전체', '주문수_오픈리스트', '주문수_배민1',
      '주문금액_전체', '주문금액_오픈리스트', '주문금액_배민1'
    ];
    
    const csv = Utils.toCSV(headers, rows);
    const filename = await Utils.downloadCSV(csv, {
      channel: 'baemin',
      purpose: 'advertisement',
      storeName: shopInfo.store_name,
      storeId: store_id,
      dateStr: yearMonth.replace('-', '')
    });
    
    Utils.showSuccess('광고 성과 수집 완료', `${rows.length}건 → ${filename}`);
    return { success: true, filename, rows: rows.length };
  },

  _extractChartData() {
    const svg = document.querySelector('svg.BaseChart-module__PRor');
    if (!svg) return null;
    
    const yMax = this._getYAxisMax(svg);
    if (!yMax) return null;
    
    const xLabels = this._getXAxisLabels(svg);
    const path = svg.querySelector('path[stroke="#223247"], path[stroke="#313335"]');
    if (!path) return null;
    
    const points = this._parsePathD(path.getAttribute('d'));
    const chartHeight = 215;
    
    const result = {};
    points.forEach((point, idx) => {
      const value = Math.round((chartHeight - point.y) / chartHeight * yMax);
      const label = xLabels[idx] || `${idx + 1}`;
      result[label] = value;
    });
    
    return result;
  },

  _extractChartDataWithCategories() {
    const svg = document.querySelector('svg.BaseChart-module__PRor');
    if (!svg) return null;
    
    const yMax = this._getYAxisMax(svg);
    if (!yMax) return null;
    
    const xLabels = this._getXAxisLabels(svg);
    
    const colorMap = {
      '#313335': '전체',
      '#2AC1BC': '오픈리스트', 
      '#1A7CFF': '배민1'
    };
    
    const paths = svg.querySelectorAll('path[fill="none"][stroke-width="2"]');
    const chartHeight = 215;
    const result = {};
    
    for (const path of paths) {
      const stroke = path.getAttribute('stroke');
      const category = colorMap[stroke];
      if (!category) continue;
      
      const points = this._parsePathD(path.getAttribute('d'));
      
      points.forEach((point, idx) => {
        const value = Math.round((chartHeight - point.y) / chartHeight * yMax);
        const label = xLabels[idx] || `${idx + 1}`;
        
        if (!result[label]) result[label] = {};
        result[label][category] = value;
      });
    }
    
    return result;
  },

  _getYAxisMax(svg) {
    const ticks = svg.querySelectorAll('g.tick text');
    if (!ticks.length) return null;
    
    let maxText = '';
    let minY = Infinity;
    
    for (const tick of ticks) {
      const transform = tick.closest('g.tick')?.getAttribute('transform') || '';
      const match = transform.match(/translate\(0,\s*([\d.]+)\)/);
      if (match) {
        const y = parseFloat(match[1]);
        if (y < minY) {
          minY = y;
          maxText = tick.textContent.trim();
        }
      }
    }
    
    return this._parseKoreanNumber(maxText);
  },

  _parseKoreanNumber(text) {
    if (!text) return 0;
    
    let value = 0;
    const 만Match = text.match(/([\d.]+)만/);
    const 천Match = text.match(/([\d.]+)천/);
    const 억Match = text.match(/([\d.]+)억/);
    
    if (억Match) value += parseFloat(억Match[1]) * 100000000;
    if (만Match) value += parseFloat(만Match[1]) * 10000;
    if (천Match) value += parseFloat(천Match[1]) * 1000;
    
    if (!만Match && !천Match && !억Match) {
      value = parseFloat(text.replace(/,/g, '')) || 0;
    }
    
    return value;
  },

  _getXAxisLabels(svg) {
    const labels = [];
    const xTexts = svg.querySelectorAll('g.LineChart-module__IKyX text');
    
    for (const text of xTexts) {
      const content = text.textContent.trim();
      if (content.includes('월')) {
        labels.push(content);
      }
    }
    
    return labels;
  },

  _parsePathD(d) {
    if (!d) return [];
    
    const points = [];
    const regex = /([ML])([\d.]+),([\d.]+)/g;
    let match;
    
    while ((match = regex.exec(d)) !== null) {
      points.push({
        x: parseFloat(match[2]),
        y: parseFloat(match[3])
      });
    }
    
    return points;
  },

  _mergeAdvertisementData(allData, store_id, iso) {
    const months = new Set();
    
    for (const metric in allData) {
      const data = allData[metric];
      if (typeof data === 'object') {
        for (const month in data) {
          months.add(month);
        }
      }
    }
    
    const rows = [];
    for (const month of months) {
      rows.push({
        collected_at: iso,
        store_id: store_id,
        '월': month,
        '노출수': allData['노출수']?.[month] || '',
        '클릭수': allData['클릭수']?.[month] || '',
        '주문수_전체': allData['주문수']?.[month]?.['전체'] || '',
        '주문수_오픈리스트': allData['주문수']?.[month]?.['오픈리스트'] || '',
        '주문수_배민1': allData['주문수']?.[month]?.['배민1'] || '',
        '주문금액_전체': allData['주문금액']?.[month]?.['전체'] || '',
        '주문금액_오픈리스트': allData['주문금액']?.[month]?.['오픈리스트'] || '',
        '주문금액_배민1': allData['주문금액']?.[month]?.['배민1'] || ''
      });
    }
    
    return rows;
  },

  async _collectChangeHistory(shopInfo) {
    Utils.showProgress('변경 이력 수집 중...', '준비 중...');
    
    const scrollContainer = document.querySelector('div[style*="overflow"]') || window;
    const allRows = [];
    const iso = new Date().toISOString();
    const processedItems = new Set();
    
    let totalProcessed = 0;
    let pauseCount = 0;
    let holidayCount = 0;
    let operationTimeCount = 0;
    let consecutiveNoNew = 0;
    const MAX_NO_NEW = 6;
    
    console.log('[변경 이력] 무한 스크롤 수집 시작 - 1개씩 처리');
    
    const areaName = this._extractAreaName(shopInfo.store_name);
    
    if (scrollContainer === window) {
      window.scrollTo(0, 0);
    } else {
      scrollContainer.scrollTop = 0;
    }
    await this._randomDelay(200, 400);
    
    let scrollIteration = 0;
    const MAX_ITERATIONS = 300;
    
    while (scrollIteration < MAX_ITERATIONS) {
      if (this._stopFlag) break;
      
      scrollIteration++;
      
      const currentItems = document.querySelectorAll('li.ListItem.self-ds');
      
      let foundUnprocessed = false;
      
      for (let i = 0; i < currentItems.length; i++) {
        if (this._stopFlag) break;
        
        const item = currentItems[i];
        
        const titleEl = item.querySelector('h5.flex-1');
        const dateEl = item.querySelector('time.ListItem-date');
        const title = titleEl?.textContent.trim() || '';
        const date = dateEl?.getAttribute('date') || '';
        const itemKey = `${title}|${date}`;
        
        if (processedItems.has(itemKey)) {
          continue;
        }
        
        const isPause = title.includes('영업임시중지');
        const isHoliday = title.includes('휴무일');
        const isOperationTime = title.includes('운영시간');
        
        if (!isPause && !isHoliday && !isOperationTime) {
          processedItems.add(itemKey);
          continue;
        }
        
        foundUnprocessed = true;
        totalProcessed++;
        
        Utils.showProgress(
          `변경 이력 수집 중... (처리: ${totalProcessed}, 임시중지: ${pauseCount}, 휴무일: ${holidayCount}, 운영시간: ${operationTimeCount})`,
          `"${title}" 처리 중...`
        );
        
        try {
          if (isHoliday || isOperationTime) {
            if (isHoliday) holidayCount++;
            if (isOperationTime) operationTimeCount++;
            
            const dateEl = item.querySelector('time.ListItem-date');
            const changeDatetime = dateEl?.getAttribute('date') || '';
            
            const row = {
              '수집일시': iso,
              '매장명': shopInfo.store_name || '',
              'store_id': shopInfo.store_id || '',
              '지역명': areaName,
              '대분류': title,
              '분류': '',
              '변경시간': changeDatetime,
              '작업자': '',
              '변경후_영업 임시중지 사유': '',
              '변경후_가게_영업_임시중지': '',
              '변경후_주문유형_가게배달': '',
              '변경후_주문유형_알뜰_한집배달': '',
              '변경전_영업 임시중지 사유': '',
              '변경전_가게_영업_임시중지': '',
              '변경전_주문유형_가게배달': '',
              '변경전_주문유형_알뜰_한집배달': ''
            };
            
            allRows.push(row);
            processedItems.add(itemKey);
            
            if (scrollContainer === window) {
              window.scrollBy(0, 200);
            } else {
              scrollContainer.scrollTop += 200;
            }
            await this._randomDelay(100, 200);
            
            break;
          }
          
          if (isPause) {
            pauseCount++;
            
            item.scrollIntoView({ behavior: 'smooth', block: 'center' });
            await this._randomDelay(200, 300);
            
            const contentDiv = item.querySelector('.ListItem-content');
            const isExpanded = contentDiv?.classList.contains('on');
            
            if (!isExpanded) {
              const expandButton = item.querySelector('button[data-atelier-component="IconButton"]');
              if (expandButton) {
                expandButton.click();
                
                let waitCount = 0;
                while (waitCount < 8) {
                  await this._randomDelay(200, 200);
                  const checkDiv = item.querySelector('.ListItem-content');
                  if (checkDiv?.classList.contains('on')) {
                    break;
                  }
                  waitCount++;
                }
                
                await this._randomDelay(200, 300);
              }
            } else {
              await this._randomDelay(100, 200);
            }
            
            const rowData = this._parseChangeHistoryItem(item, iso, shopInfo, areaName);
            if (rowData && rowData.length > 0) {
              allRows.push(...rowData);
            }
            
            processedItems.add(itemKey);
            
            if (scrollContainer === window) {
              window.scrollBy(0, 200);
            } else {
              scrollContainer.scrollTop += 200;
            }
            await this._randomDelay(200, 400);
            
            break;
          }
          
        } catch (err) {
          console.error(`[${totalProcessed}] 수집 오류:`, err);
          processedItems.add(itemKey);
        }
        
        break;
      }
      
      if (!foundUnprocessed) {
        consecutiveNoNew++;
        
        console.log(`[${scrollIteration}] 새 항목 없음 (${consecutiveNoNew}/${MAX_NO_NEW})`);
        
        if (consecutiveNoNew >= MAX_NO_NEW) {
          console.log('[변경 이력] 더 이상 새 항목 없음 - 종료');
          break;
        }
        
        if (scrollContainer === window) {
          window.scrollBy(0, 400);
        } else {
          scrollContainer.scrollTop += 400;
        }
        await this._randomDelay(300, 500);
      } else {
        consecutiveNoNew = 0;
      }
    }
    
    console.log(`[최종] 총 ${totalProcessed}개 항목 처리, 영업임시중지: ${pauseCount}, 휴무일: ${holidayCount}, 운영시간: ${operationTimeCount}`);
    
    if (!allRows.length) {
      const statusMsg = this._stopFlag ? '(ESC 중단됨)' : '';
      Utils.showError(`데이터 없음 ${statusMsg}`, 
        `총 ${totalProcessed}개 항목 처리했으나 데이터 수집 실패`
      );
      return { success: false, error: '변경 이력 데이터 없음' };
    }
    
    const headers = [
      '수집일시', '매장명', 'store_id', '지역명',
      '대분류', '분류', '변경시간', '작업자',
      '변경후_영업 임시중지 사유', '변경후_가게_영업_임시중지', '변경후_주문유형_가게배달', '변경후_주문유형_알뜰_한집배달',
      '변경전_영업 임시중지 사유', '변경전_가게_영업_임시중지', '변경전_주문유형_가게배달', '변경전_주문유형_알뜰_한집배달'
    ];
    
    const csv = Utils.toCSV(headers, allRows);
    const filename = await Utils.downloadCSV(csv, {
      channel: 'baemin',
      purpose: 'change_history',
      storeName: shopInfo.store_name,
      storeId: shopInfo.store_id,
      dateStr: Utils.getTodayStr()
    });
    
    const status = this._stopFlag ? '(ESC 중단됨)' : '';
    Utils.showSuccess(
      `변경 이력 수집 완료 ${status}`, 
      `총 ${allRows.length}행 수집 (영업임시중지: ${pauseCount}건, 휴무일: ${holidayCount}건, 운영시간: ${operationTimeCount}건)
→ ${filename}`
    );
    return { success: true, filename, rows: allRows.length };
  },

  _parseChangeHistoryItem(item, iso, shopInfo, areaName) {
    const rows = [];
    
    const titleEl = item.querySelector('h5.flex-1');
    const changeTitle = titleEl?.textContent.trim() || '';
    
    if (!changeTitle.includes('영업임시중지')) {
      return null;
    }
    
    const dateEl = item.querySelector('time.ListItem-date');
    const changeDatetime = dateEl?.getAttribute('date') || '';
    
    const content = item.querySelector('.ListItem-content.on');
    if (!content) return null;
    
    const detailsDiv = content.querySelector('.HistoryItemContents-module__rs7S');
    if (!detailsDiv) return null;
    
    const detailRows = detailsDiv.querySelectorAll('.HistoryItemContents-module__Zcx3');
    let category = '';
    let changeTime = '';
    let worker = '';
    
    detailRows.forEach(row => {
      const label = row.querySelector('.HistoryItemContents-module__sGh2')?.textContent.trim();
      const value = row.querySelector('.HistoryItemContents-module__FXZ7')?.textContent.trim();
      
      if (label === '분류') category = value;
      else if (label === '변경시간') changeTime = value;
      else if (label === '작업자') worker = value;
    });
    
    const changeContents = content.querySelector('.HistoryItemContents-module__ZwKd');
    if (!changeContents) return null;
    
    const sections = changeContents.querySelectorAll('div > div');
    let changeAfterText = '';
    let changeBeforeText = '';
    
    for (let i = 0; i < sections.length; i++) {
      const text = sections[i].textContent.trim();
      if (text === '[변경 후]' && sections[i + 1]) {
        changeAfterText = sections[i + 1].textContent.trim();
      } else if (text === '[변경 전]' && sections[i + 1]) {
        changeBeforeText = sections[i + 1].textContent.trim();
      }
    }
    
    const afterData = this._parseBusinessPauseDataNew(changeAfterText);
    const beforeData = this._parseBusinessPauseDataNew(changeBeforeText);
    
    const row = {
      '수집일시': iso,
      '매장명': shopInfo.store_name || '',
      'store_id': shopInfo.store_id || '',
      '지역명': areaName,
      '대분류': changeTitle,
      '분류': category,
      '변경시간': changeDatetime,
      '작업자': worker,
      '변경후_영업 임시중지 사유': afterData.reason,
      '변경후_가게_영업_임시중지': afterData.shop_pause,
      '변경후_주문유형_가게배달': afterData.delivery_shop,
      '변경후_주문유형_알뜰_한집배달': afterData.delivery_economy,
      '변경전_영업 임시중지 사유': beforeData.reason,
      '변경전_가게_영업_임시중지': beforeData.shop_pause,
      '변경전_주문유형_가게배달': beforeData.delivery_shop,
      '변경전_주문유형_알뜰_한집배달': beforeData.delivery_economy
    };
    
    rows.push(row);
    
    return rows;
  },

  _parseBusinessPauseDataNew(text) {
    const result = {
      reason: '없음',
      shop_pause: '없음',
      delivery_shop: '없음',
      delivery_economy: '없음'
    };
    
    if (!text) return result;
    
    const reasonMatch = text.match(/영업 임시중지 사유\s*[:：]\s*(.+)/);
    if (reasonMatch) {
      result.reason = reasonMatch[1].trim();
    }
    
    const timePattern = /시간\s*[:：]\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{1,2}):(\d{2})\s*~\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{1,2}):(\d{2})/;
    
    const sections = text.split(/(?=주문유형|가게 영업 임시중지)/);
    
    for (const section of sections) {
      if (section.includes('- 없음') || section.includes('없음')) {
        if (section.includes('가게 영업 임시중지')) {
          result.shop_pause = '없음';
        } else if (section.includes('주문유형 : 가게배달')) {
          result.delivery_shop = '없음';
        } else if (section.includes('주문유형 : 알뜰·한집배달')) {
          result.delivery_economy = '없음';
        }
        continue;
      }
      
      const match = section.match(timePattern);
      if (!match) continue;
      
      const [, y1, m1, d1, h1, min1, y2, m2, d2, h2, min2] = match;
      const pad = (n) => n.padStart(2, '0');
      
      const timeStr = `${pad(y1)}. ${pad(m1)}. ${pad(d1)}. ${pad(h1)}:${min1} ~ ${pad(y2)}. ${pad(m2)}. ${pad(d2)}. ${pad(h2)}:${min2}`;
      
      if (section.includes('가게 영업 임시중지')) {
        result.shop_pause = timeStr;
      } else if (section.includes('주문유형 : 가게배달')) {
        result.delivery_shop = timeStr;
      } else if (section.includes('주문유형 : 알뜰·한집배달')) {
        result.delivery_economy = timeStr;
      }
    }
    
    return result;
  },

  async _collectAdChangeHistory(shopInfo) {
    Utils.showProgress('광고 변경 이력 수집 중...', '준비 중...');
    
    const scrollContainer = document.querySelector('div[style*="overflow"]') || window;
    const allRows = [];
    const iso = new Date().toISOString();
    const processedItems = new Set();
    
    let totalProcessed = 0;
    let consecutiveNoNew = 0;
    const MAX_NO_NEW = 6;
    
    console.log('[광고 변경 이력] 무한 스크롤 수집 시작');
    
    const areaName = this._extractAreaName(shopInfo.store_name);
    
    if (scrollContainer === window) {
      window.scrollTo(0, 0);
    } else {
      scrollContainer.scrollTop = 0;
    }
    await this._randomDelay(200, 400);
    
    let scrollIteration = 0;
    const MAX_ITERATIONS = 300;
    
    while (scrollIteration < MAX_ITERATIONS) {
      if (this._stopFlag) break;
      
      scrollIteration++;
      
      const currentItems = document.querySelectorAll('li.ListItem.self-ds');
      
      let foundUnprocessed = false;
      
      for (let i = 0; i < currentItems.length; i++) {
        if (this._stopFlag) break;
        
        const item = currentItems[i];
        
        const titleEl = item.querySelector('h5.flex-1');
        const dateEl = item.querySelector('time.ListItem-date');
        const title = titleEl?.textContent.trim() || '';
        const date = dateEl?.getAttribute('date') || '';
        const itemKey = `${title}|${date}`;
        
        if (processedItems.has(itemKey)) {
          continue;
        }
        
        const isAdChange = title.includes('광고');
        
        if (!isAdChange) {
          processedItems.add(itemKey);
          continue;
        }
        
        foundUnprocessed = true;
        totalProcessed++;
        
        Utils.showProgress(
          `광고 변경 이력 수집 중... (처리: ${totalProcessed})`,
          `"${title}" 처리 중...`
        );
        
        try {
          item.scrollIntoView({ behavior: 'smooth', block: 'center' });
          await this._randomDelay(200, 300);
          
          const contentDiv = item.querySelector('.ListItem-content');
          const isExpanded = contentDiv?.classList.contains('on');
          
          if (!isExpanded) {
            const expandButton = item.querySelector('button[data-atelier-component="IconButton"]');
            if (expandButton) {
              expandButton.click();
              
              let waitCount = 0;
              while (waitCount < 8) {
                await this._randomDelay(200, 200);
                const checkDiv = item.querySelector('.ListItem-content');
                if (checkDiv?.classList.contains('on')) {
                  break;
                }
                waitCount++;
              }
              
              await this._randomDelay(200, 300);
            }
          } else {
            await this._randomDelay(100, 200);
          }
          
          const rowData = this._parseAdChangeItem(item, iso, shopInfo, areaName);
          if (rowData) {
            allRows.push(rowData);
          }
          
          processedItems.add(itemKey);
          
          if (scrollContainer === window) {
            window.scrollBy(0, 200);
          } else {
            scrollContainer.scrollTop += 200;
          }
          await this._randomDelay(200, 400);
          
          break;
          
        } catch (err) {
          console.error(`[${totalProcessed}] 수집 오류:`, err);
          processedItems.add(itemKey);
        }
        
        break;
      }
      
      if (!foundUnprocessed) {
        consecutiveNoNew++;
        
        console.log(`[${scrollIteration}] 새 항목 없음 (${consecutiveNoNew}/${MAX_NO_NEW})`);
        
        if (consecutiveNoNew >= MAX_NO_NEW) {
          console.log('[광고 변경 이력] 더 이상 새 항목 없음 - 종료');
          break;
        }
        
        if (scrollContainer === window) {
          window.scrollBy(0, 400);
        } else {
          scrollContainer.scrollTop += 400;
        }
        await this._randomDelay(300, 500);
      } else {
        consecutiveNoNew = 0;
      }
    }
    
    console.log(`[최종] 총 ${totalProcessed}개 항목 처리`);
    
    if (!allRows.length) {
      const statusMsg = this._stopFlag ? '(ESC 중단됨)' : '';
      Utils.showError(`데이터 없음 ${statusMsg}`, 
        `총 ${totalProcessed}개 항목 처리했으나 데이터 수집 실패`
      );
      return;
    }
    
    const headers = [
      '수집일시', '매장명', 'store_id', '지역명',
      '대분류', '캠페인정보', '변경시간', '작업자',
      '변경후_광고방식', '변경후_예산', '변경후_희망가',
      '변경전_광고방식', '변경전_예산', '변경전_희망가'
    ];
    
    const csv = Utils.toCSV(headers, allRows);
    const filename = Utils.downloadCSV(csv, {
      channel: 'baemin',
      purpose: 'ad_change_history',
      storeName: shopInfo.store_name,
      storeId: shopInfo.store_id,
      dateStr: Utils.getTodayStr()
    });
    
    const status = this._stopFlag ? '(ESC 중단됨)' : '';
    Utils.showSuccess(
      `광고 변경 이력 수집 완료 ${status}`, 
      `총 ${allRows.length}행 수집\n→ ${filename}`
    );
  },

  _parseAdChangeItem(item, iso, shopInfo, areaName) {
    const titleEl = item.querySelector('h5.flex-1');
    const changeTitle = titleEl?.textContent.trim() || '';
    
    const dateEl = item.querySelector('time.ListItem-date');
    const changeDatetime = dateEl?.getAttribute('date') || '';
    
    const content = item.querySelector('.ListItem-content.on');
    if (!content) return null;
    
    const detailsDiv = content.querySelector('.HistoryItemContents-module__rs7S');
    if (!detailsDiv) return null;
    
    const detailRows = detailsDiv.querySelectorAll('.HistoryItemContents-module__Zcx3');
    let campaignInfo = '';
    let changeTime = '';
    let worker = '';
    
    detailRows.forEach(row => {
      const label = row.querySelector('.HistoryItemContents-module__sGh2')?.textContent.trim();
      const value = row.querySelector('.HistoryItemContents-module__FXZ7')?.textContent.trim();
      
      if (label === '캠페인 정보') campaignInfo = value;
      else if (label === '변경시간') changeTime = value;
      else if (label === '작업자') worker = value;
    });
    
    const changeContents = content.querySelector('.HistoryItemContents-module__ZwKd');
    if (!changeContents) return null;
    
    const sections = changeContents.querySelectorAll('div > div');
    let changeAfterText = '';
    let changeBeforeText = '';
    
    for (let i = 0; i < sections.length; i++) {
      const text = sections[i].textContent.trim();
      if (text === '[변경 후]' && sections[i + 1]) {
        changeAfterText = sections[i + 1].textContent.trim();
      } else if (text === '[변경 전]' && sections[i + 1]) {
        changeBeforeText = sections[i + 1].textContent.trim();
      }
    }
    
    const afterData = this._parseAdData(changeAfterText);
    const beforeData = this._parseAdData(changeBeforeText);
    
    const row = {
      '수집일시': iso,
      '매장명': shopInfo.store_name || '',
      'store_id': shopInfo.store_id || '',
      '지역명': areaName,
      '대분류': changeTitle,
      '캠페인정보': campaignInfo,
      '변경시간': changeDatetime,
      '작업자': worker,
      '변경후_광고방식': afterData.method,
      '변경후_예산': afterData.budget,
      '변경후_희망가': afterData.bid,
      '변경전_광고방식': beforeData.method,
      '변경전_예산': beforeData.budget,
      '변경전_희망가': beforeData.bid
    };
    
    return row;
  },

  _parseAdData(text) {
    const result = {
      method: '',
      budget: '',
      bid: ''
    };
    
    if (!text) return result;
    
    if (text.includes('수동')) {
      result.method = '수동';
    } else if (text.includes('자동')) {
      result.method = '자동';
    }
    
    const budgetMatch = text.match(/예산\s*[:：]\s*([\d,]+)원/);
    if (budgetMatch) {
      result.budget = budgetMatch[1].replace(/,/g, '');
    }
    
    const bidMatch = text.match(/희망가\s*[:：]\s*([\d,]+)원/);
    if (bidMatch) {
      result.bid = bidMatch[1].replace(/,/g, '');
    }
    
    return result;
  },

  async _collectMetrics(shopInfo) {
    const LABELS = ['조리소요시간', '주문접수시간', '최근재주문율', '조리시간준수율', '주문접수율', '최근별점'];
    const RATIO = new Set(['조리시간준수율', '주문접수율', '최근재주문율']);
    const iso = new Date().toISOString();
    const data = {
      collected_at: iso,
      store_id: shopInfo.store_id,
      store_name: shopInfo.store_name,
      cardIndex: 0,
      url: location.href,
      조리소요시간: '', 조리소요시간_순위구분: '', 조리소요시간_순위비율: '',
      주문접수시간: '', 주문접수시간_순위구분: '', 주문접수시간_순위비율: '',
      최근재주문율: '', 
      조리시간준수율: '', 조리시간준수율_순위구분: '', 조리시간준수율_순위비율: '',
      주문접수율: '', 주문접수율_순위구분: '', 주문접수율_순위비율: '',
      최근별점: ''
    };
    let filledCount = 0;

    const items = document.querySelectorAll('.WooriShopNowItem-module__TKcC');
    for (const item of items) {
      const spans = item.querySelectorAll('span');
      if (spans.length < 2) continue;

      const label = spans[0].textContent.trim();
      if (!LABELS.includes(label)) continue;

      const rawVal = spans[1].textContent.trim();
      const rawRank = spans.length > 2 ? spans[2].textContent.trim() : '';
      const numMatch = rawVal.match(/[\d.]+/);
      let numStr = numMatch ? numMatch[0] : '';
      if (numStr && RATIO.has(label)) numStr = String(parseFloat(numStr) / 100);

      const rankMatch = rawRank.match(/^(상위|하위)\s*([\d.]+)%$/);
      data[label] = numStr;
      if (rankMatch) {
        data[`${label}_순위구분`] = rankMatch[1];
        data[`${label}_순위비율`] = String(parseFloat(rankMatch[2]) / 100);
      }
      if (numStr) filledCount++;
    }

    if (!filledCount) {
      Utils.showError('데이터 없음', '수집할 메트릭이 없습니다.');
      return { success: false, error: '메트릭 데이터 없음' };
    }

    const rows = [data];
    const headers = [
      'collected_at', 'store_id', 'store_name', 'cardIndex', 'url',
      '조리소요시간', '조리소요시간_순위구분', '조리소요시간_순위비율',
      '주문접수시간', '주문접수시간_순위구분', '주문접수시간_순위비율',
      '최근재주문율', 
      '조리시간준수율', '조리시간준수율_순위구분', '조리시간준수율_순위비율',
      '주문접수율', '주문접수율_순위구분', '주문접수율_순위비율',
      '최근별점'
    ];
    const csv = Utils.toCSV(headers, rows);
    console.log(`[배민 now] 다운로드 시작: ${shopInfo.store_name} (${shopInfo.store_id}) rows=${rows.length}`);
    const filename = await Utils.downloadCSV(csv, {
      channel: 'baemin',
      purpose: 'metrics',
      storeName: shopInfo.store_name,
      storeId: shopInfo.store_id,
      dateStr: Utils.getTodayStr()
    });
    console.log(`[배민 now] 다운로드 요청 완료: ${filename}`);
    
    // 매장 정보 저장 (마케팅 페이지에서 사용)
    if (shopInfo.store_id && shopInfo.store_name) {
      await Utils.StoreRegistry.save('baemin', shopInfo.store_id, shopInfo.store_name);
    }
    
    // 다음 페이지 URL
    const nextUrl = `https://self.baemin.com/shops/${shopInfo.store_id}/stat/marketing/woori-shop-click?initialDateOption=MONTHLY&initialMonth=${new Date().toISOString().slice(0, 7)}`;
    
    // 자동 이동 확인
    const confirmMsg = `✅ 메트릭 수집 완료!\n\n📁 파일: ${filename}\n📊 ${rows.length}건 수집\n\n⏭️ 마케팅 페이지로 이동하시겠습니까?`;
    
    if (!this._batchMode) {
      await Utils.navigateWithConfirm(nextUrl, confirmMsg);
    } else {
      console.log('[배민 now] 배치 모드: 마케팅 자동 이동 생략');
    }
    return {
      success: true,
      filename,
      rows: rows.length,
      store_id: shopInfo.store_id,
      store_name: shopInfo.store_name
    };
  },

  _findValueAndRank(labelSpan, LABELS) {
    let value = null, rank = null;
    const parentBlock = labelSpan.parentElement;
    if (!parentBlock) return { value, rank };

    const valueFlex = parentBlock.nextElementSibling;
    if (valueFlex?.getAttribute?.('data-atelier-component') === 'Flex') {
      const spans = valueFlex.querySelectorAll('span[data-atelier-component="Typography"]');
      for (const s of spans) {
        const t = (s.textContent || '').trim();
        if (t && value === null) value = t;
      }
    }

    let after = valueFlex ? valueFlex.nextElementSibling : parentBlock.nextElementSibling;
    while (after) {
      if (after.tagName === 'SPAN' && after.getAttribute?.('data-atelier-component') === 'Typography') {
        const t = (after.textContent || '').trim();
        if (/^(상위|하위)\s*\d+(\.\d+)?%$/.test(t)) { rank = t; break; }
      }
      if (after.getAttribute?.('data-atelier-component') === 'Flex') {
        const maybeLabel = after.querySelector('span[data-atelier-component="Typography"]');
        if (maybeLabel && LABELS.includes((maybeLabel.textContent || '').trim())) break;
      }
      after = after.nextElementSibling;
    }

    if (value === null) {
      const siblings = parentBlock.querySelectorAll('span[data-atelier-component="Typography"]');
      let seenLabel = false;
      for (const s of siblings) {
        const t = (s.textContent || '').trim();
        if (!t) continue;
        if (!seenLabel && t === (labelSpan.textContent || '').trim()) { seenLabel = true; continue; }
        if (seenLabel) { value = t; break; }
      }
    }

    if (rank === null) {
      const siblings = parentBlock.querySelectorAll('span[data-atelier-component="Typography"]');
      for (const s of siblings) {
        const t = (s.textContent || '').trim();
        if (/^(상위|하위)\s*\d+(\.\d+)?%$/.test(t)) { rank = t; break; }
      }
    }

    return { value, rank };
  }
};

// ════════════════════════════════════════════════════════════════════════════
// BaeminPipelineSave — 파이프라인 규격 수동수집 전용 (기존 코드 영향 없음)
// 사용: baemin_manual.js에서 COLLECT_PIPELINE_* 메시지를 받아 호출
// ════════════════════════════════════════════════════════════════════════════
window.BaeminPipelineSave = {

  ORDERS_HEADERS: [
    'collected_at', 'store_name', '주문상태', '주문번호', '주문시각',
    '광고상품', '캠페인ID', '주문내역', '주문수량', '결제타입', '수령방법',
    '결제금액', '상품금액', '즉시할인', '즉시할인_파트너부담', '즉시할인_배민지원',
    '배민부담_쿠폰할인', '총결제금액', '주문옵션상세', '주문옵션금액',
    '주문중개', '고객할인비용', '배달', '그외', '부가세', '만나서결제금액', '입금예정금액'
  ],

  ADFUNNEL_HEADERS: [
    'collected_at', 'target_date', 'store_name', 'collection_status',
    '노출수', '클릭수', '주문수', '주문금액'
  ],

  MENU_HEADERS: [
    'collected_at', 'collected_date', 'brand', 'store', 'store_id', '메뉴명', '배달판매가', '상태'
  ],

  _getBrand(storeName) {
    const known = ['도리당', '나홀로'];
    return known.find(b => storeName.includes(b)) || storeName.split(' ')[0];
  },

  async _download(csvContent, filename) {
    return new Promise((resolve) => {
      let done = false;
      const fallback = setTimeout(() => {
        if (!done) { done = true; resolve({ success: false, error: 'timeout' }); }
      }, 10000);
      chrome.runtime.sendMessage({ type: 'DOWNLOAD_CSV', content: csvContent, filename }, (res) => {
        clearTimeout(fallback);
        if (!done) { done = true; resolve(res); }
      });
    });
  },

  // 주문내역: 현재 배민 orders 페이지에서 수집 후 파이프라인 파일명으로 저장
  // 기존 _collectOrdersAllPages 실행, Utils.downloadCSV를 임시 override해 CSV 캡처
  async collectAndSaveOrders(expectedStore = '', expectedStoreId = '', optionValue = '', optionText = '') {
    const site = Sites['baemin'];
    if (!site) return { success: false, error: 'Sites.baemin 없음' };

    let shopInfo = site._getShopInfo();
    if ((shopInfo.needFilter || expectedStore || expectedStoreId || optionValue || optionText) && location.href.includes('/orders/history')) {
      const filterResult = await site.applyOrdersStoreFilter(expectedStore, expectedStoreId, optionValue, optionText);
      if (!filterResult.success) {
        return {
          success: false,
          error: filterResult.error || 'orders 가게 필터 적용 실패',
          debug: filterResult.debug
        };
      }
      shopInfo = filterResult.shopInfo || site._getShopInfo();
    }

    if (shopInfo.needFilter) {
      return { success: false, error: 'orders 가게 필터가 전체 상태입니다. 목표 매장 선택 후 다시 수집하세요.' };
    }

    const storeName = optionText
      ? site._cleanOrdersStoreLabel(optionText)
      : (shopInfo.store_name || '알수없는매장');

    const dateMatch = location.href.match(/startDate=(\d{4}-\d{2}-\d{2})/);
    const targetDate = dateMatch?.[1] || new Date().toISOString().slice(0, 10);

    // Utils.downloadCSV를 임시 override → 원본 CSV 캡처 (저장은 우리가 직접 처리)
    const origDownload = Utils.downloadCSV.bind(Utils);
    let capturedCsv = null;
    Utils.downloadCSV = async (csv) => { capturedCsv = csv; return 'pipeline_captured'; };

    let collectResult = null;
    try {
      collectResult = await site._collectOrdersAllPages(shopInfo);
    } finally {
      Utils.downloadCSV = origDownload;
    }

    if (collectResult?.success === false) {
      return collectResult;
    }

    if (!capturedCsv) return { success: false, error: '수집 결과 없음 (0건 또는 오류)' };

    const filename = await Utils.downloadCSV(capturedCsv, {
      channel: 'baemin',
      purpose: 'orders',
      storeName: storeName,
      storeId: shopInfo.store_id || '',
      dateStr: targetDate.replace(/-/g, '')
    });
    return { success: true, filename, storeName, partial: !!collectResult?.partial };
  },

  // 광고퍼널: 특정 날짜의 배민 stat/advertisement 페이지에서 숫자 읽기
  // baemin_manual.js가 ?startDate={date}&endDate={date} 파라미터로 navigate한 후 호출
  async collectAndSaveAdFunnel(targetDate) {
    const site = Sites['baemin'];
    if (!site) return { success: false, error: 'Sites.baemin 없음' };

    const shopInfo = site._getShopInfo();
    const storeName = shopInfo.store_name || '알수없는매장';
    const brand = this._getBrand(storeName);
    const iso = new Date().toISOString();

    let 노출수 = '', 클릭수 = '', 주문수 = '', 주문금액 = '';
    let collection_status = 'parse_error';

    try {
      // 배민 셀프서비스 Typography 숫자 읽기 헬퍼
      const readNumbers = (count) => {
        return [...document.querySelectorAll('[data-atelier-component="Typography"]')]
          .map(el => el.textContent.trim())
          .filter(t => /^[\d,]+$/.test(t) && parseInt(t.replace(/,/g, ''), 10) >= 0)
          .slice(0, count)
          .map(t => t.replace(/,/g, ''));
      };

      // Filter 버튼 [0] 클릭 → 노출수·클릭수 표시
      const filterBtns = [...document.querySelectorAll('button')].filter(
        b => b.className && b.className.includes('Filter-module')
      );

      if (filterBtns[0]) {
        filterBtns[0].click();
        await new Promise(r => setTimeout(r, 700));
      }
      const adNums = readNumbers(2);
      노출수 = adNums[0] || '0';
      클릭수 = adNums[1] || '0';

      // Filter 버튼 [1] 클릭 → 주문수·주문금액 표시
      if (filterBtns[1]) {
        filterBtns[1].click();
        await new Promise(r => setTimeout(r, 700));
      }
      const orderNums = readNumbers(2);
      주문수 = orderNums[0] || '0';
      주문금액 = orderNums[1] || '0';

      // 최소한 하나라도 0 이상이면 ok
      if ([노출수, 클릭수, 주문수, 주문금액].some(v => parseInt(v, 10) > 0)) {
        collection_status = 'ok';
      } else if ([노출수, 클릭수, 주문수, 주문금액].every(v => v === '0')) {
        // 광고 미집행 가능성
        collection_status = 'no_ads';
      }
    } catch (e) {
      collection_status = 'parse_error';
      console.warn('[BaeminPipelineSave] ad_funnel 읽기 오류:', e);
    }

    const row = {
      collected_at: iso, target_date: targetDate, store_name: storeName,
      collection_status, 노출수, 클릭수, 주문수, 주문금액
    };
    const csv = Utils.toCSV(this.ADFUNNEL_HEADERS, [row]);
    const filename = `[${brand}][${storeName}]baemin_ad_funnel_${targetDate}.csv`;
    await this._download(csv, filename);
    return { success: collection_status !== 'parse_error', filename, storeName, collection_status };
  }
};
