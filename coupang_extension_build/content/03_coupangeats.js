// ===================================================================================
// ================= 03_coupangeats.js : 쿠팡이츠 수집 로직 =================
// ===================================================================================

Sites['coupangeats'] = {
  name: '쿠팡이츠',
  match: (url) => url.includes('store.coupangeats.com') || url.includes('advertising.coupangeats.com'),
  ORDERS_STORE_BUDGET_MS: 480000,
  ORDERS_STORE_BUDGET_EXTENSION_MS: 480000,
  ORDERS_STORE_BUDGET_MAX_EXTENSIONS: 2,
  _stopFlag: false,
  
  async collect(opts = {}) {
    this._stopFlag = false;
    this._collectSource = opts.source || 'manual';
    this._hardThrottled = false;
    this._resumeReloadCount = 0; // 새 수집 시작 → F5 재개 누적 초기화
    this._ordersThrottleCount = 0;
    this._lastOrdersThrottleAt = 0;
    this._lastOrdersTransientErrorAt = 0;
    this._lastOrdersCollectResult = null;
    this._nextManualOrdersBreakAt = 8 + Math.floor(Math.random() * 8);
    this._setupEscListener();

    const shopInfo = this._getShopInfo();

    const isCMGPage = location.hostname.includes('advertising.coupangeats.com')
                   || document.querySelector('WUJIE-APP')
                   || location.href.includes('/merchant/management/cmg');

    if (location.pathname.includes('/coupons/detail/')) {
      this._collectCouponStats(shopInfo);
    } else if (isCMGPage) {
      await this._collectCMGMultiStore(opts);
    } else if (location.pathname.includes('/orders/')) {
      await this._collectOrdersMultiStore(opts);
    } else if (location.pathname.includes('/menu')) {
      await this._collectMenuMultiStore(opts);
    } else {
      Utils.showError('미지원 페이지', '주문관리, 쿠폰상세, CMG 또는 메뉴 페이지에서 실행해주세요.');
    }

    this._removeEscListener();
  },

  _escHandler: null,
  _setupEscListener() {
    this._escHandler = (e) => {
      if (e.key === 'Escape') {
        this._stopFlag = true;
        Utils.showError('수집 중단', 'ESC 눌림 - 현재까지 수집된 데이터를 저장합니다.');
      }
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

  _manualOrdersPace(min, max) {
    if (this._collectSource === 'manual') return this._randomDelay(min, max);
    if (this._collectSource === 'batch') {
      const batchMin = Math.max(Math.floor(min * 1.15), min + 250);
      const batchMax = Math.max(Math.floor(max * 1.35), max + 600);
      return this._randomDelay(batchMin, batchMax);
    }
    return Promise.resolve();
  },

  _isSafeOrdersPacingEnabled() {
    return this._collectSource === 'manual' || this._collectSource === 'batch';
  },

  _lastOrdersSearchAt: 0,
  _ordersSearchCooldownUntil: 0,
  _ordersThrottleCount: 0,
  _lastOrdersThrottleAt: 0,
  _lastOrdersTransientErrorAt: 0,
  _nextManualOrdersBreakAt: 12,
  _hardThrottled: false,
  ERROR_CONTAINER_SELECTORS: ['.modal.show', '.ant-modal', '[role="dialog"]', '.popup-wrapper', '.toast', '[role="alert"]'],

  _findOrdersThrottleSignal() {
    const emptySignal = { matched: false, keyword: '', snippet: '' };
    const keywords = [
      '10056',
      '10057',
      'ACCESS_DENIDE',
      'ACCESS_DENIED',
      'ACCESS_DENY',
      '\uACFC\uB3C4\uD55C \uC694\uCCAD',
      '일시적으로 제한',
      '\uB2E8\uC2DC\uAC04 \uB0B4 \uBC18\uBCF5 \uC694\uCCAD'
    ];
    const makeSignal = (text, keyword, index) => ({
      matched: true,
      keyword,
      snippet: text.slice(Math.max(0, index - 30), index + keyword.length + 30).replace(/\s+/g, ' ').trim()
    });

    for (const selector of this.ERROR_CONTAINER_SELECTORS) {
      for (const el of document.querySelectorAll(selector)) {
        const text = el.textContent || '';
        for (const keyword of keywords) {
          const index = text.indexOf(keyword);
          if (index >= 0) return makeSignal(text, keyword, index);
        }
      }
    }

    if (this._hasNoOrdersNotice()) return emptySignal;

    const bodyText = document.body?.innerText || '';
    const contextualCode = /(?:^|\D)1005[67](?:\D|$)/g;
    let match;
    while ((match = contextualCode.exec(bodyText)) !== null) {
      const context = bodyText.slice(Math.max(0, match.index - 80), match.index + match[0].length + 80);
      if (context.includes('제한') || context.includes('\uACFC\uB3C4\uD55C \uC694\uCCAD')) {
        const keyword = match[0].trim();
        return makeSignal(bodyText, keyword, match.index);
      }
    }

    const bodyKeywords = [
      '일시적으로 제한',
      '서비스 이용이 일시적',
      '\uB2E8\uC2DC\uAC04 \uB0B4 \uBC18\uBCF5 \uC694\uCCAD',
      '\uACFC\uB3C4\uD55C \uC694\uCCAD',
      'ACCESS_DENIDE',
      'ACCESS_DENIED',
      'ACCESS_DENY'
    ];
    for (const keyword of bodyKeywords) {
      const index = bodyText.indexOf(keyword);
      if (index >= 0) return makeSignal(bodyText, keyword, index);
    }
    return emptySignal;
  },

  _isOrdersThrottleVisible() {
    return this._findOrdersThrottleSignal().matched;
  },

  _markOrdersTransientError() {
    this._lastOrdersTransientErrorAt = Date.now();
  },

  _hadRecentOrdersTransientError(windowMs = 120000) {
    return Date.now() - this._lastOrdersTransientErrorAt < windowMs;
  },

  _tryExtendStoreBudget({ extensions, idCount, lastExtensionIdCount }) {
    if (this._stopFlag) return false;
    if (extensions >= this.ORDERS_STORE_BUDGET_MAX_EXTENSIONS) return false;
    if (idCount <= lastExtensionIdCount) return false;
    if (this._isOrdersThrottleVisible()) return false;
    if (this._hadRecentOrdersTransientError()) return false;
    return true;
  },

  _ordersThrottleDelayMs() {
    const ranges = [
      [55000, 75000],
      [90000, 130000],
      [150000, 210000]
    ];
    const idx = Math.min(Math.max(this._ordersThrottleCount - 1, 0), ranges.length - 1);
    const [min, max] = ranges[idx];
    return Math.floor(Math.random() * (max - min + 1)) + min;
  },

  async _waitAfterOrdersBlockedStore() {
    if (!this._isSafeOrdersPacingEnabled()) return;
    const waitMs = 60000 + Math.floor(Math.random() * 30001);
    Utils.updateProgressModal({ debug: `⏳ 10057 예방 대기 ${Math.ceil(waitMs / 1000)}초 후 다음 매장 진행` });
    await new Promise(r => setTimeout(r, waitMs));
  },

  _markOrdersHardThrottle(signal = null) {
    if (this._hardThrottled === true) return;
    const throttleSignal = signal || this._findOrdersThrottleSignal();
    this._ordersThrottleCount++;
    this._lastOrdersThrottleAt = Date.now();
    this._markOrdersTransientError();
    this._hardThrottled = true;
    Utils.updateProgressModal({ debug: `⛔ 쿠팡 제한(10057) 감지 — 현재 매장 패스 | keyword=${throttleSignal.keyword} | ...${throttleSignal.snippet}...` });
  },

  _checkOrdersHardThrottle() {
    const signal = this._findOrdersThrottleSignal();
    if (!signal.matched) return false;
    this._markOrdersHardThrottle(signal);
    return true;
  },

  async _waitBeforeOrdersSearch() {
    const MIN_SEARCH_INTERVAL_MS = this._isSafeOrdersPacingEnabled()
      ? 25000 + Math.floor(Math.random() * 15001)
      : 10000;

    if (this._checkOrdersHardThrottle()) return false;

    const waitUntil = Math.max(
      this._ordersSearchCooldownUntil,
      this._lastOrdersSearchAt + MIN_SEARCH_INTERVAL_MS
    );
    const waitMs = waitUntil - Date.now();
    if (waitMs > 0) {
      Utils.updateProgressModal({ debug: `⏳ 조회 요청 간격 보호 대기 ${Math.ceil(waitMs / 1000)}초` });
      await new Promise(r => setTimeout(r, waitMs));
    }
    return true;
  },

  async _humanizeManualOrdersBeforeClick(el) {
    if (!this._isSafeOrdersPacingEnabled() || !el) return;
    try {
      const isBatch = this._collectSource === 'batch';
      el.scrollIntoView({ block: 'center', behavior: 'smooth' });
      await this._randomDelay(isBatch ? 385 : 150, isBatch ? 1060 : 530);
      el.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
      await this._randomDelay(isBatch ? 130 : 50, isBatch ? 415 : 155);
      el.dispatchEvent(new MouseEvent('mousemove', { bubbles: true }));
      if (typeof el.focus === 'function') el.focus();
      await this._randomDelay(isBatch ? 265 : 105, isBatch ? 825 : 385);
    } catch (_) {}
  },

  async _humanizeManualOrdersAfterItem(index) {
    if (!this._isSafeOrdersPacingEnabled()) return;
    const isBatch = this._collectSource === 'batch';
    const breakAt = this._nextManualOrdersBreakAt;
    if (index + 1 >= breakAt) {
      Utils.updateProgressModal({ debug: 'batch short pacing break' });
      await this._randomDelay(isBatch ? 1475 : 2065, isBatch ? 3835 : 5310);
      this._nextManualOrdersBreakAt += (isBatch ? 9 : 8) + Math.floor(Math.random() * (isBatch ? 7 : 8));
    }
  },

  async _humanizeManualOrdersAfterPage() {
    if (!this._isSafeOrdersPacingEnabled()) return;
    const isBatch = this._collectSource === 'batch';
    await this._randomDelay(isBatch ? 1060 : 885, isBatch ? 2655 : 2655);
    if (Math.random() < (isBatch ? 0.2 : 0.22)) {
      Utils.updateProgressModal({ debug: 'batch page transition stabilization wait' });
      await this._randomDelay(isBatch ? 2655 : 3540, isBatch ? 5900 : 7080);
    }
  },

  _isDashboardBatchOrders(opts = {}) {
    return opts.source === 'batch' && Array.isArray(opts.targetStores) && opts.targetStores.length > 0;
  },

  _parseDateRange(input) {
    const trim = input.trim();
    
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    yesterday.setHours(0, 0, 0, 0);

    const normalizeDay = (date) => {
      date.setHours(0, 0, 0, 0);
      return date;
    };

    const clampToYesterday = (date) => {
      return date > yesterday ? new Date(yesterday) : date;
    };

    const buildRollingWeekRange = (baseDate) => {
      const endDate = clampToYesterday(normalizeDay(new Date(baseDate)));
      const startDate = new Date(endDate);
      startDate.setDate(startDate.getDate() - 6);
      normalizeDay(startDate);

      return {
        startDate,
        endDate,
        mode: 'rollingWeek'
      };
    };

    const bracketDotRollingWeekMatch = trim.match(/^-\{(\d{2})\.(\d{2})\.(\d{2})\}$/);
    if (bracketDotRollingWeekMatch) {
      const [, y, m, d] = bracketDotRollingWeekMatch;
      const baseDate = normalizeDay(new Date(2000 + parseInt(y, 10), parseInt(m, 10) - 1, parseInt(d, 10)));

      if (isNaN(baseDate.getTime())) {
        return { error: '유효하지 않은 날짜입니다.' };
      }

      return buildRollingWeekRange(baseDate);
    }

    const dotRollingWeekMatch = trim.match(/^-(\d{2})\.(\d{2})\.(\d{2})$/);
    if (dotRollingWeekMatch) {
      const [, y, m, d] = dotRollingWeekMatch;
      const baseDate = normalizeDay(new Date(2000 + parseInt(y, 10), parseInt(m, 10) - 1, parseInt(d, 10)));

      if (isNaN(baseDate.getTime())) {
        return { error: '유효하지 않은 날짜입니다.' };
      }

      return buildRollingWeekRange(baseDate);
    }

    const bracketNumRollingWeekMatch = trim.match(/^-\{(\d{8})\}$/);
    if (bracketNumRollingWeekMatch) {
      const baseDate = this._parseDate(bracketNumRollingWeekMatch[1]);

      if (!baseDate) {
        return { error: '유효하지 않은 날짜입니다.' };
      }

      return buildRollingWeekRange(baseDate);
    }

    const numRollingWeekMatch = trim.match(/^-(\d{8})$/);
    if (numRollingWeekMatch) {
      const baseDate = this._parseDate(numRollingWeekMatch[1]);

      if (!baseDate) {
        return { error: '유효하지 않은 날짜입니다.' };
      }

      return buildRollingWeekRange(baseDate);
    }
    
    const dotRangeMatch = trim.match(/^(\d{2})\.(\d{2})\.(\d{2})-(\d{2})\.(\d{2})\.(\d{2})$/);
    if (dotRangeMatch) {
      const [, y1, m1, d1, y2, m2, d2] = dotRangeMatch;
      const startDate = normalizeDay(new Date(2000 + parseInt(y1), parseInt(m1) - 1, parseInt(d1)));
      const endDate = normalizeDay(new Date(2000 + parseInt(y2), parseInt(m2) - 1, parseInt(d2)));
      
      if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
        return { error: '유효하지 않은 날짜입니다.' };
      }

      return {
        startDate: clampToYesterday(startDate),
        endDate: clampToYesterday(endDate)
      };
    }
    
    const dotSingleMatch = trim.match(/^(\d{2})\.(\d{2})\.(\d{2})$/);
    if (dotSingleMatch) {
      const [, y, m, d] = dotSingleMatch;
      const startDate = normalizeDay(new Date(2000 + parseInt(y), parseInt(m) - 1, parseInt(d)));
      
      if (isNaN(startDate.getTime())) {
        return { error: '유효하지 않은 날짜입니다.' };
      }

      return {
        startDate: clampToYesterday(startDate),
        endDate: new Date(yesterday)
      };
    }
    
    const numRangeMatch = trim.match(/^(\d{8})-(\d{8})$/);
    if (numRangeMatch) {
      const startDate = this._parseDate(numRangeMatch[1]);
      const endDate = this._parseDate(numRangeMatch[2]);
      
      if (!startDate || !endDate) {
        return { error: '유효하지 않은 날짜입니다.' };
      }

      return {
        startDate: clampToYesterday(startDate),
        endDate: clampToYesterday(endDate)
      };
    }
    
    const numSingleMatch = trim.match(/^(\d{8})$/);
    if (numSingleMatch) {
      const startDate = this._parseDate(numSingleMatch[1]);
      
      if (!startDate) {
        return { error: '유효하지 않은 날짜입니다.' };
      }

      return {
        startDate: clampToYesterday(startDate),
        endDate: new Date(yesterday)
      };
    }
    
    return { 
      error: '날짜 형식이 올바르지 않습니다.\n' +
             '예시:\n' +
             '  -26.04.06 (기준일 포함 최근 7일)\n' +
             '  -20260406 (기준일 포함 최근 7일)\n' +
             '  26.01.01-26.01.15 (범위)\n' +
             '  26.01.01 (해당일~어제)\n' +
             '  20260101-20260115 (범위)\n' +
             '  20260101 (해당일~어제)'
    };
  },

  // ✅ 개선된 조회 버튼 클릭 함수 - calendar-dropdown 영역 내에서만 찾기
  async _clickSearchButton(doc) {
    for (let attempt = 0; attempt < 5; attempt++) {
      // 방법 1: calendar-dropdown 영역 내 버튼 (가장 정확)
      const calendarDropdown = doc.querySelector('.calendar-dropdown');
      if (calendarDropdown) {
        // calendar-dropdown-button 클래스 버튼
        const dropdownBtn = calendarDropdown.querySelector('button.calendar-dropdown-button');
        if (dropdownBtn && dropdownBtn.offsetParent !== null && !dropdownBtn.disabled) {
          Utils.updateProgressModal({ debug: '✅ calendar-dropdown-button 클릭' });
          dropdownBtn.click();
          await this._randomDelay(200, 400);
          return true;
        }
        
        // calendar-dropdown 내부의 아무 버튼
        const anyBtn = calendarDropdown.querySelector('button:not(.ant-calendar-prev-month-btn):not(.ant-calendar-next-month-btn)');
        if (anyBtn && anyBtn.offsetParent !== null && !anyBtn.disabled) {
          const btnText = anyBtn.textContent?.trim() || '';
          // "새 광고 시작하기" 버튼은 제외
          if (!btnText.includes('새 광고') && !btnText.includes('시작하기')) {
            Utils.updateProgressModal({ debug: `✅ calendar-dropdown 내부 버튼 클릭: "${btnText}"` });
            anyBtn.click();
            await this._randomDelay(200, 400);
            return true;
          }
        }
      }
      
      // 방법 2: 정확한 클래스로 찾기 (calendar-dropdown 외부)
      const exactSelectors = [
        'button.calendar-dropdown-button',
        '.calendar-dropdown-button'
      ];
      
      for (const selector of exactSelectors) {
        try {
          const btn = doc.querySelector(selector);
          if (btn && btn.offsetParent !== null && !btn.disabled) {
            const btnText = btn.textContent?.trim() || '';
            if (!btnText.includes('새 광고') && !btnText.includes('시작하기')) {
              Utils.updateProgressModal({ debug: `✅ ${selector} 클릭` });
              btn.click();
              await this._randomDelay(200, 400);
              return true;
            }
          }
        } catch (e) {}
      }
      
      // 방법 3: "조회" 텍스트가 있는 버튼 (단, "새 광고" 버튼 제외)
      const allButtons = doc.querySelectorAll('button');
      for (const btn of allButtons) {
        const text = btn.textContent?.trim() || '';
        const hasSearchIcon = btn.querySelector('.anticon-search');
        
        // "새 광고 시작하기" 버튼은 절대 클릭하지 않음
        if (text.includes('새 광고') || text.includes('시작하기') || btn.classList.contains('btn-new-campaign')) {
          continue;
        }
        
        if ((text === '조회' || hasSearchIcon) && 
            !btn.disabled && 
            btn.offsetParent !== null) {
          Utils.updateProgressModal({ debug: `✅ 조회 버튼 클릭: "${text}"` });
          btn.click();
          await this._randomDelay(200, 400);
          return true;
        }
      }
      
      Utils.updateProgressModal({ debug: `⚠️ 조회 버튼 찾기 실패 (시도 ${attempt + 1}/5)` });
      await this._randomDelay(300, 500);
    }
    
    Utils.updateProgressModal({ debug: '❌ 조회 버튼을 찾을 수 없음' });
    return false;
  },

  _isTargetBrandStoreName(text) {
    const normalized = String(text || '').replace(/\s+/g, ' ').trim();
    return /(^|[^가-힣A-Za-z0-9])(도리당|나홀로)/.test(normalized);
  },

  _targetBrandRank(text) {
    const normalized = String(text || '').replace(/\s+/g, ' ').trim();
    if (/(^|[^가-힣A-Za-z0-9])도리당/.test(normalized)) return 0;
    if (/(^|[^가-힣A-Za-z0-9])나홀로/.test(normalized)) return 1;
    return 2;
  },

  _normalizeStoreName(text) {
    let normalized = String(text || '')
      .replace(/\s+/g, '')
      .replace(/[·ㆍ.,]/g, '')
      .trim();
    normalized = normalized.replace(/^닭도리탕전문/, '');
    normalized = normalized.replace(/구로디지털단지점/g, '구로디지털점');
    return normalized;
  },

  _targetStoreSet(opts = {}) {
    if (!Array.isArray(opts.targetStores) || opts.targetStores.length === 0) return null;
    return new Set(opts.targetStores.map(s => this._normalizeStoreName(s)).filter(Boolean));
  },

  _storeBranchKey(name) {
    const parts = String(name || '').replace(/\s+/g, ' ').trim().split(' ');
    return parts.length ? parts[parts.length - 1] : '';
  },

  _filterTargetStores(storeNames, opts = {}) {
    const targetSet = this._targetStoreSet(opts);
    const brandStores = (storeNames || []).filter(storeName => this._isTargetBrandStoreName(storeName));
    if (!targetSet) return brandStores;
    const candidates = storeNames || [];
    const targets = [...targetSet];
    const targetBranches = new Set((opts.targetStores || []).map(name => this._storeBranchKey(name)).filter(Boolean));
    return candidates.filter(storeName => {
      const normalized = this._normalizeStoreName(storeName);
      const directMatch = targets.some(target => normalized === target || normalized.includes(target) || target.includes(normalized));
      if (directMatch) return true;
      return this._isTargetBrandStoreName(storeName) && targetBranches.has(this._storeBranchKey(storeName));
    });
  },

  _isTargetRequested(storeName, opts = {}) {
    const targetSet = this._targetStoreSet(opts);
    if (!targetSet) return this._isTargetBrandStoreName(storeName);
    const normalized = this._normalizeStoreName(storeName);
    return [...targetSet]
      .some(target => normalized === target || normalized.includes(target) || target.includes(normalized));
  },

  _resolveBatchTargetDate(opts = {}) {
    const raw = String(opts.targetDate || '').trim();
    let m = raw.match(/^(\d{4})(\d{2})(\d{2})$/) || raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (m) {
      const d = new Date(parseInt(m[1], 10), parseInt(m[2], 10) - 1, parseInt(m[3], 10));
      if (!isNaN(d.getTime())) {
        d.setHours(0, 0, 0, 0);
        return d;
      }
    }
    const d = new Date();
    if (opts.targetDateMode !== 'today') d.setDate(d.getDate() - 1);
    d.setHours(0, 0, 0, 0);
    return d;
  },

  _batchTargetDateLabel(opts = {}) {
    return opts.targetDateMode === 'today' ? '오늘' : '어제';
  },

  _ordersDownloadDateStr() {
    const range = this._readOrdersDateRange?.();
    const endMs = parseInt(range?.end, 10);
    if (!isNaN(endMs)) {
      const d = new Date(endMs);
      if (!isNaN(d.getTime())) return this._formatDate(d).replace(/-/g, '');
    }
    return Utils.getTodayStr();
  },

  // ✅ 완전히 재작성된 CMG 수집 함수
  async _collectCMGMultiStore(opts = {}) {
    // runner 대시보드에 완료 신호 보내는 헬퍼 (top 프레임에서만)
    const _sendCMGComplete = (storeCount, completedCount, stopped, error, storeResults = []) => {
      if (window.top !== window.self) return;
      try {
        chrome.runtime.sendMessage({
          type: 'MULTISTORE_COMPLETE',
          payload: { category: 'cmg', storeCount, completedCount, stopped: !!stopped, error: !!error, storeResults }
        }).catch(() => {});
      } catch (_) {}
    };

    const wujieApp = document.querySelector('WUJIE-APP');
    const doc = wujieApp?.shadowRoot || document;
    const shopInfo = this._getShopInfo();

    // 도리당/나홀로 매장 목록 읽기
    Utils.showProgressModal('CMG 수집', '매장 목록 확인 중...');
    let matchingStores = await this._getCMGMatchingStores(doc);
    matchingStores = this._filterTargetStores(matchingStores, opts);
    Utils.updateProgressModal({ debug: `드롭다운 탐색 결과: ${matchingStores.length}개 (${matchingStores.join(', ') || '없음'})` });
    if (matchingStores.length === 0) {
      // 매장명 다중 셀렉터 탐색 (도리당/나홀로 매칭 우선)
      const curStoreCMG = this._findCurStoreName(doc) || this._findCurStoreName(document) || '';
      const curStore = curStoreCMG || (doc.querySelector('.dropdown-placeholder > span') || doc.querySelector('.dropdown-placeholder'))?.textContent.trim() || '';
      Utils.updateProgressModal({ debug: '🔍 CMG 매장명 탐색: "' + curStore + '"' });
      if (curStore && !this._isTargetRequested(curStore, opts)) {
        _sendCMGComplete(0, 0, false, false, []);
        return;
      }
      if (curStore.includes('전체 매장') || !curStore) {
        Utils.showError('매장 선택 필요', '드롭다운에서 도리당/나홀로 매장을 찾지 못했습니다.\n개별 매장을 직접 선택 후 다시 실행해주세요.');
        _sendCMGComplete(0, 0, false, true); // error:true → runner가 fail로 처리
        return;
      }
      // 도리당/나홀로 아닌 타 브랜드 → skipped로 표시 (재시도 대상 아님)
      if (!this._isTargetBrandStoreName(curStore)) {
        Utils.updateProgressModal({ debug: '⏭️ 수집 대상 브랜드 아님 (' + curStore + ') → 건너뜀' });
        if (window.top === window.self) {
          try { chrome.runtime.sendMessage({ type: 'MULTISTORE_COMPLETE',
            payload: { category: 'cmg', storeCount: 0, completedCount: 0, stopped: false, error: false, skipped: true, reason: 'not_target_brand', curStoreName: curStore }
          }).catch(() => {}); } catch (_) {}
        }
        return;
      }
      Utils.updateProgressModal({ debug: `단일매장 모드: ${curStore}` });
      await this._collectCMGData(shopInfo, opts);
        _sendCMGComplete(1, 1, false, false, [{ storeName: curStore, completed: true, status: 'ok', note: '단일매장 완료' }]);
        return;
    }

    // 캘린더에서 날짜 범위 읽기
    let dateRange = this._readCMGDateRange(doc);
    if (!dateRange) {
      // 달력(.calendar-dropdown-input) 없음 → 날짜 선택 불가 → 데이터 0행 수집
      // 어제날짜 강제 수집 시 calendarBtn null → 빈 결과 → cmgOk 오판 문제 있음
      // → error:true 반환해서 runner의 F5 재시도 유도
      Utils.updateProgressModal({ debug: '⚠️ 달력 없음 → F5 재시도 필요 (error 반환)' });
      _sendCMGComplete(0, 0, false, true);  // error:true → runner F5
      return;
    }

    let { startDate, endDate } = dateRange;
    if (this._isDashboardBatchOrders(opts)) {
      const targetDate = this._resolveBatchTargetDate(opts);
      startDate = new Date(targetDate);
      endDate = new Date(targetDate);
    } else {
      // 수동 수집은 기존 캘린더 날짜를 쓰되 오늘/미래는 어제로 clamp
      const yd = new Date(); yd.setDate(yd.getDate() - 1); yd.setHours(0, 0, 0, 0);
      if (startDate > yd) startDate = new Date(yd);
      if (endDate > yd) endDate = new Date(yd);
    }

    const dates = [];
    const cur = new Date(startDate);
    while (cur <= endDate) { dates.push(new Date(cur)); cur.setDate(cur.getDate() + 1); }

    // 도리당 먼저 정렬
    matchingStores.sort((a, b) => this._targetBrandRank(a) - this._targetBrandRank(b));

    // 실행 기준일별 체크포인트 로드
    const cmgCheckpointDate = dates.length > 0
      ? this._formatDate(dates[dates.length - 1]).replace(/-/g, '')
      : Utils.getTodayStr();
    const checkpoint = await this._loadCMGCheckpoint();
    const completedStores = (checkpoint?.date === cmgCheckpointDate)
      ? (checkpoint.completedStores || []).filter(store => {
          const completedKey = this._normalizeStoreName(store);
          return matchingStores.some(name => this._normalizeStoreName(name) === completedKey);
        })
      : [];
    const storeResults = [];

    Utils.showMultiStoreProgressModal(matchingStores);
    Utils.updateProgressModal({ debug: `📅 수집 기간: ${this._formatDate(startDate)} ~ ${this._formatDate(endDate)} (${dates.length}일)` });
    if (completedStores.length > 0) {
      Utils.updateProgressModal({ debug: `📌 기준일 완료된 매장: ${completedStores.join(', ')} → 건너뜀` });
    }

    for (let i = 0; i < matchingStores.length; i++) {
      if (this._stopFlag) break;

      Utils.setMultiStoreIndex(i);
      const storeName = matchingStores[i];

      if (completedStores.some(store => this._normalizeStoreName(store) === this._normalizeStoreName(storeName))) {
        Utils.updateProgressModal({ debug: `⏭️ 기준일 이미 완료됨, 건너뜀` });
        Utils.updateMultiStoreStoreResult('success', '⏭️ 기준일 이미 수집됨');
        storeResults.push({ storeName, completed: true, status: 'ok', skipped: true, note: '기준일 이미 수집됨' });
        continue;
      }

      const cmgEndDateStr = dates.length > 0
        ? this._formatDate(dates[dates.length - 1]).replace(/-/g, '')
        : Utils.getTodayStr();
      const cmgBeacon = await Utils.checkDownloadBeacon(storeName, cmgEndDateStr, 'cmg');
      if (cmgBeacon) {
        Utils.updateProgressModal({ debug: `📁 CMG beacon 확인됨(${cmgEndDateStr}), 건너뜀: ${storeName}` });
        Utils.updateMultiStoreStoreResult('success', '📁 이미 수집됨 (beacon)');
        storeResults.push({ storeName, completed: true, status: 'ok', skipped: true, note: 'beacon: 다운로드 파일 확인' });
        continue;
      }

      Utils.updateProgressModal({ debug: `→ ${storeName} 선택 시도` });
      const selected = await this._selectCMGStore(doc, storeName);
      if (!selected) {
        Utils.updateProgressModal({ debug: `❌ 매장 선택 실패` });
        Utils.updateMultiStoreStoreResult('error', '매장 선택 실패');
        storeResults.push({ storeName, completed: false, status: 'fail', error: true, note: '매장 선택 실패' });
        continue;
      }
      Utils.updateProgressModal({ debug: `✅ 매장 선택 완료, 자동조회 대기` });

      const allRows = await this._collectCMGForStore(doc, storeName, dates);

      if (!allRows.length) {
        Utils.updateMultiStoreStoreResult('warning', `데이터 없음`);
        storeResults.push({ storeName, completed: false, status: 'warn', note: '데이터 없음' });
        continue;
      }

      const headers = [
        'collected_at', '조회일자', '매장명', '광고상태', '광고기간', '광고비율',
        '광고비용', '신규고객', '광고주문수', '광고매출', '전체매출', '광고클릭수', '광고노출수'
      ];
      const csv = Utils.toCSV(headers, allRows);
      const filenameDateStr = dates.length === 1
        ? this._formatDate(dates[0]).replace(/-/g, '')
        : `${this._formatDate(dates[0]).replace(/-/g, '')}-${this._formatDate(dates[dates.length - 1]).replace(/-/g, '')}`;

      const filename = await Utils.downloadCSV(csv, {
        channel: 'coupangeats', purpose: 'cmg',
        storeName, storeId: '', dateStr: filenameDateStr
      });

      const status = this._stopFlag ? 'warning' : 'success';
      Utils.updateMultiStoreStoreResult(status, `${allRows.length}행 / ${filename}`);
      Utils.updateProgressModal({ debug: `✅ 저장 완료: ${filename}` });
      storeResults.push({ storeName, completed: !this._stopFlag, status, note: `${allRows.length}행 / ${filename}` });

      if (!this._stopFlag) {
        completedStores.push(storeName);
        await this._saveCMGCheckpoint({ date: cmgCheckpointDate, completedStores: [...completedStores] });
        Utils.updateProgressModal({ debug: `💾 체크포인트 저장 (${completedStores.length}/${matchingStores.length})` });
      }
    }

    if (!this._stopFlag) await this._clearCMGCheckpoint();
    Utils.finalizeMultiStoreModal();
    _sendCMGComplete(matchingStores.length, storeResults.filter(r => r.completed).length, this._stopFlag, false, storeResults);
  },

  _readCMGDateRange(doc) {
    const calBtn = doc.querySelector('.calendar-dropdown-input');
    if (!calBtn) return null;
    const startTxt = calBtn.querySelector('.start-date')?.textContent.trim();
    const endTxt = calBtn.querySelector('.end-date')?.textContent.replace(/[~\s]/g, '').trim();
    const parseYYMMDD = (s) => {
      if (!s) return null;
      const [y, m, d] = s.split('.');
      if (!y || !m || !d) return null;
      const dt = new Date(2000 + parseInt(y), parseInt(m) - 1, parseInt(d));
      return isNaN(dt.getTime()) ? null : dt;
    };
    const startDate = parseYYMMDD(startTxt);
    const endDate = parseYYMMDD(endTxt);
    return (startDate && endDate) ? { startDate, endDate } : null;
  },

  // ant-design 드롭다운 항목 탐색 (shadow root / document 양쪽, offsetParent 체크 없음)
  async _waitForCMGDropdownItems(doc, timeoutMs = 2000, requireTargetBrand = false) {
    const selectors = [
      'li.ant-dropdown-menu-item.store-item',  // CMG 전용 클래스
      'li.ant-dropdown-menu-item',
      '[role="menuitem"]',
    ];
    const start = Date.now();
    let lastItems = [];
    while (Date.now() - start < timeoutMs) {
      const seen = new Set();
      const items = [];
      for (const sel of selectors) {
        const found = [
          ...document.querySelectorAll(sel),
          ...(doc && doc !== document ? doc.querySelectorAll(sel) : [])
        ];
        for (const el of found) {
          const text = (el.textContent || '').trim();
          if (!text || seen.has(text)) continue;
          seen.add(text);
          items.push(el);
        }
      }
      if (items.length > 0) {
        lastItems = items;
        if (!requireTargetBrand || items.some(el => this._isTargetBrandStoreName(el.textContent))) {
          return items;
        }
      }
      await new Promise(r => setTimeout(r, 100));
    }
    return lastItems;
  },

  async _getCMGMatchingStores(doc) {
    const trigger = doc.querySelector('form.cmgeats-store-dropdown');
    if (!trigger) return [];

    trigger.click();
    const items = await this._waitForCMGDropdownItems(doc, 8000, true);

    const foundTexts = items.map(el => el.textContent.trim()).filter(Boolean);
    const matching = foundTexts.filter(t => this._isTargetBrandStoreName(t));
    if (matching.length === 0 && foundTexts.length > 0) {
      Utils.updateProgressModal({ debug: `CMG 드롭다운 항목 발견: ${foundTexts.slice(0, 8).join(' / ')}` });
    }

    // 드롭다운 닫기 — 트리거 재클릭 (ESC dispatch는 stopFlag 오작동 유발)
    trigger.click();
    await new Promise(r => setTimeout(r, 200));
    return matching.length ? matching : foundTexts;
  },

  async _selectCMGStore(doc, storeText) {
    const trigger = doc.querySelector('form.cmgeats-store-dropdown');
    if (!trigger) return false;

    trigger.click();
    const items = await this._waitForCMGDropdownItems(doc, 8000, true);

    const targetKey = this._normalizeStoreName(storeText);
    const target = items.find(el => {
      const t = el.textContent.trim();
      const key = this._normalizeStoreName(t);
      return key === targetKey || key.includes(targetKey) || targetKey.includes(key);
    });
    if (!target) {
      trigger.click(); // 드롭다운 닫기
      return false;
    }

    target.click(); // li 클릭 → 자동 선택 + 드롭다운 닫힘
    await this._randomDelay(700, 1000);
    return true;
  },

  async _collectCMGForStore(doc, storeName, dates) {
    const allRows = [];
    const iso = new Date().toISOString();

    for (let i = 0; i < dates.length; i++) {
      if (this._stopFlag) break;

      const targetDate = dates[i];
      const dateStr = this._formatDate(targetDate);

      Utils.updateProgressModal({
        currentPage: i + 1,
        totalPages: dates.length,
        rowCount: allRows.length,
        message: `${dateStr} 조회 중...`,
        debug: `===== ${dateStr} =====`
      });

      let dateSuccess = false;
      for (let retry = 0; retry < 2; retry++) {
        const calendarBtn = doc.querySelector('.calendar-dropdown-input');
        if (!calendarBtn) { Utils.updateProgressModal({ debug: '❌ 달력 버튼 없음' }); break; }

        calendarBtn.click();
        await this._randomDelay(500, 800);

        const clicked = await this._selectDateInCalendar(doc, targetDate);
        if (!clicked) { Utils.updateProgressModal({ debug: `❌ ${dateStr} 선택 실패` }); continue; }

        Utils.updateProgressModal({ debug: `✅ ${dateStr} 선택 완료` });
        dateSuccess = true;
        break;
      }

      if (!dateSuccess) {
        Utils.updateProgressModal({ debug: `❌ ${dateStr} 전체 실패, 다음 날짜로` });
        continue;
      }

      await this._randomDelay(1500, 2000);

      const pageRows = await this._collectCMGRowsWithRetry(doc, iso, dateStr, storeName);
      Utils.updateProgressModal({ rowCount: allRows.length + pageRows.length, debug: `${dateStr}: ${pageRows.length}행 수집` });
      allRows.push(...pageRows);

      await this._randomDelay(300, 500);
    }

    return allRows;
  },

  _loadCMGCheckpoint() {
    return new Promise(r => chrome.storage.local.get(['ce_cmg_checkpoint'], res => r(res.ce_cmg_checkpoint || null)));
  },
  _saveCMGCheckpoint(data) {
    return new Promise(r => chrome.storage.local.set({ ce_cmg_checkpoint: data }, r));
  },
  _clearCMGCheckpoint() {
    return new Promise(r => chrome.storage.local.remove(['ce_cmg_checkpoint'], r));
  },

  async _collectCMGData(shopInfo, opts = {}) {
    const wujieApp = document.querySelector('WUJIE-APP');
    const doc = wujieApp?.shadowRoot || document;

    // 1. 매장 선택 정보 확인
    const storeDropdown = doc.querySelector('.dropdown-placeholder > span');
    const storeText = storeDropdown?.textContent.trim() || '';

    Utils.updateProgressModal({ debug: `드롭다운 매장: "${storeText}"` });

    if (storeText.includes('전체 매장')) {
      Utils.showError('매장 선택 필요', '전체 매장이 아닌 개별 매장을 선택해주세요.');
      return;
    }

    // 매장명 정규화 (괄호와 숫자 제거)
    const selectedStore = storeText.replace(/\s*\(\d+개?\)?\s*$/, '').trim();

    let startDate, endDate;
    let parseResult = {};

    if (!this._isDashboardBatchOrders(opts)) {
      // 수동 수집: 현재 캘린더에 설정된 날짜 그대로 사용 (prompt 없음)
      const dateRange = this._readCMGDateRange(doc);
      if (!dateRange) {
        Utils.showError('날짜 읽기 실패', '현재 페이지의 달력에서 날짜를 읽을 수 없습니다.\n날짜를 직접 설정한 후 다시 시도해주세요.');
        return;
      }
      startDate = dateRange.startDate;
      endDate = dateRange.endDate;
      Utils.updateProgressModal({ debug: `📅 수동 수집: 현재 캘린더 날짜 사용 (${this._formatDate(startDate)} ~ ${this._formatDate(endDate)})` });
    } else if (opts.targetDate || opts.targetDateMode) {
      const targetDate = this._resolveBatchTargetDate(opts);
      startDate = new Date(targetDate);
      endDate = new Date(targetDate);
      Utils.updateProgressModal({ debug: `📅 배치 수집: ${this._batchTargetDateLabel(opts)} 날짜 사용 (${this._formatDate(targetDate)})` });
    } else {
      // 배치/일반 수집: 날짜 범위 입력 prompt
      const rangeInput = prompt(
        'CMG 수집 날짜 범위를 입력하세요.\n\n' +
        `선택된 매장: ${selectedStore}\n\n` +
        '형식0: -YY.MM.DD 또는 -YYYYMMDD → 입력한 날짜까지 최근 7일을 하루씩 수집\n' +
        '형식0-2: -{YY.MM.DD} 또는 -{YYYYMMDD} → 위와 동일\n' +
        '형식1: YY.MM.DD-YY.MM.DD (예: 26.01.01-26.01.15)\n' +
        '형식2: YY.MM.DD (예: 26.01.15) → 해당일부터 어제까지 (오늘 입력 시 어제 하루)\n' +
        '형식3: YYYYMMDD-YYYYMMDD (예: 20260101-20260115)\n' +
        '형식4: YYYYMMDD (예: 20260115) → 해당일부터 어제까지 (오늘 입력 시 어제 하루)'
      );

      if (!rangeInput) {
        Utils.showError('취소됨', '날짜를 입력하지 않았습니다.');
        return;
      }

      parseResult = this._parseDateRange(rangeInput);
      console.log('[CMG-DEBUG] parseResult:', JSON.stringify(parseResult, null, 2), 'raw:', parseResult);

      if (parseResult.error) {
        Utils.showError('형식 오류', parseResult.error);
        return;
      }

      startDate = parseResult.startDate;
      endDate = parseResult.endDate;

      if (!startDate || !endDate) {
        Utils.showError('날짜 오류 [03.js]', `start=${startDate}, end=${endDate}, keys=${Object.keys(parseResult)}`);
        return;
      }
    }

    // 수동/프롬프트 수집은 오늘/미래 날짜 → 어제로 clamp. 배치 버튼 날짜는 그대로 허용.
    if (!(opts.targetDate || opts.targetDateMode)) {
      const yd = new Date();
      yd.setDate(yd.getDate() - 1);
      yd.setHours(0, 0, 0, 0);
      if (startDate > yd) { startDate = new Date(yd); }
      if (endDate > yd) { endDate = new Date(yd); }
    }
    if (startDate > endDate) {
      const tmp = new Date(startDate);
      startDate = new Date(endDate);
      endDate = tmp;
    }
    
    // 날짜 목록 생성
    const dates = [];
    const current = new Date(startDate);
    while (current <= endDate) {
      dates.push(new Date(current));
      current.setDate(current.getDate() + 1);
    }
    
    const collectionLabel = parseResult.mode === 'rollingWeek'
      ? `${selectedStore} - 기준일 포함 최근 ${dates.length}일 수집 예정`
      : `${selectedStore} - ${dates.length}일 수집 예정`;

    Utils.showProgressModal('CMG 데이터 수집', collectionLabel);
    Utils.updateProgressModal({ debug: `선택 매장: "${selectedStore}"` });
    
    const allRows = [];
    const iso = new Date().toISOString();
    
    for (let i = 0; i < dates.length; i++) {
      if (this._stopFlag) break;
      
      const targetDate = dates[i];
      const dateStr = this._formatDate(targetDate);
      
      Utils.updateProgressModal({
        currentPage: i + 1,
        totalPages: dates.length,
        rowCount: allRows.length,
        message: `${dateStr} 조회 중...`,
        debug: `===== ${dateStr} 시작 =====`
      });
      
      // 날짜 선택 재시도 로직
      let dateSuccess = false;
      for (let retry = 0; retry < 2; retry++) {
        // 달력 열기
        const calendarBtn = doc.querySelector('.calendar-dropdown-input');
        if (!calendarBtn) {
          Utils.updateProgressModal({ debug: '❌ 달력 버튼 없음' });
          break;
        }
        
        Utils.updateProgressModal({ debug: `달력 열기 (시도 ${retry + 1})` });
        calendarBtn.click();
        await this._randomDelay(500, 800);

        // 날짜 선택 + 적용 버튼 클릭 (함수 내에서 처리)
        const clicked = await this._selectDateInCalendar(doc, targetDate);
        if (!clicked) {
          Utils.updateProgressModal({ debug: `❌ ${dateStr} 선택 실패` });
          continue;
        }

        Utils.updateProgressModal({ debug: `✅ ${dateStr} 선택 및 적용 완료` });
        dateSuccess = true;
        break;
      }

      if (!dateSuccess) {
        Utils.updateProgressModal({ debug: `❌ ${dateStr} 전체 실패, 다음 날짜로` });
        continue;
      }

      // 데이터 로딩 대기 (적용 버튼 클릭 후 데이터 갱신 대기)
      Utils.updateProgressModal({ debug: '데이터 로딩 대기 중...' });
      await this._randomDelay(1500, 2000);

      // 현재 페이지 데이터 수집 (매장 필터링 포함)
      const pageRows = await this._collectCMGRowsWithRetry(doc, iso, dateStr, selectedStore);

      Utils.updateProgressModal({
        rowCount: allRows.length + pageRows.length,
        debug: `${dateStr}: ${pageRows.length}행 수집 완료`
      });

      allRows.push(...pageRows);

      await this._randomDelay(300, 500);
    }
    
    if (!allRows.length) {
      Utils.showSuccessModal('데이터 없음', 
        `선택 매장: ${selectedStore}\n수집된 데이터가 없습니다.\n\n매장명이 테이블과 일치하는지 확인해주세요.`, 
        { status: 'error' });
      return;
    }
    
    const headers = [
      'collected_at', '조회일자', '매장명', '광고상태', '광고기간', '광고비율',
      '광고비용', '신규고객', '광고주문수', '광고매출', '전체매출', '광고클릭수', '광고노출수'
    ];
    
    const csv = Utils.toCSV(headers, allRows);
    const filenameDateStr = dates.length === 1 
      ? this._formatDate(dates[0]).replace(/-/g, '') 
      : `${this._formatDate(dates[0]).replace(/-/g, '')}-${this._formatDate(dates[dates.length - 1]).replace(/-/g, '')}`;
    
    const filename = Utils.downloadCSV(csv, {
      channel: 'coupangeats',
      purpose: 'cmg',
      storeName: selectedStore || 'store',
      storeId: '',
      dateStr: filenameDateStr
    });
    
    const modalStatus = this._stopFlag ? 'warning' : 'success';
    const statusText = this._stopFlag ? '(ESC 중단됨)' : '';
    
    const details = `📊 수집 결과:
- 선택 매장: ${selectedStore}
- ${dates.length}일 수집
- 총 ${allRows.length.toLocaleString()}행

📁 파일명:
${filename}`;
    
    Utils.showSuccessModal(`CMG 수집 완료 ${statusText}`, details, { status: modalStatus });
  },

  _parseDate(str) {
    const year = parseInt(str.substring(0, 4));
    const month = parseInt(str.substring(4, 6)) - 1;
    const day = parseInt(str.substring(6, 8));
    const date = new Date(year, month, day);
    return isNaN(date.getTime()) ? null : date;
  },

  _formatDate(date) {
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, '0');
    const d = String(date.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  },

  // ✅ 개선된 날짜 선택 함수
  async _selectDateInCalendar(doc, targetDate) {
    const year = targetDate.getFullYear();
    const month = targetDate.getMonth();
    const day = targetDate.getDate();
    
    // 월 이동
    const moved = await this._navigateToMonth(doc, year, month);
    if (!moved) {
      return false;
    }
    
    await this._randomDelay(300, 500);

    // 날짜 셀 찾기
    const dayStr = String(day);
    let selectedDateCell = null;
    
    // 왼쪽 캘린더에서 찾기
    const leftCalendar = doc.querySelector('.ant-calendar-range-left, .ant-calendar-range-part:first-child');
    if (!leftCalendar) {
      Utils.updateProgressModal({ debug: '❌ 왼쪽 캘린더 없음' });
      return false;
    }
    
    const leftCells = leftCalendar.querySelectorAll('td.ant-calendar-cell');
    
    for (const cell of leftCells) {
      const innerEl = cell.querySelector('.ant-calendar-date');
      const text = innerEl?.textContent.trim();
      
      if (text === dayStr) {
        const isDisabled = cell.classList.contains('ant-calendar-disabled-cell');
        const isLastMonth = cell.classList.contains('ant-calendar-last-month-cell');
        const isNextMonth = cell.classList.contains('ant-calendar-next-month-btn-day');
        
        if (isDisabled || isLastMonth || isNextMonth) {
          continue;
        }
        
        selectedDateCell = innerEl;
        break;
      }
    }
    
    if (!selectedDateCell) {
      Utils.updateProgressModal({ debug: `❌ ${day}일 찾지 못함` });
      return false;
    }
    
    // 시작일 클릭
    Utils.updateProgressModal({ debug: `${day}일 클릭 (시작)` });
    selectedDateCell.click();
    await this._randomDelay(200, 300);

    // 종료일 클릭 (같은 날짜)
    Utils.updateProgressModal({ debug: `${day}일 클릭 (종료)` });
    selectedDateCell.click();
    await this._randomDelay(200, 300);
    
    // ✅ "적용" 버튼 클릭 - 텍스트로 정확히 찾기
    let applyClicked = false;
    
    // 방법 1: "적용" 텍스트가 있는 버튼 찾기
    const allButtons = doc.querySelectorAll('button.ant-btn');
    for (const btn of allButtons) {
      const btnText = btn.textContent?.trim();
      if (btnText === '적용' && btn.offsetParent !== null && !btn.disabled) {
        Utils.updateProgressModal({ debug: '✅ "적용" 버튼 클릭' });
        btn.click();
        applyClicked = true;
        await this._randomDelay(400, 700);
        break;
      }
    }
    
    // 방법 2: 클래스로 찾기 (백업) - 단, "새 광고" 버튼 제외
    if (!applyClicked) {
      const smallPrimaryBtns = doc.querySelectorAll('button.ant-btn.ant-btn-primary.ant-btn-sm');
      for (const btn of smallPrimaryBtns) {
        const btnText = btn.textContent?.trim() || '';
        if (!btnText.includes('새 광고') && !btnText.includes('시작하기') && btn.offsetParent !== null) {
          Utils.updateProgressModal({ debug: `✅ 적용 버튼 클릭 (클래스): "${btnText}"` });
          btn.click();
          applyClicked = true;
          await this._randomDelay(400, 700);
          break;
        }
      }
    }
    
    if (!applyClicked) {
      Utils.updateProgressModal({ debug: 'ℹ️ 적용 버튼 없음 (자동 적용)' });
    }
    
    return true;
  },

  // ✅ 개선된 월 이동 함수
  async _navigateToMonth(doc, targetYear, targetMonth) {
    for (let i = 0; i < 24; i++) {
      // 현재 연월 확인
      const yearEl = doc.querySelector('.ant-calendar-year-select, .ant-calendar-my-select .ant-calendar-year-select');
      const monthEl = doc.querySelector('.ant-calendar-month-select, .ant-calendar-my-select .ant-calendar-month-select');
      
      if (!yearEl) {
        return true; // 헤더 없으면 현재 월 사용
      }
      
      const yearText = yearEl.textContent || '';
      const monthText = monthEl?.textContent || '';
      
      const yearMatch = yearText.match(/(\d{4})/);
      const monthMatch = monthText.match(/(\d{1,2})/);
      
      if (!yearMatch) {
        return true;
      }
      
      const currentYear = parseInt(yearMatch[1]);
      const currentMonth = monthMatch ? parseInt(monthMatch[1]) - 1 : 0;
      
      if (currentYear === targetYear && currentMonth === targetMonth) {
        return true;
      }
      
      const targetTime = new Date(targetYear, targetMonth, 1).getTime();
      const currentTime = new Date(currentYear, currentMonth, 1).getTime();
      
      if (targetTime < currentTime) {
        const prevBtn = doc.querySelector('.ant-calendar-prev-month-btn, button.ant-calendar-prev-month-btn');
        if (prevBtn) {
          prevBtn.click();
          await this._randomDelay(500, 800);
        } else {
          return true;
        }
      } else {
        const nextBtn = doc.querySelector('.ant-calendar-next-month-btn, button.ant-calendar-next-month-btn');
        if (nextBtn) {
          nextBtn.click();
          await this._randomDelay(500, 800);
        } else {
          return true;
        }
      }
    }
    
    return true;
  },

  _toCMGNumber(value) {
    const n = parseInt(String(value || '0').replace(/[^\d-]/g, ''), 10);
    return isNaN(n) ? 0 : n;
  },

  _summarizeCMGRows(rows) {
    return rows.reduce((sum, row) => {
      sum.adCost += this._toCMGNumber(row['광고비용']);
      sum.adSales += this._toCMGNumber(row['광고매출']);
      sum.revenue += this._toCMGNumber(row['전체매출']);
      sum.orders += this._toCMGNumber(row['광고주문수']);
      return sum;
    }, { rowCount: rows.length, adCost: 0, adSales: 0, revenue: 0, orders: 0 });
  },

  _logCMGExpected(dateStr, selectedStore, stats, suffix = '') {
    Utils.updateProgressModal({
      debug: `📊 CMG 기대값${suffix} - 매장: ${selectedStore} | 날짜: ${dateStr} | 행: ${stats.rowCount} | 광고비용: ${stats.adCost.toLocaleString()}원 | 광고매출: ${stats.adSales.toLocaleString()}원 | 전체매출: ${stats.revenue.toLocaleString()}원 | 광고주문수: ${stats.orders.toLocaleString()}건`
    });
  },

  async _collectCMGRowsWithRetry(doc, iso, dateStr, selectedStore) {
    let pageRows = this._collectCurrentCMGData(doc, iso, dateStr, selectedStore);
    let stats = this._summarizeCMGRows(pageRows);
    this._logCMGExpected(dateStr, selectedStore, stats);

    const isAllZero = stats.rowCount > 0 &&
      stats.adCost === 0 &&
      stats.adSales === 0 &&
      stats.revenue === 0 &&
      stats.orders === 0;

    if (!isAllZero || this._stopFlag) return pageRows;

    Utils.updateProgressModal({
      debug: `⚠️ CMG 0원 감지 (${selectedStore} / ${dateStr}) → 재조회 (1회)`
    });

    const clicked = await this._clickSearchButton(doc);
    if (!clicked) {
      Utils.updateProgressModal({ debug: '⚠️ CMG 재조회 버튼 클릭 실패 → 1차 결과 유지' });
      return pageRows;
    }

    await this._randomDelay(1500, 2500);
    const retryRows = this._collectCurrentCMGData(doc, iso, dateStr, selectedStore);
    const retryStats = this._summarizeCMGRows(retryRows);
    this._logCMGExpected(dateStr, selectedStore, retryStats, ' 재조회');

    const retryAllZero = retryStats.rowCount > 0 &&
      retryStats.adCost === 0 &&
      retryStats.adSales === 0 &&
      retryStats.revenue === 0 &&
      retryStats.orders === 0;

    if (retryAllZero) {
      Utils.updateProgressModal({ debug: `⚠️ CMG 재조회 후에도 0원 (${selectedStore} / ${dateStr}) → 계속 진행` });
    }

    return retryRows;
  },

  // ✅ 개선된 데이터 수집 함수 - 매장 필터링 디버그 강화
  _collectCurrentCMGData(doc, iso, dateStr, selectedStore) {
    const rows = [];
    
    let tableRows = doc.querySelectorAll('tbody.table__tbody tr.table__row');
    if (!tableRows.length) {
      tableRows = doc.querySelectorAll('tr.table__row');
    }
    
    // 매장명 정규화 함수
    const normalizeStoreName = (name) => {
      return (name || '')
        .replace(/\s+/g, '')  // 모든 공백 제거
        .replace(/[·]/g, '')  // 가운데점 제거
        .toLowerCase();
    };
    
    const normalizedSelected = normalizeStoreName(selectedStore);
    
    Utils.updateProgressModal({ debug: `테이블 행: ${tableRows.length}개` });
    Utils.updateProgressModal({ debug: `필터: "${selectedStore}" → "${normalizedSelected}"` });
    
    // 필터가 비어있으면 경고
    if (!normalizedSelected) {
      Utils.updateProgressModal({ debug: '⚠️ 필터 매장명이 비어있음 - 전체 수집' });
    }
    
    const getValue = (row, className) => {
      const cell = row.querySelector(`.table__body-${className}`);
      if (!cell) return '0';
      const innerDiv = cell.querySelector('div');
      const text = (innerDiv || cell).textContent.trim();
      return text.replace(/[,원명건번%\s]/g, '');
    };
    
    let matchCount = 0;
    let skipCount = 0;
    
    for (const row of tableRows) {
      const storeNameEl = row.querySelector('.table__body-storeName .link-to');
      const storeName = storeNameEl?.textContent.trim() || '';
      if (!storeName) continue;
      
      const normalizedTableName = normalizeStoreName(storeName);
      
      // ✅ 매장 필터링: 선택한 매장과 일치하는 행만 수집
      if (selectedStore && normalizedSelected) {
        // 정확히 일치하는지 확인
        const isExactMatch = normalizedTableName === normalizedSelected;
        
        if (!isExactMatch) {
          skipCount++;
          Utils.updateProgressModal({ debug: `스킵: "${storeName}"` });
          continue;
        }
        
        Utils.updateProgressModal({ debug: `✅ 매칭: "${storeName}"` });
      }
      
      matchCount++;
      
      const switchEl = row.querySelector('.table__body-active .ant-switch');
      const adActive = switchEl?.classList.contains('ant-switch-checked') ? 'ON' : 'OFF';
      
      const bidCell = row.querySelector('.table__body-bid');
      let bidLines = [];
      if (bidCell) {
        const bidDiv = bidCell.querySelector('div');
        const bidHtml = (bidDiv || bidCell).innerHTML;
        bidLines = bidHtml.split(/<br\s*\/?>/i)
          .map(s => s.replace(/<[^>]*>/g, '').trim())
          .filter(s => s);
      }
      
      const adBidRatio = bidLines.join(', ');
      
      const periodCell = row.querySelector('.table__body-period');
      const periodDiv = periodCell?.querySelector('div');
      const adPeriod = (periodDiv || periodCell)?.textContent.trim().replace(/\s+/g, ' ') || '';
      
      const rowData = {
        collected_at: iso,
        '조회일자': dateStr,
        '매장명': storeName,
        '광고상태': adActive,
        '광고기간': adPeriod,
        '광고비율': adBidRatio,
        '광고비용': getValue(row, 'adCost'),
        '신규고객': getValue(row, 'newCustomerCount60d'),
        '광고주문수': getValue(row, 'orders'),
        '광고매출': getValue(row, 'adSales'),
        '전체매출': getValue(row, 'revenue'),
        '광고클릭수': getValue(row, 'clicks'),
        '광고노출수': getValue(row, 'viewCount')
      };
      
      rows.push(rowData);
    }
    
    Utils.updateProgressModal({ debug: `매칭: ${matchCount}개, 스킵: ${skipCount}개` });
    
    return rows;
  },

  // 현재 페이지에서 매장명 다중 셀렉터로 탐색 (도리당/나홀로 우선)
  _findCurStoreName(scope) {
    const root = scope || document;
    // 1) dropdown-btn (멀티매장 드롭다운)
    const a = root.querySelector?.('.dropdown-btn.highlight')?.textContent?.trim();
    if (a) return a;
    // 2) 헤더/타이틀에서 도리당/나홀로 시작하는 텍스트
    const sels = 'h1, h2, [class*="store-name"], [class*="StoreName"], [class*="shop-name"], [class*="title"], [class*="store"]';
    const cands = root.querySelectorAll?.(sels) || [];
    for (const el of cands) {
      const t = (el.textContent || '').trim();
      if (!t || t.length > 50) continue;
      if (this._isTargetBrandStoreName(t)) return t;
    }
    // 3) document.title 폴백
    const title = document.title || '';
    const m = title.match(/(도리당|나홀로)[^\s\-|·]+/);
    return m ? m[0] : '';
  },

  _getShopInfo() {
    const ordersMatch = location.pathname.match(/\/orders\/(\d+)/);
    const couponsMatch = location.pathname.match(/\/coupons\/detail\/(\d+)/);
    
    let store_id = '';
    let store_name = '';
    let coupon_id = '';
    
    if (ordersMatch) {
      store_id = ordersMatch[1];
    } else if (couponsMatch) {
      coupon_id = couponsMatch[1];
    }
    
    const storeNameEl = document.querySelector('.dropdown-btn.highlight');
    store_name = storeNameEl?.textContent.trim() || '';
    
    if (!store_name && couponsMatch) {
      const couponTitleEl = document.querySelector('.css-1w3nel.e1rwb3ie3');
      if (couponTitleEl) {
        const title = couponTitleEl.textContent.trim();
        store_name = title.replace(/\s*(첫\s*주문|재주문|신규\s*고객)?\s*할인\s*쿠폰\s*$/i, '').trim();
      }
    }
    
    return { store_id, store_name, coupon_id };
  },

  _collectCouponStats(shopInfo) {
    const table = document.querySelector('table.css-1eq3etp');
    if (!table) {
      Utils.showError('테이블 없음', '쿠폰 통계 테이블을 찾을 수 없습니다.');
      return;
    }

    const rows = table.querySelectorAll('tbody tr');
    const allRows = [];
    const iso = new Date().toISOString();

    for (const row of rows) {
      if (row.querySelector('th')) continue;
      
      const cells = row.querySelectorAll('td');
      if (cells.length < 5) continue;

      allRows.push({
        collected_at: iso,
        store_id: shopInfo.coupon_id,
        날짜: cells[0]?.textContent.trim() || '',
        쿠폰적용_주문매출: Utils.cleanPrice(cells[1]?.textContent || ''),
        쿠폰발행_비용: Utils.cleanPrice(cells[2]?.textContent || ''),
        평균주문금액: Utils.cleanPrice(cells[3]?.textContent || ''),
        조회수대비_주문율: cells[4]?.textContent.trim().replace('%', '') || ''
      });
    }

    if (!allRows.length) {
      Utils.showError('데이터 없음', '수집할 쿠폰 통계가 없습니다.');
      return;
    }

    const headers = [
      'collected_at', 'store_id',
      '날짜', '쿠폰적용_주문매출', '쿠폰발행_비용', '평균주문금액', '조회수대비_주문율'
    ];
    
    const csv = Utils.toCSV(headers, allRows);
    const filename = Utils.downloadCSV(csv, {
      channel: 'coupangeats',
      purpose: 'coupon',
      storeName: shopInfo.store_name,
      storeId: shopInfo.coupon_id,
      dateStr: Utils.getTodayStr()
    });
    
    Utils.showSuccess('쿠폰 통계 수집 완료', `${allRows.length}건 → ${filename}`);
  },

  _getCurrentPage() {
    const activeBtn = Array.from(document.querySelectorAll('.merchant-pagination button.active, button.active'))
      .find(btn => !isNaN(parseInt((btn.textContent || '').trim(), 10)));
    const pageNum = parseInt(activeBtn?.textContent.trim() || '0', 10);
    return isNaN(pageNum) ? 1 : pageNum;
  },

  _clickNextPage() {
    const currentPage = this._getCurrentPage();
    const nextPageBtn = this._getVisiblePageNumbers().find(x => x.num === currentPage + 1)?.btn;
    if (nextPageBtn) {
      nextPageBtn.click();
      return true;
    }

    const nextBtn = document.querySelector('.merchant-pagination button[data-at="next-btn"], button[data-at="next-btn"]')
      || Array.from(document.querySelectorAll('.merchant-pagination button')).find(btn => {
        const label = `${btn.getAttribute('aria-label') || ''} ${btn.getAttribute('title') || ''} ${btn.textContent || ''}`.trim();
        return /next|다음|›|>|»/i.test(label);
      });
    const isHidden = !nextBtn || nextBtn.classList.contains('hide-btn');
    const isDisabled = nextBtn?.disabled || nextBtn?.getAttribute('aria-disabled') === 'true';
    if (nextBtn && !isHidden && !isDisabled) {
      nextBtn.click();
      return true;
    }
    return false;
  },

  async _clickNextOrderPage() {
    const currentPage = this._getCurrentPage();
    const targetPage = currentPage + 1;
    Utils.updateProgressModal({ debug: `페이지 이동 시작 - ${this._ordersDiag({ targetPage })}` });
    if (this._clickPageNumber(targetPage)) {
      Utils.updateProgressModal({ debug: `페이지 번호 직접 클릭 - ${this._ordersDiag({ targetPage })}` });
      return { moved: true, stalled: false };
    }

    const nextBtn = document.querySelector('.merchant-pagination button[data-at="next-btn"], button[data-at="next-btn"]')
      || Array.from(document.querySelectorAll('.merchant-pagination button')).find(btn => {
        const label = `${btn.getAttribute('aria-label') || ''} ${btn.getAttribute('title') || ''} ${btn.textContent || ''}`.trim();
        return /next|다음|›|>|»/i.test(label);
      });
    const isHidden = !nextBtn || nextBtn.classList.contains('hide-btn');
    const isDisabled = nextBtn?.disabled || nextBtn?.getAttribute('aria-disabled') === 'true';
    if (!nextBtn || isHidden || isDisabled) {
      Utils.updateProgressModal({ debug: `다음 페이지 버튼 사용 불가 - ${this._ordersDiag({ targetPage, hasNextBtn: !!nextBtn, hidden: !!isHidden, disabled: !!isDisabled })}` });
      return { moved: false, stalled: false };
    }

    nextBtn.click();
    Utils.updateProgressModal({ debug: `${targetPage}페이지 번호창 이동 중 - ${this._ordersDiag({ targetPage })}` });
    for (let i = 0; i < 15 && !this._stopFlag; i++) {
      await this._randomDelay(180, 300);
      if (this._getCurrentPage() === targetPage) {
        Utils.updateProgressModal({ debug: `페이지 이동 확인 - ${this._ordersDiag({ targetPage, poll: i + 1 })}` });
        return { moved: true, stalled: false };
      }
      if (this._clickPageNumber(targetPage)) {
        Utils.updateProgressModal({ debug: `번호창에서 목표 페이지 클릭 - ${this._ordersDiag({ targetPage, poll: i + 1 })}` });
        return { moved: true, stalled: false };
      }
    }
    const moved = this._getCurrentPage() === targetPage;
    Utils.updateProgressModal({ debug: `페이지 이동 결과 - ${this._ordersDiag({ targetPage, moved })}` });
    return { moved, stalled: !moved };
  },

  // 현재 페이지네이션 창에 보이는 숫자 버튼 목록 (first/prev/next/last 컨트롤 제외)
  _getVisiblePageNumbers() {
    const btns = document.querySelectorAll('.merchant-pagination ul li button');
    const out = [];
    for (const b of btns) {
      const dataAt = b.getAttribute('data-at') || '';
      if (dataAt && !/^\d+$/.test((b.textContent || '').trim())) continue;
      const n = parseInt((b.textContent || '').trim(), 10);
      if (!isNaN(n)) out.push({ num: n, btn: b });
    }
    return out;
  },

  // 보이는 창 안의 특정 번호 버튼 클릭
  _clickPageNumber(n) {
    const hit = this._getVisiblePageNumbers().find(x => x.num === n);
    if (hit) { hit.btn.click(); return true; }
    return false;
  },

  _parseSummaryNumber(text) {
    const num = parseInt(String(text || '').replace(/[^\d-]/g, ''), 10);
    return isNaN(num) ? 0 : num;
  },

  _getSalesSummaryValue(labelText, unitText) {
    const rows = document.querySelectorAll('.sales-summary .summary-row');
    for (const row of rows) {
      const label = row.textContent || '';
      if (!label.includes(labelText)) continue;

      const unitEls = row.querySelectorAll('.h1-txt .order-unit-price');
      for (const unitEl of unitEls) {
        if (unitEl.textContent.trim() !== unitText) continue;
        const valueEl = unitEl.previousElementSibling;
        const value = this._parseSummaryNumber(valueEl?.textContent);
        if (value > 0 || unitText === '원') return value;
      }

      const valueEl = row.querySelector('.h1-txt span:first-child');
      const value = this._parseSummaryNumber(valueEl?.textContent);
      if (value > 0 || unitText === '원') return value;
    }
    return 0;
  },

  _getExpectedOrderCount() {
    const summaryCount = this._getSalesSummaryValue('주문 수', '건');
    if (summaryCount > 0) return summaryCount;

    // 단위가 "건"인 .order-unit-price만 찾기 (매출액의 "원"은 제외)
    const allUnitEls = document.querySelectorAll('.h1-txt .order-unit-price');
    for (const unitEl of allUnitEls) {
      if (unitEl.textContent.trim() === '건') {
        const countEl = unitEl.previousElementSibling;
        // 콤마 제거 후 파싱 (예: "1,234" → 1234)
        const num = this._parseSummaryNumber(countEl?.textContent);
        if (!isNaN(num) && num > 0) return num;
      }
    }
    return 0;
  },

  _markExistingNoticesStale() {
    // SPA 전환 중 직전 매장의 0건 공지를 새 조회 결과로 오인하지 않게 한다.
    for (const el of document.querySelectorAll('.order-search-notice')) {
      el.setAttribute('data-collector-stale', '1');
    }
  },

  _hasNoOrdersNotice() {
    return [...document.querySelectorAll('.order-search-notice:not([data-collector-stale])')]
      .some(el => (el.textContent || '').includes('조회할 내역이 없습니다'));
  },

  _isNormalZeroOrders({ requireNotice = false } = {}) {
    const hasNotice = this._hasNoOrdersNotice();
    if (requireNotice && !hasNotice) return false;
    if (hasNotice && !this._findOrdersThrottleSignal().matched) {
      this._lastOrdersTransientErrorAt = 0;
      this._ordersThrottleCount = 0;
      return true;
    }
    return this._getExpectedOrderCount() === 0
      && this._getExpectedSalesTotal() === 0
      && (hasNotice || !requireNotice)
      && !this._isOrdersThrottleVisible()
      && !this._hadRecentOrdersTransientError();
  },

  _showMismatchDialog(collected, expected) {
    return new Promise((resolve) => {
      const existingModal = document.getElementById('__collector_mismatch_modal');
      if (existingModal) existingModal.remove();

      const overlay = document.createElement('div');
      overlay.id = '__collector_mismatch_modal';
      overlay.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: rgba(0, 0, 0, 0.6);
        z-index: 2147483647;
        display: flex;
        align-items: center;
        justify-content: center;
      `;

      const modal = document.createElement('div');
      modal.style.cssText = `
        background: #fff;
        border-radius: 16px;
        padding: 32px;
        min-width: 400px;
        max-width: 500px;
        box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
      `;

      modal.innerHTML = `
        <div style="text-align: center;">
          <div style="font-size: 64px; margin-bottom: 16px;">⚠️</div>
          <h2 style="margin: 0 0 8px 0; font-size: 22px; font-weight: 600; color: #1a1a1a;">수집 건수 확인해주세요</h2>
          <div style="background: #fef7e0; padding: 20px; border-radius: 12px; margin: 20px 0; text-align: left;">
            <div style="font-size: 15px; line-height: 1.8; color: #333;">
              📊 페이지 주문 수: <strong>${expected}건</strong><br>
              📦 수집 건수: <strong>${collected}건</strong><br>
              <br>
              일부 주문이 누락되었을 수 있습니다.<br>
              <strong>페이지 상단의 주문 수와 수집 건수가<br>일치하는지 확인 후 다시 조회해 주세요.</strong>
            </div>
          </div>
          <button id="mismatch_close_btn" style="
            background: #4a90d9;
            color: #fff;
            border: none;
            border-radius: 8px;
            padding: 12px 32px;
            font-size: 15px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
          ">확인</button>
        </div>
      `;

      overlay.appendChild(modal);
      document.body.appendChild(overlay);

      overlay.querySelector('#mismatch_close_btn').addEventListener('click', () => {
        overlay.remove();
        resolve('close');
      });
    });
  },

  _getFirstOrderId() {
    const firstItem = document.querySelector('.order-search-result-content > li.col-12');
    if (!firstItem) return '';
    return this._getOrderItemId(firstItem);
  },

  _getOrderItemId(item) {
    const orderSection = item?.querySelector('.order-item');
    const cols = orderSection?.querySelectorAll('[class*="col-4"]');
    return cols?.[0]?.childNodes[0]?.textContent.trim() || '';
  },

  _ordersDiag(extra = {}) {
    const visibleOrderCount = document.querySelectorAll('.order-search-result-content > li.col-12').length;
    const visiblePages = this._getVisiblePageNumbers().map(x => x.num).join(',');
    const parts = [
      `page=${this._getCurrentPage()}`,
      `cards=${visibleOrderCount}`,
      `first=${this._getFirstOrderId() || '-'}`,
      `visiblePages=${visiblePages || '-'}`
    ];
    for (const [key, value] of Object.entries(extra)) {
      if (value !== undefined && value !== null && value !== '') parts.push(`${key}=${value}`);
    }
    return parts.join(' | ');
  },

  async _waitForPageLoad(prevFirstOrderId, timeout = 10000) {
    const startTime = Date.now();
    let consecutiveChecks = 0;
    const REQUIRED_CHECKS = 3; // 3번 연속 확인
    
    while (Date.now() - startTime < timeout) {
      if (this._stopFlag) return false;
      
      // 1. 에러 팝업 체크 및 닫기
      await this._closeErrorPopups();
      
      // 2. 주문 목록 존재 확인
      const orderList = document.querySelector('.order-search-result-content');
      if (!orderList) {
        consecutiveChecks = 0;
        await new Promise(r => setTimeout(r, 300));
        continue;
      }
      
      // 3. 첫 번째 주문 ID 확인
      const currentOrderId = this._getFirstOrderId();
      if (!currentOrderId) {
        consecutiveChecks = 0;
        await new Promise(r => setTimeout(r, 300));
        continue;
      }
      
      // 4. ID가 변경되었는지 확인
      if (currentOrderId !== prevFirstOrderId) {
        consecutiveChecks++;
        
        // 5. 연속 3번 동일한 ID 확인 (안정화)
        if (consecutiveChecks >= REQUIRED_CHECKS) {
          console.log(`[쿠팡이츠] 페이지 로드 확인: ${currentOrderId} (${consecutiveChecks}번 확인)`);
          return true;
        }
        
        await new Promise(r => setTimeout(r, 200));
      } else {
        consecutiveChecks = 0;
        await new Promise(r => setTimeout(r, 300));
      }
    }
    
    return false;
  },

  // 에러 팝업 닫기 함수 추가
  async _closeErrorPopups() {
    // 모달 팝업 찾기
    const modalSelectors = this.ERROR_CONTAINER_SELECTORS;
    
    for (const selector of modalSelectors) {
      const modals = document.querySelectorAll(selector);
      
      for (const modal of modals) {
        const modalText = modal.textContent || '';
        
        // "주문을 찾을 수 없습니다" 등의 에러 메시지 확인
        if (modalText.includes('찾을 수 없') || 
            modalText.includes('오류') || 
            modalText.includes('실패')) {
          
          Utils.updateProgressModal({ 
            debug: `⚠️ 에러 팝업 감지: "${modalText.substring(0, 30)}..."` 
          });
          
          // 닫기 버튼 찾기
          const closeBtn = modal.querySelector(
            'button.close, ' +
            'button[aria-label="Close"], ' +
            '.ant-modal-close, ' +
            'button:has(.icon-close)'
          );
          
          if (closeBtn) {
            closeBtn.click();
            Utils.updateProgressModal({ debug: '✅ 에러 팝업 닫음' });
            await this._randomDelay(500, 800);
          }
          
          // 확인 버튼으로도 시도
          const okBtn = modal.querySelector(
            'button.btn-primary, ' +
            '.ant-btn-primary, ' +
            'button:contains("확인")'
          );
          
          if (okBtn) {
            okBtn.click();
            Utils.updateProgressModal({ debug: '✅ 에러 팝업 확인' });
            await this._randomDelay(500, 800);
          }
        }
      }
    }
  },

  async _waitForStableState(maxWait = 5000) {
    const startTime = Date.now();
    let lastCount = 0;
    let stableCount = 0;
    
    while (Date.now() - startTime < maxWait) {
      await this._closeErrorPopups();
      
      const currentItems = document.querySelectorAll('.order-search-result-content > li.col-12');
      const currentCount = currentItems.length;
      
      if (currentCount === lastCount && currentCount > 0) {
        stableCount++;
        if (stableCount >= 3) {
          // 3번 연속 동일한 개수 = 안정화됨
          Utils.updateProgressModal({ debug: `✅ 페이지 안정화 (주문 ${currentCount}개)` });
          return true;
        }
      } else {
        stableCount = 0;
        lastCount = currentCount;
      }
      
      await this._randomDelay(300, 500);
    }
    
    Utils.updateProgressModal({ debug: `⚠️ 안정화 타임아웃 (주문 ${lastCount}개)` });
    return false;
  },

  async _parseOrderItem(item, shopInfo, iso) {
    const rows = [];
    let order_id = '';
    let isCancelled = false;

    const orderSection = item.querySelector('.order-item');
    if (!orderSection) {
      return { rows, order_id, isCancelled, captured: false };
    }

    const cols = orderSection.querySelectorAll('[class*="col-4"]');
    order_id = cols[0]?.childNodes[0]?.textContent.trim() || '';

    let detailLoaded = false;
    for (let attempt = 0; attempt < 3 && !detailLoaded && !this._stopFlag; attempt++) {
      const icon = item.querySelector('.order-expand-btn .icon-ce-arrowdown-bold');
      if (icon) {
        const btn = icon.closest('button');
        await this._humanizeManualOrdersBeforeClick(btn);
        btn?.click();
        if (this._isSafeOrdersPacingEnabled()) {
          await this._randomDelay(530, 1355);
        } else {
          await this._randomDelay(400, 600);
        }
      }

      for (let r = 0; r < 12 && !this._stopFlag; r++) {
        await this._closeErrorPopups();
        const ds = item.querySelector('.order-details');
        isCancelled = !!ds?.querySelector('.total-cancelled-order');
        if (isCancelled || (ds && ds.querySelector('.order-detail-item'))) {
          detailLoaded = true;
          break;
        }
        if (this._isSafeOrdersPacingEnabled()) {
          await this._randomDelay(105, 305);
        } else {
          await this._randomDelay(200, 300);
        }
      }
    }

    const detailSection = item.querySelector('.order-details');
    isCancelled = !!detailSection?.querySelector('.total-cancelled-order');

    const dateEl = orderSection.querySelector('.order-date:not(.d-md-none) span:first-child');
    const timeEl = orderSection.querySelector('.order-date .gray-txt');
    const order_date = (dateEl?.textContent.trim().replace(/\s+/g, '') || '') + ' ' + (timeEl?.textContent.trim() || '');

    const deliveryType = orderSection.querySelector('.delivery-type')?.textContent.trim() || '';
    const priceEl = orderSection.querySelector('.order-price > span:first-child');
    const statusEl = orderSection.querySelector('.label-order-item');
    const total_price = Utils.cleanPrice(priceEl?.textContent || '');
    const order_status = statusEl?.textContent.trim() || '';

    const settlement = {
      매출액: '', 상점부담_쿠폰: '', 중개_이용료: '', 결제대행사_수수료: '',
      배달비: '', 광고비: '', 부가세: '', 즉시할인금액: '', 정산_예정_금액: '', 취소금액: ''
    };

    const summaryRows = detailSection?.querySelectorAll('.order-price-summary li.row') || [];
    for (const row of summaryRows) {
      const labelEl = row.querySelector('.col-8');
      const valueEl = row.querySelector('.col-4');
      if (!labelEl || !valueEl) continue;

      const label = labelEl.textContent.trim();
      const cleanVal = Utils.cleanPrice(valueEl.textContent);

      if (label.includes('매출액')) settlement.매출액 = cleanVal;
      else if (label.includes('상점부담 쿠폰')) settlement.상점부담_쿠폰 = cleanVal;
      else if (label.includes('중개 이용료')) settlement.중개_이용료 = cleanVal;
      else if (label.includes('결제대행사 수수료')) settlement.결제대행사_수수료 = cleanVal;
      else if (label.includes('배달비')) settlement.배달비 = cleanVal;
      else if (label.includes('광고비')) settlement.광고비 = cleanVal;
      else if (label.includes('부가세')) settlement.부가세 = cleanVal;
      else if (label.includes('즉시할인')) settlement.즉시할인금액 = cleanVal;
      else if (label.includes('정산 예정 금액') || label.includes('정산예정') || label.includes('정산금액')) settlement.정산_예정_금액 = cleanVal;
      else if (label.includes('취소금액')) settlement.취소금액 = cleanVal;
    }

    if (isCancelled && settlement.매출액 === '') {
      settlement.매출액 = '0';
    }

    const captured = isCancelled || settlement.매출액 !== '';
    if (!captured) {
      return { rows, order_id, isCancelled, captured: false };
    }

    const menuItems = detailSection?.querySelectorAll('.order-detail-item') || [];
    const allMenuData = [];

    for (const menuItem of menuItems) {
      const menuNameEl = menuItem.querySelector('.col-7');
      let menuName = '';
      if (menuNameEl) {
        for (const node of menuNameEl.childNodes) {
          if (node.nodeType === Node.TEXT_NODE) {
            const t = node.textContent.trim();
            if (t) { menuName = t; break; }
          }
        }
      }

      const qtyEl = menuItem.querySelector('.col-2');
      const menuQty = qtyEl?.textContent.trim().replace(/[^\d]/g, '') || '1';
      const menuPriceEl = menuItem.querySelector('.order-item-price');
      const menuPrice = Utils.cleanPrice(menuPriceEl?.textContent || '');

      const options = [];
      const optionEls = menuItem.querySelectorAll('.order-detail-option-name');
      for (const el of optionEls) {
        const optName = el.textContent.trim();
        if (optName) options.push(optName);
      }

      if (options.length === 0) {
        options.push('기본');
      }

      if (menuName) {
        allMenuData.push({ menuName, menuQty, menuPrice, options });
      }
    }

    const orderSummary = allMenuData.length > 0
      ? allMenuData[0].menuName + (allMenuData.length > 1 ? ` 외 ${allMenuData.length - 1}건` : '')
      : '';

    const base = {
      collected_at: iso,
      store_id: shopInfo.store_id,
      store_name: shopInfo.store_name,
      order_date,
      order_id,
      delivery_type: deliveryType,
      order_status,
      order_summary: orderSummary,
      total_price,
      is_cancelled: isCancelled ? 'Y' : 'N',
      ...settlement
    };

    let isFirstRow = true;

    for (const menu of allMenuData) {
      let isFirstOptionOfMenu = true;

      for (let j = 0; j < menu.options.length; j++) {
        const opt = menu.options[j];

        rows.push({
          collected_at: base.collected_at,
          store_id: base.store_id,
          store_name: base.store_name,
          order_date: base.order_date,
          order_id: base.order_id,
          delivery_type: base.delivery_type,
          order_status: base.order_status,
          order_summary: base.order_summary,
          total_price: base.total_price,
          is_cancelled: base.is_cancelled,
          menu_name: menu.menuName,
          menu_qty: menu.menuQty,
          menu_price: isFirstOptionOfMenu ? menu.menuPrice : '',
          menu_options: opt,
          매출액: isFirstRow ? base.매출액 : '',
          상점부담_쿠폰: isFirstRow ? base.상점부담_쿠폰 : '',
          중개_이용료: isFirstRow ? base.중개_이용료 : '',
          결제대행사_수수료: isFirstRow ? base.결제대행사_수수료 : '',
          배달비: isFirstRow ? base.배달비 : '',
          광고비: isFirstRow ? base.광고비 : '',
          부가세: isFirstRow ? base.부가세 : '',
          즉시할인금액: isFirstRow ? base.즉시할인금액 : '',
          정산_예정_금액: isFirstRow ? base.정산_예정_금액 : '',
          취소금액: isFirstRow ? base.취소금액 : ''
        });
        isFirstRow = false;
        isFirstOptionOfMenu = false;
      }
    }

    if (allMenuData.length === 0) {
      rows.push({
        ...base,
        menu_name: '',
        menu_qty: '',
        menu_price: '',
        menu_options: ''
      });
    }

    return { rows, order_id, isCancelled, captured };
  },

  async _collectCurrentPageData(shopInfo, iso, currentPage, storeDeadlineMs = 0, collectedOrderIds = new Set(), budgetState = null) {
    let stable = await this._waitForStableState(6000);
    if (!stable && !this._stopFlag) {
      await this._closeErrorPopups();
      await this._randomDelay(500, 800);
      stable = await this._waitForStableState(4000);
    }
    const stableTimedOut = !stable && !this._stopFlag;
    const isStoreDeadlineExceeded = () => {
      const deadlineMs = budgetState?.deadlineMs || storeDeadlineMs;
      return deadlineMs > 0 && Date.now() > deadlineMs;
    };

    let orderItems = Array.from(document.querySelectorAll('.order-search-result-content > li.col-12'));

    if (orderItems.length === 0) {
      if (this._isNormalZeroOrders({ requireNotice: true })) {
        Utils.updateProgressModal({ debug: `${currentPage}페이지 0건 확인 - 재시도 없이 빈 페이지 처리` });
        return { rows: [], pageComplete: true, missingOrderIds: [], stableTimedOut: false, skippedCount: 0 };
      }
      Utils.updateProgressModal({ debug: `${currentPage}페이지 주문 없음 - 재시도` });
      await this._randomDelay(500, 800);
      await this._closeErrorPopups();
      orderItems = Array.from(document.querySelectorAll('.order-search-result-content > li.col-12'));
      if (orderItems.length === 0) {
        if (this._isNormalZeroOrders({ requireNotice: true })) {
          Utils.updateProgressModal({ debug: `${currentPage}페이지 0건 재확인 - 정상 빈 결과` });
          return { rows: [], pageComplete: true, missingOrderIds: [], stableTimedOut: false, skippedCount: 0 };
        }
        Utils.updateProgressModal({ debug: `${currentPage}페이지 재시도 실패 - 빈 페이지로 반환` });
        return { rows: [], pageComplete: true, missingOrderIds: [], stableTimedOut, skippedCount: 0 };
      }
      Utils.updateProgressModal({ debug: `재시도 성공 - ${orderItems.length}개 주문 발견` });
    }

    const orderMap = new Map();
    let missing = [];
    let capturedCount = 0;
    let skippedCount = 0;
    let consecutiveFail = 0;
    let detailBlocked = false;
    let storeDeadlineExceeded = false;
    const totalItems = orderItems.length;
    const shouldBailForSilentThrottle = () => this._getExpectedOrderCount() > 0
      && ((consecutiveFail >= 5 && capturedCount === 0)
        || (consecutiveFail >= 4 && (this._isOrdersThrottleVisible() || this._hadRecentOrdersTransientError())));

    for (let i = 0; i < totalItems; i++) {
      if (this._stopFlag) break;
      if (isStoreDeadlineExceeded()) {
        const progressIdCount = collectedOrderIds.size + capturedCount;
        if (budgetState
            && budgetState.lastExtendedPage !== currentPage
            && this._tryExtendStoreBudget({
              extensions: budgetState.extensions,
              idCount: progressIdCount,
              lastExtensionIdCount: budgetState.lastExtensionIdCount
            })) {
          budgetState.extensions++;
          budgetState.deadlineMs = Date.now() + this.ORDERS_STORE_BUDGET_EXTENSION_MS;
          budgetState.lastExtensionIdCount = progressIdCount;
          budgetState.lastExtendedPage = currentPage;
          Utils.updateProgressModal({
            debug: `⏳ 매장 예산 연장 (${budgetState.extensions}/${this.ORDERS_STORE_BUDGET_MAX_EXTENSIONS}) — ${currentPage}페이지 ${i + 1}/${totalItems}부터 계속 수집 | ${this._ordersDiag({ captured: capturedCount, skipped: skippedCount, ids: progressIdCount })}`
          });
          continue;
        }
        storeDeadlineExceeded = true;
        Utils.updateProgressModal({
          debug: `⏱ 매장 예산 도달 — 연장 판단 대기 | ${this._ordersDiag({ item: `${i + 1}/${totalItems}`, captured: capturedCount, skipped: skippedCount, ids: collectedOrderIds.size })}`
        });
        break;
      }

      const knownOrderId = this._getOrderItemId(orderItems[i]);
      if (knownOrderId && collectedOrderIds?.has(knownOrderId)) {
        skippedCount++;
        Utils.updateProgressModal({
          message: `${currentPage}페이지 수집 중... (${i + 1}/${totalItems}건)`,
          debug: `이미 수집한 주문 스킵: ${knownOrderId}`
        });
        continue;
      }

      Utils.updateProgressModal({
        message: `${currentPage}페이지 수집 중... (${i + 1}/${totalItems}건)`
      });

      try {
        const res = await this._parseOrderItem(orderItems[i], shopInfo, iso);
        await this._manualOrdersPace(265, 795);
        await this._humanizeManualOrdersAfterItem(i);
        const key = res.order_id || `__idx_${i}`;
        if (res.captured) {
          orderMap.set(key, res);
          capturedCount++;
          consecutiveFail = 0;
        } else {
          consecutiveFail++;
          missing.push({ item: orderItems[i], order_id: res.order_id, key });
        }
        if (shouldBailForSilentThrottle()) {
          detailBlocked = true;
          this._markOrdersTransientError();
          Utils.updateProgressModal({ debug: `🚫 ${currentPage}페이지 상세패널 연속 실패 ${consecutiveFail}건 — silent 차단 판정, 조기 종료(백필)` });
          break;
        }
      } catch (err) {
        Utils.updateProgressModal({ debug: `❌ 주문 ${i + 1} 수집 오류: ${err.message}` });
        console.error(`[쿠팡이츠] 주문 ${i + 1} 수집 오류:`, err);
        await this._closeErrorPopups();
        await this._manualOrdersPace(470, 1060);
        consecutiveFail++;
        missing.push({ item: orderItems[i], order_id: '', key: `__idx_${i}` });
        if (shouldBailForSilentThrottle()) {
          detailBlocked = true;
          this._markOrdersTransientError();
          Utils.updateProgressModal({ debug: `🚫 ${currentPage}페이지 상세패널 연속 실패 ${consecutiveFail}건 — silent 차단 판정, 조기 종료(백필)` });
          break;
        }
      }
    }

    const skipRecollection = missing.length > 0
      && (detailBlocked || storeDeadlineExceeded || this._isOrdersThrottleVisible() || this._hadRecentOrdersTransientError());
    if (skipRecollection) {
      if (!detailBlocked && !storeDeadlineExceeded) this._markOrdersTransientError();
      Utils.updateProgressModal({ debug: `⚠️ ${currentPage}페이지 세부 재수집 스킵 — throttle/예산 감지, 백필로 이관` });
    }
    if (detailBlocked) missing = [];

    for (let round = 0; round < 3 && missing.length && !skipRecollection && !this._stopFlag; round++) {
      Utils.updateProgressModal({ debug: `⚠️ ${currentPage}페이지 매출액 미수집 ${missing.length}건 - 세부 재수집 ${round + 1}/3` });
      const stillMissing = [];
      for (const it of missing) {
        await this._closeErrorPopups();
        try {
          const res = await this._parseOrderItem(it.item, shopInfo, iso);
          await this._manualOrdersPace(470, 1060);
          const key = res.order_id || it.key;
          if (res.captured) {
            orderMap.set(key, res);
          } else {
            stillMissing.push({ ...it, order_id: res.order_id || it.order_id });
          }
        } catch (err) {
          Utils.updateProgressModal({ debug: `❌ 누락 주문 재수집 오류: ${err.message}` });
          console.error('[쿠팡이츠] 누락 주문 재수집 오류:', err);
          stillMissing.push(it);
        }
      }
      missing = stillMissing;
      await this._randomDelay(175, 295);
    }

    const pageRows = [];
    for (const res of orderMap.values()) {
      pageRows.push(...res.rows);
    }

    const missingOrderIds = missing.map((it, idx) => it.order_id || `unknown-${idx + 1}`);
    const pageComplete = missingOrderIds.length === 0;
    if (!pageComplete) {
      Utils.updateProgressModal({ debug: `⚠️ ${currentPage}페이지 매출액 미정처리 ${missingOrderIds.length}건` });
    }

    return { rows: pageRows, pageComplete: pageComplete && !detailBlocked && !storeDeadlineExceeded, missingOrderIds, stableTimedOut, detailBlocked, storeDeadlineExceeded, skippedCount };
  },

  // targetPage 까지 "끝번호 클릭 점프 + next-btn 창 이동" 방식으로 빠르게 복귀.
  // 보이는 5칸 창에 목표가 있으면 그 번호 직접 클릭(정확 착지),
  // 없으면 끝번호 클릭으로 점프 → 창 끝이면 next-btn 으로 창을 옆으로 이동.
  // doReSearch=false 면 재조회 없이 현재 위치에서 이동만(F5 재개 후 이미 조회된 상태).
  async _recoverToPage(targetPage, doReSearch = true) {
    Utils.updateProgressModal({ debug: `↩ ${targetPage}페이지로 복구 이동${doReSearch ? ' (재조회)' : ''}` });
    if (doReSearch) {
      if (!await this._clickOrdersSearchButton()) return false;
      await this._waitForOrdersDataReady(8000);
      await this._waitForStableState(5000);
    }

    let guard = 0;
    let cur = this._getCurrentPage();
    while (cur < targetPage && !this._stopFlag) {
      if (guard++ > 60) return false;
      const nums = this._getVisiblePageNumbers();
      if (nums.length === 0) return false;
      const prevId = this._getFirstOrderId();

      if (nums.some(x => x.num === targetPage)) {
        this._clickPageNumber(targetPage);          // 목표가 창에 보임 → 직접 클릭
      } else {
        const maxVisible = Math.max(...nums.map(x => x.num));
        if (cur < maxVisible) {
          this._clickPageNumber(maxVisible);        // 끝번호로 점프
        } else if (!this._clickNextPage()) {        // 창 끝 → next-btn 으로 창 이동
          return false;
        }
      }

      let loaded = await this._waitForPageLoad(prevId, 15000);
      if (!loaded) {
        await this._randomDelay(1500, 2500);
        await this._closeErrorPopups();
        loaded = await this._waitForPageLoad(prevId, 10000);
        if (!loaded) return false;
      }
      cur = this._getCurrentPage();
      Utils.updateProgressModal({ message: `복구 이동 중... ${cur}/${targetPage}페이지` });
    }
    return this._getCurrentPage() === targetPage;
  },

  async _collectOrdersAllPages(shopInfo) {
    await this._collectOrdersWithValidation(shopInfo);
  },

  _ordersHardThrottleNote() {
    return '10057: 과도한 요청으로 인해 서비스 이용이 일시적으로 제한되었습니다.';
  },

  _makeOrdersBlockedResult(storeName) {
    return {
      storeName: storeName || '현재 매장',
      completed: false,
      blocked: true,
      hardThrottle: true,
      status: 'warn',
      note: this._ordersHardThrottleNote()
    };
  },

  async _pushOrdersBlockedCurrentStore(storeName, storeResults) {
    Utils.updateMultiStoreStoreResult('blocked', '⛔ 쿠팡 제한(10057) — 현재 매장 패스');
    storeResults.push(this._makeOrdersBlockedResult(storeName));
    this._hardThrottled = false;
    await this._waitAfterOrdersBlockedStore();
  },

  _appendRemainingOrdersBlocked(matchingStores, storeResults) {
    const done = new Set(storeResults.map(r => this._normalizeStoreName(r.storeName || '')));
    for (const storeName of matchingStores) {
      const key = this._normalizeStoreName(storeName || '');
      if (!done.has(key)) storeResults.push(this._makeOrdersBlockedResult(storeName));
    }
  },

  _makeOrdersDeferredResult(storeName, note = '멀티매장 수집 미완료 → 백필') {
    return {
      storeName: storeName || '현재 매장',
      completed: false,
      partialThrottle: true,
      status: 'warn',
      note
    };
  },

  _appendRemainingOrdersDeferred(matchingStores, storeResults, note) {
    const done = new Set(storeResults.map(r => this._normalizeStoreName(r.storeName || '')));
    for (const storeName of matchingStores) {
      const key = this._normalizeStoreName(storeName || '');
      if (!done.has(key)) storeResults.push(this._makeOrdersDeferredResult(storeName, note));
    }
  },

  _sendOrdersCompletePayload(payload) {
    if (window.top !== window.self) return;
    try {
      chrome.runtime.sendMessage({ type: 'MULTISTORE_COMPLETE', payload }).catch(() => {});
    } catch (_) {}
  },

  _sendOrdersHardThrottleComplete(storeNames) {
    const names = Array.isArray(storeNames) && storeNames.length ? storeNames : ['현재 매장'];
    const storeResults = names.map(name => this._makeOrdersBlockedResult(name));
    this._sendOrdersCompletePayload({
      category: 'orders',
      storeCount: storeResults.length,
      completedCount: 0,
      stopped: !!this._stopFlag,
      hardThrottle: true,
      blocked: true,
      storeResults
    });
  },

  async _collectOrdersMultiStore(opts = {}) {
    // 수동 수집: 드롭다운/체크포인트/날짜 개입 없이 현재 페이지 그대로 수집
    if (!this._isDashboardBatchOrders(opts)) {
      const shopInfo = this._getShopInfo();
      const curStoreName = this._findCurStoreName() || shopInfo.store_name || '';
      Utils.showProgressModal('쿠팡이츠 수집', '현재 날짜 그대로 수집 중...');
      if (!this._isTargetBrandStoreName(curStoreName)) {
        Utils.updateProgressModal({ debug: '⏭️ 수집 대상 브랜드 아님 (' + (curStoreName || '매장명 미탐지') + ') → 건너뜀' });
        if (window.top === window.self) {
          try { chrome.runtime.sendMessage({ type: 'MULTISTORE_COMPLETE',
            payload: { category: 'orders', storeCount: 0, completedCount: 0, stopped: false, skipped: true, reason: 'not_target_brand', curStoreName }
          }).catch(() => {}); } catch (_) {}
        }
        return;
      }
      Utils.updateProgressModal({ debug: '📅 수동 수집: 날짜·드롭다운 설정 무시' });
      const prevCount = this._getExpectedOrderCount();
      await this._manualOrdersPace(680, 1275);
      await this._clickOrdersSearchButton();
      if (this._hardThrottled) {
        this._sendOrdersHardThrottleComplete([curStoreName || shopInfo.store_name || '현재 매장']);
        return;
      }
      await this._waitForOrdersCountChange(prevCount, 8000);
      await this._waitForOrdersDataReady(8000);
      await this._manualOrdersPace(1275, 2125);
      const collected = await this._collectOrdersWithValidation(shopInfo, false);
      if (window.top === window.self) {
        try {
          chrome.runtime.sendMessage({
            type: 'MULTISTORE_COMPLETE',
            payload: {
              category: 'orders',
              storeCount: 1,
              completedCount: (!this._stopFlag && collected) ? 1 : 0,
              stopped: !!this._stopFlag,
              error: !collected,
              storeResults: [{ storeName: shopInfo.store_name || '현재 매장', completed: !this._stopFlag && !!collected, status: this._stopFlag ? 'warn' : (collected ? 'ok' : 'fail'), note: collected ? '완료' : '수집 결과 없음' }]
            }
          }).catch(() => {});
        } catch (_) {}
      }
      return;
    }

    let matchingStores = await this._getOrdersDropdownStores();
    matchingStores = this._filterTargetStores(matchingStores, opts);

    if (matchingStores.length === 0) {
      const shopInfo = this._getShopInfo();
      // 현재 표시 매장이 도리당/나홀로 브랜드인지 확인 (다중 셀렉터)
      const curStoreName = this._findCurStoreName() || shopInfo.store_name || '';
      if (curStoreName && !this._isTargetRequested(curStoreName, opts)) {
        if (window.top === window.self) {
          try {
            chrome.runtime.sendMessage({
              type: 'MULTISTORE_COMPLETE',
              payload: { category: 'orders', storeCount: 0, completedCount: 0, stopped: false, storeResults: [] }
            }).catch(() => {});
          } catch (_) {}
        }
        return;
      }
      Utils.updateProgressModal({ debug: '🔍 매장명 탐색: "' + curStoreName + '"' });
      if (!this._isTargetBrandStoreName(curStoreName)) {
        Utils.updateProgressModal({ debug: '⏭️ 수집 대상 브랜드 아님 (' + (curStoreName || '매장명 미탐지') + ') → 건너뜀' });
        if (window.top === window.self) {
          try { chrome.runtime.sendMessage({ type: 'MULTISTORE_COMPLETE',
            payload: { category: 'orders', storeCount: 0, completedCount: 0, stopped: false, skipped: true, reason: 'not_target_brand', curStoreName: curStoreName }
          }).catch(() => {}); } catch (_) {}
        }
        return;
      }
      // 날짜 필터 설정 (수동 모드는 UI 날짜 그대로, 배치는 어제로 설정)
      let dateSet = true;
      let dataReady = true;
      if (!this._isDashboardBatchOrders(opts)) {
        Utils.showProgressModal('쿠팡이츠 수집', '현재 날짜 그대로 수집 중...');
        Utils.updateProgressModal({ debug: '📅 수동 수집: 현재 날짜 그대로 사용' });
        const prevCount = this._getExpectedOrderCount();
        await this._clickOrdersSearchButton();
        if (this._hardThrottled) {
          this._sendOrdersHardThrottleComplete([curStoreName || shopInfo.store_name || '현재 매장']);
          return;
        }
        await this._waitForOrdersCountChange(prevCount, 8000);
        dataReady = await this._waitForOrdersDataReady(8000);
        if (dataReady) await this._waitForStableState(5000);
      } else {
        const targetDate = this._resolveBatchTargetDate(opts);
        Utils.showProgressModal('쿠팡이츠 수집', `날짜 설정 중 (${this._batchTargetDateLabel(opts)})...`);
        dateSet = await this._setOrdersDateToYesterday(targetDate);
        Utils.updateProgressModal({ debug: dateSet ? '✅ 날짜 설정 완료' : '⚠️ 날짜 설정 실패, 기존 날짜로 진행' });
        if (dateSet) {
          const prevCount = this._getExpectedOrderCount();
          await this._clickOrdersSearchButton();
          if (this._hardThrottled) {
            this._sendOrdersHardThrottleComplete([curStoreName || shopInfo.store_name || '현재 매장']);
            return;
          }
          await this._waitForOrdersCountChange(prevCount, 8000);
          dataReady = await this._waitForOrdersDataReady(8000);
          if (dataReady) await this._waitForStableState(5000);
        }
      }
      const zeroOrders = this._isNormalZeroOrders({ requireNotice: true });
      const collected = zeroOrders ? true : await this._collectOrdersWithValidation(shopInfo, false, { skipValidation: !dateSet });
      // MULTISTORE_COMPLETE 신호 (단일매장도 동일하게 전송 — runner 무한 대기 방지)
      if (window.top === window.self) {
        try {
          chrome.runtime.sendMessage({
            type: 'MULTISTORE_COMPLETE',
            payload: {
              category: 'orders',
              storeCount: 1,
              completedCount: (!this._stopFlag && collected) ? 1 : 0,
              stopped: !!this._stopFlag,
              error: !collected,
              zeroOrders,
              storeResults: [{ storeName: curStoreName || shopInfo.store_name || '현재 매장', completed: !this._stopFlag && !!collected, status: this._stopFlag ? 'warn' : (collected ? 'ok' : 'fail'), zeroOrders, note: zeroOrders ? '0건' : (collected ? '완료' : '수집 결과 없음') }]
            }
          }).catch(() => {});
        } catch (_) {}
      }
      return;
    }

    // 도리당 먼저, 나홀로 나중
    matchingStores.sort((a, b) => this._targetBrandRank(a) - this._targetBrandRank(b));

    // 실행 기준일별 완료 매장 체크포인트 로드
    const batchTargetDate = this._resolveBatchTargetDate(opts);
    const checkpointDate = this._formatDate(batchTargetDate).replace(/-/g, '');
    const checkpoint = await this._loadOrdersCheckpoint();
    const completedStores = (checkpoint?.targetDate === checkpointDate)
      ? (checkpoint.completedStores || []).filter(store => {
          const completedKey = this._normalizeStoreName(store);
          return matchingStores.some(name => this._normalizeStoreName(name) === completedKey);
        })
      : [];
    const storeResults = [];
    const WATCHDOG_MS = 270000;
    const watchdogDeadline = Date.now() + WATCHDOG_MS;
    const MAX_STORE_RELOAD = matchingStores.length + 2;
    let forcedPartialComplete = false;

    Utils.showMultiStoreProgressModal(matchingStores);

    if (completedStores.length > 0) {
      Utils.updateProgressModal({ debug: `📌 기준일 이미 완료된 매장: ${completedStores.join(', ')} → 건너뜀` });
    }

  for (let i = 0; i < matchingStores.length; i++) {
    if (this._stopFlag) break;
    if (this._hardThrottled) {
      Utils.updateProgressModal({ debug: '⛔ 이전 10057 상태 초기화 후 다음 매장 진행' });
      this._hardThrottled = false;
    }
    if (Date.now() > watchdogDeadline) {
      Utils.updateProgressModal({ debug: `전체 수집 watchdog 초과 — 조기 종료 (완료: ${completedStores.length}/${matchingStores.length})` });
      this._appendRemainingOrdersDeferred(matchingStores, storeResults, '전체 수집 watchdog 초과 → 백필');
      forcedPartialComplete = true;
      break;
    }

      Utils.setMultiStoreIndex(i);
      const storeName = matchingStores[i];

      // 기준일 이미 수집 완료된 매장 스킵
      if (completedStores.some(store => this._normalizeStoreName(store) === this._normalizeStoreName(storeName))) {
        Utils.updateProgressModal({ debug: `⏭️ 기준일 이미 수집 완료됨, 건너뜀` });
        Utils.updateMultiStoreStoreResult('success', '⏭️ 기준일 이미 수집됨');
        storeResults.push({ storeName, completed: true, status: 'ok', skipped: true, note: '기준일 이미 수집됨' });
        continue;
      }

      const ordersBeaconDateStr = checkpointDate;
      const ordersBeacon = opts.targetDateMode === 'today'
        ? null
        : await Utils.checkDownloadBeacon(storeName, ordersBeaconDateStr, 'orders');
      if (ordersBeacon) {
        Utils.updateProgressModal({ debug: `📁 orders beacon 확인됨(${ordersBeaconDateStr}), 건너뜀: ${storeName}` });
        Utils.updateMultiStoreStoreResult('success', '📁 이미 수집됨 (beacon)');
        if (!this._stopFlag && !completedStores.some(store => this._normalizeStoreName(store) === this._normalizeStoreName(storeName))) {
          completedStores.push(storeName);
          await this._saveOrdersCheckpoint({ date: Utils.getTodayStr(), targetDate: checkpointDate, completedStores: [...completedStores] });
        }
        storeResults.push({ storeName, completed: true, status: 'ok', skipped: true, note: 'beacon: 다운로드 파일 확인' });
        continue;
      }

      Utils.updateProgressModal({ debug: `→ 매장 선택 시도` });

      const hasPriorCompletedStore = completedStores.length > 0 || storeResults.some(r => r.completed || r.skipped);
      // 대상이 이미 현재 표시 매장이면 매장 전환(네비게이션)이 없음 → reload 재개 예약 불필요
      // (예약하면 진전 없이 reloadCount 만 소모되어 마지막 selected 매장이 백필 이월됨)
      const alreadyCurrent = this._isCurrentOrdersStore(storeName);
      if (matchingStores.length > 1 && hasPriorCompletedStore && !alreadyCurrent) {
        const reloadCount = await this._loadOrdersBatchReloadCount();
        if (reloadCount >= MAX_STORE_RELOAD) {
          Utils.updateProgressModal({ debug: `⚠️ 멀티매장 reload 재개 한도 초과 (${reloadCount}/${MAX_STORE_RELOAD}) — 남은 매장 백필 이월` });
          this._appendRemainingOrdersDeferred(matchingStores, storeResults, '매장전환 reload 재개 한도 초과 → 백필');
          forcedPartialComplete = true;
          break;
        }
        await this._saveOrdersBatchRestart(opts, matchingStores, reloadCount + 1);
        Utils.updateProgressModal({ debug: `💾 매장전환 reload 재개 예약 (${reloadCount + 1}/${MAX_STORE_RELOAD}): ${storeName}` });
      }

      const selected = await this._selectStoreInOrdersDropdown(storeName);
      if (matchingStores.length > 1) {
        await this._clearOrdersBatchRestartKeys();
      }
      if (!selected) {
        Utils.updateProgressModal({ debug: `❌ 매장 선택 실패` });
        Utils.updateMultiStoreStoreResult('error', '매장 선택 실패');
        storeResults.push({ storeName, completed: false, status: 'fail', error: true, note: '매장 선택 실패' });
        continue;
      }
      Utils.updateProgressModal({ debug: `✅ 매장 선택 완료` });

      // 날짜 필터 설정 (수동 모드는 UI 날짜 그대로, 배치는 어제로 설정)
      let dateSet = true;
      if (!this._isDashboardBatchOrders(opts)) {
        Utils.updateProgressModal({ debug: `📅 수동 수집: 현재 날짜 그대로 사용` });
      } else {
        const targetDate = this._resolveBatchTargetDate(opts);
        Utils.updateProgressModal({ debug: `📅 주문일 설정 중 (${this._batchTargetDateLabel(opts)}~${this._batchTargetDateLabel(opts)})` });
        dateSet = await this._setOrdersDateToYesterday(targetDate);
        Utils.updateProgressModal({ debug: dateSet ? `✅ 주문일 설정 완료` : `⚠️ 주문일 설정 실패, 기존 날짜로 진행` });
      }
      if (this._isSafeOrdersPacingEnabled()) {
        await this._randomDelay(1200, 3500);
      } else {
        await this._randomDelay(300, 500);
      }

      const prevCount = this._getExpectedOrderCount();
      Utils.updateProgressModal({ debug: `📊 prevCount = ${prevCount}건` });

      let searched = false;
      for (let r = 0; r < 1 && !searched; r++) {
        Utils.updateProgressModal({ debug: `🔍 조회 버튼 클릭 (${r + 1}차)` });
        if (r > 0) await this._randomDelay(1500, 4500);
        searched = await this._clickOrdersSearchButton();
        if (this._hardThrottled) break;
      }
      if (this._hardThrottled) {
        await this._pushOrdersBlockedCurrentStore(storeName, storeResults);
        continue;
      }
      if (!searched) {
        Utils.updateProgressModal({ debug: `❌ 조회 버튼 클릭 실패, 건너뜀` });
        Utils.updateMultiStoreStoreResult('error', '조회 버튼 클릭 실패');
        storeResults.push({ storeName, completed: false, status: 'fail', error: true, note: '조회 버튼 클릭 실패' });
        continue;
      }
      Utils.updateProgressModal({ debug: `✅ 조회 버튼 클릭 성공` });

      Utils.updateProgressModal({ debug: `⏳ 데이터 로드 대기 중 (${prevCount}건 → 변경 대기)` });
      await this._waitForOrdersCountChange(prevCount, 8000);
      const dataLoaded = await this._waitForOrdersDataReady(8000);
      const newCount = this._getExpectedOrderCount();
      Utils.updateProgressModal({ debug: `✅ 데이터 로드 완료 (${newCount}건)` });

      // 0건: 기준일 주문 없는 정상 상황 → 체크포인트만 저장하고 다음 매장으로
      if (this._isNormalZeroOrders({ requireNotice: true })) {
        Utils.updateProgressModal({ debug: `ℹ️ 0건 확인 (기준일 주문 없음) — 다음 매장으로` });
        Utils.updateMultiStoreStoreResult('success', '0건');
        if (!this._stopFlag) {
          completedStores.push(storeName);
          await this._saveOrdersCheckpoint({ date: Utils.getTodayStr(), targetDate: checkpointDate, completedStores: [...completedStores] });
        }
        storeResults.push({ storeName, completed: !this._stopFlag, status: this._stopFlag ? 'warn' : 'ok', zeroOrders: true, note: '0건' });
        continue;
      }

      if (this._checkOrdersHardThrottle()) {
        await this._pushOrdersBlockedCurrentStore(storeName, storeResults);
        continue;
      }

      const shopInfo = this._getShopInfo();
      const collected = await this._collectOrdersWithValidation(shopInfo, true, { skipValidation: !dateSet });
      if (this._hardThrottled) {
        await this._pushOrdersBlockedCurrentStore(storeName, storeResults);
        continue;
      }
      if (!collected) {
        const r = this._lastOrdersCollectResult || {};
        const throttled = !this._stopFlag && (
          r.partialThrottle || r.detailBlocked ||
          this._hadRecentOrdersTransientError() || this._isOrdersThrottleVisible()
        );
        if (throttled) {
          // Soft throttle is deferred to backfill to avoid same-day retry loops.
          Utils.updateProgressModal({ debug: `현재 매장 백필 이월 후 다음 매장 진행 - ${r.note || '수집 제한/경고'}` });
          Utils.updateMultiStoreStoreResult('blocked', '⚠️ 수집 제한/경고 — 백필 이월');
          storeResults.push({
            storeName, completed: false, blocked: true, partialThrottle: true,
            status: 'warn', note: r.note || '수집 제한/경고 → 백필'
          });
          this._hardThrottled = false;
          await this._waitAfterOrdersBlockedStore();
          continue;
        }
        Utils.updateMultiStoreStoreResult('error', '수집 결과 없음');
        storeResults.push({ storeName, completed: false, status: this._stopFlag ? 'warn' : 'fail', error: true, note: this._stopFlag ? '중단됨' : '수집 결과 없음' });
        continue;
      }

      // 매장 수집 완료 → 체크포인트 갱신
      if (!this._stopFlag) {
        completedStores.push(storeName);
        await this._saveOrdersCheckpoint({ date: Utils.getTodayStr(), targetDate: checkpointDate, completedStores: [...completedStores] });
        Utils.updateProgressModal({ debug: `💾 체크포인트 저장 (완료: ${completedStores.length}/${matchingStores.length}개)` });
      }
      storeResults.push({ storeName, completed: !this._stopFlag, status: this._stopFlag ? 'warn' : 'ok', note: this._stopFlag ? '중단됨' : '완료' });
    }

    const hasResultForEveryStore = matchingStores.every(storeName => {
      const key = this._normalizeStoreName(storeName || '');
      return storeResults.some(r => this._normalizeStoreName(r.storeName || '') === key);
    });
    const allStoresSettled = hasResultForEveryStore && storeResults.every(r => (
      r.completed || r.blocked || r.hardThrottle || r.partialThrottle || r.error || r.status === 'fail' || r.status === 'warn'
    ));
    const allStoresFinishedCleanly = hasResultForEveryStore && storeResults.every(r => (
      r.completed || r.blocked || r.hardThrottle
    ));

    if (!allStoresSettled && !this._stopFlag) {
      Utils.updateProgressModal({ debug: `⏸️ 멀티매장 중간 run 종료 — 완료신호 보류 (완료: ${completedStores.length}/${matchingStores.length})` });
      return;
    }

    // 모든 매장 완료 → 체크포인트 삭제
    if (!this._stopFlag && allStoresFinishedCleanly) {
      await this._clearOrdersCheckpoint();
      await this._clearOrdersBatchResumeState();
    } else if (forcedPartialComplete || this._stopFlag) {
      await this._clearOrdersBatchRestartKeys();
    }

    Utils.finalizeMultiStoreModal();

    // ── runner 대시보드에 완료 신호 ──
    this._sendOrdersCompletePayload({
      category: 'orders',
      storeCount: matchingStores.length,
      completedCount: storeResults.filter(r => r.completed).length,
      stopped: !!this._stopFlag,
      zeroOrders: storeResults.length > 0 && storeResults.every(r => r.zeroOrders),
      partialThrottle: storeResults.some(r => r.partialThrottle),
      storeResults
    });
  },

  _loadOrdersCheckpoint() {
    return new Promise((resolve) => {
      chrome.storage.local.get(['ce_orders_checkpoint'], (r) => {
        resolve(r.ce_orders_checkpoint || null);
      });
    });
  },

  _saveOrdersCheckpoint(data) {
    return new Promise((resolve) => {
      chrome.storage.local.set({ ce_orders_checkpoint: data }, resolve);
    });
  },

  _clearOrdersCheckpoint() {
    return new Promise((resolve) => {
      chrome.storage.local.remove(['ce_orders_checkpoint'], resolve);
    });
  },

  _loadOrdersBatchReloadCount() {
    return new Promise((resolve) => {
      chrome.storage.local.get(['ce_orders_batch_reload_count'], (r) => {
        resolve(Number(r.ce_orders_batch_reload_count || 0));
      });
    });
  },

  _saveOrdersBatchRestart(opts, matchingStores, reloadCount) {
    const today = Utils.getTodayStr();
    return new Promise((resolve) => {
      chrome.storage.local.set({
        ce_orders_restart: { date: today },
        ce_current_target_stores: Array.isArray(opts.targetStores) ? opts.targetStores : matchingStores,
        ce_current_target_date: opts.targetDate || '',
        ce_current_target_date_mode: opts.targetDateMode || 'yesterday',
        ce_orders_batch_reload_count: reloadCount
      }, resolve);
    });
  },

  _clearOrdersBatchRestartKeys() {
    return new Promise((resolve) => {
      chrome.storage.local.remove(['ce_orders_restart', 'ce_orders_batch_reload_count'], resolve);
    });
  },

  _clearOrdersBatchResumeState() {
    return new Promise((resolve) => {
      chrome.storage.local.remove([
        'ce_orders_restart',
        'ce_current_target_stores',
        'ce_current_target_date',
        'ce_current_target_date_mode',
        'ce_orders_batch_reload_count'
      ], resolve);
    });
  },

  async _getOrdersDropdownStores() {
    const toggleBtn = document.querySelector('.dropdown-btn.highlight');
    if (!toggleBtn) return [];

    toggleBtn.click();
    await this._randomDelay(300, 500);

    const items = [...document.querySelectorAll('.dropdown-list li a')]
      .map(el => el.textContent.trim())
      .filter(Boolean);

    // 드롭다운 닫기
    toggleBtn.click();
    await this._randomDelay(200, 300);

    return items;
  },

  // storeText 가 현재 표시(selected) 매장과 같으면 true — 재클릭/이동 불필요
  _isCurrentOrdersStore(storeText) {
    const curText = document.querySelector('.dropdown-btn.highlight')?.textContent?.trim();
    if (!curText) return false;
    const targetKey = this._normalizeStoreName(storeText);
    const curKey = this._normalizeStoreName(curText);
    if (!targetKey || !curKey) return false;
    return curKey === targetKey || curKey.includes(targetKey) || targetKey.includes(curKey);
  },

  async _selectStoreInOrdersDropdown(storeText) {
    const toggleBtn = document.querySelector('.dropdown-btn.highlight');
    if (!toggleBtn) return false;

    // 이미 현재 표시 중인 매장이면 드롭다운을 열지 않고 즉시 성공 처리
    // (같은 매장 재클릭 → 불필요한 페이지 이동/reload 재개 유발 방지)
    if (this._isCurrentOrdersStore(storeText)) {
      return true;
    }

    toggleBtn.click();

    // 즉시 체크 후 50ms 간격 폴링 (클라이언트 사이드 드롭다운은 거의 즉시 열림)
    let list = document.querySelector('.dropdown-list');
    if (!list || list.offsetParent === null) {
      for (let i = 0; i < 20; i++) {
        await new Promise(r => setTimeout(r, 50));
        list = document.querySelector('.dropdown-list');
        if (list && list.offsetParent !== null) break;
      }
    }

    const items = [...document.querySelectorAll('.dropdown-list li a')];
    const targetKey = this._normalizeStoreName(storeText);
    const target = items.find(el => {
      const t = el.textContent.trim();
      const key = this._normalizeStoreName(t);
      return key === targetKey || key.includes(targetKey) || targetKey.includes(key);
    });

    if (!target) {
      toggleBtn.click();
      return false;
    }

    target.click();

    // 드롭다운 닫힘 대기 (50ms 간격)
    for (let i = 0; i < 10; i++) {
      await new Promise(r => setTimeout(r, 50));
      const closedList = document.querySelector('.dropdown-list');
      if (!closedList || closedList.offsetParent === null) break;
    }

    return true;
  },

  async _clickOrdersSearchButton() {
    const canSearch = await this._waitBeforeOrdersSearch();
    if (canSearch === false || this._hardThrottled) return false;

    const selectors = [
      'button.button--defaultOutlined',
      'button.css-casqo8',
      'button.e4pgcj01'
    ];

    for (const sel of selectors) {
      const btns = document.querySelectorAll(sel);
      for (const btn of btns) {
        if (btn.querySelector('svg') && !btn.disabled && btn.offsetParent !== null) {
          this._markExistingNoticesStale();
          btn.click();
          this._lastOrdersSearchAt = Date.now();
          await this._randomDelay(300, 500);
          return true;
        }
      }
    }

    // 최후 폴백: 돋보기 SVG path로 찾기
    for (const btn of document.querySelectorAll('button')) {
      const path = btn.querySelector('svg path');
      if (path && path.getAttribute('d')?.includes('M3.11 3.608') && !btn.disabled && btn.offsetParent !== null) {
        this._markExistingNoticesStale();
        btn.click();
        this._lastOrdersSearchAt = Date.now();
        await this._randomDelay(300, 500);
        return true;
      }
    }

    return false;
  },

  async _setOrdersDateToYesterday(targetDateObj = null) {
    const yesterday = targetDateObj ? new Date(targetDateObj) : new Date();
    if (!targetDateObj) yesterday.setDate(yesterday.getDate() - 1);
    yesterday.setHours(0, 0, 0, 0);
    const ariaLabel = yesterday.toDateString();
    const visibleLabel = `${yesterday.getFullYear()}.${yesterday.getMonth() + 1}.${yesterday.getDate()}`;
    const targetValue = String(yesterday.getTime());

    const getVisibleDateInputs = () => [...document.querySelectorAll('div[data-testid="input"]')]
      .filter(el => el.offsetParent !== null && /\d{4}\.\d{1,2}\.\d{1,2}/.test(el.textContent || ''));

    const dispatchDateEvents = (el) => {
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
      el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
    };

    const forceSetHiddenRange = () => {
      const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
      for (const name of ['startDate', 'endDate']) {
        const hidden = document.querySelector(`input[name="${name}"]`);
        if (!hidden) continue;
        if (nativeSetter) nativeSetter.call(hidden, targetValue);
        else hidden.value = targetValue;
        hidden.setAttribute('value', targetValue);
        hidden.dispatchEvent(new Event('input', { bubbles: true }));
        hidden.dispatchEvent(new Event('change', { bubbles: true }));
      }

      const inputs = getVisibleDateInputs();
      if (inputs[0]) {
        inputs[0].textContent = visibleLabel;
        dispatchDateEvents(inputs[0]);
      }
      if (inputs[1]) {
        inputs[1].textContent = visibleLabel;
        dispatchDateEvents(inputs[1]);
      }
    };

    const pad2d = n => String(n).padStart(2, '0');
    const visibleLabelPadded = visibleLabel.replace(/\.(\d)$/, (_, d) => '.' + d.padStart(2, '0'));
    const verifyYesterdayRange = () => {
      const inputs = getVisibleDateInputs();
      const shown = inputs.slice(0, 2).map(el => (el.textContent || '').trim());
      const matchLabel = s => s.includes(visibleLabelPadded) || s.includes(visibleLabel);
      const ok = shown.length >= 2 && matchLabel(shown[0]) && matchLabel(shown[1]);
      if (!ok) {
        Utils.updateProgressModal({ debug: 'WARN order date mismatch: ' + (shown.join(' / ') || 'none') + ' (expected ' + visibleLabelPadded + ')' });
      }
      return ok;
    };

    // e4pgcj08 은 CSS 모듈 해시로 페이지 빌드마다 바뀔 수 있음 → 없어도 계속 진행
    const trigger = document.querySelector('div.e4pgcj08');
    if (trigger) {
      trigger.click();
      await this._randomDelay(400, 600);
    }

    const waitForDayPicker = async (ms = 3000) => {
      const start = Date.now();
      while (Date.now() - start < ms) {
        if (document.querySelector('div.DayPicker-Day')) return true;
        await new Promise(r => setTimeout(r, 100));
      }
      return false;
    };

    const waitForDayPickerClosed = async () => {
      const start = Date.now();
      while (Date.now() - start < 1500) {
        if (!document.querySelector('div.DayPicker-Day')) return true;
        await new Promise(r => setTimeout(r, 100));
      }
      return false;
    };

    const dayNum = String(yesterday.getDate());
    const findCellsByNum = () =>
      [...document.querySelectorAll('div.DayPicker-Day')]
        .filter(c => c.textContent.trim() === dayNum
                  && !c.classList.contains('DayPicker-Day--outside')
                  && c.getAttribute('aria-disabled') !== 'true');

    const clickLastYesterdayCell = async () => {
      // 방법 1: aria-label 영문 형식
      let cells = [...document.querySelectorAll('div.DayPicker-Day[aria-label="' + ariaLabel + '"]')]
        .filter(c => c.getAttribute('aria-disabled') !== 'true' && !c.classList.contains('DayPicker-Day--outside'));
      // 방법 2: 날짜 숫자로 탐색 (로케일 무관)
      if (cells.length === 0) cells = findCellsByNum();
      // 이전 달로 이동 후 재시도
      if (cells.length === 0) {
        const prevBtn = document.querySelector('.DayPicker-NavButton--prev');
        if (prevBtn) { prevBtn.click(); await this._randomDelay(300, 400); }
        cells = [...document.querySelectorAll('div.DayPicker-Day[aria-label="' + ariaLabel + '"]')]
          .filter(c => c.getAttribute('aria-disabled') !== 'true' && !c.classList.contains('DayPicker-Day--outside'));
        if (cells.length === 0) cells = findCellsByNum();
      }
      if (cells.length === 0) return false;
      cells[cells.length - 1].click();
      return true;
    };

    let dateInputs = getVisibleDateInputs();
    // data-testid="input" 을 가진 날짜 input 이 없으면 트리거 클릭이 아직 안 된 것 → 한 번 더 시도
    if (dateInputs.length === 0) {
      // 날짜 범위 컨테이너를 다양한 셀렉터로 탐색
      const altTrigger =
        document.querySelector('[class*="DateRange"], [class*="dateRange"], [class*="datePicker"]') ||
        [...document.querySelectorAll('div')].find(el =>
          el.offsetParent !== null &&
          el.querySelectorAll('div[data-testid="input"]').length >= 2
        );
      if (altTrigger) {
        altTrigger.click();
        await this._randomDelay(400, 600);
        dateInputs = getVisibleDateInputs();
      }
    }

    const startInput =
      dateInputs[0] ||
      document.querySelector('div[data-testid="input"].e1mdtx7j1') ||
      document.querySelector('div[data-testid="input"]');
    if (!startInput) {
      // 마지막 수단: hidden input 직접 주입
      forceSetHiddenRange();
      await this._randomDelay(300, 400);
      return verifyYesterdayRange();
    }
    startInput.click();
    await this._randomDelay(300, 400);

    if (!await waitForDayPicker()) return false;
    if (!await clickLastYesterdayCell()) return false;
    await this._randomDelay(400, 500);

    if (!document.querySelector('div.DayPicker-Day')) {
      await waitForDayPickerClosed();
    }

    dateInputs = getVisibleDateInputs();
    const endInput = dateInputs[1] || document.querySelector('.e1mdtx7j2 [data-testid="input"]');
    let endClicked = false;
    if (endInput) {
      endInput.click();
      await this._randomDelay(300, 400);
      if (await waitForDayPicker()) {
        endClicked = await clickLastYesterdayCell();
      }
    }

    if (!endClicked) {
      Utils.updateProgressModal({ debug: 'WARN end date UI selection failed; applying hidden date fallback' });
      forceSetHiddenRange();
    }

    await this._randomDelay(400, 600);
    if (verifyYesterdayRange()) return true;

    forceSetHiddenRange();
    await this._randomDelay(200, 300);
    return verifyYesterdayRange();
  },
  async _setOrdersDateRange(startDateObj, endDateObj) {
    const makeLabel = d => `${d.getFullYear()}.${d.getMonth() + 1}.${d.getDate()}`;
    const makePaddedLabel = d => `${d.getFullYear()}.${d.getMonth() + 1}.${String(d.getDate()).padStart(2, '0')}`;
    const makeTimestamp = d => String(new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime());

    const startLabel = makeLabel(startDateObj);
    const endLabel = makeLabel(endDateObj);
    const startPaddedLabel = makePaddedLabel(startDateObj);
    const endPaddedLabel = makePaddedLabel(endDateObj);
    const startTs = makeTimestamp(startDateObj);
    const endTs = makeTimestamp(endDateObj);

    const getVisibleDateInputs = () => [...document.querySelectorAll('div[data-testid="input"]')]
      .filter(el => el.offsetParent !== null && /\d{4}\.\d{1,2}\.\d{1,2}/.test(el.textContent || ''));

    const dispatchDateEvents = (el) => {
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
      el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
    };

    const forceSetHiddenRange = () => {
      const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
      for (const [name, val] of [['startDate', startTs], ['endDate', endTs]]) {
        const hidden = document.querySelector(`input[name="${name}"]`);
        if (!hidden) continue;
        if (nativeSetter) nativeSetter.call(hidden, val);
        else hidden.value = val;
        hidden.setAttribute('value', val);
        hidden.dispatchEvent(new Event('input', { bubbles: true }));
        hidden.dispatchEvent(new Event('change', { bubbles: true }));
      }

      const inputs = getVisibleDateInputs();
      if (inputs[0]) {
        inputs[0].textContent = startLabel;
        dispatchDateEvents(inputs[0]);
      }
      if (inputs[1]) {
        inputs[1].textContent = endLabel;
        dispatchDateEvents(inputs[1]);
      }
    };

    const verifyRange = () => {
      const inputs = getVisibleDateInputs();
      const shown = inputs.slice(0, 2).map(el => (el.textContent || '').trim());
      const startOk = shown[0]?.includes(startLabel) || shown[0]?.includes(startPaddedLabel);
      const endOk = shown[1]?.includes(endLabel) || shown[1]?.includes(endPaddedLabel);
      const ok = shown.length >= 2 && startOk && endOk;
      if (!ok) {
        Utils.updateProgressModal({ debug: 'WARN order date mismatch: ' + (shown.join(' / ') || 'none') + ' (expected ' + startPaddedLabel + ' ~ ' + endPaddedLabel + ')' });
      }
      return ok;
    };

    const waitForDayPicker = async (ms = 3000) => {
      const start = Date.now();
      while (Date.now() - start < ms) {
        if (this._stopFlag) return false;
        if (document.querySelector('div.DayPicker-Day')) return true;
        await new Promise(r => setTimeout(r, 100));
      }
      return false;
    };

    const findCellsByDate = (dayObj) => {
      const ariaLabel = dayObj.toDateString();
      const dayNum = String(dayObj.getDate());
      let cells = [...document.querySelectorAll('div.DayPicker-Day[aria-label="' + ariaLabel + '"]')]
        .filter(c => c.getAttribute('aria-disabled') !== 'true' && !c.classList.contains('DayPicker-Day--outside'));
      if (cells.length === 0) {
        cells = [...document.querySelectorAll('div.DayPicker-Day')]
          .filter(c => c.textContent.trim() === dayNum
                    && !c.classList.contains('DayPicker-Day--outside')
                    && c.getAttribute('aria-disabled') !== 'true');
      }
      return cells;
    };

    const clickDayCell = async (dayObj, navSelector) => {
      let cells = findCellsByDate(dayObj);
      if (cells.length === 0) {
        const navBtn = document.querySelector(navSelector);
        if (navBtn) {
          navBtn.click();
          await this._randomDelay(300, 400);
          cells = findCellsByDate(dayObj);
        }
      }
      if (cells.length === 0) return false;
      cells[cells.length - 1].click();
      return true;
    };

    const trigger = document.querySelector('div.e4pgcj08') ||
      document.querySelector('[class*="DateRange"], [class*="dateRange"], [class*="datePicker"]') ||
      [...document.querySelectorAll('div')].find(el =>
        el.offsetParent !== null && el.querySelectorAll('div[data-testid="input"]').length >= 2
      );
    if (trigger) {
      trigger.click();
      await this._randomDelay(400, 600);
    }
    if (this._stopFlag) return false;

    let dateInputs = getVisibleDateInputs();
    const startInput = dateInputs[0] || document.querySelector('div[data-testid="input"].e1mdtx7j1') || document.querySelector('div[data-testid="input"]');
    if (!startInput) {
      forceSetHiddenRange();
      await this._randomDelay(300, 400);
      return verifyRange();
    }

    startInput.click();
    await this._randomDelay(300, 400);
    if (!await waitForDayPicker()) return false;
    if (!await clickDayCell(startDateObj, '.DayPicker-NavButton--prev')) return false;
    await this._randomDelay(400, 500);
    if (this._stopFlag) return false;

    dateInputs = getVisibleDateInputs();
    const endInput = dateInputs[1] || document.querySelector('.e1mdtx7j2 [data-testid="input"]');
    let endClicked = false;
    if (endInput) {
      endInput.click();
      await this._randomDelay(300, 400);
      if (await waitForDayPicker()) {
        endClicked = await clickDayCell(endDateObj, '.DayPicker-NavButton--next');
      }
    }

    if (!endClicked) {
      Utils.updateProgressModal({ debug: 'WARN end date UI selection failed; applying hidden date fallback' });
      forceSetHiddenRange();
    }

    await this._randomDelay(400, 600);
    if (verifyRange()) return true;

    forceSetHiddenRange();
    await this._randomDelay(200, 300);
    return verifyRange();
  },
  async _waitForOrdersDataReady(timeoutMs = 8000) {
    const start = Date.now();
    let stableCount = 0;
    let lastSeen = -1;
    while (Date.now() - start < timeoutMs) {
      if (this._hasNoOrdersNotice()) return true;
      if (this._checkOrdersHardThrottle()) return false;
      const count = this._getExpectedOrderCount();
      const sales = this._getExpectedSalesTotal();
      if (count > 0 || sales > 0) {
        if (count === lastSeen) {
          stableCount++;
          if (stableCount >= 3) return true;   // 300ms×3 = 0.9s 안정 확인
        } else {
          stableCount = 1;
          lastSeen = count;
        }
        await new Promise(r => setTimeout(r, 300));
      } else {
        stableCount = 0;
        lastSeen = -1;
        await new Promise(r => setTimeout(r, 100));
      }
    }
    const finalCount = this._getExpectedOrderCount();
    const finalSales = this._getExpectedSalesTotal();
    return finalCount === 0 && finalSales === 0 && !this._isOrdersThrottleVisible();
  },

  async _waitForOrdersCountChange(prevCount, timeoutMs = 8000) {
    // prevCount=0이면 0→0 변화를 감지할 수 없으므로 3초만 대기 후 리턴
    const effectiveTimeout = prevCount === 0 ? 3000 : timeoutMs;
    const start = Date.now();
    while (Date.now() - start < effectiveTimeout && !this._stopFlag) {
      if (this._checkOrdersHardThrottle()) return;
      const cur = this._getExpectedOrderCount();
      if (cur !== prevCount) return;
      await new Promise(r => setTimeout(r, 100));
    }
  },

  _getExpectedSalesTotal() {
    const summarySales = this._getSalesSummaryValue('매출액', '원');
    if (summarySales > 0) return summarySales;

    const allUnitEls = document.querySelectorAll('.h1-txt .order-unit-price');
    for (const unitEl of allUnitEls) {
      if (unitEl.textContent.trim() === '원') {
        const valueEl = unitEl.previousElementSibling;
        const num = this._parseSummaryNumber(valueEl?.textContent);
        if (!isNaN(num) && num >= 0) return num;
      }
    }
    return 0;
  },

  async _collectOrdersPages(shopInfo, seed = {}) {
    const allRows = seed.allRows || [];
    const iso = new Date().toISOString();
    let pageCount = seed.pageCount || 0;
    let hasError = false;
    let storeDeadlineExceeded = false;
    let detailBlockedInStore = false;
    let reachedEnd = false;
    let emptyPageCount = 0;
    let lastPage = this._getCurrentPage();
    const collectedOrderIds = seed.collectedOrderIds || new Set();
    let failureNote = '';
    const currentRange = this._readOrdersDateRange();
    const _originalStartDateMs = seed._originalStartDateMs || currentRange.start;
    const _originalEndDateMs = seed._originalEndDateMs || currentRange.end;
    const _dateRestartCount = seed._dateRestartCount || 0;
    let storeDeadlineMs = seed._storeDeadlineMs || (Date.now() + this.ORDERS_STORE_BUDGET_MS);
    let budgetExtensions = seed._budgetExtensions || 0;
    let lastExtensionIdCount = collectedOrderIds.size;
    let lastExtendedPage = 0;
    let nextCheckpoint = seed.checkpointEvery > 0
      ? (seed.checkpointFrom || 0) + seed.checkpointEvery
      : 0;


    while (!this._stopFlag) {
      if (this._checkOrdersHardThrottle()) {
        hasError = true;
        break;
      }
      if (Date.now() > storeDeadlineMs) {
        if (this._tryExtendStoreBudget({
          extensions: budgetExtensions,
          idCount: collectedOrderIds.size,
          lastExtensionIdCount
        })) {
          budgetExtensions++;
          storeDeadlineMs = Date.now() + this.ORDERS_STORE_BUDGET_EXTENSION_MS;
          lastExtensionIdCount = collectedOrderIds.size;
          Utils.updateProgressModal({
            debug: `⏳ 매장 예산 연장 (${budgetExtensions}/${this.ORDERS_STORE_BUDGET_MAX_EXTENSIONS}) — 진전 있음 | ${this._ordersDiag({ totalRows: allRows.length, ids: collectedOrderIds.size })}`
          });
          continue;
        }
        storeDeadlineExceeded = true;
        this._markOrdersTransientError();
        hasError = true;
        failureNote = '수집 제한/경고 - 매장 예산 초과';
        Utils.updateProgressModal({
          rowCount: allRows.length,
          message: `매장 예산 초과 - 부분 저장/백필 처리 중...`,
          debug: `⏱ 매장 예산 초과 — 부분 저장 후 백필 | ${this._ordersDiag({ totalRows: allRows.length, ids: collectedOrderIds.size, ext: `${budgetExtensions}/${this.ORDERS_STORE_BUDGET_MAX_EXTENSIONS}` })}`
        });
        break;
      }
      const currentPage = this._getCurrentPage();
      lastPage = currentPage;
      pageCount++;

      Utils.updateProgressModal({
        currentPage,
        rowCount: allRows.length,
        message: `${currentPage}페이지 수집 중...`
      });

      const budgetState = {
        deadlineMs: storeDeadlineMs,
        extensions: budgetExtensions,
        lastExtensionIdCount,
        lastExtendedPage
      };
      const { rows: pageData, pageComplete, missingOrderIds, stableTimedOut, detailBlocked, storeDeadlineExceeded: pageDeadlineExceeded, skippedCount = 0 } = await this._collectCurrentPageData(shopInfo, iso, currentPage, storeDeadlineMs, collectedOrderIds, budgetState);
      storeDeadlineMs = budgetState.deadlineMs;
      budgetExtensions = budgetState.extensions;
      lastExtensionIdCount = budgetState.lastExtensionIdCount;
      lastExtendedPage = budgetState.lastExtendedPage;
      if (this._checkOrdersHardThrottle()) {
        hasError = true;
        break;
      }
      const restartRows = pageData.length ? [...allRows, ...pageData] : allRows;

      const expectedCount = this._getExpectedOrderCount();
      const expectedSales = this._getExpectedSalesTotal();
      const pageHadKnownOrders = skippedCount > 0;
      Utils.updateProgressModal({
        debug: `페이지 수집 결과 - ${this._ordersDiag({ pageData: pageData.length, skipped: skippedCount, totalRows: allRows.length, ids: collectedOrderIds.size, expectedCount, expectedSales })}`
      });
      const isNormalZeroOrders = pageData.length === 0
        && !pageHadKnownOrders
        && expectedCount === 0
        && expectedSales === 0
        && !this._isOrdersThrottleVisible();

      if (stableTimedOut && pageData.length > 0) {
        Utils.updateProgressModal({ debug: `⚠️ ${currentPage}페이지 안정화 타임아웃이나 ${pageData.length}행 확인 - 중단 없이 계속 진행` });
      }

      if (stableTimedOut && pageData.length === 0 && isNormalZeroOrders) {
        Utils.updateProgressModal({ debug: `ℹ️ ${currentPage}페이지 0건 확인 - 정상 빈 결과` });
        reachedEnd = true;
        break;
      }

      if (stableTimedOut && pageData.length === 0 && !pageHadKnownOrders) {
        this._markOrdersTransientError();
        hasError = true;
        failureNote = '수집 중 끊김 - 페이지 안정화 실패';
        Utils.updateProgressModal({ debug: `페이지 안정화 타임아웃 - 재조회 없이 백필 이월` });
        break;
      }

      if (detailBlocked || pageDeadlineExceeded) {
        if (pageData.length > 0) {
          const newRows = pageData.filter(r => !r.order_id || !collectedOrderIds.has(r.order_id));
          pageData.forEach(r => {
            if (r.order_id) collectedOrderIds.add(r.order_id);
          });
          if (newRows.length) allRows.push(...newRows);
        }
        if (detailBlocked) detailBlockedInStore = true;
        if (pageDeadlineExceeded) storeDeadlineExceeded = true;
        this._markOrdersTransientError();
        hasError = true;
        failureNote = detailBlocked ? '수집 제한/경고 - 상세패널 차단' : '수집 제한/경고 - 매장 예산 초과';
        Utils.updateProgressModal({
          currentPage,
          rowCount: allRows.length,
          message: `${currentPage}페이지 부분 수집 저장/백필 처리 중...`
        });
        Utils.updateProgressModal({
          debug: detailBlocked
            ? `🚫 상세패널 차단 감지 — 부분 저장 후 백필 | ${this._ordersDiag({ pageData: pageData.length, skipped: skippedCount, totalRows: allRows.length, ids: collectedOrderIds.size })}`
            : `⏱ 매장 예산 초과 — 부분 저장 후 백필 | ${this._ordersDiag({ pageData: pageData.length, skipped: skippedCount, totalRows: allRows.length, ids: collectedOrderIds.size, ext: `${budgetExtensions}/${this.ORDERS_STORE_BUDGET_MAX_EXTENSIONS}` })}`
        });
        break;
      }

      if (pageData.length === 0 && !pageHadKnownOrders) {
        emptyPageCount++;
        Utils.updateProgressModal({ debug: `⚠️ ${currentPage}페이지 빈 페이지 (연속 ${emptyPageCount}번째)` });
        if (this._isOrdersThrottleVisible() || this._hadRecentOrdersTransientError()) {
          this._markOrdersTransientError();
          hasError = true;
          failureNote = this._isOrdersThrottleVisible() ? '수집 제한/경고 - 조회 제한 감지' : '수집 중 끊김 - 페이지 불안정';
          break;
        }

        if (emptyPageCount >= 3) {
          this._markOrdersTransientError();
          hasError = true;
          failureNote = '수집 중 끊김 - 연속 빈 페이지';
          Utils.updateProgressModal({ debug: '연속 3페이지 데이터 없음 - 재조회 없이 백필 이월' });
          break;
        }
      } else {
        emptyPageCount = 0;
        if (!this._isOrdersThrottleVisible()) {
          this._ordersThrottleCount = 0;
        }
        const newRows = pageData.filter(r => !r.order_id || !collectedOrderIds.has(r.order_id));
        pageData.forEach(r => {
          if (r.order_id) collectedOrderIds.add(r.order_id);
        });
        if (newRows.length) {
          allRows.push(...newRows);
          if (nextCheckpoint > 0 && typeof seed.onCheckpoint === 'function') {
            const uniqCount = new Set(allRows.map(r => r.order_id).filter(Boolean)).size;
            while (uniqCount >= nextCheckpoint) {
              await seed.onCheckpoint(allRows, uniqCount);
              nextCheckpoint += seed.checkpointEvery;
            }
          }
        }
      }

      Utils.updateProgressModal({
        currentPage,
        rowCount: allRows.length
      });
      console.log(`[쿠팡이츠] ${currentPage}페이지: ${pageData.length}행 수집 (총 ${allRows.length}행)`);

      if (this._stopFlag) break;

      if (!pageComplete) {
        this._markOrdersTransientError();
        hasError = true;
        failureNote = '수집 중 끊김 - 주문 상세 미완료';
        Utils.updateProgressModal({ debug: `페이지 미완료 주문: ${missingOrderIds.join(', ')} - 재조회 없이 백필 이월` });
        break;
      }

      await this._closeErrorPopups();

      const prevFirstOrderId = this._getFirstOrderId();
      Utils.updateProgressModal({ debug: `다음 페이지 이동 전 - ${this._ordersDiag({ currentPage, prevFirst: prevFirstOrderId || '-', totalRows: allRows.length, ids: collectedOrderIds.size })}` });
      let nextResult = await this._clickNextOrderPage();
      if (!nextResult.moved && nextResult.stalled) {
        await this._closeErrorPopups();
        await this._randomDelay(800, 1500);
        nextResult = await this._clickNextOrderPage();
      }
      if (!nextResult.moved) {
        if (nextResult.stalled) {
          this._markOrdersTransientError();
          hasError = true;
          failureNote = '페이지 이동 정체 - 재조회 없이 백필 이월';
          Utils.updateProgressModal({ debug: `다음 페이지 이동 정체(재시도 실패) - ${this._ordersDiag({ currentPage, totalRows: allRows.length, ids: collectedOrderIds.size })}` });
        } else {
          Utils.updateProgressModal({ debug: `다음 페이지 없음/이동 실패 - ${this._ordersDiag({ currentPage, totalRows: allRows.length, ids: collectedOrderIds.size })}` });
          console.log('[쿠팡이츠] 마지막 페이지 도달');
          reachedEnd = true;
        }
        break;
      }

      Utils.updateProgressModal({
        currentPage: currentPage + 1,
        rowCount: allRows.length,
        message: `${currentPage + 1}페이지 로딩 대기...`
      });

      const loaded = await this._waitForPageLoad(prevFirstOrderId, 20000);

      if (!loaded) {
        if (this._stopFlag) break;

        Utils.updateProgressModal({
          currentPage: currentPage + 1,
          rowCount: allRows.length,
          debug: `⚠️ ${currentPage + 1}페이지 로딩 타임아웃 - 재시도 | ${this._ordersDiag({ prevFirst: prevFirstOrderId || '-', totalRows: allRows.length, ids: collectedOrderIds.size })}`
        });
        await this._randomDelay(800, 1500);
        await this._closeErrorPopups();

        const retryLoaded = await this._waitForPageLoad(prevFirstOrderId, 10000);
        if (!retryLoaded) {
          const actualPage = this._getCurrentPage();
          const visibleFirstOrderId = this._getFirstOrderId();
          const visibleOrderCount = document.querySelectorAll('.order-search-result-content > li.col-12').length;
          if (visibleOrderCount > 0 && (actualPage !== currentPage || visibleFirstOrderId !== prevFirstOrderId)) {
            Utils.updateProgressModal({
              currentPage: actualPage,
              rowCount: allRows.length,
              debug: `⚠️ 페이지 로딩 확인 실패했지만 계속 진행 - ${this._ordersDiag({ actualPage, prevFirst: prevFirstOrderId || '-', visibleFirst: visibleFirstOrderId || '-', visibleOrderCount, totalRows: allRows.length })}`
            });
          } else {
            this._markOrdersTransientError();
            hasError = true;
            failureNote = '수집 중 끊김 - 페이지 로딩 타임아웃';
            Utils.updateProgressModal({ debug: `페이지 로딩 타임아웃 - 재조회 없이 백필 이월 | ${this._ordersDiag({ actualPage, prevFirst: prevFirstOrderId || '-', visibleFirst: visibleFirstOrderId || '-', visibleOrderCount, totalRows: allRows.length })}` });
            break;
          }
        }
      }

      if (this._isSafeOrdersPacingEnabled()) {
        await this._humanizeManualOrdersAfterPage();
      } else {
        await this._randomDelay(400, 700);
      }
    }

    return { allRows, pageCount, hasError, storeDeadlineExceeded, detailBlocked: detailBlockedInStore, failureNote, resumePage: lastPage, collectedOrderIds, reachedEnd, budgetExtensions };
  },

  _parseOrderDateOnly(value, fallbackRange = null) {
    const m = String(value || '').match(/(\d{4})[.-](\d{1,2})[.-](\d{1,2})/);
    const short = !m ? String(value || '').match(/(?:^|\D)(\d{1,2})[.-](\d{1,2})(?:\D|$)/) : null;
    if (!m && !short) return null;

    const fallbackEnd = parseInt(fallbackRange?.end, 10);
    const fallbackYear = !isNaN(fallbackEnd) ? new Date(fallbackEnd).getFullYear() : new Date().getFullYear();
    const y = m ? parseInt(m[1], 10) : fallbackYear;
    const mo = parseInt(m ? m[2] : short[1], 10) - 1;
    const d = parseInt(m ? m[3] : short[2], 10);
    const dt = new Date(y, mo, d);
    if (dt.getFullYear() !== y || dt.getMonth() !== mo || dt.getDate() !== d) return null;
    dt.setHours(0, 0, 0, 0);
    return dt;
  },

  _getRestartDateFromRows(rows) {
    let minDate = null;
    for (const row of rows || []) {
      const d = this._parseOrderDateOnly(row?.order_date);
      if (!d) continue;
      if (!minDate || d.getTime() < minDate.getTime()) minDate = d;
    }
    return minDate;
  },

  _isOrderRowBeforeDate(row, compareDate) {
    const d = this._parseOrderDateOnly(row?.order_date);
    return !!d && d.getTime() < compareDate.getTime();
  },

  async _restartFromErrorDate(safeRows, shopInfo, originalStartDateMs, originalEndDateMs, dateRestartCount = 0) {
    if (!this._isSafeOrdersPacingEnabled()) return { restarted: false };
    if (dateRestartCount >= 3) return { restarted: false };
    if (this._stopFlag) return { restarted: false, stopped: true };

    const restartDateObj = this._getRestartDateFromRows(safeRows);
    if (!restartDateObj) return { restarted: false };

    const startDateMs = parseInt(originalStartDateMs, 10);
    const startDateObj = isNaN(startDateMs) ? restartDateObj : new Date(startDateMs);
    const endDateMs = parseInt(originalEndDateMs, 10);
    const originalEndDateObj = isNaN(endDateMs) ? new Date() : new Date(endDateMs);
    startDateObj.setHours(0, 0, 0, 0);
    originalEndDateObj.setHours(0, 0, 0, 0);
    const preRows = safeRows || [];
    const originalEndLabel = `${originalEndDateObj.getFullYear()}.${originalEndDateObj.getMonth() + 1}.${originalEndDateObj.getDate()}`;
    const restartStartObj = new Date(Math.max(restartDateObj.getTime(), startDateObj.getTime()));
    restartStartObj.setHours(0, 0, 0, 0);
    const restartLabel = `${restartStartObj.getFullYear()}.${restartStartObj.getMonth() + 1}.${restartStartObj.getDate()}`;

    Utils.updateProgressModal({
      debug: `⚠️ 에러 발생 → 역순 이어수집 ${restartLabel} ~ ${originalEndLabel} 재조회 시작`
    });

    const ok = await this._setOrdersDateRange(restartStartObj, originalEndDateObj);
    if (!ok || this._stopFlag) return { restarted: false, stopped: this._stopFlag };

    const prevCount = this._getExpectedOrderCount();
    if (!await this._clickOrdersSearchButton()) return { restarted: false };
    if (this._stopFlag) return { restarted: false, stopped: true };
    await this._waitForOrdersCountChange(prevCount, 8000);
    await this._waitForOrdersDataReady(10000);
    if (this._stopFlag) return { restarted: false, stopped: true };

    const seed = {
      allRows: preRows,
      collectedOrderIds: new Set(preRows.map(r => r.order_id).filter(Boolean)),
      pageCount: 0,
      _originalStartDateMs: originalStartDateMs,
      _originalEndDateMs: originalEndDateMs,
      _dateRestartCount: dateRestartCount + 1
    };
    const { allRows: newRows, hasError } = await this._collectOrdersPages(shopInfo, seed);
    return { restarted: true, newRows, hasError };
  },
  // ── F5(전체 새로고침) 기반 복구: 재조회로도 안 풀리는 SPA 멈춤 대응 ──
  // 현재까지 수집한 rows/주문ID + 날짜범위 + 막힌 페이지를 저장하고 location.reload().
  // 새로고침 후 05_main 의 load 핸들러가 _resumeOrdersFromReload 를 호출해 이어서 수집.
  // manual 단일매장 수집에서만 동작(배치는 URL 매장·드롭다운 상태가 복잡해 제외).
  async _hardReloadResume(resumePage, allRows, idSet, shopInfo, pageCount) {
    if (this._collectSource !== 'manual') return false;

    const today = Utils.getTodayStr();
    const storeId = (location.href.match(/\/orders\/(\d+)/) || [])[1] || '';
    // reloadCount 는 in-memory 로 누적(_resumeOrdersFromReload 가 직전 값 복원) — 키가
    // 재개 시작 시 제거되므로 storage 가 아닌 this 값을 권위로 사용.
    const reloadCount = this._resumeReloadCount || 0;
    const MAX_RELOAD = 2;
    if (reloadCount >= MAX_RELOAD) {
      Utils.updateProgressModal({ debug: `❌ F5 재시도 한도(${MAX_RELOAD}회) 초과 — 부분 저장으로 종료` });
      await this._clearResumeState();
      return false;
    }

    const range = this._readOrdersDateRange();
    await this._saveResumeState({
      date: today,
      storeId,
      source: 'manual',
      resumePage,
      startDate: range.start,
      endDate: range.end,
      rows: allRows,
      ids: [...idSet],
      pageCount,
      reloadCount: reloadCount + 1,
      shopInfo
    });
    Utils.updateProgressModal({ debug: `🔄 ${resumePage}페이지 막힘 → F5 새로고침 후 재개 (${reloadCount + 1}/${MAX_RELOAD})` });
    await this._randomDelay(800, 1200);
    location.reload();
    return true; // 이후 페이지 언로드
  },

  // 05_main load 핸들러가 새로고침 후 호출 — 저장된 상태로 막힌 페이지부터 이어서 수집
  async _resumeOrdersFromReload(state) {
    this._stopFlag = false;
    this._collectSource = 'manual';
    this._resumeReloadCount = state.reloadCount || 0; // F5 재개 누적 횟수 복원 (한도 판정용)
    this._setupEscListener();

    const shopInfo = state.shopInfo || this._getShopInfo();
    Utils.showProgressModal('쿠팡이츠 수집', `F5 재개: ${state.resumePage}페이지 복귀 중...`);
    Utils.updateProgressModal({ debug: `♻️ 새로고침 재개 — ${(state.rows || []).length}행 보존, ${state.resumePage}페이지부터` });

    // 1) 날짜 범위 복원 (수동 모드는 새로고침 시 날짜 필터가 초기화되므로 재적용 필수)
    if (state.startDate && state.endDate) {
      await this._applyOrdersDateRange(state.startDate, state.endDate);
    }

    // 2) 재조회
    const prevCount = this._getExpectedOrderCount();
    await this._clickOrdersSearchButton();
    await this._waitForOrdersCountChange(prevCount, 8000);
    await this._waitForOrdersDataReady(8000);

    // 3) 막힌 페이지로 이동 (이미 조회된 상태이므로 재조회 없이 이동만)
    const recovered = await this._recoverToPage(state.resumePage, false);
    if (!recovered) {
      Utils.updateProgressModal({ debug: `⚠️ ${state.resumePage}페이지 복귀 실패 — 가능한 범위까지 이어서 수집` });
    }

    // 4) 보존된 rows/ID 를 seed 로 이어서 수집 (중복은 collectedOrderIds 가 차단)
    const seed = {
      allRows: state.rows || [],
      collectedOrderIds: new Set(state.ids || []),
      pageCount: state.pageCount || 0
    };
    const { allRows, pageCount, hasError, reloaded } = await this._collectOrdersPages(shopInfo, seed);
    if (reloaded) return; // 또 F5 재개로 넘어감 — 이후 처리는 다음 로드에서

    // 5) 끝까지 마쳤으면 resume 상태 삭제
    await this._clearResumeState();

    // 6) 검증값 재계산 후 다운로드 (_collectOrdersWithValidation 의 검증 로직과 동일 기준)
    const expectedSales = this._getExpectedSalesTotal();
    const expectedCount = this._getExpectedOrderCount();
    const collectedSales = allRows
      .filter(r => r.is_cancelled !== 'Y')
      .reduce((sum, r) => {
        const val = parseInt((r['매출액'] || '0').toString().replace(/,/g, ''), 10);
        return sum + (isNaN(val) ? 0 : val);
      }, 0);
    const collectedCount = new Set(allRows.filter(r => r.is_cancelled !== 'Y').map(r => r.order_id).filter(Boolean)).size;
    const salesMatch = expectedSales === 0 || collectedSales === expectedSales;
    const countMatch = expectedCount === 0 || collectedCount === expectedCount;

    await this._downloadOrdersResult(allRows, shopInfo, {
      pageCount, hasError, expectedCount, expectedSales, collectedSales, salesMatch,
      collectedCount, countMatch, attempt: 1, isMultiStore: false
    });
    this._removeEscListener();

    // 7) 단일매장 완료 신호 (runner 무한 대기 방지)
    if (window.top === window.self) {
      try {
        chrome.runtime.sendMessage({
          type: 'MULTISTORE_COMPLETE',
          payload: { category: 'orders', storeCount: 1, completedCount: this._stopFlag ? 0 : 1, stopped: !!this._stopFlag }
        }).catch(() => {});
      } catch (_) {}
    }
  },

  // 현재 주문 날짜 필터(hidden input, epoch ms) 읽기
  _readOrdersDateRange() {
    const s = document.querySelector('input[name="startDate"]');
    const e = document.querySelector('input[name="endDate"]');
    const parseVisibleDate = (text) => {
      const raw = String(text || '').trim();
      const full = raw.match(/(\d{4})[.-]\s*(\d{1,2})[.-]\s*(\d{1,2})/);
      const short = !full ? raw.match(/(\d{1,2})[.-]\s*(\d{1,2})/) : null;
      if (!full && !short) return '';
      const y = full ? parseInt(full[1], 10) : new Date().getFullYear();
      const mo = parseInt(full ? full[2] : short[1], 10) - 1;
      const d = parseInt(full ? full[3] : short[2], 10);
      const dt = new Date(y, mo, d);
      if (dt.getFullYear() !== y || dt.getMonth() !== mo || dt.getDate() !== d) return '';
      dt.setHours(0, 0, 0, 0);
      return String(dt.getTime());
    };
    const parseVisibleRange = () => {
      const candidates = Array.from(document.querySelectorAll('span, div'))
        .map(el => (el.textContent || '').trim())
        .filter(text => /\d{4}[.-]\s*\d{1,2}[.-]\s*\d{1,2}\s*-\s*\d{4}[.-]\s*\d{1,2}[.-]\s*\d{1,2}/.test(text));
      const raw = candidates[0] || '';
      const parts = raw.split(/\s*-\s*/);
      if (parts.length < 2) return { start: '', end: '' };
      return {
        start: parseVisibleDate(parts[0]),
        end: parseVisibleDate(parts[1])
      };
    };
    const visibleInputs = [...document.querySelectorAll('div[data-testid="input"]')].filter(el => el.offsetParent !== null);
    const visibleRange = parseVisibleRange();
    return {
      start: s?.value || parseVisibleDate(visibleInputs[0]?.textContent) || visibleRange.start,
      end: e?.value || parseVisibleDate(visibleInputs[1]?.textContent) || visibleRange.end
    };
  },

  // 저장된 날짜 범위 재적용 (hidden input 주입 + 보이는 라벨 갱신)
  async _applyOrdersDateRange(startMs, endMs) {
    const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
    const setHidden = (name, val) => {
      const hidden = document.querySelector(`input[name="${name}"]`);
      if (!hidden || !val) return;
      if (nativeSetter) nativeSetter.call(hidden, String(val)); else hidden.value = String(val);
      hidden.setAttribute('value', String(val));
      hidden.dispatchEvent(new Event('input', { bubbles: true }));
      hidden.dispatchEvent(new Event('change', { bubbles: true }));
    };
    setHidden('startDate', startMs);
    setHidden('endDate', endMs);

    const fmt = (ms) => { const d = new Date(parseInt(ms, 10)); return `${d.getFullYear()}.${d.getMonth() + 1}.${d.getDate()}`; };
    const inputs = [...document.querySelectorAll('div[data-testid="input"]')].filter(el => el.offsetParent !== null);
    if (inputs[0] && startMs) { inputs[0].textContent = fmt(startMs); inputs[0].dispatchEvent(new Event('input', { bubbles: true })); }
    if (inputs[1] && endMs) { inputs[1].textContent = fmt(endMs); inputs[1].dispatchEvent(new Event('input', { bubbles: true })); }
    await this._randomDelay(300, 500);
  },

  _loadResumeState() {
    return new Promise((resolve) => {
      chrome.storage.local.get(['ce_orders_resume'], (r) => resolve(r.ce_orders_resume || null));
    });
  },
  _saveResumeState(data) {
    return new Promise((resolve) => chrome.storage.local.set({ ce_orders_resume: data }, resolve));
  },
  _clearResumeState() {
    return new Promise((resolve) => chrome.storage.local.remove(['ce_orders_resume'], resolve));
  },

  _getOrderDateBoundaries(rows, fallbackRange = null) {
    const datedRows = (rows || [])
      .map(row => this._parseOrderDateOnly(row?.order_date, fallbackRange))
      .filter(Boolean)
      .map(d => d.getTime());
    if (!datedRows.length) return null;

    const firstMs = datedRows[0];
    const lastMs = datedRows[datedRows.length - 1];
    return {
      minMs: Math.min(...datedRows),
      maxMs: Math.max(...datedRows),
      isDescending: firstMs >= lastMs
    };
  },

  _calcRemainingRange(bounds, originalRange) {
    const origStartMs = parseInt(originalRange?.start, 10);
    const origEndMs = parseInt(originalRange?.end, 10);
    if (!bounds) return null;

    const normalizeDay = (ms) => {
      const d = new Date(ms);
      d.setHours(0, 0, 0, 0);
      return d.getTime();
    };
    const startMs = !isNaN(origStartMs) ? normalizeDay(origStartMs) : normalizeDay(bounds.minMs);
    const endMs = !isNaN(origEndMs) ? normalizeDay(origEndMs) : normalizeDay(bounds.maxMs);

    if (bounds.isDescending) {
      const resumeStartMs = Math.max(normalizeDay(bounds.minMs), startMs);
      if (resumeStartMs > endMs) return null;
      return { startMs: resumeStartMs, endMs };
    }

    const resumeEndMs = Math.min(normalizeDay(bounds.maxMs), endMs);
    if (resumeEndMs < startMs) return null;
    return { startMs, endMs: resumeEndMs };
  },

  _normalizeOrdersRange(originalRange) {
    const startMs = parseInt(originalRange?.start, 10);
    const endMs = parseInt(originalRange?.end, 10);
    if (isNaN(startMs) || isNaN(endMs)) return null;
    return { startMs, endMs };
  },

  async _resumeOrdersRange(remainingStartMs, remainingEndMs, shopInfo, resumeDepth) {
    const MAX_MANUAL_RESUME = 10;
    if (resumeDepth >= MAX_MANUAL_RESUME) {
      Utils.showSuccessModal('이어수집 중단', `자동 이어수집 최대 횟수(${MAX_MANUAL_RESUME}회)에 도달했습니다.`, { status: 'warning' });
      return false;
    }

    this._stopFlag = false;
    await this._applyOrdersDateRange(remainingStartMs, remainingEndMs);
    const prevCount = this._getExpectedOrderCount();
    const searched = await this._clickOrdersSearchButton();
    if (searched) {
      await this._waitForOrdersCountChange(prevCount, 8000);
      await this._waitForOrdersDataReady(8000);
      await this._collectOrdersManualWithCheckpoints(shopInfo, resumeDepth + 1, {
        start: String(remainingStartMs),
        end: String(remainingEndMs)
      });
    } else {
      Utils.showSuccessModal('수집 중단', '이어수집 검색 버튼을 찾지 못했습니다.', { status: 'warning' });
    }
    return !!searched;
  },

  async _resumeOrdersCurrentPage(shopInfo, resumeDepth, seed = {}) {
    const MAX_MANUAL_RESUME = 10;
    if (resumeDepth >= MAX_MANUAL_RESUME) {
      Utils.showSuccessModal('이어수집 중단', `자동 이어수집 최대 횟수(${MAX_MANUAL_RESUME}회)에 도달했습니다.`, { status: 'warning' });
      return false;
    }

    const waitMs = 90000 + Math.floor(Math.random() * 60001);
    Utils.updateProgressModal({
      message: `${seed.resumePage || this._getCurrentPage()}페이지 대기 후 이어수집 준비 중...`,
      debug: `현재 페이지 이어수집 - ${Math.ceil(waitMs / 1000)}초 대기 | page=${seed.resumePage || this._getCurrentPage()}, rows=${(seed.allRows || []).length}, ids=${seed.collectedOrderIds?.size || 0}`
    });
    await new Promise(r => setTimeout(r, waitMs));
    if (this._stopFlag) return false;

    const resumePage = seed.resumePage || this._getCurrentPage();
    if (this._getCurrentPage() !== resumePage) {
      Utils.updateProgressModal({ debug: `현재 페이지 이어수집 - ${resumePage}페이지 복귀 시도(재조회 없음)` });
      await this._recoverToPage(resumePage, false);
    }

    const resumeSeed = {
      allRows: seed.allRows || [],
      collectedOrderIds: seed.collectedOrderIds || new Set((seed.allRows || []).map(r => r.order_id).filter(Boolean)),
      pageCount: seed.pageCount || 0,
      _originalStartDateMs: seed._originalStartDateMs,
      _originalEndDateMs: seed._originalEndDateMs,
      _budgetExtensions: 0,
      _storeDeadlineMs: Date.now() + this.ORDERS_STORE_BUDGET_MS
    };
    Utils.updateProgressModal({
      message: `${resumePage}페이지에서 이어수집 중...`,
      debug: `현재 페이지 이어수집 시작 - 조회 버튼 클릭 없음 | page=${resumePage}, rows=${resumeSeed.allRows.length}, ids=${resumeSeed.collectedOrderIds.size}`
    });
    const result = await this._collectOrdersPages(shopInfo, resumeSeed);
    return result;
  },

  async _showResumeRangeModal(remainingStartMs, remainingEndMs, shopInfo, originalRange, resumeDepth) {
    const fmtDate = (ms) => {
      const d = new Date(ms);
      return `${d.getFullYear()}.${String(d.getMonth() + 1).padStart(2, '0')}.${String(d.getDate()).padStart(2, '0')}`;
    };
    const label = `${fmtDate(remainingStartMs)} ~ ${fmtDate(remainingEndMs)}`;

    return new Promise((resolve) => {
      const overlay = document.createElement('div');
      overlay.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);z-index:2147483647;display:flex;align-items:center;justify-content:center;';

      const modal = document.createElement('div');
      modal.style.cssText = 'background:#fff;border-radius:16px;padding:32px;min-width:400px;max-width:480px;box-shadow:0 20px 60px rgba(0,0,0,0.3);text-align:center;';
      modal.innerHTML = `
        <div style="font-size:2rem;margin-bottom:8px;">⚡</div>
        <div style="font-size:1.1rem;font-weight:700;margin-bottom:8px;">수집 중단</div>
        <div style="color:#555;margin-bottom:20px;">수집된 파일이 저장됐습니다.<br>
          <strong style="color:#e85d04;">재조회 구간: ${label}</strong>
        </div>
        <button id="__ce_resume_btn" style="background:#e85d04;color:#fff;border:none;border-radius:8px;padding:10px 20px;font-size:0.95rem;cursor:pointer;margin-right:8px;">
          이어수집 (${label})
        </button>
        <button id="__ce_close_btn" style="background:#e0e0e0;border:none;border-radius:8px;padding:10px 16px;font-size:0.95rem;cursor:pointer;">
          닫기
        </button>
      `;
      overlay.appendChild(modal);
      document.body.appendChild(overlay);

      modal.querySelector('#__ce_close_btn').onclick = () => {
        overlay.remove();
        resolve(false);
      };

      modal.querySelector('#__ce_resume_btn').onclick = async () => {
        overlay.remove();
        const searched = await this._resumeOrdersRange(remainingStartMs, remainingEndMs, shopInfo, resumeDepth);
        resolve(!!searched);
      };
    });
  },

  async _downloadOrdersPartial(allRows, shopInfo) {
    const headers = [
      'collected_at', 'store_id', 'store_name', 'order_date', 'order_id',
      'delivery_type', 'order_status', 'order_summary', 'total_price', 'is_cancelled',
      'menu_name', 'menu_qty', 'menu_price', 'menu_options',
      '매출액', '상점부담_쿠폰', '중개_이용료', '결제대행사_수수료',
      '배달비', '광고비', '부가세', '즉시할인금액', '정산_예정_금액', '취소금액'
    ];
    const csv = Utils.toCSV(headers, allRows);
    const filename = await Utils.downloadCSV(csv, {
      channel: 'coupangeats', purpose: 'orders',
      storeName: shopInfo.store_name, storeId: shopInfo.store_id,
      dateStr: this._ordersDownloadDateStr()
    });
    Utils.updateProgressModal({ debug: `💾 저장: ${filename}` });
    return filename;
  },

  _computeOrdersTotals(rows) {
    const valid = (rows || []).filter(r => r.is_cancelled !== 'Y');
    const sales = valid.reduce((sum, r) => {
      const val = parseInt((r['매출액'] || '0').toString().replace(/,/g, ''), 10);
      return sum + (isNaN(val) ? 0 : val);
    }, 0);
    const count = new Set(valid.map(r => r.order_id).filter(Boolean)).size;
    return { sales, count };
  },

  async _collectOrdersManualWithCheckpoints(shopInfo, _resumeDepth = 0, _activeRange = null) {
    if (_resumeDepth === 0) this._resumeNoProgressBase = 0;
    const CHECKPOINT_EVERY = 100;
    let partialIndex = 0;
    let lastSavedUniqueCount = 0;
    const originalRange = _activeRange || this._readOrdersDateRange();

    Utils.showProgressModal('쿠팡이츠 수집', '수집 시작...');
    const expectedSales = this._getExpectedSalesTotal();
    const expectedCount = this._getExpectedOrderCount();
    Utils.updateProgressModal({
      debug: `📊 기대값 - 매출액: ${expectedSales > 0 ? expectedSales.toLocaleString() + '원' : '읽기 실패'} | 주문: ${expectedCount > 0 ? expectedCount + '건' : '읽기 실패'}`
    });

    const seed = {
      checkpointEvery: CHECKPOINT_EVERY,
      checkpointFrom: 0,
      _originalStartDateMs: originalRange.start,
      _originalEndDateMs: originalRange.end,
      onCheckpoint: async (rows, uniqueCount) => {
        partialIndex++;
        lastSavedUniqueCount = uniqueCount;
        await this._downloadOrdersPartial(rows, shopInfo);
      },
    };

    const { allRows, pageCount, hasError, reloaded, storeDeadlineExceeded, failureNote, resumePage, collectedOrderIds, reachedEnd } = await this._collectOrdersPages(shopInfo, seed);
    if (reloaded) return false;

    if (!allRows.length) {
      if (hasError || this._hadRecentOrdersTransientError()) {
        Utils.showSuccessModal('수집 중단', failureNote || '수집 중 끊김 - 빈 결과', { status: 'warning' });
        return false;
      }
      Utils.showSuccessModal('데이터 없음', '추출할 주문이 없습니다.', { status: 'error' });
      return false;
    }

    const uniqueCount = new Set(allRows.map(r => r.order_id).filter(Boolean)).size;
    if (uniqueCount > lastSavedUniqueCount) {
      partialIndex++;
      await this._downloadOrdersPartial(allRows, shopInfo);
    }

    if (this._stopFlag && partialIndex > 0) {
      const bounds = this._getOrderDateBoundaries(allRows, originalRange);
      const remaining = bounds ? this._calcRemainingRange(bounds, originalRange) : null;
      if (remaining) {
        await this._showResumeRangeModal(remaining.startMs, remaining.endMs, shopInfo, originalRange, _resumeDepth);
      } else {
        Utils.showSuccessModal('수집 중단', `${partialIndex}개 파일 저장됨`, { status: 'warning' });
      }
      return true;
    }

    const collectedSales = allRows
      .filter(r => r.is_cancelled !== 'Y')
      .reduce((sum, r) => {
        const val = parseInt((r['매출액'] || '0').toString().replace(/,/g, ''), 10);
        return sum + (isNaN(val) ? 0 : val);
      }, 0);
    const collectedCount = new Set(allRows.filter(r => r.is_cancelled !== 'Y').map(r => r.order_id).filter(Boolean)).size;
    const salesMatch = expectedSales === 0 || collectedSales === expectedSales;
    const countMatch = expectedCount === 0 || collectedCount === expectedCount;

    if (!salesMatch || !countMatch) {
      const uniqueAllCount = new Set(allRows.map(r => r.order_id).filter(Boolean)).size;
      Utils.updateProgressModal({
        debug: `⚠️ 수집 검증 불일치 - 주문 ${collectedCount}/${expectedCount || '읽기 실패'}, 매출 ${collectedSales.toLocaleString()}원/${expectedSales ? expectedSales.toLocaleString() + '원' : '읽기 실패'} | rows=${allRows.length}, unique=${uniqueAllCount}, hasError=${hasError}, failure=${failureNote || '-'}`
      });
      const hardStop = (hasError || this._hadRecentOrdersTransientError()) && !storeDeadlineExceeded;
      if (hardStop) {
        Utils.updateProgressModal({ debug: `검증 불일치 하드중단 - ${this._ordersDiag({ rows: allRows.length, unique: uniqueAllCount, collectedCount, expectedCount: expectedCount || 'fail', failure: failureNote || '-' })}` });
        Utils.showSuccessModal(
          '수집 검증 불일치',
          `저장 완료: ${partialIndex}개 파일<br>주문: ${collectedCount}/${expectedCount || '읽기 실패'}건<br>매출: ${collectedSales.toLocaleString()}원/${expectedSales ? expectedSales.toLocaleString() + '원' : '읽기 실패'}<br>${failureNote || '수집 중 끊김 - 재조회 없이 종료'}`,
          { status: 'warning' }
        );
        return true;
      }
      if (storeDeadlineExceeded) {
        let resumed = await this._resumeOrdersCurrentPage(shopInfo, _resumeDepth + 1, {
          allRows,
          collectedOrderIds: collectedOrderIds || new Set(allRows.map(r => r.order_id).filter(Boolean)),
          pageCount,
          resumePage,
          _originalStartDateMs: originalRange.start,
          _originalEndDateMs: originalRange.end
        });
        let depth = _resumeDepth + 1;
        while (resumed && resumed.storeDeadlineExceeded && !resumed.reachedEnd && !this._stopFlag && depth < 10) {
          depth++;
          resumed = await this._resumeOrdersCurrentPage(shopInfo, depth, {
            allRows: resumed.allRows,
            collectedOrderIds: resumed.collectedOrderIds,
            pageCount: resumed.pageCount,
            resumePage: resumed.resumePage,
            _originalStartDateMs: originalRange.start,
            _originalEndDateMs: originalRange.end
          });
        }
        let finalRows = allRows;
        if (resumed?.allRows?.length) {
          finalRows = resumed.allRows;
          partialIndex++;
          await this._downloadOrdersPartial(resumed.allRows, shopInfo);
        }
        const { sales: finalSales, count: finalCount } = this._computeOrdersTotals(finalRows);
        const finalSalesMatch = expectedSales === 0 || finalSales === expectedSales;
        const finalCountMatch = expectedCount === 0 || finalCount === expectedCount;
        if (finalSalesMatch && finalCountMatch) {
          Utils.showSuccessModal('수집 완료', `총 ${partialIndex}개 파일 저장 완료`, { status: 'ok' });
        } else {
          Utils.showSuccessModal(
            '수집 검증 불일치',
            `저장 완료: ${partialIndex}개 파일<br>주문: ${finalCount}/${expectedCount || '읽기 실패'}건<br>매출: ${finalSales.toLocaleString()}원/${expectedSales ? expectedSales.toLocaleString() + '원' : '읽기 실패'}`,
            { status: 'warning' }
          );
        }
        return true;
      }
      const bounds = this._getOrderDateBoundaries(allRows, originalRange);
      const remaining = (!reachedEnd && bounds) ? this._calcRemainingRange(bounds, originalRange) : null;
      const madeProgress = uniqueAllCount > (this._resumeNoProgressBase || 0);
      if (remaining && madeProgress) {
        this._resumeNoProgressBase = uniqueAllCount;
        const waitMs = 90000 + Math.floor(Math.random() * 60001);
        Utils.updateProgressModal({ debug: `검증 불일치 - ${Math.ceil(waitMs / 1000)}초 대기 후 남은 구간 이어수집 | remaining=${new Date(remaining.startMs).toLocaleString()}~${new Date(remaining.endMs).toLocaleString()}` });
        await new Promise(r => setTimeout(r, waitMs));
        await this._resumeOrdersRange(remaining.startMs, remaining.endMs, shopInfo, _resumeDepth);
        return true;
      }
      Utils.updateProgressModal({ debug: `검증 불일치 - ${reachedEnd ? '페이지 소진' : !madeProgress ? '이전 재개 대비 진전 없음' : '남은 구간 계산 불가'}, 재조회 없이 저장본으로 종료 | ${this._ordersDiag({ rows: allRows.length, unique: uniqueAllCount, collectedCount, expectedCount: expectedCount || 'fail' })}` });
      Utils.showSuccessModal(
        '수집 검증 불일치',
        `저장 완료: ${partialIndex}개 파일<br>주문: ${collectedCount}/${expectedCount || '읽기 실패'}건<br>매출: ${collectedSales.toLocaleString()}원/${expectedSales ? expectedSales.toLocaleString() + '원' : '읽기 실패'}`,
        { status: 'warning' }
      );
      return true;
    }

    Utils.showSuccessModal('수집 완료', `총 ${partialIndex}개 파일 저장 완료`, { status: 'ok' });
    return true;
  },

  async _collectOrdersWithValidation(shopInfo, isMultiStore = false, opts = {}) {
    this._lastOrdersCollectResult = null;
    if (this._collectSource === 'manual' && !isMultiStore) {
      return await this._collectOrdersManualWithCheckpoints(shopInfo);
    }
    if (isMultiStore) {
      Utils.updateProgressModal({ message: `${shopInfo.store_name ? shopInfo.store_name + ' ' : ''}수집 시작...` });
    } else {
      Utils.showProgressModal('쿠팡이츠 수집', `${shopInfo.store_name ? shopInfo.store_name + ' ' : ''}첫 페이지 로딩 중...`);
    }

    const MAX_RETRY = 1;
    // ESC 대비: 마지막으로 수집된 결과 보존
    let lastResult = null;

    for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
      // ESC가 재시도 대기 중에 눌린 경우 → 이전 수집 결과로 저장
      if (this._stopFlag && lastResult) {
        await this._downloadOrdersResult(lastResult.allRows, shopInfo, { ...lastResult.stats, isMultiStore });
        return true;
      }

      // 날짜 설정 실패 시 기대값 검증 스킵 (잘못된 날짜 기간의 합계와 비교하면 항상 불일치)
      const expectedSales = opts.skipValidation ? 0 : this._getExpectedSalesTotal();
      const expectedCount = opts.skipValidation ? 0 : this._getExpectedOrderCount();

      Utils.updateProgressModal({
        debug: `📊 기대값 - 매출액: ${expectedSales > 0 ? expectedSales.toLocaleString() + '원' : '읽기 실패'} | 주문: ${expectedCount > 0 ? expectedCount + '건' : '읽기 실패'}`
      });

      const { allRows, pageCount, hasError, reloaded, storeDeadlineExceeded, detailBlocked, failureNote, resumePage, reachedEnd, budgetExtensions } = await this._collectOrdersPages(shopInfo);

      // F5 새로고침 재개로 넘어감 → 이후 처리는 새 페이지 로드의 _resumeOrdersFromReload 가 담당
      if (reloaded) return false;
      if (this._checkOrdersHardThrottle()) return false;

      if (allRows.length === 0) {
        // ESC로 인해 빈 결과인 경우 이전 결과 사용
        if (this._stopFlag && lastResult) {
          await this._downloadOrdersResult(lastResult.allRows, shopInfo, { ...lastResult.stats, isMultiStore });
          return true;
        }
        const normalZeroOrders = !hasError && this._isNormalZeroOrders({ requireNotice: true });
        if (normalZeroOrders) {
          if (isMultiStore) {
            Utils.updateMultiStoreStoreResult('success', '0건');
          } else {
            Utils.showSuccessModal('데이터 없음', `${shopInfo.store_name ? shopInfo.store_name + ': ' : ''}추출할 주문이 없습니다.`, { status: 'success' });
          }
          return true;
        }
        if (hasError || this._hadRecentOrdersTransientError()) {
          const note = failureNote || '수집 중 끊김 - 빈 결과';
          this._lastOrdersCollectResult = { completed: false, partial: true, partialThrottle: true, note };
          Utils.updateProgressModal({ debug: `경고: ${note}` });
          if (isMultiStore) {
            Utils.updateMultiStoreStoreResult('warning', '수집 제한/불안정');
          } else {
            Utils.showSuccessModal('수집 제한/불안정', `${shopInfo.store_name ? shopInfo.store_name + ': ' : ''}반복 요청 제한 또는 페이지 불안정으로 빈 결과가 반환됐습니다.`, { status: 'warning' });
          }
          return false;
        }
        Utils.showSuccessModal('데이터 없음', `${shopInfo.store_name ? shopInfo.store_name + ': ' : ''}추출할 주문이 없습니다.`, { status: 'error' });
        return false;
      }

      // 취소 제외 매출액 합산 — 페이지 표시값과 동일 기준
      const collectedSales = allRows
        .filter(r => r.is_cancelled !== 'Y')
        .reduce((sum, r) => {
          const val = parseInt((r['매출액'] || '0').toString().replace(/,/g, ''), 10);
          return sum + (isNaN(val) ? 0 : val);
        }, 0);

      // 취소 제외 고유 주문 건수 — 페이지 표시 기준과 동일
      const collectedCount = new Set(allRows.filter(r => r.is_cancelled !== 'Y').map(r => r.order_id).filter(Boolean)).size;

      const salesMatch = expectedSales === 0 || collectedSales === expectedSales;
      const countMatch = expectedCount === 0 || collectedCount === expectedCount;

      // 현재 결과 보존 (ESC 대비)
      lastResult = { allRows, stats: { pageCount, hasError, expectedCount, expectedSales, collectedSales, salesMatch, collectedCount, countMatch, attempt } };

      const shouldPartialSave = !this._stopFlag && allRows.length > 0
        && (hasError || this._hadRecentOrdersTransientError())
        && (storeDeadlineExceeded || detailBlocked || this._isOrdersThrottleVisible() || this._hadRecentOrdersTransientError());
      if (shouldPartialSave) {
        if (this._checkOrdersHardThrottle()) return false;
        const noteBase = failureNote || (detailBlocked ? '수집 제한/경고 - 상세패널 차단' : storeDeadlineExceeded ? '수집 제한/경고 - 매장 예산 초과' : '수집 제한/경고 - 부분 저장 후 백필');
        const note = storeDeadlineExceeded
          ? `${noteBase} (page=${resumePage}, ext=${budgetExtensions}/${this.ORDERS_STORE_BUDGET_MAX_EXTENSIONS})`
          : noteBase;
        Utils.updateProgressModal({
          rowCount: allRows.length,
          message: `부분 수집 결과 저장 중...`,
          debug: `부분 저장 시작 - ${note} | rows=${allRows.length}, collected=${collectedCount}/${expectedCount || 'fail'}, sales=${collectedSales}/${expectedSales || 'fail'}`
        });
        await this._downloadOrdersResult(allRows, shopInfo, {
          pageCount, hasError: true, expectedCount, expectedSales, collectedSales, salesMatch,
          collectedCount, countMatch, attempt, isMultiStore, partial: true
        });
        this._lastOrdersCollectResult = { completed: false, partial: true, partialThrottle: true, storeDeadlineExceeded: !!storeDeadlineExceeded, detailBlocked: !!detailBlocked, note };
        Utils.updateProgressModal({
          rowCount: allRows.length,
          message: `부분 저장 완료 - 백필 이월`,
          debug: `부분 저장 완료 - 백필 이월 | ${note}`
        });
        return false;
      }

      // 매출액 일치 OR 최대 재시도 OR ESC → 저장
      // 건수 불일치는 재시도 없이 경고만 표시 (h1-txt와 실제 목록 불일치는 쿠팡 UI 이슈)
      if ((hasError || this._hadRecentOrdersTransientError()) && !this._stopFlag) {
        const note = failureNote || '수집 중 끊김 - 재조회 없이 백필';
        await this._downloadOrdersResult(allRows, shopInfo, {
          pageCount, hasError: true, expectedCount, expectedSales, collectedSales, salesMatch,
          collectedCount, countMatch, attempt, isMultiStore, partial: true
        });
        this._lastOrdersCollectResult = { completed: false, partial: true, partialThrottle: true, storeDeadlineExceeded: !!storeDeadlineExceeded, detailBlocked: !!detailBlocked, note };
        return false;
      }

      if (salesMatch || this._stopFlag) {
        await this._downloadOrdersResult(allRows, shopInfo, {
          pageCount, hasError, expectedCount, expectedSales, collectedSales, salesMatch,
          collectedCount, countMatch, attempt, isMultiStore
        });
        this._lastOrdersCollectResult = { completed: true, partial: false, note: '완료' };
        return true;
      }

      Utils.updateProgressModal({
        debug: `매출액 불일치 (수집: ${collectedSales.toLocaleString()}원 != 기대: ${expectedSales.toLocaleString()}원) - 재조회 없이 현재 결과 저장`
      });
      if (!countMatch) {
        Utils.updateProgressModal({ debug: `ℹ️ 주문 건수: ${collectedCount}건 vs 기대: ${expectedCount}건 (경고만, 재시도 없음)` });
      }

      await this._downloadOrdersResult(allRows, shopInfo, {
        pageCount, hasError, expectedCount, expectedSales, collectedSales, salesMatch,
        collectedCount, countMatch, attempt, isMultiStore
      });
      this._lastOrdersCollectResult = { completed: true, partial: false, note: '완료(검증 불일치)' };
      return true;
    }
    return false;
  },

  async _downloadOrdersResult(allRows, shopInfo, stats) {
    const { pageCount, hasError, expectedCount, expectedSales, collectedSales, salesMatch,
            collectedCount, countMatch, attempt, isMultiStore, partial } = stats;

    const headers = [
      'collected_at', 'store_id', 'store_name', 'order_date', 'order_id',
      'delivery_type', 'order_status', 'order_summary', 'total_price', 'is_cancelled',
      'menu_name', 'menu_qty', 'menu_price', 'menu_options',
      '매출액', '상점부담_쿠폰', '중개_이용료', '결제대행사_수수료',
      '배달비', '광고비', '부가세', '즉시할인금액', '정산_예정_금액', '취소금액'
    ];

    const csv = Utils.toCSV(headers, allRows);
    const filename = await Utils.downloadCSV(csv, {
      channel: 'coupangeats',
      purpose: 'orders',
      storeName: shopInfo.store_name,
      storeId: shopInfo.store_id,
      dateStr: this._ordersDownloadDateStr()
    });

    const uniqueOrders = new Set(allRows.map(r => r.order_id)).size;
    const cancelledOrders = new Set(
      allRows.filter(r => r.is_cancelled === 'Y').map(r => r.order_id)
    ).size;

    let status = 'success';
    let statusMsg = '';
    if (this._stopFlag) {
      status = 'warning';
      statusMsg = '(ESC 중단됨)';
    } else if (hasError) {
      status = 'warning';
      statusMsg = '(페이지 로딩 타임아웃 또는 빈 페이지)';
    }

    const cancelledLine = cancelledOrders > 0 ? `\n  - 취소 ${cancelledOrders}건 포함` : '';

    if (partial) {
      Utils.updateProgressModal({ debug: `✅ 저장 완료(부분): ${filename}` });
    }

    const displayCount = collectedCount ?? uniqueOrders;
    let mismatchLine = '';
    if (!this._stopFlag && !countMatch && expectedCount > 0) {
      status = 'warning';
      mismatchLine += `\n\n⚠️ 주문 건수 불일치 (${attempt}회 시도 후)\n  페이지: ${expectedCount}건 | 수집: ${displayCount}건`;
    }
    if (!this._stopFlag && !salesMatch && expectedSales > 0) {
      status = 'warning';
      mismatchLine += `\n\n⚠️ 매출액 불일치 (${attempt}회 시도 후)\n  페이지: ${expectedSales.toLocaleString()}원 | 수집: ${collectedSales.toLocaleString()}원`;
    }

    const details = `📊 수집 결과:
  - ${pageCount}페이지 처리
  - ${uniqueOrders}건 주문${cancelledLine}
  - 총 ${allRows.length.toLocaleString()}행

  📁 파일명:
  ${filename}${mismatchLine}`;

    // 로그 결과 요약 추가 후 저장
    Utils.appendLog('=== 수집 결과 ===');
    Utils.appendLog(`매장: ${shopInfo.store_name || '-'} | 시도: ${attempt}회`);
    Utils.appendLog(`페이지: ${pageCount} | 행: ${allRows.length} | 고유 주문: ${uniqueOrders}건`);
    Utils.appendLog(`매출액: ${collectedSales.toLocaleString()}원 (기대: ${expectedSales > 0 ? expectedSales.toLocaleString() + '원' : '미확인'}) ${salesMatch ? '✅' : '❌'}`);
    Utils.appendLog(`주문건수: ${displayCount}건 (기대: ${expectedCount > 0 ? expectedCount + '건' : '미확인'}) ${countMatch ? '✅' : '❌'}`);
    if (statusMsg) Utils.appendLog(`상태: ${statusMsg}`);

    if (isMultiStore) {
      // 다중매장 모드: 해당 매장 섹션에 결과 표시
      const countInfo = collectedCount != null ? `${collectedCount}건` : `${uniqueOrders}건`;
      const salesInfo = collectedSales > 0 ? ` / ${collectedSales.toLocaleString()}원` : '';
      const warnMark = status !== 'success' ? ' ⚠️' : '';
      Utils.updateMultiStoreStoreResult(status, `${countInfo}${salesInfo} / ${allRows.length}행${warnMark}`);
      if (mismatchLine) {
        mismatchLine.split('\n').filter(l => l.trim()).forEach(l =>
          Utils.updateProgressModal({ debug: l.trim() })
        );
      }
    } else {
      Utils.showSuccessModal(`쿠팡이츠 수집 완료 ${statusMsg}`, details, { status });
    }
  },

  // ===================================================================================
  // ── 메뉴 / 옵션 수집 ──
  // ===================================================================================

  async _collectMenuMultiStore(opts = {}) {
    const _sendMenuComplete = (storeCount, completedCount, stopped, error, storeResults = []) => {
      if (window.top !== window.self) return;
      try {
        chrome.runtime.sendMessage({
          type: 'MULTISTORE_COMPLETE',
          payload: { category: 'menu', storeCount, completedCount, stopped: !!stopped, error: !!error, storeResults }
        }).catch(() => {});
      } catch (_) {}
    };

    const matchingStores = this._filterTargetStores(await this._getOrdersDropdownStores(), opts);
    const currentStore = this._findCurStoreName() || document.querySelector('.dropdown-btn.highlight')?.textContent.trim() || '현재 매장';
    const storesToCollect = matchingStores.length > 0
      ? matchingStores
      : (this._isTargetRequested(currentStore, opts) ? [currentStore] : []);

    Utils.showProgressModal('메뉴/옵션 수집', `${storesToCollect.length}개 매장 수집 시작`);

    const today = Utils.getTodayStr();
    let completedCount = 0;
    const storeResults = [];

    for (const storeName of storesToCollect) {
      if (this._stopFlag) break;

      Utils.updateProgressModal({ debug: `→ ${storeName}` });

      if (matchingStores.length > 1) {
        const selected = await this._selectStoreInOrdersDropdown(storeName);
        if (!selected) {
          Utils.updateProgressModal({ debug: `❌ 매장 선택 실패: ${storeName}` });
          storeResults.push({ storeName, completed: false, status: 'fail', error: true, note: '매장 선택 실패' });
          continue;
        }
        await this._randomDelay(400, 600);
      }

      // ── 메뉴 탭 수집 ──
      await this._ensureMenuTab('메뉴');
      const menuRows = this._scrapeMenuItems(storeName);
      Utils.updateProgressModal({ debug: `📋 메뉴 ${menuRows.length}행` });
      if (menuRows.length > 0) {
        const csv = this._rowsToCsv(
          ['매장명', '카테고리', '메뉴명', '가격', '노출여부'],
          menuRows.map(r => [r.storeName, r.category, r.name, r.price, r.visible])
        );
        await Utils.downloadCSV(csv, { channel: 'coupangeats', purpose: 'menu', storeName, dateStr: today });
        Utils.updateProgressModal({ debug: `✅ 메뉴 저장 완료` });
      } else {
        Utils.updateProgressModal({ debug: `⚠️ 메뉴 데이터 없음` });
      }

      // ── 옵션 탭 수집 ──
      await this._ensureMenuTab('옵션');
      const optRows = this._scrapeOptionItems(storeName);
      Utils.updateProgressModal({ debug: `📋 옵션 ${optRows.length}행` });
      if (optRows.length > 0) {
        const csv = this._rowsToCsv(
          ['매장명', '옵션그룹', '적용메뉴', '옵션명', '옵션가격', '노출여부'],
          optRows.map(r => [r.storeName, r.group, r.applies, r.name, r.price, r.visible])
        );
        await Utils.downloadCSV(csv, { channel: 'coupangeats', purpose: 'options', storeName, dateStr: today });
        Utils.updateProgressModal({ debug: `✅ 옵션 저장 완료` });
      } else {
        Utils.updateProgressModal({ debug: `⚠️ 옵션 데이터 없음` });
      }

      completedCount++;
      storeResults.push({ storeName, completed: true, status: 'ok', note: '완료' });
    }

    _sendMenuComplete(storesToCollect.length, completedCount, this._stopFlag, false, storeResults);

    if (opts.source !== 'batch') {
      Utils.showSuccessModal('메뉴/옵션 수집 완료', `${completedCount}/${storesToCollect.length}개 매장 수집 완료`);
    }
  },

  async _ensureMenuTab(tabText) {
    const tabs = [...document.querySelectorAll('.tab')];
    const target = tabs.find(t => t.textContent.trim() === tabText);
    if (!target) return;
    if (!target.classList.contains('is-active')) {
      target.click();
      await this._randomDelay(400, 600);
    }
  },

  _scrapeMenuItems(storeName) {
    const rows = [];
    const sections = document.querySelectorAll('div[data-testid^="menu-content-"]');
    for (const section of sections) {
      const category = section.querySelector('.menu-name')?.textContent.trim() || '';
      for (const item of section.querySelectorAll('ul.css-1xc74i7 li')) {
        const name = item.querySelector('.dish-name')?.textContent.trim() || '';
        const priceText = item.querySelector('.sale-price')?.textContent.trim() || '';
        const price = parseInt(priceText.replace(/[^0-9]/g, ''), 10) || 0;
        const visible = (item.classList.contains('not-on-sale') || item.classList.contains('dish_not-expose')) ? 'N' : 'Y';
        if (!name) continue;
        rows.push({ storeName, category, name, price, visible });
      }
    }
    return rows;
  },

  _scrapeOptionItems(storeName) {
    const rows = [];
    for (const group of document.querySelectorAll('.css-zd37k8')) {
      const titleEls = [...group.querySelectorAll('.group-item-title')];
      // 첫 번째 .group-item-title 이 그룹명, description 클래스인 것이 적용메뉴
      const groupName = titleEls.find(el => !el.classList.contains('description'))?.textContent.trim() || '';
      const appliesText = group.querySelector('.group-item-title.description span')?.textContent.trim() || '';
      for (const item of group.querySelectorAll('.css-192jtcb')) {
        const name = item.querySelector('.option-item-title span.high-light-wrapper-v2')?.textContent.trim() || '';
        const priceText = item.querySelector('.option-item-title.description')?.textContent.trim() || '';
        const price = parseInt(priceText.replace(/[^0-9]/g, ''), 10) || 0;
        const visible = item.classList.contains('option_not-expose') ? 'N' : 'Y';
        if (!name) continue;
        rows.push({ storeName, group: groupName, applies: appliesText, name, price, visible });
      }
    }
    return rows;
  },

  _rowsToCsv(headers, rows) {
    const esc = v => {
      const s = String(v ?? '');
      return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
    };
    return [headers.join(','), ...rows.map(r => r.map(esc).join(','))].join('\n');
  },

};
