// ===================================================================================
// ================= 03_coupangeats.js : 쿠팡이츠 수집 로직 =================
// ===================================================================================

Sites['coupangeats'] = {
  name: '쿠팡이츠',
  match: (url) => url.includes('store.coupangeats.com') || url.includes('advertising.coupangeats.com'),
  _stopFlag: false,
  
  async collect() {
    this._stopFlag = false;
    this._setupEscListener();
    
    const shopInfo = this._getShopInfo();
    
    const isCMGPage = location.hostname.includes('advertising.coupangeats.com') 
                   || document.querySelector('WUJIE-APP')
                   || location.href.includes('/merchant/management/cmg');
    
    if (location.pathname.includes('/coupons/detail/')) {
      this._collectCouponStats(shopInfo);
    } else if (isCMGPage) {
      await this._collectCMGMultiStore();
    } else if (location.pathname.includes('/orders/')) {
      await this._collectOrdersMultiStore();
    } else {
      Utils.showError('미지원 페이지', '주문관리, 쿠폰상세 또는 CMG 페이지에서 실행해주세요.');
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

  async _getWujieDoc(timeoutMs = 15000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const w = document.querySelector('WUJIE-APP');
      if (w && w.shadowRoot) return w.shadowRoot;
      await new Promise(r => setTimeout(r, 200));
    }
    location.reload();
    await new Promise(r => setTimeout(r, 3000));
    const w2 = document.querySelector('WUJIE-APP');
    if (w2 && w2.shadowRoot) return w2.shadowRoot;
    return null;
  },

  async _waitForCMGTableData(doc, timeoutMs = 1500) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const rows = doc.querySelectorAll('tbody.table__tbody tr.table__row, tr.table__row');
      if (rows.length > 0) return;
      await new Promise(r => setTimeout(r, 100));
    }
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

  // ✅ 완전히 재작성된 CMG 수집 함수
  async _collectCMGMultiStore() {
    Utils.updateProgressModal({ debug: 'WUJIE-APP shadowRoot 대기...' });
    const doc = await this._getWujieDoc(15000);
    if (!doc) {
      Utils.updateProgressModal({ debug: '❌ WUJIE shadowRoot 미준비 — CMG 수집 중단' });
      return;
    }
    const shopInfo = this._getShopInfo();
    const cmgStart = Date.now();
    const CMG_TOTAL_TIMEOUT = 5 * 60 * 1000;

    // 도리당/나홀로 매장 목록 읽기
    Utils.showProgressModal('CMG 수집', '매장 목록 확인 중...');
    const matchingStores = await this._getCMGMatchingStores(doc);
    Utils.updateProgressModal({ debug: `드롭다운 탐색 결과: ${matchingStores.length}개 (${matchingStores.join(', ') || '없음'})` });
    if (matchingStores.length === 0) {
      // 셀렉터 실패 시 현재 단일 매장으로 진행 (전체 매장이면 경고)
      const curStore = doc.querySelector('.dropdown-placeholder > span')?.textContent.trim() || '';
      if (curStore.includes('전체 매장') || !curStore) {
        Utils.showError('매장 선택 필요', '드롭다운에서 도리당/나홀로 매장을 찾지 못했습니다.\n개별 매장을 직접 선택 후 다시 실행해주세요.');
        return;
      }
      Utils.updateProgressModal({ debug: `단일매장 모드: ${curStore}` });
      await this._collectCMGData(shopInfo);
      return;
    }

    // 캘린더에서 날짜 범위 읽기
    const dateRange = this._readCMGDateRange(doc);
    if (!dateRange) {
      Utils.updateProgressModal({ debug: '⚠️ 날짜 범위 읽기 실패 → 단일매장 프롬프트 방식' });
      await this._collectCMGData(shopInfo);
      return;
    }

    // 날짜 목록 생성 (오늘/미래 → 어제로 clamp)
    const yd = new Date(); yd.setDate(yd.getDate() - 1); yd.setHours(0, 0, 0, 0);
    let { startDate, endDate } = dateRange;
    if (startDate > yd) startDate = new Date(yd);
    if (endDate > yd) endDate = new Date(yd);

    const dates = [];
    const cur = new Date(startDate);
    while (cur <= endDate) { dates.push(new Date(cur)); cur.setDate(cur.getDate() + 1); }

    // 도리당 먼저 정렬
    matchingStores.sort((a, b) => (a.includes('도리당') ? 0 : 1) - (b.includes('도리당') ? 0 : 1));

    // 체크포인트 로드
    const today = Utils.getTodayStr();
    const checkpoint = await this._loadCMGCheckpoint();
    const completedStores = (checkpoint?.date === today) ? (checkpoint.completedStores || []) : [];

    Utils.showMultiStoreProgressModal(matchingStores);
    Utils.updateProgressModal({ debug: `📅 수집 기간: ${this._formatDate(startDate)} ~ ${this._formatDate(endDate)} (${dates.length}일)` });
    if (completedStores.length > 0) {
      Utils.updateProgressModal({ debug: `📌 오늘 완료된 매장: ${completedStores.join(', ')} → 건너뜀` });
    }

    for (let i = 0; i < matchingStores.length; i++) {
      if (this._stopFlag) break;
      if (Date.now() - cmgStart > CMG_TOTAL_TIMEOUT) {
        Utils.updateProgressModal({ debug: '⏰ CMG 5분 초과 → 강제 종료' });
        break;
      }

      Utils.setMultiStoreIndex(i);
      const storeName = matchingStores[i];

      if (completedStores.includes(storeName)) {
        Utils.updateProgressModal({ debug: `⏭️ 오늘 이미 완료됨, 건너뜀` });
        Utils.updateMultiStoreStoreResult('success', '⏭️ 오늘 이미 수집됨');
        continue;
      }

      Utils.updateProgressModal({ debug: `→ ${storeName} 선택 시도` });
      const selected = await this._selectCMGStore(doc, storeName);
      if (!selected) {
        Utils.updateProgressModal({ debug: `❌ 매장 선택 실패` });
        Utils.updateMultiStoreStoreResult('error', '매장 선택 실패');
        continue;
      }
      Utils.updateProgressModal({ debug: `✅ 매장 선택 완료, 자동조회 대기` });

      const allRows = await this._collectCMGForStore(doc, storeName, dates, cmgStart);

      if (!allRows.length) {
        Utils.updateMultiStoreStoreResult('warning', `데이터 없음`);
        continue;
      }

      const headers = [
        'collected_at', '조회일자', '매장명', '광고상태', '광고기간', '광고비율',
        '광고비용', '신규고객', '광고주문수', '광고매출', '전체매출', '광고클릭수', '광고노출수', 'note'
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

      if (!this._stopFlag) {
        completedStores.push(storeName);
        await this._saveCMGCheckpoint({ date: today, completedStores: [...completedStores] });
        Utils.updateProgressModal({ debug: `💾 체크포인트 저장 (${completedStores.length}/${matchingStores.length})` });
      }
    }

    if (!this._stopFlag) await this._clearCMGCheckpoint();
    Utils.finalizeMultiStoreModal();
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
  async _waitForCMGDropdownItems(doc, timeoutMs = 2000) {
    const selectors = [
      'li.ant-dropdown-menu-item.store-item',  // CMG 전용 클래스
      'li.ant-dropdown-menu-item',
      '[role="menuitem"]',
    ];
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      for (const sel of selectors) {
        const inDoc = [...document.querySelectorAll(sel)];
        if (inDoc.length > 0) return inDoc;
        const inShadow = [...doc.querySelectorAll(sel)];
        if (inShadow.length > 0) return inShadow;
      }
      await new Promise(r => setTimeout(r, 50));
    }
    return [];
  },

  async _getCMGMatchingStores(doc) {
    const trigger = doc.querySelector('form.cmgeats-store-dropdown');
    if (!trigger) return [];

    trigger.click();
    const items = await this._waitForCMGDropdownItems(doc, 2000);

    const matching = items
      .map(el => el.textContent.trim())
      .filter(t => t.includes('도리당') || t.includes('나홀로'));

    // 드롭다운 닫기 — 트리거 재클릭 (ESC dispatch는 stopFlag 오작동 유발)
    trigger.click();
    await new Promise(r => setTimeout(r, 200));
    return matching;
  },

  async _selectCMGStore(doc, storeText) {
    const trigger = doc.querySelector('form.cmgeats-store-dropdown');
    if (!trigger) return false;

    trigger.click();
    const items = await this._waitForCMGDropdownItems(doc, 2000);

    const target = items.find(el => {
      const t = el.textContent.trim();
      return t === storeText || t.includes(storeText) || storeText.includes(t);
    });
    if (!target) {
      trigger.click(); // 드롭다운 닫기
      return false;
    }

    target.click(); // li 클릭 → 자동 선택 + 드롭다운 닫힘
    await this._randomDelay(1500, 2000);
    return true;
  },

  async _collectCMGForStore(doc, storeName, dates, cmgStart = Date.now()) {
    const CMG_STORE_TIMEOUT = 4 * 60 * 1000;
    const allRows = [];
    const iso = new Date().toISOString();

    for (let i = 0; i < dates.length; i++) {
      if (this._stopFlag) break;
      if (Date.now() - cmgStart > CMG_STORE_TIMEOUT) {
        Utils.updateProgressModal({ debug: `⏰ CMG 4분 초과 — 조기 종료 (${i}/${dates.length}일 완료)` });
        break;
      }

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
        await this._randomDelay(1000, 1500);

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

      await this._waitForCMGTableData(doc, 1500);

      const pageRows = this._collectCurrentCMGData(doc, iso, dateStr, storeName);
      const annotated = pageRows.map(row => {
        const cost   = Number(String(row['광고비용']  || '0').replace(/,/g, '')) || 0;
        const sales  = Number(String(row['광고매출']  || '0').replace(/,/g, '')) || 0;
        const orders = Number(String(row['광고주문수'] || '0').replace(/,/g, '')) || 0;
        return (cost === 0 && sales === 0 && orders === 0)
          ? { ...row, note: 'CMG_0원_확인필요' }
          : row;
      });
      Utils.updateProgressModal({ rowCount: allRows.length + annotated.length, debug: `${dateStr}: ${annotated.length}행 수집` });
      allRows.push(...annotated);

      await this._randomDelay(500, 1000);
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

  async _collectCMGData(shopInfo) {
    Utils.updateProgressModal({ debug: 'WUJIE-APP shadowRoot 대기...' });
    const doc = await this._getWujieDoc(15000);
    if (!doc) {
      Utils.updateProgressModal({ debug: '❌ WUJIE shadowRoot 미준비 — CMG 수집 중단' });
      return;
    }
    
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
    
    // 날짜 입력 받기
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
    
    const parseResult = this._parseDateRange(rangeInput);
    console.log('[CMG-DEBUG] parseResult:', JSON.stringify(parseResult, null, 2), 'raw:', parseResult);
    
    if (parseResult.error) {
      Utils.showError('형식 오류', parseResult.error);
      return;
    }
    
    let startDate = parseResult.startDate;
    let endDate = parseResult.endDate;
    
    if (!startDate || !endDate) {
      Utils.showError('날짜 오류 [03.js]', `start=${startDate}, end=${endDate}, keys=${Object.keys(parseResult)}`);
      return;
    }

    // 오늘/미래 날짜 → 어제로 clamp, 역순이면 swap
    const yd = new Date();
    yd.setDate(yd.getDate() - 1);
    yd.setHours(0, 0, 0, 0);
    if (startDate > yd) { startDate = new Date(yd); }
    if (endDate > yd) { endDate = new Date(yd); }
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
        await this._randomDelay(1000, 1500);
        
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
      await this._waitForCMGTableData(doc, 1500);

      // 현재 페이지 데이터 수집 (매장 필터링 포함)
      const pageRows = this._collectCurrentCMGData(doc, iso, dateStr, selectedStore);
      const annotated = pageRows.map(row => {
        const cost   = Number(String(row['광고비용']  || '0').replace(/,/g, '')) || 0;
        const sales  = Number(String(row['광고매출']  || '0').replace(/,/g, '')) || 0;
        const orders = Number(String(row['광고주문수'] || '0').replace(/,/g, '')) || 0;
        return (cost === 0 && sales === 0 && orders === 0)
          ? { ...row, note: 'CMG_0원_확인필요' }
          : row;
      });
      Utils.updateProgressModal({
        rowCount: allRows.length + annotated.length,
        debug: `${dateStr}: ${annotated.length}행 수집 완료`
      });

      allRows.push(...annotated);
      
      await this._randomDelay(500, 1000);
    }
    
    if (!allRows.length) {
      Utils.showSuccessModal('데이터 없음', 
        `선택 매장: ${selectedStore}\n수집된 데이터가 없습니다.\n\n매장명이 테이블과 일치하는지 확인해주세요.`, 
        { status: 'error' });
      return;
    }
    
    const headers = [
      'collected_at', '조회일자', '매장명', '광고상태', '광고기간', '광고비율',
      '광고비용', '신규고객', '광고주문수', '광고매출', '전체매출', '광고클릭수', '광고노출수', 'note'
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
    
    await this._randomDelay(200, 300);

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
    await this._randomDelay(400, 600);
    
    // 종료일 클릭 (같은 날짜)
    Utils.updateProgressModal({ debug: `${day}일 클릭 (종료)` });
    selectedDateCell.click();
    await this._randomDelay(400, 600);
    
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
        await this._randomDelay(800, 1200);
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
          await this._randomDelay(800, 1200);
          break;
        }
      }
    }
    
    if (!applyClicked) {
      Utils.updateProgressModal({ debug: '⚠️ 적용 버튼 못찾음' });
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
          await this._randomDelay(200, 300);
        } else {
          return true;
        }
      } else {
        const nextBtn = doc.querySelector('.ant-calendar-next-month-btn, button.ant-calendar-next-month-btn');
        if (nextBtn) {
          nextBtn.click();
          await this._randomDelay(200, 300);
        } else {
          return true;
        }
      }
    }
    
    return true;
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
    const activeBtn = document.querySelector('button.active');
    const pageNum = parseInt(activeBtn?.textContent.trim() || '0', 10);
    return isNaN(pageNum) ? 1 : pageNum;
  },

  _clickNextPage() {
    const nextBtn = document.querySelector('button[data-at="next-btn"]');
    if (nextBtn && !nextBtn.classList.contains('hide-btn')) {
      nextBtn.click();
      return true;
    }
    return false;
  },

  _getExpectedOrderCount() {
    // 단위가 "건"인 .order-unit-price만 찾기 (매출액의 "원"은 제외)
    const allUnitEls = document.querySelectorAll('.h1-txt .order-unit-price');
    for (const unitEl of allUnitEls) {
      if (unitEl.textContent.trim() === '건') {
        const countEl = unitEl.previousElementSibling;
        // 콤마 제거 후 파싱 (예: "1,234" → 1234)
        const num = parseInt((countEl?.textContent.trim() || '0').replace(/,/g, ''), 10);
        if (!isNaN(num) && num > 0) return num;
      }
    }
    return 0;
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
    const orderSection = firstItem.querySelector('.order-item');
    const cols = orderSection?.querySelectorAll('[class*="col-4"]');
    return cols?.[0]?.childNodes[0]?.textContent.trim() || '';
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
    const modalSelectors = [
      '.modal.show',
      '.ant-modal',
      '[role="dialog"]',
      '.popup-wrapper'
    ];
    
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
    return lastCount > 0; // 최소한 주문이 있으면 true
  },

  async _collectCurrentPageData(shopInfo, iso, currentPage) {
    // 페이지 안정화 대기
    await this._waitForStableState();
    
    const orderItems = document.querySelectorAll('.order-search-result-content > li.col-12');
    
    // 주문이 없으면 재시도
    if (orderItems.length === 0) {
      Utils.updateProgressModal({ 
        debug: `⚠️ ${currentPage}페이지 주문 없음 - 1초 후 재시도` 
      });
      
      await this._randomDelay(1000, 1500);
      await this._closeErrorPopups();
      
      const retryItems = document.querySelectorAll('.order-search-result-content > li.col-12');
      if (retryItems.length === 0) {
        Utils.updateProgressModal({ 
          debug: `❌ ${currentPage}페이지 재시도 실패 - 빈 페이지로 반환` 
        });
        return [];
      }
      
      Utils.updateProgressModal({ 
        debug: `✅ 재시도 성공 - ${retryItems.length}개 주문 발견` 
      });
    }
    
    const pageRows = [];
    const totalItems = orderItems.length;

    for (let i = 0; i < totalItems; i++) {
      if (this._stopFlag) break;
      
      Utils.updateProgressModal({
        message: `${currentPage}페이지 수집 중... (${i + 1}/${totalItems}건)`
      });
      
      const item = orderItems[i];
      
      try {
        // 주문 펼치기 전에 팝업 체크
        await this._closeErrorPopups();
        
        const expandIcon = item.querySelector('.order-expand-btn .icon-ce-arrowdown-bold');
        if (expandIcon) {
          expandIcon.closest('button').click();
          await this._randomDelay(400, 600);
          
          // 펼치기 후 팝업 재확인
          await this._closeErrorPopups();
          
          // 상세 정보가 로드되었는지 확인
          let detailLoaded = false;
          for (let retry = 0; retry < 5; retry++) {
            const detailSection = item.querySelector('.order-details');
            if (detailSection && detailSection.querySelector('.order-detail-item')) {
              detailLoaded = true;
              break;
            }
            await this._randomDelay(200, 300);
          }
          
          if (!detailLoaded) {
            Utils.updateProgressModal({ 
              debug: `⚠️ 주문 ${i + 1} 상세 로딩 실패 - 재시도` 
            });
            
            // 재시도
            const retryIcon = item.querySelector('.order-expand-btn .icon-ce-arrowdown-bold');
            if (retryIcon) {
              retryIcon.closest('button').click();
              await this._randomDelay(600, 800);
            }
          }
        }
        
        const orderSection = item.querySelector('.order-item');
        const detailSection = item.querySelector('.order-details');
        
        // 주문 기본 정보가 없으면 스킵
        if (!orderSection) {
          Utils.updateProgressModal({ 
            debug: `⚠️ 주문 ${i + 1} 기본 정보 없음 - 스킵` 
          });
          continue;
        }
        
        const dateEl = orderSection.querySelector('.order-date:not(.d-md-none) span:first-child');
        const timeEl = orderSection.querySelector('.order-date .gray-txt');
        const order_date = (dateEl?.textContent.trim().replace(/\s+/g, '') || '') + ' ' + (timeEl?.textContent.trim() || '');
        
        const cols = orderSection.querySelectorAll('[class*="col-4"]');
        const order_id = cols[0]?.childNodes[0]?.textContent.trim() || '';
        
        const deliveryType = orderSection.querySelector('.delivery-type')?.textContent.trim() || '';
        
        const priceEl = orderSection.querySelector('.order-price > span:first-child');
        const statusEl = orderSection.querySelector('.label-order-item');
        const total_price = Utils.cleanPrice(priceEl?.textContent || '');
        const order_status = statusEl?.textContent.trim() || '';
        
        const isCancelled = !!detailSection?.querySelector('.total-cancelled-order');

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
            
            const rowData = {
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
            };
            pageRows.push(rowData);
            isFirstRow = false;
            isFirstOptionOfMenu = false;
          }
        }

        if (allMenuData.length === 0) {
          pageRows.push({ 
            ...base, 
            menu_name: '', 
            menu_qty: '', 
            menu_price: '', 
            menu_options: '' 
          });
        }
        
      } catch (err) {
        Utils.updateProgressModal({ 
          debug: `❌ 주문 ${i + 1} 수집 오류: ${err.message}` 
        });
        console.error(`[쿠팡이츠] 주문 ${i + 1} 수집 오류:`, err);
        
        // 오류 발생 시 팝업 닫기
        await this._closeErrorPopups();
        
        // 다음 주문으로 계속
        continue;
      }
    }

    return pageRows;
  },

  async _collectOrdersAllPages(shopInfo) {
    await this._collectOrdersWithValidation(shopInfo);
  },

  async _collectOrdersMultiStore() {
    const matchingStores = await this._getOrdersDropdownStores();

    if (matchingStores.length === 0) {
      const shopInfo = this._getShopInfo();
      await this._collectOrdersWithValidation(shopInfo, false);
      return;
    }

    // 도리당 먼저, 나홀로 나중
    matchingStores.sort((a, b) => {
      return (a.includes('도리당') ? 0 : 1) - (b.includes('도리당') ? 0 : 1);
    });

    // 오늘 완료된 매장 체크포인트 로드
    const today = Utils.getTodayStr();
    const checkpoint = await this._loadOrdersCheckpoint();
    const completedStores = (checkpoint?.date === today) ? (checkpoint.completedStores || []) : [];

    Utils.showMultiStoreProgressModal(matchingStores);

    if (completedStores.length > 0) {
      Utils.updateProgressModal({ debug: `📌 오늘 이미 완료된 매장: ${completedStores.join(', ')} → 건너뜀` });
    }

    for (let i = 0; i < matchingStores.length; i++) {
      if (this._stopFlag) break;

      Utils.setMultiStoreIndex(i);
      const storeName = matchingStores[i];

      // 오늘 이미 수집 완료된 매장 스킵
      if (completedStores.includes(storeName)) {
        Utils.updateProgressModal({ debug: `⏭️ 오늘 이미 수집 완료됨, 건너뜀` });
        Utils.updateMultiStoreStoreResult('success', '⏭️ 오늘 이미 수집됨');
        continue;
      }

      Utils.updateProgressModal({ debug: `→ 매장 선택 시도` });

      const selected = await this._selectStoreInOrdersDropdown(storeName);
      if (!selected) {
        Utils.updateProgressModal({ debug: `❌ 매장 선택 실패` });
        Utils.updateMultiStoreStoreResult('error', '매장 선택 실패');
        continue;
      }
      Utils.updateProgressModal({ debug: `✅ 매장 선택 완료` });

      // 주문일 필터를 어제~어제로 설정
      Utils.updateProgressModal({ debug: `📅 주문일 설정 중 (어제~어제)` });
      const dateSet = await this._setOrdersDateToYesterday();
      Utils.updateProgressModal({ debug: dateSet ? `✅ 주문일 설정 완료` : `⚠️ 주문일 설정 실패, 기존 날짜로 진행` });
      await this._randomDelay(300, 500);

      const prevCount = this._getExpectedOrderCount();
      Utils.updateProgressModal({ debug: `📊 prevCount = ${prevCount}건` });

      let searched = false;
      for (let r = 0; r < 3 && !searched; r++) {
        Utils.updateProgressModal({ debug: `🔍 조회 버튼 클릭 (${r + 1}차)` });
        if (r > 0) await this._randomDelay(400, 600);
        searched = await this._clickOrdersSearchButton();
      }
      if (!searched) {
        Utils.updateProgressModal({ debug: `❌ 조회 버튼 클릭 실패, 건너뜀` });
        Utils.updateMultiStoreStoreResult('error', '조회 버튼 클릭 실패');
        continue;
      }
      Utils.updateProgressModal({ debug: `✅ 조회 버튼 클릭 성공` });

      Utils.updateProgressModal({ debug: `⏳ 데이터 로드 대기 중 (${prevCount}건 → 변경 대기)` });
      await this._waitForOrdersCountChange(prevCount, 8000);
      const dataLoaded = await this._waitForOrdersDataReady(8000);
      const newCount = this._getExpectedOrderCount();
      Utils.updateProgressModal({ debug: `✅ 데이터 로드 완료 (${newCount}건)` });

      // 무한루프 감지: 타임아웃(dataLoaded=false)이고 0건인 경우만 F5 재시작
      // (주문 0건인 정상 상황과 구분)
      if (!dataLoaded && newCount === 0 && prevCount === 0) {
        Utils.updateProgressModal({ debug: `⚠️ 로딩 무한루프 감지 → 체크포인트 저장 후 자동 새로고침` });
        await chrome.storage.local.set({ ce_orders_restart: { date: today, url: location.href } });
        await this._randomDelay(1000, 1500);
        location.reload();
        return;
      }

      const shopInfo = this._getShopInfo();
      await this._collectOrdersWithValidation(shopInfo, true);

      // 매장 수집 완료 → 체크포인트 갱신
      if (!this._stopFlag) {
        completedStores.push(storeName);
        await this._saveOrdersCheckpoint({ date: today, completedStores: [...completedStores] });
        Utils.updateProgressModal({ debug: `💾 체크포인트 저장 (완료: ${completedStores.length}/${matchingStores.length}개)` });
      }
    }

    // 모든 매장 완료 → 체크포인트 삭제
    if (!this._stopFlag) {
      await this._clearOrdersCheckpoint();
    }

    Utils.finalizeMultiStoreModal();
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

  async _getOrdersDropdownStores() {
    const toggleBtn = document.querySelector('.dropdown-btn.highlight');
    if (!toggleBtn) return [];

    toggleBtn.click();
    await this._randomDelay(300, 500);

    const items = [...document.querySelectorAll('.dropdown-list li a')]
      .map(el => el.textContent.trim())
      .filter(t => t.includes('도리당') || t.includes('나홀로'));

    // 드롭다운 닫기
    toggleBtn.click();
    await this._randomDelay(200, 300);

    return items;
  },

  async _selectStoreInOrdersDropdown(storeText) {
    const toggleBtn = document.querySelector('.dropdown-btn.highlight');
    if (!toggleBtn) return false;

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
    const target = items.find(el => {
      const t = el.textContent.trim();
      return t === storeText || t.includes(storeText) || storeText.includes(t);
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
    const selectors = [
      'button.button--defaultOutlined',
      'button.css-casqo8',
      'button.e4pgcj01'
    ];

    for (const sel of selectors) {
      const btns = document.querySelectorAll(sel);
      for (const btn of btns) {
        if (btn.querySelector('svg') && !btn.disabled && btn.offsetParent !== null) {
          btn.click();
          await this._randomDelay(300, 500);
          return true;
        }
      }
    }

    // 최후 폴백: 돋보기 SVG path로 찾기
    for (const btn of document.querySelectorAll('button')) {
      const path = btn.querySelector('svg path');
      if (path && path.getAttribute('d')?.includes('M3.11 3.608') && !btn.disabled && btn.offsetParent !== null) {
        btn.click();
        await this._randomDelay(300, 500);
        return true;
      }
    }

    return false;
  },

  async _setOrdersDateToYesterday() {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
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

    const verifyYesterdayRange = () => {
      const inputs = getVisibleDateInputs();
      const shown = inputs.slice(0, 2).map(el => (el.textContent || '').trim());
      const ok = shown.length >= 2 && shown[0].includes(visibleLabel) && shown[1].includes(visibleLabel);
      if (!ok) {
        Utils.updateProgressModal({ debug: `WARN order date mismatch: ${shown.join(' / ') || 'none'} (expected ${visibleLabel})` });
      }
      return ok;
    };

    const trigger = document.querySelector('div.e4pgcj08');
    if (!trigger) return false;
    trigger.click();
    await this._randomDelay(400, 600);

    const waitForDayPicker = async () => {
      const start = Date.now();
      while (Date.now() - start < 2000) {
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

    const clickLastYesterdayCell = async () => {
      let cells = [...document.querySelectorAll(`div.DayPicker-Day[aria-label="${ariaLabel}"]`)]
        .filter(c => c.getAttribute('aria-disabled') !== 'true');
      if (cells.length === 0) {
        const prevBtn = document.querySelector('.DayPicker-NavButton--prev');
        if (prevBtn) { prevBtn.click(); await this._randomDelay(300, 400); }
        cells = [...document.querySelectorAll(`div.DayPicker-Day[aria-label="${ariaLabel}"]`)]
          .filter(c => c.getAttribute('aria-disabled') !== 'true');
      }
      if (cells.length === 0) return false;
      cells[cells.length - 1].click();
      return true;
    };

    let dateInputs = getVisibleDateInputs();
    const startInput = dateInputs[0] || document.querySelector('div[data-testid="input"].e1mdtx7j1');
    if (!startInput) return false;
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
  async _waitForOrdersDataReady(timeoutMs = 8000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const count = this._getExpectedOrderCount();
      const sales = this._getExpectedSalesTotal();
      // 건수 또는 매출액이 읽히면 데이터 로드 완료
      if (count > 0 || sales > 0) return true;
      await new Promise(r => setTimeout(r, 100));
    }
    return false; // 타임아웃
  },

  async _waitForOrdersCountChange(prevCount, timeoutMs = 8000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs && !this._stopFlag) {
      const cur = this._getExpectedOrderCount();
      if (cur !== prevCount) return;
      await new Promise(r => setTimeout(r, 100));
    }
  },

  _getExpectedSalesTotal() {
    const allUnitEls = document.querySelectorAll('.h1-txt .order-unit-price');
    for (const unitEl of allUnitEls) {
      if (unitEl.textContent.trim() === '원') {
        const valueEl = unitEl.previousElementSibling;
        const num = parseInt((valueEl?.textContent.trim() || '0').replace(/,/g, ''), 10);
        if (!isNaN(num) && num >= 0) return num;
      }
    }
    return 0;
  },

  async _collectOrdersPages(shopInfo) {
    const allRows = [];
    const iso = new Date().toISOString();
    let pageCount = 0;
    let hasError = false;
    let emptyPageCount = 0;

    while (!this._stopFlag) {
      const currentPage = this._getCurrentPage();
      pageCount++;

      Utils.updateProgressModal({
        currentPage: pageCount,
        rowCount: allRows.length,
        message: `${currentPage}페이지 수집 중...`
      });

      const pageData = await this._collectCurrentPageData(shopInfo, iso, currentPage);

      if (pageData.length === 0) {
        emptyPageCount++;
        Utils.updateProgressModal({ debug: `⚠️ ${currentPage}페이지 빈 페이지 (연속 ${emptyPageCount}번째)` });

        if (emptyPageCount >= 3) {
          Utils.updateProgressModal({ debug: `❌ 연속 3페이지 데이터 없음 - 수집 종료` });
          hasError = true;
          break;
        }
      } else {
        emptyPageCount = 0;
        allRows.push(...pageData);
      }

      Utils.updateProgressModal({ rowCount: allRows.length });
      console.log(`[쿠팡이츠] ${currentPage}페이지: ${pageData.length}행 수집 (총 ${allRows.length}행)`);

      if (this._stopFlag) break;

      await this._closeErrorPopups();

      const hasNext = this._clickNextPage();
      if (!hasNext) {
        console.log('[쿠팡이츠] 마지막 페이지 도달');
        break;
      }

      Utils.updateProgressModal({ message: `${currentPage + 1}페이지 로딩 대기...` });

      const prevFirstOrderId = this._getFirstOrderId();
      const loaded = await this._waitForPageLoad(prevFirstOrderId, 20000);

      if (!loaded) {
        if (this._stopFlag) break;

        Utils.updateProgressModal({ debug: `⚠️ ${currentPage + 1}페이지 로딩 타임아웃 - 재시도` });
        await this._randomDelay(2000, 3000);
        await this._closeErrorPopups();

        const retryLoaded = await this._waitForPageLoad(prevFirstOrderId, 10000);
        if (!retryLoaded) {
          Utils.updateProgressModal({ debug: `❌ ${currentPage + 1}페이지 재시도 실패 - 수집 종료` });
          hasError = true;
          break;
        } else {
          Utils.updateProgressModal({ debug: `✅ ${currentPage + 1}페이지 재시도 성공` });
        }
      }

      await this._randomDelay(800, 1200);
    }

    return { allRows, pageCount, hasError };
  },

  async _collectOrdersWithValidation(shopInfo, isMultiStore = false) {
    if (isMultiStore) {
      Utils.updateProgressModal({ message: `${shopInfo.store_name ? shopInfo.store_name + ' ' : ''}수집 시작...` });
    } else {
      Utils.showProgressModal('쿠팡이츠 수집', `${shopInfo.store_name ? shopInfo.store_name + ' ' : ''}첫 페이지 로딩 중...`);
    }

    const MAX_RETRY = 3;
    // ESC 대비: 마지막으로 수집된 결과 보존
    let lastResult = null;

    for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
      // ESC가 재시도 대기 중에 눌린 경우 → 이전 수집 결과로 저장
      if (this._stopFlag && lastResult) {
        await this._downloadOrdersResult(lastResult.allRows, shopInfo, { ...lastResult.stats, isMultiStore });
        return;
      }

      const expectedSales = this._getExpectedSalesTotal();
      const expectedCount = this._getExpectedOrderCount();

      Utils.updateProgressModal({
        debug: `📊 기대값 - 매출액: ${expectedSales > 0 ? expectedSales.toLocaleString() + '원' : '읽기 실패'} | 주문: ${expectedCount > 0 ? expectedCount + '건' : '읽기 실패'}`
      });

      const { allRows, pageCount, hasError } = await this._collectOrdersPages(shopInfo);

      if (allRows.length === 0) {
        // ESC로 인해 빈 결과인 경우 이전 결과 사용
        if (this._stopFlag && lastResult) {
          await this._downloadOrdersResult(lastResult.allRows, shopInfo, { ...lastResult.stats, isMultiStore });
          return;
        }
        Utils.showSuccessModal('데이터 없음', `${shopInfo.store_name ? shopInfo.store_name + ': ' : ''}추출할 주문이 없습니다.`, { status: 'error' });
        return;
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

      // 매출액 일치 OR 최대 재시도 OR ESC → 저장
      // 건수 불일치는 재시도 없이 경고만 표시 (h1-txt와 실제 목록 불일치는 쿠팡 UI 이슈)
      if (salesMatch || attempt === MAX_RETRY || this._stopFlag) {
        await this._downloadOrdersResult(allRows, shopInfo, {
          pageCount, hasError, expectedCount, expectedSales, collectedSales, salesMatch,
          collectedCount, countMatch, attempt, isMultiStore
        });
        return;
      }

      Utils.updateProgressModal({
        debug: `⚠️ 매출액 불일치 (수집: ${collectedSales.toLocaleString()}원 ≠ 기대: ${expectedSales.toLocaleString()}원) → 재조회 (${attempt}/${MAX_RETRY})`
      });
      if (!countMatch) {
        Utils.updateProgressModal({ debug: `ℹ️ 주문 건수: ${collectedCount}건 vs 기대: ${expectedCount}건 (경고만, 재시도 없음)` });
      }

      const prevCountForRetry = this._getExpectedOrderCount();
      await this._clickOrdersSearchButton();
      await this._waitForOrdersCountChange(prevCountForRetry, 8000);
      await this._waitForOrdersDataReady(8000);
    }
  },

  async _downloadOrdersResult(allRows, shopInfo, stats) {
    const { pageCount, hasError, expectedCount, expectedSales, collectedSales, salesMatch,
            collectedCount, countMatch, attempt, isMultiStore } = stats;

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
      dateStr: Utils.getTodayStr()
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
  }
};
