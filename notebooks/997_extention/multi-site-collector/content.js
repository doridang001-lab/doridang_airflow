// ===================================================================================
// ================= COMMON UTILITIES (수정 불필요) =================
// ===================================================================================

const Utils = {
  downloadCSV(csvContent, filenamePrefix, store_id = 'unknown') {
    const timestamp = new Date().toISOString().slice(0, 19).replace(/[:-]/g, '').replace('T', '_');
    const filename = `${filenamePrefix}_${store_id}_${timestamp}.csv`;
    
    const blob = new Blob(['\uFEFF' + csvContent], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    return filename;
  },

  showSuccess(message, detail) {
    this._removeHUD();
    const hud = document.createElement('div');
    hud.id = '__collector_hud';
    hud.style.cssText = 'position:fixed;right:12px;top:12px;z-index:2147483647;background:rgba(0,128,0,.9);color:#fff;padding:12px 16px;border-radius:8px;font:14px system-ui;box-shadow:0 4px 12px rgba(0,0,0,0.3);';
    hud.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;gap:16px;">
        <div><strong>✓ ${message}</strong><br><span style="font-size:12px;">${detail}</span></div>
        <button id="__collector_close_btn" style="background:#fff;color:#000;border:none;border-radius:4px;padding:4px 10px;cursor:pointer;font-size:12px;">닫기</button>
      </div>`;
    document.body.appendChild(hud);
    document.getElementById('__collector_close_btn').addEventListener('click', () => hud.remove());
    setTimeout(() => { if (hud.parentNode) hud.remove(); }, 5000);
  },

  showError(title, message) {
    this._removeHUD();
    const hud = document.createElement('div');
    hud.id = '__collector_hud';
    hud.style.cssText = 'position:fixed;right:12px;top:12px;z-index:2147483647;background:rgba(200,50,50,.95);color:#fff;padding:14px 18px;border-radius:8px;font:14px system-ui;box-shadow:0 4px 12px rgba(0,0,0,0.3);max-width:360px;';
    hud.innerHTML = `
      <div>
        <strong>⚠️ ${title}</strong><br>
        <p style="margin:8px 0;font-size:13px;line-height:1.5;">${message}</p>
        <button id="__collector_close_btn" style="background:#fff;color:#000;border:none;border-radius:4px;padding:6px 14px;cursor:pointer;font-size:12px;margin-top:4px;">확인</button>
      </div>`;
    document.body.appendChild(hud);
    document.getElementById('__collector_close_btn').addEventListener('click', () => hud.remove());
  },

  showProgress(title, message) {
    this._removeHUD();
    const hud = document.createElement('div');
    hud.id = '__collector_hud';
    hud.style.cssText = 'position:fixed;right:12px;top:12px;z-index:2147483647;background:rgba(50,50,150,.9);color:#fff;padding:12px 16px;border-radius:8px;font:14px system-ui;box-shadow:0 4px 12px rgba(0,0,0,0.3);';
    hud.innerHTML = `<div><strong>⏳ ${title}</strong><br><span style="font-size:12px;">${message}</span></div>`;
    document.body.appendChild(hud);
  },

  _removeHUD() {
    const existing = document.getElementById('__collector_hud');
    if (existing) existing.remove();
  },

  escapeCSV(val) {
    const str = String(val ?? '');
    return (str.includes(',') || str.includes('"') || str.includes('\n')) 
      ? '"' + str.replace(/"/g, '""') + '"' 
      : str;
  },

  cleanPrice(text) {
    if (!text) return '';
    return text.replace(/[^\d]/g, '');
  },

  toCSV(headers, rows) {
    let csv = headers.join(',') + '\n';
    for (const row of rows) {
      csv += headers.map(h => this.escapeCSV(row[h])).join(',') + '\n';
    }
    return csv;
  }
};

// ===================================================================================
// ================= SITE REGISTRY =================
// ===================================================================================

const Sites = {};

// -------------------------------------------------------------------------------------
// [배민] self.baemin.com
// -------------------------------------------------------------------------------------
Sites['baemin'] = {
  name: '배민',
  match: (url) => url.includes('self.baemin.com'),
  _stopFlag: false,
  
  async collect() {
    this._stopFlag = false;
    this._setupEscListener();
    
    const shopInfo = this._getShopInfo();
    
    if (location.href.includes('/orders/history')) {
      await this._collectOrdersAllPages(shopInfo);
    } else {
      this._collectMetrics(shopInfo);
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

  _getShopInfo() {
    if (location.href.includes('/orders/history')) {
      const filterContainer = document.querySelector('button.FilterContainer-module__ccrG .FilterContainer-module__Axi6');
      if (filterContainer) {
        const firstBadge = filterContainer.querySelector('.Badge_b_r4ax_19agxiso');
        const text = firstBadge?.textContent.trim() || '';
        if (text === '음식배달 가게 전체') {
          return { store_name: '', store_id: '', needFilter: true };
        }
        return { store_name: text, store_id: '', needFilter: false };
      }
    }
    const nameEl = document.querySelector('.ShopSelect-module__b8Mn, h3');
    const infoEl = document.querySelector('.ShopSelect-module__j4Qm');
    return {
      store_name: nameEl?.textContent.trim() || '',
      store_id: infoEl?.textContent.match(/(\d+)/)?.[1] || '',
      needFilter: false
    };
  },

  _getCurrentPage() {
    const activeDiv = document.querySelector('.Pagination_b_r4ax_pb5p4va');
    return parseInt(activeDiv?.textContent.trim() || '1', 10);
  },

  _clickNextPage(currentPage) {
    const nextPageNum = currentPage + 1;
    const buttons = document.querySelectorAll('ul.Pagination_b_r4ax_pb5p4v5 button.Pagination_b_r4ax_pb5p4vb');
    for (const btn of buttons) {
      const pageNum = parseInt(btn.textContent.trim(), 10);
      if (pageNum === nextPageNum) {
        btn.click();
        return true;
      }
    }
    return false;
  },

  async _expandAllOrders() {
    const expandAllBtn = [...document.querySelectorAll('button[data-atelier-component="TextButton"]')]
      .find(btn => btn.textContent.includes('모두 펼쳐보기'));
    if (expandAllBtn) {
      expandAllBtn.click();
      await this._randomDelay(500, 800);
      return;
    }
    const rows = document.querySelectorAll('tr.Table_b_r4ax_1dwbr4on[data-index]');
    for (const row of rows) {
      if (this._stopFlag) return;
      const svg = row.querySelector('td[data-td-index="0"] svg');
      if (svg?.getAttribute('aria-label') === '컨텐츠 펼치기') {
        svg.closest('td')?.click();
        await this._randomDelay(150, 300);
      }
    }
  },

  _collectCurrentPageData(iso) {
    const rows = document.querySelectorAll('tr.Table_b_r4ax_1dwbr4on[data-index]');
    const pageRows = [];

    for (const row of rows) {
      const base = {
        collected_at: iso,
        store_name: this._currentShopInfo.store_name,
        data_index: row.getAttribute('data-index'),
        주문상태: '', 주문번호: '', 주문시각: '', 광고상품: '', 캠페인ID: '',
        주문내역: '', 결제타입: '', 수령방법: '', 결제금액: '',
        상품금액: '', 즉시할인: '', 총결제금액: ''
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
        else if (idx === '5') base.주문내역 = text;
        else if (idx === '6') base.결제타입 = text;
        else if (idx === '7') base.수령방법 = text;
        else if (idx === '8') base.결제금액 = Utils.cleanPrice(text);
      }

      const options = [];
      let detail = row.nextElementSibling;
      while (detail?.tagName === 'TR') {
        const section = detail.querySelector('td[colspan="9"] .DetailInfo-module__pZYe');
        if (section) {
          const itemPrice = section.querySelector('.DetailInfo-module__pC_2 .FieldItem-module__rb57');
          const discount = section.querySelector('.InstantDiscountDetailPageSheet-module__siYJ');
          const total = section.querySelector('.DetailInfo-module__PmTR .FieldItem-module__LyiN');
          if (itemPrice) base.상품금액 = Utils.cleanPrice(itemPrice.textContent);
          if (discount) base.즉시할인 = Utils.cleanPrice(discount.textContent);
          if (total) base.총결제금액 = Utils.cleanPrice(total.textContent);

          for (const opt of section.querySelectorAll('.DetailInfo-module__n2Ro')) {
            const optText = opt.textContent.trim().replace(/\s+/g, ' ');
            if (optText) {
              const match = optText.match(/^(.+?)\s*\(([^)]+)\)\s*$/);
              options.push(match 
                ? { name: match[1].trim(), price: Utils.cleanPrice(match[2]) }
                : { name: optText, price: '' }
              );
            }
          }
          break;
        }
        detail = detail.nextElementSibling;
      }

      if (options.length === 0) {
        pageRows.push({ ...base, 주문옵션상세: '', 주문옵션금액: '' });
      } else {
        for (const opt of options) {
          pageRows.push({ ...base, 주문옵션상세: opt.name, 주문옵션금액: opt.price });
        }
      }
    }
    return pageRows;
  },

  _currentShopInfo: null,
  async _collectOrdersAllPages(shopInfo) {
    if (shopInfo.needFilter) {
      Utils.showError('가게 필터 필요', '가게 필터를 걸어주세요.');
      return;
    }

    this._currentShopInfo = shopInfo;
    const allRows = [];
    const iso = new Date().toISOString();
    let pageCount = 0;

    while (!this._stopFlag) {
      const currentPage = this._getCurrentPage();
      pageCount++;
      
      Utils.showProgress(`${currentPage}페이지 펼치는 중...`, `ESC로 중단 가능`);
      await this._expandAllOrders();
      if (this._stopFlag) break;
      
      await this._randomDelay(300, 500);
      Utils.showProgress(`${currentPage}페이지 수집 중...`, `ESC로 중단 가능`);
      
      const pageData = this._collectCurrentPageData(iso);
      allRows.push(...pageData);
      
      const prevFirstOrderId = this._getFirstOrderId();
      const hasNext = this._clickNextPage(currentPage);
      if (!hasNext) break;
      
      Utils.showProgress(`${currentPage + 1}페이지 로딩 대기...`, `ESC로 중단 가능`);
      const loaded = await this._waitForPageLoad(prevFirstOrderId);
      if (!loaded && !this._stopFlag) {
        Utils.showError('로딩 타임아웃', `${currentPage + 1}페이지 로딩에 실패했습니다.`);
        break;
      }
      await this._randomDelay(800, 1500);
    }

    if (!allRows.length) {
      Utils.showError('데이터 없음', '추출할 주문이 없습니다.');
      return;
    }

    const headers = [
      'collected_at', 'store_name', '주문상태', '주문번호', '주문시각',
      '광고상품', '캠페인ID', '주문내역', '결제타입', '수령방법', '결제금액',
      '상품금액', '즉시할인', '총결제금액', '주문옵션상세', '주문옵션금액'
    ];
    const csv = Utils.toCSV(headers, allRows);
    const filename = Utils.downloadCSV(csv, 'baemin_orders', shopInfo.store_name || 'all');
    
    const uniqueOrders = new Set(allRows.map(r => r.주문번호)).size;
    const status = this._stopFlag ? '(ESC 중단됨)' : '';
    Utils.showSuccess(`주문내역 수집 완료 ${status}`, `${pageCount}페이지, ${uniqueOrders}건 주문, ${allRows.length}행 → ${filename}`);
  },

  _collectMetrics(shopInfo) {
    const LABELS = ['조리소요시간', '주문접수시간', '최근재주문율', '조리시간준수율', '주문접수율', '최근별점'];
    let cards = document.querySelectorAll('.WooriShopNowCard-module__rcFf');
    if (!cards.length) cards = document.querySelectorAll('div[data-atelier-component="Container"]');

    const rows = [];
    const iso = new Date().toISOString();

    cards.forEach((card, idx) => {
      const data = {
        collected_at: iso,
        store_id: shopInfo.store_id,
        store_name: shopInfo.store_name,
        cardIndex: idx,
        url: location.href,
        조리소요시간: '', 조리소요시간_순위구분: '', 조리소요시간_순위비율: '',
        주문접수시간: '', 주문접수시간_순위구분: '', 주문접수시간_순위비율: '',
        최근재주문율: '', 
        조리시간준수율: '', 조리시간준수율_순위구분: '', 조리시간준수율_순위비율: '',
        주문접수율: '', 주문접수율_순위구분: '', 주문접수율_순위비율: '',
        최근별점: ''
      };

      for (const labelEl of card.querySelectorAll('span[data-atelier-component="Typography"]')) {
        const label = labelEl.textContent.trim();
        if (!LABELS.includes(label)) continue;

        const { value, rank } = this._findValueAndRank(labelEl, LABELS);
        const num = value?.match(/[\d.]+/)?.[0];
        const rankMatch = rank?.match(/^(상위|하위)\s*(\d+(?:\.\d+)?)%$/);

        if (label === '조리소요시간' || label === '주문접수시간') {
          data[label] = num || '';
          if (rankMatch) {
            data[`${label}_순위구분`] = rankMatch[1];
            data[`${label}_순위비율`] = (parseFloat(rankMatch[2]) / 100).toString();
          }
        } else if (label === '조리시간준수율' || label === '주문접수율') {
          data[label] = num ? (parseFloat(num) / 100).toString() : '';
          if (rankMatch) {
            data[`${label}_순위구분`] = rankMatch[1];
            data[`${label}_순위비율`] = (parseFloat(rankMatch[2]) / 100).toString();
          }
        } else if (label === '최근재주문율') {
          data[label] = num ? (parseFloat(num) / 100).toString() : '';
        } else if (label === '최근별점') {
          data[label] = num || '';
        }
      }
      rows.push(data);
    });

    if (!rows.length) {
      Utils.showError('데이터 없음', '수집할 메트릭이 없습니다.');
      return;
    }

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
    const filename = Utils.downloadCSV(csv, 'baemin_metrics', shopInfo.store_id);
    Utils.showSuccess('메트릭 수집 완료', `${rows.length}건 → ${filename}`);
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

// -------------------------------------------------------------------------------------
// [쿠팡이츠] store.coupangeats.com
// -------------------------------------------------------------------------------------
Sites['coupangeats'] = {
  name: '쿠팡이츠',
  match: (url) => url.includes('store.coupangeats.com'),
  _stopFlag: false,
  
  async collect() {
    this._stopFlag = false;
    this._setupEscListener();
    
    const shopInfo = this._getShopInfo();
    
    if (location.pathname.includes('/coupons/detail/')) {
      this._collectCouponStats(shopInfo);
    } else if (location.pathname.includes('/orders/')) {
      await this._collectOrdersAllPages(shopInfo);
    } else {
      Utils.showError('미지원 페이지', '주문관리 또는 쿠폰상세 페이지에서 실행해주세요.');
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

  _getShopInfo() {
    const ordersMatch = location.pathname.match(/\/orders\/(\d+)/);
    const couponsMatch = location.pathname.match(/\/coupons\/detail\/(\d+)/);
    
    let store_id = '';
    let coupon_id = '';
    
    if (ordersMatch) {
      store_id = ordersMatch[1];
    } else if (couponsMatch) {
      coupon_id = couponsMatch[1];
    }
    
    const storeNameEl = document.querySelector('.dropdown-btn.highlight');
    const store_name = storeNameEl?.textContent.trim() || '';
    
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
    const filename = Utils.downloadCSV(csv, 'coupangeats_coupon', shopInfo.coupon_id);
    
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

  _getFirstOrderId() {
    const firstItem = document.querySelector('.order-search-result-content > li.col-12');
    if (!firstItem) return '';
    const orderSection = firstItem.querySelector('.order-item');
    const cols = orderSection?.querySelectorAll('[class*="col-4"]');
    return cols?.[0]?.childNodes[0]?.textContent.trim() || '';
  },

  async _waitForPageLoad(prevFirstOrderId, timeout = 10000) {
    const startTime = Date.now();
    while (Date.now() - startTime < timeout) {
      if (this._stopFlag) return false;
      await new Promise(r => setTimeout(r, 200));
      const currentOrderId = this._getFirstOrderId();
      if (currentOrderId && currentOrderId !== prevFirstOrderId) return true;
    }
    return false;
  },

  async _collectCurrentPageData(shopInfo, iso, currentPage) {
    const orderItems = document.querySelectorAll('.order-search-result-content > li.col-12');
    const pageRows = [];
    const totalItems = orderItems.length;

    for (let i = 0; i < totalItems; i++) {
      if (this._stopFlag) break;
      
      Utils.showProgress(`${currentPage}페이지 수집 중...`, `${i + 1}/${totalItems}건 | ESC로 중단`);
      
      const item = orderItems[i];
      
      const expandIcon = item.querySelector('.order-expand-btn .icon-ce-arrowdown-bold');
      if (expandIcon) {
        expandIcon.closest('button').click();
        await this._randomDelay(300, 500);
      }
      
      const orderSection = item.querySelector('.order-item');
      const detailSection = item.querySelector('.order-details');
      
      const dateEl = orderSection.querySelector('.order-date:not(.d-md-none) span:first-child');
      const timeEl = orderSection.querySelector('.order-date .gray-txt');
      const order_date = (dateEl?.textContent.trim().replace(/\s+/g, '') || '') + ' ' + (timeEl?.textContent.trim() || '');
      
      const cols = orderSection.querySelectorAll('[class*="col-4"]');
      const order_id = cols[0]?.childNodes[0]?.textContent.trim() || '';
      
      const deliveryType = orderSection.querySelector('.delivery-type')?.textContent.trim() || '';
      const orderNameSpan = orderSection.querySelector('.order-name > span:not(.delivery-type)');
      const orderSummary = orderNameSpan?.textContent.trim() || '';
      
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
        else if (label.includes('정산 예정 금액') || label.includes('정산예정')) settlement.정산_예정_금액 = cleanVal;
        else if (label.includes('취소금액')) settlement.취소금액 = cleanVal;
      }

      const baseRow = {
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

      const menuItems = detailSection?.querySelectorAll('.order-detail-item') || [];
      
      if (menuItems.length === 0) {
        pageRows.push({ ...baseRow, menu_name: '', menu_qty: '', menu_price: '', menu_options: '' });
      } else {
        for (const menuItem of menuItems) {
          const menuNameEl = menuItem.querySelector('.col-7');
          let menu_name = '';
          if (menuNameEl) {
            for (const node of menuNameEl.childNodes) {
              if (node.nodeType === Node.TEXT_NODE) {
                const t = node.textContent.trim();
                if (t) { menu_name = t; break; }
              }
            }
          }
          
          const qtyEl = menuItem.querySelector('.col-2');
          const menu_qty = qtyEl?.textContent.trim().replace(/[^\d]/g, '') || '';
          
          const menuPriceEl = menuItem.querySelector('.order-item-price');
          const menu_price = Utils.cleanPrice(menuPriceEl?.textContent || '');
          
          const optionEls = menuItem.querySelectorAll('.order-detail-option-name');
          const menu_options = [...optionEls].map(el => el.textContent.trim()).join(' | ');

          pageRows.push({ ...baseRow, menu_name, menu_qty, menu_price, menu_options });
        }
      }
    }

    return pageRows;
  },

  async _collectOrdersAllPages(shopInfo) {
    const allRows = [];
    const iso = new Date().toISOString();
    let pageCount = 0;

    while (!this._stopFlag) {
      const currentPage = this._getCurrentPage();
      pageCount++;
      
      const pageData = await this._collectCurrentPageData(shopInfo, iso, currentPage);
      allRows.push(...pageData);
      
      if (this._stopFlag) break;
      
      const prevFirstOrderId = this._getFirstOrderId();
      const hasNext = this._clickNextPage();
      if (!hasNext) break;
      
      Utils.showProgress(`${currentPage + 1}페이지 로딩 대기...`, `ESC로 중단 가능`);
      
      const loaded = await this._waitForPageLoad(prevFirstOrderId);
      if (!loaded && !this._stopFlag) {
        Utils.showError('로딩 타임아웃', `${currentPage + 1}페이지 로딩에 실패했습니다.`);
        break;
      }
      
      await this._randomDelay(800, 1500);
    }

    if (!allRows.length) {
      Utils.showError('데이터 없음', '추출할 주문이 없습니다.');
      return;
    }

    const headers = [
      'collected_at', 'store_id', 'store_name', 'order_date', 'order_id',
      'delivery_type', 'order_status', 'order_summary', 'total_price', 'is_cancelled',
      'menu_name', 'menu_qty', 'menu_price', 'menu_options',
      '매출액', '상점부담_쿠폰', '중개_이용료', '결제대행사_수수료',
      '배달비', '광고비', '부가세', '즉시할인금액', '정산_예정_금액', '취소금액'
    ];
    
    const csv = Utils.toCSV(headers, allRows);
    const filename = Utils.downloadCSV(csv, 'coupangeats_orders', shopInfo.store_id || shopInfo.store_name);
    
    const uniqueOrders = new Set(allRows.map(r => r.order_id)).size;
    const status = this._stopFlag ? '(ESC 중단됨)' : '';
    Utils.showSuccess(`쿠팡이츠 수집 완료 ${status}`, `${pageCount}페이지, ${uniqueOrders}건 주문, ${allRows.length}행 → ${filename}`);
  }
};

// ===================================================================================
// ================= KEYBOARD & MESSAGE HANDLER =================
// ===================================================================================

let SHORTCUT_KEY = '/';

chrome.storage.sync.get(['shortcutKey'], (result) => {
  SHORTCUT_KEY = result.shortcutKey ?? '/';
  console.log(`[Collector] 단축키: ${SHORTCUT_KEY || '없음'}`);
});

async function runCollector() {
  const url = location.href;
  for (const [key, site] of Object.entries(Sites)) {
    if (site.match(url)) {
      console.log(`[Collector] ${site.name} 수집 시작`);
      await site.collect();
      return true;
    }
  }
  Utils.showError('미지원 사이트', '이 사이트는 아직 지원되지 않습니다.');
  return false;
}

document.addEventListener('keydown', (e) => {
  if (!SHORTCUT_KEY) return;
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.isContentEditable) return;
  if (e.key === SHORTCUT_KEY) {
    e.preventDefault();
    runCollector();
  }
});

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg?.type === 'COLLECT') {
    runCollector();
    sendResponse({ success: true });
  }
  if (msg?.type === 'UPDATE_SHORTCUT') {
    SHORTCUT_KEY = msg.key;
    console.log(`[Collector] 단축키 변경됨: ${SHORTCUT_KEY || '없음'}`);
    sendResponse({ success: true });
  }
  if (msg?.type === 'PING') {
    sendResponse({ success: true });
  }
  return true;
});

console.log('[Collector] 로드 완료.');