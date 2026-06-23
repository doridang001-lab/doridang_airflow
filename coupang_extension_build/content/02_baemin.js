// ===================================================================================
// ================= 02_baemin.js : 배민 수집 로직 =================
// ===================================================================================

Sites['baemin'] = {
  name: '배민',
  match: (url) => url.includes('self.baemin.com'),
  _stopFlag: false,
  
  async collect() {
    this._stopFlag = false;
    this._setupEscListener();
    
    const shopInfo = this._getShopInfo();
    
    // 메인 페이지에서 수집 시 매장명 확인
    if (!location.href.includes('/orders/') && 
        !location.href.includes('/stat/') && 
        !location.href.includes('/history/')) {
      
      const storeName = shopInfo.store_name || '';
      const isDoridang = storeName.includes('도리당') || storeName.includes('곱도리탕');
      
      if (!isDoridang) {
        const confirmMsg = `⚠️ 매장명 확인\n\n현재 선택된 매장:\n"${storeName}"\n\n도리당/곱도리탕 매장이 아닌 것 같습니다.\n이 매장으로 수집하시겠습니까?`;
        
        if (!confirm(confirmMsg)) {
          console.log('[배민 수집] 사용자가 수집 취소');
          Utils.showError('수집 취소', '매장을 확인하고 다시 시도해주세요.');
          this._removeEscListener();
          return;
        }
      }
    }
    
    if (location.href.includes('/orders/history')) {
      await this._collectOrdersAllPages(shopInfo);
    } else if (location.href.includes('/stat/marketing/woori-shop-click')) {
      await this._collectMarketingData(shopInfo);
    } else if (location.href.includes('/stat/advertisement')) {
      await this._collectAdvertisementData(shopInfo);
    } else if (location.href.includes('/history/change/shop')) {
      await this._collectChangeHistory(shopInfo);
    } else if (location.href.includes('/history/change/ad')) {
      await this._collectAdChangeHistory(shopInfo);
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

  _extractAreaName(storeName) {
    const areaMatch = storeName.match(/([가-힣]+(?:점|지점|매장|본점|분점|직영점))$/);
    return areaMatch ? areaMatch[1] : '';
  },

  _getShopInfo() {
    if (location.href.includes('/history/change/shop') || location.href.includes('/history/change/ad')) {
      const selectBox = document.querySelector('select.Select-module__a623');
      if (selectBox) {
        const selectedOption = selectBox.options[selectBox.selectedIndex];
        if (selectedOption) {
          const value = selectedOption.value;
          const text = selectedOption.textContent.trim();
          const nameMatch = text.match(/\[.*?\]\s*(.+?)\s+\d+$/);
          const storeName = nameMatch ? nameMatch[1].trim() : text.replace(/\d+$/, '').trim();
          return { store_name: storeName, store_id: value, needFilter: false };
        }
      }
      return { store_name: '', store_id: '', needFilter: false };
    }
    
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

  async _collectCurrentPageData(iso) {
    const rows = document.querySelectorAll('tr.Table_b_r4ax_1dwbr4on[data-index]');
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

      base.즉시할인 = '';
      base.즉시할인_파트너부담 = '';
      base.즉시할인_배민지원 = '';
      base.배민부담_쿠폰할인 = '';
      
      const discountContainer = detailSection?.querySelector('.InstantDiscountDetailPageSheet-module__u8LB');
      
      if (discountContainer) {
        const discountAmountEl = discountContainer.querySelector('.InstantDiscountDetailPageSheet-module__siYJ');
        base.즉시할인 = Utils.cleanPrice(discountAmountEl?.textContent || '');
        
        if (discountAmountEl && base.즉시할인) {
          discountAmountEl.click();
          await this._randomDelay(400, 600);
          
          const popup = document.querySelector('.InstantDiscountDetailPageSheet-module__IbXh');
          
          if (popup) {
            const allUls = popup.querySelectorAll('ul.InstantDiscountDetailPageSheet-module__Go_F');
            const lastUl = allUls[allUls.length - 1];
            
            if (lastUl) {
              const listItems = lastUl.querySelectorAll('li');
              
              for (const li of listItems) {
                const labelText = li.textContent || '';
                const valueContainer = li.querySelector('.TextListItem_b_r4ax_n197m77');
                const valueSpan = valueContainer?.querySelector('div span');
                const value = Utils.cleanPrice(valueSpan?.textContent || '');
                
                if (labelText.includes('파트너 부담')) {
                  base.즉시할인_파트너부담 = value;
                } else if (labelText.includes('배민 지원')) {
                  base.즉시할인_배민지원 = value;
                }
              }
            }
            
            document.body.click();
            await this._randomDelay(150, 300);
          }
        }
      }
      
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
  async _collectOrdersAllPages(shopInfo) {
    if (shopInfo.needFilter) {
      Utils.showError('가게 필터 필요', '가게 필터를 걸어주세요.');
      return;
    }

    this._currentShopInfo = shopInfo;
    
    Utils.showProgressModal('주문 내역 수집', '첫 페이지 로딩 중...');
    
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
      
      const pageData = await this._collectCurrentPageData(iso);
      allRows.push(...pageData);
      
      Utils.updateProgressModal({ rowCount: allRows.length });
      
      console.log(`[배민] ${currentPage}페이지: ${pageData.length}행 수집 (총 ${allRows.length}행)`);
      
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

    if (allRows.length === 0) {
      Utils.showSuccessModal('데이터 없음', '추출할 주문이 없습니다.', { status: 'error' });
      return;
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
  },

  async _collectMarketingData(shopInfo) {
    const storeIdMatch = location.pathname.match(/\/shops\/(\d+)\//);
    const monthMatch = location.href.match(/initialMonth=(\d{4}-\d{2})/);
    
    const store_id = storeIdMatch?.[1] || shopInfo.store_id || '';
    const yearMonth = monthMatch?.[1] || '';
    const [year, month] = yearMonth.split('-');
    
    if (!year || !month) {
      Utils.showError('날짜 정보 없음', 'URL에서 월 정보를 찾을 수 없습니다.');
      return;
    }
    
    // StoreRegistry에서 매장명 조회
    let store_name = shopInfo.store_name;
    if (!store_name && store_id) {
      store_name = await Utils.StoreRegistry.get('baemin', store_id);
      console.log(`[마케팅 수집] StoreRegistry에서 조회: ${store_id} → ${store_name}`);
    }
    
    Utils.showProgress('마케팅 데이터 수집 중...', '전체보기 클릭');
    
    const expandBtn = [...document.querySelectorAll('button[data-atelier-component="Button"]')]
      .find(btn => btn.textContent.includes('전체보기'));
    
    if (expandBtn) {
      expandBtn.click();
      await this._randomDelay(500, 800);
    }
    
    const table = document.querySelector('table[data-atelier-component="Table"]');
    if (!table) {
      Utils.showError('테이블 없음', '마케팅 데이터 테이블을 찾을 수 없습니다.');
      return;
    }
    
    const rows = table.querySelectorAll('tbody tr.Table_b_r4ax_1dwbr4on');
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
      Utils.showError('데이터 없음', '수집할 마케팅 데이터가 없습니다.');
      return;
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
    
    Utils.navigateWithConfirm(nextUrl, confirmMsg);
  },

  async _collectAdvertisementData(shopInfo) {
    const storeIdMatch = location.pathname.match(/\/shops\/(\d+)\//);
    const monthMatch = location.href.match(/initialMonth=(\d{4}-\d{2})/);
    
    const store_id = storeIdMatch?.[1] || shopInfo.store_id || '';
    const yearMonth = monthMatch?.[1] || '';
    
    if (!yearMonth) {
      Utils.showError('날짜 정보 없음', 'URL에서 월 정보를 찾을 수 없습니다.');
      return;
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
      return;
    }
    
    const headers = [
      'collected_at', 'store_id', '월',
      '노출수', '클릭수', 
      '주문수_전체', '주문수_오픈리스트', '주문수_배민1',
      '주문금액_전체', '주문금액_오픈리스트', '주문금액_배민1'
    ];
    
    const csv = Utils.toCSV(headers, rows);
    const filename = Utils.downloadCSV(csv, {
      channel: 'baemin',
      purpose: 'advertisement',
      storeName: shopInfo.store_name,
      storeId: store_id,
      dateStr: yearMonth.replace('-', '')
    });
    
    Utils.showSuccess('광고 성과 수집 완료', `${rows.length}건 → ${filename}`);
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
      return;
    }
    
    const headers = [
      '수집일시', '매장명', 'store_id', '지역명',
      '대분류', '분류', '변경시간', '작업자',
      '변경후_영업 임시중지 사유', '변경후_가게_영업_임시중지', '변경후_주문유형_가게배달', '변경후_주문유형_알뜰_한집배달',
      '변경전_영업 임시중지 사유', '변경전_가게_영업_임시중지', '변경전_주문유형_가게배달', '변경전_주문유형_알뜰_한집배달'
    ];
    
    const csv = Utils.toCSV(headers, allRows);
    const filename = Utils.downloadCSV(csv, {
      channel: 'baemin',
      purpose: 'change_history',
      storeName: shopInfo.store_name,
      storeId: shopInfo.store_id,
      dateStr: Utils.getTodayStr()
    });
    
    const status = this._stopFlag ? '(ESC 중단됨)' : '';
    Utils.showSuccess(
      `변경 이력 수집 완료 ${status}`, 
      `총 ${allRows.length}행 수집 (영업임시중지: ${pauseCount}건, 휴무일: ${holidayCount}건, 운영시간: ${operationTimeCount}건)\n→ ${filename}`
    );
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
    const filename = await Utils.downloadCSV(csv, {
      channel: 'baemin',
      purpose: 'metrics',
      storeName: shopInfo.store_name,
      storeId: shopInfo.store_id,
      dateStr: Utils.getTodayStr()
    });
    
    // 매장 정보 저장 (마케팅 페이지에서 사용)
    if (shopInfo.store_id && shopInfo.store_name) {
      Utils.StoreRegistry.save('baemin', shopInfo.store_id, shopInfo.store_name);
    }
    
    // 다음 페이지 URL
    const nextUrl = `https://self.baemin.com/shops/${shopInfo.store_id}/stat/marketing/woori-shop-click?initialDateOption=MONTHLY&initialMonth=${new Date().toISOString().slice(0, 7)}`;
    
    // 자동 이동 확인
    const confirmMsg = `✅ 메트릭 수집 완료!\n\n📁 파일: ${filename}\n📊 ${rows.length}건 수집\n\n⏭️ 마케팅 페이지로 이동하시겠습니까?`;
    
    Utils.navigateWithConfirm(nextUrl, confirmMsg);
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

  _getBrand(storeName) {
    const known = ['나홀로', '도리당'];
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
  async collectAndSaveOrders() {
    const site = Sites['baemin'];
    if (!site) return { success: false, error: 'Sites.baemin 없음' };

    const shopInfo = site._getShopInfo();
    const storeName = shopInfo.store_name || '알수없는매장';
    const brand = this._getBrand(storeName);

    const dateMatch = location.href.match(/startDate=(\d{4}-\d{2}-\d{2})/);
    const targetDate = dateMatch?.[1] || new Date().toISOString().slice(0, 10);
    const ym = targetDate.slice(0, 7);

    // Utils.downloadCSV를 임시 override → 원본 CSV 캡처 (저장은 우리가 직접 처리)
    const origDownload = Utils.downloadCSV.bind(Utils);
    let capturedCsv = null;
    Utils.downloadCSV = async (csv) => { capturedCsv = csv; return 'pipeline_captured'; };

    try {
      await site._collectOrdersAllPages(shopInfo);
    } finally {
      Utils.downloadCSV = origDownload;
    }

    if (!capturedCsv) return { success: false, error: '수집 결과 없음 (0건 또는 오류)' };

    const filename = `[${brand}][${storeName}]orders_${ym}.csv`;
    await this._download(capturedCsv, filename);
    return { success: true, filename, storeName };
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
