// ===================================================================================
// ================= 01_utils.js : 공통 유틸리티 함수 =================
// ===================================================================================

// Utils 객체 정의 (00_config.js에서 var Utils = {} 선언됨)
Utils = {
  // ============ 기본 유틸리티 ============
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  },
  
  // 자동 이동 헬퍼 (설정에 따라 확인창 표시 또는 바로 이동)
  async navigateWithConfirm(url, message) {
    const settings = await new Promise(resolve => {
      chrome.storage.sync.get(['showConfirmOnNavigate'], resolve);
    });
    const showConfirm = settings.showConfirmOnNavigate ?? false; // 기본값: false (바로 이동)
    
    if (showConfirm) {
      // 확인창 표시
      if (confirm(message)) {
        console.log('[워크플로우] 사용자 확인 후 이동:', url);
        location.href = url;
      } else {
        console.log('[워크플로우] 사용자가 이동 취소');
      }
    } else {
      // 바로 이동
      console.log('[워크플로우] 자동 이동:', url);
      location.href = url;
    }
  },
  
  // ============ 매장 정보 저장소 ============
  StoreRegistry: {
    async save(channel, storeId, storeName) {
      return new Promise((resolve) => {
        chrome.storage.local.get(['storeRegistry'], (result) => {
          const registry = result.storeRegistry || {};
          if (!registry[channel]) registry[channel] = {};
          
          registry[channel][storeId] = {
            name: storeName,
            lastUpdated: new Date().toISOString()
          };
          
          chrome.storage.local.set({ storeRegistry: registry }, () => {
            console.log(`[StoreRegistry] 저장: ${channel}/${storeId} = ${storeName}`);
            resolve();
          });
        });
      });
    },
    
    async get(channel, storeId) {
      return new Promise((resolve) => {
        chrome.storage.local.get(['storeRegistry'], (result) => {
          const registry = result.storeRegistry || {};
          const storeName = registry[channel]?.[storeId]?.name || '';
          console.log(`[StoreRegistry] 조회: ${channel}/${storeId} = ${storeName || '(없음)'}`);
          resolve(storeName);
        });
      });
    },
    
    async getAll(channel) {
      return new Promise((resolve) => {
        chrome.storage.local.get(['storeRegistry'], (result) => {
          const registry = result.storeRegistry || {};
          resolve(registry[channel] || {});
        });
      });
    }
  },
  
  // ============ 진행 모달 관련 ============
  _progressModalId: '__collector_progress_modal',
  _progressModalData: {
    currentPage: 0,
    totalPages: 0,
    rowCount: 0,
    message: ''
  },
  _logLines: [],
  _multiStoreMode: false,
  _multiStoreCurrentIdx: -1,
  
  showProgressModal(title, initialMessage = '') {
    this.closeProgressModal();
    
    const overlay = document.createElement('div');
    overlay.id = this._progressModalId;
    overlay.style.cssText = `
      position: fixed;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      background: rgba(0, 0, 0, 0.6);
      z-index: 2147483646;
      display: flex;
      align-items: center;
      justify-content: center;
      animation: fadeIn 0.2s ease-out;
    `;
    
    const modal = document.createElement('div');
    modal.style.cssText = `
      background: #fff;
      border-radius: 16px;
      padding: 32px;
      min-width: 400px;
      max-width: 500px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
      animation: slideUp 0.3s ease-out;
    `;
    
    modal.innerHTML = `
      <style>
        @keyframes fadeIn {
          from { opacity: 0; }
          to { opacity: 1; }
        }
        @keyframes slideUp {
          from { transform: translateY(20px); opacity: 0; }
          to { transform: translateY(0); opacity: 1; }
        }
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
        @keyframes fadeOut {
          to { opacity: 0; transform: translateY(-10px); }
        }
      </style>
      <div style="text-align: center;">
        <div style="font-size: 48px; margin-bottom: 16px;">
          <div style="display: inline-block; animation: spin 1s linear infinite;">⏳</div>
        </div>
        <h2 style="margin: 0 0 8px 0; font-size: 24px; font-weight: 600; color: #1a1a1a;">${title}</h2>
        <div id="progress_message" style="color: #666; font-size: 14px; margin-bottom: 24px;">${initialMessage}</div>
        
        <div id="progress_bar_container" style="background: #e0e0e0; height: 8px; border-radius: 4px; margin-bottom: 20px; overflow: hidden;">
          <div id="progress_bar" style="background: linear-gradient(90deg, #4285f4, #34a853); height: 100%; width: 0%; transition: width 0.3s ease;"></div>
        </div>
        
        <div id="progress_details" style="background: #f5f5f5; padding: 16px; border-radius: 8px; margin-bottom: 20px; text-align: left;">
          <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
            <span style="color: #666;">진행:</span>
            <span id="progress_current" style="font-weight: 600; color: #1a1a1a;">대기 중...</span>
          </div>
          <div style="display: flex; justify-content: space-between;">
            <span style="color: #666;">수집된 데이터:</span>
            <span id="progress_rows" style="font-weight: 600; color: #4285f4;">0행</span>
          </div>
        </div>
        
        <div id="progress_debug" style="background: #fff3cd; padding: 12px; border-radius: 8px; margin-bottom: 20px; text-align: left; font-size: 12px; color: #856404; max-height: 150px; overflow-y: auto; display: none;">
          <strong>디버그:</strong>
          <div id="progress_debug_content"></div>
        </div>
        
        <div style="color: #999; font-size: 13px; display: flex; align-items: center; justify-content: center; gap: 8px;">
          <span style="background: #ffe0e0; color: #c00; padding: 4px 8px; border-radius: 4px; font-weight: 600;">ESC</span>
          <span>키를 눌러 중단할 수 있습니다</span>
        </div>
      </div>
    `;
    
    overlay.appendChild(modal);
    document.body.appendChild(overlay);
    
    this._progressModalData = {
      currentPage: 0,
      totalPages: 0,
      rowCount: 0,
      message: initialMessage
    };
    this._logLines = [`=== [${title}] ${new Date().toLocaleString()} ===`];
  },
  
  updateProgressModal(options = {}) {
    const modal = document.getElementById(this._progressModalId);
    if (!modal) return;
    
    const {
      currentPage,
      totalPages,
      rowCount,
      message,
      percent,
      debug
    } = options;
    
    if (currentPage !== undefined) this._progressModalData.currentPage = currentPage;
    if (totalPages !== undefined) this._progressModalData.totalPages = totalPages;
    if (rowCount !== undefined) this._progressModalData.rowCount = rowCount;
    if (message !== undefined) this._progressModalData.message = message;
    
    const messageEl = modal.querySelector('#progress_message');
    const currentEl = modal.querySelector('#progress_current');
    const rowsEl = modal.querySelector('#progress_rows');
    const barEl = modal.querySelector('#progress_bar');
    const debugEl = modal.querySelector('#progress_debug');
    const debugContentEl = modal.querySelector('#progress_debug_content');
    
    if (messageEl && this._progressModalData.message) {
      messageEl.textContent = this._progressModalData.message;
    }
    
    if (currentEl) {
      if (this._progressModalData.totalPages > 0) {
        currentEl.textContent = `${this._progressModalData.currentPage}/${this._progressModalData.totalPages} 페이지`;
      } else if (this._progressModalData.currentPage > 0) {
        currentEl.textContent = `${this._progressModalData.currentPage} 페이지`;
      }
    }
    
    if (rowsEl) {
      rowsEl.textContent = `${this._progressModalData.rowCount.toLocaleString()}행`;
    }
    
    if (barEl) {
      let progressPercent = percent;
      if (progressPercent === undefined && this._progressModalData.totalPages > 0) {
        progressPercent = Math.round((this._progressModalData.currentPage / this._progressModalData.totalPages) * 100);
      }
      if (progressPercent !== undefined) {
        barEl.style.width = `${Math.min(100, Math.max(0, progressPercent))}%`;
      }
    }
    
    // 디버그 정보 표시
    if (debug) {
      const timestamp = new Date().toLocaleTimeString();
      this._logLines.push(`[${timestamp}] ${debug}`);
      if (this._multiStoreMode && this._multiStoreCurrentIdx >= 0) {
        // 다중매장 모드: 해당 매장 섹션의 디버그 영역에 추가
        const storeDebug = document.getElementById(`__ms_debug_${this._multiStoreCurrentIdx}`);
        if (storeDebug) {
          storeDebug.style.display = 'block';
          const line = document.createElement('div');
          line.textContent = `[${timestamp}] ${debug}`;
          storeDebug.appendChild(line);
          storeDebug.scrollTop = storeDebug.scrollHeight;
        }
      } else if (debugEl && debugContentEl) {
        debugEl.style.display = 'block';
        debugContentEl.innerHTML += `<div>[${timestamp}] ${debug}</div>`;
        debugContentEl.scrollTop = debugContentEl.scrollHeight;
      }
      // runner 라이브로그에 중계
      try { chrome.runtime.sendMessage({ type: 'LIVE_LOG', msg: debug }); } catch(e) {}
    }

    // 다중매장 모드: message를 현재 매장 상태줄에 표시
    if (message !== undefined && this._multiStoreMode && this._multiStoreCurrentIdx >= 0) {
      const storeStatus = document.getElementById(`__ms_status_${this._multiStoreCurrentIdx}`);
      if (storeStatus) storeStatus.textContent = message;
    }
  },
  
  closeProgressModal() {
    if (this._multiStoreMode) return; // 다중매장 모달은 확인 버튼으로만 닫힘
    const modal = document.getElementById(this._progressModalId);
    if (modal) {
      modal.style.animation = 'fadeOut 0.2s ease-out';
      setTimeout(() => modal.remove(), 200);
    }
  },

  // ============ 완료 모달 ============
  _successModalId: '__collector_success_modal',
  
  showSuccessModal(title, details, options = {}) {
    this.closeProgressModal();
    
    const {
      status = 'success',
      autoClose = false,
      autoCloseDelay = 5000
    } = options;
    
    const statusConfig = {
      success: { icon: '✅', color: '#34a853', bg: '#e8f5e9' },
      error: { icon: '⚠️', color: '#ea4335', bg: '#fce8e6' },
      warning: { icon: '⚡', color: '#fbbc04', bg: '#fef7e0' }
    };
    
    const config = statusConfig[status] || statusConfig.success;
    
    const overlay = document.createElement('div');
    overlay.id = this._successModalId;
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
      animation: fadeIn 0.2s ease-out;
    `;
    
    const modal = document.createElement('div');
    modal.style.cssText = `
      background: #fff;
      border-radius: 16px;
      padding: 32px;
      min-width: 400px;
      max-width: 500px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
      animation: slideUp 0.3s ease-out;
    `;
    
    modal.innerHTML = `
      <style>
        @keyframes fadeIn {
          from { opacity: 0; }
          to { opacity: 1; }
        }
        @keyframes slideUp {
          from { transform: translateY(20px); opacity: 0; }
          to { transform: translateY(0); opacity: 1; }
        }
        @keyframes fadeOut {
          to { opacity: 0; transform: translateY(-10px); }
        }
      </style>
      <div style="text-align: center;">
        <div style="font-size: 64px; margin-bottom: 16px;">${config.icon}</div>
        <h2 style="margin: 0 0 8px 0; font-size: 24px; font-weight: 600; color: #1a1a1a;">${title}</h2>
        
        <div style="background: ${config.bg}; padding: 20px; border-radius: 12px; margin: 20px 0; text-align: left;">
          <div style="font-size: 14px; line-height: 1.8; color: #333; white-space: pre-line;">${details}</div>
        </div>
        
        <button id="modal_close_btn" style="
          background: ${config.color};
          color: #fff;
          border: none;
          border-radius: 8px;
          padding: 12px 32px;
          font-size: 16px;
          font-weight: 600;
          cursor: pointer;
          transition: all 0.2s;
          box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        " onmouseover="this.style.transform='translateY(-2px)'; this.style.boxShadow='0 4px 12px rgba(0,0,0,0.15)';"
           onmouseout="this.style.transform='translateY(0)'; this.style.boxShadow='0 2px 8px rgba(0,0,0,0.1)';">
          확인
        </button>
        
        ${autoClose ? `<div style="color: #999; font-size: 12px; margin-top: 12px;">${autoCloseDelay/1000}초 후 자동으로 닫힙니다</div>` : ''}
      </div>
    `;
    
    overlay.appendChild(modal);
    document.body.appendChild(overlay);
    
    const closeModal = () => {
      overlay.style.animation = 'fadeOut 0.2s ease-out';
      setTimeout(() => overlay.remove(), 200);
    };
    
    modal.querySelector('#modal_close_btn').addEventListener('click', closeModal);
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) closeModal();
    });
    
    const escHandler = (e) => {
      if (e.key === 'Escape') {
        closeModal();
        document.removeEventListener('keydown', escHandler);
      }
    };
    document.addEventListener('keydown', escHandler);
    
    if (autoClose) {
      setTimeout(closeModal, autoCloseDelay);
    }
  },

  // ============ 기존 함수들 ============
  // Utils 객체 내 downloadCSV 함수 수정
  downloadCSV(csvContent, options = {}) {
    const {
      channel = 'unknown',
      purpose = 'data',
      storeName = '',
      storeId = '',
      dateStr = ''
    } = options;
    
    const cleanStr = (str) => String(str || '').replace(/[\\/:*?"<>|\s]/g, '_').substring(0, 30);
    
    const timestamp = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const parts = [
      channel,
      purpose,
      cleanStr(storeName) || 'unknown',
      storeId || 'unknown',
      dateStr || timestamp
    ];
    
    const filename = parts.join('_') + '.csv';
    
    return new Promise((resolve, reject) => {
      let isResolved = false; // ✅ 중복 실행 방지 플래그
      
      chrome.runtime.sendMessage({
        type: 'DOWNLOAD_CSV',
        content: csvContent,
        filename: filename,
        downloadPath: DOWNLOAD_PATH
      }, (response) => {
        if (isResolved) return; // ✅ 이미 처리됨
        
        if (response?.success) {
          console.log(`[Collector] 다운로드 완료: ${response.filename}`);
          isResolved = true;
          resolve(filename);
        } else {
          console.error('[Collector] 다운로드 실패:', response?.error);
          if (!isResolved) {
            isResolved = true;
            this._fallbackDownload(csvContent, filename);
            resolve(filename);
          }
        }
      });
      
      // ✅ 타임아웃을 10초로 늘리고, 이미 처리된 경우 무시
      setTimeout(() => {
        if (!isResolved) {
          console.warn('[Collector] 다운로드 응답 타임아웃 - fallback 사용');
          isResolved = true;
          this._fallbackDownload(csvContent, filename);
          resolve(filename);
        }
      }, 10000);
    });
  },

  _fallbackDownload(content, filename, mimeType = 'text/csv;charset=utf-8') {
    const blob = new Blob(['\uFEFF' + content], { type: mimeType });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  },

  appendLog(line) {
    const ts = new Date().toLocaleTimeString();
    this._logLines.push(`[${ts}] ${line}`);
  },

  downloadLog(options = {}) {
    if (this._logLines.length <= 1) return;
    const { storeName = '', storeId = '', dateStr = '' } = options;
    const cleanStr = (s) => String(s || '').replace(/[\\/:*?"<>|\s]/g, '_').substring(0, 30);
    const timestamp = dateStr || new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const filename = `coupangeats_orders_${cleanStr(storeName)}_${storeId || 'unknown'}_${timestamp}_log.txt`;
    this._fallbackDownload(this._logLines.join('\n'), filename, 'text/plain;charset=utf-8');
  },

  // ============ 다중매장 통합 모달 ============
  showMultiStoreProgressModal(storeNames) {
    const existing = document.getElementById(this._progressModalId);
    if (existing) existing.remove();

    this._multiStoreMode = true;
    this._multiStoreCurrentIdx = -1;
    this._logLines = [`=== [쿠팡이츠 다중매장 수집] ${new Date().toLocaleString()} ===`];

    const storeSections = storeNames.map((name, i) => `
      <div id="__ms_section_${i}" style="padding:12px; border-radius:10px; background:#f9f9f9; margin-bottom:10px;">
        <div style="display:flex; align-items:center; gap:8px; margin-bottom:6px;">
          <span id="__ms_icon_${i}" style="font-size:18px;">⏳</span>
          <strong style="font-size:13px; color:#333;">${name}</strong>
        </div>
        <div id="__ms_status_${i}" style="font-size:12px; color:#888; margin-bottom:4px;">대기 중...</div>
        <div id="__ms_debug_${i}" style="display:none; font-size:11px; font-family:monospace; max-height:130px; overflow-y:auto; background:#1e1e1e; color:#d4d4d4; padding:7px 9px; border-radius:6px; line-height:1.6;"></div>
      </div>
    `).join('');

    const overlay = document.createElement('div');
    overlay.id = this._progressModalId;
    overlay.style.cssText = `
      position: fixed; top: 0; left: 0; right: 0; bottom: 0;
      background: rgba(0,0,0,0.6); z-index: 2147483647;
      display: flex; align-items: center; justify-content: center;
    `;
    overlay.innerHTML = `
      <div style="background:#fff; border-radius:16px; padding:24px 28px; width:540px; max-height:85vh; overflow-y:auto; box-shadow:0 20px 60px rgba(0,0,0,0.3);">
        <h3 style="margin:0 0 18px; font-size:17px; font-weight:700; text-align:center; color:#1a1a1a;">🏪 쿠팡이츠 다중매장 수집</h3>
        ${storeSections}
        <div style="text-align:center; margin-top:16px; padding-top:14px; border-top:1px solid #eee;">
          <div id="__ms_footer" style="color:#999; font-size:12px; margin-bottom:10px;">수집 진행 중... (ESC로 중단)</div>
          <button id="__ms_confirm" style="display:none; background:#34a853; color:#fff; border:none; border-radius:8px; padding:11px 36px; font-size:15px; font-weight:600; cursor:pointer;">확인</button>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);

    overlay.querySelector('#__ms_confirm').addEventListener('click', () => {
      this._multiStoreMode = false;
      overlay.remove();
    });
  },

  setMultiStoreIndex(idx) {
    this._multiStoreCurrentIdx = idx;
    const iconEl = document.getElementById(`__ms_icon_${idx}`);
    const sectionEl = document.getElementById(`__ms_section_${idx}`);
    if (iconEl) iconEl.textContent = '⏳';
    if (sectionEl) sectionEl.style.background = '#f0f7ff';
  },

  updateMultiStoreStoreResult(status, summary) {
    const idx = this._multiStoreCurrentIdx;
    const iconEl = document.getElementById(`__ms_icon_${idx}`);
    const statusEl = document.getElementById(`__ms_status_${idx}`);
    const sectionEl = document.getElementById(`__ms_section_${idx}`);
    if (iconEl) iconEl.textContent = status === 'success' ? '✅' : '⚠️';
    if (statusEl) { statusEl.style.color = status === 'success' ? '#34a853' : '#e67e00'; statusEl.textContent = summary; }
    if (sectionEl) sectionEl.style.background = status === 'success' ? '#f0fff4' : '#fff8e1';
  },

  finalizeMultiStoreModal() {
    const btn = document.getElementById('__ms_confirm');
    const footer = document.getElementById('__ms_footer');
    if (btn) btn.style.display = 'inline-block';
    if (footer) footer.textContent = '모든 매장 수집 완료';
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
    this.closeProgressModal();
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
    return text.replace(/[^\d-]/g, '').replace(/^-/, '');
  },

  cleanPriceWithSign(text) {
    if (!text) return '';
    const match = text.match(/-?[\d,]+/);
    return match ? match[0].replace(/,/g, '') : '';
  },

  toCSV(headers, rows) {
    let csv = headers.join(',') + '\n';
    for (const row of rows) {
      csv += headers.map(h => this.escapeCSV(row[h])).join(',') + '\n';
    }
    return csv;
  },

  getTodayStr() {
    const d = new Date();
    return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`;
  }
};
