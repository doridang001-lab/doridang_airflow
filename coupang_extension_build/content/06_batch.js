// ===================================================================================
// ================= 06_batch.js : 배치 오케스트레이터 연동 (완료 신호 + 로그아웃) =================
// ===================================================================================
//
// 역할: 기존 수집 로직(03_coupangeats.js)을 "손대지 않고" 래핑하여
//   - 매장 1개 수집이 끝나면 runner(대시보드)로 COLLECTION_COMPLETE 신호를 보낸다.
//   - runner가 보내는 LOGOUT 요청을 처리한다.
// 로딩 순서상 03(coupangeats), 05(main) 다음에 로드된다 (manifest content_scripts 마지막).
//
// 주의: 이 파일은 추가(additive)만 한다. 기존 함수 시그니처/동작을 바꾸지 않는다.

(function () {
  // all_frames:true 이므로 iframe에서도 로드된다. 완료 신호는 최상위 프레임에서만 보낸다
  // (iframe이 먼저 COLLECTION_COMPLETE를 보내 배치가 조기 종료되는 것 방지).
  if (window.top !== window.self) {
    return;
  }
  if (typeof Sites === 'undefined' || !Sites || !Sites['coupangeats']) {
    return; // 쿠팡 사이트 컨텍스트가 아니면 아무것도 안 함
  }

  const site = Sites['coupangeats'];

  // ── 1. collect() 래핑: 수집 완료 시 COLLECTION_COMPLETE 발신 ──
  // collect()는 orders/coupon/cmg 모든 분기를 await 하므로, 이걸 감싸면
  // "데이터 있음/없음/에러" 모든 경우의 완료 시점을 잡을 수 있다.
  if (!site.__batchWrapped) {
    site.__batchWrapped = true;
    const origCollect = site.collect.bind(site);

    site.collect = async function (opts = {}) {
      let collectError = null;
      try {
        await origCollect(opts);
      } catch (e) {
        collectError = e;
        console.error('[Batch] collect() 오류:', e);
      }

      // 완료 신호 (현재 페이지 URL에서 store_id 추출)
      let shopInfo = {};
      try {
        shopInfo = (typeof site._getShopInfo === 'function') ? site._getShopInfo() : {};
      } catch (_) { /* ignore */ }

      try {
        chrome.runtime.sendMessage({
          type: 'COLLECTION_COMPLETE',
          payload: {
            channel: 'coupangeats',
            store_id: shopInfo.store_id || '',
            store_name: shopInfo.store_name || '',
            url: location.href,
            error: collectError ? String(collectError.message || collectError) : null,
            ts: Date.now()
          }
        });
        console.log('[Batch] COLLECTION_COMPLETE 발신:', shopInfo.store_id, shopInfo.store_name);
      } catch (e) {
        console.warn('[Batch] COLLECTION_COMPLETE 발신 실패:', e);
      }

      if (collectError) throw collectError;
    };
  }

  // ── 2. LOGOUT 처리: 다른 계정으로 전환하기 전 현재 세션 종료 ──
  // 쿠팡이츠 로그아웃 UI 셀렉터가 버전마다 달라서, 우선 DOM 버튼을 best-effort로 찾고
  // 실패하면 runner 쪽에서 browsingData(쿠키 삭제)로 확실히 로그아웃한다(아래 runner.js).
  async function tryDomLogout() {
    // 흔한 로그아웃 트리거 후보들 (텍스트 기반)
    const candidates = Array.from(document.querySelectorAll('button, a, span, div'));
    const target = candidates.find((el) => {
      const t = (el.getAttribute('innerText') || el.textContent || '').trim();
      return t === '로그아웃' || t === 'logout' || t === 'Logout';
    });
    if (target) {
      target.click();
      return true;
    }
    return false;
  }

  chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    if (msg && msg.type === 'LOGOUT_DOM') {
      tryDomLogout().then((ok) => sendResponse({ success: ok }));
      return true; // async
    }
    return false;
  });

  console.log('[Batch] 06_batch.js 로드 완료 (완료신호 래핑 + 로그아웃 핸들러).');
})();
