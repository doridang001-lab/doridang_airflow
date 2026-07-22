// ===================================================================================
// ================= 04_auto_login.js : 자동 로그인 =================
// ===================================================================================

async function simulateTyping(input, text) {
  input.focus();
  await Utils.sleep(150 + Math.random() * 150);
  input.value = '';
  for (const ch of String(text)) {
    input.value += ch;
    input.dispatchEvent(new KeyboardEvent('keydown', { key: ch, bubbles: true }));
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.dispatchEvent(new KeyboardEvent('keyup', { key: ch, bubbles: true }));
    await Utils.sleep(60 + Math.random() * 90);
  }
  input.dispatchEvent(new Event('change', { bubbles: true }));
}

async function handleAutoLogin(platform, id, pw, storeName, autoClick = false) {
  console.log(`[Auto Login] ========== 시작 ==========`);
  console.log(`[Auto Login] 플랫폼: ${platform}`);
  console.log(`[Auto Login] 매장: ${storeName}`);
  console.log(`[Auto Login] 자동클릭: ${autoClick ? 'ON' : 'OFF'}`);
  
  try {
    if (platform === 'coupang') {
      console.log('[Auto Login] 쿠팡이츠 요소 찾기 시작...');
      
      // 쿠팡이츠 로그인
      const idInput = document.querySelector('#loginId');
      const pwInput = document.querySelector('#password');
      const loginBtn = document.querySelector('button.merchant-submit-btn');
      
      console.log('[Auto Login] 요소 찾기 결과:', {
        idInput: !!idInput,
        pwInput: !!pwInput,
        loginBtn: !!loginBtn
      });
      
      if (!idInput) {
        console.error('[Auto Login] ❌ ID 입력 필드를 찾을 수 없음');
        alert('ID 입력 필드를 찾을 수 없습니다.\n페이지를 새로고침 후 다시 시도해주세요.');
        return;
      }
      
      if (!pwInput) {
        console.error('[Auto Login] ❌ PW 입력 필드를 찾을 수 없음');
        alert('비밀번호 입력 필드를 찾을 수 없습니다.\n페이지를 새로고침 후 다시 시도해주세요.');
        return;
      }
      
      if (!loginBtn) {
        console.error('[Auto Login] ❌ 로그인 버튼을 찾을 수 없음');
        alert('로그인 버튼을 찾을 수 없습니다.\n페이지를 새로고침 후 다시 시도해주세요.');
        return;
      }
      
      console.log('[Auto Login] ✅ 모든 요소 확인 완료');
      
      // ID 입력
      console.log('[Auto Login] ID 입력 시작...');
      try {
        await simulateTyping(idInput, id);
        idInput.blur();
        
        console.log('[Auto Login] ✅ ID 입력 완료:', idInput.value);
      } catch (err) {
        console.error('[Auto Login] ❌ ID 입력 중 오류:', err);
        throw err;
      }
      
      await Utils.sleep(800);
      
      // PW 입력
      console.log('[Auto Login] PW 입력 시작...');
      try {
        await simulateTyping(pwInput, pw);
        pwInput.blur();
        
        console.log('[Auto Login] ✅ PW 입력 완료 (길이:', pw.length, ')');
      } catch (err) {
        console.error('[Auto Login] ❌ PW 입력 중 오류:', err);
        throw err;
      }
      
      // Form validation 대기
      await Utils.sleep(1000);
      
      // 로그인 버튼 처리
      if (autoClick) {
        console.log('[Auto Login] 버튼 자동 클릭 시도...');
        console.log('[Auto Login] 버튼 상태:', {
          disabled: loginBtn.disabled,
          className: loginBtn.className
        });
        
        // 1. 버튼 활성화 대기 (최대 8초)
        let waitCount = 0;
        while (loginBtn.disabled && waitCount < 16) {
          console.log(`[Auto Login] 버튼 활성화 대기 중... (${waitCount + 1}/16)`);
          await Utils.sleep(500);
          waitCount++;
        }
        
        if (loginBtn.disabled) {
          console.warn('[Auto Login] ⚠️ 버튼이 여전히 disabled 상태입니다');
          alert('로그인 버튼이 비활성화 상태입니다.\nID/PW를 확인하고 수동으로 클릭해주세요.');
          // 그래도 강조 표시
          loginBtn.style.boxShadow = '0 0 0 3px rgba(255, 152, 0, 0.8)';
          loginBtn.style.transition = 'box-shadow 0.3s';
          setTimeout(() => {
            loginBtn.style.boxShadow = '';
          }, 5000);
        } else {
          console.log('[Auto Login] ✅ 버튼 활성화됨, 클릭 시도');
          
          // 한 번만 클릭 (가장 안정적)
          loginBtn.click();
          
          console.log('[Auto Login] ✅ 클릭 완료');
        }
      } else {
        console.log('[Auto Login] ℹ️ 수동 로그인 모드 - 버튼을 직접 클릭하세요');
        // 버튼 강조
        loginBtn.style.boxShadow = '0 0 0 3px rgba(76, 175, 80, 0.5)';
        loginBtn.style.transition = 'box-shadow 0.3s';
        setTimeout(() => {
          loginBtn.style.boxShadow = '';
        }, 3000);
      }
      
      console.log('[Auto Login] ========== 완료 ==========');
      
    } else if (platform === 'baemin') {
      console.log('[Auto Login] 배민 요소 찾기 시작...');

      const findInput = (selectors) => {
        for (const selector of selectors) {
          const el = document.querySelector(selector);
          if (el) return el;
        }
        return null;
      };
      const setNativeValue = (input, value) => {
        const proto = input instanceof HTMLTextAreaElement ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype;
        const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
        if (setter) setter.call(input, value);
        else input.value = value;
        input.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: value }));
        input.dispatchEvent(new Event('change', { bubbles: true }));
      };
      const waitFor = async (getter, timeoutMs = 10000) => {
        const start = Date.now();
        while (Date.now() - start < timeoutMs) {
          const el = getter();
          if (el) return el;
          await Utils.sleep(250);
        }
        return null;
      };

      const idInput = await waitFor(() => findInput([
        'input[name="id"][data-testid="id"]',
        'input[data-testid="id"]',
        'input[name="id"]',
        'input[name="username"]',
        'input[type="text"]',
        'input[type="email"]'
      ]));
      const pwInput = await waitFor(() => findInput([
        'input[name="password"][data-testid="password"]',
        'input[data-testid="password"]',
        'input[name="password"]',
        'input[type="password"]'
      ]));
      const loginBtn = await waitFor(() => {
        const submit = document.querySelector('button[type="submit"]');
        if (submit) return submit;
        return [...document.querySelectorAll('button')].find(btn => /로그인|시작|확인/.test(btn.textContent || ''));
      });

      console.log('[Auto Login] 요소 찾기 결과:', {
        idInput: !!idInput,
        pwInput: !!pwInput,
        loginBtn: !!loginBtn,
        url: location.href
      });

      if (!idInput || !pwInput || !loginBtn) {
        console.error('[Auto Login] ❌ 배민 로그인 요소를 찾을 수 없음');
        alert('배민 로그인 요소를 찾을 수 없습니다.\n페이지를 새로고침 후 다시 시도해주세요.');
        return;
      }

      idInput.focus();
      await Utils.sleep(200);
      setNativeValue(idInput, id);
      idInput.blur();
      console.log('[Auto Login] ✅ ID 입력 완료:', idInput.value);

      await Utils.sleep(400);
      pwInput.focus();
      await Utils.sleep(200);
      setNativeValue(pwInput, pw);
      pwInput.blur();
      console.log('[Auto Login] ✅ PW 입력 완료 (길이:', pw.length, ')');

      await Utils.sleep(800);

      if (autoClick) {
        console.log('[Auto Login] 버튼 자동 클릭 시도...');
        let waitCount = 0;
        while (loginBtn.disabled && waitCount < 20) {
          setNativeValue(idInput, id);
          setNativeValue(pwInput, pw);
          await Utils.sleep(500);
          waitCount++;
        }

        console.log('[Auto Login] 버튼 상태:', {
          disabled: loginBtn.disabled,
          className: loginBtn.className,
          waitCount
        });

        if (!loginBtn.disabled) {
          loginBtn.click();
          console.log('[Auto Login] ✅ 클릭 완료');
        } else {
          const form = loginBtn.closest('form') || idInput.closest('form') || pwInput.closest('form');
          if (form?.requestSubmit) {
            console.warn('[Auto Login] 버튼 disabled - form.requestSubmit fallback 실행');
            form.requestSubmit();
          } else {
            console.warn('[Auto Login] 버튼 disabled - click fallback 실행');
            loginBtn.click();
          }
        }
      } else {
        console.log('[Auto Login] ℹ️ 수동 로그인 모드 - 버튼을 직접 클릭하세요');
        loginBtn.style.boxShadow = '0 0 0 3px rgba(76, 175, 80, 0.5)';
        loginBtn.style.transition = 'box-shadow 0.3s';
        setTimeout(() => {
          loginBtn.style.boxShadow = '';
        }, 3000);
      }

      console.log('[Auto Login] ========== 완료 ==========');

    } else if (platform === 'yogiyo') {
      console.log('[Auto Login] 요기요 요소 찾기 시작...');

      const idInput = document.querySelector('input[name="username"]');
      const pwInput = document.querySelector('input[name="password"]');
      const loginBtn = document.querySelector('button[type="submit"]');

      console.log('[Auto Login] 요소 찾기 결과:', { idInput: !!idInput, pwInput: !!pwInput, loginBtn: !!loginBtn });

      if (!idInput || !pwInput || !loginBtn) {
        alert('요기요 로그인 요소를 찾을 수 없습니다.\n페이지를 새로고침 후 다시 시도해주세요.');
        return;
      }

      // React input 세팅 (요기요는 React 기반)
      const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;

      idInput.focus();
      await Utils.sleep(300);
      nativeSetter.call(idInput, id);
      idInput.dispatchEvent(new Event('input', { bubbles: true }));
      idInput.dispatchEvent(new Event('change', { bubbles: true }));
      idInput.blur();
      console.log('[Auto Login] ✅ ID 입력 완료');

      await Utils.sleep(800);

      pwInput.focus();
      await Utils.sleep(300);
      nativeSetter.call(pwInput, pw);
      pwInput.dispatchEvent(new Event('input', { bubbles: true }));
      pwInput.dispatchEvent(new Event('change', { bubbles: true }));
      pwInput.blur();
      console.log('[Auto Login] ✅ PW 입력 완료');

      await Utils.sleep(1000);

      if (autoClick) {
        let waitCount = 0;
        while (loginBtn.disabled && waitCount < 16) {
          await Utils.sleep(500);
          waitCount++;
        }
        if (loginBtn.disabled) {
          alert('로그인 버튼이 비활성화 상태입니다.\n수동으로 클릭해주세요.');
          loginBtn.style.boxShadow = '0 0 0 3px rgba(255, 152, 0, 0.8)';
          setTimeout(() => { loginBtn.style.boxShadow = ''; }, 5000);
        } else {
          loginBtn.click();
          console.log('[Auto Login] ✅ 클릭 완료');
        }
      } else {
        loginBtn.style.boxShadow = '0 0 0 3px rgba(76, 175, 80, 0.5)';
        setTimeout(() => { loginBtn.style.boxShadow = ''; }, 3000);
      }

      console.log('[Auto Login] ========== 완료 ==========');

    } else if (platform === 'ddangyo') {
      console.log('[Auto Login] 땡겨요 요소 찾기 시작...');

      const idInput = document.querySelector('#mf_ibx_mbrId');
      const pwInput = document.querySelector('#mf_sct_pwd');
      const loginBtn = document.querySelector('#mf_btn_webLogin');

      console.log('[Auto Login] 요소 찾기 결과:', { idInput: !!idInput, pwInput: !!pwInput, loginBtn: !!loginBtn });

      if (!idInput || !pwInput || !loginBtn) {
        alert('땡겨요 로그인 요소를 찾을 수 없습니다.\n페이지를 새로고침 후 다시 시도해주세요.');
        return;
      }

      // 땡겨요는 일반 input (w2ui 프레임워크)
      idInput.focus();
      await Utils.sleep(300);
      idInput.value = id;
      idInput.dispatchEvent(new Event('input', { bubbles: true }));
      idInput.dispatchEvent(new Event('change', { bubbles: true }));
      idInput.blur();
      console.log('[Auto Login] ✅ ID 입력 완료');

      await Utils.sleep(800);

      pwInput.focus();
      await Utils.sleep(300);
      pwInput.value = pw;
      pwInput.dispatchEvent(new Event('input', { bubbles: true }));
      pwInput.dispatchEvent(new Event('change', { bubbles: true }));
      pwInput.blur();
      console.log('[Auto Login] ✅ PW 입력 완료');

      await Utils.sleep(800);

      if (autoClick) {
        loginBtn.click();
        console.log('[Auto Login] ✅ 클릭 완료');
      } else {
        loginBtn.style.boxShadow = '0 0 0 3px rgba(76, 175, 80, 0.5)';
        setTimeout(() => { loginBtn.style.boxShadow = ''; }, 3000);
      }

      console.log('[Auto Login] ========== 완료 ==========');
    }

  } catch (err) {
    console.error('[Auto Login] ❌❌❌ 오류 발생 ❌❌❌');
    console.error('[Auto Login] 오류 내용:', err);
    console.error('[Auto Login] 오류 스택:', err.stack);
    
    alert(`자동 입력 중 오류가 발생했습니다.\n\n오류 내용: ${err.message}\n\nF12 키를 눌러 Console을 확인하거나\n수동으로 입력해주세요.`);
  }
}
