from playwright.sync_api import sync_playwright
import time
import sys

# stdout UTF-8 강제
sys.stdout.reconfigure(encoding='utf-8')

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1920, "height": 1080})
    
    page.goto("https://smart.easypos.net/index.jsp", wait_until="networkidle", timeout=60000)
    time.sleep(5)
    
    main_frame = None
    for frame in page.frames:
        if frame.name == "main":
            main_frame = frame
            break
    
    if not main_frame:
        print("ERROR: main frame not found")
        for i, f in enumerate(page.frames):
            print(f"Frame[{i}]: name={f.name!r}, url={f.url}")
        browser.close()
        exit(1)
    
    # 로그인
    id_outer = main_frame.query_selector("#mainframe_childframe_form_divMain_edtId")
    id_outer.click()
    time.sleep(0.3)
    id_input = main_frame.query_selector("#mainframe_childframe_form_divMain_edtId_input")
    id_input.fill("3322002029")
    time.sleep(0.3)
    pw_outer = main_frame.query_selector("#mainframe_childframe_form_divMain_edtPw")
    pw_outer.click()
    time.sleep(0.3)
    pw_input = main_frame.query_selector("#mainframe_childframe_form_divMain_edtPw_input")
    pw_input.fill("1")
    time.sleep(0.3)
    login_btn = main_frame.query_selector("#mainframe_childframe_form_divMain_btnLogin")
    login_btn.click()
    print("로그인 버튼 클릭 완료")
    time.sleep(8)
    
    page.screenshot(path="C:/airflow/easypos_after_login.png")
    print("스크린샷 저장 완료")
    
    # 프레임 구조
    print("\n=== 로그인 후 프레임 구조 ===")
    for i, frame in enumerate(page.frames):
        print(f"  Frame[{i}]: name={frame.name!r}, url={frame.url}")
    
    # main 프레임 재획득
    main_frame_after = None
    for frame in page.frames:
        if frame.name == "main":
            main_frame_after = frame
            break
    
    if main_frame_after:
        # 로그인 폼
        login_form = main_frame_after.query_selector("#mainframe_childframe_form_divMain_edtId_input")
        print(f"\n로그인 폼 여전히 있음: {bool(login_form)} -> {'로그인 실패' if login_form else '로그인 성공'}")
        
        # 영업속보 메뉴
        menu2 = main_frame_after.query_selector("#mainframe_childframe_form_divTop_img_TA_top_menu2")
        print(f"영업속보 메뉴(menu2) 존재: {bool(menu2)}")
        
        # divTop 요소들
        all_divtop = main_frame_after.query_selector_all("[id*='divTop']")
        print(f"\ndivTop 요소 수: {len(all_divtop)}")
        for el in all_divtop[:20]:
            eid = el.get_attribute('id') or ''
            print(f"  {eid}")
        
        # 메뉴 요소들
        all_menus = main_frame_after.query_selector_all("[id*='top_menu']")
        print(f"\ntop_menu 요소 수: {len(all_menus)}")
        for el in all_menus[:20]:
            eid = el.get_attribute('id') or ''
            try:
                txt = el.inner_text()[:60]
            except:
                txt = ''
            print(f"  id={eid!r}, text={txt!r}")
        
        # 화면 텍스트
        visible_text = main_frame_after.evaluate("() => document.body.innerText")
        print(f"\n화면 텍스트 (500자):\n{visible_text[:500]!r}")
    
    # 비밀번호 변경 팝업
    for i, frame in enumerate(page.frames):
        try:
            popup = frame.query_selector("[id*='popupChangePasswd']")
            if popup:
                print(f"\n비밀번호 변경 팝업 발견 in Frame[{i}]!")
        except:
            pass
    
    browser.close()
    print("\n완료")
