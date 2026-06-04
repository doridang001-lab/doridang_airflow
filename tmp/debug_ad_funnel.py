"""광고 funnel 페이지 DOM 구조 디버그 — 버튼/라디오/지표 확인."""
import logging
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.extract.croling_beamin import launch_browser, login_baemin, wait_for_page

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ACCOUNT_ID = "doriys"
PASSWORD   = "ever123@"
STORE_ID   = "14822058"   # 나홀로 역삼점
AD_URL     = f"https://self.baemin.com/shops/{STORE_ID}/stat/advertisement?initialDateOption=MONTH"


def dump_buttons(driver, label=""):
    result = driver.execute_script("""
        return [...document.querySelectorAll('button')].map(b => ({
            cls: b.className.slice(0, 70),
            txt: b.textContent.trim().slice(0, 60),
            dis: b.disabled
        }));
    """)
    print(f"\n=== 버튼 목록 [{label}] ===")
    for i, b in enumerate(result):
        print(f"  [{i}] txt={b['txt']!r:<40}  cls={b['cls'][:50]!r}")


def dump_radios(driver, label=""):
    result = driver.execute_script("""
        return [...document.querySelectorAll('input[type=radio]')].map(r => ({
            name: r.name, value: r.value, id: r.id,
            readonly: r.hasAttribute('readonly'), checked: r.checked,
            visible: r.offsetParent !== null
        }));
    """)
    print(f"\n=== 라디오 목록 [{label}] ===")
    for r in result:
        print(f"  name={r['name']!r:<35} value={r['value']!r:<15} readonly={r['readonly']}  visible={r['visible']}")


def dump_metrics(driver, label=""):
    result = driver.execute_script("""
        // margin: 0px 2px 가 있는 span이 값 요소
        const valueSpans = [...document.querySelectorAll('span[style*="margin"]')]
            .filter(s => /[\\d,]+/.test(s.textContent.trim()));

        const out = [];
        for (const s of valueSpans) {
            // 인접한 레이블 탐색: parent/sibling/ancestor 내 텍스트
            const parent = s.closest('[data-atelier-component]') || s.parentElement;
            const label = parent ? parent.textContent.replace(s.textContent, '').trim().slice(0, 30) : '';
            out.push({ label, value: s.textContent.trim(), cls: s.className.slice(0, 50) });
        }
        return out;
    """)
    print(f"\n=== 지표 값 [{label}] (span[style*=margin] 기반) ===")
    for m in result:
        print(f"  label={m['label']!r:<25} value={m['value']!r:<15} cls={m['cls'][:40]!r}")

    # 전체 페이지 텍스트에서 숫자 패턴 있는 텍스트 노드 탐색
    all_nums = driver.execute_script("""
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
        const out = [];
        let node;
        while ((node = walker.nextNode())) {
            const t = node.textContent.trim();
            if (/^[\\d,]+$/.test(t) && t.length > 2) {
                const parent = node.parentElement;
                out.push({
                    val: t,
                    tag: parent ? parent.tagName : '',
                    cls: parent ? parent.className.slice(0, 50) : '',
                    style: parent ? (parent.getAttribute('style') || '').slice(0, 60) : ''
                });
            }
        }
        return out.slice(0, 30);
    """)
    print(f"\n=== 숫자 텍스트 노드 [{label}] ===")
    for n in all_nums:
        print(f"  val={n['val']!r:<12} tag={n['tag']:<8} style={n['style']!r}")


driver = None
try:
    driver = launch_browser(ACCOUNT_ID)
    if not login_baemin(driver, ACCOUNT_ID, PASSWORD):
        print("로그인 실패")
        raise SystemExit

    driver.set_page_load_timeout(45)
    driver.get(AD_URL)
    print(f"\n이동: {AD_URL}")
    time.sleep(3.0)

    ok = wait_for_page(driver, "button[class*='Filter-module']", timeout=30)
    print(f"Filter 버튼 로드: {ok}")

    dump_buttons(driver, "초기 로드")
    dump_radios(driver, "초기 로드")
    dump_metrics(driver, "초기 로드 (필터 적용 전)")

    # Filter 버튼 클릭
    clicked = driver.execute_script("""
        const btns = [...document.querySelectorAll('button[class*="Filter-module"]')];
        const btn = btns.find(b => b.textContent.includes('3개월') || b.textContent.includes('1개월')
                                || b.textContent.includes('직접') || b.textContent.includes('일간'));
        if (btn) { btn.click(); return btn.textContent.trim().slice(0, 40); }
        // fallback: 첫 번째 Filter-module 버튼
        if (btns[0]) { btns[0].click(); return 'fallback:' + btns[0].textContent.trim().slice(0, 40); }
        return false;
    """)
    print(f"\n[Filter 버튼 클릭]: {clicked!r}")
    time.sleep(1.0)

    dump_radios(driver, "Filter 버튼 클릭 후")
    dump_buttons(driver, "Filter 버튼 클릭 후 (팝업 내)")

finally:
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
