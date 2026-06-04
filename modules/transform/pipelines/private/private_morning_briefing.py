"""
오전 브리핑 파이프라인

실행 순서: 로그인 → 캘린더 수집 → Flow AI 메인 검색 → Flow AI KPI 검색 → 이메일
"""

import os
import re
import time
import html
import logging
import subprocess
from pathlib import Path

import pendulum
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from modules.transform.utility.selenium_uc import configure_uc_data_path

logger = logging.getLogger(__name__)

# ============================================================
# 상수
# ============================================================
_LOGIN_URL          = "https://flow.team/signin.act"
_FLOW_ID            = os.getenv("FLOW_ID", "a17019@doridang.com")
# NOTE: 레거시 호환을 위해 기본값을 유지하되, 운영 환경에서는 반드시 환경변수로 덮어쓰는 것을 권장.
_FLOW_PW            = os.getenv("FLOW_PW", "Falswns3040@")
_DASHBOARD_URL      = "https://flow.team/main.act?dashboard"
_CALENDAR_URL       = "https://team-0aay3p.flow.team/calendar.act"
_TEAM_BASE_URL      = os.getenv("FLOW_TEAM_BASE_URL", "https://team-0aay3p.flow.team")
_CALENDAR_FALLBACK_URLS = [
    "https://flow.team/calendar.act",
    "https://flow.team/main.act?calendar",
]
_HEADLESS           = os.getenv("AIRFLOW_HOME") is not None
_BRIEFING_RECIPIENT = "a17019@kakao.com"
_CALENDAR_NOTIFY_ENABLED = os.getenv("FLOW_CALENDAR_NOTIFY_ENABLED", "1").strip() not in {"0", "false", "False", "no", "NO"}
_CALENDAR_NOTIFY_TARGET_NAME = os.getenv("FLOW_CALENDAR_NOTIFY_TARGET_NAME", "조민준").strip() or "조민준"
_BRIEFING_INCLUDE_KPI_SECTION = os.getenv("FLOW_BRIEFING_INCLUDE_KPI_SECTION", "0").strip() not in {"0", "false", "False", "no", "NO"}
_CALENDAR_ALL_DAY_LABEL = "\uc885\uc77c"

_FLOW_SEARCH_QUERY = """
---
[역할]
당신은 전략기획 PM의 업무 정리를 돕는 AI다.

[목표]
Flow 데이터 기반으로 “어제 진행상황 → 커뮤니케이션 → 오늘 액션”을 한 페이지로 요약한다.

[입력 데이터]
- Flow 프로젝트/업무 (최근 5일)
- 댓글 및 멘션 로그
- 업무 상태 (시작전 / 진행중 / 피드백 / 완료)

[출력 형식]

## 우선순위 (kpi기반으로)
1. 안산중앙점 가맹 해지에 따른 토더 해지 요청, 데이터 분석 자료 반영, 네이버 플레이스·온라인 정보 삭제
2. 캐시노트 1개월 무료 가입 후 송파삼전점 매출·방문주기 데이터를 수집하고 네이버플레이스 리뷰와의 상관관계 분석 초안 정리

## 1. 어제 진행상황
- 완료: 핵심 결과 중심 3개 이내
- 진행: 현재 상태 + 막힌 포인트
- 이슈: 원인 1줄

## 2. 나를 언급한 댓글 정리
- 중요 요청: 액션 필요한 것만
- 피드백: 방향성/수정 요청
- 단순 공유: 참고만

(각 항목은 “누가 / 무엇을 / 언제까지” 형태로 요약)

## 3. 오늘 해야할 일
- 우선순위 (중요도 기준)
- 각 항목은 “실행형 문장”으로 작성
- 반드시 결과 기준으로 작성 (ex. ~정리, ~완료, ~공유)

[규칙]
- 전체 800자 이내
- 불필요한 설명 금지
- 실행 중심 요약
- 중복 제거
- 모호한 표현 금지 (구체적으로 작성)
- 반드시 마크다운으로만 출력 (제목은 '##', 항목은 '-' 또는 '1.' 사용)
- 위 템플릿의 섹션/헤더 문구를 바꾸지 말 것(추가 문구 금지)
- '플로우 검색' 같은 안내 문구 금지

[제외]
 - "CS 14일 초과 미처리"
 - "KPI 현황"

"""

_KPI_SEARCH_QUERY = "조민준 KPI 평가 점수 항목별 현황 2026 의사결정지원 완결력 업무우선순위"


# ============================================================
# 브라우저 유틸
# ============================================================

def _resolve_chrome_bin() -> str | None:
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin and Path(chrome_bin).exists():
        return chrome_bin

    if os.name == "nt":
        candidates = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files\Chromium\Application\chrome.exe",
            r"C:\Program Files (x86)\Chromium\Application\chrome.exe",
        ]
        for c in candidates:
            if Path(c).exists():
                return c

    # linux container default
    for c in ["/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"]:
        if Path(c).exists():
            return c

    return None


def _get_chrome_version() -> int | None:
    try:
        chrome_bin = _resolve_chrome_bin()
        if not chrome_bin:
            return None
        result = subprocess.run(
            [chrome_bin, "--version"], capture_output=True, text=True, timeout=5
        )
        m = re.search(r"(\d+)\.", result.stdout.strip())
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def _launch_browser():
    logger.info("브라우저 실행 (headless=%s)", _HEADLESS)

    def _opts():
        options = uc.ChromeOptions()
        chrome_bin = _resolve_chrome_bin()
        if chrome_bin:
            options.binary_location = chrome_bin
        if _HEADLESS:
            # some sites detect/deny "new" headless; prefer the more compatible flag
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--window-size=1920,1080")
        return options

    ver = _get_chrome_version()
    try:
        configure_uc_data_path()
        driver = uc.Chrome(options=_opts(), version_main=ver) if ver else uc.Chrome(options=_opts())
        driver.maximize_window()
        try:
            driver.set_page_load_timeout(60)
        except Exception:
            pass
        logger.info("브라우저 실행 성공")
        return driver
    except Exception as e:
        m = re.search(r"Current browser version is (\d+)", str(e))
        if m:
            configure_uc_data_path()
            driver = uc.Chrome(options=_opts(), version_main=int(m.group(1)))
            driver.maximize_window()
            logger.info("브라우저 실행 성공 (재시도)")
            return driver
        raise


def _do_login(driver) -> bool:
    logger.info("Flow 로그인 시도")
    try:
        driver.get(_LOGIN_URL)
        time.sleep(2.5)

        def _type(el, text):
            el.clear()
            for ch in text:
                el.send_keys(ch)
                time.sleep(0.05)

        wait = WebDriverWait(driver, 20)

        user_sel = ["input#userId", "input[name='userId']", "input[type='email']"]
        pw_sel = ["input#password", "input[name='password']", "input[type='password']"]
        btn_sel = [
            "a#normalLoginButton",
            "button#normalLoginButton",
            "button[type='submit']",
            "input[type='submit']",
        ]

        user_el = None
        for sel in user_sel:
            try:
                user_el = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
                break
            except Exception:
                pass
        pw_el = None
        for sel in pw_sel:
            try:
                pw_el = driver.find_element(By.CSS_SELECTOR, sel)
                break
            except Exception:
                pass

        if not user_el or not pw_el:
            logger.warning("Flow 로그인 폼 요소를 찾지 못했습니다 (url=%s)", driver.current_url)
            return False

        _type(user_el, _FLOW_ID)
        _type(pw_el, _FLOW_PW)

        clicked = False
        for sel in btn_sel:
            try:
                btn = driver.find_element(By.CSS_SELECTOR, sel)
                driver.execute_script("arguments[0].click();", btn)
                clicked = True
                break
            except Exception:
                pass
        if not clicked:
            try:
                pw_el.send_keys(Keys.ENTER)
            except Exception:
                pass

        # Login redirect can be slow/intermittent in headless; wait a bit for URL to change.
        start = time.time()
        while time.time() - start < 25:
            cur = (driver.current_url or "").lower()
            if "signin" not in cur and "corpsignin.act" not in cur:
                break
            time.sleep(0.5)

        if "signin" not in (driver.current_url or "").lower():
            logger.info("Flow 로그인 성공 → %s", driver.current_url)
            time.sleep(2.0)

            # 팀 서브도메인 세션도 미리 확립(캘린더 접근 시 corp signin 리다이렉트 완화 목적)
            try:
                driver.get(f"{_TEAM_BASE_URL}/main.act")
                time.sleep(1.5)
            except Exception:
                pass
            return True
        logger.warning("Flow 로그인 실패 — URL: %s", driver.current_url)
        return False
    except Exception as e:
        logger.warning("Flow 로그인 오류: %s", e)
        return False


def _ensure_team_calendar_access(driver) -> None:
    """팀 캘린더 URL 접근 시 corp signin 리다이렉트가 뜨면 한 번 더 로그인 시도"""
    try:
        driver.get(_CALENDAR_URL)
        time.sleep(2.0)
        cur = (driver.current_url or "").lower()
        if "corpsignin.act" not in cur:
            return

        logger.info("팀 캘린더 corp signin 감지 → 추가 로그인 시도 (%s)", driver.current_url)

        def _type(el, text):
            el.clear()
            for ch in text:
                el.send_keys(ch)
                time.sleep(0.05)

        wait = WebDriverWait(driver, 15)
        # 페이지별로 selector 가 다를 수 있어 후보를 순차 시도
        user_sel = ["input#userId", "input[name='userId']", "input[type='email']"]
        pw_sel = ["input#password", "input[name='password']", "input[type='password']"]
        btn_sel = ["a#normalLoginButton", "button#normalLoginButton", "button[type='submit']"]

        user_el = None
        for sel in user_sel:
            try:
                user_el = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
                break
            except Exception:
                pass
        pw_el = None
        for sel in pw_sel:
            try:
                pw_el = driver.find_element(By.CSS_SELECTOR, sel)
                break
            except Exception:
                pass
        if user_el and pw_el:
            _type(user_el, _FLOW_ID)
            _type(pw_el, _FLOW_PW)

            clicked = False
            for sel in btn_sel:
                try:
                    btn = driver.find_element(By.CSS_SELECTOR, sel)
                    driver.execute_script("arguments[0].click();", btn)
                    clicked = True
                    break
                except Exception:
                    pass
            if not clicked:
                try:
                    pw_el.send_keys(Keys.ENTER)
                except Exception:
                    pass

            time.sleep(4.0)
    except Exception as e:
        logger.info("팀 캘린더 추가 로그인 시도 실패(무시): %r", e)


# ============================================================
# 캘린더 수집
# ============================================================

def _calendar_sort_key(ev: dict) -> tuple[int, str, str]:
    ev_time = (ev.get("time") or "").strip() or _CALENDAR_ALL_DAY_LABEL
    return (0 if ev_time == _CALENDAR_ALL_DAY_LABEL else 1, ev_time, (ev.get("title") or "").strip())


def _dedupe_calendar_events(events: list[dict]) -> list[dict]:
    deduped = []
    seen = set()
    for ev in events or []:
        key = ((ev.get("time") or "").strip() or _CALENDAR_ALL_DAY_LABEL, (ev.get("title") or "").strip())
        if not key[1] or key in seen:
            continue
        seen.add(key)
        deduped.append({"time": key[0], "title": key[1], "attending": bool(ev.get("attending", True))})
    deduped.sort(key=_calendar_sort_key)
    return deduped


def _normalize_calendar_text(text: str | None) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _normalize_calendar_time(text: str | None) -> str:
    return _normalize_calendar_text(text) or _CALENDAR_ALL_DAY_LABEL


def _build_calendar_event(time_text: str | None, title_text: str | None, attending: bool = True) -> dict:
    return {
        "time": _normalize_calendar_time(time_text),
        "title": _normalize_calendar_text(title_text),
        "attending": bool(attending),
    }


def _read_visible_calendar_events(driver, date_str: str) -> list[dict]:
    raw = driver.execute_script(
        """
        return (function(dateStr, allDayLabel) {
          function normalizeText(text) { return (text || '').replace(/\\s+/g, ' ').trim(); }
          function normalizeTime(text) { return normalizeText(text) || allDayLabel; }
          function extractEvent(el) {
            var title = normalizeText((el.querySelector('.fc-title') || el).textContent || '');
            if (!title) return null;
            var timeEl = el.querySelector('.fc-time');
            return {
              time: normalizeTime(timeEl ? timeEl.textContent : ''),
              title: title,
              attending: (el.getAttribute('class') || '').indexOf('atd-attending') >= 0,
            };
          }
          function rectX(el) {
            if (!el) return null;
            var rect = el.getBoundingClientRect();
            return { left: rect.left, right: rect.right, cx: (rect.left + rect.right) / 2 };
          }
          function withinColumn(a, b) {
            if (!a || !b) return false;
            return a.cx >= b.left && a.cx < b.right;
          }
          var header = document.querySelector(".fc-day-top[data-date='" + dateStr + "'], .fc-day-header[data-date='" + dateStr + "']");
          var headerRect = rectX(header);
          var candidates = [];
          if (headerRect) {
            Array.prototype.slice.call(document.querySelectorAll('a.fc-time-grid-event')).forEach(function(el) {
              if (el.closest('.fc-popover, .fc-more-popover')) return;
              if (withinColumn(rectX(el), headerRect)) candidates.push(el);
            });
          }
          return candidates.map(extractEvent).filter(Boolean);
        })(arguments[0], arguments[1]);
        """,
        date_str,
        _CALENDAR_ALL_DAY_LABEL,
    ) or []
    return [_build_calendar_event(ev.get("time"), ev.get("title"), ev.get("attending", True)) for ev in raw]


def _click_visible_calendar_event(driver, date_str: str, event_index: int) -> bool:
    return bool(
        driver.execute_script(
            """
            return (function(dateStr, eventIndex) {
              function rectX(el) {
                if (!el) return null;
                var rect = el.getBoundingClientRect();
                return { left: rect.left, right: rect.right, cx: (rect.left + rect.right) / 2 };
              }
              function withinColumn(a, b) {
                if (!a || !b) return false;
                return a.cx >= b.left && a.cx < b.right;
              }
              function click(el) {
                if (!el) return false;
                try { el.scrollIntoView({block:'center', inline:'center'}); } catch (e) {}
                try { el.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, view:window})); return true; } catch (e) {}
                try { el.click(); return true; } catch (e2) {}
                return false;
              }
              var header = document.querySelector(".fc-day-top[data-date='" + dateStr + "'], .fc-day-header[data-date='" + dateStr + "']");
              var headerRect = rectX(header);
              var candidates = [];
              if (headerRect) {
                Array.prototype.slice.call(document.querySelectorAll('a.fc-time-grid-event')).forEach(function(el) {
                  if (el.closest('.fc-popover, .fc-more-popover')) return;
                  if (withinColumn(rectX(el), headerRect)) candidates.push(el);
                });
              }
              return click(candidates[eventIndex]);
            })(arguments[0], arguments[1]);
            """,
            date_str,
            event_index,
        )
    )


def _open_calendar_more_popover(driver, date_str: str) -> bool:
    return bool(
        driver.execute_script(
            """
            return (function(dateStr) {
              function click(el) {
                if (!el) return false;
                try { el.scrollIntoView({block:'center', inline:'center'}); } catch (e) {}
                try { el.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, view:window})); return true; } catch (e) {}
                try { el.click(); return true; } catch (e2) {}
                return false;
              }
              var dayTop = document.querySelector(".fc-day-top[data-date='" + dateStr + "'], .fc-day-header[data-date='" + dateStr + "']");
              var week = dayTop ? (dayTop.closest('.fc-row.fc-week') || dayTop.closest('.fc-week') || dayTop.closest('thead') || dayTop.parentElement) : null;
              if (!week || !dayTop) return false;
              var dayTops = Array.prototype.slice.call(week.querySelectorAll('.fc-day-top, .fc-day-header'));
              var idx = -1;
              for (var i = 0; i < dayTops.length; i++) {
                if ((dayTops[i].getAttribute('data-date') || '') === dateStr) {
                  idx = i;
                  break;
                }
              }
              if (idx < 0) return false;
              var skeleton = document.querySelector('.fc-content-skeleton');
              if (!skeleton) return false;
              var rows = Array.prototype.slice.call(skeleton.querySelectorAll('tbody tr'));
              var rowspans = new Array(dayTops.length);
              for (var s = 0; s < rowspans.length; s++) rowspans[s] = 0;
              for (var r = 0; r < rows.length; r++) {
                for (var d = 0; d < rowspans.length; d++) if (rowspans[d] > 0) rowspans[d]--;
                var col = 0;
                var tds = Array.prototype.slice.call(rows[r].children);
                for (var c = 0; c < tds.length; c++) {
                  while (col < rowspans.length && rowspans[col] > 0) col++;
                  var td = tds[c];
                  var colspan = parseInt(td.getAttribute('colspan') || '1', 10) || 1;
                  var rowspan = parseInt(td.getAttribute('rowspan') || '1', 10) || 1;
                  var moreA = td.querySelector('a.fc-more');
                  if (moreA && idx >= col && idx < col + colspan) return click(moreA);
                  if (rowspan > 1) {
                    for (var span = 0; span < colspan; span++) {
                      var targetIdx = col + span;
                      if (targetIdx >= 0 && targetIdx < rowspans.length) rowspans[targetIdx] = Math.max(rowspans[targetIdx], rowspan - 1);
                    }
                  }
                  col += colspan;
                }
              }
              return false;
            })(arguments[0]);
            """,
            date_str,
        )
    )


def _read_calendar_popover_events(driver) -> list[dict]:
    raw = driver.execute_script(
        """
        return (function(allDayLabel) {
          function normalizeText(text) { return (text || '').replace(/\\s+/g, ' ').trim(); }
          function normalizeTime(text) { return normalizeText(text) || allDayLabel; }
          var pop = document.querySelector('.fc-popover, .fc-more-popover');
          if (!pop) return [];
          return Array.prototype.slice.call(pop.querySelectorAll('a.fc-event')).map(function(el) {
            var title = normalizeText((el.querySelector('.fc-title') || el).textContent || '');
            if (!title) return null;
            var timeEl = el.querySelector('.fc-time');
            return {
              time: normalizeTime(timeEl ? timeEl.textContent : ''),
              title: title,
              attending: (el.getAttribute('class') || '').indexOf('atd-attending') >= 0,
            };
          }).filter(Boolean);
        })(arguments[0]);
        """,
        _CALENDAR_ALL_DAY_LABEL,
    ) or []
    return [_build_calendar_event(ev.get("time"), ev.get("title"), ev.get("attending", True)) for ev in raw]


def _click_calendar_popover_event(driver, event_index: int) -> bool:
    return bool(
        driver.execute_script(
            """
            return (function(eventIndex) {
              function click(el) {
                if (!el) return false;
                try { el.scrollIntoView({block:'center', inline:'center'}); } catch (e) {}
                try { el.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, view:window})); return true; } catch (e) {}
                try { el.click(); return true; } catch (e2) {}
                return false;
              }
              var pop = document.querySelector('.fc-popover, .fc-more-popover');
              if (!pop) return false;
              var items = pop.querySelectorAll('a.fc-event');
              return click(items[eventIndex]);
            })(arguments[0]);
            """,
            event_index,
        )
    )


def _close_calendar_popover(driver) -> None:
    try:
        driver.execute_script(
            """
            var pop = document.querySelector('.fc-popover, .fc-more-popover');
            if (!pop) return;
            var closeBtn = pop.querySelector('.fc-close');
            if (closeBtn) {
              try { closeBtn.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, view:window})); return; } catch (e) {}
              try { closeBtn.click(); return; } catch (e2) {}
            }
            document.body.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, view:window}));
            """
        )
    except Exception:
        pass
    time.sleep(0.2)


def _close_calendar_detail(driver) -> None:
    for sel in [
        "button.close",
        "button.btn-close",
        "a.close",
        ".layer-close",
        ".btn-layer-close",
        ".btn-close",
        ".btn_pop_close",
    ]:
        try:
            buttons = driver.find_elements(By.CSS_SELECTOR, sel)
            if not buttons:
                continue
            btn = buttons[0]
            try:
                driver.execute_script("arguments[0].click();", btn)
            except Exception:
                btn.click()
            time.sleep(0.4)
            return
        except Exception:
            pass
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        time.sleep(0.3)
    except Exception:
        pass


def _has_calendar_target_participant(driver, target_name: str) -> bool:
    wait = WebDriverWait(driver, 8)
    for css in [
        "div.js-manager-group.manager-group",
        "div.manager-group",
        ".js-manager-group",
    ]:
        try:
            mgr_group = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, css)))
            participants = mgr_group.find_elements(By.XPATH, ".//p[normalize-space()]")
            if any((p.text or "").strip() == target_name for p in participants):
                return True
        except Exception:
            pass
    return False


def _verify_calendar_event_participant(driver, target_name: str) -> bool:
    try:
        return _has_calendar_target_participant(driver, target_name)
    finally:
        _close_calendar_detail(driver)


def _collect_calendar_events_for_day(driver, date_str: str, target_name: str) -> list[dict]:
    accepted: list[dict] = []
    seen: set[tuple[str, str]] = set()

    visible_events = _read_visible_calendar_events(driver, date_str)
    for idx, ev in enumerate(visible_events):
        title = (ev.get("title") or "").strip()
        event_time = (ev.get("time") or "").strip() or _CALENDAR_ALL_DAY_LABEL
        event_key = (event_time, title)
        if not title or event_key in seen:
            continue
        try:
            if not _click_visible_calendar_event(driver, date_str, idx):
                logger.info("캘린더 상세 열기 실패로 제외: %s %s %s", date_str, event_time, title)
                continue
            if _verify_calendar_event_participant(driver, target_name):
                accepted.append({"time": event_time, "title": title, "attending": True})
                seen.add(event_key)
            else:
                logger.info("캘린더 참석자 미포함으로 제외: %s %s %s", date_str, event_time, title)
        except Exception as e:
            logger.info("캘린더 상세 확인 실패로 제외: %s %s %s (%r)", date_str, event_time, title, e)
            _close_calendar_detail(driver)

    popover_events = []
    if _open_calendar_more_popover(driver, date_str):
        time.sleep(0.3)
        popover_events = _read_calendar_popover_events(driver)
        _close_calendar_popover(driver)

    for idx, ev in enumerate(popover_events):
        title = (ev.get("title") or "").strip()
        event_time = (ev.get("time") or "").strip() or _CALENDAR_ALL_DAY_LABEL
        event_key = (event_time, title)
        if not title or event_key in seen:
            continue
        try:
            if not _open_calendar_more_popover(driver, date_str):
                logger.info("캘린더 more popover 재열기 실패로 제외: %s %s %s", date_str, event_time, title)
                continue
            time.sleep(0.3)
            if not _click_calendar_popover_event(driver, idx):
                _close_calendar_popover(driver)
                logger.info("캘린더 popover 상세 열기 실패로 제외: %s %s %s", date_str, event_time, title)
                continue
            if _verify_calendar_event_participant(driver, target_name):
                accepted.append({"time": event_time, "title": title, "attending": True})
                seen.add(event_key)
            else:
                logger.info("캘린더 참석자 미포함으로 제외: %s %s %s", date_str, event_time, title)
        except Exception as e:
            logger.info("캘린더 popover 상세 확인 실패로 제외: %s %s %s (%r)", date_str, event_time, title, e)
            _close_calendar_detail(driver)
        finally:
            _close_calendar_popover(driver)

    return _dedupe_calendar_events(accepted)


def _collect_calendar(driver, target_date: str) -> dict[str, list[dict]]:
    try:
        today_str = target_date
        tomorrow_str = pendulum.parse(target_date).add(days=1).to_date_string()
        target_dates = [today_str, tomorrow_str]

        _ensure_team_calendar_access(driver)

        def _goto_calendar():
            driver.get(_DASHBOARD_URL)
            time.sleep(2)
            cal_links = driver.find_elements(By.XPATH, "//a[contains(text(), '캘린더')]")
            if cal_links:
                driver.execute_script("arguments[0].click();", cal_links[0])
                time.sleep(2)
            else:
                driver.get(_CALENDAR_URL)

        _goto_calendar()

        cur = (getattr(driver, "current_url", "") or "").lower()
        if "corpsignin.act" in cur or "signin" in cur:
            for url in [_CALENDAR_URL, *_CALENDAR_FALLBACK_URLS]:
                try:
                    driver.get(url)
                    time.sleep(2)
                    cur2 = (getattr(driver, "current_url", "") or "").lower()
                    if "corpsignin.act" in cur2 or "signin" in cur2:
                        continue
                    break
                except Exception:
                    pass

        wait = WebDriverWait(driver, 30)
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, f".fc-day-top[data-date='{today_str}'], td[data-date='{today_str}']"))
        )
        try:
            select_el = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#calendarSelectBoxItem select.select-box"))
            )
            current_value = (select_el.get_attribute("value") or "").strip()
            if current_value != "weekView":
                Select(select_el).select_by_value("weekView")
                time.sleep(2.0)
        except Exception as e:
            logger.info("캘린더 주간뷰 전환 실패, 현재 뷰로 진행: %r", e)

        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, f".fc-day-header[data-date='{today_str}'], .fc-time-grid-container, a.fc-time-grid-event")
            )
        )
        time.sleep(1.5)

        verified_events = {
            date_str: _collect_calendar_events_for_day(driver, date_str, _CALENDAR_NOTIFY_TARGET_NAME)
            for date_str in target_dates
        }
        result = {
            "today": verified_events.get(today_str, []),
            "tomorrow": verified_events.get(tomorrow_str, []),
        }
        logger.info("캘린더 수집 완료 (today=%d, tomorrow=%d)", len(result["today"]), len(result["tomorrow"]))
        return result

    except Exception as e:
        logger.warning("캘린더 수집 실패: %r (url=%s)", e, getattr(driver, "current_url", ""))
        return {"today": [], "tomorrow": []}


# ============================================================
# Flow AI 검색 유틸
# ============================================================

_CITATION_LINE_RE = re.compile(
    r"""
    ^\s*
    (
        \[[^\]]+\]          # [프로젝트명] 으로 시작하는 줄
        |
        \d{2}\.\d{2}        # 26.03 으로 시작하는 날짜
        |
        \d{6}\s             # 260324 형태 6자리 날짜
    )
    .*
    [''`]\d{2}\.\d{2}       # '26.03 형태 날짜 포함 (곡선/직선 따옴표 모두)
    """,
    re.VERBOSE,
)
_TRAILING_DATE_RE = re.compile(r"[''`]\d{2}\.\d{2}\s*(\+\d+)?\s*$")


def _clean_ai_response(text: str) -> str:
    """AI 요약에서 메타 문구·Flow 인라인 출처 참조 제거"""
    # 메타 문구 제거
    for skip in [
        "입력 정보에 민감 항목 없음", "검색 데이터에 민감 항목 없음",
        "검색 데이터 보호됨", "작업 완료",
    ]:
        text = text.replace(skip, "")

    # 출처 섹션 이후 잘라냄
    for cutoff in ["출처", "참고 게시글", "관련 게시글", "참고자료"]:
        idx = text.find(cutoff)
        if idx != -1:
            text = text[:idx]

    clean_lines = []
    for line in text.split("\n"):
        stripped = line.strip()
        # URL 줄 제거
        if re.match(r'^https?://', stripped):
            continue
        # Flow 인라인 출처 패턴 줄 제거
        if _CITATION_LINE_RE.match(line):
            continue
        # 줄 끝이 '26.03 +N 형태로 끝나는 줄 (인라인 참조가 섞인 줄)
        if _TRAILING_DATE_RE.search(line):
            # 해당 날짜 참조 부분만 제거하고 앞부분은 유지
            line = _TRAILING_DATE_RE.sub("", line).rstrip()
            if not line.strip():
                continue
        clean_lines.append(line)

    text = "\n".join(clean_lines)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _format_ai_html(text: str) -> str:
    """AI 응답 플레인텍스트 → 가독성 있는 HTML 변환"""
    section_colors = {
        "우선순위": "#6b21a8",  # 보라
        "1": "#1e40af",  # 어제 진행상황 — 파랑
        "2": "#065f46",  # 멘션/댓글 — 초록
        "3": "#1f2937",  # 오늘 해야할 일 — 검정
    }

    def _header_html(title: str) -> tuple[str, str]:
        t = (title or "").strip()
        key = "우선순위" if "우선순위" in t else (re.search(r"^(\d+)\.", t).group(1) if re.search(r"^(\d+)\.", t) else "")
        color = section_colors.get(key, "#374151")
        return (
            color,
            f'<div style="font-weight:bold;color:{color};font-size:13px;'
            f'border-bottom:1px solid {color}33;padding-bottom:4px;margin-bottom:6px;">'
            f'{html.escape(t, quote=False)}</div>',
        )

    # 1) markdown 헤더(##)가 있으면 우선 해당 기준으로 섹션 분리
    lines = [ln.rstrip() for ln in (text or "").splitlines()]
    sections: list[tuple[str, list[str]]] = []
    cur_title = ""
    cur_lines: list[str] = []

    def _flush():
        nonlocal cur_title, cur_lines
        if cur_title or any(l.strip() for l in cur_lines):
            sections.append((cur_title.strip(), cur_lines[:]))
        cur_title, cur_lines = "", []

    heading_re = re.compile(r"^\s*#{1,6}\s+(.+)\s*$")
    loose_heading_re = re.compile(r"^\s*(우선순위|어제 진행상황|나를 언급한 댓글 정리|오늘 해야할 일)\s*$")
    numbered_heading_re = re.compile(r"^\s*(\d+)\.\s+(.+)\s*$")

    for ln in lines:
        m = heading_re.match(ln)
        if m:
            _flush()
            cur_title = m.group(1).strip()
            continue
        m2 = numbered_heading_re.match(ln)
        if m2 and not cur_title and not cur_lines:
            # "1. 어제 진행상황" 같은 케이스를 섹션 헤더로 취급
            _flush()
            cur_title = f"{m2.group(1)}. {m2.group(2).strip()}"
            continue
        m3 = loose_heading_re.match(ln)
        if m3:
            _flush()
            cur_title = m3.group(1).strip()
            continue
        cur_lines.append(ln)

    _flush()

    # 섹션이 1개뿐이면, 빈 줄 기준으로 문단 분해(기존 호환)
    if len(sections) <= 1:
        paragraphs = [p.strip() for p in re.split(r"\n\n+", (text or "").strip()) if p.strip()]
        sections = [("", p.splitlines()) for p in paragraphs] if paragraphs else []

    html_parts: list[str] = []
    for title, body in sections:
        body = [b for b in body if b is not None]
        body_non_empty = [b for b in body if b.strip()]
        if not title and not body_non_empty:
            continue

        if title:
            color, hdr = _header_html(title)
            # 우선순위 섹션은 별도 카드 UI로 강조
            if "우선순위" in title:
                items = []
                for ln in body_non_empty:
                    ln2 = re.sub(r"^\s*\d+\.\s*", "", ln).strip()
                    ln2 = re.sub(r"^\s*[-•·]\s*", "", ln2).strip()
                    if ln2:
                        items.append(ln2)
                if items:
                    lis = []
                    for i, it in enumerate(items, start=1):
                        lis.append(
                            "<li "
                            "style=\"list-style:none;margin:0 0 10px;padding:10px 12px;"
                            "border:1px solid #e5e7eb;border-radius:10px;background:#ffffff;\">"
                            f"<div style=\"display:flex;gap:10px;align-items:flex-start;\">"
                            f"<div style=\"min-width:28px;height:28px;border-radius:999px;"
                            f"background:{color};color:#fff;display:flex;align-items:center;justify-content:center;"
                            f"font-weight:700;font-size:12px;\">{i}</div>"
                            f"<div style=\"flex:1;color:#111827;font-size:13px;line-height:1.6;\">"
                            f"{html.escape(it, quote=False)}"
                            "</div>"
                            "</div>"
                            "</li>"
                        )
                    html_parts.append(
                        '<div style="margin-bottom:14px;">'
                        f'{hdr}'
                        '<div style="background:#faf5ff;border:1px solid #e9d5ff;border-radius:12px;padding:12px;">'
                        '<ol style="margin:0;padding:0;">'
                        + "".join(lis)
                        + "</ol></div></div>"
                    )
                else:
                    html_parts.append('<div style="margin-bottom:14px;">' + hdr + "</div>")
            else:
                html_parts.append('<div style="margin-bottom:14px;">')
                html_parts.append(hdr)
                if body_non_empty:
                    html_parts.append(_lines_to_items(body_non_empty, color))
                html_parts.append("</div>")
        else:
            html_parts.append(f'<div style="margin-bottom:10px;">{_lines_to_items(body_non_empty, "#374151")}</div>')

    return "\n".join([p for p in html_parts if p])


def _lines_to_items(lines: list, accent: str) -> str:
    """줄 목록 → <ul> 또는 <p> HTML"""
    # 첫 줄이 실질적인 내용인지 확인
    non_empty = [l.strip() for l in lines if l.strip()]
    if not non_empty:
        return ""

    items = []
    for line in non_empty:
        # 마크다운 헤더 라인(## ...)은 상위에서 분리 실패 시에도 과도한 노이즈가 되므로 제거
        if re.match(r"^\s*#{1,6}\s+", line):
            continue
        # 이미 번호/기호로 시작하는 경우
        if re.match(r"^[-•·]\s+", line):
            line = re.sub(r"^[-•·]\s*", "", line)
        elif re.match(r"^\d+\.\s+", line):
            # "1. ..." 형태는 번호만 제거하고 항목화
            line = re.sub(r"^\d+\.\s*", "", line)
        safe_line = html.escape(line, quote=False)
        items.append(
            f'<li style="margin-bottom:5px;line-height:1.6;">{safe_line}</li>'
        )

    if len(items) == 1:
        return f'<p style="margin:0 0 4px;color:#374151;font-size:13px;line-height:1.6;">{html.escape(non_empty[0], quote=False)}</p>'
    return f'<ul style="margin:0;padding-left:18px;color:#374151;font-size:13px;">{"".join(items)}</ul>'


_LOADING_MARKERS = ["진행 중", "검색 결과 확인 중", "플로우에서 탐색", "의도 분석"]


def _wait_for_iframe_ai_response(driver, max_wait: int = 120):
    """iframe 내 AI 요약 완료 대기

    완료 조건:
    1. 'AI 요약' 마커가 body에 존재
    2. 로딩 중 표시 문구가 없음
    3. 텍스트 길이 > 500자이고 3회 연속 안정화
    """
    deadline = time.time() + max_wait
    prev_len = 0
    stable = 0

    while time.time() < deadline:
        time.sleep(5)
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text.strip()
            cur_len = len(body_text)

            # 로딩 중이면 아직 기다림
            still_loading = any(m in body_text for m in _LOADING_MARKERS)
            has_summary = "AI 요약" in body_text

            if has_summary and not still_loading and cur_len > 500:
                if cur_len == prev_len:
                    stable += 1
                    if stable >= 3:
                        logger.info("AI 응답 안정화 완료 (길이=%d)", cur_len)
                        return
                else:
                    stable = 0
            else:
                stable = 0  # 로딩 중이면 카운터 리셋

            prev_len = cur_len
        except Exception:
            pass

    logger.warning("AI 응답 대기 타임아웃 (%ds)", max_wait)


def _collect_flow_ai(driver, query: str, label: str = "", max_wait: int = 90) -> dict:
    """Flow AI 검색 팝업 → RAG 검색 → iframe에서 AI 요약 추출"""
    try:
        driver.get(_DASHBOARD_URL)
        time.sleep(3)

        driver.execute_script(
            "document.getElementById('topRagSearchPopupLayer').style.display = 'block';"
        )
        time.sleep(1)

        ai_btn = driver.find_element(
            By.CSS_SELECTOR, "button.js-search-mode-toggle[data-mode='ai']"
        )
        driver.execute_script("arguments[0].click();", ai_btn)
        time.sleep(0.5)

        ta = driver.find_element(By.ID, "searchPopupInput")
        driver.execute_script("arguments[0].removeAttribute('maxlength');", ta)
        driver.execute_script(
            "arguments[0].value = arguments[1];"
            "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));",
            ta, query,
        )
        time.sleep(0.5)
        ta.send_keys(Keys.RETURN)
        logger.info("Flow AI 검색 전송%s (%d자)", f" [{label}]" if label else "", len(query))
        time.sleep(5)

        frames = driver.find_elements(By.TAG_NAME, "iframe")
        ai_frame = None
        for fr in frames:
            src = fr.get_attribute("src") or ""
            if "flow-search" in src:
                ai_frame = fr

        if not ai_frame:
            logger.warning("AI 결과 iframe 없음")
            return {"url": "", "text": ""}

        driver.switch_to.frame(ai_frame)
        _wait_for_iframe_ai_response(driver, max_wait=max_wait)

        # Prefer the iframe's actual location (often includes a result context/id).
        # URL이 /ai/chat/... 형태로 이동 완료될 때까지 최대 10초 대기
        result_url = ""
        for _ in range(20):
            try:
                url_try = (driver.execute_script("return window.location.href;") or "").strip()
            except Exception:
                url_try = ""
            if url_try and url_try.rstrip("/") != "https://flow.team/ai" and "/chat/" in url_try:
                result_url = url_try
                break
            time.sleep(0.5)
        else:
            try:
                result_url = (driver.execute_script("return window.location.href;") or "").strip()
            except Exception:
                result_url = ""
        if not result_url:
            result_url = (ai_frame.get_attribute("src") or "").strip()
            if result_url.startswith("/"):
                result_url = f"{_TEAM_BASE_URL}{result_url}"

        body_text = driver.find_element(By.TAG_NAME, "body").text.strip()
        marker = "AI 요약"
        idx = body_text.find(marker)
        content = body_text[idx + len(marker):].strip() if idx != -1 else body_text

        cleaned = _clean_ai_response(content)
        # If aggressive cleaning wipes everything (common for KPI queries), keep the raw text.
        content = cleaned if cleaned.strip() else content.strip()
        logger.info("Flow AI 응답 추출 완료%s (%d자)", f" [{label}]" if label else "", len(content))
        return {"url": result_url, "text": content}

    except Exception as e:
        logger.warning("Flow AI 수집 실패%s: %s", f" [{label}]" if label else "", e)
        return {"url": "", "text": ""}
    finally:
        try:
            driver.switch_to.default_content()
        except Exception:
            pass


# ============================================================
# 이메일 HTML 빌드
# ============================================================

def _build_briefing_html(
    flow_ai: dict,
    kpi_ai: dict,
    calendar_events: dict | list,
    run_date: str,
    error_message: str = "",
) -> str:
    weekday_kr = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    now_kst = pendulum.now("Asia/Seoul")
    wd = weekday_kr[now_kst.weekday()]

    flow_url = (flow_ai or {}).get("url", "")
    kpi_url = (kpi_ai or {}).get("url", "")
    flow_text = ((flow_ai or {}).get("text") or "").strip()
    kpi_text = ((kpi_ai or {}).get("text") or "").strip()
    if isinstance(calendar_events, dict):
        today_events = _dedupe_calendar_events(calendar_events.get("today") or [])
        tomorrow_events = _dedupe_calendar_events(calendar_events.get("tomorrow") or [])
    else:
        today_events = _dedupe_calendar_events(calendar_events or [])
        tomorrow_events = []

    # 섹션 A: Flow AI 메인 브리핑 (텍스트 대신 링크)
    if flow_url:
        ai_html = (
            f'<a href="{html.escape(flow_url.strip(), quote=True)}" target="_blank" rel="noopener noreferrer" '
            f'style="display:inline-block;background:#8B00EA;color:#fff;text-decoration:none;padding:10px 14px;border-radius:6px;font-size:13px;">'
            'Flow AI 브리핑 답변 페이지 열기</a>'
            '<div style="color:#6b7280;font-size:12px;margin-top:8px;">※ Flow 로그인 필요</div>'
        )
        section_a = f"""
<div style="background:#f8f4ff;border-left:4px solid #8B00EA;padding:16px;margin-bottom:16px;border-radius:4px;">
  <div style="font-weight:bold;color:#8B00EA;margin-bottom:10px;font-size:14px;">📊 Flow AI 브리핑</div>
  {ai_html}
  {f'<div style="margin-top:12px;">{_format_ai_html(flow_text)}</div>' if flow_text else ''}
</div>"""
    else:
        section_a = f"""
<div style="background:#fee2e2;border-left:4px solid #dc2626;padding:12px;margin-bottom:10px;border-radius:4px;">
  <b style="color:#dc2626;">⚠️ Flow AI 링크 생성 실패</b> — Flow에서 직접 확인해주세요.
  {f'<div style=\"color:#6b7280;font-size:12px;margin-top:6px;\">원인: {html.escape(error_message, quote=False)}</div>' if error_message else ''}
</div>
<div style="margin:0 0 16px;">
  <a href="{html.escape(_DASHBOARD_URL, quote=True)}" target="_blank" rel="noopener noreferrer"
     style="display:inline-block;background:#111827;color:#fff;text-decoration:none;padding:8px 12px;border-radius:6px;font-size:12px;margin-right:8px;">
     Flow 대시보드 열기</a>
  <a href="{html.escape(_CALENDAR_URL, quote=True)}" target="_blank" rel="noopener noreferrer"
     style="display:inline-block;background:#374151;color:#fff;text-decoration:none;padding:8px 12px;border-radius:6px;font-size:12px;">
     Flow 캘린더 열기</a>
</div>"""
        if flow_text:
            section_a += f"""
<div style="background:#fff7ed;border-left:4px solid #fb923c;padding:14px;margin-bottom:16px;border-radius:4px;">
  <div style="font-weight:bold;color:#9a3412;margin-bottom:8px;font-size:14px;">📝 Flow AI 답변(본문)</div>
  {_format_ai_html(flow_text)}
</div>"""

    def _render_calendar_block(label: str, events: list[dict]) -> str:
        if not events:
            return f"""
<div style="flex:1;min-width:280px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:10px;padding:14px;">
  <div style="font-weight:bold;color:#374151;margin-bottom:8px;">📅 {label}</div>
  <p style="color:#9ca3af;font-size:13px;margin:0;">일정 없음</p>
</div>"""
        rows = ""
        for ev in events:
            color = "#1a1a1a" if ev["attending"] else "#9ca3af"
            weight = "bold" if ev["attending"] else "normal"
            badge = (
                '<span style="background:#8B00EA;color:#fff;font-size:11px;'
                'padding:1px 6px;border-radius:10px;margin-left:6px;">참석</span>'
                if ev["attending"] else ""
            )
            rows += (
                f'<tr>'
                f'<td style="padding:5px 10px;color:#6b7280;width:60px;white-space:nowrap;">{ev["time"]}</td>'
                f'<td style="padding:5px 10px;color:{color};font-weight:{weight};">'
                f'{ev["title"]}{badge}</td>'
                f'</tr>\n'
            )
        return f"""
<div style="flex:1;min-width:280px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:10px;padding:14px;">
  <div style="font-weight:bold;color:#374151;margin-bottom:8px;">📅 {label}</div>
  <table style="border-collapse:collapse;width:100%;font-size:13px;">{rows}</table>
</div>"""

    section_b = f"""
<div style="margin-bottom:16px;">
  <div style="font-weight:bold;color:#374151;margin-bottom:10px;">🗓️ 일정</div>
  <div style="display:flex;gap:12px;flex-wrap:wrap;">
    {_render_calendar_block("오늘 일정", today_events)}
    {_render_calendar_block("내일 일정", tomorrow_events)}
  </div>
</div>"""

    # 섹션 C: KPI 현황 (요청에 따라 기본 비노출)
    if _BRIEFING_INCLUDE_KPI_SECTION:
        if kpi_url:
            kpi_html_body = (
                f'<a href="{html.escape(kpi_url.strip(), quote=True)}" target="_blank" rel="noopener noreferrer" '
                f'style="display:inline-block;background:#16a34a;color:#fff;text-decoration:none;padding:10px 14px;border-radius:6px;font-size:13px;">'
                'KPI 답변 열기</a>'
                '<div style="color:#6b7280;font-size:12px;margin-top:8px;">※ Flow 로그인 필요</div>'
            )
            section_c = f"""
<div style="background:#f0fdf4;border-left:4px solid #22c55e;padding:14px;margin-bottom:16px;border-radius:4px;">
  <div style="font-weight:bold;color:#16a34a;margin-bottom:8px;font-size:14px;">📈 KPI 현황</div>
  {kpi_html_body}
  {f'<div style="margin-top:12px;">{_format_ai_html(kpi_text)}</div>' if kpi_text else ''}
</div>"""
        else:
            section_c = f"""
<div style="background:#f0fdf4;border-left:4px solid #22c55e;padding:14px;margin-bottom:16px;border-radius:4px;">
  <div style="font-weight:bold;color:#16a34a;margin-bottom:8px;font-size:14px;">📈 KPI 현황</div>
  {f'<div style="margin-top:12px;">{_format_ai_html(kpi_text)}</div>' if kpi_text else '<div style=\"color:#9ca3af;font-size:13px;\">KPI 답변 없음</div>'}
</div>"""
    else:
        section_c = ""

    # 섹션 D: 타임스탬프
    section_d = f"""
<p style="color:#9ca3af;font-size:11px;margin-top:20px;">
  자동 생성: {now_kst.strftime('%Y-%m-%d %H:%M')} KST
</p>"""

    return f"""<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:'Malgun Gothic',Arial,sans-serif;margin:0;padding:20px;background:#f9fafb;">
<div style="max-width:720px;margin:0 auto;background:#fff;padding:24px;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.1);">
  <h2 style="margin:0 0 20px;color:#111827;font-size:18px;">
    🌅 오전 브리핑 — {run_date}({wd})
  </h2>
  {section_a}
  {section_b}
  {section_c}
  {section_d}
</div>
</body></html>"""


# ============================================================
# Airflow task 함수
# ============================================================

def _run_kst(context) -> pendulum.DateTime:
    dt = context.get("data_interval_end") or context.get("logical_date")
    if dt is None:
        return pendulum.now("Asia/Seoul")
    try:
        return pendulum.instance(dt).in_tz("Asia/Seoul")
    except Exception:
        # dt may already be pendulum; fallback is safe
        return pendulum.parse(str(dt)).in_tz("Asia/Seoul")


def run_briefing(**context):
    """캘린더 수집 → Flow AI 메인 검색 → Flow AI KPI 검색 → XCom 반환"""
    driver = None
    result = {
        "flow_ai": {"url": "", "text": ""},
        "kpi_ai": {"url": "", "text": ""},
        "calendar_events": {"today": [], "tomorrow": []},
        "error": "",
    }
    try:
        run_kst = _run_kst(context)
        target_date = run_kst.to_date_string()

        for attempt in range(1, 3):
            driver = _launch_browser()
            if _do_login(driver):
                break
            try:
                driver.quit()
            except Exception:
                pass
            driver = None
            logger.warning("Flow 로그인 재시도 (%d/2)", attempt)
            time.sleep(2.0)

        if not driver:
            raise RuntimeError("Flow 로그인 실패")

        result["calendar_events"] = _collect_calendar(driver, target_date=target_date)
        result["flow_ai"] = _collect_flow_ai(driver, _FLOW_SEARCH_QUERY, label="메인", max_wait=120)
        result["kpi_ai"] = _collect_flow_ai(driver, _KPI_SEARCH_QUERY, label="KPI", max_wait=150)

    except Exception as e:
        logger.warning("run_briefing 실패: %s", e)
        result["error"] = str(e)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
            logger.info("브라우저 종료")

    return result


def send_briefing(**context):
    """브리핑 HTML 생성 + 이메일 발송"""
    from modules.transform.utility.mailer import send_email

    data = context["ti"].xcom_pull(task_ids="run_briefing") or {}
    flow_ai         = data.get("flow_ai") or {}
    kpi_ai          = data.get("kpi_ai") or {}
    calendar_events = data.get("calendar_events") or {"today": [], "tomorrow": []}
    error_message   = data.get("error") or ""
    if isinstance(calendar_events, dict):
        cal_count = len(calendar_events.get("today") or []) + len(calendar_events.get("tomorrow") or [])
    else:
        cal_count = len(calendar_events)

    run_kst = _run_kst(context)
    run_date = run_kst.strftime("%Y-%m-%d")
    weekday_kr = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}
    wd = weekday_kr[run_kst.weekday()]

    logger.info(
        "send_briefing 시작 — flow: %d자, kpi: %d자, 캘린더: %d건",
        len((flow_ai.get("text") or "")), len((kpi_ai.get("text") or "")), cal_count,
    )

    html = _build_briefing_html(flow_ai, kpi_ai, calendar_events, run_date, error_message=error_message)
    subject = f"[브리핑] {run_date}({wd}) 오전 업무 우선순위"

    try:
        send_email(subject=subject, html_content=html, to_emails=_BRIEFING_RECIPIENT)
        logger.info("send_briefing 완료: %s", subject)
        return f"발송 완료: {subject}"
    except Exception as e:
        # SMTP/DNS 문제로 이메일 발송이 불가해도, 브리핑 내용은 로그에서 확인 가능하게 한다.
        logger.exception("send_briefing 이메일 발송 실패(로그로 대체): %r", e)

        flow_text = (flow_ai.get("text") or "").strip()
        kpi_text = (kpi_ai.get("text") or "").strip()

        logger.info("=== MORNING_BRIEFING_FALLBACK_START ===")
        logger.info("subject=%s", subject)
        if error_message:
            logger.info("run_briefing_error=%s", error_message)
        logger.info("[FLOW_AI_TEXT]\n%s", flow_text if flow_text else "(EMPTY)")
        logger.info("[KPI_TEXT]\n%s", kpi_text if kpi_text else "(EMPTY)")
        if isinstance(calendar_events, dict):
            today_lines = "\n".join(
                [f'- {ev.get("time", "")}: {ev.get("title", "")}' for ev in (calendar_events.get("today") or [])]
            ) or "(EMPTY)"
            tomorrow_lines = "\n".join(
                [f'- {ev.get("time", "")}: {ev.get("title", "")}' for ev in (calendar_events.get("tomorrow") or [])]
            ) or "(EMPTY)"
            logger.info("[CALENDAR_TODAY]\n%s", today_lines)
            logger.info("[CALENDAR_TOMORROW]\n%s", tomorrow_lines)
        elif calendar_events:
            cal_lines = "\n".join([f'- {ev.get("time", "")}: {ev.get("title", "")}' for ev in calendar_events])
            logger.info("[CALENDAR]\n%s", cal_lines)
        else:
            logger.info("[CALENDAR]\n(EMPTY)")
        logger.info("=== MORNING_BRIEFING_FALLBACK_END ===")

        # HTML 스냅샷 저장(컨테이너 내부)
        try:
            out_dir = Path("/opt/airflow/analytics/morning_briefing")
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"briefing_{run_date}.html"
            out_path.write_text(html, encoding="utf-8")
            logger.info("브리핑 HTML 스냅샷 저장: %s", out_path)
        except Exception:
            pass

        return f"이메일 발송 실패(로그로 대체): {subject}"
