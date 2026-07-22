"""
출근길 AI 브리핑 파이프라인

수집:
    - Flow Calendar 오늘 일정
    - 오늘 FAIL/WARN DAG (모니터링 CSV)
    - git status / branch / log
    - daily_summary.parquet 신선도

생성:
    - gpt-oss로 각 FAIL DAG 원인 분석 (Step A)
    - gpt-oss로 전체 우선순위 브리핑 (Step B)

발송:
    - Telegram (modules.transform.utility.telegram)
"""

import json
import logging
import multiprocessing
import os
import queue
import re
from datetime import datetime
from pathlib import Path

import pendulum

logger = logging.getLogger(__name__)

# morning_briefing_pipeline.py는 modules/transform/pipelines/strategy/ 에 위치
# → parent×5 = airflow 프로젝트 루트
_GIT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent

_FLOW_CALENDAR_COLLECTION_TIMEOUT_SEC = 600
_FLOW_CALENDAR_PROCESS_TIMEOUT_SEC = int(
    os.getenv("FLOW_CALENDAR_PROCESS_TIMEOUT_SEC", str(_FLOW_CALENDAR_COLLECTION_TIMEOUT_SEC + 60))
)
_FLOW_CALENDAR_NAV_TIMEOUT_SEC = int(os.getenv("FLOW_CALENDAR_NAV_TIMEOUT_SEC", "120"))
_FLOW_TEAM_LOGIN_WAIT_SEC = 15


# ============================================================
# 수집 헬퍼
# ============================================================

def _collect_calendar_events_worker(result_queue) -> None:
    try:
        result_queue.put({"ok": True, "events": _collect_calendar_events_inner()})
    except BaseException as e:
        result_queue.put({"ok": False, "error": repr(e)})


def _collect_calendar_events() -> tuple[list[dict], bool]:
    """Flow Calendar 수집을 격리한다. 멈추거나 실패하면 빈 목록으로 브리핑을 계속한다."""
    if multiprocessing.current_process().daemon:
        logger.warning("Flow Calendar 수집 프로세스 분리 불가(daemon task) - 현재 태스크에서 직접 수집")
        try:
            return _collect_calendar_events_inner(), True
        except BaseException as e:
            logger.warning("Flow Calendar 직접 수집 실패: %s", repr(e))
            return [], False

    ctx = multiprocessing.get_context("fork" if hasattr(os, "fork") else "spawn")
    result_queue = ctx.Queue(maxsize=1)
    process = ctx.Process(target=_collect_calendar_events_worker, args=(result_queue,))
    process.start()
    process.join(_FLOW_CALENDAR_PROCESS_TIMEOUT_SEC)

    if process.is_alive():
        logger.warning(
            "Flow Calendar 수집 프로세스 시간 초과(%s초) - 일정 수집 건너뜀",
            _FLOW_CALENDAR_PROCESS_TIMEOUT_SEC,
        )
        process.terminate()
        process.join(5)
        if process.is_alive():
            try:
                process.kill()
            except AttributeError:
                pass
            process.join(5)
        return [], False

    try:
        payload = result_queue.get(timeout=10)
    except queue.Empty:
        if process.exitcode not in (0, None):
            logger.warning("Flow Calendar 수집 프로세스 비정상 종료: exitcode=%s", process.exitcode)
        return [], False

    if not payload.get("ok"):
        logger.warning("Flow Calendar 수집 프로세스 실패: %s", payload.get("error", "unknown"))
        return [], False

    events = payload.get("events") or []
    return (events if isinstance(events, list) else []), True


def _collect_calendar_events_inner() -> list[dict]:
    """Flow Calendar 오늘 일정 수집. 실패하면 빈 목록 반환."""
    driver = None
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import Select
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from modules.transform.pipelines.sales.SMD_sales_visit_log_01_crawling import (
            do_login,
        )

        import time

        deadline = time.monotonic() + _FLOW_CALENDAR_COLLECTION_TIMEOUT_SEC
        target_date = pendulum.now("Asia/Seoul").to_date_string()
        driver = _launch_flow_calendar_browser()
        driver.set_window_size(1920, 1080)
        if not do_login(driver):
            logger.warning("Flow 로그인 1차 실패 — deadline 내 1회 재시도")
            if time.monotonic() >= deadline:
                raise RuntimeError("Flow 로그인 실패")
            time.sleep(3)
            if not do_login(driver):
                logger.warning("Flow 로그인 재시도 실패 — 일정 수집 건너뜀")
                raise RuntimeError("Flow 로그인 실패")
        _configure_flow_driver_timeouts(driver)

        if time.monotonic() >= deadline:
            logger.warning("Flow Calendar 수집 시간 초과 — 일정 수집 건너뜀")
            return []

        wait = WebDriverWait(driver, 20)
        calendar_urls = [
            "https://flow.team/main.act?prjSchd",
            "https://team-0aay3p.flow.team/main.act?prjSchd",
        ]
        collection_errors = 0
        for calendar_url in calendar_urls:
            if time.monotonic() >= deadline:
                logger.warning("Flow Calendar 수집 시간 초과 — 남은 URL 건너뜀")
                raise RuntimeError("Flow Calendar 수집 실패")
            try:
                driver.get(calendar_url)
                time.sleep(5)
                if "team-0aay3p.flow.team" in calendar_url:
                    _login_flow_team_page_if_needed(
                        driver,
                        max_attempts=1,
                        wait_seconds=_FLOW_TEAM_LOGIN_WAIT_SEC,
                    )
                    driver.get(calendar_url)
                    time.sleep(5)

                _select_flow_calendar_filters(driver)
                time.sleep(5)
                calendar_type = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "select#calendarType")))
                Select(calendar_type).select_by_value("listMonth")
                time.sleep(5)
                _select_flow_calendar_filters(driver)
                time.sleep(5)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".fc-listMonth-view .fc-list-table")))
                page_source = driver.page_source
                events = _extract_flow_calendar_events_from_html(page_source, target_date)
                if (
                    events
                    and not _calendar_events_have_status_source(events)
                    and not _html_has_calendar_status_tags(page_source, target_date)
                ):
                    logger.warning("Flow Calendar listMonth 상태 태그 0건 — 필터 강제 토글 후 1회 재추출")
                    _force_toggle_flow_calendar_filters(driver)
                    time.sleep(5)
                    page_source = driver.page_source
                    events = _extract_flow_calendar_events_from_html(page_source, target_date)
                    if (
                        events
                        and not _calendar_events_have_status_source(events)
                        and not _html_has_calendar_status_tags(page_source, target_date)
                    ):
                        logger.warning("Flow Calendar listMonth 재추출 후에도 상태 태그 0건 — 현재 결과로 진행")
                if events:
                    return events

                logger.warning("Flow Calendar listMonth 추출 0건 — weekView fallback 시도")
                events = _collect_flow_calendar_events_from_week_view(driver, target_date, deadline=deadline)
                if events:
                    return events
            except Exception as e:
                collection_errors += 1
                logger.warning("Flow Calendar URL 수집 실패 후 fallback 진행: %s (%s)", calendar_url, e)
                events = _collect_flow_calendar_events_from_week_view(driver, target_date, deadline=deadline)
                if events:
                    return events
                continue

        if collection_errors >= len(calendar_urls):
            raise RuntimeError("Flow Calendar 수집 실패")
        return []
    except Exception as e:
        if isinstance(e, RuntimeError) and str(e) in {"Flow 로그인 실패", "Flow Calendar 수집 실패"}:
            raise
        logger.warning(f"Flow Calendar 수집 실패: {e}")
        return []
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def _launch_flow_calendar_browser():
    """Flow 캘린더 수집용 Chrome 실행."""
    from modules.transform.pipelines.sales.SMD_sales_visit_log_01_crawling import launch_browser

    return launch_browser()


def _configure_flow_driver_timeouts(driver) -> None:
    try:
        driver.set_page_load_timeout(_FLOW_CALENDAR_NAV_TIMEOUT_SEC)
        driver.set_script_timeout(_FLOW_CALENDAR_NAV_TIMEOUT_SEC)
    except Exception as e:
        logger.info("Flow Calendar WebDriver timeout 설정 실패(무시): %s", e)


def _select_flow_calendar_filters(driver) -> None:
    """Flow 캘린더에서 일정 전체 + 업무 전체를 선택한다."""
    try:
        result = driver.execute_script(
            """
            function isActive(el) {
              var c = ' ' + (el.className || '') + ' ';
              var icon = el ? el.querySelector('.js-common-radio') : null;
              var iconClass = ' ' + (icon ? icon.className || '' : '') + ' ';
              return c.indexOf(' on ') >= 0 ||
                     c.indexOf(' active ') >= 0 ||
                     c.indexOf(' selected ') >= 0 ||
                     c.indexOf(' all-checked ') >= 0 ||
                     iconClass.indexOf(' all-checked ') >= 0;
            }
            function state(el) {
              var icon = el ? el.querySelector('.js-common-radio') : null;
              return {
                className: el ? (el.className || '') : '',
                iconClassName: icon ? (icon.className || '') : ''
              };
            }
            function ensureActive(el) {
              if (!el) return {found: false, clicked: false, before: '', after: '', beforeIcon: '', afterIcon: ''};
              var before = state(el);
              var clicked = false;
              if (!isActive(el)) {
                clicked = true;
                try { el.click(); }
                catch (e) { el.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, view:window})); }
              }
              var after = state(el);
              return {
                found: true,
                clicked: clicked,
                before: before.className,
                after: after.className,
                beforeIcon: before.iconClassName,
                afterIcon: after.iconClassName
              };
            }
            var scheduleAll = document.querySelector('#scheduleFilter .js-filter-button[gubun="0,1"]');
            var taskAll = document.querySelector('#taskFilter .js-filter-button[gubun="2"]');
            return {schedule: ensureActive(scheduleAll), task: ensureActive(taskAll)};
            """
        ) or {}
        schedule = result.get("schedule") or {}
        task = result.get("task") or {}
        logger.info(
            "Flow Calendar 필터 선택: 일정전체 found=%s clicked=%s class='%s'->'%s' icon='%s'->'%s' 업무전체 found=%s clicked=%s class='%s'->'%s' icon='%s'->'%s'",
            schedule.get("found", False),
            schedule.get("clicked", False),
            schedule.get("before", ""),
            schedule.get("after", ""),
            schedule.get("beforeIcon", ""),
            schedule.get("afterIcon", ""),
            task.get("found", False),
            task.get("clicked", False),
            task.get("before", ""),
            task.get("after", ""),
            task.get("beforeIcon", ""),
            task.get("afterIcon", ""),
        )
    except Exception as e:
        logger.warning("Flow Calendar 필터 선택 실패(현재 필터로 진행): %s", e)


def _force_toggle_flow_calendar_filters(driver) -> None:
    """상태 태그가 전혀 없을 때 필터가 켜진 상태인지 다시 보장한다."""
    try:
        result = driver.execute_script(
            """
            function isActive(el) {
              var c = ' ' + (el.className || '') + ' ';
              var icon = el ? el.querySelector('.js-common-radio') : null;
              var iconClass = ' ' + (icon ? icon.className || '' : '') + ' ';
              return c.indexOf(' on ') >= 0 ||
                     c.indexOf(' active ') >= 0 ||
                     c.indexOf(' selected ') >= 0 ||
                     c.indexOf(' all-checked ') >= 0 ||
                     iconClass.indexOf(' all-checked ') >= 0;
            }
            function state(el) {
              var icon = el ? el.querySelector('.js-common-radio') : null;
              return {
                className: el ? (el.className || '') : '',
                iconClassName: icon ? (icon.className || '') : ''
              };
            }
            function ensureActive(el) {
              if (!el) return {found: false, clicked: false, before: '', after: '', beforeIcon: '', afterIcon: ''};
              var before = state(el);
              var clicked = false;
              if (!isActive(el)) {
                clicked = true;
                try { el.click(); }
                catch (e) { el.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, view:window})); }
              }
              var after = state(el);
              return {
                found: true,
                clicked: clicked,
                before: before.className,
                after: after.className,
                beforeIcon: before.iconClassName,
                afterIcon: after.iconClassName
              };
            }
            var scheduleAll = document.querySelector('#scheduleFilter .js-filter-button[gubun="0,1"]');
            var taskAll = document.querySelector('#taskFilter .js-filter-button[gubun="2"]');
            return {schedule: ensureActive(scheduleAll), task: ensureActive(taskAll)};
            """
        ) or {}
        schedule = result.get("schedule") or {}
        task = result.get("task") or {}
        logger.warning(
            "Flow Calendar 필터 재보장: 일정전체 found=%s clicked=%s class='%s'->'%s' icon='%s'->'%s' 업무전체 found=%s clicked=%s class='%s'->'%s' icon='%s'->'%s'",
            schedule.get("found", False),
            schedule.get("clicked", False),
            schedule.get("before", ""),
            schedule.get("after", ""),
            schedule.get("beforeIcon", ""),
            schedule.get("afterIcon", ""),
            task.get("found", False),
            task.get("clicked", False),
            task.get("before", ""),
            task.get("after", ""),
            task.get("beforeIcon", ""),
            task.get("afterIcon", ""),
        )
    except Exception as e:
        logger.warning("Flow Calendar 필터 재보장 실패(현재 필터로 진행): %s", e)


def _login_flow_team_page_if_needed(
    driver,
    *,
    max_attempts: int = 1,
    wait_seconds: int = _FLOW_TEAM_LOGIN_WAIT_SEC,
) -> None:
    """팀 서브도메인 접근 시 corp signin이 뜨면 기존 Flow 계정으로 한 번 더 로그인한다."""
    current_url = (getattr(driver, "current_url", "") or "").lower()
    if "signin" not in current_url and "corpsignin.act" not in current_url:
        return

    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from modules.transform.pipelines.sales.SMD_sales_visit_log_01_crawling import (
            FLOW_ID,
            FLOW_PW,
        )

        wait = WebDriverWait(driver, wait_seconds)

        def _first_visible(selector: str):
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
            for element in driver.find_elements(By.CSS_SELECTOR, selector):
                try:
                    if element.is_displayed() and element.is_enabled():
                        return element
                except Exception:
                    continue
            return driver.find_elements(By.CSS_SELECTOR, selector)[0]

        def _set_input(element, value: str) -> None:
            driver.execute_script(
                """
                const el = arguments[0];
                const value = arguments[1];
                el.focus();
                el.value = value;
                el.dispatchEvent(new Event('input', {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
                """,
                element,
                value,
            )

        for attempt in range(max_attempts):
            if attempt:
                driver.get("https://team-0aay3p.flow.team/corpsignin.act")
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#userId, input[name='userId'], input[type='email']")))

            id_input = _first_visible("input#userId, input[name='userId'], input[type='email']")
            pw_input = _first_visible("input#password, input[name='password'], input[type='password']")
            _set_input(id_input, FLOW_ID)
            _set_input(pw_input, FLOW_PW)

            login_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a#normalLoginButton, button#normalLoginButton")))
            driver.execute_script("arguments[0].click();", login_button)
            try:
                wait.until(lambda d: "signin" not in (d.current_url or "").lower() and "corpsignin.act" not in (d.current_url or "").lower())
                return
            except Exception:
                logger.warning("Flow 팀 서브도메인 추가 로그인 실패 (%s/%s, url=%s)", attempt + 1, max_attempts, driver.current_url)
        logger.warning("Flow 팀 서브도메인 추가 로그인 실패: 제한 시간 초과")
    except Exception as e:
        logger.warning(f"Flow 팀 서브도메인 추가 로그인 실패: {e}")


def _normalize_calendar_event_text(text: str | None) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


_CALENDAR_STATUS_ALIASES = {
    "진행": "진행",
    "진행중": "진행",
    "진행 중": "진행",
    "세부업무": "세부업무",
    "테스트": "테스트",
    "피드백": "피드백",
    "결제중": "결제중",
    "결제 중": "결제중",
}
_CALENDAR_NON_STATUS_PREFIX_TAGS = {"요청"}
_CALENDAR_SECTION_LABELS = ("진행", "피드백", "결제중", "테스트", "세부업무")
_CALENDAR_STATUS_TAG_RE = re.compile(r"^\s*\[([^\]]+)\]\s*")
_CALENDAR_EXCLUDED_STATUS_TAGS = {"완료", "보류"}


def _calendar_events_have_status_source(events: list[dict]) -> bool:
    return any((event.get("status_source") == "tag") for event in events or [])


def _calendar_text_has_known_status_tag(text: str | None) -> bool:
    known_tags = set(_CALENDAR_STATUS_ALIASES) | _CALENDAR_EXCLUDED_STATUS_TAGS
    for tag in re.findall(r"\[([^\]]+)\]", text or ""):
        if tag.strip() in known_tags:
            return True
    return False


def _html_has_calendar_status_tags(html: str, target_date: str) -> bool:
    try:
        from bs4 import BeautifulSoup
    except Exception as e:
        logger.warning("Flow Calendar 상태 태그 검증용 HTML 파서 로드 실패: %s", e)
        return False

    soup = BeautifulSoup(html, "html.parser")
    heading = soup.select_one(f"tr.fc-list-heading[data-date='{target_date}']")
    if heading is None:
        return False

    row = heading.find_next_sibling("tr")
    while row is not None:
        classes = row.get("class", [])
        if "fc-list-heading" in classes:
            break
        if "fc-list-item" in classes:
            row_text = " ".join(
                value
                for value in [
                    row.get("mouseover-text"),
                    row.get_text(" ", strip=True),
                ]
                if value
            )
            if _calendar_text_has_known_status_tag(row_text):
                return True
        row = row.find_next_sibling("tr")
    return False


def _extract_calendar_event_status_from_tags(*texts: str | None) -> str | None:
    for text in texts:
        normalized = _normalize_calendar_event_text(text)
        if normalized in _CALENDAR_STATUS_ALIASES:
            return _CALENDAR_STATUS_ALIASES[normalized]
        while normalized:
            match = _CALENDAR_STATUS_TAG_RE.match(normalized)
            if not match:
                break
            tag = match.group(1).strip()
            if tag in _CALENDAR_NON_STATUS_PREFIX_TAGS:
                normalized = normalized[match.end():].strip()
                continue
            status = _CALENDAR_STATUS_ALIASES.get(tag)
            if status:
                return status
            break
    return None


def _has_excluded_calendar_status(*texts: str | None) -> bool:
    for text in texts:
        normalized = _normalize_calendar_event_text(text)
        if normalized in _CALENDAR_EXCLUDED_STATUS_TAGS:
            return True
        while normalized:
            match = _CALENDAR_STATUS_TAG_RE.match(normalized)
            if not match:
                break
            tag = match.group(1).strip()
            if tag in _CALENDAR_NON_STATUS_PREFIX_TAGS:
                normalized = normalized[match.end():].strip()
                continue
            if tag in _CALENDAR_EXCLUDED_STATUS_TAGS:
                return True
            break
    return False


def _find_calendar_event_context_prefix(summary: str | None, row_text: str | None) -> str:
    normalized_summary = _normalize_calendar_event_text(summary)
    normalized_row = _normalize_calendar_event_text(row_text)
    if not normalized_summary or not normalized_row:
        return ""
    index = normalized_row.find(normalized_summary)
    if index < 0:
        stripped_summary = _strip_calendar_status_prefix(normalized_summary)
        index = normalized_row.find(stripped_summary) if stripped_summary else -1
    if index < 0:
        return ""
    return normalized_row[max(0, index - 160):index]


def _extract_calendar_event_status_from_context(summary: str | None, row_text: str | None) -> str | None:
    prefix = _find_calendar_event_context_prefix(summary, row_text)
    if not prefix:
        return None
    for tag in reversed(re.findall(r"\[([^\]]+)\]", prefix)):
        normalized_tag = tag.strip()
        if normalized_tag in _CALENDAR_NON_STATUS_PREFIX_TAGS:
            continue
        status = _CALENDAR_STATUS_ALIASES.get(normalized_tag)
        if status:
            return status
        if normalized_tag in _CALENDAR_EXCLUDED_STATUS_TAGS:
            return normalized_tag
    return None


def _extract_calendar_event_status(*texts: str | None) -> str:
    status = _extract_calendar_event_status_from_tags(*texts)
    if status:
        return status
    return "진행"


def _calendar_node_data_attrs(node) -> dict[str, str]:
    if not node:
        return {}
    return {
        str(key): str(value)
        for key, value in node.attrs.items()
        if str(key).startswith("data-") and value is not None
    }


def _calendar_status_text_from_candidates(*texts: str | None) -> str:
    for text in texts:
        normalized = _normalize_calendar_event_text(text)
        while normalized:
            match = _CALENDAR_STATUS_TAG_RE.match(normalized)
            if not match:
                break
            tag = match.group(1).strip()
            if tag in _CALENDAR_NON_STATUS_PREFIX_TAGS:
                normalized = normalized[match.end():].strip()
                continue
            if tag in _CALENDAR_STATUS_ALIASES or tag in _CALENDAR_EXCLUDED_STATUS_TAGS:
                return tag
            break
    return ""


def _strip_calendar_status_prefix(summary: str) -> str:
    cleaned = _normalize_calendar_event_text(summary)
    while True:
        match = _CALENDAR_STATUS_TAG_RE.match(cleaned)
        if not match:
            return cleaned
        tag = match.group(1).strip()
        if tag not in _CALENDAR_STATUS_ALIASES and tag not in _CALENDAR_NON_STATUS_PREFIX_TAGS:
            return cleaned
        cleaned = cleaned[match.end():].strip()


def _dedupe_briefing_calendar_events(events: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen: set[tuple[str, str]] = set()
    default_status_summaries: list[str] = []
    for event in events or []:
        time_label = _normalize_calendar_event_text(event.get("time")) or "종일"
        raw_summary = _normalize_calendar_event_text(event.get("summary"))
        summary = _strip_calendar_status_prefix(raw_summary)
        if not summary or _has_excluded_calendar_status(
            raw_summary,
            event.get("status_text"),
        ):
            continue
        context_status = _extract_calendar_event_status_from_context(raw_summary, event.get("row_text"))
        if context_status in _CALENDAR_EXCLUDED_STATUS_TAGS:
            continue
        tag_status = _extract_calendar_event_status_from_tags(
            raw_summary,
            event.get("status_text"),
        ) or context_status
        event_status = tag_status or "진행"
        if not tag_status:
            default_status_summaries.append(raw_summary or summary)
        key = (time_label, summary)
        if key in seen:
            continue
        seen.add(key)
        deduped.append({
            "time": time_label,
            "summary": summary,
            "status": event_status,
            "status_source": "tag" if tag_status else "default",
        })
    if default_status_summaries:
        logger.warning(
            "Flow Calendar 상태 태그 미검출 기본값 적용: count=%s samples=%s",
            len(default_status_summaries),
            default_status_summaries[:5],
        )
    deduped.sort(key=lambda e: (0 if e["time"] == "종일" else 1, e["time"], e["summary"]))
    return deduped


def _format_calendar_sections(events: list[dict]) -> str:
    return "\n".join(f"  {e['time']}  {e['summary']}" for e in events or [])


def _collect_flow_calendar_events_from_week_view(driver, target_date: str, deadline: float | None = None) -> list[dict]:
    """Flow calendar.act 주간뷰에서 target_date 컬럼의 일정만 읽는다."""
    try:
        import time
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import Select
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC

        wait = WebDriverWait(driver, 20)
        calendar_urls = [
            "https://flow.team/calendar.act",
            "https://flow.team/main.act?calendar",
            "https://team-0aay3p.flow.team/calendar.act",
        ]

        for calendar_url in calendar_urls:
            if deadline is not None and time.monotonic() >= deadline:
                logger.warning("Flow Calendar weekView 수집 시간 초과 — 남은 URL 건너뜀")
                return []
            try:
                driver.get(calendar_url)
                time.sleep(3)
                if "team-0aay3p.flow.team" in calendar_url:
                    _login_flow_team_page_if_needed(
                        driver,
                        max_attempts=1,
                        wait_seconds=_FLOW_TEAM_LOGIN_WAIT_SEC,
                    )
                    driver.get(calendar_url)
                    time.sleep(3)

                try:
                    select_el = wait.until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, "#calendarSelectBoxItem select.select-box, select#calendarType")
                        )
                    )
                    value = (select_el.get_attribute("value") or "").strip()
                    if value != "weekView":
                        try:
                            Select(select_el).select_by_value("weekView")
                        except Exception:
                            Select(select_el).select_by_value("agendaWeek")
                        time.sleep(2)
                except Exception as e:
                    logger.info("Flow Calendar weekView 전환 실패, 현재 뷰로 진행: %s", e)

                try:
                    wait.until(
                        EC.presence_of_element_located(
                            (
                                By.CSS_SELECTOR,
                                f".fc-day-header[data-date='{target_date}'], "
                                f".fc-day-top[data-date='{target_date}'], "
                                f"td[data-date='{target_date}']",
                            )
                        )
                    )
                except Exception:
                    _click_flow_calendar_today(driver)
                    time.sleep(2)

                raw_events = driver.execute_script(
                    """
                    return (function(dateStr) {
                      function norm(text) { return (text || '').replace(/\\s+/g, ' ').trim(); }
                      function rect(el) {
                        if (!el) return null;
                        var r = el.getBoundingClientRect();
                        return {left:r.left, right:r.right, top:r.top, bottom:r.bottom, cx:(r.left+r.right)/2, cy:(r.top+r.bottom)/2};
                      }
                      function sameColumn(eventRect, dateRect) {
                        if (!eventRect || !dateRect) return false;
                        return eventRect.cx >= dateRect.left && eventRect.cx < dateRect.right;
                      }
                      function inDayCell(eventRect, dateRect) {
                        if (!eventRect || !dateRect) return false;
                        return eventRect.cx >= dateRect.left && eventRect.cx < dateRect.right &&
                               eventRect.cy >= dateRect.top && eventRect.cy < dateRect.bottom;
                      }
                      function titleOf(el) {
                        var titleEl = el.querySelector('.fc-list-item-title a, .fc-list-item-title, .fc-title, .fc-event-title');
                        var raw = norm((titleEl ? titleEl.textContent : '') || el.getAttribute('title') || el.getAttribute('mouseover-text') || el.textContent);
                        var timeEl = el.querySelector('.fc-time, .fc-event-time');
                        var timeText = norm(timeEl ? (timeEl.getAttribute('data-full') || timeEl.textContent) : '');
                        if (timeText && raw.indexOf(timeText) === 0) raw = norm(raw.slice(timeText.length));
                        return raw;
                      }
                      function timeOf(el) {
                        var timeEl = el.querySelector('.fc-time, .fc-event-time, .fc-list-item-time');
                        return norm(timeEl ? (timeEl.getAttribute('data-full') || timeEl.textContent) : '') || '종일';
                      }
                      function statusOf(title) {
                        var match = (title || '').match(/^\\s*\\[([^\\]]+)\\]/);
                        return match ? match[1].trim() : '';
                      }

                      var dateEl = document.querySelector(".fc-day-header[data-date='" + dateStr + "'], .fc-day-top[data-date='" + dateStr + "']");
                      var dateRect = rect(dateEl);
                      var dayCell = document.querySelector("td[data-date='" + dateStr + "']");
                      var dayRect = rect(dayCell);
                      var out = [];
                      Array.prototype.slice.call(document.querySelectorAll('a.fc-event, .fc-event, tr.fc-list-item')).forEach(function(el) {
                        if (el.closest('.fc-popover, .fc-more-popover')) return;
                        var er = rect(el);
                        var matched = dateRect ? sameColumn(er, dateRect) : false;
                        if (!matched && dayRect) matched = inDayCell(er, dayRect);
                        if (!matched && el.getAttribute('data-date') === dateStr) matched = true;
                        if (!matched) return;
                        var title = titleOf(el);
                        if (!title) return;
                        var rowText = norm(el.textContent);
                        out.push({
                          time: timeOf(el),
                          summary: title,
                          status_text: statusOf(title),
                          row_text: rowText
                        });
                      });
                      return out;
                    })(arguments[0]);
                    """,
                    target_date,
                ) or []
                events = _dedupe_briefing_calendar_events(raw_events)
                logger.info("Flow Calendar weekView fallback 결과: %d건", len(events))
                if events:
                    return events
            except Exception as e:
                logger.warning("Flow Calendar weekView URL 수집 실패 후 fallback 진행: %s (%s)", calendar_url, e)
                continue
    except Exception as e:
        logger.warning("Flow Calendar weekView fallback 실패: %s", e)
    return []


def _click_flow_calendar_today(driver) -> None:
    try:
        clicked = driver.execute_script(
            """
            var selectors = ['.fc-today-button', 'button[aria-label="today"]', 'button[title="today"]'];
            for (var i = 0; i < selectors.length; i++) {
              var el = document.querySelector(selectors[i]);
              if (!el) continue;
              try { el.click(); return true; } catch (e) {}
              try { el.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, view:window})); return true; } catch (e2) {}
            }
            return false;
            """
        )
        logger.info("Flow Calendar 오늘 버튼 클릭: %s", clicked)
    except Exception as e:
        logger.info("Flow Calendar 오늘 버튼 클릭 실패: %s", e)


def _extract_flow_calendar_events_from_html(html: str, target_date: str) -> list[dict]:
    """Flow 리스트 캘린더 HTML에서 target_date 일정만 추출한다."""
    try:
        from bs4 import BeautifulSoup
    except Exception as e:
        logger.warning(f"Flow Calendar HTML 파서 로드 실패: {e}")
        return []

    soup = BeautifulSoup(html, "html.parser")
    heading = soup.select_one(f"tr.fc-list-heading[data-date='{target_date}']")
    if heading is None:
        return []

    events: list[dict] = []
    row = heading.find_next_sibling("tr")
    while row is not None:
        classes = row.get("class", [])
        if "fc-list-heading" in classes:
            break
        if "fc-list-item" in classes:
            title_el = row.select_one(".fc-list-item-title a, .fc-list-item-title")
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                title = (row.get("title") or row.get("mouseover-text") or "").strip()
            if title:
                time_el = row.select_one(".fc-list-item-time")
                time_label = time_el.get_text(strip=True) if time_el else "종일"
                row_text = row.get_text(" ", strip=True)
                row_attrs = _calendar_node_data_attrs(row)
                title_attrs = _calendar_node_data_attrs(title_el)
                status_candidates = [
                    title,
                    row.get("mouseover-text"),
                    row.get("title"),
                    title_el.get("title") if title_el else None,
                    title_el.get("mouseover-text") if title_el else None,
                    row_text,
                    *row_attrs.values(),
                    *title_attrs.values(),
                ]
                status_text = _calendar_status_text_from_candidates(*status_candidates)
                if len(events) < 1:
                    logger.info("Flow Calendar raw row html sample: %s", str(row)[:3000])
                    logger.info(
                        "Flow Calendar status candidate sample: %s",
                        json.dumps(
                            {
                                "title": title,
                                "mouseover_text": row.get("mouseover-text"),
                                "row_title": row.get("title"),
                                "title_attrs": title_attrs,
                                "row_attrs": row_attrs,
                                "status_text": status_text,
                            },
                            ensure_ascii=False,
                        )[:2000],
                    )
                events.append({
                    "time": time_label or "종일",
                    "summary": title,
                    "status_text": status_text,
                    "row_text": row_text,
                })
        row = row.find_next_sibling("tr")
    logger.info(
        "Flow Calendar raw event sample: %s",
        json.dumps(events[:5], ensure_ascii=False)[:2000],
    )
    return _dedupe_briefing_calendar_events(events)


def _get_today_window_kst() -> tuple[pendulum.DateTime, pendulum.DateTime, str]:
    target_day = pendulum.now("Asia/Seoul")
    start_kst = target_day.start_of("day")
    end_kst = start_kst.add(days=1)
    return start_kst, end_kst, target_day.strftime("%Y-%m-%d")


def _normalize_run_dir_name(name: str) -> str:
    return name.replace("", ":")


def _find_run_log_dir(logs_base: Path, dag_id: str, run_id: str) -> Path | None:
    dag_dir = logs_base / f"dag_id={dag_id}"
    if not dag_dir.exists():
        return None
    expected = f"run_id={run_id}"
    for run_dir in dag_dir.glob("run_id=*"):
        if _normalize_run_dir_name(run_dir.name) == expected:
            return run_dir
    return None


def _find_latest_attempt_log(run_log_dir: Path | None, task_id: str) -> Path | None:
    if run_log_dir is None:
        return None
    task_dir = run_log_dir / f"task_id={task_id}"
    if not task_dir.exists():
        return None
    attempts = sorted(task_dir.glob("attempt=*.log"))
    return attempts[-1] if attempts else None


def _extract_error_details(log_path: Path | None) -> tuple[str, str]:
    if log_path is None or not log_path.exists():
        return "", ""

    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if not lines:
        return "", ""

    summary = ""
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped:
            continue
        lowered = stripped.lower()
        if "traceback" in lowered:
            continue
        if "error" in lowered or "exception" in lowered or stripped.startswith("KeyError"):
            summary = stripped[-220:]
            break

    traceback_start = None
    for index in range(len(lines) - 1, -1, -1):
        if "Traceback" in lines[index]:
            traceback_start = index
            break

    excerpt_lines = lines[traceback_start:traceback_start + 40] if traceback_start is not None else lines[-40:]
    excerpt = "\n".join(line.rstrip() for line in excerpt_lines if line.strip()).strip()
    if not summary and excerpt:
        summary = excerpt.splitlines()[-1][-220:]
    return summary[:220], excerpt[:2000]


def _collect_previous_day_dag_results() -> tuple[list[dict], list[dict]]:
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session
    import os

    start_kst, end_kst, _target_label = _get_today_window_kst()
    start_utc = start_kst.in_timezone("UTC")
    end_utc = end_kst.in_timezone("UTC")
    logs_base = Path(os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs"))
    excluded_dags = {"Private_MorningBriefing_Dags"}

    with create_session() as session:
        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.execution_date >= start_utc)
            .filter(DagRun.execution_date < end_utc)
            .all()
        )

        latest_runs: dict[str, object] = {}
        for dag_run in dag_runs:
            if dag_run.dag_id in excluded_dags:
                continue
            sort_key = dag_run.end_date or dag_run.start_date or dag_run.execution_date
            current = latest_runs.get(dag_run.dag_id)
            if current is None:
                latest_runs[dag_run.dag_id] = dag_run
                continue
            current_key = current.end_date or current.start_date or current.execution_date
            if sort_key and current_key:
                if sort_key >= current_key:
                    latest_runs[dag_run.dag_id] = dag_run
            elif sort_key and not current_key:
                latest_runs[dag_run.dag_id] = dag_run

        failures: list[dict] = []
        log_errors: list[dict] = []

        for dag_id, dag_run in sorted(latest_runs.items()):
            if str(getattr(dag_run, "state", "")) != "failed":
                continue

            run_id = str(dag_run.run_id)
            task_instances = (
                session.query(TaskInstance)
                .filter(TaskInstance.dag_id == dag_id)
                .filter(TaskInstance.run_id == run_id)
                .all()
            )
            failed_tasks = [ti for ti in task_instances if str(getattr(ti, "state", "")) in {"failed", "upstream_failed"}]
            if not failed_tasks:
                continue
            run_log_dir = _find_run_log_dir(logs_base, dag_id, run_id)

            error_summary = ""
            error_excerpt = ""
            message_entries: list[dict] = []

            for ti in failed_tasks:
                log_path = _find_latest_attempt_log(run_log_dir, str(ti.task_id))
                summary, excerpt = _extract_error_details(log_path)
                if summary and not error_summary:
                    error_summary = summary
                if excerpt and not error_excerpt:
                    error_excerpt = excerpt
                if summary:
                    entry = {"msg": summary[:200], "level": "ERROR"}
                    if entry not in message_entries:
                        message_entries.append(entry)

            if not error_summary:
                error_summary = f"실패 run: failed task {len(failed_tasks)}개"

            failures.append({
                "dag_id": dag_id,
                "status": "FAIL",
                "fail_type": "dag_failed",
                "error_summary": error_summary,
                "error_excerpt": error_excerpt,
            })

            if message_entries:
                log_errors.append({
                    "dag_id": dag_id,
                    "messages": message_entries[:3],
                })

    if not failures and not log_errors:
        # DB 조회 결과가 없을 때는 기존 dags_monitoring CSV로 fallback
        try:
            import csv
            from modules.transform.utility.paths import MART_DB

            today = pendulum.now("Asia/Seoul").strftime("%Y%m%d")
            csv_path = MART_DB / "dags_monitoring" / f"dags_monitoring_{today}.csv"
            if csv_path.exists():
                with csv_path.open("r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        status = (row.get("status", "") or "").upper()
                        if status not in {"FAIL", "WARN"}:
                            continue
                        excerpt = str(row.get("error_excerpt", ""))
                        failures.append(
                            {
                                "dag_id": str(row.get("dag_id", "")),
                                "status": status,
                                "fail_type": str(row.get("fail_type", "")),
                                "error_summary": str(row.get("error_summary", ""))[:500],
                                "error_excerpt": excerpt[:2000] if excerpt not in ("nan", "None", "") else "",
                            }
                        )
            else:
                logger.warning(f"모니터링 CSV 없음: {csv_path}")
        except Exception:
            logger.warning("dags_monitoring CSV fallback 실패")

    return failures, log_errors


def _collect_previous_day_dag_results_v2() -> tuple[list[dict], list[dict]]:
    failures, log_errors = _collect_previous_day_dag_results()
    if failures or log_errors:
        return failures, log_errors

    # 실패/상위실패 재시도 없이 마지막 상태를 보는 경우가 있으면, 실패 상태만 따로 보정
    from airflow.utils.session import create_session
    import os

    with create_session() as session:
        from airflow.models import DagRun, TaskInstance

        start_kst, end_kst, _ = _get_today_window_kst()
        start_utc = start_kst.in_timezone("UTC")
        end_utc = end_kst.in_timezone("UTC")
        logs_base = Path(os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/opt/airflow/logs"))

        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.execution_date >= start_utc)
            .filter(DagRun.execution_date < end_utc)
            .filter(DagRun.state.in_(["failed", "upstream_failed"]))
            .order_by(DagRun.execution_date.desc())
            .all()
        )

        seen: set[str] = set()
        for dr in dag_runs:
            if dr.dag_id in seen or dr.dag_id == "Private_MorningBriefing_Dags":
                continue
            seen.add(dr.dag_id)
            task_instances = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dr.dag_id, TaskInstance.run_id == dr.run_id
            ).all()
            failed_tasks = [ti for ti in task_instances if str(getattr(ti, "state", "")) in {"failed", "upstream_failed"}]
            if not failed_tasks:
                continue
            run_log_dir = _find_run_log_dir(logs_base, dr.dag_id, str(dr.run_id))

            msg_entries: list[dict] = []
            error_summary = ""
            error_excerpt = ""
            for ti in failed_tasks:
                log_path = _find_latest_attempt_log(run_log_dir, str(ti.task_id))
                summary, excerpt = _extract_error_details(log_path)
                if summary:
                    if not error_summary:
                        error_summary = summary
                    msg_entries.append({"msg": summary[:200], "level": "ERROR"})
                if excerpt and not error_excerpt:
                    error_excerpt = excerpt

            if not error_summary:
                error_summary = f"실패 run: failed task {len(failed_tasks)}개"
            if msg_entries:
                log_errors.append({"dag_id": dr.dag_id, "messages": msg_entries[:3]})
            failures.append({
                "dag_id": dr.dag_id,
                "status": "FAIL",
                "fail_type": "dag_failed",
                "error_summary": error_summary,
                "error_excerpt": error_excerpt,
            })

    return failures, log_errors


def _collect_dag_failures() -> list[dict]:
    failures, _ = _collect_previous_day_dag_results_v2()
    return failures


def _collect_git_status() -> dict:
    """.git 폴더 직접 파싱 — git 바이너리/gitpython 없이 브랜치·커밋 수집.
    loose refs(refs/heads/) + packed-refs 모두 처리."""
    git_dir = _GIT_ROOT / ".git"
    if not git_dir.exists():
        return {"status": "(git 없음)", "log": "(git 없음)", "unmerged_branches": "(git 없음)"}
    try:
        # 현재 브랜치
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        current_branch = head.replace("ref: refs/heads/", "") if head.startswith("ref:") else head[:7]

        # 브랜치 해시 수집: loose refs + packed-refs 합산
        branch_hashes: dict[str, str] = {}

        # 1) loose refs
        refs_dir = git_dir / "refs" / "heads"
        if refs_dir.exists():
            for p in refs_dir.rglob("*"):
                if p.is_file():
                    branch_name = str(p.relative_to(refs_dir)).replace("\\", "/")
                    branch_hashes[branch_name] = p.read_text(encoding="utf-8").strip()

        # 2) packed-refs (git pack-refs 이후 loose 파일이 없어지는 경우)
        packed_refs_path = git_dir / "packed-refs"
        if packed_refs_path.exists():
            for line in packed_refs_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("#") or not line:
                    continue
                parts = line.split()
                if len(parts) == 2 and parts[1].startswith("refs/heads/"):
                    bname = parts[1][len("refs/heads/"):]
                    if bname not in branch_hashes:  # loose ref 우선
                        branch_hashes[bname] = parts[0]

        main_hash = branch_hashes.get("main", "")
        unmerged = [b for b, h in branch_hashes.items() if b != "main" and h != main_hash]
        unmerged_str = "\n".join(unmerged) if unmerged else "(없음)"

        # 최근 커밋 메시지 (COMMIT_EDITMSG = 마지막 커밋)
        commit_msg_path = git_dir / "COMMIT_EDITMSG"
        last_msg = commit_msg_path.read_text(encoding="utf-8", errors="ignore").strip() if commit_msg_path.exists() else ""
        log = f"최근: {last_msg[:80]}" if last_msg else "(커밋 없음)"

        # 진행 중인 git 작업 확인
        dirty_indicators = ["MERGE_HEAD", "CHERRY_PICK_HEAD", "REBASE_HEAD"]
        in_progress = [f for f in dirty_indicators if (git_dir / f).exists()]
        status = f"브랜치: {current_branch}" + (f" ({', '.join(in_progress)} 진행 중)" if in_progress else "")

        return {"status": status, "log": log, "unmerged_branches": unmerged_str}
    except Exception as e:
        logger.warning(f"git 파싱 실패: {e}")
        return {"status": "(git 파싱 오류)", "log": "(git 파싱 오류)", "unmerged_branches": "(git 파싱 오류)"}


def _collect_data_freshness() -> str:
    """daily_summary.parquet 최신 수정 시각."""
    from modules.transform.utility.paths import MART_DB

    ds_path = MART_DB / "unified_sales_grp" / "daily_summary.parquet"
    if ds_path.exists():
        return datetime.fromtimestamp(ds_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    return "파일 없음"


def _collect_log_warnings() -> list[dict]:
    _, log_errors = _collect_previous_day_dag_results_v2()
    return log_errors


def _collect_scheduled_dags() -> list[str]:
    """오늘 활성화된 DAG 목록 (Airflow 메타DB)."""
    try:
        from airflow.models.dag import DagModel
        from airflow.utils.db import create_session

        with create_session() as session:
            rows = (
                session.query(DagModel.dag_id, DagModel.schedule_interval)
                .filter(DagModel.is_paused.is_(False), DagModel.is_active.is_(True))
                .order_by(DagModel.dag_id)
                .all()
            )
        return [
            f"{r.dag_id} ({r.schedule_interval})"
            for r in rows
            if r.dag_id != "Private_MorningBriefing_Dags"
        ]
    except Exception as e:
        logger.warning(f"DAG 목록 수집 실패: {e}")
        return []


# ============================================================
# Task 함수
# ============================================================

def collect_briefing_data(**context):
    """브리핑에 필요한 모든 데이터 수집 후 XCom 저장."""
    calendar, calendar_ok = _collect_calendar_events()
    failures, log_warnings = _collect_previous_day_dag_results_v2()
    git = _collect_git_status()
    freshness = _collect_data_freshness()
    scheduled = _collect_scheduled_dags()

    payload = {
        "calendar": calendar,
        "calendar_ok": calendar_ok,
        "failures": failures,
        "git": git,
        "freshness": freshness,
        "scheduled": scheduled[:10],
        "log_warnings": log_warnings,
    }
    context["ti"].xcom_push(key="briefing_data", value=json.dumps(payload, ensure_ascii=False))
    msg = f"수집 완료 — 일정 {len(calendar)}건 / 실패 {len(failures)}건 / 로그오류 DAG {len(log_warnings)}건"
    logger.info(msg)
    return msg


def _llm_call(prompt: str, system: str, num_predict: int = 300) -> str:
    """Ollama 직접 호출 — num_predict 조절 가능."""
    from modules.transform.utility.qwen_client import (
        _is_model_unhealthy,
        _prioritize_primary_models,
        _response_content,
        get_ollama_client_with_candidates,
    )

    client, candidates = get_ollama_client_with_candidates()
    messages = [{"role": "system", "content": system}, {"role": "user", "content": prompt}]
    for model in _prioritize_primary_models(candidates):
        if _is_model_unhealthy(model):
            logger.warning("LLM 모델 unhealthy 캐시로 스킵: %s", model)
            continue
        try:
            resp = client.chat(
                model=model,
                messages=messages,
                stream=False,
                think="gpt-oss" in model,
                options={"num_predict": num_predict, "temperature": 0},
            )
            return _response_content(resp, model).strip()
        except Exception as e:
            logger.warning(f"LLM 실패 ({model}): {e}")
    return "(LLM 응답 없음)"


def _analyze_fail_dag(dag_id: str, error_excerpt: str) -> str:
    """gpt-oss로 FAIL DAG 원인 한 줄 분석."""
    system = (
        "You are an Airflow expert. Analyze the error and reply ONLY in Korean, "
        "one line: '문제: X / 원인: Y / 조치: Z'."
    )
    prompt = f"DAG: {dag_id}\n에러:\n{error_excerpt[:1500]}"
    try:
        return _llm_call(prompt, system, num_predict=150)
    except Exception as e:
        logger.warning(f"LLM 분석 실패 ({dag_id}): {e}")
        return f"분석 실패: {str(e)[:80]}"


def generate_briefing(**context):
    """gpt-oss로 우선순위 브리핑 생성 후 XCom 저장."""
    ti = context["ti"]
    data = json.loads(ti.xcom_pull(task_ids="collect_briefing_data", key="briefing_data"))

    # Step A — 각 FAIL DAG 원인 분석
    fail_lines = []
    for f in data["failures"]:
        if f["error_excerpt"]:
            analysis = _analyze_fail_dag(f["dag_id"], f["error_excerpt"])
            fail_lines.append(f"• {f['dag_id']}: {analysis}")
        else:
            label = f["error_summary"] or f["fail_type"] or f["status"]
            fail_lines.append(f"• {f['dag_id']} [{f['status']}]: {label}")

    # Step B — 전체 우선순위 브리핑
    if not data.get("calendar_ok", True):
        cal_text = "  (수집 실패)"
    else:
        cal_text = "\n".join(f"  {e['time']} {e['summary']}" for e in data["calendar"]) or "  (없음)"
    fail_text = "\n".join(fail_lines) or "  (없음)"

    b_prompt = (
        f"오늘 일정:\n{cal_text}\n\n"
        f"실패/경고 DAG(오늘, KST 기준):\n{fail_text}"
    )
    b_system = (
        "너는 데이터 엔지니어의 아침 업무 비서야. "
        "아래 정보를 보고 오늘 가장 먼저 처리해야 할 작업을 번호 목록으로 정리해줘. "
        "무조건 한국어로만 답변해. 영어 금지. 최대 5줄. "
        "절대 사용자에게 되묻거나 추가 정보를 요구하지 마. "
        "주어진 정보만으로 판단하고, 처리할 실패나 일정이 없으면 "
        "번호 목록 대신 '특이사항 없음 — 정상 운영' 한 줄만 출력해."
    )
    priority_text = _llm_call(b_prompt, b_system, num_predict=400)

    # 메시지 조합
    today = pendulum.now("Asia/Seoul").strftime("%Y-%m-%d (%a)")
    fail_cnt = len(data["failures"])
    warn_cnt = sum(1 for f in data["failures"] if f.get("status") == "WARN")
    log_warnings = data.get("log_warnings", [])
    log_err_cnt = sum(
        1 for w in log_warnings for m in w["messages"] if m.get("level") == "ERROR"
    )
    log_warn_msg_cnt = sum(
        1 for w in log_warnings for m in w["messages"] if m.get("level") != "ERROR"
    )

    sections = []

    # 헤더
    sections.append(
        f"[AI 브리핑] {today}\n"
        f"FAIL {fail_cnt} / WARN {warn_cnt} / 로그오류 {log_err_cnt} / 로그경고 {log_warn_msg_cnt}"
    )

    # 우선순위
    sections.append(f"🎯 오늘 우선순위\n{priority_text}")

    # 일정
    if not data.get("calendar_ok", True):
        sections.append("📅 오늘 일정\n  (수집 실패 — collect_briefing_data 로그 확인)")
    elif data["calendar"]:
        sections.append(f"📅 오늘 일정\n{_format_calendar_sections(data['calendar'])}")
    else:
        sections.append("📅 오늘 일정\n  (없음)")

    # 실패/경고 DAG (오늘)
    if fail_lines:
        sections.append("❌ 실패/경고 DAG\n" + "\n".join(fail_lines))

    message = "\n\n".join(sections)
    ti.xcom_push(key="briefing_message", value=message)
    logger.info("브리핑 생성 완료")
    return "브리핑 생성 완료"


def send_briefing(**context):
    """Telegram으로 브리핑 전송 (notifier.send_telegram 재사용)."""
    from modules.transform.utility.notifier import send_telegram
    from modules.transform.utility.paths import MART_DB

    ti = context["ti"]
    message = ti.xcom_pull(task_ids="generate_briefing", key="briefing_message")

    if not message:
        logger.warning("브리핑 메시지 없음 — 전송 건너뜀")
        return "전송 건너뜀"

    # 출처 추가
    message_with_source = f"{message}\n\n출처: Private_MorningBriefing_Dags"

    send_telegram(message_with_source)
    logger.info("Telegram 전송 완료")

    today_str = pendulum.now("Asia/Seoul").strftime("%Y%m%d")
    log_dir = MART_DB / "briefing_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"briefing_{today_str}.md"
    log_file.write_text(message, encoding="utf-8")
    logger.info(f"브리핑 로그 저장: {log_file}")

    return "Telegram 전송 완료"
