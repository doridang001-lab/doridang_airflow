"""
정책 통합 최신화 파이프라인

8개 플랫폼 raw 정책 CSV에서 플랫폼별 최신 공지 1건씩 추출 →
통합 CSV 저장 → 신규/변경 공지 이메일 알림
"""

import json
import logging
import time
import pendulum
import pandas as pd
from datetime import datetime
from io import StringIO
from html import escape
from pathlib import Path
from airflow.exceptions import AirflowSkipException

from modules.transform.utility.paths import (
    BAEMIN_POLICY_CSV_PATH,
    COUPANG_POLICY_CSV_PATH,
    YOGIYO_POLICY_CSV_PATH,
    DDANGYO_POLICY_CSV_PATH,
    BAEDALTTEUK_POLICY_CSV_PATH,
    MUKKEBI_POLICY_CSV_PATH,
    BAEDALEUM_POLICY_CSV_PATH,
    NAVER_PLACE_POLICY_CSV_PATH,
    POLICY_LOG_PATH,
    POLICY_CONSOLIDATED_CSV,
)
from modules.transform.utility.mailer import send_email

# Flow 자동화
FLOW_POLICY_URL = "https://flow.team/l/QdNBw"
_WEEKDAY_KR = {0: "월", 1: "화", 2: "수", 3: "목", 4: "금", 5: "토", 6: "일"}

logger = logging.getLogger(__name__)

# 플랫폼 정렬 순서 (CSV 출력 및 이메일 표시 순서)
_PLATFORM_ORDER = [
    "baemin",
    "coupang",
    "ddangyo",
    "yogiyo",
    "baedaltteuk",
    "baedaleum",
    "mukkebi",
    "naver_place",
]

# 플랫폼별 raw CSV 경로 목록 (정렬 순서 동일)
_POLICY_CSV_PATHS = [
    BAEMIN_POLICY_CSV_PATH,
    COUPANG_POLICY_CSV_PATH,
    DDANGYO_POLICY_CSV_PATH,
    YOGIYO_POLICY_CSV_PATH,
    BAEDALTTEUK_POLICY_CSV_PATH,
    BAEDALEUM_POLICY_CSV_PATH,
    MUKKEBI_POLICY_CSV_PATH,
    NAVER_PLACE_POLICY_CSV_PATH,
]

# 플랫폼 한글 이름 매핑
_PLATFORM_KR = {
    "baemin": "배민",
    "coupang": "쿠팡이츠",
    "ddangyo": "땡겨요",
    "yogiyo": "요기요",
    "baedaltteuk": "배달특급",
    "baedaleum": "배달e음",
    "mukkebi": "먹깨비",
    "naver_place": "네이버플레이스",
}

# policy_type 뱃지 색상 매핑
_TYPE_COLOR = {
    "할인": "#27ae60",
    "수수료": "#e74c3c",
    "광고": "#2980b9",
    "노출": "#8e44ad",
    "정산": "#e67e22",
    "운영": "#7f8c8d",
    "기능변경": "#16a085",
    "기타": "#bdc3c7",
}

_OUTPUT_COLS = ["platform", "title", "content_summary", "policy_type", "recommended_action"]


# ============================================================
# load_and_filter_latest
# ============================================================

def load_and_filter_latest(**context) -> str:
    """8개 플랫폼 raw CSV 로드 → 플랫폼별 최신 policy_date 1건 필터링"""
    dfs = []
    for csv_path in _POLICY_CSV_PATHS:
        p = Path(csv_path)
        if not p.exists():
            logger.warning(f"정책 CSV 없음 (스킵): {p}")
            continue
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
            if df.empty:
                logger.warning(f"정책 CSV 비어있음 (스킵): {p}")
                continue
            dfs.append(df)
            logger.info(f"로드 완료: {p.name} ({len(df)}행)")
        except Exception as e:
            logger.warning(f"CSV 읽기 실패 {p}: {e}")

    if not dfs:
        raise AirflowSkipException("로드 가능한 정책 CSV가 없습니다.")

    df_all = pd.concat(dfs, ignore_index=True)

    # policy_date 내림차순 → platform 기준 중복 제거(최신 1건) → 플랫폼 순서 정렬
    df_all["policy_date"] = df_all["policy_date"].astype(str)
    df_all = df_all.sort_values("policy_date", ascending=False)
    df_latest = df_all.drop_duplicates(subset=["platform"], keep="first")

    # 지정 순서대로 정렬 (목록에 없는 플랫폼은 뒤로)
    order_map = {p: i for i, p in enumerate(_PLATFORM_ORDER)}
    df_latest = df_latest.assign(
        _order=df_latest["platform"].map(lambda p: order_map.get(p, 999))
    ).sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)

    logger.info(f"플랫폼별 최신 1건 필터링 완료: {len(df_latest)}개 플랫폼")

    result_json = df_latest.to_json(
        orient="records", force_ascii=False
    )
    context["ti"].xcom_push(key="latest_policies", value=result_json)
    return result_json


# ============================================================
# detect_new_policies
# ============================================================

def detect_new_policies(**context) -> list:
    """기존 통합 CSV(이전 실행 스냅샷)와 비교하여 신규/변경 정책 감지.

    통합 CSV는 매 실행마다 truncate+rewrite되므로,
    이 함수는 save_consolidated_csv보다 먼저 실행되어 이전 스냅샷을 읽는다.
    비교 키: platform + policy_date (제목 변경 또는 날짜 변경 = 신규)
    """
    latest_json = context["ti"].xcom_pull(
        task_ids="task_load_and_filter", key="latest_policies"
    )
    df_new = pd.read_json(StringIO(latest_json), orient="records")
    # policy_date를 문자열로 통일 (Timestamp → str 방지)
    df_new["policy_date"] = df_new["policy_date"].astype(str)

    consolidated_path = Path(POLICY_CONSOLIDATED_CSV)
    if not consolidated_path.exists():
        # 최초 실행 → 전체가 신규
        new_policies = _rows_to_json_safe(df_new)
        logger.info(f"최초 실행: 전체 {len(new_policies)}건 신규 처리")
        context["ti"].xcom_push(key="new_policies", value=new_policies)
        return new_policies

    # 이전 스냅샷 읽기 (아직 overwrite 전)
    df_prev = pd.read_csv(consolidated_path, encoding="utf-8-sig")

    # 비교 기준: platform별 이전 policy_date (없으면 title 폴백)
    if "policy_date" in df_prev.columns:
        prev_state = dict(zip(df_prev["platform"], df_prev["policy_date"].astype(str)))
        new_policies = _rows_to_json_safe(
            df_new[df_new["platform"].map(lambda p: prev_state.get(p)) != df_new["policy_date"]]
        )
    else:
        prev_state = dict(zip(df_prev["platform"], df_prev["title"].astype(str)))
        new_policies = _rows_to_json_safe(
            df_new[df_new["platform"].map(lambda p: prev_state.get(p)) != df_new["title"]]
        )

    logger.info(f"신규/변경 정책 감지: {len(new_policies)}건")
    context["ti"].xcom_push(key="new_policies", value=new_policies)
    return new_policies


def _rows_to_json_safe(df: pd.DataFrame) -> list:
    """DataFrame rows → XCom JSON 직렬화 가능한 순수 Python dict 리스트.
    pandas Timestamp, int64, float64 등을 JSON 호환 타입으로 변환한다."""
    return json.loads(df.to_json(orient="records", force_ascii=False, date_format="iso"))


# ============================================================
# save_consolidated_csv
# ============================================================

def save_consolidated_csv(**context) -> str:
    """통합 최신 정책 CSV 덮어쓰기 저장 (전체 컬럼)"""
    latest_json = context["ti"].xcom_pull(
        task_ids="task_load_and_filter", key="latest_policies"
    )
    df = pd.read_json(StringIO(latest_json), orient="records")

    out_path = Path(POLICY_CONSOLIDATED_CSV)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    logger.info(f"통합 CSV 저장 완료: {out_path} ({len(df)}행, {len(df.columns)}컬럼)")
    return str(out_path)


# ============================================================
# send_policy_alert
# ============================================================

def send_policy_alert(**context) -> str:
    """신규/변경 정책이 있을 때 HTML 이메일 알림 발송 (전체 표시, 신규에 🆕 마킹)"""
    new_policies = context["ti"].xcom_pull(
        task_ids="task_detect_new_policies", key="new_policies"
    )

    if not new_policies:
        logger.info("신규 정책 없음 → 이메일 스킵")
        raise AirflowSkipException("신규/변경 정책이 없어 이메일을 발송하지 않습니다.")

    latest_json = context["ti"].xcom_pull(
        task_ids="task_load_and_filter", key="latest_policies"
    )
    all_policies = json.loads(latest_json) if latest_json else new_policies
    new_platforms = {str(p.get("platform", "")) for p in new_policies}

    today = datetime.now().strftime("%Y-%m-%d")
    new_count = len(new_policies)
    subject = f"[플랫폼 공지] 플랫폼 정책 추가 알림 - {new_count}건 ({today})"

    html = _build_alert_html(all_policies, new_platforms, today)
    result = send_email(
        subject=subject,
        html_content=html,
        to_emails="a17019@kakao.com",
    )
    logger.info(f"정책 알림 이메일 발송 완료: {count}건")
    return result


def _build_alert_html(policies: list, new_platforms: set, today: str) -> str:
    """정책 알림 HTML 이메일 생성 (전체 표시, 신규 플랫폼에 🆕 마킹)"""
    rows_html = ""
    for p in policies:
        platform_key = str(p.get("platform", ""))
        platform_kr = _PLATFORM_KR.get(platform_key, platform_key)
        is_new = platform_key in new_platforms
        platform_label = f"🆕 {platform_kr}" if is_new else platform_kr
        policy_type = str(p.get("policy_type", "기타"))
        badge_color = _TYPE_COLOR.get(policy_type, "#bdc3c7")
        title = str(p.get("title", ""))
        summary = str(p.get("content_summary", ""))
        policy_date = str(p.get("policy_date", ""))
        source_url = str(p.get("source_url", "")).strip()
        title_html = (
            f'<a href="{source_url}" target="_blank" '
            f'style="color:#2980b9; text-decoration:none; font-weight:500;">{title}</a>'
            if source_url and source_url != "None" and source_url.startswith("http")
            else title
        )

        rows_html += f"""
        <tr>
          <td style="padding:10px 12px; border-bottom:1px solid #eee; font-weight:600; white-space:nowrap;">
            {platform_label}
          </td>
          <td style="padding:10px 12px; border-bottom:1px solid #eee; white-space:nowrap; color:#666; font-size:12px;">
            {policy_date}
          </td>
          <td style="padding:10px 12px; border-bottom:1px solid #eee;">
            <span style="background:{badge_color}; color:#fff; padding:2px 8px; border-radius:12px; font-size:11px; white-space:nowrap;">
              {policy_type}
            </span>
          </td>
          <td style="padding:10px 12px; border-bottom:1px solid #eee;">
            {title_html}
          </td>
          <td style="padding:10px 12px; border-bottom:1px solid #eee; color:#555; font-size:13px;">
            {summary}
          </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="ko">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0; padding:0; background:#f4f6f9; font-family:'Malgun Gothic',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6f9; padding:32px 0;">
    <tr><td align="center">
      <table width="700" cellpadding="0" cellspacing="0" style="background:#fff; border-radius:10px; overflow:hidden; box-shadow:0 2px 12px rgba(0,0,0,0.08);">

        <!-- 헤더 -->
        <tr>
          <td style="background:linear-gradient(135deg,#27ae60,#2ecc71); padding:28px 32px;">
            <div style="color:#fff; font-size:22px; font-weight:700; letter-spacing:-0.5px;">플랫폼 정책 변경 알림</div>
            <div style="color:rgba(255,255,255,0.85); font-size:13px; margin-top:6px;">{today} 기준 · 신규/변경 {len(policies)}건</div>
          </td>
        </tr>

        <!-- 본문 -->
        <tr>
          <td style="padding:24px 32px;">
            <p style="margin:0 0 16px; color:#333; font-size:14px;">
              플랫폼에서 <strong>새로운 정책 및 공지</strong>가 감지되었습니다.,<br>
              아래 리스트는 플랫폼별 최신 공지 1건씩 수집되었으며, 제목 클릭 시 원문 링크로 이동합니다.<br>
              정책 변경 사항을 확인하시고, 필요한 조치를 검토해주시기 바랍니다.
            </p>
            <div style="overflow-x:auto;">
              <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; font-size:13px;">
                <thead>
                  <tr style="background:#f8f9fa;">
                    <th style="padding:10px 12px; text-align:left; color:#555; font-weight:600; border-bottom:2px solid #dee2e6; white-space:nowrap;">플랫폼</th>
                    <th style="padding:10px 12px; text-align:left; color:#555; font-weight:600; border-bottom:2px solid #dee2e6; white-space:nowrap;">공지일</th>
                    <th style="padding:10px 12px; text-align:left; color:#555; font-weight:600; border-bottom:2px solid #dee2e6; white-space:nowrap;">유형</th>
                    <th style="padding:10px 12px; text-align:left; color:#555; font-weight:600; border-bottom:2px solid #dee2e6;">제목</th>
                    <th style="padding:10px 12px; text-align:left; color:#555; font-weight:600; border-bottom:2px solid #dee2e6;">요약</th>
                  </tr>
                </thead>
                <tbody>
                  {rows_html}
                </tbody>
              </table>
            </div>
          </td>
        </tr>

        <!-- Flow 링크 -->
        <tr>
          <td style="padding:20px 32px 8px; text-align:center;">
            <a href="https://flow.team/l/QdNBw" target="_blank"
               style="display:inline-block; background:#5b5ef6; color:#fff; font-size:14px; font-weight:600;
                      padding:12px 28px; border-radius:8px; text-decoration:none; letter-spacing:-0.3px;">
              Flow에서 확인하기 →
            </a>
          </td>
        </tr>

        <!-- 푸터 -->
        <tr>
          <td style="background:#f8f9fa; padding:16px 32px; border-top:1px solid #eee; margin-top:8px;">
            <p style="margin:0; color:#999; font-size:11px; line-height:1.6;">
              이 메일은 도리당 자동화 시스템에서 발송되었습니다.<br>
              문의: 조민준 PM a17019@doridang.com
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


# ============================================================
# write_consolidate_log
# ============================================================

def write_consolidate_log(**context) -> None:
    """실행 이력을 log.parquet에 기록 (trigger_rule='all_done')"""
    log_path = Path(POLICY_LOG_PATH)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    new_policies = context["ti"].xcom_pull(
        task_ids="task_detect_new_policies", key="new_policies"
    ) or []

    record = {
        "dag_id": context.get("dag").dag_id if context.get("dag") else "unknown",
        "run_id": context.get("run_id", ""),
        "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "new_policy_count": len(new_policies),
    }
    df_log = pd.DataFrame([record])

    if log_path.exists():
        try:
            df_existing = pd.read_parquet(log_path)
            df_all = pd.concat([df_existing, df_log], ignore_index=True)
        except Exception as e:
            logger.warning(f"기존 로그 읽기 실패, 새로 생성: {e}")
            df_all = df_log
    else:
        df_all = df_log

    df_all.to_parquet(log_path, index=False)
    logger.info(f"실행 로그 기록 완료: {log_path}")


# ============================================================
# post_to_flow
# ============================================================

def post_to_flow(**context) -> str:
    """Flow 프로젝트에 오늘 날짜 하위업무 생성 + 정책 테이블 본문 삽입"""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from modules.transform.pipelines.sales.SMD_sales_visit_log_01_crawling import (
        launch_browser, do_login,
    )

    latest_json = context["ti"].xcom_pull(
        task_ids="task_load_and_filter", key="latest_policies"
    )
    if not latest_json:
        raise AirflowSkipException("latest_policies XCom 없음 → Flow 게시 스킵")

    df = pd.read_json(StringIO(latest_json), orient="records")

    new_policies = context["ti"].xcom_pull(
        task_ids="task_detect_new_policies", key="new_policies"
    ) or []
    new_platforms = {str(p.get("platform", "")) for p in new_policies}

    now_kst = pendulum.now("Asia/Seoul")
    today_title = f"{now_kst.strftime('%y.%m.%d')}({_WEEKDAY_KR[now_kst.weekday()]})"
    today_str = now_kst.to_date_string()
    body_html = _build_flow_policy_html(df, today_str, new_platforms)

    driver = launch_browser()
    driver.set_window_size(1920, 1080)
    try:
        if not do_login(driver):
            raise RuntimeError("Flow 로그인 실패")

        wait = WebDriverWait(driver, 20)

        # ① 프로젝트 직접 URL 이동
        driver.get(FLOW_POLICY_URL)
        time.sleep(5)

        # ② 콘텐츠 로딩 대기
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, ".subtask-list, .js-add-subtask-button")
        ))
        time.sleep(2)

        # ③ 하단 스크롤 → 하위업무 추가 버튼 클릭
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        add_btn = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "button.js-add-subtask-button")
        ))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", add_btn)
        driver.execute_script("arguments[0].click();", add_btn)
        time.sleep(2)

        # ④ 하위업무 제목 입력 (JS 이벤트 디스패치 — send_keys 금지)
        subtask_input = wait.until(lambda d: next(
            (el for el in d.find_elements(By.CSS_SELECTOR, "input.js-subtask-input")
             if not el.get_attribute("readonly")),
            None
        ))
        driver.execute_script("""
            var el = arguments[0], title = arguments[1];
            el.scrollIntoView({block:'center'});
            el.focus();
            el.value = title;
            el.dispatchEvent(new Event('input', {bubbles:true}));
            el.dispatchEvent(new Event('change', {bubbles:true}));
            el.dispatchEvent(new KeyboardEvent('keydown', {key:'Enter',code:'Enter',keyCode:13,bubbles:true}));
            el.dispatchEvent(new KeyboardEvent('keypress',{key:'Enter',code:'Enter',keyCode:13,bubbles:true}));
            el.dispatchEvent(new KeyboardEvent('keyup',  {key:'Enter',code:'Enter',keyCode:13,bubbles:true}));
        """, subtask_input, today_title)
        time.sleep(3)

        # ⑤ 생성된 하위업무 클릭
        target = next(
            (el for el in driver.find_elements(By.CSS_SELECTOR, "p.subtask__tit--display")
             if today_title in el.text),
            None
        )
        if not target:
            raise RuntimeError(f"하위업무 '{today_title}' 찾기 실패")
        driver.execute_script("arguments[0].click();", target)
        time.sleep(2)

        # ⑥ 설정(점3개) → 수정
        setting_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "button.js-setting-button.set-btn")
        ))
        driver.execute_script("arguments[0].click();", setting_btn)
        time.sleep(1)
        modify_item = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "li.js-setting-item[data-code='modify']")
        ))
        driver.execute_script("arguments[0].click();", modify_item)
        time.sleep(2)

        # ⑦ CKEditor 본문 삽입
        ckeditor_ready = False
        for _ in range(15):
            try:
                if driver.execute_script("""
                    if (typeof CKEDITOR==='undefined' || !CKEDITOR.instances) return false;
                    var keys = Object.keys(CKEDITOR.instances);
                    return keys.length > 0
                        && CKEDITOR.instances[keys[keys.length-1]].status==='ready';
                """):
                    ckeditor_ready = True
                    break
            except Exception:
                pass
            time.sleep(1)

        if ckeditor_ready:
            driver.execute_script("""
                var keys = Object.keys(CKEDITOR.instances);
                CKEDITOR.instances[keys[keys.length-1]].setData(arguments[0]);
            """, body_html)
        else:
            cke_iframe = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "iframe.cke_wysiwyg_frame")
            ))
            driver.switch_to.frame(cke_iframe)
            driver.execute_script("document.body.innerHTML = arguments[0];", body_html)
            driver.switch_to.default_content()
        time.sleep(1)

        # ⑧ 등록
        submit_btn = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "button.js-complete-btn.confirm")
        ))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submit_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", submit_btn)
        time.sleep(3)

        logger.info(f"Flow 하위업무 생성 완료: {today_title}")
        return f"Flow 게시 완료: {today_title}"

    finally:
        driver.quit()


def _build_flow_policy_html(df: pd.DataFrame, today_str: str, new_platforms: set) -> str:
    """Flow CKEditor용 정책 테이블 HTML 빌드 (신규 플랫폼에 🆕 마킹)"""
    _th = "padding:8px;text-align:left;border:1px solid #dee2e6;background:#f8f9fa;font-size:12px;"
    _td = "padding:8px;font-size:12px;border:1px solid #dee2e6;word-break:break-word;"
    _td_nowrap = "padding:8px;font-size:12px;border:1px solid #dee2e6;white-space:nowrap;"

    rows = []
    for _, row in df.iterrows():
        platform_key = str(row.get("platform", ""))
        platform_kr = _PLATFORM_KR.get(platform_key, platform_key)
        is_new = platform_key in new_platforms
        platform_label = f"🆕 {platform_kr}" if is_new else platform_kr
        policy_date = escape(str(row.get("policy_date", "")))
        policy_type = escape(str(row.get("policy_type", "")))
        title = escape(str(row.get("title", "")))
        summary = escape(str(row.get("content_summary", "")))
        source_url = str(row.get("source_url", "")).strip()

        title_cell = (
            f'<a href="{escape(source_url)}" target="_blank" '
            f'style="color:#2980b9;text-decoration:none;">{title}</a>'
            if source_url and source_url != "None" and source_url.startswith("http")
            else title
        )

        rows.append(f"""<tr>
  <td style="{_td_nowrap}">{platform_label}</td>
  <td style="{_td_nowrap}">{policy_date}</td>
  <td style="{_td_nowrap}">{policy_type}</td>
  <td style="{_td}">{title_cell}</td>
  <td style="{_td}">{summary}</td>
</tr>""")

    rows_html = "\n".join(rows)
    count = len(df)

    return f"""<div class="post-editor-wrap">
<h1>KPI</h1>
<ul><li>정책/이벤트 적용 전후 결과(매출, 전환율 등) 변화 검증</li></ul>
<div>&nbsp;</div>
<h2>플랫폼 정책 변경 알림</h2>
<p>{escape(today_str)} 기준 &middot; {count}건</p>
<table style="border-collapse:collapse;width:100%;border:1px solid #dee2e6;">
  <thead><tr>
    <th style="{_th}">플랫폼</th>
    <th style="{_th}">공지일</th>
    <th style="{_th}">유형</th>
    <th style="{_th}">제목</th>
    <th style="{_th}">요약</th>
  </tr></thead>
  <tbody>
{rows_html}
  </tbody>
</table>
</div>"""
