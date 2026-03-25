"""
SMP_Fdam_CS_ETL_Flow_Macro_Dags.py

FLOW_ALERT_DAYS일 이상 미처리 매장 CS → Flow 프로젝트 자동 게시

📋 처리 흐름:
1. SMP_Fdam_CS_ETL_Dags send_overdue_cs_alert 완료 대기 (ExternalTaskSensor)
2. 구글시트에서 오늘(또는 최신) 수집일 데이터 로드 (메일 DAG과 동일)
3. 미완료 & FLOW_ALERT_DAYS일 이상 경과 건 필터링
4. Flow 로그인 → [물류] 프담CS 알림 BOT 프로젝트 → 글 자동 게시

⚙️ 일수 변경: FLOW_ALERT_DAYS 값만 수정 (기본 14 → 29 등)
"""

import os
import re
import time
import datetime as dt
import pendulum
import pandas as pd
from datetime import timedelta
from html import escape as html_escape
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.models.baseoperator import chain
from airflow.utils.state import DagRunState
from airflow.utils.trigger_rule import TriggerRule

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.transform.utility.io import SMP_FDAM_CS_TIME

filename = os.path.basename(__file__)

# ============================================================
# 설정
# ============================================================
FLOW_PROJECT_SRNO         = '2830044'
FLOW_PROJECT_NAME         = '[물류] 프담CS 알림 BOT'

# ▼▼▼ Flow 게시 기준 일수 (이 값만 바꾸면 전체 적용) ▼▼▼
# 예) 14 → 14일 이상 미처리 건만 게시  /  29 → 29일 이상
FLOW_ALERT_DAYS           = 14
# ▲▲▲ Flow 게시 기준 일수 ▲▲▲

CREDENTIALS_PATH          = "/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"
FALLBACK_CREDENTIALS_PATH = "/opt/airflow/config/glowing-palace-465904-h6-7f82df929812.json"
CS_GSHEET_URL             = "https://docs.google.com/spreadsheets/d/1DHbZX5jDkiZfe0SPr3eRinPXuSFomYm2AN-gMADoBkI/edit?gid=1488056813#gid=1488056813"
CS_SHEET_NAME             = "시트1"


# ============================================================
# 유틸리티
# ============================================================
def _credential_candidates() -> list:
    """사용 가능한 구글 서비스 계정 JSON 경로 목록 반환"""
    candidates = []
    env_cred = os.getenv("SMP_FDAM_CS_GSHEET_CREDENTIALS")
    if env_cred:
        candidates.append(env_cred)
    candidates.extend([CREDENTIALS_PATH, FALLBACK_CREDENTIALS_PATH])
    deduped = []
    for path in candidates:
        if path and path not in deduped and os.path.exists(path):
            deduped.append(path)
    return deduped


def _latest_successful_execution_date(dt, **context):
    """SMP_Fdam_CS_ETL_Dags 의 최신 성공 실행일 반환 (없으면 현재 dt 사용)"""
    runs = DagRun.find(dag_id='Strategy_FdamCS_01_Process_Dags', state=DagRunState.SUCCESS)
    if not runs:
        return dt
    execution_dates = [r.execution_date for r in runs if r.execution_date is not None]
    if not execution_dates:
        return dt
    return max(execution_dates)


# ============================================================
# 게시 내용 빌드
# ============================================================
def _build_flow_post_content(df_alert: pd.DataFrame, today_str: str):
    """
    Flow 게시용 제목 / 본문 HTML 생성 (메일과 동일한 표 형태)
    Returns (title: str, body_html: str)

    CKEditor setData() 로 HTML 직접 주입 → <table> 표 렌더링
    컬럼: 접수번호 | 등록일 | 예정일 | 진행률 | 소요기간 | 매장명 | CS구분 | 이슈요약 | 진행상태 | 비공개메모
    """
    n     = len(df_alert)
    title = f"[CS {FLOW_ALERT_DAYS}일 초과] 미처리 {n}건 | {today_str}"

    row_html = ''
    for _, row in df_alert.iterrows():
        # 소요기간 색상
        try:
            days_int   = int(row.get('처리소요기간', 0) or 0)
            days_color = '#e74c3c' if days_int >= 14 else '#f39c12'
            days_text  = f'<span style="color:{days_color};font-weight:bold;">{days_int}일</span>'
        except (ValueError, TypeError):
            days_text  = str(row.get('처리소요기간', ''))

        issue_summary = str(row.get('issue_summary', '') or '')[:50]

        memo_raw_html = str(row.get('비공개메모', '') or '').replace('\r\n', '\n').replace('\r', '\n')
        memo_escaped  = html_escape(memo_raw_html).replace('\n', '<br>')
        private_memo  = memo_escaped if memo_escaped.strip() else '<span style="color:#aaa;">(메모 없음)</span>'

        # 완료예정일 / 진행률
        memo_raw  = str(row.get('비공개메모', '') or '')
        due_match = re.search(r'완료예정일[:\s]*([0-9]{4}-[0-9]{2}-[0-9]{2})', memo_raw)
        if due_match:
            due_date_str  = due_match.group(1)
            due_date_cell = html_escape(due_date_str)
            try:
                reg_date  = dt.date.fromisoformat(str(row.get('등록일', '')))
                due_date  = dt.date.fromisoformat(due_date_str)
                today_date = dt.date.fromisoformat(today_str)
                total     = (due_date - reg_date).days
                elapsed   = (today_date - reg_date).days
                if total > 0:
                    pct = round(elapsed / total * 100)
                    if pct >= 100:
                        progress_cell = f'<span style="color:#e74c3c;font-weight:bold;">{pct}% 초과</span>'
                    elif pct >= 80:
                        progress_cell = f'<span style="color:#e67e22;font-weight:bold;">{pct}%</span>'
                    elif pct >= 50:
                        progress_cell = f'<span style="color:#f39c12;">{pct}%</span>'
                    else:
                        progress_cell = f'<span style="color:#27ae60;">{pct}%</span>'
                else:
                    progress_cell = '<span style="color:#aaa;">-</span>'
            except Exception:
                progress_cell = '<span style="color:#aaa;">-</span>'
        else:
            due_date_cell = '<span style="color:#aaa;">미작성</span>'
            progress_cell = '<span style="color:#aaa;">-</span>'

        _td = 'padding:8px;font-size:12px;border:1px solid #dee2e6;'
        row_html += f"""
            <tr>
                <td style="{_td}white-space:nowrap;">{html_escape(str(row.get('접수번호', '')))}</td>
                <td style="{_td}white-space:nowrap;">{html_escape(str(row.get('등록일', '')))}</td>
                <td style="{_td}white-space:nowrap;">{due_date_cell}</td>
                <td style="{_td}text-align:center;white-space:nowrap;">{progress_cell}</td>
                <td style="{_td}text-align:center;white-space:nowrap;">{days_text}</td>
                <td style="{_td}">{html_escape(str(row.get('매장명', '')))}</td>
                <td style="{_td}white-space:nowrap;">{html_escape(str(row.get('CS구분', '')))}</td>
                <td style="{_td}white-space:normal;word-break:break-word;">{html_escape(issue_summary)}</td>
                <td style="{_td}color:#7f8c8d;white-space:pre-wrap;word-break:break-word;line-height:1.45;min-width:200px;">{private_memo}</td>
            </tr>"""

    _th = 'padding:8px;text-align:left;border:1px solid #dee2e6;background:#f8f9fa;white-space:nowrap;'
    body_html = f"""<p style="margin:0 0 12px;font-size:13px;color:#c0392b;">
    📋 기준일: <strong>{today_str}</strong> &nbsp;|&nbsp; 총 <strong>{n}건</strong> ({FLOW_ALERT_DAYS}일 이상 미처리)
</p>
<table style="border-collapse:collapse;font-size:12px;width:100%;border:1px solid #dee2e6;">
    <thead>
        <tr>
            <th style="{_th}">접수번호</th>
            <th style="{_th}">등록일</th>
            <th style="{_th}">예정일</th>
            <th style="{_th}text-align:center;">진행률</th>
            <th style="{_th}text-align:center;">소요기간</th>
            <th style="{_th}">매장명</th>
            <th style="{_th}">CS구분</th>
            <th style="{_th}">이슈요약</th>
            <th style="{_th}">비공개메모</th>
        </tr>
    </thead>
    <tbody>
        {row_html}
    </tbody>
</table>"""

    return title, body_html


# ============================================================
# Task: Flow 자동 게시
# ============================================================
def post_overdue_cs_to_flow(**context):
    """
    FLOW_ALERT_DAYS일 이상 미처리 CS를 Flow 프로젝트에 자동 게시
      1) 구글시트 → 오늘(또는 최신) 수집일 데이터 로드
      2) FLOW_ALERT_DAYS일 이상 미처리 건 필터
      3) Selenium: Flow 로그인 → 프로젝트 이동 → 글 작성 → 등록
    """
    import gspread
    from google.oauth2.service_account import Credentials
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from modules.transform.pipelines.sales.SMD_sales_visit_log_01_crawling import (
        launch_browser, do_login
    )

    now_kst   = pendulum.now("Asia/Seoul")
    today_str = now_kst.to_date_string()
    print(f"\n{'='*60}")
    print(f"[Flow게시] 시작 | 기준일: {today_str}")

    # ── 1. 구글시트 로드 ───────────────────────────────────────
    print("[Flow게시] 구글시트 데이터 로드 중...")
    try:
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        candidates = _credential_candidates()
        if not candidates:
            raise RuntimeError("사용 가능한 서비스 계정 없음")
        creds    = Credentials.from_service_account_file(candidates[0], scopes=scopes)
        gc       = gspread.authorize(creds)
        sh       = gc.open_by_url(CS_GSHEET_URL)
        ws       = sh.worksheet(CS_SHEET_NAME)
        df_sheet = pd.DataFrame(ws.get_all_records())
        print(f"[Flow게시] 구글시트 로드 완료: {len(df_sheet)}건")
    except Exception as e:
        print(f"[Flow게시] 구글시트 로드 실패: {e}")
        return f"스킵 (구글시트 로드 실패: {e})"

    if df_sheet.empty:
        return "스킵 (데이터 없음)"

    # 필수 컬럼 확인
    if '수집일' not in df_sheet.columns:
        print("[Flow게시] '수집일' 컬럼 없음 → 스킵")
        return "스킵 ('수집일' 컬럼 없음)"
    if '등록일' not in df_sheet.columns:
        print("[Flow게시] '등록일' 컬럼 없음 → 스킵")
        return "스킵 ('등록일' 컬럼 없음)"

    # ── 2. 수집일 기준 데이터 선택 (메일 DAG과 동일 로직) ──────
    # 오늘 수집 데이터 우선, 없으면 최신 수집일 사용
    all_dates_col = df_sheet['수집일'].astype(str).str[:10]
    df_today_chk  = df_sheet[all_dates_col == today_str]

    if df_today_chk.empty:
        # 최신 수집일 결정: 수집일 + uploaded_at 컬럼 모두 고려
        date_candidates = [all_dates_col]
        if 'uploaded_at' in df_sheet.columns:
            date_candidates.append(df_sheet['uploaded_at'].astype(str).str[:10])
        valid_dates = (
            pd.concat(date_candidates)
            .pipe(lambda s: s[s.str.match(r'^\d{4}-\d{2}-\d{2}$')])
        )
        latest_date = valid_dates.max() if not valid_dates.empty else today_str
        print(f"[Flow게시] 오늘({today_str}) 수집 데이터 없음 → 최신 수집일({latest_date}) 기준으로 대체")
        df_work = df_sheet[all_dates_col == latest_date].copy()
    else:
        print(f"[Flow게시] 수집일={today_str} 데이터: {len(df_today_chk)}건")
        df_work = df_today_chk

    if df_work.empty:
        return "스킵 (시트에 데이터 없음)"

    print(f"[Flow게시] 수집일 기준 데이터: {len(df_work)}건")

    # ── 3. 미완료 & N일 이상 필터 (경과일 = 오늘 - 등록일) ──────
    cond_not_done = df_work['진행상태'].astype(str).str.strip() != '완료'
    df_open       = df_work[cond_not_done].copy()

    # 경과일 = 오늘(실행일) - 등록일 (스칼라 직접 사용, 임시 컬럼 불필요)
    df_open['처리소요기간'] = (
        pd.Timestamp(today_str) - pd.to_datetime(df_open['등록일'], errors='coerce')
    ).dt.days

    df_alert = df_open[df_open['처리소요기간'] >= FLOW_ALERT_DAYS].sort_values(
        '처리소요기간', ascending=False
    ).copy()

    print(f"[Flow게시] {FLOW_ALERT_DAYS}일 이상 미처리: {len(df_alert)}건")
    if df_alert.empty:
        print(f"[Flow게시] 해당 건 없음 → Flow 게시 생략")
        return f"게시 생략 ({FLOW_ALERT_DAYS}일 이상 건 없음)"

    # ── 4. 제목 / 본문 구성 ───────────────────────────────────
    title, body_html = _build_flow_post_content(df_alert, today_str)
    print(f"[Flow게시] 제목: {title}")
    print(f"[Flow게시] 본문 미리보기 ({len(body_html)}자):")
    print(body_html[:600])

    # ── 5. Selenium Flow 게시 ────────────────────────────────
    driver = None
    try:
        driver = launch_browser()
        # 헤드리스 모드에서 maximize_window()는 실제 뷰포트를 보장하지 않음
        # → 1920×1080 명시 설정으로 사이드바 프로젝트 목록 전체가 화면 안에 위치하도록 함
        driver.set_window_size(1920, 1080)
        if not do_login(driver):
            raise RuntimeError("Flow 로그인 실패")

        wait = WebDriverWait(driver, 20)

        # ① 내 프로젝트 클릭
        my_project = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "li[data-code='project'].my-project")
        ))
        driver.execute_script("arguments[0].click();", my_project)
        time.sleep(2)
        print("[Flow게시] 내 프로젝트 클릭")

        # ② 물류&구매&연구개발 프로젝트 클릭
        # element_to_be_clickable 은 화면 밖 요소(스크롤 필요)에서 타임아웃 발생
        # → presence_of_element_located 로 DOM 존재 확인 후 scrollIntoView + JS click 사용
        project_item = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, f"li#project-{FLOW_PROJECT_SRNO}")
        ))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", project_item)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", project_item)
        time.sleep(2)
        print(f"[Flow게시] '{FLOW_PROJECT_NAME}' 프로젝트 클릭")

        # ③ 글 탭 클릭 (새 글 작성 폼 진입)
        post_filter = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "li.post-filter[data-post-code='91']")
        ))
        driver.execute_script("arguments[0].click();", post_filter)
        time.sleep(2.5)  # 1.5→2.5초: 글쓰기 폼(에디터) 렌더링 대기
        print("[Flow게시] 글 탭 클릭")

        # ④ 제목 입력
        title_input = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "input#postTitle")
        ))
        title_input.clear()
        title_input.send_keys(title)
        time.sleep(0.5)
        print("[Flow게시] 제목 입력 완료")

        # ⑤ 본문 입력 (CKEditor)
        # Flow 글쓰기 에디터는 CKEditor (iframe 기반)
        #   - div#postContents : display:none 숨김 원본 (직접 조작 불가)
        #   - iframe.cke_wysiwyg_frame : 실제 편집 iframe
        # body_html 은 이미 <p>/<hr> 태그로 구성된 HTML → <br> 변환 불필요
        # 방법 A: CKEDITOR API (status='ready' 폴링, 최대 15초)
        # 방법 B: fallback → iframe body 직접 HTML 주입

        ckeditor_ready = False
        for _i in range(15):
            try:
                if driver.execute_script(
                    "return !!(typeof CKEDITOR !== 'undefined'"
                    " && CKEDITOR.instances"
                    " && CKEDITOR.instances['postContents']"
                    " && CKEDITOR.instances['postContents'].status === 'ready');"
                ):
                    ckeditor_ready = True
                    break
            except Exception as _e:
                if _i == 14:
                    print(f"[Flow게시] CKEditor 폴링 최종 실패: {_e}")
            time.sleep(1)

        if ckeditor_ready:
            driver.execute_script(
                "CKEDITOR.instances['postContents'].setData(arguments[0]);",
                body_html,
            )
            print("[Flow게시] 본문 입력 완료 (CKEditor API)")
        else:
            # CKEDITOR 미초기화 → iframe body 직접 조작
            print("[Flow게시] CKEditor 미초기화 → iframe 직접 조작")
            cke_iframe = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "iframe.cke_wysiwyg_frame")
            ))
            driver.switch_to.frame(cke_iframe)
            driver.execute_script(
                "document.body.innerHTML = arguments[0];", body_html
            )
            driver.switch_to.default_content()
            print("[Flow게시] 본문 입력 완료 (iframe 직접)")

        time.sleep(0.5)

        # ⑥ 등록 버튼 클릭
        # <button class="js-complete-btn create-button create-post-submit">등록</button>
        # element_to_be_clickable 타임아웃 방지 → presence_of_element_located + JS click
        submit_btn = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "button.js-complete-btn.create-button.create-post-submit")
        ))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submit_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", submit_btn)
        time.sleep(3)
        print(f"[Flow게시] ✅ 등록 완료: {title}")

        return f"✅ Flow 게시 완료 ({len(df_alert)}건)"

    except Exception as e:
        import traceback
        print(f"[Flow게시] ❌ 실패: {e}")
        traceback.print_exc()
        raise

    finally:
        if driver:
            driver.quit()
            print("[Flow게시] 브라우저 종료")


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    description=f'{FLOW_ALERT_DAYS}일 이상 미처리 매장CS → Flow 자동 게시',
    schedule=SMP_FDAM_CS_TIME,  # io3.py 참조 (SMP_Fdam_CS_ETL_Dags 와 동일)
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['flow', 'macro', 'relay_cs', '매장cs'],
) as dag:

    # SMP_Fdam_CS_ETL_Dags 의 send_overdue_cs_alert 완료까지 대기
    # soft_fail=True : upstream이 없거나 타임아웃 시 skipped 처리 후 계속 진행 (SMD_02 패턴)
    wait_for_cs_alert = ExternalTaskSensor(
        task_id='wait_for_cs_alert',
        external_dag_id='Strategy_FdamCS_01_Process_Dags',
        external_task_id='send_overdue_cs_alert',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',
        timeout=3600,      # 최대 60분 대기
        poke_interval=60,  # 1분마다 확인
        check_existence=False,
        soft_fail=True,    # 타임아웃/실패 시 skipped 처리 → downstream 실행 가능
        execution_date_fn=_latest_successful_execution_date,
    )

    t_post_flow = PythonOperator(
        task_id='post_overdue_cs_to_flow',
        python_callable=post_overdue_cs_to_flow,
        execution_timeout=timedelta(minutes=15),
        trigger_rule=TriggerRule.NONE_FAILED,  # 센서가 skipped 여도 실행 (실제 fail만 막음)
    )

    chain(wait_for_cs_alert, t_post_flow)
