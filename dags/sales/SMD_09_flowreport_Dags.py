"""
SMD_09 - 영업관리 주간 Flow 리포트 자동 게시 DAG

처리 흐름:
1. SMD_08 완료 대기 (ExternalTaskSensor)
2. 버퍼 GSheet에서 최신 수집날짜 데이터 로드
3. 조치/미조치 매장 분류 → Flow 게시용 HTML 생성
4. Selenium: Flow 로그인 → 프로젝트 → 하위업무 추가 → 리포트 등록

입력: 버퍼 GSheet (SMD_08에서 수집된 조치/미조치 데이터)
출력: Flow '[영업관리부] 영업관리 DX/AX 전환' 프로젝트 게시물
"""

import os
import time
import pendulum
import pandas as pd
from pathlib import Path
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from modules.transform.utility.io import SMD_ORDERS_TIME

filename = os.path.basename(__file__)

# ============================================================
# 설정
# ============================================================
FLOW_PROJECT_SRNO = '2833856'
FLOW_PROJECT_NAME = '[영업관리부] 영업관리 DX/AX 전환'

CREDENTIALS_PATH = "/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"

# 버퍼 시트 (SMD_08에서 조치/미조치 데이터가 수집되는 시트)
BUFFER_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1xLhw-qS7zBBsAtwzyUO9N1AYT-zgECcfyTX9GBsi1k8/edit?gid=0#gid=0"
BUFFER_SHEET_NAME = "시트1"

# 리포트 시트 (담당자 입력용 - 입력링크)
REPORT_GSHEET_URL = "https://docs.google.com/spreadsheets/d/1OFTQ0WyKgcwmBxwESWWpCD6E6O2i9kYYN6-YxNlx9xQ/edit?gid=0#gid=0"

# 대시보드 링크
DASHBOARD_URL = "https://lookerstudio.google.com/u/0/reporting/86e94b9f-b760-455b-b0a3-03da62790f50/page/u7omF"


# ============================================================
# 요일 한글 매핑
# ============================================================
WEEKDAY_KR = {0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'}


def _get_today_title(now_kst):
    """오늘 날짜 제목 생성 (예: 26.03.17(화))"""
    wd = WEEKDAY_KR[now_kst.weekday()]
    return f"{now_kst.strftime('%y.%m.%d')}({wd})"


# ============================================================
# HTML 리포트 빌드
# ============================================================
def _build_flow_report_html(df_current: pd.DataFrame, df_prev_no_action: pd.DataFrame, now_kst) -> str:
    """
    버퍼 GSheet 데이터로 Flow 게시용 HTML 리포트 생성.

    Args:
        df_current: 금주 수집 데이터 (조치 + 미조치)
        df_prev_no_action: 이전주 미조치 데이터 (버퍼에 남아있던 것)
        now_kst: 현재 시각

    컬럼: 날짜, 매장명, 담당자, 매출, 수수료, 배민광고, 쿠팡광고, 성실지표,
          조치일자, 목표치, 핵심조치, 조치내용, 문제점, 수집날짜, 구분
    """
    # 구분 컬럼 기준 분류
    df_action = df_current[df_current['구분'].astype(str).str.strip() == '조치'].copy()
    df_no_action = df_current[df_current['구분'].astype(str).str.strip() != '조치'].copy()

    # ── 확인필요: 이전주 미조치 매장 중 금주에도 알림이 뜬 매장 ──
    df_need_check = pd.DataFrame()
    if not df_prev_no_action.empty and not df_current.empty:
        prev_stores = set(df_prev_no_action['매장명'].astype(str).str.strip())
        current_stores = set(df_current['매장명'].astype(str).str.strip())
        repeat_stores = prev_stores & current_stores
        if repeat_stores:
            # 금주 데이터에서 해당 매장 추출 (이전주 미조치 사유도 병합)
            df_need_check = df_current[
                df_current['매장명'].astype(str).str.strip().isin(repeat_stores)
            ].copy()
            # 이전주 조치내용을 참고용으로 추가
            prev_reason = df_prev_no_action.set_index(
                df_prev_no_action['매장명'].astype(str).str.strip()
            )[['조치내용', '수집날짜']].to_dict('index')
            df_need_check['_prev_reason'] = df_need_check['매장명'].astype(str).str.strip().map(
                lambda s: str(prev_reason.get(s, {}).get('조치내용', '')).strip()
            )
            df_need_check['_prev_date'] = df_need_check['매장명'].astype(str).str.strip().map(
                lambda s: str(prev_reason.get(s, {}).get('수집날짜', '')).strip()
            )
            print(f"[FlowReport] 확인필요 매장: {len(repeat_stores)}곳 {repeat_stores}")

    total_count = len(df_current)
    action_count = len(df_action)
    no_action_count = len(df_no_action)
    need_check_count = len(set(df_need_check['매장명'].astype(str).str.strip())) if not df_need_check.empty else 0

    parts = []

    # ── 알림개요 ──
    parts.append('<div class="post-editor-wrap">')
    parts.append('<h1>알림개요</h1>')
    parts.append('<ul>')
    parts.append('<li>영업관리 알림 시스템 기준에 따라 매출/광고/수수료 지표 기반 조치 필요 매장 안내</li>')
    parts.append('<li>담당자 확인 후 조치 및 사유 공유 목적</li>')
    parts.append('<li>점주 소통 강화 목적</li>')
    parts.append('<li>AX 전환 목표<ul>')
    parts.append('<li>영업관리 조치 및 점주 의견을 데이터 기반으로 검증하여 매장 관리 의사결정을 고도화</li>')
    parts.append('</ul></li>')
    parts.append('</ul>')
    parts.append('<div>&nbsp;</div>')

    # ── 확인필요 (이전주 미조치 + 금주 재알림) ──
    if not df_need_check.empty:
        parts.append('<h1>확인필요 (이전주 미조치 재알림)</h1>')
        parts.append('<ul>')
        for manager, grp in df_need_check.groupby('담당자', sort=True):
            manager_str = str(manager).strip()
            if not manager_str or manager_str == 'nan':
                manager_str = '미지정'
            parts.append(f'<li><strong>{manager_str}</strong><ul>')
            for _, row in grp.iterrows():
                store = str(row.get('매장명', '')).strip()
                prev_reason = str(row.get('_prev_reason', '')).strip()
                prev_date = str(row.get('_prev_date', '')).strip()

                # 금주 알림 지표
                indicators = []
                for col in ['매출', '수수료', '배민광고', '쿠팡광고', '성실지표']:
                    val = str(row.get(col, '')).strip().upper()
                    if val in ('O', 'TRUE', 'Y'):
                        indicators.append(col)
                ind_str = ', '.join(indicators) if indicators else ''

                parts.append(f'<li>{store}')
                if ind_str:
                    parts.append(f' ({ind_str})')
                parts.append('<ul>')
                if prev_reason and prev_reason != 'nan':
                    parts.append(f'<li>이전주({prev_date}) 미조치 사유: {prev_reason}</li>')
                else:
                    parts.append(f'<li>이전주({prev_date}) 미조치 (사유 미기재)</li>')
                parts.append('</ul></li>')
            parts.append('</ul></li>')
        parts.append('</ul>')
        parts.append('<div>&nbsp;</div>')

    # ── 금주 요약 ──
    parts.append('<h1>금주 요약</h1>')
    parts.append('<ul>')
    parts.append(f'<li>총 {total_count}곳 알림발생</li>')
    if need_check_count > 0:
        parts.append(f'<li>확인필요 매장<ul><li>{need_check_count}곳 (이전주 미조치 재알림)</li></ul></li>')
    parts.append(f'<li>조치 매장<ul><li>{action_count}곳</li></ul></li>')
    parts.append(f'<li>미조치 매장<ul><li>{no_action_count}곳</li></ul></li>')
    parts.append('</ul>')
    parts.append('<div>&nbsp;</div>')

    # ── 조치 매장 ──
    parts.append('<h1>조치 매장</h1>')
    if df_action.empty:
        parts.append('<div>해당 없음</div>')
    else:
        for manager, grp in df_action.groupby('담당자', sort=True):
            manager_str = str(manager).strip()
            if not manager_str or manager_str == 'nan':
                manager_str = '미지정'
            parts.append(f'<h2>{manager_str}</h2>')
            parts.append('<ul>')
            for _, row in grp.iterrows():
                store = str(row.get('매장명', '')).strip()
                action_date = str(row.get('조치일자', '')).strip()
                target = str(row.get('목표치', '')).strip()
                action_category = str(row.get('핵심조치', '')).strip()
                action_content = str(row.get('조치내용', '')).strip()
                issue = str(row.get('문제점', '')).strip()

                parts.append(f'<li>{store}<ul>')
                if action_date and action_date != 'nan':
                    parts.append(f'<li>조치일자 {action_date}</li>')
                if target and target != 'nan':
                    parts.append(f'<li>목표치: {target}</li>')
                # 핵심조치 + 조치내용 합쳐서 표시
                detail = action_content if action_content and action_content != 'nan' else ''
                if action_category and action_category != 'nan' and not detail:
                    detail = action_category
                if detail:
                    parts.append(f'<li>{detail}</li>')
                if issue and issue != 'nan':
                    parts.append(f'<li>문제점: {issue}</li>')
                parts.append('</ul></li>')
            parts.append('</ul>')
            parts.append('<div>&nbsp;</div>')

    # ── 미조치 매장 ──
    parts.append('<h1>미조치 매장</h1>')
    if df_no_action.empty:
        parts.append('<div>해당 없음</div>')
    else:
        for manager, grp in df_no_action.groupby('담당자', sort=True):
            manager_str = str(manager).strip()
            if not manager_str or manager_str == 'nan':
                manager_str = '미지정'
            parts.append(f'<h2>{manager_str}</h2>')
            parts.append('<ul>')
            for _, row in grp.iterrows():
                store = str(row.get('매장명', '')).strip()
                action_content = str(row.get('조치내용', '')).strip()
                target = str(row.get('목표치', '')).strip()
                issue = str(row.get('문제점', '')).strip()

                # 해당 매장의 알림 지표들 (O 표시된 것)
                indicators = []
                for col in ['매출', '수수료', '배민광고', '쿠팡광고', '성실지표']:
                    val = str(row.get(col, '')).strip().upper()
                    if val in ('O', 'TRUE', 'Y'):
                        indicators.append(col)

                parts.append(f'<li>{store}<ul>')
                # 지표 나열
                if indicators and action_content and action_content != 'nan':
                    # 마지막 지표에 조치내용 포함
                    for ind in indicators[:-1]:
                        parts.append(f'<li>{ind}</li>')
                    parts.append(f'<li>{indicators[-1]}<ul>')
                    parts.append(f'<li>{action_content}</li>')
                    parts.append('</ul></li>')
                elif indicators:
                    for ind in indicators:
                        parts.append(f'<li>{ind}</li>')
                elif action_content and action_content != 'nan':
                    parts.append(f'<li>{action_content}</li>')
                if target and target != 'nan':
                    parts.append(f'<li>목표치: {target}</li>')
                if issue and issue != 'nan':
                    parts.append(f'<li>문제점: {issue}</li>')

                parts.append('</ul></li>')
            parts.append('</ul>')
            parts.append('<div>&nbsp;</div>')

    # ── 입력링크 / 대시보드 ──
    parts.append('<div># 입력링크</div>')
    parts.append(f'<div><a target="_blank" class="blue js-hyper-button urllink" href="{REPORT_GSHEET_URL}">{REPORT_GSHEET_URL}</a>&nbsp;</div>')
    parts.append('<div>&nbsp;</div>')
    parts.append('<div>&nbsp;</div>')
    parts.append('<div># 대시보드</div>')
    parts.append(f'<div><a target="_blank" class="blue js-hyper-button urllink" href="{DASHBOARD_URL}">{DASHBOARD_URL}</a>&nbsp;</div>')
    parts.append('</div>')

    return '\n'.join(parts)


# ============================================================
# Task: Flow 리포트 자동 게시
# ============================================================
def post_flow_report(**context):
    """
    버퍼 GSheet → 조치/미조치 분류 → Flow 리포트 HTML 생성 → Flow 게시

    Selenium 흐름:
    1) Flow 로그인
    2) [영업관리부] 영업관리 DX/AX 전환 프로젝트 클릭
    3) + 하위업무 추가 → 오늘 날짜 입력 → 엔터
    4) 하위업무 클릭 → 설정 → 수정
    5) CKEditor에 리포트 HTML 입력
    6) 등록 클릭
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from modules.transform.pipelines.sales.SMD_sales_visit_log_01_crawling import (
        launch_browser, do_login
    )
    from modules.extract.extract_gsheet import extract_gsheet

    now_kst = pendulum.now("Asia/Seoul")
    today_str = now_kst.to_date_string()
    today_title = _get_today_title(now_kst)
    print(f"\n{'='*60}")
    print(f"[FlowReport] 시작 | 기준일: {today_str} | 제목: {today_title}")

    # ── 1. 버퍼 GSheet 로드 ──
    print("[FlowReport] 버퍼 GSheet 데이터 로드 중...")
    df = extract_gsheet(
        url=BUFFER_GSHEET_URL,
        sheet_name=BUFFER_SHEET_NAME,
        credentials_path=CREDENTIALS_PATH,
    )

    if df is None or df.empty:
        print("[FlowReport] 버퍼 데이터 없음 → 스킵")
        return "스킵 (버퍼 데이터 없음)"

    # 매장명 빈 행 제거
    df = df[df['매장명'].astype(str).str.strip().replace('nan', '').ne('')].copy()
    df = df.reset_index(drop=True)

    if df.empty:
        print("[FlowReport] 유효 데이터 없음 → 스킵")
        return "스킵 (유효 데이터 없음)"

    # 수집날짜 기준으로 금주 / 이전주 미조치 분리
    df_current = pd.DataFrame()
    df_prev_no_action = pd.DataFrame()

    if '수집날짜' in df.columns:
        dates = df['수집날짜'].astype(str).str[:10]
        valid_dates = dates[dates.str.match(r'^\d{4}-\d{2}-\d{2}$')]
        if not valid_dates.empty:
            latest_date = valid_dates.max()
            df_current = df[dates == latest_date].copy().reset_index(drop=True)

            # 이전주 데이터 중 미조치만 추출
            older_mask = (dates != latest_date) & dates.str.match(r'^\d{4}-\d{2}-\d{2}$')
            if '구분' in df.columns:
                older_no_action_mask = older_mask & (df['구분'].astype(str).str.strip() != '조치')
                df_prev_no_action = df[older_no_action_mask].copy().reset_index(drop=True)

            print(f"[FlowReport] 수집날짜={latest_date} 금주: {len(df_current)}건")
            print(f"[FlowReport] 이전주 미조치: {len(df_prev_no_action)}건")
    else:
        df_current = df.copy()

    if df_current.empty:
        return "스킵 (최신 수집날짜 데이터 없음)"

    print(f"[FlowReport] 금주 데이터: {len(df_current)}건")
    print(f"[FlowReport] 구분 분포: {df_current['구분'].value_counts().to_dict() if '구분' in df_current.columns else 'N/A'}")

    # ── 2. HTML 리포트 생성 ──
    body_html = _build_flow_report_html(df_current, df_prev_no_action, now_kst)
    print(f"[FlowReport] HTML 생성 완료 ({len(body_html)}자)")
    print(body_html[:500])

    # ── 3. Selenium Flow 게시 ──
    driver = None
    try:
        driver = launch_browser()
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
        print("[FlowReport] 내 프로젝트 클릭")

        # ② [영업관리부] 영업관리 DX/AX 전환 프로젝트 클릭
        project_item = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, f"li#project-{FLOW_PROJECT_SRNO}")
        ))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", project_item)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", project_item)
        time.sleep(5)  # 프로젝트 페이지 로딩 대기 (콘텐츠 렌더링 포함)
        print(f"[FlowReport] '{FLOW_PROJECT_NAME}' 프로젝트 클릭")

        # 프로젝트 콘텐츠 로딩 대기 (게시물 목록이 나타날 때까지)
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".subtask-list, .post-list, .task-list, .js-add-subtask-button")
            ))
            print("[FlowReport] 프로젝트 콘텐츠 로딩 완료")
        except Exception:
            print("[FlowReport] 콘텐츠 로딩 대기 타임아웃 → 계속 진행")
        time.sleep(2)

        # 페이지 맨 아래로 스크롤 (+ 하위업무 추가 버튼이 하단에 위치)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        print("[FlowReport] 페이지 하단 스크롤 완료")

        # ③ + 하위업무 추가 버튼 클릭
        add_subtask_btn = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "button.js-add-subtask-button")
        ))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", add_subtask_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", add_subtask_btn)
        time.sleep(1)
        print("[FlowReport] + 하위업무 추가 클릭")

        # ④ 오늘 날짜 입력 후 엔터
        # + 하위업무 추가 클릭 후 새 입력창이 나타날 때까지 대기
        time.sleep(2)

        # JS로 non-readonly input 찾기 → scrollIntoView → focus → 값 세팅 → Enter 디스패치
        # send_keys가 element not interactable 에러를 내므로 전부 JS로 처리
        subtask_input = wait.until(lambda d: next(
            (el for el in d.find_elements(By.CSS_SELECTOR, "input.js-subtask-input")
             if not el.get_attribute("readonly")),
            None
        ))
        driver.execute_script(
            """
            var el = arguments[0];
            var title = arguments[1];
            el.scrollIntoView({block: 'center'});
            el.focus();
            el.value = title;
            el.dispatchEvent(new Event('input', {bubbles: true}));
            el.dispatchEvent(new Event('change', {bubbles: true}));
            el.dispatchEvent(new KeyboardEvent('keydown', {key: 'Enter', code: 'Enter', keyCode: 13, bubbles: true}));
            el.dispatchEvent(new KeyboardEvent('keypress', {key: 'Enter', code: 'Enter', keyCode: 13, bubbles: true}));
            el.dispatchEvent(new KeyboardEvent('keyup', {key: 'Enter', code: 'Enter', keyCode: 13, bubbles: true}));
            """,
            subtask_input, today_title
        )
        time.sleep(3)
        print(f"[FlowReport] 하위업무 제목 입력: {today_title}")

        # ⑤ 방금 생성한 하위업무 찾아서 클릭
        # 제목이 today_title인 하위업무의 display 텍스트를 찾아 클릭
        subtask_items = driver.find_elements(By.CSS_SELECTOR, "p.subtask__tit--display")
        target_subtask = None
        for item in subtask_items:
            if today_title in item.text:
                target_subtask = item
                break

        if not target_subtask:
            raise RuntimeError(f"하위업무 '{today_title}' 찾기 실패")

        driver.execute_script("arguments[0].click();", target_subtask)
        time.sleep(2)
        print(f"[FlowReport] 하위업무 '{today_title}' 클릭")

        # ⑥ 설정(점3개) 버튼 클릭
        setting_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "button.js-setting-button.set-btn")
        ))
        driver.execute_script("arguments[0].click();", setting_btn)
        time.sleep(1)
        print("[FlowReport] 설정 버튼 클릭")

        # ⑦ 수정 클릭
        modify_item = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "li.js-setting-item[data-code='modify']")
        ))
        driver.execute_script("arguments[0].click();", modify_item)
        time.sleep(2)
        print("[FlowReport] 수정 클릭")

        # ⑧ CKEditor에 리포트 HTML 입력
        ckeditor_ready = False
        for _i in range(15):
            try:
                # Flow 하위업무 수정 시 에디터 인스턴스명이 다를 수 있으므로 첫 번째 인스턴스 사용
                if driver.execute_script(
                    """
                    if (typeof CKEDITOR === 'undefined' || !CKEDITOR.instances) return false;
                    var keys = Object.keys(CKEDITOR.instances);
                    if (keys.length === 0) return false;
                    return CKEDITOR.instances[keys[keys.length - 1]].status === 'ready';
                    """
                ):
                    ckeditor_ready = True
                    break
            except Exception:
                pass
            time.sleep(1)

        if ckeditor_ready:
            driver.execute_script(
                """
                var keys = Object.keys(CKEDITOR.instances);
                CKEDITOR.instances[keys[keys.length - 1]].setData(arguments[0]);
                """,
                body_html,
            )
            print("[FlowReport] 본문 입력 완료 (CKEditor API)")
        else:
            # CKEditor 미초기화 → iframe body 직접 조작
            print("[FlowReport] CKEditor 미초기화 → iframe 직접 조작")
            cke_iframe = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "iframe.cke_wysiwyg_frame")
            ))
            driver.switch_to.frame(cke_iframe)
            driver.execute_script(
                "document.body.innerHTML = arguments[0];", body_html
            )
            driver.switch_to.default_content()
            print("[FlowReport] 본문 입력 완료 (iframe 직접)")

        time.sleep(1)

        # ⑨ 등록 버튼 클릭
        submit_btn = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "button.js-complete-btn.confirm")
        ))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submit_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", submit_btn)
        time.sleep(3)
        print(f"[FlowReport] 등록 완료: {today_title}")

        return f"Flow 리포트 게시 완료 (금주 {len(df_current)}건, 이전 미조치 {len(df_prev_no_action)}건, {today_title})"

    except Exception as e:
        import traceback
        print(f"[FlowReport] 실패: {e}")
        traceback.print_exc()
        raise

    finally:
        if driver:
            driver.quit()
            print("[FlowReport] 브라우저 종료")


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    description='영업관리 주간 Flow 리포트 자동 게시 (조치/미조치 현황)',
    schedule=SMD_ORDERS_TIME,
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['flow', 'report', '영업관리', '리포트'],
) as dag:

    wait_for_smd_08 = ExternalTaskSensor(
        task_id='wait_for_smd_08',
        external_dag_id='SMD_08_gsheet_report_Dags',
        external_task_id='upload_grp_template',
        allowed_states=['success'],
        failed_states=['failed'],
        mode='reschedule',
        timeout=3600,
        poke_interval=60,
        check_existence=True,
        soft_fail=True,
    )

    t_post_report = PythonOperator(
        task_id='post_flow_report',
        python_callable=post_flow_report,
        execution_timeout=timedelta(minutes=15),
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    wait_for_smd_08 >> t_post_report
