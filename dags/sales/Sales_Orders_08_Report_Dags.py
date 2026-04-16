"""
SMD_08 - 매장별 조치 현황 리포트 GSheet 업로드 DAG

📋 처리 흐름:
1. SMD_07 완료 대기 (sales_daily_orders_alerts_grp.csv 생성)
2. [collect_to_buffer] gsheet1(리포트) 조치일자 입력 행 → 수집날짜 추가 → gsheet2(버퍼) append
3. [process_buffer_to_accum] gsheet2에서 조치일자+7일 경과 행 → orders 조인 → gsheet3(누적) append → gsheet2에서 제거
4. [clear_report_data] gsheet1 헤더만 남기고 데이터 삭제
5. [upload_grp_template] 새 grp.csv 템플릿 → gsheet1 업로드

📁 입력 파일:
- sales_daily_orders_alerts_grp.csv (SMD_07 생성)
- sales_daily_orders_upload.csv (SMD_05/06 생성)
"""

import os
import logging
import pendulum
import pandas as pd
from pathlib import Path
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)

from modules.transform.utility.paths import LOCAL_DB
from modules.load.load_gsheet import save_to_gsheet
from modules.extract.extract_gsheet import extract_gsheet
from modules.transform.utility.io import SMD_ORDERS_TIME

filename = os.path.basename(__file__)

# ── 날짜 파싱 헬퍼 (Excel 시리얼 숫자 포함) ──────────────────────
def _parse_date_series(s: pd.Series) -> pd.Series:
    """
    문자열/숫자 혼합 날짜 시리즈를 'YYYY-MM-DD' 문자열로 변환.
    Excel 시리얼 숫자(예: 46074 → 2026-02-21)도 처리.
    """
    from datetime import datetime, timedelta

    def _convert_one(val):
        v = str(val).strip()
        if v in ('', 'nan', 'NaT', 'None'):
            return None
        # Excel 시리얼 숫자 감지: 순수 숫자 5자리 (40000~50000 범위 = 2009~2036년)
        try:
            n = float(v)
            if 40000 <= n <= 60000:
                # Excel 시리얼 → 날짜 (1899-12-30 기준)
                dt = datetime(1899, 12, 30) + timedelta(days=n)
                return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            pass
        # 일반 날짜 파싱
        try:
            return pd.to_datetime(v, errors='coerce').strftime('%Y-%m-%d')
        except Exception:
            return None

    return s.map(_convert_one)

# ── 설정 ─────────────────────────────────────────────────
CREDENTIALS_PATH = "/opt/airflow/config/rare-ethos-483607-i5-45c9bec5b193.json"

# 리포트 시트 (담당자 입력용 템플릿 → 헤더만 유지)
REPORT_GSHEET_URL  = "https://docs.google.com/spreadsheets/d/1OFTQ0WyKgcwmBxwESWWpCD6E6O2i9kYYN6-YxNlx9xQ/edit?gid=0#gid=0"
REPORT_SHEET_NAME  = "시트1"
REPORT_SHEET2_NAME = "시트2"   # 핵심조치/조치내용 소스 시트

# 버퍼 시트 (gsheet1 수집 후 7일 대기소)
BUFFER_GSHEET_URL  = "https://docs.google.com/spreadsheets/d/1xLhw-qS7zBBsAtwzyUO9N1AYT-zgECcfyTX9GBsi1k8/edit?gid=0#gid=0"
BUFFER_SHEET_NAME  = "시트1"

# 누적 시트 (append)
ACCUM_GSHEET_URL   = "https://docs.google.com/spreadsheets/d/1qOrszgQ6ztwR-lf2PyxEiolWeMbWwUZwgKWa9aQKVd8/edit?gid=0#gid=0"
ACCUM_SHEET_NAME   = "시트1"

# 입력 파일
GRP_CSV_PATH    = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_alerts_grp.csv'
ORDERS_CSV_PATH = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_upload.csv'
TMP_CSV_PATH    = LOCAL_DB / 'temp' / 'tmp_gsheet.csv'

# grp → gsheet 1 업로드 시 포함할 컬럼 (담당자 입력 템플릿)
TEMPLATE_COLUMNS = [
    '날짜', '매장명', '전주상태', '담당자',
    '매출', '수수료', '배민광고', '쿠팡광고', '성실지표',
    '조치일자', '목표치', '핵심조치', '조치내용', '조치전', '조치후', '문제점',
]

# 버퍼 시트 컬럼 (TEMPLATE_COLUMNS + 수집날짜 + 구분)
BUFFER_COLUMNS = TEMPLATE_COLUMNS + ['수집날짜', '구분']

# 최종 출력 컬럼
SELECTED_COLUMNS = [
    'order_daily', '매장명', '전주상태', '담당자', '매출', '수수료',
    '배민광고', '쿠팡광고', '성실지표', '조치일자', '목표치', '핵심조치', '조치내용', '조치전', '조치후', '문제점',
    '담당자_order', 'email',
    'total_order_count', 'total_amount', 'fee_ad', 'ARPU',
    'sum_7d_recent', 'sum_7d_prev', 'fee_ratio',
    'avg_7d_recent', 'avg_7d_prev',
    '쿠팡_광고효과', '쿠팡_광고효과_avg_7d_recent', '쿠팡_광고효과_avg_7d_prev',
    '배민_광고효과', '배민_광고효과_avg_7d_recent', '배민_광고효과_avg_7d_prev',
    '조리시간_7d_recent', '조리시간_7d_prev',
    '접수시간_7d_recent', '접수시간_7d_prev',
    '재주문율_7d_recent', '재주문율_7d_prev',
    '별점_7d_recent', '별점_7d_prev',
]
# ─────────────────────────────────────────────────────────


# ============================================================
# [Phase 1] task 0 – gsheet1 조치일자 입력 행 → gsheet2(버퍼) 수집
# ============================================================
def collect_to_buffer(**context):
    """
    gsheet1(리포트)에서 조치일자가 입력된 행을 읽어
    수집날짜(오늘)를 추가한 뒤 gsheet2(버퍼 시트)에 append.
    gsheet2는 조치일자+7일 대기 후 gsheet3 누적 이동 전 임시 보관소 역할.
    """
    logger.info("[collect_to_buffer] gsheet1 데이터 수집 시작")

    df = extract_gsheet(
        url=REPORT_GSHEET_URL,
        sheet_name=REPORT_SHEET_NAME,
        credentials_path=CREDENTIALS_PATH,
    )

    if df is None or df.empty:
        logger.info("[collect_to_buffer] gsheet1 데이터 없음 → 버퍼 이동 없음")
        return "gsheet1 데이터 없음"

    # 매장명 빈 행 제거
    df = df[df['매장명'].astype(str).str.strip().replace('nan', '').ne('')].copy()
    df = df.reset_index(drop=True)

    # 조치일자 정규화
    if '조치일자' not in df.columns:
        df['조치일자'] = ''
    df['조치일자'] = _parse_date_series(df['조치일자'].astype(str)).fillna('')

    # 조치일자 OR 조치내용 입력된 행만 버퍼로 이동
    action_date_filled = df['조치일자'].astype(str).str.strip().replace('nan', '').ne('')
    action_content_filled = (
        df.get('조치내용', pd.Series([''] * len(df), index=df.index))
        .astype(str).str.strip().replace('nan', '').ne('')
    )
    filled_mask = action_date_filled | action_content_filled
    df_filled = df[filled_mask].copy().reset_index(drop=True)

    logger.info(f"[collect_to_buffer] gsheet1 전체: {len(df)}건, 조치일자/조치내용 입력: {len(df_filled)}건")

    if df_filled.empty:
        logger.info("[collect_to_buffer] 조치일자/조치내용 입력 없음 → 버퍼 이동 없음")
        return "조치일자/조치내용 미입력 → 버퍼 이동 없음"

    # 날짜 컬럼 정규화
    if '날짜' in df_filled.columns:
        df_filled['날짜'] = _parse_date_series(df_filled['날짜'].astype(str))

    # 수집날짜 추가 (오늘 날짜)
    today_str = pendulum.now('Asia/Seoul').strftime('%Y-%m-%d')
    df_filled['수집날짜'] = today_str

    # 구분 컬럼 추가: 조치일자 + 핵심조치 모두 있으면 '조치', 아니면 '미조치'
    has_action_date = df_filled['조치일자'].astype(str).str.strip().replace('nan', '').ne('')
    has_action_category = (
        df_filled.get('핵심조치', pd.Series([''] * len(df_filled), index=df_filled.index))
        .astype(str).str.strip().replace('nan', '').ne('')
    )
    df_filled['구분'] = (has_action_date & has_action_category).map({True: '조치', False: '미조치'})
    logger.info(f"[collect_to_buffer] 조치: {(df_filled['구분']=='조치').sum()}건, 미조치: {(df_filled['구분']=='미조치').sum()}건")

    # BUFFER_COLUMNS 기준으로 컬럼 정렬
    buf_df = df_filled.reindex(columns=BUFFER_COLUMNS)

    logger.info(f"[collect_to_buffer] gsheet2 버퍼에 {len(buf_df)}건 append 예정")

    result = save_to_gsheet(
        df=buf_df,
        sheet_name=BUFFER_SHEET_NAME,
        mode='append_unique',
        primary_key=['날짜', '매장명'],
        credentials_path=CREDENTIALS_PATH,
        url=BUFFER_GSHEET_URL,
    )
    if result.get('success'):
        logger.info(f"[collect_to_buffer] ✅ gsheet2 버퍼 append 완료: {len(buf_df)}건")
    else:
        raise RuntimeError(f"gsheet2 버퍼 append 실패: {result.get('error')}")

    return f"버퍼 이동: {len(buf_df)}건 (수집날짜: {today_str})"


# ============================================================
# [Phase 1] task 1 – grp.csv → gsheet 1 템플릿 업로드
# ============================================================
def upload_grp_template(**context):
    """
    grp.csv(SMD_07 알림 대상)를 읽어 날짜/매장명/담당자/지표 컬럼만 추려
    gsheet 1에 overwrite 업로드.
    담당자가 조치일자·핵심조치·조치내용을 직접 기입할 템플릿 역할.
    """
    logger.info("[upload_grp_template] 시작")
    if not GRP_CSV_PATH.exists():
        raise FileNotFoundError(f"grp CSV 없음: {GRP_CSV_PATH}")

    grp = pd.read_csv(GRP_CSV_PATH, encoding='utf-8-sig')
    logger.info(f"[grp] {len(grp):,}건  컬럼: {grp.columns.tolist()}")

    # order_daily → 날짜 컬럼으로 사용 (Excel 시리얼 숫자 포함 변환)
    grp['날짜'] = _parse_date_series(
        grp['order_daily'].astype(str)
    )

    grp['매장명'] = grp['매장명'].astype(str).str.strip()

    # grp.csv 실제 status 컬럼 → 표시용 컬럼에 'O' 매핑
    # (위험/주의 → 'O', 그 외 → 빈값)
    STATUS_FLAG_MAP = {
        '매출':     'status',
        '수수료':   'fee_status',
        '쿠팡광고': '쿠팡_광고효과_status',
        '배민광고': '배민_광고효과_status',
        '성실지표': 'service_status',
    }
    for display_col, src_col in STATUS_FLAG_MAP.items():
        if src_col in grp.columns:
            grp[display_col] = grp[src_col].map(
                lambda x: 'O' if str(x).strip() in ('위험', '주의') else ''
            )
        else:
            grp[display_col] = ''

    # 담당자 없으면 빈 컬럼 추가
    if '담당자' not in grp.columns:
        grp['담당자'] = ''

    # ── 전주상태 계산 (pre_* 컬럼 → JSON {"매출":"위험","수수료":"정상",...}) ──
    import json as _json
    _PRE_MAP = {
        '매출':     'pre_status',
        '수수료':   'pre_fee_status',
        '배민광고': 'pre_배민_광고효과_status',
        '쿠팡광고': 'pre_쿠팡_광고효과_status',
        '성실지표': 'pre_service_status',
    }
    def _calc_prev_status(row):
        return _json.dumps(
            {label: str(row.get(col, '정상') or '정상') for label, col in _PRE_MAP.items()},
            ensure_ascii=False
        )
    grp['전주상태'] = grp.apply(_calc_prev_status, axis=1)
    logger.info(f"[전주상태] JSON 형태 계산 완료: {len(grp):,}건")
    # grp.csv는 SMD_07에서 이미 연속위험 필터링 완료 → 추가 필터 불필요

    # 매장별 1개만 (최신 날짜 기준 중복 제거)
    grp = grp.sort_values('날짜', ascending=False).drop_duplicates(subset=['매장명'], keep='first')
    grp = grp.sort_values(['담당자', '매장명']).reset_index(drop=True)
    logger.info(f"[중복 제거] 매장별 1개: {len(grp):,}건")

    # 조치일자·목표치·핵심조치·조치내용·조치전·조치후·문제점: 담당자 입력 전용 열 → 코드에서 절대 쓰지 않음
    # (J:P 열은 clear / write 대상에서 제외 → 드롭다운 유효성 규칙 보존)
    AUTO_COLUMNS = ['날짜', '매장명', '전주상태', '담당자', '매출', '수수료', '배민광고', '쿠팡광고', '성실지표']  # A:I

    tmpl = grp.reindex(columns=AUTO_COLUMNS)
    logger.info(f"[template] {len(tmpl):,}건 업로드 예정 (A:I 자동입력 / J:P 담당자 입력 보존)")

    # ── 드롭다운(데이터유효성) 보존 방식으로 업로드 ─────────────────────────
    # mode='overwrite'는 시트 전체를 지우고 헤더까지 재작성하여 드롭다운이 사라짐.
    # 대신: 1행(헤더)은 절대 건드리지 않고 A2:K{행수+여유} 값만 clear 후 A2부터 덮어씀.
    import re as _re
    from google.oauth2.service_account import Credentials as _Creds
    from googleapiclient.discovery import build as _build

    _scopes = ['https://www.googleapis.com/auth/spreadsheets']
    _creds  = _Creds.from_service_account_file(CREDENTIALS_PATH, scopes=_scopes)
    _svc    = _build('sheets', 'v4', credentials=_creds)
    _sid    = _re.search(r'/d/([a-zA-Z0-9_-]+)', REPORT_GSHEET_URL).group(1)
    _sname  = REPORT_SHEET_NAME

    # ① 헤더 행(1행) 확인 – 없으면 한 번만 작성
    _hdr_resp = _svc.spreadsheets().values().get(
        spreadsheetId=_sid, range=f"{_sname}!1:1"
    ).execute()
    _existing_hdr = (_hdr_resp.get('values') or [[]])[0]
    if _existing_hdr != TEMPLATE_COLUMNS:
        _svc.spreadsheets().values().update(
            spreadsheetId=_sid,
            range=f"{_sname}!A1",
            valueInputOption='RAW',
            body={'values': [TEMPLATE_COLUMNS]},
        ).execute()
        logger.info(f"[upload_grp_template] 헤더 작성: {TEMPLATE_COLUMNS}")

    # ② A:I(자동입력 열)만 값 지우기 → J:P(조치일자·목표치·핵심조치·조치내용·조치전·조치후·문제점) 절대 건드리지 않음
    # values().clear()는 값만 삭제하고 유효성 검사(드롭다운) 규칙은 보존하지만,
    # J:P 열은 담당자 입력 전용이므로 아예 범위에서 제외
    _last_data_row = len(tmpl) + 100          # 여유분 포함
    _clear_range   = f"{_sname}!A2:I{_last_data_row}"
    _svc.spreadsheets().values().clear(
        spreadsheetId=_sid, range=_clear_range, body={}
    ).execute()
    logger.info(f"[upload_grp_template] {_clear_range} 값만 지움 (J:P 드롭다운/입력값 보존)")

    # ③ A2:I 범위에만 데이터 작성 (헤더 제외, J:N 미접촉)
    def _fmt(v):
        if v is None or (isinstance(v, float) and v != v):
            return ''
        return str(v) if not isinstance(v, (int, float, bool)) else v

    _rows = [[_fmt(v) for v in row] for row in tmpl.values.tolist()]
    if _rows:
        _svc.spreadsheets().values().update(
            spreadsheetId=_sid,
            range=f"{_sname}!A2",
            valueInputOption='RAW',
            body={'values': _rows},
        ).execute()

    logger.info(f"[upload_grp_template] ✅ 업로드 완료 ({len(tmpl):,}건, A:I만 작성 / J:P 드롭다운 보존)")
    return f"템플릿 업로드: {len(tmpl):,}건"


# ============================================================
# task 5 – 시트2 L(핵심조치), M(조치내용) → 시트1 L:M 복사
# ============================================================
def copy_sheet2_lm_to_sheet1(**context):
    """
    시트2의 L(핵심조치), M(조치내용)을 시트1에 복사.
    - 시트1의 데이터 행 수만큼만 복사 (초과분 무시)
    - L:M 범위만 접근 → 드롭다운/서식 보존
    """
    import re
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)
    sid = re.search(r'/d/([a-zA-Z0-9_-]+)', REPORT_GSHEET_URL).group(1)

    # ① 시트1 데이터 행 수 확인
    sheet1_resp = service.spreadsheets().values().get(
        spreadsheetId=sid, range=f"{REPORT_SHEET_NAME}!A2:A"
    ).execute()
    sheet1_row_count = len(sheet1_resp.get('values') or [])

    if sheet1_row_count == 0:
        logger.info("[copy_lm] 시트1 데이터 없음 → 복사 불필요")
        return "시트1 데이터 없음"

    # ② 시트2 L:M 읽기 (헤더 제외, 2행부터)
    sheet2_resp = service.spreadsheets().values().get(
        spreadsheetId=sid, range=f"{REPORT_SHEET2_NAME}!L2:M"
    ).execute()
    sheet2_values = sheet2_resp.get('values') or []

    if not sheet2_values:
        logger.info("[copy_lm] 시트2 L:M 데이터 없음 → 복사 불필요")
        return "시트2 데이터 없음"

    # ③ 시트1 행 수만큼만 자르기 + 셀 개수 맞추기 (행당 2칸 보장)
    rows_to_copy = sheet2_values[:sheet1_row_count]
    rows_to_copy = [r + [''] * (2 - len(r)) if len(r) < 2 else r[:2] for r in rows_to_copy]

    # ④ 시트1 L2:M에 쓰기 (L:M 범위만 → J,K,N:P 드롭다운 무접촉)
    service.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{REPORT_SHEET_NAME}!L2",
        valueInputOption='USER_ENTERED',
        body={'values': rows_to_copy},
    ).execute()

    logger.info(f"[copy_lm] ✅ 시트2→시트1 L:M 복사 완료 ({len(rows_to_copy)}건)")
    return f"L:M 복사 완료: {len(rows_to_copy)}건"


# ============================================================
# [Phase 2] task 2 – gsheet2(버퍼) → 7일 경과 행 → orders 조인 → gsheet3 적재 → gsheet2 제거
# ============================================================
def process_buffer_to_accum(**context):
    """
    1) gsheet2(버퍼) 로드 → 조치일자+7일 경과 행만 처리 대상으로 분리
    2) orders CSV 로드 + 날짜 정규화
    3) 조치일자 기준 7일 전후 윈도우 직접 집계
       prev:   [조치일자-8d, 조치일자-1d] 7일간 합계/평균
       recent: [조치일자+1d, 조치일자+7d] 7일간 합계/평균
    4) 중복 제거 (order_daily + 매장명 기준)
    5) O/NaN → True/False 변환
    6) gsheet3(누적)에 append + gsheet2(버퍼)에서 처리된 행 제거
    """
    from airflow.exceptions import AirflowSkipException
    import re as _re
    from google.oauth2.service_account import Credentials as _Creds
    from googleapiclient.discovery import build as _build

    today = pd.Timestamp.now(tz='Asia/Seoul').normalize().tz_localize(None)
    logger.info(f"[process_buffer] 기준일: {today.date()}")

    # ── gsheet2(버퍼) 읽기 ───────────────────────────────
    df_buf = extract_gsheet(
        url=BUFFER_GSHEET_URL,
        sheet_name=BUFFER_SHEET_NAME,
        credentials_path=CREDENTIALS_PATH,
    )

    if df_buf is None or df_buf.empty:
        logger.info("[process_buffer] gsheet2 데이터 없음 → Skip")
        raise AirflowSkipException("[process_buffer] gsheet2 데이터 없음")

    # 매장명 빈 행 제거
    df_buf = df_buf[df_buf['매장명'].astype(str).str.strip().replace('nan', '').ne('')].copy()
    df_buf = df_buf.reset_index(drop=True)

    # 조치일자 정규화
    if '조치일자' not in df_buf.columns:
        df_buf['조치일자'] = ''
    df_buf['조치일자'] = _parse_date_series(df_buf['조치일자'].astype(str)).fillna('')

    # ── 7일 경과 여부 판단 ────────────────────────────────
    action_dt_series = pd.to_datetime(df_buf['조치일자'], errors='coerce')
    ready_mask = (action_dt_series + pd.Timedelta(days=7)) <= today

    df_ready   = df_buf[ready_mask].copy().reset_index(drop=True)
    df_pending = df_buf[~ready_mask].copy().reset_index(drop=True)

    logger.info(f"[process_buffer] 전체: {len(df_buf)}건, 처리 대상(7일 경과): {len(df_ready)}건, 대기 중: {len(df_pending)}건")

    if df_ready.empty:
        logger.info("[process_buffer] 7일 경과 데이터 없음 → Skip")
        raise AirflowSkipException("[process_buffer] 처리할 데이터 없음")

    # 구분 == '조치' 행만 gsheet3으로 이동 (미조치는 버퍼에서 제거만 됨)
    if '구분' in df_ready.columns:
        df_ready_action = df_ready[df_ready['구분'].astype(str).str.strip() == '조치'].copy().reset_index(drop=True)
    else:
        df_ready_action = df_ready.copy()
    logger.info(f"[process_buffer] gsheet3 이동(조치): {len(df_ready_action)}건, 미조치 제거: {len(df_ready) - len(df_ready_action)}건")

    # ── 컬럼 호환: 날짜 → order_daily ────────────────────
    df_filled = df_ready_action.copy()
    if '날짜' in df_filled.columns and 'order_daily' not in df_filled.columns:
        df_filled.rename(columns={'날짜': 'order_daily'}, inplace=True)

    df_filled['order_daily'] = _parse_date_series(df_filled['order_daily'].astype(str))
    df_filled['매장명'] = df_filled['매장명'].astype(str).str.strip()

    logger.info(f"[조치일자] 처리 행: {len(df_filled):,}건")
    logger.info(f"[조치일자] 범위: {df_filled['조치일자'].min()} ~ {df_filled['조치일자'].max()}")

    # ── orders 로드 ───────────────────────────────────────
    if not ORDERS_CSV_PATH.exists():
        raise FileNotFoundError(f"orders CSV 없음: {ORDERS_CSV_PATH}")

    order_df = pd.read_csv(ORDERS_CSV_PATH, encoding='utf-8-sig')
    date_token = order_df['order_daily'].astype(str).str.extract(
        r'(\d{8}|\d{4}[./-]\d{1,2}[./-]\d{1,2})', expand=False
    )
    date_token = (date_token
                  .str.replace('.', '-', regex=False)
                  .str.replace('/', '-', regex=False))
    order_df['order_daily'] = (
        pd.to_datetime(date_token, errors='coerce', format='mixed')
        .dt.strftime('%Y-%m-%d')
    )
    order_df['매장명'] = order_df['매장명'].astype(str).str.strip()
    order_df['_dt'] = pd.to_datetime(order_df['order_daily'], errors='coerce')
    logger.info(f"[orders] {len(order_df):,}건  날짜범위: {order_df['order_daily'].min()} ~ {order_df['order_daily'].max()}")

    # ── 7일 전후 윈도우 직접 계산 (영업일 기준) ─────────────────────────────
    # prev:   조치일자 당일 제외, 바로 이전 영업일 7개 (설날/휴무일 자동 스킵)
    # recent: 조치일자 다음날(+1d) 이후 첫 영업일부터 7개
    # 예) 조치일자=2026-02-13 → prev: 02-12~02-05 7개 / recent: 02-14~02-22 7개

    ORDER_7D_AGGS = [
        # (결과_prev컬럼,                  결과_recent컬럼,                  원본컬럼,       집계)
        ('sum_7d_prev',                 'sum_7d_recent',                 'total_amount',  'sum'),
        ('avg_7d_prev',                 'avg_7d_recent',                 'fee_ratio',     'mean'),
        ('쿠팡_광고효과_avg_7d_prev',    '쿠팡_광고효과_avg_7d_recent',    '쿠팡_광고효과',  'mean'),
        ('배민_광고효과_avg_7d_prev',    '배민_광고효과_avg_7d_recent',    '배민_광고효과',  'mean'),
        ('조리시간_7d_prev',             '조리시간_7d_recent',             '조리소요시간',   'mean'),
        ('접수시간_7d_prev',             '접수시간_7d_recent',             '주문접수시간',   'mean'),
        ('재주문율_7d_prev',             '재주문율_7d_recent',             '최근재주문율',   'mean'),
        ('별점_7d_prev',                '별점_7d_recent',                '최근별점',       'mean'),
    ]
    # 조치일자 당일 단건 값 (spot)
    SPOT_COLS = [
        'total_order_count', 'total_amount', 'fee_ad', 'ARPU',
        'fee_ratio', '쿠팡_광고효과', '배민_광고효과', 'email', '담당자_order',
    ]

    rows = []
    for _, row in df_filled.iterrows():
        r = row.to_dict()
        store     = str(row['매장명']).strip()
        action_dt = pd.to_datetime(row['조치일자'], errors='coerce')

        if pd.isna(action_dt):
            rows.append(r)
            continue

        store_data = order_df[order_df['매장명'] == store].copy()

        # ① 조치일자 당일 단건 값
        spot = store_data[store_data['_dt'] == action_dt]
        if not spot.empty:
            for col in SPOT_COLS:
                if col in spot.columns:
                    r[col] = spot.iloc[0][col]

        # ② 이전 7영업일: 조치일자 당일 제외, 가장 최근 7개
        prev_data   = store_data[store_data['_dt'] < action_dt].nlargest(7, '_dt')
        # ③ 이후 7영업일: 조치일자 다음날(+1d) 이후 첫 영업일부터 7개
        after_start = action_dt + pd.Timedelta(days=1)
        recent_data = store_data[store_data['_dt'] >= after_start].nsmallest(7, '_dt')

        logger.info(
            f"[{store}] 조치일자={action_dt.date()} "
            f"prev({len(prev_data)}영업일 {prev_data['_dt'].min().date() if not prev_data.empty else 'N/A'}~{prev_data['_dt'].max().date() if not prev_data.empty else 'N/A'}) "
            f"recent({len(recent_data)}영업일 {recent_data['_dt'].min().date() if not recent_data.empty else 'N/A'}~{recent_data['_dt'].max().date() if not recent_data.empty else 'N/A'} / 시작기준={after_start.date()})"
        )

        for col_prev, col_recent, src_col, func in ORDER_7D_AGGS:
            if src_col in store_data.columns:
                p_vals = pd.to_numeric(prev_data[src_col],   errors='coerce').dropna()
                q_vals = pd.to_numeric(recent_data[src_col], errors='coerce').dropna()
                if func == 'sum':
                    r[col_prev]   = float(p_vals.sum())          if len(p_vals) > 0 else None
                    r[col_recent] = float(q_vals.sum())          if len(q_vals) > 0 else None
                else:
                    r[col_prev]   = round(float(p_vals.mean()), 4) if len(p_vals) > 0 else None
                    r[col_recent] = round(float(q_vals.mean()), 4) if len(q_vals) > 0 else None

        rows.append(r)

    merged = pd.DataFrame(rows)
    merged = merged.drop_duplicates(subset=['order_daily', '매장명'], keep='last').reset_index(drop=True)
    logger.info(f"[order_daily] {sorted(merged['order_daily'].dropna().unique().tolist())}")
    logger.info(f"[조치일자]    {sorted(merged['조치일자'].dropna().unique().tolist())}")

    # ── 컬럼 선택 ─────────────────────────────────────────
    fin_df = merged.reindex(columns=SELECTED_COLUMNS)

    # ── O / 'TRUE' → True, 그 외 → False ──────────────────
    for col in ['매출', '수수료', '배민광고', '쿠팡광고', '성실지표']:
        if col in fin_df.columns:
            fin_df[col] = fin_df[col].map(
                lambda x: True if (x is True or str(x).strip().upper() in ('O', 'TRUE')) else False
            )

    # ── gsheet3(누적)에 append (조치 건만) ───────────────────
    if fin_df.empty:
        logger.info("[process_buffer] 조치 건 없음 → gsheet3 append 생략")
        # 버퍼에서 만료된 행만 제거하고 종료
        _scopes = ['https://www.googleapis.com/auth/spreadsheets']
        _creds  = _Creds.from_service_account_file(CREDENTIALS_PATH, scopes=_scopes)
        _svc    = _build('sheets', 'v4', credentials=_creds)
        _sid    = _re.search(r'/d/([a-zA-Z0-9_-]+)', BUFFER_GSHEET_URL).group(1)
        _sname  = BUFFER_SHEET_NAME
        _svc.spreadsheets().values().clear(
            spreadsheetId=_sid, range=f"{_sname}!A2:ZZ", body={}
        ).execute()
        if not df_pending.empty:
            def _fmt(v):
                if v is None or (isinstance(v, float) and v != v):
                    return ''
                return str(v) if not isinstance(v, (int, float, bool)) else v
            buf_rows = [
                [_fmt(r.get(c, '')) for c in BUFFER_COLUMNS]
                for _, r in df_pending.iterrows()
            ]
            _svc.spreadsheets().values().update(
                spreadsheetId=_sid,
                range=f"{_sname}!A2",
                valueInputOption='RAW',
                body={'values': buf_rows},
            ).execute()
            logger.info(f"[process_buffer] gsheet2 미처리 행 {len(df_pending)}건 유지")
        return f"처리 완료(조치 없음): 만료 {len(df_ready)}건 제거 / 대기 중: {len(df_pending)}건"

    result = save_to_gsheet(
        df=fin_df,
        sheet_name=ACCUM_SHEET_NAME,
        mode='append_unique',
        primary_key=['조치일자', '매장명'],
        credentials_path=CREDENTIALS_PATH,
        url=ACCUM_GSHEET_URL,
    )
    if result.get('success'):
        logger.info(f"[process_buffer] ✅ gsheet3 append 완료: {len(fin_df)}건")
    else:
        raise RuntimeError(f"누적 GSheet append 실패: {result.get('error')}")

    # ── gsheet2(버퍼)에서 처리된 행 제거 (df_pending만 남김) ──────────────
    _scopes = ['https://www.googleapis.com/auth/spreadsheets']
    _creds  = _Creds.from_service_account_file(CREDENTIALS_PATH, scopes=_scopes)
    _svc    = _build('sheets', 'v4', credentials=_creds)
    _sid    = _re.search(r'/d/([a-zA-Z0-9_-]+)', BUFFER_GSHEET_URL).group(1)
    _sname  = BUFFER_SHEET_NAME

    # A2 이하 모두 클리어
    _svc.spreadsheets().values().clear(
        spreadsheetId=_sid, range=f"{_sname}!A2:ZZ", body={}
    ).execute()

    # 미처리 행만 다시 작성
    if not df_pending.empty:
        def _fmt(v):
            if v is None or (isinstance(v, float) and v != v):
                return ''
            return str(v) if not isinstance(v, (int, float, bool)) else v

        buf_rows = [
            [_fmt(r.get(c, '')) for c in BUFFER_COLUMNS]
            for _, r in df_pending.iterrows()
        ]
        _svc.spreadsheets().values().update(
            spreadsheetId=_sid,
            range=f"{_sname}!A2",
            valueInputOption='RAW',
            body={'values': buf_rows},
        ).execute()
        logger.info(f"[process_buffer] gsheet2 미처리 행 {len(df_pending)}건 유지")
    else:
        logger.info("[process_buffer] gsheet2 모든 행 처리 완료 (빈 상태)")

    return f"처리 완료: 조치 {len(df_ready_action)}건 → gsheet3 / 미조치 {len(df_ready) - len(df_ready_action)}건 제거 / 대기 중: {len(df_pending)}건"


# ============================================================
# task 4 – 리포트 GSheet 데이터 삭제 (헤더만 남김)
# ============================================================
def clear_report_data(**context):
    """
    1행(헤더) 제외, 2행부터 값만 지움 (Delete 키와 동일).
    행 자체는 삭제하지 않으므로 드롭다운/서식 유지.
    """
    import re
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
    service = build('sheets', 'v4', credentials=creds)

    spreadsheet_id = re.search(r'/d/([a-zA-Z0-9_-]+)', REPORT_GSHEET_URL).group(1)

    # 2행부터 끝까지 값만 지우기 (행 삭제 X → 드롭다운/서식 유지)
    clear_range = f"{REPORT_SHEET_NAME}!A2:ZZ"
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=clear_range,
        body={}
    ).execute()

    logger.info(f"[clear] ✅ 리포트 시트 값 지우기 완료 (1행 헤더 유지, 드롭다운/서식 보존)")
    return "리포트 시트 클리어 완료"


# ============================================================
# DAG 정의
# ============================================================
with DAG(
    dag_id=filename.replace('.py', ''),
    description='SMD_07 grp → 버퍼(7일 대기) → gsheet3 누적 append',
    schedule=SMD_ORDERS_TIME,
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=['report', 'gsheet', '리포트'],
) as dag:

    wait_for_smd_07 = ExternalTaskSensor(
        task_id='wait_for_smd_07',
        external_dag_id='Sales_Orders_07_Alert_Dags',
        external_task_id='send_alert_email',
        # execution_date_fn 미사용: 현재 DAG run과 같은 logical date의
        # SMD_07.send_alert_email 성공만 대기한다.
        allowed_states=['success'],
        failed_states=['failed'],
        mode='reschedule',
        timeout=3600,
        poke_interval=60,
        check_existence=True,
        soft_fail=False,  # SMD_07 미완료/미존재면 실패 처리
    )

    # ── Phase 1: gsheet1 조치일자 입력분 → gsheet2(버퍼) 수집 ─────────────
    collect_to_buffer_task = PythonOperator(
        task_id='collect_to_buffer',
        python_callable=collect_to_buffer,
        trigger_rule='all_success',
    )

    # ── Phase 2: gsheet2(버퍼) 7일 경과 행 → orders 조인 → gsheet3 적재 → gsheet2 제거
    process_buffer_task = PythonOperator(
        task_id='process_buffer_to_accum',
        python_callable=process_buffer_to_accum,
        trigger_rule='all_success',
    )

    # ── Phase 3: gsheet1 클리어 → 새 grp 업로드 ─────────────────────────
    clear_report_task = PythonOperator(
        task_id='clear_report_data',
        python_callable=clear_report_data,
        trigger_rule='none_failed',  # process_buffer skip돼도 실행, 실패 시 실행 안함
    )

    upload_grp_task = PythonOperator(
        task_id='upload_grp_template',
        python_callable=upload_grp_template,
        trigger_rule='none_failed',
    )

    copy_lm_task = PythonOperator(
        task_id='copy_sheet2_lm_to_sheet1',
        python_callable=copy_sheet2_lm_to_sheet1,
        trigger_rule='none_failed',
    )

    # 체인: 센서 → 버퍼 수집 → (7일 경과 처리 → gsheet3 적재) → gsheet1 클리어 → 새 grp 업로드 → 시트2 L:M 복사
    wait_for_smd_07 >> collect_to_buffer_task >> process_buffer_task >> clear_report_task >> upload_grp_task >> copy_lm_task
