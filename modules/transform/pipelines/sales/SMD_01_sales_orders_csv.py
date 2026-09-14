# SMD_01_sales_orders_csv.py
import os
import pandas as pd
from pathlib import Path
from typing import Optional
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from modules.transform.utility.io import read_csv_glob
from modules.transform.utility.paths import COLLECT_DB, ONEDRIVE_DB, LOCAL_DB, TEMP_DIR
from modules.transform.utility.io import load_data, send_email, text_to_html, create_sub_order_id_simple, resolve_mail_recipients
from modules.load.load_local_db import local_db_save
from modules.load.backup_to_onedrive import backup_to_onedrive
from modules.transform.utility.io import load_files, preprocess_df, save_to_csv, join_dataframes, validate_and_alert_settlement
from modules.transform.utility.store_name_mapping import normalize_store_names
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM, apply_manager_mail_variables

from modules.transform.utility.io import (
    load_files, 
    preprocess_df, 
    save_to_csv, 
    join_dataframes,
    validate_and_alert_settlement  # ⭐⭐⭐ 추가!
)

# ============================================================
# 🔧 이메일 발송 모드 설정
# ============================================================
TEST_MODE = False  # True: 테스트용(MAIL_CMJ_PM), False: 실전용(직원 이메일)

# 실전 모드에서도 개발자 참조 메일을 받을지 여부
DEV_CC_IN_PROD = True

# ============================================================
# 이메일 수신자 결정 헬퍼 함수
# ============================================================
def get_recipients(manager_email=None):
    """
    테스트/실전 모드에 따라 이메일 수신자 결정
    
    Args:
        manager_email: 담당자 실제 이메일 (실전 모드에서 사용)
    
    Returns:
        list: 이메일 수신자 리스트
    """
    if TEST_MODE:
        # 테스트 모드: 개발자 메일만
        return resolve_mail_recipients(MAIL_CMJ_PM)
    else:
        # 실전 모드: 담당자 실제 이메일 + (옵션) 개발자 참조
        if DEV_CC_IN_PROD:
            return resolve_mail_recipients(manager_email, MAIL_CMJ_PM)
        return resolve_mail_recipients(manager_email)


# ============================================================
# 🔧 배민 주문 데이터 로드 함수
# ============================================================
def load_baemin_data(**context):
    """배민 주문 데이터 로드 (파일명 포함)"""
    return load_files(
        patterns=['baemin_orders*.csv'],
        search_paths=[COLLECT_DB / "영업관리부_수집"],
        xcom_key='baemin_parquet_path',
        dedup_key=['store_name', '주문번호', '주문시각','주문옵션상세'],
        file_type='auto',
        **context
    )


# ============================================================
# 🔧 쿠팡 주문 데이터 로드 함수
# ============================================================
def load_coupang_data(**context):
    """쿠팡 주문 데이터 로드 (파일명 포함)"""
    return load_files(
        patterns=['coupangeats_orders*.csv'],
        search_paths=[COLLECT_DB / "영업관리부_수집"],
        xcom_key='coupang_parquet_path',
        dedup_key=['store_name', 'order_id', 'order_date', 'menu_name', 'menu_options'],
        file_type='auto',
        **context
    )


# 배민 전처리
def preprocess_load_baemin_data(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """배민 전처리 - 날짜 파싱 + 정산금액 검증"""
    import numpy as np
    import re
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(
        task_ids=input_task_id,
        key=input_xcom_key
    )

    if not parquet_path:
        print(f"[경고] 입력 Parquet 경로 없음: task_id={input_task_id}, key={input_xcom_key}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 없음)"
    
    baemin_orders = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(baemin_orders):,}행")
    
    # ⭐⭐⭐ 검증: 결제금액은 있는데 정산금액 없는 경우 ⭐⭐⭐
    # ⚠️ 정산누락 알림 비활성화 (사용자 요청)
    # validate_and_alert_settlement(
    #     df=baemin_orders,
    #     platform='배민',
    #     order_col='주문번호',
    #     payment_col='총결제금액',
    #     settlement_col_candidates=['입금예정금액', '정산금액'],
    #     store_col='store_name',
    #     source_file_col='_source_file',
    #     recipients=get_recipients(),  # 테스트/실전 모드에 따라 수신자 결정
    #     smtp_conn_id='doridang_conn_smtp_gmail',
    #     **context
    # )
    
    # 플랫폼 구분 추가
    baemin_orders['platform'] = '배민'

    # 불필요한 컬럼 제거
    baemin_orders = baemin_orders.drop(columns=['결제타입', '결제금액', '즉시할인_배민지원',
                                                '배민부담_쿠폰할인', '주문중개', '고객할인비용'
                                                ], errors='ignore')

    # 컬럼명을 영문으로 변환
    baemin_orders.rename(columns={'주문상태':'order_status',
                                '주문번호' : 'order_id',
                                '주문시각' : 'order_date',
                                '광고상품' : 'ad_product',
                                '캠페인ID': 'ad_id',
                                '주문내역' : 'order_summary',
                                '주문수량' : 'option_qty',
                                '수령방법' : 'delivery_type',
                                '총결제금액' : 'total_amount',
                                '상품금액' : 'menu_amount',
                                '즉시할인' : 'instant_discount_coupon',
                                '부가세' : 'fee_vat',
                                '만나서결제금액' : 'fee_meet_pay_amount',
                                '그외' : 'fee_etc',
                                '배달' : 'fee_delivery',
                                '주문옵션금액' : 'menu_option_price',
                                '주문옵션상세' :'menu_option_name',
                                "즉시할인_파트너부담" : "fee_ad"
                                }, inplace=True)
    
    # ⭐ settlement_amount 처리 (정산금액 또는 입금예정금액)
    if '정산금액' in baemin_orders.columns:
        baemin_orders['settlement_amount'] = baemin_orders['정산금액']
        print(f"[배민] '정산금액' → settlement_amount 변환 완료")
    elif '입금예정금액' in baemin_orders.columns:
        baemin_orders['settlement_amount'] = baemin_orders['입금예정금액']
        print(f"[배민] '입금예정금액' → settlement_amount 변환 완료")
    else:
        baemin_orders['settlement_amount'] = np.nan
        print(f"[경고] 배민 데이터에 정산금액/입금예정금액 컬럼 없음")

    baemin_orders['store_id'] = np.nan

    # 쿠폰 사용 여부
    baemin_orders['instant_discount_coupon_YN'] = np.where(
        baemin_orders.groupby('order_id')["instant_discount_coupon"].transform('sum') > 0, 
        'Y', 'N'
    )
    
    # ⭐ 필요한 컬럼 선택 (required + optional 통합 + _source_file 추가)
    all_required_cols = ['platform','order_date','store_id','store_name',
        'ad_id', 'ad_product', 'order_id','order_summary','menu_option_name', 'delivery_type',
        'total_amount', 'menu_amount', 'menu_option_price', 'instant_discount_coupon', 
        'instant_discount_coupon_YN', 'option_qty', 'fee_ad', 'settlement_amount',
        'collected_at', '_row_hash', '_source_file']  # ⭐ _source_file 추가
    
    available_cols = [col for col in all_required_cols if col in baemin_orders.columns]
    baemin_orders = baemin_orders[available_cols]
    
    if 'menu_option_price' in baemin_orders.columns:
        baemin_orders['menu_option_price'] = pd.to_numeric(
            baemin_orders['menu_option_price'], errors='coerce'
        )
    else:
        baemin_orders['menu_option_price'] = np.nan
    
    baemin_orders = baemin_orders.rename(columns={'instant_discount_amount':'instant_discount_coupon'})

    # ============================================================
    # ⭐ 날짜 형식 변환 개선
    # ============================================================
    print(f"[DEBUG] order_date 원본 샘플: {baemin_orders['order_date'].iloc[0] if len(baemin_orders) > 0 else 'N/A'}")
    
    # 벡터화된 날짜 변환 함수
    def parse_baemin_date(date_str):
        """배민 날짜 파싱: 2026. 01. 11. (일) 오후 11:36:17"""
        if pd.isna(date_str):
            return pd.NaT
        
        date_str = str(date_str).strip()
        
        # 요일 제거: (월), (화), (수), (목), (금), (토), (일)
        date_str = re.sub(r'\([월화수목금토일]\)', '', date_str)
        
        # 오전/오후 → AM/PM
        date_str = date_str.replace('오전', 'AM').replace('오후', 'PM')
        
        # 공백 정리
        date_str = ' '.join(date_str.split())
        
        # 파싱 시도
        try:
            # 형식: "2026. 01. 11.  PM 11:36:17"
            return pd.to_datetime(date_str, format='%Y. %m. %d. %p %I:%M:%S')
        except:
            try:
                # 공백 하나인 경우: "2026. 01. 11. PM 11:36:17"
                return pd.to_datetime(date_str, format='%Y. %m. %d. %p %I:%M:%S')
            except:
                # 일반 파싱 시도
                return pd.to_datetime(date_str, errors='coerce')
    
    # 날짜 변환 적용
    baemin_orders['order_date'] = baemin_orders['order_date'].apply(parse_baemin_date)
    
    # 변환 결과 확인 (안전 처리)
    nat_count = baemin_orders['order_date'].isna().sum() if 'order_date' in baemin_orders.columns else 0
    if len(baemin_orders) == 0:
        print("[경고] 배민 데이터 없음: 필터/변환 후 0건")
    elif nat_count > 0:
        print(f"[경고] 배민 order_date 변환 실패: {nat_count}건")
        failed_dates = baemin_orders[baemin_orders['order_date'].isna()]['order_date'].head(3)
        print(f"[경고] 실패한 샘플: {failed_dates.tolist()}")
    else:
        print(f"[DEBUG] order_date 변환 성공 샘플: {baemin_orders['order_date'].head(3).tolist()}")

    # collected_at 변환 (안전 처리)
    if 'collected_at' in baemin_orders.columns:
        baemin_orders['collected_at'] = pd.to_datetime(baemin_orders['collected_at'], errors='coerce')
    
    print(f"배민 전처리 완료: {len(baemin_orders)}건")
    
    # Parquet로 저장
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    baemin_orders.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"전처리: {len(baemin_orders):,}행"


def preprocess_load_coupang_data(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """쿠팡이츠 전처리 - 날짜 파싱 + 정산금액 검증"""
    import numpy as np
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(
        task_ids=input_task_id,
        key=input_xcom_key
    )

    if not parquet_path:
        print(f"[경고] 입력 Parquet 경로 없음: task_id={input_task_id}, key={input_xcom_key}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 없음)"
    
    coupang_orders = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(coupang_orders):,}행")
    
    # ⭐⭐⭐ 취소건 제외 (여기에 추가!) ⭐⭐⭐
    if 'is_cancelled' in coupang_orders.columns:
        before = len(coupang_orders)
            
        # 취소건 필터링 (is_cancelled가 'Y' 또는 True인 경우 제외)
        coupang_orders = coupang_orders[
            (coupang_orders['is_cancelled'] != 'Y') & 
            (coupang_orders['is_cancelled'] != True)
        ].copy()
        
        after = len(coupang_orders)
        removed = before - after
        
        if removed > 0:
            print(f"\n[취소건 제외] {removed:,}건 제거됨 ({before:,}건 → {after:,}건)")
        else:
            print(f"\n[취소건 제외] 취소건 없음 ({before:,}건 유지)")
    else:
        print(f"\n[취소건 제외] is_cancelled 컬럼 없음 - 모든 데이터 유지")
    
    print(f"쿠팡이츠 전처리 시작: {len(coupang_orders)}건")
    coupang_df = coupang_orders.copy()


    # order_status 필터 제거 - 모든 데이터 포함
    print(f"[INFO] 모든 주문 데이터 포함: {len(coupang_df)}건")

    # 주요 금액 컬럼 결측치 0 처리
    cols_to_zero = [
        "매출액", "상점부담_쿠폰", "중개_이용료", "결제대행사_수수료",
        "배달비", "광고비", "부가세", "즉시할인금액",
    ]
    for col in cols_to_zero:
        if col in coupang_df.columns:
            coupang_df[col] = coupang_df[col].fillna(0)
            
    # 정산 예정 금액 계산
    cond_amt = (
        coupang_df["매출액"]
        - coupang_df["상점부담_쿠폰"]
        - coupang_df["중개_이용료"]
        - coupang_df["결제대행사_수수료"]
        - coupang_df["배달비"]
        - coupang_df["광고비"]
        - coupang_df["부가세"]
        - coupang_df["즉시할인금액"]
    )

    cond_amt = cond_amt.fillna(0).astype(int)
    coupang_df["정산_예정_금액"] = coupang_df["정산_예정_금액"].fillna(cond_amt).astype(int)
    
    # 플랫폼 구분
    coupang_df['platform'] = '쿠팡'

    # 광고 관련 컬럼
    coupang_df['ad_id'] = np.nan
    coupang_df['ad_product'] = np.nan

    # order_date 대체 컬럼 매핑
    if 'order_date' not in coupang_df.columns:
        for cand in ['주문일시', '주문일자', 'order_datetime', 'order_time']:
            if cand in coupang_df.columns:
                coupang_df['order_date'] = coupang_df[cand]
                print(f"[INFO] order_date 대체: {cand} 사용")
                break
        if 'order_date' not in coupang_df.columns:
            print("[경고] order_date 컬럼 없음")

    # 컬럼 선택 (⭐ _source_file 추가)
    select_cols = ['platform', 'order_date', 'store_id', 'store_name', 'ad_id', 'ad_product',
        'order_id','order_summary', 'menu_name', 'menu_qty','delivery_type',
        'menu_price', '매출액', '광고비','상점부담_쿠폰',
        '정산_예정_금액', 'collected_at', '_source_file']  # ⭐ 추가
    
    if '_row_hash' in coupang_df.columns:
        select_cols.append('_row_hash')
    
    coupang_dfs = coupang_df[[col for col in select_cols if col in coupang_df.columns]].copy()

    # 컬럼명 변환
    coupang_dfs = coupang_dfs.rename(columns={'menu_name':'menu_option_name',
                                'menu_qty':'option_qty',
                                'menu_price':'total_amount',
                                'menu_options':'menu_option_price',
                                '매출액': 'menu_price',
                                '광고비' : 'fee_ad',
                                '상점부담_쿠폰' : 'instant_discount_coupon',
                                '정산_예정_금액' : 'settlement_amount'
                                })

    # 표준 스키마
    base_cols = ['platform', 'order_date', 'store_id', 'store_name', 'ad_id',
        'ad_product', 'order_id', 'order_summary', 'menu_option_name',
        'option_qty', 'delivery_type', 'total_amount', 'fee_ad',
        'instant_discount_coupon', 'settlement_amount', 'collected_at', '_source_file']  # ⭐ 추가
    
    available_cols = [col for col in base_cols if col in coupang_dfs.columns]
    if '_row_hash' in coupang_dfs.columns:
        available_cols.append('_row_hash')
    
    coupang_dfs = coupang_dfs[available_cols]

    # 메뉴 금액
    coupang_dfs['menu_amount'] = coupang_dfs['total_amount']
    coupang_dfs['menu_option_price'] = np.nan

    # ⭐ 쿠팡 가격 보정: settlement_amount != 0 이고 total_amount >= 14900인 경우 -3,000원
    # 일부 메뉴의 정산금액/매출금액이 잘못 수집되는 이슈 수정
    _total = pd.to_numeric(coupang_dfs['total_amount'], errors='coerce').fillna(0)
    _settlement = pd.to_numeric(coupang_dfs['settlement_amount'], errors='coerce').fillna(0)
    fix_mask = (_settlement != 0) & (_total >= 14900)
    fix_count = int(fix_mask.sum())
    if fix_count > 0:
        coupang_dfs.loc[fix_mask, 'total_amount'] = _total[fix_mask] - 3000
        coupang_dfs.loc[fix_mask, 'menu_amount'] = _total[fix_mask] - 3000
        coupang_dfs.loc[fix_mask, 'settlement_amount'] = _settlement[fix_mask] - 3000
        print(f"[쿠팡 가격보정] {fix_count}건 보정 완료 (total_amount/menu_amount/settlement_amount -3,000원)")
    else:
        print(f"[쿠팡 가격보정] 보정 대상 없음")

    # 쿠폰 사용 여부 (통일된 컬럼명: instant_discount_coupon_YN)
    if 'instant_discount_coupon' in coupang_dfs.columns and 'order_id' in coupang_dfs.columns and len(coupang_dfs) > 0:
        coupang_dfs['instant_discount_coupon_YN'] = np.where(
            coupang_dfs.groupby('order_id')["instant_discount_coupon"].transform('sum') > 0,
            'Y', 'N'
        )
    else:
        coupang_dfs['instant_discount_coupon_YN'] = 'N'

    # 최종 컬럼
    final_cols = ['platform', 'order_date', 'store_id', 'store_name', 'ad_id',
        'ad_product', 'order_id', 'order_summary', 'menu_option_name',
        'option_qty', 'delivery_type', 'total_amount', 'menu_amount',
        'menu_option_price', 'instant_discount_coupon', 'instant_discount_coupon_YN',
        'fee_ad', 'settlement_amount',
        'collected_at', '_source_file']  # ⭐ 추가
    
    if '_row_hash' in coupang_dfs.columns:
        final_cols.append('_row_hash')
    
    coupang_dfs = coupang_dfs[[col for col in final_cols if col in coupang_dfs.columns]]

    # ============================================================
    # ⭐ 날짜 형식 변환 (쿠팡 형식 지원 추가)
    # ============================================================
    # 원본 날짜 샘플 안전 출력
    if 'order_date' in coupang_dfs.columns and len(coupang_dfs) > 0:
        print(f"[DEBUG] order_date 원본 샘플: {coupang_dfs['order_date'].head(3).tolist()}")
    else:
        print("[DEBUG] order_date 원본 샘플: N/A (데이터 없음 또는 컬럼 누락)")
    
    def parse_coupang_date(date_str):
        """쿠팡 날짜 파싱: 2026.01.11 23:06"""
        if pd.isna(date_str):
            return pd.NaT
        
        date_str = str(date_str).strip()
        
        try:
            # 형식1: "2026.01.11 23:06" (점 구분, 공백 없음)
            return pd.to_datetime(date_str, format='%Y.%m.%d %H:%M')
        except:
            try:
                # 형식2: "2026-01-11 23:06" (하이픈 구분)
                return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M')
            except:
                # 일반 파싱
                return pd.to_datetime(date_str, errors='coerce')
    
    coupang_dfs['order_date'] = coupang_dfs['order_date'].apply(parse_coupang_date)
    
    # 변환 결과 확인
    nat_count = coupang_dfs['order_date'].isna().sum() if 'order_date' in coupang_dfs.columns else 0
    if len(coupang_dfs) == 0:
        print("[경고] 쿠팡 데이터 없음: 필터 후 0건")
    elif nat_count > 0:
        print(f"[경고] 쿠팡 order_date 변환 실패: {nat_count}건")
    else:
        # 성공 샘플을 안전하게 출력
        print(f"[DEBUG] order_date 변환 성공 샘플: {coupang_dfs['order_date'].head(3).tolist()}")

    # collected_at 변환 안전 처리
    if 'collected_at' in coupang_dfs.columns:
        coupang_dfs["collected_at"] = pd.to_datetime(coupang_dfs["collected_at"], errors='coerce')
        
    print(f"쿠팡이츠 전처리 완료: {len(coupang_dfs)}건")
    
    # Parquet로 저장
    temp_dir = LOCAL_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    coupang_dfs.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"전처리: {len(coupang_dfs):,}행"


# ... (preprocess_merged_daily_orders, create_sub_order_id_with_hash는 변경 없음) ...

def preprocess_merged_daily_orders(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """병합된 주문 데이터 전처리 + sub_order_id 생성"""
    import numpy as np
    ti = context['task_instance']
    
    # 입력 데이터 로드
    parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)
    if not parquet_path:
        print(f"[경고] 입력 Parquet 경로 없음")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 없음)"
    
    df = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(df):,}행")
    
    # ⭐⭐⭐ 1단계: file_name 생성 (제일 먼저!) ⭐⭐⭐
    if '_source_file' in df.columns:
        df['file_name'] = df['_source_file']
        print(f"[file_name] ✅ _source_file → file_name 복사 완료")
        print(f"[file_name] 샘플: {df['file_name'].head(3).tolist()}")
    else:
        df['file_name'] = 'UNKNOWN'
        print(f"[경고] ❌ _source_file 없음 - file_name을 UNKNOWN으로 설정")
    
    # 디버깅: 실제 컬럼 확인
    print(f"[DEBUG] 현재 컬럼 목록: {list(df.columns)}")
    
    # 실오픈일 확인
    if '실오픈일' in df.columns:
        print(f"[DEBUG] ✅ 실오픈일 존재 - 샘플: {df['실오픈일'].dropna().head(3).tolist()}")
    else:
        print(f"[경고] ❌ 실오픈일 없음 - NaN 추가")
        df['실오픈일'] = np.nan
    
    # 매장명 추출 로직
    if 'stores' not in df.columns:
        if 'store_names' in df.columns:
            print(f"[매장명] store_names 컬럼 사용")
            df["stores"] = df["store_names"]
        elif 'store_name' in df.columns:
            print(f"[매장명] store_name 컬럼에서 추출")
            df["stores"] = df["store_name"].str.split(" ").str[-2:].str.join(" ")
        elif '매장명' in df.columns:
            print(f"[매장명] 매장명 컬럼에서 추출")
            df["stores"] = df["매장명"].str.split(" ").str[-2:].str.join(" ")
        else:
            print("[경고] 매장명 관련 컬럼을 찾을 수 없습니다 - Unknown으로 설정")
            df["stores"] = "Unknown"
    else:
        print(f"[매장명] stores 컬럼 이미 존재")
    
    # sub_order_id 생성
    print("\n[sub_order_id] 생성 중...")
    
    if '_row_hash' in df.columns and df['_row_hash'].notna().any():
        df = create_sub_order_id_with_hash(
            df,
            order_col='order_id',
            hash_col='_row_hash',
            output_col='sub_order_id'
        )
    else:
        df = create_sub_order_id_simple(
            df,
            order_col='order_id',
            output_col='sub_order_id'
        )
    
    print(f"[INFO] 중복 제거는 save_to_csv 단계에서 실행됩니다.")
    
    # ⭐⭐⭐ 2단계: 컬럼 선택 (_source_file 제외, file_name 포함!) ⭐⭐⭐
    df_col_candidates = [
        'platform', 'order_date', 'store_id', 'store_names',
        'ad_id', 'ad_product', 'order_id', 'sub_order_id', 
        'order_summary', 'menu_option_name', 'delivery_type', 
        'total_amount', 'menu_amount', 'menu_option_price',
        'instant_discount_coupon', 'instant_discount_coupon_YN',
        'collected_at', 'option_qty', 'fee_ad', 'settlement_amount',
        '_row_hash',
        '담당자', 'email', '상세주소', '광역', '시군구', '읍면동',
        '실오픈일',
        # '_source_file',  # ⬅️ 제외! (내부 처리용)
        'file_name'        # ⬅️ 최종 CSV용!
    ]
    
    # 디버깅: 담당자/email 확인
    print(f"\n[직원정보 확인]")
    print(f"  - 담당자 컬럼: {'담당자' in df.columns} (값 {(df['담당자'].astype(str).str.strip() != '').sum() if '담당자' in df.columns else 'N/A'}건)")
    print(f"  - email 컬럼: {'email' in df.columns} (값 {(df['email'].astype(str).str.strip() != '').sum() if 'email' in df.columns else 'N/A'}건)")
    print(f"  - file_name 컬럼: {'file_name' in df.columns}")
    
    df_col = [col for col in df_col_candidates if col in df.columns]
    if len(df_col) == 0:
        df_col = list(df.columns)
    
    print(f"\n[컬럼 선택] {len(df_col)}개 선택")
    print(f"  ✅ _source_file 제외됨: {'_source_file' not in df_col}")
    print(f"  ✅ file_name 포함됨: {'file_name' in df_col}")
    
    df = df[df_col]
    
    # ⭐⭐⭐ 3단계: menu_option_name 빈칸 채우기 ⭐⭐⭐
    if 'menu_option_name' in df.columns and 'order_summary' in df.columns:
        before_fill = df['menu_option_name'].isna().sum()
        df['menu_option_name'] = df['menu_option_name'].fillna(df['order_summary'])
        after_fill = df['menu_option_name'].isna().sum()
        filled_count = before_fill - after_fill
        if filled_count > 0:
            print(f"\n[menu_option_name] ✅ {filled_count}건 빈칸 채움 (order_summary로)")
        print(f"[menu_option_name] 샘플: {df['menu_option_name'].head(5).tolist()}")
    
    # 최종 확인
    print(f"\n[최종 확인]")
    if '실오픈일' in df.columns:
        print(f"  ✅ 실오픈일 포함됨")
    else:
        print(f"  ❌ 실오픈일 누락!")
    
    if '담당자' in df.columns:
        mgr_count = (df['담당자'].astype(str).str.strip() != '').sum()
        print(f"  담당자: {mgr_count}건 입력됨")
    else:
        print(f"  ❌ 담당자 누락!")
    
    if 'email' in df.columns:
        email_count = (df['email'].astype(str).str.strip() != '').sum()
        print(f"  email: {email_count}건 입력됨")
    else:
        print(f"  ❌ email 누락!")
    
    if 'file_name' in df.columns:
        print(f"  ✅ file_name 포함됨 ({df['file_name'].nunique()}개 파일)")
    else:
        print(f"  ❌ file_name 누락!")
    
    if '_source_file' in df.columns:
        print(f"  ⚠️ _source_file 아직 남아있음 (제거되어야 함)")
    else:
        print(f"  ✅ _source_file 제거됨")
    
    print(f"전처리 완료: {len(df):,}행")
    
    # Parquet 저장
    temp_dir = LOCAL_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    df.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    return f"전처리: {len(df):,}행"

def create_sub_order_id_with_hash(
    df: pd.DataFrame,
    order_col: str = 'order_id',
    hash_col: str = '_row_hash',
    output_col: str = 'sub_order_id'
) -> pd.DataFrame:
    """
    ⭐ 행 해시를 포함한 sub_order_id 생성
    
    같은 주문의 같은 메뉴 2개(완전 동일 행)도 구분 가능
    
    형식: {order_id}_{순번}_{hash앞4자리}
    예시: B22V00DE2A_1_a1b2, B22V00DE2A_2_c3d4
    """
    df = df.copy()
    
    # 순번 생성
    df['_seq'] = df.groupby(order_col).cumcount() + 1
    
    # 해시 앞 4자리 추출
    if hash_col in df.columns and df[hash_col].notna().any():
        df['_hash_short'] = df[hash_col].astype(str).str[:4]
        df[output_col] = (
            df[order_col].astype(str) + '_' + 
            df['_seq'].astype(str) + '_' +
            df['_hash_short']
        )
    else:
        # 해시 없으면 순번만 사용
        df[output_col] = (
            df[order_col].astype(str) + '_' + 
            df['_seq'].astype(str)
        )
    
    # 임시 컬럼 삭제
    df.drop(columns=['_seq', '_hash_short'], errors='ignore', inplace=True)
    
    print(f"[sub_order_id] 해시 포함 방식으로 생성 완료")
    print(f"  - 원본 데이터: {len(df):,}행")
    print(f"  - 고유 ID: {df[output_col].nunique():,}개")
    
    # 중복 체크
    duplicates = df[df.duplicated(subset=output_col, keep=False)]
    if len(duplicates) > 0:
        print(f"  ⚠️ 경고: {len(duplicates)}개 중복 발견!")
    else:
        print(f"  ✅ 중복 없음")
    
    return df


# 직원 데이터 로드 함수    
def load_employee_data(**context):
    """직원 데이터 로드 (wrapper) - 로컬 DB에서 읽기"""
    return load_files(
        patterns=['sales_employee*.csv'],
        search_paths=[LOCAL_DB / '영업관리부_DB'],
        xcom_key='employee_parquet_path',
        file_type='auto',
        add_source_filename=False,  # ⭐ 직원 데이터는 파일명 불필요
        **context
    )
    

def preprocess_load_employee_data(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """직원 데이터 전처리 - 끝 2단어로 매장명 정규화"""
    import numpy as np
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(
        task_ids=input_task_id,
        key=input_xcom_key
    )
    
    if not parquet_path:
        print(f"[경고] 입력 Parquet 경로 없음: task_id={input_task_id}, key={input_xcom_key}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 없음)"
    
    df = pd.read_parquet(parquet_path)
    print(f"[직원 전처리] 시작: {len(df):,}행, 컬럼: {list(df.columns)}")
    
    if "매장명" in df.columns:
        # ⭐ 끝 2단어 추출 (join 키)
        df["store_names_match"] = (
            df["매장명"].astype(str)
            .str.strip()
            .str.split()
            .str[-2:]
            .str.join(" ")
        )
        print(f"[직원 전처리] store_names_match 샘플: {df['store_names_match'].head(3).tolist()}")
        
        # 플랫폼별 중복 제거 (첫 행만 유지)
        before = len(df)
        df = df.drop_duplicates(subset=["store_names_match"], keep="first")
        removed = before - len(df)
        if removed > 0:
            print(f"[직원 전처리] 중복 제거: {removed}건 → {len(df)}건")
    else:
        print(f"[에러] '매장명' 컬럼 없음. 실제 컬럼: {list(df.columns)}")
        raise KeyError("'매장명' 컬럼을 찾을 수 없습니다.")
    
    # 필요한 컬럼만 유지
    keep_cols = [
        '매장명', '담당자', 'email', '실오픈일', 
        '상세주소', '광역', '시군구', '읍면동', 'store_names_match'
    ]
    available_cols = [c for c in keep_cols if c in df.columns]
    df = df[available_cols].copy()
    
    print(f"[직원 전처리] 최종 컬럼: {list(df.columns)}")
    
    temp_dir = LOCAL_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    df.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"[직원 전처리] 완료: {len(df):,}행"


def preprocess_join_orders_with_stores(
    baemin_task_id,
    baemin_xcom_key,
    coupang_task_id,
    coupang_xcom_key,
    employee_task_id,
    employee_xcom_key,
    output_xcom_key,
    **context
):
    """배민/쿠팡 주문을 합치고 직원 정보와 조인"""
    import numpy as np

    ti = context['task_instance']

    def _pull_parquet(task_id, xcom_key):
        path = ti.xcom_pull(task_ids=task_id, key=xcom_key)
        if not path:
            print(f"[경고] 입력 Parquet 경로 없음: task_id={task_id}, key={xcom_key}")
            return None
        try:
            df = pd.read_parquet(path)
            print(f"[{task_id}] 로드 완료: {len(df):,}행")
            return df
        except Exception as e:
            print(f"[{task_id}] 로드 실패: {e}")
            return None

    baemin_df = _pull_parquet(baemin_task_id, baemin_xcom_key)
    coupang_df = _pull_parquet(coupang_task_id, coupang_xcom_key)

    order_frames = [df for df in [baemin_df, coupang_df] if df is not None and len(df) > 0]
    if not order_frames:
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (주문 데이터 없음)"

    orders_df = pd.concat(order_frames, ignore_index=True)

    if 'platform' in orders_df.columns:
        print(f"📊 주문 Platform 분포: {orders_df['platform'].value_counts().to_dict()}")
    else:
        orders_df['platform'] = 'UNKNOWN'

    # ⭐⭐⭐ 주문 데이터: store_name에서 끝 2단어 추출 → store_names ⭐⭐⭐
    if 'store_name' in orders_df.columns:
        orders_df['store_name'] = normalize_store_names(orders_df['store_name'])
        orders_df['store_names'] = (
            orders_df['store_name'].astype(str)
            .str.strip()
            .str.split()
            .str[-2:]
            .str.join(' ')
        )
        print(f"[주문데이터] store_names (끝 2단어) 샘플: {orders_df['store_names'].head(5).tolist()}")
    else:
        print("[경고] store_name 컬럼 없음")
        orders_df['store_names'] = 'UNKNOWN'

    employee_df = _pull_parquet(employee_task_id, employee_xcom_key)
    
    print(f"\n[DEBUG] employee_df 상태 확인:")
    print(f"  - employee_df is None: {employee_df is None}")
    if employee_df is not None:
        print(f"  - len(employee_df): {len(employee_df)}")
        print(f"  - 컬럼: {list(employee_df.columns) if employee_df is not None else 'N/A'}")
    
    if employee_df is not None and len(employee_df) > 0:
        employee_df = apply_manager_mail_variables(employee_df)
        print(f"\n[직원정보] 로드: {len(employee_df)}건")
        print(f"[직원정보] 컬럼: {list(employee_df.columns)}")
        
        if '매장명' in employee_df.columns:
            employee_df['store_names_match'] = (
                employee_df['매장명'].astype(str)
                .str.strip()
                .str.split()
                .str[-2:]
                .str.join(' ')
            )
            
            print(f"[직원정보] store_names_match (끝 2단어) 샘플: {employee_df['store_names_match'].head(5).tolist()}")
            
            if 'email' not in employee_df.columns:
                employee_df['email'] = np.nan
                print("[경고] 직원정보에 email 컬럼 없음 - 추가됨")
            
            employee_df_dedup = employee_df.drop_duplicates(subset=['store_names_match'], keep='first')
            removed_dup = len(employee_df) - len(employee_df_dedup)
            if removed_dup > 0:
                print(f"[직원정보] 플랫폼별 중복 제거: {removed_dup}건 → {len(employee_df_dedup)}건")
            employee_df = employee_df_dedup
            
            print(f"\n[매칭 전] orders: {len(orders_df):,}건, employee: {len(employee_df):,}건")
            print(f"[디버그] orders_df.store_names 샘플: {orders_df['store_names'].head(3).tolist()}")
            print(f"[디버그] employee_df.store_names_match 샘플: {employee_df['store_names_match'].head(3).tolist()}")
            
            data_cols = ['담당자', 'email']
            if '실오픈일' in employee_df.columns:
                data_cols.append('실오픈일')
            if '상세주소' in employee_df.columns:
                data_cols.append('상세주소')
            if '광역' in employee_df.columns:
                data_cols.append('광역')
            if '시군구' in employee_df.columns:
                data_cols.append('시군구')
            if '읍면동' in employee_df.columns:
                data_cols.append('읍면동')
            
            data_cols = [c for c in data_cols if c in employee_df.columns]
            
            print(f"[조인컬럼] join key: store_names_match → store_names")
            print(f"[조인컬럼] data cols: {data_cols}")
            
            join_df = employee_df[['store_names_match'] + data_cols].copy()
            
            joined_df = orders_df.merge(
                join_df, 
                left_on='store_names',
                right_on='store_names_match',
                how='left'
            )
            
            matched = joined_df['담당자'].notna().sum() if '담당자' in joined_df.columns else 0
            print(f"[매칭 완료] 전체: {len(joined_df):,}건, 담당자 매칭: {matched:,}건 ({matched/len(joined_df)*100:.1f}%)")
            
            if matched > 0:
                sample_matched = joined_df[joined_df['담당자'].notna()][['store_names', '담당자', 'email']].head(3)
                print(f"[디버그] 매칭된 샘플:")
                for idx, row in sample_matched.iterrows():
                    print(f"  {row['store_names']} → 담당자: {row['담당자']}, email: {row['email']}")
            
            if matched < len(joined_df):
                unmatched = joined_df[joined_df['담당자'].isna()]
                unmatched_stores = unmatched['store_names'].unique()
                print(f"[매칭실패] {len(unmatched_stores)}개 매장:")
                for store in list(unmatched_stores)[:10]:
                    print(f"  - '{store}'")
                
                print(f"[디버그] employee_df에 있는 매장명들:")
                for emp_store in employee_df['store_names_match'].unique()[:10]:
                    print(f"  - '{emp_store}'")
                    
            if 'store_names_match' in joined_df.columns:
                joined_df = joined_df.drop(columns=['store_names_match'])
            
            if 'email' not in joined_df.columns:
                joined_df['email'] = np.nan
            
            if '담당자' in joined_df.columns:
                joined_df['담당자'] = joined_df['담당자'].fillna('')
            if 'email' in joined_df.columns:
                joined_df['email'] = joined_df['email'].fillna('')
            
            print(f"[최종] 담당자 입력: {(joined_df['담당자'] != '').sum():,}건, 이메일 입력: {(joined_df['email'] != '').sum():,}건")
            
        else:
            print("[경고] 직원 데이터에 '매장명' 컬럼이 없어 조인 생략")
            joined_df = orders_df.copy()
            joined_df['담당자'] = np.nan
            joined_df['email'] = np.nan
    else:
        print("[경고] 직원 데이터 없음: 조인 없이 진행")
        joined_df = orders_df.copy()
        joined_df['담당자'] = np.nan
        joined_df['email'] = np.nan

    # collected_at 컬럼 정리
    if 'collected_at_x' in joined_df.columns and 'collected_at_y' in joined_df.columns:
        joined_df['collected_at'] = joined_df['collected_at_x'].fillna(joined_df['collected_at_y'])
        joined_df = joined_df.drop(columns=['collected_at_x', 'collected_at_y'])
    elif 'collected_at_x' in joined_df.columns:
        joined_df.rename(columns={'collected_at_x': 'collected_at'}, inplace=True)
    elif 'collected_at_y' in joined_df.columns:
        joined_df.rename(columns={'collected_at_y': 'collected_at'}, inplace=True)

    # ⭐⭐⭐ 메뉴 옵션 분리 (order_summary와 menu_option_name 별도 유지) ⭐⭐⭐
    if 'order_summary' in joined_df.columns and 'menu_option_name' in joined_df.columns:
        import re
        
        def extract_pure_option(row):
            """order_summary와 중복되는 부분을 제거하고 순수 옵션만 추출"""
            summary = str(row['order_summary']).strip() if pd.notna(row['order_summary']) else ''
            option = str(row['menu_option_name']).strip() if pd.notna(row['menu_option_name']) else ''
            
            # ⭐ 둘 다 비어있거나 None이면 빈 문자열
            if (not summary or summary == 'None') and (not option or option == 'None'):
                return ''
            
            # ⭐ option이 비어있으면 summary 반환 (메인 메뉴만 있는 경우)
            if not option or option == 'None':
                return summary  # ⬅️ 수정: 대괄호 제거 안 함
            
            # summary가 비어있으면 option 그대로 반환
            if not summary or summary == 'None':
                return option
            
            # ⭐ option과 summary가 같으면 summary 반환 (중복 방지)
            if option == summary:
                return summary
            
            # "|" 구분자가 있는 경우 처리
            if '|' in option:
                parts = [p.strip() for p in option.split('|')]
                # summary와 일치하는 부분 제거
                pure_options = [p for p in parts if p != summary]
                if pure_options:
                    return ' | '.join(pure_options)
                else:
                    return summary  # 모든 부분이 summary와 일치하면 summary 반환
            
            # 그 외의 경우 원본 반환
            return option
        
        # menu_option_name을 순수 옵션으로 변환
        joined_df['menu_option_name'] = joined_df.apply(extract_pure_option, axis=1)
        
        print(f"[메뉴옵션] order_summary 유지, menu_option_name에서 중복 제거 완료 (대괄호 제거 포함)")
        print(f"[샘플] order_summary: {joined_df['order_summary'].head(3).tolist()}")
        print(f"[샘플] menu_option_name: {joined_df['menu_option_name'].head(3).tolist()}")
    else:
        print(f"[경고] order_summary 또는 menu_option_name 컬럼 없음")

    # ⭐ 임시 컬럼 제거
    temp_cols_to_drop = [c for c in joined_df.columns if c in ['store_names_match']]
    if temp_cols_to_drop:
        joined_df = joined_df.drop(columns=temp_cols_to_drop)
        print(f"[임시컬럼] 제거: {temp_cols_to_drop}")

    # ⭐ 최종 컬럼 순서 (_source_file 추가)
    col = [
        'platform', 'order_date', 'store_id', 'store_names',
        'ad_id', 'ad_product', 'order_id', 'sub_order_id', 
        'order_summary',
        'menu_option_name',
        'delivery_type', 'total_amount', 'menu_amount', 'menu_option_price',
        'instant_discount_coupon', 'instant_discount_coupon_YN',
        'collected_at', 'option_qty', 'fee_ad', 'settlement_amount',
        '매장명', '담당자', 'email', '상세주소',
        '실오픈일',
        '_source_file'  # ⭐ 추가
    ]

    print(f"\n[컬럼 확인] 현재 joined_df 컬럼: {list(joined_df.columns)}")
    print(f"[컬럼 확인] 담당자 값 존재: {(joined_df['담당자'] != '').sum():,}건")
    print(f"[컬럼 확인] email 값 존재: {(joined_df['email'] != '').sum():,}건")
    print(f"[컬럼 확인] _source_file 존재: {'_source_file' in joined_df.columns}")
    
    for c in col:
        if c not in joined_df.columns:
            joined_df[c] = np.nan
            print(f"[컬럼 추가] {c} (NaN)")

    joined_df = joined_df[[c for c in col if c in joined_df.columns]]
    joined_df.rename(columns={'collected_at_x': 'collected_at', 'collected_at_y': 'employee_collected_at'}, inplace=True)

    # ⭐ 디버깅: 실오픈일 확인
    if '실오픈일' in joined_df.columns:
        non_null_count = joined_df['실오픈일'].notna().sum()
        print(f"[DEBUG] 실오픈일 컬럼 존재 - 값 있음: {non_null_count}건")
        if non_null_count > 0:
            print(f"[DEBUG] 실오픈일 샘플: {joined_df['실오픈일'].dropna().head(3).tolist()}")
    else:
        print(f"[경고] 실오픈일 컬럼 없음!")

    temp_dir = LOCAL_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)

    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    joined_df.to_parquet(processed_path, index=False, engine='pyarrow')

    ti.xcom_push(key=output_xcom_key, value=str(processed_path))

    return f"전처리: {len(joined_df):,}행"


def preprocess_load_employee_data_grp(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """직원 데이터 전처리 - 매장명 추출"""
    import numpy as np
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(
        task_ids=input_task_id,
        key=input_xcom_key
    )
    
    df = pd.read_parquet(parquet_path)
    
    temp_dir = LOCAL_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    df.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"전처리: {len(df):,}행"
