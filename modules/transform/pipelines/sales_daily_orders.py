import os
import pandas as pd
from pathlib import Path
from typing import Optional
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from modules.transform.utility.io import read_csv_glob
from modules.transform.utility.paths import COLLECT_DB, ONEDRIVE_DB, LOCAL_DB, TEMP_DIR
from modules.transform.utility.io import load_data, send_email, text_to_html, create_sub_order_id_simple
from modules.load.load_local_db import local_db_save
from modules.load.backup_to_onedrive import backup_to_onedrive

# ============================================================
# 🔧 이메일 발송 모드 설정
# ============================================================
TEST_MODE = True  # True: 테스트용(a17019@kakao.com), False: 실전용(직원 이메일)
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
        return ["a17019@kakao.com"] # "sanbogaja81@kakao.com"
    else:
        # 실전 모드: 담당자 실제 이메일 + (옵션) 개발자 참조
        recipients = []
        if manager_email and pd.notna(manager_email) and manager_email.strip():
            recipients.append(manager_email)
        if DEV_CC_IN_PROD:
            # 개발자 참조 메일 추가
            recipients.extend(["a17019@kakao.com"]) #"sanbogaja81@kakao.com"
        return recipients


# ============================================================
# 🔧 배민 주문 데이터 로드 함수 (수정됨)
# ============================================================
def load_baemin_data(**context):
    """
    배민 주문 데이터 로드 (wrapper)
    
    ⭐ 중복 제거 전략:
    1. collected_at 제외 → 다른 시점 업로드 시 중복 감지 가능
    2. 주문시각 포함 → 같은 주문의 같은 메뉴는 주문시각도 동일
    3. 주문수량 포함 → 수량이 다르면 다른 행으로 인식
    4. 전체 행 해시 → 같은 메뉴 2개 주문(2행)도 보존
    """
    baemin_dir = COLLECT_DB / '영업관리부_수집'
    file_pattern = f"{baemin_dir}/baemin_orders*.csv"
    
    return load_data(
        file_path=file_pattern,
        xcom_key='baemin_parquet_path',
        use_glob=True,
        # ✅ 수정: collected_at 제외, 실제 주문 데이터 기준 중복 제거
        # - 같은 파일 다른 시점 업로드 → 중복 제거됨
        # - 같은 주문 같은 메뉴 2개(2행) → row_hash로 구분
        dedup_key=['store_name', '주문번호', '주문시각', '주문내역', 
                   '주문옵션상세', '주문수량'],
        add_row_hash=True,  # ⭐ 새 옵션: 행 해시 추가
        **context
    )


# ============================================================
# 🔧 쿠팡 주문 데이터 로드 함수 (수정됨)
# ============================================================
def load_coupang_data(**context):
    """
    쿠팡 주문 데이터 로드 (wrapper)
    
    ⭐ 중복 제거 전략:
    1. collected_at 제외 → 다른 시점 업로드 시 중복 감지 가능
    2. order_date 포함 → 같은 주문의 같은 메뉴는 주문시각도 동일
    3. menu_qty 포함 → 수량이 다르면 다른 행으로 인식
    4. 전체 행 해시 → 같은 메뉴 2개 주문(2행)도 보존
    """
    coupang_dir = COLLECT_DB / '영업관리부_수집'
    file_pattern = f"{coupang_dir}/coupangeats_orders*.csv"
    
    return load_data(
        file_path=file_pattern,
        xcom_key='coupang_parquet_path',
        use_glob=True,
        # ✅ 수정: collected_at 제외, 실제 주문 데이터 기준 중복 제거
        dedup_key=['store_name', 'order_id', 'order_date', 'menu_name', 
                   'menu_options', 'menu_qty'],
        add_row_hash=True,  # ⭐ 새 옵션: 행 해시 추가
        **context
    )


# 배민 전처리
def preprocess_load_baemin_data(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """배민 전처리 - 날짜 파싱 개선"""
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
    
    # ⭐ 필요한 컬럼 선택 (required + optional 통합)
    all_required_cols = ['platform','order_date','store_id','store_name',
        'ad_id', 'ad_product', 'order_id','order_summary','menu_option_name', 'delivery_type',
        'total_amount', 'menu_amount', 'menu_option_price', 'instant_discount_coupon', 
        'instant_discount_coupon_YN', 'option_qty', 'fee_ad', 'settlement_amount',
        'collected_at', '_row_hash']
    
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
    
    # 변환 결과 확인
    nat_count = baemin_orders['order_date'].isna().sum()
    if nat_count > 0:
        print(f"[경고] 배민 order_date 변환 실패: {nat_count}건")
        # 실패한 원본 출력
        failed_dates = baemin_orders[baemin_orders['order_date'].isna()]['order_date'].head(3)
        print(f"[경고] 실패한 샘플: {failed_dates.tolist()}")
    else:
        print(f"[DEBUG] order_date 변환 성공: {baemin_orders['order_date'].iloc[0]}")
    
    # collected_at 변환
    baemin_orders['collected_at'] = pd.to_datetime(baemin_orders['collected_at'])
    
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
    """쿠팡이츠 전처리 - 날짜 파싱 개선"""
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
    
    print(f"쿠팡이츠 전처리 시작: {len(coupang_orders)}건")
    
    coupang_df = coupang_orders.copy()

    # 취소되지 않은 주문만
    coupang_df = coupang_df[coupang_df["order_status"].isin(["정산예정", "정산완료"])]

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

    # 컬럼 선택
    select_cols = ['platform', 'order_date', 'store_id', 'store_name', 'ad_id', 'ad_product',
        'order_id','order_summary', 'menu_name', 'menu_qty','delivery_type',
        'menu_price', '매출액', '광고비','상점부담_쿠폰',
        '정산_예정_금액', 'collected_at']
    
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
        'instant_discount_coupon', 'settlement_amount', 'collected_at']
    
    available_cols = [col for col in base_cols if col in coupang_dfs.columns]
    if '_row_hash' in coupang_dfs.columns:
        available_cols.append('_row_hash')
    
    coupang_dfs = coupang_dfs[available_cols]

    # 메뉴 금액
    coupang_dfs['menu_amount'] = coupang_dfs['total_amount']
    coupang_dfs['menu_option_price'] = np.nan
    
    # 쿠폰 사용 여부
    coupang_dfs['instant_discount_amount_YN'] = np.where(
        coupang_dfs.groupby('order_id')["instant_discount_coupon"].transform('sum') > 0, 
        'Y', 'N'
    )

    # 최종 컬럼
    final_cols = ['platform', 'order_date', 'store_id', 'store_name', 'ad_id',
        'ad_product', 'order_id', 'order_summary', 'menu_option_name',
        'option_qty', 'delivery_type', 'total_amount', 'menu_amount',
        'menu_option_price', 'instant_discount_coupon',
        'fee_ad', 'settlement_amount',
        'collected_at']
    
    if '_row_hash' in coupang_dfs.columns:
        final_cols.append('_row_hash')
    
    coupang_dfs = coupang_dfs[[col for col in final_cols if col in coupang_dfs.columns]]

    # ============================================================
    # ⭐ 날짜 형식 변환 (쿠팡 형식 지원 추가)
    # ============================================================
    print(f"[DEBUG] order_date 원본 샘플: {coupang_dfs['order_date'].iloc[0] if len(coupang_dfs) > 0 else 'N/A'}")
    
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
    nat_count = coupang_dfs['order_date'].isna().sum()
    if nat_count > 0:
        print(f"[경고] 쿠팡 order_date 변환 실패: {nat_count}건")
    else:
        print(f"[DEBUG] order_date 변환 성공: {coupang_dfs['order_date'].iloc[0]}")
    
    coupang_dfs["collected_at"] = pd.to_datetime(coupang_dfs["collected_at"])
        
    print(f"쿠팡이츠 전처리 완료: {len(coupang_dfs)}건")
    
    # Parquet로 저장
    temp_dir = LOCAL_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    coupang_dfs.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"전처리: {len(coupang_dfs):,}행"


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
    
    # 매장명 추출
    df["stores"] = df["store_name"].str.split(" ").str[-2:].str.join(" ")
    
    # ⭐ sub_order_id 생성 (행 해시 포함 버전)
    print("\n[sub_order_id] 생성 중...")
    
    if '_row_hash' in df.columns and df['_row_hash'].notna().any():
        # _row_hash가 있으면 order_id + 행 내 순번 + 해시 일부
        df = create_sub_order_id_with_hash(
            df,
            order_col='order_id',
            hash_col='_row_hash',
            output_col='sub_order_id'
        )
    else:
        # 기존 방식: order_id + 순번
        df = create_sub_order_id_simple(
            df,
            order_col='order_id',
            output_col='sub_order_id'
        )
    
    print(f"[INFO] 중복 제거는 save_to_csv 단계에서 실행됩니다.")
    
    # 컬럼 선택
    df_col_candidates = [
        'platform', 'order_date', 'store_id', 'store_name', 'stores',
        'ad_id', 'ad_product', 'order_id', 'sub_order_id', 'order_summary',
        'menu_option_name', 'delivery_type', 'total_amount', 'menu_amount',
        'menu_option_price', 'instant_discount_coupon',
        'instant_discount_coupon_YN', 'collected_at', 'option_qty', 'fee_ad',
        'settlement_amount', '_row_hash'
    ]
    
    df_col = [col for col in df_col_candidates if col in df.columns]
    if len(df_col) == 0:
        df_col = list(df.columns)
    
    df = df[df_col]
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
    dir = LOCAL_DB / '영업관리부_DB'
    file_pattern = f"{dir}/sales_employee.csv"
    
    return load_data(
        file_path=file_pattern,
        xcom_key='employee_parquet_path',
        use_glob=False,
        **context
    )


def preprocess_load_employee_data(
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
    
    if not parquet_path:
        print(f"[경고] 입력 Parquet 경로 없음: task_id={input_task_id}, key={input_xcom_key}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 없음)"
    
    df = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(df):,}행")
    
    if "매장명" in df.columns:
        df["store_names"] = df["매장명"].str.split(" ").str[-2:].str.join(" ")
        print(f"매장명 샘플: {df['store_names'].head().tolist()}")
        before = len(df)
        df = df.drop_duplicates(subset=["store_names"], keep="first")
        removed = before - len(df)
        if removed > 0:
            print(f"[INFO] store_names 기준 중복 제거: {removed}건")
    else:
        print(f"[에러] '매장명' 컬럼 없음. 실제 컬럼: {list(df.columns)}")
        raise KeyError("'매장명' 컬럼을 찾을 수 없습니다.")
    
    temp_dir = LOCAL_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    df.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"전처리: {len(df):,}행"


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

    if 'store_names' not in orders_df.columns and 'store_name' in orders_df.columns:
        orders_df['store_names'] = orders_df['store_name'].astype(str).str.split(' ').str[-2:].str.join(' ')

    employee_df = _pull_parquet(employee_task_id, employee_xcom_key)
    if employee_df is not None and len(employee_df) > 0:
        if 'store_names' not in employee_df.columns:
            if '매장명' in employee_df.columns:
                employee_df['store_names'] = employee_df['매장명'].astype(str).str.split(' ').str[-2:].str.join(' ')
            elif 'store_name' in employee_df.columns:
                employee_df['store_names'] = employee_df['store_name'].astype(str)

        if 'store_names' in employee_df.columns:
            employee_df = employee_df.drop_duplicates(subset=['store_names'], keep='first')
            joined_df = orders_df.merge(employee_df, how='left', on='store_names')
        else:
            print("[경고] 직원 데이터에 store_names 컬럼이 없어 조인 생략")
            joined_df = orders_df.copy()
    else:
        print("[경고] 직원 데이터 없음: 조인 없이 진행")
        joined_df = orders_df.copy()

    if 'collected_at' in joined_df.columns and 'collected_at_x' not in joined_df.columns:
        joined_df.rename(columns={'collected_at': 'collected_at_x'}, inplace=True)

    col = [
        'platform', 'order_date', 'store_id', 'store_names',
        'ad_id', 'ad_product', 'order_id', 'sub_order_id', 'order_summary', 'menu_option_name',
        'delivery_type', 'total_amount', 'menu_amount', 'menu_option_price',
        'instant_discount_coupon',
        'collected_at_x', 'option_qty', 'fee_ad', 'settlement_amount',
        '매장명', '담당자', '상세주소',
        '광역', '시군구', '읍면동', 'collected_at_y', 'email', '_row_hash',
        '실오픈일'
    ]

    for c in col:
        if c not in joined_df.columns:
            joined_df[c] = np.nan
            print(f"[INFO] 컬럼 추가: {c} (NaN)")

    joined_df = joined_df[[c for c in col if c in joined_df.columns]]
    joined_df.rename(columns={'collected_at_x': 'collected_at', 'collected_at_y': 'employee_collected_at'}, inplace=True)

    if 'platform' in joined_df.columns:
        print(f"📊 저장 전 Platform 분포: {joined_df['platform'].value_counts().to_dict()}")

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


# ============================================================
# 📧 수집 현황 이메일 (날짜 파싱 개선 버전)
# ============================================================
def send_completion_email(except_employee=None, **context):
    """
    담당자별 수집 현황 이메일
    
    ⭐ 수정사항:
    1. 수집일자(DAG 실행일)와 수집기준일(어제) 명확히 구분
    2. order_date 파싱 개선 (배민/쿠팡 형식 모두 지원)
    3. 매장명 매칭 개선
    """
    from datetime import datetime, timedelta
    import numpy as np
    import pandas as pd
    import re
    from modules.transform.utility.paths import LOCAL_DB
    from modules.transform.utility.io import send_email
    
    if except_employee is None:
        except_employee = []
    
    ti = context['task_instance']
    
    # ============================================================
    # ⭐ 날짜 정의 (명확히 구분)
    # ============================================================
    # 수집일자 = DAG 실행일
    collection_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
    
    # 수집기준일 = 어제 (주문 데이터 기준일)
    target_date = (pd.to_datetime(collection_date) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"[이메일] 수집일자 (DAG 실행일): {collection_date}")
    print(f"[이메일] 수집기준일 (주문 날짜): {target_date}")
    
    # 1. 저장된 CSV 데이터 로드
    csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders.csv'
    
    if not csv_path.exists():
        print(f"[경고] 주문 CSV 없음: {csv_path}")
        html = f"""<html>
<head><meta charset="UTF-8"></head>
<body style="font-family: 'Malgun Gothic', Arial, sans-serif; margin: 20px;">
<h2>📊 주문 데이터 수집 완료</h2>
<p>수집일자: {collection_date}</p>
<p>수집기준일: {target_date}</p>
<p style="color: orange;">⚠️ 상세 데이터 없음</p>
</body>
</html>"""
        return send_email(
            subject=f'[도리당] 주문 데이터 수집 현황 ({collection_date})',
            html_content=html,
            to_emails=["a17019@kakao.com"],
            conn_id='doridang_conn_smtp_gmail',
            **context
        )
    
    # 2. CSV 로드
    try:
        for encoding in ['utf-8-sig', 'utf-8', 'cp949']:
            try:
                orders_df = pd.read_csv(csv_path, encoding=encoding, low_memory=False)
                print(f"[집계] CSV 로드 성공: {len(orders_df):,}건 ({encoding})")
                break
            except UnicodeDecodeError:
                continue
    except Exception as e:
        print(f"[오류] CSV 로드 실패: {e}")
        return "CSV 로드 실패"
    
    print(f"[집계] 전체 데이터: {len(orders_df):,}건")
    
    # ============================================================
    # ⭐ order_date 파싱 개선 (배민/쿠팡 형식 모두 지원)
    # ============================================================
    print(f"[DEBUG] order_date 샘플 (원본): {orders_df['order_date'].head(3).tolist()}")
    print(f"[DEBUG] order_date 타입: {orders_df['order_date'].dtype}")
    
    # 1단계: 이미 datetime이면 그대로 사용
    if pd.api.types.is_datetime64_any_dtype(orders_df['order_date']):
        print(f"[DEBUG] order_date는 이미 datetime 타입")
    else:
        # 2단계: 표준 datetime 변환 시도
        orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], errors='coerce')
        
        converted_count = orders_df['order_date'].notna().sum()
        failed_count = orders_df['order_date'].isna().sum()
        
        print(f"[DEBUG] 변환 성공: {converted_count:,}건 / 실패: {failed_count:,}건")
        
        # 3단계: 실패한 경우 수동 파싱 (배민/쿠팡 형식)
        if failed_count > 0:
            print(f"[경고] 변환 실패 {failed_count}건 - 수동 파싱 시도")
            
            def parse_mixed_date(date_str):
                """배민/쿠팡 날짜 혼합 파싱 (최대한 유연한 처리)"""
                if pd.isna(date_str):
                    return pd.NaT
                
                date_str = str(date_str).strip()
                
                # 1. 배민 형식: "2026. 01. 11. (일) 오후 11:36:17"
                if '오전' in date_str or '오후' in date_str:
                    date_str = re.sub(r'\([월화수목금토일]\)', '', date_str)
                    date_str = date_str.replace('오전', 'AM').replace('오후', 'PM')
                    date_str = ' '.join(date_str.split())
                    # 여러 포맷 시도
                    formats = [
                        '%Y. %m. %d. %p %I:%M:%S',
                        '%Y. %m. %d.  %p %I:%M:%S',
                        '%Y. %m. %d. %p %H:%M:%S'
                    ]
                    for fmt in formats:
                        try:
                            return pd.to_datetime(date_str, format=fmt)
                        except:
                            continue
                    return pd.NaT
                
                # 2. 쿠팡 형식 변형: "2025-08-31 20:14" (공백 + 시간:분)
                if '-' in date_str and ' ' in date_str:
                    try:
                        return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M')
                    except:
                        pass
                
                # 3. 쿠팡 형식: "2026.01.11 23:06" (점 + 공백 + 시간:분)
                if '.' in date_str and ' ' in date_str:
                    try:
                        return pd.to_datetime(date_str, format='%Y.%m.%d %H:%M')
                    except:
                        pass
                
                # 4. 쿠팡 형식 초 포함: "2026.01.11 23:06:30"
                if '.' in date_str and ':' in date_str:
                    try:
                        return pd.to_datetime(date_str, format='%Y.%m.%d %H:%M:%S')
                    except:
                        pass
                
                # 5. 일반 파싱 (최후의 수단)
                return pd.to_datetime(date_str, errors='coerce')
            
            # 실패한 행만 재파싱
            failed_mask = orders_df['order_date'].isna()
            if failed_mask.sum() > 0:
                # 원본 order_date 다시 읽기
                try:
                    original_dates = pd.read_csv(csv_path, usecols=['order_date'], encoding='utf-8-sig', dtype=str, low_memory=False)['order_date']
                    orders_df.loc[failed_mask, 'order_date'] = original_dates[failed_mask].apply(parse_mixed_date)
                    
                    final_failed = orders_df['order_date'].isna().sum()
                    print(f"[DEBUG] 재파싱 후 실패: {final_failed}건")
                    if final_failed > 0:
                        # 실패한 샘플 출력 (디버깅)
                        sample_failed = orders_df.loc[orders_df['order_date'].isna(), 'order_date'].head(5).tolist()
                        print(f"[DEBUG] 재파싱 실패 샘플: {sample_failed}")
                except Exception as e:
                    print(f"[경고] 재파싱 실패: {e}")
    
    # ⭐ 수집기준일 (어제) 필터링
    orders_df['order_date_str'] = orders_df['order_date'].dt.strftime('%Y-%m-%d')
    orders_df = orders_df[orders_df['order_date_str'] == target_date].copy()
    
    print(f"[이메일] 필터링 후 데이터: {len(orders_df):,}건 (기준일: {target_date})")
    
    # 주문 날짜 범위 확인
    if len(orders_df) > 0:
        order_date_min = orders_df['order_date'].min().strftime('%Y-%m-%d')
        order_date_max = orders_df['order_date'].max().strftime('%Y-%m-%d')
        order_date_range = f"{order_date_min} ~ {order_date_max}"
    else:
        order_date_range = "데이터 없음"
    
    print(f"[이메일] 주문 날짜 범위: {order_date_range}")
    
    # 3. 직원 DB 로드
    employee_path = LOCAL_DB / '영업관리부_DB' / 'sales_employee.csv'
    manager_stores = {}
    
    if employee_path.exists():
        try:
            for encoding in ['utf-8-sig', 'utf-8', 'cp949']:
                try:
                    employee_df = pd.read_csv(employee_path, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            for _, row in employee_df.iterrows():
                manager = row.get('담당자', 'Unknown')
                store = row.get('매장명', 'Unknown')
                email = row.get('email', None)
                
                if manager not in manager_stores:
                    manager_stores[manager] = {'email': email, 'stores': set()}
                manager_stores[manager]['stores'].add(store)
            
            total_stores = len(set(store for mgr in manager_stores.values() for store in mgr['stores']))
            print(f"[이메일] 전체 담당자: {len(manager_stores)}명, 전체 매장: {total_stores}개")
        except Exception as e:
            print(f"[경고] 직원 DB 로드 실패: {e}")
            return f"직원 DB 로드 실패: {e}"
    else:
        print(f"[오류] 직원 DB 파일 없음: {employee_path}")
        return "직원 DB 파일 없음"
    
    # 4. 수집 현황 집계 (매장명 기준)
    collected_by_store_platform = {}
    
    if len(orders_df) > 0:
        for _, row in orders_df.iterrows():
            # ⭐ 매장명 우선 순위: 매장명 > store_name
            store = row.get('매장명', None)
            if pd.isna(store) or not store:
                store = row.get('store_name', None)
            
            platform = row.get('platform', None)
            
            if store and platform and not pd.isna(store) and not pd.isna(platform):
                if store not in collected_by_store_platform:
                    collected_by_store_platform[store] = set()
                collected_by_store_platform[store].add(platform)
    
    print(f"[이메일] 수집된 매장 수: {len(collected_by_store_platform)}개")
    if len(collected_by_store_platform) > 0:
        print(f"[이메일] 수집된 매장 목록 샘플: {list(collected_by_store_platform.keys())[:5]}")
    
    # 담당자별 보고서 생성
    manager_reports = {}
    available_platforms = ['배민', '쿠팡']
    
    for manager, mdata in manager_stores.items():
        manager_email = mdata['email']
        expected_stores = mdata['stores']
        
        total_expected = len(expected_stores) * len(available_platforms)
        
        collected_stores = set()
        collected_count = 0
        
        for store in expected_stores:
            store_has_data = False
            for platform in available_platforms:
                if store in collected_by_store_platform:
                    if platform in collected_by_store_platform[store]:
                        collected_count += 1
                        store_has_data = True
            if store_has_data:
                collected_stores.add(store)
        
        uncollected_by_platform = {}
        
        for store in expected_stores:
            for platform in available_platforms:
                collected = store in collected_by_store_platform and platform in collected_by_store_platform[store]
                if not collected:
                    if platform not in uncollected_by_platform:
                        uncollected_by_platform[platform] = []
                    uncollected_by_platform[platform].append(store)
        
        for platform in uncollected_by_platform:
            uncollected_by_platform[platform] = sorted(set(uncollected_by_platform[platform]))
        
        manager_reports[manager] = {
            'email': manager_email,
            'total_stores': len(expected_stores),
            'collected_stores': len(collected_stores),
            'total_expected': total_expected,
            'collected': collected_count,
            'uncollected': total_expected - collected_count,
            'uncollected_by_platform': uncollected_by_platform
        }
    
    # 5. 전체 통계
    total_all_stores = sum(data['total_stores'] for data in manager_reports.values())
    total_collected_stores = len(collected_by_store_platform)
    total_all_combinations = sum(data['total_expected'] for data in manager_reports.values())
    total_collected_combinations = sum(data['collected'] for data in manager_reports.values())
    
    # 수집률 계산
    collection_rate = total_collected_stores / total_all_stores * 100 if total_all_stores > 0 else 0
    combination_rate = total_collected_combinations / total_all_combinations * 100 if total_all_combinations > 0 else 0
    
    if collection_rate >= 80:
        rate_color = '#27ae60'
        rate_emoji = '✅'
    elif collection_rate >= 50:
        rate_color = '#f39c12'
        rate_emoji = '⚠️'
    else:
        rate_color = '#e74c3c'
        rate_emoji = '🚨'
    
    mode_text = "🧪 테스트" if TEST_MODE else "🚀 실전"
    
    # ============================================================
    # 6. ⭐ 이메일 HTML (수집일자/기준일 명확 표시)
    # ============================================================
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', Arial, sans-serif; background-color: #f5f7fa;">
    
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #f5f7fa;">
        <tr>
            <td align="center" style="padding: 20px;">
                
                <table width="600" cellpadding="0" cellspacing="0" border="0" style="background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.08);">
                    
                    <!-- 헤더 -->
                    <tr>
                        <td style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; text-align: center;">
                            <h1 style="margin: 0 0 8px 0; font-size: 22px; color: #ffffff; font-weight: 600;">📊 주문 데이터 일일 수집 현황</h1>
                            <p style="margin: 0; font-size: 14px; color: rgba(255,255,255,0.9);">
                                <strong>수집일자:</strong> {collection_date} (DAG 실행일)
                            </p>
                            <p style="margin: 4px 0 0 0; font-size: 14px; color: rgba(255,255,255,0.9);">
                                <strong>수집기준일:</strong> {target_date} (주문 날짜)
                            </p>
                            <p style="margin: 8px 0 0 0; font-size: 12px; color: rgba(255,255,255,0.7);">수집 일정: 월 · 수 오전 10:30</p>
                            <p style="margin: 4px 0 0 0; font-size: 12px; color: rgba(255,255,255,0.7);">실제 주문 날짜 범위: {order_date_range}</p>
                        </td>
                    </tr>
                    
                    <!-- 요약 섹션 -->
                    <tr>
                        <td style="padding: 25px 30px; border-bottom: 1px solid #eee;">
                            <h2 style="margin: 0 0 20px 0; font-size: 16px; color: #2c3e50;">📈 전체 요약</h2>
                            
                            <table width="100%" cellpadding="0" cellspacing="10" border="0">
                                <tr>
                                    <td width="50%" style="background: #f8f9fa; border-radius: 10px; padding: 18px; text-align: center;">
                                        <p style="margin: 0 0 6px 0; font-size: 12px; color: #7f8c8d;">전체 매장</p>
                                        <p style="margin: 0; font-size: 28px; font-weight: 700; color: #2c3e50;">{total_all_stores}개</p>
                                    </td>
                                    <td width="50%" style="background: linear-gradient(135deg, rgba(102,126,234,0.1) 0%, rgba(118,75,162,0.1) 100%); border: 2px solid rgba(102,126,234,0.3); border-radius: 10px; padding: 18px; text-align: center;">
                                        <p style="margin: 0 0 6px 0; font-size: 12px; color: #7f8c8d;">{rate_emoji} 수집된 매장</p>
                                        <p style="margin: 0; font-size: 28px; font-weight: 700; color: {rate_color};">{total_collected_stores}개</p>
                                        <p style="margin: 4px 0 0 0; font-size: 11px; color: #95a5a6;">{collection_rate:.1f}% 완료</p>
                                    </td>
                                </tr>
                                <tr>
                                    <td width="50%" style="background: #f8f9fa; border-radius: 10px; padding: 18px; text-align: center;">
                                        <p style="margin: 0 0 6px 0; font-size: 12px; color: #7f8c8d;">전체 조합 (매장×플랫폼)</p>
                                        <p style="margin: 0; font-size: 28px; font-weight: 700; color: #2c3e50;">{total_all_combinations}개</p>
                                    </td>
                                    <td width="50%" style="background: #f8f9fa; border-radius: 10px; padding: 18px; text-align: center;">
                                        <p style="margin: 0 0 6px 0; font-size: 12px; color: #7f8c8d;">수집 완료 조합</p>
                                        <p style="margin: 0; font-size: 28px; font-weight: 700; color: {rate_color};">{total_collected_combinations}개</p>
                                        <p style="margin: 4px 0 0 0; font-size: 11px; color: #95a5a6;">{combination_rate:.1f}% 완료</p>
                                    </td>
                                </tr>
                            </table>
                            
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top: 20px;">
                                <tr>
                                    <td style="background: #e8f4f8; border-radius: 8px; padding: 12px 15px; font-size: 13px; color: #2980b9;">
                                        💡 각 매장마다 배민 + 쿠팡 = 2개 플랫폼 데이터가 필요합니다
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- 담당자별 섹션 -->
                    <tr>
                        <td style="padding: 25px 30px;">
                            <h2 style="margin: 0 0 20px 0; font-size: 16px; color: #2c3e50;">👤 담당자별 현황</h2>
"""
    
    # 담당자별 카드 추가
    for manager in sorted(manager_reports.keys()):
        data = manager_reports[manager]
        manager_email = data['email'] or '-'
        total_stores = data['total_stores']
        collected_stores = data['collected_stores']
        total_expected = data['total_expected']
        collected = data['collected']
        uncollected = data['uncollected']
        
        is_complete = uncollected == 0
        progress_rate = (collected / total_expected * 100) if total_expected > 0 else 0
        
        if progress_rate >= 100:
            progress_color = '#27ae60'
        elif progress_rate >= 50:
            progress_color = '#f39c12'
        else:
            progress_color = '#e74c3c'
        
        status_bg = '#d4edda' if is_complete else '#f8d7da'
        status_color = '#155724' if is_complete else '#721c24'
        status_text = '✅ 완료' if is_complete else f'⚠️ {uncollected}건 미완료'
        
        html += f"""
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border: 1px solid #e0e0e0; border-radius: 10px; margin-bottom: 15px; overflow: hidden;">
                                <tr>
                                    <td style="background: #fafafa; padding: 15px 18px; border-bottom: 1px solid #eee;">
                                        <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td>
                                                    <p style="margin: 0; font-size: 15px; font-weight: 600; color: #2c3e50;">{manager}</p>
                                                    <p style="margin: 4px 0 0 0; font-size: 12px; color: #7f8c8d;">{manager_email}</p>
                                                </td>
                                                <td align="right">
                                                    <span style="display: inline-block; background: {status_bg}; color: {status_color}; padding: 5px 12px; border-radius: 20px; font-size: 12px; font-weight: 600;">{status_text}</span>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                
                                <tr>
                                    <td style="padding: 15px 18px;">
                                        <p style="margin: 0 0 12px 0; font-size: 13px; color: #666;">
                                            📍 담당 매장: <strong>{total_stores}개</strong> &nbsp;&nbsp;
                                            ✅ 수집 매장: <strong style="color: {progress_color};">{collected_stores}개</strong> &nbsp;&nbsp;
                                            📊 조합: <strong>{collected}/{total_expected}</strong>
                                        </p>
                                        
                                        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 15px;">
                                            <tr>
                                                <td style="background: #e9ecef; border-radius: 4px; height: 8px;">
                                                    <table cellpadding="0" cellspacing="0" border="0" style="width: {progress_rate}%; height: 8px;">
                                                        <tr>
                                                            <td style="background: {progress_color}; border-radius: 4px;"></td>
                                                        </tr>
                                                    </table>
                                                </td>
                                            </tr>
                                        </table>
"""
        
        if data['uncollected_by_platform']:
            html += """
                                        <p style="margin: 10px 0 8px 0; font-size: 13px; font-weight: 600; color: #e74c3c;">❌ 미업로드 매장</p>
"""
            for platform in sorted(data['uncollected_by_platform'].keys()):
                stores = data['uncollected_by_platform'][platform]
                stores_text = ', '.join(stores)
            
                html += f"""
                                        <p style="margin: 8px 0; font-size: 12px;">
                                            <strong style="color: #666;">{platform} ({len(stores)}개):</strong><br>
                                            <span style="color: #c53030; line-height: 1.8;">{stores_text}</span>
                                        </p>
"""
        else:
            html += """
                                        <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td style="background: #d4edda; color: #155724; padding: 12px 15px; border-radius: 8px; font-size: 13px;">
                                                    ✅ 모든 플랫폼 데이터가 업로드되었습니다!
                                                </td>
                                            </tr>
                                        </table>
"""
        
        html += """
                                    </td>
                                </tr>
                            </table>
"""
    
    html += f"""
                        </td>
                    </tr>
                    
                    <!-- 푸터 -->
                    <tr>
                        <td style="background: #f8f9fa; padding: 20px 30px; text-align: center; border-top: 1px solid #eee;">
                            <p style="margin: 0 0 4px 0; font-size: 12px; color: #7f8c8d;">이 이메일은 자동 발송되었습니다</p>
                            <p style="margin: 0; font-size: 12px; color: #7f8c8d;">문의: a17019@kakao.com | {mode_text} 모드</p>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
    
</body>
</html>"""
    
    # 7. 수신자 결정 및 발송
    if TEST_MODE:
        recipient_emails = ["a17019@kakao.com"]
        print(f"[이메일] 🧪 테스트 모드: 개발자만 발송")
    else:
        recipient_emails = []
        if DEV_CC_IN_PROD:
            recipient_emails.append("a17019@kakao.com")
        
        for manager, data in manager_reports.items():
            if manager in except_employee:
                continue
            if data['email'] and data['email'] not in recipient_emails:
                recipient_emails.append(data['email'])
        
        print(f"[이메일] 🚀 실전 모드: 담당자 {len(recipient_emails)}명")
    
    print(f"[이메일] 수신자: {recipient_emails}")
    print(f"[이메일] 제외: {except_employee}")
    
    try:
        return send_email(
            subject=f'[도리당] 주문 데이터 수집 현황 ({collection_date})',
            html_content=html,
            to_emails=recipient_emails,
            conn_id='doridang_conn_smtp_gmail',
            **context
        )
    except Exception as e:
        print(f"[이메일] ❌ 발송 실패 (무시하고 진행): {e}")
        return f"이메일 발송 실패: {e}"

# ============================================================
# 📧 매출 이상 알람 이메일 (점수 상세 내역 표시 버전)
# ============================================================
def send_alert_email(**context):
    """
    담당자별 매출 이상 알람 이메일 발송
    
    ⭐ 수정사항:
    1. 수집일자(DAG 실행일)와 알림기준일(어제) 명확히 구분
    2. 각 점수(trend, total, 7d, 4week)의 상세 내역 표시
    3. 새로운 점수 체계 반영 (0점/1점/2점)
    """
    import pandas as pd
    from datetime import datetime, timedelta
    from modules.transform.utility.paths import LOCAL_DB
    from modules.transform.utility.io import send_email
    
    # 이메일 모드 설정
    TEST_MODE = True
    DEV_CC_IN_PROD = True
    
    def get_recipients(manager_email=None):
        if TEST_MODE:
            return ["a17019@kakao.com"]
        else:
            recipients = []
            if manager_email and pd.notna(manager_email) and manager_email.strip():
                recipients.append(manager_email)
            if DEV_CC_IN_PROD:
                recipients.insert(0, "a17019@kakao.com")
            return recipients
    
    ti = context['task_instance']
    
    # ============================================================
    # ⭐ 날짜 정의 (명확히 구분)
    # ============================================================
    # 수집일자 = DAG 실행일
    dag_run_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
    
    # 알림기준일 = 어제 (매출 분석일)
    yesterday = (pd.to_datetime(dag_run_date) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"[알림] 수집일자 (DAG 실행일): {dag_run_date}")
    print(f"[알림] 알림기준일 (매출 분석일): {yesterday}")
    
    # 1. 알람 대상 데이터 로드
    alert_path = ti.xcom_pull(task_ids='filter_alerts', key='alert_targets')
    
    if not alert_path:
        print("[알람] 알람 대상 없음 - 이메일 발송 생략")
        return "알람 대상 없음"
    
    alert_df = pd.read_parquet(alert_path)
    
    if len(alert_df) == 0:
        print("[알람] 알람 대상 0건 - 이메일 발송 생략")
        return "알람 대상 0건"
    
    # 2. 기준일 확인
    target_date = alert_df['order_daily'].max()
    target_date_str = pd.to_datetime(target_date).strftime('%Y-%m-%d')
    
    print(f"[알람] 알람 대상: {len(alert_df):,}건 (기준일: {target_date_str})")
    
    # 3. 담당자별 그룹핑
    managers = alert_df.groupby(['담당자', 'email'])
    
    email_count = 0
    
    for (manager, email), group in managers:
        if not email or pd.isna(email) or email.strip() == '':
            print(f"[알람] {manager}: 이메일 주소 없음 - 건너뜀")
            continue
        
        # 4. 매장별 상세 정보 (점수 상세 컬럼 포함)
        score_cols = ['매장명', 'status', 'score', 'total_amount', 
                      'score_trend', 'score_total', 'score_7d_total', 'score_4week_total',
                      'ma_14', 'ma_28', 'current_avg_2week', 'current_avg_4week',
                      'sum_7d_recent', 'sum_7d_prev']
        
        available_cols = [c for c in score_cols if c in group.columns]
        store_details = group[available_cols].drop_duplicates(subset=['매장명'])
        
        if len(store_details) == 0:
            continue
        
        # 5. 이메일 제목 생성
        first_store = sorted(store_details['매장명'].tolist())[0]
        if len(store_details) > 1:
            subject = f"🚨 [매출이상] {first_store} 외 {len(store_details) - 1}건"
        else:
            subject = f"🚨 [매출이상] {first_store}"
        
        # 통계
        danger_count = len(store_details[store_details['status'] == '위험'])
        warning_count = len(store_details[store_details['status'] == '주의'])
        
        # ============================================================
        # 6. ⭐ 이메일 HTML (날짜 정보 + 점수 상세 내역 포함)
        # ============================================================
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', Arial, sans-serif; background-color: #f5f7fa;">
    
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #f5f7fa;">
        <tr>
            <td align="center" style="padding: 20px;">
                
                <table width="700" cellpadding="0" cellspacing="0" border="0" style="background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.08);">
                    
                    <!-- 헤더 -->
                    <tr>
                        <td style="background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%); padding: 30px; text-align: center;">
                            <h1 style="margin: 0 0 8px 0; font-size: 22px; color: #ffffff; font-weight: 600;">🚨 매출 이상 알림</h1>
                            <p style="margin: 0; font-size: 14px; color: rgba(255,255,255,0.9);">
                                <strong>수집일자:</strong> {dag_run_date} (DAG 실행일)
                            </p>
                            <p style="margin: 4px 0 0 0; font-size: 14px; color: rgba(255,255,255,0.9);">
                                <strong>알림기준일:</strong> {target_date_str} (매출 분석일)
                            </p>
                            <p style="margin: 15px 0 0 0; font-size: 13px; color: rgba(255,255,255,0.85);">
                                어제({target_date_str}) 매출 데이터 기준 알림
                            </p>
                            
                            <table cellpadding="0" cellspacing="0" border="0" align="center" style="margin-top: 15px;">
                                <tr>
                                    <td style="background: rgba(255,255,255,0.2); padding: 8px 16px; border-radius: 20px; font-size: 13px; color: #ffffff;">
                                        🚨 위험 {danger_count}건
                                    </td>
                                    <td width="15"></td>
                                    <td style="background: rgba(255,255,255,0.2); padding: 8px 16px; border-radius: 20px; font-size: 13px; color: #ffffff;">
                                        ⚠️ 주의 {warning_count}건
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- 인사말 -->
                    <tr>
                        <td style="padding: 25px 30px 15px 30px;">
                            <p style="margin: 0; font-size: 15px; color: #2c3e50; line-height: 1.6;">
                                <strong style="color: #e74c3c;">{manager}</strong>님 안녕하세요,<br>
                                담당 매장 중 <strong style="color: #e74c3c;">{len(store_details)}개 매장</strong>이 매출 이상 상태입니다.
                            </p>
                        </td>
                    </tr>
"""
        
        # 매장별 상세 카드 생성
        store_details = store_details.sort_values(['status', 'score'], ascending=[True, False])
        
        for idx, (_, row) in enumerate(store_details.iterrows()):
            is_danger = row['status'] == '위험'
            card_border = '#e74c3c' if is_danger else '#f39c12'
            status_bg = '#e74c3c' if is_danger else '#f39c12'
            status_emoji = '🚨' if is_danger else '⚠️'
            
            score = int(row['score'])
            total_amount = int(row['total_amount']) if pd.notna(row['total_amount']) else 0
            
            # 개별 점수 가져오기
            score_trend = int(row.get('score_trend', 0)) if pd.notna(row.get('score_trend')) else 0
            score_total = int(row.get('score_total', 0)) if pd.notna(row.get('score_total')) else 0
            score_7d = int(row.get('score_7d_total', 0)) if pd.notna(row.get('score_7d_total')) else 0
            score_4week = int(row.get('score_4week_total', 0)) if pd.notna(row.get('score_4week_total')) else 0
            
            # 각 지표 값 가져오기
            ma_14 = row.get('ma_14', None)
            ma_28 = row.get('ma_28', None)
            current_avg_2week = row.get('current_avg_2week', None)
            current_avg_4week = row.get('current_avg_4week', None)
            sum_7d_recent = row.get('sum_7d_recent', None)
            sum_7d_prev = row.get('sum_7d_prev', None)
            
            # 각 지표별 변화율 계산
            def calc_rate(val1, val2):
                if pd.notna(val1) and pd.notna(val2) and val2 > 0:
                    return ((val1 - val2) / val2) * 100
                return None
            
            rate_trend = calc_rate(ma_14, ma_28)
            rate_total = calc_rate(current_avg_2week, ma_14)
            rate_7d = calc_rate(sum_7d_recent, sum_7d_prev)
            rate_4week = calc_rate(current_avg_2week, current_avg_4week)
            
            def format_rate(rate, score_val):
                """변화율 포맷팅 (점수에 따라 색상 변경)"""
                if rate is None:
                    return "-"
                # 점수가 높을수록 빨간색 (하락)
                if score_val == 2:
                    color = '#e74c3c'  # 급격 하락
                elif score_val == 1:
                    color = '#f39c12'  # 소폭 하락
                else:
                    color = '#27ae60'  # 상승/유지
                return f"<span style='color: {color}; font-weight: 600;'>{rate:+.1f}%</span>"
            
            def score_badge(score_val):
                """점수 배지 생성 (새로운 체계)"""
                if score_val == 0:
                    return "<span style='background: #27ae60; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px;'>0점</span>"
                elif score_val == 1:
                    return "<span style='background: #f39c12; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px;'>1점</span>"
                else:  # 2점
                    return "<span style='background: #e74c3c; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px;'>2점</span>"
            
            html += f"""
                    <!-- 매장 카드: {row['매장명']} -->
                    <tr>
                        <td style="padding: 0 30px 20px 30px;">
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border: 2px solid {card_border}; border-radius: 10px; overflow: hidden;">
                                
                                <!-- 매장 헤더 -->
                                <tr>
                                    <td style="background: {card_border}; padding: 12px 15px;">
                                        <table width="100%" cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td>
                                                    <span style="font-size: 16px; font-weight: 600; color: #ffffff;">{row['매장명']}</span>
                                                </td>
                                                <td align="right">
                                                    <span style="background: rgba(255,255,255,0.9); color: {card_border}; padding: 4px 12px; border-radius: 15px; font-size: 13px; font-weight: 700;">
                                                        {status_emoji} 총 {score}점
                                                    </span>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                
                                <!-- 어제 매출 -->
                                <tr>
                                    <td style="padding: 15px; background: #f8f9fa; border-bottom: 1px solid #eee;">
                                        <p style="margin: 0; font-size: 13px; color: #666;">어제({target_date_str}) 매출</p>
                                        <p style="margin: 5px 0 0 0; font-size: 24px; font-weight: 700; color: #2c3e50;">{total_amount:,}원</p>
                                    </td>
                                </tr>
                                
                                <!-- 점수 상세 테이블 -->
                                <tr>
                                    <td style="padding: 15px;">
                                        <p style="margin: 0 0 10px 0; font-size: 13px; font-weight: 600; color: #666;">📊 점수 상세 내역</p>
                                        
                                        <table width="100%" cellpadding="8" cellspacing="0" border="0" style="font-size: 12px;">
                                            <tr style="background: #f0f0f0;">
                                                <th style="text-align: left; padding: 8px; border-radius: 5px 0 0 0;">지표</th>
                                                <th style="text-align: center; padding: 8px;">비교</th>
                                                <th style="text-align: center; padding: 8px;">변화율</th>
                                                <th style="text-align: center; padding: 8px; border-radius: 0 5px 0 0;">점수</th>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>추세</strong><br><span style="color: #888; font-size: 11px;">14일MA vs 28일MA</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{int(ma_14) if pd.notna(ma_14) else '-':,} vs {int(ma_28) if pd.notna(ma_28) else '-':,}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_trend, score_trend)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(score_trend)}</td>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>일일</strong><br><span style="color: #888; font-size: 11px;">2주평균 vs 14일MA</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{int(current_avg_2week) if pd.notna(current_avg_2week) else '-':,} vs {int(ma_14) if pd.notna(ma_14) else '-':,}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_total, score_total)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(score_total)}</td>
                                            </tr>
                                            <tr style="border-bottom: 1px solid #eee;">
                                                <td style="padding: 8px;"><strong>주간</strong><br><span style="color: #888; font-size: 11px;">최근7일 vs 직전7일</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{int(sum_7d_recent) if pd.notna(sum_7d_recent) else '-':,} vs {int(sum_7d_prev) if pd.notna(sum_7d_prev) else '-':,}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_7d, score_7d)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(score_7d)}</td>
                                            </tr>
                                            <tr>
                                                <td style="padding: 8px;"><strong>월간</strong><br><span style="color: #888; font-size: 11px;">2주평균 vs 4주평균</span></td>
                                                <td style="text-align: center; padding: 8px; font-size: 11px;">{int(current_avg_2week) if pd.notna(current_avg_2week) else '-':,} vs {int(current_avg_4week) if pd.notna(current_avg_4week) else '-':,}</td>
                                                <td style="text-align: center; padding: 8px;">{format_rate(rate_4week, score_4week)}</td>
                                                <td style="text-align: center; padding: 8px;">{score_badge(score_4week)}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                
                            </table>
                        </td>
                    </tr>
"""
        
        # 점수 가이드 & 푸터
        html += f"""
                    <!-- 점수 가이드 -->
                    <tr>
                        <td style="padding: 25px 30px; background: #f8f9fa; border-top: 1px solid #eee;">
                            <h3 style="margin: 0 0 15px 0; font-size: 14px; color: #2c3e50;">📊 점수 기준 (새로운 체계)</h3>
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-size: 12px;">
                                <tr>
                                    <td style="padding: 5px 0;">
                                        <span style="background: #27ae60; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px; margin-right: 8px;">0점</span>
                                        상승 또는 유지 (0% 이상)
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 5px 0;">
                                        <span style="background: #f39c12; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px; margin-right: 8px;">1점</span>
                                        소폭 하락 (-10% ~ 0%)
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 5px 0;">
                                        <span style="background: #e74c3c; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px; margin-right: 8px;">2점</span>
                                        급격 하락 (-10% 미만)
                                    </td>
                                </tr>
                            </table>
                            
                            <p style="margin: 15px 0 0 0; font-size: 12px; color: #888;">
                                <strong>알림 기준:</strong>
                            </p>
                            <table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-size: 12px; margin-top: 8px;">
                                <tr>
                                    <td style="padding: 5px 0;">
                                        • <strong style="color: #e74c3c;">위험 (6~8점)</strong>: 즉시 알림
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 5px 0;">
                                        • <strong style="color: #f39c12;">주의 (4~5점)</strong>: 2일 연속 시 알림
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 5px 0;">
                                        • <strong style="color: #27ae60;">정상 (0~3점)</strong>: 알림 없음
                                    </td>
                                </tr>
                            </table>
                            
                            <p style="margin: 15px 0 0 0; font-size: 11px; color: #999;">
                                * 4가지 지표(추세/일일/주간/월간) 합산하여 총점 계산<br>
                                * 각 지표는 독립적으로 0~2점 부여 (총점 0~8점)
                            </p>
                        </td>
                    </tr>
                    
                    <!-- 푸터 -->
                    <tr>
                        <td style="padding: 20px 30px; text-align: center; border-top: 1px solid #eee;">
                            <p style="margin: 0; font-size: 12px; color: #7f8c8d;">이 이메일은 자동 발송되었습니다 | 문의: a17019@kakao.com</p>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
    
</body>
</html>"""
        
        # 7. 이메일 발송
        recipient_emails = get_recipients(email)
        
        mode_text = "🧪 테스트" if TEST_MODE else "🚀 실전"
        print(f"[알람] {mode_text} | {manager} ({email}): {len(store_details)}개 매장 -> {recipient_emails}")
        
        try:
            result = send_email(
                subject=subject,
                html_content=html,
                to_emails=recipient_emails,
                conn_id='doridang_conn_smtp_gmail',
                **context
            )
            email_count += 1
            print(f"[알람] ✅ {manager} 이메일 발송 성공")
        except Exception as e:
            print(f"[알람] ❌ {manager} 이메일 발송 실패: {e}")
    
    return f"알람 이메일 발송 완료: {email_count}명, 기준일: {target_date_str}"


# ============================================================
# 일자별 매출 집계
# ============================================================
def aggregate_daily_sales(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """일자별 매출 집계 (디버깅 강화 버전)"""
    import numpy as np
    from datetime import datetime, timedelta
    ti = context['task_instance']
    
    csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders.csv'
    
    if not csv_path.exists():
        print(f"[경고] CSV 파일 없음: {csv_path}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (CSV 없음)"
    
    # ============================================================
    # ⭐ CSV 로드 (파싱 전략 개선)
    # ============================================================
    print(f"\n{'='*60}")
    print(f"[CSV 로드] 시작: {csv_path.name}")
    
    try:
        # 1차: parse_dates 없이 먼저 읽기 (원본 확인용)
        for encoding in ['utf-8-sig', 'utf-8', 'cp949']:
            try:
                orders_df_raw = pd.read_csv(
                    csv_path, 
                    encoding=encoding, 
                    low_memory=False,
                    nrows=5
                )
                print(f"[샘플 로드] 성공 ({encoding})")
                print(f"[샘플] order_date 원본: {orders_df_raw['order_date'].tolist()}")
                break
            except UnicodeDecodeError:
                continue
        
        # 2차: 전체 로드 (parse_dates 사용)
        for encoding in ['utf-8-sig', 'utf-8', 'cp949']:
            try:
                orders_df = pd.read_csv(
                    csv_path, 
                    encoding=encoding, 
                    low_memory=False,
                    parse_dates=['order_date'],
                    date_format='%Y-%m-%d %H:%M'
                )
                print(f"[전체 로드] 성공: {len(orders_df):,}건 ({encoding})")
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"[경고] parse_dates 실패 ({encoding}): {e}")
                try:
                    orders_df = pd.read_csv(
                        csv_path, 
                        encoding=encoding, 
                        low_memory=False
                    )
                    print(f"[전체 로드] 성공 (parse_dates 없음): {len(orders_df):,}건")
                    break
                except UnicodeDecodeError:
                    continue
    
    except Exception as e:
        print(f"[오류] CSV 로드 실패: {e}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "CSV 로드 실패"
    
    print(f"[로드 완료] 전체 데이터: {len(orders_df):,}건")
    
    # ============================================================
    # ⭐ 날짜 변환 및 검증 (상세 로그)
    # ============================================================
    print(f"\n{'='*60}")
    print(f"[날짜 처리] 시작...")
    
    if 'order_date' not in orders_df.columns:
        raise ValueError("order_date 컬럼이 없습니다!")
    
    print(f"[DEBUG] order_date 타입: {orders_df['order_date'].dtype}")
    print(f"[DEBUG] order_date 샘플 (처음 5개): {orders_df['order_date'].head().tolist()}")
    print(f"[DEBUG] order_date 샘플 (마지막 5개): {orders_df['order_date'].tail().tolist()}")
    
    # datetime이 아니면 변환
    if not pd.api.types.is_datetime64_any_dtype(orders_df['order_date']):
        print(f"[변환] order_date를 datetime으로 변환 중...")
        
        def safe_parse_date(date_str):
            """안전한 날짜 파싱"""
            if pd.isna(date_str):
                return pd.NaT
            
            date_str = str(date_str).strip()
            
            # 형식1: "2025-12-31 20:22" (시간 포함)
            try:
                return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M')
            except:
                pass
            
            # 형식2: "2025-12-31 20:22:00" (초 포함)
            try:
                return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
            except:
                pass
            
            # 형식3: "2025-12-31" (날짜만)
            try:
                return pd.to_datetime(date_str, format='%Y-%m-%d')
            except:
                pass
            
            return pd.to_datetime(date_str, errors='coerce')
        
        orders_df['order_date'] = orders_df['order_date'].apply(safe_parse_date)
        
        nat_count = orders_df['order_date'].isna().sum()
        success_count = orders_df['order_date'].notna().sum()
        
        print(f"[변환 결과] 성공: {success_count:,}건, 실패(NaT): {nat_count:,}건")
        
        if nat_count > 0:
            try:
                raw_df = pd.read_csv(csv_path, usecols=['order_date'], dtype=str, nrows=100000, encoding='utf-8-sig')
                failed_indices = orders_df[orders_df['order_date'].isna()].index[:5]
                print(f"[실패 샘플] 인덱스: {failed_indices.tolist()}")
                print(f"[실패 샘플] 원본 값: {raw_df.loc[failed_indices, 'order_date'].tolist()}")
            except Exception as e:
                print(f"[경고] 실패 샘플 출력 불가: {e}")
    else:
        print(f"[확인] order_date는 이미 datetime 타입")
    
    print(f"[타입 확인] order_date 최종 타입: {orders_df['order_date'].dtype}")
    
    # ============================================================
    # ⭐ NaT 제거 전 통계
    # ============================================================
    print(f"\n{'='*60}")
    print(f"[NaT 제거] 시작...")
    
    before_count = len(orders_df)
    nat_count = orders_df['order_date'].isna().sum()
    
    print(f"[통계] 전체: {before_count:,}건")
    print(f"[통계] 유효: {before_count - nat_count:,}건")
    print(f"[통계] NaT: {nat_count:,}건 ({nat_count/before_count*100:.2f}%)")
    
    if nat_count > 0:
        print(f"[경고] ⚠️ NaT가 {nat_count:,}건 발견됨! 이 데이터는 집계에서 제외됩니다.")
    
    # 유효한 날짜 범위 확인 (NaT 제거 전)
    valid_dates = orders_df[orders_df['order_date'].notna()]['order_date']
    if len(valid_dates) > 0:
        print(f"[유효 날짜 범위] min={valid_dates.min()}, max={valid_dates.max()}")
        
        # ⭐ 날짜별 카운트 (최근 30일)
        date_counts = orders_df[orders_df['order_date'].notna()].groupby(
            orders_df['order_date'].dt.strftime('%Y-%m-%d')
        ).size().sort_index(ascending=False).head(30)
        
        print(f"\n[최근 30일 데이터 분포]:")
        for date, count in date_counts.items():
            print(f"  {date}: {count:,}건")
    
    # NaT 제거
    orders_df = orders_df[orders_df['order_date'].notna()].copy()
    after_count = len(orders_df)
    
    print(f"\n[NaT 제거 완료] {before_count:,}건 → {after_count:,}건 (제거: {before_count - after_count:,}건)")
    
    if after_count == 0:
        print(f"[오류] ❌ 모든 데이터가 NaT!")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (모든 날짜가 NaT)"
    
    # ============================================================
    # ⭐ 전날까지만 필터링
    # ============================================================
    print(f"\n{'='*60}")
    print(f"[날짜 필터링] 시작...")
    
    dag_run_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
    yesterday = (pd.to_datetime(dag_run_date) - timedelta(days=1)).strftime('%Y-%m-%d')
    yesterday_end = pd.to_datetime(yesterday) + timedelta(days=1)
    
    print(f"[기준일] DAG 실행일: {dag_run_date}")
    print(f"[기준일] 집계 대상: {yesterday} (전날까지)")
    print(f"[필터링] 조건: order_date < {yesterday_end}")
    
    # 필터링 전 통계
    before_filter = len(orders_df)
    date_min_before = orders_df['order_date'].min()
    date_max_before = orders_df['order_date'].max()
    
    print(f"[필터링 전] 건수: {before_filter:,}건")
    print(f"[필터링 전] 날짜 범위: {date_min_before} ~ {date_max_before}")
    
    # 필터링 실행
    orders_df = orders_df[orders_df['order_date'] < yesterday_end].copy()
    
    # 필터링 후 통계
    after_filter = len(orders_df)
    
    if after_filter > 0:
        date_min_after = orders_df['order_date'].min()
        date_max_after = orders_df['order_date'].max()
        
        print(f"[필터링 후] 건수: {after_filter:,}건")
        print(f"[필터링 후] 날짜 범위: {date_min_after} ~ {date_max_after}")
        print(f"[필터링 후] 제거: {before_filter - after_filter:,}건")
    else:
        print(f"[오류] ❌ 필터링 후 데이터 없음!")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (필터링 후 데이터 없음)"
    
    # order_daily 컬럼 생성
    orders_df['order_daily'] = orders_df['order_date'].dt.strftime('%Y-%m-%d')
    
    unique_dates = sorted(orders_df['order_daily'].unique())
    print(f"\n[order_daily] 생성 완료: {len(unique_dates)}개 날짜")
    print(f"[order_daily] 전체 날짜 목록 (최근 15개):")
    for date in unique_dates[-15:]:
        count = len(orders_df[orders_df['order_daily'] == date])
        print(f"  {date}: {count:,}건")
    
    print(f"{'='*60}\n")
    
    if '매장명' in orders_df.columns:
        orders_df['매장명_clean'] = orders_df['매장명']
    else:
        orders_df['매장명_clean'] = orders_df['store_name']
    
    # ============================================================
    # ⭐ 1️⃣ 기본 집계 (플랫폼 합침) - 알람용
    # ============================================================
    print(f"\n{'='*60}")
    print(f"[1단계] 플랫폼 합친 기본 집계...")
    
    daily_agg = orders_df.groupby(['order_daily', '매장명_clean', '담당자', 'email']).agg(
        total_order_count=('order_id', 'nunique'),
        total_amount=('total_amount', 'sum'),
        fee_ad=('fee_ad', 'sum'),
        실오픈일=('실오픈일', 'min'),
        platform=('platform', lambda x: ','.join(sorted(x.dropna().astype(str).unique()))),
        settlement_amount=('settlement_amount', 'sum'),
        # ⭐ order_date 시간 포함 (max로 가장 최근 시간)
        order_date_with_time=('order_date', 'max')
    ).reset_index()
    
    daily_agg.rename(columns={'매장명_clean': '매장명'}, inplace=True)
    daily_agg['ARPU'] = daily_agg['total_amount'] / daily_agg['total_order_count'].replace(0, np.nan)
    daily_agg['order_daily'] = pd.to_datetime(daily_agg['order_daily'])
    
    print(f"[기본 집계] 완료: {len(daily_agg):,}건")
    print(f"[기본 집계] order_daily 범위: min={daily_agg['order_daily'].min()}, max={daily_agg['order_daily'].max()}")
    
    # ============================================================
    # ⭐ 2️⃣ 플랫폼별 집계 → Pivot으로 컬럼 펼치기
    # ============================================================
    print(f"\n{'='*60}")
    print(f"[2단계] 플랫폼별 집계 및 Pivot 변환...")
    
    daily_platform = orders_df.groupby(['order_daily', '매장명_clean', '담당자', 'email', 'platform']).agg(
        total_order_count=('order_id', 'nunique'),
        total_amount=('total_amount', 'sum'),
        fee_ad=('fee_ad', 'sum'),
        settlement_amount=('settlement_amount', 'sum')
    ).reset_index()
    
    daily_platform.rename(columns={'매장명_clean': '매장명'}, inplace=True)
    
    print(f"[플랫폼별 집계] 완료: {len(daily_platform):,}건")
    print(f"[플랫폼 분포] {daily_platform['platform'].value_counts().to_dict()}")
    
    # ⭐ Pivot: 플랫폼을 컬럼으로 펼치기
    metrics_to_pivot = ['total_order_count', 'total_amount', 'fee_ad', 'settlement_amount']
    
    pivoted_dfs = []
    for metric in metrics_to_pivot:
        pivot = daily_platform.pivot_table(
            index=['order_daily', '매장명', '담당자', 'email'],
            columns='platform',
            values=metric,
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        # ⭐ order_daily를 datetime으로 변환 (merge 시 타입 일치)
        pivot['order_daily'] = pd.to_datetime(pivot['order_daily'])
        
        # 컬럼명 변경: platform → metric_platform
        pivot.columns = [f"{metric}_{col}" if col not in ['order_daily', '매장명', '담당자', 'email'] 
                        else col for col in pivot.columns]
        
        pivoted_dfs.append(pivot)
    
    # ⭐ 모든 pivot 합치기
    platform_cols = pivoted_dfs[0].copy()
    for df in pivoted_dfs[1:]:
        platform_cols = platform_cols.merge(
            df,
            on=['order_daily', '매장명', '담당자', 'email'],
            how='outer'
        )
    
    print(f"[Pivot 변환] 완료: {len(platform_cols):,}건, {len(platform_cols.columns)}개 컬럼")
    print(f"[Pivot 타입 체크] order_daily: {platform_cols['order_daily'].dtype}")
    
    # ============================================================
    # ⭐ 3️⃣ 합치기: 기본 집계 + 플랫폼별 컬럼
    # ============================================================
    print(f"\n{'='*60}")
    print(f"[3단계] 기본 집계 + 플랫폼별 컬럼 합치기...")
    print(f"[타입 체크] daily_agg order_daily: {daily_agg['order_daily'].dtype}")
    print(f"[타입 체크] platform_cols order_daily: {platform_cols['order_daily'].dtype}")
    
    daily_agg_final = daily_agg.merge(
        platform_cols,
        on=['order_daily', '매장명', '담당자', 'email'],
        how='left'
    )
    
    # NaN을 0으로 채우기 (플랫폼이 없는 경우)
    platform_metric_cols = [col for col in daily_agg_final.columns 
                           if any(col.startswith(f"{m}_") for m in metrics_to_pivot)]
    daily_agg_final[platform_metric_cols] = daily_agg_final[platform_metric_cols].fillna(0)
    
    # ============================================================
    # ⭐ 비교용 평균 컬럼 추가
    # ============================================================
    # 일별 담당자별 평균 매출 (각 날짜의 각 담당자 평균)
    daily_agg_final['일별_담당자별_평균'] = daily_agg_final.groupby(['order_daily', '담당자'])['total_amount'].transform('mean').round(0)
    
    # 일별 전체 평균 매출 (각 날짜의 전체 평균)
    daily_agg_final['일별_전체_평균'] = daily_agg_final.groupby('order_daily')['total_amount'].transform('mean').round(0)
    
    # 일별 전체 평균 대비 비율
    daily_agg_final['vs일별전체_비율'] = (daily_agg_final['total_amount'] / daily_agg_final['일별_전체_평균'] * 100).round(1)
    # 0으로 나누기 처리
    daily_agg_final['vs일별전체_비율'] = daily_agg_final['vs일별전체_비율'].replace([np.inf, -np.inf], 100)
    
    print(f"[최종 집계] 완료: {len(daily_agg_final):,}건")
    print(f"[최종 컬럼] {len(daily_agg_final.columns)}개")
    print(f"[컬럼 샘플] {list(daily_agg_final.columns[:10])} ...")
    
    # ============================================================
    # ⭐ 검증
    # ============================================================
    print(f"\n{'='*60}")
    print(f"[검증] 데이터 확인:")
    print(f"  행 수: {len(daily_agg_final):,}건")
    print(f"  컬럼 수: {len(daily_agg_final.columns)}개")
    
    # 플랫폼별 컬럼 확인
    platform_cols_list = [col for col in daily_agg_final.columns if '_' in col and 
                         any(col.startswith(f"{m}_") for m in metrics_to_pivot)]
    print(f"  플랫폼별 컬럼: {platform_cols_list}")
    
    if '실오픈일' in daily_agg_final.columns:
        print(f"  ✅ 실오픈일: {daily_agg_final['실오픈일'].notna().sum()}건 존재")
    
    if 'settlement_amount' in daily_agg_final.columns:
        total_settlement = daily_agg_final['settlement_amount'].sum()
        print(f"  ✅ settlement_amount (합계): {total_settlement:,.0f}원")
    
    print("="*60)
    
    # ============================================================
    # ⭐ Parquet 저장
    # ============================================================
    temp_dir = LOCAL_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    daily_agg_final.to_parquet(output_path, index=False, engine='pyarrow')
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    print(f"\n[저장] {output_path.name}")
    
    return f"집계 완료: {len(daily_agg_final):,}건 ({len(daily_agg_final.columns)}개 컬럼)"


# ============================================================
# 스코어 계산
# ============================================================
def calculate_scores(
    input_task_id=None,
    input_xcom_key=None,
    output_xcom_key='scores_calculated',
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    **context
) -> None:
    """
    일별 매출 데이터에 대한 스코어링 및 상태 판정
    
    ⭐ 핵심 수정:
    1. 이동평균 min_periods=7로 변경 (충분한 데이터 필요)
    2. baseline=0 처리 개선 (점수 계산 가능하도록)
    3. sum_7d_prev 계산 로직 수정
    """
    print("\n" + "="*50)
    print("[스코어 계산] 시작")
    print("="*50)
    
    try:
        ti = context.get('task_instance') if context else None
        temp_dir = LOCAL_DB / 'temp'

        # 데이터 로드
        aggregated_path = None
        if ti and input_task_id and input_xcom_key:
            aggregated_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)

        if not aggregated_path:
            prefix = (input_xcom_key or 'daily_aggregated') + '_*.parquet'
            parquet_files = sorted(temp_dir.glob(prefix))
            if not parquet_files:
                raise FileNotFoundError("집계 데이터 파일을 찾을 수 없습니다")
            aggregated_path = parquet_files[-1]

        aggregated_path = Path(aggregated_path)
        print(f"[로드] {aggregated_path.name}")

        df = pd.read_parquet(aggregated_path)
        print(f"[로드] 완료: {len(df):,}건")
        
        # 날짜 필터링
        if start_date:
            df = df[df['order_daily'] >= pd.to_datetime(start_date)]
            print(f"[필터] start_date >= {start_date}: {len(df):,}건")
        
        if end_date:
            df = df[df['order_daily'] <= pd.to_datetime(end_date)]
            print(f"[필터] end_date <= {end_date}: {len(df):,}건")
        
        if len(df) == 0:
            print("[경고] 필터링 후 데이터가 없습니다")
            return
        
        # uploaded_at 추가
        df['uploaded_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 매장별 정렬
        df = df.sort_values(['매장명', 'order_daily'])
        
        # ============================================================
        # ⭐⭐⭐ 이동평균 계산 (min_periods 조정) ⭐⭐⭐
        # ============================================================
        print("\n[이동평균] 계산 시작...")
        
        # ⭐ min_periods를 7로 변경 (최소 1주일 데이터 필요)
        df['ma_14'] = df.groupby('매장명')['total_amount'].transform(
            lambda x: x.rolling(window=14, min_periods=7).mean()
        ).round(2)
        
        df['ma_28'] = df.groupby('매장명')['total_amount'].transform(
            lambda x: x.rolling(window=28, min_periods=14).mean()
        ).round(2)
        
        print(f"[이동평균] 완료")
        print(f"[DEBUG] ma_14 결측치: {df['ma_14'].isna().sum()}건")
        print(f"[DEBUG] ma_28 결측치: {df['ma_28'].isna().sum()}건")
        
        # 요일 정보
        df['weekday'] = df['order_daily'].dt.day_name()
        
        # 전주 동일 요일 매출
        df['prev_week_same_day'] = df.groupby(['매장명', 'weekday'])['total_amount'].shift(1)
        df['prev_week_same_day'] = df['prev_week_same_day'].fillna(0).astype(int)
        
        # 최근 2주 평균
        df['current_avg_2week'] = df.groupby('매장명')['total_amount'].transform(
            lambda x: x.rolling(window=14, min_periods=7).mean()
        ).round(2)
        
        # 2주 전 동일 요일
        df['prev_2week_same_day'] = df.groupby(['매장명', 'weekday'])['total_amount'].shift(2)
        df['prev_2week_same_day'] = df['prev_2week_same_day'].fillna(0).astype(int)
        
        # 3주 전 동일 요일
        df['prev_3week_same_day'] = df.groupby(['매장명', 'weekday'])['total_amount'].shift(3)
        df['prev_3week_same_day'] = df['prev_3week_same_day'].fillna(0).astype(int)
        
        # 최근 4주 평균
        df['current_avg_4week'] = df.groupby('매장명')['total_amount'].transform(
            lambda x: x.rolling(window=28, min_periods=14).mean()
        ).round(2)
        
        # ============================================================
        # ⭐⭐⭐ 최근 7일 & 이전 7일 합계 (수정) ⭐⭐⭐
        # ============================================================
        # 최근 7일 합계
        df['sum_7d_recent'] = df.groupby('매장명')['total_amount'].transform(
            lambda x: x.rolling(window=7, min_periods=1).sum()
        ).astype(int)
        
        # ⭐ 이전 7일 합계 (8~14일 전) - 수정된 로직
        df['sum_7d_prev'] = df.groupby('매장명')['total_amount'].transform(
            lambda x: x.shift(7).rolling(window=7, min_periods=1).sum()
        ).fillna(0).astype(int)
        
        print(f"[통계 지표] 계산 완료")
        print(f"[DEBUG] sum_7d_prev 결측치: {df['sum_7d_prev'].isna().sum()}건")
        
        # ============================================================
        # ⭐⭐⭐ 스코어 계산 로직 (baseline=0 처리 개선) ⭐⭐⭐
        # ============================================================
        print("\n[스코어] 계산 시작...")
        
        def calc_score(current, baseline):
            """
            변화율 기반 점수 계산 (하락=나쁨)
            
            ⭐ 수정: baseline=0일 때도 점수 계산 가능
            - baseline=0이면 current 기준으로 판단
            - current > 0이면 0점 (신규/회복)
            - current = 0이면 0점 (데이터 없음)
            """
            # NaN 체크
            if pd.isna(current) or pd.isna(baseline):
                return 0
            
            # ⭐ baseline=0 처리 개선
            if baseline == 0:
                # baseline이 0인데 current가 있으면 신규 또는 회복 → 0점
                if current > 0:
                    return 0
                else:
                    return 0  # 둘 다 0이면 비교 불가 → 0점
            
            # 정상적인 변화율 계산
            change_rate = (current - baseline) / baseline
            
            if change_rate >= 0:
                return 0  # 상승/유지 = 좋음
            elif change_rate > -0.1:
                return 1  # -10% ~ 0% 사이 = 소폭 하락
            else:
                return 2  # -10% 미만 = 급격 하락
        
        # 1. 트렌드 스코어 (단기14일 vs 장기28일)
        df['score_trend'] = df.apply(
            lambda row: calc_score(row['ma_14'], row['ma_28']),
            axis=1
        )
        
        # 2. 일일 스코어 (최근2주평균 vs 14일MA)
        df['score_total'] = df.apply(
            lambda row: calc_score(row['current_avg_2week'], row['ma_14']),
            axis=1
        )
        
        # 3. 주간 스코어 (최근7일 vs 이전7일)
        df['score_7d_total'] = df.apply(
            lambda row: calc_score(row['sum_7d_recent'], row['sum_7d_prev']),
            axis=1
        )
        
        # 4. 월간 스코어 (최근2주평균 vs 전체4주평균)
        df['score_4week_total'] = df.apply(
            lambda row: calc_score(row['current_avg_2week'], row['current_avg_4week']),
            axis=1
        )
        
        # 5. 종합 스코어 (0~8점)
        df['score'] = (
            df['score_trend'] + 
            df['score_total'] + 
            df['score_7d_total'] + 
            df['score_4week_total']
        )
        
        print(f"[스코어] 분포:")
        score_dist = df['score'].value_counts().sort_index()
        for score, count in score_dist.items():
            print(f"  {score}점: {count:,}건")
        
        # 개별 점수 분포 확인
        print(f"\n[개별 점수 분포]:")
        print(f"  score_trend: {df['score_trend'].value_counts().sort_index().to_dict()}")
        print(f"  score_total: {df['score_total'].value_counts().sort_index().to_dict()}")
        print(f"  score_7d_total: {df['score_7d_total'].value_counts().sort_index().to_dict()}")
        print(f"  score_4week_total: {df['score_4week_total'].value_counts().sort_index().to_dict()}")
        
        # ============================================================
        # 상태 판정
        # ============================================================
        df['status'] = np.where(
            df['score'] >= 6, '위험',      # 6~8점
            np.where(
                df['score'] >= 4, '주의',  # 4~5점
                '정상'                      # 0~3점
            )
        )
        
        # 이전 상태
        df['pre_status'] = df.groupby(['매장명', '담당자'])['status'].shift(1)
        df['pre_status'] = df['pre_status'].fillna('정상')
        
        # ============================================================
        # 금일/전일 관련 컬럼
        # ============================================================
        print("\n[금일/전일/전주/전월] 계산 시작...")
        
        max_date = df['order_daily'].max()
        prev_date = max_date - pd.Timedelta(days=1)
        
        df['금일여부'] = df['order_daily'].apply(lambda x: '금일' if x == max_date else '')
        df['전일여부'] = df['order_daily'].apply(lambda x: '전일' if x == prev_date else '')
        
        # 전일 데이터 준비 (1일 전 데이터를 1일 후로 이동 → 현재 날짜와 merge)
        # 예: 1월 10일 데이터 → order_daily를 1월 11일로 변경 → 1월 11일 행과 매칭
        df_prev = df[['order_daily', '매장명', 'total_amount', 'total_order_count']].copy()
        df_prev['order_daily'] = df_prev['order_daily'] + pd.Timedelta(days=1)
        df_prev = df_prev.rename(columns={
            'total_amount': '전일_매출',
            'total_order_count': '전일_주문건수'
        })
        
        # 전주 (7일 전) 데이터 준비 (7일 전 데이터를 7일 후로 이동 → 현재 날짜와 merge)
        # 예: 1월 4일 데이터 → order_daily를 1월 11일로 변경 → 1월 11일 행과 매칭
        df_prev_week = df[['order_daily', '매장명', 'total_amount', 'total_order_count']].copy()
        df_prev_week['order_daily'] = df_prev_week['order_daily'] + pd.Timedelta(days=7)
        df_prev_week = df_prev_week.rename(columns={
            'total_amount': '전주_매출',
            'total_order_count': '전주_주문건수'
        })
        
        # ⭐ 전월 매출 (월별 전체 합계 비교)
        # 예: 2026년 1월 각 일자 → 2025년 12월 전체 매출 합계
        df['년월'] = df['order_daily'].dt.to_period('M')
        
        # 매장별 월별 매출 합계 (담당자별)
        monthly_total = df.groupby(['매장명', '담당자', '년월']).agg({
            'total_amount': 'sum',
            'total_order_count': 'sum'
        }).reset_index()
        
        # 전월로 이동 (현재 월 + 1개월 = 다음 달에 매칭)
        monthly_total['년월_next'] = monthly_total['년월'].apply(lambda x: x + 1)
        monthly_total = monthly_total.rename(columns={
            'total_amount': '전월_매출',
            'total_order_count': '전월_주문건수'
        })
        
        # 원본 데이터에 전월 합계 붙이기 (매장명 + 담당자 기준으로 merge)
        df = df.merge(
            monthly_total[['매장명', '담당자', '년월_next', '전월_매출', '전월_주문건수']],
            left_on=['매장명', '담당자', '년월'],
            right_on=['매장명', '담당자', '년월_next'],
            how='left'
        ).drop(columns=['년월_next'])
        
        # Merge 전일
        df = df.merge(
            df_prev[['order_daily', '매장명', '전일_매출', '전일_주문건수']],
            on=['order_daily', '매장명'],
            how='left'
        )
        
        # Merge 전주
        df = df.merge(
            df_prev_week[['order_daily', '매장명', '전주_매출', '전주_주문건수']],
            on=['order_daily', '매장명'],
            how='left'
        )
        
        # 전월은 이미 위에서 merge됨 (월별 전체 합계)
        # '년월' 컬럼 제거
        df = df.drop(columns=['년월'])
        
        # 전일 결측치 처리
        df['전일_매출'] = df['전일_매출'].fillna(0).astype(int)
        df['전일_주문건수'] = df['전일_주문건수'].fillna(0).astype(int)
        
        # 전주 결측치 처리
        df['전주_매출'] = df['전주_매출'].fillna(0).astype(int)
        df['전주_주문건수'] = df['전주_주문건수'].fillna(0).astype(int)
        
        # 전월 결측치 처리
        df['전월_매출'] = df['전월_매출'].fillna(0).astype(int)
        df['전월_주문건수'] = df['전월_주문건수'].fillna(0).astype(int)
        
        # 전일대비 증감액/증감률
        diff_amount = df['total_amount'] - df['전일_매출']
        df['전일대비_증감액'] = diff_amount
        df.loc[df['전일_매출'] == 0, '전일대비_증감액'] = pd.NA
        df['전일대비_증감액'] = df['전일대비_증감액'].astype('Int64')

        df['전일대비_증감률'] = (diff_amount / df['전일_매출']).replace([np.inf, -np.inf], np.nan) * 100
        df['전일대비_증감률'] = df['전일대비_증감률'].round(2)
        df.loc[df['전일_매출'] == 0, '전일대비_증감률'] = pd.NA
        
        # 전주대비 증감액/증감률
        diff_week_amount = df['total_amount'] - df['전주_매출']
        df['전주대비_증감액'] = diff_week_amount
        df.loc[df['전주_매출'] == 0, '전주대비_증감액'] = pd.NA
        df['전주대비_증감액'] = df['전주대비_증감액'].astype('Int64')

        df['전주대비_증감률'] = (diff_week_amount / df['전주_매출']).replace([np.inf, -np.inf], np.nan) * 100
        df['전주대비_증감률'] = df['전주대비_증감률'].round(2)
        df.loc[df['전주_매출'] == 0, '전주대비_증감률'] = pd.NA
        
        # 전월대비 증감액/증감률
        diff_month_amount = df['total_amount'] - df['전월_매출']
        df['전월대비_증감액'] = diff_month_amount
        df.loc[df['전월_매출'] == 0, '전월대비_증감액'] = pd.NA
        df['전월대비_증감액'] = df['전월대비_증감액'].astype('Int64')

        df['전월대비_증감률'] = (diff_month_amount / df['전월_매출']).replace([np.inf, -np.inf], np.nan) * 100
        df['전월대비_증감률'] = df['전월대비_증감률'].round(2)
        df.loc[df['전월_매출'] == 0, '전월대비_증감률'] = pd.NA
        
        print(f"[날짜 체크] 최대 날짜: {max_date.strftime('%Y-%m-%d')}")
        print(f"[금일여부] '금일' 건수: {(df['금일여부'] == '금일').sum():,}건")
        print(f"[전일여부] '전일' 건수: {(df['전일여부'] == '전일').sum():,}건")
        
        # 최종 정렬
        df = df.sort_values(['매장명', '담당자', 'order_daily'])
        
        print(f"\n[스코어] 완료: {len(df):,}건")
        print(f"[스코어] 상태 분포: {df['status'].value_counts().to_dict()}")
        
        # ============================================================
        # ⭐ sales_daily_orders.csv 컬럼 순서 (Parquet 저장용)
        # ============================================================
        # 📌 여기에 최종 저장할 컬럼 순서를 정의합니다.
        # 기본 컬럼 → 플랫폼별 컬럼 (우측 끝)
        # ============================================================
        column_order = [
            'order_daily', 'order_date_with_time', '매장명', '담당자', 'email',
            'total_order_count', 'total_amount', 'fee_ad', 'ARPU',
            'ma_14', 'ma_28', 'weekday',
            'prev_week_same_day', 'current_avg_2week', 'prev_2week_same_day',
            'prev_3week_same_day', 'current_avg_4week',
            'sum_7d_recent', 'sum_7d_prev',
            'score_trend', 'score_total', 'score_7d_total', 'score_4week_total', 'score',
            'status', 'pre_status', 'uploaded_at',
            '금일여부', '전일여부', 
            '전일_매출', '전일_주문건수', '전일대비_증감액', '전일대비_증감률',
            '전주_매출', '전주_주문건수', '전주대비_증감액', '전주대비_증감률',
            '전월_매출', '전월_주문건수', '전월대비_증감액', '전월대비_증감률',
            
            # ⭐ 기본 추가 컬럼
            '실오픈일', 'platform', 'settlement_amount',
            
            # ⭐ 일별 비교용 평균 컬럼
            '일별_담당자별_평균', '일별_전체_평균', 'vs일별전체_비율'
        ]
        
        # ⭐ 플랫폼별 컬럼 동적 추가 (우측 끝)
        platform_cols = [col for col in df.columns if '_' in col and 
                        any(col.startswith(f"{m}_") for m in 
                            ['total_order_count', 'total_amount', 'fee_ad', 'settlement_amount'])]
        
        # 정렬: 지표별 → 플랫폼별 (예: total_amount_배민, total_amount_쿠팡, fee_ad_배민, ...)
        platform_cols_sorted = sorted(platform_cols)
        
        column_order.extend(platform_cols_sorted)
        
        # 존재하는 컬럼만 선택
        available_cols = [col for col in column_order if col in df.columns]
        df = df[available_cols]
        
        print(f"\n[컬럼 순서] 총 {len(available_cols)}개 컬럼")
        print(f"[플랫폼별 컬럼] {len(platform_cols_sorted)}개: {platform_cols_sorted}")
        
        # Parquet 저장
        output_path = temp_dir / f'sales_daily_orders_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        df.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
        
        print(f"\n[저장] {output_path.name}")
        print(f"[저장] 완료: {len(df):,}건")

        if ti and output_xcom_key:
            ti.xcom_push(key=output_xcom_key, value=str(output_path))

        # ⭐ CSV 저장: sales_daily_orders_alerts.csv
        csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_alerts.csv'
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            df_csv = df.copy()
            
            # ⭐ 불필요한 컬럼 제거 (저장용)
            cols_to_drop = ['_row_hash', 'collected_at', 'order_date_with_time']
            for col in cols_to_drop:
                if col in df_csv.columns:
                    df_csv.drop(columns=[col], inplace=True)
                    print(f"[CSV 저장] {col} 컬럼 제거")
            
            # ⭐ order_daily 문자열로 확정 (datetime 아님!)
            if 'order_daily' in df_csv.columns:
                if pd.api.types.is_datetime64_any_dtype(df_csv['order_daily']):
                    df_csv['order_daily'] = df_csv['order_daily'].dt.strftime('%Y-%m-%d')
                    print(f"[CSV 저장] order_daily를 datetime에서 문자열로 변환")
                else:
                    df_csv['order_daily'] = df_csv['order_daily'].astype(str)
                    print(f"[CSV 저장] order_daily 타입 확인: {df_csv['order_daily'].dtype}")
            
            # 임시 파일로 저장 후 교체
            import tempfile
            import shutil
            
            with tempfile.NamedTemporaryFile(
                mode='w', delete=False, dir=str(csv_path.parent),
                prefix='tmp_alerts_', suffix='.csv'
            ) as tmp_file:
                tmp_csv_path = tmp_file.name
            
            df_csv.to_csv(tmp_csv_path, index=False, encoding='utf-8-sig')
            shutil.move(tmp_csv_path, csv_path)
            print(f"[CSV 저장] sales_daily_orders_alerts.csv: {len(df_csv):,}건")
            print(f"[CSV 저장] 저장 경로: {csv_path}")
            shutil.move(tmp_csv_path, csv_path)
            print(f"[CSV 저장] sales_daily_orders_alerts.csv: {len(df_csv):,}건")
        except Exception as e:
            print(f"[CSV 저장 실패] {e}")
            if 'tmp_csv_path' in locals() and os.path.exists(tmp_csv_path):
                os.remove(tmp_csv_path)
        
        print("\n" + "="*50)
        print("[스코어 계산] 완료")
        print("="*50)
        
    except Exception as e:
        print(f"\n[오류] 스코어 계산 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


# ============================================================
# 알람 필터링
# ============================================================
def filter_alerts(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """알람 대상 필터링

    - 기본: 입력 데이터의 최신 일자(max(order_daily))를 사용해 필터링
    - ds가 주어지고 ds <= 최신일자이면 ds를 사용
      (데이터가 더 최신이면 최신 일자를 사용해 누락 방지)
    """
    import numpy as np
    import shutil
    import tempfile
    from datetime import datetime, timedelta
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)
    
    if not parquet_path:
        print(f"[경고] 입력 Parquet 경로 없음")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 없음)"
    
    df = pd.read_parquet(parquet_path)
    print(f"[필터링] 입력 데이터: {len(df):,}건")
    
    # 날짜 형식 통일
    df['order_daily'] = pd.to_datetime(df['order_daily'])

    # 대상 일자 결정: 데이터 최신일자 vs ds
    max_date = df['order_daily'].max()
    ds_str = context.get('ds')
    ds_dt = pd.to_datetime(ds_str) if ds_str else None

    target_date = max_date
    target_reason = "데이터 최신일자"
    if ds_dt is not None and ds_dt <= max_date:
        target_date = ds_dt
        target_reason = "스케줄 ds"

    print(f"[필터링] 데이터 최신일자: {max_date.date() if pd.notnull(max_date) else 'N/A'}")
    print(f"[필터링] ds: {ds_dt.date() if ds_dt is not None else 'None'}")
    print(f"[필터링] 사용 일자: {target_date.date() if pd.notnull(target_date) else 'N/A'} ({target_reason})")

    # ⭐⭐⭐ 전체 집계 CSV는 항상 저장 (어제 데이터 유무와 상관없이) ⭐⭐⭐
    csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_alerts.csv'
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 전체 데이터 정렬
    combined_df = df.sort_values(['매장명', '담당자', 'order_daily']).reset_index(drop=True)
    print(f"[CSV] 전체 집계 데이터: {len(combined_df):,}건")
    
    try:
        # order_daily 문자열 변환
        combined_df_save = combined_df.copy()
        
        # ⭐ 불필요한 컬럼 제거 (저장용)
        cols_to_drop = ['_row_hash', 'collected_at', 'order_date_with_time']
        for col in cols_to_drop:
            if col in combined_df_save.columns:
                combined_df_save.drop(columns=[col], inplace=True)
                print(f"[CSV 저장] {col} 컬럼 제거")
        
        # ⭐ order_daily 문자열로 확정 (datetime 아님!)
        if 'order_daily' in combined_df_save.columns:
            if pd.api.types.is_datetime64_any_dtype(combined_df_save['order_daily']):
                combined_df_save['order_daily'] = combined_df_save['order_daily'].dt.strftime('%Y-%m-%d')
                print(f"[CSV 저장] order_daily를 datetime에서 문자열로 변환")
            else:
                combined_df_save['order_daily'] = combined_df_save['order_daily'].astype(str)
                print(f"[CSV 저장] order_daily 타입 확인: {combined_df_save['order_daily'].dtype}")
        
        # 전체 집계 CSV 저장
        with tempfile.NamedTemporaryFile(
            mode='w', delete=False, dir=str(csv_path.parent),
            prefix='tmp_grp_', suffix='.csv'
        ) as tmp_file:
            tmp_path = tmp_file.name
        
        combined_df_save.to_csv(tmp_path, index=False, encoding='utf-8-sig')
        shutil.move(tmp_path, csv_path)
        print(f"[✅ CSV] 전체 집계 저장 완료: {len(combined_df_save):,}건 → {csv_path.name}")
        
    except Exception as e:
        print(f"[❌ 에러] 전체 집계 CSV 저장 실패: {e}")
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            os.remove(tmp_path)

    # ⭐ 대상 일자만 필터링
    df_target = df[df['order_daily'] == target_date].copy()

    print(f"[필터링] 대상일({target_date.date() if pd.notnull(target_date) else 'N/A'}) 데이터: {len(df_target):,}건")

    if len(df_target) == 0:
        print(f"[경고] 대상일 데이터가 없습니다 → 알림 대상 없음 (전체 집계는 저장됨)")
        ti.xcom_push(key=output_xcom_key, value=None)
        return f"알림 대상 없음 (대상일 데이터 없음, 전체 집계 {len(combined_df):,}건 저장됨)"
    
    # 알람 조건 (대상 일자 데이터)
    alert_targets = df_target[
        (df_target['status'] == '위험') |
        ((df_target['status'] == '주의') & (df_target['pre_status'] == '주의'))
    ].copy()
    
    print(f"[필터링] 알람 대상: {len(alert_targets):,}건")
    
    # ⭐ Parquet 저장
    temp_dir = LOCAL_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    parquet_output = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    alert_targets.to_parquet(parquet_output, index=False, engine='pyarrow')
    print(f"[Parquet] 저장: {parquet_output.name}")
    
    # ⭐ Alert 대상 CSV 저장: sales_daily_orders_grp_alert.csv (어제 기준)
    alerts_csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_grp_alert.csv'
    alerts_csv_path.parent.mkdir(parents=True, exist_ok=True)

    alert_targets_sorted = alert_targets.sort_values(['매장명', '담당자', 'order_daily']).reset_index(drop=True)
    try:
        alert_save = alert_targets_sorted.copy()
        
        # ⭐ 불필요한 컬럼 제거 (저장용)
        cols_to_drop = ['_row_hash', 'collected_at', 'order_date_with_time']
        for col in cols_to_drop:
            if col in alert_save.columns:
                alert_save.drop(columns=[col], inplace=True)
                print(f"[CSV 저장] {col} 컬럼 제거")
        
        # ⭐ order_daily 문자열로 확정 (datetime 아님!)
        if 'order_daily' in alert_save.columns:
            if pd.api.types.is_datetime64_any_dtype(alert_save['order_daily']):
                alert_save['order_daily'] = alert_save['order_daily'].dt.strftime('%Y-%m-%d')
                print(f"[CSV 저장] order_daily를 datetime에서 문자열로 변환")
            else:
                alert_save['order_daily'] = alert_save['order_daily'].astype(str)
                print(f"[CSV 저장] order_daily 타입 확인: {alert_save['order_daily'].dtype}")

        with tempfile.NamedTemporaryFile(
            mode='w', delete=False, dir=str(alerts_csv_path.parent),
            prefix='tmp_alert_', suffix='.csv'
        ) as tmp_file:
            tmp_alert_path = tmp_file.name

        alert_save.to_csv(tmp_alert_path, index=False, encoding='utf-8-sig')
        shutil.move(tmp_alert_path, alerts_csv_path)
        print(f"[CSV] 알림 대상 저장 완료: {len(alert_save):,}건 → {alerts_csv_path.name}")
    except Exception as e:
        if 'tmp_alert_path' in locals() and os.path.exists(tmp_alert_path):
            os.remove(tmp_alert_path)
        raise e

    ti.xcom_push(key=output_xcom_key, value=str(parquet_output))

    return f"✅ CSV 저장 완료: alert {len(alert_targets):,}건 (어제 기준), 전체 {len(combined_df):,}건"