"""
작업.py - 토더 리뷰 파이프라인
io3의 범용 함수를 wrapper로 감싸서 사용
"""

from pathlib import Path
from modules.transform.utility.paths import COLLECT_DB, LOCAL_DB, TEMP_DIR
from modules.transform.utility.io3 import load_files, preprocess_df, save_to_csv, join_dataframes
import pandas as pd
import datetime as dt



# baemin_ad_change_history / 변경이력 수집
def load_beamin_ad_change_history_df(**context):
    """변경이력 수집"""
    return load_files(
        patterns=['baemin_ad_change_history_*.csv'], # 토더 리뷰 파일 패턴
        search_paths=[
            Path('/opt/airflow/download/업로드_temp'), # 업로드 임시 폴더
            Path('/opt/airflow/download'), # 일반 다운로드 폴더
            COLLECT_DB / "영업관리부_수집" # 수집 DB 폴더
        ],
        xcom_key='baemin_ad_change_history_path', # XCom 키
        file_type='auto',  # CSV 우선, 없으면 Excel
        **context
    )

# 변경 이력 전처리
def transform_beamin_ad_change_history_df(df):
    df = df.copy()
    df = df.drop_duplicates(subset=["변경시간", "store_id"], keep="last")
    df["수집주별"] = pd.to_datetime(df["변경시간"]).dt.to_period("W")
    df["platform"] = "배민"

    df2 = df.groupby(["store_id", "매장명", "수집주별", "platform"]).agg(변경전_희망가_최소 = ("변경전_희망가","min"),
                                                    변경전_희망가_최대 = ("변경전_희망가","max"),
                                                    변경전_희망가_평균 = ("변경전_희망가","mean"),
                                                    변경후_희망가_최소 = ("변경후_희망가","min"),
                                                    변경후_희망가_최대 = ("변경후_희망가","max"),
                                                    변경후_희망가_평균 = ("변경후_희망가","mean")
                                                ).reset_index()

    df = df[['수집일시', '매장명', "platform",'store_id','캠페인정보', '변경시간','수집주별']]
    df = pd.merge(df, df2, on=['store_id', '매장명', '수집주별', "platform"], how='left')

    df["join_date"] = pd.to_datetime(df["변경시간"]).dt.to_period("D")
    df["join_date"] = df["join_date"].dt.to_timestamp()
    df["store_names"] = df["매장명"].str.split(" ").str[-2:].str.join(" ")

    df = df[['join_date', 'store_names', 'store_id', '캠페인정보', '변경시간', '수집주별', '변경전_희망가_최소',
        '변경전_희망가_최대', '변경전_희망가_평균', '변경후_희망가_최소', '변경후_희망가_최대', '변경후_희망가_평균',
        ]]
    return df


def preprocess_beamin_ad_change_history_df(**context):
    """3. 변경 이력 전처리"""
    return preprocess_df(
        input_xcom_key='baemin_ad_change_history_path',
        output_xcom_key='baemin_ad_change_history_processed_path',
        transform_func=transform_beamin_ad_change_history_df,  # 위에서 정의한 함수
        **context
    )



# baemin_marketing_ / 우리가게 클릭 수집
def baemin_marketing_store_click_df(**context):
    """1. 우리가게 now 수집"""
    return load_files(
        patterns=['baemin_marketing_*.csv'], 
        search_paths=[
            Path('/opt/airflow/download/업로드_temp'), 
            Path('/opt/airflow/download'), # 일반 다운로드 폴더
            COLLECT_DB / "영업관리부_수집" # 수집 DB 폴더
        ],
        xcom_key='baemin_marketing_store_click_path', # XCom 키
        file_type='auto',  # CSV 우선, 없으면 Excel
        **context
    )
    
    
# 우리가게 클릭 전처리
def transform_store_click_df(df):
    # 전처리 시작
    click_df = df.copy()
    click_df = click_df.drop_duplicates(subset=["날짜", "store_id"], keep="last")
    click_df["store_names"] = click_df["store_name"].str.split(" ").str[-2:].str.join(" ")
    click_df["join_date"] = pd.to_datetime(click_df["날짜"]).dt.to_period("D")
    click_df["platform"] = "배민"

    # 우가클 click_df
    click_df["join_date"] = click_df["join_date"].dt.to_timestamp()
    click_df = click_df[['join_date', 'store_names', 'platform', '날짜', '광고지출', '노출수', '클릭수',
        '주문수', '주문금액', '광고효과', ]]
    
    return click_df

def preprocess_store_click_df(**context):
    """ 처리"""
    return preprocess_df(
        input_xcom_key='baemin_marketing_store_click_path',
        output_xcom_key='baemin_marketing_store_click_processed_path',
        transform_func=transform_store_click_df,  # 위에서 정의한 함수
        #natural_keys=['date', 'stores_name'],  # ID 생성용
        **context
    ) 


# coupangeats_cmg_ / 쿠팡 광고 수집
def coupangeats_cmg_df(**context):
    return load_files(
        patterns=['coupangeats_cmg_*.csv'], 
        search_paths=[
            Path('/opt/airflow/download/업로드_temp'), 
            Path('/opt/airflow/download'), # 일반 다운로드 폴더
            COLLECT_DB / "영업관리부_수집" # 수집 DB 폴더
        ],
        xcom_key='coupangeats_cmg_path', # XCom 키
        file_type='auto',  # CSV 우선, 없으면 Excel
        **context
    )
    
# 쿠팡 전처리
def transform_coupangeats_cmg_df(df):
    # 전처리 시작
    coupang_df = df.copy()
    coupang_df = coupang_df.drop_duplicates(subset=["조회일자", "매장명"], keep="last")
    coupang_df["조회일자"] = pd.to_datetime(coupang_df["조회일자"]).dt.to_period("D")

    # 쿠팡 광고
    coupang_df["조회일자"] = coupang_df["조회일자"].dt.to_timestamp()
    coupang_df["store_names"] = coupang_df["매장명"].str.split(" ").str[-2:].str.join(" ")
    coupang_df["platform"] = "쿠팡이츠"
    coupang_df = coupang_df[['조회일자', 'store_names','platform', '광고비율', '광고비용', '신규고객',
        '광고주문수', '광고매출', '전체매출', '광고클릭수', '광고노출수']]
    coupang_df
    return coupang_df

def preprocess_coupangeats_cmg_df(**context):
    """쿠팡 광고 전처리"""
    return preprocess_df(
        input_xcom_key='coupangeats_cmg_path',
        output_xcom_key='coupangeats_cmg_processed_path',
        transform_func=transform_coupangeats_cmg_df,  # 위에서 정의한 함수
        **context
    )


# 주문데이터 로드
def load_orders_upload_df(**context):
    """1. 우리가게 now 수집"""
    return load_files(
        patterns=['sales_daily_orders_upload.csv'], # 토더 리뷰 파일 패턴
        search_paths=[
            LOCAL_DB / "영업관리부_DB", # 일반 다운로드 폴더
        ],
        xcom_key='sales_daily_orders_upload_path', # XCom 키
        file_type='auto',  # CSV 우선, 없으면 Excel
        **context
    )

# 주문데이터 전처리
def transform_orders_upload_df(df):
    # 전처리 시작
    orders_upload_df = df.copy()
    orders_upload_df["order_daily_day"] = (
        pd.to_datetime(
            orders_upload_df["order_daily"].astype(str),
            format="mixed",
            errors="coerce"
        )
        .dt.normalize()   # 00:00:00 으로 정규화
    )     
    return orders_upload_df

def preprocess_orders_upload_df(**context):
    """ 처리"""
    return preprocess_df(
        input_xcom_key='sales_daily_orders_upload_path',
        output_xcom_key='sales_daily_orders_upload_processed_path',
        transform_func=transform_orders_upload_df,  # 위에서 정의한 함수
        #natural_keys=['date', 'stores_name'],  # ID 생성용
        **context
    )
'''

merged_df = pd.merge(
    orders_upload_df,
    df,
    left_on=["order_daily_day", "매장명"],
    right_on=["join_date", "store_names"],
    how="left",
)
merged_df["order_daily_day"] = (
    pd.to_datetime(
        merged_df["order_daily"].astype(str),
        format="mixed",
        errors="coerce"
    )
    .dt.normalize()   # 00:00:00 으로 정규화
)
'''


def left_join_orders_history(
    left_task,
    right_task,
    on=None,
    left_on=None,
    right_on=None,
    how='left',
    drop_right_keys=True,  # 조인 후 오른쪽 키 컬럼을 자동으로 삭제할지 여부
    output_xcom_key='joined_data_path', # XCom 키
    **context
):
    """
    두 Task의 Parquet 데이터를 로드하여 Join하는 범용 함수
    """
    ti = context['task_instance']
    
    # 1. 데이터 경로 가져오기 (Task ID와 Key가 딕셔너리 형태라고 가정)
    left_path = ti.xcom_pull(task_ids=left_task['task_id'], key=left_task['xcom_key'])
    right_path = ti.xcom_pull(task_ids=right_task['task_id'], key=right_task['xcom_key'])
    
    if not left_path:
        print(f"[ERROR] 왼쪽 데이터 경로 없음: {left_task['task_id']}")
        return None

    left_df = pd.read_parquet(left_path)
    
    # 2. 오른쪽 데이터가 없는 경우 처리 (Graceful Degradation)
    if not right_path:
        print(f"[WARNING] 오른쪽 데이터 없음: {right_task['task_id']}. 왼쪽 데이터만 보존합니다.")
        save_path = TEMP_DIR / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
        left_df.to_parquet(save_path, index=False)
        ti.xcom_push(key=output_xcom_key, value=str(save_path))
        return f"오른쪽 데이터 없음, 원본 유지 ({len(left_df)}행)"

    right_df = pd.read_parquet(right_path)

    # 3. Join 실행
    if on:
        joined_df = left_df.merge(right_df, on=on, how=how)
    elif left_on and right_on:
        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)
        
        # 4. 중복 키 컬럼 제거 (drop_right_keys=True 일 때)
        if drop_right_keys:
            # left_on과 right_on의 이름이 다를 경우에만 right_on 컬럼들을 삭제
            r_keys = [right_on] if isinstance(right_on, str) else right_on
            l_keys = [left_on] if isinstance(left_on, str) else left_on
            
            cols_to_drop = [c for c in r_keys if c in joined_df.columns and c not in l_keys]
            if cols_to_drop:
                joined_df.drop(columns=cols_to_drop, inplace=True)
                print(f"[INFO] 중복 방지를 위해 삭제된 오른쪽 키 컬럼: {cols_to_drop}")
    else:
        raise ValueError("조인을 위해 'on' 또는 'left_on/right_on' 매개변수가 필요합니다.")

    # 5. 결과 저장 및 XCom Push
    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    output_path = TEMP_DIR / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    joined_df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    print(f"[SUCCESS] Join 완료: {len(joined_df):,}행")
    return str(output_path)


## 주문 + 변경이력 + 우가클
def left_join_orders_history_click(
    left_task,
    right_task,
    on=None,
    left_on=None,
    right_on=None,
    how='left',
    drop_right_keys=True,  # 조인 후 오른쪽 키 컬럼을 자동으로 삭제할지 여부
    output_xcom_key='joined_data_path', # XCom 키
    **context
):
    """
    두 Task의 Parquet 데이터를 로드하여 Join하는 범용 함수
    """
    ti = context['task_instance']
    
    # 1. 데이터 경로 가져오기 (Task ID와 Key가 딕셔너리 형태라고 가정)
    left_path = ti.xcom_pull(task_ids=left_task['task_id'], key=left_task['xcom_key'])
    right_path = ti.xcom_pull(task_ids=right_task['task_id'], key=right_task['xcom_key'])
    
    if not left_path:
        print(f"[ERROR] 왼쪽 데이터 경로 없음: {left_task['task_id']}")
        return None

    left_df = pd.read_parquet(left_path)
    
    # 2. 오른쪽 데이터가 없는 경우 처리 (Graceful Degradation)
    if not right_path:
        print(f"[WARNING] 오른쪽 데이터 없음: {right_task['task_id']}. 왼쪽 데이터만 보존합니다.")
        save_path = TEMP_DIR / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
        left_df.to_parquet(save_path, index=False)
        ti.xcom_push(key=output_xcom_key, value=str(save_path))
        return f"오른쪽 데이터 없음, 원본 유지 ({len(left_df)}행)"

    right_df = pd.read_parquet(right_path)

    # 3. Join 실행
    if on:
        joined_df = left_df.merge(right_df, on=on, how=how)
    elif left_on and right_on:
        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)
        
        # 4. 중복 키 컬럼 제거 (drop_right_keys=True 일 때)
        if drop_right_keys:
            # left_on과 right_on의 이름이 다를 경우에만 right_on 컬럼들을 삭제
            r_keys = [right_on] if isinstance(right_on, str) else right_on
            l_keys = [left_on] if isinstance(left_on, str) else left_on
            
            cols_to_drop = [c for c in r_keys if c in joined_df.columns and c not in l_keys]
            if cols_to_drop:
                joined_df.drop(columns=cols_to_drop, inplace=True)
                print(f"[INFO] 중복 방지를 위해 삭제된 오른쪽 키 컬럼: {cols_to_drop}")
    else:
        raise ValueError("조인을 위해 'on' 또는 'left_on/right_on' 매개변수가 필요합니다.")

    # 5. 결과 저장 및 XCom Push
    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    output_path = TEMP_DIR / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    joined_df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    print(f"[SUCCESS] Join 완료: {len(joined_df):,}행")
    return str(output_path)


## 주문 + 변경이력 + 우가클 + 쿠팡
def left_join_orders_history_click_coupang(
    left_task,
    right_task,
    on=None,
    left_on=None,
    right_on=None,
    how='left',
    drop_right_keys=True,  # 조인 후 오른쪽 키 컬럼을 자동으로 삭제할지 여부
    output_xcom_key='joined_data_path', # XCom 키
    **context
):
    """
    두 Task의 Parquet 데이터를 로드하여 Join하는 범용 함수
    """
    ti = context['task_instance']
    
    # 1. 데이터 경로 가져오기 (Task ID와 Key가 딕셔너리 형태라고 가정)
    left_path = ti.xcom_pull(task_ids=left_task['task_id'], key=left_task['xcom_key'])
    right_path = ti.xcom_pull(task_ids=right_task['task_id'], key=right_task['xcom_key'])
    
    if not left_path:
        print(f"[ERROR] 왼쪽 데이터 경로 없음: {left_task['task_id']}")
        return None

    left_df = pd.read_parquet(left_path)
    
    # 2. 오른쪽 데이터가 없는 경우 처리 (Graceful Degradation)
    if not right_path:
        print(f"[WARNING] 오른쪽 데이터 없음: {right_task['task_id']}. 왼쪽 데이터만 보존합니다.")
        save_path = TEMP_DIR / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
        left_df.to_parquet(save_path, index=False)
        ti.xcom_push(key=output_xcom_key, value=str(save_path))
        return f"오른쪽 데이터 없음, 원본 유지 ({len(left_df)}행)"

    right_df = pd.read_parquet(right_path)

    # 3. Join 실행
    if on:
        joined_df = left_df.merge(right_df, on=on, how=how)
    elif left_on and right_on:
        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)
        
        # 4. 중복 키 컬럼 제거 (drop_right_keys=True 일 때)
        if drop_right_keys:
            # left_on과 right_on의 이름이 다를 경우에만 right_on 컬럼들을 삭제
            r_keys = [right_on] if isinstance(right_on, str) else right_on
            l_keys = [left_on] if isinstance(left_on, str) else left_on
            
            cols_to_drop = [c for c in r_keys if c in joined_df.columns and c not in l_keys]
            if cols_to_drop:
                joined_df.drop(columns=cols_to_drop, inplace=True)
                print(f"[INFO] 중복 방지를 위해 삭제된 오른쪽 키 컬럼: {cols_to_drop}")
    else:
        raise ValueError("조인을 위해 'on' 또는 'left_on/right_on' 매개변수가 필요합니다.")

    # 5. 결과 저장 및 XCom Push
    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    output_path = TEMP_DIR / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    joined_df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    print(f"[SUCCESS] Join 완료: {len(joined_df):,}행")
    return str(output_path)

# 마지막 전처리 df col정의
def transform_fin_df(df):
    # 전처리 시작
    df = df.copy()
    cols = ['order_daily',
    '매장명',
    '담당자',
    'email',
    'total_order_count',
    'total_amount',
    'fee_ad',
    'ARPU',
    'ma_14',
    'ma_28',
    'weekday',
    'prev_week_same_day',
    'current_avg_2week',
    'prev_2week_same_day',
    'prev_3week_same_day',
    'current_avg_4week',
    'sum_7d_recent',
    'sum_7d_prev',
    'score_trend',
    'score_total',
    'score_7d_total',
    'score_4week_total',
    'score',
    'status',
    'pre_status',
    'uploaded_at',
    '금일여부',
    '전일여부',
    '전일_매출',
    '전일_주문건수',
    '전일대비_증감액',
    '전일대비_증감률',
    '전주_매출',
    '전주_주문건수',
    '전주대비_증감액',
    '전주대비_증감률',
    '전월_매출',
    '전월_주문건수',
    '전월대비_증감액',
    '전월대비_증감률',
    '실오픈일',
    'platform_x',
    'settlement_amount',
    '일별_담당자별_평균',
    '일별_전체_평균',
    'vs일별전체_비율',
    'fee_ad_배민',
    'fee_ad_쿠팡',
    'settlement_amount_배민',
    'settlement_amount_쿠팡',
    'total_amount_배민',
    'total_amount_쿠팡',
    'total_order_count_배민',
    'total_order_count_쿠팡',
    '조리소요시간',
    '조리소요시간_순위비율',
    '주문접수시간',
    '주문접수시간_순위비율',
    '조리시간준수율',
    '조리시간준수율_순위비율',
    '주문접수율',
    '주문접수율_순위비율',
    '최근별점',
    '전체_주문수',
    '전체_리뷰수',
    '전체_답변완료수',
    '전체_평균별점',
    '영업임시중지 변경',
    '운영시간 변경',
    '휴무일 변경',
    '요일',
    '요일_한글',
    'order_week',
    'order_month',
    'join_pre_date',
    'join_pre_week_sameday',
    'join_pre_week',
    'join_pre_month',
    '전일_전체매출',
    '전일_전체주문건수',
    '전일_전체정산금액',
    '전일_전체매출_배민',
    '전일_전체매출_쿠팡',
    '전일_전체정산금액_배민',
    '전일_전체정산금액_쿠팡',
    '전일_전체주문건수_배민',
    '전일_전체주문건수_쿠팡',
    '전일_매장매출',
    '전일_매장주문건수',
    '전일_매장정산금액',
    '전일_매장매출_배민',
    '전일_매장매출_쿠팡',
    '전일_매장정산금액_배민',
    '전일_매장정산금액_쿠팡',
    '전일_매장주문건수_배민',
    '전일_매장주문건수_쿠팡',
    '전주동요일_전체매출',
    '전주동요일_전체주문건수',
    '전주동요일_전체정산금액',
    '전주동요일_전체매출_배민',
    '전주동요일_전체매출_쿠팡',
    '전주동요일_전체정산금액_배민',
    '전주동요일_전체정산금액_쿠팡',
    '전주동요일_전체주문건수_배민',
    '전주동요일_전체주문건수_쿠팡',
    '전주동요일_매장매출',
    '전주동요일_매장주문건수',
    '전주동요일_매장정산금액',
    '전주동요일_매장매출_배민',
    '전주동요일_매장매출_쿠팡',
    '전주동요일_매장정산금액_배민',
    '전주동요일_매장정산금액_쿠팡',
    '전주동요일_매장주문건수_배민',
    '전주동요일_매장주문건수_쿠팡',
    '전주_전체매출',
    '전주_전체주문건수',
    '전주_전체정산금액',
    '전주_전체매출_배민',
    '전주_전체매출_쿠팡',
    '전주_전체정산금액_배민',
    '전주_전체정산금액_쿠팡',
    '전주_전체주문건수_배민',
    '전주_전체주문건수_쿠팡',
    '전주_매장매출',
    '전주_매장주문건수',
    '전주_매장정산금액',
    '전주_매장매출_배민',
    '전주_매장매출_쿠팡',
    '전주_매장정산금액_배민',
    '전주_매장정산금액_쿠팡',
    '전주_매장주문건수_배민',
    '전주_매장주문건수_쿠팡',
    '전월_전체매출',
    '전월_전체주문건수',
    '전월_전체정산금액',
    '전월_전체매출_배민',
    '전월_전체매출_쿠팡',
    '전월_전체정산금액_배민',
    '전월_전체정산금액_쿠팡',
    '전월_전체주문건수_배민',
    '전월_전체주문건수_쿠팡',
    '전월_매장매출',
    '전월_매장주문건수',
    '전월_매장정산금액',
    '전월_매장매출_배민',
    '전월_매장매출_쿠팡',
    '전월_매장정산금액_배민',
    '전월_매장정산금액_쿠팡',
    '전월_매장주문건수_배민',
    '전월_매장주문건수_쿠팡',
    '수수료율',
    '수수료율_배민',
    '수수료율_쿠팡',
    '전일_수수료율',
    '전일_수수료율_배민',
    '전일_수수료율_쿠팡',
    '전주동요일_수수료율',
    '전주동요일_수수료율_배민',
    '전주동요일_수수료율_쿠팡',
    '전일대비_매출증감액',
    '전일대비_매출증감률',
    '전일대비_수수료율증감',
    '전일대비_매출증감률_배민',
    '전일대비_수수료율증감_배민',
    '전일대비_매출증감률_쿠팡',
    '전일대비_수수료율증감_쿠팡',
    '전주동요일대비_매출증감액',
    '전주동요일대비_매출증감률',
    '전주동요일대비_수수료율증감',
    '전주동요일대비_매출증감률_배민',
    '전주동요일대비_수수료율증감_배민',
    '전주동요일대비_매출증감률_쿠팡',
    '전주동요일대비_수수료율증감_쿠팡',
    '기간구분',
    'order_daily_day',
    'store_id',
    '캠페인정보',
    '변경시간',
    '수집주별',
    '변경전_희망가_최소',
    '변경전_희망가_최대',
    '변경전_희망가_평균',
    '변경후_희망가_최소',
    '변경후_희망가_최대',
    '변경후_희망가_평균',
    '조회일자',
    '광고비율',
    '광고비용',
    '신규고객',
    '광고주문수',
    '광고매출',
    '전체매출',
    '광고클릭수',
    '광고노출수',
    'store_names',
    'platform',
    '날짜',
    '광고지출',
    '노출수',
    '클릭수',
    '주문수',
    '주문금액',
    '광고효과']

    df = df[cols]
    return df
# baemin_ad_change_history_path
# baemin_ad_change_history_processed_path
def preprocess_fin_df(**context):
    """ 처리"""
    return preprocess_df(
        input_xcom_key='joined_orders_history_click_coupang_path',
        output_xcom_key='final_processed_path',
        transform_func=transform_fin_df,  # 위에서 정의한 함수
        #natural_keys=['date', 'stores_name'],  # ID 생성용
        **context
    )
    

def fin_save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename='sales_daily_orders_upload_fin.csv',
    output_subdir='영업관리부_DB',
    dedup_key=None,
    **context
):
    """Parquet 데이터를 로컬 DB에 CSV로 저장"""
    import os
    import shutil
    import tempfile
    from modules.transform.utility.paths import LOCAL_DB
    
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)
    
    if not parquet_path:
        print(f"[경고] 저장할 데이터 없음")
        return "⚠️ 저장 스킵: 데이터 없음"
    
    if not os.path.exists(parquet_path):
        print(f"[경고] 파일 경로 없음: {parquet_path}")
        return "⚠️ 저장 스킵: 파일 없음"
    
    df = pd.read_parquet(parquet_path)
    
    print(f"\n{'='*60}")
    print(f"[입력] 데이터: {len(df):,}행 × {len(df.columns)}컬럼")
    
    # 중복 제거
    if dedup_key:
        dedup_cols = [dedup_key] if isinstance(dedup_key, str) else dedup_key
        valid_cols = [c for c in dedup_cols if c in df.columns]
        if valid_cols:
            before = len(df)
            df.drop_duplicates(subset=valid_cols, keep='first', inplace=True)
            after = len(df)
            if before - after > 0:
                print(f"\n[중복 제거] {valid_cols} 기준: {before - after:,}건 제거")
    
    # 출력 경로
    if output_csv_path:
        local_csv_path = Path(output_csv_path)
    else:
        output_dir = LOCAL_DB / output_subdir
        output_dir.mkdir(parents=True, exist_ok=True)
        local_csv_path = output_dir / output_filename
    
    local_csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\n[경로] 저장 위치: {local_csv_path}")
    
    # datetime 변환
    datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
    if datetime_cols:
        for col in datetime_cols:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', 
            delete=False, 
            dir=str(local_csv_path.parent),
            prefix='tmp_', 
            suffix='.csv', 
            encoding='utf-8-sig'
        ) as tmp_file:
            tmp_path = tmp_file.name
        
        df.to_csv(tmp_path, index=False, encoding='utf-8-sig')
        
        if local_csv_path.exists():
            backup_path = local_csv_path.parent / f"{local_csv_path.name}.bak"
            shutil.copy2(local_csv_path, backup_path)
            shutil.move(tmp_path, str(local_csv_path))
            backup_path.unlink()
        else:
            shutil.move(tmp_path, str(local_csv_path))
        
        csv_size = local_csv_path.stat().st_size / (1024 * 1024)
        print(f"[저장] ✅ CSV 저장 완료: {len(df):,}건 ({csv_size:.2f} MB)")
        
    except Exception as e:
        print(f"[에러] CSV 저장 실패: {e}")
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        return f"저장 실패: {e}"
    
    print(f"{'='*60}\n")
    return f"✅ 저장 완료: {len(df):,}건"