"""
작업.py - 토더 리뷰 파이프라인
io3의 범용 함수를 wrapper로 감싸서 사용
"""

from pathlib import Path
from modules.transform.utility.paths import COLLECT_DB, DOWN_DIR, LOCAL_DB, TEMP_DIR, MART_DB
from modules.transform.utility.io import load_files, preprocess_df, save_to_csv, join_dataframes
from modules.transform.utility.store_name_mapping import normalize_store_names
import pandas as pd
import datetime as dt
import numpy as np
from modules.transform.pipelines.sales.SMD_07_store_ordesr_alert import (
    LLM_COLS,
    add_llm_columns_latest_per_store,
)


# ============================================================
# Excel 직렬값 날짜 변환 헬퍼 함수
# ============================================================
def parse_mixed_date(date_series, dayfirst=False):
    """
    Excel 직렬값과 텍스트 날짜를 모두 처리하는 범용 날짜 파서
    
    Args:
        date_series: 날짜가 포함된 pandas Series
        dayfirst: 텍스트 날짜 파싱 시 일-월-년 순서 여부
    
    Returns:
        pandas Series: datetime64 형식으로 변환된 날짜
    """
    result = pd.Series(pd.NaT, index=date_series.index)
    
    # 1단계: 이미 datetime인 경우 그대로 사용
    if pd.api.types.is_datetime64_any_dtype(date_series):
        return pd.to_datetime(date_series, errors='coerce')
    
    # 2단계: 문자열로 변환
    date_str = date_series.astype(str).str.strip()
    
    # 3단계: NaN, 빈 문자열, 'nan', 'NaT' 처리
    valid_mask = ~date_str.isin(['', 'nan', 'NaN', 'NaT', 'None'])
    
    if not valid_mask.any():
        return result
    
    valid_dates = date_str[valid_mask]
    
    # 4단계: 숫자인 경우 Excel 직렬값으로 처리
    numeric_mask = valid_dates.str.match(r'^\d+\.?\d*$', na=False)
    
    if numeric_mask.any():
        try:
            # Excel 직렬값은 1900-01-01부터의 일수
            # pandas의 to_datetime은 'excel' origin을 지원하지 않으므로 직접 계산
            excel_origin = pd.Timestamp('1899-12-30')  # Excel의 기준일 (1900-01-01이 1)
            numeric_values = pd.to_numeric(valid_dates[numeric_mask], errors='coerce')
            
            # Excel 직렬값이 합리적인 범위인지 확인 (1~100000 정도)
            reasonable_range = (numeric_values >= 1) & (numeric_values <= 100000)
            
            if reasonable_range.any():
                excel_dates = excel_origin + pd.to_timedelta(numeric_values[reasonable_range], unit='D')
                result.loc[valid_dates[numeric_mask][reasonable_range].index] = excel_dates
        except Exception as e:
            print(f"[경고] Excel 직렬값 변환 중 오류: {e}")
    
    # 5단계: 텍스트 날짜 처리 (아직 변환되지 않은 것들)
    remaining_mask = valid_mask & result.isna()
    
    if remaining_mask.any():
        try:
            text_dates = pd.to_datetime(
                date_str[remaining_mask],
                format='mixed',
                dayfirst=dayfirst,
                errors='coerce'
            )
            result.loc[remaining_mask] = text_dates
        except Exception as e:
            print(f"[경고] 텍스트 날짜 변환 중 오류: {e}")
    
    # 6단계: 유효성 검증 (1900-01-01 ~ 2100-12-31)
    min_valid = pd.Timestamp('1900-01-01')
    max_valid = pd.Timestamp('2100-12-31')
    invalid_mask = ((result < min_valid) | (result > max_valid)) & result.notna()
    
    if invalid_mask.any():
        invalid_count = invalid_mask.sum()
        print(f"[경고] {invalid_count}개 비정상 날짜 발견 (범위 밖) → NaT로 변환")
        result.loc[invalid_mask] = pd.NaT
    
    return result


def _append_llm_columns_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    print(f"\n[LLM] CSV 저장 전 llm_* 컬럼 생성 시작...")
    try:
        df = add_llm_columns_latest_per_store(df)
    except Exception as e:
        print(f"[경고] LLM 컬럼 생성 실패: {e}")

    for col in LLM_COLS:
        if col not in df.columns:
            df[col] = ''

    base_cols = [c for c in df.columns if c not in LLM_COLS]
    llm_cols = [c for c in LLM_COLS if c in df.columns]
    df = df[base_cols + llm_cols]
    print(f"[LLM] llm_* {len(llm_cols)}개 컬럼 준비 완료")
    return df



# baemin_ad_change_history / 변경이력 수집
def load_beamin_ad_change_history_df(**context):
    """변경이력 수집"""
    return load_files(
        patterns=[
            'baemin_ad_change_history_*.csv',
            'baemin_change_history_*.csv',
        ],
        search_paths=[
            DOWN_DIR / "업로드_temp",  # 업로드 임시 폴더 (Windows: E:/down/업로드_temp, Container: /opt/airflow/download/업로드_temp)
            DOWN_DIR,  # 일반 다운로드 폴더
            COLLECT_DB / "영업관리부_수집", # 수집 DB 폴더
            COLLECT_DB, # 루트에 있는 경우도 커버
        ],
        xcom_key='baemin_ad_change_history_path', # XCom 키
        file_type='auto',  # CSV 우선, 없으면 Excel
        **context
    )

# 변경 이력 전처리
def transform_beamin_ad_change_history_df(df):
    df = df.copy()

    # 최신 수집분 우선 정렬용 (겹치는 과거/최신 파일이 있을 때 최신 기준으로 덮어쓰기)
    # - 수집일시가 있으면 최우선 사용
    # - 없으면 파일명에 포함된 YYYYMMDD(예: *_20260420.csv)로 fallback
    df["_collected_at_parsed"] = pd.NaT
    if "수집일시" in df.columns:
        try:
            df["_collected_at_parsed"] = pd.to_datetime(df["수집일시"], errors="coerce")
        except Exception:
            df["_collected_at_parsed"] = pd.NaT

    df["_source_file_dt"] = pd.NaT
    if "_source_file" in df.columns:
        try:
            file_dates = df["_source_file"].astype(str).str.extract(r"(\d{8})(?:\D*\.(?:csv|xlsx|xls))?$", expand=False)
            df["_source_file_dt"] = pd.to_datetime(file_dates, format="%Y%m%d", errors="coerce")
        except Exception:
            df["_source_file_dt"] = pd.NaT

    # ⭐ 날짜 파싱을 먼저 수행 (Excel 직렬값·문자열 혼재 → 통일)
    # drop_duplicates 전에 파싱해야 동일 이벤트가 다른 포맷으로 기록된 경우에도 중복 제거 가능
    df["변경시간_parsed"] = parse_mixed_date(df["변경시간"])

    # 최신 기준으로 정렬 후 dedup (과거 0, 최신 10 -> 10)
    df = df.sort_values(
        by=["변경시간_parsed", "store_id", "_collected_at_parsed", "_source_file_dt"],
        ascending=True,
        na_position="first",
    )

    # 파싱된 datetime 기준으로 중복 제거
    df = df.drop_duplicates(subset=["변경시간_parsed", "store_id"], keep="last")

    df["수집주별"] = df["변경시간_parsed"].dt.to_period("W")
    df["platform"] = "배민"

    # join 키 (일자 + 매장)
    df["join_date"] = df["변경시간_parsed"].dt.to_period("D").dt.to_timestamp()
    if "매장명" in df.columns:
        df["store_names"] = df["매장명"].astype(str).str.split(" ").str[-2:].str.join(" ")
    else:
        df["store_names"] = ""

    # ------------------------------------------------------------
    # 1) 광고 캠페인 변경이력 (희망가) 스키마 처리
    # ------------------------------------------------------------
    ad_cols_present = ("변경전_희망가" in df.columns) and ("변경후_희망가" in df.columns)
    ad_out = None
    if ad_cols_present:
        df2 = df.groupby(["store_id", "매장명", "수집주별", "platform"]).agg(
            변경전_희망가_최소=("변경전_희망가", "min"),
            변경전_희망가_최대=("변경전_희망가", "max"),
            변경전_희망가_평균=("변경전_희망가", "mean"),
            변경후_희망가_최소=("변경후_희망가", "min"),
            변경후_희망가_최대=("변경후_희망가", "max"),
            변경후_희망가_평균=("변경후_희망가", "mean"),
        ).reset_index()

        base_cols = [c for c in ["수집일시", "매장명", "platform", "store_id", "캠페인정보", "변경시간", "수집주별", "join_date", "store_names"] if c in df.columns]
        ad_out = df[base_cols].copy()
        ad_out = pd.merge(ad_out, df2, on=["store_id", "매장명", "수집주별", "platform"], how="left")

        keep_cols = [c for c in [
            "join_date", "store_names", "store_id", "캠페인정보", "변경시간", "수집주별",
            "변경전_희망가_최소", "변경전_희망가_최대", "변경전_희망가_평균",
            "변경후_희망가_최소", "변경후_희망가_최대", "변경후_희망가_평균",
        ] if c in ad_out.columns]
        ad_out = ad_out[keep_cols]

        # ⭐ 같은 날짜+매장에 여러 변경이력이 있을 경우 최신 1건만 유지 (JOIN 시 fan-out 방지)
        ad_out = ad_out.sort_values(["join_date", "store_names", "_collected_at_parsed", "_source_file_dt"], ascending=True)
        ad_out = ad_out.drop_duplicates(subset=["join_date", "store_names"], keep="last")

    # ------------------------------------------------------------
    # 2) 운영 변경이력(영업임시중지/운영시간/휴무일) 스키마 처리
    # ------------------------------------------------------------
    ops_out = None
    if "대분류" in df.columns:
        ops = df[["join_date", "store_names", "store_id", "대분류", "_collected_at_parsed", "_source_file_dt"]].copy()
        ops["영업임시중지 변경"] = ops["대분류"].astype(str).str.contains("임시중지", na=False).astype(int)
        ops["운영시간 변경"] = (ops["대분류"].astype(str) == "운영시간 변경").astype(int)
        ops["휴무일 변경"] = (ops["대분류"].astype(str) == "휴무일 변경").astype(int)

        ops = ops.sort_values(["join_date", "store_names", "_collected_at_parsed", "_source_file_dt"], ascending=True)
        ops_out = ops.groupby(["join_date", "store_names"], as_index=False).agg(
            store_id=("store_id", "first"),
            영업임시중지_변경=("영업임시중지 변경", "max"),
            운영시간_변경=("운영시간 변경", "max"),
            휴무일_변경=("휴무일 변경", "max"),
        )
        ops_out.rename(
            columns={
                "영업임시중지_변경": "영업임시중지 변경",
                "운영시간_변경": "운영시간 변경",
                "휴무일_변경": "휴무일 변경",
            },
            inplace=True,
        )

    # ------------------------------------------------------------
    # 3) 결과 결합 (둘 다 있으면 merge)
    # ------------------------------------------------------------
    if ad_out is None and ops_out is None:
        # 최소 join 키라도 반환
        out = df[["join_date", "store_names", "store_id"]].drop_duplicates().copy()
    elif ad_out is None:
        out = ops_out
    elif ops_out is None:
        out = ad_out
    else:
        out = pd.merge(ad_out, ops_out, on=["join_date", "store_names"], how="outer", suffixes=("", "_ops"))
        # store_id는 ad_out을 우선, 없으면 ops쪽으로 채움
        if "store_id_ops" in out.columns:
            out["store_id"] = out["store_id"].fillna(out["store_id_ops"])
            out.drop(columns=["store_id_ops"], inplace=True)

    return out.drop(columns=[c for c in ["_collected_at_parsed", "_source_file_dt"] if c in out.columns])


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
            DOWN_DIR / "업로드_temp",
            DOWN_DIR,  # 일반 다운로드 폴더
            COLLECT_DB / "영업관리부_수집", # 수집 DB 폴더
            COLLECT_DB, # 루트에 있는 경우도 커버
        ],
        xcom_key='baemin_marketing_store_click_path', # XCom 키
        file_type='auto',  # CSV 우선, 없으면 Excel
        **context
    )
    
    
# 우리가게 클릭 전처리
def transform_store_click_df(df):
    click_df = df.copy()
    
    # collected_at을 datetime으로 변환하고 정렬
    click_df['collected_at'] = pd.to_datetime(click_df['collected_at'])
    click_df = click_df.sort_values('collected_at', ascending=True)
    
    # 정렬 후 중복 제거 (가장 최근 collected_at 데이터만 남김)
    click_df = click_df.drop_duplicates(subset=["날짜", "store_id"], keep="last")
    
    click_df["store_names"] = click_df["store_name"].str.split(" ").str[-2:].str.join(" ")
    
    # ⭐ Excel 직렬값 처리를 포함한 날짜 변환
    click_df["날짜_parsed"] = parse_mixed_date(click_df["날짜"], dayfirst=True)
    click_df["join_date"] = click_df["날짜_parsed"].dt.to_period("D")
    click_df["platform"] = "배민"

    click_df["join_date"] = click_df["join_date"].dt.to_timestamp()
    click_df = click_df[['join_date', 'store_names', 'platform', '날짜', '광고지출', '노출수', '클릭수',
        '주문수', '주문금액', '광고효과']]
    
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
            DOWN_DIR / "업로드_temp",
            DOWN_DIR,  # 일반 다운로드 폴더
            COLLECT_DB / "영업관리부_수집", # 수집 DB 폴더
            COLLECT_DB, # 루트에 있는 경우도 커버
        ],
        xcom_key='coupangeats_cmg_path', # XCom 키
        file_type='auto',  # CSV 우선, 없으면 Excel
        **context
    )
    
# 쿠팡 전처리
def transform_coupangeats_cmg_df(df):
    # 전처리 시작
    coupang_df = df.copy()

    # 최신 수집 파일 우선 정렬용 (파일명이 기간을 포함: *_20260406-20260419.csv 또는 *_20260406.csv)
    coupang_df["_source_file_end_dt"] = pd.NaT
    if "_source_file" in coupang_df.columns:
        try:
            end_dates = (
                coupang_df["_source_file"]
                .astype(str)
                .str.extract(r"(?:(?:\d{8})-(\d{8})|(\d{8}))(?:\D*\.(?:csv|xlsx|xls))?$")
            )
            # 그룹1(기간 end)이 우선, 없으면 그룹2(단일 날짜)
            end_date_str = end_dates[0].fillna(end_dates[1])
            coupang_df["_source_file_end_dt"] = pd.to_datetime(end_date_str, format="%Y%m%d", errors="coerce")
        except Exception:
            coupang_df["_source_file_end_dt"] = pd.NaT
    
    # ⭐ Excel 직렬값 처리를 포함한 날짜 변환
    coupang_df["조회일자_parsed"] = parse_mixed_date(coupang_df["조회일자"])

    # 최신 기준으로 정렬 후 dedup (과거 0, 최신 10 -> 10)
    coupang_df = coupang_df.sort_values(
        by=["조회일자_parsed", "매장명", "_source_file_end_dt"],
        ascending=True,
        na_position="first",
    )
    coupang_df = coupang_df.drop_duplicates(subset=["조회일자_parsed", "매장명"], keep="last")

    coupang_df["조회일자"] = coupang_df["조회일자_parsed"].dt.to_period("D")

    # 쿠팡 광고
    coupang_df["조회일자"] = coupang_df["조회일자"].dt.to_timestamp()
    coupang_df["store_names"] = coupang_df["매장명"].str.split(" ").str[-2:].str.join(" ")
    coupang_df["platform"] = "쿠팡이츠"
    coupang_df = coupang_df[['조회일자', 'store_names','platform', '광고비율', '광고비용', '신규고객',
        '광고주문수', '광고매출', '전체매출', '광고클릭수', '광고노출수']]
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
    
    # ⭐ Excel 직렬값 처리를 포함한 날짜 변환
    orders_upload_df["order_daily_day"] = parse_mixed_date(orders_upload_df["order_daily"]).dt.normalize()
    
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
    suffixes=('', '_right'),  # 중복 컬럼명 방지를 위한 suffix
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
        joined_df = left_df.merge(right_df, on=on, how=how, suffixes=suffixes)
    elif left_on and right_on:
        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how, suffixes=suffixes)
        
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
        r_keys = [right_on] if isinstance(right_on, str) else right_on
        l_keys = [left_on] if isinstance(left_on, str) else left_on
        r_keys_set = set(r_keys)

        # ⭐ suffix 충돌 방지: 이전 merge에서 누적된 right 키 컬럼을 left_df에서 제거
        left_stale = [c for c in r_keys if c in left_df.columns]
        if left_stale:
            left_df = left_df.drop(columns=left_stale)
            print(f"[INFO] left_df에서 이전 merge 누적 키 컬럼 제거: {left_stale}")

        # ⭐ suffix 충돌 방지: right_df에서 join key가 아닌 중복 컬럼 제거
        right_overlap = [c for c in right_df.columns if c in left_df.columns and c not in r_keys_set]
        if right_overlap:
            right_df = right_df.drop(columns=right_overlap)
            print(f"[INFO] right_df에서 left와 중복된 비-키 컬럼 제거: {right_overlap}")

        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)
        
        # 4. 중복 키 컬럼 제거 (drop_right_keys=True 일 때)
        if drop_right_keys:
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
        r_keys = [right_on] if isinstance(right_on, str) else right_on
        l_keys = [left_on] if isinstance(left_on, str) else left_on
        r_keys_set = set(r_keys)

        # ⭐ suffix 충돌 방지: 이전 merge에서 누적된 right 키 컬럼을 left_df에서 제거
        left_stale = [c for c in r_keys if c in left_df.columns]
        if left_stale:
            left_df = left_df.drop(columns=left_stale)
            print(f"[INFO] left_df에서 이전 merge 누적 키 컬럼 제거: {left_stale}")

        # ⭐ suffix 충돌 방지: right_df에서 join key가 아닌 중복 컬럼 제거
        right_overlap = [c for c in right_df.columns if c in left_df.columns and c not in r_keys_set]
        if right_overlap:
            right_df = right_df.drop(columns=right_overlap)
            print(f"[INFO] right_df에서 left와 중복된 비-키 컬럼 제거: {right_overlap}")

        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)
        
        # 4. 중복 키 컬럼 제거 (drop_right_keys=True 일 때)
        if drop_right_keys:
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
    """최종 점수 계산 전처리"""
    df = df.copy()
    
    # ============================================================
    # 1. 초기 정렬 및 기본 컬럼 생성
    # ============================================================
    df['order_daily'] = pd.to_datetime(df['order_daily'], errors='coerce')
    df = df.sort_values(['매장명', 'order_daily']).reset_index(drop=True)

    # ⭐ 일별 매장별 중복 제거 (LEFT JOIN fan-out 방지 안전장치)
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['order_daily', '매장명'], keep='last')
    after_dedup = len(df)
    if before_dedup - after_dedup > 0:
        print(f"[transform_fin_df] 중복 제거: {before_dedup - after_dedup:,}건 (order_daily+매장명 기준)")
        df = df.sort_values(['매장명', 'order_daily']).reset_index(drop=True)

    # ============================================================
    # 1.0. 결측 컬럼(전일/전주동요일) 보정
    # - 업스트림 스키마 변경/누락 시에도 DAG가 죽지 않도록 안전장치
    # - 중복 제거 이후의 정렬된 DF를 기준으로 shift 계산
    # ============================================================
    def _ensure_shifted_cols(group_key: str, mapping: dict[str, tuple[str, int]]) -> None:
        missing = [dst for dst in mapping.keys() if dst not in df.columns]
        if not missing:
            return

        if group_key not in df.columns:
            print(f"[WARNING][transform_fin_df] '{group_key}' 컬럼이 없어 전일/전주동요일 컬럼을 생성할 수 없습니다.")
            for dst in missing:
                df[dst] = np.nan
            return

        grp = df.groupby(group_key, sort=False)
        for dst, (src, shift_n) in mapping.items():
            if dst in df.columns:
                continue
            if src in df.columns:
                df[dst] = grp[src].shift(shift_n)
                print(f"[INFO][transform_fin_df] 누락 컬럼 생성: {dst} <= {src}.shift({shift_n})")
            else:
                df[dst] = np.nan
                print(f"[WARNING][transform_fin_df] '{src}' 컬럼이 없어 '{dst}'를 NaN으로 생성합니다.")

    _ensure_shifted_cols(
        group_key="매장명",
        mapping={
            # 전일 (t-1)
            "전일_매장매출": ("total_amount", 1),
            "전일_매장정산금액": ("settlement_amount", 1),
            "전일_매장매출_배민": ("total_amount_배민", 1),
            "전일_매장정산금액_배민": ("settlement_amount_배민", 1),
            # 전주동요일 (t-7)
            "전주동요일_매장매출": ("total_amount", 7),
            "전주동요일_매장정산금액": ("settlement_amount", 7),
            "전주동요일_매장매출_배민": ("total_amount_배민", 7),
            "전주동요일_매장정산금액_배민": ("settlement_amount_배민", 7),
        },
    )

    df['weekday'] = df['order_daily'].dt.dayofweek
    df['_order_month'] = df['order_daily'].dt.to_period('M')
    df['_order_day'] = df['order_daily'].dt.day

    # ============================================================
    # 1.1. 수수료율 재계산 - 광고지출
    # ============================================================

    # settlement_amount - 광고지출 (광고지출이 NaN인 경우 0으로 처리하여 NaN 전파 방지)
    df["settlement_amount"] = df["settlement_amount"] - df["광고지출"].fillna(0)

    # settlement_amount_배민 - 광고지출
    df["settlement_amount_배민"] = df["settlement_amount_배민"] - df["광고지출"].fillna(0)

    # 수수료율
    df["수수료율"] = ((df["total_amount"] - df["settlement_amount"]) / df["total_amount"] * 100).round(2)

    # 수수료율_배민
    df["수수료율_배민"] = ((df["total_amount_배민"] - df["settlement_amount_배민"]) / df["total_amount_배민"] * 100).round(2)

    # 배민 광고비율 (현재 기준)
    ad_ratio_baemin = (df["광고지출"] / df["total_amount_배민"].replace(0, np.nan)).fillna(0)

    # 전일_수수료율 (인라인 보정)
    df["전일_수수료율"] = (
        (df["전일_매장매출"] - (df["전일_매장정산금액"] - df["전일_매장매출"] * ad_ratio_baemin))
        / df["전일_매장매출"].replace(0, np.nan) * 100
    ).round(2)

    # 전일_수수료율_배민 (인라인 보정)
    df["전일_수수료율_배민"] = (
        (df["전일_매장매출_배민"] - (df["전일_매장정산금액_배민"] - df["전일_매장매출_배민"] * ad_ratio_baemin))
        / df["전일_매장매출_배민"].replace(0, np.nan) * 100
    ).round(2)

    # 전주동요일_수수료율 (인라인 보정)
    df["전주동요일_수수료율"] = (
        (df["전주동요일_매장매출"] - (df["전주동요일_매장정산금액"] - df["전주동요일_매장매출"] * ad_ratio_baemin))
        / df["전주동요일_매장매출"].replace(0, np.nan) * 100
    ).round(2)

    # 전주동요일_수수료율_배민 (인라인 보정)
    df["전주동요일_수수료율_배민"] = (
        (df["전주동요일_매장매출_배민"] - (df["전주동요일_매장정산금액_배민"] - df["전주동요일_매장매출_배민"] * ad_ratio_baemin))
        / df["전주동요일_매장매출_배민"].replace(0, np.nan) * 100
    ).round(2)

    # 전일대비_수수료율증감
    df["전일대비_수수료율증감"] = (df["수수료율"] - df["전일_수수료율"]).round(2)

    # 전일대비_수수료율증감_배민
    df["전일대비_수수료율증감_배민"] = (df["수수료율_배민"] - df["전일_수수료율_배민"]).round(2)

    # 전주동요일대비_수수료율증감
    df["전주동요일대비_수수료율증감"] = (df["수수료율"] - df["전주동요일_수수료율"]).round(2)

    # 전주동요일대비_수수료율증감_배민
    df["전주동요일대비_수수료율증감_배민"] = (df["수수료율_배민"] - df["전주동요일_수수료율_배민"]).round(2)
 

    # ============================================================
    # 2. 수수료율 계산
    # ============================================================
    df["fee_ratio"] = np.where(
        df["total_amount"] > 0,
        ((df["total_amount"] - df["settlement_amount"]) / df["total_amount"]).round(4),
        np.nan
    )
    
    # 14일/28일 이동평균
    df['fee_ratio_MA14'] = df.groupby('매장명')['fee_ratio'].transform(
        lambda x: x.rolling(window=14, min_periods=7).mean()
    ).round(4)
    df['fee_ratio_MA28'] = df.groupby('매장명')['fee_ratio'].transform(
        lambda x: x.rolling(window=28, min_periods=14).mean()
    ).round(4)
    
    # 당월 MTD 수수료율은 일별 fee_ratio 평균이 아니라 누적 수수료 / 누적 매출로 계산
    df['_fee_amount'] = (df['total_amount'] - df['settlement_amount']).fillna(0)
    df['_fee_amount_mtd_curr_month'] = df.groupby(['매장명', '_order_month'])['_fee_amount'].cumsum()
    df['_total_amount_mtd_curr_month'] = df.groupby(['매장명', '_order_month'])['total_amount'].cumsum()
    df['fee_ratio_mtd_curr_month'] = np.where(
        df['_total_amount_mtd_curr_month'] > 0,
        (df['_fee_amount_mtd_curr_month'] / df['_total_amount_mtd_curr_month']).round(4),
        np.nan
    )
    
    # 최근 7일 / 직전 7일 평균
    df['avg_7d_recent'] = df.groupby('매장명')['fee_ratio'].transform(
        lambda s: s.rolling(window=7, min_periods=1).mean()
    ).round(4)
    df['avg_7d_prev'] = df.groupby('매장명')['fee_ratio'].transform(
        lambda s: s.shift(7).rolling(window=7, min_periods=1).mean()
    ).round(4)
    
    # 수수료율 점수 계산
    cond_trand = (df['fee_ratio_MA14'] - df['fee_ratio_MA28']) / df['fee_ratio_MA28'].replace(0, np.nan)
    df['fee_trand_score'] = np.where(cond_trand > 0.1, 2, np.where(cond_trand > 0, 1, 0))
    
    cond_avg_7d = (df['avg_7d_recent'] - df['avg_7d_prev']) / df['avg_7d_prev'].replace(0, np.nan)
    df['fee_avg_7d_score'] = np.where(cond_avg_7d > 0.1, 2, np.where(cond_avg_7d > 0, 1, 0))
    
    # fee_month_score: 당월 MTD vs 전월 동일일자 MTD (증가=나쁨)
    _lkp = df[['매장명', '_order_month', '_order_day', 'fee_ratio_mtd_curr_month']].copy()
    _lkp['_jm'] = _lkp['_order_month'] + 1
    _lkp = _lkp.rename(columns={'fee_ratio_mtd_curr_month': 'fee_ratio_mtd_prev_month_same_day'})
    df = df.merge(_lkp[['매장명', '_jm', '_order_day', 'fee_ratio_mtd_prev_month_same_day']],
                  left_on=['매장명', '_order_month', '_order_day'],
                  right_on=['매장명', '_jm', '_order_day'], how='left').drop(columns=['_jm'])
    df['fee_ratio_mtd_prev_month_same_day'] = df['fee_ratio_mtd_prev_month_same_day'].fillna(0)
    cond_month = (df['fee_ratio_mtd_curr_month'] - df['fee_ratio_mtd_prev_month_same_day']) / df['fee_ratio_mtd_prev_month_same_day'].replace(0, np.nan)
    df['fee_month_score'] = np.where(cond_month > 0.1, 2, np.where(cond_month > 0, 1, 0))
    
    # fee_score_total: 트렌드+7일+월간 = 6점 만점
    df["fee_score_total"] = df['fee_trand_score'] + df['fee_avg_7d_score'] + df['fee_month_score']
    df['fee_status'] = np.where(df["fee_score_total"] >= 5, '위험', 
                                np.where(df["fee_score_total"] >= 3, '주의',
                                np.where(df["fee_score_total"] >= 2, '관심', '정상')))
    df["pre_fee_status"] = df.groupby(["매장명"])["fee_status"].shift(7).fillna('정상')

    # 수수료 MTD 계산용 임시 컬럼 제거
    df.drop(columns=['_fee_amount', '_fee_amount_mtd_curr_month', '_total_amount_mtd_curr_month'], inplace=True)
    
    # ============================================================
    # 3. 쿠팡 광고효과 계산
    # ============================================================
    df["쿠팡_광고효과"] = (df["광고클릭수"] / df["광고노출수"].replace(0, np.nan)).round(4)
    
    # 14일/28일 이동평균
    df['쿠팡_광고효과_MA14'] = df.groupby('매장명')['쿠팡_광고효과'].transform(
        lambda x: x.rolling(window=14, min_periods=7).mean()
    ).round(4)
    df['쿠팡_광고효과_MA28'] = df.groupby('매장명')['쿠팡_광고효과'].transform(
        lambda x: x.rolling(window=28, min_periods=14).mean()
    ).round(4)
    
    # 당월 MTD 광고효과는 일별 비율 평균이 아니라 누적 클릭수 / 누적 노출수로 계산
    df['_쿠팡_광고클릭수_mtd_curr_month'] = df.groupby(['매장명', '_order_month'])['광고클릭수'].cumsum()
    df['_쿠팡_광고노출수_mtd_curr_month'] = df.groupby(['매장명', '_order_month'])['광고노출수'].cumsum()
    df['쿠팡_광고효과_mtd_curr_month'] = np.where(
        df['_쿠팡_광고노출수_mtd_curr_month'] > 0,
        (df['_쿠팡_광고클릭수_mtd_curr_month'] / df['_쿠팡_광고노출수_mtd_curr_month']).round(4),
        np.nan
    )
    
    # 최근 7일 / 직전 7일 평균
    df['쿠팡_광고효과_avg_7d_recent'] = df.groupby('매장명')['쿠팡_광고효과'].transform(
        lambda s: s.rolling(window=7, min_periods=1).mean()
    ).round(4)
    df['쿠팡_광고효과_avg_7d_prev'] = df.groupby('매장명')['쿠팡_광고효과'].transform(
        lambda s: s.shift(7).rolling(window=7, min_periods=1).mean()
    ).round(4)
    
    # 쿠팡 광고효과 점수 계산 (감소 = 나쁨)
    coupang_cond_trand = (df['쿠팡_광고효과_MA14'] - df['쿠팡_광고효과_MA28']) / df['쿠팡_광고효과_MA28'].replace(0, np.nan)
    df['쿠팡_광고효과_trand_score'] = np.where(coupang_cond_trand < -0.1, 2, np.where(coupang_cond_trand < 0, 1, 0))
    
    coupang_cond_avg_7d = (df['쿠팡_광고효과_avg_7d_recent'] - df['쿠팡_광고효과_avg_7d_prev']) / df['쿠팡_광고효과_avg_7d_prev'].replace(0, np.nan)
    df['쿠팡_광고효과_avg_7d_score'] = np.where(coupang_cond_avg_7d < -0.1, 2, np.where(coupang_cond_avg_7d < 0, 1, 0))
    
    # 쿠팡_광고효과_month_score: 당월 MTD vs 전월 동일일자 MTD (감소=나쁨)
    _lkp = df[['매장명', '_order_month', '_order_day', '쿠팡_광고효과_mtd_curr_month']].copy()
    _lkp['_jm'] = _lkp['_order_month'] + 1
    _lkp = _lkp.rename(columns={'쿠팡_광고효과_mtd_curr_month': '쿠팡_광고효과_mtd_prev_month_same_day'})
    df = df.merge(_lkp[['매장명', '_jm', '_order_day', '쿠팡_광고효과_mtd_prev_month_same_day']],
                  left_on=['매장명', '_order_month', '_order_day'],
                  right_on=['매장명', '_jm', '_order_day'], how='left').drop(columns=['_jm'])
    df['쿠팡_광고효과_mtd_prev_month_same_day'] = df['쿠팡_광고효과_mtd_prev_month_same_day'].fillna(0)
    coupang_cond_month = (df['쿠팡_광고효과_mtd_curr_month'] - df['쿠팡_광고효과_mtd_prev_month_same_day']) / df['쿠팡_광고효과_mtd_prev_month_same_day'].replace(0, np.nan)
    df['쿠팡_광고효과_month_score'] = np.where(coupang_cond_month < -0.1, 2, np.where(coupang_cond_month < 0, 1, 0))
    
    # 쿠팡_광고효과_score_total: 트렌드+7일+월간 = 6점 만점
    df["쿠팡_광고효과_score_total"] = (df['쿠팡_광고효과_trand_score'] + 
                                      df['쿠팡_광고효과_avg_7d_score'] + df['쿠팡_광고효과_month_score'])
    df['쿠팡_광고효과_status'] = np.where(df["쿠팡_광고효과_score_total"] >= 5, '위험', 
                                        np.where(df["쿠팡_광고효과_score_total"] >= 3, '주의',
                                        np.where(df["쿠팡_광고효과_score_total"] >= 2, '관심', '정상')))
    df["pre_쿠팡_광고효과_status"] = df.groupby(["매장명"])["쿠팡_광고효과_status"].shift(7).fillna('정상')

    # 쿠팡 광고효과 MTD 계산용 임시 컬럼 제거
    df.drop(columns=['_쿠팡_광고클릭수_mtd_curr_month', '_쿠팡_광고노출수_mtd_curr_month'], inplace=True)
    
    # ============================================================
    # 4. 배민 광고효과 계산
    # ============================================================
    df["배민_광고효과"] = (df["클릭수"] / df["노출수"].replace(0, np.nan)).round(4)
    
    # 14일/28일 이동평균
    df['배민_광고효과_MA14'] = df.groupby('매장명')['배민_광고효과'].transform(
        lambda x: x.rolling(window=14, min_periods=7).mean()
    ).round(4)
    df['배민_광고효과_MA28'] = df.groupby('매장명')['배민_광고효과'].transform(
        lambda x: x.rolling(window=28, min_periods=14).mean()
    ).round(4)
    
    # 당월 MTD 광고효과는 일별 비율 평균이 아니라 누적 클릭수 / 누적 노출수로 계산
    df['_배민_클릭수_mtd_curr_month'] = df.groupby(['매장명', '_order_month'])['클릭수'].cumsum()
    df['_배민_노출수_mtd_curr_month'] = df.groupby(['매장명', '_order_month'])['노출수'].cumsum()
    df['배민_광고효과_mtd_curr_month'] = np.where(
        df['_배민_노출수_mtd_curr_month'] > 0,
        (df['_배민_클릭수_mtd_curr_month'] / df['_배민_노출수_mtd_curr_month']).round(4),
        np.nan
    )
    
    # 최근 7일 / 직전 7일 평균
    df['배민_광고효과_avg_7d_recent'] = df.groupby('매장명')['배민_광고효과'].transform(
        lambda s: s.rolling(window=7, min_periods=1).mean()
    ).round(4)
    df['배민_광고효과_avg_7d_prev'] = df.groupby('매장명')['배민_광고효과'].transform(
        lambda s: s.shift(7).rolling(window=7, min_periods=1).mean()
    ).round(4)
    
    # 배민 광고효과 점수 계산 (감소 = 나쁨)
    baemin_cond_trand = (df['배민_광고효과_MA14'] - df['배민_광고효과_MA28']) / df['배민_광고효과_MA28'].replace(0, np.nan)
    df['배민_광고효과_trand_score'] = np.where(baemin_cond_trand < -0.1, 2, np.where(baemin_cond_trand < 0, 1, 0))
    
    baemin_cond_avg_7d = (df['배민_광고효과_avg_7d_recent'] - df['배민_광고효과_avg_7d_prev']) / df['배민_광고효과_avg_7d_prev'].replace(0, np.nan)
    df['배민_광고효과_avg_7d_score'] = np.where(baemin_cond_avg_7d < -0.1, 2, np.where(baemin_cond_avg_7d < 0, 1, 0))
    
    # 배민_광고효과_month_score: 당월 MTD vs 전월 동일일자 MTD (감소=나쁨)
    _lkp = df[['매장명', '_order_month', '_order_day', '배민_광고효과_mtd_curr_month']].copy()
    _lkp['_jm'] = _lkp['_order_month'] + 1
    _lkp = _lkp.rename(columns={'배민_광고효과_mtd_curr_month': '배민_광고효과_mtd_prev_month_same_day'})
    df = df.merge(_lkp[['매장명', '_jm', '_order_day', '배민_광고효과_mtd_prev_month_same_day']],
                  left_on=['매장명', '_order_month', '_order_day'],
                  right_on=['매장명', '_jm', '_order_day'], how='left').drop(columns=['_jm'])
    df['배민_광고효과_mtd_prev_month_same_day'] = df['배민_광고효과_mtd_prev_month_same_day'].fillna(0)
    baemin_cond_month = (df['배민_광고효과_mtd_curr_month'] - df['배민_광고효과_mtd_prev_month_same_day']) / df['배민_광고효과_mtd_prev_month_same_day'].replace(0, np.nan)
    df['배민_광고효과_month_score'] = np.where(baemin_cond_month < -0.1, 2, np.where(baemin_cond_month < 0, 1, 0))
    
    # 배민_광고효과_score_total: 트렌드+7일+월간 = 6점 만점
    df["배민_광고효과_score_total"] = (df['배민_광고효과_trand_score'] + 
                                      df['배민_광고효과_avg_7d_score'] + df['배민_광고효과_month_score'])
    df['배민_광고효과_status'] = np.where(df["배민_광고효과_score_total"] >= 5, '위험', 
                                        np.where(df["배민_광고효과_score_total"] >= 3, '주의',
                                        np.where(df["배민_광고효과_score_total"] >= 2, '관심', '정상')))
    df["pre_배민_광고효과_status"] = df.groupby(["매장명"])["배민_광고효과_status"].shift(7).fillna('정상')

    # 배민 광고효과 MTD 계산용 임시 컬럼 제거
    df.drop(columns=['_배민_클릭수_mtd_curr_month', '_배민_노출수_mtd_curr_month'], inplace=True)
    
    # ============================================================
    # 5. 우리가게now - 조리시간 (증가 = 나쁨)
    # ============================================================
    df['조리시간_MA14'] = df.groupby('매장명')['조리소요시간'].transform(
        lambda x: x.rolling(window=14, min_periods=7).mean()
    ).round(2)
    df['조리시간_MA28'] = df.groupby('매장명')['조리소요시간'].transform(
        lambda x: x.rolling(window=28, min_periods=14).mean()
    ).round(2)
    
    # 조리시간_mtd_curr_month: 당월 MTD 누적 평균
    df['조리시간_mtd_curr_month'] = df.groupby(['매장명', '_order_month'])['조리소요시간'].transform(
        lambda x: x.expanding().mean()
    ).round(2)
    
    df['조리시간_7d_recent'] = df.groupby('매장명')['조리소요시간'].transform(
        lambda s: s.rolling(window=7, min_periods=1).mean()
    ).round(2)
    df['조리시간_7d_prev'] = df.groupby('매장명')['조리소요시간'].transform(
        lambda s: s.shift(7).rolling(window=7, min_periods=1).mean()
    ).round(2)
    
    # 조리시간 점수
    cond = (df['조리시간_MA14'] - df['조리시간_MA28']) / df['조리시간_MA28'].replace(0, np.nan)
    df['조리시간_trend_score'] = np.where(cond > 0.1, 2, np.where(cond > 0, 1, 0))
    
    cond = (df['조리시간_7d_recent'] - df['조리시간_7d_prev']) / df['조리시간_7d_prev'].replace(0, np.nan)
    df['조리시간_weekly_score'] = np.where(cond > 0.1, 2, np.where(cond > 0, 1, 0))
    
    # 조리시간_monthly_score: 당월 MTD vs 전월 동일일자 MTD (증가=나쁨)
    _lkp = df[['매장명', '_order_month', '_order_day', '조리시간_mtd_curr_month']].copy()
    _lkp['_jm'] = _lkp['_order_month'] + 1
    _lkp = _lkp.rename(columns={'조리시간_mtd_curr_month': '조리시간_mtd_prev_month_same_day'})
    df = df.merge(_lkp[['매장명', '_jm', '_order_day', '조리시간_mtd_prev_month_same_day']],
                  left_on=['매장명', '_order_month', '_order_day'],
                  right_on=['매장명', '_jm', '_order_day'], how='left').drop(columns=['_jm'])
    df['조리시간_mtd_prev_month_same_day'] = df['조리시간_mtd_prev_month_same_day'].fillna(0)
    cond = (df['조리시간_mtd_curr_month'] - df['조리시간_mtd_prev_month_same_day']) / df['조리시간_mtd_prev_month_same_day'].replace(0, np.nan)
    df['조리시간_monthly_score'] = np.where(cond > 0.1, 2, np.where(cond > 0, 1, 0))
    
    # 조리시간_total: 트렌드+주간+월간 = 6점 만점
    df['조리시간_total'] = (df['조리시간_trend_score'] + 
                          df['조리시간_weekly_score'] + df['조리시간_monthly_score'])
    
    # ============================================================
    # 6. 우리가게now - 접수시간 (증가 = 나쁨)
    # ============================================================
    df['접수시간_MA14'] = df.groupby('매장명')['주문접수시간'].transform(
        lambda x: x.rolling(window=14, min_periods=7).mean()
    ).round(2)
    df['접수시간_MA28'] = df.groupby('매장명')['주문접수시간'].transform(
        lambda x: x.rolling(window=28, min_periods=14).mean()
    ).round(2)
    
    # 접수시간_mtd_curr_month: 당월 MTD 누적 평균
    df['접수시간_mtd_curr_month'] = df.groupby(['매장명', '_order_month'])['주문접수시간'].transform(
        lambda x: x.expanding().mean()
    ).round(2)
    
    df['접수시간_7d_recent'] = df.groupby('매장명')['주문접수시간'].transform(
        lambda s: s.rolling(window=7, min_periods=1).mean()
    ).round(2)
    df['접수시간_7d_prev'] = df.groupby('매장명')['주문접수시간'].transform(
        lambda s: s.shift(7).rolling(window=7, min_periods=1).mean()
    ).round(2)
    
    # 접수시간 점수
    cond = (df['접수시간_MA14'] - df['접수시간_MA28']) / df['접수시간_MA28'].replace(0, np.nan)
    df['접수시간_trend_score'] = np.where(cond > 0.1, 2, np.where(cond > 0, 1, 0))
    
    cond = (df['접수시간_7d_recent'] - df['접수시간_7d_prev']) / df['접수시간_7d_prev'].replace(0, np.nan)
    df['접수시간_weekly_score'] = np.where(cond > 0.1, 2, np.where(cond > 0, 1, 0))
    
    # 접수시간_monthly_score: 당월 MTD vs 전월 동일일자 MTD (증가=나쁨)
    _lkp = df[['매장명', '_order_month', '_order_day', '접수시간_mtd_curr_month']].copy()
    _lkp['_jm'] = _lkp['_order_month'] + 1
    _lkp = _lkp.rename(columns={'접수시간_mtd_curr_month': '접수시간_mtd_prev_month_same_day'})
    df = df.merge(_lkp[['매장명', '_jm', '_order_day', '접수시간_mtd_prev_month_same_day']],
                  left_on=['매장명', '_order_month', '_order_day'],
                  right_on=['매장명', '_jm', '_order_day'], how='left').drop(columns=['_jm'])
    df['접수시간_mtd_prev_month_same_day'] = df['접수시간_mtd_prev_month_same_day'].fillna(0)
    cond = (df['접수시간_mtd_curr_month'] - df['접수시간_mtd_prev_month_same_day']) / df['접수시간_mtd_prev_month_same_day'].replace(0, np.nan)
    df['접수시간_monthly_score'] = np.where(cond > 0.1, 2, np.where(cond > 0, 1, 0))
    
    # 접수시간_total: 트렌드+주간+월간 = 6점 만점
    df['접수시간_total'] = (df['접수시간_trend_score'] + 
                          df['접수시간_weekly_score'] + df['접수시간_monthly_score'])
    
    # ============================================================
    # 7. 우리가게now - 재주문율 (감소 = 나쁨)
    # ============================================================
    df['재주문율_MA14'] = df.groupby('매장명')['최근재주문율'].transform(
        lambda x: x.rolling(window=14, min_periods=7).mean()
    ).round(4)
    df['재주문율_MA28'] = df.groupby('매장명')['최근재주문율'].transform(
        lambda x: x.rolling(window=28, min_periods=14).mean()
    ).round(4)
    
    # 재주문율_mtd_curr_month: 당월 MTD 누적 평균
    df['재주문율_mtd_curr_month'] = df.groupby(['매장명', '_order_month'])['최근재주문율'].transform(
        lambda x: x.expanding().mean()
    ).round(4)
    
    df['재주문율_7d_recent'] = df.groupby('매장명')['최근재주문율'].transform(
        lambda s: s.rolling(window=7, min_periods=1).mean()
    ).round(4)
    df['재주문율_7d_prev'] = df.groupby('매장명')['최근재주문율'].transform(
        lambda s: s.shift(7).rolling(window=7, min_periods=1).mean()
    ).round(4)
    
    # 재주문율 점수
    cond = (df['재주문율_MA14'] - df['재주문율_MA28']) / df['재주문율_MA28'].replace(0, np.nan)
    df['재주문율_trend_score'] = np.where(cond < -0.1, 2, np.where(cond < 0, 1, 0))
    
    cond = (df['재주문율_7d_recent'] - df['재주문율_7d_prev']) / df['재주문율_7d_prev'].replace(0, np.nan)
    df['재주문율_weekly_score'] = np.where(cond < -0.1, 2, np.where(cond < 0, 1, 0))
    
    # 재주문율_monthly_score: 당월 MTD vs 전월 동일일자 MTD (감소=나쁨)
    _lkp = df[['매장명', '_order_month', '_order_day', '재주문율_mtd_curr_month']].copy()
    _lkp['_jm'] = _lkp['_order_month'] + 1
    _lkp = _lkp.rename(columns={'재주문율_mtd_curr_month': '재주문율_mtd_prev_month_same_day'})
    df = df.merge(_lkp[['매장명', '_jm', '_order_day', '재주문율_mtd_prev_month_same_day']],
                  left_on=['매장명', '_order_month', '_order_day'],
                  right_on=['매장명', '_jm', '_order_day'], how='left').drop(columns=['_jm'])
    df['재주문율_mtd_prev_month_same_day'] = df['재주문율_mtd_prev_month_same_day'].fillna(0)
    cond = (df['재주문율_mtd_curr_month'] - df['재주문율_mtd_prev_month_same_day']) / df['재주문율_mtd_prev_month_same_day'].replace(0, np.nan)
    df['재주문율_monthly_score'] = np.where(cond < -0.1, 2, np.where(cond < 0, 1, 0))
    
    # 재주문율_total: 트렌드+주간+월간 = 6점 만점
    df['재주문율_total'] = (df['재주문율_trend_score'] + 
                          df['재주문율_weekly_score'] + df['재주문율_monthly_score'])
    
    # ============================================================
    # 8. 우리가게now - 별점 (감소 = 나쁨)
    # ============================================================
    df['별점_MA14'] = df.groupby('매장명')['최근별점'].transform(
        lambda x: x.rolling(window=14, min_periods=7).mean()
    ).round(2)
    df['별점_MA28'] = df.groupby('매장명')['최근별점'].transform(
        lambda x: x.rolling(window=28, min_periods=14).mean()
    ).round(2)
    
    # 별점_mtd_curr_month: 당월 MTD 누적 평균
    df['별점_mtd_curr_month'] = df.groupby(['매장명', '_order_month'])['최근별점'].transform(
        lambda x: x.expanding().mean()
    ).round(2)
    
    df['별점_7d_recent'] = df.groupby('매장명')['최근별점'].transform(
        lambda s: s.rolling(window=7, min_periods=1).mean()
    ).round(2)
    df['별점_7d_prev'] = df.groupby('매장명')['최근별점'].transform(
        lambda s: s.shift(7).rolling(window=7, min_periods=1).mean()
    ).round(2)
    
    # 별점 점수
    cond = (df['별점_MA14'] - df['별점_MA28']) / df['별점_MA28'].replace(0, np.nan)
    df['별점_trend_score'] = np.where(cond < -0.1, 2, np.where(cond < 0, 1, 0))
    
    cond = (df['별점_7d_recent'] - df['별점_7d_prev']) / df['별점_7d_prev'].replace(0, np.nan)
    df['별점_weekly_score'] = np.where(cond < -0.1, 2, np.where(cond < 0, 1, 0))
    
    # 별점_monthly_score: 당월 MTD vs 전월 동일일자 MTD (감소=나쁨)
    _lkp = df[['매장명', '_order_month', '_order_day', '별점_mtd_curr_month']].copy()
    _lkp['_jm'] = _lkp['_order_month'] + 1
    _lkp = _lkp.rename(columns={'별점_mtd_curr_month': '별점_mtd_prev_month_same_day'})
    df = df.merge(_lkp[['매장명', '_jm', '_order_day', '별점_mtd_prev_month_same_day']],
                  left_on=['매장명', '_order_month', '_order_day'],
                  right_on=['매장명', '_jm', '_order_day'], how='left').drop(columns=['_jm'])
    df['별점_mtd_prev_month_same_day'] = df['별점_mtd_prev_month_same_day'].fillna(0)
    cond = (df['별점_mtd_curr_month'] - df['별점_mtd_prev_month_same_day']) / df['별점_mtd_prev_month_same_day'].replace(0, np.nan)
    df['별점_monthly_score'] = np.where(cond < -0.1, 2, np.where(cond < 0, 1, 0))
    
    # 별점_total: 트렌드+주간+월간 = 6점 만점
    df['별점_total'] = (df['별점_trend_score'] + 
                       df['별점_weekly_score'] + df['별점_monthly_score'])
    
    
    # 최종 parquet에서 제거할 순수 내부 임시 컬럼
    # (FINAL_COLUMNS 화이트리스트에서 걸러지지만, parquet 크기 절감을 위해 여기서도 제거)
    drop_columns = [
        'weekday',
        # 수수료율 비교 원본 — 증감/스코어 컬럼만 남김
        '전일_수수료율', '전일_수수료율_배민',
        '전주동요일_수수료율', '전주동요일_수수료율_배민',
        '전일대비_수수료율증감', '전일대비_수수료율증감_배민',
        '전주동요일대비_수수료율증감', '전주동요일대비_수수료율증감_배민',
        # 내부 날짜 보조 컬럼
        '_order_month', '_order_day',
        # 내부 join 키 / 기간 보조 컬럼
        'join_pre_date', 'join_pre_week_sameday',
        'order_week', 'order_month',
        # 내부 추적 컬럼 (수집주별은 최종 출력에 포함 → 제외)
        '_source_file', 'order_daily_day', 'store_id', 'join_date',
        # 전일_전체 레벨 원본값 (비교 원본값 — 증감률/스코어만 유지)
        '전일_전체매출', '전일_전체주문건수', '전일_전체정산금액',
        '전일_전체매출_배민', '전일_전체매출_쿠팡',
        '전일_전체정산금액_배민', '전일_전체정산금액_쿠팡',
        '전일_전체주문건수_배민', '전일_전체주문건수_쿠팡',
        # 전일_매장 레벨
        '전일_매장매출', '전일_매장주문건수', '전일_매장정산금액',
        '전일_매장매출_배민', '전일_매장매출_쿠팡',
        '전일_매장정산금액_배민', '전일_매장정산금액_쿠팡',
        '전일_매장주문건수_배민', '전일_매장주문건수_쿠팡',
        # 전주동요일_전체 레벨
        '전주동요일_전체매출', '전주동요일_전체주문건수', '전주동요일_전체정산금액',
        '전주동요일_전체매출_배민', '전주동요일_전체매출_쿠팡',
        '전주동요일_전체정산금액_배민', '전주동요일_전체정산금액_쿠팡',
        '전주동요일_전체주문건수_배민', '전주동요일_전체주문건수_쿠팡',
        # 전주동요일_매장 레벨
        '전주동요일_매장매출', '전주동요일_매장주문건수', '전주동요일_매장정산금액',
        '전주동요일_매장매출_배민', '전주동요일_매장매출_쿠팡',
        '전주동요일_매장정산금액_배민', '전주동요일_매장정산금액_쿠팡',
        '전주동요일_매장주문건수_배민', '전주동요일_매장주문건수_쿠팡',
    ]
    df.drop(columns=[c for c in drop_columns if c in df.columns], inplace=True)

    # ⭐ 중앙 매핑으로 통합 (store_name_mapping.py에서 관리)
    df["store_names"] = normalize_store_names(df["store_names"])
        
    # ============================================================
    # 9. 서비스 종합 점수 및 상태 분류
    # ============================================================
    df['service_score_total'] = (df['조리시간_total'] + df['접수시간_total'] + 
                                 df['재주문율_total'] + df['별점_total'])
    
    # service_status: 각 항목 6점 만점 × 4 = 24점 만점 기준
    df['service_status'] = np.where(
        df['service_score_total'] >= 15, '위험',
        np.where(df['service_score_total'] >= 10, '주의',
        np.where(df['service_score_total'] >= 5, '관심', '정상'))
    )
    
    df = df.sort_values(['매장명', 'order_daily'])
    df['pre_service_status'] = df.groupby('매장명')['service_status'].shift(7).fillna('정상')
    
    return df
# baemin_ad_change_history_path
# baemin_ad_change_history_processed_path
def preprocess_fin_df(**context):
    """최종 전처리"""
    return preprocess_df(
        input_xcom_key='joined_orders_history_click_coupang_path',
        output_xcom_key='final_processed_path',
        transform_func=transform_fin_df,  # 전처리 함수 추가
        **context
    )
    

def fin_save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename='sales_daily_orders_upload.csv',
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
    
    print(f"\n[DEBUG] LOCAL_DB={LOCAL_DB}")
    print(f"[DEBUG] output_subdir={output_subdir}")
    
    parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)
    print(f"[DEBUG] parquet_path from XCom: {parquet_path}")
    
    if not parquet_path:
        print(f"[경고] 저장할 데이터 없음")
        return "⚠️ 저장 스킵: 데이터 없음"
    
    if not os.path.exists(parquet_path):
        print(f"[경고] 파일 경로 없음: {parquet_path}")
        return "⚠️ 저장 스킵: 파일 없음"
    
    try:
        df = pd.read_parquet(parquet_path)
        print(f"[DEBUG] Parquet 파일 읽기 성공: {len(df)}행")
    except Exception as e:
        print(f"[ERROR] Parquet 파일 읽기 실패: {e}")
        import traceback
        print(f"[ERROR] 상세: {traceback.format_exc()}")
        raise
    
    print(f"\n{'='*60}")
    print(f"[입력] 데이터: {len(df):,}행 × {len(df.columns)}컬럼")
    
    # 중복 제거
    if dedup_key:
        dedup_cols = [dedup_key] if isinstance(dedup_key, str) else dedup_key
        valid_cols = [c for c in dedup_cols if c in df.columns]
        if valid_cols:
            before = len(df)
            df.drop_duplicates(subset=valid_cols, keep='last', inplace=True)
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
    
    # ============================================================
    # 날짜 검증 및 변환 (실제 날짜 컬럼만)
    # ============================================================
    # 실제 날짜 컬럼만 명시적으로 지정
    ACTUAL_DATE_COLUMNS = [
        'order_daily', 'uploaded_at', '실오픈일', 'order_date_with_time',
        'join_date_x', 'join_date_y', '조회일자', '날짜', 'order_daily_day'
    ]
    
    datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
    
    if datetime_cols:
        print(f"\n[날짜 검증] {len(datetime_cols)}개 datetime 컬럼 발견")
        
        # 실제 날짜 컬럼 필터링
        actual_date_cols = [col for col in datetime_cols if col in ACTUAL_DATE_COLUMNS]
        non_date_cols = [col for col in datetime_cols if col not in ACTUAL_DATE_COLUMNS]
        
        print(f"  - 실제 날짜 컬럼: {len(actual_date_cols)}개 → 날짜 형식으로 변환")
        print(f"  - 숫자/값 컬럼: {len(non_date_cols)}개 → 빈 문자열로 변환")
        
        # 유효한 날짜 범위 설정 (1900-01-01 ~ 2100-12-31)
        min_valid_date = pd.Timestamp('1900-01-01')
        max_valid_date = pd.Timestamp('2100-12-31')
        
        # 1. 실제 날짜 컬럼 처리
        for col in actual_date_cols:
            if col not in df.columns:
                continue
                
            # 날짜 유효성 검증
            invalid_dates = (
                (df[col] < min_valid_date) | 
                (df[col] > max_valid_date)
            ) & df[col].notna()
            
            invalid_count = invalid_dates.sum()
            if invalid_count > 0:
                print(f"    ⚠️ {col}: {invalid_count}개 비정상 날짜 → NaT")
                df.loc[invalid_dates, col] = pd.NaT
            
            # 문자열로 변환
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
        
        # 2. 날짜가 아닌 컬럼 처리 (원래 숫자 값인데 datetime으로 파싱된 것들)
        # datetime64를 Excel serial number로 역변환하여 원래 숫자 복원
        excel_origin = pd.Timestamp('1899-12-30')
        
        for col in non_date_cols:
            if col not in df.columns:
                continue
            
            # datetime64를 숫자로 역변환
            try:
                # NaT가 아닌 값만 변환
                mask = df[col].notna()
                if mask.any():
                    # Excel serial date로 변환: (date - 1899-12-30).total_seconds() / 86400
                    numeric_values = (df.loc[mask, col] - excel_origin).dt.total_seconds() / 86400
                    df.loc[mask, col] = pd.Series('', index=df.index, dtype=object)  # 먼저 object 타입으로
                    df.loc[mask, col] = numeric_values.round(10).astype(str)  # 숫자를 문자열로
                    print(f"    ✅ {col}: datetime64 → 숫자 복원 ({mask.sum()}건)")
                else:
                    df[col] = ''
                    print(f"    🔄 {col}: 모두 NaT → 빈 문자열")
            except Exception as e:
                print(f"    ⚠️ {col}: 복원 실패 ({e}) → 빈 문자열")
                df[col] = ''
        
        print(f"[날짜 검증] ✅ 완료: {len(actual_date_cols)}개 날짜 변환, {len(non_date_cols)}개 초기화")

    # ============================================================
    # 최종 컬럼 whitelist — 원하는 컬럼만, 원하는 순서로
    # ============================================================
    df = _append_llm_columns_for_csv(df)

    FINAL_COLUMNS = [
        # ── 기본 식별/집계 ─────────────────────────────────────────
        'order_daily', '매장명', '담당자', 'email',
        'total_order_count', 'total_amount', 'fee_ad', 'ARPU',
        'ma_14', 'ma_28',
        'amt_prev_mtd', 'amt_curr_mtd',
        'sum_7d_recent', 'sum_7d_prev',
        # ── 매출 스코어 ────────────────────────────────────────────
        'score_trend', 'score_7d_total', 'score_4week_total',
        'score', 'status', 'pre_status',
        # ── 매장 기본정보 ──────────────────────────────────────────
        '실오픈일',
        # ── 정산/플랫폼별 집계 ─────────────────────────────────────
        'settlement_amount',
        'fee_ad_배민', 'fee_ad_쿠팡',
        'settlement_amount_배민', 'settlement_amount_쿠팡',
        'total_amount_배민', 'total_amount_쿠팡',
        'total_order_count_배민', 'total_order_count_쿠팡',
        # ── 서비스 지표 ────────────────────────────────────────────
        '조리소요시간', '조리소요시간_순위비율',
        '주문접수시간', '주문접수시간_순위비율',
        '조리시간준수율', '조리시간준수율_순위비율',
        '주문접수율', '주문접수율_순위비율',
        '최근재주문율', '최근별점',
        '전체_주문수', '전체_리뷰수', '전체_답변완료수', '전체_평균별점',
        # ── 운영 변경 이력 ─────────────────────────────────────────
        '영업임시중지 변경', '운영시간 변경', '휴무일 변경',
        # ── 날짜/주기 보조 ─────────────────────────────────────────
        '요일', '요일_한글',
        # ── 수수료 ─────────────────────────────────────────────────
        '수수료율', '수수료율_배민', '수수료율_쿠팡',
        # ── 광고 캠페인 변경 ───────────────────────────────────────
        '기간구분', '수집주별',
        '변경전_희망가_최소', '변경전_희망가_최대', '변경전_희망가_평균',
        '변경후_희망가_최소', '변경후_희망가_최대', '변경후_희망가_평균',
        # ── 쿠팡 광고 지표 ─────────────────────────────────────────
        '광고지출', '노출수', '클릭수', '주문수', '주문금액',
        '광고효과', '광고비율', '광고비용', '신규고객',
        '광고주문수', '광고매출', '전체매출', '광고클릭수', '광고노출수',
        # ── 수수료율 분석 ──────────────────────────────────────────
        'fee_ratio', 'fee_ratio_MA14', 'fee_ratio_MA28',
        'avg_7d_recent', 'avg_7d_prev',
        'fee_ratio_mtd_curr_month', 'fee_ratio_mtd_prev_month_same_day',   # 월간 MTD 비교 (이메일 표시용)
        'fee_trand_score', 'fee_avg_7d_score', 'fee_month_score',
        'fee_score_total', 'fee_status', 'pre_fee_status',
        # ── 쿠팡 광고효과 분석 ─────────────────────────────────────
        '쿠팡_광고효과', '쿠팡_광고효과_MA14', '쿠팡_광고효과_MA28',
        '쿠팡_광고효과_avg_7d_recent', '쿠팡_광고효과_avg_7d_prev',
        '쿠팡_광고효과_mtd_curr_month', '쿠팡_광고효과_mtd_prev_month_same_day',   # 월간 MTD 비교 (이메일 표시용)
        '쿠팡_광고효과_trand_score', '쿠팡_광고효과_avg_7d_score', '쿠팡_광고효과_month_score',
        '쿠팡_광고효과_score_total', '쿠팡_광고효과_status', 'pre_쿠팡_광고효과_status',
        # ── 배민 광고효과 분석 ─────────────────────────────────────
        '배민_광고효과', '배민_광고효과_MA14', '배민_광고효과_MA28',
        '배민_광고효과_avg_7d_recent', '배민_광고효과_avg_7d_prev',
        '배민_광고효과_mtd_curr_month', '배민_광고효과_mtd_prev_month_same_day',   # 월간 MTD 비교 (이메일 표시용)
        '배민_광고효과_trand_score', '배민_광고효과_avg_7d_score', '배민_광고효과_month_score',
        '배민_광고효과_score_total', '배민_광고효과_status', 'pre_배민_광고효과_status',
        # ── 조리시간 분석 ──────────────────────────────────────────
        '조리시간_MA14', '조리시간_MA28',
        '조리시간_7d_recent', '조리시간_7d_prev',
        '조리시간_trend_score', '조리시간_weekly_score', '조리시간_monthly_score',
        '조리시간_total',
        # ── 접수시간 분석 ──────────────────────────────────────────
        '접수시간_MA14', '접수시간_MA28',
        '접수시간_7d_recent', '접수시간_7d_prev',
        '접수시간_trend_score', '접수시간_weekly_score', '접수시간_monthly_score',
        '접수시간_total',
        # ── 재주문율 분석 ──────────────────────────────────────────
        '재주문율_MA14', '재주문율_MA28',
        '재주문율_7d_recent', '재주문율_7d_prev',
        '재주문율_trend_score', '재주문율_weekly_score', '재주문율_monthly_score',
        '재주문율_total',
        # ── 별점 분석 ──────────────────────────────────────────────
        '별점_MA14', '별점_MA28',
        '별점_7d_recent', '별점_7d_prev',
        '별점_trend_score', '별점_weekly_score', '별점_monthly_score',
        '별점_total',
        # ── 서비스 종합 ────────────────────────────────────────────
        'service_score_total', 'service_status', 'pre_service_status',
        # ── 종합 위험도 ────────────────────────────────────────────
        'total_score',
        *LLM_COLS,
    ]
    # total_score: 5개 지표 원점수 합산 (0~48점 만점)
    _score_cols = ['score', 'fee_score_total', '쿠팡_광고효과_score_total',
                   '배민_광고효과_score_total', 'service_score_total']
    df['total_score'] = sum(df[c].fillna(0) for c in _score_cols if c in df.columns)
    missing_fc = [c for c in FINAL_COLUMNS if c not in df.columns]
    if missing_fc:
        print(f"[컬럼] ⚠️ FINAL_COLUMNS 중 없는 컬럼 {len(missing_fc)}개 (무시): {missing_fc}")
    df = df[[c for c in FINAL_COLUMNS if c in df.columns]]
    print(f"[컬럼] ✅ 최종 {len(df.columns)}개 컬럼 선택 (whitelist 적용)")

    # ============================================================
    # 폐점 매장 자동 필터링 (employee_store_snapshot_history.csv 기준)
    # ============================================================
    snapshot_path = MART_DB / 'closing_rate' / 'employee_store_snapshot_history.csv'
    if snapshot_path.exists() and '매장명' in df.columns:
        try:
            snap = pd.read_csv(snapshot_path, encoding='utf-8-sig')
            snap.columns = ['ym', '매장명', '실오픈일', '해지일', 'is_active', 'is_new', 'is_closed']
            snap['매장명'] = normalize_store_names(snap['매장명'])
            closed_stores = set(snap[snap['해지일'].notna()]['매장명'].unique())
            before_closed = len(df)
            df = df[~df['매장명'].isin(closed_stores)].reset_index(drop=True)
            removed = before_closed - len(df)
            print(f"[폐점 필터] {len(closed_stores)}개 폐점 매장 기준 → {removed:,}행 제거, 잔여 {len(df):,}행")
        except Exception as e:
            print(f"[폐점 필터] ⚠️ 스냅샷 읽기 실패, 필터 생략: {e}")
    else:
        print(f"[폐점 필터] 스냅샷 파일 없음 또는 매장명 컬럼 없음, 필터 생략")

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
            local_csv_path.unlink()  # 기존 파일 삭제 후 이동 (권한 충돌 방지)
            shutil.move(tmp_path, str(local_csv_path))
            backup_path.unlink()
        else:
            shutil.move(tmp_path, str(local_csv_path))
        
        csv_size = local_csv_path.stat().st_size / (1024 * 1024)
        print(f"[저장] ✅ CSV 저장 완료: {len(df):,}건 ({csv_size:.2f} MB)")
        
    except Exception as e:
        print(f"[ERROR] CSV 저장 실패: {e}")
        import traceback
        print(f"[ERROR] 상세 traceback:")
        print(traceback.format_exc())
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass
        raise  # 에러를 re-raise해서 task가 실패로 표시되도록
    
    print(f"{'='*60}\n")
    return f"✅ 저장 완료: {len(df):,}건"
