# modules/transform/pipelines/sales_store_amount_join.py
import pandas as pd
import numpy as np
import glob
from pathlib import Path
import pandas as pd
import numpy as np
import datetime as dt

from modules.load.load_df_glob import load_data
from modules.transform.utility.paths import TEMP_DIR, LOCAL_DB, COLLECT_DB

PATH_NOW = COLLECT_DB / "영업관리부_수집" / "baemin_metrics_*.csv"
PATH_HISTORY = COLLECT_DB / "영업관리부_수집" / "baemin_change_history_*.csv"

PATH_TOORDER = "/opt/airflow/download/toorder_review_doridang1_*.xlsx"
PATH_ORDERS_ALERTS = LOCAL_DB / "영업관리부_DB" / "sales_daily_orders_alerts.csv"


# ============================================================
# 재업로드 모드 처리 함수 (업로드_temp + 원드라이브 동시 glob)
# ============================================================
def load_reupload_baemin_store_now(**context):
    """
    스마트 배민 우리가게 로더
    - 업로드_temp + 원드라이브 동시 glob 검색
    - 없으면 원본 새로 로드
    """
    upload_temp_path = Path('/opt/airflow/download/업로드_temp')
    onedrive_path = COLLECT_DB / "영업관리부_수집"
    
    all_files = []
    
    # 업로드_temp에서 찾기
    if upload_temp_path.exists():
        temp_files = list(upload_temp_path.glob('baemin_metrics_*.csv'))
        if temp_files:
            print(f"[업로드_temp] {len(temp_files)}개 파일 발견")
            all_files.extend(temp_files)
    
    # 원드라이브에서 찾기
    if onedrive_path.exists():
        onedrive_files = list(onedrive_path.glob('baemin_metrics_*.csv'))
        if onedrive_files:
            print(f"[원드라이브] {len(onedrive_files)}개 파일 발견")
            all_files.extend(onedrive_files)
    
    if all_files:
        print(f"[✅ 재사용] 총 {len(all_files)}개 파일 발견")
        
        # 🎯 중복 파일 제거 (파일명 기준, 최신 파일 우선)
        unique_files = {}
        for f in all_files:
            fname = f.name
            if fname not in unique_files or f.stat().st_mtime > unique_files[fname].stat().st_mtime:
                unique_files[fname] = f
        
        file_paths = list(unique_files.values())
        print(f"[중복 제거] {len(file_paths)}개 파일 사용")
        
        # load_data 호출 (원본 데이터에 있는 컬럼으로만 dedup)
        return load_data(
            file_path=file_paths,
            xcom_key='baemin_store_now_path',
            use_glob=False,
            dedup_key=['store_id', 'collected_at'],  # ⭐ collected_date → collected_at (원본 컬럼)
            add_source_info=False,
            **context
        )
    else:
        print(f"[🔄 새로 로드] 모든 경로에서 파일 없음 → 배민 원본 데이터 새로 로드")
        return load_baemin_store_now_df(**context)


def load_reupload_baemin_history(**context):
    """
    스마트 배민 변경이력 로더
    - 업로드_temp + 원드라이브 동시 glob 검색
    - 없으면 원본 새로 로드
    """
    upload_temp_path = Path('/opt/airflow/download/업로드_temp')
    onedrive_path = COLLECT_DB / "영업관리부_수집"
    
    all_files = []
    
    # 업로드_temp에서 찾기
    if upload_temp_path.exists():
        temp_files = list(upload_temp_path.glob('baemin_change_history_*.csv'))
        if temp_files:
            print(f"[업로드_temp] {len(temp_files)}개 파일 발견")
            all_files.extend(temp_files)
    
    # 원드라이브에서 찾기
    if onedrive_path.exists():
        onedrive_files = list(onedrive_path.glob('baemin_change_history_*.csv'))
        if onedrive_files:
            print(f"[원드라이브] {len(onedrive_files)}개 파일 발견")
            all_files.extend(onedrive_files)
    
    if all_files:
        print(f"[✅ 재사용] 총 {len(all_files)}개 파일 발견")
        
        # 🎯 중복 파일 제거
        unique_files = {}
        for f in all_files:
            fname = f.name
            if fname not in unique_files or f.stat().st_mtime > unique_files[fname].stat().st_mtime:
                unique_files[fname] = f
        
        file_paths = list(unique_files.values())
        print(f"[중복 제거] {len(file_paths)}개 파일 사용")
        
        # load_data 호출
        return load_data(
            file_path=file_paths,
            xcom_key='baemin_history_path',
            use_glob=False,
            dedup_key=['변경시간', "store_id"],  # ⭐ 원본 컬럼
            add_source_info=False,
            **context
        )
    else:
        print(f"[🔄 새로 로드] 모든 경로에서 파일 없음 → 배민 변경이력 원본 데이터 새로 로드")
        return load_baemin_history_df(**context)


def load_reupload_toorder_review(**context):
    """
    스마트 토더 리뷰 로더
    - 업로드_temp + 원드라이브 + download 폴더 동시 glob 검색
    - CSV 우선, 없으면 엑셀 로드
    """
    upload_temp_path = Path('/opt/airflow/download/업로드_temp')
    download_path = Path('/opt/airflow/download')
    onedrive_path = COLLECT_DB / "영업관리부_수집"
    
    # 1. CSV 파일 찾기
    csv_files = []
    
    if upload_temp_path.exists():
        temp_csvs = list(upload_temp_path.glob('toorder_review_*.csv'))
        if temp_csvs:
            print(f"[업로드_temp] {len(temp_csvs)}개 CSV 발견")
            csv_files.extend(temp_csvs)
    
    if onedrive_path.exists():
        onedrive_csvs = list(onedrive_path.glob('toorder_review_*.csv'))
        if onedrive_csvs:
            print(f"[원드라이브] {len(onedrive_csvs)}개 CSV 발견")
            csv_files.extend(onedrive_csvs)
    
    if csv_files:
        print(f"[✅ CSV 재사용] 총 {len(csv_files)}개 파일 발견")
        
        # 🎯 중복 파일 제거
        unique_files = {}
        for f in csv_files:
            fname = f.name
            if fname not in unique_files or f.stat().st_mtime > unique_files[fname].stat().st_mtime:
                unique_files[fname] = f
        
        file_paths = list(unique_files.values())
        print(f"[중복 제거] {len(file_paths)}개 CSV 사용")
        
        # CSV 읽기
        dfs = []
        for fpath in file_paths:
            print(f"   읽는 중: {fpath}")
            df = pd.read_csv(fpath)
            dfs.append(df)
            print(f"   ✓ {len(df)}행 로드")
        
        # 병합
        result_df = pd.concat(dfs, ignore_index=True)
        print(f"병합 완료: {len(result_df):,}행")
        
        # 🎯 중복 제거 (데이터 수준)
        before = len(result_df)
        result_df.drop_duplicates(subset=['date', 'stores_name'], keep='last', inplace=True)
        after = len(result_df)
        if before - after > 0:
            print(f"[중복 제거] {before - after:,}건 제거됨 → {after:,}행")
        
        # Parquet 저장
        temp_dir = TEMP_DIR
        temp_dir.mkdir(exist_ok=True, parents=True)
        output_path = temp_dir / f"toorder_review_raw_{context['ds_nodash']}.parquet"
        result_df.to_parquet(output_path, index=False, engine='pyarrow')
        
        context['task_instance'].xcom_push(
            key='toorder_review_path',
            value=str(output_path)
        )
        return f"✅ {len(result_df):,}건 (CSV 재사용)"
    
    # 2. CSV 없으면 엑셀 찾기
    excel_files = []
    
    if upload_temp_path.exists():
        temp_excels = list(upload_temp_path.glob('toorder_review_doridang1_*.xlsx'))
        if temp_excels:
            print(f"[업로드_temp] {len(temp_excels)}개 엑셀 발견")
            excel_files.extend(temp_excels)
    
    if download_path.exists():
        download_excels = list(download_path.glob('toorder_review_doridang1_*.xlsx'))
        if download_excels:
            print(f"[download] {len(download_excels)}개 엑셀 발견")
            excel_files.extend(download_excels)
    
    if excel_files:
        print(f"[✅ 엑셀 로드] 총 {len(excel_files)}개 파일 발견")
        
        # 🎯 중복 파일 제거
        unique_files = {}
        for f in excel_files:
            fname = f.name
            if fname not in unique_files or f.stat().st_mtime > unique_files[fname].stat().st_mtime:
                unique_files[fname] = f
        
        file_paths = list(unique_files.values())
        print(f"[중복 제거] {len(file_paths)}개 파일 사용")
        
        # 엑셀 읽기
        dfs = []
        for fpath in file_paths:
            print(f"   읽는 중: {fpath}")
            df = pd.read_excel(fpath, header=3)
            
            # 날짜 추출
            file_name = Path(fpath).name
            date_info = extract_date_from_toorder_filename(file_name)
            df['date'] = date_info['date']
            
            dfs.append(df)
            print(f"   ✓ {len(df)}행 로드")
        
        # 병합
        result_df = pd.concat(dfs, ignore_index=True)
        print(f"병합 완료: {len(result_df):,}행")
        
        # Parquet 저장
        temp_dir = TEMP_DIR
        temp_dir.mkdir(exist_ok=True, parents=True)
        output_path = temp_dir / f"toorder_review_raw_{context['ds_nodash']}.parquet"
        result_df.to_parquet(output_path, index=False, engine='pyarrow')
        
        context['task_instance'].xcom_push(
            key='toorder_review_path',
            value=str(output_path)
        )
        return f"✅ {len(result_df):,}건 (엑셀 로드)"
    
    # 3. 파일 없음
    print(f"[❌ 에러] 토더 리뷰 파일 없음")
    context['task_instance'].xcom_push(key='toorder_review_path', value=None)
    return "0건 (파일 없음)"


# ============================================================
# 토더 리뷰 헬퍼 함수
# ============================================================
def extract_date_from_toorder_filename(file_name):
    """파일명에서 날짜 추출"""
    try:
        date_str = file_name.split('_')[-1].split('.')[0]
        return {'date': date_str}
    except Exception as e:
        print(f"[경고] 날짜 추출 실패: {file_name}, {e}")
        return {'date': None}


def load_toorder_review_df(**context):
    """토더 리뷰 엑셀 로드 (원본 경로)"""
    ti = context['task_instance']
    
    file_list = sorted(glob.glob(str(PATH_TOORDER)))
    print(f"[로드] 찾은 파일: {len(file_list)}개")
    
    if not file_list:
        ti.xcom_push(key='toorder_review_path', value=None)
        return "0건 (파일 없음)"
    
    dfs = []
    for fpath in file_list:
        print(f"   읽는 중: {fpath}")
        df = pd.read_excel(fpath, header=3)
        
        file_name = fpath.split('/')[-1]
        date_info = extract_date_from_toorder_filename(file_name)
        df['date'] = date_info['date']
        
        dfs.append(df)
        print(f"   ✓ {len(df)}행 로드")
    
    result_df = pd.concat(dfs, ignore_index=True)
    print(f"병합 완료: {len(result_df):,}행")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    output_path = temp_dir / f"toorder_review_raw_{context['ds_nodash']}.parquet"
    result_df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key='toorder_review_path', value=str(output_path))
    return f"{len(result_df):,}건"


def preprocess_toorder_review_df(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """토더 리뷰 전처리"""
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(
        task_ids=input_task_id,
        key=input_xcom_key
    )
    
    if not parquet_path:
        print(f"[경고] 토더 리뷰 데이터 없음 - 스킵")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 데이터 없음)"
    
    toorder_review_df = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(toorder_review_df):,}행")
    
    # 컬럼 선택
    col = ['date', '매장명.1', '채널', '주문 수', '리뷰 수', '답변완료 수', '평균 별점']
    toorder_review_df = toorder_review_df[col]
    
    # 결측치 제거
    toorder_review_df = toorder_review_df[~toorder_review_df["채널"].isnull()]
    
    # 0점 제외 평균 함수
    def mean_excluding_zero(x):
        non_zero = x[x > 0]
        return non_zero.mean() if len(non_zero) > 0 else 0
    
    # 그룹화
    toorder_review_df = toorder_review_df.groupby(["date", "매장명.1"]).agg(
        전체_주문수=("주문 수", "sum"),
        전체_리뷰수=("리뷰 수", "sum"),
        전체_답변완료수=("답변완료 수", "sum"),
        전체_평균별점=("평균 별점", mean_excluding_zero)
    ).reset_index()
    
    # 매장명 정리
    toorder_review_df.rename(columns={"매장명.1": "stores_name"}, inplace=True)
    toorder_review_df["stores_name"] = "도리당 " + toorder_review_df["stores_name"]
    toorder_review_df["stores_name"] = toorder_review_df["stores_name"].replace({
        "도리당 일산백석점": "도리당 백석점",
        "도리당 서울대점": "도리당 서울대점",
        "도리당 구로디지털단지점": "도리당 구로디지털점",
        "도리당 충주봉방점": "도리당 충주역점"
    })
    
    print(f"전처리 완료: {len(toorder_review_df):,}행")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    toorder_review_df.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"전처리: {len(toorder_review_df):,}행"


# ============================================================
# 배민 우리가게 now
# ============================================================
def load_baemin_store_now_df(**context):
    """배민 우리가게 now 로드 (원본 경로)"""
    return load_data(
        file_path=PATH_NOW,
        xcom_key='baemin_store_now_path',
        use_glob=True,
        add_source_info=False,
        **context
    )


def preprocess_baemin_store_now_df(**context):
    """배민 매장 현황 전처리"""
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(
        task_ids='load_baemin_store_now',
        key='baemin_store_now_path'
    )
    
    if not parquet_path:
        print(f"[경고] 배민 현황 데이터 없음 - 스킵")
        ti.xcom_push(key='processed_baemin_path', value=None)
        return "0건 (입력 데이터 없음)"
    
    now_df = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(now_df):,}행")
    
    now_df["collected_date"] = now_df["collected_at"].str[:10]
    now_df.drop_duplicates(subset=['store_id', 'collected_date'], keep='last', inplace=True)
    now_df["stores_name"] = now_df["store_name"].str.split(" ").str[-2:].str.join(" ")
    
    col = ['collected_date', 'stores_name', '조리소요시간',
           '조리소요시간_순위비율', '주문접수시간', '주문접수시간_순위비율',
           '조리시간준수율', '조리시간준수율_순위비율', '주문접수율',
           '주문접수율_순위비율', '최근별점']
    
    now_df = now_df[col]
    
    print(f"전처리 완료: {len(now_df):,}행")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"processed_baemin_{context['ds_nodash']}.parquet"
    now_df.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key='processed_baemin_path', value=str(processed_path))
    return f"전처리: {len(now_df):,}행"


# ============================================================
# 배민 변경이력
# ============================================================
def load_baemin_history_df(**context):
    """배민 변경이력 로드 (원본 경로)"""
    return load_data(
        file_path=PATH_HISTORY,
        xcom_key='baemin_history_path',
        use_glob=True,
        dedup_key=['변경시간', "store_id"],
        add_source_info=False,
        **context
    )


def preprocess_baemin_history_df(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """배민 변경이력 전처리"""
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(
        task_ids=input_task_id,
        key=input_xcom_key
    )
    
    if not parquet_path:
        print(f"[경고] 배민 변경이력 데이터 없음 - 스킵")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 데이터 없음)"
    
    history_df = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(history_df):,}행")
    
    history_df["change_date"] = history_df["변경시간"].str[:10]
    history_df = history_df.drop_duplicates(subset=["변경시간", "store_id"], keep='last')
    history_df["stores_name"] = history_df["매장명"].str.split(" ").str[-2:].str.join(" ")
    history_trans_df = history_df[["change_date", "stores_name", "대분류"]]

    history_trans_df = history_trans_df.groupby(
        ["change_date", "stores_name", "대분류"]
    ).size().reset_index(name='cnt').pivot_table(
        index=["change_date", "stores_name"],
        columns="대분류",
        values="cnt",
        fill_value=0,
        aggfunc='sum'
    ).reset_index().rename_axis(None, axis=1)
    
    print(f"전처리 완료: {len(history_trans_df):,}행")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    history_trans_df.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"전처리: {len(history_trans_df):,}행"


# ============================================================
# 주문 집계 데이터
# ============================================================
def load_sales_daily_orders_alerts_df(**context):
    """
    매출 주문 알림 데이터 로드
    
    ⭐ sales_daily_orders.py의 filter_alerts()에서 이미 생성된
       sales_daily_orders_alerts.csv를 직접 로드
       (복사 X, 이미 생성된 파일 로드)
    """
    alerts_file = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders_alerts.csv'
    
    print(f"[로드] {alerts_file.name} 로드 중...")
    
    # 1. 파일 존재 확인
    if not alerts_file.exists():
        print(f"[❌ 에러] 파일 없음: {alerts_file}")
        print(f"[힌트] sales_daily_orders.py의 filter_alerts()를 먼저 실행해야 합니다")
        context['task_instance'].xcom_push(key='sales_daily_orders_alerts_path', value=None)
        return "0건 (파일 없음)"
    
    try:
        # CSV 읽기
        df = pd.read_csv(alerts_file, low_memory=False)
        
        print(f"[✅ 로드] {len(df):,}건 로드 완료")
        
        # order_daily 컬럼 확인 또는 추가
        if 'order_daily' not in df.columns:
            if 'order_date' in df.columns:
                df['order_daily'] = df['order_date']
                print(f"[변환] order_date → order_daily (JOIN 호환성)")
            else:
                print(f"[❌ 에러] order_daily/order_date 컬럼 없음")
                context['task_instance'].xcom_push(key='sales_daily_orders_alerts_path', value=None)
                return "0건 (컬럼 오류)"
        
        # Parquet 저장 (JOIN용)
        temp_dir = TEMP_DIR
        temp_dir.mkdir(exist_ok=True, parents=True)
        
        parquet_path = temp_dir / f"sales_daily_orders_alerts_{context['ds_nodash']}.parquet"
        df.to_parquet(parquet_path, index=False, engine='pyarrow')
        print(f"[✅ 저장] Parquet 저장 완료: {parquet_path.name}")
        
        # XCom 저장
        context['task_instance'].xcom_push(key='sales_daily_orders_alerts_path', value=str(parquet_path))
        
        return f"✅ {len(df):,}건 로드됨"
        
    except Exception as e:
        print(f"[❌ 에러] 로드 실패: {e}")
        import traceback
        traceback.print_exc()
        context['task_instance'].xcom_push(key='sales_daily_orders_alerts_path', value=None)
        return f"0건 (로드 에러: {str(e)[:50]})"


# ============================================================
# JOIN 함수들 (동일하게 유지)
# ============================================================
def left_join_orders_now(
    left_task,
    right_task,
    on=None,
    left_on=["order_daily", "매장명"],
    right_on=["collected_date", "stores_name"],
    how='left',
    drop_columns=["collected_date", "stores_name"],
    output_xcom_key='joined_orders_now_path',
    **context
):
    """두 task의 데이터를 join"""
    ti = context['task_instance']
    
    if isinstance(left_task, str):
        left_task = {'task_id': left_task, 'xcom_key': 'sales_daily_orders_alerts_path'}
    if isinstance(right_task, str):
        right_task = {'task_id': right_task, 'xcom_key': 'processed_baemin_path'}
    
    left_path = ti.xcom_pull(
        task_ids=left_task['task_id'],
        key=left_task['xcom_key']
    )
    if not left_path:
        print(f"[에러] 왼쪽 데이터 없음: {left_task['task_id']}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "join 실패: 왼쪽 데이터 없음"
    
    left_df = pd.read_parquet(left_path)
    print(f"[왼쪽] {left_task['task_id']}: {len(left_df):,}행")
    
    right_path = ti.xcom_pull(
        task_ids=right_task['task_id'],
        key=right_task['xcom_key']
    )
    if not right_path:
        print(f"[경고] 오른쪽 데이터 없음: {right_task['task_id']} - 왼쪽 데이터만 저장")
        temp_dir = TEMP_DIR
        temp_dir.mkdir(exist_ok=True, parents=True)
        output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
        left_df.to_parquet(output_path, index=False, engine='pyarrow')
        ti.xcom_push(key=output_xcom_key, value=str(output_path))
        return f"⚠️ 오른쪽 데이터 없음, 왼쪽만 저장: {len(left_df):,}행"
    
    right_df = pd.read_parquet(right_path)
    print(f"[오른쪽] {right_task['task_id']}: {len(right_df):,}행")
    
    # Join 실행
    if on is not None:
        print(f"\n[JOIN] how={how}, on={on}")
        joined_df = left_df.merge(right_df, on=on, how=how)
    elif left_on is not None and right_on is not None:
        print(f"\n[JOIN] how={how}, left_on={left_on}, right_on={right_on}")
        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)
    else:
        raise ValueError("on 또는 (left_on, right_on)을 지정해야 합니다.")
    
    print(f"[JOIN] 완료: {len(joined_df):,}행 × {len(joined_df.columns)}컬럼")
    
    # 중복 컬럼 제거
    if drop_columns is None and left_on != right_on:
        drop_columns = right_on if isinstance(right_on, list) else [right_on]
    
    if drop_columns:
        cols_to_drop = [col for col in drop_columns if col in joined_df.columns]
        if cols_to_drop:
            joined_df.drop(columns=cols_to_drop, inplace=True)
            print(f"[정리] 제거된 컬럼: {cols_to_drop}")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    joined_df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    return f"✅ join 완료: {len(joined_df):,}행"


def left_join_orders_now_toorder(
    left_task,
    right_task,
    on=None,
    left_on=["order_daily", "매장명"],
    right_on=["date", "stores_name"],
    how='left',
    drop_columns=["date", "stores_name"],
    output_xcom_key='joined_orders_now_toorder_path',
    **context
):
    """(주문 + 우리가게now) 데이터와 토더 리뷰 조인"""
    ti = context['task_instance']
    
    if isinstance(left_task, str):
        left_task = {'task_id': left_task, 'xcom_key': 'joined_orders_now_path'}
    if isinstance(right_task, str):
        right_task = {'task_id': right_task, 'xcom_key': 'preprocessed_toorder_review_path'}
    
    left_path = ti.xcom_pull(
        task_ids=left_task['task_id'],
        key=left_task['xcom_key']
    )
    if not left_path:
        print(f"[에러] 왼쪽 데이터 없음: {left_task['task_id']}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "join 실패: 왼쪽 데이터 없음"
    
    left_df = pd.read_parquet(left_path)
    print(f"[왼쪽] {left_task['task_id']}: {len(left_df):,}행")
    
    right_path = ti.xcom_pull(
        task_ids=right_task['task_id'],
        key=right_task['xcom_key']
    )
    
    if not right_path:
        print(f"[경고] 오른쪽 데이터 없음: {right_task['task_id']} - 왼쪽 데이터만 저장")
        temp_dir = TEMP_DIR
        temp_dir.mkdir(exist_ok=True, parents=True)
        output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
        left_df.to_parquet(output_path, index=False, engine='pyarrow')
        ti.xcom_push(key=output_xcom_key, value=str(output_path))
        return f"⚠️ 오른쪽 데이터 없음, 왼쪽만 저장: {len(left_df):,}행"
    
    right_df = pd.read_parquet(right_path)
    print(f"[오른쪽] {right_task['task_id']}: {len(right_df):,}행")
    
    # Join 실행
    if on is not None:
        print(f"\n[JOIN] how={how}, on={on}")
        joined_df = left_df.merge(right_df, on=on, how=how)
    elif left_on is not None and right_on is not None:
        print(f"\n[JOIN] how={how}, left_on={left_on}, right_on={right_on}")
        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)
    else:
        raise ValueError("on 또는 (left_on, right_on)을 지정해야 합니다.")
    
    print(f"[JOIN] 완료: {len(joined_df):,}행")
    
    # 중복 컬럼 제거
    if drop_columns is None and left_on != right_on:
        drop_columns = right_on if isinstance(right_on, list) else [right_on]
    
    if drop_columns:
        cols_to_drop = [col for col in drop_columns if col in joined_df.columns]
        if cols_to_drop:
            joined_df.drop(columns=cols_to_drop, inplace=True)
            print(f"[정리] 제거된 컬럼: {cols_to_drop}")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    joined_df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    return f"✅ join 완료: {len(joined_df):,}행"


def left_join_orders_now_toorder_history(
    left_task,
    right_task,
    on=None,
    left_on=["order_daily", "매장명"],
    right_on=["change_date", "stores_name"],
    how='left',
    drop_columns=["change_date", "stores_name"],
    output_xcom_key='joined_orders_now_toorder_history_path',
    **context
):
    """(주문 + 우리가게now + 토더) 데이터와 배민 변경이력 조인"""
    ti = context['task_instance']
    
    if isinstance(left_task, str):
        left_task = {'task_id': left_task, 'xcom_key': 'joined_orders_now_toorder_path'}
    if isinstance(right_task, str):
        right_task = {'task_id': right_task, 'xcom_key': 'preprocessed_baemin_history_path'}
    
    left_path = ti.xcom_pull(
        task_ids=left_task['task_id'],
        key=left_task['xcom_key']
    )
    if not left_path:
        print(f"[에러] 왼쪽 데이터 없음: {left_task['task_id']}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "join 실패: 왼쪽 데이터 없음"
    
    left_df = pd.read_parquet(left_path)
    print(f"[왼쪽] {left_task['task_id']}: {len(left_df):,}행")
    
    right_path = ti.xcom_pull(
        task_ids=right_task['task_id'],
        key=right_task['xcom_key']
    )
    
    if not right_path:
        print(f"[경고] 오른쪽 데이터 없음: {right_task['task_id']} - 왼쪽 데이터만 저장")
        temp_dir = TEMP_DIR
        temp_dir.mkdir(exist_ok=True, parents=True)
        output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
        left_df.to_parquet(output_path, index=False, engine='pyarrow')
        ti.xcom_push(key=output_xcom_key, value=str(output_path))
        return f"⚠️ 오른쪽 데이터 없음, 왼쪽만 저장: {len(left_df):,}행"
    
    right_df = pd.read_parquet(right_path)
    print(f"[오른쪽] {right_task['task_id']}: {len(right_df):,}행")
    
    # Join 실행
    if on is not None:
        print(f"\n[JOIN] how={how}, on={on}")
        joined_df = left_df.merge(right_df, on=on, how=how)
    elif left_on is not None and right_on is not None:
        print(f"\n[JOIN] how={how}, left_on={left_on}, right_on={right_on}")
        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)
    else:
        raise ValueError("on 또는 (left_on, right_on)을 지정해야 합니다.")
    
    print(f"[JOIN] 완료: {len(joined_df):,}행")
    
    # 중복 컬럼 제거
    if drop_columns is None and left_on != right_on:
        drop_columns = right_on if isinstance(right_on, list) else [right_on]
    
    if drop_columns:
        cols_to_drop = [col for col in drop_columns if col in joined_df.columns]
        if cols_to_drop:
            joined_df.drop(columns=cols_to_drop, inplace=True)
            print(f"[정리] 제거된 컬럼: {cols_to_drop}")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    joined_df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    return f"✅ join 완료: {len(joined_df):,}행"



# 전처리 추가
def preprocess_add_main_left_join_df(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """배민 변경이력 전처리"""
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(
        task_ids=input_task_id,
        key=input_xcom_key
    )
    
    if not parquet_path:
        print(f"[경고] 배민 변경이력 데이터 없음 - 스킵")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 데이터 없음)"
    
    df = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(df):,}행")

    # ==========================================
    # 2. 날짜 변환 및 요일 추가
    # ==========================================
    df["order_daily"] = pd.to_datetime(df["order_daily"], format="mixed")
    df["요일"] = df["order_daily"].dt.day_name()  # Monday, Tuesday...
    df["요일_한글"] = df["order_daily"].dt.dayofweek.map({
        0: '월', 1: '화', 2: '수', 3: '목', 4: '금', 5: '토', 6: '일'
    })

    df["order_week"] = df["order_daily"].dt.to_period("W")
    df["order_month"] = df["order_daily"].dt.to_period("M")

    # 매장별 정렬
    df = df.sort_values(by=["매장명", "order_daily"], ascending=[True, True])

    # ==========================================
    # 3. 조인 키 생성
    # ==========================================
    df["join_pre_date"] = df["order_daily"] - dt.timedelta(days=1)  # 전일
    df["join_pre_week_sameday"] = df["order_daily"] - dt.timedelta(days=7)  # 전주동요일
    df["join_pre_week"] = df["order_week"] - 1  # 전주
    df["join_pre_month"] = df["order_month"] - 1  # 전월

    # ==========================================
    # 4. 집계할 컬럼 정의
    # ==========================================
    agg_columns = {
        "total_amount": "sum",
        "total_order_count": "sum",
        "settlement_amount": "sum",
        "total_amount_배민": "sum",
        "total_amount_쿠팡": "sum",
        "settlement_amount_배민": "sum",
        "settlement_amount_쿠팡": "sum",
        "total_order_count_배민": "sum",
        "total_order_count_쿠팡": "sum"
    }

    # ==========================================
    # 5. 전기 비교 데이터 생성
    # ==========================================

    # ────────────────────────────────────────
    # 5-1. 전일 비교 (일별 전체 레벨)
    # ────────────────────────────────────────
    print("🔄 전일 비교 데이터 생성 중...")
    daily_total = df.groupby("order_daily").agg(agg_columns).reset_index()

    pre_date_total = daily_total.copy()
    pre_date_total = pre_date_total.rename(columns={
        "order_daily": "join_pre_date",
        "total_amount": "전일_전체매출",
        "total_order_count": "전일_전체주문건수",
        "settlement_amount": "전일_전체정산금액",
        "total_amount_배민": "전일_전체매출_배민",
        "total_amount_쿠팡": "전일_전체매출_쿠팡",
        "settlement_amount_배민": "전일_전체정산금액_배민",
        "settlement_amount_쿠팡": "전일_전체정산금액_쿠팡",
        "total_order_count_배민": "전일_전체주문건수_배민",
        "total_order_count_쿠팡": "전일_전체주문건수_쿠팡"
    })

    df = df.merge(pre_date_total, on="join_pre_date", how="left")
    print("   ✅ 전일 전체 레벨 완료")

    # ────────────────────────────────────────
    # 5-2. 전일 비교 (일별 매장 레벨)
    # ────────────────────────────────────────
    pre_date_store = df[[
        "order_daily", "매장명", 
        "total_amount", "total_order_count", "settlement_amount",
        "total_amount_배민", "total_amount_쿠팡",
        "settlement_amount_배민", "settlement_amount_쿠팡",
        "total_order_count_배민", "total_order_count_쿠팡"
    ]].copy()

    pre_date_store = pre_date_store.rename(columns={
        "order_daily": "join_pre_date",
        "total_amount": "전일_매장매출",
        "total_order_count": "전일_매장주문건수",
        "settlement_amount": "전일_매장정산금액",
        "total_amount_배민": "전일_매장매출_배민",
        "total_amount_쿠팡": "전일_매장매출_쿠팡",
        "settlement_amount_배민": "전일_매장정산금액_배민",
        "settlement_amount_쿠팡": "전일_매장정산금액_쿠팡",
        "total_order_count_배민": "전일_매장주문건수_배민",
        "total_order_count_쿠팡": "전일_매장주문건수_쿠팡"
    })

    df = df.merge(pre_date_store, on=["join_pre_date", "매장명"], how="left")
    print("   ✅ 전일 매장 레벨 완료")

    # ────────────────────────────────────────
    # 5-3. 전주동요일 비교 (7일 전, 전체 레벨)
    # ────────────────────────────────────────
    print("🔄 전주동요일 비교 데이터 생성 중...")
    pre_week_sameday_total = daily_total.copy()
    pre_week_sameday_total = pre_week_sameday_total.rename(columns={
        "order_daily": "join_pre_week_sameday",
        "total_amount": "전주동요일_전체매출",
        "total_order_count": "전주동요일_전체주문건수",
        "settlement_amount": "전주동요일_전체정산금액",
        "total_amount_배민": "전주동요일_전체매출_배민",
        "total_amount_쿠팡": "전주동요일_전체매출_쿠팡",
        "settlement_amount_배민": "전주동요일_전체정산금액_배민",
        "settlement_amount_쿠팡": "전주동요일_전체정산금액_쿠팡",
        "total_order_count_배민": "전주동요일_전체주문건수_배민",
        "total_order_count_쿠팡": "전주동요일_전체주문건수_쿠팡"
    })

    df = df.merge(pre_week_sameday_total, on="join_pre_week_sameday", how="left")
    print("   ✅ 전주동요일 전체 레벨 완료")

    # ────────────────────────────────────────
    # 5-4. 전주동요일 비교 (매장 레벨)
    # ────────────────────────────────────────
    pre_week_sameday_store = df[[
        "order_daily", "매장명", 
        "total_amount", "total_order_count", "settlement_amount",
        "total_amount_배민", "total_amount_쿠팡",
        "settlement_amount_배민", "settlement_amount_쿠팡",
        "total_order_count_배민", "total_order_count_쿠팡"
    ]].copy()

    pre_week_sameday_store = pre_week_sameday_store.rename(columns={
        "order_daily": "join_pre_week_sameday",
        "total_amount": "전주동요일_매장매출",
        "total_order_count": "전주동요일_매장주문건수",
        "settlement_amount": "전주동요일_매장정산금액",
        "total_amount_배민": "전주동요일_매장매출_배민",
        "total_amount_쿠팡": "전주동요일_매장매출_쿠팡",
        "settlement_amount_배민": "전주동요일_매장정산금액_배민",
        "settlement_amount_쿠팡": "전주동요일_매장정산금액_쿠팡",
        "total_order_count_배민": "전주동요일_매장주문건수_배민",
        "total_order_count_쿠팡": "전주동요일_매장주문건수_쿠팡"
    })

    df = df.merge(pre_week_sameday_store, on=["join_pre_week_sameday", "매장명"], how="left")
    print("   ✅ 전주동요일 매장 레벨 완료")

    # ────────────────────────────────────────
    # 5-5. 전주 비교 (주별 전체 레벨)
    # ────────────────────────────────────────
    print("🔄 전주 비교 데이터 생성 중...")
    weekly_total = df.groupby("order_week").agg(agg_columns).reset_index()

    pre_week_total = weekly_total.copy()
    pre_week_total = pre_week_total.rename(columns={
        "order_week": "join_pre_week",
        "total_amount": "전주_전체매출",
        "total_order_count": "전주_전체주문건수",
        "settlement_amount": "전주_전체정산금액",
        "total_amount_배민": "전주_전체매출_배민",
        "total_amount_쿠팡": "전주_전체매출_쿠팡",
        "settlement_amount_배민": "전주_전체정산금액_배민",
        "settlement_amount_쿠팡": "전주_전체정산금액_쿠팡",
        "total_order_count_배민": "전주_전체주문건수_배민",
        "total_order_count_쿠팡": "전주_전체주문건수_쿠팡"
    })

    df = df.merge(pre_week_total, on="join_pre_week", how="left")
    print("   ✅ 전주 전체 레벨 완료")

    # ────────────────────────────────────────
    # 5-6. 전주 비교 (주별 매장 레벨)
    # ────────────────────────────────────────
    weekly_store = df.groupby(["order_week", "매장명"]).agg(agg_columns).reset_index()

    pre_week_store = weekly_store.copy()
    pre_week_store = pre_week_store.rename(columns={
        "order_week": "join_pre_week",
        "total_amount": "전주_매장매출",
        "total_order_count": "전주_매장주문건수",
        "settlement_amount": "전주_매장정산금액",
        "total_amount_배민": "전주_매장매출_배민",
        "total_amount_쿠팡": "전주_매장매출_쿠팡",
        "settlement_amount_배민": "전주_매장정산금액_배민",
        "settlement_amount_쿠팡": "전주_매장정산금액_쿠팡",
        "total_order_count_배민": "전주_매장주문건수_배민",
        "total_order_count_쿠팡": "전주_매장주문건수_쿠팡"
    })

    df = df.merge(pre_week_store, on=["join_pre_week", "매장명"], how="left")
    print("   ✅ 전주 매장 레벨 완료")

    # ────────────────────────────────────────
    # 5-7. 전월 비교 (월별 전체 레벨)
    # ────────────────────────────────────────
    print("🔄 전월 비교 데이터 생성 중...")
    monthly_total = df.groupby("order_month").agg(agg_columns).reset_index()

    pre_month_total = monthly_total.copy()
    pre_month_total = pre_month_total.rename(columns={
        "order_month": "join_pre_month",
        "total_amount": "전월_전체매출",
        "total_order_count": "전월_전체주문건수",
        "settlement_amount": "전월_전체정산금액",
        "total_amount_배민": "전월_전체매출_배민",
        "total_amount_쿠팡": "전월_전체매출_쿠팡",
        "settlement_amount_배민": "전월_전체정산금액_배민",
        "settlement_amount_쿠팡": "전월_전체정산금액_쿠팡",
        "total_order_count_배민": "전월_전체주문건수_배민",
        "total_order_count_쿠팡": "전월_전체주문건수_쿠팡"
    })

    df = df.merge(pre_month_total, on="join_pre_month", how="left")
    print("   ✅ 전월 전체 레벨 완료")

    # ────────────────────────────────────────
    # 5-8. 전월 비교 (월별 매장 레벨)
    # ────────────────────────────────────────
    monthly_store = df.groupby(["order_month", "매장명"]).agg(agg_columns).reset_index()

    pre_month_store = monthly_store.copy()
    pre_month_store = pre_month_store.rename(columns={
        "order_month": "join_pre_month",
        "total_amount": "전월_매장매출",
        "total_order_count": "전월_매장주문건수",
        "settlement_amount": "전월_매장정산금액",
        "total_amount_배민": "전월_매장매출_배민",
        "total_amount_쿠팡": "전월_매장매출_쿠팡",
        "settlement_amount_배민": "전월_매장정산금액_배민",
        "settlement_amount_쿠팡": "전월_매장정산금액_쿠팡",
        "total_order_count_배민": "전월_매장주문건수_배민",
        "total_order_count_쿠팡": "전월_매장주문건수_쿠팡"
    })

    df = df.merge(pre_month_store, on=["join_pre_month", "매장명"], how="left")
    print("   ✅ 전월 매장 레벨 완료")

    # ==========================================
    # 6. 수수료율 계산
    # ==========================================
    print("\n💳 수수료율 계산 중...")

    # 현재 수수료율
    df["수수료율"] = ((df["total_amount"] - df["settlement_amount"]) / df["total_amount"] * 100).round(2)
    df["수수료율_배민"] = ((df["total_amount_배민"] - df["settlement_amount_배민"]) / df["total_amount_배민"] * 100).round(2)
    df["수수료율_쿠팡"] = ((df["total_amount_쿠팡"] - df["settlement_amount_쿠팡"]) / df["total_amount_쿠팡"] * 100).round(2)

    # 전일 수수료율 (매장별)
    df["전일_수수료율"] = ((df["전일_매장매출"] - df["전일_매장정산금액"]) / df["전일_매장매출"] * 100).round(2)
    df["전일_수수료율_배민"] = ((df["전일_매장매출_배민"] - df["전일_매장정산금액_배민"]) / df["전일_매장매출_배민"] * 100).round(2)
    df["전일_수수료율_쿠팡"] = ((df["전일_매장매출_쿠팡"] - df["전일_매장정산금액_쿠팡"]) / df["전일_매장매출_쿠팡"] * 100).round(2)

    # 전주동요일 수수료율 (매장별)
    df["전주동요일_수수료율"] = ((df["전주동요일_매장매출"] - df["전주동요일_매장정산금액"]) / df["전주동요일_매장매출"] * 100).round(2)
    df["전주동요일_수수료율_배민"] = ((df["전주동요일_매장매출_배민"] - df["전주동요일_매장정산금액_배민"]) / df["전주동요일_매장매출_배민"] * 100).round(2)
    df["전주동요일_수수료율_쿠팡"] = ((df["전주동요일_매장매출_쿠팡"] - df["전주동요일_매장정산금액_쿠팡"]) / df["전주동요일_매장매출_쿠팡"] * 100).round(2)

    print("   ✅ 수수료율 계산 완료")

    # ==========================================
    # 7. 증감액/증감률 계산
    # ==========================================
    print("\n📊 증감 지표 계산 중...")

    # 전일 대비
    df["전일대비_매출증감액"] = df["total_amount"] - df["전일_매장매출"]
    df["전일대비_매출증감률"] = ((df["total_amount"] - df["전일_매장매출"]) / df["전일_매장매출"] * 100).round(2)
    df["전일대비_수수료율증감"] = (df["수수료율"] - df["전일_수수료율"]).round(2)

    # 전일 대비 - 배민
    df["전일대비_매출증감률_배민"] = ((df["total_amount_배민"] - df["전일_매장매출_배민"]) / df["전일_매장매출_배민"] * 100).round(2)
    df["전일대비_수수료율증감_배민"] = (df["수수료율_배민"] - df["전일_수수료율_배민"]).round(2)

    # 전일 대비 - 쿠팡
    df["전일대비_매출증감률_쿠팡"] = ((df["total_amount_쿠팡"] - df["전일_매장매출_쿠팡"]) / df["전일_매장매출_쿠팡"] * 100).round(2)
    df["전일대비_수수료율증감_쿠팡"] = (df["수수료율_쿠팡"] - df["전일_수수료율_쿠팡"]).round(2)

    # 전주동요일 대비
    df["전주동요일대비_매출증감액"] = df["total_amount"] - df["전주동요일_매장매출"]
    df["전주동요일대비_매출증감률"] = ((df["total_amount"] - df["전주동요일_매장매출"]) / df["전주동요일_매장매출"] * 100).round(2)
    df["전주동요일대비_수수료율증감"] = (df["수수료율"] - df["전주동요일_수수료율"]).round(2)

    # 전주동요일 대비 - 배민
    df["전주동요일대비_매출증감률_배민"] = ((df["total_amount_배민"] - df["전주동요일_매장매출_배민"]) / df["전주동요일_매장매출_배민"] * 100).round(2)
    df["전주동요일대비_수수료율증감_배민"] = (df["수수료율_배민"] - df["전주동요일_수수료율_배민"]).round(2)

    # 전주동요일 대비 - 쿠팡
    df["전주동요일대비_매출증감률_쿠팡"] = ((df["total_amount_쿠팡"] - df["전주동요일_매장매출_쿠팡"]) / df["전주동요일_매장매출_쿠팡"] * 100).round(2)
    df["전주동요일대비_수수료율증감_쿠팡"] = (df["수수료율_쿠팡"] - df["전주동요일_수수료율_쿠팡"]).round(2)

    print("   ✅ 증감 지표 계산 완료")

    # ==========================================
    # 8. 기간 구분 컬럼 추가 (금일/전일/금주/저번주/2주전/금월/전월/2개월전)
    # ==========================================
    print("\n📅 기간 구분 컬럼 추가 중...")
    
    # 기준일 설정 (order_daily 최대값)
    today = df["order_daily"].max()
    today_date = pd.to_datetime(today).date()
    
    # 날짜 정보 계산
    yesterday = today - pd.Timedelta(days=1)
    two_weeks_ago = today - pd.Timedelta(days=14)
    two_months_ago = today - pd.DateOffset(months=2)
    
    # 현재 주, 저번 주, 2주전 주의 시작 및 종료
    today_dt = pd.to_datetime(today)
    this_week_start = today_dt - pd.Timedelta(days=today_dt.weekday())
    last_week_start = this_week_start - pd.Timedelta(days=7)
    last_week_end = this_week_start - pd.Timedelta(days=1)
    two_weeks_start = last_week_start - pd.Timedelta(days=7)
    two_weeks_end = last_week_start - pd.Timedelta(days=1)
    
    # 현재 달, 지난 달, 2개월 전
    this_month_start = today_dt.replace(day=1)
    last_month_start = (this_month_start - pd.Timedelta(days=1)).replace(day=1)
    two_months_start = (last_month_start - pd.Timedelta(days=1)).replace(day=1)
    
    # 기간 구분 함수
    def get_period_type(date_val):
        if pd.isna(date_val):
            return ''
        
        d = pd.to_datetime(date_val)
        
        # 금일/전일
        if d.date() == today_date:
            return '금일'
        elif d.date() == (today_date - pd.Timedelta(days=1)):
            return '전일'
        
        # 금주/저번주/2주전
        if d >= this_week_start:
            return '금주'
        elif d >= last_week_start and d <= last_week_end:
            return '저번주'
        elif d >= two_weeks_start and d <= two_weeks_end:
            return '2주전'
        
        # 금월/전월/2개월전
        if d >= this_month_start:
            return '금월'
        elif d >= last_month_start:
            return '전월'
        elif d >= two_months_start:
            return '2개월전'
        
        return ''
    
    df['기간구분'] = df['order_daily'].apply(get_period_type)
    
    print(f"   ✅ 기간 구분 완료")
    print(f"   분포: {df['기간구분'].value_counts().to_dict()}")
    
    print(f"전처리 완료: {len(df):,}행")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    df.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"전처리: {len(df):,}행"



# ============================================================
# CSV 저장
# ============================================================
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