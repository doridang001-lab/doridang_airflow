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
from modules.transform.utility.store_name_mapping import normalize_store_names

PATH_NOW = COLLECT_DB / "영업관리부_수집" / "baemin_metrics_*.csv"
PATH_HISTORY = COLLECT_DB / "영업관리부_수집" / "baemin_change_history_*.csv"

# 수집 파일 위치가 환경별로 달라질 수 있어 루트/하위 폴더를 모두 탐색
BAEMIN_METRICS_PATTERNS = [
    COLLECT_DB / "영업관리부_수집" / "baemin_metrics_*.csv",
    COLLECT_DB / "baemin_metrics_*.csv",
]
BAEMIN_HISTORY_PATTERNS = [
    COLLECT_DB / "영업관리부_수집" / "baemin_change_history_*.csv",
    COLLECT_DB / "baemin_change_history_*.csv",
]

PATH_TOORDER = "/opt/airflow/download/toorder_review_doridang1_*.xlsx"
PATH_ORDERS_ALERTS = LOCAL_DB / "영업관리부_DB" / "sales_daily_orders_alerts.csv"


def _collect_files_from_patterns(patterns):
    """glob 패턴 리스트에서 파일을 모아 중복 제거(파일명 기준 최신 우선)"""
    all_files = []
    for pattern in patterns:
        all_files.extend([Path(p) for p in glob.glob(str(pattern))])

    unique_files = {}
    for f in all_files:
        fname = f.name
        if fname not in unique_files or f.stat().st_mtime > unique_files[fname].stat().st_mtime:
            unique_files[fname] = f

    return list(unique_files.values())


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
    onedrive_paths = [COLLECT_DB / "영업관리부_수집", COLLECT_DB]
    
    all_files = []
    
    # 업로드_temp에서 찾기
    if upload_temp_path.exists():
        temp_files = list(upload_temp_path.glob('baemin_metrics_*.csv'))
        if temp_files:
            print(f"[업로드_temp] {len(temp_files)}개 파일 발견")
            all_files.extend(temp_files)
    
    # 원드라이브에서 찾기 (하위 폴더 + 루트)
    for onedrive_path in onedrive_paths:
        if onedrive_path.exists():
            onedrive_files = list(onedrive_path.glob('baemin_metrics_*.csv'))
            if onedrive_files:
                print(f"[원드라이브] {onedrive_path}에서 {len(onedrive_files)}개 파일 발견")
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
    onedrive_paths = [COLLECT_DB / "영업관리부_수집", COLLECT_DB]
    
    all_files = []
    
    # 업로드_temp에서 찾기
    if upload_temp_path.exists():
        temp_files = list(upload_temp_path.glob('baemin_change_history_*.csv'))
        if temp_files:
            print(f"[업로드_temp] {len(temp_files)}개 파일 발견")
            all_files.extend(temp_files)
    
    # 원드라이브에서 찾기 (하위 폴더 + 루트)
    for onedrive_path in onedrive_paths:
        if onedrive_path.exists():
            onedrive_files = list(onedrive_path.glob('baemin_change_history_*.csv'))
            if onedrive_files:
                print(f"[원드라이브] {onedrive_path}에서 {len(onedrive_files)}개 파일 발견")
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
    - CSV 우선, 없으면 엑셀 로드, 없으면 누적 raw parquet 합산
    """
    upload_temp_paths = [
        Path('/opt/airflow/download/업로드_temp'),
        LOCAL_DB / '업로드_temp',
    ]
    download_path = Path('/opt/airflow/download')
    onedrive_path = COLLECT_DB / "영업관리부_수집"

    # 1. CSV 파일 찾기
    csv_files = []

    for upload_temp_path in upload_temp_paths:
        if upload_temp_path.exists():
            temp_csvs = list(upload_temp_path.glob('toorder_review_*.csv'))
            if temp_csvs:
                print(f"[업로드_temp] {upload_temp_path}에서 {len(temp_csvs)}개 CSV 발견")
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

    for upload_temp_path in upload_temp_paths:
        if upload_temp_path.exists():
            temp_excels = list(upload_temp_path.glob('toorder_review_doridang1_*.xlsx'))
            if temp_excels:
                print(f"[업로드_temp] {upload_temp_path}에서 {len(temp_excels)}개 엑셀 발견")
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
            try:
                df = pd.read_excel(fpath, header=3)
            except (ValueError, Exception) as e:
                print(f"   ⚠️ 건너뜀 (불량 파일): {Path(fpath).name} - {e}")
                continue
            
            if df.empty:
                print(f"   ⚠️ 건너뜀 (빈 데이터): {Path(fpath).name}")
                continue
            
            # 날짜 추출
            file_name = Path(fpath).name
            date_info = extract_date_from_toorder_filename(file_name)
            df['date'] = date_info['date']
            
            dfs.append(df)
            print(f"   ✓ {len(df)}행 로드")
        
        # 병합
        if not dfs:
            print(f"[❌ 에러] 유효한 토더 리뷰 파일 없음 (모두 불량 파일)")
            context['task_instance'].xcom_push(key='toorder_review_path', value=None)
            return "0건 (유효 파일 없음)"
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
    
    # 3. CSV/엑셀 없으면 temp의 누적 raw parquet 전체 합산
    raw_parquets = sorted(TEMP_DIR.glob('toorder_review_raw_*.parquet'))
    if raw_parquets:
        print(f"[✅ 누적 parquet 사용] {len(raw_parquets)}개 파일 발견")
        dfs = []
        for rp in raw_parquets:
            try:
                df = pd.read_parquet(rp)
                dfs.append(df)
            except Exception as e:
                print(f"   ⚠️ 건너뜀: {rp.name} - {e}")
        if not dfs:
            print(f"[❌ 에러] 유효한 parquet 없음")
            context['task_instance'].xcom_push(key='toorder_review_path', value=None)
            return "0건 (parquet 읽기 실패)"
        result_df = pd.concat(dfs, ignore_index=True)
        print(f"병합 완료: {len(result_df):,}행 (날짜 범위: {result_df['date'].min()} ~ {result_df['date'].max()})")
        temp_dir = TEMP_DIR
        temp_dir.mkdir(exist_ok=True, parents=True)
        output_path = temp_dir / f"toorder_review_raw_{context['ds_nodash']}.parquet"
        result_df.to_parquet(output_path, index=False, engine='pyarrow')
        context['task_instance'].xcom_push(key='toorder_review_path', value=str(output_path))
        return f"✅ {len(result_df):,}건 (누적 parquet 합산)"

    # 4. 파일 없음
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
    # ⭐ 중앙 매핑으로 통합 (store_name_mapping.py에서 관리)
    toorder_review_df["stores_name"] = normalize_store_names(toorder_review_df["stores_name"])
    
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
    now_files = _collect_files_from_patterns(BAEMIN_METRICS_PATTERNS)
    print(f"[배민 metrics] 탐색 패턴: {[str(p) for p in BAEMIN_METRICS_PATTERNS]}")
    print(f"[배민 metrics] 발견 파일: {len(now_files)}개")

    return load_data(
        file_path=now_files,
        xcom_key='baemin_store_now_path',
        use_glob=False,
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
    # ⭐ stores_name을 먼저 계산한 뒤 JOIN 키 기준으로 dedup
    #    (store_id 기준 dedup은 같은 stores_name이지만 다른 store_id가 남아
    #     [collected_date, stores_name] JOIN 시 팬아웃을 유발함)
    now_df["stores_name"] = now_df["store_name"].str.split(" ").str[-2:].str.join(" ")
    # ⭐ 지점명 변경 매핑 적용 (수집 시 이전 이름 → 현재 이름)
    now_df["stores_name"] = normalize_store_names(now_df["stores_name"])
    before_dedup = len(now_df)
    now_df.drop_duplicates(subset=['stores_name', 'collected_date'], keep='last', inplace=True)
    if before_dedup != len(now_df):
        print(f"[dedup] stores_name×collected_date 기준 {before_dedup - len(now_df)}건 제거 (팬아웃 방지)")
    
    col = ['collected_date', 'stores_name', '조리소요시간',
           '조리소요시간_순위비율', '주문접수시간', '주문접수시간_순위비율',
           '조리시간준수율', '조리시간준수율_순위비율', '주문접수율',
           '주문접수율_순위비율', '최근재주문율', '최근별점']
    
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
    history_files = _collect_files_from_patterns(BAEMIN_HISTORY_PATTERNS)
    print(f"[배민 history] 탐색 패턴: {[str(p) for p in BAEMIN_HISTORY_PATTERNS]}")
    print(f"[배민 history] 발견 파일: {len(history_files)}개")

    return load_data(
        file_path=history_files,
        xcom_key='baemin_history_path',
        use_glob=False,
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
    # ⭐ 지점명 변경 매핑 적용
    history_df["stores_name"] = normalize_store_names(history_df["stores_name"])
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
    # 1-0. ⭐ 선행 LEFT JOIN(배민 now / 토더 / 변경이력)에서 발생한 팬아웃 중복 제거
    #      [order_daily × 매장명] 이 결합 키이므로 여기서 dedup 해야 self-JOIN 시
    #      중복이 기하급수적으로 증폭되는 것을 방지할 수 있음
    # ==========================================
    if 'order_daily' in df.columns and '매장명' in df.columns:
        before_dedup = len(df)
        df.drop_duplicates(subset=['order_daily', '매장명'], keep='first', inplace=True)
        removed = before_dedup - len(df)
        if removed > 0:
            print(f"[선처리 dedup] order_daily×매장명 기준 {removed}건 중복 제거 (선행 JOIN 팬아웃 방지)")

    # ==========================================
    # 1-1. ⭐ 이전 파이프라인 실행에서 남은 파생 컬럼 제거
    #      (SMD_07이 CSV를 덮어쓰면서 enriched 컬럼이 들어올 수 있음)
    # ==========================================
    stale_prefixes = ('전일_', '전주동요일_', '전주_', '전월_', '전일대비_', '전주동요일대비_', 
                      '수수료율', 'join_pre_')
    stale_cols = [c for c in df.columns if c.startswith(stale_prefixes)]
    if stale_cols:
        df.drop(columns=stale_cols, inplace=True)
        print(f"[정리] 이전 실행 파생 컬럼 {len(stale_cols)}개 제거: {stale_cols[:5]}{'...' if len(stale_cols)>5 else ''}")

    # ==========================================
    # 2. 날짜 변환 및 요일 추가
    # ==========================================
    # ⭐ Excel 직렬값 처리를 포함한 날짜 변환
    df["order_daily"] = parse_mixed_date(df["order_daily"])
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
# 담당자별 점수 계산 (매장별 집계 데이터 → 담당자별 재집계)
# ============================================================
def calculate_manager_scores(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """
    매장별 집계 데이터를 담당자별로 재집계하여 점수 계산
    """
    import numpy as np
    from datetime import datetime, timedelta
    ti = context['task_instance']
    
    print(f"\n{'='*60}")
    print(f"[담당자 점수 계산] 시작...")
    
    # ============================================================
    # 1. 매장별 집계 데이터 로드
    # ============================================================
    parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)
    
    if not parquet_path:
        csv_path = LOCAL_DB / '영업관리부_DB' / 'sales_daily_orders.csv'
        
        if not csv_path.exists():
            print(f"[오류] 데이터 파일 없음")
            ti.xcom_push(key=output_xcom_key, value=None)
            return "0건 (데이터 없음)"
        
        print(f"[CSV 로드] {csv_path.name}")
        
        try:
            for encoding in ['utf-8-sig', 'utf-8', 'cp949']:
                try:
                    df = pd.read_csv(csv_path, encoding=encoding, low_memory=False)
                    print(f"[로드 성공] {len(df):,}건 ({encoding})")
                    break
                except UnicodeDecodeError:
                    continue
        except Exception as e:
            print(f"[오류] CSV 로드 실패: {e}")
            ti.xcom_push(key=output_xcom_key, value=None)
            return "CSV 로드 실패"
    else:
        print(f"[Parquet 로드] {parquet_path}")
        df = pd.read_parquet(parquet_path)
        print(f"[로드 성공] {len(df):,}건")
    
    # ⭐ order_date → order_daily 컬럼명 통일
    if 'order_date' in df.columns and 'order_daily' not in df.columns:
        df.rename(columns={'order_date': 'order_daily'}, inplace=True)
        print(f"[컬럼 변환] order_date → order_daily")
    
    # ⭐ store_names → 매장명 (담당자 집계용)
    if 'store_names' in df.columns and '매장명' not in df.columns:
        df.rename(columns={'store_names': '매장명'}, inplace=True)
        print(f"[컬럼 변환] store_names → 매장명")
    
    # ⭐ 필수 컬럼 확인
    required_cols = ['order_daily', '담당자', 'email']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"\n[❌ 에러] 필수 컬럼 없음: {missing_cols}")
        print(f"[사용 가능 컬럼] {df.columns.tolist()[:20]}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return f"0건 (필수 컬럼 없음: {missing_cols})"
    
    # ============================================================
    # 2. 날짜 변환 (⭐ 필터링 제거 - 모든 데이터 사용)
    # ============================================================
    df['order_daily'] = pd.to_datetime(df['order_daily'])
    
    print(f"[날짜 범위] {df['order_daily'].min()} ~ {df['order_daily'].max()}")
    print(f"[전체 데이터] {len(df):,}건")
    
    if len(df) == 0:
        print(f"[오류] 데이터 없음")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (데이터 없음)"
    
    # ============================================================
    # 3. 담당자별 일별 재집계
    # ============================================================
    print(f"\n[담당자별 재집계] 시작...")

    # ⭐ 집계 딕셔너리 생성
    agg_dict = {}

    if 'total_amount' in df.columns:
        agg_dict['total_amount'] = ('total_amount', 'sum')
    if 'fee_ad' in df.columns:
        agg_dict['fee_ad'] = ('fee_ad', 'sum')
    if 'settlement_amount' in df.columns:
        agg_dict['settlement_amount'] = ('settlement_amount', 'sum')
    if '매장명' in df.columns:
        agg_dict['store_count'] = ('매장명', 'nunique')
    if 'platform' in df.columns:
        agg_dict['platform'] = ('platform', lambda x: ','.join(sorted(set(str(v) for v in x.dropna() if str(v) != 'nan'))))

    # 주문 건수 계산
    if 'order_id' in df.columns:
        agg_dict['total_order_count'] = ('order_id', 'nunique')
    elif 'sub_order_id' in df.columns:
        agg_dict['total_order_count'] = ('sub_order_id', 'nunique')

    if not agg_dict:
        print(f"[❌ 에러] 집계 가능한 컬럼 없음")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (집계 컬럼 없음)"

    print(f"[집계 컬럼] {list(agg_dict.keys())}")

    # ⭐ groupby + agg 실행
    manager_daily = df.groupby(
        ['order_daily', '담당자', 'email'],
        dropna=False
    ).agg(**agg_dict).reset_index()

    # ⭐ 날짜 타입 명시적 변환
    manager_daily['order_daily'] = pd.to_datetime(manager_daily['order_daily'])

    # ARPU 계산
    if 'total_amount' in manager_daily.columns and 'total_order_count' in manager_daily.columns:
        manager_daily['ARPU'] = (
            manager_daily['total_amount'] / 
            manager_daily['total_order_count'].replace(0, np.nan)
        ).round(0).fillna(0).astype(int)

    print(f"[재집계 완료] {len(manager_daily):,}건")
    print(f"[담당자 수] {manager_daily['담당자'].nunique()}명")
    print(f"[날짜 범위] {manager_daily['order_daily'].min()} ~ {manager_daily['order_daily'].max()}")
    print(f"[컬럼] {manager_daily.columns.tolist()}")
    
    # ============================================================
    # 4. 담당자별 정렬 후 이동평균 계산
    # ============================================================
    print(f"\n[이동평균] 계산 중...")
    
    manager_daily = manager_daily.sort_values(['담당자', 'order_daily']).reset_index(drop=True)
    
    # ⭐ 요일 컬럼 먼저 생성
    manager_daily['weekday'] = manager_daily['order_daily'].dt.day_name()
    
    # ⭐ 담당자별 그룹 인덱스 생성 (shift 연산용)
    manager_daily['_담당자_idx'] = manager_daily.groupby('담당자').cumcount()
    manager_daily['_담당자_요일_idx'] = manager_daily.groupby(['담당자', 'weekday']).cumcount()
    
    # --- 이동평균 계산 ---
    # 14일 이동평균
    manager_daily['ma_14'] = manager_daily.groupby('담당자')['total_amount'].transform(
        lambda x: x.rolling(window=14, min_periods=7).mean()
    ).round(2)
    
    # 28일 이동평균
    manager_daily['ma_28'] = manager_daily.groupby('담당자')['total_amount'].transform(
        lambda x: x.rolling(window=28, min_periods=14).mean()
    ).round(2)
    
    # 최근 2주 평균 (= ma_14와 동일)
    manager_daily['current_avg_2week'] = manager_daily['ma_14'].copy()
    
    # --- 요일별 shift 계산 (⭐ 핵심 수정) ---
    # 담당자 + 요일별로 그룹화하여 매출만 shift
    def safe_shift(group, periods):
        """안전한 shift - 매출 컬럼만 shift"""
        result = group['total_amount'].shift(periods).fillna(0).astype(int)
        return result
    
    # 1주 전 동일 요일
    manager_daily['prev_week_same_day'] = manager_daily.groupby(
        ['담당자', 'weekday'], group_keys=False
    ).apply(lambda x: safe_shift(x, 1)).values
    
    # 2주 전 동일 요일
    manager_daily['prev_2week_same_day'] = manager_daily.groupby(
        ['담당자', 'weekday'], group_keys=False
    ).apply(lambda x: safe_shift(x, 2)).values
    
    # 3주 전 동일 요일
    manager_daily['prev_3week_same_day'] = manager_daily.groupby(
        ['담당자', 'weekday'], group_keys=False
    ).apply(lambda x: safe_shift(x, 3)).values
    
    # 4주 전 동일 요일
    manager_daily['prev_4week_same_day'] = manager_daily.groupby(
        ['담당자', 'weekday'], group_keys=False
    ).apply(lambda x: safe_shift(x, 4)).values
    
    # --- 7일 합계 계산 ---
    # 최근 7일 합계
    manager_daily['sum_7d_recent'] = manager_daily.groupby('담당자')['total_amount'].transform(
        lambda x: x.rolling(window=7, min_periods=1).sum()
    ).astype(int)
    
    # 이전 7일 합계 (8~14일 전)
    manager_daily['sum_7d_prev'] = manager_daily.groupby('담당자')['total_amount'].transform(
        lambda x: x.shift(7).rolling(window=7, min_periods=1).sum()
    ).fillna(0).astype(int)

    # --- 월간 누적합 계산 (당월 1일~오늘 vs 전월 1일~동일일자) ---
    manager_daily['_order_month'] = manager_daily['order_daily'].dt.to_period('M')
    manager_daily['_order_day'] = manager_daily['order_daily'].dt.day

    # 당월 누적합 → amt_curr_mtd
    manager_daily['amt_curr_mtd'] = manager_daily.groupby(
        ['담당자', '_order_month']
    )['total_amount'].cumsum().astype(int)

    # 전월 동일 일자 누적합 (lookup 방식, 임시 컨럼)
    _mtd_lookup = manager_daily[['담당자', '_order_month', '_order_day', 'amt_curr_mtd']].copy()
    _mtd_lookup['_join_month'] = _mtd_lookup['_order_month'] + 1  # 전월 데이터를 다음달과 매칭
    _mtd_lookup = _mtd_lookup.rename(columns={'amt_curr_mtd': 'amt_prev_mtd'})
    manager_daily = manager_daily.merge(
        _mtd_lookup[['담당자', '_join_month', '_order_day', 'amt_prev_mtd']],
        left_on=['담당자', '_order_month', '_order_day'],
        right_on=['담당자', '_join_month', '_order_day'],
        how='left'
    ).drop(columns=['_join_month'])
    manager_daily['amt_prev_mtd'] = manager_daily['amt_prev_mtd'].fillna(0).astype(int)
    manager_daily.drop(columns=['_order_month', '_order_day'], inplace=True)

    # ⭐ 임시 인덱스 컬럼 제거
    manager_daily.drop(columns=['_담당자_idx', '_담당자_요일_idx'], inplace=True)
    
    print(f"[이동평균] 완료")
    
    # ============================================================
    # 5. 점수 계산
    # ============================================================
    print(f"\n[점수 계산] 시작...")
    
    def calc_score(current, baseline):
        """점수 계산 (0~2점)"""
        if pd.isna(current) or pd.isna(baseline) or baseline == 0:
            return 0
        
        change_rate = (current - baseline) / baseline
        
        if change_rate >= 0:
            return 0
        elif change_rate > -0.1:
            return 1
        else:
            return 2
    
    # 1. 트렌드 점수 (14일MA vs 28일MA)
    manager_daily['score_trend'] = manager_daily.apply(
        lambda row: calc_score(row['ma_14'], row['ma_28']),
        axis=1
    )
    
    # 2. 일일 점수 (오늘 vs 전주 동일 요일)
    manager_daily['score_total'] = manager_daily.apply(
        lambda row: calc_score(row['total_amount'], row['prev_week_same_day']),
        axis=1
    )
    
    # 3. 주간 점수 (최근7일 vs 이전7일)
    manager_daily['score_7d_total'] = manager_daily.apply(
        lambda row: calc_score(row['sum_7d_recent'], row['sum_7d_prev']),
        axis=1
    )
    
    # 4. 월간 점수 (당월 누적합 vs 전월 동일 일자 누적합)
    manager_daily['score_4week_total'] = manager_daily.apply(
        lambda row: calc_score(row['amt_curr_mtd'], row['amt_prev_mtd']),
        axis=1
    )
    
    # 종합 점수 (0~6점, score_total 제외)
    manager_daily['score'] = (
        manager_daily['score_trend'] + 
        manager_daily['score_7d_total'] + 
        manager_daily['score_4week_total']
    )
    
    # 상태 판정
    manager_daily['status'] = np.where(
        manager_daily['score'] >= 5, '위험',      # 5~6점
        np.where(
            manager_daily['score'] >= 3, '주의',  # 3~4점
            np.where(
                manager_daily['score'] >= 2, '관심',  # 2점
                '정상'                                  # 0~1점
            )
        )
    )
    
    # 전주 상태 (영업일 기준 7일전)
    manager_daily['pre_status'] = manager_daily.groupby('담당자')['status'].shift(7).fillna('정상')
    
    print(f"[점수 분포]")
    print(f"  {manager_daily['score'].value_counts().sort_index().to_dict()}")
    print(f"[상태 분포]")
    print(f"  {manager_daily['status'].value_counts().to_dict()}")
    
    # ============================================================
    # 6. 전일/전주/전월 비교
    # ============================================================
    print(f"\n[비교 데이터] 계산 중...")

    # ⭐ 원본 데이터 백업
    manager_daily_original = manager_daily[['order_daily', '담당자', 'total_amount', 'settlement_amount']].copy()
    manager_daily_original['order_daily'] = pd.to_datetime(manager_daily_original['order_daily'])

    # ⭐ 주/월 단위 Period 추가
    manager_daily['order_week'] = manager_daily['order_daily'].dt.to_period('W')
    manager_daily['order_month'] = manager_daily['order_daily'].dt.to_period('M')

    # ────────────────────────────────────────
    # 1. 전일 비교 (일별) - 유지
    # ────────────────────────────────────────
    df_prev = manager_daily_original.copy()
    df_prev['join_date'] = df_prev['order_daily'] + pd.Timedelta(days=1)
    df_prev = df_prev.rename(columns={
        'total_amount': '전일_매출',
        'settlement_amount': '전일_정산금액'
    })
    df_prev = df_prev[['join_date', '담당자', '전일_매출', '전일_정산금액']]

    manager_daily = manager_daily.merge(
        df_prev,
        left_on=['order_daily', '담당자'],
        right_on=['join_date', '담당자'],
        how='left'
    ).drop(columns=['join_date'])

    print(f"  ✓ 전일 비교 완료")

    # ────────────────────────────────────────
    # 2-1. 전주 동요일 비교 (7일 전 동일 요일) - 기존 유지
    # ────────────────────────────────────────
    df_prev_week = manager_daily_original.copy()
    df_prev_week['join_date'] = df_prev_week['order_daily'] + pd.Timedelta(days=7)
    df_prev_week = df_prev_week.rename(columns={
        'total_amount': '전주_매출',  # ⭐ 전주 동요일
        'settlement_amount': '전주_정산금액'
    })
    df_prev_week = df_prev_week[['join_date', '담당자', '전주_매출', '전주_정산금액']]

    manager_daily = manager_daily.merge(
        df_prev_week,
        left_on=['order_daily', '담당자'],
        right_on=['join_date', '담당자'],
        how='left'
    ).drop(columns=['join_date'])

    print(f"  ✓ 전주 동요일 비교 완료")

    # ────────────────────────────────────────
    # 2-2. 전주 총합 비교 (주간 총합) - ⭐ 새로 추가
    # ────────────────────────────────────────
    # 담당자별 주간 총합 계산
    manager_daily_original['order_week'] = manager_daily_original['order_daily'].dt.to_period('W')

    weekly = manager_daily_original.groupby(['담당자', 'order_week']).agg({
        'total_amount': 'sum',
        'settlement_amount': 'sum'
    }).reset_index()

    # 전주 데이터 생성
    weekly['order_week_next'] = weekly['order_week'] + 1
    weekly = weekly.rename(columns={
        'total_amount': '전주_총매출',  # ⭐ 전주 7일 총합
        'settlement_amount': '전주_총정산금액'
    })

    # 조인
    manager_daily = manager_daily.merge(
        weekly[['담당자', 'order_week_next', '전주_총매출', '전주_총정산금액']],
        left_on=['담당자', 'order_week'],
        right_on=['담당자', 'order_week_next'],
        how='left'
    ).drop(columns=['order_week_next'])

    print(f"  ✓ 전주 총합 비교 완료")

    # ────────────────────────────────────────
    # 3. 전월 비교 (월 단위 총합)
    # ────────────────────────────────────────
    print(f"[전월 총합] 계산 중...")

    # 담당자별 월간 총합 계산
    manager_daily_original['order_month'] = manager_daily_original['order_daily'].dt.to_period('M')

    monthly_sum = manager_daily_original.groupby(['담당자', 'order_month']).agg({
        'total_amount': 'sum',
        'settlement_amount': 'sum'
    }).reset_index()

    # 전월 데이터 생성
    monthly_sum['order_month_next'] = monthly_sum['order_month'] + 1

    monthly_sum = monthly_sum.rename(columns={
        'total_amount': '전월_매출',  # ⭐ 컬럼명 통일
        'settlement_amount': '전월_정산금액'
    })

    # 조인
    manager_daily = manager_daily.merge(
        monthly_sum[['담당자', 'order_month_next', '전월_매출', '전월_정산금액']],
        left_on=['담당자', 'order_month'],
        right_on=['담당자', 'order_month_next'],
        how='left'
    ).drop(columns=['order_month_next'])

    print(f"   ✓ 전월 총합 완료")

    # ⭐ 날짜 타입 재확인
    manager_daily['order_daily'] = pd.to_datetime(manager_daily['order_daily'])

    # 결측치 0 처리
    for col in ['전일_매출', '전일_정산금액', 
                '전주_매출', '전주_정산금액',  # 전주 동요일
                '전주_총매출', '전주_총정산금액',  # 전주 총합 ⭐
                '전월_매출', '전월_정산금액']:
        if col in manager_daily.columns:
            manager_daily[col] = manager_daily[col].fillna(0).astype(int)

    # ────────────────────────────────────────
    # 4. 증감액/증감률 계산
    # ────────────────────────────────────────
    # 전일 대비 (일별)
    manager_daily['전일대비_증감액'] = (manager_daily['total_amount'] - manager_daily['전일_매출']).astype('Int64')
    manager_daily['전일대비_증감률'] = (
        (manager_daily['total_amount'] - manager_daily['전일_매출']) / 
        manager_daily['전일_매출'].replace(0, np.nan) * 100
    ).round(2)

    # ⭐ 전주 동요일 대비 (일별 vs 일별)
    manager_daily['전주동요일대비_증감액'] = (manager_daily['total_amount'] - manager_daily['전주_매출']).astype('Int64')
    manager_daily['전주동요일대비_증감률'] = (
        (manager_daily['total_amount'] - manager_daily['전주_매출']) / 
        manager_daily['전주_매출'].replace(0, np.nan) * 100
    ).round(2)

    # ⭐ 전주 총합 대비 (현재 주 누적 vs 전주 총합)
    manager_daily['현재주_누적매출'] = manager_daily.groupby(['담당자', 'order_week'])['total_amount'].transform('cumsum')

    manager_daily['전주대비_증감액'] = (manager_daily['현재주_누적매출'] - manager_daily['전주_총매출']).astype('Int64')
    manager_daily['전주대비_증감률'] = (
        (manager_daily['현재주_누적매출'] - manager_daily['전주_총매출']) / 
        manager_daily['전주_총매출'].replace(0, np.nan) * 100
    ).round(2)

    # ⭐ 전월 대비 (현재 월 누적 vs 전월 총합)
    manager_daily['현재월_누적매출'] = manager_daily.groupby(['담당자', 'order_month'])['total_amount'].transform('cumsum')

    manager_daily['전월대비_증감액'] = (manager_daily['현재월_누적매출'] - manager_daily['전월_매출']).astype('Int64')
    manager_daily['전월대비_증감률'] = (
        (manager_daily['현재월_누적매출'] - manager_daily['전월_매출']) / 
        manager_daily['전월_매출'].replace(0, np.nan) * 100
    ).round(2)

    # 정산금액 비교
    manager_daily['전일대비_정산증감액'] = (manager_daily['settlement_amount'] - manager_daily['전일_정산금액']).astype('Int64')
    manager_daily['전일대비_정산증감률'] = (
        (manager_daily['settlement_amount'] - manager_daily['전일_정산금액']) / 
        manager_daily['전일_정산금액'].replace(0, np.nan) * 100
    ).round(2)

    # 전주 총합 정산금액 비교
    manager_daily['전주대비_정산증감액'] = (manager_daily['settlement_amount'] - manager_daily['전주_총정산금액']).astype('Int64')
    manager_daily['전주대비_정산증감률'] = (
        (manager_daily['settlement_amount'] - manager_daily['전주_총정산금액']) / 
        manager_daily['전주_총정산금액'].replace(0, np.nan) * 100
    ).round(2)

    # 전월 정산금액 비교
    manager_daily['전월대비_정산증감액'] = (manager_daily['settlement_amount'] - manager_daily['전월_정산금액']).astype('Int64')
    manager_daily['전월대비_정산증감률'] = (
        (manager_daily['settlement_amount'] - manager_daily['전월_정산금액']) / 
        manager_daily['전월_정산금액'].replace(0, np.nan) * 100
    ).round(2)

    # ⭐ 주/월 단위 컬럼 제거 (저장 전 정리)
    manager_daily.drop(columns=['order_week', 'order_month', '현재주_누적매출', '현재월_누적매출'], inplace=True, errors='ignore')

    print(f"[비교 데이터] 완료")
    
    # ============================================================
    # 7. 저장
    # ============================================================
    temp_dir = LOCAL_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    manager_daily.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    
    print(f"\n[저장] {output_path.name}")
    print(f"[저장 완료] {len(manager_daily):,}건")
    print(f"[컬럼 수] {len(manager_daily.columns)}개")
    print(f"{'='*60}\n")
    
    return f"담당자별 점수: {len(manager_daily):,}건"

# ============================================================
# 담당자별 점수 조인 함수
# ============================================================
def left_join_manager_scores(
    left_task,
    right_task,
    on=None,
    left_on=["order_daily", "담당자"],
    right_on=["order_daily", "담당자"],
    how='left',
    drop_columns=None,
    output_xcom_key='final_preprocessed_with_scores_path',
    **context
):
    """
    매장별 집계 데이터에 담당자별 점수 조인
    
    ⭐ 주의:
    - 매장별 데이터 (왼쪽): order_daily, 매장명, 담당자
    - 담당자별 데이터 (오른쪽): order_daily, 담당자 (매장명 없음)
    - 조인 키: order_daily + 담당자
    - 같은 날짜/담당자의 모든 매장에 동일한 담당자 점수가 붙음
    """
    ti = context['task_instance']
    
    # Task 정보 파싱
    if isinstance(left_task, str):
        left_task = {'task_id': left_task, 'xcom_key': 'final_preprocessed_path'}
    if isinstance(right_task, str):
        right_task = {'task_id': right_task, 'xcom_key': 'manager_scores_path'}
    
    # 왼쪽 데이터 (매장별)
    left_path = ti.xcom_pull(
        task_ids=left_task['task_id'],
        key=left_task['xcom_key']
    )
    if not left_path:
        print(f"[에러] 왼쪽 데이터 없음: {left_task['task_id']}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "join 실패: 왼쪽 데이터 없음"
    
    left_df = pd.read_parquet(left_path)
    print(f"[왼쪽] 매장별 데이터: {len(left_df):,}행 × {len(left_df.columns)}컬럼")
    
    # 오른쪽 데이터 (담당자별)
    right_path = ti.xcom_pull(
        task_ids=right_task['task_id'],
        key=right_task['xcom_key']
    )
    
    if not right_path:
        print(f"[경고] 담당자별 점수 데이터 없음 - 왼쪽 데이터만 저장")
        temp_dir = TEMP_DIR
        temp_dir.mkdir(exist_ok=True, parents=True)
        output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
        left_df.to_parquet(output_path, index=False, engine='pyarrow')
        ti.xcom_push(key=output_xcom_key, value=str(output_path))
        return f"⚠️ 담당자 점수 없음, 매장별 데이터만 저장: {len(left_df):,}행"
    
    right_df = pd.read_parquet(right_path)
    print(f"[오른쪽] 담당자별 데이터: {len(right_df):,}행 × {len(right_df.columns)}컬럼")
    
    # ⭐ 담당자별 컬럼에 접두사 추가 (매장별 컬럼과 구분)
    # 조인 키 제외하고 모든 컬럼에 '담당자_' 접두사
    rename_dict = {}
    for col in right_df.columns:
        if col not in ['order_daily', '담당자', 'email']:  # 조인 키 + email은 유지
            rename_dict[col] = f'담당자_{col}'
    
    right_df = right_df.rename(columns=rename_dict)
    
    print(f"\n[컬럼 변환] 담당자별 컬럼에 '담당자_' 접두사 추가")
    print(f"  예시: total_amount → 담당자_total_amount")
    print(f"  변환된 컬럼 수: {len(rename_dict)}개")
    
    # JOIN 실행
    print(f"\n[JOIN] 조인 중...")
    print(f"  방식: {how}")
    print(f"  조인 키: {left_on}")
    
    joined_df = left_df.merge(
        right_df,
        left_on=left_on,
        right_on=right_on,
        how=how,
        suffixes=('', '_담당자중복')  # 혹시 모를 중복 대비
    )
    
    print(f"[JOIN] 완료: {len(joined_df):,}행 × {len(joined_df.columns)}컬럼")
    
    # ⭐ 중복 컬럼 확인 및 제거
    dup_cols = [col for col in joined_df.columns if col.endswith('_담당자중복')]
    if dup_cols:
        print(f"[정리] 중복 컬럼 제거: {dup_cols}")
        joined_df.drop(columns=dup_cols, inplace=True)
    
    # email 컬럼 중복 처리 (email_x, email_y)
    if 'email_x' in joined_df.columns and 'email_y' in joined_df.columns:
        # 둘 중 하나 선택 (보통 왼쪽 우선)
        joined_df['email'] = joined_df['email_x'].fillna(joined_df['email_y'])
        joined_df.drop(columns=['email_x', 'email_y'], inplace=True)
        print(f"[정리] email_x, email_y → email 통합")
    
    # ⭐ 조인 결과 검증
    matched = joined_df['담당자_total_amount'].notna().sum() if '담당자_total_amount' in joined_df.columns else 0
    match_rate = matched / len(joined_df) * 100 if len(joined_df) > 0 else 0
    
    print(f"\n[검증] 담당자 점수 매칭률: {matched:,}건 / {len(joined_df):,}건 ({match_rate:.1f}%)")
    
    if match_rate < 50:
        print(f"⚠️ 매칭률 낮음! order_daily, 담당자 컬럼 확인 필요")
    
    # Parquet 저장
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    joined_df.to_parquet(output_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    
    print(f"\n[저장] {output_path.name}")
    print(f"{'='*60}\n")
    
    return f"✅ 담당자 점수 조인 완료: {len(joined_df):,}행 ({match_rate:.1f}% 매칭)"


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