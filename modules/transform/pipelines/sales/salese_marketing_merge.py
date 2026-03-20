import pandas as pd
import numpy as np
from pathlib import Path
import hashlib
import glob

from modules.load.load_df_glob import load_data
from modules.transform.utility.paths import TEMP_DIR, LOCAL_DB, COLLECT_DB
from modules.transform.utility.io import load_and_concat_csv, save_to_parquet


# ============================================================
# 경로 설정
# ============================================================
PATH_TOORDER = "/opt/airflow/download/업로드_temp"
PATH_BACKUP = LOCAL_DB / "영업관리부_DB"


# ============================================================
# 공통 유틸리티 함수
# ============================================================
def add_surrogate_key(df: pd.DataFrame, natural_key_cols: list[str]) -> pd.DataFrame:
    """
    자연키 기반 Surrogate Key 생성
    
    Args:
        df: DataFrame
        natural_key_cols: 자연키로 사용할 컬럼 리스트
        
    Returns:
        'key' 컬럼이 추가된 DataFrame
        
    Example:
        df = add_surrogate_key(df, natural_key_cols=["일자", "주문번호"])
    """
    out = df.copy()
    key_parts = [df[col].astype(str) for col in natural_key_cols]
    uk_series = pd.concat(key_parts, axis=1).agg('|'.join, axis=1)
    out['key'] = uk_series.apply(
        lambda s: hashlib.sha1(s.encode('utf-8')).hexdigest()[:16]
    )
    cols = ['key'] + [c for c in out.columns if c != 'key']
    out = out[cols]
    return out


def get_unique_files(file_list):
    """
    파일 리스트에서 중복 제거 (같은 이름이면 최신 파일만 유지)
    
    Args:
        file_list: Path 객체 리스트
        
    Returns:
        중복 제거된 Path 객체 리스트
        
    Example:
        >>> files = [
        ...     Path('/path1/data_20260101.csv'),
        ...     Path('/path2/data_20260101.csv'),  # 같은 이름 (더 최신)
        ...     Path('/path1/data_20260102.csv'),
        ... ]
        >>> unique = get_unique_files(files)
        >>> # 결과: path2의 data_20260101.csv + data_20260102.csv
    """
    unique_files = {}
    for f in file_list:
        fname = f.name
        if fname not in unique_files or f.stat().st_mtime > unique_files[fname].stat().st_mtime:
            unique_files[fname] = f
    return list(unique_files.values())


def load_and_concat_csv(file_paths):
    """
    CSV 파일들을 읽어서 병합
    
    Args:
        file_paths: 파일 경로 리스트
        
    Returns:
        병합된 DataFrame
        
    Example:
        >>> files = [
        ...     Path('/data/sales_2026_01.csv'),
        ...     Path('/data/sales_2026_02.csv'),
        ... ]
        >>> df = load_and_concat_csv(files)
        >>> print(len(df))  # 두 파일의 총 행 수
    """
    dfs = []
    for fpath in file_paths:
        print(f"   읽는 중: {fpath}")
        df = pd.read_csv(fpath)
        dfs.append(df)
        print(f"   ✓ {len(df)}행 로드")
    
    result_df = pd.concat(dfs, ignore_index=True)
    print(f"병합 완료: {len(result_df):,}행")
    return result_df


def load_and_concat_excel(file_paths, header=3, date_extractor=None):
    """
    엑셀 파일들을 읽어서 병합
    
    Args:
        file_paths: 파일 경로 리스트
        header: 헤더 행 번호 (0부터 시작, 기본값 3)
        date_extractor: 파일명에서 날짜를 추출하는 함수 (선택)
        
    Returns:
        병합된 DataFrame
        
    Example:
        >>> files = [Path('/data/report_20260101.xlsx')]
        >>> df = load_and_concat_excel(
        ...     files, 
        ...     header=2,  # 3번째 행이 헤더
        ...     date_extractor=extract_date_from_filename
        ... )
        >>> print(df.columns)  # date 컬럼 자동 추가됨
    """
    dfs = []
    for fpath in file_paths:
        print(f"   읽는 중: {fpath}")
        df = pd.read_excel(fpath, header=header)
        
        if date_extractor:
            file_name = Path(fpath).name
            date_info = date_extractor(file_name)
            df['date'] = date_info['date']
        
        dfs.append(df)
        print(f"   ✓ {len(df)}행 로드")
    
    result_df = pd.concat(dfs, ignore_index=True)
    print(f"병합 완료: {len(result_df):,}행")
    return result_df


def save_to_parquet(df, context, filename_prefix):
    """
    DataFrame을 Parquet으로 저장
    
    Args:
        df: 저장할 DataFrame
        context: Airflow context (ds_nodash 포함)
        filename_prefix: 파일명 prefix
        
    Returns:
        저장된 파일 경로 (Path 객체)
        
    Example:
        >>> # Airflow Task 내부에서
        >>> output_path = save_to_parquet(
        ...     df, 
        ...     context, 
        ...     "sales_data_raw"
        ... )
        >>> # 결과: /tmp/sales_data_raw_20260119.parquet
        >>> context['task_instance'].xcom_push(
        ...     key='data_path', 
        ...     value=str(output_path)
        ... )
    """
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    output_path = temp_dir / f"{filename_prefix}_{context['ds_nodash']}.parquet"
    df.to_parquet(output_path, index=False, engine='pyarrow')
    return output_path


def extract_date_from_filename(file_name):
    """
    파일명에서 날짜 추출 (마지막 언더스코어 뒤의 숫자)
    
    Args:
        file_name: 파일명 (str)
        
    Returns:
        {'date': 날짜문자열} 또는 {'date': None}
        
    Example:
        >>> extract_date_from_filename('toorder_review_20260119.csv')
        {'date': '20260119'}
        >>> extract_date_from_filename('data_v2_20260101.xlsx')
        {'date': '20260101'}
    """
    try:
        date_str = file_name.split('_')[-1].split('.')[0]
        return {'date': date_str}
    except Exception as e:
        print(f"[경고] 날짜 추출 실패: {file_name}, {e}")
        return {'date': None}

# ============================================================
# 메인 로드 함수
# ============================================================
def load_generic_df(**context):
    """
    # 1. 스마트 토더 리뷰 수집 (CSV)
    task_toorder_review = PythonOperator(
        task_id='load_toorder_review',
        python_callable=load_generic_df,
        op_kwargs={
            'base_paths': [Path('/opt/airflow/download/업로드_temp'), COLLECT_DB / "영업관리부_수집"],
            'file_pattern': 'toorder_review_*',
            'xcom_key': 'toorder_review',
            'file_type': 'csv'
        }
    )
    }
    """
    # 1. 파라미터 수신 (op_kwargs에서 전달된 값들)
    base_paths = context.get('base_paths', [])
    file_pattern = context.get('file_pattern')
    xcom_key = context.get('xcom_key')
    file_type = context.get('file_type', 'csv').lower()
    excel_header = context.get('excel_header', 3)
    
    found_files = []
    
    # 2. 지정된 모든 경로에서 패턴에 맞는 파일 수집
    for path in base_paths:
        search_path = Path(path)
        if search_path.exists():
            # 확장자를 포함한 전체 패턴 생성
            full_pattern = f"{file_pattern}.{file_type}"
            found_files.extend(list(search_path.glob(full_pattern)))
    
    # 3. 파일 처리 로직
    if found_files:
        print(f"[✅ 파일 발견] 총 {len(found_files)}개 파일 발견 ({file_type})")
        
        # 중복 파일명 제거 및 최신 파일 선택 (기존 공용 함수 활용)
        file_paths = get_unique_files(found_files)
        print(f"[중복 제거] {len(file_paths)}개 파일 최종 사용")
        
        # 데이터프레임 로드 (CSV / Excel 구분)
        if file_type == 'csv':
            result_df = load_and_concat_csv(file_paths)
        else:
            result_df = load_and_concat_excel(
                file_paths, 
                header=excel_header, 
                date_extractor=extract_date_from_filename
            )
            
        # Parquet 저장 및 XCom 푸시
        output_path = save_to_parquet(result_df, context, f"{xcom_key}_raw")
        
        context['task_instance'].xcom_push(
            key=f"{xcom_key}_path",
            value=str(output_path)
        )
        return f"✅ {xcom_key}: {len(result_df):,}건 처리 완료"

    # 4. 파일이 없는 경우 처리
    print(f"[⚠️ 알림] {file_pattern} 파일을 찾을 수 없습니다.")
    context['task_instance'].xcom_push(key=f"{xcom_key}_path", value=None)
    return f"0건 (파일 없음: {file_pattern})"



# ============================================================
# 기존 함수 (호환성 유지)
# ============================================================
def load_toorder_review_df(**context):
    """토더 리뷰 엑셀 로드 (레거시 함수 - load_df 사용 권장)"""
    ti = context['task_instance']
    
    file_list = sorted(glob.glob(str(PATH_TOORDER)))
    print(f"[로드] 찾은 파일: {len(file_list)}개")
    
    if not file_list:
        ti.xcom_push(key='toorder_review_path', value=None)
        return "0건 (파일 없음)"
    
    file_paths = [Path(f) for f in file_list]
    result_df = load_and_concat_excel(file_paths, header=3, date_extractor=extract_date_from_filename)
    output_path = save_to_parquet(result_df, context, "toorder_review_raw")
    
    ti.xcom_push(key='toorder_review_path', value=str(output_path))
    return f"{len(result_df):,}건"


def preprocess_toorder_review_df(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """
    토더 리뷰 데이터 전처리 (집계 + 표준화)
    
    🔄 처리 단계:
      1. 컬럼 선택: date, 매장명.1, 채널, 주문 수, 리뷰 수, 답변완료 수, 평균 별점
      2. 결측치 제거: 채널이 null인 행 제거
      3. 그룹화: date + 매장명.1 기준 집계
      4. ID 생성: date + stores_name 기반 해시 키
      5. 매장명 표준화: "도리당" 접두사 추가, 별칭 통일
    
    Args:
        input_task_id: 이전 Task의 task_id
        input_xcom_key: 읽을 XCom 키
        output_xcom_key: 저장할 XCom 키
        **context: Airflow context
    
    Returns:
        str: 처리 결과 메시지
    
    XCom:
        입력: key=input_xcom_key (Parquet 경로)
        출력: key=output_xcom_key (전처리된 Parquet 경로)
    
    💡 DAG에서 사용 예시:
        task_preprocess = PythonOperator(
            task_id='preprocess_data',
            python_callable=preprocess_toorder_review_df,
            op_kwargs={
                'input_task_id': 'load_data',             # 이전 Task
                'input_xcom_key': 'raw_data_path',        # 읽을 키
                'output_xcom_key': 'processed_data_path', # 저장할 키
            }
        )
    """
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
    
    # Surrogate key 추가 (집계 후 최종 데이터에 대해)
    toorder_review_df = add_surrogate_key(toorder_review_df, natural_key_cols=["date", "stores_name"])
    toorder_review_df.rename(columns={'key': 'id'}, inplace=True)
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
# CSV로 저장
# ============================================================
def fin_save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename='toorder_review_doridang.csv',
    output_subdir='영업관리부_DB',
    dedup_key=None,
    **context
):
    """
    Parquet 데이터를 로컬 DB에 CSV로 저장 (안전한 원자적 쓰기)
    
    💾 주요 기능:
      - Parquet을 CSV로 변환
      - 기존 파일 자동 백업 (.bak)
      - 임시 파일로 쓴 후 원자적 교체
      - datetime 컬럼 자동 포맷팅
      - 중복 제거 (선택)
    
    Args:
        input_task_id: 이전 Task의 task_id
        input_xcom_key: 읽을 XCom 키
        output_csv_path: 직접 경로 지정 (선택)
        output_filename: CSV 파일명 (기본값: 'toorder_review_doridang.csv')
        output_subdir: LOCAL_DB 하위 폴더 (기본값: '영업관리부_DB')
        dedup_key: 중복 제거 키 (str 또는 list, 선택)
        **context: Airflow context
    
    Returns:
        str: 저장 결과 메시지
    
    XCom 입력:
        key=input_xcom_key: Parquet 파일 경로
    
    💡 DAG에서 사용 예시 1 (기본):
        task_save = PythonOperator(
            task_id='save_csv',
            python_callable=fin_save_to_csv,
            op_kwargs={
                'input_task_id': 'preprocess_data',
                'input_xcom_key': 'processed_data_path',
                'output_filename': 'my_data.csv',
            }
        )
        # 결과: LOCAL_DB/영업관리부_DB/my_data.csv
    
    💡 사용 예시 2 (중복 제거 + 커스텀 폴더):
        task_save = PythonOperator(
            task_id='save_csv',
            python_callable=fin_save_to_csv,
            op_kwargs={
                'input_task_id': 'preprocess_data',
                'input_xcom_key': 'processed_data_path',
                'output_filename': 'unique_sales.csv',
                'output_subdir': '매출_데이터/월별',
                'dedup_key': ['date', 'order_id'],  # 중복 제거 키
            }
        )
        # 결과: LOCAL_DB/매출_데이터/월별/unique_sales.csv
    
    💡 사용 예시 3 (절대 경로 지정):
        task_save = PythonOperator(
            task_id='save_csv',
            python_callable=fin_save_to_csv,
            op_kwargs={
                'input_task_id': 'preprocess_data',
                'input_xcom_key': 'processed_data_path',
                'output_csv_path': '/custom/path/data.csv',
            }
        )
    """
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


# ============================================================

# ============================================================
# 기존 함수 (호환성 유지) baemin_ad_change_history_*.csv 로드
# ============================================================
def load_baemin_ad_change_history_df(**context):
    """배민 로드 (레거시 함수 - load_df 사용 권장)"""
    ti = context['task_instance']
    
    file_list = sorted(glob.glob(str(PATH_TOORDER)))
    print(f"[로드] 찾은 파일: {len(file_list)}개")
    
    if not file_list:
        ti.xcom_push(key='baemin_ad_change_history_path', value=None)
        return "0건 (파일 없음)"
    
    file_paths = [Path(f) for f in file_list]
    result_df = load_and_concat_excel(file_paths, header=3, date_extractor=extract_date_from_filename)
    output_path = save_to_parquet(result_df, context, "baemin_ad_change_history_raw")
    
    ti.xcom_push(key='baemin_ad_change_history_path', value=str(output_path))
    return f"{len(result_df):,}건"

# ============================================================
# baemin_ad_change_history_df 전처리
# ============================================================
def preprocess_baemin_ad_change_history_df(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """
    토더 리뷰 데이터 전처리 (집계 + 표준화)
    
    🔄 처리 단계:
      1. 컬럼 선택: date, 매장명.1, 채널, 주문 수, 리뷰 수, 답변완료 수, 평균 별점
      2. 결측치 제거: 채널이 null인 행 제거
      3. 그룹화: date + 매장명.1 기준 집계
      4. ID 생성: date + stores_name 기반 해시 키
      5. 매장명 표준화: "도리당" 접두사 추가, 별칭 통일
    
    Args:
        input_task_id: 이전 Task의 task_id
        input_xcom_key: 읽을 XCom 키
        output_xcom_key: 저장할 XCom 키
        **context: Airflow context
    
    Returns:
        str: 처리 결과 메시지
    
    XCom:
        입력: key=input_xcom_key (Parquet 경로)
        출력: key=output_xcom_key (전처리된 Parquet 경로)
    
    💡 DAG에서 사용 예시:
        task_preprocess = PythonOperator(
            task_id='preprocess_data',
            python_callable=preprocess_toorder_review_df,
            op_kwargs={
                'input_task_id': 'load_data',             # 이전 Task
                'input_xcom_key': 'raw_data_path',        # 읽을 키
                'output_xcom_key': 'processed_data_path', # 저장할 키
            }
        )
    """
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(
        task_ids=input_task_id,
        key=input_xcom_key
    )
    
    if not parquet_path:
        print(f"[경고] 토더 리뷰 데이터 없음 - 스킵")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 데이터 없음)"
    
    df = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(df):,}행")
    
    
    # Surrogate key 추가 (집계 후 최종 데이터에 대해)
    df = add_surrogate_key(df, natural_key_cols=["매장명", "변경시간"])
    df.rename(columns={'key': 'id'}, inplace=True)
    
    print(f"전처리 완료: {len(df):,}행")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    df.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"전처리: {len(df):,}행"


# ============================================================
# csv 저장
# ===========================================================
def baemin_ad_change_history_save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename='baemin_ad_change_history.csv',
    output_subdir='영업관리부_DB',
    dedup_key=["id"],
    **context
):
    """
    Parquet 데이터를 로컬 DB에 CSV로 저장 (안전한 원자적 쓰기)
    
    💾 주요 기능:
      - Parquet을 CSV로 변환
      - 기존 파일 자동 백업 (.bak)
      - 임시 파일로 쓴 후 원자적 교체
      - datetime 컬럼 자동 포맷팅
      - 중복 제거 (선택)
    
    Args:
        input_task_id: 이전 Task의 task_id
        input_xcom_key: 읽을 XCom 키
        output_csv_path: 직접 경로 지정 (선택)
        output_filename: CSV 파일명 (기본값: 'toorder_review_doridang.csv')
        output_subdir: LOCAL_DB 하위 폴더 (기본값: '영업관리부_DB')
        dedup_key: 중복 제거 키 (str 또는 list, 선택)
        **context: Airflow context
    
    Returns:
        str: 저장 결과 메시지
    
    XCom 입력:
        key=input_xcom_key: Parquet 파일 경로
    
    💡 DAG에서 사용 예시 1 (기본):
        task_save = PythonOperator(
            task_id='save_csv',
            python_callable=fin_save_to_csv,
            op_kwargs={
                'input_task_id': 'preprocess_data',
                'input_xcom_key': 'processed_data_path',
                'output_filename': 'my_data.csv',
            }
        )
        # 결과: LOCAL_DB/영업관리부_DB/my_data.csv
    
    💡 사용 예시 2 (중복 제거 + 커스텀 폴더):
        task_save = PythonOperator(
            task_id='save_csv',
            python_callable=fin_save_to_csv,
            op_kwargs={
                'input_task_id': 'preprocess_data',
                'input_xcom_key': 'processed_data_path',
                'output_filename': 'unique_sales.csv',
                'output_subdir': '매출_데이터/월별',
                'dedup_key': ['date', 'order_id'],  # 중복 제거 키
            }
        )
        # 결과: LOCAL_DB/매출_데이터/월별/unique_sales.csv
    
    💡 사용 예시 3 (절대 경로 지정):
        task_save = PythonOperator(
            task_id='save_csv',
            python_callable=fin_save_to_csv,
            op_kwargs={
                'input_task_id': 'preprocess_data',
                'input_xcom_key': 'processed_data_path',
                'output_csv_path': '/custom/path/data.csv',
            }
        )
    """
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
