"""
io3.py - 범용 ETL 함수 모음
모든 파이프라인에서 사용하는 공통 로직
"""

import pandas as pd
from pathlib import Path
import hashlib

from modules.transform.utility.paths import TEMP_DIR, LOCAL_DB


# ============================================================
# 1. 수집 (Load)
# ============================================================
def load_files(
    patterns: list[str],
    search_paths: list[Path],
    xcom_key: str,
    file_type: str = 'auto',  # 'csv', 'excel', 'auto'
    header: int = 0,
    **context
) -> str:
    """
    범용 파일 로더
    
    Args:
        patterns: 파일 패턴 ['sales_*.csv', 'sales_*.xlsx']
        search_paths: 검색 경로 [Path('/data1'), Path('/data2')]
        xcom_key: XCom 저장 키
        file_type: 'csv', 'excel', 'auto' (기본값)
        header: 엑셀 헤더 행 (0부터)
        **context: Airflow context
    
    Returns:
        처리 결과 메시지
    
    Example:
        def load_order_df(**context):
            return load_files(
                patterns=['order_*.csv'],
                search_paths=[Path('/data')],
                xcom_key='order_path',
                **context
            )
    """
    ti = context['task_instance']
    
    # 1. 파일 검색
    all_files = []
    for search_path in search_paths:
        if not search_path.exists():
            continue
        for pattern in patterns:
            all_files.extend(list(search_path.glob(pattern)))
    
    if not all_files:
        print(f"[❌] 파일 없음 (패턴: {patterns})")
        ti.xcom_push(key=xcom_key, value=None)
        return "0건 (파일 없음)"
    
    # 2. 중복 제거 (같은 이름이면 최신 것만)
    unique_files = _get_unique_files(all_files)
    print(f"[✅] {len(unique_files)}개 파일 발견")
    
    # 3. 파일 타입 자동 판별
    if file_type == 'auto':
        first_file = unique_files[0]
        if first_file.suffix.lower() == '.csv':
            file_type = 'csv'
        elif first_file.suffix.lower() in ['.xlsx', '.xls']:
            file_type = 'excel'
        else:
            raise ValueError(f"지원하지 않는 파일 타입: {first_file.suffix}")
    
    # 4. 로드
    if file_type == 'csv':
        df = _load_csv_files(unique_files)
    elif file_type == 'excel':
        df = _load_excel_files(unique_files, header=header)
    else:
        raise ValueError(f"지원하지 않는 file_type: {file_type}")
    
    # 5. Parquet 저장
    output_path = _save_parquet(df, context, prefix=xcom_key.replace('_path', '_raw'))
    ti.xcom_push(key=xcom_key, value=str(output_path))
    
    return f"✅ {len(df):,}건 로드"


# ============================================================
# 2. 전처리 (Preprocess)
# ============================================================
def preprocess_df(
    input_xcom_key: str,
    output_xcom_key: str,
    transform_func=None,  # 커스텀 전처리 함수
    natural_keys: list[str] = None,  # Surrogate Key용
    **context
) -> str:
    """
    범용 전처리 함수
    
    Args:
        input_xcom_key: 읽을 XCom 키 (raw data)
        output_xcom_key: 저장할 XCom 키 (processed data)
        transform_func: 전처리 함수 (df → df)
        natural_keys: Surrogate Key 생성용 컬럼
        **context: Airflow context
    
    Returns:
        처리 결과 메시지
    
    Example:
        def my_transform(df):
            df = df[['date', 'sales']]
            df = df.groupby('date').sum().reset_index()
            return df
        
        def preprocess_order_df(**context):
            return preprocess_df(
                input_xcom_key='order_path',
                output_xcom_key='order_processed_path',
                transform_func=my_transform,
                natural_keys=['date', 'order_id'],
                **context
            )
    """
    ti = context['task_instance']
    
    # 1. 이전 데이터 가져오기
    parquet_path = ti.xcom_pull(key=input_xcom_key)
    
    if not parquet_path:
        print(f"[경고] 입력 데이터 없음")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "0건 (입력 없음)"
    
    df = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(df):,}행")
    
    # 2. 커스텀 전처리 실행
    if transform_func:
        df = transform_func(df)
        print(f"커스텀 전처리 완료: {len(df):,}행")
    
    # 3. Surrogate Key 추가
    if natural_keys:
        df = _add_surrogate_key(df, natural_keys)
        df.rename(columns={'key': 'id'}, inplace=True)
        print(f"Surrogate Key 생성 완료")
    
    # 4. Parquet 저장
    output_path = _save_parquet(df, context, prefix=output_xcom_key.replace('_path', ''))
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    
    return f"전처리: {len(df):,}행"


# ============================================================
# 3. 저장 (Save)
# ============================================================
def save_to_csv(
    input_xcom_key: str,
    output_filename: str,
    output_subdir: str = '영업관리부_DB',
    dedup_key: list[str] = None,
    **context
) -> str:
    """
    범용 CSV 저장 함수
    
    Args:
        input_xcom_key: 읽을 XCom 키
        output_filename: CSV 파일명
        output_subdir: LOCAL_DB 하위 폴더
        dedup_key: 중복 제거 키
        **context: Airflow context
    
    Returns:
        저장 결과 메시지
    
    Example:
        def save_order_csv(**context):
            return save_to_csv(
                input_xcom_key='order_processed_path',
                output_filename='order_data.csv',
                dedup_key=['id'],
                **context
            )
    """
    import os
    import shutil
    import tempfile
    
    ti = context['task_instance']
    
    # 1. 데이터 가져오기
    parquet_path = ti.xcom_pull(key=input_xcom_key)
    
    if not parquet_path or not os.path.exists(parquet_path):
        print(f"[경고] 저장할 데이터 없음")
        return "⚠️ 저장 스킵"
    
    df = pd.read_parquet(parquet_path)
    print(f"\n{'='*60}")
    print(f"[입력] {len(df):,}행 × {len(df.columns)}컬럼")
    
    # 2. 중복 제거
    if dedup_key:
        valid_cols = [c for c in dedup_key if c in df.columns]
        if valid_cols:
            before = len(df)
            df.drop_duplicates(subset=valid_cols, keep='first', inplace=True)
            after = len(df)
            if before - after > 0:
                print(f"[중복 제거] {before - after:,}건 제거")
    
    # 3. 경로 설정
    output_dir = LOCAL_DB / output_subdir
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / output_filename
    
    print(f"[경로] {csv_path}")
    
    # 4. datetime 포맷 변환
    datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
    if datetime_cols:
        for col in datetime_cols:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    
    # 5. 원자적 저장 (임시 파일 → 교체)
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', delete=False, 
            dir=str(csv_path.parent),
            prefix='tmp_', suffix='.csv', 
            encoding='utf-8-sig'
        ) as tmp_file:
            tmp_path = tmp_file.name
        
        df.to_csv(tmp_path, index=False, encoding='utf-8-sig')
        
        if csv_path.exists():
            backup = csv_path.parent / f"{csv_path.name}.bak"
            shutil.copy2(csv_path, backup)
            shutil.move(tmp_path, str(csv_path))
            backup.unlink()
        else:
            shutil.move(tmp_path, str(csv_path))
        
        csv_size = csv_path.stat().st_size / (1024 * 1024)
        print(f"[저장] ✅ CSV 저장: {len(df):,}건 ({csv_size:.2f} MB)")
        
    except Exception as e:
        print(f"[에러] CSV 저장 실패: {e}")
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        return f"저장 실패: {e}"
    
    print(f"{'='*60}\n")
    return f"✅ 저장: {len(df):,}건"


# ============================================================
# 내부 헬퍼 함수들
# ============================================================
def _get_unique_files(file_list):
    """중복 파일 제거 (최신 것만 유지)"""
    unique = {}
    for f in file_list:
        fname = f.name
        if fname not in unique or f.stat().st_mtime > unique[fname].stat().st_mtime:
            unique[fname] = f
    return list(unique.values())


def _load_csv_files(file_paths):
    """CSV 파일들 병합"""
    dfs = []
    for fpath in file_paths:
        print(f"   읽는 중: {fpath.name}")
        df = pd.read_csv(fpath)
        dfs.append(df)
        print(f"   ✓ {len(df):,}행")
    
    result = pd.concat(dfs, ignore_index=True)
    print(f"병합 완료: {len(result):,}행")
    return result


def _load_excel_files(file_paths, header=0):
    """엑셀 파일들 병합"""
    dfs = []
    for fpath in file_paths:
        print(f"   읽는 중: {fpath.name}")
        df = pd.read_excel(fpath, header=header)
        dfs.append(df)
        print(f"   ✓ {len(df):,}행")
    
    result = pd.concat(dfs, ignore_index=True)
    print(f"병합 완료: {len(result):,}행")
    return result


def _save_parquet(df, context, prefix):
    """Parquet 저장"""
    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    output_path = TEMP_DIR / f"{prefix}_{context['ds_nodash']}.parquet"
    df.to_parquet(output_path, index=False, engine='pyarrow')
    return output_path


def _add_surrogate_key(df, natural_key_cols):
    """Surrogate Key 생성"""
    key_parts = [df[col].astype(str) for col in natural_key_cols]
    uk_series = pd.concat(key_parts, axis=1).agg('|'.join, axis=1)
    df['key'] = uk_series.apply(
        lambda s: hashlib.sha1(s.encode('utf-8')).hexdigest()[:16]
    )
    cols = ['key'] + [c for c in df.columns if c != 'key']
    return df[cols]



# 조인용   
def join_dataframes(
    left_xcom_key,
    right_xcom_key,
    output_xcom_key,
    join_func,  # <- 사용자 정의 join 함수
    **context
):
    """
    두 DataFrame을 join하는 헬퍼 함수
    
    Parameters:
    -----------
    left_xcom_key : str
    right_xcom_key : str
    output_xcom_key : str
    join_func : callable
        join 로직을 담은 함수 (left_df, right_df를 받아서 result_df 반환)
    """
    import pandas as pd
    
    ti = context['ti']
    
    # XCom에서 로드
    left_path = ti.xcom_pull(key=left_xcom_key)
    right_path = ti.xcom_pull(key=right_xcom_key)
    
    left_df = pd.read_csv(left_path)
    right_df = pd.read_csv(right_path)
    
    print(f"Left: {len(left_df)} rows, Right: {len(right_df)} rows")
    
    # 사용자 정의 join 함수 실행
    result_df = join_func(left_df, right_df)
    
    print(f"Result: {len(result_df)} rows")
    
    # 임시 저장
    temp_path = Path('/tmp') / f'{output_xcom_key}_{context["ts_nodash"]}.csv'
    result_df.to_csv(temp_path, index=False)
    
    ti.xcom_push(key=output_xcom_key, value=str(temp_path))
    
    return str(temp_path)
