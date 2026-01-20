"""
DB 파일 병합 파이프라인 - 간편 버전

📋 주요 기능:
  1. 여러 경로에서 CSV/엑셀 파일 자동 탐색 및 병합
  2. 중복 파일 자동 제거 (최신 파일 우선)
  3. 데이터 전처리 및 집계
  4. Surrogate Key 생성
  5. 로컬 DB에 CSV 저장

🚀 NEW! 간편 함수들:
  - create_auto_loader(): 자동 로더 함수 생성
  - create_simple_preprocessor(): 간단한 전처리 함수 생성
  - create_pipeline_tasks(): 전체 파이프라인을 한 번에 생성

💡 사용 예시:
    # 기존 방식 (복잡함)
    task_load = PythonOperator(task_id='load', python_callable=load_df, ...)
    task_preprocess = PythonOperator(task_id='preprocess', ...)
    task_save = PythonOperator(task_id='save', ...)
    task_load >> task_preprocess >> task_save
    
    # 새로운 방식 (간편함)
    tasks = create_pipeline_tasks(
        dag=dag,
        pipeline_name='sales',
        loader_config={'file_pattern': 'sales_*.csv'},
        preprocess_config={'groupby_cols': ['date', 'store'], ...},
        save_config={'output_filename': 'sales.csv'}
    )
"""

import pandas as pd
import numpy as np
from pathlib import Path
import hashlib
import glob

from modules.load.load_df_glob import load_data
from modules.transform.utility.paths import TEMP_DIR, LOCAL_DB, COLLECT_DB


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
# 🚀 간편 파이프라인 생성 함수들
# ============================================================

def create_auto_loader(
    file_pattern,
    search_paths=None,
    file_type='auto',
    excel_header=3,
    date_extractor=None,
    output_prefix='data_raw',
    xcom_key=None
):
    """
    자동 파일 로더 함수 생성기
    
    📦 기능:
      - 지정된 경로에서 파일 자동 탐색
      - CSV/엑셀 자동 선택
      - 중복 파일 제거
      - Parquet으로 저장
    
    Args:
        file_pattern: 파일명 패턴 (예: 'sales_*.csv', 'report_*.xlsx')
        search_paths: 탐색 경로 리스트 (기본값: [upload_temp, onedrive_collect])
        file_type: 파일 타입 ('csv', 'excel', 'auto')
        excel_header: 엑셀 헤더 행 번호 (기본값: 3)
        date_extractor: 날짜 추출 함수 (기본값: extract_date_from_filename)
        output_prefix: 출력 파일명 prefix
        xcom_key: XCom 키 (기본값: output_prefix + '_path')
    
    Returns:
        Airflow Task에서 사용할 수 있는 함수
    
    💡 사용 예시:
        # 1. 로더 생성
        loader = create_auto_loader(
            file_pattern='sales_*.csv',
            search_paths=[Path('/data/sales'), COLLECT_DB / '매출'],
            output_prefix='sales_raw'
        )
        
        # 2. DAG에서 사용
        task = PythonOperator(
            task_id='load_sales',
            python_callable=loader
        )
        
        # 결과: XCom['sales_raw_path'] = parquet 경로
    """
    if search_paths is None:
        upload_temp = Path('/opt/airflow/download/업로드_temp')
        onedrive = COLLECT_DB / "영업관리부_수집"
        search_paths = [upload_temp, onedrive]
    
    if xcom_key is None:
        xcom_key = f"{output_prefix}_path"
    
    if date_extractor is None:
        date_extractor = extract_date_from_filename
    
    def auto_loader(**context):
        """자동 생성된 로더 함수"""
        ti = context['task_instance']
        
        all_files = []
        
        # 파일 탐색
        for search_path in search_paths:
            if not Path(search_path).exists():
                continue
            
            if file_type == 'auto' or file_type == 'csv':
                csv_pattern = file_pattern if file_pattern.endswith('.csv') else file_pattern.replace('*', '*.csv')
                all_files.extend(list(Path(search_path).glob(csv_pattern)))
            
            if file_type == 'auto' or file_type == 'excel':
                excel_pattern = file_pattern if any(file_pattern.endswith(ext) for ext in ['.xlsx', '.xls']) else file_pattern.replace('*', '*.xlsx')
                all_files.extend(list(Path(search_path).glob(excel_pattern)))
        
        if not all_files:
            print(f"[❌] 파일 없음: {file_pattern}")
            ti.xcom_push(key=xcom_key, value=None)
            return "0건 (파일 없음)"
        
        print(f"[✅] 총 {len(all_files)}개 파일 발견")
        
        # 중복 제거
        unique_files = get_unique_files(all_files)
        print(f"[중복 제거] {len(unique_files)}개 파일 사용")
        
        # CSV vs 엑셀 구분
        csv_files = [f for f in unique_files if f.suffix.lower() == '.csv']
        excel_files = [f for f in unique_files if f.suffix.lower() in ['.xlsx', '.xls']]
        
        if csv_files:
            result_df = load_and_concat_csv(csv_files)
        elif excel_files:
            result_df = load_and_concat_excel(excel_files, header=excel_header, date_extractor=date_extractor)
        else:
            ti.xcom_push(key=xcom_key, value=None)
            return "0건 (지원하지 않는 파일 형식)"
        
        # Parquet 저장
        output_path = save_to_parquet(result_df, context, output_prefix)
        ti.xcom_push(key=xcom_key, value=str(output_path))
        
        return f"✅ {len(result_df):,}건 로드"
    
    return auto_loader


def create_simple_preprocessor(
    select_cols=None,
    dropna_cols=None,
    groupby_cols=None,
    agg_config=None,
    rename_map=None,
    replace_map=None,
    add_prefix=None,
    surrogate_key_cols=None,
    output_prefix='data_processed'
):
    """
    간단한 전처리 함수 생성기
    
    🔧 기능:
      - 컬럼 선택
      - 결측치 제거
      - 그룹화 & 집계
      - 컬럼명 변경
      - 값 치환
      - Surrogate Key 생성
    
    Args:
        select_cols: 선택할 컬럼 리스트
        dropna_cols: null 제거할 컬럼 리스트
        groupby_cols: 그룹화 컬럼
        agg_config: 집계 설정 (dict)
        rename_map: 컬럼명 변경 (dict)
        replace_map: {컬럼명: {old: new}} 형식
        add_prefix: 값 앞에 추가할 prefix (dict: {컬럼명: prefix})
        surrogate_key_cols: Surrogate Key 생성 컬럼
        output_prefix: 출력 파일명 prefix
    
    Returns:
        Airflow Task에서 사용할 수 있는 함수
    
    💡 사용 예시:
        preprocessor = create_simple_preprocessor(
            select_cols=['date', 'store', 'sales', 'orders'],
            dropna_cols=['store'],
            groupby_cols=['date', 'store'],
            agg_config={
                'total_sales': ('sales', 'sum'),
                'total_orders': ('orders', 'sum')
            },
            rename_map={'store': 'store_name'},
            replace_map={
                'store_name': {
                    '일산백석점': '백석점',
                    '구로디지털단지점': '구로디지털점'
                }
            },
            add_prefix={'store_name': '도리당 '},
            surrogate_key_cols=['date', 'store_name'],
            output_prefix='sales_processed'
        )
        
        task = PythonOperator(
            task_id='preprocess',
            python_callable=preprocessor,
            op_kwargs={
                'input_task_id': 'load_data',
                'input_xcom_key': 'raw_data_path',
                'output_xcom_key': 'processed_data_path'
            }
        )
    """
    def preprocessor(input_task_id, input_xcom_key, output_xcom_key, **context):
        """자동 생성된 전처리 함수"""
        ti = context['task_instance']
        
        # 데이터 로드
        parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)
        
        if not parquet_path:
            print(f"[경고] 입력 데이터 없음")
            ti.xcom_push(key=output_xcom_key, value=None)
            return "0건 (입력 없음)"
        
        df = pd.read_parquet(parquet_path)
        print(f"전처리 시작: {len(df):,}행")
        
        # 1. 컬럼 선택
        if select_cols:
            df = df[select_cols]
        
        # 2. 결측치 제거
        if dropna_cols:
            for col in dropna_cols:
                if col in df.columns:
                    df = df[~df[col].isnull()]
        
        # 3. 그룹화 & 집계
        if groupby_cols and agg_config:
            df = df.groupby(groupby_cols).agg(**agg_config).reset_index()
        
        # 4. 컬럼명 변경
        if rename_map:
            df.rename(columns=rename_map, inplace=True)
        
        # 5. Surrogate Key 생성
        if surrogate_key_cols:
            df = add_surrogate_key(df, natural_key_cols=surrogate_key_cols)
            df.rename(columns={'key': 'id'}, inplace=True)
        
        # 6. Prefix 추가
        if add_prefix:
            for col, prefix in add_prefix.items():
                if col in df.columns:
                    df[col] = prefix + df[col].astype(str)
        
        # 7. 값 치환
        if replace_map:
            for col, mapping in replace_map.items():
                if col in df.columns:
                    df[col] = df[col].replace(mapping)
        
        print(f"전처리 완료: {len(df):,}행")
        
        # Parquet 저장
        output_path = save_to_parquet(df, context, output_prefix)
        ti.xcom_push(key=output_xcom_key, value=str(output_path))
        
        return f"✅ {len(df):,}행 전처리"
    
    return preprocessor


def create_pipeline_tasks(
    dag,
    pipeline_name,
    loader_config,
    preprocess_config=None,
    save_config=None
):
    """
    전체 파이프라인 Task 세트를 한 번에 생성
    
    🚀 기능:
      - Load, Preprocess, Save Task 자동 생성
      - Task 간 의존성 자동 설정
      - XCom 자동 연결
    
    Args:
        dag: Airflow DAG 객체
        pipeline_name: 파이프라인 이름 (task_id prefix로 사용)
        loader_config: create_auto_loader에 전달할 설정
        preprocess_config: create_simple_preprocessor에 전달할 설정 (선택)
        save_config: fin_save_to_csv에 전달할 설정 (선택)
    
    Returns:
        dict: {'load': task, 'preprocess': task, 'save': task}
    
    💡 사용 예시:
        with DAG(...) as dag:
            tasks = create_pipeline_tasks(
                dag=dag,
                pipeline_name='sales',
                loader_config={
                    'file_pattern': 'sales_*.csv',
                    'output_prefix': 'sales_raw'
                },
                preprocess_config={
                    'select_cols': ['date', 'store', 'amount'],
                    'groupby_cols': ['date', 'store'],
                    'agg_config': {'total': ('amount', 'sum')},
                    'output_prefix': 'sales_processed'
                },
                save_config={
                    'output_filename': 'sales_final.csv'
                }
            )
            
            # 자동으로 load >> preprocess >> save 연결됨
    """
    from airflow.operators.python import PythonOperator
    
    tasks = {}
    
    # Task 1: Load
    loader = create_auto_loader(**loader_config)
    task_load = PythonOperator(
        task_id=f"{pipeline_name}_load",
        python_callable=loader,
        dag=dag
    )
    tasks['load'] = task_load
    
    # Task 2: Preprocess (선택)
    if preprocess_config:
        preprocessor = create_simple_preprocessor(**preprocess_config)
        
        load_xcom_key = loader_config.get('xcom_key', f"{loader_config['output_prefix']}_path")
        preprocess_xcom_key = f"{preprocess_config['output_prefix']}_path"
        
        task_preprocess = PythonOperator(
            task_id=f"{pipeline_name}_preprocess",
            python_callable=preprocessor,
            op_kwargs={
                'input_task_id': f"{pipeline_name}_load",
                'input_xcom_key': load_xcom_key,
                'output_xcom_key': preprocess_xcom_key
            },
            dag=dag
        )
        tasks['preprocess'] = task_preprocess
        task_load >> task_preprocess
    
    # Task 3: Save (선택)
    if save_config:
        if preprocess_config:
            input_task_id = f"{pipeline_name}_preprocess"
            input_xcom_key = f"{preprocess_config['output_prefix']}_path"
        else:
            input_task_id = f"{pipeline_name}_load"
            input_xcom_key = loader_config.get('xcom_key', f"{loader_config['output_prefix']}_path")
        
        task_save = PythonOperator(
            task_id=f"{pipeline_name}_save",
            python_callable=fin_save_to_csv,
            op_kwargs={
                'input_task_id': input_task_id,
                'input_xcom_key': input_xcom_key,
                **save_config
            },
            dag=dag
        )
        tasks['save'] = task_save
        
        if preprocess_config:
            task_preprocess >> task_save
        else:
            task_load >> task_save
    
    return tasks