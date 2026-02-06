"""
io3.py - 범용 ETL 함수 모음
모든 파이프라인에서 사용하는 공통 로직
"""

import pandas as pd
from pathlib import Path
import hashlib

from modules.transform.utility.paths import TEMP_DIR, LOCAL_DB

# from modules.transform.utility.io3 import SMD_ORDERS_TIME
SMD_ORDERS_TIME = "27 16 * * 1,2,3" # 매주 월,수 10:27 실행
SMD_VISIT_LOG = "36 16 * * 2,3" #   매주 화,수 10:28 실행

# ============================================================
# 1. 수집 (Load)
# ============================================================

# io3.py

def load_files(
    patterns: list[str],
    search_paths: list[Path],
    xcom_key: str,
    file_type: str = 'auto',  # 'csv', 'excel', 'parquet', 'auto'
    use_glob: bool = False,  # ✨ True면 확장자 자동 추가
    header: int = 0,
    dedup_key: list[str] = None,
    add_source_filename: bool = True,
    **context
) -> str:
    """
    범용 파일 로더
    
    Args:
        patterns: 파일 패턴
        use_glob: True면 확장자 자동 추가 ['flow_visit_*' → 'flow_visit_*.csv', 'flow_visit_*.parquet' 등]
    """
    ti = context['task_instance']
    
    # 1. 파일 검색
    all_files = []
    
    for search_path in search_paths:
        if not search_path.exists():
            continue
            
        for pattern in patterns:
            if use_glob:
                # ✨ glob 모드: 확장자 자동 추가
                for ext in ['.parquet', '.pq', '.csv', '.xlsx', '.xls']:
                    all_files.extend(list(search_path.glob(f"{pattern}{ext}")))
            else:
                # 일반 모드: 패턴 그대로 사용
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
        suffix = first_file.suffix.lower()
        
        if suffix == '.csv':
            file_type = 'csv'
        elif suffix in ['.xlsx', '.xls']:
            file_type = 'excel'
        elif suffix in ['.parquet', '.pq']:
            file_type = 'parquet'
        else:
            raise ValueError(f"지원하지 않는 파일 타입: {suffix}")
    
    # 4. 로드
    if file_type == 'csv':
        df = _load_csv_files(unique_files, add_source_filename=add_source_filename)
    elif file_type == 'excel':
        df = _load_excel_files(unique_files, header=header, add_source_filename=add_source_filename)
    elif file_type == 'parquet':
        df = _load_parquet_files(unique_files, add_source_filename=add_source_filename)
    
    # 5. 행 단위 중복 제거
    if dedup_key:
        valid_cols = [c for c in dedup_key if c in df.columns]
        if valid_cols:
            before = len(df)
            df = df.drop_duplicates(subset=valid_cols, keep='first')
            after = len(df)
            removed = before - after
            if removed > 0:
                print(f"\n[중복 제거] {valid_cols} 기준: {removed:,}건 제거")
                print(f"            전: {before:,}건 → 후: {after:,}건")
    
    # 6. Parquet 저장
    output_path = _save_parquet(df, context, prefix=xcom_key.replace('_path', '_raw'))
    ti.xcom_push(key=xcom_key, value=str(output_path))
    
    return f"✅ {len(df):,}건 로드"


# ============================================================
# Parquet 로더 추가
# ============================================================
def _load_parquet_files(file_paths: list[Path], add_source_filename: bool = True) -> pd.DataFrame:
    """
    여러 Parquet 파일을 로드하여 병합
    
    Args:
        file_paths: 파일 경로 리스트
        add_source_filename: 소스 파일명 컬럼 추가 여부
    
    Returns:
        병합된 DataFrame
    """
    import pandas as pd
    
    dataframes = []
    
    for file_path in file_paths:
        try:
            df = pd.read_parquet(file_path, engine='pyarrow')
            
            # 파일명 컬럼 추가
            if add_source_filename:
                df['_source_file'] = file_path.name
                print(f"  📄 {file_path.name}: {len(df):,}행 (_source_file 추가됨)")
            else:
                print(f"  📄 {file_path.name}: {len(df):,}행")
            
            dataframes.append(df)
            
        except Exception as e:
            print(f"  ❌ {file_path.name} 로드 실패: {e}")
    
    if not dataframes:
        raise ValueError("로드된 파일이 없습니다")
    
    combined_df = pd.concat(dataframes, ignore_index=True)
    print(f"\n[병합] 총 {len(combined_df):,}행")
    
    # 디버깅: _source_file 컬럼 확인
    if add_source_filename and '_source_file' in combined_df.columns:
        print(f"[확인] ✅ _source_file 컬럼 존재: {combined_df['_source_file'].nunique()}개 파일")
    
    return combined_df


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


def _load_csv_files(file_paths: list[Path], add_source_filename: bool = True) -> pd.DataFrame:
    """여러 CSV 파일을 로드하여 병합"""
    import pandas as pd
    
    dataframes = []
    
    for file_path in file_paths:
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            
            # ⭐⭐⭐ 파일명 컬럼 추가 (이 부분 확인!)
            if add_source_filename:
                df['_source_file'] = file_path.name
                print(f"  📄 {file_path.name}: {len(df):,}행 (_source_file 추가됨)")
            else:
                print(f"  📄 {file_path.name}: {len(df):,}행")
            
            dataframes.append(df)
            
        except Exception as e:
            print(f"  ❌ {file_path.name} 로드 실패: {e}")
    
    if not dataframes:
        raise ValueError("로드된 파일이 없습니다")
    
    combined_df = pd.concat(dataframes, ignore_index=True)
    print(f"\n[병합] 총 {len(combined_df):,}행")
    
    # ⭐ 디버깅: _source_file 컬럼 확인
    if add_source_filename and '_source_file' in combined_df.columns:
        print(f"[확인] ✅ _source_file 컬럼 존재: {combined_df['_source_file'].nunique()}개 파일")
    
    return combined_df


# modules/transform/utility/io3.py

# ============================================================
# Excel 로더도 add_source_filename 지원하도록 수정
# ============================================================
def _load_excel_files(files, header=0, add_source_filename=True):
    """
    Excel 파일들을 로드하여 병합
    
    Args:
        files: 파일 경로 리스트
        header: 헤더 행 번호 (기본값: 0)
        add_source_filename: 소스 파일명 컬럼 추가 여부 (기본값: True)
    """
    dfs = []
    for file in files:
        try:
            df = pd.read_excel(file, header=header)
            
            # 소스 파일명 추가
            if add_source_filename:
                df['_source_file'] = Path(file).name
                print(f"  ✅ {Path(file).name}: {len(df):,}행 (_source_file 추가됨)")
            else:
                print(f"  ✅ {Path(file).name}: {len(df):,}행")
            
            dfs.append(df)
        except Exception as e:
            print(f"  ⚠️ {Path(file).name} 로드 실패: {e}")
    
    if not dfs:
        raise ValueError("로드된 Excel 파일이 없습니다")
    
    combined = pd.concat(dfs, ignore_index=True)
    print(f"[✅] 총 {len(combined):,}행 병합 완료")
    
    # 디버깅
    if add_source_filename and '_source_file' in combined.columns:
        print(f"[확인] ✅ _source_file 컬럼 존재: {combined['_source_file'].nunique()}개 파일")
    
    return combined



def _save_parquet(df, context, prefix):
    """
    Parquet 저장 (데이터 정리 포함)
    
    Args:
        df: 저장할 DataFrame
        context: Airflow context
        prefix: 파일명 prefix
    
    Returns:
        str: 저장된 파일 경로
    """
    import pandas as pd
    from pathlib import Path
    
    # 🔥 1단계: 데이터 정리
    df_clean = df.copy()
    
    # Unnamed 컬럼 제거 (빈 컬럼)
    unnamed_cols = [col for col in df_clean.columns if 'Unnamed' in str(col)]
    if unnamed_cols:
        df_clean = df_clean.drop(columns=unnamed_cols)
        print(f"  [정리] {len(unnamed_cols)}개 빈 컬럼 제거: {unnamed_cols}")
    
    # object 타입 컬럼을 문자열로 통일
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].fillna('').astype(str)
    
    # 🔥 2단계: 파일 저장
    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    output_path = TEMP_DIR / f"{prefix}_{context['ds_nodash']}.parquet"
    
    try:
        df_clean.to_parquet(output_path, index=False, engine='pyarrow')
        print(f"[💾] Parquet 저장 완료: {output_path}")
        print(f"     {len(df_clean):,}행 × {len(df_clean.columns)}컬럼")
    except Exception as e:
        print(f"[⚠️] Parquet 저장 실패: {e}")
        # CSV로 폴백
        csv_path = output_path.with_suffix('.csv')
        df_clean.to_csv(csv_path, index=False, encoding='utf-8-sig')
        output_path = csv_path
        print(f"[💾] CSV 대체 저장: {output_path}")
    
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


# ============================================================
# 정산금액 검증 및 알림 함수
# ============================================================

def validate_and_alert_settlement(
    df,
    platform,
    order_col,
    payment_col,
    settlement_col_candidates,
    store_col,
    manager_col=None,  # ✅ 담당자 컬럼명(선택)
    source_file_col='_source_file',
    recipients=None,
    smtp_conn_id='doridang_conn_smtp_gmail',
    **context
):
    """정산금액 검증: 결제금액은 있는데 정산금액이 없는 경우 이메일 알림(디자인 개선/주문일시 노출/담당자 지원)"""
    import pandas as pd
    import numpy as np

    print(f"\n{'='*60}")
    print(f"[{platform}] 정산금액 검증 시작")
    print(f"{'='*60}")

    # ─────────────────────────────────────────────────────────
    # 1) 정산금액 컬럼 찾기
    # ─────────────────────────────────────────────────────────
    settlement_col = None
    for col in settlement_col_candidates:
        if col in df.columns:
            settlement_col = col
            print(f"[정산컬럼] '{col}' 발견")
            break

    if not settlement_col:
        print(f"[경고] 정산금액 컬럼 없음: {settlement_col_candidates}")
        return

    # ─────────────────────────────────────────────────────────
    # 2) _source_file 컬럼 보정
    # ─────────────────────────────────────────────────────────
    if source_file_col not in df.columns:
        print(f"[경고] {source_file_col} 컬럼 없음 - 파일명 추적 불가")
        df[source_file_col] = 'UNKNOWN'

    # ─────────────────────────────────────────────────────────
    # 3) 주문일시 컬럼 자동 탐색(우선순위)
    # ─────────────────────────────────────────────────────────
    order_date_candidates = [
        'order_date', '주문일시', 'order_datetime', 'orderDate', '주문시간', 'orderTime'
    ]
    order_date_col = next((c for c in order_date_candidates if c in df.columns), None)
    if order_date_col:
        print(f"[주문일시] '{order_date_col}' 사용")
    else:
        print("[주문일시] 후보 컬럼 없음 - 상세표에서 '-'로 표기")

    # ─────────────────────────────────────────────────────────
    # 4) 문제 행 필터링
    # ─────────────────────────────────────────────────────────
    problem_df = df[
        (df[payment_col].notna()) &
        (df[payment_col] > 0) &
        (df[settlement_col].isna() | (df[settlement_col] == 0))
    ].copy()

    if len(problem_df) == 0:
        print(f"[✅] 정산금액 검증 통과: 문제 없음")
        print(f"{'='*60}\n")
        return

    # ─────────────────────────────────────────────────────────
    # 5) 파일별 집계(매장명, 담당자, 파일명, 주문건수, 총결제금액)
    # ─────────────────────────────────────────────────────────
    # 매장명/파일명 추출용 함수
    def extract_store_from_filename(file_name: str) -> str:
        if not isinstance(file_name, str):
            return ''
        if '도리당_' in file_name:
            parts = file_name.split('도리당_')
            if len(parts) > 1:
                return '도리당 ' + parts[1].split('_')[0]
        return file_name.split('_')[0] if '_' in file_name else file_name

    # 파일별 집계의 기준 키
    group_keys = [source_file_col]
    # 매장명 도출: 우선 store_col이 있으면 그룹 키에 포함
    if store_col in problem_df.columns:
        group_keys.append(store_col)

    # 담당자 컬럼 처리: 없으면 임시 생성해 '-' 채우기
    manager_tmp_col = None
    if manager_col and (manager_col in problem_df.columns):
        group_keys.append(manager_col)
    else:
        manager_tmp_col = '__manager_fallback__'
        problem_df[manager_tmp_col] = '-'
        group_keys.append(manager_tmp_col)

    # 집계
    file_summary = (
        problem_df
        .groupby(group_keys)
        .agg(
            주문건수=(order_col, 'count'),
            총결제금액=(payment_col, 'sum'),
        )
        .reset_index()
    )

    # 표시용 컬럼 만들기
    file_summary['파일명'] = file_summary[source_file_col]

    # 매장명 표시: store_col 없으면 파일명에서 추출
    if store_col in file_summary.columns:
        file_summary['매장명'] = file_summary[store_col].fillna('').map(lambda x: str(x))
    else:
        file_summary['매장명'] = file_summary['파일명'].map(extract_store_from_filename)

    # 담당자 표시
    if manager_col and (manager_col in file_summary.columns):
        file_summary['담당자'] = file_summary[manager_col].fillna('-').map(lambda x: str(x))
    else:
        file_summary['담당자'] = file_summary.get(manager_tmp_col, '-').fillna('-')

    # 최종 표시 순서
    file_summary = file_summary[['매장명', '담당자', '파일명', '주문건수', '총결제금액']].copy()

    print(f"[❌] 정산금액 누락 발견: {len(problem_df):,}건")
    print("\n[파일별 집계]")
    try:
        print(file_summary.to_string(index=False))
    except:
        # 넓은 컬럼 환경에서 줄바꿈 대비
        print(file_summary.head(50).to_string(index=False))
        if len(file_summary) > 50:
            print(f"... (총 {len(file_summary)}행 중 50행만 표시)")

    # ─────────────────────────────────────────────────────────
    # 6) HTML 이메일 본문(디자인 업그레이드)
    # ─────────────────────────────────────────────────────────
    ds_val = context.get('ds', 'N/A')
    total_payment = problem_df[payment_col].sum()

    # 상세 테이블 준비 (최대 10건)
    detail_cols = [store_col, order_col, payment_col, source_file_col]
    detail_available = [c for c in detail_cols if c in problem_df.columns]
    detail_rows = problem_df[detail_available].copy()
    if order_date_col:
        detail_rows['__order_dt_str__'] = pd.to_datetime(
            problem_df[order_date_col], errors='coerce'
        ).dt.strftime('%Y-%m-%d %H:%M')
        detail_rows['__order_dt_str__'] = detail_rows['__order_dt_str__'].fillna(
            problem_df[order_date_col].astype(str).str[:16]
        )
    else:
        detail_rows['__order_dt_str__'] = '-'

    # 매장명 간단 표시
    def short_store_name(val):
        if pd.isna(val):
            return 'N/A'
        s = str(val)
        return s.split(']')[-1].strip() if ']' in s else s

    # HTML 생성(정상 HTML 태그; MIMEText('html')로 전송)
    html_table = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>[도리당] {platform} 정산금액 누락 알림</title>
<style>
    :root {{
        --bg: #f7f9fc;
        --card: #ffffff;
        --text: #1f2937;
        --muted: #6b7280;
        --primary: #2563eb;
        --danger: #dc2626;
        --warning: #f59e0b;
        --border: #e5e7eb;
        --accent: #eef2ff;
    }}
    @media (prefers-color-scheme: dark) {{
        :root {{
            --bg: #0b1220;
            --card: #0f172a;
            --text: #e5e7eb;
            --muted: #9ca3af;
            --primary: #60a5fa;
            --danger: #f87171;
            --warning: #fbbf24;
            --border: #1f2937;
            --accent: #111827;
        }}
    }}
    body {{
        margin: 0;
        padding: 24px;
        background: var(--bg);
        color: var(--text);
        font-family: 'Pretendard', 'Noto Sans KR', 'Malgun Gothic', '맑은 고딕', system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
        line-height: 1.55;
    }}
    .container {{
        max-width: 1080px;
        margin: 0 auto;
    }}
    .header {{
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 18px;
    }}
    .badge {{
        display: inline-block;
        font-size: 12px;
        font-weight: 700;
        padding: 6px 10px;
        border-radius: 999px;
        background: var(--danger);
        color: #fff;
    }}
    .title {{
        font-size: 22px;
        font-weight: 800;
        letter-spacing: -0.3px;
    }}
    .card {{
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 18px 18px 8px;
        box-shadow: 0 1px 2px rgba(0,0,0,.04);
        margin-bottom: 16px;
    }}
    .meta {{
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 12px;
        margin-top: 8px;
        margin-bottom: 6px;
    }}
    .metric {{
        background: var(--accent);
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 12px;
    }}
    .metric .label {{
        font-size: 12px;
        color: var(--muted);
        margin-bottom: 6px;
    }}
    .metric .value {{
        font-size: 18px;
        font-weight: 800;
        color: var(--text);
    }}
    h3.section {{
        margin: 16px 0 8px;
        font-size: 16px;
        font-weight: 800;
        border-left: 4px solid var(--primary);
        padding-left: 8px;
    }}
    table {{
        width: 100%;
        border-collapse: collapse;
        border-spacing: 0;
        background: var(--card);
        overflow: hidden;
        border-radius: 12px;
        margin: 10px 0 18px;
        border: 1px solid var(--border);
    }}
    thead th {{
        text-align: left;
        font-size: 12px;
        font-weight: 800;
        color: #fff;
        background: linear-gradient(180deg, #ef4444, #dc2626);
        padding: 10px 10px;
        border-bottom: 1px solid var(--border);
        position: sticky;
        top: 0;
        z-index: 1;
        letter-spacing: 0.2px;
    }}
    tbody td {{
        font-size: 13px;
        padding: 10px 10px;
        border-bottom: 1px solid var(--border);
        background: var(--card);
        color: var(--text);
        vertical-align: top;
    }}
    tbody tr:hover td {{
        background: rgba(37, 99, 235, 0.07);
    }}
    .num {{
        text-align: right;
        font-variant-numeric: tabular-nums;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        white-space: nowrap;
    }}
    .file-name {{
        color: var(--muted);
        font-size: 12px;
        word-break: break-all;
    }}
    .store-name {{
        font-weight: 700;
        color: var(--primary);
    }}
    .footer {{
        margin-top: 10px;
        padding-top: 14px;
        border-top: 1px dashed var(--border);
        color: var(--muted);
        font-size: 12px;
    }}
    @media (max-width: 720px) {{
        .meta {{ grid-template-columns: 1fr; }}
        thead {{ display: none; }}
        table, tbody, tr, td {{ display: block; width: 100%; }}
        tbody tr {{ margin-bottom: 10px; border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }}
        tbody td {{ border: none; border-bottom: 1px solid var(--border); }}
        tbody td:last-child {{ border-bottom: none; }}
        tbody td::before {{
            content: attr(data-label);
            display: block;
            font-size: 11px;
            color: var(--muted);
            margin-bottom: 4px;
        }}
    }}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <span class="badge">정산 누락</span>
        <div class="title">[{platform}] 정산금액 누락 알림</div>
    </div>

    <div class="card">
        <div class="meta">
            <div class="metric">
                <div class="label">수집일시</div>
                <div class="value">{ds_val}</div>
            </div>
            <div class="metric">
                <div class="label">문제 건수</div>
                <div class="value">{len(problem_df):,}건</div>
            </div>
            <div class="metric">
                <div class="label">총 결제금액</div>
                <div class="value">{total_payment:,.0f}원</div>
            </div>
        </div>
    </div>

    <h3 class="section">📋 파일별 집계</h3>
    <table role="table" aria-label="파일별 집계">
        <thead>
            <tr>
                <th scope="col" style="width: 160px;">매장명</th>
                <th scope="col" style="width: 140px;">담당자</th>
                <th scope="col">파일명</th>
                <th scope="col" style="width: 110px;" class="num">주문건수</th>
                <th scope="col" style="width: 140px;" class="num">총결제금액</th>
            </tr>
        </thead>
        <tbody>
"""

    # 파일별 집계 행 렌더링
    for _, row in file_summary.iterrows():
        html_table += f"""
            <tr>
                <td class="store-name" data-label="매장명">{row['매장명']}</td>
                <td data-label="담당자">{row['담당자']}</td>
                <td class="file-name" data-label="파일명">{row['파일명']}</td>
                <td class="num" data-label="주문건수">{row['주문건수']:,}건</td>
                <td class="num" data-label="총결제금액">{row['총결제금액']:,.0f}원</td>
            </tr>
"""

    html_table += """
        </tbody>
    </table>

    <h3 class="section">🔍 상세 내역 (최대 10건)</h3>
    <table role="table" aria-label="상세 내역">
        <thead>
            <tr>
                <th scope="col" style="width: 160px;">매장명</th>
                <th scope="col" style="width: 150px;">주문일시</th>
                <th scope="col" style="width: 160px;">주문번호</th>
                <th scope="col" style="width: 120px;" class="num">결제금액</th>
                <th scope="col">파일명</th>
            </tr>
        </thead>
        <tbody>
"""

    # 상세 10건
    for _, r in detail_rows.head(10).iterrows():
        store_val = short_store_name(r.get(store_col, 'N/A')) if store_col in detail_rows.columns else 'N/A'
        order_dt = r.get('__order_dt_str__', '-') or '-'
        order_no = r.get(order_col, 'N/A')
        pay_val = r.get(payment_col, 0)
        file_name = r.get(source_file_col, 'N/A')
        html_table += f"""
            <tr>
                <td class="store-name" data-label="매장명">{store_val}</td>
                <td data-label="주문일시">{order_dt}</td>
                <td style="font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px;" data-label="주문번호">{order_no}</td>
                <td class="num" data-label="결제금액">{pay_val:,.0f}원</td>
                <td class="file-name" data-label="파일명">{file_name}</td>
            </tr>
"""

    if len(problem_df) > 10:
        html_table += f"""
            <tr>
                <td colspan="5" class="num" style="text-align:center; font-weight:700; color: var(--primary); background: var(--accent);">
                    ... 외 {len(problem_df) - 10:,}건
                </td>
            </tr>
"""

    html_table += f"""
        </tbody>
    </table>

    <div class="card" style="padding-top:12px;">
        <h3 class="section" style="margin-top:0;">📌 조치 방법</h3>
        <ol style="margin: 4px 0 0 18px;">
            <li>위 <b>파일명</b>과 <b>주문일시</b>를 확인하여 원본 수집 파일을 찾습니다.</li>
            <li>해당 <b>주문번호</b>의 정산금액 누락 사유를 확인합니다.</li>
            <li>필요 시 플랫폼에서 데이터를 <b>재다운로드/재수집</b>합니다.</li>
        </ol>
        <div class="footer">
            본 메일은 데이터 파이프라인에서 자동 발송되었습니다.<br/>
            문의: 데이터팀 (a17019@kakao.com)
        </div>
    </div>
</div>
</body>
</html>
"""

    # ─────────────────────────────────────────────────────────
    # 7) 이메일 발송
    # ─────────────────────────────────────────────────────────
    if recipients and len(recipients) > 0:
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            from airflow.hooks.base import BaseHook

            conn = BaseHook.get_connection(smtp_conn_id)

            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[도리당] {platform} 정산금액 누락 알림 ({len(problem_df):,}건)"
            msg['From'] = conn.login
            msg['To'] = ', '.join(recipients)

            html_part = MIMEText(html_table, 'html', 'utf-8')
            msg.attach(html_part)

            server = smtplib.SMTP(conn.host, conn.port, timeout=30)
            server.starttls()
            server.login(conn.login, conn.password)
            server.send_message(msg)
            server.quit()

            print(f"\n[📧] 알림 이메일 발송 완료: {recipients}")

        except Exception as e:
            print(f"\n[⚠️] 이메일 발송 실패: {e}")
            import traceback
            print(f"[상세 에러]")
            print(traceback.format_exc())
    else:
        print(f"\n[⚠️] 수신자 없음 - 이메일 발송 스킵")

    print(f"{'='*60}\n")

    
# ============================================================
# 수집된 CSV 파일 정리 및 업로드 함수
# ============================================================

def sales_cleanup_collected_csvs(patterns, dest_dir, **context):
    """
    OneDrive 수집 폴더의 CSV 파일들을 지정된 경로로 이동
    
    Args:
        patterns: 이동할 파일 패턴 리스트 (예: ['baemin_*.csv'])
        dest_dir: 목적지 디렉토리 (컨테이너 경로)
    """
    import glob as glob_module
    import shutil
    from pathlib import Path
    from modules.transform.utility.paths import COLLECT_DB
    
    collect_dir = COLLECT_DB / "영업관리부_수집"
    dest_path = Path(dest_dir)
    
    # 목적지 디렉토리 생성
    dest_path.mkdir(parents=True, exist_ok=True)
    
    moved_count = 0
    for pattern in patterns:
        file_pattern = str(collect_dir / pattern)
        files = glob_module.glob(file_pattern)
        
        print(f"\n[이동] 패턴: {pattern}")
        print(f"   찾은 파일: {len(files)}개")
        
        for file_path in files:
            try:
                source = Path(file_path)
                destination = dest_path / source.name
                
                shutil.move(str(source), str(destination))
                print(f"   ✅ 이동: {source.name} → {destination}")
                moved_count += 1
            except Exception as e:
                print(f"   ⚠️  이동 실패: {Path(file_path).name} - {e}")
    
    print(f"\n✅ 총 {moved_count}개 파일 이동 완료")
    return f"이동 완료: {moved_count}개"



def save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename=None, # 'sales_daily_orders_upload_fin.csv',
    output_subdir=None, #'영업관리부_DB' 
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

# 기존 것의 중복을 체크하고 저장하는 함수
def append_save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename=None,  # 'sales_daily_orders_upload_fin.csv',
    output_subdir=None,    # '영업관리부_DB' 
    dedup_key=None,        # 중복 체크 기준 컬럼(들)
    **context
):
    """Parquet 데이터를 기존 CSV에 추가 저장 (중복 제거)"""
    import os
    import shutil
    import tempfile
    from pathlib import Path
    import pandas as pd
    from modules.transform.utility.paths import LOCAL_DB
    
    ti = context['task_instance']
    
    # 1. 새 데이터 로드
    parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)
    
    if not parquet_path:
        print(f"[경고] 저장할 데이터 없음")
        return "⚠️ 저장 스킵: 데이터 없음"
    
    if not os.path.exists(parquet_path):
        print(f"[경고] 파일 경로 없음: {parquet_path}")
        return "⚠️ 저장 스킵: 파일 없음"
    
    new_df = pd.read_parquet(parquet_path)
    
    print(f"\n{'='*60}")
    print(f"[입력] 새 데이터: {len(new_df):,}행 × {len(new_df.columns)}컬럼")
    
    # 2. 출력 경로 설정
    if output_csv_path:
        local_csv_path = Path(output_csv_path)
    else:
        output_dir = LOCAL_DB / output_subdir
        output_dir.mkdir(parents=True, exist_ok=True)
        local_csv_path = output_dir / output_filename
    
    local_csv_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[경로] 저장 위치: {local_csv_path}")
    
    # 3. 기존 CSV 파일 로드 (있으면)
    if local_csv_path.exists():
        try:
            existing_df = pd.read_csv(local_csv_path, encoding='utf-8-sig')
            print(f"[기존] 데이터: {len(existing_df):,}행 로드")
            
            # 기존 데이터와 새 데이터 합치기
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            print(f"[합침] 전체 데이터: {len(combined_df):,}행")
            
        except Exception as e:
            print(f"[경고] 기존 파일 로드 실패: {e}")
            print(f"[처리] 새 데이터만 저장합니다")
            combined_df = new_df
    else:
        print(f"[신규] 기존 파일 없음 → 새 파일 생성")
        combined_df = new_df
    
    # 4. 중복 제거
    if dedup_key:
        dedup_cols = [dedup_key] if isinstance(dedup_key, str) else dedup_key
        valid_cols = [c for c in dedup_cols if c in combined_df.columns]
        
        if valid_cols:
            before = len(combined_df)
            combined_df.drop_duplicates(subset=valid_cols, keep='last', inplace=True)
            after = len(combined_df)
            
            removed = before - after
            if removed > 0:
                print(f"\n[중복 제거] {valid_cols} 기준: {removed:,}건 제거")
                print(f"             (최신 데이터 우선 keep='last')")
        else:
            print(f"[경고] 중복 제거 컬럼 없음: {dedup_cols}")
    
    # 5. datetime 변환
    datetime_cols = combined_df.select_dtypes(include=['datetime64']).columns.tolist()
    if datetime_cols:
        for col in datetime_cols:
            combined_df[col] = combined_df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
    
    # 6. 임시 파일로 저장 후 원자적 이동
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
        
        combined_df.to_csv(tmp_path, index=False, encoding='utf-8-sig')
        
        # 백업 후 교체
        if local_csv_path.exists():
            backup_path = local_csv_path.parent / f"{local_csv_path.name}.bak"
            shutil.copy2(local_csv_path, backup_path)
            shutil.move(tmp_path, str(local_csv_path))
            backup_path.unlink()
        else:
            shutil.move(tmp_path, str(local_csv_path))
        
        csv_size = local_csv_path.stat().st_size / (1024 * 1024)
        
        print(f"\n[저장] ✅ CSV 저장 완료")
        print(f"       최종 데이터: {len(combined_df):,}건")
        print(f"       파일 크기: {csv_size:.2f} MB")
        
    except Exception as e:
        print(f"[에러] CSV 저장 실패: {e}")
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        return f"❌ 저장 실패: {e}"
    
    print(f"{'='*60}\n")
    
    # 결과 요약
    new_added = len(new_df) if not local_csv_path.exists() else len(combined_df) - (len(combined_df) - len(new_df) + (before - after if dedup_key else 0))
    return f"✅ 저장 완료: 전체 {len(combined_df):,}건 (신규 추가: {len(new_df):,}건)"


