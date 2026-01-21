"""
전처리 파이프라인
"""

from pathlib import Path
from modules.transform.utility.paths import COLLECT_DB, LOCAL_DB, TEMP_DIR
from modules.transform.utility.io3 import load_files, preprocess_df, save_to_csv, join_dataframes
import pandas as pd
import datetime as dt


# ============================================================
# 전처리 로직 (데이터별로 커스터마이징)
# ============================================================
def transform_df(df):
    # 전처리 시작
    
    return df


# ============================================================
# Wrapper 함수들 (파라미터만 지정)
# ============================================================
def transform_toorder_review(**context):
    """1. 토더 리뷰 수집"""
    return load_files(
        patterns=['toorder_review_*.csv', 'toorder_review_*.xlsx'], # 토더 리뷰 파일 패턴
        search_paths=[
            Path('/opt/airflow/download/업로드_temp'), # 업로드 임시 폴더
            Path('/opt/airflow/download'), # 일반 다운로드 폴더
            COLLECT_DB / "영업관리부_수집" # 수집 DB 폴더
        ],
        xcom_key='toorder_review_path', # XCom 키
        file_type='auto',  # CSV 우선, 없으면 Excel
        header=3,  # 엑셀용
        **context
    )



def transform_df(df):
    # 전처리 시작
    
    return df

def preprocess_df(**context):
    """ 처리"""
    return preprocess_df(
        input_xcom_key='toorder_review_path',
        output_xcom_key='toorder_review_processed_path',
        transform_func=transform_df,  # 위에서 정의한 함수
        #natural_keys=['date', 'stores_name'],  # ID 생성용
        **context
    )


def save_toorder_review_csv(**context):
    """3. 토더 리뷰 저장"""
    return save_to_csv(
        input_xcom_key='toorder_review_processed_path',
        output_filename='toorder_review_doridang.csv',
        output_subdir='영업관리부_DB',
        dedup_key=['id'],
        **context
    )

# 조인용
def join_data_tasks(
    left_task,
    right_task,
    on=None,
    left_on=None,
    right_on=None,
    how='left',
    drop_right_keys=True,  # 조인 후 오른쪽 키 컬럼을 자동으로 삭제할지 여부
    output_xcom_key='joined_data_path',
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



# ============================================================
# CSV 저장
# ============================================================
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