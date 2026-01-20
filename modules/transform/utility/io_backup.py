import glob
import os
import pandas as pd
from pathlib import Path
from typing import List, Union, Optional
from modules.transform.utility.paths import ONEDRIVE_DB, COLLECT_DB, LOCAL_DB

# ============================================================
# CSV 로드 함수들
# ============================================================
def read_csv_glob(
    files: Union[str, List[str]],
    add_source_col: bool = False,
    source_col_name: str = "source_file",
    ignore_index: bool = True,
    on_error: str = "raise",  # 'raise' | 'skip'
    **read_csv_kwargs,
) -> pd.DataFrame:
    """
    Glob 패턴 또는 파일 경로 리스트로 여러 CSV를 읽어 하나의 DataFrame으로 반환합니다.

    Parameters
    ----------
    files : str | list[str]
        - str: glob 패턴 (예: 'data/baemin/*.csv')
        - list[str]: CSV 파일 경로 리스트
    add_source_col : bool, default False
        각 행의 원본 파일명을 새 컬럼으로 추가할지 여부.
    source_col_name : str, default 'source_file'
        원본 파일명 컬럼명.
    ignore_index : bool, default True
        concat 시 인덱스를 무시하고 0..n-1로 재지정.
    on_error : {'raise', 'skip'}, default 'raise'
        파일을 읽는 중 오류 발생 시:
        - 'raise': 즉시 예외 발생
        - 'skip': 문제 파일을 건너뛰고 계속 진행(경고 출력)
    **read_csv_kwargs :
        `pd.read_csv`에 그대로 전달될 인자들.

    Returns
    -------
    pd.DataFrame
        모든 CSV를 세로 방향으로 결합한 데이터프레임.
    """
    if isinstance(files, str):
        file_list = sorted(glob.glob(files))
    elif isinstance(files, list):
        file_list = files
    else:
        raise TypeError("files는 glob 패턴(str) 또는 파일 경로 리스트(list[str])여야 합니다.")

    if not file_list:
        raise FileNotFoundError("매칭되는 CSV 파일이 없습니다.")

    dfs: List[pd.DataFrame] = []
    for fpath in file_list:
        try:
            df = pd.read_csv(fpath, **read_csv_kwargs)
            if add_source_col:
                df[source_col_name] = os.path.basename(fpath)
            dfs.append(df)
        except Exception as e:
            if on_error == "skip":
                print(f"[경고] 파일 '{fpath}'을(를) 건너뜁니다. 이유: {e}")
                continue
            raise

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=ignore_index)



def load_data(
    file_path,
    xcom_key='parquet_path',
    use_glob=True,
    dedup_key=None,  # 중복 제거 키 추가
    **context
):
    """CSV 데이터 범용 로드 함수
    
    Args:
        file_path: 파일 경로 또는 glob 패턴
        xcom_key: XCom에 저장할 키 이름
        use_glob: True면 glob 패턴으로 처리, False면 단일 파일
        dedup_key: 중복 제거 기준 컬럼 (str 또는 list, None이면 중복 제거 안함)
                  예: 'order_id' 또는 ['order_id', 'store_id']
        
    Returns:
        str: 처리 결과 메시지
        
    XCom:
        {xcom_key}: parquet 파일 경로
    """
    import glob as glob_module
    from pathlib import Path
    ti = context['task_instance']
    
    temp_dir = ONEDRIVE_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    parquet_path = temp_dir / f"{xcom_key}_{context['ds_nodash']}.parquet"
    
    try:
        # 1. 파일 목록 가져오기
        if use_glob:
            file_list = sorted(glob_module.glob(str(file_path)))
            print(f"[DEBUG] glob 패턴: {file_path}")
            print(f"[DEBUG] 찾은 파일: {len(file_list)}개")
        else:
            file_list = [str(file_path)] if Path(file_path).exists() else []
            print(f"[DEBUG] 단일 파일: {file_path}")
        
        if not file_list:
            print(f"[경고] 파일 없음: {file_path}")
            ti.xcom_push(key=xcom_key, value=None)
            return f"0건 (파일 없음)"
        
        # 파일명에 (1), (2) 등이 있어도 모두 다른 수집 데이터이므로 모두 로드
        print(f"[INFO] 로드할 파일: {len(file_list)}개")
        for fpath in file_list:
            print(f"   ✓ {Path(fpath).name}")
        
        # 2. 인코딩 자동 감지하며 CSV 읽기
        dfs = []
        encodings = ['utf-8', 'cp949', 'euc-kr', 'latin1']
        
        for fpath in file_list:
            df = None
            for encoding in encodings:
                try:
                    print(f"   시도: {Path(fpath).name} ({encoding})")
                    df = pd.read_csv(fpath, encoding=encoding)
                    print(f"   ✓ {encoding}으로 읽음: {len(df)}행")
                    dfs.append(df)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            
            if df is None:
                print(f"   ✗ 읽기 실패: {fpath} (모든 인코딩 실패)")
                continue
        
        if not dfs:
            print(f"[경고] 읽을 수 있는 파일 없음")
            ti.xcom_push(key=xcom_key, value=None)
            return f"0건 (파일 읽기 실패)"
        
        # 3. 모든 파일 병합
        result_df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
        print(f"\n병합 완료: {len(result_df):,}행")
        
        # 4. 개별 행 중복 제거 (선택적)
        # 동일 파일 재수집 방지를 위한 용도이므로, 파일이 2개 이상일 때만 적용
        if dedup_key and len(file_list) > 1:
            before = len(result_df)

            if isinstance(dedup_key, str):
                dedup_cols = [dedup_key]
            else:
                dedup_cols = list(dedup_key)

            valid_cols = [col for col in dedup_cols if col in result_df.columns]

            if valid_cols:
                result_df.drop_duplicates(subset=valid_cols, keep='first', inplace=True)
                after = len(result_df)
                removed = before - after
                if removed > 0:
                    print(f"중복 제거: {removed:,}건 제거 (기준: {valid_cols})")
            else:
                print(f"[경고] 중복 제거 컬럼 없음: {dedup_cols}")
        elif dedup_key:
            print("[정보] 단일 파일이므로 행 중복 제거 생략")
        
        # 5. Parquet로 저장 (데이터 타입 보존)
        # 혼합 타입 컬럼 안전 처리 (캠페인ID 등)
        for col in result_df.columns:
            if result_df[col].dtype == 'object':
                # 숫자처럼 보이는 object 컬럼은 문자열로 명시적 변환
                if col in ['캠페인ID', 'ad_id', 'store_id']:
                    result_df[col] = result_df[col].astype(str)
        
        result_df.to_parquet(parquet_path, index=False, engine='pyarrow')
        print(f"\n✅ 데이터 로드 완료: {len(result_df):,}건")
        print(f"   저장 경로: {parquet_path}")
        print(f"   컬럼: {list(result_df.columns)}")
        
        # 6. XCom에 경로 저장
        ti.xcom_push(key=xcom_key, value=str(parquet_path))
        return f"{len(result_df):,}건"
        
    except Exception as e:
        print(f"[에러] 데이터 로드 실패: {str(e)}")
        import traceback
        print(traceback.format_exc())
        ti.xcom_push(key=xcom_key, value=None)
        return f"0건 (에러)"
    
    


# ============================================================
# 전처리 양식
# ============================================================
def load_baemin_data(**context):
    """배민 주문 데이터 로드 (wrapper)"""
    baemin_dir = COLLECT_DB / '영업팀'
    file_pattern = f"{baemin_dir}/baemin_orders*.csv"
    
    return load_data(
        file_path=file_pattern,
        xcom_key='baemin_parquet_path',
        use_glob=True,  # glob 패턴 사용
        **context
    )



# ============================================================
# 전처리 양식
# ============================================================

def preprocess_df(
    input_task_id,      # 파라미터로 받기
    input_xcom_key,     # 파라미터로 받기
    output_xcom_key,    # 파라미터로 받기
    **context
):
    """범용 전처리 함수 - 배민/쿠팡 공통"""
    import numpy as np
    ti = context['task_instance']
    
    # ============================================================
    # 1️.입력: XCom에서 Parquet 경로 가져오기
    # ============================================================
    parquet_path = ti.xcom_pull(
        task_ids=input_task_id,      # ← 파라미터로 받음
        key=input_xcom_key            # ← 파라미터로 받음
    )
    
    
    # ============================================================
    # 2️. 읽기
    # ============================================================
    df = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(df):,}행")
    
    # ============================================================
    # 3️. 전처리 로직
    # ============================================================

    
    print(f"전처리 완료: {len(df):,}행")
    
    # ============================================================
    # 4️. 저장
    # ============================================================
    temp_dir = ONEDRIVE_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    df.to_parquet(processed_path, index=False, engine='pyarrow')
    
    # ============================================================
    # 5️. 출력
    # ============================================================
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"전처리: {len(df):,}행"


# 데이터 병합
def concat_data(input_tasks, output_xcom_key='merged_path', **context):
    """
    여러 전처리 데이터 병합
    
    사용법:
        # dict 방식
        concat_data(
            input_tasks={'task1': 'key1', 'task2': 'key2'},
            output_xcom_key='merged_path'
        )
        
        # list 방식 (같은 key)
        concat_data(
            input_tasks=['task1', 'task2'],
            output_xcom_key='merged_path'
        )
    """
    ti = context['task_instance']
    
    # list면 기본 key 사용
    if isinstance(input_tasks, list):
        input_tasks = {t: 'processed_path' for t in input_tasks}
    
    # 각 task에서 데이터 로드
    dfs = []
    for task_id, xcom_key in input_tasks.items():
        path = ti.xcom_pull(task_ids=task_id, key=xcom_key)
        if path:
            try:
                df = pd.read_parquet(path)
                if len(df) > 0:
                    # ✅ Platform 검증
                    if 'platform' in df.columns:
                        platforms = df['platform'].value_counts()
                        print(f"[{task_id}] Platform 분포: {platforms.to_dict()}")
                    else:
                        print(f"⚠️ [{task_id}] platform 컬럼 없음!")
                    
                    dfs.append(df)
                    print(f"[{task_id}] {len(df):,}행 로드")
            except Exception as e:
                print(f"[{task_id}] 로드 실패: {e}")
    
    # 병합
    if not dfs:
        print("병합할 데이터 없음")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "병합 실패: 데이터 없음"
    
    merged_df = pd.concat(dfs, ignore_index=True)
    
    # ✅ 병합 후 Platform 검증
    if 'platform' in merged_df.columns:
        platforms_after = merged_df['platform'].value_counts()
        print(f"병합 완료: {len(merged_df):,}행 - Platform 분포: {platforms_after.to_dict()}")
        
        # 배민/쿠팡이 모두 있는지 확인
        if len(platforms_after) < 2:
            print(f"⚠️ [경고] Platform이 충분하지 않습니다!")
            print(f"   기대: 배민 + 쿠팡, 실제: {list(platforms_after.index)}")
    else:
        print(f"병합 완료: {len(merged_df):,}행")
    
    # 저장
    temp_dir = ONEDRIVE_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    merged_df.to_parquet(output_path, index=False)
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    return f"병합 완료: {len(merged_df):,}행"





'''
join_orders_with_stores = PythonOperator(
    task_id='join_orders_stores',
    python_callable=join_data,
    op_kwargs={
        'left_task': {
            'task_id': 'merge_orders',
            'xcom_key': 'merged_orders_path'
        },
        'right_task': {
            'task_id': 'load_store_info',
            'xcom_key': 'store_info_path'
        },
        'on': 'store_id',  # 단일 키
        'how': 'left',
        'output_xcom_key': 'orders_with_stores',
    },
    dag=dag,
)

'''

def join_task(
    left_task,          # 왼쪽 테이블 task 정보
    right_task,         # 오른쪽 테이블 task 정보
    on=None,            # join 키 (양쪽 동일할 때)
    left_on=None,       # 왼쪽 테이블 join 키
    right_on=None,      # 오른쪽 테이블 join 키
    how='left',         # join 타입: 'left', 'right', 'inner', 'outer'
    output_xcom_key='joined_path',
    **context
):
    """
    두 task의 데이터를 join
    
    사용법:
        # 방법 1: 양쪽 키가 같을 때
        join_data(
            left_task={'task_id': 'preprocess_baemin', 'xcom_key': 'baemin_path'},
            right_task={'task_id': 'load_store', 'xcom_key': 'store_path'},
            on='store_id',
            how='left'
        )
        
        # 방법 2: 양쪽 키가 다를 때
        join_data(
            left_task='merge_orders',
            right_task='preprocess_employee',
            left_on='store_name',
            right_on='store_names',
            how='left'
        )
    
    Args:
        left_task: dict {'task_id': ..., 'xcom_key': ...} 또는 str (task_id)
        right_task: dict {'task_id': ..., 'xcom_key': ...} 또는 str (task_id)
        on: join 키 (양쪽 동일할 때, str 또는 list)
        left_on: 왼쪽 테이블 join 키 (on과 함께 사용 불가)
        right_on: 오른쪽 테이블 join 키 (on과 함께 사용 불가)
        how: 'left', 'right', 'inner', 'outer'
        output_xcom_key: 결과 저장 XCom 키
    """
    ti = context['task_instance']
    
    # task 정보 파싱
    if isinstance(left_task, str):
        left_task = {'task_id': left_task, 'xcom_key': 'processed_path'}
    if isinstance(right_task, str):
        right_task = {'task_id': right_task, 'xcom_key': 'processed_path'}
    
    # 왼쪽 데이터 로드
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
    
    # 오른쪽 데이터 로드
    right_path = ti.xcom_pull(
        task_ids=right_task['task_id'],
        key=right_task['xcom_key']
    )
    if not right_path:
        print(f"[에러] 오른쪽 데이터 없음: {right_task['task_id']}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "join 실패: 오른쪽 데이터 없음"
    
    right_df = pd.read_parquet(right_path)
    print(f"[오른쪽] {right_task['task_id']}: {len(right_df):,}행")
    
    # join 실행
    if on is not None:
        # 양쪽 키가 같은 경우
        print(f"\njoin 실행: how={how}, on={on}")
        joined_df = left_df.merge(right_df, on=on, how=how)
    elif left_on is not None and right_on is not None:
        # 양쪽 키가 다른 경우
        print(f"\njoin 실행: how={how}, left_on={left_on}, right_on={right_on}")
        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)
    else:
        raise ValueError("on 또는 (left_on, right_on)을 지정해야 합니다.")
    
    print(f"join 완료: {len(joined_df):,}행")
    
    # 저장
    temp_dir = ONEDRIVE_DB / 'temp'
    temp_dir.mkdir(exist_ok=True, parents=True)
    output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    joined_df.to_parquet(output_path, index=False)
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    return f"join 완료: {len(joined_df):,}행"





# csv에 저장
"""
Parquet을 CSV로 변환 및 저장 (중복 제거 포함)
save_csv_task1 = PythonOperator(
    
    task_id='save_to_csv_orders',
    python_callable=save_to_csv,
    op_kwargs={
        'input_task_id': 'preprocess_join_orders_with_stores',
        'input_xcom_key': 'orders_with_stores_processed',
        'output_csv_path': '/opt/airflow/Doridang/영업팀_DB/sales_daily_orders.csv',
        'dedup_key': 'order_id',
        'mode': 'append'
    },
    dag=dag,
)

"""


def save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename='sales_daily_orders.csv',
    output_subdir='영업관리부_DB',
    dedup_key='sub_order_id',  # ⭐ sub_order_id 기준
    mode='append',
    **context
):
    """
    로컬 DB에 CSV 저장 후 OneDrive에 Parquet으로 백업
    
    중복 제거:
    - 기준: sub_order_id (고유 ID)
    - 시점: 기존 CSV + 새 데이터 병합 후
    - keep='last': 최신 데이터 우선
    """
    import os
    import shutil
    import tempfile
    from pathlib import Path
    from modules.transform.utility.paths import LOCAL_DB, ONEDRIVE_DB
    
    ti = context['task_instance']
    
    # 1. Parquet 경로 가져오기
    parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)
    if not parquet_path:
        print(f"[에러] 입력 데이터 없음: {input_task_id}")
        return "저장 실패: 데이터 없음"
    
    # 2. 새 데이터 읽기
    new_df = pd.read_parquet(parquet_path)
    print(f"\n{'='*60}")
    print(f"[입력] 새 데이터: {len(new_df):,}행")
    
    # ⭐ sub_order_id 존재 확인
    if 'sub_order_id' not in new_df.columns:
        print(f"[에러] sub_order_id 컬럼 없음!")
        print(f"[에러] 실제 컬럼: {list(new_df.columns)}")
        return "저장 실패: sub_order_id 없음"
    else:
        print(f"[확인] sub_order_id 존재: {new_df['sub_order_id'].nunique():,}개 고유 ID")
    
    # Platform 검증
    if 'platform' in new_df.columns:
        platform_counts = new_df['platform'].value_counts()
        print(f"[확인] Platform 분포: {platform_counts.to_dict()}")
    
    # 3. 로컬 DB 경로 설정
    if output_csv_path:
        local_csv_path = Path(output_csv_path)
    else:
        output_dir = LOCAL_DB / output_subdir
        output_dir.mkdir(parents=True, exist_ok=True)
        local_csv_path = output_dir / output_filename
    
    local_csv_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[로컬DB] 경로: {local_csv_path}")
    
    # 4. 기존 CSV 읽기
    existing_df = None
    if mode == 'append' and local_csv_path.exists():
        try:
            if local_csv_path.stat().st_size == 0:
                print("[정보] 기존 CSV가 빈 파일입니다.")
            else:
                for encoding in ['utf-8-sig', 'utf-8', 'cp949']:
                    try:
                        existing_df = pd.read_csv(local_csv_path, encoding=encoding)
                        print(f"[기존] 데이터: {len(existing_df):,}행 ({encoding})")
                        
                        # ⭐ 기존 데이터에 sub_order_id 없으면 경고
                        if 'sub_order_id' not in existing_df.columns:
                            print(f"[경고] 기존 CSV에 sub_order_id 없음!")
                            print(f"[경고] 기존 데이터는 중복 제거 불가능")
                        else:
                            print(f"[확인] 기존 sub_order_id: {existing_df['sub_order_id'].nunique():,}개")
                        break
                    except UnicodeDecodeError:
                        continue
        except Exception as e:
            print(f"[경고] 기존 CSV 읽기 실패: {e}")
            existing_df = None
    
    # 5. 병합
    if existing_df is None or existing_df.empty:
        combined_df = new_df.copy()
        print(f"[병합] 신규 생성: {len(combined_df):,}행")
    else:
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        print(f"[병합] 병합 후: {len(combined_df):,}행")
    
    # 6. ⭐ sub_order_id 기준 중복 제거 (최종 단계)
    print(f"\n{'='*60}")
    print(f"[중복 제거] sub_order_id 기준 중복 제거 시작...")
    
    dedup_cols = [dedup_key] if isinstance(dedup_key, str) else dedup_key
    valid_cols = [c for c in dedup_cols if c in combined_df.columns]
    
    if valid_cols:
        before = len(combined_df)
        
        # 중복 체크 (삭제 전)
        dup_check = combined_df[combined_df.duplicated(subset=valid_cols, keep=False)]
        if len(dup_check) > 0:
            print(f"[중복 제거] 발견된 중복: {len(dup_check):,}행")
            print(f"[중복 제거] 중복 예시 (상위 5개):")
            for idx, row in dup_check.head(5).iterrows():
                print(f"  - {row.get('order_id', 'N/A')} / {row.get('sub_order_id', 'N/A')}")
        else:
            print(f"[중복 제거] 중복 없음")
        
        # 중복 제거 실행
        combined_df.drop_duplicates(subset=valid_cols, keep='last', inplace=True)
        after = len(combined_df)
        removed = before - after
        
        print(f"[중복 제거] 완료: {removed:,}건 제거 (기준: {valid_cols})")
        print(f"[중복 제거] 최종 데이터: {after:,}행")
    else:
        print(f"[경고] 중복 제거 키 없음: {dedup_cols}")
    
    # 7. 로컬 DB에 CSV 저장 (원자적 쓰기)
    print(f"\n{'='*60}")
    print(f"[저장] CSV 저장 시작...")
    
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
        
        backup_path = None
        if local_csv_path.exists():
            backup_path = local_csv_path.parent / f"{local_csv_path.name}.bak"
            shutil.copy2(local_csv_path, backup_path)
        
        shutil.move(tmp_path, str(local_csv_path))
        print(f"[저장] ✅ 로컬 CSV 저장 완료: {len(combined_df):,}건")
        
        if backup_path and backup_path.exists():
            backup_path.unlink()
        
    except Exception as e:
        print(f"[에러] CSV 저장 실패: {e}")
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        return f"저장 실패: {e}"
    
    # 8. OneDrive에 Parquet으로 백업
    print(f"\n{'='*60}")
    print(f"[백업] OneDrive Parquet 백업 시작...")
    
    try:
        parquet_filename = local_csv_path.stem + '.parquet'
        onedrive_parquet_path = ONEDRIVE_DB / output_subdir / parquet_filename
        onedrive_parquet_path.parent.mkdir(parents=True, exist_ok=True)
        
        combined_df.to_parquet(
            onedrive_parquet_path,
            index=False,
            engine='pyarrow',
            compression='snappy'
        )
        
        csv_size = local_csv_path.stat().st_size / (1024 * 1024)
        parquet_size = onedrive_parquet_path.stat().st_size / (1024 * 1024)
        compression_ratio = (1 - parquet_size / csv_size) * 100 if csv_size > 0 else 0
        
        print(f"[백업] ✅ OneDrive Parquet 백업 완료")
        print(f"  - 경로: {onedrive_parquet_path}")
        print(f"  - CSV 크기: {csv_size:.2f} MB")
        print(f"  - Parquet 크기: {parquet_size:.2f} MB")
        print(f"  - 압축률: {compression_ratio:.1f}%")
        
    except Exception as e:
        print(f"[백업] ⚠️ OneDrive Parquet 백업 실패: {e}")
    
    print(f"{'='*60}\n")
    return f"✅ 저장 완료: {len(combined_df):,}건 (로컬 CSV + OneDrive Parquet)"



def text_to_html(text):
    """
    일반 텍스트를 HTML로 변환
    - 줄바꿈 → <br>
    - 자동 스타일 적용
    """
    # 줄바꿈을 <br>로 변환
    text = text.replace('\n', '<br>')
    
    # 기본 HTML 템플릿
    html = f"""<html>
<head>
<meta charset="UTF-8">
</head>
<body style="font-family: 'Malgun Gothic', Arial, sans-serif; margin: 20px; line-height: 1.6;">
<div style="background: #f8f9fa; padding: 20px; border-radius: 5px; border-left: 4px solid #27ae60;">
{text}
</div>
<p style="color: #999; font-size: 12px; margin-top: 20px;">
이 메일은 자동으로 발송되었습니다.
</p>
</body>
</html>"""
    
    return html


'''
# dag 사용법

def send_completion_email(**context):
    """처리 완료 알림 이메일"""
    
    # 일반 텍스트로 작성
    message = f"""
#일일 주문 데이터 처리 완료

처리 일시: {context['ds']}
DAG: {context['dag'].dag_id}
실행 시간: {context['ts']}
상태: 정상 완료
"""
    
    return send_email(
        subject=f'[도리당] 일일 주문 데이터 처리 완료 ({context["ds"]})',
        html_content=text_to_html(message),
        to_emails=['a17019@kakao.com'],
        conn_id='conn_smtp_gmail',
        **context
    )
'''


# 이메일 발송 (Airflow Connection 사용)
def send_email(
    subject,
    html_content,
    to_emails,
    conn_id='conn_smtp_gmail',
    **context
):
    """
    이메일 발송 (Airflow SMTP Connection 사용)
    
    사용법:
        send_email(
            subject='[도리당] 가맹점 리스트 업데이트',
            html_content='<h1>완료</h1><p>처리 완료</p>',
            to_emails='a17019@kakao.com',  # 또는 ['email1', 'email2']
            conn_id='conn_smtp_gmail'
        )
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from airflow.hooks.base import BaseHook
    
    # Airflow Connection에서 SMTP 정보 가져오기
    connection = BaseHook.get_connection(conn_id)
    
    smtp_host = connection.host
    smtp_port = connection.port
    smtp_user = connection.login
    smtp_password = connection.password
    from_email = connection.extra_dejson.get('from_email') or smtp_user
    
    # to_emails를 리스트로 변환
    if isinstance(to_emails, str):
        to_list = [to_emails]
    else:
        to_list = to_emails
    
    # 이메일 생성
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ', '.join(to_list)
    
    # HTML 본문
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)
    
    # SMTP 발송
    try:
        print(f"📧 이메일 발송 시작...")
        print(f"   - 발신: {from_email}")
        print(f"   - 수신: {to_list}")
        print(f"   - 제목: {subject}")
        
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        
        print(f"✅ 이메일 발송 성공: {len(to_list)}명")
        return f"이메일 발송 완료: {len(to_list)}명"
    
    except Exception as e:
        print(f"❌ 이메일 발송 실패: {e}")
        raise


# Temp Parquet 파일 정리 함수
def cleanup_temp_parquets(**context):
    """
    temp 디렉토리의 모든 parquet 파일을 삭제합니다.
    
    DAG 사용법:
    -----------
    from modules.transform.utility.io import cleanup_temp_parquets
    
    cleanup_task = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=cleanup_temp_parquets,
        provide_context=True,
        dag=dag,
    )
    
    # 일반적으로 DAG의 맨 마지막에 배치
    [task1, task2, task3] >> cleanup_task
    
    Returns:
    --------
    str: 삭제된 파일 수와 용량 정보
    """
    import shutil
    from pathlib import Path
    
    temp_dir = ONEDRIVE_DB / 'temp'
    
    if not temp_dir.exists():
        print(f"[정리] temp 디렉토리 없음: {temp_dir}")
        return "삭제할 파일 없음 (디렉토리 없음)"
    
    # parquet 파일 찾기
    parquet_files = list(temp_dir.glob('*.parquet'))
    
    if not parquet_files:
        print(f"[정리] 삭제할 parquet 파일 없음")
        return "삭제할 파일 없음"
    
    # 파일 크기 계산
    total_size = sum(f.stat().st_size for f in parquet_files)
    total_size_mb = total_size / (1024 * 1024)
    
    print(f"[정리] 삭제 대상: {len(parquet_files)}개 파일 ({total_size_mb:.2f} MB)")
    
    # 파일 삭제
    deleted_count = 0
    failed_files = []
    
    for parquet_file in parquet_files:
        try:
            parquet_file.unlink()
            deleted_count += 1
            print(f"[정리] 삭제: {parquet_file.name}")
        except Exception as e:
            failed_files.append((parquet_file.name, str(e)))
            print(f"[경고] 삭제 실패: {parquet_file.name} - {e}")
    
    # 결과 메시지
    result = f"삭제 완료: {deleted_count}개 파일 ({total_size_mb:.2f} MB)"
    
    if failed_files:
        result += f", 실패: {len(failed_files)}개"
        print(f"[경고] 삭제 실패 파일 목록:")
        for fname, error in failed_files:
            print(f"  - {fname}: {error}")
    
    return result


# 수집 CSV 파일 정리 함수
def cleanup_collected_csvs(**context):
    """
    영업팀_수집 디렉토리의 모든 CSV 파일을 삭제합니다.
    
    DAG 사용법:
    -----------
    from modules.transform.utility.io import cleanup_collected_csvs
    
    cleanup_csv_task = PythonOperator(
        task_id='cleanup_collected_files',
        python_callable=cleanup_collected_csvs,
        provide_context=True,
        dag=dag,
    )
    
    # 일반적으로 DAG의 맨 마지막에 배치 (temp 파일 정리와 함께)
    [email_task] >> cleanup_temp_task >> cleanup_csv_task
    
    Returns:
    --------
    str: 삭제된 파일 수와 용량 정보
    """
    import shutil
    from pathlib import Path
    
    collect_dir = COLLECT_DB / '영업관리부_수집'
    
    if not collect_dir.exists():
        print(f"[정리] 수집 디렉토리 없음: {collect_dir}")
        return "삭제할 파일 없음 (디렉토리 없음)"
    
    # CSV 파일 찾기 (baemin, coupang 등)
    csv_files = list(collect_dir.glob('*.csv'))
    
    if not csv_files:
        print(f"[정리] 삭제할 CSV 파일 없음")
        return "삭제할 파일 없음"
    
    # 파일 크기 계산
    total_size = sum(f.stat().st_size for f in csv_files)
    total_size_mb = total_size / (1024 * 1024)
    
    print(f"[정리] 삭제 대상: {len(csv_files)}개 CSV 파일 ({total_size_mb:.2f} MB)")
    
    # 파일 삭제
    deleted_count = 0
    failed_files = []
    
    for csv_file in csv_files:
        try:
            csv_file.unlink()
            deleted_count += 1
            print(f"[정리] 삭제: {csv_file.name}")
        except Exception as e:
            failed_files.append((csv_file.name, str(e)))
            print(f"[경고] 삭제 실패: {csv_file.name} - {e}")
    
    # 결과 메시지
    result = f"CSV 삭제 완료: {deleted_count}개 파일 ({total_size_mb:.2f} MB)"
    
    if failed_files:
        result += f", 실패: {len(failed_files)}개"
        print(f"[경고] 삭제 실패 파일 목록:")
        for fname, error in failed_files:
            print(f"  - {fname}: {error}")
    
    return result


# ============================================================
# CSV → Parquet 변환 및 OneDrive 저장
# ============================================================
'''
from modules.transform.utility.io import csv_to_parquet_backup

'''


def csv_to_parquet_backup(
    csv_filename: str,
    onedrive_subfolder: str = "영업관리부_DB",
    **context
) -> str:
    """
    로컬DB의 CSV 파일을 읽어 Parquet으로 변환 후 OneDrive에 저장
    
    Args:
        csv_filename: 로컬DB에서 읽을 CSV 파일명 (e.g., 'sales_employee.csv')
        onedrive_subfolder: OneDrive에 저장할 서브폴더 (기본값: '영업관리부_DB')
        **context: Airflow context
    
    Returns:
        str: 처리 결과 메시지
    
    Example:
        csv_to_parquet_backup('sales_employee.csv', '영업관리부_DB')
        # LOCAL_DB/sales_employee.csv → ONEDRIVE_DB/영업관리부_DB/sales_employee.parquet
    """
    try:
        print("=" * 60)
        print("🔄 CSV → Parquet 변환 및 백업 시작")
        print("=" * 60)
        
        # 1. 로컬DB에서 CSV 읽기
        csv_path = LOCAL_DB / csv_filename
        print(f"📂 로컬DB 경로: {csv_path}")
        
        if not csv_path.exists():
            print(f"❌ 파일을 찾을 수 없음: {csv_path}")
            return f"실패: 파일 없음 ({csv_filename})"
        
        # 인코딩 자동 감지
        encodings = ['utf-8', 'cp949', 'euc-kr', 'latin1']
        df = None
        used_encoding = None
        
        for encoding in encodings:
            try:
                print(f"   시도: {encoding}으로 읽기...")
                df = pd.read_csv(csv_path, encoding=encoding)
                used_encoding = encoding
                print(f"   ✅ {encoding}으로 성공 ({len(df):,}행)")
                break
            except (UnicodeDecodeError, LookupError):
                continue
        
        if df is None:
            print(f"❌ CSV 읽기 실패 (모든 인코딩 시도 실패)")
            return f"실패: CSV 읽기 실패 ({csv_filename})"
        
        print(f"📊 CSV 정보:")
        print(f"   - 파일명: {csv_path.name}")
        print(f"   - 행 수: {len(df):,}")
        print(f"   - 컬럼 수: {len(df.columns)}")
        print(f"   - 컬럼명: {list(df.columns)}")
        print(f"   - 인코딩: {used_encoding}")
        
        # 2. Parquet 파일명 생성 (확장자 변경)
        parquet_filename = csv_path.stem + ".parquet"  # sales_employee.parquet
        
        # 3. OneDrive 저장 경로 생성
        onedrive_path = ONEDRIVE_DB / onedrive_subfolder
        onedrive_path.mkdir(parents=True, exist_ok=True)
        
        parquet_output_path = onedrive_path / parquet_filename
        print(f"\n💾 OneDrive 저장 경로: {parquet_output_path}")
        
        # 4. Parquet으로 변환 및 저장
        df.to_parquet(
            parquet_output_path,
            index=False,
            engine='pyarrow',
            compression='snappy'
        )
        print(f"✅ Parquet 저장 완료")
        
        # 5. 파일 크기 확인
        parquet_size = parquet_output_path.stat().st_size / (1024 * 1024)  # MB
        csv_size = csv_path.stat().st_size / (1024 * 1024)  # MB
        compression_ratio = (1 - parquet_size / csv_size) * 100 if csv_size > 0 else 0
        
        print(f"\n📈 파일 크기 비교:")
        print(f"   - CSV: {csv_size:.2f} MB")
        print(f"   - Parquet: {parquet_size:.2f} MB")
        print(f"   - 압축률: {compression_ratio:.1f}%")
        
        print("=" * 60)
        print(f"🎉 CSV → Parquet 변환 완료: {len(df):,}행")
        print("=" * 60)
        
        return f"✅ 완료: {len(df):,}행 ({parquet_filename})"
        
    except Exception as e:
        print(f"❌ 처리 실패: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return f"❌ 실패: {str(e)}"


# ============================================================
# sub_order_id 생성 함수들
# ============================================================

def create_sub_order_id_simple(
    df: pd.DataFrame,
    order_col: str = 'order_id',
    output_col: str = 'sub_order_id'
) -> pd.DataFrame:
    """
    주문번호 + 순번 방식으로 sub_order_id 생성 (권장)
    
    예시: B22V00DE2A_1, B22V00DE2A_2, ...
    
    Args:
        df: 데이터프레임
        order_col: 주문번호 컬럼명
        output_col: 출력 컬럼명
    
    Returns:
        sub_order_id가 추가된 데이터프레임
    """
    df = df.copy()
    
    # 주문번호별 순번 부여
    df[output_col] = (
        df[order_col].astype(str) + '_' + 
        (df.groupby(order_col).cumcount() + 1).astype(str)
    )
    
    print(f"[sub_order_id] 단순 순번 방식으로 생성 완료")
    print(f"  - 원본 데이터: {len(df):,}행")
    print(f"  - 고유 ID: {df[output_col].nunique():,}개")
    
    # 중복 체크
    duplicates = df[df.duplicated(subset=output_col, keep=False)]
    if len(duplicates) > 0:
        print(f"  ⚠️ 경고: {len(duplicates)}개 중복 발견!")
    else:
        print(f"  ✅ 중복 없음")
    
    return df


def create_sub_order_id_hash(
    df: pd.DataFrame,
    natural_key_cols: list,
    output_col: str = 'sub_order_id'
) -> pd.DataFrame:
    """
    해시 기반으로 sub_order_id 생성 (복합키 필요 시)
    
    예시: a1b2c3d4e5f6g7h8 (16자리 해시)
    
    Args:
        df: 데이터프레임
        natural_key_cols: 자연키 컬럼 리스트 (예: ['order_id', 'collected_at', 'order_summary'])
        output_col: 출력 컬럼명
    
    Returns:
        sub_order_id가 추가된 데이터프레임
    """
    import hashlib
    
    df = df.copy()
    
    # 자연키 컬럼들을 문자열로 결합
    key_parts = [df[col].astype(str) for col in natural_key_cols]
    uk_series = pd.concat(key_parts, axis=1).agg('|'.join, axis=1)
    
    # SHA1 해시의 앞 16자리 사용
    df[output_col] = uk_series.apply(
        lambda s: hashlib.sha1(s.encode('utf-8')).hexdigest()[:16]
    )
    
    print(f"[sub_order_id] 해시 방식으로 생성 완료")
    print(f"  - 원본 데이터: {len(df):,}행")
    print(f"  - 고유 ID: {df[output_col].nunique():,}개")
    print(f"  - 자연키: {natural_key_cols}")
    
    # 중복 체크
    duplicates = df[df.duplicated(subset=output_col, keep=False)]
    if len(duplicates) > 0:
        print(f"  ⚠️ 경고: {len(duplicates)}개 중복 발견!")
    else:
        print(f"  ✅ 중복 없음")
    
    return df