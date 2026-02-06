import glob
import os
import pandas as pd
from pathlib import Path
from typing import List, Union, Optional
from modules.transform.utility.paths import ONEDRIVE_DB, COLLECT_DB, LOCAL_DB, TEMP_DIR
import numpy as np

# ============================================================
# CSV 로드 함수들
# ============================================================
def read_csv_glob(
    files: Union[str, List[str]],
    add_source_col: bool = False,
    source_col_name: str = "source_file",
    ignore_index: bool = True,
    on_error: str = "raise",
    **read_csv_kwargs,
) -> pd.DataFrame:
    """Glob 패턴 또는 파일 경로 리스트로 여러 CSV를 읽어 하나의 DataFrame으로 반환"""
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
    dedup_key=None,
    add_row_hash=False,  # ⭐ 새 옵션: 행 해시 추가
    **context
):
    """
    CSV 데이터 범용 로드 함수 (중복 제거 로직 개선)
    
    Args:
        file_path: 파일 경로 또는 glob 패턴
        xcom_key: XCom에 저장할 키 이름
        use_glob: True면 glob 패턴으로 처리, False면 단일 파일
        dedup_key: 중복 제거 기준 컬럼 (str 또는 list, None이면 중복 제거 안함)
        add_row_hash: True면 각 행에 고유 해시 추가 (_row_hash 컬럼)
                      같은 주문의 같은 메뉴 2개(완전 동일 행)도 구분 가능
        
    Returns:
        str: 처리 결과 메시지
        
    중복 제거 전략:
    ================
    1. collected_at을 키에서 제외하여 다른 시점 업로드 시에도 중복 감지
    2. add_row_hash=True 사용 시 동일 행도 구분 가능 (행 인덱스 + 소스파일 기반)
    3. 같은 주문의 같은 메뉴 2개 주문 → 보존됨 (row_hash가 다름)
    """
    import glob as glob_module
    import hashlib
    from pathlib import Path
    ti = context['task_instance']
    
    temp_dir = TEMP_DIR
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
        
        print(f"[INFO] 로드할 파일: {len(file_list)}개")
        for fpath in file_list:
            print(f"   ✓ {Path(fpath).name}")
        
        # 2. 인코딩 자동 감지하며 CSV 읽기
        dfs = []
        encodings = ['utf-8', 'cp949', 'euc-kr', 'latin1']
        
        for file_idx, fpath in enumerate(file_list):
            df = None
            for encoding in encodings:
                try:
                    print(f"   시도: {Path(fpath).name} ({encoding})")
                    df = pd.read_csv(fpath, encoding=encoding)
                    print(f"   ✓ {encoding}으로 읽음: {len(df)}행")
                    
                    # ⭐ 행 해시 추가 (옵션) - 파일명 제외, 순수 데이터 기반
                    if add_row_hash:
                        # ⭐ 수정: 파일명 제외! 순수 데이터 값만으로 해시 생성
                        # 이렇게 하면 같은 내용이면 파일명이 달라도 같은 해시
                        df['_row_hash'] = df.apply(
                            lambda row: hashlib.md5(
                                str(tuple(row.values)).encode('utf-8')
                            ).hexdigest()[:12],
                            axis=1
                        )
                        print(f"   [해시] 데이터 기반 해시: {df['_row_hash'].nunique()}개 고유값")
                    
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
        
        # 4. 중복 제거 (개선된 로직)
        if dedup_key:
            before = len(result_df)
            
            if isinstance(dedup_key, str):
                dedup_cols = [dedup_key]
            else:
                dedup_cols = list(dedup_key)
            
            # 실제 존재하는 컬럼만 사용
            valid_cols = [col for col in dedup_cols if col in result_df.columns]
            
            if valid_cols:
                # ⭐ 수정: 해시 없이 원본 데이터 컬럼만으로 중복 제거
                # _row_hash는 sub_order_id 생성용으로만 사용
                print(f"[중복제거] 키: {valid_cols}")
                result_df.drop_duplicates(subset=valid_cols, keep='first', inplace=True)
                
                after = len(result_df)
                removed = before - after
                if removed > 0:
                    print(f"[중복제거] {removed:,}건 제거됨")
                else:
                    print(f"[중복제거] 중복 없음")
            else:
                print(f"[경고] 중복 제거 컬럼 없음: {dedup_cols}")
                print(f"[경고] 실제 컬럼: {list(result_df.columns)[:10]}...")
        
        # 5. Parquet로 저장
        for col in result_df.columns:
            if result_df[col].dtype == 'object':
                if col in ['캠페인ID', 'ad_id', 'store_id']:
                    result_df[col] = result_df[col].astype(str)
        
        result_df.to_parquet(parquet_path, index=False, engine='pyarrow')
        print(f"\n✅ 데이터 로드 완료: {len(result_df):,}건")
        print(f"   저장 경로: {parquet_path}")
        
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
# 나머지 함수들은 기존과 동일
# ============================================================

def preprocess_df(
    input_task_id,
    input_xcom_key,
    output_xcom_key,
    **context
):
    """범용 전처리 함수"""
    import numpy as np
    ti = context['task_instance']
    
    parquet_path = ti.xcom_pull(
        task_ids=input_task_id,
        key=input_xcom_key
    )
    
    df = pd.read_parquet(parquet_path)
    print(f"전처리 시작: {len(df):,}행")
    
    print(f"전처리 완료: {len(df):,}행")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    processed_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    df.to_parquet(processed_path, index=False, engine='pyarrow')
    
    ti.xcom_push(key=output_xcom_key, value=str(processed_path))
    
    return f"전처리: {len(df):,}행"


def concat_data(input_tasks, output_xcom_key='merged_path', **context):
    """여러 전처리 데이터 병합"""
    ti = context['task_instance']
    
    if isinstance(input_tasks, list):
        input_tasks = {t: 'processed_path' for t in input_tasks}
    
    dfs = []
    for task_id, xcom_key in input_tasks.items():
        path = ti.xcom_pull(task_ids=task_id, key=xcom_key)
        if path:
            try:
                df = pd.read_parquet(path)
                if len(df) > 0:
                    if 'platform' in df.columns:
                        platforms = df['platform'].value_counts()
                        print(f"[{task_id}] Platform 분포: {platforms.to_dict()}")
                    else:
                        print(f"⚠️ [{task_id}] platform 컬럼 없음!")
                    
                    dfs.append(df)
                    print(f"[{task_id}] {len(df):,}행 로드")
            except Exception as e:
                print(f"[{task_id}] 로드 실패: {e}")
    
    if not dfs:
        print("병합할 데이터 없음")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "병합 실패: 데이터 없음"
    
    merged_df = pd.concat(dfs, ignore_index=True)
    
    if 'platform' in merged_df.columns:
        platforms_after = merged_df['platform'].value_counts()
        print(f"병합 완료: {len(merged_df):,}행 - Platform 분포: {platforms_after.to_dict()}")
    else:
        print(f"병합 완료: {len(merged_df):,}행")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    merged_df.to_parquet(output_path, index=False)
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    return f"병합 완료: {len(merged_df):,}행"


def join_task(
    left_task,
    right_task,
    on=None,
    left_on=None,
    right_on=None,
    how='left',
    output_xcom_key='joined_path',
    **context
):
    """두 task의 데이터를 join"""
    ti = context['task_instance']
    
    if isinstance(left_task, str):
        left_task = {'task_id': left_task, 'xcom_key': 'processed_path'}
    if isinstance(right_task, str):
        right_task = {'task_id': right_task, 'xcom_key': 'processed_path'}
    
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
        print(f"[에러] 오른쪽 데이터 없음: {right_task['task_id']}")
        ti.xcom_push(key=output_xcom_key, value=None)
        return "join 실패: 오른쪽 데이터 없음"
    
    right_df = pd.read_parquet(right_path)
    print(f"[오른쪽] {right_task['task_id']}: {len(right_df):,}행")
    
    if on is not None:
        print(f"\njoin 실행: how={how}, on={on}")
        joined_df = left_df.merge(right_df, on=on, how=how)
    elif left_on is not None and right_on is not None:
        print(f"\njoin 실행: how={how}, left_on={left_on}, right_on={right_on}")
        joined_df = left_df.merge(right_df, left_on=left_on, right_on=right_on, how=how)
    else:
        raise ValueError("on 또는 (left_on, right_on)을 지정해야 합니다.")
    
    print(f"join 완료: {len(joined_df):,}행")
    
    temp_dir = TEMP_DIR
    temp_dir.mkdir(exist_ok=True, parents=True)
    output_path = temp_dir / f"{output_xcom_key}_{context['ds_nodash']}.parquet"
    joined_df.to_parquet(output_path, index=False)
    
    ti.xcom_push(key=output_xcom_key, value=str(output_path))
    return f"join 완료: {len(joined_df):,}행"

# csv 저장

def save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename='sales_daily_orders.csv',
    output_subdir='영업관리부_DB',
    dedup_key='sub_order_id',
    mode='append',
    **context
):
    """
    Parquet 데이터를 로컬 DB에 CSV로 저장 (컬럼 순서 명확히 정의)
    
    ⭐ 개선:
    - 컬럼 순서 명확히 정의
    - 날짜 형식 통일
    - 기존 CSV와 컬럼 순서 일치
    """
    import os
    import shutil
    import tempfile
    from pathlib import Path
    from modules.transform.utility.paths import LOCAL_DB
    
    ti = context['task_instance']
    
    # ============================================================
    # ⭐⭐⭐ 컬럼 순서 정의 (수정됨) ⭐⭐⭐
    # ============================================================
    STANDARD_COLUMNS = [
        'platform', 'order_date', 'store_id', 'store_names',
        'ad_id', 'ad_product', 'order_id', 'sub_order_id',
        'order_summary', 'menu_option_name',
        'delivery_type', 'total_amount', 'menu_amount', 'menu_option_price',
        'instant_discount_coupon', 'instant_discount_coupon_YN',
        'collected_at', 'option_qty', 'fee_ad', 'settlement_amount',
        # 매장명 컬럼은 최종 CSV에서 제외 (store_names로 충분)
        '담당자', 'email', '상세주소', '광역', '시군구', '읍면동',
        '_row_hash', '실오픈일'
    ]
    
    # ============================================================
    # 1. 입력 데이터 로드
    # ============================================================
    parquet_path = ti.xcom_pull(task_ids=input_task_id, key=input_xcom_key)
    if not parquet_path:
        print(f"[에러] 입력 데이터 없음: {input_task_id}")
        return "저장 실패: 데이터 없음"
    
    # Parquet 읽기
    new_df = pd.read_parquet(parquet_path)
    print(f"\n{'='*60}")
    print(f"[입력] 새 데이터: {len(new_df):,}행 × {len(new_df.columns)}컬럼")
    print(f"[입력] 실제 컬럼: {list(new_df.columns)}")  # ⭐ 디버깅 추가

    # ============================================================
    # ⭐⭐⭐ 컬럼 이름 매핑 (호환성) ⭐⭐⭐
    # ============================================================
    # instant_discount_amount_YN → instant_discount_coupon_YN로 변경 없음
    # (이미 처리됨)
    
    # collected_at_x → collected_at로 통일
    if 'collected_at_x' in new_df.columns and 'collected_at' not in new_df.columns:
        new_df.rename(columns={'collected_at_x': 'collected_at'}, inplace=True)
        print(f"[컬럼 매핑] collected_at_x → collected_at")
    
    # collected_at_y는 제거 (중복)
    if 'collected_at_y' in new_df.columns:
        new_df = new_df.drop(columns=['collected_at_y'])
        print(f"[컬럼 제거] collected_at_y (중복)")
    # 쿠폰 YN 컬럼 통일: instant_discount_amount_YN → instant_discount_coupon_YN
    if 'instant_discount_amount_YN' in new_df.columns and 'instant_discount_coupon_YN' not in new_df.columns:
        new_df.rename(columns={'instant_discount_amount_YN': 'instant_discount_coupon_YN'}, inplace=True)
        print(f"[컬럼 매핑] instant_discount_amount_YN → instant_discount_coupon_YN")
    
    
    # 중복/임시 컬럼 제거
    temp_cols = [
        'store_names_key', 'store_name_normalized', 'store_name_original', 
        '매장명_normalized', '매장명_full', 'store_names_match',
        'stores'  # 이전 데이터의 중복 컬럼
    ]
    for col in temp_cols:
        if col in new_df.columns:
            new_df = new_df.drop(columns=[col])
            print(f"[컬럼 제거] {col}")

    
    # ============================================================
    # ⭐ 날짜 형식 통일 (문자열로 변환)
    # ============================================================
    print(f"\n[날짜 정규화] 시작...")
    for col in ['order_date', 'collected_at', '실오픈일']:
        if col in new_df.columns:
            if pd.api.types.is_datetime64_any_dtype(new_df[col]):
                if col == '실오픈일':
                    new_df[col] = new_df[col].dt.strftime('%Y-%m-%d')
                else:
                    new_df[col] = new_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                print(f"  ✓ {col}: datetime → 문자열")
    
    # ============================================================
    # ⭐ 컬럼 순서 정렬 (표준 순서 적용)
    # ============================================================
    available_cols = [col for col in STANDARD_COLUMNS if col in new_df.columns]
    missing_cols = set(STANDARD_COLUMNS) - set(new_df.columns)
    extra_cols = set(new_df.columns) - set(STANDARD_COLUMNS)
    
    print(f"\n[컬럼 정렬]")
    print(f"  - 표준 컬럼: {len(STANDARD_COLUMNS)}개")
    print(f"  - 사용 가능: {len(available_cols)}개")
    if missing_cols:
        print(f"  - 누락: {missing_cols}")
        # ⭐ 누락된 필수 컬럼 추가 (NaN)
        for col in missing_cols:
            if col in ['platform', 'order_date', 'store_names', 'order_id', 'sub_order_id', 
                       'total_amount', '매장명', '담당자', 'order_summary']:  # 필수 컬럼만
                new_df[col] = np.nan
                print(f"    + {col} 추가 (NaN)")
        
        # 다시 available_cols 업데이트
        available_cols = [col for col in STANDARD_COLUMNS if col in new_df.columns]
    
    if extra_cols:
        print(f"  - 추가: {extra_cols}")
        # 추가 컬럼도 포함
        available_cols.extend(list(extra_cols))
    
    new_df = new_df[available_cols].copy()
    print(f"  ✓ 컬럼 순서 정렬 완료: {len(available_cols)}개")
    print(f"  ✓ 최종 컬럼 순서: {list(new_df.columns[:10])}... (총 {len(new_df.columns)}개)")  # ⭐ 확인
    
    # 타입 정보 출력 (디버깅용)
    print(f"\n[타입] 입력 데이터 타입:")
    for col in new_df.columns[:5]:  # 처음 5개만
        print(f"  - {col}: {new_df[col].dtype}")
    if len(new_df.columns) > 5:
        print(f"  ... (총 {len(new_df.columns)}개 컬럼)")

    
    # 중복 제거 키 확인
    if isinstance(dedup_key, str):
        dedup_cols = [dedup_key]
    else:
        dedup_cols = list(dedup_key)
    # 옵션 수량도 중복 키에 포함 (권장)
    if 'option_qty' in new_df.columns and 'option_qty' not in dedup_cols:
        dedup_cols.append('option_qty')
    
    for key in dedup_cols:
        if key not in new_df.columns:
            print(f"[에러] 중복 제거 키 없음: {key}")
            return f"저장 실패: {key} 컬럼 없음"
    
    print(f"[확인] 중복 제거 키: {dedup_cols}")
    for key in dedup_cols:
        print(f"  - {key}: {new_df[key].nunique():,}개 고유값")
    
    # ============================================================
    # 2. 출력 경로 설정
    # ============================================================
    if output_csv_path:
        local_csv_path = Path(output_csv_path)
    else:
        output_dir = LOCAL_DB / output_subdir
        output_dir.mkdir(parents=True, exist_ok=True)
        local_csv_path = output_dir / output_filename
    
    local_csv_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n[로컬DB] 경로: {local_csv_path}")
    
    # ============================================================
    # 3. 기존 CSV 읽기 (append 모드)
    # ============================================================
    existing_df = None
    if mode == 'append' and local_csv_path.exists():
        try:
            if local_csv_path.stat().st_size == 0:
                print("[정보] 기존 CSV가 빈 파일입니다.")
            else:
                print(f"[기존] CSV 읽기 시도: {local_csv_path.name}")
                
                # 여러 인코딩 시도
                encodings_to_try = ['utf-8-sig', 'utf-8', 'cp949', 'euc-kr']
                for encoding in encodings_to_try:
                    try:
                        existing_df = pd.read_csv(
                            local_csv_path, 
                            encoding=encoding, 
                            low_memory=False
                        )
                        print(f"[기존] ✓ 성공: {len(existing_df):,}행 ({encoding})")
                        print(f"[기존] 컬럼: {list(existing_df.columns[:10])}... (총 {len(existing_df.columns)}개)")  # ⭐ 확인
                        
                        # ⭐ 기존 CSV도 컬럼 이름 매핑
                        if 'instant_discount_amount_YN' in existing_df.columns:
                            existing_df.rename(columns={'instant_discount_amount_YN': 'instant_discount_coupon_YN'}, inplace=True)
                            print(f"[기존] instant_discount_amount_YN → instant_discount_coupon_YN")
                        
                        # 중복 제거 키 확인
                        for key in dedup_cols:
                            if key in existing_df.columns:
                                print(f"[기존] {key}: {existing_df[key].nunique():,}개 고유값")
                        break
                    except (UnicodeDecodeError, LookupError):
                        continue
                    except Exception as e:
                        print(f"[기존]   {encoding}: {type(e).__name__}")
                        continue
                
                if existing_df is None:
                    print(f"[경고] 모든 인코딩 실패 - 기존 파일 무시하고 신규로 시작")
                    
        except Exception as e:
            print(f"[경고] 기존 CSV 읽기 실패: {e}")
            existing_df = None
    
    # ============================================================
    # 4. 데이터 병합
    # ============================================================
    if existing_df is None or existing_df.empty:
        combined_df = new_df.copy()
        print(f"\n[병합] 신규 생성: {len(combined_df):,}행")
    else:
        # ⭐ 기존 데이터 컬럼 순서 확인 및 정렬
        print(f"\n[병합] 기존 데이터 컬럼 정렬 중...")
        
        # 공통 컬럼만 사용
        common_cols = list(set(existing_df.columns) & set(new_df.columns))
        print(f"  - 공통 컬럼: {len(common_cols)}개")
        
        if set(existing_df.columns) != set(new_df.columns):
            print(f"  [경고] 컬럼 불일치 감지")
            print(f"    기존: {len(existing_df.columns)}개")
            print(f"    신규: {len(new_df.columns)}개")
            
            # 누락/추가 컬럼
            old_only = set(existing_df.columns) - set(new_df.columns)
            new_only = set(new_df.columns) - set(existing_df.columns)
            
            if old_only:
                print(f"    기존에만: {old_only}")
            if new_only:
                print(f"    신규에만: {new_only}")
        
        # ⭐ 컬럼 순서를 new_df에 맞춰 정렬
        col_order = [c for c in new_df.columns if c in existing_df.columns]
        existing_df = existing_df[col_order]
        print(f"  ✓ 기존 데이터 컬럼 순서 정렬 완료")
        
        # 병합
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        print(f"\n[병합] 기존 {len(existing_df):,}행 + 신규 {len(new_df):,}행 = {len(combined_df):,}행")

    
    # ============================================================
    # 5. 중복 제거
    # ============================================================
    print(f"\n{'='*60}")
    print(f"[중복 제거] 시작...")
    
    valid_cols = [c for c in dedup_cols if c in combined_df.columns]
    
    if valid_cols:
        before = len(combined_df)
        
        # 1단계: 기본 키 중복 제거 (keep='first' - 기존 데이터 우선)
        combined_df.drop_duplicates(subset=valid_cols, keep='first', inplace=True)
        after_step1 = len(combined_df)
        print(f"[1단계] {valid_cols} 기준: {before - after_step1:,}건 제거 → {after_step1:,}행")
        
        # 2단계: 전체 행 중복 제거
        combined_df.drop_duplicates(inplace=True)
        after_step2 = len(combined_df)
        print(f"[2단계] 전체 행 기준: {after_step1 - after_step2:,}건 추가 제거 → {after_step2:,}행")
        
        total_removed = before - after_step2
        print(f"[중복 제거] 총 제거: {total_removed:,}건")
    else:
        print(f"[경고] 중복 제거 키 없음: {dedup_cols}")
    
    # ============================================================
    # 6. CSV 저장
    # ============================================================
    print(f"\n{'='*60}")
    print(f"[저장] CSV 저장 시작...")
    print(f"[저장] 최종 컬럼 순서: {list(combined_df.columns)}")  # ⭐ 최종 확인
    
    tmp_path = None
    try:
        # 임시 파일 생성
        with tempfile.NamedTemporaryFile(
            mode='w', delete=False, dir=str(local_csv_path.parent),
            prefix='tmp_', suffix='.csv', encoding='utf-8-sig'
        ) as tmp_file:
            tmp_path = tmp_file.name
        
        # CSV 저장
        combined_df.to_csv(tmp_path, index=False, encoding='utf-8-sig')
        
        # 기존 파일 백업
        backup_path = None
        if local_csv_path.exists():
            backup_path = local_csv_path.parent / f"{local_csv_path.name}.bak"
            shutil.copy2(local_csv_path, backup_path)
        
        # 임시 파일을 실제 경로로 이동
        shutil.move(tmp_path, str(local_csv_path))
        
        csv_size = local_csv_path.stat().st_size / (1024 * 1024)
        print(f"[저장] ✅ CSV 저장 완료: {len(combined_df):,}건 ({csv_size:.2f} MB)")
        
        # 백업 파일 삭제
        if backup_path and backup_path.exists():
            backup_path.unlink()
        
        # ============================================================
        # 7. OneDrive 백업
        # ============================================================
        try:
            from modules.transform.utility.paths import ONEDRIVE_DB
            from modules.load.backup_to_onedrive import backup_to_onedrive
            
            onedrive_csv_path = ONEDRIVE_DB / output_subdir / output_filename
            print(f"\n[백업] OneDrive 백업 시작...")
            
            backup_result = backup_to_onedrive(local_csv_path, onedrive_csv_path)
            
            if backup_result['success']:
                print(f"[백업] ✅ OneDrive 백업 완료")
            else:
                print(f"[백업] ⚠️ OneDrive 백업 실패: {backup_result['message']}")
        except Exception as e:
            print(f"[백업] ⚠️ OneDrive 백업 중 예외 발생: {e}")
        
    except Exception as e:
        print(f"[에러] CSV 저장 실패: {e}")
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        return f"저장 실패: {e}"
    
    print(f"{'='*60}\n")
    return f"✅ 저장 완료: {len(combined_df):,}건"




# modules/load/load_df_glob.py 또는 modules/transform/utility/io.py

def cleanup_collected_csvs(
    patterns=None,  # 삭제할 파일 패턴 리스트
    **context
):
    """OneDrive 수집 디렉토리의 CSV 파일 삭제"""
    from pathlib import Path
    from modules.transform.utility.paths import COLLECT_DB
    
    collect_dir = COLLECT_DB / '영업관리부_수집'
    
    if not collect_dir.exists():
        print(f"[정리] 수집 디렉토리 없음: {collect_dir}")
        return "삭제할 파일 없음 (디렉토리 없음)"
    
    # 삭제할 파일 패턴 설정 (기본값: 모든 CSV)
    if patterns is None:
        patterns = [
            'baemin_change_history_*.csv',
            'baemin_metrics_*.csv',
            'toorder_review_*.xlsx'
        ]
    
    total_deleted = 0
    total_size = 0
    failed_files = []
    
    print(f"\n{'='*60}")
    print(f"[정리] OneDrive 수집 폴더 정리 시작")
    print(f"[경로] {collect_dir}")
    print(f"{'='*60}")
    
    for pattern in patterns:
        files = list(collect_dir.glob(pattern))
        
        if not files:
            print(f"[패턴] {pattern}: 파일 없음")
            continue
        
        print(f"\n[패턴] {pattern}: {len(files)}개 파일")
        
        for file_path in files:
            try:
                file_size = file_path.stat().st_size / (1024 * 1024)  # MB
                file_path.unlink()
                total_deleted += 1
                total_size += file_size
                print(f"  ✓ 삭제: {file_path.name} ({file_size:.2f} MB)")
            except Exception as e:
                failed_files.append((file_path.name, str(e)))
                print(f"  ✗ 실패: {file_path.name} - {e}")
    
    print(f"\n{'='*60}")
    print(f"[완료] 삭제 완료: {total_deleted}개 파일 ({total_size:.2f} MB)")
    
    if failed_files:
        print(f"[경고] 실패: {len(failed_files)}개")
        for fname, error in failed_files:
            print(f"  - {fname}: {error}")
    
    print(f"{'='*60}\n")
    
    return f"✅ 정리 완료: {total_deleted}개 파일 삭제"


def upload_final_csv(
    source_filename='sales_daily_orders_upload.csv',
    source_subdir='영업관리부_DB',
    dest_dir='E:/down/업로드_temp',
    **context
):
    """최종 CSV 파일을 업로드 폴더로 복사"""
    import shutil
    from pathlib import Path
    from modules.transform.utility.paths import LOCAL_DB
    
    print(f"\n{'='*60}")
    print(f"[업로드] 최종 파일 복사 시작")
    print(f"{'='*60}")
    
    # 1. 원본 파일 경로
    source_path = LOCAL_DB / source_subdir / source_filename
    
    if not source_path.exists():
        error_msg = f"원본 파일 없음: {source_path}"
        print(f"[에러] {error_msg}")
        return f"❌ 업로드 실패: {error_msg}"
    
    source_size = source_path.stat().st_size / (1024 * 1024)
    print(f"[원본] {source_path}")
    print(f"       크기: {source_size:.2f} MB")
    
    # 2. 대상 디렉토리 생성
    dest_path = Path(dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)
    
    dest_file = dest_path / source_filename
    print(f"\n[대상] {dest_file}")
    
    # 기존 파일 확인
    if dest_file.exists():
        old_size = dest_file.stat().st_size / (1024 * 1024)
        print(f"       기존 파일 존재 ({old_size:.2f} MB) → 덮어쓰기 예정")
    
    # 3. 파일 복사
    try:
        shutil.copy2(source_path, dest_file)
        print(f"\n[복사] ✅ 복사 완료")
        
        # 복사 확인
        if dest_file.exists():
            copied_size = dest_file.stat().st_size / (1024 * 1024)
            print(f"       복사된 파일: {copied_size:.2f} MB")
            
            # 파일 크기 검증
            if abs(source_size - copied_size) < 0.01:  # 10KB 이내 오차
                print(f"       검증: ✓ 파일 크기 일치")
            else:
                print(f"       경고: 파일 크기 불일치 ({source_size:.2f} vs {copied_size:.2f} MB)")
        else:
            raise FileNotFoundError("복사 후 파일이 없습니다")
        
    except Exception as e:
        error_msg = f"파일 복사 실패: {e}"
        print(f"[에러] {error_msg}")
        return f"❌ 업로드 실패: {error_msg}"
    
    print(f"{'='*60}\n")
    return f"✅ 업로드 완료: {dest_file}"





def text_to_html(text):
    """일반 텍스트를 HTML로 변환"""
    text = text.replace('\n', '<br>')
    
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


def send_email(
    subject,
    html_content,
    to_emails,
    conn_id='doridang_conn_smtp_gmail',
    **context
):
    """이메일 발송 (Airflow SMTP Connection 사용)"""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from airflow.hooks.base import BaseHook
    
    connection = BaseHook.get_connection(conn_id)
    
    smtp_host = connection.host
    smtp_port = connection.port
    smtp_user = connection.login
    smtp_password = connection.password
    from_email = connection.extra_dejson.get('from_email') or smtp_user
    
    if isinstance(to_emails, str):
        to_list = [to_emails]
    else:
        to_list = to_emails
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ', '.join(to_list)
    
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)
    
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


def cleanup_temp_parquets(**context):
    """temp 디렉토리의 모든 parquet 파일 삭제 (OneDrive temp + Local temp)"""
    import shutil
    from pathlib import Path
    
    temp_dirs = [TEMP_DIR]
    summary = []
    
    for temp_dir in temp_dirs:
        if not temp_dir.exists():
            print(f"[정리] temp 디렉토리 없음: {temp_dir}")
            continue
        
        parquet_files = list(temp_dir.glob('*.parquet'))
        
        if not parquet_files:
            print(f"[정리] 삭제할 parquet 파일 없음: {temp_dir}")
            continue
        
        total_size = sum(f.stat().st_size for f in parquet_files)
        total_size_mb = total_size / (1024 * 1024)
        
        print(f"[정리] 대상: {temp_dir} -> {len(parquet_files)}개 ({total_size_mb:.2f} MB)")
        
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
        
        result = f"{temp_dir}: 삭제 {deleted_count}개 ({total_size_mb:.2f} MB)"
        if failed_files:
            result += f", 실패 {len(failed_files)}개"
        summary.append(result)
    
    if not summary:
        return "삭제할 파일 없음"
    return " | ".join(summary)


def cleanup_collected_csvs(**context):
    """영업팀_수집 디렉토리의 모든 CSV 파일을 업로드_temp로 이동 (삭제 X)"""
    import shutil
    from pathlib import Path
    
    collect_dir = COLLECT_DB / '영업관리부_수집'
    upload_temp_dir = Path('/opt/airflow/download/업로드_temp/sales_orders')  # Docker 경로
    
    # Windows 경로도 지원
    if not upload_temp_dir.exists():
        upload_temp_dir = Path('C:/airflow/download/업로드_temp/sales_orders')
    
    if not collect_dir.exists():
        print(f"[정리] 수집 디렉토리 없음: {collect_dir}")
        return "이동할 파일 없음 (디렉토리 없음)"
    
    csv_files = list(collect_dir.glob('*.csv'))
    
    if not csv_files:
        print(f"[정리] 이동할 CSV 파일 없음")
        return "이동할 파일 없음"
    
    total_size = sum(f.stat().st_size for f in csv_files)
    total_size_mb = total_size / (1024 * 1024)
    
    print(f"[정리] 이동 대상: {len(csv_files)}개 CSV 파일 ({total_size_mb:.2f} MB)")
    print(f"[정리] 목적지: {upload_temp_dir}")
    
    # 목적지 디렉토리 생성
    upload_temp_dir.mkdir(parents=True, exist_ok=True)
    
    moved_count = 0
    failed_files = []
    
    for csv_file in csv_files:
        try:
            dest_file = upload_temp_dir / csv_file.name
            
            # 동일한 이름의 파일이 있으면 백업
            if dest_file.exists():
                backup_file = upload_temp_dir / f"{csv_file.name}.bak"
                shutil.move(str(dest_file), str(backup_file))
                print(f"[정리] 기존 파일 백업: {backup_file.name}")
            
            shutil.move(str(csv_file), str(dest_file))
            moved_count += 1
            print(f"[정리] 이동 완료: {csv_file.name} → {upload_temp_dir.name}/")
        except Exception as e:
            failed_files.append((csv_file.name, str(e)))
            print(f"[경고] 이동 실패: {csv_file.name} - {e}")
    
    result = f"CSV 이동 완료: {moved_count}개 파일 ({total_size_mb:.2f} MB) → 업로드_temp"
    
    if failed_files:
        result += f", 실패: {len(failed_files)}개"
    
    return result


def csv_to_parquet_backup(
    csv_filename: str,
    onedrive_subfolder: str = "영업관리부_DB",
    **context
) -> str:
    """로컬DB의 CSV 파일을 읽어 Parquet으로 변환 후 OneDrive에 저장"""
    try:
        print("=" * 60)
        print("🔄 CSV → Parquet 변환 및 백업 시작")
        print("=" * 60)
        
        csv_path = LOCAL_DB / csv_filename
        print(f"📂 로컬DB 경로: {csv_path}")
        
        if not csv_path.exists():
            print(f"❌ 파일을 찾을 수 없음: {csv_path}")
            return f"실패: 파일 없음 ({csv_filename})"
        
        encodings = ['utf-8', 'cp949', 'euc-kr', 'latin1']
        df = None
        used_encoding = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_path, encoding=encoding)
                used_encoding = encoding
                print(f"   ✅ {encoding}으로 성공 ({len(df):,}행)")
                break
            except (UnicodeDecodeError, LookupError):
                continue
        
        if df is None:
            return f"실패: CSV 읽기 실패 ({csv_filename})"
        
        parquet_filename = csv_path.stem + ".parquet"
        
        onedrive_path = ONEDRIVE_DB / onedrive_subfolder
        onedrive_path.mkdir(parents=True, exist_ok=True)
        
        parquet_output_path = onedrive_path / parquet_filename
        
        df.to_parquet(
            parquet_output_path,
            index=False,
            engine='pyarrow',
            compression='snappy'
        )
        
        parquet_size = parquet_output_path.stat().st_size / (1024 * 1024)
        csv_size = csv_path.stat().st_size / (1024 * 1024)
        compression_ratio = (1 - parquet_size / csv_size) * 100 if csv_size > 0 else 0
        
        print(f"✅ CSV → Parquet 변환 완료")
        print(f"   - CSV: {csv_size:.2f} MB → Parquet: {parquet_size:.2f} MB ({compression_ratio:.1f}% 압축)")
        
        return f"✅ 완료: {len(df):,}행 ({parquet_filename})"
        
    except Exception as e:
        print(f"❌ 처리 실패: {str(e)}")
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
    주문번호 + 순번 방식으로 sub_order_id 생성
    
    예시: B22V00DE2A_1, B22V00DE2A_2, ...
    """
    df = df.copy()
    
    df[output_col] = (
        df[order_col].astype(str) + '_' + 
        (df.groupby(order_col).cumcount() + 1).astype(str)
    )
    
    print(f"[sub_order_id] 단순 순번 방식으로 생성 완료")
    print(f"  - 원본 데이터: {len(df):,}행")
    print(f"  - 고유 ID: {df[output_col].nunique():,}개")
    
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
    """
    import hashlib
    
    df = df.copy()
    
    key_parts = [df[col].astype(str) for col in natural_key_cols]
    uk_series = pd.concat(key_parts, axis=1).agg('|'.join, axis=1)
    
    df[output_col] = uk_series.apply(
        lambda s: hashlib.sha1(s.encode('utf-8')).hexdigest()[:16]
    )
    
    print(f"[sub_order_id] 해시 방식으로 생성 완료")
    print(f"  - 원본 데이터: {len(df):,}행")
    print(f"  - 고유 ID: {df[output_col].nunique():,}개")
    
    duplicates = df[df.duplicated(subset=output_col, keep=False)]
    if len(duplicates) > 0:
        print(f"  ⚠️ 경고: {len(duplicates)}개 중복 발견!")
    else:
        print(f"  ✅ 중복 없음")
    
    return df