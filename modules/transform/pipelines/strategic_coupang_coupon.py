# modules/transform/pipelines/strategic_coupang_coupon.py
"""
쿠팡이츠 쿠폰 데이터 로드 및 전처리 함수
"""
import pandas as pd
import numpy as np
from pathlib import Path
import re

from modules.load.load_df_glob import load_data
from modules.transform.utility.paths import TEMP_DIR, LOCAL_DB, COLLECT_DB

# ============================================================
# 경로 설정
# ============================================================
PATH_COUPON = COLLECT_DB / "전략기획팀_수집" / "coupangeats_coupon_*.csv"
PATH_FIN_COUPON = COLLECT_DB / "전략기획팀_수집" / "전달용"
PATH_BACKUP = "/opt/airflow/download/업로드_temp"

def extract_coupon_info(file_name: str) -> dict:
    """파일명에서 매장명(예: '도리당 가락점'), store_id만 추출.

    예시 파일명:
    coupangeats_coupon_닭도리탕_전문_도리당_가락점_2877236_20260116 (1).csv
    → stores_name/store_names: '도리당 가락점'
      store_id: '2877236'
    
    ⚠️ '날짜' 컬럼은 CSV 원본 데이터 유지를 위해 추출하지 않음
    """
    base = Path(file_name).stem
    tokens = re.split(r"[_\s]+", base)
    tokens = [t for t in tokens if t]

    # 공통 접두어 제거
    tokens = [t for t in tokens if t.lower() not in ("coupangeats", "coupon", "coupangeats_coupon")]

    # 한글 매장명 후보: 숫자/괄호가 아닌 토큰들 중 마지막 2개를 사용
    non_numeric = [t for t in tokens if not t.isdigit() and not re.fullmatch(r"\(\d+\)", t)]
    store_tokens = non_numeric[-2:] if len(non_numeric) >= 2 else non_numeric
    store_name = " ".join(store_tokens).strip()

    info = {"stores_name": store_name, "store_names": store_name}

    # store_id 추정: 숫자 중 날짜(8자리) 제외 마지막 숫자
    store_id_candidates = [t for t in tokens if t.isdigit() and len(t) != 8]
    if store_id_candidates:
        info["store_id"] = store_id_candidates[-1]

    return info

# ============================================================
# 범용 재업로드 로더 (핵심 함수)
# ============================================================
def load_reupload_generic(
    file_pattern: str,
    xcom_key: str,
    search_paths: list,
    fallback_func: callable,
    dedup_key: list = None,
    source_info_extractor=None,
    **context
):
    """
    재사용 가능한 스마트 로더
    
    Args:
        file_pattern: 파일 패턴 (예: 'coupangeats_coupon_*.csv')
        xcom_key: XCom 저장 키
        search_paths: 검색할 경로 리스트
        fallback_func: 파일 없을 때 호출할 함수
        dedup_key: 중복 제거 키 (원본 컬럼 기준)
    """
    all_files = []
    
    # 모든 경로에서 파일 찾기
    for path_str in search_paths:
        search_path = Path(path_str)
        if search_path.exists():
            found_files = list(search_path.glob(file_pattern))
            if found_files:
                print(f"[{Path(path_str).name}] {len(found_files)}개 파일 발견")
                all_files.extend(found_files)
    
    if all_files:
        print(f"[✅ 재사용] 총 {len(all_files)}개 파일 발견")
        
        # 🎯 중복 파일 제거 (파일명 기준, 최신 우선)
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
            xcom_key=xcom_key,
            use_glob=False,
            dedup_key=dedup_key,
            add_source_info=source_info_extractor is not None,
            source_info_extractor=source_info_extractor,
            **context
        )
    else:
        print(f"[🔄 새로 로드] 모든 경로에서 파일 없음 → fallback 함수 호출")
        return fallback_func(**context)


# ============================================================
# 쿠팡이츠 쿠폰 데이터 로더
# ============================================================
def load_reupload_coupang_coupon(**context):
    """쿠팡이츠 쿠폰 데이터 스마트 로더"""
    return load_reupload_generic(
        file_pattern='coupangeats_coupon_*.csv',
        xcom_key='coupang_coupon_path',
        search_paths=[
            PATH_BACKUP,
            str(COLLECT_DB / "전략기획팀_수집"),
            str(COLLECT_DB / "전략기획부_수집")
        ],
        fallback_func=load_coupang_coupon_df,
        dedup_key=None,  # ✅ 중복제거 해제: 모든 원본 행 유지
        source_info_extractor=extract_coupon_info,
        **context
    )


def load_coupang_coupon_df(**context):
    """쿠팡이츠 쿠폰 원본 로드 (fallback용)"""
    return load_data(
        file_path=PATH_COUPON,
        xcom_key='coupang_coupon_path',
        use_glob=True,
        dedup_key=["store_id", "날짜"],
        add_source_info=True, # source_info_extractor=extract_coupon_info,
        **context
    )


# ============================================================
# 전처리 함수 (필요시 추가)
# ============================================================
def preprocess_coupang_coupon_df(**context):
    """쿠팡이츠 쿠폰 데이터 전처리"""
    ti = context['task_instance']
    parquet_path = ti.xcom_pull(task_ids='load_coupang_coupon', key='coupang_coupon_path')
    
    if not parquet_path:
        ti.xcom_push(key='processed_coupang_coupon_path', value=None)
        return "0건 (입력 데이터 없음)"
    
    df = pd.read_parquet(parquet_path)
    
    print(f"[로드] 원본 컬럼: {list(df.columns)}")
    print(f"[로드] 원본 데이터: {len(df):,}행")
    
    # ⭐ 날짜를 문자열로 고정 (형식 변환 없이 원본 유지)
    if '날짜' in df.columns:
        df['날짜'] = df['날짜'].astype(str)
        print(f"[날짜 고정] 문자열 타입 유지 (예: {df['날짜'].iloc[0] if len(df) > 0 else 'N/A'})")
    
    # ✅ 필요한 모든 컬럼 선택 (있는 것만)
    desired_cols = [
        '매장명', 'stores_name', 'store_names',  # 매장명 (어떤 이름이든)
        '날짜',
        '쿠폰적용_주문매출',
        '쿠폰발행_비용',
        '평균주문금액',
        '조회수대비_주문율',
        'store_id'
    ]
    
    available_cols = [col for col in desired_cols if col in df.columns]
    
    # 매장명 컬럼 통일 (stores_name or store_names -> 매장명)
    if '매장명' not in df.columns:
        if 'stores_name' in df.columns:
            df['매장명'] = df['stores_name']
        elif 'store_names' in df.columns:
            df['매장명'] = df['store_names']
    
    # 최종 컬럼 선택
    final_cols = ['매장명', '날짜']
    for col in ['쿠폰적용_주문매출', '쿠폰발행_비용', '평균주문금액', '조회수대비_주문율']:
        if col in df.columns:
            final_cols.append(col)
    
    df = df[final_cols]
    print(f"[컬럼 선택] {len(final_cols)}개: {final_cols}")
    
    output_path = TEMP_DIR / f"processed_coupang_coupon_{context['ds_nodash']}.parquet"
    TEMP_DIR.mkdir(exist_ok=True, parents=True)
    df.to_parquet(output_path, index=False)
    
    ti.xcom_push(key='processed_coupang_coupon_path', value=str(output_path))
    return f"전처리: {len(df):,}행"


def preprocess_col_coupang_coupon_df(
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
    df = df[['store_names', '날짜', '쿠폰적용_주문매출', '쿠폰발행_비용', '평균주문금액',
       '조회수대비_주문율']]
    df = df.rename(columns={
        'store_names': '매장명',})
    
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
import datetime as dt
time = dt.datetime.now().strftime('%Y-%m-%d')


def coupon_fin_save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename=f'coupang_coupon_merge_{time}.csv',
    output_subdir=PATH_FIN_COUPON,
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




def move_original_coupang_files(**context):
    """수집했던 원본 CSV 파일을 업로드_temp로 이동"""
    import shutil
    from pathlib import Path
    
    # 수집 디렉토리에서 원본 파일 찾기
    source_dir = COLLECT_DB / "전략기획팀_수집"
    dest_dir = Path("/opt/airflow/download/업로드_temp/strategic_coupangeats_coupon")
    
    if not source_dir.exists():
        print(f"[경고] 소스 디렉토리 없음: {source_dir}")
        return "0건 (디렉토리 없음)"
    
    # 파일 찾기
    files = list(source_dir.glob("coupangeats_coupon_*.csv"))
    print(f"[찾음] {len(files)}개 파일: {[f.name for f in files]}")
    
    # 목적지 생성
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    # 파일 이동
    moved_count = 0
    for f in files:
        try:
            dest_file = dest_dir / f.name
            shutil.move(str(f), str(dest_file))
            print(f"[✅ 이동] {f.name}")
            moved_count += 1
        except Exception as e:
            print(f"[❌ 실패] {f.name}: {e}")
    
    return f"{moved_count}개 파일 이동 완료"


