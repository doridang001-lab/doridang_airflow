"""
전처리 파이프라인
"""

from pathlib import Path
from modules.transform.utility.paths import COLLECT_DB, LOCAL_DB, TEMP_DIR
from modules.transform.utility.io import load_files, preprocess_df, save_to_csv, join_dataframes
import pandas as pd
import datetime as dt
from pathlib import Path
from typing import Iterable, Dict, List, Union
import os


# 삭제 함수선언

def delete_files_by_names(
    file_names: Iterable[str],
    search_roots: Iterable[Union[str, Path]],
    recursive: bool = True,
    dry_run: bool = True,
    case_sensitive: bool = True,
) -> Dict[str, List[str]]:
    """
    주어진 파일명 리스트를 기준으로, 지정된 루트 경로들 아래에서 해당 파일을 찾아 삭제합니다.
    
    Args:
        file_names: 삭제할 '파일명'들의 리스트/배열 (경로 X, 파일명만!)
        search_roots: 탐색을 시작할 루트 경로(여러 개 가능). 각 루트 아래에서 검색.
        recursive: 하위 폴더까지 재귀적으로 검색할지 여부
        dry_run: True면 실제 삭제하지 않고 어떤 파일이 삭제될지 미리 보여줌
        case_sensitive: 파일명 매칭 시 대소문자 구분 여부
        
    Returns:
        dict: {
            "deleted": [삭제된/삭제예정 파일의 전체 경로],
            "not_found": [탐색해도 못 찾은 파일명],
            "errors": ["에러 메시지들"]
        }
    """
    # 정리
    targets = list(dict.fromkeys([str(x).strip() for x in file_names if str(x).strip()]))
    roots = [Path(r) for r in search_roots]
    
    # 검사
    for r in roots:
        if not r.exists():
            raise FileNotFoundError(f"[검색 루트 없음] {r}")
        if not r.is_dir():
            raise NotADirectoryError(f"[디렉터리 아님] {r}")

    # 대소문자 옵션 처리
    if case_sensitive:
        name_set = set(targets)
        norm = lambda s: s
    else:
        name_set = set([t.lower() for t in targets])
        norm = lambda s: s.lower()

    deleted_paths: List[str] = []
    not_found: List[str] = []
    errors: List[str] = []

    # 빠르게 lookup할 수 있도록 파일명 → 후보 경로 목록 맵을 만들자
    # (매 루프마다 전체 파일을 탐색하지 않고, 한 번 순회에서 맵을 만듦)
    candidate_map = {t: [] for t in targets}

    def walk_files(root: Path):
        if recursive:
            # 재귀적으로 순회
            for dirpath, dirnames, filenames in os.walk(root):
                d = Path(dirpath)
                for fn in filenames:
                    # 매칭 검사
                    key = norm(fn)
                    if key in name_set:
                        fullp = str(d / fn)
                        # 원본 타겟명으로 append (대소문자 옵션에 맞추어 역매핑)
                        # 여러 타겟이 같은 케이스로 매칭될 수 있어 모두 체크
                        for original in targets:
                            if norm(original) == key:
                                candidate_map[original].append(fullp)
        else:
            # 1-depth만
            for p in root.iterdir():
                if p.is_file():
                    key = norm(p.name)
                    if key in name_set:
                        for original in targets:
                            if norm(original) == key:
                                candidate_map[original].append(str(p))

    # 모든 루트 탐색
    for r in roots:
        try:
            walk_files(r)
        except Exception as e:
            errors.append(f"[탐색오류] root={r} error={e}")

    # 삭제(or dry-run) 수행
    for fname, paths in candidate_map.items():
        if not paths:
            not_found.append(fname)
            continue
        for p in paths:
            try:
                if dry_run:
                    # 미리보기만
                    deleted_paths.append(p)
                else:
                    Path(p).unlink(missing_ok=False)
                    deleted_paths.append(p)
            except Exception as e:
                errors.append(f"[삭제오류] path={p} error={e}")

    return {
        "deleted": deleted_paths,
        "not_found": not_found,
        "errors": errors
    }




# 데이터 로드
# from modules.transform.utility.io import load_files, preprocess_df, save_to_csv, join_dataframes
def load_sales_orders_daily_csv(**context):
    """1. 일별 판매 주문서 로드"""
    return load_files(
        patterns=['sales_daily_orders.csv'], # 토더 리뷰 파일 패턴
        search_paths=[
            LOCAL_DB / "영업관리부_DB", # 일반 다운로드 폴더
        ],
        xcom_key='sales_orders_daily_path', # XCom 키
        file_type='auto',  # CSV 우선, 없으면 Excel
        **context
    )
    

# 잘못된 데이터 삭제
def delete_df(problem_df):
    # 전처리 시작

    # 1) 파일명 목록 준비 (질문에서 주신 배열)
    file_names = problem_df["file_name"].unique().tolist()

    # 2) 탐색 루트 경로 지정
    #    - 예: COLLECT_DB / "영업관리부_수집"
    #    - 예: LOCAL_DB / "temp"
    #    - 문자열 경로도 가능
    search_roots = [
        COLLECT_DB / "영업관리부_수집",
        # 추가로 필요하면 여기에 더 넣으세요
    ]

    # 3) 우선 dry-run으로 검증
    report = delete_files_by_names(
        file_names=file_names,
        search_roots=search_roots,
        recursive=True,
        dry_run=True,         # 실제 삭제 X, 경로만 보고
        case_sensitive=True   # 파일명 정확히 일치할 때만
    )

    print("=== DRY RUN 결과 ===")
    print("삭제예정:", *report["deleted"], sep="\n- ")
    print("미발견:", *report["not_found"], sep="\n- ")
    print("에러:", *report["errors"], sep="\n- ")

    # 4) 실제 삭제 실행 (검증 후에만!)
    report2 = delete_files_by_names(
        file_names=file_names,
        search_roots=search_roots,
        recursive=True,
        dry_run=False,        # 실제 삭제
        case_sensitive=True
    )
    print("=== 실제 삭제 결과 ===")
    print("삭제됨:", *report2["deleted"], sep="\n- ")
    print("미발견:", *report2["not_found"], sep="\n- ")
    print("에러:", *report2["errors"], sep="\n- ")

    return problem_df

def delete_invalid_sales_orders_daily_csv(**context):
    return preprocess_df(
        input_xcom_key='sales_orders_daily_path',
        output_xcom_key='sales_orders_daily_cleaned_path',
        transform_func=delete_df,
        **context
    )



# 오류건 제거 전처리
def transform_sales_orders_daily_csv(df):
    # 전처리 시작
    df = df.copy()
    
     # 결제금액이 null인데 총금액이 있는 경우 주문ID 기준으로 삭제
    pro_df = df[~df["total_amount"].isnull()]
    pro_df = pro_df[pro_df["settlement_amount"].isnull()]
    null_col = pro_df["order_id"].unique()
    df = df[~df["order_id"].isin(null_col)]
    return df

def clear_sales_orders_daily_csv(**context):
    """ 처리"""

    return io3_preprocess_df(
        input_xcom_key='sales_orders_daily_path',
        output_xcom_key='sales_orders_daily_processed_path',
        transform_func=transform_sales_orders_daily_csv,  # 위에서 정의한 함수
        #natural_keys=['date', 'stores_name'],  # ID 생성용
        **context
    )
    
# 데이터 덮어쓰기
def fin_save_to_csv(
    input_task_id,
    input_xcom_key,
    output_csv_path=None,
    output_filename='sales_daily_orders.csv',
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