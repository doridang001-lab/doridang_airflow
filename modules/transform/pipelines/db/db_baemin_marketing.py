"""
배민 마케팅 DB 파이프라인

baemin_marketing_*.csv 파일을 수집 → 전처리 → 파티션 CSV로 저장한다.
파티션 구조: brand=X/store=Y/ym=Z/data.csv (overwrite 방식, idempotent)
"""

import re
import shutil
import traceback
from pathlib import Path

import pandas as pd

from modules.transform.utility.paths import COLLECT_DB, DOWN_DIR, TEMP_DIR, ANALYTICS_DB
from modules.transform.utility.io import load_files


# ============================================================
# 내부 유틸리티
# ============================================================

def _extract_brand_from_filename(filename: str) -> str:
    """
    파일명에서 브랜드명을 파싱한다.

    예: baemin_marketing_[음식배달]_닭도리탕_전문_도리당_초월점_14552300_202603.csv
        → "도리당"

    규칙: '_' 분리 후 6자리 이상 숫자(가게 ID) 위치에서 2칸 앞 요소를 브랜드로 사용.
    파싱 실패 시 "unknown" 반환.
    """
    try:
        stem = Path(filename).stem  # 확장자 제거
        parts = stem.split('_')
        for i, part in enumerate(parts):
            if re.match(r'^\d{6,}$', part) and i >= 2:
                brand = parts[i - 2]
                if brand == "곱도리탕":
                    return "나홀로"
                return brand
    except Exception:
        pass
    return "unknown"


def _save_parquet(df: pd.DataFrame, context: dict, prefix: str) -> str:
    """DataFrame을 TEMP_DIR에 Parquet으로 저장하고 경로 문자열을 반환한다."""
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    ds_nodash = context.get('ds_nodash', 'nodate')
    output_path = TEMP_DIR / f"{prefix}_{ds_nodash}.parquet"
    df.to_parquet(output_path, index=False, engine='pyarrow')
    return str(output_path)


# ============================================================
# 태스크 1: 파일 로드
# ============================================================

def load_baemin_marketing_files(**context):
    """
    수집 폴더에서 baemin_marketing_*.csv 파일을 로드하여 Parquet으로 저장하고
    경로를 XCom(baemin_marketing_raw_path)에 push한다.
    """
    return load_files(
        patterns=['baemin_marketing_*.csv'],
        search_paths=[
            DOWN_DIR / "업로드_temp",
            DOWN_DIR,
            COLLECT_DB / "영업관리부_수집",
        ],
        xcom_key='baemin_marketing_raw_path',
        file_type='auto',
        **context
    )


# ============================================================
# 태스크 2: 전처리
# ============================================================

def transform_baemin_marketing(**context):
    """
    XCom(baemin_marketing_raw_path)에서 Parquet 경로를 받아 전처리 후
    XCom(baemin_marketing_transformed_path)에 push한다.

    처리 순서:
    1. collected_at null 행 제거
    2. 조회일자 null 행 제거
    3. datetime 변환 (두 컬럼)
    4. ym 컬럼 생성 (조회일자 기준 YYYY-MM)
    5. store 컬럼 생성 (매장명의 공백 → _)
    6. brand 컬럼 생성 (_source_file 파일명 파싱)
    7. collected_at 기준 정렬 후 (매장명, 조회일자) 중복 제거 (최신 1건 유지)
    """
    ti = context['task_instance']

    parquet_path = ti.xcom_pull(task_ids='load_baemin_marketing', key='baemin_marketing_raw_path')
    if not parquet_path:
        print("[경고] XCom에 raw_path 없음 — 조기 반환")
        ti.xcom_push(key='baemin_marketing_transformed_path', value=None)
        return "0건 (데이터 없음)"

    df = pd.read_parquet(parquet_path)
    print(f"[로드] {len(df):,}행 × {len(df.columns)}컬럼")

    print(f"[컬럼 목록] {list(df.columns)}")

    # 1. collected_at null 제거
    if 'collected_at' in df.columns:
        before = len(df)
        df = df[df['collected_at'].notna()]
        removed = before - len(df)
        if removed:
            print(f"[경고] collected_at null 행 제거: {removed:,}건")
    else:
        print("[경고] 'collected_at' 컬럼이 없습니다.")

    # 2. 날짜 null 제거 (baemin_marketing CSV 컬럼명: '날짜')
    if '날짜' in df.columns:
        before = len(df)
        df = df[df['날짜'].notna()]
        removed = before - len(df)
        if removed:
            print(f"[경고] 날짜 null 행 제거: {removed:,}건")
    else:
        print("[경고] '날짜' 컬럼이 없습니다.")

    if df.empty:
        print("[경고] 유효 데이터 없음")
        ti.xcom_push(key='baemin_marketing_transformed_path', value=None)
        return "0건 (유효 데이터 없음)"

    # 3. datetime 변환
    if 'collected_at' in df.columns:
        df['collected_at'] = pd.to_datetime(df['collected_at'], errors='coerce')
    if '날짜' in df.columns:
        df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')

    # 4. ym 컬럼 (날짜 기준)
    if '날짜' in df.columns:
        df['ym'] = df['날짜'].dt.strftime('%Y-%m')

    # 5. store 컬럼 (store_name 기준, 공백 → _)
    if 'store_name' in df.columns:
        df['store'] = df['store_name'].str.replace(' ', '_', regex=False)
    else:
        print("[경고] 'store_name' 컬럼이 없습니다.")

    # 6. brand 컬럼
    if '_source_file' in df.columns:
        df['brand'] = df['_source_file'].apply(
            lambda f: _extract_brand_from_filename(str(f)) if pd.notna(f) else 'unknown'
        )
    else:
        df['brand'] = 'unknown'
        print("[경고] '_source_file' 컬럼 없음 — brand를 'unknown'으로 설정")

    # 7. 중복 제거 (collected_at 최신 1건 유지, store_id + 날짜 기준)
    dedup_cols = [c for c in ['store_id', '날짜'] if c in df.columns]
    if 'collected_at' in df.columns and dedup_cols:
        before = len(df)
        df = df.sort_values('collected_at')
        df = df.drop_duplicates(subset=dedup_cols, keep='last')
        removed = before - len(df)
        print(f"[중복 제거] {dedup_cols} 기준 {removed:,}건 제거 → {len(df):,}건 유지")

    output_path = _save_parquet(df, context, prefix='baemin_marketing_transformed')
    ti.xcom_push(key='baemin_marketing_transformed_path', value=output_path)
    print(f"[완료] transform_baemin_marketing: {len(df):,}건 → {output_path}")
    return f"{len(df):,}건 처리 완료"


# ============================================================
# 태스크 3: 파티션 CSV 저장
# ============================================================

def save_partitioned_csv(**context):
    """
    XCom(baemin_marketing_transformed_path)에서 Parquet 경로를 받아
    ANALYTICS_DB/baemin_marketing/brand=X/store=Y/ym=Z/data.csv 로 저장한다.

    동일 파티션 디렉터리는 overwrite (shutil.rmtree 후 재생성) 방식으로 처리하여
    idempotent를 보장한다. 파일 쓰기 실패 시 해당 파티션만 skip하고 계속 진행한다.
    """
    ti = context['task_instance']

    parquet_path = ti.xcom_pull(task_ids='transform_baemin_marketing', key='baemin_marketing_transformed_path')
    if not parquet_path:
        print("[경고] XCom에 transformed_path 없음 — 조기 반환")
        return "0건 (데이터 없음)"

    df = pd.read_parquet(parquet_path)
    print(f"[로드] {len(df):,}행 × {len(df.columns)}컬럼")

    # 필수 파티션 컬럼 확인
    required = ['brand', 'store', 'ym']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"파티션 컬럼 누락: {missing}")

    base_path = ANALYTICS_DB / "baemin_marketing"
    saved_count = 0
    skip_count = 0

    for (brand, store, ym), group in df.groupby(['brand', 'store', 'ym']):
        partition_path = base_path / f"brand={brand}" / f"store={store}" / f"ym={ym}"
        try:
            # overwrite: 기존 디렉터리 삭제 후 재생성
            shutil.rmtree(partition_path, ignore_errors=True)
            partition_path.mkdir(parents=True, exist_ok=True)

            csv_path = partition_path / "data.csv"
            group.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"[저장] {csv_path.relative_to(ANALYTICS_DB)} — {len(group):,}건")
            saved_count += len(group)
        except Exception as e:
            print(f"[스킵] {partition_path} 저장 실패 — {e}")
            traceback.print_exc()
            skip_count += 1

    result = f"저장 완료: {saved_count:,}건 / 스킵 파티션: {skip_count}개"
    print(f"[완료] save_partitioned_csv — {result}")
    return result
