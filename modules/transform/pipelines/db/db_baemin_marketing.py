"""
배민 마케팅 DB 파이프라인

baemin_marketing_*.csv 파일을 수집 → 전처리 → 파티션 CSV로 저장한다.
파티션 구조: brand=X/store=Y/ym=Z/baemin_marketing_data.csv (overwrite 방식, idempotent)
실행 이력: ANALYTICS_DB/baemin_marketing/log.parquet
"""

import logging
import re
import shutil
import traceback
from pathlib import Path

import pandas as pd
import pendulum

from modules.transform.utility.paths import COLLECT_DB, DOWN_DIR, TEMP_DIR, ANALYTICS_DB, BAEMIN_MARKETING_DB
from modules.transform.utility.io import load_files

logger = logging.getLogger(__name__)

BAEMIN_MARKETING_DATE_COL_CANDIDATES = [
    "\ub0a0\uc9dc",  # 날짜
    "?좎쭨",  # (깨진 인코딩으로 저장된 경우 fallback)
]


def _first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _normalize_date_col_as_midnight_ampm(df: pd.DataFrame, col: str) -> None:
    """
    날짜 컬럼을 'YYYY-MM-DD 12:00:00 AM' 형태로 정규화한다.
    - 기존에 datetime/문자열이 섞여 있어도 같은 날짜는 동일 값이 되도록 한다.
    """
    if col not in df.columns:
        return
    # pandas가 혼합 포맷을 엄격하게 추론하면 'YYYY-MM-DD'를 NaT로 만들 수 있어 mixed 사용
    dt = pd.to_datetime(df[col], errors="coerce", format="mixed").dt.floor("D")
    # drop_duplicates 키로 쓰기 위해 날짜를 "자정 + AM/PM"으로 고정
    df[col] = dt.dt.strftime("%Y-%m-%d %I:%M:%S %p")


# ============================================================
# 내부 유틸리티
# ============================================================

def _extract_branch(store_name: str) -> str:
    """'[음식배달] 나홀로 1인 곱도리탕 김포장기점' → '김포장기점'

    규칙: 괄호 태그([...]) 제거 후 마지막 토큰(지점명)만 반환.
    """
    name = re.sub(r"\[.*?\]\s*", "", str(store_name)).strip()
    tokens = name.split()
    return tokens[-1] if tokens else name


def _branch_from_store_folder(folder_name: str) -> str:
    """구형 store= 폴더명 → 정규화된 지점명.

    예: '[음식배달]_닭도리탕_전문_도리당_강동점' → '강동점'
    구형 파일은 store_name의 공백을 '_'로 치환한 값이므로,
    '_'를 공백으로 되돌린 뒤 _extract_branch()를 적용한다.
    """
    return _extract_branch(folder_name.replace("_", " "))


def _load_legacy_data(brand_dir: Path, store: str, ym: str) -> pd.DataFrame:
    """brand_dir 하위에서 구형 store 폴더를 찾아 해당 ym 데이터를 로드한다.

    구형 폴더: store= 접두어 뒤 값을 역변환했을 때 store 인자와 일치하는 폴더.
    로드 후 해당 구형 ym 디렉터리는 삭제한다 (마이그레이션).
    """
    frames = []
    if not brand_dir.is_dir():
        return pd.DataFrame()

    for store_dir in brand_dir.iterdir():
        if not store_dir.is_dir() or not store_dir.name.startswith("store="):
            continue
        folder_store_value = store_dir.name[len("store="):]
        # 이미 정규화된 폴더는 건너뜀
        if folder_store_value == store:
            continue
        # 역변환 후 현재 store와 같으면 구형 폴더로 판단
        if _branch_from_store_folder(folder_store_value) != store:
            continue

        ym_dir = store_dir / f"ym={ym}"
        if not ym_dir.is_dir():
            continue

        for csv_file in ym_dir.glob("*.csv"):
            try:
                legacy_df = pd.read_csv(csv_file, encoding="utf-8-sig")
                frames.append(legacy_df)
                logger.info(f"[구형 로드] {csv_file} — {len(legacy_df):,}건")
            except Exception as e:
                logger.warning(f"[구형 로드 실패] {csv_file}: {e}")

        # 마이그레이션 완료 후 구형 ym 디렉터리 삭제
        try:
            shutil.rmtree(ym_dir)
            logger.info(f"[구형 삭제] {ym_dir}")
            # store 폴더가 비었으면 함께 삭제
            if not any(store_dir.iterdir()):
                store_dir.rmdir()
        except Exception as e:
            logger.warning(f"[구형 삭제 실패] {ym_dir}: {e}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


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
    date_col = _first_existing_column(df, BAEMIN_MARKETING_DATE_COL_CANDIDATES)
    if date_col and date_col != "날짜":
        df = df.rename(columns={date_col: "날짜"})
        date_col = "날짜"
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
    if date_col and date_col in df.columns:
        before = len(df)
        df = df[df[date_col].notna()]
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
    if date_col and date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce', format='mixed')

    # 4. ym 컬럼 (날짜 기준)
    if date_col and date_col in df.columns:
        df['ym'] = df[date_col].dt.strftime('%Y-%m')
        _normalize_date_col_as_midnight_ampm(df, date_col)

    # 5. store 컬럼 (지점명만 추출: 괄호 태그 제거 + 마지막 토큰)
    if 'store_name' in df.columns:
        df['store'] = df['store_name'].apply(_extract_branch)
    else:
        logger.warning("'store_name' 컬럼이 없습니다.")

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
    ANALYTICS_DB/baemin_marketing/brand=X/store=Y/ym=Z/baemin_marketing_data.csv 로 저장한다.

    동일 파티션 디렉터리는 overwrite (shutil.rmtree 후 재생성) 방식으로 처리하여
    idempotent를 보장한다. 파일 쓰기 실패 시 해당 파티션만 skip하고 계속 진행한다.
    저장된 파일 목록을 XCom(saved_files)으로 push한다.
    """
    ti = context['task_instance']

    parquet_path = ti.xcom_pull(task_ids='transform_baemin_marketing', key='baemin_marketing_transformed_path')
    if not parquet_path:
        logger.warning("XCom에 transformed_path 없음 — 조기 반환")
        ti.xcom_push(key='saved_files', value=[])
        return "0건 (데이터 없음)"

    df = pd.read_parquet(parquet_path)
    logger.info(f"[로드] {len(df):,}행 × {len(df.columns)}컬럼")

    # 필수 파티션 컬럼 확인
    required = ['brand', 'store', 'ym']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"파티션 컬럼 누락: {missing}")

    base_path = BAEMIN_MARKETING_DB
    saved_count = 0
    skip_count = 0
    saved_files = []

    for (brand, store, ym), group in df.groupby(['brand', 'store', 'ym']):
        partition_path = base_path / f"brand={brand}" / f"store={store}" / f"ym={ym}"
        try:
            # 구형 폴더에서 동일 ym 데이터 로드 후 merge
            brand_dir = base_path / f"brand={brand}"
            legacy_df = _load_legacy_data(brand_dir, store, ym)

            # 신규 파티션에 이미 저장된 데이터가 있으면 읽어서 함께 병합
            existing_csv = partition_path / "baemin_marketing_data.csv"
            if existing_csv.exists():
                try:
                    existing_df = pd.read_csv(existing_csv, encoding="utf-8-sig")
                    group = pd.concat([existing_df, legacy_df, group], ignore_index=True)
                except Exception:
                    group = pd.concat([legacy_df, group], ignore_index=True)
            elif not legacy_df.empty:
                group = pd.concat([legacy_df, group], ignore_index=True)

            # 컬럼 정규화: 날짜/파티션 키(brand/store/ym)
            date_col = _first_existing_column(group, BAEMIN_MARKETING_DATE_COL_CANDIDATES)
            if date_col and date_col != "날짜":
                group = group.rename(columns={date_col: "날짜"})
                date_col = "날짜"
            if date_col == "날짜":
                _normalize_date_col_as_midnight_ampm(group, "날짜")

            # 파일 내 store/ym/brand 값은 파티션 기준으로 통일 (구형 데이터 혼재 방지)
            group["brand"] = brand
            group["store"] = store
            group["ym"] = ym

            # 중복 제거 (store_id + 날짜 기준, collected_at 최신 유지)
            dedup_cols = [c for c in ['store_id', '날짜'] if c in group.columns]
            if dedup_cols and 'collected_at' in group.columns:
                group['collected_at'] = pd.to_datetime(group['collected_at'], errors='coerce')
                group = group.sort_values('collected_at')
                before = len(group)
                group = group.drop_duplicates(subset=dedup_cols, keep='last')
                logger.info(f"[병합 중복제거] {before - len(group):,}건 제거")

            partition_path.mkdir(parents=True, exist_ok=True)
            csv_path = partition_path / "baemin_marketing_data.csv"
            group.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"[저장] {csv_path.relative_to(ANALYTICS_DB)} — {len(group):,}건")
            saved_count += len(group)
            saved_files.append({
                "file_path": str(csv_path),
                "brand": brand,
                "store": store,
                "ym": ym,
                "rows": len(group),
            })
        except Exception as e:
            logger.warning(f"[스킵] {partition_path} 저장 실패 — {e}")
            traceback.print_exc()
            skip_count += 1

    ti.xcom_push(key='saved_files', value=saved_files)
    result = f"저장 완료: {saved_count:,}건 / 스킵 파티션: {skip_count}개"
    logger.info(f"[완료] save_partitioned_csv — {result}")
    return result


# ============================================================
# 태스크 4: 실행 로그 기록
# ============================================================

def write_baemin_log(**context):
    """
    save_partitioned_csv 결과를 BAEMIN_MARKETING_DB/log.parquet에 기록한다.

    컬럼: run_at, brand, store, ym, result, file_path, row_count
    기존 로그가 있으면 append 후 run_at 내림차순 정렬하여 저장한다.
    """
    ti = context['task_instance']
    saved_files = ti.xcom_pull(task_ids='save_partitioned_csv', key='saved_files') or []

    run_at = pendulum.now("Asia/Seoul")
    log_path = BAEMIN_MARKETING_DB / "log.parquet"

    if not saved_files:
        logger.warning("saved_files 없음 — 로그 기록 생략")
        return "0건 (저장 파일 없음)"

    records = [
        {
            "run_at": run_at,
            "brand": f["brand"],
            "store": f["store"],
            "ym": f["ym"],
            "result": "success",
            "file_path": f["file_path"],
            "row_count": f["rows"],
        }
        for f in saved_files
    ]
    new_df = pd.DataFrame(records)

    if log_path.exists():
        existing = pd.read_parquet(log_path)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.sort_values("run_at", ascending=False)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(log_path, index=False, engine="pyarrow")

    logger.info(f"[로그] {log_path} — 누적 {len(combined):,}건 (신규 {len(new_df):,}건)")
    return f"로그 기록 완료: {len(new_df):,}건"
