"""
쿠팡이츠 CMG 데이터 파티션 저장 및 OneDrive 백업 파이프라인

처리 절차:
1. E:/down/업로드_temp/coupangeats_cmg_*.csv 전체 수집
2. collected_at 기준 내림차순 정렬 → (매장명, 조회일자) 중복 제거 (keep='first')
3. 조회일자에서 ym(YYYY-MM) 추출
4. brand=도리당/store={매장명}/ym={ym}/ 파티션 구조로 저장
5. 각 파일을 OneDrive에 백업

반환:
{
    "dataset_name": "coupang_marketing",
    "total_rows": int,
    "total_files": int,
    "saved_partitions": [
        {
            "path": str,
            "rows": int,
            "brand": str,
            "store": str,
            "ym": str,
            "onedrive_result": dict
        }
    ],
    "summary": {
        "partitions_created": int,
        "total_rows_saved": int,
        "onedrive_uploaded": int,
        "status": str
    }
}
"""

import logging
from pathlib import Path
from glob import glob
from typing import Dict, List, Any
import pandas as pd
import json

from modules.transform.utility.paths import ANALYTICS_DB
from modules.transform.utility.onedrive import backup_to_onedrive

logger = logging.getLogger(__name__)


def load_coupangeats_cmg_partition() -> str:
    """
    쿠팡이츠 CMG CSV 파일들을 수집, 전처리, 파티션 저장 및 OneDrive 백업
    """
    try:
        result = _process_coupangeats_cmg()
        logger.info(f"파이프라인 완료: {json.dumps(result, ensure_ascii=False, indent=2)}")
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        logger.error(f"파이프라인 실패: {str(e)}", exc_info=True)
        raise


def _process_coupangeats_cmg() -> Dict[str, Any]:
    """내부 처리 로직"""

    # 설정
    source_dir = Path("E:/down/업로드_temp")
    file_pattern = "coupangeats_cmg_*.csv"
    dataset_name = "coupang_marketing"
    brand = "도리당"
    store_col = "매장명"
    date_col = "조회일자"
    timestamp_col = "collected_at"
    dedup_cols = ["매장명", "조회일자"]

    base_path = ANALYTICS_DB / dataset_name

    logger.info(f"쿠팡이츠 CMG 파티션 처리 시작")
    logger.info(f"  소스: {source_dir}/{file_pattern}")
    logger.info(f"  목표: {base_path}")

    # 1. 파일 수집
    files = sorted(glob(str(source_dir / file_pattern)))
    logger.info(f"발견된 파일 수: {len(files)}")

    if not files:
        raise FileNotFoundError(f"{source_dir}/{file_pattern} 파일을 찾을 수 없습니다")

    # 2. DataFrame 병합
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f, encoding="utf-8-sig")
            df["_source_file"] = Path(f).name
            dfs.append(df)
            logger.info(f"  로드: {Path(f).name} ({len(df)}행)")
        except Exception as e:
            logger.warning(f"  건너뜀: {Path(f).name} - {str(e)}")

    if not dfs:
        raise ValueError("유효한 CSV 파일이 없습니다")

    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"병합 완료: {len(df)}행")

    # 3. 전처리
    # null 제거
    initial_rows = len(df)
    df = df.dropna(subset=[timestamp_col, date_col])
    logger.info(f"null 제거: {initial_rows} → {len(df)}행 ({initial_rows - len(df)}행 삭제)")

    # datetime 변환
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # ym 파티션 컬럼 생성
    df["ym"] = df[date_col].dt.strftime("%Y-%m")

    # store 파티션 (공백 제거)
    df["store"] = df[store_col].str.replace(" ", "_", regex=False)

    # brand 파티션 (고정값)
    df["brand"] = brand

    logger.info(f"파티션 컬럼 생성 완료")

    # 4. 중복 제거
    initial_rows = len(df)
    df = df.sort_values(timestamp_col, ascending=False)
    df = df.drop_duplicates(subset=dedup_cols, keep='first')
    logger.info(f"중복 제거: {initial_rows} → {len(df)}행 ({initial_rows - len(df)}행 제거)")

    # 5. 파티션 저장 및 OneDrive 백업
    saved_partitions = []

    for (part_brand, part_store, part_ym), grp in df.groupby(["brand", "store", "ym"]):
        part_path = base_path / f"brand={part_brand}" / f"store={part_store}" / f"ym={part_ym}"
        part_path.mkdir(parents=True, exist_ok=True)

        # 기존 파일이 있으면 로드하여 병합
        csv_file = part_path / "data.csv"
        if csv_file.exists():
            existing_df = pd.read_csv(csv_file, encoding="utf-8-sig")
            grp_merged = pd.concat([existing_df, grp], ignore_index=True)
            grp_merged = grp_merged.sort_values(timestamp_col, ascending=False)
            grp_merged = grp_merged.drop_duplicates(subset=dedup_cols, keep='first')
            logger.info(f"  기존 병합: {part_brand}/{part_store}/{part_ym} ({len(existing_df)} + {len(grp)} → {len(grp_merged)}행)")
            grp = grp_merged
        else:
            logger.info(f"  신규 저장: {part_brand}/{part_store}/{part_ym} ({len(grp)}행)")

        grp.to_csv(csv_file, index=False, encoding="utf-8-sig")

        partition_info = {
            "path": str(csv_file),
            "rows": len(grp),
            "brand": part_brand,
            "store": part_store,
            "ym": part_ym,
            "onedrive_result": None
        }

        # OneDrive 백업
        try:
            onedrive_path = f"{dataset_name}/brand={part_brand}/store={part_store}/ym={part_ym}/data.csv"
            result = backup_to_onedrive(
                local_file_path=str(csv_file),
                onedrive_file_path=onedrive_path,
                max_retries=3,
                retry_delay=2.0
            )
            partition_info["onedrive_result"] = result
            logger.info(f"  OneDrive 백업: {onedrive_path} - {result.get('message', 'OK')}")
        except Exception as e:
            logger.warning(f"  OneDrive 백업 실패: {str(e)}")
            partition_info["onedrive_result"] = {"success": False, "message": str(e)}

        saved_partitions.append(partition_info)

    # 결과 집계
    total_rows_saved = sum(p["rows"] for p in saved_partitions)
    onedrive_uploaded = sum(1 for p in saved_partitions if p["onedrive_result"] and p["onedrive_result"].get("success"))

    result = {
        "dataset_name": dataset_name,
        "total_rows": len(df),
        "total_files": len(files),
        "saved_partitions": saved_partitions,
        "summary": {
            "partitions_created": len(saved_partitions),
            "total_rows_saved": total_rows_saved,
            "onedrive_uploaded": onedrive_uploaded,
            "status": "SUCCESS" if onedrive_uploaded == len(saved_partitions) else "PARTIAL"
        }
    }

    logger.info(f"최종 결과: {result['summary']}")
    return result
