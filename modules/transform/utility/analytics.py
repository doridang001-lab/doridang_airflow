import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def read_analytics_partition(
    root: Path,
    brand: str | None = None,
    store: str | None = None,
    ym: str | None = None,
) -> pd.DataFrame:
    """
    Hive-style 파티션(brand=/store=/ym=/data.csv)을 glob으로 수집해 DataFrame 반환.
    brand/store/ym 생략 시 해당 레벨 전체 포함.
    경로에서 brand=, store=, ym= 값을 파싱해 컬럼으로 자동 추가.

    Args:
        root: 파티션 루트 경로 (예: BAEMIN_MARKETING_DB)
        brand: brand= 파티션 값. None이면 전체
        store: store= 파티션 값. None이면 전체
        ym: ym= 파티션 값 (예: "2026-03"). None이면 전체

    Returns:
        brand, store, ym 컬럼이 포함된 DataFrame

    Raises:
        FileNotFoundError: 매칭 파일이 없을 때
    """
    brand_pat = f"brand={brand}" if brand else "brand=*"
    store_pat = f"store={store}" if store else "store=*"
    ym_pat = f"ym={ym}" if ym else "ym=*"
    pattern = f"{brand_pat}/{store_pat}/{ym_pat}/data.csv"

    files = sorted(root.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"파티션 파일 없음: {root / pattern}"
        )

    logger.info("파티션 파일 %d개 로드: %s", len(files), root / pattern)
    frames = []
    for f in files:
        df = pd.read_csv(f)
        df = _attach_partition_cols(df, f)
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


def _attach_partition_cols(df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
    """경로에서 brand=, store=, ym= 값을 추출해 컬럼으로 추가."""
    parts = file_path.parts
    for part in parts:
        if part.startswith("brand="):
            df["brand"] = part[len("brand="):]
        elif part.startswith("store="):
            df["store"] = part[len("store="):]
        elif part.startswith("ym="):
            df["ym"] = part[len("ym="):]
    return df
