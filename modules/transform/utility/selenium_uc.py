import logging
import os
import tempfile
from pathlib import Path

import undetected_chromedriver as uc

logger = logging.getLogger(__name__)


def configure_uc_data_path(data_path: str | None = None) -> str | None:
    """
    Airflow(컨테이너) 환경에서 undetected_chromedriver가 ~/.local/share 아래에
    드라이버를 생성/패치하려다 권한/HOME 문제로 실패하는 케이스를 방지한다.

    - AIRFLOW_HOME 이 있는 경우에만 적용(로컬 영향 최소화)
    - 기본값은 OS temp (/tmp 등)
    - uc.patcher.Patcher.data_path 를 런타임에 덮어써서 다운로드/패치 경로를 고정
    """
    if os.getenv("AIRFLOW_HOME") is None:
        return None

    desired = (
        data_path
        or os.getenv("UC_DATA_DIR")
        or os.path.join(tempfile.gettempdir(), "undetected_chromedriver")
    )

    try:
        Path(desired).mkdir(parents=True, exist_ok=True)
    except Exception as err:
        logger.warning("UC data dir 생성 실패(무시): %s (path=%s)", err, desired)
        return None

    abs_path = os.path.abspath(os.path.expanduser(desired))
    try:
        uc.patcher.Patcher.data_path = abs_path
        logger.info("UC data dir 고정: %s", abs_path)
    except Exception as err:
        logger.warning("UC data dir 설정 실패(무시): %s (path=%s)", err, abs_path)
        return None

    return abs_path

