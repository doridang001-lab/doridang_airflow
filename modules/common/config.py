# modules/common/config.py
"""
DB 설정 및 공통 설정 관리

로컬 노트북 실행 시: localhost
Airflow Docker 실행 시: host.docker.internal
"""
import os
from pathlib import Path
from urllib.parse import quote_plus

from modules.transform.utility.mail_recipients import MAIL_CMJ_PM

# 환경 변수로 Docker 여부 판단 (Airflow에서 설정)
def _detect_docker() -> bool:
    env = os.getenv("IS_DOCKER")
    if env is not None:
        return env.lower() == "true"
    # Best-effort auto-detect when running inside a Linux container
    return Path("/.dockerenv").exists()


IS_DOCKER = _detect_docker()

# DB 설정 (docker-compose에서 postgres 포트 5433 노출)
DB_CONFIG = {
    "user": os.getenv("DORIDANG_DB_USER", "doridang"),
    "password": os.getenv("DORIDANG_DB_PASSWORD", "doridang"),
    "host": os.getenv("DORIDANG_DB_HOST", "doridang-postgres" if IS_DOCKER else "127.0.0.1"),
    "port": os.getenv("DORIDANG_DB_PORT", "5432" if IS_DOCKER else "5434"),
    "database": os.getenv("DORIDANG_DB_NAME", "doridangdb"),
}


def build_postgres_url(db_config: dict) -> str:
    user = quote_plus(str(db_config["user"]))
    password = quote_plus(str(db_config["password"]))
    host = str(db_config["host"])
    port = str(db_config["port"])
    database = quote_plus(str(db_config["database"]))
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def get_db_url():
    """Airflow용 DB 연결 문자열 (Docker 컨테이너 내부에서 실행)"""
    return build_postgres_url(DB_CONFIG)


def nb_get_db_url():
    """Jupyter Notebook용 DB 연결 문자열 (로컬 실행)"""
    cfg = dict(DB_CONFIG)
    cfg["host"] = "127.0.0.1"
    cfg["port"] = os.getenv("DORIDANG_DB_PORT_LOCAL", "5434")
    return build_postgres_url(cfg)


# 채널 코드
CHANNELS = {
    "baemin": "BM",
    "coupang": "CP",
    "yogiyo": "YG",
    "pos": "POS",
}

# 지점 목록 (실제 지점으로 수정 필요)
STORES = [
    {"store_id": "14778331", "store_name": "[음식배달] 나홀로 1인 곱도리탕 가락점"},
    {"store_id": "14535911", "store_name": "[음식배달] 닭도리탕 전문 도리당 가락점"},
]

# 관리자 이메일
ADMIN_EMAIL = [MAIL_CMJ_PM]

# 구글 인증 경로
GSHEET_CRED_PATH = os.path.join(
    os.path.dirname(__file__), "../../config/glowing-palace-465904-h6-7f82df929812.json"
)
