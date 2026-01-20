# modules/common/config.py
"""
DB 설정 및 공통 설정 관리

로컬 노트북 실행 시: localhost
Airflow Docker 실행 시: host.docker.internal
"""
import os

# 환경 변수로 Docker 여부 판단 (Airflow에서 설정)
IS_DOCKER = os.getenv("IS_DOCKER", "false").lower() == "true"

# DB 설정 (docker-compose에서 postgres 포트 5433 노출)
DB_CONFIG = {
    "user": "doridang",
    "password": "doridang",
    "host": "host.docker.internal" if IS_DOCKER else "127.0.0.1",
    "port": "5434",  # separate business Postgres mapped to 5434
    "database": "doridangdb",
}


def get_db_url():
    """Airflow용 DB 연결 문자열 (Docker 컨테이너 내부에서 실행)"""
    return "postgresql://doridang:doridang@host.docker.internal:5434/doridangdb"


def nb_get_db_url():
    """Jupyter Notebook용 DB 연결 문자열 (로컬 실행)"""
    return "postgresql://doridang:doridang@127.0.0.1:5434/doridangdb"


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

# 구글 인증 경로
GSHEET_CRED_PATH = os.path.join(
    os.path.dirname(__file__), "../../config/glowing-palace-465904-h6-7f82df929812.json"
)
