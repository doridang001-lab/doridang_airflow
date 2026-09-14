"""공유 로컬 경로의 프로세스 잠금. 파일명에는 계정 원문을 쓰지 않는다."""
import hashlib
from functools import lru_cache, wraps
from contextlib import contextmanager


@lru_cache(maxsize=512)
def named_lock(key: str, timeout: float = 120):
    from filelock import FileLock
    from modules.transform.utility.paths import LOCAL_DB
    root = LOCAL_DB / "airflow_ops" / "locks"
    root.mkdir(parents=True, exist_ok=True)
    return FileLock(str(root / (hashlib.sha256(key.encode()).hexdigest() + ".lock")), timeout=timeout)


@contextmanager
def locked_partition(key: str):
    with named_lock(key):
        yield


def unified_writer(func):
    @wraps(func)
    def locked(*args, **kwargs):
        with named_lock("unified_write_transaction"):
            return func(*args, **kwargs)
    return locked
