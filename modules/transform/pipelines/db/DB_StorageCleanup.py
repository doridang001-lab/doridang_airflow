"""Prune Airflow logs, temp files, and stale Chrome profiles to keep the host disk bounded.

컨테이너 안 Chrome이 죽으면 WSL2가 프로세스 주소공간 전체를 Windows 쪽에 덤프해
C: 드라이브를 고갈시킨 사건이 있었다(2026-07-30, 덤프 1개 47GB). 덤프 자체는
`.wslconfig`의 `crashDumpEnabled=false`로 막았고, 이 모듈은 그와 별개로 계속 누적되던
로그/임시파일/Chrome 프로필을 주기적으로 회수한다.
"""

import logging
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from modules.transform.utility.paths import DOWN_DIR, TEMP_DIR

logger = logging.getLogger(__name__)

# 보존 기간(일). 수집 배치가 하루 3회(00:15/06:15/09:15) 돌기 때문에
# 재시도·수동 재수집 여지를 남기려면 tmp/다운로드는 최소 일주일을 남긴다.
AIRFLOW_LOG_RETENTION_DAYS = 30
SYSTEM_TMP_RETENTION_DAYS = 7
LOCAL_TEMP_RETENTION_DAYS = 7
DOWNLOAD_RETENTION_DAYS = 14
CHROME_PROFILE_RETENTION_DAYS = 30

# 다운로드 폴더에서 회수 대상으로 삼을 확장자. 수집 산출물만 지우고
# 정체를 모르는 파일은 남긴다.
DOWNLOAD_SUFFIXES = {".csv", ".xlsx", ".xls", ".zip", ".crdownload", ".tmp", ".part"}

# 삭제하면 수집이 깨지는 경로. 프로필 루트 자체는 절대 지우지 않는다.
PROTECTED_NAMES = {"chrome_profiles"}

AIRFLOW_LOG_DIR = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "logs"
CHROME_PROFILE_DIR = DOWN_DIR / "chrome_profiles"


@dataclass
class PruneResult:
    """한 대상에 대한 정리 결과."""

    target: str
    items: int = 0
    bytes_freed: int = 0
    errors: int = 0

    @property
    def gb_freed(self) -> float:
        return self.bytes_freed / (1024**3)

    def describe(self, dry_run: bool) -> str:
        verb = "삭제 예정" if dry_run else "삭제"
        suffix = f", 오류 {self.errors}건" if self.errors else ""
        return f"{self.target}: {self.items}건 {verb} ({self.gb_freed:.2f} GB){suffix}"


def _cutoff(days: int) -> float:
    return time.time() - days * 86400


def _walk(root: Path, exclude: frozenset[str] = frozenset()):
    """`root` 아래 파일을 (경로, stat) 쌍으로 훑는다.

    대상 경로가 Windows 바인드 마운트(E:\\down 등)라 syscall 하나하나가 비싸다.
    `Path.rglob` + `is_file()` + `stat()`는 파일당 stat을 2~3번 호출하는데,
    `os.scandir`는 d_type으로 디렉터리 판별을 공짜로 해주므로 stat 1번으로 끝난다.
    `exclude`에 든 디렉터리는 진입 자체를 하지 않는다.
    """
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            if entry.path not in exclude:
                                stack.append(Path(entry.path))
                            continue
                        yield Path(entry.path), entry.stat(follow_symlinks=False)
                    except OSError:
                        continue
        except OSError:
            continue


def _profile_usage(root: Path) -> tuple[float, int]:
    """프로필 트리의 (최종 사용 시각, 총 바이트)를 한 번의 순회로 구한다.

    Chrome 프로필은 디렉터리 자체의 mtime이 갱신되지 않는 경우가 있어
    내부 파일까지 훑어야 실제 사용 여부를 알 수 있다. 활성 계정 프로필을
    지우면 재로그인이 발생하고 배민 봇탐지를 다시 부르기 때문에 중요하다.
    """
    try:
        newest = root.stat().st_mtime
    except OSError:
        return 0.0, 0

    total = 0
    for _, stat in _walk(root):
        newest = max(newest, stat.st_mtime)
        total += stat.st_size
    return newest, total


def _prune_files(
    root: Path,
    days: int,
    target: str,
    suffixes: set[str] | None = None,
    exclude: tuple[Path, ...] = (),
    dry_run: bool = False,
) -> PruneResult:
    """`root` 아래에서 `days`보다 오래된 파일을 지우고 빈 디렉터리를 정리한다.

    `exclude`에 준 하위 트리는 건너뛴다. Chrome 프로필처럼 계정 단위로
    따로 판단해야 하는 경로를 파일 단위 정리에서 제외하기 위한 것이다.
    """
    result = PruneResult(target=target)
    if not root.exists():
        logger.info("%s: 경로 없음 (%s) - 건너뜀", target, root)
        return result

    cutoff = _cutoff(days)
    skip = frozenset(str(p) for p in exclude)
    touched: set[Path] = set()
    for path, stat in _walk(root, skip):
        if suffixes is not None and path.suffix.lower() not in suffixes:
            continue
        if stat.st_mtime >= cutoff:
            continue
        try:
            result.items += 1
            result.bytes_freed += stat.st_size
            if not dry_run:
                path.unlink()
                touched.add(path.parent)
        except OSError as exc:
            result.errors += 1
            logger.warning("%s: 파일 삭제 실패 %s (%s)", target, path, exc)

    if touched:
        _remove_empty_dirs(root, touched)
    return result


def _remove_empty_dirs(root: Path, touched: set[Path]) -> None:
    """파일을 지운 디렉터리와 그 조상만 아래에서 위로 정리한다.

    트리 전체를 훑으면 안 된다. 삭제 대상이 하나도 없어도 Airflow 로그처럼
    디렉터리가 수만 개인 트리를 전부 rmdir 시도하게 되고, Windows 바인드
    마운트 위에서는 그것만으로 수십 분이 걸린다.
    """
    candidates: set[Path] = set()
    for directory in touched:
        current = directory
        while current != root and root in current.parents:
            candidates.add(current)
            current = current.parent

    # 깊은 것부터 지워야 부모가 비워진 뒤에 부모도 지워진다.
    for path in sorted(candidates, key=lambda p: len(p.parts), reverse=True):
        if path.name in PROTECTED_NAMES:
            continue
        try:
            path.rmdir()
        except OSError:
            # 비어 있지 않으면 그대로 둔다. 정상 흐름이라 경고하지 않는다.
            continue


def _prune_chrome_profiles(dry_run: bool = False) -> PruneResult:
    """계정 단위로 오래된 Chrome 프로필만 지운다.

    프로필 루트(`chrome_profiles`)와 최근 사용된 계정 프로필은 건드리지 않는다.
    """
    result = PruneResult(target="chrome_profiles")
    if not CHROME_PROFILE_DIR.exists():
        logger.info("chrome_profiles: 경로 없음 (%s) - 건너뜀", CHROME_PROFILE_DIR)
        return result

    cutoff = _cutoff(CHROME_PROFILE_RETENTION_DAYS)
    for profile in sorted(CHROME_PROFILE_DIR.iterdir()):
        try:
            if not profile.is_dir() or profile.is_symlink():
                continue
            newest, size = _profile_usage(profile)
            if newest >= cutoff:
                logger.info(
                    "chrome_profiles: 활성 프로필 보존 %s (최종 사용 %s)",
                    profile.name,
                    time.strftime("%Y-%m-%d", time.localtime(newest)),
                )
                continue
            result.items += 1
            result.bytes_freed += size
            logger.info(
                "chrome_profiles: 미사용 프로필 %s (%.2f GB, 최종 사용 %s)",
                profile.name,
                size / (1024**3),
                time.strftime("%Y-%m-%d", time.localtime(newest)),
            )
            if not dry_run:
                shutil.rmtree(profile)
        except OSError as exc:
            result.errors += 1
            logger.warning("chrome_profiles: 삭제 실패 %s (%s)", profile, exc)

    return result


def cleanup_storage(dry_run: bool = False, **context) -> dict:
    """Airflow 로그·임시파일·다운로드 잔여물·미사용 Chrome 프로필을 회수한다.

    `dag_run.conf`에 `{"dry_run": true}`를 주면 삭제 없이 대상만 집계한다.
    """
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    dry_run = bool(conf.get("dry_run", dry_run))

    system_tmp = Path(tempfile.gettempdir())

    if dry_run:
        logger.info("dry_run 모드 - 삭제 없이 대상만 집계한다")

    results = [
        _prune_files(
            AIRFLOW_LOG_DIR,
            AIRFLOW_LOG_RETENTION_DAYS,
            "airflow_logs",
            dry_run=dry_run,
        ),
        _prune_files(
            system_tmp,
            SYSTEM_TMP_RETENTION_DAYS,
            "system_tmp",
            dry_run=dry_run,
        ),
        _prune_files(
            TEMP_DIR,
            LOCAL_TEMP_RETENTION_DAYS,
            "local_temp",
            dry_run=dry_run,
        ),
        _prune_chrome_profiles(dry_run=dry_run),
        _prune_files(
            DOWN_DIR,
            DOWNLOAD_RETENTION_DAYS,
            "downloads",
            suffixes=DOWNLOAD_SUFFIXES,
            exclude=(CHROME_PROFILE_DIR,),
            dry_run=dry_run,
        ),
    ]

    total_bytes = sum(r.bytes_freed for r in results)
    total_items = sum(r.items for r in results)
    total_errors = sum(r.errors for r in results)

    for result in results:
        logger.info(result.describe(dry_run))
    logger.info(
        "정리 합계: %d건 / %.2f GB / 오류 %d건 (dry_run=%s)",
        total_items,
        total_bytes / (1024**3),
        total_errors,
        dry_run,
    )

    return {
        "dry_run": dry_run,
        "total_items": total_items,
        "total_gb": round(total_bytes / (1024**3), 3),
        "total_errors": total_errors,
        "targets": {r.target: {"items": r.items, "gb": round(r.gb_freed, 3)} for r in results},
    }
