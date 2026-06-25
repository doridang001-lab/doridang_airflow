import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable

import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

logger = logging.getLogger(__name__)


@contextmanager
def _uc_launch_lock(timeout_sec: int = 180):
    """Serialize UC driver patch/launch across Airflow task processes."""
    lock_dir = Path(tempfile.gettempdir()) / "undetected_chromedriver.launch.lock"
    deadline = time.time() + timeout_sec
    acquired = False
    while time.time() < deadline:
        try:
            lock_dir.mkdir()
            acquired = True
            break
        except FileExistsError:
            time.sleep(1.0)

    if not acquired:
        logger.warning("UC launch lock timeout; continuing without lock: %s", lock_dir)

    try:
        yield
    finally:
        if acquired:
            try:
                lock_dir.rmdir()
            except Exception:
                pass


def _invalidate_driver_cache_if_chrome_updated(data_dir: str, current_version: int | None) -> None:
    """Chrome 버전이 변경됐을 때 UC 캐시 바이너리를 삭제해 재다운로드를 강제한다.

    data_dir/chrome_version.txt에 마지막으로 사용한 버전을 기록하고,
    다음 호출 시 현재 버전과 비교 — 달라졌으면 chromedriver 바이너리를 삭제한다.
    """
    if current_version is None:
        return
    version_file = Path(data_dir) / "chrome_version.txt"
    try:
        if version_file.exists():
            cached = version_file.read_text().strip()
            if cached != str(current_version):
                driver_bin = Path(data_dir) / "undetected_chromedriver"
                if driver_bin.exists():
                    driver_bin.unlink()
                    logger.info(
                        "Chrome %s→%s: UC driver binary 삭제 (재다운로드 유도)", cached, current_version
                    )
        version_file.write_text(str(current_version))
    except Exception as err:
        logger.warning("UC driver cache 버전 체크 실패(무시): %s", err)


def configure_uc_data_path(data_path: str | None = None) -> str | None:
    """
    Airflow(컨테이너) 환경에서 undetected_chromedriver가 ~/.local/share 아래에
    드라이버를 생성/패치하려다 권한/HOME 문제로 실패하는 케이스를 방지한다.

    - AIRFLOW_HOME 이 있는 경우에만 적용(로컬 영향 최소화)
    - 기본값은 AIRFLOW_HOME/uc_data (컨테이너 재시작 시 다운로드 방지)
    - uc.patcher.Patcher.data_path 를 런타임에 덮어써서 다운로드/패치 경로를 고정
    """
    if os.getenv("AIRFLOW_HOME") is None:
        return None

    desired = (
        data_path
        or os.getenv("UC_DATA_DIR")
        or os.path.join(os.getenv("AIRFLOW_HOME"), "uc_data")
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


def _emit_uc_log(log_fn: Callable[[str], None] | None, message: str) -> None:
    if log_fn is not None:
        log_fn(message)
    else:
        logger.info(message)


def _resolve_chromedriver_binary() -> str | None:
    env_path = os.getenv("CHROMEDRIVER_PATH")
    candidates = [env_path] if env_path else []
    candidates.extend(
        [
            "/usr/local/bin/chromedriver",
            "/usr/bin/chromedriver",
            "chromedriver",
        ]
    )

    for candidate in candidates:
        if not candidate:
            continue
        expanded = os.path.abspath(os.path.expanduser(candidate))
        if os.path.exists(expanded):
            return expanded

        resolved = shutil.which(candidate)
        if resolved:
            return resolved

    return None


def _resolve_chrome_binary(chrome_bin: str | None = None) -> str | None:
    candidates: list[str] = []
    if chrome_bin:
        candidates.append(chrome_bin)

    env_chrome_bin = os.getenv("CHROME_BIN")
    if env_chrome_bin and env_chrome_bin not in candidates:
        candidates.append(env_chrome_bin)

    for candidate in (
        "google-chrome",
        "google-chrome-stable",
        "chromium-browser",
        "chromium",
    ):
        if candidate not in candidates:
            candidates.append(candidate)

    for candidate in candidates:
        expanded = os.path.abspath(os.path.expanduser(candidate))
        if os.path.exists(expanded):
            return expanded

        resolved = shutil.which(candidate)
        if resolved:
            return resolved

    return None


def _detect_chrome_major_version(chrome_binary: str | None) -> int | None:
    if not chrome_binary:
        return None

    try:
        result = subprocess.run(
            [chrome_binary, "--version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return None

    version_text = (result.stdout or result.stderr or "").strip()
    match = re.search(r"(\d+)\.", version_text)
    if not match:
        return None
    return int(match.group(1))


def _apply_failfast_client(driver, timeout_sec: int = 30, log_fn: Callable[[str], None] | None = None) -> None:
    """죽은 chromedriver에 대한 urllib3 재시도 폭주를 차단한다.

    세션이 끊기면(localhost 포트 Connection refused) selenium이 명령마다
    urllib3로 수 회씩 재시도하며 수백 줄의 로그와 수 분의 지연을 만든다.
    명령 timeout을 짧게(기본 무한 대기 → timeout_sec) 잡고 urllib3 Retry를
    0회로 낮춰, 죽은 세션에서 즉시 예외가 나도록 한다 → 상위 복구 로직이 바로 작동.
    """
    try:
        import urllib3

        executor = getattr(driver, "command_executor", None)
        cfg = getattr(executor, "_client_config", None)
        if executor is None or cfg is None:
            return

        cfg.timeout = timeout_sec
        pool_args = cfg.init_args_for_pool_manager
        inner = pool_args.setdefault("init_args_for_pool_manager", {})
        inner["retries"] = urllib3.Retry(total=0, connect=0, read=0, redirect=0)
        # keep_alive 풀이 이미 생성돼 있으므로 새 설정으로 재생성
        executor._conn = executor._get_connection_manager()
        _emit_uc_log(log_fn, f"failfast client 적용: timeout={timeout_sec}s, urllib3 retries=0")
    except Exception as err:
        _emit_uc_log(log_fn, f"failfast client config 적용 실패(무시): {err}")


def _is_network_launch_error(msg: str) -> bool:
    """Chrome launch 중 발생한 일시적 DNS/네트워크 오류인지 판별.

    Docker 임베디드 DNS(127.0.0.11)가 부하 시 간헐 드롭하면 undetected_chromedriver의
    launch-시 네트워크 호출이 'No address associated with hostname' 등으로 실패한다.
    이는 수 초 내 해소되는 블립이므로 재시도로 흡수한다.
    """
    return any(
        s in msg
        for s in (
            "No address associated with hostname",
            "Name or service not known",
            "Temporary failure in name resolution",
            "urlopen error",
            "Max retries exceeded with url",
            "Failed to establish a new connection",
        )
    )


def _launch_standard_chrome(
    *,
    options,
    chrome_binary: str | None,
    detected_version: int | None,
    log_fn: Callable[[str], None] | None = None,
):
    chromedriver_binary = _resolve_chromedriver_binary()
    if not chromedriver_binary:
        raise RuntimeError("standard chromedriver not found")

    _emit_uc_log(
        log_fn,
        "fallback to standard chromedriver "
        f"driver={chromedriver_binary} chrome={chrome_binary or 'auto'} version={detected_version or 'auto'}",
    )
    service = Service(executable_path=chromedriver_binary)
    driver = webdriver.Chrome(service=service, options=options)
    _apply_failfast_client(driver, log_fn=log_fn)
    return driver


def launch_uc_chrome(
    options: uc.ChromeOptions,
    account_id: str | None = None,
    chrome_bin: str | None = None,
    log_fn: Callable[[str], None] | None = None,
    prefer_standard: bool = False,
):
    chrome_binary = _resolve_chrome_binary(chrome_bin)
    detected_version = _detect_chrome_major_version(chrome_binary)

    if chrome_binary and not getattr(options, "binary_location", None):
        options.binary_location = chrome_binary

    _emit_uc_log(log_fn, f"resolved chrome binary={chrome_binary or 'auto'}")
    _emit_uc_log(log_fn, f"detected chrome major version={detected_version or 'auto'}")

    if prefer_standard:
        return _launch_standard_chrome(
            options=options,
            chrome_binary=chrome_binary,
            detected_version=detected_version,
            log_fn=log_fn,
        )

    data_dir = configure_uc_data_path()
    if data_dir:
        _invalidate_driver_cache_if_chrome_updated(data_dir, detected_version)

    kwargs: dict = {"options": options}
    if detected_version is not None:
        kwargs["version_main"] = detected_version

    # 캐시된 chromedriver가 있으면 직접 지정 → UC의 launch-시 네트워크(드라이버 다운로드/버전체크)
    # 호출을 제거한다. Docker 임베디드 DNS 간헐 드롭 시 launch가 통째로 실패(성공 0/N)하던
    # 문제를 차단하는 핵심 — 평시 실행을 네트워크 비의존으로 만든다.
    if data_dir:
        cached_driver = Path(data_dir) / "undetected_chromedriver"
        if cached_driver.exists() and os.access(cached_driver, os.X_OK):
            kwargs["driver_executable_path"] = str(cached_driver)
            _emit_uc_log(log_fn, f"캐시 드라이버 사용(네트워크 비의존): {cached_driver}")

    _emit_uc_log(log_fn, f"launch uc chrome with version_main={detected_version or 'auto'}")

    # 일시적 DNS/네트워크 오류는 재시도로 흡수 (블립은 수 초 내 해소)
    last_exc: Exception | None = None
    for net_attempt in range(3):
        try:
            with _uc_launch_lock():
                driver = uc.Chrome(**kwargs)
            _apply_failfast_client(driver, log_fn=log_fn)
            return driver
        except Exception as exc:
            last_exc = exc
            if _is_network_launch_error(str(exc)) and net_attempt < 2:
                wait = 5.0 * (net_attempt + 1)
                _emit_uc_log(
                    log_fn,
                    f"launch 네트워크 오류, {wait:.0f}s 후 재시도 {net_attempt + 1}/3: {exc}",
                )
                time.sleep(wait)
                continue
            break

    # 네트워크 재시도로도 해결 안 됨 → 기존 폴백(version mismatch / 바이너리 오염) 처리
    if True:
        exc = last_exc
        err_str = str(exc)

        # version mismatch: 에러 메시지에서 올바른 버전 추출 후 재시도
        match = re.search(r"Current browser version is (\d+)", err_str)
        if match:
            retry_version = int(match.group(1))
            _emit_uc_log(
                log_fn,
                f"chromedriver/browser version mismatch detected; retry with version_main={retry_version}",
            )
            configure_uc_data_path()
            try:
                with _uc_launch_lock():
                    driver = uc.Chrome(options=options, version_main=retry_version)
                _apply_failfast_client(driver, log_fn=log_fn)
                return driver
            except Exception:
                pass

        # RemoteDisconnected / ProtocolError: 바이너리 오염 → 삭제 후 재시도(재다운로드)
        if "RemoteDisconnected" in err_str or "Connection aborted" in err_str:
            _emit_uc_log(log_fn, "RemoteDisconnected: UC driver binary 강제 삭제 후 재시도")
            if data_dir:
                driver_bin = Path(data_dir) / "undetected_chromedriver"
                if driver_bin.exists():
                    try:
                        driver_bin.unlink()
                    except Exception:
                        pass
            configure_uc_data_path()
            # 캐시 삭제 후 재다운로드해야 하므로 driver_executable_path 제거
            retry_kwargs = {k: v for k, v in kwargs.items() if k != "driver_executable_path"}
            try:
                with _uc_launch_lock():
                    driver = uc.Chrome(**retry_kwargs)
                _apply_failfast_client(driver, log_fn=log_fn)
                return driver
            except Exception:
                pass

        # session not created / cannot connect: UC 캐시+버전파일 삭제 후 재다운로드
        if "session not created" in err_str or "cannot connect to chrome" in err_str:
            _emit_uc_log(log_fn, "session not created: UC driver 캐시+버전파일 삭제")
            if data_dir:
                for fname in ("undetected_chromedriver", "chrome_version.txt"):
                    target = Path(data_dir) / fname
                    if target.exists():
                        try:
                            target.unlink()
                        except Exception:
                            pass
            configure_uc_data_path()
            raise exc

        if "undetected_chromedriver" in err_str and "No such file or directory" in err_str:
            _emit_uc_log(log_fn, "UC driver 캐시 파일 없음: 버전파일 삭제 후 새 옵션 재시도 필요")
            if data_dir:
                version_file = Path(data_dir) / "chrome_version.txt"
                if version_file.exists():
                    try:
                        version_file.unlink()
                    except Exception:
                        pass
            configure_uc_data_path()
            raise exc

        _emit_uc_log(
            log_fn,
            "browser launch failed "
            f"binary path={chrome_binary or 'auto'} "
            f"detected version={detected_version or 'auto'} "
            f"original exception={exc}",
        )
        try:
            return _launch_standard_chrome(
                options=options,
                chrome_binary=chrome_binary,
                detected_version=detected_version,
                log_fn=log_fn,
            )
        except Exception as fallback_exc:
            _emit_uc_log(log_fn, f"standard chromedriver fallback failed: {fallback_exc}")
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("browser launch failed (no exception captured)")
