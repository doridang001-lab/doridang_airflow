"""Send a Telegram warning when Airflow/Docker disk cleanup cannot recover C:."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    # 이 스크립트는 컨테이너 밖(Windows 호스트, Task Scheduler)에서 실행된다.
    # notifier._get_telegram_creds()는 Airflow Variable을 먼저 시도하는데
    # 호스트에는 Airflow DB 연결이 없어 항상 실패하고, os.getenv() 폴백도
    # .env를 아무도 로드하지 않으면 빈 값이라 "Telegram credentials missing"으로
    # 조용히 넘어간다. 2026-08-31~09-11 디스크 경고가 한 번도 발송되지 않은
    # 원인 중 하나. mulryudam_login_confirm.py와 동일한 패턴으로 로드한다.
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
except Exception:
    pass

from modules.transform.utility.notifier import send_telegram

STATE_DIR = PROJECT_ROOT / ".tmp" / "airflow_space_cleanup"
# 호스트 .env가 비어 있어도(2026-09-11 실측: TELEGRAM_BOT_TOKEN/CHAT_ID 둘 다 값 없음)
# 컨테이너 안 Airflow Variable에는 정상 값이 있다. 호스트에서 직접 못 보내면
# 컨테이너를 통해 같은 notifier로 보낸다.
TELEGRAM_RELAY_CONTAINER = os.environ.get("TELEGRAM_RELAY_CONTAINER", "airflow-airflow-scheduler-1")
_RELAY_SCRIPT = (
    "import sys\n"
    "from modules.transform.utility.notifier import send_telegram\n"
    "sys.exit(0 if send_telegram(sys.stdin.read()) else 1)\n"
)


def _gb(path: Path) -> float:
    try:
        return path.stat().st_size / (1024**3)
    except OSError:
        return 0.0


def _send_via_container(message: str, container: str = TELEGRAM_RELAY_CONTAINER) -> bool:
    try:
        result = subprocess.run(
            ["docker", "exec", "-i", container, "python", "-c", _RELAY_SCRIPT],
            input=message,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            print(f"container relay failed: {result.stderr.strip()[:300]}")
        return result.returncode == 0
    except Exception as exc:
        print(f"container relay exception: {exc}")
        return False


def notify(free_gb: float, threshold_gb: float, log_path: str = "") -> bool:
    today = datetime.now().strftime("%Y%m%d")
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    marker = STATE_DIR / f"low_space_warning_{today}.sent"
    if marker.exists():
        print(f"already sent today: {marker}")
        return True

    docker_vhdx = Path(os.environ.get("LOCALAPPDATA", "")) / "Docker" / "wsl" / "disk" / "docker_data.vhdx"
    docker_vhdx_gb = _gb(docker_vhdx)
    message = (
        "[Airflow/Docker 디스크 경고]\n"
        f"C: 여유 공간: {free_gb:.2f}GB (기준 {threshold_gb:.0f}GB 미만)\n"
        f"Docker VHDX: {docker_vhdx_gb:.2f}GB\n"
        "상태: 자동 정리 후에도 여유 공간이 부족합니다.\n"
        "필요 조치: Docker Desktop disk image location을 C: 밖(E: 등)으로 이동해야 합니다."
    )
    if log_path:
        message += f"\n로그: {log_path}"

    sent = bool(send_telegram(message))
    if not sent:
        sent = _send_via_container(message)
    if sent:
        marker.write_text(datetime.now().isoformat(timespec="seconds"), encoding="utf-8")
    print("sent" if sent else "send_failed")
    return sent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--free-gb", required=True, type=float)
    parser.add_argument("--threshold-gb", required=True, type=float)
    parser.add_argument("--log-path", default="")
    args = parser.parse_args()
    if args.free_gb >= args.threshold_gb:
        print("free space is above threshold")
        return 0
    return 0 if notify(args.free_gb, args.threshold_gb, args.log_path) else 1


if __name__ == "__main__":
    raise SystemExit(main())
