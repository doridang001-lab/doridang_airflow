"""Send a Telegram warning when Airflow/Docker disk cleanup cannot recover C:."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.transform.utility.notifier import send_telegram

STATE_DIR = PROJECT_ROOT / ".tmp" / "airflow_space_cleanup"


def _gb(path: Path) -> float:
    try:
        return path.stat().st_size / (1024**3)
    except OSError:
        return 0.0


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
