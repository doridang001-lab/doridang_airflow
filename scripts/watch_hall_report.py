#!/usr/bin/env python3
"""Watch the latest hall weekly report and open a desktop copy on Windows."""

import json
import logging
import os
import shutil
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

LOGGER = logging.getLogger(__name__)


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            if key and key not in os.environ:
                os.environ[key] = value


_load_dotenv(PROJECT_ROOT / ".env")

from modules.transform.utility.paths import MART_DB  # noqa: E402

REPORT_PATH = MART_DB / "hall_sales_target" / "hall_weekly_report.xlsx"
STATE_PATH = Path(os.getenv("HALL_REPORT_WATCH_STATE_PATH", PROJECT_ROOT / "logs" / "hall_report_watch_state.json"))
POLL_INTERVAL = int(os.getenv("HALL_REPORT_WATCH_INTERVAL", "60"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        with STATE_PATH.open("r", encoding="utf-8") as f:
            payload = json.load(f)
            return payload if isinstance(payload, dict) else {}
    except Exception:
        LOGGER.exception("failed to load state: %s", STATE_PATH)
        return {}


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _desktop_dir() -> Path:
    desktop = Path.home() / "Desktop"
    desktop.mkdir(parents=True, exist_ok=True)
    return desktop


def _open_in_excel(path: Path) -> None:
    if not sys.platform.startswith("win"):
        LOGGER.info("skip auto-open on non-Windows: %s", path)
        return
    os.startfile(str(path))  # type: ignore[attr-defined]
    LOGGER.info("opened report copy: %s", path)


def _copy_to_desktop(source: Path) -> Path:
    target = _desktop_dir() / f"홀_주간보고_{date.today():%y%m%d}.xlsx"
    shutil.copy2(source, target)
    LOGGER.info("copied report to desktop: %s", target)
    return target


def _current_signature(path: Path) -> dict | None:
    if not path.exists():
        return None
    stat = path.stat()
    return {"mtime_ns": stat.st_mtime_ns, "size": stat.st_size}


def _handle_change(state: dict) -> dict:
    signature = _current_signature(REPORT_PATH)
    if signature is None:
        LOGGER.warning("report not found: %s", REPORT_PATH)
        return state

    if state.get("mtime_ns") == signature["mtime_ns"] and state.get("size") == signature["size"]:
        return state

    desktop_copy = _copy_to_desktop(REPORT_PATH)
    _open_in_excel(desktop_copy)
    state.update(
        signature,
        source=str(REPORT_PATH),
        desktop_copy=str(desktop_copy),
        handled_at=datetime.now(timezone.utc).isoformat(),
    )
    _save_state(state)
    return state


def main() -> None:
    LOGGER.info(
        "hall report watcher start: source=%s state=%s interval=%ss",
        REPORT_PATH,
        STATE_PATH,
        POLL_INTERVAL,
    )
    state = _load_state()
    while True:
        try:
            state = _handle_change(state)
        except Exception:
            LOGGER.exception("watch loop error")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
