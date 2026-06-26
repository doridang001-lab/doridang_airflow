"""Host-side Notion Calendar cache collector for Private_MorningBriefing_Dags.

Run this on Windows host after launching scripts/notion_calendar_host_chrome.ps1.
Airflow reads the generated JSON from /opt/airflow/config/notion_calendar_cache.json.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from pathlib import Path

import pendulum
from selenium import webdriver

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _find_edge_exe() -> str:
    candidates = [
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        str(Path(os.getenv("LOCALAPPDATA", "")) / "Microsoft" / "Edge" / "Application" / "msedge.exe"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    raise RuntimeError("Microsoft Edge 실행 파일을 찾을 수 없습니다.")


def _launch_edge(profile_dir: str):
    options = webdriver.EdgeOptions()
    options.binary_location = _find_edge_exe()
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1440,1000")
    driver = webdriver.Edge(options=options)
    driver.set_page_load_timeout(60)
    return driver


def _debugger_is_open(debugger: str) -> bool:
    host, _, port = (debugger or "").partition(":")
    if not host or not port.isdigit():
        return False
    sock = socket.socket()
    sock.settimeout(1)
    try:
        sock.connect((host, int(port)))
        return True
    except Exception:
        return False
    finally:
        sock.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parent.parent / "config" / "notion_calendar_cache.json"),
    )
    parser.add_argument("--debugger", default="127.0.0.1:9223")
    parser.add_argument("--date", default=pendulum.now("Asia/Seoul").to_date_string())
    parser.add_argument(
        "--profile-dir",
        default=str(Path(__file__).resolve().parent.parent / "chrome_profiles" / "notion_calendar_edge"),
    )
    args = parser.parse_args()

    os.environ["NOTION_CHROME_DEBUGGER"] = args.debugger
    os.environ["NOTION_BROWSER_ATTACH_ONLY"] = "1"
    os.environ.setdefault("NOTION_CLOSE_EXTRA_TABS", "1")

    from modules.transform.pipelines.private import private_morning_briefing as notion_briefing

    driver = None
    try:
        if _debugger_is_open(args.debugger):
            try:
                driver = notion_briefing._launch_browser()
            except Exception as attach_error:
                print(f"debugger attach failed, launching Edge profile directly: {attach_error}")
        if driver is None:
            os.environ["NOTION_CHROME_DEBUGGER"] = ""
            os.environ["NOTION_BROWSER_ATTACH_ONLY"] = "0"
            driver = _launch_edge(args.profile_dir)
        events_by_day = notion_briefing._collect_calendar(driver, target_date=args.date)
        today_events = events_by_day.get("today", []) if isinstance(events_by_day, dict) else []
        current_url = (getattr(driver, "current_url", "") or "").lower()
        auth_failed = any(
            marker in current_url
            for marker in [
                "notion.so/login",
                "notion.so/calendarauth",
                "calendar.notion.so/login",
                "error=true",
            ]
        )
        if auth_failed and not today_events:
            raise RuntimeError(f"Notion Calendar 로그인/인증 필요: {getattr(driver, 'current_url', '')}")
        payload = {
            "date": args.date,
            "source": "notion_calendar",
            "events": [
                {
                    "time": ev.get("time") or "종일",
                    "summary": (ev.get("title") or "").strip(),
                    "status": (ev.get("status") or "").strip(),
                    "attending": bool(ev.get("attending", True)),
                }
                for ev in today_events
                if (ev.get("title") or "").strip()
            ],
        }
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"wrote {out_path} ({len(payload['events'])} events)")
        return 0
    finally:
        if driver is not None:
            notion_briefing._quit_browser(driver)


if __name__ == "__main__":
    raise SystemExit(main())
