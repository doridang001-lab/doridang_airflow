"""Host-side Notion Calendar cache collector for Private_MorningBriefing_Dags.

Run this on Windows host after launching scripts/notion_calendar_host_chrome.ps1.
Airflow reads the generated JSON from /opt/airflow/config/notion_calendar_cache.json.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pendulum

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parent.parent / "config" / "notion_calendar_cache.json"),
    )
    parser.add_argument("--debugger", default="127.0.0.1:9223")
    parser.add_argument("--date", default=pendulum.now("Asia/Seoul").to_date_string())
    args = parser.parse_args()

    os.environ["NOTION_CHROME_DEBUGGER"] = args.debugger
    os.environ["NOTION_BROWSER_ATTACH_ONLY"] = "1"
    os.environ.setdefault("NOTION_CLOSE_EXTRA_TABS", "1")

    from modules.transform.pipelines.private import private_morning_briefing as notion_briefing

    driver = None
    try:
        driver = notion_briefing._launch_browser()
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
