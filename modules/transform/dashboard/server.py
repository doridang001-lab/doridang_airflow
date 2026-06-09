"""Combined dashboard server for Beamin Macro and DAG monitoring."""

from __future__ import annotations

import json
import logging
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from modules.transform.dashboard import db_beamin_macro_dashboard as beamin_dashboard
from modules.transform.dashboard.dag_monitoring_dashboard import DagMonitoringDashboardService

logger = logging.getLogger(__name__)

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8787


def _safe_json(data: Any) -> bytes:
    return json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")


def _render_index() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Dashboard Hub</title>
  <style>
    body {
      margin: 0;
      font-family: "Segoe UI", "Helvetica Neue", sans-serif;
      background: linear-gradient(180deg, #f8fafc 0%, #ecfeff 100%);
      color: #0f172a;
    }
    .page {
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }
    .card {
      width: min(920px, 100%);
      background: rgba(255,255,255,.92);
      border: 1px solid #dbeafe;
      border-radius: 24px;
      box-shadow: 0 24px 48px rgba(15, 23, 42, 0.08);
      padding: 28px;
    }
    h1 {
      margin: 0 0 10px;
      font-size: 38px;
      letter-spacing: -0.05em;
    }
    p {
      margin: 0 0 22px;
      color: #475569;
      line-height: 1.6;
    }
    .links {
      display: grid;
      gap: 14px;
    }
    a {
      display: block;
      text-decoration: none;
      color: #0f172a;
      border: 1px solid #dbeafe;
      border-radius: 18px;
      padding: 18px 20px;
      background: linear-gradient(135deg, rgba(15,118,110,.10), rgba(255,255,255,.98));
    }
    a strong {
      display: block;
      margin-bottom: 6px;
      font-size: 18px;
    }
    a span {
      color: #475569;
      font-size: 14px;
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="card">
      <h1>Dashboard Hub</h1>
      <p>Use the DAG monitoring view for pipeline health and task logs. The Beamin macro dashboard stays available as a separate route.</p>
      <div class="links">
        <a href="/dag-monitoring">
          <strong>Airflow DAG Monitoring</strong>
          <span>One-screen status, failure sorting, task drill-down, and copy-ready errors.</span>
        </a>
        <a href="/db-beamin-macro">
          <strong>DB_Beamin_Macro Dashboard</strong>
          <span>Live store progress and collection logs for the Beamin macro DAG.</span>
        </a>
      </div>
    </div>
  </div>
</body>
</html>"""


class CombinedDashboardHandler(BaseHTTPRequestHandler):
    beamin_service: beamin_dashboard.DashboardService
    dag_service: DagMonitoringDashboardService

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        route = parsed.path
        query = parse_qs(parsed.query)

        if route == "/health":
            self._send_bytes(b"ok", content_type="text/plain; charset=utf-8")
            return

        if route == "/":
            self._send_bytes(_render_index().encode("utf-8"), content_type="text/html; charset=utf-8")
            return

        if route in {"/db-beamin-macro", f"/{beamin_dashboard.HTML_FILENAME}"}:
            snapshot = self.beamin_service.get_snapshot()
            html_payload = snapshot.get("html") or "<html><body>dashboard warming up</body></html>"
            self._send_bytes(html_payload.encode("utf-8"), content_type="text/html; charset=utf-8")
            return
        if route == "/api/db-beamin-macro/summary":
            snapshot = self.beamin_service.get_snapshot()
            self._send_json(snapshot.get("overview") or {})
            return
        if route == "/api/db-beamin-macro/stores":
            snapshot = self.beamin_service.get_snapshot()
            self._send_json(snapshot.get("stores") or [])
            return
        if route == "/api/db-beamin-macro/logs":
            snapshot = self.beamin_service.get_snapshot()
            self._send_json(snapshot.get("live_logs") or [])
            return
        if route == "/api/db-beamin-macro/snapshot":
            snapshot = dict(self.beamin_service.get_snapshot())
            snapshot.pop("html", None)
            self._send_json(snapshot)
            return

        if route == "/dag-monitoring":
            self._send_bytes(self.dag_service.render_html().encode("utf-8"), content_type="text/html; charset=utf-8")
            return
        if route == "/api/dag-monitoring/snapshot":
            self._send_json(self.dag_service.get_snapshot())
            return
        if route == "/api/dag-monitoring/logs":
            dag_id = (query.get("dag_id") or [None])[0]
            run_id = (query.get("run_id") or [None])[0]
            if not dag_id:
                self.send_error(HTTPStatus.BAD_REQUEST, "dag_id is required")
                return
            payload = self.dag_service.get_dag_details(dag_id, run_id)
            if payload is None:
                self.send_error(HTTPStatus.NOT_FOUND, "dag not found")
                return
            self._send_json(payload)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "not found")

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, payload: Any, *, status: int = 200) -> None:
        self._send_bytes(_safe_json(payload), content_type="application/json; charset=utf-8", status=status)

    def _send_bytes(self, payload: bytes, *, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def run_server(
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    refresh_seconds: int = beamin_dashboard.DEFAULT_REFRESH_SECONDS,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    beamin_service = beamin_dashboard.DashboardService(refresh_seconds=refresh_seconds)
    beamin_service.refresh_once()
    beamin_service.start()

    CombinedDashboardHandler.beamin_service = beamin_service
    CombinedDashboardHandler.dag_service = DagMonitoringDashboardService()

    server = ThreadingHTTPServer((host, port), CombinedDashboardHandler)
    logger.info("Starting combined dashboard server on http://%s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Dashboard server interrupted.")
    finally:
        beamin_service.stop()
        server.server_close()


if __name__ == "__main__":
    run_server()
