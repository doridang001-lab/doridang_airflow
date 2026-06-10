from __future__ import annotations

import html
import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from modules.transform.utility.paths import DASHBOARD_DB

logger = logging.getLogger(__name__)

DAG_ID = "DB_Beamin_Macro_Dags"
HTML_FILENAME = "db_beamin_macro_dashboard.html"
JSON_FILENAME = "db_beamin_macro_snapshot.json"
STORE_PROGRESS_FILENAME = "db_beamin_macro_store_progress.json"
LIVE_LOG_FILENAME = "db_beamin_macro_live_log.json"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8787
DEFAULT_REFRESH_SECONDS = int(os.getenv("BEAMIN_DASHBOARD_REFRESH_SECONDS", "1"))
SSE_KEEPALIVE_SECONDS = 15
RECENT_BASELINE_LIMIT = 7
MAX_LIVE_LOGS = 300

RUNNING_STATES = {"queued", "running", "scheduled", "deferred", "up_for_retry"}
FAILED_STATES = {"failed", "upstream_failed"}
COMPLETED_STATES = {"success", "failed", "upstream_failed", "skipped", "removed"}
DONE_BADGES = {"완료", "건너뜀"}

SECTION_START_RE = re.compile(r"===\s*(?P<section>.+?)\s*(?:brand=(?P<brand>[^ ]+)\s+)?\[(?P<account>.+?)\s*/\s*(?P<store>.+?)\]\s*===")
LOGIN_FAIL_RE = re.compile(r"per-store 로그인 실패:\s*(?P<account>[^/]+?)\s*/\s*(?P<store>.+)")
STORE_EQUALS_RE = re.compile(r"store=(?P<store>.+?)(?:/ym=|\s+(?:ym=|->|→)|\s*$)")
TIME_RE = re.compile(r"(?P<time>\d{2}:\d{2}:\d{2})")
CHANGE_LABEL_RE = re.compile(r"(?P<store>[^/\[\]]+?)(?:\s*/\s*(?P<account>[A-Za-z0-9_@.-]+))?$")
BRAND_STORE_RE = re.compile(r"brand=(?P<brand>[^ ]+)\s+store=(?P<store>.+?)(?:\s+ym=|\s+->|\s+→|\s+\(|$)")
AIRFLOW_LOG_TS_RE = re.compile(r"^\[(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[^]]*)\]\s*\{[^}]+\}\s*\w+\s*-\s*")
AIRFLOW_LOG_MSG_RE = re.compile(r"^\[[\d\-T:.+Z]+\]\s*\{[^}]+\}\s*\w+\s*-\s*")
ACCOUNT_ACTIVITY_RE = re.compile(r"\[(?P<account>[A-Za-z0-9_@.-]+)\]\s*(?P<message>.+)")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _to_kst_display(value: datetime | str | None) -> str:
    if value is None:
        return "-"
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    try:
        from zoneinfo import ZoneInfo

        return value.astimezone(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _duration_seconds(start: datetime | None, end: datetime | None, *, now: datetime | None = None) -> float | None:
    if start is None:
        return None
    if end is None:
        end = now
    if end is None:
        return None
    return round(max((end - start).total_seconds(), 0.0), 1)


def _format_duration(value: float | None) -> str:
    if value is None:
        return "-"
    total = int(round(value))
    hours, rem = divmod(total, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def _safe_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def _safe_json_embed(data: Any) -> str:
    # ensure_ascii=True converts surrogates to \uXXXX escape sequences,
    # preventing JS parse errors when log data contains surrogate chars.
    raw = json.dumps(data, ensure_ascii=True)
    return raw.replace("</", "<\\/")  # prevent premature </script> tag closing


def _mean(values: list[float]) -> float | None:
    values = [value for value in values if value is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 1)


def _as_text(value: Any, limit: int = 180) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\r", "").strip()
    if not text:
        return "-"
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def _parse_time_token(line: str) -> str | None:
    match = TIME_RE.search(line)
    return match.group("time") if match else None


def _parse_log_datetime(line: str) -> datetime | None:
    m = AIRFLOW_LOG_TS_RE.match(line)
    if not m:
        return None
    ts = m.group("ts")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            raw = ts.replace("+0000", "+00:00")
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _strip_log_prefix(line: str) -> str:
    return AIRFLOW_LOG_MSG_RE.sub("", line).strip() or line.strip()


def _status_rank(value: str) -> int:
    return {
        "수집중": 4,
        "실패": 3,
        "완료": 2,
        "건너뜀": 2,
        "대기": 1,
    }.get(value, 0)


def _merge_status(current: str, incoming: str) -> str:
    if incoming == current:
        return current
    return incoming if _status_rank(incoming) >= _status_rank(current) else current


def _line_contains_any(line: str, keywords: tuple[str, ...]) -> bool:
    lowered = line.lower()
    return any(keyword.lower() in lowered for keyword in keywords)


def _is_account_activity_message(line: str) -> tuple[str, str] | None:
    msg = _strip_log_prefix(line)
    match = ACCOUNT_ACTIVITY_RE.search(msg)
    if not match:
        return None
    account = match.group("account").strip()
    message = match.group("message").strip()
    if not account or not message:
        return None
    return account, message


@dataclass
class DashboardPaths:
    root: Path

    @property
    def html_path(self) -> Path:
        return self.root / HTML_FILENAME

    @property
    def json_path(self) -> Path:
        return self.root / JSON_FILENAME

    @property
    def store_progress_path(self) -> Path:
        return self.root / STORE_PROGRESS_FILENAME

    @property
    def live_log_path(self) -> Path:
        return self.root / LIVE_LOG_FILENAME


@dataclass
class StoreRow:
    seq: int
    store_name: str
    account_id: str = "-"
    now_status: str = "대기"
    woori_status: str = "대기"
    ad_status: str = "대기"
    change_status: str = "대기"
    brand: str = "도리당"
    orders_status: str = "대기"
    marketing_status: str = "대기"
    elapsed_sec: float | None = None
    note: str = ""
    last_event: str = ""
    updated_at: str | None = None
    selected: bool = False
    started_at: datetime | None = None
    ended_at: datetime | None = None
    current_section: str = ""
    retry_status: str = ""
    last_touched_at: datetime | None = None
    raw_events: list[str] = field(default_factory=list)

    def to_payload(self, *, now: datetime, active_run: bool) -> dict[str, Any]:
        store_done = self.orders_status in DONE_BADGES and self.marketing_status in DONE_BADGES
        if "실패" in {self.orders_status, self.marketing_status}:
            store_state = "실패"
        elif "수집중" in {self.orders_status, self.marketing_status}:
            store_state = "수집중"
        elif store_done:
            store_state = "완료"
        elif self.orders_status in DONE_BADGES or self.marketing_status in DONE_BADGES:
            store_state = "부분완료"
        else:
            store_state = "대기"
        if self.ended_at is not None:
            end = self.ended_at
        elif store_done and self.last_touched_at is not None:
            end = self.last_touched_at
        elif active_run:
            end = None
        else:
            end = self.last_touched_at or now
        elapsed = self.elapsed_sec
        if elapsed is None:
            elapsed = _duration_seconds(self.started_at, end, now=now)
        note_parts: list[str] = []
        if self.marketing_status != "대기":
            note_parts.append(f"마케팅 {self.marketing_status}")
        if self.orders_status != "대기":
            note_parts.append(f"주문서 {self.orders_status}")
        if self.now_status != "대기":
            note_parts.append(f"now {self.now_status}")
        if self.woori_status != "대기":
            note_parts.append(f"우가클 {self.woori_status}")
        if self.ad_status != "대기":
            note_parts.append(f"광고 {self.ad_status}")
        if self.change_status != "대기":
            note_parts.append(f"변경 {self.change_status}")
        if self.note:
            note_parts.append(self.note)
        if self.retry_status == "재시도완료":
            failures = [p for p in note_parts if "실패" in p]
            note_parts = failures if failures else []
        return {
            "seq": self.seq,
            "store_name": self.store_name,
            "account_id": self.account_id,
            "brand": self.brand,
            "orders_status": self.orders_status,
            "marketing_status": self.marketing_status,
            "now_status": self.now_status,
            "woori_status": self.woori_status,
            "ad_status": self.ad_status,
            "change_status": self.change_status,
            "elapsed_sec": elapsed,
            "elapsed_text": _format_duration(elapsed),
            "note": " | ".join(note_parts) if note_parts else "-",
            "last_event": self.last_event or "-",
            "updated_at": self.updated_at,
            "selected": self.selected,
            "current_section": self.current_section or "-",
            "retry_status": self.retry_status,
            "store_key": f"{self.brand}/{self.store_name}",
            "store_state": store_state,
            "is_fully_done": store_done,
            "is_partially_done": store_state == "부분완료",
        }


class AirflowMetadataRepository:
    """Reads DagRun, TaskInstance, and XCom state from the Airflow metadata DB."""

    def __init__(self, dag_id: str = DAG_ID):
        self.dag_id = dag_id

    def get_latest_active_run(self) -> dict[str, Any] | None:
        return self._get_single_run(states=RUNNING_STATES)

    def get_latest_completed_run(self) -> dict[str, Any] | None:
        return self._get_single_run(exclude_states=RUNNING_STATES)

    def get_recent_success_runs(self, limit: int = RECENT_BASELINE_LIMIT) -> list[dict[str, Any]]:
        return self._get_runs(states={"success"}, limit=limit)

    def _get_single_run(
        self,
        *,
        states: set[str] | None = None,
        exclude_states: set[str] | None = None,
    ) -> dict[str, Any] | None:
        runs = self._get_runs(states=states, exclude_states=exclude_states, limit=1)
        return runs[0] if runs else None

    def _get_runs(
        self,
        *,
        states: set[str] | None = None,
        exclude_states: set[str] | None = None,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        from airflow.models import DagRun, TaskInstance
        from airflow.models.xcom import XCom
        from airflow.utils.session import create_session

        with create_session() as session:
            query = (
                session.query(DagRun)
                .filter(DagRun.dag_id == self.dag_id)
                .order_by(DagRun.execution_date.desc())
            )
            if states:
                query = query.filter(DagRun.state.in_(sorted(states)))
            if exclude_states:
                query = query.filter(~DagRun.state.in_(sorted(exclude_states)))

            dag_runs = query.limit(limit).all()
            payloads: list[dict[str, Any]] = []
            for dag_run in dag_runs:
                tasks = (
                    session.query(TaskInstance)
                    .filter(TaskInstance.dag_id == dag_run.dag_id)
                    .filter(TaskInstance.run_id == dag_run.run_id)
                    .all()
                )
                payloads.append(
                    {
                        "dag_id": dag_run.dag_id,
                        "run_id": dag_run.run_id,
                        "state": dag_run.state,
                        "execution_date": dag_run.execution_date,
                        "start_date": dag_run.start_date,
                        "end_date": dag_run.end_date,
                        "tasks": [
                            {
                                "task_id": task.task_id,
                                "state": task.state,
                                "start_date": task.start_date,
                                "end_date": task.end_date,
                                "duration": task.duration,
                            }
                            for task in tasks
                        ],
                        "xcom": self._load_xcom_map(session, XCom, dag_run.run_id),
                    }
                )
            return payloads

    def _load_xcom_map(self, session: Any, xcom_model: Any, run_id: str) -> dict[str, dict[str, Any]]:
        rows = (
            session.query(xcom_model)
            .filter(xcom_model.dag_id == self.dag_id)
            .filter(xcom_model.run_id == run_id)
            .all()
        )
        payload: dict[str, dict[str, Any]] = {}
        for row in rows:
            task_payload = payload.setdefault(row.task_id, {})
            try:
                value = row.value
                if hasattr(xcom_model, "deserialize_value"):
                    value = xcom_model.deserialize_value(row)
            except Exception:
                try:
                    value = row.value
                except Exception:
                    value = None
            task_payload[row.key] = value
        return payload


class StoreProgressParser:
    def __init__(self, logs_root: Path | None = None, dag_id: str = DAG_ID):
        self.logs_root = logs_root or (Path(os.getenv("AIRFLOW_HOME", Path.cwd())) / "logs")
        self.dag_id = dag_id

    def build_store_progress(
        self,
        run: dict[str, Any] | None,
        *,
        now: datetime,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
        if not run:
            return [], [], None

        account_list = self._extract_accounts(run)
        ad_stores = self._extract_ad_stores(run)
        store_map = self._seed_store_map(account_list, ad_stores)
        live_logs: list[dict[str, Any]] = []

        collect_all_state = self._task_state(run, "collect_all")
        collect_change_state = self._task_state(run, "collect_shop_change")
        self._parse_collect_all(run, store_map, live_logs, task_state=collect_all_state)
        self._parse_collect_shop_change(run, store_map, live_logs, task_state=collect_change_state)
        self._apply_xcom_hints(run, store_map)
        self._parse_retry_failed(run, store_map, live_logs)

        rows = list(store_map.values())
        if not rows:
            rows = self._stores_from_logs(run, now=now, live_logs=live_logs)

        active_run = run.get("state") in RUNNING_STATES
        current_store = None
        for row in rows:
            if active_run and row.current_section and row.orders_status == "수집중":
                current_store = f"{row.brand}/{row.store_name}"
                row.selected = True
                break
            if active_run and row.marketing_status == "수집중":
                current_store = f"{row.brand}/{row.store_name}"
                row.selected = True
                break
        if current_store is None and rows:
            rows[0].selected = True
            current_store = f"{rows[0].brand}/{rows[0].store_name}"

        payload_rows = [row.to_payload(now=now, active_run=active_run) for row in self._sort_rows(rows)]
        return payload_rows, live_logs[-MAX_LIVE_LOGS:], current_store

    def _seed_store_map(
        self, account_list: list[dict[str, Any]], ad_stores: list[dict[str, Any]] | None = None
    ) -> dict[str, StoreRow]:
        store_map: dict[str, StoreRow] = {}
        seq = 1

        for entry in account_list:
            acc = entry.get("account") if isinstance(entry.get("account"), dict) else entry
            full_name = str(acc.get("store_name") or entry.get("store_name") or "")
            parts = full_name.split()
            if len(parts) >= 2:
                brand, store = parts[0], " ".join(parts[1:])
            elif parts:
                brand, store = "도리당", parts[0]
            else:
                continue
            account_id = str(acc.get("account_id") or entry.get("account_id") or "-")
            key = f"{brand}/{store}"
            if key not in store_map:
                store_map[key] = StoreRow(seq=seq, store_name=store, account_id=account_id, brand=brand)
                seq += 1

        for item in (ad_stores or []):
            if not isinstance(item, dict):
                continue
            brand = str(item.get("brand") or "")
            store = str(item.get("store") or "")
            account_id = str(item.get("account_id") or "-")
            if not brand or not store:
                continue
            key = f"{brand}/{store}"
            if key not in store_map:
                store_map[key] = StoreRow(seq=seq, store_name=store, account_id=account_id, brand=brand)
                seq += 1

        return store_map

    def _extract_accounts(self, run: dict[str, Any]) -> list[dict[str, Any]]:
        xcom = run.get("xcom", {})
        load_accounts = xcom.get("load_accounts", {})
        account_list = load_accounts.get("account_list")
        if isinstance(account_list, list):
            return account_list
        return []

    def _extract_ad_stores(self, run: dict[str, Any]) -> list[dict[str, Any]]:
        xcom = run.get("xcom", {})
        collect_all = xcom.get("collect_all", {})
        ad_stores = collect_all.get("ad_stores")
        if isinstance(ad_stores, list):
            return ad_stores
        return []

    def _task_state(self, run: dict[str, Any], task_id: str) -> str | None:
        for task in run.get("tasks", []):
            if task.get("task_id") == task_id:
                return task.get("state")
        return None

    def _sort_rows(self, rows: list[StoreRow]) -> list[StoreRow]:
        def key(row: StoreRow) -> tuple[int, int, str]:
            selected_rank = 0 if row.selected else 1
            state_rank = {
                "수집중": 0,
                "실패": 1,
                "완료": 2,
                "건너뜀": 2,
                "대기": 3,
            }.get(self._overall_store_status(row), 4)
            return (selected_rank, state_rank, row.store_name)

        return sorted(rows, key=key)

    def _overall_store_status(self, row: StoreRow) -> str:
        if "실패" in {row.orders_status, row.marketing_status}:
            return "실패"
        if "수집중" in {row.orders_status, row.marketing_status}:
            return "수집중"
        if row.orders_status in DONE_BADGES and row.marketing_status in DONE_BADGES:
            return "완료"
        if row.orders_status in DONE_BADGES or row.marketing_status in DONE_BADGES:
            return "부분완료"
        return "대기"

    def _stores_from_logs(
        self,
        run: dict[str, Any],
        *,
        now: datetime,
        live_logs: list[dict[str, Any]],
    ) -> list[StoreRow]:
        inferred_map: dict[str, StoreRow] = {}
        self._parse_collect_all(run, inferred_map, live_logs, task_state=self._task_state(run, "collect_all"))
        self._parse_collect_shop_change(run, inferred_map, live_logs, task_state=self._task_state(run, "collect_shop_change"))
        if not inferred_map:
            return []
        return list(inferred_map.values())

    def _parse_collect_all(
        self,
        run: dict[str, Any],
        store_map: dict[str, StoreRow],
        live_logs: list[dict[str, Any]],
        *,
        task_state: str | None,
    ) -> None:
        log_file = self._find_latest_log(run.get("run_id"), "collect_all")
        if not log_file or not log_file.exists():
            return

        current_store: str | None = None
        current_account: str | None = None

        for raw_line in log_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line:
                continue

            section_match = SECTION_START_RE.search(line)
            if section_match:
                current_account = section_match.group("account").strip()
                current_store = section_match.group("store").strip()
                section_name = section_match.group("section").strip()
                row = self._ensure_store(store_map, current_store, current_account)
                row.started_at = row.started_at or _parse_log_datetime(line) or run.get("start_date")
                row.current_section = section_name
                if "주문" in section_name:
                    row.orders_status = "수집중"
                else:
                    row.marketing_status = "수집중"
                self._touch_row(row, line)
                self._append_live_log(live_logs, current_store, line)
                continue

            login_fail = LOGIN_FAIL_RE.search(line)
            if login_fail:
                current_account = login_fail.group("account").strip()
                current_store = login_fail.group("store").strip()
                row = self._ensure_store(store_map, current_store, current_account)
                row.orders_status = "실패"
                row.marketing_status = "실패"
                row.note = "로그인 실패"
                self._touch_row(row, line)
                self._append_live_log(live_logs, current_store, line, level="WARN")
                continue

            store_name = self._extract_store_name(line, current_store)
            if not store_name:
                continue
            row = self._ensure_store(store_map, store_name, current_account)
            brand = _extract_brand_from_line(line)
            if brand:
                row.brand = brand

            if "DB_Beamin_04_orders.py" in line or _line_contains_any(line, ("저장 완료(정상)", "저장 완료(취소)", "정상 주문 없음", "주문취소 없음")):
                if _line_contains_any(line, ("저장 완료(정상)", "저장 완료(취소)", "정상 주문 없음", "주문취소 없음")):
                    row.orders_status = "완료"
                elif _line_contains_any(line, ("오류", "실패", "timeout")):
                    row.orders_status = "실패"
                    row.note = "주문서 수집 실패"
                else:
                    row.orders_status = _merge_status(row.orders_status, "수집중")
                self._touch_row(row, line)
                self._append_live_log(live_logs, store_name, line)
                continue

            if "저장 완료:" in line and store_name:
                if "광고" in row.current_section or "funnel" in row.current_section.lower() or "마케팅" in row.current_section or "CMG" in row.current_section:
                    if row.marketing_status != "실패":
                        row.marketing_status = "완료"
                    self._touch_row(row, line)
                    self._append_live_log(live_logs, store_name, line)
                    continue

            if "DB_Beamin_02_woori_shop_click.py" in line or _line_contains_any(line, ("CMG", "우리매장", "데이터없음")):
                if _line_contains_any(line, ("저장 완료", "데이터없음")):
                    row.marketing_status = "완료" if "저장 완료" in line else "건너뜀"
                    if "데이터없음" in line:
                        row.note = "CMG 데이터없음"
                elif _line_contains_any(line, ("실패", "오류", "미감지")):
                    row.marketing_status = "실패"
                    row.note = "CMG 수집 실패"
                else:
                    row.marketing_status = _merge_status(row.marketing_status, "수집중")
                self._touch_row(row, line)
                self._append_live_log(live_logs, store_name, line)
                continue

            if "DB_Beamin_05_ad_funnel.py" in line or _line_contains_any(line, ("광고 없음", "광고 funnel", "지표 추출")):
                if _line_contains_any(line, ("저장 완료", "광고 없음")):
                    if row.marketing_status != "실패":
                        row.marketing_status = "완료" if "저장 완료" in line else "건너뜀"
                    if "광고 없음" in line:
                        row.note = "광고 없음"
                elif _line_contains_any(line, ("지표 추출 오류", "실패", "timeout")):
                    row.marketing_status = "실패"
                    row.note = "광고 수집 실패"
                else:
                    row.marketing_status = _merge_status(row.marketing_status, "수집중")
                self._touch_row(row, line)
                self._append_live_log(live_logs, store_name, line)
                continue

            if "다음 계정까지" in line:
                self._touch_row(row, line)
                self._append_live_log(live_logs, store_name, line)

        if task_state in COMPLETED_STATES:
            self._finalize_collect_all_states(store_map, failed=(task_state in FAILED_STATES))

    def _parse_collect_shop_change(
        self,
        run: dict[str, Any],
        store_map: dict[str, StoreRow],
        live_logs: list[dict[str, Any]],
        *,
        task_state: str | None,
    ) -> None:
        log_file = self._find_latest_log(run.get("run_id"), "collect_shop_change")
        if not log_file or not log_file.exists():
            return

        current_store: str | None = None
        current_account: str | None = None
        for raw_line in log_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if "변경이력 수집 시작:" in line:
                label = line.split("변경이력 수집 시작:", 1)[1].strip()
                current_store, current_account = self._parse_change_label(label)
                row = self._ensure_store(store_map, current_store, current_account)
                row.marketing_status = _merge_status(row.marketing_status, "수집중")
                row.current_section = "변경이력"
                self._touch_row(row, line)
                self._append_live_log(live_logs, current_store, line)
                continue

            if "store 변경이력 수집 완료:" in line:
                label = line.split("store 변경이력 수집 완료:", 1)[1].strip()
                store_name, account_id = self._parse_change_label(label)
                row = self._ensure_store(store_map, store_name, account_id)
                if row.marketing_status != "실패":
                    row.marketing_status = "완료"
                self._touch_row(row, line)
                self._append_live_log(live_logs, store_name, line)
                continue

            if "데이터없음" in line and current_store:
                row = self._ensure_store(store_map, current_store, current_account)
                if row.marketing_status != "실패":
                    row.marketing_status = "건너뜀"
                row.note = "변경이력 데이터없음"
                self._touch_row(row, line)
                self._append_live_log(live_logs, current_store, line)

        if task_state in COMPLETED_STATES:
            for row in store_map.values():
                if row.marketing_status == "수집중":
                    row.marketing_status = "완료" if task_state == "success" else row.marketing_status

    def _apply_xcom_hints(self, run: dict[str, Any], store_map: dict[str, StoreRow]) -> None:
        xcom = run.get("xcom", {})
        collect_all = xcom.get("collect_all", {})
        failed = collect_all.get("failed") or {}

        for item in failed.get("stores", []) or []:
            store_d = item.get("store", {}) if isinstance(item, dict) else {}
            store_name = (store_d.get("store") if isinstance(store_d, dict) else None) or ""
            if not store_name:
                continue
            acc_d = item.get("account", {}) if isinstance(item, dict) else {}
            acc_id = acc_d.get("account_id") if isinstance(acc_d, dict) else None
            brand = store_d.get("brand") if isinstance(store_d, dict) else None
            row = self._ensure_store(store_map, store_name, acc_id)
            row.marketing_status = _merge_status(row.marketing_status, "실패")
            if brand:
                row.brand = brand

        for item in failed.get("orders", []) or []:
            stores_list = item.get("stores", []) if isinstance(item, dict) else []
            acc_d = item.get("account", {}) if isinstance(item, dict) else {}
            acc_id = acc_d.get("account_id") if isinstance(acc_d, dict) else None
            for store_d in stores_list or []:
                store_name = (store_d.get("store") if isinstance(store_d, dict) else None) or ""
                if not store_name:
                    continue
                row = self._ensure_store(store_map, store_name, acc_id)
                row.orders_status = _merge_status(row.orders_status, "실패")
                if isinstance(store_d, dict) and store_d.get("brand"):
                    row.brand = store_d["brand"]

        for item in collect_all.get("validation") or []:
            if item.get("matched") is False:
                row = self._ensure_store(store_map, str(item.get("store") or "알수없음"), None)
                row.note = "주문 검증 불일치"

    def _parse_retry_failed(
        self, run: dict[str, Any], store_map: dict[str, StoreRow], live_logs: list[dict[str, Any]]
    ) -> None:
        log_file = self._find_latest_log(run.get("run_id"), "retry_failed")
        if not log_file or not log_file.exists():
            return
        for raw_line in log_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            msg = _strip_log_prefix(line)
            # "재시도 성공: account_id / store_name"
            if msg.startswith("재시도 성공:") and "/" in msg:
                parts = msg.split("재시도 성공:", 1)[1].strip().split("/", 1)
                store_name = parts[-1].strip() if len(parts) >= 2 else parts[0].strip()
                matching = [k for k in store_map if k == store_name or k.endswith(f"/{store_name}")]
                for k in matching:
                    store_map[k].retry_status = "재시도완료"
                    store_map[k].marketing_status = "완료"
                store_key = matching[0] if matching else store_name
                self._append_live_log(live_logs, store_key, line)
                continue
            # "재시도 실패 [account_id / store_name]: ..."
            if msg.startswith("재시도 실패") and "/" in msg:
                inner = msg.split("[", 1)[-1].split("]", 1)[0] if "[" in msg else ""
                store_name = inner.split("/", 1)[-1].strip() if "/" in inner else ""
                matching = [k for k in store_map if k == store_name or k.endswith(f"/{store_name}")]
                for k in matching:
                    store_map[k].retry_status = "재시도실패"
                store_key = matching[0] if matching else (store_name or "?")
                self._append_live_log(live_logs, store_key, line, level="WARN")
                continue
            # "재시도 로그인 실패: account_id / store_name"
            if "재시도 로그인 실패" in msg and "/" in msg:
                store_name = msg.split("/", 1)[-1].strip()
                matching = [k for k in store_map if k == store_name or k.endswith(f"/{store_name}")]
                for k in matching:
                    store_map[k].retry_status = "재시도실패"
                store_key = matching[0] if matching else store_name
                self._append_live_log(live_logs, store_key, line, level="WARN")
                continue
            # "ads 재시도 성공: account_id"
            if msg.startswith("ads 재시도 성공:"):
                acc_id = msg.split("ads 재시도 성공:", 1)[1].strip()
                for row in store_map.values():
                    if row.account_id == acc_id and row.ad_status == "실패":
                        row.ad_status = "완료"
                self._append_live_log(live_logs, acc_id, line)
                continue
            # "ads 재시도 실패 [account_id]: ..."
            if msg.startswith("ads 재시도 실패"):
                self._append_live_log(live_logs, "?", line, level="WARN")

    def _finalize_collect_all_states(self, store_map: dict[str, StoreRow], *, failed: bool) -> None:
        for row in store_map.values():
            if row.orders_status == "수집중":
                row.orders_status = "실패" if failed else "완료"
            if row.marketing_status == "수집중":
                row.marketing_status = "실패" if failed else "완료"
            if row.ended_at is None and row.started_at is not None:
                row.ended_at = row.last_touched_at or _utc_now()

    def _ensure_store(
        self, store_map: dict[str, StoreRow], store_name: str, account_id: str | None, brand: str | None = None
    ) -> StoreRow:
        store_name = store_name.strip()
        if brand:
            key = f"{brand}/{store_name}"
        else:
            # Search for any existing entry with this store_name
            matches = [k for k in store_map if k == store_name or k.endswith(f"/{store_name}")]
            key = matches[0] if matches else store_name

        if key not in store_map:
            plain = key.split("/", 1)[-1] if "/" in key else key
            b = key.split("/", 1)[0] if "/" in key else (brand or "도리당")
            store_map[key] = StoreRow(seq=len(store_map) + 1, store_name=plain, account_id=account_id or "-", brand=b)
        row = store_map[key]
        if account_id and row.account_id == "-":
            row.account_id = account_id
        if brand:
            row.brand = brand
        return row

    def _touch_row(self, row: StoreRow, line: str) -> None:
        row.last_event = _as_text(line)
        row.updated_at = _parse_time_token(line) or row.updated_at or _utc_now().strftime("%H:%M:%S")
        row.started_at = row.started_at or _utc_now()
        if row.ended_at is None:
            row.last_touched_at = _parse_log_datetime(line) or row.last_touched_at
        row.raw_events.append(row.last_event)

    def _append_live_log(self, live_logs: list[dict[str, Any]], store_name: str, line: str, *, level: str = "INFO") -> None:
        store_key = store_name.strip() if store_name else "?"
        display_name = store_key.split("/", 1)[-1] if "/" in store_key else store_key
        live_logs.append(
            {
                "time": _parse_time_token(line) or _utc_now().strftime("%H:%M:%S"),
                "store_name": display_name,
                "store_key": store_key,
                "level": level,
                "message": _as_text(_strip_log_prefix(line), limit=240),
            }
        )
        if len(live_logs) > MAX_LIVE_LOGS:
            del live_logs[:-MAX_LIVE_LOGS]

    def _parse_change_label(self, label: str) -> tuple[str, str | None, str | None]:
        if "[" in label and "]" in label:
            inner = label.split("[", 1)[1].split("]", 1)[0]
            if "/" in inner:
                account, store = [part.strip() for part in inner.split("/", 1)]
                return store, account, None
        slash_parts = [part.strip() for part in label.split("/") if part.strip()]
        if len(slash_parts) >= 3:
            brand = slash_parts[0]
            store = slash_parts[1]
            tail = slash_parts[2]
            account = tail if re.fullmatch(r"[A-Za-z0-9_@.-]+", tail) and not tail.isdigit() else None
            return store, account, brand
        if " / " in label:
            parts = [part.strip() for part in label.split(" / ") if part.strip()]
            if len(parts) >= 2:
                if re.fullmatch(r"[A-Za-z0-9_@.-]+", parts[-1]):
                    return parts[-2], parts[-1], None
                return parts[-1], None, None
        match = CHANGE_LABEL_RE.search(label.strip())
        if match:
            return match.group("store").strip(), match.group("account"), None
        return label.strip(), None, None

    def _extract_store_name(self, line: str, fallback: str | None) -> str | None:
        match = STORE_EQUALS_RE.search(line)
        if match:
            return match.group("store").strip()
        if fallback:
            return fallback
        return None

    def _find_latest_log(self, run_id: str | None, task_id: str) -> Path | None:
        if not run_id:
            return None
        task_root = self.logs_root / f"dag_id={self.dag_id}" / f"run_id={run_id}" / f"task_id={task_id}"
        if not task_root.exists():
            return None
        attempts = sorted(task_root.glob("attempt=*.log"))
        return attempts[-1] if attempts else None


def _parser_mark_section_started(self: StoreProgressParser, row: StoreRow, section_name: str) -> None:
    lowered = section_name.lower()
    if "now" in lowered:
        row.now_status = _merge_status(row.now_status, "수집중")
        row.marketing_status = _merge_status(row.marketing_status, "수집중")
        return
    if "order" in lowered or "주문" in section_name:
        row.orders_status = _merge_status(row.orders_status, "수집중")
        return
    if "광고" in section_name or "ad" in lowered or "funnel" in lowered:
        row.ad_status = _merge_status(row.ad_status, "수집중")
        row.marketing_status = _merge_status(row.marketing_status, "수집중")
        return
    row.woori_status = _merge_status(row.woori_status, "수집중")
    row.marketing_status = _merge_status(row.marketing_status, "수집중")


def _parser_parse_collect_all(
    self: StoreProgressParser,
    run: dict[str, Any],
    store_map: dict[str, StoreRow],
    live_logs: list[dict[str, Any]],
    *,
    task_state: str | None,
) -> None:
    log_file = self._find_latest_log(run.get("run_id"), "collect_all")
    if not log_file or not log_file.exists():
        return

    current_store: str | None = None
    current_account: str | None = None
    current_brand: str | None = None
    pending_section: dict | None = None  # {store, account, section_name, line}

    for raw_line in log_file.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        section_match = SECTION_START_RE.search(line)
        if section_match:
            current_account = section_match.group("account").strip()
            current_store = section_match.group("store").strip()
            section_brand = (section_match.group("brand") or "").strip() or None
            if section_brand:
                current_brand = section_brand
                # brand known from section line — create/update row directly
                row = self._ensure_store(store_map, current_store, current_account, brand=current_brand)
                row.started_at = row.started_at or _parse_log_datetime(line) or run.get("start_date")
                row.current_section = section_match.group("section").strip()
                self._mark_section_started(row, row.current_section)
                self._touch_row(row, line)
                self._append_live_log(live_logs, f"{current_brand}/{current_store}", line)
                pending_section = None
            else:
                current_brand = None  # brand unknown until first brand-tagged line
                pending_section = {"store": current_store, "account": current_account,
                                   "section_name": section_match.group("section").strip(), "line": line}
            continue

        login_fail = LOGIN_FAIL_RE.search(line)
        if login_fail:
            current_account = login_fail.group("account").strip()
            current_store = login_fail.group("store").strip()
            current_brand = None
            row = self._ensure_store(store_map, current_store, current_account, brand=current_brand)
            row.orders_status = "실패"
            row.marketing_status = "실패"
            row.now_status = "실패"
            row.woori_status = "실패"
            row.ad_status = "실패"
            row.note = "로그인 실패"
            self._touch_row(row, line)
            self._append_live_log(live_logs, current_store, line, level="WARN")
            continue

        account_activity = _is_account_activity_message(line)
        if account_activity:
            current_account, account_message = account_activity
            row = self._find_row_by_account(store_map, current_account)
            if row:
                current_store = row.store_name
                current_brand = row.brand
                if not row.current_section:
                    row.current_section = "브라우저 준비"
                if row.now_status == "대기":
                    row.now_status = "수집중"
                if row.marketing_status == "대기":
                    row.marketing_status = "수집중"
                self._touch_row(row, line)
                self._append_live_log(live_logs, f"{row.brand}/{row.store_name}", line)
                continue

        store_name = self._extract_store_name(line, current_store)
        if not store_name:
            continue
        extracted_brand = _extract_brand_from_line(line)
        if extracted_brand:
            current_brand = extracted_brand
        row = self._ensure_store(store_map, store_name, current_account, brand=current_brand)
        store_key = f"{row.brand}/{store_name}"

        # Apply buffered section start now that we know the brand
        if pending_section and current_brand:
            ps = pending_section
            pending_section = None
            ps_row = self._ensure_store(store_map, ps["store"], ps["account"], brand=current_brand)
            ps_row.started_at = ps_row.started_at or _parse_log_datetime(ps["line"]) or run.get("start_date")
            ps_row.current_section = ps["section_name"]
            self._mark_section_started(ps_row, ps["section_name"])
            self._touch_row(ps_row, ps["line"])
            self._append_live_log(live_logs, f"{current_brand}/{ps['store']}", ps["line"])

        if "DB_Beamin_01_now.py" in line or _line_contains_any(line, ("baemin_now.csv", "now 저장 완료", "now 수집 완료")):
            if _line_contains_any(line, ("저장 완료", "수집 완료")):
                row.now_status = "완료"
            elif _line_contains_any(line, ("데이터 없음", "빈값", "없음")):
                row.now_status = "건너뜀"
            elif _line_contains_any(line, ("오류", "실패", "timeout")):
                row.now_status = "실패"
                row.note = "now 수집 실패"
            else:
                row.now_status = _merge_status(row.now_status, "수집중")
            if row.now_status in {"수집중", "실패"}:
                row.marketing_status = _merge_status(row.marketing_status, row.now_status)
            elif row.marketing_status == "대기":
                row.marketing_status = "완료"
            self._touch_row(row, line)
            self._append_live_log(live_logs, store_key, line)
            continue

        if "DB_Beamin_04_orders.py" in line or _line_contains_any(line, ("저장 완료(정상)", "저장 완료(취소)", "정상 주문 없음", "주문취소 없음")):
            if _line_contains_any(line, ("저장 완료(정상)", "저장 완료(취소)", "정상 주문 없음", "주문취소 없음")):
                row.orders_status = "완료"
            elif _line_contains_any(line, ("오류", "실패", "timeout")):
                row.orders_status = "실패"
                row.note = "주문서 수집 실패"
            else:
                row.orders_status = _merge_status(row.orders_status, "수집중")
            self._touch_row(row, line)
            self._append_live_log(live_logs, store_key, line)
            continue

        if "DB_Beamin_02_woori_shop_click.py" in line or _line_contains_any(line, ("CMG", "우리매장", "데이터없음", "woori_shop_click.csv", "우가클 데이터없음(정상)")):
            if _line_contains_any(line, ("저장 완료", "데이터없음", "우가클 데이터없음(정상)")):
                next_status = "완료" if "저장 완료" in line else "건너뜀"
                row.woori_status = next_status
                if row.marketing_status != "실패":
                    row.marketing_status = next_status
                if "데이터없음" in line or "우가클 데이터없음(정상)" in line:
                    row.note = "우가클 데이터없음(정상)" if "우가클 데이터없음(정상)" in line else "우가클 데이터없음"
            elif _line_contains_any(line, ("실패", "오류", "미감지")):
                row.woori_status = "실패"
                row.marketing_status = "실패"
                row.note = "우가클 수집 실패"
            else:
                row.woori_status = _merge_status(row.woori_status, "수집중")
                row.marketing_status = _merge_status(row.marketing_status, "수집중")
            self._touch_row(row, line)
            self._append_live_log(live_logs, store_key, line)
            continue

        if "DB_Beamin_05_ad_funnel.py" in line or _line_contains_any(line, ("광고 없음", "광고 funnel", "지표 추출", "ad_funnel")):
            if _line_contains_any(line, ("저장 완료", "광고 없음")):
                next_status = "완료" if "저장 완료" in line else "건너뜀"
                if row.ad_status != "실패":
                    row.ad_status = next_status
                if row.marketing_status != "실패":
                    row.marketing_status = next_status
                if "광고 없음" in line:
                    row.note = "광고 없음"
            elif _line_contains_any(line, ("지표 추출 오류", "실패", "timeout")) and "광고 없음" not in line:
                row.ad_status = "실패"
                row.marketing_status = "실패"
                row.note = "광고 수집 실패"
            else:
                row.ad_status = _merge_status(row.ad_status, "수집중")
                row.marketing_status = _merge_status(row.marketing_status, "수집중")
            self._touch_row(row, line)
            self._append_live_log(live_logs, store_key, line)
            continue

        if "저장 완료:" in line:
            if "광고" in row.current_section or "funnel" in row.current_section.lower():
                row.ad_status = "완료"
            elif "order" in row.current_section.lower() or "주문" in row.current_section:
                row.orders_status = "완료"
            elif "now" in row.current_section.lower():
                row.now_status = "완료"
            else:
                row.woori_status = "완료"
            if row.marketing_status != "실패":
                row.marketing_status = "완료"
            self._touch_row(row, line)
            self._append_live_log(live_logs, store_key, line)
            continue

        if "다음 계정까지" in line:
            self._touch_row(row, line)
            self._append_live_log(live_logs, store_key, line)

    if task_state in COMPLETED_STATES:
        self._finalize_collect_all_states(store_map, failed=(task_state in FAILED_STATES))


def _parser_parse_collect_shop_change(
    self: StoreProgressParser,
    run: dict[str, Any],
    store_map: dict[str, StoreRow],
    live_logs: list[dict[str, Any]],
    *,
    task_state: str | None,
) -> None:
    log_file = self._find_latest_log(run.get("run_id"), "collect_shop_change")
    if not log_file or not log_file.exists():
        return

    current_store: str | None = None
    current_account: str | None = None
    for raw_line in log_file.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if "변경이력 수집 시작:" in line:
            label = line.split("변경이력 수집 시작:", 1)[1].strip()
            current_store, current_account, current_brand = self._parse_change_label(label)
            row = self._ensure_store(store_map, current_store, current_account, brand=current_brand)
            row.marketing_status = _merge_status(row.marketing_status, "수집중")
            row.change_status = _merge_status(row.change_status, "수집중")
            row.current_section = "변경이력"
            self._touch_row(row, line)
            self._append_live_log(live_logs, f"{row.brand}/{current_store}", line)
            continue

        if "store 변경이력 수집 완료:" in line:
            label = line.split("store 변경이력 수집 완료:", 1)[1].strip()
            store_name, account_id, brand = self._parse_change_label(label)
            row = self._ensure_store(store_map, store_name, account_id, brand=brand)
            if row.marketing_status != "실패":
                row.marketing_status = "완료"
            if row.change_status != "실패":
                row.change_status = "완료"
            self._touch_row(row, line)
            self._append_live_log(live_logs, f"{row.brand}/{store_name}", line)
            continue

        if ("데이터 없음" in line or "정상 빈값 신호:" in line) and current_store:
            row = self._ensure_store(store_map, current_store, current_account)
            if row.marketing_status != "실패":
                row.marketing_status = "건너뜀"
            if row.change_status != "실패":
                row.change_status = "건너뜀"
            row.note = "변경이력 데이터없음"
            self._touch_row(row, line)
            self._append_live_log(live_logs, f"{row.brand}/{current_store}", line)

    if task_state in COMPLETED_STATES:
        for row in store_map.values():
            if row.marketing_status == "수집중":
                row.marketing_status = "완료" if task_state == "success" else row.marketing_status
            if row.change_status == "수집중":
                row.change_status = "완료" if task_state == "success" else row.change_status


def _parser_finalize_collect_all_states(self: StoreProgressParser, store_map: dict[str, StoreRow], *, failed: bool) -> None:
    for row in store_map.values():
        if row.orders_status == "수집중":
            row.orders_status = "실패" if failed else "완료"
        if row.marketing_status == "수집중":
            row.marketing_status = "실패" if failed else "완료"
        if row.now_status == "수집중":
            row.now_status = "실패" if failed else "완료"
        if row.woori_status == "수집중":
            row.woori_status = "실패" if failed else "완료"
        if row.ad_status == "수집중":
            row.ad_status = "실패" if failed else "완료"
        if row.ended_at is None and row.started_at is not None:
            row.ended_at = row.last_touched_at or _utc_now()


StoreProgressParser._mark_section_started = _parser_mark_section_started
StoreProgressParser._parse_collect_all = _parser_parse_collect_all
StoreProgressParser._parse_collect_shop_change = _parser_parse_collect_shop_change
StoreProgressParser._finalize_collect_all_states = _parser_finalize_collect_all_states


def _parser_extract_store_name(self: StoreProgressParser, line: str, fallback: str | None) -> str | None:
    match = BRAND_STORE_RE.search(line)
    if match:
        return match.group("store").strip()
    match = STORE_EQUALS_RE.search(line)
    if match:
        return match.group("store").strip()
    if fallback:
        return fallback
    return None


def _extract_brand_from_line(line: str) -> str | None:
    m = BRAND_STORE_RE.search(line)
    return m.group("brand").strip() if m else None


StoreProgressParser._extract_store_name = _parser_extract_store_name


def _normalize_store_name(store_name: str) -> str:
    value = (store_name or "").strip()
    if not value:
        return value
    if "→" in value:
        value = value.split("→", 1)[0].strip()
    if "store=" in value:
        match = BRAND_STORE_RE.search(value)
        if match:
            value = match.group("store").strip()
        else:
            match = STORE_EQUALS_RE.search(value)
            if match:
                value = match.group("store").strip()
    for prefix in ("도리당 ", "나홀로 ", "우리동네 ", "브랜드 "):
        if value.startswith(prefix):
            value = value[len(prefix):].strip()
    return value


def _parser_ensure_store(
    self: StoreProgressParser,
    store_map: dict[str, StoreRow],
    store_name: str,
    account_id: str | None,
    brand: str | None = None,
) -> StoreRow:
    plain = _normalize_store_name(store_name)
    if brand:
        key = f"{brand}/{plain}"
    else:
        matches = [k for k in store_map if k == plain or k.endswith(f"/{plain}")]
        key = matches[0] if matches else plain
    if key not in store_map:
        b = key.split("/", 1)[0] if "/" in key else (brand or "도리당")
        store_map[key] = StoreRow(seq=len(store_map) + 1, store_name=plain, account_id=account_id or "-", brand=b)
    row = store_map[key]
    if account_id and row.account_id == "-":
        row.account_id = account_id
    if brand:
        row.brand = brand
    return row


def _parser_find_row_by_account(
    self: StoreProgressParser,
    store_map: dict[str, StoreRow],
    account_id: str,
) -> StoreRow | None:
    for row in store_map.values():
        if row.account_id == account_id:
            return row
    return None


StoreProgressParser._ensure_store = _parser_ensure_store
StoreProgressParser._find_row_by_account = _parser_find_row_by_account


def _parser_seed_store_map(
    self: StoreProgressParser,
    account_list: list[dict[str, Any]],
    ad_stores: list[dict[str, Any]] | None = None,
) -> dict[str, StoreRow]:
    store_map: dict[str, StoreRow] = {}
    seq = 1
    for account in account_list:
        full_name = str(account.get("store_name") or account.get("store") or "")
        parts = full_name.split()
        if len(parts) >= 2:
            brand, store = parts[0], " ".join(parts[1:])
        elif parts:
            brand, store = "도리당", _normalize_store_name(parts[0])
        else:
            continue
        account_id = str(account.get("account_id") or account.get("id") or "-")
        key = f"{brand}/{store}"
        if key not in store_map:
            store_map[key] = StoreRow(seq=seq, store_name=store, account_id=account_id, brand=brand)
            seq += 1
    for item in (ad_stores or []):
        if not isinstance(item, dict):
            continue
        brand = str(item.get("brand") or "")
        store = str(item.get("store") or "")
        account_id = str(item.get("account_id") or "-")
        if not brand or not store:
            continue
        key = f"{brand}/{store}"
        if key not in store_map:
            store_map[key] = StoreRow(seq=seq, store_name=store, account_id=account_id, brand=brand)
            seq += 1
    return store_map


StoreProgressParser._seed_store_map = _parser_seed_store_map


class SnapshotBuilder:
    def __init__(
        self,
        repository: AirflowMetadataRepository,
        *,
        parser: StoreProgressParser | None = None,
    ):
        self.repository = repository
        self.parser = parser or StoreProgressParser()

    def build(self, *, now: datetime | None = None) -> dict[str, Any]:
        now = now or _utc_now()
        active_run = self.repository.get_latest_active_run()
        completed_run = self.repository.get_latest_completed_run()
        recent_runs = self.repository.get_recent_success_runs(limit=RECENT_BASELINE_LIMIT)

        focus_run = active_run or completed_run
        stores, live_logs, current_store = self.parser.build_store_progress(focus_run, now=now)
        last_completed_store = self._find_last_completed_store(stores)
        summary = self._build_summary(
            active_run=active_run,
            completed_run=completed_run,
            stores=stores,
            recent_runs=recent_runs,
            current_store=current_store,
            last_completed_store=last_completed_store,
            now=now,
        )
        return {
            "generated_at": _to_iso(now),
            "dag_id": DAG_ID,
            "has_active_run": active_run is not None,
            "active_run": self._serialize_run(active_run, now=now),
            "latest_completed_run": self._serialize_run(completed_run, now=now),
            "overall_status": summary["overall_status"],
            "overall_duration_sec": summary["elapsed_sec"],
            "avg_duration_sec": summary["avg_duration_sec"],
            "warning_count": summary["warning_count"],
            "failure_count": summary["failure_count"],
            "progress_count": summary["progress_count"],
            "completed_count": summary["completed_count"],
            "total_stores": len(stores),
            "current_store": current_store,
            "last_completed_store": last_completed_store,
            "overview": summary,
            "stores": stores,
            "live_logs": live_logs,
        }

    def _build_summary(
        self,
        *,
        active_run: dict[str, Any] | None,
        completed_run: dict[str, Any] | None,
        stores: list[dict[str, Any]],
        recent_runs: list[dict[str, Any]],
        current_store: str | None,
        last_completed_store: str | None,
        now: datetime,
    ) -> dict[str, Any]:
        focus_run = active_run or completed_run
        success_count = 0
        warning_count = 0
        failure_count = 0
        progress_count = 0
        partial_count = 0

        for store in stores:
            status = self._overall_store_status(store)
            if status == "실패":
                failure_count += 1
            elif status == "수집중":
                progress_count += 1
            elif status == "경고":
                warning_count += 1
            elif status == "부분완료":
                warning_count += 1
                partial_count += 1
            elif status == "완료":
                success_count += 1

        completed_count = success_count + warning_count + failure_count
        overall_status = "IDLE"
        if active_run:
            overall_status = "RUNNING"
        elif completed_run and completed_run.get("state") == "success":
            overall_status = "WARN" if (failure_count or warning_count) else "SUCCESS"
        elif failure_count:
            overall_status = "FAILED"
        elif warning_count:
            overall_status = "WARN"
        elif completed_run:
            overall_status = str(completed_run.get("state") or "FAILED").upper()

        avg_duration = _mean(
            [
                _duration_seconds(run.get("start_date"), run.get("end_date"), now=now)
                for run in recent_runs
                if run.get("run_id") != (focus_run or {}).get("run_id")
            ]
        )

        elapsed = None
        if focus_run:
            elapsed = _duration_seconds(focus_run.get("start_date"), focus_run.get("end_date"), now=now)
        expected_finished_at = None
        if active_run and avg_duration is not None and active_run.get("start_date") is not None:
            expected_finished_at = active_run["start_date"] + timedelta(seconds=max(avg_duration, 0.0))

        return {
            "dag_id": DAG_ID,
            "overall_status": overall_status,
            "dag_state": (focus_run or {}).get("state") or "idle",
            "run_id": (focus_run or {}).get("run_id"),
            "active_run_id": (active_run or {}).get("run_id"),
            "started_at": _to_iso((focus_run or {}).get("start_date")),
            "active_started_at": _to_iso((active_run or {}).get("start_date")),
            "latest_completed_at": _to_iso((completed_run or {}).get("end_date")),
            "elapsed_sec": elapsed,
            "elapsed_text": _format_duration(elapsed),
            "avg_duration_sec": avg_duration,
            "avg_duration_text": _format_duration(avg_duration),
            "expected_finished_at": _to_iso(expected_finished_at),
            "success_count": success_count,
            "warning_count": warning_count,
            "partial_count": partial_count,
            "failure_count": failure_count,
            "progress_count": progress_count,
            "completed_count": completed_count,
            "total_stores": len(stores),
            "current_store": current_store or "-",
            "last_completed_store": last_completed_store or "-",
            "generated_at": _to_iso(now),
        }

    def _overall_store_status(self, store: dict[str, Any]) -> str:
        if "실패" in {store.get("orders_status"), store.get("marketing_status")}:
            return "실패"
        if "수집중" in {store.get("orders_status"), store.get("marketing_status")}:
            return "수집중"
        note = str(store.get("note") or "")
        if "불일치" in note or "retry" in note.lower():
            return "경고"
        if store.get("orders_status") in DONE_BADGES and store.get("marketing_status") in DONE_BADGES:
            return "완료"
        if store.get("orders_status") in DONE_BADGES or store.get("marketing_status") in DONE_BADGES:
            return "부분완료"
        return "대기"

    def _find_last_completed_store(self, stores: list[dict[str, Any]]) -> str | None:
        completed = [
            store
            for store in stores
            if self._overall_store_status(store) in {"완료", "부분완료"} and store.get("updated_at")
        ]
        if not completed:
            return None
        latest = max(completed, key=lambda store: str(store.get("updated_at") or ""))
        return str(latest.get("store_key") or "")

    def _serialize_run(self, run: dict[str, Any] | None, *, now: datetime) -> dict[str, Any] | None:
        if not run:
            return None
        return {
            "dag_id": run.get("dag_id"),
            "run_id": run.get("run_id"),
            "state": run.get("state"),
            "started_at": _to_iso(run.get("start_date")),
            "ended_at": _to_iso(run.get("end_date")),
            "duration_sec": _duration_seconds(run.get("start_date"), run.get("end_date"), now=now),
        }


def render_dashboard_html(
    *,
    dag_id: str,
    overview: dict[str, Any],
    stores: list[dict[str, Any]],
    live_logs: list[dict[str, Any]],
    generated_at: datetime,
) -> str:
    payload = {
        "dag_id": dag_id,
        "snapshot_id": overview.get("snapshot_id", 0),
        "overview": overview,
        "stores": stores,
        "live_logs": live_logs,
        "generated_at": _to_iso(generated_at),
    }
    payload_json = _safe_json_embed(payload)
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>배민 매장 대시보드</title>
  <style>
    :root {{
      --bg: #0f1116;
      --panel: #171a23;
      --panel-2: #11141b;
      --line: #2a3140;
      --text: #eef2f7;
      --muted: #aeb8c7;
      --green: #2bd46d;
      --green-bg: rgba(43, 212, 109, 0.18);
      --red: #ff5f5f;
      --red-bg: rgba(255, 95, 95, 0.18);
      --yellow: #f9c74f;
      --yellow-bg: rgba(249, 199, 79, 0.18);
      --blue: #4da3ff;
      --blue-bg: rgba(77, 163, 255, 0.16);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Apple SD Gothic Neo", "Malgun Gothic", sans-serif;
      background: radial-gradient(circle at top left, #1d2533 0%, var(--bg) 42%);
      color: var(--text);
    }}
    .page {{ padding: 20px; }}
    .header, .summary, .layout > section {{
      background: rgba(23, 26, 35, 0.94);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: 0 18px 40px rgba(0, 0, 0, 0.25);
    }}
    .header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 18px 22px;
      margin-bottom: 16px;
    }}
    .title h1 {{
      margin: 0;
      font-size: 24px;
      font-weight: 700;
    }}
    .title p {{
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 14px;
    }}
    .header-meta {{
      text-align: right;
      color: var(--muted);
      font-size: 13px;
    }}
    .summary {{
      padding: 12px 16px;
      margin-bottom: 16px;
    }}
    .progress {{
      height: 10px;
      background: #202635;
      border-radius: 999px;
      overflow: hidden;
      margin-bottom: 10px;
    }}
    .progress-bar {{
      height: 100%;
      width: calc((var(--done) / max(var(--total), 1)) * 100%);
      background: linear-gradient(90deg, #1ac965, #5ce3a1);
    }}
    .summary-line {{
      display: flex;
      gap: 16px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 14px;
    }}
    .summary-line strong {{ color: var(--text); }}
    .layout {{
      display: grid;
      grid-template-columns: minmax(640px, 1.8fr) minmax(320px, 1fr) minmax(240px, 0.7fr);
      gap: 16px;
    }}
    section {{ min-height: 680px; }}
    .stores-panel, .logs-panel, .filter-panel {{ padding: 14px; }}
    .panel-title {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      margin-bottom: 12px;
    }}
    .panel-title h2 {{
      margin: 0;
      font-size: 16px;
    }}
    .panel-title span {{
      color: var(--muted);
      font-size: 13px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }}
    thead th {{
      text-align: left;
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
    }}
    tbody td {{
      padding: 12px 8px;
      border-bottom: 1px solid rgba(42, 49, 64, 0.65);
      vertical-align: middle;
      font-size: 14px;
    }}
    tbody tr.selected {{
      background: rgba(43, 212, 109, 0.14);
      box-shadow: inset 0 0 0 1px rgba(43, 212, 109, 0.75);
    }}
    .store-name {{
      font-weight: 700;
      font-size: 16px;
    }}
    .store-account {{
      margin-top: 4px;
      color: var(--muted);
      font-size: 12px;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 48px;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
    }}
    .badge-완료 {{ color: var(--green); background: var(--green-bg); }}
    .badge-부분완료 {{ color: var(--yellow); background: var(--yellow-bg); }}
    .badge-건너뜀 {{ color: var(--yellow); background: var(--yellow-bg); }}
    .badge-수집중 {{ color: var(--blue); background: var(--blue-bg); }}
    .badge-실패 {{ color: var(--red); background: var(--red-bg); }}
    .badge-대기 {{ color: #c4cad5; background: rgba(196, 202, 213, 0.14); }}
    .mono {{ font-variant-numeric: tabular-nums; font-family: Consolas, monospace; }}
    .store-section {{ margin-top: 3px; color: var(--blue); font-size: 11px; }}
    .store-brand-tag {{ color: #a78bfa; font-size: 11px; font-weight: 700; background: rgba(167,139,250,0.14); padding: 1px 5px; border-radius: 4px; margin-right: 3px; }}
    .store-brand-tag--doridang {{ color: #ff9f43; background: rgba(255,159,67,0.18); }}
    .store-brand-tag--nahollo {{ color: #a78bfa; background: rgba(167,139,250,0.14); }}
    .badge-재시도완료 {{ color: #f9c74f; background: rgba(249,199,79,0.18); }}
    .badge-재시도실패 {{ color: #ff9c9c; background: rgba(255,95,95,0.18); }}
    .note-cell {{ max-width: 0; }}
    .note-text {{
      color: var(--muted);
      font-size: 12px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .note-last {{
      color: #586172;
      font-size: 11px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      margin-top: 2px;
    }}
    .detail-card {{
      border: 1px solid var(--line);
      background: var(--panel-2);
      border-radius: 12px;
      padding: 12px;
      margin-bottom: 12px;
      display: grid;
      gap: 10px;
    }}
    .detail-head {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
    }}
    .detail-title {{
      font-size: 16px;
      font-weight: 700;
    }}
    .detail-sub {{
      color: var(--muted);
      font-size: 12px;
      margin-top: 4px;
    }}
    .detail-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
    }}
    .detail-item {{
      border: 1px solid rgba(42, 49, 64, 0.65);
      border-radius: 10px;
      padding: 8px 10px;
      background: rgba(23, 26, 35, 0.72);
    }}
    .detail-item strong {{
      display: block;
      font-size: 11px;
      color: var(--muted);
      margin-bottom: 6px;
      font-weight: 600;
    }}
    .detail-last {{
      font-size: 12px;
      color: var(--muted);
      line-height: 1.5;
      word-break: break-word;
    }}
    .log-list {{
      height: 560px;
      overflow: auto;
      background: #0b0d12;
      border: 1px solid #1f2634;
      border-radius: 12px;
      padding: 10px 14px;
      font-family: "Malgun Gothic", "Apple SD Gothic Neo", sans-serif;
      font-size: 13px;
      line-height: 1.6;
    }}
    .log-line {{ margin-bottom: 2px; color: #d8e2f0; display: flex; gap: 6px; align-items: baseline; }}
    .log-line .time {{ color: #8998ae; font-family: Consolas, monospace; font-size: 12px; flex-shrink: 0; }}
    .log-line .store {{ color: #7ee081; font-size: 12px; font-weight: 700; flex-shrink: 0; min-width: 48px; }}
    .log-line .msg {{ word-break: break-all; }}
    .log-line.warn {{ color: #ff9c9c; }}
    .log-line.section {{ color: #a8c7fa; font-weight: 700; border-top: 1px solid #1f2634; margin-top: 6px; padding-top: 4px; }}
    .filters {{
      display: grid;
      gap: 10px;
      margin-bottom: 12px;
    }}
    input[type="search"] {{
      width: 100%;
      padding: 10px 12px;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: var(--panel-2);
      color: var(--text);
      outline: none;
    }}
    .chip-row {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}
    .chip {{
      border: 1px solid var(--line);
      background: var(--panel-2);
      color: var(--muted);
      padding: 7px 10px;
      border-radius: 999px;
      cursor: pointer;
      font-size: 12px;
    }}
    .chip.active {{
      background: rgba(43, 212, 109, 0.14);
      color: var(--text);
      border-color: rgba(43, 212, 109, 0.55);
    }}
    .store-list {{
      height: 660px;
      overflow: auto;
      display: grid;
      gap: 8px;
    }}
    .store-pill {{
      border: 1px solid var(--line);
      background: var(--panel-2);
      border-radius: 10px;
      padding: 10px 12px;
      cursor: pointer;
      font-size: 13px;
      color: var(--text);
      text-align: left;
    }}
    .store-pill.selected {{
      background: rgba(43, 212, 109, 0.14);
      border-color: rgba(43, 212, 109, 0.8);
    }}
    .store-pill.brand-doridang {{ box-shadow: inset 3px 0 0 rgba(255,159,67,0.95); }}
    .store-pill.brand-nahollo {{ box-shadow: inset 3px 0 0 rgba(167,139,250,0.95); }}
    .store-pill-name {{
      display: block;
      font-weight: 700;
    }}
    .store-pill-account {{
      display: block;
      margin-top: 4px;
      color: var(--muted);
      font-size: 12px;
    }}
    @media (max-width: 1500px) {{
      .layout {{ grid-template-columns: 1fr; }}
      section {{ min-height: auto; }}
      .log-list, .store-list {{ height: 420px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="header">
      <div class="title">
        <h1>배민 매장 실시간 대시보드</h1>
        <p>{html.escape(dag_id)} · 선택 매장 {overview.get("total_stores", 0)}개 · 실시간 연결 준비중</p>
      </div>
      <div class="header-meta">
        <div>상태: <strong>{html.escape(str(overview.get("overall_status", "-")))}</strong></div>
        <div>현재 처리중: <strong>{html.escape(str(overview.get("current_store", "-")))}</strong></div>
        <div>생성 시각: {html.escape(_to_kst_display(generated_at))}</div>
        <div>예상 완료: {html.escape(_to_kst_display(overview.get("expected_finished_at")))}</div>
      </div>
    </div>

    <div class="summary" style="--done:{overview.get("completed_count", 0)}; --total:{max(overview.get("total_stores", 0), 1)};">
      <div class="progress"><div class="progress-bar"></div></div>
      <div class="summary-line">
        <span>진행 <strong>{overview.get("completed_count", 0)} / {overview.get("total_stores", 0)}</strong></span>
        <span>성공 <strong>{overview.get("success_count", 0)}</strong></span>
        <span>부분완료 <strong>{overview.get("partial_count", 0)}</strong></span>
        <span>실패 <strong>{overview.get("failure_count", 0)}</strong></span>
        <span>수집중 <strong>{overview.get("progress_count", 0)}</strong></span>
        <span>경과 <strong>{html.escape(str(overview.get("elapsed_text", "-")))}</strong></span>
        <span>평균 <strong>{html.escape(str(overview.get("avg_duration_text", "-")))}</strong></span>
        <span>최근 완료 <strong>{html.escape(_to_kst_display(overview.get("latest_completed_at")))}</strong></span>
      </div>
    </div>

    <div class="layout">
      <section class="stores-panel">
        <div class="panel-title">
          <h2>매장별 상태</h2>
          <span>now / 우가클 / 주문서 / 마케팅 / 변경이력 / 비고 / 재수집 / 소요</span>
        </div>
        <table id="stores-table">
          <thead>
            <tr>
              <th style="width: 56px;">#</th>
              <th style="width: 38%;">매장명 / 계정</th>
              <th style="width: 86px;">now</th>
              <th style="width: 86px;">우가클</th>
              <th style="width: 92px;">주문서</th>
              <th style="width: 92px;">마케팅</th>
              <th style="width: 92px;">변경이력</th>
              <th style="width: 180px;">비고</th>
              <th style="width: 96px;">재수집</th>
              <th style="width: 92px;">소요</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </section>

      <section class="logs-panel">
        <div class="detail-card" id="selected-store-card"></div>
        <div class="panel-title">
          <h2>라이브 로그</h2>
          <div style="display:flex;gap:8px;align-items:center;">
            <span id="log-count">{len(live_logs)}줄</span>
            <button id="copy-errors-btn" title="WARN 로그 클립보드 복사" style="padding:3px 8px;border-radius:6px;border:1px solid var(--line);background:var(--panel-2);color:var(--muted);cursor:pointer;font-size:12px;">오류 복사</button>
          </div>
        </div>
        <div class="log-list" id="log-list"></div>
      </section>

      <section class="filter-panel">
        <div class="panel-title">
          <h2>매장 선택</h2>
          <span>검색 / 상태 필터</span>
        </div>
        <div class="filters">
          <input id="search" type="search" placeholder="매장명 검색..." />
          <div class="chip-row" id="filters">
            <button class="chip active" data-filter="ALL">전체</button>
            <button class="chip" data-filter="수집중">수집중</button>
            <button class="chip" data-filter="실패">실패</button>
            <button class="chip" data-filter="완료">완료</button>
            <button class="chip" data-filter="부분완료">부분완료</button>
            <button class="chip" data-filter="대기">대기</button>
          </div>
        </div>
        <div class="store-list" id="store-list"></div>
      </section>
    </div>
  </div>

  <script id="snapshot-json" type="application/json">{payload_json}</script>
  <script>
    let snapshot = JSON.parse(document.getElementById("snapshot-json").textContent);
    let connectionMode = "초기 로딩";
    let eventSource = null;
    let pollingTimer = null;
    const tbody = document.querySelector("#stores-table tbody");
    const logList = document.getElementById("log-list");
    const storeList = document.getElementById("store-list");
    const searchInput = document.getElementById("search");
    const filterButtons = Array.from(document.querySelectorAll("#filters .chip"));
    const subtitleEl = document.querySelector(".title p");
    const headerStrongEls = Array.from(document.querySelectorAll(".header-meta strong"));
    const headerMetaEls = Array.from(document.querySelectorAll(".header-meta div"));
    const summaryBox = document.querySelector(".summary");
    const summaryLine = document.querySelector(".summary-line");
    const detailCard = document.getElementById("selected-store-card");
    const STORE_SELECTION_KEY = "db_beamin_macro_selected_store";

    function esc(s) {{
      return String(s ?? "-").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    }}

    function fmtKst(value) {{
      if (!value) return "-";
      const parsed = new Date(value);
      if (Number.isNaN(parsed.getTime())) return String(value);
      return parsed.toLocaleString("ko-KR", {{
        timeZone: "Asia/Seoul",
        hour12: false,
      }});
    }}

    function readSelectedStore() {{
      try {{
        return window.localStorage.getItem(STORE_SELECTION_KEY) || "";
      }} catch (_err) {{
        return "";
      }}
    }}

    function writeSelectedStore(storeKey) {{
      selectedStore = storeKey || "";
      try {{
        window.localStorage.setItem(STORE_SELECTION_KEY, selectedStore);
      }} catch (_err) {{
      }}
    }}

    function pickDefaultStore() {{
      const currentStore = snapshot.overview?.current_store;
      const lastCompletedStore = snapshot.overview?.last_completed_store;
      return (
        (snapshot.stores.find((store) => store.store_key === selectedStore) || {{}}).store_key ||
        (snapshot.stores.find((store) => store.store_key === currentStore) || {{}}).store_key ||
        (snapshot.stores.find((store) => store.store_key === lastCompletedStore) || {{}}).store_key ||
        (snapshot.stores.find((store) => store.selected) || snapshot.stores[0] || {{}}).store_key ||
        ""
      );
    }}

    let selectedStore = readSelectedStore() || "";
    let activeFilter = "ALL";

    function badge(status) {{
      return `<span class="badge badge-${{status}}">${{status}}</span>`;
    }}

    function retryCell(status) {{
      if (!status || status === "-") return `<span class="store-account">-</span>`;
      return `<span class="badge badge-${{status}}">${{status}}</span>`;
    }}

    function overallStoreStatus(store) {{
      return store.store_state || "대기";
    }}

    function visibleStores() {{
      const keyword = searchInput.value.trim().toLowerCase();
      return snapshot.stores.filter((store) => {{
        const status = overallStoreStatus(store);
        const statusOk = activeFilter === "ALL" || status === activeFilter;
        const text = `${{store.store_name}} ${{store.account_id}} ${{store.note}}`.toLowerCase();
        const keywordOk = !keyword || text.includes(keyword);
        return statusOk && keywordOk;
      }});
    }}

    function noteSummary(store) {{
      const note = store.note || "-";
      const parts = String(note).split("|").map((s) => s.trim()).filter(Boolean);
      const errs = parts.filter((p) => p.includes("실패") || p.includes("오류"));
      if (errs.length) return esc(errs.join(" | "));
      const infos = parts.filter((p) => p && p !== "-");
      if (infos.length) return esc(infos[infos.length - 1]);
      if (store.store_state === "완료") return "전체 완료";
      if (store.store_state === "부분완료") return "한쪽 수집 완료";
      if (store.store_state === "수집중") return "수집 진행중";
      return "-";
    }}

    function selectedStoreRow() {{
      return snapshot.stores.find((store) => store.store_key === selectedStore) || snapshot.stores[0] || null;
    }}

    function logMatchesSelected(entry) {{
      if (!selectedStore) return true;
      if (entry.store_key === selectedStore) return true;
      if (entry.store_key && selectedStore.endsWith(`/${{entry.store_key}}`)) return true;
      return false;
    }}

    function logsForView() {{
      return snapshot.live_logs.filter((entry) => logMatchesSelected(entry)).slice(-80);
    }}

    function updateOverview() {{
      const overview = snapshot.overview || {{}};
      if (subtitleEl) {{
        subtitleEl.textContent = `${{snapshot.dag_id || "DB_Beamin_Macro_Dags"}} · 선택 매장 ${{overview.total_stores || 0}}개 · ${{connectionMode}}`;
      }}
      if (headerStrongEls[0]) headerStrongEls[0].textContent = overview.overall_status || "-";
      if (headerStrongEls[1]) headerStrongEls[1].textContent = overview.current_store || "-";
      if (headerMetaEls[2]) headerMetaEls[2].textContent = `생성 시각: ${{fmtKst(snapshot.generated_at)}}`;
      if (headerMetaEls[3]) headerMetaEls[3].textContent = `예상 완료: ${{fmtKst(overview.expected_finished_at)}}`;
      if (summaryBox) {{
        summaryBox.style.setProperty("--done", overview.completed_count || 0);
        summaryBox.style.setProperty("--total", Math.max(overview.total_stores || 0, 1));
      }}
      if (summaryLine) {{
        summaryLine.innerHTML = `
          <span>진행 <strong>${{overview.completed_count || 0}} / ${{overview.total_stores || 0}}</strong></span>
          <span>성공 <strong>${{overview.success_count || 0}}</strong></span>
          <span>부분완료 <strong>${{overview.partial_count || 0}}</strong></span>
          <span>실패 <strong>${{overview.failure_count || 0}}</strong></span>
          <span>수집중 <strong>${{overview.progress_count || 0}}</strong></span>
          <span>경과 <strong>${{overview.elapsed_text || "-"}}</strong></span>
          <span>평균 <strong>${{overview.avg_duration_text || "-"}}</strong></span>
          <span>최근 완료 <strong>${{fmtKst(overview.latest_completed_at)}}</strong></span>
        `;
      }}
    }}

    function renderSelectedStoreCard() {{
      const store = selectedStoreRow();
      if (!store) {{
        detailCard.innerHTML = `<div class="detail-last">선택된 매장이 없습니다.</div>`;
        return;
      }}
      detailCard.innerHTML = `
        <div class="detail-head">
          <div>
            <div class="detail-title">${{esc(store.store_name)}}</div>
            <div class="detail-sub">${{esc(store.brand || "-")}} · ${{esc(store.account_id || "-")}}</div>
          </div>
          <div>${{badge(overallStoreStatus(store))}}</div>
        </div>
        <div class="detail-grid">
          <div class="detail-item"><strong>주문서</strong>${{badge(store.orders_status || "대기")}}</div>
          <div class="detail-item"><strong>마케팅</strong>${{badge(store.marketing_status || "대기")}}</div>
          <div class="detail-item"><strong>now</strong>${{badge(store.now_status || "대기")}}</div>
          <div class="detail-item"><strong>우가클</strong>${{badge(store.woori_status || "대기")}}</div>
          <div class="detail-item"><strong>광고</strong>${{badge(store.ad_status || "대기")}}</div>
          <div class="detail-item"><strong>변경이력</strong>${{badge(store.change_status || "대기")}}</div>
        </div>
        <div class="detail-last">마지막 갱신: ${{esc(store.updated_at || "-")}} · 소요: ${{esc(store.elapsed_text || "-")}}</div>
        <div class="detail-last">현재 구간: ${{esc(store.current_section || "-")}}</div>
        <div class="detail-last">마지막 이벤트: ${{esc(store.last_event || "-")}}</div>
      `;
    }}

    function renderStores() {{
      const stores = visibleStores();
      tbody.innerHTML = stores.map((store) => `
        <tr class="${{store.store_key === selectedStore ? "selected" : ""}}" data-store="${{store.store_key}}">
          <td class="mono">${{store.seq}}</td>
          <td>
            <div class="store-name">${{esc(store.store_name)}}</div>
            <div class="store-account"><span class="store-brand-tag store-brand-tag-${{(store.brand || "").toLowerCase() === "도리당" ? "-doridang" : ((store.brand || "").toLowerCase() === "나홀로" ? "-nahollo" : "")}}">${{esc(store.brand || "")}}</span> ${{esc(store.account_id)}}</div>
            ${{store.current_section && store.current_section !== "-" ? `<div class="store-section">• ${{esc(store.current_section)}}</div>` : ""}}
          </td>
          <td>${{badge(store.now_status || "대기")}}</td>
          <td>${{badge(store.woori_status || "대기")}}</td>
          <td>${{badge(store.orders_status || "대기")}}</td>
          <td>${{badge(store.marketing_status || "대기")}}</td>
          <td>${{badge(store.change_status || "대기")}}</td>
          <td class="note-cell"><span class="note-text">${{noteSummary(store)}}</span></td>
          <td>${{retryCell(store.retry_status || "-")}}</td>
          <td class="mono">${{esc(store.elapsed_text || "-")}}</td>
        </tr>
      `).join("");
      Array.from(tbody.querySelectorAll("tr")).forEach((row) => {{
        row.addEventListener("click", () => {{
          writeSelectedStore(row.dataset.store);
          renderAll();
        }});
      }});
    }}

    function renderLogs() {{
      const logs = logsForView();
      document.getElementById("log-count").textContent = `${{logs.length}}줄`;
      logList.innerHTML = logs.map((entry) => {{
        const sec = entry.message && entry.message.startsWith("===");
        return `<div class="log-line ${{entry.level === "WARN" ? "warn" : ""}} ${{sec ? "section" : ""}}">
          <span class="time">${{esc(entry.time)}}</span>
          <span class="store">${{esc(entry.store_name)}}</span>
          <span class="msg">${{esc(entry.message)}}</span>
        </div>`;
      }}).join("") || '<div class="log-line">선택한 매장 로그가 없습니다.</div>';
      logList.scrollTop = logList.scrollHeight;
    }}

    function renderStoreList() {{
      const stores = visibleStores();
      storeList.innerHTML = stores.map((store) => `
        <button class="store-pill brand-${{(store.brand || "").toLowerCase() === "도리당" ? "doridang" : ((store.brand || "").toLowerCase() === "나홀로" ? "nahollo" : "other")}} ${{store.store_key === selectedStore ? "selected" : ""}}" data-store="${{store.store_key}}">
          <span class="store-pill-name">${{store.store_name}}</span>
          <span class="store-pill-account">${{store.brand || "-"}} · ${{store.store_state || "-"}} · ${{store.account_id || "-"}}</span>
        </button>
      `).join("");
      Array.from(storeList.querySelectorAll(".store-pill")).forEach((button) => {{
        button.addEventListener("click", () => {{
          writeSelectedStore(button.dataset.store);
          renderAll();
        }});
      }});
    }}

    function applySnapshot(nextSnapshot) {{
      snapshot = nextSnapshot;
      if (!snapshot.stores.some((store) => store.store_key === selectedStore)) {{
        writeSelectedStore(pickDefaultStore());
      }}
      renderAll();
    }}

    function isFileMode() {{
      return window.location.protocol === "file:";
    }}

    async function refreshSnapshot() {{
      if (isFileMode()) {{
        window.location.reload();
        return;
      }}
      const resp = await fetch(`/api/db-beamin-macro/snapshot?ts=${{Date.now()}}`, {{
        cache: "no-store",
        headers: {{ "Cache-Control": "no-cache" }},
      }});
      if (!resp.ok) return;
      applySnapshot(await resp.json());
    }}

    function ensurePolling(reasonText = "5초 자동 새로고침") {{
      connectionMode = reasonText;
      if (!pollingTimer) {{
        pollingTimer = window.setInterval(() => {{
          refreshSnapshot().catch(() => {{}});
        }}, 5000);
      }}
      updateOverview();
    }}

    function connectRealtime() {{
      if (isFileMode()) {{
        connectionMode = "로컬 파일 모드";
        updateOverview();
        return;
      }}
      if (!("EventSource" in window)) {{
        ensurePolling();
        return;
      }}
      eventSource = new EventSource(`/api/db-beamin-macro/events?since=${{snapshot.snapshot_id || 0}}`);
      eventSource.addEventListener("snapshot", (event) => {{
        connectionMode = "실시간 연결";
        applySnapshot(JSON.parse(event.data));
      }});
      eventSource.onopen = () => {{
        connectionMode = "실시간 연결";
        updateOverview();
      }};
      eventSource.onerror = () => {{
        if (eventSource) {{
          eventSource.close();
          eventSource = null;
        }}
        ensurePolling();
      }};
    }}

    function renderAll() {{
      selectedStore = selectedStore || pickDefaultStore();
      updateOverview();
      renderSelectedStoreCard();
      renderStores();
      renderStoreList();
      renderLogs();
    }}

    searchInput.addEventListener("input", () => renderAll());
    filterButtons.forEach((button) => {{
      button.addEventListener("click", () => {{
        activeFilter = button.dataset.filter;
        filterButtons.forEach((item) => item.classList.toggle("active", item === button));
        renderAll();
      }});
    }});

    renderAll();
    connectRealtime();

    document.getElementById("copy-errors-btn").addEventListener("click", () => {{
      const errors = logsForView()
        .filter((e) => e.level === "WARN" || /실패|오류|timeout|error/i.test(e.message))
        .map((e) => `[${{e.time}}] ${{e.store_name}}: ${{e.message}}`)
        .join("\\n");
      navigator.clipboard.writeText(errors || "오류 로그 없음").then(() => {{
        const btn = document.getElementById("copy-errors-btn");
        const prev = btn.textContent;
        btn.textContent = "복사 완료!";
        setTimeout(() => {{ btn.textContent = prev; }}, 1500);
      }});
    }});
  </script>
</body>
</html>"""


class SnapshotStore:
    def __init__(self, root: Path | str = DASHBOARD_DB):
        self.paths = DashboardPaths(Path(root))
        self.paths.root.mkdir(parents=True, exist_ok=True)

    def persist(self, snapshot: dict[str, Any]) -> None:
        self.paths.html_path.write_text(str(snapshot.get("html") or ""), encoding="utf-8")
        payload = dict(snapshot)
        html_payload = payload.pop("html", None)
        self.paths.json_path.write_text(_safe_json(payload), encoding="utf-8")
        self.paths.store_progress_path.write_text(_safe_json(payload.get("stores", [])), encoding="utf-8")
        self.paths.live_log_path.write_text(_safe_json(payload.get("live_logs", [])), encoding="utf-8")
        if html_payload is None:
            logger.warning("Snapshot persisted without HTML payload.")


class DashboardService:
    def __init__(
        self,
        *,
        builder: SnapshotBuilder | None = None,
        store: SnapshotStore | None = None,
        refresh_seconds: int = DEFAULT_REFRESH_SECONDS,
    ):
        self.builder = builder or SnapshotBuilder(AirflowMetadataRepository())
        self.store = store or SnapshotStore()
        self.refresh_seconds = refresh_seconds
        self.snapshot_lock = threading.Lock()
        self.snapshot_condition = threading.Condition(self.snapshot_lock)
        self.current_snapshot: dict[str, Any] = {}
        self.snapshot_id = 0
        self.stop_event = threading.Event()
        self.worker: threading.Thread | None = None

    def start(self) -> None:
        if self.worker and self.worker.is_alive():
            return
        self.worker = threading.Thread(target=self._run_loop, name="beamin-dashboard", daemon=True)
        self.worker.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.worker:
            self.worker.join(timeout=5)

    def get_snapshot(self) -> dict[str, Any]:
        with self.snapshot_lock:
            return dict(self.current_snapshot)

    def wait_for_snapshot(self, after_snapshot_id: int, timeout: float = SSE_KEEPALIVE_SECONDS) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout
        with self.snapshot_condition:
            while True:
                current_id = int(self.current_snapshot.get("snapshot_id") or 0)
                if current_id > after_snapshot_id:
                    return dict(self.current_snapshot)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self.snapshot_condition.wait(timeout=remaining)

    def refresh_once(self) -> dict[str, Any]:
        now = _utc_now()
        snapshot = self.builder.build(now=now)
        with self.snapshot_condition:
            self.snapshot_id += 1
            snapshot["snapshot_id"] = self.snapshot_id
            snapshot["overview"]["snapshot_id"] = self.snapshot_id
        snapshot["html"] = render_dashboard_html(
            dag_id=snapshot["dag_id"],
            overview=snapshot["overview"],
            stores=snapshot["stores"],
            live_logs=snapshot["live_logs"],
            generated_at=now,
        )
        self.store.persist(snapshot)
        with self.snapshot_condition:
            self.current_snapshot = dict(snapshot)
            self.snapshot_condition.notify_all()
        return snapshot

    def _run_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.refresh_once()
            except Exception:
                logger.exception("Dashboard refresh failed.")
            self.stop_event.wait(self.refresh_seconds)


class DashboardRequestHandler(BaseHTTPRequestHandler):
    service: DashboardService

    def do_GET(self) -> None:  # noqa: N802
        snapshot = self.service.get_snapshot()
        parsed = urlparse(self.path)
        route = parsed.path

        if route == "/health":
            self._send_text("ok")
            return
        if route in {"/", f"/{HTML_FILENAME}", "/db-beamin-macro"}:
            html_payload = snapshot.get("html") or "<html><body>dashboard warming up</body></html>"
            self._send_bytes(html_payload.encode("utf-8"), content_type="text/html; charset=utf-8")
            return
        if route == "/api/db-beamin-macro/summary":
            self._send_json(snapshot.get("overview") or {})
            return
        if route == "/api/db-beamin-macro/stores":
            self._send_json(snapshot.get("stores") or [])
            return
        if route == "/api/db-beamin-macro/logs":
            self._send_json(snapshot.get("live_logs") or [])
            return
        if route == "/api/db-beamin-macro/snapshot":
            payload = dict(snapshot)
            payload.pop("html", None)
            self._send_json(payload)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "not found")

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)

    def _send_text(self, text: str, *, status: int = 200) -> None:
        self._send_bytes(text.encode("utf-8"), content_type="text/plain; charset=utf-8", status=status)

    def _send_json(self, payload: Any, *, status: int = 200) -> None:
        self._send_bytes(_safe_json(payload).encode("utf-8"), content_type="application/json; charset=utf-8", status=status)

    def _send_bytes(self, payload: bytes, *, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def run_server(*, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, refresh_seconds: int = DEFAULT_REFRESH_SECONDS) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    service = DashboardService(refresh_seconds=refresh_seconds)
    service.refresh_once()
    service.start()

    DashboardRequestHandler.service = service
    server = ThreadingHTTPServer((host, port), DashboardRequestHandler)
    logger.info("Starting DB_Beamin_Macro_Dags dashboard on http://%s:%s/db-beamin-macro", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Dashboard server interrupted.")
    finally:
        service.stop()
        server.server_close()


if __name__ == "__main__":
    run_server()
