#!/usr/bin/env python3
"""Process Baemin ACL repair requests from inside WSL."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime
from pathlib import Path, PurePosixPath


SCHEMA_VERSION = 1
ALLOWED_ROOT = "baemin_macro"
ALLOWED_SUFFIXES = {".csv", ".parquet"}
ICACLS = Path("/mnt/c/Windows/System32/icacls.exe")


def _default_analytics_root() -> Path:
    configured = os.environ.get("ONEDRIVE_ROOT_WSL", "").strip()
    if configured:
        return Path(configured) / "data" / "analytics"

    user_root = Path("/mnt/c/Users")
    candidates = sorted(
        path / "data" / "analytics"
        for path in user_root.glob("*/OneDrive -*")
        if path.is_dir()
    )
    if not candidates:
        raise RuntimeError("OneDrive analytics root not found")
    return candidates[0]


def _write_log(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    with log_path.open("a", encoding="utf-8") as stream:
        stream.write(f"{timestamp} {message}\n")


def _write_json_atomic(value: dict[str, object], path: Path) -> None:
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)


def _resolve_approved_target(analytics_root: Path, relative_path: str) -> Path:
    if not relative_path.strip():
        raise ValueError("Empty relative path")

    relative = PurePosixPath(relative_path.replace("\\", "/"))
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or any(char in relative_path for char in "*?[]")
    ):
        raise ValueError(f"Rejected relative path: {relative_path}")
    if not relative.parts or relative.parts[0].lower() != ALLOWED_ROOT:
        raise ValueError(f"Path is outside baemin_macro: {relative_path}")
    if relative.suffix.lower() not in ALLOWED_SUFFIXES:
        raise ValueError(f"Rejected extension: {relative_path}")

    root = analytics_root.resolve()
    target = (root / Path(*relative.parts)).resolve()
    if not target.is_relative_to(root):
        raise ValueError(f"Path is outside analytics: {relative_path}")
    if not target.is_file():
        raise FileNotFoundError(f"ACL repair target missing: {target}")
    return target


def _windows_path(path: Path) -> str:
    result = subprocess.run(
        ["wslpath", "-w", str(path)],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip()


def _repair_acl(target: Path) -> None:
    result = subprocess.run(
        [str(ICACLS), _windows_path(target), "/reset"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"icacls failed(exit={result.returncode}): {message}")
    with target.open("rb") as stream:
        stream.read(1)


def process_queue(queue_root: Path, analytics_root: Path, log_path: Path) -> tuple[int, int]:
    queue_root.mkdir(parents=True, exist_ok=True)
    requests = sorted(
        queue_root.glob("*.request.json"),
        key=lambda path: (path.stat().st_mtime_ns, path.name),
    )
    completed = 0

    for request_path in requests:
        request_id = request_path.name.removesuffix(".request.json")
        done_path = queue_root / f"{request_id}.done.json"
        if done_path.exists():
            continue

        try:
            request = json.loads(request_path.read_text(encoding="utf-8"))
            if request.get("schema_version") != SCHEMA_VERSION:
                raise ValueError(
                    f"Unsupported schema_version: {request.get('schema_version')}"
                )
            if request.get("request_id") != request_id:
                raise ValueError(
                    f"request_id mismatch: {request.get('request_id')} / {request_id}"
                )

            relative_files = [str(path) for path in request.get("files", [])]
            if not relative_files:
                raise ValueError("ACL repair file list is empty")

            for relative_file in relative_files:
                target = _resolve_approved_target(analytics_root, relative_file)
                _repair_acl(target)

            _write_json_atomic(
                {
                    "schema_version": SCHEMA_VERSION,
                    "request_id": request_id,
                    "ok": True,
                    "files": relative_files,
                    "repaired_at": datetime.now().astimezone().isoformat(),
                },
                done_path,
            )
            completed += 1
            _write_log(
                log_path,
                f"ACL repair completed request={request_id} files={len(relative_files)}",
            )
        except Exception as exc:
            _write_log(
                log_path,
                f"ACL repair failed request={request_id} error={exc}",
            )

    return len(requests), completed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--queue-root",
        type=Path,
        default=Path("/mnt/c/Local_DB/baemin_acl_repair_queue"),
    )
    parser.add_argument("--analytics-root", type=Path)
    parser.add_argument(
        "--log-path",
        type=Path,
        default=Path("/mnt/c/Local_DB/logs/baemin_acl_repair.log"),
    )
    args = parser.parse_args()

    analytics_root = args.analytics_root or _default_analytics_root()
    request_count, completed = process_queue(
        args.queue_root,
        analytics_root,
        args.log_path,
    )
    print(
        f"baemin ACL queue processed: requests={request_count} completed={completed}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
