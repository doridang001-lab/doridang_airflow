import importlib.util
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKER_PATH = REPO_ROOT / "scripts" / "repair_baemin_acl_queue_wsl.py"


def _load_worker():
    spec = importlib.util.spec_from_file_location("baemin_acl_wsl_worker", WORKER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_acl_worker_repairs_approved_file_and_writes_ack(tmp_path, monkeypatch):
    worker = _load_worker()
    analytics = tmp_path / "analytics"
    target = analytics / "baemin_macro" / "orders" / "sample.csv"
    target.parent.mkdir(parents=True)
    target.write_text("value\n1\n", encoding="utf-8")
    queue = tmp_path / "queue"
    queue.mkdir()
    request_id = "approved"
    files = ["baemin_macro/orders/sample.csv"]
    (queue / f"{request_id}.request.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "request_id": request_id,
                "files": files,
            }
        ),
        encoding="utf-8",
    )

    repaired = []
    monkeypatch.setattr(worker, "_repair_acl", repaired.append)
    request_count, completed = worker.process_queue(
        queue,
        analytics,
        tmp_path / "worker.log",
    )

    assert (request_count, completed) == (1, 1)
    assert repaired == [target.resolve()]
    ack = json.loads((queue / f"{request_id}.done.json").read_text(encoding="utf-8"))
    assert ack["ok"] is True
    assert ack["files"] == files


def test_acl_worker_rejects_path_traversal_and_keeps_request(tmp_path):
    worker = _load_worker()
    analytics = tmp_path / "analytics"
    analytics.mkdir()
    queue = tmp_path / "queue"
    queue.mkdir()
    request_id = "traversal"
    request_path = queue / f"{request_id}.request.json"
    request_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "request_id": request_id,
                "files": ["../outside.csv"],
            }
        ),
        encoding="utf-8",
    )

    request_count, completed = worker.process_queue(
        queue,
        analytics,
        tmp_path / "worker.log",
    )

    assert (request_count, completed) == (1, 0)
    assert request_path.exists()
    assert not (queue / f"{request_id}.done.json").exists()
    assert "Rejected relative path" in (tmp_path / "worker.log").read_text(
        encoding="utf-8"
    )


def test_hidden_launcher_uses_wsl_without_powershell():
    launcher = (
        REPO_ROOT / "scripts" / "run_baemin_acl_repair_wsl_hidden.vbs"
    ).read_text(encoding="utf-8")

    assert "wsl.exe -d UbuntuCodex" in launcher
    assert "shell.Run(command, 0, True)" in launcher
    assert "powershell" not in launcher.lower()


def test_bash_worker_enables_wsl_interop_before_python():
    worker = (
        REPO_ROOT / "scripts" / "repair_baemin_acl_queue_wsl.sh"
    ).read_text(encoding="utf-8")

    assert "WSLInterop" in worker
    assert "/proc/sys/fs/binfmt_misc/register" in worker
    assert "repair_baemin_acl_queue_wsl.py" in worker
