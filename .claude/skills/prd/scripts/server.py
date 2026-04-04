"""PRD Generator Server — Flask app, file IPC mode (runner.py handles LLM calls)."""

import argparse
import json
import os
import re
import signal
import socket
import sys
import threading
import time
import webbrowser
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
TEMP_DIR = SCRIPT_DIR / "temp"
TEMP_DIR.mkdir(exist_ok=True)
# 세션 시작 시 이전 temp 파일 전체 초기화
for _f in TEMP_DIR.glob("request_*.json"):
    _f.unlink(missing_ok=True)
for _f in ["export_complete", "last_activity", "questions.txt", "prd.txt", "spec.txt", "trd.txt", "child.txt", "chat.txt"]:
    (_tmp := TEMP_DIR / _f).unlink(missing_ok=True)

sys.path.insert(0, str(SCRIPT_DIR))

try:
    from flask import Flask, Response, jsonify, request, send_file
except ImportError:
    print("flask not installed. Run: py -3.12 -m pip install flask", flush=True)
    sys.exit(1)

# ── App ───────────────────────────────────────────────────────────────────────
STATIC_DIR = SCRIPT_DIR / "static"
app = Flask(__name__, static_folder=str(STATIC_DIR))

MODEL = "claude-haiku-4-5-20251001"


def detect_planning_dir() -> Path:
    home = Path.home()
    for suffix in ["OneDrive - 주식회사 도리당", "OneDrive - 도리당", "OneDrive"]:
        candidate = home / suffix / "data" / "planning"
        if (home / suffix).exists():
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
    fallback = Path("C:/airflow/prd-output")
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


state: dict = {
    "answers": None,
    "prd": None,
    "feature_spec": None,
    "trd_md": None,
    "cwd": str(detect_planning_dir()),
    "project_name": "product",
    "description": "",
    "port": None,
    "questions": None,
    "spec_path": None,
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def normalize_cwd(p: str) -> Path:
    if p.startswith("/mnt/") and len(p) > 5:
        drive = p[5]
        rest = p[6:] if len(p) > 6 else ""
        return Path(f"{drive.upper()}:/{rest}")
    m = re.match(r"^/([a-zA-Z])(/.*)$", p)
    if m:
        return Path(f"{m.group(1).upper()}:{m.group(2)}")
    return Path(p)


def call_llm_bg(prompt: str, on_done, req_type: str = "generic"):
    """request_{type}.json 파일 작성 → Claude Code가 generator.py로 응답 → {type}.txt 읽어 on_done 호출."""
    def _run():
        req_file = TEMP_DIR / f"request_{req_type}.json"
        req_file.write_text(json.dumps({"prompt": prompt}, ensure_ascii=False), encoding="utf-8")
        print(f"LLM_REQUEST: {req_type}", flush=True)
        resp_file = TEMP_DIR / f"{req_type}.txt"
        for _ in range(600):
            time.sleep(1)
            if resp_file.exists():
                try:
                    content = resp_file.read_text(encoding="utf-8")
                    resp_file.unlink(missing_ok=True)
                    req_file.unlink(missing_ok=True)
                    on_done(content)
                    return
                except Exception as e:
                    print(f"RESP_READ_ERROR: {e}", flush=True)
        print(f"LLM_TIMEOUT: {req_type}", flush=True)
        on_done("")
    threading.Thread(target=_run, daemon=True).start()


def strip_json(text: str) -> str:
    raw = re.sub(r"^```json\s*", "", text.strip(), flags=re.MULTILINE)
    raw = re.sub(r"```\s*$", "", raw.strip(), flags=re.MULTILINE)
    return raw.strip()


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_file(str(STATIC_DIR / "app.html"))


@app.route("/api/state")
def get_state():
    return jsonify({
        "project_name": state["project_name"],
        "description": state["description"],
        "cwd": state["cwd"],
        "has_prd": state["prd"] is not None,
        "has_spec": state["feature_spec"] is not None,
        "has_trd": state["trd_md"] is not None,
        "spec_path": state.get("spec_path"),
        "prd_path": state.get("prd_path"),
        "trd_path": state.get("trd_path"),
    })


# ── Questions endpoint ────────────────────────────────────────────────────────
@app.route("/api/questions")
def get_questions():
    (TEMP_DIR / "last_activity").touch()
    if state["questions"] is not None:
        return jsonify({"questions": state["questions"]})
    if not state.get("_questions_pending"):
        state["_questions_pending"] = True
        desc_line = f": {state['description']}" if state["description"] else ""
        prompt = (
            f"제품 '{state['project_name']}'{desc_line}\n\n"
            "5~7개 설문 질문을 JSON 배열로 반환. 마지막은 반드시 q_additional (textarea, required:false).\n"
            "필드: id, title, hint, type(select|checkbox|radio|textarea), required, options(select/checkbox/radio만).\n\n"
            "JSON 배열만 출력하세요."
        )

        def on_done(text):
            state["_questions_pending"] = False
            try:
                state["questions"] = json.loads(strip_json(text))
            except json.JSONDecodeError:
                state["questions"] = []
            print("QUESTIONS_RECEIVED", flush=True)

        call_llm_bg(prompt, on_done, "questions")
    return jsonify({"mode": "direct", "poll": "/api/poll/questions"})


@app.route("/api/poll/questions")
def poll_questions():
    if state["questions"] is not None:
        return jsonify({"ready": True, "questions": state["questions"]})
    return jsonify({"ready": False})


# ── Generate endpoints ─────────────────────────────────────────────────────────
@app.route("/api/generate/prd", methods=["POST"])
def generate_prd():
    (TEMP_DIR / "last_activity").touch()
    data = request.json or {}
    state["answers"] = data.get("answers", {})
    state["prd"] = None

    from prompts.prd_prompt import build_prd_prompt
    prompt = build_prd_prompt(state["answers"], state["project_name"])

    def on_done(text):
        state["prd"] = text
        cwd = normalize_cwd(state["cwd"])
        cwd.mkdir(parents=True, exist_ok=True)
        prd_path = cwd / f"{state['project_name']}-prd.md"
        prd_path.write_text(text, encoding="utf-8")
        state["prd_path"] = str(prd_path)
        print(f"PRD_RECEIVED (saved: {prd_path})", flush=True)

    call_llm_bg(prompt, on_done, "prd")
    return jsonify({"mode": "direct", "poll": "/api/poll/prd"})


@app.route("/api/generate/spec", methods=["POST"])
def generate_spec():
    (TEMP_DIR / "last_activity").touch()
    data = request.json or {}
    prd_override = data.get("prd")
    if isinstance(prd_override, str) and prd_override.strip():
        state["prd"] = prd_override.strip()

    if not state["prd"]:
        return jsonify({"error": "PRD를 먼저 생성하세요."}), 400
    state["feature_spec"] = None

    from prompts.feature_spec_prompt import build_spec_prompt
    prompt = build_spec_prompt(state["prd"], state.get("answers") or {})

    def on_done(text):
        try:
            state["feature_spec"] = json.loads(strip_json(text))
        except json.JSONDecodeError:
            state["feature_spec"] = {"requirements": [], "_raw": text}
        try:
            cwd = normalize_cwd(state["cwd"])
            cwd.mkdir(parents=True, exist_ok=True)
            f = cwd / f"{state['project_name']}-feature-spec.json"
            f.write_text(json.dumps(state["feature_spec"], ensure_ascii=False, indent=2), encoding="utf-8")
            state["spec_path"] = str(f)
            print(f"SPEC_SAVED: {f}", flush=True)
        except Exception as e:
            print(f"SPEC_SAVE_ERROR: {e}", flush=True)
        print("SPEC_RECEIVED", flush=True)

    call_llm_bg(prompt, on_done, "spec")
    return jsonify({"mode": "direct", "poll": "/api/poll/spec"})


@app.route("/api/generate/trd", methods=["POST"])
def generate_trd():
    (TEMP_DIR / "last_activity").touch()
    data = request.json or {}
    prd_override = data.get("prd")
    if isinstance(prd_override, str) and prd_override.strip():
        state["prd"] = prd_override.strip()

    if not state["prd"]:
        return jsonify({"error": "PRD를 먼저 생성하세요."}), 400
    state["trd_md"] = None

    from prompts.trd_prompt import build_trd_prompt
    prompt = build_trd_prompt(state["prd"], state["feature_spec"] or {})

    def on_done(text):
        state["trd_md"] = text
        cwd = normalize_cwd(state["cwd"])
        cwd.mkdir(parents=True, exist_ok=True)
        trd_path = cwd / f"{state['project_name']}-trd.md"
        trd_path.write_text(text, encoding="utf-8")
        state["trd_path"] = str(trd_path)
        print(f"TRD_RECEIVED (saved: {trd_path})", flush=True)
        (TEMP_DIR / "export_complete").touch()
        print("\nEXPORT_COMPLETE (auto)", flush=True)
        threading.Timer(1.5, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()

    call_llm_bg(prompt, on_done, "trd")
    return jsonify({"mode": "direct", "poll": "/api/poll/trd"})


@app.route("/api/generate/child", methods=["POST"])
def generate_child():
    data = request.json or {}
    state["_child_result"] = None

    from prompts.feature_spec_prompt import build_child_prompt
    prompt = build_child_prompt(
        data.get("parent", {}),
        data.get("depth", 1),
        state["prd"][:600] if state["prd"] else "",
    )

    def on_done(text):
        try:
            state["_child_result"] = json.loads(strip_json(text))
        except json.JSONDecodeError:
            state["_child_result"] = []
        print("CHILD_RECEIVED", flush=True)

    call_llm_bg(prompt, on_done, "child")
    return jsonify({"mode": "direct", "poll": "/api/poll/child"})


@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.json or {}
    state["_chat_reply"] = None

    from prompts.feature_spec_prompt import build_chat_prompt
    prompt = build_chat_prompt(
        data.get("message", ""),
        state["feature_spec"] or {},
        state["prd"][:1000] if state["prd"] else "",
    )

    def on_done(text):
        m = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL)
        if m:
            try:
                state["feature_spec"] = json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
        state["_chat_reply"] = text
        print("CHAT_RECEIVED", flush=True)

    call_llm_bg(prompt, on_done, "chat")
    return jsonify({"mode": "direct", "poll": "/api/poll/chat"})


# ── Receive endpoints (generator.py → server) ────────────────────────────────
@app.route("/api/receive/questions", methods=["POST"])
def receive_questions():
    data = request.json or {}
    content = data.get("content", "")
    try:
        state["questions"] = json.loads(strip_json(content))
    except json.JSONDecodeError:
        state["questions"] = []
    print("QUESTIONS_RECEIVED", flush=True)
    return jsonify({"ok": True})


@app.route("/api/receive/prd", methods=["POST"])
def receive_prd():
    data = request.json or {}
    content = data.get("content", "")
    if content.startswith("FILE_SAVED:"):
        state["prd_path"] = content[len("FILE_SAVED:"):]
        try:
            state["prd"] = Path(state["prd_path"]).read_text(encoding="utf-8")
        except Exception:
            state["prd"] = ""
    else:
        state["prd"] = content
    print("PRD_RECEIVED", flush=True)
    return jsonify({"ok": True})


@app.route("/api/receive/spec", methods=["POST"])
def receive_spec():
    data = request.json or {}
    content = data.get("content", "")
    try:
        state["feature_spec"] = json.loads(strip_json(content))
    except json.JSONDecodeError:
        state["feature_spec"] = {"requirements": [], "_raw": content}
    try:
        cwd = normalize_cwd(state["cwd"])
        cwd.mkdir(parents=True, exist_ok=True)
        f = cwd / f"{state['project_name']}-feature-spec.json"
        f.write_text(json.dumps(state["feature_spec"], ensure_ascii=False, indent=2), encoding="utf-8")
        state["spec_path"] = str(f)
        print(f"SPEC_SAVED: {f}", flush=True)
    except Exception as e:
        print(f"SPEC_SAVE_ERROR: {e}", flush=True)
    print("SPEC_RECEIVED", flush=True)
    return jsonify({"ok": True})


@app.route("/api/receive/trd", methods=["POST"])
def receive_trd():
    data = request.json or {}
    content = data.get("content", "")
    if content.startswith("FILE_SAVED:"):
        state["trd_path"] = content[len("FILE_SAVED:"):]
        state["trd_md"] = content
    else:
        state["trd_md"] = content
    print("TRD_RECEIVED", flush=True)
    (TEMP_DIR / "export_complete").touch()
    print("\nEXPORT_COMPLETE (auto)", flush=True)
    threading.Timer(1.5, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()
    return jsonify({"ok": True})


@app.route("/api/receive/child", methods=["POST"])
def receive_child():
    data = request.json or {}
    content = data.get("content", "")
    try:
        children = json.loads(strip_json(content))
    except json.JSONDecodeError:
        children = []
    state["_child_result"] = children
    print("CHILD_RECEIVED", flush=True)
    return jsonify({"ok": True})


@app.route("/api/receive/chat", methods=["POST"])
def receive_chat():
    data = request.json or {}
    content = data.get("content", "")
    m = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
    if m:
        try:
            state["feature_spec"] = json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    state["_chat_reply"] = content
    print("CHAT_RECEIVED", flush=True)
    return jsonify({"ok": True})


# ── Poll endpoints (browser polls until ready) ────────────────────────────────
@app.route("/api/poll/prd")
def poll_prd():
    if state["prd"] is not None:
        return jsonify({"ready": True, "content": state["prd"]})
    return jsonify({"ready": False})


@app.route("/api/poll/spec")
def poll_spec():
    if state["feature_spec"] is not None:
        return jsonify({"ready": True, "spec": state["feature_spec"]})
    return jsonify({"ready": False})


@app.route("/api/poll/trd")
def poll_trd():
    if state["trd_md"] is not None:
        return jsonify({"ready": True, "content": state["trd_md"]})
    return jsonify({"ready": False})


@app.route("/api/poll/child")
def poll_child():
    children = state.pop("_child_result", None)
    if children is not None:
        return jsonify({"ready": True, "children": children})
    return jsonify({"ready": False})


@app.route("/api/poll/chat")
def poll_chat():
    reply = state.get("_chat_reply")
    spec = state.get("feature_spec")
    if reply is not None:
        state.pop("_chat_reply", None)
        return jsonify({"ready": True, "content": reply, "spec": spec})
    return jsonify({"ready": False})


# ── Export ────────────────────────────────────────────────────────────────────
@app.route("/api/export", methods=["POST"])
def export_files():
    data = request.json or {}
    prd_text = data.get("prd") or state["prd"] or ""
    spec_data = data.get("spec") or state["feature_spec"]
    trd_text = data.get("trd") or state["trd_md"] or ""

    name = state["project_name"]
    cwd = normalize_cwd(state["cwd"])
    cwd.mkdir(parents=True, exist_ok=True)
    written = []

    if prd_text and not prd_text.startswith("FILE_SAVED:"):
        f = cwd / f"{name}-prd.md"
        f.write_text(prd_text, encoding="utf-8")
        written.append(str(f))
    elif state.get("prd_path"):
        written.append(state["prd_path"])

    if spec_data:
        f = cwd / f"{name}-feature-spec.json"
        f.write_text(json.dumps(spec_data, ensure_ascii=False, indent=2), encoding="utf-8")
        written.append(str(f))

    if trd_text and not trd_text.startswith("FILE_SAVED:"):
        f = cwd / f"{name}-trd.md"
        f.write_text(trd_text, encoding="utf-8")
        written.append(str(f))
    elif state.get("trd_path"):
        written.append(state["trd_path"])

    (TEMP_DIR / "export_complete").touch()
    print("\nEXPORT_COMPLETE", flush=True)
    for fp in written:
        print(f"  {fp}", flush=True)

    threading.Timer(1.2, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()
    return jsonify({"success": True, "files": written})


# ── Session persistence (cross-port) ─────────────────────────────────────────
SESSIONS_FILE = detect_planning_dir() / "prd-sessions.json"


@app.route("/api/sessions")
def api_get_sessions():
    try:
        return jsonify(json.loads(SESSIONS_FILE.read_text(encoding="utf-8")))
    except Exception:
        return jsonify([])


@app.route("/api/sessions", methods=["POST"])
def api_save_sessions():
    data = request.json or []
    try:
        SESSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        SESSIONS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify({"ok": True})


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cwd", default=".")
    parser.add_argument("--name", default="product")
    parser.add_argument("--description", default="")
    args = parser.parse_args()

    state["project_name"] = (args.name or "product").strip() or "product"
    state["description"] = (args.description or "").strip()

    # 프로젝트 서브폴더: {base}/{project_name}/
    if args.cwd and args.cwd != ".":
        base = normalize_cwd(args.cwd)
    else:
        base = detect_planning_dir()
    proj_dir = base / state["project_name"]
    proj_dir.mkdir(parents=True, exist_ok=True)
    state["cwd"] = str(proj_dir)

    port = find_free_port()
    state["port"] = port
    url = f"http://localhost:{port}"

    print(f"PRD_PORT: {port}", flush=True)
    print(f"PRD_CWD: {state['cwd']}", flush=True)
    print(f"PRD Generator: {url}", flush=True)
    print(f"프로젝트: {state['project_name']}  저장경로: {state['cwd']}", flush=True)

    threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
