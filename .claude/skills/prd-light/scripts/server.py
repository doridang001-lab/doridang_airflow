"""PRD-Light Server.

Flow:
  Phase 0: description input
  Phase 1: AI-generated questions → user answers
  Phase 2: PRD review (features.md = PRD markdown)
  Phase 3: Feature spec (features.md = spec + mermaid + tree)

features.md is the single source of truth.
tree.json is always derived by Python parser (no AI needed).
"""

import argparse
import json
import os
import re
import socket
import sys
import threading
import time
import webbrowser
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
TEMP_DIR = SCRIPT_DIR / "temp"
TEMP_DIR.mkdir(exist_ok=True)

for _f in TEMP_DIR.glob("request_*.json"):
    _f.unlink(missing_ok=True)
for _name in ["last_activity", "questions.txt", "prd.txt", "spec.txt", "patch.txt", "section.txt"]:
    (TEMP_DIR / _name).unlink(missing_ok=True)

sys.path.insert(0, str(SCRIPT_DIR))

try:
    from flask import Flask, jsonify, request, send_file
except ImportError:
    print("flask not installed. Run: py -3.12 -m pip install flask", flush=True)
    sys.exit(1)

STATIC_DIR = SCRIPT_DIR / "static"
app = Flask(__name__, static_folder=str(STATIC_DIR))


# ── Helpers ───────────────────────────────────────────────────────────────────

def detect_planning_dir() -> Path:
    home = Path.home()
    for suffix in ["OneDrive - 주식회사 도리당", "OneDrive - 도리당", "OneDrive"]:
        if (home / suffix).exists():
            d = home / suffix / "data" / "planning"
            d.mkdir(parents=True, exist_ok=True)
            return d
    fallback = Path("C:/airflow/prd-output")
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def normalize_path(p: str) -> Path:
    if p.startswith("/mnt/") and len(p) > 5:
        drive, rest = p[5], p[6:] if len(p) > 6 else ""
        return Path(f"{drive.upper()}:/{rest}")
    m = re.match(r"^/([a-zA-Z])(/.*)$", p)
    if m:
        return Path(f"{m.group(1).upper()}:{m.group(2)}")
    return Path(p)


def strip_json(text: str) -> str:
    raw = re.sub(r"^```json\s*", "", text.strip(), flags=re.MULTILINE)
    raw = re.sub(r"```\s*$", "", raw.strip(), flags=re.MULTILINE)
    return raw.strip()


def extract_mermaid(features_md: str) -> str:
    m = re.search(r"```mermaid\s*(.*?)\s*```", features_md, re.DOTALL)
    return m.group(1).strip() if m else ""


def detect_phase(features_md: str | None) -> int:
    if not features_md:
        return 0
    if "```mermaid" in features_md and "# 기능 목록" in features_md:
        return 3
    return 2


def parse_features_to_tree(features_md: str, project_name: str) -> dict:
    """Parse structured feature spec (Phase 3) into tree. Python only, no AI."""
    lines = features_md.split("\n")
    root = {"id": "root", "name": project_name, "children": []}
    level_nodes: dict = {0: root}
    current = None

    for line in lines:
        s = line.strip()
        if re.match(r"^## [^#]", s):
            name = re.sub(r"^\d+\.\s*", "", s[3:]).strip()
            idx = len(root["children"]) + 1
            node = {"id": f"f{idx}", "name": name, "description": "", "priority": "Medium", "children": []}
            root["children"].append(node)
            level_nodes = {0: root, 1: node}
            current = node
        elif re.match(r"^### [^#]", s):
            name = re.sub(r"^\d+\.\d+\s*", "", s[4:]).strip()
            parent = level_nodes.get(1)
            if parent:
                idx = len(parent["children"]) + 1
                node = {"id": f"{parent['id']}-{idx}", "name": name, "description": "", "priority": "Medium", "children": []}
                parent["children"].append(node)
                level_nodes[2] = node
                level_nodes.pop(3, None)
                current = node
        elif re.match(r"^#### [^#]", s):
            name = re.sub(r"^\d+\.\d+\.\d+\s*", "", s[5:]).strip()
            parent = level_nodes.get(2)
            if parent:
                idx = len(parent["children"]) + 1
                node = {"id": f"{parent['id']}-{idx}", "name": name, "description": "", "priority": "Low", "children": []}
                parent["children"].append(node)
                level_nodes[3] = node
                current = node
        elif current:
            if "**설명:**" in s:
                current["description"] = s.replace("**설명:**", "").strip()
            elif "**우선순위:**" in s:
                current["priority"] = s.replace("**우선순위:**", "").strip()

    return root


def save_spec(features_md: str):
    """Phase 3: Save feature spec. Regenerates tree.json from markdown."""
    cwd = normalize_path(state["cwd"])
    cwd.mkdir(parents=True, exist_ok=True)
    (cwd / "features.md").write_text(features_md, encoding="utf-8")

    tree = parse_features_to_tree(features_md, state["project_name"])
    (cwd / "tree.json").write_text(json.dumps(tree, ensure_ascii=False, indent=2), encoding="utf-8")

    state["features_md"] = features_md
    state["tree"] = tree
    state["mermaid_code"] = extract_mermaid(features_md)
    state["phase"] = 3
    print(f"SPEC_SAVED: {cwd / 'features.md'}", flush=True)


def save_prd(prd_content: str):
    """Phase 2: Save PRD as features.md (raw markdown, no tree yet)."""
    cwd = normalize_path(state["cwd"])
    cwd.mkdir(parents=True, exist_ok=True)
    (cwd / "features.md").write_text(prd_content, encoding="utf-8")
    state["features_md"] = prd_content
    state["prd"] = prd_content
    state["phase"] = 2
    print(f"PRD_SAVED: {cwd / 'features.md'}", flush=True)


def call_llm_bg(prompt: str, on_done, req_type: str):
    def _run():
        req_file = TEMP_DIR / f"request_{req_type}.json"
        req_file.write_text(json.dumps({"prompt": prompt}, ensure_ascii=False), encoding="utf-8")
        (TEMP_DIR / "last_activity").touch()
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
                    print(f"RESP_READ_ERROR ({req_type}): {e}", flush=True)
        print(f"LLM_TIMEOUT: {req_type}", flush=True)
        on_done("")

    threading.Thread(target=_run, daemon=True).start()


# ── State ─────────────────────────────────────────────────────────────────────

state: dict = {
    "project_name": "product",
    "description": "",
    "phase": 0,          # 0=init, 2=prd, 3=spec
    "questions": None,   # [{id, title, hint}, ...]
    "prd": None,         # PRD markdown
    "features_md": None, # feature spec markdown (Phase 3)
    "tree": None,
    "mermaid_code": "",
    "cwd": str(detect_planning_dir()),
    # pending
    "_q_done": None,
    "_prd_done": None,
    "_spec_done": None,
    "_patch_done": None,
    "_section_done": None,
}


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_file(str(STATIC_DIR / "app.html"))


@app.route("/api/state")
def get_state():
    flat = []
    if state["tree"]:
        flat = _flatten(state["tree"])
    return jsonify({
        "project_name": state["project_name"],
        "description": state["description"],
        "phase": state["phase"],
        "questions": state["questions"],
        "prd": state["prd"],
        "features_md": state["features_md"],
        "tree": state["tree"],
        "mermaid_code": state["mermaid_code"],
        "flat_nodes": flat,
        "cwd": state["cwd"],
    })


def _flatten(node, prefix="", result=None):
    if result is None:
        result = []
    if node["id"] != "root":
        result.append({"id": node["id"], "name": prefix + node["name"],
                        "description": node.get("description", ""),
                        "priority": node.get("priority", "")})
    for c in node.get("children", []):
        _flatten(c, prefix + ("  " if node["id"] != "root" else ""), result)
    return result


# ── Phase 1: Generate questions ───────────────────────────────────────────────

@app.route("/api/questions", methods=["POST"])
def get_questions():
    data = request.json or {}
    description = data.get("description", "").strip()
    if description:
        state["description"] = description

    state["_q_done"] = None
    (TEMP_DIR / "last_activity").touch()

    from prompts.features_prompt import build_questions_prompt
    prompt = build_questions_prompt(state["project_name"], state["description"])

    def on_done(text):
        try:
            qs = json.loads(strip_json(text))
            state["questions"] = qs
            state["_q_done"] = {"ok": True}
        except Exception:
            state["_q_done"] = {"ok": False, "error": f"질문 파싱 실패: {text[:200]}"}
        print("QUESTIONS_DONE", flush=True)

    call_llm_bg(prompt, on_done, "questions")
    return jsonify({"mode": "polling", "poll": "/api/poll/questions"})


@app.route("/api/poll/questions")
def poll_questions():
    result = state.get("_q_done")
    if result is None:
        return jsonify({"ready": False})
    return jsonify({"ready": True, **result, "questions": state["questions"]})


# ── Phase 2: Generate PRD from answers ───────────────────────────────────────

@app.route("/api/prd", methods=["POST"])
def generate_prd():
    data = request.json or {}
    answers = data.get("answers", {})  # {question_title: answer_text}

    state["_prd_done"] = None
    (TEMP_DIR / "last_activity").touch()

    from prompts.features_prompt import build_prd_prompt
    prompt = build_prd_prompt(state["project_name"], state["description"], answers)

    def on_done(text):
        text = text.strip()
        if text and not text.startswith("Error:"):
            save_prd(text)
            state["_prd_done"] = {"ok": True}
        else:
            state["_prd_done"] = {"ok": False, "error": text or "PRD 생성 실패"}
        print("PRD_DONE", flush=True)

    call_llm_bg(prompt, on_done, "prd")
    return jsonify({"mode": "polling", "poll": "/api/poll/prd"})


@app.route("/api/poll/prd")
def poll_prd():
    result = state.get("_prd_done")
    if result is None:
        return jsonify({"ready": False})
    return jsonify({"ready": True, **result, "prd": state["prd"], "phase": state["phase"]})


# ── Phase 3: Generate feature spec from PRD ───────────────────────────────────

@app.route("/api/spec", methods=["POST"])
def generate_spec():
    if not state["prd"] and not state["features_md"]:
        return jsonify({"error": "PRD가 없습니다."}), 400

    prd_content = state["prd"] or state["features_md"]
    state["_spec_done"] = None
    (TEMP_DIR / "last_activity").touch()

    from prompts.features_prompt import build_spec_from_prd_prompt
    prompt = build_spec_from_prd_prompt(state["project_name"], prd_content)

    def on_done(text):
        text = text.strip()
        if text and not text.startswith("Error:"):
            save_spec(text)
            state["_spec_done"] = {"ok": True}
        else:
            state["_spec_done"] = {"ok": False, "error": text or "기능명세서 생성 실패"}
        print("SPEC_DONE", flush=True)

    call_llm_bg(prompt, on_done, "spec")
    return jsonify({"mode": "polling", "poll": "/api/poll/spec"})


@app.route("/api/poll/spec")
def poll_spec():
    result = state.get("_spec_done")
    if result is None:
        return jsonify({"ready": False})
    return jsonify({
        "ready": True, **result,
        "features_md": state["features_md"],
        "tree": state["tree"],
        "mermaid_code": state["mermaid_code"],
        "flat_nodes": _flatten(state["tree"]) if state["tree"] else [],
        "phase": state["phase"],
    })


# ── Patch (modify feature spec) ───────────────────────────────────────────────

@app.route("/api/patch", methods=["POST"])
def patch_features():
    data = request.json or {}
    if not state["features_md"] or state["phase"] != 3:
        return jsonify({"error": "기능명세서를 먼저 생성하세요."}), 400

    state["_patch_done"] = None
    (TEMP_DIR / "last_activity").touch()

    from prompts.features_prompt import build_patch_prompt
    prompt = build_patch_prompt(state["features_md"], data.get("action", ""), data.get("target", ""), data.get("content", ""))

    def on_done(text):
        text = text.strip()
        if text and not text.startswith("Error:"):
            save_spec(text)
            state["_patch_done"] = {"ok": True}
        else:
            state["_patch_done"] = {"ok": False, "error": text or "수정 실패"}
        print("PATCH_DONE", flush=True)

    call_llm_bg(prompt, on_done, "patch")
    return jsonify({"mode": "polling", "poll": "/api/poll/patch"})


@app.route("/api/poll/patch")
def poll_patch():
    result = state.get("_patch_done")
    if result is None:
        return jsonify({"ready": False})
    return jsonify({
        "ready": True, **result,
        "features_md": state["features_md"],
        "tree": state["tree"],
        "mermaid_code": state["mermaid_code"],
        "flat_nodes": _flatten(state["tree"]) if state["tree"] else [],
    })


# ── Tree regenerate (Python only, instant) ────────────────────────────────────

@app.route("/api/tree/regenerate", methods=["POST"])
def regenerate_tree():
    if not state["features_md"] or state["phase"] != 3:
        return jsonify({"error": "기능명세서가 없습니다."}), 400
    tree = parse_features_to_tree(state["features_md"], state["project_name"])
    state["tree"] = tree
    cwd = normalize_path(state["cwd"])
    (cwd / "tree.json").write_text(json.dumps(tree, ensure_ascii=False, indent=2), encoding="utf-8")
    print("TREE_REGENERATED", flush=True)
    return jsonify({"ok": True, "tree": tree, "flat_nodes": _flatten(tree)})


# ── Section detail ────────────────────────────────────────────────────────────

@app.route("/api/section", methods=["POST"])
def generate_section():
    data = request.json or {}
    feature_id = data.get("id", "")
    feature_name = data.get("name", "")
    feature_desc = data.get("description", "")

    state["_section_done"] = None
    (TEMP_DIR / "last_activity").touch()

    context = (state["prd"] or state["features_md"] or "")[:1000]

    from prompts.features_prompt import build_section_prompt
    prompt = build_section_prompt(feature_name, feature_desc, context)

    def on_done(text):
        text = text.strip()
        if text and not text.startswith("Error:"):
            cwd = normalize_path(state["cwd"])
            sections_dir = cwd / "sections"
            sections_dir.mkdir(exist_ok=True)
            safe_id = re.sub(r"[^\w가-힣-]", "_", feature_id or feature_name)
            (sections_dir / f"{safe_id}.md").write_text(text, encoding="utf-8")
            state["_section_done"] = {"ok": True, "content": text}
        else:
            state["_section_done"] = {"ok": False, "content": text}
        print(f"SECTION_DONE: {feature_name}", flush=True)

    call_llm_bg(prompt, on_done, "section")
    return jsonify({"mode": "polling", "poll": "/api/poll/section"})


@app.route("/api/poll/section")
def poll_section():
    result = state.get("_section_done")
    if result is None:
        return jsonify({"ready": False})
    return jsonify({"ready": True, **result})


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="product")
    parser.add_argument("--description", default="")
    args = parser.parse_args()

    state["project_name"] = (args.name or "product").strip() or "product"
    state["description"] = (args.description or "").strip()

    base = detect_planning_dir()
    proj_dir = base / state["project_name"]
    proj_dir.mkdir(parents=True, exist_ok=True)
    state["cwd"] = str(proj_dir)

    # 기존 features.md 로드
    features_file = proj_dir / "features.md"
    if features_file.exists():
        existing = features_file.read_text(encoding="utf-8")
        state["features_md"] = existing
        state["phase"] = detect_phase(existing)
        if state["phase"] == 3:
            state["tree"] = parse_features_to_tree(existing, state["project_name"])
            state["mermaid_code"] = extract_mermaid(existing)
        else:
            state["prd"] = existing
        print(f"LOADED (phase={state['phase']}): {features_file}", flush=True)

    port = find_free_port()
    state["port"] = port
    url = f"http://localhost:{port}"

    print(f"PRD_PORT: {port}", flush=True)
    print(f"PRD_LIGHT_CWD: {state['cwd']}", flush=True)
    print(f"PRD Light: {url}", flush=True)

    import threading
    threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
