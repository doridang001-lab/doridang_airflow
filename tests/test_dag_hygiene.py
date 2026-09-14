import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "dags"

AD_HOC_DAG_FILE_PATTERNS = [
    re.compile(r"(^|.*/)tmp_.*\.py$"),
    re.compile(r"(^|.*/).*_tmp\.py$"),
    re.compile(r"(^|.*/)debug_.*\.py$"),
    re.compile(r"(^|.*/).*_debug\.py$"),
    re.compile(r"(^|.*/)scratch_.*\.py$"),
    re.compile(r"(^|.*/).*_scratch\.py$"),
]

FORBIDDEN_TOP_LEVEL_CALLS = {
    "launch_chromium",
    "sync_playwright",
}


def _dag_py_files() -> list[Path]:
    return sorted(
        path
        for path in DAGS_DIR.rglob("*.py")
        if "__pycache__" not in path.parts
    )


def _relative_posix(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _call_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _iter_import_time_nodes(node: ast.AST):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return
    yield node
    for child in ast.iter_child_nodes(node):
        yield from _iter_import_time_nodes(child)


def test_no_ad_hoc_python_files_in_dags() -> None:
    offenders = [
        _relative_posix(path)
        for path in _dag_py_files()
        if any(pattern.search(_relative_posix(path)) for pattern in AD_HOC_DAG_FILE_PATTERNS)
    ]

    assert offenders == []


def test_no_browser_launch_at_dag_import_time() -> None:
    offenders: list[str] = []

    for path in _dag_py_files():
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        for statement in tree.body:
            nodes = _iter_import_time_nodes(statement)
            for node in nodes:
                if not isinstance(node, ast.Call):
                    continue
                call_name = _call_name(node)
                if call_name in FORBIDDEN_TOP_LEVEL_CALLS:
                    offenders.append(f"{_relative_posix(path)}:{node.lineno}:{call_name}")

    assert offenders == []


def test_airflowignore_blocks_ad_hoc_scripts() -> None:
    ignore_path = DAGS_DIR / ".airflowignore"
    content = ignore_path.read_text(encoding="utf-8")

    for expected in ("tmp_", "_tmp", "debug_", "_debug", "scratch_", "_scratch"):
        assert expected in content
