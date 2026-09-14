import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMMON_PATH = ROOT / "modules" / "transform" / "pipelines" / "db" / "DB_UnifiedSales_common.py"
DB_PIPELINE_DIR = ROOT / "modules" / "transform" / "pipelines" / "db"

REQUIRED_PUBLIC_NAMES = {
    "ADD_TEST_STORES",
    "DELIVERY_MANUAL_TEST_STORES",
    "EXCLUDE_TEST_STORES",
    "FULL_RECALC_STORES",
    "TOORDER_MANUAL_STORES",
    "UNIFIED_COLUMNS",
    "UNIFIED_ROOT",
    "clear_manual_partial_marker",
    "clear_manual_reingest_marker",
    "delivery_baseline_summary",
    "detect_manual_partial_collection",
    "fill_missing_manual_item_name",
    "list_manual_reingest_dates",
    "normalize_existing_unified_platforms",
    "notify_manual_missing_all",
    "notify_manual_partial",
    "quarantine_conflict_copies",
    "record_manual_partial_marker",
    "record_manual_reingest_marker",
    "save_unified_parquet",
}


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))


def _defined_module_names(path: Path) -> set[str]:
    names: set[str] = set()
    for node in _parse(path).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".", 1)[0])
    return names


def _common_imports_by_file() -> dict[str, set[str]]:
    imports: dict[str, set[str]] = {}
    module_name = "modules.transform.pipelines.db.DB_UnifiedSales_common"

    for path in sorted(DB_PIPELINE_DIR.glob("DB_*.py")):
        if path == COMMON_PATH:
            continue

        imported_names: set[str] = set()
        for node in ast.walk(_parse(path)):
            if isinstance(node, ast.ImportFrom) and node.module == module_name:
                imported_names.update(alias.name for alias in node.names if alias.name != "*")

        if imported_names:
            imports[path.relative_to(ROOT).as_posix()] = imported_names

    return imports


def test_unified_sales_common_required_public_contract_exists() -> None:
    defined = _defined_module_names(COMMON_PATH)

    assert sorted(REQUIRED_PUBLIC_NAMES - defined) == []


def test_db_pipeline_common_imports_resolve_statically() -> None:
    defined = _defined_module_names(COMMON_PATH)
    missing = {
        path: sorted(imported - defined)
        for path, imported in _common_imports_by_file().items()
        if imported - defined
    }

    assert missing == {}


def test_no_root_level_unified_sales_common_shadow_copy() -> None:
    shadow_copy = ROOT / "DB_UnifiedSales_common.py"

    assert not shadow_copy.exists(), (
        "DB_UnifiedSales_common.py must live only under "
        "modules/transform/pipelines/db/ to avoid stale restore copies."
    )
