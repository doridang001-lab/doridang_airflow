import ast
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def setter():
    path = Path(__file__).resolve().parents[1] / "modules/transform/pipelines/db/DB_UnionPOS_Receipt.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_set_date_input")
    ns = dict(Page=object, PlaywrightError=RuntimeError, PlaywrightTimeout=TimeoutError, logger=logging.getLogger(__name__))
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), ns)
    return ns["_set_date_input"]


def test_reapply_date_only_after_navigation():
    page = MagicMock()
    page.evaluate.side_effect = [RuntimeError("Execution context was destroyed"), True]
    setter()(page, "2026-09-07", "2026-09-07")
    assert page.wait_for_load_state.call_count == 2
    assert page.evaluate.call_args.args[1] == ["2026-09-07", "2026-09-07"]


def test_navigation_retries_are_bounded():
    page = MagicMock()
    page.evaluate.side_effect = RuntimeError("Execution context was destroyed")
    with pytest.raises(RuntimeError):
        setter()(page, "2026-09-07", "2026-09-07")
    assert page.evaluate.call_count == 3


def test_unrelated_browser_error_is_not_hidden():
    page = MagicMock()
    page.evaluate.side_effect = RuntimeError("Browser closed")
    with pytest.raises(RuntimeError):
        setter()(page, "2026-09-07", "2026-09-07")
    assert page.evaluate.call_count == 1


def test_missing_date_inputs_cannot_be_reported_as_success():
    page = MagicMock()
    page.evaluate.return_value = False
    with pytest.raises(TimeoutError):
        setter()(page, "2026-09-07", "2026-09-07")
