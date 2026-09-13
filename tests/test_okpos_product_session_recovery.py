import ast
import logging
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from selenium.common.exceptions import InvalidSessionIdException, WebDriverException


def load_helpers(product_path, sales_path):
    namespace = {
        "Path": Path,
        "WebDriverException": WebDriverException,
        "logger": logging.getLogger(__name__),
    }
    for path, name in (
        (sales_path, "_is_transient_connection_error"),
        (product_path, "_launch_product_session"),
    ):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace


class SessionRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.ns = load_helpers(PRODUCT, SALES)
        self.drivers = [Mock(), Mock(), Mock()]
        self.ns.update(
            _launch_browser=Mock(side_effect=self.drivers),
            _setup_download_dir=Mock(),
            _login=Mock(),
            WebDriverWait=Mock(side_effect=lambda driver, timeout: driver),
            WAIT_TIMEOUT=30,
            time=Mock(spec=time),
        )

    def run_session(self):
        return self.ns["_launch_product_session"](Path("unused"))

    def test_success_keeps_session_open(self):
        self.assertEqual(self.run_session(), (self.drivers[0], self.drivers[0]))
        self.drivers[0].quit.assert_not_called()

    def test_disconnect_restarts_before_retry(self):
        def login(driver, wait):
            if driver is self.drivers[0]:
                raise InvalidSessionIdException("invalid session id")
            self.drivers[0].quit.assert_called_once()
        self.ns["_login"].side_effect = login
        self.assertEqual(self.run_session()[0], self.drivers[1])
        self.assertEqual(self.ns["_setup_download_dir"].call_count, 2)
        self.ns["time"].sleep.assert_called_once_with(5)

    def test_repeated_disconnect_raises_after_three_attempts(self):
        error = InvalidSessionIdException("invalid session id")
        self.ns["_login"].side_effect = error
        self.drivers[0].quit.side_effect = RuntimeError("already closed")
        with self.assertRaises(InvalidSessionIdException) as result:
            self.run_session()
        self.assertIs(result.exception, error)
        self.assertEqual(self.ns["_launch_browser"].call_count, 3)
        self.assertEqual(self.ns["time"].sleep.call_count, 2)
        for driver in self.drivers:
            driver.quit.assert_called_once()

    def test_auth_failure_is_not_retried(self):
        self.ns["_login"].side_effect = RuntimeError("account locked")
        with self.assertRaisesRegex(RuntimeError, "account locked"):
            self.run_session()
        self.assertEqual(self.ns["_launch_browser"].call_count, 1)
        self.drivers[0].quit.assert_called_once()
        self.ns["time"].sleep.assert_not_called()

    def test_setup_disconnect_restarts(self):
        self.ns["_setup_download_dir"].side_effect = [
            InvalidSessionIdException("invalid session id"), None,
        ]
        self.assertEqual(self.run_session()[0], self.drivers[1])
        self.drivers[0].quit.assert_called_once()

    def test_launch_failure_is_propagated(self):
        self.ns["_launch_browser"].side_effect = ValueError("bad configuration")
        with self.assertRaisesRegex(ValueError, "bad configuration"):
            self.run_session()
        self.ns["time"].sleep.assert_not_called()


PRODUCT = Path(__file__).resolve().parents[1] / "modules/transform/pipelines/db/DB_OKPOS_Product.py"
SALES = PRODUCT.with_name("DB_OKPOS_Sales.py")

if __name__ == "__main__":
    if len(sys.argv) == 3:
        PRODUCT, SALES = map(Path, sys.argv[1:])
        del sys.argv[1:]
    unittest.main()
