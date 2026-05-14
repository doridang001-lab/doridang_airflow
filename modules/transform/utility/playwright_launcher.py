import logging
import os
import shutil
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip() in {"1", "true", "True", "YES", "yes", "y", "Y"}


def _in_airflow_or_docker() -> bool:
    return os.getenv("AIRFLOW_HOME") is not None or _is_truthy(os.getenv("IS_DOCKER"))


def _system_chrome_exists() -> bool:
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin and Path(chrome_bin).exists():
        return True

    return any(
        shutil.which(candidate) is not None
        for candidate in ("google-chrome", "google-chrome-stable", "chromium", "chromium-browser")
    )


def launch_chromium(
    playwright: Any,
    *,
    headless: bool,
    args: Iterable[str] | None = None,
    **kwargs: Any,
):
    """
    Launch Chromium with a Docker-safe strategy.

    In Docker/Airflow, prefer system Chrome (`channel="chrome"`) so Playwright doesn't depend on
    `/ms-playwright/...` browser caches that can go missing after package upgrades.
    """
    launch_kwargs = dict(kwargs)
    launch_kwargs["headless"] = headless
    if args is not None:
        launch_kwargs["args"] = list(args)

    if _in_airflow_or_docker() and _system_chrome_exists():
        try:
            return playwright.chromium.launch(channel="chrome", **launch_kwargs)
        except Exception as exc:
            logger.warning(
                "System Chrome launch failed; falling back to Playwright-managed Chromium: %s",
                exc,
            )

    try:
        return playwright.chromium.launch(**launch_kwargs)
    except Exception as exc:
        msg = str(exc)
        if "Executable doesn't exist" in msg and "playwright install" in msg:
            raise RuntimeError(
                "Playwright browser executable is missing.\n"
                "Fix options:\n"
                "- Rebuild the Airflow Docker image (recommended)\n"
                "- Or run in the container: `python -m playwright install chromium`\n"
                f"Diagnostics: PLAYWRIGHT_BROWSERS_PATH={os.getenv('PLAYWRIGHT_BROWSERS_PATH')!r}, "
                f"CHROME_BIN={os.getenv('CHROME_BIN')!r}, IS_DOCKER={os.getenv('IS_DOCKER')!r}"
            ) from exc
        raise

