from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from modules.transform.utility.paths import DASHBOARD_DB

DEFAULT_PROFILE = "safe"

STABILITY_PROFILES: dict[str, dict[str, Any]] = {
    "safe": {
        "initial_stagger_range": (0.0, 20.0),
        "account_wait_range": (20.0, 45.0),
        "orders_validation_retry": 2,
        "max_parallel_accounts": 2,
        "driver_restart_every_stores": 4,
        "max_session_recovery_per_account": 2,
    },
    "bulk_70": {
        "initial_stagger_range": (0.0, 10.0),
        "account_wait_range": (5.0, 15.0),
        "orders_validation_retry": 1,
        "max_parallel_accounts": 4,
        "driver_restart_every_stores": 8,
        "max_session_recovery_per_account": 2,
    },
}


def resolve_stability_profile(name: str | None) -> dict[str, Any]:
    profile_name = (name or DEFAULT_PROFILE).strip() or DEFAULT_PROFILE
    profile = deepcopy(STABILITY_PROFILES.get(profile_name, STABILITY_PROFILES[DEFAULT_PROFILE]))
    profile["name"] = profile_name if profile_name in STABILITY_PROFILES else DEFAULT_PROFILE
    return profile


def write_runtime_metrics(
    *,
    run_id: str,
    stage: str,
    payload: dict[str, Any],
    root: Path | None = None,
) -> Path:
    target_root = Path(root or DASHBOARD_DB)
    target_root.mkdir(parents=True, exist_ok=True)
    path = target_root / f"db_beamin_macro_{stage}_metrics.json"
    document = {
        "generated_at": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "stage": stage,
        **payload,
    }
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
