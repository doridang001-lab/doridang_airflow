"""운영자가 확인할 목표·금액 기준과 탐지 규칙."""
from datetime import date
from decimal import Decimal, InvalidOperation

from modules.transform.pipelines.db.samjeon_material_agent.contracts import read_config

DEFAULTS = {
    "version": "defaults-v1", "target_rate": "0.37", "target_start_date": None,
    "amount_basis_confirmed": False, "increase_rate": "0.30", "increase_won": 100000,
    "minimum_weekday_samples": 4,
}


def validate_settings(raw: dict) -> dict:
    if set(raw) - set(DEFAULTS):
        raise ValueError(f"알 수 없는 운영 설정: {sorted(set(raw) - set(DEFAULTS))}")
    settings = {**DEFAULTS, **raw}
    if not isinstance(settings["version"], str) or not settings["version"].strip():
        raise ValueError("설정 버전이 필요합니다")
    for field in ("target_rate", "increase_rate"):
        try:
            number = Decimal(str(settings[field]))
        except InvalidOperation as exc:
            raise ValueError(f"비율 설정 오류: {field}") from exc
        if not number.is_finite() or number < 0 or (field == "target_rate" and number > 1):
            raise ValueError(f"비율 설정 범위 오류: {field}")
        settings[field] = str(number)
    if type(settings["amount_basis_confirmed"]) is not bool:
        raise ValueError("amount_basis_confirmed는 boolean이어야 합니다")
    for field, low, high in (("increase_won", 0, None), ("minimum_weekday_samples", 4, 8)):
        v = settings[field]
        if type(v) is not int or v < low or (high is not None and v > high):
            raise ValueError(f"정수 설정 범위 오류: {field}")
    if settings["target_start_date"] is not None:
        date.fromisoformat(settings["target_start_date"])
    return settings


def load_settings(plan: dict) -> dict:
    if "settings_path" not in plan:
        return validate_settings({})
    return validate_settings(read_config(plan["settings_path"], plan["config_hashes"]["settings"]))
