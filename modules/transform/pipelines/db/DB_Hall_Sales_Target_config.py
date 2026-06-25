"""
홀 매장 주간/일간 보고 목표치 설정 (airflow 비의존).

DAG(DB_Hall_Sales_Target_Dags.py)와 Windows 로컬 실행 스크립트
(scripts/run_hall_weekly_report_local.py)가 공유한다.

목표 입력은 같은 폴더의 hall_sale_target_input.json에서 관리한다.
- 월별목표에 입력된 월은 해당 월부터 적용된다.
- 특정 월 입력이 없으면 가장 가까운 이전 월 목표를 그대로 승계한다.
- 예: 2026-06 입력이 없으면 2026-05 목표를 기준으로 6월 월목표를 계산한다.
- 영업일수는 선택값이다. 입력하지 않으면 해당 달의 달력 일수를 사용한다.
"""

import calendar
import json
from copy import deepcopy
from pathlib import Path

import pendulum

TARGET_INPUT_PATH = Path(__file__).with_name("hall_sale_target_input.json")
START_YM = "2026-04"


def _make_month_days(start_ym: str, end_ym: str) -> dict:
    """start_ym ~ end_ym(포함) 범위의 {ym: days} 자동 생성."""
    result = {}
    y, m = int(start_ym[:4]), int(start_ym[5:7])
    ey, em = int(end_ym[:4]), int(end_ym[5:7])
    while (y, m) <= (ey, em):
        ym = f"{y:04d}-{m:02d}"
        result[ym] = calendar.monthrange(y, m)[1]
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def _load_target_input() -> dict:
    """JSON 목표 입력 파일을 읽는다. 파일이 없거나 형식이 깨지면 즉시 실패시킨다."""
    try:
        with TARGET_INPUT_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"홀 목표 입력 JSON 없음: {TARGET_INPUT_PATH}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"홀 목표 입력 JSON 파싱 실패: {TARGET_INPUT_PATH}: {exc}") from exc

    monthly_input = data.get("월별목표")
    if not isinstance(monthly_input, dict) or not monthly_input:
        raise ValueError("hall_sale_target_input.json의 '월별목표'는 비어 있지 않은 객체여야 합니다.")
    return monthly_input


def _resolve_monthly_inputs(month_days: dict, monthly_input: dict) -> dict:
    """
    월별 입력을 실행 대상 월 범위에 맞춰 확정한다.

    입력되지 않은 월은 직전 입력 월의 목표를 승계한다. 이렇게 해야 7월 목표를 바꿔도
    5월/6월을 다시 백필할 때 당시 입력 기준이 유지된다.
    """
    configured = sorted(ym for ym in monthly_input if isinstance(monthly_input[ym], dict))
    resolved = {}
    latest = None

    for ym in month_days:
        is_configured_month = ym in monthly_input
        if ym in monthly_input:
            latest = deepcopy(monthly_input[ym])
        elif latest is None:
            previous = [key for key in configured if key <= ym]
            if previous:
                latest = deepcopy(monthly_input[previous[-1]])

        if latest is None:
            raise ValueError(f"{ym}에 적용할 홀 목표 입력이 없습니다. {START_YM} 또는 이전 월 목표를 입력하세요.")
        month_target = deepcopy(latest)
        if not is_configured_month:
            # 영업일수는 특정 월 보정값이다. 목표값은 승계하되 영업일수 보정은 다음 달로 넘기지 않는다.
            month_target.pop("영업일수", None)
        resolved[ym] = month_target

    return resolved


def _require_section(month_target: dict, ym: str, section: str) -> dict:
    value = month_target.get(section)
    if not isinstance(value, dict):
        raise ValueError(f"{ym} 목표 입력에 '{section}' 객체가 필요합니다.")
    return value


def _daily_tracking_target(daily_target: dict, marketing_daily_target: dict) -> dict:
    """일간 추적 CSV의 예측/일평균 목표에 쓰는 일목표 dict를 만든다."""
    return {
        "sale":          daily_target["sale"],
        "lunch_sale":    daily_target["lunch_sale"],
        "lunch_orders":  daily_target["lunch_orders"],
        "lunch_aov":     daily_target["lunch_aov"],
        "dinner_sale":   daily_target["dinner_sale"],
        "dinner_orders": daily_target["dinner_orders"],
        "dinner_aov":    daily_target["dinner_aov"],
        "place":         marketing_daily_target["플레이스_유입"],
        "홍보물_배포":   marketing_daily_target["홍보물_배포"],
        "coupon":        marketing_daily_target["쿠폰_회수"],
        "insta":         marketing_daily_target["인스타_노출"],
        "karrot":        marketing_daily_target["당근_노출"],
        "naver":         marketing_daily_target["네이버_오더"],
    }


def build_targets() -> tuple[dict, dict, dict]:
    """
    (MONTHLY_TARGETS, MARKETING_MONTHLY_TARGETS, DAILY_TRACKING_TARGET) 반환.

    DAILY_TRACKING_TARGET에는 월별 일목표 원장(_by_ym)도 같이 담는다. 기존 호출부는
    일반 일목표 dict처럼 사용할 수 있고, 월 지정 백필 함수는 _by_ym에서 해당 월을 고른다.
    """
    today = pendulum.now("Asia/Seoul")
    end_ym = f"{today.year + (today.month + 11) // 12:04d}-{(today.month + 11) % 12 + 1:02d}"
    month_days = _make_month_days(START_YM, end_ym)
    monthly_input = _load_target_input()
    resolved_inputs = _resolve_monthly_inputs(month_days, monthly_input)

    monthly_targets = {}
    marketing_monthly_targets = {}
    daily_tracking_by_ym = {}

    for ym, days in month_days.items():
        month_target = resolved_inputs[ym]
        target_days = int(month_target.get("영업일수") or days)
        daily_target = _require_section(month_target, ym, "일매출목표")
        marketing_daily_target = _require_section(month_target, ym, "일마케팅목표")

        monthly_targets[ym] = {
            "sale":   daily_target["sale"]   * target_days,
            "aov":    daily_target["aov"],
            "orders": daily_target["orders"] * target_days,

            "lunch_sale":   daily_target["lunch_sale"]   * target_days,
            "lunch_orders": daily_target["lunch_orders"] * target_days,
            "lunch_aov":    daily_target["lunch_aov"],

            "dinner_sale":   daily_target["dinner_sale"]   * target_days,
            "dinner_orders": daily_target["dinner_orders"] * target_days,
            "dinner_aov":    daily_target["dinner_aov"],
        }

        marketing_monthly_targets[ym] = {
            key: value * target_days
            for key, value in marketing_daily_target.items()
        }
        daily_tracking_by_ym[ym] = _daily_tracking_target(daily_target, marketing_daily_target)

    current_ym = today.strftime("%Y-%m")
    daily_tracking_target = deepcopy(daily_tracking_by_ym.get(current_ym) or daily_tracking_by_ym[START_YM])
    daily_tracking_target["_by_ym"] = daily_tracking_by_ym

    return monthly_targets, marketing_monthly_targets, daily_tracking_target
