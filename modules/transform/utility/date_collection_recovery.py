"""Date-based recovery planning for missed Airflow collection runs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Literal

import pendulum

KST = pendulum.timezone("Asia/Seoul")
RECOVERY_RUN_PREFIX = "date_recovery__"
ACTIVE_RECOVERY_STATES = {"queued", "running", "success"}

ConfMode = Literal[
    "sale_date",
    "target_date",
    "sale_date_range",
    "date_range",
    "start_end",
    "date_range_string",
]


@dataclass(frozen=True)
class RecoveryTarget:
    dag_id: str
    conf_mode: ConfMode
    schedule_to_sale_date_offset_days: int = -1
    group: Literal["source", "mart"] = "source"
    description: str = ""
    extra_conf: dict[str, Any] | None = None


@dataclass(frozen=True)
class RecoveryPlanItem:
    target: RecoveryTarget
    sale_date_from: str
    sale_date_to: str
    conf: dict[str, Any]
    run_id: str
    duplicate_prefix: str


TARGETS: tuple[RecoveryTarget, ...] = (
    RecoveryTarget(
        dag_id="DB_Beamin_Macro_Dags",
        conf_mode="target_date",
        description="배민 매크로 원천 수집",
        extra_conf={"run_all_batches": True},
    ),
    RecoveryTarget(
        dag_id="DB_OKPOS_Sales_Dags",
        conf_mode="sale_date_range",
        description="OKPOS 매출 원천 수집",
    ),
    RecoveryTarget(
        dag_id="DB_EasyPOS_Sales_Dags",
        conf_mode="date_range",
        description="EasyPOS 매출 원천 수집",
    ),
    RecoveryTarget(
        dag_id="DB_UnionPOS_Receipt_Dags",
        conf_mode="sale_date_range",
        description="UnionPOS 영수증 원천 수집",
    ),
    RecoveryTarget(
        dag_id="Food_Guide_orders_collect_Dags",
        conf_mode="date_range",
        description="Food Guide 주문 원천 수집",
    ),
    RecoveryTarget(
        dag_id="Sales_ToOrderSalesReport_Crawl_Dags",
        conf_mode="start_end",
        description="ToOrder 종합보고서 원천 수집",
    ),
    RecoveryTarget(
        dag_id="DB_Toorder_store_platform_daily_Dags",
        conf_mode="sale_date_range",
        description="ToOrder store platform 일별 mart",
    ),
    RecoveryTarget(
        dag_id="DB_ToOrder_Daily_Store_Dags",
        conf_mode="sale_date",
        description="ToOrder 일별 매장 보고서 수집",
    ),
    RecoveryTarget(
        dag_id="DB_ToOrderMenu_Dags",
        conf_mode="date_range_string",
        description="ToOrder 메뉴/옵션 상세 수집",
    ),
    RecoveryTarget(
        dag_id="DB_UnifiedSales",
        conf_mode="sale_date",
        group="mart",
        description="UnifiedSales 일별 재계산",
    ),
    RecoveryTarget(
        dag_id="DB_OrderCrossAnalysis_Dags",
        conf_mode="sale_date",
        group="mart",
        description="주문 교차분석 일별 재계산",
    ),
)


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def target_registry() -> dict[str, RecoveryTarget]:
    return {target.dag_id: target for target in TARGETS}


def parse_target_dag_ids(raw: Any) -> list[str] | None:
    if raw in (None, "", []):
        return None
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, (list, tuple, set)):
        values = [str(item).strip() for item in raw]
    else:
        raise ValueError("target_dag_ids는 문자열 또는 리스트여야 합니다.")
    values = [value for value in values if value]
    return values or None


def parse_target_groups(raw: Any) -> list[str] | None:
    if raw in (None, "", []):
        return None
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, (list, tuple, set)):
        values = [str(item).strip() for item in raw]
    else:
        raise ValueError("target_groups는 문자열 또는 리스트여야 합니다.")
    values = [value for value in values if value]
    allowed = {"source", "mart"}
    unknown = [value for value in values if value not in allowed]
    if unknown:
        raise ValueError(f"target_groups 허용값은 source, mart 입니다: {unknown}")
    return values or None


def parse_recovery_date(value: Any, *, field_name: str) -> pendulum.Date:
    if value in (None, ""):
        raise ValueError(f"{field_name} 값이 필요합니다.")
    try:
        parsed = pendulum.parse(str(value), strict=False)
    except Exception as exc:
        raise ValueError(f"{field_name} 날짜 형식 오류: YYYY-MM-DD 필요") from exc
    return parsed.in_timezone(KST).date()


def default_schedule_date_range(now: Any | None = None) -> tuple[str, str]:
    current = pendulum.instance(now or pendulum.now(KST)).in_timezone(KST)
    return (
        current.subtract(days=2).format("YYYY-MM-DD"),
        current.subtract(days=1).format("YYYY-MM-DD"),
    )


def resolve_schedule_date_range(conf: dict[str, Any], now: Any | None = None) -> tuple[str, str]:
    default_from, default_to = default_schedule_date_range(now)
    date_from = parse_recovery_date(conf.get("date_from") or default_from, field_name="date_from")
    date_to = parse_recovery_date(conf.get("date_to") or default_to, field_name="date_to")
    if date_from > date_to:
        raise ValueError(f"date_from({date_from})이 date_to({date_to})보다 큽니다.")
    return date_from.to_date_string(), date_to.to_date_string()


def date_range(start: str, end: str) -> list[str]:
    start_date = parse_recovery_date(start, field_name="start")
    end_date = parse_recovery_date(end, field_name="end")
    if start_date > end_date:
        raise ValueError(f"start({start})이 end({end})보다 큽니다.")
    days = []
    current = pendulum.datetime(start_date.year, start_date.month, start_date.day, tz=KST)
    last = pendulum.datetime(end_date.year, end_date.month, end_date.day, tz=KST)
    while current <= last:
        days.append(current.format("YYYY-MM-DD"))
        current = current.add(days=1)
    return days


def sale_date_for_schedule_date(schedule_date: str, *, offset_days: int) -> str:
    parsed = parse_recovery_date(schedule_date, field_name="schedule_date")
    base = pendulum.datetime(parsed.year, parsed.month, parsed.day, tz=KST)
    return base.add(days=offset_days).format("YYYY-MM-DD")


def sale_date_range_for_schedule_range(target: RecoveryTarget, schedule_from: str, schedule_to: str) -> tuple[str, str]:
    sale_dates = [
        sale_date_for_schedule_date(day, offset_days=target.schedule_to_sale_date_offset_days)
        for day in date_range(schedule_from, schedule_to)
    ]
    return sale_dates[0], sale_dates[-1]


def _safe_run_part(value: Any, *, max_len: int = 80) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "")).strip("_")
    return (safe or "none")[:max_len]


def _date_token(start: str, end: str) -> str:
    start_token = start.replace("-", "")
    end_token = end.replace("-", "")
    return start_token if start == end else f"{start_token}_{end_token}"


def build_recovery_run_prefix(*, dag_id: str, sale_date_from: str, sale_date_to: str) -> str:
    return f"{RECOVERY_RUN_PREFIX}{_safe_run_part(dag_id)}__{_date_token(sale_date_from, sale_date_to)}"


def build_recovery_run_id(
    *,
    dag_id: str,
    sale_date_from: str,
    sale_date_to: str,
    parent_run_id: str | None,
) -> str:
    prefix = build_recovery_run_prefix(
        dag_id=dag_id,
        sale_date_from=sale_date_from,
        sale_date_to=sale_date_to,
    )
    return f"{prefix}__{_safe_run_part(parent_run_id or 'manual', max_len=100)}"


def build_target_conf(
    *,
    target: RecoveryTarget,
    sale_date_from: str,
    sale_date_to: str,
    parent_dag_id: str,
    parent_run_id: str | None,
    schedule_date_from: str,
    schedule_date_to: str,
) -> dict[str, Any]:
    if target.conf_mode == "sale_date":
        if sale_date_from != sale_date_to:
            raise ValueError(f"{target.dag_id}는 단일 sale_date 실행만 지원합니다.")
        conf: dict[str, Any] = {"sale_date": sale_date_from}
    elif target.conf_mode == "target_date":
        if sale_date_from != sale_date_to:
            raise ValueError(f"{target.dag_id}는 단일 target_date 실행만 지원합니다.")
        conf = {"target_date": sale_date_from}
    elif target.conf_mode == "sale_date_range":
        conf = {"sale_date_from": sale_date_from, "sale_date_to": sale_date_to}
    elif target.conf_mode == "date_range":
        conf = {"date_from": sale_date_from, "date_to": sale_date_to}
    elif target.conf_mode == "start_end":
        conf = {"start_date": sale_date_from, "end_date": sale_date_to}
    elif target.conf_mode == "date_range_string":
        conf = {"date_range": f"{sale_date_from}~{sale_date_to}"}
    else:
        raise ValueError(f"지원하지 않는 conf_mode: {target.conf_mode}")

    conf.update(target.extra_conf or {})
    conf.update(
        {
            "recovered_by": parent_dag_id,
            "parent_run_id": parent_run_id,
            "schedule_date_from": schedule_date_from,
            "schedule_date_to": schedule_date_to,
            "recovery_group": target.group,
        }
    )
    return conf


def select_targets(
    target_dag_ids: Iterable[str] | None = None,
    *,
    target_groups: Iterable[str] | None = None,
) -> list[RecoveryTarget]:
    registry = target_registry()
    if target_dag_ids is None:
        groups = set(target_groups or ["source"])
        return [target for target in TARGETS if target.group in groups]

    selected: list[RecoveryTarget] = []
    missing: list[str] = []
    for dag_id in target_dag_ids:
        target = registry.get(str(dag_id).strip())
        if target is None:
            missing.append(str(dag_id))
        else:
            selected.append(target)
    if missing:
        known = ", ".join(sorted(registry))
        raise ValueError(f"미등록 복구 대상 DAG: {missing}. 등록 대상: {known}")
    return selected


def build_recovery_plan(
    *,
    schedule_date_from: str,
    schedule_date_to: str,
    parent_dag_id: str,
    parent_run_id: str | None = None,
    target_dag_ids: Iterable[str] | None = None,
    target_groups: Iterable[str] | None = None,
) -> list[RecoveryPlanItem]:
    items: list[RecoveryPlanItem] = []
    for target in select_targets(target_dag_ids, target_groups=target_groups):
        sale_from, sale_to = sale_date_range_for_schedule_range(target, schedule_date_from, schedule_date_to)
        if target.conf_mode in {"sale_date", "target_date"}:
            for sale_date in date_range(sale_from, sale_to):
                conf = build_target_conf(
                    target=target,
                    sale_date_from=sale_date,
                    sale_date_to=sale_date,
                    parent_dag_id=parent_dag_id,
                    parent_run_id=parent_run_id,
                    schedule_date_from=schedule_date_from,
                    schedule_date_to=schedule_date_to,
                )
                run_id = build_recovery_run_id(
                    dag_id=target.dag_id,
                    sale_date_from=sale_date,
                    sale_date_to=sale_date,
                    parent_run_id=parent_run_id,
                )
                items.append(
                    RecoveryPlanItem(
                        target=target,
                        sale_date_from=sale_date,
                        sale_date_to=sale_date,
                        conf=conf,
                        run_id=run_id,
                        duplicate_prefix=build_recovery_run_prefix(
                            dag_id=target.dag_id,
                            sale_date_from=sale_date,
                            sale_date_to=sale_date,
                        ),
                    )
                )
            continue

        conf = build_target_conf(
            target=target,
            sale_date_from=sale_from,
            sale_date_to=sale_to,
            parent_dag_id=parent_dag_id,
            parent_run_id=parent_run_id,
            schedule_date_from=schedule_date_from,
            schedule_date_to=schedule_date_to,
        )
        run_id = build_recovery_run_id(
            dag_id=target.dag_id,
            sale_date_from=sale_from,
            sale_date_to=sale_to,
            parent_run_id=parent_run_id,
        )
        items.append(
            RecoveryPlanItem(
                target=target,
                sale_date_from=sale_from,
                sale_date_to=sale_to,
                conf=conf,
                run_id=run_id,
                duplicate_prefix=build_recovery_run_prefix(
                    dag_id=target.dag_id,
                    sale_date_from=sale_from,
                    sale_date_to=sale_to,
                ),
            )
        )
    return items


def format_plan_summary(items: Iterable[RecoveryPlanItem], *, execute: bool, skipped: Iterable[str] = ()) -> str:
    lines = [
        "[날짜 수집 복구 계획]",
        f"mode={'execute' if execute else 'dry-run'}",
    ]
    skipped_list = list(skipped)
    for item in items:
        lines.append(
            f"- {item.target.dag_id}: sale_date={item.sale_date_from}"
            f"{'' if item.sale_date_from == item.sale_date_to else '~' + item.sale_date_to} "
            f"run_id={item.run_id} conf={item.conf}"
        )
    if skipped_list:
        lines.append("[skip]")
        lines.extend(f"- {line}" for line in skipped_list)
    return "\n".join(lines)
