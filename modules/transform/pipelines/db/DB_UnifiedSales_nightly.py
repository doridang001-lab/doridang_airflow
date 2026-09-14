"""야간 전체기간 계산: 날짜 단위 완료 기록을 다음 야간 실행에 승계한다."""
from __future__ import annotations

import importlib
import json
import logging
from contextvars import ContextVar
from datetime import timedelta

import pendulum

logger = logging.getLogger(__name__)
_progress = ContextVar("unified_nightly_progress", default=None)


class YieldNightly(BaseException):
    """기존 backfill의 일반 Exception 처리에 삼켜지지 않는 작업 양보 신호."""


class FailedNightly(BaseException):
    def __init__(self, error):
        self.error = error


def in_night_window(now=None):
    hour = (now or pendulum.now("Asia/Seoul")).hour
    return hour >= 22 or hour < 7


def _persist(state):
    from modules.transform.utility.paths import LOCAL_DB
    path = LOCAL_DB / "airflow_ops" / "nightly_unified.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    temp.replace(path)


def backfill_unit(func, date_str, **kwargs):
    state = _progress.get()
    if state is None:
        return func(date_str, **kwargs)
    from modules.transform.utility.workload import background_ready
    key = json.dumps([func.__module__, func.__name__, date_str, kwargs.get("stores")], ensure_ascii=False)
    if key in state["done"]:
        return "SKIP: 야간 완료 날짜"
    if not in_night_window() or not background_ready():
        raise YieldNightly()
    try:
        result = func(date_str, **kwargs)
    except Exception as exc:
        # 기존 full-backfill 함수가 오류를 경고로만 남기는 경우에도 완료 처리하지 않는다.
        raise FailedNightly(exc) from exc
    state["done"].append(key)
    _persist(state)
    logger.info("야간 날짜 완료: source=%s date=%s completed=%d", func.__name__, date_str, len(state["done"]))
    return result


def run_nightly(**context):
    from airflow.exceptions import AirflowRescheduleException
    from airflow.utils import timezone
    from modules.transform.utility.paths import LOCAL_DB
    from modules.transform.utility.process_lock import named_lock
    from modules.transform.pipelines.db.DB_UnifiedSales_common import FULL_RECALC_STORES, DELIVERY_MANUAL_TEST_STORES
    targets = sorted(set(str(s).strip() for s in FULL_RECALC_STORES if str(s).strip()))
    path = LOCAL_DB / "airflow_ops" / "nightly_unified.json"
    with named_lock("nightly_unified", timeout=0):
        state = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        if not state or state.get("complete"):
            state = {"stores": targets, "done": [], "complete": False,
                     "started_at": pendulum.now("UTC").isoformat()}
            _persist(state)
        # 미완료 요청의 범위는 최초 요청대로 유지한다. 새 설정은 다음 요청부터 반영한다.
        token = _progress.set(state)
        try:
            for store in state["stores"]:
                for source in ("okpos", "unionpos", "easypos", "posfeed", "toorder"):
                    module = importlib.import_module("modules.transform.pipelines.db.DB_UnifiedSales_" + source)
                    name = "backfill_toorder_manual_stores" if source == "toorder" else "backfill_" + source + "_stores"
                    # 원천 자체가 없는 소스는 기존 수집 매장 범위에 해당하지 않는다.
                    try:
                        getattr(module, name)(stores=[store])
                    except FileNotFoundError:
                        if source not in ("unionpos", "easypos"):
                            raise
                        logger.info("야간 해당 소스 원천 없음: %s", source)
                if store in DELIVERY_MANUAL_TEST_STORES:
                    for source in ("baemin", "coupang"):
                        module = importlib.import_module("modules.transform.pipelines.db.DB_UnifiedSales_" + source)
                        dates = module._resolve_target_dates([store], None, None)
                        func = getattr(module, "reconcile_" + source + "_for_test_stores")
                        for date in sorted(dates):
                            # 공통 date-first 인터페이스 어댑터; source별 key를 유지한다.
                            def apply_date(d, stores, _func=func):
                                return _func(stores=stores, sale_date=d, lookback_days=None)
                            apply_date.__name__ = func.__name__
                            backfill_unit(apply_date, date, stores=[store])
            state["complete"] = True
            state["completed_at"] = pendulum.now("UTC").isoformat()
            _persist(state)
            return f"야간 전체기간 계산 완료: {len(state['done'])}개 소스·매장·날짜"
        except YieldNightly:
            if in_night_window():
                raise AirflowRescheduleException(timezone.utcnow() + timedelta(seconds=60))
            return f"07시 실행 양보: {len(state['done'])}개 완료, 미완료 범위는 다음 야간에 재개"
        except FailedNightly as exc:
            raise exc.error
        finally:
            _progress.reset(token)
