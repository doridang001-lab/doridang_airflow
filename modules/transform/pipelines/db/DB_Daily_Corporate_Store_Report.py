"""직영점 일일 매출 감시 리포트 mart 생성 파이프라인."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_UnifiedSales_common import _unified_daily_path
from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.paths import MART_DB, ORDER_CROSS_DIR

logger = logging.getLogger(__name__)

STORE_DISPLAY_NAME = "송파삼전점"
STORE_ALIASES = {"송파삼전점", "송파점"}
HALL_ORDER_TYPES = {"홀_테이블", "홀_포장"}
OUTPUT_DIR = MART_DB / "daily_corporate_store_report"
SUMMARY_CSV = OUTPUT_DIR / "daily_corporate_store_report.csv"
LLM_LOG_MD = OUTPUT_DIR / "llm_log.md"
OKPOS_CUSTOMER_CSV = MART_DB / "okpos_cutomer" / "okpos_customer_daily.csv"
GENERIC_ACTION_WORDS = ("캠페인", "이벤트", "할인", "광고 예산", "매출 회복에 집중")
ORDER_CROSS_PAIR_TYPE_PRIORITY = {"토핑": 0, "사이드": 1, "주류": 2}
ORDER_CROSS_EXCLUDED_COMBO_WORDS = ("흑미 공기밥",)

SITUATION_GUIDELINES = {
    "주문수_급감": {
        "condition": "전체 orders_pct <= -30",
        "focus": "주문 유입·영업상태·피크 전 준비",
        "actions": [
            "빠진 채널과 시간대 주문 유입을 피크 전 확인",
            "11시 전 배민/쿠팡 영업상태와 품절 메뉴를 확인",
        ],
    },
    "배달_주문감소": {
        "condition": "배달 orders_pct < 0 또는 sales_diff < 0",
        "focus": "앱 노출·영업상태·품절·주문 누락 시간대",
        "actions": [
            "11시 전 배민/쿠팡 영업상태, 노출순서, 품절 메뉴를 확인",
            "전일보다 빠진 배달 주문 시간대를 피크 전에 확인",
        ],
    },
    "홀_주문감소": {
        "condition": "홀 orders_pct < 0 또는 sales_diff < 0",
        "focus": "입구 안내·테이블 회전·포장 응대",
        "actions": [
            "점심 전 홀 입구/테이블 안내와 포장 응대 멘트를 점검",
            "저녁 전 테이블 회전 지연과 대기 응대 기준을 확인",
        ],
    },
    "홀_점심약세": {
        "condition": "점심(홀) orders_pct < 0 또는 sales_diff < 0",
        "focus": "점심 유입·입구 안내·포장 응대",
        "actions": [
            "점심 전 입구 안내와 포장 응대 담당을 재확인",
            "점심 피크 전 홀 주문수 회복 여부를 확인",
        ],
    },
    "홀_저녁약세": {
        "condition": "저녁(홀) orders_pct < 0 또는 sales_diff < 0",
        "focus": "저녁 유입·테이블 회전·대기 응대",
        "actions": [
            "저녁 전 테이블 회전과 대기 응대 기준을 확인",
            "저녁 피크 전 홀 주문수 회복 여부를 확인",
        ],
    },
    "객단가_하락": {
        "condition": "avg_order_value_diff < 0",
        "focus": "주력 메뉴 추가 제안·사이드/음료 제안",
        "actions": [
            "{top_menu} 주문 시 사리·음료 추가 제안 멘트를 공유",
            "주문 접수 전 사이드·음료 추가 확인 멘트를 통일",
        ],
    },
    "신규고객_부진": {
        "condition": "new_ratio <= 20",
        "focus": "신규 유입·첫 구매 응대",
        "actions": [
            "신규 구매 비중이 낮아 첫 주문 응대와 리뷰 요청 누락을 확인",
        ],
    },
    "재구매_약함": {
        "condition": "재구매비율이 40% 미만",
        "focus": "단골 고객 응대·재방문 유도",
        "actions": [
            "재방문 카드 고객 비중이 낮아 단골 응대 대상 주문을 따로 확인",
        ],
    },
    "전주比_하락": {
        "condition": "전주동요일대비 sales_diff < 0",
        "focus": "요일 정상 흐름 대비 약세 확인",
        "actions": [
            "전주 같은 요일보다 빠진 채널과 시간대를 먼저 확인",
        ],
    },
    "5일평균_하회": {
        "condition": "최근5일평균대비 sales_diff < 0",
        "focus": "최근 일매출 흐름 대비 약세 확인",
        "actions": [
            "최근 5일 평균보다 낮은 채널을 피크 전 점검",
        ],
    },
    "매출_증가": {
        "condition": "매출이 전일보다 증가",
        "focus": "증가 원인 유지·품질 사고 방지",
        "actions": [
            "증가 채널의 품절·대기시간·리뷰 리스크를 피크 전에 점검",
            "잘 팔린 메뉴의 재고와 조리 준비량을 전일보다 먼저 확보",
        ],
    },
    "데이터_미준비": {
        "condition": "기준일 매출 데이터가 없거나 0에 가까움",
        "focus": "상류 수집·적재 상태 확인",
        "actions": [
            "unified_sales 기준일 파일 생성 여부와 상류 수집 상태를 확인",
            "OKPOS·배달 수집 지연 여부를 먼저 점검",
        ],
    },
}

SUMMARY_COLUMNS = [
    "report_date",
    "store",
    "status",
    "total_sales",
    "total_orders",
    "avg_order_value",
    "new_customers",
    "returning_customers",
    "returning_ratio",
    "prev_sales",
    "prev_orders",
    "sales_diff",
    "sales_pct",
    "orders_diff",
    "orders_pct",
    "avg_order_value_diff",
    "avg_order_value_pct",
    "hall_sales",
    "hall_orders",
    "delivery_sales",
    "delivery_orders",
    "main_reason",
    "today_action",
    "telegram_message",
    "llm_status",
    "created_at",
]


def _default_sale_date(context: dict[str, Any] | None = None) -> str:
    kst = pendulum.timezone("Asia/Seoul")
    data_interval_end = (context or {}).get("data_interval_end")
    if data_interval_end is not None:
        return pendulum.instance(data_interval_end).in_timezone(kst).subtract(days=1).format("YYYY-MM-DD")
    return pendulum.now(kst).subtract(days=1).format("YYYY-MM-DD")


def _normalize_date(value: Any) -> str:
    return pendulum.parse(str(value).strip(), strict=False).in_timezone("Asia/Seoul").format("YYYY-MM-DD")


def _resolve_sale_date(conf: dict[str, Any] | None = None, context: dict[str, Any] | None = None) -> str:
    conf = conf or {}
    if conf.get("sale_date"):
        return _normalize_date(conf["sale_date"])
    return _default_sale_date(context)


def _fmt_won(value: Any) -> str:
    return f"{int(round(float(value or 0))):,}원"


def _fmt_int(value: Any) -> str:
    return f"{int(round(float(value or 0))):,}"


def _pct(cur: float, base: float) -> float:
    if not base:
        return 0.0
    return round((cur - base) / base * 100, 1)


def _diff_text(cur: int, base: int, unit: str = "") -> str:
    diff = cur - base
    sign = "+" if diff > 0 else ""
    return f"{sign}{diff:,}{unit}"


def _wrap18(text: str, limit: int = 18) -> str:
    text = str(text or "").strip()
    if not text:
        return ""
    lines = []
    current = ""
    for word in text.split():
        while len(word) > limit:
            head, word = word[:limit], word[limit:]
            if current:
                lines.append(current)
                current = ""
            lines.append(head)
        if not word:
            continue
        candidate = f"{current} {word}".strip()
        if len(candidate) <= limit:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return "\n".join(lines)


def _empty_sales_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "sale_date",
            "store",
            "platform",
            "order_type",
            "order_id",
            "order_time",
            "item_name",
            "qty",
            "total_price",
            "order_cnt",
            "sale_type",
            "channel",
        ]
    )


def _load_store_day(date_str: str) -> pd.DataFrame:
    path = _unified_daily_path(date_str)
    if not path.exists():
        logger.warning("unified_sales 파일 없음: %s", path)
        return _empty_sales_df()

    df = pd.read_parquet(path)
    if df.empty or "store" not in df.columns:
        return _empty_sales_df()

    df = df.copy()
    for col in ["store", "platform", "order_type", "sale_type", "order_id", "order_time", "item_name"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()
    for col in ["qty", "total_price", "order_cnt"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    mask = df["store"].isin(STORE_ALIASES) & df["sale_type"].ne("취소")
    df = df[mask].copy()
    if df.empty:
        return _empty_sales_df()

    hall_mask = df["platform"].eq("홀") | df["order_type"].isin(HALL_ORDER_TYPES)
    df["channel"] = "배달"
    df.loc[hall_mask, "channel"] = "홀"
    return df


def _orders(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    if "order_cnt" in df.columns:
        order_cnt = int(pd.to_numeric(df["order_cnt"], errors="coerce").fillna(0).sum())
        if order_cnt > 0:
            return order_cnt
    if "order_id" in df.columns:
        ids = df["order_id"].fillna("").astype(str).str.strip()
        ids = ids[ids.ne("")]
        return int(ids.nunique())
    return 0


def _agg(df: pd.DataFrame) -> dict[str, int]:
    sales = int(pd.to_numeric(df.get("total_price", pd.Series(dtype=int)), errors="coerce").fillna(0).sum())
    orders = _orders(df)
    avg = int(round(sales / orders)) if orders else 0
    return {"sales": sales, "orders": orders, "avg_order_value": avg}


def _channel_agg(df: pd.DataFrame) -> dict[str, dict[str, int]]:
    return {
        "전체": _agg(df),
        "홀": _agg(df[df.get("channel", pd.Series(dtype=str)).eq("홀")]) if not df.empty else _agg(df),
        "배달": _agg(df[df.get("channel", pd.Series(dtype=str)).eq("배달")]) if not df.empty else _agg(df),
    }


def _customer_counts(date_str: str) -> dict[str, Any]:
    if not OKPOS_CUSTOMER_CSV.exists():
        logger.warning("OKPOS customer mart 없음: %s", OKPOS_CUSTOMER_CSV)
        return {"new": 0, "returning": 0, "new_ratio": 0.0, "returning_ratio": 0.0}

    df = pd.read_csv(OKPOS_CUSTOMER_CSV, dtype=str, encoding="utf-8-sig").fillna("")
    for col in ["일별", "지역명", "신규재구매 구분"]:
        if col not in df.columns:
            logger.warning("OKPOS customer mart 컬럼 없음: %s", col)
            return {"new": 0, "returning": 0, "new_ratio": 0.0, "returning_ratio": 0.0}

    target = df[
        df["일별"].astype(str).str.strip().eq(date_str)
        & df["지역명"].astype(str).str.strip().eq(STORE_DISPLAY_NAME)
    ]
    counts = target["신규재구매 구분"].astype(str).str.strip().value_counts()
    new = int(counts.get("신규", 0))
    returning = int(counts.get("재구매", 0))
    total = new + returning
    return {
        "new": new,
        "returning": returning,
        "new_ratio": round(new / total * 100, 1) if total else 0.0,
        "returning_ratio": round(returning / total * 100, 1) if total else 0.0,
    }


def _classify_hall_time_slots(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    raw_hour = (
        df["order_time"].astype(str).str.strip()
        .str.extract(r"^(\d{2}):", expand=False)
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )
    raw_hour = raw_hour.where(raw_hour != 0, other=pd.NA)
    df["time_slot"] = pd.NA
    df.loc[((raw_hour >= 7) & (raw_hour < 16)).fillna(False), "time_slot"] = "점심"
    df.loc[((raw_hour >= 16) & (raw_hour < 24)).fillna(False), "time_slot"] = "저녁"
    df["time_slot"] = df["time_slot"].fillna("저녁")
    return df


def _time_slot_agg(hall_df: pd.DataFrame) -> dict[str, dict[str, int]]:
    if hall_df.empty:
        return {"점심": _agg(hall_df), "저녁": _agg(hall_df)}
    slotted = _classify_hall_time_slots(hall_df)
    return {
        "점심": _agg(slotted[slotted["time_slot"].eq("점심")]),
        "저녁": _agg(slotted[slotted["time_slot"].eq("저녁")]),
    }


def _menu_top(hall_df: pd.DataFrame, n: int = 3) -> list[dict[str, Any]]:
    if hall_df.empty:
        return []
    base = hall_df.copy()
    menu_col = "menu_name" if "menu_name" in base.columns else "item_name"
    if menu_col not in base.columns:
        return []
    base[menu_col] = base[menu_col].fillna("").astype(str).str.strip()
    base = base[base[menu_col].ne("")]
    if base.empty:
        return []
    total_sales = int(base["total_price"].sum())
    grouped = (
        base.groupby(menu_col, as_index=False)
        .agg(qty=("qty", "sum"), sales=("total_price", "sum"))
        .sort_values(["sales", "qty"], ascending=[False, False])
        .head(n)
    )
    result = []
    for row in grouped.to_dict("records"):
        sales = int(row["sales"])
        result.append({
            "menu": str(row[menu_col]),
            "qty": int(row["qty"]),
            "sales": sales,
            "share": round(sales / total_sales * 100, 1) if total_sales else 0.0,
        })
    return result


def _new_menu_names(cur_df: pd.DataFrame, prev_df: pd.DataFrame, n: int = 3) -> list[str]:
    if cur_df.empty:
        return []
    cur = cur_df.copy()
    prev = prev_df.copy()
    menu_col = "menu_name" if "menu_name" in cur.columns else "item_name"
    if menu_col not in cur.columns:
        return []
    cur[menu_col] = cur[menu_col].fillna("").astype(str).str.strip()
    if menu_col not in prev.columns:
        prev[menu_col] = ""
    prev_names = set(prev[menu_col].fillna("").astype(str).str.strip())
    cur = cur[cur[menu_col].ne("") & ~cur[menu_col].isin(prev_names)]
    if cur.empty:
        return []
    grouped = (
        cur.groupby(menu_col, as_index=False)
        .agg(qty=("qty", "sum"), sales=("total_price", "sum"))
        .sort_values(["qty", "sales"], ascending=[False, False])
        .head(n)
    )
    return [str(v) for v in grouped[menu_col].tolist()]


def _compare(cur: dict[str, int], base: dict[str, int]) -> dict[str, Any]:
    return {
        "sales_diff": cur["sales"] - base["sales"],
        "sales_pct": _pct(cur["sales"], base["sales"]),
        "orders_diff": cur["orders"] - base["orders"],
        "orders_pct": _pct(cur["orders"], base["orders"]),
        "avg_order_value_diff": cur["avg_order_value"] - base["avg_order_value"],
        "avg_order_value_pct": _pct(cur["avg_order_value"], base["avg_order_value"]),
    }


def _rolling_4w_avg(ref_date: date) -> dict[str, int]:
    aggs = []
    for days in (7, 14, 21, 28):
        day = (ref_date - timedelta(days=days)).strftime("%Y-%m-%d")
        df = _load_store_day(day)
        if not df.empty:
            aggs.append(_agg(df))
    if not aggs:
        return {"sales": 0, "orders": 0, "avg_order_value": 0}
    sales = int(round(sum(a["sales"] for a in aggs) / len(aggs)))
    orders = int(round(sum(a["orders"] for a in aggs) / len(aggs)))
    avg = int(round(sales / orders)) if orders else 0
    return {"sales": sales, "orders": orders, "avg_order_value": avg}


def _rolling_5d_avg(ref_date: date) -> dict[str, int]:
    aggs = []
    for days in (1, 2, 3, 4, 5):
        day = (ref_date - timedelta(days=days)).strftime("%Y-%m-%d")
        df = _load_store_day(day)
        if not df.empty:
            aggs.append(_agg(df))
    if not aggs:
        return {"sales": 0, "orders": 0, "avg_order_value": 0}
    sales = int(round(sum(a["sales"] for a in aggs) / len(aggs)))
    orders = int(round(sum(a["orders"] for a in aggs) / len(aggs)))
    avg = int(round(sales / orders)) if orders else 0
    return {"sales": sales, "orders": orders, "avg_order_value": avg}


def _main_reason(cur_channels: dict[str, dict[str, int]], prev_channels: dict[str, dict[str, int]]) -> str:
    hall_diff = cur_channels["홀"]["sales"] - prev_channels["홀"]["sales"]
    delivery_diff = cur_channels["배달"]["sales"] - prev_channels["배달"]["sales"]
    if abs(hall_diff) >= abs(delivery_diff):
        channel = "홀"
        diff = hall_diff
    else:
        channel = "배달"
        diff = delivery_diff
    direction = "증가" if diff > 0 else "감소" if diff < 0 else "변동 없음"
    return f"{channel} 매출 {direction}({_diff_text(diff, 0, '원')}) 영향이 가장 큽니다."


def _channel_diffs(metrics: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result = {}
    for channel in ("홀", "배달"):
        cur = metrics["current"][channel]
        prev = metrics["previous"][channel]
        result[channel] = {
            "sales_diff": cur["sales"] - prev["sales"],
            "orders_diff": cur["orders"] - prev["orders"],
            "avg_order_value_diff": cur["avg_order_value"] - prev["avg_order_value"],
            "sales_pct": _pct(cur["sales"], prev["sales"]),
            "orders_pct": _pct(cur["orders"], prev["orders"]),
        }
    return result


def _time_slot_diffs(metrics: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result = {}
    for slot in ("점심", "저녁"):
        cur = metrics.get("time_slots", {}).get(slot, {"sales": 0, "orders": 0, "avg_order_value": 0})
        prev = metrics.get("previous_time_slots", {}).get(slot, {"sales": 0, "orders": 0, "avg_order_value": 0})
        result[slot] = {
            "sales_diff": cur["sales"] - prev["sales"],
            "orders_diff": cur["orders"] - prev["orders"],
            "avg_order_value_diff": cur["avg_order_value"] - prev["avg_order_value"],
            "sales_pct": _pct(cur["sales"], prev["sales"]),
            "orders_pct": _pct(cur["orders"], prev["orders"]),
        }
    return result


def _operation_action_candidates(metrics: dict[str, Any]) -> list[str]:
    actions = []
    situations = _applied_situations(metrics)
    for situation in situations:
        guide = SITUATION_GUIDELINES.get(situation, {})
        first = (guide.get("actions", []) or [])[:1]
        for action in first:
            actions.append(_render_action_template(action, metrics))
    for situation in situations:
        guide = SITUATION_GUIDELINES.get(situation, {})
        for action in (guide.get("actions", []) or [])[1:]:
            actions.append(_render_action_template(action, metrics))

    if not actions:
        actions.append("매출 상위 채널의 품절·누락·피크 대기시간만 유지 점검")
    return _dedupe_actions(actions)[:4]


def _render_action_template(action: str, metrics: dict[str, Any]) -> str:
    top_menu = metrics["menu_top3"][0]["menu"] if metrics.get("menu_top3") else "주력 메뉴"
    return action.format(top_menu=top_menu)


def _dedupe_actions(actions: list[str]) -> list[str]:
    result = []
    seen = set()
    for action in actions:
        key = action.replace(" ", "")[:24]
        if key in seen:
            continue
        seen.add(key)
        result.append(action)
    return result


def _applied_situations(metrics: dict[str, Any]) -> list[str]:
    comp = metrics["compare_prev_day"]
    channel_diffs = _channel_diffs(metrics)
    slot_diffs = _time_slot_diffs(metrics)
    situations = []
    if metrics.get("status") != "ok" or metrics["current"]["전체"]["orders"] <= 0:
        return ["데이터_미준비"]
    if comp["orders_pct"] <= -30:
        situations.append("주문수_급감")
    if channel_diffs["배달"]["sales_diff"] < 0 or channel_diffs["배달"]["orders_diff"] < 0:
        situations.append("배달_주문감소")
    if channel_diffs["홀"]["sales_diff"] < 0 or channel_diffs["홀"]["orders_diff"] < 0:
        situations.append("홀_주문감소")
    if slot_diffs["점심"]["sales_diff"] < 0 or slot_diffs["점심"]["orders_diff"] < 0:
        situations.append("홀_점심약세")
    if slot_diffs["저녁"]["sales_diff"] < 0 or slot_diffs["저녁"]["orders_diff"] < 0:
        situations.append("홀_저녁약세")
    if comp["avg_order_value_diff"] < 0:
        situations.append("객단가_하락")
    if metrics["customers"].get("new_ratio", 0.0) <= 20:
        situations.append("신규고객_부진")
    if metrics["customers"]["returning_ratio"] < 40:
        situations.append("재구매_약함")
    if metrics.get("compare_prev_week", {}).get("sales_diff", 0) < 0:
        situations.append("전주比_하락")
    if metrics.get("compare_5d_avg", {}).get("sales_diff", 0) < 0:
        situations.append("5일평균_하회")
    if comp["sales_diff"] > 0:
        situations.append("매출_증가")
    return situations or ["매출_증가"]


def _fallback_actions(metrics: dict[str, Any]) -> list[str]:
    return _operation_action_candidates(metrics)[:2]


def _fallback_llm_result(metrics: dict[str, Any], reason: str = "") -> dict[str, Any]:
    cur = metrics["current"]["전체"]
    prev = metrics["previous"]["전체"]
    comp = metrics["compare_prev_day"]
    direction = "증가" if comp["sales_diff"] > 0 else "감소" if comp["sales_diff"] < 0 else "유지"
    actions = _fallback_actions(metrics)
    summary = (
        f"{metrics['report_date']} {STORE_DISPLAY_NAME} 매출은 전일 대비 "
        f"{_diff_text(cur['sales'], prev['sales'], '원')}({comp['sales_pct']:+.1f}%) {direction}했습니다."
    )
    reason_text = _structured_reason_text(metrics)
    telegram_message = _format_telegram_message(metrics, reason_text, actions)
    return {
        "summary": summary,
        "reason": reason_text,
        "today_action": actions,
        "telegram_message": telegram_message,
    }


def _llm_payload(metrics: dict[str, Any]) -> dict[str, Any]:
    comp = metrics["compare_prev_day"]
    channel_diffs = _channel_diffs(metrics)
    slot_diffs = _time_slot_diffs(metrics)
    return {
        "매장": STORE_DISPLAY_NAME,
        "기준일": metrics["report_date"],
        "전일": metrics["previous_date"],
        "핵심목적": "일단위 트래킹으로 전일 대비 변화 원인을 보고 오늘 바로 할 일을 정한다.",
        "기준일_전체": metrics["current"]["전체"],
        "전일_전체": metrics["previous"]["전체"],
        "기준일_홀": metrics["current"]["홀"],
        "전일_홀": metrics["previous"]["홀"],
        "기준일_배달": metrics["current"]["배달"],
        "전일_배달": metrics["previous"]["배달"],
        "홀_시간대": metrics["time_slots"],
        "전일_홀_시간대": metrics["previous_time_slots"],
        "전일대비": metrics["compare_prev_day"],
        "전일대비_읽는법": {
            "sales_diff": "양수면 매출 증가, 음수면 매출 감소",
            "orders_diff": "양수면 주문 증가, 음수면 주문 감소",
            "avg_order_value_diff": "양수면 객단가 증가, 음수면 객단가 감소",
        },
        "핵심판정": {
            "매출방향": "증가" if comp["sales_diff"] > 0 else "감소" if comp["sales_diff"] < 0 else "유지",
            "주문수방향": "증가" if comp["orders_diff"] > 0 else "감소" if comp["orders_diff"] < 0 else "유지",
            "객단가방향": "증가" if comp["avg_order_value_diff"] > 0 else "감소" if comp["avg_order_value_diff"] < 0 else "유지",
            "주요원인후보": _main_reason(metrics["current"], metrics["previous"]),
            "주문수_vs_객단가_판정": _order_value_judgment(metrics),
            "채널별_전일대비": channel_diffs,
            "홀시간대_전일대비": slot_diffs,
        },
        "적용된_상황": _applied_situations(metrics),
        "상황별_가이드라인": SITUATION_GUIDELINES,
        "오늘_실행후보": _operation_action_candidates(metrics),
        "금지표현": list(GENERIC_ACTION_WORDS),
        "신규재구매": metrics["customers"],
        "홀_메뉴_top3": metrics["menu_top3"],
        "전일에_없던_주문메뉴": metrics.get("new_menus", []),
        "전주동요일대비": metrics["compare_prev_week"],
        "최근4주평균대비": metrics["compare_4w_avg"],
        "최근5일평균대비": metrics["compare_5d_avg"],
    }


def _llm_prompt(metrics: dict[str, Any]) -> str:
    payload = _llm_payload(metrics)
    return (
        "역할: 당신은 F&B 직영점의 일일 매출 운영 리포트를 작성하는 분석 담당자입니다.\n"
        "목표: 제공된 지표만으로 전일 대비 변화의 핵심과 오늘 바로 확인할 운영 조치를 JSON으로 작성합니다.\n"
        "언어: 반드시 한국어로만 씁니다.\n"
        "근거 제한: 숫자와 사실은 입력 JSON에 있는 값만 사용합니다. 추정 매출, 임의 시간대, 임의 플랫폼 수치는 만들지 않습니다.\n"
        "최종 사용처: summary는 로그와 검수에 사용되고, reason/today_action/telegram_message는 후처리 템플릿에서 재검증됩니다.\n"
        "\n"
        "판단 순서:\n"
        "1. 전일대비 매출방향을 먼저 판단합니다.\n"
        "2. 매출 감소일이면 홀/배달 중 감소 기여가 큰 채널을 우선합니다.\n"
        "3. 주문수와 객단가 중 하락 영향이 큰 항목을 구분합니다.\n"
        "4. 매출 증가 또는 보합일이면 문제 원인으로 단정하지 않고, 감소한 보조 지표만 확인 포인트로 둡니다.\n"
        "5. 적용된_상황과 상황별_가이드라인에서 오늘 실행 가능한 조치를 고릅니다.\n"
        "\n"
        "해석 규칙:\n"
        "- 객단가 상승은 문제 원인이 아니라 매출 방어 요인으로 표현합니다.\n"
        "- 주문수 감소와 객단가 상승이 함께 있으면 '매출은 방어했지만 주문수 회복이 우선'으로 해석합니다.\n"
        "- 신규/재구매 데이터가 0이면 고객 흐름을 단정하지 말고 데이터 확인 대상으로만 언급합니다.\n"
        "- 최근5일평균과 전주동요일대비는 전일대비 판단을 보조하는 근거로만 사용합니다.\n"
        "\n"
        "문체 규칙:\n"
        "- 운영자가 11시 전, 점심 피크 전, 저녁 피크 전 실행할 수 있는 말로 씁니다.\n"
        "- 각 문장은 짧게 쓰고, 한 문장에 원인과 조치를 섞지 않습니다.\n"
        "- 추상적 처방 대신 영업상태, 품절, 노출, 접수 지연, 시간대, 메뉴 준비처럼 확인 가능한 항목을 씁니다.\n"
        "- 금지 표현: 캠페인, 이벤트, 할인, 광고 예산, 매출 회복에 집중.\n"
        "\n"
        "출력 필드 규칙:\n"
        "- summary: 전일 대비 핵심 변화 1문장, 70자 이내, 숫자 근거 1개 포함.\n"
        "- reason: 주요 판단 1문장, 60자 이내, 주문수 또는 객단가 숫자 근거 1개 포함.\n"
        "- today_action: 2개 배열, 각 45자 이내, 오늘 바로 확인할 작업만 작성.\n"
        "- telegram_message: 보고용 문장. '핵심 해석(AI)'과 '제안방향(AI)' 문구를 반드시 포함.\n"
        "\n"
        "나쁜 예: 오늘은 배달 주문 유치 캠페인과 메뉴 할인 이벤트를 진행한다.\n"
        "좋은 예(감소일): 주문수 -54.0%로 객단가보다 하락 영향이 큽니다.\n"
        "좋은 예(증가일): 총매출은 유지됐지만 영수건수 -15.8%는 확인 필요합니다.\n"
        "\n"
        "아래 형식의 JSON 객체만 출력하세요. 마크다운, 코드블록, 설명 문장은 금지합니다.\n"
        "{\n"
        '  "summary": "전일 대비 핵심 변화 1문장",\n'
        '  "reason": "숫자로 판단한 주요 원인 1문장",\n'
        '  "today_action": ["오늘 할 일 1", "오늘 할 일 2"],\n'
        '  "telegram_message": "보고용 최종 멘트"\n'
        "}\n\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )


def _generate_llm_message(metrics: dict[str, Any], prompt: str | None = None) -> tuple[dict[str, Any], str]:
    prompt = prompt or _llm_prompt(metrics)
    try:
        from modules.transform.utility.qwen_client import query_qwen_json

        result = query_qwen_json(
            prompt,
            system_prompt=(
                "당신은 F&B 직영점 매출 운영 담당자입니다. "
                "전날 대비 변화 원인과 오늘 액션을 짧고 실행 가능하게 작성합니다. "
                "반드시 유효한 JSON 객체만 출력합니다."
            ),
        )
        if result.get("parse_error"):
            raise ValueError(result.get("parse_error"))
        required = ["summary", "reason", "today_action", "telegram_message"]
        if not all(str(result.get(k, "")).strip() for k in required):
            raise ValueError(f"LLM 응답 필수 키 누락: {result}")
        actions = result.get("today_action")
        if isinstance(actions, str):
            result["today_action"] = [part.strip() for part in actions.replace("\n", "/").split("/") if part.strip()]
        elif not isinstance(actions, list):
            result["today_action"] = []
        result = _polish_llm_result(metrics, result)
        if _is_generic_llm_result(result):
            raise ValueError(f"LLM 응답이 추상적 표현을 포함함: {result}")
        return result, "llm"
    except Exception as exc:
        logger.warning("Ollama 보고 멘트 생성 실패, fallback 사용: %s", exc)
        return _fallback_llm_result(metrics, reason=str(exc)[:120]), "fallback"


def _polish_llm_result(metrics: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    result["today_action"] = _operation_action_candidates(metrics)[:2]

    reason = _structured_reason_text(metrics)
    result["reason"] = reason
    result["telegram_message"] = _format_telegram_message(metrics, reason, result["today_action"])
    result["telegram_message"] = str(result["telegram_message"]).strip()[:4000]
    return result


def _order_value_judgment(metrics: dict[str, Any]) -> str:
    comp = metrics["compare_prev_day"]
    order_abs = abs(comp["orders_pct"])
    avg_abs = abs(comp["avg_order_value_pct"])
    if comp["sales_diff"] < 0:
        if order_abs >= avg_abs:
            return "주문수 감소 영향이 객단가보다 큼"
        return "객단가 하락 영향이 주문수보다 큼"
    if comp["sales_diff"] > 0:
        if comp["orders_diff"] < 0:
            return "객단가 상승으로 매출은 방어, 영수건수는 감소"
        return "매출 증가 흐름 유지"
    return "매출은 유지, 주문수와 객단가를 함께 확인"


def _primary_change_area(metrics: dict[str, Any]) -> str:
    channels = _channel_diffs(metrics)
    slots = _time_slot_diffs(metrics)
    candidates = [
        ("홀", channels["홀"]["sales_diff"]),
        ("배달", channels["배달"]["sales_diff"]),
        ("점심(홀)", slots["점심"]["sales_diff"]),
        ("저녁(홀)", slots["저녁"]["sales_diff"]),
    ]
    if metrics["compare_prev_day"]["sales_diff"] < 0:
        return min(candidates, key=lambda item: item[1])[0]
    return max(candidates, key=lambda item: item[1])[0]


def _structured_reason_text(metrics: dict[str, Any]) -> str:
    return " ".join(_core_interpretation_lines(metrics))


def _problem_reason_text(metrics: dict[str, Any]) -> str:
    comp = metrics["compare_prev_day"]
    judgment = _order_value_judgment(metrics)
    if comp["sales_diff"] < 0:
        area = _primary_change_area(metrics)
        return f"{area} 약세, {judgment} ({comp['orders_pct']:+.1f}% vs {comp['avg_order_value_pct']:+.1f}%)"
    channels = _channel_diffs(metrics)
    checks = []
    if comp["orders_diff"] < 0:
        checks.append(f"영수건수 {comp['orders_pct']:+.1f}%")
    if channels["배달"]["sales_diff"] < 0:
        checks.append(f"배달 {channels['배달']['sales_pct']:+.1f}%")
    if channels["홀"]["sales_diff"] < 0:
        checks.append(f"홀 {channels['홀']['sales_pct']:+.1f}%")
    if checks:
        return "총매출은 유지됐지만 " + ", ".join(checks[:2]) + "가 약점"
    return judgment


def _is_data_not_ready(metrics: dict[str, Any]) -> bool:
    return metrics.get("status") != "ok" or metrics["current"]["전체"]["orders"] <= 0


def _core_interpretation_lines(metrics: dict[str, Any]) -> list[str]:
    if _is_data_not_ready(metrics):
        return [
            "기준일 매출 데이터가 없거나 0건입니다.",
            "매출 흐름 판단 전에 unified_sales, OKPOS, 배달 수집 상태를 먼저 확인해야 합니다.",
        ]

    comp = metrics["compare_prev_day"]
    channels = _channel_diffs(metrics)
    slot_diffs = _time_slot_diffs(metrics)
    lines = []
    if comp["sales_diff"] >= 0 and comp["orders_diff"] < 0:
        lines.append(f"총매출은 {comp['sales_pct']:+.1f}%로 유지됐지만 영수건수는 {comp['orders_pct']:+.1f}% 감소했습니다.")
        lines.append("매출 문제라기보다 주문수 회복이 우선인 흐름입니다.")
    elif comp["sales_diff"] < 0:
        if comp["orders_diff"] < 0 and comp["avg_order_value_diff"] >= 0:
            lines.append(f"총매출이 {comp['sales_pct']:+.1f}% 감소했고 영수건수도 {comp['orders_pct']:+.1f}% 감소했습니다.")
            lines.append("객단가보다 주문수 감소가 매출 하락의 핵심 원인입니다.")
        elif comp["avg_order_value_diff"] < 0:
            lines.append(f"총매출이 {comp['sales_pct']:+.1f}% 감소했고 객단가도 {comp['avg_order_value_pct']:+.1f}% 하락했습니다.")
            lines.append("주문수보다 주문당 금액 하락을 먼저 확인해야 합니다.")
        else:
            lines.append(f"총매출이 {comp['sales_pct']:+.1f}% 감소했고 영수건수는 {comp['orders_pct']:+.1f}% 변동했습니다.")
    else:
        lines.append("총매출과 영수건수 모두 전일 흐름을 유지했습니다.")
    if comp["avg_order_value_diff"] > 0 and slot_diffs["저녁"]["sales_diff"] > 0:
        lines.append(f"객단가 {comp['avg_order_value_pct']:+.1f}% 상승과 홀 저녁 매출 {slot_diffs['저녁']['sales_pct']:+.1f}% 증가가 매출을 방어했습니다.")
    elif comp["avg_order_value_diff"] > 0:
        lines.append(f"객단가 {comp['avg_order_value_pct']:+.1f}% 상승이 매출을 방어한 구조입니다.")
    elif slot_diffs["저녁"]["sales_diff"] > 0:
        lines.append(f"홀 저녁 매출 {slot_diffs['저녁']['sales_pct']:+.1f}% 증가가 금일 매출의 긍정 요인입니다.")
    if channels["배달"]["sales_diff"] < 0:
        lines.append(f"다만 배달 매출 {channels['배달']['sales_pct']:+.1f}% 감소는 오늘 가장 먼저 보완할 약점입니다.")
    elif channels["홀"]["sales_diff"] < 0:
        lines.append(f"다만 홀 매출 {channels['홀']['sales_pct']:+.1f}% 감소는 다음날 우선 확인이 필요합니다.")
    return lines


def _order_cross_daily_path(report_date: str) -> Path:
    ymd = datetime.strptime(report_date, "%Y-%m-%d").strftime("%y%m%d")
    return ORDER_CROSS_DIR / f"order_cross_{ymd}.parquet"


def _recommended_menu_combo_lines(metrics: dict[str, Any]) -> list[str]:
    path = _order_cross_daily_path(metrics["report_date"])
    if not path.exists():
        logger.warning("order_cross 추천 조합 파일 없음: %s", path)
        return []

    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        logger.warning("order_cross 추천 조합 로드 실패: %s | %s", path, exc)
        return []

    required = {"store", "main_name", "pair_type", "pair_name", "co_order_cnt", "co_qty", "co_amount"}
    missing = required - set(df.columns)
    if missing:
        logger.warning("order_cross 추천 조합 컬럼 누락: %s | %s", path, sorted(missing))
        return []

    target = df[
        df["store"].fillna("").astype(str).str.strip().eq(STORE_DISPLAY_NAME)
        & df["pair_type"].fillna("").astype(str).str.strip().isin(ORDER_CROSS_PAIR_TYPE_PRIORITY.keys())
    ].copy()
    if target.empty:
        return []

    for col in ("main_name", "pair_name", "pair_type"):
        target[col] = target[col].fillna("").astype(str).str.strip()
    target = target[target["main_name"].ne("") & target["pair_name"].ne("")]
    target = target[target["main_name"].ne(target["pair_name"])]
    for excluded in ORDER_CROSS_EXCLUDED_COMBO_WORDS:
        target = target[
            ~target["main_name"].str.contains(excluded, regex=False)
            & ~target["pair_name"].str.contains(excluded, regex=False)
        ]
    if target.empty:
        return []

    for col in ("co_order_cnt", "co_qty", "co_amount"):
        target[col] = pd.to_numeric(target[col], errors="coerce").fillna(0)
    target["_pair_type_rank"] = target["pair_type"].map(ORDER_CROSS_PAIR_TYPE_PRIORITY).fillna(99)
    total_count = float(target["co_order_cnt"].sum())
    if total_count <= 0:
        return []

    lines = []
    for pair_type in ORDER_CROSS_PAIR_TYPE_PRIORITY:
        typed = target[target["pair_type"].eq(pair_type)]
        if typed.empty:
            continue
        top = typed.sort_values(
            ["co_order_cnt", "co_qty", "co_amount"],
            ascending=[False, False, False],
        ).iloc[0]
        count = int(top["co_order_cnt"])
        pct = round(count / total_count * 100, 1)
        lines.append(
            f"메인+{pair_type}: {top['main_name']} + {top['pair_name']} ({count}건, 전체메뉴 조합 중 {pct:.1f}%)"
        )
    return lines


def _proposal_direction_groups(metrics: dict[str, Any]) -> dict[str, list[str]]:
    if _is_data_not_ready(metrics):
        return {
            "배달": [
                "배달 매출 데이터가 0건입니다. 배민/쿠팡 수집 파일 생성 여부와 주문 누락 여부를 먼저 확인합니다."
            ],
            "홀": [
                "홀 매출 데이터가 0건입니다. OKPOS/홀 매출 수집과 unified_sales 적재 여부를 먼저 확인합니다."
            ],
        }

    comp = metrics["compare_prev_day"]
    channels = _channel_diffs(metrics)
    slot_diffs = _time_slot_diffs(metrics)
    menu_combo_lines = _recommended_menu_combo_lines(metrics)
    delivery = []
    hall = []
    if channels["배달"]["sales_diff"] < 0:
        delivery.append(
            f"배달 매출이 {channels['배달']['sales_pct']:+.1f}% 감소했습니다. 11시 전 배민/쿠팡 영업상태, 예상 조리시간, 품절 메뉴를 확인하고 광고 비용 증액 여부를 검토합니다."
        )
    elif comp["orders_diff"] < 0:
        delivery.append(
            f"총영수건수가 {comp['orders_pct']:+.1f}% 감소했습니다. 11~13시와 17~20시 중 빠진 구간을 보고 배달 주문 유입을 확인합니다."
        )
    if comp["avg_order_value_diff"] > 0:
        hall.append(
            f"객단가가 {comp['avg_order_value_pct']:+.1f}% 상승했습니다. 점심 전 추천 메뉴와 추가 주문 멘트를 정해 직원에게 공유합니다."
        )
    if slot_diffs["저녁"]["sales_diff"] > 0:
        if menu_combo_lines:
            hall.append(
                f"저녁 매출이 {slot_diffs['저녁']['sales_pct']:+.1f}% 증가했습니다. 추천 조합은 {' / '.join(menu_combo_lines)}입니다. 직원 추천 멘트로 제안합니다."
            )
        else:
            hall.append(
                f"저녁 매출이 {slot_diffs['저녁']['sales_pct']:+.1f}% 증가했습니다. 저녁에 잘 팔린 메인+토핑, 메인+사이드, 메인+주류 조합을 직원 추천 멘트로 제안합니다."
            )
    if channels["홀"]["sales_diff"] < 0:
        hall.append(
            f"홀 매출이 {channels['홀']['sales_pct']:+.1f}% 감소했습니다. 입구 안내, 테이블 회전, 포장 응대 멘트를 피크 전 확인합니다."
        )
    if not delivery:
        delivery.append("오픈 전 배민/쿠팡 영업상태, 예상 조리시간, 품절 메뉴를 먼저 확인합니다.")
    if not hall:
        hall.append("점심 전 입구 안내, 테이블 회전, 포장 응대 담당을 확인합니다.")
    return {
        "배달": _dedupe_actions(delivery)[:2],
        "홀": _dedupe_actions(hall)[:2],
    }


def _proposal_direction_lines(metrics: dict[str, Any], actions: list[str] | None = None) -> list[str]:
    groups = _proposal_direction_groups(metrics)
    return [*groups["배달"], *groups["홀"]][:3]


def _value_change_line(label: str, cur: dict[str, int], prev: dict[str, int]) -> str:
    return f"- {label}: {_fmt_won(cur['sales'])} ({_pct(cur['sales'], prev['sales']):+.1f}%)"


def _format_telegram_message(metrics: dict[str, Any], reason: str, actions: list[str]) -> str:
    cur = metrics["current"]
    prev = metrics["previous"]
    comp = metrics["compare_prev_day"]
    report_dt = datetime.strptime(metrics["report_date"], "%Y-%m-%d")
    report_date = f"{report_dt.strftime('%m/%d')}({'월화수목금토일'[report_dt.weekday()]})"
    slot_diffs = _time_slot_diffs(metrics)
    proposal_groups = _proposal_direction_groups(metrics)

    lines = [
        f"📊 {STORE_DISPLAY_NAME} 직영점 일일 현황 요약",
        "",
        f"▶ {report_date} 실적",
        f"- 총매출: {_fmt_won(cur['전체']['sales'])} (전일 {comp['sales_pct']:+.1f}%)",
        f"- 총영수건수: {_fmt_int(cur['전체']['orders'])}건 ({comp['orders_pct']:+.1f}%)",
        f"- 객단가: {_fmt_won(cur['전체']['avg_order_value'])} ({comp['avg_order_value_pct']:+.1f}%)",
        _value_change_line("홀 매출", cur["홀"], prev["홀"]),
        _value_change_line("점심(홀) 매출", metrics["time_slots"]["점심"], metrics["previous_time_slots"]["점심"]),
        _value_change_line("저녁(홀) 매출", metrics["time_slots"]["저녁"], metrics["previous_time_slots"]["저녁"]),
        _value_change_line("배달 매출", cur["배달"], prev["배달"]),
        "",
        "▶ 핵심 해석(AI)",
        *_core_interpretation_lines(metrics),
        "",
        "▶ 배달 제안방향(AI)",
        *[f"- {_wrap18(action, 34)}" for action in proposal_groups["배달"]],
        "",
        "▶ 홀 제안방향(AI)",
        *[f"- {_wrap18(action, 34)}" for action in proposal_groups["홀"]],
    ]
    return "\n".join(line for line in lines if line is not None)


def _detail_analysis_lines(metrics: dict[str, Any]) -> list[str]:
    comp = metrics["compare_prev_day"]
    channels = _channel_diffs(metrics)
    slot_diffs = _time_slot_diffs(metrics)
    return [
        "## 상세 분석",
        "",
        "### 1. 배달 주문 감소 구간 분해",
        f"- 원인 가설: 총매출은 유지됐지만 영수건수 {comp['orders_pct']:+.1f}%, 배달 매출 {channels['배달']['sales_pct']:+.1f}%로 배달 주문수 감소 영향이 큽니다.",
        "- 확인 데이터: 배민/쿠팡별 주문수, 11~13시·17~20시 시간대별 주문수, 전일 대비 감소 메뉴",
        "- 실행 액션: 감소폭이 큰 플랫폼과 시간대의 대표 메뉴 노출, 품절 상태를 우선 조정합니다.",
        "- 다음날 확인 지표: 배달 영수건수와 배달 매출 회복 여부",
        "",
        "### 2. 객단가 상승 원인 확인",
        f"- 원인 가설: 영수건수는 줄었지만 객단가 {comp['avg_order_value_pct']:+.1f}%로 총매출을 방어한 구조입니다.",
        "- 확인 데이터: 옵션 추가율, 세트/대용량 메뉴 비중, 홀 저녁 주문 메뉴",
        "- 실행 액션: 객단가 상승에 기여한 메뉴 또는 옵션을 확인해 추천 멘트로 고정합니다.",
        "- 다음날 확인 지표: 객단가 3만원 이상 유지 여부",
        "",
        "### 3. 홀 저녁 매출 재현",
        f"- 원인 가설: 홀 저녁 매출 {slot_diffs['저녁']['sales_pct']:+.1f}%가 금일 매출 방어의 핵심 역할을 했습니다.",
        "- 확인 데이터: 저녁 홀 주문 메뉴, 방문 고객 유형, 테이블당 주문금액",
        "- 실행 액션: 저녁 시간대에 많이 팔린 메뉴와 옵션을 다음날 직원 추천 멘트로 적용합니다.",
        "- 다음날 확인 지표: 저녁 홀 매출과 테이블당 객단가 유지 여부",
        "",
    ]


def _guide_lines(metrics: dict[str, Any]) -> list[str]:
    comp = metrics["compare_prev_day"]
    channels = _channel_diffs(metrics)
    customers = metrics["customers"]
    top_menu = metrics["menu_top3"][0]["menu"] if metrics.get("menu_top3") else "주력 메뉴"
    lines = []

    delivery = channels["배달"]
    hall = channels["홀"]
    if abs(delivery["sales_diff"]) >= abs(hall["sales_diff"]):
        lines.extend([
            f"1. 원인: 배달 매출 {_diff_text(delivery['sales_diff'], 0, '원')}({delivery['sales_pct']:+.1f}%), 주문 {_diff_text(delivery['orders_diff'], 0, '건')}({delivery['orders_pct']:+.1f}%).",
            "   조치: 11시 전 배민/쿠팡 영업상태, 노출순서, 품절 메뉴를 확인하고 비정상 항목을 즉시 수정.",
            "   확인: 점심 피크 전 배달 주문수가 전일 같은 시간대 대비 회복되는지 본다.",
        ])
    else:
        lines.extend([
            f"1. 원인: 홀 매출 {_diff_text(hall['sales_diff'], 0, '원')}({hall['sales_pct']:+.1f}%), 주문 {_diff_text(hall['orders_diff'], 0, '건')}({hall['orders_pct']:+.1f}%).",
            "   조치: 점심 전 입구 안내, 테이블 회전, 포장 응대 멘트를 담당자에게 다시 공유.",
            "   확인: 점심 주문수와 테이블 회전 지연 여부를 전일 같은 시간대와 비교.",
        ])

    if comp["orders_diff"] < 0 and abs(comp["orders_pct"]) >= abs(comp["avg_order_value_pct"]):
        lines.extend([
            f"2. 원인: 총 주문수 {_diff_text(comp['orders_diff'], 0, '건')}({comp['orders_pct']:+.1f}%)로 객단가 변화({comp['avg_order_value_pct']:+.1f}%)보다 영향이 큼.",
            "   조치: 주문 유입이 빠진 채널부터 영업상태·노출·품절·접수 지연을 먼저 확인.",
            f"   확인: 마감 전 주문수 차이가 {_diff_text(comp['orders_diff'], 0, '건')}에서 얼마나 줄었는지 확인.",
        ])
    elif comp["avg_order_value_diff"] < 0:
        lines.extend([
            f"2. 원인: 객단가 {_diff_text(comp['avg_order_value_diff'], 0, '원')}({comp['avg_order_value_pct']:+.1f}%).",
            f"   조치: `{top_menu}` 주문 접수 시 사리·음료 추가 제안 멘트를 통일.",
            "   확인: 객단가가 전일 대비 마이너스 폭을 줄이는지 확인.",
        ])

    if comp["avg_order_value_diff"] < 0:
        lines.extend([
            f"3. 원인: 객단가 {_diff_text(comp['avg_order_value_diff'], 0, '원')}({comp['avg_order_value_pct']:+.1f}%).",
            f"   조치: `{top_menu}` 주문 접수 시 사리·음료 추가 제안 멘트를 통일.",
            "   확인: 객단가가 전일 대비 마이너스 폭을 줄이는지 확인.",
        ])
    elif customers.get("new_ratio", 0.0) <= 20:
        lines.extend([
            f"3. 원인: 신규 구매비율 {customers.get('new_ratio', 0.0)}%, 신규 {customers.get('new', 0)}건.",
            "   조치: 첫 주문 고객 응대 누락이 없도록 배달 메모·홀 첫 방문 응대를 분리 확인.",
            "   확인: 다음날 신규 구매비율이 전일 대비 상승하는지 확인.",
        ])

    return lines[:9]


def _cause_text(metrics: dict[str, Any]) -> str:
    comp = metrics["compare_prev_day"]
    order_abs = abs(comp["orders_pct"])
    avg_abs = abs(comp["avg_order_value_pct"])
    if comp["sales_diff"] < 0:
        if order_abs >= avg_abs:
            return f"매출 감소 기여는 객단가({comp['avg_order_value_pct']:+.1f}%)보다 주문수({comp['orders_pct']:+.1f}%) 쪽이 더 큽니다."
        return f"매출 감소 기여는 주문수({comp['orders_pct']:+.1f}%)보다 객단가({comp['avg_order_value_pct']:+.1f}%) 쪽이 더 큽니다."
    if comp["sales_diff"] > 0:
        if order_abs >= avg_abs:
            return f"매출 증가 기여는 객단가({comp['avg_order_value_pct']:+.1f}%)보다 주문수({comp['orders_pct']:+.1f}%) 쪽이 더 큽니다."
        return f"매출 증가 기여는 주문수({comp['orders_pct']:+.1f}%)보다 객단가({comp['avg_order_value_pct']:+.1f}%) 쪽이 더 큽니다."
    return f"매출은 전일과 동일하며, 주문수 {comp['orders_pct']:+.1f}% / 객단가 {comp['avg_order_value_pct']:+.1f}%입니다."


def _customer_flow_text(metrics: dict[str, Any]) -> str:
    new_ratio = metrics["customers"].get("new_ratio", 0.0)
    total = metrics["customers"].get("new", 0) + metrics["customers"].get("returning", 0)
    returning_ratio = metrics["customers"].get("returning_ratio", 0.0)
    return f"카드 기준 구매 {total}건 중 재구매 {returning_ratio}%입니다."


def _menu_summary_text(metrics: dict[str, Any]) -> str:
    new_menus = metrics.get("new_menus", [])
    if new_menus:
        quoted = ", ".join(f"`{name}`" for name in new_menus[:3])
        return f"전일에 없던 주문 메뉴는 {quoted} 중심으로 발생했습니다."
    top = metrics["menu_top3"][:3]
    if top:
        quoted = ", ".join(f"`{row['menu']}`" for row in top)
        return f"주요 주문 메뉴는 {quoted} 중심입니다."
    return "메뉴별 특이 주문은 확인되지 않았습니다."


def _tomorrow_check_text(metrics: dict[str, Any]) -> str:
    checks = []
    comp = metrics["compare_prev_day"]
    if comp["orders_diff"] < 0:
        checks.append("주문수 회복 여부")
    if metrics["customers"].get("new_ratio", 0.0) <= 20:
        checks.append("신규 구매비율 반등 여부")
    if comp["avg_order_value_diff"] < 0:
        checks.append("객단가 회복 여부")
    if not checks:
        checks.append("현재 매출 흐름 유지 여부")
    return "와 ".join(checks[:2]) + "를 전일 대비 기준으로 우선 확인하겠습니다."


def _is_generic_llm_result(result: dict[str, Any]) -> bool:
    text = " ".join([
        str(result.get("summary", "")),
        str(result.get("reason", "")),
        " ".join(str(x) for x in result.get("today_action", []) if x is not None),
        str(result.get("telegram_message", "")),
    ])
    if any(word in text for word in GENERIC_ACTION_WORDS):
        return True
    actions = result.get("today_action", [])
    if not isinstance(actions, list) or len([a for a in actions if str(a).strip()]) < 2:
        return True
    return False


def _save_llm_debug(
    report_date: str,
    metrics: dict[str, Any],
    prompt: str,
    llm_result: dict[str, Any],
    llm_status: str,
) -> None:
    payload = {
        "report_date": report_date,
        "store": STORE_DISPLAY_NAME,
        "created_at": pendulum.now("Asia/Seoul").format("YYYY-MM-DD HH:mm:ss"),
        "llm_status": llm_status,
        "purpose": "전일 대비 매출 변화 원인을 해석하고 오늘 실행할 일을 만들기 위한 로컬 LLM 입력/출력 기록",
        "metrics_json_for_llm": _llm_payload(metrics),
        "prompt": prompt,
        "llm_result": llm_result,
    }
    debug_path = OUTPUT_DIR / f"llm_payload_{report_date}.json"
    debug_text = json.dumps(payload, ensure_ascii=False, indent=2)
    debug_path.write_text(debug_text, encoding="utf-8")
    (OUTPUT_DIR / "latest_llm_payload.json").write_text(debug_text, encoding="utf-8")

    log_block = "\n".join([
        "",
        f"## {report_date} {STORE_DISPLAY_NAME}",
        "",
        f"- 생성시각: {payload['created_at']}",
        f"- LLM 상태: {llm_status}",
        "",
        "### 입력 JSON",
        "```json",
        json.dumps(payload["metrics_json_for_llm"], ensure_ascii=False, indent=2),
        "```",
        "",
        "### 프롬프트",
        "```text",
        prompt,
        "```",
        "",
        "### LLM 응답",
        "```json",
        json.dumps(llm_result, ensure_ascii=False, indent=2),
        "```",
        "",
    ])
    if not LLM_LOG_MD.exists():
        LLM_LOG_MD.write_text("# daily_corporate_store_report LLM log\n", encoding="utf-8")
    with LLM_LOG_MD.open("a", encoding="utf-8") as f:
        f.write(log_block)


def _build_metrics(report_date: str) -> dict[str, Any]:
    ref = datetime.strptime(report_date, "%Y-%m-%d").date()
    prev_date = (ref - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_week_date = (ref - timedelta(days=7)).strftime("%Y-%m-%d")

    cur_df = _load_store_day(report_date)
    prev_df = _load_store_day(prev_date)
    prev_week_df = _load_store_day(prev_week_date)

    cur = _channel_agg(cur_df)
    prev = _channel_agg(prev_df)
    prev_week = _channel_agg(prev_week_df)
    avg_4w = _rolling_4w_avg(ref)
    avg_5d = _rolling_5d_avg(ref)

    hall_df = cur_df[cur_df.get("channel", pd.Series(dtype=str)).eq("홀")] if not cur_df.empty else cur_df
    prev_hall_df = prev_df[prev_df.get("channel", pd.Series(dtype=str)).eq("홀")] if not prev_df.empty else prev_df
    metrics = {
        "report_date": report_date,
        "previous_date": prev_date,
        "prev_week_date": prev_week_date,
        "status": "ok" if not cur_df.empty else "data_not_ready",
        "current": cur,
        "previous": prev,
        "prev_week": prev_week,
        "customers": _customer_counts(report_date),
        "time_slots": _time_slot_agg(hall_df),
        "previous_time_slots": _time_slot_agg(prev_hall_df),
        "menu_top3": _menu_top(hall_df),
        "new_menus": _new_menu_names(cur_df, prev_df),
        "compare_prev_day": _compare(cur["전체"], prev["전체"]),
        "compare_prev_week": _compare(cur["전체"], prev_week["전체"]),
        "compare_4w_avg": _compare(cur["전체"], avg_4w),
        "compare_5d_avg": _compare(cur["전체"], avg_5d),
    }
    return metrics


def _markdown_report(metrics: dict[str, Any], llm_result: dict[str, Any], llm_status: str) -> str:
    cur = metrics["current"]
    prev = metrics["previous"]
    comp = metrics["compare_prev_day"]
    comp_5d = metrics["compare_5d_avg"]
    slot_diffs = _time_slot_diffs(metrics)
    customers = metrics["customers"]
    menu_text = ", ".join(
        f"{m['menu']} {m['qty']}개/{_fmt_won(m['sales'])}({m['share']}%)"
        for m in metrics["menu_top3"]
    ) or "데이터 없음"
    actions = llm_result.get("today_action") or []
    if isinstance(actions, str):
        actions = [actions]
    action_text = "\n".join(f"- {a}" for a in actions) or "- 확인 필요"

    return "\n".join([
        f"# {STORE_DISPLAY_NAME} 일일 매출 감시 리포트",
        "",
        f"- 기준일: {metrics['report_date']}",
        f"- 비교일: {metrics['previous_date']}",
        f"- 상태: {metrics['status']}",
        f"- LLM 상태: {llm_status}",
        "",
        "## 전일 대비",
        f"- 총매출: {_fmt_won(cur['전체']['sales'])} / 전일 {_fmt_won(prev['전체']['sales'])} / 증감 {_diff_text(cur['전체']['sales'], prev['전체']['sales'], '원')} ({comp['sales_pct']:+.1f}%)",
        f"- 주문수: {_fmt_int(cur['전체']['orders'])}건 / 전일 {_fmt_int(prev['전체']['orders'])}건 / 증감 {_diff_text(cur['전체']['orders'], prev['전체']['orders'], '건')} ({comp['orders_pct']:+.1f}%)",
        f"- 객단가: {_fmt_won(cur['전체']['avg_order_value'])} / 전일 {_fmt_won(prev['전체']['avg_order_value'])} / 증감 {_diff_text(cur['전체']['avg_order_value'], prev['전체']['avg_order_value'], '원')} ({comp['avg_order_value_pct']:+.1f}%)",
        "",
        "## 채널",
        f"- 홀: {_fmt_won(cur['홀']['sales'])}, {_fmt_int(cur['홀']['orders'])}건, 객단가 {_fmt_won(cur['홀']['avg_order_value'])}",
        f"- 배달: {_fmt_won(cur['배달']['sales'])}, {_fmt_int(cur['배달']['orders'])}건, 객단가 {_fmt_won(cur['배달']['avg_order_value'])}",
        "",
        "## 홀 시간대 전일 대비",
        f"- 점심(홀): {_fmt_won(metrics['time_slots']['점심']['sales'])}, {_fmt_int(metrics['time_slots']['점심']['orders'])}건 / 매출 {slot_diffs['점심']['sales_pct']:+.1f}%, 주문 {slot_diffs['점심']['orders_pct']:+.1f}%",
        f"- 저녁(홀): {_fmt_won(metrics['time_slots']['저녁']['sales'])}, {_fmt_int(metrics['time_slots']['저녁']['orders'])}건 / 매출 {slot_diffs['저녁']['sales_pct']:+.1f}%, 주문 {slot_diffs['저녁']['orders_pct']:+.1f}%",
        "",
        "## 최근5일 평균 대비",
        f"- 총매출: {_diff_text(comp_5d['sales_diff'], 0, '원')} ({comp_5d['sales_pct']:+.1f}%)",
        f"- 주문수: {_diff_text(comp_5d['orders_diff'], 0, '건')} ({comp_5d['orders_pct']:+.1f}%)",
        f"- 객단가: {_diff_text(comp_5d['avg_order_value_diff'], 0, '원')} ({comp_5d['avg_order_value_pct']:+.1f}%)",
        "",
        "## 신규/재구매",
        f"- 신규 {customers['new']}건, 재구매 {customers['returning']}건, 재구매비율 {customers['returning_ratio']}%",
        "",
        "## 홀 메뉴 TOP3",
        f"- {menu_text}",
        "",
        "## 보고 멘트",
        str(llm_result.get("telegram_message", "")).strip(),
        "",
        "## 오늘 할 일",
        action_text,
        "",
        *_detail_analysis_lines(metrics),
    ])


def _summary_row(metrics: dict[str, Any], llm_result: dict[str, Any], llm_status: str) -> dict[str, Any]:
    cur = metrics["current"]
    prev = metrics["previous"]
    comp = metrics["compare_prev_day"]
    actions = llm_result.get("today_action") or []
    if isinstance(actions, list):
        action_text = " / ".join(str(a).strip() for a in actions if str(a).strip())
    else:
        action_text = str(actions).strip()
    return {
        "report_date": metrics["report_date"],
        "store": STORE_DISPLAY_NAME,
        "status": metrics["status"],
        "total_sales": cur["전체"]["sales"],
        "total_orders": cur["전체"]["orders"],
        "avg_order_value": cur["전체"]["avg_order_value"],
        "new_customers": metrics["customers"]["new"],
        "returning_customers": metrics["customers"]["returning"],
        "returning_ratio": metrics["customers"]["returning_ratio"],
        "prev_sales": prev["전체"]["sales"],
        "prev_orders": prev["전체"]["orders"],
        "sales_diff": comp["sales_diff"],
        "sales_pct": comp["sales_pct"],
        "orders_diff": comp["orders_diff"],
        "orders_pct": comp["orders_pct"],
        "avg_order_value_diff": comp["avg_order_value_diff"],
        "avg_order_value_pct": comp["avg_order_value_pct"],
        "hall_sales": cur["홀"]["sales"],
        "hall_orders": cur["홀"]["orders"],
        "delivery_sales": cur["배달"]["sales"],
        "delivery_orders": cur["배달"]["orders"],
        "main_reason": str(llm_result.get("reason", "")).strip(),
        "today_action": action_text,
        "telegram_message": str(llm_result.get("telegram_message", "")).strip(),
        "llm_status": llm_status,
        "created_at": pendulum.now("Asia/Seoul").format("YYYY-MM-DD HH:mm:ss"),
    }


def _upsert_summary(row: dict[str, Any]) -> None:
    if SUMMARY_CSV.exists():
        df = pd.read_csv(SUMMARY_CSV, dtype=str, encoding="utf-8-sig")
    else:
        df = pd.DataFrame(columns=SUMMARY_COLUMNS)
    df = df[df.get("report_date", pd.Series(dtype=str)).astype(str).ne(row["report_date"])]
    if df.empty:
        out = pd.DataFrame([row])
    else:
        out = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    for col in SUMMARY_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[SUMMARY_COLUMNS].sort_values("report_date")
    out.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")


def build_report_mart(
    sale_date: str | None = None,
    conf: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    send_report: bool = True,
) -> str:
    conf = conf or {}
    report_date = _normalize_date(sale_date) if sale_date else _resolve_sale_date(conf, context)
    metrics = _build_metrics(report_date)
    llm_prompt = _llm_prompt(metrics)
    llm_result, llm_status = _generate_llm_message(metrics, llm_prompt)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    row = _summary_row(metrics, llm_result, llm_status)
    _upsert_summary(row)
    _save_llm_debug(report_date, metrics, llm_prompt, llm_result, llm_status)

    md = _markdown_report(metrics, llm_result, llm_status)
    report_md = OUTPUT_DIR / f"daily_corporate_store_report_{report_date}.md"
    report_txt = OUTPUT_DIR / f"telegram_message_{report_date}.txt"
    report_md.write_text(md, encoding="utf-8")
    report_txt.write_text(row["telegram_message"], encoding="utf-8")
    (OUTPUT_DIR / "latest.md").write_text(md, encoding="utf-8")
    (OUTPUT_DIR / "latest_telegram_message.txt").write_text(row["telegram_message"], encoding="utf-8")

    send_enabled = send_report and str(conf.get("send_telegram", "true")).strip().lower() not in {"0", "false", "n", "no"}
    if send_enabled and row["telegram_message"]:
        telegram_status = "sent" if send_telegram(row["telegram_message"]) else "skipped"
    else:
        telegram_status = "skipped"

    logger.info("직영점 일일 보고 mart 저장 완료: %s", report_date)
    return (
        f"daily corporate store report 저장 완료: {report_date} "
        f"status={metrics['status']} llm={llm_status} telegram={telegram_status}"
    )


def build_report_mart_task(**context) -> str:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return build_report_mart(conf=conf, context=context)
