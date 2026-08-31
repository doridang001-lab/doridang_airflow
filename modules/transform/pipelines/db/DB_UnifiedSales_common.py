"""
unified_sales 채널별 파이프라인 공통 모듈.

- 스키마/저장 로직
- store 메타 로드(담당자/region/실오픈일)
- 공통 정규화 유틸
- 기존 parquet 재저장/재분류 유틸
"""

import logging
import hashlib
import json
import os
import re
import shutil
from datetime import datetime

import pandas as pd
import pendulum

from modules.transform.utility.paths import (
    ANALYTICS_DB,
    FIN_PRODUCT_CSV_PATH,
    LOCAL_DB,
    MART_DB,
    ONEDRIVE_DB,
    POSFEED_WHITELIST_CSV_PATH,
    existing_fin_product_csv_path,
)
from modules.transform.pipelines.db.DB_ItemIdAllocator import canonical_source

logger = logging.getLogger(__name__)

UNIFIED_ROOT = MART_DB / "unified_sales_grp"
UNIFIED_DAILY_RE = re.compile(r"^unified_sales_\d{6}\.parquet$")
CONFLICT_QUARANTINE_DIR = UNIFIED_ROOT / "_conflicts"
PLATFORM_NORMALIZE_MAP = {
    "배민1": "배달의민족",
}
MANUAL_FALLBACK_MARKER_ROOT = LOCAL_DB / "manual_fallback_markers"
MANUAL_REINGEST_MARKER_ROOT = LOCAL_DB / "manual_reingest_markers"
MANUAL_PARTIAL_MARKER_ROOT = LOCAL_DB / "manual_partial_markers"
MANUAL_PARTIAL_MIN_RATIO = 0.8
MANUAL_PARTIAL_MIN_GAP = 100_000
MANUAL_ITEM_DETAIL_GAP_MARKER_ROOT = LOCAL_DB / "manual_item_detail_gap_markers"
MANUAL_UNKNOWN_ITEM_NAME_FMT = "메뉴미상({label})"
TOORDER_DAILY_STORE_PLATFORM_PATH = (
    ANALYTICS_DB
    / "toorder_daily_store_platform"
    / "toorder_store_platform_daily.parquet"
)


def _kst_today_str() -> str:
    return pendulum.now("Asia/Seoul").strftime("%Y-%m-%d")


def is_canonical_unified_file(path) -> bool:
    """정규 일별 unified_sales parquet만 True."""
    return bool(UNIFIED_DAILY_RE.match(path.name))


def iter_unified_sales_files() -> list:
    """실제 unified_sales parquet만 반환한다.

    OneDrive 충돌본, 백필 백업, 원자적 쓰기 임시 파일은 단순 glob에
    같이 잡히므로 정규 일별 파일명만 허용한다.
    """
    if not UNIFIED_ROOT.exists():
        return []
    return sorted(
        path
        for path in UNIFIED_ROOT.glob("unified_sales_*.parquet")
        if is_canonical_unified_file(path)
    )


def normalize_unified_platforms(df: pd.DataFrame) -> pd.DataFrame:
    """unified_sales platform 표기를 저장 표준값으로 정규화한다."""
    if df is None or df.empty or "platform" not in df.columns:
        return df

    out = df.copy()
    platform = out["platform"].fillna("").astype(str).str.strip()
    normalized = platform.map(lambda value: PLATFORM_NORMALIZE_MAP.get(value, value))
    changed = ~platform.eq(normalized)
    if not changed.any():
        return df

    out["platform"] = normalized
    if "_pk" in out.columns:
        out["_pk"] = _make_unified_pk(out)
    return out


def save_unified_parquet(df: pd.DataFrame, path) -> None:
    """같은 폴더의 tmp 파일로 쓴 뒤 최종 경로를 원자 교체한다."""
    df = normalize_unified_platforms(df)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_parquet(tmp, index=False, engine="pyarrow")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def quarantine_conflict_copies() -> str:
    """비정규 unified_sales 파일을 격리하고 알림을 보낸다."""
    if not UNIFIED_ROOT.exists():
        return "충돌본 없음"

    targets = []
    for path in UNIFIED_ROOT.glob("unified_sales_*"):
        if not path.is_file():
            continue
        name = path.name
        if is_canonical_unified_file(path) or ".bak_" in name or name.endswith(".tmp"):
            continue
        targets.append(path)

    if not targets:
        return "충돌본 없음"

    quarantine_dir = CONFLICT_QUARANTINE_DIR / pendulum.now("Asia/Seoul").strftime("%Y%m%d")
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    moved_names = []
    for path in sorted(targets):
        dest = quarantine_dir / path.name
        if dest.exists():
            dest.unlink()
        shutil.move(str(path), str(dest))
        moved_names.append(path.name)

    try:
        from modules.transform.utility.notifier import send_telegram

        send_telegram(
            "[도리당] unified_sales OneDrive 충돌본 격리\n"
            f"건수: {len(moved_names)}\n"
            f"파일: {', '.join(moved_names)}"
        )
    except Exception as exc:
        logger.warning("충돌본 격리 텔레그램 알림 실패(무시): %s", exc)

    result = f"OK: 충돌본 격리 {len(moved_names)}건 | {', '.join(moved_names)}"
    logger.warning(result)
    return result


def pos_delivery_summary(
    date: str,
    store: str,
    platforms: set[str],
    manual_source: str,
) -> tuple[int, int, int]:
    """수동 결측 시 유지될 비수동 배달행의 금액/주문수/행수를 반환한다."""
    path = _unified_daily_path(date)
    if not path.exists():
        return 0, 0, 0

    try:
        df = pd.read_parquet(
            path,
            columns=["store", "platform", "source", "total_price", "order_cnt"],
        )
    except Exception as exc:
        logger.warning("수동 폴백 요약 로드 실패: %s | %s", path, exc)
        return 0, 0, 0

    if df.empty:
        return 0, 0, 0

    store_s = df["store"].fillna("").astype(str).str.strip()
    platform_s = df["platform"].fillna("").astype(str).str.strip()
    source_s = df["source"].fillna("").astype(str).str.strip()
    mask = (
        store_s.eq(str(store).strip())
        & platform_s.isin(platforms)
        & ~source_s.eq(str(manual_source).strip())
    )
    if not mask.any():
        return 0, 0, 0

    total_price = pd.to_numeric(df.loc[mask, "total_price"], errors="coerce").fillna(0).sum()
    order_cnt = pd.to_numeric(df.loc[mask, "order_cnt"], errors="coerce").fillna(0).sum()
    return int(total_price), int(order_cnt), int(mask.sum())


def toorder_delivery_summary(
    date: str,
    store: str,
    platforms: set[str],
) -> tuple[int, int, int]:
    """ToOrder 일별 매장×플랫폼 기준 금액/영수수/행수를 반환한다."""
    path = TOORDER_DAILY_STORE_PLATFORM_PATH
    if not path.exists():
        return 0, 0, 0

    try:
        df = pd.read_parquet(
            path,
            columns=["date", "store", "platform", "price", "receipts_num"],
        )
    except Exception as exc:
        logger.warning("ToOrder 배달 요약 로드 실패: %s | %s", path, exc)
        return 0, 0, 0

    if df.empty:
        return 0, 0, 0

    date_s = df["date"].fillna("").astype(str).str.strip()
    store_s = df["store"].fillna("").astype(str).str.strip()
    platform_s = df["platform"].fillna("").astype(str).str.strip()
    mask = (
        date_s.eq(str(date).strip())
        & store_s.eq(str(store).strip())
        & platform_s.isin(platforms)
    )
    if not mask.any():
        return 0, 0, 0

    total_price = pd.to_numeric(df.loc[mask, "price"], errors="coerce").fillna(0).sum()
    order_cnt = pd.to_numeric(df.loc[mask, "receipts_num"], errors="coerce").fillna(0).sum()
    return int(total_price), int(order_cnt), int(mask.sum())


def delivery_baseline_summary(
    date: str,
    store: str,
    platforms: set[str],
    manual_source: str,
) -> dict:
    """수동수집 검증 기준 합계를 반환한다.

    ToOrder/POS가 모두 있으면 더 낮은 금액을 기준으로 사용해 기준 과대로 인한
    부분수집 오탐을 줄인다. 한쪽만 있으면 있는 쪽을 기준으로 사용한다.
    """
    toorder_total, toorder_order_cnt, toorder_rows = toorder_delivery_summary(
        date,
        store,
        platforms,
    )
    pos_total, pos_order_cnt, pos_rows = pos_delivery_summary(
        date,
        store,
        platforms,
        manual_source,
    )
    has_toorder = toorder_rows > 0 and toorder_total > 0
    has_pos = pos_rows > 0 and pos_total > 0
    if has_pos and (not has_toorder or pos_total <= toorder_total):
        baseline_label = "POS"
        baseline_total = pos_total
        baseline_order_cnt = pos_order_cnt
        baseline_rows = pos_rows
    elif has_toorder:
        baseline_label = "ToOrder"
        baseline_total = toorder_total
        baseline_order_cnt = toorder_order_cnt
        baseline_rows = toorder_rows
    else:
        baseline_label = "POS"
        baseline_total = pos_total
        baseline_order_cnt = pos_order_cnt
        baseline_rows = pos_rows

    return {
        "baseline_label": baseline_label,
        "baseline_total": int(baseline_total),
        "baseline_order_cnt": int(baseline_order_cnt),
        "baseline_rows": int(baseline_rows),
        "pos_total": int(pos_total),
        "pos_order_cnt": int(pos_order_cnt),
        "pos_rows": int(pos_rows),
        "toorder_total": int(toorder_total),
        "toorder_order_cnt": int(toorder_order_cnt),
        "toorder_rows": int(toorder_rows),
    }


def detect_manual_partial_collection(
    date: str,
    store: str,
    platforms: set[str],
    manual_source: str,
    manual_total: int,
) -> dict | None:
    """수동수집 합계가 기준 배달 합계 대비 과소하면 이벤트를 반환한다.

    ToOrder 기준이 있으면 우선 사용하고, 없을 때만 POS 계열 합계로 fallback한다.
    """
    baseline = delivery_baseline_summary(date, store, platforms, manual_source)
    baseline_label = baseline["baseline_label"]
    baseline_total = baseline["baseline_total"]
    baseline_order_cnt = baseline["baseline_order_cnt"]
    baseline_rows = baseline["baseline_rows"]

    if baseline_rows <= 0 or baseline_total <= 0:
        return None

    manual_total = int(manual_total or 0)
    gap = baseline_total - manual_total
    if manual_total >= MANUAL_PARTIAL_MIN_RATIO * baseline_total:
        return None
    if gap < MANUAL_PARTIAL_MIN_GAP:
        return None

    return {
        "date": date,
        "store": store,
        "platform": sorted(platforms)[0] if platforms else "",
        "manual_total": manual_total,
        "pos_total": int(baseline["pos_total"]),
        "pos_order_cnt": int(baseline["pos_order_cnt"]),
        "pos_rows": int(baseline["pos_rows"]),
        "toorder_total": int(baseline["toorder_total"]),
        "toorder_order_cnt": int(baseline["toorder_order_cnt"]),
        "toorder_rows": int(baseline["toorder_rows"]),
        "baseline_label": baseline_label,
        "baseline_total": int(baseline_total),
        "baseline_order_cnt": int(baseline_order_cnt),
        "baseline_rows": int(baseline_rows),
        "gap": int(gap),
        "ratio": round(manual_total / baseline_total, 3) if baseline_total else 0.0,
        "order_cnt": int(baseline_order_cnt),
    }


def _format_other_baseline(event: dict) -> str:
    """선택되지 않은 기준 금액이 다르면 알림에 보조 기준으로 표시한다."""
    baseline_label = str(event.get("baseline_label") or "").strip()
    pos_total = int(event.get("pos_total") or 0)
    pos_order_cnt = int(event.get("pos_order_cnt") or 0)
    toorder_total = int(event.get("toorder_total") or 0)
    toorder_order_cnt = int(event.get("toorder_order_cnt") or 0)

    if pos_total <= 0 or toorder_total <= 0 or pos_total == toorder_total:
        return ""
    if baseline_label == "POS":
        return f" (ToOrder {toorder_total:,}/{toorder_order_cnt}건)"
    if baseline_label == "ToOrder":
        return f" (POS {pos_total:,}/{pos_order_cnt}건)"
    return ""


def _record_marker(root, source: str, store: str, date: str, meta: dict) -> bool:
    """마커를 기록하고 신규 생성 여부를 반환한다."""
    try:
        path = root / str(source).strip() / str(store).strip() / f"{date}.json"
        is_new = not path.exists()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(meta or {})
        payload.update(
            {
                "source": str(source).strip(),
                "store": str(store).strip(),
                "date": str(date).strip(),
                "updated_at": pendulum.now("Asia/Seoul").isoformat(),
            }
        )
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return is_new
    except Exception as exc:
        logger.warning(
            "마커 기록 실패: root=%s source=%s store=%s date=%s | %s",
            root,
            source,
            store,
            date,
            exc,
        )
        return False


def _clear_marker(root, source: str, store: str, date: str) -> bool:
    try:
        path = root / str(source).strip() / str(store).strip() / f"{date}.json"
        if not path.exists():
            return False
        path.unlink()
        return True
    except Exception as exc:
        logger.warning(
            "마커 삭제 실패: root=%s source=%s store=%s date=%s | %s",
            root,
            source,
            store,
            date,
            exc,
        )
        return False


def record_manual_fallback_marker(
    source: str,
    store: str,
    date: str,
    meta: dict,
) -> bool:
    """폴백 마커를 기록하고 신규 생성 여부를 반환한다."""
    return _record_marker(MANUAL_FALLBACK_MARKER_ROOT, source, store, date, meta)


def clear_manual_fallback_marker(source: str, store: str, date: str) -> bool:
    """수동 복원 시 폴백 마커를 삭제한다."""
    return _clear_marker(MANUAL_FALLBACK_MARKER_ROOT, source, store, date)


def record_manual_partial_marker(
    source: str,
    store: str,
    date: str,
    meta: dict,
) -> bool:
    return _record_marker(MANUAL_PARTIAL_MARKER_ROOT, source, store, date, meta)


def clear_manual_partial_marker(source: str, store: str, date: str) -> bool:
    return _clear_marker(MANUAL_PARTIAL_MARKER_ROOT, source, store, date)


def record_manual_reingest_marker(
    source: str,
    store: str,
    date: str,
    meta: dict,
) -> bool:
    """재수집으로 원천이 교체된 매장·날짜를 기록한다."""
    return _record_marker(MANUAL_REINGEST_MARKER_ROOT, source, store, date, meta)


def clear_manual_reingest_marker(source: str, store: str, date: str) -> bool:
    return _clear_marker(MANUAL_REINGEST_MARKER_ROOT, source, store, date)


def list_manual_reingest_dates(source: str, stores: list[str]) -> set[str]:
    """대상 매장들의 재수집 마커 날짜 합집합을 반환한다."""
    dates: set[str] = set()
    try:
        source_root = MANUAL_REINGEST_MARKER_ROOT / str(source).strip()
        if not source_root.exists():
            return dates
        for store in stores:
            store_name = str(store).strip()
            if not store_name:
                continue
            for path in (source_root / store_name).glob("*.json"):
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", path.stem):
                    dates.add(path.stem)
    except Exception as exc:
        logger.warning(
            "재수집 마커 조회 실패: source=%s stores=%s | %s",
            source,
            stores,
            exc,
        )
        return set()
    return dates


def filter_manual_reingest_dates_outside_recent_window(
    dates,
    *,
    recent_days: int,
    now=None,
) -> list[str]:
    """최근 보호구간 밖의 재수집 마커 날짜만 반환한다.

    recent_days=9이면 KST 기준 D-0~D-8은 제외하고 D-9 이전만 허용한다.
    """
    if recent_days < 0:
        raise ValueError("recent_days는 0 이상이어야 합니다.")
    ref = now or pendulum.now("Asia/Seoul")
    cutoff = pendulum.instance(ref).in_timezone("Asia/Seoul").subtract(days=recent_days)
    cutoff_str = cutoff.format("YYYY-MM-DD")

    normalized_dates = []
    for date in dates:
        try:
            normalized = pendulum.parse(str(date), strict=False).format("YYYY-MM-DD")
        except Exception:
            logger.warning("재수집 마커 날짜 형식 오류: date=%s", date)
            continue
        if normalized <= cutoff_str:
            normalized_dates.append(normalized)
    return sorted(set(normalized_dates))


def notify_manual_fallback(source_label: str, events: list[dict]) -> None:
    """수동 결측 기준 매출 신규 이벤트를 Telegram으로 1회 알린다."""
    if not events:
        return
    try:
        from modules.transform.utility.notifier import send_telegram

        lines = [f"[도리당] 배달 수동 결측→기준 대체({source_label})"]
        for event in events:
            baseline_label = str(event.get("baseline_label") or "POS").strip()
            amount = int(
                event.get("baseline_total")
                if event.get("baseline_total") is not None
                else event.get("total_price") or 0
            )
            order_cnt = int(
                event.get("baseline_order_cnt")
                if event.get("baseline_order_cnt") is not None
                else event.get("order_cnt") or 0
            )
            platform = str(event.get("platform") or "").strip()
            other_baseline = _format_other_baseline(event)
            lines.append(
                f"- {event.get('store')} {event.get('date')} {platform} "
                f"{baseline_label} {amount:,}/{order_cnt}건{other_baseline}"
            )
        lines.append("재수집 요망")
        send_telegram("\n".join(lines))
    except Exception as exc:
        logger.warning("수동 폴백 알림 실패: source_label=%s | %s", source_label, exc)


def notify_manual_partial(source_label: str, events: list[dict]) -> None:
    """수동 부분수집 의심 신규 이벤트를 Telegram으로 1회 알린다."""
    if not events:
        return
    try:
        from modules.transform.utility.notifier import send_telegram

        lines = [f"[도리당] 배달 수동 부분수집 의심({source_label})"]
        for event in events:
            baseline_label = str(event.get("baseline_label") or "POS").strip()
            baseline_total = int(
                event.get("baseline_total")
                if event.get("baseline_total") is not None
                else event.get("pos_total") or 0
            )
            other_baseline = _format_other_baseline(event)
            lines.append(
                f"- {event.get('store')} {event.get('date')} {event.get('platform')} "
                f"수동 {int(event.get('manual_total') or 0):,} / "
                f"{baseline_label} {baseline_total:,}{other_baseline} "
                f"(부족 {int(event.get('gap') or 0):,}, "
                f"{int(float(event.get('ratio') or 0) * 100)}%)"
            )
        lines.append("재수집 요망")
        send_telegram("\n".join(lines))
    except Exception as exc:
        logger.warning("수동 부분수집 알림 실패: source_label=%s | %s", source_label, exc)


def notify_manual_missing_all(source_label: str, events: list[dict]) -> None:
    """수동·기준 모두 없어 매출이 0으로 남은 신규 일자를 Telegram으로 알린다."""
    if not events:
        return
    try:
        from modules.transform.utility.notifier import send_telegram

        lines = [f"[도리당] 배달 수동·기준 모두 없음({source_label})"]
        for event in events:
            lines.append(
                f"- {event.get('store')} {event.get('date')} "
                f"{event.get('platform')} 매출 0"
            )
        lines.append("재수집 요망")
        send_telegram("\n".join(lines))
    except Exception as exc:
        logger.warning("수동 무데이터 알림 실패: source_label=%s | %s", source_label, exc)


def record_manual_item_detail_gap_marker(
    source: str,
    store: str,
    date: str,
    meta: dict,
) -> bool:
    """메뉴 상세 결손 마커를 기록하고 신규 생성 여부를 반환한다."""
    return _record_marker(MANUAL_ITEM_DETAIL_GAP_MARKER_ROOT, source, store, date, meta)


def notify_manual_item_detail_gap(label: str, event: dict) -> None:
    """수동수집 메뉴 상세 결손을 소스·매장·날짜별 최초 한 번 알린다."""
    try:
        from modules.transform.utility.notifier import send_telegram

        order_ids = [str(v).strip() for v in event.get("order_ids", []) if str(v).strip()]
        order_text = ",".join(order_ids) if order_ids else "-"
        omitted = int(event.get("omitted_order_id_count") or 0)
        if omitted:
            order_text = f"{order_text} 외 {omitted}건"
        send_telegram(
            "\n".join(
                [
                    f"[도리당] {label} 메뉴 상세 부분수집",
                    f"- 매장: {event.get('store')}",
                    f"- 날짜: {event.get('date')}",
                    f"- 결손: {int(event.get('missing_count') or 0)}건",
                    f"- 주문번호: {order_text}",
                    "매출은 메뉴미상 플레이스홀더로 보존했습니다.",
                ]
            )
        )
    except Exception as exc:
        logger.warning("수동 메뉴 상세 결손 알림 실패: label=%s | %s", label, exc)


def fill_missing_manual_item_name(
    item_name: pd.Series,
    *,
    source: str,
    label: str,
    store: str,
    sale_date: pd.Series | str | None = None,
    order_id: pd.Series | None = None,
) -> pd.Series:
    """부분수집으로 빈 수동배달 메뉴명을 채우고 결손 사실을 알린다."""
    filled = item_name.fillna("").astype(str).str.strip()
    missing = filled.eq("") | filled.str.lower().eq("nan")
    if not missing.any():
        return filled

    placeholder = MANUAL_UNKNOWN_ITEM_NAME_FMT.format(label=label)
    filled = filled.mask(missing, placeholder)

    if isinstance(sale_date, pd.Series):
        dates = sale_date.reindex(filled.index).fillna("").astype(str).str.strip()
    else:
        dates = pd.Series(str(sale_date or "").strip(), index=filled.index, dtype="object")
    if order_id is not None:
        order_ids = order_id.reindex(filled.index).fillna("").astype(str).str.strip()
    else:
        order_ids = pd.Series("", index=filled.index, dtype="object")

    missing_dates = sorted({v for v in dates[missing].tolist() if v})
    all_order_ids = sorted({v for v in order_ids[missing].tolist() if v})
    log_order_ids = all_order_ids[:20]
    remaining = max(0, len(all_order_ids) - len(log_order_ids))
    order_label = ",".join(log_order_ids) if log_order_ids else "-"
    if remaining:
        order_label = f"{order_label} 외 {remaining}건"

    logger.warning(
        "%s 메뉴 상세 결손 행 플레이스홀더 처리: source=%s store=%s date=%s | %d건 order_id=%s",
        label,
        source,
        store,
        ",".join(missing_dates) if missing_dates else "-",
        int(missing.sum()),
        order_label,
    )

    for date in missing_dates:
        date_mask = missing & dates.eq(date)
        date_order_ids = sorted({v for v in order_ids[date_mask].tolist() if v})
        shown_order_ids = date_order_ids[:20]
        event = {
            "store": str(store).strip(),
            "date": date,
            "missing_count": int(date_mask.sum()),
            "order_ids": shown_order_ids,
            "omitted_order_id_count": max(0, len(date_order_ids) - len(shown_order_ids)),
            "placeholder": placeholder,
        }
        if record_manual_item_detail_gap_marker(source, store, date, event):
            notify_manual_item_detail_gap(label, event)

    return filled

# 테스트매장
_BASE_DELIVERY_MANUAL_TEST_STORES = [
    "해운대중동점", #08-12
    "법흥리점",# 08-12
    "동탄영천점", # 08-12
    "중랑면목점",# 08-12
    "시흥배곧점", # 08-12 쿠팡 확인해봐야함
    "강원영월점", # 08-12
    "평택비전점", # 08-12
    "부산장림점", # 08-12 쿠팡 확인해야봐야함
    "경북상주점", # 08-12
    "창원내서점", # 08-12
    "행신점", # 08-12
    "전주전북대점", # 08-12 배민 06-24확인
    "구로디지털점", # 08-12
    "부천옥길점", # 08-12
    "송파삼전점", # 08-12 쿠팡 06-02 0614확인

]

# 사용법:
# 1. 새 테스트매장을 임시로 적용할 때
#    - ADD_TEST_STORES에 매장명을 넣는다.
#    - 예: ADD_TEST_STORES = ["창원내서점"]
#    - DAG의 partial_store_mode에서 stores를 생략하면 이 목록까지 자동 포함된다.
# 2. 수집 오류 등으로 테스트매장 정책에서 잠시 빼야 할 때
#    - EXCLUDE_TEST_STORES에 매장명을 넣는다.
#    - 예: EXCLUDE_TEST_STORES = ["창원내서점"]
#    - 기본 목록이나 ADD_TEST_STORES에 있어도 제외 목록에 있으면 최종 대상에서 빠진다.
# 3. 테스트가 끝나고 정식 운영 매장으로 확정할 때
#    - ADD_TEST_STORES의 매장을 _BASE_DELIVERY_MANUAL_TEST_STORES로 옮긴다.
#    - 옮긴 뒤 ADD_TEST_STORES에서는 삭제한다.
# 4. 최종 적용 대상
#    - DELIVERY_MANUAL_TEST_STORES = 기본 목록 + 임시 추가 목록 - 임시 제외 목록.
#    - 운영 로직과 DAG는 이 최종 목록만 참조한다.

# 임시추가
ADD_TEST_STORES = [ "삼송점", # 쿠팡거절
                   "청라점",  # 쿠팡거절
                   "대전장대점", # 08-13
                   "대전둔산점", 
                   "기흥테라타워점", "천안성정점" , "교대점", "서울대입구역점", "광명철산점", "부산서면점", # 참여매장
                   "미사점", "양주옥정점", "수유점"

]

# 기본 실행 범위는 유지하고, 지정 매장만 전체기간 추가 재계산하는 임시 운영 목록.
# 복구 완료 후 비운다.
FULL_RECALC_STORES = [
]

# 제외시 사용
EXCLUDE_TEST_STORES = [
]

_EXCLUDED_TEST_STORE_SET = {str(store).strip() for store in EXCLUDE_TEST_STORES if str(store).strip()}

DELIVERY_MANUAL_TEST_STORES = [
    store
    for store in dict.fromkeys(_BASE_DELIVERY_MANUAL_TEST_STORES + ADD_TEST_STORES)
    if str(store).strip() and str(store).strip() not in _EXCLUDED_TEST_STORE_SET
]

# POS 원천이 없어 나머지 채널을 toorder로 보충하는 매장.
TOORDER_MANUAL_STORES = ["해운대중1점"]

DELIVERY_PLATFORM_FAMILIES = {
    "배민수동": {"배달의민족", "배민1", "배민 포장", "배민 사장"},
    "쿠팡수동": {"쿠팡이츠", "쿠팡 포장"},
}

PLATFORM_TO_MANUAL_SOURCE = {
    platform: source
    for source, platforms in DELIVERY_PLATFORM_FAMILIES.items()
    for platform in platforms
}

UNIFIED_COLUMNS = [
    "sale_date",
    "ym",
    "source",
    "brand",
    "store",
    "region",
    "담당자",
    "실오픈일",
    "platform",
    "order_type",
    "order_id",
    "order_time",
    "menu_name",
    "item_seq",
    "item_id",
    "item_name",
    "qty",
    "unit_price",
    "total_price",
    "discount_amount",
    "sale_type",
    "_pk",
    "collected_at",
    "order_cnt",
]


# 모듈 레벨 캐시 (DAG 실행 단위로 재사용)
_STORE_MAP_CACHE: dict | None = None
_STORE_MAP_CACHE_MTIME: float | None = None

_FIN_PRODUCT_CACHE: pd.DataFrame | None = None
_FIN_PRODUCT_CACHE_MTIME: float | None = None

_POSFEED_WHITELIST_CACHE: dict[str, set[str] | None] | None = None
_POSFEED_WHITELIST_CACHE_MTIME: float | None = None


def _load_fin_product_latest() -> pd.DataFrame:
    """fin_product_grp_input.csv 로드 후 source+brand+store+상품코드별 최신 1행으로 정규화.

    - updated_at 컬럼이 있으면(updated_at 파싱 가능한 경우) 최신 기준으로 dedupe
    - 없으면 파일 내 마지막 행(last)을 최신으로 가정
    """
    global _FIN_PRODUCT_CACHE, _FIN_PRODUCT_CACHE_MTIME
    try:
        source_path = existing_fin_product_csv_path()
        mtime = source_path.stat().st_mtime
    except FileNotFoundError:
        _FIN_PRODUCT_CACHE = pd.DataFrame(columns=["상품코드", "상품명", "updated_at"])
        _FIN_PRODUCT_CACHE_MTIME = None
        return _FIN_PRODUCT_CACHE

    if _FIN_PRODUCT_CACHE is not None and _FIN_PRODUCT_CACHE_MTIME == mtime:
        return _FIN_PRODUCT_CACHE

    try:
        df = pd.read_csv(source_path, dtype=str).fillna("")
    except Exception:
        df = pd.DataFrame(columns=["상품코드", "상품명", "updated_at"])

    for c in ("source", "brand", "store", "상품코드", "상품명"):
        if c not in df.columns:
            df[c] = ""
    # optional canonical name columns
    for c in ("표준_메뉴명", "상품명_표준", "메뉴명"):
        if c not in df.columns:
            df[c] = ""
    if "updated_at" not in df.columns:
        df["updated_at"] = ""

    df["source"] = df["source"].fillna("").astype(str).str.strip().map(canonical_source)
    df["brand"] = df["brand"].fillna("").astype(str).str.strip()
    df["store"] = df["store"].fillna("").astype(str).str.strip()
    df["상품코드"] = df["상품코드"].fillna("").astype(str).str.strip()
    df["상품명"] = df["상품명"].fillna("").astype(str).str.strip()
    df["표준_메뉴명"] = df["표준_메뉴명"].fillna("").astype(str).str.strip()
    df["상품명_표준"] = df["상품명_표준"].fillna("").astype(str).str.strip()
    df["메뉴명"] = df["메뉴명"].fillna("").astype(str).str.strip()
    df["updated_at"] = df["updated_at"].fillna("").astype(str).str.strip()

    df = df[df["상품코드"] != ""].copy()
    if df.empty:
        _FIN_PRODUCT_CACHE = df
        _FIN_PRODUCT_CACHE_MTIME = mtime
        return _FIN_PRODUCT_CACHE

    key_cols = ["source", "brand", "store", "상품코드"]
    if df["updated_at"].astype(str).str.strip().ne("").any():
        ts = pd.to_datetime(df["updated_at"], errors="coerce")
        df["_updated_at_ts"] = ts.fillna(pd.Timestamp.min)
        df = df.sort_values(key_cols + ["_updated_at_ts"], na_position="last").groupby(key_cols, as_index=False).last()
        df = df.drop(columns=["_updated_at_ts"], errors="ignore")
    else:
        df = df.sort_values(key_cols).groupby(key_cols, as_index=False).last()

    _FIN_PRODUCT_CACHE = df
    _FIN_PRODUCT_CACHE_MTIME = mtime
    return _FIN_PRODUCT_CACHE


def _fin_code_to_name_map() -> dict[tuple[str, str, str, str], str]:
    df = _load_fin_product_latest()
    if df.empty or "상품코드" not in df.columns or "상품명" not in df.columns:
        return {}
    # Prefer canonical name if present: 표준_메뉴명 -> 상품명_표준 -> 메뉴명 -> 상품명
    canon0 = df["표준_메뉴명"].fillna("").astype(str).str.strip()
    canon = df["상품명_표준"].fillna("").astype(str).str.strip()
    menu = df["메뉴명"].fillna("").astype(str).str.strip() if "메뉴명" in df.columns else pd.Series([""] * len(df))
    raw = df["상품명"].fillna("").astype(str).str.strip()
    name_s = canon0.where(canon0 != "", canon.where(canon != "", menu.where(menu != "", raw)))
    keys = list(zip(
        df["source"].fillna("").astype(str).str.strip().map(canonical_source),
        df["brand"].fillna("").astype(str).str.strip(),
        df["store"].fillna("").astype(str).str.strip(),
        df["상품코드"].fillna("").astype(str).str.strip(),
    ))
    return dict(zip(keys, name_s.fillna("").astype(str)))


def _apply_fin_item_name(df: pd.DataFrame) -> pd.DataFrame:
    """unified_sales df의 scoped item_id 기준으로 item_name을 fin_product_grp 최신 상품명으로 정합."""
    if df.empty or "item_id" not in df.columns:
        return df
    code_to_name = _fin_code_to_name_map()
    if not code_to_name:
        return df
    for col in ("source", "brand", "store"):
        if col not in df.columns:
            df[col] = ""
    keys = list(zip(
        df["source"].fillna("").astype(str).str.strip().map(canonical_source),
        df["brand"].fillna("").astype(str).str.strip(),
        df["store"].fillna("").astype(str).str.strip(),
        df["item_id"].fillna("").astype(str).str.strip(),
    ))
    mapped = pd.Series([code_to_name.get(key, "") for key in keys], index=df.index)
    mask = mapped.fillna("").astype(str).str.strip() != ""
    if "item_name" not in df.columns:
        df["item_name"] = ""
    df.loc[mask, "item_name"] = mapped.loc[mask].astype(str)
    return df


def _normalize_item_key(name: str) -> str:
    """item_name 비교용 정규화 키 생성 (저장값이 아닌 비교 전용).

    1. [...] 기호만 제거 (내용 보존)
    2. (...) 기호만 제거 (내용 보존)
    3. 한글·영문·숫자 이외 문자(공백 포함) 제거
    """
    s = re.sub(r'[\[\]]', '', name)
    s = re.sub(r'[()]', '', s)
    s = re.sub(r'[^가-힣a-zA-Z0-9一-鿿]', '', s)
    return s


def _merge_blacklist_scope(
    result: dict[str, set[str] | None],
    key: str,
    stores: set[str] | None,
) -> None:
    if not key:
        return
    if key not in result:
        result[key] = stores
        return
    if result[key] is None or stores is None:
        result[key] = None
        return
    result[key] = set(result[key] or set()) | stores


def _load_posfeed_blacklist() -> dict[str, set[str] | None]:
    """posfeed 제외 상품을 반환.

    반환값: {normalize(item_name): None | set[str]}
    - None → 전체 매장에서 제외
    - set[str] → 해당 매장에서만 제외 (store 컬럼의 쉼표 구분 값)

    기본 소스는 fin_product_grp_input.csv의 source=posfeed & exclude_check=Y이다.
    구 whitelist에는 옵션/배달비까지 N으로 남아 있어 기본 blacklist로 쓰지 않는다.
    긴급 진단 시에만 POSFEED_USE_LEGACY_BLACKLIST=1로 합산한다.
    """
    global _POSFEED_WHITELIST_CACHE, _POSFEED_WHITELIST_CACHE_MTIME
    try:
        source_path = existing_fin_product_csv_path()
        grp_mtime = source_path.stat().st_mtime
    except FileNotFoundError:
        source_path = FIN_PRODUCT_CSV_PATH
        grp_mtime = None
    try:
        legacy_mtime = POSFEED_WHITELIST_CSV_PATH.stat().st_mtime
    except FileNotFoundError:
        legacy_mtime = None

    mtime = (grp_mtime, legacy_mtime)

    if _POSFEED_WHITELIST_CACHE is not None and _POSFEED_WHITELIST_CACHE_MTIME == mtime:
        return _POSFEED_WHITELIST_CACHE

    result: dict[str, set[str] | None] = {}

    try:
        df = pd.read_csv(source_path, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception as e:
        logger.warning("posfeed grp 블랙리스트 로드 실패: %s", e)
        df = pd.DataFrame()

    if not df.empty:
        if {"source", "상품명", "exclude_check"}.issubset(df.columns):
            source_mask = df["source"].fillna("").astype(str).str.strip().str.lower() == "posfeed"
            exclude_mask = df["exclude_check"].fillna("").astype(str).str.strip().str.upper() == "Y"
            for _, row in df[source_mask & exclude_mask].iterrows():
                key = _normalize_item_key(str(row["상품명"]).strip())
                _merge_blacklist_scope(result, key, None)
        else:
            logger.warning("posfeed grp 블랙리스트 컬럼 오류 (필요: source, 상품명, exclude_check)")

    if os.getenv("POSFEED_USE_LEGACY_BLACKLIST", "").strip().lower() in {"1", "true", "y", "yes"}:
        try:
            legacy = pd.read_csv(POSFEED_WHITELIST_CSV_PATH, dtype=str, encoding="utf-8-sig").fillna("")
        except FileNotFoundError:
            legacy = pd.DataFrame()
        except Exception as e:
            logger.warning("posfeed legacy whitelist 로드 실패: %s", e)
            legacy = pd.DataFrame()

        if not legacy.empty:
            if {"item_name", "is_valid"}.issubset(legacy.columns):
                invalid = legacy["is_valid"].fillna("").astype(str).str.strip().str.upper() == "N"
                for _, row in legacy[invalid].iterrows():
                    key = _normalize_item_key(str(row["item_name"]).strip())
                    store_val = str(row.get("store", "")).strip()
                    if store_val:
                        stores = {v.strip() for v in store_val.split(",") if v.strip()}
                        _merge_blacklist_scope(result, key, stores or None)
                    else:
                        _merge_blacklist_scope(result, key, None)
            else:
                logger.warning("posfeed legacy whitelist 컬럼 오류 (필요: item_name, is_valid)")

    logger.info("posfeed 블랙리스트 로드: %d 항목 (grp+legacy 정규화 키)", len(result))
    _POSFEED_WHITELIST_CACHE = result
    _POSFEED_WHITELIST_CACHE_MTIME = mtime
    return _POSFEED_WHITELIST_CACHE


def _apply_posfeed_blacklist(df: pd.DataFrame) -> pd.DataFrame:
    """posfeed df에서 grp 블랙리스트(exclude_check=Y) item_name 행 제거.

    - grp posfeed 제외 행은 전체 매장 제외로 적용
    - grp에 없는 item_name은 자동 통과
    """
    blacklist = _load_posfeed_blacklist()
    if not blacklist or df.empty:
        return df

    items = df["item_name"].fillna("").astype(str).str.strip()
    stores_col = df["store"].fillna("").astype(str).str.strip() if "store" in df.columns else pd.Series("", index=df.index)
    item_keys = items.map(_normalize_item_key)

    def _is_bl(key: str, store: str) -> bool:
        if key not in blacklist:
            return False
        r = blacklist[key]
        return r is None or store in r

    remove_mask = pd.Series(
        [_is_bl(k, s) for k, s in zip(item_keys, stores_col)],
        index=df.index,
        dtype=bool,
    )

    if not remove_mask.any():
        return df

    out = df.copy()
    if "order_id" in out.columns and "total_price" in out.columns:
        order_keys = out["order_id"].fillna("").astype(str).str.strip()
        amounts = pd.to_numeric(out["total_price"], errors="coerce").fillna(0)
        # 주문별로 제외 금액을 잔존행에 합산한다.
        # 잔존행이 있으면 첫 행에 합산, 주문 전체가 블랙리스트면 대표행(최고 매출) 1개를
        # 살려서 주문 매출을 보존한다(전량 제외 시 매출 누락 방지).
        for order_key, grp_idx in amounts.groupby(order_keys).groups.items():
            if not order_key:
                continue
            removed_idx = [i for i in grp_idx if remove_mask.at[i]]
            if not removed_idx:
                continue
            removed_sum = amounts.loc[removed_idx].sum()
            if removed_sum == 0:
                continue
            kept_idx = [i for i in grp_idx if not remove_mask.at[i]]
            if kept_idx:
                target_idx = kept_idx[0]
                out.at[target_idx, "total_price"] = int(round(amounts.at[target_idx] + removed_sum))
            else:
                rep_idx = amounts.loc[removed_idx].abs().idxmax()
                out.at[rep_idx, "total_price"] = int(round(removed_sum))
                remove_mask.at[rep_idx] = False

    logger.warning(
        "posfeed 블랙리스트 적용: %d행 제거 | 항목: %s",
        int(remove_mask.sum()),
        out.loc[remove_mask, "item_name"].unique().tolist()[:10],
    )
    return out[~remove_mask].copy()


# ============================================================
# 공통 내부 유틸
# ============================================================

def _load_store_map() -> dict[str, dict]:
    """sales_employee.csv → {지점명: {담당자, region, 실오픈일}} 맵 (캐시)."""
    global _STORE_MAP_CACHE, _STORE_MAP_CACHE_MTIME
    csv_path = ONEDRIVE_DB / "sales_employee.csv"
    try:
        mtime = csv_path.stat().st_mtime
    except FileNotFoundError:
        mtime = None

    # Airflow worker가 장시간 살아있으면 모듈 캐시가 다음 DAG 실행에도 남을 수 있어,
    # 파일 수정시간(mtime)이 동일할 때만 캐시를 재사용한다.
    if _STORE_MAP_CACHE is not None and _STORE_MAP_CACHE_MTIME is not None and mtime is not None:
        if _STORE_MAP_CACHE_MTIME == mtime:
            return _STORE_MAP_CACHE

    try:
        try:
            df = pd.read_csv(csv_path, dtype=str, usecols=["매장명", "담당자", "상세주소", "실오픈일"])
        except ValueError:
            df = pd.read_csv(csv_path, dtype=str, usecols=["매장명", "담당자", "상세주소"])
            df["실오픈일"] = ""
    except FileNotFoundError:
        logger.warning("sales_employee.csv 없음: %s", csv_path)
        _STORE_MAP_CACHE = {}
        _STORE_MAP_CACHE_MTIME = None
        return _STORE_MAP_CACHE

    df["_key"] = df["매장명"].str.strip().str.split().str[-1]
    df = df.drop_duplicates(subset=["_key"], keep="first")
    _STORE_MAP_CACHE = {
        row["_key"]: {
            "담당자": str(row.get("담당자", "")).strip(),
            "region": str(row.get("상세주소", "")).strip()[:2],
            "실오픈일": str(row.get("실오픈일", "")).strip(),
        }
        for _, row in df.iterrows()
    }
    _STORE_MAP_CACHE_MTIME = mtime
    return _STORE_MAP_CACHE


def _lookup_store_meta(store_map: dict[str, dict], key, field: str) -> str:
    if pd.isna(key):
        return ""
    return str(store_map.get(str(key), {}).get(field, "")).strip()


def _unified_daily_path(date_str: str):
    ymd = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")
    return UNIFIED_ROOT / f"unified_sales_{ymd}.parquet"


def _make_unified_pk(df: pd.DataFrame) -> pd.Series:
    """unified_sales 전용 PK 생성 (소스/스키마 공통).

    PK 구성:
    - sale_date | source | store | platform | order_id | item_seq

    채널별로 일부 값이 비어있을 수 있으나(예: toorder는 order_id/item_seq 없음),
    동일 채널 내에서 행 그레인이 유지되는 한 안정적으로 동작한다.
    """
    for col in ("sale_date", "source", "store", "platform", "order_id", "item_seq"):
        if col not in df.columns:
            df[col] = ""
    key = (
        df["sale_date"].fillna("").astype(str).str.strip()
        + "|" + df["source"].fillna("").astype(str).str.strip()
        + "|" + df["store"].fillna("").astype(str).str.strip()
        + "|" + df["platform"].fillna("").astype(str).str.strip()
        + "|" + df["order_id"].fillna("").astype(str).str.strip()
        + "|" + df["item_seq"].fillna("").astype(str).str.strip()
    )
    return key.map(lambda s: hashlib.md5(s.encode()).hexdigest())


def _save_unified_daily(
    df: pd.DataFrame,
    date_str: str,
    overwrite: bool = False,
    replace_stores: list[str] | None = None,
) -> int:
    """일별 unified_sales 저장.

    overwrite=False(기본): 동일 source 행을 교체(source-aware replace). 다른 source 행은 유지.
    overwrite=True: 기존 파일 전체 교체 (정정용).
    replace_stores: 지정된 매장의 동일 source 행만 교체. 금일 부분 재적재용.
    """
    UNIFIED_ROOT.mkdir(parents=True, exist_ok=True)
    daily_path = _unified_daily_path(date_str)

    # Backward compatibility: older parquet / upstream may still have "구분".
    if "sale_type" not in df.columns and "구분" in df.columns:
        df = df.copy()
        df["sale_type"] = df["구분"]

    if "order_cnt" not in df.columns:
        df = df.copy()
        df["order_cnt"] = 0

    # Normalize source so source-aware replace is stable across re-runs.
    # (Avoids accidental duplication when historical rows had different casing/whitespace.)
    if "source" in df.columns:
        df = df.copy()
        df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()

    df = df.reindex(columns=UNIFIED_COLUMNS, fill_value="")
    df = filter_manual_delivery_sources_for_test_stores(df)

    if daily_path.exists():
        existing = pd.read_parquet(daily_path)
        existing = existing.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        if "source" in existing.columns:
            existing = existing.copy()
            existing["source"] = existing["source"].fillna("").astype(str).str.strip().str.lower()
        sources = df["source"].dropna().unique().tolist()
        store_scope = {
            str(store).strip()
            for store in (replace_stores or [])
            if str(store).strip()
        }
        if sources and store_scope:
            existing_store = existing["store"].fillna("").astype(str).str.strip()
            replace_mask = existing["source"].isin(sources) & existing_store.isin(store_scope)
            existing_same_src = existing[replace_mask]
            existing_other_src = existing[~replace_mask]
        else:
            existing_same_src = existing[existing["source"].isin(sources)] if sources else existing.iloc[:0]
            existing_other_src = existing[~existing["source"].isin(sources)] if sources else existing
        # no-op 체크: overwrite=False일 때만 적용 (overwrite=True면 강제 재기록)
        if not overwrite and (
            set(existing_same_src["_pk"]) == set(df["_pk"])
            and not existing_same_src["_pk"].duplicated().any()
            and len(existing_same_src) == len(df)
        ):
            logger.info("변경 없음, 스킵: %s", daily_path)
            return 0
        merged = pd.concat([existing_other_src, df], ignore_index=True)
        # overwrite=True 로 기존 same-source rows 를 교체할 때 기존 행 수가 더 많으면
        # 단순 차분은 음수가 될 수 있다. 반환값은 "이번 저장으로 반영된 행 수"로만
        # 사용하므로 음수는 0으로 클램프한다.
        new_count = max(0, len(df) - len(existing_same_src))
    else:
        merged = df.copy()
        new_count = len(merged)

    merged = filter_manual_delivery_sources_for_test_stores(merged)

    # Final safety: keep latest row per PK (idempotency across re-runs / partial loads).
    if "_pk" in merged.columns:
        before = len(merged)
        merged = merged.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)
        dropped = before - len(merged)
        if dropped:
            logger.warning("unified_sales %s: _pk 중복 %d행 제거(방어)", date_str, dropped)

    merged["qty"] = pd.to_numeric(merged["qty"], errors="coerce").fillna(0).astype(int)
    merged["unit_price"] = pd.to_numeric(merged["unit_price"], errors="coerce").fillna(0).astype(int)
    if "total_price" in merged.columns:
        merged["total_price"] = pd.to_numeric(merged["total_price"], errors="coerce").fillna(0).astype(int)
    if "discount_amount" in merged.columns:
        merged["discount_amount"] = pd.to_numeric(merged["discount_amount"], errors="coerce").fillna(0).astype(int)
    save_unified_parquet(merged, daily_path)
    logger.info("저장(일별): %s | 전체 %d행 (신규 %d행)", daily_path, len(merged), new_count)
    return new_count


# ============================================================
# 공통 시각 정규화
# ============================================================

def _normalize_time(raw) -> str:
    s = str(raw).strip()
    if not s or s == "nan":
        return ""

    # Prefer explicit HH:MM:SS anywhere in the string (e.g. "2026-04-01 12:05:00")
    m = re.findall(r"\b(\d{2}:\d{2}:\d{2})\b", s)
    if m:
        return m[-1]

    # Fallback: HH:MM -> HH:MM:00 (e.g. "2026-04-01 12:05")
    m2 = re.findall(r"\b(\d{2}:\d{2})\b", s)
    if m2:
        return f"{m2[-1]}:00"

    # Last resort: keep as-is
    return s


def _strip_and_coalesce_columns(df: pd.DataFrame) -> pd.DataFrame:
    """CSV/HTML 소스에서 컬럼명 앞뒤 공백이 섞이는 케이스 방어 + 중복 컬럼 병합.

    월 단위로 여러 매장을 concat 할 때, 매장별로 컬럼명이 미세하게 다르면(공백 포함)
    동일한 의미의 컬럼이 2개로 늘어나며 한쪽이 전부 NaN이 되는 케이스가 생긴다.

    예) 어떤 매장은 '실매출액', 다른 매장은 ' 실매출액 ' → concat 후 2개 컬럼 공존
        → strip 후 둘 다 '실매출액'이 되므로, "첫 non-null" 기준으로 한 컬럼으로 병합한다.
    """
    if df is None or df.empty:
        return df

    bases: dict[str, list[str]] = {}
    order: list[str] = []
    for c in df.columns:
        base = str(c).strip()
        if base not in bases:
            bases[base] = []
            order.append(base)
        bases[base].append(c)

    if all(len(cols) == 1 for cols in bases.values()):
        # strip만 적용
        out = df.copy()
        out.columns = order
        return out

    out = pd.DataFrame(index=df.index)
    for base in order:
        cols = bases[base]
        if len(cols) == 1:
            out[base] = df[cols[0]]
            continue
        s = df[cols[0]]
        for c in cols[1:]:
            s = s.combine_first(df[c])
        out[base] = s
    return out


def _to_int_series(s: pd.Series) -> pd.Series:
    """금액/수량 컬럼을 안전하게 int로 변환 (콤마/공백 포함 대응)."""
    if s is None:
        return pd.Series(0)
    v = s.astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(v, errors="coerce").fillna(0).astype(int)


def _normalize_id_col(s: pd.Series) -> pd.Series:
    """포스번호/영수번호 leading-zero 정규화: '0001'→'1', '01'→'1'.

    OKPOS CSV 수집 시 order와 item 파일 간에 동일 번호가 '1' vs '01' 형식으로
    불일치하는 경우가 있어 join 키를 정수 문자열로 통일한다.
    비숫자 값(알파벳 포함 등)은 원본 그대로 보존한다.
    """
    stripped = s.astype(str).str.strip()
    numeric = pd.to_numeric(stripped, errors="coerce")
    result = stripped.copy()
    mask = numeric.notna()
    result[mask] = numeric[mask].astype(int).astype(str)
    return result


# ============================================================
# Admin / Repair API
# ============================================================

def resave_existing_unified_sales() -> str:
    """저장된 모든 unified_sales parquet을 sale_date 기준으로 재편성 저장.

    - 모든 파일을 한 번에 읽어 concat 후 _pk 기준 dedup
    - sale_date별 groupby → 날짜당 1회 overwrite 저장
    - sale_date 없거나 비어있는 행은 제외
    """
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    parts: list[pd.DataFrame] = []
    skipped = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as e:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, e)
            skipped += 1
            continue

        if "source" in df.columns:
            df = df.copy()
            df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()

        parts.append(df)

    if not parts:
        raise RuntimeError("읽을 수 있는 unified_sales parquet 없음")

    all_df = pd.concat(parts, ignore_index=True)
    before_dedup = len(all_df)
    if "_pk" in all_df.columns:
        all_df = all_df.drop_duplicates(subset=["_pk"], keep="last")
    logger.info("전체 %d행 로드 (dedup 후 %d행)", before_dedup, len(all_df))

    if "sale_date" not in all_df.columns:
        raise RuntimeError("sale_date 컬럼이 없어 재편성 불가")

    all_df["sale_date"] = all_df["sale_date"].fillna("").astype(str).str.strip()
    targets = sorted({d for d in all_df["sale_date"].unique().tolist() if d and d.lower() != "nan"})
    if not targets:
        return "SKIP: 유효한 sale_date 없음"

    total_targets = 0
    total_rows = 0
    for sale_date in targets:
        grp = all_df[all_df["sale_date"] == sale_date]
        saved = _save_unified_daily(grp, sale_date, overwrite=True)
        total_targets += 1
        total_rows += saved
        logger.info("재저장: %s | %d행", sale_date, saved)

    result = (
        f"unified_sales 재저장 완료 | 소스파일 {len(parts)}개 (스킵 {skipped}개) | "
        f"날짜 {total_targets}개 | 신규행 {total_rows}행"
    )
    logger.info(result)
    return result


def normalize_existing_unified_platforms(
    *,
    apply: bool = False,
    rebuild_summary: bool = True,
) -> str:
    """기존 unified_sales parquet의 platform 표기를 소급 정규화한다."""
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    changed_files = 0
    changed_rows = 0
    skipped = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("platform 정규화 parquet 로드 실패, 스킵: %s | %s", path, exc)
            skipped += 1
            continue

        if df.empty or "platform" not in df.columns:
            continue

        platform = df["platform"].fillna("").astype(str).str.strip()
        mask = platform.isin(PLATFORM_NORMALIZE_MAP)
        count = int(mask.sum())
        if count == 0:
            continue

        changed_files += 1
        changed_rows += count
        if not apply:
            continue

        df_out = normalize_unified_platforms(df).reindex(columns=UNIFIED_COLUMNS, fill_value="")
        for col in ("qty", "unit_price", "total_price", "discount_amount", "order_cnt"):
            if col in df_out.columns:
                df_out[col] = pd.to_numeric(df_out[col], errors="coerce").fillna(0).astype(int)
        save_unified_parquet(df_out, path)
        logger.info("platform 정규화 저장: %s | 변경=%d", path.name, count)

    summary_msg = "요약 재생성 스킵"
    if apply and rebuild_summary and changed_rows:
        from modules.transform.pipelines.db.DB_UnifiedSales_validate import build_daily_summary

        summary_msg = build_daily_summary()

    mode = "apply" if apply else "dry-run"
    result = (
        f"unified_sales platform 정규화 {mode} 완료 | 파일={changed_files} "
        f"행={changed_rows} 스킵={skipped} | {summary_msg}"
    )
    logger.info(result)
    return result


def repartition_unified_sales_by_sale_date() -> str:
    """Repartition existing unified_sales parquet by `sale_date` (YYYY-MM-DD).

    Some repair flows may leave files that are grouped by collected_at, which can contain multiple sale_date
    values. If you later read all files via glob and group by sale_date, those rows get double-counted.

    This function loads every unified_sales_*.parquet, globally deduplicates by `_pk`, then overwrites
    per-sale_date files so filename date matches the sale_date. 수동 복구 전용으로만 실행한다.
    """
    logger.warning(
        "repartition: unified_sales 전체 재기록 시작 — OneDrive 동기화 중이면 충돌본 발생 위험"
    )
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    parts: list[pd.DataFrame] = []
    skipped = 0
    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, exc)
            skipped += 1
            continue
        if "source" in df.columns:
            df = df.copy()
            df["source"] = df["source"].fillna("").astype(str).str.strip().str.lower()
        parts.append(df)

    if not parts:
        raise RuntimeError("읽을 수 있는 unified_sales parquet 없음")

    all_df = pd.concat(parts, ignore_index=True)
    if "_pk" in all_df.columns:
        before = len(all_df)
        all_df = all_df.drop_duplicates(subset=["_pk"], keep="last").reset_index(drop=True)
        logger.info("global dedup by _pk: %d -> %d", before, len(all_df))

    if "sale_date" not in all_df.columns:
        raise RuntimeError("sale_date 컬럼이 없어 repartition 불가")

    all_df["sale_date"] = all_df["sale_date"].fillna("").astype(str).str.strip()
    targets = sorted({d for d in all_df["sale_date"].unique().tolist() if d and d.lower() != "nan"})
    if not targets:
        return "SKIP: 유효한 sale_date 없음"

    total_saved = 0
    for d in targets:
        grp = all_df[all_df["sale_date"] == d]
        saved = _save_unified_daily(grp, d, overwrite=True)
        total_saved += saved

    result = f"OK: repartition by sale_date | files={len(parts)} skip={skipped} days={len(targets)} saved={total_saved}"
    logger.info(result)
    return result


def filter_manual_delivery_sources_for_test_stores(
    df: pd.DataFrame,
    stores: list[str] | None = None,
) -> pd.DataFrame:
    """테스트 매장 배달행을 platform 기준으로 정리한다.

    - 과거일(어제 이하): 같은 매장/일자/플랫폼 패밀리에 수동(배민수동/쿠팡수동)
      행이 실제로 있을 때만 비수동(POS/posfeed/okpos) 행을 제거한다.
      수동 수집이 비어 있으면 POS 계열 행을 fallback으로 유지해 매출 누락을 막는다.
    - 오늘: 테스트 매장 배달은 자동(POS/posfeed)만 사용 (수동 행 제거)
    - sale_date가 비어있는 행은 today/past 어느 쪽으로도 판정하지 않고 그대로 둔다.
    """
    if df.empty or not {"store", "platform", "source"}.issubset(df.columns):
        return df

    store_set = {
        str(store).strip()
        for store in (stores or DELIVERY_MANUAL_TEST_STORES)
        if str(store).strip()
    }
    if not store_set:
        return df

    out = df.copy()
    store = out["store"].fillna("").astype(str).str.strip()
    platform = out["platform"].fillna("").astype(str).str.strip()
    source = out["source"].fillna("").astype(str).str.strip()
    date = (
        out["sale_date"].fillna("").astype(str).str.strip()
        if "sale_date" in out.columns
        else pd.Series("", index=out.index)
    )
    today = _kst_today_str()
    has_date = date.ne("")

    remove_mask = pd.Series(False, index=out.index)
    for manual_src, family in DELIVERY_PLATFORM_FAMILIES.items():
        in_family = store.isin(store_set) & platform.isin(family)
        if not in_family.any():
            continue

        is_manual = source.eq(manual_src)
        is_today = has_date & date.eq(today)
        is_past = has_date & date.ne(today)

        # 오늘: 테스트 매장도 자동수집(POS/posfeed)만 사용 → 수동 행 제거.
        remove_mask |= in_family & is_today & is_manual

        # 과거: 같은 매장/일자/플랫폼군에 수동 행이 실제로 있을 때만 비수동 행 제거.
        manual_keys = set(
            zip(
                store[in_family & is_past & is_manual],
                date[in_family & is_past & is_manual],
            )
        )
        if manual_keys:
            row_keys = pd.Series(list(zip(store, date)), index=out.index)
            has_manual_family = row_keys.isin(manual_keys)
            remove_mask |= in_family & is_past & ~is_manual & has_manual_family

    removed = int(remove_mask.sum())
    if removed:
        logger.warning("테스트 매장 수동 우선 배달 정리: %d행 제거", removed)
    return out[~remove_mask].reset_index(drop=True)


def filter_manual_delivery_sources_for_non_test_stores(
    df: pd.DataFrame,
    stores: list[str] | None = None,
) -> pd.DataFrame:
    """비테스트 매장에 남은 배민수동/쿠팡수동 행을 제거한다."""
    if df.empty or not {"store", "source"}.issubset(df.columns):
        return df

    store_set = {
        str(store).strip()
        for store in (stores or DELIVERY_MANUAL_TEST_STORES)
        if str(store).strip()
    }
    manual_sources = set(DELIVERY_PLATFORM_FAMILIES)

    out = df.copy()
    store = out["store"].fillna("").astype(str).str.strip()
    source = out["source"].fillna("").astype(str).str.strip()
    remove_mask = source.isin(manual_sources) & ~store.isin(store_set)

    removed = int(remove_mask.sum())
    if removed:
        logger.warning("비테스트 매장 수동 배달 source 정리: %d행 제거", removed)
    return out[~remove_mask].reset_index(drop=True)


def enforce_manual_delivery_sources_for_test_stores(
    stores: list[str] | None = None,
) -> str:
    """기존 unified_sales parquet에서 테스트 매장 배달 플랫폼 중복 source를 제거."""
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    changed_files = 0
    total_removed = 0
    skipped = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("수동 source 강제 parquet 로드 실패, 스킵: %s | %s", path, exc)
            skipped += 1
            continue

        before = len(df)
        df_out = filter_manual_delivery_sources_for_test_stores(df, stores=stores)
        removed = before - len(df_out)
        if removed <= 0:
            continue

        df_out = df_out.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        for col in ("qty", "unit_price", "total_price", "discount_amount", "order_cnt"):
            if col in df_out.columns:
                df_out[col] = pd.to_numeric(df_out[col], errors="coerce").fillna(0).astype(int)
        save_unified_parquet(df_out, path)
        changed_files += 1
        total_removed += removed
        logger.warning("테스트 매장 배달 수동 source 강제: %s | 제거=%d", path.name, removed)

    result = (
        f"테스트 매장 배달 수동 source 강제 완료 | 파일={changed_files} "
        f"제거={total_removed} 스킵={skipped}"
    )
    logger.info(result)
    return result


def purge_manual_delivery_sources_for_non_test_stores(
    stores: list[str] | None = None,
) -> str:
    """기존 unified_sales parquet에서 비테스트 매장의 수동 배달 source를 제거."""
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    changed_files = 0
    total_removed = 0
    skipped = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("비테스트 수동 source parquet 로드 실패, 스킵: %s | %s", path, exc)
            skipped += 1
            continue

        before = len(df)
        df_out = filter_manual_delivery_sources_for_non_test_stores(df, stores=stores)
        removed = before - len(df_out)
        if removed <= 0:
            continue

        df_out = df_out.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        for col in ("qty", "unit_price", "total_price", "discount_amount", "order_cnt"):
            if col in df_out.columns:
                df_out[col] = pd.to_numeric(df_out[col], errors="coerce").fillna(0).astype(int)
        save_unified_parquet(df_out, path)
        changed_files += 1
        total_removed += removed
        logger.warning("비테스트 매장 수동 배달 source 정리: %s | 제거=%d", path.name, removed)

    result = (
        f"비테스트 매장 수동 배달 source 정리 완료 | 파일={changed_files} "
        f"제거={total_removed} 스킵={skipped}"
    )
    logger.info(result)
    return result


def refresh_store_meta_in_unified_sales() -> str:
    """현재 sales_employee.csv 기준으로 unified_sales 매장 메타를 일괄 갱신."""
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    store_map = _load_store_map()
    total_rows = 0
    changed_files = 0
    skipped = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("매장 메타 갱신 parquet 로드 실패, 스킵: %s | %s", path, exc)
            skipped += 1
            continue

        if df.empty or "store" not in df.columns:
            continue

        df = df.copy()
        before = df.reindex(columns=["담당자", "region", "실오픈일"], fill_value="")
        store_key = df["store"].fillna("").astype(str).str.strip().str.split().str[-1]
        known_mask = store_key.isin(store_map)
        for field in ("담당자", "region", "실오픈일"):
            if field not in df.columns:
                df[field] = ""
            df.loc[known_mask, field] = store_key.loc[known_mask].map(
                lambda key: _lookup_store_meta(store_map, key, field)
            )

        after = df[["담당자", "region", "실오픈일"]]
        if before.equals(after):
            continue

        df = df.reindex(columns=UNIFIED_COLUMNS, fill_value="")
        save_unified_parquet(df, path)
        changed_files += 1
        total_rows += len(df)

    result = (
        f"unified_sales 매장 메타 갱신 완료 | 파일={changed_files} "
        f"행={total_rows} 스킵={skipped}"
    )
    logger.info(result)
    return result


def purge_source_from_unified_sales(source: str = "toorder") -> str:
    """Remove all rows of target source from all unified_sales parquet files."""
    src = str(source).strip().lower()
    files = iter_unified_sales_files()
    if not files:
        msg = f"unified_sales parquet 없음, 스킵 | {UNIFIED_ROOT}"
        logger.warning(msg)
        return msg

    total_removed = 0
    changed_files = 0

    for path in files:
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("parquet 로드 실패, 스킵: %s | %s", path, exc)
            continue
        if "source" not in df.columns:
            continue

        mask = df["source"].fillna("").astype(str).str.strip().str.lower() == src
        removed = int(mask.sum())
        if removed == 0:
            continue

        try:
            df = df[~mask].reset_index(drop=True)
            save_unified_parquet(df, path)
        except Exception as exc:
            logger.warning("unified_sales parquet 저장 실패, 스킵: %s | %s", path, exc)
            continue

        total_removed += removed
        changed_files += 1
        logger.info("%s 행 제거: %s (%d행)", src, path.name, removed)

    result = f"OK: purge source={src} | files={changed_files} removed={total_removed}"
    logger.info(result)
    return result


def reclassify_hall_platform(date_str: str, overwrite: bool = True) -> str:
    """unified_sales parquet의 포스/제휴사주문 행을 테이블명 기반으로 재분류."""
    path = _unified_daily_path(date_str)
    if not path.exists():
        return f"파일 없음: {path}"

    df = pd.read_parquet(path)
    if "테이블명" not in df.columns:
        return f"{date_str}: 테이블명 컬럼 없음, 스킵"

    target = df["platform"].isin(["포스", "제휴사주문"])
    table_v = df.loc[target, "테이블명"].fillna("").astype(str).str.strip()
    is_pack = table_v.str.contains("포장", regex=False).reindex(df.index, fill_value=False)
    is_num = table_v.str.fullmatch(r"\d+").fillna(False).reindex(df.index, fill_value=False)

    df.loc[target & is_pack, "order_type"] = "홀_포장"
    df.loc[target & is_pack, "platform"] = "홀"
    df.loc[target & is_num, "order_type"] = "홀_테이블"
    df.loc[target & is_num, "platform"] = "홀"

    saved = _save_unified_daily(df, date_str, overwrite=overwrite)
    result = f"{date_str}: {saved}행 재분류 저장"
    logger.info(result)
    return result
