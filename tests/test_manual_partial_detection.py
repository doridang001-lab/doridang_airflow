import json

import pandas as pd

from modules.transform.pipelines.db import DB_UnifiedSales_common as common
from modules.transform.pipelines.db import DB_UnifiedSales_baemin as baemin
from modules.transform.pipelines.db import DB_UnifiedSales_coupang as coupang
from modules.transform.utility import notifier


def test_manual_fallback_telegram_is_suppressed_for_all_sources():
    baemin_message = "[도리당] 배달 수동 결측→기준 대체(배민수동)\n- 테스트매장 2026-07-15 POS 100,000/3건"
    coupang_message = "[도리당] 배달 수동 결측→기준 대체(쿠팡수동)\n- 테스트매장 2026-07-15 POS 100,000/3건"

    assert notifier._should_send_telegram(baemin_message) is False
    assert notifier._should_send_telegram(coupang_message) is False


def test_other_manual_delivery_alerts_still_send():
    assert notifier._should_send_telegram("[도리당] 배달 수동 부분수집 의심(배민수동)\n재수집 요망") is True
    assert notifier._should_send_telegram("[도리당] 배달 수동·기준 모두 없음(배민수동)\n재수집 요망") is True


def test_detects_manual_partial_collection(monkeypatch):
    monkeypatch.setattr(
        common,
        "toorder_delivery_summary",
        lambda *args: (0, 0, 0),
    )
    monkeypatch.setattr(
        common,
        "pos_delivery_summary",
        lambda *args: (1_000_000, 25, 10),
    )

    event = common.detect_manual_partial_collection(
        "2026-07-15",
        "테스트매장",
        {"배달의민족"},
        "배민수동",
        400_000,
    )

    assert event is not None
    assert event["gap"] == 600_000
    assert event["ratio"] == 0.4
    assert event["order_cnt"] == 25
    assert event["baseline_label"] == "POS"
    assert event["baseline_total"] == 1_000_000


def test_manual_ratio_above_threshold_is_not_partial(monkeypatch):
    monkeypatch.setattr(
        common,
        "toorder_delivery_summary",
        lambda *args: (0, 0, 0),
    )
    monkeypatch.setattr(
        common,
        "pos_delivery_summary",
        lambda *args: (1_000_000, 25, 10),
    )

    event = common.detect_manual_partial_collection(
        "2026-07-15",
        "테스트매장",
        {"배달의민족"},
        "배민수동",
        900_000,
    )

    assert event is None


def test_manual_gap_below_threshold_is_not_partial(monkeypatch):
    monkeypatch.setattr(
        common,
        "toorder_delivery_summary",
        lambda *args: (0, 0, 0),
    )
    monkeypatch.setattr(
        common,
        "pos_delivery_summary",
        lambda *args: (120_000, 4, 2),
    )

    event = common.detect_manual_partial_collection(
        "2026-07-15",
        "테스트매장",
        {"쿠팡이츠"},
        "쿠팡수동",
        50_000,
    )

    assert event is None


def test_missing_pos_baseline_is_not_partial(monkeypatch):
    monkeypatch.setattr(
        common,
        "toorder_delivery_summary",
        lambda *args: (0, 0, 0),
    )
    monkeypatch.setattr(
        common,
        "pos_delivery_summary",
        lambda *args: (0, 0, 0),
    )

    event = common.detect_manual_partial_collection(
        "2026-07-15",
        "테스트매장",
        {"쿠팡이츠"},
        "쿠팡수동",
        50_000,
    )

    assert event is None


def test_toorder_baseline_suppresses_false_pos_partial(monkeypatch):
    monkeypatch.setattr(
        common,
        "toorder_delivery_summary",
        lambda *args: (1_228_800, 50, 2),
    )
    monkeypatch.setattr(
        common,
        "pos_delivery_summary",
        lambda *args: (2_215_901, 80, 10),
    )

    event = common.detect_manual_partial_collection(
        "2026-03-02",
        "미사점",
        {"배달의민족", "배민1"},
        "배민수동",
        1_228_800,
    )

    assert event is None


def test_toorder_baseline_detects_partial_when_available(monkeypatch):
    monkeypatch.setattr(
        common,
        "toorder_delivery_summary",
        lambda *args: (1_000_000, 50, 2),
    )
    monkeypatch.setattr(
        common,
        "pos_delivery_summary",
        lambda *args: (1_300_000, 80, 10),
    )

    event = common.detect_manual_partial_collection(
        "2026-07-15",
        "테스트매장",
        {"배달의민족"},
        "배민수동",
        400_000,
    )

    assert event is not None
    assert event["baseline_label"] == "ToOrder"
    assert event["baseline_total"] == 1_000_000
    assert event["pos_total"] == 1_300_000
    assert event["gap"] == 600_000


def test_delivery_baseline_uses_toorder_when_toorder_is_lower(monkeypatch):
    monkeypatch.setattr(
        common,
        "toorder_delivery_summary",
        lambda *args: (900_000, 30, 1),
    )
    monkeypatch.setattr(
        common,
        "pos_delivery_summary",
        lambda *args: (1_200_000, 40, 5),
    )

    baseline = common.delivery_baseline_summary(
        "2026-07-15",
        "테스트매장",
        {"쿠팡이츠"},
        "쿠팡수동",
    )

    assert baseline["baseline_label"] == "ToOrder"
    assert baseline["baseline_total"] == 900_000
    assert baseline["baseline_order_cnt"] == 30
    assert baseline["pos_total"] == 1_200_000


def test_delivery_baseline_uses_pos_when_pos_is_lower(monkeypatch):
    monkeypatch.setattr(
        common,
        "toorder_delivery_summary",
        lambda *args: (1_000_000, 33, 1),
    )
    monkeypatch.setattr(
        common,
        "pos_delivery_summary",
        lambda *args: (900_000, 30, 10),
    )

    baseline = common.delivery_baseline_summary(
        "2026-07-15",
        "테스트매장",
        {"쿠팡이츠"},
        "쿠팡수동",
    )

    assert baseline["baseline_label"] == "POS"
    assert baseline["baseline_total"] == 900_000
    assert baseline["baseline_order_cnt"] == 30
    assert baseline["toorder_total"] == 1_000_000


def test_delivery_baseline_falls_back_to_pos(monkeypatch):
    monkeypatch.setattr(
        common,
        "toorder_delivery_summary",
        lambda *args: (0, 0, 0),
    )
    monkeypatch.setattr(
        common,
        "pos_delivery_summary",
        lambda *args: (1_000_000, 25, 10),
    )

    baseline = common.delivery_baseline_summary(
        "2026-07-15",
        "테스트매장",
        {"쿠팡이츠"},
        "쿠팡수동",
    )

    assert baseline["baseline_label"] == "POS"
    assert baseline["baseline_total"] == 1_000_000
    assert baseline["baseline_order_cnt"] == 25


def test_manual_partial_marker_suppresses_duplicates(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "MANUAL_PARTIAL_MARKER_ROOT", tmp_path)
    event = {"manual_total": 400_000, "pos_total": 1_000_000}

    first = common.record_manual_partial_marker(
        "배민수동",
        "테스트매장",
        "2026-07-15",
        event,
    )
    second = common.record_manual_partial_marker(
        "배민수동",
        "테스트매장",
        "2026-07-15",
        event,
    )

    assert first is True
    assert second is False


def test_manual_partial_marker_can_be_recorded_after_clear(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "MANUAL_PARTIAL_MARKER_ROOT", tmp_path)
    args = ("쿠팡수동", "테스트매장", "2026-07-15", {"gap": 600_000})

    assert common.record_manual_partial_marker(*args) is True
    assert common.clear_manual_partial_marker(*args[:3]) is True
    assert common.clear_manual_partial_marker(*args[:3]) is False
    assert common.record_manual_partial_marker(*args) is True


def test_manual_partial_notification_format(monkeypatch):
    sent = []
    monkeypatch.setattr(notifier, "send_telegram", sent.append)

    common.notify_manual_partial(
        "배민수동",
        [
            {
                "store": "테스트매장",
                "date": "2026-07-15",
                "platform": "배달의민족",
                "manual_total": 400_000,
                "pos_total": 1_300_000,
                "pos_order_cnt": 80,
                "baseline_label": "ToOrder",
                "baseline_total": 1_000_000,
                "toorder_total": 1_000_000,
                "toorder_order_cnt": 50,
                "gap": 600_000,
                "ratio": 0.4,
            }
        ],
    )

    assert len(sent) == 1
    assert "배달 수동 부분수집 의심(배민수동)" in sent[0]
    assert "수동 400,000 / ToOrder 1,000,000 (POS 1,300,000/80건)" in sent[0]
    assert "부족 600,000, 40%" in sent[0]
    assert sent[0].endswith("재수집 요망")


def test_manual_partial_uses_pos_and_mentions_toorder_when_pos_is_lower(monkeypatch):
    sent = []
    monkeypatch.setattr(notifier, "send_telegram", sent.append)
    monkeypatch.setattr(
        common,
        "toorder_delivery_summary",
        lambda *args: (1_000_000, 33, 1),
    )
    monkeypatch.setattr(
        common,
        "pos_delivery_summary",
        lambda *args: (900_000, 30, 10),
    )

    event = common.detect_manual_partial_collection(
        "2026-07-15",
        "테스트매장",
        {"쿠팡이츠"},
        "쿠팡수동",
        10,
    )
    assert event is not None
    assert event["baseline_label"] == "POS"
    assert event["baseline_total"] == 900_000

    common.notify_manual_partial("쿠팡수동", [event])

    assert len(sent) == 1
    assert "수동 10 / POS 900,000 (ToOrder 1,000,000/33건)" in sent[0]
    assert "부족 899,990, 0%" in sent[0]


def test_manual_fallback_notification_uses_baseline_label(monkeypatch):
    sent = []
    monkeypatch.setattr(notifier, "send_telegram", sent.append)

    common.notify_manual_fallback(
        "쿠팡수동",
        [
            {
                "store": "테스트매장",
                "date": "2026-07-15",
                "platform": "쿠팡이츠",
                "total_price": 900_000,
                "order_cnt": 30,
                "baseline_label": "POS",
                "baseline_total": 900_000,
                "baseline_order_cnt": 30,
                "pos_total": 900_000,
                "pos_order_cnt": 30,
                "toorder_total": 1_000_000,
                "toorder_order_cnt": 33,
            }
        ],
    )

    assert len(sent) == 1
    assert "배달 수동 결측→기준 대체(쿠팡수동)" in sent[0]
    assert "테스트매장 2026-07-15 쿠팡이츠 POS 900,000/30건 (ToOrder 1,000,000/33건)" in sent[0]
    assert sent[0].endswith("재수집 요망")


def test_manual_missing_all_notification_format(monkeypatch):
    sent = []
    monkeypatch.setattr(notifier, "send_telegram", sent.append)

    common.notify_manual_missing_all(
        "쿠팡수동",
        [
            {
                "store": "테스트매장",
                "date": "2026-07-15",
                "platform": "쿠팡이츠",
            }
        ],
    )

    assert len(sent) == 1
    assert "배달 수동·기준 모두 없음(쿠팡수동)" in sent[0]
    assert "테스트매장 2026-07-15 쿠팡이츠 매출 0" in sent[0]


def test_no_baseline_event_records_suppressed_marker_without_alert(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "MANUAL_FALLBACK_MARKER_ROOT", tmp_path / "fallback")
    monkeypatch.setattr(common, "MANUAL_PARTIAL_MARKER_ROOT", tmp_path / "partial")

    cases = [
        (baemin, baemin._record_baemin_fallback_event, "배민수동", "배달의민족"),
        (coupang, coupang._record_coupang_fallback_event, "쿠팡수동", "쿠팡이츠"),
    ]
    for module, record_event, source, platform in cases:
        monkeypatch.setattr(
            module,
            "delivery_baseline_summary",
            lambda *args: {
                "baseline_label": "POS",
                "baseline_total": 0,
                "baseline_order_cnt": 0,
                "baseline_rows": 0,
                "pos_total": 0,
                "pos_order_cnt": 0,
                "pos_rows": 0,
                "toorder_total": 0,
                "toorder_order_cnt": 0,
                "toorder_rows": 0,
            },
        )
        fallback_events = []
        missing_events = []

        record_event("2026-07-15", "테스트매장", fallback_events, missing_events)
        record_event("2026-07-15", "테스트매장", fallback_events, missing_events)

        assert fallback_events == []
        assert missing_events == []

        marker_path = tmp_path / "fallback" / source / "테스트매장" / "2026-07-15.json"
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
        assert marker["platform"] == platform
        assert marker["rows"] == 0
        assert marker["alert_suppressed"] is True
        assert marker["reason"] == "no_delivery_baseline"


def test_coupang_full_recalc_existing_dates_includes_pos_baseline(monkeypatch):
    df = pd.DataFrame(
        [
            {
                "sale_date": "2026-01-01",
                "store": "테스트매장",
                "platform": "쿠팡이츠",
                "source": "posfeed",
            },
            {
                "sale_date": "2026-01-02",
                "store": "테스트매장",
                "platform": "쿠팡이츠",
                "source": "쿠팡수동",
            },
            {
                "sale_date": "2026-01-03",
                "store": "다른매장",
                "platform": "쿠팡이츠",
                "source": "쿠팡수동",
            },
            {
                "sale_date": "2026-01-04",
                "store": "테스트매장",
                "platform": "배달의민족",
                "source": "쿠팡수동",
            },
        ]
    )
    monkeypatch.setattr(coupang, "iter_unified_sales_files", lambda: ["dummy.parquet"])
    monkeypatch.setattr(coupang.pd, "read_parquet", lambda *args, **kwargs: df)

    dates = coupang._collect_existing_dates(["테스트매장"])

    assert dates == {"2026-01-01", "2026-01-02"}


def test_notifications_skip_empty_events(monkeypatch):
    sent = []
    monkeypatch.setattr(notifier, "send_telegram", sent.append)

    common.notify_manual_partial("배민수동", [])
    common.notify_manual_missing_all("배민수동", [])

    assert sent == []
