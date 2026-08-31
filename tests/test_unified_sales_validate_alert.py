from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db import DB_UnifiedSales_validate as validate
from modules.transform.utility import notifier


def _alert_rows(date_col: str, date_value: str) -> pd.DataFrame:
    rows = []
    for store, total_difference, total_rate in [
        ("광명철산점", 2_944_224, 22.3),
        ("시흥배곧점", 553_300, 5.8),
    ]:
        for channel, difference in [
            ("총합", total_difference),
            ("쿠팡", 100_000),
            ("배민", total_difference - 100_000),
            ("기타", 0),
        ]:
            rows.append(
                {
                    date_col: date_value,
                    "store": store,
                    "channel": channel,
                    "excel_total": 10_000_000,
                    "unified_total": 10_000_000 + difference,
                    "difference": difference,
                    "error_rate": total_rate if channel == "총합" else 1.0,
                    "reason": "금액상이",
                    "status": "error" if channel == "총합" else "ok",
                }
            )
    return pd.DataFrame(rows)


def test_monthly_alert_ends_after_ranked_store_list(monkeypatch, tmp_path: Path):
    sent = []
    monkeypatch.setattr(validate, "send_telegram_chunks", sent.append)

    validate._send_monthly_alert(
        "2026-07",
        _alert_rows("ym", "2026-07"),
        tmp_path / "unified_sales_monthly_2026-07.csv",
        max_date="2026-07-21",
    )

    assert len(sent) == 1
    message = sent[0]
    assert "[도리당] unified_sales 월별 검증 알림" in message
    assert "대상월: 2026-07" in message
    assert "비교범위: 2026-07-01 ~ 2026-07-21" in message
    assert "오차율 2% 이상 매장: 2곳" in message
    assert "unified_sales_monthly_2026-07.csv" in message
    assert "■ 오차 매장 (2곳)" in message
    assert message.index("1. 광명철산점") < message.index("2. 시흥배곧점")
    assert message.endswith("2. 시흥배곧점  차이 +553,300 (5.8%)")
    assert "■ 상세내역" not in message
    assert "/ 총합:" not in message
    assert "/ 쿠팡:" not in message
    assert "/ 배민:" not in message
    assert "/ 기타:" not in message


def test_monthly_alert_notes_lagged_toorder_baseline(monkeypatch, tmp_path: Path):
    sent = []
    monkeypatch.setattr(validate, "send_telegram_chunks", sent.append)

    validate._send_monthly_alert(
        "2026-07",
        _alert_rows("ym", "2026-07"),
        tmp_path / "unified_sales_monthly_2026-07.csv",
        max_date="2026-07-28",
        baseline_lagged=True,
    )

    assert len(sent) == 1
    assert "비교범위: 2026-07-01 ~ 2026-07-28" in sent[0]
    assert "※ ToOrder 최신일 부분 수집 의심으로 2026-07-28까지만 비교" in sent[0]


def test_monthly_alert_notes_excluded_partial_dates(monkeypatch, tmp_path: Path):
    sent = []
    monkeypatch.setattr(validate, "send_telegram_chunks", sent.append)

    validate._send_monthly_alert(
        "2026-08",
        _alert_rows("ym", "2026-08"),
        tmp_path / "unified_sales_monthly_2026-08.csv",
        max_date="2026-08-25",
        excluded_dates={"2026-08-24": "stores 14/61 (23%)"},
    )

    assert len(sent) == 1
    assert "비교범위: 2026-08-01 ~ 2026-08-25" in sent[0]
    assert "※ ToOrder 부분 수집일 제외: 2026-08-24" in sent[0]


def test_toorder_baseline_max_date_skips_partial_latest(monkeypatch, tmp_path: Path):
    path = tmp_path / "toorder_store_platform_daily.parquet"
    rows = []
    for day in range(17, 24):
        for store_idx in range(50):
            rows.append(
                {
                    "date": f"2026-08-{day:02d}",
                    "store": f"매장{store_idx:02d}",
                    "price": 100_000,
                }
            )
    for store_idx in range(10):
        rows.append({"date": "2026-08-24", "store": f"매장{store_idx:02d}", "price": 10_000})
    pd.DataFrame(rows).to_parquet(path, index=False)
    monkeypatch.setattr(validate, "TOORDER_DAILY_PARQUET", path)

    assert validate._toorder_baseline_max_date() == "2026-08-23"
    usable, reason = validate._is_toorder_baseline_date_usable("2026-08-24")
    assert usable is False
    assert "stores" in reason


def test_toorder_partial_dates_for_month_finds_middle_partial_day(monkeypatch, tmp_path: Path):
    path = tmp_path / "toorder_store_platform_daily.parquet"
    rows = []
    for day in range(17, 24):
        for store_idx in range(50):
            rows.append(
                {
                    "date": f"2026-08-{day:02d}",
                    "store": f"매장{store_idx:02d}",
                    "price": 100_000,
                }
            )
    for store_idx in range(10):
        rows.append({"date": "2026-08-24", "store": f"매장{store_idx:02d}", "price": 10_000})
    for store_idx in range(50):
        rows.append({"date": "2026-08-25", "store": f"매장{store_idx:02d}", "price": 100_000})
    pd.DataFrame(rows).to_parquet(path, index=False)
    monkeypatch.setattr(validate, "TOORDER_DAILY_PARQUET", path)

    bad = validate._toorder_partial_dates_for_month("2026-08", max_date="2026-08-25")

    assert sorted(bad) == ["2026-08-24"]
    assert "stores" in bad["2026-08-24"]


def test_monthly_alert_is_allowed_by_telegram_policy():
    assert notifier._should_send_telegram("[도리당] unified_sales 월별 검증 알림\n대상월: 2026-07")


def test_unified_sales_non_actionable_status_messages_are_suppressed():
    assert notifier._should_send_telegram("[도리당] Today UnifiedSales 완료\nsale_date: 2026-07-21") is False
    assert (
        notifier._should_send_telegram(
            "[도리당] unified_sales 일별 검증 보류: 2026-07-29 ToOrder 기준값 없음(수집 지연)"
        )
        is False
    )


def test_daily_alert_keeps_channel_details(monkeypatch, tmp_path: Path):
    sent = []
    monkeypatch.setattr(validate, "send_telegram_chunks", sent.append)

    validate._send_alert(
        "2026-07-21",
        _alert_rows("sale_date", "2026-07-21"),
        tmp_path / "unified_sales_error_2026-07-21.csv",
    )

    assert len(sent) == 1
    message = sent[0]
    assert "[도리당] unified_sales 일별 검증 알림" in message
    assert "■ 상세내역" in message
    assert "/ 총합:" in message
    assert "/ 쿠팡:" in message
    assert "/ 배민:" in message
    assert "/ 기타:" in message


def test_validate_sales_holds_when_toorder_baseline_missing(monkeypatch):
    sent = []
    monkeypatch.setattr(validate, "send_telegram", sent.append)
    monkeypatch.setattr(validate, "_resolve_daily_validation_target_date", lambda **context: "2026-07-29")
    monkeypatch.setattr(validate, "_is_toorder_baseline_date_usable", lambda target_date: (True, ""))
    monkeypatch.setattr(
        validate,
        "_load_parquet_totals",
        lambda target_date: pd.DataFrame(columns=["sale_date", "store", "channel", "unified_total"]),
    )
    monkeypatch.setattr(validate, "_load_unified_platform_keys", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        validate,
        "_load_excel_totals",
        lambda *args, **kwargs: pd.DataFrame(columns=["sale_date", "store", "channel", "excel_total"]),
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError("기준값이 없으면 diff/CSV 저장 단계로 진행하면 안 됨")

    monkeypatch.setattr(validate, "_compute_diff", fail_if_called)
    monkeypatch.setattr(validate, "_save_validation_csv", fail_if_called)

    result = validate.validate_sales()

    expected = "[도리당] unified_sales 일별 검증 보류: 2026-07-29 ToOrder 기준값 없음(수집 지연)"
    assert result == expected
    assert sent == [expected]


def test_validate_sales_holds_when_toorder_baseline_partial(monkeypatch):
    sent = []
    monkeypatch.setattr(validate, "send_telegram", sent.append)
    monkeypatch.setattr(validate, "_resolve_daily_validation_target_date", lambda **context: "2026-08-24")
    monkeypatch.setattr(
        validate,
        "_load_parquet_totals",
        lambda target_date: pd.DataFrame(columns=["sale_date", "store", "channel", "unified_total"]),
    )
    monkeypatch.setattr(
        validate,
        "_is_toorder_baseline_date_usable",
        lambda target_date: (False, "stores 14/61 (23%)"),
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError("부분 수집일이면 ToOrder 월별/일별 비교를 진행하면 안 됨")

    monkeypatch.setattr(validate, "_load_unified_platform_keys", fail_if_called)
    monkeypatch.setattr(validate, "_load_excel_totals", fail_if_called)

    result = validate.validate_sales()

    expected = (
        "[도리당] unified_sales 일별 검증 보류: 2026-08-24 "
        "ToOrder 기준값 부분 수집 의심(stores 14/61 (23%))"
    )
    assert result == expected
    assert sent == [expected]
