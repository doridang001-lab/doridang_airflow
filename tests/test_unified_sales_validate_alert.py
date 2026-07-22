from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db import DB_UnifiedSales_validate as validate


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
