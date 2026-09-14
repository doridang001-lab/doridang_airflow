import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.strategy import SMP_chicken_price_collect as chicken_price


def test_moving_average_ignores_failed_history_rows():
    rows = [
        {
            "success": True,
            "source_site": "poultry.or.kr",
            "base_date": f"2026-06-{day:02d}",
            "price_today": 1900 + day,
            "collected_at": f"2026-06-{day:02d} 00:00:00",
        }
        for day in range(1, 21)
    ]
    rows.append(
        {
            "success": False,
            "source_site": "poultry.or.kr",
            "base_date": None,
            "price_today": None,
            "collected_at": "2026-06-30 00:00:00",
            "error_message": "temporary dns failure",
        }
    )

    history = chicken_price._prepare_price_history(pd.DataFrame(rows))
    ma_info = chicken_price._calculate_moving_averages(history)
    html = chicken_price._build_email_html([], ma_info, "2026-06-20", any_failure=False)

    assert len(history) == 20
    assert ma_info["poultry.or.kr"]["ma20"] == sum(1900 + day for day in range(1, 21)) / 20
    assert "20일 평균" in html
    assert "nan원" not in html


def test_prev_month_for_chicken_is_filled_from_history():
    history = chicken_price._prepare_price_history(
        pd.DataFrame(
            [
                {
                    "success": True,
                    "source_site": "chicken.or.kr",
                    "base_date": "2026-05-29",
                    "price_today": 2490,
                    "collected_at": "2026-05-30 00:00:00",
                },
                {
                    "success": True,
                    "source_site": "chicken.or.kr",
                    "base_date": "2026-06-29",
                    "price_today": 2190,
                    "collected_at": "2026-06-30 00:00:00",
                },
            ]
        )
    )
    results = [
        {
            "success": True,
            "source_site": "chicken.or.kr",
            "base_date": "2026-06-29",
            "price_today": 2190,
            "price_prev_day": 2190,
            "price_prev_month": None,
        }
    ]

    enriched = chicken_price._enrich_prev_month_from_history(results, history)
    html = chicken_price._build_email_html(enriched, {}, "2026-06-29", any_failure=False)

    assert enriched[0]["price_prev_month"] == 2490
    assert enriched[0]["price_prev_month_source"] == "history_csv"
    assert "전월" in html
    assert "2,490원" in html


def test_email_html_does_not_render_nan_moving_average():
    html = chicken_price._build_email_html(
        results=[],
        ma_info={"poultry.or.kr": {"ma20": float("nan"), "ma60": None, "ma120": None}},
        base_date="2026-06-29",
        any_failure=False,
    )

    assert "nan원" not in html
    assert "이동평균" not in html
