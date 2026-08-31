from pathlib import Path

import pandas as pd
import pytest

from scripts.analysis import susam_report


def _source_frame(rows):
    return pd.DataFrame(rows, columns=sorted(susam_report.REQUIRED_COLUMNS))


def _row(**overrides):
    row = {
        "sale_date": "2026-07-06",
        "store": "백석점",
        "item_name": "수삼 백숙",
        "qty": "1",
        "total_price": "20000",
    }
    row.update(overrides)
    return row


def _write_daily_parquet(root: Path, date_code: str, frame: pd.DataFrame) -> None:
    root.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(root / f"unified_sales_{date_code}.parquet", index=False)


def _daily_frame(date_str: str = "2026-07-06") -> pd.DataFrame:
    return pd.DataFrame(
        [
            [date_str, "강동<script>점", "수삼 & 백숙", 2, 50000],
            [date_str, "백석점", "왕수삼", 1, 25000],
        ],
        columns=susam_report.REPORT_COLUMNS,
    )


def _normalized_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ["2026-07-06", "백석점", "수삼 백숙", 1, 20000],
            ["2026-07-06", "백석점", "왕수삼", 2, 30000],
            ["2026-07-07", "동탄점", "수삼 백숙", 1, 25000],
        ],
        columns=susam_report.NORMALIZED_COLUMNS,
    )


def test_build_daily_frame_filters_susam_and_aggregates(tmp_path, monkeypatch):
    source_root = tmp_path / "source"
    frame = _source_frame(
        [
            _row(qty="1", total_price="20,000"),
            _row(qty="2", total_price="30,000"),
            _row(store="동탄점", item_name="왕수삼", total_price="25,000"),
            _row(item_name="일반 백숙", total_price="15,000"),
            _row(sale_date="2026-07-05", item_name="수삼 다른 날짜", total_price="99,000"),
        ]
    )
    _write_daily_parquet(source_root, "260706", frame)
    monkeypatch.setattr(susam_report, "UNIFIED_ROOT", source_root)

    report = susam_report.build_daily_frame("2026-07-06")

    assert list(report.columns) == list(susam_report.REPORT_COLUMNS)
    assert report.to_dict("records") == [
        {
            "order_date": "2026-07-06",
            "store": "동탄점",
            "item_name": "왕수삼",
            "qty": 1,
            "총매출액": 25000,
        },
        {
            "order_date": "2026-07-06",
            "store": "백석점",
            "item_name": "수삼 백숙",
            "qty": 3,
            "총매출액": 50000,
        },
    ]


def test_build_daily_frame_matches_requested_date_store_item_shape(tmp_path, monkeypatch):
    source_root = tmp_path / "source"
    frame = _source_frame(
        [
            _row(
                sale_date="2026-07-24",
                store="대전둔산점",
                item_name="[복날한정] 1인 미나리 수삼 백숙",
                qty="1",
                total_price="16900",
            ),
            _row(
                sale_date="2026-07-24",
                store="대전둔산점",
                item_name="[복날한정] 1인 미나리 수삼 백숙",
                qty="1",
                total_price="16900",
            ),
            _row(
                sale_date="2026-07-24",
                store="미사점",
                item_name="[복날한정] 미나리 수삼 백숙",
                qty="1",
                total_price="22161",
            ),
        ]
    )
    _write_daily_parquet(source_root, "260724", frame)
    monkeypatch.setattr(susam_report, "UNIFIED_ROOT", source_root)

    report = susam_report.build_daily_frame("2026-07-24")

    assert report.to_dict("records") == [
        {
            "order_date": "2026-07-24",
            "store": "대전둔산점",
            "item_name": "[복날한정] 1인 미나리 수삼 백숙",
            "qty": 2,
            "총매출액": 33800,
        },
        {
            "order_date": "2026-07-24",
            "store": "미사점",
            "item_name": "[복날한정] 미나리 수삼 백숙",
            "qty": 1,
            "총매출액": 22161,
        },
    ]


def test_load_susam_range_skips_missing_and_rejects_bad_schema(tmp_path, monkeypatch):
    source_root = tmp_path / "source"
    _write_daily_parquet(
        source_root,
        "260706",
        _source_frame([_row(), _row(item_name="일반 백숙")]),
    )
    monkeypatch.setattr(susam_report, "UNIFIED_ROOT", source_root)

    loaded = susam_report.load_susam_range(["2026-07-06", "2026-07-07"])
    assert loaded["item_name"].tolist() == ["수삼 백숙"]

    _write_daily_parquet(source_root, "260708", pd.DataFrame({"item_name": ["수삼"]}))
    with pytest.raises(ValueError, match="필수 컬럼 누락"):
        susam_report.load_susam_range(["2026-07-08"])


def test_weekly_frame_has_matching_date_and_store_totals():
    weekly = susam_report.build_weekly_frame(_normalized_frame())

    assert weekly["총매출액"].sum() == 75000
    assert weekly.groupby("order_date")["총매출액"].sum().to_dict() == {
        "2026-07-06": 50000,
        "2026-07-07": 25000,
    }
    assert weekly.groupby("store")["총매출액"].sum().to_dict() == {
        "동탄점": 25000,
        "백석점": 50000,
    }


def test_monthly_frame_uses_one_month_week_helper_and_preserves_totals():
    frame = pd.DataFrame(
        [
            ["2026-07-01", "강동점", "수삼", 1, 10000],
            ["2026-07-06", "강동점", "수삼", 2, 20000],
            ["2026-07-31", "백석점", "수삼", 3, 30000],
        ],
        columns=susam_report.NORMALIZED_COLUMNS,
    )

    monthly = susam_report.build_monthly_frame(frame)

    assert monthly["month_week"].tolist() == ["W1", "W2", "W5"]
    assert monthly["총매출액"].sum() == 60000
    assert monthly.groupby("month_week")["총매출액"].sum().sum() == 60000
    assert monthly.groupby("order_date")["총매출액"].sum().sum() == 60000
    assert susam_report.month_week_label("2026-07-06") == "W2"


def test_body_html_escapes_dynamic_text_and_contains_expected_sections():
    daily = _daily_frame()
    weekly = susam_report.build_weekly_frame(
        _normalized_frame().assign(store="강동<script>점")
    )
    monthly = susam_report.build_monthly_frame(_normalized_frame())

    daily_html = susam_report.build_daily_body_html("2026-07-06", daily)
    weekly_html = susam_report.build_weekly_body_html(2026, 28, weekly)
    monthly_html = susam_report.build_monthly_body_html("2026-07", monthly)

    assert "강동&lt;script&gt;점" in daily_html
    assert "수삼 &amp; 백숙" in daily_html
    assert "<script>" not in daily_html
    assert "2026-07-06 ~ 2026-07-12" in weekly_html
    assert "날짜별 현황" in weekly_html and "매장별 현황" in weekly_html
    assert "주차별 현황" in monthly_html and "매장별 TOP" in monthly_html
    assert weekly_html.endswith(susam_report.MEMO_SENTINEL_HTML)
    assert monthly_html.endswith(susam_report.MEMO_SENTINEL_HTML)


def test_empty_body_html_keeps_tables_and_memo_sentinel():
    daily = susam_report._empty_frame(susam_report.REPORT_COLUMNS)
    weekly = susam_report._empty_frame(susam_report.WEEKLY_COLUMNS)
    monthly = susam_report._empty_frame(susam_report.MONTHLY_COLUMNS)

    assert "판매 내역 없음" in susam_report.build_daily_body_html("2026-07-06", daily)
    assert "판매 내역 없음" in susam_report.build_weekly_body_html(2026, 28, weekly)
    assert "판매 내역 없음" in susam_report.build_monthly_body_html("2026-07", monthly)


def test_merge_preserving_memo_keeps_existing_tail_with_heading_attributes():
    existing = (
        "<p>오래된 자동 집계</p>"
        '<h2 class="memo-title">📝 이벤트·활동 기록 (수기 입력)</h2>'
        "<p>7월 8일 전단 배포</p><ul><li>담당: 홍길동</li></ul>"
    )
    new_auto = "<h1>새 집계</h1>" + susam_report.MEMO_SENTINEL_HTML

    merged = susam_report.merge_preserving_memo(existing, new_auto)

    assert merged == (
        new_auto + "<p>7월 8일 전단 배포</p><ul><li>담당: 홍길동</li></ul>"
    )
    assert "오래된 자동 집계" not in merged


def test_merge_preserving_memo_adds_empty_template_when_sentinel_missing():
    new_auto = "<h1>새 집계</h1>" + susam_report.MEMO_SENTINEL_HTML

    assert susam_report.merge_preserving_memo("<p>기존 본문</p>", new_auto) == (
        new_auto + susam_report.EMPTY_MEMO_HTML
    )


def test_parser_requires_complete_ordered_date_range():
    parser = susam_report.build_parser()
    args = parser.parse_args(["--start", "2026-07-02", "--end", "2026-07-20", "--no-upload"])
    susam_report.validate_args(parser, args)
    assert [value.isoformat() for value in susam_report.target_dates(args)][-1] == "2026-07-20"
    with pytest.raises(SystemExit):
        args = parser.parse_args(["--start", "2026-07-20", "--end", "2026-07-02"])
        susam_report.validate_args(parser, args)


def test_main_no_upload_generates_daily_weekly_and_monthly_without_driver(
    tmp_path,
    monkeypatch,
):
    generated = []
    monkeypatch.setattr(
        susam_report,
        "build_daily_frame",
        lambda date_str: generated.append(date_str) or _daily_frame(date_str),
    )
    monkeypatch.setattr(
        susam_report,
        "save_csv",
        lambda _frame, date_str: tmp_path / f"Susam_{date_str}.csv",
    )
    monkeypatch.setattr(susam_report, "load_susam_range", lambda _dates: _normalized_frame())
    monkeypatch.setattr(
        susam_report,
        "_create_driver",
        lambda *_args: pytest.fail("no-upload에서 드라이버를 생성하면 안 됩니다."),
    )

    result = susam_report.main(
        ["--start", "2026-07-06", "--end", "2026-07-07", "--no-upload"]
    )

    assert result == {
        "generated": ["2026-07-06", "2026-07-07"],
        "uploaded": [],
        "skipped": [],
        "failed": [],
    }
    assert generated == ["2026-07-06", "2026-07-07"]


def test_main_uses_one_driver_and_upserts_daily_then_weekly_monthly(
    tmp_path,
    monkeypatch,
):
    calls = []

    class Driver:
        current_url = "https://flow.team/l/QB1Na"
        quit_count = 0

        def set_window_size(self, *_args):
            return None

        def get(self, url):
            self.current_url = url

        def quit(self):
            self.quit_count += 1

    driver = Driver()
    monkeypatch.setattr(susam_report, "build_daily_frame", lambda value: _daily_frame(value))
    monkeypatch.setattr(
        susam_report,
        "save_csv",
        lambda _frame, date_str: tmp_path / f"Susam_{date_str}.csv",
    )
    monkeypatch.setattr(susam_report, "load_susam_range", lambda _dates: _normalized_frame())
    monkeypatch.setattr(susam_report, "_create_driver", lambda *_args: driver)
    monkeypatch.setattr(susam_report.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        susam_report,
        "upsert_subtask",
        lambda _driver, title, _body, *, preserve_memo: calls.append(
            (title, preserve_memo)
        ),
    )

    result = susam_report.main(
        [
            "--date",
            "2026-07-06",
            "--post-url",
            "https://flow.team/l/QB1Na",
            "--debugger-address",
            "127.0.0.1:9223",
            "--force",
        ]
    )

    assert calls == [
        ("[데이터] 2026-07-06 미수백 일별 판매", False),
        ("[집계-주] 2026-07 W2 미수백 주간 현황", True),
        ("[집계-월] 2026-07 미수백 월간 현황", True),
    ]
    assert result["uploaded"] == [title for title, _preserve in calls]
    assert result["failed"] == []
    assert driver.quit_count == 1


def test_upsert_subtask_creates_missing_item_and_preserves_memo(monkeypatch):
    order = []

    class Driver:
        def find_elements(self, *_args):
            return [object()]

    driver = Driver()
    existing = (
        susam_report.MEMO_SENTINEL_HTML + "<p>사람이 작성한 메모</p>"
    )
    monkeypatch.setattr(susam_report, "_find_subtask_by_title", lambda *_args: None)
    monkeypatch.setattr(
        susam_report,
        "_add_subtask",
        lambda _driver, title: order.append(("add", title)),
    )
    monkeypatch.setattr(
        susam_report,
        "_open_subtask_editor",
        lambda _driver, title: order.append(("open", title)),
    )
    monkeypatch.setattr(susam_report, "_read_editor_html", lambda _driver: existing)
    monkeypatch.setattr(
        susam_report,
        "_set_editor_html",
        lambda _driver, body: order.append(("set", body)),
    )
    monkeypatch.setattr(
        susam_report,
        "_submit_editor",
        lambda _driver: order.append(("submit", None)),
    )

    new_auto = "<h1>새 집계</h1>" + susam_report.MEMO_SENTINEL_HTML
    susam_report.upsert_subtask(
        driver,
        "주간 제목",
        new_auto,
        preserve_memo=True,
    )

    assert order[0] == ("add", "주간 제목")
    assert order[1] == ("open", "주간 제목")
    assert order[2] == ("set", new_auto + "<p>사람이 작성한 메모</p>")
    assert order[3] == ("submit", None)


def test_upsert_subtask_does_not_add_existing_daily_item(monkeypatch):
    added = []
    set_values = []

    class Driver:
        def find_elements(self, *_args):
            return [object()]

    driver = Driver()
    monkeypatch.setattr(susam_report, "_find_subtask_by_title", lambda *_args: object())
    monkeypatch.setattr(susam_report, "_add_subtask", lambda *_args: added.append(True))
    monkeypatch.setattr(susam_report, "_open_subtask_editor", lambda *_args: None)
    monkeypatch.setattr(
        susam_report,
        "_set_editor_html",
        lambda _driver, body: set_values.append(body),
    )
    monkeypatch.setattr(susam_report, "_submit_editor", lambda _driver: None)

    susam_report.upsert_subtask(
        driver,
        "일별 제목",
        "<p>자동 본문</p>",
        preserve_memo=False,
    )

    assert added == []
    assert set_values == ["<p>자동 본문</p>"]


def test_find_subtask_by_title_requires_exact_normalized_match():
    class Element:
        def __init__(self, text):
            self.text = text

    class Driver:
        elements = [
            Element("[데이터] 2026-07-06 미수백 일별 판매 복사본"),
            Element(" [데이터]  2026-07-06 미수백 일별 판매 "),
        ]

        def find_elements(self, *_args):
            return self.elements

        def execute_script(self, _script, element):
            return element.text

    found = susam_report._find_subtask_by_title(
        Driver(),
        "[데이터] 2026-07-06 미수백 일별 판매",
    )

    assert found is Driver.elements[1]


def test_set_editor_html_selects_all_and_replaces_ckeditor_body():
    calls = []

    class Driver:
        def set_script_timeout(self, seconds):
            calls.append(("timeout", seconds))

        def execute_async_script(self, script, body):
            calls.append(
                (
                    "async",
                    body,
                    "execCommand('selectAll')" in script,
                    "insertHtml(body)" in script,
                    "setData" in script,
                )
            )
            return True

    susam_report._set_editor_html(Driver(), "<h1>본문</h1>")

    assert calls == [
        ("timeout", 30),
        ("async", "<h1>본문</h1>", True, True, False),
    ]


def test_set_editor_html_uses_iframe_fallback():
    calls = []

    class SwitchTo:
        def frame(self, iframe):
            calls.append(("frame", iframe))

        def default_content(self):
            calls.append(("default", None))

    class Driver:
        switch_to = SwitchTo()

        def set_script_timeout(self, _seconds):
            return None

        def execute_async_script(self, *_args):
            return False

        def find_element(self, *_args):
            return "iframe"

        def execute_script(self, script, *args):
            calls.append(
                (
                    "script",
                    "selectNodeContents(body)" in script,
                    "execCommand('insertHTML'" in script,
                    "innerHTML" in script,
                    args,
                )
            )

    susam_report._set_editor_html(Driver(), "<h1>본문</h1>")

    assert calls[0] == ("frame", "iframe")
    assert calls[1] == ("script", True, True, True, ("<h1>본문</h1>",))
    assert calls[-1] == ("default", None)
