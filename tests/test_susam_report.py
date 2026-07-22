import json
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
        "item_name": "기본",
        "qty": "1",
        "total_price": "20000",
    }
    row.update(overrides)
    return row


def _write_daily_parquet(root: Path, date_code: str, frame: pd.DataFrame) -> None:
    root.mkdir(parents=True)
    frame.to_parquet(root / f"unified_sales_{date_code}.parquet", index=False)


def _report_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [["2026-07-01", "강동점", "수삼 백숙", 2, 50000]],
        columns=susam_report.REPORT_COLUMNS,
    )


def _stub_generation(monkeypatch, tmp_path, generated):
    def build(date_str):
        generated.append(date_str)
        report = _report_frame()
        report["order_date"] = date_str
        return report

    monkeypatch.setattr(susam_report, "build_report_frame", build)
    monkeypatch.setattr(
        susam_report,
        "save_csv",
        lambda _report, date_str: tmp_path / f"Susam_{date_str}.csv",
    )


def test_build_report_frame_filters_susam_and_aggregates(tmp_path, monkeypatch):
    source_root = tmp_path / "source"
    frame = _source_frame(
        [
            _row(item_name="수삼 백숙", qty="1", total_price="20,000"),
            _row(item_name="수삼 백숙", qty="2", total_price="30,000"),
            _row(store="동탄점", item_name="왕수삼", qty="1", total_price="25,000"),
            _row(item_name="일반 백숙", qty="1", total_price="15,000"),
            _row(sale_date="2026-07-05", item_name="수삼 다른 날짜", total_price="99,000"),
        ]
    )
    _write_daily_parquet(source_root, "260706", frame)
    monkeypatch.setattr(susam_report, "UNIFIED_ROOT", source_root)

    report = susam_report.build_report_frame("2026-07-06")

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


def test_save_csv_uses_utf8_sig_and_format_comment(tmp_path, monkeypatch):
    monkeypatch.setattr(susam_report, "OUTPUT_DIR", tmp_path)
    report = _report_frame()

    output = susam_report.save_csv(report, "2026-07-01")

    assert output == tmp_path / "Susam_2026-07-01.csv"
    assert output.read_bytes().startswith(b"\xef\xbb\xbf")
    loaded = pd.read_csv(output, encoding="utf-8-sig")
    assert list(loaded.columns) == list(susam_report.REPORT_COLUMNS)
    assert susam_report.format_comment(report, "2026-07-01") == (
        "order_date | store | item_name | qty | 총매출액\n"
        "2026-07-01 | 강동점 | 수삼 백숙 | 2 | 50000"
    )


def test_empty_report_keeps_columns_and_formats_no_sales(tmp_path, monkeypatch):
    source_root = tmp_path / "source"
    _write_daily_parquet(
        source_root,
        "260701",
        _source_frame([_row(sale_date="2026-07-01", item_name="일반 백숙")]),
    )
    monkeypatch.setattr(susam_report, "UNIFIED_ROOT", source_root)

    report = susam_report.build_report_frame("2026-07-01")

    assert report.empty
    assert list(report.columns) == list(susam_report.REPORT_COLUMNS)
    assert susam_report.format_comment(report, "2026-07-01") == "2026-07-01 수삼 판매 내역 없음"


def test_build_report_frame_rejects_missing_required_columns(tmp_path, monkeypatch):
    source_root = tmp_path / "source"
    _write_daily_parquet(source_root, "260706", pd.DataFrame({"item_name": ["수삼"]}))
    monkeypatch.setattr(susam_report, "UNIFIED_ROOT", source_root)
    with pytest.raises(ValueError, match="필수 컬럼 누락"):
        susam_report.build_report_frame("2026-07-06")


def test_parser_requires_complete_ordered_date_range():
    parser = susam_report.build_parser()
    args = parser.parse_args(["--start", "2026-07-02", "--end", "2026-07-20", "--no-upload"])
    susam_report.validate_args(parser, args)
    assert [value.isoformat() for value in susam_report.target_dates(args)][-1] == "2026-07-20"
    with pytest.raises(SystemExit):
        args = parser.parse_args(["--start", "2026-07-20", "--end", "2026-07-02"])
        susam_report.validate_args(parser, args)


def test_state_file_round_trip_is_sorted_and_unique(tmp_path):
    path = tmp_path / "flow_susam_uploaded.json"
    susam_report.save_uploaded_dates({"2026-07-02", "2026-07-01", "2026-07-02"}, path)
    assert json.loads(path.read_text(encoding="utf-8")) == ["2026-07-01", "2026-07-02"]
    assert susam_report.load_uploaded_dates(path) == {"2026-07-01", "2026-07-02"}


def test_main_no_upload_generates_csv_without_flow_config(tmp_path, monkeypatch):
    generated = []
    saved = []
    monkeypatch.setattr(susam_report, "STATE_PATH", tmp_path / "state.json")
    _stub_generation(monkeypatch, tmp_path, generated)
    monkeypatch.setattr(
        susam_report,
        "save_csv",
        lambda _report, date_str: saved.append(date_str) or tmp_path / f"Susam_{date_str}.csv",
    )
    for name in ("FLOW_POST_URL", "CHROME_USER_DATA_DIR", "CHROME_PROFILE", "FLOW_CHROME_DEBUGGER"):
        monkeypatch.delenv(name, raising=False)

    result = susam_report.main(["--date", "2026-07-01", "--no-upload"])

    assert result["generated"] == ["2026-07-01"]
    assert generated == saved == ["2026-07-01"]
    assert not (tmp_path / "state.json").exists()


def test_main_uploads_comment_and_records_success(tmp_path, monkeypatch):
    generated = []
    calls = []
    state_path = tmp_path / "state.json"
    monkeypatch.setattr(susam_report, "STATE_PATH", state_path)
    _stub_generation(monkeypatch, tmp_path, generated)
    monkeypatch.setattr(susam_report, "upload_to_flow", lambda *args: calls.append(args))

    result = susam_report.main(
        [
            "--date", "2026-07-01", "--post-url", "https://flow.team/main.act?detail=test",
            "--debugger-address", "192.168.65.254:9223",
        ]
    )

    assert result["uploaded"] == ["2026-07-01"]
    assert calls[0][0].startswith("order_date | store")
    assert calls[0][1] == "2026-07-01"
    assert calls[0][-1] == "192.168.65.254:9223"
    assert susam_report.load_uploaded_dates(state_path) == {"2026-07-01"}


def test_main_skips_uploaded_date_unless_force(tmp_path, monkeypatch):
    state_path = tmp_path / "state.json"
    susam_report.save_uploaded_dates({"2026-07-01"}, state_path)
    generated = []
    monkeypatch.setattr(susam_report, "STATE_PATH", state_path)
    _stub_generation(monkeypatch, tmp_path, generated)
    monkeypatch.setattr(susam_report, "upload_to_flow", lambda *_args: None)
    common = [
        "--date", "2026-07-01", "--post-url", "https://flow.team/main.act?detail=test",
        "--debugger-address", "192.168.65.254:9223",
    ]

    assert susam_report.main(common)["skipped"] == ["2026-07-01"]
    assert susam_report.main([*common, "--force"])["uploaded"] == ["2026-07-01"]
    assert generated == ["2026-07-01"]


def test_main_upload_failure_does_not_update_state(tmp_path, monkeypatch):
    state_path = tmp_path / "state.json"
    generated = []
    monkeypatch.setattr(susam_report, "STATE_PATH", state_path)
    _stub_generation(monkeypatch, tmp_path, generated)
    monkeypatch.setattr(
        susam_report,
        "upload_to_flow",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("upload failed")),
    )

    result = susam_report.main(
        [
            "--date", "2026-07-01", "--post-url", "https://flow.team/main.act?detail=test",
            "--debugger-address", "192.168.65.254:9223",
        ]
    )

    assert result["failed"] == ["2026-07-01"]
    assert not state_path.exists()


def test_main_range_uploads_comments_in_order(tmp_path, monkeypatch):
    generated = []
    calls = []
    monkeypatch.setattr(susam_report, "STATE_PATH", tmp_path / "state.json")
    _stub_generation(monkeypatch, tmp_path, generated)

    def upload_batch(reports, *_config):
        calls.extend(reports)
        return [date_str for _text, date_str in reports], []

    monkeypatch.setattr(susam_report, "upload_many_to_flow", upload_batch)
    result = susam_report.main(
        [
            "--start", "2026-07-01", "--end", "2026-07-03", "--force",
            "--post-url", "https://flow.team/main.act?detail=test",
            "--debugger-address", "192.168.65.254:9223",
        ]
    )

    assert generated == ["2026-07-01", "2026-07-02", "2026-07-03"]
    assert [date_str for _text, date_str in calls] == generated
    assert result["uploaded"] == generated


def test_set_comment_text_uses_browser_clipboard_and_ctrl_v(monkeypatch):
    calls = []

    class Element:
        def send_keys(self, *keys):
            calls.append(("keys", keys))

    class Driver:
        def execute_script(self, *_args):
            calls.append(("focus",))

    monkeypatch.setattr(
        susam_report,
        "_copy_to_browser_clipboard",
        lambda _driver, value: calls.append(("copy", value)),
    )
    monkeypatch.setattr(susam_report, "_comment_input_text", lambda *_args: "긴\n본문")
    susam_report._set_comment_text(Driver(), Element(), "긴\n본문")

    assert ("copy", "긴\n본문") in calls
    assert calls[-1][0] == "keys"
    assert "v" in calls[-1][1]


def test_set_comment_text_falls_back_to_dom_events(monkeypatch):
    assigned = []

    class Element:
        def send_keys(self, *_keys):
            pass

    class Driver:
        def execute_script(self, *_args):
            pass

    monkeypatch.setattr(
        susam_report,
        "_set_comment_text_with_events",
        lambda _driver, _element, value: assigned.append(value),
    )
    monkeypatch.setattr(
        susam_report,
        "_copy_to_browser_clipboard",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("clipboard unavailable")),
    )
    monkeypatch.setattr(susam_report, "_comment_input_text", lambda *_args: "")

    susam_report._set_comment_text(Driver(), Element(), "전체\n본문")

    assert assigned == ["", "전체\n본문"]


def test_wait_for_comment_text_handles_delayed_multiline_paste(monkeypatch):
    values = iter(["일부", "전체\n본문"])
    monkeypatch.setattr(susam_report, "_comment_input_text", lambda *_args: next(values))

    class Wait:
        def __init__(self, driver, timeout):
            self.driver = driver
            self.timeout = timeout

        def until(self, predicate):
            assert not predicate(self.driver)
            assert predicate(self.driver)

    monkeypatch.setattr("selenium.webdriver.support.ui.WebDriverWait", Wait)
    susam_report._wait_for_comment_text(object(), object(), "전체\n본문", timeout=30)


def test_upload_waits_for_full_paste_before_enter(monkeypatch):
    order = []

    class Driver:
        current_url = "https://flow.team/main.act?detail=test"

        def set_window_size(self, *_args):
            pass

        def get(self, _url):
            pass

    element = object()
    monkeypatch.setattr(susam_report, "_find_comment_input", lambda *_args: element)
    monkeypatch.setattr(susam_report, "_matching_comment_count", lambda *_args: 0 if "enter" not in order else 1)
    monkeypatch.setattr(susam_report, "_set_comment_text", lambda *_args: order.append("paste"))
    monkeypatch.setattr(susam_report, "_wait_for_comment_text", lambda *_args, **_kwargs: order.append("wait"))
    monkeypatch.setattr(susam_report, "_submit_comment_with_enter", lambda *_args: order.append("enter"))

    susam_report._upload_with_driver(Driver(), "2026-07-01 | 수삼", "2026-07-01", "https://flow.team/test")

    assert order == ["paste", "wait", "enter"]


def test_upload_falls_back_to_dom_enter_when_regular_enter_stalls(monkeypatch):
    submitted = []

    class Driver:
        current_url = "https://flow.team/main.act?detail=test"

        def set_window_size(self, *_args):
            pass

        def get(self, _url):
            pass

    element = object()
    monkeypatch.setattr(susam_report, "_find_comment_input", lambda *_args: element)
    monkeypatch.setattr(susam_report, "_set_comment_text", lambda *_args: None)
    monkeypatch.setattr(susam_report, "_wait_for_comment_text", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        susam_report,
        "_submit_comment_with_enter",
        lambda *_args: submitted.append("regular"),
    )
    monkeypatch.setattr(susam_report, "_new_comment_appeared", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(susam_report, "_comment_input_text", lambda *_args: "본문")
    monkeypatch.setattr(
        susam_report,
        "_submit_comment_with_dom_enter",
        lambda *_args: submitted.append("dom"),
    )
    monkeypatch.setattr(
        susam_report,
        "_matching_comment_count",
        lambda *_args: 1 if "dom" in submitted else 0,
    )

    susam_report._upload_with_driver(
        Driver(), "2026-07-21 | 수삼", "2026-07-21", "https://flow.team/test"
    )

    assert submitted == ["regular", "dom"]


def test_upload_timeout_never_submits(monkeypatch):
    submitted = []

    class Driver:
        current_url = "https://flow.team/main.act?detail=test"

        def set_window_size(self, *_args):
            pass

        def get(self, _url):
            pass

    monkeypatch.setattr(susam_report, "_find_comment_input", lambda *_args: object())
    monkeypatch.setattr(susam_report, "_matching_comment_count", lambda *_args: 0)
    monkeypatch.setattr(susam_report, "_set_comment_text", lambda *_args: None)
    monkeypatch.setattr(
        susam_report,
        "_wait_for_comment_text",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("paste timeout")),
    )
    monkeypatch.setattr(susam_report, "_submit_comment_with_enter", lambda *_args: submitted.append(True))

    with pytest.raises(TimeoutError, match="paste timeout"):
        susam_report._upload_with_driver(
            Driver(), "2026-07-01 | 수삼", "2026-07-01", "https://flow.team/test"
        )
    assert submitted == []
