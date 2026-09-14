import pandas as pd

from modules.transform.utility import account


SALES_EMPLOYEE_COLUMNS = [
    "오픈순서",
    "호점",
    "매장명",
    "사업자번호",
    "점주명",
    "담당자",
    "실오픈일",
    "상세주소",
    "광역",
    "시군구",
    "읍면동",
    "email",
    "플랫폼",
    "계정ID",
    "계정PW",
    "collected_at",
]


def _sales_employee_row(platform: str, store: str, account_id: str, password: str) -> dict[str, str]:
    row = {column: "" for column in SALES_EMPLOYEE_COLUMNS}
    row.update(
        {
            "매장명": store,
            "플랫폼": platform,
            "계정ID": account_id,
            "계정PW": password,
            "collected_at": "2026-07-30",
        }
    )
    return row


def _write_sales_employee_csv(tmp_path, rows: list[dict[str, str]], *, include_note: bool = False):
    columns = SALES_EMPLOYEE_COLUMNS + (["비고"] if include_note else [])
    csv_path = tmp_path / "sales_employee.csv"
    pd.DataFrame(rows, columns=columns).to_csv(csv_path, index=False, encoding="utf-8-sig")
    return csv_path


def test_get_pw_falls_back_to_legacy_account_when_automation_account_missing(monkeypatch):
    monkeypatch.setattr(account, "load_automation_account_df", lambda **_kwargs: pd.DataFrame())

    assert account.get_pw("toorder", "doridang15") == "ehfl1819!"


def test_get_default_account_falls_back_to_legacy_account_when_automation_account_missing(monkeypatch):
    monkeypatch.setattr(account, "load_automation_account_df", lambda **_kwargs: pd.DataFrame())

    assert account.get_default_account("toorder") == ("doridang15", "ehfl1819!")


def test_get_default_account_ignores_store_rows_when_note_column_missing(monkeypatch, tmp_path):
    rows = [
        _sales_employee_row("토더", "도리당 1호점", "doridang100001", "abcd9142"),
        _sales_employee_row("토더", "도리당 2호점", "doridang100002", "abcd9143"),
        _sales_employee_row("토더", "도리당 3호점", "doridang100003", "abcd9144"),
    ]
    csv_path = _write_sales_employee_csv(tmp_path, rows)
    monkeypatch.setattr(account, "_default_sales_employee_csv_path", lambda: csv_path)

    assert account.get_default_account("toorder") == ("doridang15", "ehfl1819!")


def test_load_automation_account_df_still_returns_store_rows_without_note_column(monkeypatch, tmp_path):
    rows = [
        _sales_employee_row("배달의 민족", "도리당 1호점", "baemin001", "pw1"),
        _sales_employee_row("배달의 민족", "도리당 2호점", "baemin002", "pw2"),
        _sales_employee_row("배달의 민족", "도리당 3호점", "baemin003", "pw3"),
    ]
    csv_path = _write_sales_employee_csv(tmp_path, rows)
    monkeypatch.setattr(account, "_default_sales_employee_csv_path", lambda: csv_path)

    df = account.load_automation_account_df(platform="배달의 민족")

    assert len(df) == 3
    assert df["계정ID"].tolist() == ["baemin001", "baemin002", "baemin003"]


def test_get_default_account_uses_note_rows_when_column_present(monkeypatch, tmp_path):
    note_row = _sales_employee_row("토더", "대표", "doridang15", "rotated-toorder-pw")
    note_row["비고"] = "자동화 연결"
    store_row = _sales_employee_row("토더", "도리당 1호점", "doridang100001", "abcd9142")
    store_row["비고"] = ""
    csv_path = _write_sales_employee_csv(tmp_path, [note_row, store_row], include_note=True)
    monkeypatch.setattr(account, "_default_sales_employee_csv_path", lambda: csv_path)

    assert account.get_default_account("toorder") == ("doridang15", "rotated-toorder-pw")


def test_get_default_account_prefers_legacy_id_among_note_rows(monkeypatch, tmp_path):
    other_row = _sales_employee_row("토더", "다른 대표", "doridang-company-other", "other-pw")
    other_row["비고"] = "자동화 연결"
    legacy_row = _sales_employee_row("토더", "대표", "doridang15", "preferred-pw")
    legacy_row["비고"] = "자동화 연결"
    csv_path = _write_sales_employee_csv(tmp_path, [other_row, legacy_row], include_note=True)
    monkeypatch.setattr(account, "_default_sales_employee_csv_path", lambda: csv_path)

    assert account.get_default_account("toorder") == ("doridang15", "preferred-pw")


def test_get_pw_prefers_csv_row_for_store_account(monkeypatch, tmp_path):
    rows = [
        _sales_employee_row("토더", "도리당 1호점", "doridang100001", "abcd9142"),
        _sales_employee_row("토더", "도리당 2호점", "doridang100002", "abcd9143"),
    ]
    csv_path = _write_sales_employee_csv(tmp_path, rows)
    monkeypatch.setattr(account, "_default_sales_employee_csv_path", lambda: csv_path)

    assert account.get_pw("toorder", "doridang100001") == "abcd9142"
