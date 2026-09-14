import pandas as pd

from modules.transform.utility.mail_recipients import (
    apply_manager_mail_variables,
    resolve_mail_recipients,
    resolve_manager_mail,
)


def test_resolve_mail_recipients_filters_empty_and_deduplicates():
    assert resolve_mail_recipients(
        "a17019@doridang.com",
        None,
        "",
        " none ",
        ["A17019@doridang.com", "bulu1017@kakao.com"],
    ) == ["a17019@doridang.com", "bulu1017@kakao.com"]


def test_resolve_mail_recipients_returns_empty_when_all_disabled():
    assert resolve_mail_recipients(None, "", ["nan", None, "-"]) == []


def test_resolve_manager_mail_none_overrides_fallback_email():
    assert resolve_manager_mail("황대성", "gjddkemf@kakao.com") is None


def test_apply_manager_mail_variables_overwrites_employee_csv_email():
    df = pd.DataFrame(
        [
            {"담당자": "황대성", "email": "gjddkemf@kakao.com"},
            {"담당자": "미등록", "email": "fallback@example.com"},
        ]
    )

    result = apply_manager_mail_variables(df)

    assert result.loc[0, "email"] == ""
    assert result.loc[1, "email"] == "fallback@example.com"
