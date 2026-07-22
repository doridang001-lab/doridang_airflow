import pandas as pd

from modules.transform.utility import account


def test_get_pw_falls_back_to_legacy_account_when_automation_account_missing(monkeypatch):
    monkeypatch.setattr(account, "load_automation_account_df", lambda **_kwargs: pd.DataFrame())

    assert account.get_pw("toorder", "doridang15") == "ehfl1819!"


def test_get_default_account_falls_back_to_legacy_account_when_automation_account_missing(monkeypatch):
    monkeypatch.setattr(account, "load_automation_account_df", lambda **_kwargs: pd.DataFrame())

    assert account.get_default_account("toorder") == ("doridang15", "ehfl1819!")
