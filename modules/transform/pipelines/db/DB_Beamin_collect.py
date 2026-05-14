"""배민 공통 유틸: 계정 로드"""

import logging

import pandas as pd

from modules.transform.utility.paths import ONEDRIVE_DB

logger = logging.getLogger(__name__)

CSV_PATH = ONEDRIVE_DB / "sales_employee.csv"


def load_accounts(target_stores: list[str], exact: bool = False) -> list[dict]:
    """sales_employee.csv → 배달의 민족 계정 목록.

    반환: [{"account_id": str, "password": str, "store_name": str}, ...]
    target_stores가 비어 있으면 전 매장 반환.
    exact=True 이면 매장명 완전 일치(isin), False 이면 부분 문자열 매칭.
    """
    df = pd.read_csv(CSV_PATH)
    df = df[df["플랫폼"] == "배달의 민족"].copy()

    if target_stores:
        if exact:
            df = df[df["매장명"].isin(target_stores)]
        else:
            pattern = "|".join(target_stores)
            df = df[df["매장명"].str.contains(pattern, na=False)]

    accounts = [
        {
            "account_id": str(r["계정ID"]),
            "password":   str(r["계정PW"]),
            "store_name": str(r["매장명"]),
        }
        for _, r in df.iterrows()
    ]
    logger.info("계정 로드: %d개 %s", len(accounts), [a["store_name"] for a in accounts])
    return accounts
