"""배민 공통 유틸: 계정 로드"""

import logging

from modules.transform.utility.account import load_automation_account_df

logger = logging.getLogger(__name__)


def load_accounts(target_stores: list[str], exact: bool = False) -> list[dict]:
    """sales_employee.csv → 배달의 민족 계정 목록.

    반환: [{"account_id": str, "password": str, "store_name": str}, ...]
    target_stores가 비어 있으면 전 매장 반환.
    exact=True 이면 매장명 완전 일치(isin), False 이면 부분 문자열 매칭.
    """
    df = load_automation_account_df(
        platform="배달의 민족",
        target_stores=target_stores,
        exact=exact,
    )

    accounts = [
        {
            "account_id": str(r["계정ID"]).strip(),
            "password":   str(r["계정PW"]).strip(),
            "store_name": str(r["매장명"]).strip(),
        }
        for _, r in df.iterrows()
        if str(r.get("계정ID", "")).strip() and str(r.get("계정PW", "")).strip()
    ]
    logger.info("계정 로드: %d개 %s", len(accounts), [a["store_name"] for a in accounts])
    return accounts
