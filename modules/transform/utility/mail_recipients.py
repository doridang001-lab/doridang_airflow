"""메일 수신자 정리 유틸."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
import re


_EMPTY_MAIL_VALUES = {"", "none", "nan", "null", "nat", "-"}

# ============================================================
# 메일 수신자 변수
# - 퇴사/연락 제외자는 값을 None으로 변경하면 전체 발송에서 자동 제외된다.
# ============================================================

MAIL_CMJ_PM = "a17019@doridang.com"  # 조민준 PM
MAIL_CEO = "siw22222@kakao.com"  # 대표님
MAIL_OH_NAYOUNG = "bulu1017@kakao.com"  # 오나영 차장
MAIL_KIM_DAEJIN = "sanbogaja81@kakao.com"  # 김대진 팀장
MAIL_KIM_DEOKGI = "kdk1402@kakao.com"  # 김덕기
MAIL_SIM_SUNGJUN = "simjeong01@kakao.com"  # 심성준
MAIL_SIM_SUNGJUN_1 = "simjeong01@kakao.com"  # 심성준 이사
MAIL_SIM_SUNGJUN_2 = "simjeong00@kakao.com"  # 심성준 이사
MAIL_LEE_BYOUNGDOO = "byoungd201@kakao.com"  # 이병두
MAIL_HHWANG_DAESUNG = None  # 황대성
MAIL_POLICY_REVIEWER = None  # 정책 리뷰 수신자

MANAGER_MAILS = {
    "김덕기": MAIL_KIM_DEOKGI,
    "김대진": MAIL_KIM_DAEJIN,
    "심성준": MAIL_SIM_SUNGJUN,
    "이병두": MAIL_LEE_BYOUNGDOO,
    "조민준": MAIL_CMJ_PM,
    "황대성": MAIL_HHWANG_DAESUNG,
}


def normalize_manager_name(value: Any) -> str:
    """직급/공백이 붙은 담당자 값에서 이름만 추출한다."""
    if value is None:
        return ""
    name = str(value).strip()
    if not name or name.lower() in _EMPTY_MAIL_VALUES:
        return ""
    match = re.match(r"([가-힣]{2,4})", name)
    return match.group(1) if match else name


def resolve_manager_mail(manager_name: Any, fallback_email: Any = None) -> str | None:
    """담당자명이 중앙 메일 변수에 있으면 그 값을 우선 사용한다."""
    normalized = normalize_manager_name(manager_name)
    if normalized in MANAGER_MAILS:
        resolved = resolve_mail_recipients(MANAGER_MAILS[normalized])
        return resolved[0] if resolved else None
    resolved = resolve_mail_recipients(fallback_email)
    return resolved[0] if resolved else None


def apply_manager_mail_variables(df: Any, manager_col: str = "담당자", email_col: str = "email") -> Any:
    """DataFrame의 email 컬럼을 중앙 담당자 메일 변수 기준으로 보정한다."""
    if df is None or manager_col not in getattr(df, "columns", []):
        return df
    if email_col not in df.columns:
        df[email_col] = ""

    def resolve_row(row: Any) -> str:
        resolved = resolve_manager_mail(row.get(manager_col), row.get(email_col))
        return resolved or ""

    df[email_col] = df.apply(resolve_row, axis=1)
    return df


def resolve_mail_recipients(*items: Any) -> list[str]:
    """메일 수신자 입력에서 None/빈값/중복을 제거한다."""
    recipients: list[str] = []
    seen: set[str] = set()

    def add(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, str):
            mail = value.strip()
            if mail.lower() in _EMPTY_MAIL_VALUES:
                return
            key = mail.lower()
            if key not in seen:
                seen.add(key)
                recipients.append(mail)
            return
        if isinstance(value, Iterable):
            for child in value:
                add(child)
            return
        mail = str(value).strip()
        if mail.lower() in _EMPTY_MAIL_VALUES:
            return
        key = mail.lower()
        if key not in seen:
            seen.add(key)
            recipients.append(mail)

    for item in items:
        add(item)
    return recipients
