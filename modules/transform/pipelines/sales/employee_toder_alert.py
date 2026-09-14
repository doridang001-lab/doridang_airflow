"""영업관리 구글시트의 토더 계정 누락 알림 판정."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable

import pandas as pd

from modules.transform.utility.store_normalize import normalize_for_join


@dataclass(frozen=True)
class ToderAlertResult:
    """토더 누락 판정 결과와 운영 로그용 집계."""

    message: str
    total_store_count: int = 0
    alertable_store_count: int = 0
    complete_store_count: int = 0
    missing_store_count: int = 0
    duplicate_store_count: int = 0
    skipped_reason: str = ""


def _is_present(value: object) -> bool:
    if pd.isna(value):
        return False
    return str(value).strip() not in {"", "nan", "None"}


def _column_lookup(columns) -> dict[str, object]:
    return {
        re.sub(r"\s+", "", str(column)): column
        for column in columns
    }


def _find_column(df: pd.DataFrame, column: str):
    if column in df.columns:
        return column
    return _column_lookup(df.columns).get(re.sub(r"\s+", "", column))


def _value_from_rows(rows: list[pd.Series], *columns: str) -> str:
    for row in rows:
        lookup = _column_lookup(row.index)
        for column in columns:
            row_column = column if column in row.index else lookup.get(re.sub(r"\s+", "", column))
            if row_column is not None and _is_present(row[row_column]):
                return str(row[row_column]).strip()
    return ""


def _account_from_rows(
    rows: list[pd.Series],
    id_columns: tuple[str, ...],
    pw_columns: tuple[str, ...],
) -> str:
    account_id = _value_from_rows(rows, *id_columns)
    account_pw = _value_from_rows(rows, *pw_columns)
    if not account_id and not account_pw:
        return ""
    return f"{account_id}   // {account_pw}"


def _preferred_rows(group: pd.DataFrame, manager_column) -> list[pd.Series]:
    """최신 담당자 행을 우선하고 나머지 행을 시트 역순으로 반환한다."""
    reversed_rows = [row for _, row in group.iloc[::-1].iterrows()]
    managed_rows = [row for row in reversed_rows if _is_present(row[manager_column])]
    unmanaged_rows = [row for row in reversed_rows if not _is_present(row[manager_column])]
    return managed_rows + unmanaged_rows


def _build_store_block(rows: list[pd.Series]) -> str:
    lines = [
        "[신규 매장 / 양도양수 매장 / 해지 매장]",
        f"- 매장명 : {_value_from_rows(rows, '매장명')}",
        f"- 사업자명의 : {_value_from_rows(rows, '점주명', '사업자명의')}",
        f"- 핸드폰번호 : {_value_from_rows(rows, '전화번호', '전화번호(mobile)', '핸드폰번호', '휴대폰번호', '연락처')}",
        f"- 매장주소 : {_value_from_rows(rows, '상세주소', '매장주소', '주소')}",
        f"- 발주매장코드 : {_value_from_rows(rows, '발주매장코드')}",
        f"- 배민 계정 : {_account_from_rows(rows, ('배민ID', '배달의민족ID', '배달의 민족ID'), ('배민PW', '배달의민족PW', '배달의 민족PW'))}",
        f"- 요기요 계정 : {_account_from_rows(rows, ('요기요ID',), ('요기요PW',))}",
        f"- 쿠팡 계정 : {_account_from_rows(rows, ('쿠팡ID', '쿠팡이츠ID'), ('쿠팡PW', '쿠팡이츠PW'))}",
        f"- 오픈일 : {_value_from_rows(rows, '실오픈일', '오픈일')}",
        "- 프로그램 설치 가능시간 : asap",
    ]
    return "\n".join(lines)


def build_toder_missing_alert(df: pd.DataFrame) -> ToderAlertResult:
    """매장 단위로 토더 ID/PW 누락을 판정해 알림 본문을 만든다.

    동일 매장의 어느 한 행에 ID와 PW가 함께 있으면 정상으로 본다. ID와
    PW가 서로 다른 행에 나뉘어 있으면 유효한 한 쌍이 아니므로 누락이다.
    """
    store_column = _find_column(df, "매장명")
    manager_column = _find_column(df, "담당자")
    toder_id_column = _find_column(df, "토더ID")
    toder_pw_column = _find_column(df, "토더PW")
    missing_columns = [
        name
        for name, column in (
            ("매장명", store_column),
            ("담당자", manager_column),
            ("토더ID", toder_id_column),
            ("토더PW", toder_pw_column),
        )
        if column is None
    ]
    if missing_columns:
        return ToderAlertResult(
            message="",
            skipped_reason=f"필수 컬럼 없음: {', '.join(missing_columns)}",
        )

    working = df[df[store_column].map(_is_present)].copy()
    if working.empty:
        return ToderAlertResult(message="")

    working["_store_key"] = normalize_for_join(working[store_column].astype(str).str.strip())
    working = working[working["_store_key"].map(_is_present)].copy()

    blocks: list[str] = []
    alertable_store_count = 0
    complete_store_count = 0
    duplicate_store_count = 0

    for _, group in working.groupby("_store_key", sort=False):
        if len(group) > 1:
            duplicate_store_count += 1

        has_manager = group[manager_column].map(_is_present).any()
        if not has_manager:
            continue
        alertable_store_count += 1

        complete_rows = group[toder_id_column].map(_is_present) & group[toder_pw_column].map(_is_present)
        if complete_rows.any():
            complete_store_count += 1
            continue

        blocks.append(_build_store_block(_preferred_rows(group, manager_column)))

    return ToderAlertResult(
        message="\n\n".join(blocks),
        total_store_count=working["_store_key"].nunique(),
        alertable_store_count=alertable_store_count,
        complete_store_count=complete_store_count,
        missing_store_count=len(blocks),
        duplicate_store_count=duplicate_store_count,
    )


def dispatch_toder_missing_alert(
    df: pd.DataFrame,
    sender: Callable[[str], object],
) -> ToderAlertResult:
    """누락 본문이 있을 때만 주입된 발송 함수를 한 번 호출한다."""
    result = build_toder_missing_alert(df)
    if result.message:
        sender(result.message)
    return result


__all__ = [
    "ToderAlertResult",
    "build_toder_missing_alert",
    "dispatch_toder_missing_alert",
]
