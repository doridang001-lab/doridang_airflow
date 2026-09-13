"""카카오톡 TXT 누적 스냅샷 파싱.

부작용 없는 순수 함수만 둔다 (파일 이동/parquet 적재는 DB_KakaoStoreRaw_load.py 담당).
원문(맞춤법/이모티콘/줄바꿈)은 절대 수정하지 않는다.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path

from modules.transform.pipelines.db.DB_KakaoStoreRaw_config import (
    classify_sender_type,
    resolve_conversation_id,
    resolve_stores,
)

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "message_id",
    "conversation_id",
    "store",
    "chat_room_name",
    "message_date",
    "message_time",
    "message_datetime",
    "sender",
    "type",
    "message_type",
    "message",
    "attachment_count",
    "collected_at",
    "source_file",
]

# 파일명 형식이 두 가지 확인됨:
# 1) 자동 수집 파이프라인 실 운영 형식: "{카톡방명}_YYYYMMDD_HHMMSS_고유값.txt"
# 2) 카카오톡 데스크톱 클라이언트의 수동 내보내기 원본 형식: "{카톡방명}_KakaoTalk_..._group.txt"
# 뒷부분 숫자 구조는 형식마다 다르고 저장 시각과도 어긋날 수 있어(초 단위 불일치 확인됨)
# 카톡방명만 안전하게 뽑아 쓰고 수집 시각은 본문의 "저장한 날짜" 줄에서 읽는다.
_KAKAO_FILENAME_TOKEN = "_KakaoTalk_"
_LEGACY_FILENAME_RE = re.compile(
    r"^(?P<room>.+)_(?P<date>\d{8})_(?P<time>\d{6})_(?P<uniq>\w+)\.txt$",
    re.IGNORECASE,
)
_SAVED_AT_RE = re.compile(
    r"저장한\s*날짜\s*:\s*(?P<dt>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"
)
_FALLBACK_DATE_RE = re.compile(r"(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})")
# 카카오톡 PC 내보내기 헤더는 보통 "{방이름} 님과 카카오톡 대화" 또는 "{방이름} 카카오톡 대화" 형태다.
_HEADER_ROOM_RE = re.compile(r"^(?P<room>.+?)\s*(?:님과)?\s*카카오톡\s*대화\s*$")
_DATE_SEP_RE = re.compile(
    r"^-+\s*(?P<y>\d{4})년\s*(?P<m>\d{1,2})월\s*(?P<d>\d{1,2})일\s*\S*요일\s*-+$"
)
# sender는 "[도리당]부산장림점 운영자 비성"처럼 자체적으로 대괄호를 포함할 수 있어
# 바깥쪽 대괄호(줄 구문)와 안쪽 대괄호(표시명 일부)를 구분해야 한다.
# sender를 탐욕적으로 매칭해도 뒤따르는 "[오전/오후 H:MM]" 패턴은 줄당 하나뿐이라 정확히 역추적된다.
_MESSAGE_START_RE = re.compile(
    r"^\[(?P<sender>.+)\]\s\[(?P<ampm>오전|오후)\s(?P<hour>\d{1,2}):(?P<minute>\d{2})\]\s?(?P<rest>.*)$"
)
_IMAGE_RE = re.compile(r"^사진(?:\s*(?P<count>\d+)\s*장)?$")
_DELETED_TEXT = "메시지가 삭제되었습니다."
_SYSTEM_PATTERNS = [
    re.compile(r".+님을 초대했습니다\.$"),
    re.compile(r".+님이 들어왔습니다\.$"),
    re.compile(r".+님이 나갔습니다\.$"),
    re.compile(r".*방장이 변경되었습니다\.$"),
    re.compile(r"^팀 이름이 변경되었습니다\.$"),
    re.compile(r".*채팅방 이름을.*변경하였습니다\.$"),
    re.compile(r".+님이 부방장이 되었습니다\.$"),
    re.compile(r".+님이 부방장에서 해제되었습니다\.$"),
]


class KakaoFilenameParseError(Exception):
    """파일명에서 카톡방명/수집시각을 추출하지 못했을 때 발생."""


@dataclass
class ParsedMessage:
    message_date: date
    message_time: time
    sender: str | None
    message_type: str
    message: str
    attachment_count: int

    @property
    def message_datetime(self) -> datetime:
        return datetime.combine(self.message_date, self.message_time)


def extract_room_key(filename: str) -> str:
    """파일명에서 카톡방명(CONVERSATION_MAP 키와 동일)을 추출한다. 두 형식 모두 지원."""
    if _KAKAO_FILENAME_TOKEN in filename:
        room_key, _, _ = filename.partition(_KAKAO_FILENAME_TOKEN)
        if room_key:
            return room_key

    legacy_match = _LEGACY_FILENAME_RE.match(filename)
    if legacy_match:
        return legacy_match.group("room")

    raise KakaoFilenameParseError(f"파일명 형식을 인식하지 못함: {filename}")


def extract_collected_at(text: str, filename: str) -> datetime:
    """본문의 "저장한 날짜 : YYYY-MM-DD HH:MM:SS" 줄에서 수집 시각을 읽는다.

    파일명 뒷부분 숫자는 저장 시각과 어긋나는 경우가 확인되어 신뢰하지 않는다.
    본문에서 못 찾으면 파일명의 YYYYMMDD로 대체(00:00:00)하고 경고를 남긴다.
    """
    saved_match = _SAVED_AT_RE.search(text)
    if saved_match:
        return datetime.strptime(saved_match["dt"], "%Y-%m-%d %H:%M:%S")

    fallback = _FALLBACK_DATE_RE.search(filename)
    if fallback:
        logger.warning(
            "본문에 '저장한 날짜' 줄이 없어 파일명 날짜로 대체(00:00:00): %s", filename
        )
        return datetime(int(fallback["y"]), int(fallback["m"]), int(fallback["d"]))

    raise KakaoFilenameParseError(f"수집 시각을 추출하지 못함: {filename}")


def _to_24h(ampm: str, hour: int) -> int:
    hour = hour % 12
    if ampm == "오후":
        hour += 12
    return hour


def _match_system(line: str) -> bool:
    return any(pattern.match(line) for pattern in _SYSTEM_PATTERNS)


def _classify_content(rest: str) -> tuple[str, int]:
    image_match = _IMAGE_RE.match(rest.strip())
    if image_match:
        count = int(image_match.group("count")) if image_match.group("count") else 1
        return "image", count
    if rest.strip() == _DELETED_TEXT:
        return "deleted", 0
    return "text", 0


def extract_chat_room_name(text: str, fallback: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        header_match = _HEADER_ROOM_RE.match(stripped)
        return header_match.group("room") if header_match else fallback
    return fallback


def parse_kakao_body(text: str, *, source_file: str) -> list[ParsedMessage]:
    """카카오톡 TXT 본문을 메시지 단위로 파싱한다.

    여러 줄에 걸친 메시지는 새 "[발신자] [시간]" 패턴이나 날짜 구분선이
    나오기 전까지 직전 메시지에 개행으로 이어붙여 하나로 병합한다.
    """
    current_date: date | None = None
    messages: list[ParsedMessage] = []
    open_message: ParsedMessage | None = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip("\r\n")
        stripped = line.strip()
        if not stripped:
            # 열린 메시지 중간의 빈 줄은 문단 구분이므로 버리지 않고 이어붙인다.
            if open_message is not None:
                open_message.message += "\n"
            continue

        date_match = _DATE_SEP_RE.match(stripped)
        if date_match:
            current_date = date(
                int(date_match["y"]), int(date_match["m"]), int(date_match["d"])
            )
            open_message = None
            continue

        start_match = _MESSAGE_START_RE.match(line)
        if start_match:
            if current_date is None:
                logger.warning(
                    "날짜 구분선 없이 메시지 발견, 건너뜀: %s / %s", source_file, line[:60]
                )
                open_message = None
                continue
            hour = _to_24h(start_match["ampm"], int(start_match["hour"]))
            msg_time = time(hour, int(start_match["minute"]))
            rest = start_match["rest"] or ""
            message_type, attachment_count = _classify_content(rest)
            open_message = ParsedMessage(
                message_date=current_date,
                message_time=msg_time,
                sender=start_match["sender"],
                message_type=message_type,
                message=rest,
                attachment_count=attachment_count,
            )
            messages.append(open_message)
            continue

        if _match_system(stripped):
            if current_date is None:
                logger.warning(
                    "날짜 구분선 없이 시스템 메시지 발견, 건너뜀: %s / %s",
                    source_file,
                    line[:60],
                )
                open_message = None
                continue
            # 카카오 내보내기의 시스템 알림 줄에는 시간 표시가 없어 자정(00:00:00)으로 둔다.
            open_message = ParsedMessage(
                message_date=current_date,
                message_time=time(0, 0),
                sender=None,
                message_type="system",
                message=stripped,
                attachment_count=0,
            )
            messages.append(open_message)
            continue

        if stripped == _DELETED_TEXT:
            if current_date is None:
                logger.warning(
                    "날짜 구분선 없이 삭제 메시지 발견, 건너뜀: %s / %s",
                    source_file,
                    line[:60],
                )
                open_message = None
                continue
            # 삭제된 메시지도 접두사 없는 단독 줄로 나올 수 있다(시스템 알림과 동일한 형태).
            open_message = ParsedMessage(
                message_date=current_date,
                message_time=time(0, 0),
                sender=None,
                message_type="deleted",
                message=stripped,
                attachment_count=0,
            )
            messages.append(open_message)
            continue

        if open_message is not None:
            open_message.message = (
                f"{open_message.message}\n{line}" if open_message.message else line
            )
            continue

        if current_date is not None:
            logger.warning(
                "메시지 시작 패턴이 아닌 줄, 건너뜀: %s / %s", source_file, line[:60]
            )
        # current_date가 아직 없으면 "{방이름} 님과 카카오톡 대화" / "저장한 날짜 : ..."
        # 같은 파일 상단 헤더 줄이므로 경고 없이 조용히 건너뛴다.

    return messages


def make_message_id(
    conversation_id: str, message_datetime: datetime, message: str, occurrence_index: int
) -> str:
    """conversation_id/message_datetime/message/occurrence_index로만 결정되는 해시.

    sender는 의도적으로 뺐다 — 카카오톡 내보내기는 발신자의 "현재" 닉네임을
    그 사람의 모든 과거 메시지에 소급 적용해서 보여주므로, sender를 해시에
    포함하면 닉네임이 바뀌는 순간 같은 메시지가 다른 message_id를 얻어
    중복 적재된다. occurrence_index는 같은 파일 안에서 동일
    (message_datetime, message) 조합이 몇 번째로 등장했는지이며, 누적
    스냅샷은 항상 처음부터 전체 히스토리를 포함하므로 스냅샷이 바뀌어도
    항상 동일하게 재현된다.
    """
    payload = "|".join(
        [
            conversation_id,
            message_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            message,
            str(occurrence_index),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_rows_for_file(path: Path, text: str) -> list[dict]:
    """TXT 한 개를 최종 마트 스키마의 행(dict) 리스트로 변환한다.

    복수 매장 카톡방은 동일 message_id로 매장 수만큼 행을 복제한다.
    """
    room_key = extract_room_key(path.name)
    conversation_id = resolve_conversation_id(room_key)
    collected_at = extract_collected_at(text, path.name)
    chat_room_name = extract_chat_room_name(text, fallback=room_key)
    stores = resolve_stores(room_key)
    messages = parse_kakao_body(text, source_file=path.name)

    occurrence_counts: dict[tuple[datetime, str], int] = {}
    rows: list[dict] = []
    for msg in messages:
        dedup_key = (msg.message_datetime, msg.message)
        occurrence_counts[dedup_key] = occurrence_counts.get(dedup_key, 0) + 1
        message_id = make_message_id(
            conversation_id, msg.message_datetime, msg.message, occurrence_counts[dedup_key]
        )
        sender_type = classify_sender_type(msg.sender)
        for store in stores:
            rows.append(
                {
                    "message_id": message_id,
                    "conversation_id": conversation_id,
                    "store": store,
                    "chat_room_name": chat_room_name,
                    "message_date": msg.message_date,
                    "message_time": msg.message_time,
                    "message_datetime": msg.message_datetime,
                    "sender": msg.sender,
                    "type": sender_type,
                    "message_type": msg.message_type,
                    "message": msg.message,
                    "attachment_count": msg.attachment_count,
                    "collected_at": collected_at,
                    "source_file": path.name,
                }
            )
    return rows
