"""카카오톡 가맹점 대화방 <-> conversation_id/store 매핑 (single source of truth)."""

from __future__ import annotations

# 카톡방명(원본 TXT 파일명에서 추출한 원문) -> 고정 영문 conversation_id.
# 같은 카톡방이면 파일명이 바뀌어도 항상 같은 값을 반환해야 하므로 자동 생성하지 않는다.
# 새 카톡방이 들어오면 이 딕셔너리에만 추가한다.
CONVERSATION_MAP: dict[str, str] = {
    "부산장림점": "CHAT_BUSAN_JANGRIM",
    "하늘도시점": "CHAT_HANEUL_CITY",
    "조민준점": "CHAT_JOMINJUN",
    "부천옥길, 광명철산점": "CHAT_BUCHEON_OKGIL_AND_GWANGMYEONG_CHEOLSAN",
    "김포풍무점": "CHAT_GIMPO_PUNGMU",
    "강원영월점": "CHAT_GANGWON_YEONWOL",
    "경북상주점": "CHAT_GYEONGBUK_SANGJU",
    "광주경안점": "CHAT_GWANGJU_GYEONGAN",
    "광주초월점": "CHAT_GWANGJU_CHOWOL",
    "광주태전점": "CHAT_GWANGJU_TAEJEON",
    "구로디지털점": "CHAT_GURO_DIGITAL",
    "구리다산점": "CHAT_GURI_DASAN",
    "기흥테라타워점": "CHAT_GIHEUNG_TERA_TOWER",
    "김포장기점": "CHAT_GIMPO_JANGGI",
    "김해구산점": "CHAT_GIMHAE_GUSAN",
    "남양주 진접점": "CHAT_NAMYANGJU_JINJEOP",
    "노원점": "CHAT_NOWON",
    "대전관저점": "CHAT_DAEJEON_GWANJEO",
    "대전관평점": "CHAT_DAEJEON_GWANPYEONG",
    "대전둔산점": "CHAT_DAEJEON_DUNSAN",
    "대전용전점": "CHAT_DAEJEON_YONGJEON",
    "대전장대점": "CHAT_DAEJEON_JANGDAE",
    "대전중구점": "CHAT_DAEJEON_JUNGGU",
    "대화점": "CHAT_DAEHWA",
    "동두천지행점": "CHAT_DONGDUCHEON_JIHAENG",
    "동인천점": "CHAT_DONGINCHEON",
    "동탄영천점": "CHAT_DONGTAN_YEONGCHEON",
    "미사점": "CHAT_MISA",
    "백석점": "CHAT_BAEKSEOK",
    "법흥리점": "CHAT_BEOPHEUNGNI",
    "부산광안점, 재송센텀점": "CHAT_BUSAN_GWANGAN_AND_JAESONG_CENTUM",
    "부산대신점": "CHAT_BUSAN_DAESIN",
    "부산서면점": "CHAT_BUSAN_SEOMYEON",
    "삼송점": "CHAT_SAMSONG",
    "서울대점, 상도점": "CHAT_SEOULDAE_AND_SANGDO",
    "성신여대점": "CHAT_SEONGSHIN_YEODAE",
    "세종나성점": "CHAT_SEJONG_NASEONG",
    "송파점": "CHAT_SONGPA",
    "수원화서점": "CHAT_SUWON_HWASEO",
    "수유점": "CHAT_SUYU",
    "시흥배곧점": "CHAT_SIHEUNG_BAEGOT",
    "시흥장현점": "CHAT_SIHEUNG_JANGHYEON",
    "양주옥정점": "CHAT_YANGJU_OKJEONG",
    "오산시청점": "CHAT_OSAN_SICHEONG",
    "용인동천점": "CHAT_YONGIN_DONGCHEON",
    "용현점": "CHAT_YONGHYEON",
    "응암점": "CHAT_EUNGAM",
    "의정부점": "CHAT_UIJEONGBU",
    "익산영등점": "CHAT_IKSAN_YEONGDEUNG",
    "인천간석중앙점": "CHAT_INCHEON_GANSEOK_JUNGANG",
    "인천청라점": "CHAT_INCHEON_CHEONGNA",
    "전주전북대점": "CHAT_JEONJU_JEONBUKDAE",
    "중랑면목점": "CHAT_JUNGNANG_MYEONMOK",
    "창원내서점": "CHAT_CHANGWON_NAESEO",
    "천안성정점": "CHAT_CHEONAN_SEONGJEONG",
    "평택비전점": "CHAT_PYEONGTAEK_BIJEON",
    "평택서정점": "CHAT_PYEONGTAEK_SEOJEONG",
    "해운대중동점": "CHAT_HAEUNDAE_JUNGDONG",
    "행신점": "CHAT_HAENGSIN",
    "화성봉담점": "CHAT_HWASEONG_BONGDAM",
}

# 점주 1명이 여러 매장을 같이 운영해 카톡방 이름에 매장이 2개 이상 들어가는 경우의 분리 규칙.
# 여기 없는 카톡방은 카톡방명 그대로 단일 매장(store)으로 취급한다
# (예: "남양주 진접점"의 공백은 구분자가 아니라 매장명의 일부).
MULTI_STORE_MAP: dict[str, list[str]] = {
    "부천옥길, 광명철산점": ["부천옥길점", "광명철산점"],
    "부산광안점, 재송센텀점": ["부산광안점", "재송센텀점"],
    "서울대점, 상도점": ["서울대점", "상도점"],
}


# sender가 이 목록에 "정확히" 일치하거나("EXACT") 이 목록의 단어를 "포함"하면("CONTAINS")
# type="본사", 그 외는 type="점주". 새 패턴이 생기면 이 두 리스트만 수정하면 된다.
HQ_SENDER_EXACT: list[str] = ["도리당"]
HQ_SENDER_CONTAINS: list[str] = ["직원", "이병두", "김덕기", "김대진"]


class KakaoUnmappedRoomError(Exception):
    """CONVERSATION_MAP에 없는 카톡방을 만났을 때 발생.

    잘못된 conversation_id를 자동 생성하는 대신 사람이 CONVERSATION_MAP을
    갱신하도록 유도하기 위한 하드 실패용 예외.
    """


class KakaoSourceReadError(Exception):
    """TXT 원본 파일을 읽지 못했을 때(OneDrive 미다운로드 등) 발생.

    실패한 파일은 이동/state 기록 없이 다음 실행에서 재시도되므로 데이터
    유실은 없지만, 조용히 넘어가면 수집 누락을 못 알아채므로 태스크를
    실패시켜 retry/알림이 동작하도록 한다.
    """


def resolve_conversation_id(room_key: str) -> str:
    conversation_id = CONVERSATION_MAP.get(room_key)
    if not conversation_id:
        raise KakaoUnmappedRoomError(f"미등록 카톡방: {room_key!r} - CONVERSATION_MAP에 추가 필요")
    return conversation_id


def resolve_stores(room_key: str) -> list[str]:
    return MULTI_STORE_MAP.get(room_key, [room_key])


def classify_sender_type(sender: str | None) -> str | None:
    if not sender:
        return None
    if sender in HQ_SENDER_EXACT:
        return "본사"
    if any(keyword in sender for keyword in HQ_SENDER_CONTAINS):
        return "본사"
    return "점주"
