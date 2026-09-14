import json
from datetime import date, datetime

import pandas as pd
import pytest

from modules.transform.pipelines.db import DB_KakaoStoreRaw_load as load


_WEEKDAY_KO = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"]


def _date_separator(d: date) -> str:
    weekday = _WEEKDAY_KO[d.weekday()]
    return f"--------------- {d.year}년 {d.month}월 {d.day}일 {weekday} ---------------"


def _message_line(sender: str, hour24: int, minute: int, content: str) -> str:
    ampm = "오전" if hour24 < 12 else "오후"
    hour12 = hour24 % 12
    if hour12 == 0:
        hour12 = 12
    return f"[{sender}] [{ampm} {hour12}:{minute:02d}] {content}"


def _write_room_txt(
    source_dir,
    room: str,
    saved_at: datetime,
    body_lines: list[str],
    unique: str = "450",
) -> object:
    """실제 수집 파일명(`{room}_KakaoTalk_..._group.txt`)과 헤더 형식을 재현한다."""
    source_dir.mkdir(parents=True, exist_ok=True)
    filename = (
        f"{room}_KakaoTalk_{saved_at.strftime('%Y%m%d')}_"
        f"{saved_at.strftime('%H%M')}_{saved_at.second:02d}_{unique}_group.txt"
    )
    header = (
        f"{room} 님과 카카오톡 대화\n"
        f"저장한 날짜 : {saved_at.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    )
    content = header + "\n".join(body_lines) + "\n"
    path = source_dir / filename
    path.write_text(content, encoding="utf-8")
    return path


def _write_legacy_room_txt(
    source_dir,
    room: str,
    saved_at: datetime,
    body_lines: list[str],
    unique: str = "450000",
) -> object:
    """실 운영 자동 수집 파일명(`{room}_YYYYMMDD_HHMMSS_고유값.txt`, `_KakaoTalk_` 토큰 없음)."""
    source_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{room}_{saved_at.strftime('%Y%m%d_%H%M%S')}_{unique}.txt"
    header = (
        f"{room} 님과 카카오톡 대화\n"
        f"저장한 날짜 : {saved_at.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    )
    content = header + "\n".join(body_lines) + "\n"
    path = source_dir / filename
    path.write_text(content, encoding="utf-8")
    return path


def _setup_env(tmp_path, monkeypatch):
    source_dir = tmp_path / "collect" / "카카오톡_수집"
    raw_dir = tmp_path / "analytics" / "raw"
    parquet_path = tmp_path / "mart" / "kakao_store_raw.parquet"
    state_path = tmp_path / "local" / "kakao_store_raw_processed_files.json"

    monkeypatch.setattr(load, "COLLECT_DB", tmp_path / "collect")
    monkeypatch.setattr(load, "KAKAO_STORE_RAW_DIR", raw_dir)
    monkeypatch.setattr(load, "KAKAO_STORE_RAW_PARQUET", parquet_path)
    monkeypatch.setattr(load, "KAKAO_STORE_RAW_STATE_JSON", state_path)
    return source_dir, raw_dir, parquet_path, state_path


def _read_mart(parquet_path) -> pd.DataFrame:
    return pd.read_parquet(parquet_path)


def test_same_file_rerun_adds_zero_new_rows(tmp_path, monkeypatch):
    source_dir, raw_dir, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d = date(2026, 9, 9)
    saved_at = datetime(2026, 9, 9, 15, 2, 3)
    body = [
        _date_separator(d),
        _message_line("테스트발신자", 15, 2, "A"),
        _message_line("테스트발신자", 15, 3, "B"),
        _message_line("테스트발신자", 15, 4, "C"),
    ]
    path = _write_room_txt(source_dir, "부산장림점", saved_at, body, unique="450")

    summary1 = load.run()
    assert "신규 추가: 3" in summary1
    df1 = _read_mart(parquet_path)
    assert len(df1) == 3

    archived = raw_dir / path.name
    assert archived.exists()
    # 처리 완료 후 이동된 원본을 그대로(같은 파일명) 다시 소스 위치에 되돌려
    # "동일 TXT로 재실행"을 재현한다.
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / archived.name).write_text(
        archived.read_text(encoding="utf-8"), encoding="utf-8"
    )

    summary2 = load.run()
    assert "신규 추가: 0" in summary2
    df2 = _read_mart(parquet_path)
    assert len(df2) == 3


def test_next_day_snapshot_adds_only_new_messages(tmp_path, monkeypatch):
    source_dir, _, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d1 = date(2026, 9, 9)
    body1 = [
        _date_separator(d1),
        _message_line("테스트발신자", 15, 2, "A"),
        _message_line("테스트발신자", 15, 3, "B"),
        _message_line("테스트발신자", 15, 4, "C"),
    ]
    _write_room_txt(
        source_dir, "부산장림점", datetime(2026, 9, 9, 15, 2, 3), body1, unique="001"
    )
    load.run()

    d2 = date(2026, 9, 10)
    body2 = [
        _date_separator(d1),
        _message_line("테스트발신자", 15, 2, "A"),
        _message_line("테스트발신자", 15, 3, "B"),
        _message_line("테스트발신자", 15, 4, "C"),
        _date_separator(d2),
        _message_line("테스트발신자", 6, 30, "D"),
        _message_line("테스트발신자", 6, 31, "E"),
    ]
    _write_room_txt(
        source_dir, "부산장림점", datetime(2026, 9, 10, 6, 30, 39), body2, unique="002"
    )
    summary2 = load.run()

    assert "신규 추가: 2" in summary2
    df = _read_mart(parquet_path)
    assert len(df) == 5
    assert set(df["message"]) == {"A", "B", "C", "D", "E"}


def test_multiline_message_merges_into_single_row(tmp_path, monkeypatch):
    source_dir, _, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d = date(2026, 7, 29)
    body = [
        _date_separator(d),
        "[[도리당]부산장림점 운영자 비성] [오후 7:05] 감자사리를",
        "감자토핑 으로",
        "변경해주세요",
        "국어사전에도안나와있고",
        "그렇게쓰면안된다고고객과",
        "말 씨름하기가힘들어요",
    ]
    _write_room_txt(source_dir, "부산장림점", datetime(2026, 7, 29, 19, 6, 0), body)
    load.run()

    df = _read_mart(parquet_path)
    assert len(df) == 1
    expected = (
        "감자사리를\n감자토핑 으로\n변경해주세요\n국어사전에도안나와있고\n"
        "그렇게쓰면안된다고고객과\n말 씨름하기가힘들어요"
    )
    assert df.iloc[0]["message"] == expected
    assert df.iloc[0]["sender"] == "[도리당]부산장림점 운영자 비성"


def test_blank_line_paragraph_preserved_in_multiline_message(tmp_path, monkeypatch):
    source_dir, _, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d = date(2026, 8, 21)
    body = [
        _date_separator(d),
        "[직원 이병두 과장 법인] [오후 5:24] 점주님 안녕하세요.",
        "영업관리부 이병두 과장입니다.",
        "",
        "8/22(토) ~ 8/30(일) 휴가로 인해 자리를 비우게 되었습니다.",
        "",
        "감사합니다.",
    ]
    _write_room_txt(source_dir, "김포풍무점", datetime(2026, 8, 21, 17, 30, 0), body)
    load.run()

    df = _read_mart(parquet_path)
    assert len(df) == 1
    message = df.iloc[0]["message"]
    assert "\n\n" in message
    assert message == (
        "점주님 안녕하세요.\n영업관리부 이병두 과장입니다.\n\n"
        "8/22(토) ~ 8/30(일) 휴가로 인해 자리를 비우게 되었습니다.\n\n감사합니다."
    )


def test_multi_store_room_splits_into_two_rows_same_message_id(tmp_path, monkeypatch):
    source_dir, _, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d = date(2026, 9, 9)
    body = [
        _date_separator(d),
        _message_line("점주", 10, 0, "안녕하세요"),
    ]
    _write_room_txt(
        source_dir, "부천옥길, 광명철산점", datetime(2026, 9, 9, 10, 0, 0), body
    )
    load.run()

    df = _read_mart(parquet_path)
    assert len(df) == 2
    assert set(df["store"]) == {"부천옥길점", "광명철산점"}
    assert df["message_id"].nunique() == 1


def test_original_text_preserved_verbatim(tmp_path, monkeypatch):
    source_dir, _, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d = date(2026, 9, 9)
    original = "감사힙니다~^^"
    body = [
        _date_separator(d),
        _message_line("점주", 11, 0, original),
    ]
    _write_room_txt(source_dir, "부산장림점", datetime(2026, 9, 9, 11, 30, 0), body)
    load.run()

    df = _read_mart(parquet_path)
    assert df.iloc[0]["message"] == original


def test_legacy_filename_format_parses_correctly(tmp_path, monkeypatch):
    """실 운영 자동 수집 파일명(`_KakaoTalk_` 토큰 없음)도 정상 파싱되어야 한다."""
    source_dir, _, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d = date(2026, 9, 9)
    body = [
        _date_separator(d),
        _message_line("점주", 15, 5, "안녕하세요"),
    ]
    _write_legacy_room_txt(
        source_dir, "강원영월점", datetime(2026, 9, 9, 15, 5, 30), body, unique="554214"
    )
    summary = load.run()
    assert "파일명 파싱 실패: 0" in summary
    assert "읽기 실패: 0" in summary

    df = _read_mart(parquet_path)
    assert len(df) == 1
    assert df.iloc[0]["store"] == "강원영월점"
    assert df.iloc[0]["message"] == "안녕하세요"
    assert str(df.iloc[0]["collected_at"]) == "2026-09-09 15:05:30"


def test_standalone_deleted_and_role_change_lines_are_not_dropped(tmp_path, monkeypatch):
    """접두사 없는 단독 줄(삭제/부방장 변경)도 버려지지 않고 행으로 남아야 한다."""
    source_dir, _, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d = date(2026, 9, 9)
    body = [
        _date_separator(d),
        _message_line("점주", 15, 5, "안녕하세요"),
        "메시지가 삭제되었습니다.",
        "도리당님이 부방장이 되었습니다.",
        "도리당님이 부방장에서 해제되었습니다.",
    ]
    _write_room_txt(source_dir, "삼송점", datetime(2026, 9, 9, 15, 5, 11), body)
    load.run()

    df = _read_mart(parquet_path).set_index("message")
    assert len(df) == 4
    deleted_row = df.loc["메시지가 삭제되었습니다."]
    assert deleted_row["message_type"] == "deleted"
    assert pd.isna(deleted_row["sender"])
    promoted_row = df.loc["도리당님이 부방장이 되었습니다."]
    assert promoted_row["message_type"] == "system"
    demoted_row = df.loc["도리당님이 부방장에서 해제되었습니다."]
    assert demoted_row["message_type"] == "system"


def test_existing_rows_preserved_after_new_load(tmp_path, monkeypatch):
    source_dir, _, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d1 = date(2026, 9, 9)
    body1 = [
        _date_separator(d1),
        _message_line("테스트발신자", 15, 2, "A"),
    ]
    _write_room_txt(
        source_dir, "부산장림점", datetime(2026, 9, 9, 15, 2, 3), body1, unique="001"
    )
    load.run()
    before = _read_mart(parquet_path)
    before_row = before.iloc[0].to_dict()

    d2 = date(2026, 9, 10)
    body2 = [
        _date_separator(d1),
        _message_line("테스트발신자", 15, 2, "A"),
        _date_separator(d2),
        _message_line("테스트발신자", 6, 30, "D"),
    ]
    _write_room_txt(
        source_dir, "부산장림점", datetime(2026, 9, 10, 6, 30, 39), body2, unique="002"
    )
    load.run()

    after = _read_mart(parquet_path)
    assert len(after) == 2
    kept = after[after["message"] == "A"].iloc[0]
    # 재수집되어도 최초 적재 시점의 collected_at/source_file은 바뀌면 안 된다.
    assert str(kept["source_file"]) == before_row["source_file"]
    assert kept["collected_at"] == before_row["collected_at"]


def test_nickname_change_updates_sender_without_duplicating(tmp_path, monkeypatch):
    source_dir, _, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d = date(2026, 9, 9)
    body1 = [
        _date_separator(d),
        _message_line("ひろ美...私の妻!!", 11, 0, "안녕하세요"),
    ]
    _write_room_txt(
        source_dir, "김포풍무점", datetime(2026, 9, 9, 11, 0, 30), body1, unique="001"
    )
    load.run()
    df1 = _read_mart(parquet_path)
    assert len(df1) == 1
    assert df1.iloc[0]["sender"] == "ひろ美...私の妻!!"

    # 다음날 스냅샷: 같은 메시지(같은 datetime+내용)인데 발신자 닉네임만 바뀜.
    body2 = [
        _date_separator(d),
        _message_line("점주님(새닉네임)", 11, 0, "안녕하세요"),
    ]
    _write_room_txt(
        source_dir, "김포풍무점", datetime(2026, 9, 10, 9, 0, 0), body2, unique="002"
    )
    summary2 = load.run()

    df2 = _read_mart(parquet_path)
    assert len(df2) == 1  # 중복 행이 생기면 안 됨 (message_id가 sender와 무관해야 함)
    assert df2.iloc[0]["sender"] == "점주님(새닉네임)"  # 최신 닉네임으로 덮어써야 함
    assert df2.iloc[0]["message_id"] == df1.iloc[0]["message_id"]
    assert "닉네임 갱신: 1" in summary2


def test_sender_type_classification(tmp_path, monkeypatch):
    source_dir, _, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d = date(2026, 8, 24)
    body = [
        _date_separator(d),
        _message_line("도리당", 9, 0, "공지드립니다"),
        _message_line("직원 이병두 과장 법인", 9, 1, "안녕하세요"),
        _message_line("김덕기 대리", 9, 2, "확인했습니다"),
        _message_line("ひろ美...私の妻!!", 9, 3, "네 알겠습니다"),
        "도리당 이병두 과장님이 도리당님을 초대했습니다.",
    ]
    _write_room_txt(source_dir, "김포풍무점", datetime(2026, 8, 24, 9, 30, 0), body)
    load.run()

    df = _read_mart(parquet_path).set_index("message")
    assert df.loc["공지드립니다", "type"] == "본사"
    assert df.loc["안녕하세요", "type"] == "본사"
    assert df.loc["확인했습니다", "type"] == "본사"
    assert df.loc["네 알겠습니다", "type"] == "점주"
    system_row = df.loc["도리당 이병두 과장님이 도리당님을 초대했습니다."]
    assert pd.isna(system_row["type"])


def test_unmapped_room_raises_and_does_not_crash_other_files(tmp_path, monkeypatch):
    source_dir, _, parquet_path, _ = _setup_env(tmp_path, monkeypatch)
    d = date(2026, 9, 9)
    body = [
        _date_separator(d),
        _message_line("점주", 10, 0, "hello"),
    ]
    _write_room_txt(source_dir, "미등록매장점", datetime(2026, 9, 9, 10, 0, 0), body)

    with pytest.raises(Exception):
        load.run()


def test_read_error_raises_but_still_commits_other_files(tmp_path, monkeypatch):
    """OneDrive placeholder처럼 특정 TXT가 [Errno 5] I/O 오류로 못 읽혀도,
    나머지 정상 파일은 parquet 적재/raw 이동/state 기록까지 끝나야 하고,
    실패 파일은 다음 실행에서 재시도되도록 소스 위치에 그대로 남아야 한다.
    """
    source_dir, raw_dir, parquet_path, state_path = _setup_env(tmp_path, monkeypatch)
    d = date(2026, 9, 9)
    body = [
        _date_separator(d),
        _message_line("점주", 10, 0, "hello"),
    ]
    good_path = _write_room_txt(
        source_dir, "부산장림점", datetime(2026, 9, 9, 10, 0, 0), body, unique="001"
    )
    bad_path = _write_room_txt(
        source_dir, "하늘도시점", datetime(2026, 9, 9, 10, 5, 0), body, unique="002"
    )

    original_read_text = load._read_text

    def flaky_read_text(path):
        if path.name == bad_path.name:
            raise OSError(5, "Input/output error")
        return original_read_text(path)

    monkeypatch.setattr(load, "_read_text", flaky_read_text)

    with pytest.raises(load.KakaoSourceReadError):
        load.run()

    df = _read_mart(parquet_path)
    assert len(df) == 1

    assert (raw_dir / good_path.name).exists()
    assert not good_path.exists()
    assert bad_path.exists()  # 다음 실행에서 재시도되도록 소스 위치에 남아 있어야 함

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert good_path.name in state
    assert bad_path.name not in state
