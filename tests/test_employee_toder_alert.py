import pandas as pd

from modules.transform.pipelines.sales.employee_toder_alert import (
    build_toder_missing_alert,
    dispatch_toder_missing_alert,
)


def _row(**overrides):
    row = {
        "호점": "1",
        "매장명": "도리당 테스트점",
        "담당자": "김담당 과장",
        "점주명": "홍점주",
        "상세주소": "서울시 테스트구",
        "토더ID": "",
        "토더PW": "",
    }
    row.update(overrides)
    return row


def test_duplicate_store_is_complete_when_one_row_has_full_credentials():
    df = pd.DataFrame(
        [
            _row(호점="87", 담당자="", 토더ID="", 토더PW=""),
            _row(호점="25-1", 토더ID="account-1", 토더PW="password-1"),
        ]
    )

    result = build_toder_missing_alert(df)

    assert result.message == ""
    assert result.total_store_count == 1
    assert result.alertable_store_count == 1
    assert result.complete_store_count == 1
    assert result.missing_store_count == 0
    assert result.duplicate_store_count == 1


def test_id_and_password_on_different_rows_do_not_form_valid_credentials():
    df = pd.DataFrame(
        [
            _row(호점="1", 토더ID="account-1", 토더PW=""),
            _row(호점="2", 토더ID="", 토더PW="password-1"),
        ]
    )

    result = build_toder_missing_alert(df)

    assert result.missing_store_count == 1
    assert result.message.count("[신규 매장 / 양도양수 매장 / 해지 매장]") == 1


def test_incomplete_duplicate_store_alerts_once_and_prefers_latest_managed_row():
    df = pd.DataFrame(
        [
            _row(호점="1", 담당자="김이전", 핸드폰번호="01011112222", 상세주소="이전 주소"),
            _row(호점="2", 담당자="이최신", 점주명="최신 점주", 상세주소=""),
        ]
    )

    result = build_toder_missing_alert(df)

    assert result.missing_store_count == 1
    assert result.message.count("- 매장명 : 도리당 테스트점") == 1
    assert "- 사업자명의 : 최신 점주" in result.message
    assert "- 핸드폰번호 : 01011112222" in result.message
    assert "- 매장주소 : 이전 주소" in result.message
    assert "- 프로그램 설치 가능시간 : asap" in result.message


def test_mobile_suffix_phone_column_is_supported():
    df = pd.DataFrame([_row(**{"전화번호(mobile)": "010-7455-7840"})])

    result = build_toder_missing_alert(df)

    assert result.missing_store_count == 1
    assert "- 핸드폰번호 : 010-7455-7840" in result.message
    assert "- 프로그램 설치 가능시간 : asap" in result.message


def test_managerless_store_is_not_an_alert_target():
    result = build_toder_missing_alert(pd.DataFrame([_row(담당자="")]))

    assert result.total_store_count == 1
    assert result.alertable_store_count == 0
    assert result.missing_store_count == 0
    assert result.message == ""


def test_whitespace_in_toder_column_names_is_supported():
    df = pd.DataFrame(
        [
            {
                "매장명": "도리당 테스트점",
                "담당자": "김담당",
                "토더 ID": "account-1",
                "토더 PW": "password-1",
            }
        ]
    )

    result = build_toder_missing_alert(df)

    assert result.complete_store_count == 1
    assert result.missing_store_count == 0


def test_missing_required_column_returns_diagnostic_reason():
    df = pd.DataFrame([{"매장명": "도리당 테스트점", "담당자": "김담당"}])

    result = build_toder_missing_alert(df)

    assert result.message == ""
    assert result.skipped_reason == "필수 컬럼 없음: 토더ID, 토더PW"


def test_dispatch_sends_one_message_for_duplicate_missing_store():
    sent = []
    df = pd.DataFrame([_row(호점="1"), _row(호점="2")])

    result = dispatch_toder_missing_alert(df, sent.append)

    assert result.missing_store_count == 1
    assert len(sent) == 1
    assert sent[0].count("[신규 매장 / 양도양수 매장 / 해지 매장]") == 1
