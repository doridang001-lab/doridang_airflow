import json

import pandas as pd

from modules.transform.pipelines.sales.BSP_DaangnAds_CSV import load_daangn_ads_csv


def _write_daangn_csv(path, rows):
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


def _row(collected_at, start_date, campaign_id, impressions, group="도리당 송파삼전점 #5", url=None):
    row = {
        "collected_at": collected_at,
        "시작일": start_date,
        "종료일": start_date,
        "campaign_id": campaign_id,
        "광고그룹명": group,
        "상태": "운영중",
        "광고명": "테스트 광고",
        "게재위치": "비즈프로필 홈",
        "노출수": str(impressions),
        "클릭수": "0",
        "클릭률": "0",
        "지출": "0",
        "스위치": "ON",
    }
    if url is not None:
        row["url"] = url
    return row


def test_load_daangn_ads_csv_writes_single_file(tmp_path):
    source_dir = tmp_path / "source"
    output_path = tmp_path / "analytics" / "Daangn_ads" / "daangn_ads.csv"
    source_dir.mkdir()

    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260825.csv",
        [_row("2026-08-26T08:00:00.000Z", "2026-08-25", "dg_1", 10)],
    )
    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260826.csv",
        [_row("2026-08-26T12:00:00.000Z", "2026-08-26", "dg_1", 100)],
    )

    sent = []
    email_calls = []
    result = json.loads(
        load_daangn_ads_csv(
            source_dir=source_dir,
            output_path=output_path,
            alert_sender=sent.append,
            email_sender=lambda subject, html, recipients: email_calls.append((subject, html, recipients)),
        )
    )
    saved = pd.read_csv(output_path, dtype=str, encoding="utf-8-sig").fillna("")

    assert result["status"] == "SUCCESS"
    assert output_path.exists()
    assert len(saved) == 2
    assert "_source_file" not in saved.columns
    assert result["cleanup_source"] is True
    assert len(result["cleaned_files"]) == 2
    assert list(source_dir.glob("daangn_ads_*.csv")) == []
    assert result["missing_dates"] == []
    assert result["missing_by_group"] == []
    assert result["telegram_sent"] is None
    assert result["email_sent"] is None
    assert result["email_recipients"] == []
    assert sent == []
    assert email_calls == []


def test_load_daangn_ads_csv_skips_when_no_new_files_and_output_exists(tmp_path):
    source_dir = tmp_path / "source"
    output_path = tmp_path / "analytics" / "Daangn_ads" / "daangn_ads.csv"
    source_dir.mkdir()
    output_path.parent.mkdir(parents=True)
    _write_daangn_csv(
        output_path,
        [_row("2026-08-26T12:00:00.000Z", "2026-08-26", "dg_1", 100)],
    )
    sent = []
    email_calls = []

    result = json.loads(
        load_daangn_ads_csv(
            source_dir=source_dir,
            output_path=output_path,
            alert_sender=sent.append,
            email_sender=lambda subject, html, recipients: email_calls.append((subject, html, recipients)),
        )
    )
    saved = pd.read_csv(output_path, dtype=str, encoding="utf-8-sig").fillna("")

    assert result["status"] == "NO_NEW_FILES"
    assert result["source_files"] == 0
    assert result["loaded_files"] == 0
    assert result["cleaned_files"] == []
    assert result["input_rows"] == 0
    assert result["output_rows"] == 1
    assert len(saved) == 1
    assert saved.iloc[0]["노출수"] == "100"
    assert result["telegram_sent"] is None
    assert result["email_sent"] is None
    assert sent == []
    assert email_calls == []


def test_load_daangn_ads_csv_returns_no_source_files_when_no_input_or_output(tmp_path):
    source_dir = tmp_path / "source"
    output_path = tmp_path / "analytics" / "Daangn_ads" / "daangn_ads.csv"
    source_dir.mkdir()
    sent = []
    email_calls = []

    result = json.loads(
        load_daangn_ads_csv(
            source_dir=source_dir,
            output_path=output_path,
            alert_sender=sent.append,
            email_sender=lambda subject, html, recipients: email_calls.append((subject, html, recipients)),
        )
    )

    assert result["status"] == "NO_SOURCE_FILES"
    assert result["source_files"] == 0
    assert result["loaded_files"] == 0
    assert result["cleaned_files"] == []
    assert result["input_rows"] == 0
    assert result["output_rows"] == 0
    assert not output_path.exists()
    assert result["telegram_sent"] is None
    assert result["email_sent"] is None
    assert sent == []
    assert email_calls == []


def test_load_daangn_ads_csv_keeps_latest_collected_at_for_duplicate_key(tmp_path):
    source_dir = tmp_path / "source"
    output_path = tmp_path / "analytics" / "Daangn_ads" / "daangn_ads.csv"
    source_dir.mkdir()

    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260826_0800.csv",
        [_row("2026-08-26T08:00:00.000Z", "2026-08-26", "dg_1", 1)],
    )
    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260826_1200.csv",
        [_row("2026-08-26T12:00:00.000Z", "2026-08-26", "dg_1", 100)],
    )

    load_daangn_ads_csv(source_dir=source_dir, output_path=output_path)
    saved = pd.read_csv(output_path, dtype=str, encoding="utf-8-sig").fillna("")

    assert len(saved) == 1
    assert saved.iloc[0]["노출수"] == "100"
    assert saved.iloc[0]["collected_at"] == "2026-08-26T12:00:00.000Z"
    assert list(source_dir.glob("daangn_ads_*.csv")) == []


def test_load_daangn_ads_csv_is_idempotent_with_existing_output(tmp_path):
    source_dir = tmp_path / "source"
    output_path = tmp_path / "analytics" / "Daangn_ads" / "daangn_ads.csv"
    source_dir.mkdir()

    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260826.csv",
        [_row("2026-08-26T12:00:00.000Z", "2026-08-26", "dg_1", 100)],
    )

    load_daangn_ads_csv(source_dir=source_dir, output_path=output_path)
    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260826_retry.csv",
        [_row("2026-08-26T12:00:00.000Z", "2026-08-26", "dg_1", 100)],
    )
    load_daangn_ads_csv(source_dir=source_dir, output_path=output_path)
    saved = pd.read_csv(output_path, dtype=str, encoding="utf-8-sig").fillna("")

    assert len(saved) == 1
    assert saved.iloc[0]["노출수"] == "100"
    assert list(source_dir.glob("daangn_ads_*.csv")) == []


def test_load_daangn_ads_csv_sends_missing_date_alert_with_locations(tmp_path):
    source_dir = tmp_path / "source"
    output_path = tmp_path / "analytics" / "Daangn_ads" / "daangn_ads.csv"
    source_dir.mkdir()
    sent = []
    email_calls = []

    def fake_sender(message):
        sent.append(message)
        return True

    def fake_email_sender(subject, html, recipients):
        email_calls.append((subject, html, recipients))
        return "메일 발송 완료: 1명"

    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260801.csv",
        [_row("2026-08-26T08:00:00.000Z", "2026-08-01", "dg_1", 10)],
    )
    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260803.csv",
        [_row("2026-08-26T12:00:00.000Z", "2026-08-03", "dg_1", 100)],
    )

    result = json.loads(
        load_daangn_ads_csv(
            source_dir=source_dir,
            output_path=output_path,
            alert_sender=fake_sender,
            email_sender=fake_email_sender,
        )
    )

    assert result["missing_dates"] == ["2026-08-02"]
    assert result["missing_by_group"] == [
        {
            "group": "도리당 송파삼전점 #5",
            "first_date": "2026-08-01",
            "last_date": "2026-08-03",
            "missing_dates": ["2026-08-02"],
        }
    ]
    assert result["telegram_sent"] is True
    assert result["email_sent"] is True
    assert result["email_recipients"] == ["melanie0204@kakao.com"]
    assert len(sent) == 1
    assert len(email_calls) == 1
    assert "[당근 광고 수집 누락]" in sent[0]
    assert "[당근 광고 수집 누락]\n범위:" not in sent[0]
    assert "수집 그룹: 도리당 송파삼전점 #5" in sent[0]
    assert "범위: 2026-08-01 ~ 2026-08-03" in sent[0]
    assert "누락일: 2026-08-02" in sent[0]
    assert f"입력 위치: {source_dir}" in sent[0]
    assert f"저장 위치: {output_path}" in sent[0]
    subject, html, recipients = email_calls[0]
    assert subject == "차보령 대리님 당근 광고 수집 누락일 확인 부탁드립니다"
    assert recipients == ["melanie0204@kakao.com"]
    assert "[당근 광고 수집 누락]<br>범위:" not in html
    assert "누락일: 2026-08-02" in html
    assert "수집 그룹: 도리당 송파삼전점 #5" in html
    assert f"입력 위치: {source_dir}" in html
    assert f"저장 위치: {output_path}" in html
    assert list(source_dir.glob("daangn_ads_*.csv")) == []


def test_load_daangn_ads_csv_alert_lists_only_group_with_missing_date(tmp_path):
    source_dir = tmp_path / "source"
    output_path = tmp_path / "analytics" / "Daangn_ads" / "daangn_ads.csv"
    source_dir.mkdir()
    sent = []
    email_calls = []

    def fake_sender(message):
        sent.append(message)
        return True

    def fake_email_sender(subject, html, recipients):
        email_calls.append((subject, html, recipients))
        return "메일 발송 완료: 1명"

    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260801.csv",
        [
            _row("2026-08-26T08:00:00.000Z", "2026-08-01", "dg_1", 10, group="그룹A"),
            _row("2026-08-26T08:00:00.000Z", "2026-08-01", "dg_2", 10, group="그룹B"),
        ],
    )
    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260802.csv",
        [_row("2026-08-26T09:00:00.000Z", "2026-08-02", "dg_2", 10, group="그룹B")],
    )
    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260803.csv",
        [
            _row("2026-08-26T12:00:00.000Z", "2026-08-03", "dg_1", 100, group="그룹A"),
            _row("2026-08-26T12:00:00.000Z", "2026-08-03", "dg_2", 100, group="그룹B"),
        ],
    )

    result = json.loads(
        load_daangn_ads_csv(
            source_dir=source_dir,
            output_path=output_path,
            alert_sender=fake_sender,
            email_sender=fake_email_sender,
        )
    )

    assert result["missing_dates"] == ["2026-08-02"]
    assert result["missing_by_group"] == [
        {
            "group": "그룹A",
            "first_date": "2026-08-01",
            "last_date": "2026-08-03",
            "missing_dates": ["2026-08-02"],
        }
    ]
    assert len(sent) == 1
    assert "수집 그룹: 그룹A" in sent[0]
    assert "수집 그룹: 그룹B" not in sent[0]
    assert "수집 그룹: 그룹A" in email_calls[0][1]
    assert "수집 그룹: 그룹B" not in email_calls[0][1]


def test_load_daangn_ads_csv_alert_never_uses_flat_date_only_format(tmp_path):
    source_dir = tmp_path / "source"
    output_path = tmp_path / "analytics" / "Daangn_ads" / "daangn_ads.csv"
    source_dir.mkdir()
    sent = []

    def fake_sender(message):
        sent.append(message)
        return True

    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260801.csv",
        [_row("2026-08-26T08:00:00.000Z", "2026-08-01", "dg_1", 10, group="그룹A")],
    )
    _write_daangn_csv(
        source_dir / "daangn_ads_account_unknown_20260803.csv",
        [_row("2026-08-26T12:00:00.000Z", "2026-08-03", "dg_1", 100, group="그룹A")],
    )

    load_daangn_ads_csv(
        source_dir=source_dir,
        output_path=output_path,
        alert_sender=fake_sender,
        email_sender=lambda subject, html, recipients: "메일 발송 완료: 1명",
    )

    lines = sent[0].splitlines()
    assert lines[0] == "[당근 광고 수집 누락]"
    assert "수집 그룹: 그룹A" in lines
    assert lines[1] != "범위: 2026-08-01 ~ 2026-08-03"


def test_load_daangn_ads_csv_fills_blank_url_from_matching_group(tmp_path):
    source_dir = tmp_path / "source"
    output_path = tmp_path / "analytics" / "Daangn_ads" / "daangn_ads.csv"
    source_dir.mkdir()
    output_path.parent.mkdir(parents=True)

    matched_group = "웹사이트 - 도리당 송파삼전점 플레이스 #1(08.06)"
    unmatched_group = "도리당 송파삼전점 #5"
    page_url = "https://ads-lite.business.daangn.com/ad-groups/QWRHcm91cDoxNzg1OTE3NjA1MjUxNzAxMDAx/"
    fallback_url = "https://ads-lite.business.daangn.com/ad-groups/QWRHcm91cDoxNzg2Njk2Mjc5NTgzNzU4MDAx/?filterType=ALL&startAt=2025-07-30T15%3A00%3A00.000Z&endAt=2026-08-31T14%3A59%3A59.999Z&groupConnectionId=client%3AQWR2ZXJ0aXNlcjozMDA0Mzc3%3A__adGroupList_adGroups_connection%28filter%3A%7B%22placementFilter%22%3A%5B%22ALL%22%5D%2C%22statusFilter%22%3A%5B%22ALL%22%5D%7D%29"

    _write_daangn_csv(
        output_path,
        [
            _row("2026-08-27T12:00:00.000Z", "2026-08-27", "dg_1", 10, group=matched_group),
            _row("2026-08-27T12:00:00.000Z", "2026-08-27", "dg_3", 15, group=matched_group),
            _row("2026-08-27T12:00:00.000Z", "2026-08-27", "dg_2", 20, group=unmatched_group),
        ],
    )
    _write_daangn_csv(
        source_dir / "daangn_ads_1785917605251701001_unknown_20260828.csv",
        [_row("2026-08-28T12:00:00.000Z", "2026-08-28", "dg_1", 100, group=matched_group, url=page_url)],
    )

    load_daangn_ads_csv(
        source_dir=source_dir,
        output_path=output_path,
        cleanup_source=False,
        alert_sender=lambda message: True,
        email_sender=lambda subject, html, recipients: "메일 발송 완료: 1명",
    )
    saved = pd.read_csv(output_path, dtype=str, encoding="utf-8-sig").fillna("")

    assert saved.columns[-1] == "url"
    old_matched = saved[(saved["시작일"] == "2026-08-27") & (saved["campaign_id"] == "dg_1")].iloc[0]
    old_same_group = saved[(saved["시작일"] == "2026-08-27") & (saved["campaign_id"] == "dg_3")].iloc[0]
    old_unmatched = saved[(saved["시작일"] == "2026-08-27") & (saved["campaign_id"] == "dg_2")].iloc[0]
    new_matched = saved[(saved["시작일"] == "2026-08-28") & (saved["campaign_id"] == "dg_1")].iloc[0]

    assert old_matched["url"] == page_url
    assert old_same_group["url"] == page_url
    assert old_unmatched["url"] == fallback_url
    assert new_matched["url"] == page_url


def test_load_daangn_ads_csv_keeps_unknown_group_url_blank(tmp_path):
    source_dir = tmp_path / "source"
    output_path = tmp_path / "analytics" / "Daangn_ads" / "daangn_ads.csv"
    source_dir.mkdir()

    _write_daangn_csv(
        source_dir / "daangn_ads_unknown_20260828.csv",
        [_row("2026-08-28T12:00:00.000Z", "2026-08-28", "dg_unknown", 100, group="새 광고그룹")],
    )

    load_daangn_ads_csv(
        source_dir=source_dir,
        output_path=output_path,
        cleanup_source=False,
        alert_sender=lambda message: True,
        email_sender=lambda subject, html, recipients: "메일 발송 완료: 1명",
    )
    saved = pd.read_csv(output_path, dtype=str, encoding="utf-8-sig").fillna("")

    assert saved.iloc[0]["url"] == ""
