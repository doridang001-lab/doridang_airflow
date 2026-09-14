"""BSP KPI 주간 통합 파이프라인 판정 테스트."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from openpyxl import load_workbook

from modules.transform.pipelines.strategy.SMP_bsp_kpi_weekly import (
    BRAND_VIRAL_METRICS,
    DIRECT_STORE_METRICS,
    build_missing_alert,
    dispatch_missing_alert,
    notify_missing_alert,
    render_alert_email_text,
    render_alert_html,
    render_alert_subject,
    render_alert_text,
    resolve_alert_emails,
    resolve_owner_alert_emails,
    resolve_target_week_start,
)
from modules.transform.pipelines.strategy import SMP_bsp_kpi_weekly as pipeline


VIRAL_ACTUAL_COLUMNS = ["주 시작일", "ym", "월 주차", "담당자", "참여자 수 목표", "게시물 수", "도달 수", "저장 수", "공유 수", "좋아요 수"]
STORE_ACTUAL_COLUMNS = ["주시작일", "ym", "주차", "담당자", "네이버 노출", "네이버 클릭", "네이버 비용"]
VIRAL_PROGRESS_COLUMNS = ["시작일", "종료일", "담당자", "이벤트명", "이벤트 참여자수 목표", "상품 제공 설정 금액"]


def _viral_actual(rows):
    return pd.DataFrame(rows, columns=VIRAL_ACTUAL_COLUMNS)


def _viral_target(rows):
    columns = ["주 시작일", "ym", "월 주차", "담당자", "목표 상품 제공 금액", "목표 참여자 수", "목표 게시물 수", "목표 도달 수"]
    return pd.DataFrame(rows, columns=columns)


def _viral_progress(rows):
    return pd.DataFrame(rows, columns=VIRAL_PROGRESS_COLUMNS)


def _store_actual(rows):
    return pd.DataFrame(rows, columns=STORE_ACTUAL_COLUMNS)


def _store_target(rows):
    columns = ["주시작일", "ym", "주차", "담당자", "목표 네이버 노출", "목표 네이버 클릭", "목표네이버 비용"]
    return pd.DataFrame(rows, columns=columns)


def test_key_columns_match_across_naming_variants():
    """`주 시작일`과 `주시작일`을 모두 같은 키로 인식한다."""
    spaced = pipeline._normalize_keys(_viral_actual([["2026-08-03", "2026_08", "8월 1주차", "황유경", 5, 3, 1694, None, None, None]]))
    packed = pipeline._normalize_keys(_store_actual([["2026-08-03", "2026_08", "8월 1주차", "차보령", 100, 10, 1000]]))

    assert spaced["week_start"].iloc[0] == date(2026, 8, 3)
    assert packed["week_start"].iloc[0] == date(2026, 8, 3)
    assert spaced["week_label"].iloc[0] == "8월 1주차"
    assert packed["week_label"].iloc[0] == "8월 1주차"


def test_viral_participants_column_is_mapped_to_actual():
    """실적 시트의 오타 컬럼 `참여자 수 목표`는 실적으로 매핑한다."""
    long_df = pipeline._to_long(
        _viral_actual([["2026-08-03", "2026_08", "8월 1주차", "황유경", 21, 22, 4760, 43, 101, 1126]]),
        BRAND_VIRAL_METRICS,
        value_kind="actual",
    )
    participants = long_df[long_df["metric_key"] == "participants"]
    assert participants["actual_value"].iloc[0] == 21


def test_viral_new_actual_columns_and_efficiency_inputs_are_mapped():
    """브랜드 바이럴 신컬럼 `참여자 수`, `상품 제공 금액`, `참여자 1명당 기준금액`을 인식한다."""
    columns = [
        "주 시작일",
        "ym",
        "월 주차",
        "담당자",
        "참여자 수",
        "게시물 수",
        "도달 수",
        "저장 수",
        "공유 수",
        "좋아요 수",
        "참여자 1명당 기준금액",
        "상품 제공 금액",
    ]
    actual = pd.DataFrame(
        [["2026-07-20", "2026_07", "7월 4주차", "황유경", 21, 22, 4760, 43, 101, 1126, 1000, 50000]],
        columns=columns,
    )

    long_df = pipeline._to_long(actual, BRAND_VIRAL_METRICS, value_kind="actual")

    assert long_df[long_df["metric_key"] == "participants"]["actual_value"].iloc[0] == 21
    assert long_df[long_df["metric_key"] == "product_cost"]["actual_value"].iloc[0] == 50000
    assert long_df[long_df["metric_key"] == "participant_unit_amount"]["actual_value"].iloc[0] == 1000


def test_simple_excel_formula_values_are_used_when_cache_is_empty(tmp_path):
    """openpyxl 저장으로 수식 캐시가 비어도 단순 숫자 수식은 실적으로 읽는다."""
    path = tmp_path / "formula.xlsx"
    actual = _viral_actual(
        [["2026-07-27", "2026_07", "7월 5주차", "황유경", 8, 12, None, None, None, None]]
    )
    actual.to_excel(path, index=False, engine="openpyxl")
    wb = load_workbook(path)
    ws = wb["Sheet1"]
    ws.cell(row=2, column=7).value = "=228+54+39+47+49+55+21+22+19+17+11+14"
    wb.save(path)

    df = pipeline._read_kpi_sheet(path)
    long_df = pipeline._to_long(df, BRAND_VIRAL_METRICS, value_kind="actual")

    reach = long_df[long_df["metric_key"] == "reach"].iloc[0]
    assert reach["actual_value"] == 576


def test_target_sheet_typo_columns_are_mapped():
    """목표 시트의 띄어쓰기 누락 컬럼(`목표네이버 비용`)도 매핑한다."""
    long_df = pipeline._to_long(
        _store_target([["2026-08-03", "2026_08", "8월 1주차", "차보령", 146661, 890, 420000]]),
        DIRECT_STORE_METRICS,
        value_kind="target",
    )
    cost = long_df[long_df["metric_key"] == "naver_cost"]
    assert cost["target_value"].iloc[0] == 420000


def test_brand_viral_product_and_participant_targets_are_calculated(tmp_path, monkeypatch):
    """목표 상품 제공 금액·목표 참여자 수는 진행 이벤트 기간 기준으로 일할 배분한다."""
    actual = _viral_actual(
        [["2026-07-20", "2026_07", "7월 4주차", "황유경", None, None, None, None, None, None]]
    )
    target = _viral_target(
        [["2026-07-20", "2026_07", "7월 4주차", "황유경", 999999, 99, 3, 1694]]
    )
    progress = _viral_progress(
        [["2026-07-10", "2026-08-17", "황유경", "복날에 보양하고 떠나라", 60, 1300000]]
    )
    domain = pipeline.DOMAINS[0]
    actual.to_excel(tmp_path / domain.actual_file, index=False, engine="openpyxl")
    target.to_excel(tmp_path / domain.target_file, index=False, engine="openpyxl")
    progress.to_excel(tmp_path / domain.in_progress_file, index=False, engine="openpyxl")

    df = pipeline._finalize(pipeline._merge_domain(domain, tmp_path), today=date(2026, 8, 17))

    product_cost = df[df["metric_key"] == "product_cost"].iloc[0]
    participants = df[df["metric_key"] == "participants"].iloc[0]
    unit_amount = df[df["metric_key"] == "participant_unit_amount"].iloc[0]
    assert product_cost["target_value"] == 233333
    assert participants["target_value"] == 11
    assert unit_amount["target_value"] == 21667


def test_build_kpi_weekly_syncs_brand_viral_workbooks(tmp_path, monkeypatch):
    """DAG 실행 함수는 목표 파일과 주별 실적 파일의 계산 컬럼을 엑셀에도 채운다."""
    actual = _viral_actual(
        [["2026-07-20", "2026_07", "7월 4주차", "황유경", 21, 22, 4760, 43, 101, 1126]]
    )
    target = _viral_target(
        [["2026-07-20", "2026_07", "7월 4주차", "황유경", None, None, 3, 1694]]
    )
    progress = _viral_progress(
        [["2026-07-10", "2026-08-17", "황유경", "복날에 보양하고 떠나라", 60, 1300000]]
    )
    domain = pipeline.DOMAINS[0]
    actual.to_excel(tmp_path / domain.actual_file, index=False, engine="openpyxl")
    target.to_excel(tmp_path / domain.target_file, index=False, engine="openpyxl")
    progress.to_excel(tmp_path / domain.in_progress_file, index=False, engine="openpyxl")
    monkeypatch.setattr(pipeline, "BSP_KPI_DIR", tmp_path)
    monkeypatch.setattr(pipeline, "BSP_KPI_WEEKLY_PARQUET", tmp_path / "bsp_kpi_weekly.parquet")
    monkeypatch.setattr(pipeline, "BSP_KPI_WEEKLY_CSV", tmp_path / "bsp_kpi_weekly.csv")

    assert pipeline.build_kpi_weekly() == str(tmp_path / "bsp_kpi_weekly.parquet")

    actual_wb = load_workbook(tmp_path / domain.actual_file, data_only=True)
    actual_ws = actual_wb["Sheet1"]
    actual_headers = {actual_ws.cell(row=1, column=col).value: col for col in range(1, actual_ws.max_column + 1)}
    assert actual_ws.cell(row=2, column=actual_headers["상품 제공 금액"]).value == 233333
    assert actual_ws.cell(row=2, column=actual_headers["참여자 1명당 기준금액"]).value == 11111

    target_wb = load_workbook(tmp_path / domain.target_file, data_only=True)
    target_ws = target_wb["Sheet1"]
    target_headers = {target_ws.cell(row=1, column=col).value: col for col in range(1, target_ws.max_column + 1)}
    assert target_ws.cell(row=2, column=target_headers["목표 상품 제공 금액"]).value == 233333
    assert target_ws.cell(row=2, column=target_headers["목표 참여자 수"]).value == 11
    assert target_ws.cell(row=2, column=target_headers["채워야 참여자 1명당 기준금액"]).value == 21667

    df = pd.read_parquet(tmp_path / "bsp_kpi_weekly.parquet")
    product_cost = df[(df["domain"] == "brand_viral") & (df["metric_key"] == "product_cost")].iloc[0]
    unit_amount = df[(df["domain"] == "brand_viral") & (df["metric_key"] == "participant_unit_amount")].iloc[0]
    assert product_cost["actual_value"] == 233333
    assert product_cost["target_value"] == 233333
    assert unit_amount["actual_value"] == 11111
    assert unit_amount["target_value"] == 21667


def test_brand_viral_efficiency_metrics_are_calculated(tmp_path, monkeypatch):
    columns = [
        "주 시작일",
        "ym",
        "월 주차",
        "담당자",
        "참여자 수",
        "게시물 수",
        "도달 수",
        "저장 수",
        "공유 수",
        "좋아요 수",
        "참여자 1명당 기준금액",
        "상품 제공 금액",
    ]
    actual = pd.DataFrame(
        [["2026-07-20", "2026_07", "7월 4주차", "황유경", 21, 22, 4760, 43, 101, 1126, 1000, 50000]],
        columns=columns,
    )
    target = _viral_target(
        [["2026-07-20", "2026_07", "7월 4주차", "황유경", 225806, 5, 3, 1694]]
    )
    progress = _viral_progress(
        [["2026-07-10", "2026-08-17", "황유경", "복날에 보양하고 떠나라", 60, 1300000]]
    )
    domain = pipeline.DOMAINS[0]
    actual.to_excel(tmp_path / domain.actual_file, index=False, engine="openpyxl")
    target.to_excel(tmp_path / domain.target_file, index=False, engine="openpyxl")
    progress.to_excel(tmp_path / domain.in_progress_file, index=False, engine="openpyxl")

    df = pipeline._finalize(pipeline._merge_domain(domain, tmp_path), today=date(2026, 8, 17))

    required = df[df["metric_key"] == "required_participants"].iloc[0]
    actual_cost = df[df["metric_key"] == "actual_cost_per_participant"].iloc[0]
    assert required["actual_value"] == 50
    assert actual_cost["actual_value"] == 2381
    assert df["metric_key"].ne("participant_efficiency_rate").all()


def test_metric_without_counterpart_has_null_rate(tmp_path, monkeypatch):
    """목표만/실적만 있는 지표는 달성률이 NULL이다."""
    df = _build_viral_frame(tmp_path, monkeypatch)

    product_cost = df[df["metric_key"] == "product_cost"].iloc[0]
    assert product_cost["has_target"] is True or bool(product_cost["has_target"])
    assert pd.isna(product_cost["actual_value"])
    assert pd.isna(product_cost["achievement_rate"])

    saves = df[df["metric_key"] == "saves"].iloc[0]
    assert not bool(saves["has_target"])
    assert pd.isna(saves["achievement_rate"])


def test_is_submitted_requires_all_required_inputs(tmp_path, monkeypatch):
    """필수 수기 지표가 전부 입력되어야 제출로 본다."""
    df = _build_viral_frame(tmp_path, monkeypatch)

    filled = df[df["week_start"] == date(2026, 7, 27)]
    empty = df[df["week_start"] == date(2026, 8, 3)]
    assert bool(filled["is_submitted"].all())
    assert not bool(empty["is_submitted"].any())


def test_achievement_rate_and_gap(tmp_path, monkeypatch):
    df = _build_viral_frame(tmp_path, monkeypatch)
    reach = df[(df["metric_key"] == "reach") & (df["week_start"] == date(2026, 7, 27))].iloc[0]
    assert reach["target_value"] == 1694
    assert reach["actual_value"] == 4760
    assert reach["achievement_rate"] == pytest.approx(4760 / 1694)
    assert reach["gap"] == pytest.approx(4760 - 1694)


def test_alert_lists_missing_owner():
    df = _alert_frame(has_actual_by_domain={"brand_viral": False, "direct_store_marketing": False})
    result = build_missing_alert(df, target_week_start="2026-08-03")

    assert result.missing_count == 2
    assert result.checked_count == 2
    assert "황유경" in result.message
    assert "차보령" in result.message
    assert "8월 1주차" in result.message


def test_alert_keeps_partially_filled_domain_missing():
    df = _alert_frame(has_actual_by_domain={"brand_viral": True, "direct_store_marketing": False})
    df.loc[(df["domain"] == "brand_viral") & (df["metric_key"] == "metric_1"), "has_actual"] = False
    result = build_missing_alert(df, target_week_start="2026-08-03")

    assert result.missing_count == 2
    assert "황유경" in result.message
    assert "차보령" in result.message


def test_alert_ignores_auto_metric_when_manual_metrics_are_empty():
    df = pd.DataFrame(
        [
            {
                "week_start": date(2026, 8, 3),
                "ym": "2026_08",
                "week_label": "8월 1주차",
                "domain": "brand_viral",
                "domain_name": "브랜드 바이럴",
                "owner": "황유경",
                "metric_key": "product_cost",
                "has_actual": True,
                "alert_required": False,
            },
            {
                "week_start": date(2026, 8, 3),
                "ym": "2026_08",
                "week_label": "8월 1주차",
                "domain": "brand_viral",
                "domain_name": "브랜드 바이럴",
                "owner": "황유경",
                "metric_key": "participants",
                "has_actual": False,
                "alert_required": True,
            },
            {
                "week_start": date(2026, 8, 3),
                "ym": "2026_08",
                "week_label": "8월 1주차",
                "domain": "brand_viral",
                "domain_name": "브랜드 바이럴",
                "owner": "황유경",
                "metric_key": "posts",
                "has_actual": False,
                "alert_required": True,
            },
        ]
    )

    result = build_missing_alert(df, target_week_start="2026-08-03")

    assert result.missing_count == 1
    assert "황유경" in result.message


def test_alert_keeps_direct_store_partial_input_missing():
    df = _alert_frame(has_actual_by_domain={"brand_viral": True, "direct_store_marketing": True})
    df.loc[
        (df["domain"] == "direct_store_marketing") & (df["metric_key"] == "metric_2"),
        "has_actual",
    ] = False

    result = build_missing_alert(df, target_week_start="2026-08-03")

    assert result.missing_count == 1
    assert "황유경" not in result.message
    assert "차보령" in result.message


def test_alert_not_sent_when_nothing_missing():
    df = _alert_frame(has_actual_by_domain={"brand_viral": True, "direct_store_marketing": True})
    sent: list[str] = []

    result = dispatch_missing_alert(df, sent.append, target_week_start="2026-08-03")

    assert result.message == ""
    assert result.missing_count == 0
    assert sent == []


def test_alert_skips_week_without_rows():
    df = _alert_frame(has_actual_by_domain={"brand_viral": False, "direct_store_marketing": False})
    result = build_missing_alert(df, target_week_start="2026-07-06")

    assert result.message == ""
    assert result.missing_count == 0
    assert "대상 주차 행 없음" in result.skipped_reason


def test_resolve_target_week_start():
    import pendulum

    # 2026-08-10(월) 실행 → 직전 완료 주차는 2026-08-03
    monday = pendulum.datetime(2026, 8, 10, 11, 0, tz="Asia/Seoul")
    assert resolve_target_week_start(None, now=monday) == "2026-08-03"

    # 2026-08-11(화) 실행도 같은 주차를 본다
    tuesday = pendulum.datetime(2026, 8, 11, 11, 0, tz="Asia/Seoul")
    assert resolve_target_week_start({}, now=tuesday) == "2026-08-03"

    # conf 우선
    assert resolve_target_week_start({"week_start": "2026-07-27"}, now=monday) == "2026-07-27"

    with pytest.raises(ValueError):
        resolve_target_week_start({"week_start": "2026/13/99"}, now=monday)


def test_configured_emails_drop_none_entries():
    """CMJ는 메일 수신자에서 제외되고 담당자 주소만 남는다."""
    assert resolve_alert_emails() == ["syd662@kakao.com", "melanie0204@kakao.com"]
    assert pipeline.MAIL_BSP_KPI_CMJ not in resolve_alert_emails()


def test_email_recipient_can_be_added_by_editing_top_variable(monkeypatch):
    """상단 매핑에 주소를 추가하면 담당자별 수신자가 바뀐다."""
    monkeypatch.setattr(
        pipeline,
        "BSP_KPI_ALERT_EMAILS",
        {
            "황유경": "hwang@example.com",
            "차보령": None,
            "신규": ["new@example.com", "hwang@example.com"],
        },
    )
    assert resolve_owner_alert_emails("황유경") == ["hwang@example.com"]
    assert resolve_owner_alert_emails("차보령") == []
    assert resolve_alert_emails() == ["hwang@example.com", "new@example.com"]


def test_notify_fans_out_to_both_channels(monkeypatch):
    df = _alert_frame(has_actual_by_domain={"brand_viral": False, "direct_store_marketing": False})
    monkeypatch.setattr(
        pipeline,
        "BSP_KPI_ALERT_EMAILS",
        {
            "황유경": "hwang@example.com",
            "차보령": "cha@example.com",
            "조민준": "cmj@example.com",
        },
    )
    telegram_calls: list[str] = []
    email_calls: list[tuple[str, str, list[str]]] = []

    result = notify_missing_alert(
        df,
        target_week_start="2026-08-03",
        telegram_sender=telegram_calls.append,
        email_sender=lambda subject, html, emails: email_calls.append((subject, html, emails)),
    )

    assert len(telegram_calls) == 1
    assert len(email_calls) == 2
    subjects = [call[0] for call in email_calls]
    assert subjects == [
        "황유경 실장님 브랜드 전략기획 KPI 주간입력 부탁드립니다",
        "차보령 대리님 브랜드 전략기획 KPI 주간입력 부탁드립니다",
    ]
    assert email_calls[0][2] == ["hwang@example.com"]
    assert email_calls[1][2] == ["cha@example.com"]
    assert "차보령" not in email_calls[0][1]
    assert "황유경" not in email_calls[1][1]
    for subject, html, emails in email_calls:
        assert "cmj@example.com" not in emails
        assert "브랜드 전략기획 KPI 주간 실적 입력 부탁드립니다" in html
        assert "<table" not in html
    assert result.sent_telegram is True
    assert result.sent_emails == ("hwang@example.com", "cha@example.com")


def test_notify_skips_email_when_no_address_configured(monkeypatch):
    df = _alert_frame(has_actual_by_domain={"brand_viral": False, "direct_store_marketing": False})
    monkeypatch.setattr(pipeline, "BSP_KPI_ALERT_EMAILS", {"황유경": None, "차보령": None})
    telegram_calls: list[str] = []
    email_calls: list[object] = []

    result = notify_missing_alert(
        df,
        target_week_start="2026-08-03",
        telegram_sender=telegram_calls.append,
        email_sender=lambda subject, html, emails: email_calls.append(emails),
    )

    assert len(telegram_calls) == 1
    assert email_calls == []
    assert result.sent_emails == ()


def test_notify_skips_telegram_when_switch_is_off(monkeypatch):
    """BSP_KPI_ALERT_TELEGRAM=False면 메일만 나간다."""
    df = _alert_frame(has_actual_by_domain={"brand_viral": False, "direct_store_marketing": False})
    monkeypatch.setattr(pipeline, "BSP_KPI_ALERT_TELEGRAM", False)
    monkeypatch.setattr(pipeline, "BSP_KPI_ALERT_EMAILS", {"황유경": "hwang@example.com", "차보령": "cha@example.com"})
    telegram_calls: list[str] = []
    email_calls: list[list[str]] = []

    result = notify_missing_alert(
        df,
        target_week_start="2026-08-03",
        telegram_sender=telegram_calls.append,
        email_sender=lambda subject, html, emails: email_calls.append(emails),
    )

    assert telegram_calls == []
    assert email_calls == [["hwang@example.com"], ["cha@example.com"]]
    assert result.sent_telegram is False


def test_notify_continues_when_one_channel_fails(monkeypatch):
    """텔레그램이 실패해도 메일은 나간다."""
    df = _alert_frame(has_actual_by_domain={"brand_viral": False, "direct_store_marketing": False})
    monkeypatch.setattr(pipeline, "BSP_KPI_ALERT_EMAILS", {"황유경": "hwang@example.com", "차보령": "cha@example.com"})
    email_calls: list[list[str]] = []

    def broken_telegram(message):
        raise RuntimeError("telegram down")

    result = notify_missing_alert(
        df,
        target_week_start="2026-08-03",
        telegram_sender=broken_telegram,
        email_sender=lambda subject, html, emails: email_calls.append(emails),
    )

    assert result.sent_telegram is False
    assert result.sent_emails == ("hwang@example.com", "cha@example.com")
    assert email_calls == [["hwang@example.com"], ["cha@example.com"]]


def test_notify_sends_telegram_when_smtp_auth_fails(monkeypatch):
    df = _alert_frame(has_actual_by_domain={"brand_viral": False, "direct_store_marketing": False})
    monkeypatch.setattr(pipeline, "BSP_KPI_ALERT_EMAILS", {"황유경": "hwang@example.com", "차보령": "cha@example.com"})
    telegram_calls: list[str] = []
    email_calls: list[list[str]] = []

    def broken_email(subject, html, emails):
        del subject, html
        email_calls.append(emails)
        raise RuntimeError("535 5.7.8 Username and Password not accepted BadCredentials")

    result = notify_missing_alert(
        df,
        target_week_start="2026-08-03",
        telegram_sender=telegram_calls.append,
        email_sender=broken_email,
    )

    assert email_calls == [["hwang@example.com"], ["cha@example.com"]]
    assert result.sent_emails == ()
    assert len(telegram_calls) == 2
    assert telegram_calls[0].startswith("[BSP KPI 주간 미입력]")
    assert telegram_calls[1].startswith("[BSP KPI 메일 발송 실패] SMTP 인증 필요")
    assert "https://myaccount.google.com/apppasswords" in telegram_calls[1]
    assert "doridang_conn_smtp_gmail" in telegram_calls[1]


def test_notify_does_not_count_failed_email_return_value(monkeypatch):
    df = _alert_frame(has_actual_by_domain={"brand_viral": False, "direct_store_marketing": False})
    monkeypatch.setattr(pipeline, "BSP_KPI_ALERT_EMAILS", {"황유경": "hwang@example.com", "차보령": "cha@example.com"})
    telegram_calls: list[str] = []

    def failed_email_return(subject, html, emails):
        del subject, html, emails
        return "메일 발송 실패: 535 5.7.8 Username and Password not accepted"

    result = notify_missing_alert(
        df,
        target_week_start="2026-08-03",
        telegram_sender=telegram_calls.append,
        email_sender=failed_email_return,
    )

    assert result.sent_emails == ()
    assert len([text for text in telegram_calls if text.startswith("[BSP KPI 메일 발송 실패]")]) == 1


def test_notify_sends_nothing_when_nothing_missing(monkeypatch):
    df = _alert_frame(has_actual_by_domain={"brand_viral": True, "direct_store_marketing": True})
    monkeypatch.setattr(pipeline, "BSP_KPI_ALERT_EMAILS", {"황유경": "hwang@example.com"})
    calls: list[object] = []

    result = notify_missing_alert(
        df,
        target_week_start="2026-08-03",
        telegram_sender=calls.append,
        email_sender=lambda subject, html, emails: calls.append(emails),
    )

    assert calls == []
    assert result.message == ""
    assert result.sent_telegram is False


# ============================================================
# 메일 본문
# ============================================================


def _missing_result():
    df = _alert_frame(has_actual_by_domain={"brand_viral": False, "direct_store_marketing": False})
    return build_missing_alert(df, target_week_start="2026-08-03")


def test_html_contains_owner_domain_and_source_file():
    html = render_alert_html(_missing_result())

    assert "황유경 실장님" in html
    assert "차보령" not in html
    assert "브랜드 바이럴" in html
    assert "직영점 마케팅" not in html
    assert "weekly_brand_viral_performance_tracke.xlsx" not in html
    assert "weekly_direct_store_marketing_growth.xlsx" not in html


def test_html_is_self_contained_for_mail_clients():
    """카카오·지메일이 잘라내는 요소와 카드형 표 레이아웃을 쓰지 않는다."""
    html = render_alert_html(_missing_result())

    assert html.startswith("<!DOCTYPE html>")
    assert "<table" not in html
    assert "<style" not in html
    assert "http://" not in html
    assert "https://" not in html
    assert "<img" not in html


def test_html_shows_week_period_and_action_message():
    html = render_alert_html(_missing_result())

    assert "8월 1주차" in html
    assert "08-03 ~ 08-09" in html
    assert "입력되지 않았습니다" in html
    assert "data/mart/brand_strategy_planning_team/bsp_kpi/" in html


def test_html_escapes_owner_value():
    """엑셀에서 읽은 값이라 태그 문자가 들어와도 그대로 렌더되지 않아야 한다."""
    df = _alert_frame(has_actual_by_domain={"brand_viral": False, "direct_store_marketing": False})
    df.loc[df["domain"] == "brand_viral", "owner"] = "<script>x</script>"
    result = build_missing_alert(df, target_week_start="2026-08-03")

    html = render_alert_html(result)

    assert "<script>" not in html
    assert "&lt;script&gt;x&lt;/script&gt;님" in html


def test_subject_uses_owner_display_name():
    result = _missing_result()

    assert render_alert_subject(result, owner="황유경") == "황유경 실장님 브랜드 전략기획 KPI 주간입력 부탁드립니다"
    assert render_alert_subject(result, owner="차보령") == "차보령 대리님 브랜드 전략기획 KPI 주간입력 부탁드립니다"


def test_email_text_can_render_single_owner_rows():
    result = _missing_result()
    cha_rows = tuple(row for row in result.missing_rows if row.owner == "차보령")

    text = render_alert_email_text(result, owner="차보령", missing_rows=cha_rows)

    assert text.startswith("차보령 대리님")
    assert "직영점 마케팅" in text
    assert "브랜드 바이럴" not in text


def test_text_body_lists_missing_owners():
    text = render_alert_text(_missing_result())

    assert "[BSP KPI 주간 미입력]" in text
    assert "- 브랜드 바이럴 : 황유경" in text
    assert "- 직영점 마케팅 : 차보령" in text
    assert "08-03 ~ 08-09" in text
    assert "<" not in text  # 텔레그램은 평문이어야 한다


# ============================================================
# 헬퍼
# ============================================================


def _build_viral_frame(tmp_path, monkeypatch):
    """엑셀 2개를 임시 폴더에 써서 브랜드 바이럴 도메인만 통합한다."""
    actual = _viral_actual(
        [
            ["2026-07-27", "2026_07", "7월 4주차", "황유경", 21, 22, 4760, 43, 101, 1126],
            ["2026-08-03", "2026_08", "8월 1주차", "황유경", None, None, None, None, None, None],
        ]
    )
    target = _viral_target(
        [
            ["2026-07-27", "2026_07", "7월 4주차", "황유경", 225806, 5, 3, 1694],
            ["2026-08-03", "2026_08", "8월 1주차", "황유경", 225806, 5, 3, 1694],
        ]
    )
    progress = _viral_progress(
        [["2026-07-27", "2026-08-09", "황유경", "2주 이벤트", 28, 1400000]]
    )
    domain = pipeline.DOMAINS[0]
    actual.to_excel(tmp_path / domain.actual_file, index=False, engine="openpyxl")
    target.to_excel(tmp_path / domain.target_file, index=False, engine="openpyxl")
    progress.to_excel(tmp_path / domain.in_progress_file, index=False, engine="openpyxl")

    merged = pipeline._merge_domain(domain, tmp_path)
    return pipeline._finalize(merged, today=date(2026, 8, 17))


def _alert_frame(*, has_actual_by_domain):
    rows = []
    for domain, has_actual in has_actual_by_domain.items():
        owner = "황유경" if domain == "brand_viral" else "차보령"
        domain_name = "브랜드 바이럴" if domain == "brand_viral" else "직영점 마케팅"
        for index in range(3):
            rows.append(
                {
                    "week_start": date(2026, 8, 3),
                    "ym": "2026_08",
                    "week_label": "8월 1주차",
                    "domain": domain,
                    "domain_name": domain_name,
                    "owner": owner,
                    "metric_key": f"metric_{index}",
                    "has_actual": bool(has_actual),
                    "alert_required": True,
                }
            )
    return pd.DataFrame(rows)
