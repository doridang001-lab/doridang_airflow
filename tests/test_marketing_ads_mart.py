import json

import pandas as pd
import pytest

from modules.transform.pipelines.sales.BSP_MarketingAds_Mart import (
    annotate_ads_daily_with_flow_tasks,
    build_ads_daily_mart,
    build_campaign_table,
    build_flow_ad_performance_mart,
    notify_missing_collection,
    parse_flow_campaigns,
)

NAVER_POWERLINK = "cmp-a001-01-000000010894818"
NAVER_PLACE = "cmp-a001-06-000000010947132"
DAANGN_GROUP = "도리당 송파삼전점 #3"


def _write_flow_posts(path, rows):
    columns = [
        "post_id",
        "project_id",
        "project_name",
        "store_name",
        "parent_post_id",
        "depth",
        "title",
        "content_text",
        "task_status",
        "start_dt",
        "end_dt",
        "post_url",
        "child_cnt",
        "author_name",
        "worker",
    ]
    path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).reindex(columns=columns, fill_value="").to_parquet(path / "part-0.parquet")


def _flow_post(post_id, title, start_dt="", end_dt="", status="진행", **extra):
    row = {
        "post_id": post_id,
        "project_id": "2926716",
        "project_name": "[브랜드 전략기획부] 직영점 성장전략",
        "store_name": "송파삼전점",
        "parent_post_id": "",
        "depth": 0,
        "title": title,
        "content_text": "",
        "task_status": status,
        "start_dt": start_dt,
        "end_dt": end_dt,
        "post_url": f"https://flow.team/l/{post_id}",
        "child_cnt": 0,
        "author_name": "조민준",
        "worker": "조민준",
    }
    row.update(extra)
    return row


def _naver_row(
    date,
    campaign_id,
    group_id,
    impressions,
    clicks,
    cost,
    status="운영 가능",
    ad_type="",
    keyword_id="",
    keyword="",
    creative_id="",
    creative="",
    depth_no="",
    campaign_name="파워링크#2607_광역",
    group_name=None,
    target_url="https://map.naver.com/",
    collect_url="https://ads.naver.com/manage/ad-accounts/1497096/sa/adgroups/grp",
):
    group_name = group_name if group_name is not None else (f"광고그룹 {group_id[-1]}" if group_id else "")
    row = {
        "store": "송파삼전점",
        "collected_date": date,
        "채널": "네이버 광고",
        "광고유형": ad_type,
        "depth번호": depth_no,
        "캠페인 ID": campaign_id,
        "캠페인 이름": campaign_name,
        "광고그룹 ID": group_id,
        "광고그룹 이름": group_name,
        "키워드 ID": keyword_id,
        "키워드": keyword,
        "소재 ID": creative_id,
        "소재": creative,
        "상태": status,
        "기본 입찰가": "500",
        "URL": target_url,
        "수집URL": collect_url,
        "노출수": impressions,
        "클릭수": clicks,
        "클릭률(%)": "1.26 %",
        "평균 CPC": "98",
        "총비용": cost,
        "총 재생수": "",
        "평균 CPV": "",
    }
    return row


def _daangn_row(
    date,
    ad_id,
    group,
    impressions,
    clicks,
    cost,
    placement="비즈프로필 소식",
    url="https://business.daangn.com/ad/dg",
):
    return {
        "collected_at": "2026-08-26T02:10:36.532Z",
        "시작일": date,
        "종료일": date,
        "campaign_id": ad_id,
        "광고그룹명": group,
        "상태": "광고중",
        "광고명": f"광고 문구 {ad_id}",
        "게재위치": placement,
        "노출수": impressions,
        "클릭수": clicks,
        "클릭률": "1.01",
        "지출": cost,
        "스위치": "ON",
        "url": url,
    }


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


@pytest.fixture
def sources(tmp_path):
    naver_dir = tmp_path / "analytics" / "naver" / "naver_ad"
    daangn_csv = tmp_path / "analytics" / "Daangn_ads" / "daangn_ads.csv"
    _write_csv(
        naver_dir / "naverads_adgroups_2026.csv",
        [
            # 같은 캠페인의 광고그룹 2개 → 롤업 없이 2행으로 남아야 한다
            _naver_row("2026-08-06", NAVER_POWERLINK, "grp-1", "1,192", "15", "1,476"),
            _naver_row("2026-08-06", NAVER_POWERLINK, "grp-2", "752", "5", "516"),
            _naver_row("2026-08-07", NAVER_POWERLINK, "grp-1", "500", "4", "400", status="중지: 그룹 예산 도달"),
            _naver_row("2026-08-07", NAVER_POWERLINK, "grp-2", "0", "0", "0", status="중지: 그룹 OFF"),
            # 프로젝트 기간 밖
            _naver_row("2026-08-20", NAVER_POWERLINK, "grp-1", "100", "1", "200"),
            # 플레이스 캠페인
            _naver_row("2026-08-06", NAVER_PLACE, "grp-9", "300", "3", "900"),
        ],
    )
    _write_csv(
        daangn_csv,
        [
            _daangn_row("2026-08-06", "dg_a", DAANGN_GROUP, "497", "5", "825"),
            _daangn_row("2026-08-06", "dg_b", DAANGN_GROUP, "54", "2", "339"),
            _daangn_row("2026-08-07", "dg_a", DAANGN_GROUP, "100", "1", "150"),
        ],
    )
    return {"naver_dir": naver_dir, "daangn_csv": daangn_csv}


def _campaign(campaign_key, channel, link_key, start_date, end_date, **extra):
    row = {
        "campaign_key": campaign_key,
        "channel": channel,
        "link_key": link_key,
        "project_name": f"{campaign_key} 프로젝트",
        "store_name": "송파삼전점",
        "status": "진행",
        "start_date": start_date,
        "end_date": end_date,
        "flow_post_id": campaign_key,
        "flow_url": f"https://flow.team/l/{campaign_key}",
        "owner": "조민준",
        "match_source": "flow",
        "project_id": "2926716",
    }
    row.update(extra)
    return row


def _run(tmp_path, sources, payload):
    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    campaign_path = tmp_path / "mart" / "marketing_ads_campaign.csv"
    daily_result = json.loads(
        build_ads_daily_mart(
            payload,
            naver_dir=sources["naver_dir"],
            daangn_csv=sources["daangn_csv"],
            output_path=daily_path,
        )
    )
    campaign_result = json.loads(
        build_campaign_table(payload, daily_path=daily_path, output_path=campaign_path)
    )
    daily = pd.read_csv(daily_path, encoding="utf-8-sig").fillna("")
    campaigns = pd.read_csv(campaign_path, encoding="utf-8-sig").fillna("")
    return daily_result, campaign_result, daily, campaigns


# ------------------------------------------------------------------
# Flow 파싱
# ------------------------------------------------------------------
def test_parse_flow_campaigns_accepts_rule_variants(tmp_path):
    post_dir = tmp_path / "flow_post"
    _write_flow_posts(
        post_dir,
        [
            _flow_post("p1", f"[네이버_광고] [campaign_id:{NAVER_POWERLINK}] 송파삼전점 복날", "20260806", "20260812"),
            _flow_post("p2", f"[네이버 광고] [campaign_id:{NAVER_PLACE}] 송파삼전점 주말", "20260801", "20260831"),
            _flow_post("p3", f"[당근_광고] [campaign_id:{DAANGN_GROUP}] 삼전점 오픈", "20260806", "20260812"),
            _flow_post("p4", "송파삼전점 광고계정 생성", "20260806", "20260812"),
            _flow_post("p5", "[네이버_광고] 캠페인 아이디 없는 등록", "20260806", "20260812"),
        ],
    )

    payload = parse_flow_campaigns(post_dir=post_dir, manual_csv=tmp_path / "없는파일.csv")
    campaigns = {row["campaign_key"]: row for row in payload["campaigns"]}

    assert set(campaigns) == {"p1", "p2", "p3"}
    assert campaigns["p1"]["channel"] == "네이버"
    assert campaigns["p1"]["link_key"] == NAVER_POWERLINK
    assert campaigns["p1"]["project_name"] == "송파삼전점 복날"
    assert campaigns["p1"]["start_date"] == "2026-08-06"
    assert campaigns["p1"]["end_date"] == "2026-08-12"
    assert campaigns["p1"]["match_source"] == "flow"
    assert campaigns["p3"]["channel"] == "당근"
    assert campaigns["p3"]["link_key"] == DAANGN_GROUP


def test_parse_flow_campaigns_falls_back_to_body_dates(tmp_path):
    post_dir = tmp_path / "flow_post"
    _write_flow_posts(
        post_dir,
        [
            _flow_post(
                "p1",
                f"[네이버_광고] [campaign_id:{NAVER_POWERLINK}] 송파삼전점 복날",
                content_text="광고 목적: 복날\n시작일: 2026-08-06\n마감일: 2026-08-12",
            )
        ],
    )

    campaign = parse_flow_campaigns(post_dir=post_dir)["campaigns"][0]

    assert campaign["start_date"] == "2026-08-06"
    assert campaign["end_date"] == "2026-08-12"


# ------------------------------------------------------------------
# 계층 정렬
# ------------------------------------------------------------------
def test_daily_mart_keeps_channel_hierarchy(tmp_path, sources):
    _, _, daily, _ = _run(tmp_path, sources, {"campaigns": []})

    naver = daily[daily["channel"] == "네이버"]
    assert set(naver["campaign_id"]) == {NAVER_POWERLINK, NAVER_PLACE}
    assert set(naver["campaign_id_kind"]) == {"id"}
    assert all(value.startswith("grp-") for value in naver["adgroup_id"])
    # 네이버 원본에는 소재 레벨이 없어 광고그룹이 최소 단위다
    assert all(value.startswith("grp-") for value in naver["ad_id"])
    assert list(naver["ad_id"]) == list(naver["adgroup_id"])
    assert list(naver["ad_name"]) == list(naver["adgroup_name"])
    assert set(naver["ad_level"]) == {"adgroup"}
    assert set(naver["leaf_depth"]) == {2}
    assert list(naver["leaf_id"]) == list(naver["ad_id"])

    daangn = daily[daily["channel"] == "당근"]
    # 당근은 ID가 없어 광고그룹명이 캠페인 식별자를 겸한다
    assert set(daangn["campaign_id"]) == {DAANGN_GROUP}
    assert set(daangn["campaign_id_kind"]) == {"name"}
    assert set(daangn["campaign_name"]) == {DAANGN_GROUP}
    # 당근에는 광고그룹 레벨이 실제로 없다
    assert set(daangn["adgroup_id"]) == {""}
    assert all(value.startswith("dg_") for value in daangn["ad_id"])
    assert all(value.startswith("광고 문구 dg_") for value in daangn["ad_name"])
    assert set(daangn["ad_level"]) == {"ad"}
    assert set(daangn["depth1_id"]) == {DAANGN_GROUP}
    assert set(daangn["depth2_id"]) == set(daangn["ad_id"])
    assert set(daangn["depth3_id"]) == {""}
    assert set(daangn["leaf_depth"]) == {2}
    assert list(daangn["leaf_id"]) == list(daangn["ad_id"])

    # ad_id는 양 채널 모두 채워지고, link_key는 campaign_id와 같다
    assert "" not in set(daily["ad_id"])
    assert list(daily["link_key"]) == list(daily["campaign_id"])
    assert set(daily["link_level"]) == {"campaign"}
    assert daily.columns[-1] == "url"
    assert set(naver["url"]) == {"https://ads.naver.com/manage/ad-accounts/1497096/sa/adgroups/grp"}
    assert set(daangn["url"]) == {"https://business.daangn.com/ad/dg"}


def test_daily_mart_fills_naver_campaign_url_when_source_url_blank(sources, tmp_path):
    campaign_id = "cmp-a001-01-000000010789025"
    _write_csv(
        sources["naver_dir"] / "naverads_adgroups_2026.csv",
        [
            _naver_row(
                "2026-07-01",
                campaign_id,
                "",
                "0",
                "0",
                "0",
                status="중지",
                campaign_name="⛔ 파워링크#2_세부",
                group_name="",
                target_url="",
                collect_url="",
            )
        ],
    )

    _, _, daily, _ = _run(tmp_path, sources, {"campaigns": []})

    row = daily[daily["campaign_id"] == campaign_id].iloc[0]
    assert row["url"] == (
        "https://ads.naver.com/manage/ad-accounts/1497096/sa/campaigns/"
        "cmp-a001-01-000000010789025?startDate=2026-07-01&endDate=2026-07-01"
        "&dateRange=2026-07-01%2C2026-07-01"
    )


def test_ad_type_is_derived_from_campaign_id_prefix(tmp_path, sources):
    _, _, daily, _ = _run(tmp_path, sources, {"campaigns": []})

    by_campaign = dict(zip(daily["campaign_id"], daily["ad_type"]))
    assert by_campaign[NAVER_POWERLINK] == "파워링크"
    assert by_campaign[NAVER_PLACE] == "플레이스"
    assert set(daily[daily["channel"] == "당근"]["ad_type"]) == {"비즈프로필 소식"}


def test_daily_mart_preserves_detail_grain_and_recalculates_rates(tmp_path, sources):
    payload = {"campaigns": [_campaign("p1", "네이버", NAVER_POWERLINK, "2026-08-06", "2026-08-12")]}

    _, _, daily, _ = _run(tmp_path, sources, payload)

    # 같은 날 같은 캠페인의 광고그룹 2개는 롤업되지 않고 2행으로 남는다
    same_day = daily[
        (daily["channel"] == "네이버")
        & (daily["stat_date"] == "2026-08-06")
        & (daily["campaign_id"] == NAVER_POWERLINK)
    ]
    assert len(same_day) == 2
    assert set(same_day["adgroup_id"]) == {"grp-1", "grp-2"}

    grp1 = same_day[same_day["adgroup_id"] == "grp-1"].iloc[0]
    assert grp1["impressions"] == 1192
    assert grp1["cost"] == 1476
    # CTR/CPC는 원본 '1.26 %', '98'을 그대로 쓰지 않고 행 단위로 재계산한다
    assert grp1["ctr"] == round(15 / 1192 * 100, 2)
    assert grp1["cpc"] == round(1476 / 15, 2)
    assert grp1["project_name"] == "p1 프로젝트"
    assert bool(grp1["in_period"]) is True


def test_daily_mart_reads_naver_keyword_depth_rows(tmp_path, sources):
    naver_dir = tmp_path / "keyword_naver"
    _write_csv(
        naver_dir / "naverads_adgroups_keyword.csv",
        [
            _naver_row(
                "2026-08-06",
                NAVER_POWERLINK,
                "grp-1",
                "100",
                "10",
                "1,000",
                ad_type="파워링크",
                keyword_id="kwd-1",
                keyword="가족",
                depth_no="3",
            ),
            _naver_row(
                "2026-08-06",
                NAVER_POWERLINK,
                "grp-1",
                "80",
                "4",
                "400",
                ad_type="파워링크",
                keyword_id="kwd-2",
                keyword="외식",
                depth_no="3",
            ),
        ],
    )

    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    build_ads_daily_mart(
        {"campaigns": [_campaign("p1", "네이버", NAVER_POWERLINK, "2026-08-06", "2026-08-06")]},
        naver_dir=naver_dir,
        daangn_csv=sources["daangn_csv"],
        output_path=daily_path,
    )

    daily = pd.read_csv(daily_path, encoding="utf-8-sig").fillna("")
    naver = daily[daily["channel"] == "네이버"].sort_values("ad_id")

    assert list(naver["ad_id"]) == ["kwd-1", "kwd-2"]
    assert list(naver["ad_name"]) == ["가족", "외식"]
    assert set(naver["adgroup_id"]) == {"grp-1"}
    assert set(naver["ad_level"]) == {"keyword"}
    assert set(naver["ad_type"]) == {"파워링크"}
    assert naver["cost"].sum() == 1400


def test_daily_mart_reads_naver_place_creative_depth_rows(tmp_path, sources):
    naver_dir = tmp_path / "place_creative_naver"
    _write_csv(
        naver_dir / "naverads_adgroups_place.csv",
        [
            _naver_row(
                "2026-08-06",
                NAVER_PLACE,
                "grp-place-1",
                "30",
                "3",
                "900",
                ad_type="플레이스",
                creative_id="creative-1",
                creative="10시 ~ 11시 소재",
                depth_no="3",
            ),
            _naver_row(
                "2026-08-06",
                NAVER_PLACE,
                "grp-place-1",
                "20",
                "1",
                "300",
                ad_type="플레이스",
                creative_id="creative-2",
                creative="11시 ~ 12시 소재",
                depth_no="3",
            ),
        ],
    )

    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    build_ads_daily_mart(
        {"campaigns": [_campaign("p1", "네이버", NAVER_PLACE, "2026-08-06", "2026-08-06")]},
        naver_dir=naver_dir,
        daangn_csv=sources["daangn_csv"],
        output_path=daily_path,
    )

    daily = pd.read_csv(daily_path, encoding="utf-8-sig").fillna("")
    naver = daily[daily["channel"] == "네이버"].sort_values("ad_id")

    assert list(naver["ad_id"]) == ["creative-1", "creative-2"]
    assert list(naver["ad_name"]) == ["10시 ~ 11시 소재", "11시 ~ 12시 소재"]
    assert set(naver["adgroup_id"]) == {"grp-place-1"}
    assert set(naver["ad_level"]) == {"creative"}
    assert set(naver["ad_type"]) == {"플레이스"}
    assert naver["cost"].sum() == 1200


def test_daily_mart_reads_new_naver_file_pattern_by_default(tmp_path, sources):
    naver_dir = tmp_path / "new_naver_pattern"
    _write_csv(
        naver_dir / "naver_ads_group_2026.csv",
        [
            _naver_row(
                "2026-08-06",
                NAVER_POWERLINK,
                "grp-1",
                "100",
                "10",
                "1,000",
                ad_type="파워링크",
                keyword_id="kwd-1",
                keyword="가족",
                depth_no="depth3",
            ),
        ],
    )

    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    build_ads_daily_mart({"campaigns": []}, naver_dir=naver_dir, daangn_csv=sources["daangn_csv"], output_path=daily_path)

    daily = pd.read_csv(daily_path, encoding="utf-8-sig").fillna("")
    naver = daily[daily["channel"] == "네이버"].iloc[0]

    assert naver["ad_id"] == "kwd-1"
    assert naver["depth3_id"] == "kwd-1"
    assert str(naver["leaf_depth"]) == "3"


def test_daily_mart_keeps_naver_variable_depth_rows(tmp_path, sources):
    naver_dir = tmp_path / "variable_depth_naver"
    _write_csv(
        naver_dir / "naver_ads_group_2026.csv",
        [
            _naver_row(
                "2026-08-06",
                NAVER_POWERLINK,
                "",
                "0",
                "0",
                "0",
                ad_type="파워링크",
                depth_no="depth1",
                campaign_name="캠페인만 있는 행",
            ),
            _naver_row(
                "2026-08-06",
                NAVER_PLACE,
                "grp-place-1",
                "30",
                "3",
                "900",
                ad_type="플레이스",
                depth_no="depth2",
                campaign_name="플레이스 캠페인",
                group_name="광고그룹 1",
            ),
            _naver_row(
                "2026-08-06",
                NAVER_PLACE,
                "grp-place-2",
                "20",
                "1",
                "300",
                ad_type="플레이스",
                creative_id="creative-1",
                creative="11시 소재",
                depth_no="depth3",
                campaign_name="플레이스 캠페인",
                group_name="광고그룹 2",
            ),
        ],
    )

    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    build_ads_daily_mart({"campaigns": []}, naver_dir=naver_dir, daangn_csv=sources["daangn_csv"], output_path=daily_path)

    daily = pd.read_csv(daily_path, encoding="utf-8-sig").fillna("")
    naver = daily[daily["channel"] == "네이버"].sort_values("source_depth_level")
    by_depth = {str(row["source_depth_level"]): row for row in naver.to_dict("records")}

    depth1 = by_depth["1"]
    assert depth1["depth1_id"] == NAVER_POWERLINK
    assert depth1["depth2_id"] == ""
    assert depth1["depth3_id"] == ""
    assert str(depth1["leaf_depth"]) == "1"
    assert depth1["leaf_id"] == NAVER_POWERLINK
    assert depth1["ad_level"] == "campaign"

    depth2 = by_depth["2"]
    assert depth2["depth1_id"] == NAVER_PLACE
    assert depth2["depth2_id"] == "grp-place-1"
    assert depth2["depth3_id"] == ""
    assert str(depth2["leaf_depth"]) == "2"
    assert depth2["leaf_id"] == "grp-place-1"

    depth3 = by_depth["3"]
    assert depth3["depth1_id"] == NAVER_PLACE
    assert depth3["depth2_id"] == "grp-place-2"
    assert depth3["depth3_id"] == "creative-1"
    assert depth3["depth3_name"] == "11시 소재"
    assert str(depth3["leaf_depth"]) == "3"
    assert depth3["leaf_id"] == "creative-1"


def test_status_class_marks_budget_capped_and_paused(tmp_path, sources):
    _, _, daily, _ = _run(tmp_path, sources, {"campaigns": []})

    day2 = daily[(daily["channel"] == "네이버") & (daily["stat_date"] == "2026-08-07")]
    by_group = dict(zip(day2["adgroup_id"], day2["status_class"]))
    assert by_group["grp-1"] == "예산도달"
    assert by_group["grp-2"] == "중지"

    day1 = daily[(daily["channel"] == "네이버") & (daily["stat_date"] == "2026-08-06")]
    assert set(day1["status_class"]) == {"가동"}


# ------------------------------------------------------------------
# 미매칭 보존
# ------------------------------------------------------------------
def test_out_of_period_and_unregistered_rows_are_kept(tmp_path, sources):
    payload = {"campaigns": [_campaign("p1", "네이버", NAVER_POWERLINK, "2026-08-06", "2026-08-12")]}

    daily_result, _, daily, _ = _run(tmp_path, sources, payload)

    out_of_period = daily[
        (daily["stat_date"] == "2026-08-20") & (daily["campaign_id"] == NAVER_POWERLINK)
    ].iloc[0]
    assert out_of_period["project_name"] == "(기간외)"
    assert bool(out_of_period["in_period"]) is False

    unregistered = daily[daily["channel"] == "당근"].iloc[0]
    assert unregistered["project_name"] == "(미등록)"
    assert unregistered["store_name"] == "송파삼전점"

    # 광고비 누락 없음
    assert daily_result["matched_rows"] + daily_result["unmatched_rows"] == daily_result["rows"]
    assert daily["cost"].sum() == 1476 + 516 + 400 + 0 + 200 + 900 + 825 + 339 + 150


# ------------------------------------------------------------------
# 캠페인 테이블: Flow 매칭용 광고 ID 매핑
# ------------------------------------------------------------------
def test_campaign_table_exposes_max_depth_ids(tmp_path, sources):
    payload = {
        "campaigns": [
            _campaign("p1", "네이버", NAVER_POWERLINK, "2026-08-06", "2026-08-07"),
            _campaign("p9", "네이버", "cmp-없는캠페인", "2026-09-01", "2026-09-07"),
        ]
    }

    _, campaign_result, _, campaigns = _run(tmp_path, sources, payload)

    assert list(campaigns.columns) == [
        "channel",
        "id",
        "std_name",
        "name_01",
        "name_02",
        "name_03",
        "leaf_depths",
        "ad_type",
        "start_date",
        "end_date",
    ]
    assert campaign_result["rows"] == 3
    assert set(campaigns["id"]) == {"grp-1", "grp-2", "cmp-없는캠페인"}

    matched = campaigns[campaigns["id"] == "grp-1"].iloc[0]
    assert matched["channel"] == "네이버"
    assert matched["std_name"] == "광고그룹 1"
    assert matched["name_01"] == "파워링크#2607_광역"
    assert matched["name_02"] == "광고그룹 1"
    assert matched["name_03"] == ""
    assert str(matched["leaf_depths"]) == "2"
    assert matched["ad_type"] == "파워링크"
    assert matched["start_date"] == "2026-08-06"
    assert matched["end_date"] == "2026-08-07"

    empty = campaigns[campaigns["id"] == "cmp-없는캠페인"].iloc[0]
    assert empty["std_name"] == "p9 프로젝트"
    assert empty["name_01"] == "p9 프로젝트"
    assert str(empty["leaf_depths"]) == "1"


def test_campaign_table_uses_flow_period_for_mapping_rows(tmp_path, sources):
    payload = {"campaigns": [_campaign("p1", "네이버", NAVER_POWERLINK, "2026-08-06", "2026-08-10")]}

    _, _, _, campaigns = _run(tmp_path, sources, payload)

    assert set(campaigns["id"]) == {"grp-1", "grp-2"}
    assert set(campaigns["start_date"]) == {"2026-08-06"}
    assert set(campaigns["end_date"]) == {"2026-08-10"}


def test_campaign_table_maps_place_campaign_to_adgroup_depth(tmp_path, sources):
    payload = {"campaigns": [_campaign("p1", "네이버", NAVER_PLACE, "2026-08-06", "2026-08-07")]}

    _, _, _, campaigns = _run(tmp_path, sources, payload)
    row = campaigns.iloc[0]

    assert row["id"] == "grp-9"
    assert row["name_01"] == "파워링크#2607_광역"
    assert row["name_02"] == "광고그룹 9"
    assert str(row["leaf_depths"]) == "2"


def test_campaign_table_maps_daangn_to_ad_depth(tmp_path, sources):
    payload = {"campaigns": [_campaign("p1", "당근", DAANGN_GROUP, "2026-08-06", "2026-08-10")]}

    _, _, _, campaigns = _run(tmp_path, sources, payload)
    by_id = campaigns.set_index("id")

    assert set(by_id.index) == {"dg_a", "dg_b"}
    assert by_id.loc["dg_a", "name_01"] == DAANGN_GROUP
    assert by_id.loc["dg_a", "std_name"] == "광고 문구 dg_a"
    assert by_id.loc["dg_a", "name_02"] == "광고 문구 dg_a"
    assert by_id.loc["dg_a", "name_03"] == ""
    assert str(by_id.loc["dg_a", "leaf_depths"]) == "2"


def test_campaign_table_keeps_channel_specific_ids(tmp_path, sources):
    payload = {
        "campaigns": [
            _campaign("p1", "네이버", NAVER_POWERLINK, "2026-08-06", "2026-08-07"),
            _campaign("p2", "당근", DAANGN_GROUP, "2026-08-06", "2026-08-07"),
        ]
    }

    _, _, _, campaigns = _run(tmp_path, sources, payload)

    assert set(campaigns[campaigns["channel"] == "네이버"]["id"]) == {"grp-1", "grp-2"}
    assert set(campaigns[campaigns["channel"] == "당근"]["id"]) == {"dg_a", "dg_b"}


def test_overlapping_projects_are_not_double_counted(tmp_path, sources):
    payload = {
        "campaigns": [
            _campaign("p1", "네이버", NAVER_POWERLINK, "2026-08-01", "2026-08-12", project_name="먼저 등록"),
            _campaign("p2", "네이버", NAVER_POWERLINK, "2026-08-05", "2026-08-12", project_name="나중 등록"),
        ]
    }

    daily_result, _, daily, campaigns = _run(tmp_path, sources, payload)

    overlapped = daily[
        (daily["stat_date"] == "2026-08-06")
        & (daily["campaign_id"] == NAVER_POWERLINK)
    ]
    assert set(overlapped["project_name"]) == {"먼저 등록"}
    assert all(bool(flag) for flag in overlapped["overlap_flag"])
    assert daily_result["overlap_rows"] == 4
    fallback = campaigns[campaigns["name_01"] == "나중 등록"].iloc[0]
    assert fallback["id"] == NAVER_POWERLINK
    assert str(fallback["leaf_depths"]) == "1"


def test_zero_registered_campaigns_still_writes_both_files(tmp_path, sources):
    daily_result, campaign_result, daily, campaigns = _run(tmp_path, sources, {"campaigns": []})

    assert campaign_result["rows"] == 5
    assert set(campaigns["id"]) == {"grp-1", "grp-2", "grp-9", "dg_a", "dg_b"}
    assert daily_result["rows"] == len(daily) == 9
    assert set(daily["project_name"]) == {"(미등록)"}
    assert daily_result["unmatched_cost"] == daily_result["cost_total"]


def test_campaign_table_exposes_depth_names_for_source_tracking(tmp_path, sources):
    _, _, _, campaigns = _run(tmp_path, sources, {"campaigns": []})

    naver = campaigns[campaigns["id"] == "grp-1"].iloc[0]
    assert naver["name_01"] == "파워링크#2607_광역"
    assert naver["std_name"] == "광고그룹 1"
    assert naver["name_02"] == "광고그룹 1"
    assert naver["name_03"] == ""
    assert str(naver["leaf_depths"]) == "2"

    daangn = campaigns[campaigns["id"] == "dg_a"].iloc[0]
    assert daangn["name_01"] == DAANGN_GROUP
    assert daangn["std_name"] == "광고 문구 dg_a"
    assert daangn["name_02"] == "광고 문구 dg_a"
    assert daangn["name_03"] == ""
    assert str(daangn["leaf_depths"]) == "2"


def test_campaign_table_exposes_naver_depth3_keys(tmp_path, sources):
    naver_dir = tmp_path / "keyword_naver"
    _write_csv(
        naver_dir / "naver_ads_group_2026.csv",
        [
            _naver_row(
                "2026-08-06",
                NAVER_POWERLINK,
                "grp-1",
                "100",
                "10",
                "1,000",
                ad_type="파워링크",
                keyword_id="kwd-1",
                keyword="가족",
                depth_no="depth3",
            ),
            _naver_row(
                "2026-08-06",
                NAVER_POWERLINK,
                "grp-1",
                "80",
                "4",
                "400",
                ad_type="파워링크",
                keyword_id="kwd-2",
                keyword="외식",
                depth_no="depth3",
            ),
        ],
    )
    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    campaign_path = tmp_path / "mart" / "marketing_ads_campaign.csv"

    build_ads_daily_mart({"campaigns": []}, naver_dir=naver_dir, daangn_csv=sources["daangn_csv"], output_path=daily_path)
    build_campaign_table({"campaigns": []}, daily_path=daily_path, output_path=campaign_path)

    campaigns = pd.read_csv(campaign_path, encoding="utf-8-sig").fillna("")
    by_id = campaigns.set_index("id")
    assert set(by_id.index) >= {"kwd-1", "kwd-2"}
    assert by_id.loc["kwd-1", "name_01"] == "파워링크#2607_광역"
    assert by_id.loc["kwd-1", "name_02"] == "광고그룹 1"
    assert by_id.loc["kwd-1", "std_name"] == "가족"
    assert by_id.loc["kwd-1", "name_03"] == "가족"
    assert str(by_id.loc["kwd-1", "leaf_depths"]) == "3"


# ------------------------------------------------------------------
# Flow 광고 하위업무 성과 비교 mart
# ------------------------------------------------------------------
def test_flow_ad_performance_mart_compares_current_and_previous_period(tmp_path, sources):
    naver_dir = tmp_path / "flow_compare_naver"
    naver_ad_id = "nad-a001-06-000000544076694"
    _write_csv(
        naver_dir / "naver_ads_group_2026.csv",
        [
            _naver_row(
                "2026-08-16",
                NAVER_PLACE,
                "grp-9",
                "50",
                "5",
                "500",
                ad_type="플레이스",
                creative_id=naver_ad_id,
                creative="플레이스 문구",
                depth_no="depth3",
            ),
            _naver_row(
                "2026-08-17",
                NAVER_PLACE,
                "grp-9",
                "100",
                "10",
                "1,000",
                ad_type="플레이스",
                creative_id=naver_ad_id,
                creative="플레이스 문구",
                depth_no="depth3",
            ),
        ],
    )
    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    campaign_path = tmp_path / "mart" / "marketing_ads_campaign.csv"
    output_path = tmp_path / "mart" / "marketing_ads_flow_compare.csv"
    flow_post_dir = tmp_path / "flow_post"

    build_ads_daily_mart({"campaigns": []}, naver_dir=naver_dir, daangn_csv=sources["daangn_csv"], output_path=daily_path)
    build_campaign_table({"campaigns": []}, daily_path=daily_path, output_path=campaign_path)
    _write_flow_posts(
        flow_post_dir,
        [
            _flow_post(
                "84107459",
                "네이버광고",
                project_id="2960298",
                project_name="DB 광고 프로젝트 성과",
                post_url="https://flow.team/l/QqxSN",
                child_cnt=1,
            ),
            _flow_post(
                "84107449",
                "당근광고",
                project_id="2960298",
                project_name="DB 광고 프로젝트 성과",
                post_url="https://flow.team/l/QqxSI",
                child_cnt=1,
            ),
            _flow_post(
                "naver-child",
                f"[네이버광고] [{naver_ad_id}] 문구 교체test",
                "20260817",
                "20260817",
                project_id="2960298",
                project_name="DB 광고 프로젝트 성과",
                parent_post_id="84107459",
                depth=1,
                post_url="https://flow.team/l/naver-child",
            ),
            _flow_post(
                "daangn-child",
                "[당근광고] [dg_a] 광고 위치 교체 Test",
                "20260807",
                "20260807",
                project_id="2960298",
                project_name="DB 광고 프로젝트 성과",
                parent_post_id="84107449",
                depth=1,
                post_url="https://flow.team/l/daangn-child",
            ),
        ],
    )

    result = json.loads(
        build_flow_ad_performance_mart(
            flow_post_dir=flow_post_dir,
            campaign_path=campaign_path,
            daily_path=daily_path,
            output_path=output_path,
        )
    )
    compare = pd.read_csv(output_path, encoding="utf-8-sig").fillna("")
    by_id = compare.set_index("id")

    assert result["rows"] == 2
    assert by_id.loc[naver_ad_id, "channel"] == "네이버"
    assert by_id.loc[naver_ad_id, "std_name"] == "플레이스 문구"
    assert by_id.loc[naver_ad_id, "start_date"] == "2026-08-17"
    assert by_id.loc[naver_ad_id, "prev_start_date"] == "2026-08-16"
    assert by_id.loc[naver_ad_id, "impressions"] == 100
    assert by_id.loc[naver_ad_id, "prev_impressions"] == 50
    assert by_id.loc[naver_ad_id, "ctr"] == 10.0
    assert by_id.loc[naver_ad_id, "cpc"] == 100.0
    assert by_id.loc[naver_ad_id, "pct_cost"] == 100.0

    assert by_id.loc["dg_a", "channel"] == "당근"
    assert by_id.loc["dg_a", "parent_title"] == "당근광고"
    assert by_id.loc["dg_a", "impressions"] == 100
    assert by_id.loc["dg_a", "prev_impressions"] == 497
    assert by_id.loc["dg_a", "std_title"] == "광고 위치 교체 Test"


def test_flow_ad_performance_mart_allows_channel_typos_from_parent(tmp_path, sources):
    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    campaign_path = tmp_path / "mart" / "marketing_ads_campaign.csv"
    output_path = tmp_path / "mart" / "marketing_ads_flow_compare.csv"
    flow_post_dir = tmp_path / "flow_post"

    build_ads_daily_mart({"campaigns": []}, naver_dir=sources["naver_dir"], daangn_csv=sources["daangn_csv"], output_path=daily_path)
    build_campaign_table({"campaigns": []}, daily_path=daily_path, output_path=campaign_path)
    _write_flow_posts(
        flow_post_dir,
        [
            _flow_post(
                "84107449",
                "당근광고",
                project_id="2960298",
                project_name="DB 광고 프로젝트 성과",
                post_url="https://flow.team/l/QqxSI",
                child_cnt=1,
            ),
            _flow_post(
                "child",
                "(네이버광고] [dg_a] 채널 오타",
                "20260807",
                "20260807",
                project_id="2960298",
                project_name="DB 광고 프로젝트 성과",
                parent_post_id="84107449",
                depth=1,
            ),
        ],
    )

    build_flow_ad_performance_mart(
        flow_post_dir=flow_post_dir,
        campaign_path=campaign_path,
        daily_path=daily_path,
        output_path=output_path,
    )
    row = pd.read_csv(output_path, encoding="utf-8-sig").fillna("").iloc[0]

    assert row["channel"] == "당근"
    assert row["id"] == "dg_a"
    assert row["channel_warning"] == "제목 채널=네이버, 부모 채널=당근"


def test_flow_ad_performance_mart_fails_on_unknown_id_or_missing_dates(tmp_path, sources):
    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    campaign_path = tmp_path / "mart" / "marketing_ads_campaign.csv"
    flow_post_dir = tmp_path / "flow_post"

    build_ads_daily_mart({"campaigns": []}, naver_dir=sources["naver_dir"], daangn_csv=sources["daangn_csv"], output_path=daily_path)
    build_campaign_table({"campaigns": []}, daily_path=daily_path, output_path=campaign_path)
    _write_flow_posts(
        flow_post_dir,
        [
            _flow_post(
                "84107449",
                "당근광고",
                project_id="2960298",
                project_name="DB 광고 프로젝트 성과",
                post_url="https://flow.team/l/QqxSI",
                child_cnt=2,
            ),
            _flow_post(
                "bad-id",
                "[당근광고] [없는id] 광고 위치 교체",
                "20260807",
                "20260807",
                project_id="2960298",
                project_name="DB 광고 프로젝트 성과",
                parent_post_id="84107449",
                depth=1,
            ),
            _flow_post(
                "bad-date",
                "[당근광고] [dg_a] 날짜 누락",
                "20260807",
                "",
                project_id="2960298",
                project_name="DB 광고 프로젝트 성과",
                parent_post_id="84107449",
                depth=1,
            ),
        ],
    )

    with pytest.raises(RuntimeError, match="Flow 광고 하위업무 필수값 오류"):
        build_flow_ad_performance_mart(
            flow_post_dir=flow_post_dir,
            campaign_path=campaign_path,
            daily_path=daily_path,
            output_path=tmp_path / "out.csv",
        )


def test_annotate_ads_daily_with_flow_tasks_adds_labels_and_long_mart(tmp_path, sources):
    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    campaign_path = tmp_path / "mart" / "marketing_ads_campaign.csv"
    compare_path = tmp_path / "mart" / "marketing_ads_flow_compare.csv"
    daily_flow_tasks_path = tmp_path / "mart" / "marketing_ads_daily_flow_tasks.csv"
    flow_post_dir = tmp_path / "flow_post"

    build_ads_daily_mart({"campaigns": []}, naver_dir=sources["naver_dir"], daangn_csv=sources["daangn_csv"], output_path=daily_path)
    build_campaign_table({"campaigns": []}, daily_path=daily_path, output_path=campaign_path)
    _write_flow_posts(
        flow_post_dir,
        [
            _flow_post(
                "84107449",
                "당근광고",
                project_id="2960298",
                project_name="DB 광고 프로젝트 성과",
                post_url="https://flow.team/l/QqxSI",
                child_cnt=1,
            ),
            _flow_post(
                "child",
                "[당근광고] [dg_a] 문구교체 Test",
                "20260807",
                "20260808",
                project_id="2960298",
                project_name="DB 광고 프로젝트 성과",
                parent_post_id="84107449",
                depth=1,
                post_url="https://flow.team/l/child",
            ),
        ],
    )
    build_flow_ad_performance_mart(
        flow_post_dir=flow_post_dir,
        campaign_path=campaign_path,
        daily_path=daily_path,
        output_path=compare_path,
    )

    result = json.loads(
        annotate_ads_daily_with_flow_tasks(
            daily_path=daily_path,
            flow_compare_path=compare_path,
            campaign_path=campaign_path,
            daily_flow_tasks_path=daily_flow_tasks_path,
        )
    )
    daily = pd.read_csv(daily_path, encoding="utf-8-sig").fillna("")
    tasks = pd.read_csv(daily_flow_tasks_path, encoding="utf-8-sig").fillna("")

    assert result["task_rows"] == 2
    assert result["placeholder_rows"] == 1
    assert len(tasks) == 2
    assert set(tasks["stat_date"]) == {"2026-08-07", "2026-08-08"}
    assert tasks.iloc[0]["std_title"] == "문구교체 Test"
    assert tasks.iloc[0]["flow_task_label"] == "07-08 [dg_a] 문구교체 Test"
    assert tasks.iloc[0]["FLOW_LINK"] == "https://flow.team/l/child"
    assert tasks.iloc[0]["FLOW_TITLE"] == "[당근광고] [dg_a] 문구교체 Test"

    day7 = daily[(daily["channel"] == "당근") & (daily["stat_date"] == "2026-08-07") & (daily["ad_id"] == "dg_a")].iloc[0]
    assert day7["flow_task_label"] == "07-08 [dg_a] 문구교체 Test"
    assert day7["FLOW_LINK"] == "https://flow.team/l/child"
    assert day7["FLOW_TITLE"] == "[당근광고] [dg_a] 문구교체 Test"
    assert day7["cost"] == 150

    day8 = daily[(daily["channel"] == "당근") & (daily["stat_date"] == "2026-08-08") & (daily["ad_id"] == "dg_a")].iloc[0]
    assert day8["status_class"] == "flow_schedule_only"
    assert day8["flow_task_label"] == "07-08 [dg_a] 문구교체 Test"
    assert day8["FLOW_LINK"] == "https://flow.team/l/child"
    assert day8["FLOW_TITLE"] == "[당근광고] [dg_a] 문구교체 Test"
    assert day8["cost"] == 0


def test_annotate_ads_daily_with_flow_tasks_merges_overlapping_labels(tmp_path, sources):
    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    campaign_path = tmp_path / "mart" / "marketing_ads_campaign.csv"
    compare_path = tmp_path / "mart" / "marketing_ads_flow_compare.csv"
    daily_flow_tasks_path = tmp_path / "mart" / "marketing_ads_daily_flow_tasks.csv"

    build_ads_daily_mart({"campaigns": []}, naver_dir=sources["naver_dir"], daangn_csv=sources["daangn_csv"], output_path=daily_path)
    build_campaign_table({"campaigns": []}, daily_path=daily_path, output_path=campaign_path)
    pd.DataFrame(
        [
            {
                "project_id": "2960298",
                "project_name": "DB 광고 프로젝트 성과",
                "parent_post_id": "84107449",
                "parent_title": "당근광고",
                "flow_post_id": "child-1",
                "flow_url": "https://flow.team/l/child-1",
                "channel": "당근",
                "id": "dg_a",
                "std_name": "광고 문구 dg_a",
                "std_title": "문구교체 Test",
                "task_title": "[당근광고] [dg_a] 문구교체 Test",
                "channel_warning": "",
                "start_date": "2026-08-07",
                "end_date": "2026-08-07",
            },
            {
                "project_id": "2960298",
                "project_name": "DB 광고 프로젝트 성과",
                "parent_post_id": "84107449",
                "parent_title": "당근광고",
                "flow_post_id": "child-2",
                "flow_url": "https://flow.team/l/child-2",
                "channel": "당근",
                "id": "dg_a",
                "std_name": "광고 문구 dg_a",
                "std_title": "광고위치 Test",
                "task_title": "[당근광고] [dg_a] 광고위치 Test",
                "channel_warning": "",
                "start_date": "2026-08-07",
                "end_date": "2026-08-07",
            },
        ]
    ).to_csv(compare_path, index=False, encoding="utf-8-sig")

    annotate_ads_daily_with_flow_tasks(
        daily_path=daily_path,
        flow_compare_path=compare_path,
        campaign_path=campaign_path,
        daily_flow_tasks_path=daily_flow_tasks_path,
    )
    daily = pd.read_csv(daily_path, encoding="utf-8-sig").fillna("")
    tasks = pd.read_csv(daily_flow_tasks_path, encoding="utf-8-sig").fillna("")
    row = daily[(daily["channel"] == "당근") & (daily["stat_date"] == "2026-08-07") & (daily["ad_id"] == "dg_a")].iloc[0]

    assert len(tasks) == 2
    assert row["flow_task_label"] == "07-07 [dg_a] 문구교체 Test | 07-07 [dg_a] 광고위치 Test"
    assert row["FLOW_LINK"] == "https://flow.team/l/child-1 | https://flow.team/l/child-2"
    assert row["FLOW_TITLE"] == "[당근광고] [dg_a] 문구교체 Test | [당근광고] [dg_a] 광고위치 Test"


# ------------------------------------------------------------------
# 수동 보정 CSV
# ------------------------------------------------------------------
def _manual_row(**extra):
    row = {column: "" for column in
           ["channel", "link_key", "flow_post_id", "project_name", "start_date", "end_date", "status", "store_name", "memo"]}
    row.update(extra)
    return row


def test_manual_link_overrides_flow_typo(tmp_path):
    post_dir = tmp_path / "flow_post"
    _write_flow_posts(
        post_dir,
        [_flow_post("p1", "[네이버_광고] [campaign_id:cmp-오타난값] 송파삼전점 복날", "20260806", "20260812")],
    )
    manual_csv = tmp_path / "campaign_link_manual.csv"
    _write_csv(
        manual_csv,
        [_manual_row(channel="네이버", link_key=NAVER_POWERLINK, flow_post_id="p1", memo="오타 보정")],
    )

    payload = parse_flow_campaigns(post_dir=post_dir, manual_csv=manual_csv)
    campaign = payload["campaigns"][0]

    assert len(payload["campaigns"]) == 1
    assert campaign["link_key"] == NAVER_POWERLINK
    assert campaign["project_name"] == "송파삼전점 복날"  # Flow 값 유지
    assert campaign["match_source"] == "manual"
    assert payload["manual_overridden"] == 1


def test_manual_link_adds_campaign_without_flow_post(tmp_path, sources):
    post_dir = tmp_path / "flow_post"
    _write_flow_posts(post_dir, [_flow_post("p4", "규칙에 안 맞는 제목")])
    manual_csv = tmp_path / "campaign_link_manual.csv"
    _write_csv(
        manual_csv,
        [
            _manual_row(
                channel="당근",
                link_key=DAANGN_GROUP,
                project_name="과거 당근 광고",
                start_date="2026-08-06",
                end_date="2026-08-07",
                status="완료",
                store_name="송파삼전점",
            )
        ],
    )

    payload = parse_flow_campaigns(post_dir=post_dir, manual_csv=manual_csv)
    assert payload["manual_added"] == 1

    _, _, daily, campaigns = _run(tmp_path, sources, payload)

    assert len(campaigns) == 2
    assert set(campaigns["id"]) == {"dg_a", "dg_b"}
    row = campaigns[campaigns["id"] == "dg_a"].iloc[0]
    assert row["std_name"] == "광고 문구 dg_a"
    assert row["name_01"] == DAANGN_GROUP
    assert row["name_02"] == "광고 문구 dg_a"
    assert row["start_date"] == "2026-08-06"
    assert row["end_date"] == "2026-08-07"
    assert set(daily[daily["channel"] == "당근"]["project_name"]) == {"과거 당근 광고"}


# ------------------------------------------------------------------
# 네이버 미수집 알림
# ------------------------------------------------------------------
def _recording_sender(store):
    """send_telegram_chunks와 같이 발송 성공 시 True를 돌려주는 테스트용 sender."""

    def sender(message):
        store.append(message)
        return True

    return sender


def _alert_setup(tmp_path, sources, today):
    """마트를 만들고 알림 함수 호출에 필요한 인자를 돌려준다."""
    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    build_ads_daily_mart(
        {"campaigns": []},
        naver_dir=sources["naver_dir"],
        daangn_csv=sources["daangn_csv"],
        output_path=daily_path,
    )
    return {
        "daily_path": daily_path,
        "state_path": tmp_path / "state" / "marketing_ads_missing_alert.json",
        "naver_dir": sources["naver_dir"],
        "today": today,
    }


def test_notify_reports_gap_inside_collected_range(tmp_path, sources):
    # 네이버 수집일은 8/6, 8/7, 8/20 → 8/8~8/19가 구간 공백
    sent = []
    result = json.loads(
        notify_missing_collection(alert_sender=_recording_sender(sent), **_alert_setup(tmp_path, sources, "2026-08-20"))
    )

    assert result["total_missing"] == 12
    assert result["new_missing"][0] == "2026-08-08"
    assert result["new_missing"][-1] == "2026-08-19"
    assert len(sent) == 1
    message = sent[0]
    assert "[네이버 광고 수집 누락]" in message
    assert "2026-08-08~2026-08-19" in message
    assert "naverads_adgroups_*.csv" in message
    assert "marketing_ads_daily.csv" in message


def test_notify_detects_collection_lag(tmp_path, sources):
    # 마지막 수집일이 8/20인데 전일 기준이 8/22 → 8/21·8/22가 지연으로 잡힌다
    sent = []
    result = json.loads(
        notify_missing_collection(alert_sender=_recording_sender(sent), **_alert_setup(tmp_path, sources, "2026-08-22"))
    )

    assert "2026-08-21" in result["new_missing"]
    assert "2026-08-22" in result["new_missing"]
    assert result["telegram_sent"] is True


def test_notify_does_not_repeat_already_sent_dates(tmp_path, sources):
    setup = _alert_setup(tmp_path, sources, "2026-08-20")
    first = []
    notify_missing_collection(alert_sender=_recording_sender(first), **setup)
    assert len(first) == 1

    # 같은 기준일로 다시 실행하면 신규 누락일이 없어 발송하지 않는다
    second = []
    result = json.loads(notify_missing_collection(alert_sender=_recording_sender(second), **setup))
    assert second == []
    assert result["new_missing"] == []
    assert result["telegram_sent"] is None

    # 기준일이 하루 밀리면 그날치만 새로 알린다
    setup["today"] = "2026-08-21"
    third = []
    result = json.loads(notify_missing_collection(alert_sender=_recording_sender(third), **setup))
    assert result["new_missing"] == ["2026-08-21"]
    assert len(third) == 1


def test_notify_skips_when_no_missing_dates(tmp_path, sources):
    # 전일 기준이 마지막 수집일과 같으면 누락이 없다... 8/6~8/7 구간만 보도록 8/7 기준
    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    naver_dir = tmp_path / "only_two_days"
    _write_csv(
        naver_dir / "naverads_adgroups_2026.csv",
        [
            _naver_row("2026-08-06", NAVER_POWERLINK, "grp-1", "10", "1", "100"),
            _naver_row("2026-08-07", NAVER_POWERLINK, "grp-1", "10", "1", "100"),
        ],
    )
    build_ads_daily_mart(
        {"campaigns": []},
        naver_dir=naver_dir,
        daangn_csv=sources["daangn_csv"],
        output_path=daily_path,
    )

    sent = []
    result = json.loads(
        notify_missing_collection(
            daily_path=daily_path,
            state_path=tmp_path / "state.json",
            naver_dir=naver_dir,
            today="2026-08-07",
            alert_sender=_recording_sender(sent),
        )
    )

    assert result["total_missing"] == 0
    assert sent == []
    assert not (tmp_path / "state.json").exists()


def test_notify_retries_when_telegram_fails(tmp_path, sources):
    setup = _alert_setup(tmp_path, sources, "2026-08-20")

    def failing_sender(_message):
        raise RuntimeError("텔레그램 토큰 없음")

    result = json.loads(notify_missing_collection(alert_sender=failing_sender, **setup))
    assert result["telegram_sent"] is False
    # 발송 실패 시 state를 남기지 않아 다음 실행에서 다시 시도한다
    assert not setup["state_path"].exists()

    retried = []
    result = json.loads(notify_missing_collection(alert_sender=_recording_sender(retried), **setup))
    assert len(retried) == 1
    assert result["telegram_sent"] is True


def test_notify_ignores_daangn_gaps(tmp_path, sources):
    # 당근은 8/6·8/7만 있고 event 소스라 알림 대상이 아니다
    sent = []
    result = json.loads(
        notify_missing_collection(alert_sender=_recording_sender(sent), **_alert_setup(tmp_path, sources, "2026-08-20"))
    )

    assert result["channel"] == "네이버"
    assert "당근" not in sent[0]


# ------------------------------------------------------------------
# 채널 규격 통일 (ad_id)
# ------------------------------------------------------------------
def test_ad_id_is_filled_for_both_channels(tmp_path, sources):
    _, _, daily, _ = _run(tmp_path, sources, {"campaigns": []})

    naver = daily[daily["channel"] == "네이버"]
    daangn = daily[daily["channel"] == "당근"]

    # 빈 ad_id가 하나도 없어야 한 컬럼으로 양 채널을 집계할 수 있다
    assert (daily["ad_id"] != "").all()
    assert naver["ad_id"].nunique() == 3  # grp-1, grp-2, grp-9
    assert daangn["ad_id"].nunique() == 2  # dg_a, dg_b
    assert dict(daily["ad_level"].value_counts()) == {"adgroup": len(naver), "ad": len(daangn)}


def test_campaign_id_kind_marks_name_based_key(tmp_path, sources):
    _, _, daily, _ = _run(tmp_path, sources, {"campaigns": []})

    kinds = daily.groupby("channel")["campaign_id_kind"].agg(set).to_dict()
    assert kinds["네이버"] == {"id"}
    assert kinds["당근"] == {"name"}
    # 이름을 키로 쓰는 채널도 campaign_id가 비지 않는다
    assert (daily["campaign_id"] != "").all()


def test_unified_schema_keeps_project_totals(tmp_path, sources):
    """ad_id 통일이 프로젝트 매핑의 최소 단위를 바꾸지 않는다."""
    payload = {
        "campaigns": [
            _campaign("p1", "네이버", NAVER_POWERLINK, "2026-08-06", "2026-08-07"),
            _campaign("p2", "당근", DAANGN_GROUP, "2026-08-06", "2026-08-07"),
        ]
    }

    _, _, _, campaigns = _run(tmp_path, sources, payload)

    assert set(campaigns[campaigns["channel"] == "네이버"]["id"]) == {"grp-1", "grp-2"}
    assert set(campaigns[campaigns["channel"] == "당근"]["id"]) == {"dg_a", "dg_b"}
    assert set(campaigns["leaf_depths"].astype(str)) == {"2"}


def test_campaign_without_data_still_has_channel_columns(tmp_path, sources):
    """실적 행이 없어도 채널로 결정되는 컬럼은 비우지 않는다."""
    payload = {
        "campaigns": [
            _campaign("p9", "당근", "돌리지 않은 광고그룹", "2026-09-01", "2026-09-07"),
            _campaign("p8", "네이버", "cmp-없는캠페인", "2026-09-01", "2026-09-07"),
        ]
    }

    _, _, _, campaigns = _run(tmp_path, sources, payload)
    by_id = campaigns.set_index("id")

    assert by_id.loc["돌리지 않은 광고그룹", "channel"] == "당근"
    assert by_id.loc["돌리지 않은 광고그룹", "std_name"] == "p9 프로젝트"
    assert by_id.loc["돌리지 않은 광고그룹", "name_01"] == "p9 프로젝트"
    assert str(by_id.loc["돌리지 않은 광고그룹", "leaf_depths"]) == "1"
    assert by_id.loc["cmp-없는캠페인", "channel"] == "네이버"
    assert by_id.loc["cmp-없는캠페인", "std_name"] == "p8 프로젝트"
    assert by_id.loc["cmp-없는캠페인", "name_01"] == "p8 프로젝트"
    assert str(by_id.loc["cmp-없는캠페인", "leaf_depths"]) == "1"


def test_notify_flags_dates_with_identical_metrics(tmp_path, sources):
    """날짜 필터 미적용으로 같은 기간이 여러 날짜에 복제된 경우를 잡는다.

    stat_date가 서로 달라 drop_duplicates로는 걸리지 않고, 날짜도 다 채워져 있어
    누락 점검도 통과한다. 2026-08 네이버 기간수집 사고가 정확히 이 형태였다.
    """
    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    naver_dir = tmp_path / "cloned_days"
    rows = []
    for day in ("2026-08-05", "2026-08-06", "2026-08-07"):
        # 세 날짜가 지표까지 완전히 동일한 복제본
        rows.append(_naver_row(day, NAVER_POWERLINK, "grp-1", "500", "12", "3400"))
        rows.append(_naver_row(day, NAVER_POWERLINK, "grp-2", "310", "7", "2100"))
    _write_csv(naver_dir / "naverads_adgroups_2026.csv", rows)
    build_ads_daily_mart(
        {"campaigns": []},
        naver_dir=naver_dir,
        daangn_csv=sources["daangn_csv"],
        output_path=daily_path,
    )

    result = json.loads(
        notify_missing_collection(
            daily_path=daily_path,
            state_path=tmp_path / "state.json",
            naver_dir=naver_dir,
            today="2026-08-07",
            alert_sender=_recording_sender([]),
        )
    )

    # 누락일은 없지만 복제본은 보고돼야 한다
    assert result["total_missing"] == 0
    assert result["duplicate_metric_dates"] == [["2026-08-05", "2026-08-06", "2026-08-07"]]


def test_notify_does_not_flag_days_with_different_metrics(tmp_path, sources):
    daily_path = tmp_path / "mart" / "marketing_ads_daily.csv"
    naver_dir = tmp_path / "distinct_days"
    _write_csv(
        naver_dir / "naverads_adgroups_2026.csv",
        [
            _naver_row("2026-08-05", NAVER_POWERLINK, "grp-1", "500", "12", "3400"),
            _naver_row("2026-08-06", NAVER_POWERLINK, "grp-1", "480", "11", "3120"),
            _naver_row("2026-08-07", NAVER_POWERLINK, "grp-1", "521", "14", "3610"),
        ],
    )
    build_ads_daily_mart(
        {"campaigns": []},
        naver_dir=naver_dir,
        daangn_csv=sources["daangn_csv"],
        output_path=daily_path,
    )

    result = json.loads(
        notify_missing_collection(
            daily_path=daily_path,
            state_path=tmp_path / "state.json",
            naver_dir=naver_dir,
            today="2026-08-07",
            alert_sender=_recording_sender([]),
        )
    )

    assert result["duplicate_metric_dates"] == []
