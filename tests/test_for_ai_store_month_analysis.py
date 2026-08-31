import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from modules.transform.pipelines.sales import For_AI_store_month_analysis as for_ai


def test_for_ai_payload_includes_commission_visit_and_readme(monkeypatch, tmp_path):
    analytics = tmp_path / "analytics"
    mart = tmp_path / "mart"
    store_target = analytics / "store_sales_target"
    store_target.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "기준월": "2026-08-01",
                "매장명": "송파삼전점",
                "브랜드": "도리당",
                "홀_월목표매출": "70000000",
                "홀_테이블_월목표매출": "60000000",
                "홀_포장_월목표매출": "10000000",
                "배달_월목표매출": "20000000",
                "전체_월목표매출": "90000000",
                "평일_일목표매출": "2000000",
                "주말_일목표매출": "3000000",
                "목표_테이블단가": "50000",
                "테이블수": "9",
            }
        ]
    ).to_csv(store_target / "target.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {
                "매출일자": "2026-08-03",
                "기준월": "2026-08",
                "매장명": "송파삼전점",
                "브랜드": "도리당",
                "요일구분": "평일",
                "홀매출": "1000000",
                "홀_테이블_매출": "800000",
                "홀_포장_매출": "200000",
                "배달매출": "500000",
                "총매출": "1500000",
                "테이블수": "20",
                "테이블단가": "75000",
                "수집일시": "2026-08-04 09:00",
            }
        ]
    ).to_csv(store_target / "daily_actuals.csv", index=False, encoding="utf-8-sig")

    unified_dir = mart / "unified_sales_grp"
    unified_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "sale_date": "2026-08-03",
                "ym": "2026-08",
                "source": "okpos",
                "brand": "도리당",
                "store": "송파삼전점",
                "platform": "홀",
                "order_type": "홀_테이블",
                "order_id": "1",
                "menu_name": "닭볶음탕",
                "total_price": 800000,
                "order_cnt": 1,
            },
            {
                "sale_date": "2026-08-03",
                "ym": "2026-08",
                "source": "배민수동",
                "brand": "도리당",
                "store": "송파삼전점",
                "platform": "배달의민족",
                "order_type": "배달",
                "order_id": "2",
                "menu_name": "묵은지닭볶음탕",
                "total_price": 500000,
                "order_cnt": 1,
            },
        ]
    ).to_parquet(unified_dir / "unified_sales_260803.parquet", index=False)

    commission_dir = mart / "delivery_commission"
    commission_dir.mkdir(parents=True)
    commission_path = commission_dir / "delivery_commission.parquet"
    pd.DataFrame(
        [
            {
                "sale_date": "2026-08-03",
                "store": "송파삼전점",
                "platform": "배달의민족",
                "total_amt": 500000,
                "settlement_amount": 350000,
                "diff_amt": 150000,
                "brand": "도리당",
                "배민_즉시할인": 10000,
                "우가클_평균비용": 1000,
                "우가클_주문수": 5,
                "우가클_클릭율": 10,
            }
        ]
    ).to_parquet(commission_path, index=False)

    visit_dir = mart / "Flow_mart" / "Flow_visit"
    visit_dir.mkdir(parents=True)
    visit_path = visit_dir / "flow_visit_viz.parquet"
    pd.DataFrame(
        [
            {
                "store_name": "송파삼전점",
                "visit_ym": "2026-08",
                "visit_rel_key": "p|1",
                "visit_date": "2026-08-05",
                "post_id": "1",
                "post_url": "https://flow.team/l/test",
                "author_name": "조민준",
                "category": "매출",
                "issue_label": "홀 매출 부진",
                "is_request": True,
                "status": "미해결",
                "owner_status": "홀 매출 회복 필요",
                "key_concerns": "점심 유입 부족",
                "next_visit_action": "점심 메뉴 노출 점검",
            }
        ]
    ).to_parquet(visit_path, index=False)

    monkeypatch.setattr(for_ai, "STORE_SALES_TARGET_CSV", store_target / "target.csv")
    monkeypatch.setattr(for_ai, "STORE_SALES_DAILY_ACTUALS_CSV", store_target / "daily_actuals.csv")
    monkeypatch.setattr(for_ai, "UNIFIED_SALES_DIR", unified_dir)
    monkeypatch.setattr(for_ai, "DELIVERY_COMMISSION_PATH", commission_path)
    monkeypatch.setattr(for_ai, "FLOW_VISIT_VIZ_PARQUET", visit_path)
    monkeypatch.setattr(for_ai, "UNIFIED_REVIEW_DIR", mart / "unified_review")
    monkeypatch.setattr(for_ai, "BAEMIN_WOORI_DIR", analytics / "baemin_macro" / "metrics_our_store_clicks")
    monkeypatch.setattr(for_ai, "BAEMIN_NOW_DIR", analytics / "baemin_macro" / "metrics_now")
    monkeypatch.setattr(for_ai, "BAEMIN_AD_FUNNEL_DIR", analytics / "baemin_macro" / "ad_funnel")
    monkeypatch.setattr(for_ai, "BAEMIN_MARKETING_DIR", analytics / "baemin_marketing")
    monkeypatch.setattr(for_ai, "COUPANG_MARKETING_DIR", analytics / "coupang_marketing")

    output_root = tmp_path / "For_AI"
    result = for_ai.build_for_ai_store_month_analysis(
        brand="도리당",
        ym="2026-08",
        store="송파삼전점",
        output_root=output_root,
        write=True,
    )

    assert result.startswith("OK:")
    analysis_path = output_root / "brand=도리당" / "ym=2026-08" / "store=송파삼전점" / "analysis.json"
    readme_path = analysis_path.with_name("README.md")
    orders_path = analysis_path.with_name("orders.json")
    ads_path = analysis_path.with_name("ads.json")
    visit_log_path = analysis_path.with_name("visit_log.json")
    payload = json.loads(analysis_path.read_text(encoding="utf-8"))
    orders = json.loads(orders_path.read_text(encoding="utf-8"))
    ads = json.loads(ads_path.read_text(encoding="utf-8"))
    visit_log = json.loads(visit_log_path.read_text(encoding="utf-8"))

    assert payload["month_summary"]["actual_sales"] == 1500000
    assert payload["meta"]["files"]["orders"]["path"] == "orders.json"
    assert payload["meta"]["files"]["ads"]["path"] == "ads.json"
    assert payload["meta"]["files"]["visit_log"]["path"] == "visit_log.json"
    assert payload["commission_summary"]["totals"]["commission_rate"] == 30.0
    assert payload["visit_summary"]["visit_count"] == 1
    assert payload["visit_summary"]["top_issues"][0]["issue"] == "홀 매출 부진"
    assert orders["daily_summary"][0]["order_count"] == 2
    assert orders["daily_summary"][0]["average_order_value"] == 750000
    assert orders["daily_summary"][0]["sales"] == 1500000
    assert orders["daily_menu_summary"][0]["menu_name"] in {"닭볶음탕", "묵은지닭볶음탕"}
    assert ads["commission_ad_summary"]["baemin_instant_discount"] == 10000
    assert visit_log["summary"]["visit_count"] == 1
    assert visit_log["visits"][0]["issue_label"] == "홀 매출 부진"
    assert readme_path.exists()
    assert orders_path.exists()
    assert ads_path.exists()
    assert visit_log_path.exists()
    assert "수수료/정산" in readme_path.read_text(encoding="utf-8")
    assert "orders.json" in readme_path.read_text(encoding="utf-8")


def test_for_ai_orders_target_lookback_and_recent_visits(monkeypatch, tmp_path):
    analytics = tmp_path / "analytics"
    mart = tmp_path / "mart"
    store_target = analytics / "store_sales_target"
    store_target.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "매출일자": "2026-08-03",
                "기준월": "2026-08",
                "매장명": "용인동천점",
                "브랜드": "도리당",
                "요일구분": "평일",
                "홀매출": "0",
                "홀_테이블_매출": "0",
                "홀_포장_매출": "0",
                "배달매출": "100000",
                "총매출": "100000",
                "테이블수": "0",
                "테이블단가": "0",
            }
        ]
    ).to_csv(store_target / "daily_actuals.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {
                "기준월": "2026-08",
                "매장명": "용인동천점",
                "브랜드": "도리당",
                "전체_월목표매출": "1000000",
                "평일_일목표매출": "100000",
                "주말_일목표매출": "100000",
            }
        ]
    ).to_csv(store_target / "target.csv", index=False, encoding="utf-8-sig")

    unified_dir = mart / "unified_sales_grp"
    unified_dir.mkdir(parents=True)
    for date_str in ["2026-06-01", "2026-07-01", "2026-08-03"]:
        yymmdd = pd.Timestamp(date_str).strftime("%y%m%d")
        pd.DataFrame(
            [
                {
                    "sale_date": date_str,
                    "ym": pd.Timestamp(date_str).strftime("%Y-%m"),
                    "source": "baemin",
                    "brand": "도리당",
                    "store": "용인동천점",
                    "platform": "배달의민족",
                    "order_type": "배달",
                    "order_id": f"o-{date_str}",
                    "menu_name": "닭볶음탕",
                    "total_price": 100000,
                    "order_cnt": 1,
                }
            ]
        ).to_parquet(unified_dir / f"unified_sales_{yymmdd}.parquet", index=False)

    visit_dir = mart / "Flow_mart" / "Flow_visit"
    visit_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "store_name": "용인동천점",
                "visit_ym": "2025-09",
                "visit_rel_key": "v0",
                "visit_date": "2025-09-18",
                "post_id": "0",
                "category": "사입",
                "issue_label": "전용 상품 준수",
                "status": "미해결",
            },
            {
                "store_name": "용인동천점",
                "visit_ym": "2025-12",
                "visit_rel_key": "v1",
                "visit_date": "2025-12-18",
                "post_id": "1",
                "category": "광고",
                "issue_label": "즉시할인 광고",
                "status": "미해결",
            },
            {
                "store_name": "용인동천점",
                "visit_ym": "2026-03",
                "visit_rel_key": "v2",
                "visit_date": "2026-03-18",
                "post_id": "2",
                "category": "매출",
                "issue_label": "배달 매출 정체",
                "status": "안내완료",
            },
            {
                "store_name": "용인동천점",
                "visit_ym": "2026-06",
                "visit_rel_key": "v3",
                "visit_date": "2026-06-01",
                "post_id": "3",
                "category": "광고",
                "issue_label": "우가클 단가",
                "status": "미해결",
            },
        ]
    ).to_parquet(visit_dir / "flow_visit_viz.parquet", index=False)

    monkeypatch.setattr(for_ai, "STORE_SALES_TARGET_CSV", store_target / "target.csv")
    monkeypatch.setattr(for_ai, "STORE_SALES_DAILY_ACTUALS_CSV", store_target / "daily_actuals.csv")
    monkeypatch.setattr(for_ai, "UNIFIED_SALES_DIR", unified_dir)
    monkeypatch.setattr(for_ai, "FLOW_VISIT_VIZ_PARQUET", visit_dir / "flow_visit_viz.parquet")
    monkeypatch.setattr(for_ai, "DELIVERY_COMMISSION_PATH", mart / "delivery_commission" / "delivery_commission.parquet")
    monkeypatch.setattr(for_ai, "UNIFIED_REVIEW_DIR", mart / "unified_review")
    monkeypatch.setattr(for_ai, "BAEMIN_WOORI_DIR", analytics / "baemin_macro" / "metrics_our_store_clicks")
    monkeypatch.setattr(for_ai, "BAEMIN_NOW_DIR", analytics / "baemin_macro" / "metrics_now")
    monkeypatch.setattr(for_ai, "BAEMIN_AD_FUNNEL_DIR", analytics / "baemin_macro" / "ad_funnel")
    monkeypatch.setattr(for_ai, "BAEMIN_MARKETING_DIR", analytics / "baemin_marketing")
    monkeypatch.setattr(for_ai, "COUPANG_MARKETING_DIR", analytics / "coupang_marketing")

    output_root = tmp_path / "For_AI"
    result = for_ai.build_for_ai_store_month_analysis(
        output_root=output_root,
        write=True,
        lookback=3,
        target_source="orders",
    )

    assert result.startswith("OK:")
    assert (output_root / "brand=도리당" / "ym=2026-06" / "store=용인동천점" / "analysis.json").exists()
    assert (output_root / "brand=도리당" / "ym=2026-07" / "store=용인동천점" / "analysis.json").exists()
    analysis_path = output_root / "brand=도리당" / "ym=2026-08" / "store=용인동천점" / "analysis.json"
    visit_log_path = analysis_path.with_name("visit_log.json")
    payload = json.loads(analysis_path.read_text(encoding="utf-8"))
    visit_log = json.loads(visit_log_path.read_text(encoding="utf-8"))

    assert "flow_visit_viz" not in payload["meta"]["missing_sources"]
    assert payload["visit_summary"]["current_month_visit_count"] == 0
    assert payload["visit_summary"]["visit_count"] == 4
    assert payload["visit_summary"]["visit_history_scope"] == "all_until_month_end"
    assert visit_log["meta"]["visit_history_scope"] == "all_until_month_end"
    assert visit_log["current_month_visits"] == []
    assert [row["visit_date"] for row in visit_log["recent_visits"]] == [
        "2026-06-01",
        "2026-03-18",
        "2025-12-18",
        "2025-09-18",
    ]
    assert visit_log["visit_timeline"] == ["2026-06-01", "2026-03-18", "2025-12-18", "2025-09-18"]


def test_for_ai_visit_log_serializes_timestamp_values():
    visits = pd.DataFrame(
        [
            {
                "store_name": "동탄영천점",
                "visit_ym": "2025-07",
                "visit_rel_key": "v1",
                "visit_date": pd.Timestamp("2025-07-15"),
                "post_id": "1",
                "category": "매출",
                "issue_label": "방문일지 점검",
                "status": "미해결",
            }
        ]
    )

    payload = for_ai._visit_log_payload(
        brand="도리당",
        store="동탄영천점",
        ym="2025-07",
        visits=visits,
        visit_summary=for_ai._visit_summary(visits),
    )

    encoded = json.dumps(payload, ensure_ascii=False)
    decoded = json.loads(encoded)
    assert decoded["visits"][0]["visit_date"] == "2025-07-15"
    assert decoded["visit_timeline"] == ["2025-07-15"]


def test_for_ai_visit_summary_hides_generic_visit_issue_label():
    summary = for_ai._visit_summary(
        pd.DataFrame(
            [
                {
                    "visit_rel_key": "v1",
                    "visit_date": "2026-08-12",
                    "category": "미분류",
                    "issue_label": "방문일지 주요 내용",
                    "status": "미해결",
                },
                {
                    "visit_rel_key": "v1",
                    "visit_date": "2026-08-12",
                    "category": "광고",
                    "issue_label": "토더 설정/공지",
                    "status": "미해결",
                },
            ]
        )
    )

    assert summary["top_issues"] == [{"issue": "토더 설정/공지", "count": 1}]


def test_for_ai_visit_records_omit_generic_issue_label():
    records = for_ai._visit_records(
        pd.DataFrame(
            [
                {
                    "visit_date": "2026-08-12",
                    "issue_label": "방문일지 주요 내용",
                    "category": "미분류",
                    "key_concerns": "- 도리당만 분리 계산",
                }
            ]
        )
    )

    assert "issue_label" not in records[0]
    assert records[0]["key_concerns"] == "- 도리당만 분리 계산"
