"""
정책 통합 최신화 파이프라인

8개 플랫폼 raw 정책 CSV에서 플랫폼별 최신 공지 1건씩 추출 →
통합 CSV 저장 → 신규/변경 공지 이메일 알림
"""

import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from airflow.exceptions import AirflowSkipException

from modules.transform.utility.paths import (
    BAEMIN_POLICY_CSV_PATH,
    COUPANG_POLICY_CSV_PATH,
    YOGIYO_POLICY_CSV_PATH,
    DDANGYO_POLICY_CSV_PATH,
    BAEDALTTEUK_POLICY_CSV_PATH,
    MUKKEBI_POLICY_CSV_PATH,
    BAEDALEUM_POLICY_CSV_PATH,
    NAVER_PLACE_POLICY_CSV_PATH,
    POLICY_LOG_PATH,
    POLICY_CONSOLIDATED_CSV,
)
from modules.transform.utility.mailer import send_email

logger = logging.getLogger(__name__)

# 플랫폼 정렬 순서 (CSV 출력 및 이메일 표시 순서)
_PLATFORM_ORDER = [
    "baemin",
    "coupang",
    "ddangyo",
    "yogiyo",
    "baedaltteuk",
    "baedaleum",
    "mukkebi",
    "naver_place",
]

# 플랫폼별 raw CSV 경로 목록 (정렬 순서 동일)
_POLICY_CSV_PATHS = [
    BAEMIN_POLICY_CSV_PATH,
    COUPANG_POLICY_CSV_PATH,
    DDANGYO_POLICY_CSV_PATH,
    YOGIYO_POLICY_CSV_PATH,
    BAEDALTTEUK_POLICY_CSV_PATH,
    BAEDALEUM_POLICY_CSV_PATH,
    MUKKEBI_POLICY_CSV_PATH,
    NAVER_PLACE_POLICY_CSV_PATH,
]

# 플랫폼 한글 이름 매핑
_PLATFORM_KR = {
    "baemin": "배민",
    "coupang": "쿠팡이츠",
    "ddangyo": "땡겨요",
    "yogiyo": "요기요",
    "baedaltteuk": "배달특급",
    "baedaleum": "배달e음",
    "mukkebi": "먹깨비",
    "naver_place": "네이버플레이스",
}

# policy_type 뱃지 색상 매핑
_TYPE_COLOR = {
    "할인": "#27ae60",
    "수수료": "#e74c3c",
    "광고": "#2980b9",
    "노출": "#8e44ad",
    "정산": "#e67e22",
    "운영": "#7f8c8d",
    "기능변경": "#16a085",
    "기타": "#bdc3c7",
}

_OUTPUT_COLS = ["platform", "title", "content_summary", "policy_type", "recommended_action"]


# ============================================================
# load_and_filter_latest
# ============================================================

def load_and_filter_latest(**context) -> str:
    """8개 플랫폼 raw CSV 로드 → 플랫폼별 최신 policy_date 1건 필터링"""
    dfs = []
    for csv_path in _POLICY_CSV_PATHS:
        p = Path(csv_path)
        if not p.exists():
            logger.warning(f"정책 CSV 없음 (스킵): {p}")
            continue
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
            if df.empty:
                logger.warning(f"정책 CSV 비어있음 (스킵): {p}")
                continue
            dfs.append(df)
            logger.info(f"로드 완료: {p.name} ({len(df)}행)")
        except Exception as e:
            logger.warning(f"CSV 읽기 실패 {p}: {e}")

    if not dfs:
        raise AirflowSkipException("로드 가능한 정책 CSV가 없습니다.")

    df_all = pd.concat(dfs, ignore_index=True)

    # policy_date 내림차순 → platform 기준 중복 제거(최신 1건) → 플랫폼 순서 정렬
    df_all["policy_date"] = df_all["policy_date"].astype(str)
    df_all = df_all.sort_values("policy_date", ascending=False)
    df_latest = df_all.drop_duplicates(subset=["platform"], keep="first")

    # 지정 순서대로 정렬 (목록에 없는 플랫폼은 뒤로)
    order_map = {p: i for i, p in enumerate(_PLATFORM_ORDER)}
    df_latest = df_latest.assign(
        _order=df_latest["platform"].map(lambda p: order_map.get(p, 999))
    ).sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)

    logger.info(f"플랫폼별 최신 1건 필터링 완료: {len(df_latest)}개 플랫폼")

    extra_cols = [c for c in ["policy_date", "source_url"] if c in df_latest.columns]
    result_json = df_latest[_OUTPUT_COLS + extra_cols].to_json(
        orient="records", force_ascii=False
    )
    context["ti"].xcom_push(key="latest_policies", value=result_json)
    return result_json


# ============================================================
# detect_new_policies
# ============================================================

def detect_new_policies(**context) -> list:
    """기존 통합 CSV(이전 실행 스냅샷)와 비교하여 신규/변경 정책 감지.

    통합 CSV는 매 실행마다 truncate+rewrite되므로,
    이 함수는 save_consolidated_csv보다 먼저 실행되어 이전 스냅샷을 읽는다.
    비교 키: platform + policy_date (제목 변경 또는 날짜 변경 = 신규)
    """
    latest_json = context["ti"].xcom_pull(
        task_ids="task_load_and_filter", key="latest_policies"
    )
    df_new = pd.read_json(latest_json, orient="records")

    consolidated_path = Path(POLICY_CONSOLIDATED_CSV)
    if not consolidated_path.exists():
        # 최초 실행 → 전체가 신규
        new_policies = df_new.to_dict(orient="records")
        logger.info(f"최초 실행: 전체 {len(new_policies)}건 신규 처리")
        context["ti"].xcom_push(key="new_policies", value=new_policies)
        return new_policies

    # 이전 스냅샷 읽기 (아직 overwrite 전)
    df_prev = pd.read_csv(consolidated_path, encoding="utf-8-sig")

    # 비교 기준: platform별 이전 policy_date
    # 통합 CSV에 policy_date가 없으면 title로 폴백
    if "policy_date" in df_prev.columns:
        prev_state = dict(zip(df_prev["platform"], df_prev["policy_date"]))
        new_policies = [
            row for _, row in df_new.iterrows()
            if prev_state.get(row["platform"]) != row["policy_date"]
        ]
    else:
        prev_state = dict(zip(df_prev["platform"], df_prev["title"]))
        new_policies = [
            row for _, row in df_new.iterrows()
            if prev_state.get(row["platform"]) != row["title"]
        ]

    logger.info(f"신규/변경 정책 감지: {len(new_policies)}건")
    context["ti"].xcom_push(key="new_policies", value=new_policies)
    return new_policies


# ============================================================
# save_consolidated_csv
# ============================================================

def save_consolidated_csv(**context) -> str:
    """통합 최신 정책 CSV 덮어쓰기 저장 (5개 컬럼만)"""
    latest_json = context["ti"].xcom_pull(
        task_ids="task_load_and_filter", key="latest_policies"
    )
    df = pd.read_json(latest_json, orient="records")

    # 5개 출력 컬럼만
    save_cols = [c for c in _OUTPUT_COLS if c in df.columns]
    df_save = df[save_cols]

    out_path = Path(POLICY_CONSOLIDATED_CSV)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_save.to_csv(out_path, index=False, encoding="utf-8-sig")

    logger.info(f"통합 CSV 저장 완료: {out_path} ({len(df_save)}행)")
    return str(out_path)


# ============================================================
# send_policy_alert
# ============================================================

def send_policy_alert(**context) -> str:
    """신규/변경 정책이 있을 때 HTML 이메일 알림 발송"""
    new_policies = context["ti"].xcom_pull(
        task_ids="task_detect_new_policies", key="new_policies"
    )

    if not new_policies:
        logger.info("신규 정책 없음 → 이메일 스킵")
        raise AirflowSkipException("신규/변경 정책이 없어 이메일을 발송하지 않습니다.")

    today = datetime.now().strftime("%Y-%m-%d")
    count = len(new_policies)
    subject = f"[도리당] 플랫폼 정책 변경 알림 - {count}건 ({today})"

    html = _build_alert_html(new_policies, today)
    result = send_email(
        subject=subject,
        html_content=html,
        to_emails="a17019@kakao.com",
    )
    logger.info(f"정책 알림 이메일 발송 완료: {count}건")
    return result


def _build_alert_html(policies: list, today: str) -> str:
    """정책 알림 HTML 이메일 생성"""
    rows_html = ""
    for p in policies:
        platform_key = str(p.get("platform", ""))
        platform_kr = _PLATFORM_KR.get(platform_key, platform_key)
        policy_type = str(p.get("policy_type", "기타"))
        badge_color = _TYPE_COLOR.get(policy_type, "#bdc3c7")
        title = str(p.get("title", ""))
        summary = str(p.get("content_summary", ""))
        action = str(p.get("recommended_action", ""))
        policy_date = str(p.get("policy_date", ""))
        source_url = str(p.get("source_url", "")).strip()
        title_html = (
            f'<a href="{source_url}" target="_blank" '
            f'style="color:#2980b9; text-decoration:none; font-weight:500;">{title}</a>'
            if source_url and source_url != "None" and source_url.startswith("http")
            else title
        )

        rows_html += f"""
        <tr>
          <td style="padding:10px 12px; border-bottom:1px solid #eee; font-weight:600; white-space:nowrap;">
            {platform_kr}
          </td>
          <td style="padding:10px 12px; border-bottom:1px solid #eee; white-space:nowrap; color:#666; font-size:12px;">
            {policy_date}
          </td>
          <td style="padding:10px 12px; border-bottom:1px solid #eee;">
            <span style="background:{badge_color}; color:#fff; padding:2px 8px; border-radius:12px; font-size:11px; white-space:nowrap;">
              {policy_type}
            </span>
          </td>
          <td style="padding:10px 12px; border-bottom:1px solid #eee;">
            {title_html}
          </td>
          <td style="padding:10px 12px; border-bottom:1px solid #eee; color:#555; font-size:13px;">
            {summary}
          </td>
          <td style="padding:10px 12px; border-bottom:1px solid #eee; color:#c0392b; font-size:13px; font-weight:500;">
            {action}
          </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="ko">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0; padding:0; background:#f4f6f9; font-family:'Malgun Gothic',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6f9; padding:32px 0;">
    <tr><td align="center">
      <table width="700" cellpadding="0" cellspacing="0" style="background:#fff; border-radius:10px; overflow:hidden; box-shadow:0 2px 12px rgba(0,0,0,0.08);">

        <!-- 헤더 -->
        <tr>
          <td style="background:linear-gradient(135deg,#27ae60,#2ecc71); padding:28px 32px;">
            <div style="color:#fff; font-size:22px; font-weight:700; letter-spacing:-0.5px;">🍽 도리당 정책 변경 알림</div>
            <div style="color:rgba(255,255,255,0.85); font-size:13px; margin-top:6px;">{today} 기준 · 신규/변경 {len(policies)}건</div>
          </td>
        </tr>

        <!-- 본문 -->
        <tr>
          <td style="padding:24px 32px;">
            <p style="margin:0 0 16px; color:#333; font-size:14px;">
              안녕하세요. 아래 플랫폼에서 <strong>새로운 정책 공지</strong>가 감지되었습니다.
            </p>
            <div style="overflow-x:auto;">
              <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse; font-size:13px;">
                <thead>
                  <tr style="background:#f8f9fa;">
                    <th style="padding:10px 12px; text-align:left; color:#555; font-weight:600; border-bottom:2px solid #dee2e6; white-space:nowrap;">플랫폼</th>
                    <th style="padding:10px 12px; text-align:left; color:#555; font-weight:600; border-bottom:2px solid #dee2e6; white-space:nowrap;">공지일</th>
                    <th style="padding:10px 12px; text-align:left; color:#555; font-weight:600; border-bottom:2px solid #dee2e6; white-space:nowrap;">유형</th>
                    <th style="padding:10px 12px; text-align:left; color:#555; font-weight:600; border-bottom:2px solid #dee2e6;">제목</th>
                    <th style="padding:10px 12px; text-align:left; color:#555; font-weight:600; border-bottom:2px solid #dee2e6;">요약</th>
                    <th style="padding:10px 12px; text-align:left; color:#555; font-weight:600; border-bottom:2px solid #dee2e6;">권장 조치</th>
                  </tr>
                </thead>
                <tbody>
                  {rows_html}
                </tbody>
              </table>
            </div>
          </td>
        </tr>

        <!-- 푸터 -->
        <tr>
          <td style="background:#f8f9fa; padding:16px 32px; border-top:1px solid #eee;">
            <p style="margin:0; color:#999; font-size:11px; line-height:1.6;">
              이 메일은 Airflow 자동화 파이프라인에서 발송되었습니다.<br>
              문의: a17019@kakao.com
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


# ============================================================
# write_consolidate_log
# ============================================================

def write_consolidate_log(**context) -> None:
    """실행 이력을 log.parquet에 기록 (trigger_rule='all_done')"""
    log_path = Path(POLICY_LOG_PATH)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    new_policies = context["ti"].xcom_pull(
        task_ids="task_detect_new_policies", key="new_policies"
    ) or []

    record = {
        "dag_id": context.get("dag").dag_id if context.get("dag") else "unknown",
        "run_id": context.get("run_id", ""),
        "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "new_policy_count": len(new_policies),
    }
    df_log = pd.DataFrame([record])

    if log_path.exists():
        try:
            df_existing = pd.read_parquet(log_path)
            df_all = pd.concat([df_existing, df_log], ignore_index=True)
        except Exception as e:
            logger.warning(f"기존 로그 읽기 실패, 새로 생성: {e}")
            df_all = df_log
    else:
        df_all = df_log

    df_all.to_parquet(log_path, index=False)
    logger.info(f"실행 로그 기록 완료: {log_path}")
