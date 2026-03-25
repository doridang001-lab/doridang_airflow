"""
배민 우리가게클릭 주간 성과 및 영업 액션 제안 (v3.2).

행동 중심 1페이지 보고서.
차트 3분할 (주문수·CTR·ROAS), 가독성 개선.

결과:
  scripts/output/shop_trend_{매장키워드}_{timestamp}.json
  OneDrive/data/report/sales/shop_trend_{매장키워드}_{timestamp}.pdf

사용법:
    python scripts/analyze/shop_trend_report.py --brand 도리당 --store 상도점
"""
import argparse
import io
import json
import logging
import platform
import sys
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd

if platform.system() == "Windows":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts._base import OUTPUT_DIR, KST
from modules.transform.utility.paths import BAEMIN_MARKETING_DB, REPORT_SALES_DB
from modules.transform.utility.analytics import read_analytics_partition

logger = logging.getLogger(__name__)
SCRIPT_NAME = "shop_trend_report"

_THRESHOLDS = {
    "CTR":  {"warn": 1.5, "msg": "CTR 1.5% 미만: 유입 약세 신호"},
    "CVR":  {"warn": 10.0, "msg": "CVR 10% 미만: 상세페이지/리뷰 점검"},
    "ROAS": {"warn": 5.0, "msg": "ROAS 5배 미만: 광고 효율 재검토"},
    "drop": {"warn": -30.0, "msg": "주문수 30% 이상 감소: 운영/광고 동시 점검"},
}

_COLORS = {
    "warn": "#e74c3c",
    "ok": "#27ae60",
    "neutral": "#f39c12",
    "gray": "#7f8c8d",
    "blue": "#2980b9",
    "dark": "#2c3e50",
}

# ──────────────────────────────────────────────
# 데이터 처리
# ──────────────────────────────────────────────

def _load_store_data(brand: str, store_keyword: str) -> tuple[pd.DataFrame, str]:
    df = read_analytics_partition(root=BAEMIN_MARKETING_DB, brand=brand, store=None)
    mask = df["store"].str.contains(store_keyword, case=False, na=False)
    matched = df[mask]
    if matched.empty:
        raise ValueError(f"매장 찾을 수 없음: {store_keyword}")
    store_name = matched["store"].unique()[0]
    result = matched[matched["store"] == store_name].copy()
    logger.info("데이터 로드: %s (%d행)", store_name, len(result))
    return result, store_name


def _filter_complete_weeks(df: pd.DataFrame) -> pd.DataFrame:
    if "날짜" not in df.columns:
        return df
    try:
        today = datetime.now(KST).date()
        current_week_monday = today - timedelta(days=today.weekday())
        dates = pd.to_datetime(df["날짜"], errors="coerce")
        return df[dates.dt.date < current_week_monday].copy()
    except Exception:
        return df


def _calc_weekly_metrics(df: pd.DataFrame) -> pd.DataFrame:
    dates = pd.to_datetime(df["날짜"], errors="coerce")
    df = df.copy()
    df["_week"] = dates.dt.strftime("%Y-W%V")

    agg = df.groupby("_week").agg(
        광고지출=("광고지출", "sum"),
        노출수=("노출수", "sum"),
        클릭수=("클릭수", "sum"),
        주문수=("주문수", "sum"),
        주문금액=("주문금액", "sum"),
    ).sort_index()

    agg["클릭율"] = np.where(agg["노출수"] > 0, agg["클릭수"] / agg["노출수"] * 100, np.nan)
    agg["전환율"] = np.where(agg["클릭수"] > 0, agg["주문수"] / agg["클릭수"] * 100, np.nan)
    agg["광고효과"] = np.where(agg["광고지출"] > 0, agg["주문금액"] / agg["광고지출"], np.nan)
    return agg


def _calc_delta(weekly: pd.DataFrame) -> pd.DataFrame:
    if len(weekly) < 2:
        return pd.DataFrame(np.nan, index=weekly.index, columns=weekly.columns)
    return weekly.pct_change() * 100


def _diagnose_cause(latest: pd.Series, latest_d: pd.Series) -> tuple[str, list[str]]:
    order_d = latest_d.get("주문수", 0)
    ctr_d = latest_d.get("클릭율", 0)
    cvr_d = latest_d.get("전환율", 0)
    roas_d = latest_d.get("광고효과", 0)
    spend_d = latest_d.get("광고지출", 0)

    if (order_d < -20 and pd.notna(ctr_d) and abs(ctr_d) < 10
            and cvr_d > 0 and roas_d > 0 and spend_d < 0):
        cause = ("주문수는 감소했지만 CVR과 ROAS는 개선되어 "
                 "광고 효율 저하보다는 집행 축소에 따른 유입량 감소 영향이 큼")
        actions = [
            "전주 대비 광고비 축소 원인 확인",
            "전주 수준 기준 광고비 10~20% 복원 테스트 진행",
            "주문수 회복 및 ROAS 5배 이상 유지 시 예산 확대 검토",
            "주문수 미회복 시 운영조건 점검 우선",
        ]
    else:
        cause = "복합 신호 - 점검 필요"
        actions = ["지표 종합 진단 필요"]

    return cause, actions


def _dow_to_action(df: pd.DataFrame) -> list[str]:
    if "날짜" not in df.columns:
        return []
    try:
        dates = pd.to_datetime(df["날짜"], errors="coerce")
        df = df.copy()
        df["_dow"] = dates.dt.dayofweek
        dow_agg = df.groupby("_dow")["주문수"].mean()
        dow_names = ["월", "화", "수", "목", "금", "토", "일"]

        low_days = dow_agg.nsmallest(2).index.tolist()
        low_text = "/".join([dow_names[d] for d in low_days if d < 7])
        return [f"{low_text}: 쿠폰/즉시할인 테스트"]
    except Exception:
        return []


def _chart_subtitle(weekly: pd.DataFrame) -> str:
    """차트 섹션 부제목 (데이터 기반 자동 생성)."""
    if len(weekly) < 2:
        return ""
    cur, prv = weekly.iloc[-1], weekly.iloc[-2]
    order_down = cur["주문수"] < prv["주문수"]
    improved = []
    if cur.get("전환율", 0) > prv.get("전환율", 0):
        improved.append("CVR")
    if cur.get("광고효과", 0) > prv.get("광고효과", 0):
        improved.append("ROAS")
    if order_down and improved:
        return f"주문수는 감소했지만 {'·'.join(improved)}는 개선"
    if order_down:
        return "주문수·효율 모두 하락 추세"
    if improved:
        return f"주문수 회복, {'·'.join(improved)} 개선"
    return "전주 대비 유사"


def _table_note(weekly: pd.DataFrame) -> str:
    """표 하단 해석 문구 (데이터 기반 자동 생성)."""
    if len(weekly) < 2:
        return ""
    w_prev = weekly.index[-2].split("-W")[-1]
    w_last = weekly.index[-1].split("-W")[-1]
    cur, prv = weekly.iloc[-1], weekly.iloc[-2]

    order_down = cur["주문수"] < prv["주문수"]
    improved = []
    if cur.get("전환율", 0) > prv.get("전환율", 0):
        improved.append("CVR")
    if cur.get("광고효과", 0) > prv.get("광고효과", 0):
        improved.append("ROAS")

    if order_down and improved:
        imp = "과 ".join(improved)
        return (f"W{w_last}은 W{w_prev} 대비 주문수는 감소했으나 "
                f"{imp}는 개선되어, 효율 저하보다 유입량 감소 영향이 큰 것으로 판단")
    if order_down:
        return f"W{w_last}은 W{w_prev} 대비 전반적 하락, 원인 분석 필요"
    return f"W{w_last}은 W{w_prev} 대비 회복 추세"


def _set_korean_font():
    if platform.system() == "Windows":
        plt.rcParams["font.family"] = "Malgun Gothic"
    plt.rcParams["axes.unicode_minus"] = False


# ──────────────────────────────────────────────
# PDF 렌더링
# ──────────────────────────────────────────────

def _render_pdf(
    store_name: str,
    brand: str,
    store_keyword: str,
    weekly: pd.DataFrame,
    delta: pd.DataFrame,
    cause_text: str,
    actions: list[str],
    dow_actions: list[str],
    pdf_path: Path,
    run_at: datetime,
):
    """A4 1페이지 PDF."""
    fig = plt.figure(figsize=(8.27, 11.69), dpi=150)
    gs = gridspec.GridSpec(
        6, 2,
        height_ratios=[1.0, 1.5, 0.25, 1.8, 2.2, 1.5],
        hspace=0.55, wspace=0.3,
        top=0.92, bottom=0.04, left=0.08, right=0.96,
    )

    ax_judge = fig.add_subplot(gs[0, :])
    ax_diag = fig.add_subplot(gs[1, :])

    # 차트 헤더
    ax_trend_hdr = fig.add_subplot(gs[2, :])
    ax_trend_hdr.axis("off")
    subtitle = _chart_subtitle(weekly)
    hdr = f"■ 주차별 트렌드  —  {subtitle}" if subtitle else "■ 주차별 트렌드"
    ax_trend_hdr.text(0.0, 0.5, hdr,
                      transform=ax_trend_hdr.transAxes,
                      fontsize=8, fontweight="bold", color=_COLORS["dark"], va="center")

    # 차트 3분할
    gs_trend = gridspec.GridSpecFromSubplotSpec(
        1, 3, subplot_spec=gs[3, :], wspace=0.45)
    ax_t1 = fig.add_subplot(gs_trend[0, 0])
    ax_t2 = fig.add_subplot(gs_trend[0, 1])
    ax_t3 = fig.add_subplot(gs_trend[0, 2])

    ax_reason = fig.add_subplot(gs[4, 0])
    ax_action = fig.add_subplot(gs[4, 1])
    ax_tbl = fig.add_subplot(gs[5, :])

    # 헤더
    fig.text(0.5, 0.965,
             f"배민 우리가게클릭 주간 성과 및 영업 액션 제안  |  {brand} {store_keyword}",
             ha="center", va="center", fontsize=11, fontweight="bold",
             color="white",
             bbox=dict(boxstyle="round,pad=0.5", facecolor=_COLORS["dark"],
                       edgecolor="none"),
             transform=fig.transFigure)

    _section_judge(ax_judge, weekly, delta)
    _section_diagnosis(ax_diag, weekly, delta)
    _section_trend_split(ax_t1, ax_t2, ax_t3, weekly)
    _section_reason(ax_reason, cause_text)
    _section_actions(ax_action, actions, dow_actions)
    _section_table(ax_tbl, weekly)

    fig.text(0.5, 0.015,
             f"분석시각: {run_at.strftime('%Y-%m-%d %H:%M')}  |  {brand}  |  v3.2",
             ha="center", va="center", fontsize=7, color=_COLORS["gray"],
             transform=fig.transFigure)

    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(pdf_path, format="pdf", bbox_inches="tight")
    plt.close(fig)
    logger.info("PDF 저장: %s", pdf_path)


def _section_judge(ax, weekly, delta):
    """핵심 판단."""
    ax.axis("off")
    if len(weekly) == 0:
        return

    latest = weekly.iloc[-1]
    latest_d = delta.iloc[-1] if len(delta) > 0 else pd.Series(dtype=float)

    order_d = latest_d.get("주문수", 0)
    cvr = latest.get("전환율", 0)
    roas = latest.get("광고효과", 0)

    if order_d < -20 and cvr > 10 and roas > 5:
        judgment = "효율은 유지\n볼륨 부족"
        color = _COLORS["neutral"]
    else:
        judgment = "상태 점검 필요"
        color = _COLORS["warn"]

    ax.text(0.5, 0.68, judgment, ha="center", va="center",
            transform=ax.transAxes, fontsize=17, fontweight="bold",
            color="white",
            bbox=dict(boxstyle="round,pad=0.75", facecolor=color, edgecolor="none"))

    msg = ("주문수 감소는 광고 효율 저하보다 집행 축소에 따른 "
           "유입량 감소 영향이 크며, 우선 광고비 복원 테스트가 필요")
    ax.text(0.5, 0.05, msg,
            ha="center", va="center", transform=ax.transAxes,
            fontsize=6.3, color=_COLORS["dark"], weight="bold")


def _iso_week_to_date_range(iso_week_str: str) -> str:
    """2026-W11 → 03-01~07 형식으로 변환."""
    try:
        year, week_part = iso_week_str.split("-W")
        year, week_num = int(year), int(week_part)
        jan4 = pd.Timestamp(f"{year}-01-04")
        week1_monday = jan4 - pd.Timedelta(days=jan4.weekday())
        target_monday = week1_monday + pd.Timedelta(weeks=week_num - 1)
        target_sunday = target_monday + pd.Timedelta(days=6)
        return f"{target_monday.month:02d}-{target_monday.day:02d}~{target_sunday.day:02d}"
    except Exception:
        return iso_week_str


def _section_diagnosis(ax, weekly, delta):
    """이번 주 진단 (표)."""
    ax.axis("off")
    if len(weekly) == 0:
        return

    latest_week = weekly.index[-1]
    date_range = _iso_week_to_date_range(latest_week)
    ax.set_title(f"■ 이번 주 진단 ({date_range})", loc="left",
                 fontsize=9, fontweight="bold", color=_COLORS["dark"], pad=4)

    latest = weekly.iloc[-1]
    prev = weekly.iloc[-2] if len(weekly) > 1 else None
    latest_d = delta.iloc[-1] if len(delta) > 0 else pd.Series(dtype=float)

    def _fmt_pct(val):
        """볼륨 지표용: 상대 변화율."""
        if pd.isna(val) or val == 0:
            return "보합"
        return f"{val:+.1f}%"

    def _fmt_pp(cur_val, prv_val):
        """비율 지표용: 절대 차이 (%p)."""
        if prv_val is None or pd.isna(cur_val) or pd.isna(prv_val):
            return "-"
        diff = cur_val - prv_val
        if abs(diff) < 0.5:
            return "보합"
        return f"{diff:+.1f}%p"

    def _fmt_roas_diff(cur_val, prv_val):
        """ROAS: 절대 차이."""
        if prv_val is None or pd.isna(cur_val) or pd.isna(prv_val):
            return "-"
        diff = cur_val - prv_val
        return f"{diff:+.1f}x"

    prv_ctr = prev.get("클릭율", 0) if prev is not None else None
    prv_cvr = prev.get("전환율", 0) if prev is not None else None
    prv_roas = prev.get("광고효과", 0) if prev is not None else None

    rows = [
        ("주문수", f"{int(latest['주문수']):,}건",
         f"{int(prev['주문수']):,}건" if prev is not None else "-",
         _fmt_pct(latest_d.get("주문수", 0))),
        ("클릭수", f"{int(latest['클릭수']):,}회",
         f"{int(prev['클릭수']):,}회" if prev is not None else "-",
         _fmt_pct(latest_d.get("클릭수", 0))),
        ("노출수", f"{int(latest['노출수']):,}회",
         f"{int(prev['노출수']):,}회" if prev is not None else "-",
         _fmt_pct(latest_d.get("노출수", 0))),
        ("CTR", f"{latest.get('클릭율', 0):.2f}%",
         f"{prev.get('클릭율', 0):.2f}%" if prev is not None else "-",
         _fmt_pp(latest.get("클릭율", 0), prv_ctr)),
        ("CVR", f"{latest.get('전환율', 0):.2f}%",
         f"{prev.get('전환율', 0):.2f}%" if prev is not None else "-",
         _fmt_pp(latest.get("전환율", 0), prv_cvr)),
        ("ROAS", f"{latest.get('광고효과', 0):.1f}x",
         f"{prev.get('광고효과', 0):.1f}x" if prev is not None else "-",
         _fmt_roas_diff(latest.get("광고효과", 0), prv_roas)),
    ]

    tbl = ax.table(cellText=rows, colLabels=["지표", "이번주", "전주", "변화"],
                   cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(6)
    tbl.scale(1, 1.05)

    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#ddd")
        if r == 0:
            cell.set_facecolor(_COLORS["dark"])
            cell.get_text().set_color("white")
            cell.get_text().set_fontweight("bold")


def _section_trend_split(ax1, ax2, ax3, weekly):
    """트렌드 3분할 차트 (주문수, CTR, ROAS)."""
    if len(weekly) == 0:
        for ax in (ax1, ax2, ax3):
            ax.axis("off")
        return

    x = range(len(weekly))
    xticks = [0, len(weekly) - 1] if len(weekly) > 2 else list(x)
    xlabels = [weekly.index[i] for i in xticks]

    def _style(ax):
        ax.tick_params(axis="both", labelsize=5)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(True, alpha=0.2, linestyle="--", axis="y")
        ax.set_axisbelow(True)
        ax.set_xticks(xticks)
        ax.set_xticklabels(xlabels, fontsize=5)

    # 주문수 (막대)
    ax1.set_title("주문수", fontsize=7, fontweight="bold",
                  color=_COLORS["dark"], pad=3)
    ax1.bar(x, weekly["주문수"], color=_COLORS["blue"], alpha=0.7, width=0.6)
    _style(ax1)

    # CTR (선)
    ax2.set_title("CTR (%)", fontsize=7, fontweight="bold",
                  color=_COLORS["dark"], pad=3)
    ax2.plot(x, weekly["클릭율"], marker="s", color=_COLORS["blue"],
             linewidth=1.5, markersize=3)
    ax2.axhline(y=1.5, color=_COLORS["warn"], linestyle=":",
                linewidth=1, alpha=0.6)
    ax2.text(len(weekly) - 1, 1.5, " 1.5%", fontsize=4.5,
             color=_COLORS["warn"], va="bottom")
    _style(ax2)

    # ROAS (선)
    ax3.set_title("ROAS (배)", fontsize=7, fontweight="bold",
                  color=_COLORS["dark"], pad=3)
    ax3.plot(x, weekly["광고효과"], marker="o", color=_COLORS["warn"],
             linewidth=1.5, markersize=3)
    ax3.axhline(y=5.0, color=_COLORS["warn"], linestyle=":",
                linewidth=1, alpha=0.6)
    ax3.text(len(weekly) - 1, 5.0, " 5.0x", fontsize=4.5,
             color=_COLORS["warn"], va="bottom")
    _style(ax3)


def _section_reason(ax, cause_text):
    """원인 추정 우선순위."""
    ax.axis("off")
    ax.set_title("■ 원인 추정 우선순위", loc="left",
                 fontsize=9, fontweight="bold", color=_COLORS["dark"], pad=4)

    reasons = [
        ("1. 의도적 광고비 축소 여부 확인",
         "전주 대비 예산 감액 여부 점검"),
        ("2. 예산 조기 소진 또는 집행 제한 여부 확인",
         "일 예산 한도·노출 제한 설정 점검"),
        ("3. 품절·운영시간·배달중지 등 운영 이슈 확인",
         "매장 운영 상태 점검"),
    ]

    y = 0.85
    for title, desc in reasons:
        ax.text(0.05, y, title, transform=ax.transAxes,
                fontsize=6.5, color=_COLORS["dark"], va="center",
                weight="bold")
        y -= 0.12
        ax.text(0.08, y, desc, transform=ax.transAxes,
                fontsize=5.5, color=_COLORS["gray"], va="center")
        y -= 0.18


def _section_actions(ax, actions, dow_actions):
    """즉시 액션."""
    ax.axis("off")
    ax.set_title("■ 즉시 액션", fontsize=9, fontweight="bold",
                 color=_COLORS["dark"], loc="left", pad=4)

    action_items = [
        "1. 전주 대비 광고비 축소 원인 확인",
        "2. 전주 수준 기준 광고비 10~20%\n   복원 테스트 진행",
        "3. 주문수 회복 및 ROAS 5배 이상\n   유지 시 예산 확대 검토",
        "4. 주문수 미회복 시 운영조건 점검 우선",
    ]

    y = 0.88
    for action in action_items:
        ax.text(0.05, y, action, fontsize=6.5,
                transform=ax.transAxes, color=_COLORS["dark"],
                va="top", weight="bold")
        y -= 0.22


def _section_table(ax, weekly):
    """주차별 수치 테이블."""
    ax.axis("off")
    ax.set_title("■ 주차별 수치", loc="left",
                 fontsize=9, fontweight="bold", color=_COLORS["dark"], pad=4)

    rows = []
    tail_weeks = weekly.tail(4).index.tolist()
    for week in tail_weeks:
        r = weekly.loc[week]
        rows.append([
            week,
            f"{int(r['주문수']):,}",
            f"{int(r['클릭수']):,}",
            f"{r.get('클릭율', 0):.1f}%",
            f"{r.get('전환율', 0):.1f}%",
            f"{r.get('광고효과', 0):.1f}x",
        ])

    col_labels = ["주차", "주문수", "클릭수", "CTR", "CVR", "ROAS"]
    tbl = ax.table(cellText=rows, colLabels=col_labels,
                   cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(6)
    tbl.scale(1, 1.15)

    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#ddd")
        if r == 0:
            cell.set_facecolor(_COLORS["dark"])
            cell.get_text().set_color("white")
            cell.get_text().set_fontweight("bold")
        elif r == len(rows):
            cell.set_facecolor("#fff9e6")
        elif r == len(rows) - 1:
            cell.set_facecolor("#f0f8ff")

    note = _table_note(weekly)
    if note:
        ax.text(0.05, -0.15, note, fontsize=5.3,
                transform=ax.transAxes, color=_COLORS["dark"],
                va="top", style="italic")


# ──────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────

def main(brand: str, store_keyword: str) -> dict:
    run_at = datetime.now(KST)

    df, store_name = _load_store_data(brand, store_keyword)
    df = _filter_complete_weeks(df)
    if df.empty:
        raise ValueError("데이터 부족")

    weekly = _calc_weekly_metrics(df)
    weekly = weekly.tail(13)
    delta = _calc_delta(weekly)

    if len(weekly) > 0:
        latest = weekly.iloc[-1]
        latest_d = delta.iloc[-1] if len(delta) > 0 else pd.Series(dtype=float)
        cause_text, actions = _diagnose_cause(latest, latest_d)
        dow_actions = _dow_to_action(df)
    else:
        cause_text, actions = "", []
        dow_actions = []

    _set_korean_font()
    safe_kw = store_keyword.replace(" ", "_")
    timestamp = run_at.strftime("%Y%m%d_%H%M%S")
    pdf_path = REPORT_SALES_DB / f"shop_trend_{safe_kw}_{timestamp}.pdf"
    json_path = OUTPUT_DIR / f"shop_trend_{safe_kw}_{timestamp}.json"

    _render_pdf(
        store_name=store_name,
        brand=brand,
        store_keyword=store_keyword,
        weekly=weekly,
        delta=delta,
        cause_text=cause_text,
        actions=actions,
        dow_actions=dow_actions,
        pdf_path=pdf_path,
        run_at=run_at,
    )

    result = {
        "meta": {
            "script": f"analyze/{SCRIPT_NAME}.py",
            "run_at": run_at.isoformat(),
            "status": "ok",
            "pdf_path": str(pdf_path),
        },
        "summary": {
            "brand": brand,
            "store": store_name,
            "weeks": weekly.index.tolist(),
        },
        "stats": {
            "weekly": weekly.to_dict(orient="index"),
            "cause": cause_text,
            "actions": actions,
            "dow_actions": dow_actions,
        },
    }

    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("JSON 저장: %s", json_path)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="배민 우리가게클릭 주간 성과 및 영업 액션 제안 v3.2")
    parser.add_argument("--brand", required=True, help="브랜드명")
    parser.add_argument("--store", required=True, help="매장명 키워드")
    args = parser.parse_args()

    try:
        result = main(args.brand, args.store)
        print(str(result["meta"]["pdf_path"]))
    except Exception as e:
        logger.error("실행 실패: %s", e, exc_info=True)
        sys.exit(1)
