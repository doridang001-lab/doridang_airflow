"""
홀 매장 주간보고 목표치 설정 (airflow 비의존)

DAG(DB_Hall_Sales_Target_Dags.py)와 Windows 로컬 실행 스크립트
(scripts/run_hall_weekly_report_local.py)가 공유한다.

매월 업데이트 대상: DAILY_TARGET / MARKETING_DAILY_TARGET 만 수정하면
월수(calendar 자동)에 맞춰 MONTHLY_TARGETS 가 자동 계산된다.
"""

import calendar

import pendulum

# ──────────────────────────────────────────────────────────────
# 매출 월간 목표치 (매월 업데이트)
# 세팅 기준: 일목표 × 해당월 일수
#   점심: 일 29건 / 일 812,000원 / 테이블단가 28,000원
#   저녁: 일 33건 / 일 1,544,000원 / 테이블단가 47,000원
#   테이블 총 9개
# ──────────────────────────────────────────────────────────────
DAILY_TARGET = {
    "sale": 2_356_000,          # 일 매출 목표
    "aov": 38_000,              # 전체 테이블단가
    "orders": 62,               # 일 영수건수

    "lunch_sale": 812_000,      # 점심 일 매출 목표
    "lunch_orders": 29,         # 점심 일 영수건수
    "lunch_aov": 28_000,        # 점심 테이블단가

    "dinner_sale": 1_544_000,   # 저녁 일 매출 목표
    "dinner_orders": 33,        # 저녁 일 영수건수
    "dinner_aov": 47_000,       # 저녁 테이블단가
}

# ──────────────────────────────────────────────────────────────
# 마케팅 월간 목표치 (매월 업데이트)
# 세팅 기준: 일목표 × 해당월 일수
#   플레이스 유입: 800명/일 (월 24,000명)
#   홍보물 배포:  100건/일 (월  3,000건)
#   쿠폰 회수:     7건/일 (3,000건 × 7%)
#   인스타 노출: 3,334회/일 (월 100,000회)
#   당근 노출:   3,334회/일 (월 100,000회)
#   네이버 오더:   15건/일 (월    450건)
# ──────────────────────────────────────────────────────────────
MARKETING_DAILY_TARGET = {
    "플레이스_유입":   800,      # 명/일
    "홍보물_배포":    100,       # 건/일
    "쿠폰_회수":        7,       # 건/일
    "인스타_노출":  3_334,       # 회/일
    "당근_노출":    3_334,       # 회/일
    "네이버_오더":     15,       # 건/일
}


def _make_month_days(start_ym: str, end_ym: str) -> dict:
    """start_ym ~ end_ym(포함) 범위의 {ym: days} 자동 생성"""
    result = {}
    y, m = int(start_ym[:4]), int(start_ym[5:7])
    ey, em = int(end_ym[:4]), int(end_ym[5:7])
    while (y, m) <= (ey, em):
        ym = f"{y:04d}-{m:02d}"
        result[ym] = calendar.monthrange(y, m)[1]
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def build_targets() -> tuple[dict, dict, dict]:
    """
    (MONTHLY_TARGETS, MARKETING_MONTHLY_TARGETS, DAILY_TRACKING_TARGET) 반환.

    DAG 태스크(op_kwargs)와 로컬 스크립트가 동일하게 사용한다.
    """
    # 2026-04 시작 ~ 당월+12개월 범위 자동 생성 (매월 수동 추가 불필요)
    today = pendulum.now("Asia/Seoul")
    end_ym = f"{today.year + (today.month + 11) // 12:04d}-{(today.month + 11) % 12 + 1:02d}"
    month_days = _make_month_days("2026-04", end_ym)
    # 2026-04 실제 영업일수는 14일 (오픈 기준 보정)
    month_days["2026-04"] = 14

    monthly_targets = {
        ym: {
            "sale":   DAILY_TARGET["sale"]   * days,
            "aov":    DAILY_TARGET["aov"],
            "orders": DAILY_TARGET["orders"] * days,

            "lunch_sale":   DAILY_TARGET["lunch_sale"]   * days,
            "lunch_orders": DAILY_TARGET["lunch_orders"] * days,
            "lunch_aov":    DAILY_TARGET["lunch_aov"],

            "dinner_sale":   DAILY_TARGET["dinner_sale"]   * days,
            "dinner_orders": DAILY_TARGET["dinner_orders"] * days,
            "dinner_aov":    DAILY_TARGET["dinner_aov"],
        }
        for ym, days in month_days.items()
    }

    marketing_monthly_targets = {
        ym: {k: v * days for k, v in MARKETING_DAILY_TARGET.items()}
        for ym, days in month_days.items()
    }

    # 일단위 트래킹용 일목표 (DB_Hall_Daily_Excel 전달)
    daily_tracking_target = {
        "sale":          DAILY_TARGET["sale"],           # 일 매출
        "lunch_orders":  DAILY_TARGET["lunch_orders"],   # 점심 영수건수
        "lunch_aov":     DAILY_TARGET["lunch_aov"],      # 점심 테이블단가
        "dinner_orders": DAILY_TARGET["dinner_orders"],  # 저녁 영수건수
        "dinner_aov":    DAILY_TARGET["dinner_aov"],     # 저녁 테이블단가
        "place":         MARKETING_DAILY_TARGET["플레이스_유입"],
        "홍보물_배포":   300,                              # 홍보물 배포(누적) 기간 목표
        "coupon":        MARKETING_DAILY_TARGET["쿠폰_회수"],
        "insta":         MARKETING_DAILY_TARGET["인스타_노출"],
        "karrot":        MARKETING_DAILY_TARGET["당근_노출"],
        "naver":         MARKETING_DAILY_TARGET["네이버_오더"],
    }

    return monthly_targets, marketing_monthly_targets, daily_tracking_target
