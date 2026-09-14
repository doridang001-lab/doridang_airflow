"""수집원별 날짜 결손·급감 감시.

2026-09-11 장애에서 드러난 구멍: 투오더 종합보고서가 6일(09-05~09-10) 결측이었는데
DAG는 매일 success로 끝나 아무도 몰랐다. 같은 날 배민 0건·쿠팡 절반·OKPOS 0건도
사람이 parquet를 열어 세어 보고서야 알았다. 스케줄 지연은
Strategy_ScheduleGuard_01_Overdue_Dags가 잡지만 "데이터가 비었는지"는 아무도 보지 않았다.

이 모듈은 주요 수집원의 최근 N일 날짜별 건수를 세어
  - 결손(0건)
  - 전주 동일 요일 대비 급감(기본 50% 미만)
을 판정하고 텔레그램으로 알린다. DB_CollectionCompare.validate_toorder_freshness
(한 소스만 보던 것)를 전 수집원으로 일반화한 것이다.

성능: 감시 창이 걸치는 ym= 파티션만, 날짜 컬럼만 읽는다.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_CollectionCompare import (
    _baemin_order_date,
    _coupang_order_date,
)
from modules.transform.utility.notifier import send_telegram
from modules.transform.utility.paths import ANALYTICS_DB, BAEMIN_ORDERS_DB, COUPANG_ORDERS_DB

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")

# 감시 창(일). 전주 동일 요일 비교를 위해 실제로는 +7일치를 더 센다.
DEFAULT_WINDOW_DAYS = 7
# 전주 동일 요일 대비 이 비율 미만이면 급감으로 본다.
DROP_RATIO = 0.5
# 주문 데이터는 다음날 아침에 들어오므로 오늘(D)은 보지 않고 D-1까지만 본다.
DEFAULT_EXPECTED_LAG_DAYS = 1

POSFEED_DIR = ANALYTICS_DB / "posfeed_sales"
OKPOS_DIR = ANALYTICS_DB / "okpos_sales_raw"
TOORDER_DAILY_DIR = ANALYTICS_DB / "toorder_daily_sales"


@dataclass(frozen=True)
class FreshnessSource:
    key: str
    label: str
    counter: Callable[[list[str]], Counter]
    expected_lag_days: int = DEFAULT_EXPECTED_LAG_DAYS


@dataclass
class FreshnessIssue:
    source: str
    date: str
    kind: str  # "missing" | "drop"
    count: int
    baseline: int | None = None

    def describe(self) -> str:
        if self.kind == "missing":
            return f"{self.source} {self.date}: 0건 (결손)"
        return f"{self.source} {self.date}: {self.count}건 (전주 {self.baseline}건 대비 급감)"


@dataclass
class FreshnessReport:
    window: list[str]
    counts: dict[str, Counter] = field(default_factory=dict)
    issues: list[FreshnessIssue] = field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        return bool(self.issues)


# ------------------------------------------------------------------
# 날짜 유틸
# ------------------------------------------------------------------

def _date_range(end: pendulum.Date, days: int) -> list[str]:
    return [end.subtract(days=offset).to_date_string() for offset in range(days - 1, -1, -1)]


def _months_for(dates: Iterable[str]) -> list[str]:
    return sorted({d[:7] for d in dates})


def _count_days(
    files: Iterable[Path],
    reader: Callable[[Path], pd.Series | None],
) -> Counter:
    """파일마다 날짜 시리즈를 얻어 날짜별 건수를 합산한다. 파일 하나가 깨져도 전체를 죽이지 않는다."""
    total: Counter = Counter()
    for path in files:
        try:
            dates = reader(path)
        except Exception as exc:
            logger.warning("결손 감시 파일 읽기 실패, 스킵: %s | %s", path, exc)
            continue
        if dates is None:
            continue
        total.update(dates.dropna().astype(str).tolist())
    return total


# ------------------------------------------------------------------
# 소스별 카운터 — 인자는 감시할 날짜 목록, 반환은 날짜별 건수
# ------------------------------------------------------------------

def _parquet_dates(path: Path, column: str, parser: Callable[[pd.Series], pd.Series]) -> pd.Series | None:
    # _no_data 마커 파일은 같은 폴더에 같은 이름 패턴으로 있지만 주문 컬럼이 없다.
    try:
        df = pd.read_parquet(path, columns=[column])
    except (KeyError, ValueError):
        return None
    if df.empty:
        return None
    return parser(df[column])


def _glob_partitions(root: Path, dates: list[str], pattern: str) -> list[Path]:
    return [p for ym in _months_for(dates) for p in root.glob(pattern.format(ym=ym))]


def count_baemin(dates: list[str], root: Path = BAEMIN_ORDERS_DB) -> Counter:
    files = _glob_partitions(root, dates, "brand=*/store=*/ym={ym}/orders_*.parquet")
    return _count_days(files, lambda p: _parquet_dates(p, "주문시각", _baemin_order_date))


def count_coupang(dates: list[str], root: Path = COUPANG_ORDERS_DB) -> Counter:
    files = _glob_partitions(root, dates, "brand=*/store=*/ym={ym}/orders_*.parquet")
    return _count_days(files, lambda p: _parquet_dates(p, "order_date", _coupang_order_date))


def _csv_dates(path: Path, column: str) -> pd.Series | None:
    try:
        df = pd.read_csv(path, usecols=[column], low_memory=False)
    except ValueError:
        return None
    if df.empty:
        return None
    return df[column].astype(str).str.slice(0, 10)


def count_posfeed(dates: list[str], root: Path = POSFEED_DIR) -> Counter:
    files = _glob_partitions(root, dates, "brand=*/store=*/ym={ym}/posfeed_orders.csv")
    return _count_days(files, lambda p: _csv_dates(p, "주문등록 시각"))


def count_okpos(dates: list[str], root: Path = OKPOS_DIR) -> Counter:
    files = _glob_partitions(root, dates, "brand=*/store=*/ym={ym}/okpos_daily.csv")
    return _count_days(files, lambda p: _csv_dates(p, "sale_date"))


def count_toorder_daily(dates: list[str], root: Path = TOORDER_DAILY_DIR) -> Counter:
    """파일이 날짜 단위라 존재 여부 + 데이터 행 수로 센다."""
    total: Counter = Counter()
    for date in dates:
        path = root / ("toorder_daily_sales_" + date.replace("-", "") + ".csv")
        if not path.exists():
            continue
        try:
            with open(path, encoding="utf-8-sig") as handle:
                rows = sum(1 for _ in handle) - 1
        except Exception as exc:
            logger.warning("결손 감시 파일 읽기 실패, 스킵: %s | %s", path, exc)
            continue
        total[date] = max(rows, 0)
    return total


SOURCES: list[FreshnessSource] = [
    FreshnessSource("baemin", "배민 주문", count_baemin),
    FreshnessSource("coupang", "쿠팡 주문", count_coupang),
    FreshnessSource("posfeed", "posfeed", count_posfeed),
    FreshnessSource("okpos", "OKPOS 일매출", count_okpos),
    FreshnessSource("toorder_daily", "투오더 일매출", count_toorder_daily),
]


# ------------------------------------------------------------------
# 판정
# ------------------------------------------------------------------

def evaluate_source(
    source: FreshnessSource,
    counts: Counter,
    window: list[str],
    *,
    drop_ratio: float = DROP_RATIO,
) -> list[FreshnessIssue]:
    issues: list[FreshnessIssue] = []
    for date in window:
        count = int(counts.get(date, 0))
        if count == 0:
            issues.append(FreshnessIssue(source.label, date, "missing", 0))
            continue
        baseline_date = pendulum.parse(date).subtract(days=7).to_date_string()
        baseline = int(counts.get(baseline_date, 0))
        if baseline > 0 and count < baseline * drop_ratio:
            issues.append(FreshnessIssue(source.label, date, "drop", count, baseline))
    return issues


def collect_freshness(
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    now: pendulum.DateTime | None = None,
    sources: Iterable[FreshnessSource] = SOURCES,
    drop_ratio: float = DROP_RATIO,
) -> FreshnessReport:
    current = (now or pendulum.now(KST)).in_timezone(KST).date()
    report = FreshnessReport(window=[])
    for source in sources:
        end = current.subtract(days=source.expected_lag_days)
        window = _date_range(end, window_days)
        # 전주 비교용으로 7일 더 앞까지 센다
        scan_dates = _date_range(end, window_days + 7)
        counts = source.counter(scan_dates)
        report.counts[source.key] = counts
        report.window = window
        report.issues.extend(evaluate_source(source, counts, window, drop_ratio=drop_ratio))
    return report


def format_report(report: FreshnessReport, *, labels: dict[str, str] | None = None) -> str:
    labels = labels or {s.key: s.label for s in SOURCES}
    header = f"window={report.window[0]}~{report.window[-1]}" if report.window else "window=-"
    lines = ["[수집 결손 감시]", header]
    for key, counts in report.counts.items():
        row = "  ".join(f"{d[5:]}:{int(counts.get(d, 0))}" for d in report.window)
        lines.append(f"- {labels.get(key, key)}: {row}")
    if report.issues:
        lines.append("")
        lines.append(f"이상 {len(report.issues)}건:")
        lines.extend(f"  ! {issue.describe()}" for issue in report.issues)
    else:
        lines.append("이상 없음")
    return "\n".join(lines)


# ------------------------------------------------------------------
# Airflow 태스크 진입점
# ------------------------------------------------------------------

def check_collection_freshness(**context) -> str:
    """dag_run.conf: {"days": 14, "dry_run": true} 로 창/알림 여부를 조정한다."""
    conf = getattr(context.get("dag_run"), "conf", None) or {}
    window_days = int(conf.get("days", DEFAULT_WINDOW_DAYS))
    dry_run = bool(conf.get("dry_run", False))

    report = collect_freshness(window_days=window_days)
    body = format_report(report)
    if report.has_issues:
        logger.warning(body)
        if not dry_run:
            try:
                send_telegram(body)
            except Exception as exc:  # 알림 실패가 감시 자체를 죽이면 안 된다
                logger.warning("수집 결손 알림 실패: %s", exc)
    else:
        logger.info(body)
    return body
