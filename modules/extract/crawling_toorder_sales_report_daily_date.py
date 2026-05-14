"""
투오더 일별매출보고서 크롤링 - crawling_toorder_sales_report 모듈의 /date 페이지 함수 재export
"""

from modules.extract.crawling_toorder_sales_report import (
    run_crawling_daily_date_page as run_crawling_single_date,
)

__all__ = ["run_crawling_single_date"]
