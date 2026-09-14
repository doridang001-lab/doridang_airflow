import pandas as pd
import sys
import types


class _AirflowException(Exception):
    pass


sys.modules.setdefault("airflow", types.SimpleNamespace())
sys.modules.setdefault(
    "airflow.exceptions",
    types.SimpleNamespace(AirflowException=_AirflowException, AirflowFailException=_AirflowException),
)
sys.modules.setdefault("undetected_chromedriver", types.SimpleNamespace(ChromeOptions=object))
sys.modules.setdefault("selenium", types.SimpleNamespace())
sys.modules.setdefault("selenium.webdriver", types.SimpleNamespace())
sys.modules.setdefault("selenium.webdriver.common", types.SimpleNamespace())
sys.modules.setdefault("selenium.webdriver.common.by", types.SimpleNamespace(By=types.SimpleNamespace()))
sys.modules.setdefault("selenium.webdriver.common.keys", types.SimpleNamespace(Keys=types.SimpleNamespace(RETURN="RETURN")))
_expected_conditions = types.SimpleNamespace()
sys.modules.setdefault("selenium.webdriver.support", types.SimpleNamespace(expected_conditions=_expected_conditions))
sys.modules.setdefault("selenium.webdriver.support.expected_conditions", _expected_conditions)
sys.modules.setdefault("selenium.webdriver.support.ui", types.SimpleNamespace(WebDriverWait=object))
sys.modules.setdefault(
    "modules.transform.utility.selenium_uc",
    types.SimpleNamespace(launch_uc_chrome=lambda *args, **kwargs: None),
)

from modules.transform.pipelines.sales.Sales_ToOrder_Review_collect import (
    _validate_same_month_no_review_loss,
)


def _row(writer: str, review_text: str = "") -> dict[str, object]:
    return {
        "작성일자": "2026-07-11",
        "매장명": "교대점",
        "채널": "배달의민족",
        "작성자": writer,
        "별점": "5.0",
        "주문메뉴": "[복날한정] 1인 미나리 수삼 백숙",
        "리뷰내용": review_text,
    }


def test_same_month_guard_blocks_missing_blank_review(tmp_path):
    existing_path = tmp_path / "toorder_voc_202607.parquet"
    new_path = tmp_path / "tmp_202607.parquet"
    pd.DataFrame([_row("마스", "")]).to_parquet(existing_path, index=False)
    pd.DataFrame([], columns=list(_row("마스").keys())).to_parquet(new_path, index=False)

    result = _validate_same_month_no_review_loss(new_path, existing_path)

    assert result["ok"] is False
    assert result["existing_review_count"] == 1
    assert result["new_review_count"] == 0
    assert result["missing_count"] == 1
    assert result["missing_samples"][0]["작성자"] == "마스"
    assert result["missing_samples"][0]["리뷰내용"] == ""


def test_same_month_guard_allows_existing_reviews_to_remain(tmp_path):
    existing_path = tmp_path / "toorder_voc_202607.parquet"
    new_path = tmp_path / "tmp_202607.parquet"
    pd.DataFrame([_row("마스", "")]).to_parquet(existing_path, index=False)
    pd.DataFrame([_row("마스", ""), _row("새작성자", "맛있어요")]).to_parquet(new_path, index=False)

    result = _validate_same_month_no_review_loss(new_path, existing_path)

    assert result["ok"] is True
    assert result["existing_review_count"] == 1
    assert result["new_review_count"] == 2
    assert result["missing_count"] == 0
