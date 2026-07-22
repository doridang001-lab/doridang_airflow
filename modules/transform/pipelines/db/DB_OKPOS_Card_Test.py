"""
OKPOS card approval test pipeline.
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from modules.transform.pipelines.db.DB_OKPOS_Sales import (
    DOWNLOAD_TIMEOUT,
    WAIT_TIMEOUT,
    _click_search_btn,
    _dismiss_alert,
    _kst_now,
    _launch_browser,
    _login,
    _select_store as _select_store_common,
    _setup_download_dir,
    _wait_for_download,
)
from modules.transform.utility.paths import ANALYTICS_DB, MART_DB, TEMP_DIR

logger = logging.getLogger(__name__)

OKPOS_CARD_APPROVAL_URL = "https://my.okpos.co.kr/asp/sales/approvals"
OKPOS_CARD_EXPECTED_STEMS = ("승인현황", "매장현황")
OKPOS_CARD_DOWNLOAD_DIR = TEMP_DIR / "okpos_card_test_download"
OKPOS_CARD_OUTPUT_DIR = ANALYTICS_DB / "okpos_card"
OKPOS_CUSTOMER_MART_DIR = MART_DB / "okpos_cutomer"
OKPOS_CUSTOMER_MART_CSV = OKPOS_CUSTOMER_MART_DIR / "okpos_customer_daily.csv"
OKPOS_CARD_MANUAL_PATTERNS = ("매장현황_*월*.xlsx",)
OKPOS_CARD_BRAND = "도리당"
OKPOS_CARD_STORE = {"name": "도리당 송파삼전점", "shopCd": "LQ9726"}
OKPOS_CARD_STORE_SHORT = "송파삼전점"
OKPOS_CARD_MIN_VALIDATE_DATE = "2026-04-17"


def _resolve_sale_date(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    sale_date = str(conf.get("sale_date") or "").strip()
    if sale_date:
        try:
            datetime.strptime(sale_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"sale_date must be YYYY-MM-DD: {sale_date}") from exc
        source = "dag_run.conf.sale_date"
    else:
        sale_date = (_kst_now() - timedelta(days=1)).strftime("%Y-%m-%d")
        source = "kst_yesterday"

    context["ti"].xcom_push(key="sale_date", value=sale_date)
    logger.info("OKPOS card test sale_date resolved: %s (%s)", sale_date, source)
    return sale_date


def _cleanup_download_dir(download_dir: Path) -> None:
    download_dir.mkdir(parents=True, exist_ok=True)
    for stem in OKPOS_CARD_EXPECTED_STEMS:
        for path in download_dir.glob(f"{stem}*"):
            if path.is_file():
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    continue
    for pattern in ("*.crdownload", "*.tmp", "*.part", "downloads.html", "download.html"):
        for path in download_dir.glob(pattern):
            if path.is_file():
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    continue


def _filename_matches(path: Path) -> bool:
    return path.suffix.lower() == ".xlsx" and any(stem in path.stem for stem in OKPOS_CARD_EXPECTED_STEMS)


def _get_shop_input_value(driver) -> str:
    for sel in [(By.ID, "shopNms"), (By.NAME, "shopNms")]:
        try:
            el = driver.find_element(*sel)
            value = (el.get_attribute("value") or el.text or "").strip()
            if value:
                return value
        except Exception:
            continue
    return ""


def _get_shop_codes_value(driver) -> str:
    for sel in [(By.ID, "sfShopCds"), (By.NAME, "sfShopCds")]:
        try:
            el = driver.find_element(*sel)
            value = (el.get_attribute("value") or el.text or "").strip()
            if value:
                return value
        except Exception:
            continue
    return ""


def _write_debug_artifacts(driver, download_dir: Path, prefix: str) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = download_dir / f"{prefix}_{ts}"
    try:
        driver.save_screenshot(str(base.with_suffix(".png")))
    except Exception:
        pass
    try:
        base.with_suffix(".html").write_text(driver.page_source or "", encoding="utf-8", errors="replace")
    except Exception:
        pass
    try:
        body_text = driver.find_element(By.TAG_NAME, "body").text
        base.with_suffix(".txt").write_text(body_text, encoding="utf-8", errors="replace")
    except Exception:
        pass


def _set_date_hyphen(driver, wait: WebDriverWait, sale_date: str, date_input_id: str) -> None:
    """날짜 입력 필드에 하이픈 포맷(YYYY-MM-DD)으로 직접 설정.

    OKPOS 승인현황 필드는 native value가 하이픈 포맷이라 슬래시로 넣으면
    datepicker 내부 모델이 갱신되지 않아 조회가 빈 결과로 떨어진다.
    """
    input_el = None
    for sel in [
        (By.ID, date_input_id),
        (By.NAME, date_input_id),
        (By.CSS_SELECTOR, f"[id='{date_input_id}']"),
    ]:
        try:
            input_el = wait.until(EC.presence_of_element_located(sel))
            break
        except TimeoutException:
            continue
    if input_el is None:
        raise TimeoutException(f"날짜 입력 필드를 찾을 수 없습니다: id={date_input_id}")

    driver.execute_script(
        "arguments[0].value = arguments[1];"
        "arguments[0].dispatchEvent(new Event('input',  {bubbles:true}));"
        "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
        input_el, sale_date,
    )
    time.sleep(0.3)
    new_val = input_el.get_attribute("value") or ""
    logger.info("날짜 설정 완료 (하이픈): id=%s → value=%s", date_input_id, new_val)


def _click_store_select_button(driver, wait: WebDriverWait) -> None:
    selectors = [
        (By.XPATH, "//button[normalize-space(text())='매장선택']"),
        (By.XPATH, "//a[normalize-space(text())='매장선택']"),
        (By.ID, "shopNms"),
        (By.NAME, "shopNms"),
    ]
    for sel in selectors:
        try:
            btn = wait.until(EC.element_to_be_clickable(sel))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1)
            _dismiss_alert(driver)
            return
        except Exception:
            continue
    raise TimeoutException("매장선택 버튼을 찾을 수 없습니다.")


def _select_store_in_popup(driver, wait: WebDriverWait, shop_cd: str) -> None:
    logger.info("OKPOS card store popup selection start: shopCd=%s", shop_cd)
    cell_candidates = [
        f"//tr[td[normalize-space(text())='{shop_cd}']]",
        f"//tr[td[contains(normalize-space(.),'{shop_cd}')]]",
        f"//td[normalize-space(text())='{shop_cd}']",
        f"//td[contains(normalize-space(.),'{shop_cd}')]",
    ]
    target = None
    for xpath in cell_candidates:
        try:
            target = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            if target.is_displayed():
                break
        except Exception:
            target = None
    if target is None:
        raise TimeoutException(f"shopCd={shop_cd} 행을 찾을 수 없습니다.")

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", target)
    time.sleep(0.2)
    driver.execute_script("arguments[0].click();", target)
    logger.info("OKPOS card store row clicked: shopCd=%s", shop_cd)
    time.sleep(0.5)

    confirm_selectors = [
        (By.XPATH, "//button[normalize-space(text())='매장 선택']"),
        (By.XPATH, "//a[normalize-space(text())='매장 선택']"),
        (By.XPATH, "//button[contains(@onclick,'select') and contains(.,'매장')]"),
        (By.XPATH, "//input[@type='button' and @value='매장 선택']"),
    ]
    clicked = False
    for sel in confirm_selectors:
        try:
            btn = wait.until(EC.element_to_be_clickable(sel))
            driver.execute_script("arguments[0].click();", btn)
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        raise TimeoutException("팝업 내 '매장 선택' 버튼을 찾을 수 없습니다.")

    time.sleep(1)
    _dismiss_alert(driver)

    input_selectors = [(By.ID, "shopNms"), (By.NAME, "shopNms")]
    for sel in input_selectors:
        try:
            wait.until(lambda d: (d.find_element(*sel).get_attribute("value") or "").strip() != "")
            logger.info("OKPOS card selected store reflected in input: %s", _get_shop_input_value(driver))
            return
        except Exception:
            continue
    logger.warning("매장 선택 값 반영 여부를 확인하지 못했습니다. common selector fallback을 시도합니다.")
    if not _select_store_common(driver, wait, shop_cd):
        raise TimeoutException(f"shopCd={shop_cd} 선택 fallback도 실패했습니다.")
    logger.info("OKPOS card common store selector fallback succeeded: %s", _get_shop_input_value(driver))


def _wait_for_store_in_results(driver, wait: WebDriverWait, store_name: str, shop_cd: str, download_dir: Path) -> None:
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    store_short = store_name.split()[-1] if " " in store_name else store_name
    patterns = [store_name, store_short, shop_cd]
    end_time = time.time() + WAIT_TIMEOUT
    while time.time() < end_time:
        # 1) body.text 체크 (store pre-filter 시 IBSheet가 매장명을 가시 텍스트에 노출)
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text or ""
        except Exception:
            body_text = ""
        for pattern in patterns:
            if pattern and pattern in body_text:
                logger.info("조회 결과 패턴 확인: %s", pattern)
                return
        # 2) JS querySelectorAll fallback (body.text가 IBSheet td를 못 잡는 경우)
        js_found = driver.execute_script(
            """
            var shopCd = arguments[0], storeShort = arguments[1];
            var cdTds = document.querySelectorAll('td[class*="HideCol0shopCd"]');
            for (var i = 0; i < cdTds.length; i++) {
                var t = (cdTds[i].innerText || cdTds[i].textContent || '').trim();
                if (t === shopCd) return 'shopCd:' + t;
            }
            var nmTds = document.querySelectorAll('td[class*="HideCol0shopNm"]');
            for (var i = 0; i < nmTds.length; i++) {
                var t = (nmTds[i].innerText || nmTds[i].textContent || '').trim();
                if (t.indexOf(storeShort) !== -1) return 'shopNm:' + t;
            }
            var rows = document.querySelectorAll('tr.IBDataRow');
            if (rows.length > 0) return 'IBDataRow:' + rows.length;
            return null;
            """,
            shop_cd,
            store_short,
        )
        if js_found:
            logger.info("IBSheet 데이터 행 확인 (JS): %s", js_found)
            return
        time.sleep(1)

    selected_value = _get_shop_input_value(driver)
    logger.error("조회 결과 검증 실패 | selected_store=%r | url=%s", selected_value, driver.current_url)
    _write_debug_artifacts(driver, download_dir, "okpos_card_result_missing")
    raise TimeoutException(
        f"조회 결과에서 매장명을 찾지 못했습니다: {store_name} | "
        f"selected_store={selected_value!r} | selected_codes={_get_shop_codes_value(driver)!r}"
    )


def _wait_for_popup(driver, wait: WebDriverWait) -> None:
    """매장현황 팝업이 열릴 때까지 대기"""
    selectors = [
        (By.ID, "approvalCardDetailInPeriodPopupClose"),
        (By.XPATH, "//div[contains(@class,'pop-default') and contains(.,'매장현황')]"),
        (By.XPATH, "//button[@id='excelDown']"),
    ]
    for sel in selectors:
        try:
            wait.until(EC.presence_of_element_located(sel))
            logger.info("매장현황 팝업 확인: %s", sel)
            return
        except Exception:
            continue
    raise TimeoutException("매장현황 팝업이 열리지 않았습니다.")


def _open_store_detail_popup(driver, shop_cd: str, store_short: str) -> str:
    """IBSheet8 API로 해당 매장 행 데이터를 읽어 페이지의 cardDetailPopup 로직을 재현.

    IBSheet 그리드의 onClick(evt.col==="shopNm" → cardDetailPopup)은 합성 DOM 이벤트로는
    트리거되지 않으므로, 전역 sheet 객체(id=ibSheetGrid)에서 행을 찾아
    fnPopup('approvalCardDetailInPeriodPopup', 'post', JSON.stringify({...row, ...form}))을 직접 호출.
    """
    return driver.execute_script(
        """
        var shopCd = arguments[0], storeShort = arguments[1];
        var grid = window.ibSheetGrid;
        if (!grid && window.IBSheet && typeof IBSheet.getSheet === 'function') {
            grid = IBSheet.getSheet('ibSheetGrid');
        }
        if (!grid) return 'no-grid';
        if (typeof grid.getDataRows !== 'function' || typeof grid.getRowValue !== 'function') {
            return 'no-api';
        }
        var rows = grid.getDataRows();
        var target = null;
        for (var i = 0; i < rows.length; i++) {
            var cd = (grid.getValue(rows[i], 'shopCd') || '').toString().trim();
            var nm = (grid.getValue(rows[i], 'shopNm') || '').toString();
            if (cd === shopCd || nm.indexOf(storeShort) !== -1) { target = rows[i]; break; }
        }
        if (!target) return 'no-row';
        var rowData = grid.getRowValue(target);
        if (typeof objectifySaleForm !== 'function' || typeof fnPopup !== 'function') {
            return 'no-globals';
        }
        var formData = objectifySaleForm($("#cardSearchForm").serializeArray());
        var reqData = Object.assign({}, rowData, formData);
        fnPopup('approvalCardDetailInPeriodPopup', 'post', JSON.stringify(reqData));
        return 'popup-called:' + (grid.getValue(target, 'shopNm') || '');
        """,
        shop_cd,
        store_short,
    )


def _click_store_row_in_results(driver, wait: WebDriverWait, shop_cd: str, store_name: str, download_dir: Path) -> None:
    """승인현황 IBSheet에서 해당 매장 행 → 매장현황 팝업 오픈 (IBSheet8 API 직접 호출)."""
    store_short = store_name.split()[-1] if " " in store_name else store_name

    result = _open_store_detail_popup(driver, shop_cd, store_short)
    logger.info("매장현황 팝업 호출 결과: %s", result)
    if isinstance(result, str) and result.startswith("popup-called"):
        time.sleep(1)
        _dismiss_alert(driver)
        return

    _write_debug_artifacts(driver, download_dir, "okpos_card_row_click_fail")
    raise TimeoutException(
        f"매장현황 팝업을 열지 못했습니다: shopCd={shop_cd}, name={store_name}, result={result!r}"
    )


def _click_excel_button(driver, wait: WebDriverWait) -> None:
    # id="excelDown" 우선 (매장현황 상세 다운로드 버튼)
    selectors = [
        (By.ID, "excelDown"),
        (By.XPATH, "//button[@id='excelDown']"),
        (By.XPATH, "//button[normalize-space(text())='엑셀']"),
        (By.XPATH, "//a[normalize-space(text())='엑셀']"),
        (By.XPATH, "//button[contains(@onclick,'excel') or contains(@onclick,'Excel') or contains(@onclick,'export')]"),
        (By.XPATH, "//a[contains(@onclick,'excel') or contains(@onclick,'Excel') or contains(@onclick,'export')]"),
    ]
    for sel in selectors:
        try:
            btn = wait.until(EC.element_to_be_clickable(sel))
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.5)
            _dismiss_alert(driver)
            return
        except Exception:
            continue
    raise TimeoutException("엑셀 버튼을 찾을 수 없습니다.")


def _normalize_text(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).replace("\n", " ").strip()
    return re.sub(r"\s+", " ", text)


def _normalize_int(value) -> int:
    text = _normalize_text(value).replace(",", "")
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def _normalize_date(value) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")


def _normalize_time(value) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return text
    return dt.strftime("%H:%M:%S")


def _find_header_row(raw_df: pd.DataFrame) -> int:
    for idx in range(len(raw_df)):
        row_values = [_normalize_text(v) for v in raw_df.iloc[idx].tolist()]
        next_values = [_normalize_text(v) for v in raw_df.iloc[idx + 1].tolist()] if idx + 1 < len(raw_df) else []
        first_ok = row_values and row_values[0].lower().startswith("no")
        second_ok = next_values and next_values[0].lower().startswith("no")
        if first_ok and second_ok and len([v for v in row_values[:6] if v]) >= 3:
            return idx
    raise ValueError("승인현황 엑셀에서 헤더 행을 찾지 못했습니다.")


def _build_flat_columns(raw_df: pd.DataFrame, header_row: int) -> list[str]:
    top = [_normalize_text(v) for v in raw_df.iloc[header_row].tolist()]
    bottom = [_normalize_text(v) for v in raw_df.iloc[header_row + 1].tolist()]
    columns: list[str] = []
    for upper, lower in zip(top, bottom):
        if upper in {"", "No.", "No", "매장코드", "매장명"}:
            col = upper or lower
        elif lower in {"", upper}:
            col = upper
        else:
            col = f"{upper}_{lower}"
        columns.append(col or f"col_{len(columns)}")
    return columns


_MAEJANGHWANG_COLS = [
    "번호", "영업일자", "포스번호", "승인구분", "처리구분",
    "매입사", "카드번호", "요청금액", "봉사료", "부가세",
    "할부구분", "할부개월수", "유효기간", "승인일자", "승인시각",
    "승인번호", "승인금액",
]
_SEUNGIN_COLS = [
    "No", "매장코드", "매장명",
    "전체_총건수", "전체_요청금액", "전체_승인금액",
    "승인_총건수", "승인_요청금액", "승인_승인금액",
    "취소_총건수", "취소_요청금액", "취소_승인금액",
]


def _parse_okpos_card_workbook(src: Path, sale_date: str) -> pd.DataFrame:
    raw_df = pd.read_excel(src, header=None, dtype=str, engine="openpyxl")
    logger.info("엑셀 원본 shape: %s | file: %s", raw_df.shape, src.name)
    header_row = _find_header_row(raw_df)
    data_start = header_row + 2
    n_cols = raw_df.shape[1]
    logger.info("header_row=%d | data_start=%d | total_cols=%d", header_row, data_start, n_cols)

    if n_cols >= 15:
        return _parse_maejanghwang(raw_df, data_start, n_cols, sale_date)
    return _parse_seunginhwang(raw_df, data_start, n_cols, sale_date)


def _parse_maejanghwang(raw_df: pd.DataFrame, data_start: int, n_cols: int, sale_date: str) -> pd.DataFrame:
    """매장현황 엑셀: 17컬럼 거래내역 포맷 (1·2행 병합헤더, 2행 기준 컬럼명 사용)"""
    slice_cols = min(17, n_cols)
    df = raw_df.iloc[data_start:, :slice_cols].copy()
    df.columns = _MAEJANGHWANG_COLS[:slice_cols]
    df = df.dropna(how="all")
    # 합계 행 제외: 승인일자가 비어있는 행(매입사·카드번호 등도 공란인 집계 행)
    if "승인일자" in df.columns:
        df = df[df["승인일자"].map(_normalize_text) != ""].copy()

    for col in ["요청금액", "봉사료", "부가세", "승인금액", "할부개월수"]:
        if col in df.columns:
            df[col] = df[col].map(_normalize_int)

    out = df.drop(columns=["번호"], errors="ignore").copy()
    if sale_date:
        sale_dates = pd.Series([sale_date] * len(out), index=out.index)
    else:
        sale_dates = out["영업일자"].map(_normalize_date)
        out = out[sale_dates != ""].copy()
        sale_dates = sale_dates.loc[out.index]
    out["영업일자"] = out["영업일자"].map(_normalize_date)
    out["승인일자"] = out["승인일자"].map(_normalize_date)
    out["승인시각"] = out["승인시각"].map(_normalize_time)
    out.insert(0, "ym", sale_dates.str[:7])
    out.insert(0, "store", OKPOS_CARD_STORE_SHORT)
    out.insert(0, "brand", OKPOS_CARD_BRAND)
    out.insert(0, "sale_date", sale_dates)
    logger.info("매장현황 포맷 파싱 완료: %d건", len(out))
    return out.reset_index(drop=True)


def _parse_seunginhwang(raw_df: pd.DataFrame, data_start: int, n_cols: int, sale_date: str) -> pd.DataFrame:
    """승인현황 엑셀: 12컬럼 매장별 집계 포맷"""
    slice_cols = min(12, n_cols)
    df = raw_df.iloc[data_start:, :slice_cols].copy()
    df.columns = _SEUNGIN_COLS[:slice_cols]
    df = df.dropna(how="all")
    df["매장명"] = df["매장명"].map(_normalize_text)
    df["매장코드"] = df["매장코드"].map(_normalize_text)

    df_named = df[~df["매장명"].isin(["", "합계"])].copy()
    logger.info("매장 목록: %s", df_named[["매장코드", "매장명"]].drop_duplicates().to_dict("records"))

    by_code = df_named[df_named["매장코드"] == OKPOS_CARD_STORE["shopCd"]].copy()
    if not by_code.empty:
        df = by_code
    else:
        by_name = df_named[df_named["매장명"].str.contains(OKPOS_CARD_STORE_SHORT, na=False)].copy()
        if not by_name.empty:
            df = by_name
        else:
            # 단일 매장 필터 시 합계 행만 반환하는 경우: store 메타 주입
            df_all = df.copy()
            if df_all.empty:
                raise ValueError(f"승인현황 엑셀에 데이터 행이 없습니다. shape={raw_df.shape}")
            df = df_all.head(1).copy()
            df["매장코드"] = OKPOS_CARD_STORE["shopCd"]
            df["매장명"] = OKPOS_CARD_STORE["name"]
            logger.warning("단일 매장 합계 행 fallback 사용: %s", OKPOS_CARD_STORE)

    numeric_cols = [
        "전체_총건수", "전체_요청금액", "전체_승인금액",
        "승인_총건수", "승인_요청금액", "승인_승인금액",
        "취소_총건수", "취소_요청금액", "취소_승인금액",
    ]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0
        df[col] = df[col].map(_normalize_int)

    out = df[["매장코드", "매장명"] + numeric_cols].copy()
    out.insert(0, "sale_date", sale_date)
    out.insert(1, "brand", OKPOS_CARD_BRAND)
    out.insert(2, "store", OKPOS_CARD_STORE_SHORT)
    out.insert(3, "ym", sale_date[:7])
    return out.reset_index(drop=True)


def _okpos_card_partition_dir(ym: str) -> Path:
    return (
        OKPOS_CARD_OUTPUT_DIR
        / f"brand={OKPOS_CARD_BRAND}"
        / f"store={OKPOS_CARD_STORE_SHORT}"
        / f"ym={ym}"
    )


def _okpos_card_csv_path(ym: str) -> Path:
    return _okpos_card_partition_dir(ym) / f"approvals_{ym}.csv"


def _is_maejanghwang_df(df: pd.DataFrame) -> bool:
    return {"매입사", "카드번호", "승인일자", "승인시각", "승인번호", "승인금액"}.issubset(df.columns)


def _canonicalize_card_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["sale_date", "영업일자", "승인일자"]:
        if col in out.columns:
            out[col] = out[col].map(_normalize_date)
    if "승인시각" in out.columns:
        out["승인시각"] = out["승인시각"].map(_normalize_time)
    for col in ["brand", "store", "ym"]:
        if col in out.columns:
            out[col] = out[col].map(_normalize_text)
    for col in ["매입사", "카드번호", "승인구분", "처리구분", "승인번호"]:
        if col in out.columns:
            out[col] = out[col].map(_normalize_text)
    if "ym" not in out.columns and "sale_date" in out.columns:
        out["ym"] = out["sale_date"].astype(str).str[:7]
    if "brand" not in out.columns:
        out.insert(1, "brand", OKPOS_CARD_BRAND)
    if "store" not in out.columns:
        out.insert(2, "store", OKPOS_CARD_STORE_SHORT)
    return out


def _apply_purchase_classification(df: pd.DataFrame) -> pd.DataFrame:
    out = _canonicalize_card_df(df)
    if not _is_maejanghwang_df(out):
        return out

    if "구분" in out.columns:
        out = out.drop(columns=["구분"])

    sort_cols = [c for c in ["sale_date", "승인일자", "승인시각", "승인번호"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, kind="stable").reset_index(drop=True)

    out["_card_key"] = out["매입사"].astype(str).str.strip() + "_" + out["카드번호"].astype(str).str.strip()
    approved = out["승인구분"].astype(str).str.strip() == "승인"
    valid_key = out["_card_key"].str.strip("_") != ""
    rank = out.loc[approved & valid_key].groupby("_card_key", sort=False).cumcount()
    out["구분"] = ""
    out.loc[approved & valid_key, "구분"] = rank.map(lambda n: "신규" if int(n) == 0 else "재구매").to_numpy()
    out = out.drop(columns=["_card_key"])
    return out


def _sort_card_df(df: pd.DataFrame) -> pd.DataFrame:
    sort_cols = (
        ["sale_date", "승인일자", "승인시각", "승인번호"]
        if "승인일자" in df.columns
        else ["sale_date", "매장코드"]
    )
    sort_cols = [c for c in sort_cols if c in df.columns]
    if sort_cols:
        df = df.sort_values(by=sort_cols, kind="stable")
    return df.reset_index(drop=True)


def _dedupe_card_df(df: pd.DataFrame) -> pd.DataFrame:
    subset = [
        c for c in [
            "sale_date", "매입사", "카드번호", "승인일자", "승인시각",
            "승인번호", "승인금액", "승인구분", "처리구분",
        ]
        if c in df.columns
    ]
    if len(subset) >= 3:
        return df.drop_duplicates(subset=subset, keep="last").reset_index(drop=True)
    return df.drop_duplicates(keep="last").reset_index(drop=True)


def _load_store_history() -> pd.DataFrame:
    base = OKPOS_CARD_OUTPUT_DIR / f"brand={OKPOS_CARD_BRAND}" / f"store={OKPOS_CARD_STORE_SHORT}"
    frames: list[pd.DataFrame] = []
    for path in sorted(base.glob("ym=*/approvals_*.csv")):
        try:
            frames.append(pd.read_csv(path, dtype=str, encoding="utf-8-sig"))
        except Exception as exc:
            logger.warning("OKPOS card history csv read skipped: %s (%s)", path, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _rewrite_store_history_with_classification() -> int:
    history = _load_store_history()
    if history.empty or "ym" not in history.columns:
        return 0
    history = _dedupe_card_df(_apply_purchase_classification(history))
    written = 0
    for ym, part in history.groupby("ym", sort=True):
        ym = _normalize_text(ym)
        if not ym:
            continue
        dest_dir = _okpos_card_partition_dir(ym)
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = _okpos_card_csv_path(ym)
        part = _sort_card_df(part.copy())
        part.to_csv(dest, index=False, encoding="utf-8-sig")
        written += 1
        logger.info("OKPOS card history rewrote: %s | rows=%d", dest, len(part))
    return written


def build_okpos_customer_mart(**context) -> str:
    """전체 카드 승인 이력 기준 일별 고객 신규/재구매 마트 1개 파일을 생성한다."""
    history = _load_store_history()
    if history.empty:
        raise ValueError("OKPOS customer mart 생성 실패: 저장된 okpos_card 이력이 없습니다.")

    history = _apply_purchase_classification(history)
    if not _is_maejanghwang_df(history):
        raise ValueError("OKPOS customer mart 생성 실패: 매장현황 거래 컬럼이 없습니다.")

    approved = history[history["승인구분"].astype(str).str.strip() == "승인"].copy()
    approved["키"] = approved["매입사"].astype(str).str.strip() + "_" + approved["카드번호"].astype(str).str.strip()
    approved = approved[(approved["sale_date"].astype(str).str.strip() != "") & (approved["키"].str.strip("_") != "")].copy()

    if approved.empty:
        raise ValueError("OKPOS customer mart 생성 실패: 승인 거래가 없습니다.")

    first_dates = approved.groupby("키", sort=False)["sale_date"].min().to_dict()
    mart = approved[["sale_date", "키"]].drop_duplicates(subset=["sale_date", "키"], keep="first").copy()
    mart = mart.rename(columns={"sale_date": "일별"})
    mart["신규재구매 구분"] = mart.apply(
        lambda row: "신규" if row["일별"] == first_dates.get(row["키"]) else "재구매",
        axis=1,
    )
    mart = mart.sort_values(["일별", "키"], kind="stable").reset_index(drop=True)
    mart["지역명"] = OKPOS_CARD_STORE_SHORT

    OKPOS_CUSTOMER_MART_DIR.mkdir(parents=True, exist_ok=True)
    mart.to_csv(OKPOS_CUSTOMER_MART_CSV, index=False, encoding="utf-8-sig")

    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key="okpos_customer_mart_path", value=str(OKPOS_CUSTOMER_MART_CSV))
    logger.info("OKPOS customer mart saved: %s | rows=%d", OKPOS_CUSTOMER_MART_CSV, len(mart))
    return f"OKPOS customer mart 생성 완료: rows={len(mart)}, path={OKPOS_CUSTOMER_MART_CSV}"


def _upsert_okpos_card_csv(normalized: pd.DataFrame) -> list[Path]:
    if normalized.empty:
        return []

    normalized = _canonicalize_card_df(normalized)
    if "sale_date" not in normalized.columns or "ym" not in normalized.columns:
        raise ValueError("OKPOS card csv upsert requires sale_date and ym columns.")

    saved: list[Path] = []
    for ym, part in normalized.groupby("ym", sort=True):
        ym = _normalize_text(ym)
        if not ym:
            continue
        dest_dir = _okpos_card_partition_dir(ym)
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = _okpos_card_csv_path(ym)

        if dest.exists():
            existing = pd.read_csv(dest, dtype=str, encoding="utf-8-sig")
            if "sale_date" in existing.columns:
                replace_dates = set(part["sale_date"].astype(str).str.strip())
                existing = existing[~existing["sale_date"].astype(str).str.strip().isin(replace_dates)].copy()
            combined = pd.concat([existing, part], ignore_index=True)
        else:
            combined = part.copy()

        combined = _dedupe_card_df(_sort_card_df(_canonicalize_card_df(combined)))
        combined.to_csv(dest, index=False, encoding="utf-8-sig")
        saved.append(dest)
        logger.info("OKPOS card approval csv upserted: %s | rows=%d", dest, len(combined))

    _rewrite_store_history_with_classification()
    return saved


def _manual_watch_dirs(conf: dict) -> list[Path]:
    raw_dirs = [
        conf.get("manual_watch_dir"),
        os.getenv("OKPOS_CARD_MANUAL_WATCH_DIR"),
        "/opt/airflow/manual_download",
        "/opt/airflow/download",
        "E:/d_down",
        "E:/down",
    ]
    dirs: list[Path] = []
    seen: set[str] = set()
    for raw in raw_dirs:
        if not raw:
            continue
        path = Path(str(raw))
        key = str(path).lower()
        if key not in seen:
            seen.add(key)
            dirs.append(path)
    return dirs


def _discover_manual_card_files(conf: dict) -> list[Path]:
    explicit = str(conf.get("manual_xlsx_path") or "").strip()
    if explicit:
        paths = [Path(p.strip()) for p in explicit.split(";") if p.strip()]
        return [p for p in paths if p.exists() and p.suffix.lower() == ".xlsx"]

    files: list[Path] = []
    seen: set[str] = set()
    for directory in _manual_watch_dirs(conf):
        if not directory.exists():
            continue
        for pattern in OKPOS_CARD_MANUAL_PATTERNS:
            for path in sorted(directory.glob(pattern)):
                key = str(path.resolve()).lower()
                if path.is_file() and key not in seen:
                    seen.add(key)
                    files.append(path)
    return files


def ingest_manual_okpos_card_files(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    files = _discover_manual_card_files(conf)
    if not files:
        msg = "manual OKPOS card files 없음, 스킵"
        logger.info(msg)
        return msg

    total_rows = 0
    saved_paths: list[str] = []
    for src in files:
        normalized = _parse_okpos_card_workbook(src, sale_date="")
        if normalized.empty:
            logger.warning("manual OKPOS card file empty after parsing: %s", src)
            continue
        saved = _upsert_okpos_card_csv(normalized)
        total_rows += len(normalized)
        saved_paths.extend(str(p) for p in saved)
        logger.info("manual OKPOS card file ingested: %s | rows=%d", src, len(normalized))

    context["ti"].xcom_push(key="manual_card_files", value=[str(p) for p in files])
    context["ti"].xcom_push(key="manual_card_saved_paths", value=sorted(set(saved_paths)))
    return f"manual OKPOS card 적재 완료: files={len(files)}, rows={total_rows}"


def validate_okpos_card_lookback(lookback_days: int = 7, min_date: str = OKPOS_CARD_MIN_VALIDATE_DATE, **context) -> str:
    # Use the sale_date actually collected (from XCom) as the anchor so validate never checks
    # dates beyond what the current run downloaded, regardless of when the task executes.
    ti = context.get("ti")
    collected = ti.xcom_pull(task_ids="download_okpos_card_test", key="sale_date") if ti else None
    if collected:
        today = datetime.strptime(collected, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        today = _kst_now().date()
    min_dt = datetime.strptime(min_date, "%Y-%m-%d").date()
    target_dates = [
        (today - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(1, lookback_days + 1)
        if (today - timedelta(days=i)) >= min_dt
    ]
    if not target_dates:
        return f"검증 대상 없음: lookback_days={lookback_days}, min_date={min_date}"

    history = _load_store_history()
    if history.empty:
        raise ValueError(f"OKPOS card 검증 실패: 저장된 csv 없음, target_dates={target_dates}")
    history = _canonicalize_card_df(history)
    if "승인구분" not in history.columns:
        raise ValueError("OKPOS card 검증 실패: 승인구분 컬럼 없음")

    approved = history[history["승인구분"].astype(str).str.strip() == "승인"].copy()
    counts = approved.groupby("sale_date").size().to_dict() if "sale_date" in approved.columns else {}
    missing = [d for d in target_dates if int(counts.get(d, 0)) <= 0]
    context["ti"].xcom_push(key="okpos_card_lookback_counts", value={d: int(counts.get(d, 0)) for d in target_dates})
    if missing:
        context["ti"].xcom_push(key="okpos_card_missing_dates", value=missing)
        raise ValueError(f"OKPOS card lookback 검증 실패: 승인 영수증 누락일={missing}")

    msg = f"OKPOS card lookback 검증 완료: dates={len(target_dates)}, lookback_days={lookback_days}"
    logger.info("%s | counts=%s", msg, {d: int(counts.get(d, 0)) for d in target_dates})
    return msg


def _download_one_date(driver, wait: WebDriverWait, download_dir: Path, sale_date: str) -> Path | None:
    driver.get(OKPOS_CARD_APPROVAL_URL)
    time.sleep(2)
    _dismiss_alert(driver)

    _set_date_hyphen(driver, wait, sale_date, "startDate")
    _dismiss_alert(driver)
    _set_date_hyphen(driver, wait, sale_date, "endDate")
    _dismiss_alert(driver)

    _click_search_btn(driver, wait)
    time.sleep(3)
    _dismiss_alert(driver)
    try:
        _wait_for_store_in_results(
            driver,
            wait,
            OKPOS_CARD_STORE["name"],
            OKPOS_CARD_STORE["shopCd"],
            download_dir,
        )
    except TimeoutException:
        logger.warning("OKPOS card: 매장 행 없음(승인 0건 추정) sale_date=%s -> skip", sale_date)
        return None

    _click_store_row_in_results(driver, wait, OKPOS_CARD_STORE["shopCd"], OKPOS_CARD_STORE["name"], download_dir)
    _wait_for_popup(driver, wait)

    existing_files = {path for path in download_dir.iterdir() if path.is_file()}
    _click_excel_button(driver, wait)
    downloaded = _wait_for_download(
        download_dir,
        existing_files,
        timeout=DOWNLOAD_TIMEOUT,
        expected_suffixes={".xlsx"},
        filename_predicate=_filename_matches,
    )
    if downloaded is None:
        candidates = sorted(
            [p for p in download_dir.iterdir() if p.is_file() and _filename_matches(p)],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        downloaded = candidates[0] if candidates else None
    return downloaded


def _missing_lookback_dates(
    primary: str,
    lookback_days: int = 7,
    min_date: str = OKPOS_CARD_MIN_VALIDATE_DATE,
) -> list[str]:
    anchor = datetime.strptime(primary, "%Y-%m-%d").date() + timedelta(days=1)
    min_dt = datetime.strptime(min_date, "%Y-%m-%d").date()
    targets = [
        (anchor - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(1, lookback_days + 1)
        if (anchor - timedelta(days=i)) >= min_dt
    ]

    history = _load_store_history()
    present: set[str] = set()
    if not history.empty:
        history = _canonicalize_card_df(history)
        if "승인구분" in history.columns and "sale_date" in history.columns:
            approved = history[history["승인구분"].astype(str).str.strip() == "승인"]
            present = set(approved["sale_date"].astype(str).str.strip())

    return [date for date in targets if date not in present and date != primary]


def download_okpos_card_test(**context) -> str:
    sale_date = _resolve_sale_date(**context)
    download_dir = OKPOS_CARD_DOWNLOAD_DIR
    _cleanup_download_dir(download_dir)

    driver = _launch_browser(download_dir=download_dir)
    backfilled: list[str] = []
    empty_days: list[str] = []
    try:
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        _setup_download_dir(driver, download_dir)
        _login(driver, wait)

        downloaded = _download_one_date(driver, wait, download_dir, sale_date)
        if downloaded is None:
            raise TimeoutException(
                f"OKPOS card primary download failed: sale_date={sale_date}, "
                f"download_dir={download_dir}"
            )
        primary_path = download_dir / f"primary_{sale_date}_{downloaded.name}"
        if primary_path != downloaded:
            primary_path.unlink(missing_ok=True)
            downloaded = downloaded.replace(primary_path)

        context["ti"].xcom_push(key="downloaded_path", value=str(downloaded))
        logger.info("OKPOS card approval primary downloaded: %s", downloaded)

        for missing_date in _missing_lookback_dates(sale_date):
            _cleanup_download_dir(download_dir)
            backfill_file = _download_one_date(driver, wait, download_dir, missing_date)
            if backfill_file is None:
                empty_days.append(missing_date)
                continue

            normalized = _parse_okpos_card_workbook(backfill_file, missing_date)
            _upsert_okpos_card_csv(normalized)
            backfill_file.unlink(missing_ok=True)
            backfilled.append(missing_date)
            logger.info("OKPOS card 자동 백필 완료: %s | rows=%d", missing_date, len(normalized))

        context["ti"].xcom_push(key="backfilled_dates", value=backfilled)
        context["ti"].xcom_push(key="empty_dates", value=empty_days)
        logger.info("OKPOS card download 완료: primary=%s, backfilled=%s, empty=%s", sale_date, backfilled, empty_days)
        return f"downloaded primary={sale_date}, backfilled={backfilled}, empty={empty_days}"
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def save_okpos_card_test_csv(**context) -> str:
    downloaded_path = context["ti"].xcom_pull(task_ids="download_okpos_card_test", key="downloaded_path")
    sale_date = context["ti"].xcom_pull(task_ids="download_okpos_card_test", key="sale_date")
    if not downloaded_path:
        raise ValueError("download_okpos_card_test XCom(downloaded_path) is empty.")
    if not sale_date:
        raise ValueError("download_okpos_card_test XCom(sale_date) is empty.")

    src = Path(str(downloaded_path))
    if not src.exists():
        raise FileNotFoundError(f"Downloaded card approval file not found: {src}")

    normalized = _parse_okpos_card_workbook(src, sale_date)
    saved = _upsert_okpos_card_csv(normalized)
    src.unlink(missing_ok=True)

    saved_paths = [str(path) for path in saved]
    context["ti"].xcom_push(key="saved_csv_path", value=saved_paths[0] if saved_paths else "")
    context["ti"].xcom_push(key="saved_csv_paths", value=saved_paths)
    logger.info("OKPOS card approval csv saved: %s | rows=%d", saved_paths, len(normalized))
    return f"saved: {saved_paths}"
