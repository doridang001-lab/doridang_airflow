"""송파삼전점 메뉴 계층 시험 주문서 생성 파이프라인.

원본 수집 데이터의 부모/옵션 신호를 보존해 unified_sales와 별도인 CSV 산출물을 만든다.
"""

from __future__ import annotations

import ast
import hashlib
import json
import logging
import re
import shutil
from functools import lru_cache
from pathlib import Path
from zipfile import BadZipFile

import numpy as np
import pandas as pd
import pendulum

from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    DELIVERY_PLATFORM_FAMILIES,
    UNIFIED_COLUMNS,
    _load_store_map,
    _lookup_store_meta,
    _make_unified_pk,
    _normalize_time,
    _to_int_series,
    iter_unified_sales_files,
)
from modules.transform.utility.paths import (
    ANALYTICS_DB,
    BAEMIN_ORDERS_DB,
    COUPANG_ORDERS_DB,
    DELIVERY_COMMISSION_DIR,
    FIN_PRODUCT_CSV_PATH,
    FIN_PRODUCT_MAP_CSV_PATH,
    FIN_PRODUCT_MAP_JOIN_CSV_PATH,
    NEW_FIN_PRODUCT_MAP_REVIEW_CSV_PATH,
    existing_new_fin_product_map_review_csv_path,
    existing_fin_product_map_review_csv_path,
    LOCAL_DB,
    MART_DB,
    RAW_OKPOS_SALES,
)
from modules.transform.utility.notifier import send_telegram_chunks
from modules.transform.utility.store_normalize import normalize as _normalize_store_names
from modules.transform.utility.store_normalize import strip_brand as _strip_brand
logger = logging.getLogger(__name__)

TARGET_STORE = "송파삼전점"
TARGET_STORE_TOKEN = "송파삼전점"
ACTIVE_TARGET_YMS = ("2026-07", "2026-08")
NEW_CLS_DIR = MART_DB / "new_classification_orders"
MANUAL_WORKBOOK_OUTPUT_PATH = NEW_CLS_DIR / "01_수기입력.xlsx"
MANUAL_WORKBOOK_BACKUP_ROOT = LOCAL_DB / "temp" / "backups" / "menu_hierarchy_manual_input"
FINAL_ORDERS_OUTPUT_PATH = NEW_CLS_DIR / "02_최종주문.csv"
SUMMARY_WORKBOOK_OUTPUT_PATH = NEW_CLS_DIR / "03_요약.xlsx"
USAGE_OUTPUT_PATH = NEW_CLS_DIR / "04_사용법.md"
SYSTEM_OUTPUT_DIR = NEW_CLS_DIR / "_system"
DEBUG_OUTPUT_DIR = NEW_CLS_DIR / "_debug" / "latest"
ARCHIVE_OUTPUT_DIR = NEW_CLS_DIR / "_archive"
README_OUTPUT_PATH = NEW_CLS_DIR / "설명.md"
INPUT_GUIDE_OUTPUT_PATH = NEW_CLS_DIR / "입력가이드.md"
MANAGER_INPUT_OUTPUT_PATH = NEW_CLS_DIR / "13_manager_input.csv"
MANAGER_LLM_PAYLOAD_OUTPUT_PATH = NEW_CLS_DIR / "14_manager_input_llm_payload.jsonl"
MANAGER_LLM_RESULT_OUTPUT_PATH = NEW_CLS_DIR / "15_manager_input_llm_result.jsonl"
OPTION_MATERIAL_INPUT_OUTPUT_PATH = NEW_CLS_DIR / "16_option_material_input.csv"
MENU_WEIGHT_INPUT_OUTPUT_PATH = NEW_CLS_DIR / "17_menu_weight_input.csv"
MATERIAL_USAGE_SUMMARY_OUTPUT_PATH = NEW_CLS_DIR / "18_material_usage_summary.csv"
VALIDATION_ISSUES_OUTPUT_PATH = NEW_CLS_DIR / "19_validation_issues.csv"
CLASSIFICATION_AUDIT_OUTPUT_PATH = NEW_CLS_DIR / "20_classification_audit.csv"
OPTION_KIND_MASTER_OUTPUT_PATH = NEW_CLS_DIR / "21_option_kind_master.csv"
MENU_WEIGHT_MASTER_OUTPUT_PATH = NEW_CLS_DIR / "22_menu_weight_master.csv"
MATERIAL_PRICE_MASTER_OUTPUT_PATH = NEW_CLS_DIR / "23_material_price_master.csv"
MENU_PROFIT_SUMMARY_OUTPUT_PATH = NEW_CLS_DIR / "24_menu_profit_summary.csv"
COMPLETENESS_OUTPUT_PATH = NEW_CLS_DIR / "25_completeness.csv"
LEGACY_COMPLETENESS_BASELINE_PATH = NEW_CLS_DIR / "25_completeness_baseline.json"
COMPLETENESS_BASELINE_PATH = SYSTEM_OUTPUT_DIR / "25_completeness_baseline.json"
CLASSIFICATION_PATTERN_ALERT_STATE_PATH = SYSTEM_OUTPUT_DIR / "classification_pattern_alerts.json"
CHICKEN_RATIO_MASTER_OUTPUT_PATH = NEW_CLS_DIR / "26_chicken_ratio_master.csv"
MANUAL_PROFIT_RATE_MASTER_OUTPUT_PATH = NEW_CLS_DIR / "27_profit_rate_master.csv"
MANUAL_PROFIT_SUMMARY_OUTPUT_PATH = NEW_CLS_DIR / "28_manual_profit_summary.csv"
MENU_CHICKEN_PROFILE_OUTPUT_PATH = NEW_CLS_DIR / "29_menu_chicken_profile.csv"
MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME = "수익률_미매칭보존"

HIERARCHY_COLUMNS = ["menu_seq", "line_role", "parent_item_seq", "std_menu_name", "attr_method"]
OUTPUT_COLUMNS = list(UNIFIED_COLUMNS) + HIERARCHY_COLUMNS
ORDER_OUTPUT_COLUMNS = list(UNIFIED_COLUMNS)
OPTION_COMBO_COLUMN = "옵션조합"
COST_COMBO_COLUMN = "원가조합"
# 13번 입력표의 키. 옵션조합과 달리 닭 사용량을 결정하는 옵션만 담는다.
CHICKEN_OPTION_KEY_COLUMN = "닭옵션키"
OPTION_COMBO_NONE = "옵션없음"
MANAGER_DEFAULT_OPTION_COMBO = "(메뉴기본값)"
MENU_WEIGHT_UNKNOWN = "미확인"
MENU_WEIGHT_EXTRA_NONE = "추가없음"
MATERIAL_USAGE_COLUMN = "재료사용량"
MENU_WEIGHT_USAGE_COLUMN = "표준중량사용량"
# 22번에서 값이 비어 있는 재료 목록. "이 메뉴는 우거지를 안 쓴다"와 "아무도 우거지를
# 입력하지 않았다"를 구분할 수단이 없으면, 닭값만 빼고 계산한 공헌이익이 산출된 것처럼
# 나온다. 빈칸은 미입력으로 보고, 안 쓰는 재료는 담당자가 0을 명시해야 한다.
MENU_WEIGHT_MISSING_COLUMN = "표준중량미입력재료"
OPTION_MATERIAL_USAGE_COLUMN = "옵션재료사용량"
# 사용용량은 메뉴 1개 기준값, 사용용량_합계는 qty를 반영한 라인 총량이다.
# 월별 소진량 집계는 반드시 사용용량_합계를 써야 qty>=2 주문이 누락되지 않는다.
CHICKEN_USAGE_COLUMN = "사용용량"
CHICKEN_USAGE_TOTAL_COLUMN = "사용용량_합계"
BONE_USAGE_COLUMN = "뼈사용용량"
BONELESS_USAGE_COLUMN = "순살사용용량"
BONE_USAGE_TOTAL_COLUMN = "뼈사용용량_합계"
BONELESS_USAGE_TOTAL_COLUMN = "순살사용용량_합계"
HALF_COMBO_COLUMN = "반반조합"
HALF_SLOT1_COLUMN = "반반슬롯1"
HALF_SLOT2_COLUMN = "반반슬롯2"
HALF_SLOT_COLUMNS = [HALF_SLOT1_COLUMN, HALF_SLOT2_COLUMN]
HALF_SLOT_SHARE = 0.5
HALF_BONE_RATIO_COLUMN = "뼈비율"
CHICKEN_SPLIT_COLUMNS = [
    BONE_USAGE_COLUMN,
    BONELESS_USAGE_COLUMN,
    BONE_USAGE_TOTAL_COLUMN,
    BONELESS_USAGE_TOTAL_COLUMN,
    HALF_COMBO_COLUMN,
    HALF_SLOT1_COLUMN,
    HALF_SLOT2_COLUMN,
]
CHICKEN_FINAL_COLUMNS = ["닭유형", "사이즈", CHICKEN_USAGE_COLUMN, CHICKEN_USAGE_TOTAL_COLUMN, *CHICKEN_SPLIT_COLUMNS, "수익률"]
# 주문 원천에 뼈/순살 신호가 실제로 있었는지. 최종 닭유형_판정은 수기가 덮어쓰기 때문에
# 이 컬럼이 없으면 "담당자가 한 값으로 뭉갠 구간"을 사후에 찾아낼 방법이 없다.
CHICKEN_SIGNAL_COLUMN = "닭유형_신호"
CHICKEN_SIGNAL_PRESENT = "있음"
CHICKEN_SIGNAL_ABSENT = "없음"
CHICKEN_METHOD_HALF = "반반옵션"
CHICKEN_METHOD_HALF_SLOT = "반반슬롯"
CHICKEN_METHOD_ORDER_SEQUENCE = "주문순서매칭"
CHICKEN_METHOD_NEAR_PRICE_MATCH = "주변단가매칭"
# _infer_chicken_attrs_from_group이 실제 근거를 찾았을 때만 쓰는 판정명.
# 토큰기본(닭 메뉴니까 일단 뼈닭)과 미해결은 근거가 아니다.
CHICKEN_SIGNAL_METHODS = frozenset({"변경", "선택", "메뉴명", CHICKEN_METHOD_HALF, CHICKEN_METHOD_HALF_SLOT, CHICKEN_METHOD_ORDER_SEQUENCE})
CHICKEN_METHOD_PRICE_MATCH = "단가매칭"
PRICE_MATCH_NEAR_MAX_DIFF = 1000
PRICE_MATCH_NEAR_MIN_ORDERS = 3
PRICE_MATCH_NEAR_MIN_SHARE = 0.9
PRICE_MATCH_EVIDENCE_METHODS = frozenset({
    "변경",
    "선택",
    "메뉴명",
    "메뉴프로필",
    "판정옵션",
    "수기",
    "기존수기",
    CHICKEN_METHOD_HALF,
    CHICKEN_METHOD_HALF_SLOT,
    CHICKEN_METHOD_ORDER_SEQUENCE,
})
DEFAULT_CHICKEN_ADDON_GRAMS_PER_BIRD = 300.0
CHICKEN_ADDON_EXPECTED_UNIT_PRICES = {
    150.0: 4000.0,
    300.0: 7500.0,
}
CHICKEN_RATIO_APPLIED_COLUMN = "뼈비율_적용"
CHICKEN_CONFIDENCE_COLUMN = "판정상태"
CLASSIFICATION_RULE_COLUMN = "분류룰"
JUDGEMENT_OPTION_SHEET_NAME = "판정옵션"
CHICKEN_CONVERSION_SHEET_NAME = "닭환산"
ORDER_EXCEPTION_SHEET_NAME = "예외주문"
JUDGEMENT_OPTION_COLUMNS = [
    "source",
    "brand",
    "store",
    "std_menu_name",
    "조건",
    CHICKEN_OPTION_KEY_COLUMN,
    "닭유형",
    "사이즈",
    CHICKEN_USAGE_COLUMN,
    HALF_COMBO_COLUMN,
    HALF_BONE_RATIO_COLUMN,
    HALF_SLOT1_COLUMN,
    HALF_SLOT2_COLUMN,
    "메모",
]
CHICKEN_ADDON_USAGE_COLUMN = "가산닭사용량"
CHICKEN_ADDON_BONE_COLUMN = "가산뼈사용용량"
CHICKEN_ADDON_BONELESS_COLUMN = "가산순살사용용량"
CHICKEN_ADDON_REASON_COLUMN = "닭가산사유"
ORDER_EXCEPTION_TYPE_COLUMN = "주문예외구분"
ADJUSTED_SALES_COLUMN = "매출_보정"
PRE_DISCOUNT_PRICE_COLUMN = "할인전정가"
NORMAL_PRICE_COLUMN = "정상가"
GROSS_SALES_TOTAL_COLUMN = "총매출합계"
PROFIT_SALES_COLUMN = "수익매출"
MANUAL_PROFIT_QTY_COLUMN = "수익계상수량"
MANUAL_PROFIT_COST_COLUMN = "수익계상원가"
MANUAL_PROFIT_SALES_BASIS_COLUMN = "수익매출기준"
MANUAL_PROFIT_SALES_BASIS_ACTUAL = "실매출"
MANUAL_PROFIT_SALES_BASIS_MANUAL = "수기판매가"
# 원가율 전용 기준. 담당자가 적은 판매가(없으면 자동 판매가) x 수량이라 정가 기준이다.
# 할인.번들이 섞인 실매출(수익매출)과는 다른 지표다.
COST_BASE_SALES_COLUMN = "원가_기준매출"
COST_BASE_PROFIT_COLUMN = "원가_기준수익"
CANCEL_OFFSET_GROUP_COLUMN = "취소상계그룹"
CANCEL_OFFSET_STATUS_COLUMN = "취소상계상태"
CANCEL_OFFSET_CANDIDATES_COLUMN = "취소상계후보수"
CANCEL_OFFSET_NORMAL_STATUS = "상계정상"
CANCEL_OFFSET_CANCEL_STATUS = "상계취소"
CANCEL_OFFSET_UNMATCHED_STATUS = "매칭없음"
CANCEL_OFFSET_NONE_STATUS = "해당없음"
ORDER_EXCEPTION_COLUMNS = [
    "source",
    "order_id",
    "sale_date",
    "menu_name",
    "매출",
    "할인액",
    "닭사용량",
    "자동판정",
    "구분_manual",
    "닭계상_manual",
    "메모",
]
SALESLESS_COUNTED_EXCEPTION_TYPES = {"전액할인", "직원식사", "이벤트", "서비스", "할인"}
CHICKEN_TRACE_COLUMNS = [
    "닭유형_판정",
    CHICKEN_SIGNAL_COLUMN,
    CHICKEN_RATIO_APPLIED_COLUMN,
    "사이즈_판정",
    "미해결사유",
    CLASSIFICATION_RULE_COLUMN,
    "추정수익",
]
PROFIT_COLUMNS = [
    "수익채널",
    "재료원가",
    "옵션재료원가",
    "수수료율",
    "수수료",
    "수수료율_출처",
    "공헌이익",
    "공헌이익률",
    "원가미산출사유",
]
MANUAL_PROFIT_COLUMNS = [
    "수익키",
    "수익품목명",
    MANUAL_PROFIT_QTY_COLUMN,
    MANUAL_PROFIT_COST_COLUMN,
    MANUAL_PROFIT_SALES_BASIS_COLUMN,
    "수기수익",
    COST_BASE_SALES_COLUMN,
    COST_BASE_PROFIT_COLUMN,
    "수익미산출사유",
]
LEFT_JOINED_OUTPUT_COLUMNS = (
    list(UNIFIED_COLUMNS)
    + HIERARCHY_COLUMNS
    + ["option_kind", "재료명", OPTION_COMBO_COLUMN, CHICKEN_OPTION_KEY_COLUMN]
    + CHICKEN_FINAL_COLUMNS
    + [
        MATERIAL_USAGE_COLUMN,
        MENU_WEIGHT_USAGE_COLUMN,
        MENU_WEIGHT_MISSING_COLUMN,
        OPTION_MATERIAL_USAGE_COLUMN,
        CHICKEN_ADDON_USAGE_COLUMN,
        CHICKEN_ADDON_BONE_COLUMN,
        CHICKEN_ADDON_BONELESS_COLUMN,
        CHICKEN_ADDON_REASON_COLUMN,
        ORDER_EXCEPTION_TYPE_COLUMN,
        ADJUSTED_SALES_COLUMN,
        PRE_DISCOUNT_PRICE_COLUMN,
        NORMAL_PRICE_COLUMN,
        PROFIT_SALES_COLUMN,
        CANCEL_OFFSET_GROUP_COLUMN,
        CANCEL_OFFSET_STATUS_COLUMN,
        CANCEL_OFFSET_CANDIDATES_COLUMN,
    ]
    + PROFIT_COLUMNS
    + MANUAL_PROFIT_COLUMNS
    + CHICKEN_TRACE_COLUMNS
)
ORDER_GROUP_COLUMNS = ["source", "brand", "store", "sale_date", "order_id", "menu_seq"]
MANAGER_INPUT_KEY_COLUMNS = ["source", "brand", "store", "std_menu_name", CHICKEN_OPTION_KEY_COLUMN]
OPTION_MATERIAL_INPUT_KEY_COLUMNS = ["source", "brand", "store", "item_id", "item_name"]
MENU_WEIGHT_INPUT_KEY_COLUMNS = ["source", "brand", "store", "std_menu_name", "사이즈키", "닭유형키", "추가재료키"]


def _ensure_order_group_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for col in ORDER_GROUP_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out


MANAGER_INPUT_COLUMNS = [
    "source",
    "brand",
    "store",
    "std_menu_name",
    "대표메뉴명",
    CHICKEN_OPTION_KEY_COLUMN,
    "주문건수",
    "판매수량",
    "매출합계",
    "닭유형_manual",
    "사이즈_manual",
    "닭사용량_manual",
    "수익률_manual",
    "메모",
]
OPTION_MATERIAL_INPUT_COLUMNS = [
    "source",
    "brand",
    "store",
    "item_id",
    "item_name",
    "std_menu_name",
    "대표메뉴명",
    "line_role",
    "주문건수",
    "판매수량",
    "매출합계",
    "메모",
]
MENU_WEIGHT_INPUT_COLUMNS = [
    "source",
    "brand",
    "store",
    "std_menu_name",
    "사이즈키",
    "닭유형키",
    "추가재료키",
    "주문건수",
    "판매수량",
    "매출합계",
    "닭사용량_manual",
    "우거지사용량_manual",
    "순살추가사용량_manual",
    "메모",
]
MATERIAL_USAGE_SUMMARY_COLUMNS = [
    "source",
    "brand",
    "store",
    "std_menu_name",
    "사이즈키",
    "닭유형키",
    "추가재료키",
    "재료명",
    "주문건수",
    "판매수량",
    "메뉴당사용량",
    "예상사용량",
    "매출합계",
]
OPTION_KIND_MASTER_KEY_COLUMNS = ["source", "brand", "store", "item_id", "item_name"]
OPTION_KIND_MASTER_EDIT_COLUMNS = ["option_kind_확정", "재료명_확정", "닭가산_manual", "닭가산유형_manual", "메모"]
OPTION_KIND_MASTER_COLUMNS = [
    *OPTION_KIND_MASTER_KEY_COLUMNS,
    "line_role",
    "std_menu_name",
    "주문건수",
    "판매수량",
    "매출합계",
    "option_kind_제안",
    "option_kind_확정",
    "재료명_제안",
    "재료명_확정",
    "닭가산_manual",
    "닭가산유형_manual",
    "메모",
]
# 표준중량은 레시피 속성이라 판매 채널과 무관하다. source를 키에 넣으면 같은 메뉴를
# okpos/배민/쿠팡별로 4번 입력하게 되고(155행), 실측에서 source별 값 충돌은 0건이었다.
MENU_WEIGHT_MASTER_KEY_COLUMNS = ["brand", "store", "std_menu_name", "사이즈", "닭유형"]
MENU_WEIGHT_MASTER_BASE_COLUMNS = [
    *MENU_WEIGHT_MASTER_KEY_COLUMNS,
    "주문건수",
    "판매수량",
    "매출합계",
]
# 수익 요약은 같은 메뉴라도 홀/배달 플랫폼별 수수료가 달라 채널을 반드시 나눈다.
PROFIT_CHANNEL_COLUMN = "수익채널"
NO_COMMISSION_PROFIT_CHANNELS = {"홀", "홀_포장"}
MENU_PROFIT_GROUP_COLUMNS = ["수익채널", "source", "brand", "store", "std_menu_name", "사이즈", "닭유형"]
# 담당자가 컬럼을 직접 만들 필요 없게 자주 쓰는 재료를 미리 깔아둔다.
# 16번에 *_manual 컬럼이 하나도 없던 이유가 "컬럼부터 만들어야 하는" 설계였기 때문이다.
_MENU_WEIGHT_MASTER_DEFAULT_MATERIALS = [
    "닭",
    "우거지",
    "순살추가",
    "묵은지",
    "대창",
    "우삼겹",
    "파김치",
    "미나리",
]
MATERIAL_PRICE_MASTER_COLUMNS = [
    "재료명",
    "단위",
    "단가_manual",
    "기준일_manual",
    "사용판매수량",
    "메모",
]
MENU_PROFIT_SUMMARY_COLUMNS = [
    "수익채널",
    "source",
    "brand",
    "store",
    "std_menu_name",
    "사이즈",
    "닭유형",
    "주문건수",
    "판매수량",
    "매출합계",
    "닭사용량합계",
    "재료원가합계",
    "옵션재료원가합계",
    "수수료합계",
    "공헌이익합계",
    "공헌이익률",
    "공헌이익산출행",
    "대상행",
]
MANUAL_PROFIT_RATE_MASTER_COLUMNS = [
    "수익채널",
    "수익키",
    "대표품목명",
    "사이즈",
    "닭유형",
    "option_kind",
    "source목록",
    "platform목록",
    "order_type목록",
    "대표주문메뉴명",
    "대표품목원문",
    "대표옵션조합",
    COST_COMBO_COLUMN,
    "계산닭유형",
    "계산사이즈",
    CHICKEN_SIGNAL_COLUMN,
    "원본품목명목록",
    "주문건수",
    "판매수량",
    "매출합계",
    GROSS_SALES_TOTAL_COLUMN,
    "판매가",
    "판매가_manual",
    "판매가기준",
    "메뉴원가_manual",
    "상차림비_manual",
    "상차림포함원가",
    "메모",
]
MANUAL_PROFIT_RATE_AMOUNT_COLUMNS = ["판매가_manual", "메뉴원가_manual", "상차림비_manual"]
SETTING_INCLUDED_COST_COLUMN = "상차림포함원가"
LEGACY_MANUAL_PRICE_COLUMN = "판매가_manual"
LEGACY_MANUAL_PROFIT_DROPPED_COLUMNS = {"상차림포함원가_manual"}
MANUAL_PROFIT_SUMMARY_COLUMNS = [
    "수익채널",
    "수익키",
    "대표품목명",
    "option_kind",
    "주문건수",
    "판매수량",
    "매출합계",
    "실매출합계",
    "수기판매가기준행",
    "수익률",
    "수익합계",
    "수익산출행",
    "대상행",
]
CHICKEN_DECISION_OPTION_REVIEW_COLUMNS = [
    "검토상태",
    "수익채널",
    "source목록",
    "수익키",
    "std_menu_name",
    "대표주문메뉴명",
    CHICKEN_OPTION_KEY_COLUMN,
    OPTION_COMBO_COLUMN,
    "닭유형",
    "사이즈",
    "닭유형_판정",
    "사이즈_판정",
    CHICKEN_SIGNAL_COLUMN,
    "주문건수",
    "판매수량",
    "매출합계",
    "입력위치",
    "메모",
]
STD_MENU_OVERRIDE_SHEET_NAME = "메뉴명보정"
STD_MENU_OVERRIDE_KEY_COLUMNS = ["source", "brand", "store", "line_role", "item_id", "item_name", "현재_std_menu_name"]
STD_MENU_OVERRIDE_EDIT_COLUMNS = ["std_menu_name_manual", "메모"]
STD_MENU_OVERRIDE_COLUMNS = [
    *STD_MENU_OVERRIDE_KEY_COLUMNS,
    "대표주문메뉴명",
    "주문건수",
    "판매수량",
    "매출합계",
    *STD_MENU_OVERRIDE_EDIT_COLUMNS,
]
# 뼈:순살 비율표. 신호가 있는 주문에서 실측한 비율을 신호 없는 구간에 적용해
# "전부 뼈닭" 가정의 오차를 줄인다. source를 키에 넣는 이유는 채널마다 뼈/순살
# 선호가 다르고(쿠팡은 옵션 명시, okpos는 신호 자체가 드물다) 같은 채널 실측이
# 가장 가까운 근사이기 때문이다.
CHICKEN_RATIO_MASTER_KEY_COLUMNS = ["source", "brand", "store", "std_menu_name", "사이즈"]
CHICKEN_RATIO_MASTER_COLUMNS = [
    *CHICKEN_RATIO_MASTER_KEY_COLUMNS,
    "신호_판매수량",
    "뼈_판매수량",
    "순살_판매수량",
    "뼈비율_실측",
    "무신호_판매수량",
    "무신호_매출",
    "뼈비율_manual",
    "적용비율",
    "비율출처",
    "메모",
]
# 표본이 이보다 적으면 실측 비율을 쓰지 않는다. 3~4건짜리 비율은 전부 뼈닭
# 가정보다 나을 근거가 없고, 오차를 줄이는 대신 흔들기만 한다.
CHICKEN_RATIO_MIN_SAMPLE = 20.0
CHICKEN_RATIO_SOURCE_MANUAL = "수기"
CHICKEN_RATIO_SOURCE_MENU_SIZE = "실측_동일채널_메뉴사이즈"
CHICKEN_RATIO_SOURCE_MENU = "실측_동일채널_메뉴"
CHICKEN_RATIO_SOURCE_POOLED = "실측_전채널_메뉴"
CHICKEN_RATIO_SOURCE_HALF_RULE = "반반규칙"
CHICKEN_RATIO_SOURCE_UNANIMOUS = "실측_만장일치_저표본"
MENU_CHICKEN_PROFILE_SHEET_NAME = "메뉴닭프로필"
MENU_CHICKEN_PROFILE_KEY_COLUMNS = ["source", "brand", "store", "std_menu_name"]
MENU_CHICKEN_PROFILE_EDIT_COLUMNS = ["허용닭유형", "기본닭유형", "허용사이즈", "기본사이즈", "옵션닭유형적용", "메모"]
MENU_CHICKEN_PROFILE_COLUMNS = [
    *MENU_CHICKEN_PROFILE_KEY_COLUMNS,
    "대표메뉴명",
    "주문건수",
    "판매수량",
    "매출합계",
    "허용닭유형_제안",
    "기본닭유형_제안",
    "허용사이즈_제안",
    "기본사이즈_제안",
    "옵션닭유형적용_제안",
    *MENU_CHICKEN_PROFILE_EDIT_COLUMNS,
]
CHICKEN_RATIO_SOURCE_NONE = "미적용_표본부족"
COMPLETENESS_COLUMNS = [
    "차원",
    "분모",
    "분자",
    "완결률",
    "기준선",
    "상태",
    "미완요약",
]
COMPLETENESS_BLOCKING_DIMENSIONS = {
    "option_kind",
    "chicken_attr",
    "chicken_usage",
    "commission",
    "std_menu_alias",
}
COMPLETENESS_INPUT_WAIT_DIMENSIONS = {
    "profit",
    "manual_profit",
    "menu_weight",
    "material_price",
    "chicken_addon",
    "chicken_ratio",
    "order_exception",
}
HIERARCHY_OUTPUT_COLUMNS = [
    "_pk",
    "sale_date",
    "ym",
    "source",
    "brand",
    "store",
    "platform",
    "order_id",
    "item_seq",
    "item_id",
    "item_name",
    *HIERARCHY_COLUMNS,
]
RAW_LINE_COLUMNS = [
    "ym",
    "sale_date",
    "source",
    "brand",
    "store",
    "platform",
    "order_id",
    "item_seq",
    "raw_item_name",
    "수량",
    "단가",
    "is_child",
    "boundary",
]
GAP_COLUMNS = ["item_id", "source", "item_name", "주문건수", "현재상태", "추정_수동분류"]
VALIDATION_ISSUE_COLUMNS = [
    "issue_type",
    "severity",
    "source",
    "sale_date",
    "ym",
    "order_id",
    "item_seq",
    "item_id",
    "item_name",
    "line_role",
    "std_menu_name",
    "detail",
    "rows",
    "orders",
    "sales",
]
CLASSIFICATION_AUDIT_COLUMNS = [
    "audit_type",
    "severity",
    "source",
    "item_id",
    "item_name",
    "std_menu_name",
    "line_role",
    "rows",
    "orders",
    "sales",
    "detail",
]
CHICKEN_MENU_GROUP_COLUMNS = [
    "sale_date",
    "ym",
    "source",
    "brand",
    "store",
    "platform",
    "order_id",
    "menu_seq",
    "parent_item_seq",
    "parent_item_id",
    "parent_item_name",
    "menu_name",
    "std_menu_name",
    "menu_qty",
    "chicken_type",
    "chicken_size",
    "usage_per_menu",
    "chicken_usage_total",
    "status",
    "reason",
    "inferred_types",
    "inferred_sizes",
    "line_count",
]

BAEMIN_SOURCE = "배민수동"
COUPANG_SOURCE = "쿠팡수동"
POSFEED_SOURCE = "posfeed"
OKPOS_SOURCE = "okpos"
BAEMIN_PLATFORM = "배달의민족"
COUPANG_PLATFORM = "쿠팡이츠"
_DELIVERY_MANUAL_SOURCES = {BAEMIN_SOURCE, COUPANG_SOURCE, POSFEED_SOURCE}
_COMPLETE_POSFEED_STATUSES = {"배달완료", "접수", "완료"}
_PLATFORM_MAP = {
    "배민1": BAEMIN_PLATFORM,
    "배달의민족": BAEMIN_PLATFORM,
    "쿠팡이츠": COUPANG_PLATFORM,
    "땡겨요": "땡겨요",
    "요기요": "요기요",
    "배달특급": "배달특급",
    "네이버": "네이버주문",
    "기타": "기타",
}

_FEE_RE = re.compile(r"배달비|배달팁|포장비|수수료")
_DISCOUNT_RE = re.compile(r"할인|쿠폰")
_OPTION_LIKE_RE = re.compile(
    r"추가|옵션|토핑|사리|변경|선택|곱빼기|맵기|기본맛|^\s*기본\s*$|"
    r"\[후\.참\]|\[(?:소|중|대|특)\]|한마리|반마리|^\s*뼈\s*$|순살.*변경|"
    r"순한맛|중간\s*매운맛|중간맛|매운맛|아주\s*매운맛|아주매운맛|보통맛|맛\s*선택|치킨무|파김치|국물적게"
)
_FEE_LIKE_RE = re.compile(r"배달비|배달팁|포장비|쿠폰|할인|서비스|수수료")
_OPTION_MAIN_FORBIDDEN_RE = re.compile(
    r"^\s*(?:"
    r"기본|기본맛|순한맛|중간\s*매운맛|중간맛.*|매운맛.*|아주\s*매운맛.*|아주매운맛.*|보통맛.*|"
    r"맛\s*선택.*|\[(?:소|중|대|특)\].*|뼈|순살|"
    r".*(?:추가|변경|선택|빼주세요|국물적게|주세요).*)\s*$"
)
_SIZE_TAG_RE = re.compile(r"^(?:소|중|대|특|[0-9]+\s*~?\s*[0-9]*\s*인|[0-9]+인분|반마리|한마리|뼈|순살)$")
_LEADING_TAG_RE = re.compile(r"^\s*[\[\(（【]\s*([^\]\)）】]{1,20})\s*[\]\)）】]\s*")
_MAIN_CATEGORIES = {"메인", "1인", "세트"}
_NON_MAIN_CATEGORIES = {"옵션", "토핑", "리뷰", "사이드", "음료", "주류", "기타", "할인", "제외", "수수료"}
_SIDE_LINE_CATEGORIES = {"토핑", "리뷰", "사이드", "음료", "주류"}
_STRICT_NON_MAIN_CATEGORIES = {"옵션", "토핑", "리뷰", "사이드", "음료", "주류", "할인", "제외", "수수료"}
_STANDALONE_CATEGORIES = {"사이드", "음료", "기타", ""}
_CHICKEN_MENU_TOKENS = [
    "닭도리탕",
    "닭도리",
    "도리탕",
    "곱도리탕",
    "우도리탕",
    "닭한마리",
    "삼계탕",
    "백도리탕",
    "백도리당",
    "닭칼국수",
    "닭떡볶이",
    "닭개장",
    "닭곰탕",
    "찜닭",
    "백숙",
    "반반",
]
# 닭을 쓰지 않는 메뉴. 판정 실패(빈값)와 구분해 '닭미사용'으로 명시하기 위한 목록이다.
_NON_CHICKEN_MENU_TOKENS = [
    "막국수",
    "낙곱새",
    "대새",
    "대창",
    "비빔칼국수",
    "우삼겹 전골",
]
CHICKEN_TYPE_NONE = "닭미사용"
CHICKEN_SIZE_NONE = "-"
CHICKEN_METHOD_NONE = "닭미사용"
# 뼈/순살 신호가 없어 실측 비율로 가중한 행. 뼈닭으로 단정하면 5.4%가 틀리고,
# 빈값으로 두면 완결률이 떨어진다. 추정임을 값 자체에 남긴다.
CHICKEN_TYPE_MIXED = "혼합"
CHICKEN_METHOD_RATIO = "비율추정"
_MAIN_CANDIDATE_TOKENS = [
    *_CHICKEN_MENU_TOKENS,
    "정식",
    "전골",
    "막국수 세트",
    "갈비찜닭",
]
_FEE_TOKENS = ["배달비", "배달팁", "포장비", "수수료", "할인", "쿠폰"]
_REVIEW_TOKENS = ["[후.참]", "리뷰"]
_CHICKEN_USAGE = {
    ("뼈닭", "소"): 0.5,
    ("뼈닭", "1인"): 0.5,
    ("뼈닭", "2인"): 1.0,
    ("뼈닭", "중"): 1.0,
    ("뼈닭", "대"): 1.5,
    ("순살", "소"): 0.4,
    ("순살", "중"): 0.8,
    ("순살", "대"): 1.2,
    ("순살", "1인"): 0.3,
    ("순살", "2인"): 0.6,
}
_MANUAL_CHICKEN_COLUMNS = ["닭유형_manual", "사이즈_manual", "닭사용량_manual"]
_MANAGER_INPUT_EDIT_COLUMNS = ["닭유형_manual", "사이즈_manual", "닭사용량_manual", "수익률_manual", "메모"]
NEW_REVIEW_INPUT_KEY_COLUMNS = ["store", "source", "brand", "item_id", "item_name"]
NEW_REVIEW_INPUT_COLUMNS = [
    "item_id",
    "item_key",
    "store",
    "source",
    "brand",
    "item_name",
    "unitprice",
    "표준_메뉴명_edit",
    "수동분류_edit",
    "중복_수동분류",
    "검수유무",
    "검수사유",
    *_MANUAL_CHICKEN_COLUMNS,
    "수익률_manual",
]
_MATERIAL_USAGE_MANUAL_SUFFIX = "사용량_manual"
_CANONICAL_DROP_TAGS = {
    "재주문 1위",
    "들깨",
    "묵은지",
    "밥도둑",
    "매운辛",
    "보양식",
    "삼계탕",
    "우삼겹살",
    "얼큰 국물",
    "한그릇",
    "닭한마리",
    "국물양 조절 불가",
    "복날한정",
}
_CANONICAL_TAG_PREFIX = {"한우 대창": "한우"}
# 같은 메뉴가 프로모션 태그 때문에 갈라진 것들. 태그를 기계적으로 떼면
# "[한우 대창] 순살 곱도리탕" → "순살 곱도리탕"이 되어 "한우 순살 곱도리탕"과
# 오히려 더 어긋나므로, 병합 대상은 전부 여기에 명시한다.
# 미등록 태그는 자동 병합하지 않고 완결률 미완 항목으로 보고한다.
_STD_MENU_ALIAS = {
    "[재주문 1위] 도리당 닭도리탕": "도리당 닭도리탕",
    "[한우 대창] 순살 곱도리탕": "한우 순살 곱도리탕",
    "[1인] 순살 닭도리탕 (밥포함) 1인분": "1인 순살 닭도리탕(밥포함)",
    "[밥도둑] 묵은지 도리탕": "묵은지 닭도리탕",
    "[들깨] 우거지 닭도리탕": "들깨 우거지 닭도리탕",
    "[묵은지] 순살 우도리탕": "묵은지 순살 우도리탕",
    "[단짠단짠] 순살 갈비찜닭": "순살 갈비찜닭",
    "[복날한정] 미나리 수삼 백숙": "미나리 수삼 백숙",
    "[보양식] 미나리 수삼 백숙": "미나리 수삼 백숙",
    "[복날한정] 1인 미나리 수삼 백숙": "1인 미나리 수삼 백숙",
    "[삼계탕] 누룽지 닭한마리": "누룽지 닭한마리",
    "[한그릇] 누룽지 1인 순살 나만의 백도리당": "누룽지 1인 순살 백도리당",
    "[우삼겹살] 우도리탕": "우도리탕",
    "[닭한마리] 우거지 백도리탕": "우거지 백도리탕",
    "[얼큰 국물] 낙곱새 전골": "낙곱새 전골",
    "[매운辛]실비 파김치 곱도리탕 + 계란찜": "실비파김치곱도리탕+계란찜",
    # 상류에서 대괄호가 이미 떨어진 채로 들어오는 잔여 표기.
    "한우 대창 순살 곱도리탕": "한우 순살 곱도리탕",
    "누룽지 1인 순살 나만의 백도리당": "누룽지 1인 순살 백도리당",
    "묵은지 도리탕": "묵은지 닭도리탕",
}
_STD_MENU_ALIAS_LOOKUP = {
    re.sub(r"\s+", "", str(key)): value for key, value in _STD_MENU_ALIAS.items()
}
# 태그처럼 생겼지만 메뉴 정체성의 일부라 떼면 안 되는 것들.
# "[점심] 닭칼국수"는 무태그 "닭칼국수"가 아예 없는 별개 메뉴다.
_STD_MENU_KEPT_TAGS = {"점심"}
_CATEGORY_PRIORITY = {
    "메인": 40,
    "1인": 40,
    "세트": 40,
    "사이드": 30,
    "음료": 30,
    "주류": 30,
    "토핑": 30,
    "리뷰": 30,
    "옵션": 20,
    "기타": 10,
    "": 0,
}
_ROLE_CHANGE_RE = re.compile(r"순살로\s*변경|뼈로\s*변경|100\s*%\s*순살로\s*변경")
_ROLE_ADDON_RE = re.compile(r"추가\s*$|추가\s|\d+\s*g\s*추가|^\s*1\s*인\s*추가")
_SIDE_NAME_RE = re.compile(
    r"콜라|사이다|환타|쿨피스|펩시|진로|새로|카스|테라|켈리|하이볼|막걸리|오미자|"
    r"\d+\s*ml|\d+\s*ML|계란찜|볶음밥|공기밥|주먹밥|새우전|라면사리|리뷰"
)

# option_kind: line_role이 표현하지 못하는 "옵션의 성격"을 담는다.
# line_role은 부모/자식 관계(main/option/side/fee)만 나타내므로 같은 흑미 공기밥이
# 붙으면 option, 단독이면 side가 된다. 닭 사용량을 결정하는 옵션만 골라내려면
# 역할이 아니라 성격이 필요하다.
OPTION_KIND_SIZE = "사이즈"
OPTION_KIND_CHICKEN_TYPE = "닭유형"
OPTION_KIND_CHICKEN_ADDON = "닭추가"
OPTION_KIND_SPICE = "맛선택"
OPTION_KIND_REVIEW = "리뷰서비스"
OPTION_KIND_REQUEST = "요청사항"
OPTION_KIND_MATERIAL = "재료추가"
OPTION_KIND_RICE = "공기밥"
OPTION_KIND_DRINK = "음료주류"
OPTION_KIND_SIDE = "사이드"
OPTION_KIND_SETTLEMENT_EXCLUDE = "정산제외"
OPTION_KIND_MAIN = "메인"
OPTION_KIND_FEE = "배달비수수료"
OPTION_KIND_UNSET = "미확정"

# 닭 사용량을 결정하는 성격. 13번 입력표의 키(닭옵션키)는 이 두 종류만 쓴다.
CHICKEN_DECIDING_KINDS = frozenset({OPTION_KIND_SIZE, OPTION_KIND_CHICKEN_TYPE})
# 매출 라인이 아니라 완결률 분모에서도 빼는 성격.
NON_SALES_KINDS = frozenset({OPTION_KIND_SETTLEMENT_EXCLUDE, OPTION_KIND_FEE})
# 0원이어도 실제 제공되어 수기 원가 계상이 필요한 옵션/품목 성격.
ZERO_PRICE_COST_BEARING_KINDS = frozenset(
    {
        OPTION_KIND_CHICKEN_ADDON,
        OPTION_KIND_REVIEW,
        OPTION_KIND_MATERIAL,
        OPTION_KIND_RICE,
        OPTION_KIND_DRINK,
        OPTION_KIND_SIDE,
    }
)

OPTION_KIND_VALUES = (
    OPTION_KIND_SIZE,
    OPTION_KIND_CHICKEN_TYPE,
    OPTION_KIND_CHICKEN_ADDON,
    OPTION_KIND_SPICE,
    OPTION_KIND_REVIEW,
    OPTION_KIND_REQUEST,
    OPTION_KIND_MATERIAL,
    OPTION_KIND_RICE,
    OPTION_KIND_DRINK,
    OPTION_KIND_SIDE,
    OPTION_KIND_SETTLEMENT_EXCLUDE,
    OPTION_KIND_MAIN,
    OPTION_KIND_FEE,
)

# 정산차액/선결제/직원식사처럼 매출 라인이 아닌데 옵션으로 흘러든 것들.
_SETTLEMENT_EXCLUDE_RE = re.compile(r"정산차액|선결제|직원\s*식사|직원\s*호출|직원\s*확인|테스트")
_SPICE_RE = re.compile(
    r"^\s*기본\s*$|기본맛|순한맛|보통맛|중간맛|중간\s*매운맛|중간매운맛|매운맛|아주\s*매운맛|"
    r"아주매운맛|실비맛|맛\s*선택|신라면보다"
)
_REVIEW_RE = re.compile(r"^\s*\[후\.참\]|리뷰\)|리뷰\]|^\s*리뷰")
_REQUEST_RE = re.compile(r"빼주세요|대신|국물\s*많이|국물많이|국물\s*적게|국물적게|괜찮습니다|주세요|많이\s*\(|조절")
_RICE_RE = re.compile(r"공기밥|공깃밥|흑미\s*밥|밥\s*추가")
_DRINK_RE = re.compile(
    r"콜라|사이다|환타|쿨피스|펩시|진로|새로|카스|테라|켈리|하이볼|막걸리|오미자|청하|참이슬|처음처럼|"
    r"\d+\s*(?:ml|ML|㎖)"
)
_MATERIAL_RE = re.compile(
    r"추가|사리|당면|밀떡|가래떡|치즈|대파|파김치|계란|만두|우거지|미나리|버섯|대창|우삼겹|묵은지|"
    r"낙지|새우|비엔나|감자|당근|떡|김가루|야채|채소|\d+\s*g(?:\s|$|\))"
)
_CHICKEN_ADDON_RE = re.compile(r"순살|닭다리살|뼈|^\s*\d*\s*인\s*추가")
_MENU_DEPENDENT_PROFIT_KINDS = frozenset({OPTION_KIND_CHICKEN_ADDON, OPTION_KIND_MATERIAL})

# 재료명 표기 흔들림 정규화: "한우대창 75g" / "한우 대창 75g" → "한우대창"
_MATERIAL_WEIGHT_RE = re.compile(r"\d+\s*(?:g|kg|G|KG|개|마리|장|ml|ML)\s*")
_MATERIAL_COUNT_RE = re.compile(r"\d+\s*(?:개|인분|인|장|팩)\s*")

NO_MODEL_CLASSIFICATION_POLICY = "manual_material_usage_only_no_llm"
_FORBIDDEN_MODEL_MODULE_PARTS = {
    "DB_FinProduct_Map",
    "DB_ItemIdAllocator",
    "qwen_client",
}
_FORBIDDEN_MODEL_CALL_NAMES = {
    "allocate_manual_item_ids",
    "call_llm",
    "call_manager_input_llm",
    "classify_unmapped",
    "find_llm_targets",
    "get_ollama_client_with_candidates",
    "llm_product_map",
    "query_qwen_json",
}


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def assert_no_model_classification_dependencies() -> None:
    """시험 DAG는 LLM/상품표 재분류/상품표 쓰기 호출을 금지한다."""
    source = Path(__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if any(part in alias.name for part in _FORBIDDEN_MODEL_MODULE_PARTS):
                    violations.append(f"import:{alias.name}")
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if any(part in module for part in _FORBIDDEN_MODEL_MODULE_PARTS):
                violations.append(f"from:{module}")
            for alias in node.names:
                if alias.name in _FORBIDDEN_MODEL_CALL_NAMES:
                    violations.append(f"import_name:{alias.name}")
        elif isinstance(node, ast.Call):
            name = _call_name(node.func)
            if name in _FORBIDDEN_MODEL_CALL_NAMES:
                violations.append(f"call:{name}")
    if violations:
        raise RuntimeError(
            "메뉴계층 시험 DAG는 모델 분류/상품표 쓰기 호출 금지: "
            + ", ".join(sorted(set(violations)))
        )


def default_ym() -> str:
    yms = available_yms()
    return yms[-1] if yms else pendulum.now("Asia/Seoul").subtract(months=1).format("YYYY-MM")


def _next_ym(ym: str) -> str:
    return pendulum.parse(f"{ym}-01").add(months=1).format("YYYY-MM")


def _ensure_ym(ym: str | None) -> str:
    text = str(ym or "").strip()
    if not text:
        return default_ym()
    if not re.fullmatch(r"\d{4}-\d{2}", text):
        raise ValueError("ym은 YYYY-MM 형식이어야 합니다.")
    return text


def available_yms() -> list[str]:
    """송파삼전점 원본 데이터가 존재하는 전체 ym 목록."""
    roots = [
        (ANALYTICS_DB / "posfeed_sales", "posfeed_orders.csv"),
        (ANALYTICS_DB / "posfeed_sales_detail", "posfeed_order_item.csv"),
        (RAW_OKPOS_SALES, "okpos_order_item.csv"),
        (BAEMIN_ORDERS_DB, "orders_*.parquet"),
        (COUPANG_ORDERS_DB, "orders_*.parquet"),
    ]
    yms: set[str] = set()
    for root, filename in roots:
        if not root.exists():
            continue
        for path in root.glob(f"brand=*/store=*{TARGET_STORE_TOKEN}/ym=*/{filename}"):
            if not _is_target_store_value(_path_part(path, "store=")):
                continue
            ym_part = _path_part(path, "ym=")
            if re.fullmatch(r"\d{4}-\d{2}", ym_part):
                yms.add(ym_part)
    return sorted(yms)


def resolve_yms(ym: str | list[str] | tuple[str, ...] | None = None) -> list[str]:
    """None이면 운영 대상월(기본 7~8월), all이면 전체기간, 문자열이면 단일 또는 콤마 구분 월 목록."""
    if isinstance(ym, (list, tuple)):
        raw_parts = [str(part).strip() for part in ym]
    else:
        text = str(ym or "").strip()
        if not text:
            available = set(available_yms())
            yms = [target_ym for target_ym in ACTIVE_TARGET_YMS if target_ym in available]
            if not yms:
                raise FileNotFoundError(
                    f"{TARGET_STORE} 운영 대상 원본 데이터 ym을 찾지 못했습니다: {', '.join(ACTIVE_TARGET_YMS)}"
                )
            return yms
        if text.lower() in {"all", "*"} or text in {"전체", "전체기간"}:
            yms = available_yms()
            if not yms:
                raise FileNotFoundError(f"{TARGET_STORE} 원본 데이터 ym을 찾지 못했습니다.")
            return yms
        raw_parts = [part.strip() for part in text.split(",")]
    yms = []
    for part in raw_parts:
        if not part:
            continue
        if not re.fullmatch(r"\d{4}-\d{2}", part):
            raise ValueError("ym은 YYYY-MM, 콤마 구분 YYYY-MM 목록, all, 전체 중 하나여야 합니다.")
        yms.append(part)
    return sorted(dict.fromkeys(yms))


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc, low_memory=False)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, dtype=str, encoding="utf-8", encoding_errors="replace", low_memory=False)


def _ensure_output_path(path: Path) -> None:
    resolved_base = NEW_CLS_DIR.resolve()
    resolved_path = path.resolve()
    if resolved_base != resolved_path and resolved_base not in resolved_path.parents:
        raise ValueError(f"허용되지 않은 출력 경로: {path}")


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    _ensure_output_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info("CSV 저장: %s | %d행", path, len(df))


def _write_jsonl(rows: list[dict], path: Path) -> None:
    _ensure_output_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    path.write_text(text + ("\n" if text else ""), encoding="utf-8")
    logger.info("JSONL 저장: %s | %d행", path, len(rows))


def _write_text(path: Path, text: str) -> None:
    _ensure_output_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    logger.info("설명 파일 저장: %s", path)


def _read_manual_workbook_sheet(sheet_name: str) -> pd.DataFrame:
    if not MANUAL_WORKBOOK_OUTPUT_PATH.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(
            MANUAL_WORKBOOK_OUTPUT_PATH,
            sheet_name=sheet_name,
            dtype=str,
            engine="openpyxl",
        ).fillna("")
    except (ValueError, BadZipFile, OSError):
        logger.warning("01_수기입력.xlsx 시트 읽기 실패로 빈 입력 처리: %s", sheet_name, exc_info=True)
        return pd.DataFrame()


def _manual_or_legacy_sheet(sheet_name: str, legacy_path: Path) -> pd.DataFrame:
    sheet = _read_manual_workbook_sheet(sheet_name)
    if not sheet.empty:
        return sheet
    return _read_csv(legacy_path).fillna("") if legacy_path.exists() else pd.DataFrame()


def _write_excel_workbook(sheets: dict[str, pd.DataFrame], path: Path) -> None:
    _ensure_output_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            safe_name = str(sheet_name)[:31]
            frame = df.copy() if df is not None else pd.DataFrame()
            frame.to_excel(writer, sheet_name=safe_name, index=False)
            ws = writer.book[safe_name]
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            for column_cells in ws.columns:
                max_len = max(len(str(cell.value or "")) for cell in column_cells)
                ws.column_dimensions[column_cells[0].column_letter].width = min(max(max_len + 2, 10), 45)
    logger.info("엑셀 저장: %s | %d시트", path, len(sheets))


def _manual_watch_columns(sheet_name: str, frame: pd.DataFrame) -> list[str]:
    if frame is None:
        columns = []
    else:
        columns = [str(col) for col in frame.columns]
    watched = [
        col
        for col in columns
        if col.endswith("_manual") or col in {"option_kind_확정", "재료명_확정"}
    ]
    if sheet_name == MENU_CHICKEN_PROFILE_SHEET_NAME:
        watched.extend([col for col in MENU_CHICKEN_PROFILE_EDIT_COLUMNS if col != "메모"])
    if sheet_name == JUDGEMENT_OPTION_SHEET_NAME:
        watched.extend(
            [
                "닭유형",
                "사이즈",
                CHICKEN_USAGE_COLUMN,
                HALF_BONE_RATIO_COLUMN,
                HALF_SLOT1_COLUMN,
                HALF_SLOT2_COLUMN,
            ]
        )
    return list(dict.fromkeys([col for col in watched if col in columns]))


def _filled_cell_count(frame: pd.DataFrame, column: str) -> int:
    if frame is None or frame.empty or column not in frame.columns:
        return 0
    return int(_filled(frame[column]).sum())


def _manual_workbook_filled_counts(sheets: dict[str, pd.DataFrame]) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], int] = {}
    for sheet_name, frame in sheets.items():
        if sheet_name == JUDGEMENT_OPTION_SHEET_NAME and frame is not None and not frame.empty:
            frame = frame[~frame.apply(_is_auto_generated_judgement_option, axis=1)]
        for column in _manual_watch_columns(sheet_name, frame):
            counts[(sheet_name, column)] = _filled_cell_count(frame, column)
    return counts


def _manual_profit_amount_rows(sheets: dict[str, pd.DataFrame]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for sheet_name in ("수익률", MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME):
        frame = sheets.get(sheet_name, pd.DataFrame())
        if frame is None or frame.empty:
            continue
        work = frame.reindex(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="").fillna("")
        for _, row in work.iterrows():
            key = str(row.get("수익키", "") or "").strip()
            values = {col: str(row.get(col, "") or "").strip() for col in MANUAL_PROFIT_RATE_AMOUNT_COLUMNS}
            if key and any(values.values()):
                rows.append({"수익키": key, **values})
    return rows


def _profit_manual_value_coverage(sheets: dict[str, pd.DataFrame]) -> dict[str, dict[str, set[str]]]:
    """수익키와 완화키 양쪽으로 색인한 "지금 살아 있는 수기 금액" 목록."""
    coverage: dict[str, dict[str, set[str]]] = {}
    for sheet_name in ("수익률", MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME):
        frame = sheets.get(sheet_name, pd.DataFrame())
        if frame is None or frame.empty:
            continue
        work = frame.reindex(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="").fillna("")
        for _, row in work.iterrows():
            key = str(row.get("수익키", "") or "").strip()
            if not key:
                continue
            for index_key in {key, _relaxed_manual_profit_key(key)}:
                if not index_key:
                    continue
                slot = coverage.setdefault(index_key, {})
                for column in MANUAL_PROFIT_RATE_AMOUNT_COLUMNS:
                    value = str(row.get(column, "") or "").strip()
                    if value:
                        slot.setdefault(column, set()).add(value)
    return coverage


def _read_existing_manual_workbook_sheets(sheet_names: list[str]) -> dict[str, pd.DataFrame]:
    if not MANUAL_WORKBOOK_OUTPUT_PATH.exists():
        return {}
    return {sheet_name: _read_manual_workbook_sheet(sheet_name).fillna("") for sheet_name in sheet_names}


def _guard_manual_workbook_loss(new_sheets: dict[str, pd.DataFrame]) -> None:
    """01_수기입력 workbook의 수기값이 줄어들면 덮어쓰기를 막는다."""
    sheet_names = list(dict.fromkeys([*new_sheets.keys(), MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME]))
    previous_sheets = _read_existing_manual_workbook_sheets(sheet_names)
    if not previous_sheets:
        return

    losses: list[str] = []
    # 개수 비교로는 못 잡는다. 축이 흔들려 수익키가 바뀌면 값이 사라져도 총합은 그대로일 수 있고,
    # 시드가 새 값을 채우면 유실이 상쇄되어 묻힌다. 그래서 "값이 어딘가 살아 있는가"로 본다.
    after_coverage = _profit_manual_value_coverage(new_sheets)
    for row in _manual_profit_amount_rows(previous_sheets):
        key = row["수익키"]
        relaxed = _relaxed_manual_profit_key(key)
        for column in MANUAL_PROFIT_RATE_AMOUNT_COLUMNS:
            value = str(row.get(column, "") or "").strip()
            if not value:
                continue
            if value in after_coverage.get(key, {}).get(column, frozenset()):
                continue
            if relaxed and value in after_coverage.get(relaxed, {}).get(column, frozenset()):
                continue
            # 같은 키가 남아 있으면 값이 바뀐 것이고, 키까지 없으면 통째로 사라진 것이다.
            kind = "수기금액변경" if key in after_coverage else "수기금액소실"
            losses.append(f"수익률계.{column} {kind} {key}={value}")

    before_counts = _manual_workbook_filled_counts(previous_sheets)
    after_counts = _manual_workbook_filled_counts(new_sheets)
    for (sheet_name, column), before in before_counts.items():
        if sheet_name in {"수익률", MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME} and column in MANUAL_PROFIT_RATE_AMOUNT_COLUMNS:
            continue
        if sheet_name in {"수익률", MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME} and column in LEGACY_MANUAL_PROFIT_DROPPED_COLUMNS:
            continue
        after = after_counts.get((sheet_name, column), 0)
        if before > 0 and after < before:
            losses.append(f"{sheet_name}.{column} {before}->{after}")

    if losses:
        preview = ", ".join(losses[:10])
        suffix = f" 외 {len(losses) - 10}개" if len(losses) > 10 else ""
        raise RuntimeError(
            "01_수기입력.xlsx 수기입력값 유실 감지: "
            f"{preview}{suffix}. 기존 workbook을 덮어쓰지 않고 중단합니다."
        )


def _backup_manual_workbook_before_write() -> Path | None:
    if not MANUAL_WORKBOOK_OUTPUT_PATH.exists():
        return None
    stamp = pendulum.now("Asia/Seoul").format("YYYYMMDD_HHmmss")
    backup_path = MANUAL_WORKBOOK_BACKUP_ROOT / stamp / MANUAL_WORKBOOK_OUTPUT_PATH.name
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(MANUAL_WORKBOOK_OUTPUT_PATH, backup_path)
    logger.warning("01_수기입력.xlsx 쓰기 전 로컬 백업 생성: %s", backup_path)
    return backup_path


def _build_readme_text(yms: list[str]) -> str:
    ym_text = ", ".join(yms) if yms else "원본 데이터 없음"
    return f"""# 송파삼전점 메뉴계층 시험 산출물 설명

## 목적

이 폴더는 `DB_MenuHierarchy_Test_Dags`가 송파삼전점 원본 주문 데이터를 읽어 메뉴/옵션 계층을 시험 산출한 결과만 모아두는 위치입니다.
기존 unified 주문서와 상품 검수 테이블은 수정하지 않고, 여기의 CSV와 이 설명 파일만 덮어씁니다.

## 절대 규칙

- 원본 unified 산출물(`mart/unified_sales_grp`)은 쓰지 않습니다.
- `fin_product_*.csv` 상품표는 읽기만 하며 컬럼 추가나 저장을 하지 않습니다.
- 기존 수동 분류는 `fin_product_map_review_input.csv`의 `수동분류_edit`과 `fin_product_map_join.csv`의 `category`를 우선 참고합니다.
- okpos처럼 `item_id`가 `fin_product_grp_input.csv`의 `상품코드`와 직접 맞는 경우에는 `수동분류`도 계층 판단에 사용합니다.
- LLM, Ollama, qwen, 자동 상품분류, item_id 재할당을 호출하지 않습니다.
- 산출 대상 매장은 코드 상수 `송파삼전점`으로 고정되어 있으며 DAG conf로 바꿀 수 없습니다.
- 이 폴더 밖에는 산출물을 쓰지 않습니다.

## 실행 기준

- DAG: `DB_MenuHierarchy_Test_Dags`
- 스케줄: 매일 10:00, 13:00, 15:00 KST (`0 10,13,15 * * *`)
- 기본 대상 기간: 원본 데이터가 존재하는 전체 `ym`
- 현재 탐색된 대상 기간: {ym_text}
- 수동 실행도 DAG conf와 무관하게 항상 전체기간을 재생성합니다.
- 재실행: 기존 파일을 덮어쓰되 `13_manager_input.csv`, `16_option_material_input.csv`, `21_option_kind_master.csv`, `22_menu_weight_master.csv`, `23_material_price_master.csv`, `26_chicken_ratio_master.csv`, `27_profit_rate_master.csv`의 수기 입력 컬럼은 같은 키에서 보존합니다.
- 계층 기준: product 분류가 `메인/1인/세트`인 항목만 독립 main으로 두고, 사이드/음료/기타/리뷰서비스/사이즈/맛/선택 옵션은 부모 메뉴의 option으로 붙입니다.

## 기본 파일

- `04_product_gap.csv`: 주문에는 있으나 상품표에 없거나 미검수인 항목 목록입니다.
- `10_orders.csv`: 선택된 전체기간을 합친 unified와 같은 24컬럼 주문서입니다. `line_role`, `menu_seq` 같은 계층 컬럼을 넣지 않습니다.
- `11_hierarchy.csv`: 나중에 직접 left join할 계층 정보입니다.
- `12_orders_left.csv`: 사람이 바로 검토할 수 있도록 `10_orders`에 계층 컬럼, `option_kind`, 옵션조합, 닭옵션키, 닭유형/사이즈/사용용량/사용용량_합계, 재료원가/수수료/공헌이익, 재료사용량, 표준중량사용량, 옵션재료사용량을 붙인 주문서입니다.
- `13_manager_input.csv`: 담당자가 메뉴+닭옵션키 단위로 닭 속성과 재료 사용량을 수동 입력하는 검토표입니다.
- `14_manager_input_llm_payload.jsonl`: LLM 미사용 정책 확인용 빈 JSONL입니다.
- `15_manager_input_llm_result.jsonl`: LLM 미사용 정책 확인용 빈 JSONL입니다.
- `16_option_material_input.csv`: 담당자가 파김치/대파 추가처럼 옵션 라인 자체의 재료 사용량을 수동 입력하는 검토표입니다. 재료 컬럼은 미리 만들어 두므로 값만 채우면 됩니다.
- `17_menu_weight_input.csv`: 구 표준중량 기준표입니다. `22_menu_weight_master.csv`로 대체되었고 이관 원본으로만 남습니다.
- `18_material_usage_summary.csv`: 메뉴당 사용량과 주문 판매수량을 곱해 재료별 예상 출고중량을 집계한 loss 비교용 요약표입니다.
- `19_validation_issues.csv`: 운영 이슈 목록입니다. `severity=WARN`은 담당자 입력 대기 상태라 DAG를 막지 않고, 그 외 severity가 1행이라도 있으면 DAG는 실패합니다.
- `20_classification_audit.csv`: 검증 통과와 별개로 상품별 닭속성 흔들림, 수익률 0, 수동 닭속성 공백 같은 운영 검토 대상을 남기는 리포트입니다.
- `21_option_kind_master.csv`: 주문에 등장한 품목을 전수 열거해 옵션 성격(`option_kind`)을 확정하는 마스터입니다. `option_kind_제안`은 규칙이 매번 다시 만들고, 계산에 쓰이는 값은 `option_kind_확정`입니다.
- `22_menu_weight_master.csv`: 메뉴 x 사이즈 x 닭유형별 표준 중량 마스터입니다. 재료원가와 재고 loss 비교의 기준표입니다.
- `23_material_price_master.csv`: 재료별 단가표입니다. `단가_manual`이 채워져야 재료원가와 공헌이익이 계산됩니다.
- `24_menu_profit_summary.csv`: 수익채널 x 메뉴 x 사이즈 x 닭유형별 매출/재료원가/수수료/공헌이익 요약입니다. 홀/배달 플랫폼별 수수료가 섞이지 않게 나눕니다.
- `25_completeness.csv`: 차원별 분모/분자/완결률/미완목록입니다. 무엇을 더 채워야 100%인지 여기서 봅니다.
- `25_completeness_baseline.json`: 완결률 기준선입니다. 후퇴 감지에만 쓰이며 사람이 편집하지 않습니다.
- `26_chicken_ratio_master.csv`: 채널 x 메뉴 x 사이즈별 뼈:순살 비율표입니다. 실측으로 자동 채워지고, 표본이 부족한 줄만 `뼈비율_manual`로 채웁니다.
- `27_profit_rate_master.csv`: 수익채널별 품목 원가표입니다. `판매가`와 `상차림포함원가`는 주문 매출/수량과 원가 입력값으로 자동 계산됩니다. 사용자는 `메뉴원가_manual`, `상차림비_manual`만 입력합니다. 수익률은 `1 - 상차림포함원가 / 판매가`로 자동 계산합니다. `사이즈`, `닭유형`은 메뉴와 메뉴종속 옵션의 원가 기준을 나누는 축입니다.
- `28_manual_profit_summary.csv`: `27`번 수익률을 주문 매출에 곱한 수익채널별 품목 수기수익 요약입니다.

## 뼈/순살 신호가 없는 주문

주문 원천에 뼈/순살이 아예 적히지 않는 건이 있습니다. 홀(OKPOS)은 옵션으로 남지 않고,
배민은 옵션이 아니라 메뉴명으로만 구분합니다. 이런 건을 전부 뼈닭으로 단정하면
닭 사용량이 최대 5.4% 틀립니다.

`닭유형_신호` 컬럼이 그 구간을 표시합니다. 최종 `닭유형_판정`은 수기 입력이 덮어쓰므로
그것만으로는 "주문에 실제로 적혀 있었는지"를 사후에 알 수 없어 따로 남깁니다.

- `있음`: 옵션 선택/옵션 변경/메뉴명에 뼈 또는 순살이 실제로 있었음
- `없음`: 어디에도 없었음 (닭 메뉴라서 뼈닭으로 가정했거나 담당자가 채운 구간)
- `닭미사용`: 닭을 쓰지 않는 메뉴

`없음` 구간은 `26_chicken_ratio_master.csv`의 실측 비율로 가중합니다.
비율은 **신호가 있는 주문에서만** 뽑습니다. 수기 입력값까지 섞으면 담당자가 한 값으로
뭉갠 결과가 다시 비율의 근거가 되어 순환합니다.

```
적용비율 우선순위
  1. 뼈비율_manual (담당자 입력)
  2. 실측 - 같은 채널 x 같은 메뉴 x 같은 사이즈   (표본 20개 이상)
  3. 실측 - 같은 채널 x 같은 메뉴
  4. 실측 - 전 채널 x 같은 메뉴
  5. 미적용 - 표본 부족. 기존 값을 그대로 둡니다
```

가중된 행은 `닭유형=혼합`, `닭유형_판정=비율추정`, `뼈비율_적용`에 쓴 비율이 남습니다.
`혼합`은 22번의 뼈닭/순살 행에서 파생되는 값이라 22번에 입력 행을 만들지 않습니다.

이 표는 오차를 줄일 뿐 없애지 못합니다. 근본 해결은 OKPOS에서 뼈/순살을 별도
상품코드로 분리하는 것이고, 그러면 이 표는 필요 없어집니다.

## 수익 계산

```
재료원가    = Σ(22번 메뉴당 표준중량 x 23번 재료단가) x qty
옵션재료원가 = Σ(16번 옵션 재료 사용량 x 23번 재료단가) x qty
수수료      = total_price x 플랫폼수수료율
공헌이익    = total_price - 재료원가 - 옵션재료원가 - 수수료
공헌이익률  = 공헌이익 / total_price x 100
```

`22_menu_weight_master.csv`와 `16_option_material_input.csv`의 **빈칸은 "안 씀"이 아니라 "미입력"입니다.**
안 쓰는 재료에는 `0`을 명시해야 합니다. 이 구분이 없으면 닭만 채워진 상태에서 부재료를
안 쓰는 메뉴처럼 취급되어, 닭값만 뺀 공헌이익이 산출된 것처럼 나옵니다.

- main 행: 22번에 빈 재료가 하나라도 있으면 `재료원가`를 내지 않고 `원가미산출사유`에
  `부재료미입력:<재료명>`을 남깁니다. 어떤 재료가 비었는지는 `표준중량미입력재료` 컬럼에 있습니다.
- 유가 옵션/사이드 행: 16번에 재료가 없으면 `옵션재료원가`를 0이 아니라 공백으로 두고
  `옵션재료미입력`을 남깁니다. 0원 라인(맛선택·요청사항 등)은 원가 0이 맞습니다.

반대로 `23_material_price_master.csv`의 `단가_manual`에는 0을 넣으면 안 됩니다.
**"얼마 쓰는지"에는 0을 쓰고, "얼마인지"에는 0을 쓰지 않습니다.**

수수료율은 하드코딩하지 않고 `mart/delivery_commission/delivery_commission.parquet`의 실측값을 씁니다.
일별 비율은 정산 조정일에 100%를 넘는 이상치가 나오므로(실측 배민 최대 114.3%) **월 단위 합계 비율**을 씁니다.
okpos/posfeed 같은 홀 매출은 수수료율 0입니다. 출처는 `수수료율_출처` 컬럼에서 확인합니다.

## 완결률 게이트

`25_completeness.csv`가 차원별로 분모와 분자를 명시적으로 셉니다.
분모가 코드 안에 숨어 있으면 100%인지 알 수 없으므로 판단은 이 파일에서만 합니다.

| 차원 | 100%의 뜻 |
| --- | --- |
| `option_kind` | 등장한 모든 품목이 `option_kind_확정`을 가짐 |
| `chicken_attr` | 모든 main 행이 닭유형/사이즈를 가짐 (`닭미사용` 포함) |
| `chicken_usage` | 닭을 쓰는 모든 main 행이 `사용용량`을 가짐 |
| `menu_weight` | 모든 메뉴x사이즈x닭유형 조합이 표준중량을 가짐 |
| `material_price` | 쓰이는 모든 재료가 단가를 가짐 |
| `commission` | 모든 배달 라인이 실측 수수료율에 붙음 |
| `std_menu_alias` | 프로모션 태그가 전부 정리됨 |
| `profit` | 매출 있는 모든 main 행이 공헌이익을 가짐 |
| `chicken_ratio` | 뼈/순살 신호가 없는 모든 조합이 적용 비율을 배정받음 |

게이트는 **후퇴하면 실패**합니다. 완결률이 기준선보다 떨어지면 `completeness_regression`으로 DAG가 멈춥니다.
한 차원이 100%에 닿으면 기준선이 100으로 고정되어 그 뒤로는 100% 미만이 곧 실패입니다.
래칫은 저절로 꺼지며 별도 스위치는 없습니다.

처음부터 하드 100% 게이트를 걸지 않는 이유는, 담당자가 입력을 끝낼 때까지 DAG가 몇 주간 계속 실패하면
아무도 산출물을 볼 수 없기 때문입니다. 래칫은 전진만 허용합니다.

### 신메뉴가 들어오면 DAG가 멈춥니다

이건 고장이 아니라 설계된 동작입니다. 신메뉴 하나가 22번에 빈 조합으로 추가되면
분모만 늘어 완결률이 기준선 아래로 떨어지고 `completeness_regression`으로 멈춥니다.
(예: `menu_weight` 64/96 = 66.67% → 64/97 = 65.98% → 실패)

빈칸을 방치한 채로 산출물이 계속 나오면 아무도 안 채우기 때문에 일부러 이렇게 뒀습니다.
멈췄을 때 순서는 이렇습니다.

1. `19_validation_issues.csv`에서 `completeness_regression`이 어느 차원인지 봅니다.
2. `25_completeness.csv`의 `미완요약`에 새로 생긴 빈칸이 있습니다. **채우고 재실행하면 풀립니다.**
3. 당장 못 채우는데 산출물은 봐야 하면, 그 차원의 기준선만 내리고 재실행합니다.
   빈칸은 그대로 남고 다음에 채우면 됩니다.

```python
from modules.transform.pipelines.db.DB_MenuHierarchy_Test import reset_completeness_baseline
reset_completeness_baseline("menu_weight")
```

닭 사용량 총합이 직전 실행 대비 3% 넘게 줄어도 멈춥니다. 수기값이 규칙값으로 밀리는
사고를 잡으려는 것이고(실제로 alias 도입 때 5% 빠진 적이 있습니다), 의도한 변동이면
`reset_completeness_baseline('_chicken_usage_total')` 후 재실행합니다.

### 기준선이 잘못 올라갔을 때

버그가 완결률을 부풀린 채로 기준선에 박히면, 버그를 고친 뒤의 정직한 값이 항상 `후퇴`로 걸려
DAG가 영구 차단됩니다. 실제로 단가 없는 재료를 0원으로 더하던 버그가 `profit` 기준선을 93.45%로
올려놓아 이 상황이 한 번 났습니다.

이때는 기준선을 사람이 직접 내립니다.

```python
from modules.transform.pipelines.db.DB_MenuHierarchy_Test import reset_completeness_baseline
reset_completeness_baseline("profit")          # 한 차원만
reset_completeness_baseline()                  # 전체 초기화
```

DAG는 이 함수를 부르지 않습니다. 자동으로 내려가면 후퇴 감지 자체가 무의미해지기 때문에,
"이전 값이 틀렸다"는 판단이 있을 때만 수동으로 실행합니다.

## 담당자가 채워야 할 것

1. `23_material_price_master.csv`의 `단가_manual` — 재료별 단가입니다. 이게 없으면 수익이 전부 0입니다.
2. `22_menu_weight_master.csv`의 `*사용량_manual` — 메뉴 1개당 재료 표준중량입니다. **안 쓰는 재료에는 `0`을 적습니다.**
3. `16_option_material_input.csv`의 `*사용량_manual` — 유가 옵션이 쓰는 재료입니다. 비어 있으면 그 옵션의 원가가 안 잡힙니다.
4. `21_option_kind_master.csv`의 `option_kind_확정` — `option_kind_제안`을 확인하고 확정합니다.
5. `26_chicken_ratio_master.csv`의 `뼈비율_manual` — 실측 표본이 부족한 조합만 채웁니다.
6. `13_manager_input.csv`의 `닭유형_manual`/`사이즈_manual`/`닭사용량_manual` — 규칙이 못 정한 조합만 채우면 됩니다.

무엇이 남았는지는 `25_completeness.csv`의 `미완요약`에서 봅니다.
채우는 순서와 방법은 `입력가이드.md`에 있습니다.

## 이 분석의 한계

빈칸을 다 채워도 남는 것들입니다. 숫자를 쓰기 전에 알고 있어야 합니다.

**1. 공헌이익은 순이익이 아닙니다.**
계산식이 `매출 − 재료원가 − 옵션재료원가 − 수수료`뿐입니다. 양념·기름·가스·인건비·임대료·
포장재가 들어갈 자리가 없습니다. 메뉴끼리 비교하거나 "하나 더 팔면 얼마 남나"에는 쓸 수 있지만
점포 손익과는 다릅니다.

**2. 뼈/순살 일부는 추정입니다.**
주문 원천에 뼈/순살이 아예 없는 행이 있습니다(`닭유형_신호 = 없음`). 26번 실측 비율로
가중하지만 추정은 추정입니다. 근본 해결은 OKPOS에서 뼈/순살을 별도 상품코드로 나누는 것이고,
그러면 26번 자체가 필요 없어집니다.

**3. 수수료는 월 단위 실측 평균입니다.**
개별 주문의 실제 수수료가 아닙니다. 일별 비율은 정산 조정일에 100%를 넘는 이상치가 나와서
쓸 수 없습니다(실측 배민 최대 114.3%).

**4. 단가는 시점 하나입니다.**
`기준일_manual`이 한 줄뿐이라 원가가 오르내려도 과거 주문에 소급 적용됩니다.
시세 변동을 반영하려면 기간별 단가 구조가 따로 필요합니다.

**5. 송파삼전점 하나입니다.**
`TARGET_STORE`가 코드에 고정돼 있고 DAG conf로 바꿀 수 없습니다.

**6. 재료소모량은 표에 칸을 만든 재료만 나옵니다.**
22번에 `우거지사용량_manual` 같은 컬럼이 있는 재료만 집계됩니다. 새 재료를 보려면
`~사용량_manual`로 끝나는 컬럼을 22번에 추가하면 23번 단가표에도 자동으로 줄이 생깁니다.

## 신메뉴 운영 흐름

1. 신메뉴가 들어오면 `DB_FinProduct_Map_Dags`가 기존 상품표와 규칙, LLM 후보 분류로 `fin_product_map_review_input.csv` 검수 대상을 만듭니다.
2. 사람은 `표준_메뉴명_edit`, `수동분류_edit`, `대표메뉴`, 중복 여부를 검토합니다. LLM 결과를 그대로 확정하지 않고 review 파일에서 확인합니다.
3. 검수 반영 후 `DB_MenuHierarchy_Test_Dags`를 다시 실행하면 이 폴더의 주문 계층과 닭도리탕 옵션 리포트가 갱신됩니다.
4. 메뉴계층 시험 DAG는 상품표 재분류나 상품표 쓰기, LLM 후보 생성을 하지 않고 담당자 수기 입력값만 보존·반영합니다.

## 디버그용 수동 함수

`dump_product_tables`, `build_raw_lines`, `write_reports`, `build_chicken_option_reports`는 필요할 때 함수 단위로도 실행합니다.
기본 DAG 실행과 `run_all()`은 파일 수를 줄이기 위해 디버그 CSV를 만들지 않습니다.

## 조인 기준

`11_hierarchy.csv`를 후속 작업에서 붙일 때는 `_pk`를 우선 사용합니다.
필요하면 `sale_date`, `source`, `store`, `platform`, `order_id`, `item_seq` 복합키로 대체합니다.
`10_orders.csv`는 조인 전 순수 unified 양식 비교용이므로 계층 컬럼을 직접 추가하지 않습니다.

## source별 계층 로직

- posfeed: 원본 `상품명`의 앞 작은따옴표 마커를 옵션 신호로 보고, 마커 없는 행을 부모 메뉴로 봅니다.
- 쿠팡: 원본 `menu_name`을 행별 부모 메뉴로 사용하고, `menu_options`는 옵션 판단 보조값으로만 봅니다.
- 배민: 원본 `주문내역`의 `외 N건`을 메뉴 개수 제약으로만 쓰고, 메뉴 이름은 경계 라인의 상품표 `대표메뉴`에서 그룹별로 가져옵니다. 경계가 부족하면 주문 꼬리부터 사이드/음료 단품을 승격합니다.
- okpos: 기존 상품 마스터의 메인 후보를 우선 사용하고, 없으면 금액과 옵션/수수료 명칭 규칙으로 보조 판정합니다.

## 수정 시 확인할 점

- `10_orders_{{ym}}.csv` 컬럼은 `UNIFIED_COLUMNS` 24개와 정확히 같아야 합니다.
- `11_hierarchy_{{ym}}.csv`의 `parent_item_seq`는 같은 주문 안에 존재하는 `item_seq`를 가리켜야 합니다.
- 메뉴 계층을 바꿔도 `total_price`, `unit_price`, `qty`, `discount_amount`, `order_cnt` 합계가 라인 유실/중복 없이 유지되어야 합니다.
- `menu_name`은 검수된 메뉴명(대표메뉴)이고 `item_name`은 원천 라인명을 유지합니다. 둘을 같게 만들지 않습니다.
- 배민 dedup은 주문 단위 최신 `collected_at` 기준입니다. 한 주문 안에서 반복된 동일 옵션 라인을 지우면 안 됩니다.
- 닭 정보는 같은 `order_id + menu_seq` 안의 메인메뉴와 옵션을 함께 보고 판정합니다. 기존 닭 전용 최종값은 담당자 수동값, 기존 상품 검수 수동값, 규칙값 순으로 반영합니다.
- `13_manager_input.csv`의 기존 수동 입력값은 같은 `source, brand, store, std_menu_name, 닭옵션키` 키가 유지되는 한 재생성해도 보존되어야 합니다.
- `닭옵션키`는 `option_kind`가 `사이즈`/`닭유형`인 옵션만 담습니다. 맛선택, 음료, 리뷰, 요청사항이 섞이면 키가 3,740가지로 폭발해 사람이 채울 수 없습니다. `옵션조합`은 눈으로 볼 참고용으로만 남습니다.
- 닭 옵션 없는 주문그룹의 `닭옵션키`는 빈 값 대신 `옵션없음`으로 표시합니다.
- `사용용량`은 메뉴 1개 기준값이고 `사용용량_합계`가 qty를 반영한 라인 총량입니다. 월별 소진량 집계는 반드시 `사용용량_합계`를 씁니다.
- 닭을 안 쓰는 메뉴는 빈값이 아니라 `닭유형=닭미사용`, `사이즈=-`, `사용용량=0`으로 명시합니다. 빈값으로 두면 판정 실패와 구분되지 않습니다.
- 프로모션 태그로 갈라진 표준 메뉴명은 코드의 `_STD_MENU_ALIAS`로만 병합합니다. 태그를 기계적으로 떼면 `[한우 대창] 순살 곱도리탕`이 `한우 순살 곱도리탕`과 오히려 더 어긋납니다. 미등록 태그는 자동 병합하지 않고 `25_completeness.csv`의 `std_menu_alias` 미완으로 남습니다.
- `13_manager_input.csv`에 담당자가 `미나리사용량_manual`, `소스사용량_manual`처럼 `사용량_manual`으로 끝나는 컬럼을 추가하면 `12_orders_left.csv`의 `재료사용량`에 `미나리=0.1 | 소스=0.2` 형식으로 반영합니다.
- `16_option_material_input.csv`의 기존 수동 입력값은 같은 `source, brand, store, item_id, item_name` 키가 유지되는 한 재생성해도 보존되어야 합니다.
- 파김치/대파 추가 같은 옵션 자체 수익을 보려면 `16_option_material_input.csv`에 `파김치사용량_manual`, `대파사용량_manual`처럼 입력하고 재실행합니다. 그러면 옵션 행의 `옵션재료사용량`에 반영됩니다.
- 주문서상 예상 출고중량과 재고 실사값을 비교하려면 `17_menu_weight_input.csv`에 `닭사용량_manual`, `우거지사용량_manual`, `순살추가사용량_manual` 등을 입력하고 재실행합니다. `18_material_usage_summary.csv`의 `예상사용량`을 실사 재고 변동과 비교합니다.
- `12_orders_left.csv`의 `사용용량`, `재료사용량`, `표준중량사용량`은 main 행 기준 분석 컬럼입니다. option/side 행은 부모 메뉴의 검수 맥락을 가질 수 있지만 닭 사용량 합산에는 포함하지 않습니다.
- `1인 순살`, `2인 순살`, `순살 정식(2인이상)`, `1인 백숙/닭한마리`처럼 메뉴명 자체가 닭유형과 사이즈를 고정하는 상품은 옵션 조합보다 메뉴명 기준 속성을 우선합니다.
- 수익은 마진율%가 아니라 재료원가 기반으로 계산합니다. `22_menu_weight_master.csv`의 표준중량과 `23_material_price_master.csv`의 단가가 채워져야 `공헌이익`이 나옵니다. 비면 `19_validation_issues.csv`에 `profit_missing`(severity=WARN)으로 남고 DAG는 멈추지 않습니다. 대신 `25_completeness.csv`의 `profit` 완결률이 올라가지 않습니다.
- 왜 공헌이익이 안 나오는지는 `12_orders_left.csv`의 `원가미산출사유`(`표준중량미입력`, `단가없음:닭` 등)에서 봅니다.
- 새 로직을 추가할 때도 상품표 저장이나 상품표 재분류가 생기면 안 됩니다.
- `line_role`은 원본 마커보다 기존 product 분류를 우선합니다. `메인/1인/세트`는 main, `사이드/음료/기타/옵션/토핑/리뷰`는 option입니다.
"""


def write_readme(ym: str | list[str] | tuple[str, ...] | None = None) -> str:
    yms = resolve_yms(ym)
    text = _build_readme_text(yms)
    _write_text(README_OUTPUT_PATH, text)
    return f"설명.md 저장 완료 | yms={','.join(yms)}"


def _path_part(path: Path, prefix: str) -> str:
    for part in path.parts:
        if part.startswith(prefix):
            return part.split("=", 1)[1]
    return ""


def _store_key(value: object) -> str:
    series = pd.Series([str(value or "").strip()])
    return _strip_brand(_normalize_store_names(series)).iloc[0]


def _is_target_store_value(value: object) -> bool:
    return _store_key(value) == TARGET_STORE


def _find_partition_files(root: Path, ym: str, filename: str) -> list[Path]:
    files = sorted(root.glob(f"brand=*/store=*{TARGET_STORE_TOKEN}/ym={ym}/{filename}"))
    return [path for path in files if _is_target_store_value(_path_part(path, "store="))]


def _clean_nan_series(s: pd.Series) -> pd.Series:
    out = s.fillna("").astype(str).str.strip()
    return out.mask(out.str.lower().isin({"nan", "none", "<na>"}), "")


def _normalize_item_key(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[\s\-_]+", "", text)
    return re.sub(r"[^0-9a-z가-힣一-龥]+", "", text)


def _strip_marker_name(value: object) -> str:
    text = str(value or "").strip()
    return re.sub(r"^'\s*[+\-=@]?\s*", "", text).strip()


def _is_forbidden_option_main_name(value: object) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if not _OPTION_MAIN_FORBIDDEN_RE.search(text):
        return False
    # 세트/복합 메인 메뉴명 안의 "추가" 같은 단어는 메인 정체성을 우선한다.
    if _has_any_token(text, _CHICKEN_MENU_TOKENS):
        return not re.fullmatch(
            r"\s*(?:기본|기본맛|순한맛|중간\s*매운맛|중간맛.*|매운맛.*|아주\s*매운맛.*|아주매운맛.*|보통맛.*|뼈|순살)\s*",
            text,
        )
    return True


def _strip_leading_tags(value: object) -> str:
    text = str(value or "").strip()
    while True:
        match = _LEADING_TAG_RE.match(text)
        if not match:
            return text
        inner = match.group(1).strip()
        if _SIZE_TAG_RE.fullmatch(inner):
            return text
        text = text[match.end():].strip()


def _canonical_std_menu_name(value: object) -> str:
    """프로모션 태그로 갈라진 표준 메뉴명을 alias 표로만 병합한다."""
    text = str(value or "").strip()
    if not text:
        return text
    return _STD_MENU_ALIAS_LOOKUP.get(re.sub(r"\s+", "", text), text)


def _unaliased_promo_tag_names(std_menu_names: pd.Series) -> list[str]:
    """alias에 없는데 프로모션 태그가 붙은 메뉴명. 자동 병합하지 않고 보고만 한다."""
    out: list[str] = []
    for value in _unique_nonempty([str(v or "").strip() for v in std_menu_names.tolist()]):
        match = _LEADING_TAG_RE.match(value)
        if not match:
            continue
        inner = match.group(1).strip()
        if _SIZE_TAG_RE.fullmatch(inner) or inner in _STD_MENU_KEPT_TAGS:
            continue
        if re.sub(r"\s+", "", value) in _STD_MENU_ALIAS_LOOKUP:
            continue
        out.append(value)
    return out


def _canonical_menu_key(value: object) -> str:
    text = str(value or "").strip()
    prefix = ""
    while True:
        match = _LEADING_TAG_RE.match(text)
        if not match:
            break
        inner = match.group(1).strip()
        if _SIZE_TAG_RE.fullmatch(inner):
            break
        if inner in _CANONICAL_TAG_PREFIX:
            prefix += _CANONICAL_TAG_PREFIX[inner]
            text = text[match.end():].strip()
            continue
        if inner in _CANONICAL_DROP_TAGS:
            text = text[match.end():].strip()
            continue
        break
    text = prefix + text
    text = re.sub(r"\((?:밥포함|공깃밥 포함|공기밥 미포함|2인이상|1인 단품)\)", "", text)
    text = re.sub(r"\s*1\s*인분\s*$", "", text)
    return _normalize_item_key(text)


def _make_tmp_item_id(source: object, brand: object, store: object, item_name: object, unit_price: object) -> str:
    raw = "|".join(str(v or "").strip() for v in (source, brand, store, item_name, unit_price))
    return "TMP_" + hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def _review_input_key_columns(df: pd.DataFrame) -> list[str]:
    return [col for col in ["store", "source", "brand", "item_id", "item_name"] if col in df.columns]


def _normalize_review_input_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().fillna("")
    for col in ["store", "source", "brand", "item_id", "item_name", "unitprice"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("").astype(str).str.strip()
    for col in ["검수유무", "review_status_edit"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str).str.strip()
    return out


def _load_menu_hierarchy_review_input() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    base_path = existing_fin_product_map_review_csv_path()
    new_path = NEW_FIN_PRODUCT_MAP_REVIEW_CSV_PATH
    seen_paths: set[Path] = set()
    for path in (base_path, new_path):
        if path in seen_paths or not path.exists():
            continue
        seen_paths.add(path)
        frames.append(_read_csv(path))
    if not frames:
        fallback = existing_new_fin_product_map_review_csv_path()
        return _read_csv(fallback).fillna("") if fallback.exists() else pd.DataFrame()

    out = pd.concat(frames, ignore_index=True, sort=False).fillna("")
    out = _normalize_review_input_frame(out)
    key_columns = _review_input_key_columns(out)
    if key_columns:
        out = out.drop_duplicates(subset=key_columns, keep="last")
    return out.reset_index(drop=True)


def _load_product_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    master = _read_csv(FIN_PRODUCT_CSV_PATH) if FIN_PRODUCT_CSV_PATH.exists() else pd.DataFrame()
    product_map = _read_csv(FIN_PRODUCT_MAP_CSV_PATH) if FIN_PRODUCT_MAP_CSV_PATH.exists() else pd.DataFrame()
    product_join = _read_csv(FIN_PRODUCT_MAP_JOIN_CSV_PATH) if FIN_PRODUCT_MAP_JOIN_CSV_PATH.exists() else pd.DataFrame()
    product_review = _load_menu_hierarchy_review_input()
    return master.fillna(""), product_map.fillna(""), product_join.fillna(""), product_review.fillna("")


def _target_product_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "store" not in df.columns:
        return df.iloc[0:0].copy()
    store_key = df["store"].map(_store_key)
    return df[store_key.eq(TARGET_STORE)].copy()


class _ProductLookup:
    def __init__(self) -> None:
        self.master, self.product_map, self.product_join, self.product_review = _load_product_tables()
        self.master_target = _target_product_rows(self.master)
        self.map_target = _target_product_rows(self.product_map)
        self.join_target = _target_product_rows(self.product_join)
        self.review_target = _target_product_rows(self.product_review)
        self.item_id_by_row = self._build_item_id_lookup()
        self.main_codes = self._build_main_code_set()
        self.category_by_item = self._build_category_lookup()
        self.std_by_item = self._build_std_lookup()
        self.std_edit_by_item = self._build_std_edit_lookup()
        self.reviewed_items = self._build_reviewed_items()
        self.alias_by_name = self._build_alias_lookup()
        self.representative_by_item = self._build_representative_lookup()
        self.canonical_profile = self._build_canonical_profile()

    def _build_item_id_lookup(self) -> dict[tuple[str, str, str, str, str], str]:
        out: dict[tuple[str, str, str, str, str], str] = {}
        for frame in (self.map_target, self.review_target):
            if frame.empty:
                continue
            if frame is self.review_target:
                status_col = "review_status_edit" if "review_status_edit" in frame.columns else "검수유무"
                status = frame.get(status_col, pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
                frame = frame[status.eq("1")]
            for _, row in frame.iterrows():
                key = (
                    str(row.get("source", "")).strip(),
                    str(row.get("brand", "")).strip(),
                    _store_key(row.get("store", "")),
                    _normalize_item_key(row.get("item_name", "")),
                    str(row.get("unitprice", "")).strip(),
                )
                item_id = str(row.get("item_id", "")).strip()
                if item_id and all(key[:4]) and not item_id.startswith("TMP_"):
                    out[key] = item_id
                    out.setdefault((*key[:4], ""), item_id)
        return out

    def _build_main_code_set(self) -> set[tuple[str, str, str, str]]:
        out: set[tuple[str, str, str, str]] = set()
        if self.master_target.empty:
            return out
        latest = self.master_target
        if "is_latest" in latest.columns:
            latest = latest[latest["is_latest"].fillna("").astype(str).str.upper().eq("Y")]
        exclude = latest.get("exclude_check", pd.Series("", index=latest.index)).fillna("").astype(str).str.upper()
        main = latest.get("is_main_candidate", pd.Series("", index=latest.index)).fillna("").astype(str).str.upper().eq("Y")
        manual = latest.get("수동분류", pd.Series("", index=latest.index)).fillna("").astype(str).str.strip().isin(_MAIN_CATEGORIES)
        for _, row in latest[(exclude != "Y") & (main | manual)].iterrows():
            code = str(row.get("상품코드", "")).strip()
            if not code:
                continue
            out.add((str(row.get("source", "")).strip(), str(row.get("brand", "")).strip(), _store_key(row.get("store", "")), code))
        return out

    def _build_category_lookup(self) -> dict[tuple[str, str, str, str], str]:
        out: dict[tuple[str, str, str, str], str] = {}
        for fallback in (self.map_target,):
            if fallback.empty:
                continue
            for _, row in fallback.iterrows():
                key = (
                    str(row.get("source", "")).strip(),
                    str(row.get("brand", "")).strip(),
                    _store_key(row.get("store", "")),
                    str(row.get("item_id", "")).strip(),
                )
                category = str(row.get("수동분류_edit", row.get("category", ""))).strip()
                if all(key) and category:
                    out[key] = category
        if not self.join_target.empty:
            for _, row in self.join_target.iterrows():
                key = (
                    str(row.get("source", "")).strip(),
                    str(row.get("brand", "")).strip(),
                    _store_key(row.get("store", "")),
                    str(row.get("item_id", "")).strip(),
                )
                category = str(row.get("category", "")).strip()
                if all(key) and category:
                    out[key] = category
        if not self.master_target.empty:
            latest = self.master_target
            if "is_latest" in latest.columns:
                latest = latest[latest["is_latest"].fillna("").astype(str).str.upper().eq("Y")]
            for _, row in latest.iterrows():
                key = (
                    str(row.get("source", "")).strip(),
                    str(row.get("brand", "")).strip(),
                    _store_key(row.get("store", "")),
                    str(row.get("상품코드", "")).strip(),
                )
                category = str(row.get("수동분류", "")).strip()
                if all(key) and category:
                    out[key] = category
        if not self.review_target.empty:
            for _, row in self.review_target.iterrows():
                key = (
                    str(row.get("source", "")).strip(),
                    str(row.get("brand", "")).strip(),
                    _store_key(row.get("store", "")),
                    str(row.get("item_id", "")).strip(),
                )
                category = str(row.get("수동분류_edit", row.get("category", ""))).strip()
                if all(key) and category:
                    out[key] = category
        return out

    def category(self, source: object, brand: object, store: object, item_id: object) -> str:
        key = (str(source or "").strip(), str(brand or "").strip(), _store_key(store), str(item_id or "").strip())
        return self.category_by_item.get(key, "")

    def _build_canonical_profile(self) -> dict[str, dict[str, str]]:
        out: dict[str, dict[str, str]] = {}
        if self.map_target.empty and self.review_target.empty:
            return out

        def put(name: object, row: pd.Series) -> None:
            key = _canonical_menu_key(name)
            if not key:
                return
            category = str(row.get("수동분류_edit", row.get("category", "")) or "").strip()
            if _is_forbidden_option_main_name(name):
                category = "옵션"
            current = out.setdefault(
                key,
                {
                    "category": "",
                    "std_menu_name": "",
                    "representative": "",
                    "item_id": "",
                },
            )
            if _CATEGORY_PRIORITY.get(category, 0) > _CATEGORY_PRIORITY.get(current["category"], 0):
                current["category"] = category
            item_id = str(row.get("item_id", "") or "").strip()
            if category in _MAIN_CATEGORIES and item_id and not current["item_id"]:
                current["item_id"] = item_id
            representative = str(row.get("대표메뉴", "") or "").strip()
            std = str(row.get("표준_메뉴명_edit", "") or "").strip()
            if representative and not current["representative"]:
                current["representative"] = representative
            if std and not current["std_menu_name"]:
                current["std_menu_name"] = std

        for frame in (self.map_target, self.review_target):
            if frame.empty:
                continue
            for _, row in frame.iterrows():
                for column in ("item_name", "표준_메뉴명_edit", "대표메뉴"):
                    value = str(row.get(column, "") or "").strip()
                    if value:
                        put(value, row)
        return out

    def canonical_category(self, item_name: object, std_menu_name: object = "") -> str:
        profile = getattr(self, "canonical_profile", {})
        hits = []
        for value in (item_name, std_menu_name):
            hit = profile.get(_canonical_menu_key(value), {})
            category = str(hit.get("category", "") or "").strip()
            if category:
                hits.append(category)
        if not hits:
            return ""
        return max(hits, key=lambda category: _CATEGORY_PRIORITY.get(category, 0))

    def _build_std_lookup(self) -> dict[tuple[str, str, str, str], str]:
        out: dict[tuple[str, str, str, str], str] = {}
        for frame, column in (
            (self.map_target, "표준_메뉴명_edit"),
            (self.join_target, "standard_menu_name"),
            (self.review_target, "표준_메뉴명_edit"),
        ):
            if frame.empty or column not in frame.columns:
                continue
            for _, row in frame.iterrows():
                key = (
                    str(row.get("source", "")).strip(),
                    str(row.get("brand", "")).strip(),
                    _store_key(row.get("store", "")),
                    str(row.get("item_id", "")).strip(),
                )
                std = str(row.get(column, "")).strip()
                if all(key) and std:
                    out[key] = std
        return out

    def _build_std_edit_lookup(self) -> dict[tuple[str, str, str, str], str]:
        out: dict[tuple[str, str, str, str], str] = {}
        for frame in (self.map_target, self.review_target):
            if frame.empty or "표준_메뉴명_edit" not in frame.columns:
                continue
            for _, row in frame.iterrows():
                key = (
                    str(row.get("source", "")).strip(),
                    str(row.get("brand", "")).strip(),
                    _store_key(row.get("store", "")),
                    str(row.get("item_id", "")).strip(),
                )
                std_edit = str(row.get("표준_메뉴명_edit", "")).strip()
                if all(key) and std_edit:
                    out[key] = std_edit
        return out

    def _build_representative_lookup(self) -> dict[tuple[str, str, str, str], str]:
        out: dict[tuple[str, str, str, str], str] = {}
        for frame in (self.map_target, self.review_target):
            if frame.empty or "대표메뉴" not in frame.columns:
                continue
            for _, row in frame.iterrows():
                key = (
                    str(row.get("source", "")).strip(),
                    str(row.get("brand", "")).strip(),
                    _store_key(row.get("store", "")),
                    str(row.get("item_id", "")).strip(),
                )
                representative = str(row.get("대표메뉴", "")).strip()
                if all(key) and representative:
                    out[key] = representative
        return out

    def _build_reviewed_items(self) -> set[tuple[str, str, str, str]]:
        out: set[tuple[str, str, str, str]] = set()
        for frame in (self.map_target, self.review_target):
            if frame.empty:
                continue
            status_col = "review_status_edit" if "review_status_edit" in frame.columns else "검수유무"
            status = frame.get(status_col, pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
            for _, row in frame[status.eq("1")].iterrows():
                key = (
                    str(row.get("source", "")).strip(),
                    str(row.get("brand", "")).strip(),
                    _store_key(row.get("store", "")),
                    str(row.get("item_id", "")).strip(),
                )
                if all(key):
                    out.add(key)
        return out

    def _build_alias_lookup(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for frame in (self.map_target, self.review_target):
            if frame.empty:
                continue
            status_col = "review_status_edit" if "review_status_edit" in frame.columns else "검수유무"
            status = frame.get(status_col, pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
            classified = frame.get("classified_by", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
            reviewed = frame[status.eq("1") & classified.isin(["", "human"])]
            for _, row in reviewed.iterrows():
                item_name = str(row.get("item_name", "")).strip()
                std = str(row.get("표준_메뉴명_edit", "")).strip()
                if item_name and std and _normalize_item_key(item_name) != _normalize_item_key(std):
                    out[_normalize_item_key(item_name)] = std
        return out

    def item_id(self, source: object, brand: object, store: object, item_name: object, unit_price: object = "", code: object = "") -> str:
        source_s = str(source or "").strip()
        brand_s = str(brand or "").strip()
        store_s = _store_key(store)
        code_s = str(code or "").strip()
        if source_s == OKPOS_SOURCE and code_s:
            return code_s
        key = (source_s, brand_s, store_s, _normalize_item_key(item_name), str(unit_price or "").strip())
        mapped = self.item_id_by_row.get(key) or self.item_id_by_row.get((*key[:4], ""))
        if mapped:
            return mapped
        canonical = getattr(self, "canonical_profile", {}).get(_canonical_menu_key(item_name), {})
        canonical_item_id = str(canonical.get("item_id", "") or "").strip()
        if canonical.get("category", "") in _MAIN_CATEGORIES and canonical_item_id:
            return canonical_item_id
        return _make_tmp_item_id(source_s, brand_s, store_s, item_name, unit_price)

    def is_main(self, source: object, brand: object, store: object, item_id: object, item_name: object, unit_price: object) -> bool:
        source_s = str(source or "").strip()
        brand_s = str(brand or "").strip()
        store_s = _store_key(store)
        item_id_s = str(item_id or "").strip()
        key = (source_s, brand_s, store_s, item_id_s)
        category = self.category_by_item.get(key, "")
        canonical_category = self.canonical_category(item_name)
        if _is_forbidden_option_main_name(item_name):
            return False
        if category in _MAIN_CATEGORIES:
            return True
        if canonical_category in _MAIN_CATEGORIES and category not in {"할인", "제외", "수수료"}:
            return True
        if key in self.main_codes and category not in _STRICT_NON_MAIN_CATEGORIES:
            return True
        if category in _NON_MAIN_CATEGORIES:
            return False
        price = pd.to_numeric(pd.Series([unit_price]), errors="coerce").fillna(0).iloc[0]
        name = str(item_name or "").strip()
        if price <= 0:
            return False
        if not str(item_id_s).startswith("TMP_"):
            return False
        return not _OPTION_LIKE_RE.search(name) and not _FEE_LIKE_RE.search(name)

    def is_standalone_candidate(
        self,
        source: object,
        brand: object,
        store: object,
        item_id: object,
        item_name: object,
        unit_price: object,
        menu_vocab: set[str],
    ) -> bool:
        price = pd.to_numeric(pd.Series([unit_price]), errors="coerce").fillna(0).iloc[0]
        if price <= 0:
            return False
        key = (str(source or "").strip(), str(brand or "").strip(), _store_key(store), str(item_id or "").strip())
        category = self.category_by_item.get(key, "")
        if category not in _STANDALONE_CATEGORIES:
            return False
        if category == "음료":
            return True
        name = str(item_name or "").strip()
        if name in menu_vocab:
            return True
        name_key = _normalize_item_key(name)
        representative = self.representative_by_item.get(key, "")
        std_edit = self.std_edit_by_item.get(key, "")
        return bool(
            (representative and _normalize_item_key(representative) == name_key)
            or (std_edit and _normalize_item_key(std_edit) == name_key)
        )

    def std_name(self, source: object, brand: object, store: object, item_id: object, fallback_name: object) -> str:
        key = (str(source or "").strip(), str(brand or "").strip(), _store_key(store), str(item_id or "").strip())
        category = self.category_by_item.get(key, "")
        representative = self.representative_by_item.get(key, "")
        fallback = str(fallback_name or "").strip()
        if category in _NON_MAIN_CATEGORIES and representative:
            if fallback:
                return fallback
            return representative
        if str(source or "").strip() == COUPANG_SOURCE and fallback:
            canonical = _canonical_std_menu_name(fallback)
            if canonical and canonical != fallback:
                return canonical
        std = self.std_by_item.get(key, "")
        if std:
            return std
        alias = self.alias_by_name.get(_normalize_item_key(fallback_name), "")
        if alias:
            return alias
        stripped = _strip_leading_tags(fallback)
        if stripped and stripped != fallback:
            return _canonical_std_menu_name(stripped)
        return ""

    def menu_display_name(
        self,
        source: object,
        brand: object,
        store: object,
        item_id: object,
        fallback_name: object,
        unit_price: object = "",
        alias: dict[tuple[str, int], str] | None = None,
    ) -> str:
        key = (str(source or "").strip(), str(brand or "").strip(), _store_key(store), str(item_id or "").strip())
        fallback = str(fallback_name or "").strip()
        representative = self.representative_by_item.get(key, "")
        if representative:
            return representative
        if alias:
            price = int(pd.to_numeric(pd.Series([unit_price]), errors="coerce").fillna(0).iloc[0])
            hit = alias.get((_normalize_item_key(fallback), price))
            if hit:
                return hit
        return self.std_edit_by_item.get(key, "") or self.std_by_item.get(key, "") or fallback

    def gap_status(self, source: object, brand: object, store: object, item_id: object) -> str:
        key = (str(source or "").strip(), str(brand or "").strip(), _store_key(store), str(item_id or "").strip())
        if key in self.std_by_item:
            return "" if key in self.reviewed_items or str(item_id).startswith("TMP_") is False else "미검수"
        if str(item_id or "").startswith("TMP_"):
            return "상품표_없음"
        return "join_없음"


def assign_menu_hierarchy(
    df: pd.DataFrame,
    boundary: pd.Series,
    *,
    order_col: str | list[str] = "order_id",
    sort_cols: list[str] | None = None,
    name_col: str = "item_name",
    seq_col: str = "item_seq",
    attr_method: str = "",
) -> pd.DataFrame:
    out = df.copy()
    boundary = boundary.reindex(out.index).fillna(False).astype(bool)
    out["menu_seq"] = ""
    out["parent_item_seq"] = ""
    out["_menu_name_new"] = ""
    out["attr_method"] = attr_method

    for _, group in out.groupby(order_col, sort=False):
        if sort_cols == [seq_col]:
            grp = group.assign(_sort_seq=pd.to_numeric(group[seq_col], errors="coerce")).sort_values(
                ["_sort_seq", seq_col],
                kind="mergesort",
            )
        else:
            grp = group.sort_values(sort_cols, kind="mergesort") if sort_cols else group
        cur_seq = 0
        cur_name = ""
        cur_parent = ""
        pending: list[int] = []
        for idx in grp.index:
            if bool(boundary.at[idx]):
                cur_seq += 1
                cur_name = str(out.at[idx, name_col]).strip()
                cur_parent = str(out.at[idx, seq_col]).strip()
                for pending_idx in pending:
                    out.at[pending_idx, "menu_seq"] = "1"
                    out.at[pending_idx, "parent_item_seq"] = cur_parent
                    out.at[pending_idx, "_menu_name_new"] = cur_name
                pending = []
            if cur_seq == 0:
                pending.append(idx)
                continue
            out.at[idx, "menu_seq"] = str(cur_seq)
            out.at[idx, "parent_item_seq"] = cur_parent
            out.at[idx, "_menu_name_new"] = cur_name

    return out


def _hierarchy_group_columns(df: pd.DataFrame) -> str | list[str]:
    for columns in (["_order_key"], ["source", "sale_date", "order_id"], ["source", "order_id"], ["order_id"]):
        if all(column in df.columns for column in columns):
            return columns[0] if len(columns) == 1 else columns
    return "order_id"


def _hierarchy_key_series(df: pd.DataFrame) -> pd.Series:
    columns = _hierarchy_group_columns(df)
    if isinstance(columns, str):
        if columns in df.columns:
            return df[columns].fillna("").astype(str)
        return pd.Series(df.index.astype(str), index=df.index)
    return df[columns].fillna("").astype(str).agg("|".join, axis=1)


def _line_role(name: object, is_boundary: bool, category: object = "", *, forced: bool = False) -> str:
    text = str(name or "").strip()
    category_s = str(category or "").strip()
    # 직원식사/직원호출/정산차액/선결제는 매출 라인이 아니다. main으로 두면
    # 닭 속성 분모에 들어가 완결률이 영원히 100%에 못 닿는다.
    if _SETTLEMENT_EXCLUDE_RE.search(text):
        return "제외"
    if _FEE_RE.search(text):
        return "fee"
    if _DISCOUNT_RE.search(text):
        return "discount"
    if forced and category_s not in _STRICT_NON_MAIN_CATEGORIES:
        return "main"
    if is_boundary and category_s in _SIDE_LINE_CATEGORIES:
        return "side"
    if category_s in _NON_MAIN_CATEGORIES:
        return "option"
    if category_s in _MAIN_CATEGORIES:
        return "main"
    return "main" if is_boundary else "option"


def _ensure_boundary_flags(df: pd.DataFrame, boundary: pd.Series) -> tuple[pd.Series, pd.Series]:
    out = boundary.reindex(df.index).fillna(False).astype(bool).copy()
    forced = pd.Series(False, index=df.index)
    if df.empty:
        return out, forced
    for _, group in df.groupby(_hierarchy_group_columns(df), sort=False):
        if out.loc[group.index].any():
            continue
        role_name = group["item_name"].fillna("").astype(str)
        candidate = group.index[~role_name.str.contains(_FEE_LIKE_RE, na=False)]
        idx = candidate[0] if len(candidate) else group.index[0]
        out.at[idx] = True
        forced.at[idx] = True
    return out, forced


def _ensure_boundary(df: pd.DataFrame, boundary: pd.Series) -> pd.Series:
    ensured, _ = _ensure_boundary_flags(df, boundary)
    return ensured


def _role_category_for_line(
    lookup: _ProductLookup,
    row: pd.Series,
    *,
    is_boundary: bool,
) -> str:
    category = lookup.category(row.get("source", ""), row.get("brand", ""), row.get("store", ""), row.get("item_id", ""))
    canonical = lookup.canonical_category(row.get("item_name", ""))
    name = str(row.get("item_name", "") or "").strip()
    if _is_forbidden_option_main_name(name):
        return "옵션"
    if is_boundary and _SIDE_NAME_RE.search(name) and canonical not in _MAIN_CATEGORIES:
        return "사이드"
    if canonical in _MAIN_CATEGORIES and category not in {"할인", "제외", "수수료"}:
        return canonical
    if is_boundary and category in _SIDE_LINE_CATEGORIES:
        return category
    return category


def _main_reconcile_key(row: pd.Series) -> str:
    for column in ("std_menu_name", "item_name"):
        key = _canonical_menu_key(row.get(column, ""))
        if key:
            return key
    return ""


def _can_force_order_main(row: pd.Series) -> bool:
    name = str(row.get("item_name", "") or "").strip()
    category = str(row.get("_role_category", "") or "").strip()
    price = pd.to_numeric(pd.Series([row.get("unit_price", "")]), errors="coerce").fillna(0).iloc[0]
    if price <= 0 or _FEE_LIKE_RE.search(name):
        return False
    context = " ".join(str(row.get(col, "") or "") for col in ("menu_name", "std_menu_name", "item_name"))
    delivery_one_person_boneless = (
        str(row.get("source", "") or "").strip() in _DELIVERY_MANUAL_SOURCES
        and _has_any_token(context, _CHICKEN_MENU_TOKENS)
        and "순살" in context
        and re.search(r"1\s*인|한그릇", context)
        and not _ROLE_ADDON_RE.search(name)
    )
    if delivery_one_person_boneless:
        return True
    if _is_forbidden_option_main_name(name):
        return False
    if category in _STRICT_NON_MAIN_CATEGORIES or category in _SIDE_LINE_CATEGORIES:
        return False
    return category in _MAIN_CATEGORIES or _has_any_token(context, _MAIN_CANDIDATE_TOKENS)


def _reconcile_order_mains(out: pd.DataFrame) -> pd.DataFrame:
    if out.empty or "order_id" not in out.columns or "line_role" not in out.columns:
        return out
    result = out.copy()
    result["_main_key"] = result.apply(_main_reconcile_key, axis=1)
    for _, group in result.groupby(_hierarchy_group_columns(result), sort=False):
        main_idx = list(group.index[group["line_role"].eq("main")])
        if not main_idx:
            candidates = group[group.apply(_can_force_order_main, axis=1)]
            if candidates.empty:
                continue
            price = pd.to_numeric(candidates.get("unit_price", pd.Series("", index=candidates.index)), errors="coerce").fillna(0)
            idx = price.idxmax()
            result.at[idx, "line_role"] = "main"
            result.at[idx, "parent_item_seq"] = result.at[idx, "item_seq"]
            result.at[idx, "attr_method"] = "forced_main"
            continue
        seen: dict[str, int] = {}
        first_main_idx = main_idx[0] if main_idx else None
        for idx in main_idx:
            result.at[idx, "parent_item_seq"] = result.at[idx, "item_seq"]
            key = str(result.at[idx, "_main_key"] or "").strip()
            name = str(result.at[idx, "item_name"] or "").strip()
            category = str(result.at[idx, "_role_category"] or "").strip()
            if first_main_idx is not None and idx != first_main_idx and (_ROLE_ADDON_RE.search(name) or _canonical_menu_key(name) == ""):
                result.at[idx, "line_role"] = "option"
                result.at[idx, "menu_seq"] = result.at[first_main_idx, "menu_seq"]
                result.at[idx, "parent_item_seq"] = result.at[first_main_idx, "item_seq"]
                result.at[idx, "menu_name"] = result.at[first_main_idx, "menu_name"]
                result.at[idx, "std_menu_name"] = result.at[first_main_idx, "std_menu_name"]
                result.at[idx, "attr_method"] = "duplicate_main_merged"
                continue
            if category in _SIDE_LINE_CATEGORIES or (_SIDE_NAME_RE.search(name) and not _has_any_token(name, _CHICKEN_MENU_TOKENS)):
                result.at[idx, "line_role"] = "side"
                result.at[idx, "attr_method"] = "side_demoted"
                continue
            price = pd.to_numeric(
                pd.Series([result.at[idx, "unit_price"]]), errors="coerce"
            ).fillna(0).iloc[0]
            # 단가가 붙은 중복은 한 영수증에 같은 메뉴가 2그릇 들어온 것이다. option으로
            # 접으면 매출이 메뉴키에서 빠져 재료추가/닭유형 키로 새고, 두 번째 그릇의
            # 닭 사용량도 사라진다. 단가 0인 에코(쿠팡 [후.참] 리뷰서비스)만 병합한다.
            if key and key in seen and price <= 0:
                parent_idx = seen[key]
                old_parent_seq = str(result.at[idx, "item_seq"])
                result.at[idx, "line_role"] = "option"
                result.at[idx, "menu_seq"] = result.at[parent_idx, "menu_seq"]
                result.at[idx, "parent_item_seq"] = result.at[parent_idx, "item_seq"]
                result.at[idx, "menu_name"] = result.at[parent_idx, "menu_name"]
                result.at[idx, "std_menu_name"] = result.at[parent_idx, "std_menu_name"]
                result.at[idx, "attr_method"] = "duplicate_main_merged"
                child_idx = group.index[
                    result.loc[group.index, "parent_item_seq"].fillna("").astype(str).eq(old_parent_seq)
                    & (group.index != idx)
                ]
                if len(child_idx):
                    result.loc[child_idx, "menu_seq"] = result.at[parent_idx, "menu_seq"]
                    result.loc[child_idx, "parent_item_seq"] = result.at[parent_idx, "item_seq"]
                    result.loc[child_idx, "menu_name"] = result.at[parent_idx, "menu_name"]
                    result.loc[child_idx, "std_menu_name"] = result.at[parent_idx, "std_menu_name"]
                continue
            if key:
                # 유가 중복이 폴스루해도 첫 main이 병합 부모로 남아야 한다.
                seen.setdefault(key, idx)
    return result.drop(columns=["_main_key"])


def _attach_common_fields(out: pd.DataFrame) -> pd.DataFrame:
    store_map = _load_store_map()
    out["store"] = TARGET_STORE
    out["담당자"] = _lookup_store_meta(store_map, TARGET_STORE, "담당자")
    out["region"] = _lookup_store_meta(store_map, TARGET_STORE, "region")
    out["실오픈일"] = _lookup_store_meta(store_map, TARGET_STORE, "실오픈일")
    out["collected_at"] = pendulum.now("Asia/Seoul").isoformat()
    return out


def _derive_posfeed_revenue(df: pd.DataFrame) -> pd.Series:
    order_path = _clean_nan_series(df["주문경로"])
    order_type = _clean_nan_series(df.get("주문타입", pd.Series("", index=df.index)))
    total_amount = _to_int_series(df.get("총 주문금액", pd.Series(0, index=df.index)))
    delivery_fee = _to_int_series(df.get("배달비", pd.Series(0, index=df.index)))
    discount_amount = _to_int_series(df.get("할인", pd.Series(0, index=df.index)))
    paid_amount = _to_int_series(df.get("결제금액", pd.Series(0, index=df.index)))

    revenue = pd.Series(paid_amount, index=df.index)
    is_ddangyo = order_path.str.contains("땡겨요", regex=False)
    is_pack = order_type.str.contains("포장", regex=False)
    revenue = pd.Series(np.where(is_ddangyo & is_pack, total_amount - 2000, np.where(is_ddangyo, total_amount, revenue)), index=df.index)
    revenue = pd.Series(np.where(order_path.eq("배민1"), paid_amount - delivery_fee, revenue), index=df.index)
    is_coupang = order_path.eq("쿠팡이츠")
    coupang_revenue = np.where(total_amount.eq(discount_amount), 0, np.where(delivery_fee.gt(0), total_amount - delivery_fee, total_amount))
    revenue = pd.Series(np.where(is_coupang, coupang_revenue, revenue), index=df.index)
    if "주문자 구주소" in df.columns:
        old_addr = _clean_nan_series(df["주문자 구주소"])
        is_yogibae = old_addr.str.contains("요기요 요기배달", regex=False)
    else:
        is_yogibae = pd.Series(False, index=df.index)
    is_yogiyo = order_path.eq("요기요")
    yogiyo_revenue = np.where(is_yogibae & delivery_fee.gt(0), total_amount - delivery_fee, total_amount)
    return pd.Series(np.where(is_yogiyo, yogiyo_revenue, revenue), index=df.index).fillna(0).astype(int)


def _build_posfeed(ym: str, lookup: _ProductLookup) -> pd.DataFrame:
    order_files = _find_partition_files(ANALYTICS_DB / "posfeed_sales", ym, "posfeed_orders.csv")
    item_files = _find_partition_files(ANALYTICS_DB / "posfeed_sales_detail", ym, "posfeed_order_item.csv")
    if not order_files or not item_files:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    orders = pd.concat([_read_csv(path).assign(_src_path=str(path)) for path in order_files], ignore_index=True)
    items = pd.concat([_read_csv(path).assign(_src_path=str(path)) for path in item_files], ignore_index=True)
    orders = orders[_clean_nan_series(orders["등록날짜"]).str.startswith(ym)].copy()
    if orders.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    for col in ["총 주문금액", "배달비", "할인", "결제금액"]:
        orders[col] = _to_int_series(orders.get(col, pd.Series(0, index=orders.index)))
    orders["포스피드_매출"] = _derive_posfeed_revenue(orders)
    orders["주문코드_key"] = pd.to_numeric(_clean_nan_series(orders["주문 코드"]), errors="coerce").fillna(0).astype(int).astype(str)
    items["주문코드_key"] = pd.to_numeric(_clean_nan_series(items["주문코드"]), errors="coerce").fillna(0).astype(int).astype(str)
    for col in ["단품가격", "합계", "수량"]:
        items[col] = _to_int_series(items.get(col, pd.Series(0, index=items.index)))
    dfs = pd.merge(orders, items[["주문코드_key", "상품명", "수량", "단품가격", "합계"]], on="주문코드_key", how="left")
    dfs = dfs[_clean_nan_series(dfs["상품명"]).ne("")].copy()
    if dfs.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    raw_name = _clean_nan_series(dfs["상품명"])
    numeric_option = raw_name.str.startswith("'+") & raw_name.str.lstrip("'+ ").str.fullmatch(r"\d+").fillna(False)
    if numeric_option.any():
        main_mask = ~raw_name.str.startswith("'") & raw_name.ne("배달비")
        main_idx_by_order = dfs[main_mask].sort_values("단품가격", ascending=False).groupby("주문 코드").apply(lambda g: g.index[0])
        for opt_idx in dfs.index[numeric_option]:
            order_code = dfs.at[opt_idx, "주문 코드"]
            if order_code in main_idx_by_order.index:
                main_idx = main_idx_by_order.loc[order_code]
                dfs.at[main_idx, "단품가격"] += dfs.at[opt_idx, "단품가격"]
                dfs.at[main_idx, "합계"] += dfs.at[opt_idx, "합계"]
            else:
                numeric_option.at[opt_idx] = False
        dfs = dfs[~numeric_option].copy()
        raw_name = _clean_nan_series(dfs["상품명"])

    order_total = dfs.groupby("주문 코드")["합계"].transform("sum")
    order_revenue = dfs.groupby("주문 코드")["포스피드_매출"].transform("first")
    dfs["total_price"] = np.where(order_total.eq(0), 0, np.where(order_revenue.eq(order_total), dfs["합계"], (dfs["합계"] / order_total.where(order_total.ne(0), 1)) * order_revenue))
    dfs["total_price"] = pd.Series(dfs["total_price"], index=dfs.index).round().astype(int)
    dfs["discount_amount"] = (dfs["합계"] - dfs["total_price"]).clip(lower=0)
    sale_type = _clean_nan_series(dfs["주문상태"]).map(lambda v: "정상" if v in _COMPLETE_POSFEED_STATUSES else "취소")
    dfs.loc[sale_type.eq("취소"), "total_price"] = -dfs.loc[sale_type.eq("취소"), "total_price"].abs()

    out = pd.DataFrame(index=dfs.index)
    out["sale_date"] = _clean_nan_series(dfs["등록날짜"])
    out["ym"] = out["sale_date"].str[:7]
    out["source"] = POSFEED_SOURCE
    out["brand"] = _clean_nan_series(dfs.get("brand_x", dfs.get("brand", pd.Series("", index=dfs.index))))
    out["platform"] = _clean_nan_series(dfs["주문경로"]).map(lambda v: _PLATFORM_MAP.get(v, v))
    out["order_type"] = _clean_nan_series(dfs["주문타입"]).map(lambda v: "배달_포장" if "포장" in v else "배달")
    out["order_id"] = _clean_nan_series(dfs["외부 주문 번호"])
    no_id = out["order_id"].eq("")
    out.loc[no_id, "order_id"] = "tmp_" + TARGET_STORE + "_" + _clean_nan_series(dfs.loc[no_id, "주문등록 시각"])
    out["order_time"] = dfs["주문등록 시각"].apply(_normalize_time)
    out["item_name"] = raw_name.map(_strip_marker_name)
    out["qty"] = dfs["수량"].astype(str)
    out["unit_price"] = dfs["단품가격"].astype(int)
    out["total_price"] = dfs["total_price"].astype(int)
    out["discount_amount"] = dfs["discount_amount"].astype(int)
    out["sale_type"] = sale_type
    out["item_seq"] = out.groupby("order_id").cumcount().add(1).astype(int).astype(str)
    out["item_id"] = [
        lookup.item_id(POSFEED_SOURCE, brand, TARGET_STORE, name, price)
        for brand, name, price in zip(out["brand"], out["item_name"], out["unit_price"])
    ]
    out = _attach_common_fields(out)

    boundary = (~raw_name.str.startswith("'")) & (~raw_name.eq("배달비"))
    out["_raw_item_name"] = raw_name
    out["_boundary"], out["_forced_boundary"] = _ensure_boundary_flags(out, boundary)
    out["_menu_name_current"] = out["order_id"].map(out.loc[out.groupby("order_id")["unit_price"].rank(method="first", ascending=False).eq(1)].set_index("order_id")["item_name"].to_dict()).fillna("")
    out["order_cnt"] = 0
    out.loc[out.groupby("order_id")["unit_price"].rank(method="first", ascending=False).eq(1), "order_cnt"] = out["sale_type"].map(lambda v: -1 if v == "취소" else 1)
    out["_pk"] = _make_unified_pk(out)
    return _finalize_hierarchy(out, out["_boundary"], "marker", lookup)


def _parse_baemin_datetime(value: object) -> tuple[str, str]:
    text = str(value or "").strip()
    match = re.search(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\..*?(오전|오후)\s*(\d{1,2}):(\d{2}):(\d{2})", text)
    if not match:
        return "", ""
    year, month, day, ampm, hour, minute, second = match.groups()
    h = int(hour)
    if ampm == "오후" and h != 12:
        h += 12
    if ampm == "오전" and h == 12:
        h = 0
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}", f"{h:02d}:{int(minute):02d}:{int(second):02d}"


def _parse_baemin_amount(s: pd.Series) -> pd.Series:
    v = s.astype(str).str.replace(",", "", regex=False).str.strip()
    extracted = v.str.extract(r"(-?\d+)", expand=False)
    return pd.to_numeric(extracted, errors="coerce").fillna(0).astype(int)


def _build_baemin(ym: str, lookup: _ProductLookup) -> pd.DataFrame:
    files = []
    for month in (ym, _next_ym(ym)):
        files.extend(_find_partition_files(BAEMIN_ORDERS_DB, month, f"orders_{month}.parquet"))
    if not files:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    frames = [pd.read_parquet(path).assign(_src_path=str(path), _brand=_path_part(path, "brand=")) for path in dict.fromkeys(files)]
    df = pd.concat(frames, ignore_index=True)
    parsed = df["주문시각"].map(_parse_baemin_datetime)
    df["sale_date"] = parsed.map(lambda x: x[0])
    df["order_time"] = parsed.map(lambda x: x[1])
    df = df[df["sale_date"].str.startswith(ym) & _clean_nan_series(df["주문상태"]).eq("배달완료")].copy()
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    key_cols = ["_src_path", "주문시각", "주문번호", "주문상태", "수령방법", "주문내역", "주문옵션상세", "주문수량", "주문옵션금액", "결제금액"]
    collected_ts = pd.to_datetime(df.get("collected_at", pd.Series("", index=df.index)), utc=True, errors="coerce")
    if collected_ts.notna().any():
        order_key = _clean_nan_series(df["주문번호"])
        rank_key = pd.DataFrame(
            {"_order": order_key, "_ts": collected_ts, "_src": df["_src_path"].astype(str)},
            index=df.index,
        )
        latest = (
            rank_key[rank_key["_ts"].notna()]
            .sort_values(["_order", "_ts", "_src"], kind="mergesort")
            .groupby("_order", sort=False)
            .tail(1)
            .set_index("_order")
        )
        target_ts = order_key.map(latest["_ts"])
        target_src = order_key.map(latest["_src"])
        keep_latest = target_ts.isna() | (collected_ts.eq(target_ts) & df["_src_path"].astype(str).eq(target_src))
        df = df[keep_latest].reset_index(drop=True)
    else:
        df = df.drop_duplicates(subset=[c for c in key_cols if c in df.columns], keep="last").reset_index(drop=True)

    out = pd.DataFrame(index=df.index)
    out["sale_date"] = df["sale_date"]
    out["ym"] = out["sale_date"].str[:7]
    out["source"] = BAEMIN_SOURCE
    out["brand"] = _clean_nan_series(df["_brand"])
    out["platform"] = BAEMIN_PLATFORM
    out["order_type"] = _clean_nan_series(df["수령방법"]).map(lambda v: "배달_포장" if v == "포장" else "배달")
    out["order_id"] = _clean_nan_series(df["주문번호"])
    out["order_time"] = df["order_time"]
    order_summary = _clean_nan_series(df["주문내역"])
    current_menu = order_summary.str.replace(r"\s*외\s*\d+건$", "", regex=True).str.strip()
    extra_cnt = order_summary.str.extract(r"외\s*(\d+)건$")[0]
    expected_menus = pd.to_numeric(extra_cnt, errors="coerce").fillna(0).astype(int) + 1
    menu_vocab = set(current_menu[current_menu.ne("")].unique())
    option_name = _clean_nan_series(df["주문옵션상세"])
    is_price_name = option_name.str.fullmatch(r"[\d,]+").fillna(False)
    out["item_name"] = option_name.mask(is_price_name, current_menu)
    out["menu_name"] = current_menu
    out["qty"] = _to_int_series(df.get("주문수량", pd.Series(1, index=df.index))).replace(0, 1).astype(str)
    out["unit_price"] = _to_int_series(df.get("주문옵션금액", pd.Series(0, index=df.index)))
    out["item_seq"] = out.groupby("order_id").cumcount().add(1).astype(int).astype(str)
    item_total = out["unit_price"] * pd.to_numeric(out["qty"], errors="coerce").fillna(1).astype(int)
    order_item_sum = item_total.groupby(out["order_id"]).transform("sum")
    order_amount = _parse_baemin_amount(df["결제금액"]).groupby(out["order_id"]).transform("max")
    product_amount = _parse_baemin_amount(
        df.get("상품금액", pd.Series(0, index=df.index))
    ).groupby(out["order_id"]).transform("max")
    gross_amount = product_amount.where(product_amount.gt(0), order_item_sum)
    gross_amount = gross_amount.where(gross_amount.gt(0), order_amount)
    out["total_price"] = ((item_total / order_item_sum.replace(0, 1)) * gross_amount).round().astype(int)
    residual = gross_amount - out.groupby("order_id")["total_price"].transform("sum")
    idx_max = item_total.groupby(out["order_id"]).idxmax()
    out.loc[idx_max, "total_price"] = out.loc[idx_max, "total_price"] + residual.loc[idx_max]
    paid_total = ((item_total / order_item_sum.replace(0, 1)) * order_amount).round().astype(int)
    paid_residual = order_amount - paid_total.groupby(out["order_id"]).transform("sum")
    paid_total.loc[idx_max] = paid_total.loc[idx_max] + paid_residual.loc[idx_max]
    out["discount_amount"] = (out["total_price"] - paid_total).clip(lower=0).astype(int)
    order_status = _clean_nan_series(df.get("주문상태", pd.Series("", index=df.index)))
    out["sale_type"] = order_status.map(lambda value: "취소" if "취소" in value or "거절" in value else "정상")
    out["item_id"] = [
        lookup.item_id(BAEMIN_SOURCE, brand, TARGET_STORE, name, price)
        for brand, name, price in zip(out["brand"], out["item_name"], out["unit_price"])
    ]
    out = _attach_common_fields(out)
    out["_raw_item_name"] = option_name
    out["_menu_name_current"] = current_menu
    boundary = pd.Series([
        lookup.is_main(BAEMIN_SOURCE, brand, TARGET_STORE, item_id, name, price)
        for brand, item_id, name, price in zip(out["brand"], out["item_id"], out["item_name"], out["unit_price"])
    ], index=out.index)
    standalone_candidate = pd.Series([
        lookup.is_standalone_candidate(BAEMIN_SOURCE, brand, TARGET_STORE, item_id, name, price, menu_vocab)
        for brand, item_id, name, price in zip(out["brand"], out["item_id"], out["item_name"], out["unit_price"])
    ], index=out.index)
    for _, group in out.groupby("order_id", sort=False):
        expected = int(expected_menus.loc[group.index].max())
        need = expected - max(int(boundary.loc[group.index].sum()), 1)
        if need <= 0:
            continue
        ordered_index = group.assign(_sort_seq=pd.to_numeric(group["item_seq"], errors="coerce")).sort_values(
            ["_sort_seq", "item_seq"],
            kind="mergesort",
        ).index
        for idx in reversed(list(ordered_index)):
            if need <= 0:
                break
            if not boundary.at[idx] and standalone_candidate.at[idx]:
                boundary.at[idx] = True
                need -= 1
    out["_boundary"], out["_forced_boundary"] = _ensure_boundary_flags(out, boundary)
    out["order_cnt"] = 0
    out.loc[out.groupby("order_id").head(1).index, "order_cnt"] = 1
    out["_pk"] = _make_unified_pk(out)
    alias_rows = out.loc[expected_menus.eq(1)].groupby("order_id", sort=False).head(1)
    baemin_menu_alias: dict[tuple[str, int], str] = {}
    for idx in alias_rows.index:
        line_name = str(out.at[idx, "item_name"]).strip()
        head = str(current_menu.at[idx]).strip()
        if not line_name or not head or _normalize_item_key(line_name) == _normalize_item_key(head):
            continue
        baemin_menu_alias.setdefault((_normalize_item_key(line_name), int(out.at[idx, "unit_price"])), head)
    return _finalize_hierarchy(out, out["_boundary"], "master_main", lookup, name_alias=baemin_menu_alias)


def _parse_coupang_date(value: object) -> str:
    text = str(value or "").strip()
    match = re.search(r"(\d{4})\.(\d{1,2})\.(\d{1,2})", text)
    if not match:
        return ""
    return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"


def _parse_coupang_time(value: object) -> str:
    text = str(value or "").strip()
    match = re.search(r"\b(\d{1,2}):(\d{2})\b", text)
    if not match:
        return ""
    return f"{int(match.group(1)):02d}:{int(match.group(2)):02d}:00"


def _coupang_item_id_name(parent_name: object, option_name: object, is_priced_menu: object) -> str:
    parent = str(parent_name or "").strip()
    option = str(option_name or "").strip()
    if not bool(is_priced_menu):
        return option or parent
    if not option or option in {"기본", "기본맛"}:
        return parent or option
    return option


def _redistribute_coupang_multi_main_totals(frame: pd.DataFrame) -> pd.DataFrame:
    """쿠팡 다중 main 주문에서 첫 main에 몰린 주문 총액을 main별 단가로 되돌린다."""
    if frame.empty or not {"order_id", "line_role", "unit_price", "qty", "total_price", "sale_type"}.issubset(frame.columns):
        return frame
    out = frame.copy()
    group_cols = [col for col in ["source", "brand", "store", "sale_date", "order_id"] if col in out.columns]
    if not group_cols:
        group_cols = ["order_id"]
    role = out["line_role"].fillna("").astype(str).str.strip()
    for _, group in out.groupby(group_cols, dropna=False, sort=False):
        main = group[role.loc[group.index].eq("main")]
        if len(main) < 2:
            continue
        if out.loc[group.index, "sale_type"].fillna("").astype(str).str.strip().eq("취소").any():
            continue
        unit = pd.to_numeric(main["unit_price"], errors="coerce").fillna(0)
        qty = pd.to_numeric(main["qty"], errors="coerce").fillna(0)
        expected = (unit * qty).round()
        if expected.le(0).any():
            continue
        current_total = pd.to_numeric(group["total_price"], errors="coerce").fillna(0)
        non_main_total = current_total.loc[group.index.difference(main.index)].abs().sum()
        if float(non_main_total) > 1:
            continue
        if abs(float(expected.sum()) - float(current_total.sum())) > 1:
            continue
        out.loc[main.index, "total_price"] = expected.astype(int)
        if "discount_amount" in out.columns:
            out.loc[main.index, "discount_amount"] = 0
    return out


def _build_coupang(ym: str, lookup: _ProductLookup) -> pd.DataFrame:
    files = []
    for month in (ym, _next_ym(ym)):
        files.extend(_find_partition_files(COUPANG_ORDERS_DB, month, f"orders_{month}.parquet"))
    if not files:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    frames = [pd.read_parquet(path).assign(_src_path=str(path), _brand=_path_part(path, "brand=")) for path in dict.fromkeys(files)]
    df = pd.concat(frames, ignore_index=True)
    df["sale_date"] = df["order_date"].map(_parse_coupang_date)
    df["order_time"] = df["order_date"].map(_parse_coupang_time)
    df = df[df["sale_date"].str.startswith(ym)].copy()
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    key_cols = ["order_date", "order_id", "delivery_type", "order_summary", "menu_name", "menu_qty", "menu_price", "menu_options"]
    df = df.drop_duplicates(subset=[c for c in key_cols if c in df.columns], keep="last").reset_index(drop=True)

    out = pd.DataFrame(index=df.index)
    out["sale_date"] = df["sale_date"]
    out["ym"] = out["sale_date"].str[:7]
    out["source"] = COUPANG_SOURCE
    out["brand"] = _clean_nan_series(df["_brand"])
    out["platform"] = COUPANG_PLATFORM
    out["order_type"] = _clean_nan_series(df.get("delivery_type", pd.Series("", index=df.index))).map(lambda v: "배달_포장" if "포장" in v else "배달")
    out["order_id"] = _clean_nan_series(df["order_id"])
    out["order_time"] = df["order_time"]
    current_menu = _clean_nan_series(df["order_summary"]).str.replace(r"\s*외\s*\d+건$", "", regex=True).str.strip()
    parent_name = _clean_nan_series(df["menu_name"])
    option_name = _clean_nan_series(df["menu_options"])
    menu_price = _clean_nan_series(df.get("menu_price", pd.Series("", index=df.index)))
    priced_menu = menu_price.ne("") & menu_price.ne("nan")
    item_name = option_name.mask(priced_menu | option_name.eq(""), parent_name)
    out["item_name"] = item_name
    out["menu_name"] = current_menu
    out["item_seq"] = out.groupby("order_id").cumcount().add(1).astype(int).astype(str)
    out["qty"] = pd.to_numeric(df.get("menu_qty", 1), errors="coerce").fillna(1).astype(int).astype(str)
    out["unit_price"] = pd.to_numeric(df.get("menu_price", 0), errors="coerce").fillna(0).astype(int)
    out["total_price"] = 0
    out["discount_amount"] = 0
    out["sale_type"] = _clean_nan_series(df.get("is_cancelled", pd.Series("", index=df.index))).str.upper().map(lambda v: "취소" if v == "Y" else "정상")
    id_lookup_name = pd.Series([
        _coupang_item_id_name(parent, option, priced)
        for parent, option, priced in zip(parent_name, option_name, priced_menu)
    ], index=out.index)
    out["item_id"] = [
        lookup.item_id(COUPANG_SOURCE, brand, TARGET_STORE, name, price)
        for brand, name, price in zip(out["brand"], id_lookup_name, out["unit_price"])
    ]
    maechul = pd.to_numeric(_clean_nan_series(df.get("매출액", pd.Series("", index=df.index))).str.replace(",", "", regex=False), errors="coerce")
    fallback_amount = pd.to_numeric(df.get("total_price", 0), errors="coerce").fillna(0)
    out["order_cnt"] = 0
    for order_id, group in out.groupby("order_id", sort=False):
        idx = group.index[0]
        is_cancel = bool(out.loc[group.index, "sale_type"].eq("취소").any())
        order_sales = maechul.loc[group.index]
        if order_sales.notna().any():
            amount = order_sales.fillna(0).sum()
        elif is_cancel:
            amount = 0
        else:
            amount = fallback_amount.loc[group.index].iloc[0]
        out.at[idx, "total_price"] = int(amount)
        out.at[idx, "order_cnt"] = 0 if is_cancel else 1
    out = _attach_common_fields(out)
    out["_raw_item_name"] = option_name.mask(option_name.eq(""), parent_name)
    out["_menu_name_current"] = current_menu
    out["_hier_name"] = parent_name.mask(parent_name.eq(""), out["item_name"])
    boundary = priced_menu
    boundary |= parent_name.ne(parent_name.groupby(out["order_id"]).shift()) & parent_name.ne("")
    out["_boundary"], out["_forced_boundary"] = _ensure_boundary_flags(out, boundary)
    out["_pk"] = _make_unified_pk(out)
    finalized = _finalize_hierarchy(out, out["_boundary"], "raw_parent", lookup, name_col="_hier_name")
    return _redistribute_coupang_multi_main_totals(finalized)


def _build_okpos(ym: str, lookup: _ProductLookup) -> pd.DataFrame:
    order_files = _find_partition_files(RAW_OKPOS_SALES, ym, "okpos_order.csv")
    item_files = _find_partition_files(RAW_OKPOS_SALES, ym, "okpos_order_item.csv")
    if not item_files:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    if order_files:
        from modules.transform.pipelines.db.DB_UnifiedSales_okpos import _transform_okpos_df

        order_df = pd.concat([_read_csv(path).assign(_src_path=str(path)) for path in order_files], ignore_index=True)
        item_df = pd.concat([_read_csv(path).assign(_src_path=str(path)) for path in item_files], ignore_index=True)
        order_df = order_df[_clean_nan_series(order_df["sale_date"]).str.startswith(ym)].copy()
        item_df = item_df[_clean_nan_series(item_df["sale_date"]).str.startswith(ym)].copy()
        if order_df.empty or item_df.empty:
            return pd.DataFrame(columns=OUTPUT_COLUMNS)
        out = _transform_okpos_df(order_df, item_df, amount_basis="gross")
        out = out[_clean_nan_series(out["store"]).eq(TARGET_STORE) & _clean_nan_series(out["ym"]).eq(ym)].copy()
        if out.empty:
            return pd.DataFrame(columns=OUTPUT_COLUMNS)
        out["_order_key"] = out["sale_date"].astype(str) + "|" + out["store"].astype(str) + "|" + out["order_id"].astype(str)
    else:
        df = pd.concat([_read_csv(path).assign(_src_path=str(path), _brand=_path_part(path, "brand=")) for path in item_files], ignore_index=True)
        df = df[_clean_nan_series(df["sale_date"]).str.startswith(ym)].copy()
        if df.empty:
            return pd.DataFrame(columns=OUTPUT_COLUMNS)
        df = df.reset_index(drop=True)
        out = pd.DataFrame(index=df.index)
        out["sale_date"] = _clean_nan_series(df["sale_date"])
        out["ym"] = out["sale_date"].str[:7]
        out["source"] = OKPOS_SOURCE
        out["brand"] = _clean_nan_series(df["_brand"]).mask(lambda s: s.eq(""), "도리당")
        out["platform"] = ""
        table = _clean_nan_series(df.get("테이블명", pd.Series("", index=df.index)))
        out["order_type"] = np.where(table.str.contains("포장", regex=False), "홀_포장", np.where(table.str.contains("배달", regex=False), "배달", "홀_테이블"))
        out["order_time"] = df.get("최초주문", df.get("결제시각", pd.Series("", index=df.index))).apply(_normalize_time)
        pos = _clean_nan_series(df.get("포스번호", pd.Series("", index=df.index)))
        receipt = _clean_nan_series(df.get("영수증번호", pd.Series("", index=df.index)))
        out["order_id"] = TARGET_STORE + "_" + pos + "-" + receipt + "_" + out["order_time"].mask(out["order_time"].eq(""), "00:00:00")
        out["item_name"] = _clean_nan_series(df["상품명"])
        out["qty"] = pd.to_numeric(df.get("수량", 0), errors="coerce").fillna(0).astype(int).astype(str)
        gross_price = _to_int_series(df.get("총매출액", pd.Series(0, index=df.index)))
        if "총매출액" not in df.columns:
            logger.warning("OKPOS 총매출액 컬럼 없음: 메뉴계층 fallback=실매출액")
            gross_price = _to_int_series(df.get("실매출액", pd.Series(0, index=df.index)))
        out["total_price"] = gross_price
        out["discount_amount"] = _to_int_series(df.get("할인액", pd.Series(0, index=df.index)))
        qty_num = pd.to_numeric(out["qty"], errors="coerce").fillna(0).astype(int)
        safe_qty = qty_num.where(qty_num.ne(0), 1)
        out["unit_price"] = (out["total_price"] / safe_qty).where(qty_num.gt(0), out["total_price"]).round().astype(int)
        out["sale_type"] = _clean_nan_series(df.get("구분", pd.Series("", index=df.index))).map(lambda v: "취소" if v == "반품" else "정상")
        out.loc[out["sale_type"].eq("취소"), "total_price"] = -out.loc[out["sale_type"].eq("취소"), "total_price"].abs()
        out.loc[out["sale_type"].eq("취소"), "discount_amount"] = -out.loc[out["sale_type"].eq("취소"), "discount_amount"].abs()
        pos = _clean_nan_series(df.get("포스번호", pd.Series("", index=df.index)))
        receipt = _clean_nan_series(df.get("영수증번호", pd.Series("", index=df.index)))
        out["_order_key"] = out["sale_date"] + "|" + TARGET_STORE + "|" + pos + "|" + receipt
        out["item_seq"] = out.groupby("_order_key").cumcount().add(1).astype(int).astype(str)
        out["item_id"] = _clean_nan_series(df.get("상품코드", pd.Series("", index=df.index)))
        out["item_id"] = [
            item_id if item_id else lookup.item_id(OKPOS_SOURCE, brand, TARGET_STORE, name, price)
            for item_id, brand, name, price in zip(out["item_id"], out["brand"], out["item_name"], out["unit_price"])
        ]
        out = _attach_common_fields(out)
        out["_pk"] = _make_unified_pk(out)
    out["_raw_item_name"] = out["item_name"]
    out["_menu_name_current"] = out["menu_name"] if "menu_name" in out.columns else out["order_id"].map(out.loc[out.groupby("_order_key")["unit_price"].rank(method="first", ascending=False).eq(1)].set_index("order_id")["item_name"].to_dict()).fillna("")
    boundary = pd.Series([
        lookup.is_main(OKPOS_SOURCE, brand, TARGET_STORE, item_id, name, price)
        for brand, item_id, name, price in zip(out["brand"], out["item_id"], out["item_name"], out["unit_price"])
    ], index=out.index)
    if not boundary.any():
        threshold = max(2000.0, float(pd.to_numeric(out["unit_price"], errors="coerce").fillna(0).max()) * 0.25)
        boundary = pd.to_numeric(out["unit_price"], errors="coerce").fillna(0).ge(threshold) & ~out["item_name"].str.contains(_OPTION_LIKE_RE, na=False) & ~out["item_name"].str.contains(_FEE_LIKE_RE, na=False)
        attr_method = "price_heuristic"
    else:
        attr_method = "master_main"
    out["_boundary"], out["_forced_boundary"] = _ensure_boundary_flags(out, boundary)
    out["order_cnt"] = 0
    last_idx = out.groupby("_order_key").tail(1).index
    out.loc[last_idx, "order_cnt"] = out.loc[last_idx, "sale_type"].map(lambda v: -1 if v == "취소" else 1)
    return _finalize_hierarchy(out, out["_boundary"], attr_method, lookup)


def _finalize_hierarchy(
    out: pd.DataFrame,
    boundary: pd.Series,
    attr_method: str,
    lookup: _ProductLookup,
    *,
    name_col: str = "item_name",
    name_alias: dict[tuple[str, int], str] | None = None,
) -> pd.DataFrame:
    out = assign_menu_hierarchy(
        out,
        boundary,
        order_col=_hierarchy_group_columns(out),
        sort_cols=["item_seq"],
        name_col=name_col,
        attr_method=attr_method,
    )
    boundary_values = boundary.reindex(out.index).fillna(False)
    forced_values = out.get("_forced_boundary", pd.Series(False, index=out.index)).reindex(out.index).fillna(False).astype(bool)
    product_categories = [
        _role_category_for_line(lookup, row, is_boundary=bool(boundary_values.at[idx]))
        for idx, row in out.iterrows()
    ]
    out["_role_category"] = product_categories
    if attr_method == "raw_parent":
        role_categories = [
            category if category in _SIDE_LINE_CATEGORIES or category in _MAIN_CATEGORIES else ""
            for category in product_categories
        ]
    else:
        role_categories = product_categories
    out["line_role"] = [
        _line_role(name, is_boundary, category, forced=forced)
        for name, is_boundary, category, forced in zip(out["item_name"], boundary_values, role_categories, forced_values)
    ]
    canonical_main = pd.Series(product_categories, index=out.index).isin(_MAIN_CATEGORIES) & boundary_values
    out.loc[canonical_main & out["attr_method"].ne("raw_parent"), "attr_method"] = "canonical_main"
    out.loc[out["line_role"].eq("side"), "attr_method"] = "side_demoted"
    out.loc[forced_values & out["line_role"].eq("main"), "attr_method"] = "forced_main"
    hierarchy_key = _hierarchy_key_series(out)
    parent_item_id_by_key = dict(zip(zip(hierarchy_key, out["item_seq"]), out["item_id"]))
    parent_name_by_key = dict(zip(zip(hierarchy_key, out["item_seq"]), out[name_col]))
    parent_price_by_key = dict(zip(zip(hierarchy_key, out["item_seq"]), out["unit_price"]))
    std_values = []
    menu_values = []
    for _, row in out.iterrows():
        parent_seq = str(row.get("parent_item_seq", "")).strip()
        row_key = hierarchy_key.at[row.name]
        parent_item_id = parent_item_id_by_key.get((row_key, parent_seq), row.get("item_id", ""))
        parent_name = parent_name_by_key.get((row_key, parent_seq), row.get(name_col, row.get("item_name", "")))
        parent_price = parent_price_by_key.get((row_key, parent_seq), row.get("unit_price", ""))
        std_values.append(lookup.std_name(row["source"], row["brand"], row["store"], parent_item_id, parent_name))
        menu_values.append(
            lookup.menu_display_name(
                row["source"],
                row["brand"],
                row["store"],
                parent_item_id,
                parent_name,
                parent_price,
                name_alias,
            )
        )
    out["std_menu_name"] = [_canonical_std_menu_name(value) for value in std_values]
    out["menu_name"] = pd.Series(menu_values, index=out.index).where(
        pd.Series(menu_values, index=out.index).ne(""),
        out.get("menu_name", ""),
    )
    out = _reconcile_order_mains(out)
    return out.drop(columns=[col for col in ["_role_category", "_forced_boundary"] if col in out.columns])


def _all_source_orders(ym: str) -> pd.DataFrame:
    assert_no_model_classification_dependencies()
    ym = _ensure_ym(ym)
    lookup = _ProductLookup()
    frames = [
        _build_posfeed(ym, lookup),
        _build_baemin(ym, lookup),
        _build_coupang(ym, lookup),
        _build_okpos(ym, lookup),
    ]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    out = pd.concat(frames, ignore_index=True)
    out["_canonical"] = True
    return _apply_canonical_filter(out)


def _repair_duplicate_main_menu_seq(left_joined: pd.DataFrame) -> pd.DataFrame:
    if left_joined.empty:
        return left_joined
    out = _ensure_order_group_columns(left_joined)
    role = out.get("line_role", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    repaired = 0
    order_keys = [col for col in ["source", "brand", "store", "sale_date", "order_id"] if col in out.columns]
    for order_key, order_group in out.groupby(order_keys, dropna=False, sort=False):
        max_seq = pd.to_numeric(order_group.get("menu_seq", pd.Series("", index=order_group.index)), errors="coerce").fillna(0).max()
        next_seq = int(max_seq) + 1
        for menu_seq, seq_group in order_group.groupby("menu_seq", dropna=False, sort=False):
            main_idx = list(seq_group.index[role.loc[seq_group.index].eq("main")])
            if len(main_idx) <= 1:
                continue
            ordered = seq_group.assign(_sort_seq=pd.to_numeric(seq_group["item_seq"], errors="coerce")).sort_values(
                ["_sort_seq", "item_seq"],
                kind="mergesort",
            )
            current_seq = str(menu_seq)
            current_parent = str(out.at[main_idx[0], "item_seq"])
            split_map = {main_idx[0]: current_seq}
            for idx in ordered.index:
                if idx in main_idx:
                    if idx != main_idx[0]:
                        current_seq = str(next_seq)
                        next_seq += 1
                        current_parent = str(out.at[idx, "item_seq"])
                        out.at[idx, "menu_seq"] = current_seq
                        out.at[idx, "parent_item_seq"] = current_parent
                        split_map[idx] = current_seq
                        repaired += 1
                    else:
                        out.at[idx, "parent_item_seq"] = current_parent
                    continue
                if role.at[idx] == "option":
                    out.at[idx, "menu_seq"] = current_seq
                    out.at[idx, "parent_item_seq"] = current_parent
        if repaired:
            logger.info("중복 main menu_seq 보정: order=%s | 누적 %d행", order_key, repaired)
    return out


def _repair_option_parent_to_main(left_joined: pd.DataFrame) -> pd.DataFrame:
    if left_joined.empty or not {"line_role", "item_seq", "parent_item_seq", "menu_seq"}.issubset(left_joined.columns):
        return left_joined
    out = left_joined.copy()
    order_keys = [col for col in ["source", "brand", "store", "sale_date", "order_id"] if col in out.columns]
    role = out.get("line_role", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    kind = out.get("option_kind", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    repaired = 0
    demoted = 0

    for _, order_group in out.groupby(order_keys, dropna=False, sort=False):
        order_idx = list(order_group.index)
        main_idx = [idx for idx in order_idx if role.at[idx] == "main"]
        parent_by_seq = {
            str(out.at[idx, "item_seq"] or "").strip(): idx
            for idx in order_idx
            if str(out.at[idx, "item_seq"] or "").strip()
        }
        first_main_by_menu_seq: dict[str, int] = {}
        for idx in main_idx:
            menu_seq = str(out.at[idx, "menu_seq"] or "").strip()
            first_main_by_menu_seq.setdefault(menu_seq, idx)

        for idx in order_idx:
            if role.at[idx] != "option":
                continue
            parent_seq = str(out.at[idx, "parent_item_seq"] or "").strip()
            parent_idx = parent_by_seq.get(parent_seq)
            target_idx = parent_idx if parent_idx is not None and role.at[parent_idx] == "main" else None
            if target_idx is None:
                target_idx = first_main_by_menu_seq.get(str(out.at[idx, "menu_seq"] or "").strip())
            if target_idx is None and len(main_idx) == 1:
                target_idx = main_idx[0]

            if target_idx is not None:
                out.at[idx, "parent_item_seq"] = str(out.at[target_idx, "item_seq"] or "").strip()
                out.at[idx, "menu_seq"] = str(out.at[target_idx, "menu_seq"] or "").strip()
                for col in ("menu_name", "std_menu_name"):
                    if col in out.columns:
                        out.at[idx, col] = str(out.at[target_idx, col] or "").strip()
                repaired += 1
                continue

            if kind.at[idx] in {
                OPTION_KIND_DRINK,
                OPTION_KIND_RICE,
                OPTION_KIND_SIDE,
                OPTION_KIND_REVIEW,
                OPTION_KIND_REQUEST,
                OPTION_KIND_MATERIAL,
                OPTION_KIND_SPICE,
            }:
                out.at[idx, "line_role"] = "side"
                out.at[idx, "parent_item_seq"] = str(out.at[idx, "item_seq"] or "").strip()
                item_name = str(out.at[idx, "item_name"] or "").strip()
                if "menu_name" in out.columns:
                    out.at[idx, "menu_name"] = item_name
                if "std_menu_name" in out.columns:
                    out.at[idx, "std_menu_name"] = item_name
                demoted += 1

    if repaired or demoted:
        logger.info("option 부모 main 보정: parent_sync=%d side_demote=%d", repaired, demoted)
    return out


def _apply_canonical_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    today = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
    source = _clean_nan_series(out["source"])
    platform = _clean_nan_series(out["platform"])
    date = _clean_nan_series(out["sale_date"])
    for manual_src, platforms in DELIVERY_PLATFORM_FAMILIES.items():
        family = platform.isin(platforms)
        manual = source.eq(manual_src) & family
        today_manual = manual & date.eq(today)
        if today_manual.any():
            out.loc[today_manual, "_canonical"] = False
        manual_past_keys = set(zip(date[manual & date.ne(today)], platform[manual & date.ne(today)]))
        if manual_past_keys:
            row_keys = pd.Series(list(zip(date, platform)), index=out.index)
            out.loc[source.eq(POSFEED_SOURCE) & family & row_keys.isin(manual_past_keys), "_canonical"] = False
    return out


def _product_table_frames(lookup: _ProductLookup) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    product_map = lookup.map_target.copy()
    if "review_status_edit" in product_map.columns and "검수유무" not in product_map.columns:
        product_map["검수유무"] = product_map["review_status_edit"]
    map_cols = [
        c
        for c in ["item_id", "source", "brand", "store", "item_name", "unitprice", "표준_메뉴명_edit", "수동분류_edit", "대표메뉴", "검수유무", "classified_by"]
        if c in product_map.columns
    ]
    product_map = product_map.reindex(columns=map_cols)
    join_cols = [c for c in ["item_id", "store", "source", "brand", "standard_menu_name", "category"] if c in lookup.join_target.columns]
    product_join = lookup.join_target.reindex(columns=join_cols)
    master = lookup.master_target.copy()
    if "is_latest" in master.columns:
        master = master[master["is_latest"].fillna("").astype(str).str.upper().eq("Y")]
    master_cols = [c for c in ["상품코드", "source", "brand", "store", "상품명", "판매단가", "is_main_candidate", "수동분류", "exclude_check", "is_latest"] if c in master.columns]
    product_master = master.reindex(columns=master_cols)
    return product_map, product_join, product_master


def dump_product_tables(ym: str | list[str] | tuple[str, ...] | None = None) -> str:
    assert_no_model_classification_dependencies()
    lookup = _ProductLookup()
    product_map, product_join, product_master = _product_table_frames(lookup)
    _write_csv(product_map, NEW_CLS_DIR / "01_product_map.csv")
    _write_csv(product_join, NEW_CLS_DIR / "02_product_join.csv")
    _write_csv(product_master, NEW_CLS_DIR / "03_product_master.csv")
    _write_csv(pd.DataFrame(columns=GAP_COLUMNS), NEW_CLS_DIR / "04_product_gap.csv")
    return f"상품표 CSV 저장 완료 | map={len(product_map)} join={len(product_join)} master={len(product_master)}"


def build_raw_lines(ym: str | list[str] | tuple[str, ...] | None = None) -> str:
    messages = []
    for target_ym in resolve_yms(ym):
        df = _all_source_orders(target_ym)
        raw = pd.DataFrame(columns=RAW_LINE_COLUMNS)
        if not df.empty:
            raw = pd.DataFrame({
                "ym": df["ym"],
                "sale_date": df["sale_date"],
                "source": df["source"],
                "brand": df["brand"],
                "store": df["store"],
                "platform": df["platform"],
                "order_id": df["order_id"],
                "item_seq": df["item_seq"],
                "raw_item_name": df["_raw_item_name"],
                "수량": df["qty"],
                "단가": df["unit_price"],
                "is_child": (~df["_boundary"].fillna(False).astype(bool)).astype(str),
                "boundary": df["_boundary"].fillna(False).astype(bool).astype(str),
            })
        _write_csv(raw.reindex(columns=RAW_LINE_COLUMNS), NEW_CLS_DIR / f"05_raw_lines_{target_ym}.csv")
        messages.append(f"{target_ym}:{len(raw)}")
    return "raw lines 저장 완료 | " + ", ".join(messages)


def _new_review_output_columns(existing: pd.DataFrame) -> list[str]:
    columns = [*NEW_REVIEW_INPUT_COLUMNS]
    for col in existing.columns:
        if col not in columns:
            columns.append(col)
    return columns


def _line_role_review_category(line_role: object) -> str:
    role = str(line_role or "").strip()
    if role == "main":
        return "메인"
    if role == "option":
        return "옵션"
    if role == "side":
        return "사이드"
    return ""


def _build_new_review_seed_rows(yms: list[str]) -> pd.DataFrame:
    frames = []
    for target_ym in yms:
        df = _all_source_orders(target_ym)
        canonical = df[df.get("_canonical", True).astype(bool)].copy() if not df.empty else df
        if not canonical.empty:
            frames.append(canonical)
    if not frames:
        return pd.DataFrame(columns=NEW_REVIEW_INPUT_COLUMNS)

    work = pd.concat(frames, ignore_index=True, sort=False).fillna("")
    for col in ["source", "brand", "store", "item_id", "item_name", "unit_price", "std_menu_name", "line_role", "total_price"]:
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].fillna("").astype(str).str.strip()
    work = _target_product_rows(work)
    work = work[work["item_id"].ne("") & work["item_name"].ne("")]
    if work.empty:
        return pd.DataFrame(columns=NEW_REVIEW_INPUT_COLUMNS)

    grouped = (
        work.groupby(NEW_REVIEW_INPUT_KEY_COLUMNS, dropna=False, sort=False)
        .agg(
            unitprice=("unit_price", _first_clean_value),
            표준_메뉴명_edit=("std_menu_name", _first_clean_value),
            _line_role=("line_role", _first_clean_value),
            _sales=("total_price", lambda values: pd.to_numeric(values, errors="coerce").fillna(0).sum()),
        )
        .reset_index()
    )
    grouped["item_key"] = grouped["item_name"].map(_normalize_item_key)
    grouped["수동분류_edit"] = grouped["_line_role"].map(_line_role_review_category)
    grouped["중복_수동분류"] = ""
    grouped["검수유무"] = "0"
    grouped["검수사유"] = "메뉴계층 전용 신규 적재"
    grouped.loc[pd.to_numeric(grouped["_sales"], errors="coerce").fillna(0).ne(0), "검수사유"] = (
        "메뉴계층 전용 신규 적재; 수익률 입력 필요"
    )
    for col in [*_MANUAL_CHICKEN_COLUMNS, "수익률_manual"]:
        grouped[col] = ""
    return grouped.drop(columns=["_line_role", "_sales"]).reindex(columns=NEW_REVIEW_INPUT_COLUMNS, fill_value="")


def _write_new_review_input(df: pd.DataFrame) -> None:
    path = NEW_FIN_PRODUCT_MAP_REVIEW_CSV_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def build_new_fin_product_map_review_input(
    ym: str | list[str] | tuple[str, ...] | None = None,
    *,
    dry_run: bool = True,
) -> dict[str, object]:
    """송파삼전점 메뉴계층 전용 상품 검수표를 4~8월 등 지정 월 주문서 기준으로 만든다."""
    yms = resolve_yms(ym)
    existing = _load_menu_hierarchy_review_input().fillna("")
    if existing.empty:
        existing = pd.DataFrame(columns=NEW_REVIEW_INPUT_COLUMNS)
    existing = _normalize_review_input_frame(existing)
    output_columns = _new_review_output_columns(existing)
    existing = existing.reindex(columns=output_columns, fill_value="")

    seed = _build_new_review_seed_rows(yms).reindex(columns=output_columns, fill_value="")
    if not seed.empty:
        existing_keys = {
            tuple(str(row.get(col, "") or "").strip() for col in NEW_REVIEW_INPUT_KEY_COLUMNS)
            for row in existing.reindex(columns=NEW_REVIEW_INPUT_KEY_COLUMNS, fill_value="").to_dict("records")
        }
        seed = seed[
            seed.reindex(columns=NEW_REVIEW_INPUT_KEY_COLUMNS, fill_value="").apply(
                lambda row: tuple(str(row.get(col, "") or "").strip() for col in NEW_REVIEW_INPUT_KEY_COLUMNS)
                not in existing_keys,
                axis=1,
            )
        ]

    result = pd.concat([existing, seed], ignore_index=True, sort=False).reindex(columns=output_columns, fill_value="")
    if not result.empty:
        result = _normalize_review_input_frame(result).reindex(columns=output_columns, fill_value="")
        result = result.drop_duplicates(subset=NEW_REVIEW_INPUT_KEY_COLUMNS, keep="last").reset_index(drop=True)

    summary: dict[str, object] = {
        "yms": yms,
        "dry_run": dry_run,
        "output_path": str(NEW_FIN_PRODUCT_MAP_REVIEW_CSV_PATH),
        "existing_rows": int(len(existing)),
        "seed_rows": int(len(seed)),
        "output_rows": int(len(result)),
    }
    if dry_run:
        return summary
    _write_new_review_input(result)
    return summary


def _manual_chicken_attrs() -> pd.DataFrame:
    columns = ["source", "brand", "store", "item_id", "item_name", *_MANUAL_CHICKEN_COLUMNS, "수익률_manual"]
    review = _load_menu_hierarchy_review_input().fillna("")
    if review.empty:
        return pd.DataFrame(columns=columns)
    if not {"source", "brand", "store", "item_id", "item_name"}.issubset(review.columns):
        return pd.DataFrame(columns=columns)
    for col in columns:
        if col not in review.columns:
            review[col] = ""
        review[col] = review[col].fillna("").astype(str).str.strip()
    review = _target_product_rows(review)
    return (
        review.reindex(columns=columns, fill_value="")
        .drop_duplicates(subset=["source", "brand", "store", "item_id", "item_name"], keep="last")
        .reset_index(drop=True)
    )


def _first_clean_value(values: pd.Series) -> str:
    cleaned = _clean_nan_series(values)
    cleaned = cleaned[cleaned.ne("")]
    return cleaned.iloc[0] if len(cleaned) else ""


def _unique_join(values: list[str]) -> str:
    return " | ".join(_unique_nonempty([str(value or "").strip() for value in values]))


def _option_row_price(row: pd.Series) -> float:
    return float(pd.to_numeric(pd.Series([row.get("total_price", "")]), errors="coerce").fillna(0).iloc[0])


def _has_size_option(rows: pd.DataFrame, final_size: str) -> bool:
    if not final_size or rows.empty:
        return False
    for _, row in rows.iterrows():
        if str(row.get("option_kind", "") or "").strip() != OPTION_KIND_SIZE:
            continue
        if final_size in _unique_nonempty(_infer_chicken_sizes(row.get("item_name", ""))):
            return True
    return False


def _is_redundant_default_size_option(row: pd.Series, final_size: str, has_matching_size_option: bool) -> bool:
    if not final_size or final_size == CHICKEN_SIZE_NONE or not has_matching_size_option:
        return False
    if str(row.get("option_kind", "") or "").strip() != OPTION_KIND_SIZE:
        return False
    option_sizes = _unique_nonempty(_infer_chicken_sizes(row.get("item_name", "")))
    return bool(option_sizes and final_size not in option_sizes and _option_row_price(row) == 0)


def _is_chicken_attr_option(value: object) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return bool(_infer_chicken_types(text) or _infer_chicken_sizes(text) or _has_any_token(text, _CHICKEN_MENU_TOKENS))


def _normalize_material_name(value: object) -> str:
    """재료명 표기 흔들림 흡수: '한우대창 75g'/'한우 대창 75g' → '한우대창'."""
    text = str(value or "").strip()
    if not text:
        return ""
    text = _strip_leading_tags(text)
    text = _MATERIAL_WEIGHT_RE.sub(" ", text)
    text = _MATERIAL_COUNT_RE.sub(" ", text)
    text = re.sub(r"\((?:[^)]*)\)", " ", text)
    text = re.sub(r"추가|변경|선택|주세요|제공", " ", text)
    return re.sub(r"[\s\-_+]+", "", text).strip()


def _looks_like_chicken_addon_option(value: object) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if _looks_like_one_serving_addon_option(text):
        return True
    if _ROLE_CHANGE_RE.search(text):
        return False
    if re.fullmatch(r"\s*1\s*인\s*추가\s*", text):
        return True
    grams = _extract_chicken_addon_grams(text)
    if grams is None or not _infer_chicken_addon_type(text):
        return False
    normalized = re.sub(r"\s+", "", text)
    if re.fullmatch(r"(?:한그릇\])?(?:순살(?:\([^)]*\))?|닭다리살100%순살)\d+(?:\.\d+)?[gG](?:추가)?", normalized):
        return True
    if _ROLE_ADDON_RE.search(text) and _CHICKEN_ADDON_RE.search(text) and "+" not in normalized:
        return True
    return False


def _looks_like_one_serving_addon_option(value: object) -> bool:
    return bool(re.fullmatch(r"\s*1\s*인\s*분\s*", str(value or "")))


def _suggest_option_kind(item_name: object, line_role: object = "", category: object = "") -> str:
    """규칙 기반 option_kind 제안값.

    마스터 테이블의 초안일 뿐이며 확정값은 사람이 정한다. 닭 결정 옵션(사이즈/닭유형)은
    기존 _infer_chicken_types/_infer_chicken_sizes와 반드시 같은 판정을 내야 한다.
    다르면 닭옵션키가 실제 닭 속성을 결정하지 못한다.
    """
    role = str(line_role or "").strip()
    name = str(item_name or "").strip()
    if role == "main":
        return OPTION_KIND_MAIN
    if role in {"fee", "discount"}:
        return OPTION_KIND_FEE
    if not name:
        return OPTION_KIND_UNSET
    if _SETTLEMENT_EXCLUDE_RE.search(name):
        return OPTION_KIND_SETTLEMENT_EXCLUDE
    if _FEE_RE.search(name) or _DISCOUNT_RE.search(name):
        return OPTION_KIND_FEE
    if _looks_like_chicken_addon_option(name):
        return OPTION_KIND_CHICKEN_ADDON
    # 닭유형/사이즈는 기존 추론 함수를 그대로 쓴다. 여기서 규칙을 새로 쓰면 갈라진다.
    if _infer_chicken_types(name):
        return OPTION_KIND_CHICKEN_TYPE
    if _infer_chicken_sizes(name):
        return OPTION_KIND_SIZE
    if _REVIEW_RE.search(name):
        return OPTION_KIND_REVIEW
    if _SPICE_RE.search(name):
        return OPTION_KIND_SPICE
    if _REQUEST_RE.search(name):
        return OPTION_KIND_REQUEST
    if _RICE_RE.search(name):
        return OPTION_KIND_RICE
    if _DRINK_RE.search(name):
        return OPTION_KIND_DRINK
    if _MATERIAL_RE.search(name):
        return OPTION_KIND_MATERIAL
    category_text = str(category or "").strip()
    if category_text in _SIDE_LINE_CATEGORIES or _SIDE_NAME_RE.search(name):
        return OPTION_KIND_SIDE
    return OPTION_KIND_SIDE


def _forced_option_kind(item_name: object, line_role: object, suggested: object, confirmed: object) -> str:
    """닭 사용량을 바꾸는 옵션은 수동 확정값보다 안전 규칙을 우선한다."""
    role = str(line_role or "").strip()
    name = str(item_name or "").strip()
    suggestion = str(suggested or "").strip()
    confirm = str(confirmed or "").strip()
    if role == "main":
        return OPTION_KIND_MAIN
    if suggestion == OPTION_KIND_CHICKEN_ADDON and confirm in {OPTION_KIND_MATERIAL, OPTION_KIND_CHICKEN_TYPE}:
        return OPTION_KIND_CHICKEN_ADDON
    if role == "option" and _looks_like_chicken_addon_option(name):
        return OPTION_KIND_CHICKEN_ADDON
    return confirm or suggestion


def _chicken_addon_profit_name(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    compact = re.sub(r"\s+", "", text)
    if "1인추가" in compact or _looks_like_one_serving_addon_option(text):
        return "1인분 추가"
    grams = _extract_chicken_addon_grams(text)
    addon_type = _infer_chicken_addon_type(text)
    if grams is not None and addon_type:
        return f"{addon_type}추가{_format_number(grams)}g"
    return _normalize_material_name(text) or _strip_leading_tags(text) or text


def _suggest_material_name(item_name: object, option_kind: object) -> str:
    """재료추가 성격일 때만 재료명을 제안한다. 23번 단가표의 키가 된다."""
    if str(option_kind or "").strip() != OPTION_KIND_MATERIAL:
        return ""
    return _normalize_material_name(item_name)


def _chicken_conversion_attrs() -> dict[str, float]:
    frame = _read_manual_workbook_sheet(CHICKEN_CONVERSION_SHEET_NAME).fillna("")
    if frame.empty or not {"항목", "값_manual"}.issubset(frame.columns):
        return {
            "순살_1마리_g": DEFAULT_CHICKEN_ADDON_GRAMS_PER_BIRD,
            "뼈닭_1마리_g": DEFAULT_CHICKEN_ADDON_GRAMS_PER_BIRD,
        }
    out: dict[str, float] = {}
    for _, row in frame.iterrows():
        key = str(row.get("항목", "") or "").strip()
        value = pd.to_numeric(pd.Series([row.get("값_manual", "")]), errors="coerce").iloc[0]
        if key and not pd.isna(value) and float(value) > 0:
            out[key] = float(value)
    out.setdefault("순살_1마리_g", DEFAULT_CHICKEN_ADDON_GRAMS_PER_BIRD)
    out.setdefault("뼈닭_1마리_g", DEFAULT_CHICKEN_ADDON_GRAMS_PER_BIRD)
    return out


def _extract_chicken_addon_grams(value: object) -> float | None:
    text = str(value or "")
    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:g|G)", text)
    if not match:
        return None
    return float(match.group(1))


def _expected_chicken_addon_unit_price(value: object) -> float | None:
    if _looks_like_one_serving_addon_option(value):
        return 16900.0
    grams = _extract_chicken_addon_grams(value)
    if grams is None:
        return None
    return CHICKEN_ADDON_EXPECTED_UNIT_PRICES.get(float(grams))


def _infer_chicken_addon_type(value: object) -> str:
    text = str(value or "")
    if "순살" in text or "닭다리살" in text:
        return "순살"
    if re.search(r"(^|\s|\[|\(|\+|/)뼈(\s|\]|\)|$|\+|/)|뼈닭", text):
        return "뼈닭"
    return ""


def _format_number(value: object) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0).iloc[0]
    as_float = float(number)
    if as_float.is_integer():
        return str(int(as_float))
    return f"{as_float:.4f}".rstrip("0").rstrip(".")


def _usage_for(chicken_type: object, chicken_size: object) -> str:
    usage = _CHICKEN_USAGE.get((str(chicken_type or "").strip(), str(chicken_size or "").strip()))
    return f"{usage:g}" if usage is not None else ""


def _expected_usage_for(chicken_type: object, chicken_size: object) -> str:
    if str(chicken_type or "").strip() == CHICKEN_TYPE_NONE:
        return "0"
    return _usage_for(chicken_type, chicken_size)


def _normalize_chicken_type(value: object) -> str:
    text = str(value or "").strip()
    if text in {"뼈", "뼈닭"}:
        return "뼈닭"
    if text in {"순살", "닭다리살"}:
        return "순살"
    return text if text in {"뼈닭", "순살"} else ""


def _half_combo_from_slots(slot1: object, slot2: object) -> str:
    left = _normalize_chicken_type(slot1)
    right = _normalize_chicken_type(slot2)
    if not left or not right:
        return ""
    return f"{'뼈' if left == '뼈닭' else left}+{'뼈' if right == '뼈닭' else right}"


def _chicken_type_from_slots(slot1: object, slot2: object) -> str:
    left = _normalize_chicken_type(slot1)
    right = _normalize_chicken_type(slot2)
    if not left or not right:
        return ""
    if left == right:
        return left
    return CHICKEN_TYPE_MIXED


def _bone_ratio_from_slots(slot1: object, slot2: object) -> float | None:
    slots = [_normalize_chicken_type(slot1), _normalize_chicken_type(slot2)]
    if any(not slot for slot in slots):
        return None
    return slots.count("뼈닭") / 2.0


def _usage_from_half_slots(chicken_size: object, slot1: object, slot2: object) -> tuple[str, str, str]:
    size = str(chicken_size or "").strip()
    slots = [_normalize_chicken_type(slot1), _normalize_chicken_type(slot2)]
    if not size or any(not slot for slot in slots):
        return "", "", ""
    bone = 0.0
    boneless = 0.0
    for slot in slots:
        usage = _usage_for(slot, size)
        if not usage:
            return "", "", ""
        amount = float(usage) * HALF_SLOT_SHARE
        if slot == "뼈닭":
            bone += amount
        elif slot == "순살":
            boneless += amount
    return _format_number(bone + boneless), _format_number(bone), _format_number(boneless)


def _infer_half_slots_detail(
    menu_context: object,
    menu_type_signals: list[str] | None = None,
    option_type_signals: list[str] | None = None,
    ordered_option_type_signals: list[str] | None = None,
) -> tuple[str, str, str]:
    text = str(menu_context or "")
    if "반반" not in text:
        return "", "", ""
    if option_type_signals is None and ordered_option_type_signals is None:
        ordered_option_type_signals = menu_type_signals or []
        menu_type_signals = _infer_chicken_types(text)
    elif menu_type_signals is None:
        menu_type_signals = _infer_chicken_types(text)
    menu_types = [_normalize_chicken_type(value) for value in (menu_type_signals or [])]
    menu_unique = _unique_nonempty([value for value in menu_types if value])
    if len(menu_unique) == 1 and not (ordered_option_type_signals or option_type_signals):
        return menu_unique[0], menu_unique[0], ""
    ordered = [_normalize_chicken_type(value) for value in (ordered_option_type_signals or option_type_signals or [])]
    ordered = [value for value in ordered if value]
    if len(ordered) >= 2:
        reason = "충돌:반반슬롯" if len(ordered) > 2 else ""
        return ordered[0], ordered[1], reason
    if len(ordered) == 1:
        if len(menu_unique) == 1:
            return ordered[0], menu_unique[0], ""
        other = "순살" if ordered[0] == "뼈닭" else "뼈닭"
        return ordered[0], other, ""
    if len(menu_unique) == 1:
        return menu_unique[0], menu_unique[0], ""
    if len(menu_unique) > 1:
        return menu_unique[0], menu_unique[1], "충돌:반반슬롯"
    return "", "", "반반슬롯미입력"


def _infer_half_slots(
    menu_context: object,
    ordered_type_signals: list[str] | None = None,
) -> tuple[str, str]:
    slot1, slot2, _reason = _infer_half_slots_detail(menu_context, ordered_type_signals)
    return slot1, slot2


def _half_combo_from_type_signals(type_signals: list[str], context: object) -> tuple[str, float | None]:
    """반반 메뉴의 뼈/순살 조합을 확정한다.

    같은 주문 안에서 뼈/순살 신호가 2개 이상 나오거나 메뉴명에 반반이 있을 때만
    조합으로 본다. 일반 메뉴의 중복 옵션명 하나가 반반으로 오인되는 것을 막기 위해
    신호가 하나뿐이면 확정하지 않는다.
    """
    cleaned = [value for value in type_signals if value in {"뼈닭", "순살"}]
    if len(cleaned) < 2:
        return "", None
    text = str(context or "")
    if "반반" not in text:
        return "", None
    bone = cleaned.count("뼈닭")
    boneless = cleaned.count("순살")
    if bone >= 2 and boneless == 0:
        return "뼈+뼈", 1.0
    if bone >= 1 and boneless >= 1:
        return "뼈+순살", 0.5
    if boneless >= 2 and bone == 0:
        return "순살+순살", 0.0
    return "", None


def _chicken_type_from_half_ratio(ratio: float) -> str:
    if ratio >= 1.0:
        return "뼈닭"
    if ratio <= 0.0:
        return "순살"
    return CHICKEN_TYPE_MIXED


def _bone_ratio_from_half_combo(value: object) -> float | None:
    text = str(value or "").replace(" ", "").strip()
    if text in {"뼈+뼈", "뼈|뼈", "뼈닭+뼈닭", "뼈닭|뼈닭"}:
        return 1.0
    if text in {"뼈+순살", "순살+뼈", "뼈|순살", "순살|뼈", "뼈닭+순살", "순살+뼈닭"}:
        return 0.5
    if text in {"순살+순살", "순살|순살"}:
        return 0.0
    return None


def _usage_for_half_ratio(chicken_size: object, ratio: float) -> str:
    bone_usage = _usage_for("뼈닭", chicken_size)
    boneless_usage = _usage_for("순살", chicken_size)
    if not bone_usage and not boneless_usage:
        return ""
    bone_value = float(bone_usage or 0)
    boneless_value = float(boneless_usage or 0)
    return _format_number(ratio * bone_value + (1.0 - ratio) * boneless_value)


def _attach_chicken_usage_split_columns(left_joined: pd.DataFrame) -> pd.DataFrame:
    out = left_joined.copy()
    if out.empty:
        for col in CHICKEN_SPLIT_COLUMNS:
            out[col] = ""
        return out
    role = out.get("line_role", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    qty = pd.to_numeric(out.get("qty", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    usage = pd.to_numeric(out.get(CHICKEN_USAGE_COLUMN, pd.Series("", index=out.index)), errors="coerce").fillna(0)
    ratio_text = out.get(CHICKEN_RATIO_APPLIED_COLUMN, pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    chicken_type = out.get("닭유형", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    size = out.get("사이즈", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    slot1 = out.get(HALF_SLOT1_COLUMN, pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    slot2 = out.get(HALF_SLOT2_COLUMN, pd.Series("", index=out.index)).fillna("").astype(str).str.strip()

    bone_values: dict[int, str] = {}
    boneless_values: dict[int, str] = {}
    bone_totals: dict[int, str] = {}
    boneless_totals: dict[int, str] = {}
    for idx in out.index:
        if role.at[idx] != "main":
            bone_values[idx] = ""
            boneless_values[idx] = ""
            bone_totals[idx] = ""
            boneless_totals[idx] = ""
            continue
        ctype = chicken_type.at[idx]
        slot_usage, slot_bone, slot_boneless = _usage_from_half_slots(size.at[idx], slot1.at[idx], slot2.at[idx])
        if slot_usage:
            bone = float(slot_bone or 0)
            boneless = float(slot_boneless or 0)
        elif ctype == "뼈닭":
            bone = float(usage.at[idx])
            boneless = 0.0
        elif ctype == "순살":
            bone = 0.0
            boneless = float(usage.at[idx])
        elif ctype == CHICKEN_TYPE_MIXED:
            ratio = _parse_bone_ratio(ratio_text.at[idx])
            if ratio is None:
                ratio = 0.5
            bone_usage = _usage_for("뼈닭", size.at[idx])
            boneless_usage = _usage_for("순살", size.at[idx])
            if bone_usage or boneless_usage:
                bone = ratio * float(bone_usage or 0)
                boneless = (1.0 - ratio) * float(boneless_usage or 0)
            else:
                bone = float(usage.at[idx]) * ratio
                boneless = float(usage.at[idx]) * (1.0 - ratio)
            if abs((bone + boneless) - float(usage.at[idx])) > 0.0001:
                bone = float(usage.at[idx]) * ratio
                boneless = float(usage.at[idx]) * (1.0 - ratio)
        else:
            bone = 0.0
            boneless = 0.0
        bone_values[idx] = _format_number(bone)
        boneless_values[idx] = _format_number(boneless)
        bone_totals[idx] = _format_number(bone * float(qty.at[idx]))
        boneless_totals[idx] = _format_number(boneless * float(qty.at[idx]))

    out[BONE_USAGE_COLUMN] = pd.Series(bone_values)
    out[BONELESS_USAGE_COLUMN] = pd.Series(boneless_values)
    out[BONE_USAGE_TOTAL_COLUMN] = pd.Series(bone_totals)
    out[BONELESS_USAGE_TOTAL_COLUMN] = pd.Series(boneless_totals)
    if HALF_COMBO_COLUMN not in out.columns:
        out[HALF_COMBO_COLUMN] = ""
    return out


def _repair_main_parent_invariant(left_joined: pd.DataFrame) -> pd.DataFrame:
    out = left_joined.copy()
    if out.empty or not {"line_role", "item_seq", "parent_item_seq"}.issubset(out.columns):
        return out
    role = out.get("line_role", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    main = role.eq("main")
    mismatch = main & out["item_seq"].fillna("").astype(str).str.strip().ne(
        out["parent_item_seq"].fillna("").astype(str).str.strip()
    )
    if mismatch.any():
        out.loc[mismatch, "parent_item_seq"] = out.loc[mismatch, "item_seq"].astype(str)
        logger.info("main parent_item_seq self 보정: %d행", int(mismatch.sum()))
    return out


def _repair_chicken_split_invariant(left_joined: pd.DataFrame) -> pd.DataFrame:
    out = left_joined.copy()
    if out.empty or not {"line_role", "닭유형"}.issubset(out.columns):
        return out
    role = out.get("line_role", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    ctype = out.get("닭유형", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    slot1 = out.get(HALF_SLOT1_COLUMN, pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    slot2 = out.get(HALF_SLOT2_COLUMN, pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    slot_type = pd.Series(
        [_chicken_type_from_slots(a, b) if a and b else "" for a, b in zip(slot1, slot2)],
        index=out.index,
    )
    conflict = role.eq("main") & ctype.isin(["뼈닭", "순살"]) & slot_type.ne("") & slot_type.ne(ctype)
    if conflict.any():
        for col in (HALF_COMBO_COLUMN, HALF_SLOT1_COLUMN, HALF_SLOT2_COLUMN, CHICKEN_RATIO_APPLIED_COLUMN):
            if col in out.columns:
                out.loc[conflict, col] = ""
        logger.info("수기 닭유형과 충돌하는 반반슬롯 제거: %d행", int(conflict.sum()))
        out = _attach_chicken_usage_split_columns(out)
    out = _sync_chicken_split_totals_to_usage_total(out)
    return out


def _sync_chicken_split_totals_to_usage_total(left_joined: pd.DataFrame) -> pd.DataFrame:
    out = left_joined.copy()
    required = {"line_role", "닭유형", CHICKEN_USAGE_TOTAL_COLUMN, BONE_USAGE_TOTAL_COLUMN, BONELESS_USAGE_TOTAL_COLUMN}
    if out.empty or not required.issubset(out.columns):
        return out
    role = out.get("line_role", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    ctype = out.get("닭유형", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    usage_total = pd.to_numeric(out.get(CHICKEN_USAGE_TOTAL_COLUMN, pd.Series("", index=out.index)), errors="coerce").fillna(0)
    bone_total = pd.to_numeric(out.get(BONE_USAGE_TOTAL_COLUMN, pd.Series("", index=out.index)), errors="coerce").fillna(0)
    boneless_total = pd.to_numeric(out.get(BONELESS_USAGE_TOTAL_COLUMN, pd.Series("", index=out.index)), errors="coerce").fillna(0)
    target = role.eq("main") & ctype.ne(CHICKEN_TYPE_NONE) & (usage_total - (bone_total + boneless_total)).abs().gt(0.0001)
    if not target.any():
        return out
    ratio_text = out.get(CHICKEN_RATIO_APPLIED_COLUMN, pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    repaired = 0
    for idx in out.index[target]:
        total = float(usage_total.at[idx])
        row_type = ctype.at[idx]
        if total == 0:
            bone = 0.0
            boneless = 0.0
        elif row_type == "뼈닭":
            bone = total
            boneless = 0.0
        elif row_type == "순살":
            bone = 0.0
            boneless = total
        else:
            ratio = _parse_bone_ratio(ratio_text.at[idx])
            if ratio is None:
                ratio = 0.5
            bone = total * ratio
            boneless = total * (1.0 - ratio)
        out.at[idx, BONE_USAGE_TOTAL_COLUMN] = _format_number(bone)
        out.at[idx, BONELESS_USAGE_TOTAL_COLUMN] = _format_number(boneless)
        repaired += 1
    logger.info("닭 분해 합계 동기화: %d행", repaired)
    return out


def _repair_missing_half_menu_attrs(left_joined: pd.DataFrame) -> pd.DataFrame:
    out = left_joined.copy()
    if out.empty or not {"line_role", "닭유형", "사이즈"}.issubset(out.columns):
        return out
    role = out.get("line_role", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    ctype = out.get("닭유형", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    size = out.get("사이즈", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    text = (
        out.get("item_name", pd.Series("", index=out.index)).fillna("").astype(str)
        + " "
        + out.get("menu_name", pd.Series("", index=out.index)).fillna("").astype(str)
        + " "
        + out.get("std_menu_name", pd.Series("", index=out.index)).fillna("").astype(str)
    )
    target = role.eq("main") & ctype.eq("") & size.ne("") & text.str.contains("반반", regex=False)
    if not target.any():
        return out
    out.loc[target, "닭유형"] = CHICKEN_TYPE_MIXED
    out.loc[target, "사용용량"] = [
        _usage_for_half_ratio(row_size, 0.5) for row_size in size.loc[target]
    ]
    if HALF_COMBO_COLUMN in out.columns:
        out.loc[target, HALF_COMBO_COLUMN] = "뼈+순살"
    if HALF_SLOT1_COLUMN in out.columns:
        out.loc[target, HALF_SLOT1_COLUMN] = "뼈닭"
    if HALF_SLOT2_COLUMN in out.columns:
        out.loc[target, HALF_SLOT2_COLUMN] = "순살"
    qty = pd.to_numeric(out.get("qty", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    usage = pd.to_numeric(out.get("사용용량", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    out.loc[target, CHICKEN_USAGE_TOTAL_COLUMN] = (qty.loc[target] * usage.loc[target]).map(_format_number)
    out.loc[target, "닭유형_판정"] = CHICKEN_METHOD_HALF_SLOT
    if CHICKEN_RATIO_APPLIED_COLUMN in out.columns:
        out.loc[target, CHICKEN_RATIO_APPLIED_COLUMN] = "0.5"
    reason = out.get("미해결사유", pd.Series("", index=out.index)).fillna("").astype(str)
    out.loc[target, "미해결사유"] = reason.loc[target].map(
        lambda value: " | ".join(
            part.strip()
            for part in str(value or "").split("|")
            if part.strip()
            and part.strip()
            not in {
                "닭유형없음",
                "반반슬롯미입력",
                "수기 닭유형없음",
                "선택 닭유형없음",
                "닭유형옵션없음",
            }
        )
    )
    logger.info("반반 슬롯 없는 main 혼합 기본 보정: %d행", int(target.sum()))
    return _attach_chicken_usage_split_columns(out)


def _profit_channel_series(frame: pd.DataFrame) -> pd.Series:
    if PROFIT_CHANNEL_COLUMN in frame.columns:
        current = frame.get(PROFIT_CHANNEL_COLUMN, pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
    else:
        current = pd.Series("", index=frame.index)
    missing = current.eq("")
    if not missing.any():
        return current
    source = frame.get("source", pd.Series("", index=frame.index))
    platform = frame.get("platform", pd.Series("", index=frame.index))
    order_type = frame.get("order_type", pd.Series("", index=frame.index))
    inferred = pd.Series(
        [_profit_channel_from_values(src, plt, typ) for src, plt, typ in zip(source, platform, order_type)],
        index=frame.index,
    )
    return current.where(~missing, inferred)


def _price_match_axis_key_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_order_group_columns(frame.copy())
    out["_price_match_channel"] = _profit_channel_series(out)
    total = pd.to_numeric(out.get("total_price", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    role = out.get("line_role", pd.Series("", index=out.index)).astype(str).str.strip()
    kind = out.get("option_kind", pd.Series("", index=out.index)).astype(str).str.strip()
    price_part = total.abs().where(role.eq("main") | kind.isin(CHICKEN_DECIDING_KINDS), 0)
    group_price = price_part.groupby([out[col] for col in ORDER_GROUP_COLUMNS], dropna=False, sort=False).transform("sum")
    out["_price_match_unit"] = group_price.astype(float).round().astype(int).astype(str)
    return out


def _repair_option_none_attrs_from_visible_text(left_joined: pd.DataFrame) -> pd.DataFrame:
    """판정옵션이 덮은 옵션없음 행도 품목명 자체가 명확하면 메뉴명 판정을 복구한다."""
    if left_joined.empty:
        return left_joined
    out = left_joined.copy().fillna("")
    role = out.get("line_role", pd.Series("", index=out.index)).astype(str).str.strip()
    chicken_key = out.get(CHICKEN_OPTION_KEY_COLUMN, pd.Series("", index=out.index)).astype(str).str.strip()
    chicken_type = out.get("닭유형", pd.Series("", index=out.index)).astype(str).str.strip()
    type_method = out.get("닭유형_판정", pd.Series("", index=out.index)).astype(str).str.strip()
    total = pd.to_numeric(out.get("total_price", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    sale_type = out.get("sale_type", pd.Series("", index=out.index)).astype(str).str.strip()
    text = (
        out.get("menu_name", pd.Series("", index=out.index)).astype(str)
        + " "
        + out.get("std_menu_name", pd.Series("", index=out.index)).astype(str)
        + " "
        + out.get("item_name", pd.Series("", index=out.index)).astype(str)
    )
    target = (
        role.eq("main")
        & total.gt(0)
        & sale_type.ne("취소")
        & chicken_key.eq(OPTION_COMBO_NONE)
        & ~text.str.contains("반반", regex=False, na=False)
        & chicken_type.isin(["", CHICKEN_TYPE_MIXED])
        & type_method.isin(["", "판정옵션", "비율추정", "미해결"])
    )
    if not target.any():
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")

    profiles = out.loc[target].apply(_fixed_main_chicken_profile, axis=1)
    repaired = 0
    for idx, profile in profiles.dropna().items():
        matched_type, matched_size, matched_method = profile
        if matched_type == CHICKEN_TYPE_NONE:
            continue
        usage = _usage_for(matched_type, matched_size)
        if not usage:
            continue
        out.at[idx, "닭유형"] = matched_type
        out.at[idx, "사이즈"] = matched_size
        out.at[idx, CHICKEN_USAGE_COLUMN] = usage
        out.at[idx, "닭유형_판정"] = matched_method
        out.at[idx, "사이즈_판정"] = matched_method
        out.at[idx, "미해결사유"] = ""
        out.at[idx, CHICKEN_SIGNAL_COLUMN] = CHICKEN_SIGNAL_PRESENT
        repaired += 1
    if repaired:
        qty = pd.to_numeric(out.get("qty", pd.Series("", index=out.index)), errors="coerce").fillna(0)
        usage = pd.to_numeric(out.get(CHICKEN_USAGE_COLUMN, pd.Series("", index=out.index)), errors="coerce")
        out[CHICKEN_USAGE_TOTAL_COLUMN] = (usage * qty).map(lambda value: "" if pd.isna(value) else f"{float(value):g}")
        out = _attach_chicken_usage_split_columns(out)
        logger.info("옵션없음 품목명 보정: %d행", repaired)
    return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")


def _repair_option_none_attrs_by_price(left_joined: pd.DataFrame) -> pd.DataFrame:
    """옵션없는 주문은 같은 메뉴/채널/단가에서 신호 있는 주문이 한 값일 때만 보정한다."""
    if left_joined.empty:
        return left_joined
    out = _price_match_axis_key_frame(left_joined.fillna(""))
    role = out.get("line_role", pd.Series("", index=out.index)).astype(str).str.strip()
    chicken_key = out.get(CHICKEN_OPTION_KEY_COLUMN, pd.Series("", index=out.index)).astype(str).str.strip()
    chicken_type = out.get("닭유형", pd.Series("", index=out.index)).astype(str).str.strip()
    chicken_size = out.get("사이즈", pd.Series("", index=out.index)).astype(str).str.strip()
    type_method = out.get("닭유형_판정", pd.Series("", index=out.index)).astype(str).str.strip()
    signal = out.get(CHICKEN_SIGNAL_COLUMN, pd.Series("", index=out.index)).astype(str).str.strip()
    size_method = out.get("사이즈_판정", pd.Series("", index=out.index)).astype(str).str.strip()
    total = pd.to_numeric(out.get("total_price", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    sale_type = out.get("sale_type", pd.Series("", index=out.index)).astype(str).str.strip()
    normal_sale = total.gt(0) & sale_type.ne("취소")
    menu_text = (
        out.get("std_menu_name", pd.Series("", index=out.index)).astype(str)
        + " "
        + out.get("menu_name", pd.Series("", index=out.index)).astype(str)
        + " "
        + out.get("item_name", pd.Series("", index=out.index)).astype(str)
    )
    no_visible_signal = ~menu_text.map(lambda value: bool(_infer_chicken_types(value) or _infer_chicken_sizes(value)))
    non_half = ~menu_text.str.contains("반반", regex=False, na=False)
    target = (
        role.eq("main")
        & normal_sale
        & chicken_key.eq(OPTION_COMBO_NONE)
        & non_half
        & no_visible_signal
        & (chicken_type.eq("") | chicken_type.eq(CHICKEN_TYPE_MIXED) | type_method.isin(["판정옵션", "비율추정", "미해결"]))
    )
    evidence = (
        role.eq("main")
        & normal_sale
        & chicken_key.ne(OPTION_COMBO_NONE)
        & (signal.eq(CHICKEN_SIGNAL_PRESENT) | type_method.isin(PRICE_MATCH_EVIDENCE_METHODS) | size_method.isin(PRICE_MATCH_EVIDENCE_METHODS))
        & chicken_type.isin(["뼈닭", "순살"])
        & chicken_size.ne("")
        & chicken_size.ne(CHICKEN_SIZE_NONE)
    )
    base_key_cols = ["source", "brand", "store", "_price_match_channel", "std_menu_name"]
    key_cols = [*base_key_cols, "_price_match_unit"]
    lookup: dict[tuple[str, ...], tuple[str, str]] = {}
    nearby_evidence = pd.DataFrame()
    if evidence.any():
        ev = out.loc[evidence, [*key_cols, "닭유형", "사이즈", "order_id"]].copy()
        ev["_price_match_unit_num"] = pd.to_numeric(ev["_price_match_unit"], errors="coerce")
        combo_counts = (
            ev.groupby([*key_cols, "닭유형", "사이즈"], dropna=False, sort=False)["order_id"]
            .nunique()
            .reset_index(name="_combo_orders")
        )
        totals = combo_counts.groupby(key_cols, dropna=False, sort=False)["_combo_orders"].sum().reset_index(name="_total_orders")
        ranked = combo_counts.merge(totals, on=key_cols, how="left")
        ranked["_share"] = ranked["_combo_orders"] / ranked["_total_orders"].where(ranked["_total_orders"].ne(0), 1)
        ranked = ranked.sort_values([*key_cols, "_combo_orders", "_share"], ascending=[True, True, True, True, True, True, False, False])
        ranked = ranked.drop_duplicates(subset=key_cols, keep="first")
        reliable = ranked[
            (ranked["_share"].ge(0.8) & ranked["_combo_orders"].ge(3))
            | ranked["_total_orders"].eq(ranked["_combo_orders"])
        ]
        for _, row in reliable.iterrows():
            lookup[tuple(str(row.get(col, "") or "").strip() for col in key_cols)] = (
                str(row.get("닭유형", "") or "").strip(),
                str(row.get("사이즈", "") or "").strip(),
            )
        nearby_evidence = ev[ev["_price_match_unit_num"].notna()].copy()
    if (not lookup and nearby_evidence.empty) or not target.any():
        return out.drop(columns=[col for col in out.columns if col.startswith("_price_match_")], errors="ignore").reindex(
            columns=LEFT_JOINED_OUTPUT_COLUMNS,
            fill_value="",
        )
    repaired = 0
    near_repaired = 0
    for idx, row in out.loc[target].iterrows():
        key = tuple(str(row.get(col, "") or "").strip() for col in key_cols)
        matched = lookup.get(key)
        matched_method = CHICKEN_METHOD_PRICE_MATCH
        if not matched and not nearby_evidence.empty:
            target_unit = pd.to_numeric(pd.Series([row.get("_price_match_unit", "")]), errors="coerce").iloc[0]
            if not pd.isna(target_unit):
                candidates = nearby_evidence.copy()
                for col in base_key_cols:
                    candidates = candidates[
                        candidates[col].astype(str).str.strip().eq(str(row.get(col, "") or "").strip())
                    ]
                if not candidates.empty:
                    candidates["_price_diff"] = (candidates["_price_match_unit_num"] - float(target_unit)).abs()
                    candidates = candidates[candidates["_price_diff"].le(PRICE_MATCH_NEAR_MAX_DIFF)]
                    if not candidates.empty:
                        best_diff = candidates["_price_diff"].min()
                        nearest = candidates[candidates["_price_diff"].eq(best_diff)].copy()
                        combo_counts = (
                            nearest.groupby(["닭유형", "사이즈"], dropna=False, sort=False)["order_id"]
                            .nunique()
                            .reset_index(name="_combo_orders")
                            .sort_values(["_combo_orders", "닭유형", "사이즈"], ascending=[False, True, True])
                        )
                        total_orders = int(combo_counts["_combo_orders"].sum()) if not combo_counts.empty else 0
                        if total_orders >= PRICE_MATCH_NEAR_MIN_ORDERS and not combo_counts.empty:
                            top = combo_counts.iloc[0]
                            share = float(top["_combo_orders"]) / float(total_orders) if total_orders else 0.0
                            tied = combo_counts["_combo_orders"].eq(top["_combo_orders"]).sum() > 1
                            if share >= PRICE_MATCH_NEAR_MIN_SHARE and not tied:
                                matched = (
                                    str(top.get("닭유형", "") or "").strip(),
                                    str(top.get("사이즈", "") or "").strip(),
                                )
                                matched_method = CHICKEN_METHOD_NEAR_PRICE_MATCH
        if not matched:
            continue
        matched_type, matched_size = matched
        usage = _usage_for(matched_type, matched_size)
        if not usage:
            continue
        out.at[idx, "닭유형"] = matched_type
        out.at[idx, "사이즈"] = matched_size
        out.at[idx, CHICKEN_USAGE_COLUMN] = usage
        out.at[idx, "닭유형_판정"] = matched_method
        out.at[idx, "사이즈_판정"] = matched_method
        out.at[idx, "미해결사유"] = ""
        out.at[idx, CHICKEN_SIGNAL_COLUMN] = CHICKEN_SIGNAL_ABSENT
        repaired += 1
        if matched_method == CHICKEN_METHOD_NEAR_PRICE_MATCH:
            near_repaired += 1
    if repaired:
        qty = pd.to_numeric(out.get("qty", pd.Series("", index=out.index)), errors="coerce").fillna(0)
        usage = pd.to_numeric(out.get(CHICKEN_USAGE_COLUMN, pd.Series("", index=out.index)), errors="coerce")
        out[CHICKEN_USAGE_TOTAL_COLUMN] = (usage * qty).map(lambda value: "" if pd.isna(value) else f"{float(value):g}")
        out = _attach_chicken_usage_split_columns(out)
        logger.info("옵션없음 단가매칭 보정: %d행 (주변단가=%d행)", repaired, near_repaired)
    return out.drop(columns=[col for col in out.columns if col.startswith("_price_match_")], errors="ignore").reindex(
        columns=LEFT_JOINED_OUTPUT_COLUMNS,
        fill_value="",
    )


def _fixed_chicken_profile_from_text(text: object) -> tuple[str, str, str] | None:
    text = str(text or "").strip()
    if not text:
        return None
    if "막국수" in text:
        return (CHICKEN_TYPE_NONE, CHICKEN_SIZE_NONE, CHICKEN_METHOD_NONE)
    if "닭칼국수" in text:
        return ("뼈닭", "중", "메뉴명")
    if "순살" in text and re.search(r"1\s*인|한그릇", text):
        return ("순살", "1인", "메뉴명")
    if "순살" in text and re.search(r"2\s*인|2인이상", text):
        return ("순살", "2인", "메뉴명")
    if re.search(r"1\s*인", text) and _has_any_token(text, ["백숙", "닭한마리"]):
        return ("뼈닭", "1인", "메뉴명")
    if "닭개장" in text:
        return ("뼈닭", "중", "메뉴명")
    return None


def _fixed_main_chicken_profile(row: pd.Series) -> tuple[str, str, str] | None:
    if str(row.get("line_role", "") or "").strip() != "main":
        return None
    text = " ".join(
        str(row.get(col, "") or "").strip()
        for col in ("item_name", "std_menu_name")
        if str(row.get(col, "") or "").strip()
    )
    return _fixed_chicken_profile_from_text(text)


def _locked_main_chicken_profile(row: pd.Series) -> tuple[str, str, str] | None:
    if str(row.get("line_role", "") or "").strip() != "main":
        return None
    text = " ".join(
        str(row.get(col, "") or "").strip()
        for col in ("menu_name", "std_menu_name", "item_name")
        if str(row.get(col, "") or "").strip()
    )
    if "막국수" in text:
        return (CHICKEN_TYPE_NONE, CHICKEN_SIZE_NONE, CHICKEN_METHOD_NONE)
    if "닭칼국수" in text:
        return ("뼈닭", "중", "메뉴명")
    if "닭개장" in text:
        return ("뼈닭", "중", "메뉴명")
    return None


def _locked_chicken_profile_for_profit_row(row: pd.Series) -> tuple[str, str, str] | None:
    text = " ".join(
        str(row.get(col, "") or "").strip()
        for col in ("menu_name", "std_menu_name")
        if str(row.get(col, "") or "").strip()
    )
    if "막국수" in text:
        return (CHICKEN_TYPE_NONE, CHICKEN_SIZE_NONE, CHICKEN_METHOD_NONE)
    if "닭칼국수" in text:
        return ("뼈닭", "중", "메뉴명")
    if "닭개장" in text:
        return ("뼈닭", "중", "메뉴명")
    return None


def _split_profile_values(value: object) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return _unique_nonempty([part.strip() for part in re.split(r"[|,/]", text) if part.strip()])


def _profile_yes(value: object) -> bool:
    return str(value or "").strip().upper() in {"Y", "YES", "TRUE", "1", "적용"}


def _suggest_menu_chicken_profile(std_menu_name: object) -> dict[str, str]:
    text = str(std_menu_name or "").strip()
    if not text:
        return {
            "허용닭유형_제안": "",
            "기본닭유형_제안": "",
            "허용사이즈_제안": "",
            "기본사이즈_제안": "",
            "옵션닭유형적용_제안": "",
        }
    if "막국수" in text:
        return {
            "허용닭유형_제안": CHICKEN_TYPE_NONE,
            "기본닭유형_제안": CHICKEN_TYPE_NONE,
            "허용사이즈_제안": CHICKEN_SIZE_NONE,
            "기본사이즈_제안": CHICKEN_SIZE_NONE,
            "옵션닭유형적용_제안": "N",
        }
    if "닭떡볶이" in text:
        return {
            "허용닭유형_제안": "순살",
            "기본닭유형_제안": "순살",
            "허용사이즈_제안": "중",
            "기본사이즈_제안": "중",
            "옵션닭유형적용_제안": "N",
        }
    if "실비파김치" in text:
        return {
            "허용닭유형_제안": "순살",
            "기본닭유형_제안": "순살",
            "허용사이즈_제안": "",
            "기본사이즈_제안": "",
            "옵션닭유형적용_제안": "N",
        }
    if "순살" in text and re.search(r"1\s*인|한그릇", text) and "반반" not in text:
        return {
            "허용닭유형_제안": "순살",
            "기본닭유형_제안": "순살",
            "허용사이즈_제안": "1인",
            "기본사이즈_제안": "1인",
            "옵션닭유형적용_제안": "N",
        }
    if "순살" in text and re.search(r"2\s*인|2인이상|정식", text) and "반반" not in text:
        return {
            "허용닭유형_제안": "순살",
            "기본닭유형_제안": "순살",
            "허용사이즈_제안": "중",
            "기본사이즈_제안": "중",
            "옵션닭유형적용_제안": "N",
        }
    if "순살" in text and "반반" not in text:
        return {
            "허용닭유형_제안": "순살",
            "기본닭유형_제안": "순살",
            "허용사이즈_제안": "",
            "기본사이즈_제안": "중",
            "옵션닭유형적용_제안": "N",
        }
    if "닭칼국수" in text or "닭개장" in text or "닭한마리 칼국수 정식" in text:
        return {
            "허용닭유형_제안": "뼈닭",
            "기본닭유형_제안": "뼈닭",
            "허용사이즈_제안": "중",
            "기본사이즈_제안": "중",
            "옵션닭유형적용_제안": "N",
        }
    return {
        "허용닭유형_제안": "",
        "기본닭유형_제안": "",
        "허용사이즈_제안": "",
        "기본사이즈_제안": "",
        "옵션닭유형적용_제안": "",
    }


def _effective_menu_chicken_profile(row: pd.Series | None) -> dict[str, object] | None:
    if row is None:
        return None
    allowed_types_text = str(row.get("허용닭유형", "") or "").strip() or str(row.get("허용닭유형_제안", "") or "").strip()
    default_type = str(row.get("기본닭유형", "") or "").strip() or str(row.get("기본닭유형_제안", "") or "").strip()
    allowed_sizes_text = str(row.get("허용사이즈", "") or "").strip() or str(row.get("허용사이즈_제안", "") or "").strip()
    default_size = str(row.get("기본사이즈", "") or "").strip() or str(row.get("기본사이즈_제안", "") or "").strip()
    apply_option_text = str(row.get("옵션닭유형적용", "") or "").strip()
    if not apply_option_text:
        apply_option_text = str(row.get("옵션닭유형적용_제안", "") or "").strip()
    allowed_types = _split_profile_values(allowed_types_text)
    allowed_sizes = _split_profile_values(allowed_sizes_text)
    if not allowed_types and not default_type and not allowed_sizes and not default_size:
        return None
    return {
        "allowed_types": allowed_types,
        "default_type": default_type,
        "allowed_sizes": allowed_sizes,
        "default_size": default_size,
        "apply_option_type": _profile_yes(apply_option_text),
    }


def _menu_chicken_profile_attrs() -> pd.DataFrame:
    master = _manual_or_legacy_sheet(MENU_CHICKEN_PROFILE_SHEET_NAME, MENU_CHICKEN_PROFILE_OUTPUT_PATH).fillna("")
    if master.empty:
        return pd.DataFrame(columns=MENU_CHICKEN_PROFILE_COLUMNS)
    return master.reindex(columns=MENU_CHICKEN_PROFILE_COLUMNS, fill_value="")


def _build_menu_chicken_profile_master(left_joined: pd.DataFrame) -> pd.DataFrame:
    previous = _menu_chicken_profile_attrs()
    kept: dict[tuple[str, str, str, str], dict[str, str]] = {}
    if not previous.empty:
        for _, row in previous.iterrows():
            key = tuple(str(row.get(col, "") or "").strip() for col in MENU_CHICKEN_PROFILE_KEY_COLUMNS)
            if key[-1]:
                kept[key] = {col: str(row.get(col, "") or "").strip() for col in MENU_CHICKEN_PROFILE_EDIT_COLUMNS}
    if left_joined.empty:
        if kept:
            preserved = []
            for key, existing in kept.items():
                row = dict(zip(MENU_CHICKEN_PROFILE_KEY_COLUMNS, key))
                row.update({col: "" for col in MENU_CHICKEN_PROFILE_COLUMNS if col not in row})
                for col in MENU_CHICKEN_PROFILE_EDIT_COLUMNS:
                    row[col] = existing.get(col, "")
                preserved.append(row)
            return pd.DataFrame(preserved).reindex(columns=MENU_CHICKEN_PROFILE_COLUMNS, fill_value="")
        return pd.DataFrame(columns=MENU_CHICKEN_PROFILE_COLUMNS)
    work = left_joined.fillna("").copy()
    role = work.get("line_role", pd.Series("", index=work.index)).astype(str).str.strip()
    main = work[role.eq("main")].copy()
    if main.empty:
        if kept:
            preserved = []
            for key, existing in kept.items():
                row = dict(zip(MENU_CHICKEN_PROFILE_KEY_COLUMNS, key))
                row.update({col: "" for col in MENU_CHICKEN_PROFILE_COLUMNS if col not in row})
                for col in MENU_CHICKEN_PROFILE_EDIT_COLUMNS:
                    row[col] = existing.get(col, "")
                preserved.append(row)
            return pd.DataFrame(preserved).reindex(columns=MENU_CHICKEN_PROFILE_COLUMNS, fill_value="")
        return pd.DataFrame(columns=MENU_CHICKEN_PROFILE_COLUMNS)
    for col in ("qty", "total_price"):
        main[f"_{col}_num"] = pd.to_numeric(main.get(col, pd.Series("", index=main.index)), errors="coerce").fillna(0)
    grouped = (
        main.groupby(MENU_CHICKEN_PROFILE_KEY_COLUMNS, dropna=False, sort=False)
        .agg(
            대표메뉴명=("item_name", _first_clean_value),
            주문건수=("order_id", "nunique"),
            판매수량=("_qty_num", "sum"),
            매출합계=("_total_price_num", "sum"),
        )
        .reset_index()
    )
    suggestions = grouped["std_menu_name"].map(_suggest_menu_chicken_profile)
    for col in ("허용닭유형_제안", "기본닭유형_제안", "허용사이즈_제안", "기본사이즈_제안", "옵션닭유형적용_제안"):
        grouped[col] = [item.get(col, "") for item in suggestions]
    for col in ("주문건수", "판매수량", "매출합계"):
        grouped[col] = pd.to_numeric(grouped[col], errors="coerce").fillna(0).map(_format_number)
    for col in MENU_CHICKEN_PROFILE_EDIT_COLUMNS:
        grouped[col] = ""
    for idx, row in grouped.iterrows():
        key = tuple(str(row.get(col, "") or "").strip() for col in MENU_CHICKEN_PROFILE_KEY_COLUMNS)
        existing = kept.get(key, {})
        for col in MENU_CHICKEN_PROFILE_EDIT_COLUMNS:
            grouped.at[idx, col] = existing.get(col, "")
    grouped_keys = {
        tuple(str(row.get(col, "") or "").strip() for col in MENU_CHICKEN_PROFILE_KEY_COLUMNS)
        for _, row in grouped.iterrows()
    }
    preserved = []
    for key, existing in kept.items():
        if key in grouped_keys:
            continue
        row = dict(zip(MENU_CHICKEN_PROFILE_KEY_COLUMNS, key))
        row.update({col: "" for col in MENU_CHICKEN_PROFILE_COLUMNS if col not in row})
        for col in MENU_CHICKEN_PROFILE_EDIT_COLUMNS:
            row[col] = existing.get(col, "")
        row["메모"] = existing.get("메모", "") or "기존 수기값 보존"
        preserved.append(row)
    if preserved:
        grouped = pd.concat([grouped, pd.DataFrame(preserved)], ignore_index=True, sort=False)
    return grouped.reindex(columns=MENU_CHICKEN_PROFILE_COLUMNS, fill_value="")


def _menu_chicken_profile_lookup(menu_chicken_profile_master: pd.DataFrame | None) -> dict[tuple[str, str, str, str], dict[str, object]]:
    if menu_chicken_profile_master is None or menu_chicken_profile_master.empty:
        return {}
    lookup: dict[tuple[str, str, str, str], dict[str, object]] = {}
    for _, row in menu_chicken_profile_master.fillna("").iterrows():
        key = tuple(str(row.get(col, "") or "").strip() for col in MENU_CHICKEN_PROFILE_KEY_COLUMNS)
        if not key[-1]:
            continue
        profile = _effective_menu_chicken_profile(row)
        if profile is not None:
            lookup[key] = profile
    return lookup


@lru_cache(maxsize=1)
def _cached_menu_chicken_profile_lookup() -> dict[tuple[str, str, str, str], dict[str, object]]:
    return _menu_chicken_profile_lookup(_menu_chicken_profile_attrs())


def _profile_for_menu_row(row: pd.Series) -> dict[str, object] | None:
    key = tuple(str(row.get(col, "") or "").strip() for col in MENU_CHICKEN_PROFILE_KEY_COLUMNS)
    profile = _cached_menu_chicken_profile_lookup().get(key)
    if profile is None:
        profile = _effective_menu_chicken_profile(pd.Series(_suggest_menu_chicken_profile(key[-1])))
    return profile


def _profile_final_values(profile: dict[str, object]) -> tuple[str, str, str] | None:
    allowed_types = [str(value) for value in profile.get("allowed_types", []) if str(value or "").strip()]
    allowed_sizes = [str(value) for value in profile.get("allowed_sizes", []) if str(value or "").strip()]
    default_type = str(profile.get("default_type", "") or "").strip()
    default_size = str(profile.get("default_size", "") or "").strip()
    apply_option_type = bool(profile.get("apply_option_type", False))
    if not allowed_types:
        return None
    if apply_option_type and CHICKEN_TYPE_NONE not in allowed_types:
        return None
    if CHICKEN_TYPE_NONE in allowed_types:
        return CHICKEN_TYPE_NONE, CHICKEN_SIZE_NONE, "0"
    chicken_type = default_type or (allowed_types[0] if len(allowed_types) == 1 else "")
    chicken_size = default_size or (allowed_sizes[0] if len(allowed_sizes) == 1 else "")
    if not chicken_type or not chicken_size:
        return None
    return chicken_type, chicken_size, _usage_for(chicken_type, chicken_size)


def _profile_constrained_values_for_row(row: pd.Series, profile: dict[str, object]) -> tuple[str, str, str] | None:
    allowed_types = [str(value) for value in profile.get("allowed_types", []) if str(value or "").strip()]
    if not allowed_types or bool(profile.get("apply_option_type", False)):
        return None
    if CHICKEN_TYPE_NONE in allowed_types:
        return CHICKEN_TYPE_NONE, CHICKEN_SIZE_NONE, "0"
    if len(allowed_types) != 1:
        return None
    chicken_type = str(profile.get("default_type", "") or "").strip() or allowed_types[0]
    if not chicken_type:
        return None
    allowed_sizes = [str(value) for value in profile.get("allowed_sizes", []) if str(value or "").strip()]
    current_size = str(row.get("사이즈", "") or "").strip()
    default_size = str(profile.get("default_size", "") or "").strip()
    if allowed_sizes and current_size not in allowed_sizes:
        chicken_size = default_size or (allowed_sizes[0] if len(allowed_sizes) == 1 else "")
    else:
        chicken_size = current_size or default_size or (allowed_sizes[0] if len(allowed_sizes) == 1 else "")
    if not chicken_size:
        return None
    usage = _usage_for(chicken_type, chicken_size)
    if not usage:
        return None
    return chicken_type, chicken_size, usage


def _apply_menu_chicken_profile_final(
    left_joined: pd.DataFrame,
    menu_chicken_profile_master: pd.DataFrame | None,
) -> pd.DataFrame:
    if left_joined.empty:
        return left_joined
    lookup = _menu_chicken_profile_lookup(menu_chicken_profile_master)
    if not lookup:
        return left_joined
    out = left_joined.copy()
    role = out.get("line_role", pd.Series("", index=out.index)).astype(str).str.strip()
    target = role.isin(["main", "option", "side"])
    if not target.any():
        return out
    for idx, row in out[target].iterrows():
        key = tuple(str(row.get(col, "") or "").strip() for col in MENU_CHICKEN_PROFILE_KEY_COLUMNS)
        profile = lookup.get(key)
        if not profile:
            continue
        values = _profile_final_values(profile) or _profile_constrained_values_for_row(row, profile)
        if values is None:
            continue
        chicken_type, chicken_size, usage = values
        qty = pd.to_numeric(pd.Series([row.get("qty", "")]), errors="coerce").fillna(0).iloc[0]
        if qty == 0:
            qty = 1
        usage_num = pd.to_numeric(pd.Series([usage]), errors="coerce").fillna(0).iloc[0]
        usage_total = float(qty) * float(usage_num)
        out.at[idx, "닭유형"] = chicken_type
        out.at[idx, "사이즈"] = chicken_size
        out.at[idx, "닭유형_판정"] = "메뉴프로필"
        out.at[idx, "사이즈_판정"] = "메뉴프로필"
        out.at[idx, HALF_COMBO_COLUMN] = ""
        out.at[idx, HALF_SLOT1_COLUMN] = ""
        out.at[idx, HALF_SLOT2_COLUMN] = ""
        out.at[idx, CHICKEN_RATIO_APPLIED_COLUMN] = ""
        if str(row.get("line_role", "") or "").strip() != "main":
            continue
        out.at[idx, CHICKEN_USAGE_COLUMN] = usage
        out.at[idx, CHICKEN_USAGE_TOTAL_COLUMN] = _format_number(usage_total)
        if chicken_type == CHICKEN_TYPE_NONE:
            out.at[idx, BONE_USAGE_COLUMN] = "0"
            out.at[idx, BONELESS_USAGE_COLUMN] = "0"
            out.at[idx, BONE_USAGE_TOTAL_COLUMN] = "0"
            out.at[idx, BONELESS_USAGE_TOTAL_COLUMN] = "0"
        elif chicken_type == "뼈닭":
            out.at[idx, BONE_USAGE_COLUMN] = usage
            out.at[idx, BONELESS_USAGE_COLUMN] = "0"
            out.at[idx, BONE_USAGE_TOTAL_COLUMN] = _format_number(usage_total)
            out.at[idx, BONELESS_USAGE_TOTAL_COLUMN] = "0"
        elif chicken_type == "순살":
            out.at[idx, BONE_USAGE_COLUMN] = "0"
            out.at[idx, BONELESS_USAGE_COLUMN] = usage
            out.at[idx, BONE_USAGE_TOTAL_COLUMN] = "0"
            out.at[idx, BONELESS_USAGE_TOTAL_COLUMN] = _format_number(usage_total)
    return out


def _is_material_usage_manual_column(column: object) -> bool:
    return str(column or "").strip().endswith(_MATERIAL_USAGE_MANUAL_SUFFIX)


def _material_name_from_usage_column(column: object) -> str:
    text = str(column or "").strip()
    if text.endswith("_manual"):
        text = text[: -len("_manual")]
    if text.endswith("사용량"):
        text = text[: -len("사용량")]
    return text.strip()


def _manager_input_columns(extra_manual_columns: list[str] | None = None) -> list[str]:
    columns = list(MANAGER_INPUT_COLUMNS)
    insert_at = columns.index("수익률_manual")
    for column in extra_manual_columns or []:
        if column in columns:
            continue
        if not _is_material_usage_manual_column(column):
            continue
        columns.insert(insert_at, column)
        insert_at += 1
    return columns


def _option_material_input_columns(extra_manual_columns: list[str] | None = None) -> list[str]:
    columns = list(OPTION_MATERIAL_INPUT_COLUMNS)
    insert_at = columns.index("메모")
    for column in extra_manual_columns or []:
        if column in columns:
            continue
        if not _is_material_usage_manual_column(column):
            continue
        columns.insert(insert_at, column)
        insert_at += 1
    return columns


def _menu_weight_input_columns(extra_manual_columns: list[str] | None = None) -> list[str]:
    columns = list(MENU_WEIGHT_INPUT_COLUMNS)
    insert_at = columns.index("메모")
    for column in extra_manual_columns or []:
        if column in columns:
            continue
        if not _is_material_usage_manual_column(column):
            continue
        columns.insert(insert_at, column)
        insert_at += 1
    return columns


def _manager_edit_columns(columns: list[str] | pd.Index) -> list[str]:
    out = list(_MANAGER_INPUT_EDIT_COLUMNS)
    for column in columns:
        text = str(column or "").strip()
        if text not in out and _is_material_usage_manual_column(text):
            out.insert(max(len(out) - 2, 0), text)
    return out


def _option_material_edit_columns(columns: list[str] | pd.Index) -> list[str]:
    out = ["메모"]
    for column in columns:
        text = str(column or "").strip()
        if text not in out and _is_material_usage_manual_column(text):
            out.insert(max(len(out) - 1, 0), text)
    return out


def _menu_weight_edit_columns(columns: list[str] | pd.Index) -> list[str]:
    out = ["닭사용량_manual", "우거지사용량_manual", "순살추가사용량_manual", "메모"]
    for column in columns:
        text = str(column or "").strip()
        if text not in out and _is_material_usage_manual_column(text):
            out.insert(max(len(out) - 1, 0), text)
    return out


def _normalize_option_combo_key_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if CHICKEN_OPTION_KEY_COLUMN not in out.columns and OPTION_COMBO_COLUMN in out.columns:
        out[CHICKEN_OPTION_KEY_COLUMN] = out[OPTION_COMBO_COLUMN].map(_chicken_option_key_from_combo)
    elif CHICKEN_OPTION_KEY_COLUMN in out.columns and OPTION_COMBO_COLUMN in out.columns:
        missing_key = out[CHICKEN_OPTION_KEY_COLUMN].fillna("").astype(str).str.strip().eq("")
        if missing_key.any():
            out.loc[missing_key, CHICKEN_OPTION_KEY_COLUMN] = out.loc[missing_key, OPTION_COMBO_COLUMN].map(
                _chicken_option_key_from_combo
            )
    for column in (OPTION_COMBO_COLUMN, CHICKEN_OPTION_KEY_COLUMN):
        if column in out.columns:
            cleaned = out[column].fillna("").astype(str).str.strip()
            out[column] = cleaned.where(cleaned.ne(""), OPTION_COMBO_NONE)
    return out


def _combine_material_usage(row: pd.Series, material_columns: list[tuple[str, str]]) -> str:
    pairs = []
    for original_col, merged_col in material_columns:
        material = _material_name_from_usage_column(original_col)
        value = str(row.get(merged_col, "") or "").strip()
        if material and value:
            pairs.append(f"{material}={value}")
    return " | ".join(pairs)


def _remove_chicken_material_usage(value: object) -> str:
    parts = [part.strip() for part in str(value or "").split("|") if part.strip()]
    return " | ".join(part for part in parts if not part.startswith("닭="))


def _review_menu_name_fallback(row: pd.Series) -> str:
    for col in ("menu_name", "item_name"):
        value = str(row.get(col, "") or "").strip()
        if value:
            return value
    return ""


def _infer_chicken_attrs_from_group(group: pd.DataFrame, menu_profile: dict[str, object] | None = None) -> dict[str, str]:
    work = group.copy().fillna("")
    role = work.get("line_role", pd.Series("", index=work.index)).astype(str)
    work = work[~role.isin(["fee", "discount", "side"])]
    main_rows = work[work.get("line_role", pd.Series("", index=work.index)).astype(str).str.strip().eq("main")]

    def choose_size(values: list[str], label: str, *, prefer_largest: bool = False) -> str:
        unique = _unique_nonempty(values)
        if len(unique) == 1:
            return unique[0]
        if len(unique) > 1 and prefer_largest:
            rank = {"1인": 1, "2인": 2, "소": 3, "중": 4, "대": 5}
            ranked = sorted(unique, key=lambda value: rank.get(value, 0), reverse=True)
            return ranked[0]
        if len(unique) > 1:
            conflicts.append(f"{label}충돌:{'|'.join(unique)}")
        return ""

    profile_option_sizes: list[str] = []
    profile_paid_option_sizes: list[str] = []
    for _, row in work.iterrows():
        if str(row.get("line_role", "") or "").strip() == "main":
            continue
        item_name = str(row.get("item_name", "") or "")
        if str(row.get("option_kind", "") or "").strip() != OPTION_KIND_SIZE:
            continue
        inferred_sizes = _infer_chicken_sizes(item_name)
        if not inferred_sizes:
            continue
        profile_option_sizes.extend(inferred_sizes)
        price = pd.to_numeric(pd.Series([row.get("total_price", "")]), errors="coerce").fillna(0).iloc[0]
        if float(price) > 0:
            profile_paid_option_sizes.extend(inferred_sizes)

    conflicts: list[str] = []
    if menu_profile:
        allowed_types = [str(value) for value in menu_profile.get("allowed_types", []) if str(value or "").strip()]
        allowed_sizes = [str(value) for value in menu_profile.get("allowed_sizes", []) if str(value or "").strip()]
        default_type = str(menu_profile.get("default_type", "") or "").strip()
        default_size = str(menu_profile.get("default_size", "") or "").strip()
        apply_option_type = bool(menu_profile.get("apply_option_type", False))
        prefer_explicit_size = default_type == "순살" and allowed_types == ["순살"]
        if allowed_types and (not apply_option_type or CHICKEN_TYPE_NONE in allowed_types):
            option_type_signals: list[str] = []
            option_size_signals: list[str] = []
            menu_size_signals: list[str] = []
            for _, row in work.iterrows():
                item_name = str(row.get("item_name", "") or "")
                if str(row.get("line_role", "") or "").strip() == "main":
                    menu_text = " ".join(str(row.get(col, "") or "") for col in ("menu_name", "std_menu_name", "item_name"))
                    menu_size_signals.extend(_infer_chicken_sizes(menu_text))
                    continue
                option_kind = str(row.get("option_kind", "") or "").strip()
                inferred_sizes = _infer_chicken_sizes(item_name)
                if option_kind == OPTION_KIND_SIZE or inferred_sizes:
                    option_size_signals.extend(inferred_sizes)
                if str(row.get("line_role", "") or "").strip() == "main":
                    continue
                if _ROLE_ADDON_RE.search(item_name):
                    continue
                option_type_signals.extend(_infer_chicken_types(item_name))
            if CHICKEN_TYPE_NONE in allowed_types:
                size_method = "메뉴프로필"
                chicken_type = CHICKEN_TYPE_NONE
                chicken_size = CHICKEN_SIZE_NONE
                usage = "0"
            else:
                chicken_type = default_type or (allowed_types[0] if len(allowed_types) == 1 else "")
                inferred_sizes = _unique_nonempty(option_size_signals) or _unique_nonempty(menu_size_signals)
                paid_explicit_size = choose_size(profile_paid_option_sizes, "사이즈", prefer_largest=True)
                option_explicit_size = choose_size(profile_option_sizes, "사이즈", prefer_largest=True)
                explicit_size = (paid_explicit_size or option_explicit_size) if prefer_explicit_size else ""
                size_method = "유료사이즈" if paid_explicit_size and prefer_explicit_size else ("선택" if option_explicit_size and prefer_explicit_size else "메뉴프로필")
                inferred_size = explicit_size or (inferred_sizes[0] if len(inferred_sizes) == 1 else "")
                if allowed_sizes and inferred_size not in allowed_sizes:
                    inferred_size = ""
                chicken_size = inferred_size or default_size or (allowed_sizes[0] if len(allowed_sizes) == 1 else "")
                usage = _usage_for(chicken_type, chicken_size)
            ignored_types = [value for value in _unique_nonempty(option_type_signals) if value not in allowed_types]
            reasons = [f"메뉴프로필_무시닭유형옵션:{'|'.join(ignored_types)}"] if ignored_types else []
            if not chicken_type:
                reasons.append("닭유형옵션없음")
            if not chicken_size:
                reasons.append("사이즈옵션없음")
            if chicken_type and chicken_size and not usage:
                reasons.append(f"사용량기준없음:{chicken_type}/{chicken_size}")
            return {
                "닭유형_auto": chicken_type,
                "사이즈_auto": chicken_size,
                "닭사용량_auto": usage,
                "닭유형_판정_auto": "메뉴프로필",
                "사이즈_판정_auto": size_method if chicken_size else "메뉴프로필",
                "반반조합_auto": "",
                "반반슬롯1_auto": "",
                "반반슬롯2_auto": "",
                "뼈비율_auto": "",
                "미해결사유_auto": " | ".join(_unique_nonempty(reasons)),
            }
    fixed_profiles = _unique_nonempty(
        [
            profile
            for _, main_row in main_rows.iterrows()
            for profile in [_locked_main_chicken_profile(main_row)]
            if profile is not None
        ]
    )
    if len(fixed_profiles) == 1:
        chicken_type, chicken_size, method = fixed_profiles[0]
        usage = "0" if chicken_type == CHICKEN_TYPE_NONE else _usage_for(chicken_type, chicken_size)
        return {
            "닭유형_auto": chicken_type,
            "사이즈_auto": chicken_size,
            "닭사용량_auto": usage,
            "닭유형_판정_auto": method,
            "사이즈_판정_auto": method,
            "반반조합_auto": "",
            "반반슬롯1_auto": "",
            "반반슬롯2_auto": "",
            "뼈비율_auto": "",
            "미해결사유_auto": "",
        }
    change_types: list[str] = []
    select_types: list[str] = []
    menu_types: list[str] = []
    ordered_option_type_signals: list[str] = []
    size_option_sizes: list[str] = []
    deciding_option_sizes: list[str] = []
    menu_sizes: list[str] = []
    menu_context_parts: list[str] = []
    for _, row in work.iterrows():
        item_name = str(row.get("item_name", "") or "")
        base_menu_text = " ".join(str(row.get(col, "") or "") for col in ("std_menu_name", "item_name"))
        if _has_any_token(base_menu_text, _CHICKEN_MENU_TOKENS):
            menu_text = " ".join(str(row.get(col, "") or "") for col in ("menu_name", "std_menu_name"))
        else:
            menu_text = str(row.get("std_menu_name", "") or "")
        menu_context_parts.append(menu_text)
        if str(row.get("line_role", "") or "") == "main":
            menu_types.extend(_infer_chicken_types(" ".join([menu_text, item_name])))
            menu_sizes.extend(_infer_chicken_sizes(" ".join([menu_text, item_name])))
            continue
        option_kind = str(row.get("option_kind", "") or "").strip()
        if option_kind == OPTION_KIND_CHICKEN_ADDON:
            continue
        if _ROLE_ADDON_RE.search(item_name):
            continue
        if _ROLE_CHANGE_RE.search(item_name):
            inferred = _infer_chicken_types(item_name)
            change_types.extend(inferred)
            ordered_option_type_signals.extend(inferred)
            if option_kind in CHICKEN_DECIDING_KINDS:
                deciding_option_sizes.extend(_infer_chicken_sizes(item_name))
            continue
        inferred = _infer_chicken_types(item_name)
        select_types.extend(inferred)
        if option_kind == OPTION_KIND_CHICKEN_TYPE:
            ordered_option_type_signals.extend(inferred)
        inferred_sizes = _infer_chicken_sizes(item_name)
        if option_kind == OPTION_KIND_SIZE or (not option_kind and inferred_sizes):
            size_option_sizes.extend(inferred_sizes)
        elif option_kind in CHICKEN_DECIDING_KINDS:
            deciding_option_sizes.extend(inferred_sizes)

    def choose(values: list[str], label: str) -> str:
        unique = _unique_nonempty(values)
        if len(unique) == 1:
            return unique[0]
        if len(unique) > 1:
            conflicts.append(f"{label}충돌:{'|'.join(unique)}")
        return ""

    menu_context = " ".join(menu_context_parts)
    is_non_chicken_menu = _has_any_token(menu_context, _NON_CHICKEN_MENU_TOKENS) and not _has_any_token(
        menu_context, _CHICKEN_MENU_TOKENS
    )
    if is_non_chicken_menu:
        # 닭을 안 쓰는 메뉴는 빈값이 아니라 '닭미사용'으로 명시한다.
        # 빈값으로 두면 판정 실패와 구분되지 않아 완결률을 셀 수 없다.
        return {
            "닭유형_auto": CHICKEN_TYPE_NONE,
            "사이즈_auto": CHICKEN_SIZE_NONE,
            "닭사용량_auto": "0",
            "닭유형_판정_auto": CHICKEN_METHOD_NONE,
            "사이즈_판정_auto": CHICKEN_METHOD_NONE,
            "반반조합_auto": "",
            "반반슬롯1_auto": "",
            "반반슬롯2_auto": "",
            "뼈비율_auto": "",
            "미해결사유_auto": "",
        }

    slot1, slot2, slot_reason = _infer_half_slots_detail(
        menu_context,
        menu_type_signals=menu_types,
        option_type_signals=[*change_types, *select_types],
        ordered_option_type_signals=ordered_option_type_signals,
    )
    half_combo = _half_combo_from_slots(slot1, slot2)
    half_ratio = _bone_ratio_from_slots(slot1, slot2)

    chicken_type = choose(change_types, "닭유형")
    type_method = "변경" if chicken_type else ""
    if not chicken_type:
        chicken_type = choose(select_types, "닭유형")
        type_method = "선택" if chicken_type else ""
    if not chicken_type:
        chicken_type = choose(menu_types, "닭유형")
        type_method = "메뉴명" if chicken_type else ""
    if not chicken_type and _has_any_token(menu_context, _CHICKEN_MENU_TOKENS):
        chicken_type = "뼈닭"
        type_method = "토큰기본"

    chicken_size = choose_size(profile_paid_option_sizes, "사이즈", prefer_largest=True)
    size_method = "유료사이즈" if chicken_size else ""
    if not chicken_size:
        chicken_size = choose_size(profile_option_sizes, "사이즈", prefer_largest=True)
        size_method = "선택" if chicken_size else ""
    if not chicken_size:
        chicken_size = choose(size_option_sizes, "사이즈")
        size_method = "선택" if chicken_size else ""
    if not chicken_size:
        chicken_size = choose(deciding_option_sizes, "사이즈")
        size_method = "선택" if chicken_size else ""
    if not chicken_size:
        chicken_size = choose(menu_sizes, "사이즈")
        size_method = "메뉴명" if chicken_size else ""

    if half_combo and half_ratio is not None:
        chicken_type = _chicken_type_from_slots(slot1, slot2)
        type_method = CHICKEN_METHOD_HALF_SLOT
        conflicts = [value for value in conflicts if not value.startswith("닭유형충돌:")]
    elif slot1 or slot2:
        chicken_type = ""
        type_method = CHICKEN_METHOD_HALF_SLOT

    reasons = list(conflicts)
    if slot_reason:
        reasons.append(slot_reason)
    if menu_profile:
        allowed_types = [str(value) for value in menu_profile.get("allowed_types", []) if str(value or "").strip()]
        allowed_sizes = [str(value) for value in menu_profile.get("allowed_sizes", []) if str(value or "").strip()]
        default_type = str(menu_profile.get("default_type", "") or "").strip()
        default_size = str(menu_profile.get("default_size", "") or "").strip()
        apply_option_type = bool(menu_profile.get("apply_option_type", False))
        if allowed_types and CHICKEN_TYPE_NONE in allowed_types:
            chicken_type = CHICKEN_TYPE_NONE
            chicken_size = CHICKEN_SIZE_NONE
            type_method = "메뉴프로필"
            size_method = "메뉴프로필"
            half_combo = ""
            slot1 = ""
            slot2 = ""
            half_ratio = None
        else:
            ignored_types = [value for value in _unique_nonempty([*change_types, *select_types]) if allowed_types and value not in allowed_types]
            if ignored_types:
                reasons.append(f"메뉴프로필_무시닭유형옵션:{'|'.join(ignored_types)}")
            if (not apply_option_type and default_type) or (allowed_types and chicken_type not in allowed_types):
                chicken_type = default_type or (allowed_types[0] if len(allowed_types) == 1 else "")
                type_method = "메뉴프로필"
                half_combo = ""
                slot1 = ""
                slot2 = ""
                half_ratio = None
            if allowed_sizes and chicken_size not in allowed_sizes:
                chicken_size = default_size or (allowed_sizes[0] if len(allowed_sizes) == 1 else "")
                size_method = "메뉴프로필"
            elif not chicken_size and default_size:
                chicken_size = default_size
                size_method = "메뉴프로필"
    usage = _usage_for(chicken_type, chicken_size)
    if half_combo and half_ratio is not None and chicken_size:
        usage = _usage_from_half_slots(chicken_size, slot1, slot2)[0]

    if not chicken_type:
        reasons.append("닭유형옵션없음")
    if not chicken_size:
        reasons.append("사이즈옵션없음")
    if chicken_type and chicken_size and not usage:
        reasons.append(f"사용량기준없음:{chicken_type}/{chicken_size}")
    return {
        "닭유형_auto": chicken_type,
        "사이즈_auto": chicken_size,
        "닭사용량_auto": usage,
        "닭유형_판정_auto": type_method or "미해결",
        "사이즈_판정_auto": size_method or "미해결",
        "반반조합_auto": half_combo,
        "반반슬롯1_auto": slot1,
        "반반슬롯2_auto": slot2,
        "뼈비율_auto": "" if half_ratio is None else _format_number(half_ratio),
        "미해결사유_auto": " | ".join(_unique_nonempty(reasons)),
    }


def _build_order_group_attrs(
    left_joined: pd.DataFrame,
    menu_chicken_profile_master: pd.DataFrame | None = None,
) -> pd.DataFrame:
    columns = [
        *ORDER_GROUP_COLUMNS,
        "std_menu_name",
        "대표메뉴명",
        "옵션조합",
        CHICKEN_OPTION_KEY_COLUMN,
        "주문건수",
        "판매수량",
        "매출합계",
        "order_context",
        "is_chicken_group",
        "has_main",
        "닭유형_auto",
        "사이즈_auto",
        "닭사용량_auto",
        "닭유형_판정_auto",
        "사이즈_판정_auto",
        "반반조합_auto",
        "반반슬롯1_auto",
        "반반슬롯2_auto",
        "뼈비율_auto",
        "미해결사유_auto",
    ]
    if left_joined.empty:
        return pd.DataFrame(columns=columns)

    work = _attach_profit_channel(_ensure_order_group_columns(left_joined)).fillna("")
    menu_profile_lookup = _menu_chicken_profile_lookup(menu_chicken_profile_master)
    for col in ["qty", "total_price"]:
        work[f"{col}_num"] = pd.to_numeric(work.get(col, pd.Series("", index=work.index)), errors="coerce").fillna(0)
    rows = []
    for group_key, group in work.groupby(ORDER_GROUP_COLUMNS, dropna=False, sort=False):
        non_fee = group[~group.get("line_role", pd.Series("", index=group.index)).isin(["fee", "discount"])].copy()
        if non_fee.empty:
            continue
        main = non_fee[non_fee["line_role"].eq("main")]
        parent = main.iloc[0] if len(main) else non_fee.iloc[0]
        std_menu_name = _first_clean_value(main.get("std_menu_name", pd.Series(dtype=str)))
        if not std_menu_name:
            std_menu_name = _first_clean_value(non_fee.get("std_menu_name", pd.Series(dtype=str)))
        representative = std_menu_name or _first_clean_value(main.get("item_name", pd.Series(dtype=str)))
        if not representative:
            representative = _first_clean_value(non_fee.get("menu_name", pd.Series(dtype=str)))
        if not representative:
            representative = _first_clean_value(non_fee.get("item_name", pd.Series(dtype=str)))
        if not std_menu_name:
            std_menu_name = representative
        option_rows = non_fee[non_fee["line_role"].eq("option")]
        menu_profile_key = (
            str(group_key[0] if isinstance(group_key, tuple) else non_fee.get("source", pd.Series([""])).iloc[0] or "").strip(),
            str(group_key[1] if isinstance(group_key, tuple) and len(group_key) > 1 else non_fee.get("brand", pd.Series([""])).iloc[0] or "").strip(),
            str(group_key[2] if isinstance(group_key, tuple) and len(group_key) > 2 else non_fee.get("store", pd.Series([""])).iloc[0] or "").strip(),
            str(std_menu_name or "").strip(),
        )
        attrs = _infer_chicken_attrs_from_group(non_fee, menu_profile=menu_profile_lookup.get(menu_profile_key))
        final_size = str(attrs.get("사이즈_auto", "") or "").strip()
        has_matching_size_option = _has_size_option(option_rows, final_size)
        display_option_rows = option_rows[
            ~option_rows.apply(
                lambda row: _is_redundant_default_size_option(row, final_size, has_matching_size_option),
                axis=1,
            )
        ].copy()
        option_names = [
            str(row.get("item_name", "")).strip() for _, row in display_option_rows.iterrows()
        ]
        option_combo = _unique_join(option_names) or OPTION_COMBO_NONE
        # 닭옵션키는 사이즈/닭유형 옵션만 담는다. 맛·음료·리뷰·요청사항이 섞이면
        # 키가 3,740가지로 폭발해 사람이 채울 수 없다.
        chicken_option_names = sorted(
            _unique_nonempty(
                [
                    str(row.get("item_name", "")).strip()
                    for _, row in display_option_rows.iterrows()
                    if str(row.get("option_kind", "") or "").strip() in CHICKEN_DECIDING_KINDS
                ]
            )
        )
        chicken_option_key = " | ".join(chicken_option_names) or OPTION_COMBO_NONE
        context = " ".join(
            non_fee[[c for c in ["std_menu_name", "menu_name", "item_name"] if c in non_fee.columns]].astype(str).agg(" ".join, axis=1).tolist()
        )
        menu_context = " ".join([
            str(std_menu_name or ""),
            str(representative or ""),
            str(parent.get("item_name", "") or ""),
        ])
        main_qty = pd.to_numeric(main.get("qty", pd.Series(dtype=str)), errors="coerce").fillna(0).max() if len(main) else 0
        if not main_qty or pd.isna(main_qty):
            main_qty = pd.to_numeric(non_fee["qty"], errors="coerce").fillna(0).max()
        rows.append({
            **dict(zip(ORDER_GROUP_COLUMNS, group_key)),
            "std_menu_name": std_menu_name,
            "대표메뉴명": representative,
            "옵션조합": option_combo,
            CHICKEN_OPTION_KEY_COLUMN: chicken_option_key,
            "주문건수": "1",
            "판매수량": _format_number(main_qty or 1),
            "매출합계": _format_number(non_fee["total_price_num"].sum()),
            "order_context": " | ".join(_unique_nonempty(
                (
                    non_fee.get("menu_name", pd.Series("", index=non_fee.index)).astype(str)
                    + " "
                    + non_fee["std_menu_name"].astype(str)
                    + " "
                    + non_fee["item_name"].astype(str)
                ).tolist()
            )),
            "is_chicken_group": bool(
                _has_any_token(menu_context, _CHICKEN_MENU_TOKENS)
            ),
            "has_main": bool(len(main)),
            **attrs,
        })
    result = pd.DataFrame(rows, columns=columns)
    return _repair_multi_main_order_sequence_attrs(work, result).reindex(columns=columns, fill_value="")


def _sort_frame_by_item_seq(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    return frame.assign(_sort_seq=pd.to_numeric(frame.get("item_seq", pd.Series("", index=frame.index)), errors="coerce")).sort_values(
        ["_sort_seq", "item_seq"],
        kind="mergesort",
    ).drop(columns=["_sort_seq"], errors="ignore")


def _single_inferred_chicken_type(value: object) -> str:
    signals = _unique_nonempty(_infer_chicken_types(value))
    return signals[0] if len(signals) == 1 else ""


def _single_inferred_chicken_size(value: object) -> str:
    signals = _unique_nonempty(_infer_chicken_sizes(value))
    return signals[0] if len(signals) == 1 else ""


def _repair_multi_main_order_sequence_attrs(source_rows: pd.DataFrame, group_attrs: pd.DataFrame) -> pd.DataFrame:
    """OKPOS 다중 메인 주문에서 한 부모에 몰린 닭 결정 옵션을 메인 순서대로 나눈다."""
    if source_rows.empty or group_attrs.empty:
        return group_attrs
    required = {"source", "brand", "store", "sale_date", "order_id", "menu_seq", "item_seq", "line_role", "option_kind", "item_name"}
    if not required.issubset(source_rows.columns) or not set(ORDER_GROUP_COLUMNS).issubset(group_attrs.columns):
        return group_attrs

    out = group_attrs.copy()
    order_columns = ["source", "brand", "store", "sale_date", "order_id"]
    repaired = 0
    for order_key, order in source_rows.groupby(order_columns, dropna=False, sort=False):
        main_rows = _sort_frame_by_item_seq(
            order[order.get("line_role", pd.Series("", index=order.index)).astype(str).str.strip().eq("main")]
        )
        if len(main_rows) < 2:
            continue
        main_rows = main_rows[
            main_rows.apply(
                lambda row: bool(
                    _main_row_chicken_profile(row)
                    or _has_any_token(
                        " ".join(str(row.get(col, "") or "") for col in ("menu_name", "std_menu_name", "item_name")),
                        _CHICKEN_MENU_TOKENS,
                    )
                ),
                axis=1,
            )
        ]
        if len(main_rows) < 2:
            continue
        main_text = " ".join(
            " ".join(str(row.get(col, "") or "") for col in ("menu_name", "std_menu_name", "item_name"))
            for _, row in main_rows.iterrows()
        )
        if "반반" in main_text:
            continue

        options = _sort_frame_by_item_seq(
            order[
                order.get("line_role", pd.Series("", index=order.index)).astype(str).str.strip().eq("option")
                & order.get("option_kind", pd.Series("", index=order.index)).astype(str).str.strip().isin(CHICKEN_DECIDING_KINDS)
            ]
        )
        type_options: list[tuple[str, str]] = []
        size_options: list[tuple[str, str]] = []
        for _, option in options.iterrows():
            item_name = str(option.get("item_name", "") or "").strip()
            option_kind = str(option.get("option_kind", "") or "").strip()
            if option_kind == OPTION_KIND_CHICKEN_TYPE:
                chicken_type = _single_inferred_chicken_type(item_name)
                if chicken_type:
                    type_options.append((item_name, chicken_type))
            elif option_kind == OPTION_KIND_SIZE:
                chicken_size = _single_inferred_chicken_size(item_name)
                if chicken_size:
                    size_options.append((item_name, chicken_size))

        if len(type_options) != len(main_rows):
            continue
        size_values: list[tuple[str, str]] = []
        if len(size_options) == len(main_rows):
            size_values = size_options
        else:
            unique_sizes = _unique_nonempty([value for _, value in size_options])
            if len(unique_sizes) == 1:
                size_values = [(size_options[0][0], unique_sizes[0])] * len(main_rows)
        if not size_values:
            continue

        for (_, main_row), (type_name, chicken_type), (size_name, chicken_size) in zip(
            main_rows.iterrows(),
            type_options,
            size_values,
        ):
            usage = _usage_for(chicken_type, chicken_size)
            if not usage:
                continue
            group_key = tuple(str(main_row.get(col, "") or "").strip() for col in ORDER_GROUP_COLUMNS)
            mask = pd.Series(True, index=out.index)
            for col, value in zip(ORDER_GROUP_COLUMNS, group_key):
                mask &= out.get(col, pd.Series("", index=out.index)).fillna("").astype(str).str.strip().eq(value)
            if not mask.any():
                continue
            deciding_key = " | ".join(_unique_nonempty([size_name, type_name]))
            out.loc[mask, CHICKEN_OPTION_KEY_COLUMN] = deciding_key
            out.loc[mask, "닭유형_auto"] = chicken_type
            out.loc[mask, "사이즈_auto"] = chicken_size
            out.loc[mask, "닭사용량_auto"] = usage
            out.loc[mask, "닭유형_판정_auto"] = CHICKEN_METHOD_ORDER_SEQUENCE
            out.loc[mask, "사이즈_판정_auto"] = CHICKEN_METHOD_ORDER_SEQUENCE
            out.loc[mask, "반반조합_auto"] = ""
            out.loc[mask, "반반슬롯1_auto"] = ""
            out.loc[mask, "반반슬롯2_auto"] = ""
            out.loc[mask, "뼈비율_auto"] = ""
            out.loc[mask, "미해결사유_auto"] = ""
            repaired += 1
    if repaired:
        logger.info("다중 메인 닭옵션 주문순서매칭 보정: %d그룹", repaired)
    return out


def _order_sequence_deciding_option_indexes(work: pd.DataFrame, order_keys: list[str]) -> set[object]:
    """메인 순서대로 나눌 수 있는 닭 결정 옵션 index 집합."""
    if work.empty:
        return set()
    required = {*order_keys, "item_seq", "line_role", "option_kind", "item_name"}
    if not required.issubset(work.columns):
        return set()

    indexes: set[object] = set()
    for _, order in work.groupby(order_keys, dropna=False, sort=False):
        main_rows = _sort_frame_by_item_seq(order[order["line_role"].astype(str).str.strip().eq("main")])
        if len(main_rows) < 2:
            continue
        main_rows = main_rows[
            main_rows.apply(
                lambda row: bool(
                    _main_row_chicken_profile(row)
                    or _has_any_token(
                        " ".join(str(row.get(col, "") or "") for col in ("menu_name", "std_menu_name", "item_name")),
                        _CHICKEN_MENU_TOKENS,
                    )
                ),
                axis=1,
            )
        ]
        if len(main_rows) < 2:
            continue
        main_text = " ".join(
            " ".join(str(row.get(col, "") or "") for col in ("menu_name", "std_menu_name", "item_name"))
            for _, row in main_rows.iterrows()
        )
        if "반반" in main_text:
            continue
        options = _sort_frame_by_item_seq(
            order[
                order["line_role"].astype(str).str.strip().eq("option")
                & order["option_kind"].astype(str).str.strip().isin(CHICKEN_DECIDING_KINDS)
            ]
        )
        type_indexes: list[object] = []
        size_indexes: list[object] = []
        size_values: list[str] = []
        for idx, option in options.iterrows():
            item_name = str(option.get("item_name", "") or "").strip()
            option_kind = str(option.get("option_kind", "") or "").strip()
            if option_kind == OPTION_KIND_CHICKEN_TYPE and _single_inferred_chicken_type(item_name):
                type_indexes.append(idx)
            elif option_kind == OPTION_KIND_SIZE:
                chicken_size = _single_inferred_chicken_size(item_name)
                if chicken_size:
                    size_indexes.append(idx)
                    size_values.append(chicken_size)
        if len(type_indexes) != len(main_rows):
            continue
        if len(size_indexes) != len(main_rows) and len(_unique_nonempty(size_values)) != 1:
            continue
        indexes.update(type_indexes)
        indexes.update(size_indexes)
    return indexes


def _chicken_option_key_from_combo(option_combo: object) -> str:
    """옵션조합 문자열에서 닭 결정 옵션만 남겨 닭옵션키로 환산한다.

    13번 입력표 키를 옵션조합 → 닭옵션키로 바꿀 때 기존 수기 입력을 이관하는 데 쓴다.
    """
    text = str(option_combo or "").strip()
    if not text or text in {OPTION_COMBO_NONE, MANAGER_DEFAULT_OPTION_COMBO}:
        return text or OPTION_COMBO_NONE
    parts = [part.strip() for part in text.split("|") if part.strip()]
    chicken_parts = [
        part
        for part in parts
        if _suggest_option_kind(part, "option") in CHICKEN_DECIDING_KINDS
        and not _looks_like_chicken_addon_option(part)
    ]
    return " | ".join(sorted(_unique_nonempty(chicken_parts))) or OPTION_COMBO_NONE


def _fold_manager_rows(work: pd.DataFrame, key_columns: list[str]) -> tuple[pd.DataFrame, int]:
    """같은 키로 모인 수기값을 하나로 접는다.

    값이 갈리면 자동으로 고르지 않는다. 최빈값 채택은 오답을 확정값으로 위장시키므로
    충돌은 비워두고 메모에 남겨 담당자가 직접 고르게 한다.
    """
    edit_columns = [col for col in work.columns if col.endswith("_manual")]
    rows = []
    conflicts = 0
    for key, group in work.groupby(key_columns, dropna=False, sort=False):
        row = dict(zip(key_columns, key if isinstance(key, tuple) else (key,)))
        notes: list[str] = []
        for col in edit_columns:
            values = _unique_nonempty([str(value or "").strip() for value in group[col].tolist()])
            if len(values) == 1:
                row[col] = values[0]
            elif len(values) > 1:
                row[col] = ""
                notes.append(f"이관충돌:{col}={'/'.join(values)}")
                conflicts += 1
            else:
                row[col] = ""
        existing_notes = _unique_nonempty(
            [str(value or "").strip() for value in group.get("메모", pd.Series(dtype=str)).tolist()]
        )
        row["메모"] = " | ".join([*existing_notes, *notes])
        rows.append(row)
    return pd.DataFrame(rows), conflicts


def _apply_std_menu_alias_to_keys(df: pd.DataFrame) -> pd.DataFrame:
    """저장된 입력표의 std_menu_name에도 alias를 적용한다.

    산출물 쪽만 alias를 걸고 입력표를 그냥 두면, `[한우 대창] 순살 곱도리탕`으로 저장된
    수기값이 `한우 순살 곱도리탕`을 찾는 주문 행과 더 이상 만나지 못해 조용히 규칙값으로
    떨어진다. 실측에서 이 누락만으로 닭 사용량이 103마리 빠졌다.
    """
    if df.empty or "std_menu_name" not in df.columns:
        return df
    out = df.copy()
    out["std_menu_name"] = out["std_menu_name"].map(_canonical_std_menu_name)
    return out


def _migrate_manager_input_to_chicken_key(manager: pd.DataFrame) -> pd.DataFrame:
    """옵션조합 키로 저장된 기존 수기값을 닭옵션키 + alias 적용 메뉴명으로 접는다."""
    if manager.empty:
        return manager
    work = manager.copy().fillna("")
    needs_key_migration = CHICKEN_OPTION_KEY_COLUMN not in work.columns
    if needs_key_migration:
        if OPTION_COMBO_COLUMN not in work.columns:
            return manager
        work[CHICKEN_OPTION_KEY_COLUMN] = work[OPTION_COMBO_COLUMN].map(_chicken_option_key_from_combo)
    work = _apply_std_menu_alias_to_keys(work)

    key_columns = [col for col in MANAGER_INPUT_KEY_COLUMNS if col in work.columns]
    if len(key_columns) != len(MANAGER_INPUT_KEY_COLUMNS):
        return work
    # alias 적용 후 키가 겹치지 않으면 접을 게 없다. 매 실행마다 재구성하지 않는다.
    if not needs_key_migration and not work.duplicated(subset=key_columns).any():
        return work

    folded, conflicts = _fold_manager_rows(work, key_columns)
    logger.info("13번 수기값 이관: %d행 → %d행 | 충돌 %d건", len(work), len(folded), conflicts)
    return folded


def _fill_size_from_menu_mode(merged: pd.DataFrame) -> pd.DataFrame:
    """사이즈 옵션이 없는 main은 같은 메뉴의 최빈 사이즈로 채운다.

    `[점심] 닭개장(공깃밥 포함)`처럼 사이즈 선택지가 아예 없는 메뉴는 옵션에서 사이즈를
    끌어올 방법이 없다. 빈칸으로 두면 영원히 미완이므로, 같은 std_menu_name에서 실제로
    가장 많이 팔린 사이즈를 쓰고 판정을 `메뉴최빈`으로 남겨 추적 가능하게 한다.
    닭 사용량 자체는 22번 표준중량에서 오므로 이 규칙이 사용량을 지어내지는 않는다.
    """
    if merged.empty or "사이즈" not in merged.columns:
        return merged
    out = merged.copy()
    role = out.get("line_role", pd.Series("", index=out.index)).astype(str)
    size = out["사이즈"].fillna("").astype(str).str.strip()
    chicken_type = out.get("닭유형", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    target = role.eq("main") & size.eq("") & chicken_type.ne("") & chicken_type.ne(CHICKEN_TYPE_NONE)
    if not target.any():
        return out

    qty = pd.to_numeric(out.get("qty", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    known = role.eq("main") & size.ne("") & size.ne(CHICKEN_SIZE_NONE)
    modes = (
        pd.DataFrame({"menu": out["std_menu_name"].astype(str), "size": size, "qty": qty})[known]
        .groupby(["menu", "size"])["qty"]
        .sum()
        .reset_index()
        .sort_values("qty", ascending=False)
        .drop_duplicates(subset="menu", keep="first")
        .set_index("menu")["size"]
        .to_dict()
    )
    filled = out.loc[target, "std_menu_name"].astype(str).map(modes).fillna("")
    applied = filled.ne("")
    if not applied.any():
        return out
    index = filled[applied].index
    out.loc[index, "사이즈"] = filled[applied]
    out.loc[index, "사이즈_판정"] = "메뉴최빈"
    logger.info("사이즈 메뉴최빈 보정: %d행", int(applied.sum()))
    return out


def _manual_coverage(df: pd.DataFrame) -> float:
    """판매수량 기준 닭 속성 수기 입력 커버리지.

    행 수로 세면 안 된다. 옵션조합 4,174행을 닭옵션키 467행으로 접으면 채워진 셀 수는
    당연히 줄어드는데, 그건 유실이 아니라 중복 제거다. 접기에 영향받지 않는 척도는
    "수기값이 붙은 판매수량의 비중"이다.
    """
    if df.empty or "닭유형_manual" not in df.columns:
        return 0.0
    qty = pd.to_numeric(df.get("판매수량", pd.Series("", index=df.index)), errors="coerce").fillna(0)
    total = float(qty.sum())
    if total <= 0:
        return 0.0
    return float(qty.where(_filled(df["닭유형_manual"]), 0).sum()) / total


def _guard_manager_input_loss(manager_input: pd.DataFrame) -> pd.DataFrame:
    """재생성된 13번이 기존 수기 입력을 잃었으면 덮어쓰기 전에 막는다.

    키를 바꾸면(옵션조합 → 닭옵션키, std_menu_name alias) 기존 행이 merge에서 빗나가
    빈 값으로 덮어써진다. 실제로 이 경로로 배민 30행의 닭 속성이 지워졌고, 백업이
    없었으면 복구할 수 없었다. 조용히 사라지는 대신 실패시킨다.
    """
    if not MANAGER_INPUT_OUTPUT_PATH.exists():
        return manager_input
    previous = _read_csv(MANAGER_INPUT_OUTPUT_PATH).fillna("")
    before = _manual_coverage(previous)
    after = _manual_coverage(manager_input)
    if before > 0 and after < before - 0.05:
        raise RuntimeError(
            f"13번 수기 입력 유실 감지: 판매수량 커버리지 {before:.1%} → {after:.1%}. "
            f"키 변경으로 기존 입력이 빗나갔을 수 있습니다. 덮어쓰지 않고 중단합니다. "
            f"백업 위치: {MANAGER_INPUT_OUTPUT_PATH.parent}"
        )
    if after < before:
        logger.warning("13번 수기 커버리지 감소: %.1f%% → %.1f%% (허용 범위)", before * 100, after * 100)
    return manager_input


def _backup_manager_input() -> None:
    if not MANAGER_INPUT_OUTPUT_PATH.exists():
        return
    existing = _read_csv(MANAGER_INPUT_OUTPUT_PATH)
    if CHICKEN_OPTION_KEY_COLUMN in existing.columns:
        return
    stamp = pendulum.now("Asia/Seoul").format("YYYYMMDD_HHmmss")
    backup_path = NEW_CLS_DIR / f"13_manager_input_backup_{stamp}.csv"
    _write_csv(existing, backup_path)
    logger.warning("13번 키 변경 전 백업 생성: %s", backup_path)


def _manager_input_attrs() -> pd.DataFrame:
    columns = [*MANAGER_INPUT_KEY_COLUMNS, *_MANAGER_INPUT_EDIT_COLUMNS]
    manager = _manual_or_legacy_sheet("예외보정", MANAGER_INPUT_OUTPUT_PATH).fillna("")
    if manager.empty:
        return pd.DataFrame(columns=columns)
    # 키 교체 전 파일이면 먼저 닭옵션키로 이관한다. 이관 없이 키 검사부터 하면
    # 기존 수기 입력 2,000여 건이 조용히 사라진다.
    manager = _migrate_manager_input_to_chicken_key(manager)
    if not set(MANAGER_INPUT_KEY_COLUMNS).issubset(manager.columns):
        return pd.DataFrame(columns=columns)
    extra_manual_columns = [
        str(col).strip()
        for col in manager.columns
        if str(col).strip() not in _MANAGER_INPUT_EDIT_COLUMNS and _is_material_usage_manual_column(col)
    ]
    columns = [*MANAGER_INPUT_KEY_COLUMNS, *_manager_edit_columns(_manager_input_columns(extra_manual_columns))]
    for col in columns:
        if col not in manager.columns:
            manager[col] = ""
        manager[col] = manager[col].fillna("").astype(str).str.strip()
    manager = _target_product_rows(manager)
    manager = _normalize_option_combo_key_frame(manager)
    return (
        manager.reindex(columns=columns, fill_value="")
        .drop_duplicates(subset=MANAGER_INPUT_KEY_COLUMNS, keep="last")
        .reset_index(drop=True)
    )


def _option_kind_master_attrs() -> pd.DataFrame:
    columns = [*OPTION_KIND_MASTER_KEY_COLUMNS, *OPTION_KIND_MASTER_EDIT_COLUMNS]
    master = _manual_or_legacy_sheet("옵션분류", OPTION_KIND_MASTER_OUTPUT_PATH).fillna("")
    if master.empty:
        return pd.DataFrame(columns=columns)
    if not set(OPTION_KIND_MASTER_KEY_COLUMNS).issubset(master.columns):
        return pd.DataFrame(columns=columns)
    for col in columns:
        if col not in master.columns:
            master[col] = ""
        master[col] = master[col].fillna("").astype(str).str.strip()
    master = _target_product_rows(master)
    return (
        master.reindex(columns=columns, fill_value="")
        .drop_duplicates(subset=OPTION_KIND_MASTER_KEY_COLUMNS, keep="last")
        .reset_index(drop=True)
    )


def _option_kind_row_keys(df: pd.DataFrame) -> list[tuple[str, str, str, str, str]]:
    def col(name: str) -> pd.Series:
        return df.get(name, pd.Series("", index=df.index)).fillna("").astype(str).str.strip()

    return list(
        zip(
            col("source"),
            col("brand"),
            col("store").map(_store_key),
            col("item_id"),
            col("item_name"),
        )
    )


def _std_menu_override_attrs() -> pd.DataFrame:
    frame = _read_manual_workbook_sheet(STD_MENU_OVERRIDE_SHEET_NAME).fillna("")
    columns = [*STD_MENU_OVERRIDE_KEY_COLUMNS, *STD_MENU_OVERRIDE_EDIT_COLUMNS]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    if not set(STD_MENU_OVERRIDE_KEY_COLUMNS).issubset(frame.columns):
        return pd.DataFrame(columns=columns)
    for col in columns:
        if col not in frame.columns:
            frame[col] = ""
        frame[col] = frame[col].fillna("").astype(str).str.strip()
    frame = _target_product_rows(frame)
    return (
        frame.reindex(columns=columns, fill_value="")
        .drop_duplicates(subset=STD_MENU_OVERRIDE_KEY_COLUMNS, keep="last")
        .reset_index(drop=True)
    )


def _std_menu_override_row_keys(df: pd.DataFrame) -> list[tuple[str, str, str, str, str, str, str]]:
    def col(name: str) -> pd.Series:
        return df.get(name, pd.Series("", index=df.index)).fillna("").astype(str).str.strip()

    return list(
        zip(
            col("source"),
            col("brand"),
            col("store").map(_store_key),
            col("line_role"),
            col("item_id"),
            col("item_name"),
            col("현재_std_menu_name").map(_canonical_std_menu_name),
        )
    )


def _apply_std_menu_name_overrides(left_joined: pd.DataFrame) -> pd.DataFrame:
    if left_joined.empty:
        return left_joined.reindex(columns=left_joined.columns, fill_value="")
    overrides = _std_menu_override_attrs()
    if overrides.empty:
        return left_joined.copy()
    override_values = {
        key: _canonical_std_menu_name(row.get("std_menu_name_manual", ""))
        for key, (_, row) in zip(_std_menu_override_row_keys(overrides), overrides.iterrows())
        if str(row.get("std_menu_name_manual", "") or "").strip()
    }
    if not override_values:
        return left_joined.copy()

    out = left_joined.copy()
    for col in ["source", "brand", "store", "line_role", "item_id", "item_name", "std_menu_name"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("").astype(str).str.strip()

    key_frame = out.assign(현재_std_menu_name=out["std_menu_name"])
    row_keys = _std_menu_override_row_keys(key_frame)
    explicit = pd.Series([override_values.get(key, "") for key in row_keys], index=out.index)

    group_values: dict[tuple[str, ...], str] = {}
    main_explicit = explicit.ne("") & out["line_role"].eq("main")
    if main_explicit.any() and set(ORDER_GROUP_COLUMNS).issubset(out.columns):
        main_rows = out[main_explicit].copy()
        main_rows["_manual_std"] = explicit[main_explicit]
        for group_key, group in main_rows.groupby(ORDER_GROUP_COLUMNS, dropna=False, sort=False):
            values = _unique_nonempty([str(value).strip() for value in group["_manual_std"].tolist()])
            if len(values) == 1:
                group_values[tuple(str(value) for value in group_key)] = values[0]
            elif values:
                logger.warning("메뉴명보정 main 충돌로 그룹 전파 제외: %s | %s", group_key, values)

    if group_values and set(ORDER_GROUP_COLUMNS).issubset(out.columns):
        group_keys = out[ORDER_GROUP_COLUMNS].fillna("").astype(str).agg(tuple, axis=1)
        propagated = pd.Series([group_values.get(key, "") for key in group_keys], index=out.index)
        out["std_menu_name"] = propagated.where(propagated.ne(""), out["std_menu_name"])

    out["std_menu_name"] = explicit.where(explicit.ne(""), out["std_menu_name"])
    return out


@lru_cache(maxsize=1)
def _std_menu_override_manual_name_lookup() -> dict[tuple[str, str, str, str, str, str], str]:
    overrides = _std_menu_override_attrs()
    if overrides.empty:
        return {}
    lookup: dict[tuple[str, str, str, str, str, str], str] = {}
    work = overrides.fillna("").copy()
    for col in ["source", "brand", "store", "line_role", "item_id", "item_name", "std_menu_name_manual"]:
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].astype(str).str.strip()
    work = work[work["std_menu_name_manual"].ne("")]
    for _, item in work.iterrows():
        key = (
            str(item.get("source", "") or "").strip(),
            str(item.get("brand", "") or "").strip(),
            _store_key(item.get("store", "")),
            str(item.get("line_role", "") or "").strip(),
            str(item.get("item_id", "") or "").strip(),
            str(item.get("item_name", "") or "").strip(),
        )
        if all(key):
            lookup[key] = _canonical_std_menu_name(item.get("std_menu_name_manual", ""))
    return lookup


def _std_menu_override_manual_name_for_row(row: pd.Series) -> str:
    source = str(row.get("source", "") or "").strip()
    brand = str(row.get("brand", "") or "").strip()
    store = _store_key(row.get("store", ""))
    role = str(row.get("line_role", "") or "").strip()
    item_id = str(row.get("item_id", "") or "").strip()
    item_name = str(row.get("item_name", "") or "").strip()
    return _std_menu_override_manual_name_lookup().get((source, brand, store, role, item_id, item_name), "")


def _build_std_menu_override_input(left_joined: pd.DataFrame) -> pd.DataFrame:
    previous = _std_menu_override_attrs()
    if left_joined.empty:
        return previous.reindex(columns=STD_MENU_OVERRIDE_COLUMNS, fill_value="")

    work = left_joined.copy().fillna("")
    for col in ["qty", "total_price"]:
        work[f"{col}_num"] = pd.to_numeric(work.get(col, pd.Series("", index=work.index)), errors="coerce").fillna(0)
    for col in ["source", "brand", "store", "line_role", "item_id", "item_name", "std_menu_name", "menu_name", "order_id"]:
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].fillna("").astype(str).str.strip()
    work["현재_std_menu_name"] = work["std_menu_name"].map(_canonical_std_menu_name)

    grouped = (
        work.groupby(STD_MENU_OVERRIDE_KEY_COLUMNS, dropna=False, sort=False)
        .agg(
            대표주문메뉴명=("menu_name", _first_clean_value),
            주문건수=("order_id", "nunique"),
            판매수량=("qty_num", "sum"),
            매출합계=("total_price_num", "sum"),
        )
        .reset_index()
    )
    for col in ["주문건수", "판매수량", "매출합계"]:
        grouped[col] = grouped[col].map(_format_number)

    if previous.empty:
        for col in STD_MENU_OVERRIDE_EDIT_COLUMNS:
            grouped[col] = ""
    else:
        previous_keys = _std_menu_override_row_keys(previous)
        kept = {
            col: dict(zip(previous_keys, previous[col].astype(str).str.strip()))
            for col in STD_MENU_OVERRIDE_EDIT_COLUMNS
        }
        grouped_keys = _std_menu_override_row_keys(grouped)
        for col in STD_MENU_OVERRIDE_EDIT_COLUMNS:
            grouped[col] = [kept[col].get(key, "") for key in grouped_keys]
        grouped_key_set = set(grouped_keys)
        preserved = previous[
            previous.apply(
                lambda row: tuple(str(value) for value in _std_menu_override_row_keys(pd.DataFrame([row]))[0]) not in grouped_key_set
                and any(str(row.get(col, "") or "").strip() for col in STD_MENU_OVERRIDE_EDIT_COLUMNS),
                axis=1,
            )
        ]
        if not preserved.empty:
            grouped = pd.concat(
                [grouped.reindex(columns=STD_MENU_OVERRIDE_COLUMNS, fill_value=""), preserved.reindex(columns=STD_MENU_OVERRIDE_COLUMNS, fill_value="")],
                ignore_index=True,
                sort=False,
            )

    return grouped.reindex(columns=STD_MENU_OVERRIDE_COLUMNS, fill_value="")


def _attach_option_kind(left_joined: pd.DataFrame) -> pd.DataFrame:
    """규칙 제안값을 깔고 마스터의 확정값으로 덮는다.

    `option_kind`는 확정값이 있으면 확정값, 없으면 제안값(유효값)이다. 처음부터 확정값만
    쓰면 마스터가 비어 있는 첫 실행에서 모든 키가 무너져 담당자가 확인할 초안조차 못 본다.
    대신 `option_kind_출처`로 확정/제안을 구분하고, 완결률은 확정만 분자로 센다.
    """
    out = left_joined.copy()
    if out.empty:
        for col in ("option_kind", "option_kind_제안", "option_kind_출처", "재료명"):
            if col not in out.columns:
                out[col] = pd.Series(dtype=str)
        return out

    role = out.get("line_role", pd.Series("", index=out.index)).fillna("").astype(str)
    name = out.get("item_name", pd.Series("", index=out.index)).fillna("").astype(str)
    suggested = [_suggest_option_kind(n, r) for n, r in zip(name, role)]
    suggested_material = [_suggest_material_name(n, k) for n, k in zip(name, suggested)]

    master = _option_kind_master_attrs()
    confirmed_kind: dict[tuple[str, str, str, str, str], str] = {}
    confirmed_material: dict[tuple[str, str, str, str, str], str] = {}
    if not master.empty:
        for key, kind, material in zip(
            _option_kind_row_keys(master),
            master["option_kind_확정"],
            master["재료명_확정"],
        ):
            kind_text = str(kind or "").strip()
            if kind_text and kind_text != OPTION_KIND_UNSET:
                confirmed_kind[key] = kind_text
            material_text = str(material or "").strip()
            if material_text:
                confirmed_material[key] = material_text

    row_keys = _option_kind_row_keys(out)
    out["option_kind_제안"] = suggested
    out["option_kind"] = [
        _forced_option_kind(item, row_role, fallback, confirmed_kind.get(key, ""))
        for key, fallback, row_role, item in zip(row_keys, suggested, role, name)
    ]
    out["option_kind_출처"] = [
        "확정" if confirmed_kind.get(key) and kind == confirmed_kind.get(key) else "제안"
        for key, kind in zip(row_keys, out["option_kind"])
    ]
    out["재료명"] = [
        "" if kind == OPTION_KIND_CHICKEN_ADDON else confirmed_material.get(key, fallback)
        for key, fallback, kind in zip(row_keys, suggested_material, out["option_kind"])
    ]
    return out


def _menu_weight_master_attrs() -> pd.DataFrame:
    master = _manual_or_legacy_sheet("메뉴중량", MENU_WEIGHT_MASTER_OUTPUT_PATH).fillna("")
    if master.empty:
        return pd.DataFrame(columns=MENU_WEIGHT_MASTER_KEY_COLUMNS)
    if not set(MENU_WEIGHT_MASTER_KEY_COLUMNS).issubset(master.columns):
        return pd.DataFrame(columns=MENU_WEIGHT_MASTER_KEY_COLUMNS)
    for col in master.columns:
        master[col] = master[col].fillna("").astype(str).str.strip()
    master = _apply_std_menu_alias_to_keys(_target_product_rows(master))
    if not master.duplicated(subset=MENU_WEIGHT_MASTER_KEY_COLUMNS).any():
        return master.reset_index(drop=True)
    # source를 키에서 뺐거나 alias가 겹치면 같은 키가 여러 행이 된다. keep="last"로 자르면
    # 채워진 값을 빈 행이 덮을 수 있으므로, 비어 있지 않은 값을 살리는 fold를 쓴다.
    folded, conflicts = _fold_manager_rows(master, MENU_WEIGHT_MASTER_KEY_COLUMNS)
    logger.info("22번 키 축약: %d행 → %d행 | 충돌 %d건", len(master), len(folded), conflicts)
    return folded.reset_index(drop=True)


def _legacy_menu_weight_values() -> dict[tuple[str, str, str], dict[str, str]]:
    """17번(사이즈키/닭유형키/추가재료키)에 입력된 표준중량을 22번 키로 옮긴다.

    17번의 사이즈키/닭유형키는 22번의 사이즈/닭유형과 같은 어휘를 쓰고,
    (std_menu_name, 사이즈, 닭유형)으로 접었을 때 값 충돌이 없어 손실 없이 이관된다.
    """
    if not MENU_WEIGHT_INPUT_OUTPUT_PATH.exists():
        return {}
    legacy = _read_csv(MENU_WEIGHT_INPUT_OUTPUT_PATH).fillna("")
    if not {"std_menu_name", "사이즈키", "닭유형키"}.issubset(legacy.columns):
        return {}
    manual_columns = [col for col in legacy.columns if _is_material_usage_manual_column(col)]
    out: dict[tuple[str, str, str], dict[str, str]] = {}
    for _, row in legacy.iterrows():
        key = (
            # 17번은 alias 적용 전 메뉴명으로 저장돼 있다. 그대로 두면 22번 키와 못 만난다.
            _canonical_std_menu_name(row.get("std_menu_name", "")),
            str(row.get("사이즈키", "")).strip(),
            str(row.get("닭유형키", "")).strip(),
        )
        if not key[0]:
            continue
        bucket = out.setdefault(key, {})
        for col in manual_columns:
            value = str(row.get(col, "")).strip()
            if value and not bucket.get(col):
                bucket[col] = value
    return out


def _chicken_ratio_master_attrs() -> dict[tuple[str, ...], str]:
    """26번에 담당자가 입력한 뼈비율_manual만 읽는다. 나머지 컬럼은 매 실행 재산출된다."""
    master = _manual_or_legacy_sheet("뼈순살비율", CHICKEN_RATIO_MASTER_OUTPUT_PATH).fillna("")
    if master.empty:
        return {}
    if not set(CHICKEN_RATIO_MASTER_KEY_COLUMNS).issubset(master.columns):
        return {}
    if "뼈비율_manual" not in master.columns:
        return {}
    for col in master.columns:
        master[col] = master[col].fillna("").astype(str).str.strip()
    master = _apply_std_menu_alias_to_keys(_target_product_rows(master))
    out: dict[tuple[str, ...], str] = {}
    for _, row in master.iterrows():
        key = tuple(str(row.get(col, "")).strip() for col in CHICKEN_RATIO_MASTER_KEY_COLUMNS)
        value = str(row.get("뼈비율_manual", "")).strip()
        if value and not out.get(key):
            out[key] = value
    return out


def _parse_bone_ratio(value: object) -> float | None:
    """뼈비율을 0~1로 읽는다. 담당자가 70이라고 써도 0.7로 받는다."""
    text = str(value or "").strip().replace("%", "")
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    if parsed > 1.0:
        parsed = parsed / 100.0
    if parsed < 0.0 or parsed > 1.0:
        return None
    return parsed


def _build_chicken_ratio_master(left_joined: pd.DataFrame) -> pd.DataFrame:
    """뼈/순살 신호가 있는 주문에서 메뉴별 실측 비율을 뽑아 신호 없는 구간에 쓸 표를 만든다.

    분자를 신호 있는 주문으로만 한정하는 것이 핵심이다. 수기 입력값까지 섞으면
    담당자가 "전부 뼈닭"으로 뭉갠 값이 다시 비율의 근거가 되어 순환한다.
    """
    manual = _chicken_ratio_master_attrs()
    if left_joined.empty:
        return pd.DataFrame(columns=CHICKEN_RATIO_MASTER_COLUMNS)

    work = _profit_calculation_frame(left_joined).fillna("")
    work = work[work.get("line_role", pd.Series("", index=work.index)).eq("main")]
    work = work[work.get("닭유형", pd.Series("", index=work.index)).isin(["뼈닭", "순살", CHICKEN_TYPE_MIXED])]
    if work.empty:
        return pd.DataFrame(columns=CHICKEN_RATIO_MASTER_COLUMNS)

    for col in CHICKEN_RATIO_MASTER_KEY_COLUMNS:
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].fillna("").astype(str).str.strip()
    work["_qty"] = pd.to_numeric(work.get("qty", ""), errors="coerce").fillna(0)
    work["_total"] = pd.to_numeric(work.get("total_price", ""), errors="coerce").fillna(0)
    signal = work.get(CHICKEN_SIGNAL_COLUMN, pd.Series("", index=work.index)).astype(str).str.strip()
    work["_signal"] = signal.eq(CHICKEN_SIGNAL_PRESENT)
    work["_bone"] = work["_qty"].where(work["_signal"] & work["닭유형"].eq("뼈닭"), 0.0)
    work["_boneless"] = work["_qty"].where(work["_signal"] & work["닭유형"].eq("순살"), 0.0)
    work["_signal_qty"] = work["_qty"].where(work["_signal"], 0.0)
    work["_blind_qty"] = work["_qty"].where(~work["_signal"], 0.0)
    work["_blind_sales"] = work["_total"].where(~work["_signal"], 0.0)

    grouped = (
        work.groupby(CHICKEN_RATIO_MASTER_KEY_COLUMNS, dropna=False, sort=False)
        .agg(
            신호_판매수량=("_signal_qty", "sum"),
            뼈_판매수량=("_bone", "sum"),
            순살_판매수량=("_boneless", "sum"),
            무신호_판매수량=("_blind_qty", "sum"),
            무신호_매출=("_blind_sales", "sum"),
        )
        .reset_index()
    )

    # 폴백용 상위 집계. 메뉴×사이즈 표본이 얇을 때 사이즈를 접고, 그래도 얇으면 채널을 접는다.
    def ratio_table(keys: list[str]) -> dict[tuple[str, ...], tuple[float, float]]:
        agg = work.groupby(keys, dropna=False, sort=False).agg(
            bone=("_bone", "sum"), boneless=("_boneless", "sum")
        )
        return {
            (key if isinstance(key, tuple) else (key,)): (float(row.bone), float(row.boneless))
            for key, row in agg.iterrows()
        }

    menu_keys = ["source", "brand", "store", "std_menu_name"]
    pooled_keys = ["brand", "store", "std_menu_name"]
    by_menu = ratio_table(menu_keys)
    by_pooled = ratio_table(pooled_keys)
    menu_idx = [CHICKEN_RATIO_MASTER_KEY_COLUMNS.index(col) for col in menu_keys]
    pooled_idx = [CHICKEN_RATIO_MASTER_KEY_COLUMNS.index(col) for col in pooled_keys]

    def half_rule_ratio(name: object) -> float | None:
        text = str(name or "")
        if "반반" not in text:
            return None
        has_bone = "뼈" in text
        has_boneless = "순살" in text
        if has_boneless and not has_bone:
            return 0.0
        if has_bone and not has_boneless:
            return 1.0
        return 0.5

    def unanimous_ratio(bone_value: float, boneless_value: float) -> float | None:
        sample_value = bone_value + boneless_value
        if sample_value <= 0:
            return None
        if bone_value == 0 or boneless_value == 0:
            return bone_value / sample_value
        return None

    measured: list[str] = []
    applied: list[str] = []
    origins: list[str] = []
    for _, row in grouped.iterrows():
        key = tuple(str(row[col]).strip() for col in CHICKEN_RATIO_MASTER_KEY_COLUMNS)
        bone = float(row["뼈_판매수량"])
        boneless = float(row["순살_판매수량"])
        sample = bone + boneless
        measured.append(f"{bone / sample:.3f}" if sample > 0 else "")

        manual_ratio = _parse_bone_ratio(manual.get(key, ""))
        if manual_ratio is not None:
            applied.append(f"{manual_ratio:.3f}")
            origins.append(CHICKEN_RATIO_SOURCE_MANUAL)
            continue
        half_ratio = half_rule_ratio(row.get("std_menu_name", ""))
        if half_ratio is not None:
            applied.append(f"{half_ratio:.3f}")
            origins.append(CHICKEN_RATIO_SOURCE_HALF_RULE)
            continue
        if sample >= CHICKEN_RATIO_MIN_SAMPLE:
            applied.append(f"{bone / sample:.3f}")
            origins.append(CHICKEN_RATIO_SOURCE_MENU_SIZE)
            continue
        menu_bone, menu_boneless = by_menu.get(tuple(key[i] for i in menu_idx), (0.0, 0.0))
        if menu_bone + menu_boneless >= CHICKEN_RATIO_MIN_SAMPLE:
            applied.append(f"{menu_bone / (menu_bone + menu_boneless):.3f}")
            origins.append(CHICKEN_RATIO_SOURCE_MENU)
            continue
        pooled_bone, pooled_boneless = by_pooled.get(tuple(key[i] for i in pooled_idx), (0.0, 0.0))
        if pooled_bone + pooled_boneless >= CHICKEN_RATIO_MIN_SAMPLE:
            applied.append(f"{pooled_bone / (pooled_bone + pooled_boneless):.3f}")
            origins.append(CHICKEN_RATIO_SOURCE_POOLED)
            continue
        menu_unanimous = unanimous_ratio(menu_bone, menu_boneless)
        if menu_unanimous is not None:
            applied.append(f"{menu_unanimous:.3f}")
            origins.append(CHICKEN_RATIO_SOURCE_UNANIMOUS)
            continue
        pooled_unanimous = unanimous_ratio(pooled_bone, pooled_boneless)
        if pooled_unanimous is not None:
            applied.append(f"{pooled_unanimous:.3f}")
            origins.append(CHICKEN_RATIO_SOURCE_UNANIMOUS)
            continue
        applied.append("")
        origins.append(CHICKEN_RATIO_SOURCE_NONE)

    grouped["뼈비율_실측"] = measured
    grouped["뼈비율_manual"] = [manual.get(tuple(str(row[col]).strip() for col in CHICKEN_RATIO_MASTER_KEY_COLUMNS), "") for _, row in grouped.iterrows()]
    grouped["적용비율"] = applied
    grouped["비율출처"] = origins
    grouped["메모"] = ""
    for col in ("신호_판매수량", "뼈_판매수량", "순살_판매수량", "무신호_판매수량", "무신호_매출"):
        grouped[col] = grouped[col].map(_format_number)
    # 무신호가 없는 조합은 담당자가 볼 이유가 없다. 다만 실측 비율은 폴백에 쓰이므로
    # 집계 자체에서는 뺄 수 없고, 출력에서만 뒤로 민다.
    grouped = grouped.sort_values("무신호_매출", key=lambda s: pd.to_numeric(s, errors="coerce").fillna(0), ascending=False)
    return grouped.reindex(columns=CHICKEN_RATIO_MASTER_COLUMNS, fill_value="").reset_index(drop=True)


def _apply_chicken_ratio(left_joined: pd.DataFrame, ratio_master: pd.DataFrame) -> pd.DataFrame:
    """신호 없는 main 행의 닭유형을 '혼합'으로 바꾸고 사용용량을 비율 가중으로 다시 낸다.

    신호가 있는 행은 건드리지 않는다. 실제로 적혀 있던 값이 통계 추정보다 언제나 낫다.
    """
    out = left_joined.copy()
    if out.empty or ratio_master.empty:
        return out
    ratios: dict[tuple[str, ...], tuple[float, str]] = {}
    for _, row in ratio_master.iterrows():
        applied = _parse_bone_ratio(row.get("적용비율", ""))
        if applied is None:
            continue
        key = tuple(str(row.get(col, "")).strip() for col in CHICKEN_RATIO_MASTER_KEY_COLUMNS)
        ratios[key] = (applied, str(row.get("비율출처", "")).strip())
    if not ratios:
        return out

    role = out.get("line_role", pd.Series("", index=out.index)).astype(str).str.strip()
    signal = out.get(CHICKEN_SIGNAL_COLUMN, pd.Series("", index=out.index)).astype(str).str.strip()
    chicken_type = out.get("닭유형", pd.Series("", index=out.index)).astype(str).str.strip()
    target = role.eq("main") & signal.eq(CHICKEN_SIGNAL_ABSENT) & chicken_type.isin(["뼈닭", "순살"])
    if not target.any():
        return out

    key_frame = pd.DataFrame(
        {col: out.get(col, pd.Series("", index=out.index)).fillna("").astype(str).str.strip() for col in CHICKEN_RATIO_MASTER_KEY_COLUMNS}
    )
    sizes = out.get("사이즈", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()

    new_type: dict[int, str] = {}
    new_usage: dict[int, str] = {}
    new_method: dict[int, str] = {}
    new_ratio: dict[int, str] = {}
    for idx in out.index[target]:
        key = tuple(key_frame.at[idx, col] for col in CHICKEN_RATIO_MASTER_KEY_COLUMNS)
        found = ratios.get(key)
        if found is None:
            continue
        ratio, _origin = found
        size = sizes.at[idx]
        bone_usage = _usage_for("뼈닭", size)
        boneless_usage = _usage_for("순살", size)
        if not bone_usage or not boneless_usage:
            # 한쪽 환산값이 없으면 가중할 수 없다. 기존 값을 그대로 둔다.
            continue
        blended = ratio * float(bone_usage) + (1.0 - ratio) * float(boneless_usage)
        new_type[idx] = CHICKEN_TYPE_MIXED
        new_usage[idx] = f"{blended:g}"
        new_method[idx] = CHICKEN_METHOD_RATIO
        new_ratio[idx] = f"{ratio:.3f}"
    if not new_type:
        return out

    if CHICKEN_RATIO_APPLIED_COLUMN not in out.columns:
        out[CHICKEN_RATIO_APPLIED_COLUMN] = ""
    out.loc[list(new_type), "닭유형"] = pd.Series(new_type)
    out.loc[list(new_usage), CHICKEN_USAGE_COLUMN] = pd.Series(new_usage)
    out.loc[list(new_method), "닭유형_판정"] = pd.Series(new_method)
    out.loc[list(new_ratio), CHICKEN_RATIO_APPLIED_COLUMN] = pd.Series(new_ratio)
    line_qty = pd.to_numeric(out.get("qty", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    usage_per_menu = pd.to_numeric(out[CHICKEN_USAGE_COLUMN], errors="coerce")
    out[CHICKEN_USAGE_TOTAL_COLUMN] = (usage_per_menu * line_qty).map(
        lambda value: "" if pd.isna(value) else f"{float(value):g}"
    )
    out = _attach_chicken_usage_split_columns(out)
    logger.info("뼈:순살 비율추정 적용: %d행", len(new_type))
    return out


def _build_menu_weight_master(left_joined: pd.DataFrame) -> pd.DataFrame:
    """메뉴 x 사이즈 x 닭유형별 표준중량 마스터. 재료원가와 재고 loss 비교의 기준표."""
    previous = _menu_weight_master_attrs()
    manual_columns = _unique_nonempty(
        [f"{name}사용량_manual" for name in _MENU_WEIGHT_MASTER_DEFAULT_MATERIALS]
        + [col for col in previous.columns if _is_material_usage_manual_column(col)]
    )
    output_columns = [*MENU_WEIGHT_MASTER_BASE_COLUMNS, *manual_columns, "메모"]
    if left_joined.empty:
        return pd.DataFrame(columns=output_columns)

    work = left_joined.copy().fillna("")
    work = work[work.get("line_role", pd.Series("", index=work.index)).eq("main")]
    # 혼합은 뼈닭/순살 표준중량에서 파생되는 값이라 담당자가 따로 입력할 행이 아니다.
    # 여기 넣으면 22번 분모가 늘고 영원히 100%가 안 된다.
    work = work[work.get("닭유형", pd.Series("", index=work.index)).ne(CHICKEN_TYPE_MIXED)]
    if work.empty:
        return pd.DataFrame(columns=output_columns)
    work["qty_num"] = pd.to_numeric(work.get("qty", ""), errors="coerce").fillna(0)
    work["total_num"] = pd.to_numeric(work.get("total_price", ""), errors="coerce").fillna(0)
    for col in MENU_WEIGHT_MASTER_KEY_COLUMNS:
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].fillna("").astype(str).str.strip()

    grouped = (
        work.groupby(MENU_WEIGHT_MASTER_KEY_COLUMNS, dropna=False, sort=False)
        .agg(주문건수=("order_id", "nunique"), 판매수량=("qty_num", "sum"), 매출합계=("total_num", "sum"))
        .reset_index()
    )
    for col in ("주문건수", "판매수량", "매출합계"):
        grouped[col] = grouped[col].map(_format_number)

    def key_of(frame: pd.DataFrame) -> list[tuple[str, ...]]:
        return list(
            zip(*[frame[col].fillna("").astype(str).str.strip() for col in MENU_WEIGHT_MASTER_KEY_COLUMNS])
        )

    kept: dict[str, dict[tuple[str, ...], str]] = {}
    if not previous.empty:
        previous_keys = key_of(previous)
        for col in [*manual_columns, "메모"]:
            if col in previous.columns:
                kept[col] = dict(zip(previous_keys, previous[col].astype(str).str.strip()))

    # 17번 폴백은 22번이 아직 없는 최초 이관 때만 쓴다. 계속 켜두면 담당자가 틀린 값을
    # 일부러 지워도 17번에서 조용히 되살아나고, 22번이 authoritative가 아니게 된다.
    legacy = {} if MENU_WEIGHT_MASTER_OUTPUT_PATH.exists() else _legacy_menu_weight_values()
    grouped_keys = key_of(grouped)
    # 17번 폴백 조회용 (std_menu_name, 사이즈, 닭유형) 위치
    legacy_slice = [MENU_WEIGHT_MASTER_KEY_COLUMNS.index(col) for col in ("std_menu_name", "사이즈", "닭유형")]
    for col in [*manual_columns, "메모"]:
        existing = kept.get(col, {})
        values = []
        for key in grouped_keys:
            value = existing.get(key, "")
            if not value and col != "메모":
                legacy_key = tuple(key[i] for i in legacy_slice)
                value = legacy.get(legacy_key, {}).get(col, "")
            if not value and col == "닭사용량_manual":
                value = _usage_for(key[legacy_slice[2]], key[legacy_slice[1]])
            values.append(value)
        grouped[col] = values

    if not previous.empty:
        current_keys = set(grouped_keys)
        preserved = previous[
            previous.apply(
                lambda row: (
                    tuple(str(row.get(col, "") or "").strip() for col in MENU_WEIGHT_MASTER_KEY_COLUMNS) not in current_keys
                    and any(str(row.get(col, "") or "").strip() for col in manual_columns)
                ),
                axis=1,
            )
        ]
        if not preserved.empty:
            grouped = pd.concat(
                [grouped, preserved.reindex(columns=output_columns, fill_value="")],
                ignore_index=True,
                sort=False,
            )

    return grouped.reindex(columns=output_columns, fill_value="")


def _guess_material_unit(name: str) -> str:
    """재료명에서 단위를 추정한다. 담당자가 단위 칸까지 채우지 않게 하려는 것."""
    text = str(name or "")
    if "닭" in text and "다리" not in text:
        return "마리"
    if re.search(r"만두|사리|오뎅|떡|계란|주먹밥|새우전|피자|공기밥|당면", text):
        return "개"
    return "kg"


def _material_usage_volume(left_joined: pd.DataFrame) -> dict[str, float]:
    """재료별 등장 판매수량. 단가표를 영향 큰 순서로 정렬하는 데 쓴다."""
    if left_joined.empty:
        return {}
    left_joined = _profit_calculation_frame(left_joined)
    qty = pd.to_numeric(left_joined.get("qty", pd.Series("", index=left_joined.index)), errors="coerce").fillna(0)
    out: dict[str, float] = {}
    for column in (MENU_WEIGHT_USAGE_COLUMN, MATERIAL_USAGE_COLUMN, OPTION_MATERIAL_USAGE_COLUMN):
        if column not in left_joined.columns:
            continue
        for text, line_qty in zip(left_joined[column].fillna(""), qty):
            for material, _ in _parse_material_usage(str(text or "")):
                out[material] = out.get(material, 0.0) + float(line_qty)
    return out


def _guide_rows(df: pd.DataFrame, columns: list[str], limit: int = 8) -> str:
    """가이드에 붙일 미완 목록 표. 실제 남은 항목을 그대로 보여준다."""
    if df.empty:
        return "_남은 항목 없음_\n"
    view = df.reindex(columns=[col for col in columns if col in df.columns]).head(limit)
    header = "| " + " | ".join(view.columns) + " |"
    sep = "| " + " | ".join("---" for _ in view.columns) + " |"
    body = [
        "| " + " | ".join(str(value).replace("|", "/") for value in row) + " |"
        for row in view.itertuples(index=False)
    ]
    more = f"\n_… 외 {len(df) - len(view)}건 (파일에서 이어서 확인)_\n" if len(df) > len(view) else ""
    return "\n".join([header, sep, *body]) + "\n" + more


def _build_input_guide_text(
    completeness: pd.DataFrame,
    option_kind_master: pd.DataFrame,
    menu_weight_master: pd.DataFrame,
    material_price_master: pd.DataFrame,
    chicken_ratio_master: pd.DataFrame | None = None,
    manual_profit_rate_master: pd.DataFrame | None = None,
) -> str:
    """지금 이 폴더에서 사람이 채워야 할 것만 모은 실행형 가이드."""
    rate = {}
    unfinished = {}
    if not completeness.empty:
        rate = {str(r["차원"]): float(r["완결률"]) for _, r in completeness.iterrows()}
        unfinished = {str(r["차원"]): (int(r["분모"]) - int(r["분자"])) for _, r in completeness.iterrows()}

    kind_todo = (
        option_kind_master[
            ~_filled(option_kind_master["option_kind_확정"])
            & ~option_kind_master["option_kind_제안"].isin(list(NON_SALES_KINDS) + [OPTION_KIND_MAIN])
        ].sort_values("매출합계", key=lambda s: pd.to_numeric(s, errors="coerce").fillna(0), ascending=False)
        if not option_kind_master.empty
        else pd.DataFrame()
    )
    weight_todo = (
        menu_weight_master[
            menu_weight_master["닭유형"].astype(str).str.strip().ne(CHICKEN_TYPE_NONE)
            & ~_filled(menu_weight_master.get("닭사용량_manual", pd.Series("", index=menu_weight_master.index)))
        ].sort_values("판매수량", key=lambda s: pd.to_numeric(s, errors="coerce").fillna(0), ascending=False)
        if not menu_weight_master.empty
        else pd.DataFrame()
    )
    price_todo = (
        material_price_master[~_filled(material_price_master["단가_manual"])]
        if not material_price_master.empty
        else pd.DataFrame()
    )
    profit_master = pd.DataFrame() if manual_profit_rate_master is None else manual_profit_rate_master
    profit_todo = (
        profit_master[
            profit_master.apply(lambda row: _manual_profit_rate_from_cost(row)[0] is None, axis=1)
        ].sort_values(
            "매출합계",
            key=lambda s: pd.to_numeric(s, errors="coerce").fillna(0),
            ascending=False,
        )
        if not profit_master.empty
        else pd.DataFrame()
    )
    ratio_master = pd.DataFrame() if chicken_ratio_master is None else chicken_ratio_master
    if not ratio_master.empty:
        blind_qty = pd.to_numeric(ratio_master.get("무신호_판매수량", ""), errors="coerce").fillna(0)
        ratio_todo = ratio_master[
            blind_qty.gt(0) & ~_filled(ratio_master.get("적용비율", pd.Series("", index=ratio_master.index)))
        ].sort_values("무신호_매출", key=lambda s: pd.to_numeric(s, errors="coerce").fillna(0), ascending=False)
        blind_total = float(blind_qty.sum())
    else:
        ratio_todo = pd.DataFrame()
        blind_total = 0.0

    def pct(key: str) -> str:
        return f"{rate.get(key, 0.0):g}%"

    return f"""# 입력 가이드

이 폴더에서 **사람이 채워야 하는 것만** 모았습니다. 위에서부터 채우면 됩니다.
각 표는 영향이 큰 순서(매출·판매수량)로 정렬돼 있어서, 위쪽 몇 줄만 채워도 커버리지가 크게 오릅니다.

셀을 채우고 DAG를 다시 돌리면 `25_completeness.csv`의 완결률이 올라갑니다.
파일을 다시 만들어도 **같은 키가 유지되는 한 입력한 값은 보존**됩니다.

## 지금 상태

| 채울 것 | 파일 | 남은 개수 | 완결률 |
| --- | --- | --- | --- |
| 옵션 성격 확정 | `21_option_kind_master.csv` | {unfinished.get('option_kind', 0)} | {pct('option_kind')} |
| 메뉴 표준중량 | `22_menu_weight_master.csv` | {unfinished.get('menu_weight', 0)} | {pct('menu_weight')} |
| 품목별 원가 | `27_profit_rate_master.csv` | {unfinished.get('manual_profit', 0)} | {pct('manual_profit')} |
| 재료 단가(공헌이익 선택) | `23_material_price_master.csv` | {unfinished.get('material_price', 0)} | {pct('material_price')} |
| 뼈:순살 비율 (선택) | `26_chicken_ratio_master.csv` | {unfinished.get('chicken_ratio', 0)} | {pct('chicken_ratio')} |

닭 속성(`chicken_attr` {pct('chicken_attr')}), 수수료(`commission` {pct('commission')}),
메뉴명 정리(`std_menu_alias` {pct('std_menu_alias')})는 자동으로 채워지므로 손댈 필요 없습니다.
26번도 실측으로 자동 채워지며, 표본이 부족한 줄만 손대면 됩니다.

---

## 전체 흐름

주문이 새로 들어오면 아래가 자동으로 돌아갑니다. **파란 부분은 자동, 노란 부분만 사람이 합니다.**

```mermaid
flowchart TD
    A["새 주문 수집<br/>배민 · 쿠팡 · OKPOS · posfeed"] --> B["DB_MenuHierarchy_Test_Dags<br/>매일 10시 · 13시 · 15시"]
    B --> C["메뉴/옵션 계층 분해<br/>main · option · side"]
    C --> D["옵션 성격 판정<br/>21번 확정값 우선, 없으면 규칙 제안"]
    D --> E["닭유형 · 사이즈 결정<br/>닭옵션키 기준"]
    E --> N{{"주문에 뼈/순살이<br/>적혀 있나?"}}
    N -- 예 --> F["닭 사용량 산출<br/>22번 표준중량"]
    N -- 아니오 --> R["26번 실측 비율로 가중<br/>닭유형=혼합"]
    R --> F
    F --> G["수익 산출<br/>23번 단가 + 실측 수수료율"]
    G --> H["25_completeness.csv<br/>차원별 완결률"]
    H --> I{{"완결률이<br/>떨어졌나?"}}
    I -- 예 --> J["DAG 중단<br/>completeness_regression"]
    I -- 아니오 --> K["24_menu_profit_summary.csv<br/>메뉴별 공헌이익"]
    H --> L["입력가이드.md<br/>남은 항목 갱신"]
    L --> M["사람이 빈칸 채움<br/>23 → 22 → 21 순서"]
    M --> B

    style A fill:#e3f2fd,stroke:#1976d2
    style B fill:#e3f2fd,stroke:#1976d2
    style C fill:#e3f2fd,stroke:#1976d2
    style D fill:#e3f2fd,stroke:#1976d2
    style E fill:#e3f2fd,stroke:#1976d2
    style F fill:#e3f2fd,stroke:#1976d2
    style G fill:#e3f2fd,stroke:#1976d2
    style H fill:#e3f2fd,stroke:#1976d2
    style N fill:#e3f2fd,stroke:#1976d2
    style R fill:#e3f2fd,stroke:#1976d2
    style K fill:#e8f5e9,stroke:#388e3c
    style L fill:#fff8e1,stroke:#f9a825
    style M fill:#fff8e1,stroke:#f9a825
    style J fill:#ffebee,stroke:#d32f2f
```

도식이 안 보이면 아래 순서만 기억하면 됩니다.

```
새 주문 수집
   ↓ (자동)
계층 분해 → 옵션 성격 → 닭 속성 → (뼈/순살 없으면 실측비율 가중) → 닭 사용량 → 수기수익
   ↓ (자동)
25_completeness.csv 에 "무엇이 비었는지" 기록
   ↓ (자동)
입력가이드.md 갱신  ←← 지금 보고 있는 이 파일
   ↓ (사람)
21 옵션성격 → 22 표준중량 → 27 수익률  순서로 빈칸 채움
   ↓
DAG 재실행 → 완결률 상승 → 18번 닭사용량, 28번 수기수익 확인
```

## 내가 해야 할 순서

| 순서 | 할 일 | 왜 이 순서인가 |
| --- | --- | --- |
| 1 | `21_option_kind_master.csv` 옵션성격 | 옵션/사이드/음료를 표준 품목으로 묶는 기준입니다. 제안값 일괄 확정 후 틀린 것만 고칩니다 |
| 2 | `22_menu_weight_master.csv` 표준중량 | 메뉴별 닭사용량과 재고 loss 비교의 기준입니다 |
| 3 | `27_profit_rate_master.csv` 품목별 원가 | 판매가와 원가를 입력하면 28번 수기수익이 자동 계산됩니다 |
| 4 | `26_chicken_ratio_master.csv` 뼈비율 | 실측으로 대부분 자동입니다. 표본이 얇아 `{CHICKEN_RATIO_SOURCE_NONE}`으로 남은 줄만 채우면 됩니다 |
| 5 | DAG 재실행 | `25_completeness.csv`에서 완결률 확인 |
| 6 | `18_material_usage_summary.csv`, `28_manual_profit_summary.csv` 확인 | 메뉴별 닭사용량과 입력 수익률 기준 수익 |

`24_menu_profit_summary.csv`는 재료단가 기반 공헌이익용으로 남겨둡니다. 이번 수익 기준은 사용자가 입력한 수익률이므로 최종 확인 파일은 `28_manual_profit_summary.csv`입니다.

## 신메뉴 · 신규 옵션이 생기면

새 메뉴나 새 옵션이 주문에 등장하면 **자동으로 표에 줄이 추가**됩니다. 직접 만들 필요 없습니다.

```
신메뉴 주문 발생
   ↓
21번에 새 품목 줄 추가 (option_kind_제안 채워진 상태)
22번에 새 메뉴x사이즈x닭유형 조합 줄 추가 (빈칸)
23번에 새 재료 줄 추가 (공헌이익 계산을 쓰는 경우)
26번에 새 메뉴x사이즈 줄 추가 (뼈비율 실측이 쌓이면 자동으로 채워짐)
27번에 새 수익키 줄 추가 (빈칸)
   ↓
완결률이 내려감 → 입력가이드.md 상단 "남은 개수"에 반영
   ↓
사람이 새로 생긴 빈칸만 채움
```

즉 **평소에는 이 파일 상단의 "남은 개수"만 보면 됩니다.** 0이면 할 일이 없습니다.

---

## 1. `21_option_kind_master.csv` — 옵션 성격 확정

주문에 나온 품목이 사이즈 옵션인지, 맛 선택인지, 음료인지 정하는 표입니다.
닭 사용량 계산에서 **어떤 옵션을 볼지**가 여기서 결정됩니다.

- **채울 칸**: `option_kind_확정`
- `option_kind_제안`에 규칙이 만든 초안이 있습니다. **맞으면 그대로 복사**, 틀리면 올바른 값을 적으세요.
- 쓸 수 있는 값: {", ".join(f"`{value}`" for value in OPTION_KIND_VALUES)}
- 닭 사용량에 직접 영향을 주는 건 `사이즈`, `닭유형` 둘뿐입니다. 이 둘을 먼저 정확히 보세요.
- 제안값을 한꺼번에 받아들이려면 아래를 실행하고, 그 뒤 틀린 것만 고치면 됩니다.

```python
from modules.transform.pipelines.db.DB_MenuHierarchy_Test import confirm_option_kind_suggestions
confirm_option_kind_suggestions()          # 전체 제안값을 확정으로 복사
confirm_option_kind_suggestions("사이즈")   # 특정 성격만
```

{_guide_rows(kind_todo, ["item_name", "option_kind_제안", "매출합계", "판매수량"])}

## 2. `22_menu_weight_master.csv` — 메뉴 1개당 재료 표준중량

메뉴 하나 만들 때 재료가 얼마나 들어가는지입니다. 재고 loss 비교와 재료원가의 기준입니다.

- **채울 칸**: `닭사용량_manual`, `우거지사용량_manual` 등 `*사용량_manual`로 끝나는 칸.
- **한 칸도 비우지 마세요.** 안 쓰는 재료는 **`0`을 적습니다.** 비워두면 "안 씀"이 아니라
  "미입력"으로 봅니다.
- 단위는 `23_material_price_master.csv`의 `단위`와 같아야 합니다 (닭이 `마리`면 `1.2` = 1.2마리).
- 키는 **메뉴 x 사이즈 x 닭유형**입니다. 판매 채널(배민/쿠팡/홀)과 무관하므로 한 번만 입력합니다.
- 재료 칸을 새로 만들고 싶으면 `미나리사용량_manual`처럼 **`사용량_manual`로 끝나는 컬럼**을 추가하면 됩니다.

{_guide_rows(weight_todo, ["std_menu_name", "사이즈", "닭유형", "판매수량", "닭사용량_manual"])}

## 3. `27_profit_rate_master.csv` — 품목별 원가

사용자가 직접 넣는 품목별 판매가/원가 표입니다. 같은 메뉴라도 홀/포장/배달 플랫폼별 원가와 수수료가 달라 `수익채널`별로 나뉩니다.

- **채울 칸**: `판매가_manual`, `메뉴원가_manual`, `상차림비_manual`
- `판매가`는 원주문서 기준 자동 참고값입니다. 실제 계산은 `판매가_manual`이 있으면 수기값, 없으면 자동 `판매가`를 씁니다.
- 수익률은 직접 입력하지 않습니다. `상차림포함원가 / 계산 판매가` 기준으로 자동 계산합니다.
- `상차림포함원가`는 `메뉴원가_manual + 상차림비_manual`로 자동 계산합니다. 포장은 상차림비가 항상 0입니다.
- main 메뉴는 `메뉴|수익채널|표준메뉴|사이즈|닭유형` 단위로 봅니다.
- `1인분 추가`, `순살 추가`처럼 부모 메뉴별로 달라지는 옵션/사이드는 `품목|수익채널|부모표준메뉴|사이즈|닭유형|대표품목명` 단위로 봅니다.
- 콜라 같은 독립 옵션/사이드는 `품목|수익채널|대표품목명` 단위로 봅니다.
- 콜라 같은 옵션/사이드는 부모 메뉴에 합산하지 않고 별도 품목으로 집계합니다.
- 결과는 `28_manual_profit_summary.csv`에서 봅니다.

{_guide_rows(profit_todo, ["수익채널", "수익키", "대표품목명", "사이즈", "닭유형", "option_kind", "매출합계", "판매가", "판매가_manual", "판매가기준", "메뉴원가_manual", "상차림비_manual", "상차림포함원가"])}

## 4. `23_material_price_master.csv` — 재료 단가 (공헌이익 선택)

이 표는 `24_menu_profit_summary.csv`의 재료원가 기반 공헌이익을 볼 때만 필요합니다.
이번 기준 수익은 `27_profit_rate_master.csv`를 사용합니다.

- **채울 칸**: `단가_manual` (1단위당 원). `기준일_manual`은 언제 기준 단가인지(예: `2026-08-01`).
- `단위`는 미리 넣어놨습니다. 다르면 고치세요 (`마리` / `kg` / `개`).
- `사용판매수량`이 큰 재료가 위에 옵니다. 위에서부터 채우세요.
- **0을 넣지 마세요.** 0은 "원가 없음"으로 계산되어 수익이 부풀려집니다. 모르면 비워두세요.

{_guide_rows(price_todo, ["재료명", "단위", "단가_manual", "기준일_manual", "사용판매수량"])}
예) `닭` / `마리` / `5500` / `2026-08-01`

## 5. `26_chicken_ratio_master.csv` — 뼈:순살 비율 (대부분 자동)

**왜 있는 표인가**: 주문 데이터에 뼈/순살이 아예 안 적힌 건이 있습니다.
특히 홀(OKPOS)은 옵션으로 안 남고, 배민은 옵션 대신 메뉴명으로만 구분합니다.
이런 건을 전부 뼈닭으로 치면 닭 사용량이 최대 5.4% 틀립니다.

**어떻게 줄이는가**: 뼈/순살이 **실제로 적혀 있던 주문**에서 메뉴별 비율을 뽑아,
안 적힌 건에 그 비율로 가중합니다. 예를 들어 실측이 뼈 62% / 순살 38%이고
뼈닭 1.0마리 · 순살 0.8마리면 그 건은 `0.62x1.0 + 0.38x0.8 = 0.924`마리로 잡힙니다.
이 행은 `닭유형=혼합`, `닭유형_판정=비율추정`으로 표시되고 `뼈비율_적용`에 쓴 비율이 남습니다.

- **적혀 있던 건은 건드리지 않습니다.** 실제 값이 언제나 추정보다 낫습니다.
- 비율은 **같은 채널 · 같은 메뉴 · 같은 사이즈** 실측을 1순위로 씁니다.
  표본이 {CHICKEN_RATIO_MIN_SAMPLE:g}개 미만이면 사이즈를 접고, 그래도 부족하면 채널을 접습니다.
- 그래도 표본이 안 되는 줄만 `비율출처`가 `{CHICKEN_RATIO_SOURCE_NONE}`으로 남습니다.
  **이 줄만 채우면 됩니다** (남은 {unfinished.get('chicken_ratio', 0)}줄).
- **채울 칸**: `뼈비율_manual`. `0.7`도 되고 `70`도 됩니다(둘 다 뼈 70%).
  담당자가 넣으면 실측보다 우선합니다.
- 현재 무신호 판매수량 합계: {blind_total:g}개.

{_guide_rows(ratio_todo, ["source", "std_menu_name", "사이즈", "무신호_판매수량", "무신호_매출", "뼈비율_실측", "비율출처"])}
예) `okpos` / `도리당 닭도리탕` / `중` / `뼈비율_manual = 0.65`

> 이 표는 **오차를 줄일 뿐 없애지는 못합니다.** 원천적으로 없애려면 OKPOS에서
> 뼈/순살을 별도 상품코드로 분리해야 합니다. 그러면 이 표 자체가 필요 없어집니다.

---

## 채우고 나면

1. DAG를 다시 실행합니다 (또는 `build_orders(None)`).
2. `25_completeness.csv`에서 완결률이 올랐는지 봅니다.
3. `18_material_usage_summary.csv`에서 메뉴별 닭/재료 사용량을 확인합니다.
4. `28_manual_profit_summary.csv`에서 사용자가 입력한 수익률 기준 품목별 수익을 확인합니다.
5. `수익산출행` / `대상행`을 같이 보세요. 산출행이 대상행보다 적으면 아직 `27`번 수익률이 빈 품목이 있다는 뜻입니다.

## 주의

- **23번 단가에는 0을 넣지 마세요.** 비워두면 "미입력"으로 남지만, 0을 넣으면 원가 0으로 계산되어 수익이 실제보다 크게 나옵니다.
- **22번·16번 사용량에는 반대로 0을 넣으세요.** 여기서 빈칸은 "안 씀"이 아니라 "미입력"입니다.
  안 쓰는 재료에 0을 명시해야 그 메뉴의 공헌이익이 계산됩니다. 헷갈리면 이렇게 기억하세요 —
  **"얼마 쓰는지"는 0을 쓰고, "얼마인지"는 0을 쓰지 않는다.**
- 완결률이 **떨어지면 DAG가 멈춥니다**(후퇴 방지). 값을 지웠다면 의도한 것인지 확인하세요.
- 값이 틀렸다고 판단해 기준선을 내려야 하면 `reset_completeness_baseline("차원명")`을 실행합니다. 자세한 건 `설명.md` 참고.

## DAG가 멈췄을 때

**신메뉴가 들어오면 멈춥니다. 고장이 아닙니다.**
새 메뉴가 22번에 빈 조합으로 추가되면 분모만 늘어 완결률이 떨어지고, 후퇴로 판단해 멈춥니다.

1. `19_validation_issues.csv`에서 `completeness_regression`이 **어느 차원**인지 봅니다.
2. `25_completeness.csv`의 `미완요약`에 새로 생긴 빈칸이 있습니다. **채우고 재실행하면 풀립니다.**
3. 당장 못 채우는데 산출물은 봐야 하면, 그 차원 기준선만 내리고 재실행합니다. 빈칸은 남고 나중에 채우면 됩니다.

```python
from modules.transform.pipelines.db.DB_MenuHierarchy_Test import reset_completeness_baseline
reset_completeness_baseline("menu_weight")
```

닭 사용량 총합이 직전보다 3% 넘게 줄어도 멈춥니다. 의도한 변동이면
`reset_completeness_baseline('_chicken_usage_total')` 후 재실행합니다.

## 다 채우면 완벽한가

아닙니다. 빈칸을 다 채워도 남는 것이 있습니다.

- **공헌이익은 순이익이 아닙니다.** 양념·가스·인건비·임대료·포장재가 계산식에 없습니다.
  메뉴 비교용이지 점포 손익이 아닙니다.
- **뼈/순살 일부는 추정입니다.** 주문에 안 적힌 건은 26번 실측 비율로 가중할 뿐입니다.
- **수수료는 월 단위 실측 평균**이라 개별 주문의 실제 수수료가 아닙니다.
- **단가는 시점 하나**라 원가가 오르내려도 과거 주문에 그대로 적용됩니다.
- **송파삼전점 하나**입니다.

자세한 건 `설명.md`의 "이 분석의 한계"를 보세요.
"""


def _build_material_price_master(
    menu_weight_master: pd.DataFrame,
    option_material_input: pd.DataFrame,
    left_joined: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """실제로 쓰이는 재료를 전수 열거한 단가표. 재료 수가 작아 한 번 채우면 끝난다."""
    materials: list[str] = []
    for frame in (menu_weight_master, option_material_input):
        if frame is None or frame.empty:
            continue
        for col in frame.columns:
            if _is_material_usage_manual_column(col):
                name = _material_name_from_usage_column(col)
                if name:
                    materials.append(name)
    volume = _material_usage_volume(left_joined if left_joined is not None else pd.DataFrame())
    # 영향 큰 재료를 위로 올린다. 위에서부터 채우면 커버리지가 빨리 오른다.
    materials = _unique_nonempty(sorted(set(materials), key=lambda n: (-volume.get(n, 0.0), n)))

    previous = _manual_or_legacy_sheet("재료단가", MATERIAL_PRICE_MASTER_OUTPUT_PATH).fillna("")
    kept: dict[str, dict[str, str]] = {}
    if not previous.empty and "재료명" in previous.columns:
        for _, row in previous.iterrows():
            name = str(row.get("재료명", "")).strip()
            if name:
                kept[name] = {col: str(row.get(col, "")).strip() for col in MATERIAL_PRICE_MASTER_COLUMNS}

    rows = []
    for name in materials:
        prior = kept.get(name, {})
        rows.append(
            {
                "재료명": name,
                "단위": prior.get("단위") or _guess_material_unit(name),
                "단가_manual": prior.get("단가_manual", ""),
                "기준일_manual": prior.get("기준일_manual", ""),
                "사용판매수량": _format_number(volume.get(name, 0.0)),
                "메모": prior.get("메모", ""),
            }
        )
    return pd.DataFrame(rows, columns=MATERIAL_PRICE_MASTER_COLUMNS)


def _build_menu_profit_summary(left_joined: pd.DataFrame) -> pd.DataFrame:
    """메뉴 x 사이즈 x 닭유형별 수익 요약. '메뉴별로 얼마나 남는지'의 최종 답."""
    if left_joined.empty:
        return pd.DataFrame(columns=MENU_PROFIT_SUMMARY_COLUMNS)
    work = _attach_profit_channel(_profit_calculation_frame(left_joined)).fillna("")
    work = work[work.get("line_role", pd.Series("", index=work.index)).eq("main")]
    if work.empty:
        return pd.DataFrame(columns=MENU_PROFIT_SUMMARY_COLUMNS)
    numeric = {
        "qty_num": "qty",
        "total_num": "total_price",
        "usage_num": CHICKEN_USAGE_TOTAL_COLUMN,
        "material_num": "재료원가",
        "option_num": "옵션재료원가",
        "fee_num": "수수료",
        "profit_num": "공헌이익",
    }
    for target, source_col in numeric.items():
        work[target] = pd.to_numeric(work.get(source_col, pd.Series("", index=work.index)), errors="coerce").fillna(0)
    # 공헌이익이 안 나온 행은 0으로 더해진다. 합계만 보면 원가가 없는 건지 0인 건지
    # 구분이 안 되므로 산출된 행 수를 같이 남긴다.
    work["_profit_ok"] = _filled(work.get("공헌이익", pd.Series("", index=work.index))).astype(int)

    grouped = (
        work.groupby(MENU_PROFIT_GROUP_COLUMNS, dropna=False, sort=False)
        .agg(
            주문건수=("order_id", "nunique"),
            판매수량=("qty_num", "sum"),
            매출합계=("total_num", "sum"),
            닭사용량합계=("usage_num", "sum"),
            재료원가합계=("material_num", "sum"),
            옵션재료원가합계=("option_num", "sum"),
            수수료합계=("fee_num", "sum"),
            공헌이익합계=("profit_num", "sum"),
            공헌이익산출행=("_profit_ok", "sum"),
            대상행=("_profit_ok", "size"),
        )
        .reset_index()
    )
    revenue = grouped["매출합계"].where(grouped["매출합계"].ne(0))
    grouped["공헌이익률"] = (grouped["공헌이익합계"] / revenue * 100).round(2)
    for col in grouped.columns:
        if col not in MENU_PROFIT_GROUP_COLUMNS:
            grouped[col] = grouped[col].map(lambda value: "" if pd.isna(value) else f"{float(value):g}")
    return grouped.reindex(columns=MENU_PROFIT_SUMMARY_COLUMNS, fill_value="").sort_values(
        "매출합계", key=lambda s: pd.to_numeric(s, errors="coerce").fillna(0), ascending=False
    )


def _manual_profit_item_name(row: pd.Series) -> str:
    """사용자 수익률표의 품목명. source별 원본명 흔들림을 줄여 입력 줄 수를 줄인다."""
    role = str(row.get("line_role", "") or "").strip()
    item_name = str(row.get("item_name", "") or "").strip()
    std_name = str(row.get("std_menu_name", "") or "").strip()
    menu_name = str(row.get("menu_name", "") or "").strip()
    kind = str(row.get("option_kind", "") or "").strip()
    if role == "main":
        return std_name or menu_name or item_name

    text = item_name
    compact = _normalize_item_key(text)
    if kind == OPTION_KIND_CHICKEN_ADDON:
        return _chicken_addon_profit_name(text)
    if kind == OPTION_KIND_DRINK:
        manual_std_name = _std_menu_override_manual_name_for_row(row)
        if manual_std_name and _infer_drink_profit_name(manual_std_name) == _infer_drink_profit_name(text):
            return manual_std_name
        item_drink_signals = _infer_drink_profit_name(text)
        if item_drink_signals:
            return item_drink_signals
    material = str(row.get("재료명", "") or "").strip()
    if material:
        return material
    normalized = _normalize_material_name(text)
    return normalized or _strip_leading_tags(text) or text


def _infer_drink_profit_name(value: object) -> str:
    text = str(value or "").strip()
    compact = _normalize_item_key(text)
    if "펩시제로" in compact or ("펩시" in text and "제로" in text):
        return "펩시 제로 355ml (캔)"
    if "펩시콜라" in compact:
        return "펩시콜라 355ml (캔)"
    if "펩시" in text or "pepsi" in compact:
        return "펩시콜라 355ml (캔)"
    if "콜라" in text or "cola" in compact:
        return "콜라"
    if "새로다래" in compact:
        return "새로 다래"
    if "새로오미자" in compact:
        return "새로 오미자"
    if "새로" in text:
        return "새로 360ml"
    if "사이다" in text:
        return "사이다"
    if "환타" in text:
        return "환타"
    if "쿨피스" in text:
        return "쿨피스"
    for token in ["참이슬", "처음처럼", "진로", "카스", "테라", "켈리", "하이볼", "막걸리", "오미자", "청하"]:
        if token in text:
            return token
    return ""


def _manual_profit_parent_name(row: pd.Series) -> str:
    return (
        str(row.get("std_menu_name", "") or "").strip()
        or str(row.get("menu_name", "") or "").strip()
        or str(row.get("item_name", "") or "").strip()
    )


def _strip_profit_name_size_suffix(value: object) -> str:
    text = str(value or "").strip()
    while True:
        match = re.search(r"\s*\[([^\]]+)\]\s*$", text)
        if not match:
            return text
        if not _SIZE_TAG_RE.fullmatch(match.group(1).strip()):
            return text
        text = text[: match.start()].strip()


def _manual_profit_main_name(row: pd.Series) -> str:
    std_name = str(row.get("std_menu_name", "") or "").strip()
    item_name = str(row.get("item_name", "") or "").strip()
    if std_name and _has_set_identity(std_name):
        return std_name
    if item_name and std_name and _has_set_identity(item_name) != _has_set_identity(std_name):
        return _strip_profit_name_size_suffix(item_name)
    return std_name or str(row.get("menu_name", "") or "").strip() or _manual_profit_item_name(row)


def _manual_profit_size(row: pd.Series) -> str:
    return str(row.get("사이즈", "") or "").strip()


def _manual_profit_size_axis(row: pd.Series) -> str:
    """수익키에 넣을 사이즈 축. 2인이상 점심류의 인원수는 사람이 수기로 입력한다."""
    size = _manual_profit_size(row)
    context = " ".join(
        str(row.get(col, "") or "").strip()
        for col in ("std_menu_name", "menu_name", "item_name")
        if str(row.get(col, "") or "").strip()
    )
    compact_context = re.sub(r"\s+", "", context)
    if "2인이상" not in compact_context:
        return size
    suggested = _suggest_menu_chicken_profile(_manual_profit_parent_name(row))
    default_size = str(suggested.get("기본사이즈_제안", "") or "").strip()
    return default_size or size


def _manual_profit_chicken_type(row: pd.Series) -> str:
    role = str(row.get("line_role", "") or "").strip()
    kind = str(row.get("option_kind", "") or "").strip()
    if role != "main" and kind == OPTION_KIND_CHICKEN_TYPE:
        key = tuple(str(row.get(col, "") or "").strip() for col in MENU_CHICKEN_PROFILE_KEY_COLUMNS)
        profile = _cached_menu_chicken_profile_lookup().get(key)
        if profile is None:
            profile = _effective_menu_chicken_profile(pd.Series(_suggest_menu_chicken_profile(key[-1])))
        if profile is not None and not bool(profile.get("apply_option_type", False)):
            values = _profile_final_values(profile)
            if values is not None:
                return values[0]
        locked = _locked_chicken_profile_for_profit_row(row)
        if locked is not None:
            return locked[0]
        inferred = _unique_nonempty(_infer_chicken_types(row.get("item_name", "")))
        if len(inferred) == 1:
            return inferred[0]
    return str(row.get("닭유형", "") or "").strip()


def _manual_profit_ignores_visible_chicken_type(row: pd.Series) -> bool:
    role = str(row.get("line_role", "") or "").strip()
    kind = str(row.get("option_kind", "") or "").strip()
    if role == "main" or kind != OPTION_KIND_CHICKEN_TYPE:
        return False
    key = tuple(str(row.get(col, "") or "").strip() for col in MENU_CHICKEN_PROFILE_KEY_COLUMNS)
    profile = _cached_menu_chicken_profile_lookup().get(key)
    if profile is None:
        profile = _effective_menu_chicken_profile(pd.Series(_suggest_menu_chicken_profile(key[-1])))
    if not profile or bool(profile.get("apply_option_type", False)):
        return False
    allowed_types = [str(value) for value in profile.get("allowed_types", []) if str(value or "").strip()]
    signals = _infer_chicken_types(row.get("item_name", ""))
    return bool(signals and allowed_types and any(signal not in allowed_types for signal in signals))


def _manual_profit_deciding_option_type_signals(row: pd.Series) -> list[str]:
    texts = [
        str(row.get(CHICKEN_OPTION_KEY_COLUMN, "") or "").strip(),
        str(row.get(OPTION_COMBO_COLUMN, "") or "").strip(),
    ]
    signals: list[str] = []
    for text in texts:
        for part in str(text or "").split("|"):
            token = part.strip()
            if not token or _ROLE_ADDON_RE.search(token):
                continue
            signals.extend(_infer_chicken_types(token))
        if signals:
            return _unique_nonempty(signals)
    return []


def _manual_profit_chicken_type_axis(row: pd.Series) -> str:
    """수익키에 넣을 닭유형 축. 주문서에서 확인 가능한 뼈/순살 신호만 쓴다."""
    chicken_type = _manual_profit_chicken_type(row)
    if not chicken_type or chicken_type == CHICKEN_TYPE_NONE:
        return ""
    row_chicken_type = str(row.get("닭유형", "") or "").strip()
    if row_chicken_type == CHICKEN_TYPE_NONE:
        return ""
    if _manual_profit_ignores_visible_chicken_type(row):
        return ""
    own_context = " ".join(
        str(row.get(col, "") or "").strip()
        for col in ("menu_name", "std_menu_name", "item_name")
        if str(row.get(col, "") or "").strip()
    )
    own_signals = _infer_chicken_types(own_context)
    if chicken_type in own_signals:
        return chicken_type
    role = str(row.get("line_role", "") or "").strip()
    kind = str(row.get("option_kind", "") or "").strip()
    if role != "main" and kind in CHICKEN_DECIDING_KINDS:
        deciding_signals = _manual_profit_deciding_option_type_signals(row)
        if len(deciding_signals) == 1 and chicken_type == deciding_signals[0]:
            return chicken_type
    if role == "main":
        key = tuple(str(row.get(col, "") or "").strip() for col in MENU_CHICKEN_PROFILE_KEY_COLUMNS)
        profile = _cached_menu_chicken_profile_lookup().get(key)
        if profile is None:
            profile = _effective_menu_chicken_profile(pd.Series(_suggest_menu_chicken_profile(key[-1])))
        if profile is not None and not bool(profile.get("apply_option_type", False)) and not own_signals:
            default_type = str(profile.get("default_type", "") or "").strip()
            allowed_types = [str(value) for value in profile.get("allowed_types", []) if str(value or "").strip()]
            if default_type == "순살" and chicken_type == default_type and allowed_types == ["순살"]:
                return chicken_type
            return ""
    method = str(row.get("닭유형_판정", "") or "").strip()
    if method in {"선택", "변경", "메뉴명", CHICKEN_METHOD_HALF, CHICKEN_METHOD_HALF_SLOT}:
        return chicken_type
    signal = str(row.get(CHICKEN_SIGNAL_COLUMN, "") or "").strip()
    if signal == CHICKEN_SIGNAL_PRESENT and method != "메뉴프로필":
        return chicken_type
    context = " ".join(
        str(row.get(col, "") or "").strip()
        for col in ("menu_name", "std_menu_name", "item_name", OPTION_COMBO_COLUMN, CHICKEN_OPTION_KEY_COLUMN)
        if str(row.get(col, "") or "").strip()
    )
    return chicken_type if chicken_type in _infer_chicken_types(context) else ""


def _manual_profit_combo_menu_family(row: pd.Series) -> str:
    name = _manual_profit_main_name(row)
    compact = _normalize_item_key(name)
    if "2인순살반반세트" in compact:
        return "2인 순살 반반세트"
    if "2인순살반반" in compact:
        return "2인 순살 반반"
    if "베스트반반세트" in compact:
        return "베스트 반반세트"
    if "베스트반반" in compact:
        return "베스트 반반"
    if "시그니처반반세트" in compact:
        return "시그니처 반반세트"
    if "시그니처반반" in compact:
        return "시그니처 반반"
    return ""


def _profit_combo_side_name(value: object) -> str:
    text = str(value or "").strip()
    compact = _normalize_item_key(text)
    if "가브리" in compact:
        return "가브리살"
    if "미나리새우전" in compact:
        return "미나리 새우전"
    return ""


def _profit_combo_chicken_part(value: object, *, typed: bool = False) -> str:
    text = str(value or "").strip()
    if _ROLE_ADDON_RE.search(text) and not re.search(r"^\s*(?:2\s*인\s*순살\s*)?(?:닭도리탕|닭한마리|곱도리탕)\s*(?:\[[^\]]+\])?\s*$", text):
        return ""
    compact = _normalize_item_key(text)
    if "곱도리탕" in compact:
        return "곱도리탕"
    if "닭한마리" in compact:
        if not typed:
            return "닭한마리"
        ctype = _single_inferred_chicken_type(text)
        return f"닭한마리({'뼈' if ctype == '뼈닭' else '순'})" if ctype else "닭한마리"
    if "닭도리탕" in compact or "도리탕" in compact:
        if not typed:
            return "닭도리탕"
        ctype = _single_inferred_chicken_type(text)
        return f"닭도리탕({'뼈' if ctype == '뼈닭' else '순'})" if ctype else "닭도리탕"
    return ""


def _ordered_profit_combo_parts(text: object, family: str) -> list[str]:
    typed = family.startswith("베스트") or family.startswith("시그니처")
    parts: list[str] = []
    for raw in str(text or "").split("|"):
        part = _profit_combo_chicken_part(raw, typed=typed)
        if part:
            parts.append(part)
    return _unique_nonempty(parts)


def _ordered_profit_combo_sides(text: object) -> list[str]:
    return _unique_nonempty([_profit_combo_side_name(part) for part in str(text or "").split("|")])


def _normalize_profit_combo_parts(parts: list[str], family: str) -> list[str]:
    order = ["닭도리탕", "닭도리탕(뼈)", "닭도리탕(순)", "닭한마리", "닭한마리(뼈)", "닭한마리(순)", "곱도리탕"]
    unique = _unique_nonempty(parts)
    unique.sort(key=lambda value: order.index(value) if value in order else len(order))
    if family.startswith("베스트") and not any(part == "곱도리탕" for part in unique):
        unique.append("곱도리탕")
    return unique


def _manual_profit_cost_combo(row: pd.Series) -> str:
    family = _manual_profit_combo_menu_family(row)
    if not family:
        return ""
    parts: list[str] = []
    chicken_key = str(row.get(CHICKEN_OPTION_KEY_COLUMN, "") or "").strip()
    option_combo = str(row.get(OPTION_COMBO_COLUMN, "") or "").strip()
    for text in [
        chicken_key,
        " | ".join([str(row.get(HALF_SLOT1_COLUMN, "") or "").strip(), str(row.get(HALF_SLOT2_COLUMN, "") or "").strip()]),
    ]:
        parts.extend(_ordered_profit_combo_parts(text, family))
    if len(_unique_nonempty(parts)) < 2:
        parts.extend(_ordered_profit_combo_parts(option_combo, family))
    parts = _normalize_profit_combo_parts(parts, family)
    if family.startswith("2인") and len(parts) > 2:
        parts = [part for part in parts if part in {"닭도리탕", "닭한마리", "곱도리탕"}][:2]
    sides: list[str] = []
    if family.endswith("세트"):
        sides.extend(_ordered_profit_combo_sides(option_combo))
        sides = _unique_nonempty(sides)[:1]
    combo_parts = [*parts, *sides]
    return "+".join(combo_parts)


_MANUAL_PROFIT_COST_SEEDS: dict[tuple[str, str, str, str], tuple[str, str]] = {
    ("2인 순살 반반", "2인", "순살", "닭도리탕+닭한마리"): ("10368", "33000"),
    ("2인 순살 반반", "2인", "순살", "닭도리탕+곱도리탕"): ("14526", "39000"),
    ("2인 순살 반반", "2인", "순살", "닭한마리+곱도리탕"): ("12618", "39000"),
    ("2인 순살 반반세트", "2인", "순살", "닭도리탕+닭한마리+가브리살"): ("14849", "43900"),
    ("2인 순살 반반세트", "2인", "순살", "닭도리탕+곱도리탕+가브리살"): ("19007", "49900"),
    ("2인 순살 반반세트", "2인", "순살", "닭한마리+곱도리탕+가브리살"): ("17099", "49900"),
    ("2인 순살 반반세트", "2인", "순살", "닭도리탕+닭한마리+미나리 새우전"): ("15923", "45900"),
    ("2인 순살 반반세트", "2인", "순살", "닭도리탕+곱도리탕+미나리 새우전"): ("20081", "51900"),
    ("2인 순살 반반세트", "2인", "순살", "닭한마리+곱도리탕+미나리 새우전"): ("18173", "51900"),
}

for _family, _base, _rows in [
    ("베스트 반반", "곱도리탕", [
        ("중", "뼈닭", "닭도리탕(뼈)+곱도리탕", "16138", "47500"),
        ("중", "순살", "닭도리탕(순)+곱도리탕", "16088", "48500"),
        ("대", "뼈닭", "닭도리탕(뼈)+곱도리탕", "24039", "67500"),
        ("대", "순살", "닭도리탕(순)+곱도리탕", "23939", "68500"),
    ]),
    ("베스트 반반세트", "곱도리탕", [
        ("중", "뼈닭", "닭도리탕(뼈)+곱도리탕+가브리살", "20620", "58900"),
        ("중", "순살", "닭도리탕(순)+곱도리탕+가브리살", "20570", "59900"),
        ("대", "뼈닭", "닭도리탕(뼈)+곱도리탕+가브리살", "28520", "78900"),
        ("대", "순살", "닭도리탕(순)+곱도리탕+가브리살", "28420", "79900"),
        ("중", "뼈닭", "닭도리탕(뼈)+곱도리탕+미나리 새우전", "21694", "60900"),
        ("중", "순살", "닭도리탕(순)+곱도리탕+미나리 새우전", "21644", "61900"),
        ("대", "뼈닭", "닭도리탕(뼈)+곱도리탕+미나리 새우전", "29594", "80900"),
        ("대", "순살", "닭도리탕(순)+곱도리탕+미나리 새우전", "29494", "81900"),
    ]),
    ("시그니처 반반", "", [
        ("중", "뼈닭", "닭도리탕(뼈)+닭한마리(뼈)", "12009", "38500"),
        ("중", "혼합", "닭도리탕(뼈)+닭한마리(순)", "11959", "39500"),
        ("중", "혼합", "닭도리탕(순)+닭한마리(뼈)", "11959", "39500"),
        ("중", "순살", "닭도리탕(순)+닭한마리(순)", "11909", "40500"),
        ("대", "뼈닭", "닭도리탕(뼈)+닭한마리(뼈)", "18610", "58500"),
        ("대", "혼합", "닭도리탕(뼈)+닭한마리(순)", "18510", "59500"),
        ("대", "혼합", "닭도리탕(순)+닭한마리(뼈)", "18510", "59500"),
        ("대", "순살", "닭도리탕(순)+닭한마리(순)", "18410", "60500"),
    ]),
]:
    for _size, _ctype, _combo, _cost, _price in _rows:
        _MANUAL_PROFIT_COST_SEEDS[(_family, _size, _ctype, _combo)] = (_cost, _price)

for _side, _rows in {
    "가브리살": [
        ("중", "뼈닭", "닭도리탕(뼈)+닭한마리(뼈)", "16490", "49900"),
        ("중", "혼합", "닭도리탕(뼈)+닭한마리(순)", "16440", "50900"),
        ("중", "혼합", "닭도리탕(순)+닭한마리(뼈)", "16440", "50900"),
        ("중", "순살", "닭도리탕(순)+닭한마리(순)", "16390", "51900"),
        ("대", "뼈닭", "닭도리탕(뼈)+닭한마리(뼈)", "23092", "69900"),
        ("대", "혼합", "닭도리탕(뼈)+닭한마리(순)", "22992", "70900"),
        ("대", "혼합", "닭도리탕(순)+닭한마리(뼈)", "22992", "70900"),
        ("대", "순살", "닭도리탕(순)+닭한마리(순)", "22892", "71900"),
    ],
    "미나리 새우전": [
        ("중", "뼈닭", "닭도리탕(뼈)+닭한마리(뼈)", "17564", "51900"),
        ("중", "혼합", "닭도리탕(뼈)+닭한마리(순)", "17514", "52900"),
        ("중", "혼합", "닭도리탕(순)+닭한마리(뼈)", "17514", "52900"),
        ("중", "순살", "닭도리탕(순)+닭한마리(순)", "17464", "53900"),
        ("대", "뼈닭", "닭도리탕(뼈)+닭한마리(뼈)", "24165", "71900"),
        ("대", "혼합", "닭도리탕(뼈)+닭한마리(순)", "24065", "72900"),
        ("대", "혼합", "닭도리탕(순)+닭한마리(뼈)", "24065", "72900"),
        ("대", "순살", "닭도리탕(순)+닭한마리(순)", "23965", "73900"),
    ],
}.items():
    for _size, _ctype, _combo, _cost, _price in _rows:
        _MANUAL_PROFIT_COST_SEEDS[("시그니처 반반세트", _size, _ctype, f"{_combo}+{_side}")] = (_cost, _price)


def _manual_profit_needs_parent_axis(row: pd.Series) -> bool:
    role = str(row.get("line_role", "") or "").strip()
    kind = str(row.get("option_kind", "") or "").strip()
    return role in {"option", "side"} and kind in _MENU_DEPENDENT_PROFIT_KINDS


def _legacy_drink_group_profit_name(value: object) -> str:
    text = str(value or "").strip()
    compact = _normalize_item_key(text)
    if "콜라" in text or "펩시" in text or "cola" in compact:
        return "콜라"
    if "새로" in text:
        return "새로"
    return ""


def _legacy_channel_drink_group_profit_key(row: pd.Series) -> str:
    if str(row.get("option_kind", "") or "").strip() != OPTION_KIND_DRINK:
        return ""
    group_name = _legacy_drink_group_profit_name(row.get("item_name", ""))
    if not group_name:
        return ""
    channel = str(row.get(PROFIT_CHANNEL_COLUMN, "") or "").strip() or _profit_channel_from_values(
        row.get("source", ""),
        row.get("platform", ""),
        row.get("order_type", ""),
    )
    return "|".join(["품목", channel, group_name])


def _profit_channel_from_values(source: object, platform: object, order_type: object) -> str:
    """수익 산정용 채널. 같은 메뉴라도 홀 테이블, 홀 포장, 배달 플랫폼은 원가/수수료가 다르다."""
    source_text = str(source or "").strip()
    platform_text = str(platform or "").strip()
    order_type_text = str(order_type or "").strip()

    if platform_text in {"", "홀"}:
        if "포장" in order_type_text:
            return "홀_포장"
        if source_text == OKPOS_SOURCE and order_type_text.startswith("홀"):
            return "홀"
        if order_type_text.startswith("홀"):
            return "홀"
    if platform_text and platform_text != "홀":
        return platform_text
    if "배달" in order_type_text:
        return "배달:미확인"
    return source_text or "기타"


def _attach_profit_channel(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if out.empty:
        if PROFIT_CHANNEL_COLUMN not in out.columns:
            out[PROFIT_CHANNEL_COLUMN] = pd.Series(dtype=str)
        return out
    out[PROFIT_CHANNEL_COLUMN] = _profit_channel_series(out)
    return out


def _attach_profit_sales_base(left_joined: pd.DataFrame) -> pd.DataFrame:
    """수익 계산용 매출. 할인/이벤트로 나간 메뉴는 음수 매출이 아니라 0원 판매로 본다."""
    out = left_joined.copy()
    total = pd.to_numeric(out.get("total_price", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    out[PROFIT_SALES_COLUMN] = total.clip(lower=0).map(_format_number)
    return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")


def _attach_normal_price_column(left_joined: pd.DataFrame) -> pd.DataFrame:
    """원천 정상가. 기본은 할인 전 기준 total_price, 전액할인 0원 행은 할인액으로 보완한다."""
    out = left_joined.copy()
    total = pd.to_numeric(out.get("total_price", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    discount = pd.to_numeric(out.get("discount_amount", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    normal = total.abs().where(total.ne(0), discount.abs().where(discount.ne(0), 0))
    out[NORMAL_PRICE_COLUMN] = normal.map(_format_number)
    return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")


def _normal_price_num(frame: pd.DataFrame) -> pd.Series:
    total = pd.to_numeric(frame.get("total_price", pd.Series("", index=frame.index)), errors="coerce").fillna(0)
    discount = pd.to_numeric(frame.get("discount_amount", pd.Series("", index=frame.index)), errors="coerce").fillna(0)
    fallback = total.abs().where(total.ne(0), discount.abs().where(discount.ne(0), 0))
    if NORMAL_PRICE_COLUMN in frame.columns:
        raw = frame.get(NORMAL_PRICE_COLUMN, pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
        parsed = pd.to_numeric(raw, errors="coerce")
        return parsed.where(raw.ne("") & parsed.notna(), fallback).fillna(0)
    return fallback


def _net_sales_num(frame: pd.DataFrame) -> pd.Series:
    gross = _profit_sales_num(frame)
    discount = pd.to_numeric(frame.get("discount_amount", pd.Series("", index=frame.index)), errors="coerce").fillna(0)
    return (gross - discount.clip(lower=0)).clip(lower=0)


def _profit_sales_num(frame: pd.DataFrame) -> pd.Series:
    total = pd.to_numeric(frame.get("total_price", pd.Series("", index=frame.index)), errors="coerce").fillna(0)
    fallback = total.clip(lower=0)
    if PROFIT_SALES_COLUMN in frame.columns:
        raw = frame.get(PROFIT_SALES_COLUMN, pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
        parsed = pd.to_numeric(raw, errors="coerce")
        return parsed.where(raw.ne("") & parsed.notna(), fallback).fillna(0)
    return fallback


def _manual_profit_reference_sales_num(frame: pd.DataFrame) -> pd.Series:
    profit_sales = _profit_sales_num(frame)
    pre_discount = pd.to_numeric(frame.get(PRE_DISCOUNT_PRICE_COLUMN, pd.Series("", index=frame.index)), errors="coerce")
    total = pd.to_numeric(frame.get("total_price", pd.Series("", index=frame.index)), errors="coerce")
    discount = pd.to_numeric(frame.get("discount_amount", pd.Series("", index=frame.index)), errors="coerce")
    exception_type = frame.get(ORDER_EXCEPTION_TYPE_COLUMN, pd.Series("", index=frame.index)).astype(str).str.strip()
    salesless = exception_type.isin(SALESLESS_COUNTED_EXCEPTION_TYPES)
    reference = profit_sales.copy()
    reference = reference.where(reference.gt(0), pre_discount)
    reference = reference.where(reference.gt(0), discount.where(salesless))
    reference = reference.where(reference.gt(0), total.abs().where(salesless))
    return reference.fillna(0).clip(lower=0)


def _manual_profit_counted_qty_num(frame: pd.DataFrame) -> pd.Series:
    qty = pd.to_numeric(frame.get("qty", pd.Series("", index=frame.index)), errors="coerce").fillna(0)
    role = frame.get("line_role", pd.Series("", index=frame.index)).astype(str).str.strip()
    kind = frame.get("option_kind", pd.Series("", index=frame.index)).astype(str).str.strip()
    exception_type = frame.get(ORDER_EXCEPTION_TYPE_COLUMN, pd.Series("", index=frame.index)).astype(str).str.strip()
    usage_total = pd.to_numeric(frame.get(CHICKEN_USAGE_TOTAL_COLUMN, pd.Series("", index=frame.index)), errors="coerce").fillna(0)
    usage = pd.to_numeric(frame.get(CHICKEN_USAGE_COLUMN, pd.Series("", index=frame.index)), errors="coerce").fillna(0)
    counted = qty.copy()
    restore = exception_type.isin(SALESLESS_COUNTED_EXCEPTION_TYPES) & role.eq("main") & counted.le(0) & (usage_total.gt(0) | usage.gt(0))
    counted = counted.where(~restore, 1)
    absorbed_option = (
        role.ne("main")
        & (
            kind.isin([*CHICKEN_DECIDING_KINDS, OPTION_KIND_SPICE])
            | frame.get("_profit_absorb_to_parent", pd.Series(False, index=frame.index)).fillna(False).astype(bool)
        )
    )
    return counted.where(~absorbed_option, 0).fillna(0)


def _manual_profit_target(frame: pd.DataFrame) -> pd.Series:
    role = frame.get("line_role", pd.Series("", index=frame.index)).astype(str).str.strip()
    kind = frame.get("option_kind", pd.Series("", index=frame.index)).astype(str).str.strip()
    base = _profit_sales_num(frame)
    qty = _manual_profit_counted_qty_num(frame)
    discount = pd.to_numeric(frame.get("discount_amount", pd.Series("", index=frame.index)), errors="coerce").fillna(0)
    exception_type = frame.get(ORDER_EXCEPTION_TYPE_COLUMN, pd.Series("", index=frame.index)).astype(str).str.strip()
    sale_type = frame.get("sale_type", pd.Series("", index=frame.index)).astype(str).str.strip()
    counted_without_sales = discount.gt(0) | exception_type.isin(SALESLESS_COUNTED_EXCEPTION_TYPES)
    zero_price_cost_bearing = (
        base.eq(0)
        & qty.gt(0)
        & kind.isin(ZERO_PRICE_COST_BEARING_KINDS)
        & ~sale_type.eq("취소")
        & ~exception_type.isin({"취소", "테스트"})
    )
    return (
        role.isin(["main", "option", "side"])
        & (base.ne(0) | counted_without_sales | zero_price_cost_bearing)
        & ~kind.isin(NON_SALES_KINDS)
        & ~_cancel_offset_excluded(frame)
    )


def _mark_profit_set_component_options(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or not set(ORDER_GROUP_COLUMNS).issubset(frame.columns):
        return frame
    out = frame.copy()
    out["_profit_absorb_to_parent"] = False
    for _group_key, group in out.groupby(ORDER_GROUP_COLUMNS, dropna=False, sort=False):
        main_rows = group[group.get("line_role", pd.Series("", index=group.index)).astype(str).str.strip().eq("main")]
        if main_rows.empty:
            continue
        main = main_rows.iloc[0]
        family = _manual_profit_combo_menu_family(main)
        if not family.endswith("세트"):
            continue
        option_rows = _sort_frame_by_item_seq(
            group[group.get("line_role", pd.Series("", index=group.index)).astype(str).str.strip().isin(["option", "side"])]
        )
        component_indexes = [
            idx
            for idx, option in option_rows.iterrows()
            if _profit_combo_side_name(option.get("item_name", ""))
        ]
        if component_indexes:
            out.at[component_indexes[0], "_profit_absorb_to_parent"] = True
    return out


def _legacy_manual_profit_profile(row: pd.Series) -> tuple[str, str, str]:
    role = str(row.get("line_role", "") or "").strip()
    kind = str(row.get("option_kind", "") or "").strip()
    if role == "main" or kind in CHICKEN_DECIDING_KINDS or kind == OPTION_KIND_SPICE:
        name = str(row.get("std_menu_name", "") or "").strip()
        if not name:
            name = str(row.get("menu_name", "") or "").strip()
        if not name:
            name = _manual_profit_item_name(row)
        size = _manual_profit_size_axis(row)
        chicken_type = str(row.get("닭유형", "") or "").strip()
        key = "|".join(["메뉴", name, size, chicken_type])
        return key, name, OPTION_KIND_MAIN
    name = _manual_profit_item_name(row)
    key = "|".join(["품목", name])
    return key, name, kind


def _legacy_channel_manual_profit_profile(row: pd.Series) -> tuple[str, str, str]:
    role = str(row.get("line_role", "") or "").strip()
    kind = str(row.get("option_kind", "") or "").strip()
    channel = str(row.get(PROFIT_CHANNEL_COLUMN, "") or "").strip() or _profit_channel_from_values(
        row.get("source", ""),
        row.get("platform", ""),
        row.get("order_type", ""),
    )
    if role == "main" or kind in CHICKEN_DECIDING_KINDS or kind == OPTION_KIND_SPICE:
        name = str(row.get("std_menu_name", "") or "").strip()
        if not name:
            name = str(row.get("menu_name", "") or "").strip()
        if not name:
            name = _manual_profit_item_name(row)
        size = _manual_profit_size_axis(row)
        chicken_type = str(row.get("닭유형", "") or "").strip()
        key = "|".join(["메뉴", channel, name, size, chicken_type])
        return key, name, OPTION_KIND_MAIN
    name = _manual_profit_item_name(row)
    key = "|".join(["품목", channel, name])
    return key, name, kind


def _manual_profit_profile(row: pd.Series) -> tuple[str, str, str]:
    role = str(row.get("line_role", "") or "").strip()
    kind = str(row.get("option_kind", "") or "").strip()
    channel = str(row.get(PROFIT_CHANNEL_COLUMN, "") or "").strip() or _profit_channel_from_values(
        row.get("source", ""),
        row.get("platform", ""),
        row.get("order_type", ""),
    )
    if role == "main" or kind in CHICKEN_DECIDING_KINDS or kind == OPTION_KIND_SPICE or bool(row.get("_profit_absorb_to_parent", False)):
        name = _manual_profit_main_name(row) if role == "main" else _manual_profit_parent_name(row)
        size = _manual_profit_size_axis(row)
        parts = ["메뉴", channel, name, size]
        chicken_type = _manual_profit_chicken_type_axis(row)
        if chicken_type:
            parts.append(chicken_type)
        combo = _manual_profit_cost_combo(row)
        if combo:
            parts.append(combo)
        key = "|".join(parts)
        return key, name, OPTION_KIND_MAIN
    name = _manual_profit_item_name(row)
    if _manual_profit_needs_parent_axis(row):
        parts = ["품목", channel, _manual_profit_parent_name(row), _manual_profit_size_axis(row)]
        chicken_type = _manual_profit_chicken_type_axis(row)
        if chicken_type:
            parts.append(chicken_type)
        parts.append(name)
        key = "|".join(parts)
    else:
        key = "|".join(["품목", channel, name])
    return key, name, kind


def _manual_profit_rate_attrs() -> pd.DataFrame:
    master = _manual_or_legacy_sheet("수익률", MANUAL_PROFIT_RATE_MASTER_OUTPUT_PATH).fillna("")
    if master.empty:
        return pd.DataFrame(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS)
    return master.reindex(
        columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS,
        fill_value="",
    )


def _manual_profit_rate_preserved_attrs() -> pd.DataFrame:
    master = _read_manual_workbook_sheet(MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME).fillna("")
    if master.empty:
        return pd.DataFrame(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS)
    return master.reindex(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="")


def _has_manual_profit_amount(row: pd.Series) -> bool:
    return any(str(row.get(col, "") or "").strip() for col in MANUAL_PROFIT_RATE_AMOUNT_COLUMNS)


def _build_preserved_manual_profit_rate_rows(manual_profit_rate_master: pd.DataFrame) -> pd.DataFrame:
    previous = _manual_profit_rate_attrs()
    preserved = _manual_profit_rate_preserved_attrs()
    current_keys = set(
        manual_profit_rate_master.get("수익키", pd.Series(dtype=str)).astype(str).str.strip()
        if not manual_profit_rate_master.empty
        else []
    )
    # 이번 회차 수익률 시트가 실제로 들고 있는 (완화키, 금액) 조합.
    # 축이 바뀌어 키만 달라진 채 값이 되살아난 행은 보존시트에서 내린다.
    migrated: dict[str, dict[str, set[str]]] = {}
    if not manual_profit_rate_master.empty:
        current = manual_profit_rate_master.reindex(
            columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value=""
        ).fillna("")
        for _, row in current.iterrows():
            relaxed = _relaxed_manual_profit_key(row.get("수익키", ""))
            if not relaxed:
                continue
            slot = migrated.setdefault(relaxed, {})
            for col in MANUAL_PROFIT_RATE_AMOUNT_COLUMNS:
                value = str(row.get(col, "") or "").strip()
                if value:
                    slot.setdefault(col, set()).add(value)

    def _already_migrated(row: pd.Series) -> bool:
        """이 보존행이 들고 있던 금액이 지금 수익률 시트에 전부 살아 있으면 내려도 된다."""
        relaxed = _relaxed_manual_profit_key(row.get("수익키", ""))
        if not relaxed:
            return False
        slot = migrated.get(relaxed)
        if not slot:
            return False
        for col in MANUAL_PROFIT_RATE_AMOUNT_COLUMNS:
            value = str(row.get(col, "") or "").strip()
            if value and value not in slot.get(col, set()):
                return False
        return True

    frames: list[pd.DataFrame] = []
    if not previous.empty:
        prev = previous.reindex(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="").fillna("")
        manual_rows = prev.apply(_has_manual_profit_amount, axis=1)
        unmatched = ~prev.get("수익키", pd.Series("", index=prev.index)).astype(str).str.strip().isin(current_keys)
        keep = prev[manual_rows & unmatched].copy()
        if not keep.empty:
            frames.append(keep)
    if not preserved.empty:
        kept_preserved = preserved.reindex(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="").fillna("")
        # 키가 원래대로 돌아온 행은 이제 수익률 시트가 다시 들고 있으므로 보존시트에서 내린다.
        kept_preserved = kept_preserved[
            ~kept_preserved.get("수익키", pd.Series("", index=kept_preserved.index))
            .astype(str)
            .str.strip()
            .isin(current_keys)
        ]
        if not kept_preserved.empty:
            frames.append(kept_preserved)
    if not frames:
        return pd.DataFrame(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS)
    out = pd.concat(frames, ignore_index=True, sort=False).fillna("")
    out = out[out.apply(_has_manual_profit_amount, axis=1)].copy()
    if not out.empty and migrated:
        out = out[~out.apply(_already_migrated, axis=1)].copy()
    if out.empty:
        return pd.DataFrame(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS)
    if "수익키" in out.columns:
        out["_has_key"] = out["수익키"].astype(str).str.strip().ne("")
        keyed = out[out["_has_key"]].drop_duplicates(subset=["수익키"], keep="last")
        unkeyed = out[~out["_has_key"]]
        out = pd.concat([keyed, unkeyed], ignore_index=True, sort=False).drop(columns=["_has_key"], errors="ignore")
    out = _normalize_takeout_manual_profit_costs(out.reindex(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value=""))
    return out.reindex(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="")


def _format_manual_profit_rate(rate: float | None) -> str:
    if rate is None:
        return ""
    return f"{rate:g}"


def _parse_manual_profit_amount(value: object) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = pd.to_numeric(pd.Series([text.replace(",", "")]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _manual_profit_effective_price(row: pd.Series) -> tuple[float | None, str]:
    manual_price = _parse_manual_profit_amount(row.get(LEGACY_MANUAL_PRICE_COLUMN, ""))
    if manual_price is not None:
        return manual_price, "수기"
    auto_price = _parse_manual_profit_amount(row.get("판매가", ""))
    if auto_price is not None:
        return auto_price, "자동"
    return None, ""


def _manual_profit_rate_from_cost(row: pd.Series) -> tuple[float | None, str]:
    price, _ = _manual_profit_effective_price(row)
    if price is None:
        return None, "판매가미산출"
    if price == 0:
        return None, "판매가0"

    channel = str(row.get(PROFIT_CHANNEL_COLUMN, row.get("수익채널", "")) or "").strip()
    is_takeout = channel == "홀_포장" or "포장" in channel
    menu_cost = _parse_manual_profit_amount(row.get("메뉴원가_manual", ""))
    if menu_cost is None:
        return None, "원가_manual미입력"
    setting_cost = 0.0 if is_takeout else (_parse_manual_profit_amount(row.get("상차림비_manual", "")) or 0.0)
    cost = menu_cost + setting_cost
    return 1.0 - (cost / price), ""


def _manual_profit_cost_from_cost(row: pd.Series) -> tuple[float | None, str]:
    channel = str(row.get(PROFIT_CHANNEL_COLUMN, row.get("수익채널", "")) or "").strip()
    is_takeout = channel == "홀_포장" or "포장" in channel
    menu_cost = _parse_manual_profit_amount(row.get("메뉴원가_manual", ""))
    if menu_cost is None:
        return None, "원가_manual미입력"
    setting_cost = 0.0 if is_takeout else (_parse_manual_profit_amount(row.get("상차림비_manual", "")) or 0.0)
    return menu_cost + setting_cost, ""


def _normalize_manual_profit_amounts(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    if "판매가" not in out.columns:
        out["판매가"] = ""
    if LEGACY_MANUAL_PRICE_COLUMN not in out.columns:
        out[LEGACY_MANUAL_PRICE_COLUMN] = ""
    if "판매가기준" not in out.columns:
        out["판매가기준"] = ""
    if SETTING_INCLUDED_COST_COLUMN not in out.columns:
        out[SETTING_INCLUDED_COST_COLUMN] = ""
    sales_basis = out.get(
        "기준판매가합계",
        out.get(GROSS_SALES_TOTAL_COLUMN, out.get("매출합계", pd.Series("", index=out.index))),
    )
    sales = pd.to_numeric(sales_basis, errors="coerce")
    qty = pd.to_numeric(out.get("판매수량", pd.Series("", index=out.index)), errors="coerce")
    auto_price = sales / qty.where(qty.ne(0))
    out["판매가"] = auto_price.map(lambda value: "" if pd.isna(value) else _format_number(value))
    price_basis = []
    for idx, row in out.iterrows():
        _, basis = _manual_profit_effective_price(row)
        price_basis.append(basis)
    out["판매가기준"] = price_basis
    channel = out.get(PROFIT_CHANNEL_COLUMN, pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    takeout = channel.eq("홀_포장") | channel.str.contains("포장", regex=False, na=False)
    # 포장 채널은 상차림비를 계산에서만 0으로 본다. 입력 셀은 사용자가 적은 값 그대로 둔다.
    menu_cost = out.get("메뉴원가_manual", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    setting_cost = out.get("상차림비_manual", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    for idx in out.index:
        menu_value = _parse_manual_profit_amount(menu_cost.at[idx] if idx in menu_cost.index else "")
        if menu_value is None:
            out.at[idx, SETTING_INCLUDED_COST_COLUMN] = ""
            continue
        setting_value = 0.0 if bool(takeout.at[idx]) else (_parse_manual_profit_amount(setting_cost.at[idx] if idx in setting_cost.index else "") or 0.0)
        out.at[idx, SETTING_INCLUDED_COST_COLUMN] = _format_number(menu_value + setting_value)
    return out


def _normalize_takeout_manual_profit_costs(frame: pd.DataFrame) -> pd.DataFrame:
    return _normalize_manual_profit_amounts(frame)


AXIS_MIGRATED_MEMO = "축변경 이관: 사이즈/닭유형 재확인 필요"
_MANUAL_PROFIT_MEMO_AUTO_TAGS = {
    "기존수익키 이관: 채널별 재검토 필요",
    "기존 묶음값 이관: 종류별 재검토 필요",
    AXIS_MIGRATED_MEMO,
}


def _relaxed_manual_profit_key(key: object) -> str:
    """사이즈/닭유형 축을 뺀 완화 수익키.

    분류가 흔들려 축이 바뀌어도 같은 품목의 수기값을 다시 찾아 붙이기 위한 마지막 폴백이다.
    """
    parts = str(key or "").strip().split("|")
    if len(parts) < 5:
        return ""
    if parts[0] == "품목":
        # 품목|채널|부모메뉴명|사이즈[|닭유형]|옵션명 -> 품목|채널|부모메뉴명|옵션명
        return "|".join([*parts[:3], parts[-1]])
    if parts[0] == "메뉴":
        # 메뉴|채널|메뉴명|사이즈|닭유형[|조합] -> 메뉴|채널|메뉴명|사이즈[|조합]
        return "|".join([*parts[:4], *parts[5:]])
    return ""


def _clean_manual_profit_memo(value: object) -> str:
    parts = [
        part.strip()
        for part in str(value or "").split("|")
        if part.strip() and part.strip() not in _MANUAL_PROFIT_MEMO_AUTO_TAGS
    ]
    return " | ".join(_unique_nonempty(parts))


def _build_manual_profit_rate_master(left_joined: pd.DataFrame) -> pd.DataFrame:
    """품목별 판매가/원가 입력표. 수익률은 금액 컬럼에서 자동 계산한다."""
    previous = _manual_profit_rate_attrs()
    preserved_previous = _manual_profit_rate_preserved_attrs()

    def _kept_values(row: pd.Series) -> dict[str, str]:
        return {
            LEGACY_MANUAL_PRICE_COLUMN: str(row.get(LEGACY_MANUAL_PRICE_COLUMN, "") or "").strip(),
            "메뉴원가_manual": str(row.get("메뉴원가_manual", "") or "").strip(),
            "상차림비_manual": str(row.get("상차림비_manual", "") or "").strip(),
            "메모": _clean_manual_profit_memo(row.get("메모", "")),
        }

    kept = {}
    # 보존시트를 먼저 깔고 수익률 시트로 덮는다. 같은 키면 수익률 시트가 이긴다.
    for source_frame in (preserved_previous, previous):
        if source_frame is None or source_frame.empty:
            continue
        for _, row in source_frame.fillna("").iterrows():
            key = str(row.get("수익키", "") or "").strip()
            if key:
                kept[key] = _kept_values(row)

    # 축이 흔들려 키가 바뀐 행을 되찾기 위한 완화키 색인.
    # 보존시트(= 갈 곳을 잃은 값)만 재료로 쓴다. 수익률 시트끼리 옆줄 값을 퍼오면
    # 담당자가 일부러 비운 칸까지 다시 채워버린다.
    # 금액 컬럼별로 따로 본다. 한 컬럼만 채운 행과 전부 채운 행이 같은 완화키에 걸리는 일이 흔해서,
    # 행 단위로 비교하면 멀쩡한 값까지 "충돌"로 버려진다.
    relaxed_candidates: dict[str, dict[str, set[str]]] = {}
    if preserved_previous is not None and not preserved_previous.empty:
        for _, row in preserved_previous.fillna("").iterrows():
            key = str(row.get("수익키", "") or "").strip()
            relaxed = _relaxed_manual_profit_key(key)
            if not relaxed or relaxed == key:
                continue
            values = _kept_values(row)
            slot = relaxed_candidates.setdefault(relaxed, {})
            for col in MANUAL_PROFIT_RATE_AMOUNT_COLUMNS:
                if values.get(col, ""):
                    slot.setdefault(col, set()).add(values[col])
    relaxed_kept: dict[str, dict[str, str]] = {}
    for relaxed, per_column in relaxed_candidates.items():
        agreed = {col: next(iter(values)) for col, values in per_column.items() if len(values) == 1}
        if agreed:
            relaxed_kept[relaxed] = agreed

    if left_joined.empty:
        return pd.DataFrame(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS)
    work = _attach_profit_channel(left_joined).fillna("")
    work = _mark_profit_set_component_options(work)
    work[PROFIT_SALES_COLUMN] = _profit_sales_num(work).map(_format_number)
    all_work = _ensure_order_group_columns(work)
    parent_profit_rows: dict[tuple[str, ...], pd.Series] = {}
    main_all = all_work[
        all_work.get("line_role", pd.Series("", index=all_work.index)).astype(str).str.strip().eq("main")
    ]
    if not main_all.empty:
        for group_key, group in main_all.groupby(ORDER_GROUP_COLUMNS, dropna=False, sort=False):
            parent_profit_rows[tuple(str(value) for value in group_key)] = group.iloc[0]
    target = _manual_profit_target(work)
    work = work[target].copy()
    if work.empty:
        return pd.DataFrame(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS)
    work = _ensure_order_group_columns(work)

    profiles = work.apply(_manual_profit_profile, axis=1)
    legacy_profiles = work.apply(_legacy_manual_profit_profile, axis=1)
    legacy_channel_profiles = work.apply(_legacy_channel_manual_profit_profile, axis=1)
    work["수익키"] = [p[0] for p in profiles]
    work["기존수익키"] = [p[0] for p in legacy_profiles]
    work["기존채널수익키"] = [p[0] for p in legacy_channel_profiles]
    work["기존채널무닭유형수익키"] = [
        "|".join(["메뉴", str(row.get(PROFIT_CHANNEL_COLUMN, "") or "").strip(), _manual_profit_parent_name(row), _manual_profit_size_axis(row)])
        if str(row.get("line_role", "") or "").strip() == "main" or str(row.get("option_kind", "") or "").strip() in CHICKEN_DECIDING_KINDS or str(row.get("option_kind", "") or "").strip() == OPTION_KIND_SPICE
        else ""
        for _, row in work.iterrows()
    ]
    work["기존채널부모수익키"] = [
        "|".join(["품목", str(row.get(PROFIT_CHANNEL_COLUMN, "") or "").strip(), _manual_profit_parent_name(row), _manual_profit_item_name(row)])
        if _manual_profit_needs_parent_axis(row)
        else ""
        for _, row in work.iterrows()
    ]
    work["기존채널부모사이즈수익키"] = [
        "|".join(["품목", str(row.get(PROFIT_CHANNEL_COLUMN, "") or "").strip(), _manual_profit_parent_name(row), _manual_profit_size_axis(row), _manual_profit_item_name(row)])
        if _manual_profit_needs_parent_axis(row)
        else ""
        for _, row in work.iterrows()
    ]
    work["기존채널음료묶음수익키"] = [
        _legacy_channel_drink_group_profit_key(row)
        for _, row in work.iterrows()
    ]
    work["대표품목명"] = [p[1] for p in profiles]
    work["사이즈_profit"] = [_manual_profit_size_axis(row) if _manual_profit_needs_parent_axis(row) or p[2] == OPTION_KIND_MAIN else "" for (_, row), p in zip(work.iterrows(), profiles)]
    work["닭유형_profit"] = [_manual_profit_chicken_type_axis(row) if _manual_profit_needs_parent_axis(row) or p[2] == OPTION_KIND_MAIN else "" for (_, row), p in zip(work.iterrows(), profiles)]
    work["계산사이즈_profit"] = [_manual_profit_size(row) if _manual_profit_needs_parent_axis(row) or p[2] == OPTION_KIND_MAIN else "" for (_, row), p in zip(work.iterrows(), profiles)]
    work["계산닭유형_profit"] = [_manual_profit_chicken_type(row) if _manual_profit_needs_parent_axis(row) or p[2] == OPTION_KIND_MAIN else "" for (_, row), p in zip(work.iterrows(), profiles)]
    work["option_kind_profit"] = [p[2] for p in profiles]
    absorbed_option = (
        work.get("line_role", pd.Series("", index=work.index)).astype(str).str.strip().ne("main")
        & (
            work.get("option_kind", pd.Series("", index=work.index)).astype(str).str.strip().isin([*CHICKEN_DECIDING_KINDS, OPTION_KIND_SPICE])
            | work.get("_profit_absorb_to_parent", pd.Series(False, index=work.index)).fillna(False).astype(bool)
        )
    )
    if absorbed_option.any():
        for idx, row in work[absorbed_option].iterrows():
            parent = parent_profit_rows.get(tuple(str(row.get(col, "") or "").strip() for col in ORDER_GROUP_COLUMNS))
            if parent is None:
                continue
            profile = _manual_profit_profile(parent)
            legacy_profile = _legacy_manual_profit_profile(parent)
            legacy_channel_profile = _legacy_channel_manual_profit_profile(parent)
            parent_channel = str(parent.get(PROFIT_CHANNEL_COLUMN, "") or "").strip()
            work.at[idx, "수익키"] = profile[0]
            work.at[idx, "기존수익키"] = legacy_profile[0]
            work.at[idx, "기존채널수익키"] = legacy_channel_profile[0]
            work.at[idx, "기존채널무닭유형수익키"] = "|".join(
                ["메뉴", parent_channel, _manual_profit_parent_name(parent), _manual_profit_size_axis(parent)]
            )
            work.at[idx, "대표품목명"] = profile[1]
            work.at[idx, "사이즈_profit"] = _manual_profit_size_axis(parent)
            work.at[idx, "닭유형_profit"] = _manual_profit_chicken_type_axis(parent)
            work.at[idx, "계산사이즈_profit"] = _manual_profit_size(parent)
            work.at[idx, "계산닭유형_profit"] = _manual_profit_chicken_type(parent)
    dependent_option = work.apply(_manual_profit_needs_parent_axis, axis=1) & ~absorbed_option
    if dependent_option.any():
        for idx, row in work[dependent_option].iterrows():
            parent = parent_profit_rows.get(tuple(str(row.get(col, "") or "").strip() for col in ORDER_GROUP_COLUMNS))
            if parent is None:
                continue
            legacy_parent_key = "|".join(["품목", str(parent.get(PROFIT_CHANNEL_COLUMN, "") or "").strip(), _manual_profit_parent_name(parent), _manual_profit_item_name(row)])
            legacy_parent_size_key = "|".join(
                ["품목", str(parent.get(PROFIT_CHANNEL_COLUMN, "") or "").strip(), _manual_profit_parent_name(parent), _manual_profit_size_axis(parent), _manual_profit_item_name(row)]
            )
            name = _manual_profit_item_name(row)
            parts = ["품목", str(parent.get(PROFIT_CHANNEL_COLUMN, "") or "").strip(), _manual_profit_parent_name(parent), _manual_profit_size_axis(parent)]
            chicken_type = _manual_profit_chicken_type_axis(parent)
            if chicken_type:
                parts.append(chicken_type)
            parts.append(name)
            work.at[idx, "수익키"] = "|".join(parts)
            work.at[idx, "기존채널부모수익키"] = legacy_parent_key
            work.at[idx, "기존채널부모사이즈수익키"] = legacy_parent_size_key
            work.at[idx, "대표품목명"] = name
            work.at[idx, "사이즈_profit"] = _manual_profit_size_axis(parent)
            work.at[idx, "닭유형_profit"] = _manual_profit_chicken_type_axis(parent)
            work.at[idx, "계산사이즈_profit"] = _manual_profit_size(parent)
            work.at[idx, "계산닭유형_profit"] = _manual_profit_chicken_type(parent)
    profile_lookup = _menu_chicken_profile_lookup(_menu_chicken_profile_attrs())

    def ignored_by_menu_profile(row: pd.Series) -> bool:
        if str(row.get("line_role", "") or "").strip() == "main":
            return False
        if str(row.get("option_kind", "") or "").strip() not in CHICKEN_DECIDING_KINDS:
            return False
        key = (
            str(row.get("source", "") or "").strip(),
            str(row.get("brand", "") or "").strip(),
            str(row.get("store", "") or "").strip(),
            _manual_profit_parent_name(row),
        )
        profile = profile_lookup.get(key)
        if profile is None:
            suggested = _suggest_menu_chicken_profile(key[-1])
            profile = _effective_menu_chicken_profile(pd.Series(suggested))
        if not profile or bool(profile.get("apply_option_type", False)):
            return False
        allowed_types = [str(value) for value in profile.get("allowed_types", []) if str(value or "").strip()]
        if not allowed_types:
            return False
        signals = _infer_chicken_types(row.get("item_name", ""))
        return bool(signals and any(signal not in allowed_types for signal in signals))

    ignored_profile_option = work.apply(ignored_by_menu_profile, axis=1)
    work["원본품목명_profit"] = work.get("item_name", pd.Series("", index=work.index)).where(~ignored_profile_option, "")
    main_line = work.get("line_role", pd.Series("", index=work.index)).astype(str).str.strip().eq("main")
    absorbed_kind = work.get("option_kind", pd.Series("", index=work.index)).astype(str).str.strip().isin(
        [*CHICKEN_DECIDING_KINDS, OPTION_KIND_SPICE]
    )
    work["대표품목원문_profit"] = work.get("item_name", pd.Series("", index=work.index)).where(main_line, "")
    work["원본품목명목록_profit"] = work["원본품목명_profit"].where(main_line | ~absorbed_kind, "")
    work["qty_num"] = _manual_profit_counted_qty_num(work)
    work["total_num"] = _net_sales_num(work)
    work["gross_total_num"] = _normal_price_num(work)
    work["reference_total_num"] = work["gross_total_num"].where(
        work["gross_total_num"].ne(0),
        _manual_profit_reference_sales_num(work).where(~absorbed_option, 0),
    )
    folded_candidates: dict[str, str] = {}
    candidate_sales = (
        work[work["기존채널수익키"].isin(kept)]
        .groupby(["수익키", "기존채널수익키"], dropna=False, sort=False)["total_num"]
        .sum()
        .reset_index()
    )
    if not candidate_sales.empty:
        candidate_sales["_abs_sales"] = candidate_sales["total_num"].abs()
        candidate_sales = candidate_sales.sort_values("_abs_sales", ascending=False)
        folded_candidates = (
            candidate_sales.drop_duplicates(subset=["수익키"], keep="first")
            .set_index("수익키")["기존채널수익키"]
            .astype(str)
            .to_dict()
        )
    work[COST_COMBO_COLUMN] = work.apply(_manual_profit_cost_combo, axis=1)
    if absorbed_option.any():
        for idx, row in work[absorbed_option].iterrows():
            parent = parent_profit_rows.get(tuple(str(row.get(col, "") or "").strip() for col in ORDER_GROUP_COLUMNS))
            if parent is not None:
                work.at[idx, COST_COMBO_COLUMN] = _manual_profit_cost_combo(parent)
    for col in ("menu_name", "item_name", OPTION_COMBO_COLUMN, CHICKEN_SIGNAL_COLUMN, COST_COMBO_COLUMN):
        if col not in work.columns:
            work[col] = ""
    grouped = (
        work.groupby([PROFIT_CHANNEL_COLUMN, "수익키", "대표품목명", "사이즈_profit", "닭유형_profit", "option_kind_profit"], dropna=False, sort=False)
        .agg(
            기존수익키=("기존수익키", lambda s: _first_clean_value(s)),
            기존채널수익키=("기존채널수익키", lambda s: _first_clean_value(s)),
            기존채널무닭유형수익키=("기존채널무닭유형수익키", lambda s: _first_clean_value(s)),
            기존채널부모수익키=("기존채널부모수익키", lambda s: _first_clean_value(s)),
            기존채널부모사이즈수익키=("기존채널부모사이즈수익키", lambda s: _first_clean_value(s)),
            기존채널음료묶음수익키=("기존채널음료묶음수익키", lambda s: _first_clean_value(s)),
            source목록=("source", lambda s: " | ".join(_unique_nonempty([str(v).strip() for v in s]))),
            platform목록=("platform", lambda s: " | ".join(_unique_nonempty([str(v).strip() for v in s]))),
            order_type목록=("order_type", lambda s: " | ".join(_unique_nonempty([str(v).strip() for v in s]))),
            대표주문메뉴명=("menu_name", lambda s: _first_clean_value(s)),
            대표품목원문=("대표품목원문_profit", lambda s: _first_clean_value(s)),
            대표옵션조합=(OPTION_COMBO_COLUMN, lambda s: _first_clean_value(s)),
            **{COST_COMBO_COLUMN: (COST_COMBO_COLUMN, lambda s: _first_clean_value(s))},
            계산닭유형=("계산닭유형_profit", lambda s: " | ".join(_unique_nonempty([str(v).strip() for v in s]))),
            계산사이즈=("계산사이즈_profit", lambda s: " | ".join(_unique_nonempty([str(v).strip() for v in s]))),
            **{CHICKEN_SIGNAL_COLUMN: (CHICKEN_SIGNAL_COLUMN, lambda s: " | ".join(_unique_nonempty([str(v).strip() for v in s])))},
            원본품목명목록=("원본품목명목록_profit", lambda s: " | ".join(_unique_nonempty([str(v).strip() for v in s])[:8])),
            주문건수=("order_id", "nunique"),
            판매수량=("qty_num", "sum"),
            매출합계=("total_num", "sum"),
            **{GROSS_SALES_TOTAL_COLUMN: ("gross_total_num", "sum")},
            기준판매가합계=("reference_total_num", "sum"),
        )
        .reset_index()
        .rename(columns={"사이즈_profit": "사이즈", "닭유형_profit": "닭유형", "option_kind_profit": "option_kind"})
    )
    for col in ("주문건수", "판매수량", "매출합계", GROSS_SALES_TOTAL_COLUMN):
        grouped[col] = grouped[col].map(_format_number)
    def inherited(row: pd.Series, field: str) -> str:
        key = str(row.get("수익키", "") or "").strip()
        legacy_key = str(row.get("기존수익키", "") or "").strip()
        legacy_channel_key = str(row.get("기존채널수익키", "") or "").strip()
        legacy_no_chicken_key = str(row.get("기존채널무닭유형수익키", "") or "").strip()
        legacy_parent_key = str(row.get("기존채널부모수익키", "") or "").strip()
        legacy_parent_size_key = str(row.get("기존채널부모사이즈수익키", "") or "").strip()
        legacy_drink_group_key = str(row.get("기존채널음료묶음수익키", "") or "").strip()
        if key in kept:
            return kept[key].get(field, "")
        if legacy_key in kept:
            return kept[legacy_key].get(field, "")
        if legacy_channel_key in kept:
            return kept[legacy_channel_key].get(field, "")
        if legacy_no_chicken_key in kept:
            return kept[legacy_no_chicken_key].get(field, "")
        if legacy_parent_key in kept:
            return kept[legacy_parent_key].get(field, "")
        if legacy_parent_size_key in kept:
            return kept[legacy_parent_size_key].get(field, "")
        if legacy_drink_group_key in kept:
            value = kept[legacy_drink_group_key].get(field, "")
            if field == "메모":
                return " | ".join(_unique_nonempty([value, "기존 묶음값 이관: 종류별 재검토 필요"]))
            return value
        folded_key = folded_candidates.get(key, "")
        if folded_key in kept:
            return kept[folded_key].get(field, "")
        return ""

    def inherited_with_recovery(row: pd.Series, field: str) -> tuple[str, bool]:
        """폴백 체인에서 값을 못 찾으면 완화키로 보존시트 값을 되살린다.

        체인이 "빈 값이 든 행"에 걸려 멈추는 경우까지 되살려야 한다. 담당자가 채우기 전의
        빈 행은 이번 회차에도 그대로 빈 행으로 존재하기 때문에, 그냥 두면 완화키까지 못 간다.
        """
        value = inherited(row, field)
        if value:
            return value, False
        relaxed_key = _relaxed_manual_profit_key(str(row.get("수익키", "") or "").strip())
        if not relaxed_key:
            return value, False
        recovered = relaxed_kept.get(relaxed_key, {}).get(field, "")
        return (recovered, True) if recovered else (value, False)

    def has_existing_manual_profit_row(row: pd.Series) -> bool:
        key = str(row.get("수익키", "") or "").strip()
        keys = [
            key,
            str(row.get("기존수익키", "") or "").strip(),
            str(row.get("기존채널수익키", "") or "").strip(),
            str(row.get("기존채널무닭유형수익키", "") or "").strip(),
            str(row.get("기존채널부모수익키", "") or "").strip(),
            str(row.get("기존채널부모사이즈수익키", "") or "").strip(),
            str(row.get("기존채널음료묶음수익키", "") or "").strip(),
            folded_candidates.get(key, ""),
        ]
        if any(candidate in kept for candidate in keys if candidate):
            return True
        relaxed_key = _relaxed_manual_profit_key(key)
        return bool(relaxed_key and relaxed_kept.get(relaxed_key))

    recovered_rows: set[int] = set()
    for col in (LEGACY_MANUAL_PRICE_COLUMN, "메뉴원가_manual", "상차림비_manual"):
        values = []
        for idx, row in grouped.iterrows():
            value, recovered = inherited_with_recovery(row, col)
            values.append(value)
            if recovered:
                recovered_rows.add(idx)
        grouped[col] = values
    grouped["메모"] = [
        " | ".join(_unique_nonempty([inherited(row, "메모"), AXIS_MIGRATED_MEMO]))
        if idx in recovered_rows
        else inherited(row, "메모")
        for idx, row in grouped.iterrows()
    ]
    for idx, row in grouped.iterrows():
        if has_existing_manual_profit_row(row):
            continue
        seed_key = (
            str(row.get("대표품목명", "") or "").strip(),
            str(row.get("사이즈", "") or "").strip(),
            str(row.get("닭유형", "") or "").strip(),
            str(row.get(COST_COMBO_COLUMN, "") or "").strip(),
        )
        seed = _MANUAL_PROFIT_COST_SEEDS.get(seed_key)
        if not seed:
            continue
        seed_cost, seed_price = seed
        if not str(grouped.at[idx, "메뉴원가_manual"] or "").strip():
            grouped.at[idx, "메뉴원가_manual"] = seed_cost
        if not str(grouped.at[idx, LEGACY_MANUAL_PRICE_COLUMN] or "").strip():
            grouped.at[idx, LEGACY_MANUAL_PRICE_COLUMN] = seed_price
    grouped = _normalize_manual_profit_amounts(grouped)
    grouped = grouped.drop(columns=["기준판매가합계"], errors="ignore")
    grouped = grouped.drop(
        columns=[
            "기존수익키",
            "기존채널수익키",
            "기존채널무닭유형수익키",
            "기존채널부모수익키",
            "기존채널부모사이즈수익키",
            "기존채널음료묶음수익키",
        ],
        errors="ignore",
    )
    return grouped.reindex(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="").sort_values(
        "매출합계",
        key=lambda s: pd.to_numeric(s, errors="coerce").fillna(0),
        ascending=False,
    )


def _attach_manual_profit_columns(
    left_joined: pd.DataFrame,
    rate_master: pd.DataFrame | None = None,
) -> pd.DataFrame:
    out = _attach_profit_channel(left_joined)
    out[PROFIT_SALES_COLUMN] = _profit_sales_num(out).map(_format_number)
    for col in MANUAL_PROFIT_COLUMNS:
        out[col] = ""
    if out.empty:
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")
    profiles = out.apply(_manual_profit_profile, axis=1)
    out["수익키"] = [p[0] for p in profiles]
    out["수익품목명"] = [p[1] for p in profiles]

    master = _manual_profit_rate_attrs() if rate_master is None else rate_master
    rates: dict[str, float] = {}
    costs: dict[str, float] = {}
    prices: dict[str, float] = {}
    base_prices: dict[str, float] = {}
    missing_reasons: dict[str, str] = {}
    if not master.empty and "수익키" in master.columns:
        for _, row in master.iterrows():
            key = str(row.get("수익키", "") or "").strip()
            rate, reason = _manual_profit_rate_from_cost(row)
            cost, cost_reason = _manual_profit_cost_from_cost(row)
            if not key:
                continue
            manual_price = _parse_manual_profit_amount(row.get(LEGACY_MANUAL_PRICE_COLUMN, ""))
            if manual_price is not None:
                prices[key] = manual_price
            # 원가율 기준 단가는 수기값 우선, 없으면 자동 판매가.
            base_price, _ = _manual_profit_effective_price(row)
            if base_price is not None:
                base_prices[key] = base_price
            if rate is not None:
                rates[key] = rate
            if cost is not None:
                costs[key] = cost
            elif cost_reason:
                missing_reasons[key] = cost_reason
            if rate is None and key not in missing_reasons:
                missing_reasons[key] = reason

    total = _profit_sales_num(out)
    target = _manual_profit_target(out)
    def lookup_value(row: pd.Series, values: dict[str, float]) -> float | None:
        key = str(row.get("수익키", "") or "").strip()
        value = values.get(key)
        if value is not None:
            return value
        fallback_key = _legacy_channel_drink_group_profit_key(row)
        if fallback_key:
            return values.get(fallback_key)
        return None

    def lookup_missing_reason(row: pd.Series) -> str:
        key = str(row.get("수익키", "") or "").strip()
        reason = missing_reasons.get(key, "")
        if reason:
            return reason
        fallback_key = _legacy_channel_drink_group_profit_key(row)
        return missing_reasons.get(fallback_key, "") if fallback_key else ""

    rate_values = [lookup_value(row, rates) for _, row in out.iterrows()]
    cost_values = [lookup_value(row, costs) for _, row in out.iterrows()]
    price_values = [lookup_value(row, prices) for _, row in out.iterrows()]
    qty = _manual_profit_counted_qty_num(out)
    # 쿠팡이츠처럼 옵션 행에 매출이 안 실리는 채널은 실매출이 0이라 원가만 잡힌다.
    # 수기 판매가가 있으면 그것을 매출 기준으로 쓴다. 없으면 종전대로 실매출을 쓴다.
    sales_basis = [
        MANUAL_PROFIT_SALES_BASIS_MANUAL
        if (is_target and line_total <= 0 and price is not None and qty_value > 0)
        else MANUAL_PROFIT_SALES_BASIS_ACTUAL
        for line_total, price, qty_value, is_target in zip(total, price_values, qty, target)
    ]
    effective_total = [
        float(price * qty_value) if basis == MANUAL_PROFIT_SALES_BASIS_MANUAL else float(line_total)
        for line_total, price, qty_value, basis in zip(total, price_values, qty, sales_basis)
    ]
    cost_total = [
        float(cost * qty_value) if is_target and cost is not None else None
        for cost, qty_value, is_target in zip(cost_values, qty, target)
    ]
    manual_profit = [
        f"{float(line_total - cost):g}" if is_target and cost is not None else ""
        for line_total, cost, is_target in zip(effective_total, cost_total, target)
    ]
    # 원가율 전용 열. 단가 x 수량이라 할인.번들이 안 섞인다.
    # 원가율 = 1 - SUM(원가_기준수익) / SUM(원가_기준매출)
    base_price_values = [lookup_value(row, base_prices) for _, row in out.iterrows()]
    base_sales = [
        float(price * qty_value) if is_target and price is not None else None
        for price, qty_value, is_target in zip(base_price_values, qty, target)
    ]
    out[COST_BASE_SALES_COLUMN] = [
        _format_number(value) if value is not None and cost is not None else ""
        for value, cost in zip(base_sales, cost_total)
    ]
    out[COST_BASE_PROFIT_COLUMN] = [
        _format_number(value - cost) if value is not None and cost is not None else ""
        for value, cost in zip(base_sales, cost_total)
    ]
    out[MANUAL_PROFIT_SALES_BASIS_COLUMN] = [
        basis if is_target and cost is not None else ""
        for basis, cost, is_target in zip(sales_basis, cost_values, target)
    ]
    out[MANUAL_PROFIT_QTY_COLUMN] = [
        _format_number(qty_value) if is_target and cost is not None else ""
        for qty_value, cost, is_target in zip(qty, cost_values, target)
    ]
    out[MANUAL_PROFIT_COST_COLUMN] = [
        _format_number(cost) if cost is not None else ""
        for cost in cost_total
    ]
    out["수기수익"] = manual_profit
    out["수익미산출사유"] = [
        "" if (not is_target or cost is not None) else (lookup_missing_reason(row) or "원가_manual미입력")
        for (_, row), cost, is_target in zip(out.iterrows(), cost_values, target)
    ]
    manual_rate_text = pd.Series([_format_manual_profit_rate(rate) for rate in rate_values], index=out.index)
    manual_profit_text = out["수기수익"].astype(str).str.strip()
    out["수익률"] = manual_rate_text.where(manual_rate_text.ne(""), out.get("수익률", ""))
    out["추정수익"] = manual_profit_text.where(manual_profit_text.ne(""), out.get("추정수익", ""))
    non_target = ~target
    out.loc[non_target, MANUAL_PROFIT_COLUMNS] = ""
    return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")


def _build_manual_profit_summary(left_joined: pd.DataFrame) -> pd.DataFrame:
    """사용자 입력 수익률 기반 품목별 수익 요약."""
    if left_joined.empty:
        return pd.DataFrame(columns=MANUAL_PROFIT_SUMMARY_COLUMNS)
    work = _attach_profit_channel(left_joined).fillna("")
    work[PROFIT_SALES_COLUMN] = _profit_sales_num(work).map(_format_number)
    target = _manual_profit_target(work)
    work = work[target].copy()
    if work.empty:
        return pd.DataFrame(columns=MANUAL_PROFIT_SUMMARY_COLUMNS)
    work["qty_num"] = _manual_profit_counted_qty_num(work)
    actual_total = _profit_sales_num(work)
    work["actual_total_num"] = actual_total
    # 수익은 _attach_manual_profit_columns가 쓴 기준(실매출 또는 수기판매가)을 그대로 따른다.
    # 수기판매가 기준 행은 매출합계도 같은 기준이라야 수익률이 맞는다.
    manual_basis = work.get(
        MANUAL_PROFIT_SALES_BASIS_COLUMN, pd.Series("", index=work.index)
    ).astype(str).str.strip().eq(MANUAL_PROFIT_SALES_BASIS_MANUAL)
    manual_profit_num = pd.to_numeric(work.get("수기수익", ""), errors="coerce").fillna(0)
    manual_cost_num = pd.to_numeric(work.get(MANUAL_PROFIT_COST_COLUMN, ""), errors="coerce").fillna(0)
    work["total_num"] = actual_total.where(~manual_basis, manual_profit_num + manual_cost_num)
    work["profit_num"] = manual_profit_num
    work["profit_ok"] = _filled(work.get("수기수익", pd.Series("", index=work.index))).astype(int)
    work["manual_basis_rows"] = manual_basis.astype(int)
    grouped = (
        work.groupby([PROFIT_CHANNEL_COLUMN, "수익키", "수익품목명"], dropna=False, sort=False)
        .agg(
            option_kind=("option_kind", lambda s: " | ".join(_unique_nonempty([str(v).strip() for v in s]))),
            주문건수=("order_id", "nunique"),
            판매수량=("qty_num", "sum"),
            매출합계=("total_num", "sum"),
            실매출합계=("actual_total_num", "sum"),
            수익합계=("profit_num", "sum"),
            수익산출행=("profit_ok", "sum"),
            대상행=("profit_ok", "size"),
            수기판매가기준행=("manual_basis_rows", "sum"),
        )
        .reset_index()
        .rename(columns={"수익품목명": "대표품목명"})
    )
    has_profit = pd.to_numeric(grouped["수익산출행"], errors="coerce").fillna(0).gt(0)
    revenue = grouped["매출합계"].where(grouped["매출합계"].ne(0))
    grouped["수익률"] = (grouped["수익합계"] / revenue).round(4).where(has_profit, pd.NA)
    grouped["수익합계"] = grouped["수익합계"].where(has_profit, pd.NA)
    for col in (
        "주문건수",
        "판매수량",
        "매출합계",
        "실매출합계",
        "수익합계",
        "수익산출행",
        "대상행",
        "수기판매가기준행",
        "수익률",
    ):
        grouped[col] = grouped[col].map(lambda value: "" if pd.isna(value) else f"{float(value):g}")
    return grouped.reindex(columns=MANUAL_PROFIT_SUMMARY_COLUMNS, fill_value="").sort_values(
        "매출합계",
        key=lambda s: pd.to_numeric(s, errors="coerce").fillna(0),
        ascending=False,
    )


def _chicken_decision_option_review_status(row: pd.Series) -> tuple[str, str, str]:
    chicken_type = str(row.get("닭유형", "") or "").strip()
    chicken_size = str(row.get("사이즈", "") or "").strip()
    usage = str(row.get(CHICKEN_USAGE_COLUMN, "") or "").strip()
    type_method = str(row.get("닭유형_판정", "") or "").strip()
    size_method = str(row.get("사이즈_판정", "") or "").strip()
    confidence = str(row.get(CHICKEN_CONFIDENCE_COLUMN, "") or "").strip()
    reason = str(row.get("미해결사유", "") or "").strip()
    half_combo = str(row.get(HALF_COMBO_COLUMN, "") or "").strip()
    half_ratio = str(row.get(HALF_BONE_RATIO_COLUMN, row.get(CHICKEN_RATIO_APPLIED_COLUMN, "")) or "").strip()
    slot1 = str(row.get(HALF_SLOT1_COLUMN, "") or "").strip()
    slot2 = str(row.get(HALF_SLOT2_COLUMN, "") or "").strip()
    manual_profit_missing = (
        bool(row.get("_manual_profit_target", False))
        and str(row.get("수기수익", "") or "").strip() == ""
    )

    if (
        not chicken_type
        or not chicken_size
        or not usage
        or "미해결" in {type_method, size_method}
        or confidence == "입력필요"
        or reason
    ):
        return "입력필요", "판정옵션", reason or "닭유형/사이즈/사용용량 확인 필요"
    if chicken_type == CHICKEN_TYPE_MIXED and not (half_combo or half_ratio or (slot1 and slot2)):
        return "입력필요", "판정옵션", "혼합 비율 또는 반반 슬롯 확인 필요"
    if CHICKEN_METHOD_NEAR_PRICE_MATCH in {type_method, size_method}:
        return "검토권장", "판정옵션", "주변단가매칭 결과 확인"
    if manual_profit_missing:
        return "원가입력필요", "수익률", str(row.get("수익미산출사유", "") or "원가_manual미입력").strip()
    return "판정완료", "", "입력 불필요"


def _build_chicken_decision_option_review(left_joined: pd.DataFrame) -> pd.DataFrame:
    if left_joined.empty:
        return pd.DataFrame(columns=CHICKEN_DECISION_OPTION_REVIEW_COLUMNS)
    work = _attach_profit_channel(left_joined).fillna("")
    role = work.get("line_role", pd.Series("", index=work.index)).astype(str).str.strip()
    chicken_key = work.get(CHICKEN_OPTION_KEY_COLUMN, pd.Series("", index=work.index)).astype(str).str.strip()
    target = role.eq("main") & chicken_key.eq(OPTION_COMBO_NONE)
    if not target.any():
        return pd.DataFrame(columns=CHICKEN_DECISION_OPTION_REVIEW_COLUMNS)
    work = work[target].copy()
    if "수익키" not in work.columns or work["수익키"].astype(str).str.strip().eq("").any():
        profiles = work.apply(_manual_profit_profile, axis=1)
        work["수익키"] = [p[0] for p in profiles]
    work["_manual_profit_target"] = _manual_profit_target(work)
    work["_qty_num"] = pd.to_numeric(work.get("qty", pd.Series("", index=work.index)), errors="coerce").fillna(0)
    work["_sales_num"] = _profit_sales_num(work)
    statuses = work.apply(_chicken_decision_option_review_status, axis=1)
    work["검토상태"] = [item[0] for item in statuses]
    work["입력위치"] = [item[1] for item in statuses]
    work["_review_memo"] = [item[2] for item in statuses]
    group_cols = [
        "검토상태",
        PROFIT_CHANNEL_COLUMN,
        "수익키",
        "std_menu_name",
        "닭유형",
        "사이즈",
        "닭유형_판정",
        "사이즈_판정",
        CHICKEN_SIGNAL_COLUMN,
        "입력위치",
    ]
    for col in group_cols + ["source", "menu_name", OPTION_COMBO_COLUMN, "_review_memo", "order_id"]:
        if col not in work.columns:
            work[col] = ""
    grouped = (
        work.groupby(group_cols, dropna=False, sort=False)
        .agg(
            source목록=("source", lambda s: " | ".join(_unique_nonempty([str(v).strip() for v in s]))),
            대표주문메뉴명=("menu_name", lambda s: _first_clean_value(s)),
            **{CHICKEN_OPTION_KEY_COLUMN: (CHICKEN_OPTION_KEY_COLUMN, lambda s: _first_clean_value(s))},
            **{OPTION_COMBO_COLUMN: (OPTION_COMBO_COLUMN, lambda s: _first_clean_value(s))},
            주문건수=("order_id", "nunique"),
            판매수량=("_qty_num", "sum"),
            매출합계=("_sales_num", "sum"),
            메모=("_review_memo", lambda s: " | ".join(_unique_nonempty([str(v).strip() for v in s]))),
        )
        .reset_index()
        .rename(columns={PROFIT_CHANNEL_COLUMN: "수익채널"})
    )
    for col in ("주문건수", "판매수량", "매출합계"):
        grouped[col] = grouped[col].map(_format_number)
    priority = {"입력필요": 0, "검토권장": 1, "원가입력필요": 2, "판정완료": 3}
    grouped["_priority"] = grouped["검토상태"].map(priority).fillna(99)
    grouped["_abs_sales"] = pd.to_numeric(grouped["매출합계"], errors="coerce").fillna(0).abs()
    grouped = grouped.sort_values(["_priority", "_abs_sales"], ascending=[True, False]).drop(
        columns=["_priority", "_abs_sales"],
        errors="ignore",
    )
    return grouped.reindex(columns=CHICKEN_DECISION_OPTION_REVIEW_COLUMNS, fill_value="")


@lru_cache(maxsize=8192)
def _parse_material_usage(value: str) -> tuple[tuple[str, float], ...]:
    """'닭=1.0 | 우거지=0.2' → (('닭',1.0),('우거지',0.2)).

    30,000행 x 재료 수만큼 불리므로 pandas를 쓰면 안 된다. 반환값을 캐시하려고
    dict 대신 tuple을 돌려준다.
    """
    out: dict[str, float] = {}
    for part in str(value or "").split("|"):
        name, _, amount = part.partition("=")
        name = name.strip()
        if not name:
            continue
        try:
            parsed = float(amount.strip())
        except ValueError:
            continue
        out[name] = out.get(name, 0.0) + parsed
    return tuple(out.items())


def _load_commission_rates() -> dict[tuple[str, str], float]:
    """송파삼전점의 ym x platform 실측 수수료율.

    일별 비율은 정산 조정일에 100%를 넘는 이상치가 나오므로 그대로 쓰지 않는다
    (실측 배민 최대 114.3%). 월 단위 합계 비율로 눌러서 쓴다.
    """
    path = DELIVERY_COMMISSION_DIR / "delivery_commission.parquet"
    if not path.exists():
        logger.warning("수수료 마트 없음: %s", path)
        return {}
    frame = pd.read_parquet(path)
    if frame.empty or not {"store", "platform", "sale_date", "total_amt", "settlement_amount"}.issubset(frame.columns):
        return {}
    frame = frame[frame["store"].map(_is_target_store_value)].copy()
    if frame.empty:
        return {}
    frame["ym"] = frame["sale_date"].astype(str).str.slice(0, 7)
    for col in ("total_amt", "settlement_amount"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0)
    grouped = frame.groupby(["ym", "platform"], dropna=False)[["total_amt", "settlement_amount"]].sum()
    out: dict[tuple[str, str], float] = {}
    for (ym, platform), row in grouped.iterrows():
        total = float(row["total_amt"])
        if total <= 0:
            continue
        rate = (total - float(row["settlement_amount"])) / total
        if rate < 0 or rate > 1:
            logger.warning("수수료율 이상치 제외: %s %s rate=%.4f", ym, platform, rate)
            continue
        out[(str(ym), str(platform))] = rate
    return out


def _material_price_master_attrs() -> dict[str, float]:
    master = _manual_or_legacy_sheet("재료단가", MATERIAL_PRICE_MASTER_OUTPUT_PATH).fillna("")
    if master.empty:
        return {}
    if "재료명" not in master.columns or "단가_manual" not in master.columns:
        return {}
    out: dict[str, float] = {}
    for name, price in zip(master["재료명"], master["단가_manual"]):
        key = str(name or "").strip()
        parsed = pd.to_numeric(pd.Series([str(price or "").strip()]), errors="coerce").iloc[0]
        if key and not pd.isna(parsed):
            out[key] = float(parsed)
    return out


def _attach_profit_columns(left_joined: pd.DataFrame) -> pd.DataFrame:
    """매출 × 마진율%가 아니라 재료원가 + 수수료를 빼서 공헌이익을 낸다."""
    out = _attach_profit_channel(left_joined)
    if out.empty:
        for col in PROFIT_COLUMNS:
            if col not in out.columns:
                out[col] = pd.Series(dtype=str)
        return out

    prices = _material_price_master_attrs()
    rates = _load_commission_rates()

    total_price = pd.to_numeric(out.get("total_price", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    qty = pd.to_numeric(out.get("qty", pd.Series("", index=out.index)), errors="coerce").fillna(0)
    ym = out.get("ym", pd.Series("", index=out.index)).fillna("").astype(str)
    platform = out.get("platform", pd.Series("", index=out.index)).fillna("").astype(str)
    profit_channel = out.get(PROFIT_CHANNEL_COLUMN, pd.Series("", index=out.index)).fillna("").astype(str)

    def cost_of(usage_text: object, line_qty: float) -> tuple[float | None, list[str]]:
        """단가가 하나라도 빠지면 None을 돌려준다.

        빠진 재료를 0원으로 치고 더하면 원가를 과소평가한 공헌이익이 계산된 것처럼
        보인다. 그러면 완결률이 100%에 가까워 보이면서 숫자는 틀린다.
        """
        usage = _parse_material_usage(str(usage_text or ""))
        if not usage:
            return None, []
        cost = 0.0
        missing = []
        for material, amount in usage:
            price = prices.get(material)
            if price is None:
                missing.append(material)
                continue
            cost += amount * price * line_qty
        if missing:
            return None, missing
        return cost, missing

    material_costs: list[str] = []
    option_costs: list[str] = []
    missing_reasons: list[str] = []
    menu_usage = out.get(MENU_WEIGHT_USAGE_COLUMN, pd.Series("", index=out.index)).fillna("")
    menu_missing = out.get(MENU_WEIGHT_MISSING_COLUMN, pd.Series("", index=out.index)).fillna("")
    manual_usage = out.get(MATERIAL_USAGE_COLUMN, pd.Series("", index=out.index)).fillna("")
    option_usage = out.get(OPTION_MATERIAL_USAGE_COLUMN, pd.Series("", index=out.index)).fillna("")
    role = out.get("line_role", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    option_kind = out.get("option_kind", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    for menu_text, missing_text, manual_text, option_text, line_qty, line_role, kind, line_total in zip(
        menu_usage, menu_missing, manual_usage, option_usage, qty, role, option_kind, total_price
    ):
        # 표준중량(22번)이 있으면 그걸 쓰고, 없으면 13번 수기 사용량으로 보완한다.
        primary_text = menu_text if _parse_material_usage(str(menu_text or "")) else manual_text
        has_primary = bool(_parse_material_usage(str(primary_text or "")))
        has_option = bool(_parse_material_usage(str(option_text or "")))
        unfilled = [part.strip() for part in str(missing_text or "").split("|") if part.strip()]
        main_cost, main_missing = cost_of(primary_text, line_qty)
        opt_cost, opt_missing = cost_of(option_text, line_qty)
        # 22번에 빈 재료가 남아 있으면 원가를 내지 않는다. 닭만 채워진 상태로 계산하면
        # 부재료를 안 쓴 메뉴처럼 취급되어 공헌이익이 실제보다 크게 나온다.
        # 안 쓰는 재료는 담당자가 0을 명시하면 이 목록에서 빠진다.
        if unfilled:
            main_cost = None
        material_costs.append("" if main_cost is None else f"{main_cost:g}")
        # 유가 옵션에 재료가 없으면 0원이 아니라 미입력이다. 0으로 치면 라면사리·계란찜
        # 같은 원가가 통째로 사라진 채 공헌이익이 산출된다. 0원 라인은 원가도 0이 맞다.
        priced_option = (
            line_role in ("option", "side")
            and float(line_total or 0) != 0
            and kind not in NON_SALES_KINDS
        )
        if not has_option:
            option_costs.append("" if priced_option else "0")
        else:
            option_costs.append("" if opt_cost is None else f"{opt_cost:g}")
        reasons = []
        if not has_primary:
            reasons.append("표준중량미입력")
        if unfilled:
            reasons.append("부재료미입력:" + ",".join(unfilled))
        if priced_option and not has_option:
            reasons.append("옵션재료미입력")
        for material in _unique_nonempty([*main_missing, *opt_missing]):
            reasons.append(f"단가없음:{material}")
        missing_reasons.append(" | ".join(_unique_nonempty(reasons)))

    out["재료원가"] = material_costs
    out["옵션재료원가"] = option_costs

    supported_platforms = {str(p) for _, p in rates}
    rate_values: list[str] = []
    rate_sources: list[str] = []
    for row_ym, row_platform, row_channel in zip(ym, platform, profit_channel):
        # 홀 테이블과 홀 포장은 플랫폼 수수료가 없다. 배달 플랫폼은 수수료율을 조회한다.
        if row_channel in NO_COMMISSION_PROFIT_CHANNELS:
            rate_values.append("0")
            rate_sources.append("홀매출")
            continue
        rate = rates.get((row_ym, row_platform))
        if rate is not None:
            rate_values.append(f"{rate * 100:.2f}")
            rate_sources.append("실측월합계")
            continue
        rate_values.append("")
        # 마트가 아예 안 담는 플랫폼(땡겨요 등)과, 담는데 그 달이 빈 경우를 구분한다.
        rate_sources.append("없음" if row_platform in supported_platforms else "마트미지원")
    out["수수료율"] = rate_values
    out["수수료율_출처"] = rate_sources

    rate_num = pd.to_numeric(pd.Series(rate_values, index=out.index), errors="coerce")
    fee = (total_price * rate_num / 100).round()
    out["수수료"] = fee.map(lambda value: "" if pd.isna(value) else f"{float(value):g}")

    material_num = pd.to_numeric(out["재료원가"], errors="coerce")
    option_num = pd.to_numeric(out["옵션재료원가"], errors="coerce")
    contribution = total_price - material_num.fillna(0) - option_num.fillna(0) - fee.fillna(0)
    # 재료원가/옵션재료원가/수수료 중 하나라도 못 구하면 공헌이익은 없는 것으로 둔다.
    computable = material_num.notna() & option_num.notna() & fee.notna()
    out["공헌이익"] = [
        f"{float(value):g}" if ok else ""
        for value, ok in zip(contribution, computable)
    ]
    margin = (contribution / total_price.where(total_price.ne(0)) * 100).round(2)
    out["공헌이익률"] = [
        f"{float(value):g}" if ok and not pd.isna(value) else ""
        for value, ok in zip(margin, computable)
    ]
    out["원가미산출사유"] = [
        reason if not ok else ""
        for reason, ok in zip(missing_reasons, computable)
    ]

    # 기존 수익률/추정수익은 하위호환용이다. 수기값이 있으면 공헌이익 파생값보다 우선한다.
    existing_rate = out.get("수익률", pd.Series("", index=out.index)).astype(str).str.strip()
    existing_profit = out.get("추정수익", pd.Series("", index=out.index)).astype(str).str.strip()
    out["수익률"] = existing_rate.where(existing_rate.ne(""), out["공헌이익률"])
    out["추정수익"] = existing_profit.where(existing_profit.ne(""), out["공헌이익"])
    return out


def _completeness_row(
    dimension: str,
    denominator: int,
    numerator: int,
    unfinished: list[str],
) -> dict[str, object]:
    rate = 100.0 if denominator == 0 else round(numerator / denominator * 100, 2)
    summary = ", ".join(unfinished[:5])
    if len(unfinished) > 5:
        summary += f" 외 {len(unfinished) - 5}건"
    return {
        "차원": dimension,
        "분모": denominator,
        "분자": numerator,
        "완결률": rate,
        "미완요약": summary,
    }


def _filled(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().ne("")


def _build_completeness(
    left_joined: pd.DataFrame,
    option_kind_master: pd.DataFrame,
    menu_weight_master: pd.DataFrame,
    material_price_master: pd.DataFrame,
    chicken_ratio_master: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """차원별 분모/분자를 명시적으로 센다.

    분모가 코드 안에 숨어 있으면 100%인지 알 수 없다. 여기서만 판단한다.
    """
    rows: list[dict[str, object]] = []
    work = left_joined.copy().fillna("") if not left_joined.empty else pd.DataFrame()

    # 1. option_kind: 주문에 등장한 품목이 전부 계산 가능한 성격을 가지는가
    # 확정값이 비어도 제안값이 있으면 주문 계산은 가능하다. 확정 대기는 24번 audit에 남긴다.
    if option_kind_master.empty:
        rows.append(_completeness_row("option_kind", 0, 0, []))
    else:
        target = option_kind_master[
            ~option_kind_master["option_kind_제안"].isin(list(NON_SALES_KINDS) + [OPTION_KIND_MAIN])
        ]
        usable = _filled(target["option_kind_확정"]) | _filled(target["option_kind_제안"])
        rows.append(
            _completeness_row(
                "option_kind",
                len(target),
                int(usable.sum()),
                target.loc[~usable, "item_name"].tolist(),
            )
        )

    if not work.empty:
        role = work.get("line_role", pd.Series("", index=work.index)).astype(str)
        main_rows = work[role.eq("main")]

        # 2. chicken_attr: main 행이 전부 닭유형/사이즈를 가지는가 (닭미사용 포함)
        has_attr = _filled(main_rows["닭유형"]) & _filled(main_rows["사이즈"])
        rows.append(
            _completeness_row(
                "chicken_attr",
                len(main_rows),
                int(has_attr.sum()),
                main_rows.loc[~has_attr, "std_menu_name"].tolist(),
            )
        )

        # 3. chicken_usage: 닭을 쓰는 main 행이 전부 사용량을 가지는가
        chicken_rows = main_rows[main_rows["닭유형"].astype(str).str.strip().ne(CHICKEN_TYPE_NONE)]
        has_usage = _filled(chicken_rows[CHICKEN_USAGE_COLUMN])
        rows.append(
            _completeness_row(
                "chicken_usage",
                len(chicken_rows),
                int(has_usage.sum()),
                chicken_rows.loc[~has_usage, "std_menu_name"].tolist(),
            )
        )

        slot1 = main_rows.get(HALF_SLOT1_COLUMN, pd.Series("", index=main_rows.index)).astype(str).str.strip()
        slot2 = main_rows.get(HALF_SLOT2_COLUMN, pd.Series("", index=main_rows.index)).astype(str).str.strip()
        half_target = main_rows[slot1.ne("") | slot2.ne("")]
        half_complete = slot1.loc[half_target.index].ne("") & slot2.loc[half_target.index].ne("")
        rows.append(
            _completeness_row(
                "half_slot",
                len(half_target),
                int(half_complete.sum()),
                half_target.loc[~half_complete, "std_menu_name"].tolist(),
            )
        )

        addon_reason = work.get(CHICKEN_ADDON_REASON_COLUMN, pd.Series("", index=work.index)).astype(str).str.strip()
        addon_target = work[addon_reason.ne("")]
        rows.append(
            _completeness_row(
                "chicken_addon",
                len(addon_target),
                int(_filled(addon_target.get(CHICKEN_ADDON_USAGE_COLUMN, pd.Series("", index=addon_target.index))).sum()),
                addon_target.get("item_name", pd.Series(dtype=str)).tolist(),
            )
        )

        order_exception = main_rows.get(ORDER_EXCEPTION_TYPE_COLUMN, pd.Series("", index=main_rows.index)).astype(str).str.strip()
        exception_target = main_rows[order_exception.ne("")]
        rows.append(
            _completeness_row(
                "order_exception",
                len(exception_target),
                len(exception_target),
                [],
            )
        )

        # 6. commission: 배달 라인이 전부 실측 수수료율에 붙었는가.
        # 수수료 마트가 담지 않는 플랫폼(땡겨요 등)은 분모에서 뺀다. 넣어두면
        # 우리가 채울 수 없는 항목 때문에 100%에 영원히 못 닿는다.
        work = _attach_profit_channel(work)
        rate_source = work.get("수수료율_출처", pd.Series("", index=work.index)).astype(str)
        profit_channel = work.get(PROFIT_CHANNEL_COLUMN, pd.Series("", index=work.index)).astype(str)
        delivery = work[
            ~profit_channel.isin(NO_COMMISSION_PROFIT_CHANNELS) & rate_source.ne("마트미지원")
        ]
        joined = delivery.get("수수료율_출처", pd.Series("", index=delivery.index)).astype(str).eq("실측월합계")
        rows.append(
            _completeness_row(
                "commission",
                len(delivery),
                int(joined.sum()),
                delivery.loc[~joined, "sale_date"].tolist(),
            )
        )

        # 7. std_menu_alias: 프로모션 태그가 전부 정리되었는가
        std_names = main_rows["std_menu_name"]
        unaliased = _unaliased_promo_tag_names(std_names)
        distinct = len(_unique_nonempty([str(v).strip() for v in std_names.tolist()]))
        rows.append(
            _completeness_row("std_menu_alias", distinct, distinct - len(unaliased), unaliased)
        )

        # 8. profit: 매출 있는 main 행이 전부 공헌이익을 가지는가
        priced = main_rows[pd.to_numeric(main_rows["total_price"], errors="coerce").fillna(0).ne(0)]
        has_profit = _filled(priced["공헌이익"])
        rows.append(
            _completeness_row(
                "profit",
                len(priced),
                int(has_profit.sum()),
                priced.loc[~has_profit, "원가미산출사유"].tolist(),
            )
        )

        manual_profit_target = work[_manual_profit_target(work)]
        has_manual_profit = _filled(manual_profit_target.get("수기수익", pd.Series("", index=manual_profit_target.index)))
        rows.append(
            _completeness_row(
                "manual_profit",
                len(manual_profit_target),
                int(has_manual_profit.sum()),
                manual_profit_target.loc[~has_manual_profit, "수익품목명"].tolist(),
            )
        )

    # 4. menu_weight: 메뉴x사이즈x닭유형 조합이 전부 표준중량을 가지는가
    if menu_weight_master.empty:
        rows.append(_completeness_row("menu_weight", 0, 0, []))
    else:
        chicken_combos = menu_weight_master[
            menu_weight_master["닭유형"].astype(str).str.strip().ne(CHICKEN_TYPE_NONE)
        ]
        weight_columns = [c for c in chicken_combos.columns if _is_material_usage_manual_column(c)]
        if weight_columns:
            filled_any = pd.concat([_filled(chicken_combos[c]) for c in weight_columns], axis=1).any(axis=1)
        else:
            filled_any = pd.Series(False, index=chicken_combos.index)
        rows.append(
            _completeness_row(
                "menu_weight",
                len(chicken_combos),
                int(filled_any.sum()),
                (
                    chicken_combos.loc[~filled_any, "std_menu_name"].astype(str)
                    + "/"
                    + chicken_combos.loc[~filled_any, "사이즈"].astype(str)
                    + "/"
                    + chicken_combos.loc[~filled_any, "닭유형"].astype(str)
                ).tolist(),
            )
        )

    # 5. material_price: 쓰이는 재료가 전부 단가를 가지는가
    if material_price_master.empty:
        rows.append(_completeness_row("material_price", 0, 0, []))
    else:
        has_price = _filled(material_price_master["단가_manual"])
        rows.append(
            _completeness_row(
                "material_price",
                len(material_price_master),
                int(has_price.sum()),
                material_price_master.loc[~has_price, "재료명"].tolist(),
            )
        )

    # 9. chicken_ratio: 뼈/순살 신호가 없는 구간이 전부 비율을 배정받았는가.
    # 분모는 무신호가 실제로 있는 조합만이다. 신호가 다 있는 메뉴까지 넣으면
    # 채울 것이 없는데 분모만 커진다.
    if chicken_ratio_master is None or chicken_ratio_master.empty:
        rows.append(_completeness_row("chicken_ratio", 0, 0, []))
    else:
        blind = pd.to_numeric(chicken_ratio_master.get("무신호_판매수량", ""), errors="coerce").fillna(0)
        target = chicken_ratio_master[blind.gt(0)]
        has_ratio = _filled(target.get("적용비율", pd.Series("", index=target.index)))
        rows.append(
            _completeness_row(
                "chicken_ratio",
                len(target),
                int(has_ratio.sum()),
                (
                    target.loc[~has_ratio, "std_menu_name"].astype(str)
                    + "/"
                    + target.loc[~has_ratio, "사이즈"].astype(str)
                ).tolist(),
            )
        )

    return pd.DataFrame(rows)


def _guard_chicken_usage_total(left_joined: pd.DataFrame) -> None:
    """닭 사용량 총합이 이전 실행 대비 크게 떨어지면 막는다.

    완결률은 "값이 있는가"만 보므로, 수기값이 규칙값으로 조용히 대체돼 사용량이
    줄어드는 건 못 잡는다. 실제로 alias 도입 때 배민 30행의 수기값이 규칙값으로
    떨어지며 총합이 5% 빠졌는데 완결률은 그대로였다. 총량은 따로 지켜야 한다.
    """
    if left_joined.empty or CHICKEN_USAGE_TOTAL_COLUMN not in left_joined.columns:
        return
    main = left_joined[left_joined.get("line_role", pd.Series("", index=left_joined.index)).eq("main")]
    total = float(pd.to_numeric(main.get(CHICKEN_USAGE_TOTAL_COLUMN, pd.Series(dtype=str)), errors="coerce").fillna(0).sum())
    baseline = _load_completeness_baseline()
    previous = float(baseline.get("_chicken_usage_total", 0.0))
    if previous > 0 and total < previous * 0.97:
        raise RuntimeError(
            f"닭 사용량 총합 급감: {previous:.1f} → {total:.1f}마리 ({total / previous - 1:+.1%}). "
            f"수기값이 규칙값으로 밀렸을 수 있습니다. 확인 전까지 중단합니다. "
            f"의도한 변동이면 reset_completeness_baseline('_chicken_usage_total') 후 재실행하세요."
        )
    if previous and total < previous:
        logger.warning("닭 사용량 총합 감소: %.1f → %.1f마리 (허용 범위)", previous, total)
    logger.info("닭 사용량 총합: %.1f마리 (직전 %.1f)", total, previous)


def _guard_left_joined_output_schema(left_joined: pd.DataFrame) -> None:
    """12번 주문서가 운영 검수에 필요한 컬럼을 모두 가진 상태인지 확인한다."""
    missing = [col for col in LEFT_JOINED_OUTPUT_COLUMNS if col not in left_joined.columns]
    if missing:
        raise RuntimeError(f"12_orders_left 필수 컬럼 누락: {', '.join(missing)}")
    duplicate_columns = left_joined.columns[left_joined.columns.duplicated()].tolist()
    if duplicate_columns:
        raise RuntimeError(f"12_orders_left 중복 컬럼: {', '.join(map(str, duplicate_columns))}")
    manual_profit_missing = [col for col in MANUAL_PROFIT_COLUMNS if col not in left_joined.columns]
    if manual_profit_missing:
        raise RuntimeError(f"12_orders_left 수기수익 컬럼 누락: {', '.join(manual_profit_missing)}")


def _attach_chicken_confidence_columns(left_joined: pd.DataFrame) -> pd.DataFrame:
    out = left_joined.copy()
    if out.empty:
        out[CHICKEN_CONFIDENCE_COLUMN] = pd.Series(dtype=str)
        return out
    role = out.get("line_role", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    chicken_type = out.get("닭유형", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    reason = out.get("미해결사유", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    signal = out.get(CHICKEN_SIGNAL_COLUMN, pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    ratio = out.get(CHICKEN_RATIO_APPLIED_COLUMN, pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    half_combo = out.get(HALF_COMBO_COLUMN, pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    type_decision = out.get("닭유형_판정", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    size_decision = out.get("사이즈_판정", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()

    statuses: list[str] = []
    for line_role, ctype, why, sig, ratio_value, combo, t_method, s_method in zip(
        role, chicken_type, reason, signal, ratio, half_combo, type_decision, size_decision
    ):
        if line_role != "main" or ctype == CHICKEN_TYPE_NONE:
            statuses.append("확정")
            continue
        if "충돌" in why:
            statuses.append("충돌")
            continue
        if why:
            statuses.append("입력필요")
            continue
        if combo or t_method in {CHICKEN_METHOD_HALF, CHICKEN_METHOD_PRICE_MATCH, CHICKEN_METHOD_NEAR_PRICE_MATCH, "메뉴프로필", "판정옵션", "수기", "기존수기"}:
            statuses.append("확정")
            continue
        if t_method in {"메뉴명", "메뉴프로필", "변경", "선택"} or s_method in {"수기", "기존수기", "메뉴명", "메뉴프로필", "변경", "선택", "판정옵션"}:
            statuses.append("확정")
            continue
        if ratio_value or t_method == "비율추정" or sig == CHICKEN_SIGNAL_ABSENT:
            statuses.append("추정")
            continue
        statuses.append("추정")
    out[CHICKEN_CONFIDENCE_COLUMN] = statuses
    return out


def _classification_rule_from_row(row: pd.Series) -> str:
    if str(row.get(CHICKEN_CONFIDENCE_COLUMN, "") or "").strip() == "확정":
        methods = _unique_nonempty(
            [
                str(row.get("닭유형_판정", "") or "").strip(),
                str(row.get("사이즈_판정", "") or "").strip(),
            ]
        )
        methods = [method for method in methods if method != "미해결"]
        return "확정:" + ",".join(methods or ["규칙"])
    reason = str(row.get("미해결사유", "") or "").strip()
    return "미분류:" + (reason or str(row.get(CHICKEN_CONFIDENCE_COLUMN, "") or "").strip() or "확인필요")


def _judgement_option_attrs() -> pd.DataFrame:
    frame = _read_manual_workbook_sheet(JUDGEMENT_OPTION_SHEET_NAME).fillna("")
    if frame.empty:
        return pd.DataFrame(columns=JUDGEMENT_OPTION_COLUMNS)
    for col in JUDGEMENT_OPTION_COLUMNS:
        if col not in frame.columns:
            frame[col] = ""
        frame[col] = frame[col].fillna("").astype(str).str.strip()
    frame = _target_product_rows(frame)
    return frame.reindex(columns=JUDGEMENT_OPTION_COLUMNS, fill_value="").drop_duplicates(
        subset=["source", "brand", "store", "std_menu_name", "조건", CHICKEN_OPTION_KEY_COLUMN],
        keep="last",
    )


def _judgement_condition_for_row(row: pd.Series) -> str:
    reason = str(row.get("미해결사유", "") or "").strip()
    if "반반슬롯" in reason:
        return "반반슬롯미입력"
    if "닭유형충돌" in reason:
        return "닭유형충돌"
    if "사이즈충돌" in reason:
        return "사이즈충돌"
    if "사이즈옵션없음" in reason:
        return "사이즈옵션없음"
    if str(row.get(CHICKEN_OPTION_KEY_COLUMN, "") or "").strip() == OPTION_COMBO_NONE:
        return "옵션없음"
    return "자동보정"


def _is_auto_generated_judgement_option(row: pd.Series) -> bool:
    return str(row.get("메모", "") or "").strip().startswith("자동생성 ")


def _attrs_from_chicken_option_key(value: object) -> tuple[str, str]:
    key = str(value or "").strip()
    if not key or key == OPTION_COMBO_NONE:
        return "", ""
    types: list[str] = []
    sizes: list[str] = []
    for part in key.split("|"):
        token = str(part or "").strip()
        if not token:
            continue
        types.extend(_infer_chicken_types(token))
        sizes.extend(_infer_chicken_sizes(token))
    unique_types = _unique_nonempty(types)
    unique_sizes = _unique_nonempty(sizes)
    return (
        unique_types[0] if len(unique_types) == 1 else "",
        unique_sizes[0] if len(unique_sizes) == 1 else "",
    )


def _constrain_stale_judgement_option_by_key(row: pd.Series) -> pd.Series:
    has_half_detail = any(
        str(row.get(col, "") or "").strip()
        for col in (HALF_COMBO_COLUMN, HALF_BONE_RATIO_COLUMN, HALF_SLOT1_COLUMN, HALF_SLOT2_COLUMN)
    )
    if has_half_detail:
        return row
    key_type, key_size = _attrs_from_chicken_option_key(row.get(CHICKEN_OPTION_KEY_COLUMN, ""))
    if key_type:
        row["닭유형"] = key_type
    if key_size:
        row["사이즈"] = key_size
    if key_type and key_size:
        usage = _usage_for(key_type, key_size)
        if usage:
            row[CHICKEN_USAGE_COLUMN] = usage
    return row


def _build_judgement_option_input(left_joined: pd.DataFrame) -> pd.DataFrame:
    existing = _judgement_option_attrs()
    existing_map = {}
    if not existing.empty:
        for _, row in existing[~existing.apply(_is_auto_generated_judgement_option, axis=1)].iterrows():
            key = tuple(str(row.get(col, "") or "").strip() for col in ["source", "brand", "store", "std_menu_name", "조건", CHICKEN_OPTION_KEY_COLUMN])
            existing_map[key] = {col: str(row.get(col, "") or "").strip() for col in JUDGEMENT_OPTION_COLUMNS}

    if left_joined.empty or CHICKEN_CONFIDENCE_COLUMN not in left_joined.columns:
        return existing.reindex(columns=JUDGEMENT_OPTION_COLUMNS, fill_value="")
    slot1 = left_joined.get(HALF_SLOT1_COLUMN, pd.Series("", index=left_joined.index)).fillna("").astype(str).str.strip()
    slot2 = left_joined.get(HALF_SLOT2_COLUMN, pd.Series("", index=left_joined.index)).fillna("").astype(str).str.strip()
    half_incomplete = (slot1.ne("") | slot2.ne("")) & ~(slot1.ne("") & slot2.ne(""))
    work = left_joined[
        left_joined.get("line_role", pd.Series("", index=left_joined.index)).eq("main")
        & (left_joined[CHICKEN_CONFIDENCE_COLUMN].astype(str).str.strip().ne("확정") | half_incomplete)
    ].copy()
    if work.empty:
        return existing.reindex(columns=JUDGEMENT_OPTION_COLUMNS, fill_value="")
    for col in ["qty", "total_price"]:
        work[f"{col}_num"] = pd.to_numeric(work.get(col, pd.Series("", index=work.index)), errors="coerce").fillna(0)
    work["조건"] = work.apply(_judgement_condition_for_row, axis=1)
    rows = []
    group_cols = ["source", "brand", "store", "std_menu_name", "조건", CHICKEN_OPTION_KEY_COLUMN]
    for key, group in work.groupby(group_cols, dropna=False, sort=False):
        current = existing_map.get(tuple(str(part or "").strip() for part in key), {})
        row0 = group.iloc[0]
        rows.append(
            {
                "source": key[0],
                "brand": key[1],
                "store": key[2],
                "std_menu_name": key[3],
                "조건": key[4],
                CHICKEN_OPTION_KEY_COLUMN: key[5],
                "닭유형": current.get("닭유형") or str(row0.get("닭유형", "") or "").strip(),
                "사이즈": current.get("사이즈") or str(row0.get("사이즈", "") or "").strip(),
                CHICKEN_USAGE_COLUMN: current.get(CHICKEN_USAGE_COLUMN) or str(row0.get(CHICKEN_USAGE_COLUMN, "") or "").strip(),
                HALF_COMBO_COLUMN: current.get(HALF_COMBO_COLUMN) or str(row0.get(HALF_COMBO_COLUMN, "") or "").strip(),
                HALF_BONE_RATIO_COLUMN: current.get(HALF_BONE_RATIO_COLUMN) or str(row0.get(CHICKEN_RATIO_APPLIED_COLUMN, "") or "").strip(),
                HALF_SLOT1_COLUMN: current.get(HALF_SLOT1_COLUMN) or str(row0.get(HALF_SLOT1_COLUMN, "") or "").strip(),
                HALF_SLOT2_COLUMN: current.get(HALF_SLOT2_COLUMN) or str(row0.get(HALF_SLOT2_COLUMN, "") or "").strip(),
                "메모": current.get("메모") or f"자동생성 {len(group)}행 / 매출 {_format_number(group['total_price_num'].sum())}",
            }
        )
    output = pd.DataFrame(rows, columns=JUDGEMENT_OPTION_COLUMNS)
    if existing.empty:
        return output
    merged_keys = set(
        tuple(str(row.get(col, "") or "").strip() for col in group_cols)
        for _, row in output.iterrows()
    )
    keep_existing = existing[
        ~existing.apply(lambda row: tuple(str(row.get(col, "") or "").strip() for col in group_cols) in merged_keys, axis=1)
        & ~existing.apply(_is_auto_generated_judgement_option, axis=1)
    ]
    return pd.concat([output, keep_existing], ignore_index=True, sort=False).reindex(columns=JUDGEMENT_OPTION_COLUMNS, fill_value="")


def _apply_judgement_options(
    left_joined: pd.DataFrame,
    judgement_options: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = left_joined.copy()
    result_columns = [
        "source",
        "brand",
        "store",
        "std_menu_name",
        "조건",
        CHICKEN_OPTION_KEY_COLUMN,
        "적용행수",
        "적용주문건수",
        "적용매출",
        "적용전상태",
        "닭유형",
        "사이즈",
        CHICKEN_USAGE_COLUMN,
        HALF_COMBO_COLUMN,
        HALF_BONE_RATIO_COLUMN,
        HALF_SLOT1_COLUMN,
        HALF_SLOT2_COLUMN,
    ]
    if out.empty or judgement_options.empty:
        return out, pd.DataFrame(columns=result_columns)
    target = judgement_options.copy().fillna("")
    for col in JUDGEMENT_OPTION_COLUMNS:
        if col not in target.columns:
            target[col] = ""
        target[col] = target[col].astype(str).str.strip()
    target = target.apply(_constrain_stale_judgement_option_by_key, axis=1)
    half_ratio = target[HALF_BONE_RATIO_COLUMN].map(_parse_bone_ratio)
    half_ratio = half_ratio.where(half_ratio.notna(), target[HALF_COMBO_COLUMN].map(_bone_ratio_from_half_combo))
    slots_ready = target[HALF_SLOT1_COLUMN].ne("") & target[HALF_SLOT2_COLUMN].ne("")
    target.loc[slots_ready, HALF_COMBO_COLUMN] = target.loc[slots_ready].apply(
        lambda row: _half_combo_from_slots(row.get(HALF_SLOT1_COLUMN, ""), row.get(HALF_SLOT2_COLUMN, "")),
        axis=1,
    )
    target.loc[slots_ready, "닭유형"] = target.loc[slots_ready].apply(
        lambda row: _chicken_type_from_slots(row.get(HALF_SLOT1_COLUMN, ""), row.get(HALF_SLOT2_COLUMN, "")),
        axis=1,
    )
    target.loc[slots_ready, HALF_BONE_RATIO_COLUMN] = target.loc[slots_ready].apply(
        lambda row: _format_number(_bone_ratio_from_slots(row.get(HALF_SLOT1_COLUMN, ""), row.get(HALF_SLOT2_COLUMN, ""))),
        axis=1,
    )
    needs_slot_usage = slots_ready & target["사이즈"].ne("")
    target.loc[needs_slot_usage, CHICKEN_USAGE_COLUMN] = target.loc[needs_slot_usage].apply(
        lambda row: _usage_from_half_slots(
            row.get("사이즈", ""),
            row.get(HALF_SLOT1_COLUMN, ""),
            row.get(HALF_SLOT2_COLUMN, ""),
        )[0],
        axis=1,
    )
    half_ratio = target[HALF_BONE_RATIO_COLUMN].map(_parse_bone_ratio)
    half_ratio = half_ratio.where(half_ratio.notna(), target[HALF_COMBO_COLUMN].map(_bone_ratio_from_half_combo))
    target["_half_ratio"] = half_ratio
    needs_auto_usage = target[CHICKEN_USAGE_COLUMN].eq("") & target["_half_ratio"].notna() & target["사이즈"].ne("")
    needs_auto_type = target["닭유형"].eq("") & target["_half_ratio"].notna()
    target.loc[needs_auto_type, "닭유형"] = target.loc[needs_auto_type, "_half_ratio"].map(
        lambda value: _chicken_type_from_half_ratio(float(value))
    )
    target.loc[needs_auto_usage, CHICKEN_USAGE_COLUMN] = target.loc[needs_auto_usage].apply(
        lambda row: _usage_for_half_ratio(row.get("사이즈", ""), float(row.get("_half_ratio", 0))),
        axis=1,
    )
    for idx, row in target.iterrows():
        profile = _profile_for_menu_row(row)
        if not profile:
            continue
        values = _profile_constrained_values_for_row(row, profile)
        if values is None:
            continue
        chicken_type, _chicken_size, usage = values
        target.at[idx, "닭유형"] = chicken_type
        target.at[idx, CHICKEN_USAGE_COLUMN] = usage
        target.at[idx, HALF_COMBO_COLUMN] = ""
        target.at[idx, HALF_BONE_RATIO_COLUMN] = ""
        target.at[idx, HALF_SLOT1_COLUMN] = ""
        target.at[idx, HALF_SLOT2_COLUMN] = ""
    target = target[
        target["닭유형"].ne("")
        & target["사이즈"].ne("")
        & target[CHICKEN_USAGE_COLUMN].ne("")
    ].copy()
    if target.empty:
        return out, pd.DataFrame(columns=result_columns)
    if CHICKEN_CONFIDENCE_COLUMN not in out.columns:
        out = _attach_chicken_confidence_columns(out)
    out["조건"] = out.apply(_judgement_condition_for_row, axis=1)
    rows = []
    key_cols = ["source", "brand", "store", "std_menu_name", "조건", CHICKEN_OPTION_KEY_COLUMN]
    for _, rule in target.iterrows():
        mask = out.get("line_role", pd.Series("", index=out.index)).eq("main")
        for col in key_cols:
            mask &= out.get(col, pd.Series("", index=out.index)).fillna("").astype(str).str.strip().eq(str(rule.get(col, "") or "").strip())
        if not mask.any():
            continue
        before_status = " | ".join(_unique_nonempty(out.loc[mask, CHICKEN_CONFIDENCE_COLUMN].astype(str).tolist()))
        out.loc[mask, "닭유형"] = str(rule.get("닭유형", "") or "").strip()
        out.loc[mask, "사이즈"] = str(rule.get("사이즈", "") or "").strip()
        out.loc[mask, CHICKEN_USAGE_COLUMN] = str(rule.get(CHICKEN_USAGE_COLUMN, "") or "").strip()
        combo = str(rule.get(HALF_COMBO_COLUMN, "") or "").strip()
        slot1 = str(rule.get(HALF_SLOT1_COLUMN, "") or "").strip()
        slot2 = str(rule.get(HALF_SLOT2_COLUMN, "") or "").strip()
        ratio = _parse_bone_ratio(rule.get(HALF_BONE_RATIO_COLUMN, ""))
        if ratio is None:
            ratio = _bone_ratio_from_half_combo(combo)
        out.loc[mask, HALF_COMBO_COLUMN] = combo
        out.loc[mask, HALF_SLOT1_COLUMN] = slot1
        out.loc[mask, HALF_SLOT2_COLUMN] = slot2
        qty = pd.to_numeric(out.loc[mask, "qty"], errors="coerce").fillna(0)
        usage = pd.to_numeric(out.loc[mask, CHICKEN_USAGE_COLUMN], errors="coerce").fillna(0)
        out.loc[mask, CHICKEN_USAGE_TOTAL_COLUMN] = (qty * usage).map(_format_number)
        out.loc[mask, "닭유형_판정"] = CHICKEN_METHOD_HALF_SLOT if slot1 and slot2 else (CHICKEN_METHOD_HALF if combo else "판정옵션")
        out.loc[mask, "사이즈_판정"] = "판정옵션"
        out.loc[mask, "미해결사유"] = ""
        out.loc[mask, CHICKEN_SIGNAL_COLUMN] = CHICKEN_SIGNAL_PRESENT
        out.loc[mask, CHICKEN_RATIO_APPLIED_COLUMN] = "" if ratio is None else _format_number(ratio)
        out = _attach_chicken_usage_split_columns(out)
        out.loc[mask, CHICKEN_CONFIDENCE_COLUMN] = "확정"
        out.loc[mask, CLASSIFICATION_RULE_COLUMN] = "판정옵션"
        sales = pd.to_numeric(out.loc[mask, "total_price"], errors="coerce").fillna(0).sum()
        rows.append(
            {
                "source": rule.get("source", ""),
                "brand": rule.get("brand", ""),
                "store": rule.get("store", ""),
                "std_menu_name": rule.get("std_menu_name", ""),
                "조건": rule.get("조건", ""),
                CHICKEN_OPTION_KEY_COLUMN: rule.get(CHICKEN_OPTION_KEY_COLUMN, ""),
                "적용행수": _format_number(mask.sum()),
                "적용주문건수": _format_number(out.loc[mask, "order_id"].nunique()),
                "적용매출": _format_number(sales),
                "적용전상태": before_status,
                "닭유형": rule.get("닭유형", ""),
                "사이즈": rule.get("사이즈", ""),
                CHICKEN_USAGE_COLUMN: rule.get(CHICKEN_USAGE_COLUMN, ""),
                HALF_COMBO_COLUMN: rule.get(HALF_COMBO_COLUMN, ""),
                HALF_BONE_RATIO_COLUMN: rule.get(HALF_BONE_RATIO_COLUMN, ""),
                HALF_SLOT1_COLUMN: rule.get(HALF_SLOT1_COLUMN, ""),
                HALF_SLOT2_COLUMN: rule.get(HALF_SLOT2_COLUMN, ""),
            }
        )
    out = out.drop(columns=["조건"], errors="ignore")
    return out, pd.DataFrame(rows, columns=result_columns)


def confirm_option_kind_suggestions(only_kind: str | None = None, overwrite: bool = False) -> str:
    """21번의 `option_kind_제안`을 `option_kind_확정`으로 복사한다.

    360건을 한 줄씩 타이핑하는 대신, 제안값을 일괄로 받아들이고 틀린 것만 고치는 용도다.
    DAG는 이 함수를 부르지 않는다. 규칙 결과를 자동으로 확정하면 "규칙은 초안, 사람이 확정"이
    무너지고 완결률이 검증 없이 100%가 되기 때문이다. 사람이 명시적으로 실행할 때만 동작한다.

    only_kind를 주면 그 성격의 제안만 확정한다(예: 닭 사용량에 직접 걸리는 "사이즈"부터).
    overwrite=False면 이미 확정된 값은 건드리지 않는다.
    """
    master = _manual_or_legacy_sheet("옵션분류", OPTION_KIND_MASTER_OUTPUT_PATH).fillna("")
    if master.empty:
        return "01_수기입력.xlsx의 옵션분류 시트가 없습니다. 먼저 DAG를 한 번 실행하세요."
    if "option_kind_제안" not in master.columns or "option_kind_확정" not in master.columns:
        return "21번에 option_kind_제안/확정 컬럼이 없습니다."

    suggestion = master["option_kind_제안"].fillna("").astype(str).str.strip()
    confirmed = master["option_kind_확정"].fillna("").astype(str).str.strip()
    target = suggestion.ne("") & ~suggestion.isin(list(NON_SALES_KINDS) + [OPTION_KIND_MAIN, OPTION_KIND_UNSET])
    if only_kind:
        target &= suggestion.eq(str(only_kind).strip())
    if not overwrite:
        target &= confirmed.eq("")
    count = int(target.sum())
    if not count:
        return "확정할 대상이 없습니다."

    master.loc[target, "option_kind_확정"] = suggestion[target]
    memo = master.get("메모", pd.Series("", index=master.index)).fillna("").astype(str)
    stamp = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
    master["메모"] = memo.where(
        ~target,
        (memo + f" | 제안일괄승인 {stamp}").str.strip(" |"),
    )
    sheets = {
        name: _read_manual_workbook_sheet(name)
        for name in [
            "수익률",
            "메뉴중량",
            "옵션분류",
            "재료단가",
            "뼈순살비율",
            JUDGEMENT_OPTION_SHEET_NAME,
            CHICKEN_CONVERSION_SHEET_NAME,
            "옵션재료",
            ORDER_EXCEPTION_SHEET_NAME,
            "예외보정",
        ]
    }
    sheets["옵션분류"] = master
    _guard_manual_workbook_loss(sheets)
    _backup_manual_workbook_before_write()
    _write_excel_workbook(sheets, MANUAL_WORKBOOK_OUTPUT_PATH)
    logger.warning("option_kind 제안 일괄 확정: %d건 (only_kind=%s)", count, only_kind)
    return f"제안값 확정 완료: {count}건. 틀린 행은 option_kind_확정을 직접 고치세요."


def reset_completeness_baseline(dimensions: str | list[str] | None = None) -> str:
    """완결률 기준선을 내린다.

    버그가 부풀린 값이 기준선에 박히면, 고친 뒤의 정직한 값이 항상 '후퇴'로 걸려
    DAG가 영구 차단된다. 실제로 단가 없는 재료를 0원으로 더하던 버그가 profit 기준선을
    93.45%로 올려놔서 한 번 이 상황이 났다.

    기준선을 내리는 건 사람이 "이전 값이 틀렸다"고 판단했을 때만 하는 수동 조치다.
    자동으로 내려가면 후퇴 감지 자체가 무의미해지므로 DAG는 이 함수를 부르지 않는다.

    사용법:
        reset_completeness_baseline("profit")          # 한 차원만
        reset_completeness_baseline(["profit", "menu_weight"])
        reset_completeness_baseline()                  # 전체 초기화
    """
    baseline = _load_completeness_baseline()
    if not baseline:
        return "기준선 파일이 없어 초기화할 것이 없습니다."
    if dimensions is None:
        targets = [key for key in baseline if not key.startswith("_")]
    elif isinstance(dimensions, str):
        targets = [dimensions]
    else:
        targets = list(dimensions)

    changed = {}
    for key in targets:
        if key in baseline:
            changed[key] = baseline[key]
            baseline[key] = 0.0
    if not changed:
        return f"해당 차원을 기준선에서 찾지 못했습니다: {targets}"
    COMPLETENESS_BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    COMPLETENESS_BASELINE_PATH.write_text(
        json.dumps(baseline, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    logger.warning("완결률 기준선 초기화: %s", changed)
    return f"기준선 초기화 완료: {changed}"


def _load_completeness_baseline() -> dict[str, float]:
    path = COMPLETENESS_BASELINE_PATH if COMPLETENESS_BASELINE_PATH.exists() else LEGACY_COMPLETENESS_BASELINE_PATH
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        logger.warning("완결률 기준선 파일을 읽지 못해 새로 만듭니다: %s", path)
        return {}


def _to_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _apply_completeness_gate(completeness: pd.DataFrame, chicken_usage_total: float = 0.0) -> pd.DataFrame:
    """후퇴하면 실패, 100%에 닿으면 그 뒤로는 100% 미만이 곧 실패.

    처음부터 하드 100% 게이트를 걸면 담당자가 입력을 끝낼 때까지 DAG가 계속 실패해
    아무도 산출물을 못 본다. 래칫은 전진만 허용하고, 100% 도달과 동시에 저절로
    하드 게이트가 된다. 별도 스위치는 두지 않는다.
    """
    if completeness.empty:
        return completeness
    baseline = _load_completeness_baseline()
    statuses = []
    baselines = []
    updated = dict(baseline)
    meta = baseline.get("_completeness_meta", {})
    if not isinstance(meta, dict):
        meta = {}
    updated_meta = dict(meta)
    for _, row in completeness.iterrows():
        dimension = str(row["차원"])
        rate = float(row["완결률"])
        prior = float(baseline.get(dimension, 0.0))
        denominator = _to_int(row.get("분모"))
        numerator = _to_int(row.get("분자"))
        missing_count = max(denominator - numerator, 0)
        prior_meta = meta.get(dimension, {})
        prior_missing_count = (
            _to_int(prior_meta.get("missing_count"))
            if isinstance(prior_meta, dict) and "missing_count" in prior_meta
            else None
        )
        baselines.append(prior)
        if rate >= 100.0:
            statuses.append("완결")
        elif prior >= 100.0:
            statuses.append("후퇴")
        elif prior_missing_count is None:
            # 구 기준선은 퍼센트만 있어 분모 변동과 실제 미입력 증가를 구분할 수 없다.
            # 이번 실행부터 개수 기준 메타를 저장해 다음 실행에서 정확히 판정한다.
            statuses.append("진행중")
        elif missing_count > prior_missing_count:
            statuses.append("후퇴")
        elif rate < prior:
            statuses.append("진행중")
        else:
            statuses.append("진행중")
        updated[dimension] = max(prior, rate)
        updated_meta[dimension] = {
            "denominator": denominator,
            "numerator": numerator,
            "missing_count": missing_count,
        }
    if chicken_usage_total > 0:
        updated["_chicken_usage_total"] = round(chicken_usage_total, 1)
    updated["_completeness_meta"] = updated_meta
    completeness = completeness.copy()
    completeness["기준선"] = baselines
    completeness["상태"] = statuses
    COMPLETENESS_BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    COMPLETENESS_BASELINE_PATH.write_text(
        json.dumps(updated, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    return completeness


def _completeness_issues(completeness: pd.DataFrame) -> list[dict[str, object]]:
    issues: list[dict[str, object]] = []
    for _, row in completeness.iterrows():
        if str(row.get("상태", "")) != "후퇴":
            continue
        dimension = str(row.get("차원", "") or "").strip()
        severity = "WARN" if dimension in COMPLETENESS_INPUT_WAIT_DIMENSIONS else "block"
        issues.append(
            {
                "issue_type": "completeness_regression",
                "severity": severity,
                "detail": (
                    f"{row['차원']} 완결률 {row['완결률']}% < 기준선 {row['기준선']}% | "
                    f"미완: {row.get('미완요약', '')}"
                ),
                "rows": row.get("분모", ""),
            }
        )
    return issues


def _takeout_setting_cost_issues(manual_profit_rate_master: pd.DataFrame) -> list[dict[str, object]]:
    """포장 채널인데 상차림비_manual이 들어간 행. 입력값은 지우지 않고 계산에서만 뺀다."""
    if manual_profit_rate_master is None or manual_profit_rate_master.empty:
        return []
    work = manual_profit_rate_master.fillna("")
    channel = work.get(PROFIT_CHANNEL_COLUMN, pd.Series("", index=work.index)).astype(str).str.strip()
    takeout = channel.eq("홀_포장") | channel.str.contains("포장", regex=False, na=False)
    setting = work.get("상차림비_manual", pd.Series("", index=work.index)).astype(str).str.strip()
    setting_num = pd.to_numeric(setting.str.replace(",", "", regex=False), errors="coerce")
    target = takeout & setting.ne("") & setting_num.fillna(0).ne(0)
    issues: list[dict[str, object]] = []
    for _, row in work[target].iterrows():
        issues.append(
            {
                "issue_type": "takeout_setting_cost_ignored",
                "severity": "WARN",
                "item_name": str(row.get("대표품목명", "") or ""),
                "std_menu_name": str(row.get("수익키", "") or ""),
                "detail": (
                    f"포장 채널 상차림비_manual={str(row.get('상차림비_manual', '') or '').strip()} "
                    "입력됨. 계산에는 0으로 반영합니다"
                ),
                "sales": str(row.get("매출합계", "") or ""),
            }
        )
    return issues


def _classification_unresolved_issues(left_joined: pd.DataFrame) -> list[dict[str, object]]:
    if left_joined.empty or CHICKEN_CONFIDENCE_COLUMN not in left_joined.columns:
        return []
    work = left_joined[
        left_joined.get("line_role", pd.Series("", index=left_joined.index)).eq("main")
        & left_joined[CHICKEN_CONFIDENCE_COLUMN].astype(str).str.strip().ne("확정")
    ].copy()
    issues: list[dict[str, object]] = []
    for _, row in work.iterrows():
        detail = row.get("미해결사유", "") or row.get(CLASSIFICATION_RULE_COLUMN, "") or "판정옵션 필요"
        issues.append(
            {
                "issue_type": "classification_unresolved",
                "severity": "ERROR",
                "source": row.get("source", ""),
                "sale_date": row.get("sale_date", ""),
                "ym": row.get("ym", ""),
                "order_id": row.get("order_id", ""),
                "item_seq": row.get("item_seq", ""),
                "item_id": row.get("item_id", ""),
                "item_name": row.get("item_name", ""),
                "line_role": row.get("line_role", ""),
                "std_menu_name": row.get("std_menu_name", ""),
                "detail": detail,
                "rows": 1,
                "orders": 1,
                "sales": row.get("total_price", ""),
            }
        )
    return issues


def _build_option_kind_master(left_joined: pd.DataFrame) -> pd.DataFrame:
    """주문에 실제로 등장한 품목을 전수 열거한 option_kind 마스터.

    제안값은 매번 규칙으로 다시 계산하고, 확정값은 같은 키가 유지되는 한 보존한다.
    """
    if left_joined.empty:
        return pd.DataFrame(columns=OPTION_KIND_MASTER_COLUMNS)

    work = left_joined.copy().fillna("")
    for col in ["qty", "total_price"]:
        work[f"{col}_num"] = pd.to_numeric(
            work.get(col, pd.Series("", index=work.index)), errors="coerce"
        ).fillna(0)
    for col in OPTION_KIND_MASTER_KEY_COLUMNS:
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].fillna("").astype(str).str.strip()

    grouped = (
        work.groupby(OPTION_KIND_MASTER_KEY_COLUMNS, dropna=False, sort=False)
        .agg(
            line_role=("line_role", _first_clean_value),
            std_menu_name=("std_menu_name", _first_clean_value),
            주문건수=("order_id", "nunique"),
            판매수량=("qty_num", "sum"),
            매출합계=("total_price_num", "sum"),
        )
        .reset_index()
    )
    # 제안값은 여기서 다시 계산한다. left_joined는 출력 컬럼으로 reindex되면서
    # option_kind_제안을 떨어뜨리므로 컬럼에 의존하면 깨진다.
    grouped["option_kind_제안"] = [
        _suggest_option_kind(name, role)
        for name, role in zip(grouped["item_name"], grouped["line_role"])
    ]
    grouped["주문건수"] = grouped["주문건수"].map(_format_number)
    grouped["판매수량"] = grouped["판매수량"].map(_format_number)
    grouped["매출합계"] = grouped["매출합계"].map(_format_number)
    grouped["재료명_제안"] = [
        _suggest_material_name(name, kind)
        for name, kind in zip(grouped["item_name"], grouped["option_kind_제안"])
    ]

    previous = _option_kind_master_attrs()
    if previous.empty:
        for col in OPTION_KIND_MASTER_EDIT_COLUMNS:
            grouped[col] = ""
    else:
        previous_keys = _option_kind_row_keys(previous)
        kept = {
            col: dict(zip(previous_keys, previous[col].astype(str).str.strip()))
            for col in OPTION_KIND_MASTER_EDIT_COLUMNS
        }
        grouped_keys = _option_kind_row_keys(grouped)
        for col in OPTION_KIND_MASTER_EDIT_COLUMNS:
            grouped[col] = [kept[col].get(key, "") for key in grouped_keys]
        current_keys = set(grouped_keys)
        preserved = previous[
            [
                key not in current_keys
                and any(str(row.get(col, "") or "").strip() for col in OPTION_KIND_MASTER_EDIT_COLUMNS)
                for key, (_, row) in zip(previous_keys, previous.iterrows())
            ]
        ]
        if not preserved.empty:
            grouped = pd.concat(
                [grouped, preserved.reindex(columns=OPTION_KIND_MASTER_COLUMNS, fill_value="")],
                ignore_index=True,
                sort=False,
            )

    forced_addon = [
        str(role or "").strip() == "option" and _looks_like_chicken_addon_option(name)
        for name, role in zip(grouped["item_name"], grouped["line_role"])
    ]
    if any(forced_addon):
        grouped.loc[forced_addon, "option_kind_확정"] = OPTION_KIND_CHICKEN_ADDON

    return grouped.reindex(columns=OPTION_KIND_MASTER_COLUMNS, fill_value="")


def _option_material_attrs() -> pd.DataFrame:
    columns = [*OPTION_MATERIAL_INPUT_KEY_COLUMNS, "메모"]
    option_input = _manual_or_legacy_sheet("옵션재료", OPTION_MATERIAL_INPUT_OUTPUT_PATH).fillna("")
    if option_input.empty:
        return pd.DataFrame(columns=columns)
    if not set(OPTION_MATERIAL_INPUT_KEY_COLUMNS).issubset(option_input.columns):
        return pd.DataFrame(columns=columns)
    extra_manual_columns = [
        str(col).strip()
        for col in option_input.columns
        if _is_material_usage_manual_column(col)
    ]
    columns = [*OPTION_MATERIAL_INPUT_KEY_COLUMNS, *_option_material_edit_columns(_option_material_input_columns(extra_manual_columns))]
    for col in columns:
        if col not in option_input.columns:
            option_input[col] = ""
        option_input[col] = option_input[col].fillna("").astype(str).str.strip()
    option_input = _target_product_rows(option_input)
    return (
        option_input.reindex(columns=columns, fill_value="")
        .drop_duplicates(subset=OPTION_MATERIAL_INPUT_KEY_COLUMNS, keep="last")
        .reset_index(drop=True)
    )


def _menu_weight_attrs() -> pd.DataFrame:
    columns = [*MENU_WEIGHT_INPUT_KEY_COLUMNS, *_menu_weight_edit_columns(MENU_WEIGHT_INPUT_COLUMNS)]
    weight_input = _manual_or_legacy_sheet("메뉴중량", MENU_WEIGHT_INPUT_OUTPUT_PATH).fillna("")
    if weight_input.empty:
        return pd.DataFrame(columns=columns)
    if "source" not in weight_input.columns:
        weight_input["source"] = ""
    if "사이즈키" not in weight_input.columns and "사이즈" in weight_input.columns:
        weight_input["사이즈키"] = weight_input["사이즈"]
    if "닭유형키" not in weight_input.columns and "닭유형" in weight_input.columns:
        weight_input["닭유형키"] = weight_input["닭유형"]
    if "추가재료키" not in weight_input.columns:
        weight_input["추가재료키"] = ""
    if not set(MENU_WEIGHT_INPUT_KEY_COLUMNS).issubset(weight_input.columns):
        return pd.DataFrame(columns=columns)
    extra_manual_columns = [
        str(col).strip()
        for col in weight_input.columns
        if _is_material_usage_manual_column(col)
    ]
    columns = [*MENU_WEIGHT_INPUT_KEY_COLUMNS, *_menu_weight_edit_columns(_menu_weight_input_columns(extra_manual_columns))]
    for col in columns:
        if col not in weight_input.columns:
            weight_input[col] = ""
        weight_input[col] = weight_input[col].fillna("").astype(str).str.strip()
    weight_input = _target_product_rows(weight_input)
    return (
        weight_input.reindex(columns=columns, fill_value="")
        .drop_duplicates(subset=MENU_WEIGHT_INPUT_KEY_COLUMNS, keep="last")
        .reset_index(drop=True)
    )


def _weight_size_key(values: list[str]) -> str:
    sizes = _unique_nonempty(values)
    if len(sizes) == 1:
        return sizes[0]
    concrete = [size for size in sizes if size in {"소", "중", "대", "1인", "2인"}]
    concrete = _unique_nonempty(concrete)
    return concrete[0] if len(concrete) == 1 else MENU_WEIGHT_UNKNOWN


def _weight_chicken_type_key(values: list[str], context: str) -> str:
    types = _unique_nonempty(values)
    if "순살" in types:
        return "순살"
    if "뼈닭" in types:
        return "뼈닭"
    if _has_any_token(context, _CHICKEN_MENU_TOKENS):
        return "뼈닭"
    return MENU_WEIGHT_UNKNOWN


def _weight_extra_key(context: str) -> str:
    extras: list[str] = []
    if "우거지" in context:
        extras.append("우거지")
    if re.search(r"순살\s*(?:변경|로\s*변경)", context):
        extras.append("순살추가")
    return _unique_join(extras) or MENU_WEIGHT_EXTRA_NONE


def _weight_keys_from_group(group: pd.DataFrame) -> dict[str, str]:
    context_texts = (
        group.get("std_menu_name", pd.Series("", index=group.index)).astype(str)
        + " "
        + group.get("menu_name", pd.Series("", index=group.index)).astype(str)
        + " "
        + group.get("item_name", pd.Series("", index=group.index)).astype(str)
    ).tolist()
    context = " ".join(context_texts)
    sizes = [item for text in context_texts for item in _infer_chicken_sizes(text)]
    types = [item for text in context_texts for item in _infer_chicken_types(text)]
    return {
        "사이즈키": _weight_size_key(sizes),
        "닭유형키": _weight_chicken_type_key(types, context),
        "추가재료키": _weight_extra_key(context),
    }


def _build_menu_weight_group_attrs(left_joined: pd.DataFrame) -> pd.DataFrame:
    columns = [
        *ORDER_GROUP_COLUMNS,
        *[col for col in MENU_WEIGHT_INPUT_KEY_COLUMNS if col not in ORDER_GROUP_COLUMNS],
        "주문건수",
        "판매수량",
        "매출합계",
    ]
    if left_joined.empty:
        return pd.DataFrame(columns=columns)

    work = _ensure_order_group_columns(left_joined).fillna("")
    for col in ["qty", "total_price"]:
        work[f"{col}_num"] = pd.to_numeric(work.get(col, pd.Series("", index=work.index)), errors="coerce").fillna(0)
    rows = []
    for group_key, group in work.groupby(ORDER_GROUP_COLUMNS, dropna=False, sort=False):
        non_fee = group[~group.get("line_role", pd.Series("", index=group.index)).isin(["fee", "discount"])].copy()
        if non_fee.empty:
            continue
        context = " ".join(
            non_fee[[c for c in ["std_menu_name", "menu_name", "item_name"] if c in non_fee.columns]]
            .astype(str)
            .agg(" ".join, axis=1)
            .tolist()
        )
        keys = _weight_keys_from_group(non_fee)
        if (
            not _has_any_token(context, _CHICKEN_MENU_TOKENS)
            and keys["사이즈키"] == MENU_WEIGHT_UNKNOWN
            and keys["닭유형키"] == MENU_WEIGHT_UNKNOWN
            and keys["추가재료키"] == MENU_WEIGHT_EXTRA_NONE
        ):
            continue
        main = non_fee[non_fee["line_role"].eq("main")]
        std_menu_name = _first_clean_value(main.get("std_menu_name", pd.Series(dtype=str)))
        if not std_menu_name:
            std_menu_name = _first_clean_value(non_fee.get("std_menu_name", pd.Series(dtype=str)))
        if not std_menu_name:
            std_menu_name = _first_clean_value(main.get("item_name", pd.Series(dtype=str)))
        main_qty = pd.to_numeric(main.get("qty", pd.Series(dtype=str)), errors="coerce").fillna(0).max() if len(main) else 0
        if not main_qty or pd.isna(main_qty):
            main_qty = pd.to_numeric(non_fee["qty"], errors="coerce").fillna(0).max()
        rows.append({
            **dict(zip(ORDER_GROUP_COLUMNS, group_key)),
            "source": group_key[0],
            "brand": group_key[1],
            "store": group_key[2],
            "std_menu_name": std_menu_name,
            **keys,
            "주문건수": "1",
            "판매수량": _format_number(main_qty or 1),
            "매출합계": _format_number(non_fee["total_price_num"].sum()),
        })
    return pd.DataFrame(rows, columns=columns)


def _attach_manual_chicken_columns(
    left_joined: pd.DataFrame,
    group_attrs: pd.DataFrame | None = None,
) -> pd.DataFrame:
    out = _ensure_order_group_columns(left_joined)
    for col in CHICKEN_FINAL_COLUMNS:
        out[col] = ""
    out[MATERIAL_USAGE_COLUMN] = ""
    out[MENU_WEIGHT_USAGE_COLUMN] = ""
    group_attrs = _build_order_group_attrs(out) if group_attrs is None else group_attrs
    review_attrs = _manual_chicken_attrs()
    manager_attrs = _normalize_option_combo_key_frame(_manager_input_attrs())
    manager_material_columns = [
        col for col in manager_attrs.columns if col not in _MANUAL_CHICKEN_COLUMNS and _is_material_usage_manual_column(col)
    ]
    if "닭사용량_manual" in manager_attrs.columns:
        manager_material_columns.insert(0, "닭사용량_manual")
    manager_material_columns = list(dict.fromkeys(manager_material_columns))
    if group_attrs.empty and review_attrs.empty and manager_attrs.empty:
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")

    product_key = ["source", "brand", "store", "item_id", "item_name"]
    merged = out
    if not group_attrs.empty:
        group_merge = group_attrs[
            [
                *ORDER_GROUP_COLUMNS,
                "std_menu_name",
                OPTION_COMBO_COLUMN,
                CHICKEN_OPTION_KEY_COLUMN,
                "is_chicken_group",
                "has_main",
                "닭유형_auto",
                "사이즈_auto",
                "닭사용량_auto",
                "닭유형_판정_auto",
                "사이즈_판정_auto",
                "반반조합_auto",
                "반반슬롯1_auto",
                "반반슬롯2_auto",
                "뼈비율_auto",
                "미해결사유_auto",
            ]
        ].rename(columns={"std_menu_name": "_review_std_menu_name"})
        merged = merged.merge(
            group_merge,
            on=ORDER_GROUP_COLUMNS,
            how="left",
        )
        for col in (OPTION_COMBO_COLUMN, CHICKEN_OPTION_KEY_COLUMN):
            right_col = f"{col}_y"
            left_col = f"{col}_x"
            if right_col in merged.columns:
                merged[col] = merged[right_col].astype(str).str.strip()
                if left_col in merged.columns:
                    fallback = merged[col].eq("")
                    merged.loc[fallback, col] = merged.loc[fallback, left_col].astype(str).str.strip()
                merged = merged.drop(columns=[left_col, right_col], errors="ignore")
    else:
        for col in [
            "_review_std_menu_name",
            OPTION_COMBO_COLUMN,
            CHICKEN_OPTION_KEY_COLUMN,
            "is_chicken_group",
            "has_main",
            "닭유형_auto",
            "사이즈_auto",
            "닭사용량_auto",
            "닭유형_판정_auto",
            "사이즈_판정_auto",
            "반반조합_auto",
            "반반슬롯1_auto",
            "반반슬롯2_auto",
            "뼈비율_auto",
            "미해결사유_auto",
        ]:
            if col not in merged.columns:
                merged[col] = ""
    merged["std_menu_name"] = merged["std_menu_name"].astype(str).str.strip().where(
        merged["std_menu_name"].astype(str).str.strip().ne(""),
        merged.get("_review_std_menu_name", pd.Series("", index=merged.index)).astype(str).str.strip(),
    )
    missing_std = merged["std_menu_name"].astype(str).str.strip().eq("")
    if missing_std.any():
        merged.loc[missing_std, "std_menu_name"] = merged.loc[missing_std].apply(_review_menu_name_fallback, axis=1)
    if not review_attrs.empty:
        for col in product_key:
            if col not in review_attrs.columns:
                review_attrs[col] = ""
        merged = merged.merge(review_attrs, on=product_key, how="left")
    else:
        for col in _MANUAL_CHICKEN_COLUMNS:
            merged[col] = ""
    manager_edit_columns = _manager_edit_columns(manager_attrs.columns)
    default_key = [col for col in MANAGER_INPUT_KEY_COLUMNS if col != CHICKEN_OPTION_KEY_COLUMN]
    if not manager_attrs.empty:
        manager_exact = manager_attrs[manager_attrs[CHICKEN_OPTION_KEY_COLUMN].ne(MANAGER_DEFAULT_OPTION_COMBO)].copy()
        if not manager_exact.empty:
            merged = merged.merge(
                manager_exact.reindex(columns=[*MANAGER_INPUT_KEY_COLUMNS, *manager_edit_columns], fill_value="").rename(
                    columns={col: f"{col}_manager" for col in manager_edit_columns}
                ),
                on=MANAGER_INPUT_KEY_COLUMNS,
                how="left",
            )
        manager_default = manager_attrs[manager_attrs[CHICKEN_OPTION_KEY_COLUMN].eq(MANAGER_DEFAULT_OPTION_COMBO)].copy()
        if not manager_default.empty:
            merged = merged.merge(
                manager_default.reindex(columns=[*default_key, *manager_edit_columns], fill_value="")
                .drop_duplicates(subset=default_key, keep="last")
                .rename(columns={col: f"{col}_default" for col in manager_edit_columns}),
                on=default_key,
                how="left",
            )
    for col in manager_edit_columns:
        for suffix in ("_manager", "_default"):
            merged_col = f"{col}{suffix}"
            if merged_col not in merged.columns:
                merged[merged_col] = ""
    merged = merged.fillna("")

    for col in _MANUAL_CHICKEN_COLUMNS:
        manager_col = f"{col}_manager"
        if manager_col not in merged.columns:
            merged[manager_col] = ""
        default_col = f"{col}_default"
        if default_col not in merged.columns:
            merged[default_col] = ""

    def coalesce(*columns: str) -> pd.Series:
        result = pd.Series("", index=merged.index)
        for column in columns:
            values = merged.get(column, pd.Series("", index=merged.index)).astype(str).str.strip()
            result = result.where(result.ne(""), values)
        return result

    def is_codex_auto_memo(column: str) -> pd.Series:
        memo = merged.get(column, pd.Series("", index=merged.index)).astype(str)
        return memo.str.contains(r"\bCodex\b|자동채움", regex=True, na=False)

    manager_type = coalesce("닭유형_manual_manager")
    manager_default_type = coalesce("닭유형_manual_default")
    review_type = coalesce("닭유형_manual")
    auto_type = coalesce("닭유형_auto")
    manager_size = coalesce("사이즈_manual_manager")
    manager_default_size = coalesce("사이즈_manual_default")
    review_size = coalesce("사이즈_manual")
    auto_size = coalesce("사이즈_auto")
    manager_usage = coalesce("닭사용량_manual_manager")
    manager_default_usage = coalesce("닭사용량_manual_default")
    review_usage = coalesce("닭사용량_manual")
    auto_usage = coalesce("닭사용량_auto")
    auto_half_combo = coalesce("반반조합_auto")
    auto_slot1 = coalesce("반반슬롯1_auto")
    auto_slot2 = coalesce("반반슬롯2_auto")
    auto_bone_ratio = coalesce("뼈비율_auto")
    auto_size_method = merged.get("사이즈_판정_auto", pd.Series("", index=merged.index)).astype(str).str.strip()
    auto_size_is_explicit = auto_size_method.isin(["유료사이즈", "선택", "변경"])
    codex_manager_size_conflict = (
        manager_size.ne("")
        & auto_size.ne("")
        & manager_size.ne(auto_size)
        & auto_size_is_explicit
        & is_codex_auto_memo("메모_manager")
    )
    manager_size = manager_size.where(~codex_manager_size_conflict, "")
    codex_default_size_conflict = (
        manager_default_size.ne("")
        & auto_size.ne("")
        & manager_default_size.ne(auto_size)
        & auto_size_is_explicit
        & is_codex_auto_memo("메모_default")
    )
    manager_default_size = manager_default_size.where(~codex_default_size_conflict, "")
    is_half_context = auto_slot1.ne("") | auto_slot2.ne("") | auto_half_combo.ne("")
    manager_type = manager_type.where(~is_half_context, "")
    manager_usage = manager_usage.where(~is_half_context, "")
    manager_default_type = manager_default_type.where(~is_half_context, "")
    manager_default_usage = manager_default_usage.where(~is_half_context, "")

    merged["닭유형"] = manager_type.where(
        manager_type.ne(""),
        review_type.where(review_type.ne(""), auto_type.where(auto_type.ne(""), manager_default_type)),
    )
    merged["사이즈"] = manager_size.where(
        manager_size.ne(""),
        review_size.where(review_size.ne(""), auto_size.where(auto_size.ne(""), manager_default_size)),
    )
    merged["사용용량"] = manager_usage.where(
        manager_usage.ne(""),
        review_usage.where(review_usage.ne(""), auto_usage.where(auto_usage.ne(""), manager_default_usage)),
    )
    missing_usage = merged["사용용량"].astype(str).str.strip().eq("")
    inferred_usage = merged.apply(lambda row: _usage_for(row.get("닭유형", ""), row.get("사이즈", "")), axis=1)
    explicit_usage = manager_usage.ne("") | review_usage.ne("")
    inferred_available = inferred_usage.astype(str).str.strip().ne("")
    merged.loc[~explicit_usage & inferred_available, "사용용량"] = inferred_usage[~explicit_usage & inferred_available]
    merged.loc[missing_usage, "사용용량"] = inferred_usage[missing_usage]
    merged["닭유형_판정"] = "미해결"
    auto_type_method = merged.get("닭유형_판정_auto", pd.Series("", index=merged.index)).astype(str).str.strip()
    # 수기가 닭유형_판정을 덮어쓰기 전에 원천 신호 유무를 따로 남긴다. 이 값이 없으면
    # 나중에 "담당자가 감으로 채운 구간"과 "주문에 실제로 적혀 있던 구간"을 구분할 수 없다.
    merged[CHICKEN_SIGNAL_COLUMN] = auto_type_method.map(
        lambda value: CHICKEN_SIGNAL_PRESENT if value in CHICKEN_SIGNAL_METHODS else CHICKEN_SIGNAL_ABSENT
    )
    visible_chicken_signal = (
        merged.get("menu_name", pd.Series("", index=merged.index)).astype(str)
        + " "
        + merged.get("std_menu_name", pd.Series("", index=merged.index)).astype(str)
        + " "
        + merged.get("item_name", pd.Series("", index=merged.index)).astype(str)
    ).map(lambda text: bool(_infer_chicken_types(text)))
    merged.loc[visible_chicken_signal, CHICKEN_SIGNAL_COLUMN] = CHICKEN_SIGNAL_PRESENT
    # 비율은 _apply_chicken_ratio에서 채우지만 컬럼은 여기서 만든다. 아래 fee/discount
    # 블랭킹이 CHICKEN_TRACE_COLUMNS를 통째로 지우면서 없는 컬럼을 float64로 만들어버린다.
    merged[CHICKEN_RATIO_APPLIED_COLUMN] = ""
    merged[HALF_COMBO_COLUMN] = auto_half_combo
    merged[HALF_SLOT1_COLUMN] = auto_slot1
    merged[HALF_SLOT2_COLUMN] = auto_slot2
    half_ratio_present = auto_half_combo.ne("") & auto_bone_ratio.ne("")
    merged.loc[half_ratio_present, CHICKEN_RATIO_APPLIED_COLUMN] = auto_bone_ratio[half_ratio_present]
    merged.loc[manager_default_type.ne(""), "닭유형_판정"] = "기본값"
    merged.loc[auto_type.ne(""), "닭유형_판정"] = auto_type_method.where(auto_type_method.ne(""), "미해결")
    merged.loc[review_type.ne(""), "닭유형_판정"] = "수기"
    merged.loc[manager_type.ne(""), "닭유형_판정"] = "수기"

    merged["사이즈_판정"] = "미해결"
    merged.loc[manager_default_size.ne(""), "사이즈_판정"] = "기본값"
    merged.loc[auto_size.ne(""), "사이즈_판정"] = auto_size_method.where(auto_size_method.ne(""), "미해결")
    merged.loc[review_size.ne(""), "사이즈_판정"] = "수기"
    merged.loc[manager_size.ne(""), "사이즈_판정"] = "수기"

    fixed_profiles = merged.apply(_fixed_main_chicken_profile, axis=1)
    existing_type_method = merged.get("닭유형_판정", pd.Series("", index=merged.index)).astype(str).str.strip()
    fixed_mask = fixed_profiles.notna() & existing_type_method.ne("메뉴프로필")
    if fixed_mask.any():
        fixed_values = pd.DataFrame(
            fixed_profiles[fixed_mask].tolist(),
            index=merged.index[fixed_mask],
            columns=["_fixed_type", "_fixed_size", "_fixed_method"],
        )
        merged.loc[fixed_mask, "닭유형"] = fixed_values["_fixed_type"]
        merged.loc[fixed_mask, "사이즈"] = fixed_values["_fixed_size"]
        merged.loc[fixed_mask, "사용용량"] = fixed_values.apply(
            lambda row: _expected_usage_for(row["_fixed_type"], row["_fixed_size"]),
            axis=1,
        )
        merged.loc[fixed_mask, "닭유형_판정"] = fixed_values["_fixed_method"]
        merged.loc[fixed_mask, "사이즈_판정"] = fixed_values["_fixed_method"]
        # 고정 메뉴 규칙은 계산용 판정이다. 주문서 텍스트에 뼈/순살이 실제로 있을 때만
        # 원천 신호로 본다.
        fixed_context = (
            merged.get("menu_name", pd.Series("", index=merged.index)).fillna("").astype(str)
            + " "
            + merged.get("std_menu_name", pd.Series("", index=merged.index)).fillna("").astype(str)
            + " "
            + merged.get("item_name", pd.Series("", index=merged.index)).fillna("").astype(str)
        )
        fixed_signal = fixed_context.map(lambda value: bool(_infer_chicken_types(value)))
        merged.loc[fixed_mask, CHICKEN_SIGNAL_COLUMN] = fixed_signal.loc[fixed_mask].map(
            lambda value: CHICKEN_SIGNAL_PRESENT if value else CHICKEN_SIGNAL_ABSENT
        )

    manager_profit = coalesce("수익률_manual_manager", "수익률_manual_default")
    manager_profit = manager_profit.where(~manager_profit.isin(["0", "0.0", "0.00", "0%"]), "")
    review_profit = merged.get("수익률_manual", pd.Series("", index=merged.index)).astype(str).str.strip()
    review_profit = review_profit.where(~review_profit.isin(["0", "0.0", "0.00", "0%"]), "")
    profit = manager_profit.where(manager_profit.ne(""), review_profit)
    merged["수익률"] = profit.where(profit.ne(""), "")
    profit_num = pd.to_numeric(merged["수익률"].astype(str).str.replace("%", "", regex=False), errors="coerce")
    total_price = pd.to_numeric(merged.get("total_price", pd.Series("", index=merged.index)), errors="coerce")
    estimated_profit = (total_price * profit_num / 100).round()
    merged["추정수익"] = estimated_profit.map(lambda value: "" if pd.isna(value) else f"{float(value):g}")
    for col in manager_material_columns:
        merged[f"{col}_effective"] = coalesce(f"{col}_manager", f"{col}_default", col)
    material_source_cols = [
        (col, f"{col}_effective")
        for col in manager_material_columns
    ]
    if material_source_cols:
        merged[MATERIAL_USAGE_COLUMN] = merged.apply(lambda row: _combine_material_usage(row, material_source_cols), axis=1)
    is_chicken_group = merged.get("is_chicken_group", pd.Series(False, index=merged.index)).astype(bool)
    non_chicken_group = ~is_chicken_group
    if non_chicken_group.any():
        # 닭을 안 쓰는 그룹은 빈값이 아니라 '닭미사용'으로 명시한다. 빈값으로 두면
        # 판정 실패와 구분되지 않아 완결률의 분자를 셀 수 없다.
        merged.loc[non_chicken_group, "닭유형"] = CHICKEN_TYPE_NONE
        merged.loc[non_chicken_group, "사이즈"] = CHICKEN_SIZE_NONE
        merged.loc[non_chicken_group, "사용용량"] = "0"
        merged.loc[non_chicken_group, ["닭유형_판정", "사이즈_판정"]] = CHICKEN_METHOD_NONE
        merged.loc[non_chicken_group, CHICKEN_SIGNAL_COLUMN] = CHICKEN_METHOD_NONE
        merged.loc[non_chicken_group, MATERIAL_USAGE_COLUMN] = (
            merged.loc[non_chicken_group, MATERIAL_USAGE_COLUMN].map(_remove_chicken_material_usage)
        )
    role_text = merged.get("line_role", pd.Series("", index=merged.index)).astype(str).str.strip()
    non_main_line = role_text.ne("") & role_text.ne("main")
    if non_main_line.any():
        merged.loc[non_main_line, "사용용량"] = ""
        merged.loc[non_main_line, CHICKEN_SIGNAL_COLUMN] = ""
        merged.loc[non_main_line, MATERIAL_USAGE_COLUMN] = (
            merged.loc[non_main_line, MATERIAL_USAGE_COLUMN].map(_remove_chicken_material_usage)
        )
    has_main = merged.get("has_main", pd.Series(True, index=merged.index)).astype(bool)
    line_role = merged.get("line_role", pd.Series("", index=merged.index))
    item_name = merged.get("item_name", pd.Series("", index=merged.index)).astype(str).str.strip()
    total_price_num = pd.to_numeric(
        merged.get("total_price", pd.Series("", index=merged.index)),
        errors="coerce",
    ).fillna(0)
    no_main_candidate = item_name.map(
        lambda value: bool(
            value
            and _has_any_token(value, _MAIN_CANDIDATE_TOKENS)
            and not _OPTION_LIKE_RE.search(value)
            and not _SIDE_NAME_RE.search(value)
        )
    ) & total_price_num.ne(0)
    main_chicken_group = is_chicken_group & line_role.eq("main")
    merged = _fill_size_from_menu_mode(merged)
    missing_usage = merged["사용용량"].astype(str).str.strip().eq("")
    inferred_usage = merged.apply(lambda row: _usage_for(row.get("닭유형", ""), row.get("사이즈", "")), axis=1)
    inferred_available = inferred_usage.astype(str).str.strip().ne("")
    merged.loc[main_chicken_group & missing_usage & inferred_available, "사용용량"] = inferred_usage[
        main_chicken_group & missing_usage & inferred_available
    ]
    missing_reasons = pd.Series("", index=merged.index)
    missing_reasons = missing_reasons.mask(
        ~has_main & no_main_candidate & ~line_role.isin(["side", "fee", "discount"]),
        "메인없음",
    )
    missing_reasons = missing_reasons.mask(main_chicken_group & merged["닭유형"].astype(str).str.strip().eq(""), "닭유형없음")
    missing_reasons = missing_reasons.mask(
        main_chicken_group
        & merged["닭유형"].astype(str).str.strip().ne("")
        & merged["사이즈"].astype(str).str.strip().eq(""),
        "사이즈없음",
    )
    missing_reasons = missing_reasons.mask(
        main_chicken_group
        & merged["닭유형"].astype(str).str.strip().ne("")
        & merged["사이즈"].astype(str).str.strip().ne("")
        & merged["사용용량"].astype(str).str.strip().eq(""),
        "환산표없음",
    )
    # 그룹 추론이 남긴 사유(사이즈충돌:소|중 등)를 함께 붙인다. 규칙 기반 사유만으로는
    # 왜 못 정했는지가 아니라 무엇이 비었는지만 알 수 있다.
    auto_reasons = merged.get("미해결사유_auto", pd.Series("", index=merged.index)).fillna("").astype(str).str.strip()
    auto_reasons = auto_reasons.where(line_role.eq("main"), "")
    merged["미해결사유"] = [
        " | ".join(_unique_nonempty([str(rule).strip(), str(auto).strip()]))
        for rule, auto in zip(missing_reasons, auto_reasons)
    ]
    non_chicken_final = merged["닭유형"].astype(str).str.strip().eq(CHICKEN_TYPE_NONE)
    if non_chicken_final.any():
        merged.loc[non_chicken_final, "미해결사유"] = ""
    usage_per_menu = pd.to_numeric(merged[CHICKEN_USAGE_COLUMN], errors="coerce")
    line_qty = pd.to_numeric(merged.get("qty", pd.Series("", index=merged.index)), errors="coerce").fillna(0)
    usage_total = usage_per_menu * line_qty
    merged[CHICKEN_USAGE_TOTAL_COLUMN] = usage_total.map(
        lambda value: "" if pd.isna(value) else f"{float(value):g}"
    )
    merged = _attach_chicken_usage_split_columns(merged)
    non_order_line = merged.get("line_role", pd.Series("", index=merged.index)).isin(["fee", "discount"])
    blank_columns = [*CHICKEN_FINAL_COLUMNS, *CHICKEN_TRACE_COLUMNS, MATERIAL_USAGE_COLUMN]
    for column in blank_columns:
        if column in merged.columns:
            merged[column] = merged[column].astype(object)
            merged.loc[non_order_line, column] = ""
    drop_cols = [
        col
        for col in [
            "닭유형_auto",
            "사이즈_auto",
            "닭사용량_auto",
            "닭유형_판정_auto",
            "사이즈_판정_auto",
            "반반조합_auto",
            "반반슬롯1_auto",
            "반반슬롯2_auto",
            "뼈비율_auto",
            "미해결사유_auto",
            "is_chicken_group",
            "has_main",
            "_review_std_menu_name",
            *_MANUAL_CHICKEN_COLUMNS,
            *[f"{col}_manager" for col in _MANUAL_CHICKEN_COLUMNS],
            *[f"{col}_default" for col in _MANUAL_CHICKEN_COLUMNS],
            "수익률_manual",
            "수익률_manual_manager",
            "수익률_manual_default",
            "메모",
            "메모_manager",
            "메모_default",
        ]
        if col in merged.columns
    ]
    drop_cols.extend(
        col
        for original_col in manager_material_columns
        for col in [original_col, f"{original_col}_manager", f"{original_col}_default", f"{original_col}_effective"]
        if col in merged.columns and col not in _MANUAL_CHICKEN_COLUMNS
    )
    merged = merged.drop(columns=drop_cols)
    return merged.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")


def _blend_mixed_menu_weight_usage(
    merged: pd.DataFrame,
    master: pd.DataFrame,
    material_columns: list[str],
) -> pd.DataFrame:
    """닭유형='혼합' 행의 표준중량을 뼈닭/순살 두 행에서 가중 합성한다.

    22번에 혼합 행을 만들지 않기로 했으므로(담당자가 채울 값이 아니다) 조인이 비고,
    비워두면 무신호 구간의 재료원가가 통째로 사라진다. 여기서 파생값을 만든다.
    한쪽 유형만 22번에 있으면 있는 쪽 값을 그대로 쓴다 — 재료 구성은 대개 같고
    다른 것은 닭 중량뿐이라, 비우는 것보다 낫다.
    """
    if merged.empty or master.empty or not material_columns:
        return merged
    chicken_type = merged.get("닭유형", pd.Series("", index=merged.index)).astype(str).str.strip()
    mixed = chicken_type.eq(CHICKEN_TYPE_MIXED)
    if not mixed.any():
        return merged

    type_pos = MENU_WEIGHT_MASTER_KEY_COLUMNS.index("닭유형")
    lookup: dict[tuple[str, ...], dict[str, float]] = {}
    for _, row in master.iterrows():
        key = tuple(str(row.get(col, "")).strip() for col in MENU_WEIGHT_MASTER_KEY_COLUMNS)
        values: dict[str, float] = {}
        for col in material_columns:
            try:
                values[col] = float(str(row.get(col, "")).strip())
            except ValueError:
                continue
        if values:
            lookup[key] = values

    key_frame = {
        col: merged.get(col, pd.Series("", index=merged.index)).fillna("").astype(str).str.strip()
        for col in MENU_WEIGHT_MASTER_KEY_COLUMNS
    }
    ratios = pd.to_numeric(
        merged.get(CHICKEN_RATIO_APPLIED_COLUMN, pd.Series("", index=merged.index)), errors="coerce"
    )
    blended_texts: dict[int, str] = {}
    blended_missing: dict[int, str] = {}
    for idx in merged.index[mixed]:
        ratio = ratios.at[idx]
        if pd.isna(ratio):
            continue
        base = [key_frame[col].at[idx] for col in MENU_WEIGHT_MASTER_KEY_COLUMNS]
        bone_key = list(base)
        bone_key[type_pos] = "뼈닭"
        boneless_key = list(base)
        boneless_key[type_pos] = "순살"
        bone = lookup.get(tuple(bone_key), {})
        boneless = lookup.get(tuple(boneless_key), {})
        if not bone and not boneless:
            continue
        pairs = []
        missing = []
        for col in material_columns:
            material = _material_name_from_usage_column(col)
            if not material:
                continue
            bone_value = bone.get(col)
            boneless_value = boneless.get(col)
            if bone_value is None and boneless_value is None:
                # 뼈닭/순살 어느 쪽에도 값이 없으면 미입력이다. 혼합만 조용히 넘어가면
                # 비율추정 행에서만 부재료 가드가 뚫린다.
                missing.append(material)
                continue
            if bone_value is None:
                amount = boneless_value
            elif boneless_value is None:
                amount = bone_value
            else:
                amount = float(ratio) * bone_value + (1.0 - float(ratio)) * boneless_value
            pairs.append(f"{material}={float(amount):g}")
        if pairs:
            blended_texts[idx] = " | ".join(pairs)
            blended_missing[idx] = " | ".join(missing)
    if blended_texts:
        merged.loc[list(blended_texts), MENU_WEIGHT_USAGE_COLUMN] = pd.Series(blended_texts)
        merged.loc[list(blended_missing), MENU_WEIGHT_MISSING_COLUMN] = pd.Series(blended_missing)
        logger.info("혼합 표준중량 합성: %d행", len(blended_texts))
    return merged


def _attach_menu_weight_usage_columns(
    left_joined: pd.DataFrame,
    menu_weight_master: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """22번 마스터의 표준중량을 main 행에 붙인다.

    22번은 (메뉴, 사이즈, 닭유형)이라 이미 확정된 닭 속성으로 바로 조인된다.
    17번의 추가재료키 기반 조인보다 조합이 적고 어긋날 여지가 없다.
    """
    out = left_joined.copy()
    out[MENU_WEIGHT_USAGE_COLUMN] = ""
    out[MENU_WEIGHT_MISSING_COLUMN] = ""
    master = _menu_weight_master_attrs() if menu_weight_master is None else menu_weight_master
    master_material_columns = [
        col for col in master.columns if _is_material_usage_manual_column(col)
    ]
    if not out.empty and not master.empty and master_material_columns:
        usable = master.reindex(
            columns=[*MENU_WEIGHT_MASTER_KEY_COLUMNS, *master_material_columns], fill_value=""
        ).drop_duplicates(subset=MENU_WEIGHT_MASTER_KEY_COLUMNS, keep="last")
        work = out.copy()
        for col in MENU_WEIGHT_MASTER_KEY_COLUMNS:
            if col not in work.columns:
                work[col] = ""
            work[col] = work[col].fillna("").astype(str).str.strip()
        merged = work.merge(
            usable, on=MENU_WEIGHT_MASTER_KEY_COLUMNS, how="left", suffixes=("", "_master")
        ).fillna("")
        source_cols = [
            (col, f"{col}_master" if f"{col}_master" in merged.columns else col)
            for col in master_material_columns
        ]
        merged[MENU_WEIGHT_USAGE_COLUMN] = merged.apply(
            lambda row: _combine_material_usage(row, source_cols), axis=1
        )
        # 값이 빈 재료를 이름으로 남긴다. 이게 없으면 "안 쓰는 재료"와 "미입력"이 같아 보여
        # 닭값만 반영한 원가가 산출된 것처럼 나온다.
        merged[MENU_WEIGHT_MISSING_COLUMN] = merged.apply(
            lambda row: " | ".join(
                name
                for original_col, merged_col in source_cols
                for name in [_material_name_from_usage_column(original_col)]
                if name and not str(row.get(merged_col, "") or "").strip()
            ),
            axis=1,
        )
        merged = _blend_mixed_menu_weight_usage(merged, usable, master_material_columns)
        role_text = merged.get("line_role", pd.Series("", index=merged.index)).astype(str).str.strip()
        merged.loc[role_text.ne("main"), [MENU_WEIGHT_USAGE_COLUMN, MENU_WEIGHT_MISSING_COLUMN]] = ""
        return merged.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")

    # 22번이 아직 비어 있으면 기존 17번 경로로 폴백한다.
    weight_attrs = _menu_weight_attrs()
    weight_material_columns = [
        col for col in weight_attrs.columns if _is_material_usage_manual_column(col)
    ]
    if out.empty or weight_attrs.empty or not weight_material_columns:
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")

    group_attrs = _build_menu_weight_group_attrs(out)
    if group_attrs.empty:
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")

    merged = out.merge(
        group_attrs.reindex(
            columns=[*ORDER_GROUP_COLUMNS, *[col for col in MENU_WEIGHT_INPUT_KEY_COLUMNS if col not in ORDER_GROUP_COLUMNS]],
            fill_value="",
        ),
        on=ORDER_GROUP_COLUMNS,
        how="left",
        suffixes=("", "_weight_key"),
    ).fillna("")
    merged = merged.merge(
        weight_attrs.reindex(columns=[*MENU_WEIGHT_INPUT_KEY_COLUMNS, *weight_material_columns], fill_value=""),
        on=MENU_WEIGHT_INPUT_KEY_COLUMNS,
        how="left",
        suffixes=("", "_weight_input"),
    ).fillna("")
    material_source_cols = [
        (col, f"{col}_weight_input" if f"{col}_weight_input" in merged.columns else col)
        for col in weight_material_columns
    ]
    merged[MENU_WEIGHT_USAGE_COLUMN] = merged.apply(lambda row: _combine_material_usage(row, material_source_cols), axis=1)
    role_text = merged.get("line_role", pd.Series("", index=merged.index)).astype(str).str.strip()
    non_main_line = role_text.ne("") & role_text.ne("main")
    merged.loc[non_main_line, MENU_WEIGHT_USAGE_COLUMN] = ""
    non_order_line = merged.get("line_role", pd.Series("", index=merged.index)).isin(["fee", "discount"])
    merged.loc[non_order_line, MENU_WEIGHT_USAGE_COLUMN] = ""
    drop_cols = [
        col
        for original_col in weight_material_columns
        for col in [original_col, f"{original_col}_weight_input"]
        if col in merged.columns
    ]
    drop_cols.extend([col for col in ["사이즈키", "닭유형키", "추가재료키"] if col in merged.columns])
    merged = merged.drop(columns=drop_cols)
    return merged.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")


def _attach_option_material_usage_columns(left_joined: pd.DataFrame) -> pd.DataFrame:
    out = left_joined.copy()
    out[OPTION_MATERIAL_USAGE_COLUMN] = ""
    option_attrs = _option_material_attrs()
    option_material_columns = [
        col for col in option_attrs.columns if _is_material_usage_manual_column(col)
    ]
    if out.empty or option_attrs.empty or not option_material_columns:
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")
    merged = out.merge(
        option_attrs.reindex(columns=[*OPTION_MATERIAL_INPUT_KEY_COLUMNS, *option_material_columns], fill_value=""),
        on=OPTION_MATERIAL_INPUT_KEY_COLUMNS,
        how="left",
        suffixes=("", "_option_input"),
    ).fillna("")
    material_source_cols = [
        (col, f"{col}_option_input" if f"{col}_option_input" in merged.columns else col)
        for col in option_material_columns
    ]
    is_option_line = merged.get("line_role", pd.Series("", index=merged.index)).eq("option")
    merged.loc[is_option_line, OPTION_MATERIAL_USAGE_COLUMN] = merged.loc[is_option_line].apply(
        lambda row: _combine_material_usage(row, material_source_cols),
        axis=1,
    )
    non_option_line = ~is_option_line
    merged.loc[non_option_line, OPTION_MATERIAL_USAGE_COLUMN] = ""
    drop_cols = [
        col
        for original_col in option_material_columns
        for col in [original_col, f"{original_col}_option_input"]
        if col in merged.columns
    ]
    merged = merged.drop(columns=drop_cols)
    return merged.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")


def _attach_chicken_addon_columns(left_joined: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_order_group_columns(left_joined)
    for col in [CHICKEN_ADDON_USAGE_COLUMN, CHICKEN_ADDON_BONE_COLUMN, CHICKEN_ADDON_BONELESS_COLUMN, CHICKEN_ADDON_REASON_COLUMN]:
        out[col] = ""
    if out.empty or "option_kind" not in out.columns:
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")

    option_attrs = _option_kind_master_attrs()
    manual_addon: dict[tuple[str, str, str, str, str], tuple[str, str]] = {}
    if not option_attrs.empty:
        for _, row in option_attrs.iterrows():
            key = tuple(str(row.get(col, "") or "").strip() for col in OPTION_KIND_MASTER_KEY_COLUMNS)
            manual_addon[key] = (
                str(row.get("닭가산_manual", "") or "").strip(),
                str(row.get("닭가산유형_manual", "") or "").strip(),
            )
    conversions = _chicken_conversion_attrs()
    work = out.fillna("").copy()
    role = work.get("line_role", pd.Series("", index=work.index)).astype(str).str.strip()
    kind = work.get("option_kind", pd.Series("", index=work.index)).astype(str).str.strip()
    addon_lines = work[role.eq("option") & kind.eq(OPTION_KIND_CHICKEN_ADDON)].copy()
    if addon_lines.empty:
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")

    main_rows = work[role.eq("main")].copy()
    parent_profiles: dict[tuple[str, str, str, str, str], tuple[str, float | None]] = {}
    for key, group in main_rows.groupby(ORDER_GROUP_COLUMNS, dropna=False, sort=False):
        row = group.iloc[0]
        parent_profiles[tuple(str(value) for value in key)] = (
            str(row.get("닭유형", "") or "").strip(),
            _parse_bone_ratio(row.get(CHICKEN_RATIO_APPLIED_COLUMN, "")),
        )

    addon_totals: dict[tuple[str, str, str, str, str], dict[str, float | list[str]]] = {}
    line_values: dict[int, tuple[float, float, float, str]] = {}
    for idx, row in addon_lines.iterrows():
        key = tuple(str(row.get(col, "") or "").strip() for col in OPTION_KIND_MASTER_KEY_COLUMNS)
        manual_amount, manual_type = manual_addon.get(key, ("", ""))
        amount = pd.to_numeric(pd.Series([manual_amount]), errors="coerce").iloc[0]
        reason = ""
        item_name = str(row.get("item_name", "") or "")
        group_key = tuple(str(row.get(col, "") or "").strip() for col in ORDER_GROUP_COLUMNS)
        parent_type, parent_ratio = parent_profiles.get(group_key, ("", None))
        if pd.isna(amount):
            if "1인추가" in re.sub(r"\s+", "", item_name) or _looks_like_one_serving_addon_option(item_name):
                amount = 0.5 if parent_type and parent_type != CHICKEN_TYPE_NONE else 0.0
            else:
                grams = _extract_chicken_addon_grams(item_name)
                addon_type_for_conversion = _infer_chicken_addon_type(item_name) or _normalize_chicken_type(manual_type)
                conversion_key = "순살_1마리_g" if addon_type_for_conversion == "순살" else "뼈닭_1마리_g"
                if grams is not None and conversions.get(conversion_key):
                    amount = grams / conversions[conversion_key]
                else:
                    amount = 0.0
                    reason = "닭가산미입력"
        amount = float(amount)
        qty = pd.to_numeric(pd.Series([row.get("qty", "")]), errors="coerce").fillna(0).iloc[0]
        total = float(qty) * amount
        addon_type = _normalize_chicken_type(manual_type) or _infer_chicken_addon_type(item_name) or parent_type
        bone = 0.0
        boneless = 0.0
        if addon_type == "뼈닭":
            bone = total
        elif addon_type == "순살":
            boneless = total
        elif addon_type == CHICKEN_TYPE_MIXED:
            ratio = parent_ratio if parent_ratio is not None else 0.5
            bone = total * ratio
            boneless = total * (1.0 - ratio)
        elif total:
            reason = reason or "닭가산유형미입력"
        line_values[idx] = (total, bone, boneless, reason)
        bucket = addon_totals.setdefault(group_key, {"total": 0.0, "bone": 0.0, "boneless": 0.0, "reasons": []})
        bucket["total"] = float(bucket["total"]) + total
        bucket["bone"] = float(bucket["bone"]) + bone
        bucket["boneless"] = float(bucket["boneless"]) + boneless
        if reason:
            cast_reasons = bucket["reasons"]
            assert isinstance(cast_reasons, list)
            cast_reasons.append(reason)

    for idx, (total, bone, boneless, reason) in line_values.items():
        out.at[idx, CHICKEN_ADDON_USAGE_COLUMN] = _format_number(total) if total else ""
        out.at[idx, CHICKEN_ADDON_BONE_COLUMN] = _format_number(bone) if bone else ""
        out.at[idx, CHICKEN_ADDON_BONELESS_COLUMN] = _format_number(boneless) if boneless else ""
        out.at[idx, CHICKEN_ADDON_REASON_COLUMN] = reason

    for key, totals in addon_totals.items():
        mask = role.eq("main")
        for col, value in zip(ORDER_GROUP_COLUMNS, key):
            mask &= work.get(col, pd.Series("", index=work.index)).astype(str).str.strip().eq(value)
        if not mask.any():
            continue
        idx = mask[mask].index[0]
        base_total = pd.to_numeric(pd.Series([out.at[idx, CHICKEN_USAGE_TOTAL_COLUMN]]), errors="coerce").fillna(0).iloc[0]
        base_bone = pd.to_numeric(pd.Series([out.at[idx, BONE_USAGE_TOTAL_COLUMN]]), errors="coerce").fillna(0).iloc[0]
        base_boneless = pd.to_numeric(pd.Series([out.at[idx, BONELESS_USAGE_TOTAL_COLUMN]]), errors="coerce").fillna(0).iloc[0]
        out.at[idx, CHICKEN_ADDON_USAGE_COLUMN] = _format_number(totals["total"])
        out.at[idx, CHICKEN_ADDON_BONE_COLUMN] = _format_number(totals["bone"])
        out.at[idx, CHICKEN_ADDON_BONELESS_COLUMN] = _format_number(totals["boneless"])
        out.at[idx, CHICKEN_USAGE_TOTAL_COLUMN] = _format_number(base_total + float(totals["total"]))
        out.at[idx, BONE_USAGE_TOTAL_COLUMN] = _format_number(base_bone + float(totals["bone"]))
        out.at[idx, BONELESS_USAGE_TOTAL_COLUMN] = _format_number(base_boneless + float(totals["boneless"]))
        reasons = totals["reasons"]
        if isinstance(reasons, list) and reasons:
            out.at[idx, CHICKEN_ADDON_REASON_COLUMN] = " | ".join(_unique_nonempty([str(value) for value in reasons]))

    return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")


def _order_exception_attrs() -> pd.DataFrame:
    frame = _read_manual_workbook_sheet(ORDER_EXCEPTION_SHEET_NAME).fillna("")
    if frame.empty:
        return pd.DataFrame(columns=ORDER_EXCEPTION_COLUMNS)
    for col in ORDER_EXCEPTION_COLUMNS:
        if col not in frame.columns:
            frame[col] = ""
        frame[col] = frame[col].fillna("").astype(str).str.strip()
    return frame.reindex(columns=ORDER_EXCEPTION_COLUMNS, fill_value="").drop_duplicates(
        subset=["source", "sale_date", "order_id", "자동판정"],
        keep="last",
    )


def _auto_order_exception_type(group: pd.DataFrame) -> str:
    sale_type = group.get("sale_type", pd.Series("", index=group.index)).astype(str).str.strip()
    if sale_type.eq("취소").any():
        return "취소"
    total = pd.to_numeric(group.get("total_price", pd.Series("", index=group.index)), errors="coerce").fillna(0).sum()
    discount = pd.to_numeric(group.get("discount_amount", pd.Series("", index=group.index)), errors="coerce").fillna(0).sum()
    if total == 0 and discount > 0:
        return "전액할인"
    role = group.get("line_role", pd.Series("", index=group.index)).astype(str).str.strip()
    if role.eq("main").sum() == 0:
        return "main없음"
    menu_main = role.eq("main").groupby(group.get("menu_seq", pd.Series("", index=group.index)), dropna=False).sum()
    if menu_main.gt(1).any():
        return "menu_seq중복main"
    return ""


def _build_order_exception_input(left_joined: pd.DataFrame) -> pd.DataFrame:
    existing = _order_exception_attrs()
    existing_map = {
        tuple(str(row.get(col, "") or "").strip() for col in ["source", "sale_date", "order_id", "자동판정"]): row.to_dict()
        for _, row in existing.iterrows()
    } if not existing.empty else {}
    if left_joined.empty:
        return existing.reindex(columns=ORDER_EXCEPTION_COLUMNS, fill_value="")
    rows = []
    work = left_joined.fillna("").copy()
    for (source, sale_date, order_id), group in work.groupby(["source", "sale_date", "order_id"], dropna=False, sort=False):
        auto_type = _auto_order_exception_type(group)
        if not auto_type:
            continue
        key = (str(source), str(sale_date), str(order_id), auto_type)
        current = existing_map.get(key, {})
        main = group[group.get("line_role", pd.Series("", index=group.index)).astype(str).str.strip().eq("main")]
        menu_name = _first_clean_value(main.get("menu_name", pd.Series(dtype=str))) or _first_clean_value(group.get("menu_name", pd.Series(dtype=str)))
        total = pd.to_numeric(group.get("total_price", ""), errors="coerce").fillna(0).sum()
        discount = pd.to_numeric(group.get("discount_amount", ""), errors="coerce").fillna(0).sum()
        chicken = pd.to_numeric(group.get(CHICKEN_USAGE_TOTAL_COLUMN, ""), errors="coerce").fillna(0).sum()
        rows.append(
            {
                "source": source,
                "order_id": order_id,
                "sale_date": _first_clean_value(group.get("sale_date", pd.Series(dtype=str))),
                "menu_name": menu_name,
                "매출": _format_number(total),
                "할인액": _format_number(discount),
                "닭사용량": _format_number(chicken),
                "자동판정": auto_type,
                "구분_manual": str(current.get("구분_manual", "") or "").strip(),
                "닭계상_manual": str(current.get("닭계상_manual", "") or "").strip(),
                "메모": str(current.get("메모", "") or "").strip(),
            }
        )
    output = pd.DataFrame(rows, columns=ORDER_EXCEPTION_COLUMNS)
    if existing.empty:
        return output
    new_keys = {
        tuple(str(row.get(col, "") or "").strip() for col in ["source", "sale_date", "order_id", "자동판정"])
        for _, row in output.iterrows()
    }
    keep = existing[
        ~existing.apply(
            lambda row: tuple(str(row.get(col, "") or "").strip() for col in ["source", "sale_date", "order_id", "자동판정"]) in new_keys,
            axis=1,
        )
    ]
    return pd.concat([output, keep], ignore_index=True, sort=False).reindex(columns=ORDER_EXCEPTION_COLUMNS, fill_value="")


def _apply_order_exception_policy(left_joined: pd.DataFrame, order_exception_input: pd.DataFrame | None = None) -> pd.DataFrame:
    out = left_joined.copy()
    for col in [ORDER_EXCEPTION_TYPE_COLUMN, ADJUSTED_SALES_COLUMN, PRE_DISCOUNT_PRICE_COLUMN]:
        out[col] = ""
    if out.empty:
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")
    manual = _order_exception_attrs() if order_exception_input is None else order_exception_input.fillna("")
    manual_map = {}
    if not manual.empty:
        for _, row in manual.iterrows():
            manual_map[(
                str(row.get("source", "") or "").strip(),
                str(row.get("sale_date", "") or "").strip(),
                str(row.get("order_id", "") or "").strip(),
                str(row.get("자동판정", "") or "").strip(),
            )] = (
                str(row.get("구분_manual", "") or "").strip(),
                str(row.get("닭계상_manual", "") or "").strip().upper(),
            )
    work = out.fillna("").copy()
    for (source, sale_date, order_id), group in work.groupby(["source", "sale_date", "order_id"], dropna=False, sort=False):
        auto_type = _auto_order_exception_type(group)
        if not auto_type:
            continue
        manual_type, manual_count = manual_map.get((str(source), str(sale_date), str(order_id), auto_type), ("", ""))
        exception_type = manual_type or auto_type
        count_chicken = manual_count
        if not count_chicken:
            count_chicken = "N" if exception_type in {"취소", "테스트"} else "Y"
        idx = group.index
        out.loc[idx, ORDER_EXCEPTION_TYPE_COLUMN] = exception_type
        total = pd.to_numeric(group.get("total_price", ""), errors="coerce").fillna(0).sum()
        discount = pd.to_numeric(group.get("discount_amount", ""), errors="coerce").fillna(0).sum()
        if exception_type in SALESLESS_COUNTED_EXCEPTION_TYPES:
            line_discount = pd.to_numeric(group.get("discount_amount", ""), errors="coerce").fillna(0)
            out.loc[idx, ADJUSTED_SALES_COLUMN] = "0"
            line_total = pd.to_numeric(group.get("total_price", ""), errors="coerce").fillna(0)
            reference_price = line_discount.where(line_discount.gt(0), line_total.abs())
            out.loc[idx, PRE_DISCOUNT_PRICE_COLUMN] = reference_price.map(_format_number)
        if count_chicken == "N":
            for col in [
                CHICKEN_USAGE_TOTAL_COLUMN,
                BONE_USAGE_TOTAL_COLUMN,
                BONELESS_USAGE_TOTAL_COLUMN,
                CHICKEN_ADDON_USAGE_COLUMN,
                CHICKEN_ADDON_BONE_COLUMN,
                CHICKEN_ADDON_BONELESS_COLUMN,
            ]:
                if col in out.columns:
                    out.loc[idx, col] = "0"
        elif count_chicken == "Y":
            role = group.get("line_role", pd.Series("", index=group.index)).astype(str).str.strip()
            qty = pd.to_numeric(group.get("qty", pd.Series("", index=group.index)), errors="coerce").fillna(0)
            usage = pd.to_numeric(group.get(CHICKEN_USAGE_COLUMN, pd.Series("", index=group.index)), errors="coerce").fillna(0)
            usage_total = pd.to_numeric(group.get(CHICKEN_USAGE_TOTAL_COLUMN, pd.Series("", index=group.index)), errors="coerce").fillna(0)
            repair = role.eq("main") & qty.le(0) & usage.gt(0) & usage_total.eq(0)
            for row_idx in group.index[repair]:
                row_usage = float(usage.at[row_idx])
                out.at[row_idx, CHICKEN_USAGE_TOTAL_COLUMN] = _format_number(row_usage)
                chicken_type = str(out.at[row_idx, "닭유형"] if "닭유형" in out.columns else "").strip()
                if chicken_type == "뼈닭":
                    out.at[row_idx, BONE_USAGE_TOTAL_COLUMN] = _format_number(row_usage)
                    out.at[row_idx, BONELESS_USAGE_TOTAL_COLUMN] = "0"
                elif chicken_type == "순살":
                    out.at[row_idx, BONE_USAGE_TOTAL_COLUMN] = "0"
                    out.at[row_idx, BONELESS_USAGE_TOTAL_COLUMN] = _format_number(row_usage)
                elif chicken_type == CHICKEN_TYPE_MIXED:
                    ratio = _parse_bone_ratio(out.at[row_idx, CHICKEN_RATIO_APPLIED_COLUMN] if CHICKEN_RATIO_APPLIED_COLUMN in out.columns else "")
                    if ratio is None:
                        ratio = 0.5
                    out.at[row_idx, BONE_USAGE_TOTAL_COLUMN] = _format_number(row_usage * ratio)
                    out.at[row_idx, BONELESS_USAGE_TOTAL_COLUMN] = _format_number(row_usage * (1.0 - ratio))
    return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")


def _cancel_offset_excluded(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(False, index=frame.index)
    status = frame.get(CANCEL_OFFSET_STATUS_COLUMN, pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
    exception_type = frame.get(ORDER_EXCEPTION_TYPE_COLUMN, pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
    return status.isin({CANCEL_OFFSET_NORMAL_STATUS, CANCEL_OFFSET_CANCEL_STATUS, CANCEL_OFFSET_UNMATCHED_STATUS}) & ~exception_type.isin(
        SALESLESS_COUNTED_EXCEPTION_TYPES
    )


def _profit_calculation_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    return frame.loc[~_cancel_offset_excluded(frame)].copy()


def _pair_cancel_with_nearest_normal(
    cancel: pd.DataFrame,
    positive: pd.DataFrame,
    match_cols: list[str],
) -> pd.DataFrame:
    """취소 주문을 시각이 가장 가까운 정상 주문에 붙인다.

    OKPOS는 취소를 원주문 직후 순번으로 찍는다. 그래서 같은 금액 후보가 여럿이면
    "취소 직전"이 진짜 짝이다. 순서대로 짝짓던 예전 방식은 그날 가장 이른 주문을
    골라서, 점심 주문이 저녁 취소와 묶이는 일이 있었다.
    """
    # match_cols에 이미 source/store/sale_date가 들어 있다. 중복 컬럼을 만들면
    # row.get("source")가 스칼라가 아니라 Series를 돌려줘서 뒷단이 깨진다.
    extra = [col for col in ("source", "store", "sale_date") if col not in match_cols]
    columns = [*match_cols, *extra, "order_id_cancel", "order_id_normal"]
    empty = pd.DataFrame(columns=columns)
    if cancel.empty or positive.empty:
        return empty

    positive_by_group: dict[tuple[str, ...], list[tuple[str, str, pd.Series]]] = {}
    for _, row in positive.iterrows():
        key = tuple(str(row.get(col) or "") for col in match_cols)
        positive_by_group.setdefault(key, []).append(
            (str(row.get("_sort_time") or ""), str(row.get("_sort_id") or ""), row)
        )

    used: set[tuple[tuple[str, ...], int]] = set()
    records: list[dict[str, object]] = []
    for _, row in cancel.iterrows():
        key = tuple(str(row.get(col) or "") for col in match_cols)
        candidates = positive_by_group.get(key, [])
        cancel_sort = (str(row.get("_sort_time") or ""), str(row.get("_sort_id") or ""))
        before: tuple[int, tuple[str, str]] | None = None
        after: tuple[int, tuple[str, str]] | None = None
        for idx, (time_text, id_text, _) in enumerate(candidates):
            if (key, idx) in used:
                continue
            candidate_sort = (time_text, id_text)
            if candidate_sort <= cancel_sort:
                if before is None or candidate_sort > before[1]:
                    before = (idx, candidate_sort)
            elif after is None or candidate_sort < after[1]:
                after = (idx, candidate_sort)
        chosen = before or after
        record: dict[str, object] = {col: str(row.get(col) or "") for col in columns[:-2]}
        record["order_id_cancel"] = str(row.get("order_id") or "")
        record["order_id_normal"] = ""
        if chosen is not None:
            used.add((key, chosen[0]))
            record["order_id_normal"] = str(candidates[chosen[0]][2].get("order_id") or "")
        records.append(record)
    return pd.DataFrame(records, columns=columns) if records else empty


def _attach_cancel_offset_columns(left_joined: pd.DataFrame) -> pd.DataFrame:
    """같은 일자/채널/결제금액의 정상-취소 주문을 원가율 제외 묶음으로 표시한다."""
    out = _attach_profit_channel(left_joined).fillna("")
    out[CANCEL_OFFSET_GROUP_COLUMN] = ""
    out[CANCEL_OFFSET_STATUS_COLUMN] = CANCEL_OFFSET_NONE_STATUS
    out[CANCEL_OFFSET_CANDIDATES_COLUMN] = ""
    if out.empty:
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")

    work = out.copy()
    order_cols = ["source", "store", "sale_date", "order_id"]
    for col in [*order_cols, PROFIT_CHANNEL_COLUMN, "order_time", "sale_type"]:
        if col not in work.columns:
            work[col] = ""
    work["_line_total_num"] = pd.to_numeric(work.get("total_price", pd.Series("", index=work.index)), errors="coerce").fillna(0)
    order_summary = (
        work.groupby(order_cols, dropna=False, sort=False)
        .agg(
            수익채널=(PROFIT_CHANNEL_COLUMN, _first_clean_value),
            order_time=("order_time", _first_clean_value),
            order_total=("_line_total_num", "sum"),
            has_cancel=("sale_type", lambda s: s.astype(str).str.strip().eq("취소").any()),
        )
        .reset_index()
    )
    if order_summary.empty:
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")

    order_summary["order_total"] = pd.to_numeric(order_summary["order_total"], errors="coerce").fillna(0)
    order_summary["abs_total"] = order_summary["order_total"].abs()
    positive = order_summary[order_summary["order_total"].gt(0)].copy()
    cancel = order_summary[order_summary["has_cancel"] | order_summary["order_total"].lt(0)].copy()
    cancel = cancel[cancel["abs_total"].gt(0)].copy()
    if positive.empty or cancel.empty:
        if not cancel.empty:
            cancel_keys = set(zip(cancel["source"], cancel["store"], cancel["sale_date"], cancel["order_id"]))
            cancel_mask = list(zip(out["source"], out["store"], out["sale_date"], out["order_id"]))
            out.loc[[key in cancel_keys for key in cancel_mask], CANCEL_OFFSET_STATUS_COLUMN] = CANCEL_OFFSET_UNMATCHED_STATUS
        return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")

    match_cols = ["source", "store", "sale_date", "수익채널", "abs_total"]
    for col in match_cols:
        positive[col] = positive[col].astype(str)
        cancel[col] = cancel[col].astype(str)
    positive["_sort_time"] = positive["order_time"].astype(str)
    positive["_sort_id"] = positive["order_id"].astype(str)
    cancel["_sort_time"] = cancel["order_time"].astype(str)
    cancel["_sort_id"] = cancel["order_id"].astype(str)
    positive = positive.sort_values([*match_cols, "_sort_time", "_sort_id"]).copy()
    cancel = cancel.sort_values([*match_cols, "_sort_time", "_sort_id"]).copy()
    candidate_counts = positive.groupby(match_cols, dropna=False).size().to_dict()
    pairs = _pair_cancel_with_nearest_normal(cancel, positive, match_cols)
    row_key = list(zip(out["source"], out["store"], out["sale_date"], out["order_id"]))
    row_key_index = pd.Series(row_key, index=out.index)
    matched_normal_keys: set[tuple[str, str, str, str]] = set()
    matched_cancel_keys: set[tuple[str, str, str, str]] = set()
    unmatched_cancel_keys: set[tuple[str, str, str, str]] = set()
    group_by_order: dict[tuple[str, str, str, str], str] = {}
    candidates_by_order: dict[tuple[str, str, str, str], str] = {}

    for _, row in pairs.iterrows():
        cancel_key = (
            str(row.get("source") or ""),
            str(row.get("store") or ""),
            str(row.get("sale_date") or ""),
            str(row.get("order_id_cancel") or ""),
        )
        candidates = int(candidate_counts.get(tuple(str(row.get(col) or "") for col in match_cols), 0))
        candidates_by_order[cancel_key] = str(candidates)
        raw_normal_id = row.get("order_id_normal")
        normal_id = "" if pd.isna(raw_normal_id) else str(raw_normal_id or "").strip()
        normal_store = str(row.get("store") or "").strip()
        if normal_id and normal_store:
            normal_key = (
                str(row.get("source") or ""),
                normal_store,
                str(row.get("sale_date") or ""),
                normal_id,
            )
            group_id = "CANCEL_OFFSET_" + hashlib.md5(
                "|".join([*cancel_key, normal_id, str(row.get("abs_total") or "")]).encode("utf-8")
            ).hexdigest()[:12]
            matched_cancel_keys.add(cancel_key)
            matched_normal_keys.add(normal_key)
            group_by_order[cancel_key] = group_id
            group_by_order[normal_key] = group_id
            candidates_by_order[normal_key] = str(candidates)
        else:
            unmatched_cancel_keys.add(cancel_key)

    if matched_normal_keys:
        out.loc[row_key_index.isin(matched_normal_keys), CANCEL_OFFSET_STATUS_COLUMN] = CANCEL_OFFSET_NORMAL_STATUS
    if matched_cancel_keys:
        out.loc[row_key_index.isin(matched_cancel_keys), CANCEL_OFFSET_STATUS_COLUMN] = CANCEL_OFFSET_CANCEL_STATUS
    if unmatched_cancel_keys:
        out.loc[row_key_index.isin(unmatched_cancel_keys), CANCEL_OFFSET_STATUS_COLUMN] = CANCEL_OFFSET_UNMATCHED_STATUS
    for key, group_id in group_by_order.items():
        out.loc[row_key_index.map(lambda value, expected=key: value == expected), CANCEL_OFFSET_GROUP_COLUMN] = group_id
    for key, candidates in candidates_by_order.items():
        out.loc[row_key_index.map(lambda value, expected=key: value == expected), CANCEL_OFFSET_CANDIDATES_COLUMN] = candidates
    return out.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value="")


def _build_menu_weight_input(left_joined: pd.DataFrame) -> pd.DataFrame:
    edits = _menu_weight_attrs()
    extra_manual_columns = [
        col for col in edits.columns if _is_material_usage_manual_column(col)
    ]
    output_columns = _menu_weight_input_columns(extra_manual_columns)
    edit_columns = _menu_weight_edit_columns(output_columns)
    groups = _build_menu_weight_group_attrs(_profit_calculation_frame(left_joined))
    if groups.empty:
        base = pd.DataFrame(columns=output_columns)
    else:
        grouped = (
            groups.groupby(MENU_WEIGHT_INPUT_KEY_COLUMNS, dropna=False, sort=False)
            .agg(
                주문건수=("주문건수", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                판매수량=("판매수량", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                매출합계=("매출합계", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
            )
            .reset_index()
        )
        for col in ["주문건수", "판매수량", "매출합계"]:
            grouped[col] = pd.to_numeric(grouped[col], errors="coerce").fillna(0).round(2)
            grouped[col] = grouped[col].map(lambda value: f"{value:g}")
        base = grouped.reindex(columns=output_columns, fill_value="")

    if not edits.empty:
        base = base.merge(
            edits.reindex(columns=[*MENU_WEIGHT_INPUT_KEY_COLUMNS, *edit_columns], fill_value=""),
            on=MENU_WEIGHT_INPUT_KEY_COLUMNS,
            how="left",
            suffixes=("", "_existing"),
        ).fillna("")
        for col in edit_columns:
            existing_col = f"{col}_existing"
            if existing_col in base.columns:
                base[col] = base[existing_col].astype(str).str.strip().where(
                    base[existing_col].astype(str).str.strip().ne(""),
                    base.get(col, pd.Series("", index=base.index)).astype(str).str.strip(),
                )
        base = base.drop(columns=[col for col in base.columns if col.endswith("_existing")])
        base_keys = {
            tuple(str(row.get(col, "") or "").strip() for col in MENU_WEIGHT_INPUT_KEY_COLUMNS)
            for _, row in base.iterrows()
        }
        preserved = edits[
            edits.apply(
                lambda row: (
                    tuple(str(row.get(col, "") or "").strip() for col in MENU_WEIGHT_INPUT_KEY_COLUMNS) not in base_keys
                    and any(str(row.get(col, "") or "").strip() for col in edit_columns)
                ),
                axis=1,
            )
        ]
        if not preserved.empty:
            base = pd.concat(
                [base, preserved.reindex(columns=output_columns, fill_value="")],
                ignore_index=True,
                sort=False,
            )

    return base.reindex(columns=output_columns, fill_value="")


def _build_material_usage_summary(menu_weight_input: pd.DataFrame) -> pd.DataFrame:
    if menu_weight_input.empty:
        return pd.DataFrame(columns=MATERIAL_USAGE_SUMMARY_COLUMNS)
    material_columns = [
        col for col in menu_weight_input.columns if _is_material_usage_manual_column(col)
    ]
    if not material_columns:
        return pd.DataFrame(columns=MATERIAL_USAGE_SUMMARY_COLUMNS)

    rows = []
    for _, row in menu_weight_input.fillna("").iterrows():
        qty = pd.to_numeric(pd.Series([row.get("판매수량", "")]), errors="coerce").fillna(0).iloc[0]
        order_count = pd.to_numeric(pd.Series([row.get("주문건수", "")]), errors="coerce").fillna(0).iloc[0]
        revenue = pd.to_numeric(pd.Series([row.get("매출합계", "")]), errors="coerce").fillna(0).iloc[0]
        for col in material_columns:
            usage_per_menu = pd.to_numeric(pd.Series([row.get(col, "")]), errors="coerce").fillna(0).iloc[0]
            if usage_per_menu <= 0:
                continue
            rows.append({
                "source": row.get("source", ""),
                "brand": row.get("brand", ""),
                "store": row.get("store", ""),
                "std_menu_name": row.get("std_menu_name", ""),
                "사이즈키": row.get("사이즈키", ""),
                "닭유형키": row.get("닭유형키", ""),
                "추가재료키": row.get("추가재료키", ""),
                "재료명": _material_name_from_usage_column(col),
                "주문건수": _format_number(order_count),
                "판매수량": _format_number(qty),
                "메뉴당사용량": _format_number(usage_per_menu),
                "예상사용량": _format_number(float(qty) * float(usage_per_menu)),
                "매출합계": _format_number(revenue),
            })
    return pd.DataFrame(rows, columns=MATERIAL_USAGE_SUMMARY_COLUMNS)


def _build_manager_input(
    left_joined: pd.DataFrame,
    group_attrs: pd.DataFrame | None = None,
) -> pd.DataFrame:
    edits = _normalize_option_combo_key_frame(_manager_input_attrs())
    extra_manual_columns = [
        col
        for col in edits.columns
        if col not in _MANAGER_INPUT_EDIT_COLUMNS and _is_material_usage_manual_column(col)
    ]
    output_columns = _manager_input_columns(extra_manual_columns)
    edit_columns = _manager_edit_columns(output_columns)
    if left_joined.empty:
        base = pd.DataFrame(columns=output_columns)
    else:
        groups = _build_order_group_attrs(left_joined) if group_attrs is None else group_attrs
        if groups.empty:
            base = pd.DataFrame(columns=output_columns)
        else:
            grouped = (
                groups.groupby(MANAGER_INPUT_KEY_COLUMNS, dropna=False, sort=False)
                .agg(
                    대표메뉴명=("대표메뉴명", _first_clean_value),
                    주문건수=("주문건수", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                    판매수량=("판매수량", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                    매출합계=("매출합계", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                )
                .reset_index()
            )
            for col in ["주문건수", "판매수량", "매출합계"]:
                grouped[col] = pd.to_numeric(grouped[col], errors="coerce").fillna(0).round(2)
                grouped[col] = grouped[col].map(lambda value: f"{value:g}")
            default_grouped = (
                groups.groupby(["source", "brand", "store", "std_menu_name"], dropna=False, sort=False)
                .agg(
                    대표메뉴명=("대표메뉴명", _first_clean_value),
                    주문건수=("주문건수", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                    판매수량=("판매수량", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                    매출합계=("매출합계", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()),
                )
                .reset_index()
            )
            default_grouped[CHICKEN_OPTION_KEY_COLUMN] = MANAGER_DEFAULT_OPTION_COMBO
            for col in ["주문건수", "판매수량", "매출합계"]:
                default_grouped[col] = pd.to_numeric(default_grouped[col], errors="coerce").fillna(0).round(2)
                default_grouped[col] = default_grouped[col].map(lambda value: f"{value:g}")
            base = pd.concat([grouped, default_grouped], ignore_index=True)
            base = _normalize_option_combo_key_frame(base).drop_duplicates(subset=MANAGER_INPUT_KEY_COLUMNS, keep="first")
            base = base.reindex(columns=output_columns, fill_value="")

    if not edits.empty:
        base = base.merge(
            edits.reindex(columns=[*MANAGER_INPUT_KEY_COLUMNS, *edit_columns], fill_value=""),
            on=MANAGER_INPUT_KEY_COLUMNS,
            how="left",
            suffixes=("", "_existing"),
        ).fillna("")
        for col in edit_columns:
            existing_col = f"{col}_existing"
            if existing_col in base.columns:
                base[col] = base[existing_col].astype(str).str.strip().where(
                    base[existing_col].astype(str).str.strip().ne(""),
                    base.get(col, pd.Series("", index=base.index)).astype(str).str.strip(),
                )
        base = base.drop(columns=[col for col in base.columns if col.endswith("_existing")])
        base_keys = {
            tuple(str(row.get(col, "") or "").strip() for col in MANAGER_INPUT_KEY_COLUMNS)
            for _, row in base.iterrows()
        }
        preserved = edits[
            edits.apply(
                lambda row: (
                    tuple(str(row.get(col, "") or "").strip() for col in MANAGER_INPUT_KEY_COLUMNS) not in base_keys
                    and any(str(row.get(col, "") or "").strip() for col in edit_columns)
                ),
                axis=1,
            )
        ]
        if not preserved.empty:
            base = pd.concat(
                [base, preserved.reindex(columns=output_columns, fill_value="")],
                ignore_index=True,
                sort=False,
            )

    return base.reindex(columns=output_columns, fill_value="")


def _seed_option_material_columns(left_joined: pd.DataFrame) -> list[str]:
    """실제 등장하는 재료추가 옵션에서 *_manual 컬럼을 미리 만들어 둔다.

    16번에 *_manual 컬럼이 0개였던 이유는 담당자가 컬럼부터 만들어야 하는 설계였기
    때문이다. 컬럼을 깔아두면 값만 채우면 된다.
    """
    if left_joined.empty or "option_kind" not in left_joined.columns:
        return []
    material_lines = left_joined[left_joined["option_kind"].astype(str).str.strip().eq(OPTION_KIND_MATERIAL)]
    if material_lines.empty:
        return []
    names = material_lines.get("재료명", pd.Series("", index=material_lines.index)).fillna("").astype(str).str.strip()
    counts = names[names.ne("")].value_counts()
    return [f"{name}사용량_manual" for name in counts.index.tolist()]


def _build_option_material_input(left_joined: pd.DataFrame) -> pd.DataFrame:
    edits = _option_material_attrs()
    extra_manual_columns = _unique_nonempty(
        [col for col in edits.columns if _is_material_usage_manual_column(col)]
        + _seed_option_material_columns(left_joined)
    )
    output_columns = _option_material_input_columns(extra_manual_columns)
    edit_columns = _option_material_edit_columns(output_columns)
    if left_joined.empty:
        base = pd.DataFrame(columns=output_columns)
    else:
        work = _profit_calculation_frame(left_joined).fillna("")
        if "line_role" not in work.columns:
            base = pd.DataFrame(columns=output_columns)
        else:
            option_lines = work[work["line_role"].eq("option")].copy()
            option_lines = option_lines[~option_lines["item_name"].astype(str).str.contains(_FEE_LIKE_RE, na=False)]
            if option_lines.empty:
                base = pd.DataFrame(columns=output_columns)
            else:
                for col in ["qty", "total_price"]:
                    option_lines[f"{col}_num"] = pd.to_numeric(option_lines.get(col, pd.Series("", index=option_lines.index)), errors="coerce").fillna(0)
                grouped = (
                    option_lines.groupby(OPTION_MATERIAL_INPUT_KEY_COLUMNS, dropna=False, sort=False)
                    .agg(
                        std_menu_name=("std_menu_name", _first_clean_value),
                        대표메뉴명=("menu_name", _first_clean_value),
                        line_role=("line_role", _first_clean_value),
                        주문건수=("order_id", "nunique"),
                        판매수량=("qty_num", "sum"),
                        매출합계=("total_price_num", "sum"),
                    )
                    .reset_index()
                )
                for col in ["주문건수", "판매수량", "매출합계"]:
                    grouped[col] = pd.to_numeric(grouped[col], errors="coerce").fillna(0).round(2)
                    grouped[col] = grouped[col].map(lambda value: f"{value:g}")
                base = grouped.reindex(columns=output_columns, fill_value="")

    if not edits.empty:
        base = base.merge(
            edits.reindex(columns=[*OPTION_MATERIAL_INPUT_KEY_COLUMNS, *edit_columns], fill_value=""),
            on=OPTION_MATERIAL_INPUT_KEY_COLUMNS,
            how="left",
            suffixes=("", "_existing"),
        ).fillna("")
        for col in edit_columns:
            existing_col = f"{col}_existing"
            if existing_col in base.columns:
                base[col] = base[existing_col].astype(str).str.strip().where(
                    base[existing_col].astype(str).str.strip().ne(""),
                    base.get(col, pd.Series("", index=base.index)).astype(str).str.strip(),
                )
        base = base.drop(columns=[col for col in base.columns if col.endswith("_existing")])

    return base.reindex(columns=output_columns, fill_value="")


def _validation_text(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series("", index=df.index)
    return df[column].fillna("").astype(str).str.strip()


def _validation_number(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(0, index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").fillna(0)


def _validation_row_issue(
    row: pd.Series,
    issue_type: str,
    detail: str,
    *,
    severity: str = "ERROR",
    rows: int = 1,
    orders: int = 1,
    sales: object = "",
) -> dict[str, object]:
    return {
        "issue_type": issue_type,
        "severity": severity,
        "source": str(row.get("source", "") or ""),
        "sale_date": str(row.get("sale_date", "") or ""),
        "ym": str(row.get("ym", "") or ""),
        "order_id": str(row.get("order_id", "") or ""),
        "item_seq": str(row.get("item_seq", "") or ""),
        "item_id": str(row.get("item_id", "") or ""),
        "item_name": str(row.get("item_name", "") or ""),
        "line_role": str(row.get("line_role", "") or ""),
        "std_menu_name": str(row.get("std_menu_name", "") or ""),
        "detail": detail,
        "rows": rows,
        "orders": orders,
        "sales": sales,
    }


def _menu_profile_ignores_deciding_option(row: pd.Series, option_kind: str) -> bool:
    if option_kind not in CHICKEN_DECIDING_KINDS:
        return False
    key = tuple(str(row.get(col, "") or "").strip() for col in MENU_CHICKEN_PROFILE_KEY_COLUMNS)
    profile = _cached_menu_chicken_profile_lookup().get(key)
    if profile is None:
        profile = _effective_menu_chicken_profile(pd.Series(_suggest_menu_chicken_profile(key[-1])))
    if not profile:
        return False
    allowed_types = [str(value) for value in profile.get("allowed_types", []) if str(value or "").strip()]
    allowed_sizes = [str(value) for value in profile.get("allowed_sizes", []) if str(value or "").strip()]
    default_size = str(profile.get("default_size", "") or "").strip()
    if CHICKEN_TYPE_NONE in allowed_types:
        return True
    if option_kind == OPTION_KIND_SIZE and (default_size or len(allowed_sizes) == 1):
        return True
    return option_kind == OPTION_KIND_CHICKEN_TYPE and not bool(profile.get("apply_option_type", False))


def _has_set_identity(value: object) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return bool(re.search(r"세트(?:\s|$|\])|set(?:\s|$)", text, flags=re.IGNORECASE))


def _contextual_std_menu_name(row: pd.Series) -> str:
    item_name = str(row.get("item_name", "") or "").strip()
    menu_name = str(row.get("menu_name", "") or "").strip()
    if _normalize_item_key(item_name) != _normalize_item_key("메밀 물 막국수"):
        if menu_name and _has_set_identity(menu_name) and item_name and not _has_set_identity(item_name):
            return _strip_profit_name_size_suffix(menu_name)
        return ""
    if _has_set_identity(menu_name):
        return "[점심] 메밀 물 막국수 세트"
    return "메밀 물 막국수"


def _apply_contextual_std_menu_name_corrections(left_joined: pd.DataFrame) -> pd.DataFrame:
    if left_joined.empty:
        return left_joined.reindex(columns=left_joined.columns, fill_value="")
    out = left_joined.copy()
    for col in ["line_role", "item_name", "menu_name", "std_menu_name"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("").astype(str).str.strip()
    target_main = out["line_role"].eq("main")
    corrected = out.loc[target_main].apply(_contextual_std_menu_name, axis=1)
    corrected = corrected[corrected.astype(str).str.strip().ne("")]
    if corrected.empty:
        return out
    out.loc[corrected.index, "std_menu_name"] = corrected
    if set(ORDER_GROUP_COLUMNS).issubset(out.columns):
        main_rows = out.loc[corrected.index, [*ORDER_GROUP_COLUMNS, "std_menu_name"]].copy()
        main_rows["_group_key"] = main_rows[ORDER_GROUP_COLUMNS].fillna("").astype(str).agg(tuple, axis=1)
        group_values: dict[tuple[str, ...], str] = {}
        for group_key, group in main_rows.groupby("_group_key", dropna=False, sort=False):
            values = _unique_nonempty([str(value).strip() for value in group["std_menu_name"].tolist()])
            if len(values) == 1:
                group_values[tuple(group_key)] = values[0]
        if group_values:
            group_keys = out[ORDER_GROUP_COLUMNS].fillna("").astype(str).agg(tuple, axis=1)
            propagated = pd.Series([group_values.get(key, "") for key in group_keys], index=out.index)
            out["std_menu_name"] = propagated.where(propagated.ne(""), out["std_menu_name"])
    out["std_menu_name"] = out["std_menu_name"].map(_canonical_std_menu_name)
    return out


def _validation_gap_issue(row: pd.Series, *, severity: str | None = None) -> dict[str, object]:
    role = str(row.get("추정_수동분류", "") or "")
    item_name = str(row.get("item_name", "") or "")
    blocking = role == "main" or _has_any_token(item_name, _CHICKEN_MENU_TOKENS) or "순살" in item_name or "뼈" in item_name
    return {
        "issue_type": "product_gap",
        "severity": severity or ("ERROR" if blocking else "WARN"),
        "source": str(row.get("source", "") or ""),
        "sale_date": "",
        "ym": "",
        "order_id": "",
        "item_seq": "",
        "item_id": str(row.get("item_id", "") or ""),
        "item_name": str(row.get("item_name", "") or ""),
        "line_role": role,
        "std_menu_name": "",
        "detail": str(row.get("현재상태", "") or ""),
        "rows": "",
        "orders": str(row.get("주문건수", "") or ""),
        "sales": "",
    }


def _product_gap_context_key(row: pd.Series) -> tuple[str, str, str]:
    return (
        str(row.get("source", "") or "").strip(),
        str(row.get("item_id", "") or "").strip(),
        str(row.get("item_name", "") or "").strip(),
    )


def _is_operationally_resolved_product_gap_row(row: pd.Series) -> bool:
    role = str(row.get("line_role", "") or "").strip()
    if role != "main":
        return False
    std_name = str(row.get("std_menu_name", "") or "").strip()
    item_name = str(row.get("item_name", "") or "").strip()
    if not std_name or std_name.startswith("TMP_"):
        return False
    if str(row.get("미해결사유", "") or "").strip():
        return False
    if not str(row.get("수익키", "") or "").strip():
        return False
    chicken_like = _has_any_token(f"{item_name} {std_name}", _CHICKEN_MENU_TOKENS) or "순살" in item_name or "뼈" in item_name
    if not chicken_like:
        return True
    chicken_type = str(row.get("닭유형", "") or "").strip()
    chicken_size = str(row.get("사이즈", "") or "").strip()
    return bool(chicken_type and chicken_size)


def _resolved_product_gap_context_keys(left_joined: pd.DataFrame) -> set[tuple[str, str, str]]:
    if left_joined.empty:
        return set()
    required = {"source", "item_id", "item_name", "line_role", "std_menu_name", "수익키"}
    if not required.issubset(left_joined.columns):
        return set()
    work = left_joined.fillna("")
    resolved = work[work.apply(_is_operationally_resolved_product_gap_row, axis=1)]
    if resolved.empty:
        return set()
    return {_product_gap_context_key(row) for _, row in resolved.iterrows()}


def _looks_like_validation_main_candidate(row: pd.Series) -> bool:
    role = str(row.get("line_role", "") or "").strip()
    if role in {"main", "side", "fee", "discount"}:
        return False
    sales = pd.to_numeric(pd.Series([row.get("total_price", "")]), errors="coerce").fillna(0).iloc[0]
    if sales == 0:
        return False
    item_name = str(row.get("item_name", "") or "").strip()
    if _OPTION_LIKE_RE.search(item_name) or _SIDE_NAME_RE.search(item_name):
        return False
    return _has_any_token(item_name, _MAIN_CANDIDATE_TOKENS)


def _main_row_chicken_profile(row: pd.Series) -> tuple[str, str, str] | None:
    profile = _locked_main_chicken_profile(row)
    if profile is not None:
        return profile
    text = " ".join(
        str(row.get(col, "") or "").strip()
        for col in ("menu_name", "std_menu_name", "item_name")
        if str(row.get(col, "") or "").strip()
    )
    if _has_any_token(text, _NON_CHICKEN_MENU_TOKENS) and not _has_any_token(text, _CHICKEN_MENU_TOKENS):
        return (CHICKEN_TYPE_NONE, CHICKEN_SIZE_NONE, CHICKEN_METHOD_NONE)
    if _has_any_token(text, _CHICKEN_MENU_TOKENS) or _infer_chicken_types(text):
        return ("", "", "닭메뉴")
    return None


def _repair_deciding_option_parent_to_chicken_main(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or not {"source", "sale_date", "order_id", "line_role", "item_seq", "parent_item_seq", "option_kind"}.issubset(frame.columns):
        return frame
    out = frame.copy()
    order_cols = ["source", "sale_date", "order_id"]
    repaired = 0
    for _, group in out.groupby(order_cols, dropna=False, sort=False):
        main_rows = group[group.get("line_role", pd.Series("", index=group.index)).astype(str).str.strip().eq("main")].copy()
        if main_rows.empty:
            continue
        main_by_seq = {
            str(row.get("item_seq", "") or "").strip(): (idx, row)
            for idx, row in main_rows.iterrows()
        }
        compatible: list[tuple[float, int, pd.Series]] = []
        for idx, row in main_rows.iterrows():
            profile = _main_row_chicken_profile(row)
            if profile is None or profile[0] == CHICKEN_TYPE_NONE:
                continue
            seq_num = pd.to_numeric(pd.Series([row.get("item_seq", "")]), errors="coerce").iloc[0]
            if pd.isna(seq_num):
                seq_num = float(len(compatible))
            compatible.append((float(seq_num), idx, row))
        if not compatible:
            continue
        for idx, row in group.iterrows():
            if str(row.get("line_role", "") or "").strip() != "option":
                continue
            if str(row.get("option_kind", "") or "").strip() not in CHICKEN_DECIDING_KINDS:
                continue
            parent_seq = str(row.get("parent_item_seq", "") or "").strip()
            parent_hit = main_by_seq.get(parent_seq)
            if parent_hit is None:
                continue
            _, parent = parent_hit
            parent_profile = _main_row_chicken_profile(parent)
            if parent_profile is None or parent_profile[0] != CHICKEN_TYPE_NONE:
                continue
            option_seq = pd.to_numeric(pd.Series([row.get("item_seq", "")]), errors="coerce").iloc[0]
            option_seq_num = float(option_seq) if not pd.isna(option_seq) else float("inf")
            previous = [(seq, main_idx, main_row) for seq, main_idx, main_row in compatible if seq < option_seq_num]
            if not previous:
                continue
            _, _, new_parent = sorted(previous, key=lambda value: value[0])[-1]
            new_parent_seq = str(new_parent.get("item_seq", "") or "").strip()
            if not new_parent_seq or new_parent_seq == parent_seq:
                continue
            out.at[idx, "parent_item_seq"] = new_parent_seq
            for col in ("menu_seq", "std_menu_name"):
                if col in out.columns:
                    out.at[idx, col] = new_parent.get(col, "")
            repaired += 1
    if repaired:
        logger.info("닭 결정 옵션 부모 재보정: %d행", repaired)
    return out


def _is_non_product_validation_role(value: object) -> bool:
    return str(value or "").strip() in {"fee", "discount", "제외", "수수료", "할인", "배달비수수료", "정산제외"}


def _build_validation_issues(
    orders: pd.DataFrame,
    left_joined: pd.DataFrame,
    gap: pd.DataFrame,
) -> pd.DataFrame:
    issues: list[dict[str, object]] = []
    resolved_gap_keys = _resolved_product_gap_context_keys(left_joined)

    if not gap.empty:
        for _, row in gap.fillna("").iterrows():
            if _is_non_product_validation_role(row.get("추정_수동분류", "")):
                continue
            severity = "WARN" if _product_gap_context_key(row) in resolved_gap_keys else None
            issues.append(_validation_gap_issue(row, severity=severity))

    if left_joined.empty:
        return pd.DataFrame(issues, columns=VALIDATION_ISSUE_COLUMNS)

    work = left_joined.fillna("").copy()
    work["_total_num"] = _validation_number(work, "total_price")
    work["_profit_sales_num"] = _profit_sales_num(work)
    role = _validation_text(work, "line_role")
    std_name = _validation_text(work, "std_menu_name")
    item_id = _validation_text(work, "item_id")
    item_name = _validation_text(work, "item_name")
    chicken_type = _validation_text(work, "닭유형")
    chicken_size = _validation_text(work, "사이즈")
    chicken_type_method = _validation_text(work, "닭유형_판정")
    chicken_size_method = _validation_text(work, "사이즈_판정")
    usage = _validation_text(work, "사용용량")
    usage_total = pd.to_numeric(work.get(CHICKEN_USAGE_TOTAL_COLUMN, pd.Series("", index=work.index)), errors="coerce").fillna(0)
    bone_usage_total = pd.to_numeric(work.get(BONE_USAGE_TOTAL_COLUMN, pd.Series("", index=work.index)), errors="coerce").fillna(0)
    boneless_usage_total = pd.to_numeric(work.get(BONELESS_USAGE_TOTAL_COLUMN, pd.Series("", index=work.index)), errors="coerce").fillna(0)
    material_usage = _validation_text(work, MATERIAL_USAGE_COLUMN)
    menu_weight_usage = _validation_text(work, MENU_WEIGHT_USAGE_COLUMN)
    menu_seq = _validation_text(work, "menu_seq")
    item_seq = _validation_text(work, "item_seq")
    parent_seq = _validation_text(work, "parent_item_seq")
    sale_type = _validation_text(work, "sale_type")
    order_exception = _validation_text(work, ORDER_EXCEPTION_TYPE_COLUMN)
    adjusted_sales = _validation_text(work, ADJUSTED_SALES_COLUMN)
    option_kind_text = _validation_text(work, "option_kind")
    profit_excluded = _cancel_offset_excluded(work)
    profit_sales = work["_profit_sales_num"]
    item_std_context = item_name + " " + std_name
    chicken_like_item = item_std_context.map(lambda value: _has_any_token(value, _CHICKEN_MENU_TOKENS) or "순살" in str(value or "") or "뼈" in str(value or ""))
    chicken_like_tmp_item = item_name.map(lambda value: _has_any_token(value, _CHICKEN_MENU_TOKENS) or "순살" in str(value or "") or "뼈" in str(value or ""))
    resolved_tmp_item = work.apply(_is_operationally_resolved_product_gap_row, axis=1)
    fixed_profiles = work.apply(_fixed_main_chicken_profile, axis=1)
    fixed_override_methods = {"메뉴프로필", "판정옵션", "수기", "기존수기"}
    fixed_mask = fixed_profiles.notna() & ~chicken_type_method.isin(fixed_override_methods)
    fixed_type = pd.Series("", index=work.index)
    fixed_size = pd.Series("", index=work.index)
    fixed_usage = pd.Series("", index=work.index)
    if fixed_mask.any():
        fixed_values = pd.DataFrame(
            fixed_profiles[fixed_mask].tolist(),
            index=work.index[fixed_mask],
            columns=["type", "size", "method"],
        )
        fixed_type.loc[fixed_mask] = fixed_values["type"]
        fixed_size.loc[fixed_mask] = fixed_values["size"]
        fixed_usage.loc[fixed_mask] = fixed_values.apply(lambda row: _expected_usage_for(row["type"], row["size"]), axis=1)

    critical_masks = [
        ("line_role_blank", role.eq(""), "line_role 공백"),
        ("std_menu_blank", role.isin(["main", "option"]) & std_name.eq(""), "main/option 표준메뉴명 공백"),
        ("menu_seq_blank", role.isin(["main", "option"]) & menu_seq.eq(""), "main/option menu_seq 공백"),
        ("option_parent_blank", role.eq("option") & parent_seq.eq(""), "option parent_item_seq 공백"),
        ("main_parent_mismatch", role.eq("main") & item_seq.ne(parent_seq), "main parent_item_seq가 자기 item_seq가 아님"),
        (
            "tmp_item",
            (item_id.str.startswith("TMP_") | std_name.str.startswith("TMP_") | item_name.str.startswith("TMP_"))
            & ~role.map(_is_non_product_validation_role)
            & (role.eq("main") | chicken_like_tmp_item)
            & ~resolved_tmp_item,
            "TMP 임시 상품이 남음",
        ),
        ("option_like_main", role.eq("main") & item_name.map(_is_forbidden_option_main_name), "맛/선택/추가 옵션 라인이 main으로 남음"),
        (
            "chicken_attr_conflict",
            role.eq("main")
            & item_std_context.str.contains("순살", regex=False)
            & chicken_type.eq("뼈닭")
            & chicken_type_method.ne("선택"),
            "순살 main이 뼈닭으로 정규화됨",
        ),
        (
            "chicken_main_marked_none",
            role.eq("main")
            & chicken_type.eq(CHICKEN_TYPE_NONE)
            & item_std_context.map(
                lambda value: _has_any_token(value, _CHICKEN_MENU_TOKENS) or "순살" in str(value or "")
            ),
            "닭 메뉴 main이 닭미사용으로 정규화됨",
        ),
        (
            "chicken_size_conflict",
            role.eq("main")
            & item_std_context.str.contains(r"1\s*인|한그릇", regex=True)
            & chicken_size.ne("1인")
            & chicken_size_method.ne("선택"),
            "1인 main이 1인 사이즈가 아님",
        ),
        (
            "fixed_main_chicken_mismatch",
            fixed_mask
            & (
                chicken_type.ne(fixed_type)
                | chicken_size.ne(fixed_size)
                | usage.ne(fixed_usage)
            ),
            "고정 메뉴명 main의 닭유형/사이즈/사용용량 불일치",
        ),
        (
            "non_main_chicken_usage",
            role.isin(["option", "side"])
            & (
                usage.ne("")
                | _validation_text(work, BONE_USAGE_COLUMN).ne("")
                | _validation_text(work, BONELESS_USAGE_COLUMN).ne("")
                | material_usage.str.contains("닭=", regex=False)
                | menu_weight_usage.str.contains("닭=", regex=False)
            ),
            "option/side 행에 분석용 닭사용량이 남음",
        ),
        (
            "chicken_split_total_mismatch",
            role.eq("main")
            & chicken_type.ne(CHICKEN_TYPE_NONE)
            & (usage_total - (bone_usage_total + boneless_usage_total)).abs().gt(0.0001),
            "사용용량_합계가 뼈+순살 분해 합계와 다름",
        ),
        (
            "cancel_chicken_usage",
            sale_type.eq("취소") & order_exception.eq("") & usage_total.ne(0),
            "예외정책을 거치지 않은 취소 주문에 닭사용량이 남음",
        ),
    ]
    for issue_type, mask, detail in critical_masks:
        for _, row in work[mask].iterrows():
            issues.append(_validation_row_issue(row, issue_type, detail, sales=row.get("_total_num", "")))

    order_keys = ["source", "sale_date", "order_id"]
    existing_item_seq: dict[tuple[str, str, str], set[str]] = {}
    for key, group in work.groupby(order_keys, dropna=False, sort=False):
        existing_item_seq[tuple(str(value) for value in key)] = set(_validation_text(group, "item_seq"))
    for _, row in work[role.eq("option")].iterrows():
        key = tuple(str(row.get(col, "") or "") for col in order_keys)
        if str(row.get("parent_item_seq", "") or "") not in existing_item_seq.get(key, set()):
            issues.append(_validation_row_issue(row, "option_orphan", "option parent_item_seq가 같은 주문 안에 없음", sales=row.get("_total_num", "")))

    parent_role_by_key: dict[tuple[str, str, str, str], str] = {}
    for _, row in work.iterrows():
        key = tuple(str(row.get(col, "") or "") for col in [*order_keys, "item_seq"])
        parent_role_by_key[key] = str(row.get("line_role", "") or "").strip()
    for _, row in work[role.eq("option")].iterrows():
        key = tuple(str(row.get(col, "") or "") for col in [*order_keys, "parent_item_seq"])
        parent_role = parent_role_by_key.get(key, "")
        if parent_role and parent_role != "main":
            issues.append(_validation_row_issue(row, "option_parent_not_main", "option parent_item_seq가 main 행을 가리키지 않음", sales=row.get("_total_num", "")))

    main_by_key: dict[tuple[str, str, str, str], pd.Series] = {}
    for _, row in work[role.eq("main")].iterrows():
        key = tuple(str(row.get(col, "") or "") for col in [*order_keys, "item_seq"])
        main_by_key[key] = row
    deciding_options = work[role.eq("option") & option_kind_text.isin(CHICKEN_DECIDING_KINDS)].copy()
    order_sequence_deciding_indexes = _order_sequence_deciding_option_indexes(work, order_keys)
    for _, row in deciding_options.iterrows():
        if row.name in order_sequence_deciding_indexes:
            continue
        parent_key = tuple(str(row.get(col, "") or "") for col in [*order_keys, "parent_item_seq"])
        parent = main_by_key.get(parent_key)
        if parent is None:
            continue
        kind = str(row.get("option_kind", "") or "").strip()
        if _menu_profile_ignores_deciding_option(parent, kind):
            continue
        parent_type = str(parent.get("닭유형", "") or "").strip()
        parent_size = str(parent.get("사이즈", "") or "").strip()
        option_name = str(row.get("item_name", "") or "").strip()
        if kind == OPTION_KIND_CHICKEN_TYPE:
            option_types = _unique_nonempty(_infer_chicken_types(option_name))
            if option_types and parent_type and parent_type != CHICKEN_TYPE_MIXED and parent_type not in option_types:
                issues.append(
                    _validation_row_issue(
                        parent,
                        "parent_child_chicken_type_conflict",
                        f"자식 닭유형 옵션({option_name})과 부모 닭유형({parent_type}) 불일치",
                        severity="WARN",
                        sales=parent.get("_profit_sales_num", parent.get("_total_num", "")),
                    )
                )
        if kind == OPTION_KIND_SIZE:
            option_sizes = _unique_nonempty(_infer_chicken_sizes(option_name))
            if option_sizes and parent_size and parent_size != CHICKEN_SIZE_NONE and parent_size not in option_sizes:
                sibling_mask = pd.Series(True, index=deciding_options.index)
                for col in [*order_keys, "parent_item_seq"]:
                    sibling_mask &= deciding_options[col].astype(str).eq(str(row.get(col, "") or ""))
                siblings = deciding_options[sibling_mask]
                if _is_redundant_default_size_option(row, parent_size, _has_size_option(siblings, parent_size)):
                    continue
                issues.append(
                    _validation_row_issue(
                        parent,
                        "parent_child_size_conflict",
                        f"자식 사이즈 옵션({option_name})과 부모 사이즈({parent_size}) 불일치",
                        severity="WARN",
                        sales=parent.get("_profit_sales_num", parent.get("_total_num", "")),
                    )
                )
            elif option_sizes and parent_size == CHICKEN_SIZE_NONE and parent_type != CHICKEN_TYPE_NONE:
                issues.append(
                    _validation_row_issue(
                        parent,
                        "parent_child_size_conflict",
                        f"자식 사이즈 옵션({option_name})이 있으나 부모 사이즈가 {CHICKEN_SIZE_NONE}",
                        severity="WARN",
                        sales=parent.get("_profit_sales_num", parent.get("_total_num", "")),
                    )
                )

    has_main_by_order = role.eq("main").groupby([work[col] for col in order_keys], dropna=False, sort=False).transform("sum")
    zero_main_candidates = work[has_main_by_order.eq(0) & work.apply(_looks_like_validation_main_candidate, axis=1)]
    for _, row in zero_main_candidates.iterrows():
        issues.append(_validation_row_issue(row, "zero_main_candidate", "main 없는 주문에 main 후보가 option으로 남음", sales=row.get("_total_num", "")))

    main_rows = work[role.eq("main")].copy()
    if not main_rows.empty:
        menu_main_counts = main_rows.groupby([main_rows[col] for col in [*order_keys, "menu_seq"]], dropna=False, sort=False).size()
        duplicate_menu_keys = {tuple(str(value) for value in key if value is not None) for key, count in menu_main_counts.items() if count > 1}
        if duplicate_menu_keys:
            for _, row in main_rows.iterrows():
                row_key = tuple(str(row.get(col, "") or "") for col in [*order_keys, "menu_seq"])
                if row_key in duplicate_menu_keys:
                    issues.append(_validation_row_issue(row, "menu_seq_duplicate_main", "한 menu_seq 안에 main이 둘 이상 남음", sales=row.get("_total_num", "")))
        main_rows["_main_key"] = main_rows.apply(_main_reconcile_key, axis=1)
        # 한 영수증에 같은 메뉴를 2그릇 시키는 것은 정상이다(옵션도 그릇 수만큼 붙는다).
        # 단가가 붙은 중복 main은 실제 판매이므로 이슈가 아니고, 단가 0인 중복만
        # 소스가 부모 라인을 한 번 더 내보낸 에코로 의심한다.
        duplicated = main_rows[
            main_rows["_main_key"].ne("")
            & main_rows.duplicated(subset=[*order_keys, "_main_key"], keep=False)
            & _validation_number(main_rows, "unit_price").le(0)
        ]
        for _, row in duplicated.iterrows():
            issues.append(_validation_row_issue(row, "duplicate_main_key", "같은 주문 안에 같은 표준메뉴 main이 둘 이상 남음", sales=row.get("_total_num", "")))

        if "수익키" in main_rows.columns:
            keyed = main_rows[_validation_text(main_rows, "수익키").ne("")].copy()
            if not keyed.empty:
                item_identity = _validation_text(keyed, "item_name")
                std_identity = _validation_text(keyed, "std_menu_name")
                keyed["_is_set_identity"] = item_identity.map(_has_set_identity).where(
                    item_identity.ne(""),
                    std_identity.map(_has_set_identity),
                )
                mixed_keys = (
                    keyed.groupby("수익키", dropna=False, sort=False)
                    .agg(
                        rows=("order_id", "size"),
                        orders=("order_id", "nunique"),
                        sales=("_profit_sales_num", "sum"),
                        has_set=("_is_set_identity", "any"),
                        has_non_set=("_is_set_identity", lambda s: (~s).any()),
                        item_sample=("item_name", lambda s: " | ".join(_unique_nonempty([str(v).strip() for v in s])[:6])),
                    )
                    .reset_index()
                )
                mixed_keys = mixed_keys[mixed_keys["has_set"] & mixed_keys["has_non_set"]]
                for _, row in mixed_keys.iterrows():
                    sample = keyed[keyed["수익키"].eq(row["수익키"])].iloc[0]
                    issues.append(
                        _validation_row_issue(
                            sample,
                            "profit_key_set_single_mixed",
                            f"세트/단품이 같은 수익키로 묶임: {row.get('수익키', '')} | 품목={row.get('item_sample', '')}",
                            severity="WARN",
                            rows=int(row.get("rows", 1) or 1),
                            orders=int(row.get("orders", 1) or 1),
                            sales=row.get("sales", ""),
                        )
                    )

    has_main_by_menu = role.eq("main").groupby([work[col] for col in [*order_keys, "menu_seq"]], dropna=False, sort=False).transform("sum")
    for _, row in work[role.eq("option") & has_main_by_menu.eq(0)].iterrows():
        issues.append(_validation_row_issue(row, "option_menu_seq_without_main", "option menu_seq 안에 main 행이 없음", sales=row.get("_total_num", "")))

    chicken_like_addon = role.eq("option") & item_name.map(
        lambda value: bool(_ROLE_ADDON_RE.search(str(value or "")) and _CHICKEN_ADDON_RE.search(str(value or "")))
    )
    for _, row in work[chicken_like_addon & option_kind_text.ne(OPTION_KIND_CHICKEN_ADDON)].iterrows():
        issues.append(_validation_row_issue(row, "chicken_like_option_not_chicken_addon", "닭추가 후보 옵션이 닭추가로 분류되지 않음", sales=row.get("_total_num", "")))

    chicken_addon = role.eq("option") & option_kind_text.eq(OPTION_KIND_CHICKEN_ADDON)
    unit_price = _validation_number(work, "unit_price").abs()
    for _, row in work[chicken_addon].iterrows():
        expected_price = _expected_chicken_addon_unit_price(row.get("item_name", ""))
        if expected_price is None:
            continue
        actual_price = unit_price.loc[row.name]
        if abs(float(actual_price) - float(expected_price)) <= 1:
            continue
        issues.append(
            _validation_row_issue(
                row,
                "chicken_addon_price_mismatch",
                f"닭추가 기준단가 불일치: 기대={_format_number(expected_price)}, 실제={_format_number(actual_price)}",
                severity="WARN",
                sales=row.get("_total_num", ""),
            )
        )

    # 수익률 수기 입력은 담당자 입력 대기 상태다. 그 외 분류/수수료 공백은
    # 실제 주문 계산을 왜곡하므로 차단한다.
    total_price = work["_total_num"]
    profit_required = role.isin(["main", "option", "side"]) & total_price.ne(0) & ~profit_excluded
    profit_blank = _validation_text(work, "공헌이익").eq("")
    for _, row in work[profit_required & profit_blank].iterrows():
        issues.append(
            _validation_row_issue(
                row,
                "profit_missing",
                str(row.get("원가미산출사유", "") or "매출 있는 주문 라인의 공헌이익 공백"),
                severity="WARN",
                sales=row.get("_total_num", ""),
            )
        )

    manual_profit_required = _manual_profit_target(work)
    negative_sales_review = total_price.lt(0) & ~order_exception.isin(SALESLESS_COUNTED_EXCEPTION_TYPES)
    for _, row in work[negative_sales_review & role.isin(["main", "option", "side"])].iterrows():
        issues.append(
            _validation_row_issue(
                row,
                "negative_sale_needs_exception_review",
                "음수 매출 주문입니다. 음식이 실제 제공된 이벤트/서비스면 예외주문에서 구분_manual과 닭계상_manual=Y를 입력하세요",
                severity="WARN",
                sales=row.get("_total_num", ""),
            )
        )
    reference_sales = _manual_profit_reference_sales_num(work)
    salesless_price_missing = manual_profit_required & order_exception.isin(SALESLESS_COUNTED_EXCEPTION_TYPES) & reference_sales.le(0)
    for _, row in work[salesless_price_missing].iterrows():
        issues.append(
            _validation_row_issue(
                row,
                "salesless_price_missing",
                "이벤트/서비스/전액할인 주문의 기준 판매가를 만들 수 없음",
                severity="WARN",
                sales=row.get("_profit_sales_num", ""),
            )
        )
    zero_price_cost_missing = (
        manual_profit_required
        & profit_sales.eq(0)
        & _manual_profit_counted_qty_num(work).gt(0)
        & option_kind_text.isin(ZERO_PRICE_COST_BEARING_KINDS)
        & _validation_text(work, "수기수익").eq("")
    )
    for _, row in work[zero_price_cost_missing].iterrows():
        issues.append(
            _validation_row_issue(
                row,
                "zero_price_cost_missing",
                "0원 원가성 품목의 메뉴원가_manual 미입력",
                severity="WARN",
                sales=row.get("_profit_sales_num", ""),
            )
        )
    profit_key_blank = _validation_text(work, "수익키").eq("")
    for _, row in work[manual_profit_required & profit_key_blank].iterrows():
        issues.append(
            _validation_row_issue(
                row,
                "profit_sales_uncovered",
                "수익매출이 있는데 수익키가 없음",
                severity="ERROR",
                sales=row.get("_profit_sales_num", ""),
            )
        )
    drift_rows = []
    for idx, row in work[manual_profit_required & ~profit_key_blank].iterrows():
        key = str(row.get("수익키", "") or "").strip()
        expected_key = _manual_profit_profile(row)[0]
        if key != expected_key:
            drift_rows.append((idx, row, expected_key))
    for _, row, expected_key in drift_rows:
        issues.append(
            _validation_row_issue(
                row,
                "profit_key_drift",
                "수익키 축 불일치: 기대=" + expected_key,
                severity="ERROR",
                sales=row.get("_profit_sales_num", ""),
            )
        )
    manual_profit_blank = _validation_text(work, "수기수익").eq("")
    manual_profit_missing = work[manual_profit_required & manual_profit_blank].copy()
    if not manual_profit_missing.empty:
        required_sales_sum = float(profit_sales[manual_profit_required].abs().sum())
        for source, group in manual_profit_missing.groupby("source", dropna=False, sort=False):
            source_text = str(source or "").strip() or "source없음"
            sales_sum = float(group["_profit_sales_num"].sum())
            sales_share = abs(sales_sum) / required_sales_sum * 100 if required_sales_sum else 0.0
            order_count = int(group["order_id"].nunique()) if "order_id" in group.columns else len(group)
            top_keys = " | ".join(
                _unique_nonempty(
                    [
                        str(value or "").strip()
                        for value in group.get("수익키", pd.Series("", index=group.index)).tolist()
                    ]
                )[:5]
            )
            sample = group.iloc[0]
            issues.append(
                _validation_row_issue(
                    sample,
                    "manual_profit_missing",
                    f"{source_text} 수기원가 미입력: {len(group)}행, 미입력 매출비중 {sales_share:.2f}%, 수익키={top_keys}",
                    severity="WARN",
                    rows=len(group),
                    orders=order_count,
                    sales=f"{sales_sum:g}",
                )
            )

    unmatched_cancel = _validation_text(work, CANCEL_OFFSET_STATUS_COLUMN).eq(CANCEL_OFFSET_UNMATCHED_STATUS)
    for _, row in work[unmatched_cancel & role.eq("main")].iterrows():
        issues.append(
            _validation_row_issue(
                row,
                "cancel_offset_unmatched",
                "같은 일자/채널/결제금액 정상 주문을 찾지 못한 취소",
                severity="WARN",
                sales=row.get("_total_num", ""),
            )
        )

    commission_required = (
        role.isin(["main", "option", "side"])
        & total_price.ne(0)
        & ~profit_excluded
        & ~_validation_text(work, PROFIT_CHANNEL_COLUMN).isin(NO_COMMISSION_PROFIT_CHANNELS)
        & ~_validation_text(work, "option_kind").isin(NON_SALES_KINDS)
    )
    commission_source = _validation_text(work, "수수료율_출처")
    commission_blank = _validation_text(work, "수수료율").eq("") & commission_source.ne("마트미지원")
    for _, row in work[commission_required & commission_blank].iterrows():
        issues.append(
            _validation_row_issue(
                row,
                "commission_missing",
                str(row.get("수수료율_출처", "") or "배달 매출 행의 수수료율 공백"),
                severity="ERROR",
                sales=row.get("_total_num", ""),
            )
        )

    unresolved = _validation_text(work, "미해결사유").ne("")
    for _, row in work[unresolved].iterrows():
        issues.append(
            _validation_row_issue(
                row,
                "chicken_attr_unresolved",
                str(row.get("미해결사유", "") or ""),
                severity="ERROR",
                sales=row.get("_total_num", ""),
            )
        )

    addon_unresolved = _validation_text(work, CHICKEN_ADDON_REASON_COLUMN).ne("")
    for _, row in work[addon_unresolved].iterrows():
        issues.append(
            _validation_row_issue(
                row,
                "chicken_addon_unresolved",
                str(row.get(CHICKEN_ADDON_REASON_COLUMN, "") or ""),
                severity="ERROR",
                sales=row.get("_total_num", ""),
            )
        )

    group_total = work["_total_num"].groupby([work[col] for col in order_keys], dropna=False, sort=False).transform("sum")
    discount_num = pd.to_numeric(work.get("discount_amount", pd.Series("", index=work.index)), errors="coerce").fillna(0)
    group_discount = discount_num.groupby([work[col] for col in order_keys], dropna=False, sort=False).transform("sum")
    group_cancel = sale_type.eq("취소").groupby([work[col] for col in order_keys], dropna=False, sort=False).transform("any")
    exception_wait = order_exception.eq("") & (
        group_cancel
        | (group_total.eq(0) & group_discount.gt(0))
    )
    for _, row in work[exception_wait].iterrows():
        issues.append(
            _validation_row_issue(
                row,
                "order_exception_unresolved",
                "주문예외구분 미입력",
                severity="ERROR",
                sales=row.get("_total_num", ""),
            )
        )

    full_discount_without_adjustment = (
        group_total.eq(0)
        & group_discount.gt(0)
        & adjusted_sales.eq("")
    )
    for _, row in work[full_discount_without_adjustment].iterrows():
        issues.append(
            _validation_row_issue(
                row,
                "full_discount_without_exception",
                "전액할인 주문에 매출_보정이 없음",
                severity="ERROR",
                sales=row.get("_total_num", ""),
            )
        )

    if not orders.empty and "_pk" in orders.columns and "_pk" in left_joined.columns:
        order_pk = set(_validation_text(orders, "_pk"))
        left_pk = set(_validation_text(left_joined, "_pk"))
        for missing_pk in sorted(order_pk ^ left_pk):
            issues.append({
                "issue_type": "pk_mismatch",
                "severity": "ERROR",
                "source": "",
                "sale_date": "",
                "ym": "",
                "order_id": "",
                "item_seq": "",
                "item_id": missing_pk,
                "item_name": "",
                "line_role": "",
                "std_menu_name": "",
                "detail": "10_orders와 12_orders_left의 _pk set 불일치",
                "rows": 1,
                "orders": "",
                "sales": "",
            })

    return pd.DataFrame(issues, columns=VALIDATION_ISSUE_COLUMNS)


def _classification_pattern_signature(row: pd.Series) -> str:
    parts = [
        str(row.get("issue_type", "") or "").strip(),
        str(row.get("source", "") or "").strip(),
        str(row.get("line_role", "") or "").strip(),
        str(row.get("std_menu_name", "") or "").strip(),
        str(row.get("detail", "") or "").strip(),
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def _load_classification_pattern_alert_state() -> dict[str, object]:
    if not CLASSIFICATION_PATTERN_ALERT_STATE_PATH.exists():
        return {"signatures": []}
    try:
        loaded = json.loads(CLASSIFICATION_PATTERN_ALERT_STATE_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("분류패턴 알림 상태 읽기 실패, 새로 생성합니다: %s", exc)
        return {"signatures": []}
    if not isinstance(loaded, dict):
        return {"signatures": []}
    signatures = loaded.get("signatures", [])
    if not isinstance(signatures, list):
        signatures = []
    loaded["signatures"] = [str(value) for value in signatures]
    return loaded


def _notify_new_classification_patterns(validation_issues: pd.DataFrame, completeness: pd.DataFrame) -> None:
    """자동 분류를 깨는 새 패턴만 Telegram으로 알린다. 입력 대기 WARN은 제외한다."""
    pattern_rows: list[dict[str, object]] = []
    if not validation_issues.empty:
        severity = validation_issues.get("severity", pd.Series("", index=validation_issues.index)).astype(str).str.upper()
        blocking = validation_issues[~severity.isin(["WARN", "INFO"])].fillna("").copy()
        for _, row in blocking.iterrows():
            pattern_rows.append({
                "issue_type": str(row.get("issue_type", "") or "").strip(),
                "source": str(row.get("source", "") or "").strip(),
                "line_role": str(row.get("line_role", "") or "").strip(),
                "std_menu_name": str(row.get("std_menu_name", "") or "").strip(),
                "detail": str(row.get("detail", "") or "").strip(),
                "rows": str(row.get("rows", "") or ""),
                "orders": str(row.get("orders", "") or ""),
                "sales": str(row.get("sales", "") or ""),
            })
    if not completeness.empty:
        for _, row in completeness.fillna("").iterrows():
            dimension = str(row.get("차원", "") or "").strip()
            if dimension in COMPLETENESS_INPUT_WAIT_DIMENSIONS:
                continue
            rate = pd.to_numeric(pd.Series([row.get("완결률", "")]), errors="coerce").iloc[0]
            if pd.isna(rate) or float(rate) >= 100.0:
                continue
            pattern_rows.append({
                "issue_type": "classification_completeness_below_100",
                "source": "",
                "line_role": "",
                "std_menu_name": dimension,
                "detail": str(row.get("미완요약", "") or ""),
                "rows": str(row.get("분모", "") or ""),
                "orders": "",
                "sales": "",
            })
    if not pattern_rows:
        return

    pattern_frame = pd.DataFrame(pattern_rows).drop_duplicates(
        subset=["issue_type", "source", "line_role", "std_menu_name", "detail"],
        keep="first",
    )
    pattern_frame["_signature"] = pattern_frame.apply(_classification_pattern_signature, axis=1)
    state = _load_classification_pattern_alert_state()
    known = set(str(value) for value in state.get("signatures", []))
    new_patterns = pattern_frame[~pattern_frame["_signature"].isin(known)].copy()
    if new_patterns.empty:
        return

    lines = [
        "[도리당] 메뉴계층 신규 분류패턴",
        "dag_id=DB_MenuHierarchy_Test_Dags",
        f"new_patterns={len(new_patterns)}",
    ]
    for _, row in new_patterns.head(12).iterrows():
        lines.append(
            "- "
            + f"{row.get('issue_type', '')}"
            + f" | source={row.get('source', '') or '-'}"
            + f" | role={row.get('line_role', '') or '-'}"
            + f" | name={row.get('std_menu_name', '') or '-'}"
            + f" | detail={str(row.get('detail', '') or '')[:120]}"
        )
    if len(new_patterns) > 12:
        lines.append(f"... 외 {len(new_patterns) - 12}개")

    sent = send_telegram_chunks("\n".join(lines))
    state["signatures"] = sorted(known | set(new_patterns["_signature"].astype(str)))
    CLASSIFICATION_PATTERN_ALERT_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CLASSIFICATION_PATTERN_ALERT_STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    logger.warning("신규 분류패턴 감지: %d개, telegram_sent=%s", len(new_patterns), sent)


def _completeness_audit_rows(
    left_joined: pd.DataFrame,
    option_kind_master: pd.DataFrame,
    material_price_master: pd.DataFrame,
) -> list[dict[str, object]]:
    """완결률을 막고 있는 대상을 이름 단위로 남긴다. 25번은 숫자, 여기는 목록이다."""
    rows: list[dict[str, object]] = []

    def add(audit_type: str, item_name: str, detail: str) -> None:
        rows.append(
            {
                "audit_type": audit_type,
                "severity": "REVIEW",
                "source": "",
                "item_id": "",
                "item_name": item_name,
                "std_menu_name": "",
                "line_role": "",
                "rows": 1,
                "orders": "",
                "sales": "",
                "detail": detail,
            }
        )

    if not option_kind_master.empty:
        unconfirmed = option_kind_master[
            ~_filled(option_kind_master["option_kind_확정"])
            & ~option_kind_master["option_kind_제안"].isin(list(NON_SALES_KINDS) + [OPTION_KIND_MAIN])
        ]
        for _, row in unconfirmed.iterrows():
            add(
                "option_kind_unclassified",
                str(row.get("item_name", "")),
                f"option_kind 확정 필요 (제안={row.get('option_kind_제안', '')})",
            )

    if not material_price_master.empty:
        for _, row in material_price_master[~_filled(material_price_master["단가_manual"])].iterrows():
            add("material_price_missing", str(row.get("재료명", "")), "23번 단가_manual 미입력으로 재료원가 계산 불가")

    if not left_joined.empty and "std_menu_name" in left_joined.columns:
        main_rows = left_joined[left_joined.get("line_role", pd.Series("", index=left_joined.index)).eq("main")]
        for name in _unaliased_promo_tag_names(main_rows.get("std_menu_name", pd.Series(dtype=str))):
            add("std_menu_alias_missing", name, "프로모션 태그 미정리. _STD_MENU_ALIAS 등록 검토 필요")

    return rows


def _build_classification_audit(
    left_joined: pd.DataFrame,
    option_kind_master: pd.DataFrame | None = None,
    material_price_master: pd.DataFrame | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = _completeness_audit_rows(
        left_joined,
        option_kind_master if option_kind_master is not None else pd.DataFrame(),
        material_price_master if material_price_master is not None else pd.DataFrame(),
    )
    if left_joined.empty:
        return pd.DataFrame(rows, columns=CLASSIFICATION_AUDIT_COLUMNS)

    work = left_joined.fillna("").copy()
    work["_total_num"] = pd.to_numeric(work.get("total_price", pd.Series("", index=work.index)), errors="coerce").fillna(0)

    def add_row(audit_type: str, severity: str, row: pd.Series, detail: str, rows_count: object = 1, orders: object = "", sales: object = "") -> None:
        rows.append({
            "audit_type": audit_type,
            "severity": severity,
            "source": row.get("source", ""),
            "item_id": row.get("item_id", ""),
            "item_name": row.get("item_name", ""),
            "std_menu_name": row.get("std_menu_name", ""),
            "line_role": row.get("line_role", ""),
            "rows": rows_count,
            "orders": orders,
            "sales": _format_number(sales),
            "detail": detail,
        })

    main = work[work.get("line_role", pd.Series("", index=work.index)).eq("main")].copy()
    if not main.empty:
        grouped = (
            main.groupby(["item_id", "source", "item_name", "std_menu_name"], dropna=False, sort=False)
            .agg(
                rows=("item_id", "size"),
                orders=("order_id", "nunique"),
                sales=("_total_num", "sum"),
                types=("닭유형", _unique_join),
                sizes=("사이즈", _unique_join),
                usages=("사용용량", _unique_join),
            )
            .reset_index()
        )
        unstable = grouped[
            grouped["types"].str.contains("|", regex=False)
            | grouped["sizes"].str.contains("|", regex=False)
            | grouped["usages"].str.contains("|", regex=False)
        ]
        for _, row in unstable.iterrows():
            add_row(
                "main_attr_unstable",
                "WARN",
                row,
                f"main 상품의 최종 닭속성이 옵션 조합에 따라 흔들림: 닭유형={row.get('types', '')}, 사이즈={row.get('sizes', '')}, 사용용량={row.get('usages', '')}",
                rows_count=row.get("rows", ""),
                orders=row.get("orders", ""),
                sales=row.get("sales", ""),
            )

        signal_rows = main.copy()
        signal_rows["_menu_name_signal"] = signal_rows.get("menu_name", pd.Series("", index=signal_rows.index)).map(
            lambda value: "|".join(_infer_chicken_types(value))
        )
        signal_rows["_final_type"] = signal_rows.get("닭유형", pd.Series("", index=signal_rows.index)).astype(str).str.strip()
        signal_conflict = signal_rows[
            signal_rows["_menu_name_signal"].isin(["뼈닭", "순살"])
            & signal_rows["_final_type"].isin(["뼈닭", "순살"])
            & signal_rows["_menu_name_signal"].ne(signal_rows["_final_type"])
        ]
        if not signal_conflict.empty:
            grouped_conflict = (
                signal_conflict.groupby(
                    ["item_id", "source", "item_name", "std_menu_name", "menu_name", "_menu_name_signal", "_final_type"],
                    dropna=False,
                    sort=False,
                )
                .agg(rows=("item_id", "size"), orders=("order_id", "nunique"), sales=("_total_num", "sum"))
                .reset_index()
            )
            for _, row in grouped_conflict.iterrows():
                add_row(
                    "main_menu_name_chicken_type_conflict",
                    "WARN",
                    row,
                    "대표주문메뉴명 닭유형 신호와 최종 닭유형 불일치: "
                    f"menu_name={row.get('menu_name', '')}, "
                    f"menu_name신호={row.get('_menu_name_signal', '')}, "
                    f"최종닭유형={row.get('_final_type', '')}; 메뉴명보정/메뉴닭프로필 확인",
                    rows_count=row.get("rows", ""),
                    orders=row.get("orders", ""),
                    sales=row.get("sales", ""),
                )

    fixed_profiles = work.apply(_fixed_main_chicken_profile, axis=1)
    fixed_mask = fixed_profiles.notna() & work.get("닭유형_판정", pd.Series("", index=work.index)).astype(str).str.strip().ne("메뉴프로필")
    if fixed_mask.any():
        fixed_values = pd.DataFrame(
            fixed_profiles[fixed_mask].tolist(),
            index=work.index[fixed_mask],
            columns=["type", "size", "method"],
        )
        fixed_work = work.loc[fixed_mask].copy()
        fixed_work["_fixed_type"] = fixed_values["type"]
        fixed_work["_fixed_size"] = fixed_values["size"]
        fixed_work["_fixed_usage"] = fixed_values.apply(lambda row: _expected_usage_for(row["type"], row["size"]), axis=1)
        mismatch = fixed_work[
            fixed_work["닭유형"].ne(fixed_work["_fixed_type"])
            | fixed_work["사이즈"].ne(fixed_work["_fixed_size"])
            | fixed_work["사용용량"].ne(fixed_work["_fixed_usage"])
        ]
        for _, row in mismatch.iterrows():
            add_row(
                "fixed_main_mismatch",
                "ERROR",
                row,
                f"고정 메뉴명 기대값={row.get('_fixed_type', '')}/{row.get('_fixed_size', '')}/{row.get('_fixed_usage', '')}",
                sales=row.get("_total_num", ""),
            )

    non_main_usage = work[
        work.get("line_role", pd.Series("", index=work.index)).isin(["option", "side"])
        & (
            work.get("사용용량", pd.Series("", index=work.index)).astype(str).str.strip().ne("")
            | work.get(MATERIAL_USAGE_COLUMN, pd.Series("", index=work.index)).astype(str).str.contains("닭=", regex=False)
            | work.get(MENU_WEIGHT_USAGE_COLUMN, pd.Series("", index=work.index)).astype(str).str.contains("닭=", regex=False)
        )
    ]
    for _, row in non_main_usage.iterrows():
        add_row("non_main_chicken_usage", "ERROR", row, "option/side 행에 분석용 닭사용량 또는 닭 재료사용량이 남음", sales=row.get("_total_num", ""))

    priced = work[
        work.get("line_role", pd.Series("", index=work.index)).isin(["main", "option", "side"])
        & work["_total_num"].ne(0)
    ]
    profit_zero = priced[priced.get("수익률", pd.Series("", index=priced.index)).astype(str).str.strip().isin(["0", "0.0", "0.00", "0%"])]
    if not profit_zero.empty:
        grouped_zero = (
            profit_zero.groupby(["source", "line_role"], dropna=False, sort=False)
            .agg(rows=("item_id", "size"), orders=("order_id", "nunique"), sales=("_total_num", "sum"))
            .reset_index()
        )
        for _, row in grouped_zero.iterrows():
            add_row(
                "profit_zero_placeholder",
                "WARN",
                row,
                "매출 있는 행의 수익률이 0이라 실제 수익분석 전 마진율 확인 필요",
                rows_count=row.get("rows", ""),
                orders=row.get("orders", ""),
                sales=row.get("sales", ""),
            )

    review = _load_menu_hierarchy_review_input().fillna("")
    required = {"item_id", "source", "brand", "store", "item_name", "표준_메뉴명_edit", "수동분류_edit", *_MANUAL_CHICKEN_COLUMNS}
    if not review.empty and required.issubset(review.columns):
        review = _target_product_rows(review).copy()
        for col in required:
            review[col] = review[col].astype(str).str.strip()
        review_context = review["item_name"] + " " + review["표준_메뉴명_edit"]
        main_like = review["수동분류_edit"].isin(["메인", "1인", "2인", "3인", "중", "대", "반마리", "한마리", "세트"])
        chicken_like = review_context.map(lambda value: _has_any_token(value, _CHICKEN_MENU_TOKENS) or "순살" in value or "뼈" in value)
        blank_attr = (
            review["닭유형_manual"].eq("")
            | review["사이즈_manual"].eq("")
            | review["닭사용량_manual"].eq("")
        )
        counts = (
            work.groupby(["item_id", "source"], dropna=False, sort=False)
            .agg(rows=("item_id", "size"), orders=("order_id", "nunique"), sales=("_total_num", "sum"))
            .reset_index()
        )
        review_blank = review[chicken_like & main_like & blank_attr].merge(
            counts,
            on=["item_id", "source"],
            how="left",
        ).fillna("")
        for _, row in review_blank.iterrows():
            add_row(
                "review_chicken_attr_blank",
                "WARN",
                pd.Series({
                    "source": row.get("source", ""),
                    "item_id": row.get("item_id", ""),
                    "item_name": row.get("item_name", ""),
                    "std_menu_name": row.get("표준_메뉴명_edit", ""),
                    "line_role": row.get("수동분류_edit", ""),
                }),
                "닭 main 후보 상품의 수동 닭유형/사이즈/사용량 중 공백 존재",
                rows_count=row.get("rows", ""),
                orders=row.get("orders", ""),
                sales=row.get("sales", ""),
            )

    return pd.DataFrame(rows, columns=CLASSIFICATION_AUDIT_COLUMNS)


def _build_chicken_usage_summary(left_joined: pd.DataFrame) -> pd.DataFrame:
    if left_joined.empty:
        return pd.DataFrame(
            columns=[
                "source", "brand", "store", "std_menu_name", "사이즈", "닭유형", HALF_COMBO_COLUMN, "분류룰",
                "주문건수", "판매수량", "매출합계", "닭사용량합계", "뼈사용량합계", "순살사용량합계",
            ]
        )
    work = left_joined[left_joined.get("line_role", pd.Series("", index=left_joined.index)).eq("main")].copy()
    if work.empty:
        return pd.DataFrame()
    work["qty_num"] = pd.to_numeric(work.get("qty", ""), errors="coerce").fillna(0)
    work["sales_num"] = pd.to_numeric(work.get("total_price", ""), errors="coerce").fillna(0)
    work["usage_num"] = pd.to_numeric(work.get(CHICKEN_USAGE_TOTAL_COLUMN, ""), errors="coerce").fillna(0)
    work["bone_usage_num"] = pd.to_numeric(work.get(BONE_USAGE_TOTAL_COLUMN, ""), errors="coerce").fillna(0)
    work["boneless_usage_num"] = pd.to_numeric(work.get(BONELESS_USAGE_TOTAL_COLUMN, ""), errors="coerce").fillna(0)
    grouped = (
        work.groupby(["source", "brand", "store", "std_menu_name", "사이즈", "닭유형", HALF_COMBO_COLUMN, CLASSIFICATION_RULE_COLUMN], dropna=False, sort=False)
        .agg(
            주문건수=("order_id", "nunique"),
            판매수량=("qty_num", "sum"),
            매출합계=("sales_num", "sum"),
            닭사용량합계=("usage_num", "sum"),
            뼈사용량합계=("bone_usage_num", "sum"),
            순살사용량합계=("boneless_usage_num", "sum"),
        )
        .reset_index()
    )
    for col in ("주문건수", "판매수량", "매출합계", "닭사용량합계", "뼈사용량합계", "순살사용량합계"):
        grouped[col] = grouped[col].map(_format_number)
    return grouped.sort_values("닭사용량합계", key=lambda s: pd.to_numeric(s, errors="coerce").fillna(0), ascending=False)


def _build_source_validation_summary(left_joined: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "source",
        "main행",
        "주문건수",
        "매출합계",
        "완전분류행",
        "미분류행",
        "완전분류율",
        "미분류매출",
    ]
    if left_joined.empty:
        return pd.DataFrame(columns=columns)
    work = left_joined[left_joined.get("line_role", pd.Series("", index=left_joined.index)).eq("main")].copy()
    if work.empty:
        return pd.DataFrame(columns=columns)
    work["sales_num"] = pd.to_numeric(work.get("total_price", ""), errors="coerce").fillna(0)
    status = work.get(CHICKEN_CONFIDENCE_COLUMN, pd.Series("", index=work.index)).fillna("").astype(str).str.strip()
    rows = []
    for source, group in work.groupby("source", dropna=False, sort=False):
        group_status = status.loc[group.index]
        total = len(group)
        classified = group_status.eq("확정")
        unresolved = ~classified
        rows.append(
            {
                "source": source,
                "main행": total,
                "주문건수": group["order_id"].nunique(),
                "매출합계": group["sales_num"].sum(),
                "완전분류행": int(classified.sum()),
                "미분류행": int(unresolved.sum()),
                "완전분류율": round(float(classified.sum()) / total * 100, 2) if total else 100.0,
                "미분류매출": group.loc[unresolved, "sales_num"].sum(),
            }
        )
    out = pd.DataFrame(rows, columns=columns)
    for col in ["main행", "주문건수", "매출합계", "완전분류행", "미분류행", "미분류매출"]:
        out[col] = out[col].map(_format_number)
    return out


def _build_unresolved_top_summary(left_joined: pd.DataFrame) -> pd.DataFrame:
    columns = ["source", "std_menu_name", "사이즈", "닭유형", "미해결사유", "분류룰", "행수", "주문건수", "매출합계"]
    if left_joined.empty:
        return pd.DataFrame(columns=columns)
    work = left_joined[
        left_joined.get("line_role", pd.Series("", index=left_joined.index)).eq("main")
        & left_joined.get(CHICKEN_CONFIDENCE_COLUMN, pd.Series("", index=left_joined.index)).fillna("").astype(str).str.strip().ne("확정")
    ].copy()
    if work.empty:
        return pd.DataFrame(columns=columns)
    work["sales_num"] = pd.to_numeric(work.get("total_price", ""), errors="coerce").fillna(0)
    grouped = (
        work.groupby(["source", "std_menu_name", "사이즈", "닭유형", "미해결사유", CLASSIFICATION_RULE_COLUMN], dropna=False, sort=False)
        .agg(행수=("_pk", "size"), 주문건수=("order_id", "nunique"), 매출합계=("sales_num", "sum"))
        .reset_index()
    )
    for col in ("행수", "주문건수", "매출합계"):
        grouped[col] = grouped[col].map(_format_number)
    return grouped.reindex(columns=columns).sort_values("매출합계", key=lambda s: pd.to_numeric(s, errors="coerce").fillna(0).abs(), ascending=False)


def _write_manual_input_workbook(
    option_kind_master: pd.DataFrame,
    std_menu_override_input: pd.DataFrame,
    menu_weight_master: pd.DataFrame,
    material_price_master: pd.DataFrame,
    chicken_ratio_master: pd.DataFrame,
    menu_chicken_profile_master: pd.DataFrame,
    manual_profit_rate_master: pd.DataFrame,
    judgement_option_input: pd.DataFrame,
    manager_input: pd.DataFrame,
    option_material_input: pd.DataFrame,
    order_exception_input: pd.DataFrame,
) -> None:
    chicken_conversion = _read_manual_workbook_sheet(CHICKEN_CONVERSION_SHEET_NAME)
    if chicken_conversion.empty:
        chicken_conversion = pd.DataFrame(
            [
                {"항목": "순살_1마리_g", "값_manual": "", "메모": "순살 추가 옵션 g -> 마리 환산"},
                {"항목": "뼈닭_1마리_g", "값_manual": "", "메모": "뼈 추가 옵션 g -> 마리 환산"},
            ],
            columns=["항목", "값_manual", "메모"],
        )
    profit_rate = _normalize_manual_profit_amounts(
        manual_profit_rate_master.reindex(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS, fill_value="")
    )
    sheets = {
        "수익률": profit_rate,
        MANUAL_PROFIT_RATE_PRESERVED_SHEET_NAME: _build_preserved_manual_profit_rate_rows(profit_rate),
        STD_MENU_OVERRIDE_SHEET_NAME: std_menu_override_input.reindex(columns=STD_MENU_OVERRIDE_COLUMNS, fill_value=""),
        "메뉴중량": menu_weight_master,
        "옵션분류": option_kind_master.reindex(columns=OPTION_KIND_MASTER_COLUMNS, fill_value=""),
        "재료단가": material_price_master.reindex(columns=MATERIAL_PRICE_MASTER_COLUMNS, fill_value=""),
        "뼈순살비율": chicken_ratio_master.reindex(columns=CHICKEN_RATIO_MASTER_COLUMNS, fill_value=""),
        MENU_CHICKEN_PROFILE_SHEET_NAME: menu_chicken_profile_master.reindex(columns=MENU_CHICKEN_PROFILE_COLUMNS, fill_value=""),
        JUDGEMENT_OPTION_SHEET_NAME: judgement_option_input.reindex(columns=JUDGEMENT_OPTION_COLUMNS, fill_value=""),
        CHICKEN_CONVERSION_SHEET_NAME: chicken_conversion.reindex(columns=["항목", "값_manual", "메모"], fill_value=""),
        "옵션재료": option_material_input,
        ORDER_EXCEPTION_SHEET_NAME: order_exception_input.reindex(columns=ORDER_EXCEPTION_COLUMNS, fill_value=""),
        "예외보정": manager_input,
    }
    _guard_manual_workbook_loss(sheets)
    _backup_manual_workbook_before_write()
    _write_excel_workbook(sheets, MANUAL_WORKBOOK_OUTPUT_PATH)


def _write_summary_workbook(
    left_joined: pd.DataFrame,
    material_usage_summary: pd.DataFrame,
    menu_profit_summary: pd.DataFrame,
    manual_profit_summary: pd.DataFrame,
    completeness: pd.DataFrame,
    validation_issues: pd.DataFrame,
    judgement_option_result: pd.DataFrame,
) -> None:
    _write_excel_workbook(
        {
            "메뉴별_닭사용량": _build_chicken_usage_summary(left_joined),
            "메뉴별_수익": manual_profit_summary.reindex(columns=MANUAL_PROFIT_SUMMARY_COLUMNS, fill_value=""),
            "공헌이익": menu_profit_summary.reindex(columns=MENU_PROFIT_SUMMARY_COLUMNS, fill_value=""),
            "재료사용량": material_usage_summary.reindex(columns=MATERIAL_USAGE_SUMMARY_COLUMNS, fill_value=""),
            "source별_분류검증": _build_source_validation_summary(left_joined),
            "미분류_TOP": _build_unresolved_top_summary(left_joined),
            "닭결정옵션_검토": _build_chicken_decision_option_review(left_joined),
            "판정옵션_적용결과": judgement_option_result,
            "완결률": completeness.reindex(columns=COMPLETENESS_COLUMNS, fill_value=""),
            "검증이슈": validation_issues.reindex(columns=VALIDATION_ISSUE_COLUMNS, fill_value=""),
        },
        SUMMARY_WORKBOOK_OUTPUT_PATH,
    )


def _build_compact_usage_text(yms: list[str], completeness: pd.DataFrame, validation_issues: pd.DataFrame) -> str:
    warn_count = 0 if validation_issues.empty else int(validation_issues["severity"].astype(str).str.upper().eq("WARN").sum())
    block_count = 0 if validation_issues.empty else int((~validation_issues["severity"].astype(str).str.upper().isin(["WARN", "INFO"])).sum())
    completeness_text = "완결률 없음"
    if not completeness.empty:
        rows = ["| 차원 | 완결률 | 상태 | 미완요약 |", "| --- | ---: | --- | --- |"]
        for _, row in completeness.iterrows():
            rows.append(
                f"| {row.get('차원', '')} | {row.get('완결률', '')} | {row.get('상태', '')} | {str(row.get('미완요약', '')).replace('|', '/')} |"
            )
        completeness_text = "\n".join(rows)
    return f"""# 송파삼전점 메뉴계층 사용법

## 보는 파일

1. `01_수기입력.xlsx`만 입력합니다.
2. DAG를 다시 실행합니다.
3. `02_최종주문.csv`와 `03_요약.xlsx`를 확인합니다.

## 입력 위치

- 품목 원가: `01_수기입력.xlsx` > `수익률` 시트의 `판매가_manual`, `메뉴원가_manual`, `상차림비_manual` 입력 및 `판매가`, `판매가기준`, `상차림포함원가` 자동 계산
- 메뉴명 보정: `메뉴명보정` 시트의 `std_menu_name_manual`
- 닭/재료 사용량: `메뉴중량` 시트의 `*사용량_manual`
- 옵션 분류: `옵션분류` 시트의 `option_kind_확정`
- 재료 단가: `재료단가` 시트의 `단가_manual`
- 뼈순살 비율: `뼈순살비율` 시트의 `뼈비율_manual`
- 메뉴별 닭유형 허용범위: `메뉴닭프로필` 시트의 `허용닭유형`, `기본닭유형`, `옵션닭유형적용`
- 판정옵션: `판정옵션` 시트의 `닭유형`, `사이즈`, `사용용량`
- 반반 슬롯: `판정옵션` 시트의 `반반슬롯1`, `반반슬롯2`
- 닭 가산 옵션: `옵션분류` 시트의 `닭가산_manual`, `닭가산유형_manual`
- 닭 g 환산: `닭환산` 시트의 `값_manual`
- 주문 예외: `예외주문` 시트의 `구분_manual`, `닭계상_manual`

## 월별 원가율 보기

- `02_최종주문.csv`에서 `ym`으로 묶고 아래 식을 씁니다.
  - 원가율 = 1 - SUM(`원가_기준수익`) / SUM(`원가_기준매출`)
- 두 열은 담당자가 적은 `판매가_manual`(없으면 자동 `판매가`) x 수량 기준이라 정가 기준 원가율입니다.
- 실제 정산 매출은 `수익매출` 열입니다. 할인과 쿠팡 옵션 번들 때문에 둘은 다릅니다.
  분모에 `수익매출`을 넣으면 원가율이 어긋납니다.

## 완전분류 확인

- `02_최종주문.csv`에는 최종 분석값과 `분류룰`만 남깁니다.
- `03_요약.xlsx`의 `source별_분류검증`에서 `미분류행`이 0이어야 합니다.
- `미분류_TOP`에 행이 남으면 `판정옵션` 시트를 채우고 DAG를 다시 실행합니다.

## 이번 실행

- 대상월: {", ".join(yms)}
- WARN: {warn_count}건
- BLOCK: {block_count}건

## 완결률

{completeness_text}
"""


def _write_debug_outputs(
    out: pd.DataFrame,
    hierarchy: pd.DataFrame,
    left_joined: pd.DataFrame,
    manager_input: pd.DataFrame,
    llm_payload: list[dict],
    llm_rows: list[dict],
    option_material_input: pd.DataFrame,
    option_kind_master: pd.DataFrame,
    menu_weight_master: pd.DataFrame,
    chicken_ratio_master: pd.DataFrame,
    material_price_master: pd.DataFrame,
    menu_profit_summary: pd.DataFrame,
    manual_profit_rate_master: pd.DataFrame,
    manual_profit_summary: pd.DataFrame,
    menu_chicken_profile_master: pd.DataFrame,
    completeness: pd.DataFrame,
    menu_weight_input: pd.DataFrame,
    material_usage_summary: pd.DataFrame,
    gap: pd.DataFrame,
    validation_issues: pd.DataFrame,
    classification_audit: pd.DataFrame,
) -> None:
    _write_csv(out, DEBUG_OUTPUT_DIR / "10_orders.csv")
    _write_csv(hierarchy, DEBUG_OUTPUT_DIR / "11_hierarchy.csv")
    _write_csv(left_joined.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value=""), DEBUG_OUTPUT_DIR / "12_orders_left.csv")
    _write_csv(manager_input, DEBUG_OUTPUT_DIR / "13_manager_input.csv")
    _write_jsonl(llm_payload, DEBUG_OUTPUT_DIR / "14_manager_input_llm_payload.jsonl")
    _write_jsonl(llm_rows, DEBUG_OUTPUT_DIR / "15_manager_input_llm_result.jsonl")
    _write_csv(option_material_input, DEBUG_OUTPUT_DIR / "16_option_material_input.csv")
    _write_csv(option_kind_master.reindex(columns=OPTION_KIND_MASTER_COLUMNS), DEBUG_OUTPUT_DIR / "21_option_kind_master.csv")
    _write_csv(menu_weight_master, DEBUG_OUTPUT_DIR / "22_menu_weight_master.csv")
    _write_csv(chicken_ratio_master.reindex(columns=CHICKEN_RATIO_MASTER_COLUMNS), DEBUG_OUTPUT_DIR / "26_chicken_ratio_master.csv")
    _write_csv(material_price_master.reindex(columns=MATERIAL_PRICE_MASTER_COLUMNS), DEBUG_OUTPUT_DIR / "23_material_price_master.csv")
    _write_csv(menu_profit_summary.reindex(columns=MENU_PROFIT_SUMMARY_COLUMNS), DEBUG_OUTPUT_DIR / "24_menu_profit_summary.csv")
    _write_csv(manual_profit_rate_master.reindex(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS), DEBUG_OUTPUT_DIR / "27_profit_rate_master.csv")
    _write_csv(manual_profit_summary.reindex(columns=MANUAL_PROFIT_SUMMARY_COLUMNS), DEBUG_OUTPUT_DIR / "28_manual_profit_summary.csv")
    _write_csv(menu_chicken_profile_master.reindex(columns=MENU_CHICKEN_PROFILE_COLUMNS), DEBUG_OUTPUT_DIR / "29_menu_chicken_profile.csv")
    _write_csv(completeness.reindex(columns=COMPLETENESS_COLUMNS), DEBUG_OUTPUT_DIR / "25_completeness.csv")
    _write_csv(menu_weight_input, DEBUG_OUTPUT_DIR / "17_menu_weight_input.csv")
    _write_csv(material_usage_summary, DEBUG_OUTPUT_DIR / "18_material_usage_summary.csv")
    _write_csv(gap.reindex(columns=GAP_COLUMNS), DEBUG_OUTPUT_DIR / "04_product_gap.csv")
    _write_csv(validation_issues.reindex(columns=VALIDATION_ISSUE_COLUMNS), DEBUG_OUTPUT_DIR / "19_validation_issues.csv")
    _write_csv(classification_audit.reindex(columns=CLASSIFICATION_AUDIT_COLUMNS), DEBUG_OUTPUT_DIR / "20_classification_audit.csv")


def _archive_legacy_root_outputs() -> None:
    legacy_names = {
        "04_product_gap.csv",
        "10_orders.csv",
        "11_hierarchy.csv",
        "12_orders_left.csv",
        "13_manager_input.csv",
        "14_manager_input_llm_payload.jsonl",
        "15_manager_input_llm_result.jsonl",
        "16_option_material_input.csv",
        "17_menu_weight_input.csv",
        "18_material_usage_summary.csv",
        "21_option_kind_master.csv",
        "22_menu_weight_master.csv",
        "23_material_price_master.csv",
        "24_menu_profit_summary.csv",
        "25_completeness.csv",
        "25_completeness_baseline.json",
        "26_chicken_ratio_master.csv",
        "27_profit_rate_master.csv",
        "28_manual_profit_summary.csv",
        "19_validation_issues.csv",
        "20_classification_audit.csv",
        "설명.md",
        "입력가이드.md",
        "log.md",
    }
    existing = [NEW_CLS_DIR / name for name in legacy_names if (NEW_CLS_DIR / name).exists()]
    if not existing:
        return
    stamp = pendulum.now("Asia/Seoul").format("YYYYMMDD_HHmmss")
    archive_dir = ARCHIVE_OUTPUT_DIR / f"legacy_csv_{stamp}"
    archive_dir.mkdir(parents=True, exist_ok=True)
    for path in existing:
        shutil.move(str(path), str(archive_dir / path.name))
    logger.warning("legacy 루트 산출물 archive 이동: %s | %d개", archive_dir, len(existing))


def build_orders(
    ym: str | list[str] | tuple[str, ...] | None = None,
    *,
    debug_outputs: bool = False,
    archive_legacy: bool = True,
) -> str:
    _cached_menu_chicken_profile_lookup.cache_clear()
    yms = resolve_yms(ym)
    _backup_manager_input()
    frames = []
    messages = []
    for target_ym in yms:
        df = _all_source_orders(target_ym)
        canonical = df[df.get("_canonical", True).astype(bool)].copy() if not df.empty else df
        if not canonical.empty:
            frames.append(canonical)
        messages.append(f"{target_ym}:{len(canonical)}")
    if not frames and MANUAL_WORKBOOK_OUTPUT_PATH.exists():
        raise RuntimeError(
            "원본 데이터 0건 감지: "
            f"{', '.join(messages)}. 기존 01_수기입력.xlsx를 덮어쓰지 않고 중단합니다."
        )
    if frames:
        canonical_all = pd.concat(frames, ignore_index=True)
        out = canonical_all.reindex(columns=ORDER_OUTPUT_COLUMNS, fill_value="")
        hierarchy = canonical_all.reindex(columns=HIERARCHY_OUTPUT_COLUMNS, fill_value="")
        base_left = canonical_all.reindex(columns=[*UNIFIED_COLUMNS, *HIERARCHY_COLUMNS], fill_value="")
        base_left = _apply_contextual_std_menu_name_corrections(base_left)
        base_left = _apply_std_menu_name_overrides(base_left)
        hierarchy = base_left.reindex(columns=HIERARCHY_OUTPUT_COLUMNS, fill_value="")
        # option_kind는 옵션조합/닭옵션키보다 먼저 정해져야 한다. 그룹 속성이 이걸 쓴다.
        base_left = _attach_option_kind(base_left)
        base_left = _repair_duplicate_main_menu_seq(base_left)
        base_left = _repair_option_parent_to_main(base_left)
        base_left = _repair_deciding_option_parent_to_chicken_main(base_left)
        menu_chicken_profile_master = _build_menu_chicken_profile_master(base_left)
        group_attrs = _build_order_group_attrs(base_left, menu_chicken_profile_master=menu_chicken_profile_master)
        llm_payload = []
        llm_rows = []
        left_joined = _attach_manual_chicken_columns(base_left, group_attrs=group_attrs)
        # 비율표는 신호 있는 주문에서만 뽑으므로 혼합이 생기기 전에 만들어야 한다.
        chicken_ratio_master = _build_chicken_ratio_master(left_joined)
        left_joined = _apply_chicken_ratio(left_joined, chicken_ratio_master)
        left_joined = _attach_chicken_confidence_columns(left_joined)
        left_joined[CLASSIFICATION_RULE_COLUMN] = left_joined.apply(_classification_rule_from_row, axis=1)
        judgement_option_input = _build_judgement_option_input(left_joined)
        left_joined, judgement_option_result = _apply_judgement_options(left_joined, judgement_option_input)
        left_joined = _apply_menu_chicken_profile_final(left_joined, menu_chicken_profile_master)
        left_joined = _repair_option_none_attrs_from_visible_text(left_joined)
        left_joined = _repair_option_none_attrs_by_price(left_joined)
        left_joined = _repair_chicken_split_invariant(left_joined)
        left_joined = _attach_chicken_addon_columns(left_joined)
        order_exception_input = _build_order_exception_input(left_joined)
        left_joined = _apply_order_exception_policy(left_joined, order_exception_input)
        left_joined = _attach_normal_price_column(left_joined)
        left_joined = _attach_cancel_offset_columns(left_joined)
        left_joined = _repair_missing_half_menu_attrs(left_joined)
        left_joined = _repair_main_parent_invariant(left_joined)
        left_joined = _repair_chicken_split_invariant(left_joined)
        # 22번은 확정된 닭 속성과 취소 상계 제외 상태를 키로 쓰므로 예외 처리 뒤에 만든다.
        menu_weight_master = _build_menu_weight_master(left_joined)
        left_joined = _attach_menu_weight_usage_columns(left_joined, menu_weight_master=menu_weight_master)
        left_joined = _attach_option_material_usage_columns(left_joined)
        left_joined = _attach_profit_columns(left_joined)
        left_joined = _attach_profit_sales_base(left_joined)
        manual_profit_rate_master = _build_manual_profit_rate_master(left_joined)
        left_joined = _attach_manual_profit_columns(left_joined, rate_master=manual_profit_rate_master)
        left_joined = _attach_chicken_confidence_columns(left_joined)
        left_joined[CLASSIFICATION_RULE_COLUMN] = left_joined.apply(_classification_rule_from_row, axis=1)
        gap = _build_gap(canonical_all)
    else:
        out = pd.DataFrame(columns=ORDER_OUTPUT_COLUMNS)
        hierarchy = pd.DataFrame(columns=HIERARCHY_OUTPUT_COLUMNS)
        left_joined = pd.DataFrame(columns=LEFT_JOINED_OUTPUT_COLUMNS)
        menu_weight_master = pd.DataFrame(columns=MENU_WEIGHT_MASTER_BASE_COLUMNS)
        chicken_ratio_master = pd.DataFrame(columns=CHICKEN_RATIO_MASTER_COLUMNS)
        menu_chicken_profile_master = pd.DataFrame(columns=MENU_CHICKEN_PROFILE_COLUMNS)
        manual_profit_rate_master = pd.DataFrame(columns=MANUAL_PROFIT_RATE_MASTER_COLUMNS)
        judgement_option_input = pd.DataFrame(columns=JUDGEMENT_OPTION_COLUMNS)
        judgement_option_result = pd.DataFrame()
        order_exception_input = pd.DataFrame(columns=ORDER_EXCEPTION_COLUMNS)
        group_attrs = pd.DataFrame()
        llm_payload = []
        llm_rows = []
        gap = pd.DataFrame(columns=GAP_COLUMNS)
    option_kind_master = _build_option_kind_master(left_joined)
    std_menu_override_input = _build_std_menu_override_input(left_joined)
    manager_input = _guard_manager_input_loss(_build_manager_input(left_joined, group_attrs=group_attrs))
    option_material_input = _build_option_material_input(left_joined)
    material_price_master = _build_material_price_master(menu_weight_master, option_material_input, left_joined)
    menu_profit_summary = _build_menu_profit_summary(left_joined)
    manual_profit_summary = _build_manual_profit_summary(left_joined)
    menu_weight_input = _build_menu_weight_input(left_joined)
    material_usage_summary = _build_material_usage_summary(menu_weight_input)
    classification_audit = _build_classification_audit(
        left_joined,
        option_kind_master=option_kind_master,
        material_price_master=material_price_master,
    )
    _guard_chicken_usage_total(left_joined)
    chicken_usage_total = float(
        pd.to_numeric(
            left_joined[left_joined.get("line_role", pd.Series("", index=left_joined.index)).eq("main")].get(
                CHICKEN_USAGE_TOTAL_COLUMN, pd.Series(dtype=str)
            ),
            errors="coerce",
        ).fillna(0).sum()
    ) if not left_joined.empty else 0.0
    completeness = _apply_completeness_gate(
        _build_completeness(
            left_joined,
            option_kind_master,
            menu_weight_master,
            material_price_master,
            chicken_ratio_master=chicken_ratio_master,
        ),
        chicken_usage_total=chicken_usage_total,
    )
    validation_issues = _build_validation_issues(out, left_joined, gap)
    regression_issues = _completeness_issues(completeness)
    if regression_issues:
        validation_issues = pd.concat(
            [validation_issues, pd.DataFrame(regression_issues)], ignore_index=True, sort=False
        )
    takeout_issues = _takeout_setting_cost_issues(manual_profit_rate_master)
    if takeout_issues:
        validation_issues = pd.concat(
            [validation_issues, pd.DataFrame(takeout_issues)], ignore_index=True, sort=False
        )
    unresolved_issues = _classification_unresolved_issues(left_joined)
    if unresolved_issues:
        validation_issues = pd.concat(
            [validation_issues, pd.DataFrame(unresolved_issues)], ignore_index=True, sort=False
        )
    _guard_left_joined_output_schema(left_joined)
    _write_manual_input_workbook(
        option_kind_master=option_kind_master,
        std_menu_override_input=std_menu_override_input,
        menu_weight_master=menu_weight_master,
        material_price_master=material_price_master,
        chicken_ratio_master=chicken_ratio_master,
        menu_chicken_profile_master=menu_chicken_profile_master,
        manual_profit_rate_master=manual_profit_rate_master,
        judgement_option_input=judgement_option_input,
        manager_input=manager_input,
        option_material_input=option_material_input,
        order_exception_input=order_exception_input,
    )
    _write_csv(left_joined.reindex(columns=LEFT_JOINED_OUTPUT_COLUMNS, fill_value=""), FINAL_ORDERS_OUTPUT_PATH)
    _notify_new_classification_patterns(validation_issues, completeness)
    _write_summary_workbook(
        left_joined=left_joined,
        material_usage_summary=material_usage_summary,
        menu_profit_summary=menu_profit_summary,
        manual_profit_summary=manual_profit_summary,
        completeness=completeness,
        validation_issues=validation_issues,
        judgement_option_result=judgement_option_result,
    )
    _write_text(USAGE_OUTPUT_PATH, _build_compact_usage_text(yms, completeness, validation_issues))
    if debug_outputs:
        _write_debug_outputs(
            out=out,
            hierarchy=hierarchy,
            left_joined=left_joined,
            manager_input=manager_input,
            llm_payload=llm_payload,
            llm_rows=llm_rows,
            option_material_input=option_material_input,
            option_kind_master=option_kind_master,
            menu_weight_master=menu_weight_master,
            chicken_ratio_master=chicken_ratio_master,
            material_price_master=material_price_master,
            menu_profit_summary=menu_profit_summary,
            manual_profit_rate_master=manual_profit_rate_master,
            manual_profit_summary=manual_profit_summary,
            menu_chicken_profile_master=menu_chicken_profile_master,
            completeness=completeness,
            menu_weight_input=menu_weight_input,
            material_usage_summary=material_usage_summary,
            gap=gap,
            validation_issues=validation_issues,
            classification_audit=classification_audit,
        )
    if archive_legacy:
        _archive_legacy_root_outputs()
    if not validation_issues.empty:
        severity = validation_issues.get("severity", pd.Series("", index=validation_issues.index))
        blocking = validation_issues[~severity.astype(str).str.upper().isin(["WARN", "INFO"])]
        warned = validation_issues[severity.astype(str).str.upper().eq("WARN")]
        if not warned.empty:
            logger.warning("입력 대기 항목(차단 아님): %s", warned["issue_type"].value_counts().to_dict())
        if not blocking.empty:
            counts = blocking["issue_type"].value_counts().to_dict()
            raise RuntimeError(f"메뉴계층 검증 실패: {counts}")
    if not completeness.empty:
        logger.info(
            "완결률: %s",
            {str(row["차원"]): f"{row['완결률']}%" for _, row in completeness.iterrows()},
        )
    return "통합 주문서 CSV 저장 완료 | " + ", ".join(messages)


def _build_gap(canonical: pd.DataFrame) -> pd.DataFrame:
    lookup = _ProductLookup()
    gap_rows = []
    if canonical.empty:
        return pd.DataFrame(columns=GAP_COLUMNS)
    for (source, item_id, item_name), group in canonical.groupby(["source", "item_id", "item_name"], dropna=False):
        row = group.iloc[0]
        if _is_non_product_validation_role(row.get("line_role", "")):
            continue
        status = lookup.gap_status(row["source"], row["brand"], row["store"], row["item_id"])
        if not status:
            continue
        gap_rows.append({
            "item_id": item_id,
            "source": source,
            "item_name": item_name,
            "주문건수": int(group["order_id"].nunique()),
            "현재상태": status,
            "추정_수동분류": str(row.get("line_role", "")),
        })
    return pd.DataFrame(gap_rows, columns=GAP_COLUMNS)


def _has_any_token(value: object, tokens: list[str]) -> bool:
    text = str(value or "")
    return any(token in text for token in tokens)


def _unique_nonempty(values: list[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        if value and value not in out:
            out.append(value)
    return out


def _infer_chicken_types(value: object) -> list[str]:
    text = str(value or "")
    if _ROLE_ADDON_RE.search(text):
        return []
    out: list[str] = []
    if "순살" in text or "닭다리살" in text:
        out.append("순살")
    if re.search(r"(^|\s|\[|\(|\+|/)뼈(\s|\]|\)|$|\+|/)|뼈닭", text):
        out.append("뼈닭")
    return out


def _infer_chicken_sizes(value: object) -> list[str]:
    text = str(value or "")
    if _ROLE_ADDON_RE.search(text):
        return []
    stripped = _strip_leading_tags(text)
    search_text = " ".join([text, stripped])
    out: list[str] = []
    if re.search(r"\[\s*소\s*\]|반마리", search_text):
        out.append("소")
    if re.search(r"\[\s*중\s*\]|^\s*한마리(?:\s|\+|$)|3\s*~\s*4\s*인", search_text) and "한마리반" not in search_text and "중간매운맛" not in search_text:
        out.append("중")
    if re.search(r"\[\s*대\s*\]|\[\s*3\s*~\s*6\s*인\s*\]|한마리반|5\s*~\s*6\s*인", search_text):
        out.append("대")
    if out:
        return out
    if re.search(r"(^|\s|\[)\s*1\s*인(?:분)?(\]|\s|$)", search_text):
        out.append("1인")
    if re.search(r"(^|\s|\[)\s*2\s*인(?:분)?(\]|\s|$).{0,10}순살|2\s*인\s*순살|순살.{0,20}2\s*인\s*이상", search_text):
        out.append("2인")
    return out


def _normalize_chicken_group(group: pd.DataFrame) -> pd.Series:
    context_texts = (
        group["menu_name"].astype(str) + " " + group["std_menu_name"].astype(str) + " " + group["item_name"].astype(str)
    ).tolist()
    combined_context = " ".join(context_texts)
    types = _unique_nonempty([item for text in context_texts for item in _infer_chicken_types(text)])
    sizes = _unique_nonempty([item for text in context_texts for item in _infer_chicken_sizes(text)])
    if not types and _has_any_token(combined_context, _CHICKEN_MENU_TOKENS):
        types = ["뼈닭"]
    if not sizes and "곱도리탕" in combined_context:
        sizes = ["중"]

    main = group[group["line_role"].eq("main")]
    parent = main.iloc[0] if len(main) else group.iloc[0]
    main_qty = (
        pd.to_numeric(main["qty"], errors="coerce").fillna(0).max()
        if len(main)
        else pd.to_numeric(group["qty"], errors="coerce").fillna(0).max()
    )
    if not main_qty or pd.isna(main_qty):
        main_qty = 1

    status = "OK"
    reason = ""
    chicken_type = ""
    chicken_size = ""
    if len(types) == 1:
        chicken_type = types[0]
    elif len(types) == 0:
        status = "UNRESOLVED"
        reason = "닭유형 없음"
    else:
        status = "UNRESOLVED"
        reason = "닭유형 충돌: " + "|".join(types)

    if status == "OK":
        if len(sizes) == 1:
            chicken_size = sizes[0]
        elif len(sizes) == 0:
            status = "UNRESOLVED"
            reason = "사이즈 없음"
        else:
            concrete = [size for size in sizes if size in {"소", "중", "대", "1인"}]
            if len(concrete) == 1:
                chicken_size = concrete[0]
            else:
                status = "UNRESOLVED"
                reason = "사이즈 충돌: " + "|".join(sizes)

    usage_per_menu = 0.0
    if status == "OK":
        usage_per_menu = _CHICKEN_USAGE.get((chicken_type, chicken_size), 0.0)
        if usage_per_menu == 0.0:
            status = "UNRESOLVED"
            reason = f"환산표 없음: {chicken_type}/{chicken_size}"

    return pd.Series({
        "sale_date": parent.get("sale_date", ""),
        "ym": parent.get("ym", ""),
        "source": parent.get("source", ""),
        "brand": parent.get("brand", ""),
        "store": parent.get("store", ""),
        "platform": parent.get("platform", ""),
        "order_id": parent.get("order_id", ""),
        "menu_seq": parent.get("menu_seq", ""),
        "parent_item_seq": parent.get("parent_item_seq", ""),
        "parent_item_id": parent.get("item_id", ""),
        "parent_item_name": parent.get("item_name", ""),
        "menu_name": parent.get("menu_name", ""),
        "std_menu_name": parent.get("std_menu_name", ""),
        "menu_qty": float(main_qty),
        "chicken_type": chicken_type,
        "chicken_size": chicken_size,
        "usage_per_menu": usage_per_menu,
        "chicken_usage_total": float(main_qty) * usage_per_menu,
        "status": status,
        "reason": reason,
        "inferred_types": "|".join(types),
        "inferred_sizes": "|".join(sizes),
        "line_count": len(group),
    })


def build_chicken_option_reports(ym: str | list[str] | tuple[str, ...] | None = None) -> str:
    assert_no_model_classification_dependencies()
    orders_path = NEW_CLS_DIR / "12_orders_left.csv"
    if not orders_path.exists():
        raise FileNotFoundError(orders_path)
    orders = _read_csv(orders_path).fillna("")
    yms = set(resolve_yms(ym)) if ym is not None else set(_clean_nan_series(orders.get("ym", pd.Series("", index=orders.index))).unique())
    if yms:
        orders = orders[_clean_nan_series(orders["ym"]).isin(yms)].copy()

    lookup = _ProductLookup()
    product_map, product_join, product_master = _product_table_frames(lookup)
    _write_csv(product_map, NEW_CLS_DIR / "01_product_map.csv")
    _write_csv(product_join, NEW_CLS_DIR / "02_product_join.csv")
    _write_csv(product_master, NEW_CLS_DIR / "03_product_master.csv")

    gap_path = NEW_CLS_DIR / "04_product_gap.csv"
    gap = _read_csv(gap_path).fillna("") if gap_path.exists() else pd.DataFrame(columns=GAP_COLUMNS)
    product_key = ["source", "brand", "store", "item_id"]
    map_attrs = product_map[[c for c in [*product_key, "표준_메뉴명_edit", "수동분류_edit", "대표메뉴"] if c in product_map.columns]].drop_duplicates(product_key, keep="last")
    join_attrs = product_join[[c for c in [*product_key, "standard_menu_name", "category"] if c in product_join.columns]].drop_duplicates(product_key, keep="last")
    lines = orders.merge(map_attrs, on=product_key, how="left").merge(join_attrs, on=product_key, how="left", suffixes=("", "_join")).fillna("")
    for col in ["qty", "unit_price", "total_price"]:
        lines[f"{col}_num"] = pd.to_numeric(lines[col], errors="coerce").fillna(0)

    context = lines[["std_menu_name", "item_name"]].astype(str).agg(" ".join, axis=1)
    lines["is_chicken_context"] = context.map(lambda value: _has_any_token(value, _CHICKEN_MENU_TOKENS))
    group_cols = ["sale_date", "store", "source", "brand", "order_id", "menu_seq"]
    chicken_keys = lines.loc[lines["is_chicken_context"], group_cols].drop_duplicates()
    chicken_lines = lines.merge(chicken_keys, on=group_cols, how="inner")
    if chicken_lines.empty:
        menu_group = pd.DataFrame(columns=CHICKEN_MENU_GROUP_COLUMNS)
        option_summary = pd.DataFrame()
    else:
        menu_group = chicken_lines.groupby(group_cols, sort=False).apply(_normalize_chicken_group).reset_index(drop=True)
        option_lines = chicken_lines[~chicken_lines["line_role"].isin(["fee", "discount"])].copy()
        option_lines["item_chicken_type_hint"] = option_lines["item_name"].map(lambda value: "|".join(_unique_nonempty(_infer_chicken_types(value))))
        option_lines["item_size_hint"] = option_lines["item_name"].map(lambda value: "|".join(_unique_nonempty(_infer_chicken_sizes(value))))
        option_lines["is_review_option"] = option_lines["item_name"].map(lambda value: _has_any_token(value, _REVIEW_TOKENS))
        option_lines["is_fee_like"] = option_lines["item_name"].map(lambda value: _has_any_token(value, _FEE_TOKENS))
        option_summary = (
            option_lines.groupby([
                "source", "brand", "store", "item_id", "item_name", "line_role", "menu_name", "std_menu_name",
                "수동분류_edit", "category", "item_chicken_type_hint", "item_size_hint", "is_review_option", "is_fee_like",
            ], dropna=False)
            .agg(
                order_groups=("order_id", lambda s: option_lines.loc[s.index, group_cols].drop_duplicates().shape[0]),
                qty_sum=("qty_num", "sum"),
                unit_price_sum=("unit_price_num", "sum"),
                total_price_sum=("total_price_num", "sum"),
            )
            .reset_index()
            .sort_values(["qty_sum", "order_groups"], ascending=[False, False])
        )

    size_type_summary = (
        menu_group.groupby(["menu_name", "std_menu_name", "chicken_type", "chicken_size", "status"], dropna=False)
        .agg(order_groups=("order_id", "count"), menu_qty_sum=("menu_qty", "sum"), chicken_usage_sum=("chicken_usage_total", "sum"))
        .reset_index()
        .sort_values(["status", "menu_qty_sum", "order_groups"], ascending=[True, False, False])
        if not menu_group.empty
        else pd.DataFrame(columns=["menu_name", "std_menu_name", "chicken_type", "chicken_size", "status", "order_groups", "menu_qty_sum", "chicken_usage_sum"])
    )
    unresolved = menu_group[menu_group["status"].ne("OK")].copy() if not menu_group.empty else pd.DataFrame(columns=CHICKEN_MENU_GROUP_COLUMNS)
    if not unresolved.empty:
        unresolved = unresolved.sort_values(["reason", "menu_name", "order_id"])
    product_gap_for_chicken = gap[gap["item_name"].map(lambda value: _has_any_token(value, _CHICKEN_MENU_TOKENS))].copy() if not gap.empty else pd.DataFrame(columns=GAP_COLUMNS)

    _write_csv(menu_group.reindex(columns=CHICKEN_MENU_GROUP_COLUMNS), NEW_CLS_DIR / "30_chicken_menu_group.csv")
    _write_csv(option_summary, NEW_CLS_DIR / "31_chicken_option_summary.csv")
    _write_csv(size_type_summary, NEW_CLS_DIR / "32_chicken_size_type_summary.csv")
    _write_csv(unresolved.reindex(columns=CHICKEN_MENU_GROUP_COLUMNS), NEW_CLS_DIR / "33_chicken_unresolved.csv")
    _write_csv(product_gap_for_chicken.reindex(columns=GAP_COLUMNS), NEW_CLS_DIR / "34_product_gap_for_chicken.csv")
    ok_count = int(menu_group["status"].eq("OK").sum()) if not menu_group.empty else 0
    return f"닭도리탕 옵션 리포트 저장 완료 | groups={len(menu_group)} ok={ok_count} unresolved={len(unresolved)}"


def write_reports(ym: str | list[str] | tuple[str, ...] | None = None) -> str:
    messages = []
    for target_ym in resolve_yms(ym):
        messages.append(_write_reports_one(target_ym))
    return " | ".join(messages)


def _write_reports_one(ym: str) -> str:
    lookup = _ProductLookup()
    df = _all_source_orders(ym)
    canonical = df[df.get("_canonical", True).astype(bool)].copy() if not df.empty else df
    if canonical.empty:
        diff = pd.DataFrame(columns=["order_id", "item_seq", "item_name", "menu_name_현재", "menu_name_신규", "menu_seq", "attr_method"])
        summary = pd.DataFrame(columns=["ym", "source", "attr_method", "rows", "orders", "std_covered", "std_total", "total_price", "order_cnt"])
        gap = pd.DataFrame(columns=GAP_COLUMNS)
    else:
        diff_mask = _clean_nan_series(canonical["_menu_name_current"]).ne(_clean_nan_series(canonical["menu_name"]))
        diff = canonical.loc[diff_mask, ["order_id", "item_seq", "item_name", "_menu_name_current", "menu_name", "menu_seq", "attr_method"]].copy()
        diff = diff.rename(columns={"_menu_name_current": "menu_name_현재", "menu_name": "menu_name_신규"})

        tmp = canonical.copy()
        for col in ("total_price", "order_cnt"):
            tmp[col] = pd.to_numeric(tmp[col], errors="coerce").fillna(0)
        summary = tmp.groupby(["ym", "source", "attr_method"], dropna=False).agg(
            rows=("order_id", "size"),
            orders=("order_id", "nunique"),
            std_covered=("std_menu_name", lambda s: int(_clean_nan_series(s).ne("").sum())),
            std_total=("std_menu_name", "size"),
            total_price=("total_price", "sum"),
            order_cnt=("order_cnt", "sum"),
        ).reset_index()
        excluded = df[~df.get("_canonical", True).astype(bool)].groupby(["ym", "source"], dropna=False).size().reset_index(name="canonical_excluded_rows")
        if not excluded.empty:
            summary = summary.merge(excluded, on=["ym", "source"], how="left")
        else:
            summary["canonical_excluded_rows"] = 0
        summary["canonical_excluded_rows"] = pd.to_numeric(summary["canonical_excluded_rows"], errors="coerce").fillna(0).astype(int)

        gap = _build_gap(canonical)

    _write_csv(gap.reindex(columns=GAP_COLUMNS), NEW_CLS_DIR / "04_product_gap.csv")
    _write_csv(diff, NEW_CLS_DIR / f"20_diff_{ym}.csv")
    _write_csv(summary, NEW_CLS_DIR / f"21_summary_{ym}.csv")
    return f"리포트 CSV 저장 완료 | ym={ym} diff={len(diff)} gap={len(gap)}"


def compare_with_unified(ym: str | None = None) -> pd.DataFrame:
    ym = _ensure_ym(ym)
    csv_path = NEW_CLS_DIR / "10_orders.csv"
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)
    test = _read_csv(csv_path)
    test = test[_clean_nan_series(test["ym"]).eq(ym)].copy()
    parts = []
    for path in iter_unified_sales_files():
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            logger.warning("unified parquet 읽기 실패, 스킵: %s | %s", path, exc)
            continue
        if {"store", "ym"}.issubset(df.columns):
            sub = df[_clean_nan_series(df["store"]).eq(TARGET_STORE) & _clean_nan_series(df["ym"]).eq(ym)]
            if not sub.empty:
                parts.append(sub)
    unified = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=UNIFIED_COLUMNS)
    rows = []
    for source in sorted(set(_clean_nan_series(test.get("source", pd.Series(dtype=str)))) | set(_clean_nan_series(unified.get("source", pd.Series(dtype=str))))):
        t = test[_clean_nan_series(test["source"]).eq(source)] if "source" in test.columns else pd.DataFrame()
        u = unified[_clean_nan_series(unified["source"]).eq(source)] if "source" in unified.columns else pd.DataFrame()
        row = {"ym": ym, "source": source, "test_rows": len(t), "unified_rows": len(u)}
        for col in ("total_price", "unit_price", "qty", "discount_amount", "order_cnt"):
            row[f"{col}_test"] = int(pd.to_numeric(t.get(col, pd.Series(dtype=str)), errors="coerce").fillna(0).sum())
            row[f"{col}_unified"] = int(pd.to_numeric(u.get(col, pd.Series(dtype=str)), errors="coerce").fillna(0).sum())
            row[f"{col}_diff"] = row[f"{col}_test"] - row[f"{col}_unified"]
        rows.append(row)
    return pd.DataFrame(rows)


def run_all(ym: str | list[str] | tuple[str, ...] | None = None) -> str:
    yms = resolve_yms(ym)
    results = [
        write_readme(yms),
        build_orders(yms),
    ]
    return " | ".join(results)
