"""
스케줄 상수 모음
DAG에서 사용하는 cron 표현식과 이메일 설정을 관리
"""

# ============================================================
# DAG 스케줄 (cron 표현식)
# ============================================================

SMD_ORDERS_TIME      = "33 16 * * 1,5"    # 매주 월요일 15:15 실행
SMD_VISIT_LOG        = "0 12 * * 1,3,5" # 매주 월,수,금 12:00 실행

SMP_TOORDER_VOC_TIME = "30 7 * * *"     # 매일 07:30 실행
SMP_FDAM_CS_TIME     = "5 7 * * *"      # 매일 07:05 실행

SMP_DELIVERY_ALERT_TIME  = "5 9 * * 1,2,3,4,5"  # 매주 월~금 09:00 실행
SMP_CLOSING_RATE_TIME    = "0 7 * * 1"             # 매주 월요일 07:00 실행
SMP_DAG_MONITORING_TIME = "0 15 * * *"  # 매일 15:00 실행 (KST)
SMP_HARNESS_MONITORING_TIME = "0 13 * * *"  # 매일 13:00 실행 (KST)
SMP_POLICY_TIME  = "0 8 * * *"   # 매일 08:00 실행 (KST)
SMP_CHICKEN_PRICE_TIME  = "0 9 * * *"   # 매일 09:00 실행 (KST)
SMP_SUSAM_REPORT_TIME = "0 9 * * *"  # 매일 09:00 실행 (전일 상품 매출 Flow 업로드)

# 정책 수집 DAG 전용 스케줄 (5분 간격, 비중첩)
SMP_POLICY_BAEMIN_TIME      = "0 8 * * *"   # 매일 08:00
SMP_POLICY_COUPANG_TIME     = "5 8 * * *"   # 매일 08:05
SMP_POLICY_YOGIYO_TIME      = "10 8 * * *"  # 매일 08:10
SMP_POLICY_DDANGYO_TIME     = "15 8 * * *"  # 매일 08:15
SMP_POLICY_BAEDALTTEUK_TIME = "20 8 * * *"  # 매일 08:20
SMP_POLICY_MUKKEBI_TIME     = "25 8 * * *"  # 매일 08:25
SMP_POLICY_BAEDALEUM_TIME   = "30 8 * * *"  # 매일 08:30
SMP_POLICY_NAVERPLACE_TIME  = "35 8 * * *"  # 매일 08:35
SMP_POLICY_CONSOLIDATE_TIME = "45 8 * * *"  # 매일 08:45 (수집 완료 후 집계)



DB_COUPANG_MACRO_TIME = "30,50 * * * *"  # 매시 30분, 50분 실행 (KST)
DB_UNIFIED_SALES_TIME        = "37 8 * * *"  # 매일 08:37 실행 (전일 lookback/검증)
DB_UNIFIED_SALES_GUARD_TIME  = "5 9 * * *"  # 매일 09:05 실행 (08:37 미생성 감시/보정)
DB_ORDER_CROSS_ANALYSIS_TIME = "0 9 * * *"  # 매일 09:00 실행 (전일 lookback/검증)
DB_DAILY_CORPORATE_STORE_REPORT_TIME = "50 8 * * 1-5"  # 월~금 08:50 실행 (전일 직영점 보고 mart)
DB_ITEM_MASTER_TIME          = "30 9 * * *" # 매일 09:30 실행 (UnifiedSales 빌드 후)
DB_COLLECTION_COMPARE_TIME   = "20 8,12,15 * * *"  # 매일 11:50, 14:50 실행 (DB_UnifiedSales 10분 전)
DB_DELIVERY_COMMISSION_TIME  = "10 8 * * *"  # 매일 08:10 실행
DB_FIN_PRODUCT_TIME          = "35 8 * * *" # 매일 08:35 실행 (OKPOS Product 완료 후)
DB_FIN_PRODUCT_MAP_TIME      = "20 8 * * *"  # 매일 08:20 실행 (fin_product mart 생성 전)
DB_POSFEED_SALES_TIME        = "15 3 * * *"  # 매일 03:15 실행
DB_POSFEED_SALES_DETAIL_TIME = "45 7 * * *"  # 매일 07:45 실행
DB_OKPOS_SALES_TIME          = "10 6 * * *"  # 매일 06:10 실행 (1차 OKPOS 수집)
DB_OKPOS_SALES_RECHECK_TIME  = None  # 수동 실행 전용 (OKPOS 누락/0원 보정)
DB_OKPOS_PRODUCT_TIME        = "40 6 * * *" # 매일 06:40 실행 (OKPOS 상품조회 엑셀)
DB_EASYPOS_SALES_TIME        = "45 4 * * *"  # 매일 04:45 실행
DB_UNIONPOS_RECEIPT_TIME     = "20 6 * * *"  # 매일 06:20 실행
DB_OKPOS_SALES_TODAY_TIME            = "0 8,12,14,16,19,21 * * *"   # 당일 매출 목표 15분 전 통합용 원천 수집
DB_EASYPOS_SALES_TODAY_TIME          = "5 8,12,14,16,19,21 * * *"   # 당일 매출 5분 스태거
DB_UNIONPOS_RECEIPT_TODAY_TIME       = "10 8,12,14,16,19,21 * * *"  # 당일 매출 10분 스태거
DB_POSFEED_SALES_TODAY_TIME          = "15 8,12,14,16,19,21 * * *"  # 당일 매출 15분 스태거
DB_OKPOS_REVIEW_CARD_TODAY_TIME      = "25 12-21 * * *"  # 당일 OKPOS 카드 승인 25분 스태거
DB_UNIFIED_SALES_TODAY_TRIGGER_TIME  = "45 8,12,14,16,21 * * *"  # 09/13/15/17/22시 목표 15분 전 통합
DB_UNIFIED_SALES_TODAY_TRIGGER_1915_TIME = "15 19 * * *"  # 19:30 목표 15분 전 통합

AI_DAILY_COLLECTION_TIME = "20 7 * * *"  # 매일 07:20 실행 (KST)

SMD_TOORDER_SALES_REPORT_TIME = "5 6 * * *"  # 매일 6:05 실행

DB_TOORDER_STORE_PLATFORM_TIME = "10 7 * * *"  # 매일 07:10 실행 (SMD_TOORDER_SALES_REPORT 이후)

DB_UNIFIED_REVIEW_TIME         = "33 7 * * *"  # 매일 07:33 실행 (ToOrderVoc Transform 7:30 이후)

SMD_BAEMIN_COLLECT_BATCH1_TIME = "15 3 * * *"  # 매일 KST 03:15 실행
SMD_BAEMIN_COLLECT_BATCH2_TIME = "30 4 * * *"  # 매일 KST 04:30 실행
SMD_BAEMIN_COLLECT_BATCH3_TIME = "45 5 * * *"  # 매일 KST 05:45 실행
SMD_BAEMIN_COLLECT_TIME = SMD_BAEMIN_COLLECT_BATCH1_TIME
SMD_BAEMIN_UPLOAD_TIME = "40 7 * * *"  # 매일 KST 07:40 실행 (중앙 PC 전용)

SMP_MORNING_BRIEFING_TIME = "50 6 * * 1,2,3,4,5"  # 매주 월~금 06:47 실행 (KST)

SMD_STORE_SALES_TIME = "10 9 * * *"  # 매일 09:10 실행 (POS 수집 완료 후)

DB_HALL_SALES_TARGET_TIME = "0 11 * * *"  # 매일 월요일 11:00 (DB_UnifiedSales grp 갱신 완료 후)

DB_TOORDER_MENU_TIME = "0 7 * * *"  # 매일 07:00 실행 (메뉴별 판매량 분석)
DB_TOORDER_MENU_LLM_TIME = "30 8 * * *"  # ToOrder menu 수집 완료 후 LLM 분석


# ============================================================
# SMD_07 이메일 발송 제어
# ============================================================

SMD_07_EMAIL_TEST_MODE = True # True로 설정 시 테스트 이메일 수신자에게만 발송
from modules.transform.utility.mail_recipients import MAIL_CMJ_PM

SMD_07_EMAIL_TEST_RECIPIENTS = [MAIL_CMJ_PM]
SMD_07_EMAIL_DEV_CC_IN_PROD = True



# ============================================================
# 이메일 정리
# ============================================================
# 대표님: MAIL_CEO
# 오나영 차장: MAIL_OH_NAYOUNG
# 조민준 PM: MAIL_CMJ_PM
# 심성준 이사: MAIL_SIM_SUNGJUN_1 / MAIL_SIM_SUNGJUN_2
# 김대진 팀장: MAIL_KIM_DAEJIN
