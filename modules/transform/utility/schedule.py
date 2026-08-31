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
SMP_BSP_KPI_TIME         = "0 11 * * 1,2"          # 매주 월·화 11:00 실행 (주간 KPI 통합 + 미입력 리마인드)
SMP_DAG_MONITORING_TIME = "0 15 * * *"  # 매일 15:00 실행 (KST)
SMP_HARNESS_MONITORING_TIME = "0 13 * * *"  # 매일 13:00 실행 (KST)
AIRFLOW_SCHEDULE_GUARD_TIME = "*/10 * * * *"  # 10분마다 스케줄 생성 지연 점검
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
SMP_FLOW_COLLECT_TIME       = "7,37 8,12,14,16,19,21 * * *"  # 슬롯 가드로 08:07/12:37/14:37/16:37/19:37/21:37 6회만 통과
# Sales_FlowVisit_01_Mart_Dags는 수집 완료 트리거로 구동한다(스케줄 없음)
SMP_SNS_SNAPSHOT_TIME       = "0 4 * * *"   # 매일 04:00 (SNS 팔로워/친구수 스냅샷)



DB_COUPANG_MACRO_TIME = "30,50 * * * *"  # 매시 30분, 50분 실행 (KST)
DB_UNIFIED_SALES_TIME        = "40 7 * * *"  # 매일 07:40 실행 (08:10 전 완료 목표)
DB_UNIFIED_SALES_GUARD_TIME  = "15 8 * * *"  # 매일 08:15 실행 (07:40 미생성 감시/보정)
DB_ORDER_CROSS_ANALYSIS_TIME = "0 9 * * *"  # 매일 09:00 실행 (전일 lookback/검증)
DB_DAILY_CORPORATE_STORE_REPORT_TIME = "50 8 * * 1-5"  # 월~금 08:50 실행 (전일 직영점 보고 mart)
DB_ITEM_MASTER_TIME          = "30 9 * * *" # 매일 09:30 실행 (UnifiedSales 빌드 후)
DB_COLLECTION_COMPARE_TIME   = "20 8,12,15 * * *"  # 매일 08:20, 12:20, 15:20 실행
DB_DELIVERY_COMMISSION_TIME  = "10 8,12,14,16,19,21 * * *"  # 매일 08:10, 12:10, 14:10, 16:10, 19:10, 21:10 실행
DB_FIN_PRODUCT_TIME          = "0 10 * * *"  # 매일 10:00 실행 (UnifiedSales 및 상품 map 완료 후)
DB_FIN_PRODUCT_MAP_TIME      = "30 9 * * *"  # 매일 09:30 실행 (UnifiedSales 및 가드 이후)
DB_POSFEED_SALES_TIME        = "15 3 * * *"  # 매일 03:15 실행
DB_POSFEED_SALES_DETAIL_TIME = "45 7 * * *"  # 매일 07:45 실행
DB_OKPOS_SALES_TIME          = "10 6 * * *"  # 매일 06:10 실행 (1차 OKPOS 수집)
DB_OKPOS_SALES_RECHECK_TIME  = None  # 수동 실행 전용 (OKPOS 누락/0원 보정)
DB_OKPOS_PRODUCT_TIME        = "40 6 * * *" # 매일 06:40 실행 (OKPOS 상품조회 엑셀)
DB_FOOD_GUIDE_ORDERS_TIME    = "0 5 * * *"  # 매일 05:00 실행 (Food Guide 주문 예정 목록 월별 parquet)
DB_EASYPOS_SALES_TIME        = "45 4 * * *"  # 매일 04:45 실행
DB_UNIONPOS_RECEIPT_TIME     = "20 6 * * *"  # 매일 06:20 실행
DB_OKPOS_SALES_TODAY_TIME            = "0 8,12,14,16,19,21 * * *"   # 당일 매출 목표 15분 전 통합용 원천 수집
DB_EASYPOS_SALES_TODAY_TIME          = "5 8,12,14,16,19,21 * * *"   # 당일 매출 5분 스태거
DB_UNIONPOS_RECEIPT_TODAY_TIME       = "10 8,12,14,16,19,21 * * *"  # 당일 매출 10분 스태거
DB_POSFEED_SALES_TODAY_TIME          = "15 8,12,14,16,19,21 * * *"  # 당일 매출 15분 스태거
DB_OKPOS_REVIEW_CARD_TODAY_TIME      = "25 12-21 * * *"  # 당일 OKPOS 카드 승인 25분 스태거
DB_UNIFIED_SALES_TODAY_TIME = "15,45 8,12,14,16,19,21 * * *"  # Today 통합 슬롯은 DAG 내부 gate로 제한
DB_UNIFIED_SALES_TODAY_TRIGGER_TIME  = DB_UNIFIED_SALES_TODAY_TIME  # 호환용
DB_UNIFIED_SALES_TODAY_TRIGGER_1915_TIME = None  # 호환용: Today DAG 하나로 통합

AI_DAILY_COLLECTION_TIME = "20 7 * * *"  # 매일 07:20 실행 (KST)

SMD_TOORDER_SALES_REPORT_TIME = "5 6 * * *"  # 매일 6:05 실행
BSP_DAANGN_ADS_TIME = "0 12 * * 1-5"  # 매주 월~금 12:00 실행 (당근 광고 CSV 통합)
BSP_MARKETING_ADS_MART_TIME = "15 7 * * *"  # 매일 7:15 실행 (당근 병합 12:00 / Flow 수집 이후)

DB_TOORDER_STORE_PLATFORM_TIME = "45 6 * * *"  # 매일 06:45 실행 (UnifiedSales 07:40 이전 완료)

DB_UNIFIED_REVIEW_TIME         = "33 7 * * *"  # 매일 07:33 실행 (ToOrderVoc Transform 7:30 이후)

SMD_BAEMIN_COLLECT_BATCH1_TIME = "15 0 * * *"  # 매일 KST 00:15 실행
SMD_BAEMIN_COLLECT_BATCH2_TIME = "15 6 * * *"  # 매일 KST 06:15 실행
SMD_BAEMIN_COLLECT_BATCH3_TIME = "15 9 * * *"  # 매일 KST 09:15 실행
SMD_BAEMIN_COLLECT_TIME = SMD_BAEMIN_COLLECT_BATCH1_TIME
SMD_BAEMIN_UPLOAD_TIME = "20 7 * * *"  # 매일 KST 07:20 실행 (UnifiedSales 07:40 이전 완료)
SMD_BAEMIN_UPLOAD_PC2_TIME = "5,35 * * * *"  # 30분 간격 종일 하위 PC 도착분 확인

SMP_MORNING_BRIEFING_TIME = "50 6 * * 1,2,3,4,5"  # 매주 월~금 06:47 실행 (KST)

SMD_STORE_SALES_TIME = "10 9 * * *"  # 매일 09:10 실행 (POS 수집 완료 후)
DB_HALL_SALES_TARGET_TIME = "0 11 * * 1,2,3,4,5"  # 매주 월~금 11:00 (DB_UnifiedSales grp 갱신 완료 후)

DB_TOORDER_MENU_TIME = "0 7 * * *"  # 매일 07:00 실행 (메뉴별 판매량 분석)
DB_TOORDER_MENU_LLM_TIME = "30 8 * * *"  # ToOrder menu 수집 완료 후 LLM 분석

# 매일 04:20 실행 (배민 수집 배치 00:15/06:15/09:15 사이 유휴 시간대)
DB_STORAGE_CLEANUP_TIME = "20 4 * * *"


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
