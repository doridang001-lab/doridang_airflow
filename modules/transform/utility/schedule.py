"""
스케줄 상수 모음
DAG에서 사용하는 cron 표현식과 이메일 설정을 관리
"""

# ============================================================
# DAG 스케줄 (cron 표현식)
# ============================================================

SMD_ORDERS_TIME      = "33 16 * * 1,5"    # 매주 월요일 15:15 실행
SMD_VISIT_LOG        = "0 12 * * 1,3,5" # 매주 월,수,금 12:00 실행

SMP_TOORDER_VOC_TIME = "30 9 * * *"     # 매일 09:30 실행
SMP_FDAM_CS_TIME     = "5 7 * * *"      # 매일 07:05 실행

SMP_DELIVERY_ALERT_TIME  = "5 9 * * 1,2,3,4,5"  # 매주 월~금 09:00 실행
SMP_CLOSING_RATE_TIME    = "0 7 * * 1"             # 매주 월요일 07:00 실행
SMP_DAG_MONITORING_TIME = "0 15 * * *"  # 매일 15:00 실행 (KST)
SMP_POLICY_TIME  = "0 8 * * *"   # 매일 08:00 실행 (KST)
SMP_CHICKEN_PRICE_TIME  = "0 9 * * *"   # 매일 09:00 실행 (KST)

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



DB_COUPANG_MACRO_TIME = "0 * * * *"  # 매시간 실행 (쿠팡이츠 매크로 적재)
DB_UNIFIED_SALES_TIME        = "40 8 * * *"  # 매일 08:40 실행 (POS 수집 완료 후)
DB_ITEM_MASTER_TIME          = "30 9 * * *" # 매일 09:30 실행 (UnifiedSales 빌드 후)
DB_COLLECTION_COMPARE_TIME   = "47 7 * * *"  # 매일 07:47 실행 (수집 비교 마트)
DB_FIN_PRODUCT_TIME          = "35 8 * * *" # 매일 08:35 실행 (OKPOS Product 완료 후)
DB_FIN_PRODUCT_MAP_TIME      = "50 8 * * *" # 매일 08:50 실행 (UnifiedSales 갱신 후 송파삼전점 상품 표준화)
DB_POSFEED_SALES_TIME        = "15 7 * * *"  # 매일 07:15 실행
DB_POSFEED_SALES_DETAIL_TIME = "45 7 * * *"  # 매일 07:45 실행
DB_OKPOS_SALES_TIME          = "15 7 * * *"  # 매일 07:15 KST 실행
DB_OKPOS_PRODUCT_TIME        = "20 8 * * *" # 매일 08:20 실행 (OKPOS 상품조회 엑셀)
DB_EASYPOS_SALES_TIME        = "50 7 * * *"  # 매일 07:50 실행
DB_UNIONPOS_RECEIPT_TIME     = "55 7 * * *"  # 매일 07:55 실행
DB_LAUNCH_TIME               = "0 15 * * *"   # 매일 15:00 KST 실행 (송파삼전점 Launch 수집)
DB_LAUNCH_UNIFIED_TIME       = "15 15 * * *"  # 매일 15:15 KST 실행 (송파삼전점 Launch 집계)

AI_DAILY_COLLECTION_TIME = "20 7 * * *"  # 매일 07:20 실행 (KST)

SMD_TOORDER_SALES_REPORT_TIME = "5 6 * * *"  # 매일 6:05 실행

DB_TOORDER_STORE_PLATFORM_TIME = "10 7 * * *"  # 매일 07:10 실행 (SMD_TOORDER_SALES_REPORT 이후)

DB_UNIFIED_REVIEW_TIME         = "49 7 * * *"  # 매일 07:49 실행 (ToOrderVOC 완료 기준으로 직전 실행 추적)

SMD_BAEMIN_COLLECT_TIME = "10 3 * * *"  # 매일 KST 07:15 실행 (토더 매출 수집 완료 후)

SMP_MORNING_BRIEFING_TIME = "50 6 * * 1,2,3,4,5"  # 매주 월~금 06:47 실행 (KST)

SMD_STORE_SALES_TIME = "0 9 * * *"  # 매일 09:00 실행 (POS 수집 완료 후)

DB_HALL_SALES_TARGET_TIME = "0 11 * * 1"  # 매주 월요일 11:00 (DB_UnifiedSales grp 갱신 완료 후)

DB_TOORDER_MENU_TIME = "0 7 * * *"  # 매일 07:00 실행 (메뉴별 판매량 분석)
DB_TOORDER_MENU_LLM_TIME = "30 8 * * *"  # ToOrder menu 수집 완료 후 LLM 분석


# ============================================================
# SMD_07 이메일 발송 제어
# ============================================================

SMD_07_EMAIL_TEST_MODE = True # True로 설정 시 테스트 이메일 수신자에게만 발송
SMD_07_EMAIL_TEST_RECIPIENTS = ["a17019@kakao.com"]
SMD_07_EMAIL_DEV_CC_IN_PROD = True



# ============================================================
# 이메일 정리
# ============================================================
# ['bulu1017@kakao.com', 'a17019@kakao.com', 'siw22222@kakao.com']
# siw22222@kakao.com 대표님 이메일
# bulu1017@kakao.com 오나영 차장
# a17019@kakao.com 관리자 이메일
# "simjeong01@kakao.com", "simjeong00@kakao.com" 심성준 이사
# sanbogaja81@kakao.com 김대진 팀장
