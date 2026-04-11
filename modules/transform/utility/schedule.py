"""
스케줄 상수 모음
DAG에서 사용하는 cron 표현식과 이메일 설정을 관리
"""

# ============================================================
# DAG 스케줄 (cron 표현식)
# ============================================================

SMD_ORDERS_TIME      = "15 15 * * 1"    # 매주 월요일 15:15 실행
SMD_VISIT_LOG        = "0 12 * * 1,3,5" # 매주 월,수,금 12:00 실행

SMP_TOORDER_VOC_TIME = "30 9 * * *"     # 매일 09:30 실행
SMP_FDAM_CS_TIME     = "5 9 * * *"      # 매일 09:05 실행

SMP_DELIVERY_ALERT_TIME = "40 9 * * 1,2,3,4,5"  # 매주 월~금 09:40 실행
SMP_DAG_MONITORING_TIME = "30 16 * * *"  # 매일 16:30 실행 (KST)
SMP_POLICY_TIME  = "0 10 * * *"   # 매일 10:00 실행 (KST)
SMP_CHICKEN_PRICE_TIME  = "10 9 * * *"   # 매일 09:10 실행 (KST)

# 정책 수집 DAG 전용 스케줄 (5분 간격, 비중첩)
SMP_POLICY_BAEMIN_TIME      = "0 10 * * *"   # 매일 10:00
SMP_POLICY_COUPANG_TIME     = "5 10 * * *"   # 매일 10:05
SMP_POLICY_YOGIYO_TIME      = "10 10 * * *"  # 매일 10:10
SMP_POLICY_DDANGYO_TIME     = "15 10 * * *"  # 매일 10:15
SMP_POLICY_BAEDALTTEUK_TIME = "20 10 * * *"  # 매일 10:20
SMP_POLICY_MUKKEBI_TIME     = "25 10 * * *"  # 매일 10:25
SMP_POLICY_BAEDALEUM_TIME   = "30 10 * * *"  # 매일 10:30
SMP_POLICY_NAVERPLACE_TIME  = "35 10 * * *"  # 매일 10:35
SMP_POLICY_CONSOLIDATE_TIME = "45 10 * * *"  # 매일 10:45 (수집 완료 후 집계)



DB_POSFEED_SALES_TIME        = "15 9 * * *"  # 매일 09:15 실행
DB_POSFEED_SALES_DETAIL_TIME = "45 9 * * *"  # 매일 09:45 실행
DB_OKPOS_SALES_TIME          = "35 9 * * *"  # 매일 09:35 실행

SMD_TOORDER_SALES_REPORT_TIME = "0 9 * * *"  # 매일 09:00 실행


# ============================================================
# SMD_07 이메일 발송 제어
# ============================================================

SMD_07_EMAIL_TEST_MODE = False # True로 설정 시 테스트 이메일 수신자에게만 발송
SMD_07_EMAIL_TEST_RECIPIENTS = ["a17019@kakao.com", "bulu1017@kakao.com"]
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
