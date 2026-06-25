"""알림 수신자 상수 및 알림 발송 단축 함수."""

# ──个人 이메일 ──────────────────────────────────────
MAIL_CMJ = "a17019@kakao.com"        # 조민준 pm
MAIL_YGS = "siw22222@kakao.com"      # 대표님
MAIL_ONY = "bulu1017@kakao.com"      # 오나영 차장
MAIL_SSJ = "simjeong01@kakao.com"    # 심성준 이사
MAIL_SSJ2 = "simjeong00@kakao.com"   # 심성준 이사 (보조)
MAIL_KDJ = "sanbogaja81@kakao.com"   # 김대진 팀장
MAIL_JYP = "puding83@kakao.com"      # 박진영 차장

# ── 자주 쓰는 수신자 묶음 ────────────────────────────
MAIL_ADMIN = [MAIL_CMJ]
MAIL_CS_TO = [MAIL_SSJ, MAIL_SSJ2]
MAIL_CS_CC = [MAIL_ONY, MAIL_CMJ, MAIL_YGS]

# ── send_telegram re-export ──────────────────────────
from modules.transform.utility.notifier import send_telegram  # noqa: E402
