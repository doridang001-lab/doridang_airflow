# alert.py 생성 (알림 수신자 상수 + send_telegram 통합)

## Task
이메일 수신자 주소가 40+ 파일에 하드코딩되어 분산되어 있어, 새 DAG 작성 시마다 복붙이 필요한 문제를 해결한다.
`modules/transform/utility/alert.py` 하나를 import하면 수신자 상수 + send_telegram 모두 사용 가능하게 통합한다.

## Project Conventions
- logging: `logger = logging.getLogger(__name__)` — print 절대 금지
- import 경로: `from modules.transform.utility.XXX import ...` (절대경로)
- 스케줄 상수: `from modules.transform.utility.schedule import ...` (하드코딩 금지)
- 경로 상수: `from modules.transform.utility.paths import ...` (하드코딩 금지)

## Files to Create / Modify

- **생성**: `C:\airflow\modules\transform\utility\alert.py`
- **변경 없음**: `notifier.py`, `mailer.py`, 기존 DAG 파일들

## Implementation Steps

1. `C:\airflow\modules\transform\utility\alert.py` 파일 생성

```python
"""알림 수신자 상수 및 알림 발송 단축 함수."""

# ── 개인 이메일 ──────────────────────────────────────
MAIL_CMJ = "a17019@kakao.com"        # 민준
MAIL_SIW = "siw22222@kakao.com"      # 대표님
MAIL_ONA = "bulu1017@kakao.com"      # 오나영 차장
MAIL_SIM = "simjeong01@kakao.com"    # 심성준 이사
MAIL_SIM2 = "simjeong00@kakao.com"   # 심성준 이사 (보조)
MAIL_KDJ = "sanbogaja81@kakao.com"   # 김대진 팀장
MAIL_PUD = "puding83@kakao.com"      # 마케팅

# ── 자주 쓰는 수신자 묶음 ────────────────────────────
MAIL_ADMIN = [MAIL_CMJ]
MAIL_CS_TO = [MAIL_SIM, MAIL_SIM2]
MAIL_CS_CC = [MAIL_ONA, MAIL_CMJ, MAIL_SIW]

# ── send_telegram re-export ──────────────────────────
from modules.transform.utility.notifier import send_telegram  # noqa: E402
```

## Reference Code

### notifier.py (핵심 부분)
```python
# C:\airflow\modules\transform\utility\notifier.py

_ALERT_EMAILS = ["a17019@kakao.com"]

def _get_telegram_creds() -> tuple[str, str]:
    try:
        from airflow.models import Variable
        token = Variable.get("TELEGRAM_BOT_TOKEN", default_var="")
        chat_id = Variable.get("TELEGRAM_CHAT_ID", default_var="")
        return token, chat_id
    except Exception:
        return "", ""

def send_telegram(text: str) -> None:
    token, chat_id = _get_telegram_creds()
    if not token or not chat_id:
        logger.warning("Telegram credentials missing; skip send")
        return
    try:
        payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=10):
            pass
        logger.info("Telegram alert sent")
    except Exception as e:
        logger.warning("Telegram send failed (ignored): %s", e)
```

## Test Cases

1. **상수 import 확인**
   ```
   python -c "from modules.transform.utility.alert import MAIL_CMJ, MAIL_ADMIN; print(MAIL_CMJ, MAIL_ADMIN)"
   ```
   → 기대: `a17019@kakao.com ['a17019@kakao.com']`

2. **send_telegram re-export 확인**
   ```
   python -c "from modules.transform.utility.alert import send_telegram; print(send_telegram)"
   ```
   → 기대: `<function send_telegram at 0x...>` (ImportError 없음)

3. **전체 상수 목록 확인**
   ```
   python -c "import modules.transform.utility.alert as a; print([x for x in dir(a) if x.startswith('MAIL')])"
   ```
   → 기대: `['MAIL_ADMIN', 'MAIL_CMJ', 'MAIL_CS_CC', 'MAIL_CS_TO', 'MAIL_KDJ', 'MAIL_ONA', 'MAIL_PUD', 'MAIL_SIM', 'MAIL_SIM2', 'MAIL_SIW']`

## Verification Loop

```
LOOP until all PASS:
  1. Test Cases 1~3 순서대로 실행
  2. FAIL 항목 → 원인 분석
  3. 코드 수정
  4. 전체 Test Cases 재실행
종료 조건: 전체 PASS + Constraints 위반 없음
```

## Constraints

- `notifier.py` 수정 금지 — send_telegram 구현은 원본 유지
- `mailer.py` 수정 금지
- 기존 DAG 파일 이메일 하드코딩 마이그레이션 금지 (이번 작업 범위 밖)
- 새 이메일 주소 추가 금지 — 위 6명 + 보조 1명만
- `from __future__ import annotations` 불필요 (상수 파일)

## Do Not Ask — Decide Yourself
- 파일이 이미 존재하면: 덮어쓰기
- import가 모호하면: Reference Code의 패턴 따라가기
- 타입 힌트 여부: 없음 (상수 파일)
- 주석 여부: 이름/역할 주석만 (위 코드 그대로)
- 변수명 스타일: UPPER_SNAKE_CASE (상수이므로)
