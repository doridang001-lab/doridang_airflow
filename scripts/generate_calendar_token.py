"""
Google Calendar OAuth2 token 생성 스크립트 (최초 1회 실행)

실행 방법 (Windows, .venv 활성화 후):
    python scripts/generate_calendar_token.py

브라우저가 열리면 Google 계정으로 로그인 → 권한 허용
→ config/calendar_token.json 생성됨

이후 Airflow 컨테이너에서 refresh_token으로 자동 갱신됨.
"""

from pathlib import Path

SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]
CREDENTIALS_PATH = Path(__file__).parent.parent / "config" / "credentials.json"
TOKEN_PATH = Path(__file__).parent.parent / "config" / "calendar_token.json"


def main():
    from google_auth_oauthlib.flow import InstalledAppFlow

    if not CREDENTIALS_PATH.exists():
        print(f"오류: credentials.json 없음 — {CREDENTIALS_PATH}")
        return

    flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
    creds = flow.run_local_server(port=0, open_browser=True)

    TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
    print(f"token.json 저장 완료: {TOKEN_PATH}")
    print("이제 Strategy_MorningBriefing_Dags에서 Calendar 일정을 수집할 수 있습니다.")


if __name__ == "__main__":
    main()
