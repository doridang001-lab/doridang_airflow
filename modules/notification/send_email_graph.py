"""
Microsoft Graph API를 사용한 이메일 전송 모듈
Outlook/Office 365 계정을 통해 HTTP 방식으로 이메일을 발송합니다.

Airflow Connection 설정:
------------------------
Connection ID: msgraph_outlook
Connection Type: HTTP
Host: https://graph.microsoft.com
Login: <발신자 이메일 주소, 예: admin@doridang.com>
Extra: {
    "tenant_id": "849b5035-01a1-48ed-9a23-5bb83c6d0873",
    "client_id": "428db174-7fdf-44fe-9c3d-b032360cf695",
    "client_secret": "<Azure에서 생성한 Client Secret>"
}

from modules.notification.send_email_graph import send_email_graph

사용 예시:
---------
# 1. 기본 HTML 이메일 발송
send_email_graph(
    conn_id='msgraph_outlook',
    to=['recipient@example.com'],
    subject='테스트 메일',
    html_content='<h1>안녕하세요</h1><p>테스트 메일입니다.</p>'
)

# 2. 여러 수신자에게 발송
send_email_graph(
    conn_id='msgraph_outlook',
    to=['user1@example.com', 'user2@example.com'],
    subject='공지사항',
    html_content='<p>중요 공지사항입니다.</p>'
)

# 3. Airflow Task에서 사용
def send_report(**context):
    df_html = context['task_instance'].xcom_pull(key='df_html')
    send_email_graph(
        conn_id='msgraph_outlook',
        to=['a17019@kakao.com'],
        subject='일일 리포트',
        html_content=f'<p>오늘의 데이터:</p>{df_html}'
    )
"""

import requests
import json
from airflow.hooks.base import BaseHook


def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """
    OAuth 2.0 Client Credentials Flow로 Access Token 발급
    
    Parameters:
    -----------
    tenant_id : str
        Azure AD 테넌트 ID
    client_id : str
        앱 등록의 애플리케이션(클라이언트) ID
    client_secret : str
        Azure Portal에서 생성한 클라이언트 암호
    
    Returns:
    --------
    str
        Access Token
    """
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default"
    }
    
    response = requests.post(token_url, data=data)
    
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception(f"토큰 발급 실패: {response.text}")


def send_email_graph(conn_id: str, to: list, subject: str, html_content: str, cc: list = None, bcc: list = None):
    """
    Microsoft Graph API를 사용하여 이메일을 전송합니다.
    
    Parameters:
    -----------
    conn_id : str
        Airflow Connection ID (msgraph_outlook 등)
        - Connection Type: HTTP
        - Host: https://graph.microsoft.com
        - Login: 발신자 이메일 주소
        - Extra: {"tenant_id": "...", "client_id": "...", "client_secret": "..."}
    to : list
        받는 사람 이메일 주소 리스트
    subject : str
        이메일 제목
    html_content : str
        HTML 형식의 이메일 본문
    cc : list, optional
        참조 이메일 주소 리스트
    bcc : list, optional
        숨은 참조 이메일 주소 리스트
    
    Returns:
    --------
    dict
        성공 시 {'success': True, 'message': '이메일 발송 완료'}
        실패 시 {'success': False, 'error': '에러 메시지'}
    """
    try:
        # Airflow Connection에서 설정 가져오기
        connection = BaseHook.get_connection(conn_id)
        base_url = connection.host  # https://graph.microsoft.com
        sender_email = connection.login  # 발신자 이메일
        
        # Extra 필드에서 Azure 설정 추출
        extra = connection.extra_dejson if hasattr(connection, 'extra_dejson') else json.loads(connection.extra or '{}')
        
        # Access Token 발급
        tenant_id = extra.get('tenant_id')
        client_id = extra.get('client_id')
        client_secret = extra.get('client_secret')
        
        if not all([tenant_id, client_id, client_secret]):
            raise ValueError("Connection의 Extra 필드에 tenant_id, client_id, client_secret이 모두 설정되어야 합니다.")
        
        # OAuth 2.0으로 Access Token 발급
        print(f"🔑 Access Token 발급 중...")
        access_token = get_access_token(tenant_id, client_id, client_secret)
        print(f"✅ Access Token 발급 완료")
        
        # 받는 사람 형식 변환
        recipients = [{"emailAddress": {"address": email}} for email in to]
        
        # 이메일 메시지 구성
        email_msg = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": html_content
                },
                "toRecipients": recipients
            },
            "saveToSentItems": "true"
        }
        
        # CC 추가
        if cc:
            email_msg["message"]["ccRecipients"] = [{"emailAddress": {"address": email}} for email in cc]
        
        # BCC 추가
        if bcc:
            email_msg["message"]["bccRecipients"] = [{"emailAddress": {"address": email}} for email in bcc]
        
        # Microsoft Graph API 호출
        # Application 권한 사용 시: /users/{sender_email}/sendMail
        url = f"{base_url}/v1.0/users/{sender_email}/sendMail"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(url, headers=headers, json=email_msg)
        
        # 응답 확인
        if response.status_code == 202:  # Accepted
            print(f"✅ 이메일 발송 성공: {', '.join(to)}")
            return {'success': True, 'message': '이메일 발송 완료'}
        else:
            error_msg = response.text
            print(f"❌ 이메일 발송 실패: {error_msg}")
            return {'success': False, 'error': error_msg, 'status_code': response.status_code}
            
    except Exception as e:
        error_msg = f"이메일 발송 중 오류 발생: {str(e)}"
        print(f"❌ {error_msg}")
        return {'success': False, 'error': error_msg}
