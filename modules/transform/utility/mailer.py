"""
이메일 발송 함수
Airflow SMTP Connection을 통한 이메일 발송 유틸리티
"""


def text_to_html(text):
    """일반 텍스트를 HTML로 변환"""
    text = text.replace('\n', '<br>')
    return f"""<html>
<head><meta charset="UTF-8"></head>
<body style="font-family: 'Malgun Gothic', Arial, sans-serif; margin: 20px; line-height: 1.6;">
<div style="background: #f8f9fa; padding: 20px; border-radius: 5px; border-left: 4px solid #27ae60;">
{text}
</div>
<p style="color: #999; font-size: 12px; margin-top: 20px;">이 메일은 자동으로 발송되었습니다.</p>
</body>
</html>"""


def send_email(
    subject,
    html_content,
    to_emails,
    conn_id='doridang_conn_smtp_gmail',
    **context
):
    """이메일 발송 (Airflow SMTP Connection 사용)"""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from airflow.hooks.base import BaseHook

    connection = BaseHook.get_connection(conn_id)
    assert connection.host and connection.port and connection.login and connection.password, \
        f"SMTP 연결 설정 불완전: {conn_id}"
    smtp_host: str = connection.host
    smtp_port: int = connection.port
    smtp_user: str = connection.login
    smtp_password: str = connection.password
    from_email = connection.extra_dejson.get('from_email') or smtp_user

    to_list = [to_emails] if isinstance(to_emails, str) else to_emails

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ', '.join(to_list)
    msg.attach(MIMEText(html_content, 'html', 'utf-8'))

    try:
        print(f"📧 이메일 발송 시작... 수신: {to_list}, 제목: {subject}")
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        print(f"✅ 이메일 발송 성공: {len(to_list)}명")
        return f"이메일 발송 완료: {len(to_list)}명"
    except Exception as e:
        print(f"❌ 이메일 발송 실패: {e}")
        raise
