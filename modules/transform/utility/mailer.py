"""
메일 발송 유틸리티.
Airflow SMTP Connection을 통해 HTML 메일과 inline image를 함께 전송한다.
"""

import logging

logger = logging.getLogger(__name__)


def text_to_html(text):
    """일반 텍스트를 HTML로 변환한다."""
    text = text.replace("\n", "<br>")
    return f"""<html>
<head><meta charset="UTF-8"></head>
<body style="font-family: 'Malgun Gothic', Arial, sans-serif; margin: 20px; line-height: 1.6;">
<div style="background: #f8f9fa; padding: 20px; border-radius: 5px; border-left: 4px solid #27ae60;">
{text}
</div>
<p style="color: #999; font-size: 12px; margin-top: 20px;">본 메일은 자동으로 발송되었습니다.</p>
</body>
</html>"""


def send_email(
    subject,
    html_content,
    to_emails,
    conn_id="doridang_conn_smtp_gmail",
    attachments=None,
    inline_images=None,
    cc_emails=None,
    **context,
):
    """Airflow SMTP Connection을 사용해 메일을 발송한다."""
    del context

    import smtplib
    import time
    from email.mime.application import MIMEApplication
    from email.mime.image import MIMEImage
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    from airflow.hooks.base import BaseHook

    connection = BaseHook.get_connection(conn_id)
    assert connection.host and connection.port and connection.login and connection.password, (
        f"SMTP 연결 설정 불완전: {conn_id}"
    )
    smtp_host = connection.host
    smtp_port = int(connection.port)
    smtp_user = connection.login
    smtp_password = connection.password
    extra = connection.extra_dejson
    from_email = extra.get("from_email") or smtp_user
    timeout = int(extra.get("timeout") or 30)
    retry_limit = max(1, int(extra.get("retry_limit") or 1))

    to_list = [to_emails] if isinstance(to_emails, str) else list(to_emails)
    cc_list = [cc_emails] if isinstance(cc_emails, str) else list(cc_emails or [])
    attachments = list(attachments or [])
    inline_images = list(inline_images or [])

    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)

    alternative_part = MIMEMultipart("alternative")
    alternative_part.attach(MIMEText(html_content, "html", "utf-8"))
    msg.attach(alternative_part)

    for image in inline_images:
        if not image:
            continue
        data = image.get("data")
        if not data:
            continue
        mime_subtype = image.get("mime_subtype") or "png"
        content_id = str(image.get("content_id") or "").strip()
        filename = image.get("filename") or f"inline-image.{mime_subtype}"

        image_part = MIMEImage(data, _subtype=mime_subtype)
        if content_id:
            image_part.add_header("Content-ID", f"<{content_id}>")
        image_part.add_header("Content-Disposition", "inline", filename=filename)
        msg.attach(image_part)

    for attachment in attachments:
        if not attachment:
            continue
        data = attachment.get("data")
        if data is None:
            continue
        filename = attachment.get("filename") or "attachment.bin"
        mime_subtype = attachment.get("mime_subtype") or "octet-stream"
        attachment_part = MIMEApplication(data, _subtype=mime_subtype)
        attachment_part.add_header("Content-Disposition", "attachment", filename=filename)
        msg.attach(attachment_part)

    last_exc = None
    for attempt in range(1, retry_limit + 1):
        try:
            logger.info("메일 발송 시작: 수신=%s, 제목=%s, 시도=%d/%d", to_list, subject, attempt, retry_limit)
            if smtp_port == 465:
                # SSL-only 포트: 처음부터 SSL 연결 (starttls 불필요)
                with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=timeout) as server:
                    server.login(smtp_user, smtp_password)
                    server.send_message(msg)
            else:
                # STARTTLS 포트 (587 등)
                with smtplib.SMTP(smtp_host, smtp_port, timeout=timeout) as server:
                    server.ehlo()
                    server.starttls()
                    server.ehlo()
                    server.login(smtp_user, smtp_password)
                    server.send_message(msg)
            logger.info("메일 발송 성공: %d명", len(to_list))
            return f"메일 발송 완료: {len(to_list)}명"
        except Exception as exc:
            last_exc = exc
            if attempt >= retry_limit:
                logger.error("메일 발송 실패: %s", exc)
                raise
            wait_sec = min(30, attempt * 5)
            logger.warning("메일 발송 실패 후 재시도 대기: %s (wait=%ss)", exc, wait_sec)
            time.sleep(wait_sec)

    raise last_exc
