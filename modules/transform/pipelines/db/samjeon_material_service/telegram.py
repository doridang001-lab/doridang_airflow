"""Telegram Bot API 전용 전송기. 토큰·원문 URL을 오류 로그에 남기지 않는다."""
import json
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import uuid


class DeliveryError(Exception):
    def __init__(self, uncertain=True, code=None):
        super().__init__('전송 결과 확인 필요' if uncertain else f'Telegram 요청 거절({code})')
        self.uncertain = uncertain


class Telegram:
    def __init__(self, token):
        self.token = token

    def call(self, method, payload):
        headers = {'Content-Type': 'application/json'}
        if method == 'sendDocument' and isinstance(payload.get('document'), Path):
            boundary = 'Samjeon' + uuid.uuid4().hex
            body = bytearray()
            for k, v in payload.items():
                body.extend(f'--{boundary}\r\n'.encode())
                if k == 'document':
                    body.extend(b'Content-Disposition: form-data; name="document"; filename="samjeon-report.pdf"\r\nContent-Type: application/pdf\r\n\r\n')
                    body.extend(v.read_bytes())
                else:
                    body.extend(f'Content-Disposition: form-data; name="{k}"\r\n\r\n{v}'.encode('utf-8'))
                body.extend(b'\r\n')
            body.extend(f'--{boundary}--\r\n'.encode())
            data = bytes(body)
            headers = {'Content-Type': f'multipart/form-data; boundary={boundary}'}
        else:
            data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        try:
            request = Request(f'https://api.telegram.org/bot{self.token}/{method}', data=data, headers=headers)
            with urlopen(request, timeout=40) as response:
                answer = json.loads(response.read())
            if not answer.get('ok'):
                raise DeliveryError(False, answer.get('error_code'))
            return answer['result']
        except HTTPError as exc:
            raise DeliveryError(exc.code >= 500, exc.code) from None
        except (OSError, URLError, ValueError):
            raise DeliveryError(True) from None
