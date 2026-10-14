"""운영 값은 별도 로컬 설정에서 읽고 원천 경로는 읽기 전용으로 사용한다."""
from dataclasses import dataclass, fields
from pathlib import Path
import json
import os
import re


def local_path(value) -> Path:
    path = Path(value).resolve()
    if any('onedrive' in p.lower() for p in path.parts) or str(path).replace('\\', '/').startswith((
        '/opt/airflow/analytics', '/opt/airflow/Repository')):
        raise ValueError('서비스 쓰기 경로는 OneDrive 밖의 로컬 경로여야 합니다')
    return path


@dataclass(frozen=True)
class Settings:
    state_root: str = '.tmp/samjeon-service'
    snapshot_root: str = ''
    snapshot: str = ''
    enabled: bool = False
    auto_send: bool = False
    allowed_users: tuple = ()
    allowed_chats: tuple = ()
    recipient: int | None = None
    token_env: str = 'SAMJEON_TELEGRAM_BOT_TOKEN'
    ai_enabled: bool = False
    ai_verified: bool = False
    codex_executable: str = 'codex'
    codex_backend: str = 'windows'
    wsl_distribution: str = ''
    wsl_user: str = ''
    wsl_codex_executable: str = ''
    model_reasoning_effort: str = 'medium'
    model: str = 'gpt-6-astra'
    font_path: str = 'C:/Windows/Fonts/malgun.ttf'
    notification_start: str = '09:30'
    notification_end: str = '21:00'
    poll_seconds: int = 60

    @classmethod
    def load(cls, filename):
        filename = Path(filename).resolve()
        raw = json.loads(filename.read_text(encoding='utf-8'))
        if not isinstance(raw, dict) or set(raw) - {f.name for f in fields(cls)}:
            raise ValueError('서비스 설정 키 오류')
        for key in ('state_root', 'snapshot_root', 'snapshot', 'font_path'):
            value = raw.get(key, getattr(cls(), key))
            if value:
                p = Path(value)
                raw[key] = str((filename.parent / p).resolve() if not p.is_absolute() else p.resolve())
        for key in ('allowed_users', 'allowed_chats'):
            values = raw.get(key, [])
            if not isinstance(values, list) or any(type(v) is not int for v in values):
                raise ValueError(f'{key}는 정수 ID 배열이어야 합니다')
            raw[key] = tuple(values)
        settings = cls(**raw)
        settings.validate()
        return settings

    def validate(self):
        local_path(self.state_root)
        for key in ('enabled', 'auto_send', 'ai_enabled', 'ai_verified'):
            if type(getattr(self, key)) is not bool:
                raise ValueError(f'{key}는 boolean이어야 합니다')
        if self.recipient is not None and (type(self.recipient) is not int or self.recipient not in self.allowed_chats):
            raise ValueError('자동 안내 수신자는 허용된 개인 대화여야 합니다')
        if any(type(v) is not int or v <= 0 for v in (*self.allowed_users, *self.allowed_chats)):
            raise ValueError('개인 대화·사용자 ID는 양의 정수여야 합니다')
        if self.auto_send and self.recipient is None:
            raise ValueError('자동 안내 수신자를 등록하세요')
        if self.model != 'gpt-6-astra':
            raise ValueError('이 서비스의 승인 모델은 gpt-6-astra입니다')
        if self.codex_backend not in ('windows', 'wsl') or self.model_reasoning_effort != 'medium':
            raise ValueError('Codex 실행 방식 또는 추론 수준 오류')
        if self.codex_backend == 'wsl':
            for value in (self.wsl_distribution, self.wsl_user):
                if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}', value):
                    raise ValueError('WSL 배포판과 사용자를 명시하세요')
            value = self.wsl_codex_executable
            if (not isinstance(value, str) or not value.startswith('/') or value.startswith('/mnt/')
                    or any(c in value for c in ('\x00', '\n', '\r', '\\')) or '..' in value.split('/')):
                raise ValueError('WSL 안에 설치한 Linux Codex 절대경로가 필요합니다')
        if not re.fullmatch(r'[A-Z][A-Z0-9_]+', self.token_env):
            raise ValueError('토큰 환경변수 이름 오류')
        for t in (self.notification_start, self.notification_end):
            if not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d', t):
                raise ValueError('알림 시각은 HH:MM이어야 합니다')
        if self.notification_start >= self.notification_end:
            raise ValueError('알림 시간 범위 오류')
        if type(self.poll_seconds) is not int or not 10 <= self.poll_seconds <= 3600:
            raise ValueError('스냅샷 확인 간격은 10~3600초입니다')

    @property
    def token(self):
        return os.environ.get(self.token_env, '')

    def ready(self):
        self.validate()
        if not self.enabled or not self.token or not self.allowed_users or not self.allowed_chats:
            raise ValueError('서비스 활성화·전용 봇 환경변수·사용자·개인 대화 등록이 필요합니다')
