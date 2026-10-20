"""배포에 포함되는 UTF-8 프롬프트. 불변 실행 정책과 학습 문맥을 구분한다."""
from pathlib import Path

ROOT = Path(__file__).with_name('prompts')


def read_prompt(name):
    if name not in ('conversation', 'daily', 'data', 'schedule', 'report',
                    'learning', 'sharing', 'development', 'validation'):
        raise ValueError('prompt_unknown')
    return (ROOT / (name + '.md')).read_text(encoding='utf-8').strip()
