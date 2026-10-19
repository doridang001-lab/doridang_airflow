"""대표님 전용 작업만 복구한다. 기존 삼전점 작업은 조작하지 않는다."""
import argparse
import json
from pathlib import Path

import watch_samjeon_material_agent as watchdog


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    settings = json.loads(Path(args.config).read_text(encoding='utf-8'))
    if settings.get('assistant_profile') != 'ceo_data':
        raise ValueError('대표님 전용 설정이 필요합니다')
    watchdog.TASK = 'DoridangCeoAssistant'
    watchdog.main(args.config)
