# 스크립트 규칙

## 목적
분석/검증용 단발성 스크립트 (DAG 아님)
결과는 `scripts/output/{name}_{YYYYMMDD_HHMMSS}.json`으로 자동 저장

## 구조
```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts._base import run_script, save_summary

def main(...) -> dict:
    # 비즈니스 로직 작성
    return {"meta": {...}, "summary": {...}, "stats": {...}}

if __name__ == "__main__":
    run_script(lambda: main(...), "script_name")
```

## 규칙
- `main()` 반환: dict (meta/summary/stats 포함)
- `_base.run_script()`로 자동 예외 처리 + JSON 저장
- logger 필수: `logger = logging.getLogger(__name__)`
- 경로는 `paths.py` 상수 사용
- argparse로 필터 옵션 제공

## 참조
- `scripts/_base.py` - run_script, save_summary
- `docs/architecture.md` - 아키텍처
