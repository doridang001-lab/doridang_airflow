"""스크립트 공통 베이스: sys.path 설정, summary.json 저장, 예외 처리."""
import json
import logging
import sys
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUTPUT_DIR = PROJECT_ROOT / "scripts" / "output"
KST = timezone(timedelta(hours=9))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def save_summary(name: str, data: dict[str, Any]) -> Path:
    """
    data를 scripts/output/{name}_{YYYYMMDD_HHMMSS}.json 으로 저장.

    Args:
        name: 파일명 접두어 (예: "onedrive_summary")
        data: 저장할 dict (meta/summary/stats 포함)

    Returns:
        저장된 파일 경로
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"{name}_{ts}.json"
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("결과 저장: %s", out_path)
    return out_path


def run_script(main_fn: Callable[[], dict[str, Any]], script_name: str) -> None:
    """
    main_fn을 실행하고 예외 발생 시 status=error summary를 자동 저장.

    Args:
        main_fn: 실행할 함수 (dict 반환)
        script_name: 파일명 접두어
    """
    import time
    start = time.time()
    try:
        result = main_fn()
        result.setdefault("meta", {})["duration_sec"] = round(time.time() - start, 2)
        out = save_summary(script_name, result)
        print(out)
    except Exception:
        tb = traceback.format_exc()
        logger.error("스크립트 실행 실패:\n%s", tb)
        data = {
            "meta": {
                "script": script_name,
                "run_at": datetime.now(KST).isoformat(),
                "duration_sec": round(time.time() - start, 2),
                "status": "error",
            },
            "summary": {"result": "FAIL"},
            "issues": [{"type": "exception", "traceback": tb}],
            "stats": {},
        }
        out = save_summary(script_name, data)
        print(out)
        sys.exit(1)
