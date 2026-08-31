"""FinProduct 규칙 제안을 검토하고 승인된 경우에만 활성 규칙으로 승격한다."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from modules.transform.pipelines.db.DB_FinProduct_Rules import promote_rule_proposal


logger = logging.getLogger(__name__)


def review_proposal(path: Path, *, apply: bool = False) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    report = payload.get("change_report") if isinstance(payload, dict) else None
    rules = payload.get("rules") if isinstance(payload, dict) else None
    if not isinstance(report, dict) or not isinstance(rules, list):
        raise ValueError(f"잘못된 규칙 제안 파일: {path}")

    summary = {
        "proposal_path": str(path),
        "old_active_count": int(report.get("old_active_count") or 0),
        "new_active_count": int(report.get("new_active_count") or 0),
        "active_change_ratio": float(report.get("active_change_ratio") or 0),
        "added_active_count": len(report.get("added_active_rules") or []),
        "removed_active_count": len(report.get("removed_active_rules") or []),
        "proposal_sha256": str(report.get("proposal_sha256") or ""),
        "applied": False,
    }
    if apply:
        summary.update(promote_rule_proposal(path))
        summary["applied"] = True
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FinProduct 규칙 변경 제안 검토")
    parser.add_argument("proposal", type=Path, help="검토할 제안 JSON 경로")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="승인된 제안을 OneDrive 활성 규칙에 반영합니다. 사용자 승인 후에만 사용하세요.",
    )
    return parser.parse_args()


def main() -> dict:
    args = _parse_args()
    result = review_proposal(args.proposal, apply=args.apply)
    logger.info("FinProduct 규칙 제안 검토 결과: %s", json.dumps(result, ensure_ascii=False))
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
