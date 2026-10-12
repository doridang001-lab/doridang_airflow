"""로컬 계산·진단·검수 반영·조회 명령. 게시나 메시지 발송은 수행하지 않는다."""
import argparse
import logging
from pathlib import Path

from modules.transform.pipelines.db.samjeon_material_agent.classification import import_review
from modules.transform.pipelines.db.samjeon_material_agent.contracts import json_bytes
from modules.transform.pipelines.db.samjeon_material_agent.mart import (
    DEFAULT_CLASSIFICATION, DEFAULT_COVERAGE, DEFAULT_SETTINGS, DEFAULT_WORK, _aggregate_tables,
    _check_source_versions, load_purchases, load_sales_lines, plan_build, save_plan,
    stage_aggregate, stage_purchases, stage_sales, validate_snapshot,
)
from modules.transform.pipelines.db.samjeon_material_agent.quality import import_coverage, quality_daily
from modules.transform.pipelines.db.samjeon_material_agent.query import INTENTS, query_snapshot
from modules.transform.pipelines.db.samjeon_material_agent.sources import aggregate_sales

logger = logging.getLogger(__name__)


def _local_path(value: str | Path) -> Path:
    path = Path(value).resolve()
    if any("onedrive" in part.lower() for part in path.parts) or str(path).startswith("/opt/airflow/analytics"):
        raise ValueError("이 명령의 쓰기 대상은 로컬 경로여야 합니다")
    return path


def main(argv=None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("build", "doctor", "review-import", "coverage-import"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--date")
        cmd.add_argument("--date-from")
        cmd.add_argument("--date-to")
        cmd.add_argument("--classification", type=Path, default=DEFAULT_CLASSIFICATION)
        cmd.add_argument("--coverage", type=Path, default=DEFAULT_COVERAGE)
        cmd.add_argument("--settings", type=Path, default=DEFAULT_SETTINGS)
        if name == "build":
            cmd.add_argument("--work-root", type=Path, default=DEFAULT_WORK)
            cmd.add_argument("--dry-run", action="store_true")
        if name.endswith("-import"):
            cmd.add_argument("--review", type=Path, required=True)
        if name == "review-import":
            cmd.add_argument("--version", required=True)
    query = sub.add_parser("query")
    query.add_argument("--snapshot", type=Path, required=True)
    query.add_argument("--intent", choices=sorted(INTENTS), default="summary")
    query.add_argument("--date-from")
    query.add_argument("--date-to")
    query.add_argument("--evidence-id")
    query.add_argument("--product-key")
    query.add_argument("--source")
    query.add_argument("--offset", type=int, default=0)
    query.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(argv)
    if args.command == "query":
        return query_snapshot(args.snapshot, intent=args.intent, date_from=args.date_from, date_to=args.date_to,
                              evidence_id=args.evidence_id, product_key=args.product_key, source=args.source,
                              offset=args.offset, limit=args.limit)
    work = _local_path(args.work_root) if args.command == "build" else DEFAULT_WORK
    plan = plan_build(args.date, date_from=args.date_from, date_to=args.date_to, work_root=work,
                      classification_path=args.classification, coverage_path=args.coverage, settings_path=args.settings)
    if args.command == "review-import":
        return import_review(args.review, _local_path(args.classification), version=args.version,
                             expected_hash=plan["config_hashes"]["classification"])
    if args.command == "build" and not args.dry_run:
        reference = save_plan(plan)
        stage_sales(plan)
        stage_purchases(plan)
        stage_aggregate(plan)
        return {**validate_snapshot(plan), "stage_dir": plan["stage_dir"], "plan": reference, "published": False}
    sales_lines, _ = load_sales_lines(plan)
    purchases = load_purchases(plan)
    if args.command == "coverage-import":
        quality = quality_daily(plan, sales_lines, purchases)
        _check_source_versions(plan)
        return import_coverage(args.review, _local_path(args.coverage), expected_hash=plan["config_hashes"]["coverage"],
                               current_quality=quality)
    tables, context = _aggregate_tables(plan, aggregate_sales(sales_lines), purchases, sales_lines)
    _check_source_versions(plan)
    return {"snapshot_id": plan["snapshot_id"], "status": context["status"], "context": context,
            "rows": {key: len(value) for key, value in tables.items()}, "files_written": False, "published": False}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("%s", json_bytes(main()).decode("utf-8"))
