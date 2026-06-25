try:
    from _base import save_summary
except ModuleNotFoundError:
    from scripts._base import save_summary

import argparse

from modules.transform.pipelines.db.DB_FinProduct_Map import llm_product_map


def main(dry_run: bool = False, limit: int | None = None) -> dict:
    summary = llm_product_map(dry_run=dry_run, limit=limit)
    save_summary("product_map_llm", summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="LLM 호출과 CSV 저장 없이 대상만 확인")
    parser.add_argument("--limit", type=int, default=None, help="처리할 미매핑 항목 수 제한")
    args = parser.parse_args()
    main(dry_run=args.dry_run, limit=args.limit)
