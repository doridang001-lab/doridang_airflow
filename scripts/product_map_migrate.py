try:
    from _base import save_summary
except ModuleNotFoundError:
    from scripts._base import save_summary

import argparse

from modules.transform.pipelines.db.DB_FinProduct_Map import migrate_product_map


def main(dry_run: bool = False) -> dict:
    summary = migrate_product_map(dry_run=dry_run)
    save_summary("product_map_migrate", summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="CSV 저장 없이 대상과 요약만 확인")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
