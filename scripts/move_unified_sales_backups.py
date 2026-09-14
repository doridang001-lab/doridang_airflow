"""Move unified_sales in-place backup parquet files out of the sales glob.

Default mode is dry-run. Pass --apply to move files into unified_sales_grp/_backup.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.transform.pipelines.db.DB_UnifiedSales_common import UNIFIED_ROOT  # noqa: E402


def _target_path(path: Path, backup_dir: Path) -> Path:
    target = backup_dir / f"backup_{path.name}"
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    idx = 1
    while True:
        candidate = backup_dir / f"{stem}.{idx}{suffix}"
        if not candidate.exists():
            return candidate
        idx += 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="실제 백업 parquet를 _backup 폴더로 이동합니다.")
    args = parser.parse_args()

    if not UNIFIED_ROOT.exists():
        print(f"root_missing={UNIFIED_ROOT}")
        return 0

    backups = sorted(path for path in UNIFIED_ROOT.glob("unified_sales_*.parquet") if ".bak_" in path.name)
    real = sorted(path for path in UNIFIED_ROOT.glob("unified_sales_*.parquet") if ".bak_" not in path.name)
    backup_dir = UNIFIED_ROOT / "_backup"

    print(f"root={UNIFIED_ROOT}")
    print(f"real_files={len(real)} backup_files={len(backups)}")
    if backups:
        print("sample=" + ",".join(path.name for path in backups[:10]))

    if args.apply and backups:
        backup_dir.mkdir(parents=True, exist_ok=True)
        for path in backups:
            shutil.move(str(path), str(_target_path(path, backup_dir)))
        print(f"moved={len(backups)} target_dir={backup_dir}")
    else:
        print("moved=0 dry_run=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
