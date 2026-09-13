"""교차분석 전체기간 로컬 재생성과 운영 원본 비교. OneDrive에는 쓰지 않는다."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from modules.transform.pipelines.db import DB_OrderCrossAnalysis as cross
from scripts.order_cross_reference import check_reference

logger = logging.getLogger(__name__)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", default=".tmp/order_cross_validation")
    parser.add_argument("--date", action="append")
    parser.add_argument("--force", action="store_true", help="검증 완료 날짜도 다시 계산")
    args = parser.parse_args(argv)
    root = Path(args.output_root).resolve()
    cross._assert_output_allowed(root, False)
    root.mkdir(parents=True, exist_ok=True)
    inputs, outputs = root / "inputs", root / "outputs"
    inputs.mkdir(exist_ok=True)
    catalog = cross.load_catalog()
    # 검증 중 운영 파일이 갱신되어도 동일 입력으로 재현할 수 있는 로컬 자료.
    for index, (path, expected) in enumerate(catalog.files.items()):
        content = Path(path).read_bytes()
        if hashlib.sha256(content).hexdigest() != expected:
            raise RuntimeError("검증 준비 중 매핑 변경")
        (inputs / f"catalog_{index}.csv").write_bytes(content)
    catalog.files = {str(inputs / f"catalog_{index}.csv"): expected for index, expected in enumerate(catalog.files.values())}
    dates = sorted(set(args.date or cross._dates()))
    results, failures = [], []
    started = time.monotonic()
    summary = {"version": cross.VERSION, "expected_dates": dates, "completed_dates": [], "failures": [], "results": []}
    for index, date in enumerate(dates, 1):
        source = cross._input_path(date)
        try:
            content = source.read_bytes()
            local = inputs / source.name
            local.write_bytes(content)
            before_path = cross._cross_daily_path(date)
            before = pd.read_parquet(before_path) if before_path.exists() else None
            current = not args.force and cross._is_current(date, outputs, catalog, inputs)
            if not current:
                cross._process_order_cross(date, output_root=outputs, input_root=inputs, catalog=catalog)
            bundle = cross.load_validated_support(date, outputs, catalog=catalog, input_root=inputs)
            after = bundle["cross"]
            reference = check_reference(pd.read_parquet(local), bundle)
            entry = {"date": date, "input_hash": hashlib.sha256(content).hexdigest(), "rows": len(after),
                     "old_rows": len(before) if before is not None else None,
                     "quality": bundle["quality"], "independent_reference": reference}
            if before is not None:
                if list(before.columns) != cross.CROSS_COLUMNS:
                    raise ValueError("운영 파일 컬럼 계약 불일치")
                # 옛 빈 프레임은 object 숫자형일 수 있어 비어 있지 않은 운영 파일과 비교한다.
                if len(before) and before.dtypes.astype(str).to_dict() != after.dtypes.astype(str).to_dict():
                    raise ValueError("운영 파일 자료형 불일치")
                merged = before.merge(after, on=cross.UNIQUE_KEY, how="outer", suffixes=("_before", "_after"), indicator=True)
                entry["key_changes"] = {str(k): int(v) for k, v in merged["_merge"].value_counts().items()}
            results.append(entry)
        except Exception as exc:
            logger.exception("검증 실패: %s", date)
            failures.append({"date": date, "error": str(exc)})
        if index % 10 == 0 or index == len(dates):
            logger.info("진행 %d/%d | 성공=%d 실패=%d", index, len(dates), len(results), len(failures))
            summary = {"version": cross.VERSION, "catalog_hash": catalog.fingerprint, "expected_dates": dates,
                       "completed_dates": [r["date"] for r in results], "failures": failures,
                       "elapsed_seconds": round(time.monotonic() - started, 2), "results": results}
            (root / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (root / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = main()
    raise SystemExit(1 if result["failures"] else 0)
