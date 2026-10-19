"""목적별 월간 CSV 게시와 검증된 읽기 계약."""
from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import uuid
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from modules.transform.pipelines.db.samjeon_material_agent.analytics import build_context, period_result
from modules.transform.pipelines.db.samjeon_material_agent.contracts import RULE_VERSION, SCHEMA_VERSION, atomic_json, digest
from modules.transform.pipelines.db.samjeon_material_agent.settings import validate_settings


TABLE_FILES = {
    "sales_lines": "매출근거", "purchase_lines": "매입근거",
    "sales_daily": "매출집계", "purchase_daily": "매입집계",
    "item_daily": "매입품목", "quality_daily": "데이터상태",
    "review_candidates": "분류검토", "coverage_candidates": "수집검토",
}
DATE_COLUMNS = {
    "sales_lines": "sale_date", "sales_daily": "sale_date",
    "purchase_lines": "purchase_date", "purchase_daily": "purchase_date",
    "item_daily": "purchase_date", "quality_daily": "date",
    "review_candidates": None, "coverage_candidates": "date",
}
SUMMARY_COLUMNS = {
    "date_to": "날짜", "status": "자료상태", "observed_sales_won": "관측매출원",
    "hall_sales_won": "홀매출원", "observed_purchase_won": "관측원재료포장재매입원",
    "ingredient_won": "원재료매입원", "packaging_won": "포장재매입원",
    "staff_meal_won": "직원식대원", "other_won": "기타매입원",
    "unclassified_won": "미분류매입원", "unclassified_row_count": "미분류행수",
    "sales_complete": "매출마감완료", "purchase_complete": "매입마감완료",
    "purchase_rate_pct": "확정매입률퍼센트",
    "provisional_purchase_rate_pct": "잠정매입률퍼센트",
    "reasons": "상태사유",
}
SOURCE_STATUS_COLUMNS = {"sales": "매출원천상태", "food_guide": "푸드가이드상태",
                         "logistics_dam": "물류담상태"}

EVIDENCE_FILES = {
    "sales_lines.parquet": "각 월의 YYYY_MM_매출근거.csv",
    "purchase_lines.parquet": "각 월의 YYYY_MM_매입근거.csv",
    "quality_daily.parquet": "각 월의 YYYY_MM_데이터상태.csv",
    "issues.json": "각 월의 YYYY_MM_데이터상태.csv",
}


def csv_evidence(value):
    if isinstance(value, dict):
        return {key: csv_evidence(item) for key, item in value.items()}
    if isinstance(value, list):
        return [csv_evidence(item) for item in value]
    return EVIDENCE_FILES.get(value, value) if isinstance(value, str) else value


def month_name(day: str) -> str:
    return day[:7].replace("-", "_")


def file_name(month: str, table: str) -> str:
    return f"{month}_{TABLE_FILES[table]}.csv"


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    frame.to_csv(path, index=False, encoding="utf-8-sig", lineterminator="\n")


def _write_check(path: Path, rows: list[tuple[str, str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("구분", "이름", "값"))
        writer.writerows(rows)


def stage_months(plan: dict, stage: Path, tables: dict, context: dict) -> list[str]:
    """검증된 계산 결과에서 월 전체 또는 기준일까지의 CSV를 로컬에 구성한다."""
    start, end = date.fromisoformat(plan["start_date"]), date.fromisoformat(plan["as_of_date"])
    if start.day != 1:
        raise ValueError("월간 CSV 계산 시작일은 월 초여야 합니다")
    months = []
    cursor = start
    while cursor <= end:
        month = month_name(str(cursor))
        month_first = str(cursor)
        next_month = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_last = str(min(end, next_month - timedelta(days=1)))
        folder = stage / "monthly" / month
        folder.mkdir(parents=True, exist_ok=True)
        quality_rows = tables["quality_daily"].loc[
            tables["quality_daily"]["date"].between(month_first, month_last)]
        selected = {}
        if not plan.get("summary_only_enabled", False):
            for name in TABLE_FILES:
                frame = tables[name]
                column = DATE_COLUMNS[name]
                if column is None:
                    unknown = tables["purchase_lines"].loc[
                        tables["purchase_lines"]["purchase_date"].between(month_first, month_last)
                        & tables["purchase_lines"]["category"].eq("미분류")]
                    amounts = unknown.groupby("product_key").agg(
                        amount_won=("amount_won", "sum"),
                        absolute_amount_won=("amount_won", lambda values: int(values.abs().sum())),
                        row_count=("evidence_id", "size"))
                    selected[name] = frame.loc[frame["product_key"].isin(amounts.index)].copy()
                    for field in ("amount_won", "absolute_amount_won", "row_count"):
                        selected[name][field] = selected[name]["product_key"].map(amounts[field]).astype("int64")
                else:
                    selected[name] = frame.loc[frame[column].between(month_first, month_last)].copy()
                _write_csv(folder / file_name(month, name), selected[name])
        summary = []
        current = cursor
        while str(current) <= month_last:
            day = str(current)
            result = period_result(day, day, tables, context["settings"])
            row = {label: ("; ".join(result[key]) if key == "reasons" else result[key])
                   for key, label in SUMMARY_COLUMNS.items()}
            quality = quality_rows.loc[quality_rows["date"].eq(day)]
            pending = []
            for source, label in SOURCE_STATUS_COLUMNS.items():
                matches = quality.loc[quality["source"].eq(source)]
                status = str(matches.iloc[0]["status"]) if len(matches) == 1 else "누락"
                row[label] = status
                if status != "정상":
                    reason = str(matches.iloc[0]["reason"]) if len(matches) == 1 else "원천 상태 행 없음"
                    pending.append(f"{source}: {reason}")
            row["확인필요"] = "; ".join(pending)
            summary.append(row)
            current += timedelta(days=1)
        _write_csv(folder / f"{month}.csv", pd.DataFrame(
            summary, columns=[*SUMMARY_COLUMNS.values(), *SOURCE_STATUS_COLUMNS.values(), "확인필요"]))
        meta = {
            "format": "monthly_csv_v3" if plan.get("summary_only_enabled", False) else "monthly_csv_v1",
            "month": month, "as_of_date": month_last,
            "window_start": month_first, "schema_version": str(SCHEMA_VERSION),
            "rule_version": RULE_VERSION,
            "implementation_hash": plan["implementation_hash"],
            "guide_sha256": plan["guide_sha256"],
            "classification_version": context["classification_version"],
            "status": period_result(month_first, month_last, tables, context["settings"])["status"],
        }
        rows = [("metadata", key, str(value)) for key, value in meta.items()]
        for key, value in context["settings"].items():
            rows.append(("settings", key, "" if value is None else str(value)))
        for key, value in plan["config_hashes"].items():
            rows.append(("config_hash", key, value))
        for entry in plan["sales_files"] + plan["food_files"] + plan["dam_files"]:
            if entry["date"].replace("-", "")[:6] == month.replace("_", ""):
                rows.append(("source_hash", entry["source_ref"], entry["sha256"]))
        if selected:
            for name, frame in selected.items():
                rows.append(("rows", name, str(len(frame))))
                for column, dtype in frame.dtypes.items():
                    rows.append(("dtype", f"{name}.{column}", str(dtype)))
            for file in sorted(folder.glob("*.csv")):
                rows.append(("sha256", file.name, digest(file)))
        else:
            rows.append(("rows", "daily_summary", str(len(summary))))
            rows.append(("sha256", f"{month}.csv", digest(folder / f"{month}.csv")))
        _write_check(folder / f"{month}_검증.csv", rows)
        months.append(month)
        cursor = next_month
    return months


def _read_check(folder: Path) -> dict:
    month = folder.name
    if len(month) != 7 or month[4] != "_" or not month[:4].isdigit() or not month[5:].isdigit():
        raise ValueError("월 폴더 이름 오류")
    path = folder.parent / "_internal" / month / f"{month}_검증.csv"
    if not path.is_file():
        path = folder / f"{month}_검증.csv"
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows or any(set(row) != {"구분", "이름", "값"} for row in rows):
        raise ValueError("월간 검증표 형식 오류")
    groups = {}
    for row in rows:
        group = groups.setdefault(row["구분"], {})
        if row["이름"] in group:
            raise ValueError("월간 검증표 중복 키")
        group[row["이름"]] = row["값"]
    meta = groups.get("metadata", {})
    if meta.get("format") not in {"monthly_csv_v1", "monthly_csv_v2", "monthly_csv_v3"} or meta.get("month") != month:
        raise ValueError("월간 CSV 판본 오류")
    if meta["format"] == "monthly_csv_v3":
        if (set(groups.get("sha256", {})) != {f"{month}.csv"} or
                set(p.name for p in folder.glob("*.csv")) != {f"{month}.csv", path.name} or
                digest(folder / f"{month}.csv") != groups["sha256"][f"{month}.csv"]):
            raise ValueError("월간 요약 파일 또는 해시 오류")
        groups["_check_path"] = path
        return groups
    expected = {f"{month}.csv", *(file_name(month, name) for name in TABLE_FILES)}
    if set(groups.get("sha256", {})) != expected:
        raise ValueError("월간 CSV 파일 목록 오류")
    if meta["format"] == "monthly_csv_v1":
        if set(p.name for p in folder.glob("*.csv")) != expected | {path.name}:
            raise ValueError("월간 CSV 파일 목록 오류")
    elif (path.parent != folder.parent / "_internal" / month or
          not (folder / f"{month}.csv").is_file() or
          set(p.name for p in path.parent.glob("*.csv")) != expected - {f"{month}.csv"} | {path.name}):
        raise ValueError("월간 CSV 파일 목록 오류")
    for name, sha in groups["sha256"].items():
        location = folder / name if meta["format"] == "monthly_csv_v1" or name == f"{month}.csv" else path.parent / name
        if digest(location) != sha:
            raise ValueError(f"월간 CSV 해시 불일치: {name}")
    groups["_check_path"] = path
    return groups


def _read_table(folder: Path, name: str, check: dict) -> pd.DataFrame:
    prefix = name + "."
    dtypes = {key[len(prefix):]: value for key, value in check["dtype"].items() if key.startswith(prefix)}
    numeric = {column: [""] for column, dtype in dtypes.items()
               if dtype.startswith(("int", "float", "Int", "Float"))}
    detail = (folder.parent / "_internal" / folder.name if check["metadata"]["format"] == "monthly_csv_v2" else folder)
    frame = pd.read_csv(detail / file_name(folder.name, name), encoding="utf-8-sig",
                        dtype=dtypes, keep_default_na=False, na_values=numeric)
    if len(frame) != int(check["rows"][name]) or set(frame.columns) != set(dtypes):
        raise ValueError(f"월간 CSV 행수·열 불일치: {name}")
    column = DATE_COLUMNS[name]
    if column and not frame.empty and not frame[column].between(folder.name.replace("_", "-") + "-01",
                                                                 check["metadata"]["as_of_date"]).all():
        raise ValueError(f"월간 CSV 날짜 범위 오류: {name}")
    return frame


def load_monthly(root: Path) -> tuple[dict, dict, dict]:
    """완료 검증표가 있는 월만 읽고 전체 기간 조회용 표를 결합한다."""
    root = Path(root)
    if not any(root.glob("_internal/????_??/????_??_검증.csv")) and any(root.glob("????_??/????_??.csv")):
        manifest, context, summary = load_daily_summary(root)
        return manifest, context, {"daily_summary": summary}
    folders = sorted(p for p in root.iterdir() if p.is_dir() and len(p.name) == 7 and p.name[4] == "_")
    if not folders:
        raise FileNotFoundError("월간 CSV 폴더가 없습니다")
    checks = [_read_check(folder) for folder in folders]
    for previous, current in zip(folders, folders[1:]):
        expected = (date.fromisoformat(previous.name.replace("_", "-") + "-01").replace(day=28)
                    + timedelta(days=4)).replace(day=1).strftime("%Y_%m")
        if current.name != expected:
            raise ValueError("월간 CSV 사이의 월이 누락됐습니다")
    tables = {name: pd.concat([_read_table(folder, name, check) for folder, check in zip(folders, checks)],
                              ignore_index=True) for name in TABLE_FILES}
    latest = checks[-1]
    raw = latest["settings"]
    settings = validate_settings({key: (value == "True" if key == "amount_basis_confirmed" else
                                       int(value) if key in {"increase_won", "minimum_weekday_samples"} else
                                       None if key == "target_start_date" and not value else value)
                                  for key, value in raw.items()})
    first = checks[0]["metadata"]["window_start"]
    last = latest["metadata"]["as_of_date"]
    if tables["quality_daily"].duplicated(["date", "source"]).any():
        raise ValueError("월간 품질 행 중복")
    for name in ("purchase_lines", "sales_lines"):
        if tables[name]["evidence_id"].duplicated().any():
            raise ValueError("월간 근거 ID 중복")
    signature = hashlib.sha256("".join(digest(check["_check_path"]) for check in checks).encode()).hexdigest()[:16]
    snapshot_id = f"{last}-{signature}"
    manifest = {"schema_version": SCHEMA_VERSION, "snapshot_id": snapshot_id,
                "as_of_date": last, "window_start": first,
                "guide_sha256": latest["metadata"]["guide_sha256"],
                "format": latest["metadata"]["format"]}
    plan = {"as_of_date": last, "start_date": first, "query_start": last[:7] + "-01",
            "snapshot_id": snapshot_id, "guide_sha256": manifest["guide_sha256"]}
    context = csv_evidence(build_context(plan, tables, latest["metadata"]["classification_version"], settings))
    context["format"] = manifest["format"]
    context["guide"] = {"file": "월별 CSV", "sha256": manifest["guide_sha256"]}
    return manifest, context, tables


def load_daily_summary(root: Path, *, public_only: bool = False) -> tuple[dict, dict, pd.DataFrame]:
    """일별 요약만 읽는다. 이전 v2 게시본은 기존 검증표도 허용한다."""
    root = Path(root)
    folders = sorted(p for p in root.glob("????_??") if p.is_dir())
    if not folders:
        raise FileNotFoundError("월간 CSV 폴더가 없습니다")
    frames, check_hashes, metas = [], [], []
    summary_only = public_only or not any(root.glob("_internal/????_??/????_??_검증.csv"))
    for folder in folders:
        month = folder.name
        summary_path = folder / f"{month}.csv"
        if summary_only:
            if not public_only and set(p.name for p in folder.glob("*.csv")) != {summary_path.name}:
                raise ValueError(f"월간 요약 외 CSV 존재: {month}")
            meta = {"window_start": month.replace("_", "-") + "-01"}
            check_hashes.append(digest(summary_path))
        else:
            check_path = root / "_internal" / month / f"{month}_검증.csv"
            with check_path.open("r", encoding="utf-8-sig", newline="") as handle:
                entries = list(csv.DictReader(handle))
            if not entries or any(set(row) != {"구분", "이름", "값"} for row in entries):
                raise ValueError("월간 검증표 형식 오류")
            groups = {}
            for entry in entries:
                group = groups.setdefault(entry["구분"], {})
                if entry["이름"] in group:
                    raise ValueError("월간 검증표 중복 키")
                group[entry["이름"]] = entry["값"]
            meta = groups.get("metadata", {})
            if meta.get("format") != "monthly_csv_v2" or meta.get("month") != month:
                raise ValueError("월간 요약 판본 오류")
            if digest(summary_path) != groups.get("sha256", {}).get(summary_path.name):
                raise ValueError(f"월간 요약 해시 불일치: {month}")
            check_hashes.append(digest(check_path))
        frame = pd.read_csv(summary_path, encoding="utf-8-sig")
        if (not {*SUMMARY_COLUMNS.values(), *SOURCE_STATUS_COLUMNS.values(), "확인필요"}.issubset(frame.columns)
                or frame["날짜"].duplicated().any()
                or frame.empty or str(frame.iloc[0]["날짜"]) != meta["window_start"]
                or not str(frame.iloc[-1]["날짜"]).startswith(month.replace("_", "-") + "-")
                or list(frame["날짜"]) != list(pd.date_range(meta["window_start"], str(frame.iloc[-1]["날짜"])).strftime("%Y-%m-%d"))
                or (not summary_only and str(frame.iloc[-1]["날짜"]) != meta["as_of_date"])):
            raise ValueError(f"월간 요약 행 오류: {month}")
        for column in ("자료상태", *SOURCE_STATUS_COLUMNS.values()):
            if not frame[column].isin({"정상", "잠정", "누락", "오래됨", "검증 실패"}).all():
                raise ValueError(f"월간 요약 상태 오류: {column}")
        for column in ("매출마감완료", "매입마감완료"):
            if not frame[column].astype(str).str.lower().isin({"true", "false"}).all():
                raise ValueError(f"월간 요약 완료 열 오류: {column}")
        for column in ("관측매출원", "홀매출원", "관측원재료포장재매입원", "원재료매입원", "포장재매입원",
                       "직원식대원", "기타매입원", "미분류매입원", "미분류행수"):
            if not pd.api.types.is_integer_dtype(frame[column]):
                raise ValueError(f"월간 요약 정수 열 오류: {column}")
        if summary_only:
            meta["as_of_date"] = str(frame.iloc[-1]["날짜"])
        frames.append(frame)
        metas.append(meta)
    for previous, current in zip(folders, folders[1:]):
        expected = (date.fromisoformat(previous.name.replace("_", "-") + "-01").replace(day=28)
                    + timedelta(days=4)).replace(day=1).strftime("%Y_%m")
        if current.name != expected:
            raise ValueError("월간 CSV 사이의 월이 누락됐습니다")
    last = metas[-1]["as_of_date"]
    signature = hashlib.sha256("".join(check_hashes).encode()).hexdigest()[:16]
    manifest = {"schema_version": SCHEMA_VERSION, "snapshot_id": f"{last}-{signature}",
                "as_of_date": last, "window_start": metas[0]["window_start"],
                "guide_sha256": metas[-1].get("guide_sha256", ""),
                "format": "monthly_csv_v3" if summary_only else "monthly_csv_v2"}
    summary = pd.concat(frames, ignore_index=True)
    context = {**manifest, "status": str(summary.iloc[-1]["자료상태"])}
    if summary_only:
        context["daily"] = summary_period(summary, last, last)
        context["month_to_date"] = summary_period(summary, last[:7] + "-01", last)
        context["alerts"] = []
        context["issues"] = []
        context["supported_queries"] = ["summary", "staff_meal", "status"]
    return manifest, context, summary


def summary_period(frame: pd.DataFrame, start: str, end: str) -> dict:
    """집계 CSV만으로 계산할 수 있는 기간 수치와 완료 여부."""
    rows = frame.loc[frame["날짜"].between(start, end)]
    if len(rows) != len(pd.date_range(start, end)):
        raise ValueError("집계 CSV 날짜 누락")
    amount_keys = {"관측매출원": "observed_sales_won", "홀매출원": "hall_sales_won",
                   "관측원재료포장재매입원": "observed_purchase_won", "원재료매입원": "ingredient_won",
                   "포장재매입원": "packaging_won", "직원식대원": "staff_meal_won",
                   "기타매입원": "other_won", "미분류매입원": "unclassified_won",
                   "미분류행수": "unclassified_row_count"}
    result = {key: int(rows[column].sum()) for column, key in amount_keys.items()}
    statuses = list(rows["자료상태"])
    status = next((item for item in ("검증 실패", "누락", "오래됨", "잠정") if item in statuses), "정상")
    sales_complete = rows["매출마감완료"].astype(str).str.lower().eq("true").all()
    purchase_complete = rows["매입마감완료"].astype(str).str.lower().eq("true").all()
    meal_complete = rows["푸드가이드상태"].eq("정상").all()
    complete = bool(sales_complete and purchase_complete and result["unclassified_row_count"] == 0)
    rate = (round(result["observed_purchase_won"] / result["observed_sales_won"] * 100, 1)
            if result["observed_sales_won"] > 0 else None)
    reasons = list(dict.fromkeys(str(value) for value in rows["상태사유"] if pd.notna(value) and str(value)))
    result.update(date_from=start, date_to=end, status=status, complete=complete,
                  sales_complete=bool(sales_complete), purchase_complete=bool(purchase_complete),
                  staff_meal_complete=bool(meal_complete),
                  purchase_rate_pct=rate if complete else None,
                  provisional_purchase_rate_pct=rate if not complete and status == "잠정" and result["unclassified_row_count"] == 0 else None,
                  target_comparison_status="보류", target_gap_pp=None, reasons=reasons)
    return result


def _publish_legacy_months(plan: dict, stage: Path, months: list[str]) -> dict:
    """운영 서비스 전환 전에는 기존 월별 v2 계약을 계속 게시한다."""
    root = Path(plan["output_root"])
    root.mkdir(parents=True, exist_ok=True)
    updated = []
    for month in months:
        source = stage / "monthly" / month
        new = _read_check(source)
        target = root / month
        target.mkdir(exist_ok=True)
        internal = root / "_internal" / month
        internal.mkdir(parents=True, exist_ok=True)
        check_name = f"{month}_검증.csv"
        old = None
        if (target / check_name).is_file() or (internal / check_name).is_file():
            try:
                old = _read_check(target)
            except (OSError, ValueError, KeyError):
                pass
        if old and old["metadata"]["as_of_date"] > new["metadata"]["as_of_date"]:
            continue
        if (old and old["metadata"]["as_of_date"] == new["metadata"]["as_of_date"]
                and {key: value for key, value in old["metadata"].items() if key != "format"}
                == {key: value for key, value in new["metadata"].items() if key != "format"}
                and all(old.get(key) == new.get(key) for key in
                        ("sha256", "settings", "config_hash", "source_hash", "rows", "dtype"))):
            continue
        for file in sorted(source.glob("*.csv")):
            if file.name == check_name:
                continue
            destination = target if file.name == f"{month}.csv" else internal
            temporary = destination / f".{file.name}.{uuid.uuid4().hex}.tmp"
            try:
                shutil.copy2(file, temporary)
                if digest(temporary) != digest(file):
                    raise RuntimeError("월간 CSV 복사 검증 실패")
                os.replace(temporary, destination / file.name)
            finally:
                temporary.unlink(missing_ok=True)
        temporary = internal / f".{check_name}.{uuid.uuid4().hex}.tmp"
        try:
            with (source / check_name).open("r", encoding="utf-8-sig", newline="") as handle:
                rows = [(row["구분"], row["이름"],
                         "monthly_csv_v2" if row["구분"] == "metadata" and row["이름"] == "format" else row["값"])
                        for row in csv.DictReader(handle)]
            _write_check(temporary, rows)
            os.replace(temporary, internal / check_name)
        finally:
            temporary.unlink(missing_ok=True)
        _read_check(target)
        updated.append(month)
    return {"months": months, "updated_months": updated, "output_root": str(root)}


def publish_months(plan: dict, stage: Path, months: list[str]) -> dict:
    """일별 집계만 공개하고 판본 기록은 로컬에 둔다."""
    if not plan.get("summary_only_enabled", False):
        return _publish_legacy_months(plan, stage, months)
    root = Path(plan["output_root"])
    root.mkdir(parents=True, exist_ok=True)
    index_path = Path(plan["publication_index_path"])
    try:
        index = json.loads(index_path.read_text(encoding="utf-8"))
        if index.get("output_root") != str(root) or not isinstance(index.get("months"), dict):
            raise ValueError("게시 기록 형식 오류")
    except (OSError, ValueError, TypeError):
        index = {"output_root": str(root), "months": {}}
    updated = []
    for month in months:
        source = stage / "monthly" / month
        check = _read_check(source)
        target = root / month
        target.mkdir(exist_ok=True)
        old = index["months"].get(month, {})
        summary = source / f"{month}.csv"
        published = target / summary.name
        if published.is_file():
            current_dates = pd.read_csv(published, encoding="utf-8-sig", usecols=["날짜"])["날짜"]
            if not current_dates.empty and str(current_dates.iloc[-1]) > check["metadata"]["as_of_date"]:
                continue
        if old.get("as_of_date", "") > check["metadata"]["as_of_date"]:
            continue
        if (old.get("sha256") == digest(summary) and published.is_file() and
                digest(published) == old["sha256"]):
            continue
        temporary = target / f".{summary.name}.{uuid.uuid4().hex}.tmp"
        try:
            shutil.copy2(summary, temporary)
            if digest(temporary) != digest(summary):
                raise RuntimeError("월간 요약 복사 검증 실패")
            os.replace(temporary, published)
        finally:
            temporary.unlink(missing_ok=True)
        index["months"][month] = {
            "as_of_date": check["metadata"]["as_of_date"],
            "sha256": digest(published),
            "implementation_hash": check["metadata"]["implementation_hash"],
            "guide_sha256": check["metadata"]["guide_sha256"],
            "config_hash": check.get("config_hash", {}),
            "source_hash": check.get("source_hash", {}),
        }
        atomic_json(index_path, index)
        updated.append(month)
    return {"months": months, "updated_months": updated, "output_root": str(root)}
