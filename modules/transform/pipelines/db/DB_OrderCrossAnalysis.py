"""검증된 주문 교차 결과와 같은 세대의 보조 자료를 원자적으로 반영한다."""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import re
import shutil
import stat
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from modules.transform.pipelines.db.DB_OrderCrossAnalysis_core import (
    VERSION, CROSS_COLUMNS, REQUIRED_SALES_COLUMNS, ALLOWED_CATEGORIES, UNIQUE_KEY,
    Catalog, build_day, make_catalog, validate_frames,
)
from modules.transform.pipelines.db.DB_UnifiedSales_common import (
    UNIFIED_ROOT, _unified_daily_path, iter_unified_sales_files,
)
from modules.transform.utility.paths import (
    FIN_PRODUCT_MAP_JOIN_CSV_PATH, ORDER_CROSS_DIR, LOCAL_DB, existing_fin_product_csv_path,
)
from modules.transform.utility.process_lock import named_lock

logger = logging.getLogger(__name__)
CROSS_ROOT = ORDER_CROSS_DIR
METADATA_KEY = b"order_cross_generation"
SUPPORT_FILES = ("main_orders", "standard_pairs", "item_mapping")


class StaleCrossInputError(ValueError):
    """발행 시점 이후 입력 매출 파일 또는 상품 매핑이 변경되어 재검증이 필요함을 나타낸다."""


def _now():
    return datetime.now(ZoneInfo("Asia/Seoul"))


def _hash_bytes(value):
    return hashlib.sha256(value).hexdigest()


def _remove_under(path, root):
    path, root = Path(path).resolve(), Path(root).resolve()
    if path == root or not path.is_relative_to(root):
        raise ValueError(f"정리 대상이 전용 폴더 밖입니다: {path}")
    def retry_readonly(function, failed_path, exc_info):
        target = Path(failed_path).resolve()
        if target == root or not target.is_relative_to(root):
            raise ValueError(f"정리 대상이 전용 폴더 밖입니다: {target}")
        # OneDrive가 동기화한 디렉터리도 Windows 읽기 전용 속성을 가질 수 있다.
        attributes = getattr(target.stat(), "st_file_attributes", 0)
        if os.name != "nt" or not attributes & stat.FILE_ATTRIBUTE_READONLY:
            raise exc_info[1]
        target.chmod(stat.S_IWRITE | stat.S_IREAD)
        function(failed_path)

    shutil.rmtree(path, onerror=retry_readonly)


def _fingerprint(path):
    return _hash_bytes(Path(path).read_bytes())


def _frame_hash(frame):
    return _hash_bytes(pd.util.hash_pandas_object(frame, index=False).values.tobytes())


def _read_csv_bytes(value):
    for encoding in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(io.BytesIO(value), dtype=str, encoding=encoding).fillna("")
        except UnicodeDecodeError:
            continue
    raise ValueError("상품 매핑 CSV 인코딩 오류")


def load_catalog() -> Catalog:
    paths = [existing_fin_product_csv_path(), FIN_PRODUCT_MAP_JOIN_CSV_PATH]
    content = [p.read_bytes() for p in paths]
    files = {str(p): _hash_bytes(data) for p, data in zip(paths, content)}
    catalog = make_catalog(*[_read_csv_bytes(data) for data in content])
    catalog.files = files
    catalog.fingerprint = _hash_bytes(json.dumps(list(files.values())).encode())
    _check_catalog(catalog)
    return catalog


def _check_catalog(catalog):
    if any(_fingerprint(path) != value for path, value in (catalog.files or {}).items()):
        raise RuntimeError("계산 중 상품 매핑 변경: 동일 매핑으로 재시도 필요")


def _cross_daily_path(date_str, output_root=None):
    ymd = datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")
    return Path(output_root or CROSS_ROOT) / f"order_cross_{ymd}.parquet"


def _input_path(date_str, input_root=None):
    if input_root is None:
        return _unified_daily_path(date_str)
    return Path(input_root) / ("unified_sales_" + datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d") + ".parquet")


def _assert_output_allowed(root, allow_publish):
    resolved = Path(root).resolve()
    is_onedrive = resolved == Path(ORDER_CROSS_DIR).resolve() or any("onedrive" in part.lower() for part in resolved.parts)
    if is_onedrive and not allow_publish:
        raise PermissionError("OneDrive 반영 승인이 필요합니다. 로컬 output_root로 검증하세요.")


def _generation(path):
    metadata = pq.read_metadata(path).metadata or {}
    raw = metadata.get(METADATA_KEY)
    if raw is None:
        raise ValueError("기존 결과에 검증된 교차분석 생성번호가 없습니다")
    result = json.loads(raw)
    if not isinstance(result, dict) or not isinstance(result.get("id"), str) or not re.fullmatch(r"[0-9a-f]{32}", result["id"]):
        raise ValueError("교차분석 생성번호 오류")
    return result


def _support_dir(path, generation):
    return path.parent / "_support" / path.stem.removeprefix("order_cross_") / generation["id"]


def load_validated_support(date_str, output_root=None, *, require_fresh=True, catalog=None, input_root=None):
    path = _cross_daily_path(date_str, output_root)
    meta = _generation(path)
    if meta.get("version") != VERSION or meta.get("date") != date_str:
        raise ValueError("교차분석 처리 버전/날짜 불일치")
    support = _support_dir(path, meta)
    quality_bytes = (support / "quality.json").read_bytes()
    if _hash_bytes(quality_bytes) != meta["quality_hash"]:
        raise ValueError("교차분석 품질 기록 변경")
    quality = json.loads(quality_bytes)
    if not isinstance(quality, dict) or quality.get("generation") != meta["id"] or not quality.get("validated"):
        raise ValueError("교차분석 보조 자료 세대/검증 상태 불일치")
    frames = {}
    for name in SUPPORT_FILES:
        data = (support / f"{name}.parquet").read_bytes()
        if _hash_bytes(data) != quality["support_hashes"][name]:
            raise ValueError(f"교차분석 보조 자료 변경: {name}")
        frames[name] = pd.read_parquet(io.BytesIO(data))
    frames["cross"] = pd.read_parquet(path)
    frames["quality"] = quality
    if _frame_hash(frames["cross"]) != quality.get("cross_content_hash"):
        raise ValueError("교차 결과 내용 변경")
    validate_frames(frames["cross"], frames["main_orders"], frames["standard_pairs"])
    if require_fresh:
        catalog = catalog or load_catalog()
        _check_catalog(catalog)
        if meta["catalog_hash"] != catalog.fingerprint or meta["input_hash"] != _fingerprint(_input_path(date_str, input_root)):
            raise StaleCrossInputError("교차분석 갱신 대기: 입력 또는 매핑 변경")
    return frames


def _schema():
    return pa.schema([(c, pa.int64() if c in {"co_order_cnt", "co_qty", "co_amount"} else pa.bool_() if c == "is_multi_main" else pa.string()) for c in CROSS_COLUMNS])


def _publish(bundle, date_str, root, input_hash, catalog, input_path, *, allow_publish=False):
    _assert_output_allowed(root, allow_publish)
    path = _cross_daily_path(date_str, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    generation = uuid.uuid4().hex
    meta = {"id": generation, "date": date_str, "version": VERSION, "input_hash": input_hash, "catalog_hash": catalog.fingerprint}
    support = _support_dir(path, meta)
    support.mkdir(parents=True)
    temporary = path.with_name(path.name + "." + generation + ".tmp")
    backup_dir = Path(LOCAL_DB) / "temp" / "backups" / "order_cross" / generation
    backup = backup_dir / path.name
    previous = None
    committed = False
    rollback_failed = False
    try:
        if path.exists():
            try:
                previous = _support_dir(path, _generation(path))
            except (ValueError, KeyError):
                previous = None
            backup_dir.mkdir(parents=True)
            shutil.copy2(path, backup)
        hashes = {}
        for name in SUPPORT_FILES:
            target = support / f"{name}.parquet"
            bundle[name].to_parquet(target, index=False, engine="pyarrow")
            hashes[name] = _fingerprint(target)
        quality = {**bundle["quality"], "generation": generation, "input_hash": input_hash,
                   "catalog_hash": catalog.fingerprint, "support_hashes": hashes,
                   "cross_content_hash": _frame_hash(bundle["cross"])}
        qbytes = json.dumps(quality, ensure_ascii=False, indent=2).encode("utf-8")
        (support / "quality.json").write_bytes(qbytes)
        meta["quality_hash"] = _hash_bytes(qbytes)
        table = pa.Table.from_pandas(bundle["cross"], schema=_schema(), preserve_index=False)
        table = table.replace_schema_metadata({**(table.schema.metadata or {}), METADATA_KEY: json.dumps(meta).encode()})
        pq.write_table(table, temporary)
        reread = pd.read_parquet(temporary)
        validate_frames(reread, pd.read_parquet(support / "main_orders.parquet"), pd.read_parquet(support / "standard_pairs.parquet"))
        pd.testing.assert_frame_equal(reread, bundle["cross"])
        _check_catalog(catalog)
        if _fingerprint(input_path) != input_hash:
            raise RuntimeError("계산 중 매출 입력 변경: 반영 중단")
        os.replace(temporary, path)
        committed = True
        load_validated_support(date_str, root, catalog=catalog, input_root=input_path.parent)
    except Exception:
        if committed:
            try:
                if backup.exists():
                    restore = path.with_name(path.name + ".restore.tmp")
                    shutil.copy2(backup, restore)
                    os.replace(restore, path)
                else:
                    path.unlink(missing_ok=True)
            except Exception:
                rollback_failed = True
                logger.exception("롤백 실패, 복구본 보존: %s", backup_dir)
        if not rollback_failed:
            _remove_under(support, path.parent / "_support")
        raise
    finally:
        temporary.unlink(missing_ok=True)
        if backup_dir.exists() and not rollback_failed:
            _remove_under(backup_dir, Path(LOCAL_DB) / "temp" / "backups" / "order_cross")
    # 이전 세대는 새 세대의 재읽기 검증이 끝난 뒤 정리한다.
    if previous and previous != support and previous.exists():
        _remove_under(previous, path.parent / "_support")
    return quality


def _process_order_cross(date_str, overwrite=True, *, output_root=None, input_root=None, catalog=None, allow_publish=False):
    root = Path(output_root or CROSS_ROOT)
    _assert_output_allowed(root, allow_publish)
    path = _cross_daily_path(date_str, root)
    input_path = _input_path(date_str, input_root)
    catalog = catalog or load_catalog()
    with named_lock("order_cross:" + str(path.resolve()), timeout=120):
        if not overwrite and path.exists():
            loaded = load_validated_support(date_str, root, catalog=catalog, input_root=input_root)
            return {"date": date_str, "rows": len(loaded["cross"]), "path": str(path), "skipped": True}
        _check_catalog(catalog)
        content = input_path.read_bytes()
        bundle = build_day(pd.read_parquet(io.BytesIO(content)), date_str, catalog, _now().strftime("%Y-%m-%d %H:%M:%S"))
        quality = _publish(bundle, date_str, root, _hash_bytes(content), catalog, input_path, allow_publish=allow_publish)
    result = {"date": date_str, "rows": len(bundle["cross"]), "standard_rows": len(bundle["standard_pairs"]), "path": str(path), "quality": quality}
    logger.info("교차분석 검증·저장 완료: %s | rows=%d", date_str, result["rows"])
    return result


def run_order_cross_analysis(date_str, overwrite=True, **kwargs):
    r = _process_order_cross(date_str, overwrite, **kwargs)
    return f"order_cross 검증 완료 | {date_str} | rows={r['rows']}"


def validate_order_cross(date_str, *, output_root=None, input_root=None, catalog=None):
    frames = load_validated_support(date_str, output_root, catalog=catalog, input_root=input_root)
    return f"order_cross validate | {date_str} | rows={len(frames['cross'])}"


def _dates(input_root=None):
    files = iter_unified_sales_files() if input_root is None else sorted(Path(input_root).glob("unified_sales_*.parquet"))
    dates = []
    for path in files:
        try:
            date = datetime.strptime(path.stem, "unified_sales_%y%m%d").strftime("%Y-%m-%d")
            if path.name != "unified_sales_" + datetime.strptime(date, "%Y-%m-%d").strftime("%y%m%d") + ".parquet":
                continue
        except ValueError:
            continue
        if date < _now().strftime("%Y-%m-%d"):
            dates.append(date)
    return sorted(set(dates))


def _is_current(date_str, root, catalog, input_root=None):
    try:
        metadata = _generation(_cross_daily_path(date_str, root))
        if metadata.get("version") != VERSION or metadata.get("catalog_hash") != catalog.fingerprint:
            return False
        if metadata.get("input_hash") != _fingerprint(_input_path(date_str, input_root)):
            return False
        # 보조 자료까지 검사하므로 중간 저장/손상 파일은 완료 체크포인트가 아니다.
        load_validated_support(date_str, root, require_fresh=False)
        return True
    except (OSError, ValueError, KeyError, pa.ArrowException):
        return False


def process_pending(*, output_root=None, input_root=None, days=3, max_history_dates=10, allow_publish=False, all_dates=False):
    if days < 1 or max_history_dates < 0:
        raise ValueError("최근 일수는 양수, 과거 처리 한도는 0 이상이어야 합니다")
    root = Path(output_root or CROSS_ROOT)
    _assert_output_allowed(root, allow_publish)
    catalog = load_catalog()
    dates = _dates(input_root)
    cutoff = (_now() - timedelta(days=days)).strftime("%Y-%m-%d")
    pending = [d for d in dates if not _is_current(d, root, catalog, input_root)]
    recent = sorted([d for d in pending if d >= cutoff], reverse=True)
    history = [d for d in pending if d < cutoff]
    selected = pending if all_dates else recent + history[:max_history_dates]
    results, failed = [], []
    for date in selected:
        try:
            results.append(_process_order_cross(date, output_root=root, input_root=input_root, catalog=catalog, allow_publish=allow_publish))
        except Exception as exc:
            logger.exception("교차분석 날짜 처리 실패: %s", date)
            failed.append({"date": date, "error": str(exc)})
    result = {"dates": [r["date"] for r in results], "rows": sum(r["rows"] for r in results),
              "remaining": len(pending) - len(results), "failed": failed}
    if failed:
        raise RuntimeError("교차분석 미완료: " + json.dumps(result, ensure_ascii=False))
    return result


def run_lookback_order_cross_analysis(days=3, **kwargs):
    return json.dumps(process_pending(days=days, **kwargs), ensure_ascii=False)


def backfill_order_cross_analysis(**kwargs):
    return json.dumps(process_pending(all_dates=True, **kwargs), ensure_ascii=False)


def backup_order_cross_to_onedrive(date_str=None):
    return "별도 OneDrive 백업 없음: 검증 후 원자 교체 및 로컬 임시 복구본 사용"
