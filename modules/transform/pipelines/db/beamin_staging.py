"""Baemin macro staging and inbox export helpers."""

from __future__ import annotations

import importlib
import json
import logging
import os
import re
import shutil
from pathlib import Path
from typing import Any

from modules.transform.utility.paths import LOCAL_DB

logger = logging.getLogger(__name__)

_MISSING = object()

_MACRO_ROLE_ALIASES: dict[str, tuple[str, str]] = {
    "상위": ("상위", "top"),
    "top": ("상위", "top"),
    "upper": ("상위", "top"),
    "1": ("상위", "top"),
    "하위": ("하위", "bottom"),
    "bottom": ("하위", "bottom"),
    "lower": ("하위", "bottom"),
    "2": ("하위", "bottom"),
}

BAEMIN_STAGE_ATTRS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("modules.transform.pipelines.db.DB_Beamin_01_now", "BAEMIN_METRICS_DB", ("metrics_now",)),
    (
        "modules.transform.pipelines.db.DB_Beamin_02_woori_shop_click",
        "BAEMIN_OUR_STORE_CLICKS_DB",
        ("metrics_our_store_clicks",),
    ),
    ("modules.transform.pipelines.db.DB_Beamin_03_shop_change", "BAEMIN_SHOP_CHANGE_DB", ("shop_change",)),
    (
        "modules.transform.pipelines.db.DB_Beamin_03_shop_change",
        "BAEMIN_SHOP_OPERATION_DB",
        ("shop_operation",),
    ),
    ("modules.transform.pipelines.db.DB_Beamin_04_orders", "BAEMIN_ORDERS_DB", ("orders",)),
    ("modules.transform.pipelines.db.DB_Beamin_05_ad_funnel", "BAEMIN_AD_FUNNEL_DB", ("ad_funnel",)),
    (
        "modules.transform.pipelines.db.DB_Beamin_monthly_operation",
        "BAEMIN_MONTHLY_OPERATION_DB",
        ("monthly_operation",),
    ),
    ("modules.transform.pipelines.db.DB_Beamin_monthly_operation", "BAEMIN_SHOP_CHANGE_DB", ("shop_change",)),
    (
        "modules.transform.pipelines.db.DB_Beamin_monthly_operation",
        "BAEMIN_SHOP_OPERATION_DB",
        ("shop_operation",),
    ),
)


def safe_run_id_part(value: str | None) -> str:
    return re.sub(r"[^A-Za-z0-9_.~-]+", "_", str(value or "manual")).strip("_")[:120]


def resolve_macro_role(raw: str | None) -> dict[str, str | None]:
    """PC 역할을 기존 수집 범위와 안전한 폴더 slug로 정규화한다."""
    key = str(raw or "").strip().lower()
    collect_range, slug = _MACRO_ROLE_ALIASES.get(key, (None, ""))
    return {"range": collect_range, "slug": slug}


def local_stage_paths(subdir: str, run_id: str | None = None) -> tuple[Path, Path]:
    local_analytics = LOCAL_DB / subdir
    if run_id:
        local_analytics = local_analytics / safe_run_id_part(run_id)
    return local_analytics, local_analytics / "baemin_macro"


def init_empty_staging(local_baemin: Path) -> None:
    local_root = LOCAL_DB.resolve()
    target = local_baemin.resolve()
    if local_baemin.exists():
        if not target.is_relative_to(local_root):
            raise RuntimeError(f"staging 삭제 범위 오류: {local_baemin}")
        shutil.rmtree(local_baemin)
    local_baemin.mkdir(parents=True, exist_ok=True)
    logger.info("배민 빈 staging 생성: %s", local_baemin)


def cleanup_staging(local_analytics: Path) -> None:
    local_root = LOCAL_DB.resolve()
    target = local_analytics.resolve()
    if not target.is_relative_to(local_root):
        raise RuntimeError(f"staging cleanup 범위 오류: {local_analytics}")
    if local_analytics.exists():
        shutil.rmtree(local_analytics)
        logger.info("배민 staging cleanup 완료: %s", local_analytics)


def patch_baemin_staging_paths(local_analytics: Path) -> tuple[str | None, list[tuple[Any, str, Any]]]:
    analytics_original = os.environ.get("ANALYTICS_DB")
    os.environ["ANALYTICS_DB"] = str(local_analytics)

    local_baemin = local_analytics / "baemin_macro"
    originals: list[tuple[Any, str, Any]] = []
    for module_name, attr_name, relative_parts in BAEMIN_STAGE_ATTRS:
        module = importlib.import_module(module_name)
        originals.append((module, attr_name, getattr(module, attr_name, _MISSING)))
        setattr(module, attr_name, local_baemin.joinpath(*relative_parts))

    logger.info("배민 staging 모드 전환: %s", local_analytics)
    return analytics_original, originals


def restore_baemin_staging_paths(
    analytics_original: str | None,
    originals: list[tuple[Any, str, Any]],
) -> None:
    for module, attr_name, original_value in reversed(originals):
        if original_value is _MISSING:
            try:
                delattr(module, attr_name)
            except AttributeError:
                pass
        else:
            setattr(module, attr_name, original_value)

    if analytics_original is None:
        os.environ.pop("ANALYTICS_DB", None)
    else:
        os.environ["ANALYTICS_DB"] = analytics_original
    logger.info("배민 staging 모드 해제")


def export_staging_to_inbox(
    local_baemin: Path,
    run_id: str | None,
    inbox_dir: Path,
    meta: dict | None = None,
    *,
    replace_existing: bool = False,
    folder_prefix: str = "",
) -> Path:
    if not local_baemin.exists():
        raise RuntimeError(f"staging 경로 없음: {local_baemin}")

    run_part = safe_run_id_part(run_id)
    prefix = safe_run_id_part(folder_prefix) if folder_prefix else ""
    name = f"manual__{prefix}__{run_part}" if prefix else f"manual__{run_part}"
    inbox_run = inbox_dir / name
    tmp_run = inbox_run.with_name(f"_tmp__{inbox_run.name}")
    if tmp_run.exists():
        shutil.rmtree(tmp_run)
    if inbox_run.exists():
        if not replace_existing:
            raise FileExistsError(f"inbox run 폴더가 이미 존재함: {inbox_run}")
        shutil.rmtree(inbox_run)

    dst_root = tmp_run / "baemin_macro"
    file_count = 0
    for src in local_baemin.rglob("*"):
        if not src.is_file():
            continue
        rel = src.relative_to(local_baemin)
        dst = dst_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        file_count += 1

    tmp_run.mkdir(parents=True, exist_ok=True)
    if meta is not None:
        (tmp_run / "_meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    tmp_run.rename(inbox_run)
    logger.info("배민 inbox export 완료: %s (%d files)", inbox_run, file_count)
    return inbox_run
