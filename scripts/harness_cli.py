#!/usr/bin/env python3
"""Inventory, validate, and scaffold Airflow DAG harness metadata."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from harness_registry import HarnessRegistry, RegistryError, REGISTRY_PATH, normalize_relative_path


KST = timezone(timedelta(hours=9))


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_registry_data(root: Path) -> dict:
    path = root / REGISTRY_PATH
    if not path.is_file():
        raise RegistryError(f"registry not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def command_inventory(args: argparse.Namespace) -> int:
    registry = HarnessRegistry.load(args.target)
    rows = []
    for dag in registry.candidates():
        try:
            item = registry.resolve(dag)
            rows.append({
                "dag": item.dag,
                "group": item.group,
                "override": item.override,
                "guidance": list(item.guidance),
                "harness_docs": list(item.harness_docs),
                "status": "ok",
            })
        except RegistryError as exc:
            rows.append({"dag": dag, "status": "error", "error": str(exc)})
    if args.format == "json":
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        for row in rows:
            if row["status"] == "ok":
                docs = ",".join(row["harness_docs"]) or "-"
                print(f"OK\t{row['group']}\t{row['dag']}\t{docs}")
            else:
                print(f"ERROR\t-\t{row['dag']}\t{row['error']}")
    return 1 if any(row["status"] == "error" for row in rows) else 0


def _validate_phases(root: Path, registry: HarnessRegistry) -> list[str]:
    errors: list[str] = []
    phases_root = root / "phases"
    top = phases_root / "index.json"
    if not top.exists():
        return errors
    try:
        top_data = json.loads(top.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [f"phases/index.json read failed: {exc}"]
    registered = {
        str(item.get("dir")).strip()
        for item in top_data.get("phases", [])
        if isinstance(item, dict) and str(item.get("dir") or "").strip()
    }
    actual = {path.name for path in phases_root.iterdir() if path.is_dir()}
    for extra in sorted(actual - registered):
        errors.append(f"unregistered phase directory: phases/{extra}")
    for item in top_data.get("phases", []):
        phase_name = item.get("dir")
        index_path = phases_root / str(phase_name) / "index.json"
        if not index_path.is_file():
            errors.append(f"phase index not found: phases/{phase_name}/index.json")
            continue
        try:
            phase = json.loads(index_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"phase index read failed ({phase_name}): {exc}")
            continue
        for step in phase.get("steps", []):
            number = step.get("step")
            if not (index_path.parent / f"step{number}.md").is_file():
                errors.append(f"step file not found: phases/{phase_name}/step{number}.md")
            targets = step.get("dag_targets")
            if targets is not None:
                if isinstance(targets, str):
                    targets = [targets]
                if not isinstance(targets, list):
                    errors.append(f"{phase_name} step{number}: dag_targets must be a string or list")
                    continue
                for target in targets:
                    try:
                        registry.resolve(target)
                    except RegistryError as exc:
                        errors.append(f"{phase_name} step{number}: {exc}")
    return errors


def command_validate(args: argparse.Namespace) -> int:
    registry = HarnessRegistry.load(args.target)
    errors = registry.validate() + _validate_phases(Path(args.target).resolve(), registry)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        print(f"validation failed: {len(errors)} error(s)")
        return 1
    print(f"validation ok: {len(registry.candidates())} DAG candidate(s)")
    return 0


def command_scaffold_group(args: argparse.Namespace) -> int:
    root = Path(args.target).resolve()
    data = _load_registry_data(root)
    if any(group.get("id") == args.id for group in data.get("groups", [])):
        raise RegistryError(f"group already exists: {args.id}")
    for doc in args.harness_doc:
        clean = HarnessRegistry._doc_names([doc])[0]
        path = root / "harness" / f"{clean}.md"
        if path.exists():
            raise RegistryError(f"refusing to overwrite: {path}")
    group = {
        "id": args.id,
        "patterns": [normalize_relative_path(pattern, field="group pattern") for pattern in args.pattern],
        "guidance": [normalize_relative_path(path, field="guidance") for path in args.guidance],
        "harness_docs": args.harness_doc,
    }
    data.setdefault("groups", []).append(group)
    HarnessRegistry(root, data)
    for doc in args.harness_doc:
        _create_doc(root, doc, f"{args.id} DAG Group Harness")
    _write_json(root / REGISTRY_PATH, data)
    print(f"created group: {args.id}")
    return 0


def _create_doc(root: Path, name: str, title: str) -> None:
    clean = HarnessRegistry._doc_names([name])[0]
    path = root / "harness" / f"{clean}.md"
    if path.exists():
        raise RegistryError(f"refusing to overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"# {title}\n\n## 대상\n- TODO\n\n## 작업 기준\n- TODO\n\n"
        "## 검증\n- TODO\n\n## 금지사항\n- TODO\n",
        encoding="utf-8",
    )


def command_scaffold_override(args: argparse.Namespace) -> int:
    root = Path(args.target).resolve()
    data = _load_registry_data(root)
    dag = normalize_relative_path(args.dag, field="override dag")
    if any(item.get("dag") == dag for item in data.get("overrides", [])):
        raise RegistryError(f"override already exists: {dag}")
    HarnessRegistry(root, data).resolve(dag)
    for doc in args.harness_doc:
        HarnessRegistry._doc_names([doc])
    for doc in args.harness_doc:
        if not (root / "harness" / f"{doc}.md").exists():
            _create_doc(root, doc, f"{Path(dag).stem} Harness")
    data.setdefault("overrides", []).append({"dag": dag, "harness_docs": args.harness_doc})
    HarnessRegistry(root, data).resolve(dag)
    _write_json(root / REGISTRY_PATH, data)
    print(f"created override: {dag}")
    return 0


def command_scaffold_phase(args: argparse.Namespace) -> int:
    root = Path(args.target).resolve()
    registry = HarnessRegistry.load(root)
    targets = [registry.resolve(target).dag for target in args.dag]
    phase_name = normalize_relative_path(args.name, field="phase name")
    if "/" in phase_name:
        raise RegistryError("phase name must not contain a directory separator")
    phase_dir = root / "phases" / phase_name
    if phase_dir.exists():
        raise RegistryError(f"phase already exists: {phase_name}")
    top_path = root / "phases" / "index.json"
    top = json.loads(top_path.read_text(encoding="utf-8")) if top_path.exists() else {"phases": []}
    if any(item.get("dir") == phase_name for item in top.get("phases", [])):
        raise RegistryError(f"phase already registered: {phase_name}")
    phase_dir.mkdir(parents=True)
    now = datetime.now(KST).strftime("%Y-%m-%dT%H:%M:%S%z")
    _write_json(phase_dir / "index.json", {
        "project": "airflow",
        "phase": phase_name,
        "steps": [{"step": 0, "name": args.step_name, "status": "pending", "dag_targets": targets}],
        "created_at": now,
    })
    (phase_dir / "step0.md").write_text(
        f"# Step 0: {args.step_name}\n\n## 읽어야 할 파일\n"
        + "\n".join(f"- `/{target}`" for target in targets)
        + "\n\n## 작업\n- TODO\n\n## Acceptance Criteria\n```powershell\n"
        + "\n".join(
            f"python -c \"import importlib; importlib.import_module('{Path(target).with_suffix('').as_posix().replace('/', '.')}'); print('import ok')\""
            for target in targets
        )
        + "\n```\n\n## 검증 절차\n1. Acceptance Criteria를 실행한다.\n"
        "2. 성공 시 step 상태와 summary를 갱신한다.\n\n## 금지사항\n"
        "- 범위 밖 DAG와 pipeline을 수정하지 않는다.\n",
        encoding="utf-8",
    )
    top.setdefault("phases", []).append({"dir": phase_name, "status": "pending"})
    _write_json(top_path, top)
    print(f"created phase: {phase_name}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Airflow DAG harness manager")
    sub = parser.add_subparsers(dest="command", required=True)
    inventory = sub.add_parser("inventory")
    inventory.add_argument("--target", required=True)
    inventory.add_argument("--format", choices=("table", "json"), default="table")
    inventory.set_defaults(func=command_inventory)
    validate = sub.add_parser("validate")
    validate.add_argument("--target", required=True)
    validate.set_defaults(func=command_validate)
    scaffold = sub.add_parser("scaffold")
    scaffold_sub = scaffold.add_subparsers(dest="kind", required=True)
    group = scaffold_sub.add_parser("group")
    group.add_argument("--target", required=True)
    group.add_argument("--id", required=True)
    group.add_argument("--pattern", action="append", required=True)
    group.add_argument("--guidance", action="append", default=[])
    group.add_argument("--harness-doc", action="append", default=[])
    group.set_defaults(func=command_scaffold_group)
    override = scaffold_sub.add_parser("override")
    override.add_argument("--target", required=True)
    override.add_argument("--dag", required=True)
    override.add_argument("--harness-doc", action="append", required=True)
    override.set_defaults(func=command_scaffold_override)
    phase = scaffold_sub.add_parser("phase")
    phase.add_argument("--target", required=True)
    phase.add_argument("--name", required=True)
    phase.add_argument("--dag", action="append", required=True)
    phase.add_argument("--step-name", default="dag-change")
    phase.set_defaults(func=command_scaffold_phase)
    return parser


def main() -> int:
    try:
        args = build_parser().parse_args()
        return args.func(args)
    except (RegistryError, OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
