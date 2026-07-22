"""Airflow DAG harness registry loading and resolution."""

from __future__ import annotations

import fnmatch
import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable


REGISTRY_PATH = Path("harness/registry.json")


class RegistryError(ValueError):
    """Raised when the registry or a DAG mapping is invalid."""


def _as_list(value: Any, field: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise RegistryError(f"{field} must be a string or list of strings")
    return [item.strip() for item in value if item.strip()]


def normalize_relative_path(value: str, *, field: str = "path") -> str:
    raw = str(value).strip().replace("\\", "/")
    path = PurePosixPath(raw)
    if not raw or path.is_absolute() or ".." in path.parts or raw.startswith("."):
        raise RegistryError(f"invalid {field}: {value}")
    return path.as_posix()


def _matches(path: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


@dataclass(frozen=True)
class DagContext:
    dag: str
    group: str
    guidance: tuple[str, ...]
    harness_docs: tuple[str, ...]
    override: bool


class HarnessRegistry:
    def __init__(self, root: Path, data: dict[str, Any]):
        self.root = root.resolve()
        self.data = data
        if data.get("version") != 1:
            raise RegistryError("registry version must be 1")
        self.groups = data.get("groups") or []
        self.overrides = data.get("overrides") or []
        self.ignore = tuple(
            normalize_relative_path(item, field="ignore pattern")
            for item in _as_list(data.get("ignore"), "ignore")
        )
        if not isinstance(self.groups, list) or not isinstance(self.overrides, list):
            raise RegistryError("groups and overrides must be lists")
        self._validate_structure()

    @classmethod
    def load(cls, root: Path | str) -> "HarnessRegistry":
        root_path = Path(root).resolve()
        path = root_path / REGISTRY_PATH
        if not path.is_file():
            raise RegistryError(f"registry not found: {path}")
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RegistryError(f"registry read failed: {exc}") from exc
        if not isinstance(data, dict):
            raise RegistryError("registry root must be an object")
        return cls(root_path, data)

    def _validate_structure(self) -> None:
        group_ids: set[str] = set()
        for index, group in enumerate(self.groups):
            if not isinstance(group, dict):
                raise RegistryError(f"groups[{index}] must be an object")
            group_id = str(group.get("id", "")).strip()
            if not group_id or group_id in group_ids:
                raise RegistryError(f"invalid or duplicate group id: {group_id}")
            group_ids.add(group_id)
            patterns = _as_list(group.get("patterns"), f"group {group_id} patterns")
            if not patterns:
                raise RegistryError(f"group {group_id} requires patterns")
            group["patterns"] = [normalize_relative_path(p, field="group pattern") for p in patterns]
            group["guidance"] = [normalize_relative_path(p, field="guidance") for p in _as_list(group.get("guidance"), "guidance")]
            group["harness_docs"] = self._doc_names(group.get("harness_docs"))

        override_dags: set[str] = set()
        for index, override in enumerate(self.overrides):
            if not isinstance(override, dict):
                raise RegistryError(f"overrides[{index}] must be an object")
            dag = normalize_relative_path(override.get("dag", ""), field="override dag")
            if dag in override_dags:
                raise RegistryError(f"duplicate override dag: {dag}")
            override_dags.add(dag)
            override["dag"] = dag
            override["guidance"] = [normalize_relative_path(p, field="guidance") for p in _as_list(override.get("guidance"), "guidance")]
            override["harness_docs"] = self._doc_names(override.get("harness_docs"))

    @staticmethod
    def _doc_names(value: Any) -> list[str]:
        names = _as_list(value, "harness_docs")
        result: list[str] = []
        for raw in names:
            name = raw[:-3] if raw.endswith(".md") else raw
            if "/" in name or "\\" in name or name.startswith(".") or not name:
                raise RegistryError(f"invalid harness document name: {raw}")
            result.append(name)
        return result

    def candidates(self) -> list[str]:
        dags = self.root / "dags"
        if not dags.is_dir():
            raise RegistryError(f"dags directory not found: {dags}")
        result = []
        for path in dags.rglob("*.py"):
            relative = path.relative_to(self.root).as_posix()
            if path.name == "__init__.py" or _matches(relative, self.ignore):
                continue
            result.append(relative)
        return sorted(result)

    def resolve(self, dag: str, *, check_files: bool = True) -> DagContext:
        normalized = normalize_relative_path(dag, field="dag target")
        if not normalized.startswith("dags/") or not normalized.endswith(".py"):
            raise RegistryError(f"dag target must be dags/**/*.py: {dag}")
        if check_files and not (self.root / normalized).is_file():
            raise RegistryError(f"dag target not found: {normalized}")

        matches = [group for group in self.groups if _matches(normalized, group["patterns"])]
        if not matches:
            raise RegistryError(f"unmapped dag: {normalized}")
        if len(matches) > 1:
            ids = ", ".join(group["id"] for group in matches)
            raise RegistryError(f"dag matches multiple groups ({ids}): {normalized}")
        group = matches[0]
        override = next((item for item in self.overrides if item["dag"] == normalized), None)
        guidance = list(group["guidance"])
        docs = list(group["harness_docs"])
        if override:
            guidance.extend(override["guidance"])
            docs.extend(override["harness_docs"])
        guidance = list(dict.fromkeys(guidance))
        docs = list(dict.fromkeys(docs))

        if check_files:
            for name in guidance:
                if not (self.root / name).is_file():
                    raise RegistryError(f"guidance not found: {name}")
            for name in docs:
                if not (self.root / "harness" / f"{name}.md").is_file():
                    raise RegistryError(f"harness document not found: harness/{name}.md")
        return DagContext(normalized, group["id"], tuple(guidance), tuple(docs), bool(override))

    def validate(self) -> list[str]:
        errors: list[str] = []
        candidates = self.candidates()
        for dag in candidates:
            try:
                self.resolve(dag)
            except RegistryError as exc:
                errors.append(str(exc))
        candidate_set = set(candidates)
        for override in self.overrides:
            if override["dag"] not in candidate_set:
                errors.append(f"override dag is not a candidate: {override['dag']}")
        return errors


def load_dag_context(root: Path | str, dag_targets: Any) -> list[DagContext]:
    targets = _as_list(dag_targets, "dag_targets")
    if not targets:
        return []
    registry = HarnessRegistry.load(root)
    return [registry.resolve(target) for target in targets]
