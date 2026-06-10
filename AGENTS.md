# Codex Workspace Guide

## Scope
- This file is for Codex and local coding agents.
- Claude-specific instructions live in `CLAUDE.md` and `.claude/`.
- Shared project facts should stay in neutral docs such as `docs/architecture.md` and `docs/db-schema.md`.

## Primary Workflow
- Read `docs/architecture.md` before broad refactors or new pipeline work.
- For DAG changes, check `docs/codex/dag-change-checklist.md`.
- For Airflow execution and environment expectations, check `docs/codex/airflow-workflow.md`.
- For Selenium or crawler changes, check `docs/codex/crawling-gotchas.md`.
- Use `$doridang-flow-upload` for Flow posting or subtask automation work.
- Use `$doridang-data-report` to turn raw findings into the team's report markdown format.
- Use `$doridang-ppt` for static report-to-PowerPoint conversion in the Doridang workflow.
- Use `$doridang-gitpush` for branch, push, PR, and merge-safe git publishing.

## Repo Map
- `dags/` defines orchestration only; business logic should stay in `modules/`.
- `modules/transform/pipelines/` contains most business logic.
- `scripts/` is for one-off analysis and verification, not DAG runtime logic.
- `docs/` contains stable reference material and decision records.

## Working Rules
- Prefer minimal, targeted edits over large rewrites.
- Keep DAG files thin and move reusable logic into `modules/`.
- Reuse existing utility modules before adding new helpers.
- Preserve Windows vs WSL environment separation.

## Verification
- Run the smallest relevant validation first.
- For DAG work, verify importability or targeted script behavior before broader tests.
- If an instruction differs between this file and `CLAUDE.md`, Codex should follow this file.
