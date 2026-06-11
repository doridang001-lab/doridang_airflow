# Codex Airflow Workflow

## Environment
- Use `.venv` on Windows for editing and local Python checks.
- Use `.venv_wsl` on WSL for runtime-oriented commands.
- Do not mix Windows and WSL virtual environments in one workflow.

## Runtime Basics
- Airflow services are managed through `docker compose`.
- Scheduler logs are the first stop for DAG-level failures.
- Task logs inside the worker or scheduler containers are the source of truth for task execution errors.

## Change Strategy
- Keep orchestration in `dags/` and move data logic into `modules/transform/pipelines/`.
- When adding behavior, search for an existing pipeline in the same domain before creating a new pattern.
- Prefer targeted smoke checks over full-system runs unless the change affects shared infrastructure.
