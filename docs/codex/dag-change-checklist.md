# Codex DAG Change Checklist

## Before Editing
- Confirm whether the change belongs in `dags/` or in `modules/transform/pipelines/`.
- Check naming and schedule conventions in root `AGENTS.md`.
- Identify existing helpers in `modules/transform/utility/`.

## During Editing
- Keep `dag_id=Path(__file__).stem` and existing orchestration conventions unless the change requires otherwise.
- Avoid embedding heavy business logic directly in the DAG definition.
- Match existing conf handling patterns for `sale_date` and `backfill` where applicable.

## Before Finishing
- Verify imports or run the narrowest relevant test command.
- Review downstream assumptions such as output paths, parquet schemas, and alert/report tasks.
- Check whether the change introduces new operational gotchas that belong in Codex docs.
