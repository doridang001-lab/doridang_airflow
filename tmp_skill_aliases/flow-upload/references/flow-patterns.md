# Doridang Flow Patterns

## Choose a Mode
- Use the post mode when the task writes a new article in a Flow project board.
- Use the subtask mode when the task appends date-based work items inside an existing project.

## Workspace References
- Subtask pattern:
  - `dags/sales/Sales_Orders_09_FlowReport_Dags.py`
- Post pattern:
  - `dags/strategy/Strategy_FdamCS_02_FlowMacro_Dags.py`
- Browser launch and login helpers:
  - `modules/transform/pipelines/sales/SMD_sales_visit_log_01_crawling.py`

## Post Mode Rules
- Title is a normal input; `send_keys` is acceptable.
- Body uses `CKEDITOR.instances['postContents']` when available.
- Scroll the project item and submit button into view before clicking.
- Keep the editor render wait; shorter waits are known to be unstable.

## Subtask Mode Rules
- Wait for the project list before clicking a specific project.
- Scroll the sidebar and the add button before clicking.
- For the subtask input, use JS value assignment and dispatch input/change/Enter events.
- For the body editor, use the last CKEditor instance when the instance is unnamed.

## HTML Body Shapes
- Use list-style HTML for subtask detail pages.
- Use table-style HTML for project post summaries.
- Escape HTML content before inserting dynamic text.

## DAG Structure
- Keep Flow work inside the correct `dags/sales/` or `dags/strategy/` path.
- Use schedule constants instead of hardcoded cron strings.
- When a sensor uses `soft_fail=True`, keep the Flow posting task on `TriggerRule.NONE_FAILED`.
