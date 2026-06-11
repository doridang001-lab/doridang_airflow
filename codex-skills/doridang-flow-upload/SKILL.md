---
name: doridang-flow-upload
description: Design or update Doridang Flow automation that posts reports or creates subtasks in Flow with the established Selenium patterns. Use when Codex needs to implement Flow posting, Flow report DAGs, Flow subtask creation, or any automation that interacts with the Doridang Flow project UI.
---

# Doridang Flow Upload

Read [references/flow-patterns.md](references/flow-patterns.md) before writing Flow automation.

## Workflow
- Decide whether the task is a post-writing flow or a subtask-creation flow.
- Copy the verified Selenium pattern from the matching workspace reference, not a fresh from-scratch approach.
- Build the HTML body in the expected list or table shape before wiring the browser steps.
- Keep `try/finally: driver.quit()` around the browser lifecycle.
- Place the final DAG in the correct domain directory and keep orchestration thin.

## Critical Rules
- Do not use `send_keys` for readonly subtask inputs; dispatch JS events instead.
- Do not mix the CKEditor instance selection rules between the post mode and subtask mode.
- Keep the proven render waits and scrolling steps; they are part of correctness, not polish.
- Pair `soft_fail=True` sensors with `TriggerRule.NONE_FAILED` on the downstream post task.
