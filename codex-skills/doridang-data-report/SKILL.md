---
name: doridang-data-report
description: Structure raw findings, hypotheses, notes, and tables into the Doridang report markdown format and save the result to the team's OneDrive report folder. Use when Codex needs to turn incomplete analysis inputs into a standardized report, preserve comparison tables, or produce a sharable markdown report file.
---

# Doridang Data Report

Read [references/report-format.md](references/report-format.md) before drafting the report.

## Workflow
- Gather the available inputs: notes, datasets, hypotheses, draft prose, or ad hoc conclusions.
- Map the input into the standard report sections and mark any gaps that cannot be inferred.
- Convert comparisons into markdown tables whenever the source contains period-over-period or segment comparisons.
- Draft the full report in markdown, then save it to the Doridang OneDrive report directory.
- Return the saved path and the major sections included.

## Quality Rules
- Keep the report structured even when the source is incomplete.
- Use `[DATA NEEDED]` only where missing evidence blocks a concrete claim.
- Keep conclusions and recommendations explicit and easy to scan.
- Preserve numeric facts exactly and compute comparison deltas when the data supports them.
