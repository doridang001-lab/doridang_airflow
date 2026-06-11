---
name: doridang-gitpush
description: Publish repo changes with the Doridang branch-and-PR workflow instead of pushing directly to main. Use when Codex needs to inspect git changes, create or reuse a feature branch, run the workspace gitpush helper, open a PR, or merge the PR after user approval.
---

# Doridang Gitpush

Read [references/gitpush-workflow.md](references/gitpush-workflow.md) before publishing changes.

## Workflow
- Inspect `git status --porcelain` first.
- If there are no changes, stop and report that clearly.
- If there are changes, run the bundled wrapper script or the workspace helper it calls.
- Extract the PR URL from the output and show it to the user.
- Merge only after the user explicitly approves.

## Safety Rules
- Do not push directly to `main` or `master`.
- Prefer the existing workspace helper over handwritten git command sequences.
- Treat PR creation as the default completion point unless the user separately requests merge.
- Run merge only after an explicit approval phrase from the user.
