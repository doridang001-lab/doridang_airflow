# Doridang Gitpush Workflow

## Primary Helper
- Prefer the existing workspace helper:
  - `.claude/scripts/gitpush.sh`

## Behavior
- Check for changes with `git status --porcelain`.
- If no changes exist, report `No changes` and stop.
- If on `main` or `master`, create a feature branch before commit and push.
- If already on a feature branch, keep using that branch.
- Create a PR with `gh pr create` after push.
- Show the PR URL to the user and wait for explicit approval before merge.

## Merge Rule
- Merge only after the user says a clear approval phrase such as `merge`, `머지해줘`, or `승인`.
- Use `gh pr merge --merge --delete-branch <PR_NUMBER_OR_URL>`.

## Preconditions
- `git` must be available.
- `gh` must be installed and authenticated for PR creation and merge.
- The repo must have a valid GitHub remote if PR creation is expected to succeed.
