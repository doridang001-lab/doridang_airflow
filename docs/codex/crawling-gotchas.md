# Codex Crawling Gotchas

## General
- Search for an existing crawler in the same platform before adding a new automation path.
- Keep browser lifecycle and login flows consistent with existing extract helpers.

## Baemin-Specific
- When Selenium `.text` is unreliable, prefer `innerText` on the relevant element.
- If the SPA state gets stuck, reset through the dashboard flow before assuming selector drift.
- Keep the combined multi-stage collection pattern for store-isolated browser sessions.

## Safety
- Treat new crawler behavior as high-regression work.
- Prefer small validation runs and log inspection before broader execution.
