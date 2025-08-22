---
# Agent Guide — Altus Empire

## Golden Rules
1) Never push directly to `main`. Always use PRs.
2) Keep diffs small and scoped; if touching shared libs or DB, include migration notes.
3) Tests required for any new handler or bug fix.
4) No hardcoded counts or dashboards on the frontend; data must come from backend DB/API.
5) Deploys use GitHub Actions with Azure **OIDC** only (no publish profiles or long-lived secrets).
6) Use feature flags instead of breaking deletes or behavior changes.

## Project Map (high level)
- doorloop-sync-backend — Azure Functions backend
- altus-dealroom-backend — Azure Functions backend
- altus-dropbox-integration — Azure Functions backend
- empirecommandcenter — React UI (Command Center)
- dealroom-ui — React UI (Deal Room)

## PR Expectations
- Use the PR template (What/Why, Tests, Risk, Rollback).
- Link issues/tickets. Include screenshots for UI.
- If changing routes or envs, update ROUTES.md and smoke tests.
- All changes land via PR; staging first, production via approval only.
---