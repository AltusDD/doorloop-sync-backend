
# Azure Sync Health Monitor Function

## What It Does
- Runs every 15 minutes
- Checks `doorloop_pipeline_audit` for errors in the last 15 minutes
- Sends Teams alert if errors are found

## Environment Variables (via Azure App Settings)
- `SUPABASE_DB`
- `SUPABASE_USER`
- `SUPABASE_PASS`
- `SUPABASE_HOST`
- `TEAMS_WEBHOOK`

## Deployment
Zip and deploy as an Azure Function App (Python)
