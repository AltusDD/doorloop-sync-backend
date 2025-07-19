#!/bin/bash
# Fix and deploy all normalized views with schema qualification from GitHub Actions

PROJECT_REF="${PROJECT_REF:-REPLACE_ME}"
SQL_PROXY_SECRET="${SQL_PROXY_SECRET:-REPLACE_ME}"

if [ "$PROJECT_REF" = "REPLACE_ME" ] || [ "$SQL_PROXY_SECRET" = "REPLACE_ME" ]; then
  echo "❌ ERROR: PROJECT_REF or SQL_PROXY_SECRET is not set."
  exit 1
fi

echo "🔧 Starting fix for all normalized, full, and sync views..."

# Deploy views with a specific prefix
deploy_views() {
  local pattern=$1
  local label=$2

  for file in views/${pattern}_*.sql; do
    if [ ! -f "$file" ]; then
      echo "⚠️ View file not found: $file"
      continue
    fi

    echo "🚀 Fixing and deploying ($label): $file"
    temp_file="${file}.temp"
    cat "$file" | sed 's/FROM doorloop_raw_/FROM public.doorloop_raw_/g' |                   sed 's/JOIN doorloop_raw_/JOIN public.doorloop_raw_/g' |                   sed 's/FROM doorloop_normalized_/FROM public.doorloop_normalized_/g' |                   sed 's/JOIN doorloop_normalized_/JOIN public.doorloop_normalized_/g' > "$temp_file"

    sql_content=$(cat "$temp_file")

    response=$(curl -s -X POST "https://${PROJECT_REF}.supabase.co/functions/v1/sql-proxy"       -H "Authorization: Bearer $SQL_PROXY_SECRET"       -H "Content-Type: application/json"       -d "{"sql_file": "$file", "sql_content": "$sql_content"}")

    if echo "$response" | grep -q "error"; then
      echo "❌ Failed to deploy $file:"
      echo "$response"
    else
      echo "✅ Successfully deployed $file"
    fi

    rm -f "$temp_file"
  done
}

deploy_views "normalized" "Normalized View"
deploy_views "get_full" "Full View"
deploy_views "sync" "Sync View"

echo "🎉 All views processed."
