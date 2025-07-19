#!/bin/bash
# Fixed version using safe SQL escaping via jq -Rs
set -e

if [[ -z "$PROJECT_REF" || -z "$SQL_PROXY_SECRET" ]]; then
  echo "❌ ERROR: PROJECT_REF or SQL_PROXY_SECRET is not set."
  exit 1
fi

echo "🔧 Starting fix for all normalized, full, and sync views..."

deploy_sql_file() {
  local file="$1"
  local sql_content
  sql_content=$(jq -Rs . < "$file")  # Safe JSON escape

  local json_payload=$(jq -n     --arg sql_file "$file"     --argjson sql_content "$sql_content"     '{sql_file: $sql_file, sql_content: $sql_content}')

  response=$(curl -s -X POST "https://$PROJECT_REF.supabase.co/functions/v1/sql-proxy" \
    -H "Authorization: Bearer $SQL_PROXY_SECRET" \
    -H "Content-Type: application/json" \
    -d "$json_payload")

  if echo "$response" | grep -q "error"; then
    echo "❌ Failed to deploy $file:"
    echo "$response"
  else
    echo "✅ Successfully deployed $file"
  fi
}

process_view_type() {
  local pattern="$1"
  local label="$2"

  for view_file in $pattern; do
    if [[ -f "$view_file" ]]; then
      echo "🚀 Fixing and deploying ($label): $view_file"
      tmp_file="${view_file}.tmp"
      sed 's/FROM doorloop_/FROM public.doorloop_/g; s/JOIN doorloop_/JOIN public.doorloop_/g' "$view_file" > "$tmp_file"
      deploy_sql_file "$tmp_file"
      rm -f "$tmp_file"
    fi
  done
}

process_view_type "views/normalized_*.sql" "Normalized View"
process_view_type "views/get_full_*.sql" "Full View"
process_view_type "views/sync_*.sql" "Sync View"

echo "🎉 All views processed."
