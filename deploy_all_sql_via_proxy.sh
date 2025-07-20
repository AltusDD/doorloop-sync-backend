#!/bin/bash

# -----------------------------------------
# 🚀 Altus Empire SQL Auto-Deploy via Proxy
# Uses Supabase SQL Proxy Edge Function
# -----------------------------------------

set -e

# Required environment variables
PROJECT_REF="${PROJECT_REF:-ssexobxvtuxwnblwplzh}"
SQL_DIR="${SQL_DIR:-./scripts}"
SQL_PROXY_SECRET="${SQL_PROXY_SECRET:?❌ ERROR: SQL_PROXY_SECRET is not set}"
SUPABASE_SERVICE_ROLE_KEY="${SUPABASE_SERVICE_ROLE_KEY:?❌ ERROR: SUPABASE_SERVICE_ROLE_KEY is not set}"
SQL_PROXY_URL="${SQL_PROXY_URL:-https://$PROJECT_REF.functions.supabase.co/sql-proxy}"

# Confirm SQL directory exists
if [ ! -d "$SQL_DIR" ]; then
  echo "❌ ERROR: SQL directory not found: $SQL_DIR"
  exit 1
fi

echo "📦 Deploying SQL files via proxy to Supabase project: $PROJECT_REF"
echo "📁 Using SQL directory: $SQL_DIR"

for file in $(find "$SQL_DIR" -type f -name "*.sql" | sort); do
  echo "🚀 Applying: $file"

  # Read and prepare SQL content (escaped JSON-safe format)
  sql_content=$(jq -Rs . < "$file")

  # Make request to proxy
  response=$(curl -s -w "\n%{http_code}" -X POST "$SQL_PROXY_URL" \
    -H "Authorization: Bearer $SUPABASE_SERVICE_ROLE_KEY" \
    -H "x-proxy-secret: $SQL_PROXY_SECRET" \
    -H "Content-Type: application/json" \
    -d "{\"sql_file\": \"$(basename "$file")\", \"sql_content\": $sql_content}")

  http_code=$(echo "$response" | tail -n1)
  body=$(echo "$response" | sed '$d')

  if [ "$http_code" -ne 200 ]; then
    echo "❌ Failed to apply $file (HTTP $http_code)"
    echo "Response: $body"
    exit 1
  else
    echo "✅ Success: $(basename "$file")"
  fi
done

echo "🎉 All SQL files deployed successfully!"
