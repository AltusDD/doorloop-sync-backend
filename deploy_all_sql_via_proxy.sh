#!/bin/bash

# -----------------------------------------
# 🚀 Altus Empire SQL Auto-Deploy Script
# Uses Supabase SQL Proxy (Edge Function)
# -----------------------------------------

# REQUIRED ENV VARS
#   - SUPABASE_SERVICE_ROLE_KEY
#   - PROJECT_REF (e.g., ssexobxvtuxwnblwplzh)
#   - SQL_DIR (defaults to ./scripts)

set -e

# Set default script directory if not provided
SQL_DIR="${SQL_DIR:-./scripts}"
PROJECT_REF="${PROJECT_REF:-ssexobxvtuxwnblwplzh}"

if [ -z "$SUPABASE_SERVICE_ROLE_KEY" ]; then
  echo "❌ ERROR: SUPABASE_SERVICE_ROLE_KEY is not set."
  exit 1
fi

if [ ! -d "$SQL_DIR" ]; then
  echo "❌ ERROR: SQL directory not found: $SQL_DIR"
  exit 1
fi

echo "📦 Deploying SQL files via proxy to Supabase project: $PROJECT_REF"
echo "📁 Using SQL directory: $SQL_DIR"

for file in $(find "$SQL_DIR" -type f -name "*.sql" | sort); do
  echo "🚀 Applying: $file"

  sql_content=$(<"$file" sed ':a;N;$!ba;s/\n/\\n/g' | sed 's/"/\"/g')

  response=$(curl -s -X POST "https://$PROJECT_REF.supabase.co/functions/v1/sql-proxy" \
    -H "Authorization: Bearer $SUPABASE_SERVICE_ROLE_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"sql_file\": \"$(basename "$file")\", \"sql_content\": \"$sql_content\"}")

  if echo "$response" | grep -q '"error"'; then
    echo "❌ Failed: $file"
    echo "Response: $response"
    exit 1
  else
    echo "✅ Success: $file"
  fi
done

echo "🎉 All SQL scripts applied successfully!"
