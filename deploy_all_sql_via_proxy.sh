#!/bin/bash
set -e

echo "📦 Deploying SQL files via proxy to Supabase project: ${PROJECT_REF}"
echo "📁 Using SQL directory: ./scripts"

deploy_sql_file() {
  local file="$1"
  echo "🚀 Applying: $file"
  local SQL_CONTENT
  SQL_CONTENT=$(cat "$file")

  local RESPONSE
  RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$SQL_PROXY_URL" \
    -H "Authorization: Bearer $SQL_PROXY_SECRET" \
    -H "Content-Type: application/json" \
    -d "{\"sql_file\": \"$(basename "$file")\", \"sql_content\": $(jq -Rs . <<<"$SQL_CONTENT")}"
  )

  HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)
  RESPONSE_BODY=$(echo "$RESPONSE" | sed '$d')

  if [ "$HTTP_STATUS" -eq 200 ]; then
    echo "✅ Success: $file"
  else
    echo "❌ Failed to apply $file (HTTP $HTTP_STATUS)"
    echo "Response: $RESPONSE_BODY"
    exit 1
  fi
}

for file in ./scripts/*.sql; do
  if [ -f "$file" ]; then
    deploy_sql_file "$file"
  fi
done

echo "✅ All SQL files deployed successfully!"
