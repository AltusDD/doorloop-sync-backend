#!/bin/bash

# Function to deploy a SQL file and check its status
deploy_sql_file() {
  local file="$1"
  local sql_content=$(cat "$file")
  local json_payload=$(jq -n     --arg sql_file "$file"     --arg sql_content "$sql_content"     '{sql_file: $sql_file, sql_content: $sql_content}')

  echo "🚀 Deploying $file"
  RESPONSE=$(curl -s -X POST "$SQL_PROXY_URL"     -H "Authorization: Bearer $SQL_PROXY_SECRET"     -H "Content-Type: application/json"     -d "$json_payload")

  local edge_fn_success=$(echo "$RESPONSE" | jq -r '.success // "false"')
  local db_error_status=$(echo "$RESPONSE" | jq -r '.data.status // "OK"')

  if [ "$edge_fn_success" = "true" ] && [ "$db_error_status" = "error" ]; then
    local db_error_message=$(echo "$RESPONSE" | jq -r '.data.message')
    local db_error_detail=$(echo "$RESPONSE" | jq -r '.data.detail')
    local db_error_code=$(echo "$RESPONSE" | jq -r '.data.code')
    echo "❌ Database SQL Error for $file:"
    echo "  Code: $db_error_code (SQLSTATE)"
    echo "  Message: $db_error_message"
    echo "  Detail: $db_error_detail"
    echo "  Full response: $RESPONSE"
    return 1
  elif [ "$edge_fn_success" != "true" ]; then
    echo "❌ Edge Function Error for $file:"
    echo "  Response: $RESPONSE"
    return 1
  else
    echo "✅ SQL execution succeeded for $file."
    return 0
  fi
}

# Ensure jq is installed
if ! command -v jq &> /dev/null; then
  echo "jq is not installed. Please install it to run this script."
  exit 1
fi

echo "📁 Deploying tables..."
for file in $(ls tables/*.sql | sort); do
  deploy_sql_file "$file" || exit 1
done

echo "✅ All tables deployed. Forcing schema refresh and waiting 10 seconds..."
curl -s "$SQL_PROXY_URL"   -H "Authorization: Bearer $SQL_PROXY_SECRET"   -H "Content-Type: application/json"   -d '{"sql_file":"schema_refresh", "sql_content":"SELECT * FROM public.doorloop_raw_leases LIMIT 1;"}' || true

sleep 10

echo "📁 Deploying normalized views..."
for file in $(ls views/normalized_*.sql | sort); do
  deploy_sql_file "$file" || exit 1
done

echo "📁 Deploying full views..."
for file in $(ls views/get_full_*.sql | sort); do
  deploy_sql_file "$file" || exit 1
done

echo "📁 Deploying sync views..."
for file in $(ls views/sync_*.sql | sort); do
  deploy_sql_file "$file" || exit 1
done
