#!/bin/bash
PROJECT_REF="your_project_ref"
SQL_PROXY_SECRET="your_sql_proxy_secret"

deploy_sql() {
  local file=$1
  echo "🚀 Deploying $file"
  sql_content=$(cat "$file")
  response=$(curl -s -X POST "https://$PROJECT_REF.supabase.co/functions/v1/sql-proxy"     -H "Authorization: Bearer $SQL_PROXY_SECRET"     -H "Content-Type: application/json"     -d "{"sql_file": "$file", "sql_content": "$sql_content"}")
  echo "$response"
  if echo "$response" | grep -q "error"; then
    echo "❌ Failed to deploy $file"
    return 1
  else
    echo "✅ Successfully deployed $file"
    return 0
  fi
}

mkdir -p deployment_logs

echo "📁 Step 1: Deploying base tables..."
for table in tables/doorloop_raw_*.sql; do
  [ -f "$table" ] && deploy_sql "$table" | tee -a deployment_logs/tables.log && sleep 1
done

echo "📁 Step 2: Deploying normalized views..."
for view in views/normalized_*.sql; do
  [ -f "$view" ] && deploy_sql "$view" | tee -a deployment_logs/normalized_views.log && sleep 1
done

echo "📁 Step 3: Deploying full views..."
for view in views/get_full_*.sql; do
  [ -f "$view" ] && deploy_sql "$view" | tee -a deployment_logs/full_views.log && sleep 1
done

echo "📁 Step 4: Deploying sync views..."
for view in views/sync_*.sql; do
  [ -f "$view" ] && deploy_sql "$view" | tee -a deployment_logs/sync_views.log && sleep 1
done

echo "✅ Database deployment completed!"
