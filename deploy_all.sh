#!/bin/bash
set -e
echo "📁 Deploying tables from /tables..."
for file in tables/*.sql; do
  echo "🚀 Deploying $file"
  curl -X POST "$SQL_PROXY_URL" -H "Authorization: $SQL_PROXY_SECRET" -H "Content-Type: application/json" -d '{"sql_file":"'"$file"'", "sql_content":"'"$(cat "$file" | sed ':a;N;$!ba;s/\n/ /g')"'" }'
  echo ""
done

echo "📁 Deploying normalized views..."
for file in views/normalized/*.sql; do
  echo "🚀 Deploying $file"
  curl -X POST "$SQL_PROXY_URL" -H "Authorization: $SQL_PROXY_SECRET" -H "Content-Type: application/json" -d '{"sql_file":"'"$file"'", "sql_content":"'"$(cat "$file" | sed ':a;N;$!ba;s/\n/ /g')"'" }'
  echo ""
done

echo "📁 Deploying get_full views..."
for file in views/get_full/*.sql; do
  echo "🚀 Deploying $file"
  curl -X POST "$SQL_PROXY_URL" -H "Authorization: $SQL_PROXY_SECRET" -H "Content-Type: application/json" -d '{"sql_file":"'"$file"'", "sql_content":"'"$(cat "$file" | sed ':a;N;$!ba;s/\n/ /g')"'" }'
  echo ""
done

echo "📁 Deploying sync views..."
for file in views/sync/*.sql; do
  echo "🚀 Deploying $file"
  curl -X POST "$SQL_PROXY_URL" -H "Authorization: $SQL_PROXY_SECRET" -H "Content-Type: application/json" -d '{"sql_file":"'"$file"'", "sql_content":"'"$(cat "$file" | sed ':a;N;$!ba;s/\n/ /g')"'" }'
  echo ""
done
