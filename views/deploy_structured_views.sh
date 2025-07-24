#!/bin/bash
echo "📁 Deploying normalized views..."
for file in views/sql/normalized/*.sql; do
  echo "🚀 Deploying $file"
  curl -X POST "$SQL_PROXY_URL"     -H "Authorization: $SQL_PROXY_SECRET"     -H "Content-Type: application/json"     -d '{"sql_file":"'"$file"'", "sql_content":"'"$(cat "$file" | sed ':a;N;$!ba;s/\n/ /g')"'" }'
done

echo "📁 Deploying full views..."
for file in views/sql/full/*.sql; do
  echo "🚀 Deploying $file"
  curl -X POST "$SQL_PROXY_URL"     -H "Authorization: $SQL_PROXY_SECRET"     -H "Content-Type: application/json"     -d '{"sql_file":"'"$file"'", "sql_content":"'"$(cat "$file" | sed ':a;N;$!ba;s/\n/ /g')"'" }'
done

echo "📁 Deploying sync views..."
for file in views/sql/sync/*.sql; do
  echo "🚀 Deploying $file"
  curl -X POST "$SQL_PROXY_URL"     -H "Authorization: $SQL_PROXY_SECRET"     -H "Content-Type: application/json"     -d '{"sql_file":"'"$file"'", "sql_content":"'"$(cat "$file" | sed ':a;N;$!ba;s/\n/ /g')"'" }'
done
