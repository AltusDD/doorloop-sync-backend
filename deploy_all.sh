#!/bin/bash
echo "📁 Deploying tables from /tables folder..."
for file in $(ls tables/*.sql); do
  echo "🚀 Deploying $file"
  curl -X POST "$SQL_PROXY_URL" \
    -H "Authorization: $SQL_PROXY_SECRET" \
    -H "Content-Type: application/json" \
    -d '{"sql_file":"'"$file"'", "sql_content":"'"$(cat "$file" | sed ':a;N;$!ba;s/\n/ /g')"'" }'
  echo ""
done

echo "📁 Deploying normalized views..."
for file in $(ls views/normalized_*.sql); do
  echo "🚀 Deploying $file"
  curl -X POST "$SQL_PROXY_URL" \
    -H "Authorization: $SQL_PROXY_SECRET" \
    -H "Content-Type: application/json" \
    -d '{"sql_file":"'"$file"'", "sql_content":"'"$(cat "$file" | sed ':a;N;$!ba;s/\n/ /g')"'" }'
  echo ""
done

echo "📁 Deploying full views..."
for file in $(ls views/get_full_*.sql); do
  echo "🚀 Deploying $file"
  curl -X POST "$SQL_PROXY_URL" \
    -H "Authorization: $SQL_PROXY_SECRET" \
    -H "Content-Type: application/json" \
    -d '{"sql_file":"'"$file"'", "sql_content":"'"$(cat "$file" | sed ':a;N;$!ba;s/\n/ /g')"'" }'
  echo ""
done

echo "📁 Deploying sync views..."
for file in $(ls views/sync_*.sql); do
  echo "🚀 Deploying $file"
  curl -X POST "$SQL_PROXY_URL" \
    -H "Authorization: $SQL_PROXY_SECRET" \
    -H "Content-Type: application/json" \
    -d '{"sql_file":"'"$file"'", "sql_content":"'"$(cat "$file" | sed ':a;N;$!ba;s/\n/ /g')"'" }'
  echo ""
done
