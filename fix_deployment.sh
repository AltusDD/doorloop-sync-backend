#!/bin/bash
# Quick fix script for the specific deployment issue

# Set these variables
PROJECT_REF="your_project_ref"
SQL_PROXY_SECRET="your_sql_proxy_secret"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to execute a SQL query and return the result
execute_query() {
  local query=$1
  local description=$2

  echo -e "${BLUE}Executing query: $description${NC}"

  response=$(curl -s -X POST "https://$PROJECT_REF.supabase.co/functions/v1/sql-proxy"     -H "Authorization: Bearer $SQL_PROXY_SECRET"     -H "Content-Type: application/json"     -d "{\"sql_file\": \"query.sql\", \"sql_content\": \"$query\"}")

  if echo "$response" | grep -q "error"; then
    echo -e "${RED}Query failed: $description${NC}"
    echo "  Response: $response"
    return 1
  fi

  echo "$response"
  return 0
}

# Function to refresh schema cache properly
refresh_schema() {
  echo -e "${BLUE}♻️ Refreshing schema cache...${NC}"

  refresh_sql="SELECT pg_catalog.pg_reload_conf();"

  response=$(execute_query "$refresh_sql" "Schema refresh")
  echo "$response"

  echo -e "${YELLOW}⏳ Waiting for schema refresh to take effect (15 seconds)...${NC}"
  sleep 15
}

# Function to check if tables exist and are visible
check_tables() {
  echo -e "${BLUE}📋 Checking if tables exist and are visible...${NC}"

  query="SELECT table_schema, table_name FROM information_schema.tables WHERE table_name LIKE 'doorloop_raw_%' ORDER BY table_schema, table_name;"

  result=$(execute_query "$query" "Check tables")
  echo "$result"

  if echo "$result" | grep -q "doorloop_raw_"; then
    echo -e "${GREEN}✅ Tables found!${NC}"
    return 0
  else
    echo -e "${RED}❌ No doorloop_raw_ tables found!${NC}"
    return 1
  fi
}

# Function to check search_path
check_search_path() {
  echo -e "${BLUE}📋 Checking current search_path...${NC}"

  query="SHOW search_path;"

  result=$(execute_query "$query" "Check search_path")
  echo -e "${GREEN}Current search_path: $result${NC}"

  echo -e "${BLUE}Setting search_path to include public schema...${NC}"

  query="SET search_path TO public, pg_catalog;"

  result=$(execute_query "$query" "Set search_path")
  echo "$result"

  query="SHOW search_path;"

  result=$(execute_query "$query" "Verify search_path")
  echo -e "${GREEN}Updated search_path: $result${NC}"
}

# Function to check schema of a specific table
check_table_schema() {
  local table_name=$1

  echo -e "${BLUE}📋 Checking schema for table: $table_name${NC}"

  query="SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = '$table_name';"

  result=$(execute_query "$query" "Check table schema")
  echo "$result"
}

# Function to fix view deployment
fix_view_deployment() {
  local view_file=$1
  local view_name=$(basename "$view_file" .sql)

  echo -e "${BLUE}🔧 Fixing view deployment for: $view_name${NC}"

  if [ ! -f "$view_file" ]; then
    echo -e "${RED}❌ View file not found: $view_file${NC}"
    return 1
  fi

  temp_file="${view_file}.temp"
  cat "$view_file" | sed 's/FROM doorloop_raw_/FROM public.doorloop_raw_/g' |                      sed 's/JOIN doorloop_raw_/JOIN public.doorloop_raw_/g' > "$temp_file"

  echo -e "${YELLOW}Modified view SQL with schema qualification:${NC}"
  cat "$temp_file"

  echo -e "${BLUE}🚀 Deploying modified view: $view_name${NC}"

  sql_content=$(cat "$temp_file")

  response=$(curl -s -X POST "https://$PROJECT_REF.supabase.co/functions/v1/sql-proxy"     -H "Authorization: Bearer $SQL_PROXY_SECRET"     -H "Content-Type: application/json"     -d "{\"sql_file\": \"$view_file\", \"sql_content\": \"$sql_content\"}")

  if echo "$response" | grep -q "error"; then
    echo -e "${RED}❌ View deployment failed:${NC}"
    echo "  Response: $response"
    rm "$temp_file"
    return 1
  else
    echo -e "${GREEN}✅ View deployed successfully!${NC}"
    rm "$temp_file"
    return 0
  fi
}

echo -e "${BLUE}=== Starting deployment fix ===${NC}"
check_tables
check_search_path
check_table_schema "doorloop_raw_leases"
check_table_schema "doorloop_raw_accounts"
refresh_schema

if [ -f "views/normalized_leases.sql" ]; then
  fix_view_deployment "views/normalized_leases.sql"
else
  echo -e "${YELLOW}⚠️ View file not found: views/normalized_leases.sql${NC}"
  echo -e "${YELLOW}Please provide the path to the view file you want to fix.${NC}"
fi

echo -e "${BLUE}=== Deployment fix completed ===${NC}"
echo -e "${GREEN}You can now continue with your regular deployment script.${NC}"
