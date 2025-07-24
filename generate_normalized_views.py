import os
import sys
import json
import requests
import glob
from urllib.parse import urlparse

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SQL_PROXY_SECRET = os.environ.get("SQL_PROXY_SECRET")
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "normalized_sql")

def execute_sql_via_edge_function(sql_file, sql_content):
    if not SUPABASE_URL:
        print("❌ SUPABASE_URL environment variable is not set")
        sys.exit(1)

    if not SQL_PROXY_SECRET:
        print("❌ SQL_PROXY_SECRET environment variable is not set")
        sys.exit(1)

    parsed_url = urlparse(SUPABASE_URL)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    edge_function_url = f"{base_url}/functions/v1/sql-proxy"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SQL_PROXY_SECRET}"
    }

    payload = {
        "sql_file": os.path.basename(sql_file),
        "sql_content": sql_content
    }

    print(f"🔐 Using Edge Function proxy to execute SQL")
    try:
        response = requests.post(edge_function_url, headers=headers, json=payload)
        response.raise_for_status()
        result = response.json()
        if result.get("success"):
            print(f"✅ Successfully executed {os.path.basename(sql_file)}")
            return True
        else:
            print(f"❌ Error executing {os.path.basename(sql_file)}: {result.get('error')}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Error executing {os.path.basename(sql_file)}: {str(e)}")
        return False

def main():
    print("🚀 Starting normalized views generation...")

    sql_files = glob.glob(os.path.join(SQL_DIR, "*.sql"))

    if not sql_files:
        print("❌ No SQL files found in directory:", SQL_DIR)
        sys.exit(1)

    success_count = 0

    for sql_file in sql_files:
        print(f"📂 Loading SQL file: {sql_file}")
        try:
            with open(sql_file, "r") as f:
                sql_content = f.read()

            print("Executing SQL:")
            print(sql_content)

            if execute_sql_via_edge_function(sql_file, sql_content):
                success_count += 1
        except Exception as e:
            print(f"❌ Error processing {sql_file}: {str(e)}")

    print(f"✅ Processed {len(sql_files)} files with {success_count} successful executions.")

if __name__ == "__main__":
    main()
