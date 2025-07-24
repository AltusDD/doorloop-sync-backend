import os
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SQL_PROXY_ENDPOINT = f"{SUPABASE_URL}/functions/v1/sql-proxy"

headers = {
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

# 1. Query all materialized views in the public or private schema
fetch_views_sql = """
SELECT schemaname || '.' || matviewname AS full_name
FROM pg_matviews
WHERE schemaname IN ('public', 'private');
"""

def execute_sql(sql):
    response = requests.post(
        SQL_PROXY_ENDPOINT,
        headers=headers,
        json={"sql": sql}
    )
    return response.json()

# Step 1: Get all mat views
views_result = execute_sql(fetch_views_sql)

if "data" in views_result and isinstance(views_result["data"], list):
    for view in views_result["data"]:
        view_name = view.get("full_name")
        print(f"🔁 Refreshing: {view_name}")
        refresh_result = execute_sql(f"REFRESH MATERIALIZED VIEW {view_name};")
        print("✅", refresh_result)
else:
    print("❌ Failed to list views:", views_result)
