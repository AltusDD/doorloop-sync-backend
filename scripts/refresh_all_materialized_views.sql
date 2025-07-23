import os
import requests

SUPABASE_PROJECT_ID = os.getenv("SUPABASE_PROJECT_ID")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

EDGE_FUNCTION_URL = f"https://{SUPABASE_PROJECT_ID}.supabase.co/functions/v1/sql-proxy"
HEADERS = {
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

FETCH_VIEWS_SQL = """
SELECT schemaname || '.' || matviewname AS view_name
FROM pg_matviews
WHERE schemaname IN ('public', 'private');
"""

def get_materialized_views():
    payload = { "sql": FETCH_VIEWS_SQL }
    response = requests.post(EDGE_FUNCTION_URL, headers=HEADERS, json=payload)
    response.raise_for_status()
    result = response.json()
    if "data" in result and isinstance(result["data"], list):
        return [row["view_name"] for row in result["data"]]
    else:
        raise RuntimeError("❌ Failed to fetch view names.")

def refresh_view(view_name):
    sql = f'REFRESH MATERIALIZED VIEW {view_name};'
    response = requests.post(EDGE_FUNCTION_URL, headers=HEADERS, json={"sql": sql})
    result = response.json()
    if response.ok and result.get("success") and "error" not in result.get("data", {}):
        print(f"✅ Refreshed: {view_name}")
    else:
        print(f"❌ Failed to refresh: {view_name} → {result.get('data', {}).get('error')}")

def main():
    try:
        views = get_materialized_views()
        if not views:
            print("⚠️ No materialized views found.")
            return
        for view in views:
            refresh_view(view)
    except Exception as e:
        print(f"💥 Error during refresh process: {e}")

if __name__ == "__main__":
    main()
-- refresh_materialized_views.sql SQL content goes here