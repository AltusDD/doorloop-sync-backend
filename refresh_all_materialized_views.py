import os
import requests

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

SQL_PROXY_ENDPOINT = f"{SUPABASE_URL}/functions/v1/sql-proxy"
HEADERS = {
    "Authorization": SUPABASE_SERVICE_ROLE_KEY,
    "Content-Type": "application/json"
}

def refresh_all_views():
    sql = '''
    DO $$
    DECLARE
        view_record RECORD;
    BEGIN
        FOR view_record IN
            SELECT schemaname, matviewname
            FROM pg_matviews
            WHERE schemaname IN ('public', 'private')
        LOOP
            EXECUTE format('REFRESH MATERIALIZED VIEW %I.%I', view_record.schemaname, view_record.matviewname);
        END LOOP;
    END
    $$;
    '''

    response = requests.post(SQL_PROXY_ENDPOINT, headers=HEADERS, json={"sql": sql})
    if response.status_code == 200:
        print("✅ Successfully refreshed all materialized views.")
    else:
        print("❌ Failed to refresh views:", response.text)

if __name__ == "__main__":
    refresh_all_views()
