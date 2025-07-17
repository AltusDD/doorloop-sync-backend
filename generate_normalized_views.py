import os
import requests

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

def execute_sql(sql):
    response = requests.post(f"{SUPABASE_URL}/rest/v1/rpc/execute_sql", headers=HEADERS, json={"sql": sql})
    print(f"Executing SQL:
{sql}
Status: {response.status_code}, Response: {response.text}")
    if not response.ok:
        raise Exception(f"SQL execution failed: {response.text}")

if __name__ == "__main__":
    normalize_dir = "normalize"
    for filename in os.listdir(normalize_dir):
        if filename.endswith(".sql"):
            with open(os.path.join(normalize_dir, filename), "r") as f:
                sql = f.read()
            try:
                execute_sql(sql)
            except Exception as e:
                print(f"❌ Failed to execute {filename}: {e}")