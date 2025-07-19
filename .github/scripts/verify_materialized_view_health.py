
import os
import requests

SQL_PROXY_SECRET = os.getenv("SQL_PROXY_SECRET")
PROJECT_REF = os.getenv("PROJECT_REF")

if not SQL_PROXY_SECRET or not PROJECT_REF:
    raise EnvironmentError("Missing SQL_PROXY_SECRET or PROJECT_REF environment variables.")

endpoint = f"https://{PROJECT_REF}.supabase.co/functions/v1/sql-proxy"
query = {
    "sql": "select * from materialized_view_healthcheck()"
}

response = requests.post(endpoint, json=query, headers={
    "Authorization": f"Bearer {SQL_PROXY_SECRET}",
    "Content-Type": "application/json"
})

if not response.ok:
    raise RuntimeError(f"Request failed: {response.status_code} - {response.text}")

data = response.json()
print("✅ Materialized View Healthcheck Passed")
print(data)
