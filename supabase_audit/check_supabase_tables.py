
import os
import requests

SUPABASE_URL = os.getenv("SUPABASE_URL")
SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HEADERS = {
    "apikey": SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SERVICE_ROLE_KEY}"
}

TABLES = [
    "doorloop_normalized_properties", "doorloop_normalized_units", "doorloop_normalized_leases",
    "doorloop_normalized_tenants", "doorloop_normalized_owners", "doorloop_normalized_tasks"
]

def audit_table(table):
    url = f"{SUPABASE_URL}/rest/v1/{table}?select=count"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        print(f"[✅] {table}: {response.json()}")
    else:
        print(f"[❌] {table} - {response.status_code} - {response.text}")

for table in TABLES:
    audit_table(table)
