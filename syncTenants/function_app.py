
import azure.functions as func
import logging
import os
import requests
from supabase import create_client, Client

app = func.FunctionApp()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_BASE_URL = "https://api.doorloop.com/v1"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.function_name(name="syncTenants")
@app.route(route="syncTenants", auth_level=func.AuthLevel.FUNCTION)
def sync_tenants(req: func.HttpRequest) -> func.HttpResponse:
    headers = {"Authorization": f"Bearer {DOORLOOP_API_KEY}"}
    all_tenants = []
    page = 1

    while True:
        resp = requests.get(f"{DOORLOOP_BASE_URL}/tenants?page={page}&limit=100", headers=headers)
        if resp.status_code != 200:
            return func.HttpResponse("Failed to fetch tenants", status_code=500)

        data = resp.json().get("data", [])
        if not data:
            break

        for t in data:
            email = next((e.get("address") for e in t.get("emails", []) if e.get("type") in ["Primary", "Work"]), None)
            phone = next((p.get("number") for p in t.get("phones", []) if p.get("type") in ["Mobile", "Work"]), None)

            all_tenants.append({
                "doorloop_id": t.get("id"),
                "first_name": t.get("firstName", "Unknown"),
                "last_name": t.get("lastName", "Unknown"),
                "email": email,
                "phone": phone,
                "status": t.get("status"),
                "tenant_type": t.get("type"),
                "company_name": t.get("companyName")
            })

        page += 1

    for tenant in all_tenants:
        supabase.table("tenants").upsert(tenant, on_conflict=["doorloop_id"]).execute()

    return func.HttpResponse(f"✅ Synced {len(all_tenants)} tenants.", status_code=200)
