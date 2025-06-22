
import azure.functions as func
import logging
import os
import requests
from supabase import create_client, Client

app = func.FunctionApp()

# Environment variables required
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_BASE_URL = "https://api.doorloop.com/v1"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.function_name(name="syncOwners")
@app.route(route="syncOwners", auth_level=func.AuthLevel.FUNCTION)
def sync_owners(req: func.HttpRequest) -> func.HttpResponse:
    headers = {
        "Authorization": f"Bearer {DOORLOOP_API_KEY}"
    }

    all_owners = []
    page = 1
    while True:
        resp = requests.get(f"{DOORLOOP_BASE_URL}/owners?page={page}&limit=100", headers=headers)
        if resp.status_code != 200:
            return func.HttpResponse("Failed to fetch owners", status_code=500)

        data = resp.json()
        owners = data.get("data", [])
        if not owners:
            break

        for o in owners:
            email = next((e.get("address") for e in o.get("emails", []) if e.get("type") in ["Primary", "Work"]), None)
            phone = next((p.get("number") for p in o.get("phones", []) if p.get("type") in ["Mobile", "Work"]), None)

            all_owners.append({
                "doorloop_id": o.get("id"),
                "first_name": o.get("firstName", "Unknown"),
                "last_name": o.get("lastName", "Unknown"),
                "email": email,
                "phone": phone,
                "company_name": o.get("companyName"),
                "active": o.get("active", True)
            })
        page += 1

    for owner in all_owners:
        supabase.table("owners").upsert(owner, on_conflict=["doorloop_id"]).execute()

    return func.HttpResponse(f"✅ Synced {len(all_owners)} owners.", status_code=200)
