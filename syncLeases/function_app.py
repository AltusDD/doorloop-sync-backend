
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

@app.function_name(name="syncLeases")
@app.route(route="syncLeases", auth_level=func.AuthLevel.FUNCTION)
def sync_leases(req: func.HttpRequest) -> func.HttpResponse:
    headers = {"Authorization": f"Bearer {DOORLOOP_API_KEY}"}
    all_leases = []
    page = 1

    # Load maps for foreign key resolution
    unit_map = {r["doorloop_id"]: r["id"] for r in supabase.table("units").select("id, doorloop_id").execute().data}
    prop_map = {r["doorloop_id"]: r["id"] for r in supabase.table("properties").select("id, doorloop_id").execute().data}
    tenant_map = {r["doorloop_id"]: r["id"] for r in supabase.table("tenants").select("id, doorloop_id").execute().data}

    while True:
        resp = requests.get(f"{DOORLOOP_BASE_URL}/leases?page={page}&limit=100", headers=headers)
        if resp.status_code != 200:
            return func.HttpResponse("Failed to fetch leases", status_code=500)

        data = resp.json().get("data", [])
        if not data:
            break

        for l in data:
            all_leases.append({
                "doorloop_id": l.get("id"),
                "status": l.get("status"),
                "start_date": l.get("start"),
                "end_date": l.get("end"),
                "total_recurring_rent": l.get("totalRecurringRent"),
                "total_balance_due": l.get("totalBalanceDue"),
                "security_deposit": l.get("totalDepositsHeld", 0),
                "property_id": prop_map.get(l.get("property")),
                "unit_id": unit_map.get(l.get("unit")),
                "tenant_id": tenant_map.get(l.get("tenant"))
            })

        page += 1

    for lease in all_leases:
        supabase.table("leases").upsert(lease, on_conflict=["doorloop_id"]).execute()

    return func.HttpResponse(f"✅ Synced {len(all_leases)} leases.", status_code=200)
