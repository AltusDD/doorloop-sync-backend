
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

@app.function_name(name="syncProperties")
@app.route(route="syncProperties", auth_level=func.AuthLevel.FUNCTION)
def sync_properties(req: func.HttpRequest) -> func.HttpResponse:
    headers = {"Authorization": f"Bearer {DOORLOOP_API_KEY}"}
    all_properties = []
    page = 1

    # Cache owners for ID resolution
    owner_map = {}
    owners_resp = supabase.table("owners").select("id, doorloop_id").execute()
    for owner in owners_resp.data:
        owner_map[owner["doorloop_id"]] = owner["id"]

    while True:
        resp = requests.get(f"{DOORLOOP_BASE_URL}/properties?page={page}&limit=100", headers=headers)
        if resp.status_code != 200:
            return func.HttpResponse("Failed to fetch properties", status_code=500)

        data = resp.json().get("data", [])
        if not data:
            break

        for p in data:
            owner_id = None
            if p.get("owners"):
                first_owner = p["owners"][0].get("owner")
                owner_id = owner_map.get(first_owner)

            all_properties.append({
                "doorloop_id": p.get("id"),
                "name": p.get("name"),
                "address_street1": p.get("address", {}).get("street1"),
                "address_city": p.get("address", {}).get("city"),
                "address_state": p.get("address", {}).get("state"),
                "address_zip": p.get("address", {}).get("zip"),
                "property_type": p.get("type"),
                "active": p.get("active", True),
                "owner_id": owner_id
            })

        page += 1

    for prop in all_properties:
        supabase.table("properties").upsert(prop, on_conflict=["doorloop_id"]).execute()

    return func.HttpResponse(f"✅ Synced {len(all_properties)} properties.", status_code=200)
