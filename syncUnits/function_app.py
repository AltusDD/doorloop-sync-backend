
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

@app.function_name(name="syncUnits")
@app.route(route="syncUnits", auth_level=func.AuthLevel.FUNCTION)
def sync_units(req: func.HttpRequest) -> func.HttpResponse:
    headers = {"Authorization": f"Bearer {DOORLOOP_API_KEY}"}
    all_units = []
    page = 1

    # Property map for foreign key linking
    property_map = {}
    props = supabase.table("properties").select("id, doorloop_id").execute()
    for prop in props.data:
        property_map[prop["doorloop_id"]] = prop["id"]

    while True:
        resp = requests.get(f"{DOORLOOP_BASE_URL}/units?page={page}&limit=100", headers=headers)
        if resp.status_code != 200:
            return func.HttpResponse("Failed to fetch units", status_code=500)

        data = resp.json().get("data", [])
        if not data:
            break

        for u in data:
            property_id = property_map.get(u.get("property"))
            all_units.append({
                "doorloop_id": u.get("id"),
                "name": u.get("name"),
                "bedroom_count": u.get("bedrooms"),
                "bathroom_count": u.get("bathrooms"),
                "market_rent": u.get("marketRent"),
                "square_footage": u.get("squareFeet"),
                "unit_type": u.get("type"),
                "property_id": property_id,
                "active": u.get("active", True)
            })

        page += 1

    for unit in all_units:
        supabase.table("units").upsert(unit, on_conflict=["doorloop_id"]).execute()

    return func.HttpResponse(f"✅ Synced {len(all_units)} units.", status_code=200)
