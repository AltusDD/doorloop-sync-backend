
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

@app.function_name(name="syncLeasePayments")
@app.route(route="syncLeasePayments", auth_level=func.AuthLevel.FUNCTION)
def sync_lease_payments(req: func.HttpRequest) -> func.HttpResponse:
    headers = {"Authorization": f"Bearer {DOORLOOP_API_KEY}"}
    all_payments = []
    page = 1

    lease_map = {r["doorloop_id"]: r["id"] for r in supabase.table("leases").select("id, doorloop_id").execute().data}

    while True:
        resp = requests.get(f"{DOORLOOP_BASE_URL}/lease-payments?page={page}&limit=100", headers=headers)
        if resp.status_code != 200:
            return func.HttpResponse("Failed to fetch lease payments", status_code=500)

        data = resp.json().get("data", [])
        if not data:
            break

        for p in data:
            lease_id = lease_map.get(p.get("lease"))
            all_payments.append({
                "doorloop_id": p.get("id"),
                "amount": p.get("amount", 0),
                "payment_date": p.get("date"),
                "status": p.get("status"),
                "payment_method": p.get("paymentMethod"),
                "memo": p.get("memo"),
                "lease_id": lease_id
            })

        page += 1

    for payment in all_payments:
        supabase.table("lease_payments").upsert(payment, on_conflict=["doorloop_id"]).execute()

    return func.HttpResponse(f"✅ Synced {len(all_payments)} lease payments.", status_code=200)
