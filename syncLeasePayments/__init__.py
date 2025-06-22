import azure.functions as func
import os
import requests
import json
import traceback

def main(req: func.HttpRequest) -> func.HttpResponse:
    test_mode = req.params.get('test') == 'true'
    doorloop_api_key = os.environ.get("DOORLOOP_API_KEY")
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    base_url = "https://api.doorloop.com/v1/leasepayments"
    headers = {"Authorization": f"Bearer {doorloop_api_key}" }
    results = []
    page = 1

    try:
        while True:
            r = requests.get(f"{base_url}?page={page}", headers=headers)
            r.raise_for_status()
            data = r.json()
            page_data = data.get("data", []) if isinstance(data, dict) else data
            if not page_data:
                break
            results.extend(page_data)
            if len(page_data) < 50:
                break
            page += 1

        if test_mode:
            return func.HttpResponse(f"✅ [TEST MODE] syncLeasePayments - {len(results)} records fetched", status_code=200)

        resp = requests.post(
            f"{supabase_url}/rest/v1/leasepayments",
            headers={
                "apikey": supabase_key,
                "Authorization": f"Bearer {supabase_key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates"
            },
            data=json.dumps(results)
        )
        resp.raise_for_status()
        return func.HttpResponse(f"✅ syncLeasePayments completed: {len(results)} records synced", status_code=200)

    except Exception as e:
        tb = traceback.format_exc()
        return func.HttpResponse(f"❌ ERROR: {str(e)}\nTraceback:\n{tb}", status_code=500)
