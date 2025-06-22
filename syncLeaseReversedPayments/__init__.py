import azure.functions as func
import os
import requests
import json
import logging
import traceback

def main(req: func.HttpRequest) -> func.HttpResponse:
    test_mode = req.params.get('test') == 'true'
    api_key = os.environ.get("DOORLOOP_API_KEY")
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    base_url = "https://api.doorloop.com/v1/leasereversedpayments"
    headers = {"Authorization": f"Bearer {api_key}" }
    all_data = []
    page = 1

    try:
        logging.info(f">>> syncLeaseReversedPayments triggered")
        while True:
            r = requests.get(f"{base_url}?page={page}", headers=headers)
            r.raise_for_status()
            data = r.json()
            page_data = data.get("data", []) if isinstance(data, dict) else data
            if not page_data:
                break
            all_data.extend(page_data)
            if len(page_data) < 50:
                break
            page += 1

        logging.info(f"✅ Pulled {len(all_data)} records from DoorLoop for syncLeaseReversedPayments")

        if test_mode:
            return func.HttpResponse(f"✅ [TEST MODE] syncLeaseReversedPayments - {len(all_data)} records fetched", status_code=200)

        resp = requests.post(
            f"{supabase_url}/rest/v1/leasereversedpayments",
            headers={
                "apikey": supabase_key,
                "Authorization": f"Bearer {supabase_key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates"
            },
            data=json.dumps(all_data)
        )
        resp.raise_for_status()
        logging.info(f"✅ Uploaded {len(all_data)} records to Supabase for syncLeaseReversedPayments")
        return func.HttpResponse(f"✅ syncLeaseReversedPayments completed: {len(all_data)} records synced", status_code=200)

    except Exception as e:
        tb = traceback.format_exc()
        logging.error(f"❌ Error in syncLeaseReversedPayments: {str(e)}\n{tb}")
        return func.HttpResponse(f"❌ ERROR: {str(e)}\nTraceback:\n{tb}", status_code=500)
