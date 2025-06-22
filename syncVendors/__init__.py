import azure.functions as func
import logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(">>> Triggered syncVendors")
    try:
        import os
        import requests
        import json
        import traceback

        test_mode = req.params.get('test') == 'true'
        api_key = os.environ.get("DOORLOOP_API_KEY")
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

        if not api_key or not supabase_url or not supabase_key:
            raise ValueError("Missing DOORLOOP_API_KEY or SUPABASE_* environment vars")

        headers = {"Authorization": f"Bearer {api_key}" }
        url = f"https://api.doorloop.com/v1/vendors?page=1"
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        records = data.get("data", []) if isinstance(data, dict) else data
        logging.info(f"✅ Pulled {len(records)} records from DoorLoop")

        return func.HttpResponse(f"✅ syncVendors - {len(records)} records", status_code=200)

    except Exception as e:
        tb = traceback.format_exc()
        logging.error(f"❌ Error in syncVendors: {str(e)}\n{tb}")
        return func.HttpResponse(f"❌ ERROR: {str(e)}\nTraceback:\n{tb}", status_code=500)
