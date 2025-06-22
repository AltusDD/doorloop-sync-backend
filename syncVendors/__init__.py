import sys
import logging
logging.info(">>> Global import reached in syncVendors")

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(">>> Entered main() in syncVendors")
    try:
        import os
        import requests
        import json
        import traceback

        logging.info(">>> All imports successful in main()")

        test_mode = req.params.get('test') == 'true'
        api_key = os.environ.get("DOORLOOP_API_KEY")
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

        if not all([api_key, supabase_url, supabase_key]):
            raise Exception("Missing required env vars")

        headers = {"Authorization": f"Bearer {api_key}" }
        url = f"https://api.doorloop.com/v1/vendors?page=1"
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        records = data.get("data", []) if isinstance(data, dict) else data

        logging.info(f"✅ Pulled {len(records)} records from DoorLoop")
        return func.HttpResponse(f"✅ syncVendors pulled {len(records)} records", status_code=200)

    except Exception as e:
        tb = traceback.format_exc()
        logging.error(f"❌ Error in syncVendors: {str(e)}\n{tb}")
        return func.HttpResponse(f"❌ ERROR: {str(e)}\nTraceback:\n{tb}", status_code=500)
