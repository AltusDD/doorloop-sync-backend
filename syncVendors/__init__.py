import logging
logging.info(">>> Global import reached: syncVendors")

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(">>> Entered main() in syncVendors")
    try:
        import os
        import requests
        import json

        logging.info(">>> All imports successful in syncVendors")

        api_key = os.environ.get("DOORLOOP_API_KEY")
        if not api_key:
            raise Exception("Missing DOORLOOP_API_KEY")

        headers = {
            "Authorization": f"Bearer {api_key}"
        }
        url = f"https://api.doorloop.com/v1/vendors?page=1"
        res = requests.get(url, headers=headers)
        res.raise_for_status()

        data = res.json()
        count = len(data.get("data", []))
        logging.info(f"✅ Pulled {count} records from vendors")
        return func.HttpResponse(f"✅ Pulled {count} records from vendors", status_code=200)

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logging.error(f"❌ Failed in syncVendors: {str(e)}\n{tb}")
        return func.HttpResponse(f"❌ Error in syncVendors: {str(e)}\n{tb}", status_code=500)
