import logging
logging.info(">>> Loaded syncLeases")

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(">>> Entered main() in syncLeases")
    try:
        import os
        import requests
        import json
        import traceback

        api_key = os.getenv("DOORLOOP_API_KEY")
        if not api_key:
            raise Exception("Missing DOORLOOP_API_KEY")

        headers = {
            "Authorization": f"Bearer {api_key}"
        }
        url = f"https://api.doorloop.com/v1/leases?page=1"
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        count = len(data.get("data", []))
        logging.info(f"✅ Pulled {count} records from leases")
        return func.HttpResponse(f"✅ Pulled {count} records from leases", status_code=200)
    except Exception as e:
        logging.error("❌ Exception occurred")
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"❌ ERROR in syncLeases: " + str(e), status_code=500)
