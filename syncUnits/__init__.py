import logging
logging.info(">>> Global import reached: syncUnits")

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(">>> Entered main() in syncUnits")
    try:
        import os
        import requests
        import json

        logging.info(">>> All imports successful in syncUnits")

        api_key = os.environ.get("DOORLOOP_API_KEY")
        if not api_key:
            raise Exception("Missing DOORLOOP_API_KEY")

        headers = {
            "Authorization": f"Bearer {api_key}"
        }
        url = f"https://api.doorloop.com/v1/units?page=1"
        res = requests.get(url, headers=headers)
        res.raise_for_status()

        data = res.json()
        count = len(data.get("data", []))
        logging.info(f"✅ Pulled {count} records from units")
        return func.HttpResponse(f"✅ Pulled {count} records from units", status_code=200)

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logging.error(f"❌ Failed in syncUnits: {str(e)}\n{tb}")
        return func.HttpResponse(f"❌ Error in syncUnits: {str(e)}\n{tb}", status_code=500)
