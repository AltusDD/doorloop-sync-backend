import azure.functions as func
import os
import requests

def main(req: func.HttpRequest) -> func.HttpResponse:
    test_mode = req.params.get('test') == 'true'
    api_key = os.environ.get("DOORLOOP_API_KEY")
    endpoint = "https://api.doorloop.com/v1/units"

    headers = {{
        "Authorization": f"Bearer {{api_key}}"
    }}

    try:
        r = requests.get(endpoint, headers=headers)
        r.raise_for_status()
        data = r.json()
        count = len(data if isinstance(data, list) else data.get("data", []))
        if test_mode:
            return func.HttpResponse(f"✅ [TEST MODE] Synced {count} records from units", status_code=200)
        return func.HttpResponse(f"✅ Synced {count} records from units", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"❌ Sync failed: {{str(e)}}", status_code=500)
