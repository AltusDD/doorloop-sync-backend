print(">>> 🔥 syncTenants __init__.py loaded")

try:
    import azure.functions as func
    print("✅ azure.functions import successful")
except Exception as e:
    print("❌ Failed to import azure.functions:", e)

try:
    import os
    import requests
    import json
    import traceback
    print("✅ All other imports successful")
except Exception as e:
    print("❌ Failed during secondary imports:", e)

def main(req: func.HttpRequest) -> func.HttpResponse:
    print(">>> Entered main() in syncTenants")
    try:
        api_key = os.getenv("DOORLOOP_API_KEY")
        if not api_key:
            raise Exception("Missing DOORLOOP_API_KEY")

        headers = {
            "Authorization": f"Bearer {api_key}"
        }
        url = f"https://api.doorloop.com/v1/tenants?page=1"
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        count = len(data.get("data", []))
        return func.HttpResponse(f"✅ Pulled {count} records from tenants", status_code=200)
    except Exception as e:
        print("❌ Exception in main():", traceback.format_exc())
        return func.HttpResponse(f"❌ ERROR: {str(e)}", status_code=500)
