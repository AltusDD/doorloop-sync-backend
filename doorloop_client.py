import os
import requests

def fetch_all_doorloop_data(endpoint):
    headers = {
        "Authorization": f"Bearer {os.getenv('DOORLOOP_API_KEY')}",
        "Content-Type": "application/json"
    }

    url = f"{os.getenv('DOORLOOP_API_BASE_URL')}/{endpoint}"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"[ERROR] API call to {url} failed with {response.status_code}")
        print(f"[ERROR] Body: {response.text}")
        response.raise_for_status()

    try:
        return response.json()
    except Exception as e:
        print(f"[ERROR] Could not parse JSON from: {url}")
        print(f"[ERROR] Raw text: {response.text}")
        raise e

