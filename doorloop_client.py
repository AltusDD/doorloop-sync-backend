import os
import requests

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")

if not DOORLOOP_API_KEY or not DOORLOOP_API_BASE_URL:
    raise EnvironmentError("Missing DoorLoop API configuration in environment variables.")

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Accept": "application/json"
}

def fetch_all_records(endpoint):
    all_data = []
    page = 1
    page_size = 100
    has_more = True

    while has_more:
        url = f"{DOORLOOP_API_BASE_URL}{endpoint}?page_number={page}&page_size={page_size}&sort_by=createdAt&descending=true"
        print(f"➡️ Requesting: {url}")
        try:
            response = requests.get(url, headers=HEADERS)

            print(f"↩️ Status Code: {response.status_code}")
            print(f"📝 Raw Response: {response.text}")

            response.raise_for_status()
            json_data = response.json()

            data = json_data.get("data", [])
            all_data.extend(data)

            pagination = json_data.get("pagination", {})
            total_pages = pagination.get("totalPages", 1)
            has_more = page < total_pages
            page += 1
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            print(f"Response content: {response.text}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise

    print(f"📦 Total records fetched from {endpoint}: {len(all_data)}")
    return all_data
