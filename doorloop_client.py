import os
import httpx

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://api.doorloop.com/v1")

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

def fetch_all_entities(endpoint: str, page_size=100):
    records = []
    page = 1

    while True:
        url = f"{DOORLOOP_BASE_URL}/{endpoint}?page={page}&limit={page_size}"
        print(f"🌐 Fetching: {url}")
        response = httpx.get(url, headers=HEADERS, follow_redirects=True)

        if response.status_code != 200:
            raise httpx.HTTPStatusError(f"Failed to fetch {endpoint}: {response.status_code}",
                                        request=response.request,
                                        response=response)

        try:
            data = response.json()
        except Exception as e:
            raise ValueError(f"JSON decode error for {endpoint}: {e}")

        if not data:
            break

        records.extend(data)
        if len(data) < page_size:
            break
        page += 1

    return records
