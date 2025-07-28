import logging
import requests

class DoorLoopClient:
    def __init__(self, api_key: str, base_url: str = "https://api.doorloop.com/v1"):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def fetch_all(self, endpoint: str, page_size: int = 100):
        """Fetch all paginated results from a given DoorLoop endpoint."""
        logging.info(f"📡 Fetching from {endpoint}...")
        results = []
        page = 1

        while True:
            url = f"{self.base_url}/{endpoint}?page={page}&limit={page_size}"
            logging.debug(f"➡️ GET {url}")
            response = requests.get(url, headers=self.headers)

            if response.status_code != 200:
                raise Exception(f"❌ API Error {response.status_code}: {response.text}")

            data = response.json()
            if not data or "data" not in data:
                break

            page_data = data["data"]
            if not page_data:
                break

            results.extend(page_data)
            page += 1

        logging.info(f"✅ Fetched {len(results)} records from {endpoint}")
        return results
