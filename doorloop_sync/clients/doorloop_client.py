import os
import requests
import time

class DoorLoopClient:
    def __init__(self, api_key=None, base_url=None):
        self.api_key = api_key or os.getenv("DOORLOOP_API_KEY")
        self.base_url = base_url or os.getenv("DOORLOOP_API_BASE_URL")
        if not self.api_key or not self.base_url:
            raise ValueError("Missing DoorLoop API credentials.")

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def get_all(self, endpoint, delay_between_pages=0.3, max_pages=None):
        all_results = []
        page = 1
        while True:
            url = f"{self.base_url}{endpoint}?page={page}"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 1))
                time.sleep(retry_after)
                continue
            elif not response.ok:
                raise Exception(f"DoorLoop API error {response.status_code}: {response.text}")

            data = response.json()
            results = data.get("data", [])
            if not results:
                break

            all_results.extend(results)
            if max_pages and page >= max_pages:
                break
            page += 1
            time.sleep(delay_between_pages)
        return all_results
