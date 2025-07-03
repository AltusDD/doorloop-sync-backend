import os
import requests
import time
import logging

class DoorLoopClient:
    def __init__(self, api_key: str, base_url: str):
        if not api_key:
            raise ValueError("DOORLOOP_API_KEY must be provided to DoorLoopClient.")
        if not base_url:
            raise ValueError("DOORLOOP_API_BASE_URL must be provided to DoorLoopClient.")

        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }
        self.last_api_call_time = 0

    def fetch_all(self, endpoint: str, params=None):
        url = f"{self.base_url}/{endpoint}"
        results = []
        page = 1
        RATE_LIMIT_DELAY = 0.1

        while True:
            current_time = time.time()
            elapsed = current_time - self.last_api_call_time
            if elapsed < RATE_LIMIT_DELAY:
                time.sleep(RATE_LIMIT_DELAY - elapsed)
            self.last_api_call_time = time.time()

            full_params = {"page_number": page, "page_size": 100}
            if params:
                full_params.update(params)

            response = requests.get(url, headers=self.headers, params=full_params)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                logging.warning(f"Rate limit hit. Retrying in {retry_after} seconds.")
                time.sleep(retry_after)
                continue

            if not response.ok:
                logging.error(f"❌ Failed to fetch {endpoint}: {response.status_code} - {response.text}")
                raise Exception(f"❌ Failed to fetch {endpoint}: {response.status_code} - {response.text}")

            page_data = response.json()
            data_list = page_data.get("data") if isinstance(page_data, dict) and "data" in page_data else page_data
            if not data_list or not isinstance(data_list, list):
                break
            results.extend(data_list)
            if len(data_list) < 100:
                break
            page += 1

        return results