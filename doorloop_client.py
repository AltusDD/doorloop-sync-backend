import os
import time
import logging
import requests
from urllib.parse import urljoin

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DoorLoopClient:
    def __init__(self):
        self.api_key = os.environ["DOORLOOP_API_KEY"]
        self.base_url = os.environ["DOORLOOP_API_BASE_URL"]
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def _call(self, method, endpoint, params=None, data=None, retries=3):
        url = urljoin(self.base_url, endpoint)
        for attempt in range(1, retries + 1):
            try:
                response = requests.request(
                    method, url, headers=self.headers, params=params, json=data
                )
                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    logger.warning(f"429 Too Many Requests. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt == retries:
                    raise
                logger.warning(f"Request error (attempt {attempt}): {e}")
                time.sleep(2 ** attempt)

    def fetch_all(self, endpoint):
        page = 1
        page_size = 100
        all_records = []

        while True:
            params = {"page": page, "pageSize": page_size}
            response = self._call("GET", endpoint, params=params)
            data = response.json()
            items = data.get("data", data)

            if not items:
                break

            all_records.extend(items)
            if len(items) < page_size:
                break

            page += 1

        return all_records
