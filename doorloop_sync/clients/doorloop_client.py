
import requests
import logging
import time

class DoorLoopClient:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.logger = logging.getLogger(__name__)
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _make_request(self, method, url, retries=3, delay=1):
        full_url = f"{self.base_url}{url}"
        for attempt in range(1, retries + 1):
            try:
                response = requests.request(method, full_url, headers=self.headers)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                self.logger.warning(f"Attempt {attempt} failed for {method} {full_url}: {e}")
                if attempt == retries:
                    raise Exception(f"Failed after {retries} attempts for {method} {url}")
                time.sleep(delay)

    def get_all(self, endpoint, retries=3, delay=0.5):
        all_records = []
        page = 1

        while True:
            url = f"{endpoint}?page={page}"
            self.logger.info(f"📡 GET {url}")
            response = self._make_request("GET", url, retries=retries)

            if not isinstance(response, dict) or "data" not in response:
                self.logger.warning(f"⚠️ Unexpected response format on page {page}: {response}")
                break

            records = response["data"]
            if not records:
                self.logger.info(f"✅ No more records found. Stopping at page {page}.")
                break

            all_records.extend(records)
            page += 1
            time.sleep(delay)

        return all_records
