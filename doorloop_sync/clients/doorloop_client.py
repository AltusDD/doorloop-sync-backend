
import requests
import time
import logging

logger = logging.getLogger(__name__)

class DoorLoopClient:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _make_request(self, method, endpoint, retries=10, **kwargs):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        for attempt in range(retries):
            response = requests.request(method, url, headers=self.headers, **kwargs)
            if response.status_code == 429:
                wait_time = min(300, (2 ** attempt) + 5)
                print(f"⚠️ Rate limit hit. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            if response.status_code >= 400:
                print(f"HTTP Error for {url}: {response.status_code} {response.reason}")
                print(f"Response Body: {response.text}")
                time.sleep(3)
                continue
            return response
        raise Exception(f"Failed after {retries} attempts for {method} {url}")

    def get_all(self, endpoint, delay_between_pages=0):
        results = []
        page = 1
        while True:
            print(f"📡 GET {self.base_url}/{endpoint}?page={page}")
            response = self._make_request("GET", f"{endpoint}?page={page}")
            try:
                data = response.json()
            except Exception:
                print(f"❌ Failed to decode JSON from {endpoint}")
                break
            if not data:
                break
            results.extend(data)
            page += 1
            if delay_between_pages:
                time.sleep(delay_between_pages)
        return results

# silent_tag: empire_sync_patch_v3_retry_delay
