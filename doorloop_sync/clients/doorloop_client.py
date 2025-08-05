import time
import requests
from typing import List, Dict, Any

class DoorLoopClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        full_url = self.base_url.rstrip("/") + "/" + url.lstrip("/")
        for attempt in range(3):
            try:
                response = requests.request(method, full_url, headers=headers, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException:
                time.sleep(1)
        raise Exception(f"Failed after 3 attempts for {method} {url}")

    def get_all(self, endpoint: str, max_pages: int = 1000, delay_between_pages: float = 0.2) -> List[Dict[str, Any]]:
        all_data = []
        page = 1
        consecutive_empty_pages = 0
        while page <= max_pages:
            response = self._make_request("GET", f"{endpoint}?page={page}")
            data = response.json()
            if not isinstance(data, list):
                raise Exception(f"Expected list from API, got {type(data)} with content: {data}")

            if len(data) == 0:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 3:
                    break
            else:
                consecutive_empty_pages = 0
                all_data.extend(data)

            page += 1
            time.sleep(delay_between_pages)
        return all_data