import os
import requests
import time
from typing import List, Dict, Any


class DoorLoopClient:
    def __init__(self):
        self.base_url = os.getenv("DOORLOOP_API_BASE_URL")
        self.api_key = os.getenv("DOORLOOP_API_KEY")

        if not self.base_url or not self.api_key:
            raise EnvironmentError("DOORLOOP_API_BASE_URL and DOORLOOP_API_KEY must be set as environment variables.")

        print(f"[DoorLoopClient] ✅ Initialized. Using validated BASE URL: {self.base_url}")

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Internal helper to make authorized requests to the DoorLoop API.
        """
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        print(f"[DoorLoopClient] 🌐 Requesting: {url}")
        response = requests.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        return response

    def get_all(self, endpoint: str, max_pages: int = 1000, delay_between_pages: float = 0.2) -> List[Dict[str, Any]]:
        """
        Fetches all pages of data from a given DoorLoop endpoint,
        handling both simple list and enveloped dictionary responses.
        """
        all_data = []
        page = 1
        seen_ids = set()

        while page <= max_pages:
            print(f"[DoorLoopClient] 🔄 Fetching page {page} of {endpoint}")
            try:
                response = self._make_request("GET", f"{endpoint}?page={page}")
                data = response.json()
            except Exception as e:
                print(f"[DoorLoopClient] ❌ Failed to fetch page {page} for {endpoint}: {e}")
                break

            records = data.get('data', data) if isinstance(data, dict) else data

            if not isinstance(records, list):
                print(f"[DoorLoopClient] ⚠️ Expected a list of records but got {type(records)}. Stopping.")
                break

            if not records:
                print(f"[DoorLoopClient] 🚪 Ending pagination for {endpoint} on page {page} (no new records).")
                break

            unique_records = [item for item in records if item.get("id") not in seen_ids]
            if not unique_records:
                print(f"[DoorLoopClient] 🛑 No new unique records found on page {page}. Ending pagination.")
                break

            for item in unique_records:
                if item_id := item.get("id"):
                    seen_ids.add(item_id)

            all_data.extend(unique_records)

            page += 1
            if delay_between_pages > 0:
                time.sleep(delay_between_pages)

        return all_data
