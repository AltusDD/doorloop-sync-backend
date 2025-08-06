import os
import time
import requests
from typing import List, Dict, Any

class DoorLoopClient:
    def __init__(self):
        """
        Initializes the client by automatically loading credentials from
        environment variables.
        """
        self.api_key = os.getenv("DOORLOOP_API_KEY")
        self.base_url = os.getenv("DOORLOOP_API_BASE_URL")

        if not self.api_key or not self.base_url:
            raise ValueError("DOORLOOP_API_KEY and DOORLOOP_API_BASE_URL must be set in the environment.")

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Internal helper to make requests to the DoorLoop API with authentication.
        """
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
            except requests.RequestException as e:
                print(f"[DoorLoopClient] Request failed on attempt {attempt + 1}: {e}")
                time.sleep(1 * (attempt + 1))
        
        raise Exception(f"Failed after 3 attempts for {method} {url}")

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

            # --- THIS IS THE FIX ---
            # Handle enveloped responses by extracting the list from the 'data' key.
            # If it's not a dictionary (i.e., it's already a list), use it directly.
            records = data.get('data', data) if isinstance(data, dict) else data

            if not isinstance(records, list):
                print(f"[DoorLoopClient] ⚠️ Expected a list of records but got {type(records)}. Stopping.")
                break

            if not records:
                print(f"[DoorLoopClient] 🚪 Ending pagination for {endpoint} on page {page} (no new records).")
                break

            unique_records = [item for item in records if item.get("id") not in seen_ids]
            for item in unique_records:
                if item_id := item.get("id"):
                    seen_ids.add(item_id)
            
            all_data.extend(unique_records)

            page += 1
            if delay_between_pages > 0:
                time.sleep(delay_between_pages)

        return all_data
