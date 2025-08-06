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
        # Ensure the URL is constructed correctly
        full_url = self.base_url.rstrip("/") + "/" + url.lstrip("/")
        
        # Simple retry logic
        for attempt in range(3):
            try:
                response = requests.request(method, full_url, headers=headers, **kwargs)
                response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
                return response
            except requests.RequestException as e:
                print(f"[DoorLoopClient] Request failed on attempt {attempt + 1}: {e}")
                time.sleep(1 * (attempt + 1)) # Exponential backoff
        
        raise Exception(f"Failed after 3 attempts for {method} {url}")

    def get_all(self, endpoint: str, max_pages: int = 1000, delay_between_pages: float = 0.2) -> List[Dict[str, Any]]:
        """
        Fetches all pages of data from a given DoorLoop endpoint.
        """
        all_data = []
        page = 1
        seen_ids = set()
        consecutive_empty_pages = 0

        while page <= max_pages:
            print(f"[DoorLoopClient] 🔄 Fetching page {page} of {endpoint}")
            try:
                response = self._make_request("GET", f"{endpoint}?page={page}")
                data = response.json()
            except Exception as e:
                print(f"[DoorLoopClient] ❌ Failed to fetch page {page} for {endpoint}: {e}")
                break # Stop if a page fails

            if not isinstance(data, list):
                print(f"[DoorLoopClient] ⚠️ Expected list from API, got {type(data)}. Stopping.")
                break

            # Filter out duplicate items seen on previous pages
            unique_data = [item for item in data if item.get("id") not in seen_ids]
            for item in unique_data:
                if item_id := item.get("id"):
                    seen_ids.add(item_id)

            if not unique_data:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 3:
                    print(f"[DoorLoopClient] 🚪 Ending pagination for {endpoint} after {consecutive_empty_pages} empty/redundant pages.")
                    break
            else:
                consecutive_empty_pages = 0
                all_data.extend(unique_data)

            page += 1
            if delay_between_pages > 0:
                time.sleep(delay_between_pages)

        return all_data
