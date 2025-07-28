import os
import requests
import logging
import time
from typing import List, Dict, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DoorLoopClient:
    def __init__(self, api_key: str, base_url: str = "https://api.doorloop.com/v1"):
        if not api_key:
            raise ValueError("DOORLOOP_API_KEY must be provided to DoorLoopClient.")
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

    def _call(self, method: str, endpoint: str, params: Dict[str, Any] = None, json_data: Dict[str, Any] = None) -> requests.Response:
        url = f"{self.base_url}/{endpoint}"
        for attempt in range(3):
            try:
                response = self.session.request(method, url, params=params, json=json_data, timeout=60)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                if response.status_code in [429, 500, 502, 503, 504] and attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                raise
            except requests.exceptions.RequestException as e:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                raise

    def fetch_all(self, endpoint: str, page_size: int = 100) -> List[Dict[str, Any]]:
        all_data = []
        page = 1
        while True:
            params = {"page": page, "pageSize": page_size}
            response = self._call("GET", endpoint, params=params)
            data = response.json()
            if not data:
                break
            all_data.extend(data)
            page += 1
        return all_data

    def fetch_properties(self):
        return self.fetch_all("properties")

    def fetch_units(self):
        return self.fetch_all("units")

    def fetch_leases(self):
        return self.fetch_all("leases")

    def fetch_tenants(self):
        return self.fetch_all("tenants")

    def fetch_owners(self):
        return self.fetch_all("owners")

    def fetch_lease_payments(self):
        return self.fetch_all("lease-payments")
