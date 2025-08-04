import os
import time
import requests
from doorloop_sync.utils.writeback_guard import is_write_allowed
from doorloop_sync.utils.writeback_logger import log_write_attempt

# 🛠️ Empire Patch: Silent Tag Added

class DoorLoopClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

    def _make_request(self, method, endpoint, retries=3, **kwargs):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        for attempt in range(retries):
            response = requests.request(method, url, headers=self.headers, **kwargs)
            if response.status_code == 429:
                wait_time = min(60, (2 ** attempt))  # exponential backoff with max cap
                time.sleep(wait_time)
                continue
            response.raise_for_status()
            return response
        raise Exception(f"Failed after {retries} attempts for {method} {url}")

    def get_all(self, endpoint):
        results = []
        page = 1
        while True:
            response = self._make_request("GET", f"{endpoint}?page={page}")
            data = response.json()
            if not data:
                break
            results.extend(data)
            page += 1
        return results

    def write(self, method, endpoint, payload, user_role, entity_type):
        if not is_write_allowed(user_role, method, entity_type):
            raise PermissionError(f"User role {user_role} not authorized to perform {method} on {entity_type}")
        log_write_attempt(user_role, method, endpoint, entity_type)
        response = self._make_request(method, endpoint, json=payload)
        return response.json()