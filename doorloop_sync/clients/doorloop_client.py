
# doorloop_client.py (patched version)
from doorloop_sync.security.writeback_guard import check_permission
from doorloop_sync.security.writeback_logger import log_write_attempt
import requests

class DoorLoopClient:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def _make_request(self, method, endpoint, **kwargs):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        for attempt in range(3):
            response = requests.request(method, url, headers=self.headers, **kwargs)
            if response.status_code == 429:
                import time
                time.sleep(2 ** attempt)
                continue
            break
        response.raise_for_status()
        return response.json()

    def get_all(self, endpoint):
        results = []
        page = 1
        while True:
            response = self._make_request("GET", endpoint, params={"page": page})
            if not response or len(response) == 0:
                break
            results.extend(response)
            if len(response) < 50:
                break
            page += 1
        return results

    def write(self, method, endpoint, data, user):
        if method.upper() in ["POST", "PATCH", "DELETE"]:
            if not check_permission(user, method, endpoint):
                raise PermissionError(f"User {user} not authorized to perform {method} on {endpoint}")
            log_write_attempt(user, method, endpoint, data)
        return self._make_request(method, endpoint, json=data)
