
import requests

class DoorLoopClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

    def fetch(self, endpoint):
        url = f"{self.base_url}/v1/{endpoint}"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()
        return resp.json().get("data", [])
