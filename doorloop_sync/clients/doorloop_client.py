import requests

class DoorLoopClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def get(self, endpoint: str, params=None):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.get(url, headers=self.headers, params=params or {})
        response.raise_for_status()
        return response.json()

    def post(self, endpoint: str, payload: dict):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()
