import requests

class DoorLoopClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def get(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_all(self, endpoint, params=None):
        results = []
        page = 1
        while True:
            paged_params = params.copy() if params else {}
            paged_params.update({"page": page})
            response = self.get(endpoint, params=paged_params)
            if not response or not response.get("data"):
                break
            results.extend(response["data"])
            if not response.get("hasMore", False):
                break
            page += 1
        return results
