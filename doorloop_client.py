import requests

class DoorLoopClient:
    def __init__(self, api_key: str, base_url: str = "https://api.doorloop.com"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

    def get(self, endpoint: str, params: dict = None):
        url = f"{self.base_url}{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
