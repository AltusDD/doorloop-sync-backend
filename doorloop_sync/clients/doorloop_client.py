import requests
import logging

class DoorLoopClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.api_key = api_key
        self.headers = {'Authorization': f'Bearer {self.api_key}'}

    def get_all(self, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 405:
                logging.warning(f"Skipping endpoint {endpoint} due to 405 Method Not Allowed")
                return None
            try:
                data = response.json()
                return data
            except Exception as e:
                logging.error(f"JSON decode failed for {url}: {e}; raw response: {response.text}")
                return None
        except Exception as e:
            logging.error(f"Request failed for {url}: {e}")
            return None