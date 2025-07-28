
import requests
import os

class DoorLoopClient:
    def __init__(self, api_key=None, base_url=None):
        self.api_key = api_key or os.getenv("DOORLOOP_API_KEY")
        self.base_url = base_url or os.getenv("DOORLOOP_API_BASE_URL")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def get_all(self, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        results = []
        page = 1
        while True:
            query = params or {}
            query["page"] = page
            response = requests.get(url, headers=self.headers, params=query)
            if response.status_code != 200:
                raise Exception(f"API error: {response.status_code} - {response.text}")
            data = response.json()
            items = data.get("data", [])
            if not items:
                break
            results.extend(items)
            if len(items) < 100:
                break
            page += 1
        return results

    def get_properties(self):
        return self.get_all("/properties")

    def get_units(self):
        return self.get_all("/units")

    def get_leases(self):
        return self.get_all("/leases")

    def get_tenants(self):
        return self.get_all("/tenants")

    def get_owners(self):
        return self.get_all("/owners")

    def get_payments(self):
        return self.get_all("/lease-payments")

    def get_work_orders(self):
        return self.get_all("/work-orders")

    def get_vendors(self):
        return self.get_all("/vendors")
