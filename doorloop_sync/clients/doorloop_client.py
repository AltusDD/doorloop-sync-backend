import requests

class DoorLoopClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        # Map logical entity names to real DoorLoop API endpoints
        self.endpoint_map = {
            "accounts": "chart-of-accounts",
            "activity_logs": "activity-logs",
            "applications": "applications",
            "communications": "communications",
            "files": "files",
            "inspections": "inspections",
            "insurance_policies": "insurance-policies",
            "lease_charges": "lease/charges",
            "lease_credits": "lease/credits",
            "lease_payments": "lease-payments",
            "leases": "leases",
            "notes": "notes",
            "owners": "owners",
            "payments": "payments",
            "portfolios": "portfolios",
            "properties": "properties",
            "recurring_charges": "recurring-charges",
            "recurring_credits": "recurring-credits",
            "reports": "reports",
            "tasks": "tasks",
            "tenants": "tenants",
            "units": "units",
            "users": "user",
            "vendors": "vendors"
        }

    def get_all(self, entity, params=None):
        if entity not in self.endpoint_map:
            raise ValueError(f"Unknown entity type: {entity}")

        endpoint = f"{self.base_url}/{self.endpoint_map[entity]}"
        params = params or {}
        results = []
        page = 1

        while True:
            paged_params = {**params, "page": page}
            response = requests.get(endpoint, headers=self.headers, params=paged_params)
            response.raise_for_status()
            data = response.json()

            if not data:
                break

            results.extend(data)
            if len(data) < 50:  # assume pagination limit
                break
            page += 1

        return results