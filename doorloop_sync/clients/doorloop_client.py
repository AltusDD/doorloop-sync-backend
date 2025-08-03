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
            "accounts": "accounts",
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
            "users": "users",
            "vendors": "vendors"
        }

    def get_all(self, entity, params=None):
        entity = entity.lstrip("/")
        # FIX: Standardize entity name by replacing hyphens with underscores to match map keys.
        entity = entity.replace('-', '_')

        if entity not in self.endpoint_map:
            raise ValueError(f"Unknown entity type: {entity}")

        # FIX: Add the required '/api/v1/' prefix to the request URL to resolve 404 errors.
        endpoint = f"{self.base_url}/api/v1/{self.endpoint_map[entity]}"
        
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
            if len(data) < 50:
                break
            page += 1

        return results