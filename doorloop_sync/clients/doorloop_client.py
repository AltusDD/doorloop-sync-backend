import requests
import os
import logging

class DoorLoopClient:
    def __init__(self):
        """
        Initializes the DoorLoopClient with the API key and the correct base URL
        from environment variables.
        """
        self.api_key = os.getenv("DOORLOOP_API_KEY")
        # FIX: Use the correct base URL and strip any trailing slashes.
        self.base_url = os.getenv("DOORLOOP_API_BASE_URL", "https://app.doorloop.com/api").rstrip("/")

        if not self.api_key:
            raise ValueError("DOORLOOP_API_KEY environment variable not set.")

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        # This map translates a simplified entity name to the required API endpoint path.
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
        """
        Fetches all records for a given entity, handling pagination automatically.
        """
        # Standardize entity name to match keys in endpoint_map
        entity_key = entity.replace('-', '_')

        if entity_key not in self.endpoint_map:
            raise ValueError(f"Unknown entity type: {entity}")

        # FIX: Construct the endpoint URL correctly without hardcoding /v1 or /api.
        endpoint = f"{self.base_url}/{self.endpoint_map[entity_key]}"
        
        params = params or {}
        results = []
        page = 1

        while True:
            paged_params = {**params, "page": page}
            try:
                response = requests.get(endpoint, headers=self.headers, params=paged_params)
                response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
                data = response.json()

                if not data:
                    break

                results.extend(data)
                if len(data) < 50:  # Assumes a page size of 50
                    break
                page += 1

            except requests.exceptions.HTTPError as e:
                logging.error(f"HTTP Error fetching {entity} from {endpoint}: {e}")
                # Depending on desired behavior, you might want to stop or just return what you have
                break 
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed for {entity}: {e}")
                break

        return results
