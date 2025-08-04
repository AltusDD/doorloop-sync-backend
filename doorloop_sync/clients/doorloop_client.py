import requests
import os
import logging

logger = logging.getLogger(__name__)

class DoorLoopClient:
    def __init__(self):
        self.api_key = os.getenv("DOORLOOP_API_KEY")
        self.base_url = os.getenv("DOORLOOP_API_BASE_URL", "https://app.doorloop.com").rstrip("/")

        print(f"📡 Using API base: {self.base_url}")
print(f"🔑 Using API key: {self.api_key}")


        if not self.api_key:
            raise ValueError("DOORLOOP_API_KEY environment variable not set.")

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        self.endpoint_map = {
            "accounts": "accounts", "activity_logs": "activity-logs", "applications": "applications",
            "communications": "communications", "files": "files", "inspections": "inspections",
            "insurance_policies": "insurance-policies", "lease_charges": "lease/charges",
            "lease_credits": "lease/credits", "lease_payments": "lease-payments", "leases": "leases",
            "notes": "notes", "owners": "owners", "payments": "payments", "portfolios": "portfolios",
            "properties": "properties", "recurring_charges": "recurring-charges",
            "recurring_credits": "recurring-credits", "reports": "reports", "tasks": "tasks",
            "tenants": "tenants", "units": "units", "users": "users", "vendors": "vendors"
        }

    def get_all(self, entity, params=None):
        entity_key = entity.replace('-', '_')
        if entity_key not in self.endpoint_map:
            raise ValueError(f"Unknown entity type: {entity}")

        endpoint = f"{self.base_url}/api/{self.endpoint_map[entity_key]}"
        params = params or {}
        results = []
        page = 1

        while True:
            paged_params = {**params, "page": page}
            try:
                response = requests.get(endpoint, headers=self.headers, params=paged_params)
                response.raise_for_status()

                # FIX: Add a check for a valid JSON response before decoding.
                if "application/json" in response.headers.get("Content-Type", ""):
                    data = response.json()
                else:
                    logger.error(f"Non-JSON response received from {endpoint}. Status: {response.status_code}. Body: {response.text[:200]}")
                    break # Stop trying to process a bad response

                if not data:
                    break

                results.extend(data)
                if len(data) < 50:
                    break
                page += 1
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP Error fetching {entity} from {endpoint}: {e}")
                break
            except requests.exceptions.JSONDecodeError as e:
                logger.error(f"JSONDecodeError for {entity} from {endpoint}. Response text: {response.text[:200]}")
                break
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {entity}: {e}")
                break
        return results
