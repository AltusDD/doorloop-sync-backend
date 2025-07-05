import requests
import logging
import json
from datetime import datetime, date

logger = logging.getLogger(__name__)

class SupabaseSchemaManager:
    def __init__(self, url: str, service_role_key: str):
        self.url = url.rstrip("/")
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json"
        }

    def _run_sql(self, sql: str) -> bool:
        """Internal method to execute SQL via Supabase RPC."""
        payload = {"sql": sql}
        try:
            r = requests.post(
                f"{self.url}/rest/v1/rpc/execute_sql",
                headers=self.headers,
                data=json.dumps(payload)
            )
            r.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            logger.debug(f"✅ SQL executed successfully: {sql.strip()}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ SQL failed: {sql.strip()}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            else:
                logger.error(f"Error: {e}")
            return False

    def _get_table_columns_from_db(self, table_name: str) -> set:
        """
        Fetches existing column names for a table from information_schema.
        Handles potential non-JSON responses from execute_sql RPC.
        """
        sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = '{table_name}';
        """
        payload = {"sql": sql}
        try:
            r = requests.post(
                f"{self.url}/rest/v1/rpc/execute_sql",
                headers=self.headers,
                data=json.dumps(payload)
            )
            r.raise_for_status()
            # Try to parse JSON. If it fails, it means the RPC didn't return valid JSON for SELECT.
            try:
                response_data = r.json()
                # Ensure response_data is a list of dicts, and each dict has 'column_name'
                if isinstance(response_data, list) and all(isinstance(item, dict) and 'column_name' in item for item in response_data):
                    return {col['column_name'] for col in response_data}
                else:
                    logger.warning(f"Unexpected JSON structure for columns of table '{table_name}'. Response: {r.text[:200]}...")
                    return set() # Assume no columns if structure is unexpected
            except json.JSONDecodeError:
                logger.warning(f"Could not parse JSON response for columns of table '{table_name}'. Response: {r.text[:200]}...")
                return set() # Assume no columns if response is not valid JSON
        except requests.exceptions.RequestException as e:
            logger.warning(f"Could not retrieve columns for table '{table_name}'. It might not exist or permissions are insufficient for info_schema: {e}")
            return set() # Return empty set if table doesn't exist or no access

    def infer_type(self, value):
        """Infers PostgreSQL type from a Python value."""
        if isinstance(value, str):
            # Attempt to infer date/datetime strings
            try:
                datetime.fromisoformat(value.replace('Z', '+00:00'))
                return "timestamp with time zone"
            except ValueError:
                try:
                    date.fromisoformat(value)
                    return "date"
                except ValueError:
                    return "text"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "numeric"
        elif isinstance(value, list):
            # If all items are simple strings, infer TEXT[]
            if all(isinstance(item, str) for item in value):
                return "text[]"
            # If all items are simple numbers, infer NUMERIC[]
            if all(isinstance(item, (int, float)) for item in value):
                return "numeric[]"
            return "jsonb" # Default for complex lists
        elif isinstance(value, dict):
            return "jsonb"
        else:
            return "text" # Fallback for other types

    def infer_type_from_api_props(self, field_props: dict) -> str:
        """Infers PostgreSQL type from API schema properties (from API_SCHEMAS)."""
        api_type = field_props.get("type")
        api_format = field_props.get("format")
        is_array = api_type == "array"

        if is_array:
            items_type = field_props.get("items", {}).get("type")
            if items_type == "string":
                return "text[]"
            elif items_type in ["number", "integer"]:
                return "numeric[]"
            else: # Array of objects or mixed types
                return "jsonb"
        else:
            if api_type == "string":
                if api_format in ["date", "date-time"]:
                    return "timestamp with time zone" # Or 'date' if only date part
                return "text"
            elif api_type == "integer":
                return "integer"
            elif api_type == "number":
                return "numeric"
            elif api_type == "boolean":
                return "boolean"
            elif api_type == "object":
                return "jsonb"
            else:
                return "jsonb" # Fallback for unknown types or complex objects

    def _map_api_field_to_column_name(self, api_field: str) -> str:
        """Converts camelCase API field names to snake_case column names and handles specific renames."""
        # Basic camelCase to snake_case
        column_name = ''.join(['_' + c.lower() if c.isupper() else c for c in api_field]).lstrip('_')

        # Specific renames based on DoorLoop API and PostgreSQL keywords/conventions
        if column_name == "class": return "class_name"
        if column_name == "from": return "from_participant"
        if column_name == "to": return "to_participants"
        if column_name == "cc": return "cc_participants"
        if column_name == "bcc": return "bcc_participants"
        if column_name == "announcement": return "announcement_id"
        if column_name == "lease_payment": return "lease_payment_id"
        if column_name == "property": return "property_id"
        if column_name == "unit": return "unit_id"
        if column_name == "lease": return "lease_id"
        if column_name == "vendor": return "vendor_id"
        if column_name == "account": return "account_id"
        if column_name == "received_from_tenant": return "received_from_tenant_id"
        if column_name == "deposit_to_account": return "deposit_to_account_id"
        if column_name == "total_balance_due": return "total_balance_due" # Corrected from totalBalancedue
        if column_name == "total_recurring_charges": return "total_recurring_charges" # Corrected from TotalRecurringCharges
        if column_name == "evicton_pending": return "eviction_pending" # Corrected typo
        if column_name == "proof_of_insurance_required": return "proof_of_insurance_required"
        if column_name == "rollover_to_at_will": return "rollover_to_at_will"
        if column_name == "pay_from_account": return "pay_from_account_id"
        if column_name == "payment_method": return "payment_method"
        if column_name == "pay_to_resource_type": return "pay_to_resource_type"
        if column_name == "pay_to_resource_id": return "pay_to_resource_id"
        if column_name == "date": return "date_field" # Avoid reserved keyword
        if column_name == "due_date": return "due_date"
        if column_name == "total_amount": return "total_amount"
        if column_name == "total_balance": return "total_balance"
        if column_name == "sent_at": return "sent_at"
        if column_name == "opened_at": return "opened_at"
        if column_name == "clicked_at": return "clicked_at"
        if column_name == "bounced_at": return "bounced_at"
        if column_name == "linked_resource": return "linked_resource"
        if column_name == "created_by": return "created_by"
        if column_name == "created_at": return "created_at_ts" # Renamed to avoid conflict with default Supabase column
        if column_name == "updated_at": return "updated_at_ts" # Renamed
        if column_name == "download_url": return "download_url"
        if column_name == "num_active_units": return "num_active_units"
        if column_name == "pets_policy": return "pets_policy"
        if column_name == "board_members": return "board_members"
        if column_name == "settings": return "settings_json" # Renamed to avoid potential conflict

        # Specific fields from your properties sample data that might not be in generic API schema
        if api_field == "isValidAddress": return "is_valid_address"
        if api_field == "externalId": return "external_id"
        if api_field == "managerId": return "manager_id"
        if api_field == "insuranceJson": return "insurance_json"
        if api_field == "taxInfoJson": return "tax_info_json"
        if api_field == "financialsJson": return "financials_json"
        if api_field == "complianceJson": return "compliance_json"
        if api_field == "purchaseDate": return "purchase_date"
        if api_field == "purchasePrice": return "purchase_price"
        if api_field == "currentValue": return "current_value"
        if api_field == "bedroomCount": return "bedroom_count"
        if api_field == "systemAccount": return "system_account"
        if api_field == "fullyQualifiedName": return "fully_qualified_name"
        if api_field == "cashFlowActivity": return "cash_flow_activity"
        if api_field == "defaultAccountFor": return "default_account_for"


        return column_name


    def ensure_schema(self, table_name: str, api_schema: dict) -> bool:
        """
        Ensures the Supabase table exists and its columns match the API schema.
        This method is idempotent and handles adding missing columns.
        """
        logger.info(f"Ensuring schema for table: public.{table_name}")

        # 1. Create table if it doesn't exist
        # Primary key is always 'id' and TEXT for DoorLoop's mongoId
        create_table_sql = f'CREATE TABLE IF NOT EXISTS public."{table_name}" (id TEXT PRIMARY KEY);'
        if not self._run_sql(create_table_sql):
            logger.error(f"Failed to create or ensure table '{table_name}'. Aborting schema sync for this table.")
            return False

        # Set ownership to service_role to allow future alterations
        # This is a critical step for permissions.
        alter_owner_sql = f'ALTER TABLE public."{table_name}" OWNER TO service_role;'
        if not self._run_sql(alter_owner_sql):
            logger.warning(f"Failed to set owner of table '{table_name}' to 'service_role'. Permissions for future ALTERs might be an issue.")
            # Do not abort, as table might still be usable for data ingestion

        # 2. Add missing columns based on API schema
        existing_cols = self._get_table_columns_from_db(table_name)

        for api_field, field_props in api_schema.items():
            # Skip primary key as it's already handled
            if api_field == "id":
                continue

            # Convert API field name to snake_case column name using the consistent mapping
            column_name = self._map_api_field_to_column_name(api_field)

            if column_name not in existing_cols:
                pg_type = self.infer_type_from_api_props(field_props)
                is_nullable = field_props.get("nullable", True) # Default to nullable
                nullable_constraint = "" if is_nullable else " NOT NULL"

                # CRITICAL FIX: ADD "IF NOT EXISTS" HERE
                add_column_sql = f'ALTER TABLE public."{table_name}" ADD COLUMN IF NOT EXISTS "{column_name}" {pg_type}{nullable_constraint};'
                if not self._run_sql(add_column_sql):
                    logger.error(f"Failed to add column '{column_name}' to '{table_name}'.")
                    # Continue to try other columns, but log the failure
            else:
                logger.debug(f"Column '{column_name}' already exists in '{table_name}'. Skipping ADD COLUMN.")


        return True

