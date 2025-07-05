import requests
import os
import logging
import json
from datetime import datetime, date # Added date for infer_type consistency

# Import the schema manager class
from supabase_schema_manager import SupabaseSchemaManager

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, url: str, service_role_key: str):
        """
        Initializes the Supabase client.
        Accepts Supabase URL and service role key as arguments.
        """
        if not url:
            raise ValueError("SUPABASE_URL must be provided to SupabaseClient.")
        if not service_role_key:
            raise ValueError("SUPABASE_SERVICE_ROLE_KEY must be provided to SupabaseClient.")

        self.url = url.rstrip("/") # Ensure no trailing slash for consistent URL building
        self.service_role_key = service_role_key
        self.headers = {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates" # For UPSERT behavior
        }
        # Instantiate the schema manager here, passing its required arguments
        self.schema_manager = SupabaseSchemaManager(self.url, self.service_role_key)


    def _map_api_field_to_column_name(self, api_field: str) -> str:
        """
        Converts camelCase API field names to snake_case column names and handles specific renames.
        This mapping must be consistent with the logic in supabase_schema_manager.py.
        """
        # Basic camelCase to snake_case
        column_name = ''.join(['_' + c.lower() if c.isupper() else c for c in api_field]).lstrip('_')

        # Specific renames (keep consistent with supabase_schema_manager.py)
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
        if column_name == "total_balance_due": return "total_balance_due"
        if column_name == "total_recurring_charges": return "total_recurring_charges"
        if column_name == "evicton_pending": return "eviction_pending"
        if column_name == "proof_of_insurance_required": return "proof_of_insurance_required"
        if column_name == "rollover_to_at_will": return "rollover_to_at_will"
        if column_name == "pay_from_account": return "pay_from_account_id"
        if column_name == "payment_method": return "payment_method"
        if column_name == "pay_to_resource_type": return "pay_to_resource_type"
        if column_name == "pay_to_resource_id": return "pay_to_resource_id"
        if column_name == "date": return "date_field"
        if column_name == "due_date": return "due_date"
        if column_name == "total_amount": return "total_amount"
        if column_name == "total_balance": return "total_balance"
        if column_name == "sent_at": return "sent_at"
        if column_name == "opened_at": return "opened_at"
        if column_name == "clicked_at": return "clicked_at"
        if column_name == "bounced_at": return "bounced_at"
        if column_name == "linked_resource": return "linked_resource"
        if column_name == "created_by": return "created_by"
        if column_name == "created_at": return "created_at_ts"
        if column_name == "updated_at": return "updated_at_ts"
        if column_name == "download_url": return "download_url"
        if column_name == "num_active_units": return "num_active_units"
        if column_name == "pets_policy": return "pets_policy"
        if column_name == "board_members": return "board_members"
        if column_name == "settings": return "settings_json"
        if column_name == "is_valid_address": return "is_valid_address"
        if api_field == "externalId": return "external_id" # Corrected from column_name == "external_id"
        if api_field == "managerId": return "manager_id" # Corrected from column_name == "manager_id"
        if api_field == "insuranceJson": return "insurance_json" # Corrected
        if api_field == "taxInfoJson": return "tax_info_json" # Corrected
        if api_field == "financialsJson": return "financials_json" # Corrected
        if api_field == "complianceJson": return "compliance_json" # Corrected
        if api_field == "purchaseDate": return "purchase_date" # Corrected
        if api_field == "purchasePrice": return "purchase_price" # Corrected
        if api_field == "currentValue": return "current_value" # Corrected
        if api_field == "bedroomCount": return "bedroom_count" # Corrected
        if api_field == "systemAccount": return "system_account" # Corrected
        if api_field == "fullyQualifiedName": return "fully_qualified_name" # Corrected
        if api_field == "cashFlowActivity": return "cash_flow_activity" # Corrected
        if api_field == "defaultAccountFor": return "default_account_for" # Corrected
        if api_field == "bankAccounts": return "bank_accounts" # Corrected
        if api_field == "acceptedOnTOS": return "accepted_on_t_o_s" # Corrected
        if api_field == "amountAppliedToCharges": return "amount_applied_to_charges" # Corrected
        if api_field == "completedAt": return "completed_at" # Corrected
        if api_field == "alternateAddress": return "alternate_address" # Corrected
        if api_field == "checkInfo": return "check_info" # Corrected
        if api_field == "conversation": return "conversation" # Corrected
        if api_field == "createdByName": return "created_by_name" # Corrected


        return column_name


    def upsert_data(self, table_name: str, records: list, primary_key_field: str = "id"):
        """
        Upserts a list of records into the specified Supabase table.
        Applies snake_case transformation to keys and ensures all records in a batch
        have a consistent set of keys (columns) to satisfy PostgREST's "All object keys must match" constraint.
        """
        if not records:
            logger.info(f"No records for {table_name}")
            return

        # Step 1: Collect all unique keys (column names after transformation) across all records in the batch
        all_unique_transformed_keys = set()
        for record in records:
            for k in record.keys():
                all_unique_transformed_keys.add(self._map_api_field_to_column_name(k))

        # Step 2: Transform and standardize each record
        transformed_and_standardized_records = []
        for record in records:
            item = {}
            for original_key, original_value in record.items():
                transformed_key = self._map_api_field_to_column_name(original_key)
                
                # Handle specific type conversions for Supabase PostgREST
                if isinstance(original_value, datetime):
                    item[transformed_key] = original_value.isoformat() # Convert datetime objects to ISO 8601 string
                elif isinstance(original_value, date):
                    item[transformed_key] = original_value.isoformat() # Convert date objects to ISO 8601 string
                elif isinstance(original_value, (list, dict)):
                    # For lists/dicts, send as is; Supabase JSONB will handle it
                    item[transformed_key] = original_value
                else:
                    item[transformed_key] = original_value
            
            # Ensure all records have all unique keys, filling with None if missing
            for key in all_unique_transformed_keys:
                if key not in item:
                    item[key] = None # Fill missing keys with None
            
            transformed_and_standardized_records.append(item)

        url = f"{self.url}/rest/v1/{table_name}?on_conflict={primary_key_field}"
        try:
            # Send the standardized list of records
            r = requests.post(url, headers=self.headers, json=transformed_and_standardized_records)
            r.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            logger.info(f"Successfully upserted {len(transformed_and_standardized_records)} records into {table_name}.")
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Supabase insert failed for {table_name}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise # Re-raise to propagate the error

    def ensure_table_and_columns(self, table_name: str, api_schema: dict) -> bool:
        """
        Orchestrates schema creation and patching for a given table using the schema manager.
        """
        return self.schema_manager.ensure_schema(table_name, api_schema)

