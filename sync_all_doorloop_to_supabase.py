import logging
import time
import os
# Import the client classes
from doorloop_client import DoorLoopClient
from supabase_client import SupabaseClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("doorloop_sync.log"),
        logging.StreamHandler()
    ]
)

def log_success(message):
    logging.info("✅ " + message)

def log_error(message):
    logging.error("❌ " + message)

def log_warning(message):
    logging.warning("⚠️ " + message)

ENDPOINTS = [
    "accounts", "users", "properties", "units", "leases", "tenants",
    "lease-payments", "lease-charges", "lease-credits", "tasks",
    "owners", "vendors", "expenses", "vendor-bills", "vendor-credits",
    "communications", "notes", "files", "portfolios",
    # Add any other endpoints you need to sync
]

# --- IMPORTANT: Define API_SCHEMAS based on DoorLoop OpenAPI Spec ---
# This is a CRITICAL component for schema auto-patching.
# You MUST populate this dictionary completely and accurately.
# The 'type' can be 'string', 'number', 'integer', 'boolean', 'object', 'array'.
# 'format' can be 'date', 'date-time' for strings.
# 'items' is used if 'type' is 'array' to describe elements.
API_SCHEMAS = {
    "accounts": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "active": {"type": "boolean"},
        "type": {"type": "string"},
        "description": {"type": "string"},
        "systemAccount": {"type": "boolean"},
        "fullyQualifiedName": {"type": "string"},
        "cashFlowActivity": {"type": "string"},
        "defaultAccountFor": {"type": "object"},
        "createdAt": {"type": "string", "format": "date-time"},
        "updatedAt": {"type": "string", "format": "date-time"},
        "createdBy": {"type": "string"},
        "updatedBy": {"type": "string"}
    },
    "users": {
        "id": {"type": "string"},
        "firstName": {"type": "string"},
        "lastName": {"type": "string"},
        "middleName": {"type": "string"},
        "gender": {"type": "string"},
        "fullName": {"type": "string"},
        "dateOfBirth": {"type": "string", "format": "date"},
        "timezone": {"type": "string"},
        "company": {"type": "string"},
        "companyName": {"type": "string"},
        "jobTitle": {"type": "string"},
        "notes": {"type": "string"},
        "phones": {"type": "array", "items": {"type": "object"}},
        "emails": {"type": "array", "items": {"type": "object"}},
        "primaryAddress": {"type": "object"},
        "pictureUrl": {"type": "string"},
        "active": {"type": "boolean"},
        "loginEmail": {"type": "string"},
        "role": {"type": "string"},
        "properties": {"type": "string"}, # Check if this is truly a single string or array of IDs
        "lastSeenAt": {"type": "string", "format": "date-time"}
    },
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "type": {"type": "string"},
        "address": {"type": "object"},
        "description": {"type": "string"},
        "class": {"type": "string"}, # Will be mapped to class_name
        "owners": {"type": "array", "items": {"type": "object"}},
        "pictures": {"type": "array", "items": {"type": "object"}},
        "amenities": {"type": "array", "items": {"type": "string"}},
        "numActiveUnits": {"type": "integer"},
        "batch": {"type": "string"},
        "settings": {"type": "object"},
        "createdAt": {"type": "string", "format": "date-time"},
        "updatedAt": {"type": "string", "format": "date-time"},
        "createdBy": {"type": "string"},
        "updatedBy": {"type": "string"},
        "boardMembers": {"type": "array", "items": {"type": "object"}},
        "petsPolicy": {"type": "object"},
        "isValidAddress": {"type": "boolean"},
        "externalId": {"type": "string"},
        "managerId": {"type": "string"},
        "insuranceJson": {"type": "object"},
        "taxInfoJson": {"type": "object"},
        "financialsJson": {"type": "object"},
        "complianceJson": {"type": "object"},
        "purchaseDate": {"type": "string", "format": "date"},
        "purchasePrice": {"type": "number"},
        "currentValue": {"type": "number"},
        "bedroomCount": {"type": "integer"}
    },
    "units": {
        "id": {"type": "string"},
        "active": {"type": "boolean"},
        "address": {"type": "object"},
        "addressSameAsProperty": {"type": "boolean"},
        "name": {"type": "string"},
        "beds": {"type": "number"},
        "baths": {"type": "number"},
        "size": {"type": "number"},
        "marketRent": {"type": "number"},
        "property": {"type": "string"}, # Will be mapped to property_id
        "pictures": {"type": "array", "items": {"type": "object"}},
        "description": {"type": "string"},
        "listing": {"type": "object"},
        "amenities": {"type": "array", "items": {"type": "string"}}
    },
    "leases": {
        "id": {"type": "string"},
        "property": {"type": "string"}, # Will be mapped to property_id
        "name": {"type": "string"},
        "notes": {"type": "string"},
        "reference": {"type": "string"},
        "start": {"type": "string", "format": "date"},
        "end": {"type": "string", "format": "date"},
        "term": {"type": "string"},
        "rolloverToAtWill": {"type": "boolean"},
        "units": {"type": "array", "items": {"type": "string"}}, # Array of unit IDs
        "status": {"type": "string"},
        "evictonPending": {"type": "boolean"}, # Note: API typo "evictonPending"
        "proofOfInsuranceRequired": {"type": "boolean"},
        "totalBalancedue": {"type": "number"}, # Note: API typo "totalBalancedue"
        "totalDepositsHeld": {"type": "number"},
        "totalRecurringRent": {"type": "number"},
        "totalRecurringPayments": {"type": "number"},
        "totalRecurringCredits": {"type": "number"},
        "TotalRecurringCharges": {"type": "number"} # Note: API typo "TotalRecurringCharges"
    },
    "tenants": {
        "id": {"type": "string"},
        "firstName": {"type": "string"},
        "lastName": {"type": "string"},
        "middleName": {"type": "string"},
        "gender": {"type": "string"},
        "fullName": {"type": "string"},
        "dateOfBirth": {"type": "string", "format": "date"},
        "timezone": {"type": "string"},
        "company": {"type": "string"},
        "companyName": {"type": "string"},
        "jobTitle": {"type": "string"},
        "notes": {"type": "string"},
        "phones": {"type": "array", "items": {"type": "object"}},
        "emails": {"type": "array", "items": {"type": "object"}},
        "primaryAddress": {"type": "object"},
        "pictureUrl": {"type": "string"},
        "dependants": {"type": "array", "items": {"type": "object"}},
        "pets": {"type": "array", "items": {"type": "object"}},
        "vehicles": {"type": "array", "items": {"type": "object"}},
        "emergencyContacts": {"type": "array", "items": {"type": "object"}},
        "prospectInfo": {"type": "object"},
        "portalInfo": {"type": "object"},
        "type": {"type": "string"}
    },
    "lease-payments": {
        "id": {"type": "string"},
        "reference": {"type": "string"},
        "amountReceived": {"type": "number"},
        "paymentMethod": {"type": "string"},
        "lease": {"type": "string"}, # Will be mapped to lease_id
        "receivedFromTenant": {"type": "string"}, # Will be mapped to received_from_tenant_id
        "depositToAccount": {"type": "string"}, # Will be mapped to deposit_to_account_id
        "autoApplyPaymentOnCharges": {"type": "boolean"},
        "autoDeposit": {"type": "boolean"},
        "depositStatus": {"type": "string"},
        "reversedPayment": {"type": "string"} # Will be mapped to reversed_payment_id
    },
    "lease-charges": {
        "id": {"type": "string"},
        "lease": {"type": "string"}, # Will be mapped to lease_id
        "lines": {"type": "array", "items": {"type": "object"}},
        "memo": {"type": "string"},
        "reference": {"type": "string"},
        "date": {"type": "string", "format": "date"}, # Will be mapped to date_field
        "batch": {"type": "string"},
        "totalAmount": {"type": "number"}
    },
    "lease-credits": {
        "id": {"type": "string"},
        "lease": {"type": "string"}, # Will be mapped to lease_id
        "lines": {"type": "array", "items": {"type": "object"}},
        "memo": {"type": "string"},
        "reference": {"type": "string"},
        "date": {"type": "string", "format": "date"}, # Will be mapped to date_field
        "batch": {"type": "string"},
        "totalAmount": {"type": "number"}
    },
    "tasks": {
        "id": {"type": "string"},
        "type": {"type": "string"},
        "reference": {"type": "string"},
        "subject": {"type": "string"},
        "description": {"type": "string"},
        "dueDate": {"type": "string", "format": "date"},
        "status": {"type": "string"},
        "priority": {"type": "string"},
        "requestedByUser": {"type": "string"},
        "requestedByTenant": {"type": "string"},
        "requestedByOwner": {"type": "string"},
        "assignedToUsers": {"type": "array", "items": {"type": "string"}},
        "property": {"type": "string"}, # Will be mapped to property_id
        "unit": {"type": "string"}, # Will be mapped to unit_id
        "notifyTenant": {"type": "boolean"},
        "notifyAssignees": {"type": "boolean"},
        "entryNotes": {"type": "string"},
        "workOrder": {"type": "object"},
        "entryPermission": {"type": "string"},
        "createdAt": {"type": "number"}, # Unix timestamp
        "updatedAt": {"type": "number"} # Unix timestamp
    },
    "owners": {
        "id": {"type": "string"},
        "firstName": {"type": "string"},
        "lastName": {"type": "string"},
        "middleName": {"type": "string"},
        "gender": {"type": "string"},
        "fullName": {"type": "string"},
        "dateOfBirth": {"type": "string", "format": "date"},
        "timezone": {"type": "string"},
        "company": {"type": "string"},
        "companyName": {"type": "string"},
        "jobTitle": {"type": "string"},
        "notes": {"type": "string"},
        "phones": {"type": "array", "items": {"type": "object"}},
        "emails": {"type": "array", "items": {"type": "object"}},
        "primaryAddress": {"type": "object"},
        "pictureUrl": {"type": "string"},
        "active": {"type": "boolean"},
        "managementStartDate": {"type": "string", "format": "date"},
        "managementEndDate": {"type": "string", "format": "date"},
        "federalTaxInfo": {"type": "object"}
    },
    "vendors": {
        "id": {"type": "string"},
        "firstName": {"type": "string"},
        "lastName": {"type": "string"},
        "middleName": {"type": "string"},
        "gender": {"type": "string"},
        "fullName": {"type": "string"},
        "dateOfBirth": {"type": "string", "format": "date"},
        "timezone": {"type": "string"},
        "company": {"type": "string"},
        "companyName": {"type": "string"},
        "jobTitle": {"type": "string"},
        "notes": {"type": "string"},
        "phones": {"type": "array", "items": {"type": "object"}},
        "emails": {"type": "array", "items": {"type": "object"}},
        "primaryAddress": {"type": "object"},
        "pictureUrl": {"type": "string"},
        "active": {"type": "boolean"},
        "balance": {"type": "number"},
        "properties": {"type": "array", "items": {"type": "string"}}, # Array of property IDs
        "insuranceInfo": {"type": "object"},
        "federalTaxInfo": {"type": "object"}
    },
    "expenses": {
        "id": {"type": "string"},
        "payFromAccount": {"type": "string"}, # Will be mapped to pay_from_account_id
        "paymentMethod": {"type": "string"},
        "payToResourceType": {"type": "string"},
        "payToResourceId": {"type": "string"},
        "lines": {"type": "array", "items": {"type": "object"}},
        "memo": {"type": "string"},
        "reference": {"type": "string"},
        "date": {"type": "string", "format": "date"}, # Will be mapped to date_field
        "batch": {"type": "string"},
        "totalAmount": {"type": "number"}
    },
    "vendor-bills": {
        "id": {"type": "string"},
        "vendor": {"type": "string"}, # Will be mapped to vendor_id
        "lines": {"type": "array", "items": {"type": "object"}},
        "date": {"type": "string", "format": "date"}, # Will be mapped to date_field
        "dueDate": {"type": "string", "format": "date"},
        "memo": {"type": "string"},
        "reference": {"type": "string"},
        "batch": {"type": "string"},
        "totalAmount": {"type": "number"},
        "totalBalance": {"type": "number"}
    },
    "vendor-credits": {
        "id": {"type": "string"},
        "vendor": {"type": "string"}, # Will be mapped to vendor_id
        "lines": {"type": "array", "items": {"type": "object"}},
        "date": {"type": "string", "format": "date"}, # Will be mapped to date_field
        "dueDate": {"type": "string", "format": "date"},
        "memo": {"type": "string"},
        "reference": {"type": "string"},
        "batch": {"type": "string"},
        "totalAmount": {"type": "number"},
        "totalBalance": {"type": "number"}
    },
    "communications": {
        "id": {"type": "string"},
        "subject": {"type": "string"},
        "bodyHtml": {"type": "string"},
        "bodyPreview": {"type": "string"},
        "from": {"type": "object"}, # Will be mapped to from_participant
        "to": {"type": "array", "items": {"type": "object"}}, # Will be mapped to to_participants
        "cc": {"type": "array", "items": {"type": "object"}}, # Will be mapped to cc_participants
        "bcc": {"type": "array", "items": {"type": "object"}}, # Will be mapped to bcc_participants
        "sentAt": {"type": "number"}, # Unix timestamp
        "type": {"type": "string"},
        "threadId": {"type": "string"},
        "externalId": {"type": "string"},
        "openedAt": {"type": "array", "items": {"type": "number"}}, # Array of Unix timestamps
        "clickedAt": {"type": "array", "items": {"type": "number"}}, # Array of Unix timestamps
        "bouncedAt": {"type": "array", "items": {"type": "number"}}, # Array of Unix timestamps
        "status": {"type": "string"},
        "announcement": {"type": "string"} # Will be mapped to announcement_id
    },
    "notes": {
        "id": {"type": "string"},
        "title": {"type": "string"},
        "body": {"type": "string"},
        "tags": {"type": "array", "items": {"type": "string"}}, # Array of mongoId
        "linkedResource": {"type": "object"}, # Will be mapped to linked_resource
        "createdAt": {"type": "number"}, # Unix timestamp
        "createdBy": {"type": "string"}
    },
    "files": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "notes": {"type": "string"},
        "linkedResource": {"type": "object"}, # Will be mapped to linked_resource
        "tags": {"type": "array", "items": {"type": "string"}}, # Array of mongoId
        "size": {"type": "number"},
        "mimeType": {"type": "string"},
        "createdBy": {"type": "string"},
        "createdAt": {"type": "string"}, # API has this as string, not timestamp
        "downloadUrl": {"type": "string"}
    },
    "portfolios": { # Maps to property_groups
        "id": {"type": "string"},
        "name": {"type": "string"},
        "properties": {"type": "array", "items": {"type": "string"}} # Array of Property IDs
    },
    "lease-returned-payments": { # Assuming this endpoint exists and has a schema
        "id": {"type": "string"},
        "leasePayment": {"type": "string"}, # Will be mapped to lease_payment_id
        "lease": {"type": "string"}, # Will be mapped to lease_id
        "lines": {"type": "array", "items": {"type": "object"}},
        "memo": {"type": "string"},
        "reference": {"type": "string"},
        "date": {"type": "string", "format": "date"}, # Will be mapped to date_field
        "batch": {"type": "string"}
    }
}


if __name__ == "__main__":
    start = time.time()
    log_success("🚀 Starting DoorLoop → Supabase sync")

    try:
        # Fetch environment variables at the application's entry point
        # This is the crucial change to resolve the ValueError.
        DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
        DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")
        SUPABASE_URL = os.getenv("SUPABASE_URL")
        SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        # Validate that they are set
        if not DOORLOOP_API_KEY:
            raise ValueError("❌ Environment variable DOORLOOP_API_KEY is not set.")
        if not DOORLOOP_API_BASE_URL:
            raise ValueError("❌ Environment variable DOORLOOP_API_BASE_URL is not set.")
        if not SUPABASE_URL:
            raise ValueError("❌ Environment variable SUPABASE_URL is not set.")
        if not SUPABASE_SERVICE_ROLE_KEY:
            raise ValueError("❌ Environment variable SUPABASE_SERVICE_ROLE_KEY is not set.")

        # Initialize client instances
        dl_client = DoorLoopClient(api_key=DOORLOOP_API_KEY, base_url=DOORLOOP_API_BASE_URL)
        sb_client = SupabaseClient(url=SUPABASE_URL, service_role_key=SUPABASE_SERVICE_ROLE_KEY)

        for endpoint in ENDPOINTS:
            try:
                log_success(f"🔄 Syncing endpoint: {endpoint}")

                # Determine target Supabase table name
                table_name = endpoint.replace("-", "_")
                if endpoint == "portfolios":
                    table_name = "property_groups"
                # Add other specific mappings if needed (e.g., lease-returned-payments -> lease_reversed_payments)

                # 1. Schema Validation & Auto-creation
                api_schema_for_endpoint = API_SCHEMAS.get(endpoint)
                if api_schema_for_endpoint:
                    log_success(f"Ensuring schema for '{table_name}' using defined API schema.")
                    # Call the schema manager's ensure_schema method via supabase_client
                    schema_ok = sb_client.ensure_table_and_columns(table_name, api_schema_for_endpoint)
                    if not schema_ok:
                        log_error(f"❌ Failed to ensure schema for {table_name}. Skipping data sync for this endpoint.")
                        continue # Skip data sync if schema setup failed
                    # Add a short delay here to allow PostgREST's schema cache to refresh
                    time.sleep(1) # Sleep for 1 second. Adjust as needed (e.g., 0.5 to 2 seconds).
                else:
                    log_warning(f"⚠️ No API schema defined for endpoint '{endpoint}'. Skipping schema auto-patch for this table. Data upsert might fail if table/columns are missing or types are mismatched.")
                    # If you want to strictly require schema definition, uncomment the 'continue' below:
                    # continue

                # 2. Fetch records using the DoorLoopClient instance
                records = dl_client.fetch_all(endpoint)
                if not records:
                    log_success(f"No data for {endpoint}. Skipping.")
                    continue

                # 3. Upsert data using the SupabaseClient instance
                sb_client.upsert_data(
                    table_name=table_name,
                    records=records,
                    primary_key_field="id" # Assuming 'id' is always the primary key
                )

                log_success(f"✅ Synced {len(records)} records to {table_name}")
            except Exception as e:
                log_error(f"❌ Failed syncing {endpoint}: {e}")
                # Continue to next endpoint even if one fails
                continue

    except ValueError as ve: # Catch specific ValueError for missing env vars
        log_error(f"❌ Configuration Error: {ve}")
        exit(1) # Exit with a non-zero status code to indicate failure
    except Exception as e:
        log_error(f"❌ An unexpected error occurred during sync process: {e}")
        exit(1) # Exit with a non-zero status code

    log_success(f"🎉 Sync complete in {time.time() - start:.2f}s")
