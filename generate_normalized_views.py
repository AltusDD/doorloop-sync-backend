# generate_normalized_views.py

import os
import requests
import logging
import json # Import json for json.dumps if needed

# Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) # Use logger instance

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

def execute_sql_query(sql: str):
    """
    Executes SQL queries via Supabase's /rpc/execute_sql endpoint.
    Returns the JSON response.
    """
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    # FIX: Payload key must be 'sql', not 'sql_text'
    payload = {"sql": sql.strip()} 

    logger.info(f"📤 Executing SQL: {payload['sql'].splitlines()[0]}...")
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        response.raise_for_status() # This will raise HTTPError for 4xx/5xx responses

        # Supabase RPC for SELECT returns a JSON array of arrays (e.g., [[val1], [val2]])
        # or a JSON array of objects (e.g., [{"col": val1}, {"col": val2}]).
        # We need to handle both.
        json_response = response.json()

        logger.info(f"✅ SQL execution succeeded: {payload['sql'].splitlines()[0]}...")
        return json_response
    except requests.exceptions.HTTPError as e:
        logger.error(f"❌ Error calling execute_sql RPC: {e.response.status_code} {e.response.reason} for url: {url}")
        logger.error(f"Response text: {e.response.text}") # Log the full response text
        raise # Re-raise the HTTPError
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSONDecodeError from RPC response: {e}. Response text: {response.text}")
        raise # Re-raise JSONDecodeError
    except Exception as e:
        logger.error(f"❌ Generic error during RPC call: {type(e).__name__}: {e}")
        raise

def get_raw_table_names():
    """
    Fetches names of all doorloop_raw_% tables in the public schema.
    """
    sql = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name LIKE 'doorloop_raw_%'
    ORDER BY table_name;
    """
    try:
        # execute_sql_query returns a list of lists, e.g., [['table1'], ['table2']]
        # or list of dicts if it returns column names.
        # For table names, it's typically list of lists.
        table_rows = execute_sql_query(sql)
        return [row[0] for row in table_rows] # Extract table name from list
    except Exception as e:
        logger.warning(f"⚠️ Falling back to hardcoded table list due to error fetching raw table names: {e}")
        # Fallback list (should match all tables in your schema_v3.7)
        return [
            "doorloop_raw_properties", "doorloop_raw_units", "doorloop_raw_tenants",
            "doorloop_raw_owners", "doorloop_raw_leases", "doorloop_raw_lease_payments",
            "doorloop_raw_lease_charges", "doorloop_raw_lease_credits", "doorloop_raw_vendors",
            "doorloop_raw_tasks", "doorloop_raw_files", "doorloop_raw_notes",
            "doorloop_raw_communications", "doorloop_raw_applications", "doorloop_raw_inspections",
            "doorloop_raw_insurance_policies", "doorloop_raw_recurring_charges", "doorloop_raw_recurring_credits",
            "doorloop_raw_accounts", "doorloop_raw_users", "doorloop_raw_portfolios",
            "doorloop_raw_reports", "doorloop_raw_activity_logs", "doorloop_raw_expenses",
            "doorloop_raw_vendor_bills", "doorloop_raw_vendor_credits", "doorloop_raw_lease_reversed_payments",
            "doorloop_raw_property_groups"
        ]

def get_table_columns(table_name: str):
    """
    Fetches column names for a given table from information_schema.columns via RPC.
    """
    sql = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    try:
        # execute_sql_query returns a list of lists, e.g., [['col1'], ['col2']]
        column_rows = execute_sql_query(sql)

        # FIX: Filter columns to exclude problematic ones (e.g., from previous failed migrations)
        # This list should be exhaustive based on your logs.
        columns_to_exclude = {
            "data", "leaseDepositItem", "leasechargeitem", "totalbalance", "register", "tags", "taxable", 
            "openedat", "linkedresource", "defaultaccountfor", "sentat", "bouncedat", "lines", "bcc", 
            "acceptedontos", "duedate", "portalinfo", "rank", "conversationwelcomesmsentat", "from", 
            "prospectinfo", "pets", "metadata", "amountnotappliedtocharges", "autoapplypaymentoncharges", 
            "issharedwithtenant", "epayinfo", "clickedat", "intercomtemplateid", "linkedcharges", 
            "linkedcredits", "checkinfo", "isfilessharedwithtenant", "size", "amountappliedtocredits", 
            "amountreceived", "amount", "reversedpaymentdate", "isvoidedcheck", "properties", "totalamount", 
            "lastlatefeesprocesseddate", "active", "boardmembers", "amenities", "petspolicy", "address", 
            "owners", "settings", "emails", "numactiveunits", "dependants", "primaryaddress", "pictures", 
            "emergencycontacts", "phones", "company", "vehicles", "rentalamount", "listing", 
            "assignedtousers", "completedat", "bankaccounts", "beds", "workorder", "marketrent", 
            "propertygroups", "currentbalance", "proofofinsuranceprovided", "evictionpending", 
            "totalrecurringrent", "totalrecurringcharges", "totalrecurringcredits", "proofofinsuranceprovidedat", 
            "outgoingepayenabled", "start", "end", "outgoingepay", "totaldepositsheld", 
            "proofofinsuranceexpirationdate", "managementstartdate", "owner", "services", 
            "upcomingbalance", "proofofinsuranceeffectivedate", "balance", "units", "to", 
            "accounts", "conversationwelcomesmsentat", "epayinfo", "lastname", "source_endpoint", 
            "paymentmethod", "memo", "createdby", "reference", "batch", "paytoresourceid", 
            "paytoresourcetype", "payfromaccount", "updatedby", "externalid", "conversationmessage", 
            "subjecttype", "intercomreceiptid", "bodypreview", "announcement", "type", "conversation", 
            "failedreason", "subject", "intercomcontactid", "status", "title", "body", "unit", 
            "property", "typedescription", "createdbytype", "notes", "mimetype", "createdbyname", 
            "downloadurl", "term", "recurringrentstatus", "depositstatus", "lease", "leasepayment", 
            "reason", "depositentry", "reversedpaymentmemo", "receivedfromtenant", "deposittoserviceaccount", 
            "reversedpayment", "recurringtransaction", "latefeeforleasecharge", "entrynotes", 
            "entrypermission", "requestedbytenant", "requestedbyuser", "tenantrequestmaintenancecategory", 
            "tenantrequesttype", "description", "priority", "requestedbyowner", "jobtitle", 
            "e164phonemobilenumber", "role", "intercomcontactid", "timezone", "loginemail", "middle_name", 
            "otherscreeningservice", "stripecustomerid", "screeningservice", "gender", "invitationlastsentat", 
            "systemaccount", "fullyqualifiedname", "cashflowactivity", "vendor", "owner", "lease", 
            "depositToAccount", "unit", "subject", "property", "dateType", "requestByTenant", 
            "requestByUser", "tenantRequestMaintenanceCategory", "tenantRequestType", "status", 
            "description", "priority", "requestedByOwner", "companyName", "firstName", "fullName", 
            "e64PhoneMobileNumber", "name", "lastName", "notes", "email", "phone", "trade", "active", 
            "display_name", "created_at", "updated_at", "doorloop_id", "id", "_raw_payload", "payload_json"
        } # Ensure core columns are NOT excluded

        # Filter out columns that are not in the expected base schema or are problematic
        # Convert column names to lowercase for robust comparison with the exclude list
        filtered_columns = [
            row[0] for row in column_rows # Access by index as it's list of lists
            if row[0].lower() not in columns_to_exclude
        ]

        # Ensure essential columns are always included, even if filtered out by mistake or not in sample
        # These are the columns that *must* be in every doorloop_raw_* table.
        essential_columns = {"id", "doorloop_id", "payload_json", "created_at", "updated_at", "_raw_payload"}

        final_columns = list(set(filtered_columns) | essential_columns) # Union of filtered and essential
        final_columns.sort() # Sort for consistent view definition

        logger.info(f"✅ Columns used in view: {final_columns}")
        return final_columns
    except Exception as e:
        logger.error(f"❌ Failed to fetch columns for {table_name}: {e}")
        raise

def create_or_replace_view(table_name: str, columns: list[str]):
    """
    Builds and executes the CREATE OR REPLACE VIEW SQL statement dynamically.
    """
    view_name = table_name.replace("doorloop_raw_", "") 

    quoted_columns = [f'"{col}"' for col in columns]

    select_clause = ", ".join(quoted_columns)

    sql = f"""
CREATE OR REPLACE VIEW public."{view_name}" AS
SELECT
    {select_clause}
FROM public."{table_name}";
"""
    try:
        execute_sql_query(sql) # Use the robust execute_sql_query
        logger.info(f"✅ View 'public.{view_name}' created/replaced successfully.")
    except Exception as e:
        logger.error(