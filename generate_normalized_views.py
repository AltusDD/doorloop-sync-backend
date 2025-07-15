# generate_normalized_views.py

import os
import json
import logging
import requests

# Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DEBUG: Log Environment Variables ---
_supabase_url_val = os.environ.get("SUPABASE_URL")
_supabase_key_val = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

logger.info(f"DEBUG_ENV: SUPABASE_URL: {'SET' if _supabase_url_val else 'NOT SET'}")
logger.info(f"DEBUG_ENV: SUPABASE_SERVICE_ROLE_KEY: {'SET' if _supabase_key_val else 'NOT SET'}")
if _supabase_key_val:
    logger.info(f"DEBUG_ENV: SUPABASE_SERVICE_ROLE_KEY (first 5 chars): {_supabase_key_val[:5]}...")
# --- END DEBUG ---

SUPABASE_URL = _supabase_url_val
SUPABASE_SERVICE_ROLE_KEY = _supabase_key_val

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}

# List of all doorloop_raw_* tables for which to generate views
RAW_TABLES_TO_VIEW = [
    "doorloop_raw_properties",
    "doorloop_raw_units",
    "doorloop_raw_tenants",
    "doorloop_raw_owners",
    "doorloop_raw_leases",
    "doorloop_raw_lease_payments",
    "doorloop_raw_lease_charges",
    "doorloop_raw_lease_credits",
    "doorloop_raw_vendors",
    "doorloop_raw_tasks",
    "doorloop_raw_files",
    "doorloop_raw_notes",
    "doorloop_raw_communications",
    "doorloop_raw_applications",
    "doorloop_raw_inspections",
    "doorloop_raw_insurance_policies",
    "doorloop_raw_recurring_charges",
    "doorloop_raw_recurring_credits",
    "doorloop_raw_accounts",
    "doorloop_raw_users",
    "doorloop_raw_portfolios",
    "doorloop_raw_reports",
    "doorloop_raw_activity_logs",
]

def get_raw_table_names():
    sql = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name LIKE 'doorloop_raw_%'
    ORDER BY table_name;
    """
    try:
        table_rows = execute_sql_query(sql)
        return [row[0] for row in table_rows]
    except Exception as e:
        logger.warning(f"⚠️ Falling back to hardcoded table list due to error fetching raw table names: {e}")
        return [
            "doorloop_raw_properties", "doorloop_raw_units", "doorloop_raw_leases", "doorloop_raw_tenants",
            "doorloop_raw_lease_payments", "doorloop_raw_lease_charges", "doorloop_raw_owners", "doorloop_raw_vendors",
            "doorloop_raw_tasks", "doorloop_raw_files", "doorloop_raw_notes", "doorloop_raw_communications",
            "doorloop_raw_applications", "doorloop_raw_inspections", "doorloop_raw_insurance_policies",
            "doorloop_raw_recurring_charges", "doorloop_raw_recurring_credits", "doorloop_raw_accounts",
            "doorloop_raw_users", "doorloop_raw_portfolios", "doorloop_raw_reports", "doorloop_raw_activity_logs",
        ]

def get_table_columns(table_name: str):
    sql = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    try:
        data = execute_sql_query(sql)

        columns_to_exclude = {
            "data", "leaseDepositItem", "leasechargeitem", "totalbalance", "register", "tags", "taxable", 
            "openedat", "linkedresource", "defaultaccountfor", "sentat", "bouncedat", "lines", "bcc", 
            "acceptedontos", "duedate", "portalinfo", "rank", "conversationwelcomesmsentat", "from", 
            "prospectinfo", "pets", "metadata", "amountnotappliedtocharges", "autoapplypaymentoncharges", 
            "issharedwithtenant", "epayinfo", "clickedat", "intercomtemplateid", "linkedcharges", 
            "linkedcredits", "checkinfo", "isfilessharedwithtenant", "size", "amountappliedtocredits", 
            "amountreceived", "amount", "reversedpaymentdate", "isvoidedcheck", "properties", 
            "totalamount", "lastlatefeesprocesseddate", "active", "boardmembers", "amenities", 
            "petspolicy", "address", "owners", "settings", "emails", "numactiveunits", 
            "dependants", "primaryaddress", "pictures", "emergencycontacts", "phones", 
            "company", "vehicles", "workorder", "marketrent", "propertygroups", "currentbalance", 
            "proofofinsuranceprovided", "evictionpending", "totalrecurringrent", "totalrecurringcharges", 
            "totalrecurringcredits", "proofofinsuranceprovidedat", "outgoingepayenabled", "start", 
            "end", "outgoingepay", "totaldepositsheld", "proofofinsuranceexpirationdate", 
            "managementstartdate", "owner", "services", "upcomingbalance", "proofofinsuranceeffectivedate", 
            "balance", "units", "to", "accounts", "conversationwelcomesmsentat", "epayinfo", 
            "lastname", "source_endpoint", "paymentmethod", "memo", "createdby", "reference", 
            "batch", "paytoresourceid", "paytoresourcetype", "payfromaccount", "updatedby", 
            "externalid", "conversationmessage", "subjecttype", "intercomreceiptid", "bodypreview", 
            "announcement", "type", "conversation", "failedreason", "subject", "intercomcontactid", 
            "status", "title", "body", "unit", "property", "typedescription", "createdbytype", 
            "notes", "mimetype", "createdbyname", "downloadurl", "term", "recurringrentstatus", 
            "depositstatus", "lease", "leasepayment", "reason", "depositentry", "reversedpaymentmemo", 
            "receivedfromtenant", "deposittoserviceaccount", "reversedpayment", "recurringtransaction", 
            "latefeeforleasecharge", "entrynotes", "entrypermission", "requestedbytenant", 
            "requestedbyuser", "tenantrequestmaintenancecategory", "tenantrequesttype", "description", 
            "priority", "requestedbyowner", "jobtitle", "e164phonemobilenumber", "role", "timezone", 
            "loginemail", "middlename", "otherscreeningservice", "stripecustomerid", "screeningservice", 
            "gender", "invitationlastsentat", "systemaccount", "fullyqualifiedname", "cashflowactivity", 
            "vendor", "owner", "lease", "depositToAccount", "unit", "subject", "property", "dateType", 
            "requestByTenant", "requestByUser", "tenantRequestMaintenanceCategory", "tenantRequestType", 
            "status", "description", "priority", "requestedByOwner", "companyName", "firstName", 
            "fullName", "e64PhoneMobileNumber", "name", "lastName", "notes", "email", "phone", "trade", 
            "active", "display_name", "created_at", "updated_at", "doorloop_id", "id", "_raw_payload", "payload_json"
        }

        filtered_columns = []
        if isinstance(data, list) and data:
            if isinstance(data[0], list): 
                filtered_columns = [
                    row[0] for row in data
                    if row[0].lower() not in columns_to_exclude
                ]
            elif isinstance(data[0], dict) and 'column_name' in data[0]: 
                filtered_columns = [
                    row['column_name'] for row in data
                    if row['column_name'].lower() not in columns_to_exclude
                ]

        essential_columns = {"id", "doorloop_id", "payload_json", "created_at", "updated_at", "_raw_payload"}

        final_columns = list(set(filtered_columns) | essential_columns) 
        final_columns.sort() 

        logger.info(f"✅ Columns used in view: {final_columns}")
        return final_columns
    except Exception as e:
        logger.error(f"❌ Failed to fetch columns for {table_name}: {e}")
        raise

def build_view_sql(raw_table_name, columns):
    view_name = raw_table_name.replace("doorloop_raw_", "") 
    quoted_columns = [f'"{col}"' for col in columns]
    select_clause = ", ".join(quoted_columns)

    sql = f"""
CREATE OR REPLACE VIEW public."{view_name}" AS
SELECT
    {select_clause}
FROM public."{raw_table_name}";
"""
    return sql

def execute_sql_via_rpc(sql_command):
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql" 
    payload = {"sql": sql_command}

    logger.info(f"DEBUG_EXEC_SQL: Executing SQL via RPC: {sql_command.splitlines()[0].strip()}...")
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        response.raise_for_status() 

        logger.info(f"DEBUG_EXEC_SQL: SQL RPC response: {response.status_code} -> {response.text[:200]}...")
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"ERROR_EXEC_SQL: Failed to execute SQL via RPC: {e.response.status_code if e.response else ''} -> {e.response.text if e.response else str(e)}")
        raise

def run():
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logger.error("❌ CRITICAL: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
        raise ValueError("Missing Supabase environment variables.")

    raw_tables = get_raw_table_names()
    if not raw_tables:
        logger.warning("⚠️ No raw tables found. Exiting.")
        return

    for table in raw_tables:
        logger.info(f"🔄 Processing {table}...")
        try:
            columns = get_table_columns(table)

            if not columns:
                logger.warning(f"⚠️ No columns found for {table}. Skipping view creation.")
                continue

            create_or_replace_view(table, columns)
        except Exception as e:
            logger.error(f"❌ Failed to process {table} for view generation: {type(e).__name__}: {e}")
            continue 

if __name__ == "__main__":
    run()