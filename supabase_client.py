import os
import requests
import json
import hashlib
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

# supabase_client.py

_logger.setLevel(logging.INFO)

def _safe_json_dumps(data: Any) -> Optional[str]:
    """
    Safely dumps a Python object to a JSON string. Handles non-serializable
    types by converting them to strings and logs a warning.
    Returns None if the input data is None.
    """
    if data is None:
        return None
    try:
        return json.dumps(data, default=str)
    except TypeError as e:
        _logger.warning(f"WARN: Failed to dump data to JSON for JSONB storage: {type(data)} - {data}. Error: {e}")
        return str(data)

def _convert_date_to_iso(date_str: Optional[str]) -> Optional[str]:
    """
    Converts various date string formats to ISO 8601 format (YYYY-MM-DDTHH:MM:SS).
    Handles 'Z' for UTC, timezone offsets, and milliseconds.
    Returns the original string if parsing fails, logging a warning.
    Returns None if the input date_str is None or empty.
    """
    if not date_str:
        return None
    if isinstance(date_str, datetime):
        return date_str.isoformat(timespec='seconds')
    try:
        # Try parsing with milliseconds and 'Z' (UTC)
        if date_str.endswith('Z'):
            try:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            except ValueError:
                # Fallback for no milliseconds but with 'Z'
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
        elif '+' in date_str:
            # Handle timezone offset with potential milliseconds
            try:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
            except ValueError:
                # Fallback for no milliseconds but with timezone offset
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')
        elif 'T' in date_str:
            # Handle local time with potential milliseconds
            try:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
            except ValueError:
                # Fallback for no milliseconds and local time
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
        else:
            # Assume YYYY-MM-DD format
            dt_obj = datetime.strptime(date_str, '%Y-%m-%d')

        return dt_obj.isoformat(timespec='seconds')
    except ValueError:
        _logger.warning(f"WARN: Could not parse date string '{date_str}'. Returning original string.")
        return date_str

def upsert_raw_doorloop_data(
    endpoint: str,
    records: List[Dict[str, Any]],
    supabase_url: str,
    supabase_service_role_key: str
):
    """
    Upserts raw DoorLoop data into a generic 'raw_doorloop_data' table
    and also into entity-specific mirror tables (e.g., 'doorloop_raw_properties').

    Args:
        endpoint (str): The DoorLoop API endpoint (e.g., "/properties").
        records (List[Dict[str, Any]]): A list of dictionaries, where each dict
                                         is a record from the DoorLoop API.
        supabase_url (str): The base URL for your Supabase project.
        supabase_service_role_key (str): The Supabase service role key with
                                         permissions to insert/upsert data.
    Raises:
        ValueError: If Supabase URL, service role key, or endpoint is missing.
    """
        requests.exceptions.RequestException: If there's an HTTP error during
                                              the API call to Supabase.
    """
    """
    if not supabase_service_role_key:
        raise ValueError("Supabase service role key is missing.")
    if not supabase_url:
        raise ValueError("Supabase URL is missing.")
    if not endpoint:
        raise ValueError("Endpoint is missing for raw data upsert.")
    if not isinstance(records, list):
        raise TypeError(f"Expected records to be a list, got {type(records)}")

    headers = {
        "apikey": supabase_service_role_key,
        "Authorization": f"Bearer {supabase_service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal" # Get minimal response body on success
    }

    base_rest_url = f"{supabase_url}/rest/v1"

    # URL for the generic raw_doorloop_data table
    raw_doorloop_data_table_url = f"{base_rest_url}/raw_doorloop_data"
    # Derive table name for the mirror table (e.g., doorloop_raw_properties)
    table_name_raw = f"doorloop_raw_{endpoint.strip('/').replace('-', '_')}"
    mirror_table_url = f"{base_rest_url}/{table_name_raw}"

    _logger.info(f"DEBUG_SUPABASE_CLIENT: Starting raw upsert for {endpoint} ({len(records)} records)")

    # This list holds records for the generic 'raw_doorloop_data' table
    prepared_raw_doorloop_data = []
    # This list holds records mapped for specific 'doorloop_raw_<entity>' table
    prepared_mirror_table_data = []

    for i, record in enumerate(records):
        try:
            entity_dl_id = record.get('id', record.get('ID')) # Try 'id' then 'ID'
            if not entity_dl_id:
                _logger.warning(f"WARN_RECORD_PROCESS: Record {i+1} from {endpoint} has no identifiable ID. Skipping: {record}")
                continue # Skip records without an ID

            # Generate a SHA256 hash of the JSON payload for content-based deduplication/integrity check
            payload_hash = hashlib.sha256(_safe_json_dumps(record).encode('utf-8')).hexdigest()

            # Prepare data for the generic raw_doorloop_data table
            prepared_raw_doorloop_data.append({
                "endpoint": endpoint,
                "entity_dl_id": str(entity_dl_id),
                "payload_json": record, # Store the full raw payload as JSONB
                "payload_hash": payload_hash
            })

            # --- Prepare for specific doorloop_raw_<entity> table (MIRROR) ---
            # All mirror tables must have 'doorloop_id' and '_raw_payload'
            mapped_mirror_record = {
                "doorloop_id": str(entity_dl_id),
                "_raw_payload": record # Store raw payload in mirror table too for completeness
            }

            # --- START ENTITY-SPECIFIC MAPPING FOR doorloop_raw_* TABLES ---
            # These mappings must match your schema_v3.5_ultimate_full_rebuild_final_syntax_fix.sql
            # Add elif blocks for ALL DoorLoop endpoints you wish to mirror.

            if endpoint == "/properties":
                mapped_mirror_record.update({
                    "name": record.get('name'), "type": record.get('type'), "class": record.get('class'), "active": record.get('active'),
                    "address_street1": record.get('address',{}).get('street1'), "address_street2": record.get('address',{}).get('street2'),
                    "address_city": record.get('address',{}).get('city'), "address_state": record.get('address',{}).get('state'),
                    "address_zip": record.get('address',{}).get('zip'), "address_country": record.get('address',{}).get('country'),
                    "address_lat": record.get('address',{}).get('lat'), "address_lng": record.get('address',{}).get('lng'),
                    "description": record.get('description'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')),
                    "external_id": record.get('externalId'), "manager_id": record.get('managerId'),
                    "purchase_date": _convert_date_to_iso(record.get('purchaseDate')), "purchase_price": record.get('purchasePrice'),
                    "current_value": record.get('currentValue'), "bedroom_count": record.get('bedroomCount'),
                    "owners_json": _safe_json_dumps(record.get('owners')), "pictures_json": _safe_json_dumps(record.get('pictures')),
                    "amenities_json": _safe_json_dumps(record.get('amenities')),
                    "custom_fields_json": _safe_json_dumps(record.get('settings', {}).get('customFields')),
                    "board_members_json": _safe_json_dumps(record.get('boardMembers')),
                    "insurance_json": _safe_json_dumps(record.get('insurance')),
                    "tax_info_json": _safe_json_dumps(record.get('taxInfo')),
                    "financials_json": _safe_json_dumps(record.get('financials')),
                    "compliance_json": _safe_json_dumps(record.get('compliance'))
                })
            elif endpoint == "/units":
                mapped_mirror_record.update({
                    "property_id": record.get('property_id'), "unit_number": record.get('name'),
                    "beds": float(record.get('beds')) if record.get('beds') is not None else None,
                    "baths": float(record.get('baths')) if record.get('baths') is not None else None,
                    "sq_ft": float(record.get('size')) if record.get('size') is not None else None,
                    "status": record.get('status'), "active": record.get('active'), "rent_amount": float(record.get('marketRent')) if record.get('marketRent') is not None else None,
                    "description": record.get('description'), "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')),
                    "floor_plan": record.get('floorPlan'), "is_rentable": record.get('isRentable'), "last_renovated": _convert_date_to_iso(record.get('lastRenovated')), "unit_condition": record.get('condition'),
                    "address_json": _safe_json_dumps(record.get('address')), "listing_json": _safe_json_dumps(record.get('rentalApplicationListing')),
                    "amenities_json": _safe_json_dumps(record.get('amenities')), "pictures_json": _safe_json_dumps(record.get('photos')),
                    "features_json": _safe_json_dumps(record.get('features')), "utilities_json": _safe_json_dumps(record.get('utilities'))
                })
            elif endpoint == "/tenants":
                mapped_mirror_record.update({
                    "first_name": record.get('firstName'), "last_name": record.get('lastName'), "full_name": record.get('fullName'), "display_name": record.get('name'),
                    "date_of_birth": _convert_date_to_iso(record.get('dateOfBirth')), "timezone": record.get('timezone'), "company_name": record.get('companyName'), "job_title": record.get('jobTitle'), "notes": record.get('notes'),
                    "primary_email": next((e.get('address') for e in record.get('emails', []) if e.get('type') == 'Primary'), None),
                    "primary_phone": next((p.get('number') for p in record.get('phones', []) if p.get('type') in ['Mobile', 'Primary']), None),
                    "ssn": record.get('ssn'), "credit_score": record.get('creditScore'), "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')),
                    "primary_address_json": _safe_json_dumps(record.get('primaryAddress')), "emails_json": _safe_json_dumps(record.get('emails')), "phones_json": _safe_json_dumps(record.get('phones')),
                    "dependants_json": _safe_json_dumps(record.get('dependants')), "pets_json": _safe_json_dumps(record.get('pets')), "vehicles_json": _safe_json_dumps(record.get('vehicles')),
                    "emergency_contacts_json": _safe_json_dumps(record.get('emergencyContacts')), "prospect_info_json": _safe_json_dumps(record.get('prospectInfo')),
                    "portal_info_json": _safe_json_dumps(record.get('portalInfo')), "employment_info_json": _safe_json_dumps(record.get('employmentInfo')),
                    "background_check_json": _safe_json_dumps(record.get('backgroundCheck')), "references_json": _safe_json_dumps(record.get('references')),
                    "banking_info_json": _safe_json_dumps(record.get('bankingInfo'))
                })
            elif endpoint == "/owners":
                mapped_mirror_record.update({
                    "first_name": record.get('firstName'), "last_name": record.get('lastName'), "full_name": record.get('fullName'), "display_name": record.get('name'),
                    "date_of_birth": _convert_date_to_iso(record.get('dateOfBirth')), "timezone": record.get('timezone'), "company_name": record.get('companyName'), "job_title": record.get('jobTitle'), "notes": record.get('notes'),
                    "primary_email": next((e.get('address') for e in record.get('emails', []) if e.get('type') == 'Primary'), None),
                    "primary_phone": next((p.get('number') for p in record.get('phones', []) if p.get('type') in ['Mobile', 'Primary']), None),
                    "active": record.get('active'), "management_start_date": _convert_date_to_iso(record.get('managementStartDate')), "management_end_date": _convert_date_to_iso(record.get('managementEndDate')),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')),
                    "primary_address_json": _safe_json_dumps(record.get('primaryAddress')), "emails_json": _safe_json_dumps(record.get('emails')), "phones_json": _safe_json_dumps(record.get('phones')),
                    "federal_tax_info_json": _safe_json_dumps(record.get('federalTaxInfo')), "banking_info_json": _safe_json_dumps(record.get('bankingInfo'))
                })
            elif endpoint == "/leases":
                mapped_mirror_record.update({
                    "property_id": record.get('property'), "units_json": _safe_json_dumps(record.get('units')), "tenants_json": _safe_json_dumps(record.get('tenants')), "name": record.get('name'),
                    "start_date": _convert_date_to_iso(record.get('start')), "end_date": _convert_date_to_iso(record.get('end')), "term": record.get('term'), "status": record.get('status'), "eviction_pending": record.get('evictionPending'), "rollover_to_at_will": record.get('rolloverToAtWill'), "proof_of_insurance_required": record.get('proofOfInsuranceRequired'),
                    "total_balance_due": record.get('totalBalancedue'), "total_deposits_held": record.get('totalDepositsHeld'), "total_recurring_rent": record.get('totalRecurringRent'), "total_recurring_payments": record.get('totalRecurringPayments'), "total_recurring_credits": record.get('totalRecurringCredits'), "total_recurring_charges": record.get('TotalRecurringCharges'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')),
                    "reference_json": _safe_json_dumps(record.get('reference')), "notes_json": _safe_json_dumps(record.get('notes')), "settings_json": _safe_json_dumps(record.get('settings')), "renewal_info_json": _safe_json_dumps(record.get('renewalInfo')),
                    "custom_fields_json": _safe_json_dumps(record.get('customFields')), "pet_policy_json": _safe_json_dumps(record.get('petPolicy')), "addendums_json": _safe_json_dumps(record.get('addendums')),
                    "cosigners_json": _safe_json_dumps(record.get('cosigners')), "utilities_json": _safe_json_dumps(record.get('utilities')), "insurance_status_json": _safe_json_dumps(record.get('proofOfInsuranceStatus'))
                })
            elif endpoint == "/lease-payments":
                mapped_mirror_record.update({
                    "lease_id": record.get('lease_id'), "amount": record.get('amountReceived'), "payment_date": _convert_date_to_iso(record.get('payment_date')), "status": record.get('status'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/lease-charges":
                mapped_mirror_record.update({
                    "lease_id": record.get('lease_id'), "amount": record.get('amount'), "description": record.get('description'), "due_date": _convert_date_to_iso(record.get('due_date')), "status": record.get('status'), "category": record.get('category'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/lease-credits":
                mapped_mirror_record.update({
                    "lease_id": record.get('lease_id'), "amount": record.get('amount'), "description": record.get('description'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/vendors":
                mapped_mirror_record.update({
                    "name": record.get('name'), "display_name": record.get('displayName'), "phone": record.get('phone'), "email": record.get('email'), "trade": record.get('trade'), "active": record.get('active'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/tasks":
                mapped_mirror_record.update({
                    "property_id": record.get('property'), "unit_id": record.get('unit'), "tenant_id": record.get('tenant'), "assigned_to_user_id": record.get('assignedToUser'), "assigned_to_vendor_id": record.get('assignedToVendor'),
                    "type": record.get('type'), "subject": record.get('subject'), "description": record.get('description'), "due_date": _convert_date_to_iso(record.get('dueDate')), "status": record.get('status'), "priority": record.get('priority'), "estimated_cost": record.get('estimatedCost'), "actual_cost": record.get('actualCost'),
                    "is_program_inspection": record.get('isProgramInspection'), "program_id": record.get('programId'), "original_inspection_date": _convert_date_to_iso(record.get('originalInspectionDate')),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "work_order_json": _safe_json_dumps(record.get('workOrder'))
                })
            elif endpoint == "/files":
                mapped_mirror_record.update({
                    "name": record.get('name'), "mime_type": record.get('mimeType'), "size": record.get('size'), "download_url": record.get('downloadUrl'), "notes": record.get('notes'), "created_by": record.get('createdBy'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/notes":
                mapped_mirror_record.update({
                    "resource_id": record.get('resourceId'), "resource_type": record.get('resourceType'), "user_id": record.get('userId'), "body": record.get('body'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/communications":
                mapped_mirror_record.update({
                    "type": record.get('type'), "subject": record.get('subject'), "body": record.get('body'), "sender": record.get('sender'), "recipient": record.get('recipient'), "status": record.get('status'), "resource_id": record.get('resourceId'), "resource_type": record.get('resourceType'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "attachments_json": _safe_json_dumps(record.get('attachments')), "metadata_json": _safe_json_dumps(record.get('metadata'))
                })
            elif endpoint == "/applications":
                mapped_mirror_record.update({
                    "status": record.get('status'), "property_id": record.get('propertyId'), "unit_id": record.get('unitId'), "lease_id": record.get('leaseId'), "applicant_name": record.get('applicantName'), "submission_date": _convert_date_to_iso(record.get('submissionDate')), "decision_date": _convert_date_to_iso(record.get('decisionDate')), "move_in_date": _convert_date_to_iso(record.get('moveInDate')), "source": record.get('source'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields')), "applicants_json": _safe_json_dumps(record.get('applicants')), "screening_json": _safe_json_dumps(record.get('screening')), "fees_json": _safe_json_dumps(record.get('fees'))
                })
            elif endpoint == "/inspections":
                mapped_mirror_record.update({
                    "property_id": record.get('propertyId'), "unit_id": record.get('unitId'), "lease_id": record.get('leaseId'), "inspector_user_id": record.get('inspectorUserId'), "inspection_type": record.get('inspectionType'), "scheduled_date": _convert_date_to_iso(record.get('scheduledDate')), "actual_date": _convert_date_to_iso(record.get('actualDate')), "status": record.get('status'), "overall_condition": record.get('overallCondition'), "notes": record.get('notes'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "findings_json": _safe_json_dumps(record.get('findings')), "photos_json": _safe_json_dumps(record.get('photos'))
                })
            elif endpoint == "/insurance-policies":
                mapped_mirror_record.update({
                    "entity_type": record.get('entityType'), "entity_id": record.get('entityId'), "policy_number": record.get('policyNumber'), "provider": record.get('provider'), "coverage_amount": record.get('coverageAmount'), "expiration_date": _convert_date_to_iso(record.get('expirationDate')),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/recurring-charges":
                mapped_mirror_record.update({
                    "lease_id": record.get('leaseId'), "account_id": record.get('accountId'), "type": record.get('type'), "description": record.get('description'), "amount": record.get('amount'), "start_date": _convert_date_to_iso(record.get('startDate')), "end_date": _convert_date_to_iso(record.get('endDate')), "frequency": record.get('frequency'), "next_charge_date": _convert_date_to_iso(record.get('nextChargeDate')), "status": record.get('status'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields'))
                })
            elif endpoint == "/recurring-credits":
                mapped_mirror_record.update({
                    "lease_id": record.get('leaseId'), "account_id": record.get('accountId'), "type": record.get('type'), "description": record.get('description'), "amount": record.get('amount'), "start_date": _convert_date_to_iso(record.get('startDate')), "end_date": _convert_date_to_iso(record.get('endDate')), "frequency": record.get('frequency'), "next_credit_date": _convert_date_to_iso(record.get('nextCreditDate')), "status": record.get('status'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields'))
                })
            elif endpoint == "/accounts":
                mapped_mirror_record.update({
                    "name": record.get('name'), "type": record.get('type'), "sub_type": record.get('subType'), "class": record.get('class'), "number": record.get('number'), "is_system_account": record.get('isSystemAccount'), "active": record.get('active'), "cash_account": record.get('cashAccount'), "bank_account": record.get('bankAccount'), "parent_account_json": _safe_json_dumps(record.get('parentAccount')),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields'))
                })
            elif endpoint == "/users":
                mapped_mirror_record.update({
                    "first_name": record.get('firstName'), "last_name": record.get('lastName'), "email": record.get('email'), "role": record.get('role'), "active": record.get('active'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields'))
                })
            elif endpoint == "/portfolios":
                mapped_mirror_record.update({
                    "name": record.get('name'), "description": record.get('description'), "active": record.get('active'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields'))
                })
            elif endpoint == "/reports":
                mapped_mirror_record.update({
                    "name": record.get('name'), "type": record.get('type'), "status": record.get('status'), "download_url": record.get('downloadUrl'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "parameters_json": _safe_json_dumps(record.get('parameters'))
                })
            elif endpoint == "/activity-logs":
                mapped_mirror_record.update({
                    "type": record.get('type'), "resource_id": record.get('resourceId'), "resource_type": record.get('resourceType'), "user_id": record.get('userId'), "action": record.get('action'), "description": record.get('description'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            # Add more elif blocks here for other endpoints as needed following the same pattern

            prepared_mirror_table_data.append(mapped_mirror_record)

        except Exception as e:
            # Catch errors during single record processing to prevent the whole batch from failing
            _logger.error(f"ERROR_RECORD_PROCESS: Failed to process record {i+1} (ID: {record.get('id')}) for {endpoint}: {type(e).__name__}: {str(e)}. Record: {record}")
            continue # Continue to the next record, skipping the problematic one

    # === 2. Send prepared data to Supabase ===

    # Send to raw_doorloop_data table
    if prepared_raw_doorloop_data:
        _logger.info(f"DEBUG_SUPABASE_CLIENT: Upserting {len(prepared_raw_doorloop_data)} records to raw_doorloop_data URL: {raw_doorloop_data_table_url}")
        try:
            raw_response = requests.post(raw_doorloop_data_table_url, headers=headers, json=prepared_raw_doorloop_data, timeout=60)
            raw_response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            _logger.info(f"DEBUG_SUPABASE_CLIENT: Raw data upsert successful for {endpoint}.")
        except requests.exceptions.RequestException as e:
            error_message = f"ERROR_RAW_UPSERT: Failed to upsert raw data for {endpoint}: {e.response.status_code if e.response else ''} - {e.response.text if e.response else str(e)}"
            _logger.error(error_message)
            raise # Re-raise the exception to indicate a critical failure

    # Send to entity-specific mirror table
    
    if prepared_raw_doorloop_data:
        try:
            raw_response = requests.post(
                raw_doorloop_data_table_url,
                headers=headers,
                json=prepared_raw_doorloop_data,
                timeout=60
            )
            raw_response.raise_for_status()
            print(f"DEBUG_SUPABASE_CLIENT: raw_doorloop_data upsert successful ({len(prepared_raw_doorloop_data)} records).")
        except requests.exceptions.RequestException as e:
            print(f"ERROR_RAW_UPSERT: Failed to upsert raw data for {endpoint}: {e.response.status_code if e.response else ''} - {e.response.text if e.response else str(e)}")
            raise
        _logger.info(f"DEBUG_SUPABASE_CLIENT: Upserting {len(prepared_mirror_table_data)} records to {table_name_raw} URL: {mirror_table_url}")
    if prepared_mirror_table_data:
        _logger.info(f"DEBUG_SUPABASE_CLIENT: Upserting {len(prepared_mirror_table_data)} records to {table_name_raw} URL: {mirror_table_url}")
        try:
            mirror_response = requests.post(
                mirror_table_url,
                headers=headers,
                json=prepared_mirror_table_data,
                timeout=60
            )
            mirror_response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        except requests.exceptions.RequestException as e:
            _logger.error(f"ERROR_MIRROR_UPSERT: Failed to upsert to {table_name_raw}: {e.response.status_code if e.response else ''} - {e.response.text if e.response else str(e)}")
            raise
        _logger.info(f"DEBUG_SUPABASE_CLIENT: Completed raw ingestion for {endpoint}.")
    types by converting them to strings and logs a warning.
    Returns None if the input data is None.
    """
    if data is None:
        return None
    try:
        return json.dumps(data, default=str)
    except TypeError as e:
        _logger.warning(f"WARN: Failed to dump data to JSON for JSONB storage: {type(data)} - {data}. Error: {e}")
        return str(data)

def _convert_date_to_iso(date_str: Optional[str]) -> Optional[str]:
    """
    Converts various date string formats to ISO 8601 format (YYYY-MM-DDTHH:MM:SS).
    Handles 'Z' for UTC, timezone offsets, and milliseconds.
    Returns the original string if parsing fails, logging a warning.
    Returns None if the input date_str is None or empty.
    """
    if not date_str:
        return None
    if isinstance(date_str, datetime):
        return date_str.isoformat(timespec='seconds')
    try:
        # Try parsing with milliseconds and 'Z' (UTC)
        if date_str.endswith('Z'):
            try:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            except ValueError:
                # Fallback for no milliseconds but with 'Z'
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
        elif '+' in date_str:
            # Handle timezone offset with potential milliseconds
            try:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
            except ValueError:
                # Fallback for no milliseconds but with timezone offset
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')
        elif 'T' in date_str:
            # Handle local time with potential milliseconds
            try:
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f')
            except ValueError:
                # Fallback for no milliseconds and local time
                dt_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
        else:
            # Assume YYYY-MM-DD format
            dt_obj = datetime.strptime(date_str, '%Y-%m-%d')

        return dt_obj.isoformat(timespec='seconds')
    except ValueError:
        _logger.warning(f"WARN: Could not parse date string '{date_str}'. Returning original string.")
        return date_str

def upsert_raw_doorloop_data(
    endpoint: str,
    records: List[Dict[str, Any]],
    supabase_url: str,
    supabase_service_role_key: str
):
    """
    Upserts raw DoorLoop data into a generic 'raw_doorloop_data' table
    and also into entity-specific mirror tables (e.g., 'doorloop_raw_properties').

    Args:
        endpoint (str): The DoorLoop API endpoint (e.g., "/properties").
        records (List[Dict[str, Any]]): A list of dictionaries, where each dict
                                         is a record from the DoorLoop API.
        supabase_url (str): The base URL for your Supabase project.
        supabase_service_role_key (str): The Supabase service role key with
                                         permissions to insert/upsert data.
    Raises:
        ValueError: If Supabase URL, service role key, or endpoint is missing.
    """
        requests.exceptions.RequestException: If there's an HTTP error during
                                              the API call to Supabase.
    """
    """
    if not supabase_service_role_key:
        raise ValueError("Supabase service role key is missing.")
    if not supabase_url:
        raise ValueError("Supabase URL is missing.")
    if not endpoint:
        raise ValueError("Endpoint is missing for raw data upsert.")
    if not isinstance(records, list):
        raise TypeError(f"Expected records to be a list, got {type(records)}")

    headers = {
        "apikey": supabase_service_role_key,
        "Authorization": f"Bearer {supabase_service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal" # Get minimal response body on success
    }

    base_rest_url = f"{supabase_url}/rest/v1"

    # URL for the generic raw_doorloop_data table
    raw_doorloop_data_table_url = f"{base_rest_url}/raw_doorloop_data"
    # Derive table name for the mirror table (e.g., doorloop_raw_properties)
    table_name_raw = f"doorloop_raw_{endpoint.strip('/').replace('-', '_')}"
    mirror_table_url = f"{base_rest_url}/{table_name_raw}"

    _logger.info(f"DEBUG_SUPABASE_CLIENT: Starting raw upsert for {endpoint} ({len(records)} records)")

    # This list holds records for the generic 'raw_doorloop_data' table
    prepared_raw_doorloop_data = []
    # This list holds records mapped for specific 'doorloop_raw_<entity>' table
    prepared_mirror_table_data = []

    for i, record in enumerate(records):
        try:
            entity_dl_id = record.get('id', record.get('ID')) # Try 'id' then 'ID'
            if not entity_dl_id:
                _logger.warning(f"WARN_RECORD_PROCESS: Record {i+1} from {endpoint} has no identifiable ID. Skipping: {record}")
                continue # Skip records without an ID

            # Generate a SHA256 hash of the JSON payload for content-based deduplication/integrity check
            payload_hash = hashlib.sha256(_safe_json_dumps(record).encode('utf-8')).hexdigest()

            # Prepare data for the generic raw_doorloop_data table
            prepared_raw_doorloop_data.append({
                "endpoint": endpoint,
                "entity_dl_id": str(entity_dl_id),
                "payload_json": record, # Store the full raw payload as JSONB
                "payload_hash": payload_hash
            })

            # --- Prepare for specific doorloop_raw_<entity> table (MIRROR) ---
            # All mirror tables must have 'doorloop_id' and '_raw_payload'
            mapped_mirror_record = {
                "doorloop_id": str(entity_dl_id),
                "_raw_payload": record # Store raw payload in mirror table too for completeness
            }

            # --- START ENTITY-SPECIFIC MAPPING FOR doorloop_raw_* TABLES ---
            # These mappings must match your schema_v3.5_ultimate_full_rebuild_final_syntax_fix.sql
            # Add elif blocks for ALL DoorLoop endpoints you wish to mirror.

            if endpoint == "/properties":
                mapped_mirror_record.update({
                    "name": record.get('name'), "type": record.get('type'), "class": record.get('class'), "active": record.get('active'),
                    "address_street1": record.get('address',{}).get('street1'), "address_street2": record.get('address',{}).get('street2'),
                    "address_city": record.get('address',{}).get('city'), "address_state": record.get('address',{}).get('state'),
                    "address_zip": record.get('address',{}).get('zip'), "address_country": record.get('address',{}).get('country'),
                    "address_lat": record.get('address',{}).get('lat'), "address_lng": record.get('address',{}).get('lng'),
                    "description": record.get('description'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')),
                    "external_id": record.get('externalId'), "manager_id": record.get('managerId'),
                    "purchase_date": _convert_date_to_iso(record.get('purchaseDate')), "purchase_price": record.get('purchasePrice'),
                    "current_value": record.get('currentValue'), "bedroom_count": record.get('bedroomCount'),
                    "owners_json": _safe_json_dumps(record.get('owners')), "pictures_json": _safe_json_dumps(record.get('pictures')),
                    "amenities_json": _safe_json_dumps(record.get('amenities')),
                    "custom_fields_json": _safe_json_dumps(record.get('settings', {}).get('customFields')),
                    "board_members_json": _safe_json_dumps(record.get('boardMembers')),
                    "insurance_json": _safe_json_dumps(record.get('insurance')),
                    "tax_info_json": _safe_json_dumps(record.get('taxInfo')),
                    "financials_json": _safe_json_dumps(record.get('financials')),
                    "compliance_json": _safe_json_dumps(record.get('compliance'))
                })
            elif endpoint == "/units":
                mapped_mirror_record.update({
                    "property_id": record.get('property_id'), "unit_number": record.get('name'),
                    "beds": float(record.get('beds')) if record.get('beds') is not None else None,
                    "baths": float(record.get('baths')) if record.get('baths') is not None else None,
                    "sq_ft": float(record.get('size')) if record.get('size') is not None else None,
                    "status": record.get('status'), "active": record.get('active'), "rent_amount": float(record.get('marketRent')) if record.get('marketRent') is not None else None,
                    "description": record.get('description'), "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')),
                    "floor_plan": record.get('floorPlan'), "is_rentable": record.get('isRentable'), "last_renovated": _convert_date_to_iso(record.get('lastRenovated')), "unit_condition": record.get('condition'),
                    "address_json": _safe_json_dumps(record.get('address')), "listing_json": _safe_json_dumps(record.get('rentalApplicationListing')),
                    "amenities_json": _safe_json_dumps(record.get('amenities')), "pictures_json": _safe_json_dumps(record.get('photos')),
                    "features_json": _safe_json_dumps(record.get('features')), "utilities_json": _safe_json_dumps(record.get('utilities'))
                })
            elif endpoint == "/tenants":
                mapped_mirror_record.update({
                    "first_name": record.get('firstName'), "last_name": record.get('lastName'), "full_name": record.get('fullName'), "display_name": record.get('name'),
                    "date_of_birth": _convert_date_to_iso(record.get('dateOfBirth')), "timezone": record.get('timezone'), "company_name": record.get('companyName'), "job_title": record.get('jobTitle'), "notes": record.get('notes'),
                    "primary_email": next((e.get('address') for e in record.get('emails', []) if e.get('type') == 'Primary'), None),
                    "primary_phone": next((p.get('number') for p in record.get('phones', []) if p.get('type') in ['Mobile', 'Primary']), None),
                    "ssn": record.get('ssn'), "credit_score": record.get('creditScore'), "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')),
                    "primary_address_json": _safe_json_dumps(record.get('primaryAddress')), "emails_json": _safe_json_dumps(record.get('emails')), "phones_json": _safe_json_dumps(record.get('phones')),
                    "dependants_json": _safe_json_dumps(record.get('dependants')), "pets_json": _safe_json_dumps(record.get('pets')), "vehicles_json": _safe_json_dumps(record.get('vehicles')),
                    "emergency_contacts_json": _safe_json_dumps(record.get('emergencyContacts')), "prospect_info_json": _safe_json_dumps(record.get('prospectInfo')),
                    "portal_info_json": _safe_json_dumps(record.get('portalInfo')), "employment_info_json": _safe_json_dumps(record.get('employmentInfo')),
                    "background_check_json": _safe_json_dumps(record.get('backgroundCheck')), "references_json": _safe_json_dumps(record.get('references')),
                    "banking_info_json": _safe_json_dumps(record.get('bankingInfo'))
                })
            elif endpoint == "/owners":
                mapped_mirror_record.update({
                    "first_name": record.get('firstName'), "last_name": record.get('lastName'), "full_name": record.get('fullName'), "display_name": record.get('name'),
                    "date_of_birth": _convert_date_to_iso(record.get('dateOfBirth')), "timezone": record.get('timezone'), "company_name": record.get('companyName'), "job_title": record.get('jobTitle'), "notes": record.get('notes'),
                    "primary_email": next((e.get('address') for e in record.get('emails', []) if e.get('type') == 'Primary'), None),
                    "primary_phone": next((p.get('number') for p in record.get('phones', []) if p.get('type') in ['Mobile', 'Primary']), None),
                    "active": record.get('active'), "management_start_date": _convert_date_to_iso(record.get('managementStartDate')), "management_end_date": _convert_date_to_iso(record.get('managementEndDate')),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')),
                    "primary_address_json": _safe_json_dumps(record.get('primaryAddress')), "emails_json": _safe_json_dumps(record.get('emails')), "phones_json": _safe_json_dumps(record.get('phones')),
                    "federal_tax_info_json": _safe_json_dumps(record.get('federalTaxInfo')), "banking_info_json": _safe_json_dumps(record.get('bankingInfo'))
                })
            elif endpoint == "/leases":
                mapped_mirror_record.update({
                    "property_id": record.get('property'), "units_json": _safe_json_dumps(record.get('units')), "tenants_json": _safe_json_dumps(record.get('tenants')), "name": record.get('name'),
                    "start_date": _convert_date_to_iso(record.get('start')), "end_date": _convert_date_to_iso(record.get('end')), "term": record.get('term'), "status": record.get('status'), "eviction_pending": record.get('evictionPending'), "rollover_to_at_will": record.get('rolloverToAtWill'), "proof_of_insurance_required": record.get('proofOfInsuranceRequired'),
                    "total_balance_due": record.get('totalBalancedue'), "total_deposits_held": record.get('totalDepositsHeld'), "total_recurring_rent": record.get('totalRecurringRent'), "total_recurring_payments": record.get('totalRecurringPayments'), "total_recurring_credits": record.get('totalRecurringCredits'), "total_recurring_charges": record.get('TotalRecurringCharges'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')),
                    "reference_json": _safe_json_dumps(record.get('reference')), "notes_json": _safe_json_dumps(record.get('notes')), "settings_json": _safe_json_dumps(record.get('settings')), "renewal_info_json": _safe_json_dumps(record.get('renewalInfo')),
                    "custom_fields_json": _safe_json_dumps(record.get('customFields')), "pet_policy_json": _safe_json_dumps(record.get('petPolicy')), "addendums_json": _safe_json_dumps(record.get('addendums')),
                    "cosigners_json": _safe_json_dumps(record.get('cosigners')), "utilities_json": _safe_json_dumps(record.get('utilities')), "insurance_status_json": _safe_json_dumps(record.get('proofOfInsuranceStatus'))
                })
            elif endpoint == "/lease-payments":
                mapped_mirror_record.update({
                    "lease_id": record.get('lease_id'), "amount": record.get('amountReceived'), "payment_date": _convert_date_to_iso(record.get('payment_date')), "status": record.get('status'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/lease-charges":
                mapped_mirror_record.update({
                    "lease_id": record.get('lease_id'), "amount": record.get('amount'), "description": record.get('description'), "due_date": _convert_date_to_iso(record.get('due_date')), "status": record.get('status'), "category": record.get('category'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/lease-credits":
                mapped_mirror_record.update({
                    "lease_id": record.get('lease_id'), "amount": record.get('amount'), "description": record.get('description'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/vendors":
                mapped_mirror_record.update({
                    "name": record.get('name'), "display_name": record.get('displayName'), "phone": record.get('phone'), "email": record.get('email'), "trade": record.get('trade'), "active": record.get('active'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/tasks":
                mapped_mirror_record.update({
                    "property_id": record.get('property'), "unit_id": record.get('unit'), "tenant_id": record.get('tenant'), "assigned_to_user_id": record.get('assignedToUser'), "assigned_to_vendor_id": record.get('assignedToVendor'),
                    "type": record.get('type'), "subject": record.get('subject'), "description": record.get('description'), "due_date": _convert_date_to_iso(record.get('dueDate')), "status": record.get('status'), "priority": record.get('priority'), "estimated_cost": record.get('estimatedCost'), "actual_cost": record.get('actualCost'),
                    "is_program_inspection": record.get('isProgramInspection'), "program_id": record.get('programId'), "original_inspection_date": _convert_date_to_iso(record.get('originalInspectionDate')),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "work_order_json": _safe_json_dumps(record.get('workOrder'))
                })
            elif endpoint == "/files":
                mapped_mirror_record.update({
                    "name": record.get('name'), "mime_type": record.get('mimeType'), "size": record.get('size'), "download_url": record.get('downloadUrl'), "notes": record.get('notes'), "created_by": record.get('createdBy'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/notes":
                mapped_mirror_record.update({
                    "resource_id": record.get('resourceId'), "resource_type": record.get('resourceType'), "user_id": record.get('userId'), "body": record.get('body'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/communications":
                mapped_mirror_record.update({
                    "type": record.get('type'), "subject": record.get('subject'), "body": record.get('body'), "sender": record.get('sender'), "recipient": record.get('recipient'), "status": record.get('status'), "resource_id": record.get('resourceId'), "resource_type": record.get('resourceType'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "attachments_json": _safe_json_dumps(record.get('attachments')), "metadata_json": _safe_json_dumps(record.get('metadata'))
                })
            elif endpoint == "/applications":
                mapped_mirror_record.update({
                    "status": record.get('status'), "property_id": record.get('propertyId'), "unit_id": record.get('unitId'), "lease_id": record.get('leaseId'), "applicant_name": record.get('applicantName'), "submission_date": _convert_date_to_iso(record.get('submissionDate')), "decision_date": _convert_date_to_iso(record.get('decisionDate')), "move_in_date": _convert_date_to_iso(record.get('moveInDate')), "source": record.get('source'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields')), "applicants_json": _safe_json_dumps(record.get('applicants')), "screening_json": _safe_json_dumps(record.get('screening')), "fees_json": _safe_json_dumps(record.get('fees'))
                })
            elif endpoint == "/inspections":
                mapped_mirror_record.update({
                    "property_id": record.get('propertyId'), "unit_id": record.get('unitId'), "lease_id": record.get('leaseId'), "inspector_user_id": record.get('inspectorUserId'), "inspection_type": record.get('inspectionType'), "scheduled_date": _convert_date_to_iso(record.get('scheduledDate')), "actual_date": _convert_date_to_iso(record.get('actualDate')), "status": record.get('status'), "overall_condition": record.get('overallCondition'), "notes": record.get('notes'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "findings_json": _safe_json_dumps(record.get('findings')), "photos_json": _safe_json_dumps(record.get('photos'))
                })
            elif endpoint == "/insurance-policies":
                mapped_mirror_record.update({
                    "entity_type": record.get('entityType'), "entity_id": record.get('entityId'), "policy_number": record.get('policyNumber'), "provider": record.get('provider'), "coverage_amount": record.get('coverageAmount'), "expiration_date": _convert_date_to_iso(record.get('expirationDate')),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            elif endpoint == "/recurring-charges":
                mapped_mirror_record.update({
                    "lease_id": record.get('leaseId'), "account_id": record.get('accountId'), "type": record.get('type'), "description": record.get('description'), "amount": record.get('amount'), "start_date": _convert_date_to_iso(record.get('startDate')), "end_date": _convert_date_to_iso(record.get('endDate')), "frequency": record.get('frequency'), "next_charge_date": _convert_date_to_iso(record.get('nextChargeDate')), "status": record.get('status'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields'))
                })
            elif endpoint == "/recurring-credits":
                mapped_mirror_record.update({
                    "lease_id": record.get('leaseId'), "account_id": record.get('accountId'), "type": record.get('type'), "description": record.get('description'), "amount": record.get('amount'), "start_date": _convert_date_to_iso(record.get('startDate')), "end_date": _convert_date_to_iso(record.get('endDate')), "frequency": record.get('frequency'), "next_credit_date": _convert_date_to_iso(record.get('nextCreditDate')), "status": record.get('status'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields'))
                })
            elif endpoint == "/accounts":
                mapped_mirror_record.update({
                    "name": record.get('name'), "type": record.get('type'), "sub_type": record.get('subType'), "class": record.get('class'), "number": record.get('number'), "is_system_account": record.get('isSystemAccount'), "active": record.get('active'), "cash_account": record.get('cashAccount'), "bank_account": record.get('bankAccount'), "parent_account_json": _safe_json_dumps(record.get('parentAccount')),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields'))
                })
            elif endpoint == "/users":
                mapped_mirror_record.update({
                    "first_name": record.get('firstName'), "last_name": record.get('lastName'), "email": record.get('email'), "role": record.get('role'), "active": record.get('active'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields'))
                })
            elif endpoint == "/portfolios":
                mapped_mirror_record.update({
                    "name": record.get('name'), "description": record.get('description'), "active": record.get('active'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "custom_fields_json": _safe_json_dumps(record.get('customFields'))
                })
            elif endpoint == "/reports":
                mapped_mirror_record.update({
                    "name": record.get('name'), "type": record.get('type'), "status": record.get('status'), "download_url": record.get('downloadUrl'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt')), "parameters_json": _safe_json_dumps(record.get('parameters'))
                })
            elif endpoint == "/activity-logs":
                mapped_mirror_record.update({
                    "type": record.get('type'), "resource_id": record.get('resourceId'), "resource_type": record.get('resourceType'), "user_id": record.get('userId'), "action": record.get('action'), "description": record.get('description'),
                    "created_at": _convert_date_to_iso(record.get('createdAt')), "updated_at": _convert_date_to_iso(record.get('updatedAt'))
                })
            # Add more elif blocks here for other endpoints as needed following the same pattern

            prepared_mirror_table_data.append(mapped_mirror_record)

        except Exception as e:
            # Catch errors during single record processing to prevent the whole batch from failing
            _logger.error(f"ERROR_RECORD_PROCESS: Failed to process record {i+1} (ID: {record.get('id')}) for {endpoint}: {type(e).__name__}: {str(e)}. Record: {record}")
            continue # Continue to the next record, skipping the problematic one

    # === 2. Send prepared data to Supabase ===

    # Send to raw_doorloop_data table
    if prepared_raw_doorloop_data:
        _logger.info(f"DEBUG_SUPABASE_CLIENT: Upserting {len(prepared_raw_doorloop_data)} records to raw_doorloop_data URL: {raw_doorloop_data_table_url}")
        try:
            raw_response = requests.post(raw_doorloop_data_table_url, headers=headers, json=prepared_raw_doorloop_data, timeout=60)
            raw_response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            _logger.info(f"DEBUG_SUPABASE_CLIENT: Raw data upsert successful for {endpoint}.")
        except requests.exceptions.RequestException as e:
            error_message = f"ERROR_RAW_UPSERT: Failed to upsert raw data for {endpoint}: {e.response.status_code if e.response else ''} - {e.response.text if e.response else str(e)}"
            _logger.error(error_message)
            raise # Re-raise the exception to indicate a critical failure

    # Send to entity-specific mirror table
    
    if prepared_raw_doorloop_data:
        try:
            raw_response = requests.post(
                raw_doorloop_data_table_url,
                headers=headers,
                json=prepared_raw_doorloop_data,
                timeout=60
            )
            raw_response.raise_for_status()
            print(f"DEBUG_SUPABASE_CLIENT: raw_doorloop_data upsert successful ({len(prepared_raw_doorloop_data)} records).")
        except requests.exceptions.RequestException as e:
            print(f"ERROR_RAW_UPSERT: Failed to upsert raw data for {endpoint}: {e.response.status_code if e.response else ''} - {e.response.text if e.response else str(e)}")
            raise
        _logger.info(f"DEBUG_SUPABASE_CLIENT: Upserting {len(prepared_mirror_table_data)} records to {table_name_raw} URL: {mirror_table_url}")
        try:
            mirror_response = requests.post(
                mirror_table_url,
                headers=headers,
                json=prepared_mirror_table_data,
                timeout=60
            )
            mirror_response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            _logger.info(f"DEBUG_SUPABASE_CLIENT: Mirror table {table_name_raw} upsert successful.")
        except requests.exceptions.RequestException as e:
            error_message = f"ERROR_MIRROR_UPSERT: Failed to upsert to {table_name_raw}: {e.response.status_code if e.response else ''} - {e.response.text if e.response else str(e)}"
            _logger.error(error_message)
            raise # Re-raise the exception to indicate a critical failure

        _logger.info(f"DEBUG_SUPABASE_CLIENT: Completed raw ingestion for {endpoint}.")
        _logger.info(f"DEBUG_SUPABASE_CLIENT: Completed raw ingestion for {endpoint}.")
        _logger.info(f"DEBUG_SUPABASE_CLIENT: Completed raw ingestion for {endpoint}.")