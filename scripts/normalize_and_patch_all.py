import uuid
import datetime
import logging
from sync_pipeline.normalize_properties import normalize_properties_logic # Renamed to avoid confusion
from sync_pipeline.normalize_units import normalize_units_logic
from sync_pipeline.normalize_leases import normalize_leases_logic
from sync_pipeline.normalize_tenants import normalize_tenants_logic
from sync_pipeline.normalize_owners import normalize_owners_logic
from supabase_client import log_pipeline_event_to_audit_db, handle_dead_letter_records # Import audit utilities

# Basic logging setup for this script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_entity_type(
    entity_type: str,
    normalization_function, # Function that handles fetching and transforming
    upsert_function,        # Function that handles upserting to Supabase
    batch_id: uuid.UUID
):
    """Generic function to process a single entity type."""
    logging.info(f"--- Processing {entity_type.capitalize()} ---")
    start_time = datetime.datetime.now(datetime.timezone.utc)
    total_records_processed = 0
    total_records_failed_upsert = 0

    log_pipeline_event_to_audit_db({
        "entity_type": entity_type,
        "pipeline_stage": f"{entity_type}_processing_start",
        "status": "in_progress",
        "batch_id": batch_id,
        "event_timestamp": start_time
    })

    try:
        # Step 1: Fetch raw data (e.g., from DoorLoop API or Supabase raw table)
        # This function would be specific to each entity type or generic if fetching from raw table
        raw_data = normalization_function.fetch_raw_data() # Assuming fetch_raw_data is part of the logic

        # Step 2: Normalize and Validate
        normalized_data = []
        validation_errors = []
        for record in raw_data:
            try:
                # Assuming normalization_function has a method for single record
                normalized_record = normalization_function.normalize_record(record)
                # Add pre-upsert schema validation here
                # if not validate_schema(normalized_record, ENTITY_SCHEMA[entity_type]):
                #    raise ValueError("Schema validation failed")
                normalized_data.append(normalized_record)
            except Exception as e:
                logging.error(f"Normalization failed for {entity_type} record: {record.get('doorloop_id', 'N/A')}. Error: {e}", exc_info=True)
                handle_dead_letter_records(
                    table_name=entity_type,
                    records=[record], # Send the raw record to DLQ
                    error_message=f"Normalization error: {e}",
                    error_code="NORMALIZATION_FAILED",
                    pipeline_stage=f"{entity_type}_normalization",
                    batch_id=batch_id
                )
                validation_errors.append(e) # Collect errors

        # Step 3: Auto-chunking and Upsert
        CHUNK_SIZE = 100 # Define per-entity if needed, or globally
        chunks_processed = 0
        for chunk in chunk_list(normalized_data, CHUNK_SIZE): # Assuming chunk_list is imported
            try:
                upsert_function(table_name=f"doorloop_normalized_{entity_type}", data=chunk, batch_id=batch_id)
                chunks_processed += 1
                total_records_processed += len(chunk)
            except Exception as e:
                logging.error(f"Upsert failed for {entity_type} chunk. Error: {e}", exc_info=True)
                handle_dead_letter_records(
                    table_name=f"doorloop_normalized_{entity_type}",
                    records=chunk, # Send the normalized records that failed to DLQ
                    error_message=f"Upsert error: {e}",
                    error_code="UPSERT_FAILED",
                    pipeline_stage=f"{entity_type}_upsert",
                    batch_id=batch_id
                )
                total_records_failed_upsert += len(chunk)
                # CRITICAL: Do NOT re-raise here. Continue processing other chunks/entities.

        status = "success"
        error_message = None
        error_code = None
        if validation_errors or total_records_failed_upsert > 0:
            status = "failed"
            error_code = "PARTIAL_SUCCESS_OR_FAILURE"
            error_message = f"{len(validation_errors)} records failed normalization, {total_records_failed_upsert} records failed upsert."

        end_time = datetime.datetime.now(datetime.timezone.utc)
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        log_pipeline_event_to_audit_db({
            "entity_type": entity_type,
            "pipeline_stage": f"{entity_type}_processing_complete",
            "status": status,
            "record_count": total_records_processed,
            "error_code": error_code,
            "error_message": error_message,
            "duration_ms": duration_ms,
            "batch_id": batch_id,
            "event_timestamp": end_time
        })

    except Exception as e: # Catch any unhandled errors in this entity's overall process
        logging.critical(f"Unhandled critical error during {entity_type} processing: {e}", exc_info=True)
        log_pipeline_event_to_audit_db({
            "entity_type": entity_type,
            "pipeline_stage": f"{entity_type}_processing_critical_failure",
            "status": "failed",
            "error_code": "CRITICAL_UNHANDLED_ERROR",
            "error_message": f"Critical unhandled error for {entity_type}: {e}",
            "event_timestamp": datetime.datetime.now(datetime.timezone.utc),
            "batch_id": batch_id
        })
        # Optionally re-raise if a critical failure for one entity should stop the entire pipeline
        # For full graceful failure, you might just log and move on.
        # For an Empire-grade system, a critical error *might* stop the full pipeline to prevent cascading issues.
        raise

# Example structure for normalize_properties_logic in sync_pipeline/normalize_properties.py
# class NormalizePropertiesLogic:
#     @staticmethod
#     def fetch_raw_data():
#         # Use supabase_client.fetch_raw_data('doorloop_raw_properties')
#         pass
#     @staticmethod
#     def normalize_record(raw_record):
#         # Transformation logic
#         pass


def main():
    current_batch_id = uuid.uuid4()
    logging.info(f"🚀 Full normalization pipeline started with Batch ID: {current_batch_id}")

    # Log pipeline start
    log_pipeline_event_to_audit_db({
        "entity_type": "pipeline_run",
        "pipeline_stage": "full_sync_start",
        "status": "in_progress",
        "batch_id": current_batch_id,
        "event_timestamp": datetime.datetime.now(datetime.timezone.utc)
    })

    pipeline_success = True

    # Process each entity type
    entities_to_process = {
        "properties": normalize_properties_logic, # Replace with actual logic classes/modules
        "units": normalize_units_logic,
        "leases": normalize_leases_logic,
        "tenants": normalize_tenants_logic,
        "owners": normalize_owners_logic,
        # "payments": normalize_payments_logic, # If payments also follow this flow
    }

    # Assuming you have corresponding upsert functions in supabase_client.py
    upsert_functions = {
        "properties": upsert_normalized_data, # Using generic upsert for normalized tables
        "units": upsert_normalized_data,
        "leases": upsert_normalized_data,
        "tenants": upsert_normalized_data,
        "owners": upsert_normalized_data,
        # "payments": upsert_normalized_data,
    }

    for entity_type, normalization_logic in entities_to_process.items():
        try:
            # Pass the specific upsert function for this entity
            process_entity_type(entity_type, normalization_logic, upsert_functions[entity_type], current_batch_id)
        except Exception as e:
            logging.error(f"Overall processing failed for entity {entity_type}: {e}", exc_info=True)
            pipeline_success = False
            # Do not re-raise; allow other entities to process

    # Log final pipeline status
    final_status = "success" if pipeline_success else "failed"
    log_pipeline_event_to_audit_db({
        "entity_type": "pipeline_run",
        "pipeline_stage": "full_sync_complete",
        "status": final_status,
        "batch_id": current_batch_id,
        "event_timestamp": datetime.datetime.now(datetime.timezone.utc),
        "error_message": "Pipeline completed with errors in some entities." if not pipeline_success else None,
        "error_code": "PARTIAL_SUCCESS" if not pipeline_success else None
    })
    logging.info(f"Pipeline completed with status: {final_status}")

    if not pipeline_success:
        # Exit with non-zero code if any part of the pipeline failed for CI signal
        sys.exit(1)


if __name__ == "__main__":
    main()