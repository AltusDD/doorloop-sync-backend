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
        raw_data = normalization_function.fetch_raw_data()
        normalized_data = []
        validation_errors = []
        for record in raw_data:
            try:
                normalized_record = normalization_function.normalize_record(record)
                normalized_data.append(normalized_record)
            except Exception as e:
                logging.error(f"Normalization failed for {entity_type} record: {record.get('doorloop_id', 'N/A')}. Error: {e}", exc_info=True)
                handle_dead_letter_records(
                    table_name=entity_type,
                    records=[record],
                    error_message=f"Normalization error: {e}",
                    error_code="NORMALIZATION_FAILED",
                    pipeline_stage=f"{entity_type}_normalization",
                    batch_id=batch_id
                )
                validation_errors.append(e)

        CHUNK_SIZE = 100
        chunks_processed = 0
        for i in range(0, len(normalized_data), CHUNK_SIZE):
            chunk = normalized_data[i:i+CHUNK_SIZE]
            try:
                upsert_function(table_name=f"doorloop_normalized_{entity_type}", data=chunk, batch_id=batch_id)
                chunks_processed += 1
                total_records_processed += len(chunk)
            except Exception as e:
                logging.error(f"Upsert failed for {entity_type} chunk. Error: {e}", exc_info=True)
                handle_dead_letter_records(
                    table_name=f"doorloop_normalized_{entity_type}",
                    records=chunk,
                    error_message=f"Upsert error: {e}",
                    error_code="UPSERT_FAILED",
                    pipeline_stage=f"{entity_type}_upsert",
                    batch_id=batch_id
                )
                total_records_failed_upsert += len(chunk)

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

    except Exception as e:
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
        raise

def main():
    current_batch_id = uuid.uuid4()
    logging.info(f"🚀 Full normalization pipeline started with Batch ID: {current_batch_id}")

    log_pipeline_event_to_audit_db({
        "entity_type": "pipeline_run",
        "pipeline_stage": "full_sync_start",
        "status": "in_progress",
        "batch_id": current_batch_id,
        "event_timestamp": datetime.datetime.now(datetime.timezone.utc)
    })

    pipeline_success = True
    entities_to_process = {
        "properties": normalize_properties_logic,
        "units": normalize_units_logic,
        "leases": normalize_leases_logic,
        "tenants": normalize_tenants_logic,
        "owners": normalize_owners_logic,
    }

    upsert_functions = {
        "properties": upsert_normalized_data,
        "units": upsert_normalized_data,
        "leases": upsert_normalized_data,
        "tenants": upsert_normalized_data,
        "owners": upsert_normalized_data,
    }

    for entity_type, normalization_logic in entities_to_process.items():
        try:
            process_entity_type(entity_type, normalization_logic, upsert_functions[entity_type], current_batch_id)
        except Exception as e:
            logging.error(f"Overall processing failed for entity {entity_type}: {e}", exc_info=True)
            pipeline_success = False

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
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
