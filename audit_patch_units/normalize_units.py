import os
import uuid
import json
import time
import traceback
from supabase import create_client, Client
from log_pipeline_event import log_pipeline_event  # assumes helper is in the same directory

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def normalize_units():
    batch_id = uuid.uuid4()
    start_time = time.perf_counter()

    log_pipeline_event(
        entity_type="unit",
        pipeline_stage="raw_ingestion",
        status="in_progress",
        batch_id=batch_id,
        metadata={"function": "normalize_units"}
    )

    try:
        raw_units = supabase.table("doorloop_raw_units").select("*").execute().data
        log_pipeline_event(
            entity_type="unit",
            pipeline_stage="raw_ingestion",
            status="success",
            record_count=len(raw_units),
            batch_id=batch_id,
            duration_ms=int((time.perf_counter() - start_time) * 1000)
        )
    except Exception as e:
        log_pipeline_event(
            entity_type="unit",
            pipeline_stage="raw_ingestion",
            status="failed",
            error_code="FETCH_ERROR",
            error_message=str(e),
            error_details={"traceback": traceback.format_exc()},
            batch_id=batch_id
        )
        return

    start_norm_time = time.perf_counter()
    success_count = 0
    for unit in raw_units:
        try:
            if not unit.get("id"):
                raise ValueError("Missing 'id' field")

            norm_unit = {
                "id": unit["id"],
                "doorloop_property_id": unit.get("propertyId"),
                "name": unit.get("name"),
                "bedrooms": unit.get("bedrooms"),
                "bathrooms": unit.get("bathrooms"),
                "squareFeet": unit.get("squareFeet"),
                "marketRent": unit.get("marketRent"),
                "status": unit.get("status"),
                "metadata": unit,
            }

            supabase.table("doorloop_normalized_units").upsert(norm_unit).execute()
            success_count += 1

            log_pipeline_event(
                entity_type="unit",
                doorloop_id=unit["id"],
                pipeline_stage="normalization_units",
                status="success",
                record_count=1,
                batch_id=batch_id
            )

        except Exception as e:
            log_pipeline_event(
                entity_type="unit",
                doorloop_id=unit.get("id"),
                pipeline_stage="normalization_units",
                status="failed",
                error_code="NORMALIZATION_ERROR",
                error_message=str(e),
                error_details={"raw_data": unit, "traceback": traceback.format_exc()},
                batch_id=batch_id
            )

    log_pipeline_event(
        entity_type="unit",
        pipeline_stage="normalization_units",
        status="success" if success_count == len(raw_units) else "partial_success",
        record_count=success_count,
        batch_id=batch_id,
        duration_ms=int((time.perf_counter() - start_norm_time) * 1000)
    )

if __name__ == "__main__":
    normalize_units()
