
import os
import psycopg2
import uuid
import datetime
import json
import traceback

def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get("SUPABASE_DB_HOST"),
        database=os.environ.get("SUPABASE_DB_NAME"),
        user=os.environ.get("SUPABASE_DB_USER"),
        password=os.environ.get("SUPABASE_DB_PASSWORD"),
        port=os.environ.get("SUPABASE_DB_PORT", "5432")
    )

def log_pipeline_event(
    entity_type, pipeline_stage, status, doorloop_id=None, internal_id=None,
    record_count=None, error_code=None, error_message=None, error_details=None,
    duration_ms=None, batch_id=None, source_checksum=None, target_checksum=None,
    metadata=None
):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.doorloop_pipeline_audit (
                entity_type, doorloop_id, internal_id, pipeline_stage, status,
                record_count, error_code, error_message, error_details, event_timestamp,
                duration_ms, batch_id, source_checksum, target_checksum, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                entity_type,
                doorloop_id,
                str(internal_id) if internal_id else None,
                pipeline_stage,
                status,
                record_count,
                error_code,
                error_message,
                json.dumps(error_details) if error_details else None,
                datetime.datetime.now(datetime.timezone.utc),
                duration_ms,
                str(batch_id) if batch_id else None,
                source_checksum,
                target_checksum,
                json.dumps(metadata) if metadata else None
            )
        )
        conn.commit()
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to log audit event: {e}")
        print(traceback.format_exc())
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()
