
import os
import psycopg2
import json
import uuid

def move_errors_to_dlq():
    conn = psycopg2.connect(
        dbname=os.environ['SUPABASE_DB'],
        user=os.environ['SUPABASE_USER'],
        password=os.environ['SUPABASE_PASS'],
        host=os.environ['SUPABASE_HOST'],
        port='5432'
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT entity_type, doorloop_id, error_details
        FROM doorloop_pipeline_audit
        WHERE status = 'error'
          AND doorloop_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM doorloop_error_records d
              WHERE d.doorloop_id = doorloop_pipeline_audit.doorloop_id
              AND d.entity_type = doorloop_pipeline_audit.entity_type
          )
        LIMIT 50
    """)

    rows = cursor.fetchall()
    for row in rows:
        entity, dl_id, error_json = row
        cursor.execute("""
            INSERT INTO doorloop_error_records (
                id, entity_type, doorloop_id, error_payload, error_message
            ) VALUES (%s, %s, %s, %s, %s)
        """, (
            str(uuid.uuid4()),
            entity,
            dl_id,
            json.dumps(error_json),
            error_json.get('error') if isinstance(error_json, dict) else str(error_json)
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ DLQ records inserted.")

if __name__ == "__main__":
    move_errors_to_dlq()
