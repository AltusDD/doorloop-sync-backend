
import os
import psycopg2
import uuid
import json

def push_all_mismatches_to_dlq():
    conn = psycopg2.connect(
        dbname=os.environ['SUPABASE_DB'],
        user=os.environ['SUPABASE_USER'],
        password=os.environ['SUPABASE_PASS'],
        host=os.environ['SUPABASE_HOST'],
        port='5432'
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT entity_type, doorloop_id
        FROM doorloop_pipeline_mismatch_report
        WHERE normalized = '❌'
        LIMIT 250
    """)

    rows = cur.fetchall()
    print(f"🚨 Loading {len(rows)} failed records into DLQ...")

    for row in rows:
        entity, dl_id = row
        cur.execute("""
            INSERT INTO doorloop_error_records (
                id, entity_type, doorloop_id, error_payload, error_message
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (doorloop_id, entity_type) DO NOTHING;
        """, (
            str(uuid.uuid4()),
            entity,
            dl_id,
            json.dumps({"source": "pipeline_mismatch_report"}),
            "Auto-pushed from mismatch audit"
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ DLQ updated.")

if __name__ == "__main__":
    push_all_mismatches_to_dlq()
