import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor

def retry_from_dlq():
    conn = psycopg2.connect(os.getenv("SUPABASE_DIRECT_DB_URL"))
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT * FROM doorloop_error_records
        WHERE status = 'unresolved'
        LIMIT 10
    """)
    records = cur.fetchall()

    for record in records:
        entity_type = record['entity_type']
        doorloop_id = record['doorloop_id']
        raw_data = record['raw_data']

        # Add your retry logic here...
        print(f"Retrying {entity_type} with DoorLoop ID {doorloop_id}...")

        # For now, just mark as 'retried' placeholder
        cur.execute("""
            UPDATE doorloop_error_records
            SET status = 'retried'
            WHERE id = %s
        """, (record['id'],))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    retry_from_dlq()
