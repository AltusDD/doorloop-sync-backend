import os
import psycopg2
import importlib
import traceback

NORMALIZER_MAP = {
    "property": "normalizers.normalize_properties",
    "unit": "normalizers.normalize_units",
    "lease": "normalizers.normalize_leases",
    "tenant": "normalizers.normalize_tenants",
    "owner": "normalizers.normalize_owners"
}

def retry_from_dlq(limit=50):
    conn = psycopg2.connect(
        dbname=os.environ['SUPABASE_DB'],
        user=os.environ['SUPABASE_USER'],
        password=os.environ['SUPABASE_PASS'],
        host=os.environ['SUPABASE_HOST'],
        port='5432'
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT id, entity_type, doorloop_id, retry_count
        FROM doorloop_error_records
        WHERE status = 'open'
        ORDER BY created_at
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()

    print(f"🔁 Retrying {len(rows)} DLQ records...\n")

    for row in rows:
        dlq_id, entity_type, doorloop_id, retry_count = row
        try:
            print(f"→ Retrying {entity_type} :: {doorloop_id}")
            module_path = NORMALIZER_MAP.get(entity_type)
            if not module_path:
                raise Exception(f"No normalizer for entity type '{entity_type}'")
            normalizer = importlib.import_module(module_path)
            normalizer.run_single(doorloop_id)

            cur.execute("DELETE FROM doorloop_error_records WHERE id = %s", (dlq_id,))
            print(f"✅ Success. Removed DLQ record.\n")

        except Exception as e:
            print(f"❌ Error: {doorloop_id}: {str(e)}")
            retry_count += 1
            new_status = 'permafail' if retry_count >= 5 else 'open'
            cur.execute("""
                UPDATE doorloop_error_records
                SET retry_count = %s, status = %s
                WHERE id = %s
            """, (retry_count, new_status, dlq_id))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ DLQ retry loop complete.")

if __name__ == "__main__":
    retry_from_dlq()
