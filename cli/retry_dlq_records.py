
import os
import psycopg2
import importlib
import traceback

# Map entities to their normalizer function
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
        SELECT id, entity_type, doorloop_id
        FROM doorloop_error_records
        ORDER BY created_at
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()

    print(f"🔁 Retrying {len(rows)} DLQ records...
")

    for row in rows:
        dlq_id, entity_type, doorloop_id = row
        try:
            print(f"→ Retrying {entity_type} :: {doorloop_id}")
            module_path = NORMALIZER_MAP.get(entity_type)
            if not module_path:
                raise Exception(f"No normalizer for entity type '{entity_type}'")
            normalizer = importlib.import_module(module_path)
            normalizer.run_single(doorloop_id)  # this must be implemented

            cur.execute("DELETE FROM doorloop_error_records WHERE id = %s", (dlq_id,))
            print(f"✅ Success. Removed DLQ record.
")

        except Exception as e:
            print(f"❌ Error retrying {doorloop_id}: {str(e)}
{traceback.format_exc()}")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    retry_from_dlq()
