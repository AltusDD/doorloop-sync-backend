
import os
import psycopg2
import json
from utils.logger import log_pipeline_event

def run_single(doorloop_id: str):
    conn = psycopg2.connect(
        dbname=os.environ['SUPABASE_DB'],
        user=os.environ['SUPABASE_USER'],
        password=os.environ['SUPABASE_PASS'],
        host=os.environ['SUPABASE_HOST'],
        port='5432'
    )
    cur = conn.cursor()

    # Fetch raw data
    cur.execute("SELECT data FROM doorloop_raw_owners WHERE id = %s", (doorloop_id,))
    row = cur.fetchone()
    if not row:
        print(f"❌ No raw record for ID {doorloop_id}")
        return

    raw_json = row[0]

    # Simulate normalization logic here
    try:
        # INSERT/UPSERT normalized data here
        print(f"✅ Processed {doorloop_id} for owners")
        log_pipeline_event(
            entity_type="owners",
            doorloop_id=doorloop_id,
            internal_id="from-normalization",
            status="success",
            stage="normalize",
            error_details=None
        )

    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        log_pipeline_event(
            entity_type="owners",
            doorloop_id=doorloop_id,
            internal_id=None,
            status="failure",
            stage="normalize",
            error_details={"error": str(e)}
        )

    cur.close()
    conn.close()
