import logging
from supabase_ingest_client import SupabaseIngestClient

logger = logging.getLogger(__name__)

def compute_and_store_kpis(supabase: SupabaseIngestClient):
    logger.info("📊 Starting KPI computation...")

    kpis = {}

    # --- Property Count ---
    properties = supabase.client.table("doorloop_normalized_properties").select("*").execute()
    kpis["total_properties"] = len(properties.data or [])

    # --- Unit Count ---
    units = supabase.client.table("doorloop_normalized_units").select("*").execute()
    kpis["total_units"] = len(units.data or [])

    # --- Active Leases ---
    leases = supabase.client.table("doorloop_normalized_leases").select("*").execute()
    active_leases = [l for l in leases.data or [] if l.get("status") == "active"]
    kpis["active_leases"] = len(active_leases)

    # --- Tenants ---
    tenants = supabase.client.table("doorloop_normalized_tenants").select("*").execute()
    kpis["total_tenants"] = len(tenants.data or [])

    # --- Vacancy Rate ---
    occupied_units = [u for u in units.data or [] if u.get("status") == "occupied"]
    kpis["occupied_units"] = len(occupied_units)
    try:
        kpis["occupancy_rate"] = round((len(occupied_units) / len(units.data)) * 100, 2)
    except ZeroDivisionError:
        kpis["occupancy_rate"] = 0

    # Store it
    supabase.client.table("kpi_summary").insert({"kpis": kpis}).execute()
    logger.info(f"✅ Stored KPI summary: {kpis}")