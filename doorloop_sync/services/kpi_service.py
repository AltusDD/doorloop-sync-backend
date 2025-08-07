import logging
# Use the full SupabaseClient that can read data, not just the ingest client.
from doorloop_sync.clients.supabase_client import SupabaseClient 

logger = logging.getLogger(__name__)

class KpiService:
    @staticmethod
    def compute_and_store_all_kpis():
        """
        Connects to Supabase, computes all core KPIs from the clean tables,
        and stores them in the kpi_summary table.
        """
        logger.info("📊 Starting KPI computation...")
        
        try:
            supabase = SupabaseClient()
            kpis = {}

            # --- Fetch all necessary data from the clean tables ---
            properties = supabase.fetch_all("properties")
            units = supabase.fetch_all("units")
            leases = supabase.fetch_all("leases")
            
            # --- Perform Real Calculations ---
            kpis["total_properties"] = len(properties) if properties else 0
            kpis["total_units"] = len(units) if units else 0
            
            active_leases = [l for l in leases if l and l.get("status") == "ACTIVE"] if leases else []
            kpis["active_leases"] = len(active_leases)
            
            # Calculate Occupancy Rate based on active leases with assigned units
            occupied_unit_ids = {l.get("unit_id_dl") for l in active_leases if l.get("unit_id_dl")}
            occupied_units_count = len(occupied_unit_ids)
            
            if kpis["total_units"] > 0:
                occupancy_rate = (occupied_units_count / kpis["total_units"]) * 100
                kpis["occupancy_rate"] = round(occupancy_rate, 2)
            else:
                kpis["occupancy_rate"] = 0

            # --- Store the KPIs ---
            # Upsert the results into the kpi_summary table.
            # The 'id' of 1 ensures we are always overwriting the same row.
            supabase.upsert("kpi_summary", [{"id": 1, "kpis": kpis}])
            
            logger.info(f"✅ Stored KPI summary: {kpis}")

        except Exception as e:
            logger.error(f"❌ KPI Service failed: {e}", exc_info=True)
            raise

# kpi_service.py [silent tag]
