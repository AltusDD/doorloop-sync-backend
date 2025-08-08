import logging
from doorloop_sync.clients.supabase_client import SupabaseClient
import pandas as pd

logger = logging.getLogger(__name__)

class KpiService:
    @staticmethod
    def compute_and_store_all_kpis():
        """
        Main orchestrator for all KPI-related calculations and storage.
        """
        logger.info("--- Starting KPI Computation ---")
        supabase = SupabaseClient()

        # Phase 1: Core calculations that UPDATE existing data
        KpiService._calculate_delinquency_and_collections(supabase)

        # Phase 2: Final summary table generation
        KpiService._summarize_kpis(supabase)

        logger.info("--- KPI Computation Finished ---")

    @staticmethod
    def _calculate_delinquency_and_collections(supabase: SupabaseClient):
        """
        Updates lease delinquency stages based on their current balance.
        """
        logger.info("Calculating delinquency stages for all active leases...")
        try:
            leases_response = supabase.fetch_all('leases')
            if not leases_response:
                logger.warning("No leases found to process for delinquency.")
                return

            updated_count = 0
            for lease in leases_response:
                if lease.get('status') != 'ACTIVE':
                    continue

                lease_id = lease.get('id')
                current_stage = lease.get('delinquency_stage')
                new_stage = 'Current'

                if float(lease.get('total_balance_due') or 0) > 0:
                    new_stage = 'Delinquent'
                
                if current_stage != new_stage:
                    # ✅ FIX: Use a direct 'update' operation instead of 'upsert'.
                    # This modifies the record in place without sending the 'id'.
                    update_payload = {'delinquency_stage': new_stage}
                    supabase.supabase.table('leases').update(update_payload).eq('id', lease_id).execute()
                    updated_count += 1
            
            if updated_count > 0:
                logger.info(f"Successfully updated delinquency status for {updated_count} leases.")
            else:
                logger.info("No delinquency stage updates were needed.")

        except Exception as e:
            logger.error(f"Failed during delinquency calculation: {e}", exc_info=True)
            raise

    @staticmethod
    def _summarize_kpis(supabase: SupabaseClient):
        """
        Calculates all core business KPIs and saves the snapshot to the kpi_summary table.
        """
        logger.info("Summarizing all core KPIs...")
        try:
            properties = supabase.fetch_all('properties')
            units = supabase.fetch_all('units')
            leases = supabase.fetch_all('leases')

            total_properties = len(properties) if properties else 0
            total_units = len(units) if units else 0
            
            active_leases_list = [l for l in leases if l.get('status') == 'ACTIVE'] if leases else []
            total_active_leases = len(active_leases_list)

            occupancy_rate = (total_active_leases / total_units) * 100 if total_units > 0 else 0

            delinquent_leases = [l for l in active_leases_list if float(l.get('total_balance_due') or 0) > 0]
            total_delinquency = sum(float(l.get('total_balance_due') or 0) for l in delinquent_leases)

            kpi_payload = {
                'total_properties': total_properties,
                'total_units': total_units,
                'occupancy_rate': round(occupancy_rate, 2),
                'total_active_leases': total_active_leases,
                'total_delinquency': round(total_delinquency, 2)
            }
            logger.info(f"Calculated KPIs: {kpi_payload}")

            supabase.supabase.table('kpi_summary').delete().neq('id', -1).execute()
            logger.info("Cleared previous KPI summary.")

            payload_to_insert = [{'id': 1, 'kpis': kpi_payload}]
            supabase.supabase.table('kpi_summary').insert(payload_to_insert).execute()

            logger.info("✅ Successfully inserted new KPI summary snapshot.")

        except Exception as e:
            logger.error(f"Failed during KPI summarization: {e}", exc_info=True)
            raise