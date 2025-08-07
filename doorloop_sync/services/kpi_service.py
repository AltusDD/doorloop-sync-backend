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

        # Phase 1: Core calculations
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

            leases = leases_response

            updates_to_perform = []
            for lease in leases:
                # Only consider active leases for delinquency updates
                if lease.get('status') != 'ACTIVE':
                    continue

                lease_id = lease.get('id')
                current_stage = lease.get('delinquency_stage')
                new_stage = 'Current'

                # ✅ FIX: Explicitly convert total_balance_due to a float for comparison.
                if float(lease.get('total_balance_due') or 0) > 0:
                    new_stage = 'Delinquent'
                else:
                    new_stage = 'Current'

                # Only create an update payload if the stage has actually changed
                if current_stage != new_stage:
                    updates_to_perform.append({'id': lease_id, 'delinquency_stage': new_stage})

            if updates_to_perform:
                logger.info(f"Found {len(updates_to_perform)} leases to update delinquency status.")
                # Use the batch upsert method for efficiency
                supabase.upsert('leases', updates_to_perform)
                logger.info("Successfully updated delinquency stages.")
            else:
                logger.info("No delinquency stage updates were needed; all leases are current.")

        except Exception as e:
            logger.error(f"Failed during delinquency calculation: {e}", exc_info=True)
            raise

    @staticmethod
    def _summarize_kpis(supabase: SupabaseClient):
        """
        Placeholder for summarizing data into KPI tables.
        """
        logger.info("Summarizing KPIs (not yet implemented).")
        pass