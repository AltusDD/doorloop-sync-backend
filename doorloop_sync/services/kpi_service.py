import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

class KpiService:
    @staticmethod
    def compute_and_store_all_kpis():
        """
        Main orchestrator for all KPI-related calculations and storage.
        This single, robust function reads all data once, performs all calculations,
        and then writes all results back to the database.
        """
        logger.info("--- Starting KPI Computation ---")
        supabase = SupabaseClient()

        try:
            # ======================================================================
            # PHASE 1: READ ALL DATA
            # Fetch all data from the database a single time at the beginning.
            # ======================================================================
            logger.info("Fetching all required data from Supabase...")
            properties = supabase.fetch_all('properties')
            units = supabase.fetch_all('units')
            leases = supabase.fetch_all('leases')
            logger.info(f"Successfully fetched {len(properties)} properties, {len(units)} units, and {len(leases)} leases.")

            if not leases:
                logger.warning("No leases found. Skipping KPI calculation.")
                return

            # ======================================================================
            # PHASE 2: PERFORM ALL CALCULATIONS IN MEMORY
            # ======================================================================
            logger.info("Performing all calculations in memory...")

            # --- Delinquency Stage Calculation ---
            lease_updates_to_perform = []
            for lease in leases:
                if lease.get('status') == 'ACTIVE':
                    current_stage = lease.get('delinquency_stage')
                    new_stage = 'Current'
                    if float(lease.get('total_balance_due') or 0) > 0:
                        new_stage = 'Delinquent'
                    
                    if current_stage != new_stage:
                        lease_updates_to_perform.append({'id': lease.get('id'), 'delinquency_stage': new_stage})

            # --- KPI Summary Calculation ---
            total_properties = len(properties)
            total_units = len(units)
            
            active_leases_list = [l for l in leases if l.get('status') == 'ACTIVE']
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

            # ======================================================================
            # PHASE 3: WRITE ALL RESULTS TO DATABASE
            # ======================================================================
            logger.info("Writing all results to the database...")

            # --- Write lease delinquency updates ---
            if lease_updates_to_perform:
                logger.info(f"Updating delinquency status for {len(lease_updates_to_perform)} leases...")
                supabase.upsert('leases', lease_updates_to_perform)
                logger.info("Successfully updated delinquency stages.")

            # --- Write the KPI summary snapshot ---
            supabase.supabase.table('kpi_summary').delete().neq('id', -1).execute()
            payload_to_insert = [{'id': 1, 'kpis': kpi_payload}]
            supabase.supabase.table('kpi_summary').insert(payload_to_insert).execute()
            logger.info("✅ Successfully inserted new KPI summary snapshot.")

            logger.info("--- KPI Computation Finished ---")

        except Exception as e:
            logger.error(f"A critical error occurred during KPI computation: {e}", exc_info=True)
            raise