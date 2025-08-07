@staticmethod
    def _summarize_kpis(supabase: SupabaseClient):
        """
        Calculates all core business KPIs and saves the snapshot to the kpi_summary table.
        """
        logger.info("Summarizing all core KPIs...")
        try:
            # --- Fetch all necessary data and add DEBUG logging ---
            properties = supabase.fetch_all('properties')
            logger.info(f"DEBUG: Fetched {len(properties) if properties else 0} records from 'properties'.")

            units = supabase.fetch_all('units')
            logger.info(f"DEBUG: Fetched {len(units) if units else 0} records from 'units'.")
            
            leases = supabase.fetch_all('leases')
            logger.info(f"DEBUG: Fetched {len(leases) if leases else 0} records from 'leases'.")

            # --- Perform KPI Calculations ---
            total_properties = len(properties) if properties else 0
            total_units = len(units) if units else 0

            active_leases_list = [l for l in leases if l.get('status') == 'ACTIVE'] if leases else []
            total_active_leases = len(active_leases_list)

            occupied_units = total_active_leases
            occupancy_rate = (occupied_units / total_units) * 100 if total_units > 0 else 0

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