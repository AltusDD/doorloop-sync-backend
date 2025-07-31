
import logging
from doorloop_sync.services import (
    PropertySyncService,
    UnitSyncService,
    OwnerSyncService,
    LeaseSyncService,
    TenantSyncService,
    PaymentSyncService,
)
from doorloop_sync.kpi.kpi_engine import compute_and_store_kpis
from doorloop_sync.audit.logger import audit_log

logging.basicConfig(level=logging.INFO)

def run_full_pipeline():
    logging.info("🚀 Starting full DoorLoop sync pipeline")

    for service_class in [
        PropertySyncService,
        UnitSyncService,
        OwnerSyncService,
        LeaseSyncService,
        TenantSyncService,
        PaymentSyncService,
    ]:
        service = service_class()
        try:
            service.sync()
        except Exception as e:
            audit_log("sync_error", str(e), entity=service_class.__name__)
            logging.exception(f"❌ Error syncing {service_class.__name__}")

    logging.info("✅ Finished entity syncing. Computing KPIs...")
    try:
        compute_and_store_kpis()
    except Exception as e:
        audit_log("kpi_error", str(e), entity="KPIEngine")
        logging.exception("❌ Error computing KPIs")

if __name__ == "__main__":
    run_full_pipeline()
