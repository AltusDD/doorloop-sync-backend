
import logging
from doorloop_sync.audit.logger import audit_log
from doorloop_sync.services.property_service import PropertySyncService
from doorloop_sync.services.unit_service import UnitSyncService
from doorloop_sync.services.owner_service import OwnerSyncService
from doorloop_sync.services.lease_service import LeaseSyncService
from doorloop_sync.services.tenant_service import TenantSyncService
from doorloop_sync.services.payment_service import PaymentSyncService

logging.basicConfig(level=logging.INFO)

def run_full_pipeline():
    logging.info("🚀 Starting full DoorLoop sync pipeline")

    services = [
        PropertySyncService(),
        UnitSyncService(),
        OwnerSyncService(),
        LeaseSyncService(),
        TenantSyncService(),
        PaymentSyncService()
    ]

    for service in services:
        try:
            service.sync()
        except Exception as e:
            logging.error(f"❌ Error syncing {service.__class__.__name__}")
            audit_log("sync_error", str(e), entity_type=service.__class__.__name__)

    logging.info("✅ Finished entity syncing. Computing KPIs...")
    print("📊 Computing and storing KPIs... (stub)")

if __name__ == "__main__":
    run_full_pipeline()
