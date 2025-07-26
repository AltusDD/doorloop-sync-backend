
import logging
from sync_pipeline.normalize_properties import normalize_properties
from sync_pipeline.normalize_units import normalize_units
from sync_pipeline.normalize_leases import normalize_leases
from sync_pipeline.normalize_tenants import normalize_tenants
from sync_pipeline.normalize_owners import normalize_owners

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    try:
        logging.info("🚀 Starting normalization and patch process...")

        logging.info("🔄 Normalizing properties...")
        normalize_properties()
        logging.info("✅ Properties normalization complete.")

        logging.info("🔄 Normalizing units...")
        normalize_units()
        logging.info("✅ Units normalization complete.")

        logging.info("🔄 Normalizing leases...")
        normalize_leases()
        logging.info("✅ Leases normalization complete.")

        logging.info("🔄 Normalizing tenants...")
        normalize_tenants()
        logging.info("✅ Tenants normalization complete.")

        logging.info("🔄 Normalizing owners...")
        normalize_owners()
        logging.info("✅ Owners normalization complete.")

        logging.info("🎉 All normalization steps complete.")
    except Exception as e:
        logging.exception("💥 Normalization process failed!")

if __name__ == "__main__":
    main()
