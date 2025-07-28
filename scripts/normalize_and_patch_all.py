# scripts/normalize_and_patch_all.py

print("🔥 normalize_and_patch_all.py started...")

from normalizers.normalized_properties import normalize_properties
from supabase_client import upsert_raw_doorloop_data

def main():
    # Normalize data
    print("🔧 Normalizing properties...")
    normalized = normalize_properties()

    print(f"✅ Normalized {len(normalized)} records. Now upserting...")

    # Upload to Supabase
    upsert_raw_doorloop_data(
        table_name="doorloop_normalized_properties", 
        records=normalized
    )

    print("🎉 Normalization and upsert completed!")

if __name__ == "__main__":
    main()
