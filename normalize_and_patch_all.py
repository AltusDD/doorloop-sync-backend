from normalize.normalize_properties import normalize_properties
from normalize.normalize_units import normalize_units

if __name__ == "__main__":
    print("🔁 Running normalize_and_patch_all.py")
    normalize_properties()
    normalize_units()
