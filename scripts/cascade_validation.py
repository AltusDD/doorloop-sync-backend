#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path

VALIDATION_SEQUENCE = [
    "sql/audit/validate_record_counts.sql",
    "sql/audit/validate_foreign_keys.sql",
    "sql/audit/check_normalized_views.sql",
    "sql/audit/check_full_views.sql"
]

def run_sql_file(sql_path):
    print(f"🔍 Running {sql_path}...")
    result = subprocess.run(
        ["psql", "-v", "ON_ERROR_STOP=1", "-f", sql_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if result.returncode != 0:
        print(f"❌ FAILED: {sql_path}")
        print(result.stderr)
        sys.exit(1)
    else:
        print(f"✅ PASSED: {sql_path}")

def main():
    root_dir = Path(__file__).resolve().parent.parent
    for sql_file in VALIDATION_SEQUENCE:
        full_path = root_dir / sql_file
        if not full_path.exists():
            print(f"🚨 MISSING FILE: {sql_file}")
            sys.exit(1)
        run_sql_file(str(full_path))
    print("🎉 ALL VALIDATIONS PASSED")

if __name__ == "__main__":
    main()
