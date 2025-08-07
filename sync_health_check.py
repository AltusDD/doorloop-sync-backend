import sys
import re

def main(log_path):
    try:
        with open(log_path, 'r') as f:
            log = f.read()
    except FileNotFoundError:
        print(f"❌ Log file not found: {log_path}")
        sys.exit(1)

    # Patterns to catch major failures
    error_patterns = [
        r'Traceback \(most recent call last\)',
        r'Exception:',
        r'Error:',
        r'CRITICAL',
        r'FATAL',
        r'connection refused',
        r'failed to connect',
        r'permission denied',
        r'KeyError:',
        r'ValueError:',
        r'TypeError:',
        r'ImportError:',
        r'SQL error',
        r'database .* does not exist',
        r'cannot import name',
        r'pg_stat_activity',
    ]

    # Check each pattern
    failed = False
    for pattern in error_patterns:
        if re.search(pattern, log, re.IGNORECASE):
            print(f"❌ Detected issue: '{pattern}'")
            failed = True

    if failed:
        print("\n❌ Sync Pipeline Health Check FAILED. Investigate the above errors.\n")
        sys.exit(1)
    else:
        print("\n✅ Sync Pipeline Health Check PASSED. No critical errors detected.\n")
        sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python sync_health_check.py sync_output.log")
        sys.exit(1)
    main(sys.argv[1])
# sync_reports.py [silent tag]
# Empire grade sync task
