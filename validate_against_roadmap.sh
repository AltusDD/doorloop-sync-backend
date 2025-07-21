#!/bin/bash
echo "🔍 Validating repo files against roadmap_backend_cleanup_plan.md..."

PLAN_FILE="roadmap_backend_cleanup_plan.md"
KEEP_SECTION=$(awk '/^KEEP:/ {flag=1; next} /^---/ {flag=0} flag' "$PLAN_FILE")

EXIT_CODE=0

for file in $(git ls-files); do
    if ! grep -qx "$file" <<< "$KEEP_SECTION"; then
        echo "🛑 File '$file' is NOT listed in roadmap as KEEP. Please update roadmap or remove file."
        EXIT_CODE=1
    fi
done

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "✅ Validation passed: All files conform to roadmap plan."
else
    echo "❌ Validation failed: Some files are not approved in roadmap."
fi

exit $EXIT_CODE