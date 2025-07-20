#!/bin/bash
echo "🔍 Validating repo files against roadmap_backend_cleanup_plan.md..."

PLAN_FILE="roadmap_backend_cleanup_plan.md"
FILES_TO_KEEP=$(grep -oP '(?<=✅ KEEP: ).*' "$PLAN_FILE")

EXIT_CODE=0

for file in $(git ls-files); do
    if ! grep -q "$file" <<< "$FILES_TO_KEEP"; then
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
