#!/bin/bash
echo "🔍 Validating repo files against roadmap_backend_cleanup_plan.md..."
if [ ! -f roadmap_backend_cleanup_plan.md ]; then
  echo "❌ Roadmap file not found."
  exit 1
fi
roadmap_files=$(grep -v '^#' roadmap_backend_cleanup_plan.md | tr -d '')
for f in $(find . -type f | sed 's|^\./||'); do
  if ! grep -Fxq "$f" <<< "$roadmap_files"; then
    echo "🛑 File '$f' is NOT listed in roadmap as KEEP. Please update roadmap or remove file."
  fi
done
echo "✅ Validation completed."
