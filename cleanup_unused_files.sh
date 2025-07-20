#!/bin/bash
# cleanup_unused_files.sh
set -e

AUDIT_FILE="roadmap_backend_cleanup_plan.md"

if [ ! -f "$AUDIT_FILE" ]; then
  echo "❌ Audit file $AUDIT_FILE not found."
  exit 1
fi

echo "📋 Reading audit file: $AUDIT_FILE"

# Remove files marked with 🛑 REMOVE
grep "🛑 REMOVE" "$AUDIT_FILE" | cut -d '|' -f2 | while read -r FILE; do
  FILE=$(echo "$FILE" | xargs)  # Trim whitespace
  if [ -f "$FILE" ]; then
    echo "🗑️ Deleting file: $FILE"
    rm "$FILE"
  elif [ -d "$FILE" ]; then
    echo "🗑️ Deleting folder: $FILE"
    rm -rf "$FILE"
  else
    echo "⚠️ File not found: $FILE (skipped)"
  fi
done

# Move files marked with ⚠️ MOVE
grep "⚠️ MOVE" "$AUDIT_FILE" | cut -d '|' -f2,3 | while IFS='|' read -r FILE ACTION; do
  FILE=$(echo "$FILE" | xargs)
  DEST_DIR=$(echo "$ACTION" | sed -n 's/.*MOVE TO: \(.*\)/\1/p' | xargs)
  if [ -n "$DEST_DIR" ] && [ -f "$FILE" ]; then
    mkdir -p "$DEST_DIR"
    echo "📦 Moving $FILE → $DEST_DIR/"
    mv "$FILE" "$DEST_DIR"/
  fi
done

echo "✅ Cleanup complete."
