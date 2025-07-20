#!/bin/bash
echo "📦 Backing up files before cleanup..."
mkdir -p .backup
find . -type f ! -path "./.backup/*" ! -path "./.git/*" ! -path "./node_modules/*" -exec cp --parents {} .backup/ \;
echo "✅ Backup complete. All files copied to .backup/"
