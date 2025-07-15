name: Generate Normalized Views

on:
  workflow_dispatch:
  push:
    paths:
      - generate_normalized_views.py

jobs:
  generate-views:
    runs-on: ubuntu-latest
    steps:
      - name: 🧱 Checkout repo
        uses: actions/checkout@v3

      - name: 🐍 Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: 🔧 Install dependencies
        run: pip install requests

      - name: 🚀 Run normalization script
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}
        run: python generate_normalized_views.py
