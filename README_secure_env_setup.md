# Secure Environment Setup for doorloop-sync-backend

## ✅ Files in this package

1. `.env.template` — Safe template to push to GitHub
2. `.gitignore` — Prevents `.env` from being committed
3. This README — What to do

---

## 🔐 Local Setup Instructions

1. **DO NOT commit your `.env` file to GitHub**
2. Create a `.env` file in your project root:
   ```env
   DOORLOOP_API_KEY=your-doorloop-token
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_SERVICE_ROLE_KEY=your-secret-role-key
   ```
3. Leave `.env.template` in GitHub so collaborators know what’s expected.

You're now secure and production-ready.
