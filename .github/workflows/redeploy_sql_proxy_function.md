
# 🚀 Supabase Edge Function: Redeploy `sql-proxy`

This handoff file will **completely reset and redeploy** the `sql-proxy` Edge Function with audit logging, secure validation, and proper environment variables. Follow each step in order.

---

## 1. Prerequisites

You must have the Supabase CLI installed and be authenticated to the correct project:

```bash
supabase login
supabase link --project-ref [your-project-ref]
```

---

## 2. Set up Environment Variables

Create a file named `.env.build` in the root of your Supabase project directory:

```env
SUPABASE_URL=https://[your-project-ref].supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
SQL_PROXY_SECRET=your-secure-random-string
```

---

## 3. Create Folder for Function

```bash
mkdir supabase/functions/sql-proxy
cd supabase/functions/sql-proxy
```

---

## 4. Create `index.ts`

```ts
import { createClient } from 'npm:@supabase/supabase-js@2.39.3'

Deno.serve(async (req) => {
  const authHeader = req.headers.get("Authorization");
  const token = authHeader?.replace("Bearer ", "").trim();
  const expectedSecret = Deno.env.get("SQL_PROXY_SECRET")?.trim();

  if (!token || token !== expectedSecret) {
    return new Response(JSON.stringify({
      success: false,
      error: "Unauthorized: SQL_PROXY_SECRET mismatch",
      received: token,
      expected_present: !!expectedSecret
    }), { status: 401 });
  }

  const { sql_file, sql_content } = await req.json();
  const client = createClient(
    Deno.env.get("SUPABASE_URL") ?? "",
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
  );

  const { data, error } = await client.rpc("execute_sql", {
    sql: sql_content,
    sql_file
  });

  if (error) {
    return new Response(JSON.stringify({ success: false, error: error.message }), { status: 500 });
  }

  return new Response(JSON.stringify({ success: true, data }), { status: 200 });
});
```

---

## 5. SQL: Create Logging + Function (Run in Supabase SQL Editor)

```sql
-- Drop the old function
DROP FUNCTION IF EXISTS execute_sql(text);

-- Create audit table
CREATE TABLE IF NOT EXISTS sql_execution_logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  executed_at timestamptz DEFAULT now(),
  sql_file text,
  sql_text text,
  success boolean,
  error_message text
);

-- Create SQL runner
CREATE OR REPLACE FUNCTION execute_sql(sql text, sql_file text DEFAULT null)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $$
DECLARE
  start_time timestamptz := clock_timestamp();
  result jsonb;
BEGIN
  EXECUTE sql;

  result := jsonb_build_object(
    'success', true,
    'sql_file', sql_file,
    'executed_at', start_time
  );

  INSERT INTO sql_execution_logs (sql_file, sql_text, success)
  VALUES (sql_file, sql, true);

  RETURN result;
EXCEPTION WHEN OTHERS THEN
  INSERT INTO sql_execution_logs (sql_file, sql_text, success, error_message)
  VALUES (sql_file, sql, false, SQLERRM);
  RETURN jsonb_build_object(
    'success', false,
    'error', SQLERRM,
    'sql_file', sql_file
  );
END;
$$;
```

---

## 6. Deploy the Edge Function

```bash
supabase functions deploy sql-proxy --no-verify-jwt --env-file ../../.env.build --force
```

---

## 7. Test from GitHub Actions

Make sure your GitHub repository has these secrets:

- `SUPABASE_URL`
- `SQL_PROXY_SECRET`

Then run your `generate_normalized_views.py` workflow.

---

## ✅ You're Done!

Check Supabase → Functions → sql-proxy → Logs to view SQL executions.
