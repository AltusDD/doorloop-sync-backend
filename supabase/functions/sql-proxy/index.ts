// sql-proxy/index.ts
import { createClient } from 'npm:@supabase/supabase-js@2.39.3';
// This comment is important: it tells Supabase to disable JWT verification
// @supabase/functions-js
// deno-lint-ignore-file

Deno.serve(async (req) => {
  try {
    console.log("📥 Incoming SQL Proxy request...");

    // 🔐 1. Read Auth Header and Expected Secret
    const authHeader = req.headers.get('Authorization') || '';
    const expectedSecret = Deno.env.get('SQL_PROXY_SECRET')?.trim();

    console.log("🔐 Provided Auth Header:", authHeader);
    console.log("🔐 Expected Secret from Env:", expectedSecret);
    console.log("🔐 Secrets match?", authHeader === expectedSecret ? "✅ YES" : "❌ NO");

    if (!expectedSecret) {
      return new Response(JSON.stringify({
        code: 500,
        message: 'Missing SQL_PROXY_SECRET in Edge Function Environment'
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    if (authHeader !== expectedSecret) {
      return new Response(JSON.stringify({
        code: 401,
        message: `Unauthorized - Auth header '${authHeader}' does not match expected secret`
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // 📦 2. Parse Body
    const body = await req.json();
    const sqlContent = body.sql;

    console.log("📦 Parsed Request Body:", JSON.stringify(body).slice(0, 500));

    if (!sqlContent) {
      return new Response(JSON.stringify({
        code: 400,
        message: 'Missing SQL content'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // 🔗 3. Connect to Supabase
    const supabaseUrl = Deno.env.get('SUPABASE_URL') || '';
    const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || '';

    console.log("🔗 Supabase URL:", supabaseUrl);
    console.log("🔗 Supabase Key Present:", !!supabaseKey);

    if (!supabaseUrl || !supabaseKey) {
      return new Response(JSON.stringify({
        code: 500,
        message: 'Missing Supabase URL or Service Role Key'
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const supabase = createClient(supabaseUrl, supabaseKey);

    // 🚀 4. Execute SQL
    const { data, error } = await supabase.rpc('execute_sql', {
      sql: sqlContent
    });

    if (error) {
      console.error('❌ SQL execution error:', error);
      return new Response(JSON.stringify({
        code: 500,
        message: error.message
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    console.log("✅ SQL executed successfully.");
    return new Response(JSON.stringify({
      success: true,
      data
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  } catch (err) {
    console.error('🔥 Function error:', err);
    return new Response(JSON.stringify({
      code: 500,
      message: err.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
});
