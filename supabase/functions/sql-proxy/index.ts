// supabase/functions/sql-proxy/index.ts
import { createClient } from 'npm:@supabase/supabase-js@2.39.3';
// @supabase/functions-js
// deno-lint-ignore-file
Deno.serve(async (req) => {
  try {
    const authHeader = req.headers.get('Authorization') || '';
    const expectedSecret = Deno.env.get('SQL_PROXY_SECRET')?.trim();

    if (authHeader !== expectedSecret) {
      return new Response(JSON.stringify({
        code: 401,
        message: `Auth header is not '${authHeader}'`
      }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const body = await req.json();
    const sqlContent = body.sql;
    if (!sqlContent) {
      return new Response(JSON.stringify({
        code: 400,
        message: 'Missing SQL content'
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const supabaseUrl = Deno.env.get('SUPABASE_URL') || '';
    const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || '';
    const supabase = createClient(supabaseUrl, supabaseKey);

    const { data, error } = await supabase.rpc('execute_sql', {
      sql: sqlContent
    });

    if (error) {
      return new Response(JSON.stringify({
        code: 500,
        message: error.message
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    return new Response(JSON.stringify({
      success: true,
      data
    }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });

  } catch (err) {
    return new Response(JSON.stringify({
      code: 500,
      message: err.message
    }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
});
