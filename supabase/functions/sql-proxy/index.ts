import { createClient } from 'npm:@supabase/supabase-js@2.39.3'

Deno.serve(async (req) => {
  // Accept Authorization: Bearer <token> OR x-proxy-secret
  const authHeader = req.headers.get('Authorization')?.replace('Bearer ', '').trim();
  const proxySecretHeader = req.headers.get('x-proxy-secret')?.trim();
  const expectedSecret = Deno.env.get('SQL_PROXY_SECRET')?.trim();

  if ((!authHeader || authHeader !== expectedSecret) &&
      (!proxySecretHeader || proxySecretHeader !== expectedSecret)) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), {
      status: 401,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  let body;
  try {
    body = await req.json();
  } catch (e) {
    return new Response(JSON.stringify({ error: 'Invalid JSON', details: e.message }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  const sqlContent = body.sql_content || body.sql;
  if (!sqlContent) {
    return new Response(JSON.stringify({ error: 'Missing SQL content (use sql or sql_content field)' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  try {
    const client = createClient(
      Deno.env.get('SUPABASE_URL') || '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || ''
    );

    const { data, error } = await client.rpc('execute_sql', { sql: sqlContent });

    if (error) {
      return new Response(JSON.stringify({ success: false, error: error.message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    return new Response(JSON.stringify({ success: true, data }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  } catch (e) {
    return new Response(JSON.stringify({ success: false, error: e.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
});
