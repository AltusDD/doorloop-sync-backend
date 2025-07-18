import { createClient } from 'npm:@supabase/supabase-js@2.39.3'

Deno.serve(async (req) => {
  const token = req.headers.get('Authorization')?.replace('Bearer ', '').trim();
  const expectedSecret = Deno.env.get('SQL_PROXY_SECRET')?.trim();

  if (!token || token !== expectedSecret) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401 });
  }

  let body;
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: 'Invalid JSON' }), { status: 400 });
  }

  const { sql_file, sql_content } = body;
  if (!sql_content) {
    return new Response(JSON.stringify({ error: 'Missing sql_content' }), { status: 400 });
  }

  const client = createClient(Deno.env.get('SUPABASE_URL'), Deno.env.get('SUPABASE_SERVICE_ROLE_KEY'));
  const { data, error } = await client.rpc('execute_sql', { sql: sql_content });

  if (error) {
    return new Response(JSON.stringify({ success: false, error: error.message }), { status: 500 });
  }

  return new Response(JSON.stringify({ success: true, data }), { status: 200 });
});