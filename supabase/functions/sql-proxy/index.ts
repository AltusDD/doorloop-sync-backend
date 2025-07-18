import { createClient } from 'npm:@supabase/supabase-js@2.39.3'

Deno.serve(async (req) => {
  console.log('--- Edge Function Request Received ---');
  const authHeaderValue = req.headers.get('Authorization');
  const token = authHeaderValue?.replace('Bearer ', '').trim();
  const expectedSecret = Deno.env.get('SQL_PROXY_SECRET')?.trim();

  if (!token || token !== expectedSecret) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401 });
  }

  let requestBody;
  try {
    requestBody = await req.json();
  } catch (e) {
    return new Response(JSON.stringify({ success: false, error: 'Invalid JSON body' }), { status: 400 });
  }

  const { sql_file, sql_content } = requestBody;

  if (!sql_content) {
    return new Response(JSON.stringify({ success: false, error: 'Missing "sql_content" in body' }), { status: 400 });
  }

  const lowerSqlContent = sql_content.toLowerCase().trimStart();
  if (
    !lowerSqlContent.startsWith('create or replace view') &&
    !lowerSqlContent.startsWith('create table if not exists') &&
    !lowerSqlContent.startsWith('alter table')
  ) {
    console.warn(`Unauthorized SQL: ${sql_content.substring(0, 100)}...`);
  }

  const supabaseUrl = Deno.env.get('SUPABASE_URL');
  const serviceRoleKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');

  if (!supabaseUrl || !serviceRoleKey) {
    return new Response(JSON.stringify({ success: false, error: 'Supabase environment not configured' }), { status: 500 });
  }

  const client = createClient(supabaseUrl, serviceRoleKey);
  const { data, error } = await client.rpc('execute_sql', { sql: sql_content });

  if (error) {
    return new Response(JSON.stringify({ success: false, error: error.message, details: error.details }), { status: 500 });
  }

  return new Response(JSON.stringify({ success: true, data: data }), { status: 200 });
});