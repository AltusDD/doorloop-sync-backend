import { serve } from "https://deno.land/std@0.131.0/http/server.ts";
import { supabaseClient } from "../_shared/supabase.ts";

serve(async (req) => {
  const { filename } = await req.json();
  const { data, error } = await supabaseClient
    .from("doorloop_pipeline_audit")
    .insert([{ action: "EXCEL_UPLOAD_ENGINE", filename, status: "received" }]);
  return new Response(JSON.stringify({ data, error }), { status: 200 });
});
