// File: supabase/functions/sql-proxy/index.ts

// This is a minimal Supabase Edge Function for `sql-proxy`
// It can be expanded to relay API calls, log SQL activity, or handle custom auth logic.

export const main = async (req: Request): Promise<Response> => {
  return new Response("✅ sql-proxy function is live and responding!", {
    headers: { "Content-Type": "text/plain" },
  });
};
