import logging, os
import azure.functions as func
import requests

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY')

HEADERS = {
    'apikey': SUPABASE_SERVICE_KEY or '',
    'Authorization': f'Bearer {SUPABASE_SERVICE_KEY or ""}',
    'Content-Type': 'application/json'
}

CACHE_SECONDS = int(os.getenv('RPC_CACHE_TTL_SECONDS', '30'))
_cache = {}

def _now_ts():
    # simple seconds timestamp without importing datetime explicitly
    import time
    return int(time.time())

def _cache_get(key):
    v = _cache.get(key)
    if not v:
        return None
    data, exp = v
    if exp < _now_ts():
        _cache.pop(key, None)
        return None
    return data

def _cache_set(key, data):
    _cache[key] = (data, _now_ts() + CACHE_SECONDS)

def main(req: func.HttpRequest) -> func.HttpResponse:
    entity = req.route_params.get('entity')  # property|unit|lease|tenant|owner
    try:
        _id = int(req.params.get('id'))
    except Exception:
        return func.HttpResponse('Missing or invalid id', status_code=400)

    if entity not in {'property', 'unit', 'lease', 'tenant', 'owner'}:
        return func.HttpResponse('Invalid entity', status_code=400)

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return func.HttpResponse('Server misconfigured', status_code=500)

    cache_key = f'{entity}:{_id}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return func.HttpResponse(cached, headers={'Content-Type': 'application/json'})

    url = f"{SUPABASE_URL}/rest/v1/rpc/get_{entity}_dashboard"
    try:
        r = requests.post(url, headers=HEADERS, json={"p_id": _id}, timeout=20)
        if r.status_code >= 400:
            logging.error('RPC error %s: %s', r.status_code, r.text)
            return func.HttpResponse('RPC error', status_code=502)
        body = r.text
        _cache_set(cache_key, body)
        return func.HttpResponse(body, headers={'Content-Type': 'application/json'})
    except Exception:
        logging.exception('RPC proxy failed')
        return func.HttpResponse('Upstream failure', status_code=502)
