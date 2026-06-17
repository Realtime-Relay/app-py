import asyncio
import inspect
import json
import urllib.error
import urllib.request


async def invoke_callback(cb, *args):
    """Call a callback, awaiting it if it's an async function."""
    if inspect.iscoroutinefunction(cb):
        return await cb(*args)
    else:
        return cb(*args)


def build_credentials(api_key, secret):
    jwt = api_key.strip()
    seed = secret.strip()

    return (
        f"-----BEGIN NATS USER JWT-----\n"
        f"{jwt}\n"
        f"------END NATS USER JWT------\n"
        f"\n"
        f"************************* IMPORTANT *************************\n"
        f"NKEY Seed printed below can be used to sign and prove identity.\n"
        f"NKEYs are sensitive and should be treated as secrets.\n"
        f"\n"
        f"-----BEGIN USER NKEY SEED-----\n"
        f"{seed}\n"
        f"------END USER NKEY SEED------\n"
        f"\n"
        f"*************************************************************"
    )


def decode_stored_value(value):
    """Mirror JS decodeStoredValue: JSON-parse strings starting with `{` or `[`,
    pass everything else through untouched."""
    if isinstance(value, str) and len(value) > 0 and value[0] in ('{', '['):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value
    return value


# ─── HTTP history (influx-db-service) ─────────────────────────────────────
#
# The history endpoints moved off NATS streaming onto the influx-db-service
# REST API. Auth mirrors OTA: exchange the NATS credential for a short-lived
# HS256 bearer + base URL via accounts.user.get_http_token (service "influx"),
# cached on ctx and shared by every history() method, refetched on a 401/403.

async def ensure_influx_auth(ctx, force=False):
    token = getattr(ctx, '_influx_token', None)
    url = getattr(ctx, '_influx_url', None)
    if not force and token and url:
        return token, url

    try:
        res = await ctx.nats_client.request(
            'accounts.user.get_http_token',
            json.dumps({'jwt': ctx.api_key, 'service': 'influx'}).encode(),
            timeout=20.0,
        )
        reply = json.loads(res.data.decode())
    except Exception as e:
        raise RuntimeError(f'get_http_token (influx) failed: {e}')

    data = reply.get('data') if isinstance(reply, dict) else None
    if (not isinstance(reply, dict) or reply.get('status') != 'HTTP_TOKEN_SUCCESS'
            or not (data or {}).get('token') or not (data or {}).get('http_url')):
        reason = (reply.get('msg') or (data or {}).get('msg')
                  or reply.get('status') or 'unknown error') if isinstance(reply, dict) else 'unknown error'
        raise RuntimeError(f'get_http_token (influx) failed: {reason}')

    ctx._influx_token = data['token']
    ctx._influx_url = data['http_url'].rstrip('/')
    return ctx._influx_token, ctx._influx_url


def _influx_post(url, token, body):
    """Blocking POST (run via asyncio.to_thread). Returns (status_code, parsed_json)."""
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        method='POST',
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}',
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        # Non-2xx: the body still carries the error envelope.
        raw = e.read().decode() if e.fp is not None else ''
        try:
            parsed = json.loads(raw) if raw else None
        except ValueError:
            parsed = None
        return e.code, parsed


def _read_history_error(body, http_status):
    d = (body or {}).get('data') if isinstance(body, dict) else None
    d = d or {}
    msg = d.get('code') or d.get('message')
    if not msg:
        errs = d.get('errors')
        msg = ', '.join(errs) if isinstance(errs, list) else f'HTTP {http_status}'
    return d.get('code'), msg


async def http_history(ctx, path, payload, page_limit=10000):
    """POST a history query to the influx-db-service and collect ALL pages.

    The REST endpoint paginates (limit/offset, has_more/next_offset); this loops
    through every page so the caller gets the full range, matching the old NATS
    streaming behavior. Each frame is the endpoint's raw row -- for events,
    {'<name>': {'value': ..., 'timestamp': ...}}.

    Returns {'frames': [...], 'error': bool, 'error_message': str | None}
    (frames = whatever arrived before an error). Callers check result['error'],
    then aggregate result['frames'].
    """
    frames = []
    offset = 0

    # Fetch the token once up front; only re-fetch if a page comes back 401/403.
    token, url = await ensure_influx_auth(ctx)
    tried_refresh = False

    while True:
        try:
            status, body = await asyncio.to_thread(
                _influx_post,
                f'{url}{path}',
                token,
                {**payload, 'limit': page_limit, 'offset': offset},
            )
        except Exception as e:
            return {'error': True, 'error_message': str(e) or 'network error', 'frames': frames}

        # Token expired / invalid: refresh once, then retry the same page.
        if status in (401, 403) and not tried_refresh:
            tried_refresh = True
            token, url = await ensure_influx_auth(ctx, force=True)
            continue

        if status != 200 or not (isinstance(body, dict) and body.get('status')):
            code, error_message = _read_history_error(body, status)
            return {'error': True, 'status': code, 'error_message': error_message, 'frames': frames}

        data = body.get('data') or {}
        for frame in (data.get('frames') or []):
            frames.append(frame)

        page = data.get('page') or {}
        if not page.get('has_more'):
            break

        offset = page.get('next_offset')
        tried_refresh = False  # allow one refresh per page if a long run outlives the token

    return {'frames': frames, 'error': False, 'error_message': None}


def topic_pattern_matcher(pattern_a, pattern_b):
    a = pattern_a.split('.')
    b = pattern_b.split('.')

    i, j = 0, 0
    star_ai, star_aj = -1, -1
    star_bi, star_bj = -1, -1

    while i < len(a) or j < len(b):
        tok_a = a[i] if i < len(a) else None
        tok_b = b[j] if j < len(b) else None

        if tok_a == '>':
            if i != len(a) - 1:
                return False
            if j >= len(b):
                return False

            star_ai = i
            i += 1
            j += 1
            star_aj = j
            continue

        if tok_b == '>':
            if j != len(b) - 1:
                return False
            if i >= len(a):
                return False

            star_bi = j
            j += 1
            i += 1
            star_bj = i
            continue

        single_wildcard = (
            (tok_a == '*' and j < len(b)) or
            (tok_b == '*' and i < len(a))
        )

        if (tok_a is not None and tok_a == tok_b) or single_wildcard:
            i += 1
            j += 1
            continue

        if star_ai != -1:
            star_aj += 1
            j = star_aj
            continue

        if star_bi != -1:
            star_bj += 1
            i = star_bj
            continue

        return False

    return True
