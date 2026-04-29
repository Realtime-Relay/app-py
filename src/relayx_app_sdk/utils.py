import asyncio
import inspect
import json
import uuid

import msgpack


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


async def stream_history(ctx, request_subject, payload, on_frame=None,
                         request_timeout=20.0, idle_timeout=30.0,
                         ready_timeout=10.0):
    """Drives the streaming-history protocol against the db_manager service.

    Wire protocol mirrors JS streamHistory:
      1. Generate a stream_token (uuid).
      2. Subscribe to `import.<orgID>.<env>.history.<token>` BEFORE sending
         the request.
      3. Send the NATS request including stream_token in the payload.
      4. Server replies with one of three statuses:
           *_FETCH_STREAM_STARTED      -> server is waiting on ready signal
           *_FETCH_SUCCESS_NO_STREAM   -> no data; no frames will be published
           *_FETCH_FAILURE             -> validation or query error
      5. On STREAM_STARTED: publish empty msgpack body to ready_subject to
         signal readiness; server begins streaming.
      6. Receive frames until `last: true`. Each frame's shape is endpoint-
         specific.

    Returns:
      {'status': <reply status>, 'data': <reply.data | None>, 'frames': [...],
       'error': bool, 'error_message': str | None}
    """
    stream_token = str(uuid.uuid4())
    export_subject = f'import.{ctx.org_id}.{ctx.env}.history.{stream_token}'

    sub = await ctx.nats_client.subscribe(export_subject)

    # Send request with stream_token included.
    try:
        req_payload = {**payload, 'stream_token': stream_token}
        res = await ctx.nats_client.request(
            request_subject,
            json.dumps(req_payload).encode(),
            timeout=request_timeout,
        )
    except Exception:
        try:
            await sub.unsubscribe()
        except Exception:
            pass
        raise

    # Server may reply with msgpack on success, JSON on failure.
    try:
        decoded = msgpack.unpackb(res.data, raw=False)
    except Exception:
        try:
            decoded = json.loads(res.data.decode())
        except Exception:
            try:
                await sub.unsubscribe()
            except Exception:
                pass
            raise

    status = decoded.get('status') if isinstance(decoded, dict) else None
    data_field = decoded.get('data') if isinstance(decoded, dict) else None

    # No-data success: nothing will be published.
    if isinstance(status, str) and status.endswith('_NO_STREAM'):
        try:
            await sub.unsubscribe()
        except Exception:
            pass
        return {'status': status, 'data': data_field, 'frames': [], 'error': False, 'error_message': None}

    # Anything other than STREAM_STARTED: failure.
    if not isinstance(status, str) or not status.endswith('_STREAM_STARTED'):
        try:
            await sub.unsubscribe()
        except Exception:
            pass
        return {'status': status, 'data': data_field, 'frames': [], 'error': True, 'error_message': None}

    ready_subject = (data_field or {}).get('ready_subject') if isinstance(data_field, dict) else None
    if not ready_subject:
        try:
            await sub.unsubscribe()
        except Exception:
            pass
        raise RuntimeError(f'{request_subject} replied STREAM_STARTED without ready_subject')

    # Signal ready -- empty msgpack body, server doesn't read it.
    await ctx.nats_client.publish(ready_subject, msgpack.packb({}))

    frames = []
    timed_out = False

    try:
        while True:
            try:
                msg = await asyncio.wait_for(sub.next_msg(timeout=idle_timeout), timeout=idle_timeout)
            except asyncio.TimeoutError:
                timed_out = True
                break
            except Exception:
                # Underlying client may surface its own timeout errors; treat as idle timeout.
                timed_out = True
                break

            if msg is None:
                timed_out = True
                break

            try:
                frame = msgpack.unpackb(msg.data, raw=False)
            except Exception:
                continue

            frames.append(frame)

            if on_frame is not None:
                try:
                    await invoke_callback(on_frame, frame)
                except Exception as cb_err:
                    ctx.logger.error('on_frame callback threw', cb_err)

            if isinstance(frame, dict) and frame.get('last'):
                break
    finally:
        try:
            await sub.unsubscribe()
        except Exception:
            pass

    if timed_out:
        last = frames[-1] if frames else None
        if not last or not (isinstance(last, dict) and last.get('last')):
            raise RuntimeError(
                f'stream_history: idle timeout after {idle_timeout}s on {request_subject}'
            )

    last = frames[-1] if frames else None
    if isinstance(last, dict) and last.get('error'):
        return {'status': status, 'frames': frames, 'error': True, 'error_message': last.get('error'), 'data': data_field}

    return {'status': status, 'frames': frames, 'error': False, 'error_message': None, 'data': data_field}


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
