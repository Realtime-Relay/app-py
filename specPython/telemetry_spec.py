"""
============================================================
TELEMETRY SPEC — Telemetry Streaming + History
============================================================

Covers: app.telemetry.stream, app.telemetry.off,
        app.telemetry.history, app.telemetry.latest
"""

# ─────────────────────────────────────────────────────────────
# await app.telemetry.stream(params)
# ─────────────────────────────────────────────────────────────

"""
@method telemetry.stream
@description Subscribes to a real-time telemetry stream for a specific
             device via a wildcard JetStream consumer. Supports subscribing
             to all metrics or a specific set with client-side filtering.
             Multiple independent subscriptions per device are allowed.

@param params: dict
    - device_ident: str                     — Required. Device identifier. [a-zA-Z0-9_-]+
    - metric: str | list[str]               — Required.
                                               "*" (str) — subscribe to all metrics.
                                               list[str] — specific metric names to filter on.
                                               Non-"*" strings are rejected.
    - callback: Callable | AsyncCallable    — Required. Called with each telemetry data point.
                                               Supports both sync and async callbacks.

@raises ValueError — If device_ident is missing/empty or fails validation
@raises ValueError — If metric is a non-"*" string
@raises ValueError — If metric is not a string or list
@raises ValueError — If metric is an empty list
@raises ValueError — If any metric in list is not a key in device.schema
@raises ValueError — If callback is not callable
@raises RuntimeError — If not connected

@nats_subject {org_id}.{env}.telemetry.<device_id>.*
@nats_type jetstream_consumer
@encoding msgpack (decode on receive)

@callback_payload
{
    "metric": str,        # Metric name extracted from last token of NATS subject
    "data": Any           # Raw msgpack-decoded payload from the message
}

@metric_validation
- metric accepts two forms:
  1. "*" (str) — subscribe to all metrics, no client-side filter
  2. list[str] — each entry MUST be a key in device.schema
- Non-"*" strings raise: 'metric as a string must be "*". Use a list for specific metrics.'
- Invalid schema keys raise: 'metric "<name>" is not a valid key in device schema'

@consumer_tracking
- Internal dict: {device_ident: list of {sub, metrics: set|None, callback}}
- Each stream() call creates a new independent subscription (no dedup)
- metrics is None for "*" (all metrics), set for specific metrics
- Multiple subscriptions for the same device are allowed with different callbacks/metrics

@behavior
- If metric is list: validates each metric against device.schema
- Resolves device_ident -> device_id via device cache
- Always creates a wildcard JetStream consumer: {org_id}.{env}.telemetry.{device_id}.*
- Client-side filtering: if metrics is a set, only invokes callback when the
  extracted metric name is in the set. If metrics is None ("*"), all messages pass.
- Consumer name format: apppy_telemetry_{device_ident}_{uuid}
- Registers subscription in ctx._subscription_registry for reconnect resubscription

@returns None

@example
    # Subscribe to specific metrics
    await app.telemetry.stream({
        "device_ident": "sensor_01",
        "metric": ["temperature", "humidity"],
        "callback": lambda data: print(f"{data['metric']}: {data['data']}")
    })

    # Subscribe to all metrics
    await app.telemetry.stream({
        "device_ident": "sensor_01",
        "metric": "*",
        "callback": lambda data: print(data)
    })

    # Multiple independent subscriptions for same device
    await app.telemetry.stream({
        "device_ident": "sensor_01",
        "metric": ["temperature"],
        "callback": temp_handler
    })
    await app.telemetry.stream({
        "device_ident": "sensor_01",
        "metric": ["humidity"],
        "callback": humidity_handler
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.telemetry.off(params)
# ─────────────────────────────────────────────────────────────

"""
@method telemetry.off
@description Unsubscribes from telemetry streams for a device.
             Can target all subscriptions or remove specific metrics from
             filtered subscriptions.

@param params: dict
    - device_ident: str       — Required. Device identifier.
    - metric: list[str]       — Optional. List of metric names to unsubscribe.
                                 If omitted, unsubscribes ALL subscriptions for the device.

@raises ValueError — If device_ident is missing/empty or fails validation
@raises ValueError — If metric is provided but not a list

@behavior
- If metric is omitted: delete ALL JetStream consumers for device_ident
  (all subscriptions including "*" subscriptions), remove dict entry.
- If metric is provided: iterate all subscriptions for the device.
  - Wildcard subscriptions (metrics = None, i.e. "*") are SKIPPED — unaffected.
  - For filtered subscriptions: remove each specified metric from the set.
  - If a subscription's metric set becomes empty, delete its consumer.
  - If all subscriptions are removed, clean up the dict entry.
- No-op if device has no active subscriptions.
- Unregisters from ctx._subscription_registry

@returns None

@example
    # Unsubscribe all subscriptions for a device (including "*" subscriptions)
    await app.telemetry.off({"device_ident": "sensor_01"})

    # Remove specific metrics from filtered subscriptions (leaves "*" subscriptions intact)
    await app.telemetry.off({
        "device_ident": "sensor_01",
        "metric": ["temperature"]
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.telemetry.history(params)
# ─────────────────────────────────────────────────────────────

"""
@method telemetry.history
@description Fetches historical telemetry data for a device within a time range.

@param params: dict
    - device_ident: str     — Required. Device identifier. [a-zA-Z0-9_-]+
    - fields: list[str]     — Required. List of metric field names.
    - start: str            — Required. ISO8601 datetime string.
    - end: str              — Required. ISO8601 datetime string.

@raises ValueError — If device_ident is missing/empty
@raises ValueError — If fields is not a non-empty list
@raises ValueError — If any field is not a key in device.schema
@raises ValueError — If start or end is not a valid ISO8601 string
@raises ValueError — If start >= end
@raises RuntimeError — If not connected

@nats_subject api.iot.db.{org_id}.telemetry.history
@nats_type request
@encoding JSON (request) / msgpack (response)

@request_payload
{
    "device_id": str,      # Resolved from device_ident
    "env": str,
    "start": str,          # ISO8601 (cursor on subsequent pages)
    "end": str,            # ISO8601
    "fields": list[str],
    "last_value": False     # Always False for history()
}

@response_payload (msgpack decoded)
# Success (paginated):
{
    "status": "TELEMETRY_FETCH_SUCCESS",
    "data": {
        "has_more": bool,                   # True if more pages exist
        "cursor": str,                      # ISO8601 cursor for next page (if has_more)
        "data": {"<metric>": list}          # Telemetry records per metric for this page
    }
}

@pagination
- Uses a while True loop with cursor-based pagination
- First request uses params.start as the start cursor
- If response has has_more=True, sets start_cursor = data.cursor and loops
- If response has has_more=False or status is not SUCCESS, breaks
- Accumulates records across pages per metric
- Pre-initializes telemetry dict with empty lists for all requested fields
- Request timeout: 20s

@returns dict — Object keyed by metric name -> list of records

@example
    history = await app.telemetry.history({
        "device_ident": "sensor_01",
        "fields": ["temperature", "humidity"],
        "start": "2026-01-01T00:00:00.000Z",
        "end": "2026-01-02T00:00:00.000Z"
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.telemetry.latest(params)
# ─────────────────────────────────────────────────────────────

"""
@method telemetry.latest
@description Fetches the latest (most recent) telemetry value for each
             requested field within a caller-specified time range.

@param params: dict
    - device_ident: str     — Required. Device identifier. [a-zA-Z0-9_-]+
    - fields: list[str]     — Required. List of metric field names.
    - start: str            — Required. ISO8601 datetime string.
    - end: str              — Required. ISO8601 datetime string.

@raises ValueError — If device_ident is missing/empty
@raises ValueError — If fields is not a non-empty list
@raises ValueError — If any field is not a key in device.schema
@raises ValueError — If start or end is not a valid ISO8601 string
@raises ValueError — If start >= end
@raises RuntimeError — If not connected

@nats_subject api.iot.db.{org_id}.telemetry.history
@nats_type request
@encoding JSON (request) / msgpack (response)

@request_payload
{
    "device_id": str,
    "env": str,
    "start": str,          # ISO8601 (caller-provided)
    "end": str,            # ISO8601 (caller-provided)
    "fields": list[str],
    "last_value": True      # Always True for latest()
}

@behavior
- Validates start < end before making request
- Resolves device_ident -> device_id via device cache
- Sends a single request (no pagination) with last_value: True
- On TELEMETRY_FETCH_SUCCESS: extracts first element of each metric array
  -> returns {"<metric>": <single_record>} (flat dict)
- On non-success status: returns empty dict {}
- Request timeout: 20s

@returns dict — Object keyed by metric name -> single latest record

@example
    latest = await app.telemetry.latest({
        "device_ident": "sensor_01",
        "fields": ["temperature", "humidity"],
        "start": "2026-03-27T00:00:00.000Z",
        "end": "2026-03-28T00:00:00.000Z"
    })
    # latest == {"temperature": {"value": 25.3, "time": "..."}, "humidity": {"value": 60, "time": "..."}}
"""
