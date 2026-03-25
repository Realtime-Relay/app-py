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
             device and metric via JetStream consumer.

@param params: dict
    - device_ident: str      — Required. Device identifier. [a-zA-Z0-9_-]+
    - metric: str            — Required. Metric name or "*" for all. Validated by validate_telemetry_metric(). [a-zA-Z0-9_-] or "*"
    - callback: Callable | AsyncCallable — Required. Called with each telemetry data point.
                                Supports both sync and async callbacks.

@raises ValueError — If device_ident is missing/empty or fails validation
@raises ValueError — If metric is missing/empty or contains invalid characters (via validate_telemetry_metric)
@raises ValueError — If metric is not "*" and not a key in device.schema
@raises ValueError — If callback is not callable
@raises RuntimeError — If not connected

@nats_subject {org_id}.{env}.telemetry.<device_id>.<metric>
@nats_type jetstream_consumer
@encoding msgpack (decode on receive)

@callback_payload
{
    "metric": str,        # Metric name (e.g., "temperature")
    "value": Any,         # Metric value
    "timestamp": int      # Unix timestamp (ms)
}

@metric_validation
- Only two forms of metric are allowed:
  1. "*" — subscribe to all metrics for the device
  2. A specific metric name that MUST be a key in device.schema
- If metric is not "*" and is not found in device.schema, raise ValueError

@behavior
- Validates metric against device.schema (fetched via device.get())
- Resolves device_ident -> device_id via device cache
- Creates a JetStream consumer for the specific device_id + metric subject
- Decodes msgpack payload on each message, invokes callback
- One consumer per device_ident + metric combination
- If already subscribed to same device_ident + metric, return False
- Consumer name format: apppy_telemetry_{device_ident}_{uuid}
- Registers subscription in ctx._subscription_registry for reconnect resubscription

@returns bool — True if subscription created, False if already exists

@example
    await app.telemetry.stream({
        "device_ident": "sensor_01",
        "metric": "temperature",
        "callback": lambda data: print(f"{data['metric']}: {data['value']}")
    })

    # Subscribe to all metrics
    await app.telemetry.stream({
        "device_ident": "sensor_01",
        "metric": "*",
        "callback": lambda data: print(data)
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.telemetry.off(params)
# ─────────────────────────────────────────────────────────────

"""
@method telemetry.off
@description Unsubscribes from telemetry streams for a device.
             Can target all metrics or specific ones.

@param params: dict
    - device_ident: str       — Required. Device identifier.
    - metric: list[str]       — Optional. List of metric names to unsubscribe.
                                 If omitted, unsubscribes ALL metrics for the device.

@raises ValueError — If device_ident is missing/empty or fails validation
@raises ValueError — If metric is provided but not a list

@behavior
- If metric is omitted: delete ALL JetStream consumers for device_ident
- If metric is provided: delete consumer for each specific metric
- Removes consumer from internal tracking map
- Unregisters from ctx._subscription_registry

@returns None

@example
    # Unsubscribe from all metrics for a device
    await app.telemetry.off({"device_ident": "sensor_01"})

    # Unsubscribe from specific metrics only
    await app.telemetry.off({
        "device_ident": "sensor_01",
        "metric": ["temperature", "humidity"]
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
@encoding JSON

@request_payload
{
    "device_id": str,      # Resolved from device_ident
    "env": str,
    "start": str,          # ISO8601
    "end": str,            # ISO8601
    "fields": list[str]
}

@returns dict — Telemetry history data

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
@description Convenience method that fetches telemetry history for
             the last 24 hours (now - 1 day -> now).

@param params: dict
    - device_ident: str     — Required. Device identifier. [a-zA-Z0-9_-]+
    - fields: list[str]     — Required. List of metric field names.

@raises ValueError — If device_ident is missing/empty
@raises ValueError — If fields is not a non-empty list
@raises ValueError — If any field is not a key in device.schema
@raises RuntimeError — If not connected

@nats_subject api.iot.db.{org_id}.telemetry.history
@nats_type request
@encoding JSON

@request_payload
{
    "device_id": str,
    "env": str,
    "start": str,          # now() - 24 hours, ISO8601
    "end": str,            # now(), ISO8601
    "fields": list[str]
}

@returns dict — Telemetry history data for last 24 hours

@example
    latest = await app.telemetry.latest({
        "device_ident": "sensor_01",
        "fields": ["temperature", "humidity"]
    })
"""
