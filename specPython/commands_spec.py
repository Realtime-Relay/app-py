"""
============================================================
COMMANDS SPEC — Command Send + History
============================================================

Covers: app.command.send, app.command.history
"""

# ─────────────────────────────────────────────────────────────
# await app.command.send(params)
# ─────────────────────────────────────────────────────────────

"""
@method command.send
@description Sends a command to one or more devices via JetStream publish.
             Resolves device idents to device IDs and publishes individually
             to each device's command queue subject.

@param params: dict
    - name: str              — Required. Command name. Validated by validate_command_name(). [a-zA-Z0-9_-]+
    - device_ident: list[str] — Required. List of device identifiers.
    - data: Any              — Required. Arbitrary command payload.

@raises ValueError — If name is missing/empty or fails validation
@raises ValueError — If device_ident is not a non-empty list
@raises ValueError — If any device_ident fails validation
@raises ValueError — If data is None

@nats_subject {org_id}.{env}.command.queue.<device_id>.<command_name>
@nats_type publish (JetStream publish per device, or buffered if offline)
@encoding msgpack

@publish_payload (per device)
{
    "value": Any,          # The user-provided data
    "timestamp": int       # Unix ms timestamp
}

@offline_behavior
- Uses ctx.publish_or_buffer() instead of direct jetstream.publish()
- If connected: publishes immediately via JetStream
- If disconnected: buffers the message in ctx.offline_buffer
- Buffered messages are flushed automatically on reconnect
- Does NOT raise when disconnected — commands are queued

@behavior
- Resolves each device_ident to device_id via device cache
- Unfound devices are SKIPPED, marked with error in result
- For each found device: msgpack encode payload, publish or buffer
- Returns a per-ident result dict:
    {"sent": True}                              — ack received
    {"sent": False, "buffered": True}           — queued for later
    {"sent": False, "error": "Device not found"} — ident could not be resolved

@returns dict[str, dict] — Map of ident -> send result

@example
    result = await app.command.send({
        "name": "reboot",
        "device_ident": ["sensor_01", "sensor_99"],
        "data": {"force": True, "delay": 5}
    })
    # result == {
    #   "sensor_01": {"sent": True},
    #   "sensor_99": {"sent": False, "error": "Device not found"}
    # }
"""

# ─────────────────────────────────────────────────────────────
# await app.command.history(params)
# ─────────────────────────────────────────────────────────────

"""
@method command.history
@description Fetches historical command records for specific devices
             within a time range.

@param params: dict
    - name: str              — Required. Command name. Validated by validate_command_name(). [a-zA-Z0-9_-]+
    - device_idents: list[str] — Required. List of device identifiers.
    - start: str             — Required. ISO8601 datetime string.
    - end: str               — Optional. ISO8601 datetime. Defaults to now().

@raises ValueError — If name is missing/empty
@raises ValueError — If device_idents is not a non-empty list
@raises ValueError — If start is not a valid ISO8601 string
@raises ValueError — If end is provided and not a valid ISO8601 string
@raises RuntimeError — If not connected

@nats_subject api.iot.db.{org_id}.command.history
@nats_type request
@encoding JSON

@request_payload
{
    "device_ids": list[str],    # Only found device IDs
    "env": str,
    "command_name": str,
    "start": str,               # ISO8601
    "end": str                  # ISO8601
}

@behavior
- Resolves device_idents to device_ids via device cache
- Unfound idents are SKIPPED from request, marked with error in result
- Sends a single NATS request with only found device_ids
- Remaps backend response keys from device_id -> ident
- If end is not provided, defaults to current time

@returns dict[str, Any] — Map of ident -> history records or error

@example
    history = await app.command.history({
        "name": "reboot",
        "device_idents": ["sensor_01", "sensor_99"],
        "start": "2026-01-01T00:00:00.000Z",
        "end": "2026-01-02T00:00:00.000Z"
    })
    # history == {
    #   "sensor_01": [{"value": {...}, "time": "..."}],
    #   "sensor_99": {"error": "Device not found"}
    # }
"""
