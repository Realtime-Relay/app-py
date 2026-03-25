"""
============================================================
HEIRARCHY GROUP SPEC — Hierarchy Group CRUD, Device List, Stream
============================================================

Covers: app.heirarchy_group.create, update, delete, list, get,
        list_devices, heirarchy_group.stream
"""

# ─────────────────────────────────────────────────────────────
# await app.heirarchy_group.create(params)
# ─────────────────────────────────────────────────────────────

"""
@method heirarchy_group.create
@description Creates a new hierarchy group with a hierarchy path and devices.

@param params: dict
    - name: str              — Required. Group name. [a-zA-Z0-9_.-]+
    - heirarchy: str         — Required. Hierarchy path (dot-separated). [a-zA-Z0-9_.-]+
    - device_idents: list[str] — Required. List of device identifiers.

@raises ValueError — If name is missing or fails validation
@raises ValueError — If heirarchy is missing or fails validation
@raises ValueError — If device_idents is not a list
@raises RuntimeError — If not connected

@nats_subject api.iot.cohort.{org_id}.heirarchy.create
@nats_type request
@encoding JSON

@request_payload
{
    "device_ids": list[str],    # Resolved from device_idents
    "name": str,
    "heirarchy": str            # Dot-separated hierarchy path
}

@returns HeirarchyGroup | None — Group object with .stream() and .off() methods

@example
    group = await app.heirarchy_group.create({
        "name": "building_a_floor_1",
        "heirarchy": "building_a.floor_1",
        "device_idents": ["sensor_01", "sensor_02"]
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.heirarchy_group.update(params)
# ─────────────────────────────────────────────────────────────

"""
@method heirarchy_group.update
@description Updates a hierarchy group's hierarchy path and/or devices.

@param params: dict
    - id: str                           — Required. Group ID.
    - heirarchy: str                    — Optional. [a-zA-Z0-9_.-]+
    - devices: dict                     — Optional. {"add": list[ident], "remove": list[ident]}

@raises ValueError — If id is missing
@raises ValueError — If heirarchy fails validation when provided
@raises RuntimeError — If not connected

@nats_subject api.iot.cohort.{org_id}.heirarchy.update

@request_payload
{
    "id": str,
    "devices": {"add": list[str], "remove": list[str]},   # Resolved device_ids — always sent, defaults to empty
    "heirarchy": str                                        # Only sent if provided
}

@behavior
- devices is always included in the payload
- If not provided by the caller, defaults to {"add": [], "remove": []}
- device idents in devices.add / devices.remove are resolved to device_ids

@returns HeirarchyGroup | None

@example
    # Update only hierarchy path (devices defaults to {add: [], remove: []})
    group = await app.heirarchy_group.update({
        "id": "<group_id>",
        "heirarchy": "building_a.floor_2",
    })

    # Update both
    group = await app.heirarchy_group.update({
        "id": "<group_id>",
        "heirarchy": "building_a.floor_3",
        "devices": {"add": ["sensor_05"], "remove": ["sensor_01"]},
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.heirarchy_group.delete(group_id)
# ─────────────────────────────────────────────────────────────

"""
@method heirarchy_group.delete
@description Deletes a hierarchy group by ID.

@param group_id: str — Required. The group ID (passed directly, not as a dict).

@raises ValueError — If group_id is falsy
@raises RuntimeError — If not connected

@nats_subject api.iot.cohort.{org_id}.heirarchy.delete
@returns bool

@example
    deleted = await app.heirarchy_group.delete("abc123")
"""

# ─────────────────────────────────────────────────────────────
# await app.heirarchy_group.list()
# ─────────────────────────────────────────────────────────────

"""
@method heirarchy_group.list
@description Lists all hierarchy groups for the org.

@nats_subject api.iot.cohort.{org_id}.heirarchy.list
@returns list[dict]

@example
    groups = await app.heirarchy_group.list()
"""

# ─────────────────────────────────────────────────────────────
# await app.heirarchy_group.get(group_id)
# ─────────────────────────────────────────────────────────────

"""
@method heirarchy_group.get
@description Gets a single hierarchy group by ID. Returns with .stream() and .off() methods.

@param group_id: str — Required. The group ID (passed directly, not as a dict).

@raises ValueError — If group_id is falsy
@raises RuntimeError — If not connected

@nats_subject api.iot.cohort.{org_id}.heirarchy.get

@behavior
- On success: injects id into data, wraps as HeirarchyGroup with .stream() and .off()
- On failure: returns raw response

@returns HeirarchyGroup | dict

@example
    group = await app.heirarchy_group.get("abc123")
    await group.stream({"callback": on_data})
"""

# ─────────────────────────────────────────────────────────────
# await app.heirarchy_group.list_devices(group_id)
# ─────────────────────────────────────────────────────────────

"""
@method heirarchy_group.list_devices
@description Lists all devices in a hierarchy group.

@param group_id: str — Required. The group ID (passed directly, not as a dict).

@raises ValueError — If group_id is falsy
@raises RuntimeError — If not connected

@nats_subject api.iot.cohort.{org_id}.heirarchy.device.list
@returns list[dict]

@example
    devices = await app.heirarchy_group.list_devices("abc123")
"""

# ─────────────────────────────────────────────────────────────
# await group.stream(params)
# ─────────────────────────────────────────────────────────────

"""
@method heirarchy_group.stream
@description Subscribes to the hierarchy group's real-time data stream.
             Metric and hierarchy are part of the NATS subject (server-side).
             Device idents and multi-metric lists are filtered client-side.

@param params: dict
    - device_idents: list[str]  — Optional. Device idents to filter (client-side).
    - heirarchy: str            — Optional. Hierarchy wildcard override for NATS subject.
                                   If omitted, uses group's stored hierarchy path.
                                   Validated: [a-zA-Z0-9_.*>-]+, ">" must be last token.
    - metric: str               — Optional. Must be "*". Mutually exclusive with metrics.
    - metrics: list[str]        — Optional. List of specific metric names.
                                   Mutually exclusive with metric.
                                   If single item: used directly in NATS subject.
                                   If multiple: subject uses "*", filter client-side.
    - callback: Callable        — Required. Called with each data point.

@raises ValueError — If callback is not callable
@raises ValueError — If both metric and metrics are provided
@raises ValueError — If heirarchy fails wildcard validation
@raises ValueError — If ">" appears anywhere other than last token
@raises RuntimeError — If not connected

@nats_subject import.{org_id}.{env}.heirarchy.listen.{metric_token}.{heirarchy_token}
@nats_type jetstream_consumer
@encoding msgpack (decode on receive)

@subject_construction
metric_token:
    metric: "*"                     -> "*"
    metrics: ["temperature"]        -> "temperature" (single -> direct)
    metrics: ["temp", "humidity"]   -> "*" (multiple -> client-side filter)
    neither provided                -> "*" (default: all metrics)

heirarchy_token:
    params.heirarchy provided       -> use params.heirarchy (wildcards allowed)
    params.heirarchy omitted        -> use group.heirarchy

@callback_payload
{
    "ident": str,         # Device identifier
    "metric": str,        # Metric name
    "value": Any,         # Metric value
    "timestamp": int      # Unix timestamp
}

@behavior
- Constructs NATS subject with metric_token and heirarchy_token
- Creates JetStream consumer
- Client-side filtering (AND logic):
  1. If device_idents: check ident in list
  2. If metrics (multiple): check metric in list
- Registers subscription for reconnect

@returns None

@example
    group = await app.heirarchy_group.get("<group_id>")

    # All metrics, group's stored hierarchy
    await group.stream({
        "callback": lambda data: print(data)
    })

    # Specific metric, override hierarchy
    await group.stream({
        "metrics": ["temperature"],
        "heirarchy": "campus.building_a.*",
        "callback": lambda data: print(data)
    })

    # Multiple metrics (client-side filter), filter by device
    await group.stream({
        "device_idents": ["sensor_01"],
        "metrics": ["temperature", "humidity"],
        "callback": lambda data: print(data)
    })
"""

# ─────────────────────────────────────────────────────────────
# await group.off()
# ─────────────────────────────────────────────────────────────

"""
@method heirarchy_group.off
@description Stops the stream for this hierarchy group instance.
             Unsubscribes the JetStream consumer and removes it
             from the subscription registry.

@behavior
- Pops the consumer from _stream_consumers by group ID
- Unsubscribes the JetStream consumer
- Unregisters the subscription from context (disables reconnect resubscription)
- Safe to call even if no stream is active

@returns None

@example
    group = await app.heirarchy_group.get("<group_id>")

    await group.stream({
        "callback": lambda data: print(data)
    })

    # ... later ...

    await group.off()
"""
