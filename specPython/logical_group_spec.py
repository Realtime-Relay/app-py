"""
============================================================
LOGICAL GROUP SPEC — Logical Group CRUD, Device List, Stream
============================================================

Covers: app.logical_group.create, update, delete, list, get,
        list_devices, logical_group.stream
"""

# ─────────────────────────────────────────────────────────────
# await app.logical_group.create(params)
# ─────────────────────────────────────────────────────────────

"""
@method logical_group.create
@description Creates a new logical group with devices and tags.

@param params: dict
    - name: str              — Required. Group name. [a-zA-Z0-9_-]+
    - tags: list[str]        — Required. List of tag strings.
    - device_idents: list[str] — Required. List of device identifiers.

@raises ValueError — If name is missing or fails validation
@raises ValueError — If tags is not a list
@raises ValueError — If device_idents is not a list
@raises RuntimeError — If not connected

@nats_subject api.iot.cohort.{org_id}.logical.create
@nats_type request
@encoding JSON

@request_payload
{
    "device_ids": list[str],    # Resolved from device_idents
    "name": str,
    "tags": list[str]
}

@behavior
- Resolves device_idents to device_ids via device cache (skips unfound)
- If response has data, wraps with .stream() method and returns
- If no data, returns None

@returns LogicalGroup | None — LogicalGroup object with .stream() and .off() methods

@example
    group = await app.logical_group.create({
        "name": "floor_1_sensors",
        "tags": ["floor_1", "temperature"],
        "device_idents": ["sensor_01", "sensor_02", "sensor_03"]
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.logical_group.update(params)
# ─────────────────────────────────────────────────────────────

"""
@method logical_group.update
@description Updates a logical group's name, tags, and/or device membership.
             Tags and devices use add/remove pattern.

@param params: dict
    - id: str                           — Required. Group ID.
    - name: str                         — Optional. New group name.
    - tags: dict                        — Optional. {"add": list, "remove": list}
    - devices: dict                     — Optional. {"add": list[ident], "remove": list[ident]}

@raises ValueError — If id is missing
@raises RuntimeError — If not connected

@nats_subject api.iot.cohort.{org_id}.logical.update
@nats_type request
@encoding JSON

@request_payload
{
    "id": str,
    "devices": {"add": list[str], "remove": list[str]},   # Resolved device_ids — always sent, defaults to empty
    "tags": {"add": list[str], "remove": list[str]}        # Always sent, defaults to empty
}

@behavior
- devices and tags are always included in the payload
- If not provided by the caller, they default to {"add": [], "remove": []}
- device idents in devices.add / devices.remove are resolved to device_ids

@returns LogicalGroup | None

@example
    # Update only tags (devices defaults to {add: [], remove: []})
    group = await app.logical_group.update({
        "id": "<group_id>",
        "tags": {"add": ["humidity"], "remove": ["floor_1"]},
    })

    # Update both
    group = await app.logical_group.update({
        "id": "<group_id>",
        "tags": {"add": ["humidity"], "remove": ["floor_1"]},
        "devices": {"add": ["sensor_04"], "remove": ["sensor_01"]}
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.logical_group.delete(group_id)
# ─────────────────────────────────────────────────────────────

"""
@method logical_group.delete
@description Deletes a logical group by ID.

@param group_id: str — Required. The group ID (passed directly, not as a dict).

@raises ValueError — If group_id is falsy
@raises RuntimeError — If not connected

@nats_subject api.iot.cohort.{org_id}.logical.delete
@returns bool — True on successful deletion

@example
    deleted = await app.logical_group.delete("abc123")
"""

# ─────────────────────────────────────────────────────────────
# await app.logical_group.list()
# ─────────────────────────────────────────────────────────────

"""
@method logical_group.list
@description Lists all logical groups for the org.

@nats_subject api.iot.cohort.{org_id}.logical.list
@returns list[dict] — List of logical group dicts

@example
    groups = await app.logical_group.list()
"""

# ─────────────────────────────────────────────────────────────
# await app.logical_group.get(group_id)
# ─────────────────────────────────────────────────────────────

"""
@method logical_group.get
@description Gets a single logical group by ID. Returns with .stream() and .off() methods.

@param group_id: str — Required. The group ID (passed directly, not as a dict).

@raises ValueError — If group_id is falsy
@raises RuntimeError — If not connected

@nats_subject api.iot.cohort.{org_id}.logical.get

@behavior
- On success: injects id into data, returns LogicalGroup object with .stream() and .off() methods
- On failure: returns raw response

@returns LogicalGroup | dict

@example
    group = await app.logical_group.get("abc123")
    await group.stream({"callback": on_data})
"""

# ─────────────────────────────────────────────────────────────
# await app.logical_group.list_devices(group_id)
# ─────────────────────────────────────────────────────────────

"""
@method logical_group.list_devices
@description Lists all devices in a logical group.

@param group_id: str — Required. The group ID (passed directly, not as a dict).

@raises ValueError — If group_id is falsy
@raises RuntimeError — If not connected

@nats_subject api.iot.cohort.{org_id}.logical.device.list
@returns list[dict] — List of device dicts

@example
    devices = await app.logical_group.list_devices("abc123")
"""

# ─────────────────────────────────────────────────────────────
# await group.stream(params)
# ─────────────────────────────────────────────────────────────

"""
@method logical_group.stream
@description Subscribes to the group's real-time data stream.
             Optionally filters by device identifiers (client-side).

@param params: dict
    - device_idents: list[str]  — Optional. Device idents to filter on.
    - callback: Callable        — Required. Called with each data point.

@raises ValueError — If callback is not callable
@raises RuntimeError — If not connected

@nats_subject import.{org_id}.{env}.group.listen.{group_id}
@nats_type jetstream_consumer
@encoding msgpack (decode on receive)

@callback_payload
{
    "ident": str,                       # Device identifier
    "data": {"<metric>": <value>}       # Metric key-value pairs
}

@behavior
- Creates JetStream consumer on group's listen subject
- If device_idents provided: client-side filter by ident
- If omitted: invoke callback for all data
- Registers subscription for reconnect

@returns None

@example
    group = await app.logical_group.get("<group_id>")

    await group.stream({
        "callback": lambda data: print(f"Device {data['ident']}: {data['data']}")
    })

    # With device filter
    await group.stream({
        "device_idents": ["sensor_01", "sensor_02"],
        "callback": lambda data: print(f"Device {data['ident']}: {data['data']}")
    })
"""

# ─────────────────────────────────────────────────────────────
# await group.off()
# ─────────────────────────────────────────────────────────────

"""
@method logical_group.off
@description Stops the stream for this logical group instance.
             Unsubscribes the JetStream consumer and removes it
             from the subscription registry.

@behavior
- Pops the consumer from _stream_consumers by group ID
- Unsubscribes the JetStream consumer
- Unregisters the subscription from context (disables reconnect resubscription)
- Safe to call even if no stream is active

@returns None

@example
    group = await app.logical_group.get("<group_id>")

    await group.stream({
        "callback": lambda data: print(data)
    })

    # ... later ...

    await group.off()
"""
