"""
============================================================
DEVICE SPEC — Device CRUD, List, Get + Cache
============================================================

Covers: app.device.create, app.device.update, app.device.delete,
        app.device.list, app.device.get, device cache behavior
"""

# ─────────────────────────────────────────────────────────────
# DEVICE CACHE BEHAVIOR
# ─────────────────────────────────────────────────────────────

"""
@description Internal device cache for ident-to-device mapping.

@behavior
- Populated on app.connect() via app.device.list()
- Stores full device dicts keyed by ident
- TTL: 2 hours from last population/refresh
- On TTL expiry: lazy-reload on next access (next .get() or ident resolution)
- On device.create() success: immediately add new device to cache
- On device.update() success: immediately update cached device
- On device.delete() success: immediately remove device from cache
- Used by other managers (command, rpc, telemetry, groups) to resolve ident -> device_id
- Resolution flow: check cache -> if miss or TTL expired -> NATS request to device.get

@cache_structure
dict[str, dict]  # key: device_ident, value: full device dict

@ttl 7200 (2 hours in seconds)
"""

# ─────────────────────────────────────────────────────────────
# await app.device.create(params)
# ─────────────────────────────────────────────────────────────

"""
@method device.create
@description Creates a new device. Subject to org limit checks on the backend.

@param params: dict
    - ident: str    — Required. Unique device identifier. [a-zA-Z0-9_-]+
    - schema: dict  — Required. Device schema.
    - config: dict  — Required. Device config.

@raises ValueError — If ident is missing/empty or fails validation
@raises ValueError — If schema is not a dict
@raises ValueError — If config is not a dict
@raises RuntimeError — If not connected

@nats_subject api.iot.devices.{org_id}.create
@nats_type request
@encoding JSON

@request_payload
{
    "ident": str,
    "env": str,        # From app mode ("production" | "test")
    "schema": dict,
    "config": dict
}

@response_payload
# Success:
{
    "status": "DEVICE_CREATE_SUCCESS",
    "data": {
        "id": str,
        "org_id": str,
        "ident": str,
        "env": str,
        "schema": dict,
        "config": dict,
        "creds": dict
    }
}
# Failure:
{
    "status": "DEVICE_CREATE_FAILURE",
    "data": {
        "msg": list[str],
        "code": "DEVICE_CREATE_LIMIT_REACHED",  # optional
        "data": {"limit": int, "current_count": int}  # optional
    }
}

@behavior
- Sends request to backend
- On success: adds device to local cache, returns data (device dict)
- On failure: returns None

@returns dict | None — Device dict on success, None on failure

@example
    device = await app.device.create({
        "ident": "sensor_01",
        "schema": {"temperature": "float", "humidity": "float"},
        "config": {"report_interval": 30}
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.device.update(params)
# ─────────────────────────────────────────────────────────────

"""
@method device.update
@description Updates an existing device's schema and/or config.

@param params: dict
    - id: str           — Required. Device ID.
    - ident: str        — Optional. New device identifier.
    - schema: dict      — Optional. New device schema.
    - config: dict      — Optional. New device config.

@raises ValueError — If id is missing/empty
@raises RuntimeError — If not connected

@nats_subject api.iot.devices.{org_id}.update
@nats_type request
@encoding JSON

@request_payload
{
    "id": str,
    "ident": str,       # Optional
    "schema": dict,     # Optional
    "config": dict      # Optional
}

@response_payload
# Success:
{
    "status": "DEVICE_UPDATE_SUCCESS",
    "data": {
        "org_id": str,
        "device": {
            "id": str,
            "ident": str,
            "schema": dict,
            "config": dict
        }
    }
}

@behavior
- Takes device ID directly (no ident-to-id resolution)
- On success: updates device in local cache, returns data["device"]
- On failure: returns None

@returns dict | None — Updated device dict on success, None on failure

@example
    device = await app.device.update({
        "id": "69bffcb28cc30a4f716936bc",
        "config": {"report_interval": 60}
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.device.delete(ident)
# ─────────────────────────────────────────────────────────────

"""
@method device.delete
@description Deletes a device by its identifier.

@param ident: str — Required. Device identifier. [a-zA-Z0-9_-]+

@raises ValueError — If ident is missing/empty or fails validation
@raises RuntimeError — If not connected

@nats_subject api.iot.devices.{org_id}.delete
@nats_type request
@encoding JSON

@request_payload
{"ident": str}

@response_payload
# Success:
{"status": "DEVICE_DELETE_SUCCESS", "data": {"org_id": str, "device_id": str}}

@behavior
- On success: removes device from local cache
- Returns True on successful deletion

@returns bool

@example
    deleted = await app.device.delete("sensor_01")
"""

# ─────────────────────────────────────────────────────────────
# await app.device.list()
# ─────────────────────────────────────────────────────────────

"""
@method device.list
@description Fetches all devices for the org. Also refreshes the local device cache.

@raises RuntimeError — If not connected

@nats_subject api.iot.devices.{org_id}.list
@nats_type request
@encoding JSON

@request_payload
{} (empty dict)

@response_payload
{
    "status": "DEVICE_FETCH_SUCCESS",
    "data": {
        "org_id": str,
        "devices": list[dict]
    }
}

@behavior
- Replaces entire local device cache with response data
- Resets cache TTL to 2 hours from now
- Called automatically during app.connect()

@returns list[dict] — List of device dicts

@example
    devices = await app.device.list()
"""

# ─────────────────────────────────────────────────────────────
# await app.device.get(params)
# ─────────────────────────────────────────────────────────────

"""
@method device.get
@description Gets a single device by identifier.
             Checks local cache first, falls back to NATS request.

@param params: dict
    - ident: str — Required. Device identifier. [a-zA-Z0-9_-]+

@raises ValueError — If ident is missing/empty or fails validation
@raises RuntimeError — If not connected

@nats_subject api.iot.devices.{org_id}.get
@nats_type request
@encoding JSON

@request_payload
{"ident": str}

@response_payload
# Success:
{"status": "DEVICE_GET_SUCCESS", "data": dict}
# Failure:
{"status": "DEVICE_GET_FAILURE", "data": {"msg": str}}

@behavior
- Step 1: Check local cache for ident
- Step 2: If cache hit AND TTL is valid -> return cached device (no network call)
- Step 3: If cache miss OR TTL expired -> send NATS request to backend
- Step 4: On success, update cache with response and return device
- Step 5: On failure, return failure response

@returns dict — Device dict

@example
    device = await app.device.get({"ident": "sensor_01"})
    # device = {"id": "...", "ident": "sensor_01", "schema": {...}, "config": {...}}
"""
