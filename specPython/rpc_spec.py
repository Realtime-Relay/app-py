"""
============================================================
RPC SPEC — Remote Procedure Calls
============================================================

Covers: app.rpc.call
"""

# ─────────────────────────────────────────────────────────────
# await app.rpc.call(params)
# ─────────────────────────────────────────────────────────────

"""
@method rpc.call
@description Sends a synchronous RPC request to a specific device and
             waits for a response within the specified timeout.
             Uses NATS request/reply pattern.

@param params: dict
    - device_ident: str  — Required. Device identifier. [a-zA-Z0-9_-]+
    - name: str          — Required. RPC method name. Validated by validate_rpc_name(). [a-zA-Z0-9_-]+
    - timeout: float     — Optional. Timeout in seconds. Defaults to 10. Must be > 0.
    - data: Any          — Required. RPC payload (arbitrary JSON-serializable).

@raises ValueError — If device_ident is missing/empty or fails validation
@raises ValueError — If name is missing/empty or fails validation
@raises ValueError — If timeout is provided and not a positive number
@raises ValueError — If data is None
@raises RuntimeError — If not connected
@raises TimeoutError — On timeout (device offline or did not respond)

@nats_subject {org_id}.{env}.command.rpc.<device_id>.<rpc_name>
@nats_type request (NATS request/reply)
@encoding JSON

@request_payload
The user-provided data dict, encoded as JSON.

@response_payload
Response from the device, decoded as JSON.
Structure is device-defined (arbitrary).

@behavior
- Resolves device_ident to device_id via device cache
- Sends NATS request to the RPC subject with timeout (seconds)
- Waits for response from device
- If device is offline or does not respond, NATS will timeout
- Returns the decoded response from the device

@returns dict — Response from the device

@example
    response = await app.rpc.call({
        "device_ident": "gateway_01",
        "name": "get_diagnostics",
        "timeout": 20,
        "data": {"include_logs": True}
    })
    # response = {"cpu": 45, "memory": 72, "uptime": 86400}
"""
