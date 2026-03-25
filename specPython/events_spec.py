"""
============================================================
EVENTS SPEC — Event Streaming
============================================================

Covers: app.events.stream, app.events.off
"""

# ─────────────────────────────────────────────────────────────
# await app.events.stream(params)
# ─────────────────────────────────────────────────────────────

"""
@method events.stream
@description Subscribes to a named event stream across all devices.
             Uses a wildcard for device_id position in the subject.

@param params: dict
    - name: str          — Required. Event name. Validated by validate_event_name(). [a-zA-Z0-9_-]+
    - callback: Callable | AsyncCallable — Required. Called with each event data.
                           Supports both sync and async callbacks.

@raises ValueError — If name is missing/empty or contains invalid characters (via validate_event_name)
@raises ValueError — If callback is not callable
@raises RuntimeError — If not connected

@nats_subject {org_id}.{env}.events.*.<event_name>
@nats_type jetstream_consumer
@encoding msgpack (decode on receive)

@callback_payload
Decoded msgpack data from the event. Structure is event-defined.

@behavior
- Creates a JetStream consumer on {org_id}.{env}.events.*.<event_name>
- Wildcard * matches any device_id in the subject
- Decodes msgpack payload, invokes callback
- One callback per event name — no duplicates
- If already subscribed to same event name, returns False
- Consumer name format: apppy_events_{event_name}_{uuid}
- Registers subscription in ctx._subscription_registry for reconnect

@returns bool — True if subscription created, False if already exists

@example
    await app.events.stream({
        "name": "door_opened",
        "callback": lambda data: print("Door event:", data)
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.events.off(params)
# ─────────────────────────────────────────────────────────────

"""
@method events.off
@description Unsubscribes from a named event stream.

@param params: dict
    - name: str — Required. Event name. [a-zA-Z0-9_-]+

@raises ValueError — If name is missing/empty or fails validation

@behavior
- Deletes the JetStream consumer for the event name
- Removes callback from internal event map
- Unregisters from ctx._subscription_registry
- If no subscription exists for the event name, no-op

@returns None

@example
    await app.events.off({"name": "door_opened"})
"""
