"""
============================================================
CONNECTION SPEC — RelayApp Core + Connection Management
============================================================

Covers: RelayApp constructor, connect, disconnect,
        connection.listeners, connection.presence
"""

# ─────────────────────────────────────────────────────────────
# RelayApp(config)
# ─────────────────────────────────────────────────────────────

"""
@class RelayApp
@description Creates a new RelayApp instance.
             Validates config, extracts org_id from api_key, and
             instantiates all sub-manager classes.

@param config: dict
    - api_key: str       — Required. NATS JWT credential.
    - secret: str        — Required. NATS NKEY seed.
    - mode: str          — Required. "production" | "test".
                           Used as `env` in NATS subjects.

@raises ValueError — If api_key is missing/empty
@raises ValueError — If secret is missing/empty
@raises ValueError — If mode is not "production" or "test"
@raises ValueError — If config is not a dict

@behavior
- Extracts org_id by base64-decoding the JWT payload from api_key
  (standard JWT format: header.payload.signature, decode the payload
   section to get nats.org_data.org_id)
- Stores mode as `env` for use in all NATS subject construction
- Instantiates sub-managers as properties (NATS not live until connect()):
    app.connection       → ConnectionManager
    app.telemetry        → TelemetryManager
    app.command          → CommandManager
    app.rpc              → RPCManager
    app.device           → DeviceManager
    app.events           → EventManager
    app.alert            → AlertManager
    app.logical_group    → LogicalGroupManager
    app.heirarchy_group  → HeirarchyGroupManager
    app.notification     → NotificationManager
- Each manager receives a shared Context object (not a raw dict)
- No network calls are made in the constructor

@returns RelayApp instance with all sub-managers attached

@example
    app = RelayApp({
        "api_key": "<nats_jwt>",
        "secret": "<nkey_seed>",
        "mode": "production"
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.connect()
# ─────────────────────────────────────────────────────────────

"""
@method connect
@description Establishes NATS connection, initializes JetStream,
             populates device cache, and starts status monitoring.
             Idempotent — calling connect() when already connected is a no-op.

@nats_servers ["wss://api.relay-x.io:4421", "wss://api.relay-x.io:4422", "wss://api.relay-x.io:4423"]

@raises Exception — On connection failure (auth_failed, network error)

@behavior
- Builds NATS creds file from api_key (JWT) and secret (NKEY seed):
    -----BEGIN NATS USER JWT-----
    <api_key>
    ------END NATS USER JWT------
    ...
    -----BEGIN USER NKEY SEED-----
    <secret>
    ------END USER NKEY SEED------
- Connects via nats.connect() with:
    servers: <server_array>
    no_echo: True
    allow_reconnect: True
    max_reconnect_attempts: 1200
    reconnect_time_wait: 0.5
    max_outstanding_pings: 2
    ping_interval: 5
    user_credentials: <creds_file_path_or_handler>
    token: api_key
    disconnected_cb: _on_disconnected
    reconnected_cb: _on_reconnected
    closed_cb: _on_closed
    error_cb: _on_error
- Initializes JetStream context via nc.jetstream()
- Initializes KV bucket for ephemeral alert locks
- Populates device cache via app.device.list()
- On success: fires "connected" event via connection.listeners
- On failure: fires "auth_failed" event via connection.listeners

@returns None

@example
    await app.connect()
"""

# ─────────────────────────────────────────────────────────────
# await app.disconnect()
# ─────────────────────────────────────────────────────────────

"""
@method disconnect
@description Gracefully closes the NATS connection and cleans up all resources.

@behavior
- Deletes all active JetStream consumers (telemetry, events, presence, alerts, group streams)
- Clears device cache
- Clears offline message buffer (discards any unsent buffered messages)
- Closes NATS connection via nc.close()
- "disconnected" event is emitted by the closed_cb handler,
  NOT directly by disconnect()
- Resets internal connected state and connect_called flag
- If nats_client is None, returns (no-op)

@returns None

@example
    await app.disconnect()
"""

# ─────────────────────────────────────────────────────────────
# app.connection.listeners(callback)
# ─────────────────────────────────────────────────────────────

"""
@method connection.listeners
@description Registers a callback for connection lifecycle events.

@param callback: Callable[[str], None] — Required. Called with event name string.

@raises ValueError — If callback is not callable

@events
- "connected"         — Fired on successful initial connection
- "disconnected"      — Fired when connection is fully closed (clean or max reconnects exhausted)
- "reconnecting"      — Fired when client is attempting to reconnect (fires ONCE per cycle)
- "reconnected"       — Fired when client successfully reconnects
- "auth_failed"       — Fired when authentication fails during connect

@behavior
- "disconnected" fires ONLY from closed_cb — NOT from disconnected_cb.
  disconnected_cb only sets connected = False (transient network drop).
  closed_cb fires when the connection is fully terminated.
- "reconnecting" fires ONCE on first disconnected_cb, then suppressed
  until state returns to "reconnected". Prevents repeated callbacks
  on every reconnect attempt.
- "reconnected" fires on reconnected_cb:
  1. Guards with _is_reconnecting flag (fires only once per cycle)
  2. Sets connected = True
  3. Re-initializes JetStream context
  4. Resubscribes ALL registered JetStream consumers and NATS core subs
  5. Flushes the offline message buffer
  6. Resets _is_reconnecting flag
  7. Emits "reconnected" event
- "auth_failed" fires on error_cb with authorization errors, then closes connection.
- Only one listener callback at a time. Calling again replaces previous.

@offline_buffer
- All JetStream publish calls go through ctx.publish_or_buffer()
- If connected: attempts jetstream.publish() wrapped in try/except
  - On success: returns the JetStream ack
  - On failure (timeout, disconnect race): catches error, buffers, returns None
- If disconnected: buffers in ctx.offline_buffer, returns None
- On reconnect: _flush_offline_buffer() drains and publishes all buffered messages
- On disconnect(): buffer is cleared (unsent messages are discarded)
- Applies to: command.send, ephemeral alerting engine (fire, resolved, ack, ack_all)
- NEVER raises — callers always get either an ack or None (buffered)

@resubscription
- On reconnect, ALL active subscriptions are recreated:
  - JetStream consumers (telemetry, events, presence, alert listen, group streams, ephemeral)
  - NATS core subscriptions (ephemeral owner RPC handlers)
- Each manager registers subscription info in ctx._subscription_registry when creating consumers
- On reconnect, _resubscribe_all() iterates registry and recreates each subscription

@returns None

@example
    app.connection.listeners(lambda event: print(f"Connection event: {event}"))
    # "connected" | "disconnected" | "reconnecting" | "reconnected" | "auth_failed"
"""

# ─────────────────────────────────────────────────────────────
# await app.connection.presence(callback)
# ─────────────────────────────────────────────────────────────

"""
@method connection.presence
@description Subscribes to device presence events (connect/disconnect).

@param callback: Callable[[dict], None] | AsyncCallable — Required.
    Called with presence data dict. Supports both sync and async callbacks.

@raises ValueError — If callback is not callable
@raises RuntimeError — If not connected

@nats_subject import.{org_id}.{env}.presence.*
@nats_type jetstream_consumer
@encoding msgpack (decode on receive)

@callback_payload
{
    "device_ident": str,                         # Device identifier
    "event": "connected" | "disconnected",       # Presence event type
    "data": dict                                 # User-defined payload, passthrough
}

@behavior
- Creates a JetStream consumer on the presence subject
- Wildcard * matches any device_id in the subject
- Decodes msgpack payload, invokes callback with decoded data
- The "data" field is passed through without processing
- Only one presence callback at a time. Calling again replaces previous.
- Consumer is cleaned up on disconnect()

@returns None

@example
    async def on_presence(data):
        print(f"Device {data['device_ident']} {data['event']}")

    await app.connection.presence(on_presence)
"""

# ─────────────────────────────────────────────────────────────
# await app.connection.presence_off()
# ─────────────────────────────────────────────────────────────

"""
@method connection.presence_off
@description Unsubscribes from device presence events and clears callback.

@behavior
- Unsubscribes the JetStream presence consumer
- Unregisters from ctx._subscription_registry
- Clears the presence callback
- No-op if no presence subscription exists

@returns None

@example
    await app.connection.presence_off()
"""

# ─────────────────────────────────────────────────────────────
# Logger
# ─────────────────────────────────────────────────────────────

"""
@class Logger
@description Simple logger that only prints when mode is 'test'.
             Attached to Context as ctx.logger.
             All except blocks in the SDK log errors through this logger.

@methods
- logger.error(msg, exc=None)   — Prints error message + full traceback if exc provided
- logger.warn(msg)              — Prints warning message
- logger.info(msg)              — Prints info message
- logger.debug(msg)             — Prints debug message

@behavior
- In 'test' mode: all messages are printed with [relay-sdk] prefix
- In 'production' mode: all messages are silently suppressed
- Every except block in the SDK captures the exception and logs via ctx.logger.error()

@format
    [relay-sdk] ERROR: <message>
    Traceback (most recent call last):
      ...
"""

# ─────────────────────────────────────────────────────────────
# Async Callback Support
# ─────────────────────────────────────────────────────────────

"""
@description
All callbacks across the SDK (telemetry, events, alerts, presence,
ephemeral alerting, group streams) support both sync and async functions.

Uses inspect.iscoroutinefunction() to detect async callbacks and
awaits them accordingly via invoke_callback() in utils.py.

@example
    # Sync callback
    await app.telemetry.stream({
        "device_ident": "s-1",
        "metric": "temp",
        "callback": lambda data: print(data)
    })

    # Async callback
    async def on_data(data):
        await save_to_db(data)

    await app.telemetry.stream({
        "device_ident": "s-1",
        "metric": "temp",
        "callback": on_data
    })
"""
