"""
============================================================
ALERTS SPEC — Alert CRUD, Ack, Mute, Listen + Ephemeral Engine
============================================================

Covers: app.alert.create, update, delete, list, get, ack, ack_all,
        mute, unmute, alert.listen, alert.set_evaluator,
        Ephemeral Alert Engine (client-side state machine)
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.create(config)
# ─────────────────────────────────────────────────────────────

"""
@method alert.create
@description Creates a new alert rule (THRESHOLD or RATE_CHANGE).
             For EPHEMERAL type, use create_ephemeral() instead.

@param config: dict
    - name: str                       — Required. Unique alert name. [a-zA-Z0-9_-]+
    - description: str                — Optional. Alert description.
    - type: str                       — Required. "THRESHOLD" | "RATE_CHANGE"
    - metric: str                     — Required. Metric name to evaluate.
    - config: dict                    — Required. Alert configuration.
    - notification_channel: list[str] — Optional. Notification IDs. Defaults to [].
    - alert_mute_config: dict         — Optional. Initial mute configuration.

@raises ValueError — If name is missing or fails validation
@raises ValueError — If type is "EPHEMERAL" (must use create_ephemeral())
@raises ValueError — If type is not THRESHOLD or RATE_CHANGE
@raises RuntimeError — If not connected

@nats_subject api.iot.alerts.{org_id}.create
@nats_type request
@encoding JSON

@request_payload
{
    "name": str,
    "description": str | None,
    "type": "THRESHOLD" | "RATE_CHANGE",
    "metric": str,
    "config": dict,
    "notification_channel": list[str],
    "alert_mute_config": dict | None,
    "env": str                          # Injected from ctx.env
}

@returns AlertObject | None — Alert dict with .listen() and .set_evaluator() methods

@example
    alert = await app.alert.create({
        "name": "high_temp",
        "type": "THRESHOLD",
        "metric": "temperature",
        "config": {
            "scope": {"type": "DEVICE", "value": "<device_id>"},
            "operator": ">",
            "value": 85,
            "duration": 300,
            "recovery_duration": 120,
            "cooldown": 600
        },
        "notification_channel": ["notif_webhook_01"]
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.create_ephemeral(config)
# ─────────────────────────────────────────────────────────────

"""
@method alert.create_ephemeral
@description Creates an ephemeral alert rule. The evaluator runs client-side.

@param config: dict
    - name: str                                — Required. Unique alert name. [a-zA-Z0-9_-]+
    - description: str                         — Optional.
    - config: dict                             — Required.
        - topic: dict                          — Required.
            - source: str                      — Required. "TELEMETRY" | "COMMAND" | "EVENT"
            - device_ident: str                — Required. Device ident or "*".
            - last_token: str                  — Required. Metric/command/event name or "*".
        - duration: float                      — Required. Seconds breach must hold before FIRE.
        - recovery_duration: float             — Required. Seconds clear must hold before RESOLVED.
        - cooldown: float                      — Optional. Seconds between re-fires. Defaults to 0.
    - notification_channel: list[str]          — Optional. Defaults to [].

@raises ValueError — If name is missing or fails validation
@raises ValueError — If config.topic.source is not TELEMETRY/COMMAND/EVENT
@raises ValueError — If duration or recovery_duration are missing/not positive
@raises RuntimeError — If not connected

@nats_subject api.iot.alerts.{org_id}.create_ephemeral
@nats_type request
@encoding JSON

@returns EphemeralAlertObject | None — Wrapped alert with:
    - .listen(callbacks) — starts engine
    - .set_evaluator(fn) — sets client-side evaluator
    - .stop() — stops engine

@example
    alert = await app.alert.create_ephemeral({
        "name": "high_cpu_custom",
        "config": {
            "topic": {"source": "TELEMETRY", "device_ident": "s-3", "last_token": "cpu_usage"},
            "duration": 30,
            "recovery_duration": 15,
            "cooldown": 60,
        },
        "notification_channel": ["notif_1"],
    })

    alert.set_evaluator(lambda data: data.get("s-3", {}).get("cpu_usage", {}).get("last_value", 0) > 90)

    await alert.listen({
        "on_fire": lambda d: print("FIRE", d),
        "on_resolved": lambda d: print("RESOLVED", d),
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.update(config)
# ─────────────────────────────────────────────────────────────

"""
@method alert.update
@description Updates an existing alert rule (non-ephemeral).
             Injects env from ctx.env into the payload.

@param config: dict
    - id: str — Required. Alert rule ID.
    - (any other alert fields to update)

@raises ValueError — If id is missing
@raises RuntimeError — If not connected

@nats_subject api.iot.alerts.{org_id}.update
@nats_type request
@encoding JSON

@behavior
- Spreads all caller params into the payload
- Adds env: ctx.env to the payload

@returns AlertObject — Updated alert with .listen()
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.update_ephemeral(config)
# ─────────────────────────────────────────────────────────────

"""
@method alert.update_ephemeral
@description Updates an existing ephemeral alert rule.

@param config: dict
    - id: str — Required. Alert rule ID.
    - (any other alert fields to update)

@raises ValueError — If id is missing
@raises RuntimeError — If not connected

@nats_subject api.iot.alerts.{org_id}.update_ephemeral
@nats_type request
@encoding JSON

@returns EphemeralAlertObject | None
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.delete(alert_id)
# ─────────────────────────────────────────────────────────────

"""
@method alert.delete
@description Deletes an alert rule by ID.

@param alert_id: str — Required. Alert rule ID.

@raises ValueError — If id is missing
@raises RuntimeError — If not connected

@nats_subject api.iot.alerts.{org_id}.delete
@nats_type request
@encoding JSON

@returns bool — True on successful deletion

@example
    deleted = await app.alert.delete("67e1a2b3c4d5e6f7a8b9c0d1")
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.list()
# ─────────────────────────────────────────────────────────────

"""
@method alert.list
@description Lists all alert rules for the org.

@nats_subject api.iot.alerts.{org_id}.list
@returns list[dict] — List of alert rule dicts

@example
    alerts = await app.alert.list()
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.get(alert_name)
# ─────────────────────────────────────────────────────────────

"""
@method alert.get
@description Gets a single alert rule by name. Returns appropriate wrapper
             based on type (EPHEMERAL vs THRESHOLD/RATE_CHANGE).

@param alert_name: str — Required. Alert name. [a-zA-Z0-9_-]+

@nats_subject api.iot.alerts.{org_id}.get
@returns AlertObject | EphemeralAlertObject | None

@example
    alert = await app.alert.get("high_temp")
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.history(params)
# ─────────────────────────────────────────────────────────────

"""
@method alert.history
@description Fetches historical alert events. Two query modes:
             DEVICE (by device_ident) or RULE (by rule_id).

@param params: dict
    - rule_type: str           — Required. "DEVICE" | "RULE"
    - device_ident: str        — Required for DEVICE, optional for RULE.
    - rule_id: str             — Required for RULE.
    - rule_states: list[str]   — Required. ["fire", "resolved", "ack", "ack_all"]
    - start: str               — Required. ISO8601 datetime.
    - end: str                 — Required. ISO8601 datetime.

@raises ValueError — If rule_type is not DEVICE or RULE
@raises ValueError — If rule_type is DEVICE and device_ident is missing
@raises ValueError — If rule_type is RULE and rule_id is missing
@raises ValueError — If rule_states contains invalid values
@raises ValueError — If rule_states includes ack/ack_all but rule_id not provided

@nats_subject api.iot.db.{org_id}.alerts.history
@nats_type request
@encoding JSON

@returns dict — Alert history data keyed by rule_state

@example
    history = await app.alert.history({
        "rule_type": "DEVICE",
        "device_ident": "s-3",
        "rule_states": ["fire", "resolved"],
        "start": "2026-03-01T00:00:00.000Z",
        "end": "2026-03-25T00:00:00.000Z",
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.ack(params)
# ─────────────────────────────────────────────────────────────

"""
@method alert.ack
@description Acknowledges an alert for a specific device.

@routing
- Non-ephemeral: backend via api.iot.alerts.{org_id}.ack
- Ephemeral + local owner: calls engine.ack() directly
- Ephemeral + no local engine: NATS request to {org_id}.{env}.alerts.custom.{rule_id}.ack

@param params: dict
    - device_id: str    — Required. Device ID.
    - alert_id: str     — Required. Alert rule ID.
    - acked_by: str     — Required. Who is acknowledging.
    - ack_notes: str    — Optional.

@returns bool — True if ack successful

@example
    ack = await app.alert.ack({
        "device_id": "69bffcb28cc30a4f716936bc",
        "alert_id": "rule_1",
        "acked_by": "operator_jane",
        "ack_notes": "Investigating cooling system"
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.ack_all(params)
# ─────────────────────────────────────────────────────────────

"""
@method alert.ack_all
@description Acknowledges an alert across all devices for a rule.
             Same routing as ack().

@param params: dict
    - alert_id: str     — Required. Alert rule ID.
    - acked_by: str     — Required.
    - ack_notes: str    — Optional.

@returns bool — True if ack_all successful

@example
    ack_all = await app.alert.ack_all({
        "alert_id": "rule_1",
        "acked_by": "operator_jane",
        "ack_notes": "Bulk ack for maintenance window"
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.mute(params)
# ─────────────────────────────────────────────────────────────

"""
@method alert.mute
@description Mutes an alert rule. Muted alerts are not evaluated.

@routing
- Non-ephemeral: backend via api.iot.alerts.{org_id}.mute
- Ephemeral: NATS request to {org_id}.{env}.alerts.custom.{rule_id}.mute

RPC BASE SUBJECT: {org_id}.{env}.alerts.custom.{rule_id}
RPC HANDLERS:
    - {base}.ack       — Handle ack RPC
    - {base}.ack_all   — Handle ack_all RPC
    - {base}.mute      — Handle mute RPC

@param params: dict
    - id: str                       — Required. Alert rule ID.
    - mute_config: dict             — Required.
        - type: str                 — Required. "FOREVER" | "TIME_BASED"
        - mute_till: str            — Required for TIME_BASED. ISO8601 timestamp.

@returns dict — Updated rule with mute config

@example
    await app.alert.mute({
        "id": "<alert_id>",
        "mute_config": {
            "type": "TIME_BASED",
            "mute_till": "2026-03-25T00:00:00Z"
        }
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.alert.unmute(alert_id)
# ─────────────────────────────────────────────────────────────

"""
@method alert.unmute
@description Unmutes an alert rule. Uses same subject as mute with type="CLEAR".

@param alert_id: str — Required. Alert rule ID.

@returns dict — Updated rule

@example
    await app.alert.unmute("<alert_id>")
"""

# ─────────────────────────────────────────────────────────────
# await alert.listen(callbacks)
# ─────────────────────────────────────────────────────────────

"""
@method alert.listen
@description Subscribes to alert lifecycle events for a specific rule.
             Non-ephemeral: passthrough from backend JetStream.
             Ephemeral: starts client-side alert engine.

@param callbacks: dict — All callbacks support both sync and async functions.
    - on_fire: Callable | AsyncCallable       — Called when alert fires.
    - on_resolved: Callable | AsyncCallable   — Called when alert resolves.
    - on_ack: Callable | AsyncCallable        — Called when alert is acknowledged.
    - on_ack_all: Callable | AsyncCallable    — Called when all alerts for rule are ack'd.
    - on_error: Callable | AsyncCallable      — (Ephemeral owner only) Called when evaluator throws.

@nats_subject import.{org_id}.{env}.alerts.listen.<rule_id>.*
@encoding msgpack

@callback_payloads

on_fire / on_resolved:
{
    "alert": {"id": str, "name": str, "type": str, "config": dict},
    "device_id": str,
    "rolling_state": dict,       # (ephemeral) or "last_value" (non-ephemeral)
    "timestamp": str             # ISO8601 (converted from unix ms)
}

on_ack:
{
    "status": "acknowledged",
    "device_ident": str,
    "ack": {"acked_by": str, "ack_notes": str | None, "acked_at": int}
}

on_ack_all:
{
    "status": "acknowledged",
    "ack": {"acked_by": str, "ack_notes": str | None, "acked_at": int}
}

@example
    await alert.listen({
        "on_fire": lambda data: print(f"ALERT FIRED on {data['device_id']}"),
        "on_resolved": lambda data: print("ALERT RESOLVED"),
        "on_ack": lambda data: print(f"Acked by {data['ack']['acked_by']}"),
        "on_ack_all": lambda data: print(f"All acked by {data['ack']['acked_by']}"),
    })
"""

# ─────────────────────────────────────────────────────────────
# alert.set_evaluator(fn)
# ─────────────────────────────────────────────────────────────

"""
@method alert.set_evaluator
@description Sets the client-side evaluator function for an EPHEMERAL alert.
             Must be called before .listen() to enter owner mode.

@param fn: Callable | AsyncCallable — Evaluator function.
    Supports both sync and async functions.
    Receives a single argument: the rolling state dict, structured as
    { device_ident: { metric: state_dict } }.
    Must return bool: True = breach, False = clear.

    Device IDs in the rolling state are resolved back to device_idents
    via resolve_ident_from_id() so the evaluator can use human-readable keys.

Rolling state contains RAW DATA from the source (keyed by device_ident, then metric):
    TELEMETRY: {"sensor_01": {"temperature": {"value": 90.5, "timestamp": 1711234567000}}}
    COMMAND:   {"sensor_01": {"reboot": <raw_command_data>}}
    EVENT:     {"sensor_01": {"door_opened": <raw_event_data>}}

Note: The state machine state (status, alerting_since, fire_count, etc.) is tracked
separately in _machine_state and is NOT passed to the evaluator.

@raises ValueError — If fn is not callable
@raises ValueError — If alert type is not EPHEMERAL

@example
    alert = await app.alert.create_ephemeral({...})
    alert.set_evaluator(lambda data: data.get("sensor_01", {}).get("cpu_usage", {}).get("last_value", 0) > 90)
"""

# ═════════════════════════════════════════════════════════════
# EPHEMERAL ALERT ENGINE (Client-Side)
# ═════════════════════════════════════════════════════════════

"""
@description
The ephemeral alert engine runs entirely client-side in the SDK.
Supports TELEMETRY, COMMAND, EVENT sources. Two modes:

MODE 1: OWNER (evaluator set via .set_evaluator() before .listen())
- Acquires NATS KV lock for single-owner enforcement
- Subscribes to data topic (JetStream consumer)
- Maintains rolling state, passes to evaluator on each message
- State machine fires/resolves locally -> callbacks AND publishes events
- Subscribes to RPC subjects via nc.subscribe() for remote ack/ack_all/mute

MODE 2: LISTENER (no evaluator set)
- Subscribes to import.{org_id}.{env}.alerts.listen.{rule_id}.* (JetStream)
- Routes events to callbacks by last token
- For ack/ack_all: sends NATS request (RPC) to owner

DATA TOPIC CONSTRUCTION:
| Source    | Subject                                                     |
|-----------|-------------------------------------------------------------|
| TELEMETRY | {org_id}.{env}.telemetry.{device_id|*}.{last_token}        |
| COMMAND   | {org_id}.{env}.command.queue.{device_id|*}.{last_token}    |
| EVENT     | {org_id}.{env}.events.{device_id|*}.{last_token}           |

ROLLING STATE (raw source data, keyed by device_ident -> metric):
    TELEMETRY: { "sensor_01": { "temperature": { "value": 90.5, "timestamp": 1711234567000 } } }
    COMMAND:   { "sensor_01": { "reboot": <raw_command_data> } }
    EVENT:     { "sensor_01": { "door_opened": <raw_event_data> } }

MACHINE STATE (internal state machine, keyed by device_id:metric):
    { "dev-id-1:temperature": { "status": "normal", "alerting_since": None, ... } }
    Not exposed to evaluator or callbacks.

ALERT PAYLOAD (passed to callbacks and published via msgpack):
    {
        "alert": {
            "id": str,                 # Rule ID
            "name": str,               # Rule name
            "type": str,               # Source type (TELEMETRY/COMMAND/EVENT)
            "config": dict,            # Full rule config
        },
        "device_id": str,              # Device ID from the NATS subject
        "rolling_state": dict,         # Copy of the full rolling state (raw source data)
        "timestamp": int,              # Unix timestamp in ms
    }

NATS KV LOCK (Owner Mode Only):
    Bucket: {org_id}
    Key: ephemeral_owner_{rule_id}
    Value: {"started_at": int, "expires_at": int} (JSON)
    TTL: 30 seconds, heartbeat every 15 seconds via asyncio.Task

STATE MACHINE:
    States: "normal" | "alerting" | "acknowledged"

    State dict (create_fresh_state()):
    {
        "status": "normal",
        "last_value": None,
        "fired_at": None,
        "resolved_at": None,
        "acked_at": None,
        "acked_by": None,
        "ack_notes": None,
        "fire_count": 0,
        "alerting_since": None,
        "recovery_since": None,
        "last_fire_time": None,
    }

EVALUATION FLOW (per incoming data message):
1. Decode msgpack, ack message
2. Update rolling state
3. Check mute -> skip if muted
4. Check staleness -> reset if gap > duration
5. Run evaluator(rolling_state) -> bool
   - If throws: call on_error(err), skip cycle
   - If non-bool: call on_error(TypeError), skip cycle
6. State transitions:
   BREACHED: track breached_since, fire if duration met, re-fire if cooldown elapsed
   CLEAR: track clear_since, resolve if recovery_duration met

NOTIFICATION DISPATCH SUBJECT: api.iot.alerts.{org_id}.dispatch

On FIRE: publish event, dispatch notifications, invoke on_fire
On RESOLVED: publish event, dispatch notifications, invoke on_resolved, reset state
On ACK: update state, publish event, invoke on_ack
On ACK_ALL: update state, publish event, invoke on_ack_all

STATE TRANSITION TABLE:
| Current       | Condition                      | Duration Met? | Next State    | Action         |
|---------------|--------------------------------|---------------|---------------|----------------|
| normal        | breach, held < duration         | No            | normal        | Track          |
| normal        | breach, held >= duration        | Yes           | alerting      | FIRE           |
| alerting      | breach, cooldown elapsed        | -             | alerting      | Re-FIRE        |
| alerting      | breach, cooldown not elapsed    | -             | alerting      | Silent         |
| alerting      | acked                           | -             | acknowledged  | Silent         |
| alerting      | clear, held < recovery          | -             | alerting      | Track          |
| alerting      | clear, held >= recovery         | -             | normal        | RESOLVED       |
| acknowledged  | breach                          | -             | acknowledged  | Silent         |
| acknowledged  | clear, held < recovery          | -             | acknowledged  | Track          |
| acknowledged  | clear, held >= recovery         | -             | normal        | RESOLVED       |
"""
