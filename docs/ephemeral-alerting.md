# Ephemeral Alerting Guide

Ephemeral alerts let you define custom alert rules with your own evaluation logic. Unlike standard alerts (THRESHOLD, RATE_CHANGE) which are evaluated server-side, ephemeral alerts run your evaluator function client-side, giving you full control over when alerts fire and resolve.
 
## Architecture: Owner & Listener

Ephemeral alerts use an **owner/listener** model:

- **Owner**: Subscribes to raw data (telemetry, events, or commands), runs your evaluator function, and publishes alert state changes (fire, resolved). Only one owner can run per alert rule at a time.
- **Listener**: Subscribes to alert state changes published by the owner. Multiple listeners can run simultaneously. Useful for dashboards or notification services that need to react to alerts without running the evaluation logic.

## Creating an Ephemeral Alert

```python
alert = await app.alert.create_ephemeral({
    "name": "high-temperature",
    "description": "Fires when temperature exceeds 85°C",
    "config": {
        "topic": {
            "source": "TELEMETRY",
            "device_ident": "sensor-1",
            "last_token": "temperature",
        },
        "duration": 5,
        "recovery_duration": 10,
        "recovery_eval_type": "VALUE",
    },
    "notification_channel": ["ops-webhook"],
})
```

### Config Reference

| Field | Type | Description |
|-------|------|-------------|
| `topic.source` | string | Data source: `"TELEMETRY"`, `"EVENT"`, or `"COMMAND"` |
| `topic.device_ident` | string | Device identifier to monitor |
| `topic.last_token` | string | Metric name (telemetry), event name (events), or command name (commands) |
| `duration` | number | Seconds the breach condition must hold before the alert fires |
| `recovery_duration` | number | Seconds the clear condition must hold before the alert resolves |
| `cooldown` | number | Minimum seconds between consecutive fires (optional) |
| `recovery_eval_type` | string | `"VALUE"` (default) or `"TIMER"` — see [Recovery Modes](#recovery-modes) |

### Updating

```python
alert = await app.alert.update_ephemeral({
    "id": alert["id"],
    "config": {"duration": 10, "recovery_duration": 20},
})
```

## Owner Mode

Set an evaluator function before calling `listen()` to run in owner mode. The evaluator receives the rolling state (accumulated data from the data source) and returns `True` (breached) or `False` (clear).

```python
alert = await app.alert.get("high-temperature")

# Define your evaluator
def evaluate(rolling_state):
    temp = rolling_state.get("sensor-1", {}).get("temperature", {}).get("value", 0)
    return temp > 85

alert.set_evaluator(evaluate)

await alert.listen({
    "on_fire": lambda data: print("ALERT FIRED:", data),
    "on_resolved": lambda data: print("ALERT RESOLVED:", data),
    "on_ack": lambda data: print("ACKNOWLEDGED:", data),
    "on_error": lambda err: print("ERROR:", err),
})
```

The evaluator is called every time new data arrives. The rolling state is a dict keyed by device identifier:

```python
# Rolling state structure
{
    "sensor-1": {
        "temperature": {
            "value": 87.5,
            "timestamp": 1711382400000,
        }
    }
}
```

### Stopping

```python
await alert.stop()
```

## Listener Mode

Call `listen()` without setting an evaluator to run in listener mode. You receive fire/resolved events published by the owner running elsewhere.

```python
alert = await app.alert.get("high-temperature")

await alert.listen({
    "on_fire": lambda data: print("FIRED:", data),
    "on_resolved": lambda data: print("RESOLVED:", data),
    "on_ack": lambda data: print("ACK:", data),
    "on_ack_all": lambda data: print("ACK ALL:", data),
})
```

## Recovery Modes

The `recovery_eval_type` config controls how alerts recover (transition from fired back to normal).

### VALUE (default)

Recovery happens when the evaluator returns `False` for `recovery_duration` seconds. This requires data to keep flowing — if data stops arriving, the evaluator never runs and the alert stays in the fired state.

**Best for**: Telemetry monitoring where sensors publish continuously. Example: temperature drops back below threshold.

```python
"config": {
    "recovery_eval_type": "VALUE",
    "recovery_duration": 10,  # evaluator must return False for 10s
}
```

### TIMER

Recovery happens automatically after `recovery_duration` seconds of silence (no new messages on the data topic). If data does arrive and the evaluator returns `False`, that also triggers recovery.

**Best for**: Event-based alerts where silence means "the problem stopped." Example: door opened events stop occurring.

```python
"config": {
    "recovery_eval_type": "TIMER",
    "recovery_duration": 30,  # no events for 30s = resolved
}
```

## Callbacks

All callbacks are optional and support both sync and async functions.

| Callback | Triggered When | Payload |
|----------|---------------|---------|
| `on_fire` | Alert transitions to fired state | Alert payload with rule, device, value, timestamp |
| `on_resolved` | Alert transitions back to normal | Alert payload with rule, device, value, timestamp |
| `on_ack` | A specific device instance is acknowledged | Ack payload with acked_by, notes |
| `on_ack_all` | All instances are acknowledged | Ack payload with acked_by, notes |
| `on_error` | An error occurs during evaluation | Error object |

```python
async def on_fire(data):
    print(f"Alert {data['alert']['name']} fired!")
    print(f"Value: {data['last_value']}")

await alert.listen({"on_fire": on_fire})
```

## Acknowledge & Mute

### Acknowledge

Acknowledge transitions the alert from `alerting` to `acknowledged`. The alert stays acknowledged until it resolves naturally.

```python
# Acknowledge for a specific device
await alert.ack("operator-1", "Looking into it")

# Acknowledge all instances
await alert.ack_all("operator-1", "Team notified")
```

### Mute / Unmute

Muting suppresses notification dispatch but the alert continues to evaluate and fire.

```python
# Mute forever
await app.alert.mute({
    "id": alert["id"],
    "mute_config": {"type": "FOREVER"},
})

# Mute for a duration
await app.alert.mute({
    "id": alert["id"],
    "mute_config": {
        "type": "TIME_BASED",
        "mute_till": "2026-03-26T00:00:00.000Z",
    },
})

# Unmute
await app.alert.unmute(alert["id"])
```

## Data Sources

### TELEMETRY

Monitor continuous numeric sensor data.

```python
"topic": {
    "source": "TELEMETRY",
    "device_ident": "sensor-1",
    "last_token": "temperature",  # metric name from device schema
}
```

### EVENT

Monitor discrete events published by devices.

```python
"topic": {
    "source": "EVENT",
    "device_ident": "sensor-1",
    "last_token": "door_opened",  # event name
}
```

### COMMAND

Monitor commands sent to devices.

```python
"topic": {
    "source": "COMMAND",
    "device_ident": "sensor-1",
    "last_token": "firmware_update",  # command name
}
```

## Example: Temperature Threshold

An owner monitors temperature and fires when it exceeds 85°C for 5 seconds. Resolves when it stays below 85°C for 10 seconds.

```python
import asyncio
from relayx_app_sdk import RelayApp

app = RelayApp({
    "api_key": "<YOUR_API_KEY>",
    "secret": "<YOUR_SECRET>",
    "mode": "test",
})

async def main():
    app.connection.listeners(lambda e: print(f"[connection] {e}"))
    await app.connect()

    # Fetch or create the ephemeral alert
    alert = await app.alert.get("high-temp-alert")

    if not alert:
        alert = await app.alert.create_ephemeral({
            "name": "high-temp-alert",
            "config": {
                "topic": {
                    "source": "TELEMETRY",
                    "device_ident": "sensor-1",
                    "last_token": "temperature",
                },
                "duration": 5,
                "recovery_duration": 10,
                "recovery_eval_type": "VALUE",
            },
        })

    # Set evaluator — fires when temp > 85
    alert.set_evaluator(
        lambda state: state.get("sensor-1", {}).get("temperature", {}).get("value", 0) > 85
    )

    # Start
    await alert.listen({
        "on_fire": lambda d: print(f"FIRE: temp={d['last_value']}"),
        "on_resolved": lambda d: print("RESOLVED"),
    })

    print("Monitoring... Press Ctrl+C to stop.")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass

    await alert.stop()
    await app.disconnect()

asyncio.run(main())
```

## Example: Event Counting (with TIMER Recovery)

An owner monitors door_opened events and fires when more than 3 occur. Uses TIMER recovery — resolves after 30 seconds of no events.

```python
import asyncio
from relayx_app_sdk import RelayApp

app = RelayApp({
    "api_key": "<YOUR_API_KEY>",
    "secret": "<YOUR_SECRET>",
    "mode": "test",
})

async def main():
    app.connection.listeners(lambda e: print(f"[connection] {e}"))
    await app.connect()

    alert = await app.alert.get("door-alert")

    if not alert:
        alert = await app.alert.create_ephemeral({
            "name": "door-alert",
            "config": {
                "topic": {
                    "source": "EVENT",
                    "device_ident": "sensor-1",
                    "last_token": "door_opened",
                },
                "duration": 0,
                "recovery_duration": 30,
                "recovery_eval_type": "TIMER",
            },
        })

    event_count = 0

    def evaluate(rolling_state):
        nonlocal event_count
        event_count += 1
        return event_count > 3

    alert.set_evaluator(evaluate)

    await alert.listen({
        "on_fire": lambda d: print(f"ALERT: {event_count} door events!"),
        "on_resolved": lambda d: print("RESOLVED: no events for 30s"),
    })

    print("Monitoring door events... Press Ctrl+C to stop.")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass

    await alert.stop()
    await app.disconnect()

asyncio.run(main())
```
