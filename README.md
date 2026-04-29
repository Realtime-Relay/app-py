# RelayX App SDK for Python

Official Python SDK for building applications on the RelayX platform.

> **[View Full Documentation →](https://docs.relay-x.io/app-sdk/overview)**

## Installation

```bash
pip install relayx_app_sdk
```

## Quick Start

```python
import asyncio
from relayx_app_sdk import RelayApp

app = RelayApp({
    "api_key": "<YOUR_API_KEY>",
    "secret": "<YOUR_SECRET>",
    "mode": "production",
})

async def main():
    app.connection.listeners(lambda event: print(f"[connection] {event}"))
    await app.connect()

    await app.telemetry.stream({
        "device_ident": "sensor-1",
        "metric": "temperature",
        "callback": lambda data: print(f"temp: {data}"),
    })

    await app.log.stream({
        "device_ident": "sensor-1",
        "callback": lambda entry: print(f"[{entry['level']}] {entry['data']}"),
    })

    # ... your application logic ...

    await app.disconnect()

asyncio.run(main())
```

## Configuration

```python
app = RelayApp({
    "api_key": "<YOUR_API_KEY>",   # JWT credential from RelayX console
    "secret": "<YOUR_SECRET>",      # Secret key
    "mode": "production",           # "production" | "test"
    "debug": False,                 # Enable debug logging (default: False)
})
```

Get your credentials at [console.relay-x.io](https://console.relay-x.io).
<!-- TODO: Link to sign-up tutorial -->

## Connection

```python
await app.connect()
await app.disconnect()

# Connection lifecycle events
app.connection.listeners(lambda event: print(event))
# Events: "connected" | "disconnected" | "reconnecting" | "reconnected" | "auth_failed"
```

### Presence

Subscribe to device connect/disconnect events.

```python
async def on_presence(data):
    print(f"{data['device_ident']} {data['event']}")
    # data["event"]: "connected" | "disconnected"

await app.connection.presence(on_presence)

# Unsubscribe
await app.connection.presence_off()
```

## Devices

```python
# List all devices
devices = await app.device.list()

# Get a single device
device = await app.device.get({"ident": "sensor-1"})

# Create a device
device = await app.device.create({
    "ident": "sensor-1",
    "schema": {
        "temperature": {"type": "number", "unit": "Celsius", "unit_symbol": "°C"},
        "humidity": {"type": "number", "unit": "Percentage", "unit_symbol": "%"},
    },
    "config": {},
})

# Update a device
device = await app.device.update({
    "id": device["id"],
    "config": {"interval": 5000},
})

# Delete a device
await app.device.delete("sensor-1")
```

## Telemetry

### Live Streaming

Stream real-time telemetry from a device. The `metric` is validated against the device schema.

```python
# Stream a specific metric
await app.telemetry.stream({
    "device_ident": "sensor-1",
    "metric": "temperature",
    "callback": lambda data: print(f"[{data['metric']}] {data['data']}"),
})

# Stream all metrics
await app.telemetry.stream({
    "device_ident": "sensor-1",
    "metric": "*",
    "callback": lambda data: print(data),
})

# Unsubscribe from specific metrics
await app.telemetry.off({"device_ident": "sensor-1", "metric": ["temperature"]})

# Unsubscribe from all metrics for a device
await app.telemetry.off({"device_ident": "sensor-1"})
```

### History

Returns each requested field as a list of `{value, timestamp}` points.

```python
history = await app.telemetry.history({
    "device_ident": "sensor-1",
    "fields": ["temperature", "humidity"],
    "start": "2026-03-01T00:00:00.000Z",
    "end": "2026-03-25T00:00:00.000Z",
})
```

#### Aggregation

Bucket by time with `interval` + `aggregate_fn`. Both must be supplied
together. `interval` is a Flux duration (`"30s"`, `"5m"`, `"1h"`, `"1d"`).
`aggregate_fn` is one of:

| function | meaning |
|---|---|
| `mean` | arithmetic mean per bucket |
| `min` / `max` | extrema per bucket |
| `sum` | total per bucket |
| `count` | number of points per bucket |
| `first` / `last` | first or last point per bucket |
| `median` | median per bucket |
| `stddev` | standard deviation per bucket |

```python
# Hourly average temperature for the past day
hourly_avg = await app.telemetry.history({
    "device_ident": "sensor-1",
    "fields": ["temperature"],
    "start": "2026-04-28T00:00:00.000Z",
    "end": "2026-04-29T00:00:00.000Z",
    "interval": "1h",
    "aggregate_fn": "mean",
})

# Daily peak humidity for the past month
daily_max = await app.telemetry.history({
    "device_ident": "sensor-1",
    "fields": ["humidity"],
    "start": start, "end": end,
    "interval": "1d",
    "aggregate_fn": "max",
})
```

Numeric aggregates (`mean`, `min`, `max`, `sum`, `median`, `stddev`)
require numeric metric values; non-numeric points are ignored.
`count`, `first`, and `last` work on any value type.

### Latest

Fetches the most recent telemetry values (last 24 hours).

```python
latest = await app.telemetry.latest({
    "device_ident": "sensor-1",
    "fields": ["temperature", "humidity"],
})
```

## Commands

Send one-way commands to devices.

```python
# Send to one or more devices
result = await app.command.send({
    "name": "set_interval",
    "device_ident": ["sensor-1", "sensor-2"],
    "data": {"interval": 5000},
})
# result: {"sensor-1": {"sent": True}, "sensor-2": {"sent": True}}

# Command history
history = await app.command.history({
    "name": "set_interval",
    "device_idents": ["sensor-1"],
    "start": "2026-03-01T00:00:00.000Z",
    "end": "2026-03-25T00:00:00.000Z",
})
```

## RPC

Make request/reply calls to devices.

```python
response = await app.rpc.call({
    "device_ident": "sensor-1",
    "name": "get_status",
    "data": {"verbose": True},
    "timeout": 10,  # seconds (default: 10)
})
```

## Events

Subscribe to device-published events. `device_ident` accepts:

- `"*"` — all devices in your org
- `[ident]` — a single device
- `[a, b, …]` — a specific list of devices

The callback receives `{<device_ident>: <event_data>}` so you always
know which device fired the event.

```python
# One device
await app.events.stream({
    "name": "door_opened",
    "device_ident": ["entry-sensor"],
    "callback": lambda payload: print(payload),
})

# All devices
await app.events.stream({
    "name": "boot",
    "device_ident": "*",
    "callback": lambda payload: print(payload),
})

await app.events.off({"name": "door_opened"})
```

### History

```python
events = await app.events.history({
    "device_ident": "sensor-1",
    "event_names": ["door_opened", "boot"],
    "start": "2026-03-01T00:00:00.000Z",
    "end": "2026-03-25T00:00:00.000Z",
})
```

## Alerts

### CRUD

```python
# Create a threshold alert
alert = await app.alert.create({
    "name": "high-temp",
    "type": "THRESHOLD",  # "THRESHOLD" | "RATE_CHANGE"
    "metric": "temperature",
    "config": {"threshold": 85, "duration": 5},
    "notification_channel": ["ops-webhook"],
})

# Get, update, delete
alert = await app.alert.get("high-temp")
alert = await app.alert.update({"id": alert["id"], "config": {"threshold": 90}})
await app.alert.delete(alert["id"])

# List all alerts
alerts = await app.alert.list()
```

### Listening

```python
alert = await app.alert.get("high-temp")

await alert.listen({
    "on_fire": lambda data: print("FIRED:", data),
    "on_resolved": lambda data: print("RESOLVED:", data),
    "on_ack": lambda data: print("ACK:", data),
})
```

Each fire / resolved / ack event carries an `incident_id` that's stable
across the lifetime of an alerting episode — minted when the alert
goes from `normal → alerting`, persisted across cooldown re-fires and
acks, and cleared only on resolution. Use it to group related events.

### History

History fires through the same streaming protocol as telemetry/events
and supports filtering by alert state (`fire`, `resolved`, `ack`) and
optionally by `incident_id`.

```python
history = await app.alert.history({
    "rule_type": "RULE",  # "RULE" | "DEVICE"
    "rule_id": alert["id"],
    "rule_states": ["fire", "resolved", "ack"],
    "start": "2026-03-01T00:00:00.000Z",
    "end": "2026-03-25T00:00:00.000Z",
})

# Walk a single incident end-to-end
incident = await app.alert.history({
    "rule_type": "DEVICE",
    "device_ident": "sensor-1",
    "incident_id": "<incident_uuid>",
    "start": start,
    "end": end,
})
```

### Acknowledge

`device_id` is required — it identifies which device's incident gets
acknowledged. After ack, cooldown re-fires for the same incident are
recorded for audit but do not dispatch notifications until the alert
resolves and a new incident begins.

```python
await app.alert.ack({
    "device_id": "<device_id>",
    "alert_id": alert["id"],
    "acked_by": "operator-1",
    "ack_notes": "Investigating",
})
```

### Mute / Unmute

```python
await app.alert.mute({
    "id": alert["id"],
    "mute_config": {"type": "FOREVER"},
    # or {"type": "TIME_BASED", "mute_till": "2026-04-01T00:00:00.000Z"}
})

await app.alert.unmute(alert["id"])
```

## Ephemeral Alerts

Ephemeral alerts let you define custom alert rules that are evaluated client-side with your own logic. See the full guide: [Ephemeral Alerting Guide](docs/ephemeral-alerting.md).

```python
# Create an ephemeral alert
alert = await app.alert.create_ephemeral({
    "name": "custom-temp-alert",
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

# Set your evaluator
alert.set_evaluator(
    lambda state: state.get("sensor-1", {}).get("temperature", {}).get("value", 0) > 85
)

# Start monitoring
await alert.listen({
    "on_fire": lambda data: print("ALERT:", data),
    "on_resolved": lambda data: print("RESOLVED:", data),
})

# Stop
await alert.stop()
```

## Logs

Subscribe to live device logs and query history. Each log entry carries
a `level` (`info` | `warn` | `error`), a `data` payload, and a
device-side `timestamp`.

> **Note:** When running an interactive REPL or any prompt-driven
> example, wrap blocking calls like `input()` with
> `await asyncio.to_thread(input, ...)`. A bare `input()` inside an
> `async def` blocks the whole event loop and the log dispatch task
> will starve.

### Live Streaming

```python
# All levels
await app.log.stream({
    "device_ident": "sensor-1",
    "callback": lambda entry: print(f"[{entry['level']}] {entry['data']}"),
})

# Errors only
await app.log.stream({
    "device_ident": "sensor-1",
    "levels": ["error"],
    "callback": lambda entry: print("ERROR:", entry["data"]),
})

await app.log.off({"device_ident": "sensor-1"})
```

### History

Returns logs grouped by level. Optionally bucket with `interval` +
`aggregate_fn="count"` for per-level counts over time.

```python
logs = await app.log.history({
    "device_ident": "sensor-1",
    "start": "2026-04-28T00:00:00.000Z",
    "end": "2026-04-29T00:00:00.000Z",
})
# {"info": [...], "warn": [...], "error": [...]}

# Hourly error counts
hourly = await app.log.history({
    "device_ident": "sensor-1",
    "levels": ["error"],
    "start": start,
    "end": end,
    "interval": "1h",
    "aggregate_fn": "count",
})
```

## Logical Groups

Group devices by tags for batch operations and streaming.

```python
# Create
group = await app.logical_group.create({
    "name": "floor-1-sensors",
    "tags": ["floor_1", "temperature"],
    "device_idents": ["sensor-1", "sensor-2"],
})

# Update membership
group = await app.logical_group.update({
    "id": group["id"],
    "devices": {"add": ["sensor-3"], "remove": ["sensor-1"]},
    "tags": {"add": ["humidity"], "remove": ["floor_1"]},
})

# List, get, delete
groups = await app.logical_group.list()
group = await app.logical_group.get(group["id"])
devices = await app.logical_group.list_devices(group["id"])
await app.logical_group.delete(group["id"])
```

### Group Streaming

Each group instance has `stream()` and `off()` methods.

```python
group = await app.logical_group.get("<group_id>")

await group.stream({
    "callback": lambda data: print(data),
})

await group.off()
```

## Hierarchy Groups

Organize devices in a hierarchy path (e.g., `building_1.floor_2.zone_a`).

```python
# Create
group = await app.heirarchy_group.create({
    "name": "zone-a-sensors",
    "heirarchy": "building_1.floor_2.zone_a",
    "device_idents": ["sensor-1", "sensor-2"],
})

# Update
group = await app.heirarchy_group.update({
    "id": group["id"],
    "devices": {"add": ["sensor-3"], "remove": []},
    "heirarchy": "building_1.floor_3.zone_a",
})

# List, get, delete
groups = await app.heirarchy_group.list()
group = await app.heirarchy_group.get(group["id"])
devices = await app.heirarchy_group.list_devices(group["id"])
await app.heirarchy_group.delete(group["id"])
```

### Hierarchy Group Streaming

Supports metric and hierarchy path filtering with wildcards.

```python
group = await app.heirarchy_group.get("<group_id>")

# Stream all data
await group.stream({"callback": lambda data: print(data)})

# Filter by metric
await group.stream({
    "metric": "temperature",
    "callback": lambda data: print(data),
})

# Filter by hierarchy path (supports * and > wildcards)
await group.stream({
    "heirarchy": "building_1.*.zone_a",
    "callback": lambda data: print(data),
})

await group.off()
```

## Notifications

Create webhook or email notification channels for alerts.

```python
# Webhook
notif = await app.notification.create({
    "name": "ops-webhook",
    "type": "WEBHOOK",
    "config": {"endpoint": "https://hooks.example.com/alerts"},
})

# Email
notif = await app.notification.create({
    "name": "ops-email",
    "type": "EMAIL",
    "config": {
        "recipients": ["ops@example.com"],
        "subject": "Alert Notification",
        "template": "Alert {{alert_name}} fired on {{device_ident}}",
    },
})

# Update, delete, list, get
notif = await app.notification.update({
    "name": "ops-webhook",
    "type": "WEBHOOK",
    "config": {"endpoint": "https://new-url.com"},
})
await app.notification.delete(notif["id"])
notifs = await app.notification.list()
notif = await app.notification.get("<notif_id>")
```

## Offline Behavior

- **Commands**: Buffered in memory while disconnected and flushed automatically on reconnect.
- **Subscriptions**: All active telemetry, event, presence, alert, and group stream subscriptions are automatically restored on reconnect.
- **Ephemeral Alerts**: Alert state events (fire, resolved, ack) are buffered and published on reconnect.

## Error Handling

The SDK raises standard Python exceptions:

- `ValueError` — Invalid arguments, missing required fields, schema validation failures
- `RuntimeError` — Operations attempted while disconnected

```python
try:
    await app.telemetry.stream({
        "device_ident": "sensor-1",
        "metric": "nonexistent",
        "callback": lambda d: None,
    })
except ValueError as e:
    print(f"Validation error: {e}")
```

## License

Apache-2.0
