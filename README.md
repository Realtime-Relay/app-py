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

```python
history = await app.telemetry.history({
    "device_ident": "sensor-1",
    "fields": ["temperature", "humidity"],
    "start": "2026-03-01T00:00:00.000Z",
    "end": "2026-03-25T00:00:00.000Z",
})
```

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

Subscribe to device-published events.

```python
await app.events.stream({
    "name": "door_opened",
    "callback": lambda data: print(f"Event: {data}"),
})

await app.events.off({"name": "door_opened"})
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
    "on_ack_all": lambda data: print("ACK ALL:", data),
})
```

### History

```python
history = await app.alert.history({
    "rule_type": "RULE",  # "RULE" | "DEVICE"
    "rule_id": alert["id"],
    "rule_states": ["fire", "resolved"],
    "start": "2026-03-01T00:00:00.000Z",
    "end": "2026-03-25T00:00:00.000Z",
})
```

### Acknowledge

```python
# Acknowledge for a specific device
await app.alert.ack({
    "device_id": "<device_id>",
    "alert_id": alert["id"],
    "acked_by": "operator-1",
    "ack_notes": "Investigating",
})

# Acknowledge all instances
await app.alert.ack_all({
    "alert_id": alert["id"],
    "acked_by": "operator-1",
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
