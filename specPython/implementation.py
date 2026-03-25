"""
============================================================
IMPLEMENTATION — Quick Reference Guide (Python)
============================================================

Shows practical usage of all SDK methods.
"""

# ─── Initialization ───

app = RelayApp({
    "api_key": "",
    "secret": "",
    "mode": "production"  # or "test"
})

app.connection.listeners(lambda event: print(event))
# "connected" | "disconnected" | "reconnecting" | "reconnected" | "auth_failed"

await app.connect()
await app.disconnect()

# ─── TELEMETRY ───

await app.telemetry.stream({
    "device_ident": "<ident>",
    "metric": "<metric or *>",
    "callback": lambda data: print(data)
    # {"metric": "<metric name>", "value": "<value>", "timestamp": <unix timestamp>}
})

await app.telemetry.off({"device_ident": "<ident>"})

telemetry_history = await app.telemetry.history({
    "device_ident": "<ident>",
    "fields": ["temperature"],
    "start": "ISO8601 datetime",
    "end": "ISO8601 datetime"
})

telemetry_latest = await app.telemetry.latest({
    "device_ident": "<ident>",
    "fields": ["temperature"]
})

# ─── COMMANDS ───

result = await app.command.send({
    "name": "<command_name>",
    "device_ident": ["<ident_1>"],
    "data": {}
})

command_history = await app.command.history({
    "name": "<command_name>",
    "device_idents": ["<ident_1>", "<ident_2>"],
    "start": "ISO8601 datetime",
    "end": "ISO8601 datetime"  # Optional, defaults to now()
})

# ─── RPC ───

response = await app.rpc.call({
    "device_ident": "<ident>",
    "name": "<rpc_name>",
    "timeout": 20,  # seconds
    "data": {}
})

# ─── DEVICES ───

device = await app.device.create({
    "ident": "<unique ident>",
    "schema": {},
    "config": {}
})

device = await app.device.update({
    "id": "<device_id>",
    "schema": {},
    "config": {}
})

deleted = await app.device.delete("<unique ident>")

devices = await app.device.list()

device = await app.device.get({"ident": "<unique ident>"})

# ─── PRESENCE ───

await app.connection.presence(lambda data: print(data))
await app.connection.presence_off()

# ─── EVENTS ───

await app.events.stream({
    "name": "<event_name>",
    "callback": lambda data: print(data)
})

await app.events.off({"name": "<event_name>"})

# ─── ALERTS ───

alert = await app.alert.create({
    "name": "<unique_name>",
    "description": "",  # optional
    "type": "THRESHOLD",  # THRESHOLD or RATE_CHANGE
    "metric": "<metric>",
    "config": {
        "scope": {
            "type": "DEVICE",  # DEVICE, LOGICAL_GROUP, HEIRARCHY
            "value": ""  # device_id or group_id
        },
        "operator": ">",  # >, >=, ==, <=, <
        "value": 85,
        "duration": 300,
        "recovery_duration": 120,
        "cooldown": 600
    },
    "notification_channel": ["notification_id"]
})

alert = await app.alert.update({
    "id": "<alert_id>",
    # alert config fields to update
})

deleted = await app.alert.delete("<alert_id>")

alerts = await app.alert.list()

alert = await app.alert.get("<alert_name>")

ack = await app.alert.ack({
    "device_id": "<device_id>",
    "alert_id": "<alert_id>",
    "acked_by": "<string>"
})

ack_all = await app.alert.ack_all({
    "alert_id": "<alert_id>",
    "acked_by": "<string>"
})

await app.alert.mute({
    "id": "<alert_id>",
    "mute_config": {
        "type": "FOREVER",  # or "TIME_BASED"
        "mute_till": "ISO8601 timestamp utc"  # Required for TIME_BASED
    }
})

await app.alert.unmute("<alert_id>")

await alert.listen({
    "on_fire": lambda data: print("FIRE", data),
    "on_resolved": lambda data: print("RESOLVED", data),
    "on_ack": lambda data: print("ACK", data),
    "on_ack_all": lambda data: print("ACK ALL", data),
})

# ─── EPHEMERAL ALERTS ───

ephemeral_alert = await app.alert.create_ephemeral({
    "name": "<unique_name>",
    "config": {
        "topic": {
            "source": "TELEMETRY",  # TELEMETRY, COMMAND, EVENT
            "device_ident": "<ident>",
            "last_token": "<metric_name>"
        },
        "duration": 30,
        "recovery_duration": 15,
        "cooldown": 60
    },
    "notification_channel": ["notif_id"]
})

ephemeral_alert.set_evaluator(lambda data: data.get("sensor_01", {}).get("cpu_usage", {}).get("value", 0) > 90)

await ephemeral_alert.listen({
    "on_fire": lambda d: print("FIRE", d),
    "on_resolved": lambda d: print("RESOLVED", d),
    "on_error": lambda e: print("ERROR", e),
})

# Get existing ephemeral alert (listener mode - no evaluator)
listener_alert = await app.alert.get("<alert_name>")
await listener_alert.listen({
    "on_fire": lambda d: print("FIRE", d),
    "on_resolved": lambda d: print("RESOLVED", d),
})

# ─── LOGICAL GROUPS ───

logical_group = await app.logical_group.create({
    "name": "<name>",
    "tags": ["<tag_1>"],
    "device_idents": ["<ident_1>"]
})

logical_group = await app.logical_group.update({
    "id": "<group_id>",
    "name": "<name>",
    "tags": {"add": ["<tag_1>"], "remove": ["<tag_2>"]},
    "devices": {"add": ["<ident_1>"], "remove": ["<ident_2>"]}
})

deleted = await app.logical_group.delete("<group_id>")

logical_groups = await app.logical_group.list()

devices = await app.logical_group.list_devices("<group_id>")

logical_group = await app.logical_group.get("<group_id>")

await logical_group.stream({
    "device_idents": ["<ident_1>"],  # optional filter
    "callback": lambda data: print(data)
})

# ─── HEIRARCHY GROUPS ───

heirarchy_group = await app.heirarchy_group.create({
    "name": "<name>",
    "heirarchy": "token.token",
    "device_idents": ["<ident_1>"]
})

heirarchy_group = await app.heirarchy_group.update({
    "id": "<group_id>",
    "name": "<name>",
    "heirarchy": "token.token",
    "devices": {"add": ["<ident_1>"], "remove": ["<ident_2>"]}
})

deleted = await app.heirarchy_group.delete("<group_id>")

heirarchy_groups = await app.heirarchy_group.list()

devices = await app.heirarchy_group.list_devices("<group_id>")

heirarchy_group = await app.heirarchy_group.get("<group_id>")

await heirarchy_group.stream({
    "device_idents": ["<ident_1>"],  # optional filter
    "heirarchy": "<wildcard_topic>",  # optional
    "metrics": ["<metric_1>"],  # metrics can't exist with metric
    # "metric": "*",  # metric can't exist with metrics
    "callback": lambda data: print(data)
})

# ─── NOTIFICATIONS ───

# Create webhook notification
notification = await app.notification.create({
    "name": "<name>",
    "type": "WEBHOOK",
    "config": {
        "endpoint": "<URL>",
        "headers": {"<header_name>": "<header_value>"}
    }
})

# Create email notification
notification = await app.notification.create({
    "name": "<name>",
    "type": "EMAIL",
    "config": {
        "recipients": ["<email_id_1>"],
        "subject": "<subject>",
        "template": "<template>"
    }
})

notification = await app.notification.update({
    "name": "<name>",
    "type": "<type>",
    "config": {}  # Complete config for the type
})

deleted = await app.notification.delete("<notif_id>")

notifs = await app.notification.list()

notification = await app.notification.get("<notif_id>")
