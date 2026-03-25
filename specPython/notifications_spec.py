"""
============================================================
NOTIFICATIONS SPEC — Notification Channel CRUD
============================================================

Covers: app.notification.create, update, delete, list, get

NOTE: Only EMAIL and WEBHOOK types can be created via the SDK.
"""

# ─────────────────────────────────────────────────────────────
# await app.notification.create(params)
# ─────────────────────────────────────────────────────────────

"""
@method notification.create
@description Creates a new notification channel (EMAIL or WEBHOOK).

@param params: dict
    - name: str     — Required. Channel name. [a-zA-Z0-9_-]+
    - type: str     — Required. "WEBHOOK" | "EMAIL"
    - config: dict  — Required. Type-specific configuration.

For type = "WEBHOOK":
    - config.endpoint: str           — Required. Webhook URL.
    - config.headers: dict           — Required. HTTP headers (key-value pairs).
    - config.retry_back_off: list[int] — Optional. Retry backoff intervals in ms.

For type = "EMAIL":
    - config.recipients: list[str]   — Required. List of email addresses.
    - config.subject: str            — Required. Email subject.
    - config.template: str           — Required. Handlebars template.

@raises ValueError — If name is missing or fails validation
@raises ValueError — If type is not "WEBHOOK" or "EMAIL"
@raises ValueError — If config is missing required fields for the type
@raises RuntimeError — If not connected

@nats_subject api.iot.notification.{org_id}.create
@nats_type request
@encoding JSON

@request_payload
# WEBHOOK:
{
    "name": str,
    "type": "WEBHOOK",
    "config": {
        "endpoint": str,
        "headers": dict,
        "retry_back_off": list[int] | None
    }
}
# EMAIL:
{
    "name": str,
    "type": "EMAIL",
    "config": {
        "recipients": list[str],
        "subject": str,
        "template": str
    }
}

@returns dict — Notification object on success

@example
    # Webhook
    notif = await app.notification.create({
        "name": "ops_webhook",
        "type": "WEBHOOK",
        "config": {
            "endpoint": "https://hooks.example.com/alerts",
            "headers": {"Authorization": "Bearer <token>"}
        }
    })

    # Email
    notif = await app.notification.create({
        "name": "ops_email",
        "type": "EMAIL",
        "config": {
            "recipients": ["ops@example.com", "alerts@example.com"],
            "subject": "Alert: {{rule.name}}",
            "template": "Device {{device_id}} triggered {{rule.name}} at {{timestamp}}"
        }
    })
"""

# ─────────────────────────────────────────────────────────────
# await app.notification.update(params)
# ─────────────────────────────────────────────────────────────

"""
@method notification.update
@description Updates an existing notification channel.
             Must send the ENTIRE config — not partial updates.

@param params: dict
    - name: str     — Required. Channel name.
    - type: str     — Required. "WEBHOOK" | "EMAIL"
    - config: dict  — Required. Complete config for the type.

@raises ValueError — If name is missing
@raises ValueError — If type is not "WEBHOOK" or "EMAIL"
@raises RuntimeError — If not connected

@nats_subject api.iot.notification.{org_id}.update
@returns dict — Updated notification object
"""

# ─────────────────────────────────────────────────────────────
# await app.notification.delete(notif_id)
# ─────────────────────────────────────────────────────────────

"""
@method notification.delete
@description Deletes a notification channel by ID.

@param notif_id: str — Required.

@nats_subject api.iot.notification.{org_id}.delete
@returns bool — True on successful deletion

@example
    deleted = await app.notification.delete("<notif_id>")
"""

# ─────────────────────────────────────────────────────────────
# await app.notification.list()
# ─────────────────────────────────────────────────────────────

"""
@method notification.list
@description Lists all notification channels for the org.

@nats_subject api.iot.notification.{org_id}.list
@returns list[dict]

@example
    notifs = await app.notification.list()
"""

# ─────────────────────────────────────────────────────────────
# await app.notification.get(notif_id)
# ─────────────────────────────────────────────────────────────

"""
@method notification.get
@description Gets a single notification channel by ID.

@param notif_id: str — Required.

@nats_subject api.iot.notification.{org_id}.get
@returns dict — Notification channel dict

@example
    notif = await app.notification.get("<notif_id>")
"""
