"""
Notifications Example
=====================

Demonstrates notification channel CRUD for both WEBHOOK and EMAIL types.

Usage:
    python examples/notifications.py

Requires:
    - RELAY_API_KEY environment variable
    - RELAY_SECRET environment variable
"""

import asyncio
import json
import os

from relayx_app_sdk import RelayApp


API_KEY = os.environ.get('RELAY_API_KEY', '')
SECRET = os.environ.get('RELAY_SECRET', '')


async def main():

    # ── Initialize ────────────────────────────────────────────

    app = RelayApp({
        'api_key': API_KEY,
        'secret': SECRET,
        'mode': 'test',
    })

    app.connection.listeners(lambda event: print(f'[connection] {event}'))

    await app.connect()
    print('Connected.\n')


    # ── List existing notifications ───────────────────────────

    notifications = await app.notification.list()

    print(f'Found {len(notifications)} notification(s):')
    for n in notifications:
        print(f"  {n.get('name', '?')} — {n.get('type', '?')} (id: {n.get('id', '?')})")

        deleted = await app.notification.delete(n["id"])
        print(f'Deleted notif: {deleted}')
    print()


    # ── Create a webhook notification ─────────────────────────

    webhook = await app.notification.create({
        'name': 'ops-webhook',
        'type': 'WEBHOOK',
        'config': {
            'endpoint': 'https://hooks.example.com/alerts',
            'headers': {
                'Authorization': 'Bearer my-token',
                'Content-Type': 'application/json',
            },
        },
    })

    print('Created webhook:')
    print(json.dumps(webhook, indent=4))
    print()

    webhook_id = webhook.get('id') if webhook else None


    # ── Create an email notification ──────────────────────────

    email = await app.notification.create({
        'name': 'ops-email',
        'type': 'EMAIL',
        'config': {
            'recipients': ['ops@example.com', 'alerts@example.com'],
            'subject': 'Alert: {{rule.name}}',
            'template': 'Device {{device_id}} triggered {{rule.name}} at {{timestamp}}',
        },
    })

    print('Created email:')
    print(json.dumps(email, indent=4))
    print()

    email_id = email.get('id') if email else None


    # ── Get a notification ────────────────────────────────────

    if webhook_id:
        fetched = await app.notification.get(webhook_id)

        print('Fetched webhook:')
        print(json.dumps(fetched, indent=4))
        print()


    # ── Update the webhook ────────────────────────────────────

    updated = await app.notification.update({
        'id': webhook_id,
        'name': 'ops-webhook',
        'type': 'WEBHOOK',
        'config': {
            'endpoint': 'https://hooks.example.com/v2/alerts',
            'headers': {
                'Authorization': 'Bearer new-token',
            },
        },
    })

    print('Updated webhook:')
    print(json.dumps(updated, indent=4))
    print()


    # ── Delete both notifications ─────────────────────────────

    if webhook_id:
        deleted = await app.notification.delete(webhook_id)
        print(f'Deleted webhook: {deleted}')

    if email_id:
        deleted = await app.notification.delete(email_id)
        print(f'Deleted email: {deleted}')

    print()


    # ── Cleanup ───────────────────────────────────────────────

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
