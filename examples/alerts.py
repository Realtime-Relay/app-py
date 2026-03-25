"""
Alerts Example
==============

Demonstrates alert CRUD (create, get, update, list, delete) for
THRESHOLD type alerts.

Usage:
    python examples/alerts.py

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


    # ── List existing alerts ──────────────────────────────────

    alerts = await app.alert.list()

    print(f'Found {len(alerts)} alert(s):')
    for a in alerts:
        print(f"  {a.get('name', '?')} — {a.get('type', '?')} (id: {a.get('id', '?')})")
    print()


    # ── Create a threshold alert ──────────────────────────────

    alert = await app.alert.create({
        'name': 'high-temp',
        'type': 'THRESHOLD',
        'metric': 'temperature',
        'config': {
            'scope': {'type': 'DEVICE', 'value': 's-3'},
            'operator': '>',
            'value': 85,
            'duration': 300,
            'recovery_duration': 120,
            'cooldown': 600,
        },
    })

    if alert:
        print('Created alert:')
        print(json.dumps(dict(alert.items()), indent=4))
    else:
        print('Alert creation failed.')
    print()

    alert_id = alert.get('id') if alert else None


    # ── Get the alert by name ─────────────────────────────────

    fetched = await app.alert.get({'name': 'high-temp'})

    if fetched:
        print('Fetched alert:')
        print(json.dumps(dict(fetched.items()), indent=4))
    print()


    # ── Update the alert ──────────────────────────────────────

    if alert_id:
        updated = await app.alert.update({
            'id': alert_id,
            'name': 'high-temp',
            'config': {
                'scope': {'type': 'DEVICE', 'value': 's-3'},
                'operator': '>',
                'value': 90,
                'duration': 300,
                'recovery_duration': 120,
                'cooldown': 600,
            },
        })

        if updated:
            print('Updated alert (threshold 85 -> 90):')
            print(json.dumps(dict(updated.items()), indent=4))
        print()


    # ── List again to confirm ─────────────────────────────────

    alerts = await app.alert.list()

    print(f'Alerts after create + update ({len(alerts)}):')
    for a in alerts:
        print(f"  {a.get('name', '?')} — {a.get('type', '?')} (id: {a.get('id', '?')})")
    print()


    # ── Delete the alert ──────────────────────────────────────

    if alert_id:
        deleted = await app.alert.delete({'id': alert_id})
        print(f'Deleted alert: {deleted}\n')


    # ── Cleanup ───────────────────────────────────────────────

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
