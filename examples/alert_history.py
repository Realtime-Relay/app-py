"""
Alert History Example
=====================

Demonstrates fetching alert history by device and by rule.

Usage:
    python examples/alert_history.py

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


    # ── List alerts to get a rule_id ──────────────────────────

    alerts = await app.alert.list()

    print(f'Found {len(alerts)} alert(s):')
    for a in alerts:
        print(f"  {a.get('name', '?')} — {a.get('type', '?')} (id: {a.get('id', '?')})")
    print()

    rule_id = alerts[0]['id'] if alerts else None


    # ── History by device ─────────────────────────────────────

    print('Fetching alert history for device s-3 (fire + resolved)...\n')

    device_history = await app.alert.history({
        'rule_type': 'DEVICE',
        'device_ident': 's-3',
        'rule_states': ['fire', 'resolved'],
        'start': '2024-01-01T00:00:00.000Z',
        'end': '2026-12-31T23:59:59.000Z',
    })

    print('Device history:')
    print(json.dumps(device_history, indent=4))
    print()


    # ── History by rule ───────────────────────────────────────

    if rule_id:
        print(f'Fetching alert history for rule {rule_id}...\n')

        rule_history = await app.alert.history({
            'rule_type': 'RULE',
            'rule_id': rule_id,
            'rule_states': ['fire', 'resolved', 'ack'],
            'start': '2024-01-01T00:00:00.000Z',
            'end': '2026-12-31T23:59:59.000Z',
        })

        print('Rule history:')
        print(json.dumps(rule_history, indent=4))
        print()

    else:
        print('No alerts found, skipping rule history.\n')


    # ── Cleanup ───────────────────────────────────────────────

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
