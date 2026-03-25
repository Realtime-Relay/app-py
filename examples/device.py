"""
Device Example
==============

Demonstrates device CRUD operations: list, get, create, update, and delete.

Usage:
    python examples/device.py

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


    # ── List all devices ──────────────────────────────────────

    devices = await app.device.list()

    print(f'Found {len(devices)} device(s):')
    for d in devices:
        print(f"  {d['ident']} (id: {d['id']})")
    print()


    # ── Get a single device ───────────────────────────────────

    device = await app.device.get({'ident': 's-3'})

    print('Device s-3:')
    print(json.dumps(device, indent=4))
    print()


    # ── Create a new device ───────────────────────────────────

    new_device = await app.device.create({
        'ident': 'example-sensor',
        'schema': {
            'temperature': {'type': 'number', 'unit': 'celsius'},
            'humidity': {'type': 'number', 'unit': 'percent'},
        },
        'config': {
            'reporting_interval': 60,
        },
    })

    if new_device:
        print('Created device:')
        print(json.dumps(new_device, indent=4))
    else:
        print('Device creation failed (may already exist).')
    print()


    # ── Update the device ─────────────────────────────────────

    if new_device:
        updated = await app.device.update({
            'id': new_device['id'],
            'config': {
                'reporting_interval': 30,
                'mode': 'low-power',
            },
        })

        print('Updated device:')
        print(json.dumps(updated, indent=4))
        print()


    # ── Delete the device ─────────────────────────────────────

    deleted = await app.device.delete('example-sensor')
    print(f'Deleted example-sensor: {deleted}\n')


    # ── Cleanup ───────────────────────────────────────────────

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
