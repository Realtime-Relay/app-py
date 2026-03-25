"""
Logical Groups Example
======================

Demonstrates logical group CRUD, listing groups, and listing devices in a group.

Usage:
    python examples/logical_groups.py

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


    # ── List existing groups ──────────────────────────────────

    groups = await app.logical_group.list()

    print(f'Found {len(groups)} group(s):')
    for g in groups:
        print(f"  {g.get('name', '?')} (id: {g.get('id', '?')})")
    print()


    # ── Create a new group ────────────────────────────────────

    group = await app.logical_group.create({
        'name': 'floor-2-sensors',
        'tags': ['building-a', 'floor-2'],
        'device_idents': ['s-3'],
    })

    if group:
        print('Created group:')
        print(json.dumps(dict(group.items()), indent=4))
    else:
        print('Group creation failed.')
    print()


    # ── Get the group back ────────────────────────────────────

    if group:
        fetched = await app.logical_group.get(group['id'])

        print('Fetched group:')
        print(json.dumps(dict(fetched.items()), indent=4))
        print()


    # ── List devices in the group ─────────────────────────────

    if group:
        devices = await app.logical_group.list_devices(group['id'])

        print(f'Devices in group ({len(devices)}):')
        for d in devices:
            print(f"  {d.get('ident', '?')} (id: {d.get('id', '?')})")
        print()


    # ── Update the group (tags only, devices defaults to empty) ─

    if group:
        updated = await app.logical_group.update({
            'id': group['id'],
            'tags': {
                'add': ['updated'],
                'remove': ['floor-2'],
            },
        })

        print('Updated group (tags only):')
        print(json.dumps(dict(updated.items()), indent=4))
        print()


    # ── Update the group (both tags and devices) ──────────────

    if group:
        updated = await app.logical_group.update({
            'id': group['id'],
            'tags': {
                'add': ['active'],
            },
            'devices': {
                'add': ['s-3'],
                'remove': [],
            },
        })

        print('Updated group (tags + devices):')
        print(json.dumps(dict(updated.items()), indent=4))
        print()


    # ── Delete the group ──────────────────────────────────────

    if group:
        deleted = await app.logical_group.delete(group['id'])
        print(f'Deleted group: {deleted}\n')


    # ── Cleanup ───────────────────────────────────────────────

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
