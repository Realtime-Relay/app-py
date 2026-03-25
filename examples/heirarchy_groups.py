"""
Heirarchy Groups Example
========================

Demonstrates hierarchy group CRUD, listing groups, and listing devices in a group.

Usage:
    python examples/heirarchy_groups.py

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

    groups = await app.heirarchy_group.list()

    print(json.dumps(groups, indent=4))

    print(f'Found {len(groups)} hierarchy group(s):')
    for g in groups:
        print(f"  {g.get('name', '?')} — {g.get('heirarchy', '?')} (id: {g.get('id', '?')})")
    print()


    # ── Create a new group ────────────────────────────────────

    group = await app.heirarchy_group.create({
        'name': 'building-a-floor-1',
        'heirarchy': 'building-a.floor-1',
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
        fetched = await app.heirarchy_group.get(group['id'])

        print('Fetched group:')
        print(json.dumps(dict(fetched.items()), indent=4))
        print()


    # ── List devices in the group ─────────────────────────────

    if group:
        devices = await app.heirarchy_group.list_devices(group['id'])

        print(f'Devices in group ({len(devices)}):')
        for d in devices:
            print(f"  {d.get('ident', '?')} (id: {d.get('id', '?')})")
        print()


    # ── Update the group (hierarchy path only) ─────────────────
    # devices defaults to {add: [], remove: []}

    if group:
        updated = await app.heirarchy_group.update({
            'id': group['id'],
            'heirarchy': 'building-a.floor-2',
        })

        print('Updated group (hierarchy path only, devices defaults to empty):')
        print(json.dumps(dict(updated.items()), indent=4))
        print()


    # ── Update the group (devices + hierarchy) ────────────────

    if group:
        updated = await app.heirarchy_group.update({
            'id': group['id'],
            'heirarchy': 'building-a.floor-3',
            'devices': {
                'add': ['s-3'],
                'remove': [],
            },
        })

        print('Updated group (devices + hierarchy):')
        print(json.dumps(dict(updated.items()), indent=4))
        print()


    # ── Delete the group ──────────────────────────────────────

    if group:
        deleted = await app.heirarchy_group.delete(group['id'])
        print(f'Deleted group: {deleted}\n')


    # ── Cleanup ───────────────────────────────────────────────

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
