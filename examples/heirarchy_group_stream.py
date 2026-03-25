"""
Heirarchy Group Stream Example
===============================

Creates a hierarchy group, streams telemetry for 20 seconds,
then stops the stream, deletes the group, and disconnects.

Usage:
    python examples/heirarchy_group_stream.py

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


    # ── Clean up any leftover groups ──────────────────────────

    groups = await app.heirarchy_group.list()

    for group in groups:
        print(group)
        deleted = await app.heirarchy_group.delete(group['id'])
        print(f"Deleted: {deleted}")


    # ── Create a group ────────────────────────────────────────

    group = await app.heirarchy_group.create({
        'name': 'stream-test',
        'heirarchy': 'campus.building-a',
        'device_idents': ['s-3'],
    })

    print(f"Created group: {group.get('name')} — {group.get('heirarchy')} (id: {group['id']})\n")


    # ── Stream telemetry from the group ───────────────────────

    def on_data(data):
        ident = data.get('ident', '?')
        metric = data.get('metric', '?')
        print(f"[{ident}] {metric}: {json.dumps(data, indent=4)}")

    await group.stream({
        'callback': on_data,
        'device_idents': ['s-3'],
    })

    print('Listening for 20 seconds...\n')
    await asyncio.sleep(20)


    # ── Cleanup ───────────────────────────────────────────────

    print('Stopping stream...')
    await group.off()

    print('Verifying silence for 5 seconds...\n')
    await asyncio.sleep(5)

    deleted = await app.heirarchy_group.delete(group['id'])
    print(f'Deleted group: {deleted}')

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
