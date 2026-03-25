"""
Group Stream Reconnection Test
================================

Tests that logical group streams survive a NATS reconnection.

Steps:
    1. Connects and streams telemetry from a logical group
    2. Runs indefinitely — you should see group telemetry data
    3. Kill the NATS server or disconnect the network
    4. Watch for: [connection] reconnecting → [connection] reconnected
    5. After reconnect, group stream should resume automatically

Press Ctrl+C to stop.

Usage:
    python examples/reconnection/group_stream_reconnect.py
"""

import asyncio
import os

from relayx_app_sdk import RelayApp


API_KEY = os.environ.get('RELAY_API_KEY', '')
SECRET = os.environ.get('RELAY_SECRET', '')


async def main():

    app = RelayApp({
        'api_key': API_KEY,
        'secret': SECRET,
        'mode': 'test',
    })

    app.connection.listeners(lambda event: print(f'\n  ** [connection] {event} **\n'))

    await app.connect()
    print('Connected.\n')


    # ── Create or get a logical group ──────────────────────────

    group = await app.logical_group.create({
        'name': 'reconnect-test-group',
        'tags': ['reconnect-test'],
        'devices': ['s-3'],
    })

    if not group:
        groups = await app.logical_group.list()
        for g in (groups or []):
            if g.get('name') == 'reconnect-test-group':
                group = g
                break

    if not group:
        print('Failed to create or find group.')
        await app.disconnect()
        return

    print(f'Group: {group.get("name")} (id: {group.get("id")})')


    # ── Stream group telemetry ─────────────────────────────────

    def on_group_data(data):
        print(f'  [group] {data}')

    group.stream({
        'callback': on_group_data,
    })

    print('Streaming group data. Kill the NATS connection to test reconnection.\n')


    # ── Run until Ctrl+C ──────────────────────────────────────

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print('\nStopping...')

    # Cleanup
    await app.logical_group.delete(group['id'])
    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
