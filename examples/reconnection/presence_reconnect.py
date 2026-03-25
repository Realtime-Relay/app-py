"""
Presence Reconnection Test
===========================

Tests that presence subscriptions survive a NATS reconnection.

Steps:
    1. Connects and subscribes to presence events
    2. Runs indefinitely — you should see device connect/disconnect events
    3. Kill the NATS server or disconnect the network
    4. Watch for: [connection] reconnecting → [connection] reconnected
    5. After reconnect, presence events should resume automatically

Press Ctrl+C to stop.

Usage:
    python examples/reconnection/presence_reconnect.py
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
    print('Connected. Listening for presence events...')
    print('Connect/disconnect a device and kill the NATS connection to test.\n')


    # ── Listen for presence ────────────────────────────────────

    def on_presence(data):
        print(f'  [presence] {data}')

    await app.connection.presence(on_presence)


    # ── Run until Ctrl+C ──────────────────────────────────────

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print('\nStopping...')

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
