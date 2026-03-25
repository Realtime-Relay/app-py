"""
Offline Buffer Reconnection Test
==================================

Tests that commands sent while disconnected are buffered and flushed on reconnect.

Steps:
    1. Connects to NATS
    2. Kill the NATS server or disconnect the network
    3. Watch for: [connection] reconnecting
    4. The script sends a command every 3 seconds while offline — these get buffered
    5. Restore the NATS connection
    6. Watch for: [connection] reconnected
    7. Buffered commands should be flushed and sent

Press Ctrl+C to stop.

Usage:
    python examples/reconnection/offline_buffer_reconnect.py
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

    connected = True

    def on_connection(event):
        nonlocal connected
        print(f'\n  ** [connection] {event} **\n')

        if event == 'reconnecting':
            connected = False
        elif event == 'reconnected':
            connected = True

    app.connection.listeners(on_connection)

    await app.connect()
    print('Connected.')
    print('Kill the NATS connection, then watch commands buffer and flush on reconnect.\n')

    count = 0

    try:
        while True:
            count += 1
            status = 'ONLINE' if connected else 'OFFLINE (buffered)'

            result = await app.command.send({
                'device_ident': ['s-3'],
                'name': 'setConfig',
                'data': {'interval': count},
            })

            print(f'  [cmd #{count}] {status} → result: {result}')

            await asyncio.sleep(10)
    except KeyboardInterrupt:
        print('\nStopping...')

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
