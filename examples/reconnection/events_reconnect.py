"""
Events Reconnection Test
=========================

Tests that event listeners survive a NATS reconnection.

Steps:
    1. Connects and subscribes to 'door_opened' events
    2. Runs indefinitely — you should see events arriving (run the event simulator)
    3. Kill the NATS server or disconnect the network
    4. Watch for: [connection] reconnecting → [connection] reconnected
    5. After reconnect, events should resume automatically

Press Ctrl+C to stop.

Usage:
    python examples/reconnection/events_reconnect.py
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
    print('Connected. Listening for door_opened events...')
    print('Run the event simulator and kill the NATS connection to test.\n')


    # ── Listen for events ──────────────────────────────────────

    def on_event(data):
        print(f'  [event] {data}')

    await app.events.on({
        'name': 'door_opened',
        'callback': on_event,
    })


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
