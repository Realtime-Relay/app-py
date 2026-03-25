"""
Telemetry Reconnection Test
============================

Tests that telemetry streams survive a NATS reconnection.

Steps:
    1. Connects and subscribes to telemetry for s-3
    2. Runs indefinitely — you should see telemetry data arriving
    3. Kill the NATS server or disconnect the network
    4. Watch for: [connection] reconnecting → [connection] reconnected
    5. After reconnect, telemetry data should resume automatically

Press Ctrl+C to stop.

Usage:
    python examples/reconnection/telemetry_reconnect.py
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
    print('Connected. Streaming telemetry from s-3...')
    print('Kill the NATS connection to test reconnection.\n')


    # ── Stream telemetry ───────────────────────────────────────

    def on_telemetry(data):
        print(f'  [telemetry] {data}')

    await app.telemetry.stream({
        'device_ident': 's-3',
        'metric': '*',
        'callback': on_telemetry,
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
