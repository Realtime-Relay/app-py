"""
Presence Example
================

Demonstrates how to listen for device presence events (online/offline).

Usage:
    python examples/presence.py

Requires:
    - RELAY_API_KEY environment variable
    - RELAY_SECRET environment variable
"""

import asyncio
import json
import os
import signal

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


    # ── Subscribe to presence events ──────────────────────────

    def on_presence(data):
        print(f'[presence] {json.dumps(data, indent=4)}')

    await app.connection.presence(on_presence)

    print('Listening for presence events...')
    print('Try turning a device on or off.')
    print('Press Ctrl+C to quit.\n')


    # ── Wait until Ctrl+C ─────────────────────────────────────

    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, stop.set)

    await stop.wait()


    # ── Cleanup ───────────────────────────────────────────────

    print('\nDisconnecting...')
    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
