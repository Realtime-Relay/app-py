"""
Alert Listen Reconnection Test
================================

Tests that alert.listen() subscriptions survive a NATS reconnection.

Steps:
    1. Connects and subscribes to alert events for an existing alert
    2. Runs indefinitely — you should see fire/resolved/ack events
    3. Kill the NATS server or disconnect the network
    4. Watch for: [connection] reconnecting → [connection] reconnected
    5. After reconnect, alert events should resume automatically

Press Ctrl+C to stop.

Usage:
    python examples/reconnection/alert_listen_reconnect.py
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


    # ── Find an alert to listen to ─────────────────────────────

    alerts = await app.alert.list()
    if not alerts:
        print('No alerts found. Create an alert first.')
        await app.disconnect()
        return

    alert = alerts[0]
    print(f'Listening to alert: {alert.get("name")} (id: {alert.get("id")})\n')


    # ── Listen for alert events ────────────────────────────────

    await alert['_listen']({
        'on_fire': lambda data: print(f'  [FIRE] {data}'),
        'on_resolved': lambda data: print(f'  [RESOLVED] {data}'),
        'on_ack': lambda data: print(f'  [ACK] {data}'),
        'on_ack_all': lambda data: print(f'  [ACK_ALL] {data}'),
    })

    print('Kill the NATS connection to test reconnection.\n')


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
