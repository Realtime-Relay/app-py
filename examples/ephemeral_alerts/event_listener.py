"""
Ephemeral Alert — Event Listener Mode
========================================

Subscribes to alert events published by the event owner instance
without running the evaluator. Includes an interactive menu
for acknowledging and muting alerts.

Usage:
    python examples/ephemeral_alerts/event_listener.py

Requires:
    - RELAY_API_KEY environment variable
    - RELAY_SECRET environment variable
    - event_owner.py must be running first
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from relayx_app_sdk import RelayApp


API_KEY = os.environ.get('RELAY_API_KEY', '')
SECRET = os.environ.get('RELAY_SECRET', '')

ALERT_NAME = 'ephemeral_event_door'


def print_menu():
    print('\n─── Commands ───────────────────')
    print('  1  → ack')
    print('  2  → ack_all')
    print('  3  → mute (FOREVER)')
    print('  4  → mute (TIME_BASED, 60s)')
    print('  5  → unmute')
    print('  q  → quit')
    print('────────────────────────────────')


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


    # ── Get the ephemeral alert (created by event_owner.py) ───

    eph_alert = await app.alert.get(ALERT_NAME)

    if not eph_alert:
        print(f'Alert "{ALERT_NAME}" not found. Run event_owner.py first.')
        await app.disconnect()
        return

    alert_id = eph_alert['id']
    print(f"Found alert: {eph_alert.get('name')} (id: {alert_id})\n")


    # ── Track the last known device_id from fire events ───────

    device_id_holder = {'value': None}


    # ── Listen in LISTENER mode (no set_evaluator) ────────────

    def on_fire(data):
        print(f"\n[FIRE] {json.dumps(data, indent=2)}")
        device_id_holder['value'] = data.get('device_id')
        print_menu()

    def on_resolved(data):
        print(f"\n[RESOLVED] {json.dumps(data, indent=2)}")
        device_id_holder['value'] = data.get('device_id')
        print_menu()

    def on_ack(data):
        print(f"\n[ACK] {json.dumps(data, indent=2)}")
        print_menu()

    def on_ack_all(data):
        print(f"\n[ACK_ALL] {json.dumps(data, indent=2)}")
        print_menu()

    await eph_alert.listen({
        'on_fire': on_fire,
        'on_resolved': on_resolved,
        'on_ack': on_ack,
        'on_ack_all': on_ack_all,
    })

    print('Listening for event alert events (listener mode).')
    print_menu()


    # ── Interactive input loop ────────────────────────────────

    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    running = True

    while running:
        try:
            line = await reader.readline()
            if not line:
                break

            cmd = line.decode().strip()

            if cmd == '1':
                device_id = device_id_holder['value']
                if not device_id:
                    print('No device_id yet — wait for a fire event first.')
                    continue

                print('Sending ack...')
                result = await app.alert.ack({
                    'device_id': device_id,
                    'alert_id': alert_id,
                    'acked_by': 'listener_operator',
                    'ack_notes': 'Acked from event listener terminal',
                })
                print(f'Ack result: {result}')

            elif cmd == '2':
                print('Sending ack_all...')
                result = await app.alert.ack_all({
                    'alert_id': alert_id,
                    'acked_by': 'listener_operator',
                    'ack_notes': 'AckAll from event listener terminal',
                })
                print(f'AckAll result: {result}')

            elif cmd == '3':
                print('Muting FOREVER...')
                result = await app.alert.mute({
                    'id': alert_id,
                    'mute_config': {'type': 'FOREVER'},
                })
                print(f'Mute result: {result}')

            elif cmd == '4':
                now = datetime.now(timezone.utc)
                mute_till = (now + timedelta(seconds=60)).strftime(
                    '%Y-%m-%dT%H:%M:%S.'
                ) + f'{now.microsecond // 1000:03d}Z'

                print(f'Muting TIME_BASED until {mute_till}...')
                result = await app.alert.mute({
                    'id': alert_id,
                    'mute_config': {'type': 'TIME_BASED', 'mute_till': mute_till},
                })
                print(f'Mute result: {result}')

            elif cmd == '5':
                print('Unmuting...')
                result = await app.alert.unmute({'id': alert_id})
                print(f'Unmute result: {result}')

            elif cmd == 'q':
                running = False

            else:
                print(f'Unknown command: "{cmd}"')

        except Exception as err:
            print(f'Error: {err}')


    # ── Cleanup ───────────────────────────────────────────────

    print('Stopping...')
    await eph_alert.stop()
    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
