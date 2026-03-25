"""
Ephemeral Alert — Owner Mode
=============================

Creates an ephemeral alert that monitors temperature AND humidity
from device s-3. The evaluator runs client-side — no backend evaluation.

This instance becomes the OWNER because set_evaluator() is called
before listen(). It subscribes to telemetry, runs the evaluator,
and manages the state machine.

Usage:
    python examples/ephemeral_alerts/owner.py

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

DEVICE_IDENT = 's-3'
TEMP_THRESHOLD = 85
HUMIDITY_THRESHOLD = 70


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


    # ── Get or create the ephemeral alert ─────────────────────

    eph_alert = await app.alert.get('ephemeral_multi_metric')

    if eph_alert is None:
        eph_alert = await app.alert.create_ephemeral({
            'name': 'ephemeral_multi_metric',
            'description': f'Client-side alert: temperature > {TEMP_THRESHOLD}°C AND humidity > {HUMIDITY_THRESHOLD}% on {DEVICE_IDENT}',
            'config': {
                'topic': {
                    'source': 'TELEMETRY',
                    'device_ident': DEVICE_IDENT,
                    'last_token': '*',
                },
                'duration': 5,
                'recovery_duration': 10,
                'cooldown': 10,
            },
            'notification_channel': [],
        })

    print(f"Ephemeral alert: {eph_alert.get('name')} (id: {eph_alert.get('id')})\n")


    # ── Set the evaluator ─────────────────────────────────────
    # Rolling state with last_token: "*" accumulates all metrics:
    # { "s-3": { "temperature": { "value": 90.5, ... }, "humidity": { "value": 75.2, ... } } }
    #
    # Breach condition: BOTH temperature > 85°C AND humidity > 70%

    def evaluator(data):
        """
        Receives the full rolling state — raw data from the source:
        {
            "s-3": {
                "temperature": { "value": 90.5, "timestamp": 1711234567000 },
                "humidity": { "value": 75.2, "timestamp": 1711234567000 },
            }
        }

        Returns True if the alert condition is breached.
        """
        device = data.get(DEVICE_IDENT)
        if not device:
            return False

        temp_data = device.get('temperature')
        humidity_data = device.get('humidity')

        temp = temp_data.get('value') if temp_data else None
        humidity = humidity_data.get('value') if humidity_data else None

        if temp is None or humidity is None:
            print(f"  [eval] Waiting for data... temp={temp or '?'} humidity={humidity or '?'}")
            return False

        temp_breached = temp > TEMP_THRESHOLD
        humidity_breached = humidity > HUMIDITY_THRESHOLD
        breached = temp_breached and humidity_breached

        temp_sign = '>' if temp_breached else '<='
        humidity_sign = '>' if humidity_breached else '<='
        result = 'BREACH' if breached else 'CLEAR'

        print(f"  [eval] temp={temp}°C {temp_sign} {TEMP_THRESHOLD}°C | humidity={humidity}% {humidity_sign} {HUMIDITY_THRESHOLD}% → {result}")

        return breached

    eph_alert.set_evaluator(evaluator)


    # ── Start listening ───────────────────────────────────────
    # Since set_evaluator() was called, this instance becomes the OWNER.

    await eph_alert.listen({
        'on_fire': lambda data: print(f"\n[FIRE] {json.dumps(data, indent=2)}"),
        'on_resolved': lambda data: print(f"\n[RESOLVED] {json.dumps(data, indent=2)}"),
        'on_ack': lambda data: print(f"\n[ACK] {data}"),
        'on_ack_all': lambda data: print(f"\n[ACK_ALL] {data}"),
        'on_error': lambda err: print(f"\n[EVALUATOR ERROR] {err}"),
    })

    print('Ephemeral alert engine started (owner mode).')
    print(f'Monitoring {DEVICE_IDENT}: temperature > {TEMP_THRESHOLD}°C AND humidity > {HUMIDITY_THRESHOLD}%')
    print('Run the device simulator to send telemetry data.\n')
    print('Press Ctrl+C to stop.\n')


    # ── Graceful shutdown on Ctrl+C ───────────────────────────

    stop_event = asyncio.Event()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, stop_event.set)

    await stop_event.wait()

    print('\nStopping ephemeral engine...')
    await eph_alert.stop()
    print('Engine stopped.')

    # Uncomment to delete the alert on exit:
    # await app.alert.delete(eph_alert['id'])

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
