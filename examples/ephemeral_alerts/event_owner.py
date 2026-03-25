"""
Ephemeral Alert — Event-Based (Owner Mode)
============================================

Monitors "door_opened" events from device s-3.
Fires if the door is opened more than 3 times within the rolling window.

The rolling state for EVENT sources stores the raw event data:
{
    "s-3": {
        "door_opened": { ... raw event payload ... }
    }
}

Since each event message overwrites the previous one in rolling state,
this example tracks a local counter to accumulate event occurrences.

Usage:
    python examples/ephemeral_alerts/event_owner.py

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
EVENT_NAME = 'door_opened'
MAX_OPENS = 3


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

    eph_alert = await app.alert.get({'name': 'ephemeral_event_door'})

    if eph_alert is None:
        eph_alert = await app.alert.create_ephemeral({
            'name': 'ephemeral_event_door',
            'description': f'Client-side alert: {EVENT_NAME} event from {DEVICE_IDENT}',
            'config': {
                'topic': {
                    'source': 'EVENT',
                    'device_ident': DEVICE_IDENT,
                    'last_token': EVENT_NAME,
                },
                'duration': 0,
                'recovery_duration': 30,
                'cooldown': 15,
            },
            'notification_channel': [],
        })

    print(f"Ephemeral alert: {eph_alert.get('name')} (id: {eph_alert.get('id')})\n")


    # ── Set the evaluator ─────────────────────────────────────

    event_counter = {'count': 0}

    def evaluator(data):
        """
        Receives the rolling state — raw event data:
        {
            "s-3": {
                "door_opened": { "action": "open", "zone": "warehouse", ... }
            }
        }

        Each new event overwrites the previous in rolling state.
        We track a local counter to count occurrences.
        """

        device = data.get(DEVICE_IDENT)
        if not device:
            return False

        event_data = device.get(EVENT_NAME)
        if not event_data:
            return False

        event_counter['count'] += 1
        count = event_counter['count']

        breached = count > MAX_OPENS
        result = 'BREACH' if breached else 'CLEAR'

        print(f"  [eval] {EVENT_NAME} count={count} (threshold={MAX_OPENS}) → {result}")

        return breached

    eph_alert.set_evaluator(evaluator)


    # ── Start listening ───────────────────────────────────────

    await eph_alert.listen({
        'on_fire': lambda data: print(f"\n[FIRE] {json.dumps(data, indent=2)}"),
        'on_resolved': lambda data: print(f"\n[RESOLVED] {json.dumps(data, indent=2)}"),
        'on_error': lambda err: print(f"\n[EVALUATOR ERROR] {err}"),
    })

    print('Ephemeral alert engine started (owner mode — EVENT source).')
    print(f'Monitoring {DEVICE_IDENT}: fires when {EVENT_NAME} count > {MAX_OPENS}')
    print('Press Ctrl+C to stop.\n')


    # ── Graceful shutdown on Ctrl+C ───────────────────────────

    stop_event = asyncio.Event()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, stop_event.set)

    await stop_event.wait()

    print('\nStopping ephemeral engine...')
    await eph_alert.stop()
    print('Engine stopped.')

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
