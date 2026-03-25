"""
Ephemeral Alert — Command-Based (Owner Mode)
===============================================

Monitors "reboot" commands sent to device s-3.
Fires if a reboot command is received with status "failed".

The rolling state for COMMAND sources stores the raw command data:
{
    "s-3": {
        "reboot": { "value": { ... }, "timestamp": 1711234567000 }
    }
}

Usage:
    python examples/ephemeral_alerts/command_owner.py

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
COMMAND_NAME = 'reboot'


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

    eph_alert = await app.alert.get({'name': 'ephemeral_cmd_reboot'})

    if eph_alert is None:
        eph_alert = await app.alert.create_ephemeral({
            'name': 'ephemeral_cmd_reboot',
            'description': f'Client-side alert: failed {COMMAND_NAME} commands on {DEVICE_IDENT}',
            'config': {
                'topic': {
                    'source': 'COMMAND',
                    'device_ident': DEVICE_IDENT,
                    'last_token': COMMAND_NAME,
                },
                'duration': 0,
                'recovery_duration': 60,
                'cooldown': 30,
            },
            'notification_channel': [],
        })

    print(f"Ephemeral alert: {eph_alert.get('name')} (id: {eph_alert.get('id')})\n")


    # ── Set the evaluator ─────────────────────────────────────

    def evaluator(data):
        """
        Receives the rolling state — raw command data:
        {
            "s-3": {
                "reboot": { "value": { "status": "failed", "reason": "..." }, "timestamp": ... }
            }
        }

        Returns True if the latest reboot command has status "failed".
        """
        device = data.get(DEVICE_IDENT)
        if not device:
            return False

        cmd_data = device.get(COMMAND_NAME)
        if not cmd_data:
            return False

        # Command payloads are msgpacked as { value: <data>, timestamp: <ts> }
        value = cmd_data.get('value', cmd_data)
        status = value.get('status') if isinstance(value, dict) else None

        breached = status == 'failed'
        result = 'BREACH' if breached else 'CLEAR'

        print(f"  [eval] {COMMAND_NAME} status={status or '?'} → {result}")

        return breached

    eph_alert.set_evaluator(evaluator)


    # ── Start listening ───────────────────────────────────────

    await eph_alert.listen({
        'on_fire': lambda data: print(f"\n[FIRE] {json.dumps(data, indent=2)}"),
        'on_resolved': lambda data: print(f"\n[RESOLVED] {json.dumps(data, indent=2)}"),
        'on_error': lambda err: print(f"\n[EVALUATOR ERROR] {err}"),
    })

    print('Ephemeral alert engine started (owner mode — COMMAND source).')
    print(f'Monitoring {DEVICE_IDENT}: fires on failed {COMMAND_NAME} commands')
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
