"""
Events Example
==============

Demonstrates how to stream live events, query event history, and stop streams.

Usage:
    python examples/events.py

Requires:
    - RELAY_API_KEY environment variable
    - RELAY_SECRET environment variable
"""

import asyncio
import json
import os
from datetime import datetime, timezone

from relayx_app_sdk import RelayApp


API_KEY = os.environ.get('RELAY_API_KEY', '')
SECRET = os.environ.get('RELAY_SECRET', '')


async def ask(prompt):
    return (await asyncio.to_thread(input, prompt)).strip()


async def main():

    # ── Initialize ────────────────────────────────────────────

    app = RelayApp({
        'api_key': API_KEY,
        'secret': SECRET,
        'mode': 'production',
    })

    app.connection.listeners(lambda event: print(f'[connection] {event}'))

    await app.connect()
    print('Connected to RelayX.\n')


    # ── Interactive CLI ───────────────────────────────────────

    while True:
        op = (await ask('\nOperation (on/off/history/quit): ')).lower()

        if op in ('quit', 'q'):
            break

        if op not in ('on', 'off', 'history'):
            print('Invalid operation. Use "on", "off", "history", or "quit".')
            continue

        if op == 'history':
            ident = await ask('Device ident: ')
            if not ident:
                print('Ident cannot be empty.')
                continue

            names_raw = await ask('Event names (comma-separated): ')
            event_names = [n.strip() for n in names_raw.split(',') if n.strip()]

            if not event_names:
                print('At least one event name required.')
                continue

            start = '2025-01-01T00:00:00.000Z'
            end = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')

            agg_mode = await ask('Aggregate? (count <interval> | <enter> to skip): ')

            params = {
                'device_ident': ident,
                'event_names': event_names,
                'start': start,
                'end': end,
            }

            if agg_mode:
                parts = agg_mode.split()
                if len(parts) != 2:
                    print('Format: "count 5m" or similar.')
                    continue
                params['aggregate_fn'] = parts[0]
                params['interval'] = parts[1]

            try:
                data = await app.events.history(params)
                counts = {k: len(v) for k, v in data.items()}
                print('Counts:', counts)
                print(json.dumps(data, indent=2, default=str))
            except Exception as err:
                print(f'Failed: {err}')
            continue

        # on / off
        name = await ask('Event name: ')
        if not name:
            print('Event name cannot be empty.')
            continue

        if op == 'on':
            idents_raw = await ask('Device idents ("*" or comma-separated): ')
            if not idents_raw:
                print('Device idents cannot be empty.')
                continue

            if idents_raw == '*':
                device_ident = '*'
            else:
                device_ident = [s.strip() for s in idents_raw.split(',') if s.strip()]
                if not device_ident:
                    print('At least one device ident required.')
                    continue

            def make_callback(event_name):
                def cb(payload):
                    # payload shape: {<device_ident>: <event_data>}
                    for ident, data in payload.items():
                        print(f'[event:{event_name}] {ident}', data)
                return cb

            try:
                ok = await app.events.stream({
                    'name': name,
                    'device_ident': device_ident,
                    'callback': make_callback(name),
                })
                if ok:
                    label = ','.join(device_ident) if isinstance(device_ident, list) else device_ident
                    print(f'Streaming "{name}" for [{label}]')
                else:
                    print(f'Already streaming "{name}"')
            except Exception as err:
                print(f'Failed: {err}')
        else:
            await app.events.off({'name': name})
            print(f'Stopped streaming "{name}"')


    # ── Cleanup ───────────────────────────────────────────────

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
