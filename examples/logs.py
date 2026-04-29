"""
Logs Example
============

Interactive CLI for streaming and querying device logs.

Usage:
    RELAY_API_KEY=...  RELAY_SECRET=... python examples/logs.py
"""

import asyncio
import os
from datetime import datetime, timezone

from relayx_app_sdk import RelayApp


API_KEY = os.environ.get('RELAY_API_KEY', '')
SECRET = os.environ.get('RELAY_SECRET', '')


async def ask(prompt):
    return (await asyncio.to_thread(input, prompt)).strip()


def parse_levels(raw):
    if not raw or raw == '*':
        return None  # default = all
    return [l.strip() for l in raw.split(',') if l.strip()]


async def main():
    app = RelayApp({
        'api_key': API_KEY,
        'secret': SECRET,
        'mode': 'production',
    })

    app.connection.listeners(lambda event: print(f'[connection] {event}'))

    await app.connect()
    print('Connected to RelayX\n')

    while True:
        op = (await ask('\nOperation (on/off/history/quit): ')).lower()
        if op in ('quit', 'q'):
            break
        if op not in ('on', 'off', 'history'):
            print('Invalid operation. Use "on", "off", "history", or "quit".')
            continue

        ident = await ask('Device ident: ')
        if not ident:
            print('Ident cannot be empty.')
            continue

        if op == 'history':
            levels_raw = (await ask('Levels (* / info,warn,error): ')) or '*'
            levels = parse_levels(levels_raw)

            start = '2025-01-01T00:00:00.000Z'
            now = datetime.now(timezone.utc)
            end = now.strftime('%Y-%m-%dT%H:%M:%S.') + f'{now.microsecond // 1000:03d}Z'

            agg_mode = await ask('Aggregate? (count <interval> | <enter> to skip): ')

            params = {'device_ident': ident, 'start': start, 'end': end}
            if levels:
                params['levels'] = levels
            if agg_mode:
                parts = agg_mode.split()
                if len(parts) != 2:
                    print('Format: "count 1h" or similar.')
                    continue
                params['aggregate_fn'] = parts[0]
                params['interval'] = parts[1]

            try:
                data = await app.log.history(params)
                print('Counts by level:', {k: len(v) for k, v in data.items()})
                for level, entries in data.items():
                    if not entries:
                        continue
                    print(f'\n--- {level.upper()} ({len(entries)}) ---')
                    for e in entries[:20]:
                        print(f"  {e.get('timestamp')}  {e.get('value')}")
                    if len(entries) > 20:
                        print(f'  ... {len(entries) - 20} more')
            except Exception as err:
                print(f'Failed: {err}')
            continue

        if op == 'on':
            levels_raw = (await ask('Levels (* / info,warn,error): ')) or '*'
            levels = '*' if levels_raw == '*' else [l.strip() for l in levels_raw.split(',') if l.strip()]

            try:
                def make_cb(device_ident):
                    def cb(entry):
                        tag = f"[{entry['level'].upper()}]".ljust(8)
                        print(f"{entry.get('timestamp')} {tag} {device_ident}: {entry.get('data')}")
                    return cb

                await app.log.stream({
                    'device_ident': ident,
                    'levels': levels,
                    'callback': make_cb(ident),
                })
                label = ','.join(levels) if isinstance(levels, list) else 'all'
                print(f'Streaming {ident} logs ({label})')
            except Exception as err:
                print(f'Failed: {err}')
        else:  # off
            await app.log.off({'device_ident': ident})
            print(f'Stopped log stream for {ident}')

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
