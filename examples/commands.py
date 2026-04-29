"""
Commands Example
================

Demonstrates how to send commands to devices and query command history.

Usage:
    python examples/commands.py

Requires:
    - RELAY_API_KEY environment variable
    - RELAY_SECRET environment variable
"""

import asyncio
import os
import json

from relayx_app_sdk import RelayApp


API_KEY = os.environ.get('RELAY_API_KEY', '')
SECRET = os.environ.get('RELAY_SECRET', '')


async def main():

    # ── Initialize ────────────────────────────────────────────

    app = RelayApp({
        'api_key': API_KEY,
        'secret': SECRET,
        'mode': 'production',
    })

    # Listen for connection lifecycle events
    app.connection.listeners(lambda event: print(f'[connection] {event}'))

    await app.connect()
    print('Connected.\n')


    # ── Send a command to one device ──────────────────────────

    # for i in range(0, 2000):
    #     result = await app.command.send({
    #         'name': 'setConfig',
    #         'device_ident': ['s-3'],
    #         'data': {
    #             'reason': 'firmware update',
    #             'delay_seconds': 5,
    #         },
    #     })

    # print('Send to one device:')
    # for ident, status in result.items():
    #     print(f'  {ident}: {status}')
    # print()


    # # ── Send a command to multiple devices ────────────────────

    # result = await app.command.send({
    #     'name': 'setConfig',
    #     'device_ident': ['sensor-1', 'sensor-2', 'sensor-3'],
    #     'data': {
    #         'reporting_interval': 30,
    #         'mode': 'low-power',
    #     },
    # })

    # print('Send to multiple devices:')
    # for ident, status in result.items():
    #     print(f'  {ident}: {status}')
    # print()


    # ── Query command history ─────────────────────────────────

    history = await app.command.history({
        'name': 'setConfig',
        'device_idents': ['s-3'],
        'start': '2024-01-01T00:00:00.000Z',
    })

    print('Command history:')
    for ident, records in history.items():
        if isinstance(records, dict) and 'error' in records:
            print(f'  {ident}: {records["error"]}')
        elif isinstance(records, list):
            print(f'  {ident}: {len(records)} record(s)')
            for rec in records:
                print(f'    {rec}')
        else:
            print(f'  {ident}: {records}')
    print()


    # ── Query history with custom end time ────────────────────

    history = await app.command.history({
        'name': 'setConfig',
        'device_idents': ['s-3'],
        'start': '2024-06-01T00:00:00.000Z',
        'end': '2026-06-30T23:59:59.000Z',
    })

    print(json.dumps(history, indent=4))

    print('Command history (June 2024):')
    for ident, records in history.items():
        if isinstance(records, list):
            print(f'  {ident}: {len(records)} record(s)')
        else:
            print(f'  {ident}: {records}')
    print()


    # ── Cleanup ───────────────────────────────────────────────

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
