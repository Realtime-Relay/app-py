"""
RPC Example
===========

Demonstrates how to make RPC calls to devices and get responses back.

Usage:
    python examples/rpc.py

Requires:
    - RELAY_API_KEY environment variable
    - RELAY_SECRET environment variable
"""

import asyncio
import json
import os

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


    # ── Basic RPC call ────────────────────────────────────────

    try:
        result = await app.rpc.call({
            'device_ident': 's-3',
            'name': 'reboot_device',
            'data': {},
        })

        print('RPC getStatus response:')
        print(json.dumps(result, indent=4))
        print()
    except Exception as e:
        print(f'RPC getStatus failed: {e}\n')


    # ── RPC call with payload ─────────────────────────────────

    try:
        result = await app.rpc.call({
            'device_ident': 's-3',
            'name': 'reboot_device',
            'data': {'keys': ['reporting_interval', 'mode']},
        })

        print('RPC getConfig response:')
        print(json.dumps(result, indent=4))
        print()
    except Exception as e:
        print(f'RPC getConfig failed: {e}\n')


    # ── RPC call with custom timeout ──────────────────────────

    try:
        result = await app.rpc.call({
            'device_ident': 's-3',
            'name': 'reboot_device',
            'data': {'level': 'full'},
            'timeout': 30,
        })

        print('RPC runDiagnostic response:')
        print(json.dumps(result, indent=4))
        print()
    except Exception as e:
        print(f'RPC runDiagnostic failed: {e}\n')


    # ── Cleanup ───────────────────────────────────────────────

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
