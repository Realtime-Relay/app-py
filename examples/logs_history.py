"""
Logs history example — fetch device log history for "mc-01" (info/warn/error).

Goes through app.log.history(), which now hits the influx-db-service REST API
(POST /iot/db/log/history) under the hood — same inputs/outputs as before.

Usage:
    RELAY_API_KEY=... RELAY_SECRET=... python examples/logs_history.py
"""

import asyncio
import json
import os
import time

from relayx_app_sdk import RelayApp

API_KEY = os.environ.get('RELAY_API_KEY', '')
SECRET = os.environ.get('RELAY_SECRET', '')

DEVICE_IDENT = 'mc-01'
LEVELS = ['info', 'warn', 'error']
START = '2026-06-16T00:00:00.000Z'
END = '2026-06-18T00:00:00.000Z'


async def main():
    app = RelayApp({'api_key': API_KEY, 'secret': SECRET, 'mode': 'production'})
    await app.connect()
    print('Connected to RelayX')

    try:
        t0 = time.perf_counter()
        data = await app.log.history({
            'device_ident': DEVICE_IDENT,
            'levels': LEVELS,
            'start': START,
            'end': END,
        })
        elapsed_ms = round((time.perf_counter() - t0) * 1000)

        # data = { 'info': [...], 'warn': [...], 'error': [...] }
        print(json.dumps(data, indent=2, default=str))
        for level in LEVELS:
            print(f'mc-01 "{level}": {len(data.get(level, []))} log(s)')
        print(f'fetched in {elapsed_ms}ms')
    except Exception as e:
        print(f'log.history failed: {e}')
    finally:
        await app.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
