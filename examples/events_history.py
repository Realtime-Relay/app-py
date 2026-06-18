"""
Events history example — fetch event history for "mc-01", event "state".

Goes through app.events.history(), which now hits the influx-db-service REST
API (POST /iot/db/event/history) under the hood — same inputs/outputs as before.

Usage:
    RELAY_API_KEY=... RELAY_SECRET=... python examples/events_history.py
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone

from relayx_app_sdk import RelayApp

API_KEY = os.environ.get('RELAY_API_KEY', '')
SECRET = os.environ.get('RELAY_SECRET', '')

DEVICE_IDENT = 'mc-01'
EVENT_NAMES = ['state']
START = '2026-06-16T00:00:00.000Z'
END = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z')


async def main():
    app = RelayApp({'api_key': API_KEY, 'secret': SECRET, 'mode': 'production'})
    await app.connect()
    print('Connected to RelayX')

    try:
        t0 = time.perf_counter()
        data = await app.events.history({
            'device_ident': DEVICE_IDENT,
            'event_names': EVENT_NAMES,
            'start': START,
            'end': END,
        })
        elapsed_ms = round((time.perf_counter() - t0) * 1000)

        # data = { 'state': [{ 'value': ..., 'timestamp': ... }, ...] }
        points = data.get('state', [])
        print(json.dumps(data, indent=2, default=str))
        print(f'mc-01 "state": {len(points)} event(s)')
        print(f'fetched in {elapsed_ms}ms')
    except Exception as e:
        print(f'events.history failed: {e}')
    finally:
        await app.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
