"""
Telemetry history example — fetch telemetry for "mc-01", fields current_s1 +
wifi_rssi. Optionally also fetch the latest value per field (latest()).

Goes through app.telemetry.history() / .latest(), which now hit the
influx-db-service REST API (POST /iot/db/telemetry/history) under the hood.

Usage:
    RELAY_API_KEY=... RELAY_SECRET=... python examples/telemetry_history.py
"""

import asyncio
import json
import os
import time

from relayx_app_sdk import RelayApp

API_KEY = os.environ.get('RELAY_API_KEY', '')
SECRET = os.environ.get('RELAY_SECRET', '')

DEVICE_IDENT = 'mc-01'
FIELDS = ['current_s1', 'wifi_rssi']
START = '2026-06-16T00:00:00.000Z'
END = '2026-06-17T00:00:00.000Z'

# Also fetch the latest reading per field over the window (telemetry.latest()).
SHOW_LATEST = True


async def main():
    app = RelayApp({'api_key': API_KEY, 'secret': SECRET, 'mode': 'production'})
    await app.connect()
    print('Connected to RelayX')

    try:
        t0 = time.perf_counter()
        data = await app.telemetry.history({
            'device_ident': DEVICE_IDENT,
            'fields': FIELDS,
            'start': START,
            'end': END,
        })
        elapsed_ms = round((time.perf_counter() - t0) * 1000)

        # data = { 'current_s1': [{ 'value': ..., 'timestamp': ... }, ...], ... }
        print(json.dumps(data, indent=2, default=str))
        for field in FIELDS:
            print(f'mc-01 "{field}": {len(data.get(field, []))} reading(s)')
        print(f'history fetched in {elapsed_ms}ms')

        if SHOW_LATEST:
            lt0 = time.perf_counter()
            latest = await app.telemetry.latest({
                'device_ident': DEVICE_IDENT,
                'fields': FIELDS,
                'start': START,
                'end': END,
            })
            latest_ms = round((time.perf_counter() - lt0) * 1000)

            # latest = { 'current_s1': { 'value': ..., 'timestamp': ... }, ... }
            print('\nlatest:')
            print(json.dumps(latest, indent=2, default=str))
            print(f'latest fetched in {latest_ms}ms')
    except Exception as e:
        print(f'telemetry.history failed: {e}')
    finally:
        await app.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
