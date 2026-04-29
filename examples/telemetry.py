"""
Telemetry Example
=================

Demonstrates how to stream live telemetry, query history, and fetch latest values.

Usage:
    python examples/telemetry.py

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
        'mode': 'production',
    })

    app.connection.listeners(lambda event: print(f'[connection] {event}'))

    await app.connect()
    print('Connected.\n')


    # ── Stream live telemetry ─────────────────────────────────

    # Subscribe to specific metrics
    def on_telemetry(data):
        print(f'  [{data["metric"]}] {data["data"]}')

    await app.telemetry.stream({
        'device_ident': 's-3',
        'metric': ['temperature', 'humidity'],
        'callback': on_telemetry,
    })

    print('Streaming temperature & humidity from s-3 for 15 seconds...')
    await asyncio.sleep(150)

    # Subscribe to all metrics
    # await app.telemetry.stream({
    #     'device_ident': 's-3',
    #     'metric': '*',
    #     'callback': on_telemetry,
    # })


    # ── Stop streaming ────────────────────────────────────────

    # Remove specific metrics from filtered subscriptions (leaves "*" intact)
    # await app.telemetry.off({
    #     'device_ident': 's-3',
    #     'metric': ['temperature'],
    # })

    # Remove all subscriptions for a device
    # await app.telemetry.off({
    #     'device_ident': 's-3',
    # })

    # print('Stopped streaming.\n')


    # ── Query telemetry history ───────────────────────────────

    # history = await app.telemetry.history({
    #     'device_ident': 's-3',
    #     'fields': ['temperature', 'humidity'],
    #     'start': '2025-01-01T00:00:00.000Z',
    #     'end': '2026-12-31T23:59:59.000Z',
    # })

    # print('Telemetry history:')
    # print(json.dumps(history, indent=4))
    # print()

    # print(f"Temperature => {len(history["temperature"])}")
    # print(f"Humidity => {len(history["humidity"])}")
    # print()

    # # ── Fetch latest values ───────────────────────────────────

    # latest = await app.telemetry.latest({
    #     'device_ident': 's-3',
    #     'fields': ['temperature', 'humidity'],
    #     'start': '2025-01-01T00:00:00.000Z',
    #     'end': '2026-12-31T23:59:59.000Z',
    # })

    # print('Latest telemetry:')
    # print(json.dumps(latest, indent=4))
    # print()


    # ── Cleanup ───────────────────────────────────────────────

    await app.disconnect()
    print('Disconnected.')


if __name__ == '__main__':
    asyncio.run(main())
