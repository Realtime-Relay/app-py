import asyncio
import json
import uuid
import msgpack
import nats.js.api

from .utils import invoke_callback
from .validation import (
    validate_ident, validate_telemetry_metric, validate_callable,
    validate_connected, validate_list, validate_non_empty_list,
    validate_iso8601, validate_start_before_end,
)


class TelemetryManager:

    def __init__(self, ctx):
        self._ctx = ctx
        self._consumers = {}  # key: "{device_ident}:{metric}" -> sub

    async def stream(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('device_ident'), 'device_ident')

        metric = params.get('metric')
        validate_telemetry_metric(metric)

        validate_callable(params.get('callback'), 'callback')
        await self._validate_metric(params['device_ident'], metric)

        key = f"{params['device_ident']}:{metric}"

        if key in self._consumers:
            return False

        # Build NATS subject
        device_id = await self._ctx.device.resolve_device_id(params['device_ident'])
        subject = f'{self._ctx.org_id}.{self._ctx.env}.telemetry.{device_id}.{metric}'
        stream = f'{self._ctx.org_id}_stream'
        consumer_name_prefix = f"apppy_telemetry_{params['device_ident']}"

        sub = await self._ctx.jetstream.subscribe(
            subject,
            stream=stream,
            config=nats.js.api.ConsumerConfig(
                name=f'{consumer_name_prefix}_{uuid.uuid4()}',
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                deliver_policy=nats.js.api.DeliverPolicy.NEW,
                replay_policy=nats.js.api.ReplayPolicy.INSTANT,
            ),
        )

        self._consumers[key] = sub
        callback = params['callback']

        async def msg_handler(msg):
            data = msgpack.unpackb(msg.data, raw=False)
            await msg.ack()
            await invoke_callback(callback, data)

        self._ctx.register_subscription({
            'key': f'telemetry:{key}',
            'type': 'jetstream',
            'subject': subject,
            'stream': stream,
            'consumer_name_prefix': consumer_name_prefix,
            'callback': msg_handler,
            'sub_ref': sub,
        })

        asyncio.create_task(self._consume(sub, msg_handler))
        return True

    async def off(self, params):
        validate_ident(params.get('device_ident'), 'device_ident')

        metric = params.get('metric')

        if metric is not None:
            validate_list(metric, 'metric')

            for m in metric:
                key = f"{params['device_ident']}:{m}"
                sub = self._consumers.get(key)

                if sub:
                    try:
                        await sub.unsubscribe()
                    except Exception as e:
                        self._ctx.logger.error(f'Failed to unsubscribe telemetry {key}', e)

                    del self._consumers[key]
                    self._ctx.unregister_subscription(f'telemetry:{key}')
        else:
            keys_to_remove = [
                k for k in self._consumers
                if k.startswith(f"{params['device_ident']}:")
            ]

            for key in keys_to_remove:
                sub = self._consumers[key]

                try:
                    await sub.unsubscribe()
                except Exception as e:
                    self._ctx.logger.error(f'Failed to unsubscribe telemetry {key}', e)

                del self._consumers[key]
                self._ctx.unregister_subscription(f'telemetry:{key}')

    async def history(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('device_ident'), 'device_ident')
        validate_non_empty_list(params.get('fields'), 'fields')
        await self._validate_fields(params['device_ident'], params['fields'])
        validate_iso8601(params.get('start'), 'start')
        validate_iso8601(params.get('end'), 'end')
        validate_start_before_end(params['start'], params['end'])

        device_id = await self._ctx.device.resolve_device_id(params['device_ident'])

        data = json.dumps({
            'device_id': device_id,
            'env': self._ctx.env,
            'start': params['start'],
            'end': params['end'],
            'fields': params['fields'],
        }).encode()

        res = await self._ctx.nats_client.request(
            f'api.iot.db.{self._ctx.org_id}.telemetry.history',
            data,
            timeout=20,
        )

        return json.loads(res.data.decode())

    async def latest(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('device_ident'), 'device_ident')
        validate_non_empty_list(params.get('fields'), 'fields')
        await self._validate_fields(params['device_ident'], params['fields'])

        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)
        one_day_ago = now - timedelta(days=1)

        device_id = await self._ctx.device.resolve_device_id(params['device_ident'])

        data = json.dumps({
            'device_id': device_id,
            'env': self._ctx.env,
            'start': one_day_ago.strftime('%Y-%m-%dT%H:%M:%S.') + f'{one_day_ago.microsecond // 1000:03d}Z',
            'end': now.strftime('%Y-%m-%dT%H:%M:%S.') + f'{now.microsecond // 1000:03d}Z',
            'fields': params['fields'],
        }).encode()

        res = await self._ctx.nats_client.request(
            f'api.iot.db.{self._ctx.org_id}.telemetry.history',
            data,
            timeout=20,
        )

        return json.loads(res.data.decode())


    # ─── Internal Helpers ──────────────────────────────────────

    async def _validate_fields(self, device_ident, fields):
        device = self._ctx.device.cache.get(device_ident)

        if not device or not device.get('schema'):
            device = await self._ctx.device.get({'ident': device_ident})

        if not device or not isinstance(device, dict) or not device.get('schema'):
            raise ValueError(f'Device {device_ident} not found or has no schema')

        invalid = [f for f in fields if f not in device['schema']]

        if invalid:
            valid_keys = ', '.join(device['schema'].keys())
            raise ValueError(f"fields contain invalid metrics: {', '.join(invalid)}. Valid keys: {valid_keys}")

    async def _validate_metric(self, device_ident, metric):
        if metric == '*':
            return

        device = self._ctx.device.cache.get(device_ident)

        if not device or not device.get('schema'):
            device = await self._ctx.device.get({'ident': device_ident})

        if not device or not isinstance(device, dict) or not device.get('schema'):
            raise ValueError(f'Device {device_ident} not found or has no schema')

        if metric not in device['schema']:
            valid_keys = ', '.join(device['schema'].keys())
            raise ValueError(f'metric "{metric}" is not a valid key in device schema. Valid keys: {valid_keys}')

    async def _consume(self, sub, handler):
        try:
            async for msg in sub.messages:
                try:
                    await handler(msg)
                except Exception as e:
                    self._ctx.logger.error('Error processing telemetry message', e)
        except Exception as e:
            self._ctx.logger.error('Telemetry consumer loop ended', e)

    async def delete_all_consumers(self):
        for key, sub in list(self._consumers.items()):
            try:
                await sub.unsubscribe()
            except Exception as e:
                self._ctx.logger.error(f'Failed to unsubscribe telemetry {key}', e)
            self._ctx.unregister_subscription(f'telemetry:{key}')

        self._consumers.clear()
