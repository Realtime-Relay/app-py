import asyncio
import json
import uuid
import msgpack
import nats.js.api
from datetime import datetime, timedelta, timezone

from .utils import invoke_callback, stream_history, decode_stored_value
from .validation import (
    validate_ident, validate_callable,
    validate_connected, validate_list, validate_non_empty_list,
    validate_iso8601, validate_start_before_end,
)


class TelemetryManager:

    def __init__(self, ctx):
        self._ctx = ctx
        self._consumers = {}  # key: device_ident -> list of {sub, metrics: set|None, callback}

    async def stream(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('device_ident'), 'device_ident')
        validate_callable(params.get('callback'), 'callback')

        metric = params.get('metric')
        metrics_filter = None

        if isinstance(metric, str):
            if metric != '*':
                raise ValueError('metric as a string must be "*". Use a list for specific metrics.')
        elif isinstance(metric, list):
            validate_non_empty_list(metric, 'metric')
            for m in metric:
                await self._validate_metric(params['device_ident'], m)
            metrics_filter = set(metric)
        else:
            raise ValueError('metric must be "*" or a non-empty list of metric names')

        device_ident = params['device_ident']
        device_id = await self._ctx.device.resolve_device_id(device_ident)
        subject = f'{self._ctx.org_id}.{self._ctx.env}.telemetry.{device_id}.*'
        stream = f'{self._ctx.org_id}_stream'
        consumer_name_prefix = f"apppy_telemetry_{device_ident}"

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

        entry = {'sub': sub, 'metrics': metrics_filter, 'callback': params['callback']}

        if device_ident not in self._consumers:
            self._consumers[device_ident] = []
        self._consumers[device_ident].append(entry)

        callback = params['callback']

        async def msg_handler(msg):
            data = msgpack.unpackb(msg.data, raw=False)
            await msg.ack()

            tokens = msg.subject.split('.')
            metric_name = tokens[-1]

            if entry['metrics'] is not None and metric_name not in entry['metrics']:
                return

            await invoke_callback(callback, {
                'metric': metric_name,
                'data': data,
            })

        sub_key = f'telemetry:{device_ident}:{uuid.uuid4()}'
        entry['sub_key'] = sub_key

        self._ctx.register_subscription({
            'key': sub_key,
            'type': 'jetstream',
            'subject': subject,
            'stream': stream,
            'consumer_name_prefix': consumer_name_prefix,
            'callback': msg_handler,
            'sub_ref': sub,
        })

        asyncio.create_task(self._consume(sub, msg_handler))

    async def off(self, params):
        validate_ident(params.get('device_ident'), 'device_ident')

        device_ident = params['device_ident']
        subs = self._consumers.get(device_ident)
        if not subs:
            return

        metric = params.get('metric')

        if metric is not None:
            validate_list(metric, 'metric')

            for i in range(len(subs) - 1, -1, -1):
                entry = subs[i]

                # Skip wildcard ("*") subscriptions — metrics is None
                if entry['metrics'] is None:
                    continue

                for m in metric:
                    entry['metrics'].discard(m)

                if len(entry['metrics']) == 0:
                    try:
                        await entry['sub'].unsubscribe()
                    except Exception as e:
                        self._ctx.logger.error(f'Failed to unsubscribe telemetry', e)

                    self._ctx.unregister_subscription(entry.get('sub_key', ''))
                    subs.pop(i)

            if len(subs) == 0:
                del self._consumers[device_ident]
        else:
            for entry in subs:
                try:
                    await entry['sub'].unsubscribe()
                except Exception as e:
                    self._ctx.logger.error(f'Failed to unsubscribe telemetry', e)

                self._ctx.unregister_subscription(entry.get('sub_key', ''))

            del self._consumers[device_ident]


    async def history(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('device_ident'), 'device_ident')
        validate_non_empty_list(params.get('fields'), 'fields')

        await self._validate_fields(params['device_ident'], params['fields'])

        validate_iso8601(params.get('start'), 'start')
        validate_iso8601(params.get('end'), 'end')
        validate_start_before_end(params['start'], params['end'])

        on_frame = params.get('on_frame')
        if on_frame is not None:
            validate_callable(on_frame, 'on_frame')

        device_id = await self._ctx.device.resolve_device_id(params['device_ident'])

        payload = {
            'device_id': device_id,
            'env': self._ctx.env,
            'start': params['start'],
            'end': params['end'],
            'fields': params['fields'],
            'last_value': False,
        }
        if params.get('interval'):
            payload['interval'] = params['interval']
        if params.get('aggregate_fn'):
            payload['aggregate_fn'] = params['aggregate_fn']

        result = await stream_history(
            self._ctx,
            f'api.iot.db.{self._ctx.org_id}.telemetry.history',
            payload,
            on_frame=on_frame,
        )

        if result.get('error'):
            raise RuntimeError(
                f"Telemetry history failed: {result.get('error_message') or result.get('status')}"
            )

        telemetry = {field: [] for field in params['fields']}

        for frame in result['frames']:
            data = frame.get('data') if isinstance(frame, dict) else None
            if not data:
                continue
            for metric, point in data.items():
                if metric not in telemetry:
                    telemetry[metric] = []
                value = decode_stored_value(point.get('value'))
                telemetry[metric].append({
                    'value': value,
                    'timestamp': point.get('timestamp'),
                })

        return telemetry


    async def latest(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('device_ident'), 'device_ident')
        validate_non_empty_list(params.get('fields'), 'fields')
        await self._validate_fields(params['device_ident'], params['fields'])

        validate_iso8601(params.get('start'), 'start')
        validate_iso8601(params.get('end'), 'end')
        validate_start_before_end(params['start'], params['end'])

        device_id = await self._ctx.device.resolve_device_id(params['device_ident'])

        result = await stream_history(
            self._ctx,
            f'api.iot.db.{self._ctx.org_id}.telemetry.history',
            {
                'device_id': device_id,
                'env': self._ctx.env,
                'start': params['start'],
                'end': params['end'],
                'fields': params['fields'],
                'last_value': True,
            },
        )

        if result.get('error'):
            raise RuntimeError(
                f"Telemetry latest failed: {result.get('error_message') or result.get('status')}"
            )

        latest = {}
        if not result['frames']:
            return latest

        only_frame = result['frames'][0]
        data = only_frame.get('data') if isinstance(only_frame, dict) else None
        if data:
            for metric, point in data.items():
                value = decode_stored_value(point.get('value'))
                latest[metric] = {'value': value, 'timestamp': point.get('timestamp')}

        return latest


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
        for device_ident, subs in list(self._consumers.items()):
            for entry in subs:
                try:
                    await entry['sub'].unsubscribe()
                except Exception as e:
                    self._ctx.logger.error(f'Failed to unsubscribe telemetry', e)
                self._ctx.unregister_subscription(entry.get('sub_key', ''))

        self._consumers.clear()
