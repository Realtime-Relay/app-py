import asyncio
import uuid

import msgpack
import nats.js.api

from .utils import invoke_callback, stream_history, decode_stored_value
from .validation import (
    validate_ident, validate_callable, validate_connected,
    validate_non_empty_list, validate_iso8601, validate_start_before_end,
)


VALID_LEVELS = ['info', 'warn', 'error']


class LogManager:
    """Mirrors JS LogManager: real-time + history streaming for device logs."""

    def __init__(self, ctx):
        self._ctx = ctx
        self._consumers = {}  # device_ident -> list of {sub, levels: set|None, callback}

    # ─── Real-time streaming ─────────────────────────────────

    async def stream(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('device_ident'), 'device_ident')
        validate_callable(params.get('callback'), 'callback')

        levels = params.get('levels')
        level_filter = None

        if isinstance(levels, str):
            if levels != '*':
                raise ValueError('levels as a string must be "*". Use a list for specific levels.')
        elif isinstance(levels, list):
            validate_non_empty_list(levels, 'levels')
            invalid = [l for l in levels if l not in VALID_LEVELS]
            if invalid:
                raise ValueError(
                    f"levels contains invalid values: {', '.join(invalid)}. Valid: {', '.join(VALID_LEVELS)}"
                )
            level_filter = set(levels)
        elif levels is None:
            pass
        else:
            raise ValueError(
                'levels must be "*", None, or a non-empty list of "info"|"warn"|"error"'
            )

        device_ident = params['device_ident']
        device_id = await self._ctx.device.resolve_device_id(device_ident)

        subject = f'{self._ctx.org_id}.{self._ctx.env}.logs.{device_id}.*'
        stream = f'{self._ctx.org_id}_stream'
        consumer_name_prefix = f'apppy_logs_{device_ident}'

        callback = params['callback']
        entry = {'sub': None, 'levels': level_filter, 'callback': callback}

        async def msg_handler(msg):
            data = msgpack.unpackb(msg.data, raw=False)

            await msg.ack()

            tokens = msg.subject.split('.')
            level = tokens[-1]

            if entry['levels'] is not None and level not in entry['levels']:
                return

            payload = {
                'level': level,
                'data': data.get('data') if isinstance(data, dict) else data,
                'timestamp': data.get('timestamp') if isinstance(data, dict) else None,
            }

            await invoke_callback(callback, payload)


        sub_key = f'logs:{device_ident}:{uuid.uuid4()}'
        entry['sub_key'] = sub_key

        if device_ident not in self._consumers:
            self._consumers[device_ident] = []
        self._consumers[device_ident].append(entry)

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
        entry['sub'] = sub
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

        for entry in subs:
            try:
                await entry['sub'].unsubscribe()
            except Exception as e:
                self._ctx.logger.error('Failed to unsubscribe log stream', e)
            self._ctx.unregister_subscription(entry.get('sub_key', ''))

        del self._consumers[device_ident]

    # ─── History ─────────────────────────────────────────────

    async def history(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('device_ident'), 'device_ident')
        validate_iso8601(params.get('start'), 'start')
        validate_iso8601(params.get('end'), 'end')
        validate_start_before_end(params['start'], params['end'])

        levels = params.get('levels')
        if levels is not None:
            validate_non_empty_list(levels, 'levels')
            invalid = [l for l in levels if l not in VALID_LEVELS]
            if invalid:
                raise ValueError(
                    f"levels contains invalid values: {', '.join(invalid)}. Valid: {', '.join(VALID_LEVELS)}"
                )

        on_frame = params.get('on_frame')
        if on_frame is not None:
            validate_callable(on_frame, 'on_frame')

        device_id = await self._ctx.device.resolve_device_id(params['device_ident'])

        payload = {
            'device_id': device_id,
            'env': self._ctx.env,
            'start': params['start'],
            'end': params['end'],
        }
        if levels:
            payload['levels'] = levels
        if params.get('interval'):
            payload['interval'] = params['interval']
        if params.get('aggregate_fn'):
            payload['aggregate_fn'] = params['aggregate_fn']

        result = await stream_history(
            self._ctx,
            f'api.iot.db.{self._ctx.org_id}.log.history',
            payload,
            on_frame=on_frame,
        )

        if result.get('error'):
            raise RuntimeError(
                f"Log history failed: {result.get('error_message') or result.get('status')}"
            )

        logs = {lvl: [] for lvl in (levels or VALID_LEVELS)}

        for frame in result['frames']:
            data = frame.get('data') if isinstance(frame, dict) else None
            if not data:
                continue
            for level, point in data.items():
                if level not in logs:
                    logs[level] = []
                value = decode_stored_value(point.get('value'))
                logs[level].append({
                    'value': value,
                    'timestamp': point.get('timestamp'),
                })

        return logs

    # ─── Internals ───────────────────────────────────────────

    async def _consume(self, sub, handler):
        try:
            async for msg in sub.messages:
                try:
                    await handler(msg)
                except Exception as e:
                    self._ctx.logger.error('Error processing log message', e)
        except Exception as e:
            self._ctx.logger.error('Log consumer loop ended', e)

    async def delete_all_consumers(self):
        for device_ident, subs in list(self._consumers.items()):
            for entry in subs:
                try:
                    await entry['sub'].unsubscribe()
                except Exception as e:
                    self._ctx.logger.error('Failed to unsubscribe log stream', e)
                self._ctx.unregister_subscription(entry.get('sub_key', ''))

        self._consumers.clear()
