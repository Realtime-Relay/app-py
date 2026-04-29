import asyncio
import uuid
import msgpack
import nats.js.api

from .utils import invoke_callback, stream_history, decode_stored_value
from .validation import (
    validate_ident, validate_event_name, validate_callable, validate_connected,
    validate_non_empty_list, validate_iso8601, validate_start_before_end,
)


class EventManager:

    def __init__(self, ctx):
        self._ctx = ctx
        self._consumers = {}   # event_name -> sub
        self._callbacks = {}   # event_name -> callback

    async def stream(self, params):
        """Stream live events for one or more devices.

        params:
            name          str          required — event name
            device_ident  str|list     required:
                - "*"        → all devices in org (wildcard subject; reverse-resolved ident in callback)
                - [ident]    → single device (concrete subject)
                - [a, b, …]  → list of devices (wildcard subject + in-callback filter)
                - []         → raises
            callback      callable(payload) — invoked with {<device_ident>: data}
        """
        validate_connected(self._ctx.connected)
        validate_event_name(params.get('name'))
        validate_callable(params.get('callback'), 'callback')

        device_ident_param = params.get('device_ident')
        if device_ident_param is None:
            raise ValueError('device_ident is required')

        # device_filter: dict[device_id, ident] — present except in "*" wildcard mode.
        # subject_device: concrete device_id when filtering to exactly one device, else "*".
        device_filter = None
        subject_device = '*'
        wildcard = False

        if isinstance(device_ident_param, str):
            if device_ident_param != '*':
                raise ValueError(
                    'device_ident as a string must be "*". Use a list for specific devices.'
                )
            wildcard = True
            # Warm the device cache so the callback can reverse-resolve device_id → ident.
            try:
                await self._ctx.device.list()
            except Exception:
                # Best-effort — callback falls back to device_id as the key on miss.
                pass
        elif isinstance(device_ident_param, list):
            if len(device_ident_param) == 0:
                raise ValueError('device_ident list cannot be empty')
            for ident in device_ident_param:
                validate_ident(ident, 'device_ident')
            device_filter = {}
            for ident in device_ident_param:
                device_id = await self._ctx.device.resolve_device_id(ident)
                device_filter[device_id] = ident
            if len(device_filter) == 1:
                subject_device = next(iter(device_filter.keys()))
        else:
            raise ValueError(
                'device_ident must be "*" or a non-empty list of device idents'
            )

        name = params['name']

        if name in self._consumers:
            return False

        subject = f'{self._ctx.org_id}.{self._ctx.env}.events.{subject_device}.{name}'
        stream = f'{self._ctx.org_id}_stream'
        consumer_name_prefix = f'apppy_events_{name}'

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

        self._consumers[name] = sub
        self._callbacks[name] = params['callback']

        def reverse_lookup(device_id):
            for ident, dev in self._ctx.device.cache.items():
                if isinstance(dev, dict) and dev.get('id') == device_id:
                    return ident
            return None

        async def msg_handler(msg):
            data = msgpack.unpackb(msg.data, raw=False)
            await msg.ack()

            # subject = <org>.<env>.events.<device_id>.<name>
            tokens = msg.subject.split('.')
            device_id = tokens[3] if len(tokens) > 3 else ''

            if device_filter is not None:
                ident = device_filter.get(device_id)
                if ident is None:
                    return  # device not in this subscription's filter
            elif wildcard:
                ident = reverse_lookup(device_id) or device_id
            else:
                ident = device_id  # unreachable, but be safe

            cb = self._callbacks.get(name)
            if cb:
                await invoke_callback(cb, {ident: data})

        self._ctx.register_subscription({
            'key': f'events:{name}',
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
        validate_event_name(params.get('name'))

        name = params['name']
        sub = self._consumers.get(name)

        if sub:
            try:
                await sub.unsubscribe()
            except Exception as e:
                self._ctx.logger.error(f'Failed to unsubscribe event {name}', e)

            del self._consumers[name]
            self._callbacks.pop(name, None)
            self._ctx.unregister_subscription(f'events:{name}')

    async def history(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('device_ident'), 'device_ident')
        validate_non_empty_list(params.get('event_names'), 'event_names')
        for n in params['event_names']:
            validate_event_name(n)
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
            'event_names': params['event_names'],
            'start': params['start'],
            'end': params['end'],
        }
        if params.get('interval'):
            payload['interval'] = params['interval']
        if params.get('aggregate_fn'):
            payload['aggregate_fn'] = params['aggregate_fn']

        result = await stream_history(
            self._ctx,
            f'api.iot.db.{self._ctx.org_id}.event.history',
            payload,
            on_frame=on_frame,
        )

        if result.get('error'):
            raise RuntimeError(
                f"Event history failed: {result.get('error_message') or result.get('status')}"
            )

        events = {name: [] for name in params['event_names']}

        for frame in result['frames']:
            data = frame.get('data') if isinstance(frame, dict) else None
            if not data:
                continue
            for name, point in data.items():
                if name not in events:
                    events[name] = []
                value = decode_stored_value(point.get('value'))
                events[name].append({
                    'value': value,
                    'timestamp': point.get('timestamp'),
                })

        return events

    async def _consume(self, sub, handler):
        try:
            async for msg in sub.messages:
                try:
                    await handler(msg)
                except Exception as e:
                    self._ctx.logger.error('Error processing event message', e)
        except Exception as e:
            self._ctx.logger.error('Event consumer loop ended', e)

    async def delete_all_consumers(self):
        for name, sub in list(self._consumers.items()):
            try:
                await sub.unsubscribe()
            except Exception as e:
                self._ctx.logger.error(f'Failed to unsubscribe event {name}', e)
            self._ctx.unregister_subscription(f'events:{name}')

        self._consumers.clear()
        self._callbacks.clear()
