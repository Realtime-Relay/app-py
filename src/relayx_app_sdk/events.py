import asyncio
import uuid
import msgpack
import nats.js.api

from .utils import invoke_callback
from .validation import validate_ident, validate_event_name, validate_callable, validate_connected


class EventManager:

    def __init__(self, ctx):
        self._ctx = ctx
        self._consumers = {}   # event_name -> sub
        self._callbacks = {}   # event_name -> callback

    async def stream(self, params):
        validate_connected(self._ctx.connected)
        validate_event_name(params.get('name'))
        validate_callable(params.get('callback'), 'callback')

        name = params['name']

        if name in self._consumers:
            return False

        subject = f'{self._ctx.org_id}.{self._ctx.env}.events.*.{name}'
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
        callback = params['callback']

        async def msg_handler(msg):
            data = msgpack.unpackb(msg.data, raw=False)
            await msg.ack()

            cb = self._callbacks.get(name)
            if cb:
                await invoke_callback(cb, data)

        self._ctx.register_subscription({
            'key': f'events:{name}',
            'type': 'jetstream',
            'subject': subject,
            'stream': stream,
            'consumer_name_prefix': consumer_name_prefix,
            'callback': msg_handler,
            'sub_ref': sub,
        })

        asyncio.create_task(self._consume(sub, name))
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

    async def _consume(self, sub, name):
        try:
            async for msg in sub.messages:
                try:
                    data = msgpack.unpackb(msg.data, raw=False)
                    await msg.ack()

                    cb = self._callbacks.get(name)
                    if cb:
                        await invoke_callback(cb, data)
                except Exception as e:
                    self._ctx.logger.error(f'Error processing event message for {name}', e)
        except Exception as e:
            self._ctx.logger.error(f'Event consumer loop ended for {name}', e)

    async def delete_all_consumers(self):
        for name, sub in list(self._consumers.items()):
            try:
                await sub.unsubscribe()
            except Exception as e:
                self._ctx.logger.error(f'Failed to unsubscribe event {name}', e)
            self._ctx.unregister_subscription(f'events:{name}')

        self._consumers.clear()
        self._callbacks.clear()
