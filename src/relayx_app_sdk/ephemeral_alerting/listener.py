import asyncio
import uuid
import msgpack
import nats.js.api
from datetime import datetime, timezone

from ..utils import invoke_callback
from .shared import get_rule_id, create_fresh_state


# Map event type tokens to callback names (matches JS callbackMap)
EVENT_TYPE_TO_CALLBACK = {
    'fire': 'on_fire',
    'resolved': 'on_resolved',
    'ack': 'on_ack',
}


class EphemeralListener:
    """Listens for alert events published by an EphemeralOwner.
    Matches JS EphemeralListener exactly."""

    def __init__(self, ctx, rule, callbacks):
        self._ctx = ctx
        self._rule = rule
        self._callbacks = callbacks or {}
        self._consumer = None
        self._consume_task = None
        self._internal_state = create_fresh_state()
        self._running = True

    async def start(self):
        if self._consumer is not None:
            return

        rule_id = get_rule_id(self._rule)
        subject = f'{self._ctx.org_id}.{self._ctx.env}.alerts.listen.{rule_id}.*'
        stream = f'{self._ctx.org_id}_stream'
        consumer_name_prefix = f'apppy_alert_listen_{rule_id}'

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

        self._consumer = sub

        async def msg_handler(msg):
            data = msgpack.unpackb(msg.data, raw=False)
            await msg.ack()
            await self._dispatch_event(msg.subject, data)

        self._msg_handler = msg_handler

        self._ctx.register_subscription({
            'key': f'ephemeral_listen:{rule_id}',
            'type': 'jetstream',
            'subject': subject,
            'stream': stream,
            'consumer_name_prefix': consumer_name_prefix,
            'callback': msg_handler,
            'sub_ref': sub,
        })

        self._consume_task = asyncio.create_task(self._consume(sub))

    async def stop(self):
        self._running = False

        if self._consumer:
            try:
                await self._consumer.unsubscribe()
            except Exception as e:
                self._ctx.logger.error('Failed to unsubscribe ephemeral listener', e)

            rule_id = get_rule_id(self._rule)
            self._ctx.unregister_subscription(f'ephemeral_listen:{rule_id}')
            self._consumer = None

        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except (asyncio.CancelledError, Exception):
                pass
            self._consume_task = None

        self._internal_state = create_fresh_state()

    async def _consume(self, sub):
        try:
            async for msg in sub.messages:
                try:
                    await self._msg_handler(msg)
                except Exception as e:
                    self._ctx.logger.error('Error processing ephemeral listener message', e)
        except Exception as e:
            self._ctx.logger.error('Ephemeral listener consumer loop ended', e)

    async def _dispatch_event(self, subject, data):
        """Extract event type from subject and call the matching callback.
        Matches JS: only transforms timestamp for fire/resolved events."""
        tokens = subject.split('.')
        event_type = tokens[-1] if tokens else None

        callback_name = EVENT_TYPE_TO_CALLBACK.get(event_type)
        if not callback_name:
            return

        cb = self._callbacks.get(callback_name)
        if not cb:
            return

        transformed = dict(data) if isinstance(data, dict) else {'data': data}

        # Only transform timestamp for fire/resolved events (matches JS)
        if event_type in ('fire', 'resolved') and isinstance(transformed.get('timestamp'), (int, float)):
            dt = datetime.fromtimestamp(transformed['timestamp'] / 1000.0, tz=timezone.utc)
            transformed['timestamp'] = dt.strftime('%Y-%m-%dT%H:%M:%S.') + f'{dt.microsecond // 1000:03d}Z'

        await invoke_callback(cb, transformed)

    @property
    def state(self):
        return dict(self._internal_state)

    @property
    def rolling_state(self):
        return {}
