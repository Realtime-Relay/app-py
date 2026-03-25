import asyncio
import uuid
import json
import nats
import msgpack
from nats.aio.client import RawCredentials

from .utils import build_credentials, invoke_callback
from .validation import validate_callable


SERVERS = [
    'tls://api.relay-x.io:4221',
    'tls://api.relay-x.io:4222',
    'tls://api.relay-x.io:4223',
]


class ConnectionManager:

    def __init__(self, ctx):
        self._ctx = ctx
        self._listener_callback = None
        self._presence_callback = None
        self._presence_sub = None
        self._connect_called = False
        self._is_reconnecting = False

    def listeners(self, callback):
        validate_callable(callback, 'callback')
        self._listener_callback = callback

    async def presence(self, callback):
        validate_callable(callback, 'callback')

        if not self._ctx.connected:
            raise RuntimeError('Not connected. Call app.connect() first.')

        self._presence_callback = callback
        await self._start_presence_consumer()

    async def presence_off(self):
        if self._presence_sub:
            try:
                await self._presence_sub.unsubscribe()
            except Exception as e:
                self._ctx.logger.error('Failed to unsubscribe presence', e)

            self._ctx.unregister_subscription('presence')
            self._presence_sub = None

        self._presence_callback = None

    async def connect(self):
        if self._connect_called:
            return

        creds_content = build_credentials(self._ctx.api_key, self._ctx.secret)
        creds = RawCredentials(creds_content)

        try:
            self._ctx.nats_client = await nats.connect(
                servers=SERVERS,
                no_echo=True,
                allow_reconnect=True,
                max_reconnect_attempts=1200,
                reconnect_time_wait=0.5,
                max_outstanding_pings=2,
                ping_interval=5,
                user_credentials=creds,
                token=self._ctx.api_key,
                disconnected_cb=self._on_disconnected,
                reconnected_cb=self._on_reconnected,
                closed_cb=self._on_closed,
                error_cb=self._on_error,
            )

            self._ctx.jetstream = self._ctx.nats_client.jetstream()

            # Initialize KV bucket for ephemeral alert locks
            try:
                self._ctx.kv_bucket = await self._ctx.jetstream.key_value(self._ctx.org_id)
            except Exception as e:
                self._ctx.logger.error('Failed to initialize KV bucket', e)
                self._ctx.kv_bucket = None

            self._ctx.connected = True
            self._connect_called = True

            # Populate device cache
            await self._ctx.device.list()

            self._emit_event('connected')

        except Exception as err:
            self._ctx.connected = False
            self._emit_event('auth_failed')
            raise

    async def disconnect(self):
        if not self._ctx.nats_client:
            return

        # Clean up presence consumer
        if self._presence_sub:
            try:
                await self._presence_sub.unsubscribe()
            except Exception as e:
                self._ctx.logger.error('Failed to unsubscribe presence on disconnect', e)
            self._presence_sub = None

        self._ctx.connected = False
        self._connect_called = False
        self._ctx.device.clear_cache()
        self._ctx.offline_buffer.clear()
        self._ctx._subscription_registry.clear()

        await self._ctx.nats_client.close()


    # ─── Presence Consumer ─────────────────────────────────────

    async def _start_presence_consumer(self):
        if self._presence_sub:
            try:
                await self._presence_sub.unsubscribe()
            except Exception as e:
                self._ctx.logger.error('Failed to unsubscribe old presence consumer', e)

        subject = f'import.{self._ctx.org_id}.{self._ctx.env}.presence.*'
        stream = f'{self._ctx.org_id}_stream'
        consumer_name = f'apppy_presence_{uuid.uuid4()}'

        sub = await self._ctx.jetstream.subscribe(
            subject,
            stream=stream,
            config=nats.js.api.ConsumerConfig(
                name=consumer_name,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                deliver_policy=nats.js.api.DeliverPolicy.NEW,
                replay_policy=nats.js.api.ReplayPolicy.INSTANT,
            ),
        )

        self._presence_sub = sub

        self._ctx.register_subscription({
            'key': 'presence',
            'type': 'jetstream',
            'subject': subject,
            'stream': stream,
            'consumer_name_prefix': 'apppy_presence',
            'callback': self._presence_message_handler,
            'sub_ref': sub,
        })

        asyncio.create_task(self._consume_presence(sub))

    async def _consume_presence(self, sub):
        try:
            async for msg in sub.messages:
                try:
                    data = msgpack.unpackb(msg.data, raw=False)
                    await msg.ack()

                    if self._presence_callback:
                        await invoke_callback(self._presence_callback, data)
                except Exception as e:
                    self._ctx.logger.error('Error processing presence message', e)
        except Exception as e:
            self._ctx.logger.error('Presence consumer loop ended', e)

    async def _presence_message_handler(self, msg):
        data = msgpack.unpackb(msg.data, raw=False)
        await msg.ack()

        if self._presence_callback:
            await invoke_callback(self._presence_callback, data)


    # ─── Connection Callbacks ──────────────────────────────────

    async def _on_disconnected(self):
        self._ctx.connected = False

        # Emit 'reconnecting' once per cycle (matches JS DebugEvents.Reconnecting)
        if not self._is_reconnecting:
            self._is_reconnecting = True
            self._emit_event('reconnecting')

    async def _on_reconnected(self):
        self._ctx.connected = True

        # Re-init JetStream
        self._ctx.jetstream = self._ctx.nats_client.jetstream()

        # Resubscribe all registered consumers + subs
        await self._resubscribe_all()

        # Flush offline buffer
        await self._flush_offline_buffer()

        self._is_reconnecting = False
        self._emit_event('reconnected')

    async def _on_closed(self):
        self._ctx.connected = False
        self._connect_called = False
        self._is_reconnecting = False
        self._emit_event('disconnected')

    async def _on_error(self, e):
        if 'authorization' in str(e).lower():
            self._emit_event('auth_failed')

            try:
                await self._ctx.nats_client.close()
            except Exception as e:
                self._ctx.logger.error('Failed to close NATS client after auth error', e)


    # ─── Resubscription ────────────────────────────────────────

    async def _resubscribe_all(self):
        registry = list(self._ctx._subscription_registry)
        self._ctx._subscription_registry.clear()

        for entry in registry:
            try:
                sub_type = entry.get('type')

                if sub_type == 'jetstream':
                    await self._resubscribe_jetstream(entry)
                elif sub_type == 'core':
                    await self._resubscribe_core(entry)
            except Exception as e:
                self._ctx.logger.error(f"Failed to resubscribe {entry.get('key')}", e)

    async def _resubscribe_jetstream(self, entry):
        subject = entry['subject']
        stream = entry['stream']
        consumer_name = f"{entry['consumer_name_prefix']}_{uuid.uuid4()}"
        callback = entry['callback']

        sub = await self._ctx.jetstream.subscribe(
            subject,
            stream=stream,
            config=nats.js.api.ConsumerConfig(
                name=consumer_name,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                deliver_policy=nats.js.api.DeliverPolicy.NEW,
                replay_policy=nats.js.api.ReplayPolicy.INSTANT,
            ),
        )

        new_entry = {**entry, 'sub_ref': sub}
        self._ctx.register_subscription(new_entry)

        asyncio.create_task(self._consume_generic(sub, callback))

    async def _resubscribe_core(self, entry):
        subject = entry['subject']
        callback = entry['callback']

        sub = await self._ctx.nats_client.subscribe(subject)

        new_entry = {**entry, 'sub_ref': sub}
        self._ctx.register_subscription(new_entry)

        asyncio.create_task(self._consume_core_generic(sub, callback))

    async def _consume_generic(self, sub, callback):
        try:
            async for msg in sub.messages:
                try:
                    await callback(msg)
                except Exception as e:
                    self._ctx.logger.error('Error in generic consumer callback', e)
        except Exception as e:
            self._ctx.logger.error('Generic consumer loop ended', e)

    async def _consume_core_generic(self, sub, callback):
        try:
            async for msg in sub.messages:
                try:
                    await callback(msg)
                except Exception as e:
                    self._ctx.logger.error('Error in core consumer callback', e)
        except Exception as e:
            self._ctx.logger.error('Core consumer loop ended', e)


    # ─── Offline Buffer ────────────────────────────────────────

    async def _flush_offline_buffer(self):
        messages = list(self._ctx.offline_buffer)
        self._ctx.offline_buffer.clear()

        for subject, payload in messages:
            try:
                await self._ctx.jetstream.publish(subject, payload)
            except Exception as e:
                self._ctx.logger.error(f'Failed to flush buffered message to {subject}', e)

    def _emit_event(self, event):
        if self._listener_callback:
            self._listener_callback(event)
