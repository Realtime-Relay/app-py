import asyncio
import json
import time
import uuid
import msgpack
import nats.js.api

from ..utils import invoke_callback
from .shared import (
    get_rule_id,
    build_data_subject,
    resolve_ident_from_id,
    build_alert_payload,
    is_muted,
    create_fresh_state,
    dispatch_notifications,
    publish_event,
    SUBJECT_DEVICE_INDEX,
    SUBJECT_LAST_TOKEN_INDEX,
)


class EphemeralOwner:
    """Owns an ephemeral alert rule: subscribes to data, evaluates the rule,
    manages state transitions, and handles RPC commands (ack, mute, etc.).
    Matches JS EphemeralOwner exactly."""

    def __init__(self, ctx, rule, evaluator, callbacks):
        self._ctx = ctx
        self._rule = rule
        self._evaluator = evaluator
        self._callbacks = callbacks or {}

        self._data_consumer = None
        self._rpc_subscriptions = []
        self._rolling_state = {}
        self._state = create_fresh_state()

        self._kv_bucket = ctx.kv_bucket
        self._heartbeat_task = None
        self._consume_task = None
        self._rpc_task = None
        self._recovery_task = None
        self._last_data_at = None
        self._running = True


    # ─── Public API ────────────────────────────────────────────

    async def start(self):
        lock_acquired = await self._acquire_lock()

        if not lock_acquired:
            self._running = False
            raise RuntimeError('Evaluator already active for this rule')

        self._start_heartbeat()
        self._start_recovery_check()
        await self._subscribe_data_topic()
        await self._subscribe_rpcs()

    async def stop(self):
        self._running = False

        # Cancel recovery tick
        if self._recovery_task:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except (asyncio.CancelledError, Exception):
                pass
            self._recovery_task = None

        # Unsubscribe data consumer
        if self._data_consumer:
            try:
                await self._data_consumer.unsubscribe()
            except Exception as e:
                self._ctx.logger.error('Failed to unsubscribe ephemeral data consumer', e)

            rule_id = get_rule_id(self._rule)
            self._ctx.unregister_subscription(f'ephemeral_data:{rule_id}')
            self._data_consumer = None

        # Cancel consume task
        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except (asyncio.CancelledError, Exception):
                pass
            self._consume_task = None

        # Drain RPC subscriptions
        for sub in self._rpc_subscriptions:
            try:
                await sub.unsubscribe()
            except Exception as e:
                self._ctx.logger.error('Failed to unsubscribe RPC subscription', e)

        rule_id = get_rule_id(self._rule)
        self._ctx.unregister_subscription(f'ephemeral_rpc:{rule_id}')
        self._rpc_subscriptions.clear()

        # Release KV lock
        await self._release_lock()

        self._rolling_state = {}
        self._state = create_fresh_state()

    async def ack(self, acked_by, ack_notes=None):
        if self._state['status'] != 'alerting':
            return False

        self._state['status'] = 'acknowledged'
        self._state['acked_by'] = acked_by
        self._state['acked_at'] = int(time.time() * 1000)
        self._state['ack_notes'] = ack_notes

        payload = {
            'status': 'acknowledged',
            'ack': {
                'acked_by': acked_by,
                'ack_notes': ack_notes,
                'acked_at': self._state['acked_at'],
            },
        }

        await publish_event(self._ctx, self._rule, 'ack', payload)

        cb = self._callbacks.get('on_ack')
        if cb:
            await invoke_callback(cb, payload)

        return True

    async def ack_all(self, acked_by, ack_notes=None):
        if self._state['status'] != 'alerting':
            return False

        self._state['status'] = 'acknowledged'
        self._state['acked_by'] = acked_by
        self._state['acked_at'] = int(time.time() * 1000)
        self._state['ack_notes'] = ack_notes

        payload = {
            'status': 'acknowledged',
            'ack': {
                'acked_by': acked_by,
                'ack_notes': ack_notes,
                'acked_at': self._state['acked_at'],
            },
        }

        await publish_event(self._ctx, self._rule, 'ack_all', payload)

        cb = self._callbacks.get('on_ack_all')
        if cb:
            await invoke_callback(cb, payload)

        return True


    # ─── Data Subscription ─────────────────────────────────────

    async def _subscribe_data_topic(self):
        subject = await build_data_subject(self._ctx, self._rule)
        stream = f'{self._ctx.org_id}_stream'
        rule_id = get_rule_id(self._rule)
        consumer_name_prefix = f'apppy_ephemeral_{rule_id}'

        self._ctx.logger.info(f'Subscribing to subject: {subject}')
        self._ctx.logger.info(f'Stream: {stream}')

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

        self._data_consumer = sub

        async def msg_handler(msg):
            data = msgpack.unpackb(msg.data, raw=False)
            await msg.ack()

            self._last_data_at = int(time.time() * 1000)
            self._update_rolling_state(msg.subject, data)
            await self._evaluate(msg.subject, data)

        self._msg_handler = msg_handler

        self._ctx.register_subscription({
            'key': f'ephemeral_data:{rule_id}',
            'type': 'jetstream',
            'subject': subject,
            'stream': stream,
            'consumer_name_prefix': consumer_name_prefix,
            'callback': msg_handler,
            'sub_ref': sub,
        })

        self._consume_task = asyncio.create_task(self._consume_data(sub))

    async def _consume_data(self, sub):
        try:
            async for msg in sub.messages:
                try:
                    await self._msg_handler(msg)
                except Exception as e:
                    self._ctx.logger.error('Error processing ephemeral data message', e)
        except Exception as e:
            self._ctx.logger.error('Ephemeral data consumer loop ended', e)


    # ─── RPC Subscription (single wildcard, matches JS) ────────

    async def _subscribe_rpcs(self):
        rule_id = get_rule_id(self._rule)
        subject = f'{self._ctx.org_id}.{self._ctx.env}.alerts.custom.{rule_id}.*'

        sub = await self._ctx.nats_client.subscribe(subject)
        self._rpc_subscriptions.append(sub)

        self._ctx.register_subscription({
            'key': f'ephemeral_rpc:{rule_id}',
            'type': 'core',
            'subject': subject,
            'callback': None,
            'sub_ref': self._rpc_subscriptions,
        })

        self._rpc_task = asyncio.create_task(self._consume_rpc(sub))

    async def _consume_rpc(self, sub):
        try:
            async for msg in sub.messages:
                try:
                    tokens = msg.subject.split('.')
                    last_token = tokens[-1]

                    if last_token == 'ack':
                        await self._handle_ack_rpc(msg)
                    elif last_token == 'ack_all':
                        await self._handle_ack_all_rpc(msg)
                    elif last_token == 'mute':
                        await self._handle_mute_rpc(msg)
                except Exception as e:
                    self._ctx.logger.error('Error processing RPC message', e)
        except Exception as e:
            self._ctx.logger.error('RPC consumer loop ended', e)

    async def _handle_ack_rpc(self, msg):
        data = msgpack.unpackb(msg.data, raw=False)

        if self._state['status'] != 'alerting':
            await msg.respond(json.dumps({'status': 'ACK_FAILED', 'reason': 'not in alerting state'}).encode())
            return

        self._state['status'] = 'acknowledged'
        self._state['acked_by'] = data.get('acked_by')
        self._state['acked_at'] = int(time.time() * 1000)
        self._state['ack_notes'] = data.get('ack_notes')

        payload = {
            'status': 'acknowledged',
            'device_id': data.get('device_id'),
            'ack': {
                'acked_by': data.get('acked_by'),
                'ack_notes': data.get('ack_notes'),
                'acked_at': self._state['acked_at'],
            },
        }

        await publish_event(self._ctx, self._rule, 'ack', payload)

        cb = self._callbacks.get('on_ack')
        if cb:
            await invoke_callback(cb, payload)

        await msg.respond(json.dumps({'status': 'ACK_SUCCESS'}).encode())

    async def _handle_ack_all_rpc(self, msg):
        data = msgpack.unpackb(msg.data, raw=False)

        if self._state['status'] != 'alerting':
            await msg.respond(json.dumps({'status': 'ACK_FAILED', 'reason': 'not in alerting state'}).encode())
            return

        self._state['status'] = 'acknowledged'
        self._state['acked_by'] = data.get('acked_by')
        self._state['acked_at'] = int(time.time() * 1000)
        self._state['ack_notes'] = data.get('ack_notes')

        payload = {
            'status': 'acknowledged',
            'ack': {
                'acked_by': data.get('acked_by'),
                'ack_notes': data.get('ack_notes'),
                'acked_at': self._state['acked_at'],
            },
        }

        await publish_event(self._ctx, self._rule, 'ack_all', payload)

        cb = self._callbacks.get('on_ack_all')
        if cb:
            await invoke_callback(cb, payload)

        await msg.respond(json.dumps({'status': 'ACK_SUCCESS'}).encode())

    async def _handle_mute_rpc(self, msg):
        data = msgpack.unpackb(msg.data, raw=False)
        mute_config = data.get('mute_config')

        if mute_config is None or mute_config.get('type') == 'CLEAR':
            self._rule['alert_mute_config'] = None
        else:
            self._rule['alert_mute_config'] = mute_config

        # Sync mute to backend (matches JS #syncMuteToBackend)
        await self._sync_mute_to_backend()

        await msg.respond(json.dumps({'status': 'MUTE_SUCCESS'}).encode())

    async def _sync_mute_to_backend(self):
        try:
            mute_config = self._rule.get('alert_mute_config')
            rule_id = get_rule_id(self._rule)

            if mute_config:
                payload = {
                    'rule_id': rule_id,
                    'type': mute_config.get('type'),
                    'mute_till': mute_config.get('mute_till'),
                }
            else:
                payload = {
                    'rule_id': rule_id,
                    'type': 'CLEAR',
                }

            await self._ctx.nats_client.request(
                f'api.iot.alerts.{self._ctx.org_id}.mute',
                json.dumps(payload).encode(),
                timeout=10,
            )
        except Exception as e:
            self._ctx.logger.error('Failed to sync mute to backend', e)


    # ─── Rolling State ─────────────────────────────────────────

    def _update_rolling_state(self, subject, data):
        """Update rolling state with raw data from the source.
        Keyed by ident -> metric. Matches JS #updateRollingState."""
        source = self._rule.get('config', {}).get('topic', {}).get('source', 'TELEMETRY')
        tokens = subject.split('.')

        device_id_idx = SUBJECT_DEVICE_INDEX.get(source, 3)
        last_token_idx = SUBJECT_LAST_TOKEN_INDEX.get(source, 4)

        device_id = tokens[device_id_idx] if len(tokens) > device_id_idx else 'unknown'
        last_token = tokens[last_token_idx] if len(tokens) > last_token_idx else 'unknown'

        ident = resolve_ident_from_id(self._ctx, device_id) or device_id

        if ident not in self._rolling_state:
            self._rolling_state[ident] = {}

        if source == 'TELEMETRY':
            self._rolling_state[ident][last_token] = {
                'value': data.get('value') if isinstance(data, dict) else data,
                'timestamp': data.get('timestamp', int(time.time() * 1000)) if isinstance(data, dict) else int(time.time() * 1000),
            }
        else:
            self._rolling_state[ident][last_token] = data


    # ─── State Machine (matches JS #evaluate exactly) ──────────

    async def _evaluate(self, subject, data):
        if not self._running or not self._evaluator:
            return

        source = self._rule.get('config', {}).get('topic', {}).get('source', 'TELEMETRY')
        tokens = subject.split('.')
        device_id_idx = SUBJECT_DEVICE_INDEX.get(source, 3)
        device_id = tokens[device_id_idx] if len(tokens) > device_id_idx else 'unknown'

        now = int(time.time() * 1000)
        config = self._rule.get('config', {})
        duration_ms = (config.get('duration') or 0) * 1000
        recovery_ms = (config.get('recovery_duration') or 0) * 1000
        cooldown_ms = (config.get('cooldown') or 0) * 1000

        if is_muted(self._rule):
            return

        # Staleness check (matches JS)
        if self._state['last_evaluated_at'] is not None:
            gap = now - self._state['last_evaluated_at']
            if gap > duration_ms and duration_ms > 0:
                self._state['breached_since'] = None
                self._state['clear_since'] = None

        # Run evaluator with error handling (matches JS)
        try:
            breached = await invoke_callback(self._evaluator, self._rolling_state)
        except Exception as err:
            cb = self._callbacks.get('on_error')
            if cb:
                await invoke_callback(cb, err)
            return

        if not isinstance(breached, bool):
            cb = self._callbacks.get('on_error')
            if cb:
                await invoke_callback(cb, TypeError(f'Evaluator must return a boolean, got {type(breached).__name__}'))
            return

        if breached:
            self._state['clear_since'] = None

            if self._state['breached_since'] is None:
                self._state['breached_since'] = now

            held_for = now - self._state['breached_since']

            if self._state['status'] == 'normal' and held_for >= duration_ms:
                # FIRE
                self._state['status'] = 'alerting'
                self._state['last_fired'] = now

                fire_payload = build_alert_payload(self._rule, self._rolling_state, now, device_id)
                await publish_event(self._ctx, self._rule, 'fire', fire_payload)

                await dispatch_notifications(self._ctx, self._rule, {
                    'alert': {
                        'id': get_rule_id(self._rule),
                        'name': self._rule.get('name'),
                        'config': self._rule.get('config', {}),
                    },
                    'device_id': device_id,
                    'last_value': self._rolling_state,
                    'timestamp': int(time.time() * 1000),
                })

                cb = self._callbacks.get('on_fire')
                if cb:
                    await invoke_callback(cb, fire_payload)

            elif self._state['status'] == 'alerting' and (now - self._state['last_fired']) >= cooldown_ms:
                # Re-FIRE (cooldown elapsed)
                self._state['last_fired'] = now

                fire_payload = build_alert_payload(self._rule, self._rolling_state, now, device_id)
                await publish_event(self._ctx, self._rule, 'fire', fire_payload)

                await dispatch_notifications(self._ctx, self._rule, {
                    'alert': {
                        'id': get_rule_id(self._rule),
                        'name': self._rule.get('name'),
                        'config': self._rule.get('config', {}),
                    },
                    'device_id': device_id,
                    'last_value': self._rolling_state,
                    'timestamp': int(time.time() * 1000),
                })

                cb = self._callbacks.get('on_fire')
                if cb:
                    await invoke_callback(cb, fire_payload)

            # acknowledged -> silent (no action)

        else:
            self._state['breached_since'] = None

            if self._state['clear_since'] is None:
                self._state['clear_since'] = now

            cleared_for = now - self._state['clear_since']

            if self._state['status'] in ('alerting', 'acknowledged') and cleared_for >= recovery_ms:
                # RESOLVED
                resolved_payload = build_alert_payload(self._rule, self._rolling_state, now, device_id)
                await publish_event(self._ctx, self._rule, 'resolved', resolved_payload)

                await dispatch_notifications(self._ctx, self._rule, {
                    'alert': {
                        'id': get_rule_id(self._rule),
                        'name': self._rule.get('name'),
                        'config': self._rule.get('config', {}),
                    },
                    'device_id': '',
                    'last_value': self._rolling_state,
                    'timestamp': int(time.time() * 1000),
                })

                cb = self._callbacks.get('on_resolved')
                if cb:
                    await invoke_callback(cb, resolved_payload)

                # Reset state
                self._state['status'] = 'normal'
                self._state['acked_by'] = None
                self._state['acked_at'] = None
                self._state['ack_notes'] = None
                self._state['breached_since'] = None
                self._state['clear_since'] = None

        self._state['last_evaluated_at'] = now


    # ─── KV Lock (matches JS key/value format) ─────────────────

    async def _acquire_lock(self):
        kv = self._kv_bucket
        if not kv:
            return True

        rule_id = get_rule_id(self._rule)
        key = f'ephemeral_owner_{rule_id}'

        now = int(time.time() * 1000)
        owner_id = str(uuid.uuid4())
        lock_value = json.dumps({
            'owner_id': owner_id,
            'started_at': now,
            'expires_at': now + 30000,
        }).encode()

        # Check if lock exists and is active
        try:
            entry = await kv.get(key)

            if entry and entry.value:
                lock_data = json.loads(entry.value.decode())
                expires_at = lock_data.get('expires_at', 0)

                if now > expires_at:
                    await kv.update(key, lock_value, entry.revision)
                    return True
                else:
                    return False
        except Exception:
            pass

        # No active lock — write and verify
        try:
            await kv.put(key, lock_value)

            verify = await kv.get(key)
            verify_data = json.loads(verify.value.decode())

            if verify_data.get('owner_id') == owner_id:
                return True

            return False
        except Exception:
            return False

    def _start_heartbeat(self):
        if not self._kv_bucket:
            return

        rule_id = get_rule_id(self._rule)
        key = f'ephemeral_owner_{rule_id}'

        async def heartbeat_loop():
            while self._running:
                try:
                    value = json.dumps({
                        'started_at': int(time.time() * 1000),
                        'expires_at': int(time.time() * 1000) + 30000,
                    }).encode()

                    await self._kv_bucket.put(key, value)
                except Exception as e:
                    self._ctx.logger.error('Heartbeat tick failed', e)

                await asyncio.sleep(15)

        self._heartbeat_task = asyncio.create_task(heartbeat_loop())

    def _start_recovery_check(self):
        """Start a background tick that auto-resolves alerts during silence.
        Only active when config.recovery_eval_type == 'TIMER'.
        When no data arrives for recovery_duration, the alert resolves."""

        config = self._rule.get('config', {})
        recovery_eval_type = config.get('recovery_eval_type', 'VALUE')

        if recovery_eval_type != 'TIMER':
            return

        recovery_s = config.get('recovery_duration') or 0
        recovery_ms = recovery_s * 1000

        # Tick at half the recovery window, floored at 1s, capped at 30s
        tick_s = max(1, min(30, recovery_s / 2)) if recovery_s > 0 else 5

        async def recovery_loop():
            while self._running:
                await asyncio.sleep(tick_s)

                if not self._running:
                    break

                now = int(time.time() * 1000)

                # Only check when in alerting or acknowledged state
                if self._state['status'] not in ('alerting', 'acknowledged'):
                    continue

                # No data has ever arrived
                if self._last_data_at is None:
                    continue

                silence_ms = now - self._last_data_at

                if silence_ms >= recovery_ms:
                    # Auto-resolve: silence exceeded recovery_duration
                    resolved_payload = build_alert_payload(
                        self._rule, self._rolling_state, now, '',
                    )

                    await publish_event(self._ctx, self._rule, 'resolved', resolved_payload)

                    await dispatch_notifications(self._ctx, self._rule, {
                        'alert': {
                            'id': get_rule_id(self._rule),
                            'name': self._rule.get('name'),
                            'config': config,
                        },
                        'device_id': '',
                        'last_value': self._rolling_state,
                        'timestamp': now,
                    })

                    cb = self._callbacks.get('on_resolved')
                    if cb:
                        await invoke_callback(cb, resolved_payload)

                    # Reset state
                    self._state['status'] = 'normal'
                    self._state['acked_by'] = None
                    self._state['acked_at'] = None
                    self._state['ack_notes'] = None
                    self._state['breached_since'] = None
                    self._state['clear_since'] = None

        self._recovery_task = asyncio.create_task(recovery_loop())

    async def _release_lock(self):
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
            self._heartbeat_task = None

        if self._kv_bucket:
            try:
                rule_id = get_rule_id(self._rule)
                await self._kv_bucket.purge(f'ephemeral_owner_{rule_id}')
            except Exception as e:
                self._ctx.logger.error('Failed to release lock', e)


    # ─── Properties ────────────────────────────────────────────

    @property
    def state(self):
        return dict(self._state)

    @property
    def rolling_state(self):
        return dict(self._rolling_state)
