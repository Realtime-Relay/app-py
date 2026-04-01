import asyncio
import json
import uuid
import msgpack
import nats.js.api
import time

from .utils import invoke_callback
from .validation import (
    validate_ident, validate_callable, validate_connected,
    validate_non_empty_list, validate_iso8601, validate_start_before_end,
    validate_positive_number,
)
from .ephemeral_alerting import EphemeralEngine


VALID_SOURCES = ['TELEMETRY', 'COMMAND', 'EVENT']
VALID_RULE_TYPES = ['DEVICE', 'RULE']
VALID_EVENT_STATES = ['fire', 'resolved']
VALID_ACK_STATES = ['ack', 'ack_all']


class Alert:

    def __init__(self, data, manager):
        self._data = data
        self._manager = manager

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)

    def items(self):
        return self._data.items()

    def set_evaluator(self, fn):
        raise RuntimeError('set_evaluator is only allowed for EPHEMERAL alerts')

    async def listen(self, callbacks):
        await self._manager._listen(self._data, callbacks)

    async def stop(self):
        pass


class EphemeralAlert:

    def __init__(self, data, engine):
        self._data = data
        self._engine = engine

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)

    def items(self):
        return self._data.items()

    def set_evaluator(self, fn):
        validate_callable(fn, 'evaluator')
        self._engine.set_evaluator(fn)

    async def listen(self, callbacks):
        await self._engine.listen(callbacks)

    async def stop(self):
        await self._engine.stop()


class AlertManager:

    def __init__(self, ctx):
        self._ctx = ctx
        self._listen_consumers = {}   # rule_id -> sub
        self._ephemeral_engines = {}  # rule_id -> EphemeralEngine
        self._alert_metadata = {}     # alert_id -> {'type': ...}

    def _subject(self, op):
        return f'api.iot.alerts.{self._ctx.org_id}.{op}'

    async def _request(self, op, payload):
        data = json.dumps(payload).encode()
        res = await self._ctx.nats_client.request(self._subject(op), data, timeout=20)

        return json.loads(res.data.decode())


    # ─── Alert Wrapping ────────────────────────────────────────

    def _wrap_alert(self, data):
        if not data:
            return data

        if data.get('id'):
            self._alert_metadata[data['id']] = {'type': data.get('type', 'THRESHOLD')}

        return Alert(data, self)

    def _wrap_ephemeral_alert(self, data):
        if not data:
            return data

        if data.get('id'):
            self._alert_metadata[data['id']] = {'type': 'EPHEMERAL'}

        engine = EphemeralEngine(self._ctx, data)
        self._ephemeral_engines[data['id']] = engine

        return EphemeralAlert(data, engine)


    # ─── CRUD ──────────────────────────────────────────────────

    async def create(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('name'), 'name')

        if params.get('type') == 'EPHEMERAL':
            raise ValueError('Use create_ephemeral() for EPHEMERAL alerts')

        if params.get('type') not in ('THRESHOLD', 'RATE_CHANGE'):
            raise ValueError('type must be THRESHOLD or RATE_CHANGE')

        payload = {
            'name': params['name'],
            'description': params.get('description'),
            'type': params['type'],
            'metric': params.get('metric'),
            'config': params.get('config'),
            'notification_channel': params.get('notification_channel', []),
            'alert_mute_config': params.get('alert_mute_config'),
            'env': self._ctx.env,
        }

        res = await self._request('create', payload)

        if res.get('data'):
            return self._wrap_alert(res['data'])

        return None

    async def create_ephemeral(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('name'), 'name')

        config = params.get('config')
        if not config:
            raise ValueError('config is required')

        topic = config.get('topic')
        if not topic:
            raise ValueError('config.topic is required')

        if topic.get('source') not in VALID_SOURCES:
            raise ValueError('config.topic.source must be TELEMETRY, COMMAND, or EVENT')

        if not topic.get('device_ident'):
            raise ValueError('config.topic.device_ident is required')

        if not topic.get('last_token'):
            raise ValueError('config.topic.last_token is required')

        validate_positive_number(config.get('duration'), 'config.duration')
        validate_positive_number(config.get('recovery_duration'), 'config.recovery_duration')

        recovery_eval_type = config.get('recovery_eval_type')
        if recovery_eval_type is not None and recovery_eval_type not in ('VALUE', 'TIMER'):
            raise ValueError('config.recovery_eval_type must be "VALUE" or "TIMER"')

        payload = {
            'name': params['name'],
            'description': params.get('description'),
            'config': config,
            'notification_channel': params.get('notification_channel', []),
            'type': 'EPHEMERAL',
            'env': self._ctx.env,
        }

        res = await self._request('create_ephemeral', payload)

        if res.get('data'):
            return self._wrap_ephemeral_alert(res['data'])

        return None

    async def update(self, params):
        validate_connected(self._ctx.connected)

        if not params.get('id'):
            raise ValueError('id is required')

        payload = {**params, 'env': self._ctx.env}
        res = await self._request('update', payload)

        if res.get('data'):
            return self._wrap_alert(res['data'])

        return res

    async def update_ephemeral(self, params):
        validate_connected(self._ctx.connected)

        if not params.get('id'):
            raise ValueError('id is required')

        config = params.get('config', {})
        recovery_eval_type = config.get('recovery_eval_type')
        if recovery_eval_type is not None and recovery_eval_type not in ('VALUE', 'TIMER'):
            raise ValueError('config.recovery_eval_type must be "VALUE" or "TIMER"')

        payload = {**params, 'type': 'EPHEMERAL', 'env': self._ctx.env}
        res = await self._request('update_ephemeral', payload)

        if res.get('data'):
            return self._wrap_ephemeral_alert(res['data'])

        return None

    async def delete(self, alert_id):
        validate_connected(self._ctx.connected)

        if not alert_id:
            raise ValueError('id is required')

        engine = self._ephemeral_engines.get(alert_id)
        if engine:
            await engine.stop()
            del self._ephemeral_engines[alert_id]

        self._alert_metadata.pop(alert_id, None)

        res = await self._request('delete', {'id': alert_id})
        return res.get('status') == 'ALERT_DELETE_SUCCESS'

    async def list(self):
        validate_connected(self._ctx.connected)

        res = await self._request('list', {})
        alerts = res.get('data', [])

        for a in alerts:
            if a.get('id'):
                self._alert_metadata[a['id']] = {'type': a.get('type', 'THRESHOLD')}

        return alerts

    async def get(self, alert_name):
        validate_connected(self._ctx.connected)

        validate_ident(alert_name, 'name')

        res = await self._request('get', {'name': alert_name})

        if not res.get('data'):
            return None

        data = res['data']

        if data.get('id'):
            self._alert_metadata[data['id']] = {'type': data.get('type', 'THRESHOLD')}

        if data.get('type') == 'EPHEMERAL':
            return self._wrap_ephemeral_alert(data)

        return self._wrap_alert(data)


    # ─── History ───────────────────────────────────────────────

    async def history(self, params):
        validate_connected(self._ctx.connected)

        rule_type = params.get('rule_type')
        if not rule_type:
            raise ValueError('rule_type is required')

        if rule_type not in VALID_RULE_TYPES:
            raise ValueError('rule_type must be DEVICE or RULE')

        if rule_type == 'DEVICE' and not params.get('device_ident'):
            raise ValueError('device_ident is required for rule_type DEVICE')

        if rule_type == 'RULE' and not params.get('rule_id'):
            raise ValueError('rule_id is required for rule_type RULE')

        rule_states = params.get('rule_states')
        if rule_states:
            validate_non_empty_list(rule_states, 'rule_states')

            invalid_states = [s for s in rule_states if s not in VALID_EVENT_STATES]
            if invalid_states:
                raise ValueError(f"rule_states contains invalid values: {', '.join(invalid_states)}. Valid: {', '.join(VALID_EVENT_STATES)}")

        validate_iso8601(params.get('start'), 'start')
        validate_iso8601(params.get('end'), 'end')
        validate_start_before_end(params['start'], params['end'])

        payload = {
            'rule_type': rule_type,
            'env': self._ctx.env,
            'rule_states': rule_states or ['fire', 'resolved'],
            'start': params['start'],
            'end': params['end'],
        }

        if params.get('device_ident'):
            payload['device_id'] = await self._ctx.device.resolve_device_id(params['device_ident'])

        if rule_type == 'RULE':
            payload['rule_id'] = params['rule_id']

        res = await self._ctx.nats_client.request(
            f'api.iot.db.{self._ctx.org_id}.alerts.history',
            json.dumps(payload).encode(),
            timeout=20,
        )

        decoded = json.loads(res.data.decode())

        if decoded.get('status') == 'ALERT_FETCH_SUCCESS':
            data = decoded.get('data', {})
            return {
                'has_more': data.get('has_more'),
                'cursor': data.get('cursor'),
                'data': data.get('data'),
            }

        return decoded

    async def ack_history(self, params):
        validate_connected(self._ctx.connected)

        if not params.get('rule_id'):
            raise ValueError('rule_id is required')

        ack_states = params.get('ack_states')
        if ack_states:
            validate_non_empty_list(ack_states, 'ack_states')

            invalid_states = [s for s in ack_states if s not in VALID_ACK_STATES]
            if invalid_states:
                raise ValueError(f"ack_states contains invalid values: {', '.join(invalid_states)}. Valid: {', '.join(VALID_ACK_STATES)}")

        validate_iso8601(params.get('start'), 'start')
        validate_iso8601(params.get('end'), 'end')
        validate_start_before_end(params['start'], params['end'])

        payload = {
            'rule_id': params['rule_id'],
            'env': self._ctx.env,
            'ack_states': ack_states or ['ack', 'ack_all'],
            'start': params['start'],
            'end': params['end'],
        }

        res = await self._ctx.nats_client.request(
            f'api.iot.db.{self._ctx.org_id}.alerts.ack_history',
            json.dumps(payload).encode(),
            timeout=20,
        )

        decoded = json.loads(res.data.decode())

        if decoded.get('status') == 'ALERT_ACK_FETCH_SUCCESS':
            data = decoded.get('data', {})
            return {
                'has_more': data.get('has_more'),
                'cursor': data.get('cursor'),
                'data': data.get('data'),
            }

        return decoded


    # ─── Ack / AckAll ──────────────────────────────────────────

    async def ack(self, params):
        validate_connected(self._ctx.connected)

        if not params.get('device_id'):
            raise ValueError('device_id is required')
        if not params.get('alert_id'):
            raise ValueError('alert_id is required')
        if not params.get('acked_by'):
            raise ValueError('acked_by is required')

        # Check local ephemeral owner
        engine = self._ephemeral_engines.get(params['alert_id'])
        if engine and engine.mode == 'owner':
            return await engine.ack(params['acked_by'], params.get('ack_notes'))

        # Check ephemeral without local engine — RPC to owner
        meta = self._alert_metadata.get(params['alert_id'])
        if meta and meta.get('type') == 'EPHEMERAL':
            subject = f"{self._ctx.org_id}.{self._ctx.env}.alerts.custom.{params['alert_id']}.ack"

            data = msgpack.packb({
                'status': 'acknowledged',
                'device_id': params['device_id'],
                'ack': {
                    'acked_by': params['acked_by'],
                    'ack_notes': params.get('ack_notes'),
                    'acked_at': int(time.time() * 1000),
                },
            })

            res = await self._ctx.nats_client.request(subject, data, timeout=10)
            result = json.loads(res.data.decode())
            return result.get('status') == 'ACK_SUCCESS'

        # Non-ephemeral — backend
        res = await self._request('ack', {
            'device_id': params['device_id'],
            'rule_id': params['alert_id'],
            'acked_by': params['acked_by'],
            'env': self._ctx.env,
            'ack_notes': params.get('ack_notes'),
        })

        return res.get('status') == 'ALERT_ACK_SUCCESS'

    async def ack_all(self, params):
        validate_connected(self._ctx.connected)

        if not params.get('alert_id'):
            raise ValueError('alert_id is required')
        if not params.get('acked_by'):
            raise ValueError('acked_by is required')

        engine = self._ephemeral_engines.get(params['alert_id'])
        if engine and engine.mode == 'owner':
            return await engine.ack_all(params['acked_by'], params.get('ack_notes'))

        meta = self._alert_metadata.get(params['alert_id'])
        if meta and meta.get('type') == 'EPHEMERAL':
            subject = f"{self._ctx.org_id}.{self._ctx.env}.alerts.custom.{params['alert_id']}.ack_all"

            data = msgpack.packb({
                'status': 'acknowledged',
                'ack': {
                    'acked_by': params['acked_by'],
                    'ack_notes': params.get('ack_notes'),
                    'acked_at': int(time.time() * 1000),
                },
            })

            res = await self._ctx.nats_client.request(subject, data, timeout=10)
            result = json.loads(res.data.decode())
            return result.get('status') == 'ACK_SUCCESS'

        res = await self._request('ack_all', {
            'rule_id': params['alert_id'],
            'acked_by': params['acked_by'],
            'env': self._ctx.env,
            'ack_notes': params.get('ack_notes'),
        })

        return res.get('status') == 'ALERT_ACK_SUCCESS'


    # ─── Mute / Unmute ─────────────────────────────────────────

    async def mute(self, params):
        validate_connected(self._ctx.connected)

        if not params.get('id'):
            raise ValueError('id is required')

        mute_config = params.get('mute_config')
        if not mute_config:
            raise ValueError('mute_config is required')

        if mute_config.get('type') not in ('FOREVER', 'TIME_BASED'):
            raise ValueError('mute_config.type must be FOREVER or TIME_BASED')

        if mute_config.get('type') == 'TIME_BASED' and not mute_config.get('mute_till'):
            raise ValueError('mute_till is required for TIME_BASED mute')

        # Ephemeral — RPC to owner
        meta = self._alert_metadata.get(params['id'])
        if meta and meta.get('type') == 'EPHEMERAL':
            subject = f"{self._ctx.org_id}.{self._ctx.env}.alerts.custom.{params['id']}.mute"
            data = msgpack.packb({'mute_config': mute_config})
            res = await self._ctx.nats_client.request(subject, data, timeout=10)
            return json.loads(res.data.decode())

        # Non-ephemeral — backend
        payload = {'rule_id': params['id'], 'type': mute_config['type']}

        if mute_config.get('type') == 'TIME_BASED':
            payload['mute_till'] = mute_config['mute_till']

        return await self._request('mute', payload)

    async def unmute(self, alert_id):
        validate_connected(self._ctx.connected)

        if not alert_id:
            raise ValueError('id is required')

        meta = self._alert_metadata.get(alert_id)
        if meta and meta.get('type') == 'EPHEMERAL':
            subject = f"{self._ctx.org_id}.{self._ctx.env}.alerts.custom.{alert_id}.mute"
            data = msgpack.packb({'mute_config': {'type': 'CLEAR'}})
            res = await self._ctx.nats_client.request(subject, data, timeout=10)
            return json.loads(res.data.decode())

        return await self._request('mute', {'rule_id': alert_id, 'type': 'CLEAR'})


    # ─── Listen (non-ephemeral) ────────────────────────────────

    async def _listen(self, rule, callbacks):
        validate_connected(self._ctx.connected)

        rule_id = rule['id']
        subject = f"import.{self._ctx.org_id}.{self._ctx.env}.alerts.listen.{rule_id}.*"
        stream = f'{self._ctx.org_id}_stream'
        consumer_name_prefix = f'apppy_alert_listen_{rule_id}'

        callback_map = {
            'fire': callbacks.get('on_fire'),
            'resolved': callbacks.get('on_resolved'),
            'ack': callbacks.get('on_ack'),
            'ack_all': callbacks.get('on_ack_all'),
        }

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

        self._listen_consumers[rule_id] = sub

        async def msg_handler(msg):
            data = msgpack.unpackb(msg.data, raw=False)
            await msg.ack()

            tokens = msg.subject.split('.')
            event_type = tokens[-1]
            cb = callback_map.get(event_type)

            if cb:
                transformed = dict(data)

                if isinstance(transformed.get('timestamp'), (int, float)):
                    from datetime import datetime, timezone
                    dt = datetime.fromtimestamp(transformed['timestamp'] / 1000, tz=timezone.utc)
                    transformed['timestamp'] = dt.strftime('%Y-%m-%dT%H:%M:%S.') + f'{dt.microsecond // 1000:03d}Z'

                await invoke_callback(cb, transformed)

        self._ctx.register_subscription({
            'key': f'alerts:{rule_id}',
            'type': 'jetstream',
            'subject': subject,
            'stream': stream,
            'consumer_name_prefix': consumer_name_prefix,
            'callback': msg_handler,
            'sub_ref': sub,
        })

        asyncio.create_task(self._consume(sub, msg_handler))

    async def _consume(self, sub, handler):
        try:
            async for msg in sub.messages:
                try:
                    await handler(msg)
                except Exception as e:
                    self._ctx.logger.error('Error processing alert message', e)
        except Exception as e:
            self._ctx.logger.error('Alert consumer loop ended', e)


    # ─── Cleanup ───────────────────────────────────────────────

    async def delete_all_consumers(self):
        for rule_id, sub in list(self._listen_consumers.items()):
            try:
                await sub.unsubscribe()
            except Exception as e:
                self._ctx.logger.error(f'Failed to unsubscribe alert listener {rule_id}', e)
            self._ctx.unregister_subscription(f'alerts:{rule_id}')

        self._listen_consumers.clear()

        for rule_id, engine in list(self._ephemeral_engines.items()):
            try:
                await engine.stop()
            except Exception as e:
                self._ctx.logger.error(f'Failed to stop ephemeral engine {rule_id}', e)

        self._ephemeral_engines.clear()
