import asyncio
import json
import uuid
import msgpack
import nats.js.api

from .utils import invoke_callback
from .validation import (
    validate_ident, validate_hierarchy_name, validate_hierarchy_wildcard,
    validate_list, validate_callable, validate_connected,
)


class HeirarchyGroup:

    def __init__(self, data, manager):
        self._data = data
        self._manager = manager

    def __getitem__(self, key):
        return self._data[key]

    def get(self, key, default=None):
        return self._data.get(key, default)

    def items(self):
        return self._data.items()

    async def stream(self, params):
        heirarchy = self._data.get('heirarchy', '')
        await self._manager._stream_group(self._data['id'], heirarchy, params)

    async def off(self):
        await self._manager._off_group(self._data['id'])


class HeirarchyGroupManager:

    def __init__(self, ctx):
        self._ctx = ctx
        self._stream_consumers = {}  # group_id -> sub

    def _subject(self, op):
        return f'api.iot.cohort.{self._ctx.org_id}.heirarchy.{op}'

    async def _request(self, op, payload):
        data = json.dumps(payload).encode()
        res = await self._ctx.nats_client.request(self._subject(op), data, timeout=20)

        return json.loads(res.data.decode())

    def _wrap_group(self, data):
        if not data:
            return data

        return HeirarchyGroup(data, self)


    # ─── CRUD ──────────────────────────────────────────────────

    async def create(self, params):
        validate_connected(self._ctx.connected)
        validate_hierarchy_name(params.get('name'), 'name')
        validate_hierarchy_name(params.get('heirarchy'), 'heirarchy')
        validate_list(params.get('device_idents'), 'device_idents')

        device_ids = await self._ctx.device.resolve_device_ids(params['device_idents'])

        res = await self._request('create', {
            'device_ids': device_ids,
            'name': params['name'],
            'heirarchy': params['heirarchy'],
        })

        if res.get('data'):
            return self._wrap_group(res['data'])

        return res.get('data')

    async def update(self, params):
        validate_connected(self._ctx.connected)

        if not params.get('id'):
            raise ValueError('id is required')

        payload = {'id': params['id']}

        devices = params.get('devices')
        if devices:
            add_idents = devices.get('add', [])
            remove_idents = devices.get('remove', [])

            add_ids = await self._ctx.device.resolve_device_ids(add_idents) if add_idents else []
            remove_ids = await self._ctx.device.resolve_device_ids(remove_idents) if remove_idents else []
            payload['devices'] = {'add': add_ids, 'remove': remove_ids}

        if params.get('heirarchy'):
            validate_hierarchy_name(params['heirarchy'], 'heirarchy')
            payload['heirarchy'] = params['heirarchy']

        res = await self._request('update', payload)

        if res.get('data'):
            return self._wrap_group(res['data'])

        return res.get('data')

    async def delete(self, group_id):
        validate_connected(self._ctx.connected)

        if not group_id:
            raise ValueError('id is required')

        res = await self._request('delete', {'id': group_id})
        return res.get('status') == 'HEIRARCHY_GROUP_DELETE_SUCCESS'

    async def list(self):
        validate_connected(self._ctx.connected)

        res = await self._request('list', {})
        return res.get('data', [])

    async def get(self, group_id):
        validate_connected(self._ctx.connected)

        if not group_id:
            raise ValueError('id is required')

        res = await self._request('get', {'id': group_id})

        if res.get('status') == 'HEIRARCHY_GROUP_GET_SUCCESS':
            res['data']['id'] = group_id
            return self._wrap_group(res['data'])

        return res

    async def list_devices(self, group_id):
        validate_connected(self._ctx.connected)

        if not group_id:
            raise ValueError('id is required')

        data = json.dumps({'id': group_id}).encode()

        res = await self._ctx.nats_client.request(
            f'api.iot.cohort.{self._ctx.org_id}.heirarchy.device.list',
            data,
            timeout=20,
        )

        decoded = json.loads(res.data.decode())
        return decoded.get('data', [])


    # ─── Streaming ─────────────────────────────────────────────

    async def _stream_group(self, group_id, group_heirarchy, params):
        validate_connected(self._ctx.connected)
        validate_callable(params.get('callback'), 'callback')

        if params.get('metric') and params.get('metrics'):
            raise ValueError('metric and metrics are mutually exclusive')

        if params.get('heirarchy'):
            validate_hierarchy_wildcard(params['heirarchy'], 'heirarchy')

        if params.get('metrics'):
            validate_list(params['metrics'], 'metrics')
            for m in params['metrics']:
                validate_ident(m, 'metrics[]')

        # Determine metric token for the subject
        metric_token = '*'
        client_metric_filter = None

        if params.get('metric') == '*':
            metric_token = '*'
        elif params.get('metrics') and len(params['metrics']) == 1:
            metric_token = params['metrics'][0]
        elif params.get('metrics') and len(params['metrics']) > 1:
            metric_token = '*'
            client_metric_filter = params['metrics']

        heirarchy_token = params.get('heirarchy') or group_heirarchy

        # Subscribe
        subject = f'import.{self._ctx.org_id}.{self._ctx.env}.heirarchy.listen.{metric_token}.{heirarchy_token}'
        stream = f'{self._ctx.org_id}_stream'
        consumer_name_prefix = f'apppy_heirarchy_group_{group_id}'

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

        self._stream_consumers[group_id] = sub
        filter_idents = params.get('device_idents')
        callback = params['callback']

        async def msg_handler(msg):
            data = msgpack.unpackb(msg.data, raw=False)
            await msg.ack()

            if filter_idents and data.get('ident') not in filter_idents:
                return

            if client_metric_filter and data.get('metric') not in client_metric_filter:
                return

            await invoke_callback(callback, data)

        self._ctx.register_subscription({
            'key': f'heirarchy_group:{group_id}',
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
                    self._ctx.logger.error('Error processing hierarchy group message', e)
        except Exception as e:
            self._ctx.logger.error('Hierarchy group consumer loop ended', e)

    async def _off_group(self, group_id):
        sub = self._stream_consumers.pop(group_id, None)

        if sub:
            try:
                await sub.unsubscribe()
            except Exception as e:
                self._ctx.logger.error(f'Failed to unsubscribe hierarchy group {group_id}', e)

        self._ctx.unregister_subscription(f'heirarchy_group:{group_id}')

    async def delete_all_consumers(self):
        for group_id, sub in list(self._stream_consumers.items()):
            try:
                await sub.unsubscribe()
            except Exception as e:
                self._ctx.logger.error(f'Failed to unsubscribe hierarchy group {group_id}', e)
            self._ctx.unregister_subscription(f'heirarchy_group:{group_id}')

        self._stream_consumers.clear()
