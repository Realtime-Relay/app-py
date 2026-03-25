import json

from .validation import validate_ident, validate_dict, validate_connected


class NotificationManager:

    def __init__(self, ctx):
        self._ctx = ctx

    def _subject(self, op):
        return f'api.iot.notification.{self._ctx.org_id}.{op}'

    async def _request(self, op, payload):
        data = json.dumps(payload).encode()
        res = await self._ctx.nats_client.request(self._subject(op), data, timeout=20)

        return json.loads(res.data.decode())

    async def create(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('name'), 'name')

        notif_type = params.get('type')
        if notif_type not in ('WEBHOOK', 'EMAIL'):
            raise ValueError('type must be "WEBHOOK" or "EMAIL"')

        validate_dict(params.get('config'), 'config')
        config = params['config']

        if notif_type == 'WEBHOOK':
            if not config.get('endpoint'):
                raise ValueError('config.endpoint is required for WEBHOOK')

        if notif_type == 'EMAIL':
            if not isinstance(config.get('recipients'), list):
                raise ValueError('config.recipients is required for EMAIL')

            if not config.get('subject'):
                raise ValueError('config.subject is required for EMAIL')

            if not config.get('template'):
                raise ValueError('config.template is required for EMAIL')

        res = await self._request('create', {
            'name': params['name'],
            'type': notif_type,
            'config': config,
        })

        return res.get('data')

    async def update(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('name'), 'name')

        notif_type = params.get('type')
        if notif_type not in ('WEBHOOK', 'EMAIL'):
            raise ValueError('type must be "WEBHOOK" or "EMAIL"')

        validate_dict(params.get('config'), 'config')

        payload = {
            'name': params['name'],
            'type': notif_type,
            'config': params['config'],
        }

        if params.get('id'):
            payload['id'] = params['id']

        res = await self._request('update', payload)

        return res.get('data')

    async def delete(self, notif_id):
        validate_connected(self._ctx.connected)

        if not notif_id:
            raise ValueError('id is required')

        res = await self._request('delete', {'id': notif_id})
        return res.get('status') == 'NOTIFICATION_DELETE_SUCCESS'

    async def list(self):
        validate_connected(self._ctx.connected)

        res = await self._request('list', {})
        return res.get('data', [])

    async def get(self, notif_id):
        validate_connected(self._ctx.connected)

        if not notif_id:
            raise ValueError('id is required')

        return await self._request('get', {'id': notif_id})
