import json
import time

from .validation import validate_ident, validate_dict, validate_connected


CACHE_TTL = 2 * 60 * 60  # 2 hours in seconds


class DeviceManager:

    def __init__(self, ctx):
        self._ctx = ctx
        self._cache = {}
        self._cache_timestamp = 0

    @property
    def cache(self):
        return self._cache

    def _is_cache_valid(self):
        return len(self._cache) > 0 and (time.time() - self._cache_timestamp) < CACHE_TTL

    def _refresh_cache_timestamp(self):
        self._cache_timestamp = time.time()

    def _subject(self, op):
        return f'api.iot.devices.{self._ctx.org_id}.{op}'

    async def _request(self, op, payload):
        data = json.dumps(payload).encode()
        res = await self._ctx.nats_client.request(self._subject(op), data, timeout=20)

        return json.loads(res.data.decode())

    async def resolve_device_id(self, ident):
        cached = self._cache.get(ident)
        if cached and cached.get('id'):
            return cached['id']

        device = await self.get({'ident': ident})

        if device and isinstance(device, dict) and device.get('id'):
            return device['id']

        raise ValueError(f'Device not found: {ident}')

    async def resolve_device_ids(self, idents):
        ids = []

        for ident in idents:
            try:
                device_id = await self.resolve_device_id(ident)
                ids.append(device_id)
            except (ValueError, Exception):
                pass

        return ids

    async def create(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('ident'), 'ident')
        validate_dict(params.get('schema'), 'schema')
        validate_dict(params.get('config'), 'config')

        res = await self._request('create', {
            'ident': params['ident'],
            'env': self._ctx.env,
            'schema': params['schema'],
            'config': params['config'],
        })

        if res.get('status') == 'DEVICE_CREATE_SUCCESS':
            self._cache[params['ident']] = res['data']
            return res['data']

        return None

    async def update(self, params):
        validate_connected(self._ctx.connected)

        if not params.get('id'):
            raise ValueError('id is required')

        payload = {'id': params['id']}

        if params.get('ident'):
            payload['ident'] = params['ident']
        if params.get('schema'):
            payload['schema'] = params['schema']
        if params.get('config'):
            payload['config'] = params['config']

        res = await self._request('update', payload)

        if res.get('status') == 'DEVICE_UPDATE_SUCCESS':
            device = res.get('data', {}).get('device')
            if device and device.get('ident'):
                self._cache[device['ident']] = device
            return device

        return None

    async def delete(self, ident):
        validate_connected(self._ctx.connected)
        validate_ident(ident, 'ident')

        res = await self._request('delete', {'ident': ident})

        if res.get('status') == 'DEVICE_DELETE_SUCCESS':
            self._cache.pop(ident, None)

        return res.get('status') == 'DEVICE_DELETE_SUCCESS'

    async def list(self):
        validate_connected(self._ctx.connected)

        res = await self._request('list', {})

        if res.get('status') == 'DEVICE_FETCH_SUCCESS':
            self._cache.clear()

            for device in res.get('data', {}).get('devices', []):
                self._cache[device['ident']] = device

            self._refresh_cache_timestamp()

        return res.get('data', {}).get('devices', [])

    async def get(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('ident'), 'ident')

        cached = self._cache.get(params['ident'])
        if cached and self._is_cache_valid():
            return cached

        res = await self._request('get', {'ident': params['ident']})

        if res.get('status') == 'DEVICE_GET_SUCCESS':
            self._cache[params['ident']] = res['data']
            return res['data']

        return res

    def clear_cache(self):
        self._cache.clear()
        self._cache_timestamp = 0
