import json

from .validation import validate_ident, validate_rpc_name, validate_connected, validate_positive_number


class RPCManager:

    def __init__(self, ctx):
        self._ctx = ctx

    async def call(self, params):
        validate_connected(self._ctx.connected)
        validate_ident(params.get('device_ident'), 'device_ident')
        validate_rpc_name(params.get('name'))

        timeout = params.get('timeout')
        if timeout is not None:
            validate_positive_number(timeout, 'timeout')

        if params.get('data') is None:
            raise ValueError('data is required')

        timeout_s = timeout if timeout else 10

        device_id = await self._ctx.device.resolve_device_id(params['device_ident'])
        subject = f"{self._ctx.org_id}.{self._ctx.env}.command.rpc.{device_id}.{params['name']}"

        data = json.dumps(params['data']).encode()
        res = await self._ctx.nats_client.request(subject, data, timeout=timeout_s)

        return json.loads(res.data.decode())
