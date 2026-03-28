import json
import time
import msgpack
from datetime import datetime, timezone

from .validation import (
    validate_ident, validate_command_name, validate_non_empty_list,
    validate_connected, validate_iso8601,
)


class CommandManager:

    def __init__(self, ctx):
        self._ctx = ctx

    async def send(self, params):
        validate_command_name(params.get('name'))
        validate_non_empty_list(params.get('device_ident'), 'device_ident')

        if params.get('data') is None:
            raise ValueError('data is required')

        for ident in params['device_ident']:
            validate_ident(ident, 'device_ident[]')

        result = {}

        for ident in params['device_ident']:
            try:
                device_id = await self._ctx.device.resolve_device_id(ident)
            except (ValueError, Exception):
                result[ident] = {'sent': False, 'error': 'Device not found'}
                continue

            subject = f"{self._ctx.org_id}.{self._ctx.env}.command.queue.{device_id}.{params['name']}"

            payload = msgpack.packb({
                'value': params['data'],
                'timestamp': int(time.time() * 1000),
            })

            ack = await self._ctx.publish_or_buffer(subject, payload)

            if ack is not None:
                result[ident] = {'sent': True}
            else:
                result[ident] = {'sent': False, 'buffered': True}

        return result

    async def history(self, params):
        validate_connected(self._ctx.connected)
        validate_command_name(params.get('name'))
        validate_non_empty_list(params.get('device_idents'), 'device_idents')
        validate_iso8601(params.get('start'), 'start')

        for ident in params['device_idents']:
            validate_ident(ident, 'device_idents[]')

        end = params.get('end')
        if end:
            validate_iso8601(end, 'end')
        else:
            now = datetime.now(timezone.utc)
            end = now.strftime('%Y-%m-%dT%H:%M:%S.') + f'{now.microsecond // 1000:03d}Z'

        # Resolve device idents to IDs
        id_to_ident = {}
        device_ids = []
        unfound = []

        command_history = {}

        start_cursor = params['start']

        for ident in params['device_idents']:
            try:
                device_id = await self._ctx.device.resolve_device_id(ident)
                device_ids.append(device_id)
                id_to_ident[device_id] = ident

                command_history[ident] = []
            except (ValueError, Exception):
                unfound.append(ident)

        if not device_ids:
            result = {}
            for ident in unfound:
                result[ident] = {'error': 'Device not found'}
            return result

        # Fetch history
        while True:
            data = json.dumps({ 
                'device_ids': device_ids,
                'env': self._ctx.env,
                'command_name': params['name'],
                'start': start_cursor,
                'end': end,
            }).encode("utf-8")

            res = None

            try:
                res = await self._ctx.nats_client.request(
                    f'api.iot.db.{self._ctx.org_id}.command.history',
                    data,
                    timeout=20,
                )

                decoded = msgpack.unpackb(res.data, raw=False)

                if decoded["status"] == "COMMAND_FETCH_SUCCESS":
                    command_data = decoded["data"]

                    has_more = command_data["has_more"]

                    for device_id, records in command_data['data'].items():
                        ident = id_to_ident.get(device_id, device_id)
                        command_history[ident] = command_history[ident] + records

                    if has_more:
                        start_cursor = command_data["cursor"]

                        continue
                    else:
                        break
                else:
                    break

            except Exception as e:
                print(e)
                raise ValueError("Command history request timed-out")

        return command_history
