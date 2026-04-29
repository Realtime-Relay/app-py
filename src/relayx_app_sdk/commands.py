import json
import time
import msgpack
from datetime import datetime, timezone

from .utils import stream_history, decode_stored_value
from .validation import (
    validate_ident, validate_command_name, validate_non_empty_list,
    validate_connected, validate_iso8601, validate_callable,
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

        on_frame = params.get('on_frame')
        if on_frame is not None:
            validate_callable(on_frame, 'on_frame')

        # Resolve device idents to IDs.
        id_to_ident = {}
        device_ids = []
        command_history = {}

        for ident in params['device_idents']:
            command_history[ident] = []
            try:
                device_id = await self._ctx.device.resolve_device_id(ident)
                device_ids.append(device_id)
                id_to_ident[device_id] = ident
            except (ValueError, Exception):
                command_history[ident] = {'error': 'Device not found'}

        if not device_ids:
            return command_history

        payload = {
            'device_ids': device_ids,
            'env': self._ctx.env,
            'command_name': params['name'],
            'start': params['start'],
            'end': end,
        }
        if params.get('interval'):
            payload['interval'] = params['interval']
        if params.get('aggregate_fn'):
            payload['aggregate_fn'] = params['aggregate_fn']

        result = await stream_history(
            self._ctx,
            f'api.iot.db.{self._ctx.org_id}.command.history',
            payload,
            on_frame=on_frame,
        )

        if result.get('error'):
            raise RuntimeError(
                f"Command history failed: {result.get('error_message') or result.get('status')}"
            )

        for frame in result['frames']:
            data = frame.get('data') if isinstance(frame, dict) else None
            if not data:
                continue
            for device_id, point in data.items():
                ident = id_to_ident.get(device_id, device_id)
                if not isinstance(command_history.get(ident), list):
                    # was marked unfound, but server returned something
                    command_history[ident] = []
                value = decode_stored_value(point.get('value'))
                command_history[ident].append({
                    'value': value,
                    'timestamp': point.get('timestamp'),
                })

        return command_history
