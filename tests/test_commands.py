"""Tests for the CommandManager."""

import json
import pytest
from unittest.mock import AsyncMock

from relayx_app_sdk.commands import CommandManager
from tests.conftest import make_nats_response, make_msgpack_response


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def command(ctx):
    return CommandManager(ctx)


# ──────────────────────────────────────────────────────────────
# send
# ──────────────────────────────────────────────────────────────

class TestCommandSend:

    @pytest.mark.asyncio
    async def test_sends_to_single_device(self, command, ctx):
        ctx.jetstream.publish = AsyncMock(return_value='ack')

        result = await command.send({
            'name': 'reboot',
            'device_ident': ['sensor-1'],
            'data': {'force': True},
        })

        assert result['sensor-1']['sent'] is True

    @pytest.mark.asyncio
    async def test_sends_to_multiple_devices(self, command, ctx):
        ctx.jetstream.publish = AsyncMock(return_value='ack')

        result = await command.send({
            'name': 'reboot',
            'device_ident': ['sensor-1', 'sensor-2'],
            'data': {'force': True},
        })

        assert result['sensor-1']['sent'] is True
        assert result['sensor-2']['sent'] is True

    @pytest.mark.asyncio
    async def test_buffers_when_disconnected(self, command, ctx):
        ctx.connected = False

        result = await command.send({
            'name': 'reboot',
            'device_ident': ['sensor-1'],
            'data': {'force': True},
        })

        assert result['sensor-1']['buffered'] is True
        assert result['sensor-1']['sent'] is False

    @pytest.mark.asyncio
    async def test_handles_unknown_device(self, command, ctx):
        # Remove from cache so resolve fails
        ctx.device.cache.pop('sensor-1', None)

        async def _resolve_fail(ident):
            raise ValueError(f'Device not found: {ident}')

        ctx.device.resolve_device_id = _resolve_fail

        result = await command.send({
            'name': 'reboot',
            'device_ident': ['sensor-1'],
            'data': {},
        })

        assert result['sensor-1']['sent'] is False
        assert 'error' in result['sensor-1']

    @pytest.mark.asyncio
    async def test_missing_name_raises(self, command):
        with pytest.raises(ValueError, match='command name is required'):
            await command.send({
                'device_ident': ['sensor-1'],
                'data': {},
            })

    @pytest.mark.asyncio
    async def test_missing_device_ident_raises(self, command):
        with pytest.raises(ValueError, match='must be a non-empty list'):
            await command.send({
                'name': 'reboot',
                'data': {},
            })

    @pytest.mark.asyncio
    async def test_missing_data_raises(self, command):
        with pytest.raises(ValueError, match='data is required'):
            await command.send({
                'name': 'reboot',
                'device_ident': ['sensor-1'],
            })


# ──────────────────────────────────────────────────────────────
# history
# ──────────────────────────────────────────────────────────────

class TestCommandHistory:

    @pytest.mark.asyncio
    async def test_returns_history(self, command, ctx, monkeypatch):
        async def fake_stream_history(c, subject, payload, on_frame=None):
            return {
                'status': 'COMMAND_FETCH_STREAM_STARTED',
                'frames': [
                    {'last': True, 'data': {'dev-id-1': {'value': 'ok', 'timestamp': 123}}},
                ],
                'error': False,
                'error_message': None,
            }

        import relayx_app_sdk.commands as cmd_module
        monkeypatch.setattr(cmd_module, 'stream_history', fake_stream_history)

        result = await command.history({
            'name': 'reboot',
            'device_idents': ['sensor-1'],
            'start': '2024-01-01T00:00:00Z',
        })

        assert 'sensor-1' in result
        assert result['sensor-1'][0]['value'] == 'ok'

    @pytest.mark.asyncio
    async def test_unfound_devices(self, command, ctx):
        async def _resolve_fail(ident):
            raise ValueError(f'not found: {ident}')

        ctx.device.resolve_device_id = _resolve_fail

        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'data': {}})
        )

        result = await command.history({
            'name': 'reboot',
            'device_idents': ['ghost'],
            'start': '2024-01-01T00:00:00Z',
        })

        assert result['ghost']['error'] == 'Device not found'

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, command, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await command.history({
                'name': 'reboot',
                'device_idents': ['sensor-1'],
                'start': '2024-01-01T00:00:00Z',
            })
