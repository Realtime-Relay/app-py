"""Tests for the RPCManager."""

import json
import pytest
from unittest.mock import AsyncMock

from relayx_app_sdk.rpc import RPCManager
from tests.conftest import make_nats_response


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def rpc(ctx):
    return RPCManager(ctx)


# ──────────────────────────────────────────────────────────────
# call
# ──────────────────────────────────────────────────────────────

class TestRPCCall:

    @pytest.mark.asyncio
    async def test_successful_call(self, rpc, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'ok', 'result': 42})
        )

        result = await rpc.call({
            'device_ident': 'sensor-1',
            'name': 'get_status',
            'data': {'verbose': True},
        })

        assert result['status'] == 'ok'
        assert result['result'] == 42

        call_args = ctx.nats_client.request.call_args
        subject = call_args[0][0]
        assert 'command.rpc.dev-id-1.get_status' in subject

    @pytest.mark.asyncio
    async def test_custom_timeout(self, rpc, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'ok'})
        )

        await rpc.call({
            'device_ident': 'sensor-1',
            'name': 'slow_op',
            'data': {},
            'timeout': 30,
        })

        call_kwargs = ctx.nats_client.request.call_args[1]
        assert call_kwargs['timeout'] == 30

    @pytest.mark.asyncio
    async def test_default_timeout(self, rpc, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'ok'})
        )

        await rpc.call({
            'device_ident': 'sensor-1',
            'name': 'op',
            'data': {},
        })

        call_kwargs = ctx.nats_client.request.call_args[1]
        assert call_kwargs['timeout'] == 10

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, rpc, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await rpc.call({
                'device_ident': 'sensor-1',
                'name': 'op',
                'data': {},
            })

    @pytest.mark.asyncio
    async def test_missing_device_ident_raises(self, rpc):
        with pytest.raises(ValueError, match='device_ident is required'):
            await rpc.call({'name': 'op', 'data': {}})

    @pytest.mark.asyncio
    async def test_missing_name_raises(self, rpc):
        with pytest.raises(ValueError, match='rpc name is required'):
            await rpc.call({'device_ident': 'sensor-1', 'data': {}})

    @pytest.mark.asyncio
    async def test_missing_data_raises(self, rpc):
        with pytest.raises(ValueError, match='data is required'):
            await rpc.call({'device_ident': 'sensor-1', 'name': 'op'})

    @pytest.mark.asyncio
    async def test_invalid_timeout_raises(self, rpc):
        with pytest.raises(ValueError, match='must be a non-negative number'):
            await rpc.call({
                'device_ident': 'sensor-1',
                'name': 'op',
                'data': {},
                'timeout': -5,
            })
