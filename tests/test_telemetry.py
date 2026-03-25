"""Tests for the TelemetryManager."""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from relayx_app_sdk.telemetry import TelemetryManager
from tests.conftest import make_nats_response


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def telemetry(ctx):
    return TelemetryManager(ctx)


# ──────────────────────────────────────────────────────────────
# stream
# ──────────────────────────────────────────────────────────────

class TestTelemetryStream:

    @pytest.mark.asyncio
    async def test_subscribes_and_returns_true(self, telemetry, ctx):
        callback = MagicMock()

        # _validate_metric needs device with schema
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1',
            'ident': 'sensor-1',
            'schema': {'temp': {}},
        })

        result = await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': 'temp',
            'callback': callback,
        })

        assert result is True
        ctx.jetstream.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_duplicate_stream_returns_false(self, telemetry, ctx):
        callback = MagicMock()

        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1',
            'ident': 'sensor-1',
            'schema': {'temp': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': 'temp',
            'callback': callback,
        })

        result = await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': 'temp',
            'callback': callback,
        })

        assert result is False

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, telemetry, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await telemetry.stream({
                'device_ident': 'sensor-1',
                'metric': 'temp',
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_missing_metric_raises(self, telemetry):
        with pytest.raises(ValueError, match='metric is required'):
            await telemetry.stream({
                'device_ident': 'sensor-1',
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_missing_callback_raises(self, telemetry):
        with pytest.raises(ValueError, match='callback is required'):
            await telemetry.stream({
                'device_ident': 'sensor-1',
                'metric': 'temp',
            })

    @pytest.mark.asyncio
    async def test_wildcard_metric_skips_validation(self, telemetry, ctx):
        callback = MagicMock()

        result = await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': '*',
            'callback': callback,
        })

        assert result is True

    @pytest.mark.asyncio
    async def test_invalid_metric_raises(self, telemetry, ctx):
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1',
            'ident': 'sensor-1',
            'schema': {'temp': {}},
        })

        with pytest.raises(ValueError, match='not a valid key'):
            await telemetry.stream({
                'device_ident': 'sensor-1',
                'metric': 'nonexistent',
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_registers_subscription(self, telemetry, ctx):
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1',
            'ident': 'sensor-1',
            'schema': {'temp': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': 'temp',
            'callback': lambda x: x,
        })

        keys = [e['key'] for e in ctx._subscription_registry]
        assert 'telemetry:sensor-1:temp' in keys


# ──────────────────────────────────────────────────────────────
# off
# ──────────────────────────────────────────────────────────────

class TestTelemetryOff:

    @pytest.mark.asyncio
    async def test_unsubscribes_specific_metrics(self, telemetry, ctx):
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1',
            'ident': 'sensor-1',
            'schema': {'temp': {}, 'humidity': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': 'temp',
            'callback': lambda x: x,
        })

        await telemetry.off({
            'device_ident': 'sensor-1',
            'metric': ['temp'],
        })

        assert 'sensor-1:temp' not in telemetry._consumers

    @pytest.mark.asyncio
    async def test_unsubscribes_all_for_device(self, telemetry, ctx):
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1',
            'ident': 'sensor-1',
            'schema': {'temp': {}, 'humidity': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': 'temp',
            'callback': lambda x: x,
        })

        await telemetry.off({'device_ident': 'sensor-1'})

        matching = [k for k in telemetry._consumers if k.startswith('sensor-1:')]
        assert len(matching) == 0


# ──────────────────────────────────────────────────────────────
# history
# ──────────────────────────────────────────────────────────────

class TestTelemetryHistory:

    @pytest.mark.asyncio
    async def test_sends_correct_request(self, telemetry, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'data': [{'temp': 25}]})
        )

        result = await telemetry.history({
            'device_ident': 'sensor-1',
            'fields': ['temp'],
            'start': '2024-01-01T00:00:00Z',
            'end': '2024-01-02T00:00:00Z',
        })

        assert result == {'data': [{'temp': 25}]}

        call_args = ctx.nats_client.request.call_args
        assert 'telemetry.history' in call_args[0][0]

    @pytest.mark.asyncio
    async def test_validates_fields(self, telemetry, ctx):
        with pytest.raises(ValueError, match='must be a non-empty list'):
            await telemetry.history({
                'device_ident': 'sensor-1',
                'fields': [],
                'start': '2024-01-01T00:00:00Z',
                'end': '2024-01-02T00:00:00Z',
            })

    @pytest.mark.asyncio
    async def test_validates_start_before_end(self, telemetry, ctx):
        with pytest.raises(ValueError, match='start must be before end'):
            await telemetry.history({
                'device_ident': 'sensor-1',
                'fields': ['temp'],
                'start': '2024-01-02T00:00:00Z',
                'end': '2024-01-01T00:00:00Z',
            })


# ──────────────────────────────────────────────────────────────
# latest
# ──────────────────────────────────────────────────────────────

class TestTelemetryLatest:

    @pytest.mark.asyncio
    async def test_calls_history_endpoint(self, telemetry, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'data': [{'temp': 30}]})
        )

        result = await telemetry.latest({
            'device_ident': 'sensor-1',
            'fields': ['temp'],
        })

        call_args = ctx.nats_client.request.call_args
        assert 'telemetry.history' in call_args[0][0]


# ──────────────────────────────────────────────────────────────
# delete_all_consumers
# ──────────────────────────────────────────────────────────────

class TestTelemetryCleanup:

    @pytest.mark.asyncio
    async def test_clears_all_consumers(self, telemetry, ctx):
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1',
            'ident': 'sensor-1',
            'schema': {'temp': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': 'temp',
            'callback': lambda x: x,
        })

        await telemetry.delete_all_consumers()

        assert len(telemetry._consumers) == 0
