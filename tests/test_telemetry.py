"""Tests for the TelemetryManager."""

import json
import msgpack
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from relayx_app_sdk.telemetry import TelemetryManager
from tests.conftest import make_nats_response, MockMsg


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
    async def test_subscribes_with_wildcard_subject_for_star(self, telemetry, ctx):
        callback = MagicMock()

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': '*',
            'callback': callback,
        })

        ctx.jetstream.subscribe.assert_awaited_once()
        call_args = ctx.jetstream.subscribe.call_args
        assert call_args[0][0].endswith('.*')

    @pytest.mark.asyncio
    async def test_subscribes_with_wildcard_subject_for_list(self, telemetry, ctx):
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': ['temp'],
            'callback': MagicMock(),
        })

        ctx.jetstream.subscribe.assert_awaited_once()
        call_args = ctx.jetstream.subscribe.call_args
        assert call_args[0][0].endswith('.*')

    @pytest.mark.asyncio
    async def test_throws_on_non_star_string(self, telemetry):
        with pytest.raises(ValueError, match='metric as a string must be'):
            await telemetry.stream({
                'device_ident': 'sensor-1',
                'metric': 'temp',
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_throws_on_non_string_non_list(self, telemetry):
        with pytest.raises(ValueError, match='metric must be'):
            await telemetry.stream({
                'device_ident': 'sensor-1',
                'metric': 123,
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_throws_on_empty_list(self, telemetry):
        with pytest.raises(ValueError, match='non-empty list'):
            await telemetry.stream({
                'device_ident': 'sensor-1',
                'metric': [],
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_validates_each_metric_in_list(self, telemetry, ctx):
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}},
        })

        with pytest.raises(ValueError, match='not a valid key'):
            await telemetry.stream({
                'device_ident': 'sensor-1',
                'metric': ['temp', 'nonexistent'],
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_allows_multiple_subscriptions_same_device(self, telemetry, ctx):
        ctx.device.cache['sensor-1'] = {'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}, 'humidity': {}}}
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}, 'humidity': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': ['temp'],
            'callback': MagicMock(),
        })
        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': ['humidity'],
            'callback': MagicMock(),
        })

        assert len(telemetry._consumers['sensor-1']) == 2
        assert ctx.jetstream.subscribe.await_count == 2

    @pytest.mark.asyncio
    async def test_returns_none(self, telemetry, ctx):
        result = await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': '*',
            'callback': MagicMock(),
        })

        assert result is None

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, telemetry, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await telemetry.stream({
                'device_ident': 'sensor-1',
                'metric': '*',
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_missing_callback_raises(self, telemetry):
        with pytest.raises(ValueError, match='callback is required'):
            await telemetry.stream({
                'device_ident': 'sensor-1',
                'metric': '*',
            })

    @pytest.mark.asyncio
    async def test_registers_subscription(self, telemetry, ctx):
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': ['temp'],
            'callback': lambda x: x,
        })

        keys = [e['key'] for e in ctx._subscription_registry]
        assert any('telemetry:sensor-1:' in k for k in keys)


# ──────────────────────────────────────────────────────────────
# off
# ──────────────────────────────────────────────────────────────

class TestTelemetryOff:

    @pytest.mark.asyncio
    async def test_unsubscribes_all_for_device(self, telemetry, ctx):
        ctx.device.cache['sensor-1'] = {'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}, 'humidity': {}}}
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}, 'humidity': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': ['temp'],
            'callback': lambda x: x,
        })
        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': ['humidity'],
            'callback': lambda x: x,
        })

        await telemetry.off({'device_ident': 'sensor-1'})

        assert 'sensor-1' not in telemetry._consumers

    @pytest.mark.asyncio
    async def test_removes_specific_metrics_from_filtered_sub(self, telemetry, ctx):
        ctx.device.cache['sensor-1'] = {'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}, 'humidity': {}}}
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}, 'humidity': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': ['temp', 'humidity'],
            'callback': lambda x: x,
        })

        await telemetry.off({
            'device_ident': 'sensor-1',
            'metric': ['temp'],
        })

        # Subscription still alive with humidity
        assert 'sensor-1' in telemetry._consumers
        assert len(telemetry._consumers['sensor-1']) == 1
        assert 'humidity' in telemetry._consumers['sensor-1'][0]['metrics']
        assert 'temp' not in telemetry._consumers['sensor-1'][0]['metrics']

    @pytest.mark.asyncio
    async def test_deletes_sub_when_all_metrics_removed(self, telemetry, ctx):
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': ['temp'],
            'callback': lambda x: x,
        })

        await telemetry.off({
            'device_ident': 'sensor-1',
            'metric': ['temp'],
        })

        assert 'sensor-1' not in telemetry._consumers

    @pytest.mark.asyncio
    async def test_leaves_wildcard_untouched(self, telemetry, ctx):
        ctx.device.get = AsyncMock(return_value={
            'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}},
        })

        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': '*',
            'callback': lambda x: x,
        })
        await telemetry.stream({
            'device_ident': 'sensor-1',
            'metric': ['temp'],
            'callback': lambda x: x,
        })

        await telemetry.off({
            'device_ident': 'sensor-1',
            'metric': ['temp'],
        })

        # Wildcard sub still alive, filtered sub removed
        assert 'sensor-1' in telemetry._consumers
        assert len(telemetry._consumers['sensor-1']) == 1
        assert telemetry._consumers['sensor-1'][0]['metrics'] is None

    @pytest.mark.asyncio
    async def test_noop_when_no_subscriptions(self, telemetry):
        # Should not raise
        await telemetry.off({'device_ident': 'sensor-1'})
        await telemetry.off({'device_ident': 'sensor-1', 'metric': ['temp']})


# ──────────────────────────────────────────────────────────────
# history
# ──────────────────────────────────────────────────────────────

class TestTelemetryHistory:

    @pytest.mark.asyncio
    async def test_sends_correct_request(self, telemetry, ctx, monkeypatch):
        # Mock the streaming protocol via stream_history
        async def fake_stream_history(c, subject, payload, on_frame=None):
            return {
                'status': 'TELEMETRY_FETCH_STREAM_STARTED',
                'frames': [
                    {'last': True, 'data': {'temp': {'value': 25, 'timestamp': 123}}},
                ],
                'error': False,
                'error_message': None,
            }

        import relayx_app_sdk.telemetry as tlm_module
        monkeypatch.setattr(tlm_module, 'stream_history', fake_stream_history)

        result = await telemetry.history({
            'device_ident': 'sensor-1',
            'fields': ['temp'],
            'start': '2024-01-01T00:00:00Z',
            'end': '2024-01-02T00:00:00Z',
        })

        assert result == {'temp': [{'value': 25, 'timestamp': 123}]}

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
    async def test_calls_history_endpoint(self, telemetry, ctx, monkeypatch):
        captured = {}

        async def fake_stream_history(c, subject, payload, on_frame=None):
            captured['subject'] = subject
            captured['payload'] = payload
            return {
                'status': 'TELEMETRY_FETCH_STREAM_STARTED',
                'frames': [
                    {'last': True, 'data': {'temp': {'value': 30, 'timestamp': '2026-03-24T00:00:00Z'}}},
                ],
                'error': False,
                'error_message': None,
            }

        import relayx_app_sdk.telemetry as tlm_module
        monkeypatch.setattr(tlm_module, 'stream_history', fake_stream_history)

        result = await telemetry.latest({
            'device_ident': 'sensor-1',
            'fields': ['temp'],
            'start': '2026-03-23T00:00:00Z',
            'end': '2026-03-24T00:00:00Z',
        })

        assert 'telemetry.history' in captured['subject']
        assert captured['payload']['last_value'] is True
        assert result == {'temp': {'value': 30, 'timestamp': '2026-03-24T00:00:00Z'}}


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
            'metric': ['temp'],
            'callback': lambda x: x,
        })

        await telemetry.delete_all_consumers()

        assert len(telemetry._consumers) == 0
