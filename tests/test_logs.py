"""Tests for the LogManager — stream, history, off, cleanup."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from relayx_app_sdk.logs import LogManager


@pytest.fixture
def logs(ctx):
    return LogManager(ctx)


# ──────────────────────────────────────────────────────────────
# stream
# ──────────────────────────────────────────────────────────────

class TestLogStream:

    @pytest.mark.asyncio
    async def test_subscribes_with_wildcard(self, logs, ctx):
        await logs.stream({
            'device_ident': 'sensor-1',
            'levels': '*',
            'callback': MagicMock(),
        })

        ctx.jetstream.subscribe.assert_awaited_once()
        assert ctx.jetstream.subscribe.call_args[0][0].endswith('.*')

    @pytest.mark.asyncio
    async def test_subscribes_with_level_list(self, logs, ctx):
        await logs.stream({
            'device_ident': 'sensor-1',
            'levels': ['info', 'warn'],
            'callback': MagicMock(),
        })

        ctx.jetstream.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_default_levels_undefined(self, logs, ctx):
        # No 'levels' key — defaults to all
        await logs.stream({
            'device_ident': 'sensor-1',
            'callback': MagicMock(),
        })
        ctx.jetstream.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_invalid_string_raises(self, logs):
        with pytest.raises(ValueError, match='must be "\\*"'):
            await logs.stream({
                'device_ident': 'sensor-1',
                'levels': 'info',
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_invalid_level_in_list_raises(self, logs):
        with pytest.raises(ValueError, match='invalid values'):
            await logs.stream({
                'device_ident': 'sensor-1',
                'levels': ['debug'],
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_empty_list_raises(self, logs):
        with pytest.raises(ValueError, match='non-empty list'):
            await logs.stream({
                'device_ident': 'sensor-1',
                'levels': [],
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, logs, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await logs.stream({
                'device_ident': 'sensor-1',
                'levels': '*',
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_missing_callback_raises(self, logs):
        with pytest.raises(ValueError, match='callback is required'):
            await logs.stream({
                'device_ident': 'sensor-1',
                'levels': '*',
            })


# ──────────────────────────────────────────────────────────────
# off
# ──────────────────────────────────────────────────────────────

class TestLogOff:

    @pytest.mark.asyncio
    async def test_unsubscribes(self, logs, ctx):
        await logs.stream({
            'device_ident': 'sensor-1',
            'levels': '*',
            'callback': lambda x: x,
        })

        await logs.off({'device_ident': 'sensor-1'})

        assert 'sensor-1' not in logs._consumers

    @pytest.mark.asyncio
    async def test_off_silent_when_no_sub(self, logs):
        await logs.off({'device_ident': 'nonexistent'})  # should not raise


# ──────────────────────────────────────────────────────────────
# history
# ──────────────────────────────────────────────────────────────

class TestLogHistory:

    @pytest.mark.asyncio
    async def test_returns_logs_grouped_by_level(self, logs, ctx, monkeypatch):
        async def fake_stream_history(c, subject, payload, on_frame=None):
            assert 'log.history' in subject
            return {
                'status': 'LOG_FETCH_STREAM_STARTED',
                'frames': [
                    {'last': True, 'data': {
                        'info': {'value': 'hello', 'timestamp': 100},
                        'error': {'value': 'oops', 'timestamp': 101},
                    }},
                ],
                'error': False,
                'error_message': None,
            }

        import relayx_app_sdk.logs as logs_module
        monkeypatch.setattr(logs_module, 'stream_history', fake_stream_history)

        result = await logs.history({
            'device_ident': 'sensor-1',
            'start': '2024-01-01T00:00:00Z',
            'end': '2024-01-02T00:00:00Z',
        })

        assert result['info'] == [{'value': 'hello', 'timestamp': 100}]
        assert result['error'] == [{'value': 'oops', 'timestamp': 101}]
        assert result['warn'] == []

    @pytest.mark.asyncio
    async def test_decodes_json_string_values(self, logs, ctx, monkeypatch):
        async def fake_stream_history(c, subject, payload, on_frame=None):
            return {
                'status': 'LOG_FETCH_STREAM_STARTED',
                'frames': [
                    {'last': True, 'data': {
                        'info': {'value': '{"k":"v"}', 'timestamp': 100},
                    }},
                ],
                'error': False,
                'error_message': None,
            }

        import relayx_app_sdk.logs as logs_module
        monkeypatch.setattr(logs_module, 'stream_history', fake_stream_history)

        result = await logs.history({
            'device_ident': 'sensor-1',
            'start': '2024-01-01T00:00:00Z',
            'end': '2024-01-02T00:00:00Z',
        })

        # decode_stored_value should JSON-parse the string
        assert result['info'][0]['value'] == {'k': 'v'}

    @pytest.mark.asyncio
    async def test_validates_levels(self, logs):
        with pytest.raises(ValueError, match='invalid values'):
            await logs.history({
                'device_ident': 'sensor-1',
                'levels': ['debug'],
                'start': '2024-01-01T00:00:00Z',
                'end': '2024-01-02T00:00:00Z',
            })

    @pytest.mark.asyncio
    async def test_validates_start_end(self, logs):
        with pytest.raises(ValueError, match='start must be before end'):
            await logs.history({
                'device_ident': 'sensor-1',
                'start': '2024-01-02T00:00:00Z',
                'end': '2024-01-01T00:00:00Z',
            })

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, logs, ctx):
        ctx.connected = False
        with pytest.raises(RuntimeError, match='Not connected'):
            await logs.history({
                'device_ident': 'sensor-1',
                'start': '2024-01-01T00:00:00Z',
                'end': '2024-01-02T00:00:00Z',
            })

    @pytest.mark.asyncio
    async def test_error_status_raises(self, logs, ctx, monkeypatch):
        async def fake_stream_history(c, subject, payload, on_frame=None):
            return {'status': 'LOG_FETCH_FAILURE', 'frames': [], 'error': True, 'error_message': 'boom'}

        import relayx_app_sdk.logs as logs_module
        monkeypatch.setattr(logs_module, 'stream_history', fake_stream_history)

        with pytest.raises(RuntimeError, match='Log history failed'):
            await logs.history({
                'device_ident': 'sensor-1',
                'start': '2024-01-01T00:00:00Z',
                'end': '2024-01-02T00:00:00Z',
            })


# ──────────────────────────────────────────────────────────────
# delete_all_consumers
# ──────────────────────────────────────────────────────────────

class TestLogCleanup:

    @pytest.mark.asyncio
    async def test_clears_all(self, logs, ctx):
        await logs.stream({
            'device_ident': 'sensor-1',
            'levels': '*',
            'callback': lambda x: x,
        })

        await logs.delete_all_consumers()

        assert len(logs._consumers) == 0
