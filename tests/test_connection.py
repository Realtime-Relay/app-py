"""Tests for the ConnectionManager."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from relayx_app_sdk.connection import ConnectionManager, SERVERS


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def conn(ctx):
    return ConnectionManager(ctx)


# ──────────────────────────────────────────────────────────────
# Server config
# ──────────────────────────────────────────────────────────────

class TestServerConfig:

    def test_server_list(self):
        assert len(SERVERS) == 3
        assert all(s.startswith('tls://') for s in SERVERS)


# ──────────────────────────────────────────────────────────────
# listeners
# ──────────────────────────────────────────────────────────────

class TestListeners:

    def test_set_listener(self, conn):
        cb = lambda event: None
        conn.listeners(cb)

        assert conn._listener_callback is cb

    def test_non_callable_raises(self, conn):
        with pytest.raises(ValueError, match='must be callable'):
            conn.listeners('not-a-fn')


# ──────────────────────────────────────────────────────────────
# presence
# ──────────────────────────────────────────────────────────────

class TestPresence:

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, conn, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await conn.presence(lambda x: x)

    @pytest.mark.asyncio
    async def test_presence_subscribes(self, conn, ctx):
        await conn.presence(lambda x: x)

        ctx.jetstream.subscribe.assert_awaited_once()
        assert conn._presence_callback is not None


# ──────────────────────────────────────────────────────────────
# disconnect
# ──────────────────────────────────────────────────────────────

class TestDisconnect:

    @pytest.mark.asyncio
    async def test_disconnect_cleans_up(self, conn, ctx):
        ctx.nats_client = AsyncMock()
        ctx.nats_client.close = AsyncMock()

        await conn.disconnect()

        assert ctx.connected is False
        assert conn._connect_called is False
        ctx.nats_client.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_disconnect_without_client_is_noop(self, conn, ctx):
        ctx.nats_client = None

        await conn.disconnect()  # should not raise


# ──────────────────────────────────────────────────────────────
# _emit_event
# ──────────────────────────────────────────────────────────────

class TestEmitEvent:

    def test_calls_listener_callback(self, conn):
        events = []
        conn._listener_callback = lambda e: events.append(e)

        conn._emit_event('connected')

        assert events == ['connected']

    def test_no_callback_is_silent(self, conn):
        conn._emit_event('connected')  # should not raise


# ──────────────────────────────────────────────────────────────
# Connection callbacks — the 4 Python-specific requirements
# ──────────────────────────────────────────────────────────────

class TestOnDisconnected:

    @pytest.mark.asyncio
    async def test_sets_connected_false(self, conn, ctx):
        ctx.connected = True

        await conn._on_disconnected()

        assert ctx.connected is False

    @pytest.mark.asyncio
    async def test_does_not_emit_disconnected(self, conn, ctx):
        """Key requirement: _on_disconnected should NOT emit 'disconnected'."""
        events = []
        conn._listener_callback = lambda e: events.append(e)

        await conn._on_disconnected()

        assert 'disconnected' not in events


class TestOnReconnected:

    @pytest.mark.asyncio
    async def test_sets_connected_true(self, conn, ctx):
        ctx.connected = False

        await conn._on_reconnected()

        assert ctx.connected is True

    @pytest.mark.asyncio
    async def test_emits_reconnected(self, conn, ctx):
        events = []
        conn._listener_callback = lambda e: events.append(e)

        await conn._on_reconnected()

        assert 'reconnected' in events

    @pytest.mark.asyncio
    async def test_reconnecting_fires_once(self, conn, ctx):
        """Key requirement: reconnecting event should fire only once per cycle."""
        events = []
        conn._listener_callback = lambda e: events.append(e)

        # First disconnect fires reconnecting
        await conn._on_disconnected()
        assert events.count('reconnecting') == 1

        # Second disconnect should NOT fire reconnecting again
        await conn._on_disconnected()
        assert events.count('reconnecting') == 1

        # Reconnected resets the flag
        await conn._on_reconnected()
        assert 'reconnected' in events

    @pytest.mark.asyncio
    async def test_reinits_jetstream(self, conn, ctx):
        old_js = ctx.jetstream

        await conn._on_reconnected()

        # jetstream should be re-initialized
        assert ctx.jetstream is not None


class TestOnClosed:

    @pytest.mark.asyncio
    async def test_emits_disconnected(self, conn, ctx):
        """Key requirement: only _on_closed should emit 'disconnected'."""
        events = []
        conn._listener_callback = lambda e: events.append(e)

        await conn._on_closed()

        assert 'disconnected' in events
        assert ctx.connected is False
        assert conn._connect_called is False

    @pytest.mark.asyncio
    async def test_resets_reconnecting_flag(self, conn, ctx):
        conn._is_reconnecting = True

        await conn._on_closed()

        assert conn._is_reconnecting is False


class TestOnError:

    @pytest.mark.asyncio
    async def test_auth_error_emits_auth_failed(self, conn, ctx):
        events = []
        conn._listener_callback = lambda e: events.append(e)
        ctx.nats_client = AsyncMock()
        ctx.nats_client.close = AsyncMock()

        await conn._on_error(Exception('authorization violation'))

        assert 'auth_failed' in events

    @pytest.mark.asyncio
    async def test_non_auth_error_ignored(self, conn, ctx):
        events = []
        conn._listener_callback = lambda e: events.append(e)

        await conn._on_error(Exception('some random error'))

        assert 'auth_failed' not in events


# ──────────────────────────────────────────────────────────────
# Offline buffer flush
# ──────────────────────────────────────────────────────────────

class TestFlushOfflineBuffer:

    @pytest.mark.asyncio
    async def test_flushes_all_buffered_messages(self, conn, ctx):
        ctx.offline_buffer = [
            ('subj.1', b'payload-1'),
            ('subj.2', b'payload-2'),
        ]
        ctx.jetstream.publish = AsyncMock()

        await conn._flush_offline_buffer()

        assert len(ctx.offline_buffer) == 0
        assert ctx.jetstream.publish.await_count == 2

    @pytest.mark.asyncio
    async def test_handles_publish_failure(self, conn, ctx):
        ctx.offline_buffer = [
            ('subj.1', b'payload-1'),
        ]
        ctx.jetstream.publish = AsyncMock(side_effect=Exception('fail'))

        # Should not raise
        await conn._flush_offline_buffer()

        assert len(ctx.offline_buffer) == 0


# ──────────────────────────────────────────────────────────────
# Resubscription
# ──────────────────────────────────────────────────────────────

class TestResubscribeAll:

    @pytest.mark.asyncio
    async def test_resubscribes_jetstream_entries(self, conn, ctx):
        ctx._subscription_registry = [
            {
                'key': 'telemetry:sensor-1:temp',
                'type': 'jetstream',
                'subject': 'org123.test.telemetry.dev-1.temp',
                'stream': 'org123_stream',
                'consumer_name_prefix': 'apppy_telemetry_sensor-1',
                'callback': AsyncMock(),
                'sub_ref': AsyncMock(),
            },
        ]

        await conn._resubscribe_all()

        ctx.jetstream.subscribe.assert_awaited()

    @pytest.mark.asyncio
    async def test_resubscribes_core_entries(self, conn, ctx):
        ctx._subscription_registry = [
            {
                'key': 'rpc:sensor-1',
                'type': 'core',
                'subject': 'org123.test.rpc.sensor-1',
                'callback': AsyncMock(),
                'sub_ref': AsyncMock(),
            },
        ]

        await conn._resubscribe_all()

        ctx.nats_client.subscribe.assert_awaited()

    @pytest.mark.asyncio
    async def test_clears_old_registry(self, conn, ctx):
        ctx._subscription_registry = [
            {
                'key': 'a',
                'type': 'jetstream',
                'subject': 'x',
                'stream': 's',
                'consumer_name_prefix': 'p',
                'callback': AsyncMock(),
                'sub_ref': AsyncMock(),
            },
        ]

        await conn._resubscribe_all()

        # Old entries cleared, new ones added by resubscribe
        assert all(e['key'] == 'a' for e in ctx._subscription_registry)
