"""Tests for the EventManager."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from relayx_app_sdk.events import EventManager


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def events(ctx):
    return EventManager(ctx)


# ──────────────────────────────────────────────────────────────
# stream
# ──────────────────────────────────────────────────────────────

class TestEventStream:

    @pytest.mark.asyncio
    async def test_subscribes_and_returns_true(self, events, ctx):
        result = await events.stream({
            'name': 'door-open',
            'callback': lambda x: x,
        })

        assert result is True
        ctx.jetstream.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_duplicate_returns_false(self, events, ctx):
        await events.stream({
            'name': 'door-open',
            'callback': lambda x: x,
        })

        result = await events.stream({
            'name': 'door-open',
            'callback': lambda x: x,
        })

        assert result is False

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, events, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await events.stream({
                'name': 'door-open',
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_missing_name_raises(self, events):
        with pytest.raises(ValueError, match='event name is required'):
            await events.stream({
                'callback': lambda x: x,
            })

    @pytest.mark.asyncio
    async def test_missing_callback_raises(self, events):
        with pytest.raises(ValueError, match='callback is required'):
            await events.stream({
                'name': 'door-open',
            })

    @pytest.mark.asyncio
    async def test_registers_subscription(self, events, ctx):
        await events.stream({
            'name': 'door-open',
            'callback': lambda x: x,
        })

        keys = [e['key'] for e in ctx._subscription_registry]
        assert 'events:door-open' in keys


# ──────────────────────────────────────────────────────────────
# off
# ──────────────────────────────────────────────────────────────

class TestEventOff:

    @pytest.mark.asyncio
    async def test_unsubscribes_event(self, events, ctx):
        await events.stream({
            'name': 'door-open',
            'callback': lambda x: x,
        })

        await events.off({'name': 'door-open'})

        assert 'door-open' not in events._consumers
        keys = [e['key'] for e in ctx._subscription_registry]
        assert 'events:door-open' not in keys

    @pytest.mark.asyncio
    async def test_off_nonexistent_is_silent(self, events):
        # Should not raise
        await events.off({'name': 'nonexistent'})


# ──────────────────────────────────────────────────────────────
# delete_all_consumers
# ──────────────────────────────────────────────────────────────

class TestEventCleanup:

    @pytest.mark.asyncio
    async def test_clears_all(self, events, ctx):
        await events.stream({'name': 'event-a', 'callback': lambda x: x})
        await events.stream({'name': 'event-b', 'callback': lambda x: x})

        await events.delete_all_consumers()

        assert len(events._consumers) == 0
        assert len(events._callbacks) == 0
