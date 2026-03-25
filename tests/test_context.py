"""Tests for the Context class."""

import pytest
from unittest.mock import AsyncMock

from relayx_app_sdk.context import Context


# ──────────────────────────────────────────────────────────────
# Construction
# ──────────────────────────────────────────────────────────────

class TestContextInit:

    def test_stores_fields(self):
        ctx = Context('key', 'secret', 'org1', 'test')

        assert ctx.api_key == 'key'
        assert ctx.secret == 'secret'
        assert ctx.org_id == 'org1'
        assert ctx.env == 'test'

    def test_defaults(self):
        ctx = Context('k', 's', 'o', 'production')

        assert ctx.nats_client is None
        assert ctx.jetstream is None
        assert ctx.kv_bucket is None
        assert ctx.connected is False
        assert ctx.offline_buffer == []
        assert ctx.device is None
        assert ctx._subscription_registry == []


# ──────────────────────────────────────────────────────────────
# publish_or_buffer
# ──────────────────────────────────────────────────────────────

class TestPublishOrBuffer:

    @pytest.mark.asyncio
    async def test_publishes_when_connected(self):
        ctx = Context('k', 's', 'o', 'test')
        ctx.connected = True
        ctx.jetstream = AsyncMock()
        ctx.jetstream.publish = AsyncMock(return_value='ack-obj')

        result = await ctx.publish_or_buffer('subj', b'payload')

        assert result == 'ack-obj'
        ctx.jetstream.publish.assert_awaited_once_with('subj', b'payload')
        assert len(ctx.offline_buffer) == 0

    @pytest.mark.asyncio
    async def test_buffers_when_disconnected(self):
        ctx = Context('k', 's', 'o', 'test')
        ctx.connected = False

        result = await ctx.publish_or_buffer('subj', b'payload')

        assert result is None
        assert len(ctx.offline_buffer) == 1
        assert ctx.offline_buffer[0] == ('subj', b'payload')

    @pytest.mark.asyncio
    async def test_buffers_on_publish_failure(self):
        ctx = Context('k', 's', 'o', 'test')
        ctx.connected = True
        ctx.jetstream = AsyncMock()
        ctx.jetstream.publish = AsyncMock(side_effect=Exception('timeout'))

        result = await ctx.publish_or_buffer('subj', b'payload')

        assert result is None
        assert len(ctx.offline_buffer) == 1

    @pytest.mark.asyncio
    async def test_buffers_when_no_jetstream(self):
        ctx = Context('k', 's', 'o', 'test')
        ctx.connected = True
        ctx.jetstream = None

        result = await ctx.publish_or_buffer('subj', b'payload')

        assert result is None
        assert len(ctx.offline_buffer) == 1


# ──────────────────────────────────────────────────────────────
# Subscription registry
# ──────────────────────────────────────────────────────────────

class TestSubscriptionRegistry:

    def test_register_and_unregister(self):
        ctx = Context('k', 's', 'o', 'test')

        ctx.register_subscription({'key': 'tel:sensor-1:temp', 'type': 'jetstream'})
        ctx.register_subscription({'key': 'events:alert', 'type': 'jetstream'})

        assert len(ctx._subscription_registry) == 2

        ctx.unregister_subscription('tel:sensor-1:temp')

        assert len(ctx._subscription_registry) == 1
        assert ctx._subscription_registry[0]['key'] == 'events:alert'

    def test_unregister_nonexistent_key(self):
        ctx = Context('k', 's', 'o', 'test')
        ctx.register_subscription({'key': 'a', 'type': 'jetstream'})

        ctx.unregister_subscription('nonexistent')

        assert len(ctx._subscription_registry) == 1

    def test_register_multiple_with_same_key(self):
        ctx = Context('k', 's', 'o', 'test')

        ctx.register_subscription({'key': 'dup', 'type': 'jetstream'})
        ctx.register_subscription({'key': 'dup', 'type': 'core'})

        assert len(ctx._subscription_registry) == 2

        ctx.unregister_subscription('dup')

        assert len(ctx._subscription_registry) == 0
