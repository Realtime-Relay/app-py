"""Tests for the HeirarchyGroupManager."""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock

from relayx_app_sdk.heirarchy_group import HeirarchyGroupManager
from tests.conftest import make_nats_response


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def heirarchy_group(ctx):
    return HeirarchyGroupManager(ctx)


# ──────────────────────────────────────────────────────────────
# create
# ──────────────────────────────────────────────────────────────

class TestHeirarchyGroupCreate:

    @pytest.mark.asyncio
    async def test_successful_create(self, heirarchy_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': {
                    'id': 'hg-1',
                    'name': 'building-a',
                    'heirarchy': 'building.floor.room',
                },
            })
        )

        result = await heirarchy_group.create({
            'name': 'building-a',
            'heirarchy': 'building.floor.room',
            'device_idents': ['sensor-1'],
        })

        assert result['id'] == 'hg-1'
        assert hasattr(result, 'stream')
        assert hasattr(result, 'off')

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, heirarchy_group, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await heirarchy_group.create({
                'name': 'x',
                'heirarchy': 'a.b',
                'device_idents': [],
            })

    @pytest.mark.asyncio
    async def test_invalid_hierarchy_name_raises(self, heirarchy_group):
        with pytest.raises(ValueError, match='invalid characters'):
            await heirarchy_group.create({
                'name': 'valid-name',
                'heirarchy': 'has spaces here',
                'device_idents': [],
            })


# ──────────────────────────────────────────────────────────────
# update
# ──────────────────────────────────────────────────────────────

class TestHeirarchyGroupUpdate:

    @pytest.mark.asyncio
    async def test_update_hierarchy_path(self, heirarchy_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': {
                    'id': 'hg-1',
                    'heirarchy': 'new.path',
                },
            })
        )

        result = await heirarchy_group.update({
            'id': 'hg-1',
            'heirarchy': 'new.path',
        })

        assert result['id'] == 'hg-1'

    @pytest.mark.asyncio
    async def test_missing_id_raises(self, heirarchy_group):
        with pytest.raises(ValueError, match='id is required'):
            await heirarchy_group.update({'heirarchy': 'a.b'})


# ──────────────────────────────────────────────────────────────
# delete
# ──────────────────────────────────────────────────────────────

class TestHeirarchyGroupDelete:

    @pytest.mark.asyncio
    async def test_successful_delete(self, heirarchy_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'HEIRARCHY_GROUP_DELETE_SUCCESS'})
        )

        result = await heirarchy_group.delete('hg-1')
        assert result is True


# ──────────────────────────────────────────────────────────────
# list / get
# ──────────────────────────────────────────────────────────────

class TestHeirarchyGroupListGet:

    @pytest.mark.asyncio
    async def test_list(self, heirarchy_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': [{'id': 'hg-1'}],
            })
        )

        result = await heirarchy_group.list()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_wraps_result(self, heirarchy_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'status': 'HEIRARCHY_GROUP_GET_SUCCESS',
                'data': {
                    'name': 'building-a',
                    'heirarchy': 'b.f.r',
                },
            })
        )

        result = await heirarchy_group.get('hg-1')

        assert result['id'] == 'hg-1'
        assert hasattr(result, 'stream')
        assert hasattr(result, 'off')


# ──────────────────────────────────────────────────────────────
# stream validation
# ──────────────────────────────────────────────────────────────

class TestHeirarchyGroupStream:

    @pytest.mark.asyncio
    async def test_metric_and_metrics_mutually_exclusive(self, heirarchy_group, ctx):
        """Calling stream with both metric and metrics should raise."""

        # First we need a wrapped group
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'status': 'HEIRARCHY_GROUP_GET_SUCCESS',
                'data': {
                    'name': 'g',
                    'heirarchy': 'a.b',
                },
            })
        )

        group = await heirarchy_group.get('hg-1')

        with pytest.raises(ValueError, match='mutually exclusive'):
            await group.stream({
                'callback': lambda x: x,
                'metric': '*',
                'metrics': ['temp'],
            })

    @pytest.mark.asyncio
    async def test_stream_subscribes(self, heirarchy_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'status': 'HEIRARCHY_GROUP_GET_SUCCESS',
                'data': {
                    'name': 'g',
                    'heirarchy': 'a.b',
                },
            })
        )

        group = await heirarchy_group.get('hg-1')

        await group.stream({
            'callback': lambda x: x,
            'metrics': ['temp'],
        })

        ctx.jetstream.subscribe.assert_awaited_once()

        keys = [e['key'] for e in ctx._subscription_registry]
        assert 'heirarchy_group:hg-1' in keys


# ──────────────────────────────────────────────────────────────
# list_devices
# ──────────────────────────────────────────────────────────────

class TestHeirarchyGroupListDevices:

    @pytest.mark.asyncio
    async def test_returns_devices(self, heirarchy_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': [{'id': 'd1', 'ident': 'sensor-1'}],
            })
        )

        result = await heirarchy_group.list_devices('hg-1')
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_missing_id_raises(self, heirarchy_group):
        with pytest.raises(ValueError, match='id is required'):
            await heirarchy_group.list_devices('')


# ──────────────────────────────────────────────────────────────
# cleanup
# ──────────────────────────────────────────────────────────────

class TestHeirarchyGroupCleanup:

    @pytest.mark.asyncio
    async def test_delete_all_consumers(self, heirarchy_group):
        mock_sub = AsyncMock()
        mock_sub.unsubscribe = AsyncMock()
        heirarchy_group._stream_consumers['hg-1'] = mock_sub

        await heirarchy_group.delete_all_consumers()

        assert len(heirarchy_group._stream_consumers) == 0
