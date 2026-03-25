"""Tests for the LogicalGroupManager."""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock

from relayx_app_sdk.logical_group import LogicalGroupManager
from tests.conftest import make_nats_response


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def logical_group(ctx):
    return LogicalGroupManager(ctx)


# ──────────────────────────────────────────────────────────────
# create
# ──────────────────────────────────────────────────────────────

class TestLogicalGroupCreate:

    @pytest.mark.asyncio
    async def test_successful_create(self, logical_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': {
                    'id': 'grp-1',
                    'name': 'floor-sensors',
                    'tags': ['floor-1'],
                },
            })
        )

        result = await logical_group.create({
            'name': 'floor-sensors',
            'tags': ['floor-1'],
            'device_idents': ['sensor-1'],
        })

        assert result['id'] == 'grp-1'
        assert hasattr(result, 'stream')
        assert hasattr(result, 'off')

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, logical_group, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await logical_group.create({
                'name': 'x',
                'tags': [],
                'device_idents': [],
            })

    @pytest.mark.asyncio
    async def test_missing_name_raises(self, logical_group):
        with pytest.raises(ValueError, match='name is required'):
            await logical_group.create({
                'tags': [],
                'device_idents': [],
            })

    @pytest.mark.asyncio
    async def test_tags_must_be_list(self, logical_group):
        with pytest.raises(ValueError, match='tags must be a list'):
            await logical_group.create({
                'name': 'x',
                'tags': 'not-a-list',
                'device_idents': [],
            })


# ──────────────────────────────────────────────────────────────
# update
# ──────────────────────────────────────────────────────────────

class TestLogicalGroupUpdate:

    @pytest.mark.asyncio
    async def test_update_with_devices_and_tags(self, logical_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': {'id': 'grp-1', 'name': 'updated'},
            })
        )

        result = await logical_group.update({
            'id': 'grp-1',
            'devices': {
                'add': ['sensor-1'],
                'remove': [],
            },
            'tags': {
                'add': ['new-tag'],
                'remove': ['old-tag'],
            },
        })

        assert result['id'] == 'grp-1'

    @pytest.mark.asyncio
    async def test_missing_id_raises(self, logical_group):
        with pytest.raises(ValueError, match='id is required'):
            await logical_group.update({'name': 'x'})


# ──────────────────────────────────────────────────────────────
# delete
# ──────────────────────────────────────────────────────────────

class TestLogicalGroupDelete:

    @pytest.mark.asyncio
    async def test_successful_delete(self, logical_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'LOGICAL_GROUP_DELETE_SUCCESS'})
        )

        result = await logical_group.delete('grp-1')
        assert result is True

    @pytest.mark.asyncio
    async def test_missing_id_raises(self, logical_group):
        with pytest.raises(ValueError, match='id is required'):
            await logical_group.delete('')


# ──────────────────────────────────────────────────────────────
# list
# ──────────────────────────────────────────────────────────────

class TestLogicalGroupList:

    @pytest.mark.asyncio
    async def test_returns_groups(self, logical_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': [
                    {'id': 'g1', 'name': 'a'},
                    {'id': 'g2', 'name': 'b'},
                ],
            })
        )

        result = await logical_group.list()
        assert len(result) == 2


# ──────────────────────────────────────────────────────────────
# get
# ──────────────────────────────────────────────────────────────

class TestLogicalGroupGet:

    @pytest.mark.asyncio
    async def test_wraps_successful_get(self, logical_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'status': 'LOGICAL_GROUP_GET_SUCCESS',
                'data': {'name': 'my-group'},
            })
        )

        result = await logical_group.get('grp-1')

        assert result['id'] == 'grp-1'
        assert result['name'] == 'my-group'
        assert hasattr(result, 'stream')
        assert hasattr(result, 'off')


# ──────────────────────────────────────────────────────────────
# list_devices
# ──────────────────────────────────────────────────────────────

class TestLogicalGroupListDevices:

    @pytest.mark.asyncio
    async def test_returns_device_list(self, logical_group, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': [
                    {'id': 'd1', 'ident': 'sensor-1'},
                ],
            })
        )

        result = await logical_group.list_devices('grp-1')

        assert len(result) == 1
        assert result[0]['ident'] == 'sensor-1'

    @pytest.mark.asyncio
    async def test_missing_id_raises(self, logical_group):
        with pytest.raises(ValueError, match='id is required'):
            await logical_group.list_devices('')


# ──────────────────────────────────────────────────────────────
# cleanup
# ──────────────────────────────────────────────────────────────

class TestLogicalGroupCleanup:

    @pytest.mark.asyncio
    async def test_delete_all_consumers(self, logical_group):
        mock_sub = AsyncMock()
        mock_sub.unsubscribe = AsyncMock()
        logical_group._stream_consumers['g1'] = mock_sub

        await logical_group.delete_all_consumers()

        assert len(logical_group._stream_consumers) == 0
        mock_sub.unsubscribe.assert_awaited_once()
