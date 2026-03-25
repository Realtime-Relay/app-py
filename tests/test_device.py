"""Tests for the DeviceManager."""

import json
import time
import pytest
from unittest.mock import AsyncMock

from relayx_app_sdk.device import DeviceManager, CACHE_TTL
from tests.conftest import make_nats_response


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def device_mgr(ctx):
    """DeviceManager wired to the shared test context."""
    mgr = DeviceManager(ctx)
    ctx.device = mgr
    return mgr


# ──────────────────────────────────────────────────────────────
# resolve_device_id
# ──────────────────────────────────────────────────────────────

class TestResolveDeviceId:

    @pytest.mark.asyncio
    async def test_returns_id_from_cache(self, device_mgr):
        device_mgr._cache['sensor-1'] = {'id': 'abc', 'ident': 'sensor-1'}

        result = await device_mgr.resolve_device_id('sensor-1')
        assert result == 'abc'

    @pytest.mark.asyncio
    async def test_fetches_and_caches_on_miss(self, device_mgr, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'status': 'DEVICE_GET_SUCCESS',
                'data': {'id': 'xyz', 'ident': 'new-device'},
            })
        )
        device_mgr._cache_timestamp = time.time()

        result = await device_mgr.resolve_device_id('new-device')
        assert result == 'xyz'

    @pytest.mark.asyncio
    async def test_raises_on_not_found(self, device_mgr, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'status': 'DEVICE_NOT_FOUND',
            })
        )

        with pytest.raises(ValueError, match='Device not found'):
            await device_mgr.resolve_device_id('ghost')


# ──────────────────────────────────────────────────────────────
# resolve_device_ids
# ──────────────────────────────────────────────────────────────

class TestResolveDeviceIds:

    @pytest.mark.asyncio
    async def test_resolves_multiple(self, device_mgr):
        device_mgr._cache = {
            'a': {'id': '1', 'ident': 'a'},
            'b': {'id': '2', 'ident': 'b'},
        }

        result = await device_mgr.resolve_device_ids(['a', 'b'])
        assert result == ['1', '2']

    @pytest.mark.asyncio
    async def test_skips_not_found(self, device_mgr, ctx):
        device_mgr._cache = {'a': {'id': '1', 'ident': 'a'}}
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'NOT_FOUND'})
        )

        result = await device_mgr.resolve_device_ids(['a', 'missing'])
        assert result == ['1']


# ──────────────────────────────────────────────────────────────
# CRUD — create
# ──────────────────────────────────────────────────────────────

class TestDeviceCreate:

    @pytest.mark.asyncio
    async def test_successful_create(self, device_mgr, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'status': 'DEVICE_CREATE_SUCCESS',
                'data': {'id': 'new-id', 'ident': 'sensor-new'},
            })
        )

        result = await device_mgr.create({
            'ident': 'sensor-new',
            'schema': {'temp': {}},
            'config': {'interval': 60},
        })

        assert result['id'] == 'new-id'
        assert device_mgr._cache['sensor-new']['id'] == 'new-id'

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, device_mgr, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await device_mgr.create({
                'ident': 'x',
                'schema': {},
                'config': {},
            })

    @pytest.mark.asyncio
    async def test_missing_ident_raises(self, device_mgr):
        with pytest.raises(ValueError, match='ident is required'):
            await device_mgr.create({'schema': {}, 'config': {}})

    @pytest.mark.asyncio
    async def test_missing_schema_raises(self, device_mgr):
        with pytest.raises(ValueError, match='schema must be a dict'):
            await device_mgr.create({'ident': 'x', 'config': {}})

    @pytest.mark.asyncio
    async def test_create_returns_none_on_failure(self, device_mgr, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'DEVICE_CREATE_FAILED'})
        )

        result = await device_mgr.create({
            'ident': 'x',
            'schema': {},
            'config': {},
        })

        assert result is None


# ──────────────────────────────────────────────────────────────
# CRUD — update
# ──────────────────────────────────────────────────────────────

class TestDeviceUpdate:

    @pytest.mark.asyncio
    async def test_successful_update(self, device_mgr, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'status': 'DEVICE_UPDATE_SUCCESS',
                'data': {
                    'device': {'id': 'dev-1', 'ident': 'sensor-1'},
                },
            })
        )

        result = await device_mgr.update({
            'id': 'dev-1',
            'schema': {'temp': {}, 'humidity': {}},
        })

        assert result['id'] == 'dev-1'
        assert 'sensor-1' in device_mgr._cache

    @pytest.mark.asyncio
    async def test_missing_id_raises(self, device_mgr):
        with pytest.raises(ValueError, match='id is required'):
            await device_mgr.update({'ident': 'sensor-1'})


# ──────────────────────────────────────────────────────────────
# CRUD — delete
# ──────────────────────────────────────────────────────────────

class TestDeviceDelete:

    @pytest.mark.asyncio
    async def test_successful_delete(self, device_mgr, ctx):
        device_mgr._cache['sensor-1'] = {'id': 'x'}

        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'DEVICE_DELETE_SUCCESS'})
        )

        result = await device_mgr.delete('sensor-1')

        assert result is True
        assert 'sensor-1' not in device_mgr._cache

    @pytest.mark.asyncio
    async def test_delete_failure(self, device_mgr, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'DEVICE_DELETE_FAILED'})
        )

        result = await device_mgr.delete('sensor-1')
        assert result is False


# ──────────────────────────────────────────────────────────────
# CRUD — list
# ──────────────────────────────────────────────────────────────

class TestDeviceList:

    @pytest.mark.asyncio
    async def test_list_populates_cache(self, device_mgr, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'status': 'DEVICE_FETCH_SUCCESS',
                'data': {
                    'devices': [
                        {'id': '1', 'ident': 'a'},
                        {'id': '2', 'ident': 'b'},
                    ],
                },
            })
        )

        devices = await device_mgr.list()

        assert len(devices) == 2
        assert 'a' in device_mgr._cache
        assert 'b' in device_mgr._cache


# ──────────────────────────────────────────────────────────────
# CRUD — get
# ──────────────────────────────────────────────────────────────

class TestDeviceGet:

    @pytest.mark.asyncio
    async def test_returns_from_valid_cache(self, device_mgr):
        device_mgr._cache['sensor-1'] = {'id': 'cached-id', 'ident': 'sensor-1'}
        device_mgr._cache_timestamp = time.time()

        result = await device_mgr.get({'ident': 'sensor-1'})
        assert result['id'] == 'cached-id'

    @pytest.mark.asyncio
    async def test_fetches_when_cache_expired(self, device_mgr, ctx):
        device_mgr._cache['sensor-1'] = {'id': 'stale', 'ident': 'sensor-1'}
        device_mgr._cache_timestamp = time.time() - CACHE_TTL - 1

        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'status': 'DEVICE_GET_SUCCESS',
                'data': {'id': 'fresh', 'ident': 'sensor-1'},
            })
        )

        result = await device_mgr.get({'ident': 'sensor-1'})
        assert result['id'] == 'fresh'


# ──────────────────────────────────────────────────────────────
# Cache helpers
# ──────────────────────────────────────────────────────────────

class TestCacheHelpers:

    def test_clear_cache(self, device_mgr):
        device_mgr._cache['x'] = {'id': '1'}
        device_mgr._cache_timestamp = time.time()

        device_mgr.clear_cache()

        assert device_mgr._cache == {}
        assert device_mgr._cache_timestamp == 0

    def test_cache_property(self, device_mgr):
        device_mgr._cache['x'] = {'id': '1'}
        assert device_mgr.cache == {'x': {'id': '1'}}
