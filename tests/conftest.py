"""
Shared fixtures for all tests.

Provides mock NATS client, JetStream, KV bucket, and a fully wired-up
Context object that every manager test can import.
"""

import asyncio
import json
import msgpack
import pytest
from unittest.mock import AsyncMock, MagicMock, PropertyMock

from relayx_app_sdk.context import Context


# ---------------------------------------------------------------------------
# Mock NATS message
# ---------------------------------------------------------------------------

class MockMsg:
    """Simulates a NATS message with .data, .subject, .ack(), .respond()."""

    def __init__(self, data, subject='test.subject'):
        self.data = data
        self.subject = subject
        self.ack = AsyncMock()
        self.respond = AsyncMock()


def make_nats_response(payload_dict):
    """Build a MockMsg whose .data is JSON-encoded bytes."""
    return MockMsg(json.dumps(payload_dict).encode())


def make_msgpack_response(payload_dict, subject='test.subject'):
    """Build a MockMsg whose .data is msgpack-encoded bytes."""
    return MockMsg(msgpack.packb(payload_dict), subject=subject)


# ---------------------------------------------------------------------------
# Mock JetStream subscription
# ---------------------------------------------------------------------------

class MockJetStreamSub:
    """Simulates a JetStream push subscription returned by js.subscribe()."""

    def __init__(self):
        self.unsubscribe = AsyncMock()
        self.messages = self._empty_iter()

    async def _empty_iter(self):
        return
        yield  # makes this an async generator


# ---------------------------------------------------------------------------
# Context fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_nats_client():
    """A mock nats.Client with .request() and .subscribe()."""

    client = AsyncMock()
    client.request = AsyncMock()
    client.subscribe = AsyncMock(return_value=MockJetStreamSub())
    client.close = AsyncMock()

    return client


@pytest.fixture
def mock_jetstream():
    """A mock JetStreamContext with .subscribe() and .publish()."""

    js = AsyncMock()
    js.subscribe = AsyncMock(return_value=MockJetStreamSub())
    js.publish = AsyncMock()

    return js


class MockKVEntry:
    """Simulates a NATS KV entry with .value and .revision."""

    def __init__(self, value, revision=1):
        self.value = value
        self.revision = revision


class MockKVBucket:
    """Simulates a NATS KV bucket backed by an in-memory dict."""

    def __init__(self):
        self._store = {}
        self._revision = 0

    async def get(self, key):
        if key not in self._store:
            raise KeyError(f'key not found: {key}')
        return MockKVEntry(self._store[key], self._revision)

    async def put(self, key, value):
        self._revision += 1
        self._store[key] = value
        return self._revision

    async def update(self, key, value, revision):
        self._revision += 1
        self._store[key] = value
        return self._revision

    async def create(self, key, value):
        if key in self._store:
            raise KeyError(f'key already exists: {key}')
        self._revision += 1
        self._store[key] = value
        return self._revision

    async def delete(self, key):
        self._store.pop(key, None)

    async def purge(self, key):
        self._store.pop(key, None)


@pytest.fixture
def mock_kv_bucket():
    """A mock KV bucket backed by an in-memory dict."""
    return MockKVBucket()


@pytest.fixture
def ctx(mock_nats_client, mock_jetstream, mock_kv_bucket):
    """
    A fully-wired Context with mocked NATS infrastructure.

    - connected = True  (most tests need this)
    - device manager attached with a pre-populated cache
    """

    context = Context(
        api_key='test-key',
        secret='test-secret',
        org_id='org123',
        env='test',
    )

    context.nats_client = mock_nats_client
    context.jetstream = mock_jetstream
    context.kv_bucket = mock_kv_bucket
    context.connected = True

    # Attach a minimal mock DeviceManager so resolve_device_id works
    device_mgr = AsyncMock()
    device_mgr.cache = {
        'sensor-1': {'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}}},
        'sensor-2': {'id': 'dev-id-2', 'ident': 'sensor-2', 'schema': {'humidity': {}}},
    }

    async def _resolve(ident):
        cached = device_mgr.cache.get(ident)
        if cached:
            return cached['id']
        raise ValueError(f'Device not found: {ident}')

    async def _resolve_many(idents):
        ids = []
        for i in idents:
            try:
                ids.append(await _resolve(i))
            except ValueError:
                pass
        return ids

    device_mgr.resolve_device_id = _resolve
    device_mgr.resolve_device_ids = _resolve_many
    device_mgr.get = AsyncMock(return_value={'id': 'dev-id-1', 'ident': 'sensor-1', 'schema': {'temp': {}}})
    device_mgr.list = AsyncMock(return_value=[])
    device_mgr.clear_cache = MagicMock()

    context.device = device_mgr

    return context
