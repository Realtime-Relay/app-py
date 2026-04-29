"""Tests for the utils module — build_credentials, topic_pattern_matcher,
decode_stored_value, stream_history."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from relayx_app_sdk.utils import (
    build_credentials, topic_pattern_matcher, decode_stored_value, stream_history,
)


# ──────────────────────────────────────────────────────────────
# build_credentials
# ──────────────────────────────────────────────────────────────

class TestBuildCredentials:

    def test_contains_jwt(self):
        result = build_credentials('my-jwt', 'my-seed')

        assert 'my-jwt' in result
        assert 'my-seed' in result

    def test_has_nats_headers(self):
        result = build_credentials('jwt', 'seed')

        assert '-----BEGIN NATS USER JWT-----' in result
        assert '------END NATS USER JWT------' in result
        assert '-----BEGIN USER NKEY SEED-----' in result
        assert '------END USER NKEY SEED------' in result


# ──────────────────────────────────────────────────────────────
# topic_pattern_matcher
# ──────────────────────────────────────────────────────────────

class TestTopicPatternMatcher:

    def test_exact_match(self):
        assert topic_pattern_matcher('foo.bar.baz', 'foo.bar.baz') is True

    def test_exact_no_match(self):
        assert topic_pattern_matcher('foo.bar.baz', 'foo.bar.qux') is False

    def test_single_wildcard_star(self):
        assert topic_pattern_matcher('foo.*.baz', 'foo.bar.baz') is True

    def test_single_wildcard_no_match_extra_token(self):
        assert topic_pattern_matcher('foo.*', 'foo.bar.baz') is False

    def test_star_in_second_pattern(self):
        assert topic_pattern_matcher('foo.bar.baz', 'foo.*.baz') is True

    def test_gt_wildcard_matches_rest(self):
        assert topic_pattern_matcher('foo.>', 'foo.bar.baz') is True

    def test_gt_wildcard_single_token(self):
        assert topic_pattern_matcher('foo.>', 'foo.bar') is True

    def test_gt_in_second_pattern(self):
        assert topic_pattern_matcher('foo.bar.baz', 'foo.>') is True

    def test_gt_must_be_last(self):
        assert topic_pattern_matcher('foo.>.bar', 'foo.bar.baz') is False

    def test_both_stars(self):
        assert topic_pattern_matcher('foo.*', 'foo.*') is True

    def test_different_lengths_no_match(self):
        assert topic_pattern_matcher('foo.bar', 'foo.bar.baz') is False

    def test_empty_vs_token(self):
        assert topic_pattern_matcher('foo', 'foo.bar') is False

    def test_star_matches_any_single_token(self):
        assert topic_pattern_matcher('*.bar', 'foo.bar') is True
        assert topic_pattern_matcher('*.bar', 'anything.bar') is True


# ──────────────────────────────────────────────────────────────
# decode_stored_value
# ──────────────────────────────────────────────────────────────

class TestDecodeStoredValue:

    def test_passes_through_non_strings(self):
        assert decode_stored_value(42) == 42
        assert decode_stored_value(3.14) == 3.14
        assert decode_stored_value(True) is True
        assert decode_stored_value(None) is None

    def test_passes_through_plain_strings(self):
        assert decode_stored_value('hello') == 'hello'

    def test_decodes_json_object_string(self):
        assert decode_stored_value('{"a": 1}') == {'a': 1}

    def test_decodes_json_array_string(self):
        assert decode_stored_value('[1, 2, 3]') == [1, 2, 3]

    def test_returns_string_on_decode_error(self):
        # malformed JSON falls back to original string
        assert decode_stored_value('{not-json') == '{not-json'

    def test_empty_string_passes_through(self):
        assert decode_stored_value('') == ''


# ──────────────────────────────────────────────────────────────
# stream_history
# ──────────────────────────────────────────────────────────────

import msgpack


class _FakeMsg:
    def __init__(self, data, subject='s'):
        self.data = data
        self.subject = subject


class _FakeSub:
    def __init__(self, msgs):
        self._msgs = list(msgs)
        self.unsubscribed = False

    async def next_msg(self, timeout=None):
        if not self._msgs:
            raise asyncio.TimeoutError()
        return self._msgs.pop(0)

    async def unsubscribe(self):
        self.unsubscribed = True


import asyncio


class TestStreamHistory:

    @pytest.mark.asyncio
    async def test_no_stream_status(self, ctx):
        ctx.nats_client.subscribe = AsyncMock(return_value=_FakeSub([]))
        ctx.nats_client.request = AsyncMock(return_value=_FakeMsg(
            msgpack.packb({'status': 'TELEMETRY_FETCH_SUCCESS_NO_STREAM', 'data': {'foo': 1}})
        ))
        ctx.nats_client.publish = AsyncMock()

        result = await stream_history(ctx, 'api.test', {'q': 1})

        assert result['status'] == 'TELEMETRY_FETCH_SUCCESS_NO_STREAM'
        assert result['frames'] == []
        assert result['error'] is False

    @pytest.mark.asyncio
    async def test_streams_frames_until_last(self, ctx):
        frame1 = msgpack.packb({'last': False, 'data': {'temp': {'value': 1, 'timestamp': 1}}})
        frame2 = msgpack.packb({'last': True, 'data': {'temp': {'value': 2, 'timestamp': 2}}})

        ctx.nats_client.subscribe = AsyncMock(return_value=_FakeSub([
            _FakeMsg(frame1), _FakeMsg(frame2),
        ]))
        ctx.nats_client.request = AsyncMock(return_value=_FakeMsg(
            msgpack.packb({
                'status': 'TELEMETRY_FETCH_STREAM_STARTED',
                'data': {'ready_subject': 'ready.subj'},
            })
        ))
        ctx.nats_client.publish = AsyncMock()

        result = await stream_history(ctx, 'api.test', {'q': 1})

        assert result['error'] is False
        assert len(result['frames']) == 2
        assert result['frames'][1]['last'] is True
        ctx.nats_client.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_failure_status(self, ctx):
        ctx.nats_client.subscribe = AsyncMock(return_value=_FakeSub([]))
        ctx.nats_client.request = AsyncMock(return_value=_FakeMsg(
            msgpack.packb({'status': 'TELEMETRY_FETCH_FAILURE'})
        ))

        result = await stream_history(ctx, 'api.test', {'q': 1})

        assert result['error'] is True
