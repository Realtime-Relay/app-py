"""Tests for the utils module — build_credentials, topic_pattern_matcher,
decode_stored_value, http_history."""

import pytest
from unittest.mock import AsyncMock, MagicMock

import relayx_app_sdk.utils as utils_module
from relayx_app_sdk.utils import (
    build_credentials, topic_pattern_matcher, decode_stored_value, http_history,
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
# http_history
# ──────────────────────────────────────────────────────────────

class TestHttpHistory:

    def _auth(self, ctx):
        # Pre-seed the cached influx token/url so ensure_influx_auth
        # short-circuits without a NATS round trip.
        ctx._influx_token = 'tok'
        ctx._influx_url = 'http://influx'

    @pytest.mark.asyncio
    async def test_collects_single_page(self, ctx, monkeypatch):
        self._auth(ctx)

        def fake_post(url, token, body):
            assert url == 'http://influx/iot/db/telemetry/history'
            assert token == 'tok'
            return 200, {'status': True, 'data': {
                'frames': [{'temp': {'value': 1, 'timestamp': 1}}],
                'page': {'has_more': False},
            }}

        monkeypatch.setattr(utils_module, '_influx_post', fake_post)

        result = await http_history(ctx, '/iot/db/telemetry/history', {'q': 1})

        assert result['error'] is False
        assert result['frames'] == [{'temp': {'value': 1, 'timestamp': 1}}]

    @pytest.mark.asyncio
    async def test_paginates_until_no_more(self, ctx, monkeypatch):
        self._auth(ctx)
        pages = [
            (200, {'status': True, 'data': {
                'frames': [{'temp': {'value': 1, 'timestamp': 1}}],
                'page': {'has_more': True, 'next_offset': 1},
            }}),
            (200, {'status': True, 'data': {
                'frames': [{'temp': {'value': 2, 'timestamp': 2}}],
                'page': {'has_more': False},
            }}),
        ]
        calls = []

        def fake_post(url, token, body):
            calls.append(body['offset'])
            return pages.pop(0)

        monkeypatch.setattr(utils_module, '_influx_post', fake_post)

        result = await http_history(ctx, '/iot/db/telemetry/history', {})

        assert result['error'] is False
        assert len(result['frames']) == 2
        assert calls == [0, 1]  # second page requested at next_offset

    @pytest.mark.asyncio
    async def test_failure_envelope(self, ctx, monkeypatch):
        self._auth(ctx)

        def fake_post(url, token, body):
            return 400, {'status': False, 'data': {
                'code': 'TELEMETRY_FETCH_FAILURE', 'errors': ['bad start'],
            }}

        monkeypatch.setattr(utils_module, '_influx_post', fake_post)

        result = await http_history(ctx, '/iot/db/telemetry/history', {})

        assert result['error'] is True
        assert result['status'] == 'TELEMETRY_FETCH_FAILURE'
        assert result['frames'] == []

    @pytest.mark.asyncio
    async def test_refetches_token_on_401(self, ctx, monkeypatch):
        self._auth(ctx)
        # token request the forced refetch will make
        ctx.nats_client.request = AsyncMock(return_value=MagicMock(
            data=b'{"status":"HTTP_TOKEN_SUCCESS","data":{"token":"tok2","http_url":"http://influx"}}'
        ))
        responses = [(401, None), (200, {'status': True, 'data': {
            'frames': [{'temp': {'value': 9, 'timestamp': 9}}],
            'page': {'has_more': False},
        }})]

        def fake_post(url, token, body):
            return responses.pop(0)

        monkeypatch.setattr(utils_module, '_influx_post', fake_post)

        result = await http_history(ctx, '/iot/db/telemetry/history', {})

        assert result['error'] is False
        assert result['frames'] == [{'temp': {'value': 9, 'timestamp': 9}}]
