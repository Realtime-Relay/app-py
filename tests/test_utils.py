"""Tests for the utils module — build_credentials and topic_pattern_matcher."""

from relayx_app_sdk.utils import build_credentials, topic_pattern_matcher


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
