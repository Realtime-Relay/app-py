"""Tests for the ephemeral alerting subsystem — shared, engine, owner, listener."""

import time
import pytest
from unittest.mock import AsyncMock, MagicMock

from relayx_app_sdk.ephemeral_alerting.shared import (
    SUBJECT_MAP,
    SUBJECT_DEVICE_INDEX,
    SUBJECT_LAST_TOKEN_INDEX,
    build_data_subject,
    resolve_ident_from_id,
    build_alert_payload,
    is_muted,
    create_fresh_state,
    dispatch_notifications,
    publish_event,
)
from relayx_app_sdk.ephemeral_alerting.engine import EphemeralEngine
from relayx_app_sdk.ephemeral_alerting.owner import EphemeralOwner
from relayx_app_sdk.ephemeral_alerting.listener import EphemeralListener


# ══════════════════════════════════════════════════════════════
# shared.py
# ══════════════════════════════════════════════════════════════


class TestSubjectMap:

    def test_telemetry_subject(self):
        subject = SUBJECT_MAP['TELEMETRY']('org1', 'test', 'dev1', 'temp')
        assert subject == 'org1.test.telemetry.dev1.temp'

    def test_command_subject(self):
        subject = SUBJECT_MAP['COMMAND']('org1', 'test', 'dev1', 'reboot')
        assert subject == 'org1.test.command.queue.dev1.reboot'

    def test_event_subject(self):
        subject = SUBJECT_MAP['EVENT']('org1', 'test', 'dev1', 'door')
        assert subject == 'org1.test.events.dev1.door'

    def test_device_indices(self):
        assert SUBJECT_DEVICE_INDEX['TELEMETRY'] == 3
        assert SUBJECT_DEVICE_INDEX['COMMAND'] == 4
        assert SUBJECT_DEVICE_INDEX['EVENT'] == 3

    def test_last_token_indices(self):
        assert SUBJECT_LAST_TOKEN_INDEX['TELEMETRY'] == 4
        assert SUBJECT_LAST_TOKEN_INDEX['COMMAND'] == 5
        assert SUBJECT_LAST_TOKEN_INDEX['EVENT'] == 4


class TestBuildDataSubject:

    @pytest.mark.asyncio
    async def test_builds_telemetry_wildcard(self, ctx):
        rule = {'config': {'topic': {'source': 'TELEMETRY', 'device_ident': '*', 'last_token': '*'}}}

        subject = await build_data_subject(ctx, rule)

        assert subject == 'org123.test.telemetry.*.*'

    @pytest.mark.asyncio
    async def test_builds_telemetry_resolved(self, ctx):
        rule = {'config': {'topic': {'source': 'TELEMETRY', 'device_ident': 'sensor-1', 'last_token': 'temp'}}}

        subject = await build_data_subject(ctx, rule)

        assert subject == 'org123.test.telemetry.dev-id-1.temp'

    @pytest.mark.asyncio
    async def test_unknown_source_raises(self, ctx):
        rule = {'config': {'topic': {'source': 'UNKNOWN', 'device_ident': '*', 'last_token': '*'}}}

        with pytest.raises(ValueError, match='Unknown source type'):
            await build_data_subject(ctx, rule)


class TestResolveIdentFromId:

    def test_finds_ident(self, ctx):
        result = resolve_ident_from_id(ctx, 'dev-id-1')
        assert result == 'sensor-1'

    def test_returns_none_when_not_found(self, ctx):
        result = resolve_ident_from_id(ctx, 'unknown-id')
        assert result is None


class TestBuildAlertPayload:

    def test_builds_payload(self):
        rule = {
            'id': 'r1',
            'name': 'high-temp',
            'config': {
                'topic': {
                    'source': 'TELEMETRY',
                    'device_ident': 'sensor-1',
                    'last_token': 'temp',
                },
                'duration': 0,
                'recovery_duration': 0,
                'cooldown': 0,
            },
        }

        rolling_state = {
            'sensor-1': {
                'temp': {'value': 105, 'timestamp': 1234567890},
            },
        }

        payload = build_alert_payload(rule, rolling_state, 1234567890, 'dev-1')

        assert payload['alert']['id'] == 'r1'
        assert payload['alert']['name'] == 'high-temp'
        assert payload['alert']['type'] == 'TELEMETRY'
        assert payload['alert']['config'] == rule['config']
        assert payload['device_id'] == 'dev-1'
        assert payload['rolling_state']['sensor-1']['temp']['value'] == 105
        assert payload['timestamp'] == 1234567890


class TestIsMuted:

    def test_not_muted_when_no_config(self):
        assert is_muted({}) is False

    def test_not_muted_when_none(self):
        assert is_muted({'alert_mute_config': None}) is False

    def test_muted_forever(self):
        assert is_muted({'alert_mute_config': {'type': 'FOREVER'}}) is True

    def test_muted_time_based_future(self):
        future_ms = int(time.time() * 1000) + 60000

        assert is_muted({
            'alert_mute_config': {'type': 'TIME_BASED', 'mute_till': future_ms},
        }) is True

    def test_not_muted_time_based_past(self):
        past_ms = int(time.time() * 1000) - 60000

        assert is_muted({
            'alert_mute_config': {'type': 'TIME_BASED', 'mute_till': past_ms},
        }) is False


class TestCreateFreshState:

    def test_has_expected_keys(self):
        state = create_fresh_state()

        assert state['status'] == 'normal'
        assert state['last_evaluated_at'] is None
        assert state['clear_since'] is None
        assert state['breached_since'] is None
        assert state['last_fired'] == 0
        assert state['acked_by'] is None
        assert state['acked_at'] is None
        assert state['ack_notes'] is None


class TestDispatchNotifications:

    @pytest.mark.asyncio
    async def test_sends_request(self, ctx):
        ctx.nats_client.request = AsyncMock()

        rule = {'id': 'r1', 'notification_channel': ['ch1']}

        await dispatch_notifications(ctx, rule, {'event': 'fire'})

        ctx.nats_client.request.assert_awaited_once()

        subject = ctx.nats_client.request.call_args[0][0]
        assert 'notification' in subject
        assert 'dispatch' in subject

    @pytest.mark.asyncio
    async def test_skips_when_no_channels(self, ctx):
        ctx.nats_client.request = AsyncMock()

        await dispatch_notifications(ctx, {'id': 'r1'}, {})

        ctx.nats_client.request.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_swallows_errors(self, ctx):
        ctx.nats_client.request = AsyncMock(side_effect=Exception('timeout'))

        rule = {'id': 'r1', 'notification_channel': ['ch1']}

        # Should not raise
        await dispatch_notifications(ctx, rule, {})


class TestPublishEvent:

    @pytest.mark.asyncio
    async def test_publishes_to_correct_subject(self, ctx):
        ctx.jetstream.publish = AsyncMock(return_value='ack')

        await publish_event(ctx, {'id': 'r1'}, 'fire', {'test': True})

        ctx.jetstream.publish.assert_awaited_once()
        subject = ctx.jetstream.publish.call_args[0][0]
        assert 'alerts.listen.r1.fire' in subject


# ══════════════════════════════════════════════════════════════
# engine.py
# ══════════════════════════════════════════════════════════════


class TestEphemeralEngine:

    def test_initial_state(self, ctx):
        engine = EphemeralEngine(ctx, {'id': 'r1'})

        assert engine.mode is None
        assert engine.rolling_state == {}

    def test_set_evaluator(self, ctx):
        engine = EphemeralEngine(ctx, {'id': 'r1'})

        engine.set_evaluator(lambda state: True)

        assert engine._evaluator is not None

    def test_set_evaluator_non_callable_raises(self, ctx):
        engine = EphemeralEngine(ctx, {'id': 'r1'})

        with pytest.raises(ValueError, match='callable'):
            engine.set_evaluator('not-a-fn')

    @pytest.mark.asyncio
    async def test_listen_creates_owner_with_evaluator(self, ctx):
        engine = EphemeralEngine(ctx, {'id': 'r1', 'config': {'topic': {'source': 'TELEMETRY', 'device_ident': '*', 'last_token': '*'}}})
        engine.set_evaluator(lambda state: True)

        await engine.listen({'on_fire': lambda x: x})

        assert engine.mode == 'owner'

    @pytest.mark.asyncio
    async def test_listen_creates_listener_without_evaluator(self, ctx):
        engine = EphemeralEngine(ctx, {'id': 'r1'})

        await engine.listen({'on_fire': lambda x: x})

        assert engine.mode == 'listener'

    @pytest.mark.asyncio
    async def test_stop_clears_delegate(self, ctx):
        engine = EphemeralEngine(ctx, {'id': 'r1'})
        await engine.listen()

        await engine.stop()

        assert engine._delegate is None
        assert engine.mode is None

    @pytest.mark.asyncio
    async def test_ack_without_owner_raises(self, ctx):
        engine = EphemeralEngine(ctx, {'id': 'r1'})
        await engine.listen()  # listener mode

        with pytest.raises(RuntimeError, match='only available in owner mode'):
            await engine.ack('user')

    @pytest.mark.asyncio
    async def test_double_listen_is_noop(self, ctx):
        engine = EphemeralEngine(ctx, {'id': 'r1'})

        await engine.listen()
        delegate1 = engine._delegate

        await engine.listen()  # second call should be ignored

        assert engine._delegate is delegate1


# ══════════════════════════════════════════════════════════════
# owner.py — state machine (single global state, matches JS)
# ══════════════════════════════════════════════════════════════


class TestEphemeralOwnerStateMachine:

    def _make_owner(self, ctx, evaluator=None, callbacks=None):
        rule = {
            'id': 'r1',
            'name': 'test-rule',
            'config': {
                'topic': {
                    'source': 'TELEMETRY',
                    'device_ident': 'sensor-1',
                    'last_token': 'temp',
                },
                'duration': 0,
                'recovery_duration': 0,
                'cooldown': 0,
            },
        }

        return EphemeralOwner(
            ctx,
            rule,
            evaluator or (lambda state: state.get('sensor-1', {}).get('temp', {}).get('value', 0) > 100),
            callbacks or {},
        )

    def test_initial_state(self, ctx):
        owner = self._make_owner(ctx)

        state = owner.state
        assert state['status'] == 'normal'
        assert owner.rolling_state == {}

    @pytest.mark.asyncio
    async def test_evaluate_normal_to_alerting(self, ctx):
        ctx.jetstream.publish = AsyncMock(return_value='ack')
        ctx.nats_client.request = AsyncMock()
        owner = self._make_owner(ctx)

        subject = 'org123.test.telemetry.dev-id-1.temp'

        # First message: updates rolling state + evaluate
        owner._update_rolling_state(subject, {'value': 105})
        await owner._evaluate(subject, {'value': 105})

        # With duration=0, should immediately fire (normal -> alerting)
        assert owner._state['status'] == 'alerting'

    @pytest.mark.asyncio
    async def test_evaluate_refire_on_cooldown(self, ctx):
        fired = []
        ctx.jetstream.publish = AsyncMock(return_value='ack')
        ctx.nats_client.request = AsyncMock()

        owner = self._make_owner(ctx, callbacks={'on_fire': lambda p: fired.append(p)})

        subject = 'org123.test.telemetry.dev-id-1.temp'

        # First: fire
        owner._update_rolling_state(subject, {'value': 105})
        await owner._evaluate(subject, {'value': 105})

        assert owner._state['status'] == 'alerting'
        assert len(fired) == 1

        # Second: re-fire (cooldown=0 means immediate)
        owner._update_rolling_state(subject, {'value': 110})
        await owner._evaluate(subject, {'value': 110})

        assert len(fired) == 2

    @pytest.mark.asyncio
    async def test_evaluate_recovery(self, ctx):
        resolved = []
        ctx.jetstream.publish = AsyncMock(return_value='ack')
        ctx.nats_client.request = AsyncMock()

        owner = self._make_owner(ctx, callbacks={'on_resolved': lambda p: resolved.append(p)})

        subject = 'org123.test.telemetry.dev-id-1.temp'

        # Trigger: fire
        owner._update_rolling_state(subject, {'value': 105})
        await owner._evaluate(subject, {'value': 105})

        assert owner._state['status'] == 'alerting'

        # Recovery: value drops below threshold (recovery_duration=0)
        owner._update_rolling_state(subject, {'value': 50})
        await owner._evaluate(subject, {'value': 50})

        assert owner._state['status'] == 'normal'
        assert len(resolved) == 1

    @pytest.mark.asyncio
    async def test_muted_rule_skips_evaluation(self, ctx):
        fired = []

        owner = self._make_owner(ctx, callbacks={'on_fire': lambda p: fired.append(p)})
        owner._rule['alert_mute_config'] = {'type': 'FOREVER'}

        subject = 'org123.test.telemetry.dev-id-1.temp'

        owner._update_rolling_state(subject, {'value': 999})
        await owner._evaluate(subject, {'value': 999})

        assert len(fired) == 0

    @pytest.mark.asyncio
    async def test_evaluator_non_boolean_calls_on_error(self, ctx):
        errors = []

        owner = self._make_owner(
            ctx,
            evaluator=lambda state: "not_a_bool",
            callbacks={'on_error': lambda e: errors.append(e)},
        )

        subject = 'org123.test.telemetry.dev-id-1.temp'

        owner._update_rolling_state(subject, {'value': 105})
        await owner._evaluate(subject, {'value': 105})

        assert len(errors) == 1
        assert isinstance(errors[0], TypeError)


# ══════════════════════════════════════════════════════════════
# owner.py — ack (checks alerting state, matches JS)
# ══════════════════════════════════════════════════════════════


class TestEphemeralOwnerAck:

    @pytest.mark.asyncio
    async def test_ack_transitions_state(self, ctx):
        acked = []
        ctx.jetstream.publish = AsyncMock(return_value='ack')
        ctx.nats_client.request = AsyncMock()

        rule = {
            'id': 'r1',
            'name': 'test',
            'config': {
                'topic': {
                    'source': 'TELEMETRY',
                    'device_ident': 'sensor-1',
                    'last_token': 'temp',
                },
                'duration': 0,
                'recovery_duration': 0,
                'cooldown': 0,
            },
        }

        owner = EphemeralOwner(
            ctx, rule,
            lambda state: state.get('sensor-1', {}).get('temp', {}).get('value', 0) > 100,
            {'on_ack': lambda p: acked.append(p)},
        )

        subject = 'org123.test.telemetry.dev-id-1.temp'

        # Trigger to alerting (fire)
        owner._update_rolling_state(subject, {'value': 105})
        await owner._evaluate(subject, {'value': 105})

        assert owner._state['status'] == 'alerting'

        # Now ack
        result = await owner.ack('admin@test.com', 'looking into it')

        assert result is True
        assert owner._state['status'] == 'acknowledged'
        assert owner._state['acked_by'] == 'admin@test.com'
        assert len(acked) == 1

    @pytest.mark.asyncio
    async def test_ack_when_not_alerting_returns_false(self, ctx):
        rule = {
            'id': 'r1',
            'name': 'test',
            'config': {
                'topic': {'source': 'TELEMETRY', 'device_ident': '*', 'last_token': '*'},
                'duration': 0, 'recovery_duration': 0, 'cooldown': 0,
            },
        }

        owner = EphemeralOwner(ctx, rule, lambda s: True, {})

        result = await owner.ack('admin')

        assert result is False


# ══════════════════════════════════════════════════════════════
# listener.py
# ══════════════════════════════════════════════════════════════


class TestEphemeralListener:

    def test_initial_state(self, ctx):
        listener = EphemeralListener(ctx, {'id': 'r1'}, {})

        assert listener.state['status'] == 'normal'
        assert listener.rolling_state == {}

    @pytest.mark.asyncio
    async def test_start_subscribes(self, ctx):
        listener = EphemeralListener(ctx, {'id': 'r1'}, {})

        await listener.start()

        ctx.jetstream.subscribe.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_unsubscribes(self, ctx):
        listener = EphemeralListener(ctx, {'id': 'r1'}, {})

        await listener.start()
        await listener.stop()

        assert listener._consumer is None

    @pytest.mark.asyncio
    async def test_double_start_is_noop(self, ctx):
        listener = EphemeralListener(ctx, {'id': 'r1'}, {})

        await listener.start()
        await listener.start()  # second call

        # subscribe should only be called once
        assert ctx.jetstream.subscribe.await_count == 1

    @pytest.mark.asyncio
    async def test_dispatch_event_calls_callback(self, ctx):
        fired = []

        listener = EphemeralListener(
            ctx,
            {'id': 'r1'},
            {'on_fire': lambda d: fired.append(d)},
        )

        await listener._dispatch_event(
            'org123.test.alerts.listen.r1.fire',
            {'device_id': 'dev-1', 'timestamp': 1700000000000},
        )

        assert len(fired) == 1
        # timestamp should be converted to ISO string
        assert 'Z' in fired[0]['timestamp']

    @pytest.mark.asyncio
    async def test_dispatch_event_ignores_unknown_type(self, ctx):
        fired = []

        listener = EphemeralListener(
            ctx,
            {'id': 'r1'},
            {'on_fire': lambda d: fired.append(d)},
        )

        await listener._dispatch_event(
            'org123.test.alerts.listen.r1.unknown_event',
            {'data': 'x'},
        )

        assert len(fired) == 0
