"""Tests for the AlertManager."""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock

from relayx_app_sdk.alerts import AlertManager
from tests.conftest import make_nats_response


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def alerts(ctx):
    return AlertManager(ctx)


# ──────────────────────────────────────────────────────────────
# create (non-ephemeral)
# ──────────────────────────────────────────────────────────────

class TestAlertCreate:

    @pytest.mark.asyncio
    async def test_creates_threshold_alert(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': {
                    'id': 'alert-1',
                    'name': 'high-temp',
                    'type': 'THRESHOLD',
                },
            })
        )

        result = await alerts.create({
            'name': 'high-temp',
            'type': 'THRESHOLD',
            'config': {'threshold': 100},
        })

        assert result['id'] == 'alert-1'
        assert hasattr(result, 'listen')
        assert 'alert-1' in alerts._alert_metadata

    @pytest.mark.asyncio
    async def test_ephemeral_type_raises(self, alerts):
        with pytest.raises(ValueError, match='Use create_ephemeral'):
            await alerts.create({
                'name': 'x',
                'type': 'EPHEMERAL',
            })

    @pytest.mark.asyncio
    async def test_invalid_type_raises(self, alerts):
        with pytest.raises(ValueError, match='type must be'):
            await alerts.create({
                'name': 'x',
                'type': 'INVALID',
            })

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, alerts, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await alerts.create({
                'name': 'x',
                'type': 'THRESHOLD',
            })

    @pytest.mark.asyncio
    async def test_returns_none_when_no_data(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'FAILED'})
        )

        result = await alerts.create({
            'name': 'x',
            'type': 'THRESHOLD',
        })

        assert result is None


# ──────────────────────────────────────────────────────────────
# create_ephemeral
# ──────────────────────────────────────────────────────────────

class TestAlertCreateEphemeral:

    @pytest.mark.asyncio
    async def test_creates_ephemeral_alert(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': {
                    'id': 'eph-1',
                    'name': 'temp-spike',
                    'type': 'EPHEMERAL',
                },
            })
        )

        result = await alerts.create_ephemeral({
            'name': 'temp-spike',
            'config': {
                'topic': {
                    'source': 'TELEMETRY',
                    'device_ident': 'sensor-1',
                    'last_token': 'temp',
                },
                'duration': 10,
                'recovery_duration': 5,
            },
        })

        assert result['id'] == 'eph-1'
        assert hasattr(result, 'set_evaluator')
        assert hasattr(result, 'listen')
        assert hasattr(result, 'stop')
        assert 'eph-1' in alerts._ephemeral_engines

    @pytest.mark.asyncio
    async def test_missing_config_raises(self, alerts):
        with pytest.raises(ValueError, match='config is required'):
            await alerts.create_ephemeral({'name': 'x'})

    @pytest.mark.asyncio
    async def test_missing_topic_raises(self, alerts):
        with pytest.raises(ValueError, match='config.topic is required'):
            await alerts.create_ephemeral({
                'name': 'x',
                'config': {'duration': 1, 'recovery_duration': 1},
            })

    @pytest.mark.asyncio
    async def test_invalid_source_raises(self, alerts):
        with pytest.raises(ValueError, match='source must be'):
            await alerts.create_ephemeral({
                'name': 'x',
                'config': {
                    'topic': {
                        'source': 'INVALID',
                        'device_ident': 'x',
                        'last_token': 'y',
                    },
                    'duration': 1,
                    'recovery_duration': 1,
                },
            })


# ──────────────────────────────────────────────────────────────
# update
# ──────────────────────────────────────────────────────────────

class TestAlertUpdate:

    @pytest.mark.asyncio
    async def test_update_returns_wrapped(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': {'id': 'alert-1', 'name': 'updated'},
            })
        )

        result = await alerts.update({'id': 'alert-1', 'name': 'updated'})

        assert result['name'] == 'updated'
        assert hasattr(result, 'listen')

    @pytest.mark.asyncio
    async def test_missing_id_raises(self, alerts):
        with pytest.raises(ValueError, match='id is required'):
            await alerts.update({'name': 'x'})


# ──────────────────────────────────────────────────────────────
# delete
# ──────────────────────────────────────────────────────────────

class TestAlertDelete:

    @pytest.mark.asyncio
    async def test_successful_delete(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'ALERT_DELETE_SUCCESS'})
        )

        result = await alerts.delete('alert-1')
        assert result is True

    @pytest.mark.asyncio
    async def test_missing_id_raises(self, alerts):
        with pytest.raises(ValueError, match='id is required'):
            await alerts.delete('')


# ──────────────────────────────────────────────────────────────
# list
# ──────────────────────────────────────────────────────────────

class TestAlertList:

    @pytest.mark.asyncio
    async def test_returns_alerts_and_tracks_metadata(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': [
                    {'id': 'a1', 'type': 'THRESHOLD'},
                    {'id': 'a2', 'type': 'EPHEMERAL'},
                ],
            })
        )

        result = await alerts.list()

        assert len(result) == 2
        assert alerts._alert_metadata['a1']['type'] == 'THRESHOLD'
        assert alerts._alert_metadata['a2']['type'] == 'EPHEMERAL'


# ──────────────────────────────────────────────────────────────
# get
# ──────────────────────────────────────────────────────────────

class TestAlertGet:

    @pytest.mark.asyncio
    async def test_get_threshold_alert(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': {'id': 'a1', 'name': 'my-alert', 'type': 'THRESHOLD'},
            })
        )

        result = await alerts.get('my-alert')

        assert result['id'] == 'a1'
        assert hasattr(result, 'listen')

    @pytest.mark.asyncio
    async def test_get_ephemeral_alert(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': {'id': 'e1', 'name': 'eph', 'type': 'EPHEMERAL'},
            })
        )

        result = await alerts.get('eph')

        assert result['id'] == 'e1'
        assert hasattr(result, 'set_evaluator')
        assert hasattr(result, 'stop')

    @pytest.mark.asyncio
    async def test_get_returns_none_when_not_found(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'NOT_FOUND'})
        )

        result = await alerts.get('nonexistent')
        assert result is None


# ──────────────────────────────────────────────────────────────
# history
# ──────────────────────────────────────────────────────────────

class TestAlertHistory:

    @pytest.mark.asyncio
    async def test_device_history(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'status': 'ALERT_FETCH_SUCCESS',
                'data': [{'event': 'fire'}],
            })
        )

        result = await alerts.history({
            'rule_type': 'DEVICE',
            'device_ident': 'sensor-1',
            'rule_states': ['fire'],
            'start': '2024-01-01T00:00:00Z',
            'end': '2024-01-02T00:00:00Z',
        })

        assert result == [{'event': 'fire'}]

    @pytest.mark.asyncio
    async def test_missing_rule_type_raises(self, alerts):
        with pytest.raises(ValueError, match='rule_type is required'):
            await alerts.history({
                'rule_states': ['fire'],
                'start': '2024-01-01T00:00:00Z',
                'end': '2024-01-02T00:00:00Z',
            })

    @pytest.mark.asyncio
    async def test_invalid_rule_type_raises(self, alerts):
        with pytest.raises(ValueError, match='rule_type must be'):
            await alerts.history({
                'rule_type': 'INVALID',
                'rule_states': ['fire'],
                'start': '2024-01-01T00:00:00Z',
                'end': '2024-01-02T00:00:00Z',
            })

    @pytest.mark.asyncio
    async def test_ack_state_requires_rule_id(self, alerts):
        with pytest.raises(ValueError, match='rule_id is required when'):
            await alerts.history({
                'rule_type': 'DEVICE',
                'device_ident': 'sensor-1',
                'rule_states': ['ack'],
                'start': '2024-01-01T00:00:00Z',
                'end': '2024-01-02T00:00:00Z',
            })

    @pytest.mark.asyncio
    async def test_invalid_rule_states_raises(self, alerts):
        with pytest.raises(ValueError, match='invalid values'):
            await alerts.history({
                'rule_type': 'DEVICE',
                'device_ident': 'sensor-1',
                'rule_states': ['garbage'],
                'start': '2024-01-01T00:00:00Z',
                'end': '2024-01-02T00:00:00Z',
            })


# ──────────────────────────────────────────────────────────────
# ack
# ──────────────────────────────────────────────────────────────

class TestAlertAck:

    @pytest.mark.asyncio
    async def test_non_ephemeral_ack(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'ALERT_ACK_SUCCESS'})
        )

        result = await alerts.ack({
            'device_id': 'dev-1',
            'alert_id': 'alert-1',
            'acked_by': 'user@test.com',
        })

        assert result is True

    @pytest.mark.asyncio
    async def test_missing_device_id_raises(self, alerts):
        with pytest.raises(ValueError, match='device_id is required'):
            await alerts.ack({
                'alert_id': 'a',
                'acked_by': 'u',
            })

    @pytest.mark.asyncio
    async def test_missing_alert_id_raises(self, alerts):
        with pytest.raises(ValueError, match='alert_id is required'):
            await alerts.ack({
                'device_id': 'd',
                'acked_by': 'u',
            })

    @pytest.mark.asyncio
    async def test_missing_acked_by_raises(self, alerts):
        with pytest.raises(ValueError, match='acked_by is required'):
            await alerts.ack({
                'device_id': 'd',
                'alert_id': 'a',
            })


# ──────────────────────────────────────────────────────────────
# ack_all
# ──────────────────────────────────────────────────────────────

class TestAlertAckAll:

    @pytest.mark.asyncio
    async def test_non_ephemeral_ack_all(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'ALERT_ACK_SUCCESS'})
        )

        result = await alerts.ack_all({
            'alert_id': 'alert-1',
            'acked_by': 'admin',
        })

        assert result is True

    @pytest.mark.asyncio
    async def test_missing_fields_raises(self, alerts):
        with pytest.raises(ValueError, match='alert_id is required'):
            await alerts.ack_all({'acked_by': 'admin'})

        with pytest.raises(ValueError, match='acked_by is required'):
            await alerts.ack_all({'alert_id': 'a'})


# ──────────────────────────────────────────────────────────────
# mute / unmute
# ──────────────────────────────────────────────────────────────

class TestAlertMute:

    @pytest.mark.asyncio
    async def test_mute_forever(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'ok'})
        )

        result = await alerts.mute({
            'id': 'alert-1',
            'mute_config': {'type': 'FOREVER'},
        })

        assert result is not None

    @pytest.mark.asyncio
    async def test_time_based_mute_requires_mute_till(self, alerts):
        with pytest.raises(ValueError, match='mute_till is required'):
            await alerts.mute({
                'id': 'alert-1',
                'mute_config': {'type': 'TIME_BASED'},
            })

    @pytest.mark.asyncio
    async def test_invalid_mute_type_raises(self, alerts):
        with pytest.raises(ValueError, match='mute_config.type must be'):
            await alerts.mute({
                'id': 'alert-1',
                'mute_config': {'type': 'INVALID'},
            })

    @pytest.mark.asyncio
    async def test_unmute(self, alerts, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'ok'})
        )

        result = await alerts.unmute('alert-1')
        assert result is not None

    @pytest.mark.asyncio
    async def test_unmute_missing_id_raises(self, alerts):
        with pytest.raises(ValueError, match='id is required'):
            await alerts.unmute('')


# ──────────────────────────────────────────────────────────────
# wrap_alert — set_evaluator should reject on non-ephemeral
# ──────────────────────────────────────────────────────────────

class TestWrapAlert:

    def test_non_ephemeral_set_evaluator_raises(self, alerts):
        wrapped = alerts._wrap_alert({
            'id': 'a1',
            'name': 'x',
            'type': 'THRESHOLD',
        })

        with pytest.raises(RuntimeError, match='only allowed for EPHEMERAL'):
            wrapped.set_evaluator(lambda: True)


# ──────────────────────────────────────────────────────────────
# delete_all_consumers
# ──────────────────────────────────────────────────────────────

class TestAlertCleanup:

    @pytest.mark.asyncio
    async def test_clears_everything(self, alerts):
        alerts._listen_consumers['r1'] = AsyncMock()
        alerts._listen_consumers['r1'].unsubscribe = AsyncMock()

        await alerts.delete_all_consumers()

        assert len(alerts._listen_consumers) == 0
        assert len(alerts._ephemeral_engines) == 0
