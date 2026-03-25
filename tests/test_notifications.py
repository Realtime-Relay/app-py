"""Tests for the NotificationManager."""

import json
import pytest
from unittest.mock import AsyncMock

from relayx_app_sdk.notifications import NotificationManager
from tests.conftest import make_nats_response


# ──────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────

@pytest.fixture
def notifications(ctx):
    return NotificationManager(ctx)


# ──────────────────────────────────────────────────────────────
# create
# ──────────────────────────────────────────────────────────────

class TestNotificationCreate:

    @pytest.mark.asyncio
    async def test_create_webhook(self, notifications, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'ok'})
        )

        result = await notifications.create({
            'name': 'my-webhook',
            'type': 'WEBHOOK',
            'config': {'endpoint': 'https://example.com/hook'},
        })

        assert result is not None

    @pytest.mark.asyncio
    async def test_create_email(self, notifications, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'ok'})
        )

        result = await notifications.create({
            'name': 'my-email',
            'type': 'EMAIL',
            'config': {
                'recipients': ['a@b.com'],
                'subject': 'Alert!',
                'template': 'Hello {{name}}',
            },
        })

        assert result is not None

    @pytest.mark.asyncio
    async def test_invalid_type_raises(self, notifications):
        with pytest.raises(ValueError, match='type must be'):
            await notifications.create({
                'name': 'x',
                'type': 'SMS',
                'config': {},
            })

    @pytest.mark.asyncio
    async def test_webhook_missing_endpoint_raises(self, notifications):
        with pytest.raises(ValueError, match='endpoint is required'):
            await notifications.create({
                'name': 'x',
                'type': 'WEBHOOK',
                'config': {},
            })

    @pytest.mark.asyncio
    async def test_email_missing_recipients_raises(self, notifications):
        with pytest.raises(ValueError, match='recipients is required'):
            await notifications.create({
                'name': 'x',
                'type': 'EMAIL',
                'config': {
                    'subject': 's',
                    'template': 't',
                },
            })

    @pytest.mark.asyncio
    async def test_email_missing_subject_raises(self, notifications):
        with pytest.raises(ValueError, match='subject is required'):
            await notifications.create({
                'name': 'x',
                'type': 'EMAIL',
                'config': {
                    'recipients': ['a@b.com'],
                    'template': 't',
                },
            })

    @pytest.mark.asyncio
    async def test_email_missing_template_raises(self, notifications):
        with pytest.raises(ValueError, match='template is required'):
            await notifications.create({
                'name': 'x',
                'type': 'EMAIL',
                'config': {
                    'recipients': ['a@b.com'],
                    'subject': 's',
                },
            })

    @pytest.mark.asyncio
    async def test_not_connected_raises(self, notifications, ctx):
        ctx.connected = False

        with pytest.raises(RuntimeError, match='Not connected'):
            await notifications.create({
                'name': 'x',
                'type': 'WEBHOOK',
                'config': {'endpoint': 'https://example.com'},
            })


# ──────────────────────────────────────────────────────────────
# update
# ──────────────────────────────────────────────────────────────

class TestNotificationUpdate:

    @pytest.mark.asyncio
    async def test_update(self, notifications, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'ok'})
        )

        result = await notifications.update({
            'name': 'my-webhook',
            'type': 'WEBHOOK',
            'config': {'endpoint': 'https://new.example.com'},
        })

        assert result is not None

    @pytest.mark.asyncio
    async def test_invalid_type_raises(self, notifications):
        with pytest.raises(ValueError, match='type must be'):
            await notifications.update({
                'name': 'x',
                'type': 'INVALID',
                'config': {},
            })


# ──────────────────────────────────────────────────────────────
# delete
# ──────────────────────────────────────────────────────────────

class TestNotificationDelete:

    @pytest.mark.asyncio
    async def test_successful_delete(self, notifications, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({'status': 'NOTIFICATION_DELETE_SUCCESS'})
        )

        result = await notifications.delete('notif-1')
        assert result is True

    @pytest.mark.asyncio
    async def test_missing_id_raises(self, notifications):
        with pytest.raises(ValueError, match='id is required'):
            await notifications.delete('')


# ──────────────────────────────────────────────────────────────
# list / get
# ──────────────────────────────────────────────────────────────

class TestNotificationListGet:

    @pytest.mark.asyncio
    async def test_list(self, notifications, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'data': [{'id': 'n1', 'name': 'hook'}],
            })
        )

        result = await notifications.list()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get(self, notifications, ctx):
        ctx.nats_client.request = AsyncMock(
            return_value=make_nats_response({
                'id': 'n1',
                'name': 'hook',
                'type': 'WEBHOOK',
            })
        )

        result = await notifications.get('n1')
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_missing_id_raises(self, notifications):
        with pytest.raises(ValueError, match='id is required'):
            await notifications.get('')
