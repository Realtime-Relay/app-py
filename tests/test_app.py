"""Tests for the RelayApp main class."""

import base64
import json
import pytest

from relayx_app_sdk.app import RelayApp


# ──────────────────────────────────────────────────────────────
# Helper — build a fake JWT with embedded org_id
# ──────────────────────────────────────────────────────────────

def _make_jwt(org_id='org-abc-123'):
    """Create a fake NATS JWT with the given org_id embedded."""

    header = base64.urlsafe_b64encode(json.dumps({'alg': 'none'}).encode()).rstrip(b'=').decode()

    payload_data = {
        'nats': {
            'org_data': {
                'org_id': org_id,
            },
        },
    }
    payload = base64.urlsafe_b64encode(json.dumps(payload_data).encode()).rstrip(b'=').decode()

    signature = base64.urlsafe_b64encode(b'fake-sig').rstrip(b'=').decode()

    return f'{header}.{payload}.{signature}'


# ──────────────────────────────────────────────────────────────
# Constructor validation
# ──────────────────────────────────────────────────────────────

class TestRelayAppInit:

    def test_valid_config(self):
        app = RelayApp({
            'api_key': _make_jwt(),
            'secret': 'test-secret',
            'mode': 'test',
        })

        assert app._ctx.org_id == 'org-abc-123'
        assert app._ctx.env == 'test'
        assert app._ctx.secret == 'test-secret'

    def test_production_mode(self):
        app = RelayApp({
            'api_key': _make_jwt(),
            'secret': 's',
            'mode': 'production',
        })

        assert app._ctx.env == 'production'

    def test_none_config_raises(self):
        with pytest.raises(ValueError, match='config must be a dict'):
            RelayApp(None)

    def test_missing_api_key_raises(self):
        with pytest.raises(ValueError, match='api_key is required'):
            RelayApp({'secret': 's', 'mode': 'test'})

    def test_missing_secret_raises(self):
        with pytest.raises(ValueError, match='secret is required'):
            RelayApp({'api_key': _make_jwt(), 'mode': 'test'})

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError, match='mode must be'):
            RelayApp({'api_key': _make_jwt(), 'secret': 's', 'mode': 'staging'})

    def test_invalid_jwt_raises(self):
        with pytest.raises(ValueError, match='Invalid api_key'):
            RelayApp({'api_key': 'not.a.jwt', 'secret': 's', 'mode': 'test'})

    def test_jwt_without_org_id_raises(self):
        # JWT with empty nats payload
        header = base64.urlsafe_b64encode(b'{}').rstrip(b'=').decode()
        payload = base64.urlsafe_b64encode(json.dumps({'nats': {}}).encode()).rstrip(b'=').decode()
        sig = base64.urlsafe_b64encode(b'x').rstrip(b'=').decode()
        bad_jwt = f'{header}.{payload}.{sig}'

        with pytest.raises(ValueError, match='Could not extract org_id'):
            RelayApp({'api_key': bad_jwt, 'secret': 's', 'mode': 'test'})


# ──────────────────────────────────────────────────────────────
# Manager wiring
# ──────────────────────────────────────────────────────────────

class TestRelayAppManagers:

    def test_all_managers_attached(self):
        app = RelayApp({
            'api_key': _make_jwt(),
            'secret': 's',
            'mode': 'test',
        })

        assert app.device is not None
        assert app.connection is not None
        assert app.telemetry is not None
        assert app.command is not None
        assert app.rpc is not None
        assert app.events is not None
        assert app.alert is not None
        assert app.logical_group is not None
        assert app.heirarchy_group is not None
        assert app.notification is not None

    def test_device_attached_to_context(self):
        app = RelayApp({
            'api_key': _make_jwt(),
            'secret': 's',
            'mode': 'test',
        })

        assert app._ctx.device is app.device
