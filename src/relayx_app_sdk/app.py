import base64
import json

from .context import Context
from .connection import ConnectionManager
from .device import DeviceManager
from .telemetry import TelemetryManager
from .commands import CommandManager
from .rpc import RPCManager
from .events import EventManager
from .alerts import AlertManager
from .logical_group import LogicalGroupManager
from .heirarchy_group import HeirarchyGroupManager
from .notifications import NotificationManager


class RelayApp:

    def __init__(self, config):
        if config is None or not isinstance(config, dict):
            raise ValueError('config must be a dict')

        if not config.get('api_key'):
            raise ValueError('api_key is required')

        if not config.get('secret'):
            raise ValueError('secret is required')

        if config.get('mode') not in ('production', 'test'):
            raise ValueError('mode must be "production" or "test"')

        org_id = self._extract_org_id(config['api_key'])

        if not org_id:
            raise ValueError('Could not extract org_id from api_key')

        ctx = Context(
            api_key=config['api_key'],
            secret=config['secret'],
            org_id=org_id,
            env=config['mode'],
        )

        # Wire up all managers

        self.device = DeviceManager(ctx)
        ctx.device = self.device

        self.connection = ConnectionManager(ctx)
        self.telemetry = TelemetryManager(ctx)
        self.command = CommandManager(ctx)
        self.rpc = RPCManager(ctx)
        self.events = EventManager(ctx)
        self.alert = AlertManager(ctx)
        self.logical_group = LogicalGroupManager(ctx)
        self.heirarchy_group = HeirarchyGroupManager(ctx)
        self.notification = NotificationManager(ctx)

        self._ctx = ctx

    def _extract_org_id(self, api_key):
        try:
            parts = api_key.split('.')
            if len(parts) != 3:
                raise ValueError('Invalid JWT format')

            payload = parts[1]

            # Add padding if needed
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += '=' * padding

            decoded = base64.urlsafe_b64decode(payload)
            data = json.loads(decoded)

            return data.get('nats', {}).get('org_data', {}).get('org_id')

        except Exception:
            raise ValueError('Invalid api_key: could not decode JWT to extract org_id')

    async def connect(self):
        return await self.connection.connect()

    async def disconnect(self):
        return await self.connection.disconnect()
