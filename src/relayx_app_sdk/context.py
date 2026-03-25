import traceback


class Logger:
    """Simple logger that only prints when mode is 'test'."""

    def __init__(self, env):
        self._enabled = (env == 'test')

    def error(self, msg, exc=None):
        if not self._enabled:
            return

        print(f'[relay-sdk] ERROR: {msg}')

        if exc:
            traceback.print_exception(type(exc), exc, exc.__traceback__)

    def warn(self, msg):
        if not self._enabled:
            return

        print(f'[relay-sdk] WARN: {msg}')

    def info(self, msg):
        if not self._enabled:
            return

        print(f'[relay-sdk] INFO: {msg}')

    def debug(self, msg):
        if not self._enabled:
            return

        print(f'[relay-sdk] DEBUG: {msg}')


class Context:
    """Shared state across all managers."""

    def __init__(self, api_key, secret, org_id, env):
        self.api_key = api_key
        self.secret = secret
        self.org_id = org_id
        self.env = env

        self.logger = Logger(env)

        self.nats_client = None
        self.jetstream = None
        self.kv_bucket = None
        self.connected = False

        self.offline_buffer = []
        self.device = None
        self._subscription_registry = []

    async def publish_or_buffer(self, subject, payload):
        if self.connected and self.jetstream:
            try:
                ack = await self.jetstream.publish(subject, payload)
                return ack
            except Exception as e:
                self.logger.error(f'publish_or_buffer failed for {subject}, buffering', e)
                self.offline_buffer.append((subject, payload))
                return None

        self.offline_buffer.append((subject, payload))
        return None

    def register_subscription(self, entry):
        self._subscription_registry.append(entry)

    def unregister_subscription(self, key):
        self._subscription_registry = [
            e for e in self._subscription_registry if e.get('key') != key
        ]
