from .owner import EphemeralOwner
from .listener import EphemeralListener


class EphemeralEngine:
    """Facade for the ephemeral alerting subsystem.

    If an evaluator function is set before calling listen(), the engine
    operates in "owner" mode (EphemeralOwner) — it subscribes to raw data,
    evaluates the rule, and publishes alert events.

    Otherwise it operates in "listener" mode (EphemeralListener) — it
    subscribes to alert events published by an owner.
    """

    def __init__(self, ctx, rule):
        self._ctx = ctx
        self._rule = rule
        self._evaluator = None
        self._delegate = None
        self._mode = None

    def set_evaluator(self, fn):
        """Set the evaluator function for owner mode.

        The evaluator receives (value, rule, data) and must return a boolean
        indicating whether the alert condition is triggered.
        """
        if not callable(fn):
            raise ValueError('evaluator must be callable')

        self._evaluator = fn

    async def listen(self, callbacks=None):
        """Start listening. Creates an EphemeralOwner if an evaluator is set,
        otherwise creates an EphemeralListener."""
        if self._delegate:
            return

        if self._evaluator:
            self._mode = 'owner'
            self._delegate = EphemeralOwner(
                self._ctx, self._rule, self._evaluator, callbacks,
            )
        else:
            self._mode = 'listener'
            self._delegate = EphemeralListener(
                self._ctx, self._rule, callbacks,
            )

        await self._delegate.start()

    async def stop(self):
        """Stop the delegate (owner or listener)."""
        if self._delegate:
            await self._delegate.stop()
            self._delegate = None
            self._mode = None

    async def ack(self, acked_by, ack_notes=None):
        """Acknowledge the current alert. Only available in owner mode."""
        if not self._delegate or self._mode != 'owner':
            raise RuntimeError('ack is only available in owner mode')

        await self._delegate.ack(acked_by, ack_notes)

    async def ack_all(self, acked_by, ack_notes=None):
        """Acknowledge all alerts. Only available in owner mode."""
        if not self._delegate or self._mode != 'owner':
            raise RuntimeError('ack_all is only available in owner mode')

        await self._delegate.ack_all(acked_by, ack_notes)

    @property
    def state(self):
        if self._delegate:
            return self._delegate.state
        return 'stopped'

    @property
    def rolling_state(self):
        if self._delegate:
            return self._delegate.rolling_state
        return {}

    @property
    def mode(self):
        return self._mode
