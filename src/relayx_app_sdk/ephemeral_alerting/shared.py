import json
import time
import msgpack


# Maps source type to a lambda that builds the NATS subject.
SUBJECT_MAP = {
    'TELEMETRY': lambda org_id, env, device_id, metric: f'{org_id}.{env}.telemetry.{device_id}.{metric}',
    'COMMAND': lambda org_id, env, device_id, metric: f'{org_id}.{env}.command.queue.{device_id}.{metric}',
    'EVENT': lambda org_id, env, device_id, metric: f'{org_id}.{env}.events.{device_id}.{metric}',
}

# Index of the device_id token in each subject pattern
SUBJECT_DEVICE_INDEX = {
    'TELEMETRY': 3,
    'COMMAND': 4,
    'EVENT': 3,
}

# Index of the last meaningful token (metric) in each subject pattern
SUBJECT_LAST_TOKEN_INDEX = {
    'TELEMETRY': 4,
    'COMMAND': 5,
    'EVENT': 4,
}


def get_rule_id(rule):
    """Get the rule ID from either 'id' or 'rule_id' key."""
    return rule.get('id') or rule.get('rule_id')


async def build_data_subject(ctx, rule):
    """Build the NATS subject from the rule configuration."""
    topic = rule.get('config', {}).get('topic', {})

    source = topic.get('source') or rule.get('source', 'TELEMETRY')
    device_ident = topic.get('device_ident') or rule.get('device_ident', '*')
    metric = topic.get('last_token') or rule.get('metric', '*')

    if device_ident == '*':
        device_id = '*'
    else:
        device_id = await ctx.device.resolve_device_id(device_ident)

    builder = SUBJECT_MAP.get(source)
    if not builder:
        raise ValueError(f'Unknown source type: {source}')

    return builder(ctx.org_id, ctx.env, device_id, metric)


def resolve_ident_from_id(ctx, device_id):
    """Look up the device cache to find the ident from a device_id."""
    for ident, device in ctx.device.cache.items():
        if device.get('id') == device_id:
            return ident

    return None


def build_alert_payload(rule, rolling_state, timestamp, device_id, incident_id=None):
    """Build the alert payload dict for fire/resolved/ack events.
    Matches JS buildAlertPayload exactly (now with incident_id)."""
    return {
        'alert': {
            'id': get_rule_id(rule),
            'name': rule.get('name'),
            'type': rule.get('config', {}).get('topic', {}).get('source', 'TELEMETRY'),
            'config': rule.get('config', {}),
        },
        'device_id': device_id,
        'incident_id': incident_id,
        'rolling_state': dict(rolling_state),
        'timestamp': timestamp,
    }


def is_muted(rule):
    """Check if a rule is currently muted. Uses alert_mute_config (matches JS)."""
    mute_config = rule.get('alert_mute_config')
    if not mute_config:
        return False

    mute_type = mute_config.get('type')

    if mute_type == 'FOREVER':
        return True

    if mute_type == 'TIME_BASED':
        mute_till = mute_config.get('mute_till')
        if mute_till is not None:
            now_ms = int(time.time() * 1000)
            return now_ms < mute_till

    return False


def create_fresh_state():
    """Return a fresh state dict. Matches JS createFreshState exactly."""
    return {
        'status': 'normal',
        'last_evaluated_at': None,
        'clear_since': None,
        'breached_since': None,
        'last_fired': 0,
        'acked_by': None,
        'acked_at': None,
        'ack_notes': None,
        'incident_id': None,
    }


async def dispatch_notifications(ctx, rule, data):
    """Send a notification via NATS request. Matches JS subject + payload."""
    channels = rule.get('notification_channel')
    if not channels or len(channels) == 0:
        return

    subject = f'api.iot.notification.{ctx.org_id}.dispatch'

    payload = json.dumps({
        'notification_channel': channels,
        'alert_data': data,
    }).encode()

    try:
        await ctx.nats_client.request(subject, payload, timeout=10)
    except Exception as e:
        ctx.logger.error('Failed to dispatch notification', e)


async def publish_event(ctx, rule, event_type, payload):
    """Publish an alert event via publish_or_buffer with msgpack encoding."""
    rule_id = get_rule_id(rule)
    subject = f'{ctx.org_id}.{ctx.env}.alerts.listen.{rule_id}.{event_type}'

    data = msgpack.packb(payload)
    await ctx.publish_or_buffer(subject, data)
