import re
from datetime import datetime


IDENT_REGEX = re.compile(r'^[a-zA-Z0-9_-]+$')
HIERARCHY_REGEX = re.compile(r'^[a-zA-Z0-9_.\-]+$')
HIERARCHY_WILDCARD_REGEX = re.compile(r'^[a-zA-Z0-9_.*>\-]+$')


def validate_ident(value, field_name):
    if value is None or value == '':
        raise ValueError(f'{field_name} is required')

    if not isinstance(value, str):
        raise ValueError(f'{field_name} must be a string')

    if not IDENT_REGEX.match(value):
        raise ValueError(f'{field_name} contains invalid characters. Allowed: a-z, A-Z, 0-9, _, -')


def validate_hierarchy_name(value, field_name):
    if value is None or value == '':
        raise ValueError(f'{field_name} is required')

    if not isinstance(value, str):
        raise ValueError(f'{field_name} must be a string')

    if not HIERARCHY_REGEX.match(value):
        raise ValueError(f'{field_name} contains invalid characters. Allowed: a-z, A-Z, 0-9, _, -, .')


def validate_hierarchy_wildcard(value, field_name):
    if value is None or value == '':
        raise ValueError(f'{field_name} is required')

    if not isinstance(value, str):
        raise ValueError(f'{field_name} must be a string')

    if not HIERARCHY_WILDCARD_REGEX.match(value):
        raise ValueError(f'{field_name} contains invalid characters. Allowed: a-z, A-Z, 0-9, _, -, ., *, >')

    tokens = value.split('.')
    gt_indices = [i for i, t in enumerate(tokens) if t == '>']

    if gt_indices and gt_indices[0] != len(tokens) - 1:
        raise ValueError(f'{field_name} invalid: ">" can only be at the end of a topic')


def validate_event_name(value):
    if value is None or value == '':
        raise ValueError('event name is required')

    if not isinstance(value, str):
        raise ValueError('event name must be a string')

    if not IDENT_REGEX.match(value):
        raise ValueError('event name contains invalid characters. Allowed: a-z, A-Z, 0-9, _, -')


def validate_command_name(value):
    if value is None or value == '':
        raise ValueError('command name is required')

    if not isinstance(value, str):
        raise ValueError('command name must be a string')

    if not IDENT_REGEX.match(value):
        raise ValueError('command name contains invalid characters. Allowed: a-z, A-Z, 0-9, _, -')


def validate_rpc_name(value):
    if value is None or value == '':
        raise ValueError('rpc name is required')

    if not isinstance(value, str):
        raise ValueError('rpc name must be a string')

    if not IDENT_REGEX.match(value):
        raise ValueError('rpc name contains invalid characters. Allowed: a-z, A-Z, 0-9, _, -')


def validate_telemetry_metric(value):
    if value is None or value == '':
        raise ValueError('metric is required')

    if not isinstance(value, str):
        raise ValueError('metric must be a string')

    if value == '*':
        return

    if not IDENT_REGEX.match(value):
        raise ValueError('metric contains invalid characters. Allowed: a-z, A-Z, 0-9, _, -')


def validate_callable(value, field_name):
    if value is None:
        raise ValueError(f'{field_name} is required')

    if not callable(value):
        raise ValueError(f'{field_name} must be callable')


def validate_non_empty_list(value, field_name):
    if not isinstance(value, list) or len(value) == 0:
        raise ValueError(f'{field_name} must be a non-empty list')


def validate_list(value, field_name):
    if not isinstance(value, list):
        raise ValueError(f'{field_name} must be a list')


def validate_dict(value, field_name):
    if value is None or not isinstance(value, dict):
        raise ValueError(f'{field_name} must be a dict')


def validate_connected(connected):
    if not connected:
        raise RuntimeError('Not connected. Call app.connect() first.')


def validate_positive_number(value, field_name):
    if not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f'{field_name} must be a non-negative number')


def validate_iso8601(value, field_name):
    if value is None or value == '':
        raise ValueError(f'{field_name} is required')

    if not isinstance(value, str):
        raise ValueError(f'{field_name} must be a string')

    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        raise ValueError(f'{field_name} must be a valid ISO8601 datetime string')


def validate_start_before_end(start, end):
    s = datetime.fromisoformat(start.replace('Z', '+00:00'))
    e = datetime.fromisoformat(end.replace('Z', '+00:00'))

    if s >= e:
        raise ValueError('start must be before end')
