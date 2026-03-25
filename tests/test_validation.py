"""Tests for the validation module."""

import pytest

from relayx_app_sdk.validation import (
    validate_ident,
    validate_event_name,
    validate_command_name,
    validate_rpc_name,
    validate_telemetry_metric,
    validate_hierarchy_name,
    validate_hierarchy_wildcard,
    validate_callable,
    validate_non_empty_list,
    validate_list,
    validate_dict,
    validate_connected,
    validate_positive_number,
    validate_iso8601,
    validate_start_before_end,
)


# ──────────────────────────────────────────────────────────────
# validate_ident
# ──────────────────────────────────────────────────────────────

class TestValidateIdent:

    def test_valid_ident(self):
        validate_ident('sensor-1', 'name')
        validate_ident('my_device', 'name')
        validate_ident('ABC123', 'name')

    def test_none_raises(self):
        with pytest.raises(ValueError, match='name is required'):
            validate_ident(None, 'name')

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match='name is required'):
            validate_ident('', 'name')

    def test_non_string_raises(self):
        with pytest.raises(ValueError, match='must be a string'):
            validate_ident(123, 'name')

    def test_invalid_chars_raises(self):
        with pytest.raises(ValueError, match='invalid characters'):
            validate_ident('hello world', 'name')

    def test_special_chars_raises(self):
        with pytest.raises(ValueError, match='invalid characters'):
            validate_ident('sensor@1', 'name')

    def test_dots_not_allowed(self):
        with pytest.raises(ValueError, match='invalid characters'):
            validate_ident('sensor.1', 'name')


# ──────────────────────────────────────────────────────────────
# validate_event_name
# ──────────────────────────────────────────────────────────────

class TestValidateEventName:

    def test_valid_names(self):
        validate_event_name('door-open')
        validate_event_name('motion_detected')
        validate_event_name('Alert123')

    def test_none_raises(self):
        with pytest.raises(ValueError, match='event name is required'):
            validate_event_name(None)

    def test_empty_raises(self):
        with pytest.raises(ValueError, match='event name is required'):
            validate_event_name('')

    def test_non_string_raises(self):
        with pytest.raises(ValueError, match='event name must be a string'):
            validate_event_name(42)

    def test_invalid_chars_raises(self):
        with pytest.raises(ValueError, match='event name contains invalid characters'):
            validate_event_name('door open')

    def test_dots_raises(self):
        with pytest.raises(ValueError, match='event name contains invalid characters'):
            validate_event_name('event.name')

    def test_special_chars_raises(self):
        with pytest.raises(ValueError, match='event name contains invalid characters'):
            validate_event_name('event@name')


# ──────────────────────────────────────────────────────────────
# validate_command_name
# ──────────────────────────────────────────────────────────────

class TestValidateCommandName:

    def test_valid_names(self):
        validate_command_name('setConfig')
        validate_command_name('reboot')
        validate_command_name('firmware-update')

    def test_none_raises(self):
        with pytest.raises(ValueError, match='command name is required'):
            validate_command_name(None)

    def test_empty_raises(self):
        with pytest.raises(ValueError, match='command name is required'):
            validate_command_name('')

    def test_non_string_raises(self):
        with pytest.raises(ValueError, match='command name must be a string'):
            validate_command_name(42)

    def test_invalid_chars_raises(self):
        with pytest.raises(ValueError, match='command name contains invalid characters'):
            validate_command_name('set config')

    def test_dots_raises(self):
        with pytest.raises(ValueError, match='command name contains invalid characters'):
            validate_command_name('set.config')

    def test_special_chars_raises(self):
        with pytest.raises(ValueError, match='command name contains invalid characters'):
            validate_command_name('cmd!name')


# ──────────────────────────────────────────────────────────────
# validate_rpc_name
# ──────────────────────────────────────────────────────────────

class TestValidateRpcName:

    def test_valid_names(self):
        validate_rpc_name('getStatus')
        validate_rpc_name('ping')
        validate_rpc_name('firmware-check')

    def test_none_raises(self):
        with pytest.raises(ValueError, match='rpc name is required'):
            validate_rpc_name(None)

    def test_empty_raises(self):
        with pytest.raises(ValueError, match='rpc name is required'):
            validate_rpc_name('')

    def test_non_string_raises(self):
        with pytest.raises(ValueError, match='rpc name must be a string'):
            validate_rpc_name(42)

    def test_invalid_chars_raises(self):
        with pytest.raises(ValueError, match='rpc name contains invalid characters'):
            validate_rpc_name('get status')

    def test_dots_raises(self):
        with pytest.raises(ValueError, match='rpc name contains invalid characters'):
            validate_rpc_name('rpc.call')

    def test_special_chars_raises(self):
        with pytest.raises(ValueError, match='rpc name contains invalid characters'):
            validate_rpc_name('rpc#call')


# ──────────────────────────────────────────────────────────────
# validate_telemetry_metric
# ──────────────────────────────────────────────────────────────

class TestValidateTelemetryMetric:

    def test_valid_names(self):
        validate_telemetry_metric('temperature')
        validate_telemetry_metric('cpu_usage')
        validate_telemetry_metric('pm2-5')

    def test_wildcard_allowed(self):
        validate_telemetry_metric('*')

    def test_none_raises(self):
        with pytest.raises(ValueError, match='metric is required'):
            validate_telemetry_metric(None)

    def test_empty_raises(self):
        with pytest.raises(ValueError, match='metric is required'):
            validate_telemetry_metric('')

    def test_non_string_raises(self):
        with pytest.raises(ValueError, match='metric must be a string'):
            validate_telemetry_metric(42)

    def test_invalid_chars_raises(self):
        with pytest.raises(ValueError, match='metric contains invalid characters'):
            validate_telemetry_metric('cpu usage')

    def test_dots_raises(self):
        with pytest.raises(ValueError, match='metric contains invalid characters'):
            validate_telemetry_metric('temp.celsius')

    def test_special_chars_raises(self):
        with pytest.raises(ValueError, match='metric contains invalid characters'):
            validate_telemetry_metric('metric@value')


# ──────────────────────────────────────────────────────────────
# validate_hierarchy_name
# ──────────────────────────────────────────────────────────────

class TestValidateHierarchyName:

    def test_valid_hierarchy(self):
        validate_hierarchy_name('floor.room.rack', 'heirarchy')
        validate_hierarchy_name('building-1.floor_2', 'heirarchy')

    def test_none_raises(self):
        with pytest.raises(ValueError, match='is required'):
            validate_hierarchy_name(None, 'heirarchy')

    def test_wildcards_not_allowed(self):
        with pytest.raises(ValueError, match='invalid characters'):
            validate_hierarchy_name('floor.*.rack', 'heirarchy')

    def test_gt_not_allowed(self):
        with pytest.raises(ValueError, match='invalid characters'):
            validate_hierarchy_name('floor.>', 'heirarchy')


# ──────────────────────────────────────────────────────────────
# validate_hierarchy_wildcard
# ──────────────────────────────────────────────────────────────

class TestValidateHierarchyWildcard:

    def test_valid_with_star(self):
        validate_hierarchy_wildcard('floor.*.rack', 'heirarchy')

    def test_valid_with_gt_at_end(self):
        validate_hierarchy_wildcard('floor.>', 'heirarchy')

    def test_gt_in_middle_raises(self):
        with pytest.raises(ValueError, match='">" can only be at the end'):
            validate_hierarchy_wildcard('floor.>.rack', 'heirarchy')

    def test_none_raises(self):
        with pytest.raises(ValueError, match='is required'):
            validate_hierarchy_wildcard(None, 'heirarchy')


# ──────────────────────────────────────────────────────────────
# validate_callable
# ──────────────────────────────────────────────────────────────

class TestValidateCallable:

    def test_valid_function(self):
        validate_callable(lambda x: x, 'callback')

    def test_none_raises(self):
        with pytest.raises(ValueError, match='is required'):
            validate_callable(None, 'callback')

    def test_non_callable_raises(self):
        with pytest.raises(ValueError, match='must be callable'):
            validate_callable('not-a-fn', 'callback')


# ──────────────────────────────────────────────────────────────
# validate_non_empty_list
# ──────────────────────────────────────────────────────────────

class TestValidateNonEmptyList:

    def test_valid(self):
        validate_non_empty_list(['a'], 'items')

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match='must be a non-empty list'):
            validate_non_empty_list([], 'items')

    def test_not_a_list_raises(self):
        with pytest.raises(ValueError, match='must be a non-empty list'):
            validate_non_empty_list('hello', 'items')

    def test_none_raises(self):
        with pytest.raises(ValueError, match='must be a non-empty list'):
            validate_non_empty_list(None, 'items')


# ──────────────────────────────────────────────────────────────
# validate_list
# ──────────────────────────────────────────────────────────────

class TestValidateList:

    def test_valid_empty_list(self):
        validate_list([], 'items')

    def test_valid_non_empty(self):
        validate_list([1, 2, 3], 'items')

    def test_not_a_list_raises(self):
        with pytest.raises(ValueError, match='must be a list'):
            validate_list('hello', 'items')


# ──────────────────────────────────────────────────────────────
# validate_dict
# ──────────────────────────────────────────────────────────────

class TestValidateDict:

    def test_valid(self):
        validate_dict({'key': 'val'}, 'config')

    def test_none_raises(self):
        with pytest.raises(ValueError, match='must be a dict'):
            validate_dict(None, 'config')

    def test_not_a_dict_raises(self):
        with pytest.raises(ValueError, match='must be a dict'):
            validate_dict([1, 2], 'config')


# ──────────────────────────────────────────────────────────────
# validate_connected
# ──────────────────────────────────────────────────────────────

class TestValidateConnected:

    def test_connected(self):
        validate_connected(True)

    def test_not_connected_raises(self):
        with pytest.raises(RuntimeError, match='Not connected'):
            validate_connected(False)


# ──────────────────────────────────────────────────────────────
# validate_positive_number
# ──────────────────────────────────────────────────────────────

class TestValidatePositiveNumber:

    def test_valid_int(self):
        validate_positive_number(5, 'timeout')

    def test_valid_float(self):
        validate_positive_number(1.5, 'timeout')

    def test_zero_is_valid(self):
        validate_positive_number(0, 'timeout')

    def test_negative_raises(self):
        with pytest.raises(ValueError, match='must be a non-negative number'):
            validate_positive_number(-1, 'timeout')

    def test_string_raises(self):
        with pytest.raises(ValueError, match='must be a non-negative number'):
            validate_positive_number('ten', 'timeout')


# ──────────────────────────────────────────────────────────────
# validate_iso8601
# ──────────────────────────────────────────────────────────────

class TestValidateISO8601:

    def test_valid_utc(self):
        validate_iso8601('2024-01-01T00:00:00Z', 'start')

    def test_valid_with_offset(self):
        validate_iso8601('2024-01-01T00:00:00+05:30', 'start')

    def test_none_raises(self):
        with pytest.raises(ValueError, match='is required'):
            validate_iso8601(None, 'start')

    def test_empty_raises(self):
        with pytest.raises(ValueError, match='is required'):
            validate_iso8601('', 'start')

    def test_garbage_raises(self):
        with pytest.raises(ValueError, match='valid ISO8601'):
            validate_iso8601('not-a-date', 'start')


# ──────────────────────────────────────────────────────────────
# validate_start_before_end
# ──────────────────────────────────────────────────────────────

class TestValidateStartBeforeEnd:

    def test_valid(self):
        validate_start_before_end(
            '2024-01-01T00:00:00Z',
            '2024-01-02T00:00:00Z',
        )

    def test_same_raises(self):
        with pytest.raises(ValueError, match='start must be before end'):
            validate_start_before_end(
                '2024-01-01T00:00:00Z',
                '2024-01-01T00:00:00Z',
            )

    def test_end_before_start_raises(self):
        with pytest.raises(ValueError, match='start must be before end'):
            validate_start_before_end(
                '2024-01-02T00:00:00Z',
                '2024-01-01T00:00:00Z',
            )
