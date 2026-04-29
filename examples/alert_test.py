"""
Interactive end-to-end test harness for the alerting changes.

Drive a device with telemetry separately (e.g. a simulator publishing
`temperature` and `humidity` for DEVICE_IDENT).

Usage:
    RELAY_API_KEY=...  RELAY_SECRET=...  DEVICE_IDENT=sensor_01 \\
        python examples/alert_test.py
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

from relayx_app_sdk import RelayApp


API_KEY = os.environ.get('RELAY_API_KEY', '')
SECRET = os.environ.get('RELAY_SECRET', '')
DEVICE_IDENT = os.environ.get('DEVICE_IDENT')

RULE_THRESHOLD_NAME = 'high_temp_test'
RULE_EPHEMERAL_NAME = 'high_humidity_test_eph'


def iso_now():
    n = datetime.now(timezone.utc)
    return n.strftime('%Y-%m-%dT%H:%M:%S.') + f'{n.microsecond // 1000:03d}Z'


async def ask(prompt):
    return (await asyncio.to_thread(input, prompt)).strip()


def iso_ago(seconds):
    n = datetime.now(timezone.utc) - timedelta(seconds=seconds)
    return n.strftime('%Y-%m-%dT%H:%M:%S.') + f'{n.microsecond // 1000:03d}Z'


async def main():
    if not DEVICE_IDENT:
        print('DEVICE_IDENT env var required', file=sys.stderr)
        sys.exit(1)

    app = RelayApp({
        'api_key': API_KEY,
        'secret': SECRET,
        'mode': 'production',
    })

    app.connection.listeners(lambda evt: print(f'[connection] {evt}'))

    await app.connect()
    print('Connected to RelayX\n')

    # Resolve device id.
    device = await app.device.get({'ident': DEVICE_IDENT})
    if not device:
        print(f'Device "{DEVICE_IDENT}" not found', file=sys.stderr)
        sys.exit(1)
    device_id = device['id']
    print(f'Using device "{DEVICE_IDENT}" (id={device_id})\n')

    threshold_alert = None
    ephemeral_alert = None

    # Backend THRESHOLD rule.
    threshold_alert = await app.alert.get(RULE_THRESHOLD_NAME)
    if threshold_alert is None:
        threshold_alert = await app.alert.create({
            'name': RULE_THRESHOLD_NAME,
            'description': 'test rig: temp > 30 for 5s',
            'type': 'THRESHOLD',
            'metric': 'temperature',
            'config': {
                'scope': {'type': 'DEVICE', 'value': device_id},
                'operator': '>',
                'value': 30,
                'duration': 5,
                'recovery_duration': 5,
                'cooldown': 10,
            },
            'notification_channel': [],
        })
        print(f'[setup] created backend rule "{RULE_THRESHOLD_NAME}"')
    else:
        print(f'[setup] reusing backend rule "{RULE_THRESHOLD_NAME}"')

    await threshold_alert.listen({
        'on_fire': lambda d: print(f"[backend] FIRE     incident={d.get('incident_id') or '-'}"),
        'on_resolved': lambda d: print(f"[backend] RESOLVED incident={d.get('incident_id') or '-'}"),
        'on_ack': lambda d: print(
            f"[backend] ACK      incident={d.get('incident_id') or '-'}  "
            f"by={(d.get('ack') or {}).get('acked_by')}"
        ),
    })

    # Ephemeral rule.
    ephemeral_alert = await app.alert.get(RULE_EPHEMERAL_NAME)
    if ephemeral_alert is None:
        ephemeral_alert = await app.alert.create_ephemeral({
            'name': RULE_EPHEMERAL_NAME,
            'description': 'test rig: humidity > 70 for 5s',
            'config': {
                'topic': {
                    'source': 'TELEMETRY',
                    'device_ident': DEVICE_IDENT,
                    'last_token': 'humidity',
                },
                'duration': 5,
                'recovery_duration': 5,
                'cooldown': 10,
                'recovery_eval_type': 'VALUE',
            },
            'notification_channel': [],
        })
        print(f'[setup] created ephemeral rule "{RULE_EPHEMERAL_NAME}"')
    else:
        print(f'[setup] reusing ephemeral rule "{RULE_EPHEMERAL_NAME}"')

    def evaluator(rolling):
        v = (rolling.get(DEVICE_IDENT, {}).get('humidity') or {}).get('value', 0)
        return v > 70

    ephemeral_alert.set_evaluator(evaluator)
    await ephemeral_alert.listen({
        'on_fire': lambda d: print(f"[ephem]   FIRE     incident={d.get('incident_id') or '-'}"),
        'on_resolved': lambda d: print(f"[ephem]   RESOLVED incident={d.get('incident_id') or '-'}"),
        'on_ack': lambda d: print(
            f"[ephem]   ACK      incident={d.get('incident_id') or '-'}  "
            f"by={(d.get('ack') or {}).get('acked_by')}"
        ),
    })

    print()

    async def action(choice):
        if choice == '1':
            print('\n-> ack backend incident')
            ok = await app.alert.ack({
                'alert_id': threshold_alert['id'],
                'device_id': device_id,
                'acked_by': 'alice',
                'ack_notes': 'test ack from menu',
            })
            print(f'   result: {ok}')

        elif choice == '2':
            print('\n-> ack ephemeral incident')
            ok = await app.alert.ack({
                'alert_id': ephemeral_alert['id'],
                'device_id': device_id,
                'acked_by': 'bob',
                'ack_notes': 'test ack from menu',
            })
            print(f'   result: {ok}')

        elif choice == '3':
            print('\n-> alert.history (backend, all states, last hour)')
            res = await app.alert.history({
                'rule_type': 'DEVICE',
                'device_ident': DEVICE_IDENT,
                'rule_states': ['fire', 'resolved', 'ack'],
                'start': iso_ago(360 * 60),
                'end': iso_now(),
            })
            events = res['events']
            print(f'   {len(events)} events')
            by_incident = {}
            for e in events:
                k = e.get('incident_id') or '(none)'
                by_incident.setdefault(k, []).append(e['state'])
            for inc, states in by_incident.items():
                print(f"   incident {inc}: {' -> '.join(states)}")

        elif choice == '4':
            print('\n-> alert.history filtered by incident_id')
            inc = await ask('   incident_id: ')
            if not inc:
                print('   skipped')
                return

            res = await app.alert.history({
                'rule_type': 'DEVICE',
                'device_ident': DEVICE_IDENT,
                'incident_id': inc,
                'rule_states': ['fire', 'resolved', 'ack'],
                'start': iso_ago(24 * 60 * 60),
                'end': iso_now(),
            })
            for e in res['events']:
                print(f"   {e.get('timestamp')}  {e.get('state')}")

        elif choice == '5':
            print('\n-> NEGATIVE: ack backend without device_id (should throw)')
            try:
                await app.alert.ack({
                    'alert_id': threshold_alert['id'],
                    'acked_by': 'x',
                })
                print('   X DID NOT throw')
            except Exception as err:
                print(f'   threw: {err}')

        elif choice == '6':
            print('\n-> NEGATIVE: history with rule_states=["ack_all"]')
            try:
                await app.alert.history({
                    'rule_type': 'DEVICE',
                    'device_ident': DEVICE_IDENT,
                    'rule_states': ['ack_all'],
                    'start': iso_ago(60),
                    'end': iso_now(),
                })
                print('   X DID NOT throw')
            except Exception as err:
                print(f'   threw: {err}')

        elif choice == '7':
            print('\n-> NEGATIVE: ack_all / ack_history presence')
            print(f"   hasattr(app.alert, 'ack_all')      = {hasattr(app.alert, 'ack_all')}")
            print(f"   hasattr(app.alert, 'ack_history')  = {hasattr(app.alert, 'ack_history')}")
            print('   (both should be False)')

        elif choice == '8':
            print('\n-> list all alerts on this org')
            alerts = await app.alert.list()
            for a in alerts:
                print(f"   - {a.get('name')}  ({a.get('type')})  id={a.get('id')}")

        elif choice == 'q':
            print('\n-> cleanup')
            try:
                await threshold_alert.stop()
            except Exception:
                pass
            try:
                await ephemeral_alert.stop()
            except Exception:
                pass
            if threshold_alert and threshold_alert.get('id'):
                await app.alert.delete(threshold_alert['id'])
                print(f'   deleted {RULE_THRESHOLD_NAME}')
            if ephemeral_alert and ephemeral_alert.get('id'):
                await app.alert.delete(ephemeral_alert['id'])
                print(f'   deleted {RULE_EPHEMERAL_NAME}')
            await app.disconnect()
            print('   disconnected')
            sys.exit(0)

        else:
            print(f'unknown: {choice}')

    def print_menu():
        print('\n------------ alert_test menu ------------')
        print('  1   ack backend incident')
        print('  2   ack ephemeral incident')
        print('  3   alert.history all states (last 6h)')
        print('  4   alert.history filtered by incident_id')
        print('  5   neg: ack backend w/o device_id')
        print('  6   neg: history with ack_all')
        print('  7   neg: ack_all/ack_history presence')
        print('  8   list all alerts')
        print('  q   cleanup + quit')
        print('-----------------------------------------')

    print_menu()
    while True:
        choice = await ask('> ')
        if not choice:
            print_menu()
            continue
        try:
            await action(choice)
        except Exception as err:
            print(f'action failed: {err}')


if __name__ == '__main__':
    asyncio.run(main())
