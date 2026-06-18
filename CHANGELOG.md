# Changelog

All notable changes to `relayx_app_sdk` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-06-18

### Changed
- **History reads now go over HTTP** (the influx-db-service) instead of NATS
  streaming. Affects `telemetry.history()`, `telemetry.latest()`,
  `events.history()`, `command.history()`, `log.history()`, and
  `alert.history()`. Results are paginated and fetched transparently; the auth
  token is fetched once and refreshed on a 401/403. Method inputs and return
  shapes are otherwise unchanged.

### Added
- New examples: `telemetry_history.py`, `events_history.py`, `logs_history.py`.

### Removed
- **BREAKING:** The `on_frame` live callback has been removed from every
  `history()` method. HTTP pagination cannot deliver per-frame live updates. Use
  the returned data directly, or subscribe to live data via the relevant
  manager's streaming method (e.g. `telemetry.stream()`).

[0.2.0]: https://github.com/relay-x/app-sdk/releases/tag/v0.2.0
