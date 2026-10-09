# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- Heads requests no longer run channel authorizers: only public, granted and
  relayed channels get heads; others are reported denied.
- Auth callouts are handled by a bounded pool of workers
  (`Limits.ConcurrentCallouts`, 32 by default) that `Close` waits for. The
  four-second budget of a callout starts when it arrives; requests that no
  longer fit in it or in the queue are dropped and counted in
  `Stats.CalloutDropped`.
- Relay reauthorization runs at most 50 due relays per renewal, least recently
  attempted first, four at a time per renewal and 16 per node. A relay that is
  not authorized again within twice `ReauthorizeInterval`, because the
  authorizer failed or was not reached, is removed with an `interrupted`
  control and the client subscribes again.
- `leave` requests skip the registry read and the concurrent request limit.
- `Authenticate` and `Grants` panic and `Channel` returns an error when called
  after `Start`.
- An unmanaged registry bucket needs a TTL of at least `MaxConnectionTTL` plus
  one minute.
- `Disconnect` revokes existing connections even when writing the revocation
  mark fails, and still returns the error.

### Fixed

- Panics in `Authenticate`, `Grants` and channel authorizers reject the
  connection or fail the request instead of crashing the process.
- A failed flush when re-adding an existing relay reports `unavailable`.

## [0.1.1] - 2026-10-08

### Fixed

- Retrying `Disconnect` enforces previously revoked connections after a transient
  connection-admin failure; `Revoked` counts only records changed by that call.
- System-account enforcement treats NATS's explicit “no such client or leafnode
  id” response as an already-closed connection, so repeated kicks succeed.
- `Server.Close` releases partial state when `Start` fails before creating its
  cancellation context.
- Document the application's responsibility for retrying failed revocations.

## [0.1.0] - 2026-10-08

### Added

- Server: auth callout with a JetStream connection registry, connect-time
  grants, subscribe-time channel authorizers with relayed delivery, broadcast
  and dispatch with toOthers, Refresh, Leave and Disconnect with pluggable
  connection admins.
- Event retention in a JetStream stream with gap detection, head checks and
  recovery that reports when it cannot be complete.
- Go client and TypeScript SDK (`@runforyou/jetcast`).
- Development server `cmd/jetcast-dev` and an embedded example.

[Unreleased]: https://github.com/runforyou-ai/jetcast/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/runforyou-ai/jetcast/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/runforyou-ai/jetcast/releases/tag/v0.1.0
