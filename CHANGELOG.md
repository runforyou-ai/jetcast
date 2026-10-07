# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Server: auth callout with a JetStream connection registry, connect-time
  grants, subscribe-time channel authorizers with relayed delivery, broadcast
  and dispatch with toOthers, Refresh, Leave and Disconnect with pluggable
  connection admins.
- Event retention in a JetStream stream with gap detection, head checks and
  recovery that reports when it cannot be complete.
- Go client and TypeScript SDK (`@runforyou/jetcast`).
- Development server `cmd/jetcast-dev` and an embedded example.
