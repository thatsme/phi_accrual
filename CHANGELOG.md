# Changelog

All notable changes to this project are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Telemetry schema stability.** Within the `0.1.x` and later `1.x`
lines, the event names, measurement keys, and metadata keys of the
`[:phi_accrual, ...]` telemetry schema are contract — breaking
changes only in `v2.0.0`. See the README for the committed schema.

## [Unreleased]

## [0.1.0] - 2026-04-20

Initial public release. **Alpha** — the API may change before `v1.0`.
The telemetry event schema is already stable; the rest is subject to
tuning based on real-deployment feedback.

### Added

- **Core EWMA estimator** (`PhiAccrual.Core`) — pure Hayashibara 2004
  math with West 1979 incremental variance and **separate α for mean
  and variance**. Numerically stable φ computation via softplus (no
  `:math.exp` overflow on extreme deltas).
- **Four-state φ result** — distinguishes `:steady`, `:recovering`
  (warm estimator absorbing a recent large gap), `:insufficient_data`
  (bootstrap phase), and `:stale` (no heartbeat past grace window).
- **Per-node estimator processes** — each monitored node runs its own
  `PhiAccrual.Estimator` GenServer under a `DynamicSupervisor`,
  registered via `Registry`. Isolation means one slow node cannot
  block observations for others.
- **Overload shedding** — `PhiAccrual.observe/2` drops samples and
  emits `[:phi_accrual, :overload, :shed]` telemetry when the target
  estimator's mailbox exceeds `:shed_threshold`.
- **Local-pause awareness** (`PhiAccrual.PauseMonitor`) — subscribes
  to `:erlang.system_monitor` for `:long_gc`, `:long_schedule`, and
  `:busy_dist_port`. Publishes pause state via `:persistent_term` and
  tags φ output with `local_pause?` / `confidence` metadata so
  consumers can filter rather than having the detector silently
  freeze.
- **Bring-your-own-signal API** — call `PhiAccrual.observe/2` from
  any code that already receives cross-node traffic (GenServer
  replies, `:pg`, `:global` sync).
- **Reference source** (`PhiAccrual.Source.DistributionPing`) —
  opt-in 1 s app-layer ping over BEAM distribution. Supervised
  separately from estimators so a ping-source restart does not wipe
  state.
- **Optional threshold layer** (`PhiAccrual.Threshold`) — subscribes
  to the φ gauge stream and emits `:suspected` / `:recovered` events
  with configurable hysteresis. Multiple instances coexist.
- **Telemetry schema (contract)** — `[:sample, :observed]`,
  `[:phi, :computed]`, `[:local_pause, :start|:stop]`,
  `[:overload, :shed]`, `[:source, :started]`,
  `[:threshold, :suspected|:recovered]`.

### Not included (deferred to v2)

- Dedicated UDP channel (`UdpSource`) — necessary for decision-grade
  detection by escaping head-of-line blocking on the BEAM
  distribution socket.
- Non-parametric / mixture estimators — pending real-deployment
  traces to justify the added complexity.
- Multi-node `:peer`-based integration tests.
- `phi_accrual_libcluster` companion package.

[Unreleased]: https://github.com/thatsme/phi_accrual/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/thatsme/phi_accrual/releases/tag/v0.1.0
