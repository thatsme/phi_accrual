---
name: φ-accrual Elixir library — committed v1 scope
description: Shippable library plan: source-agnostic φ detector, observability-grade, dual-α EWMA, confidence-flagged output, per-node GenServers, shed-on-overload, ElixirConf EU soft deadline
type: project
originSessionId: e06628ed-879d-4801-a8dd-58695f5393b9
---
User has committed to building an Elixir φ-accrual failure detector library (gap is real — existing libcluster/swarm/horde/partisan ecosystem uses binary detectors or entangles detection with membership). Name: `phi_accrual` (searchable, matches what people look for).

## v1 architecture (fixed)

**Core estimator — EWMA with two α values (West 1979 incremental form):**
```
delta = sample - mean
mean'     = mean + α_mean * delta
variance' = (1 - α_var) * (variance + α_var * delta^2)
```
Separate `alpha_mean` / `alpha_var` — variance needs more smoothing than mean, otherwise a single anomalous sample craters φ. Default both equal, document that tuning variance slower is usually correct. Two knobs is still fewer than Akka's de facto three (window, min-samples, threshold) and more statistically honest.

**Local-pause suppression via `:erlang.system_monitor`:**
```elixir
:erlang.system_monitor(self(), [
  {:long_schedule, 100},   # scheduler hold (ms)
  {:long_gc, 50},          # GC pause (ms)
  :busy_dist_port          # distribution TCP send buffer full → HoL blocking
])
```
`:busy_dist_port` is the sleeper — it's the HoL-blocking event made observable and is arguably more important than GC for v1's stated failure mode.

**Confidence-flagged output instead of freeze-or-widen.** Freezing creates silent-detector failure modes; widening variance needs a principled multiplier. Tag φ output with `local_pause?` / `confidence` metadata and push policy to consumers — dashboards show degraded confidence visually, alerts filter on flag, future decision-grade consumers require `confidence=true`.

**Four-state result type** — distinguish cold-start from post-partition return:
```elixir
@type phi_result ::
  {:ok, float(), :steady}                # N samples, estimator warm
  | {:ok, float(), :recovering}          # N samples but recent gap > threshold
  | {:insufficient_data, pos_integer()}  # bootstrap, samples remaining
  | {:stale, milliseconds()}             # no heartbeat for > grace period
```
Cold-start ≠ node-comes-back-after-partition; operators alert on them differently. Bootstrap N=8 (Akka default).

**Clock discipline:** `:erlang.monotonic_time(:native)` only. Source adapters MUST use the same clock source; cross-node timestamps are meaningless and must not appear in arrival-interval calculations. Detector only reasons about local arrival times. State explicitly in the source-adapter behaviour docs.

**Threshold is a consumer concern, not library config.** The detector emits `[:phi_accrual, :sample, :observed]` with φ values — it does NOT bake in a default threshold. A separate optional `PhiAccrual.Threshold` module subscribes to samples and emits `:suspected` / `:recovered` events. Two processes, two responsibilities. Consumers can skip `Threshold` entirely and roll their own hysteresis, or run multiple threshold instances (φ=4 for dashboards, φ=8 for automated routing). Prevents the library from owning policy: per-consumer thresholds, hysteresis tuning, flapping suppression.

**Concurrency model — one GenServer per monitored node:**
- Per-node estimator GenServers, supervised under a `DynamicSupervisor`, registered via `Registry`.
- `PhiAccrual.observe(node, ts)` is a `GenServer.cast` to the per-node pid (Registry lookup via ETS — fine to do per-call, cache in caller only if profiling shows it).
- Rationale: estimator state is independent per node; per-node isolation means one slow node's system_monitor handling can't block others; single-GenServer-with-map serializes all observations behind one mailbox, defeating BEAM's advantage.

**Back-pressure: shed on overload, with telemetry.**
- `observe/2` checks mailbox length; drops the sample if backed up.
- Emits `[:phi_accrual, :overload, :shed]` so shedding is observable (silent shedding is worse than useless in a failure detector).
- Philosophy in docs: "for failure detection you don't need every heartbeat, you need enough heartbeats — if you're dropping, your heartbeat rate is too high for your estimator settings, tune α."
- Sampling (every Nth) is rejected in favor of shedding because it's simpler and the detector is inherently statistical.

**Committed `:telemetry` event schema (shape-stable across v1.x) — as shipped:**
```
[:phi_accrual, :sample, :observed]       # measurements: %{interval_ms}
                                          # metadata: %{node, local_pause?}
[:phi_accrual, :phi, :computed]          # measurements: %{phi, elapsed_ms}
                                          # metadata: %{node, state, local_pause?, confidence}
                                          #   state ∈ [:steady, :recovering, :insufficient_data, :stale]
[:phi_accrual, :local_pause, :start]     # metadata: %{kind}
[:phi_accrual, :local_pause, :stop]
[:phi_accrual, :overload, :shed]         # measurements: %{mailbox_len}
                                          # metadata: %{node}
[:phi_accrual, :source, :started]        # metadata: %{source, interval_ms}
[:phi_accrual, :threshold, :suspected]   # from optional Threshold module
[:phi_accrual, :threshold, :recovered]
```
**Split rationale:** `[:sample, :observed]` is sparse (one per heartbeat arrival, carries the just-observed interval). `[:phi, :computed]` is a periodic gauge stream (per-node tick, dashboard-friendly). Consumers wanting a simple Prometheus gauge subscribe to `[:phi, :computed]`; consumers wanting individual-sample scrutiny subscribe to `[:sample, :observed]`. Users wire either to `telemetry_metrics_prometheus`, logs, or custom alerting. Schema shape is a v1 commitment; changing in v2 is a breaking change we don't want.

**Source model — bring-your-own-signal:**
- Public API `PhiAccrual.observe(node, :erlang.monotonic_time(:native))` — apps with existing cross-node chatter (GenServer replies, `:pg`, `:global` sync) call this on received traffic.
- Reference `DistributionPing` source: cheap app-layer pings via `:rpc.cast/4` at **1s default interval** (Akka default), configurable. Documented as inheriting HoL blocking. Note: in a 50-node cluster that's 2500 pings/sec of distribution traffic — cheap per-ping, not free in aggregate.
- `DistributionPing` is supervised **separately** from the per-node estimator supervisor — estimator state must survive a ping-source restart. Emits a telemetry event on restart; crash-recovery path is tested.
- `UdpSource` is a **v2 feature, not a polish pass** — it's what makes the detector decision-grade by escaping HoL.

## v1 component list

```
phi_accrual/
├── Core estimator (EWMA mean + EWMA variance, separate α)
├── Pause suppression (:erlang.system_monitor, confidence flag)
├── Bootstrap / staleness state machine (four-state result)
├── Clock discipline (:erlang.monotonic_time/1 enforced)
├── Per-node estimator GenServer + DynamicSupervisor + Registry
├── Overload shedding (mailbox check + telemetry event)
├── Source-adapter behaviour (bring-your-own or use reference)
├── Reference source: DistributionPing (observability grade, HoL-affected, 1s default)
├── Optional PhiAccrual.Threshold module (subscribes to samples, emits suspected/recovered)
├── Telemetry events (committed schema)
└── README: SWIM positioning, HoL-not-GC as v1 caveat, v2 roadmap
```
Target ~1000–1500 lines including docs and tests.

## Explicitly out of v1
Own UDP channel, non-parametric estimator (Satzger / two-component mixture), Ra/Raft integration, cluster-management responsibilities, **libcluster strategy wrapper** (dropped — it's membership-discovery, not failure-detection; confuses positioning; ship `phi_accrual_libcluster` as a separate package later if there's demand).

## Testing strategy
Failure detectors are hard to test against wall-clock. Use StreamData for property-based tests of estimator math (given this sequence of intervals, φ must fall in this range); integration tests use OTP 25+ `:peer` module for simulated nodes and partitions. **Never sleep against wall-clock** — use injected clocks (functions returning monotonic time) so tests are deterministic and fast.

## Release / versioning discipline
v1.x is telemetry-schema-stable: event names, measurement keys, metadata keys are contract. Breaking schema changes = v2. Document this in README so contributors understand the constraint is load-bearing, not preference.

## README positioning
Not a SWIM impl. Observability-grade, not decision-grade. **Primary v1 caveat: HoL blocking** (passive source inherits BEAM distribution's TCP-multiplexing behaviour). Secondary caveat: Gaussian misbehaves under bimodal BEAM GC distributions — correlate with `:erlang.statistics(:garbage_collection)` before alerting. UDP source (v2) is the fix for both.

## v2 line
Add `UdpSource`, collect real traces from v1 deployments, evaluate whether Gaussian-EWMA needs replacement with Satzger non-parametric or two-component mixture **based on evidence, not speculation**.

## Why (roadmap shape)
Three composable small libraries (φ-accrual → HLC + causal broadcast → SWIM-Lifeguard standalone) is the user's stated roadmap, rather than one framework.

## How to apply
When returning to this topic, default to the v1 scope above — don't re-litigate decisions already settled (EWMA vs window, dual-α vs single, confidence-flag vs freeze/widen, bring-your-own-signal vs own-channel, UDP deferred to v2, Gaussian kept for v1, threshold as consumer concern, per-node GenServer concurrency, shed-on-overload, libcluster dropped). Soft deadline: ElixirConf EU (exact date unconfirmed — user mentioned a "concurrency training week" adjacent to it).
