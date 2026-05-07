# phi_accrual

A source-agnostic φ-accrual failure detector for Elixir/OTP, built on
Hayashibara et al. 2004 with a dual-α EWMA estimator, head-of-line and
local-pause awareness, and a telemetry-first API.

> ⚠️ **Alpha — `v0.1.x`.** The API and configuration surface may change
> before `v1.0`. The **telemetry event schema is already stable** (see
> [Versioning](#versioning)), but everything else is subject to tuning
> based on real-deployment feedback. Production use at your own risk;
> please open issues as you find rough edges.

> **Observability-grade, not decision-grade.** Designed for dashboards,
> alerting, and operator intuition — not for automated routing, quorum,
> or correctness decisions. See [limitations](#limitations) for why.

## Quick start

```elixir
# mix.exs
def deps do
  [{:phi_accrual, "~> 0.1"}]
end
```

The application auto-starts. Feed in heartbeat arrivals from anywhere
your code already receives cross-node traffic, and read out φ on demand:

```elixir
# Call this whenever you receive evidence that a peer is alive —
# a GenServer reply, a :pg broadcast, an :rpc response, a custom ping.
# First call for an unknown node auto-tracks it with defaults.
PhiAccrual.observe(:"peer@host")

# Query φ at any time.
PhiAccrual.phi(:"peer@host")
#=> {:ok, 0.42, :steady}
```

That's the whole core loop: **feed in arrivals, read out φ.** Everything
below is about making it useful in production — reference heartbeat
sources if you have none of your own, telemetry wiring for Prometheus,
thresholding with hysteresis, and honest limitations.

## What it does

Given a stream of heartbeat arrivals from a remote node, the detector
maintains an EWMA estimate of the inter-arrival distribution (mean and
variance, independently smoothed) and emits a continuous suspicion
value φ. φ is calibrated so that `φ ≈ -log₁₀(P(arrival still pending))`:

| φ value | Rough meaning                                          |
| ------- | ------------------------------------------------------ |
| 1       | 1-in-10 chance the node is dead                        |
| 3       | 1-in-1000                                              |
| 8       | 1-in-100 000 000 — very likely down                    |

**Thresholding is a consumer concern.** The detector does not decide
whether a node is up or down; it publishes φ, and you (or the optional
`PhiAccrual.Threshold` module) decide what crosses what line.

## Why another failure detector?

The Elixir/OTP ecosystem has plenty of cluster-management libraries
(`libcluster`, `swarm`, `horde`, `partisan`), but all of them use
binary up/down detectors or entangle detection with membership.
`phi_accrual` is the thing that goes alongside them: a pure detector,
unopinionated about who sends heartbeats, what the topology looks like,
or what to do when φ gets high.

## Usage — bring your own signal

Anything that arrives from a remote node is evidence of liveness. If
your app already has cross-node traffic, call `observe/2` from the
receive path — no extra network cost:

```elixir
defmodule MyApp.Chatter do
  use GenServer

  def handle_info({:reply_from, node}, state) do
    PhiAccrual.observe(node)
    {:noreply, state}
  end
end
```

Then pattern-match on `phi/1` to handle every result state:

```elixir
case PhiAccrual.phi(:"node_a@host") do
  {:ok, phi, :steady}        -> # warm estimator, normal
  {:ok, phi, :recovering}    -> # warm estimator, absorbing a recent gap
  {:insufficient_data, n}    -> # still in bootstrap, `n` samples remaining
  {:stale, elapsed_ms}       -> # no arrival for > stale_after_ms
  {:error, :not_tracked}     -> # never observed
end
```

Call `PhiAccrual.track(node, opts)` **before** your first `observe` if
you need custom per-node estimator options; otherwise the first
`observe` auto-tracks with defaults.

## Usage — reference source

If you have no existing cross-node chatter, enable the bundled
`DistributionPing` source in config:

```elixir
# config/runtime.exs
config :phi_accrual,
  distribution_ping: [interval_ms: 1_000, auto_track: true]
```

Each node then pings every peer every `interval_ms` over BEAM
distribution. Cheap per-ping, but cluster cost is O(N²) —
at 50 nodes and 1 s interval that's 2 500 pings/second of distribution
traffic.

**This source inherits HoL blocking** — see
[limitations](#limitations). The v2 `UdpSource` will escape it.

## What happens when a node fails

Suppose `:node_a@host` has been heartbeating every ~1 s for a few
minutes. Its estimator has mean ≈ 1 000 ms, σ ≈ 50 ms, and φ hovers
around 0.3 (the median for an on-schedule arrival).

Then the node goes dark. Here is the timeline, using the default
options and a threshold instance configured at `suspect_at: 4.0`,
`recover_at: 3.0`:

```
t=0s    last heartbeat arrives. φ ≈ 0.3.
        → [:phi_accrual, :sample, :observed]  (interval_ms: ~1000)

t=1s    no new heartbeat. φ ≈ 0.3 (still on-schedule).
        → [:phi_accrual, :phi, :computed]  (periodic gauge tick)

t=2s    φ ≈ 3.5. starting to get suspicious.
        → [:phi_accrual, :phi, :computed]

t=3s    φ crosses 4.0.
        → [:phi_accrual, :phi, :computed]
        → [:phi_accrual, :threshold, :suspected]

t=10s   φ very high. state still :steady (stale_after_ms default 60 s).
        → [:phi_accrual, :phi, :computed]

t=60s   elapsed > stale_after_ms.
        → [:phi_accrual, :phi, :computed]  (state: :stale)
```

If `:node_a@host` comes back at t=15s and resumes heartbeating, the
first-arrival interval of 15 000 ms exceeds `recovering_threshold_ms`
(default 10 000). The state transitions to `:recovering` for the next
3 samples while the EWMA absorbs the outlier. Once φ drops below 3.0:

```
t=15s   first heartbeat after outage. interval = 15 000 ms.
        → [:phi_accrual, :sample, :observed]
        state becomes :recovering.

t=16s   next heartbeat. φ has fallen sharply (elapsed is small).
        → [:phi_accrual, :phi, :computed]  (state: :recovering)
        → [:phi_accrual, :threshold, :recovered]    (φ crossed 3.0 downward)

t=19s   three samples since the outlier.
        → state returns to :steady.
```

**Nowhere in this flow does the library decide the node is "down."**
It just publishes φ and state labels; the `Threshold` module (or your
own consumer) decides what to do. That separation is why the detector
can be wired to a dashboard, an alert, and an automated-routing policy
simultaneously with different thresholds.

## Telemetry schema (v1.x stable)

Event names, measurement keys, and metadata keys are a contract.
**Breaking changes only in v2.**

```
[:phi_accrual, :sample, :observed]
  measurements: %{interval_ms}
  metadata:     %{node, local_pause?}

[:phi_accrual, :phi, :computed]                  # periodic gauge stream
  measurements: %{phi, elapsed_ms}
  metadata:     %{node, state, local_pause?, confidence}
    # state ∈ [:steady, :recovering, :insufficient_data, :stale]
    # phi is 0.0 when state is :insufficient_data or :stale; consumers
    # should filter on state if they want to graph only meaningful values.

[:phi_accrual, :local_pause, :start]             # rising edge
  metadata:     %{kind}                          # :long_gc | :long_schedule | :busy_dist_port
[:phi_accrual, :local_pause, :stop]              # falling edge

[:phi_accrual, :overload, :shed]
  measurements: %{mailbox_len}
  metadata:     %{node}

[:phi_accrual, :source, :started]
  metadata:     %{source, interval_ms}

[:phi_accrual, :threshold, :suspected]           # emitted by Threshold module
[:phi_accrual, :threshold, :recovered]
  measurements: %{phi}
  metadata:     %{node, instance, threshold, confidence, detector_state}
```

Pipe these to Prometheus via `telemetry_metrics_prometheus`, to logs,
or to your own alerting (see next section).

## Wiring telemetry to Prometheus

Pull in [`telemetry_metrics_prometheus`](https://hex.pm/packages/telemetry_metrics_prometheus)
(or your preferred `telemetry_metrics` reporter) and declare the
metrics you care about:

```elixir
# mix.exs — add dependency
{:telemetry_metrics_prometheus, "~> 1.1"}

# In your supervision tree
children = [
  {TelemetryMetricsPrometheus,
   metrics: [
     # φ as a gauge — one series per (node, state) pair.
     Telemetry.Metrics.last_value(
       "phi_accrual.phi.computed.phi",
       event_name: [:phi_accrual, :phi, :computed],
       measurement: :phi,
       tags: [:node, :state, :confidence]
     ),

     # Counter of every heartbeat observed.
     Telemetry.Metrics.counter(
       "phi_accrual.sample.observed.count",
       event_name: [:phi_accrual, :sample, :observed],
       tags: [:node]
     ),

     # Local-pause events — correlate noise in φ with GC / HoL.
     Telemetry.Metrics.counter(
       "phi_accrual.local_pause.start.count",
       event_name: [:phi_accrual, :local_pause, :start],
       tags: [:kind]
     ),

     # Overload shedding — if this is ever non-zero in steady state,
     # tune α instead of raising :shed_threshold.
     Telemetry.Metrics.counter(
       "phi_accrual.overload.shed.count",
       event_name: [:phi_accrual, :overload, :shed],
       tags: [:node]
     ),

     # Discrete alert events from the Threshold module.
     Telemetry.Metrics.counter(
       "phi_accrual.threshold.suspected.count",
       event_name: [:phi_accrual, :threshold, :suspected],
       tags: [:node, :instance]
     ),
     Telemetry.Metrics.counter(
       "phi_accrual.threshold.recovered.count",
       event_name: [:phi_accrual, :threshold, :recovered],
       tags: [:node, :instance]
     )
   ]}
]
```

For ad-hoc logging, attach a handler directly:

```elixir
:telemetry.attach_many(
  "phi-accrual-logger",
  [
    [:phi_accrual, :threshold, :suspected],
    [:phi_accrual, :threshold, :recovered]
  ],
  &MyApp.PhiLogger.handle/4,
  nil
)

defmodule MyApp.PhiLogger do
  require Logger

  def handle([:phi_accrual, :threshold, kind], %{phi: phi}, %{node: node}, _) do
    Logger.warning("node=#{node} #{kind} phi=#{Float.round(phi, 2)}")
  end
end
```

## Thresholding (optional)

`PhiAccrual.Threshold` converts the φ gauge stream into discrete
`:suspected` / `:recovered` events with hysteresis:

```elixir
# In your supervision tree
children = [
  {PhiAccrual.Threshold, name: :dash, suspect_at: 4.0, recover_at: 3.0},
  {PhiAccrual.Threshold, name: :route, suspect_at: 8.0, recover_at: 7.0}
]
```

Multiple instances coexist — one for dashboards at φ=4, another for
automated routing at φ=8. Skip the module entirely if you want to roll
your own.

## Configuration

```elixir
# config/runtime.exs
config :phi_accrual,
  # enable the node-global :erlang.system_monitor hook (default: true).
  # Disable if another library already subscribes.
  pause_monitor: true,

  # back-pressure threshold — observe/2 sheds samples when mailbox
  # exceeds this count and emits [:overload, :shed] telemetry.
  shed_threshold: 10_000,

  # bundled reference source — off by default, opt in:
  distribution_ping: [interval_ms: 1_000, auto_track: true]
```

Per-node estimator options (passed to `PhiAccrual.track/2`):

| Option                       | Default  | Notes                                         |
| ---------------------------- | -------- | --------------------------------------------- |
| `:alpha_mean`                | `0.125`  | EWMA smoothing for mean                       |
| `:alpha_var`                 | `0.125`  | EWMA smoothing for variance (tune lower)      |
| `:min_std_dev_ms`            | `50.0`   | Floor on σ — prevents singular distribution   |
| `:min_samples`               | `8`      | Bootstrap gate before φ is reported           |
| `:stale_after_ms`            | `60_000` | Elapsed past which state becomes `:stale`     |
| `:recovering_threshold_ms`   | `10_000` | Large-gap detection for `:recovering` tag     |
| `:recovering_grace_samples`  | `3`      | Samples the `:recovering` tag persists for    |
| `:initial_interval_ms`       | `1_000`  | Prior mean before any observation             |
| `:initial_std_dev_ms`        | `500`    | Prior σ (variance = σ²)                       |

## Limitations

Read these before wiring φ to anything that takes irreversible action.

**Head-of-line blocking (primary v1 caveat).** `DistributionPing` and
any source that travels over BEAM distribution shares a TCP socket
with user traffic. A large GenServer reply or `:pg` broadcast can
delay heartbeats for arbitrary periods. `PauseMonitor` subscribes to
`:busy_dist_port` so you can *observe* this (pause telemetry +
`confidence: false` on φ events), but the underlying problem cannot be
fixed by this library while the source is distribution-based. The v2
`UdpSource` solves it by using a dedicated socket.

**Local-pause suppression is best-effort.** `:erlang.system_monitor`
fires on `:long_gc`, `:long_schedule`, and `:busy_dist_port`. The
monitor marks φ output with `local_pause?: true` and
`confidence: false` for a short lockout window after any event. It
does **not** freeze φ or widen the variance — we decided the silent-
detector failure mode is worse than noisy φ. Consumers are expected to
filter on the confidence flag (the `Threshold` module passes it
through in metadata).

**Gaussian assumption misbehaves under bimodal distributions.** BEAM
GC produces intermittent large pauses that, combined with normal
intervals, yield a bimodal inter-arrival distribution. A Gaussian EWMA
is a poor fit and will over-alert. Correlate φ with
`:erlang.statistics(:garbage_collection)` before acting on high φ. A
non-parametric estimator (Satzger or a two-component mixture) is a v2
consideration once we have real traces from deployments.

**One `:erlang.system_monitor` per node.** Only one subscription can
exist. If another library installs its own, enabling both will cause
one to silently win. Disable `pause_monitor` in config and feed pause
state to `PhiAccrual.PauseMonitor.put_state/1` yourself if you need
coexistence.

## Testing strategy

Failure detectors are hard to test against wall-clock. This project:

* Uses [`StreamData`](https://hex.pm/packages/stream_data) for
  property-based tests of estimator math (`test/phi_accrual/core_test.exs`).
* Injects clocks into `PhiAccrual.Estimator` via the `:clock_fn`
  option — no `Process.sleep` in unit tests.
* Integration tests against live distribution (`:peer`-based,
  multi-node) are planned for v2 alongside the `UdpSource` work.

## Versioning

v1.x is **telemetry-schema-stable**: event names, measurement keys,
and metadata keys will not change until v2. Per-node option defaults
may be tuned within v1.x.

## Roadmap

### v1 (shipped)

- Dual-α EWMA estimator with bootstrap / stale / recovering states
- `PauseMonitor` with `:busy_dist_port` tracking
- Per-node estimator GenServer + `DynamicSupervisor` + `Registry`
- Overload shedding with telemetry
- Bring-your-own-signal API + `DistributionPing` reference source
- Optional `Threshold` module with hysteresis
- Committed telemetry event schema

### v2 (planned)

- `UdpSource` — dedicated UDP socket for heartbeats, escapes HoL,
  makes the detector decision-grade
- Evidence-based evaluation of non-parametric / mixture estimators
- `:peer`-based multi-node integration tests
- Optional `phi_accrual_libcluster` companion package

### Related ideas

This library is the first of three composable primitives:
φ-accrual → HLC + causal broadcast → SWIM-Lifeguard standalone.

## License

Apache-2.0. See LICENSE.
