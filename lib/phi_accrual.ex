defmodule PhiAccrual do
  @moduledoc """
  φ-accrual failure detector — public API.

  The detector emits a continuous suspicion value φ per monitored node.
  φ grows monotonically with time since the last heartbeat and is
  calibrated against observed inter-arrival statistics (EWMA mean and
  variance). See `PhiAccrual.Core` for the math and `README.md` for
  the positioning (observability-grade, not decision-grade in v1).

  ## Typical usage (bring-your-own-signal)

      # Track a node with default estimator settings.
      PhiAccrual.track(:node_a@host)

      # Call observe/2 from wherever you already receive cross-node
      # traffic — GenServer replies, :pg broadcasts, your own pings.
      PhiAccrual.observe(:node_a@host)

      # Query φ at any time.
      PhiAccrual.phi(:node_a@host)
      #=> {:ok, 1.82, :steady}

  `observe/1` auto-tracks unknown nodes with default settings — call
  `track/2` first if you want custom per-node estimator options.

  The monitored key is an Erlang `node()` when fed by the bundled
  `DistributionPing` source, but the detector treats the key opaquely
  and accepts any term — see `t:detector_key/0` for the contract used
  by non-distribution transport companion packages.

  ## Overload behaviour

  `observe/2` is bounded: if the target estimator's mailbox exceeds
  `:shed_threshold` (default 10_000) the sample is **dropped** and a
  `[:phi_accrual, :overload, :shed]` telemetry event is emitted. For
  failure detection you don't need every heartbeat, you need enough
  heartbeats — if you're shedding, your heartbeat rate is too high for
  your EWMA α settings. Tune α before raising the threshold.
  """

  alias PhiAccrual.{Clock, Core, Estimator, EstimatorSupervisor}

  @default_shed_threshold 10_000

  @typedoc """
  Identity of a monitored entity.

  Historically this was always an Erlang `node()`, and for the bundled
  `DistributionPing` source it still is. Transport companion packages
  (`phi_accrual_udp`, `phi_accrual_amqp`, ...) monitor entities that are
  not BEAM nodes — a UDP peer, an AMQP routing key — so the key may be
  any term: a string, a tuple, an atom.

  The detector treats the key opaquely: it is a `Registry` key and a
  telemetry metadata value, nothing more. Two rules apply:

    * the key must be a valid `Registry` key (any term is — no constraint
      in practice), and
    * the same term must be used consistently for `track/2`, `observe/2`,
      `phi/1`, and `untrack/1` for a given entity.
  """
  @type detector_key :: node() | term()

  @type phi_result :: Core.phi_result()

  @doc """
  Start tracking `node` with optional estimator configuration.

  `core_opts` are forwarded to `PhiAccrual.Core.new/1`.
  """
  @spec track(detector_key(), keyword()) :: {:ok, pid()} | {:error, term()}
  def track(node, core_opts \\ []), do: EstimatorSupervisor.track(node, core_opts)

  @doc "Stop tracking `node`. No-op if the node is not tracked."
  @spec untrack(detector_key()) :: :ok
  def untrack(node), do: EstimatorSupervisor.untrack(node)

  @doc "List all currently-tracked nodes."
  @spec tracked_nodes() :: [detector_key()]
  def tracked_nodes, do: EstimatorSupervisor.tracked_nodes()

  @doc """
  Record a heartbeat arrival for `node` at the current monotonic time.
  Auto-tracks the node with default options if not already tracked.
  """
  @spec observe(detector_key()) :: :ok
  def observe(node), do: observe(node, Clock.now())

  @doc """
  Record a heartbeat arrival for `node` at monotonic-ms `ts`.

  **`ts` must come from the same local monotonic clock as `Clock.now/0`.**
  Cross-node timestamps are meaningless for the detector and must never
  enter interval calculations.
  """
  @spec observe(detector_key(), integer()) :: :ok
  def observe(node, ts) when is_integer(ts) do
    case Estimator.whereis(node) do
      nil ->
        {:ok, _pid} = track(node)
        observe(node, ts)

      pid ->
        cast_or_shed(pid, node, ts)
    end
  end

  @doc """
  Current φ for `node`. See `t:PhiAccrual.Core.phi_result/0`.
  """
  @spec phi(detector_key()) :: phi_result() | {:error, :not_tracked}
  def phi(node), do: Estimator.phi(node)

  @doc """
  Return the current `PhiAccrual.Core` estimator state for `node`, or
  `{:error, :not_tracked}` if the node is not tracked.

  Intended for IEx introspection and debugging — inspect `mean`,
  `variance`, `samples_seen`, `last_interval_ms`, and `last_arrival_ts`
  to see what the estimator currently believes about a node. Not for
  hot-path use.
  """
  @spec inspect_state(detector_key()) :: Core.t() | {:error, :not_tracked}
  defdelegate inspect_state(node), to: Estimator, as: :core_state

  defp cast_or_shed(pid, node, ts) do
    threshold = Application.get_env(:phi_accrual, :shed_threshold, @default_shed_threshold)

    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, n} when n >= threshold ->
        :telemetry.execute(
          [:phi_accrual, :overload, :shed],
          %{mailbox_len: n},
          %{node: node}
        )

        :ok

      {:message_queue_len, _} ->
        GenServer.cast(pid, {:observe, ts})

      nil ->
        :ok
    end
  end
end
