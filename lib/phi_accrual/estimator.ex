defmodule PhiAccrual.Estimator do
  @moduledoc """
  Per-node estimator GenServer.

  Each monitored node has its own estimator process, registered in
  `PhiAccrual.Registry` under the node atom and supervised under
  `PhiAccrual.EstimatorSupervisor`. Isolating per-node state means
  one slow node's scheduling hiccups cannot block observations for
  other nodes — a single shared GenServer would serialise every
  observation behind one mailbox.

  On a configurable tick (`:phi_tick_ms`, default 1_000) the estimator
  emits a `[:phi_accrual, :phi, :computed]` telemetry event carrying
  the current φ and state classification. Set `phi_tick_ms: nil` to
  disable the gauge stream; consumers can still poll `phi/1` directly.

  `phi` is `0.0` when state is `:insufficient_data` or `:stale`;
  consumers should filter on `state` if they want to graph only
  meaningful values.
  """

  use GenServer

  alias PhiAccrual.{Clock, Core, PauseMonitor}

  @default_phi_tick_ms 1_000

  @type start_opts :: [
          node: node(),
          core_opts: keyword(),
          clock_fn: (-> integer()),
          phi_tick_ms: pos_integer() | nil
        ]

  @doc false
  @spec child_spec(start_opts()) :: Supervisor.child_spec()
  def child_spec(opts) do
    node = Keyword.fetch!(opts, :node)

    %{
      id: {__MODULE__, node},
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient,
      shutdown: 5_000,
      type: :worker
    }
  end

  @doc """
  Start an estimator process for a single node. Normally called via
  `PhiAccrual.track/2`; use directly only when you need to inject a custom
  `:clock_fn` (e.g., in tests).
  """
  @spec start_link(start_opts()) :: GenServer.on_start()
  def start_link(opts) do
    node = Keyword.fetch!(opts, :node)
    GenServer.start_link(__MODULE__, opts, name: via(node))
  end

  @doc false
  @spec via(node()) :: {:via, Registry, {PhiAccrual.Registry, node()}}
  def via(node), do: {:via, Registry, {PhiAccrual.Registry, node}}

  @doc """
  Return the pid of the estimator tracking `node`, or `nil` if not tracked.
  """
  @spec whereis(node()) :: pid() | nil
  def whereis(node) do
    case Registry.lookup(PhiAccrual.Registry, node) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  @doc """
  Lower-level φ query. Most callers should use `PhiAccrual.phi/1` instead,
  which forwards here via the Registry.
  """
  @spec phi(node()) :: Core.phi_result() | {:error, :not_tracked}
  def phi(node) do
    case whereis(node) do
      nil -> {:error, :not_tracked}
      pid -> GenServer.call(pid, :phi)
    end
  end

  @doc """
  Return the current `PhiAccrual.Core` estimator state for `node`. Intended
  for debugging and introspection — not for hot-path use.
  """
  @spec core_state(node()) :: Core.t() | {:error, :not_tracked}
  def core_state(node) do
    case whereis(node) do
      nil -> {:error, :not_tracked}
      pid -> GenServer.call(pid, :core_state)
    end
  end

  @impl true
  def init(opts) do
    node = Keyword.fetch!(opts, :node)
    core_opts = Keyword.get(opts, :core_opts, [])
    clock_fn = Keyword.get(opts, :clock_fn, &Clock.now/0)
    phi_tick_ms = Keyword.get(opts, :phi_tick_ms, @default_phi_tick_ms)

    state = %{
      node: node,
      core: Core.new(core_opts),
      clock_fn: clock_fn,
      phi_tick_ms: phi_tick_ms
    }

    schedule_tick(phi_tick_ms)
    {:ok, state}
  end

  @impl true
  def handle_cast({:observe, ts}, state) do
    prev = state.core.last_arrival_ts
    core = Core.observe(state.core, ts)

    if prev do
      interval = (ts - prev) * 1.0

      :telemetry.execute(
        [:phi_accrual, :sample, :observed],
        %{interval_ms: interval},
        %{node: state.node, local_pause?: PauseMonitor.paused?()}
      )
    end

    {:noreply, %{state | core: core}}
  end

  @impl true
  def handle_call(:phi, _from, state) do
    {:reply, Core.phi(state.core, state.clock_fn.()), state}
  end

  def handle_call(:core_state, _from, state) do
    {:reply, state.core, state}
  end

  @impl true
  def handle_info(:tick, state) do
    emit_phi(state)
    schedule_tick(state.phi_tick_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp schedule_tick(nil), do: :ok

  defp schedule_tick(ms) when is_integer(ms) and ms > 0 do
    Process.send_after(self(), :tick, ms)
  end

  defp emit_phi(%{core: %Core{last_arrival_ts: nil}}), do: :ok

  defp emit_phi(state) do
    now = state.clock_fn.()
    paused? = PauseMonitor.paused?()
    elapsed = now - state.core.last_arrival_ts

    {phi_value, status} =
      case Core.phi(state.core, now) do
        {:ok, phi, s} -> {phi, s}
        {:insufficient_data, _} -> {0.0, :insufficient_data}
        {:stale, _} -> {0.0, :stale}
      end

    :telemetry.execute(
      [:phi_accrual, :phi, :computed],
      %{phi: phi_value, elapsed_ms: elapsed},
      %{node: state.node, state: status, local_pause?: paused?, confidence: not paused?}
    )
  end
end
