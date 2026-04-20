defmodule PhiAccrual.PauseMonitor do
  @moduledoc """
  Owns the node-local `:erlang.system_monitor/2` subscription and tracks
  whether a local-pause event is currently suppressing φ-computation
  confidence.

  Subscribes to three conditions:

    * `{:long_schedule, N}` — a process held a scheduler for > N ms
    * `{:long_gc, N}`       — GC took > N ms
    * `:busy_dist_port`     — the distribution TCP send buffer filled;
      heartbeats are queued behind user traffic. This is the head-of-line
      blocking signal and is the primary reason v1 is observability-grade
      rather than decision-grade.

  When any of these fire, pause state flips to `true` for a lockout
  window (default 500 ms). Each new event extends the window. State is
  published via `:persistent_term` so estimators can read it without
  synchronising on this GenServer.

  Telemetry:

    * `[:phi_accrual, :local_pause, :start]` on rising edge
    * `[:phi_accrual, :local_pause, :stop]`  on falling edge

  **Caveat:** Only one `system_monitor` can be installed per node.
  If your application already uses `:erlang.system_monitor/2`, disable
  this module via `config :phi_accrual, pause_monitor: false` and feed
  pause state in yourself by calling `put_state/1`.
  """

  use GenServer
  require Logger

  @pause_key {__MODULE__, :active?}
  @default_long_schedule_ms 100
  @default_long_gc_ms 50
  @default_lockout_ms 500

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  True if a local pause was detected within the lockout window.

  Cheap lock-free read (`:persistent_term`); safe to call from any process.
  Returns `false` if the monitor has not been started.
  """
  @spec paused?() :: boolean()
  def paused?, do: :persistent_term.get(@pause_key, false)

  @doc """
  Force pause state — for users who have disabled the built-in
  `system_monitor` subscription and want to feed pause state from their
  own instrumentation.
  """
  @spec put_state(boolean()) :: :ok
  def put_state(active?) when is_boolean(active?) do
    :persistent_term.put(@pause_key, active?)
  end

  @impl true
  def init(opts) do
    long_schedule = Keyword.get(opts, :long_schedule_ms, @default_long_schedule_ms)
    long_gc = Keyword.get(opts, :long_gc_ms, @default_long_gc_ms)
    lockout_ms = Keyword.get(opts, :lockout_ms, @default_lockout_ms)

    :persistent_term.put(@pause_key, false)

    previous =
      :erlang.system_monitor(self(), [
        {:long_schedule, long_schedule},
        {:long_gc, long_gc},
        :busy_dist_port
      ])

    if previous != :undefined do
      Logger.warning(
        "PhiAccrual.PauseMonitor replaced a pre-existing :erlang.system_monitor. " <>
          "If another library relies on it, disable this monitor via " <>
          "`config :phi_accrual, pause_monitor: false`."
      )
    end

    {:ok, %{active?: false, lockout_ref: nil, lockout_ms: lockout_ms}}
  end

  @impl true
  def handle_info({:monitor, _pid_or_port, kind, _info}, state)
      when kind in [:long_schedule, :long_gc, :busy_dist_port] do
    {:noreply, note_pause(state, kind)}
  end

  def handle_info(:lockout_expired, state) do
    :persistent_term.put(@pause_key, false)
    :telemetry.execute([:phi_accrual, :local_pause, :stop], %{}, %{})
    {:noreply, %{state | active?: false, lockout_ref: nil}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, _state) do
    _ = :erlang.system_monitor(:undefined)
    :persistent_term.put(@pause_key, false)
    :ok
  end

  defp note_pause(%{active?: false} = state, kind) do
    :persistent_term.put(@pause_key, true)
    :telemetry.execute([:phi_accrual, :local_pause, :start], %{}, %{kind: kind})
    ref = Process.send_after(self(), :lockout_expired, state.lockout_ms)
    %{state | active?: true, lockout_ref: ref}
  end

  defp note_pause(%{active?: true, lockout_ref: ref} = state, _kind) do
    if is_reference(ref), do: Process.cancel_timer(ref)
    ref = Process.send_after(self(), :lockout_expired, state.lockout_ms)
    %{state | lockout_ref: ref}
  end
end
