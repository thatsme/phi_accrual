defmodule PhiAccrual.Source.DistributionPing do
  @moduledoc """
  Reference heartbeat source — sends an app-layer ping to every
  currently-connected node at a fixed interval.

  Pings travel over BEAM distribution, so this source **inherits HoL
  blocking**: if the distribution TCP send buffer to a peer is full
  (see `:busy_dist_port` in `PhiAccrual.PauseMonitor`), pings queue
  behind user traffic. That is why v1 is observability-grade rather
  than decision-grade. A future `UdpSource` escapes HoL by using its
  own socket.

  ## Cost

  O(N²) cluster-wide at steady state: each node pings every peer. A
  50-node cluster at the default 1 s interval is 2 500 pings/second of
  distribution traffic — cheap per-ping, not free in aggregate.

  ## Opting in

  Not started by default. Enable via application config:

      config :phi_accrual,
        distribution_ping: [interval_ms: 1_000, auto_track: true]

  This source is supervised **separately** from the per-node estimator
  supervisor: a restart here does not wipe estimator state.
  """

  @behaviour PhiAccrual.Source

  use GenServer

  @default_interval_ms 1_000

  @impl PhiAccrual.Source
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  @spec receive_ping(node()) :: :ok
  def receive_ping(from_node) do
    PhiAccrual.observe(from_node)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, @default_interval_ms)
    auto_track = Keyword.get(opts, :auto_track, true)

    :telemetry.execute(
      [:phi_accrual, :source, :started],
      %{},
      %{source: :distribution_ping, interval_ms: interval}
    )

    schedule(interval)
    {:ok, %{interval_ms: interval, auto_track: auto_track}}
  end

  @impl true
  def handle_info(:tick, state) do
    for node <- Node.list() do
      if state.auto_track, do: _ = PhiAccrual.track(node)
      :rpc.cast(node, __MODULE__, :receive_ping, [Node.self()])
    end

    schedule(state.interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp schedule(ms), do: Process.send_after(self(), :tick, ms)
end
