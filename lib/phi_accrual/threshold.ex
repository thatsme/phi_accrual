defmodule PhiAccrual.Threshold do
  @moduledoc """
  Optional threshold layer.

  The core detector emits φ values; this module turns them into
  `:suspected` / `:recovered` events with hysteresis. Thresholding is
  policy, not detection — the library keeps it out of the core so that
  consumers can:

    * run multiple thresholds concurrently (e.g. φ=4 for dashboards,
      φ=8 for automated routing), each a separate instance,
    * skip this module entirely and roll their own hysteresis,
    * filter on confidence, `:recovering` state, etc. without touching
      the detector.

  ## Events

  Subscribes to `[:phi_accrual, :phi, :computed]`. Emits:

    * `[:phi_accrual, :threshold, :suspected]` when φ crosses
      `suspect_at` from below,
    * `[:phi_accrual, :threshold, :recovered]` when φ crosses
      `recover_at` from above.

  Both events include `%{node: node(), instance: name, threshold: ...}`
  in metadata. The `instance` tag lets you disambiguate multiple
  threshold modules sharing the same event namespace.

  ## Hysteresis

  `recover_at` must be strictly less than `suspect_at`. Default band:
  suspect at 8.0, recover at 7.0. Widen the band if you see flapping
  around the boundary.

  φ events for nodes in `:insufficient_data` state are ignored — φ is
  not meaningful there. `:steady`, `:recovering`, and `:stale` states
  all feed the threshold logic.
  """

  use GenServer
  require Logger

  @default_suspect_at 8.0
  @default_recover_at 7.0

  @type opts :: [
          name: GenServer.name(),
          suspect_at: float(),
          recover_at: float()
        ]

  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    suspect_at = Keyword.get(opts, :suspect_at, @default_suspect_at) * 1.0
    recover_at = Keyword.get(opts, :recover_at, @default_recover_at) * 1.0

    if recover_at >= suspect_at do
      raise ArgumentError,
            "recover_at (#{recover_at}) must be strictly less than suspect_at (#{suspect_at}); " <>
              "a hysteresis band of 0 produces flapping"
    end

    instance_name = Keyword.get(opts, :name, __MODULE__)
    handler_id = {__MODULE__, instance_name, self()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:phi_accrual, :phi, :computed],
        &__MODULE__.__handle_event__/4,
        self()
      )

    state = %{
      handler_id: handler_id,
      instance: instance_name,
      suspect_at: suspect_at,
      recover_at: recover_at,
      node_states: %{}
    }

    {:ok, state}
  end

  @impl true
  def terminate(_reason, state) do
    _ = :telemetry.detach(state.handler_id)
    :ok
  end

  @doc false
  def __handle_event__(_event, %{phi: phi}, %{node: node, state: node_state} = meta, pid) do
    if node_state != :insufficient_data do
      GenServer.cast(pid, {:phi, node, phi, meta})
    end

    :ok
  end

  @impl true
  def handle_cast({:phi, node, phi, meta}, state) do
    current = Map.get(state.node_states, node, :below)

    new_value =
      cond do
        current == :below and phi >= state.suspect_at ->
          emit(:suspected, node, phi, state, meta)
          :above

        current == :above and phi <= state.recover_at ->
          emit(:recovered, node, phi, state, meta)
          :below

        true ->
          current
      end

    {:noreply, %{state | node_states: Map.put(state.node_states, node, new_value)}}
  end

  defp emit(kind, node, phi, state, meta) do
    threshold =
      case kind do
        :suspected -> state.suspect_at
        :recovered -> state.recover_at
      end

    :telemetry.execute(
      [:phi_accrual, :threshold, kind],
      %{phi: phi},
      %{
        node: node,
        instance: state.instance,
        threshold: threshold,
        confidence: Map.get(meta, :confidence, true),
        detector_state: Map.get(meta, :state)
      }
    )
  end
end
