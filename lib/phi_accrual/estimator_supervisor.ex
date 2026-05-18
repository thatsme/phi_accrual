defmodule PhiAccrual.EstimatorSupervisor do
  @moduledoc """
  DynamicSupervisor owning one `PhiAccrual.Estimator` per tracked node.

  Estimator processes are restarted `:transient` — a crash inside an
  estimator restarts only that node's estimator, leaving other nodes'
  state intact. This supervisor is started from the top-level
  `phi_accrual` application supervisor and is intentionally kept **separate** from
  the source-adapter supervisor so that restarting a source (e.g.,
  `PhiAccrual.Source.DistributionPing`) does not wipe estimator state.
  """

  use DynamicSupervisor

  alias PhiAccrual.Estimator

  @doc false
  @spec start_link(term()) :: Supervisor.on_start()
  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Start tracking `node`. Returns the existing pid if already tracked.
  """
  @spec track(PhiAccrual.detector_key(), keyword()) :: {:ok, pid()} | {:error, term()}
  def track(node, core_opts \\ []) do
    spec = {Estimator, [node: node, core_opts: core_opts]}

    case DynamicSupervisor.start_child(__MODULE__, spec) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  @doc """
  Stop tracking `node`. No-op if the node is not tracked.
  """
  @spec untrack(PhiAccrual.detector_key()) :: :ok
  def untrack(node) do
    case Estimator.whereis(node) do
      nil -> :ok
      pid -> DynamicSupervisor.terminate_child(__MODULE__, pid)
    end
  end

  @doc "List every node currently tracked by an estimator."
  @spec tracked_nodes() :: [PhiAccrual.detector_key()]
  def tracked_nodes do
    Registry.select(PhiAccrual.Registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  end
end
