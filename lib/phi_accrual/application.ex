defmodule PhiAccrual.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      [{Registry, keys: :unique, name: PhiAccrual.Registry}]
      |> maybe_add_pause_monitor()
      |> add_estimator_supervisor()
      |> maybe_add_distribution_ping()

    opts = [strategy: :one_for_one, name: PhiAccrual.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp maybe_add_pause_monitor(children) do
    if Application.get_env(:phi_accrual, :pause_monitor, true) do
      children ++ [PhiAccrual.PauseMonitor]
    else
      children
    end
  end

  defp add_estimator_supervisor(children) do
    children ++ [PhiAccrual.EstimatorSupervisor]
  end

  defp maybe_add_distribution_ping(children) do
    case Application.get_env(:phi_accrual, :distribution_ping) do
      nil -> children
      false -> children
      true -> children ++ [{PhiAccrual.Source.DistributionPing, []}]
      opts when is_list(opts) -> children ++ [{PhiAccrual.Source.DistributionPing, opts}]
    end
  end
end
