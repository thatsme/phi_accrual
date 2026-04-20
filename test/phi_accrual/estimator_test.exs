defmodule PhiAccrual.EstimatorTest do
  use ExUnit.Case, async: false

  alias PhiAccrual.Estimator

  setup do
    clock = start_supervised!({Agent, fn -> 0 end})
    clock_fn = fn -> Agent.get(clock, & &1) end

    node = :"test_#{System.unique_integer([:positive])}@nohost"

    {:ok, _pid} =
      start_supervised({Estimator,
        node: node,
        core_opts: [
          min_samples: 3,
          initial_interval_ms: 1_000,
          initial_std_dev_ms: 200,
          min_std_dev_ms: 200,
          alpha_mean: 0.2,
          alpha_var: 0.1
        ],
        clock_fn: clock_fn,
        phi_tick_ms: nil
      })

    %{node: node, clock: clock}
  end

  defp set_time(clock, t), do: Agent.update(clock, fn _ -> t end)
  defp cast_observe(node, ts), do: GenServer.cast(Estimator.via(node), {:observe, ts})

  defp wait_observation(node, ts) do
    cast_observe(node, ts)
    # flush the cast by making a sync call
    _ = Estimator.phi(node)
  end

  test "phi before any observation is :insufficient_data", %{node: node} do
    assert {:insufficient_data, 3} = Estimator.phi(node)
  end

  test "phi reports :insufficient_data until min_samples intervals seen", %{
    node: node,
    clock: clock
  } do
    wait_observation(node, 0)
    set_time(clock, 500)
    assert {:insufficient_data, 3} = Estimator.phi(node)

    wait_observation(node, 1_000)
    set_time(clock, 1_500)
    assert {:insufficient_data, 2} = Estimator.phi(node)

    wait_observation(node, 2_000)
    set_time(clock, 2_500)
    assert {:insufficient_data, 1} = Estimator.phi(node)

    wait_observation(node, 3_000)
    set_time(clock, 3_500)
    assert {:ok, _phi, :steady} = Estimator.phi(node)
  end

  test "phi returns :stale after long silence", %{node: node, clock: clock} do
    for t <- 0..5, do: wait_observation(node, t * 1_000)

    set_time(clock, 5_000 + 70_000)
    assert {:stale, _} = Estimator.phi(node)
  end

  test "emits :sample :observed telemetry with interval_ms", %{node: node} do
    ref = attach_handler([:phi_accrual, :sample, :observed])

    cast_observe(node, 0)
    # first observe has no interval — no event
    refute_receive {:event, ^ref, _, _}, 50

    cast_observe(node, 1_000)
    assert_receive {:event, ^ref, %{interval_ms: 1_000.0}, %{node: ^node}}, 500
  after
    :telemetry.detach(make_handler_id())
  end

  test "emits :phi :computed on tick when phi_tick_ms is set", %{clock: clock} do
    clock_fn = fn -> Agent.get(clock, & &1) end

    node = :"tick_#{System.unique_integer([:positive])}@nohost"

    {:ok, _pid} =
      start_supervised(
        {Estimator,
         node: node,
         core_opts: [
           min_samples: 2,
           initial_interval_ms: 1_000,
           initial_std_dev_ms: 200,
           min_std_dev_ms: 200
         ],
         clock_fn: clock_fn,
         phi_tick_ms: 30},
        id: {Estimator, node}
      )

    ref = attach_handler([:phi_accrual, :phi, :computed])

    cast_observe(node, 0)
    cast_observe(node, 1_000)
    cast_observe(node, 2_000)
    set_time(clock, 3_000)

    assert_receive {:event, ^ref, %{phi: _, elapsed_ms: _}, %{node: ^node, state: :steady}}, 500
  end

  defp attach_handler(event) do
    ref = make_ref()
    id = make_handler_id()
    Process.put(:__handler_id__, id)

    :ok =
      :telemetry.attach(
        id,
        event,
        fn _, m, md, {pid, r} -> send(pid, {:event, r, m, md}) end,
        {self(), ref}
      )

    on_exit_detach(id)
    ref
  end

  defp make_handler_id, do: "estimator-test-#{System.unique_integer([:positive])}"

  defp on_exit_detach(id) do
    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
  end
end
