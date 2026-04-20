defmodule PhiAccrualTest do
  use ExUnit.Case, async: false

  alias PhiAccrual.{Clock, Estimator}

  defp unique_node, do: :"node_#{System.unique_integer([:positive])}@nohost"

  defp subscribe(events) do
    ref = make_ref()
    id = "api-test-#{inspect(ref)}"

    :ok =
      :telemetry.attach_many(
        id,
        events,
        fn ev, m, md, {pid, r} -> send(pid, {:event, r, ev, m, md}) end,
        {self(), ref}
      )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
    ref
  end

  setup do
    # every test owns its nodes and cleans up
    node = unique_node()
    on_exit(fn -> PhiAccrual.untrack(node) end)
    %{node: node}
  end

  test "track/2 starts an estimator, untrack/1 stops it", %{node: node} do
    assert {:ok, pid} = PhiAccrual.track(node)
    assert is_pid(pid)
    assert node in PhiAccrual.tracked_nodes()

    :ok = PhiAccrual.untrack(node)
    # Registry deregistration is async relative to supervisor terminate;
    # give it a tick.
    Process.sleep(20)
    refute node in PhiAccrual.tracked_nodes()
  end

  test "track is idempotent", %{node: node} do
    {:ok, pid1} = PhiAccrual.track(node)
    {:ok, pid2} = PhiAccrual.track(node)
    assert pid1 == pid2
  end

  test "observe auto-tracks unknown nodes", %{node: node} do
    refute node in PhiAccrual.tracked_nodes()
    :ok = PhiAccrual.observe(node)
    assert node in PhiAccrual.tracked_nodes()
  end

  test "phi of untracked node returns :not_tracked error" do
    assert {:error, :not_tracked} = PhiAccrual.phi(:"never_seen@nohost")
  end

  test "observe + phi over time produces a :steady reading", %{node: node} do
    {:ok, _pid} =
      PhiAccrual.track(node,
        min_samples: 3,
        initial_interval_ms: 50,
        initial_std_dev_ms: 20,
        min_std_dev_ms: 20
      )

    for _ <- 1..5 do
      PhiAccrual.observe(node)
      Process.sleep(50)
    end

    # flush — make a sync call to guarantee all casts processed
    _ = Estimator.phi(node)

    assert {:ok, _phi, :steady} = PhiAccrual.phi(node)
  end

  test "overload shedding emits telemetry and drops sample when mailbox is full" do
    node = unique_node()
    {:ok, pid} = PhiAccrual.track(node)

    # suspend the estimator so casts pile up
    :sys.suspend(pid)

    # fill mailbox above a low threshold
    Application.put_env(:phi_accrual, :shed_threshold, 3)
    on_exit(fn -> Application.delete_env(:phi_accrual, :shed_threshold) end)

    for _ <- 1..5, do: GenServer.cast(pid, {:observe, Clock.now()})

    ref = subscribe([[:phi_accrual, :overload, :shed]])

    :ok = PhiAccrual.observe(node)

    assert_receive {:event, ^ref, _, %{mailbox_len: n}, %{node: ^node}}, 500
    assert n >= 3

    :sys.resume(pid)
    PhiAccrual.untrack(node)
  end
end
