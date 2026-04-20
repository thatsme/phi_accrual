defmodule PhiAccrual.PauseMonitorTest do
  use ExUnit.Case, async: false

  alias PhiAccrual.PauseMonitor

  setup do
    # PauseMonitor is disabled in the test env (see config/config.exs).
    # Start a fresh one under the test supervisor and let on_exit shut it
    # down — terminate/2 removes the node-global :erlang.system_monitor hook.
    pid = start_supervised!({PauseMonitor, lockout_ms: 100})
    %{pid: pid}
  end

  defp subscribe(events) do
    ref = make_ref()
    id = "pause-test-#{inspect(ref)}"

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

  test "paused? is false at rest" do
    refute PauseMonitor.paused?()
  end

  defp sync(pid), do: :sys.get_state(pid)

  test "simulated :long_gc message flips pause state and emits :start", %{pid: pid} do
    ref = subscribe([[:phi_accrual, :local_pause, :start]])

    send(pid, {:monitor, self(), :long_gc, [{:timeout, 120}]})
    sync(pid)

    assert_receive {:event, ^ref, _, _, %{kind: :long_gc}}, 500
    assert PauseMonitor.paused?()
  end

  test ":busy_dist_port message also trips pause state", %{pid: pid} do
    ref = subscribe([[:phi_accrual, :local_pause, :start]])

    send(pid, {:monitor, self(), :busy_dist_port, self()})
    sync(pid)

    assert_receive {:event, ^ref, _, _, %{kind: :busy_dist_port}}, 500
    assert PauseMonitor.paused?()
  end

  test "pause clears after lockout and emits :stop", %{pid: pid} do
    ref = subscribe([[:phi_accrual, :local_pause, :stop]])

    send(pid, {:monitor, self(), :long_schedule, [{:timeout, 150}]})
    sync(pid)
    assert PauseMonitor.paused?()

    assert_receive {:event, ^ref, _, _, _}, 500
    refute PauseMonitor.paused?()
  end

  test "repeated events extend the lockout window", %{pid: pid} do
    send(pid, {:monitor, self(), :long_gc, [{:timeout, 120}]})
    sync(pid)
    assert PauseMonitor.paused?()

    Process.sleep(60)
    send(pid, {:monitor, self(), :long_gc, [{:timeout, 120}]})
    sync(pid)

    Process.sleep(60)
    assert PauseMonitor.paused?()

    Process.sleep(120)
    refute PauseMonitor.paused?()
  end

  test "put_state/1 directly sets the flag" do
    PauseMonitor.put_state(true)
    assert PauseMonitor.paused?()
    PauseMonitor.put_state(false)
    refute PauseMonitor.paused?()
  end
end
