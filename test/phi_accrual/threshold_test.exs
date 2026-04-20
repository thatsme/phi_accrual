defmodule PhiAccrual.ThresholdTest do
  use ExUnit.Case, async: false

  alias PhiAccrual.Threshold

  setup do
    name = :"threshold_#{System.unique_integer([:positive])}"
    start_supervised!({Threshold, name: name, suspect_at: 8.0, recover_at: 6.0})
    %{name: name}
  end

  defp emit_phi(node, phi, state \\ :steady) do
    :telemetry.execute(
      [:phi_accrual, :phi, :computed],
      %{phi: phi, elapsed_ms: 0},
      %{node: node, state: state, local_pause?: false, confidence: true}
    )
  end

  defp subscribe(events) do
    ref = make_ref()
    id = "test-#{inspect(ref)}"

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

  test "emits :suspected when φ crosses suspect_at from below" do
    ref =
      subscribe([
        [:phi_accrual, :threshold, :suspected],
        [:phi_accrual, :threshold, :recovered]
      ])

    emit_phi(:n1, 2.0)
    refute_receive {:event, ^ref, _, _, _}, 50

    emit_phi(:n1, 9.0)

    assert_receive {:event, ^ref, [:phi_accrual, :threshold, :suspected], %{phi: 9.0},
                    %{node: :n1}},
                   500
  end

  test "emits :recovered only when φ drops below recover_at" do
    ref =
      subscribe([
        [:phi_accrual, :threshold, :suspected],
        [:phi_accrual, :threshold, :recovered]
      ])

    emit_phi(:n2, 9.0)
    assert_receive {:event, ^ref, [:phi_accrual, :threshold, :suspected], _, _}, 500

    # Inside hysteresis band: still suspected, no recovery event.
    emit_phi(:n2, 7.0)
    refute_receive {:event, ^ref, [:phi_accrual, :threshold, :recovered], _, _}, 50

    emit_phi(:n2, 5.0)
    assert_receive {:event, ^ref, [:phi_accrual, :threshold, :recovered], _, _}, 500
  end

  test "hysteresis band prevents flapping" do
    ref =
      subscribe([
        [:phi_accrual, :threshold, :suspected],
        [:phi_accrual, :threshold, :recovered]
      ])

    emit_phi(:n3, 8.5)
    assert_receive {:event, ^ref, [:phi_accrual, :threshold, :suspected], _, _}, 500

    emit_phi(:n3, 7.0)
    emit_phi(:n3, 8.5)
    emit_phi(:n3, 7.0)
    refute_receive {:event, ^ref, _, _, _}, 100
  end

  test "ignores :insufficient_data events even with large φ" do
    ref =
      subscribe([
        [:phi_accrual, :threshold, :suspected],
        [:phi_accrual, :threshold, :recovered]
      ])

    emit_phi(:n4, 100.0, :insufficient_data)
    refute_receive {:event, ^ref, _, _, _}, 50
  end

  test "carries instance tag and threshold value in metadata" do
    ref = subscribe([[:phi_accrual, :threshold, :suspected]])

    emit_phi(:n5, 10.0)

    assert_receive {:event, ^ref, _, _, %{instance: instance, threshold: 8.0, node: :n5}}, 500
    assert is_atom(instance)
  end

  test "rejects recover_at >= suspect_at" do
    Process.flag(:trap_exit, true)

    assert {:error, {%ArgumentError{}, _}} =
             Threshold.start_link(
               name: :"bad_#{System.unique_integer([:positive])}",
               suspect_at: 5.0,
               recover_at: 5.0
             )
  end
end
