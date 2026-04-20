defmodule PhiAccrual.Clock do
  @moduledoc """
  Clock discipline for the φ-accrual detector.

  The detector reasons ONLY about local arrival times — every timestamp
  passed into `PhiAccrual.observe/2` MUST come from the same monotonic
  clock running on the node hosting the detector. Cross-node timestamps
  are meaningless here and must never appear in interval calculations.

  `now/0` returns integer milliseconds from `:erlang.monotonic_time/1`.
  Estimators accept an alternate `clock_fn` at startup for deterministic
  testing — production code should not call alternate clocks.
  """

  @type t :: integer()

  @spec now() :: t()
  def now, do: :erlang.monotonic_time(:millisecond)
end
