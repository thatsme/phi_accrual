defmodule PhiAccrual.Source do
  @moduledoc """
  Behaviour for heartbeat source adapters.

  A *source* is any process that calls `PhiAccrual.observe/2` in
  response to evidence that a remote node is alive. The evidence may be
  an explicit ping, a GenServer reply, a `:pg` broadcast, a `:global`
  sync — anything that arrives from the remote node on the local BEAM.

  v1 ships one reference source: `PhiAccrual.Source.DistributionPing`.
  Applications are free to write their own — the behaviour is minimal
  because sources are ordinary supervised processes that happen to call
  into the detector.

  **Clock discipline.** A source MUST pass timestamps from
  `PhiAccrual.Clock.now/0` (or `:erlang.monotonic_time(:millisecond)`,
  equivalently). Cross-node timestamps are meaningless here and must
  never appear in `observe/2` calls.
  """

  @callback start_link(keyword()) :: GenServer.on_start()
end
