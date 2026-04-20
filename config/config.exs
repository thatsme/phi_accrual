import Config

if config_env() == :test do
  # Disable node-global :erlang.system_monitor subscription during tests —
  # it is one-per-node and would interact badly with async test runs.
  # PauseMonitor is exercised explicitly in its own test.
  config :phi_accrual, pause_monitor: false
end
