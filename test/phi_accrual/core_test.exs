defmodule PhiAccrual.CoreTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias PhiAccrual.Core

  describe "new/1" do
    test "uses sensible defaults" do
      c = Core.new()
      assert c.mean == 1_000.0
      assert c.variance == 500.0 * 500.0
      assert c.samples_seen == 0
      assert c.last_arrival_ts == nil
      assert c.min_samples == 8
    end

    test "accepts overrides" do
      c = Core.new(min_samples: 4, alpha_mean: 0.2, initial_interval_ms: 2_000)
      assert c.min_samples == 4
      assert c.alpha_mean == 0.2
      assert c.mean == 2_000.0
    end

    test "keeps α_mean and α_var separate" do
      c = Core.new(alpha_mean: 0.2, alpha_var: 0.05)
      assert c.alpha_mean == 0.2
      assert c.alpha_var == 0.05
    end
  end

  describe "observe/2" do
    test "first call seeds last_arrival_ts only" do
      c = Core.new() |> Core.observe(100)
      assert c.last_arrival_ts == 100
      assert c.samples_seen == 0
      assert c.last_interval_ms == nil
    end

    test "second call performs a West 1979 dual-α update" do
      c =
        [
          initial_interval_ms: 1_000,
          initial_std_dev_ms: 100,
          alpha_mean: 0.5,
          alpha_var: 0.5
        ]
        |> Core.new()
        |> Core.observe(0)
        |> Core.observe(1_100)

      assert c.samples_seen == 1
      assert c.last_interval_ms == 1_100.0
      assert_in_delta c.mean, 1_050.0, 1.0e-9
      assert_in_delta c.variance, 7_500.0, 1.0e-9
    end

    test "triggers :recovering on large interval and counts down" do
      c =
        [
          initial_interval_ms: 1_000,
          recovering_threshold_ms: 5_000,
          recovering_grace_samples: 3
        ]
        |> Core.new()
        |> Core.observe(0)
        |> Core.observe(10_000)

      assert c.recovering_remaining == 3
      c = Core.observe(c, 11_000)
      assert c.recovering_remaining == 2
      c = Core.observe(c, 12_000)
      assert c.recovering_remaining == 1
      c = Core.observe(c, 13_000)
      assert c.recovering_remaining == 0
    end
  end

  describe "phi/2" do
    test "before any arrival: :insufficient_data" do
      c = Core.new()
      assert {:insufficient_data, 8} = Core.phi(c, 1_000)
    end

    test "after first arrival but before min_samples: :insufficient_data" do
      c = Core.new(min_samples: 4) |> Core.observe(100)
      assert {:insufficient_data, 4} = Core.phi(c, 500)

      c = Core.observe(c, 1_100)
      assert {:insufficient_data, 3} = Core.phi(c, 1_200)
    end

    test ":stale when elapsed exceeds stale_after_ms" do
      c = Core.new(stale_after_ms: 5_000, min_samples: 2) |> Core.observe(0)
      assert {:stale, 10_000} = Core.phi(c, 10_000)
    end

    test ":steady when warm and next arrival is on-schedule" do
      c = warm_core(interval: 1_000, samples: 20)
      last_ts = 20 * 1_000
      {:ok, phi, :steady} = Core.phi(c, last_ts + 1_000)
      assert phi < 2.0
    end

    test ":recovering tag persists through the grace window" do
      c =
        [
          initial_interval_ms: 1_000,
          min_samples: 2,
          recovering_threshold_ms: 5_000,
          recovering_grace_samples: 3
        ]
        |> Core.new()
        |> Core.observe(0)
        |> Core.observe(1_000)
        |> Core.observe(11_000)

      assert {:ok, _phi, :recovering} = Core.phi(c, 12_000)
    end

    test "phi grows as elapsed grows" do
      c = warm_core(interval: 1_000, samples: 10)
      last_ts = 10 * 1_000
      {:ok, phi1, :steady} = Core.phi(c, last_ts + 1_100)
      {:ok, phi2, :steady} = Core.phi(c, last_ts + 1_500)
      {:ok, phi3, :steady} = Core.phi(c, last_ts + 2_500)
      assert phi1 < phi2
      assert phi2 < phi3
    end

    test "phi becomes very large well past mean + many σ" do
      c = warm_core(interval: 1_000, samples: 20)
      last_ts = 20 * 1_000
      {:ok, phi, _} = Core.phi(c, last_ts + 30_000)
      assert phi > 5.0
    end
  end

  describe "properties" do
    property "phi is non-negative" do
      check all(elapsed <- integer(0..100_000)) do
        c = warm_core(interval: 1_000, samples: 10)
        last_ts = 10 * 1_000

        case Core.phi(c, last_ts + elapsed) do
          {:ok, phi, _} -> assert phi >= 0.0
          {:stale, _} -> :ok
          {:insufficient_data, _} -> :ok
        end
      end
    end

    property "phi is non-decreasing in elapsed (within :ok)" do
      check all(
              e1 <- integer(0..30_000),
              delta <- integer(1..30_000)
            ) do
        c = warm_core(interval: 1_000, samples: 20)
        last_ts = 20 * 1_000

        case {Core.phi(c, last_ts + e1), Core.phi(c, last_ts + e1 + delta)} do
          {{:ok, p1, _}, {:ok, p2, _}} -> assert p1 <= p2 + 1.0e-9
          _ -> :ok
        end
      end
    end

    property "bootstrap requires min_samples observations" do
      check all(n <- integer(1..7)) do
        c =
          Enum.reduce(1..n, Core.observe(Core.new(min_samples: 8), 0), fn i, acc ->
            Core.observe(acc, i * 1_000)
          end)

        assert {:insufficient_data, _} = Core.phi(c, (n + 1) * 1_000)
      end
    end
  end

  defp warm_core(opts) do
    interval = Keyword.fetch!(opts, :interval)
    samples = Keyword.fetch!(opts, :samples)

    c =
      Core.new(
        initial_interval_ms: interval,
        initial_std_dev_ms: interval * 0.2,
        min_samples: 8,
        alpha_mean: 0.2,
        alpha_var: 0.05,
        min_std_dev_ms: interval * 0.2
      )

    Enum.reduce(0..samples, c, fn i, acc -> Core.observe(acc, i * interval) end)
  end
end
