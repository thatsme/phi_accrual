defmodule PhiAccrual.Core do
  @moduledoc """
  Pure EWMA φ-accrual math. No processes, no side effects.

  State evolves via `observe/2` (fed the arrival timestamp of a new
  heartbeat) and is queried via `phi/2` (computes φ for the elapsed
  time since the last arrival).

  The estimator follows Hayashibara 2004, with West 1979 incremental
  variance and **separate α for mean and variance** (dual-α EWMA):

      delta      = sample - mean
      mean'      = mean + α_mean * delta
      variance'  = (1 - α_var) * (variance + α_var * delta²)

  Variance typically needs more smoothing than mean — otherwise a single
  anomalous sample doubles variance and craters φ. Defaults keep both α
  equal; tune `alpha_var` smaller than `alpha_mean` under bursty input.
  """

  @default_alpha_mean 0.125
  @default_alpha_var 0.125
  @default_min_std_dev_ms 50.0
  @default_min_samples 8
  @default_stale_after_ms 60_000
  @default_recovering_threshold_ms 10_000
  @default_recovering_grace_samples 3
  @default_initial_interval_ms 1_000.0
  @default_initial_std_dev_ms 500.0

  defstruct [
    :mean,
    :variance,
    :last_arrival_ts,
    :last_interval_ms,
    samples_seen: 0,
    recovering_remaining: 0,
    alpha_mean: @default_alpha_mean,
    alpha_var: @default_alpha_var,
    min_std_dev_ms: @default_min_std_dev_ms,
    min_samples: @default_min_samples,
    stale_after_ms: @default_stale_after_ms,
    recovering_threshold_ms: @default_recovering_threshold_ms,
    recovering_grace_samples: @default_recovering_grace_samples
  ]

  @type t :: %__MODULE__{
          mean: float(),
          variance: float(),
          last_arrival_ts: integer() | nil,
          last_interval_ms: float() | nil,
          samples_seen: non_neg_integer(),
          recovering_remaining: non_neg_integer(),
          alpha_mean: float(),
          alpha_var: float(),
          min_std_dev_ms: float(),
          min_samples: pos_integer(),
          stale_after_ms: pos_integer(),
          recovering_threshold_ms: pos_integer(),
          recovering_grace_samples: non_neg_integer()
        }

  @type phi_result ::
          {:ok, float(), :steady}
          | {:ok, float(), :recovering}
          | {:insufficient_data, pos_integer()}
          | {:stale, non_neg_integer()}

  @doc """
  Build fresh estimator state. Accepts any struct field as a keyword override,
  plus `:initial_interval_ms` / `:initial_std_dev_ms` which seed `mean` and
  `variance` before the first sample lands.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    initial_interval =
      Keyword.get(opts, :initial_interval_ms, @default_initial_interval_ms) * 1.0

    initial_std =
      Keyword.get(opts, :initial_std_dev_ms, @default_initial_std_dev_ms) * 1.0

    fields =
      opts
      |> Keyword.drop([:initial_interval_ms, :initial_std_dev_ms])
      |> Keyword.merge(
        mean: initial_interval,
        variance: initial_std * initial_std,
        last_arrival_ts: nil,
        last_interval_ms: nil
      )

    struct!(__MODULE__, fields)
  end

  @doc """
  Record a heartbeat arrival at local monotonic-ms timestamp `ts`.

  First call seeds `last_arrival_ts` without updating mean/variance —
  an EWMA update needs two timestamps to derive an interval.
  """
  @spec observe(t(), integer()) :: t()
  def observe(%__MODULE__{last_arrival_ts: nil} = state, ts) do
    %{state | last_arrival_ts: ts}
  end

  def observe(%__MODULE__{} = state, ts) do
    interval = (ts - state.last_arrival_ts) * 1.0

    delta = interval - state.mean
    mean = state.mean + state.alpha_mean * delta
    variance = (1.0 - state.alpha_var) * (state.variance + state.alpha_var * delta * delta)

    recovering_remaining =
      cond do
        interval > state.recovering_threshold_ms -> state.recovering_grace_samples
        state.recovering_remaining > 0 -> state.recovering_remaining - 1
        true -> 0
      end

    %{
      state
      | mean: mean,
        variance: variance,
        last_arrival_ts: ts,
        last_interval_ms: interval,
        samples_seen: state.samples_seen + 1,
        recovering_remaining: recovering_remaining
    }
  end

  @doc """
  Compute φ given state and current monotonic-ms timestamp.

  Returns one of four states per the v1 contract:

    * `{:ok, phi, :steady}`     — warm estimator, normal operation
    * `{:ok, phi, :recovering}` — warm estimator, still absorbing a recent gap
    * `{:insufficient_data, n}` — bootstrap phase, `n` samples remaining
    * `{:stale, elapsed_ms}`    — no arrival for longer than `stale_after_ms`
  """
  @spec phi(t(), integer()) :: phi_result()
  def phi(%__MODULE__{last_arrival_ts: nil, min_samples: n}, _now) do
    {:insufficient_data, n}
  end

  def phi(%__MODULE__{} = state, now) do
    elapsed = now - state.last_arrival_ts

    cond do
      elapsed > state.stale_after_ms ->
        {:stale, max(elapsed, 0)}

      state.samples_seen < state.min_samples ->
        {:insufficient_data, state.min_samples - state.samples_seen}

      state.recovering_remaining > 0 ->
        {:ok, compute_phi(elapsed, state), :recovering}

      true ->
        {:ok, compute_phi(elapsed, state), :steady}
    end
  end

  @doc false
  @spec compute_phi(number(), t()) :: float()
  def compute_phi(elapsed, %__MODULE__{mean: mean, variance: variance, min_std_dev_ms: floor}) do
    std = max(:math.sqrt(variance), floor)
    y = (elapsed - mean) / std
    g = y * (1.5976 + 0.070566 * y * y)

    # φ = -log₁₀(1 - Φ(y)) where Φ is the Hayashibara logistic Gaussian
    # approximation. That equals softplus(g) / ln(10). The softplus
    # formulation is numerically stable for any g (no :math.exp overflow
    # on very large |y|).
    safe_softplus(g) * 0.43429448190325176
  end

  @compile {:inline, safe_softplus: 1}
  defp safe_softplus(x) when x >= 0.0, do: x + :math.log(1.0 + :math.exp(-x))
  defp safe_softplus(x), do: :math.log(1.0 + :math.exp(x))
end
