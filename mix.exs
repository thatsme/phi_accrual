defmodule PhiAccrual.MixProject do
  use Mix.Project

  @version "1.0.0"
  @source_url "https://github.com/thatsme/phi_accrual"

  def project do
    [
      app: :phi_accrual,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      name: "phi_accrual",
      source_url: @source_url,
      docs: docs(),
      elixirc_options: [warnings_as_errors: true]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {PhiAccrual.Application, []}
    ]
  end

  defp deps do
    [
      {:telemetry, "~> 1.2"},
      {:stream_data, "~> 1.1", only: [:test, :dev]},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp description do
    "Source-agnostic φ-accrual failure detector for Elixir/OTP. " <>
      "Observability-grade, telemetry-first, EWMA-based. " <>
      "Emits a continuous suspicion value per monitored node; " <>
      "thresholding and policy are consumer concerns."
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"
      },
      files: ~w(lib mix.exs README.md CHANGELOG.md LICENSE .formatter.exs)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        "README.md",
        "CHANGELOG.md",
        "LICENSE"
      ],
      source_ref: "v#{@version}",
      source_url: @source_url,
      formatters: ["html"],
      groups_for_modules: [
        "Public API": [
          PhiAccrual
        ],
        "Core & math": [
          PhiAccrual.Core,
          PhiAccrual.Clock
        ],
        "Runtime": [
          PhiAccrual.Estimator,
          PhiAccrual.EstimatorSupervisor,
          PhiAccrual.PauseMonitor
        ],
        "Sources": [
          PhiAccrual.Source,
          PhiAccrual.Source.DistributionPing
        ],
        "Thresholding": [
          PhiAccrual.Threshold
        ]
      ]
    ]
  end
end
