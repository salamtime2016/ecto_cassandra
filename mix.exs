defmodule EctoCassandra.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_cassandra,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: dialyzer(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:credo, "~> 1.0", only: ~w(dev test)a},
      {:db_connection, "~> 2.0", override: true},
      {:dialyxir, "~> 1.0.0-rc.3", only: ~w(dev test)a, runtime: false},
      {:ecto, "~> 3.0"},
      {:ecto_sql, "~> 3.0"},
      {:ex_doc, "~> 0.14", only: :dev},
      {:ex_machina, "~> 2.2", only: :test},
      {:excoveralls, "~> 0.8", only: :test},
      {:faker, "~> 0.10", only: :test},
      {:mix_test_watch, "~> 0.7", only: :dev, runtime: false},
      {:mox, "~> 0.4", only: :test},
      {:xandra, "~> 0.10"},
      {:uuid, "~> 1.1"}
    ]
  end

  # Dialyzer's configuration
  defp dialyzer do
    [
      flags: ~w(unmatched_returns error_handling race_conditions underspecs unknown)a,
      ignore_warnings: "config/dialyzer.ignore-warnings",
      plt_add_apps: ~w(mix ex_unit)a
    ]
  end
end
