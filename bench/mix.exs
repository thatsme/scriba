defmodule ScribaBench.MixProject do
  use Mix.Project

  def project do
    [
      app: :scriba_bench,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: false,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      mod: {ScribaBench.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      # Scriba via path — the benchmark measures the working tree, not a
      # published release.
      {:scriba, path: ".."},
      {:commanded, "~> 1.4"},

      # The whole point: a real persistent event store, not the InMemory
      # adapter. Subscription delivery semantics differ from InMemory only
      # in that these are the ones production runs on.
      {:eventstore, "~> 1.4"},
      {:commanded_eventstore_adapter, "~> 1.4"},
      {:ecto_sql, "~> 3.11"},
      {:postgrex, "~> 0.17"},
      {:jason, "~> 1.4"}
    ]
  end

  defp aliases do
    [
      "bench.setup": ["event_store.create", "event_store.init", "ecto.create", "ecto.migrate"]
    ]
  end
end
