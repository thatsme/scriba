defmodule Bank.MixProject do
  use Mix.Project

  def project do
    [
      app: :bank,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      mod: {Bank.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      # Scriba via path while iterating in-repo. Post-publish users would
      # use {:scriba, "~> 0.1.0"} from Hex.
      {:scriba, path: "../.."},
      {:commanded, "~> 1.4"},
      {:ecto_sql, "~> 3.11"},
      {:postgrex, "~> 0.17"},
      {:jason, "~> 1.4"}
    ]
  end

  defp aliases do
    [
      # `mix bank.setup` chains ecto.create + ecto.migrate so a fresh
      # clone runs the demo in two commands (mix deps.get; mix bank.demo
      # — which calls bank.setup internally if needed). Kept as a
      # standalone alias for users who want explicit control.
      "bank.setup": ["ecto.create", "ecto.migrate"]
    ]
  end
end
